"""Single-control-plane SSH dispatch and restart reconciliation."""
import concurrent.futures
import hashlib
import json
import shlex
import subprocess
import time
import uuid
from pathlib import Path

from . import agent
from .planner import base_health, gpu_healthy, placements
from .schema import job_filesystem, node_filesystem, RTL_KINDS
from .store import ACTIVE, dumps
from .states import observe_health, recovery_due, transition
from .draining import publication_probe_due, publication_probe_health


class ExpiredBatchSnapshot(ValueError):
    def __init__(self, nodes):
        super().__init__('batch resource snapshot expired before reservation')
        self.nodes = nodes


class Transport:
    def call(self, node, action, request):
        source = Path(agent.__file__).read_text()
        argv = [node["python"], "-c", source]
        if node["transport"] == "ssh":
            argv = ["ssh", "-T", "-o", "BatchMode=yes", "-o", "ConnectTimeout=5",
                    node["target"], shlex.join(argv)]
        timeout = node.get("_rpc_timeout_s", max(30, node["policy"]["read_probe_timeout_s"] + 20))
        if node["transport"] == "ssh":
            argv[5] = "ConnectTimeout=" + str(max(5, int(timeout / 3)))
        try:
            result = subprocess.run(argv, input=dumps({"action": action, "request": request, "source": source}),
                                    text=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE, timeout=timeout)
        except subprocess.TimeoutExpired as exc:
            raise TimeoutError(f"{node['id']} {action}: response timeout after {timeout}s") from exc
        if result.returncode:
            raise RuntimeError("remote helper failed: " + result.stderr[-1500:])
        value=json.loads(result.stdout)
        if action=='probe':
            from .gpu_ownership import enrich
            value=enrich(node,request,value)
        return value


class Controller:
    def __init__(self, store, transport=None):
        self.store = store
        self.transport = transport or Transport()

    def snapshots(self):
        return {r["node"]: json.loads(r["data"]) for r in self.store.db.execute("SELECT * FROM snapshots")}

    def node_health(self):
        return {r["node"]: json.loads(r["data"]) for r in self.store.db.execute("SELECT * FROM node_health")}

    def refresh(self, node_ids=None):
        """Caller holds scheduler lock; probes run concurrently, all are read-only."""
        nodes = self.store.specs("nodes")
        health = self.node_health()
        selected = [n for key, n in nodes.items() if (node_ids is None or key in node_ids)
                    and (recovery_due(health.get(key, {}), time.time())
                         or publication_probe_due(n, health.get(key, {}), time.time()))]
        previous = self.snapshots()
        # Authoritative active registrations, not all processes owned by a UID.
        registered = {}
        query = "SELECT id,node,json_extract(spec,'$.attempt_dir') AS directory FROM attempts WHERE status IN ('starting','running','unknown') UNION ALL SELECT id,node,json_extract(spec,'$.attempt_dir') FROM artifact_transfers WHERE status IN ('starting','running','unknown')"
        for row in self.store.db.execute(query):
            registered.setdefault(row['node'], []).append(dict(id=row['id'], attempt_dir=row['directory']))
        results = {}
        with concurrent.futures.ThreadPoolExecutor(max_workers=8) as pool:
            futures = {}
            for n in selected:
                recovery = health.get(n["id"], {})
                transport_node = dict(n)
                if recovery.get("phase") == "ssh_retrying":
                    idx = recovery["retries_done"] % n["recovery"]["ssh_attempts_per_round"]
                    transport_node["_rpc_timeout_s"] = n["recovery"]["ssh_timeouts_s"][idx]
                prior = previous.get(n['id'], {})
                request = dict(n, _registered_attempts=registered.get(n['id'], []),
                               _d_state_previous={k:prior[k] for k in ('boot_id','d_state_tracks','d_state_sample_at') if k in prior})
                futures[pool.submit(self.transport.call, transport_node, "probe", request)] = n
            for future in concurrent.futures.as_completed(futures):
                n = futures[future]
                try:
                    data = future.result()
                except Exception as exc:
                    data = {"error": str(exc)}
                now = time.time()
                data["received_at"] = now
                old = previous.get(n["id"], {})
                # Observation cadence is separate from admission freshness.
                # A healthy 62s polling cycle must not reset a three-poll streak
                # when the newly received snapshot still has a 60s launch TTL.
                poll_gap = n["policy"].get("max_health_poll_gap_s", 120)
                continuous = (old.get("boot_id") == data.get("boot_id")
                              and 0 <= now - old.get("received_at", 0) <= poll_gap)
                healthy = not base_health(n, data, now)
                # Rapid CLI calls cannot manufacture three independent health polls.
                count_poll = now - old.get("last_counted_at", 0) >= 2
                data["last_counted_at"] = now if count_poll else old.get("last_counted_at", now)
                data["stable_polls"] = ((old.get("stable_polls", 0) if continuous else 0)
                                        + int(count_poll)) if healthy else 0
                old_gpus = {g["uuid"]: g for g in old.get("gpus", [])}
                mode = "shared" if n["policy"]["allow_gpu_sharing"] else "exclusive"
                for gpu in data.get("gpus", []):
                    good = healthy and gpu_healthy(gpu, n, mode)
                    gpu["stable_polls"] = ((old_gpus.get(gpu["uuid"], {}).get("stable_polls", 0)
                                            if continuous else 0) + int(count_poll)) if good else 0
                results[n["id"]] = data
        with self.store.db:
            for key, value in results.items():
                self.store.db.execute("INSERT OR REPLACE INTO snapshots VALUES(?,?)", (key, dumps(value)))
                old_health = health.get(key, {})
                state = observe_health(old_health, value, nodes[key]["recovery"], value["received_at"])
                publication_state = publication_probe_health(
                    nodes[key], old_health, value['stable_polls'],
                    not base_health(nodes[key], value, value['received_at']), value['received_at'])
                if publication_state is not None:
                    state = publication_state
                self.store.db.execute("INSERT OR REPLACE INTO node_health VALUES(?,?)", (key, dumps(state)))
                if state != health.get(key, {}):
                    self.store.event("node_health", key, state)
        return results

    def stabilize_healthy_nodes(self):
        """Collect bounded independent polls when a long loop loses its streak.

        Retain the existing three-poll gate and snapshot TTL. This never declares
        an unhealthy node/GPU healthy or lifts any admission restriction.
        """
        for _ in range(2):
            now = time.time()
            snapshots = self.snapshots()
            health = self.node_health()
            selected = []
            for key, node in self.store.specs('nodes').items():
                snap = snapshots.get(key, {})
                publication_source = (not node['enabled']
                    and bool(node.get('labels',{}).get('gpu_runtime_quarantine'))
                    and health.get(key, {}).get('phase') == 'unavailable')
                if (not node['enabled'] and not publication_source) or base_health(node, snap, now):
                    continue
                required = node['policy']['stable_polls']
                allowed = {g['uuid'] for g in node['gpus'] if g['enabled']
                           and g['uuid'] not in node['policy'].get('disabled_gpu_uuids', [])}
                mode = 'shared' if node['policy']['allow_gpu_sharing'] else 'exclusive'
                needs_gpu = any(g['uuid'] in allowed and gpu_healthy(g, node, mode)
                                and g.get('stable_polls', 0) < required for g in snap.get('gpus', []))
                if snap.get('stable_polls', 0) < required or (node['enabled'] and needs_gpu):
                    selected.append(key)
            if not selected:
                break
            # refresh() independently enforces >=2s between counted samples.
            time.sleep(2)
            self.refresh(selected)
            self.invalidate_unavailable()

    def plan(self):
        from .artifacts import reservations
        nodes, snapshots = self.store.specs("nodes"), self.snapshots()
        jobs = self.store.jobs()
        groups = self.store.specs("groups_")
        health = self.node_health()
        for key, state in health.items():
            if state.get("phase") in ("unavailable", "ssh_retrying"):
                snapshots[key] = dict(snapshots.get(key, {}), error="node " + state["phase"])
        # Shared-backend faults block new starts on all members, not only on the
        # node where the fault was observed. An unreachable member fails closed.
        blocked_groups = {}
        now = time.time()
        for key, n in nodes.items():
            if n["enabled"] and n["startup_group"]:
                s = snapshots.get(key, {})
                if (s.get("error") or not s.get("read_ok") or s.get("d_state", 1)
                        or not 0 <= now - s.get("received_at", 0) <= n["policy"]["max_snapshot_age_s"]):
                    blocked_groups[n["startup_group"]] = key
        # Do not poison a whole node snapshot: jobs using approved local data do
        # not depend on the shared cold-start backend. fit() applies this fault
        # only when that particular job still has an effective startup group.
        nodes={key:dict(n,_blocked_startup_groups=blocked_groups) for key,n in nodes.items()}
        return placements(jobs, self.store.specs("experiments"), nodes,
                          snapshots, self._planning_attempts(jobs, groups, now) + reservations(self.store),
                          groups, now)

    def _planning_attempts(self, jobs, groups, now):
        """Load only attempts needed by live admission, lineage, and recent starts."""
        by_id = {j['id']: j for j in jobs}
        queued = [j for j in jobs if j['status'] == 'queued']
        relevant = {j['id'] for j in queued}
        relevant.update(d for j in queued for d in j['spec'].get('depends_on', []))
        # Lineage walks the whole ancestor DAG, but only frozen boundaries and
        # explicit source bindings need historical attempt receipts.
        visited, frontier = set(), [j['id'] for j in queued]
        while frontier:
            key = frontier.pop()
            if key in visited or key not in by_id:
                continue
            visited.add(key)
            spec = by_id[key]['spec']
            if spec.get('metadata', {}).get('frozen_initialization_authority'):
                relevant.add(key)
            source = spec.get('metadata', {}).get('epoch_dependency', {}).get('source_job')
            if source in by_id:
                relevant.add(source)
            frontier.extend(spec.get('depends_on', []))
        relevant.update(r[0] for r in self.store.db.execute(
            "SELECT DISTINCT job FROM attempts WHERE status IN ('starting','running','unknown')"))
        max_interval = max((g.get('min_start_interval_s', 0) for g in groups.values()), default=0)
        if max_interval:
            relevant.update(r[0] for r in self.store.db.execute(
                'SELECT DISTINCT job FROM attempts WHERE created>=?', (now-max_interval,)))
        # Failed startup reservations can quarantine a GPU for the same boot;
        # do not lose this evidence when trimming unrelated historical attempts.
        relevant.update(r[0] for r in self.store.db.execute(
            "SELECT DISTINCT job FROM attempts WHERE status='failed' AND released=1 "
            "AND json_extract(report,'$.failure_class')='gpu_startup_unavailable'"))
        return self.store.attempts(job_ids=relevant, planning=True)

    def invalidate_unavailable(self):
        health = self.node_health()
        for a in self.store.attempts(active=True):
            h = health.get(a["node"], {})
            if h.get("phase") != "unavailable":
                continue
            # A timeout is not proof that a tool or board operation stopped.
            # Preserve its reservation across all gateways and aliases.
            if (a['spec'].get('job_spec', {}).get('kind') in RTL_KINDS
                    or a['spec']['resources'].get('tokens')
                    or a['spec'].get('node_spec', {}).get('physical_host')):
                continue
            row = self.store.db.execute('SELECT spec,status FROM jobs WHERE id=?', (a['job'],)).fetchone()
            job = dict(spec=json.loads(row['spec']), status=row['status'])
            count = self.store.db.execute('SELECT COUNT(*) FROM attempts WHERE job=?', (a['job'],)).fetchone()[0]
            safe = job["spec"]["failover_safe"] and count < job["spec"]["max_attempts"]
            if h.get('reason') == 'continuous D-state exceeded timeout' and not safe:
                # Host I/O stalls do not establish that this scientific child died
                # or that its outputs are corrupt. Keep its identity and resources.
                reason = 'node I/O health unavailable; existing process not declared failed; reservation preserved'
                report = dict(a['report'], status='unknown', reason=reason,
                              node_health_reason=h['reason'])
                with self.store.db:
                    self.store.db.execute("UPDATE attempts SET status='unknown',released=0,report=? WHERE id=?",
                                          (dumps(report), a['id']))
                    self.store.db.execute("UPDATE jobs SET status='unknown',reason=? WHERE id=?", (reason, a['job']))
                    if a['status'] != 'unknown':
                        self.store.event('node_io_attempt_held', a['id'], {'reason': reason, 'node_health': h})
                continue
            status = "queued" if safe else "blocked"
            reason = "previous attempt invalid: " + h["reason"]
            if not safe:
                reason += "; automatic failover disabled or attempt budget exhausted"
            transition("attempt", a["status"], "invalid")
            transition("job", job["status"], status)
            with self.store.db:
                self.store.db.execute("UPDATE attempts SET status='invalid',released=1 WHERE id=?", (a["id"],))
                self.store.db.execute("UPDATE jobs SET status=?,reason=? WHERE id=?", (status, reason, a["job"]))
                self.store.event("attempt_invalid", a["id"], {"reason": reason, "remote_process": "unknown; NOT killed",
                                                             "late_results": "ignored", "failover": safe})

    def _status_reports(self, attempts, health):
        """Read status concurrently across nodes, one bounded RPC per node.

        Workers never access SQLite or reserve/launch anything. Consume reports
        in original attempt order so lifecycle writes retain serial semantics.
        """
        grouped, reports = {}, {}
        for a in attempts:
            if health.get(a['node'], {}).get('phase') in ('ssh_retrying', 'unavailable'):
                reports[a['id']] = dict(a['report'], status='unknown', reason='node recovery in progress',
                                       node_health_reason=health[a['node']].get('reason', 'SSH recovery'))
            else:
                grouped.setdefault(a['node'], []).append(a)

        def read_node(rows):
            requests = []
            for a in rows:
                request=dict(a['spec'],_oom_previous=a.get('report',{}).get('oom_observation',{}))
                if a.get('report',{}).get('failure_class')=='experiment_oom':
                    request['_oom_verified_evidence']=a['report'].get('failure_evidence')
                requests.append(request)
            try:
                result=self.transport.call(rows[0]['spec']['node_spec'],'status_batch',
                                           {'attempts':requests})
                if not isinstance(result,dict) or set(result)!={a['id'] for a in rows}:
                    raise ValueError('incomplete status batch')
                return result
            except Exception:
                # Keep existing per-attempt recovery for old/fake transports or
                # a lost batch response; no lifecycle write is inferred from ACK.
                result={}
                for a,request in zip(rows,requests):
                    try:
                        result[a['id']]=self.transport.call(a['spec']['node_spec'],'status',request)
                    except Exception as exc:
                        result[a['id']]=dict(status='unknown',reason=str(exc))
                return result

        with concurrent.futures.ThreadPoolExecutor(max_workers=8) as pool:
            for result in pool.map(read_node, grouped.values()):
                reports.update(result)
        return [(a, reports[a['id']]) for a in attempts]

    def reconcile(self, recover_oom=False, exclude_attempts=()):
        snapshots, groups = self.snapshots(), self.store.specs("groups_")
        health = self.node_health()
        active=[a for a in self.store.attempts(active=True) if a['id'] not in exclude_attempts]
        for a, report in self._status_reports(active, health):
            node = a['spec']['node_spec']
            if (recover_oom and report.get('failure_class')=='experiment_oom'
                    and not report.get('termination_verified')):
                cleanup_count=a.get('report',{}).get('oom_cleanup_attempts',0)
                if cleanup_count>=2:
                    report=dict(report,status='unknown',oom_cleanup_attempts=cleanup_count,
                                reason='OOM cleanup budget exhausted; manual termination confirmation required')
                    # Preserve the reservation; do not create a replacement.
                    status='unknown'
                else:
                    try:
                        request=dict(a['spec'],_oom_previous=report.get('oom_observation',{}),
                                     _oom_verified_evidence=report.get('failure_evidence'))
                        report=self.transport.call(node,'recover_oom',request)
                    except Exception as exc:
                        report=dict(report,status='unknown',termination_verified=False,
                                    reason='OOM cleanup unconfirmed: '+type(exc).__name__)
                    report=dict(report,oom_cleanup_attempts=cleanup_count+1)
            status = report.get("status", "unknown")
            if status not in (*ACTIVE, "succeeded", "failed"):
                status = "unknown"
            if status in ("running", "starting") and report.get("heartbeat", report.get("started", time.time())) < time.time() - 120:
                status = "unknown"
                report["reason"] = "stale runner heartbeat; no automatic relaunch"
            if status == "succeeded":
                if report.get("returncode") != 0 or set(report.get("outputs", {})) != set(a["spec"]["outputs"]):
                    status = "unknown"
                    report["reason"] = "success receipt is incomplete"
                job_spec = a['spec'].get('job_spec', {})
                declared = job_spec.get('dependency_artifacts', job_spec.get('hf_artifacts', []))
                if status == 'succeeded' and declared and not report.get('dependency_artifacts'):
                    try:
                        repair_request = dict(a['spec'])
                        current = self.store.db.execute('SELECT spec FROM jobs WHERE id=?', (a['job'],)).fetchone()
                        current_spec = json.loads(current['spec']) if current else {}
                        minimal = current_spec.get('dependency_artifacts')
                        if minimal:
                            repair_request['job_spec'] = dict(job_spec, dependency_artifacts=minimal)
                        repaired = self.transport.call(node, 'dependency_manifest', repair_request)
                        report['dependency_artifacts'] = repaired['dependency_artifacts']
                    except Exception as exc:
                        status = 'unknown'
                        report['reason'] = ('dependency artifact manifest unavailable: '
                                            + type(exc).__name__ + ': ' + str(exc))
                if (a['spec'].get('job_spec', {}).get('kind') in RTL_KINDS
                        and report.get('validation', {}).get('status') != 'pass'):
                    status = 'unknown'
                    report['reason'] = 'RTL validation receipt is incomplete'
            released = a["released"]
            polls = a["ready_polls"]
            group = a["spec"]["startup_group"]
            if status in ("succeeded", "failed"):
                released = True
            elif group and status == "running":
                ready = report.get("ready") or not groups[group]["require_ready_marker"]
                snap = snapshots.get(a["node"], {})
                if ready and not base_health(node, snap, time.time()):
                    # Count at most once for each newly counted resource poll.
                    previous_poll = a["report"].get("health_poll_at", 0)
                    polls += int(snap["last_counted_at"] > previous_poll)
                    report["health_poll_at"] = snap["last_counted_at"]
                else:
                    polls = 0
                released = released or polls >= node["policy"]["stable_polls"]
            with self.store.db:
                transition("attempt", a["status"], status)
                self.store.db.execute("UPDATE attempts SET status=?,released=?,ready_polls=?,report=? WHERE id=?",
                                      (status, int(released), polls, dumps(report), a["id"]))
                job_status = status
                if status == "failed":
                    j = self.store.db.execute('SELECT spec FROM jobs WHERE id=?', (a['job'],)).fetchone()
                    count = self.store.db.execute("SELECT COUNT(*) FROM attempts WHERE job=?", (a["job"],)).fetchone()[0]
                    from .gpu_recovery import retry_spec
                    retried = retry_spec(json.loads(j['spec']), report, count)
                    if retried is not None:
                        self.store.db.execute('UPDATE jobs SET spec=? WHERE id=?', (dumps(retried), a['job']))
                        self.store.event('experiment_oom_failover' if report.get('failure_class') == 'experiment_oom' else 'gpu_startup_failover', a['job'],
                                         dict(attempt=a['id'], node=a['node'], gpus=a['spec']['gpus'],
                                              retry=retried['metadata'].get('gpu_startup_failovers', len(retried['metadata'].get('oom_failovers', [])))))
                        j = dict(spec=dumps(retried))
                    if count < json.loads(j['spec'])["max_attempts"]:
                        job_status = "queued"
                reason = report.get("reason", report.get("error", ""))
                old_job = self.store.db.execute('SELECT status FROM jobs WHERE id=?', (a['job'],)).fetchone()
                transition("job", old_job["status"], job_status)
                self.store.db.execute("UPDATE jobs SET status=?,reason=? WHERE id=?", (job_status, reason, a["job"]))
                if status != a["status"]:
                    self.store.event("attempt_" + status, a["id"], report)

    def _request_context(self, placements):
        """Read selected jobs and their dependencies once; retain full launch audit specs."""
        keys = list(dict.fromkeys(p['job'] for p in placements))
        if not keys:
            return dict(nodes={}, jobs={}, experiments={}, successful={})
        query = 'SELECT * FROM jobs WHERE id IN (' + ','.join('?' for _ in keys) + ')'
        jobs = {r['id']: dict(r, spec=json.loads(r['spec'])) for r in self.store.db.execute(query, keys)}
        if set(jobs) != set(keys):
            raise ValueError('selected job disappeared before request preparation')
        experiments = {}
        for key in {j['experiment'] for j in jobs.values()}:
            row = self.store.db.execute('SELECT spec FROM experiments WHERE id=?', (key,)).fetchone()
            experiments[key] = json.loads(row['spec'])
        dependencies = {d for j in jobs.values() for d in j['spec']['depends_on']}
        attempts = self.store.attempts(job_ids=dependencies, summary=True)
        return dict(nodes=self.store.specs('nodes'), jobs=jobs, experiments=experiments,
                    successful={a['job']: a for a in attempts if a['status']=='succeeded'})

    def request(self, placement, context=None):
        s = self.store
        context = context if context is not None else self._request_context([placement])
        node = context['nodes'][placement['node']]
        job = context['jobs'][placement['job']]
        spec = job["spec"]
        from .model_vram_policy import normalize
        spec = normalize(spec)
        resources = placement.get('resources', spec['resources'])
        from .execution_profiles import for_node
        profile=spec.get('metadata',{}).get('execution_profiles',{}).get(node['id'])
        if profile and resources!=profile.get('resource_contract'):
            raise ValueError('placement differs from validated execution profile')
        spec=for_node(spec,node['id'])
        if spec.get('metadata',{}).get('required_resources'):
            import copy
            from .resources import marker
            spec=copy.deepcopy(spec)
            for rid,sha in spec['metadata']['required_resources'].items():
                m=marker(rid,sha);asset=node['assets'].get(m['name'])
                if not asset or asset['sha256']!=m['sha256']:
                    raise ValueError('resource not verified on selected node: '+rid)
                spec['input_files'].append(copy.deepcopy(asset))
        from .hardware_resources import effective_resources
        resources = effective_resources(spec, node, resources)
        if resources not in [effective_resources(spec, node, r) for r in [spec['resources'], *spec.get('resource_variants', [])]]:
            raise ValueError('placement resources are not a registered variant')
        if spec.get('resource_variants') and len(placement['gpus']) != resources['gpu_count']:
            raise ValueError('GPU assignment differs from selected resource variant')
        dataset = spec.get("dataset", "")
        if dataset:
            dataset_path = node.get("datasets", {}).get(dataset)
            if not dataset_path:
                raise ValueError("dataset path not registered on node: " + dataset)
        else:
            dataset_path = spec.get("dataset_path", "")
        filesystem_request = job_filesystem(spec)
        filesystem = node_filesystem(node)
        if filesystem_request != "any" and filesystem_request != filesystem:
            raise ValueError("placement filesystem no longer satisfies job request")
        attempt_id = job["id"] + "." + uuid.uuid4().hex
        directory = str(Path(node["work_root"], "attempts", attempt_id))
        successful = context['successful']
        substitutions = {"{attempt_dir}": directory, "{config_path}": directory + "/config.json",
                         "{gpu_count}": str(len(placement["gpus"])), "{gpus}": ",".join(placement["gpus"]),
                         "{dataset_path}": dataset_path, "{filesystem}": filesystem}
        inputs = list(spec["input_files"])
        for dep in spec["depends_on"]:
            a = successful[dep]
            if dep in spec.get("order_only_dependencies", []):
                continue
            cached = a.get('artifact_locations', {}).get(node['id'])
            local = a['node'] == node['id'] or (node['storage_domain'] and
                    a['spec']['node_spec']['storage_domain'] == node['storage_domain'])
            if not local and cached:
                substitutions['{dep:' + dep + '}'] = cached['root']
                inputs.extend(dict(path=f['path'], sha256=f['sha256']) for f in cached['files'].values())
                continue
            if not local:
                raise ValueError('dependency has no verified destination artifacts: ' + dep)
            substitutions["{dep:" + dep + "}"] = a["spec"]["attempt_dir"]
            # Rehash declared predecessor artifacts on the destination before launch.
            artifacts = a["report"].get("dependency_artifacts", a["report"].get("outputs", {}))
            inputs.extend({"path": out["path"], "sha256": out["sha256"]}
                          for out in artifacts.values())
        inputs.extend(node["assets"][key] for key in spec["assets"])

        def expand(value):
            if "{dataset_path}" in value and not dataset_path:
                raise ValueError("dataset_path placeholder used without a registered path")
            for key, replacement in substitutions.items():
                value = value.replace(key, replacement)
            if "{dep:" in value:
                raise ValueError("unresolved dependency placeholder: " + value)
            return value

        def expand_config(value):
            if isinstance(value, str):
                return expand(value)
            if isinstance(value, list):
                return [expand_config(v) for v in value]
            if isinstance(value, dict):
                return {k: expand_config(v) for k, v in value.items()}
            return value

        from .startup import startup_group
        request = dict(id=attempt_id, job=job["id"], experiment=job["experiment"],
                       experiment_spec=context['experiments'][job['experiment']],
                       job_spec=spec, node_spec=node, attempt_dir=directory,
                       argv=[expand(v) for v in spec["argv"]], cwd=expand(spec["cwd"]),
                       env={k: expand(v) for k, v in spec["env"].items()}, config=expand_config(spec["config"]),
                       dataset=dataset, dataset_path=dataset_path,
                       filesystem_request=filesystem_request, filesystem=filesystem,
                       input_files=[dict(f, path=expand(f["path"])) for f in inputs],
                       outputs=spec["outputs"], resources=resources,
                       startup_group=startup_group(spec,node), gpus=placement["gpus"])
        request["runner_sha256"] = hashlib.sha256(Path(agent.__file__).read_bytes()).hexdigest()
        if spec['kind'] in RTL_KINDS:
            request['validation'] = spec['validation']
            request['preflight_argv'] = [expand(v) for v in spec.get('preflight_argv', [])]
        if spec['kind'] == 'board_test':
            request['board_lock'] = node['board_locks'][spec['board_id']]
        request["spec_sha256"] = hashlib.sha256(dumps(request).encode()).hexdigest()
        return request

    def _launch(self, chosen):
        request = self.request(chosen)
        with self.store.db:
            self.store.db.execute("INSERT INTO attempts(id,job,node,spec,status,created) VALUES(?,?,?,?,?,?)",
                                  (request["id"], request["job"], chosen["node"], dumps(request), "starting", time.time()))
            self.store.db.execute("UPDATE jobs SET status='starting',reason='' WHERE id=?", (request["job"],))
            self.store.event("attempt_reserved", request["id"], chosen)
        try:
            report = self.transport.call(request["node_spec"], "launch", request)
            # A lost ACK after the remote process starts is reconciled by ID.
            status = report.get("status", "unknown")
            if status not in ACTIVE:
                status = "unknown"
        except Exception as exc:
            report, status = {"reason": str(exc)}, "unknown"
        with self.store.db:
            self.store.db.execute("UPDATE attempts SET status=?,report=? WHERE id=?",
                                  (status, dumps(report), request["id"]))
            self.store.db.execute("UPDATE jobs SET status=?,reason=? WHERE id=?",
                                  (status, report.get("reason", ""), request["job"]))
            self.store.event("launch_ack", request["id"], report)
        return dict(chosen, attempt=request["id"], status=status)

    def _launch_batch(self, chosen):
        """Reserve one simulated plan atomically, then send independent RPCs."""
        context=self._request_context(chosen)
        requests=[self.request(p, context=context) for p in chosen]
        snapshots=self.snapshots()
        now=time.time()
        expired=[]
        for request in requests:
            node=request['node_spec'];snap=snapshots.get(node['id'],{})
            if not 0<=now-snap.get('received_at',0)<=node['policy']['max_snapshot_age_s']:
                expired.append(node['id'])
        if expired:
            raise ExpiredBatchSnapshot(list(dict.fromkeys(expired)))
        with self.store.db:
            for placement,request in zip(chosen,requests):
                row=self.store.db.execute('SELECT status FROM jobs WHERE id=?',(request['job'],)).fetchone()
                if not row or row['status']!='queued':raise ValueError('batch job is no longer queued')
                self.store.db.execute("INSERT INTO attempts(id,job,node,spec,status,created) VALUES(?,?,?,?,?,?)",
                    (request['id'],request['job'],placement['node'],dumps(request),'starting',time.time()))
                self.store.db.execute("UPDATE jobs SET status='starting',reason='' WHERE id=?",(request['job'],))
                self.store.event('attempt_reserved',request['id'],placement)
        def launch(request):
            try:
                report=self.transport.call(request['node_spec'],'launch',request)
                status=report.get('status','unknown')
                if status not in ACTIVE:status='unknown'
            except Exception as exc:report,status={'reason':str(exc)},'unknown'
            return report,status
        result=[]
        with concurrent.futures.ThreadPoolExecutor(max_workers=min(8,len(requests))) as pool:
            reports=list(pool.map(launch,requests))
        for placement,request,(report,status) in zip(chosen,requests,reports):
            with self.store.db:
                self.store.db.execute('UPDATE attempts SET status=?,report=? WHERE id=?',(status,dumps(report),request['id']))
                self.store.db.execute('UPDATE jobs SET status=?,reason=? WHERE id=?',(status,report.get('reason',''),request['job']))
                self.store.event('launch_ack',request['id'],report)
            result.append(dict(placement,attempt=request['id'],status=status))
        return result

    def tick(self, execute=False, refresh=True, max_launches=1, warmup=True, launch_budget_s=None, parallel_launches=False):
        if not isinstance(max_launches, int) or isinstance(max_launches, bool) or max_launches < 1:
            raise ValueError("max_launches must be a positive integer")
        if launch_budget_s is not None and (isinstance(launch_budget_s, bool)
                or not isinstance(launch_budget_s, (int, float)) or not 0 < launch_budget_s < float('inf')):
            raise ValueError('launch_budget_s must be positive and finite')
        with self.store.lock():
            phase_times = {}
            stage_started = time.monotonic()
            def mark(name):
                nonlocal stage_started
                now = time.monotonic()
                phase_times[name] = round(now - stage_started, 3)
                stage_started = now
            if refresh:
                self.refresh()
            self.invalidate_unavailable()
            mark('controller_refresh_s')
            self.reconcile(recover_oom=execute)
            mark('controller_reconcile_s')
            from .execution_profiles import tick as execution_tick
            execution_tick(self, execute=execute)
            mark('controller_execution_s')
            from .resources import tick as resource_tick
            resource_tick(self, execute=execute)
            mark('controller_resources_s')
            from .datasets import tick as dataset_tick
            dataset_tick(self, execute=execute)
            mark('controller_datasets_s')
            from .artifacts import tick as artifact_tick
            # Reconciliation/transfers may take longer than the launch TTL.
            # Refresh expired observations instead of returning an all-stale
            # plan and starving otherwise idle executors on every long cycle.
            def refresh_expired():
                if not refresh:
                    return
                now = time.time()
                snapshots = self.snapshots()
                expired = [key for key, node in self.store.specs('nodes').items()
                           if node['enabled'] and (key not in snapshots
                           or now - snapshots[key].get('received_at', 0) > node['policy']['max_snapshot_age_s'])]
                if expired:
                    self.refresh(expired)
                    self.invalidate_unavailable()
                return bool(expired)
            refresh_expired()
            if execute and refresh and warmup:
                self.stabilize_healthy_nodes()
            # Artifact admission needs the same fresh independent observations
            # as model admission, not the stale samples from before reconcile.
            artifact_tick(self, execute=execute)
            refresh_expired()
            mark('controller_artifacts_s')
            plan = self.plan()
            mark('controller_plan_s')
            if not execute:
                return {"mode": "dry-run", "plan": plan, "launches": [], "phase_times": phase_times}
            launches, skipped = [], set()
            batch_revalidations = 0
            current = plan
            launch_started = time.monotonic()
            # Probe the candidate hosts together instead of serial SSH before
            # every reservation. Replanning still includes every new lease.
            candidate_nodes=list(dict.fromkeys(p['node'] for p in current if p['decision']=='ready'))[:max_launches]
            probed=set()
            if candidate_nodes:
                self.refresh(candidate_nodes)
                self.invalidate_unavailable()
                current=self.plan()
                probed.update(candidate_nodes)
            while len(launches) < max_launches:
                # Yield between complete admissions, never interrupt a reserve
                # or RPC. Always permit one candidate so slow RPCs cannot starve it.
                if ((launches or skipped) and launch_budget_s is not None
                        and time.monotonic()-launch_started >= launch_budget_s):
                    break
                if refresh_expired():
                    current = self.plan()
                if parallel_launches:
                    candidates=[p for p in current if p['decision']=='ready' and p['job'] not in skipped][:max_launches-len(launches)]
                    if not candidates:break
                    snaps=self.snapshots()
                    stale=list(dict.fromkeys(p['node'] for p in candidates
                        if p['node'] not in probed or not 0<=time.time()-snaps.get(p['node'],{}).get('received_at',0)<=10))
                    if stale:
                        self.refresh(stale);self.invalidate_unavailable();probed.update(stale)
                        current=self.plan()
                        candidates=[p for p in current if p['decision']=='ready' and p['job'] not in skipped][:max_launches-len(launches)]
                    if not candidates:break
                    try:
                        launches.extend(self._launch_batch(candidates))
                    except ExpiredBatchSnapshot as exc:
                        if batch_revalidations >= 1:
                            skipped.update(p['job'] for p in candidates)
                            break  # Bounded recovery; never reserve stale resources.
                        self.refresh(exc.nodes)
                        self.invalidate_unavailable()
                        probed.update(exc.nodes)
                        batch_revalidations += 1
                        current=self.plan()
                        continue
                    current=self.plan()
                    continue
                chosen = next((p for p in current
                               if p["decision"] == "ready" and p["job"] not in skipped), None)
                if chosen is None:
                    break
                # Revalidate only the selected node immediately before reservation.
                # Existing reservations make subsequent replans account for every
                # launch in this cycle; startup groups therefore remain serialized.
                selected_snapshot=self.snapshots().get(chosen['node'],{})
                if chosen['node'] not in probed or not 0<=time.time()-selected_snapshot.get('received_at',0)<=10:
                    self.refresh([chosen["node"]])
                    self.invalidate_unavailable()
                    current = self.plan()
                    probed.add(chosen['node'])
                fresh = next((p for p in current if p["job"] == chosen["job"]), {})
                if fresh.get("decision") != "ready" or fresh.get("node") != chosen["node"]:
                    skipped.add(chosen["job"])
                    continue
                # Temperature/VRAM tie-breaks can reorder healthy GPUs on the
                # freshly probed node. Reserve the freshly validated choice;
                # do not starve a job solely because its old ranking changed.
                launches.append(self._launch(fresh))
                current = self.plan()
            # Reap work that ended during preparation/planning/launch RPCs now,
            # rather than leaving it active until the next long dispatch cycle.
            mark('controller_launch_s')
            self.reconcile(recover_oom=execute, exclude_attempts={p.get('attempt') for p in launches})
            mark('controller_final_reconcile_s')
            final_plan = current
            return {"mode": "execute", "launched": launches[0] if launches else None,
                    "launches": launches, "plan": plan, "final_plan": final_plan,
                    "phase_times": phase_times}
