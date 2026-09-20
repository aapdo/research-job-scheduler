"""Durable local SQLite registry and audit events; one dispatcher via flock."""
import contextlib
import fcntl
import json
import os
import sqlite3
import time
from pathlib import Path

from .schema import absolute, check, experiment_spec, fields, group_spec, identifier, node_spec, validate_dag

ACTIVE = ("starting", "running", "unknown")


def dumps(value):
    return json.dumps(value, sort_keys=True, ensure_ascii=False, allow_nan=False)


class Store:
    def __init__(self, path):
        self.path = Path(path).resolve()
        self.path.parent.mkdir(parents=True, exist_ok=True)
        # flock/SQLite safety relies on a local control filesystem, not NFS/CIFS.
        mounts = []
        for line in Path("/proc/mounts").read_text().splitlines():
            parts = line.split()
            mount = parts[1].replace("\\040", " ")
            if self.path.is_relative_to(mount):
                mounts.append((len(mount), parts[2]))
        if mounts:
            check(max(mounts)[1] not in ("nfs", "nfs4", "cifs", "smb3"), "SQLite state must be on local disk")
        fd = os.open(self.path, os.O_CREAT | os.O_APPEND | os.O_WRONLY, 0o600)
        os.close(fd)
        os.chmod(self.path, 0o600)
        self.db = sqlite3.connect(self.path, timeout=10)
        self.db.row_factory = sqlite3.Row
        self.db.executescript("""
            PRAGMA foreign_keys=ON;
            PRAGMA journal_mode=WAL;
            CREATE TABLE IF NOT EXISTS nodes(id TEXT PRIMARY KEY, spec TEXT NOT NULL);
            CREATE TABLE IF NOT EXISTS groups_(id TEXT PRIMARY KEY, spec TEXT NOT NULL);
            CREATE TABLE IF NOT EXISTS experiments(id TEXT PRIMARY KEY, spec TEXT NOT NULL);
            CREATE TABLE IF NOT EXISTS jobs(
                id TEXT PRIMARY KEY, experiment TEXT NOT NULL REFERENCES experiments(id),
                spec TEXT NOT NULL, status TEXT NOT NULL, created REAL NOT NULL, reason TEXT NOT NULL DEFAULT '');
            CREATE TABLE IF NOT EXISTS attempts(
                id TEXT PRIMARY KEY, job TEXT NOT NULL REFERENCES jobs(id), node TEXT NOT NULL,
                spec TEXT NOT NULL, status TEXT NOT NULL, created REAL NOT NULL,
                released INTEGER NOT NULL DEFAULT 0, ready_polls INTEGER NOT NULL DEFAULT 0,
                report TEXT NOT NULL DEFAULT '{}');
            CREATE TABLE IF NOT EXISTS snapshots(node TEXT PRIMARY KEY, data TEXT NOT NULL);
            CREATE TABLE IF NOT EXISTS node_health(node TEXT PRIMARY KEY, data TEXT NOT NULL);
            CREATE TABLE IF NOT EXISTS events(seq INTEGER PRIMARY KEY, time REAL, kind TEXT, subject TEXT, data TEXT);
            CREATE TABLE IF NOT EXISTS dataset_catalog(id TEXT PRIMARY KEY, spec TEXT NOT NULL);
            CREATE TABLE IF NOT EXISTS dataset_preparations(
                dataset TEXT NOT NULL, node TEXT NOT NULL, job TEXT NOT NULL, state TEXT NOT NULL,
                receipt TEXT NOT NULL DEFAULT '{}', PRIMARY KEY(dataset,node));
            CREATE TABLE IF NOT EXISTS artifact_transfers(
                id TEXT PRIMARY KEY, attempt TEXT NOT NULL, node TEXT NOT NULL,
                direction TEXT NOT NULL, status TEXT NOT NULL, spec TEXT NOT NULL,
                report TEXT NOT NULL DEFAULT '{}', created REAL NOT NULL);
            CREATE INDEX IF NOT EXISTS attempts_status_job_idx ON attempts(status,job);
            CREATE INDEX IF NOT EXISTS attempts_created_job_idx ON attempts(created,job);
            CREATE INDEX IF NOT EXISTS artifact_transfers_attempt_status_created_idx
                ON artifact_transfers(attempt,status,created);
        """)
        os.chmod(self.path, 0o600)

    @contextlib.contextmanager
    def lock(self, timeout=None, poll_interval=0.1):
        """Exclusive registry lock; dispatchers fail fast, control jobs may wait.

        The timeout covers lock acquisition only. Never wait while holding a
        SQLite transaction: callers use ``with store.lock(...), store.db``.
        """
        from .schema import number
        timeout = getattr(self, 'lock_wait_s', 0) if timeout is None else timeout
        number(timeout, 'lock timeout', 0)
        number(poll_interval, 'lock poll interval', 0)
        check(poll_interval > 0, 'lock poll interval must be positive')
        deadline = time.monotonic() + timeout
        with self.path.with_suffix(self.path.suffix + ".lock").open("a") as f:
            while True:
                try:
                    fcntl.flock(f, fcntl.LOCK_EX | fcntl.LOCK_NB)
                    break
                except BlockingIOError as exc:
                    remaining = deadline - time.monotonic()
                    if remaining <= 0:
                        raise RuntimeError("another controller/registry operation holds the scheduler lock") from exc
                    time.sleep(min(poll_interval, remaining))
            yield

    def event(self, kind, subject, data):
        self.db.execute("INSERT INTO events(time,kind,subject,data) VALUES(?,?,?,?)",
                        (time.time(), kind, subject, dumps(data)))

    def specs(self, table):
        check(table in ("nodes", "groups_", "experiments"), "invalid table")
        return {r["id"]: json.loads(r["spec"]) for r in self.db.execute("SELECT * FROM " + table)}

    def register_node(self, raw):
        n = node_spec(raw)
        with self.lock(), self.db:
            check(not self.db.execute("SELECT 1 FROM attempts WHERE node=? AND status IN (?,?,?)",
                                      (n["id"], *ACTIVE)).fetchone(), "cannot alter a node with active/unknown attempts")
            # One inventory entry per physical UUID prevents alias double-booking.
            for other in self.specs("nodes").values():
                if other["id"] != n["id"]:
                    check(not set(n.get('board_locks', {})) & set(other.get('board_locks', {})),
                          'board already has a gateway; all tunnel clients must use the same gateway')
                    for token in set(n.get('tokens', {})) & set(other.get('tokens', {})):
                        check(n['tokens'][token] == other['tokens'][token], 'shared token capacities disagree: ' + token)
                    if n.get('physical_host') and n.get('physical_host') == other.get('physical_host'):
                        for key in ('cpu_limit', 'ram_limit_mib'):
                            check(n.get(key) == other.get(key), 'physical-host limits disagree: ' + key)
                        if n.get('rtl_build_slots') and other.get('rtl_build_slots'):
                            check(n['rtl_build_slots'] == other['rtl_build_slots'], 'physical-host build limits disagree')
                    check(not {g["uuid"] for g in n["gpus"]} & {g["uuid"] for g in other["gpus"]},
                          "GPU UUID already registered under another node")
                    if n["transport"] == other["transport"]:
                        check(n.get("target", "local") != other.get("target", "local"), "duplicate transport target")
            self.db.execute("INSERT OR REPLACE INTO nodes VALUES(?,?)", (n["id"], dumps(n)))
            self.db.execute("DELETE FROM snapshots WHERE node=?", (n["id"],))
            self.event("node_registered", n["id"], n)
        return n

    def register_group(self, raw):
        g = group_spec(raw)
        with self.lock(), self.db:
            check(not any(a["spec"].get("startup_group") == g["id"] for a in self.attempts(active=True)),
                  "cannot alter an active startup group")
            self.db.execute("INSERT OR REPLACE INTO groups_ VALUES(?,?)", (g["id"], dumps(g)))
            self.event("group_registered", g["id"], g)
        return g

    def set_dataset(self, node_id, name, path):
        """Update future placement only; live attempts keep their frozen mapping."""
        identifier(name)
        absolute(path)
        with self.lock(), self.db:
            n = self.specs("nodes").get(node_id)
            check(n is not None, "unknown node: " + node_id)
            n.setdefault("datasets", {})[name] = path
            n = node_spec(n)
            self.db.execute("UPDATE nodes SET spec=? WHERE id=?", (dumps(n), node_id))
            self.db.execute("DELETE FROM snapshots WHERE node=?", (node_id,))
            self.event("dataset_path_registered", node_id, {"dataset": name, "path": path})
        return {"node": node_id, "dataset": name, "path": path}

    def set_storage_profile(self, node_id, raw):
        """Change only future storage admission while attempts keep frozen specs."""
        fields(raw, "filesystem work_root storage_domain startup_group datasets assets "
               "read_probe_path read_probe_bytes enabled max_jobs")
        with self.lock(), self.db:
            current = self.specs("nodes").get(node_id)
            check(current is not None, "unknown node: " + node_id)
            candidate = json.loads(dumps(current))
            for key in ("filesystem", "work_root", "storage_domain", "startup_group",
                        "datasets", "assets", "enabled", "max_jobs"):
                if key in raw:
                    candidate[key] = raw[key]
            policy = dict(candidate["policy"])
            if "read_probe_path" in raw:
                if raw["read_probe_path"]:
                    policy["read_probe_path"] = raw["read_probe_path"]
                else:
                    policy.pop("read_probe_path", None)
            if "read_probe_bytes" in raw:
                policy["read_probe_bytes"] = raw["read_probe_bytes"]
            candidate["policy"] = policy
            candidate = node_spec(candidate)
            if candidate == current:
                return {"node": node_id, "changed": False, "active_attempts_unchanged": True}
            self.db.execute("UPDATE nodes SET spec=? WHERE id=?", (dumps(candidate), node_id))
            self.db.execute("DELETE FROM snapshots WHERE node=?", (node_id,))
            data = {"changed": True, "active_attempts_unchanged": True,
                    "filesystem": candidate["filesystem"], "work_root": candidate["work_root"],
                    "storage_domain": candidate["storage_domain"],
                    "startup_group": candidate["startup_group"], "enabled": candidate["enabled"],
                    "max_jobs": candidate["max_jobs"]}
            self.event("node_storage_profile_changed", node_id, data)
        return {"node": node_id, **data}

    def set_gpu_enabled(self, node_id, gpu_uuid, enabled):
        """Change future admission without mutating an active attempt snapshot."""
        check(isinstance(enabled, bool), "enabled must be boolean")
        with self.lock(), self.db:
            n = self.specs("nodes").get(node_id)
            check(n is not None, "unknown node: " + node_id)
            gpu = next((g for g in n["gpus"] if g["uuid"] == gpu_uuid), None)
            check(gpu is not None, "unknown GPU UUID on node: " + gpu_uuid)
            if gpu["enabled"] == enabled:
                return {"node": node_id, "gpu": gpu_uuid, "enabled": enabled, "changed": False}
            gpu["enabled"] = enabled
            self.db.execute("UPDATE nodes SET spec=? WHERE id=?", (dumps(node_spec(n)), node_id))
            self.db.execute("DELETE FROM snapshots WHERE node=?", (node_id,))
            data = {"gpu": gpu_uuid, "enabled": enabled, "active_attempts_unchanged": True}
            self.event("gpu_enabled_changed", node_id, data)
        return {"node": node_id, "changed": True, **data}

    def set_external_gpu_processes_allowed(self, node_id, enabled):
        """Opt one node into/out of external-process VRAM headroom admission."""
        check(isinstance(enabled, bool), "enabled must be boolean")
        with self.lock(), self.db:
            n = self.specs("nodes").get(node_id)
            check(n is not None, "unknown node: " + node_id)
            if n["policy"]["allow_external_gpu_processes"] == enabled:
                return {"node": node_id, "enabled": enabled, "changed": False}
            n["policy"]["allow_external_gpu_processes"] = enabled
            self.db.execute("UPDATE nodes SET spec=? WHERE id=?", (dumps(node_spec(n)), node_id))
            self.db.execute("DELETE FROM snapshots WHERE node=?", (node_id,))
            data = {"enabled": enabled, "active_attempts_unchanged": True}
            self.event("external_gpu_process_admission_changed", node_id, data)
        return {"node": node_id, "changed": True, **data}

    def set_min_free_disk_mib(self, node_id, value):
        """Change the future local-disk admission floor without touching live attempts."""
        from .schema import number
        number(value, "min_free_disk_mib", 1, True)
        with self.lock(), self.db:
            n = self.specs("nodes").get(node_id)
            check(n is not None, "unknown node: " + node_id)
            before = n["policy"].get("min_free_disk_mib")
            if before == value:
                return {"node": node_id, "min_free_disk_mib": value, "changed": False}
            n["policy"]["min_free_disk_mib"] = value
            self.db.execute("UPDATE nodes SET spec=? WHERE id=?", (dumps(node_spec(n)), node_id))
            data = {"old_min_free_disk_mib": before, "min_free_disk_mib": value,
                    "active_attempts_unchanged": True}
            self.event("min_free_disk_policy_changed", node_id, data)
        return {"node": node_id, "changed": True, **data}

    def set_gpu_packing(self, node_id, enabled, max_shared_jobs_per_gpu=2):
        """Configure future scheduler-owned sharing; active attempts stay frozen."""
        from .schema import number
        check(isinstance(enabled, bool), "enabled must be boolean")
        number(max_shared_jobs_per_gpu, "max_shared_jobs_per_gpu", 1, True)
        with self.lock(), self.db:
            n = self.specs("nodes").get(node_id)
            check(n is not None, "unknown node: " + node_id)
            changed = (n["policy"].get("allow_gpu_sharing", False) != enabled
                       or n["policy"].get("max_shared_jobs_per_gpu", 2) != max_shared_jobs_per_gpu)
            if not changed:
                return {"node": node_id, "enabled": enabled,
                        "max_shared_jobs_per_gpu": max_shared_jobs_per_gpu, "changed": False}
            n["policy"].update(allow_gpu_sharing=enabled,
                               max_shared_jobs_per_gpu=max_shared_jobs_per_gpu)
            self.db.execute("UPDATE nodes SET spec=? WHERE id=?", (dumps(node_spec(n)), node_id))
            self.db.execute("DELETE FROM snapshots WHERE node=?", (node_id,))
            data = {"enabled": enabled, "max_shared_jobs_per_gpu": max_shared_jobs_per_gpu,
                    "active_attempts_unchanged": True}
            self.event("gpu_packing_changed", node_id, data)
        return {"node": node_id, "changed": True, **data}

    def set_temperature_policy(self, node_id, warm_c=80, hard_c=85, warm_max_jobs=1):
        """Set future launch limits; this never stops an active attempt."""
        from .schema import number
        for key, value in (("warm_c", warm_c), ("hard_c", hard_c)):
            number(value, key)
        number(warm_max_jobs, "warm_max_jobs", 1, True)
        check(warm_c < hard_c, "warm_c must be below hard_c")
        with self.lock(), self.db:
            n = self.specs("nodes").get(node_id)
            check(n is not None, "unknown node: " + node_id)
            values = {"warm_gpu_temp_c": warm_c, "max_gpu_temp_c": hard_c,
                      "warm_max_jobs": warm_max_jobs}
            changed = any(n["policy"].get(key) != value for key, value in values.items())
            if not changed:
                return {"node": node_id, "changed": False, **values}
            n["policy"].update(values)
            self.db.execute("UPDATE nodes SET spec=? WHERE id=?", (dumps(node_spec(n)), node_id))
            self.db.execute("DELETE FROM snapshots WHERE node=?", (node_id,))
            self.event("temperature_policy_changed", node_id,
                       {**values, "active_attempts_unchanged": True})
        return {"node": node_id, "changed": True, **values,
                "active_attempts_unchanged": True}

    def set_pending_gpu_mode(self, job_id, mode):
        """Change packing mode only for an unstarted/queued job."""
        check(mode in ("exclusive", "shared"), "invalid gpu_mode")
        with self.lock(), self.db:
            job = next((j for j in self.jobs() if j["id"] == job_id), None)
            check(job is not None and job["status"] == "queued", "only queued jobs can change gpu_mode")
            old = job["spec"]["resources"]["gpu_mode"]
            if old == mode:
                return {"job": job_id, "mode": mode, "changed": False}
            job["spec"]["resources"]["gpu_mode"] = mode
            self.db.execute("UPDATE jobs SET spec=? WHERE id=?", (dumps(job["spec"]), job_id))
            self.event("pending_gpu_mode_changed", job_id, {"before": old, "after": mode})
        return {"job": job_id, "mode": mode, "changed": True}

    def replace_pending_path_prefix(self, job_id, old_prefix, new_prefix):
        """Rebind an unstarted job to an equivalent immutable release tree."""
        absolute(old_prefix)
        absolute(new_prefix)
        check(old_prefix != new_prefix, "path prefixes must differ")

        def replace(value):
            if isinstance(value, str):
                return new_prefix + value[len(old_prefix):] if (
                    value == old_prefix or value.startswith(old_prefix + "/")) else value
            if isinstance(value, list):
                return [replace(v) for v in value]
            if isinstance(value, dict):
                return {k: replace(v) for k, v in value.items()}
            return value

        with self.lock(), self.db:
            job = next((j for j in self.jobs() if j["id"] == job_id), None)
            check(job is not None and job["status"] == "queued", "only queued jobs can change release path")
            spec = dict(job["spec"])
            for key in ("argv", "cwd", "env", "config", "input_files"):
                spec[key] = replace(spec[key])
            spec = experiment_spec({"id": "path-check", "name": "path-check", "rq": "path-check",
                                    "jobs": [spec]})["jobs"][0]
            if spec == job["spec"]:
                return {"job": job_id, "changed": False, "old_prefix": old_prefix,
                        "new_prefix": new_prefix}
            self.db.execute("UPDATE jobs SET spec=? WHERE id=?", (dumps(spec), job_id))
            data = {"old_prefix": old_prefix, "new_prefix": new_prefix}
            self.event("pending_path_prefix_changed", job_id, data)
        return {"job": job_id, "changed": True, **data}

    def set_gpu_margin_mib(self, node_id, margin_mib):
        """Change future per-GPU safety headroom without touching active attempts."""
        from .schema import number
        number(margin_mib, "gpu_margin_mib")
        with self.lock(), self.db:
            n = self.specs("nodes").get(node_id)
            check(n is not None, "unknown node: " + node_id)
            if n["policy"]["gpu_margin_mib"] == margin_mib:
                return {"node": node_id, "gpu_margin_mib": margin_mib, "changed": False}
            n["policy"]["gpu_margin_mib"] = margin_mib
            self.db.execute("UPDATE nodes SET spec=? WHERE id=?", (dumps(node_spec(n)), node_id))
            self.db.execute("DELETE FROM snapshots WHERE node=?", (node_id,))
            data = {"gpu_margin_mib": margin_mib, "active_attempts_unchanged": True}
            self.event("gpu_margin_changed", node_id, data)
        return {"node": node_id, "changed": True, **data}

    def register_experiment(self, raw):
        e = experiment_spec(raw)
        with self.lock(), self.db:
            existing = self.specs("experiments").get(e["id"])
            if existing is not None and experiment_spec(existing) == e:
                return e  # idempotent registration, never resets completed jobs
            from .model_priority import priority_for
            e['priority'] = priority_for(e)
            from .report_placement import on_archive_host
            report_node = self.specs('nodes').get('lab4')
            if report_node is not None:
                e['jobs'] = [on_archive_host(job, report_node) for job in e['jobs']]
            if existing is not None and experiment_spec(existing) == e:
                return e
            check(existing is None, "experiment already exists; use a new revision ID")
            from .report_placement import validate_report_policy
            for job in e["jobs"]:
                validate_report_policy(job)
            jobs = {j["id"]: j["spec"] for j in self.jobs()}
            for j in e["jobs"]:
                check(j["id"] not in jobs, "job IDs are globally unique: " + j["id"])
                jobs[j["id"]] = j
            validate_dag(jobs)
            self.db.execute("INSERT INTO experiments VALUES(?,?)", (e["id"], dumps(e)))
            for j in e["jobs"]:
                self.db.execute("INSERT INTO jobs(id,experiment,spec,status,created) VALUES(?,?,?,?,?)",
                                (j["id"], e["id"], dumps(j), "queued", time.time()))
            self.event("experiment_registered", e["id"], e)
        return e

    def jobs(self):
        return [dict(r, spec=json.loads(r["spec"])) for r in self.db.execute("SELECT * FROM jobs")]

    def observation_attempts(self):
        """Fresh campaign fields without decoding immutable launch requests."""
        return [dict(r, report=json.loads(r['report'])) for r in self.db.execute(
            'SELECT id,job,node,status,created,released,ready_polls,report FROM attempts')]

    def attempts(self, active=False, *, job_ids=None, summary=False, planning=False):
        # Scheduling needs resource/lineage fields, not a duplicate of every
        # other job in the frozen campaign. Full execution/audit reads remain
        # the default; never submit summary specs to a launch/validation RPC.
        check(not (summary and planning), 'choose summary or planning attempt projection')
        if planning:
            columns = "id,job,node,status,created,released,ready_polls,report"
        else:
            columns = (("id,job,node,status,created,released,ready_polls,report,"
                        "json_remove(spec,'$.experiment_spec') AS spec") if summary else '*')
        conditions, parameters = [], []
        if active:
            conditions.append("status IN ('starting','running','unknown')")
        if job_ids is not None:
            parameters = list(job_ids)
            if not parameters: return []
            conditions.append('job IN ('+','.join('?' for _ in parameters)+')')
        query = 'SELECT '+columns+' FROM attempts'+(' WHERE '+' AND '.join(conditions) if conditions else '')
        result=[]
        planning_cache = getattr(self, '_planning_spec_cache', None)
        if planning and planning_cache is None:
            planning_cache = self._planning_spec_cache = {}
        missing = []
        for r in self.db.execute(query, parameters):
            item=dict(r);item['report']=json.loads(item['report'])
            if planning:
                if item['id'] not in planning_cache:
                    missing.append(item['id'])
            else:item['spec']=json.loads(item['spec'])
            result.append(item)
        if planning and missing:
            # Attempt specs are frozen at reservation. Parse each large original
            # only once per manager lifetime; report/health remain fresh reads.
            for offset in range(0,len(missing),900):
                batch=missing[offset:offset+900]
                projection=("SELECT id,json_extract(spec,'$.resources','$.gpus',"
                            "'$.startup_group','$.node_spec.storage_domain',"
                            "'$.node_spec.physical_host','$.job_spec.kind') AS value "
                            "FROM attempts WHERE id IN ("+','.join('?' for _ in batch)+")")
                for row in self.db.execute(projection,batch):
                    resources,gpus,startup_group,storage_domain,physical_host,job_kind=json.loads(row['value'])
                    node_spec_summary={'storage_domain':storage_domain or ''}
                    if physical_host is not None:node_spec_summary['physical_host']=physical_host
                    planning_cache[row['id']]=dict(resources=resources,gpus=gpus or [],
                                                   startup_group=startup_group or '',
                                                   node_spec=node_spec_summary,job_kind=job_kind)
        if planning:
            for item in result:
                item['spec']=dict(planning_cache[item['id']])
        by_id = {a['id']: a for a in result}
        if not by_id:
            return result
        # Active and planning reads usually contain only a small subset of the
        # historical attempts. Do not decode every old transfer receipt merely
        # to discard it after the lookup below.
        if len(by_id) <= 900:
            ids = list(by_id)
            transfer_query = ("SELECT attempt,node,direction,report FROM artifact_transfers "
                              "WHERE status='succeeded' AND attempt IN ("
                              + ','.join('?' for _ in ids) + ") ORDER BY created")
        else:
            ids = []
            transfer_query = ("SELECT attempt,node,direction,report FROM artifact_transfers "
                              "WHERE status='succeeded' ORDER BY created")
        transfer_rows = list(self.db.execute(transfer_query, ids))
        # Publication manifests can be newer and more complete than an early
        # speculative local relay. Load them first regardless of creation order.
        for row in transfer_rows:
            attempt = by_id.get(row['attempt'])
            if attempt is None or attempt['status'] != 'succeeded':
                continue
            receipt = json.loads(row['report']).get('artifact')
            if not receipt: continue
            if row['direction'] == 'upload':
                attempt['report']['hf_artifact'] = receipt
        for row in transfer_rows:
            if row['direction'] not in ('download', 'archive'): continue
            attempt = by_id.get(row['attempt'])
            if attempt is None or attempt['status'] != 'succeeded': continue
            transfer_report = json.loads(row['report'])
            receipt = transfer_report.get('artifact')
            if not receipt: continue
            report=attempt['report']
            job_spec=attempt.get('spec',{}).get('job_spec',{})
            declared=job_spec.get('dependency_artifacts',job_spec.get('hf_artifacts',[]))
            published=report.get('hf_artifact',{})
            if declared and not report.get('dependency_artifacts') and published.get('attempt') != attempt['id']:
                continue
            required=(report.get('dependency_artifacts') or
                      report.get('hf_artifact',{}).get('files') or report.get('outputs',{}))
            # Old output-only relays must not shadow a complete checkpoint
            # manifest published later by the same immutable attempt.
            if not set(required).issubset(receipt.get('files',{})): continue
            if row['direction'] == 'archive' and any((receipt['files'][name].get('sha256'), receipt['files'][name].get('bytes')) !=
                   (value.get('sha256'), value.get('bytes')) for name, value in required.items()):
                continue
            attempt.setdefault('artifact_locations', {})[row['node']] = receipt
            if row['direction'] == 'archive' and (transfer_report.get('origin_retired')
                    or transfer_report.get('source_cleanup', {}).get('deleted')):
                attempt['origin_retired'] = True
        return result

    def prioritize(self, job_id, priority):
        from .schema import number
        number(priority, "priority", 0, True)
        with self.lock(), self.db:
            j = next((j for j in self.jobs() if j["id"] == job_id), None)
            check(j is not None and j["status"] == "queued", "only queued job priorities can change")
            j["spec"]["priority"] = priority
            self.db.execute("UPDATE jobs SET spec=? WHERE id=?", (dumps(j["spec"]), job_id))
            self.event("priority_changed", job_id, {"priority": priority})

    def set_pending_resources(self, job_id, resources):
        """Explicitly amend future placement; active attempt specs never change."""
        with self.lock(), self.db:
            job = next((j for j in self.jobs() if j['id'] == job_id), None)
            check(job is not None and job['status'] == 'queued', 'only queued jobs can change resources')
            spec = dict(job['spec'], resources=resources)
            spec = experiment_spec(dict(id='resource-check', name='resource-check', rq='resource-check', jobs=[spec]))['jobs'][0]
            before = job['spec']['resources']
            self.db.execute('UPDATE jobs SET spec=? WHERE id=?', (dumps(spec), job_id))
            self.event('pending_resources_changed', job_id, dict(before=before, after=spec['resources']))
        return dict(job=job_id, before=before, after=spec['resources'])

    def set_pending_resource_variants(self, job_id, variants):
        """Explicit opt-in only; the research worker must preserve its batch contract."""
        with self.lock(), self.db:
            job = next((j for j in self.jobs() if j['id'] == job_id), None)
            check(job is not None and job['status'] == 'queued', 'only queued jobs can gain resource variants')
            spec = dict(job['spec'], resource_variants=variants)
            spec = experiment_spec(dict(id='variant-check', name='variant-check', rq='variant-check', jobs=[spec]))['jobs'][0]
            self.db.execute('UPDATE jobs SET spec=? WHERE id=?', (dumps(spec), job_id))
            self.event('pending_resource_variants_changed', job_id,
                       dict(before=job['spec'].get('resource_variants', []), after=spec['resource_variants']))
        return dict(job=job_id, resource_variants=spec['resource_variants'])

    def add_pending_order_dependency(self, job_id, dependency):
        """Add an explicit cross-storage validation gate to an unstarted job."""
        with self.lock(), self.db:
            jobs = {j['id']: j for j in self.jobs()}
            job = jobs.get(job_id)
            check(job is not None and job['status']=='queued', 'only queued jobs can gain validation gates')
            check(dependency in jobs and dependency not in job['spec']['depends_on'], 'missing or existing dependency')
            spec = dict(job['spec'], depends_on=job['spec']['depends_on']+[dependency],
                        order_only_dependencies=job['spec'].get('order_only_dependencies',[])+[dependency])
            spec = experiment_spec(dict(id='gate-check', name='gate-check', rq='gate-check',jobs=[spec]))['jobs'][0]
            validate_dag({k: spec if k==job_id else j['spec'] for k,j in jobs.items()})
            self.db.execute('UPDATE jobs SET spec=? WHERE id=?', (dumps(spec),job_id))
            self.event('pending_validation_gate_added', job_id, dict(dependency=dependency))
        return dict(job=job_id, dependency=dependency)

    def requeue_dependency_blocked(self, job_id):
        """Recover only unstarted jobs blocked by the finite driver's failed DAG."""
        with self.lock(), self.db:
            jobs = {j['id']: j for j in self.jobs()}
            job = jobs.get(job_id)
            check(job is not None and job['status'] == 'blocked', 'job is not blocked')
            check(job['reason'] == 'upstream failed; no evaluation result', 'not a dependency-only block')
            check(not self.db.execute('SELECT 1 FROM attempts WHERE job=?', (job_id,)).fetchone(), 'started attempts require separate recovery')
            check(all(jobs[d]['status'] not in {'failed','blocked','cancelled'} for d in job['spec']['depends_on']), 'upstream failure not recovered')
            self.db.execute("UPDATE jobs SET status='queued',reason='' WHERE id=?", (job_id,))
            self.event('dependency_block_requeued', job_id, dict(depends_on=job['spec']['depends_on']))
        return dict(job=job_id, status='queued')

    def retry_failed(self, job_id, additional_attempts=1):
        """Explicitly add retry budget to a failed job; old attempts stay terminal."""
        from .schema import number
        number(additional_attempts, "additional_attempts", 1, True)
        with self.lock(), self.db:
            job = next((j for j in self.jobs() if j["id"] == job_id), None)
            check(job is not None and job["status"] == "failed", "only failed jobs can be retried")
            attempts = self.db.execute("SELECT COUNT(*) FROM attempts WHERE job=?", (job_id,)).fetchone()[0]
            old_limit = job["spec"]["max_attempts"]
            job["spec"]["max_attempts"] = max(old_limit, attempts) + additional_attempts
            self.db.execute("UPDATE jobs SET spec=?,status='queued',reason='' WHERE id=?",
                            (dumps(job["spec"]), job_id))
            data = {"previous_max_attempts": old_limit,
                    "max_attempts": job["spec"]["max_attempts"],
                    "previous_attempts": attempts, "additional_attempts": additional_attempts}
            self.event("failed_job_requeued", job_id, data)
        return {"job": job_id, "status": "queued", **data}
