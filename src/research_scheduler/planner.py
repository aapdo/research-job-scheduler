"""Pure admission planning: priority backfill, DAG, fresh health and reservations."""
import time

from .schema import job_filesystem, node_filesystem
from .store import ACTIVE
from .startup import startup_group
from .build_placement import placement_key, build_pressure
from .draining import epoch_publication_allowed, host_resource_reservations


def gpu_placement_key(chosen, snapshot, held, node):
    """Combine configured node preference with per-GPU scheduler load.

    ``admission_priority`` is configured in 100-point tiers.  One eval consumes
    three admission units (0.3 job) and other GPU work consumes ten.  Scaling
    load by 100 means an occupied preferred GPU loses to an empty GPU in the
    next tier, while an empty preferred GPU still wins.  This prevents a fast
    node from being filled repeatedly before idle lower-tier nodes are used.
    """
    observed={g['uuid']:g for g in snapshot.get('gpus',[])}
    loads=[]
    counts=[]
    for gpu in chosen:
        users=[a for a in held if gpu in a['spec'].get('gpus',[])]
        units=sum(node_job_units(
            a['spec'].get('job_kind',a['spec'].get('job_spec',{}).get('kind')),node)
            for a in users)
        if observed.get(gpu,{}).get('processes') and not users:
            units=max(units,10)
        loads.append(units);counts.append(len(users))
    priority=node.get('admission_priority',0)
    maximum=max(loads,default=0)
    # Host pressure and resource-variant preference remain in placement_key().
    # Keeping node id / chosen UUIDs out of this prefix lets those existing
    # tie-breakers decide genuinely equal GPU-load candidates.
    return (maximum*100-priority,maximum,sum(loads),max(counts,default=0),
            sum(counts),-priority)


def gpu_occupancy_key(chosen, snapshot, held):
    """Backward-compatible occupancy-only key used by audit callers/tests."""
    node = dict(id='', gpus=snapshot.get('gpus', []), admission_priority=0)
    return gpu_placement_key(chosen, snapshot, held, node)


def node_job_units(kind, node):
    """Server admission units: eval=0.3, other work=1 on GPU model nodes."""
    return 3 if node.get('gpus') and kind == 'eval' else 10


def occupied_vram(gpu, users, allow_external):
    """Live unowned memory plus per-attempt max(live owned, reservation)."""
    import math
    reserved=sum(a['spec']['resources']['vram_mib'] for a in users)
    live=gpu['used_mib']
    known={a['id']:a for a in users if a.get('id')}
    owned={key:0.0 for key in known}
    for p in gpu.get('processes',[]):
        if p.get('attempt') not in known:continue
        try:value=float(p['used_mib'])
        except (KeyError,TypeError,ValueError):continue
        if math.isfinite(value) and value>=0:owned[p['attempt']]+=value
    if sum(owned.values())>live:return live+reserved
    if any(owned.values()):
        def reservation(a):
            from .model_vram_policy import default_mib
            measured=owned.get(a.get('id'),0)
            value=a.get('admission_vram_mib')
            original=a['spec']['resources']['vram_mib']
            if default_mib() is not None and type(value) in (int,float) and value>0 and measured>0:
                return value
            return value if type(value) in (int,float) and 0<measured<=value<=original else original
        return live+sum(max(0,reservation(a)-owned.get(a.get('id'),0)) for a in users)
    # Preserve legacy conservative behavior if process ownership is unavailable.
    return max(live,reserved) if users and not allow_external else live+reserved


def base_health(node, snap, now):
    p = node["policy"]
    if not snap or snap.get("error"):
        return "probe unavailable: " + str((snap or {}).get("error", "not probed"))
    if not 0 <= now - snap.get("received_at", 0) <= p["max_snapshot_age_s"]:
        return "stale resource snapshot"
    if snap.get("d_state", 1):
        return ("registered process D-state >=180s; new launches paused" if snap.get('d_state_policy') == 'registered-process-180s-v1'
                else "D-state detected; new launches paused")
    if not snap.get("read_ok", False):
        return "storage read health failed"
    if not node.get('labels', {}).get('ignore_cpu_admission', False) and snap["cpu_percent"] > p["max_cpu_percent"]:
        return "CPU utilization above limit"
    if snap["ram_available_mib"] < p["min_free_ram_mib"]:
        return "insufficient host RAM headroom"
    if snap["disk_free_mib"] < p["min_free_disk_mib"]:
        return "insufficient work filesystem space"
    return ""


def gpu_healthy(gpu, node, mode):
    p = node["policy"]
    if gpu.get("temperature_c") is None or gpu["temperature_c"] >= p.get("max_gpu_temp_c", 85):
        return False
    limit = p.get("max_shared_gpu_percent", p["max_gpu_percent"]) if mode == "shared" else p["max_gpu_percent"]
    if gpu.get("util_percent") is None or gpu["util_percent"] > limit:
        return False
    if mode == "exclusive":
        # Exclusive means one scheduler reservation per GPU. A node may opt in to
        # coexistence with already-visible external processes; actual free VRAM is
        # still checked later with the configured safety margin.
        return ((not gpu.get("processes") and gpu["used_mib"] <= p["max_idle_used_mib"])
                or (bool(gpu.get("processes")) and p["allow_external_gpu_processes"]))
    return p["allow_gpu_sharing"]


def dependency_priorities(jobs, experiments):
    """Inherit descendants' lexicographic campaign/job priority."""
    by_id = {j['id']: j for j in jobs}
    successors = {key: set() for key in by_id}
    for j in jobs:
        if j['status'] != 'queued':
            continue
        frontier = list(j['spec']['depends_on'])
        seen = set()
        while frontier:
            key = frontier.pop()
            if key in seen or key not in by_id:
                continue
            seen.add(key)
            if by_id[key]['status'] == 'succeeded':
                continue
            successors[key].add(j['id'])
            frontier.extend(by_id[key]['spec']['depends_on'])
    priority = {j['id']: (experiments[j['experiment']]['priority'], j['spec']['priority']) for j in jobs}
    return {key: (*max([priority[key], *[priority[d] for d in downstream]]), len(downstream))
            for key, downstream in successors.items()}


def admission_ram(attempt, job, now):
    """Operator-audited reservation only; immutable execution specs are untouched."""
    original = attempt['spec']['resources']['ram_mib']
    item = job.get('spec', {}).get('metadata', {}).get('ram_admission_overrides', {}).get(attempt['id'], {})
    value = item.get('ram_mib')
    report = attempt.get('report', {})
    rss = report.get('rss_mib')
    heartbeat = report.get('heartbeat', 0)
    if (attempt['status'] != 'running' or not item.get('evidence')
            or type(value) not in (int, float) or not 0 < value <= original
            or type(rss) not in (int, float) or not 0 <= rss <= value
            or not 0 <= now - heartbeat <= 120):
        return original
    return value


def admission_vram(attempt, job, now):
    original=attempt['spec']['resources']['vram_mib']
    from .model_vram_policy import reservation
    policy=reservation(job.get('spec',{}))
    if policy is not None and attempt['status']=='running' and attempt.get('report',{}).get('ready') and 0<=now-attempt.get('report',{}).get('heartbeat',0)<=120:
        return policy
    item=job.get('spec',{}).get('metadata',{}).get('vram_admission_overrides',{}).get(attempt['id'],{})
    value=item.get('vram_mib')
    report=attempt.get('report',{})
    if (attempt['status']!='running' or not report.get('ready') or not item.get('evidence')
            or item.get('original_vram_mib')!=original
            or type(value) not in (int,float) or not 0<value<=original
            or not 0<=now-report.get('heartbeat',0)<=120):
        return original
    return value


def lineage_error(key, by_id, latest, lineage_cache):
    """An early artifact never silently follows a producer into a new retry.

    Check ancestors too, so a completed intermediate evaluation cannot let a
    final report combine checkpoints from superseded training attempts.
    """
    if key in lineage_cache:
        return lineage_cache[key]
    spec = by_id[key]['spec']
    from .recovery import frozen_initialization_valid
    frozen = frozen_initialization_valid(by_id[key], latest.get(key))
    if frozen is not None:
        reason = '' if frozen else 'frozen initialization authority invalid: ' + key
        lineage_cache[key] = reason
        return reason
    binding = spec.get('metadata', {}).get('epoch_dependency')
    reason = ''
    if binding:
        source = latest.get(binding['source_job'])
        if source is None or source['id'] != binding['source_attempt']:
            reason = 'checkpoint lineage superseded or unavailable: ' + binding['source_job']
        elif source['status'] not in ('running', 'succeeded'):
            reason = 'checkpoint producer is not running/successful: ' + binding['source_job']
    if not reason:
        for dep in spec['depends_on']:
            reason = lineage_error(dep, by_id, latest, lineage_cache)
            if reason: break
    lineage_cache[key] = reason
    return reason


def placements(jobs, experiments, nodes, snapshots, attempts, groups, now=None):
    """Return simulated placements with reasons; never mutate runtime or launch jobs."""
    now = time.time() if now is None else now
    from .model_vram_policy import normalize
    jobs=[dict(j,spec=normalize(j['spec'])) for j in jobs]
    from .gpu_recovery import quarantine_snapshots
    snapshots = quarantine_snapshots(snapshots, attempts)
    by_id = {j["id"]: j for j in jobs}
    active = [dict(a, admission_ram_mib=admission_ram(a, by_id.get(a.get('job'), {}), now),
                   admission_vram_mib=admission_vram(a, by_id.get(a.get('job'), {}), now))
              if a.get('job') in by_id else a
              for a in attempts if a["status"] in ACTIVE]
    successful = {a["job"]: a for a in attempts if a["status"] == "succeeded"}
    held = list(active)
    plan = []
    latest = {}
    for attempt in sorted(attempts, key=lambda a: a['created']):
        # Artifact-transfer reservations occupy resources but are not producer attempts.
        if 'job' in attempt:
            latest[attempt['job']] = attempt
    lineage_cache = {}

    scores = dependency_priorities(jobs, experiments)
    def validation_expansion(j):
        # Admit runnable consumers before expanding validation to more hosts.
        # Blocked consumers still backfill normally into parallel validation.
        return (j['id'].startswith('EXEC_VERIFY_')
                and experiments[j['experiment']].get('project')=='execution-preparation'
                and j['spec']['kind']=='prepare'
                and j['spec']['resources']['gpu_count']>0)
    queued = sorted((j for j in jobs if j["status"] == "queued"),
                    key=lambda j: (-scores[j['id']][0], -scores[j['id']][1], validation_expansion(j),
                                   -scores[j['id']][2],
                                   j["created"], j["id"]))
    for j in queued:
        spec, failures = j["spec"], {}
        invalid_lineage = lineage_error(j['id'], by_id, latest, lineage_cache)
        if invalid_lineage:
            plan.append({'job': j['id'], 'decision': 'blocked', 'reason': invalid_lineage})
            continue
        waiting = [dep for dep in spec["depends_on"] if by_id[dep]["status"] != "succeeded"]
        if waiting:
            plan.append({"job": j["id"], "decision": "blocked", "reason": "dependencies not successful: " + ", ".join(waiting)})
            continue
        candidates = []
        for node_id, node in sorted(nodes.items()):
            reasons = []
            for index, resources in enumerate([spec['resources'], *spec.get('resource_variants', [])]):
                from .hardware_resources import effective_resources
                resources = effective_resources(spec, node, resources)
                reason, chosen = fit(dict(spec, resources=resources), node, snapshots.get(node_id), held, attempts,
                                     successful, groups, now)
                if reason:
                    reasons.append(reason)
                else:
                    count = sum(a["node"] == node_id for a in held)
                    key = placement_key(node, snapshots[node_id], held, resources, now, count, index, chosen, kind=spec['kind'])
                    if spec.get('metadata', {}).get('prefer_primary_resources'):
                        key = (index, *key)
                    if resources['gpu_count']:
                        key = (*gpu_placement_key(chosen,snapshots[node_id],held,node),*key)
                    candidates.append((key, node_id, chosen, resources))
                    if not resources['gpu_count']:break
            if reasons and len(reasons) == 1 + len(spec.get('resource_variants', [])):
                failures[node_id] = '; '.join(dict.fromkeys(reasons))
        if not candidates:
            plan.append({"job": j["id"], "decision": "waiting", "reasons": failures or {"inventory": "no nodes registered"}})
            continue
        _, node_id, chosen, resources = min(candidates, key=lambda c: c[0])
        placement = {"job": j["id"], "decision": "ready", "node": node_id, "gpus": chosen,
                     "filesystem_request": job_filesystem(spec),
                     "filesystem": node_filesystem(nodes[node_id])}
        if resources.get('build_slots', 0):
            score, pressure = build_pressure(nodes[node_id], snapshots[node_id], held, resources, now)
            placement['build_placement'] = dict(policy='spread-first-v2', score=score, projected_pressure=pressure)
        if spec.get('resource_variants') or resources != spec['resources']:
            placement['resources'] = resources
        if spec.get("dataset"):
            placement.update(dataset=spec["dataset"], dataset_path=nodes[node_id]["datasets"][spec["dataset"]])
        elif spec.get("dataset_path"):
            placement["dataset_path"] = spec["dataset_path"]
        plan.append(placement)
        held.append({"id": "planned:" + j["id"], "job": j["id"], "node": node_id,
                     "created": now, "released": False, "status": "starting",
                     "spec": {"resources": resources, "gpus": chosen, "job_kind": spec['kind'],
                              "node_spec": nodes[node_id],
                              "job_spec": spec,
                              "startup_group": startup_group(spec,nodes[node_id])}})
    for row in plan:
        (row['effective_campaign_priority'], row['effective_job_priority'],
         row['pending_descendants']) = scores[row['job']]
        # Keep the old field during the API transition, but do not use its
        # additive value for ordering.
        row['effective_priority'] = row['effective_job_priority']
    return plan


def fit(job, node, snap, held, history, successful, groups, now):
    from .model_vram_policy import normalize
    job=normalize(job)
    # A partial runtime qualification is not permission for score/statistics jobs.
    capability=node.get('labels',{}).get('execution_capability_limits',{}).get(job.get('dataset'))
    if capability:
        profile=job.get('metadata',{}).get('execution_profiles',{}).get(node['id'],{})
        if (job.get('config',{}).get('mode') not in capability['modes']
                or profile.get('catalog') not in capability['catalogs']):
            return 'execution capability not verified for this operation', []
    if node['id'] in job.get('metadata', {}).get('excluded_hosts', []):
        return 'user excluded host for this job', []
    from .execution_profiles import for_node
    profile=job.get('metadata',{}).get('execution_profiles',{}).get(node['id'])
    if profile and job['resources']!=profile.get('resource_contract'):
        return 'execution-profile resource constraint', []
    original=job.get('metadata',{}).get('execution_original_resources')
    if not profile and original is not None and job['resources'] not in original:
        return 'resource variant requires a verified execution profile', []
    try:
        job=for_node(job,node['id'],readonly=True)
    except ValueError:
        return 'invalid execution profile', []
    p, req = node["policy"], job["resources"]
    runtime_hold = node.get('labels', {}).get('gpu_runtime_quarantine', {})
    if (req['gpu_count'] and runtime_hold.get('boot_id')
            and runtime_hold['boot_id'] == (snap or {}).get('boot_id')):
        return 'CUDA runtime unavailable; awaiting reboot or verified recovery', []
    gpu_limit = node.get('labels', {}).get('max_gpus_per_job')
    diagnostic_limit = node.get('labels', {}).get('diagnostic_gpu_count_overrides', {}).get(job['id'])
    if (diagnostic_limit is not None and job.get('kind') == 'prepare'
            and job.get('metadata', {}).get('diagnostic_only') is True):
        gpu_limit = diagnostic_limit
    if gpu_limit is not None:
        if isinstance(gpu_limit, bool) or not isinstance(gpu_limit, int) or gpu_limit < 1:
            return 'invalid node GPU-per-job limit', []
        if req['gpu_count'] > gpu_limit:
            return 'node GPU-per-job limit: ' + str(gpu_limit), []
    if not node["enabled"] and not epoch_publication_allowed(job, node):
        return "node disabled/drained", []
    if job["hosts"] and node["id"] not in job["hosts"]:
        return "host constraint", []
    mapping=job.get('metadata',{}).get('gpu_count_by_host')
    if mapping is not None and mapping.get(node['id'])!=req['gpu_count']:
        return 'host-specific GPU count constraint', []
    requested_filesystem = job_filesystem(job)
    actual_filesystem = node_filesystem(node)
    if requested_filesystem != "any" and requested_filesystem != actual_filesystem:
        return ("filesystem constraint: requires " + requested_filesystem
                + ", node is " + actual_filesystem), []
    if any(node["labels"].get(k) != v for k, v in job["labels"].items()):
        return "label constraint", []
    dataset = job.get("dataset", "")
    dataset_path = node.get("datasets", {}).get(dataset)
    if dataset and not dataset_path:
        return "dataset path not registered on node: " + dataset, []
    group=startup_group(job,node)
    failed_member=node.get('_blocked_startup_groups',{}).get(group) if group else None
    if failed_member:
        return "shared backend unhealthy/unreachable: " + failed_member, []
    reason = base_health(node, snap, now)
    if reason:
        return reason, []
    if snap.get("stable_polls", 0) < p["stable_polls"]:
        return "waiting for stable health polls", []
    from .resources import availability
    reason=availability(job,node,snap)
    if reason:return reason, []
    if dataset:
        observed = snap.get("datasets", {}).get(dataset, {})
        if observed.get("path") != dataset_path or not observed.get("available"):
            return "dataset path unavailable/unprobed: " + dataset + " (" + dataset_path + ")", []
    for key, sha in job["assets"].items():
        if snap.get("assets", {}).get(key) != sha:
            return "missing/unverified frozen asset: " + key, []
    # Dependency paths can only cross hosts in an explicitly shared storage domain.
    for dep in job["depends_on"]:
        a = successful.get(dep)
        if a is None:
            return "missing successful dependency receipt: " + dep, []
        if dep in job.get("order_only_dependencies", []):
            continue
        if a["node"] != node["id"] and not (node["storage_domain"] and
                a["spec"]["node_spec"]["storage_domain"] == node["storage_domain"]):
            if node['id'] not in a.get('artifact_locations', {}):
                suffix = ' (HF download pending)' if a.get('report', {}).get('hf_artifact') else ''
                return "dependency artifacts on another local filesystem: " + dep + suffix, []
    own = [a for a in held if a["node"] == node["id"]]
    if job['kind']=='eval' and 'max_eval_jobs' in node.get('labels',{}):
        eval_cap=node['labels']['max_eval_jobs']
        if type(eval_cap) is not int or eval_cap<1:
            return 'invalid eval job cap', []
        if sum(a['spec'].get('job_kind')=='eval' for a in own)>=eval_cap:
            return 'node eval job cap reached', []
    domain = node.get('physical_host', node['id'])
    host_held = host_resource_reservations(node, held)
    for token, count in req.get('tokens', {}).items():
        capacity = node.get('tokens', {}).get(token, 0)
        used = sum(a['spec']['resources'].get('tokens', {}).get(token, 0) for a in held)
        if count + used > capacity:
            return 'shared token unavailable: ' + token, []
    if job['kind'] == 'board_test' and job['board_id'] not in node.get('board_locks', {}):
        return 'board gateway not registered on node', []
    build_slots = req.get('build_slots', 0)
    psi_limit = node.get('labels', {}).get('rtl_memory_psi_full_avg10_limit')
    if build_slots and psi_limit is not None:
        psi = snap.get('memory_pressure_full_avg10')
        if not isinstance(psi, (int, float)) or not 0 <= psi < float(psi_limit):
            return 'temporary memory PSI gate; recheck next scheduling cycle', []
    if build_slots and build_slots + sum(a['spec']['resources'].get('build_slots', 0) for a in host_held) > node.get('rtl_build_slots', 0):
        return 'RTL build slots reserved', []
    if build_slots:
        starts = [a['created'] for a in history + held
                  if a['spec'].get('node_spec', {}).get('physical_host', a['node']) == domain
                  and a['spec']['resources'].get('build_slots', 0)]
        if starts and now - max(starts) < 30:
            return 'RTL build startup stagger (30 seconds)', []
    disk_claims = sum(a['spec']['resources'].get('disk_mib', 0) for a in own)
    if req.get('disk_mib', 0) + disk_claims + p['min_free_disk_mib'] > snap['disk_free_mib']:
        return 'disk reservations/headroom exhausted', []
    registered = {g["uuid"]: g for g in node["gpus"] if g["enabled"]
                  and g["uuid"] not in p.get("disabled_gpu_uuids", [])
                  and g["uuid"] not in snap.get("gpu_unavailable_uuids", [])}
    if req["gpu_count"]:
        if not registered:
            return "not enough enabled GPUs", []
        observed = {g["uuid"]: g for g in snap["gpus"] if g["uuid"] in registered}
        if p.get("temperature_scope", "node") == "node" and any(gpu not in observed or observed[gpu].get("temperature_c") is None for gpu in registered):
            return "GPU temperature telemetry unavailable", []
        if p.get("temperature_scope", "node") == "node":
            hottest = max(observed[gpu]["temperature_c"] for gpu in registered)
            if hottest >= p.get("max_gpu_temp_c", 85):
                return "GPU temperature at or above hard launch limit", []
            if hottest >= p.get("warm_gpu_temp_c", 80) and len(own) >= p.get("warm_max_jobs", 1):
                return "warm-node job cap reached", []
    used_job_units = sum(node_job_units(
        a['spec'].get('job_kind', a['spec'].get('job_spec', {}).get('kind')), node)
        for a in own)
    if used_job_units + node_job_units(job['kind'], node) > 10 * node["max_jobs"]:
        return "node job slots reserved", []
    if any(a["status"] == "unknown" for a in host_held):
        return "unknown attempt requires reconciliation", []
    used_cpu = sum(a["spec"]["resources"]["cpu"] for a in host_held)
    # MemAvailable already reflects current RSS. Reserve only each active job's
    # unrealized growth to its declared peak; without a fresh process-tree RSS,
    # fall back to the full reservation.
    def remaining_ram(a):
        requested = a.get('admission_ram_mib', a["spec"]["resources"]["ram_mib"])
        observed = a.get("report", {}).get("rss_mib")
        return requested if not isinstance(observed, (int, float)) else max(0, requested - observed)
    outstanding_ram = sum(remaining_ram(a) for a in host_held)
    cpu_capacity = min(snap["cpu_count"], node.get("cpu_limit", snap["cpu_count"]))
    if not node.get('labels', {}).get('ignore_cpu_admission', False) and req["cpu"] + used_cpu > cpu_capacity:
        return "CPU reservations exhausted", []
    # Deliberately conservative: live available minus reservations. No inferred
    # PID accounting in containers, and no claim that requests are hard limits.
    ram_available = min(snap["ram_available_mib"], node.get("ram_limit_mib", float("inf")))
    if not node.get('labels', {}).get('ignore_ram_reservations', False) and req["ram_mib"] + outstanding_ram + p["min_free_ram_mib"] > ram_available:
        return "RAM reservations/headroom exhausted", []
    if group:
        if group not in groups:
            return "startup group not registered", []
        # Health of all enabled group members is required: a shared backend failure
        # must not send the next cold start to a different machine.
        if any(a["spec"].get("startup_group") == group and not a["released"] for a in held):
            return "shared-storage cold-start slot occupied", []
        starts = [a["created"] for a in history + held if a["spec"].get("startup_group") == group]
        if starts and now - max(starts) < groups[group]["min_start_interval_s"]:
            return "shared-storage start interval", []
    candidates = []
    for gpu in snap["gpus"]:
        if gpu["uuid"] not in registered:
            continue
        if gpu["uuid"] in snap.get('gpu_startup_quarantine', []):
            continue
        # GPU inventory may move to a replacement container while old attempts
        # keep running. UUID reservations remain global across those containers.
        users = [a for a in held if gpu["uuid"] in a["spec"]["gpus"]]
        if (p.get("temperature_scope", "node") == "gpu"
                and gpu.get("temperature_c") is not None
                and gpu["temperature_c"] >= p.get("warm_gpu_temp_c", 80)
                and (len(users) >= p.get("warm_max_jobs", 1) or gpu.get("processes"))):
            continue
        required_polls = (p.get("shared_stable_polls", 1)
                          if req["gpu_mode"] == "shared" and users else p["stable_polls"])
        if gpu.get("stable_polls", 0) < required_polls or not gpu_healthy(gpu, node, req["gpu_mode"]):
            continue
        if req["gpu_mode"] == "exclusive" and users:
            continue
        if any(a["spec"]["resources"]["gpu_mode"] == "exclusive" for a in users):
            continue
        # Do not stack onto a newly reserved job before it proves scientific
        # progress. The next cycle then observes its real utilization and VRAM.
        if users and any(not a.get("report", {}).get("ready", False) for a in users):
            continue
        shared_cap=p.get("max_shared_jobs_per_gpu",2)
        if job['kind']=='eval' and 'max_eval_jobs_per_gpu' in node.get('labels',{}):
            shared_cap=node['labels']['max_eval_jobs_per_gpu']
            if type(shared_cap) is not int or shared_cap<1:
                return 'invalid eval GPU sharing cap', []
            mixed_cap=node['labels'].get('max_mixed_jobs_per_gpu')
            if any(a['spec'].get('job_kind')!='eval' for a in users) and mixed_cap is not None:
                if type(mixed_cap) is not int or mixed_cap<1:
                    return 'invalid mixed GPU sharing cap', []
                shared_cap=min(shared_cap,mixed_cap)
        # Eval=0.3 applies to the server-wide admission budget.  The physical
        # per-GPU concurrency cap remains an absolute number of processes.
        if len(users) >= shared_cap:
            continue
        # A process on a scheduler-reserved GPU is treated as owned for packing.
        # A process with no reservation remains external and needs explicit opt-in.
        if gpu.get("processes") and not users and not p["allow_external_gpu_processes"]:
            continue
        reserved = sum(a["spec"]["resources"]["vram_mib"] for a in users)
        total = min(gpu["memory_mib"], registered[gpu["uuid"]]["memory_mib"])
        # On nodes that disallow external processes, live use and scheduler
        # reservations describe the same occupants; use the larger value instead
        # of double-counting. External-process opt-in retains conservative addition.
        occupied = occupied_vram(gpu, users, p["allow_external_gpu_processes"])
        if req["vram_mib"] + occupied + p["gpu_margin_mib"] > total:
            continue
        candidates.append((bool(users or gpu.get('processes')),len(users), occupied, gpu["temperature_c"], gpu["index"], gpu["uuid"]))
    candidates.sort()
    if req["gpu_count"] > len(candidates):
        return "not enough healthy GPUs with requested per-device VRAM/ownership", []
    return "", [row[-1] for row in candidates[:req["gpu_count"]]]
