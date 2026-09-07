"""Pure admission planning: priority backfill, DAG, fresh health and reservations."""
import time

from .schema import job_filesystem, node_filesystem
from .store import ACTIVE


def base_health(node, snap, now):
    p = node["policy"]
    if not snap or snap.get("error"):
        return "probe unavailable: " + str((snap or {}).get("error", "not probed"))
    if not 0 <= now - snap.get("received_at", 0) <= p["max_snapshot_age_s"]:
        return "stale resource snapshot"
    if snap.get("d_state", 1):
        return "D-state detected; new launches paused"
    if not snap.get("read_ok", False):
        return "storage read health failed"
    if snap["cpu_percent"] > p["max_cpu_percent"]:
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
    """Inherit pending descendants' priority; use their count to break ties."""
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
    priority = {j['id']: experiments[j['experiment']]['priority'] + j['spec']['priority'] for j in jobs}
    return {key: (max([priority[key], *[priority[d] for d in downstream]]), len(downstream))
            for key, downstream in successors.items()}


def placements(jobs, experiments, nodes, snapshots, attempts, groups, now=None):
    """Return simulated placements with reasons; never mutate runtime or launch jobs."""
    now = time.time() if now is None else now
    by_id = {j["id"]: j for j in jobs}
    active = [a for a in attempts if a["status"] in ACTIVE]
    successful = {a["job"]: a for a in attempts if a["status"] == "succeeded"}
    held = list(active)
    plan = []
    latest = {}
    for attempt in sorted(attempts, key=lambda a: a['created']):
        # Artifact-transfer reservations occupy resources but are not producer attempts.
        if 'job' in attempt:
            latest[attempt['job']] = attempt
    lineage_cache = {}

    def lineage_error(key):
        """An early artifact never silently follows a producer into a new retry.

        Check ancestors too, so a completed intermediate evaluation cannot let a
        final report combine checkpoints from superseded training attempts.
        """
        if key in lineage_cache:
            return lineage_cache[key]
        spec = by_id[key]['spec']
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
                reason = lineage_error(dep)
                if reason: break
        lineage_cache[key] = reason
        return reason
    scores = dependency_priorities(jobs, experiments)
    queued = sorted((j for j in jobs if j["status"] == "queued"),
                    key=lambda j: (-scores[j['id']][0], -scores[j['id']][1],
                                   j["created"], j["id"]))
    for j in queued:
        spec, failures = j["spec"], {}
        invalid_lineage = lineage_error(j['id'])
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
                reason, chosen = fit(dict(spec, resources=resources), node, snapshots.get(node_id), held, attempts,
                                     successful, groups, now)
                if reason:
                    reasons.append(reason)
                else:
                    count = sum(a["node"] == node_id for a in held)
                    candidates.append((-node.get("admission_priority", 0), count, index, node_id, chosen, resources))
                    break
            if reasons and len(reasons) == 1 + len(spec.get('resource_variants', [])):
                failures[node_id] = '; '.join(dict.fromkeys(reasons))
        if not candidates:
            plan.append({"job": j["id"], "decision": "waiting", "reasons": failures or {"inventory": "no nodes registered"}})
            continue
        _, _, _, node_id, chosen, resources = min(candidates, key=lambda c: c[:5])
        placement = {"job": j["id"], "decision": "ready", "node": node_id, "gpus": chosen,
                     "filesystem_request": job_filesystem(spec),
                     "filesystem": node_filesystem(nodes[node_id])}
        if spec.get('resource_variants'):
            placement['resources'] = resources
        if spec.get("dataset"):
            placement.update(dataset=spec["dataset"], dataset_path=nodes[node_id]["datasets"][spec["dataset"]])
        elif spec.get("dataset_path"):
            placement["dataset_path"] = spec["dataset_path"]
        plan.append(placement)
        held.append({"id": "planned:" + j["id"], "job": j["id"], "node": node_id,
                     "created": now, "released": False, "status": "starting",
                     "spec": {"resources": resources, "gpus": chosen,
                              "startup_group": nodes[node_id]["startup_group"]}})
    for row in plan:
        row['effective_priority'], row['pending_descendants'] = scores[row['job']]
    return plan


def fit(job, node, snap, held, history, successful, groups, now):
    p, req = node["policy"], job["resources"]
    if not node["enabled"]:
        return "node disabled/drained", []
    if job["hosts"] and node["id"] not in job["hosts"]:
        return "host constraint", []
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
    reason = base_health(node, snap, now)
    if reason:
        return reason, []
    if snap.get("stable_polls", 0) < p["stable_polls"]:
        return "waiting for stable health polls", []
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
    registered = {g["uuid"]: g for g in node["gpus"] if g["enabled"]
                  and g["uuid"] not in p.get("disabled_gpu_uuids", [])}
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
    if len(own) >= node["max_jobs"]:
        return "node job slots reserved", []
    if any(a["status"] == "unknown" for a in own):
        return "unknown attempt requires reconciliation", []
    used_cpu = sum(a["spec"]["resources"]["cpu"] for a in own)
    # MemAvailable already reflects current RSS. Reserve only each active job's
    # unrealized growth to its declared peak; without a fresh process-tree RSS,
    # fall back to the full reservation.
    def remaining_ram(a):
        requested = a["spec"]["resources"]["ram_mib"]
        observed = a.get("report", {}).get("rss_mib")
        return requested if not isinstance(observed, (int, float)) else max(0, requested - observed)
    outstanding_ram = sum(remaining_ram(a) for a in own)
    cpu_capacity = min(snap["cpu_count"], node.get("cpu_limit", snap["cpu_count"]))
    if req["cpu"] + used_cpu > cpu_capacity:
        return "CPU reservations exhausted", []
    # Deliberately conservative: live available minus reservations. No inferred
    # PID accounting in containers, and no claim that requests are hard limits.
    ram_available = min(snap["ram_available_mib"], node.get("ram_limit_mib", float("inf")))
    if req["ram_mib"] + outstanding_ram + p["min_free_ram_mib"] > ram_available:
        return "RAM reservations/headroom exhausted", []
    group = node["startup_group"]
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
        users = [a for a in own if gpu["uuid"] in a["spec"]["gpus"]]
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
        if len(users) >= p.get("max_shared_jobs_per_gpu", 2):
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
        occupied = (max(gpu["used_mib"], reserved) if users and not p["allow_external_gpu_processes"]
                    else gpu["used_mib"] + reserved)
        if req["vram_mib"] + occupied + p["gpu_margin_mib"] > total:
            continue
        candidates.append((len(users), occupied, gpu["temperature_c"], gpu["index"], gpu["uuid"]))
    candidates.sort()
    if req["gpu_count"] > len(candidates):
        return "not enough healthy GPUs with requested per-device VRAM/ownership", []
    return "", [row[-1] for row in candidates[:req["gpu_count"]]]
