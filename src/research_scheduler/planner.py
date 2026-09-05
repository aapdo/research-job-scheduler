"""Pure admission planning: priority backfill, DAG, fresh health and reservations."""
import time

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
    if gpu.get("util_percent") is None or gpu["util_percent"] > p["max_gpu_percent"]:
        return False
    if mode == "exclusive":
        return not gpu.get("processes") and gpu["used_mib"] <= p["max_idle_used_mib"]
    return p["allow_gpu_sharing"]


def placements(jobs, experiments, nodes, snapshots, attempts, groups, now=None):
    """Return simulated placements with reasons; never mutate runtime or launch jobs."""
    now = time.time() if now is None else now
    by_id = {j["id"]: j for j in jobs}
    active = [a for a in attempts if a["status"] in ACTIVE]
    successful = {a["job"]: a for a in attempts if a["status"] == "succeeded"}
    held = list(active)
    plan = []
    queued = sorted((j for j in jobs if j["status"] == "queued"),
                    key=lambda j: (-(experiments[j["experiment"]]["priority"] + j["spec"]["priority"]),
                                   j["created"], j["id"]))
    for j in queued:
        spec, failures = j["spec"], {}
        waiting = [dep for dep in spec["depends_on"] if by_id[dep]["status"] != "succeeded"]
        if waiting:
            plan.append({"job": j["id"], "decision": "blocked", "reason": "dependencies not successful: " + ", ".join(waiting)})
            continue
        candidates = []
        for node_id, node in sorted(nodes.items()):
            reason, chosen = fit(spec, node, snapshots.get(node_id), held, attempts,
                                 successful, groups, now)
            if reason:
                failures[node_id] = reason
            else:
                count = sum(a["node"] == node_id for a in held)
                candidates.append((count, node_id, chosen))
        if not candidates:
            plan.append({"job": j["id"], "decision": "waiting", "reasons": failures or {"inventory": "no nodes registered"}})
            continue
        _, node_id, chosen = min(candidates)
        placement = {"job": j["id"], "decision": "ready", "node": node_id, "gpus": chosen}
        if spec.get("dataset"):
            placement.update(dataset=spec["dataset"], dataset_path=nodes[node_id]["datasets"][spec["dataset"]])
        elif spec.get("dataset_path"):
            placement["dataset_path"] = spec["dataset_path"]
        plan.append(placement)
        held.append({"id": "planned:" + j["id"], "job": j["id"], "node": node_id,
                     "created": now, "released": False, "status": "starting",
                     "spec": {"resources": spec["resources"], "gpus": chosen,
                              "startup_group": nodes[node_id]["startup_group"]}})
    return plan


def fit(job, node, snap, held, history, successful, groups, now):
    p, req = node["policy"], job["resources"]
    if not node["enabled"]:
        return "node disabled/drained", []
    if job["hosts"] and node["id"] not in job["hosts"]:
        return "host constraint", []
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
        if a["node"] != node["id"] and not (node["storage_domain"] and
                a["spec"]["node_spec"]["storage_domain"] == node["storage_domain"]):
            return "dependency artifacts on another local filesystem: " + dep, []
    own = [a for a in held if a["node"] == node["id"]]
    if len(own) >= node["max_jobs"]:
        return "node job slots reserved", []
    if any(a["status"] == "unknown" for a in own):
        return "unknown attempt requires reconciliation", []
    used_cpu = sum(a["spec"]["resources"]["cpu"] for a in own)
    used_ram = sum(a["spec"]["resources"]["ram_mib"] for a in own)
    cpu_capacity = min(snap["cpu_count"], node.get("cpu_limit", snap["cpu_count"]))
    if req["cpu"] + used_cpu > cpu_capacity:
        return "CPU reservations exhausted", []
    # Deliberately conservative: live available minus reservations. No inferred
    # PID accounting in containers, and no claim that requests are hard limits.
    ram_available = min(snap["ram_available_mib"], node.get("ram_limit_mib", float("inf")))
    if req["ram_mib"] + used_ram + p["min_free_ram_mib"] > ram_available:
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
    chosen = []
    registered = {g["uuid"]: g for g in node["gpus"] if g["enabled"]}
    for gpu in sorted(snap["gpus"], key=lambda g: g["index"]):
        if gpu["uuid"] not in registered:
            continue
        if gpu.get("stable_polls", 0) < p["stable_polls"] or not gpu_healthy(gpu, node, req["gpu_mode"]):
            continue
        users = [a for a in own if gpu["uuid"] in a["spec"]["gpus"]]
        if req["gpu_mode"] == "exclusive" and users:
            continue
        if any(a["spec"]["resources"]["gpu_mode"] == "exclusive" for a in users):
            continue
        # External GPU processes cannot be reliably mapped through PID namespaces.
        # Shared mode requires a deliberate separate approval when any are visible.
        if gpu.get("processes") and not p["allow_external_gpu_processes"]:
            continue
        reserved = sum(a["spec"]["resources"]["vram_mib"] for a in users)
        free = min(gpu["memory_mib"], registered[gpu["uuid"]]["memory_mib"]) - gpu["used_mib"]
        if req["vram_mib"] + reserved + p["gpu_margin_mib"] > free:
            continue
        chosen.append(gpu["uuid"])
    if req["gpu_count"] > len(chosen):
        return "not enough healthy GPUs with requested per-device VRAM/ownership", []
    return "", chosen[:req["gpu_count"]]
