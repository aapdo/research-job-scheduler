"""Strict JSON registration contracts. Sizes are MiB; GPU memory is per device."""
import copy
import json
import math
import re
from pathlib import PurePosixPath


FILESYSTEMS = ("local", "nfs")
FILESYSTEM_REQUESTS = ("any", *FILESYSTEMS)


def node_filesystem(node):
    """Return a node's storage class, including legacy-spec compatibility."""
    return node.get("filesystem", "nfs" if node.get("startup_group") else "local")


def job_filesystem(job):
    """Return a job's requested storage class, including legacy specs."""
    return job.get("filesystem", "any")


def check(value, message):
    if not value:
        raise ValueError(message)


def identifier(value):
    check(isinstance(value, str) and re.fullmatch(r"[A-Za-z0-9][A-Za-z0-9_.-]{0,95}", value),
          "IDs must be 1–96 safe alphanumeric/._- characters")
    return value


def fields(obj, allowed):
    check(isinstance(obj, dict), "expected an object")
    check(not (set(obj) - set(allowed.split())), "unknown fields: " + str(set(obj) - set(allowed.split())))


def number(value, name, minimum=0, integer=False):
    check(isinstance(value, (int, float)) and not isinstance(value, bool)
          and math.isfinite(value) and value >= minimum
          and (not integer or isinstance(value, int)), "invalid " + name)


def absolute(value):
    check(isinstance(value, str) and "\x00" not in value and PurePosixPath(value).is_absolute()
          and ".." not in PurePosixPath(value).parts, "expected absolute path without '..'")


def file_contract(value):
    fields(value, "path sha256")
    absolute(value["path"])
    check(bool(re.fullmatch(r"[0-9a-f]{64}", value["sha256"])), "invalid SHA256")


def node_spec(raw):
    n = copy.deepcopy(raw)
    fields(n, "id transport target python work_root storage_domain labels gpus enabled max_jobs "
           "cpu_limit ram_limit_mib policy assets startup_group recovery datasets filesystem hf")
    identifier(n["id"])
    n.setdefault("transport", "ssh")
    check(n["transport"] in ("local", "ssh"), "transport must be local or ssh")
    if n["transport"] == "ssh":
        check(bool(re.fullmatch(r"[A-Za-z0-9_][A-Za-z0-9_.@:-]*", n.get("target", ""))),
              "target must be a safe SSH alias; use ~/.ssh/config for ports/keys")
    n.setdefault("python", "python3")
    absolute(n["work_root"])
    if "hf" in n:
        fields(n["hf"], "python token_file")
        absolute(n["hf"]["python"])
        if n["hf"].get("token_file"):
            absolute(n["hf"]["token_file"])
    check(n["work_root"] not in ("/", "/home", "/tmp"), "use a dedicated work_root")
    for k, v in dict(enabled=False, max_jobs=1, labels={}, gpus=[], assets={},
                     policy={}, recovery={}, datasets={}, storage_domain="", startup_group="").items():
        n.setdefault(k, v)
    # Before filesystem was explicit, startup_group was used only for NFS cold
    # starts. Preserve that interpretation when normalizing legacy node JSON.
    n.setdefault("filesystem", "nfs" if n["startup_group"] else "local")
    check(n["filesystem"] in FILESYSTEMS, "node filesystem must be local or nfs")
    check(isinstance(n["enabled"], bool), "enabled must be boolean")
    number(n["max_jobs"], "max_jobs", 1, True)
    for k in ("cpu_limit", "ram_limit_mib"):
        if k in n:
            number(n[k], k, 1)
    check(isinstance(n["labels"], dict) and isinstance(n["assets"], dict), "labels/assets must be objects")
    for a in n["assets"].values():
        file_contract(a)
    check(isinstance(n["datasets"], dict), "datasets must map dataset names to absolute paths")
    for name, path in n["datasets"].items():
        identifier(name)
        absolute(path)
    p = n["policy"]
    fields(p, "stable_polls max_snapshot_age_s max_cpu_percent max_gpu_percent min_free_ram_mib "
           "min_free_disk_mib gpu_margin_mib max_idle_used_mib allow_gpu_sharing "
           "allow_external_gpu_processes max_shared_jobs_per_gpu warm_gpu_temp_c max_gpu_temp_c "
           "warm_max_jobs shared_stable_polls read_probe_path read_probe_bytes read_probe_timeout_s")
    defaults = dict(stable_polls=3, max_snapshot_age_s=60, max_cpu_percent=90,
                    max_gpu_percent=10, min_free_ram_mib=1024, min_free_disk_mib=1024,
                    gpu_margin_mib=1024, max_idle_used_mib=256, allow_gpu_sharing=False,
                    allow_external_gpu_processes=False, max_shared_jobs_per_gpu=2,
                    warm_gpu_temp_c=80, max_gpu_temp_c=85, warm_max_jobs=1, shared_stable_polls=1,
                    read_probe_bytes=64*1024*1024,
                    read_probe_timeout_s=10)
    for k, v in defaults.items():
        p.setdefault(k, v)
        if isinstance(v, bool):
            check(isinstance(p[k], bool), k + " must be boolean")
        else:
            number(p[k], k, 1 if k in ("stable_polls", "max_snapshot_age_s", "read_probe_timeout_s",
                                       "max_shared_jobs_per_gpu", "warm_max_jobs", "shared_stable_polls") else 0,
                   k in ("stable_polls", "read_probe_bytes", "max_shared_jobs_per_gpu", "warm_max_jobs",
                         "shared_stable_polls"))
    for k in ("max_cpu_percent", "max_gpu_percent"):
        check(p[k] <= 100, k + " must be <=100")
    check(p["warm_gpu_temp_c"] < p["max_gpu_temp_c"],
          "warm_gpu_temp_c must be below max_gpu_temp_c")
    if "read_probe_path" in p:
        absolute(p["read_probe_path"])
    r = n["recovery"]
    fields(r, "ssh_attempts_per_round ssh_retry_offsets_s ssh_timeouts_s d_state_timeout_s d_observation_max_gap_s")
    for k, v in dict(ssh_attempts_per_round=3, ssh_retry_offsets_s=[0, 300, 600, 900],
                     ssh_timeouts_s=[30, 60, 90], d_state_timeout_s=600,
                     d_observation_max_gap_s=120).items():
        r.setdefault(k, v)
    number(r["ssh_attempts_per_round"], "ssh_attempts_per_round", 1, True)
    for k in ("d_state_timeout_s", "d_observation_max_gap_s"):
        number(r[k], k, 1)
    check(isinstance(r["ssh_retry_offsets_s"], list) and r["ssh_retry_offsets_s"]
          and r["ssh_retry_offsets_s"][0] == 0
          and r["ssh_retry_offsets_s"] == sorted(set(r["ssh_retry_offsets_s"])), "retry offsets must increase from zero")
    check(len(r["ssh_timeouts_s"]) == r["ssh_attempts_per_round"], "one timeout per retry in a batch required")
    for t in r["ssh_retry_offsets_s"]:
        number(t, "retry offset")
    for t in r["ssh_timeouts_s"]:
        number(t, "retry timeout", 1)
    seen = set()
    for g in n["gpus"]:
        fields(g, "uuid index name memory_mib enabled")
        check(isinstance(g["uuid"], str) and g["uuid"].startswith("GPU-"), "full NVIDIA GPU UUID required (MIG unsupported)")
        check(g["uuid"] not in seen, "duplicate GPU UUID")
        seen.add(g["uuid"])
        number(g["index"], "GPU index", 0, True)
        number(g["memory_mib"], "GPU memory", 1)
        g.setdefault("enabled", False)
        check(isinstance(g["enabled"], bool), "GPU enabled must be boolean")
    return n


def group_spec(raw):
    g = copy.deepcopy(raw)
    fields(g, "id min_start_interval_s require_ready_marker")
    identifier(g["id"])
    g.setdefault("min_start_interval_s", 60)
    g.setdefault("require_ready_marker", True)
    number(g["min_start_interval_s"], "min_start_interval_s")
    check(isinstance(g["require_ready_marker"], bool), "require_ready_marker must be boolean")
    return g


def experiment_spec(raw):
    e = copy.deepcopy(raw)
    fields(e, "id project name rq priority tags jobs filesystem")
    identifier(e["id"])
    e.setdefault("project", "general")
    identifier(e["project"])
    for k in ("name", "rq"):
        check(isinstance(e[k], str) and e[k].strip(), k + " is required")
    e.setdefault("priority", 0)
    number(e["priority"], "priority", 0, True)
    e.setdefault("tags", [])
    e.setdefault("filesystem", "any")
    check(e["filesystem"] in FILESYSTEM_REQUESTS,
          "experiment filesystem must be any, local or nfs")
    check(isinstance(e["tags"], list) and all(isinstance(t, str) for t in e["tags"]), "tags must be strings")
    check(isinstance(e["jobs"], list) and e["jobs"], "at least one job required")
    for j in e["jobs"]:
        fields(j, "id name kind purpose argv cwd env config resources depends_on order_only_dependencies priority labels hosts "
               "assets input_files outputs max_attempts metadata failover_safe dataset_path dataset filesystem hf_artifacts hf_relocate_json")
        identifier(j["id"])
        check(j["kind"] in ("train", "eval", "prepare", "analysis"), "invalid job kind")
        check(isinstance(j.get("name"), str) and j["name"].strip(), "job name required")
        check(isinstance(j["argv"], list) and j["argv"]
              and all(isinstance(v, str) and "\x00" not in v for v in j["argv"]), "argv must be a string array")
        absolute(j["cwd"])
        for k, v in dict(env={}, config={}, depends_on=[], priority=0, labels={}, hosts=[],
                         assets={}, input_files=[], outputs=[], max_attempts=1, metadata={}, purpose="",
                         failover_safe=False, dataset_path="", dataset="",
                         filesystem=e["filesystem"]).items():
            j.setdefault(k, v)
        check(j["filesystem"] in FILESYSTEM_REQUESTS,
              "job filesystem must be any, local or nfs")
        number(j["priority"], "job priority", 0, True)
        number(j["max_attempts"], "max_attempts", 1, True)
        check(isinstance(j["failover_safe"], bool), "failover_safe must be boolean")
        if j["failover_safe"]:
            check(j["outputs"], "failover-safe jobs must declare attempt-local outputs")
        check(isinstance(j["dataset"], str) and isinstance(j["dataset_path"], str),
              "dataset and dataset_path must be strings")
        check(not (j["dataset"] and j["dataset_path"]), "choose dataset OR dataset_path, not both")
        if j["dataset"]:
            identifier(j["dataset"])
        if j["dataset_path"]:
            absolute(j["dataset_path"])
        check(isinstance(j["env"], dict) and all(isinstance(v, str) for v in j["env"].values()), "env values must be strings")
        check(all(re.fullmatch(r"[A-Za-z_][A-Za-z0-9_]*", k) for k in j["env"]), "invalid env name")
        check(not any(k.startswith("RS_") or k == "CUDA_VISIBLE_DEVICES" for k in j["env"]), "RS_* and CUDA_VISIBLE_DEVICES are reserved")
        for dep in j["depends_on"]:
            identifier(dep)
        check(len(j["depends_on"]) == len(set(j["depends_on"])), "duplicate dependency")
        order_only = j.get("order_only_dependencies", [])
        check(isinstance(order_only, list) and all(isinstance(d, str) for d in order_only),
              "order_only_dependencies must be an array of dependency IDs")
        check(len(order_only) == len(set(order_only)) and set(order_only) <= set(j["depends_on"]),
              "order-only dependencies must be unique members of depends_on")
        # Control dependencies only wait for success. Artifact transfer must be
        # explicit in the workflow; they never imply remote path accessibility.
        text = json.dumps({k: j[k] for k in ("argv", "cwd", "env", "config", "input_files")})
        check(not any("{dep:" + d + "}" in text for d in order_only),
              "order-only dependency cannot be used as an artifact path")
        for asset_hash in j["assets"].values():
            check(bool(re.fullmatch(r"[0-9a-f]{64}", asset_hash)), "asset must bind a marker SHA256")
        for f in j["input_files"]:
            # {dep:job-id} is an absolute attempt path at execution time.
            fields(f, "path sha256")
            check(isinstance(f["path"], str), "input path required")
            check(bool(re.fullmatch(r"[0-9a-f]{64}", f["sha256"])), "input SHA256 required")
        for out in j["outputs"]:
            check(isinstance(out, str) and out and not PurePosixPath(out).is_absolute()
                  and ".." not in PurePosixPath(out).parts, "outputs must be relative files inside attempt_dir")
        for key in ("hf_artifacts", "hf_relocate_json"):
            if key in j:
                check(isinstance(j[key], list), key + " must be a list")
                for path in j[key]:
                    check(isinstance(path, str) and path and not PurePosixPath(path).is_absolute()
                          and ".." not in PurePosixPath(path).parts and "\\" not in path,
                          key + " must contain relative paths/globs inside attempt_dir")
        r = j.setdefault("resources", {})
        fields(r, "gpu_count vram_mib cpu ram_mib gpu_mode parameter_count")
        for k, v in dict(gpu_count=0, vram_mib=0, cpu=1, ram_mib=512, gpu_mode="exclusive").items():
            r.setdefault(k, v)
        for k in ("gpu_count", "vram_mib", "cpu", "ram_mib"):
            number(r[k], k, 0 if k in ("gpu_count", "vram_mib") else 1, k == "gpu_count")
        check(not r["gpu_count"] or r["vram_mib"] > 0, "GPU jobs require an explicit per-GPU vram_mib estimate")
        check(r["gpu_mode"] in ("exclusive", "shared"), "invalid gpu_mode")
        if "parameter_count" in r:
            number(r["parameter_count"], "parameter_count", 0, True)
    check(len({j["id"] for j in e["jobs"]}) == len(e["jobs"]), "duplicate job ID")
    return e


def validate_dag(jobs):
    visited, active = set(), set()

    def visit(key):
        check(key in jobs, "missing dependency: " + key)
        check(key not in active, "dependency cycle at " + key)
        if key in visited:
            return
        active.add(key)
        for dep in jobs[key]["depends_on"]:
            visit(dep)
        active.remove(key)
        visited.add(key)

    for key in jobs:
        visit(key)
