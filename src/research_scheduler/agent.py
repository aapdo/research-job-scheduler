"""Standalone stdlib remote helper. Read-only probe needs no remote installation.

Launched attempts receive an immutable copy of this file and a detached runner.
The agent is NOT a sandbox: only trusted operators may register commands.
"""
import csv
import fcntl
import hashlib
import io
import json
import os
import shutil
import subprocess
import sys
import time
import traceback
from pathlib import Path


def atomic_json(path, value):
    path = Path(path)
    temp = path.with_name(path.name + ".tmp." + str(os.getpid()))
    with temp.open("x") as f:
        json.dump(value, f, ensure_ascii=False, allow_nan=False)
        f.flush()
        os.fsync(f.fileno())
    os.replace(temp, path)


def digest(path):
    h = hashlib.sha256()
    with Path(path).open("rb") as stream:
        for block in iter(lambda: stream.read(1024 * 1024), b""):
            h.update(block)
    return h.hexdigest()


def process(pid):
    try:
        text = Path("/proc", str(pid), "stat").read_text()
        parts = text[text.rfind(")") + 2:].split()
        return {"state": parts[0], "pgrp": int(parts[2]), "start": parts[19]}
    except (OSError, ValueError, IndexError):
        return None


def group_alive(pgid):
    return any((p := process(x.name)) and p["pgrp"] == pgid and p["state"] != "Z"
               for x in Path("/proc").iterdir() if x.name.isdigit())


def cpu_sample():
    ticks = [int(x) for x in Path("/proc/stat").read_text().splitlines()[0].split()[1:9]]
    return sum(ticks), ticks[3] + ticks[4]


def probe(node):
    start = time.time()
    before = cpu_sample()
    time.sleep(0.15)
    after = cpu_sample()
    cpu = 100 * (1 - (after[1] - before[1]) / max(1, after[0] - before[0]))
    mem = {line.split(":")[0]: int(line.split()[1]) / 1024
           for line in Path("/proc/meminfo").read_text().splitlines()}
    cpu_count = len(os.sched_getaffinity(0))
    available = mem["MemAvailable"]
    # cgroup v2 root-relative limits; /proc/affinity remains the fallback on v1.
    try:
        quota, period = Path("/sys/fs/cgroup/cpu.max").read_text().split()
        if quota != "max":
            cpu_count = min(cpu_count, float(quota) / float(period))
    except (OSError, ValueError):
        pass
    try:
        maximum = Path("/sys/fs/cgroup/memory.max").read_text().strip()
        if maximum != "max":
            current = int(Path("/sys/fs/cgroup/memory.current").read_text())
            available = min(available, max(0, int(maximum) - current) / 1024**2)
    except (OSError, ValueError):
        pass
    d_pids = [int(x.name) for x in Path("/proc").iterdir() if x.name.isdigit()
              and (p := process(x.name)) and p["state"] == "D"]
    root = Path(node["work_root"])
    while not root.exists():
        root = root.parent
    result = dict(time=start, hostname=os.uname().nodename, boot_id=Path("/proc/sys/kernel/random/boot_id").read_text().strip(),
                  cpu_percent=cpu, cpu_count=cpu_count, ram_available_mib=available,
                  ram_total_mib=mem["MemTotal"], disk_free_mib=shutil.disk_usage(root).free / 1024**2,
                  d_state=len(d_pids), d_state_pids=d_pids, gpus=[], assets={}, read_ok=True)
    try:
        query = subprocess.check_output(["nvidia-smi", "--query-gpu=index,uuid,name,memory.total,memory.used,utilization.gpu",
                                         "--format=csv,noheader,nounits"], text=True, stderr=subprocess.PIPE, timeout=8)
        apps = subprocess.check_output(["nvidia-smi", "--query-compute-apps=gpu_uuid,pid,used_gpu_memory",
                                        "--format=csv,noheader,nounits"], text=True, stderr=subprocess.PIPE, timeout=8)
        processes = {}
        for row in csv.reader(io.StringIO(apps), skipinitialspace=True):
            if len(row) == 3:
                processes.setdefault(row[0], []).append({"pid": int(row[1]), "used_mib": row[2]})
        for row in csv.reader(io.StringIO(query), skipinitialspace=True):
            result["gpus"].append(dict(index=int(row[0]), uuid=row[1], name=row[2], memory_mib=float(row[3]),
                                       used_mib=float(row[4]), util_percent=float(row[5]), processes=processes.get(row[1], [])))
    except (OSError, subprocess.SubprocessError, ValueError) as exc:
        result["gpus"] = []  # fail closed for GPU admission, still report CPU health
        result["gpu_error"] = str(exc)
    for key, contract in node["assets"].items():
        try:
            sha = digest(contract["path"])
            if sha == contract["sha256"]:
                result["assets"][key] = sha
        except OSError:
            pass
    policy = node["policy"]
    if policy.get("read_probe_path"):
        if d_pids:
            result["read_ok"] = False
            result["read_skipped"] = "D-state already present; do not pile up read probes"
            return result
        # A separate timed subprocess prevents a hung filesystem read from blocking
        # the control plane. A timed-out D-state child is NOT assumed terminated.
        code = "import sys; f=open(sys.argv[1],'rb'); b=f.read(int(sys.argv[2])); sys.exit(0 if len(b)==int(sys.argv[2]) else 2)"
        t = time.monotonic()
        try:
            child = subprocess.Popen([sys.executable, "-c", code, policy["read_probe_path"], str(policy["read_probe_bytes"])],
                                     stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
            result["read_ok"] = child.wait(timeout=policy["read_probe_timeout_s"]) == 0
        except subprocess.TimeoutExpired:
            child.kill()
            # Do not unbounded-wait for an uninterruptible task.
            result["read_ok"] = False
            result["read_timeout_pid"] = child.pid
        result["read_seconds"] = time.monotonic() - t
    return result


def read_status(request):
    directory = Path(request["attempt_dir"])
    if not directory.exists():
        return {"status": "unknown", "reason": "attempt directory absent; no automatic relaunch"}
    try:
        state = json.loads((directory / "state.json").read_text())
        if state["attempt"] != request["id"]:
            return {"status": "unknown", "reason": "attempt identity mismatch"}
        if state["status"] in ("succeeded", "failed"):
            if state.get("child_pgid") and group_alive(state["child_pgid"]):
                return dict(state, status="unknown", reason="descendant processes still alive")
            return state
        runner = process(state.get("runner_pid", 0))
        boot = Path("/proc/sys/kernel/random/boot_id").read_text().strip()
        if (runner and runner["start"] == state.get("runner_start") and runner["state"] != "Z"
                and boot == state.get("boot_id")):
            return state
        return dict(state, status="unknown", reason="runner absent/rebooted; preserve reservation for reconciliation")
    except (OSError, ValueError, KeyError) as exc:
        return {"status": "unknown", "reason": "unreadable attempt receipt: " + str(exc)}


def launch(request, source):
    directory = Path(request["attempt_dir"])
    directory.parent.mkdir(parents=True, exist_ok=True)
    try:
        directory.mkdir(mode=0o700)
    except FileExistsError:
        # Lost SSH acknowledgement must never create a second scientific child.
        return read_status(request)
    for name, text in (("spec.json", json.dumps(request)), ("runner.py", source)):
        with (directory / name).open("x") as f:
            f.write(text)
            f.flush()
            os.fsync(f.fileno())
    with (directory / "runner.log").open("xb") as log:
        child = subprocess.Popen([sys.executable, str(directory / "runner.py"), "run", str(directory / "spec.json")],
                                 stdin=subprocess.DEVNULL, stdout=log, stderr=subprocess.STDOUT,
                                 start_new_session=True, close_fds=True)
    return {"status": "starting", "runner_pid": child.pid, "attempt": request["id"]}


def run(request):
    directory = Path(request["attempt_dir"])
    with (directory / "runner.lock").open("a") as lock:
        fcntl.flock(lock, fcntl.LOCK_EX | fcntl.LOCK_NB)
        # Even a manual repeated runner invocation must not rerun an attempt.
        claim = directory / "execution.claim"
        with claim.open("x") as f:
            f.write(str(os.getpid()))
        state = dict(attempt=request["id"], status="starting", runner_pid=os.getpid(),
                     runner_start=process(os.getpid())["start"],
                     boot_id=Path("/proc/sys/kernel/random/boot_id").read_text().strip(),
                     started=time.time(), ready=False)
        atomic_json(directory / "state.json", state)
        try:
            if digest(directory / "runner.py") != request["runner_sha256"]:
                raise ValueError("immutable runner source hash mismatch")
            frozen = {k: v for k, v in request.items() if k != "spec_sha256"}
            encoded = json.dumps(frozen, sort_keys=True, ensure_ascii=False, allow_nan=False).encode()
            if hashlib.sha256(encoded).hexdigest() != request["spec_sha256"]:
                raise ValueError("immutable attempt specification hash mismatch")
            for contract in request["input_files"]:
                if digest(contract["path"]) != contract["sha256"]:
                    raise ValueError("input hash mismatch: " + contract["path"])
            atomic_json(directory / "config.json", request["config"])
            env = os.environ.copy()
            env.update(request["env"])
            env.update(CUDA_VISIBLE_DEVICES=",".join(request["gpus"]),
                       RS_ATTEMPT_ID=request["id"], RS_ATTEMPT_DIR=str(directory),
                       RS_CONFIG_PATH=str(directory / "config.json"), RS_READY_PATH=str(directory / "startup.ready"),
                       RS_DATASET_PATH=request.get("dataset_path", ""))
            with (directory / "stdout.log").open("xb") as out, (directory / "stderr.log").open("xb") as err:
                child = subprocess.Popen(request["argv"], cwd=request["cwd"], env=env,
                                         stdin=subprocess.DEVNULL, stdout=out, stderr=err, start_new_session=True)
                state.update(status="running", child_pid=child.pid, child_pgid=child.pid)
                while child.poll() is None:
                    state.update(heartbeat=time.time(), ready=(directory / "startup.ready").is_file())
                    atomic_json(directory / "state.json", state)
                    time.sleep(0.5)
                code = child.wait()
            outputs = {}
            if code == 0:
                for name in request["outputs"]:
                    path = directory / name
                    if not path.resolve().is_relative_to(directory.resolve()) or not path.is_file():
                        raise ValueError("missing/escaping output file: " + name)
                    outputs[name] = {"path": str(path), "sha256": digest(path), "bytes": path.stat().st_size}
            state.update(status="succeeded" if code == 0 else "failed", returncode=code, outputs=outputs)
        except Exception as exc:
            state.update(status="failed", error=str(exc), traceback=traceback.format_exc())
        state.update(finished=time.time(), heartbeat=time.time())
        atomic_json(directory / "state.json", state)
        return state


def main():
    if len(sys.argv) > 1 and sys.argv[1] == "run":
        run(json.loads(Path(sys.argv[2]).read_text()))
        return
    payload = json.load(sys.stdin)
    action, request = payload["action"], payload["request"]
    if action == "probe":
        result = probe(request)
    elif action == "status":
        result = read_status(request)
    elif action == "launch":
        result = launch(request, payload["source"])
    else:
        raise ValueError("unknown agent action")
    print(json.dumps(result, ensure_ascii=False, allow_nan=False))


if __name__ == "__main__":
    main()
