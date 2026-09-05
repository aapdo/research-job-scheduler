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
from .store import ACTIVE, dumps
from .states import observe_health, recovery_due, transition


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
        return json.loads(result.stdout)


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
                    and recovery_due(health.get(key, {}), time.time())]
        previous = self.snapshots()
        results = {}
        with concurrent.futures.ThreadPoolExecutor(max_workers=8) as pool:
            futures = {}
            for n in selected:
                recovery = health.get(n["id"], {})
                transport_node = dict(n)
                if recovery.get("phase") == "ssh_retrying":
                    idx = recovery["retries_done"] % n["recovery"]["ssh_attempts_per_round"]
                    transport_node["_rpc_timeout_s"] = n["recovery"]["ssh_timeouts_s"][idx]
                futures[pool.submit(self.transport.call, transport_node, "probe", n)] = n
            for future in concurrent.futures.as_completed(futures):
                n = futures[future]
                try:
                    data = future.result()
                except Exception as exc:
                    data = {"error": str(exc)}
                now = time.time()
                data["received_at"] = now
                old = previous.get(n["id"], {})
                continuous = 0 <= now - old.get("received_at", 0) <= n["policy"]["max_snapshot_age_s"]
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
                state = observe_health(health.get(key, {}), value, nodes[key]["recovery"], value["received_at"])
                self.store.db.execute("INSERT OR REPLACE INTO node_health VALUES(?,?)", (key, dumps(state)))
                if state != health.get(key, {}):
                    self.store.event("node_health", key, state)
        return results

    def plan(self):
        nodes, snapshots = self.store.specs("nodes"), self.snapshots()
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
        for key, n in nodes.items():
            if n["startup_group"] in blocked_groups and key in snapshots:
                snapshots[key] = dict(snapshots[key], error="shared backend unhealthy/unreachable: " + blocked_groups[n["startup_group"]])
        return placements(self.store.jobs(), self.store.specs("experiments"), nodes,
                          snapshots, self.store.attempts(), self.store.specs("groups_"), now)

    def invalidate_unavailable(self):
        health = self.node_health()
        for a in self.store.attempts(active=True):
            h = health.get(a["node"], {})
            if h.get("phase") != "unavailable":
                continue
            job = next(j for j in self.store.jobs() if j["id"] == a["job"])
            count = sum(x["job"] == a["job"] for x in self.store.attempts())
            safe = job["spec"]["failover_safe"] and count < job["spec"]["max_attempts"]
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

    def reconcile(self):
        snapshots, groups = self.snapshots(), self.store.specs("groups_")
        health = self.node_health()
        for a in self.store.attempts(active=True):
            node = a["spec"]["node_spec"]  # immutable attempt transport, not edited inventory
            if health.get(a["node"], {}).get("phase") in ("ssh_retrying", "unavailable"):
                report = {"status": "unknown", "reason": "node recovery in progress"}
            else:
                try:
                    report = self.transport.call(node, "status", a["spec"])
                except Exception as exc:
                    report = {"status": "unknown", "reason": str(exc)}
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
                    j = next(j for j in self.store.jobs() if j["id"] == a["job"])
                    count = self.store.db.execute("SELECT COUNT(*) FROM attempts WHERE job=?", (a["job"],)).fetchone()[0]
                    if count < j["spec"]["max_attempts"]:
                        job_status = "queued"
                reason = report.get("reason", report.get("error", ""))
                old_job = next(j for j in self.store.jobs() if j["id"] == a["job"])
                transition("job", old_job["status"], job_status)
                self.store.db.execute("UPDATE jobs SET status=?,reason=? WHERE id=?", (job_status, reason, a["job"]))
                if status != a["status"]:
                    self.store.event("attempt_" + status, a["id"], report)

    def request(self, placement):
        s = self.store
        node = s.specs("nodes")[placement["node"]]
        job = next(j for j in s.jobs() if j["id"] == placement["job"])
        spec = job["spec"]
        dataset = spec.get("dataset", "")
        if dataset:
            dataset_path = node.get("datasets", {}).get(dataset)
            if not dataset_path:
                raise ValueError("dataset path not registered on node: " + dataset)
        else:
            dataset_path = spec.get("dataset_path", "")
        attempt_id = job["id"] + "." + uuid.uuid4().hex
        directory = str(Path(node["work_root"], "attempts", attempt_id))
        successful = {a["job"]: a for a in s.attempts() if a["status"] == "succeeded"}
        substitutions = {"{attempt_dir}": directory, "{config_path}": directory + "/config.json",
                         "{gpu_count}": str(len(placement["gpus"])), "{gpus}": ",".join(placement["gpus"]),
                         "{dataset_path}": dataset_path}
        inputs = list(spec["input_files"])
        for dep in spec["depends_on"]:
            a = successful[dep]
            substitutions["{dep:" + dep + "}"] = a["spec"]["attempt_dir"]
            # Rehash declared predecessor artifacts on the destination before launch.
            inputs.extend({"path": out["path"], "sha256": out["sha256"]}
                          for out in a["report"].get("outputs", {}).values())
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

        request = dict(id=attempt_id, job=job["id"], experiment=job["experiment"],
                       experiment_spec=s.specs("experiments")[job["experiment"]],
                       job_spec=spec, node_spec=node, attempt_dir=directory,
                       argv=[expand(v) for v in spec["argv"]], cwd=expand(spec["cwd"]),
                       env={k: expand(v) for k, v in spec["env"].items()}, config=expand_config(spec["config"]),
                       dataset=dataset, dataset_path=dataset_path,
                       input_files=[dict(f, path=expand(f["path"])) for f in inputs],
                       outputs=spec["outputs"], resources=spec["resources"],
                       startup_group=node["startup_group"], gpus=placement["gpus"])
        request["runner_sha256"] = hashlib.sha256(Path(agent.__file__).read_bytes()).hexdigest()
        request["spec_sha256"] = hashlib.sha256(dumps(request).encode()).hexdigest()
        return request

    def tick(self, execute=False, refresh=True):
        with self.store.lock():
            if refresh:
                self.refresh()
            self.invalidate_unavailable()
            self.reconcile()
            plan = self.plan()
            if not execute:
                return {"mode": "dry-run", "plan": plan}
            chosen = next((p for p in plan if p["decision"] == "ready"), None)
            if chosen is None:
                return {"mode": "execute", "plan": plan, "launched": None}
            # Last-moment fresh check, before committing a reservation. An external
            # scheduler can still race us: exclusive fleet ownership is not enforced.
            self.refresh([chosen["node"]])
            self.invalidate_unavailable()
            fresh = next((p for p in self.plan() if p["job"] == chosen["job"]), {})
            if fresh.get("decision") != "ready" or fresh.get("node") != chosen["node"] or fresh.get("gpus") != chosen["gpus"]:
                return {"mode": "execute", "plan": self.plan(), "launched": None}
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
                self.store.db.execute("UPDATE attempts SET status=?,report=? WHERE id=?", (status, dumps(report), request["id"]))
                self.store.db.execute("UPDATE jobs SET status=?,reason=? WHERE id=?", (status, report.get("reason", ""), request["job"]))
                self.store.event("launch_ack", request["id"], report)
            return {"mode": "execute", "launched": dict(chosen, attempt=request["id"], status=status), "plan": plan}
