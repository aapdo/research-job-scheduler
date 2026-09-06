"""CLI; registration and planning never opt in to job execution implicitly."""
import argparse
import json
import signal
import sys
import threading
from pathlib import Path

from .controller import Controller
from .notifications import poll_campaigns, register_campaign, status as campaign_status
from .store import Store, dumps


def load(path):
    return json.loads(Path(path).read_text())


def parser():
    p = argparse.ArgumentParser(description="Generic research scheduler (Linux, NVIDIA, SSH/local)")
    p.add_argument("--db", required=True, help="SQLite path on the control machine's LOCAL disk")
    sub = p.add_subparsers(dest="command", required=True)
    for name in ("register-node", "register-group", "register-experiment", "register-campaign"):
        sub.add_parser(name).add_argument("file", help="JSON specification")
    d = sub.add_parser("discover", help="read-only live hardware discovery; --apply records GPUs disabled")
    d.add_argument("node")
    d.add_argument("--apply", action="store_true")
    for name in ("enable-node", "drain-node"):
        sub.add_parser(name).add_argument("node")
    r = sub.add_parser("readmit-node", help="clear sticky unavailable state after operator inspection; does not repair the server")
    r.add_argument("node")
    r.add_argument("--ack-old-attempts-may-still-run", action="store_true", required=True)
    g = sub.add_parser("set-gpu", help="enable/disable a registered GPU by UUID")
    g.add_argument("node")
    g.add_argument("uuid")
    g.add_argument("state", choices=["enabled", "disabled"])
    ds = sub.add_parser("set-dataset", help="register a node-specific path for a named dataset (future attempts only)")
    ds.add_argument("node")
    ds.add_argument("dataset")
    ds.add_argument("path")
    storage = sub.add_parser(
        "set-storage-profile",
        help="change future filesystem/path admission without mutating active attempts")
    storage.add_argument("node")
    storage.add_argument("file", help="JSON storage profile")
    ext = sub.add_parser("set-external-gpu-processes",
                         help="allow/disallow future placement beside visible external GPU processes")
    ext.add_argument("node")
    ext.add_argument("state", choices=["enabled", "disabled"])
    margin = sub.add_parser("set-gpu-margin", help="set per-GPU VRAM safety margin for future placement")
    margin.add_argument("node")
    margin.add_argument("mib", type=int)
    packing = sub.add_parser("set-gpu-packing", help="configure scheduler-owned VRAM packing")
    packing.add_argument("node")
    packing.add_argument("state", choices=["enabled", "disabled"])
    packing.add_argument("--max-shared-jobs-per-gpu", type=int, default=2)
    temperature = sub.add_parser("set-temperature-policy", help="configure warm/hard launch temperature limits")
    temperature.add_argument("node")
    temperature.add_argument("--warm-c", type=float, default=80)
    temperature.add_argument("--hard-c", type=float, default=85)
    temperature.add_argument("--warm-max-jobs", type=int, default=1)
    mode = sub.add_parser("set-job-gpu-mode", help="change exclusive/shared mode on a queued job")
    mode.add_argument("job")
    mode.add_argument("mode", choices=["exclusive", "shared"])
    release = sub.add_parser("replace-job-path-prefix", help="rebind a queued job to an immutable release tree")
    release.add_argument("job")
    release.add_argument("old_prefix")
    release.add_argument("new_prefix")
    sub.add_parser("inventory")
    sub.add_parser("probe", help="read-only server/GPU resource and health probes")
    sub.add_parser("plan", help="refresh resources and show hypothetical placements; no launches")
    s = sub.add_parser("status")
    s.add_argument("--json", action="store_true")
    pr = sub.add_parser("priority")
    pr.add_argument("job")
    pr.add_argument("value", type=int)
    retry = sub.add_parser("retry-failed", help="explicitly add bounded retry budget to a failed job")
    retry.add_argument("job")
    retry.add_argument("--additional-attempts", type=int, default=1)
    sub.add_parser("events")
    sub.add_parser("campaign-status", help="show campaign state and notification outbox without secrets")
    sub.add_parser('artifact-status', help='show HF publication and download state, revisions and links')
    hf = sub.add_parser('set-node-hf', help='register node-local HF Python/token paths; credentials are never arguments')
    hf.add_argument('node')
    hf.add_argument('file', help='JSON with python and optional token_file path')
    outputs = sub.add_parser('set-job-hf-artifacts', help='declare export file globs and relocatable result JSON')
    outputs.add_argument('job')
    outputs.add_argument('file', help='JSON with hf_artifacts and hf_relocate_json lists')
    c = sub.add_parser("cancel-pending", help="cancel an unstarted job only; never kills a process")
    c.add_argument("job")
    t = sub.add_parser("tick", help="one scheduling cycle (dry-run unless --execute)")
    t.add_argument("--execute", action="store_true")
    t.add_argument("--max-launches-per-cycle", type=int, default=8)
    d = sub.add_parser("daemon", help="foreground loop; stop controller without stopping children")
    d.add_argument("--execute", action="store_true")
    d.add_argument("--interval", type=float, default=20)
    d.add_argument("--max-launches-per-cycle", type=int, default=8)
    return p


def status_text(store):
    jobs, attempts = store.jobs(), store.attempts()
    active = {a["job"]: a for a in attempts}
    lines = []
    for key, experiment in store.specs("experiments").items():
        lines.extend([f"[{experiment.get('project', 'general')}] {experiment['name']} ({key})",
                      "RQ: " + experiment["rq"]])
        for j in jobs:
            if j["experiment"] != key:
                continue
            a = active.get(j["id"])
            location = (a["node"] + " " + ",".join(a["spec"]["gpus"])) if a else "unassigned"
            lines.append(f"  {j['id']} | {j['spec']['kind']} | {j['status']} | {location}")
            lines.append("    확인할 질문: " + (j["spec"]["purpose"] or experiment["rq"]))
            requested = j["spec"].get("filesystem", experiment.get("filesystem", "any"))
            effective = a["spec"].get("filesystem") if a else None
            lines.append("    filesystem: " + requested
                         + ((" -> " + effective) if effective else " (resolved at placement)"))
            if j["spec"].get("dataset"):
                path = a["spec"].get("dataset_path", "") if a else "resolved at placement"
                lines.append("    dataset: " + j["spec"]["dataset"] + " -> " + path)
            if j["reason"]:
                lines.append("    reason: " + j["reason"])
    return "\n".join(lines) or "No experiments registered."


def main(argv=None):
    args = parser().parse_args(argv)
    store = Store(args.db)
    controller = Controller(store)
    try:
        cmd = args.command
        if cmd.startswith("register-"):
            fn = {"register-node": store.register_node, "register-group": store.register_group,
                  "register-experiment": store.register_experiment}.get(cmd)
            result = register_campaign(store, load(args.file)) if cmd == "register-campaign" else fn(load(args.file))
        elif cmd == "inventory":
            result = {"nodes": store.specs("nodes"), "groups": store.specs("groups_"), "health": controller.node_health()}
        elif cmd == "discover":
            node = store.specs("nodes")[args.node]
            result = controller.transport.call(node, "probe", node)
            if args.apply:
                node["gpus"] = [{k: g[k] for k in ("uuid", "index", "name", "memory_mib")}
                                for g in result["gpus"]]
                for gpu in node["gpus"]:
                    gpu["enabled"] = False
                store.register_node(node)
        elif cmd in ("enable-node", "drain-node"):
            # Draining is safe with live jobs; no transport/scientific state change.
            with store.lock(), store.db:
                node = store.specs("nodes")[args.node]
                node["enabled"] = cmd == "enable-node"
                store.db.execute("UPDATE nodes SET spec=? WHERE id=?", (dumps(node), args.node))
                store.event(cmd, args.node, {"enabled": node["enabled"]})
            result = node
        elif cmd == "set-gpu":
            result = store.set_gpu_enabled(args.node, args.uuid, args.state == "enabled")
        elif cmd == "set-dataset":
            result = store.set_dataset(args.node, args.dataset, args.path)
        elif cmd == "set-storage-profile":
            result = store.set_storage_profile(args.node, load(args.file))
        elif cmd == "set-external-gpu-processes":
            result = store.set_external_gpu_processes_allowed(args.node, args.state == "enabled")
        elif cmd == "set-gpu-margin":
            result = store.set_gpu_margin_mib(args.node, args.mib)
        elif cmd == "set-gpu-packing":
            result = store.set_gpu_packing(args.node, args.state == "enabled",
                                           args.max_shared_jobs_per_gpu)
        elif cmd == "set-temperature-policy":
            result = store.set_temperature_policy(args.node, args.warm_c, args.hard_c,
                                                  args.warm_max_jobs)
        elif cmd == "set-job-gpu-mode":
            result = store.set_pending_gpu_mode(args.job, args.mode)
        elif cmd == "replace-job-path-prefix":
            result = store.replace_pending_path_prefix(args.job, args.old_prefix, args.new_prefix)
        elif cmd == "readmit-node":
            with store.lock(), store.db:
                if controller.node_health().get(args.node, {}).get("phase") != "unavailable":
                    raise ValueError("node is not quarantined/unavailable")
                store.db.execute("DELETE FROM node_health WHERE node=?", (args.node,))
                store.db.execute("DELETE FROM snapshots WHERE node=?", (args.node,))
                store.event(cmd, args.node, {"old_attempts": "remain invalid; late results remain rejected"})
            result = {"node": args.node, "phase": "requires fresh stable health polls"}
        elif cmd == "probe":
            with store.lock():
                result = controller.refresh()
        elif cmd == "plan":
            result = controller.tick(execute=False)
        elif cmd == "status":
            if not args.json:
                print(status_text(store))
                return
            result = {"experiments": store.specs("experiments"), "jobs": store.jobs(), "attempts": store.attempts(),
                      "snapshots": controller.snapshots(), "node_health": controller.node_health(),
                      "campaigns": campaign_status(store)}
        elif cmd == "priority":
            store.prioritize(args.job, args.value)
            result = {"job": args.job, "priority": args.value}
        elif cmd == "retry-failed":
            result = store.retry_failed(args.job, args.additional_attempts)
        elif cmd == "cancel-pending":
            with store.lock(), store.db:
                cursor = store.db.execute("UPDATE jobs SET status='cancelled',reason='cancelled by operator' WHERE id=? AND status='queued'",
                                          (args.job,))
                if cursor.rowcount != 1:
                    raise ValueError("job is absent or not queued; running jobs are never killed by this command")
                store.event(cmd, args.job, {})
            result = {"job": args.job, "status": "cancelled"}
        elif cmd == "events":
            result = [dict(r, data=json.loads(r["data"])) for r in store.db.execute("SELECT * FROM events ORDER BY seq")]
        elif cmd == "campaign-status":
            result = campaign_status(store)
        elif cmd == 'artifact-status':
            from .artifacts import status, publication_summary
            result = dict(transfers=status(store), publication=publication_summary(store))
        elif cmd == 'set-node-hf':
            from .schema import node_spec
            config = load(args.file)
            with store.lock(), store.db:
                n = store.specs('nodes')[args.node]
                n['hf'] = config
                n = node_spec(n)
                store.db.execute('UPDATE nodes SET spec=? WHERE id=?', (dumps(n), args.node))
                store.event('node_hf_changed', args.node, config)
            result = dict(node=args.node, hf=config)
        elif cmd == 'set-job-hf-artifacts':
            from .schema import fields, experiment_spec
            config = load(args.file)
            fields(config, 'hf_artifacts hf_relocate_json')
            with store.lock(), store.db:
                j = next(j for j in store.jobs() if j['id'] == args.job)
                if store.db.execute('SELECT 1 FROM artifact_transfers WHERE attempt IN '
                                    '(SELECT id FROM attempts WHERE job=?)', (args.job,)).fetchone():
                    raise ValueError('artifact contract already frozen by a transfer')
                j['spec'].update(config)
                experiment_spec(dict(id='check', name='check', rq='check', jobs=[j['spec']]))
                store.db.execute('UPDATE jobs SET spec=? WHERE id=?', (dumps(j['spec']), args.job))
                store.event('job_hf_artifacts_changed', args.job, config)
            result = dict(job=args.job, **config)
        elif cmd == "tick":
            result = controller.tick(execute=args.execute, max_launches=args.max_launches_per_cycle)
            result["notifications"] = poll_campaigns(store)
        elif cmd == "daemon":
            if args.interval < 2:
                raise ValueError("minimum interval is 2 seconds")
            if args.max_launches_per_cycle < 1:
                raise ValueError("max launches per cycle must be positive")
            stop = threading.Event()
            for sig in (signal.SIGINT, signal.SIGTERM):
                signal.signal(sig, lambda *_: stop.set())
            while not stop.is_set():
                try:
                    result = controller.tick(execute=args.execute,
                                             max_launches=args.max_launches_per_cycle)
                    result["notifications"] = poll_campaigns(store)
                    print(dumps(result), flush=True)
                except Exception as exc:
                    print(dumps({"controller_error": str(exc), "action": "no new launch in this cycle"}), flush=True)
                stop.wait(args.interval)
            return
        print(json.dumps(result, ensure_ascii=False, indent=2, allow_nan=False))
    except (KeyError, ValueError, RuntimeError, StopIteration) as exc:
        print("error: " + str(exc), file=sys.stderr)
        raise SystemExit(2) from exc
    finally:
        store.db.close()


if __name__ == "__main__":
    main()
