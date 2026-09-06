"""Four real CPU dummy jobs with actual local telemetry; never touches a GPU/SSH."""
import argparse
import json
import sys
import time
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "src"))
from research_scheduler.cli import status_text
from research_scheduler.controller import Controller
from research_scheduler.store import Store


def main():
    p = argparse.ArgumentParser(description=__doc__)
    p.add_argument("--directory", required=True, help="dedicated local runtime directory (preserved after test)")
    p.add_argument("--timeout", type=float, default=60)
    args = p.parse_args()
    root = Path(args.directory).resolve()
    store = Store(root / "state.db")
    n = dict(id="local-demo", transport="local", python=sys.executable,
             work_root=str(root / "runs"), filesystem="local", enabled=True, max_jobs=2, gpus=[],
             policy=dict(stable_polls=1, max_cpu_percent=100, min_free_ram_mib=128, min_free_disk_mib=10))
    if not store.specs("nodes"):
        store.register_node(n)
    code = ("import json,os,pathlib,time; time.sleep(1); "
            "pathlib.Path(os.environ['RS_ATTEMPT_DIR'],'result.json').write_text(json.dumps({'value':42}))")
    evaluation = ("import json,os,pathlib,sys; assert json.loads(pathlib.Path(sys.argv[1]).read_text())['value']==42; "
                  "pathlib.Path(os.environ['RS_ATTEMPT_DIR'],'result.json').write_text(json.dumps({'threshold':sys.argv[2]}))")
    jobs = []
    for key in ("baseline", "independent"):
        jobs.append(dict(id="demo-" + key, name=key, kind="train", purpose="Validate an independent training job.",
                         cwd=str(root), argv=[sys.executable, "-c", code], outputs=["result.json"],
                         resources=dict(gpu_count=0, cpu=1, ram_mib=64), failover_safe=True, max_attempts=2))
    for threshold in ("0.2", "0.5"):
        jobs.append(dict(id="demo-eval-" + threshold, name="Evaluation " + threshold, kind="eval",
                         purpose="Read the successful baseline result and evaluate one threshold.",
                         depends_on=["demo-baseline"], cwd=str(root), outputs=["result.json"],
                         resources=dict(gpu_count=0, cpu=1, ram_mib=64),
                         argv=[sys.executable, "-c", evaluation, "{dep:demo-baseline}/result.json", threshold]))
    store.register_experiment(dict(id="local-demo-v1", project="scheduler-validation", name="Local CPU DAG smoke",
                                   rq="Do registration, placement, real execution and dependent evaluation work?",
                                   filesystem="local", jobs=jobs))
    controller = Controller(store)
    deadline = time.time() + args.timeout
    while time.time() < deadline:
        result = controller.tick(execute=True)
        print(json.dumps(result, ensure_ascii=False), flush=True)
        if all(j["status"] == "succeeded" for j in store.jobs()):
            print(status_text(store))
            print("PASS: 4/4 real CPU jobs completed; artifacts retained at " + str(root))
            store.db.close()
            return
        if any(j["status"] in ("failed", "blocked") for j in store.jobs()):
            break
        time.sleep(2)
    print(status_text(store))
    store.db.close()
    raise SystemExit("Not complete within demo timeout; inspect retained state/logs. No process was killed.")


if __name__ == "__main__":
    main()
