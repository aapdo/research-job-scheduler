"""Node-local dataset mapping, compatibility and real CPU execution tests."""
import json
import sys
import tempfile
import time
import unittest
from pathlib import Path
from unittest.mock import patch

from test_scheduler import FakeProbeTransport, experiment, job, node, plan, snapshot
from research_scheduler.agent import dataset_status
from research_scheduler.cli import main
from research_scheduler.controller import Controller
from research_scheduler.schema import node_spec
from research_scheduler.store import Store, dumps


def mapped_snapshot(n):
    s = snapshot(n)
    s["datasets"] = {name: dataset_status(path) for name, path in n.get("datasets", {}).items()}
    return s


class DatasetTransport(FakeProbeTransport):
    def call(self, n, action, request):
        if action == "probe":
            return mapped_snapshot(n)
        return super().call(n, action, request)


class DatasetPathTests(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory(prefix="scheduler-datasets-")
        self.root = Path(self.temp.name)
        self.data = self.root / "data with spaces"
        self.data.mkdir()
        self.store = Store(self.root / "state.db")
        self.n = node(str(self.root / "runs"))
        self.store.register_node(self.n)
        self.transport = DatasetTransport()
        self.controller = Controller(self.store, self.transport)

    def tearDown(self):
        self.store.db.close()
        self.temp.cleanup()

    def named_job(self):
        j = job(gpu_count=0, vram=0)
        j.update(dataset="vehicle-v1", cwd=str(self.root), outputs=["result.json"],
                 argv=[sys.executable, "-c", "pass", "{dataset_path}"],
                 config={"root": "{dataset_path}", "nested": [{"train": "{dataset_path}/train"}]},
                 env={"DATA_ROOT": "{dataset_path}"})
        return j

    def test_schema_rejects_conflict_and_invalid_mappings(self):
        j = self.named_job()
        j["dataset_path"] = "/another/path"
        with self.assertRaisesRegex(ValueError, "choose dataset OR"):
            experiment([j])
        for mapping in ([], {"name": "relative/path"}, {"name": 123},
                        {"bad/name": "/data"}, {"name": "/data/\x00bad"}):
            with self.subTest(mapping=mapping), self.assertRaises(ValueError):
                node_spec(dict(self.n, datasets=mapping))

    def test_probe_accepts_readable_dir_file_and_symlink(self):
        self.assertTrue(dataset_status(str(self.data))["available"])
        path = self.data / "manifest.json"
        path.write_text("{}")
        self.assertTrue(dataset_status(str(path))["available"])
        link = self.root / "dataset-link"
        link.symlink_to(self.data, target_is_directory=True)
        self.assertTrue(dataset_status(str(link))["available"])
        self.assertFalse(dataset_status(str(self.root / "absent"))["available"])
        with patch("research_scheduler.agent.os.access", return_value=False):
            self.assertFalse(dataset_status(str(self.data))["available"])

    def test_set_dataset_is_audited_and_drops_old_snapshot(self):
        with self.store.db:
            self.store.db.execute("INSERT INTO snapshots VALUES(?,?)", ("a", dumps(snapshot(self.n))))
        result = self.store.set_dataset("a", "vehicle-v1", str(self.data))
        self.assertEqual(result["path"], str(self.data))
        self.assertEqual(self.controller.snapshots(), {})
        self.assertEqual(self.store.specs("nodes")["a"]["datasets"]["vehicle-v1"], str(self.data))
        self.assertEqual(self.store.db.execute("SELECT kind FROM events ORDER BY seq DESC LIMIT 1").fetchone()[0],
                         "dataset_path_registered")
        with self.assertRaisesRegex(ValueError, "unknown node"):
            self.store.set_dataset("missing", "vehicle-v1", str(self.data))

    def test_missing_mapping_does_not_fall_back_to_other_node_path(self):
        j = self.named_job()
        b = node(str(self.root / "b"), key="b")
        b["datasets"] = {"vehicle-v1": str(self.data)}
        p = plan([j], nodes={"a": self.n, "b": b},
                 snaps={"a": mapped_snapshot(self.n), "b": mapped_snapshot(b)})[0]
        self.assertEqual(p["node"], "b")
        self.assertEqual(p["dataset_path"], str(self.data))

    def test_missing_inaccessible_and_stale_paths_are_ineligible(self):
        n = dict(self.n, datasets={"vehicle-v1": str(self.data)})
        for observed in ({}, {"path": str(self.data), "available": False},
                         {"path": "/old/location", "available": True}):
            snap = mapped_snapshot(n)
            snap["datasets"] = {"vehicle-v1": observed}
            p = plan([self.named_job()], n=n, snap=snap)[0]
            self.assertEqual(p["decision"], "waiting")
            self.assertIn("dataset path unavailable", p["reasons"]["a"])

    def test_same_experiment_resolves_distinct_node_paths(self):
        self.store.set_dataset("a", "vehicle-v1", str(self.data))
        b = node(str(self.root / "b"), key="b")
        b.update(transport="ssh", target="example-b", datasets={"vehicle-v1": "/different/server/data"})
        self.store.register_node(b)
        self.store.register_experiment(experiment([self.named_job()]))
        for name, path in (("a", str(self.data)), ("b", "/different/server/data")):
            r = self.controller.request({"job": "j", "node": name, "gpus": []})
            self.assertEqual(r["dataset_path"], path)
            self.assertEqual(r["argv"][-1], path)
            self.assertEqual(r["env"]["DATA_ROOT"], path)
            self.assertEqual(r["config"]["nested"][0]["train"], path + "/train")
            self.assertEqual(r["job_spec"]["dataset"], "vehicle-v1")

    def test_editing_mapping_preserves_frozen_live_attempt(self):
        self.store.set_dataset("a", "vehicle-v1", str(self.data))
        self.store.register_experiment(experiment([self.named_job()]))
        request = self.controller.request({"job": "j", "node": "a", "gpus": []})
        with self.store.db:
            self.store.db.execute("INSERT INTO attempts(id,job,node,spec,status,created) VALUES(?,?,?,?,?,?)",
                                  (request["id"], "j", "a", dumps(request), "running", time.time()))
        self.store.set_dataset("a", "vehicle-v1", "/new/location")
        self.assertEqual(self.store.attempts()[0]["spec"]["dataset_path"], str(self.data))
        self.assertEqual(self.controller.request({"job": "j", "node": "a", "gpus": []})["dataset_path"], "/new/location")

    def test_real_cpu_job_receives_resolved_path(self):
        self.store.set_dataset("a", "vehicle-v1", str(self.data))
        j = self.named_job()
        j["argv"][2] = (
            "import json,os,pathlib,sys; p=sys.argv[1]; "
            "assert p==os.environ['RS_DATASET_PATH']==os.environ['DATA_ROOT']; "
            "assert json.loads(pathlib.Path(os.environ['RS_CONFIG_PATH']).read_text())['root']==p; "
            "assert pathlib.Path(p).is_dir(); "
            "pathlib.Path(os.environ['RS_ATTEMPT_DIR'],'result.json').write_text(json.dumps({'dataset':p}))")
        self.store.register_experiment(experiment([j]))
        deadline = time.time() + 10
        while time.time() < deadline:
            self.controller.tick(execute=True)
            if self.store.jobs()[0]["status"] in ("succeeded", "failed"):
                break
            time.sleep(0.15)
        self.assertEqual(self.store.jobs()[0]["status"], "succeeded")
        receipt = self.store.attempts()[0]
        self.assertEqual(json.loads(Path(receipt["spec"]["attempt_dir"], "result.json").read_text())["dataset"], str(self.data))

    def test_runner_rechecks_path_before_starting_scientific_child(self):
        self.store.set_dataset("a", "vehicle-v1", str(self.data))
        self.store.register_experiment(experiment([self.named_job()]))
        request = self.controller.request({"job": "j", "node": "a", "gpus": []})
        self.data.rename(self.root / "moved-data")
        self.transport.call(self.n, "launch", request)
        report = {}
        deadline = time.time() + 10
        while time.time() < deadline:
            report = self.transport.call(self.n, "status", request)
            if report.get("status") == "failed":
                break
            time.sleep(0.1)
        self.assertEqual(report["status"], "failed")
        self.assertIn("dataset path became unavailable", report["error"])
        self.assertNotIn("child_pid", report)

    def test_cli_set_dataset(self):
        with patch("sys.stdout"):
            main(["--db", str(self.store.path), "set-dataset", "a", "vehicle-v1", str(self.data)])
        self.assertEqual(self.store.specs("nodes")["a"]["datasets"]["vehicle-v1"], str(self.data))

    def test_legacy_database_reregistration_and_direct_path(self):
        j = job(gpu_count=0, vram=0)
        j["dataset_path"] = "/legacy/data"
        e = experiment([j])
        old = json.loads(dumps(e))
        old["jobs"][0].pop("dataset")
        with self.store.db:
            self.store.db.execute("INSERT INTO experiments VALUES(?,?)", ("e", dumps(old)))
            self.store.db.execute("INSERT INTO jobs(id,experiment,spec,status,created) VALUES(?,?,?,?,?)",
                                  ("j", "e", dumps(old["jobs"][0]), "succeeded", time.time()))
        self.store.register_experiment(e)
        self.assertEqual(self.store.jobs()[0]["status"], "succeeded")
        r = self.controller.request({"job": "j", "node": "a", "gpus": []})
        self.assertEqual(r["dataset_path"], "/legacy/data")


if __name__ == "__main__":
    unittest.main()
