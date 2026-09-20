import copy
import json
import sys
import tempfile
import unittest
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "src"))

from research_scheduler.report_placement import (
    attempt_cwd, on_archive_host, on_dependency_host, required_dependency_host, validate_report_policy,
)
from research_scheduler.schema import experiment_spec
from research_scheduler.store import Store


RESOURCES = {"gpu_count": 0, "vram_mib": 0, "gpu_mode": "exclusive", "cpu": 1, "ram_mib": 1024}


def report(metadata=None, hosts=None):
    return dict(
        id="STUDY_REPORT_V1", name="study report", kind="analysis", cwd="/control",
        argv=["/control/python", "-c", "print('ok')"], depends_on=["STUDY_EVAL_V1"],
        resources=copy.deepcopy(RESOURCES), metadata=metadata or {}, hosts=hosts or ["resource-control"],
    )


def profile(root):
    return {"argv": [root + "/python", "-c", "print('ok')"], "cwd": root,
            "resource_contract": copy.deepcopy(RESOURCES)}


class ReportPlacementTests(unittest.TestCase):
    def test_new_control_host_report_is_rejected(self):
        with self.assertRaisesRegex(ValueError, "dependency host"):
            validate_report_policy(experiment_spec(dict(
                id="e", name="e", rq="e", jobs=[report()]))["jobs"][0])

    def test_explicit_control_exception_requires_reason(self):
        value = report({"control_report_exception": "licensed local parser is unavailable remotely"})
        self.assertEqual(validate_report_policy(experiment_spec(dict(
            id="e", name="e", rq="e", jobs=[value]))["jobs"][0])["hosts"], ["resource-control"])
        with self.assertRaises(ValueError):
            validate_report_policy(experiment_spec(dict(
                id="bad", name="bad", rq="bad",
                jobs=[report({"control_report_exception": ""})]))["jobs"][0])

    def test_helper_pins_profiles_and_dependency(self):
        value = on_dependency_host(report(hosts=[]), "STUDY_EVAL_V1", {
            "lab1": profile("/lab/report"), "rp2": profile("/workspace/report")})
        self.assertEqual(value["hosts"], ["lab1", "rp2"])
        self.assertEqual(value["metadata"]["report_execution_dependency"], "STUDY_EVAL_V1")

    def test_required_host_uses_successful_dependency_attempt(self):
        value = on_dependency_host(report(hosts=[]), "STUDY_EVAL_V1", {"rp2": profile("/workspace/report")})
        self.assertIsNone(required_dependency_host(value, {}))
        self.assertEqual(required_dependency_host(value, {"STUDY_EVAL_V1": {"node": "rp2"}}), "rp2")

    def test_store_enforces_only_new_registration(self):
        with tempfile.TemporaryDirectory() as folder:
            store = Store(Path(folder) / "state.db")
            try:
                raw = dict(id="e", name="e", rq="e", jobs=[report()])
                with self.assertRaisesRegex(ValueError, "dependency host"):
                    store.register_experiment(raw)
                fixed = on_dependency_host(report(hosts=[]), "STUDY_EVAL_V1", {"rp2": profile("/workspace/report")})
                # DAG validation is deliberately reached after report-policy validation.
                with self.assertRaisesRegex(ValueError, "missing dependency"):
                    store.register_experiment(dict(id="fixed", name="fixed", rq="fixed", jobs=[fixed]))
            finally:
                store.db.close()

    def test_existing_portable_lab4_profile_uses_attempt_directory(self):
        value = report({"execution_profiles": {"lab4": profile("/retired/release")}}, hosts=["lab4"])
        fixed = on_archive_host(value, {"python": "/lab4/python", "work_root": "/lab4/runs"})
        self.assertTrue(fixed["metadata"]["report_attempt_cwd"])
        self.assertEqual(fixed["metadata"]["execution_profiles"]["lab4"]["cwd"], "/lab4/runs")
        self.assertEqual(attempt_cwd(fixed, "/lab4/runs/attempts/report.1"),
                         "/lab4/runs/attempts/report.1")
        resolved = copy.deepcopy(fixed)
        resolved.setdefault("env", {})["LD_LIBRARY_PATH"] = "/lab4/runtime/lib"
        self.assertEqual(attempt_cwd(resolved, "/lab4/runs/attempts/report.2"),
                         "/lab4/runs/attempts/report.2")

    def test_label_suffix_is_not_mistaken_for_a_host_path(self):
        value = report({"execution_profiles": {"lab4": {
            **profile("/retired/release"),
            "argv": ["/lab4/python", "-c", "label='x/int8/e5'; print(label.endswith('/int8/e5'))"],
        }}}, hosts=["lab4"])
        fixed = on_archive_host(value, {"python": "/lab4/python", "work_root": "/lab4/runs"})
        self.assertTrue(fixed["metadata"]["report_attempt_cwd"])

    def test_inline_host_path_requires_a_prepared_profile(self):
        value = report(hosts=["lab4"])
        value["argv"] = ["python3", "-c", "from pathlib import Path; print(Path('/home/jy/input'))"]
        with self.assertRaisesRegex(ValueError, "LAB4 runtime profile"):
            on_archive_host(value, {"python": "/lab4/python", "work_root": "/lab4/runs"})


if __name__ == "__main__":
    unittest.main()
