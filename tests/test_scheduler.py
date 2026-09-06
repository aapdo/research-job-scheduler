"""No GPU/server needed: fake resource telemetry + real detached CPU processes."""
import copy
import json
import os
import subprocess
import sys
import tempfile
import time
import unittest
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "src"))

from research_scheduler.controller import Controller, Transport
from research_scheduler.agent import process_tree_rss_mib
from research_scheduler.planner import placements
from research_scheduler.schema import experiment_spec, job_filesystem, node_filesystem, node_spec
from research_scheduler.store import Store, dumps
from research_scheduler.states import observe_health, recovery_due, transition


def node(root="/tmp/research-scheduler-test", key="a", group=""):
    return node_spec(dict(id=key, transport="local", python=sys.executable, work_root=root,
                          enabled=True, max_jobs=4, startup_group=group,
                          policy=dict(stable_polls=1, min_free_ram_mib=0, min_free_disk_mib=0,
                                      gpu_margin_mib=1000),
                          gpus=[dict(uuid=f"GPU-{key}-{i}", index=i, memory_mib=24000,
                                     name="test GPU", enabled=True) for i in range(2)]))


def snapshot(n, now=None):
    return dict(received_at=now or time.time(), stable_polls=3, last_counted_at=time.time(),
                cpu_percent=10, cpu_count=16, ram_available_mib=64000, disk_free_mib=100000,
                read_ok=True, d_state=0, assets={},
                gpus=[dict(g, used_mib=0, util_percent=0, temperature_c=35,
                           processes=[], stable_polls=3) for g in n["gpus"]])


def job(key="j", gpu_count=1, vram=10000, priority=0, deps=None):
    return dict(id=key, name=key, kind="train", cwd="/tmp", argv=["true"],
                priority=priority, depends_on=deps or [],
                resources=dict(gpu_count=gpu_count, vram_mib=vram, cpu=1, ram_mib=512))


def experiment(jobs, key="e"):
    return experiment_spec(dict(id=key, name="Experiment", rq="Does the method help?", jobs=jobs))


def plan(jobs, n=None, snap=None, attempts=None, statuses=None, nodes=None, snaps=None, groups=None):
    n = n or node()
    e = experiment(jobs)
    return placements([dict(id=j["id"], spec=j, experiment="e", status=(statuses or {}).get(j["id"], "queued"), created=i)
                       for i, j in enumerate(e["jobs"])], {"e": e}, nodes or {n["id"]: n},
                      snaps or {n["id"]: snap or snapshot(n)}, attempts or [], groups or {})


def reservation(n, key="old", gpu=0, group="", status="running"):
    return dict(id=key, job=key, node=n["id"], created=time.time(), released=False, status=status,
                spec=dict(gpus=[n["gpus"][gpu]["uuid"]], startup_group=group,
                          resources=dict(cpu=1, ram_mib=512, gpu_mode="exclusive", vram_mib=10000)))


class SchemaAndStoreTests(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.store = Store(Path(self.temp.name) / "state.db")

    def tearDown(self):
        self.store.db.close()
        self.temp.cleanup()

    def test_idempotent_registration(self):
        e = experiment([job()])
        self.store.register_experiment(e)
        with self.store.db:
            self.store.db.execute("UPDATE jobs SET status='succeeded'")
        self.store.register_experiment(e)
        self.assertEqual(self.store.jobs()[0]["status"], "succeeded")

    def test_cycle_and_missing_dependency_atomic(self):
        for jobs in ([job("x", deps=["y"]), job("y", deps=["x"])], [job(deps=["missing"])]):
            with self.assertRaises(ValueError):
                self.store.register_experiment(experiment(jobs))
        self.assertEqual(self.store.jobs(), [])

    def test_global_duplicate_job_rejected(self):
        self.store.register_experiment(experiment([job()]))
        with self.assertRaises(ValueError):
            self.store.register_experiment(experiment([job()], key="other"))

    def test_typo_and_parameter_count_not_vram(self):
        j = job()
        j["resources"] = {"gpu_count": 1, "parameter_count": 1000}
        with self.assertRaises(ValueError):
            experiment([j])
        j = job()
        j["resources"]["vrma_mib"] = 1000
        with self.assertRaises(ValueError):
            experiment([j])

    def test_reject_unsafe_paths_env_and_numbers(self):
        for changes in ({"outputs": ["../escape"]}, {"env": {"CUDA_VISIBLE_DEVICES": "0"}},
                        {"resources": {"cpu": float("nan")}}, {"resources": {"gpu_count": -1}}):
            j = job()
            j.update(changes)
            with self.assertRaises(ValueError):
                experiment([j])

    def test_default_inventory_disabled(self):
        n = node_spec(dict(id="server", work_root="/tmp/dedicated", target="my-server"))
        self.assertFalse(n["enabled"])

    def test_filesystem_defaults_inheritance_override_and_validation(self):
        self.assertEqual(node()["filesystem"], "local")
        self.assertEqual(node(root="/tmp/nfs", group="shared")["filesystem"], "nfs")
        explicit = node_spec(dict(id="explicit", transport="local", work_root="/tmp/explicit",
                                  filesystem="nfs"))
        self.assertEqual(explicit["filesystem"], "nfs")
        first, second = job("first"), job("second")
        second["filesystem"] = "local"
        e = experiment_spec(dict(id="fs", name="Filesystem", rq="Does routing work?",
                                 filesystem="nfs", jobs=[first, second]))
        self.assertEqual([j["filesystem"] for j in e["jobs"]], ["nfs", "local"])
        self.assertEqual(experiment([job()])["jobs"][0]["filesystem"], "any")
        with self.assertRaises(ValueError):
            node_spec(dict(id="bad", work_root="/tmp/bad", target="bad", filesystem="network"))
        bad = job()
        bad["filesystem"] = "network"
        with self.assertRaises(ValueError):
            experiment([bad])

    def test_legacy_filesystem_helpers_preserve_old_specs(self):
        self.assertEqual(node_filesystem({"startup_group": "shared"}), "nfs")
        self.assertEqual(node_filesystem({"startup_group": ""}), "local")
        self.assertEqual(job_filesystem({}), "any")

    def test_duplicate_gpu_uuid_rejected(self):
        first = node()
        second = node(key="b")
        second["transport"], second["target"] = "ssh", "b"
        second["gpus"] = first["gpus"]
        self.store.register_node(first)
        with self.assertRaises(ValueError):
            self.store.register_node(second)

    def test_controller_lock(self):
        second = Store(self.store.path)
        try:
            with self.store.lock():
                with self.assertRaises(RuntimeError), second.lock():
                    pass
        finally:
            second.db.close()

    def test_priority_changes_are_audited(self):
        self.store.register_experiment(experiment([job()]))
        self.store.prioritize("j", 99)
        self.assertEqual(self.store.jobs()[0]["spec"]["priority"], 99)
        self.assertEqual(self.store.db.execute("SELECT kind FROM events ORDER BY seq DESC LIMIT 1").fetchone()[0], "priority_changed")

    def test_pending_resources_preserve_running_jobs(self):
        self.store.register_experiment(experiment([job()]))
        self.store.set_pending_resources('j',dict(gpu_count=4,cpu=8,ram_mib=16000,vram_mib=9000))
        self.assertEqual(self.store.jobs()[0]['spec']['resources']['gpu_count'],4)
        with self.store.db:self.store.db.execute("UPDATE jobs SET status='running'")
        with self.assertRaises(ValueError):self.store.set_pending_resources('j',dict(gpu_count=2))

    def test_only_dependency_block_can_be_requeued(self):
        self.store.register_experiment(experiment([job('a'),job('b',deps=['a'])]))
        with self.store.db:
            self.store.db.execute("UPDATE jobs SET status='failed' WHERE id='a'")
            self.store.db.execute("UPDATE jobs SET status='blocked',reason='upstream failed; no evaluation result' WHERE id='b'")
        with self.assertRaises(ValueError):self.store.requeue_dependency_blocked('b')
        self.store.retry_failed('a')
        self.store.requeue_dependency_blocked('b')
        self.assertEqual(next(j for j in self.store.jobs() if j['id']=='b')['status'],'queued')
        self.assertEqual(self.store.db.execute("SELECT kind FROM events ORDER BY seq DESC LIMIT 1").fetchone()[0], 'dependency_block_requeued')

    def test_pending_validation_gate_checks_cycles_and_running_state(self):
        self.store.register_experiment(experiment([job('a'),job('b')]))
        self.store.add_pending_order_dependency('b','a')
        b=next(j for j in self.store.jobs() if j['id']=='b')['spec']
        self.assertEqual(b['order_only_dependencies'],['a'])
        with self.assertRaises(ValueError):self.store.add_pending_order_dependency('a','b')
        with self.store.db:self.store.db.execute("UPDATE jobs SET status='running' WHERE id='a'")
        with self.assertRaises(ValueError):self.store.add_pending_order_dependency('a','b')

    def test_failed_job_can_be_explicitly_requeued_with_more_attempt_budget(self):
        self.store.register_experiment(experiment([job()]))
        with self.store.db:
            self.store.db.execute("UPDATE jobs SET status='failed',reason='test failure' WHERE id='j'")
            self.store.db.execute("INSERT INTO attempts(id,job,node,spec,status,created) VALUES(?,?,?,?,?,?)",
                                  ("old", "j", "a", dumps({}), "failed", time.time()))
        result = self.store.retry_failed("j")
        current = self.store.jobs()[0]
        self.assertEqual((result["status"], current["status"], current["spec"]["max_attempts"]),
                         ("queued", "queued", 2))
        self.assertEqual(self.store.attempts()[0]["status"], "failed")
        with self.assertRaises(ValueError):
            self.store.retry_failed("j")

    def test_process_tree_rss_includes_current_process(self):
        self.assertGreater(process_tree_rss_mib(os.getpid()), 0)

    def test_gpu_enablement_can_change_for_future_jobs_while_attempt_is_active(self):
        n = node()
        self.store.register_node(n)
        request = reservation(n)
        request["spec"]["node_spec"] = copy.deepcopy(n)
        with self.store.db:
            self.store.db.execute("INSERT INTO experiments VALUES(?,?)", ("e", dumps(experiment([job()]))) )
            self.store.db.execute("INSERT INTO jobs(id,experiment,spec,status,created) VALUES(?,?,?,?,?)",
                                  ("old", "e", dumps(job("old")), "running", time.time()))
            self.store.db.execute("INSERT INTO attempts(id,job,node,spec,status,created) VALUES(?,?,?,?,?,?)",
                                  ("old", "old", "a", dumps(request["spec"]), "running", time.time()))
        result = self.store.set_gpu_enabled("a", n["gpus"][1]["uuid"], False)
        self.assertTrue(result["changed"])
        self.assertFalse(self.store.specs("nodes")["a"]["gpus"][1]["enabled"])
        self.assertTrue(self.store.attempts()[0]["spec"]["node_spec"]["gpus"][1]["enabled"])
        self.assertFalse(self.store.set_gpu_enabled("a", n["gpus"][1]["uuid"], False)["changed"])

    def test_storage_profile_change_is_future_only_with_active_attempt(self):
        n = node()
        self.store.register_node(n)
        e = experiment([job("old")])
        request = reservation(n)
        request["spec"]["node_spec"] = copy.deepcopy(n)
        with self.store.db:
            self.store.db.execute("INSERT INTO experiments VALUES(?,?)", ("e", dumps(e)))
            self.store.db.execute("INSERT INTO jobs(id,experiment,spec,status,created) VALUES(?,?,?,?,?)",
                                  ("old", "e", dumps(e["jobs"][0]), "running", time.time()))
            self.store.db.execute("INSERT INTO attempts(id,job,node,spec,status,created) VALUES(?,?,?,?,?,?)",
                                  ("old", "old", "a", dumps(request["spec"]), "running", time.time()))
            self.store.db.execute("INSERT INTO snapshots VALUES(?,?)", ("a", dumps(snapshot(n))))
        result = self.store.set_storage_profile("a", {
            "filesystem": "nfs", "work_root": "/tmp/new-runs", "storage_domain": "shared",
            "startup_group": "shared", "datasets": {"data": "/tmp/data"},
            "assets": {}, "read_probe_path": "/tmp/probe", "read_probe_bytes": 1024,
            "enabled": True, "max_jobs": 6})
        current = self.store.specs("nodes")["a"]
        frozen = self.store.attempts()[0]["spec"]["node_spec"]
        self.assertTrue(result["changed"])
        self.assertEqual((current["filesystem"], current["work_root"]), ("nfs", "/tmp/new-runs"))
        self.assertEqual(current["max_jobs"], 6)
        self.assertEqual((frozen["filesystem"], frozen["work_root"]), ("local", n["work_root"]))
        self.assertIsNone(self.store.db.execute("SELECT data FROM snapshots WHERE node='a'").fetchone())

    def test_external_process_policy_change_preserves_active_attempt_snapshot(self):
        n = node()
        self.store.register_node(n)
        e = experiment([job("old")])
        request = reservation(n)
        request["spec"]["node_spec"] = copy.deepcopy(n)
        with self.store.db:
            self.store.db.execute("INSERT INTO experiments VALUES(?,?)", ("e", dumps(e)))
            self.store.db.execute("INSERT INTO jobs(id,experiment,spec,status,created) VALUES(?,?,?,?,?)",
                                  ("old", "e", dumps(e["jobs"][0]), "running", time.time()))
            self.store.db.execute("INSERT INTO attempts(id,job,node,spec,status,created) VALUES(?,?,?,?,?,?)",
                                  ("old", "old", "a", dumps(request["spec"]), "running", time.time()))
        changed = self.store.set_external_gpu_processes_allowed("a", True)
        self.assertTrue(changed["changed"])
        self.assertTrue(self.store.specs("nodes")["a"]["policy"]["allow_external_gpu_processes"])
        self.assertFalse(self.store.attempts()[0]["spec"]["node_spec"]["policy"]["allow_external_gpu_processes"])

    def test_gpu_margin_change_is_future_only_and_validated(self):
        n = node()
        self.store.register_node(n)
        result = self.store.set_gpu_margin_mib("a", 512)
        self.assertTrue(result["changed"])
        self.assertEqual(self.store.specs("nodes")["a"]["policy"]["gpu_margin_mib"], 512)
        self.assertFalse(self.store.set_gpu_margin_mib("a", 512)["changed"])
        for value in (-1, float("nan"), True):
            with self.subTest(value=value), self.assertRaises(ValueError):
                self.store.set_gpu_margin_mib("a", value)

    def test_gpu_packing_temperature_policy_and_pending_mode_are_audited(self):
        n = node()
        self.store.register_node(n)
        self.assertTrue(self.store.set_gpu_packing("a", True, 2)["changed"])
        self.assertFalse(self.store.set_temperature_policy("a", 80, 85, 1)["changed"])
        self.assertTrue(self.store.set_temperature_policy("a", 79, 85, 1)["changed"])
        current = self.store.specs("nodes")["a"]["policy"]
        self.assertTrue(current["allow_gpu_sharing"])
        self.assertEqual((current["max_shared_jobs_per_gpu"], current["warm_gpu_temp_c"],
                          current["max_gpu_temp_c"], current["warm_max_jobs"]), (2, 79, 85, 1))
        self.store.register_experiment(experiment([job()]))
        self.assertTrue(self.store.set_pending_gpu_mode("j", "shared")["changed"])
        self.assertEqual(self.store.jobs()[0]["spec"]["resources"]["gpu_mode"], "shared")
        with self.assertRaises(ValueError):
            self.store.set_pending_gpu_mode("j", "invalid")
        with self.assertRaises(ValueError):
            self.store.set_temperature_policy("a", 85, 80, 1)

    def test_queued_job_can_move_to_equivalent_immutable_release_prefix(self):
        j = job()
        j.update(cwd="/releases/old", argv=["/env/python", "/releases/old/tool.py"],
                 config={"manifest": "/releases/old/config.json"},
                 input_files=[{"path": "/releases/old/config.json", "sha256": "0" * 64}])
        self.store.register_experiment(experiment([j]))
        result = self.store.replace_pending_path_prefix("j", "/releases/old", "/releases/new")
        spec = self.store.jobs()[0]["spec"]
        self.assertTrue(result["changed"])
        self.assertEqual(spec["cwd"], "/releases/new")
        self.assertEqual(spec["argv"][1], "/releases/new/tool.py")
        self.assertEqual(spec["config"]["manifest"], "/releases/new/config.json")
        self.assertEqual(spec["input_files"][0]["path"], "/releases/new/config.json")


class PlannerTests(unittest.TestCase):
    def test_filesystem_request_filters_nodes_and_reports_effective_value(self):
        local = node(key="local")
        nfs = node(key="nfs")
        nfs["filesystem"] = "nfs"
        nodes = {n["id"]: n for n in (local, nfs)}
        snaps = {n["id"]: snapshot(n) for n in (local, nfs)}
        for requested, expected in (("local", "local"), ("nfs", "nfs")):
            j = job()
            j["filesystem"] = requested
            placement = plan([j], nodes=nodes, snaps=snaps)[0]
            self.assertEqual(placement["node"], expected)
            self.assertEqual(placement["filesystem_request"], requested)
            self.assertEqual(placement["filesystem"], expected)
        placement = plan([job()], nodes=nodes, snaps=snaps)[0]
        self.assertEqual(placement["filesystem_request"], "any")
        self.assertIn(placement["filesystem"], {"local", "nfs"})

    def test_priority_backfill_skips_infeasible(self):
        p = plan([job("large", 3, priority=100), job("small", 1)])
        self.assertEqual(p[0]["decision"], "waiting")
        self.assertEqual(p[1]["decision"], "ready")

    def test_priority_and_gpu_reservations(self):
        p = plan([job("low", 2), job("high", 2, priority=10)])
        self.assertEqual(p[0]["job"], "high")
        self.assertEqual(len(p[0]["gpus"]), 2)
        self.assertEqual(p[1]["decision"], "waiting")

    def test_dependency_blocks_only_successors(self):
        p = plan([job("base"), job("eval", deps=["base"]), job("independent")])
        self.assertEqual([x["decision"] for x in p], ["ready", "blocked", "ready"])

    def test_failed_dependency_remains_blocked(self):
        p = plan([job("base"), job("eval", deps=["base"])], statuses={"base": "failed"})
        self.assertEqual(p[0]["decision"], "blocked")

    def test_stale_dstate_storage_and_cpu(self):
        n = node()
        for key, value in (("received_at", time.time()-1000), ("d_state", 1), ("read_ok", False), ("cpu_percent", 99)):
            s = snapshot(n)
            s[key] = value
            self.assertEqual(plan([job()], n=n, snap=s)[0]["decision"], "waiting", key)

    def test_temperature_hard_limit_and_warm_node_cap_recover_automatically(self):
        n = node()
        s = snapshot(n)
        s["gpus"][0]["temperature_c"] = 85
        self.assertIn("hard launch limit", plan([job()], n=n, snap=s)[0]["reasons"]["a"])
        s["gpus"][0]["temperature_c"] = 80
        self.assertEqual(plan([job()], n=n, snap=s)[0]["decision"], "ready")
        self.assertIn("warm-node job cap", plan([job()], n=n, snap=s,
                      attempts=[reservation(n)])[0]["reasons"]["a"])
        s["gpus"][0]["temperature_c"] = 79
        self.assertEqual(plan([job()], n=n, snap=s,
                         attempts=[reservation(n)])[0]["decision"], "ready")

    def test_vram_per_device_not_sum(self):
        self.assertEqual(plan([job(gpu_count=2, vram=30000)])[0]["decision"], "waiting")

    def test_external_gpu_process_and_utilization(self):
        n = node()
        s = snapshot(n)
        s["gpus"][0]["processes"] = [{"pid": 123}]
        s["gpus"][1]["util_percent"] = 99
        self.assertEqual(plan([job()], n=n, snap=s)[0]["decision"], "waiting")

    def test_opted_in_external_process_uses_vram_headroom_but_remains_scheduler_exclusive(self):
        n = node()
        n["policy"]["allow_external_gpu_processes"] = True
        s = snapshot(n)
        for gpu in s["gpus"]:
            gpu.update(used_mib=600, util_percent=0, processes=[{"pid": 123, "used_mib": "600"}])
        self.assertEqual(plan([job(gpu_count=2, vram=9000)], n=n, snap=s)[0]["decision"], "ready")
        s["gpus"][1]["used_mib"] = 15000
        self.assertEqual(plan([job(gpu_count=2, vram=9000)], n=n, snap=s)[0]["decision"], "waiting")
        s = snapshot(n)
        for gpu in s["gpus"]:
            gpu.update(used_mib=600, util_percent=0, processes=[{"pid": 123}])
        old = reservation(n)
        self.assertEqual(plan([job()], n=n, snap=s, attempts=[old])[0]["gpus"], ["GPU-a-1"])

    def test_disabled_gpu_not_selected(self):
        n = node()
        n["gpus"][0]["enabled"] = False
        p = plan([job()], n=n)[0]
        self.assertEqual(p["gpus"], ["GPU-a-1"])

    def test_unknown_reservation_blocks_node(self):
        n = node()
        p = plan([job()], n=n, attempts=[reservation(n, status="unknown")])
        self.assertEqual(p[0]["decision"], "waiting")

    def test_startup_group_serializes_without_consuming_busy_slot(self):
        n = node(group="nfs")
        p = plan([job("first"), job("second")], n=n, groups={"nfs": {"min_start_interval_s": 60}})
        self.assertEqual([x["decision"] for x in p], ["ready", "waiting"])

    def test_cpu_jobs_do_not_require_gpu(self):
        n = node()
        n["gpus"] = []
        self.assertEqual(plan([job(gpu_count=0, vram=0)], n=n)[0]["decision"], "ready")

    def test_host_labels_and_ram(self):
        j = job()
        j["labels"] = {"env": "missing"}
        self.assertEqual(plan([j])[0]["decision"], "waiting")
        j = job()
        j["resources"]["ram_mib"] = 999999
        self.assertEqual(plan([j])[0]["decision"], "waiting")

    def test_ram_reservation_subtracts_only_unrealized_growth_from_memavailable(self):
        n = node()
        n["policy"]["min_free_ram_mib"] = 4096
        s = snapshot(n)
        n["gpus"] = []
        s["gpus"] = []
        s["ram_available_mib"] = 65000
        active = []
        for i in range(3):
            a = dict(id="old-" + str(i), job="old-" + str(i), node="a", created=time.time(),
                     released=True, status="running",
                     spec=dict(gpus=[], startup_group="", resources=dict(cpu=1, ram_mib=16000,
                               gpu_mode="exclusive", vram_mib=0)))
            a["spec"]["resources"]["ram_mib"] = 16000
            a["report"] = {"rss_mib": 12000}
            active.append(a)
        new = job(gpu_count=0, vram=0)
        new["resources"]["ram_mib"] = 16000
        self.assertEqual(plan([new], n=n, snap=s, attempts=active)[0]["decision"], "ready")
        for a in active:
            a["report"] = {}  # Missing attribution falls back to full reservations.
        self.assertEqual(plan([new], n=n, snap=s, attempts=active)[0]["decision"], "waiting")

    def test_local_dependency_cannot_silently_cross_host(self):
        a, b = node(), node(key="b")
        successful = reservation(a, key="base")
        successful.update(status="succeeded", released=True)
        successful["spec"]["node_spec"] = a
        p = plan([job("base"), job("eval", deps=["base"])], nodes={"b": b}, snaps={"b": snapshot(b)},
                 attempts=[successful], statuses={"base": "succeeded"})
        self.assertEqual(p[0]["decision"], "waiting")
        a["storage_domain"] = b["storage_domain"] = "shared"
        p = plan([job("base"), job("eval", deps=["base"])], nodes={"b": b}, snaps={"b": snapshot(b)},
                 attempts=[successful], statuses={"base": "succeeded"})
        self.assertEqual(p[0]["decision"], "ready")

    def test_shared_memory_conservative_and_opt_in(self):
        n = node()
        n["gpus"] = n["gpus"][:1]
        j = job(vram=8000)
        j["resources"]["gpu_mode"] = "shared"
        self.assertEqual(plan([j], n=n)[0]["decision"], "waiting")
        n["policy"]["allow_gpu_sharing"] = True
        old = reservation(n)
        old["spec"]["resources"]["gpu_mode"] = "shared"
        old["report"] = {"ready": True}
        s = snapshot(n)
        s["gpus"][0].update(used_mib=6000, processes=[{"pid": 123}])
        # Observed use and the scheduler reservation describe the same owned
        # process, so 10k reserved + 8k new + 1k margin fits in 24k.
        self.assertEqual(plan([j], n=n, snap=s, attempts=[old])[0]["decision"], "ready")
        n["policy"]["allow_external_gpu_processes"] = True
        # With external-process opt-in, attribution is ambiguous and both values
        # are retained conservatively, so this placement no longer fits.
        self.assertEqual(plan([j], n=n, snap=s, attempts=[old])[0]["decision"], "waiting")

    def test_shared_jobs_spread_then_pack_only_after_ready_and_obey_cap(self):
        n = node()
        n["policy"].update(allow_gpu_sharing=True, max_shared_jobs_per_gpu=2)
        first, second, third = job("first"), job("second"), job("third")
        for j in (first, second, third):
            j["resources"].update(gpu_mode="shared", vram_mib=5000)
        placements_ = plan([first, second, third], n=n)
        self.assertEqual([p["gpus"] for p in placements_[:2]], [["GPU-a-0"], ["GPU-a-1"]])
        self.assertEqual(placements_[2]["decision"], "waiting")
        active = [reservation(n, "old-0", gpu=0), reservation(n, "old-1", gpu=1)]
        for attempt in active:
            attempt["spec"]["resources"].update(gpu_mode="shared", vram_mib=5000)
            attempt["report"] = {"ready": True}
        self.assertEqual(plan([third], n=n, attempts=active)[0]["gpus"], ["GPU-a-0"])
        active.append(reservation(n, "old-2", gpu=0))
        active[-1]["spec"]["resources"].update(gpu_mode="shared", vram_mib=5000)
        active[-1]["report"] = {"ready": True}
        self.assertEqual(plan([third], n=n, attempts=active)[0]["gpus"], ["GPU-a-1"])


class FakeProbeTransport(Transport):
    def __init__(self, lost_ack=False):
        self.lost_ack = lost_ack
        self.launch_count = 0

    def call(self, n, action, request):
        if action == "probe":
            return snapshot(n)
        result = super().call(n, action, request)
        if action == "launch":
            self.launch_count += 1
            if self.lost_ack:
                self.lost_ack = False
                raise TimeoutError("simulated lost SSH acknowledgement AFTER launch")
        return result


class ExecutionTests(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory(prefix="scheduler-test-")
        self.root = Path(self.temp.name)
        self.store = Store(self.root / "state.db")
        self.n = node(str(self.root / "runs with spaces"))
        self.store.register_node(self.n)
        self.transport = FakeProbeTransport()
        self.controller = Controller(self.store, self.transport)

    def tearDown(self):
        self.store.db.close()
        self.temp.cleanup()

    def cpu_job(self, key="base", code=None, deps=None):
        j = job(key, gpu_count=0, vram=0, deps=deps)
        j.update(cwd=str(self.root), outputs=["result.json"], config={"epochs": 5},
                 argv=[sys.executable, "-c", code or
                       "import os,pathlib; pathlib.Path(os.environ['RS_ATTEMPT_DIR'],'result.json').write_text('42')"])
        return j

    def finish(self, timeout=15):
        deadline = time.time() + timeout
        while time.time() < deadline:
            self.controller.tick(execute=True)
            if all(j["status"] in ("succeeded", "failed") for j in self.store.jobs()):
                return
            time.sleep(0.15)
        self.fail(str(self.store.jobs()))

    def test_real_detached_train_eval_dag_and_independent_job(self):
        evaluation = self.cpu_job("eval", deps=["base"], code=
            "import os,pathlib,sys; assert pathlib.Path(sys.argv[1]).read_text()=='42'; "
            "pathlib.Path(os.environ['RS_ATTEMPT_DIR'],'result.json').write_text('ok')")
        evaluation["kind"] = "eval"
        evaluation["argv"].append("{dep:base}/result.json")
        self.store.register_experiment(experiment([self.cpu_job(), evaluation, self.cpu_job("independent")]))
        self.finish()
        self.assertTrue(all(j["status"] == "succeeded" for j in self.store.jobs()))
        self.assertEqual(self.transport.launch_count, 3)
        for a in self.store.attempts():
            self.assertEqual(a["report"]["returncode"], 0)
            self.assertIn("sha256", a["report"]["outputs"]["result.json"])

    def test_dry_run_never_launches(self):
        self.store.register_experiment(experiment([self.cpu_job()]))
        result = self.controller.tick()
        self.assertEqual(result["plan"][0]["decision"], "ready")
        self.assertEqual(self.store.attempts(), [])
        self.assertFalse((self.root / "runs with spaces").exists())

    def test_multi_launch_fills_independent_local_slots_in_one_cycle(self):
        jobs = [self.cpu_job("job-" + str(i)) for i in range(4)]
        self.store.register_experiment(experiment(jobs))
        result = self.controller.tick(execute=True, max_launches=3)
        self.assertEqual(len(result["launches"]), 3)
        self.assertEqual(len(self.store.attempts()), 3)
        self.assertEqual(len({a["job"] for a in self.store.attempts()}), 3)
        self.finish()

    def test_multi_launch_preserves_shared_startup_group_serialization(self):
        n = copy.deepcopy(self.n)
        n["startup_group"] = "storage"
        self.store.register_node(n)
        self.store.register_group({"id": "storage", "min_start_interval_s": 0})
        self.store.register_experiment(experiment([self.cpu_job("one"), self.cpu_job("two")]))
        result = self.controller.tick(execute=True, max_launches=8)
        self.assertEqual(len(result["launches"]), 1)
        self.assertEqual(len(self.store.attempts()), 1)
        self.finish()

    def test_multi_launch_argument_validation(self):
        for value in (0, -1, 1.5, True):
            with self.subTest(value=value), self.assertRaises(ValueError):
                self.controller.tick(execute=True, max_launches=value)

    def test_lost_ack_and_controller_restart_do_not_duplicate(self):
        self.transport.lost_ack = True
        self.store.register_experiment(experiment([self.cpu_job()]))
        self.controller.tick(execute=True)
        self.assertEqual(self.store.jobs()[0]["status"], "unknown")
        self.controller = Controller(self.store, self.transport)
        self.finish()
        self.assertEqual(self.transport.launch_count, 1)
        self.assertEqual(len(self.store.attempts()), 1)

    def test_duplicate_remote_launch_is_idempotent(self):
        self.store.register_experiment(experiment([self.cpu_job()]))
        self.controller.tick(execute=True)
        a = self.store.attempts()[0]
        self.transport.call(self.n, "launch", a["spec"])
        self.finish()
        self.assertEqual(len(self.store.attempts()), 1)
        self.assertEqual(len(list((self.root / "runs with spaces" / "attempts").iterdir())), 1)

    def test_exit_zero_missing_output_is_failure(self):
        self.store.register_experiment(experiment([self.cpu_job(code="pass")]))
        self.finish()
        self.assertEqual(self.store.jobs()[0]["status"], "failed")

    def test_failed_job_bounded_retries_have_separate_directories(self):
        j = self.cpu_job(code="raise SystemExit(3)")
        j["max_attempts"] = 2
        self.store.register_experiment(experiment([j]))
        self.finish()
        self.assertEqual(len(self.store.attempts()), 2)
        self.assertEqual(len({a["spec"]["attempt_dir"] for a in self.store.attempts()}), 2)

    def test_frozen_input_hash_mismatch_never_executes_user_command(self):
        source = self.root / "input"
        source.write_text("changed")
        j = self.cpu_job()
        j["input_files"] = [{"path": str(source), "sha256": "0"*64}]
        self.store.register_experiment(experiment([j]))
        self.finish()
        a = self.store.attempts()[0]
        self.assertEqual(a["status"], "failed")
        self.assertNotIn("child_pid", a["report"])

    def test_argv_no_shell_interpolation(self):
        j = self.cpu_job(code="import pathlib,sys,os; pathlib.Path(os.environ['RS_ATTEMPT_DIR'],'result.json').write_text(sys.argv[1])")
        literal = "$(touch SHOULD_NOT_EXIST); `whoami` $HOME"
        j["argv"].append(literal)
        self.store.register_experiment(experiment([j]))
        self.finish()
        a = self.store.attempts()[0]
        self.assertEqual(Path(a["spec"]["attempt_dir"], "result.json").read_text(), literal)
        self.assertFalse((self.root / "SHOULD_NOT_EXIST").exists())

    def test_shared_backend_fault_blocks_other_node(self):
        self.store.register_group({"id": "storage", "min_start_interval_s": 60})
        n = copy.deepcopy(self.n)
        n["startup_group"] = "storage"
        self.store.register_node(n)
        b = node(str(self.root / "b"), key="b", group="storage")
        b.update(transport="ssh", target="fake-b")
        self.store.register_node(b)
        self.store.register_experiment(experiment([self.cpu_job()]))
        with self.store.db:
            for key, snap in (("a", snapshot(n)), ("b", dict(snapshot(b), d_state=1))):
                self.store.db.execute("INSERT INTO snapshots VALUES(?,?)", (key, dumps(snap)))
        self.assertEqual(self.controller.plan()[0]["decision"], "waiting")

    def test_cli_help_is_standalone(self):
        result = subprocess.run([sys.executable, "-m", "research_scheduler", "--help"],
                                env={"PYTHONPATH": str(Path(__file__).resolve().parents[1] / "src")},
                                text=True, capture_output=True)
        self.assertEqual(result.returncode, 0, result.stderr)
        self.assertIn("register-node", result.stdout)

    def test_dataset_path_is_passed_without_mapping(self):
        j = self.cpu_job()
        j["dataset_path"] = "/datasets/symlink-managed-by-user"
        j["config"] = {"train": "{dataset_path}/train", "nested": [{"root": "{dataset_path}"}]}
        self.store.register_experiment(experiment([j]))
        self.finish()
        a = self.store.attempts()[0]
        config = json.loads(Path(a["spec"]["attempt_dir"], "config.json").read_text())
        self.assertEqual(config["train"], "/datasets/symlink-managed-by-user/train")

    def test_filesystem_is_frozen_substituted_and_exported(self):
        code = ("import json,os,pathlib,sys; "
                "cfg=json.loads(pathlib.Path(os.environ['RS_CONFIG_PATH']).read_text()); "
                "pathlib.Path(os.environ['RS_ATTEMPT_DIR'],'result.json').write_text("
                "json.dumps({'arg':sys.argv[1],'env':os.environ['RS_FILESYSTEM'],'config':cfg['fs']}))")
        j = self.cpu_job(code=code)
        j.update(filesystem="any", config={"fs": "{filesystem}"})
        j["argv"].append("{filesystem}")
        self.store.register_experiment(experiment([j]))
        self.finish()
        a = self.store.attempts()[0]
        result = json.loads(Path(a["spec"]["attempt_dir"], "result.json").read_text())
        self.assertEqual(result, {"arg": "local", "env": "local", "config": "local"})
        self.assertEqual(a["spec"]["filesystem_request"], "any")
        self.assertEqual(a["spec"]["filesystem"], "local")

    def test_safe_failover_invalidates_old_attempt_and_rejects_late_success(self):
        j = self.cpu_job(code="import time,os,pathlib; time.sleep(1); pathlib.Path(os.environ['RS_ATTEMPT_DIR'],'result.json').write_text('old')")
        j.update(failover_safe=True, max_attempts=2)
        self.store.register_experiment(experiment([j]))
        self.controller.tick(execute=True)
        original = self.store.attempts()[0]
        with self.store.db:
            self.store.db.execute("INSERT OR REPLACE INTO node_health VALUES(?,?)", ("a", dumps({"phase": "unavailable", "reason": "test SSH timeout"})))
        self.controller.tick(execute=False)
        self.assertEqual(self.store.attempts()[0]["status"], "invalid")
        self.assertEqual(self.store.jobs()[0]["status"], "queued")
        # Simulate the old node returning an exit-zero receipt much later.
        time.sleep(1.5)
        self.controller.reconcile()
        self.assertEqual(self.store.attempts()[0]["status"], "invalid")
        self.assertNotEqual(self.store.jobs()[0]["status"], "succeeded")
        self.assertTrue(Path(original["spec"]["attempt_dir"], "state.json").exists())

    def test_unsafe_node_loss_blocks_instead_of_reexecuting(self):
        self.store.register_experiment(experiment([self.cpu_job()]))
        self.controller.tick(execute=True)
        with self.store.db:
            self.store.db.execute("INSERT OR REPLACE INTO node_health VALUES(?,?)", ("a", dumps({"phase": "unavailable", "reason": "test"})))
        self.controller.tick(execute=False)
        self.assertEqual(self.store.jobs()[0]["status"], "blocked")
        time.sleep(0.7)  # allow owned dummy runner to finish before deleting its temp directory

    def test_safe_failover_executes_on_other_node_not_failed_source(self):
        class TwoLocalHosts(FakeProbeTransport):
            def call(self, n, action, request):
                # Simulate two SSH machines using separate local attempt roots;
                # scientific children are real CPU processes, not mocked.
                return super().call(dict(n, transport="local"), action, request)
        self.transport = TwoLocalHosts()
        self.controller = Controller(self.store, self.transport)
        b = node(str(self.root / "second-node"), key="b")
        b.update(transport="ssh", target="simulated-node-b")
        self.store.register_node(b)
        j = self.cpu_job(code="import time,os,pathlib; time.sleep(1); pathlib.Path(os.environ['RS_ATTEMPT_DIR'],'result.json').write_text('42')")
        j.update(failover_safe=True, max_attempts=2)
        self.store.register_experiment(experiment([j]))
        self.controller.tick(execute=True)
        self.assertEqual(self.store.attempts()[0]["node"], "a")
        with self.store.db:
            self.store.db.execute("INSERT OR REPLACE INTO node_health VALUES(?,?)", ("a", dumps({"phase": "unavailable", "reason": "test"})))
        self.finish()
        attempts = self.store.attempts()
        self.assertEqual([(a["node"], a["status"]) for a in attempts], [("a", "invalid"), ("b", "succeeded")])

    def test_recovery_probe_budget_is_persisted_and_honored(self):
        from unittest.mock import patch
        class Offline(FakeProbeTransport):
            def __init__(self):
                super().__init__()
                self.calls = 0
            def call(self, n, action, request):
                self.calls += 1
                raise TimeoutError("offline")
        transport = Offline()
        self.controller = Controller(self.store, transport)
        with patch("research_scheduler.controller.time.time", return_value=1000):
            self.controller.refresh()
            for _ in range(3):
                self.controller.refresh()
            self.assertEqual(transport.calls, 4)  # original failure + three retries
            self.controller = Controller(self.store, transport)
            self.controller.refresh()
            self.assertEqual(transport.calls, 4)  # restart does not reset next_retry_at
        with patch("research_scheduler.controller.time.time", return_value=1300):
            self.controller.refresh()
        self.assertEqual(transport.calls, 5)

    def test_ready_group_requires_post_start_stable_polls(self):
        n = copy.deepcopy(self.n)
        n["startup_group"] = "storage"
        n["policy"]["stable_polls"] = 3
        self.store.register_node(n)
        self.store.register_group({"id": "storage", "min_start_interval_s": 0})
        j = self.cpu_job()
        self.store.register_experiment(experiment([j]))
        request = self.controller.request({"job": "base", "node": "a", "gpus": []})
        with self.store.db:
            self.store.db.execute("INSERT INTO attempts(id,job,node,spec,status,created) VALUES(?,?,?,?,?,?)",
                                  (request["id"], "base", "a", dumps(request), "running", time.time()))
            self.store.db.execute("UPDATE jobs SET status='running'")
        class Ready(FakeProbeTransport):
            def call(self, n, action, request):
                if action == "status":
                    return {"status": "running", "ready": True, "heartbeat": time.time()}
                return super().call(n, action, request)
        self.controller.transport = Ready()
        for i in range(3):
            snap = snapshot(n)
            snap["last_counted_at"] = i + 1
            with self.store.db:
                self.store.db.execute("INSERT OR REPLACE INTO snapshots VALUES(?,?)", ("a", dumps(snap)))
            self.controller.reconcile()
            self.assertEqual(bool(self.store.attempts()[0]["released"]), i == 2)


class RecoveryStateTests(unittest.TestCase):
    def test_twelve_retries_in_four_batches(self):
        policy = node()["recovery"]
        state = observe_health({}, {"error": "offline"}, policy, 1000)
        self.assertEqual(state["retries_done"], 0)
        for batch, offset in enumerate((0, 300, 600, 900)):
            self.assertTrue(recovery_due(state, 1000 + offset))
            for retry in range(3):
                state = observe_health(state, {"error": "offline"}, policy, 1000 + offset)
                self.assertEqual(state["retries_done"], batch*3 + retry + 1)
            if batch < 3:
                self.assertFalse(recovery_due(state, 1000 + offset + 299))
        self.assertEqual(state["phase"], "unavailable")
        self.assertFalse(recovery_due(state, 99999))

    def test_retry_recovery_resets_on_success(self):
        p = node()["recovery"]
        state = observe_health({}, {"error": "offline"}, p, 0)
        state = observe_health(state, {"d_state": 0}, p, 30)
        self.assertEqual(state["phase"], "healthy")
        self.assertNotIn("retries_done", state)

    def test_dstate_ten_minutes_continuous_only(self):
        p = node()["recovery"]
        state = {}
        for now in range(0, 600, 20):
            state = observe_health(state, {"d_state": 1}, p, now)
            self.assertEqual(state["phase"], "d_state_wait")
        state = observe_health(state, {"d_state": 1}, p, 600)
        self.assertEqual(state["phase"], "unavailable")

    def test_dstate_resets_on_clear_or_observation_gap(self):
        p = node()["recovery"]
        state = observe_health({}, {"d_state": 1}, p, 0)
        state = observe_health(state, {"d_state": 1}, p, 601)
        self.assertEqual(state["phase"], "d_state_wait")
        self.assertEqual(state["d_since"], 601)
        state = observe_health(state, {"d_state": 0}, p, 620)
        state = observe_health(state, {"d_state": 1}, p, 640)
        self.assertEqual(state["d_since"], 640)

    def test_invalid_attempt_is_terminal(self):
        for state in ("running", "succeeded", "failed"):
            with self.assertRaises(ValueError):
                transition("attempt", "invalid", state)

    def test_ssh_timeout_does_not_overwrite_target(self):
        from unittest.mock import patch
        n = node()
        n.update(transport="ssh", target="my-ssh-alias", _rpc_timeout_s=60)
        with patch("research_scheduler.controller.subprocess.run") as mocked:
            mocked.return_value = subprocess.CompletedProcess([], 0, "{}", "")
            Transport().call(n, "probe", n)
            args = mocked.call_args[0][0]
            self.assertIn("my-ssh-alias", args)
            self.assertIn("ConnectTimeout=20", args)


if __name__ == "__main__":
    unittest.main()
