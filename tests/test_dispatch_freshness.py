"""Long maintenance and harmless GPU ranking changes must not starve dispatch."""
import copy
import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch

from test_scheduler import node, snapshot, job, experiment
from research_scheduler.controller import Controller
from research_scheduler.store import Store


class DispatchFreshnessTests(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.store = Store(Path(self.tmp.name) / 'state.db')
        self.n = node()
        self.store.register_node(self.n)
        self.store.register_experiment(experiment([job()]))
        self.calls = []
        self.clock = 1000.
        self.jitter = False
        self.unhealthy = False
        owner = self
        class Probe:
            def call(self, n, action, request):
                assert action == 'probe'
                owner.calls.append(n['id'])
                data = snapshot(n, now=owner.clock)
                if owner.jitter and len(owner.calls) > 1:
                    data['gpus'][0]['temperature_c'] = 40
                if owner.unhealthy and len(owner.calls) > 1:
                    data['ram_available_mib'] = 0
                return copy.deepcopy(data)
        self.controller = Controller(self.store, Probe())

    def tearDown(self):
        self.store.db.close()
        self.tmp.cleanup()

    def test_gpu_reranking_uses_fresh_validated_selection(self):
        self.jitter = True
        with patch.object(self.controller, '_launch', side_effect=lambda p: p) as launch:
            result = self.controller.tick(execute=True)
        self.assertEqual(len(result['launches']), 1)
        self.assertEqual(launch.call_args.args[0]['gpus'], ['GPU-a-1'])

    def test_slow_reconciliation_refreshes_before_planning(self):
        def slow(**kwargs):
            self.clock += 61
        with patch('research_scheduler.controller.time.time', side_effect=lambda: self.clock), \
             patch.object(self.controller, 'reconcile', side_effect=slow), \
             patch.object(self.controller, '_launch', side_effect=lambda p: p):
            result = self.controller.tick(execute=True)
        self.assertEqual(len(result['launches']), 1)
        self.assertEqual(len(self.calls), 3)  # initial, expired, selected node

    def test_bad_revalidation_still_prevents_launch(self):
        self.unhealthy = True
        with patch.object(self.controller, '_launch') as launch:
            result = self.controller.tick(execute=True)
        self.assertEqual(result['launches'], [])
        launch.assert_not_called()

    def test_artifact_admission_receives_fresh_stable_samples(self):
        self.n['policy']['stable_polls']=3
        self.store.register_node(self.n)
        def slow(**kwargs):self.clock+=121
        def sleep(seconds):self.clock+=seconds
        def artifacts(controller,execute):
            snap=controller.snapshots()['a']
            self.assertLessEqual(self.clock-snap['received_at'],60)
            self.assertGreaterEqual(snap['stable_polls'],3)
        with patch('research_scheduler.controller.time.time',side_effect=lambda:self.clock), patch('research_scheduler.controller.time.sleep',side_effect=sleep), patch.object(self.controller,'reconcile',side_effect=slow), patch('research_scheduler.artifacts.tick',side_effect=artifacts), patch.object(self.controller,'_launch',side_effect=lambda p:p):
            self.controller.tick(execute=True)

    def test_no_refresh_dry_run_keeps_stale_data_blocked(self):
        with patch('research_scheduler.controller.time.time', return_value=1000):
            self.controller.refresh()
        with patch('research_scheduler.controller.time.time', return_value=1100):
            result = self.controller.tick(execute=False, refresh=False)
        self.assertEqual(result['plan'][0]['decision'], 'waiting')
        self.assertEqual(len(self.calls), 1)


if __name__ == '__main__':
    unittest.main()
