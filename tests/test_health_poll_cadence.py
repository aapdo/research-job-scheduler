"""Fresh launch telemetry and bounded health-history cadence are independent."""
import copy
import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch

from test_scheduler import node, snapshot
from research_scheduler.controller import Controller
from research_scheduler.planner import base_health
from research_scheduler.store import Store


class PollCadenceTests(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.store = Store(Path(self.tmp.name) / 'state.db')
        self.n = node()
        self.n['policy']['stable_polls'] = 3
        self.store.register_node(self.n)
        self.data = snapshot(self.n)
        self.data['boot_id'] = 'boot-a'
        class Probe:
            def call(inner, *args):
                return copy.deepcopy(self.data)
        self.controller = Controller(self.store, Probe())

    def tearDown(self):
        self.store.db.close()
        self.tmp.cleanup()

    def poll(self, now):
        with patch('research_scheduler.controller.time.time', return_value=now):
            return self.controller.refresh()['a']

    def test_62_second_cadence_and_freshness(self):
        first=self.poll(1000)
        self.assertEqual(first['stable_polls'], 1)
        self.assertEqual(first['stable_since'],1000)
        second=self.poll(1062)
        self.assertEqual(second['stable_polls'], 2)
        self.assertEqual(second['stable_since'],1000)
        result = self.poll(1124)
        self.assertEqual(result['stable_polls'], 3)
        self.assertTrue(all(g['stable_polls'] == 3 for g in result['gpus']))
        self.assertEqual(base_health(self.n, result, 1125), '')
        self.assertEqual(base_health(self.n, result, 1185), 'stale resource snapshot')

    def test_long_gap_and_reboot_reset(self):
        self.poll(1000)
        self.poll(1062)
        self.assertEqual(self.poll(1183)['stable_polls'], 1)
        self.data['boot_id'] = 'boot-b'
        self.assertEqual(self.poll(1245)['stable_polls'], 1)

    def test_bad_sample_and_rapid_calls(self):
        self.poll(1000)
        self.assertEqual(self.poll(1001)['stable_polls'], 1)
        self.data['d_state'] = 1
        self.assertEqual(self.poll(1062)['stable_polls'], 0)
        self.data['d_state'] = 0
        recovered=self.poll(1124)
        self.assertEqual(recovered['stable_polls'], 1)
        self.assertEqual(recovered['stable_since'],1124)

    def test_gpu_bad_sample_resets_only_gpu_streak(self):
        self.poll(1000)
        self.data['gpus'][0]['temperature_c'] = 100
        result = self.poll(1062)
        self.assertEqual(result['gpus'][0]['stable_polls'], 0)
        self.assertEqual(result['gpus'][1]['stable_polls'], 2)

    def test_long_cycle_warmup_collects_three_real_polls(self):
        clock=[1000.0]
        with patch('research_scheduler.controller.time.time', side_effect=lambda:clock[0]), patch('research_scheduler.controller.time.sleep', side_effect=lambda seconds:clock.__setitem__(0,clock[0]+seconds)):
            self.controller.refresh()
            self.controller.stabilize_healthy_nodes()
        result=self.controller.snapshots()['a']
        self.assertEqual(result['stable_polls'],3)
        self.assertEqual(clock[0],1004)
        self.assertTrue(all(g['stable_polls']==3 for g in result['gpus']))

    def test_warmup_never_promotes_unhealthy_node(self):
        self.data['d_state']=1
        self.poll(1000)
        with patch('research_scheduler.controller.time.time',return_value=1000), patch('research_scheduler.controller.time.sleep') as sleep:
            self.controller.stabilize_healthy_nodes()
        sleep.assert_not_called()
        self.assertEqual(self.controller.snapshots()['a']['stable_polls'],0)
