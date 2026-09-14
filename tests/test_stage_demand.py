import copy
import time
import unittest
from test_scheduler import node, snapshot, job, experiment
from research_scheduler.artifacts import runnable_stage_demand


class StageDemandTests(unittest.TestCase):
    def demand(self, mutate=None):
        n = node(key='destination')
        source = node(key='source')
        e = experiment([job('producer', gpu_count=0), job('consumer', deps=['producer'])])
        jobs = {j['id']: dict(id=j['id'], spec=j, experiment='e',
                status='succeeded' if j['id']=='producer' else 'queued') for j in e['jobs']}
        consumer = jobs['consumer']['spec']
        consumer['hosts'] = ['destination']
        now = time.time()
        snap = snapshot(n, now)
        a = dict(node='source', spec=dict(node_spec=source), report={})
        if mutate: mutate(consumer, snap, jobs)
        before = copy.deepcopy(a)
        result = runnable_stage_demand([jobs['consumer']], jobs, {'e':e},
                 {'destination':n}, {'destination':snap}, [], {'producer':a}, {}, now)
        self.assertEqual(a, before)
        return result

    def test_unpublished_dependency_gets_demand(self):
        self.assertIn('producer', self.demand())

    def test_hold_is_not_promoted(self):
        self.assertFalse(self.demand(lambda j,s,b: j['labels'].update(user_hold='yes')))

    def test_stale_destination_is_not_promoted(self):
        self.assertFalse(self.demand(lambda j,s,b: s.update(received_at=s['received_at']-61)))

    def test_insufficient_vram_is_not_promoted(self):
        self.assertFalse(self.demand(lambda j,s,b: j['resources'].update(vram_mib=999999)))

    def test_unfinished_dependency_is_not_promoted(self):
        self.assertFalse(self.demand(lambda j,s,b: b['producer'].update(status='running')))
