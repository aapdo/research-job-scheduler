"""Placement and relay must agree before falling back across GPU pools."""
import time
import unittest

from test_scheduler import node, snapshot, job, experiment, reservation
from research_scheduler.planner import placements, fit
from research_scheduler.artifacts import runnable_stage_demand


class PoolDependencyRelayTests(unittest.TestCase):
    def setup_case(self, kind='eval'):
        self.now = time.time()
        # RP2 is dual-role; FARM7 is the train-only fallback candidate here.
        source_host = 'farm7' if kind == 'eval' else 'rp2'
        self.nodes = {n: node(key=n) for n in (source_host, 'lab4')}
        self.snaps = {n: snapshot(v, self.now) for n, v in self.nodes.items()}
        consumer = job('consumer', deps=['producer'])
        consumer.update(kind=kind, hosts=[source_host, 'lab4'])
        self.exp = experiment([job('producer', gpu_count=0), consumer])
        self.jobs = {s['id']: dict(id=s['id'], spec=s, experiment='e', created=1,
                     status='succeeded' if s['id']=='producer' else 'queued') for s in self.exp['jobs']}
        ready = source_host if kind=='eval' else 'lab4'
        self.producer = dict(id='producer.1', job='producer', node=ready,
            created=1, status='succeeded', released=True,
            spec=dict(node_spec=self.nodes[ready], job_spec=self.exp['jobs'][0], gpus=[],
                      resources=self.exp['jobs'][0]['resources']),
            report={'outputs': {'RESULT.json': {'path':'/runs/RESULT.json','sha256':'a'*64,'bytes':1}}})

    def decisions(self):
        attempts = [self.producer, *getattr(self, 'held', [])]
        result = placements(list(self.jobs.values()), {'e':self.exp}, self.nodes,
                            self.snaps, attempts, {}, self.now)
        demand = runnable_stage_demand([self.jobs['consumer']], self.jobs, {'e':self.exp},
            self.nodes, self.snaps, attempts, {'producer':self.producer}, {}, self.now)
        return next(r for r in result if r['job']=='consumer'), demand

    def test_eval_waits_for_lab_relay_despite_ready_train_host(self):
        self.setup_case()
        decision, demand = self.decisions()
        self.assertEqual(decision['decision'], 'waiting')
        self.assertIn('lab4', decision['reasons'])
        self.assertIn('producer', demand)
        # Feasibility did not manufacture a receipt for real admission.
        self.assertNotIn('artifact_locations', self.producer)
        reason, _ = fit(self.jobs['consumer']['spec'], self.nodes['lab4'], self.snaps['lab4'],
                        [], [self.producer], {'producer':self.producer}, {}, self.now)
        self.assertIn('dependency artifacts', reason)

    def test_verified_relay_launches_eval_in_lab_pool(self):
        self.setup_case()
        self.producer['artifact_locations'] = {'lab4': {'root':'/verified'}}
        decision, demand = self.decisions()
        self.assertEqual(decision['node'], 'lab4')
        self.assertFalse(demand)

    def test_train_waits_for_primary_pool_relay(self):
        self.setup_case('train')
        decision, demand = self.decisions()
        self.assertEqual(decision['decision'], 'waiting')
        self.assertIn('rp2', decision['reasons'])
        self.assertIn('producer', demand)

    def test_pending_relay_pins_consumer_to_its_reserved_destination(self):
        self.setup_case('train')
        result=placements(list(self.jobs.values()), {'e':self.exp}, self.nodes,
                          self.snaps, [self.producer], {}, self.now,
                          relay_intents={'consumer':{'node':'rp2','state':'pending'}})
        decision=next(row for row in result if row['job']=='consumer')
        self.assertEqual(decision['decision'],'waiting')
        self.assertEqual(list(decision['reasons']),['rp2'])
        self.assertIn('reserved destination',decision['reasons']['rp2'])
        self.producer['artifact_locations']={'rp2':{'root':'/verified'}}
        result=placements(list(self.jobs.values()), {'e':self.exp}, self.nodes,
                          self.snaps, [self.producer], {}, self.now,
                          relay_intents={'consumer':{'node':'rp2','state':'pending'}})
        decision=next(row for row in result if row['job']=='consumer')
        self.assertEqual((decision['decision'],decision['node']),('ready','rp2'))

    def test_full_eval_gpu_slots_do_not_fallback_to_train_pool(self):
        self.setup_case()
        self.held = [reservation(self.nodes['lab4'], key='busy'+str(i), gpu=i) for i in range(2)]
        decision, demand = self.decisions()
        self.assertEqual(decision['decision'], 'waiting')
        self.assertFalse(demand)

    def test_missing_producer_output_does_not_invent_relay(self):
        self.setup_case()
        self.producer['report']['outputs'] = {}
        decision, demand = self.decisions()
        self.assertEqual(decision['decision'], 'waiting')
        self.assertIn('producer', demand)

    def test_unusable_eval_gpu_does_not_cross_into_train_pool(self):
        for condition in ('disabled','hot','vram','stale','runtime','host'):
            with self.subTest(condition=condition):
                self.setup_case()
                n, s = self.nodes['lab4'], self.snaps['lab4']
                if condition=='disabled': n['enabled']=False
                if condition=='hot':
                    for g in s['gpus']: g['temperature_c']=95
                if condition=='vram':
                    for g in s['gpus']: g['used_mib']=23999
                if condition=='stale': s['received_at']=self.now-120
                if condition=='runtime':
                    self.jobs['consumer']['spec']['metadata']['execution_profiles']={'lab4': {
                        'resource_contract': self.jobs['consumer']['spec']['resources'],
                        'assets': {'missing-runtime':'a'*64}}}
                if condition=='host': self.jobs['consumer']['spec']['hosts']=['farm7']
                decision, demand=self.decisions()
                self.assertEqual(decision['decision'],'waiting')
                self.assertFalse(demand)


if __name__ == '__main__':
    unittest.main()
