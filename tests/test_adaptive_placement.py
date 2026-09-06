"""Registered DDP alternatives and dependency-aware scheduling, without GPUs."""
import tempfile
import unittest
from pathlib import Path
from test_scheduler import node, snapshot, job, experiment, plan
from research_scheduler.controller import Controller
from research_scheduler.store import Store


def flexible(key='train'):
    value=job(key,gpu_count=4,vram=9000)
    value['resource_variants']=[dict(gpu_count=2,vram_mib=18000)]
    return value


class AdaptivePlacementTests(unittest.TestCase):
    def test_ddp4_falls_back_to_registered_ddp2_only_with_enough_per_gpu_vram(self):
        n=node()
        result=plan([flexible()],n=n)[0]
        self.assertEqual(result['decision'],'ready')
        self.assertEqual(len(result['gpus']),2)
        self.assertEqual(result['resources']['vram_mib'],18000)
        for g in n['gpus']:g['memory_mib']=11264
        self.assertEqual(plan([flexible()],n=n)[0]['decision'],'waiting')

    def test_default_ddp4_preferred_if_equal_load_server_can_fit_it(self):
        a,b=node(),node(key='b')
        b['gpus'] += [dict(uuid='GPU-b-'+str(i),index=i,memory_mib=24000,enabled=True) for i in [2,3]]
        result=plan([flexible()],nodes={'a':a,'b':b},snaps={'a':snapshot(a),'b':snapshot(b)})[0]
        self.assertEqual((result['node'],len(result['gpus'])),('b',4))

    def test_predecessor_inherits_high_priority_and_fanout_breaks_ties(self):
        work=[job('unrelated',priority=50),job('source'),job('urgent',priority=100,deps=['source'])]
        result=plan(work)
        self.assertEqual(result[0]['job'],'source')
        self.assertEqual((result[0]['effective_priority'],result[0]['pending_descendants']),(100,1))
        self.assertEqual(next(x for x in result if x['job']=='urgent')['decision'],'blocked')

    def test_attempt_freezes_chosen_resources_and_rejects_unregistered_variants(self):
        with tempfile.TemporaryDirectory() as root:
            s=Store(Path(root)/'db');s.register_node(node())
            s.register_experiment(experiment([flexible()]))
            placement=plan([flexible()])[0]
            c=Controller(s)
            request=c.request(placement)
            self.assertEqual(request['resources']['gpu_count'],2)
            self.assertEqual(request['job_spec']['resources']['gpu_count'],4)
            with self.assertRaises(ValueError):c.request(dict(placement,resources=dict(request['resources'],gpu_count=8)))
            with s.db:s.db.execute("UPDATE jobs SET status='running'")
            with self.assertRaises(ValueError):s.set_pending_resource_variants('train',[])
            s.db.close()

    def test_unregistered_job_keeps_exact_gpu_request(self):
        self.assertEqual(plan([job(gpu_count=4)])[0]['decision'],'waiting')
        value=flexible();value['resource_variants']=[dict(gpu_count=0)]
        with self.assertRaises(ValueError):experiment([value])


if __name__=='__main__':unittest.main()
