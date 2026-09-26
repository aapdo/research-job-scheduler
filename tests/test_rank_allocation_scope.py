import unittest
from test_scheduler import node, snapshot, job, plan
from research_scheduler.planner import workload_node_allowed


class RankScopeTests(unittest.TestCase):
    def test_pool_scope(self):
        j=job();j['kind']='train'
        self.assertFalse(workload_node_allowed('train','farm8-gui2',j))
        j['metadata']={'train_gpu_allowlist':{'farm8-gui2':['GPU-farm8-gui2-1']}}
        self.assertTrue(workload_node_allowed('train','farm8-gui2',j))
        self.assertFalse(workload_node_allowed('train','farm7',j))
        self.assertFalse(workload_node_allowed('train','rp1',j))
        self.assertFalse(workload_node_allowed('eval','farm7',j))
        self.assertTrue(workload_node_allowed('eval','rp2',j))

    def test_uuid_filter_and_disable(self):
        n=node(key='lab1');j=job();j['kind']='train'
        j['metadata']={'train_gpu_allowlist':{'lab1':['GPU-lab1-1']}}
        r=plan([j],n=n)[0]
        self.assertEqual(r['gpus'],['GPU-lab1-1'])
        n['policy']['disabled_gpu_uuids']=['GPU-lab1-1']
        self.assertEqual(plan([j],n=n)[0]['decision'],'waiting')


if __name__=='__main__':unittest.main()
