import copy
import unittest
from test_scheduler import node, snapshot, job, experiment, plan


class HostGPUCountTests(unittest.TestCase):
    def fixture(self):
        nodes={}
        for key,count in [('lab',4),('farm',3)]:
            n=node(key=key)
            n['gpus']=[dict(uuid=f'GPU-{key}-{i}',index=i,memory_mib=24000,name='test',enabled=True) for i in range(count)]
            nodes[key]=n
        j=job(gpu_count=4);j['hosts']=['lab','farm']
        j['resource_variants']=[dict(gpu_count=3)]
        j['metadata']={'gpu_count_by_host':{'lab':4,'farm':3}}
        return j,nodes

    def test_host_uses_only_its_configured_count(self):
        j,nodes=self.fixture()
        for key,count in [('lab',4),('farm',3)]:
            rows=plan([j],nodes={key:nodes[key]},snaps={key:snapshot(nodes[key])})
            self.assertEqual(rows[0]['decision'],'ready')
            self.assertEqual(len(rows[0]['gpus']),count)

    def test_three_available_on_lab_does_not_trigger_fallback(self):
        j,nodes=self.fixture();n=nodes['lab'];s=snapshot(n)
        s['gpus'][0]['used_mib']=23000
        self.assertEqual(plan([j],nodes={'lab':n},snaps={'lab':s})[0]['decision'],'waiting')

    def test_missing_host_or_unregistered_count_rejected(self):
        j,nodes=self.fixture()
        for mapping in ({'lab':4},{'lab':4,'farm':2},{'lab':4,'farm':True}):
            bad=copy.deepcopy(j);bad['metadata']['gpu_count_by_host']=mapping
            with self.assertRaises(ValueError):experiment([bad])


if __name__=='__main__':unittest.main()
