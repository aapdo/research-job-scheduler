import copy
import unittest
from test_scheduler import node, job, snapshot, reservation, plan


class VariantPreferenceTests(unittest.TestCase):
    def setup_case(self):
        a,b=node(key='a'),node(key='b')
        b['gpus'].append(dict(b['gpus'][0],uuid='GPU-b-2',index=2))
        j=job('multi',gpu_count=3,vram=8000)
        j['resource_variants']=[dict(j['resources'],gpu_count=2)]
        held=reservation(b)
        held['spec']['gpus']=[]
        held['spec']['resources']['gpu_count']=0
        return a,b,j,held

    def test_primary_gpu_count_before_host_load(self):
        a,b,j,held=self.setup_case()
        j['metadata']={'prefer_primary_resources':True}
        out=plan([j],nodes={'a':a,'b':b},snaps={'a':snapshot(a),'b':snapshot(b)},attempts=[held])
        self.assertEqual(out[0]['node'],'b')
        self.assertEqual(len(out[0]['gpus']),3)

    def test_fallback_when_primary_cannot_fit(self):
        a,b,j,held=self.setup_case()
        j['metadata']={'prefer_primary_resources':True}
        b['enabled']=False
        out=plan([j],nodes={'a':a,'b':b},snaps={'a':snapshot(a),'b':snapshot(b)})
        self.assertEqual(out[0]['node'],'a')
        self.assertEqual(len(out[0]['gpus']),2)

    def test_legacy_choice_unchanged_without_opt_in(self):
        a,b,j,held=self.setup_case()
        out=plan([j],nodes={'a':a,'b':b},snaps={'a':snapshot(a),'b':snapshot(b)},attempts=[held])
        self.assertEqual(out[0]['node'],'a')
