import copy,os,unittest
from unittest.mock import patch
from research_scheduler.model_vram_policy import normalize,reservation
from research_scheduler.gpu_recovery import retry_spec

class ModelVramPolicyTests(unittest.TestCase):
    @patch.dict(os.environ,{'RS_MODEL_VRAM_MIB':'9216'})
    def test_nine_gib_default_preserves_oom_increase_and_original(self):
        s=self.spec();n=normalize(s)
        self.assertEqual(n['resources']['vram_mib'],9216)
        self.assertEqual(s['resources']['vram_mib'],14000)
        s['metadata']['vram_after_oom_mib']=12288
        self.assertEqual(normalize(s)['resources']['vram_mib'],12288)
    @patch.dict(os.environ,{'RS_MODEL_VRAM_MIB':'10240'})
    def test_controller_launch_request_uses_policy_not_frozen_registration(self):
        import tempfile
        from test_scheduler import node,snapshot,job,experiment
        from research_scheduler.store import Store,dumps
        from research_scheduler.controller import Controller
        with tempfile.TemporaryDirectory() as root:
            s=Store(root+'/state.db');n=node(root+'/runs');s.register_node(n)
            s.register_experiment(experiment([job(vram=14000)]))
            with s.db:s.db.execute('INSERT INTO snapshots VALUES(?,?)',(n['id'],dumps(snapshot(n))))
            c=Controller(s);placement=c.plan()[0];self.assertEqual(placement['decision'],'ready')
            request=c.request(placement);self.assertEqual(request['resources']['vram_mib'],10240)
            self.assertEqual(s.jobs()[0]['spec']['resources']['vram_mib'],14000)
            s.db.close()
    def spec(self):
        r=dict(gpu_count=1,vram_mib=14000)
        return dict(kind='train',resources=r,hosts=['a','b'],max_attempts=1,metadata={
            'execution_original_resources':[copy.deepcopy(r)],
            'execution_profiles':{'a':{'resource_contract':copy.deepcopy(r)}}})
    @patch.dict(os.environ,{'RS_MODEL_VRAM_MIB':'10240'})
    def test_normalized_spec_skips_copy_but_checks_all_contracts(self):
        n=normalize(self.spec())
        with patch('research_scheduler.model_vram_policy.copy.deepcopy',side_effect=AssertionError('redundant copy')):
            self.assertIs(normalize(n),n)
        for resource in (n['metadata']['execution_profiles']['a']['resource_contract'],
                         n['metadata']['execution_original_resources'][0]):
            resource['vram_mib']=14000
            fixed=normalize(n)
            self.assertIsNot(fixed,n)
            self.assertEqual(resource['vram_mib'],14000)
            resource['vram_mib']=10240
        n['resource_variants']=[dict(gpu_count=1,vram_mib=14000)]
        self.assertEqual(normalize(n)['resource_variants'][0]['vram_mib'],10240)
        n=normalize(n)
        n['metadata']['vram_after_oom_mib']=12288
        self.assertEqual(normalize(n)['resources']['vram_mib'],12288)
    @patch.dict(os.environ,{'RS_MODEL_VRAM_MIB':'10240'})
    def test_normalization_keeps_profile_and_science_consistent(self):
        s=self.spec();before=copy.deepcopy(s);n=normalize(s)
        self.assertEqual(n['resources']['vram_mib'],10240)
        self.assertEqual(n['metadata']['execution_profiles']['a']['resource_contract'],n['resources'])
        self.assertEqual(s,before)
        self.assertIsNone(reservation(dict(kind='rtl_build',resources={'gpu_count':0})))
    @patch.dict(os.environ,{'RS_MODEL_VRAM_MIB':'10240'})
    def test_gpu_oom_increases_only_failed_job_and_not_twice(self):
        s=self.spec();r=dict(failure_class='experiment_oom',status='failed',termination_verified=True,oom_node='a',oom_memory='gpu',oom_reserved_vram_mib=10240)
        n=retry_spec(s,r,1)
        self.assertEqual(normalize(n)['resources']['vram_mib'],12288)
        self.assertEqual(n['metadata']['excluded_hosts'],['a'])
        self.assertEqual(retry_spec(n,r,1)['metadata']['vram_after_oom_mib'],12288)
        r['oom_reserved_vram_mib']=12288
        self.assertEqual(retry_spec(n,r,2)['metadata']['vram_after_oom_mib'],14336)
        r.pop('oom_memory')
        self.assertNotIn('vram_after_oom_mib',retry_spec(s,r,1)['metadata'])

if __name__=='__main__':unittest.main()
