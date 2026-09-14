import copy
import unittest
from research_scheduler.execution_profiles import recovered_validation_retry


class ValidationRuntimeRetryTest(unittest.TestCase):
    def setUp(self):
        self.spec={'max_attempts':1,'metadata':{}}
        self.attempt={'status':'failed','released':1,'report':{'failure_class':'gpu_validation_unavailable','finished':10}}
        self.node={'enabled':True,'labels':{'gpu_runtime_recovery':{'verified_at':20,'boot_id':'b','cuda_verified':True}},'gpus':[{'uuid':'g','enabled':True}],'policy':{}}
        self.snap={'boot_id':'b','received_at':25,'stable_polls':3,'read_ok':True,'d_state':0,'gpus':[{'uuid':'g'}]}
    def retry(self,spec=None):
        return recovered_validation_retry(spec or self.spec,self.attempt,self.node,self.snap,{'phase':'healthy'},30)
    def test_once_per_recovery_and_bounded(self):
        out=self.retry();self.assertEqual(out['max_attempts'],2)
        self.assertEqual(self.spec['metadata'],{})
        self.assertIsNone(self.retry(out))
        self.node['labels']['gpu_runtime_recovery']['verified_at']=28
        out=self.retry(out);self.assertEqual(len(out['metadata']['validation_recovery_retries']),2)
        self.node['labels']['gpu_runtime_recovery']['verified_at']=29
        self.assertIsNone(self.retry(out))
    def test_model_errors_are_not_retried(self):
        self.attempt['report']['failure_class']='experiment_oom';self.assertIsNone(self.retry())
    def test_incomplete_recovery_is_rejected(self):
        for key,value in [('stable_polls',2),('gpus',[]),('received_at',-100),('boot_id','other'),('d_state',1)]:
            old=copy.deepcopy(self.snap);self.snap[key]=value
            self.assertIsNone(self.retry());self.snap=old
        self.attempt['released']=0;self.assertIsNone(self.retry())

if __name__=='__main__':unittest.main()
