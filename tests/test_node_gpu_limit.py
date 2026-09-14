import unittest
from test_scheduler import job, node, plan


class NodeGPULimitTests(unittest.TestCase):
    def test_per_job_exclusion_does_not_disable_node_for_other_jobs(self):
        n=node();j=job();j['metadata']={'excluded_hosts':[n['id']]}
        row=plan([j],n=n)[0]
        self.assertEqual(row['decision'],'waiting')
        self.assertIn('user excluded host',row['reasons'][n['id']])
        self.assertEqual(plan([job()],n=n)[0]['decision'],'ready')

    def test_rejects_multigpu_on_limited_node(self):
        n=node();n['labels']['max_gpus_per_job']=1
        row=plan([job(gpu_count=2)],n=n)[0]
        self.assertEqual(row['decision'],'waiting')
        self.assertIn('node GPU-per-job limit: 1',row['reasons'][n['id']])

    def test_single_gpu_and_cpu_remain_eligible(self):
        for count in (0,1):
            n=node();n['labels']['max_gpus_per_job']=1
            self.assertEqual(plan([job(gpu_count=count)],n=n)[0]['decision'],'ready')

    def test_unrestricted_node_keeps_multigpu_support(self):
        self.assertEqual(plan([job(gpu_count=2)])[0]['decision'],'ready')

    def test_invalid_limit_is_fail_closed(self):
        for value in (True,0,'1',-1):
            n=node();n['labels']['max_gpus_per_job']=value
            row=plan([job()],n=n)[0]
            self.assertEqual(row['decision'],'waiting')
            self.assertIn('invalid node GPU-per-job limit',row['reasons'][n['id']])

    def test_verified_single_gpu_variant_can_be_selected(self):
        n=node();n['labels']['max_gpus_per_job']=1
        j=job(gpu_count=2);j['resource_variants']=[dict(j['resources'],gpu_count=1)]
        row=plan([j],n=n)[0]
        self.assertEqual(row['decision'],'ready')
        self.assertEqual(row['resources']['gpu_count'],1)

    def test_exception_requires_exact_job_and_diagnostic_kind(self):
        n=node();n['labels'].update(max_gpus_per_job=1,diagnostic_gpu_count_overrides={'trace':2})
        j=job(key='trace',gpu_count=2);j['metadata']={'diagnostic_only':True}
        self.assertEqual(plan([j],n=n)[0]['decision'],'waiting')
        j['kind']='prepare'
        self.assertEqual(plan([j],n=n)[0]['decision'],'ready')
        j['id']='another-trace'
        self.assertEqual(plan([j],n=n)[0]['decision'],'waiting')

    def test_unmarked_prepare_cannot_use_exception(self):
        n=node();n['labels'].update(max_gpus_per_job=1,diagnostic_gpu_count_overrides={'trace':2})
        j=job(key='trace',gpu_count=2);j['kind']='prepare'
        self.assertEqual(plan([j],n=n)[0]['decision'],'waiting')
