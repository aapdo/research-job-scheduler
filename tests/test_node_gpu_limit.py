import unittest
from test_scheduler import job, node, plan, reservation


class WeightedNodeJobLimitTests(unittest.TestCase):
    def test_one_server_slot_admits_three_evals_not_four(self):
        n=node();n['max_jobs']=1
        n['gpus'].extend(dict(n['gpus'][0], uuid=f'GPU-a-{i}', index=i)
                         for i in range(2, 4))
        attempts=[]
        for i in range(3):
            a=reservation(n,key=f'old{i}',gpu=i)
            a['spec']['job_kind']='eval'
            attempts.append(a)
        candidate=job('fourth',vram=1000);candidate['kind']='eval'
        self.assertIn('node job slots reserved',
                      plan([candidate],n=n,attempts=attempts)[0]['reasons'][n['id']])
        self.assertEqual(plan([candidate],n=n,attempts=attempts[:2])[0]['decision'],
                         'ready')

    def test_two_slots_admit_one_train_and_three_evals(self):
        n=node();n['max_jobs']=2
        n['gpus'].extend(dict(n['gpus'][0], uuid=f'GPU-a-{i}', index=i)
                         for i in range(2, 5))
        train=reservation(n,key='train',gpu=0)
        train['spec']['job_kind']='train'
        evals=[]
        for i in (1, 2, 3):
            a=reservation(n,key=f'eval{i}',gpu=i)
            a['spec']['job_kind']='eval'
            evals.append(a)
        candidate=job('next',vram=1000);candidate['kind']='eval'
        self.assertEqual(plan([candidate],n=n,attempts=[train,*evals[:2]])[0]['decision'],
                         'ready')
        self.assertIn('node job slots reserved',
                      plan([candidate],n=n,attempts=[train,*evals])[0]['reasons'][n['id']])

    def test_gpu_less_control_keeps_integer_slot_limit(self):
        n=node();n['max_jobs']=1;n['gpus']=[]
        old=reservation(node(),key='old')
        old['spec']['job_kind']='eval'
        old['spec']['gpus']=[]
        old['spec']['resources']['gpu_count']=0
        candidate=job('second',gpu_count=0);candidate['kind']='eval'
        self.assertIn('node job slots reserved',
                      plan([candidate],n=n,attempts=[old])[0]['reasons'][n['id']])


class NodeGPULimitTests(unittest.TestCase):
    def test_eval_weight_does_not_relax_physical_gpu_process_cap(self):
        n=node();n['gpus']=n['gpus'][:1];n['max_jobs']=10
        n['gpus'][0]['memory_mib']=48000
        n['policy'].update(allow_gpu_sharing=True,max_shared_jobs_per_gpu=3,
                           gpu_margin_mib=100)
        attempts=[]
        for i,kind in enumerate(('eval','eval')):
            a=reservation(n,key='old'+str(i),gpu=0);a['report']={'ready':True}
            a['spec']['job_kind']=kind
            a['spec']['resources'].update(vram_mib=3000,gpu_mode='shared')
            attempts.append(a)
        e=job('another-eval',vram=1000);e['kind']='eval';e['resources']['gpu_mode']='shared'
        self.assertEqual(plan([e],n=n,attempts=attempts)[0]['decision'],'ready')
        extra=reservation(n,key='old2',gpu=0);extra['report']={'ready':True}
        extra['spec']['job_kind']='eval'
        extra['spec']['resources'].update(vram_mib=3000,gpu_mode='shared')
        self.assertIn('requested per-device VRAM',
                      plan([e],n=n,attempts=[*attempts,extra])[0]['reasons'][n['id']])

    def test_eval_only_caps_allow_five_on_two_gpus(self):
        n=node();n['max_jobs']=7;n['policy'].update(allow_gpu_sharing=True,max_shared_jobs_per_gpu=1)
        n['labels'].update(max_eval_jobs=5,max_eval_jobs_per_gpu=3)
        attempts=[]
        for i,gpu in enumerate((0,0,1,1)):
            a=reservation(n,key='old'+str(i),gpu=gpu);a['report']={'ready':True};a['spec']['job_kind']='eval';a['spec']['resources'].update(vram_mib=5000,gpu_mode='shared');attempts.append(a)
        e=job('fifth',vram=1000);e['kind']='eval';e['resources']['gpu_mode']='shared'
        self.assertEqual(plan([e],n=n,attempts=attempts)[0]['decision'],'ready')
        a=reservation(n,key='old4',gpu=0);a['report']={'ready':True};a['spec']['job_kind']='eval';a['spec']['resources'].update(vram_mib=5000,gpu_mode='shared');attempts.append(a)
        self.assertIn('node eval job cap reached',plan([e],n=n,attempts=attempts)[0]['reasons'][n['id']])
        t=job('train',vram=1000);t['resources']['gpu_mode']='shared'
        self.assertIn('requested per-device VRAM',plan([t],n=n,attempts=attempts[:4])[0]['reasons'][n['id']])

    def test_mixed_gpu_cap_blocks_third_job(self):
        n=node();n['gpus']=n['gpus'][:1];n['max_jobs']=4
        n['policy'].update(allow_gpu_sharing=True,max_shared_jobs_per_gpu=2)
        n['labels'].update(max_eval_jobs_per_gpu=3,max_mixed_jobs_per_gpu=2)
        attempts=[]
        for i,kind in enumerate(('train','eval')):
            a=reservation(n,key='old'+str(i),gpu=0);a['report']={'ready':True}
            a['spec']['job_kind']=kind;a['spec']['resources'].update(vram_mib=3000,gpu_mode='shared');attempts.append(a)
        e=job('third',vram=1000);e['kind']='eval';e['resources']['gpu_mode']='shared'
        self.assertIn('requested per-device VRAM',plan([e],n=n,attempts=attempts)[0]['reasons'][n['id']])

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
