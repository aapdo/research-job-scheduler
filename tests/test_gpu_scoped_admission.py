import time
import unittest
from test_scheduler import node, snapshot, job, plan, reservation
from research_scheduler.planner import workload_node_rank


class GPUScopedAdmissionTests(unittest.TestCase):
    def test_shared_workload_pool_rank(self):
        self.assertLess(workload_node_rank('train','rp2'),workload_node_rank('train','farm9-gui2'))
        self.assertLess(workload_node_rank('train','farm9-gui2'),workload_node_rank('train','lab1'))
        self.assertLess(workload_node_rank('train','lab1'),workload_node_rank('train','farm6'))
        self.assertLess(workload_node_rank('train','farm6'),workload_node_rank('train','farm7'))
        self.assertLess(workload_node_rank('eval','lab1'),workload_node_rank('eval','rp2'))
        self.assertLess(workload_node_rank('eval','rp2'),workload_node_rank('eval','farm9-gui2'))
        self.assertLess(workload_node_rank('eval','farm9-gui2'),workload_node_rank('eval','farm8-gui2'))
        self.assertLess(workload_node_rank('eval','lab2'),workload_node_rank('eval','lab3'))
        self.assertLess(workload_node_rank('eval','lab6'),workload_node_rank('eval','lab8'))
        self.assertLess(workload_node_rank('train','farm7'),workload_node_rank('train','lab2'))
        self.assertEqual(workload_node_rank('train','cps2-model'),(3,999))
        self.assertEqual(workload_node_rank('eval','cps1-model'),(3,999))

    def shared_node(self,key):
        value=node(key=key);value['max_jobs']=20
        value['policy'].update(allow_gpu_sharing=True,max_shared_jobs_per_gpu=4,
                               max_shared_gpu_percent=100)
        return value

    def active(self,n,gpu,kind,key):
        value=reservation(n,key=key,gpu=gpu)
        value['spec']['job_kind']=kind
        value['spec']['resources'].update(gpu_mode='shared',vram_mib=1000)
        value['report']={'ready':True}
        return value

    def candidate(self,kind):
        value=job('next',vram=1000);value['kind']=kind
        value['resources']['gpu_mode']='shared'
        return value

    def test_hot_or_missing_gpu_does_not_block_cool_neighbor(self):
        n = node(); n['policy']['temperature_scope'] = 'gpu'
        for temperature in (None, 80, 85, 95):
            s = snapshot(n); s['gpus'][0]['temperature_c'] = temperature
            s['gpus'][0]['processes'] = [123]
            r = plan([job()], n=n, snap=s)[0]
            self.assertEqual(r['gpus'], ['GPU-a-1'])

    def test_shared_utilization_limit_does_not_relax_exclusive_or_vram(self):
        n = node(); n['policy'].update(allow_gpu_sharing=True, max_shared_gpu_percent=100)
        s = snapshot(n)
        for g in s['gpus']: g['util_percent'] = 99
        self.assertEqual(plan([job()], n=n, snap=s)[0]['decision'], 'waiting')
        j = job(); j['resources']['gpu_mode'] = 'shared'
        self.assertEqual(plan([j], n=n, snap=s)[0]['decision'], 'ready')
        for g in s['gpus']: g['used_mib'] = 23500
        self.assertEqual(plan([j], n=n, snap=s)[0]['decision'], 'waiting')

    def test_disabled_uuid_cannot_be_admitted_even_if_enabled_flag_is_true(self):
        n = node(); n['policy']['disabled_gpu_uuids'] = ['GPU-a-0']
        self.assertEqual(plan([job()], n=n)[0]['gpus'], ['GPU-a-1'])

    def test_priority_only_prefers_healthy_eligible_node(self):
        a, b = node(), node(key='b'); b['admission_priority'] = 1
        snaps = {'a': snapshot(a), 'b': snapshot(b)}
        self.assertEqual(plan([job()], nodes={'a': a, 'b': b}, snaps=snaps)[0]['node'], 'b')
        snaps['b']['read_ok'] = False
        self.assertEqual(plan([job()], nodes={'a': a, 'b': b}, snaps=snaps)[0]['node'], 'a')

    def test_gpu_load_outweighs_one_priority_tier(self):
        high, lower = node(key='high'), node(key='lower')
        high['gpus']=high['gpus'][:1];lower['gpus']=lower['gpus'][:1]
        high['admission_priority']=400;lower['admission_priority']=300
        for n in (high,lower):
            n['policy'].update(allow_gpu_sharing=True,max_shared_jobs_per_gpu=3)
        old=reservation(high,key='old',gpu=0);old['report']={'ready':True}
        old['spec']['job_kind']='eval';old['spec']['resources'].update(gpu_mode='shared',vram_mib=1000)
        candidate=job('next',vram=1000);candidate['kind']='eval';candidate['resources']['gpu_mode']='shared'
        row=plan([candidate],nodes={'high':high,'lower':lower},
                 snaps={'high':snapshot(high),'lower':snapshot(lower)},attempts=[old])[0]
        self.assertEqual(row['node'],'lower')

    def test_server_weighted_load_is_normalized_by_gpu_count(self):
        a,b=node(key='a'),node(key='b')
        b['gpus']=[dict(b['gpus'][0],uuid='GPU-b-'+str(i),index=i) for i in range(8)]
        old_eval=reservation(a,key='eval',gpu=0);old_eval['spec']['job_kind']='eval'
        old_train=reservation(b,key='train',gpu=0);old_train['spec']['job_kind']='train'
        candidate=job('next',vram=1000);candidate['kind']='eval'
        row=plan([candidate],nodes={'a':a,'b':b},
                 snaps={'a':snapshot(a),'b':snapshot(b)},
                 attempts=[old_eval,old_train])[0]
        # Both nodes have one reservation and an empty selected GPU.  The
        # eight-GPU node has less projected weighted work per GPU.
        self.assertEqual(row['node'],'b')

    def test_better_stabilizing_node_gets_only_bounded_grace(self):
        ready,better=node(key='a'),node(key='b')
        old=reservation(ready,key='old',gpu=0)
        now=time.time()
        ready_snap=snapshot(ready,now)
        better_snap=snapshot(better,now)
        better['policy']['stable_polls']=3
        better_snap.update(stable_polls=1,stable_since=now-20)
        for gpu in better_snap['gpus']:gpu['stable_polls']=1
        row=plan([job('next',vram=1000)],nodes={'a':ready,'b':better},
                 snaps={'a':ready_snap,'b':better_snap},attempts=[old])[0]
        self.assertEqual(row['decision'],'waiting')
        self.assertIn('stabilizing',row['reasons']['b'])
        better_snap['stable_since']=now-181
        row=plan([job('next',vram=1000)],nodes={'a':ready,'b':better},
                 snaps={'a':ready_snap,'b':better_snap},attempts=[old])[0]
        self.assertEqual(row['decision'],'ready')
        self.assertEqual(row['node'],'a')

    def test_train_pool_fills_each_server_breadth_then_second_round(self):
        farm9=self.shared_node('farm9-gui2');lab1=self.shared_node('lab1')
        nodes={n['id']:n for n in (farm9,lab1)}
        snaps={key:snapshot(value) for key,value in nodes.items()}
        active=[self.active(farm9,0,'train','f0')]
        row=plan([self.candidate('train')],nodes=nodes,snaps=snaps,attempts=active)[0]
        self.assertEqual((row['node'],row['gpus']),('farm9-gui2',['GPU-farm9-gui2-1']))
        active=[self.active(n,gpu,'train',n['id']+str(gpu))
                for n in (farm9,lab1) for gpu in range(2)]
        row=plan([self.candidate('train')],nodes=nodes,snaps=snaps,attempts=active)[0]
        self.assertEqual(row['node'],'farm9-gui2')

    def test_all_train_servers_reach_depth_one_before_any_second_train(self):
        rp2=self.shared_node('rp2')
        farm9=self.shared_node('farm9-gui2');lab1=self.shared_node('lab1')
        farm6=self.shared_node('farm6');farm7=self.shared_node('farm7')
        nodes={n['id']:n for n in (rp2,farm9,lab1,farm6,farm7)}
        snaps={key:snapshot(value) for key,value in nodes.items()}
        active=[]
        for n in (rp2,farm9,lab1,farm6):
            for gpu in range(2):
                active.append(self.active(n,gpu,'train',n['id']+str(gpu)))
        row=plan([self.candidate('train')],nodes=nodes,snaps=snaps,attempts=active)[0]
        self.assertEqual(row['node'],'farm7')

    def test_dense_train_pool_uses_strict_server_priority(self):
        nodes={key:self.shared_node(key) for key in ('rp2','farm9-gui2','lab1')}
        for priority,key in enumerate(('lab1','farm9-gui2','rp2'),1):
            nodes[key]['admission_priority']=priority*10
        row=plan([self.candidate('train')],nodes=nodes,
                 snaps={key:snapshot(value) for key,value in nodes.items()})[0]
        self.assertEqual(row['node'],'rp2')

    def test_primary_empty_gpu_beats_second_train_on_higher_priority_node(self):
        rp2=self.shared_node('rp2');rp2['gpus']=rp2['gpus'][:1];rp2['admission_priority']=600
        farm9=self.shared_node('farm9-gui2');farm9['gpus']=farm9['gpus'][:1];farm9['admission_priority']=590
        row=plan([self.candidate('train')],nodes={'rp2':rp2,'farm9-gui2':farm9},
                 snaps={'rp2':snapshot(rp2),'farm9-gui2':snapshot(farm9)},
                 attempts=[self.active(rp2,0,'train','first')])[0]
        self.assertEqual((row['node'],row['gpus']),('farm9-gui2',['GPU-farm9-gui2-0']))

    def test_secondary_empty_gpu_precedes_primary_second_train(self):
        rp2=self.shared_node('rp2');rp2['gpus']=rp2['gpus'][:1];rp2['admission_priority']=600
        farm6=self.shared_node('farm6');farm6['gpus']=farm6['gpus'][:1];farm6['admission_priority']=440
        row=plan([self.candidate('train')],nodes={'rp2':rp2,'farm6':farm6},
                 snaps={'rp2':snapshot(rp2),'farm6':snapshot(farm6)},
                 attempts=[self.active(rp2,0,'train','first')])[0]
        self.assertEqual((row['node'],row['gpus']),('farm6',['GPU-farm6-0']))

    def test_stabilizing_empty_primary_gpu_blocks_packing_and_secondary_for_180s(self):
        now=time.time()
        rp2=self.shared_node('rp2');rp2['policy']['stable_polls']=3;rp2['admission_priority']=600
        farm6=self.shared_node('farm6');farm6['admission_priority']=440
        snaps={'rp2':snapshot(rp2,now),'farm6':snapshot(farm6,now)}
        snaps['rp2'].update(stable_polls=1,stable_since=now-20)
        for gpu in snaps['rp2']['gpus']:gpu['stable_polls']=1
        row=plan([self.candidate('train')],nodes={'rp2':rp2,'farm6':farm6},snaps=snaps,
                 attempts=[self.active(rp2,0,'train','first')])[0]
        self.assertEqual(row['decision'],'waiting')
        self.assertIn('stabilizing',row['reasons']['rp2'])
        snaps['rp2']['stable_since']=now-181
        row=plan([self.candidate('train')],nodes={'rp2':rp2,'farm6':farm6},snaps=snaps,
                 attempts=[self.active(rp2,0,'train','first')])[0]
        self.assertEqual(row['node'],'farm6')

    def test_primary_gpu_validation_blocks_secondary_train_spill(self):
        rp2=self.shared_node('rp2');rp2['gpus']=rp2['gpus'][:1];rp2['admission_priority']=600
        farm6=self.shared_node('farm6');farm6['gpus']=farm6['gpus'][:1];farm6['admission_priority']=440
        validation=self.active(rp2,0,'prepare','EXEC_VERIFY_profile')
        validation['report']={'ready':False}
        row=plan([self.candidate('train')],nodes={'rp2':rp2,'farm6':farm6},
                 snaps={'rp2':snapshot(rp2),'farm6':snapshot(farm6)},attempts=[validation])[0]
        self.assertEqual(row['decision'],'waiting')
        self.assertIn('validation in progress',row['reasons']['primary_train_pool'])

    def test_dense_train_pool_skips_busy_or_warm_gpu(self):
        rp2=self.shared_node('rp2');rp2['gpus']=rp2['gpus'][:1]
        farm9=self.shared_node('farm9-gui2');farm9['gpus']=farm9['gpus'][:1]
        snaps={'rp2':snapshot(rp2),'farm9-gui2':snapshot(farm9)}
        snaps['rp2']['gpus'][0]['util_percent']=70
        row=plan([self.candidate('train')],nodes={'rp2':rp2,'farm9-gui2':farm9},snaps=snaps)[0]
        self.assertEqual(row['node'],'farm9-gui2')
        snaps['rp2']['gpus'][0]['util_percent']=0
        snaps['rp2']['gpus'][0]['temperature_c']=80
        row=plan([self.candidate('train')],nodes={'rp2':rp2,'farm9-gui2':farm9},snaps=snaps)[0]
        self.assertEqual(row['node'],'farm9-gui2')

    def test_per_node_train_cap_blocks_third_train_on_same_gpu(self):
        farm9=self.shared_node('farm9-gui2');farm9['gpus']=farm9['gpus'][:1]
        active=[self.active(farm9,0,'train','first'),self.active(farm9,0,'train','second')]
        row=plan([self.candidate('train')],nodes={'farm9-gui2':farm9},
                 snaps={'farm9-gui2':snapshot(farm9)},attempts=active)[0]
        self.assertEqual(row['decision'],'waiting')

    def test_rp2_train_cap_override_allows_three_but_not_four(self):
        rp2=self.shared_node('rp2');rp2['gpus']=rp2['gpus'][:1]
        rp2['policy']['max_shared_jobs_per_gpu']=3
        rp2['labels']['max_train_jobs_per_gpu']=3
        active=[self.active(rp2,0,'train','first'),self.active(rp2,0,'train','second')]
        row=plan([self.candidate('train')],nodes={'rp2':rp2},
                 snaps={'rp2':snapshot(rp2)},attempts=active)[0]
        self.assertEqual(row['decision'],'ready')
        active.append(self.active(rp2,0,'train','third'))
        row=plan([self.candidate('train')],nodes={'rp2':rp2},
                 snaps={'rp2':snapshot(rp2)},attempts=active)[0]
        self.assertEqual(row['decision'],'waiting')
        self.assertIn('requested per-device VRAM',row['reasons']['rp2'])

    def test_rp2_train_cap_override_is_gpu_specific(self):
        rp2=self.shared_node('rp2')
        rp2['policy']['max_shared_jobs_per_gpu']=3
        rp2['labels']['max_train_jobs_per_gpu']=2
        rp2['labels']['max_train_jobs_by_gpu_uuid']={rp2['gpus'][0]['uuid']:3}
        for index,expected in ((0,'ready'),(1,'waiting')):
            single=__import__('copy').deepcopy(rp2)
            active=[self.active(rp2,index,'train','first'),self.active(rp2,index,'train','second')]
            single['gpus']=[rp2['gpus'][index]]
            row=plan([self.candidate('train')],nodes={'rp2':single},
                     snaps={'rp2':snapshot(single)},attempts=active)[0]
            self.assertEqual(row['decision'],expected)

    def test_train_never_spills_to_eval_pool(self):
        farm9=self.shared_node('farm9-gui2');lab1=self.shared_node('lab1')
        farm9['policy']['max_shared_jobs_per_gpu']=2
        lab1['policy']['max_shared_jobs_per_gpu']=2
        lab2=self.shared_node('lab2')
        nodes={n['id']:n for n in (farm9,lab1,lab2)}
        snaps={key:snapshot(value) for key,value in nodes.items()}
        active=[]
        for n in (farm9,lab1):
            for gpu in range(2):
                active.extend(self.active(n,gpu,'train',n['id']+str(gpu)+'-'+str(copy_))
                              for copy_ in range(2))
        row=plan([self.candidate('train')],nodes=nodes,snaps=snaps,attempts=active)[0]
        self.assertEqual(row['decision'],'waiting')
        self.assertIn('workload pool restriction',row['reasons']['lab2'])

    def test_eval_remains_in_eval_pool_after_first_coverage_round(self):
        lab2=self.shared_node('lab2');lab4=self.shared_node('lab4')
        farm7=self.shared_node('farm7')
        nodes={n['id']:n for n in (lab2,lab4,farm7)}
        snaps={key:snapshot(value) for key,value in nodes.items()}
        active=[self.active(lab2,0,'eval','l20')]
        row=plan([self.candidate('eval')],nodes=nodes,snaps=snaps,attempts=active)[0]
        self.assertEqual((row['node'],row['gpus']),('lab2',['GPU-lab2-1']))
        active=[self.active(n,gpu,'eval',n['id']+str(gpu))
                for n in (lab2,lab4) for gpu in range(2)]
        row=plan([self.candidate('eval')],nodes=nodes,snaps=snaps,attempts=active)[0]
        self.assertEqual(row['node'],'lab2')

    def test_lab3_is_eval_only(self):
        lab3=self.shared_node('lab3')
        train=plan([self.candidate('train')],nodes={'lab3':lab3},
                   snaps={'lab3':snapshot(lab3)})[0]
        self.assertEqual(train['decision'],'waiting')
        self.assertIn('workload pool restriction',train['reasons']['lab3'])
        evaluation=plan([self.candidate('eval')],nodes={'lab3':lab3},
                        snaps={'lab3':snapshot(lab3)})[0]
        self.assertEqual(evaluation['decision'],'ready')

    def test_lab8_is_eval_only(self):
        lab8=self.shared_node('lab8')
        train=plan([self.candidate('train')],nodes={'lab8':lab8},
                   snaps={'lab8':snapshot(lab8)})[0]
        self.assertEqual(train['decision'],'waiting')
        self.assertIn('workload pool restriction',train['reasons']['lab8'])
        evaluation=plan([self.candidate('eval')],nodes={'lab8':lab8},
                        snaps={'lab8':snapshot(lab8)})[0]
        self.assertEqual(evaluation['decision'],'ready')

    def test_rp2_farm9_and_lab1_are_dual_role(self):
        for key in ('rp2','farm9-gui2','lab1'):
            with self.subTest(node=key):
                n=self.shared_node(key)
                for kind in ('train','eval'):
                    row=plan([self.candidate(kind)],nodes={key:n},snaps={key:snapshot(n)})[0]
                    self.assertEqual(row['decision'],'ready')

    def test_catalog_job_waits_until_this_node_has_verified_profile(self):
        n=self.shared_node('farm9-gui2')
        pending=self.candidate('train')
        pending['metadata']={'execution_preparation_catalog':'runtime-v1'}
        row=plan([pending],nodes={'farm9-gui2':n},snaps={'farm9-gui2':snapshot(n)})[0]
        self.assertEqual(row['decision'],'waiting')
        self.assertIn('execution profile validation pending',row['reasons']['farm9-gui2'])
        verified=self.candidate('train')
        verified['metadata']={'execution_preparation_catalog':'runtime-v1',
                              'execution_profiles':{'farm9-gui2':{'resource_contract':verified['resources']}}}
        row=plan([verified],nodes={'farm9-gui2':n},snaps={'farm9-gui2':snapshot(n)})[0]
        self.assertEqual(row['decision'],'ready')

    def test_scoped_eval_may_follow_only_its_successful_train_host(self):
        farm7=self.shared_node('farm7');farm6=self.shared_node('farm6')
        producer=self.active(farm7,0,'train','train-attempt')
        producer.update(job='producer',status='succeeded',released=True)
        producer['spec'].update(node_spec=farm7,attempt_dir='/runs/producer')
        producer['report']={'outputs':{'TRAIN_RESULT.json':{
            'path':'/runs/producer/TRAIN_RESULT.json','sha256':'a'*64,'bytes':1}}}
        base=self.candidate('eval');base.update(id='evaluation',depends_on=['producer'],hosts=['farm7'])
        statuses={'producer':'succeeded'}
        blocked=plan([job('producer'),base],nodes={'farm7':farm7,'farm6':farm6},
            snaps={'farm7':snapshot(farm7),'farm6':snapshot(farm6)},attempts=[producer],statuses=statuses)[0]
        self.assertEqual(blocked['decision'],'waiting')
        base['metadata']={'same_host_eval_dependency':'producer'}
        allowed=plan([job('producer'),base],nodes={'farm7':farm7,'farm6':farm6},
            snaps={'farm7':snapshot(farm7),'farm6':snapshot(farm6)},attempts=[producer],statuses=statuses)[0]
        self.assertEqual((allowed['decision'],allowed['node']),('ready','farm7'))
        base['hosts']=['farm6']
        wrong=plan([job('producer'),base],nodes={'farm7':farm7,'farm6':farm6},
            snaps={'farm7':snapshot(farm7),'farm6':snapshot(farm6)},attempts=[producer],statuses=statuses)[0]
        self.assertEqual(wrong['decision'],'waiting')
        self.assertIn('workload pool restriction',wrong['reasons']['farm6'])

    def test_explicit_eval_train_pool_override_requires_matching_hosts(self):
        farm6=self.shared_node('farm6')
        base=self.candidate('eval');base['hosts']=['farm6']
        base['metadata']={'allowed_execution_hosts':['farm6'],
                          'eval_train_pool_override_hosts':['farm6'],
                          'eval_train_pool_override_reason':'operator-approved campaign placement'}
        allowed=plan([base],nodes={'farm6':farm6},snaps={'farm6':snapshot(farm6)})[0]
        self.assertEqual((allowed['decision'],allowed['node']),('ready','farm6'))
        base['metadata']['allowed_execution_hosts']=['rp2']
        blocked=plan([base],nodes={'farm6':farm6},snaps={'farm6':snapshot(farm6)})[0]
        self.assertEqual(blocked['decision'],'waiting')
        self.assertIn('workload pool restriction',blocked['reasons']['farm6'])

    def test_retired_node_cannot_reenter_either_pool(self):
        for key,kind in (('rp1','train'),('rp3','train'),('cps1-model','eval'),
                         ('cps2-model','train'),('farm1','eval'),('farm2','train')):
            with self.subTest(node=key,kind=kind):
                retired=self.shared_node(key)
                row=plan([self.candidate(kind)],nodes={key:retired},
                         snaps={key:snapshot(retired)})[0]
                self.assertEqual(row['decision'],'waiting')
                self.assertIn('workload pool restriction',row['reasons'][key])

    def test_warm_occupied_gpu_blocked_but_other_gpu_usable(self):
        n = node(); n['policy'].update(temperature_scope='gpu', allow_gpu_sharing=True)
        s = snapshot(n); s['gpus'][0]['temperature_c'] = 81
        r = reservation(n); r['spec']['resources']['gpu_mode'] = 'shared'; r['report'] = {'ready': True}
        j = job(); j['resources']['gpu_mode'] = 'shared'
        self.assertEqual(plan([j], n=n, snap=s, attempts=[r])[0]['gpus'], ['GPU-a-1'])


if __name__ == '__main__': unittest.main()
