import copy
import hashlib
import json
from pathlib import Path
import tempfile
import time
import unittest

from test_scheduler import node,job,experiment,snapshot,plan
from research_scheduler.store import Store,dumps
from research_scheduler.controller import Controller
from research_scheduler import execution_profiles as ep


class ExecutionPreparationTests(unittest.TestCase):
    def setUp(self):
        self.tmp=tempfile.TemporaryDirectory();self.root=Path(self.tmp.name)
        self.store=Store(self.root/'db')
        for key in ('a','b','control'):
            n=node(str(self.root/key),key=key)
            if key=='control':n['gpus']=[]
            else:n.update(transport='ssh',target=key)
            self.store.register_node(n)
        j=job('consumer');j.update(hosts=['a'],cwd='/original',dataset='data')
        self.store.register_experiment(experiment([j]))
        self.recipe=dict(target='b',copies=[],verify_argv=['true'],execution=dict(cwd='/prepared',argv=['python','/prepared/run.py'],env={'PYTHONNOUSERSITE':'1'},config={'input_profiles':{'b':'b'*64}},input_files=[]),resources={'train':dict(gpu_count=1,vram_mib=2000,cpu=1,ram_mib=1000,gpu_mode='exclusive')},dataset_path='/prepared/data',asset=dict(path='/prepared/SHA256SUMS',sha256='a'*64))
        self.catalog=ep.register(self.store,dict(id='demo-v1',coordinator='control',match=dict(cwd='/original',dataset='data'),targets={'b':self.recipe},max_parallel=1))
        self.controller=Controller(self.store)

    def tearDown(self):
        self.store.db.close();self.tmp.cleanup()

    def consumer(self):return next(j['spec'] for j in self.store.jobs() if j['id']=='consumer')

    def test_unchanged_admission_reuses_only_identical_profile(self):
        j=ep.admit(self.consumer(),'b',self.catalog,self.recipe)
        self.assertIs(ep.admit(j,'b',self.catalog,self.recipe,reuse_unchanged=True),j)
        recipe=copy.deepcopy(self.recipe);recipe['resources']['train']['ram_mib']+=1024
        changed=ep.admit(j,'b',self.catalog,recipe,reuse_unchanged=True)
        self.assertIsNot(changed,j)
        self.assertNotEqual(changed['metadata']['execution_profiles']['b']['resource_contract'],j['metadata']['execution_profiles']['b']['resource_contract'])

    def test_readonly_profile_does_not_copy_large_manifests(self):
        j=self.consumer();j=ep.admit(j,'b',self.catalog,self.recipe)
        before=copy.deepcopy(j)
        from unittest.mock import patch
        with patch.object(ep.copy,'deepcopy',side_effect=AssertionError('planning must not deep-copy')):
            resolved=ep.for_node(j,'b',readonly=True)
        self.assertEqual(resolved['cwd'],'/prepared')
        self.assertEqual(j,before)
        resolved['env']['new']='value';resolved['config']['new']=1
        self.assertEqual(j,before)

    def test_capability_mode_match_and_admission_gate(self):
        catalog=copy.deepcopy(self.catalog)
        catalog['match']['config_modes']=['smoke','train','eval']
        ep.specification(catalog)
        j=self.consumer();j['config']['mode']='score'
        self.assertFalse(ep.matches(dict(spec=j,status='queued'),catalog))
        j['config']['mode']='train'
        self.assertTrue(ep.matches(dict(spec=j,status='queued'),catalog))
        from research_scheduler.planner import fit
        n=self.store.specs('nodes')['b']
        n['labels']['execution_capability_limits']={'data':{'modes':['train'],'catalogs':['demo-v1']}}
        reason,_=fit(j,n,{},[],[],{}, {},time.time())
        self.assertEqual(reason,'execution capability not verified for this operation')
        admitted=ep.admit(j,'b',catalog,catalog['targets']['b'])
        admitted['config']['mode']='score'
        reason,_=fit(admitted,n,{},[],[],{}, {},time.time())
        self.assertEqual(reason,'execution capability not verified for this operation')
        admitted['config']['mode']='train'
        reason,_=fit(admitted,n,{},[],[],{}, {},time.time())
        self.assertNotEqual(reason,'execution capability not verified for this operation')

    def test_future_host_admission_preserves_inline_validation(self):
        j=self.consumer();j['metadata']['inline_validation']={'code':'preflight_then_train()'}
        recipe=copy.deepcopy(self.recipe)
        recipe['execution']['argv']=['python','-B','/prepared/worker.py','--parent','/prepared/parent']
        admitted=ep.admit(j,'b',self.catalog,recipe)
        argv=ep.for_node(admitted,'b')['argv']
        self.assertEqual(argv[:4],['python','-B','-c','preflight_then_train()'])
        self.assertEqual(argv[4],'/prepared/worker.py')

    def succeed(self,wrong=False):
        row=self.store.db.execute('SELECT * FROM execution_preparations').fetchone()
        directory=self.root/'receipt';directory.mkdir()
        recipe=self.catalog['targets']['b']
        receipt=dict(status='complete',node='b',profile='wrong' if wrong else 'demo-v1',recipe_sha256=ep.digest(recipe))
        path=directory/'EXECUTION_READY.json';path.write_text(json.dumps(receipt))
        output=dict(path=str(path),sha256=hashlib.sha256(path.read_bytes()).hexdigest())
        with self.store.db:
            self.store.db.execute("UPDATE jobs SET status='succeeded' WHERE id=?",(row['job'],))
            self.store.db.execute('INSERT INTO attempts(id,job,node,spec,status,created,report) VALUES(?,?,?,?,?,?,?)',('prep-attempt',row['job'],'control',dumps(dict(attempt_dir=str(directory),node_spec={'transport':'local'})),'succeeded',time.time(),dumps({'outputs':{'EXECUTION_READY.json':output}})))

    def test_prepares_once_then_adds_alternative_and_freezes_override(self):
        original=copy.deepcopy(self.consumer())
        ep.tick(self.controller,execute=False)
        self.assertEqual(len(self.store.jobs()),1)
        ep.tick(self.controller,execute=True);ep.tick(self.controller,execute=True)
        self.assertEqual(len(self.store.jobs()),2)
        self.assertEqual(self.consumer()['hosts'],['a'])
        self.succeed();ep.tick(self.controller,execute=True)
        prepared=self.consumer()
        self.assertEqual(prepared['hosts'],['a','b'])
        for key in ('cwd','argv','resources','config','depends_on'):
            self.assertEqual(prepared[key],original[key])
        selected=ep.for_node(prepared,'b')
        self.assertEqual(selected['cwd'],'/prepared')
        self.assertEqual(selected['config']['input_profiles']['b'],'b'*64)
        self.assertEqual(ep.for_node(prepared,'a')['cwd'],'/original')
        request=self.controller.request(dict(job='consumer',node='b',gpus=[self.store.specs('nodes')['b']['gpus'][0]['uuid']],resources=self.catalog['targets']['b']['resources']['train']))
        self.assertEqual(request['cwd'],'/prepared')
        self.assertEqual(request['argv'],['python','/prepared/run.py'])
        self.assertEqual(request['config']['input_profiles']['b'],'b'*64)
        before=dict(self.store.db.execute('SELECT id,spec FROM attempts'))
        ep.tick(self.controller,execute=True)
        self.assertEqual(before,dict(self.store.db.execute('SELECT id,spec FROM attempts')))

    def test_completed_validation_reconciles_without_queued_consumer(self):
        recipe=self.catalog['targets']['b']
        recipe['validation']=dict(argv=['true'],cwd='/prepared',env={},config={},resources=recipe['resources']['train'],outputs=['VALIDATED.json'])
        with self.store.db:self.store.db.execute('UPDATE execution_catalog SET spec=?',(dumps(self.catalog),))
        ep.tick(self.controller,execute=True);self.succeed();ep.tick(self.controller,execute=True)
        validation=next(j for j in self.store.jobs() if j['id'].startswith('EXEC_VERIFY_'))
        before=self.consumer()
        with self.store.db:
            self.store.db.execute("UPDATE jobs SET status='succeeded' WHERE id=?",(validation['id'],))
            self.store.db.execute("UPDATE jobs SET status='running' WHERE id='consumer'")
        ep.tick(self.controller,execute=True)
        self.assertEqual(self.store.db.execute('SELECT state FROM execution_preparations').fetchone()[0],'ready')
        self.assertEqual(self.consumer(),before)

    def test_no_demand_does_not_create_validation(self):
        recipe=self.catalog['targets']['b']
        recipe['validation']=dict(argv=['true'],cwd='/prepared',env={},config={},resources=recipe['resources']['train'],outputs=['VALIDATED.json'])
        with self.store.db:self.store.db.execute('UPDATE execution_catalog SET spec=?',(dumps(self.catalog),))
        ep.tick(self.controller,execute=True);self.succeed()
        with self.store.db:self.store.db.execute("UPDATE jobs SET status='running' WHERE id='consumer'")
        ep.tick(self.controller,execute=True)
        self.assertFalse(any(j['id'].startswith('EXEC_VERIFY_') for j in self.store.jobs()))

    def test_disabled_node_is_not_prepared_or_admitted(self):
        n=self.store.specs('nodes')['b'];n['enabled']=False
        with self.store.db:self.store.db.execute('UPDATE nodes SET spec=? WHERE id=?',(dumps(n),'b'))
        ep.tick(self.controller,execute=True)
        self.assertEqual(len(self.store.jobs()),1)

    def test_excluded_host_is_not_automatically_readmitted(self):
        original=self.consumer();original.setdefault('metadata',{})['excluded_hosts']=['b']
        with self.store.db:self.store.db.execute('UPDATE jobs SET spec=? WHERE id=?',(dumps(original),'consumer'))
        ep.tick(self.controller,execute=True)
        self.assertEqual(len(self.store.jobs()),1)
        self.assertEqual(ep.admit(original,'b',self.catalog,self.recipe),original)

    def test_node_one_gpu_limit_excludes_legacy_two_gpu_recipe(self):
        n=self.store.specs('nodes')['b'];n['labels']['max_gpus_per_job']=1
        self.catalog['targets']['b']['resources']['train']['gpu_count']=2
        with self.store.db:
            self.store.db.execute('UPDATE nodes SET spec=? WHERE id=?',(dumps(n),'b'))
            self.store.db.execute('UPDATE execution_catalog SET spec=?',(dumps(self.catalog),))
        ep.tick(self.controller,execute=True)
        self.assertEqual(len(self.store.jobs()),1)
        self.assertEqual(self.consumer()['hosts'],['a'])

    def test_node_one_gpu_limit_still_prepares_one_gpu_recipe(self):
        n=self.store.specs('nodes')['b'];n['labels']['max_gpus_per_job']=1
        with self.store.db:self.store.db.execute('UPDATE nodes SET spec=? WHERE id=?',(dumps(n),'b'))
        ep.tick(self.controller,execute=True)
        self.assertEqual(len(self.store.jobs()),2)

    def test_audited_ram_budget_preserves_runtime_recipe(self):
        original=copy.deepcopy(self.recipe)
        budget=dict(ram_mib=512,original_ram_mib=1000,source_sha256='a'*64,evidence='rss audit')
        adjusted=ep.budgeted_recipe(self.recipe,{'train':budget})
        self.assertEqual(adjusted['resources']['train']['ram_mib'],512)
        self.assertEqual(self.recipe,original)
        self.assertEqual(adjusted['execution'],original['execution'])
        for change in ({'source_sha256':'b'*64},{'ram_mib':0},{'ram_mib':True},{'original_ram_mib':999},{'evidence':''}):
            with self.assertRaises(ValueError):ep.budgeted_recipe(self.recipe,{'train':dict(budget,**change)})

    def test_budget_is_kept_on_repeated_automatic_admission(self):
        ep.tick(self.controller,execute=True);self.succeed()
        budget=dict(ram_mib=512,original_ram_mib=1000,source_sha256='a'*64,evidence='rss audit')
        with self.store.db:
            self.store.db.execute('INSERT INTO execution_resource_budgets VALUES(?,?,?,?)',('demo-v1','b','train',dumps(budget)))
        for _ in range(2):
            ep.tick(self.controller,execute=True)
            self.assertEqual(self.consumer()['metadata']['execution_profiles']['b']['resource_contract']['ram_mib'],512)

    def test_vram_budget_updates_validation_without_changing_runtime_identity(self):
        recipe=copy.deepcopy(self.recipe)
        recipe['validation']={'resources':copy.deepcopy(recipe['resources']['train'])}
        original=copy.deepcopy(recipe)
        budget=dict(vram_mib=1500,original_vram_mib=2000,source_sha256='a'*64,evidence='measured peak')
        adjusted=ep.budgeted_recipe(recipe,{'train':budget})
        self.assertEqual(adjusted['resources']['train']['vram_mib'],1500)
        self.assertEqual(adjusted['validation']['resources']['vram_mib'],1500)
        self.assertEqual(adjusted['resources']['train']['ram_mib'],1000)
        self.assertEqual(recipe,original)
        for change in ({'vram_mib':True},{'vram_mib':0},{'original_vram_mib':999},{'source_sha256':'b'*64}):
            with self.assertRaises(ValueError):ep.budgeted_recipe(recipe,{'train':dict(budget,**change)})

    def test_new_validation_uses_vram_budget_before_host_admission(self):
        recipe=self.catalog['targets']['b']
        recipe['validation']=dict(argv=['true'],cwd='/prepared',env={},config={},resources=copy.deepcopy(recipe['resources']['train']),outputs=['VALIDATED.json'])
        budget=dict(vram_mib=1500,original_vram_mib=2000,source_sha256='a'*64,evidence='measured peak')
        with self.store.db:
            self.store.db.execute('UPDATE execution_catalog SET spec=?',(dumps(self.catalog),))
            self.store.db.execute('INSERT INTO execution_resource_budgets VALUES(?,?,?,?)',('demo-v1','b','train',dumps(budget)))
        ep.tick(self.controller,execute=True);self.succeed();ep.tick(self.controller,execute=True)
        validation=next(j for j in self.store.jobs() if j['id'].startswith('EXEC_VERIFY_'))
        self.assertEqual(validation['spec']['resources']['vram_mib'],1500)
        self.assertEqual(self.consumer()['hosts'],['a'])

    def test_wrong_receipt_does_not_admit(self):
        ep.tick(self.controller,execute=True);self.succeed(wrong=True);ep.tick(self.controller,execute=True)
        self.assertEqual(self.consumer()['hosts'],['a'])
        self.assertEqual(self.store.db.execute('SELECT state FROM execution_preparations').fetchone()['state'],'verification_failed')

    def test_gpu_validation_is_scheduler_owned_and_gates_candidate_admission(self):
        recipe=self.catalog['targets']['b']
        recipe['validation']=dict(argv=['python','/prepared/validate.py'],cwd='/prepared',env={},config={},resources=recipe['resources']['train'],outputs=['VALIDATED.json'])
        with self.store.db:self.store.db.execute('UPDATE execution_catalog SET spec=?',(dumps(self.catalog),))
        ep.tick(self.controller,execute=True);self.succeed();ep.tick(self.controller,execute=True)
        validation=next(j for j in self.store.jobs() if j['id'].startswith('EXEC_VERIFY_'))
        self.assertEqual(validation['status'],'queued')
        self.assertEqual(validation['spec']['hosts'],['b'])
        self.assertEqual(validation['spec']['kind'],'prepare')
        self.assertEqual(self.consumer()['hosts'],['a'])
        with self.store.db:self.store.db.execute("UPDATE jobs SET status='failed' WHERE id=?",(validation['id'],))
        ep.tick(self.controller,execute=True)
        self.assertEqual(self.consumer()['hosts'],['a'])
        with self.store.db:self.store.db.execute("UPDATE jobs SET status='succeeded' WHERE id=?",(validation['id'],))
        ep.tick(self.controller,execute=True)
        self.assertEqual(self.consumer()['hosts'],['a','b'])

    def test_failed_preparation_is_not_duplicated(self):
        ep.tick(self.controller,execute=True)
        key=self.store.db.execute('SELECT job FROM execution_preparations').fetchone()['job']
        with self.store.db:self.store.db.execute("UPDATE jobs SET status='failed' WHERE id=?",(key,))
        ep.tick(self.controller,execute=True)
        self.assertEqual(len(self.store.jobs()),2);self.assertEqual(self.consumer()['hosts'],['a'])

    def test_verification_after_user_disables_node_does_not_readmit(self):
        ep.tick(self.controller,execute=True);self.succeed()
        n=self.store.specs('nodes')['b'];n['enabled']=False
        with self.store.db:self.store.db.execute('UPDATE nodes SET spec=? WHERE id=?',(dumps(n),'b'))
        ep.tick(self.controller,execute=True);self.assertEqual(self.consumer()['hosts'],['a'])

    def test_scientific_config_override_is_rejected(self):
        recipe=copy.deepcopy(self.catalog['targets']['b']);recipe['execution']['config']={'epochs':1}
        with self.assertRaises(ValueError):ep.admit(self.consumer(),'b',self.catalog,recipe)

    def test_unrestricted_original_hosts_are_not_narrowed(self):
        original=self.consumer();original['hosts']=[]
        after=ep.admit(original,'b',self.catalog,self.catalog['targets']['b'])
        self.assertEqual(after['hosts'],[])

    def test_nonmatching_entry_and_changed_source_are_not_prepared(self):
        catalog=copy.deepcopy(self.catalog);catalog['match']['entry']='/original/other.py'
        row=next(j for j in self.store.jobs() if j['id']=='consumer')
        self.assertFalse(ep.matches(row,catalog))
        catalog['match'].pop('entry');catalog['match']['files']={'/original/worker.py':'a'*64}
        row['spec']['input_files']=[{'path':'/original/worker.py','sha256':'b'*64}]
        self.assertFalse(ep.matches(row,catalog))


class ExecutionPlacementTests(unittest.TestCase):
    def test_planner_uses_profile_assets_and_keeps_gpu_ban(self):
        n=node(key='b');n['datasets']={'data':'/prepared/data'}
        j=experiment([dict(job('consumer'),hosts=['a'],cwd='/original',dataset='data')])['jobs'][0]
        recipe=dict(execution=dict(cwd='/prepared',argv=['python','worker'],env={},config={},input_files=[]),resources={'train':dict(j['resources'])},asset={'path':'/prepared/SHA256SUMS','sha256':'a'*64})
        updated=ep.admit(j,'b',{'id':'demo'},recipe)
        snap=snapshot(n);snap['datasets']={'data':{'path':'/prepared/data','available':True}};snap['assets']={'execution-demo-b':'a'*64}
        rows=plan([updated],nodes={'b':n},snaps={'b':snap})
        self.assertEqual(rows[0]['decision'],'ready')
        for g in n['gpus']:g['enabled']=False
        rows=plan([updated],nodes={'b':n},snaps={'b':snap})
        self.assertNotEqual(rows[0]['decision'],'ready')
