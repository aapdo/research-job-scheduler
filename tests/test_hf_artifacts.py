"""Offline HF transfers: immutable revisions, hashes, paths and admission gates."""
import copy
import hashlib
import json
import shutil
import sys
import tempfile
import types
import unittest
from pathlib import Path
from unittest.mock import patch

sys.path.insert(0, str(Path(__file__).resolve().parents[1]/'src'))
from research_scheduler import hf_worker
from research_scheduler.artifacts import hf_spec, campaign_for, tick, reservations, publication_summary
from research_scheduler.notifications import register_campaign
from research_scheduler.controller import Controller
from research_scheduler.store import Store, dumps
from test_scheduler import node, snapshot, job, experiment, plan, reservation


class FakeAdd:
    def __init__(self, path_in_repo, path_or_fileobj):
        self.path, self.content = path_in_repo, path_or_fileobj


class Hub:
    def __init__(self, root):
        self.root = root
        self.revision = 'a'*40
        self.calls = []

    def create_commit(self, **kwargs):
        self.calls.append(kwargs)
        for op in kwargs['operations']:
            target = self.root/op.path
            target.parent.mkdir(parents=True, exist_ok=True)
            target.write_bytes(op.content if isinstance(op.content, bytes) else Path(op.content).read_bytes())
        return types.SimpleNamespace(oid=self.revision)

    def download(self, **kwargs):
        assert kwargs['revision'] == self.revision
        return self.root/kwargs['filename']


class HFWorkerTests(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.root = Path(self.temp.name)
        self.source = self.root/'source'
        self.source.mkdir()
        (self.source/'weights.bin').write_bytes(b'checkpoint')
        (self.source/'result.json').write_text(json.dumps(dict(checkpoint=str(self.source/'weights.bin'))))
        self.config = dict(source_root=str(self.source), patterns=['result.json','weights.bin'],
            outputs={'result.json':{'sha256':hf_worker.digest(self.source/'result.json')}},
            relocate_json=['result.json'], attempt='first.123', job='first', campaign='study',
            hf=hf_spec({'repo_id':'test/results'}))
        self.hub = Hub(self.root/'hub')
        fake = types.ModuleType('huggingface_hub')
        fake.HfApi = object
        fake.CommitOperationAdd = FakeAdd
        fake.hf_hub_download = self.hub.download
        self.mock = patch.dict(sys.modules, huggingface_hub=fake)
        self.mock.start()

    def tearDown(self):
        self.mock.stop()
        self.temp.cleanup()

    def test_publish_and_restore_hashes_and_relocate_only_declared_json(self):
        receipt = hf_worker.upload(self.config, self.hub)
        self.assertEqual(receipt['revision'], 'a'*40)
        self.assertIn('/tree/'+'a'*40+'/', receipt['url'])
        restored = hf_worker.download(dict(receipt=receipt,destination=str(self.root/'other')), self.hub.download)
        self.assertEqual((self.root/'other/weights.bin').read_bytes(), b'checkpoint')
        self.assertEqual(json.loads((self.root/'other/result.json').read_text())['checkpoint'],
                         str(self.root/'other/weights.bin'))
        self.assertEqual(restored['derived']['result.json']['original_sha256'],
                         self.config['outputs']['result.json']['sha256'])
        self.assertEqual(hf_worker.digest(self.source/'result.json'),
                         self.config['outputs']['result.json']['sha256'])

    def test_missing_checkpoint_in_descriptor_fails_before_remote_write(self):
        config = dict(self.config,patterns=['result.json'])
        with self.assertRaises(ValueError):hf_worker.upload(config,self.hub)
        self.assertEqual(self.hub.calls,[])

    def test_tampered_download_and_mutable_revision_are_rejected(self):
        receipt=hf_worker.upload(self.config,self.hub)
        (self.hub.root/receipt['path']/'weights.bin').write_bytes(b'corrupt')
        with self.assertRaises(ValueError):
            hf_worker.download(dict(receipt=receipt,destination=str(self.root/'bad')),self.hub.download)
        with self.assertRaises(ValueError):
            hf_worker.download(dict(receipt=dict(receipt,revision='main'),destination=str(self.root/'bad2')),self.hub.download)

    def test_output_mutation_symlink_and_traversal_are_rejected(self):
        (self.source/'result.json').write_text('{}')
        with self.assertRaises(ValueError):hf_worker.collect(self.config)
        (self.source/'escape').symlink_to(self.root/'outside')
        with self.assertRaises(ValueError):hf_worker.collect(dict(self.config,patterns=['escape']))
        with self.assertRaises(ValueError):hf_worker.relative('../escape')

    def test_detached_transfer_lifecycle_unblocks_real_destination_paths(self):
        s=Store(self.root/'state.db')
        a,b=node(root=str(self.root/'a')),node(root=str(self.root/'b'),key='b')
        a['hf']=b['hf']={'python':sys.executable}
        b.update(transport='ssh',target='simulated-b')
        s.register_node(a);s.register_node(b)
        first=job('first',gpu_count=0);first.update(outputs=['result.json'],hf_artifacts=['weights.bin'],
                                                 hf_relocate_json=['result.json'])
        child=job('child',gpu_count=0,deps=['first']);child.update(hosts=['b'],argv=['cat','{dep:first}/weights.bin'])
        s.register_experiment(experiment([first,child]))
        register_campaign(s,dict(id='study',name='Study',rq='why',projects=['general'],hf={'repo_id':'test/results'}))
        with s.db:
            s.db.execute("UPDATE jobs SET status='succeeded' WHERE id='first'")
            s.db.execute('INSERT INTO attempts(id,job,node,spec,status,created,report) VALUES(?,?,?,?,?,?,?)',
                ('first.123','first','a',dumps(dict(attempt_dir=str(self.source),node_spec=a,startup_group='')),
                 'succeeded',0,dumps({'outputs':self.config['outputs']})))
            for n in (a,b):s.db.execute('INSERT INTO snapshots VALUES(?,?)',(n['id'],dumps(snapshot(n))))
        parent=self
        class Transfers:
            def __init__(self):self.reports={}
            def call(self,n,action,request):
                if action=='launch':
                    cfg=request['config']
                    artifact=(hf_worker.upload(cfg,parent.hub) if cfg['direction']=='upload'
                              else hf_worker.download(cfg,parent.hub.download))
                    self.reports[request['id']]=dict(status='succeeded',artifact=artifact)
                    return {'status':'starting'}
                if action=='artifact_status':return self.reports[request['id']]
                raise AssertionError(action)
        transport=Transfers();c=Controller(s,transport)
        self.assertEqual(c.plan()[0]['decision'],'waiting')
        with s.lock():tick(c,execute=False)
        self.assertEqual(transport.reports,{})
        with s.lock():tick(c,execute=True)
        self.assertEqual(len(reservations(s)),1)
        self.assertEqual(c.plan()[0]['decision'],'waiting')
        with s.lock():tick(c,execute=True)
        self.assertEqual(len(transport.reports),2)
        with s.lock():tick(c,execute=True)
        self.assertEqual(reservations(s),[])
        self.assertEqual(c.plan()[0]['decision'],'ready')
        request=c.request(c.plan()[0])
        self.assertEqual(Path(request['argv'][1]).read_bytes(),b'checkpoint')
        self.assertIn('hf_artifact',s.attempts()[0]['report'])
        # Restarting/extra cycles preserve completed transfer receipts.
        with s.lock():tick(Controller(s,transport),execute=True)
        self.assertEqual(len(transport.reports),2)
        s.db.close()


class HFIntegrationTests(unittest.TestCase):
    def test_hf_binding_allows_cross_node_only_after_download_receipt(self):
        a,b=node(),node(key='b')
        done=reservation(a,key='first');done.update(status='succeeded',released=True,report={})
        done['spec']['node_spec']=a
        child=job('child',deps=['first'])
        args=dict(nodes={'b':b},snaps={'b':snapshot(b)},attempts=[done],statuses={'first':'succeeded'})
        done['report']['hf_artifact']={'revision':'a'*40}
        self.assertEqual(plan([job('first'),child],**args)[0]['decision'],'waiting')
        done['artifact_locations']={'b':{'root':'/b/cache'}}
        self.assertEqual(plan([job('first'),child],**args)[0]['decision'],'ready')

    def test_request_uses_relocated_json_hashes_and_all_checkpoint_contracts(self):
        with tempfile.TemporaryDirectory() as root:
            s=Store(Path(root)/'db');s.register_node(node(key='b'))
            first=job('first');child=job('child',deps=['first'])
            child['argv']=['cat','{dep:first}/result.json']
            s.register_experiment(experiment([first,child]))
            source=dict(attempt_dir='/a/private',node_spec=node())
            binding=dict(root='/b/payload',files={'result.json':dict(path='/b/payload/result.json',sha256='1'*64),
                                                'weights':dict(path='/b/payload/weights',sha256='2'*64)})
            with s.db:
                s.db.execute("UPDATE jobs SET status='succeeded' WHERE id='first'")
                s.db.execute('INSERT INTO attempts(id,job,node,spec,status,created,report) VALUES(?,?,?,?,?,?,?)',
                             ('done','first','a',dumps(source),'succeeded',0,'{}'))
                s.db.execute('INSERT INTO artifact_transfers VALUES(?,?,?,?,?,?,?,?)',
                             ('download','done','b','download','succeeded','{}',dumps({'artifact':binding}),0))
            request=Controller(s).request({'job':'child','node':'b','gpus':[]})
            self.assertEqual(request['argv'],['cat','/b/payload/result.json'])
            self.assertEqual(len(request['input_files']),2)
            self.assertNotIn('/a/private',dumps(request['input_files']))
            s.db.close()

    def test_optional_campaign_hf_can_be_added_without_resetting_campaign(self):
        with tempfile.TemporaryDirectory() as root:
            s=Store(Path(root)/'db')
            c=dict(id='campaign',name='Study',rq='why',projects=['general'])
            register_campaign(s,c)
            new=register_campaign(s,dict(c,hf={'repo_id':'user/study'}))
            self.assertEqual(new['hf']['repo_type'],'model')
            self.assertEqual(campaign_for(dict(id='e',project='general'),{'c':new})['id'],'campaign')
            with self.assertRaises(ValueError):hf_spec({'repo_id':'user/study','token':'secret'})
            s.db.close()

    def test_overlapping_monitors_share_one_destination_and_owner(self):
        with tempfile.TemporaryDirectory() as root:
            s=Store(Path(root)/'db')
            s.register_experiment(experiment([job('first')]))
            a=register_campaign(s,dict(id='all',name='All',rq='why',projects=['general'],
                                      hf={'repo_id':'user/results'}))
            b=register_campaign(s,dict(id='subset',name='Subset',rq='why',experiments=['e'],
                                      hf={'repo_id':'user/results'}))
            self.assertEqual(campaign_for(dict(id='e',project='general'),{'subset':b,'all':a})['id'],'all')
            with self.assertRaises(ValueError):
                register_campaign(s,dict(id='conflict',name='Conflict',rq='why',experiments=['e'],
                                         hf={'repo_id':'user/another'}))
            s.db.close()

    def test_needed_high_priority_checkpoint_uploads_before_old_archive(self):
        with tempfile.TemporaryDirectory() as root:
            s=Store(Path(root)/'db');n=node(root=str(Path(root)/'runs'))
            n['hf']={'python':sys.executable};s.register_node(n)
            old=job('old',gpu_count=0);old['outputs']=['weights']
            urgent=job('urgent',gpu_count=0,priority=60000);urgent['outputs']=['weights']
            children=[job('old-child',deps=['old']),job('new-child',deps=['urgent'])]
            s.register_experiment(experiment([old,urgent,*children]))
            register_campaign(s,dict(id='campaign',name='Study',rq='why',projects=['general'],
                                    hf={'repo_id':'user/results'}))
            with s.db:
                s.db.execute("UPDATE jobs SET status='succeeded' WHERE id in ('old','urgent')")
                for index,key in enumerate(('old','urgent')):
                    s.db.execute('INSERT INTO attempts(id,job,node,spec,status,created,report) VALUES(?,?,?,?,?,?,?)',
                        (key+'.done',key,'a',dumps(dict(attempt_dir='/source/'+key,node_spec=n,startup_group='')),
                         'succeeded',index,dumps({'outputs':{'weights':{'sha256':'0'*64}}})))
                s.db.execute('INSERT INTO snapshots VALUES(?,?)',('a',dumps(snapshot(n))))
            class Capture:
                def __init__(self):self.launched=[]
                def call(self,node,action,request):
                    self.launched.append(request['job']);return {'status':'starting'}
            transport=Capture()
            with s.lock():tick(Controller(s,transport),execute=True)
            self.assertEqual(transport.launched,['urgent'])
            s.db.close()

    def test_failed_publication_never_changes_training_success_and_has_finite_retries(self):
        with tempfile.TemporaryDirectory() as root:
            s=Store(Path(root)/'db');n=node(root=str(Path(root)/'runs'))
            n['hf']={'python':sys.executable};s.register_node(n)
            first=job('first');first['outputs']=['weights']
            s.register_experiment(experiment([first]))
            campaign=register_campaign(s,dict(id='campaign',name='Study',rq='why',projects=['general'],
                                               hf={'repo_id':'user/results'}))
            with s.db:
                s.db.execute("UPDATE jobs SET status='succeeded' WHERE id='first'")
                s.db.execute('INSERT INTO attempts(id,job,node,spec,status,created,report) VALUES(?,?,?,?,?,?,?)',
                    ('done','first','a',dumps(dict(attempt_dir='/source',node_spec=n,startup_group='')),
                     'succeeded',0,dumps({'outputs':{'weights':{'sha256':'0'*64}}})))
                s.db.execute('INSERT INTO snapshots VALUES(?,?)',('a',dumps(snapshot(n))))
                for i in range(3):
                    s.db.execute('INSERT INTO artifact_transfers VALUES(?,?,?,?,?,?,?,?)',
                                 (str(i),'done','a','upload','failed','{}','{}',i))
            class NoLaunch:
                def call(self,*args):raise AssertionError('retry budget must prevent launch')
            with s.lock():tick(Controller(s,NoLaunch()),execute=True)
            self.assertEqual(s.jobs()[0]['status'],'succeeded')
            self.assertEqual(publication_summary(s,campaign)['counts']['error'],1)
            s.db.close()


if __name__=='__main__':unittest.main()
