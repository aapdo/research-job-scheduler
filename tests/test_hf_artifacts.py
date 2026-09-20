"""Offline HF transfers: immutable revisions, hashes, paths and admission gates."""
import copy
import hashlib
import io
import json
import shutil
import sqlite3
import sys
import tempfile
import tarfile
import types
import unittest
from pathlib import Path
from unittest.mock import patch

sys.path.insert(0, str(Path(__file__).resolve().parents[1]/'src'))
from research_scheduler import agent, hf_worker, relay_worker
from research_scheduler.artifacts import (hf_spec, campaign_for, tick, reservations,
    publication_summary, cleanup_archived_sources, start_attempt_archive,
    urgent_archive_destination_staging, rows)
from research_scheduler.notifications import register_campaign
from research_scheduler.controller import Controller
from research_scheduler.store import Store, dumps
from test_scheduler import node, snapshot, job, experiment, plan, reservation


class LocalRelayWorkerTests(unittest.TestCase):
    def test_lab4_report_dependency_staging_precedes_more_archives(self):
        with tempfile.TemporaryDirectory() as root:
            s=Store(Path(root)/'db')
            source=node(key='source');lab4=node(key='lab4')
            source.update(transport='ssh',target='source-host')
            lab4.update(transport='ssh',target='lab4-host')
            s.register_node(source);s.register_node(lab4)
            predecessor=job('predecessor',gpu_count=0);predecessor['outputs']=['RESULT.json']
            report=job('report',deps=['predecessor'],gpu_count=0)
            report.update(kind='analysis',hosts=['lab4'],name='LAB4 report')
            report['resources']=dict(gpu_count=0,gpu_mode='exclusive',vram_mib=0,
                                     cpu=1,ram_mib=1024)
            report['metadata']={
                'report_role':'report',
                'report_execution':'archive_host',
                'report_storage':'lab4-direct-relay',
                'execution_profiles':{'lab4':{
                    'argv':['python3','-c','print(1)'],
                    'cwd':'/lab4',
                    'resource_contract':copy.deepcopy(report['resources']),
                }},
            }
            s.register_experiment(experiment([predecessor,report]))
            with s.db:
                s.db.execute("UPDATE jobs SET status='succeeded' WHERE id='predecessor'")
                s.db.execute('INSERT INTO attempts(id,job,node,spec,status,created,report) VALUES(?,?,?,?,?,?,?)',
                    ('predecessor.done','predecessor','source','{}','succeeded',1,
                     dumps({'outputs':{'RESULT.json':dict(bytes=1,sha256='0'*64)}})))
            self.assertTrue(urgent_archive_destination_staging(s,'lab4'))
            with s.db:
                s.db.execute('INSERT INTO artifact_transfers VALUES(?,?,?,?,?,?,?,?)',
                    ('relay','predecessor.done','lab4','download','succeeded','{}',
                     dumps({'artifact':{'attempt':'predecessor.done','files':{
                         'RESULT.json':dict(bytes=1,sha256='0'*64)}}}),2))
            self.assertFalse(urgent_archive_destination_staging(s,'lab4'))
            s.db.close()

    def test_remote_relay_streaming_does_not_require_rsync(self):
        with tempfile.TemporaryDirectory() as root:
            root=Path(root);destination=root/'payload';payload=b'checkpoint'
            calls=[]
            def remote(argv,**kwargs):
                calls.append(argv)
                kwargs['stdout'].write(payload)
                return types.SimpleNamespace(returncode=0)
            with patch.object(relay_worker.subprocess,'run',side_effect=remote):
                relay_worker.stream_from_remote('farm','/attempt/model',destination)
            self.assertEqual(destination.read_bytes(),payload)
            self.assertFalse(any('rsync' in part for call in calls for part in call))

    def test_agent_freezes_declared_checkpoint_globs_for_dependency_relay(self):
        with tempfile.TemporaryDirectory() as root:
            root=Path(root);(root/'run/epochs/e20').mkdir(parents=True)
            result=root/'RESULT.json';checkpoint=root/'run/epochs/e20/model.pdparams'
            result.write_bytes(b'result');checkpoint.write_bytes(b'checkpoint')
            outputs={'RESULT.json':dict(path=str(result),bytes=result.stat().st_size,
                                        sha256=hf_worker.digest(result))}
            files=agent.dependency_artifacts(root,['run/epochs/*/*.pdparams'],outputs)
            self.assertEqual(set(files),{'RESULT.json','run/epochs/e20/model.pdparams'})
            self.assertEqual(files['run/epochs/e20/model.pdparams']['sha256'],hf_worker.digest(checkpoint))
            with self.assertRaises(ValueError):agent.dependency_artifacts(root,['../escape'],outputs)

    def test_local_relay_copies_and_verifies_declared_outputs(self):
        with tempfile.TemporaryDirectory() as root:
            root=Path(root);source=root/'source';attempt=root/'attempt';destination=root/'destination'
            source.mkdir();attempt.mkdir();payload=b'layout-v1\n';(source/'LAYOUTS.json').write_bytes(payload)
            sha=hashlib.sha256(payload).hexdigest()
            config=dict(source_attempt='layout.done',source_target='@local',source_root=str(source),
                        destination_target='@local',destination_root=str(destination),
                        files={'LAYOUTS.json':dict(bytes=len(payload),sha256=sha)},manifest_sha256=sha)
            (attempt/'config.json').write_text(json.dumps(config))
            with patch.dict('os.environ',{'RS_CONFIG_PATH':str(attempt/'config.json'),
                                           'RS_ATTEMPT_DIR':str(attempt)}):
                relay_worker.main()
            receipt=json.loads((attempt/'HF_RECEIPT.json').read_text())
            self.assertEqual((destination/'LAYOUTS.json').read_bytes(),payload)
            self.assertEqual(receipt['transport'],'controller-local-relay')
            self.assertEqual(receipt['files']['LAYOUTS.json']['sha256'],sha)
            self.assertFalse((attempt/'relay-staging').exists())

    def test_complete_attempt_archive_includes_config_checkpoint_and_allows_verified_cleanup(self):
        with tempfile.TemporaryDirectory() as root:
            root=Path(root);work=root/'runs';source=work/'done';transfer=root/'transfer';destination=root/'lab4'
            source.mkdir(parents=True);transfer.mkdir()
            frozen=dict(id='attempt.done',attempt_dir=str(source),
                        node_spec=dict(work_root=str(work)),job_spec=dict(kind='train'))
            spec_sha=hashlib.sha256(dumps(frozen).encode()).hexdigest()
            request=dict(frozen,spec_sha256=spec_sha)
            (source/'spec.json').write_text(json.dumps(request))
            (source/'state.json').write_text(json.dumps(
                dict(attempt='attempt.done',status='succeeded',finished=1)))
            (source/'config.json').write_text('{"epochs":5}\n')
            (source/'run/epochs/e5').mkdir(parents=True)
            (source/'run/epochs/e5/model.pdparams').write_bytes(b'checkpoint')
            config=dict(mode='attempt-archive',source_attempt='attempt.done',source_node='farm9',
                        source_target='@local',source_root=str(source),source_spec_sha256=spec_sha,
                        destination_target='@local',destination_root=str(destination),
                        campaigns=['campaign'],experiment='experiment',job='job',
                        transport_route='controller-local-staging')
            (transfer/'config.json').write_text(json.dumps(config))
            with patch.dict('os.environ',{'RS_CONFIG_PATH':str(transfer/'config.json'),
                                           'RS_ATTEMPT_DIR':str(transfer)}):
                relay_worker.main()
            receipt=json.loads((transfer/'HF_RECEIPT.json').read_text())
            self.assertTrue(receipt['complete_attempt'])
            self.assertEqual(set(receipt['files']),{
                'spec.json','state.json','config.json','run/epochs/e5/model.pdparams'})
            self.assertEqual((destination/'run/epochs/e5/model.pdparams').read_bytes(),b'checkpoint')
            with patch.object(agent, 'assert_attempt_not_open'):
                result=agent.cleanup_archived_attempt(dict(request,archive_receipt=receipt))
            self.assertTrue(result['deleted'])
            self.assertFalse(source.exists())

    def test_archive_receipt_is_a_dependency_location(self):
        with tempfile.TemporaryDirectory() as root:
            s=Store(Path(root)/'db');first=job('first',gpu_count=0);first['outputs']=['RESULT.json']
            s.register_experiment(experiment([first]))
            receipt=dict(attempt='first.done',root='/lab4/archive/first.done',complete_attempt=True,
                         files={'RESULT.json':dict(path='/lab4/archive/first.done/RESULT.json',
                                                   bytes=1,sha256='0'*64)})
            with s.db:
                s.db.execute("UPDATE jobs SET status='succeeded' WHERE id='first'")
                s.db.execute('INSERT INTO attempts(id,job,node,spec,status,created,report) VALUES(?,?,?,?,?,?,?)',
                    ('first.done','first','farm9',dumps(dict(attempt_dir='/farm9/first.done')),
                     'succeeded',1,dumps({'outputs':{'RESULT.json':dict(bytes=1,sha256='0'*64)}})))
                s.db.execute('INSERT INTO artifact_transfers VALUES(?,?,?,?,?,?,?,?)',
                    ('archive','first.done','lab4','archive','succeeded','{}',
                     dumps({'artifact':receipt}),2))
            self.assertEqual(s.attempts()[0]['artifact_locations']['lab4']['root'],receipt['root'])
            s.db.close()

    def test_source_cleanup_waits_for_recent_attempt_users(self):
        with tempfile.TemporaryDirectory() as root:
            s=Store(Path(root)/'db');source=node(key='source');lab4=node(key='lab4')
            lab4.update(transport='ssh',target='lab4-host')
            s.register_node(source);s.register_node(lab4)
            first=job('first',gpu_count=0);consumer=job('consumer',gpu_count=0)
            s.register_experiment(experiment([first,consumer]))
            source_root='/source/attempt.done';now=10_000
            receipt=dict(attempt='attempt.done',root='/lab4/attempt.done',attempt_archive=True,
                         complete_attempt=True,manifest_sha256='1'*64,
                         files={'spec.json':dict(path='/lab4/attempt.done/spec.json',bytes=1,sha256='0'*64)})
            with s.db:
                s.db.execute("UPDATE jobs SET status='succeeded' WHERE id='first'")
                s.db.execute('INSERT INTO attempts(id,job,node,spec,status,created,report) VALUES(?,?,?,?,?,?,?)',
                    ('attempt.done','first','source',dumps(dict(id='attempt.done',attempt_dir=source_root,
                     node_spec=source,spec_sha256='2'*64)),'succeeded',1,dumps({'finished':1})))
                s.db.execute('INSERT INTO attempts(id,job,node,spec,status,created,report) VALUES(?,?,?,?,?,?,?)',
                    ('consumer.recent','consumer','lab4',dumps(dict(attempt_dir='/consumer',
                     input_files=[{'path':source_root+'/RESULT.json'}])),'succeeded',now-60,dumps({'finished':now-60})))
                s.db.execute('INSERT INTO artifact_transfers VALUES(?,?,?,?,?,?,?,?)',
                    ('archive','attempt.done','lab4','archive','succeeded','{}',
                     dumps({'artifact':receipt,'finished':1}),1))
            class Capture:
                def __init__(self):self.calls=[]
                def call(self,node,action,request):
                    self.calls.append((node['id'],action,request))
                    if action == 'verify_archived_attempt':
                        return {'verified':True,'manifest_sha256':'1'*64}
                    return {'deleted':True}
            transport=Capture();controller=Controller(s,transport)
            with patch.object(__import__('research_scheduler.artifacts',fromlist=['time']).time,'time',return_value=now):
                self.assertFalse(cleanup_archived_sources(
                    controller,s.specs('nodes'),{'delete_after_idle_s':1800}))
                with s.db:s.db.execute("DELETE FROM attempts WHERE id='consumer.recent'")
                self.assertTrue(cleanup_archived_sources(
                    controller,s.specs('nodes'),{'delete_after_idle_s':1800}))
            self.assertEqual([(n,a) for n,a,_ in transport.calls],
                             [('lab4','verify_archived_attempt'),('source','cleanup_archived_attempt')])
            s.db.close()

    def test_one_unverifiable_source_does_not_starve_later_cleanup(self):
        with tempfile.TemporaryDirectory() as root:
            s=Store(Path(root)/'db');lab4=node(key='lab4')
            sources=[node(key='source-a'),node(key='source-b')]
            for n in [lab4,*sources]:
                n.update(transport='ssh',target=n['id']+'-host')
                s.register_node(n)
            jobs=[job('first',gpu_count=0),job('second',gpu_count=0)]
            s.register_experiment(experiment(jobs))
            for index,(j,n) in enumerate(zip(jobs,sources),1):
                attempt=j['id']+'.done';sha=str(index)*64
                receipt=dict(attempt=attempt,root='/lab4/'+attempt,attempt_archive=True,
                             complete_attempt=True,manifest_sha256=sha,
                             files={'spec.json':dict(path='/lab4/'+attempt+'/spec.json',bytes=1,
                                                     sha256='0'*64)})
                with s.db:
                    s.db.execute("UPDATE jobs SET status='succeeded' WHERE id=?",(j['id'],))
                    s.db.execute('INSERT INTO attempts(id,job,node,spec,status,created,report) VALUES(?,?,?,?,?,?,?)',
                        (attempt,j['id'],n['id'],dumps(dict(id=attempt,attempt_dir='/'+attempt,
                         node_spec=n,spec_sha256='2'*64)),'succeeded',index,dumps({'finished':index})))
                    s.db.execute('INSERT INTO artifact_transfers VALUES(?,?,?,?,?,?,?,?)',
                        ('archive-'+str(index),attempt,'lab4','archive','succeeded','{}',
                         dumps({'artifact':receipt,'finished':index}),index))
            class Capture:
                def call(self,n,action,request):
                    if action=='verify_archived_attempt':
                        return {'verified':True,'manifest_sha256':request['archive_receipt']['manifest_sha256']}
                    if n['id']=='source-a':raise PermissionError('cannot inspect proc fd')
                    return {'deleted':True}
            controller=Controller(s,Capture())
            with patch.object(__import__('research_scheduler.artifacts',fromlist=['time']).time,
                              'time',return_value=10_000):
                self.assertTrue(cleanup_archived_sources(
                    controller,s.specs('nodes'),{'archive_node':'lab4','delete_after_idle_s':300}))
            first_report=json.loads(s.db.execute(
                "SELECT report FROM artifact_transfers WHERE id='archive-1'").fetchone()[0])
            second_report=json.loads(s.db.execute(
                "SELECT report FROM artifact_transfers WHERE id='archive-2'").fetchone()[0])
            self.assertIn('PermissionError',first_report['cleanup_error'])
            self.assertTrue(second_report['source_cleanup']['deleted'])
            s.db.close()

    def test_attempt_archive_route_and_campaign_folder_are_frozen(self):
        with tempfile.TemporaryDirectory() as root:
            s=Store(Path(root)/'db')
            farm=node(root=str(Path(root)/'farm'),key='farm9-gui2');farm.update(transport='ssh',target='farm9')
            lab4=node(root=str(Path(root)/'lab4'),key='lab4');lab4.update(transport='ssh',target='lab4')
            cps1=node(root=str(Path(root)/'cps1'),key='cps1-model');cps1.update(transport='ssh',target='cps1')
            relay=node(root=str(Path(root)/'relay'),key='resource-control')
            for n in (farm,lab4,cps1,relay):s.register_node(n)
            class Capture:
                def __init__(self):self.requests=[]
                def call(self,node,action,request):self.requests.append((node,action,request));return {'status':'starting'}
            transport=Capture();controller=Controller(s,transport)
            attempt=dict(id='attempt.done',job='job',node='farm9-gui2',status='succeeded',
                         experiment_id='experiment',report={},spec=dict(
                             attempt_dir='/farm/attempt.done',spec_sha256='1'*64))
            start_attempt_archive(controller,attempt,farm,lab4,['campaign-a','campaign-b'])
            config=json.loads(s.db.execute(
                "SELECT spec FROM artifact_transfers WHERE direction='archive'").fetchone()[0])['config']
            self.assertEqual(config['transport_route'],'cps1-staging')
            self.assertEqual(config['staging_target'],'cps1')
            self.assertTrue(config['destination_root'].endswith(
                '/attempt-archive/campaign-a/experiment/job/attempt.done'))
            self.assertEqual(config['campaigns'],['campaign-a','campaign-b'])
            s.db.close()

    def test_dependency_staging_does_not_require_hf_campaign(self):
        with tempfile.TemporaryDirectory() as root:
            s=Store(Path(root)/'db')
            source=node(root=str(Path(root)/'source-runs'),key='source')
            destination=node(root=str(Path(root)/'destination-runs'),key='destination')
            relay=node(root=str(Path(root)/'relay-runs'),key='resource-control')
            source.update(transport='ssh',target='source-host')
            destination.update(transport='ssh',target='destination-host')
            for n in (source,destination,relay):s.register_node(n)
            predecessor=job('predecessor',gpu_count=0);predecessor['outputs']=['RESULT.json']
            consumer=job('consumer',deps=['predecessor']);consumer['hosts']=['destination']
            s.register_experiment(experiment([predecessor,consumer]))
            register_campaign(s,dict(id='local-only',name='Local only',rq='why',projects=['general']))
            with s.db:
                s.db.execute("UPDATE jobs SET status='succeeded' WHERE id='predecessor'")
                s.db.execute('INSERT INTO attempts(id,job,node,spec,status,created,report) VALUES(?,?,?,?,?,?,?)',
                    ('predecessor.done','predecessor','source',
                     dumps(dict(attempt_dir=str(Path(root)/'source'),node_spec=source,startup_group='')),
                     'succeeded',1,dumps({'outputs':{'RESULT.json':{'sha256':'0'*64,'bytes':1}}})))
                s.db.execute('INSERT INTO artifact_transfers VALUES(?,?,?,?,?,?,?,?)',
                    ('published','predecessor.done','source','upload','succeeded','{}',dumps({'artifact':{
                        'attempt':'predecessor.done','files':{
                            'RESULT.json':{'sha256':'0'*64,'bytes':1},
                            'run/epochs/e20/model.pdparams':{'sha256':'1'*64,'bytes':2}}}}),0))
                for n in (source,destination,relay):
                    s.db.execute('INSERT INTO snapshots VALUES(?,?)',(n['id'],dumps(snapshot(n))))
            class Capture:
                def __init__(self):self.requests=[]
                def call(self,node,action,request):
                    self.requests.append((node['id'],action,request));return {'status':'starting'}
            transport=Capture()
            with s.lock():tick(Controller(s,transport),execute=True)
            self.assertEqual([(node,action) for node,action,_ in transport.requests],
                             [('resource-control','launch')])
            transfer=s.db.execute("SELECT node,status,spec FROM artifact_transfers WHERE direction='download'").fetchone()
            self.assertEqual((transfer['node'],transfer['status']),('destination','starting'))
            config=json.loads(transfer['spec'])['config']
            self.assertEqual(config['destination_target'],'destination-host')
            self.assertEqual(set(config['files']),{'RESULT.json','run/epochs/e20/model.pdparams'})
            s.db.close()


class ReservationReadTests(unittest.TestCase):
    def test_compact_terminal_transfer_omits_large_file_maps(self):
        with tempfile.TemporaryDirectory() as root:
            store=Store(Path(root)/'db')
            huge={'x'+str(i):{'bytes':i,'sha256':'a'*64} for i in range(100)}
            with store.db:
                store.db.execute('INSERT INTO artifact_transfers VALUES(?,?,?,?,?,?,?,?)',(
                    'done','attempt','lab4','download','succeeded',
                    dumps({'argv':['large'], 'config':{'files':huge,'repair_revision':'r1'}}),
                    dumps({'artifact':{'root':'/archive','files':huge}}),1))
            item=rows(store,compact=True)[0]
            self.assertNotIn('files',item['spec']['config'])
            self.assertEqual(item['spec']['config']['repair_revision'],'r1')
            self.assertNotIn('files',item['report']['artifact'])
            self.assertEqual(item['report']['artifact']['root'],'/archive')
            self.assertEqual(rows(store,compact=True,active_only=True),[])
            store.db.close()

    def test_terminal_transfer_requests_are_not_decoded(self):
        db=sqlite3.connect(':memory:');db.row_factory=sqlite3.Row
        try:
            db.execute('CREATE TABLE artifact_transfers(id TEXT,status TEXT,spec TEXT,report TEXT,created REAL)')
            db.executemany('INSERT INTO artifact_transfers VALUES(?,?,?,?,?)',[
                ('old','succeeded','not decoded','not decoded',1),
                ('live','running','{}','{}',2)])
            rows=reservations(types.SimpleNamespace(db=db))
            self.assertEqual([r['id'] for r in rows],['live'])
            self.assertFalse(rows[0]['released'])
            self.assertEqual(db.execute('SELECT count(*) FROM artifact_transfers').fetchone()[0],2)
        finally:
            db.close()


class PublicationDownloadSummaryTests(unittest.TestCase):
    def test_completed_consumer_ignores_failed_speculative_destination_copies(self):
        with tempfile.TemporaryDirectory() as root:
            s=Store(Path(root)/'db');n=node();n['hf']={'python':sys.executable};s.register_node(n)
            producer=job('producer',gpu_count=0);producer['outputs']=['result']
            consumer=job('consumer',gpu_count=0,deps=['producer'])
            s.register_experiment(experiment([producer,consumer]))
            campaign=register_campaign(s,dict(id='study',name='Study',rq='why',projects=['general']))
            with s.db:
                s.db.execute("UPDATE jobs SET status='succeeded'")
                s.db.execute('INSERT INTO attempts(id,job,node,spec,status,created,report) VALUES(?,?,?,?,?,?,?)',
                    ('producer.done','producer','a',dumps(dict(attempt_dir='/source',node_spec=n,startup_group='')),
                     'succeeded',0,dumps({'outputs':{'result':{'sha256':'0'*64}}})))
                for index in range(3):
                    s.db.execute('INSERT INTO artifact_transfers VALUES(?,?,?,?,?,?,?,?)',
                        ('failed-'+str(index),'producer.done','b','download','failed','{}','{}',index))
            self.assertEqual(publication_summary(s,campaign)['counts']['error'],0)
            s.db.close()


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

    def test_archive_roundtrip_preserves_all_files_and_uses_two_hub_entries(self):
        receipt=hf_worker.upload(dict(self.config,archive_payload=True),self.hub)
        self.assertEqual(len(self.hub.calls),1)
        self.assertEqual(len(self.hub.calls[0]['operations']),2)
        self.assertIn('archive',receipt)
        restored=hf_worker.download(dict(receipt=receipt,destination=str(self.root/'packed')),self.hub.download)
        self.assertEqual((self.root/'packed/weights.bin').read_bytes(),b'checkpoint')
        self.assertEqual(set(restored['files']),set(receipt['files']))
        self.assertEqual(restored['derived']['result.json']['original_sha256'],self.config['outputs']['result.json']['sha256'])
        archive=self.hub.root/receipt['path']/receipt['archive']['file']
        archive.write_bytes(b'corrupt')
        with self.assertRaisesRegex(ValueError,'archive hash mismatch'):
            hf_worker.download(dict(receipt=receipt,destination=str(self.root/'corrupt')),self.hub.download)

    def test_campaign_policy_archives_future_transfers_without_one_off_flag(self):
        cfg=dict(self.config,hf=dict(self.config['hf'],archive_payload=True))
        receipt=hf_worker.upload(cfg,self.hub)
        self.assertIn('archive',receipt)
        self.assertEqual(len(self.hub.calls[0]['operations']),2)

    def test_archive_rejects_unsafe_duplicate_missing_or_changed_members(self):
        for case in ('escape','symlink','duplicate','missing','hash','size'):
            with self.subTest(case=case):
                path=self.root/(case+'.tar.gz');entries={'ok':dict(bytes=1,sha256=hashlib.sha256(b'x').hexdigest())}
                with tarfile.open(path,'w:gz') as bundle:
                    if case!='missing':
                        info=tarfile.TarInfo('../escape' if case=='escape' else 'ok');info.size=1
                        if case=='symlink':info.type=tarfile.SYMTYPE;info.linkname='/tmp/unsafe';info.size=0
                        if case=='size':info.size=2
                        bundle.addfile(info,io.BytesIO(b'zz' if case=='size' else b'z' if case=='hash' else b'x'))
                        if case=='duplicate':bundle.addfile(info,io.BytesIO(b'x'))
                with self.assertRaises(ValueError):hf_worker.unpack_archive(path,self.root/case,entries)

    def test_repository_total_file_limit_does_not_retry_smaller_commits(self):
        e=RuntimeError('too many files. Your git repo would contain 20043 files')
        e.response=types.SimpleNamespace(status_code=400)
        with patch.object(self.hub,'create_commit',side_effect=e) as call:
            with self.assertRaises(RuntimeError):hf_worker.upload(self.config,self.hub)
            self.assertEqual(call.call_count,1)

    def test_missing_checkpoint_in_descriptor_fails_before_remote_write(self):
        config = dict(self.config,patterns=['result.json'])
        with self.assertRaises(ValueError):hf_worker.upload(config,self.hub)
        self.assertEqual(self.hub.calls,[])

    def test_batched_upload_pins_last_commit_and_manifest_is_last(self):
        for i in range(101):(self.source/('extra%d.bin'%i)).write_bytes(b'data')
        cfg=dict(self.config,patterns=['result.json','*.bin'])
        original=self.hub.create_commit
        def commit(**kwargs):
            original(**kwargs)
            return types.SimpleNamespace(oid=('%040x'%len(self.hub.calls)))
        self.hub.create_commit=commit
        receipt=hf_worker.upload(cfg,self.hub)
        self.assertEqual([len(c['operations']) for c in self.hub.calls],[50,50,4])
        self.assertEqual(receipt['revision'],'%040x'%3)
        self.assertTrue(self.hub.calls[-1]['operations'][-1].path.endswith('/HF_MANIFEST.json'))
        self.assertFalse(any(o.path.endswith('/HF_MANIFEST.json') for c in self.hub.calls[:-1] for o in c['operations']))

    def test_batched_failure_or_mutation_does_not_publish_manifest(self):
        for mode in ('failure','mutation'):
            with self.subTest(mode=mode):
                for i in range(51):(self.source/('part%d.bin'%i)).write_bytes(b'data')
                cfg=dict(self.config,patterns=['result.json','weights.bin','part*.bin'])
                calls=[]
                def commit(**kwargs):
                    calls.append(kwargs)
                    if mode=='failure':raise RuntimeError('upload failed')
                    (self.source/'part0.bin').write_bytes(b'changed')
                    return types.SimpleNamespace(oid='a'*40)
                with patch.object(self.hub,'create_commit',side_effect=commit):
                    with self.assertRaises((RuntimeError,ValueError)):hf_worker.upload(cfg,self.hub)
                self.assertEqual(len(calls),1)
                self.assertFalse(any(o.path.endswith('/HF_MANIFEST.json') for o in calls[0]['operations']))

    def test_explicit_file_limit_shrinks_but_other_errors_do_not_retry(self):
        for i in range(51):(self.source/('part%d.bin'%i)).write_bytes(b'data')
        cfg=dict(self.config,patterns=['result.json','weights.bin','part*.bin'])
        original=self.hub.create_commit;attempted=[]
        def commit(**kwargs):
            size=len(kwargs['operations']);attempted.append(size)
            if size>20:
                e=RuntimeError('Too many files (limit 20 files)');e.response=types.SimpleNamespace(status_code=400);raise e
            return original(**kwargs)
        with patch.object(self.hub,'create_commit',side_effect=commit):
            receipt=hf_worker.upload(cfg,self.hub)
        self.assertEqual(attempted[:3],[50,25,12])
        self.assertTrue(all(len(c['operations'])<=20 for c in self.hub.calls))
        self.assertEqual(len(receipt['files']),53)
        for message,status in [('Too many files',429),('bad configuration',400)]:
            e=RuntimeError(message);e.response=types.SimpleNamespace(status_code=status)
            with patch.object(self.hub,'create_commit',side_effect=e) as call, patch.object(hf_worker.time,'sleep'):
                with self.assertRaises(RuntimeError):hf_worker.upload(cfg,self.hub)
                self.assertEqual(call.call_count,1)

    def test_file_limit_retry_stops_at_one_file(self):
        e=RuntimeError('Too many files');e.response=types.SimpleNamespace(status_code=400)
        with patch.object(self.hub,'create_commit',side_effect=e) as call:
            with self.assertRaises(RuntimeError):hf_worker.upload(self.config,self.hub)
            self.assertEqual(call.call_count,2)

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
        self._detached_transfer_lifecycle(archive=False)

    def test_archived_transfer_unblocks_consumer_with_pinned_branch_and_hashes(self):
        self._detached_transfer_lifecycle(archive=True)

    def test_scheduler_consumes_explicit_repair_queue(self):
        self._detached_transfer_lifecycle(archive=True, repair=True)

    def _detached_transfer_lifecycle(self, archive, repair=False):
        s=Store(self.root/'state.db')
        a,b=node(root=str(self.root/'a')),node(root=str(self.root/'b'),key='b')
        a['hf']=b['hf']={'python':sys.executable}
        b.update(transport='ssh',target='simulated-b')
        s.register_node(a);s.register_node(b)
        first=job('first',gpu_count=0);first.update(outputs=['result.json'],hf_artifacts=['weights.bin'],
                                                 hf_relocate_json=['result.json'])
        child=job('child',gpu_count=0,deps=['first']);child.update(hosts=['b'],argv=['cat','{dep:first}/weights.bin'])
        s.register_experiment(experiment([first,child]))
        register_campaign(s,dict(id='study',name='Study',rq='why',projects=['general'],hf={'repo_id':'test/results', 'enabled':True,
            'revision':'codex/archive-test' if archive else 'main'}))
        with s.db:
            s.db.execute("UPDATE jobs SET status='succeeded' WHERE id='first'")
            s.db.execute('INSERT INTO attempts(id,job,node,spec,status,created,report) VALUES(?,?,?,?,?,?,?)',
                ('first.123','first','a',dumps(dict(attempt_dir=str(self.source),node_spec=a,startup_group='')),
                 'succeeded',0,dumps({'outputs':self.config['outputs']})))
            for n in (a,b):s.db.execute('INSERT INTO snapshots VALUES(?,?)',(n['id'],dumps(snapshot(n))))
        if repair:
            with s.db:
                s.db.execute('CREATE TABLE artifact_repair_queue(attempt TEXT PRIMARY KEY,config TEXT,state TEXT,created REAL,expires REAL)')
                cfg=dict(self.config,archive_payload=True,repair_revision='test-repair',
                         hf=dict(self.config['hf'],revision='codex/archive-test',enabled=True))
                s.db.execute('INSERT INTO artifact_repair_queue VALUES(?,?,?,?,?)',('first.123',dumps(cfg),'pending',0,__import__('time').time()+1000))
        parent=self
        class Transfers:
            def __init__(self):self.reports={}
            def call(self,n,action,request):
                if action=='launch':
                    cfg=request['config']
                    if cfg['direction']=='upload':
                        parent.assertTrue(cfg['archive_payload'])
                        if not archive:cfg=dict(cfg,archive_payload=False)
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
        if repair:self.assertEqual(s.db.execute('SELECT state FROM artifact_repair_queue').fetchone()[0],'submitted')
        self.assertEqual(len(reservations(s)),1)
        self.assertEqual(c.plan()[0]['decision'],'waiting')
        with s.lock():tick(c,execute=True)
        self.assertEqual(len(transport.reports),2)
        with s.lock():tick(c,execute=True)
        self.assertEqual(reservations(s),[])
        self.assertEqual(c.plan()[0]['decision'],'ready')
        request=c.request(c.plan()[0])
        self.assertEqual(Path(request['argv'][1]).read_bytes(),b'checkpoint')
        for item in request['input_files']:
            self.assertEqual(hf_worker.digest(item['path']),item['sha256'])
        self.assertIn('hf_artifact',s.attempts()[0]['report'])
        if archive:
            receipt=s.attempts()[0]['report']['hf_artifact']
            self.assertIn('archive',receipt)
            self.assertEqual(receipt['revision'],'a'*40)
            self.assertEqual(self.hub.calls[0]['revision'],'codex/archive-test')
        # Restarting/extra cycles preserve completed transfer receipts.
        with s.lock():tick(Controller(s,transport),execute=True)
        self.assertEqual(len(transport.reports),2)
        s.db.close()


class HFIntegrationTests(unittest.TestCase):
    def test_legacy_output_only_relay_is_not_a_checkpoint_location(self):
        with tempfile.TemporaryDirectory() as root:
            s=Store(Path(root)/'db');a=node();b=node(key='b');b.update(transport='ssh',target='b-host');s.register_node(a);s.register_node(b)
            producer=job('producer',gpu_count=0);producer.update(outputs=['RESULT.json'],hf_artifacts=['run/model'])
            s.register_experiment(experiment([producer]))
            outputs={'RESULT.json':{'path':'/source/RESULT.json','sha256':'0'*64,'bytes':1}}
            attempt_spec=dict(attempt_dir='/source',node_spec=a,startup_group='',job_spec=producer)
            with s.db:
                s.db.execute("UPDATE jobs SET status='succeeded' WHERE id='producer'")
                s.db.execute('INSERT INTO attempts(id,job,node,spec,status,created,report) VALUES(?,?,?,?,?,?,?)',
                    ('producer.done','producer','a',dumps(attempt_spec),'succeeded',0,dumps({'outputs':outputs})))
                s.db.execute('INSERT INTO artifact_transfers VALUES(?,?,?,?,?,?,?,?)',
                    ('old','producer.done','b','download','succeeded','{}',dumps({'artifact':{
                        'root':'/b','files':outputs}}),1))
            self.assertNotIn('artifact_locations',s.attempts()[0])
            s.db.close()

    def test_incomplete_legacy_download_cannot_shadow_published_checkpoint_manifest(self):
        with tempfile.TemporaryDirectory() as root:
            s=Store(Path(root)/'db');a=node();b=node(key='b');b.update(transport='ssh',target='b-host');s.register_node(a);s.register_node(b)
            producer=job('producer',gpu_count=0);producer['outputs']=['RESULT.json']
            s.register_experiment(experiment([producer]))
            outputs={'RESULT.json':{'path':'/source/RESULT.json','sha256':'0'*64,'bytes':1}}
            complete={'RESULT.json':{'path':'/b/RESULT.json','sha256':'0'*64,'bytes':1},
                      'run/model':{'path':'/b/run/model','sha256':'1'*64,'bytes':2}}
            with s.db:
                s.db.execute("UPDATE jobs SET status='succeeded' WHERE id='producer'")
                s.db.execute('INSERT INTO attempts(id,job,node,spec,status,created,report) VALUES(?,?,?,?,?,?,?)',
                    ('producer.done','producer','a',dumps(dict(attempt_dir='/source',node_spec=a,startup_group='')),
                     'succeeded',0,dumps({'outputs':outputs})))
                s.db.execute('INSERT INTO artifact_transfers VALUES(?,?,?,?,?,?,?,?)',
                    ('old','producer.done','b','download','succeeded','{}',dumps({'artifact':{
                        'root':'/b','files':{'RESULT.json':complete['RESULT.json']}}}),1))
                s.db.execute('INSERT INTO artifact_transfers VALUES(?,?,?,?,?,?,?,?)',
                    ('upload','producer.done','a','upload','succeeded','{}',dumps({'artifact':{
                        'attempt':'producer.done','files':complete}}),2))
            self.assertNotIn('b',s.attempts()[0].get('artifact_locations',{}))
            with s.db:
                s.db.execute('INSERT INTO artifact_transfers VALUES(?,?,?,?,?,?,?,?)',
                    ('new','producer.done','b','download','succeeded','{}',dumps({'artifact':{
                        'root':'/b','files':complete}}),3))
            self.assertEqual(set(s.attempts()[0]['artifact_locations']['b']['files']),set(complete))
            s.db.close()

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
            new=register_campaign(s,dict(c,hf={'repo_id':'user/study','enabled':True}))
            self.assertEqual(new['hf']['repo_type'],'model')
            self.assertEqual(campaign_for(dict(id='e',project='general'),{'c':new})['id'],'campaign')
            with self.assertRaises(ValueError):hf_spec({'repo_id':'user/study','token':'secret'})
            s.db.close()

    def test_overlapping_monitors_share_one_destination_and_owner(self):
        with tempfile.TemporaryDirectory() as root:
            s=Store(Path(root)/'db')
            s.register_experiment(experiment([job('first')]))
            a=register_campaign(s,dict(id='all',name='All',rq='why',projects=['general'],
                                      hf={'repo_id':'user/results','enabled':True}))
            b=register_campaign(s,dict(id='subset',name='Subset',rq='why',experiments=['e'],
                                      hf={'repo_id':'user/results','enabled':True}))
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
                                    hf={'repo_id':'user/results','enabled':True}))
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
                                               hf={'repo_id':'user/results','enabled':True}))
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
            summary=publication_summary(s,campaign)
            self.assertEqual(summary['counts']['error'],0)
            self.assertEqual(summary['counts']['pending'],1)
            self.assertEqual(summary['errors'],[])
            self.assertEqual(summary['warnings'][0]['status'],'pending')
            s.db.close()


if __name__=='__main__':unittest.main()
