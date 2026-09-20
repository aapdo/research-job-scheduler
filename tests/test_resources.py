import hashlib
import json
from pathlib import Path
import tempfile
import time
import unittest
from unittest.mock import patch

from test_scheduler import node,job,experiment
from test_dataset_preparation import Probe
from research_scheduler.store import Store,dumps
from research_scheduler.controller import Controller
from research_scheduler import resources as resources
from research_scheduler import resource_worker as worker


class ResourceTests(unittest.TestCase):
    def setUp(self):
        self.tmp=tempfile.TemporaryDirectory();self.root=Path(self.tmp.name)
        self.source=self.root/'source';self.source.mkdir()
        (self.source/'data.bin').write_bytes(b'immutable resource'*100)
        self.dest=self.root/'destination';self.out=self.root/'out';self.out.mkdir()
        self.manifest=self.root/'manifest.json'
        self.manifest.write_text(json.dumps({'files':{'data.bin':dict(bytes=(self.source/'data.bin').stat().st_size,sha256=hashlib.sha256((self.source/'data.bin').read_bytes()).hexdigest())}}))
        self.sha=hashlib.sha256(self.manifest.read_bytes()).hexdigest()

    def tearDown(self):self.tmp.cleanup()

    def config(self):
        return dict(resource='demo-v1',manifest_file=str(self.manifest),manifest_sha256=self.sha,node='b',target='@local',root=str(self.dest),
                    sources=[dict(node='a',target='@local',root=str(self.source))],marker=resources.marker('demo-v1',self.sha))

    def test_real_chunk_relay_source_fallback_and_verified_reuse(self):
        bad=self.root/'bad';bad.mkdir();(bad/'data.bin').write_bytes(b'wrong')
        config=self.config();config['sources'].insert(0,dict(node='bad',target='@local',root=str(bad)))
        with patch.object(worker,'CHUNK_BYTES',1024):result=worker.run(config,self.out)
        self.assertEqual(result['source'],'a');self.assertEqual(result['copied'],1)
        self.assertGreater(len(result['chunks']),1)
        self.assertEqual(result['source_failures'][0]['node'],'bad')
        self.assertEqual((self.dest/'data.bin').read_bytes(),(self.source/'data.bin').read_bytes())
        self.assertEqual(worker.run(config,self.out)['copied'],0)
        (self.dest/'data.bin').write_bytes(b'belongs to someone else')
        with self.assertRaises(RuntimeError):worker.run(config,self.out)
        self.assertEqual((self.dest/'data.bin').read_bytes(),b'belongs to someone else')

    def test_changed_manifest_rejected_before_network(self):
        config=self.config();self.manifest.write_text('{}')
        with patch.object(worker,'remote') as remote:
            with self.assertRaises(AssertionError):worker.run(config,self.out)
            remote.assert_not_called()

    def test_scheduler_creates_cpu_copy_then_unblocks_consumer(self):
        store=Store(self.root/'db')
        for key in ('a','b','control'):
            n=node(str(self.root/key),key=key)
            n['target']=key
            if key=='control':n['gpus']=[]
            store.register_node(n)
        j=job('consumer',gpu_count=0,vram=0);j.update(kind='eval',hosts=['b'],cwd=str(self.dest),outputs=['ok'],
            argv=['python3','-c',"import os;from pathlib import Path;assert Path('data.bin').read_bytes()=="+repr((self.source/'data.bin').read_bytes())+";Path(os.environ['RS_ATTEMPT_DIR'],'ok').write_text('pass')"])
        store.register_experiment(experiment([j]))
        c=dict(id='demo-v1',manifest_file=str(self.manifest),manifest_sha256=self.sha,sources={'a':str(self.source)},destinations={'b':str(self.dest)},coordinator='control',match={'cwd':str(self.dest)},max_parallel=2)
        resources.register(store,c);resources.register(store,c)
        controller=Controller(store,Probe())
        resources.tick(controller,execute=False);self.assertEqual(len(store.jobs()),1)
        until=time.time()+20
        while time.time()<until:
            controller.tick(execute=True)
            statuses={j['id']:j['status'] for j in store.jobs()}
            if statuses['consumer']=='succeeded':break
            time.sleep(.2)
        self.assertEqual(statuses['consumer'],'succeeded')
        location=next(r for r in resources.status(store)['locations'] if r['node']=='b')
        self.assertEqual(location['state'],'ready')
        preparations=[a for a in store.attempts() if a['job'].startswith('RESOURCE_')]
        self.assertEqual(len(preparations),1);self.assertEqual(preparations[0]['spec']['resources']['gpu_count'],0)
        attempt=next(a for a in store.attempts() if a['job']=='consumer')
        self.assertTrue(any('/.resource-ready/' in f['path'] for f in attempt['spec']['input_files']))
        # A missing readiness marker is discovered from fresh normal probes,
        # and a later consumer triggers verification again without deleting data.
        (self.dest/'.resource-ready/demo-v1').unlink()
        next_job=dict(j,id='consumer2')
        next_experiment=experiment([next_job]);next_experiment['id']='next'
        store.register_experiment(next_experiment)
        controller.refresh();resources.tick(controller,execute=True)
        self.assertEqual(sum(j['id'].startswith('RESOURCE_') for j in store.jobs()),2)
        self.assertEqual((self.dest/'data.bin').read_bytes(),(self.source/'data.bin').read_bytes())
        store.db.close()

    def test_disabled_destination_cannot_be_prepared(self):
        store=Store(self.root/'db')
        for key in ('a','b','control'):
            n=node(str(self.root/key),key=key);n['target']=key;n['enabled']=key!='b'
            if key=='control':n['gpus']=[]
            store.register_node(n)
        j=job('consumer');j.update(cwd=str(self.dest),hosts=['b']);store.register_experiment(experiment([j]))
        resources.register(store,dict(id='demo-v1',manifest_file=str(self.manifest),manifest_sha256=self.sha,sources={'a':str(self.source)},destinations={'b':str(self.dest)},coordinator='control',match={'cwd':str(self.dest)},max_parallel=1))
        resources.tick(Controller(store),execute=True)
        self.assertEqual(len(store.jobs()),1);self.assertFalse(store.specs('nodes')['b']['enabled'])
        store.db.close()

    def test_retired_source_and_destination_are_ignored(self):
        store=Store(self.root/'retired-db')
        for key in ('b','control','retired'):
            n=node(str(self.root/key),key=key);n['target']=key
            if key=='control':n['gpus']=[]
            store.register_node(n)
        j=job('consumer');j.update(cwd=str(self.dest),hosts=['b'])
        store.register_experiment(experiment([j]))
        resources.register(store,dict(id='demo-v1',manifest_file=str(self.manifest),
            manifest_sha256=self.sha,sources={'retired':str(self.source)},
            destinations={'b':str(self.dest),'retired':str(self.root/'retired')},
            coordinator='control',match={'cwd':str(self.dest)},max_parallel=1))
        with store.db:store.db.execute("DELETE FROM nodes WHERE id='retired'")
        resources.tick(Controller(store),execute=True)
        row=store.db.execute("SELECT spec FROM jobs WHERE id LIKE 'RESOURCE_%'").fetchone()
        self.assertIsNotNone(row)
        self.assertEqual(json.loads(row['spec'])['config']['sources'],[])
        store.db.close()

    def test_verified_resource_placeholder_resolves_to_replica_root(self):
        store=Store(self.root/'placeholder-db')
        n=node(str(self.root/'runs'),key='b');n['target']='b'
        m=resources.marker('demo-v1',self.sha)
        n['assets'][m['name']]={'path':str(self.dest/'.resource-ready'/'demo-v1'),'sha256':m['sha256']}
        store.register_node(n)
        j=job('consumer',gpu_count=0,vram=0)
        j['config']={'root':'{resource:demo-v1}'}
        j['metadata']={'required_resources':{'demo-v1':self.sha}}
        store.register_experiment(experiment([j]))
        request=Controller(store).request({'job':'consumer','node':'b','gpus':[]})
        self.assertEqual(request['config']['root'],str(self.dest))
        self.assertEqual(request['input_files'][-1]['path'],str(self.dest/'.resource-ready'/'demo-v1'))
        store.db.close()
