"""Automatic missing-dataset preparation, verified reuse and failure isolation."""
import hashlib
import json
import sys
import tempfile
import time
import unittest
from pathlib import Path

sys.path.insert(0,str(Path(__file__).resolve().parents[1]/'src'))
from research_scheduler.controller import Controller
from research_scheduler.store import Store
from research_scheduler.datasets import register,status
from test_scheduler import node,job,experiment,FakeProbeTransport


class Probe(FakeProbeTransport):
    def call(self,n,action,request):
        result=super().call(n,action,request)
        if action=='probe':
            result['datasets']={k:dict(path=p,available=Path(p).is_dir()) for k,p in n['datasets'].items()}
            result['assets']={k:hashlib.sha256(Path(v['path']).read_bytes()).hexdigest()
                              for k,v in n['assets'].items() if Path(v['path']).is_file()}
        return result


class DatasetPreparationTests(unittest.TestCase):
    def run_case(self,reuse=False,wrong=False):
        with tempfile.TemporaryDirectory() as root:
            root=Path(root);data=root/'data'
            if reuse or wrong:
                data.mkdir();(data/'IDENTITY').write_text('wrong' if wrong else 'version1')
            store=Store(root/'db');n=node(str(root/'runs'));store.register_node(n)
            j=job('consumer',gpu_count=0,vram=0)
            j.update(dataset='demo',assets={'demo-ready':hashlib.sha256(b'version1').hexdigest()},cwd=str(root),
                     outputs=['result'],argv=[sys.executable,'-c',
                     "import os;from pathlib import Path;assert (Path(os.environ['RS_DATASET_PATH'])/'IDENTITY').read_text()=='version1';Path(os.environ['RS_ATTEMPT_DIR'],'result').write_text('ok')"])
            store.register_experiment(experiment([j]))
            prepare="from pathlib import Path;p=Path("+repr(str(data))+");p.mkdir();(p/'IDENTITY').write_text('version1')"
            verify="from pathlib import Path;assert Path("+repr(str(data/'IDENTITY'))+").read_text()=='version1'"
            catalog=dict(id='demo',version='v1',identity_file='IDENTITY',identity_sha256=hashlib.sha256(b'version1').hexdigest(),
                asset_name='demo-ready',metadata_files=['IDENTITY'],replicas={'a':dict(path=str(data),cwd=str(root),
                    prepare_argv=[sys.executable,'-c',prepare],verify_argv=[sys.executable,'-c',verify],max_attempts=1)})
            register(store,catalog);register(store,catalog)
            c=Controller(store,Probe())
            c.tick(execute=False)
            self.assertEqual(len(store.jobs()),1)
            deadline=time.time()+15
            while time.time()<deadline:
                c.tick(execute=True)
                records=status(store)['preparations']
                states={x['id']:x['status'] for x in store.jobs()}
                if states['consumer']=='succeeded' or (wrong and records and records[0]['state']=='failed'):break
                time.sleep(.2)
            if wrong:
                self.assertEqual(states['consumer'],'queued')
                self.assertNotIn('demo',store.specs('nodes')['a']['datasets'])
                self.assertEqual((data/'IDENTITY').read_text(),'wrong')
            else:
                self.assertEqual(states['consumer'],'succeeded')
                self.assertEqual(records[0]['state'],'ready')
                self.assertEqual(records[0]['receipt']['reused'],reuse)
                self.assertEqual(store.specs('nodes')['a']['datasets']['demo'],str(data))
            self.assertEqual(len(store.jobs()),2)
            self.assertTrue(all(a['spec']['resources']['gpu_count']==0 for a in store.attempts()))
            store.db.close()

    def test_missing_dataset_prepared_and_consumer_runs(self):self.run_case()
    def test_existing_verified_replica_reused_without_prepare(self):self.run_case(reuse=True)
    def test_wrong_identity_never_overwritten_or_admitted(self):self.run_case(wrong=True)

    def test_retired_replica_is_preserved_but_not_reconciled(self):
        with tempfile.TemporaryDirectory() as root:
            root=Path(root);store=Store(root/'db');n=node(str(root/'runs'));store.register_node(n)
            j=job('consumer',gpu_count=0,vram=0);store.register_experiment(experiment([j]))
            data=root/'retired-data'
            catalog=dict(id='retired-demo',version='v1',identity_file='IDENTITY',
                identity_sha256=hashlib.sha256(b'version1').hexdigest(),asset_name='',metadata_files=[],
                replicas={'a':dict(path=str(data),cwd=str(root),prepare_argv=[sys.executable,'-c','pass'],
                    verify_argv=[sys.executable,'-c','pass'],max_attempts=1)})
            register(store,catalog)
            with store.db:
                store.db.execute("INSERT INTO dataset_preparations(dataset,node,job,state) VALUES(?,?,?,?)",
                                 ('retired-demo','a','consumer','cancelled'))
                store.db.execute("DELETE FROM nodes WHERE id='a'")
            Controller(store,Probe()).tick(execute=True,refresh=False,warmup=False)
            row=store.db.execute("SELECT state FROM dataset_preparations WHERE dataset='retired-demo'").fetchone()
            self.assertEqual(row['state'],'cancelled')
            store.db.close()


if __name__=='__main__':unittest.main()
