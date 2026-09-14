import hashlib,json,sys,tempfile,unittest
from pathlib import Path
sys.path.insert(0,str(Path(__file__).resolve().parents[1]/'src'))
from research_scheduler.agent import diagnostic_startup_ready

class DiagnosticReadinessTests(unittest.TestCase):
    def test_only_verified_completed_r18_diagnostic_releases_startup(self):
        with tempfile.TemporaryDirectory() as tmp:
            root=Path(tmp);cell=root/'diagnostics/Clean_s00';cell.mkdir(parents=True)
            cp=root/'weights';cp.write_bytes(b'model');sha=hashlib.sha256(b'model').hexdigest()
            progress=root/'diagnostics/PROGRESS.json';progress.write_text(json.dumps(dict(completed_cells=1)))
            (cell/'REQUEST.json').write_text(json.dumps(dict(cell='Clean_s00',checkpoint=str(cp),checkpoint_sha256=sha)))
            out=cell/'DIAGNOSTIC_CELL.json';out.write_text(json.dumps(dict(status='complete',cell='Clean_s00',checkpoint_sha256=sha)))
            req=dict(job='R18_TEST_DIAGNOSTIC',attempt_dir=tmp,config={'mode':'diagnose'})
            state=dict(status='running',ready=False,started=0)
            result=diagnostic_startup_ready(req,state)
            self.assertTrue(result['ready']);self.assertFalse(state['ready'])
            for changed in (dict(req,job='OTHER'),dict(req,config={'mode':'train'})):
                self.assertFalse(diagnostic_startup_ready(changed,state)['ready'])
            self.assertFalse(diagnostic_startup_ready(req,dict(state,status='unknown'))['ready'])
            self.assertFalse(diagnostic_startup_ready(req,dict(state,started=out.stat().st_mtime+10))['ready'])
            cp.write_bytes(b'wrong');self.assertFalse(diagnostic_startup_ready(req,state)['ready'])
            cp.write_bytes(b'model');progress.write_text('{"completed_cells":0}')
            self.assertFalse(diagnostic_startup_ready(req,state)['ready'])
            progress.write_text('{"completed_cells":1}');out.write_text('{"status":"running"}')
            self.assertFalse(diagnostic_startup_ready(req,state)['ready'])
