import json
import os
from pathlib import Path
import subprocess
import sys
import tempfile
import unittest


class ScopedStopTests(unittest.TestCase):
    def test_only_requested_predecessor_tree_is_stopped(self):
        tool=Path(__file__).resolve().parents[2]/'tools/stop_cg18_attempt_for_multigpu.py'
        if not tool.is_file():
            self.skipTest('requires the parent carla_online_switch tools directory')
        with tempfile.TemporaryDirectory() as temp:
            root=Path(temp)/'CG18_shared_TRAIN.test';root.mkdir()
            code="import subprocess,sys,time; p=subprocess.Popen([sys.executable,'-c','import time;time.sleep(60)']);print(p.pid,flush=True);time.sleep(60)"
            process=subprocess.Popen([sys.executable,'-c',code],env=dict(os.environ,RS_ATTEMPT_DIR=str(root)),
                                     stdout=subprocess.PIPE,text=True,start_new_session=True)
            unrelated=subprocess.Popen([sys.executable,'-c','import time;time.sleep(60)'])
            try:
                child=int(process.stdout.readline())
                (root/'spec.json').write_text(json.dumps(dict(id=root.name,job='CG18_shared_TRAIN',resources={'gpu_count':1})))
                (root/'state.json').write_text(json.dumps(dict(attempt=root.name,status='running',child_pid=process.pid)))
                result=subprocess.run([sys.executable,str(tool),root.name,str(root)],capture_output=True,text=True,timeout=35)
                self.assertEqual(result.returncode,0,result.stderr)
                out=json.loads(result.stdout)
                self.assertEqual(set(out['pids']),{process.pid,child})
                self.assertIsNone(unrelated.poll())
                self.assertTrue((root/'spec.json').exists())
                self.assertTrue((root/'USER_MULTIGPU_STOP.json').exists())
                process.wait(timeout=5)
            finally:
                if process.poll() is None:process.kill()
                process.wait();process.stdout.close()
                unrelated.terminate();unrelated.wait()
