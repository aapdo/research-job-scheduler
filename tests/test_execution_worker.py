import hashlib
import json
from pathlib import Path
import shlex
import tempfile
import unittest
from unittest.mock import patch
from research_scheduler import execution_worker as worker


class ExecutionWorkerTests(unittest.TestCase):
    def test_verified_atomic_copy_and_reuse(self):
        with tempfile.TemporaryDirectory() as folder:
            root=Path(folder);source=root/'source';source.mkdir();target=root/'target';output=root/'out';output.mkdir()
            (source/'nested').mkdir();(source/'nested/file').write_bytes(b'unchanged source')
            manifest=root/'manifest.json';manifest.write_text(json.dumps({'files':{'nested/file':{'bytes':16,'sha256':hashlib.sha256(b'unchanged source').hexdigest()}}}))
            recipe=dict(source_target='@local',source_root=str(source),destination_root=str(target),manifest_file=str(manifest),manifest_sha256=worker.sha(manifest))
            remote=worker.remote;popen=worker.subprocess.Popen
            def local(target,data):return remote('@local',data)
            def launch(args,**kwargs):return popen(shlex.split(args[-1]) if args[0]=='ssh' else args,**kwargs)
            with patch.object(worker,'remote',local),patch.object(worker.subprocess,'Popen',launch):
                self.assertEqual(worker.copy_set('fixture',recipe,output)['copied'],1)
                self.assertEqual(worker.copy_set('fixture',recipe,output)['copied'],0)
                self.assertEqual((target/'nested/file').read_bytes(),b'unchanged source')
                (target/'nested/file').write_bytes(b'other work')
                with self.assertRaises(RuntimeError):worker.copy_set('fixture',recipe,output)
                self.assertEqual((target/'nested/file').read_bytes(),b'other work')

    def test_unsafe_manifest_is_rejected_before_network_or_write(self):
        with tempfile.TemporaryDirectory() as folder:
            root=Path(folder);manifest=root/'manifest.json'
            manifest.write_text(json.dumps({'files':{'../outside':{'bytes':0,'sha256':'a'*64}}}))
            recipe=dict(manifest_file=str(manifest),manifest_sha256=worker.sha(manifest))
            with patch.object(worker,'remote') as remote:
                with self.assertRaises(ValueError):worker.copy_set('fixture',recipe,root)
                remote.assert_not_called()
