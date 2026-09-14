import json
import os
from pathlib import Path
import tempfile
import time
import unittest
from research_scheduler.progress_phases import legacy_posterior_phase


class PosteriorPhaseTests(unittest.TestCase):
    def test_requires_epoch_checkpoint_live_identity_and_recent_heartbeat(self):
        with tempfile.TemporaryDirectory() as tmp:
            root=Path(tmp);(root/'models').mkdir();(root/'resume').mkdir()
            now=time.time();progress=dict(epoch=10,member=0)
            cfg=dict(arm='bayesian',mode='train')
            (root/'config.json').write_text(json.dumps(cfg))
            (root/'LEARNING_CURVE.jsonl').write_text(json.dumps(dict(epoch=10,member=0))+'\n')
            raw=Path('/proc',str(os.getpid()),'stat').read_text()
            state=dict(attempt=root.name,status='running',heartbeat=now,runner_pid=os.getpid(),
                       runner_start=raw[raw.rfind(')')+2:].split()[19],
                       boot_id=Path('/proc/sys/kernel/random/boot_id').read_text().strip())
            def save(): (root/'state.json').write_text(json.dumps(state))
            save()
            self.assertEqual(legacy_posterior_phase(root,progress,now),{})
            (root/'models/m0_e10.pdparams').write_bytes(b'checkpoint')
            (root/'resume/m0_e10.pdstate').write_bytes(b'committed')
            os.utime(root/'models/m0_e10.pdparams',(now-1300,now-1300))
            self.assertEqual(legacy_posterior_phase(root,progress,now)['phase'],'posterior')
            state['heartbeat']=now-121;save()
            self.assertEqual(legacy_posterior_phase(root,progress,now),{})
            state['heartbeat']=now;state['runner_start']='reused';save()
            self.assertEqual(legacy_posterior_phase(root,progress,now),{})
            state['runner_start']=raw[raw.rfind(')')+2:].split()[19];save()
            (root/'models/posterior_e10.npz').write_bytes(b'result')
            self.assertEqual(legacy_posterior_phase(root,progress,time.time())['phase'],'posterior_complete')
            self.assertEqual(legacy_posterior_phase(root,dict(epoch=11,member=0),time.time()),{})
            cfg['arm']='quantile';(root/'config.json').write_text(json.dumps(cfg))
            self.assertEqual(legacy_posterior_phase(root,progress,time.time()),{})
