import json
from pathlib import Path
import sys
import tempfile
import unittest
from unittest.mock import patch

sys.path.insert(0,str(Path(__file__).resolve().parents[1]/'src'))
from research_scheduler import agent


class RebootStatusTests(unittest.TestCase):
    def read(self, old_boot='old', current_boot='new', status='running', kind='train', tokens=None):
        with tempfile.TemporaryDirectory() as tmp:
            state=dict(attempt='a',status=status,boot_id=old_boot,runner_pid=20,
                       runner_start='30',child_pgid=21,ready=True)
            (Path(tmp)/'state.json').write_text(json.dumps(state))
            original=Path.read_text
            def read_text(path,*args,**kwargs):
                if str(path)=='/proc/sys/kernel/random/boot_id':return current_boot
                return original(path,*args,**kwargs)
            with patch.object(Path,'read_text',read_text),patch.object(agent,'process',return_value=None),patch.object(agent,'group_alive',return_value=True):
                result=agent.read_status(dict(id='a',attempt_dir=tmp,job_spec=dict(kind=kind),resources=dict(tokens=tokens or {})))
            self.assertEqual(json.loads((Path(tmp)/'state.json').read_text()),state)
            return result

    def test_confirmed_reboot_terminates_old_local_work(self):
        r=self.read()
        self.assertEqual(r['status'],'failed')
        self.assertEqual(r['termination_cause'],'host_reboot')
        self.assertEqual(r['boot_id'],'old')
        self.assertEqual(r['current_boot_id'],'new')

    def test_missing_runner_on_same_boot_is_unknown(self):
        self.assertEqual(self.read(current_boot='old')['status'],'unknown')

    def test_missing_boot_proof_is_unknown(self):
        self.assertEqual(self.read(old_boot=None)['status'],'unknown')
        self.assertEqual(self.read(current_boot='')['status'],'unknown')

    def test_terminal_history_survives_pid_reuse(self):
        for status in ('succeeded','failed'):
            self.assertEqual(self.read(status=status)['status'],status)

    def test_remote_hardware_tokens_remain_conservative(self):
        self.assertEqual(self.read(kind='board_test')['status'],'unknown')
        self.assertEqual(self.read(tokens={'board':1})['status'],'unknown')

    def test_optional_oom_sample_does_not_flip_live_runner_unknown(self):
        with tempfile.TemporaryDirectory() as tmp:
            state=dict(attempt='a',status='running',boot_id='same',runner_pid=20,
                       runner_start='30',child_pid=21,child_pgid=21,ready=True)
            (Path(tmp)/'state.json').write_text(json.dumps(state))
            original=Path.read_text
            def read_text(path,*args,**kwargs):
                if str(path)=='/proc/sys/kernel/random/boot_id':return 'same'
                return original(path,*args,**kwargs)
            runner=dict(state='S',ppid=1,pgrp=20,start='30')
            request=dict(id='a',attempt_dir=tmp,job_spec=dict(kind='eval'),resources={})
            with patch.object(Path,'read_text',read_text), \
                    patch.object(agent,'process',return_value=runner), \
                    patch.object(agent,'diagnostic_startup_ready',return_value=state), \
                    patch.object(agent,'process_tree_rss_mib',return_value=123), \
                    patch.object(agent,'oom_owned_processes',side_effect=agent.UncertainExecution('protected')):
                result=agent.read_status(request)
            self.assertEqual(result['status'],'running')
            self.assertEqual(result['rss_mib'],123)
            self.assertEqual(result['oom_observation_incomplete'],'protected')
