import copy
import json
from pathlib import Path
import tempfile
import unittest
from unittest.mock import patch
from types import SimpleNamespace

from test_scheduler import node, snapshot, job, plan, reservation, experiment
from research_scheduler.agent import gpu_startup_failure
from research_scheduler import agent
from research_scheduler.gpu_recovery import retry_spec


class GpuFailoverTests(unittest.TestCase):
    def test_runtime_hold_blocks_gpu_not_cpu_and_clears_after_boot(self):
        n = node(); s = snapshot(n); s['boot_id'] = 'boot'
        n['labels']['gpu_runtime_quarantine'] = {'boot_id':'boot'}
        self.assertEqual(plan([job()], n=n, snap=s)[0]['decision'], 'waiting')
        self.assertEqual(plan([job(gpu_count=0)], n=n, snap=s)[0]['decision'], 'ready')
        s['boot_id'] = 'new'
        self.assertEqual(plan([job()], n=n, snap=s)[0]['decision'], 'ready')

    def test_probe_retains_valid_rows_when_one_gpu_disappears(self):
        n = node()
        output = 'Unable to determine device handle\n1, GPU-a-1, test, 24000, 0, 0, 35\n'
        with patch.object(agent.subprocess, 'run', return_value=SimpleNamespace(stdout=output, returncode=1)), \
             patch.object(agent.subprocess, 'check_output', return_value=''), \
             patch.object(agent, 'registered_d_processes', return_value=([], 0)):
            result = agent.probe(n)
        self.assertEqual([g['uuid'] for g in result['gpus']], ['GPU-a-1'])
        self.assertEqual(result['gpu_unavailable_uuids'], ['GPU-a-0'])

    def failure(self, n):
        a = reservation(n, status='failed')
        a.update(released=True, report=dict(status='failed', boot_id='boot',
                 failure_class='gpu_startup_unavailable', failed_gpu_uuids=[n['gpus'][0]['uuid']]))
        return a

    def test_failure_selects_other_gpu_for_all_jobs(self):
        n = node(); s = snapshot(n); s['boot_id'] = 'boot'
        row = plan([job()], n=n, snap=s, attempts=[self.failure(n)])[0]
        self.assertEqual(row['gpus'], [n['gpus'][1]['uuid']])

    def test_boot_change_requires_new_health_but_clears_quarantine(self):
        n = node(); s = snapshot(n); s['boot_id'] = 'new'
        row = plan([job()], n=n, snap=s, attempts=[self.failure(n)])[0]
        self.assertEqual(row['gpus'], [n['gpus'][0]['uuid']])

    def test_missing_device_can_use_remaining_observed_device(self):
        n = node(); s = snapshot(n)
        s['gpu_unavailable_uuids'] = [n['gpus'][0]['uuid']]; s['gpus'] = s['gpus'][1:]
        self.assertEqual(plan([job()], n=n, snap=s)[0]['gpus'], [n['gpus'][1]['uuid']])

    def test_missing_temperature_is_not_bypassed(self):
        n = node(); s = snapshot(n); s['gpus'][1]['temperature_c'] = None
        self.assertEqual(plan([job()], n=n, snap=s)[0]['decision'], 'waiting')

    def test_retry_bounded_and_science_preserved(self):
        s = experiment([job()])['jobs'][0]; original = copy.deepcopy(s)
        report = self.failure(node())['report']
        for count in range(1, 4):
            s = retry_spec(s, report, count)
            self.assertEqual(s['metadata']['gpu_startup_failovers'], count)
        self.assertIsNone(retry_spec(s, report, 4))
        for key in ('resources', 'config', 'depends_on', 'hosts', 'argv'):
            self.assertEqual(s[key], original[key])
        for report in ({'status': 'unknown'}, {'status': 'failed'}, dict(report, ready=True)):
            self.assertIsNone(retry_spec(original, report, 1))

    def test_only_exact_pretraining_failure_classified(self):
        with tempfile.TemporaryDirectory() as tmp:
            p = Path(tmp); (p/'smoke').mkdir()
            (p/'smoke/rank0.stdout').write_text('CUDA device is not set properly\nExpected exactly one visible GPU')
            req = dict(attempt_dir=tmp, gpus=['GPU-a'], job_spec={'kind':'train'})
            state = dict(status='failed', ready=False, boot_id='boot')
            self.assertEqual(gpu_startup_failure(req, state)['failure_class'], 'gpu_startup_unavailable')
            (p/'run').mkdir(); (p/'run/TRAIN_PROGRESS.json').write_text('{}')
            self.assertEqual(gpu_startup_failure(req, state), state)


if __name__ == '__main__':
    unittest.main()
