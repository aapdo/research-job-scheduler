import copy
import time
import unittest
from test_scheduler import node, job, plan, reservation, experiment
from research_scheduler.recovery import current_campaign_jobs, output_manifest_sha256


class RecoveryAuthoritiesTests(unittest.TestCase):
    def frozen_case(self):
        producer = job('producer')
        proxy = job('proxy', deps=['producer'])
        frozen = reservation(node(), key='proxy-run', status='succeeded')
        frozen['job'] = 'proxy'
        frozen['report'] = dict(returncode=0, outputs={'proxy.npz': dict(sha256='a'*64, bytes=20)})
        proxy['metadata'] = dict(
            epoch_dependency=dict(source_job='producer', source_attempt='failed-producer'),
            frozen_initialization_authority=dict(attempt='proxy-run', source_epoch=0,
                evidence='Verified frozen E0 covariance; not final training evidence',
                outputs_sha256=output_manifest_sha256(frozen['report']['outputs'])))
        child = job('candidate', deps=['proxy'])
        return [producer, proxy, child], frozen

    def test_frozen_e0_does_not_follow_failed_training(self):
        jobs, frozen = self.frozen_case()
        rows = plan(jobs, attempts=[frozen], statuses={'producer':'failed', 'proxy':'succeeded'})
        self.assertEqual(rows[0]['job'], 'candidate')
        self.assertEqual(rows[0]['decision'], 'ready')

    def test_changed_output_or_attempt_blocks(self):
        for mutation in ('output', 'attempt', 'epoch'):
            jobs, frozen = self.frozen_case()
            if mutation == 'output': frozen['report']['outputs']['proxy.npz']['sha256'] = 'b'*64
            elif mutation == 'attempt': frozen['id'] = 'different-run'
            else: jobs[1]['metadata']['frozen_initialization_authority']['source_epoch'] = 1
            rows = plan(jobs, attempts=[frozen], statuses={'producer':'failed', 'proxy':'succeeded'})
            self.assertEqual(rows[0]['decision'], 'blocked')

    def test_missing_frozen_authority_keeps_original_guard(self):
        jobs, frozen = self.frozen_case()
        jobs[1]['metadata'].pop('frozen_initialization_authority')
        rows = plan(jobs, attempts=[frozen], statuses={'producer':'failed', 'proxy':'succeeded'})
        self.assertEqual(rows[0]['decision'], 'blocked')

    def replacement_case(self):
        old = dict(id='old', experiment='e', status='failed', spec=dict(kind='train',
            config={'plan_sha256':'plan'}, metadata={'recovery_replacement':{'job':'new','evidence':'operator approved'}}))
        new = dict(id='new', experiment='other', status='running', spec=dict(kind='train',
            config={'plan_sha256':'plan'}, metadata={'independent_restart':{'original_job':'old'}}))
        return [old, new], {'e': {'project':'main'},'other':{'project':'recovery'}}, {'projects':['main'], 'experiments':[]}

    def test_explicit_replacement_is_counted_without_erasing_history(self):
        jobs, experiments, campaign = self.replacement_case()
        before = copy.deepcopy(jobs)
        self.assertEqual([x['id'] for x in current_campaign_jobs(jobs, experiments, campaign)], ['new'])
        self.assertEqual(jobs, before)

    def test_invalid_replacement_does_not_hide_failure(self):
        jobs, experiments, campaign = self.replacement_case()
        jobs[1]['spec']['config']['plan_sha256'] = 'other'
        self.assertEqual(current_campaign_jobs(jobs, experiments, campaign)[0]['status'], 'failed')
