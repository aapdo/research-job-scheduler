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

    def test_successful_eval_replacement_is_counted_without_erasing_failure(self):
        old = dict(id='old-eval', experiment='e', status='failed', spec=dict(kind='eval',
            config={'mode':'eval','arm':'learned','epoch':0}, metadata={
                'plan_sha256':'science-plan',
                'recovery_replacement':{'job':'new-eval','evidence':'verified output'}}))
        new = dict(id='new-eval', experiment='other', status='succeeded', spec=dict(kind='eval',
            config={'mode':'eval','arm':'learned','epoch':0}, metadata={
                'plan_sha256':'science-plan',
                'independent_restart':{'original_job':'old-eval','output_sha256':'a'*64}}))
        experiments = {'e': {'project':'main'}, 'other': {'project':'recovery'}}
        campaign = {'projects':['main'], 'experiments':['other']}
        before = copy.deepcopy([old, new])
        self.assertEqual([x['id'] for x in current_campaign_jobs([old, new], experiments, campaign)], ['new-eval'])
        self.assertEqual([old, new], before)

    def test_eval_replacement_requires_success_and_matching_science(self):
        old = dict(id='old-eval', experiment='e', status='failed', spec=dict(kind='eval',
            config={'mode':'eval','arm':'learned','epoch':0}, metadata={
                'plan_sha256':'science-plan',
                'recovery_replacement':{'job':'new-eval','evidence':'verified output'}}))
        new = dict(id='new-eval', experiment='other', status='running', spec=dict(kind='eval',
            config={'mode':'eval','arm':'learned','epoch':0}, metadata={
                'plan_sha256':'science-plan',
                'independent_restart':{'original_job':'old-eval','output_sha256':'a'*64}}))
        experiments = {'e': {'project':'main'}, 'other': {'project':'recovery'}}
        campaign = {'projects':['main'], 'experiments':['other']}
        self.assertEqual(current_campaign_jobs([old, new], experiments, campaign)[0]['id'], 'old-eval')
        new['status'] = 'succeeded';new['spec']['config']['epoch'] = 5
        self.assertEqual(current_campaign_jobs([old, new], experiments, campaign)[0]['id'], 'old-eval')

    def test_successful_analysis_repair_replaces_failed_support_job(self):
        old = dict(id='old-head', experiment='e', status='failed', spec=dict(kind='analysis',
            config={'train_dependency':'train','eval_dependency':'eval'}, metadata={
                'recovery_replacement':{'job':'new-head','evidence':'verified output'}}))
        new = dict(id='new-head', experiment='other', status='succeeded', spec=dict(kind='analysis',
            config={'train_dependency':'train','eval_dependency':'eval'}, metadata={
                'independent_restart':{'original_job':'old-head','output_sha256':'b'*64}}))
        experiments = {'e': {'project':'main'}, 'other': {'project':'repair'}}
        campaign = {'projects':['main'], 'experiments':['other']}
        self.assertEqual([x['id'] for x in current_campaign_jobs([old, new], experiments, campaign)], ['new-head'])
