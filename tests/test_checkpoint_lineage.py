import unittest
from test_scheduler import node, job, plan, reservation


class CheckpointLineageTests(unittest.TestCase):
    def cases(self):
        producer = job('producer')
        early = job('early')
        early['metadata'] = {'epoch_dependency': {'source_job':'producer', 'source_attempt':'attempt-original'}}
        report = job('report', deps=['early'])
        attempt = reservation(node(), key='attempt-original')
        attempt['job'] = 'producer'
        return [producer, early, report], attempt

    def test_same_running_producer_allows_early_artifact(self):
        jobs, a = self.cases()
        result = plan(jobs, attempts=[a], statuses={'producer':'running'})
        self.assertEqual(next(r for r in result if r['job']=='early')['decision'], 'ready')

    def test_retry_blocks_direct_and_transitive_consumers(self):
        jobs, a = self.cases()
        new = dict(a, id='attempt-retry', created=a['created']+1)
        for statuses in ({'producer':'running'}, {'producer':'succeeded','early':'succeeded'}):
            rows = plan(jobs, attempts=[dict(a, status='failed'), new], statuses=statuses)
            for row in rows:
                if row['job'] in ('early','report'):
                    self.assertEqual(row['decision'], 'blocked')
                    self.assertIn('checkpoint lineage', row['reason'])

    def test_failed_original_cannot_launch_new_descendants(self):
        jobs, a = self.cases()
        rows = plan(jobs, attempts=[dict(a, status='failed')], statuses={'producer':'failed'})
        self.assertIn('not running/successful', next(r for r in rows if r['job']=='early')['reason'])

    def test_artifact_transfer_reservation_is_not_a_new_producer_attempt(self):
        jobs, a = self.cases()
        transfer = dict(a, id='hf-transfer', created=a['created']+1)
        transfer.pop('job')
        transfer['spec'] = dict(a['spec'], gpus=[], resources=dict(a['spec']['resources'], gpu_count=0))
        rows = plan(jobs, attempts=[a, transfer], statuses={'producer':'running'})
        self.assertEqual(next(r for r in rows if r['job']=='early')['decision'], 'ready')


if __name__ == '__main__': unittest.main()
