import unittest
from research_scheduler.waiting import waiting_detail, summarize


class WaitingTests(unittest.TestCase):
    def row(self,key,status='queued',deps=()):
        return dict(id=key,status=status,spec=dict(depends_on=list(deps)))

    def test_unfinished_dependency_including_order_gate(self):
        source=self.row('a','running');child=self.row('b',deps=['a'])
        detail=waiting_detail(child,{'a':source,'b':child})
        self.assertEqual(detail['category'],'dependency_wait')
        self.assertEqual(detail['dependencies'],[dict(job='a',status='running')])

    def test_no_or_completed_prerequisites_are_resource_wait(self):
        self.assertEqual(waiting_detail(self.row('b'),{})['category'],'resource_wait')
        self.assertEqual(waiting_detail(self.row('b',deps=['a']),{'a':self.row('a','succeeded')})['category'],'resource_wait')

    def test_actual_failure_and_block_are_not_normal_waits(self):
        for status in ['blocked','failed','unknown','running','starting','succeeded']:
            self.assertIsNone(waiting_detail(self.row('a',status),{}))

    def test_wait_totals_are_conserved(self):
        jobs=[self.row('a'),self.row('b',deps=['a']),self.row('c','blocked')]
        self.assertEqual(summarize(jobs,{j['id']:j for j in jobs}),
                         {'resource_wait':1,'dependency_wait':1,'blocked':1})

    def test_missing_dependency_is_not_resource_shortage(self):
        self.assertEqual(waiting_detail(self.row('b',deps=['missing']),{})['category'],'dependency_wait')

    def test_execution_profile_wait_and_failure_are_not_resource_shortage(self):
        row=self.row('profile');row['spec']['metadata']={'execution_preparation_catalog':'runtime-v1'}
        self.assertEqual(waiting_detail(row,{},{} )['category'],'validation_wait')
        self.assertEqual(waiting_detail(row,{}, {'runtime-v1':['validation_queued','validation_failed']})['category'],'validation_wait')
        detail=waiting_detail(row,{}, {'runtime-v1':['validation_failed','failed']})
        self.assertEqual((detail['category'],detail['label']),('validation_failed','검증 실패'))
        row['spec']['metadata']['execution_profiles']={'farm9':{}}
        self.assertEqual(waiting_detail(row,{}, {'runtime-v1':['validation_failed']})['category'],'resource_wait')

    def test_notification_uses_both_labels(self):
        from research_scheduler.notifications import _message
        text=_message(dict(id='c',name='C',rq='Q'),
                      dict(state='error',counts={'resource_wait':2,'dependency_wait':5}),0)
        self.assertIn('자원 대기 2개',text)
        self.assertIn('선행 대기 5개',text)
