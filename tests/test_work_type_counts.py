import unittest
from research_scheduler.waiting import summarize_by_type,work_type
from research_scheduler.progress_notifications import messages


class WorkTypeCountsTest(unittest.TestCase):
    def test_train_eval_and_support_are_disjoint(self):
        jobs=[dict(id='train',status='queued',spec=dict(kind='train',depends_on=[])),
              dict(id='eval',status='queued',spec=dict(kind='eval',depends_on=['train'])),
              dict(id='smoke',status='succeeded',spec=dict(kind='train',config={'smoke_only':True})),
              dict(id='publication',status='succeeded',spec=dict(kind='prepare'))]
        counts=summarize_by_type(jobs,{j['id']:j for j in jobs})
        self.assertEqual(counts,dict(train={'resource_wait':1},eval={'dependency_wait':1},support={'succeeded':2}))
        self.assertEqual(sum(sum(v.values()) for v in counts.values()),4)
        data=dict(time_kst='now',allocation=dict(assigned_gpus=0,total_gpus=0,active_jobs=0),gpus=[],
                  campaigns=[dict(id='test',counts={},counts_by_type=counts)],hardware=[],warnings=[])
        payloads=messages(data)
        rows=payloads[1]['blocks'][1]['rows']
        self.assertEqual([c['text'] for c in rows[0]], ['캠페인',
            'train-finish','train-run','train-err','train-res-wait','train-dep-wait',
            'eval-finish','eval-run','eval-err','eval-res-wait','eval-dep-wait'])
        self.assertEqual(len(rows),2)
        self.assertEqual([c['text'] for c in rows[1]],['test','0','0','0','1','0','0','0','0','0','1'])
        aux=payloads[2]['blocks'][1]['rows']
        self.assertEqual(aux[1][1]['text'],'보조(smoke·준비·보고서)')
        self.assertEqual(aux[1][2]['text'],'2')

    def test_missing_type_and_additional_states_not_misclassified(self):
        data=dict(time_kst='now',allocation=dict(assigned_gpus=0,total_gpus=0,active_jobs=0),gpus=[],
                  campaigns=[dict(id='train-only',counts={},counts_by_type={
                      'train':dict(succeeded=7,running=2,failed=1,starting=3,unknown=4,blocked=5,cancelled=6)})],hardware=[])
        payloads=messages(data)
        row=payloads[1]['blocks'][1]['rows'][1]
        self.assertEqual([c['text'] for c in row],['train-only','7','2','1','0','0','—','—','—','—','—'])
        aux=payloads[2]['blocks'][1]['rows'][1]
        self.assertEqual(aux[1]['text'],'train 추가 상태')
        self.assertEqual(aux[2]['text'],'18')

    def test_hardware_build_and_test_mapping(self):
        for kind in ('rtl_build','rtl_ooc'):self.assertEqual(work_type({'kind':kind}),'build')
        for kind in ('rtl_sim','board_test'):self.assertEqual(work_type({'kind':kind}),'test')
        self.assertEqual(work_type({'kind':'analysis'}),'support')


if __name__=='__main__':unittest.main()
