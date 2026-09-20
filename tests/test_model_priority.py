import unittest
import tempfile
from pathlib import Path
from research_scheduler.model_priority import priority_for


class ModelPriorityTest(unittest.TestCase):
    def test_quantization_always_above_final_fp32_bank(self):
        for project in ('s-bootstrap-joint20-ptq-w8a8', 's-i3-perchannel-w0-adapter-joint-e5',
                        's-picodet-w0-activation-a8-isolation', 's-bank-final-i3-qat'):
            self.assertGreater(priority_for(dict(project=project, priority=1)),
                               priority_for(dict(project='s-bank-final-i3-top1-taylor-fp32-e0e5', priority=999999)))

    def test_unrelated_and_higher_quant_priority_preserved(self):
        for project in ('rtl-int8-build', 's-bootstrap-colorquant-e5', 's-i3-k6-joint20-beta8-e5'):
            self.assertEqual(priority_for(dict(project=project, priority=1234)),1234)
        self.assertEqual(priority_for(dict(project='s-bootstrap-ptq',priority=10000000)),10000000)

    def test_registration_is_idempotent_after_priority_normalization(self):
        from research_scheduler.store import Store
        with tempfile.TemporaryDirectory() as directory:
            store=Store(str(Path(directory)/'state.db'))
            raw=dict(id='quant_test',name='quant test',project='s-bootstrap-ptq',
                     rq='test priority normalization',priority=100,
                     jobs=[dict(id='quant_job',name='quant job',kind='eval',cwd=directory,argv=['true'])])
            first=store.register_experiment(raw)
            second=store.register_experiment(raw)
            self.assertEqual(first,second)
            self.assertEqual(first['priority'],30000)
            self.assertEqual(store.db.execute('select count(*) from jobs').fetchone()[0],1)
            store.db.close()

if __name__=='__main__':unittest.main()
