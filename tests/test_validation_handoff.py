"""A successful host gate should release consumers ahead of more validation."""
import unittest
from test_scheduler import node,snapshot,job,experiment
from research_scheduler.planner import placements

class ValidationHandoffTests(unittest.TestCase):
    def setup_rows(self,blocked=False):
        model=experiment([job('model',deps=['missing'] if blocked else [])],key='model-exp')
        probe=job('EXEC_VERIFY_probe');probe['kind']='prepare'
        validation=experiment([probe],key='probe-exp');validation.update(priority=20000,project='execution-preparation')
        jobs=[dict(id=j['id'],spec=j,experiment=e['id'],status='queued',created=0) for e in [validation,model] for j in e['jobs']]
        exps={e['id']:e for e in [validation,model]}
        if blocked:
            prior=job('missing');prior=experiment([prior],key='prior');exps['prior']=prior
            jobs.append(dict(id='missing',spec=prior['jobs'][0],experiment='prior',status='running',created=0))
        return jobs,exps
    def test_consumer_first_and_validation_still_runs_in_parallel(self):
        jobs,exps=self.setup_rows();n=node()
        result=placements(jobs,exps,{'a':n},{'a':snapshot(n)},[],{})
        self.assertEqual([r['job'] for r in result],['model','EXEC_VERIFY_probe'])
        self.assertTrue(all(r['decision']=='ready' for r in result))
        self.assertNotEqual(result[0]['gpus'],result[1]['gpus'])
    def test_dependency_wait_does_not_hold_validation(self):
        jobs,exps=self.setup_rows(True);n=node()
        result=placements(jobs,exps,{'a':n},{'a':snapshot(n)},[],{})
        self.assertEqual(next(r for r in result if r['job']=='model')['decision'],'blocked')
        self.assertEqual(next(r for r in result if r['job']=='EXEC_VERIFY_probe')['decision'],'ready')

if __name__=='__main__':unittest.main()
