import unittest

from research_scheduler.controller import Controller, status_request


def attempt(key, node):
    return {'id':key,'node':node,'report':{},
            'spec':{'id':key,'node_spec':{'id':node}}}


class BatchTransport:
    def __init__(self, fail=False):
        self.calls=[]
        self.fail=fail

    def call(self, node, action, request):
        self.calls.append((node['id'],action))
        if action=='status_batch':
            if self.fail:raise RuntimeError('batch unavailable')
            return {a['id']:{'status':'running','ready':True} for a in request['attempts']}
        return {'status':'running','ready':True}


class StatusBatchTests(unittest.TestCase):
    def test_one_rpc_per_node_and_attempt_order_preserved(self):
        transport=BatchTransport()
        rows=[attempt('a1','a'),attempt('b1','b'),attempt('a2','a')]
        result=Controller(None,transport)._status_reports(rows,{})
        self.assertEqual([a['id'] for a,_ in result],['a1','b1','a2'])
        self.assertEqual(sorted(transport.calls),[('a','status_batch'),('b','status_batch')])

    def test_lost_batch_response_is_bounded_and_keeps_unknown(self):
        transport=BatchTransport(fail=True)
        result=Controller(None,transport)._status_reports(
            [attempt('a1','a'),attempt('a2','a')],{})
        self.assertTrue(all(report['status']=='unknown' for _,report in result))
        self.assertEqual(transport.calls,[('a','status_batch')])

    def test_status_payload_preserves_identity_and_oom_inputs_without_launch_bundle(self):
        a=attempt('a1','a')
        a['spec'].update(attempt_dir='/runs/a1', job='job1', resources={'tokens':{},'vram_mib':3072},
                         config={'mode':'diagnose','large':'x'*100000},
                         experiment_spec={'large':'x'*100000}, argv=['x'*100000],
                         job_spec={'kind':'eval','metadata':{'oom_same_host_retry_allowed':True}})
        a['report']={'oom_observation':{'boot_id':'b'},'failure_class':'experiment_oom','failure_evidence':{'attempt':'a1'}}
        r=status_request(a)
        self.assertEqual(r['attempt_dir'],'/runs/a1')
        self.assertEqual(r['_oom_previous'],{'boot_id':'b'})
        self.assertEqual(r['_oom_verified_evidence'],{'attempt':'a1'})
        self.assertTrue(r['job_spec']['metadata']['oom_same_host_retry_allowed'])
        self.assertLess(len(str(r)),1000)
        self.assertIn('experiment_spec',a['spec'])


if __name__=='__main__':unittest.main()
