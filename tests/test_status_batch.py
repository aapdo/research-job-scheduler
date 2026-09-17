import unittest

from research_scheduler.controller import Controller


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

    def test_lost_batch_response_falls_back_to_exact_attempts(self):
        transport=BatchTransport(fail=True)
        result=Controller(None,transport)._status_reports(
            [attempt('a1','a'),attempt('a2','a')],{})
        self.assertTrue(all(report['status']=='running' for _,report in result))
        self.assertEqual(transport.calls,[('a','status_batch'),('a','status'),('a','status')])


if __name__=='__main__':unittest.main()
