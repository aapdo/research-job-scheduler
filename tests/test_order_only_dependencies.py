"""Ordering across local stores must not grant implicit artifact access."""
import json
import tempfile
import unittest
from pathlib import Path
from test_scheduler import node, snapshot, job, experiment, plan, reservation
from research_scheduler.controller import Controller
from research_scheduler.store import Store, dumps


class OrderOnlyDependenciesTests(unittest.TestCase):
    def test_cross_store_ordering_requires_success_and_opt_in(self):
        a,b=node(),node(key='b')
        done=reservation(a,key='first');done.update(status='succeeded',released=True)
        done['spec']['node_spec']=a
        child=job('child',deps=['first'])
        kwargs=dict(nodes={'b':b},snaps={'b':snapshot(b)},attempts=[done],statuses={'first':'succeeded'})
        self.assertEqual(plan([job('first'),child],**kwargs)[0]['decision'],'waiting')
        child['order_only_dependencies']=['first']
        self.assertEqual(plan([job('first'),child],**kwargs)[0]['decision'],'ready')
        kwargs['statuses']['first']='failed'
        self.assertEqual(plan([job('first'),child],**kwargs)[0]['decision'],'blocked')

    def test_order_only_cannot_reference_remote_artifact(self):
        child=job('child',deps=['first']);child['order_only_dependencies']=['first']
        for field,value in [('argv',['cat','{dep:first}/weights']),('config',{'nested':['{dep:first}']})]:
            c=dict(child);c[field]=value
            with self.assertRaises(ValueError):experiment([job('first'),c])
        child['order_only_dependencies']=['unknown']
        with self.assertRaises(ValueError):experiment([job('first'),child])

    def test_order_only_request_does_not_rehash_unreachable_files(self):
        with tempfile.TemporaryDirectory() as root:
            s=Store(Path(root)/'state.db');s.register_node(node(key='b'))
            first=job('first');child=job('child',deps=['first'])
            child['order_only_dependencies']=['first']
            s.register_experiment(experiment([first,child]))
            with s.db:
                s.db.execute("UPDATE jobs SET status='succeeded' WHERE id='first'")
                s.db.execute('INSERT INTO attempts(id,job,node,spec,status,created,report) VALUES(?,?,?,?,?,?,?)',
                    ('done','first','a',dumps({'attempt_dir':'/a/private'}),'succeeded',0,
                     dumps({'outputs':{'x':{'path':'/a/private/x','sha256':'0'*64}}})))
            r=Controller(s).request({'job':'child','node':'b','gpus':[]})
            self.assertEqual(r['input_files'],[])
            self.assertNotIn('/a/private',json.dumps(r['config']))
            s.db.close()


if __name__=='__main__':unittest.main()
