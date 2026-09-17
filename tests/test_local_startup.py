import copy
import unittest
import tempfile
from research_scheduler.store import Store,dumps
from research_scheduler.controller import Controller
from research_scheduler.startup import startup_group
from test_scheduler import node,job,snapshot,experiment
from research_scheduler.planner import placements

class LocalStartupTests(unittest.TestCase):
    def test_unhealthy_group_peer_does_not_poison_approved_local_data(self):
        with tempfile.TemporaryDirectory() as root:
            s=Store(root+'/state.db');s.register_group({'id':'shared','min_start_interval_s':60})
            good=node(root+'/good',key='good',group='shared');good['filesystem']='local';good['datasets']={'data':'/data/inputs'};good['labels']['local_startup_dataset_paths']=['/data/inputs']
            bad=node(root+'/bad',key='bad',group='shared');bad.update(transport='ssh',target='bad')
            s.register_node(good);s.register_node(bad)
            j=job();j['hosts']=['good'];j['dataset']='data';s.register_experiment(experiment([j]))
            with s.db:
                healthy=dict(snapshot(good),datasets={'data':{'path':'/data/inputs','available':True}})
                s.db.execute('INSERT INTO snapshots VALUES(?,?)',('good',dumps(healthy)))
                s.db.execute('INSERT INTO snapshots VALUES(?,?)',('bad',dumps(dict(snapshot(bad),d_state=1))))
            self.assertEqual(Controller(s).plan()[0]['decision'],'ready')
            s.db.close()

    def test_parallel_plan_and_request_do_not_reserve_shared_slot(self):
        with tempfile.TemporaryDirectory() as root:
            s=Store(root+'/state.db');n=node(root,group='shared');n['filesystem']='local'
            n['labels']['local_startup_dataset_paths']=['/data/inputs']
            s.register_node(n);s.register_group({'id':'shared','min_start_interval_s':60})
            jobs=[dict(job(k),dataset_path='/data/inputs') for k in ['one','two']]
            s.register_experiment(experiment(jobs))
            with s.db:s.db.execute('INSERT INTO snapshots VALUES(?,?)',(n['id'],dumps(snapshot(n))))
            controller=Controller(s);plan=controller.plan()
            self.assertEqual([p['decision'] for p in plan],['ready','ready'])
            self.assertEqual(controller.request(plan[0])['startup_group'],'')
            self.assertEqual(s.specs('nodes')[n['id']]['startup_group'],'shared')
            s.db.close()

    def test_only_exact_approved_local_data_bypasses_group(self):
        n=node(group='shared');n['filesystem']='local';n['datasets']={'data':'/data/inputs'}
        n['labels']['local_startup_dataset_paths']=['/data/inputs']
        j=job();j['dataset']='data'
        self.assertEqual(startup_group(j,n),'')
        j['dataset']='other';self.assertEqual(startup_group(j,n),'shared')
        j.pop('dataset');j['dataset_path']='/data/inputs';self.assertEqual(startup_group(j,n),'')
        n['filesystem']='nfs';self.assertEqual(startup_group(j,n),'shared')

if __name__=='__main__':unittest.main()
