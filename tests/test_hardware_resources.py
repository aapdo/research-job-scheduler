import copy,json,tempfile,unittest
from pathlib import Path
from test_scheduler import node,snapshot,plan,experiment
from test_rtl_workflows import rtl_job
from research_scheduler.hardware_resources import effective_resources
from research_scheduler.store import Store
from research_scheduler.controller import Controller

class HardwareResourceTests(unittest.TestCase):
    def test_portable_build_keeps_cps2_disk_exception_scoped(self):
        j=rtl_job();j['resources'].update(ram_mib=24576,disk_mib=32768)
        j['metadata']={'hardware_resources_by_node':{'cps2':{'disk_mib':8192}}}
        self.assertEqual(effective_resources(j,node(key='cps2'),j['resources'])['disk_mib'],8192)
        self.assertEqual(effective_resources(j,node(key='farm9'),j['resources'])['disk_mib'],32768)
        self.assertEqual(j['resources']['disk_mib'],32768)

    def test_scoped_budget_used_in_plan_and_new_request_only(self):
        with tempfile.TemporaryDirectory() as root:
            n=node(root);n['rtl_build_slots']=2
            n['labels']['build_ram_overrides_mib']=json.dumps({'rtl':24576})
            j=rtl_job();j['resources']['ram_mib']=32768;original=copy.deepcopy(j)
            snap=snapshot(n);snap['ram_available_mib']=30000
            p=plan([j],nodes={'a':n},snaps={'a':snap})[0]
            self.assertEqual(p['decision'],'ready');self.assertEqual(p['resources']['ram_mib'],24576)
            s=Store(Path(root)/'state.db');s.register_node(n);s.register_experiment(experiment([j]))
            request=Controller(s).request(p)
            self.assertEqual(request['resources']['ram_mib'],24576)
            self.assertEqual(request['job_spec']['resources']['ram_mib'],32768)
            self.assertEqual(j,original)
    def test_unrelated_jobs_and_safety_gate_preserved(self):
        n=node();n['rtl_build_slots']=2;n['labels']['build_ram_overrides_mib']='{"rtl":24576}'
        j=rtl_job();j['resources']['ram_mib']=32768
        self.assertEqual(effective_resources(dict(j,kind='rtl_sim'),n,j['resources'])['ram_mib'],32768)
        self.assertEqual(effective_resources(dict(j,id='other'),n,j['resources'])['ram_mib'],32768)
        snap=snapshot(n);snap['ram_available_mib']=20000
        self.assertEqual(plan([j],nodes={'a':n},snaps={'a':snap})[0]['decision'],'waiting')
if __name__=='__main__':unittest.main()
