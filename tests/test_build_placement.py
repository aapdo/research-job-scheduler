"""Load ranking is common to campaigns; never changes existing attempts."""
import copy
import unittest
from test_scheduler import node, snapshot, plan, job
from test_rtl_workflows import rtl_job, attempt


class BuildPlacementTests(unittest.TestCase):
    def test_cpu_preference_repeats_only_after_all_hosts_get_one(self):
        from research_scheduler.build_placement import placement_key
        order=['cps2','cps1','farm8','farm9']
        nodes={key:node(key=key) for key in order}
        for i,n in enumerate(nodes.values()):n.update(admission_priority=40-i*10,rtl_build_slots=4)
        snaps={key:snapshot(n) for key,n in nodes.items()};held=[];result=[]
        for i in range(8):
            j=rtl_job(str(i),'rtl_sim');req=attempt(nodes['cps2'],j)['spec']['resources']
            selected=min(nodes,key=lambda key:placement_key(nodes[key],snaps[key],held,req,100,0,0,[],kind='rtl_sim'))
            result.append(selected);a=attempt(nodes[selected],j);a['job']=str(i);held.append(a)
        self.assertEqual(result,order*2)

    def setup_pair(self):
        a, b = node(key='a'), node(key='b')
        for n in (a, b): n.update(rtl_build_slots=2)
        a['admission_priority']=40; b['admission_priority']=10
        return {'a':a,'b':b}, {'a':snapshot(a),'b':snapshot(b)}

    def test_empty_host_beats_preferred_host_with_spare_slot(self):
        ns, ss=self.setup_pair(); old=attempt(ns['a'],rtl_job())
        before=copy.deepcopy(old)
        row=plan([rtl_job()],nodes=ns,snaps=ss,attempts=[old])[0]
        self.assertEqual(row['node'],'b');self.assertEqual(old,before)
        self.assertEqual(row['build_placement']['policy'],'spread-first-v2')

    def test_admissible_cpu_load_does_not_override_cpu_preference(self):
        ns,ss=self.setup_pair();ss['a']['cpu_percent']=85
        self.assertEqual(plan([rtl_job()],nodes=ns,snaps=ss)[0]['node'],'a')

    def test_admissible_ram_headroom_does_not_override_cpu_preference(self):
        ns,ss=self.setup_pair();ss['a']['ram_available_mib']=600
        self.assertEqual(plan([rtl_job()],nodes=ns,snaps=ss)[0]['node'],'a')

    def test_admissible_disk_headroom_does_not_override_cpu_preference(self):
        ns,ss=self.setup_pair();j=rtl_job();j['resources']['disk_mib']=1000
        ss['a']['disk_free_mib']=1200
        self.assertEqual(plan([j],nodes=ns,snaps=ss)[0]['node'],'a')

    def test_priority_only_breaks_similar_pressure_ties(self):
        ns,ss=self.setup_pair();ss['a']['cpu_percent']=11
        self.assertEqual(plan([rtl_job()],nodes=ns,snaps=ss)[0]['node'],'a')

    def test_physical_host_alias_reservations_count(self):
        ns,ss=self.setup_pair();alias=copy.deepcopy(ns['a']);alias['id']='old-container'
        alias['physical_host']='host-a';ns['a']['physical_host']='host-a'
        self.assertEqual(plan([rtl_job()],nodes=ns,snaps=ss,
            attempts=[attempt(alias,rtl_job())])[0]['node'],'b')

    def test_health_gate_still_excludes_empty_unhealthy_host(self):
        ns,ss=self.setup_pair();ss['b']['d_state']=1
        self.assertEqual(plan([rtl_job()],nodes=ns,snaps=ss,
            attempts=[attempt(ns['a'],rtl_job())])[0]['node'],'a')

    def test_gpu_priority_unchanged(self):
        ns,ss=self.setup_pair();ss['a']['cpu_percent']=85
        self.assertEqual(plan([job()],nodes=ns,snaps=ss)[0]['node'],'a')

    def test_planned_builds_spread_and_ceiling_is_not_target(self):
        ns,ss=self.setup_pair()
        rows=plan([rtl_job('one'),rtl_job('two')],nodes=ns,snaps=ss)
        self.assertEqual([r['node'] for r in rows],['a','b'])

    def test_validation_spreads_before_preferred_host_gets_second_job(self):
        ns,ss=self.setup_pair()
        rows=plan([rtl_job('one','rtl_sim'),rtl_job('two','rtl_sim')],nodes=ns,snaps=ss)
        self.assertEqual([r['node'] for r in rows],['a','b'])

    def test_raw_occupancy_beats_capacity_ratio_and_resource_pressure(self):
        from research_scheduler.build_placement import placement_key
        ns,ss=self.setup_pair();ns['a']['rtl_build_slots']=4
        old=attempt(ns['a'],rtl_job())
        ss['b']['cpu_percent']=60
        req=old['spec']['resources']
        self.assertLess(placement_key(ns['b'],ss['b'],[old],req,100,0,0,[],kind='rtl_build'),
                        placement_key(ns['a'],ss['a'],[old],req,100,1,0,[],kind='rtl_build'))

    def test_validation_and_build_share_physical_host_workload(self):
        from research_scheduler.build_placement import hardware_workload
        ns,ss=self.setup_pair();alias=copy.deepcopy(ns['a']);alias['id']='alias'
        ns['a']['physical_host']=alias['physical_host']='host-a'
        old=attempt(alias,rtl_job('validation','rtl_sim'))
        self.assertEqual(hardware_workload(ns['a'],[old]),1)

    def test_validations_fill_one_round_before_next(self):
        from research_scheduler.build_placement import placement_key
        ns,ss=self.setup_pair();ns['a']['rtl_build_slots']=4
        held=[];chosen=[]
        for i in range(4):
            req=attempt(ns['a'],rtl_job(str(i),'rtl_sim'))['spec']['resources']
            key=min(ns,key=lambda n:placement_key(ns[n],ss[n],held,req,100,0,0,[],kind='rtl_sim'))
            chosen.append(key);a=attempt(ns[key],rtl_job(str(i),'rtl_sim'));a['job']=str(i);held.append(a)
        self.assertEqual(chosen,['a','b','a','b'])

if __name__=='__main__':unittest.main()
