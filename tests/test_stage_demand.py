import copy
import time
import unittest
from test_scheduler import node, snapshot, job, experiment
from research_scheduler.artifacts import (
    dependency_relay_available, dependency_relay_route, dependency_staging_node_key, missing_dependency_count, runnable_stage_demand,
    reserve_transfer_slots, warmed_staging_snapshot)
from research_scheduler.planner import fit


class StageDemandTests(unittest.TestCase):
    def demand(self, mutate=None):
        n = node(key='destination')
        source = node(key='source')
        e = experiment([job('producer', gpu_count=0), job('consumer', deps=['producer'])])
        jobs = {j['id']: dict(id=j['id'], spec=j, experiment='e',
                status='succeeded' if j['id']=='producer' else 'queued') for j in e['jobs']}
        consumer = jobs['consumer']['spec']
        consumer['hosts'] = ['destination']
        now = time.time()
        snap = snapshot(n, now)
        a = dict(node='source', spec=dict(node_spec=source), report={})
        if mutate: mutate(consumer, snap, jobs)
        before = copy.deepcopy(a)
        result = runnable_stage_demand([jobs['consumer']], jobs, {'e':e},
                 {'destination':n}, {'destination':snap}, [], {'producer':a}, {}, now)
        self.assertEqual(a, before)
        return result

    def test_unpublished_dependency_gets_demand(self):
        self.assertIn('producer', self.demand())

    def test_cpu_only_consumer_gets_dependency_relay_demand(self):
        def cpu_only(consumer, _snapshot, _jobs):
            consumer['resources'].update(gpu_count=0, vram_mib=0)
        self.assertIn('producer', self.demand(cpu_only))

    def test_hold_is_not_promoted(self):
        self.assertFalse(self.demand(lambda j,s,b: j['labels'].update(user_hold='yes')))

    def test_stale_destination_is_not_promoted(self):
        self.assertFalse(self.demand(lambda j,s,b: s.update(received_at=s['received_at']-61)))

    def test_insufficient_vram_is_not_promoted(self):
        self.assertFalse(self.demand(lambda j,s,b: j['resources'].update(vram_mib=999999)))

    def test_unfinished_dependency_is_not_promoted(self):
        self.assertFalse(self.demand(lambda j,s,b: b['producer'].update(status='running')))

    def test_dependency_relays_precede_single_archive_lane(self):
        calls = []
        def dependency(live):
            calls.append(('dependency', [x['direction'] for x in live]))
            return 'relay-'+str(len(calls))
        def archive(live):
            calls.append(('archive', [x['direction'] for x in live]))
            return 'archive'
        self.assertEqual(reserve_transfer_slots([], dependency, archive, max_total=4, max_dependency=3),
                         ['relay-1', 'relay-2', 'relay-3', 'archive'])
        self.assertEqual([kind for kind,_ in calls], ['dependency','dependency','dependency','archive'])

    def test_existing_archive_leaves_other_slot_for_dependency(self):
        live = [{'id':'archive-live','direction':'archive','status':'running'}]
        count=iter(range(3))
        self.assertEqual(reserve_transfer_slots(live, lambda _: 'relay-'+str(next(count)), lambda _: 'extra',
                                                max_total=4, max_dependency=3),
                         ['relay-0','relay-1','relay-2'])

    def test_existing_dependency_leaves_other_slot_for_archive(self):
        live = [{'id':'relay-live','direction':'download','status':'running'}]
        count=iter(range(2))
        self.assertEqual(reserve_transfer_slots(live, lambda _: 'extra-'+str(next(count)), lambda _: 'archive',
                                                max_total=4, max_dependency=3),
                         ['extra-0','extra-1','archive'])

    def test_urgent_lab4_dependency_does_not_compete_with_archive(self):
        count=iter(range(4))
        self.assertEqual(reserve_transfer_slots([], lambda _: 'relay-'+str(next(count)), lambda _: 'archive', True,
                                                max_total=4, max_dependency=4),
                         ['relay-0','relay-1','relay-2','relay-3'])

    def test_dependency_route_respects_farm_lab_firewall(self):
        nodes={key:node(key=key) for key in ('farm9-gui2','lab4','rp2','cps1-model')}
        self.assertEqual(dependency_relay_route(nodes['farm9-gui2'],nodes['lab4']),
                         'controller-local-staging')
        self.assertEqual(dependency_relay_route(nodes['lab4'],nodes['farm9-gui2']),
                         'controller-local-staging')
        self.assertEqual(dependency_relay_route(nodes['rp2'],nodes['farm9-gui2']),'direct-stream')
        self.assertEqual(dependency_relay_route(nodes['cps1-model'],nodes['lab4']),'direct-stream')

    def test_default_transfer_capacity_is_twenty_four_dependencies_plus_eight_archives(self):
        counter=iter(range(24))
        archive=iter(range(8))
        result=reserve_transfer_slots([],lambda _: 'relay-'+str(next(counter)),lambda _: 'archive-'+str(next(archive)))
        self.assertEqual(len(result),32)
        self.assertEqual(result[-8:],['archive-'+str(i) for i in range(8)])

    def test_destination_accepts_twenty_four_distinct_relays_but_not_duplicates(self):
        live=[{'id':'r'+str(i),'attempt':'a'+str(i),'node':'lab4',
               'direction':'download','status':'running'} for i in range(23)]
        self.assertTrue(dependency_relay_available(live,'new','lab4'))
        self.assertFalse(dependency_relay_available(live,'a1','lab4'))
        live.append({'id':'r23','attempt':'a23','node':'lab4','direction':'download','status':'running'})
        self.assertFalse(dependency_relay_available(live,'new','lab4'))

    def test_candidate_with_partial_verified_inputs_is_completed_first(self):
        source=node(key='source')
        destination=node(key='destination')
        spec=job('consumer',deps=['head','weights'])
        successful={
            'head':dict(node='source',spec={'node_spec':source},artifact_locations={'destination':{}}),
            'weights':dict(node='source',spec={'node_spec':source},artifact_locations={}),
        }
        self.assertEqual(missing_dependency_count(spec,successful,destination),1)
        empty=node(key='empty')
        self.assertEqual(missing_dependency_count(spec,successful,empty),2)

    def test_gpu_pool_priority_precedes_partial_relay_convenience(self):
        source=node(key='source')
        rp2=node(key='rp2')
        lab6=node(key='lab6')
        spec=job('consumer',deps=['head','weights'])
        successful={
            'head':dict(node='source',spec={'node_spec':source},artifact_locations={'lab6':{}}),
            'weights':dict(node='source',spec={'node_spec':source},artifact_locations={}),
        }
        self.assertEqual(missing_dependency_count(spec,successful,rp2),2)
        self.assertEqual(missing_dependency_count(spec,successful,lab6),1)
        self.assertLess(dependency_staging_node_key(spec,successful,rp2,[]),
                        dependency_staging_node_key(spec,successful,lab6,[]))

    def test_preferred_relay_waits_for_bounded_stable_poll_gate(self):
        n=node(key='rp2');n['policy']['stable_polls']=2
        now=time.time();snap=snapshot(n,now)
        snap.update(stable_polls=1,stable_since=now-30)
        for gpu in snap['gpus']:gpu['stable_polls']=1
        warmed=warmed_staging_snapshot(n,snap,now)
        self.assertEqual(warmed['stable_polls'],2)
        self.assertTrue(all(g['stable_polls']==2 for g in warmed['gpus']))
        self.assertIsNone(warmed_staging_snapshot(n,dict(snap,stable_since=now-181),now))

    def test_stable_poll_wait_does_not_bypass_permanent_gpu_gates(self):
        n=node(key='rp2');n['policy']['stable_polls']=2
        now=time.time();snap=snapshot(n,now)
        snap.update(stable_polls=1,stable_since=now-30)
        for gpu in snap['gpus']:gpu['stable_polls']=1
        warmed=warmed_staging_snapshot(n,snap,now)
        oversized=experiment([job('consumer',vram=999999)])['jobs'][0]
        reason,_=fit(oversized,n,warmed,[],[],{}, {},now)
        self.assertIn('not enough healthy GPUs',reason)
        n['enabled']=False
        disabled=experiment([job('disabled')])['jobs'][0]
        reason,_=fit(disabled,n,warmed,[],[],{}, {},now)
        self.assertEqual(reason,'node disabled/drained')

    def test_runnable_verified_destination_stops_replication_to_other_nodes(self):
        ready=node(key='ready')
        empty=node(key='empty')
        source=node(key='source')
        e=experiment([job('producer',gpu_count=0),job('consumer',deps=['producer'])])
        jobs={j['id']:dict(id=j['id'],spec=j,experiment='e',
              status='succeeded' if j['id']=='producer' else 'queued') for j in e['jobs']}
        jobs['consumer']['spec']['hosts']=['ready','empty']
        attempt=dict(node='source',spec={'node_spec':source},report={},
                     artifact_locations={'ready':{'root':'/ready'}})
        now=time.time()
        result=runnable_stage_demand(
            [jobs['consumer']],jobs,{'e':e},{'ready':ready,'empty':empty},
            {'ready':snapshot(ready,now),'empty':snapshot(empty,now)},[],
            {'producer':attempt},{},now)
        self.assertEqual(result,{})

    def test_retired_archive_location_is_not_a_current_relay_source(self):
        # The source-selection invariant is covered through the public demand
        # calculation: historical locations do not make a current destination
        # ready and therefore cannot suppress required staging.
        destination=node(key='destination')
        producer=node(key='retired')
        e=experiment([job('producer',gpu_count=0),job('consumer',deps=['producer'])])
        jobs={j['id']:dict(id=j['id'],spec=j,experiment='e',
              status='succeeded' if j['id']=='producer' else 'queued') for j in e['jobs']}
        jobs['consumer']['spec']['hosts']=['destination']
        successful={'producer':dict(node='retired',spec={'node_spec':producer},
            report={'outputs':{'RESULT.json':{'path':'/gone','sha256':'a'*64,'bytes':1}}},
            artifact_locations={'retired':{'root':'/gone','complete_attempt':True}})}
        now=time.time()
        result=runnable_stage_demand([jobs['consumer']],jobs,{'e':e},
            {'destination':destination},{'destination':snapshot(destination,now)},[],successful,{},now)
        self.assertIn('producer',result)
