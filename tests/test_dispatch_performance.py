"""Bounded parallel observation, selective reads and serial placement safety."""
import copy
import tempfile
import threading
import time
import unittest
from pathlib import Path
from unittest.mock import patch

from research_scheduler.controller import Controller
from research_scheduler.store import Store, dumps
from test_scheduler import node, snapshot, experiment, job


class ParallelStatusTests(unittest.TestCase):
    def test_parallel_nodes_serial_attempts_and_preserved_order(self):
        barrier = threading.Barrier(2)
        calls = []
        main_thread = threading.get_ident()
        class Transport:
            def call(self, node, action, request):
                self_test.assertNotEqual(threading.get_ident(), main_thread)
                self_test.assertEqual(action, 'status')
                if request['id'].endswith('1'): barrier.wait(timeout=3)
                calls.append(request['id'])
                if request['id'] == 'b2': raise TimeoutError('bounded error')
                return dict(status='running', ready=True)
        self_test = self
        attempts = [dict(id=k, node=k[0], report={}, spec=dict(id=k, node_spec={'id': k[0]}))
                    for k in ['a1', 'b1', 'a2', 'b2', 'c1']]
        before = copy.deepcopy(attempts)
        result = Controller(None, Transport())._status_reports(attempts, {'c': {'phase': 'unavailable'}})
        self.assertEqual([a['id'] for a, _ in result], [a['id'] for a in attempts])
        self.assertLess(calls.index('a1'), calls.index('a2'))
        self.assertLess(calls.index('b1'), calls.index('b2'))
        self.assertNotIn('c1', calls)
        self.assertEqual([r['status'] for _, r in result], ['running', 'running', 'running', 'unknown', 'unknown'])
        self.assertEqual(attempts, before)


class DispatchReadTests(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.store = Store(Path(self.tmp.name)/'db')
        self.n = node()
        self.store.register_node(self.n)
        self.store.register_experiment(experiment([job()]))

    def tearDown(self):
        self.store.db.close()
        self.tmp.cleanup()

    def test_request_reads_only_selected_jobs_and_dependency_summaries(self):
        controller=Controller(self.store)
        statements=[];self.store.db.set_trace_callback(statements.append)
        with patch.object(self.store,'jobs',side_effect=AssertionError('full job scan')), \
                patch.object(self.store,'attempts',wraps=self.store.attempts) as attempts:
            request=controller.request(dict(job='j',node='a',gpus=['GPU-a-0']))
        self.assertEqual(request['experiment_spec']['id'],'e')
        self.assertEqual(attempts.call_args.kwargs,dict(job_ids=set(),summary=True))
        self.assertFalse(any(q.strip().upper()=='SELECT * FROM JOBS' for q in statements))
        self.assertFalse(any(q.strip().upper()=='SELECT * FROM EXPERIMENTS' for q in statements))

    def test_twenty_four_launches_keep_unique_gpu_reservations(self):
        for i in range(1,12):
            n=node(key='n'+str(i));n.update(transport='ssh',target='node'+str(i))
            self.store.register_node(n)
        self.store.register_experiment(experiment([job('extra'+str(i)) for i in range(23)],key='batch24'))
        class Transport:
            def call(self,node,action,request):
                if action=='probe':return snapshot(node)
                if action=='launch':return dict(status='starting')
                raise AssertionError(action)
        result=Controller(self.store,Transport()).tick(execute=True,warmup=False,max_launches=24,parallel_launches=True)
        self.assertEqual(len(result['launches']),24)
        self.assertEqual(len({(p['node'],g) for p in result['launches'] for g in p['gpus']}),24)

    def test_batch_context_is_read_once(self):
        self.store.register_experiment(experiment([job('j2')],key='e2'))
        controller=Controller(self.store)
        choices=[dict(job='j',node='a',gpus=['GPU-a-0']),dict(job='j2',node='a',gpus=['GPU-a-1'])]
        with patch.object(self.store,'attempts',wraps=self.store.attempts) as attempts:
            context=controller._request_context(choices)
            requests=[controller.request(p,context=context) for p in choices]
        self.assertEqual(attempts.call_count,1)
        self.assertEqual([r['job'] for r in requests],['j','j2'])

    def test_expired_batch_refreshes_and_replans_without_stale_reservation(self):
        clock=[1000.0];probes=[]
        class Transport:
            def call(self,node,action,request):
                if action=='probe':probes.append(clock[0]);return snapshot(node,now=clock[0])
                if action=='launch':return dict(status='running')
                raise AssertionError(action)
        controller=Controller(self.store,Transport());original=controller.request;calls=[]
        def slow_once(p,context=None):
            if not calls:clock[0]+=61
            calls.append(p['job']);return original(p,context=context)
        with patch('research_scheduler.controller.time.time',side_effect=lambda:clock[0]), \
                patch.object(controller,'request',side_effect=slow_once):
            result=controller.tick(execute=True,warmup=False,parallel_launches=True)
        self.assertEqual(len(result['launches']),1)
        self.assertEqual(len(self.store.attempts()),1)
        self.assertEqual(len(calls),2)
        self.assertIn(1061.0,probes)

    def test_expired_batch_rechecks_capacity_instead_of_reusing_old_plan(self):
        clock=[1000.0]
        class Transport:
            def call(self,node,action,request):
                if action=='probe':
                    snap=snapshot(node,now=clock[0])
                    if clock[0]>1000:snap['ram_available_mib']=0
                    return snap
                raise AssertionError('Must not launch after resource loss')
        controller=Controller(self.store,Transport());original=controller.request
        def slow(p,context=None):clock[0]+=61;return original(p,context=context)
        with patch('research_scheduler.controller.time.time',side_effect=lambda:clock[0]),patch.object(controller,'request',side_effect=slow):
            result=controller.tick(execute=True,warmup=False,parallel_launches=True)
        self.assertEqual(result['launches'],[]);self.assertEqual(self.store.attempts(),[])

    def test_summary_keeps_frozen_audit_and_fresh_runtime_separate(self):
        key = self.store.jobs()[0]['id']
        spec = dict(experiment_spec={'large': 'x'*100000}, node_spec=self.n,
                    resources={'cpu': 1}, attempt_dir='/tmp/a')
        with self.store.db:
            self.store.db.execute('INSERT INTO attempts(id,job,node,spec,status,created) VALUES(?,?,?,?,?,?)',
                                  ('a', key, 'a', dumps(spec), 'running', time.time()))
        full = self.store.attempts()
        compact = self.store.attempts(summary=True)
        self.assertEqual(compact[0]['spec'], {k:v for k,v in spec.items() if k != 'experiment_spec'})
        self.assertEqual(full[0]['spec'], spec)
        self.assertEqual(self.store.attempts(job_ids=set()), [])
        self.assertEqual(self.store.attempts(job_ids={'absent'}), [])
        compact[0]['spec']['resources']['cpu'] = 999
        with self.store.db:
            self.store.db.execute("UPDATE attempts SET status='failed' WHERE id='a'")
        self.assertEqual(self.store.attempts(summary=True)[0]['status'], 'failed')
        self.assertEqual(self.store.attempts()[0]['spec'], spec)

    def test_persistent_loop_counts_three_independent_polls_without_warmup_sleep(self):
        self.n['policy']['stable_polls'] = 3
        self.store.register_node(self.n)
        clock = [1000.0]
        n = self.n
        class Probe:
            def call(self, node, action, request):
                self_test.assertEqual(action, 'probe')
                return snapshot(node, now=clock[0])
        self_test = self
        controller = Controller(self.store, Probe())
        with patch('research_scheduler.controller.time.time', side_effect=lambda: clock[0]), \
                patch('research_scheduler.controller.time.sleep') as sleep, \
                patch.object(controller, '_launch', side_effect=lambda p:p):
            for poll in range(3):
                result = controller.tick(execute=True, warmup=False)
                self.assertEqual(len(result['launches']), int(poll == 2))
                clock[0] += 15
            sleep.assert_not_called()

    def test_no_runnable_job_plans_once(self):
        n = self.n
        class Probe:
            def call(self, node, action, request):
                return dict(snapshot(node), ram_available_mib=0)
        controller = Controller(self.store, Probe())
        with patch.object(controller, 'plan', wraps=controller.plan) as plan:
            result = controller.tick(execute=True, warmup=False)
        self.assertEqual(plan.call_count, 1)
        self.assertEqual(result['launches'], [])

    def test_lock_wait_is_bounded_and_never_bypasses_owner(self):
        other = Store(self.store.path)
        other.lock_wait_s = .03
        try:
            with self.store.lock():
                started = time.monotonic()
                with self.assertRaises(RuntimeError):
                    with other.lock(poll_interval=.005): pass
                self.assertGreaterEqual(time.monotonic()-started, .03)
                with patch('research_scheduler.store.time.sleep') as sleep:
                    with self.assertRaises(RuntimeError):
                        with other.lock(timeout=0): pass
                    sleep.assert_not_called()
            with other.lock(): pass
        finally:
            other.db.close()

    def test_launch_budget_yields_only_between_complete_launches(self):
        clock = [1000.0]
        class Probe:
            def call(self, node, action, request): return snapshot(node, now=clock[0])
        controller = Controller(self.store, Probe())
        def launch(placement):
            clock[0] += 6
            return placement
        with patch('research_scheduler.controller.time.time', side_effect=lambda: clock[0]), \
                patch('research_scheduler.controller.time.monotonic', side_effect=lambda: clock[0]), \
                patch.object(controller, '_launch', side_effect=launch) as launched:
            result = controller.tick(execute=True, warmup=False, max_launches=8, launch_budget_s=5)
        self.assertEqual(len(result['launches']), 1)
        self.assertEqual(launched.call_count, 1)

    def test_batch_reuses_fresh_probe_but_preserves_gpu_reservations(self):
        self.store.register_experiment(experiment([job('j2')],key='e2'))
        probes=[]
        class Transport:
            def call(self,node,action,request):
                if action=='probe':probes.append(node['id']);return snapshot(node)
                if action=='launch':return dict(status='starting')
                raise AssertionError(action)
        result=Controller(self.store,Transport()).tick(execute=True,warmup=False,max_launches=2,launch_budget_s=20)
        self.assertEqual(len(result['launches']),2)
        self.assertEqual(len(probes),2)  # cycle probe + one batched revalidation
        self.assertNotEqual(result['launches'][0]['gpus'],result['launches'][1]['gpus'])

    def test_parallel_model_launches_reserve_before_rpc_and_isolate_lost_ack(self):
        self.store.register_experiment(experiment([job('j2')],key='e2'))
        barrier=threading.Barrier(2);path=self.store.path
        class Transport:
            def call(self,node,action,request):
                if action=='probe':return snapshot(node)
                if action=='launch':
                    import sqlite3
                    with sqlite3.connect('file:'+str(path)+'?mode=ro',uri=True) as db:
                        assert db.execute("SELECT COUNT(*) FROM attempts WHERE status='starting'").fetchone()[0]==2
                    barrier.wait(timeout=2)
                    if request['job']=='j2':raise TimeoutError('lost ack')
                    return dict(status='running')
                raise AssertionError(action)
        result=Controller(self.store,Transport()).tick(execute=True,warmup=False,max_launches=2,parallel_launches=True)
        self.assertEqual(len(result['launches']),2)
        self.assertEqual({p['status'] for p in result['launches']},{'running','unknown'})
        self.assertNotEqual(result['launches'][0]['gpus'],result['launches'][1]['gpus'])
        self.assertEqual(len(self.store.attempts(active=True)),2)
