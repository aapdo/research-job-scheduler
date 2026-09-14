"""CPU-only, temporary-state tests. Never invoke Vivado, SSH or a real board."""
import copy
import fcntl
import hashlib
import json
import sys
import tempfile
import time
import unittest
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / 'src'))
from research_scheduler.agent import validate_rtl
from research_scheduler.controller import Controller
from research_scheduler.schema import node_spec
from research_scheduler.store import Store, dumps
from test_scheduler import node, snapshot, job, experiment, plan, FakeProbeTransport


TIMING = 'WNS(ns) TNS(ns) TNS_FAIL TOTAL WHS(ns) THS(ns) THS_FAIL TOTAL WPWS(ns) TPWS(ns) TPWS_FAIL TOTAL\n' \
         '------- ------- ----- ----- ------- ------- ----- ----- -------- -------- ----- -----\n' \
         '1.5 0 0 100 0.02 0 0 100 0.7 0 0 50\n'
ROUTE = '# of routable nets........ : 40 :\n# of fully routed nets........ : 40 :\n# of nets with routing errors........ : 0 :\n'


def rtl_job(key='rtl', kind='rtl_build'):
    j = job(key, gpu_count=0, vram=0)
    j.update(kind=kind, filesystem='local', preflight_argv=[sys.executable, '-c', 'pass'])
    if kind in ('rtl_ooc', 'rtl_build'):
        j['validation'] = dict(timing_report='timing.rpt', route_report='route.rpt')
        if kind == 'rtl_build':
            j['validation']['bitstream'] = 'design.bit'
            j['resources']['build_slots'] = 1
        j['outputs'] = list(j['validation'].values())
    elif kind == 'rtl_sim':
        j['validation'] = dict(log='stdout.log', pass_marker='PASS RTL')
        j['outputs'] = ['stdout.log']
    else:
        j['board_id'] = 'cps1'
        j['resources']['tokens'] = {'board.cps1': 1}
        j['validation'] = dict(comparisons=[dict(capture='capture.bin', expected_sha256=hashlib.sha256(b'ok').hexdigest())])
        j['outputs'] = ['capture.bin']
    return j


def attempt(n, j, status='running', age=40):
    normalized = experiment([j])['jobs'][0]
    return dict(id='old', job='old', node=n['id'], created=time.time()-age, status=status,
                released=False, report={}, spec=dict(resources=normalized['resources'],
                    node_spec=n, job_spec=normalized, gpus=[], startup_group=''))


class RTLPlanningTests(unittest.TestCase):
    def test_transient_psi_rechecks_and_recovers_without_disabling_node(self):
        n=node();n['rtl_build_slots']=2
        n['labels']['rtl_memory_psi_full_avg10_limit']='0.5'
        snap=snapshot(n);snap['ram_available_mib']=320000
        original=copy.deepcopy(n)
        for pressure in (1.11,None,float('nan')):
            snap['memory_pressure_full_avg10']=pressure
            result=plan([rtl_job()],n=n,snap=snap)[0]
            self.assertEqual(result['decision'],'waiting')
            self.assertIn('recheck next scheduling cycle',str(result))
        snap['memory_pressure_full_avg10']=0.05
        self.assertEqual(plan([rtl_job()],n=n,snap=snap)[0]['decision'],'ready')
        self.assertEqual(n,original)

    def test_psi_busy_node_falls_back_then_returns_to_preferred_node(self):
        a,b=node(key='a'),node(key='b')
        for n in (a,b):
            n['rtl_build_slots']=2;n['labels']['rtl_memory_psi_full_avg10_limit']='0.5'
        a['admission_priority']=20;b['admission_priority']=10
        snapshots={'a':snapshot(a),'b':snapshot(b)}
        snapshots['a']['memory_pressure_full_avg10']=1.11
        snapshots['b']['memory_pressure_full_avg10']=0.0
        self.assertEqual(plan([rtl_job()],nodes={'a':a,'b':b},snaps=snapshots)[0]['node'],'b')
        snapshots['a']['memory_pressure_full_avg10']=0.05
        self.assertEqual(plan([rtl_job()],nodes={'a':a,'b':b},snaps=snapshots)[0]['node'],'a')

    def test_cpu_only_without_gpus(self):
        n = node(); n['gpus'] = []; n['rtl_build_slots'] = 2
        self.assertEqual(plan([rtl_job()], n=n)[0]['decision'], 'ready')

    def test_slots_and_stagger_are_separate(self):
        n = node(); n['rtl_build_slots'] = 2
        j = rtl_job()
        rows = plan([j, rtl_job('second')], n=n)
        self.assertEqual(rows[0]['decision'], 'ready')
        self.assertIn('stagger', str(rows[1]))
        self.assertEqual(plan([j], n=n, attempts=[attempt(n, j)])[0]['decision'], 'ready')
        n['rtl_build_slots'] = 1
        self.assertIn('slots', str(plan([j], n=n, attempts=[attempt(n, j)])))

    def test_unknown_build_keeps_slot(self):
        n = node(); n['rtl_build_slots'] = 1
        self.assertIn('slots', str(plan([rtl_job()], n=n, attempts=[attempt(n, rtl_job(), 'unknown')])))

    def test_build_cap_and_stagger_span_physical_host_aliases(self):
        a, b = node(key='a'), node(key='b')
        for n in (a, b): n.update(physical_host='farm9', rtl_build_slots=1)
        self.assertIn('slots', str(plan([rtl_job()], n=b, attempts=[attempt(a, rtl_job())])))
        b['rtl_build_slots'] = 2
        self.assertIn('stagger', str(plan([rtl_job()], n=b, attempts=[attempt(a, rtl_job(), age=10)])))
        b.pop('rtl_build_slots')
        self.assertEqual(plan([job('gpu-alias-cpu-job', 0, 0)], n=b, attempts=[attempt(a, rtl_job())])[0]['decision'], 'ready')

    def test_named_resources_serialize_across_nodes_and_dry_run(self):
        a, b = node(key='a'), node(key='b')
        for n in (a, b): n['tokens'] = {'license.vivado': 1}
        first, second = job('first', 0, 0), job('second', 0, 0)
        for j in (first, second): j['resources']['tokens'] = {'license.vivado': 1}
        first['hosts'] = ['a']; second['hosts'] = ['b']
        rows = plan([first, second], nodes={'a': a, 'b': b}, snaps={'a': snapshot(a), 'b': snapshot(b)})
        self.assertEqual(rows[0]['decision'], 'ready')
        self.assertIn('token', str(rows[1]))

    def test_physical_host_cpu_ram_and_unknown_across_aliases(self):
        a, b = node(key='a'), node(key='b')
        for n in (a, b): n['physical_host'] = 'farm9'
        held_job = job('held', 0, 0); held_job['resources']['cpu'] = 16
        self.assertIn('CPU reservations', str(plan([job('new', 0, 0)], n=b, attempts=[attempt(a, held_job)])))
        held_job['resources'].update(cpu=1, ram_mib=64000)
        self.assertIn('RAM reservations', str(plan([job('new', 0, 0)], n=b, attempts=[attempt(a, held_job)])))
        self.assertIn('unknown attempt', str(plan([job('new', 0, 0)], n=b, attempts=[attempt(a, job('held', 0, 0), 'unknown')])))

    def test_disk_reservation(self):
        j = job(gpu_count=0, vram=0); j['resources']['disk_mib'] = 100001
        self.assertIn('disk reservations', str(plan([j])))

    def test_schema_rejects_unsafe_rtl_contracts(self):
        changes = [dict(max_attempts=2), dict(failover_safe=True), dict(filesystem='any'),
                   dict(preflight_argv=[]), dict(validation=dict(timing_report='../escape', route_report='route.rpt', bitstream='design.bit')),
                   dict(outputs=[]), dict(resources=dict(gpu_count=0, build_slots=0)),
                   dict(resources=dict(gpu_count=1, vram_mib=1000, build_slots=1))]
        for change in changes:
            with self.subTest(change=change), self.assertRaises(ValueError):
                j = rtl_job(); j.update(change); experiment([j])
        j = rtl_job(kind='board_test'); j['resources']['tokens'] = {}
        with self.assertRaises(ValueError): experiment([j])

    def test_board_only_runs_at_gateway(self):
        n = node(); n['tokens'] = {'board.cps1': 1}
        self.assertIn('gateway', str(plan([rtl_job(kind='board_test')], n=n)))
        n['board_locks'] = {'cps1': '/tmp/test-board.lock'}
        self.assertEqual(plan([rtl_job(kind='board_test')], n=n)[0]['decision'], 'ready')

    def test_inventory_rejects_duplicate_gateway_and_inconsistent_pool(self):
        with tempfile.TemporaryDirectory() as d:
            store = Store(Path(d)/'state.db')
            self.addCleanup(store.db.close)
            a, b = node(key='a'), node(key='b')
            a.update(tokens={'board.cps1': 1}, board_locks={'cps1': '/tmp/cps1.lock'})
            store.register_node(a)
            b.update(transport='ssh', target='fake-b', tokens={'board.cps1': 1}, board_locks={'cps1': '/tmp/another.lock'})
            with self.assertRaisesRegex(ValueError, 'gateway'): store.register_node(b)
            b.pop('board_locks'); b['tokens'] = {'board.cps1': 2}
            with self.assertRaises(ValueError): store.register_node(b)


class RTLValidationTests(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory(); self.root = Path(self.temp.name)
        self.addCleanup(self.temp.cleanup)
        (self.root/'timing.rpt').write_text(TIMING)
        (self.root/'route.rpt').write_text(ROUTE)
        (self.root/'design.bit').write_bytes(b'bitstream')

    def validate(self):
        return validate_rtl(self.root, 'rtl_build', rtl_job()['validation'])

    def test_complete_build(self):
        r = self.validate(); self.assertEqual(r['status'], 'pass'); self.assertEqual(r['whs_ns'], 0.02)

    def test_timing_failures_and_nonfinite_missing(self):
        for text in [TIMING.replace('1.5', '-1.5'), TIMING.replace('0.02', '-0.02'),
                     TIMING.replace('0.7', '-0.7'), TIMING.replace('1.5', 'NaN'), '']:
            with self.subTest(text=text), self.assertRaises(ValueError):
                (self.root/'timing.rpt').write_text(text); self.validate()

    def test_incomplete_route_or_empty_bitstream(self):
        for route in [ROUTE.replace('errors........ : 0', 'errors........ : 1'), ROUTE.replace('nets........ : 40', 'nets........ : 0'), '']:
            (self.root/'route.rpt').write_text(route)
            with self.assertRaises(ValueError): self.validate()
        (self.root/'route.rpt').write_text(ROUTE); (self.root/'design.bit').write_bytes(b'')
        with self.assertRaises(ValueError): self.validate()

    def test_board_hash_simulation_and_escape(self):
        (self.root/'capture.bin').write_bytes(b'wrong')
        with self.assertRaises(ValueError): validate_rtl(self.root, 'board_test', rtl_job(kind='board_test')['validation'])
        (self.root/'capture.bin').write_bytes(b'ok')
        self.assertEqual(validate_rtl(self.root, 'board_test', rtl_job(kind='board_test')['validation'])['status'], 'pass')
        (self.root/'stdout.log').write_text('FATAL: failed\nPASS RTL\n')
        with self.assertRaises(ValueError): validate_rtl(self.root, 'rtl_sim', rtl_job(kind='rtl_sim')['validation'])
        with self.assertRaises(ValueError): validate_rtl(self.root, 'rtl_sim', dict(log='../outside', pass_marker='PASS'))


class RTLExecutionTests(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory(prefix='rtl-scheduler-test-'); self.root = Path(self.temp.name)
        self.store = Store(self.root/'state.db')
        self.n = node(str(self.root/'runs'))
        self.n.update(gpus=[], rtl_build_slots=2, tokens={'board.cps1': 1}, board_locks={'cps1': str(self.root/'board.lock')})
        self.store.register_node(self.n)
        self.controller = Controller(self.store, FakeProbeTransport())

    def tearDown(self):
        self.store.db.close(); self.temp.cleanup()

    def finish(self):
        for _ in range(80):
            self.controller.tick(execute=True)
            if all(j['status'] in ('succeeded', 'failed') for j in self.store.jobs()): return
            time.sleep(.1)
        self.fail(str(self.store.jobs()))

    def test_exit_zero_bad_timing_is_failed_and_blocks_board(self):
        j = rtl_job(); j['cwd'] = str(self.root)
        j['argv'] = [sys.executable, '-c', 'import os,pathlib; p=pathlib.Path(os.environ["RS_ATTEMPT_DIR"]); '
                     f'(p/"timing.rpt").write_text({TIMING.replace("1.5", "-1.5")!r}); '
                     f'(p/"route.rpt").write_text({ROUTE!r}); (p/"design.bit").write_bytes(b"bit")']
        self.store.register_experiment(experiment([j])); self.finish()
        self.assertEqual(self.store.jobs()[0]['status'], 'failed')
        self.assertIn('gate failed', self.store.attempts()[0]['report']['error'])

    def test_preflight_failure_does_not_launch_workload(self):
        j = rtl_job(kind='rtl_sim'); j['argv'] = [sys.executable, '-c', 'print("PASS RTL")']
        j['preflight_argv'] = [sys.executable, '-c', 'raise SystemExit(7)']
        self.store.register_experiment(experiment([j])); self.finish()
        self.assertEqual(self.store.jobs()[0]['status'], 'failed')
        self.assertFalse((Path(self.store.attempts()[0]['spec']['attempt_dir'])/'stdout.log').exists())

    def test_successful_full_build_receipt(self):
        j = rtl_job(); j['cwd'] = str(self.root)
        j['argv'] = [sys.executable, '-c', 'import os,pathlib; p=pathlib.Path(os.environ["RS_ATTEMPT_DIR"]); '
                     f'(p/"timing.rpt").write_text({TIMING!r}); '
                     f'(p/"route.rpt").write_text({ROUTE!r}); (p/"design.bit").write_bytes(b"bit")']
        self.store.register_experiment(experiment([j])); self.finish()
        self.assertEqual(self.store.jobs()[0]['status'], 'succeeded')
        self.assertEqual(self.store.attempts()[0]['report']['validation']['wns_ns'], 1.5)

    def test_workload_failure_does_not_retain_preflight_reason(self):
        j = rtl_job(kind='rtl_sim')
        j['argv'] = [sys.executable, '-c', 'raise SystemExit(7)']
        self.store.register_experiment(experiment([j])); self.finish()
        self.assertEqual(self.store.jobs()[0]['status'], 'failed')
        self.assertEqual(self.store.jobs()[0]['reason'], 'workload exited with code 7')

    def test_successful_simulation_and_lost_ack_never_duplicate(self):
        j = rtl_job(kind='rtl_sim'); j['argv'] = [sys.executable, '-c', 'print("PASS RTL")']
        self.controller.transport.lost_ack = True
        self.store.register_experiment(experiment([j]))
        self.finish()
        self.assertEqual(self.store.jobs()[0]['status'], 'succeeded')
        self.assertEqual(self.controller.transport.launch_count, 1)
        self.assertEqual(self.store.attempts()[0]['report']['validation']['status'], 'pass')

    def test_external_board_lock_is_held_and_capture_checked(self):
        j = rtl_job(kind='board_test'); j['cwd'] = str(self.root)
        j['argv'] = [sys.executable, '-c', 'import os,pathlib; os.fstat(int(os.environ["RS_BOARD_LOCK_FD"])); '
                     'pathlib.Path(os.environ["RS_ATTEMPT_DIR"],"capture.bin").write_bytes(b"ok")']
        with (self.root/'board.lock').open('a') as lock:
            fcntl.flock(lock, fcntl.LOCK_EX)
            self.store.register_experiment(experiment([j]))
            self.controller.tick(execute=True)
            time.sleep(.7); self.controller.tick()
            a = self.store.attempts()[0]
            self.assertEqual(a['status'], 'starting')
            self.assertFalse((Path(a['spec']['attempt_dir'])/'capture.bin').exists())
        self.finish()
        self.assertEqual(self.store.jobs()[0]['status'], 'succeeded')
        self.assertEqual(self.store.attempts()[0]['report']['validation']['status'], 'pass')
        with (self.root/'board.lock').open('a') as unlocked:
            fcntl.flock(unlocked, fcntl.LOCK_EX | fcntl.LOCK_NB)

    def test_unavailable_rtl_keeps_reservation(self):
        self.store.register_experiment(experiment([rtl_job(kind='board_test')]))
        request = self.controller.request(dict(job='rtl', node='a', gpus=[]))
        with self.store.db:
            self.store.db.execute('INSERT INTO attempts(id,job,node,spec,status,created) VALUES(?,?,?,?,?,?)',
                                 ('lost', 'rtl', 'a', dumps(request), 'unknown', time.time()))
            self.store.db.execute("UPDATE jobs SET status='unknown'")
            self.store.db.execute('INSERT INTO node_health VALUES(?,?)', ('a', dumps(dict(phase='unavailable', reason='SSH down'))))
        self.controller.invalidate_unavailable()
        self.assertEqual(self.store.attempts()[0]['status'], 'unknown')
        self.assertFalse(self.store.attempts()[0]['released'])


if __name__ == '__main__': unittest.main()
