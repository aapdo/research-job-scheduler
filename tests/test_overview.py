import json
import io
from contextlib import redirect_stdout
from pathlib import Path
import sqlite3
import tempfile
import time
import unittest
from unittest.mock import patch

from test_scheduler import node, job, experiment, snapshot, reservation
from research_scheduler.store import Store, dumps
from research_scheduler.notifications import register_campaign
from research_scheduler.overview import collect, markdown, ReadStore, attach_progress


class OverviewTests(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.db = Path(self.tmp.name)/'state.db'
        s = Store(self.db)
        n = node(); s.register_node(n)
        s.register_experiment(experiment([job('train'),job('pending')]))
        register_campaign(s,dict(id='main',name='Main',rq='Test',experiments=['e']))
        a = reservation(n,key='attempt-1');a['job']='train';a['spec']['attempt_dir']=str(Path(self.tmp.name)/'attempt')
        a['report']={'heartbeat':time.time()}
        with s.db:
            s.db.execute("UPDATE jobs SET status='running' WHERE id='train'")
            s.db.execute('INSERT INTO attempts(id,job,node,spec,status,created,report) VALUES(?,?,?,?,?,?,?)',
                (a['id'],a['job'],a['node'],dumps(a['spec']),a['status'],a['created'],dumps(a['report'])))
            s.db.execute('INSERT INTO snapshots VALUES(?,?)',(n['id'],dumps(snapshot(n))))
        s.db.close()

    def tearDown(self): self.tmp.cleanup()

    def test_one_decoded_copy_per_read_snapshot(self):
        reader=ReadStore(self.db)
        try:
            queries=[];reader.db.set_trace_callback(queries.append)
            attempts=reader.attempts()
            self.assertIs(reader.attempts()[0],attempts[0])
            self.assertIs(reader.attempts(active=True)[0],attempts[0])
            self.assertIs(reader.specs('nodes'),reader.specs('nodes'))
            self.assertEqual(sum(q=='SELECT * FROM attempts' for q in queries),1)
            before=json.dumps(reader.jobs(),sort_keys=True)
            from research_scheduler.controller import Controller
            Controller(reader).plan()
            self.assertEqual(json.dumps(reader.jobs(),sort_keys=True),before)
            self.assertEqual(sum(q=='SELECT * FROM attempts' for q in queries),1)
        finally:
            reader.db.close()

    def test_summary_omits_duplicate_campaign_but_full_audit_still_available(self):
        with sqlite3.connect(self.db) as db:
            raw=json.loads(db.execute('SELECT spec FROM attempts').fetchone()[0])
            raw['experiment_spec']={'large_frozen_campaign':'x'*100000}
            db.execute('UPDATE attempts SET spec=?',(json.dumps(raw),))
        reader=ReadStore(self.db)
        try:
            queries=[];reader.db.set_trace_callback(queries.append)
            summary=reader.attempts(summary=True)
            self.assertNotIn('experiment_spec',summary[0]['spec'])
            self.assertIs(reader.attempts(summary=True)[0],summary[0])
            self.assertIs(reader.attempts(planning=True)[0],summary[0])
            self.assertEqual(summary[0]['spec']['attempt_dir'],raw['attempt_dir'])
            self.assertFalse(any(q=='SELECT * FROM attempts' for q in queries))
            self.assertEqual(reader.attempts()[0]['spec']['experiment_spec'],raw['experiment_spec'])
        finally:reader.db.close()

    def test_read_only_planner_loads_verified_dependency_locations(self):
        receipt={'root':'/b/relay','files':{'RESULT.json':{'path':'/b/relay/RESULT.json',
                 'sha256':'a'*64,'bytes':1}}}
        with sqlite3.connect(self.db) as db:
            spec=json.loads(db.execute("SELECT spec FROM attempts WHERE id='attempt-1'").fetchone()[0])
            report={'outputs':{'RESULT.json':{'path':'/a/RESULT.json','sha256':'a'*64,'bytes':1}},
                    'dependency_artifacts':{'RESULT.json':{'path':'/a/RESULT.json','sha256':'a'*64,'bytes':1}}}
            db.execute("UPDATE attempts SET status='succeeded',spec=?,report=? WHERE id='attempt-1'",
                       (json.dumps(spec),json.dumps(report)))
            db.execute("UPDATE jobs SET status='succeeded' WHERE id='train'")
            db.execute('INSERT INTO artifact_transfers VALUES(?,?,?,?,?,?,?,?)',
                       ('relay','attempt-1','b','download','succeeded','{}',
                        json.dumps({'artifact':receipt}),time.time()))
        reader=ReadStore(self.db)
        try:
            attempt=reader.attempts(planning=True,job_ids={'train'})[0]
            self.assertEqual(attempt['artifact_locations']['b']['root'],'/b/relay')
        finally:reader.db.close()

    def test_assignment_campaign_and_read_only(self):
        c=sqlite3.connect(self.db);before=list(c.execute('SELECT * FROM events'));c.close()
        with patch('subprocess.run', side_effect=AssertionError('default must not use SSH')):
            data=collect(self.db)
        self.assertTrue(data['read_only'])
        self.assertEqual(data['campaigns'][0]['counts']['running'],1)
        used=[g for g in data['gpus'] if g['jobs']]
        self.assertEqual(len(used),1)
        self.assertEqual(used[0]['jobs'][0]['campaigns'],['main'])
        c=sqlite3.connect(self.db);self.assertEqual(before,list(c.execute('SELECT * FROM events')));c.close()
        reader=ReadStore(self.db)
        with self.assertRaises(sqlite3.OperationalError):reader.db.execute('DELETE FROM jobs')
        reader.db.close()

    def test_job_filter_keeps_waiting_reason_and_requests(self):
        data=collect(self.db,job='pending')
        self.assertEqual([r['id'] for r in data['jobs']],['pending'])
        self.assertIsNotNone(data['jobs'][0]['plan'])
        self.assertIn('Resources:',markdown(data))

    def test_dependency_wait_is_derived_without_changing_lifecycle(self):
        s=Store(self.db)
        spec=next(j for j in s.jobs() if j['id']=='pending')['spec'];spec['depends_on']=['train']
        with s.db:s.db.execute("UPDATE jobs SET spec=? WHERE id='pending'",(dumps(spec),))
        s.db.close()
        d=collect(self.db,job='pending');r=d['jobs'][0]
        self.assertEqual(r['status'],'queued')
        self.assertEqual(r['display_status'],'dependency_wait')
        self.assertEqual(r['waiting']['dependencies'],[{'job':'train','status':'running'}])
        self.assertEqual(d['campaigns'][0]['counts']['dependency_wait'],1)

    def test_missing_db_is_not_created(self):
        missing=Path(self.tmp.name)/'missing.db'
        with self.assertRaises(sqlite3.OperationalError):collect(missing)
        self.assertFalse(missing.exists())

    def test_stale_and_disabled_are_visible(self):
        c=sqlite3.connect(self.db)
        n=json.loads(c.execute('SELECT spec FROM nodes').fetchone()[0]);n['enabled']=False
        v=json.loads(c.execute('SELECT data FROM snapshots').fetchone()[0]);v['received_at']=1;v['time']=1
        c.execute('UPDATE nodes SET spec=?',(dumps(n),));c.execute('UPDATE snapshots SET data=?',(dumps(v),));c.commit();c.close()
        data=collect(self.db)
        self.assertFalse(data['gpus'][0]['enabled']);self.assertTrue(data['gpus'][0]['stale'])

    def test_hardware_failure_warns_not_silently_empty(self):
        data=collect(self.db,hardware_index=Path(self.tmp.name)/'missing.json')
        self.assertTrue(any('Hardware registry unavailable' in w for w in data['warnings']))

    def test_live_progress_failure_is_explicit(self):
        rows=[dict(id='j',node='a',status='running',attempt_dir='/tmp/attempt')]
        with patch('subprocess.run',side_effect=TimeoutError('unreachable')):
            attach_progress(rows,{'a':node()},1)
        self.assertIn('error',rows[0]['progress'])

    def read_progress_fixture(self, files):
        root=Path(self.tmp.name)/'progress-fixture';root.mkdir(exist_ok=True)
        for name,value in files.items():
            p=root/name;p.parent.mkdir(parents=True,exist_ok=True)
            p.write_text(value if isinstance(value,str) else json.dumps(value))
        rows=[dict(id='train',node='a',status='running',attempt_dir=str(root))]
        attach_progress(rows,{'a':node()},3)
        return rows[0]['progress']

    def test_root_bootstrap_progress_uses_applied_not_attempted_steps(self):
        p=self.read_progress_fixture({'TRAIN_PROGRESS.json':dict(epoch=3,planned_epochs=20,
            applied=0,training_iterations_executed=15,skipped=15,member=0,members=3)})
        self.assertEqual((p['epoch'],p['optimizer_steps_executed'],p['member'],p['members']),(3,0,0,3))
        self.assertLess(p['age_s'],3)

    def test_existing_nested_progress_and_canonical_counter_take_precedence(self):
        p=self.read_progress_fixture({'run/TRAIN_PROGRESS.json':dict(epoch=5,optimizer_steps_executed=42,applied=8),
                                      'TRAIN_PROGRESS.json':dict(epoch=1,applied=3)})
        self.assertEqual((p['epoch'],p['optimizer_steps_executed']),(5,42))

    def test_iteration_counter_is_not_claimed_as_optimizer_updates(self):
        p=self.read_progress_fixture({'TRAIN_PROGRESS.json':dict(epoch=1,training_iterations_executed=100)})
        self.assertIsNone(p['optimizer_steps_executed'])

    def test_worker_supplied_steps_per_epoch_is_preserved(self):
        p=self.read_progress_fixture({'TRAIN_PROGRESS.json':dict(
            epoch=1, planned_epochs=5, optimizer_steps_executed=21,
            step_in_epoch=21, steps_per_epoch=1789)})
        self.assertEqual(p['steps_per_epoch'],1789)

    def test_constant_epoch_length_is_recovered_from_consistent_counters(self):
        p=self.read_progress_fixture({'TRAIN_PROGRESS.json':dict(
            epoch=3, planned_epochs=5, optimizer_steps_executed=3778,
            step_in_epoch=200)})
        self.assertEqual(p['steps_per_epoch'],1789)

    def test_inconsistent_epoch_counters_do_not_invent_a_denominator(self):
        p=self.read_progress_fixture({'TRAIN_PROGRESS.json':dict(
            epoch=3, planned_epochs=5, optimizer_steps_executed=3779,
            step_in_epoch=200)})
        self.assertIsNone(p['steps_per_epoch'])

    def test_root_eval_progress_exposes_phase_and_batches(self):
        p=self.read_progress_fixture({'PROGRESS.json':dict(phase='w8a8',batches=2050,config='percentile')})
        self.assertEqual((p['phase'],p['batches'],p['config']),('w8a8',2050,'percentile'))

    def test_root_eval_progress_exposes_image_cursor(self):
        p=self.read_progress_fixture({'PROGRESS.json':dict(
            phase='head_cache',completed_images=5064,planned_images=195244)})
        self.assertEqual((p['completed_images'],p['planned_images']),(5064,195244))

    def test_eval_progress_contract_supplies_stable_denominator(self):
        p=self.read_progress_fixture({
            'PROGRESS.json':dict(phase='w8a8',batches=2050,config='percentile'),
            'PROGRESS_CONTRACT.json':dict(planned_batches=9913)})
        self.assertEqual(p['planned_batches'],9913)

    def test_legacy_paddle_eval_log_exposes_phase_and_batches(self):
        p=self.read_progress_fixture({
            'stdout.log':'[09/18] ppdet.engine.callbacks INFO: Eval iter: 3300\n',
            'evaluation/fp32/bbox.json':'[]',
            'evaluation/quantized/placeholder':'running'})
        self.assertEqual((p['completed_cells'],p['planned_cells'],p['phase'],p['batches']),
                         (1,2,'quantized',3300))

    def test_invalid_progress_is_explicit(self):
        self.assertIn('error',self.read_progress_fixture({'TRAIN_PROGRESS.json':'{broken'}))

    def test_progress_size_limit_is_preserved(self):
        self.assertIn('error',self.read_progress_fixture({'TRAIN_PROGRESS.json':' '*131073}))

    def test_cli_overview_does_not_construct_writable_store(self):
        from research_scheduler.cli import main
        with patch('research_scheduler.cli.Store', side_effect=AssertionError('write-capable Store used')):
            with redirect_stdout(io.StringIO()) as out:
                main(['--db',str(self.db),'overview','--job','pending','--format','json'])
        self.assertTrue(json.loads(out.getvalue())['read_only'])

    def test_superseded_failure_is_not_a_second_project_campaign(self):
        s=Store(self.db)
        with s.db:
            old=next(j for j in s.jobs() if j['id']=='train')['spec']
            new=next(j for j in s.jobs() if j['id']=='pending')['spec']
            old['config']['plan_sha256']=new['config']['plan_sha256']='plan'
            old['kind']=new['kind']='train'
            old['metadata']={'recovery_replacement':{'job':'pending','evidence':'approved'}}
            new['metadata']={'independent_restart':{'original_job':'train'}}
            s.db.execute("UPDATE jobs SET status='failed',spec=? WHERE id='train'",(dumps(old),))
            s.db.execute("UPDATE jobs SET status='running',spec=? WHERE id='pending'",(dumps(new),))
            s.db.execute("UPDATE attempts SET status='failed'")
        s.db.close()
        result=collect(self.db)
        self.assertEqual(len(result['campaigns']),1)
        self.assertEqual(result['campaigns'][0]['counts'],{'running':1})

    def test_retry_does_not_display_failed_host_as_current_assignment(self):
        c=sqlite3.connect(self.db)
        c.execute("UPDATE jobs SET status='queued' WHERE id='train'")
        c.execute("UPDATE attempts SET status='failed'");c.commit();c.close()
        d=collect(self.db,job='train');row=d['jobs'][0]
        self.assertNotIn('node',row)
        self.assertEqual(row['last_attempt']['status'],'failed')
        self.assertEqual(d['allocation']['assigned_gpus'],0)

    def test_profile_flip_flop_is_reported(self):
        s=Store(self.db)
        with s.db:
            for value in [8,9,8,9]:s.event('node_storage_profile_changed','a',{'max_jobs':value})
        s.db.close()
        d=collect(self.db)
        self.assertEqual(d['diagnostics'][0]['kind'],'repeating_storage_profile')
        self.assertTrue(any('stable polls' in w for w in d['warnings']))

    def test_old_flip_flop_is_not_a_current_warning_after_stable_probes(self):
        s=Store(self.db)
        with s.db:
            for value in [8,9,8,9]:s.event('node_storage_profile_changed','a',{'max_jobs':value})
            value=json.loads(s.db.execute("SELECT data FROM snapshots WHERE node='a'").fetchone()[0])
            value.update(stable_polls=3,received_at=time.time())
            s.db.execute("UPDATE snapshots SET data=? WHERE node='a'",(dumps(value),))
        s.db.close()
        d=collect(self.db)
        self.assertFalse(d['diagnostics'][0]['active'])
        self.assertFalse(any('stable polls' in w for w in d['warnings']))
