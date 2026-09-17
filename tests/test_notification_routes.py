import json
import tempfile
import time
import unittest
from pathlib import Path
from unittest.mock import patch

from test_notifications import experiment,campaign
from research_scheduler.notifications import poll_campaigns,register_campaign,deliver_outbox
from research_scheduler.progress_notifications import initialize,enqueue,messages
from research_scheduler.store import Store


class RoutedNotificationTests(unittest.TestCase):
    def setUp(self):
        self.tmp=tempfile.TemporaryDirectory();self.root=Path(self.tmp.name)
        self.home=patch('pathlib.Path.home',return_value=self.root);self.home.start()
        self.s=Store(self.root/'state.db');self.sent=[]
        events={}
        for kind in ('started','error','complete','progress'):
            f=self.root/kind;f.write_text('https://hooks.slack.com/services/TEST/'+kind+'/SECRET');f.chmod(0o600);events[kind]=str(f)
        events['recovered']=events['error']
        route=self.root/'routes.json';route.write_text(json.dumps(dict(version=1,events=events)));route.chmod(0o600)
        self.env=patch.dict('os.environ',{'RS_SLACK_ROUTES_FILE':str(route)});self.env.start()

    def tearDown(self):
        self.env.stop();self.home.stop();self.s.db.close();self.tmp.cleanup()

    def send(self,url,payload):self.sent.append((url,payload['text']))

    def test_registration_does_not_start_but_actual_running_starts_once(self):
        self.s.register_experiment(experiment());register_campaign(self.s,campaign())
        poll_campaigns(self.s,sender=self.send);self.assertEqual(self.sent,[])
        with self.s.db:self.s.db.execute("UPDATE jobs SET status='running'")
        poll_campaigns(self.s,sender=self.send);poll_campaigns(self.s,sender=self.send)
        self.assertEqual(len(self.sent),1);self.assertIn('/started/',self.sent[0][0]);self.assertIn('캠페인 시작',self.sent[0][1])
        with self.s.db:self.s.db.execute("UPDATE jobs SET status='failed',reason='test failure'")
        poll_campaigns(self.s,sender=self.send);self.assertIn('/error/',self.sent[-1][0])
        with self.s.db:self.s.db.execute("UPDATE jobs SET status='succeeded'")
        poll_campaigns(self.s,sender=self.send);self.assertIn('/complete/',self.sent[-1][0])
        raw=self.s.db.execute('SELECT payload FROM notification_outbox').fetchall()
        self.assertNotIn('hooks.slack.com',str(raw));self.assertNotIn('SECRET',str(raw))

    def test_existing_running_campaign_is_not_reannounced(self):
        register_campaign(self.s,campaign('existing',external=True))
        with self.s.db:self.s.db.execute('INSERT INTO campaign_runtime VALUES(?,?,?,?,?)',
            ('existing','running',1,json.dumps(dict(counts={'running':1})),time.time()))
        poll_campaigns(self.s,external_observations={'existing':dict(state='running',counts={'running':1})},sender=self.send)
        self.assertEqual(self.sent,[])

    def test_shared_successful_validation_does_not_consume_start_alert(self):
        register_campaign(self.s,campaign('hardware',external=True))
        poll_campaigns(self.s,external_observations={'hardware':dict(state='running',counts={'succeeded':1,'queued':2})},sender=self.send)
        self.assertEqual(self.sent,[])
        poll_campaigns(self.s,external_observations={'hardware':dict(state='running',counts={'succeeded':1,'running':1,'queued':1})},sender=self.send)
        self.assertEqual(len(self.sent),1)
        self.assertIn('/started/',self.sent[0][0])

    def test_new_error_within_error_state_alerts_without_repeating_same_error(self):
        register_campaign(self.s,campaign('external',external=True))
        def poll(job):return poll_campaigns(self.s,external_observations={'external':dict(state='error',
            counts={'failed':1},errors=[dict(job=job,status='failed',reason='test')])},sender=self.send)
        poll('a');poll('a');poll('b');poll('b');self.assertEqual(len(self.sent),2)

    def test_progress_runs_every_600_seconds_and_restart_deduplicates(self):
        initialize(self.s);p=[dict(text='unchanged progress')]
        self.assertTrue(enqueue(self.s,p,1000))
        self.assertFalse(enqueue(self.s,p,1599))
        self.assertTrue(enqueue(self.s,p,1600))
        self.assertFalse(enqueue(self.s,p,1600))
        self.assertEqual(self.s.db.execute("SELECT COUNT(*) FROM notification_outbox WHERE status='pending'").fetchone()[0],1)

    def test_completion_does_not_repeat_after_state_fluctuation(self):
        register_campaign(self.s,campaign('external',external=True))
        for state in ('complete','running','complete','complete'):
            poll_campaigns(self.s,external_observations={'external':dict(state=state,counts={state:1})},sender=self.send)
        self.assertEqual(len(self.sent),1)
        self.assertIn('/complete/',self.sent[0][0])

    def test_success_ack_survives_scientific_lock_contention(self):
        initialize(self.s);enqueue(self.s,[dict(text='progress')],time.time())
        held=[]
        def sender(url,payload):
            lock=self.s.lock();lock.__enter__();held.append(lock)
            # A concurrent sender is excluded even if its lease would be due.
            self.assertTrue(deliver_outbox(self.s,sender=self.send)['busy'])
        try:self.assertEqual(deliver_outbox(self.s,sender=sender)['sent'],1)
        finally:
            for lock in held:lock.__exit__(None,None,None)
        self.assertEqual(deliver_outbox(self.s,sender=self.send)['sent'],0)
        self.assertEqual(self.s.db.execute('SELECT status FROM notification_outbox').fetchone()[0],'sent')

    def test_recovery_uses_error_route_and_retries_durably(self):
        register_campaign(self.s,campaign('external',external=True))
        def failed_sender(url,payload):raise RuntimeError('offline')
        poll_campaigns(self.s,external_observations={'external':dict(state='error',counts={'failed':1})},sender=failed_sender)
        poll_campaigns(self.s,external_observations={'external':dict(state='running',counts={'running':1},recovery_ready=True)},sender=failed_sender)
        self.assertEqual(self.sent,[])
        with self.s.db:self.s.db.execute("UPDATE notification_outbox SET next_attempt=0 WHERE state='recovered'")
        self.assertEqual(deliver_outbox(self.s,sender=self.send)['sent'],1)
        self.assertIn('/error/',self.sent[0][0])
        self.assertIn('복구',self.sent[0][1])
        self.assertEqual(deliver_outbox(self.s,sender=self.send)['sent'],0)

    def test_formatter_includes_every_gpu_and_hardware_allocation(self):
        data=dict(time_kst='now',allocation=dict(assigned_gpus=1,total_gpus=2,active_jobs=1),
            gpus=[dict(node='farm8-gui2',index=i,jobs=[dict(job='experiment',status='running')] if i==0 else [],enabled=True) for i in range(2)],
            campaigns=[dict(id='campaign',counts={'running':1,'dependency_wait':2})],hardware=[],warnings=[])
        output=''.join(p['text'] for p in messages(data,[dict(node='CPS2',job='build-job',status='running')]))
        for needle in ('GPU0','GPU1','미배정','train-dep-wait','CPS2','build-job'):self.assertIn(needle,output)
        payloads=messages(data,[])
        self.assertTrue(payloads[0]['text'].startswith('========\n'))
        self.assertTrue(payloads[0]['blocks'][0]['text']['text'].startswith('========\n'))
        self.assertTrue(all(not p['text'].startswith('========') for p in payloads[1:]))
        self.assertEqual(len(payloads),4)
        for payload in payloads:
            table=payload['blocks'][1]
            self.assertEqual(table['type'],'table')
            self.assertLessEqual(len(table['rows']),100)
            self.assertLessEqual(sum(len(c['text']) for row in table['rows'] for c in row),9000)

    def test_table_pagination_and_secret_redaction(self):
        data=dict(time_kst='now',allocation=dict(assigned_gpus=0,total_gpus=205,active_jobs=0),
            gpus=[dict(node='lab',index=i,jobs=[],enabled=True,stale=True) for i in range(205)],
            campaigns=[],hardware=[],warnings=['https://hooks.slack.com/services/secret'])
        payloads=messages(data,limit=1000)
        gpu_rows=[]
        for payload in payloads:
            if 'blocks' not in payload:continue
            table=payload['blocks'][1]
            self.assertLessEqual(len(table['rows']),100)
            self.assertLessEqual(sum(len(c['text']) for row in table['rows'] for c in row),1000)
            if table['rows'][0][0]['text']=='서버':gpu_rows.extend(table['rows'][1:])
        self.assertEqual(len(gpu_rows),205)
        self.assertNotIn('https://hooks.slack.com',json.dumps(payloads))

    def test_hardware_latest15_no_rtl_or_gpu_boilerplate(self):
        data=dict(time_kst='now',allocation=dict(assigned_gpus=49,total_gpus=68,active_jobs=22),
                  gpus=[],campaigns=[],warnings=[],hardware=[dict(id=f'h{i:02}',registered_at=i+1,
                  phase='build_running',build_status='running',validation={'secret-rtl':'succeeded'},
                  board={'status':'queued'}) for i in range(20)])
        payloads=messages(data,[])
        hardware=[p for p in payloads if '하드웨어' in p['blocks'][0]['text']['text']]
        self.assertEqual(len(hardware),2)
        for p in hardware:
            self.assertNotIn('GPU 배정',p['text'])
            self.assertNotIn('스케줄러 예약 기준',p['text'])
            self.assertNotIn('test: RTL',p['text'])
        rows=hardware[-1]['blocks'][1]['rows']
        self.assertEqual(len(rows),16)
        self.assertTrue(rows[1][0]['text'].startswith('h19-'))
        self.assertTrue(rows[-1][0]['text'].startswith('h05-'))
        self.assertTrue(all(len(row)==4 for row in rows))
        self.assertEqual(rows[1][1]['text'],'building')
        self.assertEqual(rows[1][2]['text'],'building')
        self.assertNotIn('보조 작업 · 외부 집계 · 추가 상태',json.dumps(payloads,ensure_ascii=False))

    def test_explicit_campaigns_hidden_only_from_periodic_messages(self):
        hidden=['cssa-main','cssa-anchor','cssa-context','cssa-followup','picodet-s-cssa60','cssa-recovery']
        data=dict(time_kst='now',allocation=dict(assigned_gpus=0,total_gpus=0,active_jobs=0),
                  gpus=[],hardware=[],warnings=[],campaigns=[dict(id=k,counts={'running':1},
                  counts_by_type={'train':{'running':1}}) for k in hidden+['keep-visible']])
        raw=json.dumps(messages(data),ensure_ascii=False)
        for key in hidden:self.assertNotIn(key,raw)
        self.assertIn('keep-visible',raw)
        self.assertEqual(len(data['campaigns']),7)

    def test_disabled_gpu_rows_hidden_and_denominator_matches(self):
        data=dict(time_kst='now',allocation=dict(assigned_gpus=0,total_gpus=2,active_jobs=0),
                  campaigns=[],hardware=[],warnings=[],gpus=[
                  dict(node='allowed',index=0,jobs=[],enabled=True),
                  dict(node='forbidden',index=0,jobs=[],enabled=False)])
        payload=messages(data)[0]
        self.assertIn('GPU 배정 0/1개',payload['text'])
        self.assertNotIn('forbidden',json.dumps(payload))


if __name__=='__main__':unittest.main()
