import sys,types,unittest,io,json,tempfile,contextlib,time
from pathlib import Path
from unittest.mock import Mock,patch
sys.path.insert(0,str(Path(__file__).resolve().parents[1]/'src'))
from research_scheduler import hf_worker
from research_scheduler import agent
from research_scheduler.artifacts import hf_spec,upload_pause,retry_possible

class PacingTests(unittest.TestCase):
    def test_scheduler_interval_overrides_campaign_and_keeps_safety(self):
        hf=hf_spec(dict(repo_id='a/b',commit_interval_s=120))
        campaign={'hf':hf}
        t=dict(direction='upload',created=100,status='succeeded',report={'finished':100},spec={'config':{'hf':hf}})
        with patch.dict('os.environ',{'RS_HF_UPLOAD_INTERVAL_S':'20'}):
            self.assertTrue(upload_pause(campaign,[t],{'c':campaign},now=119))
            self.assertFalse(upload_pause(campaign,[t],{'c':campaign},now=120))
            t['status']='running'
            self.assertTrue(upload_pause(campaign,[t],{},now=200))
            t.update(status='failed',report={'rate_limit':{'retry_at':400}})
            self.assertTrue(upload_pause(campaign,[t],{},now=200))
        with patch.dict('os.environ',{'RS_HF_UPLOAD_INTERVAL_S':'-1'}):
            with self.assertRaises(ValueError):upload_pause(campaign,[],{})
    def test_429_worker_exits_and_agent_reports_cooldown(self):
        with tempfile.TemporaryDirectory() as tmp:
            root=Path(tmp);cfg=root/'config.json';cfg.write_text('{"direction":"upload"}')
            env={'RS_CONFIG_PATH':str(cfg),'RS_ATTEMPT_DIR':tmp,'RS_ATTEMPT_ID':'hf-test'}
            with patch.dict('os.environ',env),patch.object(hf_worker,'upload',side_effect=hf_worker.RateLimitDeferred(120)),contextlib.redirect_stdout(io.StringIO()):
                with self.assertRaises(SystemExit) as caught:hf_worker.main()
            self.assertEqual(caught.exception.code,75)
            retry=json.loads((root/'HF_RETRY.json').read_text());self.assertEqual(retry['retry_at']-retry['created'],120)
            payload={'action':'artifact_status','request':{'id':'hf-test','attempt_dir':tmp}}
            output=io.StringIO()
            with patch.object(sys,'argv',['agent']),patch.object(sys,'stdin',io.StringIO(json.dumps(payload))),patch.object(agent,'read_status',return_value={'status':'failed'}),contextlib.redirect_stdout(output):
                agent.main()
            self.assertEqual(json.loads(output.getvalue())['rate_limit'],retry)
    def error(self,headers=None,text='rate limited',status=429):
        e=RuntimeError(text);e.response=types.SimpleNamespace(status_code=status,headers=headers or {});return e
    def test_headers_hourly_limit_and_fallback(self):
        self.assertEqual(hf_worker.rate_limit_delay(self.error({'Retry-After':'123'})),123)
        self.assertEqual(hf_worker.rate_limit_delay(self.error({'RateLimit':'"api";r=0;t=240'})),240)
        self.assertEqual(hf_worker.rate_limit_delay(self.error({'Retry-After':'60'},'repository commits per hour')),3600)
        self.assertEqual(hf_worker.rate_limit_delay(self.error()),3600)
        with self.assertRaises(ValueError):hf_worker.rate_limit_delay(self.error({'Retry-After':'999999'}))
    def test_bounded_retry_only_on_429(self):
        api=Mock();api.create_commit.side_effect=self.error({'Retry-After':'120'})
        with patch.object(hf_worker.time,'sleep') as sleep:
            with self.assertRaises(hf_worker.RateLimitDeferred) as caught:hf_worker.commit_with_backoff(api,{})
            self.assertEqual(caught.exception.delay,120);sleep.assert_not_called()
        api=Mock();api.create_commit.side_effect=self.error(status=400)
        with patch.object(hf_worker.time,'sleep') as sleep:
            with self.assertRaises(RuntimeError):hf_worker.commit_with_backoff(api,{})
            sleep.assert_not_called()
    def test_cooldown_and_retry_budget_keep_failure_history(self):
        def row(rate=False,revision=None):
            return dict(status='failed',created=100,spec={'config':{'repair_revision':revision}},
                        report={'rate_limit':{'retry_at':500}} if rate else {})
        self.assertFalse(retry_possible([row(True)],now=499))
        self.assertTrue(retry_possible([row(True)],now=501))
        self.assertFalse(retry_possible([row(True)]*4,now=501))
        self.assertFalse(retry_possible([row()]*3,now=501))
        history=[row()]*3+[row(True,'approved-repair')]
        self.assertTrue(retry_possible(history,now=501))
        self.assertEqual(len(history),4)
    def test_repo_pacing_applies_across_branches_not_downloads(self):
        hf=hf_spec(dict(repo_id='a/b',archive_payload=True,commit_interval_s=120))
        campaign={'hf':hf};campaigns={'c':campaign}
        t=dict(direction='upload',created=100,status='running',report={},spec={'config':{'hf':dict(hf,revision='another')}})
        self.assertTrue(upload_pause(campaign,[t],campaigns,now=500))
        t.update(status='succeeded',report={'finished':400})
        self.assertTrue(upload_pause(campaign,[t],campaigns,now=500))
        self.assertFalse(upload_pause(campaign,[t],campaigns,now=521))
        t['direction']='download';self.assertFalse(upload_pause(campaign,[t],campaigns,now=500))
        with self.assertRaises(ValueError):hf_spec(dict(repo_id='a/b',archive_payload='yes'))
