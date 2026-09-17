"""Campaign notifications use a durable outbox and never persist webhook secrets."""
import sys
import tempfile
import unittest
from unittest.mock import patch
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "src"))

from research_scheduler.notifications import (poll_campaigns, register_campaign, status, webhook_path,
                                                campaign_processing_due, _record_observation)
from research_scheduler.store import Store


def experiment(key="study", project="weather"):
    return {
        "id": key,
        "project": project,
        "name": "Weather study",
        "rq": "Does context improve detection?",
        "jobs": [{
            "id": key + "-train",
            "name": "train",
            "kind": "train",
            "argv": ["true"],
            "cwd": "/tmp",
            "resources": {"gpu_count": 0, "cpu": 1, "ram_mib": 512},
        }],
    }


def campaign(key="context-campaign", external=False):
    return {
        "id": key,
        "name": "Context campaign",
        "rq": "Which context mechanism works?",
        "projects": [] if external else ["weather"],
        "experiments": [],
        "external": external,
        "enabled": True,
    }


class CampaignNotificationTests(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.root = Path(self.temp.name)
        home=patch('pathlib.Path.home',return_value=self.root)
        home.start();self.addCleanup(home.stop)
        self.store = Store(self.root / "state.db")
        self.secret = self.root / "slack-webhook"
        self.secret.write_text("https://hooks.slack.com/services/test/secret/value\n")
        self.secret.chmod(0o600)
        self.sent = []

    def tearDown(self):
        self.store.db.close()
        self.temp.cleanup()

    def sender(self, url, payload):
        self.sent.append((url, payload))

    def test_terminal_campaigns_leave_hot_poll_after_policy_window(self):
        self.store.register_experiment(experiment())
        spec=campaign();register_campaign(self.store,spec)
        with self.store.db:
            _record_observation(self.store,spec,dict(state='error',counts={'failed':1},
                errors=[dict(job='study-train',status='failed',reason='x')],failed_jobs=['study-train']),100)
        self.assertTrue(campaign_processing_due(self.store,spec,100+86400-1))
        self.assertFalse(campaign_processing_due(self.store,spec,100+86400))
        with self.store.db:
            _record_observation(self.store,spec,dict(state='complete',counts={'succeeded':1},
                errors=[],failed_jobs=[]),200)
        self.assertFalse(campaign_processing_due(self.store,spec,201))
        with self.store.db:
            self.store.db.execute("UPDATE campaign_runtime SET state='cancelled' WHERE id=?", (spec['id'],))
        self.assertFalse(campaign_processing_due(self.store,spec,202))
        self.assertFalse(campaign_processing_due(self.store,dict(spec,enabled=False),201))

    def test_operator_disable_blocks_all_routes_and_preserves_local_events(self):
        from research_scheduler.notifications import route_webhook_path, _send
        marker=self.root/'.config/research-scheduler/slack-disabled'
        marker.parent.mkdir(parents=True);marker.write_text('disabled')
        self.store.register_experiment(experiment())
        register_campaign(self.store,campaign())
        with self.store.db:self.store.db.execute("UPDATE jobs SET status='failed'")
        with patch('urllib.request.urlopen',side_effect=AssertionError('must not access Slack')):
            result=poll_campaigns(self.store,webhook_file=self.secret,sender=self.sender)
            for event in ['started','error','recovered','complete','progress']:
                self.assertEqual(route_webhook_path(event, self.secret),'')
            with self.assertRaisesRegex(RuntimeError,'disabled'):
                _send('https://hooks.slack.com/services/TEST/TEST/TEST',{})
        self.assertFalse(result['enabled'])
        self.assertEqual(result['sent'],0)
        self.assertEqual(self.sent,[])
        saved=status(self.store)
        self.assertEqual(saved['runtime']['context-campaign']['state'],'error')
        self.assertEqual(saved['outbox'][0]['status'],'pending')
        self.assertFalse(saved['webhook_configured'])

    def test_poll_reads_shared_inputs_once_and_refreshes_next_poll(self):
        from research_scheduler import artifacts
        self.store.register_experiment(experiment())
        for i in range(11):
            spec = campaign('campaign-' + str(i))
            spec['hf'] = dict(repo_id='test/model')
            register_campaign(self.store, spec)
        with patch.object(self.store, 'jobs', wraps=self.store.jobs) as jobs, \
                patch.object(self.store, 'observation_attempts', wraps=self.store.observation_attempts) as attempts, \
                patch.object(self.store, 'specs', wraps=self.store.specs) as specs, \
                patch.object(artifacts, 'rows', wraps=artifacts.rows) as transfers:
            poll_campaigns(self.store, webhook_file='', sender=self.sender)
            self.assertEqual(jobs.call_count, 1)
            self.assertEqual(attempts.call_count, 1)
            self.assertEqual(transfers.call_count, 1)
            self.assertEqual([c.args[0] for c in specs.call_args_list], ['experiments', 'nodes'])
            with self.store.db:
                self.store.db.execute("UPDATE jobs SET status='failed' WHERE id='study-train'")
            poll_campaigns(self.store, webhook_file='', sender=self.sender)
            self.assertEqual(jobs.call_count, 2)
            self.assertEqual(attempts.call_count, 2)
            self.assertEqual(transfers.call_count, 2)
        self.assertTrue(all(r['state'] == 'error' for r in status(self.store)['runtime'].values()))

    def test_default_secret_survives_missing_environment_on_restart(self):
        secret_dir = self.root / '.config/research-scheduler'
        secret_dir.mkdir(parents=True)
        target = secret_dir / 'slack-webhook'
        target.write_text(self.secret.read_text())
        target.chmod(0o600)
        with patch.dict('os.environ', {}, clear=True), patch('pathlib.Path.home', return_value=self.root):
            self.assertEqual(webhook_path(), str(target))
            self.assertEqual(webhook_path(''), '')
            register_campaign(self.store, campaign('external', external=True))
            result = poll_campaigns(self.store, external_observations={
                'external': {'state':'complete','counts':{'complete':1}}}, sender=self.sender)
            self.assertEqual(result['sent'], 1)
            self.assertTrue(status(self.store)['webhook_configured'])
            with patch.dict('os.environ', {'RS_SLACK_WEBHOOK_FILE':''}):
                self.assertEqual(webhook_path(), '')

    def test_project_campaign_alerts_once_per_terminal_transition(self):
        self.store.register_experiment(experiment())
        register_campaign(self.store, campaign())

        first = poll_campaigns(self.store, webhook_file=self.secret, sender=self.sender)
        self.assertEqual((first["sent"], status(self.store)["runtime"]["context-campaign"]["state"]),
                         (0, "running"))

        with self.store.db:
            self.store.db.execute(
                "UPDATE jobs SET status='failed',reason='numeric instability' WHERE id='study-train'")
        failed = poll_campaigns(self.store, webhook_file=self.secret, sender=self.sender)
        repeated = poll_campaigns(self.store, webhook_file=self.secret, sender=self.sender)
        self.assertEqual((failed["sent"], repeated["sent"], len(self.sent)), (1, 0, 1))
        self.assertIn("실험 캠페인 오류 발생", self.sent[0][1]["text"])
        self.assertIn("발생 시각(감지 기준):", self.sent[0][1]["text"])
        self.assertIn("KST", self.sent[0][1]["text"])

        with self.store.db:
            self.store.db.execute("UPDATE jobs SET status='running',reason='' WHERE id='study-train'")
        poll_campaigns(self.store, webhook_file=self.secret, sender=self.sender)
        with self.store.db:
            self.store.db.execute("UPDATE jobs SET status='succeeded' WHERE id='study-train'")
        completed = poll_campaigns(self.store, webhook_file=self.secret, sender=self.sender)
        self.assertEqual((completed["sent"], len(self.sent)), (1, 2))
        self.assertIn("실험 캠페인 완료", self.sent[1][1]["text"])

        outbox = status(self.store)["outbox"]
        self.assertEqual([row["status"] for row in outbox], ["sent", "sent"])

    def test_missing_webhook_disables_delivery_without_consuming_outbox(self):
        self.store.register_experiment(experiment())
        register_campaign(self.store, campaign())
        with self.store.db:
            self.store.db.execute("UPDATE jobs SET status='failed' WHERE id='study-train'")
        result = poll_campaigns(self.store, webhook_file="", sender=self.sender)
        row = status(self.store)["outbox"][0]
        self.assertEqual((result["enabled"], result["queued"], row["status"], row["attempts"]),
                         (False, 1, "pending", 0))

        delivered = poll_campaigns(self.store, webhook_file=self.secret, sender=self.sender)
        self.assertEqual((delivered["sent"], len(self.sent)), (1, 1))

    def test_external_campaign_uses_supplied_group_observation(self):
        register_campaign(self.store, campaign("spatial", external=True))
        no_observation = poll_campaigns(self.store, webhook_file=self.secret, sender=self.sender)
        self.assertEqual((no_observation["sent"], self.sent), (0, []))
        result = poll_campaigns(
            self.store,
            external_observations={"spatial": {
                "state": "complete", "counts": {"complete": 11}, "jobs": 11,
                "experiments": 1, "errors": [],
            }},
            webhook_file=self.secret,
            sender=self.sender,
        )
        self.assertEqual((result["sent"], len(self.sent)), (1, 1))

    def test_secret_permissions_are_enforced_and_secret_is_not_persisted(self):
        self.store.register_experiment(experiment())
        register_campaign(self.store, campaign())
        with self.store.db:
            self.store.db.execute("UPDATE jobs SET status='failed' WHERE id='study-train'")
        self.secret.chmod(0o644)
        result = poll_campaigns(self.store, webhook_file=self.secret, sender=self.sender)
        self.assertEqual((result["enabled"], result["config_error"], self.sent),
                         (False, "ValueError", []))

        self.secret.chmod(0o600)
        poll_campaigns(self.store, webhook_file=self.secret, sender=self.sender)
        raw = (self.root / "state.db").read_bytes()
        self.assertNotIn(self.secret.read_bytes().strip(), raw)

    def test_registration_is_strict_and_idempotent(self):
        spec = campaign()
        self.assertEqual(register_campaign(self.store, spec), register_campaign(self.store, spec))
        with self.assertRaises(ValueError):
            register_campaign(self.store, dict(spec, surprise=True))
        with self.assertRaises(ValueError):
            register_campaign(self.store, dict(spec, experiments=["missing"]))

    def test_confirmed_recovery_alerts_once_and_survives_restart(self):
        self.store.register_experiment(experiment())
        self.store.register_experiment(experiment('unrelated'))
        register_campaign(self.store, campaign())
        def poll():
            return poll_campaigns(self.store, webhook_file=self.secret, sender=self.sender)
        with self.store.db:
            self.store.db.execute("UPDATE jobs SET status='failed' WHERE id='study-train'")
            self.store.db.execute("UPDATE jobs SET status='running' WHERE id='unrelated-train'")
        poll()
        with self.store.db:
            self.store.db.execute("UPDATE jobs SET status='queued' WHERE id='study-train'")
        with patch.object(self.store, 'observation_attempts', return_value=[
                {'job':'unrelated-train','status':'running','report':{'ready':True}}]):
            self.assertEqual(poll()['sent'], 0)
        self.store.db.close()
        self.store = Store(self.root / 'state.db')
        with self.store.db:
            self.store.db.execute("UPDATE jobs SET status='running' WHERE id='study-train'")
        self.assertEqual(poll()['sent'], 0)
        with patch.object(self.store, 'observation_attempts', return_value=[
                {'job':'study-train','status':'running','report':{'ready':True}}]):
            self.assertEqual(poll()['sent'], 1)
            self.assertEqual(poll()['sent'], 0)
        self.assertIn('복구 · 정상 실행 재개', self.sent[-1][1]['text'])
        self.assertIn('KST', self.sent[-1][1]['text'])
        with self.store.db:
            self.store.db.execute("UPDATE jobs SET status='failed' WHERE id='study-train'")
        self.assertEqual(poll()['sent'], 1)
        with self.store.db:
            self.store.db.execute("UPDATE jobs SET status='running' WHERE id='study-train'")
        with patch.object(self.store, 'observation_attempts', return_value=[
                {'job':'study-train','status':'running','report':{'ready':True}}]):
            self.assertEqual(poll()['sent'], 1)

    def test_direct_completion_sends_completion_not_recovery(self):
        self.store.register_experiment(experiment())
        register_campaign(self.store, campaign())
        with self.store.db: self.store.db.execute("UPDATE jobs SET status='failed'")
        poll_campaigns(self.store, webhook_file='', sender=self.sender)
        with self.store.db: self.store.db.execute("UPDATE jobs SET status='succeeded'")
        poll_campaigns(self.store, webhook_file='', sender=self.sender)
        self.assertEqual([r['state'] for r in status(self.store)['outbox']], ['error', 'complete'])

    def test_train_retry_alert_ignores_waiting_eval_and_cancelled_prepare(self):
        exp = experiment()
        exp['jobs'] += [dict(exp['jobs'][0], id='later-eval', kind='eval'),
                        dict(exp['jobs'][0], id='old-prepare', kind='prepare')]
        self.store.register_experiment(exp)
        register_campaign(self.store, campaign())
        def poll(attempts):
            with patch.object(self.store, 'observation_attempts', return_value=attempts):
                return poll_campaigns(self.store, webhook_file=self.secret, sender=self.sender)
        failed = dict(id='failed-1', job='study-train', status='failed', created=1, report={})
        retry = dict(id='retry-1', job='study-train', node='lab1', status='running', created=2,
                     report={'ready': False})
        with self.store.db:
            self.store.db.execute("UPDATE jobs SET status='failed'")
        poll([failed])
        with self.store.db:
            self.store.db.execute("UPDATE jobs SET status='running' WHERE id='study-train'")
            self.store.db.execute("UPDATE jobs SET status='queued' WHERE id='later-eval'")
            self.store.db.execute("UPDATE jobs SET status='cancelled' WHERE id='old-prepare'")
        self.assertEqual(poll([failed, retry])['sent'], 0)
        retry['report']['ready'] = True
        self.assertEqual(poll([failed, retry])['sent'], 1)
        self.assertIn('학습 작업: study-train', self.sent[-1][1]['text'])
        self.store.db.close(); self.store = Store(self.root/'state.db')
        self.assertEqual(poll([failed, retry])['sent'], 0)
        # A second failure/retry between polls must still yield its own notice.
        failed_retry = dict(retry, status='failed')
        retry2 = dict(retry, id='retry-2', created=3)
        self.assertEqual(poll([failed, failed_retry, retry2])['sent'], 1)
        self.assertEqual(poll([failed, failed_retry, retry2])['sent'], 0)

    def test_train_recovery_upgrade_catches_pending_only_and_uses_latest_attempt(self):
        from research_scheduler.notifications import _record_train_recoveries, _job_observation
        import json
        self.store.register_experiment(experiment())
        register_campaign(self.store, campaign())
        c = dict(job='study-train', attempt='retry-1', node='lab1', failed_job='study-train', failed_attempt='failed-1')
        with self.store.db:
            historical = dict(train_recoveries=[c])
            self.assertEqual(_record_train_recoveries(self.store, campaign(), historical, {}, 100), 0)
            pending = dict(train_recoveries=[c])
            old = {'_pending_recovery': {'jobs': ['study-train', 'later-eval', 'old-prepare']}}
            self.assertEqual(_record_train_recoveries(self.store, campaign(), pending, old, 101), 1)
            # Durable outbox ID dedupes even when observation metadata is replayed.
            self.assertEqual(_record_train_recoveries(self.store, campaign(), dict(train_recoveries=[c]), old, 102), 0)
            self.store.db.execute("UPDATE jobs SET status='running'")
        attempts = [dict(id='old', job='study-train', created=1, status='failed', report={'ready':True}),
                    dict(id='new', job='study-train', created=2, status='starting', report={})]
        with patch.object(self.store, 'observation_attempts', return_value=attempts):
            self.assertEqual(_job_observation(self.store, campaign())['train_recoveries'], [])
        payload = json.loads(self.store.db.execute("SELECT payload FROM notification_outbox WHERE state='recovered'").fetchone()[0])
        self.assertEqual(set(payload), {'text'})  # preserve the proven Slack payload contract
        self.assertIn('학습 작업: study-train', payload['text'])

    def test_individual_train_recovery_can_notify_while_another_job_is_failed(self):
        self.store.register_experiment(experiment())
        self.store.register_experiment(experiment('other'))
        register_campaign(self.store, campaign())
        with self.store.db:
            self.store.db.execute("UPDATE jobs SET status='failed'")
        poll_campaigns(self.store, webhook_file=self.secret, sender=self.sender)
        with self.store.db:
            self.store.db.execute("UPDATE jobs SET status='running' WHERE id='study-train'")
        attempts = [dict(id='failed', job='study-train', created=1, status='failed', report={}),
                    dict(id='retry', job='study-train', created=2, status='running', report={'ready':True})]
        with patch.object(self.store, 'observation_attempts', return_value=attempts):
            poll_campaigns(self.store, webhook_file=self.secret, sender=self.sender)
        self.assertEqual(status(self.store)['runtime']['context-campaign']['state'], 'error')
        recoveries = [r for r in status(self.store)['outbox'] if r['state']=='recovered']
        self.assertEqual(len(recoveries), 1)
        self.assertEqual(recoveries[0]['status'], 'sent')

    def test_eleven_train_recoveries_are_one_campaign_message(self):
        from research_scheduler.notifications import _record_train_recoveries
        import json
        register_campaign(self.store, campaign())
        candidates = [dict(job=f'train-{i}', attempt=f'retry-{i}', node='lab1',
                           failed_job=f'train-{i}', failed_attempt=f'failed-{i}') for i in range(11)]
        old = {'_train_recovery_seen': []}
        observation = {'train_recoveries': candidates}
        with self.store.db:
            self.assertEqual(_record_train_recoveries(self.store, campaign(), observation, old, 100), 1)
        rows=list(self.store.db.execute("SELECT payload FROM notification_outbox WHERE state='recovered'"))
        self.assertEqual(len(rows), 1)
        text=json.loads(rows[0][0])['text']
        self.assertIn('복구된 학습: 11개', text)
        for item in candidates:self.assertIn('학습 작업: '+item['job']+' ·', text)
        with self.store.db:
            self.assertEqual(_record_train_recoveries(self.store, campaign(), dict(train_recoveries=list(reversed(candidates))), old, 101), 0)
            self.assertEqual(_record_train_recoveries(self.store, campaign(), dict(train_recoveries=candidates), observation, 102), 0)

    def test_upgrade_does_not_replay_legacy_individual_recovery(self):
        from research_scheduler.notifications import _record_train_recoveries
        import hashlib
        register_campaign(self.store, campaign())
        c=dict(job='study-train', attempt='retry-1', node='lab1', failed_job='study-train', failed_attempt='failed-1')
        key=hashlib.sha256(b'context-campaign\0train-recovered\0retry-1').hexdigest()
        with self.store.db:
            self.store.db.execute("INSERT INTO notification_outbox(id,campaign,state,payload,status,next_attempt,created) VALUES(?,?,?,?,?,?,?)",
                                 (key,'context-campaign','recovered','{}','sent',100,100))
            self.assertEqual(_record_train_recoveries(self.store, campaign(), dict(train_recoveries=[c]),
                             {'_train_recovery_seen': []}, 101), 0)


if __name__ == "__main__":
    unittest.main()
