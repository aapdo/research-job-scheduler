"""Campaign notifications use a durable outbox and never persist webhook secrets."""
import sys
import tempfile
import unittest
from unittest.mock import patch
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "src"))

from research_scheduler.notifications import poll_campaigns, register_campaign, status, webhook_path
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


if __name__ == "__main__":
    unittest.main()
