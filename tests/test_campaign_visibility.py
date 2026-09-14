import json
import sqlite3
import time
import unittest
from datetime import datetime, timezone
from pathlib import Path

from test_overview import OverviewTests
from research_scheduler.overview import collect
from research_scheduler.progress_notifications import messages
from research_scheduler.visibility import expired_complete


class VisibilityTests(OverviewTests):
    def complete(self, stamp):
        c = sqlite3.connect(self.db)
        c.execute("UPDATE jobs SET status='succeeded'")
        c.execute("UPDATE attempts SET status='succeeded',report=?", (json.dumps({'finished': stamp}),))
        c.execute("INSERT INTO campaign_runtime VALUES('main','complete',1,'{}',?)", (time.time(),))
        c.execute("INSERT INTO notification_outbox(id,campaign,state,payload,status,next_attempt,created) VALUES('done','main','complete','{}','sent',?,?)", (stamp, stamp))
        c.commit(); c.close()

    def test_old_complete_hidden_despite_fresh_observer(self):
        self.complete(time.time()-37*3600)
        data = collect(self.db)
        self.assertEqual(data['campaigns'], [])
        self.assertEqual(data['jobs'], [])
        self.assertEqual(data['visibility']['hidden_campaign_count'], 1)
        c = sqlite3.connect(self.db)
        self.assertEqual(c.execute('SELECT COUNT(*) FROM campaigns').fetchone()[0], 1)
        c.close()

    def test_recent_complete_visible(self):
        self.complete(time.time()-35*3600)
        self.assertEqual(collect(self.db)['campaigns'][0]['id'], 'main')

    def test_reopened_jobs_remain_visible(self):
        self.complete(time.time()-37*3600)
        c = sqlite3.connect(self.db)
        c.execute("UPDATE jobs SET status='queued' WHERE id='pending'"); c.commit(); c.close()
        data = collect(self.db)
        self.assertEqual(data['campaigns'][0]['id'], 'main')
        self.assertIn('main', json.dumps(messages(data)))

    def test_unknown_completion_time_visible(self):
        self.complete(time.time()-37*3600)
        c = sqlite3.connect(self.db)
        c.execute('DELETE FROM notification_outbox'); c.commit(); c.close()
        self.assertEqual(collect(self.db)['campaigns'][0]['id'], 'main')

    def test_hardware_uses_completion_transition_not_state_refresh(self):
        root = Path(self.tmp.name)/'hardware'; root.mkdir()
        (root/'STATE.json').write_text(json.dumps({'phase':'complete','observed_at':time.time()}))
        (root/'HISTORY.jsonl').write_text(json.dumps({'phase':'complete','time':time.time()-37*3600})+'\n')
        (root/'INDEX.json').write_text(json.dumps({'campaigns':[{'id':'old-hw','state':'STATE.json'}]}))
        data = collect(self.db, hardware_index=root/'INDEX.json')
        self.assertEqual(data['hardware'], [])
        self.assertEqual(data['visibility']['hidden_campaign_count'], 1)


class VisibilityBoundaryTests(unittest.TestCase):
    def test_boundary_and_noncomplete(self):
        now=200000
        self.assertTrue(expired_complete('complete',now-36*3600,now))
        self.assertFalse(expired_complete('complete',now-36*3600+1,now))
        for state in ['running','error','pending']:
            self.assertFalse(expired_complete(state,1,now))
        for stamp in [None,0,True,float('nan'),float('inf')]:
            self.assertFalse(expired_complete('complete',stamp,now))

    def test_slack_omits_old_complete_rows(self):
        now=time.time()
        data=dict(time_kst=datetime.fromtimestamp(now,timezone.utc).isoformat(),
            allocation=dict(assigned_gpus=0,total_gpus=0,active_jobs=0),gpus=[],
            campaigns=[dict(id='old-hidden',recorded_state='complete',completed_at=now-37*3600,counts={'succeeded':1}),
                       dict(id='recent-kept',recorded_state='complete',completed_at=now-35*3600,counts={'succeeded':1})],
            hardware=[dict(id='old-hardware-hidden',phase='complete',completed_at=now-37*3600)])
        payload=json.dumps(messages(data))
        self.assertNotIn('old-hidden',payload)
        self.assertNotIn('old-hardware-hidden',payload)
        self.assertIn('recent-kept',payload)
