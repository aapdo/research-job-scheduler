import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch

from research_scheduler.store import Store
from research_scheduler.notifications import register_campaign, status
from research_scheduler.registration import registration_fields, campaign_label
from research_scheduler.overview import collect, markdown
from research_scheduler.progress_notifications import messages


class RegistrationTimeTests(unittest.TestCase):
    def test_automatic_time_is_immutable_on_reregistration(self):
        with tempfile.TemporaryDirectory() as tmp:
            s=Store(Path(tmp)/'state.db')
            spec=dict(id='example',name='Example',rq='Registration audit',external=True)
            with patch('research_scheduler.notifications.time.time',return_value=1789000000.):
                first=register_campaign(s,spec)
            with patch('research_scheduler.notifications.time.time',return_value=1789090000.):
                again=register_campaign(s,spec)
            self.assertEqual(first['registered_at'],1789000000.)
            self.assertEqual(first,again)
            self.assertTrue(first['registered_at_kst'].endswith('+09:00'))
            self.assertTrue(first['registered_at_utc'].endswith('+00:00'))
            self.assertEqual(status(s)['campaigns']['example']['registered_at'],1789000000.)
            data=collect(s.path)
            self.assertEqual(data['campaigns'][0]['registered_at'],1789000000.)
            self.assertIn(campaign_label(data['campaigns'][0]).replace('\n',' '),markdown(data,view='campaign'))
            self.assertEqual(s.db.execute('SELECT created FROM campaigns').fetchone()[0],1789000000.)
            s.db.close()

    def test_slack_keeps_column_order_and_one_row(self):
        c=dict(id='study',registered_at=1789000000.,counts={},counts_by_type={'train':{'running':1},'eval':{'succeeded':2}})
        data=dict(time_kst='2026-09-10T15:00:00+09:00',allocation=dict(assigned_gpus=1,total_gpus=8,active_jobs=1),
                  gpus=[],campaigns=[c],hardware=[],warnings=[])
        payload=next(p for p in messages(data) if '캠페인별 train / eval 진행' in p['text'])
        table=payload['blocks'][1]['rows']
        self.assertEqual(len(table),2)
        self.assertEqual([x['text'] for x in table[0]],['캠페인','train-finish','train-run','train-err','train-res-wait','train-dep-wait',
            'eval-finish','eval-run','eval-err','eval-res-wait','eval-dep-wait'])
        self.assertEqual(table[1][0]['text'],campaign_label(c))
        self.assertRegex(table[1][0]['text'],r'^study-\d{2}d-\d{2}h\d{2}m$')
        self.assertNotIn('등록',table[1][0]['text'])
        self.assertNotIn('KST',table[1][0]['text'])

    def test_compact_display_uses_kst_and_24_hour_clock(self):
        from datetime import datetime, timezone
        stamp=datetime(2026,9,9,21,21,20,tzinfo=timezone.utc).timestamp()
        self.assertEqual(campaign_label(dict(id='study',registered_at=stamp)),'study-10d-06h21m')
        stamp=datetime(2026,9,10,6,59,59,tzinfo=timezone.utc).timestamp()
        self.assertEqual(campaign_label(dict(id='study',registered_at=stamp)),'study-10d-15h59m')

    def test_missing_timestamp_is_not_invented(self):
        for value in (None,0,True,float('nan')):
            self.assertIsNone(registration_fields(value)['registered_at'])
            self.assertEqual(campaign_label(dict(id='legacy',registered_at=value)),'legacy')

if __name__=='__main__':unittest.main()
