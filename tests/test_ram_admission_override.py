import copy
import unittest
from research_scheduler.planner import admission_ram


class RamAdmissionOverrideTest(unittest.TestCase):
    def setUp(self):
        self.a = dict(id='a', status='running', spec=dict(resources=dict(ram_mib=40000)),
                      report=dict(rss_mib=26000, heartbeat=100))
        self.j = dict(spec=dict(metadata=dict(ram_admission_overrides={
            'a': dict(ram_mib=32768, evidence='matched training RSS audit')})))

    def test_explicit_fresh_override_preserves_execution_spec(self):
        before = copy.deepcopy(self.a)
        self.assertEqual(admission_ram(self.a, self.j, 110), 32768)
        self.assertEqual(self.a, before)

    def test_stale_or_unknown_falls_back(self):
        self.assertEqual(admission_ram(self.a, self.j, 221), 40000)
        self.a['status'] = 'unknown'
        self.assertEqual(admission_ram(self.a, self.j, 110), 40000)

    def test_growth_and_missing_measurement_fall_back(self):
        self.a['report']['rss_mib'] = 33000
        self.assertEqual(admission_ram(self.a, self.j, 110), 40000)
        self.a['report'].pop('rss_mib')
        self.assertEqual(admission_ram(self.a, self.j, 110), 40000)

    def test_different_attempt_and_no_evidence_do_not_apply(self):
        self.assertEqual(admission_ram(self.a, {}, 110), 40000)
        self.j['spec']['metadata']['ram_admission_overrides']['a']['evidence'] = ''
        self.assertEqual(admission_ram(self.a, self.j, 110), 40000)
