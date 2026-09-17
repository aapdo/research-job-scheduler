import unittest
from test_scheduler import node, snapshot, job, plan, reservation


class GPUScopedAdmissionTests(unittest.TestCase):
    def test_hot_or_missing_gpu_does_not_block_cool_neighbor(self):
        n = node(); n['policy']['temperature_scope'] = 'gpu'
        for temperature in (None, 80, 85, 95):
            s = snapshot(n); s['gpus'][0]['temperature_c'] = temperature
            s['gpus'][0]['processes'] = [123]
            r = plan([job()], n=n, snap=s)[0]
            self.assertEqual(r['gpus'], ['GPU-a-1'])

    def test_shared_utilization_limit_does_not_relax_exclusive_or_vram(self):
        n = node(); n['policy'].update(allow_gpu_sharing=True, max_shared_gpu_percent=100)
        s = snapshot(n)
        for g in s['gpus']: g['util_percent'] = 99
        self.assertEqual(plan([job()], n=n, snap=s)[0]['decision'], 'waiting')
        j = job(); j['resources']['gpu_mode'] = 'shared'
        self.assertEqual(plan([j], n=n, snap=s)[0]['decision'], 'ready')
        for g in s['gpus']: g['used_mib'] = 23500
        self.assertEqual(plan([j], n=n, snap=s)[0]['decision'], 'waiting')

    def test_disabled_uuid_cannot_be_admitted_even_if_enabled_flag_is_true(self):
        n = node(); n['policy']['disabled_gpu_uuids'] = ['GPU-a-0']
        self.assertEqual(plan([job()], n=n)[0]['gpus'], ['GPU-a-1'])

    def test_priority_only_prefers_healthy_eligible_node(self):
        a, b = node(), node(key='b'); b['admission_priority'] = 1
        snaps = {'a': snapshot(a), 'b': snapshot(b)}
        self.assertEqual(plan([job()], nodes={'a': a, 'b': b}, snaps=snaps)[0]['node'], 'b')
        snaps['b']['read_ok'] = False
        self.assertEqual(plan([job()], nodes={'a': a, 'b': b}, snaps=snaps)[0]['node'], 'a')

    def test_gpu_load_outweighs_one_priority_tier(self):
        high, lower = node(key='high'), node(key='lower')
        high['gpus']=high['gpus'][:1];lower['gpus']=lower['gpus'][:1]
        high['admission_priority']=400;lower['admission_priority']=300
        for n in (high,lower):
            n['policy'].update(allow_gpu_sharing=True,max_shared_jobs_per_gpu=3)
        old=reservation(high,key='old',gpu=0);old['report']={'ready':True}
        old['spec']['job_kind']='eval';old['spec']['resources'].update(gpu_mode='shared',vram_mib=1000)
        candidate=job('next',vram=1000);candidate['kind']='eval';candidate['resources']['gpu_mode']='shared'
        row=plan([candidate],nodes={'high':high,'lower':lower},
                 snaps={'high':snapshot(high),'lower':snapshot(lower)},attempts=[old])[0]
        self.assertEqual(row['node'],'lower')

    def test_warm_occupied_gpu_blocked_but_other_gpu_usable(self):
        n = node(); n['policy'].update(temperature_scope='gpu', allow_gpu_sharing=True)
        s = snapshot(n); s['gpus'][0]['temperature_c'] = 81
        r = reservation(n); r['spec']['resources']['gpu_mode'] = 'shared'; r['report'] = {'ready': True}
        j = job(); j['resources']['gpu_mode'] = 'shared'
        self.assertEqual(plan([j], n=n, snap=s, attempts=[r])[0]['gpus'], ['GPU-a-1'])


if __name__ == '__main__': unittest.main()
