import copy
import json
import unittest
from test_scheduler import node,job,plan,reservation
from research_scheduler.draining import (source_upload_allowed,epoch_publication_allowed,
                                         publication_probe_due,publication_probe_health)
from research_scheduler.draining import host_resource_reservations


class DrainingTests(unittest.TestCase):
    def test_inventory_handoff_keeps_old_container_gpu_exclusivity(self):
        n=node();n['gpus']=n['gpus'][:1]
        old=reservation(n);old['node']='old-container'
        old['spec']['node_spec']=dict(storage_domain='old-local')
        n['labels']['reservation_peer_nodes']='["old-container"]'
        self.assertEqual(plan([job()],n=n,attempts=[old])[0]['decision'],'waiting')

    def test_inventory_handoff_counts_shared_job_cap(self):
        n=node();n['gpus']=n['gpus'][:1]
        n['policy'].update(allow_gpu_sharing=True,max_shared_jobs_per_gpu=2)
        old=reservation(n);old['node']='old-container'
        old['spec']['node_spec']=dict(storage_domain='old-local')
        old['spec']['resources'].update(gpu_mode='shared',vram_mib=1000)
        old['report']={'ready':True}
        another=copy.deepcopy(old);another.update(id='old2',job='old2')
        work=job(vram=1000);work['resources']['gpu_mode']='shared'
        self.assertEqual(plan([work],n=n,attempts=[old,another])[0]['decision'],'waiting')
        self.assertEqual(plan([work],n=n,attempts=[old])[0]['decision'],'ready')

    def test_partial_cutover_counts_both_container_reservations(self):
        n=dict(id='farm8-gui2',labels={'reservation_peer_nodes':'["farm8"]'})
        held=[dict(node=k,spec={}) for k in ['farm8','farm8-gui2','farm9']]
        self.assertEqual(host_resource_reservations(n,held),held[:2])
    def fixture(self):
        n=node();n['enabled']=False;n['target']='old-container'
        n['labels'].update(drain_upload_only='approved',drain_epoch_sources=json.dumps(['train.original']))
        a=dict(node=n['id'],spec=dict(node_spec=copy.deepcopy(n)))
        return n,a

    def test_only_original_source_upload_not_download(self):
        n,a=self.fixture()
        self.assertTrue(source_upload_allowed(a,n,'upload'))
        self.assertFalse(source_upload_allowed(a,n,'download'))
        n['target']='new-container'
        self.assertFalse(source_upload_allowed(a,n,'upload'))

    def test_gpu_quarantine_allows_only_original_source_publication(self):
        n,a=self.fixture()
        n['labels'].pop('drain_upload_only')
        self.assertFalse(source_upload_allowed(a,n,'upload'))
        n['labels']['gpu_runtime_quarantine']={'boot_id':'faulted-boot'}
        self.assertTrue(source_upload_allowed(a,n,'upload'))
        self.assertFalse(source_upload_allowed(a,n,'download'))
        n['target']='another-container'
        self.assertFalse(source_upload_allowed(a,n,'upload'))

    def test_quarantined_source_recovers_publication_without_gpu_readmission(self):
        n,_=self.fixture()
        n['policy']['stable_polls']=3
        n['labels'].pop('drain_upload_only')
        old={'phase':'unavailable','reason':'SSH/response recovery budget exhausted'}
        self.assertFalse(publication_probe_due(n,dict(old,next_publication_probe_at=100),99))
        self.assertFalse(publication_probe_due(n,old,0))
        n['labels']['gpu_runtime_quarantine']={'boot_id':'faulted-boot'}
        self.assertTrue(publication_probe_due(n,old,0))
        waiting=publication_probe_health(n,old,1,True,10)
        self.assertEqual(waiting['phase'],'unavailable')
        self.assertEqual(waiting['next_publication_probe_at'],12)
        restored=publication_probe_health(n,waiting,n['policy']['stable_polls'],True,12)
        self.assertEqual(restored['phase'],'publication_only')
        self.assertFalse(n['enabled'])
        self.assertEqual(publication_probe_health(n,restored,0,True,73)['phase'],'publication_only')
        self.assertEqual(publication_probe_health(n,restored,0,False,73)['phase'],'unavailable')

    def test_scientific_jobs_stay_drained(self):
        n,_=self.fixture()
        for g in (0,1):
            result=plan([job(gpu_count=g)],n=n)
            self.assertEqual(result[0]['decision'],'waiting')
            self.assertIn('disabled',str(result))

    def test_only_frozen_epoch_export_is_allowed(self):
        n,_=self.fixture();j=job(key='CSSA_EPOCH_safe',gpu_count=0)
        j.update(kind='prepare',hosts=[n['id']],config=dict(mode='publish_epoch_checkpoint',source=dict(source_attempt='train.original')))
        self.assertTrue(epoch_publication_allowed(j,n))
        self.assertEqual(plan([j],n=n)[0]['decision'],'ready')
        j['config']['source']['source_attempt']='another.run'
        self.assertFalse(epoch_publication_allowed(j,n))


if __name__=='__main__':unittest.main()
