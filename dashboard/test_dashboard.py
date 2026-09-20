import importlib.util
import json
import sqlite3
import tempfile
import time
import unittest
from pathlib import Path

spec=importlib.util.spec_from_file_location('dashboard_server',Path(__file__).with_name('server.py'))
server=importlib.util.module_from_spec(spec);spec.loader.exec_module(server)


class DashboardTests(unittest.TestCase):
    def test_gpu_pool_order_matches_scheduler_policy(self):
        self.assertEqual(server.gpu_pool('rp2'),('train',1))
        self.assertEqual(server.gpu_pool('cps2-model'),('train',2))
        self.assertEqual(server.gpu_pool('cps1-model'),('train',3))
        self.assertEqual(server.gpu_pool('farm9-gui2'),('train',4))
        self.assertEqual(server.gpu_pool('lab1'),('train',5))
        self.assertEqual(server.gpu_pool('farm6'),('train',7))
        self.assertEqual(server.gpu_pool('farm7'),('train',8))
        self.assertEqual(server.gpu_pool('lab3'),('train',9))
        self.assertEqual(server.gpu_pool('rp1'),('other',999))
        self.assertEqual(server.gpu_pool('rp3'),('other',999))
        self.assertEqual(server.gpu_pool('farm1'),('other',999))
        self.assertEqual(server.gpu_pool('lab2'),('eval',1))
        self.assertEqual(server.gpu_pool('lab6'),('eval',4))

    def test_dashboard_compact_gzip_preserves_campaign_details(self):
        import gzip
        import threading
        import urllib.request
        from types import SimpleNamespace
        data={'campaigns':[{'jobs':[{'id':'train','reason':'x'*1000}]}],'waiting':[{'id':'duplicate'}]}
        http=server.ThreadingHTTPServer(('127.0.0.1',0),server.Handler)
        http.cache=SimpleNamespace(snapshot=lambda:(data,200))
        worker=threading.Thread(target=http.serve_forever,daemon=True);worker.start()
        try:
            url='http://127.0.0.1:'+str(http.server_port)+'/api/state'
            req=urllib.request.Request(url+'?view=dashboard',headers={'Accept-Encoding':'gzip'})
            with urllib.request.urlopen(req,timeout=3) as response:
                self.assertEqual(response.headers['Content-Encoding'],'gzip')
                decoded=json.loads(gzip.decompress(response.read()))
                self.assertEqual(decoded['campaigns'],data['campaigns'])
                self.assertNotIn('waiting',decoded)
            with urllib.request.urlopen(url,timeout=3) as response:
                self.assertEqual(json.load(response),data)
            self.assertIn('waiting',data)
        finally:http.shutdown();http.server_close();worker.join()

    def test_bulk_read_is_scoped_idempotent_and_validated(self):
        with tempfile.TemporaryDirectory() as tmp:
            box=server.Inbox(Path(tmp)/'inbox.db')
            box.ingest([dict(id=k,category=cat,created=i,kind='error',text='test') for i,(k,cat) in enumerate([('model:a','model'),('model:b','model'),('hardware:a','hardware')])])
            result=box.mark_read_many(['model:a','model:b','model:a','missing'])
            self.assertEqual(set(result['ids']),{'model:a','model:b'})
            rows={r['id']:r for r in box.rows()}
            self.assertIsNone(rows['hardware:a']['read_at'])
            stamp=rows['model:a']['read_at'];box.mark_read_many(['model:a'])
            self.assertEqual({r['id']:r for r in box.rows()}['model:a']['read_at'],stamp)
            for bad in ([], 'model:a', [None], ['a']*501, ['']):
                with self.assertRaises(ValueError):box.mark_read_many(bad)

    def test_model_resource_wait_counts_unique_train_eval_only(self):
        def job(key,kind='train',status='queued',category='resource_wait'):
            return dict(id=key,work_type=kind,status=status,waiting=dict(category=category))
        rows=[job('train'),job('eval','eval'),job('train'),job('dependency',category='dependency_wait'),
              job('support','support'),job('build','build'),job('running',status='running'),
              job('cancelled',status='cancelled')]
        self.assertEqual(server.model_resource_waiting(rows),2)
        self.assertEqual(server.model_resource_waiting([]),0)

    def test_gpu_average_includes_unassigned_and_preserves_job_transitions(self):
        averages=server.GPUAverages()
        gpu=dict(node='lab3',uuid='GPU-A',assigned_attempts=[],sample_started_at=990,
                 snapshot_at=1000,utilization_percent=0,stale=False)
        def observe(now, **changes):
            gpu.update(changes);averages.update([gpu],now)
            return gpu['utilization_average']
        self.assertEqual(observe(1000)['sample_count'],1)
        # Unassigned physical usage (including other users' jobs) is included.
        self.assertEqual(observe(1030,sample_started_at=1020,utilization_percent=100)['percent'],50)
        self.assertEqual(observe(1040,assigned_attempts=['attempt-1'])['sample_count'],2)
        self.assertEqual(observe(1060,sample_started_at=1050,utilization_percent=50)['percent'],50)
        self.assertEqual(observe(1070,assigned_attempts=['attempt-2'])['sample_count'],3)
        self.assertEqual(observe(1080,assigned_attempts=['attempt-2','attempt-3'])['percent'],50)
        self.assertEqual(observe(1090,assigned_attempts=[])['percent'],50)
        self.assertEqual(observe(1090)['window_s'],180)
        # Samples expire by measurement time, not job lifetime.
        self.assertEqual(observe(1171)['percent'],75)
        self.assertIsNone(observe(1201)['percent'])
        self.assertEqual(gpu['utilization_percent'],50)  # raw telemetry unchanged

    def test_gpu_average_window_invalid_values_and_real_zero(self):
        averages=server.GPUAverages()
        gpu=dict(node='lab3',uuid='GPU-A',assigned_attempts=['attempt-1'],
                 sample_started_at=700,utilization_percent=100,stale=False)
        averages.update([gpu],1000)
        for now,stamp,value,stale in [(1030,1020,100,False),(1060,1050,0,False),
                (1090,1080,None,False),(1120,1110,float('nan'),False),
                (1100,1090,True,False),(1110,1100,101,False),
                (1150,1140,50,True),(1180,1200,50,False)]:
            gpu.update(sample_started_at=stamp,utilization_percent=value,stale=stale)
            averages.update([gpu],now)
        result=gpu['utilization_average']
        self.assertEqual(result['sample_count'],2)
        self.assertEqual(result['percent'],50)  # real assigned zero is not hidden
        averages.update([gpu],1551)
        self.assertIsNone(gpu['utilization_average']['percent'])
        self.assertEqual(gpu['utilization_average']['sample_count'],0)
        averages.update([],1552)
        self.assertEqual(averages.series,{})

    def test_inbox_read_persists_and_expires_without_reimport(self):
        with tempfile.TemporaryDirectory() as tmp:
            path=Path(tmp)/'inbox.db';box=server.Inbox(path)
            events=[dict(id='model:a',category='model',created=100,kind='error',text='test')]
            box.ingest(events);self.assertIsNone(box.rows()[0]['read_at'])
            self.assertTrue(box.mark_read('model:a'))
            stamp=box.rows()[0]['read_at'];box.mark_read('model:a')
            self.assertEqual(server.Inbox(path).rows()[0]['read_at'],stamp)
            with box.connect() as db:db.execute('UPDATE inbox SET read_at=?',(time.time()-86401,))
            self.assertEqual(box.rows(),[])
            box.ingest(events);self.assertEqual(box.rows(),[])
            self.assertFalse(box.mark_read('missing'))
            box.ingest([dict(id='model:b',category='model',created=101,kind='started',text='next')])
            self.assertEqual(box.rows()[0]['id'],'model:b')

    def test_equal_timestamp_events_and_independent_sources(self):
        with tempfile.TemporaryDirectory() as tmp:
            box=server.Inbox(Path(tmp)/'inbox.db')
            box.ingest([dict(id=x,category=x.split(':')[0],created=10,kind='error')
                        for x in ['model:b','hardware:a','model:a']])
            self.assertEqual(len(box.rows()),3)

    def test_experiment_database_connection_is_query_only(self):
        with tempfile.TemporaryDirectory() as tmp:
            path=Path(tmp)/'source.db'
            with sqlite3.connect(path) as db:db.execute('CREATE TABLE original(id INTEGER)')
            db=server.database(path)
            with self.assertRaises(sqlite3.OperationalError):db.execute('INSERT INTO original VALUES(1)')
            db.close()

    def test_secret_and_nonfinite_redaction(self):
        cleaned=server.clean(dict(text='https://hooks.slack.com/services/PRIVATE hf_abcdefghijklmno',value=float('nan')))
        self.assertNotIn('PRIVATE',cleaned['text']);self.assertNotIn('hf_',cleaned['text'])
        json.dumps(cleaned,allow_nan=False)


if __name__=='__main__':unittest.main()
