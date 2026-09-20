"""Archive policy, location retirement, and report placement regressions (offline)."""
import copy
import json
import os
import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch

from test_scheduler import node, snapshot, job, experiment, plan, reservation
from research_scheduler import relay_worker
from research_scheduler.artifacts import attempt_archive_policy, start_pending_attempt_archive
from research_scheduler.controller import Controller
from research_scheduler.notifications import register_campaign
from research_scheduler.report_placement import on_archive_host, required_dependency_host
from research_scheduler.store import Store, dumps


class ArchiveLifecycleTests(unittest.TestCase):
    def test_retired_origin_cannot_be_used_even_on_original_server(self):
        origin = node()
        done = reservation(origin, key='first', status='succeeded')
        done['spec']['node_spec'] = origin
        done['origin_retired'] = True
        child = job('child', deps=['first'])
        args = dict(attempts=[done], statuses={'first': 'succeeded'})
        self.assertEqual(plan([job('first'), child], n=origin, **args)[0]['decision'], 'waiting')
        done['artifact_locations'] = {'a': {'root': '/verified/new-copy'}}
        self.assertEqual(plan([job('first'), child], n=origin, **args)[0]['decision'], 'ready')

    def test_same_host_request_uses_verified_archive_when_origin_retired(self):
        with tempfile.TemporaryDirectory() as folder:
            s = Store(Path(folder) / 'state.db')
            try:
                n = node(key='lab4'); s.register_node(n)
                first = job('first'); child = job('child', deps=['first'])
                child['argv'] = ['cat', '{dep:first}/RESULT.json']
                s.register_experiment(experiment([first, child]))
                files = {'RESULT.json': dict(bytes=1, sha256='a'*64)}
                receipt = dict(root='/lab4/attempt-archive/first', complete_attempt=True,
                               files={'RESULT.json': dict(path='/lab4/attempt-archive/first/RESULT.json', **files['RESULT.json'])})
                with s.db:
                    s.db.execute("UPDATE jobs SET status='succeeded' WHERE id='first'")
                    s.db.execute('INSERT INTO attempts(id,job,node,spec,status,created,report) VALUES(?,?,?,?,?,?,?)',
                                 ('first.done','first','lab4',dumps(dict(node_spec=n,attempt_dir='/deleted')),
                                  'succeeded',1,dumps({'outputs':files})))
                    s.db.execute('INSERT INTO artifact_transfers VALUES(?,?,?,?,?,?,?,?)',
                                 ('arch','first.done','lab4','archive','succeeded','{}',
                                  dumps(dict(artifact=receipt, origin_retired=True)),2))
                request = Controller(s).request(dict(job='child', node='lab4', gpus=[]))
                self.assertEqual(request['argv'], ['cat','/lab4/attempt-archive/first/RESULT.json'])
                self.assertEqual(len(request['input_files']), 1)
            finally:
                s.db.close()

    def test_future_campaign_selected_without_editing_allowlist(self):
        with tempfile.TemporaryDirectory() as folder:
            s = Store(Path(folder)/'db')
            try:
                s.register_node(node(key='source'))
                s.register_experiment(experiment([job('old')],key='old-exp'))
                s.register_experiment(experiment([job('new')],key='new-exp'))
                register_campaign(s,dict(id='old-campaign',name='old',rq='old',experiments=['old-exp']))
                register_campaign(s,dict(id='new-campaign',name='new',rq='new',experiments=['new-exp']))
                policy = Path(folder)/'policy.json'
                policy.write_text(dumps(dict(archive_node='lab4',campaigns=['old-campaign'],
                                             delete_after_idle_s=300,automatic_since=10,
                                             backfill_nodes=['source'])))
                with patch.dict(os.environ,RS_ATTEMPT_ARCHIVE_POLICY=str(policy)):
                    value = attempt_archive_policy(s)
                self.assertIn('new-exp', value['experiments'])
                self.assertNotIn('new-exp', value['backfill_experiments'])
                self.assertEqual(value['experiment_campaigns']['new-exp'], ['new-campaign'])
                self.assertEqual(value['backfill_nodes'], ['source'])
                self.assertEqual(value['delete_after_idle_s'], 300)
            finally:
                s.db.close()

    def test_future_portable_report_registered_on_lab4_idempotently(self):
        with tempfile.TemporaryDirectory() as folder:
            s = Store(Path(folder)/'db')
            try:
                s.register_node(node(key='lab4'))
                j = job('STUDY_REPORT',gpu_count=0)
                j.update(kind='analysis',argv=['/control/python','-c','import json; print(json.dumps({}))'])
                raw = experiment([j])
                first = s.register_experiment(raw)
                second = s.register_experiment(raw)
                self.assertEqual(first, second)
                stored = s.jobs()[0]['spec']
                self.assertEqual(stored['hosts'], ['lab4'])
                self.assertEqual(required_dependency_host(stored, {}), 'lab4')
                self.assertEqual(stored['metadata']['report_execution'], 'archive_host')
            finally:
                s.db.close()

    def test_report_with_nonportable_runtime_requires_lab4_profile(self):
        j = job('STUDY_REPORT',gpu_count=0)
        j.update(kind='analysis',argv=['/control/python','/control/worker.py'])
        with self.assertRaisesRegex(ValueError, 'LAB4 runtime profile'):
            on_archive_host(j)

    def test_server_direct_never_calls_controller_payload_stream(self):
        config = dict(destination_target='lab4',destination_root='/lab4/archive/x',
                      source_target='source',source_root='/source/x',files={},transport_route='server-direct')
        with patch.object(relay_worker,'run'), patch.object(relay_worker,'pull_on_lab4') as pull, \
                patch.object(relay_worker,'stream_between_remotes') as stream:
            relay_worker.publish_direct(config)
        pull.assert_called_once()
        stream.assert_not_called()

    def test_failed_direct_connection_does_not_fallback_to_local_payload(self):
        config = dict(destination_target='lab4',destination_root='/lab4/archive/x',
                      source_target='source',source_root='/source/x',files={},transport_route='server-direct')
        with patch.object(relay_worker,'run'), patch.object(relay_worker,'subprocess') as subprocess, \
                patch.object(relay_worker,'pull_on_lab4',side_effect=RuntimeError('no route')), \
                patch.object(relay_worker,'stream_between_remotes') as stream:
            with self.assertRaises(RuntimeError):
                relay_worker.publish_direct(config)
        stream.assert_not_called()


if __name__ == '__main__':
    unittest.main()
