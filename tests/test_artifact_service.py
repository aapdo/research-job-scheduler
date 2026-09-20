"""Concurrency regressions: artifact SSH must not block the model dispatcher."""
import fcntl
import sqlite3
import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch

from research_scheduler.artifact_service import (
    RegistrySection, UnlockedTransport, external_artifacts_enabled, cycle)
from research_scheduler.artifacts import archive_source_in_use
from research_scheduler.store import Store, dumps


class ArtifactServiceTests(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.store = Store(Path(self.tmp.name)/'db')

    def tearDown(self):
        self.store.db.close()
        self.tmp.cleanup()

    def test_rpc_allows_another_dispatcher_lock_and_db_write_then_relocks(self):
        store = self.store
        with RegistrySection(store) as section:
            class Remote:
                def call(self, *args):
                    with store.path.with_suffix('.lock').open('a') as other:
                        fcntl.flock(other, fcntl.LOCK_EX | fcntl.LOCK_NB)
                        with sqlite3.connect(store.path) as db:
                            db.execute("INSERT INTO events(kind) VALUES('concurrent_dispatch')")
                    return {'verified': True}
            transport = UnlockedTransport(section, Remote())
            self.assertEqual(transport.call({}, 'verify', {}), {'verified': True})
            with store.path.with_suffix('.lock').open('a') as other:
                with self.assertRaises(BlockingIOError):
                    fcntl.flock(other, fcntl.LOCK_EX | fcntl.LOCK_NB)
        self.assertEqual(store.db.execute('SELECT count(*) FROM events').fetchone()[0], 1)

    def test_failed_rpc_reacquires_lock(self):
        with RegistrySection(self.store) as section:
            class Remote:
                def call(self, *args):
                    raise TimeoutError('SSH')
            with self.assertRaises(TimeoutError):
                UnlockedTransport(section, Remote()).call({}, 'status', {})
            self.assertTrue(section.held)

    def test_rpc_refuses_uncommitted_reservation(self):
        with RegistrySection(self.store) as section:
            self.store.db.execute("INSERT INTO events(kind) VALUES('uncommitted')")
            with self.assertRaisesRegex(RuntimeError, 'open SQLite transaction'):
                UnlockedTransport(section).call({}, 'launch', {})
            self.store.db.rollback()

    def test_external_owner_is_persisted_and_required(self):
        self.assertFalse(external_artifacts_enabled(self.store))
        with self.assertRaisesRegex(RuntimeError, 'not enabled'):
            cycle(self.store)
        with self.store.db:
            self.store.db.execute('CREATE TABLE artifact_service_config(id INTEGER PRIMARY KEY, enabled INTEGER)')
            self.store.db.execute('INSERT INTO artifact_service_config VALUES(1,1)')
        self.assertTrue(external_artifacts_enabled(self.store))
        with patch('research_scheduler.artifacts.tick') as tick:
            result = cycle(self.store)
        tick.assert_called_once()
        self.assertIn('registry_locked_s', result)

    def test_new_reference_during_verification_protects_source(self):
        root = '/runs/source.done'
        self.assertFalse(archive_source_in_use(self.store, root, 300))
        # Simulate a dispatcher reserving a new consumer during remote hashing.
        with self.store.db:
            self.store.db.execute('INSERT INTO experiments VALUES(?,?)', ('exp', '{}'))
            self.store.db.execute('INSERT INTO jobs(id,experiment,spec,status,created) VALUES(?,?,?,?,?)',
                                 ('consumer','exp','{}','running',1))
            self.store.db.execute('INSERT INTO attempts(id,job,node,spec,status,created) VALUES(?,?,?,?,?,?)',
                ('consumer.1','consumer','rp2',dumps({'input_files':[{'path':root+'/checkpoint'}]}),'running',1))
        self.assertTrue(archive_source_in_use(self.store, root, 300))


if __name__ == '__main__':
    unittest.main()
