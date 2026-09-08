import tempfile
import threading
import time
import unittest
from pathlib import Path
from research_scheduler.store import Store


class RegistryLockWaitTests(unittest.TestCase):
    def test_default_still_fails_fast_and_timeout_is_bounded(self):
        with tempfile.TemporaryDirectory() as root:
            store=Store(Path(root)/'db')
            with store.lock():
                with self.assertRaisesRegex(RuntimeError,'scheduler lock'):
                    with store.lock(): pass
                started=time.monotonic()
                with self.assertRaisesRegex(RuntimeError,'scheduler lock'):
                    with store.lock(timeout=.06,poll_interval=.01): pass
                self.assertGreaterEqual(time.monotonic()-started,.05)
                self.assertLess(time.monotonic()-started,1)
            with store.lock(timeout=.1): pass
            store.db.close()

    def test_waiter_acquires_after_other_operation_releases(self):
        with tempfile.TemporaryDirectory() as root:
            store=Store(Path(root)/'db')
            entered=threading.Event();release=threading.Event()
            def holder():
                with store.lock():
                    entered.set();release.wait(2)
            thread=threading.Thread(target=holder);thread.start()
            self.assertTrue(entered.wait(1))
            timer=threading.Timer(.05,release.set);timer.start()
            with store.lock(timeout=1,poll_interval=.01):
                self.assertTrue(release.is_set())
            thread.join(1);timer.join(1)
            self.assertFalse(thread.is_alive())
            store.db.close()


if __name__=='__main__':unittest.main()
