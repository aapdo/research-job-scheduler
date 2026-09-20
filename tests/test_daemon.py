import tempfile
import unittest
from pathlib import Path
from unittest.mock import Mock,patch
from research_scheduler import daemon

class DaemonTests(unittest.TestCase):
    def test_notification_lock_contention_preserves_successful_dispatch(self):
        controller=Mock()
        controller.tick.return_value={'launches':[], 'final_plan':[], 'phase_times':{'controller_artifacts_s':0}}
        controller.store.attempts.return_value=[]
        with tempfile.TemporaryDirectory() as folder, patch.object(daemon,'poll_campaigns',side_effect=
                RuntimeError('another controller/registry operation holds the scheduler lock')):
            result=daemon.cycle(controller,Path(folder))
        self.assertEqual(result['controller_artifacts_s'],0)

    def test_second_daemon_fails_before_opening_scheduler(self):
        import fcntl
        with tempfile.TemporaryDirectory() as folder:
            db=Path(folder)/'state.db'
            with db.with_suffix('.manager.lock').open('a') as owner:
                fcntl.flock(owner,fcntl.LOCK_EX|fcntl.LOCK_NB)
                with patch.object(daemon.signal,'signal'),patch.object(daemon,'Store') as store:
                    with self.assertRaises(BlockingIOError):
                        daemon.main(['--db',str(db),'--output',str(Path(folder)/'output')])
                    store.assert_not_called()

    def test_cycle_uses_existing_dispatcher_no_legacy_hooks(self):
        controller=Mock()
        controller.tick.return_value={'launches':[],'final_plan':[{'decision':'waiting'}]}
        controller.store.attempts.return_value=[]
        with tempfile.TemporaryDirectory() as folder,patch.object(daemon,'poll_campaigns',return_value={'sent':0}):
            daemon.cycle(controller,Path(folder))
            self.assertTrue((Path(folder)/'STATE.json').is_file())
        controller.tick.assert_called_once_with(execute=True,max_launches=24,warmup=False,launch_budget_s=20,parallel_launches=True,final_reconcile=False)
        controller.store.attempts.assert_called_once_with(active=True,summary=True)

if __name__=='__main__':unittest.main()
