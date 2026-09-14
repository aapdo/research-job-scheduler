"""Planning must release its snapshot without waiting for cyclic GC."""
import gc,sys,unittest,weakref
from pathlib import Path
sys.path.insert(0,str(Path(__file__).resolve().parents[1]/'src'))
from research_scheduler.planner import placements

class PlannerMemoryTests(unittest.TestCase):
    def test_completed_snapshot_not_retained_in_recursive_closure(self):
        class Sentinel:pass
        gc.collect();enabled=gc.isenabled();gc.disable()
        try:
            marker=Sentinel();ref=weakref.ref(marker)
            row=dict(id='j',experiment='e',status='succeeded',created=0,
                     spec=dict(priority=0,depends_on=[]),sentinel=marker)
            self.assertEqual(placements([row],{'e':{'priority':0}},{},{},[],{},now=1),[])
            del row,marker
            self.assertIsNone(ref(), 'planner retained the previous snapshot until cyclic GC')
        finally:
            if enabled:gc.enable()
            gc.collect()
