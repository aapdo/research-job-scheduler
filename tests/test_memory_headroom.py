import sys
import unittest
from pathlib import Path
sys.path.insert(0, str(Path(__file__).resolve().parents[1]/'src'))
from research_scheduler.agent import cgroup_memory_headroom


class MemoryHeadroomTests(unittest.TestCase):
    def test_clean_inactive_cache_is_reclaimable(self):
        m=1024**2
        value,telemetry=cgroup_memory_headroom(2000,1000*m,990*m,{'inactive_file':500*m})
        self.assertEqual(value,510)
        self.assertEqual(telemetry['raw_headroom_mib'],10)

    def test_dirty_active_and_missing_cache_not_counted(self):
        m=1024**2
        self.assertEqual(cgroup_memory_headroom(2000,1000*m,990*m,{'active_file':900*m})[0],10)
        self.assertEqual(cgroup_memory_headroom(2000,1000*m,990*m,{})[0],10)
        stat={'inactive_file':500*m,'file_dirty':100*m,'file_writeback':50*m}
        self.assertEqual(cgroup_memory_headroom(2000,1000*m,990*m,stat)[0],360)

    def test_host_and_cgroup_limits_are_still_respected(self):
        m=1024**2
        self.assertEqual(cgroup_memory_headroom(100,1000*m,990*m,{'inactive_file':500*m})[0],100)
        self.assertEqual(cgroup_memory_headroom(2000,1000*m,0,{'inactive_file':5000*m})[0],1000)
        self.assertEqual(cgroup_memory_headroom(2000,1000*m,1100*m,{'inactive_file':50*m})[0],0)


if __name__=='__main__':unittest.main()
