import sys
import unittest
from pathlib import Path
sys.path.insert(0, str(Path(__file__).resolve().parents[1]/'src'))
from research_scheduler.agent import cgroup_memory_headroom, active_cache_allowance_fraction


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

    def test_opt_in_active_cache_is_discounted_and_excludes_mapped_dirty(self):
        m=1024**2
        stat={'inactive_file':200*m,'active_file':600*m,'file_mapped':100*m,
              'file_dirty':20*m,'file_writeback':10*m,'anon':900*m}
        value, telemetry=cgroup_memory_headroom(2000,1000*m,990*m,stat,.5)
        self.assertEqual(value,10+170+235)
        self.assertEqual(telemetry['clean_active_file_allowance_mib'],235)
        self.assertEqual(telemetry['active_file_fraction'],.5)

    def test_active_allowance_never_exceeds_host_or_container_limit(self):
        m=1024**2
        self.assertEqual(cgroup_memory_headroom(100,1000*m,990*m,{'active_file':900*m},.5)[0],100)
        self.assertEqual(cgroup_memory_headroom(2000,1000*m,0,{'active_file':9000*m},.5)[0],1000)
        for bad in (-1,.51,float('nan')):
            with self.assertRaises(ValueError):
                cgroup_memory_headroom(2000,1000*m,990*m,{},bad)

    def test_allowance_requires_local_opt_in_and_healthy_pressure(self):
        node={'filesystem':'local','labels':{'clean_active_cache_fraction':'0.5'}}
        good='some avg10=0.10 avg60=0.10\nfull avg10=0.00 avg60=0.00'
        self.assertEqual(active_cache_allowance_fraction(node,good),.5)
        self.assertEqual(active_cache_allowance_fraction(dict(node,filesystem='nfs'),good),0)
        self.assertEqual(active_cache_allowance_fraction(dict(node,labels={}),good),0)
        for pressure in ('', 'full avg10=0.50', 'full avg10=nan', 'full malformed'):
            self.assertEqual(active_cache_allowance_fraction(node,pressure),0)
        for value in ('nan','1','-1','bad'):
            self.assertEqual(active_cache_allowance_fraction(dict(node,labels={'clean_active_cache_fraction':value}),good),0)


if __name__=='__main__':unittest.main()
