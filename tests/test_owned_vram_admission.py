import unittest
from research_scheduler.planner import occupied_vram,gpu_occupancy_key,admission_vram

class OwnedVramTest(unittest.TestCase):
    def test_audited_override_requires_fresh_owned_measurement(self):
        a={'id':'a','status':'running','spec':{'resources':{'vram_mib':14000}},'report':{'ready':True,'heartbeat':100}}
        j={'spec':{'metadata':{'vram_admission_overrides':{'a':{'vram_mib':10240,'original_vram_mib':14000,'evidence':'audit'}}}}}
        a['admission_vram_mib']=admission_vram(a,j,110)
        gpu={'used_mib':8800,'processes':[{'attempt':'a','used_mib':'8700'}]}
        self.assertEqual(occupied_vram(gpu,[a],True),10340)
        gpu['processes']=[]
        self.assertEqual(occupied_vram(gpu,[a],True),22800)
        gpu={'used_mib':11000,'processes':[{'attempt':'a','used_mib':'11000'}]}
        self.assertEqual(occupied_vram(gpu,[a],True),14000)
        self.assertEqual(admission_vram(a,j,221),14000)
        self.assertEqual(admission_vram(a,{},110),14000)
    def test_owned_memory_not_counted_twice(self):
        users=[{'id':'a','spec':{'resources':{'vram_mib':10000}}}]
        gpu={'used_mib':9000,'processes':[{'attempt':'a','used_mib':'8900'}]}
        self.assertEqual(occupied_vram(gpu,users,True),10100)
        gpu['used_mib']=15000
        self.assertEqual(occupied_vram(gpu,users,True),16100)
    def test_unidentified_memory_stays_conservative(self):
        users=[{'id':'a','spec':{'resources':{'vram_mib':10000}}}]
        gpu={'used_mib':9000,'processes':[{'attempt':'another','used_mib':'8900'}]}
        self.assertEqual(occupied_vram(gpu,users,True),19000)
        gpu['processes']=[{'attempt':'a','used_mib':'N/A'}]
        self.assertEqual(occupied_vram(gpu,users,True),19000)
    def test_free_gpu_wins_across_hosts(self):
        snap={'gpus':[{'uuid':'free','processes':[]},{'uuid':'busy','processes':[{'pid':1}]}]}
        held=[{'spec':{'gpus':['busy']}}]
        self.assertLess(gpu_occupancy_key(['free'],snap,held),gpu_occupancy_key(['busy'],snap,held))
        self.assertLess(gpu_occupancy_key(['free'],snap,[]),gpu_occupancy_key(['busy'],snap,[]))

if __name__=='__main__':unittest.main()
