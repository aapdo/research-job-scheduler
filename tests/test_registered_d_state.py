import json
import tempfile
import unittest
from pathlib import Path

from research_scheduler.agent import registered_d_processes, dstate_observation
from research_scheduler.states import observe_health, recovery_due
from test_scheduler import node, snapshot
from research_scheduler.planner import base_health


class RegisteredDStateTests(unittest.TestCase):
    def test_runner_descendants_orphan_tokens_and_pid_reuse(self):
        with tempfile.TemporaryDirectory() as tmp:
            root=Path(tmp)/'proc';(root/'sys/kernel/random').mkdir(parents=True)
            (root/'sys/kernel/random/boot_id').write_text('boot')
            attempt=Path(tmp)/'attempt';attempt.mkdir()
            (attempt/'state.json').write_text(json.dumps(dict(attempt='registered',runner_pid=10,runner_start='100',boot_id='boot')))
            def proc(pid,ppid,state,start='100',env=b''):
                p=root/str(pid);p.mkdir(exist_ok=True)
                fields=[state,str(ppid),'10']+['0']*16+[start]
                (p/'stat').write_text(str(pid)+' (worker) '+' '.join(fields))
                (p/'environ').write_bytes(env)
            proc(10,1,'S');proc(11,10,'D');proc(12,1,'D')
            proc(13,1,'D',env=('RS_ATTEMPT_ID=registered\0RS_ATTEMPT_DIR='+str(attempt)+'\0').encode())
            registrations=[dict(id='registered',attempt_dir=str(attempt))]
            owned,other=registered_d_processes(registrations,root)
            self.assertEqual({r['pid'] for r in owned},{11,13})
            self.assertEqual(other,1)
            proc(10,1,'S',start='reused')
            owned,other=registered_d_processes(registrations,root)
            self.assertEqual({r['pid'] for r in owned},{13})
            self.assertEqual(other,2)
            self.assertEqual(registered_d_processes([],root)[0],[])

    def test_only_same_process_for_180_seconds_is_a_problem(self):
        candidates=[dict(pid=7,start='100',attempt='a')];previous={}
        for stamp in (0,60,120,179):
            previous=dstate_observation(candidates,'boot',previous,stamp)
            self.assertEqual(previous['d_state'],0)
        result=dstate_observation(candidates,'boot',previous,180)
        self.assertEqual(result['d_state'],1)
        n=node();snap=dict(snapshot(n,now=180),**result)
        self.assertIn('180s',base_health(n,snap,180))
        self.assertEqual(base_health(n,dict(snap,**dstate_observation([], 'boot',result,181)),181),'')
        policy=n['recovery']
        state=observe_health({},result,policy,180)
        self.assertEqual(state['phase'],'d_state_wait')
        self.assertNotEqual(state['phase'],'unavailable')

    def test_recovery_gap_reboot_and_different_process_reset_timer(self):
        a=[dict(pid=7,start='100',attempt='a')]
        prev={}
        for now in (0,60,120): prev=dstate_observation(a,'boot',prev,now)
        for candidates,boot,now in [(a,'boot',181),(a,'reboot',180),
                ([dict(pid=7,start='101',attempt='a')],'boot',180),
                ([dict(pid=8,start='100',attempt='a')],'boot',180)]:
            result=dstate_observation(candidates,boot,prev,now)
            self.assertEqual(result['d_state_tracks'][0]['duration_s'],0)
        recovered=dstate_observation([],'boot',prev,150)
        self.assertEqual(dstate_observation(a,'boot',recovered,180)['d_state'],0)

    def test_legacy_d_hold_can_recover_but_ssh_hold_stays(self):
        old=dict(phase='unavailable',reason='continuous D-state exceeded timeout')
        fresh=dstate_observation([],'boot',{},200)
        self.assertTrue(recovery_due(old,200))
        self.assertEqual(observe_health(old,fresh,node()['recovery'],200)['phase'],'healthy')
        old['reason']='SSH/response recovery budget exhausted'
        self.assertFalse(recovery_due(old,200))
        self.assertEqual(observe_health(old,fresh,node()['recovery'],200),old)
