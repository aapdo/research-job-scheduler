import copy,json,os,signal,tempfile,time,unittest
from pathlib import Path
from unittest.mock import patch
from test_scheduler import job,node,experiment
from research_scheduler import agent
from research_scheduler.gpu_recovery import retry_spec
from research_scheduler.store import Store,dumps
from research_scheduler.controller import Controller

class OOMTests(unittest.TestCase):
    def test_controller_cleanup_only_on_execute_then_requeues(self):
        with tempfile.TemporaryDirectory() as d:
            store=Store(Path(d)/'state.db');n=node(d);store.register_node(n)
            spec=experiment([job()]);spec['jobs'][0]['hosts']=['a','b'];store.register_experiment(spec)
            request=dict(id='j.a',job='j',node_spec=n,job_spec=spec['jobs'][0],startup_group='',gpus=['GPU-a-0'],resources=spec['jobs'][0]['resources'],attempt_dir=d)
            with store.db:
                store.db.execute('insert into attempts(id,job,node,spec,status,created,report) values(?,?,?,?,?,?,?)',('j.a','j','a',dumps(request),'running',time.time(),'{}'))
                store.db.execute("update jobs set status='running' where id='j'")
            calls=[]
            class Transport:
                def call(self,node,action,request):
                    calls.append(action)
                    return dict(status='failed' if action=='recover_oom' else 'unknown',failure_class='experiment_oom',oom_node='a',termination_verified=action=='recover_oom',failure_evidence='run/stdout.log')
            controller=Controller(store,Transport())
            controller.reconcile();self.assertNotIn('recover_oom',calls);self.assertEqual(store.jobs()[0]['status'],'unknown')
            controller.reconcile(recover_oom=True)
            self.assertIn('recover_oom',calls);self.assertEqual(store.jobs()[0]['status'],'queued')
            self.assertEqual(store.jobs()[0]['spec']['metadata']['excluded_hosts'],['a'])
            self.assertTrue(store.specs('nodes')['a']['enabled']);store.db.close()

    def test_cleanup_never_signals_a_reused_pid(self):
        with tempfile.TemporaryDirectory() as d:
            root=Path(d);request=self.request(root)
            state=dict(attempt='j.a',status='failed',boot_id=Path('/proc/sys/kernel/random/boot_id').read_text().strip())
            (root/'state.json').write_text(json.dumps(state))
            report=dict(state,status='unknown',failure_class='experiment_oom',termination_verified=False,
                        oom_survivors=[dict(pid=42,start='old')],failure_evidence='stderr.log')
            with patch.object(agent,'oom_failure',return_value=report), \
                 patch.object(agent,'oom_owned_processes',side_effect=[[dict(pid=42,start='new')],[],[]]), \
                 patch.object(agent.os,'pidfd_open',return_value=55,create=True), \
                 patch.object(agent.os,'close') as close, \
                 patch.object(signal,'pidfd_send_signal',create=True) as send, \
                 patch.object(agent,'finish_oom_classification',return_value=dict(state,termination_verified=True)), \
                 patch.object(agent,'atomic_json'):
                result=agent.recover_oom(request)
            send.assert_not_called();close.assert_called_once_with(55);self.assertTrue(result['termination_verified'])

    def test_kernel_requires_fresh_matching_identity(self):
        previous=dict(boot_id='boot',time=100,processes=[dict(pid=42,start='100')])
        state=dict(boot_id='boot',attempt='job.a',finished=120)
        row=dict(_BOOT_ID='boot',__REALTIME_TIMESTAMP='110000000',__MONOTONIC_TIMESTAMP='110000000',MESSAGE='Out of memory: Killed process 42 (python)')
        self.assertEqual(agent.match_kernel_oom([row],previous,state,120)['pid'],42)
        self.assertIsNone(agent.match_kernel_oom([dict(row,MESSAGE='Killed process 43')],previous,state,120))
        self.assertIsNone(agent.match_kernel_oom([row],previous,state,200))
        self.assertIsNone(agent.match_kernel_oom([row],previous,dict(state,boot_id='new'),120))
        self.assertIsNone(agent.match_kernel_oom([row],previous,state,120,{42:dict(start='999')}))
        self.assertIsNone(agent.match_kernel_oom([dict(row,__REALTIME_TIMESTAMP='90000000')],previous,state,120))

    def request(self,root):
        return dict(id='j.a',attempt_dir=str(root),job_spec=dict(kind='train'),node_spec=dict(id='host-a'))

    def test_bank4_log_oom_waits_for_survivors_and_read_never_kills(self):
        with tempfile.TemporaryDirectory() as d:
            root=Path(d);(root/'run').mkdir();(root/'run/stdout.log').write_text('CUDA out of memory')
            request=self.request(root);state=dict(status='failed',attempt='j.a',returncode=1)
            with patch.object(agent,'oom_owned_processes',return_value=[dict(pid=42,start='1')]),patch.object(agent.os,'kill') as kill:
                result=agent.oom_failure(request,state)
                self.assertEqual(result['status'],'unknown');self.assertFalse(result['termination_verified']);kill.assert_not_called()
            with patch.object(agent,'oom_owned_processes',return_value=[]):
                result=agent.oom_failure(request,state)
                self.assertEqual(result['status'],'failed');self.assertTrue(result['termination_verified'])

    def test_exit_137_is_not_oom(self):
        with tempfile.TemporaryDirectory() as d:
            state=dict(status='failed',returncode=137)
            with patch.object(agent,'kernel_oom_evidence',return_value=None):
                self.assertEqual(agent.oom_failure(self.request(Path(d)),state),state)

    def test_hardware_is_not_model_oom(self):
        with tempfile.TemporaryDirectory() as d:
            req=self.request(Path(d));req['job_spec']['kind']='rtl_build';state=dict(status='failed')
            with patch.object(agent,'kernel_oom_evidence') as query:
                self.assertEqual(agent.oom_failure(req,state),state);query.assert_not_called()

    def test_retry_is_bounded_idempotent_and_host_specific(self):
        spec=experiment([job()])['jobs'][0];spec.update(hosts=['a','b','c','d'],max_attempts=1)
        report=dict(failure_class='experiment_oom',status='failed',termination_verified=True,oom_node='a')
        original=copy.deepcopy(spec);one=retry_spec(spec,report,1)
        self.assertEqual(spec,original);self.assertEqual(one['metadata']['excluded_hosts'],['a']);self.assertEqual(one['max_attempts'],2)
        self.assertEqual(retry_spec(one,report,1),one)
        two=retry_spec(one,dict(report,oom_node='b'),2);three=retry_spec(two,dict(report,oom_node='c'),3)
        four=retry_spec(three,dict(report,oom_node='d'),4)
        self.assertEqual(four['max_attempts'],4);self.assertEqual(len(four['metadata']['oom_failovers']),3)

    def test_unconstrained_hosts_retry_and_local_resume_holds(self):
        spec=experiment([job()])['jobs'][0];spec['hosts']=[]
        report=dict(failure_class='experiment_oom',status='failed',termination_verified=True,oom_node='a')
        self.assertEqual(retry_spec(spec,report,1)['max_attempts'],2)
        spec['config']['resume_from']='/host-only/COMMITTED.json'
        self.assertEqual(retry_spec(spec,report,1)['max_attempts'],1)
        self.assertIsNone(retry_spec(spec,dict(report,termination_verified=False),1))

if __name__=='__main__':unittest.main()
