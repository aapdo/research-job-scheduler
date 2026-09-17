"""Persistent model control plane without legacy experiment/migration hooks."""
import argparse
from collections import Counter
import fcntl
import json
from pathlib import Path
import signal
import threading
import time

from .controller import Controller
from .notifications import poll_campaigns
from .store import Store


def save(path,value):
    tmp=path.with_suffix('.tmp')
    tmp.write_text(json.dumps(value,ensure_ascii=False,indent=2)+'\n')
    tmp.replace(path)


def cycle(controller,output,max_launches=24):
    started=time.monotonic()
    result=controller.tick(execute=True,max_launches=max_launches,warmup=False,
                           launch_budget_s=20,parallel_launches=True)
    dispatch=time.monotonic()-started
    notification_started=time.monotonic()
    notifications=poll_campaigns(controller.store)
    notification_seconds=time.monotonic()-notification_started
    active=controller.store.attempts(active=True,summary=True)
    state=dict(time=time.time(),manager='research_scheduler.daemon',
               launches=result.get('launches',[]),
               decisions=dict(Counter(p['decision'] for p in result.get('final_plan',[]))),
               active=[dict(job=a['job'],node=a['node'],attempt=a['id'],status=a['status'],
                            gpus=a['spec']['gpus'],directory=a['spec']['attempt_dir']) for a in active],
               notifications=notifications,
               phases=dict(dispatch_s=dispatch,notifications_s=notification_seconds,
                           **result.get('phase_times',{})))
    save(output/'STATE.json',state)
    return state['phases']


def main(argv=None):
    parser=argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--db',required=True,type=Path)
    parser.add_argument('--output',required=True,type=Path)
    parser.add_argument('--interval',type=float,default=15)
    parser.add_argument('--max-launches',type=int,default=24)
    args=parser.parse_args(argv)
    if not 1<=args.interval<=3600 or not 1<=args.max_launches<=256:parser.error('invalid cadence or batch size')
    args.output.mkdir(parents=True,exist_ok=True)
    stop=threading.Event()
    for sig in (signal.SIGTERM,signal.SIGINT):signal.signal(sig,lambda *_:stop.set())
    # Keep the same Store/DB dispatch lock; additionally reject duplicate daemons.
    with args.db.with_suffix('.manager.lock').open('a') as owner:
        fcntl.flock(owner,fcntl.LOCK_EX|fcntl.LOCK_NB)
        store=Store(args.db);store.lock_wait_s=2
        try:
            controller=Controller(store)
            while not stop.is_set():
                started=time.monotonic();phases={}
                try:phases=cycle(controller,args.output,args.max_launches)
                except Exception as exc:
                    save(args.output/'LAST_CONTROLLER_ERROR.json',dict(time=time.time(),error=type(exc).__name__))
                    print(json.dumps(dict(event='controller_error',error=type(exc).__name__)),flush=True)
                elapsed=time.monotonic()-started
                timing=dict(time=time.time(),phases=phases,target_interval_s=args.interval,
                            elapsed_s=elapsed,wait_s=max(0,args.interval-elapsed),
                            overrun_s=max(0,elapsed-args.interval),max_launches=args.max_launches)
                save(args.output/'CYCLE_TIMING.json',timing)
                print(json.dumps(dict(event='cycle_timing',**timing)),flush=True)
                stop.wait(timing['wait_s'])
        finally:store.db.close()


if __name__=='__main__':main()
