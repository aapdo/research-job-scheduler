"""Bounded host PID attribution for NVML observations inside PID namespaces."""
import json,subprocess,shlex

HOSTS={
 'lab1':('192.168.1.11',8081,9052),
 'lab3':('192.168.1.13',8083,9252),'lab4':('192.168.1.14',8084,9352),
 'lab6':('192.168.1.16',8086,9552),'lab8':('192.168.1.18',8088,9752),
 'farm6':('192.168.2.16',8086,9550),'farm7':('192.168.2.17',8087,9650),
 'farm8-gui2':('192.168.2.18',8088,9753),'farm9-gui2':('192.168.2.19',8089,9853),
}
CODE=r'''
import os,json,subprocess,sys
from pathlib import Path
x=json.load(sys.stdin)
assert Path('/proc/sys/kernel/random/boot_id').read_text().strip()==x['boot']
names=subprocess.check_output(['docker','ps','--filter','publish='+str(x['port']),'--format','{{.ID}}'],text=True,timeout=2).splitlines()
assert len(names)==1
init=int(subprocess.check_output(['docker','inspect','--format','{{.State.Pid}}',names[0]],text=True,timeout=2))
ns=os.readlink('/proc/'+str(init)+'/ns/pid');out={}
for pid in x['pids']:
 try:
  root=Path('/proc')/str(pid)
  if os.readlink(root/'ns/pid')!=ns:continue
  env=dict(v.split(b'=',1) for v in (root/'environ').read_bytes().split(b'\0') if b'=' in v)
  key=env.get(b'RS_ATTEMPT_ID',b'').decode();directory=env.get(b'RS_ATTEMPT_DIR',b'').decode()
  if key in x['known'] and directory==x['known'][key]:out[str(pid)]=key
 except (OSError,UnicodeError,ValueError):continue
print(json.dumps(out))
'''

def enrich(node,request,snapshot):
    endpoint=HOSTS.get(node['id'])
    registrations=request.get('_registered_attempts',[])
    missing=[p['pid'] for g in snapshot.get('gpus',[]) for p in g.get('processes',[]) if not p.get('attempt')]
    if not endpoint or not endpoint[2] or not registrations or not missing:return snapshot
    host,port,publish=endpoint
    payload=dict(boot=snapshot.get('boot_id'),port=publish,pids=missing,
                 known={r['id']:r['attempt_dir'] for r in registrations})
    try:
        r=subprocess.run(['ssh','-T','-p',str(port),'-o','BatchMode=yes','-o','ConnectTimeout=3','jy@'+host,
            shlex.join(['sudo','-n','python3','-c',CODE])],input=json.dumps(payload),text=True,capture_output=True,timeout=8,check=True)
        owners=json.loads(r.stdout)
        for g in snapshot.get('gpus',[]):
            for p in g.get('processes',[]):
                owner=owners.get(str(p['pid']))
                if owner in payload['known']:p['attempt']=owner
    except (OSError,subprocess.SubprocessError,ValueError):pass
    return snapshot
