"""Content-addressed resource inventory and demand-driven verified replication.

Known locations are hints, never proof of complete bytes. CPU jobs inspect the
destination, select/revalidate a source, copy missing bytes, and publish a receipt.
Runtime compatibility and the existing GPU/host policy remain separate gates.
"""
import copy
import hashlib
import json
from pathlib import Path
import time

from .schema import absolute,check,identifier,experiment_spec,number
from .store import ACTIVE,dumps


def digest(value):return hashlib.sha256(dumps(value).encode()).hexdigest()

def target(node):return '@local' if node['transport']=='local' else node['target']

def tables(store):
    store.db.execute('CREATE TABLE IF NOT EXISTS resource_catalog(id TEXT PRIMARY KEY,spec TEXT NOT NULL)')
    store.db.execute("CREATE TABLE IF NOT EXISTS resource_locations(resource TEXT,node TEXT,path TEXT,state TEXT,job TEXT,receipt TEXT NOT NULL DEFAULT '{}',PRIMARY KEY(resource,node))")

def marker(resource,manifest_sha256):
    value=dict(resource=resource,manifest_sha256=manifest_sha256)
    return dict(name='resource-'+resource,content=dumps(value),sha256=digest(value))

def specification(raw):
    c=copy.deepcopy(raw)
    check(set(c)=={'id','manifest_file','manifest_sha256','sources','destinations','coordinator','match','max_parallel'},'invalid resource catalog')
    identifier(c['id']);check(len(c['id'])<=70,'resource ID too long');identifier(c['coordinator'])
    absolute(c['manifest_file']);check(len(c['manifest_sha256'])==64 and all(v in '0123456789abcdef' for v in c['manifest_sha256']),'invalid manifest SHA')
    check(c['sources'] and c['destinations'],'source and destination locations required')
    for locations in (c['sources'],c['destinations']):
        check(isinstance(locations,dict),'locations must map registered nodes to roots')
        for node,path in locations.items():
            identifier(node);absolute(path);check(len(Path(path).parts)>=4,'dedicated resource root required')
    check(set(c['match'])<= {'dataset','cwd'},'invalid resource demand matcher')
    check(c['match'],'explicit demand matcher required')
    if c['match'].get('cwd'):absolute(c['match']['cwd'])
    number(c['max_parallel'],'max_parallel',1,True);check(c['max_parallel']<=4,'max four resource workers')
    return c

def register(store,raw):
    c=specification(raw)
    path=Path(c['manifest_file']);check(hashlib.sha256(path.read_bytes()).hexdigest()==c['manifest_sha256'],'manifest changed')
    with store.lock(),store.db:
        tables(store);nodes=store.specs('nodes')
        check(set(c['sources'])|set(c['destinations'])<=set(nodes),'unknown resource location')
        check(nodes[c['coordinator']]['transport']=='local','resource worker must run on local coordinator')
        old=store.db.execute('SELECT spec FROM resource_catalog WHERE id=?',(c['id'],)).fetchone()
        check(not old or json.loads(old['spec'])==c,'immutable resource: register new ID for changed content')
        if not old:
            store.db.execute('INSERT INTO resource_catalog VALUES(?,?)',(c['id'],dumps(c)))
            for n,p in c['sources'].items():
                store.db.execute('INSERT INTO resource_locations(resource,node,path,state) VALUES(?,?,?,?)',(c['id'],n,p,'known'))
            store.event('resource_registered',c['id'],dict(sources=list(c['sources']),destinations=list(c['destinations'])))
    return c

def required(job,catalog):
    return job['status']=='queued' and job['spec']['kind'] in ('train','eval') and all(job['spec'].get(k)==v for k,v in catalog['match'].items())

def availability(job,node,snapshot):
    for resource,sha in job.get('metadata',{}).get('required_resources',{}).items():
        m=marker(resource,sha)
        if (snapshot or {}).get('assets',{}).get(m['name'])!=m['sha256']:
            return 'resource replication/verification pending: '+resource
    return ''

def tick(controller,execute=False):
    if not execute:return
    store=controller.store;tables(store)
    catalogs=[json.loads(r['spec']) for r in store.db.execute('SELECT * FROM resource_catalog')]
    if not catalogs:return
    jobs={j['id']:j for j in store.jobs()};nodes=store.specs('nodes');snapshots=controller.snapshots()
    prepared_jobs={r[0] for r in store.db.execute('SELECT job FROM resource_locations WHERE job IS NOT NULL')}
    attempts={a['job']:a for a in store.attempts(job_ids=prepared_jobs) if a['status']=='succeeded'}
    for c in catalogs:
        locations={r['node']:dict(r) for r in store.db.execute('SELECT * FROM resource_locations WHERE resource=?',(c['id'],))}
        for n,location in locations.items():
            if location['state']=='ready':
                snap=snapshots.get(n,{})
                m=marker(c['id'],c['manifest_sha256'])
                receipt=json.loads(location['receipt'])
                if (nodes[n]['enabled'] and snap.get('read_ok') and not snap.get('error')
                    and 0<=time.time()-snap.get('received_at',0)<=nodes[n]['policy']['max_snapshot_age_s']
                    and snap.get('received_at',0)>receipt.get('verified_at',0)
                    and snap.get('assets',{}).get(m['name'])!=m['sha256']):
                    with store.db:
                        store.db.execute("UPDATE resource_locations SET state='known',job=NULL WHERE resource=? AND node=?",(c['id'],n))
                        store.event('resource_replica_recheck_needed',c['id'],dict(node=n))
                    location['state']='known';location['job']=None
            if not location['job'] or location['state']=='ready':continue
            state=jobs[location['job']]['status']
            if state=='succeeded':
                try:
                    a=attempts[location['job']];check(a['spec']['node_spec']['transport']=='local','local receipt required')
                    check(target(nodes[n])==a['spec']['config']['target'],'destination identity changed during copy')
                    p=Path(a['spec']['attempt_dir'])/'RESOURCE_READY.json'
                    output=a['report']['outputs']['RESOURCE_READY.json']
                    check(output['path']==str(p) and hashlib.sha256(p.read_bytes()).hexdigest()==output['sha256'],'resource receipt changed')
                    receipt=json.loads(p.read_text());check(receipt['status']=='complete' and receipt['resource']==c['id'] and receipt['node']==n and receipt['root']==c['destinations'][n] and receipt['manifest_sha256']==c['manifest_sha256'],'resource identity mismatch')
                    # Preparation cannot lift a user restriction changed mid-copy.
                    if not nodes[n]['enabled']:continue
                    m=marker(c['id'],c['manifest_sha256']);node=copy.deepcopy(nodes[n])
                    node['assets'][m['name']]=dict(path=str(Path(location['path'])/'.resource-ready'/c['id']),sha256=m['sha256'])
                    from .schema import node_spec
                    node_spec(node)
                    with store.db:
                        store.db.execute('UPDATE nodes SET spec=? WHERE id=?',(dumps(node),n))
                        store.db.execute('DELETE FROM snapshots WHERE node=?',(n,))
                        store.db.execute("UPDATE resource_locations SET state='ready',receipt=? WHERE resource=? AND node=?",(dumps(receipt),c['id'],n))
                        store.event('resource_replica_ready',c['id'],dict(node=n,source=receipt.get('source'),copied=receipt.get('copied')))
                    nodes[n]=node;location['state']='ready'
                except Exception as exc:
                    with store.db:store.db.execute("UPDATE resource_locations SET state='verification_failed',receipt=? WHERE resource=? AND node=?",(dumps(dict(error=type(exc).__name__)),c['id'],n))
            else:
                with store.db:store.db.execute('UPDATE resource_locations SET state=? WHERE resource=? AND node=?',(state,c['id'],n))
        consumers=[j for j in jobs.values() if required(j,c)]
        if not consumers:continue
        # Attach content identity, not mutable source/host paths, to future jobs.
        with store.db:
            for j in consumers:
                spec=copy.deepcopy(j['spec']);req=spec.setdefault('metadata',{}).setdefault('required_resources',{})
                # A conflicting job stays behind its immutable identity gate;
                # never halt unrelated scheduling or silently change that SHA.
                if c['id'] in req and req[c['id']]!=c['manifest_sha256']:continue
                if c['id'] not in req:
                    req[c['id']]=c['manifest_sha256']
                    store.db.execute("UPDATE jobs SET spec=? WHERE id=? AND status='queued'",(dumps(spec),j['id']));j['spec']=spec
        active=sum(bool(r['job']) and jobs[r['job']]['status'] in (*ACTIVE,'queued') for r in locations.values())
        for n,root in c['destinations'].items():
            node=nodes[n];old=locations.get(n)
            if not node['enabled'] or active>=c['max_parallel']:continue
            previous=[j for j in jobs.values() if j['spec'].get('config',{}).get('resource')==c['id'] and j['spec'].get('config',{}).get('node')==n and j['id'].startswith('RESOURCE_')]
            if old and old['job']:
                prior=jobs[old['job']]
                if prior['status']!='failed' or len(previous)>=3 or time.time()-prior['created']<60:continue
            if not any((not j['spec']['hosts'] or n in j['spec']['hosts']) and all(node['labels'].get(k)==v for k,v in j['spec']['labels'].items()) for j in consumers):continue
            if not any(g['enabled'] and g['uuid'] not in node['policy'].get('disabled_gpu_uuids',[]) for g in node['gpus']):continue
            # Only registered, enabled model locations may participate. Verify
            # each source's bytes in the worker; failed sources get alternatives.
            known=dict(c['sources'])
            known.update({k:r['path'] for k,r in locations.items() if r['state']=='ready'})
            sources=[dict(node=k,target=target(nodes[k]),root=p)
                     for k,p in known.items() if nodes[k]['enabled'] and k!=n]
            sources.sort(key=lambda s:(locations.get(s['node'],{}).get('state')!='ready',s['node']))
            control=nodes[c['coordinator']]
            if not control['enabled']:continue
            key='RESOURCE_'+digest(dict(resource=c['id'],node=n,retry=len(previous)))[:24]
            from . import resource_worker
            config=dict(resource=c['id'],manifest_file=c['manifest_file'],manifest_sha256=c['manifest_sha256'],node=n,
                        target=target(node),root=root,sources=sources,marker=marker(c['id'],c['manifest_sha256']))
            experiment=experiment_spec(dict(id=key,name='자원 동기화 '+c['id']+' / '+n,project='resource-preparation',rq='Locate verified bytes and replicate only missing files',priority=25000,jobs=[dict(id=key,name='자원 전송·검증 '+n,kind='prepare',hosts=[control['id']],cwd=control['work_root'],argv=[control['python'],'-c',Path(resource_worker.__file__).read_text()],env={'PYTHONPATH':str(Path(__file__).resolve().parents[1])},resources=dict(gpu_count=0,cpu=1,ram_mib=512),outputs=['RESOURCE_READY.json'],config=config,max_attempts=1)]))
            with store.db:
                store.db.execute('INSERT INTO experiments VALUES(?,?)',(key,dumps(experiment)))
                store.db.execute('INSERT INTO jobs(id,experiment,spec,status,created) VALUES(?,?,?,?,?)',(key,key,dumps(experiment['jobs'][0]),'queued',time.time()))
                store.db.execute('INSERT OR REPLACE INTO resource_locations(resource,node,path,state,job) VALUES(?,?,?,?,?)',(c['id'],n,root,'queued',key))
                store.event('resource_sync_requested',key,dict(resource=c['id'],destination=n,sources=[s['node'] for s in sources]))
            active+=1

def status(store):
    exists=store.db.execute("SELECT 1 FROM sqlite_master WHERE name='resource_catalog'").fetchone()
    if not exists:return dict(resources=[],locations=[])
    transfers=[]
    if store.db.execute("SELECT 1 FROM sqlite_master WHERE name='artifact_transfers'").fetchone():
        # Expose existing checkpoint location receipts without decoding every
        # historical launch command or checkpoint file dictionary.
        transfers=[dict(r) for r in store.db.execute("SELECT attempt,node,direction,status,created,json_extract(report,'$.artifact.root') AS local_path,json_extract(report,'$.artifact.revision') AS revision FROM artifact_transfers ORDER BY created DESC")]
    return dict(resources=[json.loads(r['spec']) for r in store.db.execute('SELECT * FROM resource_catalog')],
                locations=[dict(r,receipt=json.loads(r['receipt'])) for r in store.db.execute('SELECT * FROM resource_locations')],checkpoint_transfers=transfers)
