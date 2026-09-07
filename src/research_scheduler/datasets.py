"""Demand-driven per-node dataset preparation using ordinary CPU scheduler jobs."""
import hashlib
import json
import time
from pathlib import Path, PurePosixPath

from . import dataset_worker
from .schema import absolute, check, fields, identifier, experiment_spec, number
from .store import ACTIVE, dumps


def specification(raw):
    value=json.loads(dumps(raw))
    fields(value,'id version identity_file identity_sha256 asset_name metadata_files replicas project max_parallel_prepares')
    identifier(value['id']);identifier(value['version'])
    value.setdefault('project','dataset-preparation');identifier(value['project'])
    value.setdefault('asset_name','');value.setdefault('metadata_files',[])
    value.setdefault('max_parallel_prepares',2)
    number(value['max_parallel_prepares'],'max_parallel_prepares',1,True)
    if value['asset_name']:identifier(value['asset_name'])
    check(isinstance(value['identity_sha256'],str) and len(value['identity_sha256'])==64
          and all(c in '0123456789abcdef' for c in value['identity_sha256']),'invalid identity SHA256')
    check(isinstance(value['metadata_files'],list),'metadata_files must be a list')
    for name in [value['identity_file'],*value['metadata_files']]:
        check(isinstance(name,str) and name and not PurePosixPath(name).is_absolute()
              and '..' not in PurePosixPath(name).parts,'metadata paths must be relative')
    check(isinstance(value['replicas'],dict) and value['replicas'],'replicas must map registered nodes to recipes')
    for node,recipe in value['replicas'].items():
        identifier(node);fields(recipe,'path cwd prepare_argv verify_argv env resources input_files max_attempts')
        absolute(recipe['path']);absolute(recipe['cwd'])
        check(recipe['path'] not in ('/','/home','/tmp'),'dedicated dataset path required')
        for k in ['prepare_argv','verify_argv']:
            check(isinstance(recipe[k],list) and recipe[k] and all(isinstance(a,str) and '\x00' not in a for a in recipe[k]),'invalid '+k)
        recipe.setdefault('env',{});recipe.setdefault('resources',dict(cpu=2,ram_mib=2048))
        recipe.setdefault('input_files',[]);recipe.setdefault('max_attempts',2)
        sample=experiment_spec(dict(id='check',name='check',rq='check',jobs=[dict(id='check',name='check',kind='prepare',
            cwd=recipe['cwd'],argv=recipe['prepare_argv'],env=recipe['env'],resources=recipe['resources'],
            input_files=recipe['input_files'],max_attempts=recipe['max_attempts'])]))['jobs'][0]
        check(sample['resources']['gpu_count']==0,'dataset preparation must not reserve GPUs')
        recipe['resources']=sample['resources']
    return value


def register(store,raw):
    value=specification(raw)
    with store.lock(),store.db:
        check(set(value['replicas'])<=set(store.specs('nodes')),'unknown replica node')
        old=store.db.execute('SELECT spec FROM dataset_catalog WHERE id=?',(value['id'],)).fetchone()
        if old:
            check(json.loads(old[0])==value,'dataset catalog already exists; use a new dataset ID for changed contracts')
            return value
        store.db.execute('INSERT INTO dataset_catalog VALUES(?,?)',(value['id'],dumps(value)))
        store.event('dataset_catalog_registered',value['id'],value)
    return value


def tick(controller,execute=False):
    """Controller holds the registry lock; jobs launch through normal health admission."""
    store=controller.store
    catalogs={r['id']:json.loads(r['spec']) for r in store.db.execute('SELECT * FROM dataset_catalog')}
    if not catalogs:return
    jobs={j['id']:j for j in store.jobs()};nodes=store.specs('nodes')
    successful={a['job']:a for a in store.attempts() if a['status']=='succeeded'}
    for row in store.db.execute("SELECT * FROM dataset_preparations WHERE state!='ready'").fetchall():
        j=jobs[row['job']];state=j['status']
        if state=='succeeded':
            a=successful[j['id']]
            try:
                report=controller.transport.call(a['spec']['node_spec'],'dataset_receipt',a['spec'])
                receipt=report['dataset_receipt'];c=catalogs[row['dataset']];recipe=c['replicas'][row['node']]
                check(receipt['status']=='complete' and receipt['dataset']==c['id'] and receipt['version']==c['version']
                      and receipt['path']==recipe['path'] and receipt['identity_sha256']==c['identity_sha256'],
                      'dataset receipt identity mismatch')
                n=nodes[row['node']]
                n['datasets'][c['id']]=receipt['path']
                if c['asset_name']:
                    n['assets'][c['asset_name']]=dict(path=str(Path(receipt['path'])/c['identity_file']),sha256=c['identity_sha256'])
                with store.db:
                    store.db.execute('UPDATE nodes SET spec=? WHERE id=?',(dumps(n),n['id']))
                    store.db.execute('DELETE FROM snapshots WHERE node=?',(n['id'],))
                    store.db.execute("UPDATE dataset_preparations SET state='ready',receipt=? WHERE dataset=? AND node=?",
                                     (dumps(receipt),c['id'],n['id']))
                    store.event('dataset_replica_ready',c['id'],dict(node=n['id'],receipt=receipt))
            except Exception as exc:
                with store.db:
                    store.event('dataset_receipt_check_failed',row['job'],dict(error=type(exc).__name__))
            continue
        with store.db:
            store.db.execute('UPDATE dataset_preparations SET state=? WHERE dataset=? AND node=?',(state,row['dataset'],row['node']))
    if not execute:return
    records={(r['dataset'],r['node']):dict(r) for r in store.db.execute('SELECT * FROM dataset_preparations')}
    for c in catalogs.values():
        count=sum(jobs[r['job']]['status'] in (*ACTIVE,'queued') for r in records.values() if r['dataset']==c['id'])
        for node_id,recipe in c['replicas'].items():
            if count>=c['max_parallel_prepares']:break
            n=nodes[node_id]
            if not n['enabled'] or (c['id'],node_id) in records:continue
            consumers=[j for j in jobs.values() if j['status']=='queued' and j['spec'].get('dataset')==c['id']
                       and (not j['spec']['hosts'] or node_id in j['spec']['hosts'])
                       and all(n['labels'].get(k)==v for k,v in j['spec']['labels'].items())]
            if not consumers:continue
            # Already configured replicas stay under ordinary snapshot/asset validation.
            asset=n['assets'].get(c['asset_name'],{}) if c['asset_name'] else {}
            if n['datasets'].get(c['id'])==recipe['path'] and (not c['asset_name'] or asset.get('sha256')==c['identity_sha256']):continue
            suffix=hashlib.sha256(dumps(dict(catalog=c,node=node_id)).encode()).hexdigest()[:20]
            key='DATASET_'+node_id+'_'+suffix
            spec=experiment_spec(dict(id=key,project=c['project'],name='데이터 준비: '+c['id']+' / '+node_id,
                rq='검증된 데이터 복제본을 준비하고 후속 GPU 작업을 허용',priority=50000,jobs=[dict(
                    id=key,name='데이터 준비·검증 '+c['id'],kind='prepare',hosts=[node_id],
                    argv=[n['python'],'-c',Path(dataset_worker.__file__).read_text()],cwd=recipe['cwd'],
                    env=recipe['env'],resources=recipe['resources'],input_files=recipe['input_files'],
                    dataset_path=recipe['path'],config=dict(contract={k:v for k,v in c.items() if k!='replicas'},recipe=recipe),
                    outputs=['DATASET_READY.json'],max_attempts=recipe['max_attempts'])]))
            with store.db:
                store.db.execute('INSERT INTO experiments VALUES(?,?)',(key,dumps(spec)))
                store.db.execute('INSERT INTO jobs(id,experiment,spec,status,created) VALUES(?,?,?,?,?)',
                                 (key,key,dumps(spec['jobs'][0]),'queued',time.time()))
                store.db.execute('INSERT INTO dataset_preparations(dataset,node,job,state) VALUES(?,?,?,?)',
                                 (c['id'],node_id,key,'queued'))
                store.event('dataset_preparation_registered',key,dict(dataset=c['id'],node=node_id))
            count+=1


def status(store):
    return dict(catalogs={r['id']:json.loads(r['spec']) for r in store.db.execute('SELECT * FROM dataset_catalog')},
                preparations=[dict(r,receipt=json.loads(r['receipt'])) for r in store.db.execute('SELECT * FROM dataset_preparations')])
