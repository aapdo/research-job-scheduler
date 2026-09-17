"""Demand-driven, verified execution preparation and future-only host admission."""
import copy
import hashlib
import json
import time
from pathlib import Path

from .schema import check, experiment_spec, node_spec, identifier
from .store import ACTIVE, dumps


def tables(store):
    store.db.execute('CREATE TABLE IF NOT EXISTS execution_catalog(id TEXT PRIMARY KEY,spec TEXT NOT NULL)')
    store.db.execute('CREATE TABLE IF NOT EXISTS execution_preparations(profile TEXT NOT NULL,node TEXT NOT NULL,job TEXT NOT NULL,state TEXT NOT NULL,receipt TEXT NOT NULL DEFAULT \'{}\',PRIMARY KEY(profile,node))')
    store.db.execute('CREATE TABLE IF NOT EXISTS execution_resource_budgets(profile TEXT NOT NULL,node TEXT NOT NULL,kind TEXT NOT NULL,spec TEXT NOT NULL,PRIMARY KEY(profile,node,kind))')


def budgeted_recipe(recipe, budgets):
    """Audited admission estimates are separate from immutable runtime receipts."""
    result=copy.deepcopy(recipe)
    for kind,item in budgets.items():
        check(kind in recipe['resources'],'resource budget kind missing')
        original=recipe['resources'][kind]
        check(item.get('evidence') and item.get('source_sha256')==recipe['asset']['sha256'],
              'resource budget evidence/source mismatch')
        dimensions=[d for d in ('ram_mib','vram_mib') if d in item]
        check(dimensions and original['gpu_count']==1,'resource budget original allocation mismatch')
        for dimension in dimensions:
            value=item[dimension]
            check(item.get('original_'+dimension)==original[dimension],'resource budget original allocation mismatch')
            check(type(value) is int and 0<value<=original[dimension],'invalid audited resource estimate')
            result['resources'][kind][dimension]=value
        if kind=='train' and 'vram_mib' in item and 'validation' in result:
            check(result['validation']['resources']['vram_mib']==original['vram_mib'],'validation VRAM differs from training profile')
            result['validation']['resources']['vram_mib']=item['vram_mib']
    return result


def digest(value):
    return hashlib.sha256(dumps(value).encode()).hexdigest()


def specification(raw):
    value=copy.deepcopy(raw)
    identifier(value['id']);identifier(value['coordinator'])
    check(set(value)=={'id','coordinator','match','targets','max_parallel'},'invalid execution catalog fields')
    check({'cwd','dataset'}<=set(value['match']) and set(value['match'])<= {'cwd','dataset','entry','files','config_modes','inline_validation_sha256'},'execution match must bind source and dataset')
    if 'inline_validation_sha256' in value['match']:
        sha=value['match']['inline_validation_sha256']
        check(isinstance(sha,str) and len(sha)==64 and all(c in '0123456789abcdef' for c in sha),'invalid inline validation SHA')
    if 'config_modes' in value['match']:
        check(isinstance(value['match']['config_modes'],list) and value['match']['config_modes']
              and all(isinstance(m,str) and m for m in value['match']['config_modes']),'invalid capability modes')
    check(Path(value['match']['cwd']).is_absolute(),'source cwd must be absolute')
    check(isinstance(value['max_parallel'],int) and 1<=value['max_parallel']<=4,'invalid preparation concurrency')
    check(isinstance(value['targets'],dict) and value['targets'],'targets required')
    for key,recipe in value['targets'].items():
        identifier(key)
        required={'target','copies','verify_argv','execution','resources','dataset_path','asset'}
        check(required<=set(recipe) and set(recipe)<=required|{'validation'},'invalid execution recipe fields')
        check(isinstance(recipe['verify_argv'],list) and recipe['verify_argv'] and all(isinstance(x,str) for x in recipe['verify_argv']),'verifier required')
        check(Path(recipe['dataset_path']).is_absolute(),'dataset path must be absolute')
        override=recipe['execution']
        check(set(override)<= {'cwd','argv','env','config','input_files'},'unsafe execution override')
        check(Path(override['cwd']).is_absolute(),'execution cwd required')
        check(set(recipe['resources'])<= {'train','eval'},'only model train/eval profiles supported')
        for kind,resources in recipe['resources'].items():
            normalized=experiment_spec(dict(id='profile_check',name='profile check',rq='execution compatibility',jobs=[dict(id='profile_check',name='profile check',kind=kind,cwd=override['cwd'],argv=override.get('argv',['true']),resources=resources)]))
            recipe['resources'][kind]=normalized['jobs'][0]['resources']
        for item in recipe['copies']:
            check(set(item)=={'source_target','source_root','destination_root','manifest_file','manifest_sha256'},'invalid copy recipe')
            for name in ('source_root','destination_root','manifest_file'):
                path=Path(item[name]);check(path.is_absolute() and len(path.parts)>=4,'copy path must be dedicated')
            check(len(item['manifest_sha256'])==64,'manifest SHA required')
        check(set(recipe['asset'])=={'path','sha256'} and Path(recipe['asset']['path']).is_absolute(),'frozen source marker required')
        if 'validation' in recipe:
            validation=recipe['validation']
            check(set(validation)=={'argv','cwd','env','resources','config','outputs'},'invalid validation job')
            experiment_spec(dict(id='verification_check',name='verification check',rq='compatibility probe',jobs=[dict(id='verification_check',name='verification check',kind='prepare',**copy.deepcopy(validation))]))
    return value


def register(store, raw):
    value=specification(raw)
    with store.lock(),store.db:
        tables(store);nodes=store.specs('nodes')
        check(value['coordinator'] in nodes and nodes[value['coordinator']]['transport']=='local','preparation coordinator must be local')
        check(set(value['targets'])<=set(nodes),'unknown preparation target')
        old=store.db.execute('SELECT spec FROM execution_catalog WHERE id=?',(value['id'],)).fetchone()
        check(not old or json.loads(old['spec'])==value,'catalog revision is immutable; register a new ID')
        if not old:
            store.db.execute('INSERT INTO execution_catalog VALUES(?,?)',(value['id'],dumps(value)))
            store.event('execution_catalog_registered',value['id'],{'targets':list(value['targets'])})
    return value


def for_node(spec, node_id, *, readonly=False):
    """Resolve only a scheduler-admitted host profile; do not alter the stored job."""
    profile=spec.get('metadata',{}).get('execution_profiles',{}).get(node_id)
    if not profile:
        return spec
    check(set(profile)<= {'cwd','argv','input_files','assets','env','config','catalog','resource_contract'},'invalid admitted execution profile')
    result=dict(spec,env=dict(spec['env']),config=dict(spec['config'])) if readonly else copy.deepcopy(spec)
    for key in ('cwd','argv','input_files','assets'):
        if key in profile:result[key]=profile[key] if readonly else copy.deepcopy(profile[key])
    result['env'].update(profile.get('env',{}))
    result['config'].update(profile.get('config',{}) if readonly else copy.deepcopy(profile.get('config',{})))
    return result


def matches(job, catalog):
    spec=job['spec']
    required=catalog['match'].get('inline_validation_sha256')
    if required:
        inline=spec.get('metadata',{}).get('inline_validation',{})
        if inline.get('sha256')!=required or hashlib.sha256(inline.get('code','').encode()).hexdigest()!=required:
            return False
    if 'config_modes' in catalog['match'] and spec.get('config',{}).get('mode') not in catalog['match']['config_modes']:
        return False
    approved=catalog['match'].get('files')
    if approved is not None and any(approved.get(f['path'])!=f['sha256'] for f in spec['input_files'] if f['path'].startswith(spec['cwd']+'/')):
        return False
    return (job['status']=='queued' and spec['kind'] in ('train','eval') and
            spec['cwd']==catalog['match']['cwd'] and spec.get('dataset')==catalog['match']['dataset'] and
            (not catalog['match'].get('entry') or catalog['match']['entry'] in spec['argv']) and
            not spec.get('metadata',{}).get('placement_locked',False))


def admit(spec, node_id, catalog, recipe, *, reuse_unchanged=False):
    """Keep all original candidate hosts and scientific fields; add one alternative."""
    if node_id in spec.get('metadata', {}).get('excluded_hosts', []):
        return copy.deepcopy(spec)
    kind=spec['kind'];resource=recipe['resources'][kind]
    metadata=spec.get('metadata',{})
    overlay=copy.deepcopy(recipe['execution'])
    inline=metadata.get('inline_validation')
    if inline and 'argv' in overlay:
        from .inline_validation import wrap_argv
        overlay['argv']=wrap_argv(overlay['argv'],inline['code'])
    # Preserve arbitrary scientific config; only node input-profile identities may differ.
    cfg=overlay.get('config',{})
    check(set(cfg)<= {'input_profiles'},'profile must not modify scientific configuration')
    if cfg:
        inputs=dict(spec['config'].get('input_profiles',{}));inputs.update(cfg['input_profiles'])
        overlay['config']={'input_profiles':inputs}
    asset='execution-'+catalog['id']+'-'+node_id
    overlay['assets']={asset:recipe['asset']['sha256']}
    overlay['resource_contract']=copy.deepcopy(resource)
    overlay['catalog']=catalog['id']
    if (reuse_unchanged and metadata.get('execution_profiles',{}).get(node_id)==overlay
            and (not spec['hosts'] or node_id in spec['hosts'])
            and 'execution_original_resources' in metadata
            and resource in [spec['resources'],*spec.get('resource_variants',[])]
            and ('gpu_count_by_host' not in metadata or metadata['gpu_count_by_host'].get(node_id)==resource['gpu_count'])):
        return spec
    result=copy.deepcopy(spec)
    if result['hosts'] and node_id not in result['hosts']:result['hosts'].append(node_id)
    metadata=result.setdefault('metadata',{})
    metadata.setdefault('execution_original_resources',copy.deepcopy([spec['resources'],*spec.get('resource_variants',[])]))
    metadata.setdefault('execution_profiles',{})[node_id]=overlay
    if 'gpu_count_by_host' in metadata:
        metadata['gpu_count_by_host'][node_id]=resource['gpu_count']
    variants=result.setdefault('resource_variants',[])
    if resource not in [result['resources'],*variants]:variants.append(copy.deepcopy(resource))
    return result


def recovered_validation_retry(spec, attempt, node, snapshot, health, now):
    """One retry per verified runtime recovery, at most two per validation job."""
    report=attempt.get('report',{})
    recovery=node.get('labels',{}).get('gpu_runtime_recovery',{})
    used=spec.get('metadata',{}).get('validation_recovery_retries',[])
    stamp=recovery.get('verified_at',0)
    if (attempt.get('status')!='failed' or not attempt.get('released')
            or report.get('failure_class')!='gpu_validation_unavailable'
            or not node.get('enabled') or len(used)>=2
            or stamp<=report.get('finished',float('inf'))
            or stamp in used or recovery.get('boot_id')!=snapshot.get('boot_id')
            or not recovery.get('cuda_verified') or health.get('phase')!='healthy'
            or not 0<=now-snapshot.get('received_at',0)<=60
            or snapshot.get('stable_polls',0)<3 or snapshot.get('gpu_error')
            or snapshot.get('d_state',1) or not snapshot.get('read_ok')):
        return None
    expected={g['uuid'] for g in node['gpus'] if g['enabled'] and g['uuid'] not in node['policy'].get('disabled_gpu_uuids',[])}
    if not expected or not expected.issubset({g['uuid'] for g in snapshot.get('gpus',[])}):return None
    result=copy.deepcopy(spec)
    result.setdefault('metadata',{})['validation_recovery_retries']=[*used,stamp]
    result['max_attempts']+=1
    return result


def refresh_validation_runtime_recovery(store,node,snapshot,health,attempt,now):
    """Bounded CUDA initialization check for failed validation only, once per minute."""
    report=attempt.get('report',{})
    labels=node.get('labels',{})
    if (report.get('failure_class')!='gpu_validation_unavailable'
            or labels.get('gpu_runtime_recovery',{}).get('verified_at',0)>report.get('finished',float('inf'))
            or now-labels.get('validation_cuda_probe_at',0)<60
            or health.get('phase')!='healthy' or snapshot.get('stable_polls',0)<3
            or not 0<=now-snapshot.get('received_at',0)<=60 or snapshot.get('gpu_error')
            or not snapshot.get('gpus') or not snapshot.get('read_ok') or snapshot.get('d_state',1)):
        return
    import subprocess,shlex
    code="import ctypes,json,time;from pathlib import Path;assert ctypes.CDLL('libcuda.so.1').cuInit(0)==0;print(json.dumps(dict(verified_at=time.time(),boot_id=Path('/proc/sys/kernel/random/boot_id').read_text().strip(),cuda_verified=True)))"
    args=[node['python'],'-c',code]
    if node['transport']=='ssh':args=['ssh','-T','-o','BatchMode=yes','-o','ConnectTimeout=3',node['target'],shlex.join(args)]
    labels['validation_cuda_probe_at']=now
    try:
        p=subprocess.run(args,text=True,capture_output=True,timeout=8,check=True)
        recovery=json.loads(p.stdout)
        if recovery['boot_id']==snapshot.get('boot_id') and recovery['cuda_verified']:
            labels['gpu_runtime_recovery']=recovery
            store.event('gpu_runtime_recovery_verified',node['id'],recovery)
    except (OSError,subprocess.SubprocessError,ValueError,KeyError):
        pass
    with store.db:store.db.execute('UPDATE nodes SET spec=? WHERE id=?',(dumps(node),node['id']))


def tick(controller, execute=False):
    """Called under controller lock. Transfers run in detached ordinary prepare jobs."""
    store=controller.store
    # Read-only planning must not register jobs or materialize state.
    if not execute:return
    tables(store)
    catalogs={r['id']:json.loads(r['spec']) for r in store.db.execute('SELECT * FROM execution_catalog')}
    if not catalogs:return
    jobs={j['id']:j for j in store.jobs()};nodes=store.specs('nodes')
    records={(r['profile'],r['node']):dict(r) for r in store.db.execute('SELECT * FROM execution_preparations')}
    budgets={}
    for r in store.db.execute('SELECT * FROM execution_resource_budgets'):
        budgets.setdefault((r['profile'],r['node']),{})[r['kind']]=json.loads(r['spec'])
    attempts={a['job']:a for a in store.attempts(job_ids={r['job'] for r in records.values()}) if a['status']=='succeeded'}
    for catalog in catalogs.values():
        active=sum(jobs[r['job']]['status'] in (*ACTIVE,'queued') for (key,_),r in records.items() if key==catalog['id'])
        for target,recipe in catalog['targets'].items():
            node=nodes[target]
            if node.get('target')!=recipe['target']:continue
            if not node['enabled'] or not any(g['enabled'] and g['uuid'] not in node['policy'].get('disabled_gpu_uuids',[]) for g in node['gpus']):continue
            consumers=[j for j in jobs.values() if matches(j,catalog) and j['spec']['kind'] in recipe['resources']
                       and target not in j['spec'].get('metadata',{}).get('excluded_hosts',[])]
            limit=node.get('labels',{}).get('max_gpus_per_job')
            if limit is not None:
                consumers=[j for j in consumers if recipe['resources'][j['spec']['kind']]['gpu_count']<=limit]
            key=(catalog['id'],target);record=records.get(key)
            if not consumers:
                # Finish already successful preparation even if the last consumer
                # started elsewhere. Do not create new work without demand.
                if record and record['state']=='ready':continue
                if not record or jobs[record['job']]['status']!='succeeded':continue
                if 'validation' in recipe:
                    completed_id='EXEC_VERIFY_'+digest(dict(profile=catalog['id'],node=target,recipe=recipe))[:24]
                    if jobs.get(completed_id,{}).get('status')!='succeeded':continue
            if not record:
                if active>=catalog['max_parallel']:continue
                coordinator=nodes[catalog['coordinator']]
                if not coordinator['enabled']:continue
                job_id='EXEC_PREP_'+digest(dict(profile=catalog['id'],node=target,recipe=recipe))[:24]
                from . import execution_worker
                config=dict(profile=catalog['id'],node=target,recipe=recipe,recipe_sha256=digest(recipe))
                prepared=experiment_spec(dict(id=job_id,name='실행 환경 준비: '+target,project='execution-preparation',rq='검증된 실행본·공통 입력 준비; 작업 배정은 스케줄러 소유',priority=20000,jobs=[dict(id=job_id,name='실행본·입력 준비 '+target,kind='prepare',hosts=[coordinator['id']],cwd=coordinator['work_root'],argv=[coordinator['python'],'-c',Path(execution_worker.__file__).read_text()],resources=dict(gpu_count=0,cpu=1,ram_mib=1024),config=config,outputs=['EXECUTION_READY.json'],max_attempts=2)]))
                with store.db:
                    store.db.execute('INSERT INTO experiments VALUES(?,?)',(job_id,dumps(prepared)))
                    store.db.execute('INSERT INTO jobs(id,experiment,spec,status,created) VALUES(?,?,?,?,?)',(job_id,job_id,dumps(prepared['jobs'][0]),'queued',time.time()))
                    store.db.execute('INSERT INTO execution_preparations(profile,node,job,state) VALUES(?,?,?,?)',(catalog['id'],target,job_id,'queued'))
                    store.event('execution_preparation_registered',job_id,dict(profile=catalog['id'],node=target))
                active+=1;continue
            state=jobs[record['job']]['status']
            if state!='succeeded':
                with store.db:store.db.execute('UPDATE execution_preparations SET state=? WHERE profile=? AND node=?',(state,*key))
                continue
            try:
                attempt=attempts[record['job']]
                check(attempt['spec']['node_spec']['transport']=='local','receipt must come from local coordinator')
                path=Path(attempt['spec']['attempt_dir'])/'EXECUTION_READY.json'
                output=next(v for v in attempt['report']['outputs'].values() if v['path']==str(path))
                check(hashlib.sha256(path.read_bytes()).hexdigest()==output['sha256'],'preparation receipt changed')
                receipt=json.loads(path.read_text())
                check(receipt['status']=='complete' and receipt['node']==target and receipt['profile']==catalog['id'] and receipt['recipe_sha256']==digest(recipe),'preparation receipt identity mismatch')
                if 'validation' in recipe:
                    validation_id='EXEC_VERIFY_'+digest(dict(profile=catalog['id'],node=target,recipe=recipe))[:24]
                    validation_job=jobs.get(validation_id)
                    if validation_job is None:
                        validation=copy.deepcopy(budgeted_recipe(recipe,budgets.get(key,{}))['validation'])
                        spec=experiment_spec(dict(id=validation_id,name='실행 호환성 검증: '+target,project='execution-preparation',rq='스케줄러가 허용 GPU로 검증 후 자동 후보 등록',priority=20000,jobs=[dict(id=validation_id,name='실행 호환성 검증 '+target,kind='prepare',hosts=[target],depends_on=[record['job']],order_only_dependencies=[record['job']],dataset_path=recipe['dataset_path'],input_files=[recipe['asset']],max_attempts=1,**validation)]))
                        with store.db:
                            store.db.execute('INSERT INTO experiments VALUES(?,?)',(validation_id,dumps(spec)))
                            store.db.execute('INSERT INTO jobs(id,experiment,spec,status,created) VALUES(?,?,?,?,?)',(validation_id,validation_id,dumps(spec['jobs'][0]),'queued',time.time()))
                            store.db.execute('UPDATE execution_preparations SET state=\'validating\' WHERE profile=? AND node=?',key)
                            store.event('execution_validation_registered',validation_id,dict(profile=catalog['id'],node=target))
                        continue
                    if validation_job['status']!='succeeded':
                        if validation_job['status']=='failed':
                            latest=store.db.execute('SELECT * FROM attempts WHERE job=? ORDER BY created DESC LIMIT 1',(validation_id,)).fetchone()
                            if latest:
                                attempt=dict(latest);attempt['report']=json.loads(attempt['report'])
                                if len(validation_job['spec'].get('metadata',{}).get('validation_recovery_retries',[]))<2:
                                    refresh_validation_runtime_recovery(store,node,controller.snapshots().get(target,{}),controller.node_health().get(target,{}),attempt,time.time())
                                retry=recovered_validation_retry(validation_job['spec'],attempt,node,
                                    controller.snapshots().get(target,{}),controller.node_health().get(target,{}),time.time())
                                if retry:
                                    count=store.db.execute('SELECT COUNT(*) FROM attempts WHERE job=?',(validation_id,)).fetchone()[0]
                                    retry['max_attempts']=max(retry['max_attempts'],count+1)
                                    experiment_spec(dict(id='validation-retry-check',name='validation-retry-check',rq='verified runtime recovery',jobs=[retry]))
                                    with store.db:
                                        store.db.execute("UPDATE jobs SET spec=?,status='queued',reason='' WHERE id=? AND status='failed'",(dumps(retry),validation_id))
                                        store.event('execution_validation_recovery_retry',validation_id,dict(node=target,previous_attempt=attempt['id'],recovery=node['labels']['gpu_runtime_recovery']))
                                    validation_job.update(status='queued',spec=retry)
                        with store.db:store.db.execute('UPDATE execution_preparations SET state=? WHERE profile=? AND node=?',('validation_'+validation_job['status'],*key))
                        continue
                asset='execution-'+catalog['id']+'-'+target
                updated=copy.deepcopy(node);updated['assets'][asset]=recipe['asset'];updated['datasets'][catalog['match']['dataset']]=recipe['dataset_path'];node_spec(updated)
                changes=[]
                admission_recipe=budgeted_recipe(recipe,budgets.get(key,{}))
                for consumer in consumers:
                    after=admit(consumer['spec'],target,catalog,admission_recipe,reuse_unchanged=True)
                    if after!=consumer['spec']:
                        experiment_spec(dict(id='admit_check',name='admit check',rq='verified alternative host',jobs=[copy.deepcopy(after)]))
                        changes.append((consumer,after))
                with store.db:
                    if updated!=node:
                        store.db.execute('UPDATE nodes SET spec=? WHERE id=?',(dumps(updated),target));nodes[target]=updated
                        store.db.execute('DELETE FROM snapshots WHERE node=?',(target,))
                    for consumer,after in changes:
                        store.db.execute('UPDATE jobs SET spec=? WHERE id=? AND status=\'queued\'',(dumps(after),consumer['id']))
                        consumer['spec']=after
                    store.db.execute('UPDATE execution_preparations SET state=\'ready\',receipt=? WHERE profile=? AND node=?',(dumps(receipt),*key))
                    if changes:store.event('execution_hosts_admitted',catalog['id'],dict(node=target,jobs=[j['id'] for j,_ in changes],existing_hosts_preserved=True))
            except Exception as exc:
                with store.db:
                    store.db.execute('UPDATE execution_preparations SET state=\'verification_failed\',receipt=? WHERE profile=? AND node=?',(dumps({'error':type(exc).__name__}),*key))
                    if record['state']!='verification_failed':store.event('execution_preparation_rejected',record['job'],dict(error=type(exc).__name__))
