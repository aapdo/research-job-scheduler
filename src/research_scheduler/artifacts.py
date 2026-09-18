"""Bounded detached HF publication and destination staging; no GPU transfer lease."""
import copy
import hashlib
import json
import os
import re
import time
import uuid
from pathlib import Path, PurePosixPath

from . import agent, hf_worker, relay_worker
from .schema import check, fields
from .store import ACTIVE, dumps
from .draining import source_upload_allowed


def hf_spec(raw):
    h = dict(raw)
    fields(h, 'repo_id repo_type revision path_prefix archive_payload commit_interval_s enabled')
    if 'enabled' in h:
        check(type(h['enabled']) is bool, 'HF enabled must be boolean')
    else:
        default=os.environ.get('RS_HF_UPLOAD_DEFAULT','1')
        check(default in ('0','1'), 'RS_HF_UPLOAD_DEFAULT must be 0 or 1')
        h['enabled']=default=='1'
    if 'archive_payload' in h:
        check(type(h['archive_payload']) is bool, 'archive_payload must be boolean')
    if 'commit_interval_s' in h:
        check(type(h['commit_interval_s']) is int and 0 <= h['commit_interval_s'] <= 3600, 'invalid commit interval')
    check(bool(re.fullmatch(r'[A-Za-z0-9_.-]+/[A-Za-z0-9_.-]+', h.get('repo_id', ''))),
          'HF repo_id must be namespace/repository')
    h.setdefault('repo_type', 'model')
    h.setdefault('revision', 'main')
    h.setdefault('path_prefix', 'scheduler-artifacts')
    check(h['repo_type'] in ('model', 'dataset'), 'HF repo_type must be model or dataset')
    check(isinstance(h['revision'], str) and bool(h['revision']) and not any(c.isspace() for c in h['revision']),
          'invalid HF revision')
    check(isinstance(h['path_prefix'], str), 'invalid HF path_prefix')
    p = PurePosixPath(h['path_prefix'])
    check(not p.is_absolute() and '..' not in p.parts
          and '\\' not in h['path_prefix'], 'invalid HF path_prefix')
    return h


def rows(store):
    return [dict(r, spec=json.loads(r['spec']), report=json.loads(r['report']))
            for r in store.db.execute('SELECT * FROM artifact_transfers ORDER BY created')]


def reservations(store):
    # Completed transfers cannot reserve resources. Avoid decoding their large
    # frozen requests merely to discard them on every read-only dashboard plan.
    states=sorted(ACTIVE)
    query='SELECT * FROM artifact_transfers WHERE status IN ('+','.join('?' for _ in states)+') ORDER BY created'
    return [dict(r, spec=json.loads(r['spec']), report=json.loads(r['report']), released=False)
            for r in store.db.execute(query, states)]


def campaign_for(experiment, campaigns):
    matches = [c for c in campaigns.values() if c.get('hf',{}).get('enabled') is True and c['enabled'] and not c['external']
               and (experiment['id'] in c['experiments'] or experiment['project'] in c['projects'])]
    # A focused upload monitor may overlap its parent scientific campaign. Both
    # observe the same receipt; only one deterministic owner creates the upload.
    check(len({dumps(c['hf']) for c in matches}) <= 1,
          'experiment selected by HF campaigns with different destinations')
    return min(matches, key=lambda c: c['id']) if matches else None


def retry_history(history):
    """A new operator repair revision grants a separate bounded retry series."""
    if not history:
        return history
    revision = history[-1]['spec'].get('config', {}).get('repair_revision')
    return [t for t in history if t['spec'].get('config', {}).get('repair_revision') == revision] if revision else history


def retry_possible(history, now=None, check_time=True):
    history = retry_history(history)
    if not history:
        return True
    if any(t['status'] != 'failed' for t in history):
        return False
    rate_limited = [t for t in history if t['report'].get('rate_limit')]
    if len(history)-len(rate_limited) >= 3 or len(rate_limited) >= 4:
        return False
    now = time.time() if now is None else now
    deadline = max([t['report'].get('rate_limit', {}).get('retry_at', 0) for t in history]+[0])
    return not check_time or (now >= deadline and (not history or now-history[-1]['created'] > 60))


def upload_pause(config, transfers, campaigns, now=None):
    """One upload per repository, with a durable gap shared across branches/nodes."""
    now = time.time() if now is None else now
    hf = config.get('hf', {})
    identity = (hf.get('repo_id'), hf.get('repo_type', 'model'))
    policies = [c.get('hf') or {} for c in campaigns.values()]
    interval = max([hf.get('commit_interval_s', 0)] + [h.get('commit_interval_s', 0) for h in policies
        if (h.get('repo_id'), h.get('repo_type', 'model')) == identity])
    configured = os.environ.get('RS_HF_UPLOAD_INTERVAL_S')
    if configured is not None:
        interval = int(configured)
        check(1 <= interval <= 3600, 'RS_HF_UPLOAD_INTERVAL_S must be 1..3600')
    if not interval:
        return False
    history = [t for t in transfers if t['direction'] == 'upload'
        and (t['spec']['config'].get('hf', {}).get('repo_id'),
             t['spec']['config'].get('hf', {}).get('repo_type', 'model')) == identity]
    if any(t['status'] in ACTIVE for t in history):
        return True
    if any(t['report'].get('rate_limit', {}).get('retry_at', 0) > now for t in history):
        return True
    last = max((max(t['created'], t['report'].get('finished', 0)) for t in history), default=0)
    return now-last < interval


def start(controller, attempt, node, direction, config):
    store = controller.store
    key = 'hf-' + uuid.uuid4().hex
    directory = str(Path(node['work_root'], 'artifact-transfers', key))
    config = dict(config, direction=direction, token_file=node['hf'].get('token_file', ''))
    if direction == 'download':
        config['destination'] = directory + '/payload'
    request = dict(id=key, job=attempt['job'], node_spec=node, attempt_dir=directory,
                   argv=[node['hf']['python'], '-c', Path(hf_worker.__file__).read_text()],
                   cwd=directory,
                   env={'HF_HUB_CACHE': str(Path(directory, 'hub-cache')),
                        'HF_XET_CACHE': str(Path(directory, 'xet-cache'))},
                   config=config, input_files=[], outputs=['HF_RECEIPT.json'],
                   resources=dict(gpu_count=0, vram_mib=0, gpu_mode='exclusive', cpu=2, ram_mib=1024),
                   startup_group=node['startup_group'], gpus=[])
    request['runner_sha256'] = hashlib.sha256(Path(agent.__file__).read_bytes()).hexdigest()
    request['spec_sha256'] = hashlib.sha256(dumps(request).encode()).hexdigest()
    with store.db:
        store.db.execute('INSERT INTO artifact_transfers VALUES(?,?,?,?,?,?,?,?)',
                         (key, attempt['id'], node['id'], direction, 'starting', dumps(request), '{}', time.time()))
        store.event('artifact_transfer_reserved', key, dict(attempt=attempt['id'], node=node['id'], direction=direction))
    try:
        controller.transport.call(node, 'launch', request)
    except Exception:
        pass  # Lost ACK: reconcile the same immutable transfer, never duplicate it.
    return key


def start_local_relay(controller, attempt, source_node, destination_node):
    """Stage declared dependency outputs/artifacts through a bounded local relay."""
    store = controller.store
    relay_node = store.specs('nodes').get('resource-control')
    check(relay_node is not None and relay_node.get('transport') == 'local',
          'resource-control local relay node is not configured')
    report = attempt.get('report', {})
    outputs = report.get('outputs', {})
    artifacts = report.get('dependency_artifacts')
    job_spec = attempt.get('spec', {}).get('job_spec', {})
    declared = job_spec.get('dependency_artifacts', job_spec.get('hf_artifacts', []))
    if not artifacts:
        published = report.get('hf_artifact', {})
        artifacts = published.get('files') if published.get('attempt') == attempt['id'] else None
    # A legacy successful attempt may predate dependency_artifacts collection.
    # Never turn its small success-output receipt into a supposedly complete
    # checkpoint relay when the immutable job declared additional artifacts.
    check(not declared or artifacts,
          'dependency artifact manifest missing; repair the legacy attempt before relay')
    artifacts = artifacts or outputs
    check(bool(outputs), 'dependency has no declared successful outputs to relay')
    check(set(outputs).issubset(artifacts), 'dependency artifact receipt omits successful outputs')
    files = {}
    source_root = Path(attempt['spec']['attempt_dir'])
    for name, item in artifacts.items():
        rel = PurePosixPath(name)
        check(not rel.is_absolute() and '..' not in rel.parts and '\\' not in name,
              'unsafe dependency output path')
        files[name] = {'sha256': item['sha256'], 'bytes': item['bytes']}
    manifest_sha = hashlib.sha256(dumps(files).encode()).hexdigest()
    key = 'relay-' + uuid.uuid4().hex
    directory = str(Path(relay_node['work_root'], 'artifact-transfers', key))
    destination = str(Path(destination_node['work_root'], 'artifact-relays', key, 'payload'))
    config = dict(source_attempt=attempt['id'], source_target=(source_node['target']
                  if source_node['transport'] == 'ssh' else '@local'), source_root=str(source_root),
                  destination_target=(destination_node['target']
                  if destination_node['transport'] == 'ssh' else '@local'),
                  destination_root=destination, files=files, manifest_sha256=manifest_sha)
    request = dict(id=key, job=attempt['job'], node_spec=relay_node, attempt_dir=directory,
                   argv=[relay_node['python'], '-c', Path(relay_worker.__file__).read_text()],
                   cwd=directory, env={}, config=config, input_files=[], outputs=['HF_RECEIPT.json'],
                   resources=dict(gpu_count=0, vram_mib=0, gpu_mode='exclusive', cpu=2, ram_mib=1024),
                   startup_group='', gpus=[])
    request['runner_sha256'] = hashlib.sha256(Path(agent.__file__).read_bytes()).hexdigest()
    request['spec_sha256'] = hashlib.sha256(dumps(request).encode()).hexdigest()
    with store.db:
        store.db.execute('INSERT INTO artifact_transfers VALUES(?,?,?,?,?,?,?,?)',
                         (key, attempt['id'], destination_node['id'], 'download', 'starting',
                          dumps(request), '{}', time.time()))
        store.event('artifact_transfer_reserved', key,
                    dict(attempt=attempt['id'], node=destination_node['id'],
                         direction='download', transport='controller-local-relay'))
    try:
        controller.transport.call(relay_node, 'launch', request)
    except Exception:
        pass
    return key


def runnable_stage_demand(queued, jobs, experiments, nodes, snapshots, attempts, successful, groups, now):
    """Rank missing artifacts by GPU work that can run once staging finishes.

    Hypothetical locations are used only here, never for actual admission.
    All non-artifact gates (including explicit holds and VRAM) still apply.
    """
    from .planner import fit
    held = [a for a in attempts if a['status'] in ACTIVE]
    demand = {}
    for j in queued:
        spec = j['spec']
        if not spec['resources']['gpu_count'] or any(jobs[d]['status'] != 'succeeded' for d in spec['depends_on']):
            continue
        deps = [d for d in spec['depends_on'] if d not in spec.get('order_only_dependencies', [])]
        for n in nodes.values():
            if (not n['enabled'] or (spec['hosts'] and n['id'] not in spec['hosts'])
                    or any(n['labels'].get(k) != v for k,v in spec['labels'].items())):
                continue
            missing = [d for d in deps if successful[d]['node'] != n['id']
                       and n['id'] not in successful[d].get('artifact_locations', {})
                       and not (n['storage_domain'] and n['storage_domain'] == successful[d]['spec']['node_spec']['storage_domain'])]
            if not missing:
                continue
            hypothetical = dict(successful)
            for d in missing:
                item = dict(successful[d])
                item['artifact_locations'] = dict(item.get('artifact_locations', {}))
                item['artifact_locations'][n['id']] = {}
                hypothetical[d] = item
            if any(not fit(dict(spec, resources=req), n, snapshots.get(n['id']), held,
                           attempts, hypothetical, groups, now)[0]
                   for req in [spec['resources'], *spec.get('resource_variants', [])]):
                priority = experiments[j['experiment']]['priority'] + spec['priority']
                for d in missing:
                    demand[d] = max(demand.get(d, priority), priority)
    return demand


def tick(controller, execute):
    """Called with scheduler lock. At most two cluster transfers, one per node."""
    from .notifications import campaign_specs, campaign_processing_due
    from .planner import base_health, fit, dependency_priorities
    store = controller.store
    campaigns = {key:value for key,value in campaign_specs(store).items()
                 if campaign_processing_due(store,value)}
    health = controller.node_health()
    for r in rows(store):
        if r['status'] not in ACTIVE or health.get(r['node'], {}).get('phase') in ('ssh_retrying', 'unavailable'):
            continue
        try:
            report = controller.transport.call(r['spec']['node_spec'], 'artifact_status', r['spec'])
        except Exception:
            continue
        state = report.get('status', 'unknown')
        if state not in (*ACTIVE, 'succeeded', 'failed'):
            state = 'unknown'
        if state == 'succeeded' and not report.get('artifact'):
            state = 'unknown'
        with store.db:
            store.db.execute('UPDATE artifact_transfers SET status=?,report=? WHERE id=?',
                             (state, dumps(report), r['id']))
            if state != r['status']:
                store.event('artifact_transfer_' + state, r['id'], dict(attempt=r['attempt'], direction=r['direction']))
    if not execute:
        return
    nodes, snapshots = store.specs('nodes'), controller.snapshots()
    transfers = rows(store)
    live = [r for r in transfers if r['status'] in ACTIVE]
    if len(live) >= 2:
        return
    jobs = {j['id']: j for j in store.jobs()}
    experiments = store.specs('experiments')
    from .model_vram_policy import normalize
    jobs = {k:dict(j,spec=normalize(j['spec'])) for k,j in jobs.items()}
    # Dependency staging is an internal scheduler guarantee, independent of
    # whether a campaign publishes long-term results to HF. This also covers
    # future campaigns that only declare ordinary depends_on outputs.
    ready_queued = [j for j in jobs.values() if j['status'] == 'queued'
                    and all(jobs[d]['status'] == 'succeeded' for d in j['spec']['depends_on'])]
    detailed_jobs = {d for j in ready_queued for d in j['spec']['depends_on']}
    detailed_jobs.update(r[0] for r in store.db.execute(
        "SELECT DISTINCT job FROM attempts WHERE status IN ('starting','running','unknown')"))
    if store.db.execute("SELECT 1 FROM sqlite_master WHERE name='artifact_repair_queue'").fetchone():
        detailed_jobs.update(r[0] for r in store.db.execute(
            "SELECT job FROM attempts WHERE id IN "
            "(SELECT attempt FROM artifact_repair_queue WHERE state='pending')"))
    groups = store.specs('groups_')
    max_interval = max((g.get('min_start_interval_s', 0) for g in groups.values()), default=0)
    if max_interval:
        detailed_jobs.update(r[0] for r in store.db.execute(
            'SELECT DISTINCT job FROM attempts WHERE created>=?', (time.time()-max_interval,)))
    attempts = store.attempts(summary=True, job_ids=detailed_jobs)
    from .planner import admission_vram
    attempts = [dict(a, admission_vram_mib=admission_vram(a, jobs.get(a.get('job'),{}), time.time()))
                if a['status'] in ACTIVE else a for a in attempts]

    pause_cache = {}
    def eligible(a, n, direction, repair_revision=None, require_hf=True):
        if direction == 'upload':
            owner = campaign_for(experiments[jobs[a['job']]['experiment']], campaigns)
            if owner:
                hf = owner.get('hf') or {}
                identity = (hf.get('repo_id'), hf.get('repo_type','model'))
                if identity not in pause_cache:
                    pause_cache[identity] = upload_pause(owner, transfers, campaigns)
                if pause_cache[identity]:
                    return False
        if ((require_hf and 'hf' not in n)
                or (not n['enabled'] and not source_upload_allowed(a,n,direction))
                or any(r['node'] == n['id'] for r in live)):
            return False
        if health.get(n['id'], {}).get('phase') in ('ssh_retrying', 'unavailable'):
            return False
        snap = snapshots.get(n['id'], {})
        if base_health(n, snap, time.time()) or snap.get('stable_polls', 0) < n['policy']['stable_polls']:
            return False
        if snap['ram_available_mib'] < n['policy']['min_free_ram_mib'] + 1024:
            return False
        if n['startup_group'] and any(x['spec']['startup_group'] == n['startup_group']
                and not x.get('released') for x in attempts + live if x['status'] in ACTIVE):
            return False
        if n['startup_group']:
            group = n['startup_group']
            for other in nodes.values():
                if other['enabled'] and other['startup_group'] == group:
                    s = snapshots.get(other['id'], {})
                    if (s.get('error') or s.get('d_state', 1) or not s.get('read_ok')
                            or time.time() - s.get('received_at', 0) > other['policy']['max_snapshot_age_s']):
                        return False
            last = max((x['created'] for x in attempts + transfers if x['spec']['startup_group'] == group), default=0)
            if time.time() - last < store.specs('groups_')[group]['min_start_interval_s']:
                return False
        history = [r for r in transfers if r['attempt'] == a['id'] and r['node'] == n['id']
                   and r['direction'] == direction]
        if repair_revision:
            if any(r['status'] in ACTIVE or r['status']=='succeeded' for r in history):return False
            history=[r for r in history if r['spec']['config'].get('repair_revision')==repair_revision]
        return retry_possible(history)

    # Explicit finite operator repairs are consumed by the single dispatcher,
    # ahead of ordinary publication. Preserve all health, slot and 429 gates.
    if store.db.execute("SELECT 1 FROM sqlite_master WHERE name='artifact_repair_queue'").fetchone():
        for pending in store.db.execute("SELECT * FROM artifact_repair_queue WHERE state='pending' ORDER BY created").fetchall():
            if pending['expires'] < time.time():
                with store.db:store.db.execute("UPDATE artifact_repair_queue SET state='expired' WHERE attempt=?",(pending['attempt'],))
                continue
            a=next((v for v in attempts if v['id']==pending['attempt'] and v['status']=='succeeded'),None)
            if not a:continue
            cfg=json.loads(pending['config'])
            if not hf_spec(cfg.get('hf',{})).get('enabled',False):continue
            if any(t['attempt']==a['id'] and t['direction']=='upload' and (t['status']=='succeeded' or t['spec']['config'].get('repair_revision')==cfg['repair_revision']) for t in transfers):
                with store.db:store.db.execute("UPDATE artifact_repair_queue SET state='submitted' WHERE attempt=?",(a['id'],))
                continue
            n=nodes[a['node']]
            if eligible(a,n,'upload',cfg['repair_revision']):
                key=start(controller,a,n,'upload',cfg)
                with store.db:
                    store.db.execute("UPDATE artifact_repair_queue SET state='submitted' WHERE attempt=?",(a['id'],))
                    store.event('artifact_repair_submitted',a['job'],dict(transfer=key))
                return

    # Stage the most urgent runnable successor before publishing unrelated history.
    successful = {a['job']: a for a in attempts if a['status'] == 'succeeded'}
    queued = sorted(ready_queued,
                    key=lambda j: -(experiments[j['experiment']]['priority'] + j['spec']['priority']))
    demand = runnable_stage_demand(queued, jobs, experiments, nodes, snapshots,
                                   attempts + live, successful, groups, time.time())
    # Prefer unused capacity, not inventory insertion order; calculate once.
    staging_nodes = sorted(nodes.values(), key=lambda n: (
        sum(len(x['spec'].get('gpus', [])) for x in attempts
            if x['node'] == n['id'] and x['status'] in ACTIVE)
        / max(1, sum(g['enabled'] for g in n['gpus'])),
        -sum(g['enabled'] for g in n['gpus']), n['id']))
    for j in queued:
        if not all(jobs[d]['status'] == 'succeeded' for d in j['spec']['depends_on']):
            continue
        for dep in j['spec']['depends_on']:
            if dep in j['spec'].get('order_only_dependencies', []):
                continue
            a = successful[dep]
            receipt = a['report'].get('hf_artifact')
            for n in staging_nodes:
                if n['id'] == a['node'] or n['id'] in a.get('artifact_locations', {}):
                    continue
                if n['storage_domain'] and n['storage_domain'] == a['spec']['node_spec']['storage_domain']:
                    continue
                relay_node = nodes.get('resource-control')
                local_relay = bool(a.get('report', {}).get('outputs') and relay_node
                                   and relay_node.get('transport') == 'local')
                if not local_relay and not receipt:
                    continue
                if not eligible(a, n, 'download', require_hf=not local_relay):
                    continue
                # Copy only the location maps we change. Deep-copying every
                # historical attempt/report per candidate can outlast the
                # health freshness window and starve scientific dispatch.
                hypothetical = dict(successful)
                for d in j['spec']['depends_on']:
                    if (hypothetical[d]['report'].get('hf_artifact')
                            or hypothetical[d]['report'].get('outputs')):
                        item = dict(hypothetical[d])
                        item['artifact_locations'] = dict(item.get('artifact_locations', {}))
                        item['artifact_locations'][n['id']] = {}
                        hypothetical[d] = item
                fits = [fit(dict(j['spec'], resources=resources), n, snapshots[n['id']],
                            [a for a in attempts if a['status'] in ACTIVE] + reservations(store),
                            attempts, hypothetical, groups, time.time())[0]
                        for resources in [j['spec']['resources'], *j['spec'].get('resource_variants', [])]]
                if any(not reason for reason in fits):
                    # Internal dependency staging prefers the controller-local
                    # relay. HF remains the immutable publication/fallback path.
                    if local_relay:
                        start_local_relay(controller, a, nodes[a['node']], n)
                    elif receipt:
                        start(controller, a, n, 'download', dict(receipt=receipt))
                    return
    # Prioritize publications that unblock successors; successful computation stays successful.
    needed = {d for j in queued for d in j['spec']['depends_on']}
    priorities = dependency_priorities(list(jobs.values()), experiments)
    # Select a publication candidate from small immutable metadata; only the
    # chosen source needs its full frozen attempt/export report.
    latest_success = {}
    for row in store.db.execute("SELECT id,job,node,created FROM attempts WHERE status='succeeded' ORDER BY created"):
        latest_success[row['job']] = dict(row)
    published = {t['attempt'] for t in transfers if t['direction']=='upload' and t['status']=='succeeded'}
    detailed = {a['id']:a for a in attempts}
    for candidate in sorted(latest_success.values(), key=lambda a: (a['job'] not in demand,
                    -demand.get(a['job'], 0), a['job'] not in needed,
                    -priorities[a['job']][0], -priorities[a['job']][1],
                    -priorities[a['job']][2], a['created'])):
        if candidate['id'] in published:
            continue
        job = jobs.get(candidate['job'])
        if job is None:
            continue
        spec = job['spec']
        if not spec['outputs']:
            continue
        campaign = campaign_for(experiments[job['experiment']], campaigns)
        n = nodes.get(candidate['node'])
        if not campaign or n is None:
            continue
        a = detailed.get(candidate['id'])
        if a is None and n['enabled'] and not eligible(candidate, n, 'upload'):
            continue
        if a is None:
            a = next((item for item in store.attempts(summary=True,job_ids={candidate['job']})
                      if item['id']==candidate['id']), None)
            if a is None:
                continue
        if a['report'].get('hf_artifact') or not eligible(a,n,'upload'):
            continue
        relocations = list(spec.get('hf_relocate_json', []))
        if 'TRAIN_RESULT.json' in spec['outputs'] and 'TRAIN_RESULT.json' not in relocations:
            relocations.append('TRAIN_RESULT.json')
        config = dict(hf=campaign['hf'], campaign=campaign['id'], attempt=a['id'], job=a['job'],
                      source_root=a['spec']['attempt_dir'], outputs=a['report']['outputs'],
                      patterns=list(dict.fromkeys(spec['outputs'] + spec.get('hf_artifacts', []))),
                      relocate_json=relocations,
                      archive_payload=campaign['hf'].get('archive_payload', True))
        history = [r for r in transfers if r['attempt'] == a['id'] and r['direction'] == 'upload']
        if history and history[-1]['spec']['config'].get('repair_revision'):
            config['repair_revision'] = history[-1]['spec']['config']['repair_revision']
        start(controller, a, n, 'upload', config)
        return


def status(store):
    return [dict(id=r['id'], attempt=r['attempt'], node=r['node'], direction=r['direction'],
                 status=r['status'], result=r['report'].get('artifact')) for r in rows(store)]


def publication_summary(store, campaign=None, snapshot=None):
    from .notifications import campaign_specs
    from .observation_snapshot import ObservationSnapshot
    snapshot = snapshot if snapshot is not None else ObservationSnapshot(store)
    campaigns = {campaign['id']: campaign} if campaign else campaign_specs(store)
    if campaign and not campaign.get('hf',{}).get('enabled',False):
        return dict(counts=dict(pending=0,published=0,error=0),errors=[],warnings=[],disabled=True)
    jobs = snapshot.jobs_by_id
    experiments, nodes = snapshot.experiments, snapshot.nodes
    from .recovery import current_campaign_jobs
    current = ({j['id'] for j in current_campaign_jobs(list(jobs.values()), experiments, campaign)}
               if campaign else None)
    transfers = snapshot.transfers
    counts = dict(pending=0, published=0, error=0)
    errors = []
    warnings = []
    for a in snapshot.attempts:
        if a['status'] != 'succeeded' or not jobs[a['job']]['spec']['outputs']:
            continue
        if (a['job'] not in current if current is not None else
                not campaign_for(experiments[jobs[a['job']]['experiment']], campaigns)):
            continue
        history = snapshot.uploads_by_attempt.get(a['id'], [])
        if a['report'].get('hf_artifact'):
            counts['published'] += 1
        elif 'hf' not in nodes.get(a['node'], {}):
            counts['pending'] += 1
            warnings.append(dict(job=a['job'], status='pending', reason='HF Python/auth paths not configured on source node'))
        elif history and all(r['status'] == 'failed' for r in history) and not retry_possible(history, check_time=False):
            counts['pending'] += 1
            warnings.append(dict(job=a['job'], status='pending', reason='HF upload retry budget exhausted; computation remains successful'))
        else:
            counts['pending'] += 1
    downloads = {}
    selected = current if current is not None else {key for key,j in jobs.items() if campaign_for(experiments[j['experiment']], campaigns)}
    # A failed speculative copy is no longer actionable after every current
    # consumer of that producer has completed (possibly on another node).
    pending_dependencies = {
        dep for key in selected if jobs[key]['status'] == 'queued'
        for dep in jobs[key]['spec']['depends_on']
        if dep not in jobs[key]['spec'].get('order_only_dependencies', [])}
    needed_attempts = {a['id'] for a in snapshot.attempts
                       if a['job'] in pending_dependencies and a['status'] == 'succeeded'}
    for r in transfers:
        if r['direction'] == 'download' and r['attempt'] in needed_attempts:
            downloads.setdefault((r['attempt'], r['node']), []).append(r)
    for (attempt, node), history in downloads.items():
        if len(history) >= 3 and all(r['status'] == 'failed' for r in history):
            counts['error'] += 1
            errors.append(dict(job=attempt, status='artifact_error', reason='HF download retries exhausted on ' + node))
    return dict(counts=counts, errors=errors[:8], warnings=warnings[:8])
