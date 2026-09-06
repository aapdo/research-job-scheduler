"""Bounded detached HF publication and destination staging; no GPU transfer lease."""
import copy
import hashlib
import json
import re
import time
import uuid
from pathlib import Path, PurePosixPath

from . import agent, hf_worker
from .schema import check, fields
from .store import ACTIVE, dumps


def hf_spec(raw):
    h = dict(raw)
    fields(h, 'repo_id repo_type revision path_prefix')
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
    return [dict(r, released=False) for r in rows(store) if r['status'] in ACTIVE]


def campaign_for(experiment, campaigns):
    matches = [c for c in campaigns.values() if c.get('hf') and c['enabled'] and not c['external']
               and (experiment['id'] in c['experiments'] or experiment['project'] in c['projects'])]
    check(len(matches) <= 1, 'experiment selected by multiple HF campaigns')
    return matches[0] if matches else None


def start(controller, attempt, node, direction, config):
    store = controller.store
    key = 'hf-' + uuid.uuid4().hex
    directory = str(Path(node['work_root'], 'artifact-transfers', key))
    config = dict(config, direction=direction, token_file=node['hf'].get('token_file', ''))
    if direction == 'download':
        config['destination'] = directory + '/payload'
    request = dict(id=key, job=attempt['job'], node_spec=node, attempt_dir=directory,
                   argv=[node['hf']['python'], '-c', Path(hf_worker.__file__).read_text()],
                   cwd=directory, env={}, config=config, input_files=[], outputs=['HF_RECEIPT.json'],
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


def tick(controller, execute):
    """Called with scheduler lock. At most two cluster transfers, one per node."""
    from .notifications import campaign_specs
    from .planner import base_health, fit
    store = controller.store
    campaigns = campaign_specs(store)
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
    attempts = store.attempts()
    jobs = {j['id']: j for j in store.jobs()}
    experiments = store.specs('experiments')

    def eligible(a, n, direction):
        if 'hf' not in n or not n['enabled'] or any(r['node'] == n['id'] for r in live):
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
        return (not any(r['status'] != 'failed' for r in history) and len(history) < 3
                and (not history or time.time() - history[-1]['created'] > 60))

    # Stage the most urgent runnable successor before publishing unrelated history.
    successful = {a['job']: a for a in attempts if a['status'] == 'succeeded'}
    queued = sorted((j for j in jobs.values() if j['status'] == 'queued'),
                    key=lambda j: -(experiments[j['experiment']]['priority'] + j['spec']['priority']))
    for j in queued:
        if not all(jobs[d]['status'] == 'succeeded' for d in j['spec']['depends_on']):
            continue
        for dep in j['spec']['depends_on']:
            if dep in j['spec'].get('order_only_dependencies', []):
                continue
            a = successful[dep]
            receipt = a['report'].get('hf_artifact')
            if not receipt:
                continue
            for n in nodes.values():
                if n['id'] == a['node'] or n['id'] in a.get('artifact_locations', {}):
                    continue
                if n['storage_domain'] and n['storage_domain'] == a['spec']['node_spec']['storage_domain']:
                    continue
                if not eligible(a, n, 'download'):
                    continue
                hypothetical = copy.deepcopy(successful)
                for d in j['spec']['depends_on']:
                    if hypothetical[d]['report'].get('hf_artifact'):
                        hypothetical[d].setdefault('artifact_locations', {})[n['id']] = {}
                fits = [fit(dict(j['spec'], resources=resources), n, snapshots[n['id']],
                            [a for a in attempts if a['status'] in ACTIVE] + reservations(store),
                            attempts, hypothetical, store.specs('groups_'), time.time())[0]
                        for resources in [j['spec']['resources'], *j['spec'].get('resource_variants', [])]]
                if any(not reason for reason in fits):
                    start(controller, a, n, 'download', dict(receipt=receipt))
                    return
    # Prioritize publications that unblock successors; successful computation stays successful.
    needed = {d for j in queued for d in j['spec']['depends_on']}
    for a in sorted(successful.values(), key=lambda a: (a['job'] not in needed, a['created'])):
        if a['report'].get('hf_artifact'):
            continue
        campaign = campaign_for(experiments[jobs[a['job']]['experiment']], campaigns)
        n = nodes.get(a['node'])
        if not campaign or n is None or not eligible(a, n, 'upload'):
            continue
        spec = jobs[a['job']]['spec']
        if not spec['outputs']:
            continue
        config = dict(hf=campaign['hf'], campaign=campaign['id'], attempt=a['id'], job=a['job'],
                      source_root=a['spec']['attempt_dir'], outputs=a['report']['outputs'],
                      patterns=list(dict.fromkeys(spec['outputs'] + spec.get('hf_artifacts', []))),
                      relocate_json=spec.get('hf_relocate_json', []))
        start(controller, a, n, 'upload', config)
        return


def status(store):
    return [dict(id=r['id'], attempt=r['attempt'], node=r['node'], direction=r['direction'],
                 status=r['status'], result=r['report'].get('artifact')) for r in rows(store)]


def publication_summary(store, campaign=None):
    from .notifications import campaign_specs
    campaigns = {campaign['id']: campaign} if campaign else campaign_specs(store)
    jobs = {j['id']: j for j in store.jobs()}
    experiments, nodes = store.specs('experiments'), store.specs('nodes')
    transfers = rows(store)
    counts = dict(pending=0, published=0, error=0)
    errors = []
    for a in store.attempts():
        if a['status'] != 'succeeded' or not jobs[a['job']]['spec']['outputs']:
            continue
        if not campaign_for(experiments[jobs[a['job']]['experiment']], campaigns):
            continue
        history = [r for r in transfers if r['attempt'] == a['id'] and r['direction'] == 'upload']
        if a['report'].get('hf_artifact'):
            counts['published'] += 1
        elif 'hf' not in nodes.get(a['node'], {}):
            counts['error'] += 1
            errors.append(dict(job=a['job'], status='artifact_error', reason='HF Python/auth paths not configured on source node'))
        elif len(history) >= 3 and all(r['status'] == 'failed' for r in history):
            counts['error'] += 1
            errors.append(dict(job=a['job'], status='artifact_error', reason='HF upload retry budget exhausted; training remains successful'))
        else:
            counts['pending'] += 1
    downloads = {}
    selected = {key for key,j in jobs.items() if campaign_for(experiments[j['experiment']], campaigns)}
    needed_attempts = {a['id'] for a in store.attempts() if a['job'] in selected}
    for r in transfers:
        if r['direction'] == 'download' and r['attempt'] in needed_attempts:
            downloads.setdefault((r['attempt'], r['node']), []).append(r)
    for (attempt, node), history in downloads.items():
        if len(history) >= 3 and all(r['status'] == 'failed' for r in history):
            counts['error'] += 1
            errors.append(dict(job=attempt, status='artifact_error', reason='HF download retries exhausted on ' + node))
    return dict(counts=counts, errors=errors[:8])
