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
from .planner import workload_node_rank


def hf_spec(raw):
    h = dict(raw)
    fields(h, 'repo_id repo_type revision path_prefix archive_payload commit_interval_s enabled')
    if 'enabled' in h:
        check(type(h['enabled']) is bool, 'HF enabled must be boolean')
    else:
        # Publication is opt-in even for one-off registration processes that
        # do not inherit the controller service environment.
        default=os.environ.get('RS_HF_UPLOAD_DEFAULT','0')
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


def rows(store, compact=False, active_only=False):
    if compact:
        where = (" WHERE status IN ('starting','running','unknown')"
                 if active_only else '')
        return [dict(r, spec=json.loads(r['spec']), report=json.loads(r['report']))
                for r in store.db.execute(
                    "SELECT id,attempt,node,direction,status,created,"
                    "CASE WHEN status IN ('starting','running','unknown') THEN report "
                    "ELSE json_remove(report,'$.artifact.files') END AS report,"
                    "CASE WHEN status IN ('starting','running','unknown') THEN spec "
                    "ELSE json_remove(spec,'$.argv','$.config.files') END AS spec "
                    "FROM artifact_transfers" + where + " ORDER BY created")]
    return [dict(r, spec=json.loads(r['spec']), report=json.loads(r['report']))
            for r in store.db.execute('SELECT * FROM artifact_transfers ORDER BY created')]


def reservations(store):
    # Completed transfers cannot reserve resources. Avoid decoding their large
    # frozen requests merely to discard them on every read-only dashboard plan.
    states=sorted(ACTIVE)
    query='SELECT * FROM artifact_transfers WHERE status IN ('+','.join('?' for _ in states)+') ORDER BY created'
    return [dict(r, spec=json.loads(r['spec']), report=json.loads(r['report']), released=False)
            for r in store.db.execute(query, states)]


def dependency_relay_available(live, attempt_id, node_id, limit=24):
    """Allow distinct artifacts to share a destination, never duplicate one."""
    rows = [r for r in live if r['direction'] != 'archive' and r['node'] == node_id]
    return len(rows) < limit and not any(r['attempt'] == attempt_id for r in rows)


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


def _reserve_relay_intent(store, consumer_job, node_id, transfer_id):
    if not consumer_job:
        return
    now=time.time()
    existing=store.db.execute("SELECT node,state FROM relay_intents WHERE consumer_job=?", (consumer_job,)).fetchone()
    if existing and existing['state']=='pending' and existing['node'] != node_id:
        return False
    store.db.execute(
        "INSERT INTO relay_intents(consumer_job,node,state,created,updated,last_transfer) VALUES(?,?, 'pending',?,?,?) "
        "ON CONFLICT(consumer_job) DO UPDATE SET node=excluded.node,state='pending',updated=excluded.updated,last_transfer=excluded.last_transfer",
        (consumer_job,node_id,now,now,transfer_id))
    return True


def start(controller, attempt, node, direction, config, consumer_job=None):
    store = controller.store
    key = 'hf-' + uuid.uuid4().hex
    directory = str(Path(node['work_root'], 'artifact-transfers', key))
    config = dict(config, direction=direction, token_file=node['hf'].get('token_file', ''))
    if consumer_job:
        config['consumer_job']=consumer_job
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
        if direction == 'download' and not _reserve_relay_intent(store, consumer_job, node['id'], key):
            return None
        store.db.execute('INSERT INTO artifact_transfers VALUES(?,?,?,?,?,?,?,?)',
                         (key, attempt['id'], node['id'], direction, 'starting', dumps(request), '{}', time.time()))
        store.event('artifact_transfer_reserved', key, dict(attempt=attempt['id'], node=node['id'], direction=direction))
    try:
        controller.transport.call(node, 'launch', request)
    except Exception:
        pass  # Lost ACK: reconcile the same immutable transfer, never duplicate it.
    return key


def dependency_relay_route(source_node, destination_node):
    """Choose a topology-safe dependency transfer route.

    FARM and LAB cannot initiate connections to each other, so that boundary
    uses bounded controller-local staging. Other node pairs are streamed
    through the controller without a local disk copy; this uses the controller's
    already verified SSH identities and does not require peer SSH configuration.
    """
    source = source_node['id'].lower()
    destination = destination_node['id'].lower()
    source_zone = 'farm' if source.startswith('farm') else 'lab' if source.startswith('lab') else 'other'
    destination_zone = ('farm' if destination.startswith('farm') else
                        'lab' if destination.startswith('lab') else 'other')
    if {source_zone, destination_zone} == {'farm', 'lab'}:
        return 'controller-local-staging'
    if source_node['id'] == destination_node['id']:
        return 'lab4-local' if destination_node['id'] == 'lab4' else 'direct-stream'
    return 'direct-stream'


def start_local_relay(controller, attempt, source_node, destination_node, source_receipt=None, consumer_job=None):
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
    source_root = Path(source_receipt['root'] if source_receipt else attempt['spec']['attempt_dir'])
    for name, item in artifacts.items():
        rel = PurePosixPath(name)
        check(not rel.is_absolute() and '..' not in rel.parts and '\\' not in name,
              'unsafe dependency output path')
        expected = {'sha256': item['sha256'], 'bytes': item['bytes']}
        if source_receipt:
            archived = source_receipt.get('files', {}).get(name, {})
            check({'sha256': archived.get('sha256'), 'bytes': archived.get('bytes')} == expected,
                  'attempt archive does not match dependency artifact: ' + name)
        files[name] = expected
    manifest_sha = hashlib.sha256(dumps(files).encode()).hexdigest()
    key = 'relay-' + uuid.uuid4().hex
    directory = str(Path(relay_node['work_root'], 'artifact-transfers', key))
    destination = str(Path(destination_node['work_root'], 'artifact-relays', key, 'payload'))
    route = dependency_relay_route(source_node, destination_node)
    config = dict(source_attempt=attempt['id'], source_target=(source_node['target']
                  if source_node['transport'] == 'ssh' else '@local'), source_root=str(source_root),
                  destination_target=(destination_node['target']
                  if destination_node['transport'] == 'ssh' else '@local'),
                  destination_root=destination, files=files, manifest_sha256=manifest_sha,
                  transport_route=route, consumer_job=consumer_job)
    if route == 'controller-local-staging':
        config['controller_min_free_bytes']=relay_node['policy']['min_free_disk_mib'] * 1024**2
    request = dict(id=key, job=attempt['job'], node_spec=relay_node, attempt_dir=directory,
                   argv=[relay_node['python'], '-c', Path(relay_worker.__file__).read_text()],
                   cwd=directory, env={}, config=config, input_files=[], outputs=['HF_RECEIPT.json'],
                   resources=dict(gpu_count=0, vram_mib=0, gpu_mode='exclusive', cpu=2, ram_mib=1024),
                   startup_group='', gpus=[])
    request['runner_sha256'] = hashlib.sha256(Path(agent.__file__).read_bytes()).hexdigest()
    request['spec_sha256'] = hashlib.sha256(dumps(request).encode()).hexdigest()
    with store.db:
        if not _reserve_relay_intent(store, consumer_job, destination_node['id'], key):
            return None
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


def start_attempt_archive(controller, attempt, source_node, archive_node, campaign_ids):
    """Copy a complete terminal attempt through controller-local staging."""
    store = controller.store
    relay_node = store.specs('nodes').get('resource-control')
    check(relay_node is not None and relay_node.get('transport') == 'local',
          'resource-control local relay node is not configured')
    check(attempt['status'] in ('succeeded', 'failed'), 'only terminal attempts can be archived')
    check(attempt['spec'].get('spec_sha256'), 'attempt has no immutable specification hash')
    rel = PurePosixPath(attempt['id'])
    check(not rel.is_absolute() and len(rel.parts) == 1 and '..' not in rel.parts,
          'unsafe attempt archive identifier')
    key = 'archive-' + uuid.uuid4().hex
    directory = str(Path(relay_node['work_root'], 'artifact-transfers', key))
    experiment = attempt['experiment_id']
    primary_campaign = campaign_ids[0]
    destination = str(Path(archive_node['work_root'], 'attempt-archive', primary_campaign,
                           experiment, attempt['job'], attempt['id']))
    route = ('cps1-staging' if source_node['id'].lower().startswith('farm')
             else 'lab4-local' if source_node['id'] == archive_node['id']
             else 'direct-stream' if source_node['transport'] == 'local' else 'server-direct')
    config = dict(mode='attempt-archive', source_attempt=attempt['id'],
                  source_node=source_node['id'],
                  source_target=(source_node['target'] if source_node['transport'] == 'ssh' else '@local'),
                  source_root=attempt['spec']['attempt_dir'],
                  source_spec_sha256=attempt['spec']['spec_sha256'],
                  destination_target=(archive_node['target'] if archive_node['transport'] == 'ssh' else '@local'),
                  destination_root=destination, transport_route=route,
                  campaigns=campaign_ids, experiment=experiment, job=attempt['job'],
                  destination_min_free_bytes=archive_node['policy']['min_free_disk_mib'] * 1024**2,
                  controller_min_free_bytes=relay_node['policy']['min_free_disk_mib'] * 1024**2)
    if route == 'cps1-staging':
        staging_node = store.specs('nodes').get('cps1-model')
        check(staging_node is not None and staging_node.get('enabled')
              and staging_node.get('transport') == 'ssh',
              'CPS1 archive staging node is unavailable')
        config.update(staging_target=staging_node['target'],
                      staging_root=str(Path(staging_node['work_root'], 'artifact-relay-staging', key)),
                      staging_min_free_bytes=staging_node['policy']['min_free_disk_mib'] * 1024**2)
    request = dict(id=key, job=attempt['job'], node_spec=relay_node, attempt_dir=directory,
                   argv=[relay_node['python'], '-c', Path(relay_worker.__file__).read_text()],
                   cwd=directory, env={}, config=config, input_files=[], outputs=['HF_RECEIPT.json'],
                   resources=dict(gpu_count=0, vram_mib=0, gpu_mode='exclusive', cpu=2, ram_mib=1024),
                   startup_group='', gpus=[])
    request['runner_sha256'] = hashlib.sha256(Path(agent.__file__).read_bytes()).hexdigest()
    request['spec_sha256'] = hashlib.sha256(dumps(request).encode()).hexdigest()
    with store.db:
        store.db.execute('INSERT INTO artifact_transfers VALUES(?,?,?,?,?,?,?,?)',
                         (key, attempt['id'], archive_node['id'], 'archive', 'starting',
                          dumps(request), '{}', time.time()))
        store.event('attempt_archive_reserved', attempt['id'],
                    dict(transfer=key, source=source_node['id'], destination=archive_node['id'],
                         route=route, campaigns=campaign_ids))
    try:
        controller.transport.call(relay_node, 'launch', request)
    except Exception:
        pass
    return key


def _contains_attempt_path(value, root):
    if isinstance(value, str):
        return root in value
    if isinstance(value, list):
        return any(_contains_attempt_path(item, root) for item in value)
    if isinstance(value, dict):
        return any(_contains_attempt_path(item, root) for item in value.values())
    return False


def attempt_archive_policy(store):
    path = os.environ.get('RS_ATTEMPT_ARCHIVE_POLICY', '')
    if not path:
        return None
    raw = json.loads(Path(path).read_text())
    check({'archive_node', 'campaigns', 'delete_after_idle_s'} <= set(raw)
          and set(raw) <= {'archive_node', 'campaigns', 'delete_after_idle_s', 'automatic_since',
                           'backfill_nodes'},
          'invalid attempt archive policy fields')
    check(isinstance(raw['archive_node'], str) and raw['archive_node'],
          'attempt archive node required')
    check(isinstance(raw['campaigns'], list) and raw['campaigns']
          and len(raw['campaigns']) == len(set(raw['campaigns'])),
          'attempt archive campaigns must be a non-empty unique list')
    check(type(raw['delete_after_idle_s']) is int and raw['delete_after_idle_s'] >= 300,
          'attempt archive deletion grace must be at least 300 seconds')
    automatic_since = raw.get('automatic_since')
    check(automatic_since is None or (type(automatic_since) in (int, float) and automatic_since > 0),
          'automatic_since must be a positive timestamp')
    backfill_nodes = raw.get('backfill_nodes', [])
    check(isinstance(backfill_nodes, list) and len(backfill_nodes) == len(set(backfill_nodes))
          and all(isinstance(node, str) and node for node in backfill_nodes),
          'backfill_nodes must be a unique node list')
    check(set(backfill_nodes) <= set(store.specs('nodes')),
          'unknown attempt archive backfill node')
    from .notifications import campaign_specs
    campaigns = campaign_specs(store)
    check(set(raw['campaigns']) <= set(campaigns), 'unknown attempt archive campaign')
    experiments = {row['id']: {'project': row['project']} for row in store.db.execute(
        "SELECT id,json_extract(spec,'$.project') AS project FROM experiments")}
    selected = set()
    experiment_campaigns = {}
    for key in raw['campaigns']:
        campaign = campaigns[key]
        chosen = set(campaign['experiments'])
        chosen.update(eid for eid, experiment in experiments.items()
                      if experiment['project'] in campaign['projects'])
        selected.update(chosen)
        for experiment in chosen:
            experiment_campaigns.setdefault(experiment, []).append(key)
    backfill = set(selected)
    if automatic_since is not None:
        for key, campaign in campaigns.items():
            if campaign.get('external'):
                continue
            chosen = set(campaign['experiments']) | {
                eid for eid, experiment in experiments.items()
                if experiment['project'] in campaign['projects']}
            for eid in chosen:
                if key not in experiment_campaigns.setdefault(eid, []):
                    experiment_campaigns[eid].append(key)
        selected.update(experiments)
        for eid in experiments:
            experiment_campaigns.setdefault(eid, ['uncategorized'])
    return dict(raw, experiments=selected, backfill_experiments=backfill,
                backfill_nodes=backfill_nodes,
                experiment_campaigns=experiment_campaigns)


def ensure_attempt_archive_index(store):
    """Backfill terminal-attempt archive metadata once; triggers keep it current."""
    row = store.db.execute(
        "SELECT complete FROM scheduler_migrations WHERE name='attempt_archive_index_v1'"
    ).fetchone()
    if row and row[0]:
        return False
    with store.db:
        store.db.execute(
            "INSERT OR REPLACE INTO attempt_archive_index(attempt,finished,root,kind) "
            "SELECT id,COALESCE(CAST(json_extract(report,'$.finished') AS REAL),created),"
            "COALESCE(json_extract(spec,'$.attempt_dir'),''),"
            "COALESCE(json_extract(spec,'$.job_spec.kind'),'') FROM attempts "
            "WHERE status IN ('succeeded','failed')")
        store.db.execute(
            "UPDATE scheduler_migrations SET complete=1 "
            "WHERE name='attempt_archive_index_v1'")
        store.event('attempt_archive_index_backfilled', 'attempt_archive_index_v1', {
            'rows': store.db.execute('SELECT count(*) FROM attempt_archive_index').fetchone()[0]})
    return True


def urgent_archive_destination_staging(store, archive_node_id):
    """Prioritize explicitly LAB4-hosted report inputs over more archives."""
    for row in store.db.execute("SELECT id,spec FROM jobs WHERE status='queued' ORDER BY id"):
        spec = json.loads(row['spec'])
        if (spec.get('metadata', {}).get('report_storage') != 'lab4-direct-relay'
                or spec.get('hosts') != [archive_node_id]):
            continue
        order_only = set(spec.get('order_only_dependencies', []))
        ready = True
        missing = False
        for dependency in spec.get('depends_on', []):
            if dependency in order_only:
                continue
            attempt = store.db.execute(
                "SELECT id,node FROM attempts WHERE job=? AND status='succeeded' "
                "ORDER BY created DESC LIMIT 1", (dependency,)).fetchone()
            if attempt is None:
                ready = False
                break
            if attempt['node'] == archive_node_id:
                continue
            location = store.db.execute(
                "SELECT 1 FROM artifact_transfers WHERE attempt=? AND node=? "
                "AND status='succeeded' AND direction IN ('download','archive') LIMIT 1",
                (attempt['id'], archive_node_id)).fetchone()
            missing = missing or location is None
        if ready and missing:
            return True
    return False


def archive_source_in_use(store, root, idle_s):
    """Recheck live references under the registry lock after an unlocked RPC."""
    cutoff = time.time() - idle_s
    queries = [
        ("SELECT json_remove(spec,'$.experiment_spec') FROM attempts "
         "WHERE status IN ('starting','running','unknown') UNION "
         "SELECT json_remove(spec,'$.experiment_spec') FROM attempts WHERE created>=? UNION "
         "SELECT json_remove(a.spec,'$.experiment_spec') FROM attempt_archive_index i "
         "JOIN attempts a ON a.id=i.attempt WHERE i.finished>=?", (cutoff, cutoff)),
        ("SELECT spec FROM artifact_transfers WHERE status IN ('starting','running','unknown') "
         "OR created>=? OR COALESCE(json_extract(report,'$.finished'),0)>=?", (cutoff, cutoff)),
        ("SELECT spec FROM jobs WHERE status='queued'", ()),
        ("SELECT spec FROM nodes", ()),
    ]
    return any(_contains_attempt_path(json.loads(row[0]), root)
               for query, params in queries for row in store.db.execute(query, params))


def cleanup_archived_sources(controller, nodes, policy):
    """Delete verified archive sources only when no active request still binds them."""
    store = controller.store
    cutoff = time.time() - policy['delete_after_idle_s']
    protected_specs = [json.loads(r['spec']) for r in store.db.execute(
        "SELECT json_remove(spec,'$.experiment_spec') AS spec FROM attempts "
        "WHERE status IN ('starting','running','unknown') UNION "
        "SELECT json_remove(spec,'$.experiment_spec') AS spec FROM attempts WHERE created>=? UNION "
        "SELECT json_remove(a.spec,'$.experiment_spec') AS spec FROM attempt_archive_index i "
        "JOIN attempts a ON a.id=i.attempt WHERE i.finished>=?", (cutoff, cutoff))]
    protected_transfers = [json.loads(r['spec']) for r in store.db.execute(
        "SELECT spec FROM artifact_transfers WHERE status IN ('starting','running','unknown') "
        "OR created>=? OR COALESCE(json_extract(report,'$.finished'),0)>=?", (cutoff, cutoff))]
    protected_specs.extend(json.loads(r[0]) for r in store.db.execute(
        "SELECT spec FROM jobs WHERE status='queued'"))
    priority_nodes = list(policy.get('backfill_nodes', []))
    cleanup_order = ("CASE a.node " + ' '.join(
        'WHEN ? THEN ' + str(index) for index, _ in enumerate(priority_nodes))
        + " ELSE " + str(len(priority_nodes)) + " END," if priority_nodes else '')
    cleanup_checks = 0
    for row in store.db.execute(
            "SELECT t.id,t.attempt,t.node AS archive_node,t.report,t.created,a.node,a.spec,a.status "
            "FROM artifact_transfers t JOIN attempts a ON a.id=t.attempt "
            "WHERE t.direction='archive' AND t.status='succeeded' ORDER BY "
            + cleanup_order + "t.created", tuple(priority_nodes)).fetchall():
        report = json.loads(row['report'])
        if report.get('source_cleanup', {}).get('deleted'):
            continue
        if time.time() - report.get('cleanup_last_check', 0) < 60:
            continue
        archive = report.get('artifact', {})
        finished = report.get('finished', 0)
        finished = finished if isinstance(finished, (int, float)) else 0
        if max(row['created'], finished) >= cutoff:
            continue
        attempt_spec = json.loads(row['spec'])
        source = nodes.get(row['node'])
        root = attempt_spec.get('attempt_dir', '')
        if (source is None or row['status'] not in ('succeeded', 'failed')
                or not archive.get('complete_attempt') or not root):
            continue
        if any(_contains_attempt_path(spec, root) for spec in protected_specs):
            continue
        if any(_contains_attempt_path(spec, root) for spec in protected_transfers):
            continue
        archive_node = nodes.get(row['archive_node'])
        if archive_node is None or row['archive_node'] != policy.get('archive_node', 'lab4'):
            continue
        if cleanup_checks >= 3:
            return False
        cleanup_checks += 1
        report['cleanup_last_check'] = time.time()
        with store.db:
            store.db.execute('UPDATE artifact_transfers SET report=? WHERE id=?', (dumps(report), row['id']))
        try:
            verified = controller.transport.call(archive_node, 'verify_archived_attempt',
                dict(archive_receipt=archive, archive_work_root=archive_node['work_root']))
            if not verified.get('verified') or verified.get('manifest_sha256') != archive['manifest_sha256']:
                continue
            if archive_source_in_use(store, root, policy['delete_after_idle_s']):
                continue
            # Retire the old location before any destructive RPC. Even if its
            # acknowledgement is lost, no new request may bind that location.
            report['origin_retired'] = True
            with store.db:
                store.db.execute('UPDATE artifact_transfers SET report=? WHERE id=?', (dumps(report), row['id']))
            request = dict(attempt_spec, archive_receipt=archive,
                           archive_idle_s=policy['delete_after_idle_s'])
            result = controller.transport.call(source, 'cleanup_archived_attempt', request)
        except Exception as exc:
            report['cleanup_error'] = type(exc).__name__ + ': ' + str(exc)[-400:]
            with store.db:
                store.db.execute('UPDATE artifact_transfers SET report=? WHERE id=?', (dumps(report), row['id']))
            # One source may be unverifiable (for example a container that
            # hides another same-UID process' /proc/fd). Keep that source and
            # continue; it must not starve cleanup of independent archives.
            continue
        if not result.get('deleted'):
            continue
        report['source_cleanup'] = result
        report.pop('cleanup_error', None)
        with store.db:
            store.db.execute('UPDATE artifact_transfers SET report=? WHERE id=?',
                             (dumps(report), row['id']))
            store.event('attempt_archive_source_deleted', row['attempt'],
                        dict(transfer=row['id'], node=row['node'], already_absent=result.get('already_absent', False)))
        return True
    return False


def start_pending_attempt_archive(controller, nodes, snapshots, health, live, policy):
    """Reserve one complete terminal-attempt archive on LAB4 when transfer slots permit."""
    from .planner import base_health
    store = controller.store
    archive_node = nodes.get(policy['archive_node'])
    if archive_node is None or any(t['node'] == archive_node['id'] for t in live):
        return None
    snap = snapshots.get(archive_node['id'])
    if (health.get(archive_node['id'], {}).get('phase') in ('ssh_retrying', 'unavailable')
            or base_health(archive_node, snap, time.time())
            or snap.get('stable_polls', 0) < archive_node['policy']['stable_polls']):
        return None
    selected = sorted(policy['experiments'])
    if not selected:
        return None
    backfill = sorted(policy.get('backfill_experiments', selected))
    backfill_nodes = list(policy.get('backfill_nodes', []))
    automatic_since = policy.get('automatic_since', 1e100)
    cutoff = time.time() - policy['delete_after_idle_s']
    protected = [json.loads(r[0]) for r in store.db.execute(
        "SELECT json_remove(spec,'$.experiment_spec') FROM attempts "
        "WHERE status IN ('starting','running','unknown') UNION "
        "SELECT json_remove(spec,'$.experiment_spec') FROM attempts WHERE created>=? UNION "
        "SELECT json_remove(a.spec,'$.experiment_spec') FROM attempt_archive_index i "
        "JOIN attempts a ON a.id=i.attempt WHERE i.finished>=?", (cutoff, cutoff))]
    protected.extend(json.loads(r[0]) for r in store.db.execute(
        "SELECT spec FROM artifact_transfers WHERE status IN ('starting','running','unknown') "
        "OR created>=? OR COALESCE(json_extract(report,'$.finished'),0)>=?", (cutoff, cutoff)))
    node_selection = (" OR a.node IN (" + ','.join('?' for _ in backfill_nodes) + ")"
                      if backfill_nodes else '')
    node_order = ("CASE a.node " + ' '.join(
        'WHEN ? THEN ' + str(index) for index, _ in enumerate(backfill_nodes))
        + " ELSE " + str(len(backfill_nodes)) + " END," if backfill_nodes else '')
    candidates = store.db.execute(
        "SELECT a.id,a.node,j.experiment AS experiment_id,i.root "
        "FROM attempt_archive_index i JOIN attempts a ON a.id=i.attempt "
        "JOIN jobs j ON j.id=a.job "
        "WHERE a.status IN ('succeeded','failed') AND (j.experiment IN ("
        + ','.join('?' for _ in backfill) + ")" + node_selection
        + " OR a.created>=? OR i.finished>=?) "
        "AND i.kind NOT IN ('rtl_sim','rtl_build','rtl_ooc','board_test') "
        "AND i.finished<? "
        "AND NOT EXISTS (SELECT 1 FROM artifact_transfers t "
        "WHERE t.attempt=a.id AND t.direction='archive' AND t.status IN "
        "('starting','running','unknown','succeeded')) ORDER BY " + node_order + "a.created",
        (*backfill, *backfill_nodes, automatic_since, automatic_since, cutoff,
         *backfill_nodes)).fetchall()
    for row in candidates:
        if not row['root'] or any(_contains_attempt_path(spec, row['root']) for spec in protected):
            continue
        source = nodes.get(row['node'])
        if source is None or any(t['node'] == source['id'] for t in live):
            continue
        if health.get(source['id'], {}).get('phase') in ('ssh_retrying', 'unavailable'):
            continue
        history = [dict(t, spec=json.loads(t['spec']), report=json.loads(t['report']))
                   for t in store.db.execute(
                       "SELECT * FROM artifact_transfers WHERE attempt=? AND direction='archive' ORDER BY created",
                       (row['id'],))]
        if not retry_possible(history):
            continue
        full = store.db.execute('SELECT * FROM attempts WHERE id=?', (row['id'],)).fetchone()
        attempt = dict(full, spec=json.loads(full['spec']), report=json.loads(full['report']),
                       experiment_id=row['experiment_id'])
        experiment = attempt['experiment_id']
        return start_attempt_archive(
            controller, attempt, source, archive_node,
            policy['experiment_campaigns'][experiment])
    return None


def missing_dependency_count(spec, successful, node):
    """Count verified dependency locations still needed on one candidate node."""
    order_only = set(spec.get('order_only_dependencies', []))
    missing = 0
    for dependency in spec.get('depends_on', []):
        if dependency in order_only:
            continue
        attempt = successful[dependency]
        same_domain = (node['storage_domain'] and
                       node['storage_domain'] == attempt['spec']['node_spec']['storage_domain'])
        if (attempt.get('origin_retired') or
                (attempt['node'] != node['id'] and not same_domain)):
            missing += node['id'] not in attempt.get('artifact_locations', {})
    return missing


def dependency_staging_node_key(spec, successful, node, attempts):
    """Rank the consumer's GPU pool before transfer convenience.

    A partially populated low-priority node must not capture a train merely
    because it needs fewer files than an RP/primary-pool node.
    """
    active = [a for a in attempts if a['status'] in ACTIVE and a['node'] == node['id']]
    gpu_count = max(1, sum(g['enabled'] for g in node['gpus']))
    load = sum(len(a['spec'].get('gpus', [])) for a in active) / gpu_count
    return (*workload_node_rank(spec['kind'], node['id']),
            missing_dependency_count(spec, successful, node),
            load, -sum(g['enabled'] for g in node['gpus']), node['id'])


def warmed_staging_snapshot(node, snapshot, now, wait_s=180):
    """Simulate only an in-progress stable-poll gate for relay placement."""
    required = node['policy']['stable_polls']
    since = snapshot.get('stable_since')
    if (snapshot.get('stable_polls', 0) >= required
            or not isinstance(since, (int, float)) or not 0 <= now-since < wait_s):
        return None
    warmed = copy.deepcopy(snapshot)
    warmed['stable_polls'] = required
    for gpu in warmed.get('gpus', []):
        gpu['stable_polls'] = max(gpu.get('stable_polls', 0), required)
    return warmed


def runnable_stage_demand(queued, jobs, experiments, nodes, snapshots, attempts, successful, groups, now):
    """Rank missing artifacts by GPU work that can run once staging finishes.

    Hypothetical locations are used only here, never for actual admission.
    All non-artifact gates (including explicit holds and VRAM) still apply.
    """
    from .planner import fit, dependency_pool_candidates
    held = [a for a in attempts if a['status'] in ACTIVE]
    demand = {}
    for j in queued:
        spec = j['spec']
        if any(jobs[d]['status'] != 'succeeded' for d in spec['depends_on']):
            continue
        deps = [d for d in spec['depends_on'] if d not in spec.get('order_only_dependencies', [])]
        candidates = [n for n in nodes.values()
            if n['enabled'] and (not spec['hosts'] or n['id'] in spec['hosts'])
            and not any(n['labels'].get(k) != v for k,v in spec['labels'].items())]
        pool = dependency_pool_candidates(spec, nodes, snapshots, held, attempts, successful, groups, now)
        if pool is not None:
            candidates = [n for n in candidates if n['id'] in pool]
        # A verified existing location that can run now is better than another
        # speculative replica. Let the ordinary dispatcher reserve it first.
        if any(any(not fit(dict(spec, resources=req), n, snapshots.get(n['id']), held,
                           attempts, successful, groups, now)[0]
                       for req in [spec['resources'], *spec.get('resource_variants', [])])
               for n in candidates):
            continue
        for n in candidates:
            missing = [d for d in deps if n['id'] not in successful[d].get('artifact_locations', {})
                       and (successful[d].get('origin_retired') or (successful[d]['node'] != n['id']
                       and not (n['storage_domain'] and n['storage_domain'] == successful[d]['spec']['node_spec']['storage_domain'])))]
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


def reserve_transfer_slots(live, start_dependency, start_archive, archive_urgent=False,
                           refresh_live=None, max_total=32, max_dependency=24, max_archive=8):
    """Fill bounded transfer capacity while reserving eight archive lanes.

    Dependency staging is considered first and may use up to 24 slots.
    Complete attempt archival owns at most eight separate slots. The dependency
    cap remains 24 even when archive candidates are temporarily absent.
    """
    live = list(live)
    launched = []
    if max_total <= 0 or len(live) >= max_total:
        return launched
    def include_reserved(current, key, direction):
        refreshed = list(refresh_live()) if refresh_live else []
        if any(t.get('id') == key for t in refreshed):
            return refreshed
        base = refreshed if refresh_live else current
        return [*base, {'id': key, 'node': '', 'direction': direction, 'status': 'starting'}]
    archive_limit=min(max_archive, max(0, max_total-max_dependency))
    dependency_limit = min(max_dependency, max_total if archive_urgent else max(0, max_total - archive_limit))
    while (len(live) < max_total
           and sum(t['direction'] != 'archive' for t in live) < dependency_limit):
        key = start_dependency(live)
        if not key:
            break
        launched.append(key)
        live = include_reserved(live, key, 'download')
    archive_count=sum(t['direction'] == 'archive' for t in live)
    while len(live) < max_total and not archive_urgent and archive_count < archive_limit:
        key = start_archive(live)
        if not key:break
        launched.append(key)
        live = include_reserved(live, key, 'archive')
        archive_count += 1
    return launched


def tick(controller, execute):
    """Called with scheduler lock. Ten dependency relays plus one archive lane."""
    from .notifications import campaign_specs, campaign_processing_due
    from .planner import base_health, fit, dependency_priorities, dependency_missing_for_node
    store = controller.store
    phase_times = {}; phase_started = time.monotonic()
    def mark(name):
        nonlocal phase_started
        now=time.monotonic();phase_times[name]=round(now-phase_started,4);phase_started=now
        controller.artifact_phase_times=phase_times
    campaigns = {key:value for key,value in campaign_specs(store).items()
                 if campaign_processing_due(store,value)}
    health = controller.node_health()
    mark('campaigns_health_s')
    hf_enabled = any(c.get('hf', {}).get('enabled') is True for c in campaigns.values())
    transfers = rows(store, compact=True, active_only=not hf_enabled)
    mark('transfer_rows_s')
    for r in transfers:
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
            consumer=r['spec'].get('config', {}).get('consumer_job')
            if consumer and state == 'failed':
                store.db.execute("UPDATE relay_intents SET state='failed',updated=? WHERE consumer_job=? AND last_transfer=?",
                                 (time.time(),consumer,r['id']))
            if state != r['status']:
                store.event('artifact_transfer_' + state, r['id'], dict(attempt=r['attempt'], direction=r['direction']))
        r.update(status=state, report=report)
    mark('transfer_reconcile_s')
    if not execute:
        return
    nodes, snapshots = store.specs('nodes'), controller.snapshots()
    live = [r for r in transfers if r['status'] in ACTIVE]
    archive_policy = attempt_archive_policy(store)
    mark('nodes_policy_s')
    if archive_policy and not getattr(controller, 'artifact_cleanup_deferred', False):
        cleanup_archived_sources(controller, nodes, archive_policy)
    transfer_capacity = 32  # 24 dependency relays plus eight independent archive lanes.
    if len(live) >= transfer_capacity:
        return
    if hf_enabled:
        jobs = {j['id']: j for j in store.jobs()}
    else:
        # Only runnable successors and their direct producers are needed for
        # internal relay. Historical terminal job profiles are audit data.
        needed_jobs = {r[0] for r in store.db.execute("SELECT id FROM jobs WHERE status='queued'")}
        needed_jobs.update(r[0] for r in store.db.execute(
            "SELECT d.value FROM jobs j,json_each(j.spec,'$.depends_on') d WHERE j.status='queued'"))
        needed_jobs.update(r[0] for r in store.db.execute(
            "SELECT job FROM attempts WHERE status IN ('starting','running','unknown')"))
        if store.db.execute("SELECT 1 FROM sqlite_master WHERE name='artifact_repair_queue'").fetchone():
            needed_jobs.update(r[0] for r in store.db.execute(
                "SELECT job FROM attempts WHERE id IN (SELECT attempt FROM artifact_repair_queue WHERE state='pending')"))
        jobs = {}
        ids = sorted(needed_jobs)
        for offset in range(0, len(ids), 500):
            batch = ids[offset:offset+500]
            for r in store.db.execute('SELECT * FROM jobs WHERE id IN ('+','.join('?' for _ in batch)+')', batch):
                jobs[r['id']] = dict(r, spec=json.loads(r['spec']))
    mark('jobs_s')
    experiments = {r['id']:json.loads(r['spec']) for r in store.db.execute(
        "SELECT id,json_remove(spec,'$.jobs') AS spec FROM experiments")}
    mark('experiments_s')
    from .model_vram_policy import normalize
    jobs = {k:dict(j,spec=normalize(j['spec'])) for k,j in jobs.items()}
    # Dependency staging is an internal scheduler guarantee, independent of
    # whether a campaign publishes long-term results to HF. This also covers
    # future campaigns that only declare ordinary depends_on outputs.
    ready_queued = [j for j in jobs.values() if j['status'] == 'queued'
                    and not j['spec'].get('metadata',{}).get('operator_hold')
                    and not j['spec'].get('labels',{}).get('operator_hold')
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
    mark('attempts_s')
    from .planner import admission_vram
    attempts = [dict(a, admission_vram_mib=admission_vram(a, jobs.get(a.get('job'),{}), time.time()))
                if a['status'] in ACTIVE else a for a in attempts]
    relay_intents={row['consumer_job']:dict(row) for row in store.db.execute(
        "SELECT consumer_job,node,state,last_transfer FROM relay_intents WHERE state='pending'")}

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
        destination_busy = (not dependency_relay_available(live, a['id'], n['id'])
                            if direction == 'download' else
                            any(r['node'] == n['id'] for r in live))
        if ((require_hf and 'hf' not in n)
                or (not n['enabled'] and not source_upload_allowed(a,n,direction))
                or destination_busy):
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
        if not hf_enabled and direction == 'download':
            history = [dict(r, spec=json.loads(r['spec']), report=json.loads(r['report']))
                       for r in store.db.execute(
                           "SELECT * FROM artifact_transfers WHERE attempt=? AND node=? "
                           "AND direction=? ORDER BY created", (a['id'], n['id'], direction))]
        else:
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
            # A repair request may outlive a retired source node.  Keep the
            # audit row pending; it can only be submitted from a current node.
            n=nodes.get(a['node'])
            if n is None:continue
            if eligible(a,n,'upload',cfg['repair_revision']):
                key=start(controller,a,n,'upload',cfg)
                with store.db:
                    store.db.execute("UPDATE artifact_repair_queue SET state='submitted' WHERE attempt=?",(a['id'],))
                    store.event('artifact_repair_submitted',a['job'],dict(transfer=key))
                return

    # Stage the most urgent runnable successor before routine retention. One
    # separate slot remains available for LAB4 archival.
    successful = {a['job']: a for a in attempts if a['status'] == 'succeeded'}
    queued = sorted(ready_queued,
                    key=lambda j: -(experiments[j['experiment']]['priority'] + j['spec']['priority']))
    demand = runnable_stage_demand(queued, jobs, experiments, nodes, snapshots,
                                   attempts + live, successful, groups, time.time())
    mark('demand_scan_s')
    def start_dependency(_live):
        from .planner import dependency_pool_candidates
        for j in queued:
            if not all(jobs[d]['status'] == 'succeeded' for d in j['spec']['depends_on']):
                continue
            # Stage onto the same pool order the consumer planner will use. This
            # prevents a low-priority node from becoming the only eligible host
            # merely because generic relay balancing copied an artifact there first.
            intent=relay_intents.get(j['id'])
            if intent and intent['node'] in nodes:
                pool={intent['node']: dependency_missing_for_node(j['spec'], intent['node'], successful, nodes)}
            else:
                pool = dependency_pool_candidates(j['spec'], nodes, snapshots,
                    [a for a in attempts if a['status'] in ACTIVE] + reservations(store),
                    attempts, successful, groups, time.time())
            if pool is not None and any(not missing for missing in pool.values()):
                continue
            staging_nodes = sorted((n for n in nodes.values() if pool is None or n['id'] in pool), key=lambda n:
                dependency_staging_node_key(j['spec'], successful, n, attempts))
            for dep in j['spec']['depends_on']:
                if dep in j['spec'].get('order_only_dependencies', []):
                    continue
                if dep not in demand:
                    continue
                a = successful[dep]
                receipt = a['report'].get('hf_artifact')
                for n in staging_nodes:
                    if not dependency_relay_available(_live, a['id'], n['id']):
                        continue
                    if (n['id'] == a['node'] and not a.get('origin_retired')) or n['id'] in a.get('artifact_locations', {}):
                        continue
                    if not a.get('origin_retired') and n['storage_domain'] and n['storage_domain'] == a['spec']['node_spec']['storage_domain']:
                        continue
                    relay_node = nodes.get('resource-control')
                    local_relay = bool(a.get('report', {}).get('outputs') and relay_node
                                       and relay_node.get('transport') == 'local')
                    if not local_relay and not receipt:
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
                    active = [a for a in attempts if a['status'] in ACTIVE] + reservations(store)
                    snapshot = snapshots.get(n['id'], {})
                    fits = [fit(dict(j['spec'], resources=resources), n, snapshot,
                                active,
                                attempts, hypothetical, groups, time.time())[0]
                            for resources in [j['spec']['resources'], *j['spec'].get('resource_variants', [])]]
                    if any(not reason for reason in fits):
                        if not eligible(a, n, 'download', require_hf=not local_relay):
                            continue
                        # Internal dependency staging prefers the controller-local
                        # relay. HF remains the immutable publication/fallback path.
                        if local_relay:
                            archive_sources = [(node_id, receipt) for node_id, receipt
                                               in a.get('artifact_locations', {}).items()
                                               if node_id in nodes and receipt.get('complete_attempt')]
                            if archive_sources:
                                source_id, source_receipt = sorted(archive_sources)[0]
                                return start_local_relay(controller, a, nodes[source_id], n, source_receipt,
                                                         consumer_job=j['id'])
                            source = nodes.get(a['node'])
                            if source is not None:
                                return start_local_relay(controller, a, source, n, consumer_job=j['id'])
                        if receipt:
                            return start(controller, a, n, 'download', dict(receipt=receipt), consumer_job=j['id'])
                    warmed = warmed_staging_snapshot(n, snapshot, time.time())
                    if warmed:
                        warm_fits = [fit(dict(j['spec'], resources=resources), n, warmed,
                                         active, attempts, hypothetical, groups, time.time())[0]
                                     for resources in [j['spec']['resources'],
                                                       *j['spec'].get('resource_variants', [])]]
                        if any(not reason for reason in warm_fits):
                            # Keep the relay slot free while the preferred GPU
                            # finishes its bounded health streak. The next cycle
                            # re-evaluates it; lower pools are considered after
                            # the same 180-second bound used by placement.
                            return None
        return None

    urgent_lab4_staging = (archive_policy and urgent_archive_destination_staging(
        store, archive_policy['archive_node']))
    launched = reserve_transfer_slots(
        live, start_dependency,
        lambda current: (start_pending_attempt_archive(
            controller, nodes, snapshots, health, current, archive_policy)
            if archive_policy else None),
        archive_urgent=bool(urgent_lab4_staging),
        refresh_live=lambda: reservations(store), max_total=transfer_capacity, max_dependency=24, max_archive=8)
    mark('dependency_reserve_s')
    if launched or not hf_enabled:
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
