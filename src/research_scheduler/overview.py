"""Read-only GPU/campaign overview, with optional bounded live progress reads."""
import concurrent.futures
import json
from pathlib import Path
import shlex
import sqlite3
import subprocess
import time
from urllib.parse import quote
from collections import Counter
from datetime import datetime, timezone, timedelta

from .controller import Controller
from .recovery import current_campaign_jobs
from .waiting import summarize, summarize_by_type, work_type, display_status, waiting_detail
from .visibility import completed_jobs_at, expired_complete, valid_timestamp

ACTIVE = {'starting', 'running', 'unknown'}


class ReadStore:
    def __init__(self, db):
        self.db = sqlite3.connect('file:' + quote(str(Path(db).resolve()), safe='/') + '?mode=ro', uri=True)
        self.db.row_factory = sqlite3.Row
        self.db.execute('PRAGMA query_only=ON')
        self.db.execute('BEGIN')
        # All readers share one SQLite snapshot. Reuse decoded rows when the
        # overview and admission planner inspect the same historical attempts.
        self._rows = {}
        self._specs = {}

    def rows(self, table):
        if table not in {'nodes', 'experiments', 'groups_', 'jobs', 'attempts', 'campaigns', 'campaign_runtime'}:
            raise ValueError('unsupported overview table')
        if table in self._rows:
            return self._rows[table]
        if not self.db.execute("SELECT 1 FROM sqlite_master WHERE type='table' AND name=?", (table,)).fetchone():
            return []
        result = []
        for row in self.db.execute('SELECT * FROM ' + table):
            value = dict(row)
            for k in ('spec', 'report', 'artifact_locations', 'details'):
                if isinstance(value.get(k), str): value[k] = json.loads(value[k])
            result.append(value)
        self._rows[table] = result
        return result

    def specs(self, table):
        if table not in self._specs:
            self._specs[table] = {r['id']:r['spec'] for r in self.rows(table)}
        return self._specs[table]

    def jobs(self): return self.rows('jobs')

    def attempts(self, active=False, *, job_ids=None, summary=False, planning=False):
        # Overview/admission need resource and lineage fields, not the full
        # frozen campaign duplicated in every historical execution request.
        # Retain full reads for explicit audit callers, never for launch RPCs.
        if (summary or planning) and 'attempts' not in self._rows:
            if not hasattr(self, '_attempt_summary'):
                self._attempt_summary = [dict(r, spec=json.loads(r['spec']), report=json.loads(r['report']))
                    for r in self.db.execute("SELECT id,job,node,status,created,released,ready_polls,report,"
                        "json_remove(spec,'$.experiment_spec') AS spec FROM attempts")]
            rows = self._attempt_summary
        else:
            rows = self.rows('attempts')
        result = [a for a in rows if (not active or a['status'] in ACTIVE)
                  and (job_ids is None or a['job'] in job_ids)]
        # The read-only admission planner must see the same verified artifact
        # locations as the writable controller. Otherwise the dashboard reports
        # a cross-filesystem dependency wait after the relay has already
        # succeeded. Keep this targeted to planner/summary job subsets so the
        # overview never decodes every historical transfer receipt per refresh.
        if job_ids is not None and (summary or planning):
            by_id = {a['id']: a for a in result if a['status'] == 'succeeded'}
            if by_id:
                ids = list(by_id)
                query = ("SELECT attempt,node,direction,report FROM artifact_transfers "
                         "WHERE status='succeeded' AND attempt IN ("
                         + ','.join('?' for _ in ids) + ") ORDER BY created")
                transfers = list(self.db.execute(query, ids))
                for row in transfers:
                    attempt = by_id.get(row['attempt'])
                    if attempt is None:
                        continue
                    receipt = json.loads(row['report']).get('artifact')
                    if receipt and row['direction'] == 'upload':
                        attempt['report']['hf_artifact'] = receipt
                for row in transfers:
                    if row['direction'] not in ('download', 'archive'):
                        continue
                    attempt = by_id.get(row['attempt'])
                    if attempt is None:
                        continue
                    transfer_report = json.loads(row['report'])
                    receipt = transfer_report.get('artifact')
                    if not receipt:
                        continue
                    report = attempt['report']
                    job_spec = attempt.get('spec', {}).get('job_spec', {})
                    declared = job_spec.get('dependency_artifacts', job_spec.get('hf_artifacts', []))
                    published = report.get('hf_artifact', {})
                    if (declared and not report.get('dependency_artifacts')
                            and published.get('attempt') != attempt['id']):
                        continue
                    required = (report.get('dependency_artifacts') or
                                published.get('files') or
                                report.get('outputs', {}))
                    if not set(required).issubset(receipt.get('files', {})):
                        continue
                    if row['direction'] == 'archive' and any(
                            (receipt['files'][name].get('sha256'), receipt['files'][name].get('bytes')) !=
                            (value.get('sha256'), value.get('bytes'))
                            for name, value in required.items()):
                        continue
                    attempt.setdefault('artifact_locations', {})[row['node']] = receipt
                    if row['direction'] == 'archive' and (
                            transfer_report.get('origin_retired') or
                            transfer_report.get('source_cleanup', {}).get('deleted')):
                        attempt['origin_retired'] = True
        return result


def dependency_details(job, by_id, trail=(), depth=0):
    result = []
    for key in job['spec'].get('depends_on', []):
        source = by_id.get(key)
        status = source['status'] if source else 'missing'
        row = dict(job=key, status=status, satisfied=status == 'succeeded',
                   work_type=work_type(source['spec']) if source else None)
        if source and status != 'succeeded' and key not in trail and depth < 3:
            row['dependencies'] = dependency_details(source, by_id, trail+(key,), depth+1)
        result.append(row)
    return result


def age(now, stamp):
    return round(max(0, now-stamp), 1) if isinstance(stamp, (int, float)) and stamp > 0 else None


def collect(db, *, node=None, campaign=None, job=None, hardware_index=None, live_progress=False, timeout=12):
    now = time.time()
    store = ReadStore(db)
    warnings = []
    try:
        nodes, experiments = store.specs('nodes'), store.specs('experiments')
        jobs, attempts = store.jobs(), store.attempts(summary=True)
        controller = Controller(store)
        snapshots, health = controller.snapshots(), controller.node_health()
        campaigns = store.specs('campaigns')
        from .registration import registration_fields
        registrations = {r['id']:registration_fields(r.get('created')) for r in store.rows('campaigns')}
        runtimes = {r['id']:r for r in store.rows('campaign_runtime')}
        completion_alerts, publication_times = {}, {}
        tables = {r[0] for r in store.db.execute("SELECT name FROM sqlite_master WHERE type='table'")}
        validation_states = {}
        if 'execution_preparations' in tables:
            for r in store.db.execute('SELECT profile,state FROM execution_preparations'):
                validation_states.setdefault(r['profile'], []).append(r['state'])
        if 'notification_outbox' in tables:
            completion_alerts = dict(store.db.execute(
                "SELECT campaign,MAX(created) FROM notification_outbox WHERE state='complete' GROUP BY campaign"))
        if 'artifact_transfers' in tables:
            for r in store.db.execute("SELECT attempt,report FROM artifact_transfers WHERE direction='upload' AND status='succeeded'"):
                stamp = json.loads(r['report']).get('finished')
                if valid_timestamp(stamp):
                    publication_times[r['attempt']] = max(publication_times.get(r['attempt'], 0), stamp)
        diagnostics = []
        if store.db.execute("SELECT 1 FROM sqlite_master WHERE type='table' AND name='events'").fetchone():
            changes = {}
            latest_change = {}
            for r in store.db.execute("SELECT subject,time,data FROM events WHERE kind='node_storage_profile_changed' ORDER BY seq DESC LIMIT 40"):
                if now-r['time'] > 600: continue
                value = json.loads(r['data'])
                if 'max_jobs' in value:
                    changes.setdefault(r['subject'], []).append(value['max_jobs'])
                    latest_change.setdefault(r['subject'], r['time'])
            for name, values in changes.items():
                if len(values) >= 4 and values[0] == values[2] and values[1] == values[3] and values[0] != values[1]:
                    snap = snapshots.get(name, {})
                    required = nodes.get(name, {}).get('policy', {}).get('stable_polls', 3)
                    resolved = (snap.get('stable_polls', 0) >= required
                                and snap.get('received_at', 0) > latest_change[name])
                    diagnostics.append(dict(node=name, kind='repeating_storage_profile', recent_max_jobs=values[:4],
                                            active=not resolved, last_change_age_s=age(now, latest_change[name])))
                    if not resolved and (not node or node == name):
                        warnings.append(f'{name}: max_jobs repeatedly alternates {values[1]}↔{values[0]}; profile changes can reset snapshots/stable polls')
        try: plans = {p['job']:p for p in controller.plan()}
        except Exception as exc:
            plans = {}; warnings.append('Admission plan unavailable: ' + str(exc))
    finally:
        store.db.close()
    membership = {j['id']:[] for j in jobs}
    by_id = {j['id']:j for j in jobs}
    groups = []
    latest = {}
    for a in sorted(attempts, key=lambda a:a['created']): latest[a['job']] = a
    hidden_campaigns = []
    covered_experiments = set()
    for key, spec in campaigns.items():
        covered_experiments.update(spec['experiments'])
        covered_experiments.update(k for k,v in experiments.items() if v.get('project','general') in spec['projects'])
        selected = current_campaign_jobs(jobs, experiments, spec)
        for j in selected: membership[j['id']].append(key)
        runtime = runtimes.get(key, {})
        completed_at = None
        if runtime.get('state') == 'complete':
            candidates = [completion_alerts.get(key), runtime.get('details', {}).get('completed_at'),
                          completed_jobs_at(selected, latest, publication_times)]
            candidates = [v for v in candidates if valid_timestamp(v)]
            if candidates: completed_at = max(candidates)
        # Never hide a reopened campaign merely because its observer is behind.
        can_hide = spec.get('external') or (selected and all(j['status'] in {'succeeded','cancelled'} for j in selected))
        if not can_hide: completed_at = None
        if can_hide and expired_complete(runtime.get('state'), completed_at, now):
            hidden_campaigns.append(key)
            continue
        groups.append(dict(id=key, name=spec['name'], external=spec.get('external', False),
            counts=summarize(selected, by_id, validation_states), counts_by_type=summarize_by_type(selected, by_id, validation_states), job_ids=[j['id'] for j in selected],
            recorded_state=runtime.get('state'), runtime_age_s=age(now, runtime.get('updated')),
            completed_at=completed_at, **registrations.get(key, registration_fields(None)),
            external_counts=runtime.get('details', {}).get('counts') if spec.get('external') else None,
            publication=runtime.get('details', {}).get('publication')))
    # Do not lose unregistered/precheck projects merely because they lack alerts.
    for project in sorted({experiments[j['experiment']].get('project', 'general') for j in jobs if j['experiment'] not in covered_experiments}):
        selected = [j for j in jobs if j['experiment'] not in covered_experiments and experiments[j['experiment']].get('project', 'general') == project]
        key = 'project:' + project
        for j in selected: membership[j['id']].append(key)
        completed_at = completed_jobs_at(selected, latest, publication_times)
        if expired_complete('complete', completed_at, now):
            hidden_campaigns.append(key)
            continue
        groups.append(dict(id=key, name=project, counts=summarize(selected, by_id, validation_states), counts_by_type=summarize_by_type(selected, by_id, validation_states),
                           job_ids=[j['id'] for j in selected], completed_at=completed_at, unregistered_project=True))
    for keys in membership.values():
        keys[:] = [key for key in keys if key not in hidden_campaigns]
    def wanted(j):
        a = latest.get(j['id'])
        visible = [key for key in membership[j['id']] if key not in hidden_campaigns]
        return ((bool(visible) or j['status'] in ACTIVE) and (not campaign or campaign in visible) and (not job or job in j['id'])
                and (not node or (a and a['status'] in ACTIVE and a['node'] == node)
                     or plans.get(j['id'], {}).get('node') == node))
    rows = []
    for j in jobs:
        if not wanted(j): continue
        a = latest.get(j['id'])
        row = dict(id=j['id'], status=j['status'], kind=j['spec']['kind'], work_type=work_type(j['spec']), campaigns=membership[j['id']],
                   display_status=display_status(j, by_id, validation_states), waiting=waiting_detail(j, by_id, validation_states),
                   dependencies=dependency_details(j, by_id, (j['id'],)),
                   reason=j['reason'], resources=j['spec']['resources'],
                   resource_variants=j['spec'].get('resource_variants', []), plan=plans.get(j['id']),
                   historical_replacement=j['spec'].get('metadata', {}).get('recovery_replacement'))
        if a:
            row['last_attempt'] = dict(id=a['id'], status=a['status'], node=a['node'],
                                       gpus=a['spec'].get('gpus', []), directory=a['spec']['attempt_dir'])
        if a and a['status'] in ACTIVE and j['status'] in ACTIVE:
            row.update(attempt=a['id'], attempt_status=a['status'], node=a['node'],
                       gpus=a['spec'].get('gpus', []), attempt_dir=a['spec']['attempt_dir'],
                       heartbeat_age_s=age(now, a['report'].get('heartbeat')))
        rows.append(row)
    included = {r['id'] for r in rows}
    gpu_rows = []
    for name, n in nodes.items():
        if node and node != name: continue
        snap = snapshots.get(name, {})
        observed = {g['uuid']:g for g in snap.get('gpus', [])}
        for gpu in n.get('gpus', []):
            held = [a for a in attempts if a['status'] in ACTIVE and gpu['uuid'] in a['spec'].get('gpus', [])]
            shown = [a for a in held if a['job'] in included]
            if (campaign or job) and not shown: continue
            live = observed.get(gpu['uuid'], {})
            snapshot_age = age(now, snap.get('received_at', snap.get('time')))
            gpu_rows.append(dict(node=name, index=gpu['index'], uuid=gpu['uuid'],
                enabled=bool(n['enabled'] and gpu['enabled'] and gpu['uuid'] not in n['policy'].get('disabled_gpu_uuids', [])),
                jobs=[dict(job=a['job'], attempt=a['id'], status=a['status'], work_type=work_type(by_id[a['job']]['spec']), execution_node=a['node'], campaigns=membership.get(a['job'], [])) for a in shown],
                held_jobs_total=len(held), temperature_c=live.get('temperature_c'), used_mib=live.get('used_mib'),
                total_mib=live.get('memory_mib'), utilization_percent=live.get('util_percent'),
                snapshot_at=snap.get('received_at', snap.get('time')),
                sample_started_at=snap.get('time'),
                snapshot_age_s=snapshot_age, stale=snapshot_age is None or snapshot_age > n['policy']['max_snapshot_age_s'],
                health=health.get(name, {})))
    if live_progress:
        attach_progress(rows, nodes, timeout)
    hardware = []
    if hardware_index and not node and not job:
        try:
            index_path = Path(hardware_index).resolve()
            index = json.loads(index_path.read_text())
            for item in index['campaigns']:
                if campaign and item['id'] != campaign: continue
                try:
                    state_path = Path(item['state'])
                    if not state_path.is_absolute(): state_path = index_path.parent / state_path
                    d = json.loads(state_path.read_text())
                    completed_at = None
                    if d['phase'] == 'complete':
                        history_path = state_path.parent / 'HISTORY.jsonl'
                        if history_path.exists():
                            # The last completion transition, not refreshed STATE mtime.
                            try:
                                for line in history_path.read_text().splitlines():
                                    event = json.loads(line)
                                    if event.get('phase') == 'complete' and valid_timestamp(event.get('time')):
                                        completed_at = event['time']
                                    elif event.get('phase') != 'complete':
                                        completed_at = None
                            except (ValueError, OSError):
                                completed_at = None
                                warnings.append(item['id'] + ': completion history unreadable; kept visible')
                    if expired_complete(d['phase'], completed_at, now):
                        hidden_campaigns.append(item['id'])
                        continue
                    registration = registration_fields(None)
                    if d.get('scheduler_database'):
                        try:
                            with sqlite3.connect(Path(d['scheduler_database']).resolve().as_uri()+'?mode=ro', uri=True) as hardware_db:
                                row = hardware_db.execute('SELECT created FROM campaigns WHERE id=?', (item['id'],)).fetchone()
                                if row: registration = registration_fields(row[0])
                        except sqlite3.Error:
                            pass  # Missing historical registration must not become today's date.
                    hardware.append(dict(id=item['id'], phase=d['phase'], validation=d.get('validation'),
                        build_status=d.get('build_status'), board=d.get('board'),
                        completed_at=completed_at, **registration,
                        age_s=age(now, d.get('observed_at')), state_file=str(state_path)))
                except Exception as exc: warnings.append(item['id'] + ': ' + str(exc))
        except Exception as exc: warnings.append('Hardware registry unavailable: ' + str(exc))
    selected_groups = [g for g in groups if not campaign or g['id'] == campaign]
    if node or job:
        matching_groups = {key for r in rows for key in r['campaigns']}
        selected_groups = [g for g in selected_groups if g['id'] in matching_groups]
    if node and node not in nodes: warnings.append('Unknown node filter: ' + node)
    if campaign and not selected_groups and not hardware and campaign not in hidden_campaigns:
        warnings.append('Unknown campaign filter: ' + campaign)
    if job and not rows: warnings.append('No matching job: ' + job)
    return dict(time_kst=datetime.fromtimestamp(now, timezone(timedelta(hours=9))).isoformat(),
        read_only=True, live_progress=live_progress, db=str(Path(db).resolve()),
        filters=dict(node=node, campaign=campaign, job=job),
        visibility=dict(completed_max_age_hours=36, hidden_campaign_count=len(hidden_campaigns)),
        campaigns=selected_groups, gpus=gpu_rows, jobs=rows, hardware=hardware, warnings=warnings, diagnostics=diagnostics,
        allocation=dict(total_gpus=len({g['uuid'] for g in gpu_rows}),
                        assigned_gpus=len({g['uuid'] for g in gpu_rows if g['jobs']}),
                        active_by_type=dict(Counter(work_type(by_id[key]['spec']) for key in
                            {a['job'] for a in attempts if a['status'] in ACTIVE and a['job'] in included})),
                        active_jobs=len({a['job'] for a in attempts if a['status'] in ACTIVE and a['job'] in included})),
        notes=['GPU rows represent scheduler reservations, not exclusive physical use.',
               'Campaigns may overlap; do not sum their counts as unique jobs.',
               'Queued jobs may have unmet dependencies; this is distinct from job status blocked.',
               'Node/job filters narrow job/GPU detail; campaign counts remain campaign-wide.'])


def attach_progress(rows, nodes, timeout):
    import inspect
    from .progress_phases import legacy_posterior_phase
    grouped = {}
    for row in rows:
        if row['status'] in ACTIVE and row.get('attempt_dir'):
            grouped.setdefault(row['node'], []).append(row)
    code = inspect.getsource(legacy_posterior_phase) + '''
import json,sys,time,re
from pathlib import Path
for key,d in json.loads(sys.argv[1]):
 out={'id':key}
 try:
  root=Path(d);p=root/'run/TRAIN_PROGRESS.json'
  if not p.is_file():p=root/'TRAIN_PROGRESS.json'
  if p.is_file():
   with p.open('rb') as stream:raw=stream.read(131073)
   assert len(raw)<=131072
   v=json.loads(raw);out['progress']={k:v.get(k) for k in ['epoch','planned_epochs','optimizer_steps_executed','step_in_epoch','steps_per_epoch','status','member','members','training_iterations_executed']}
   # Bootstrap counts actual optimizer updates per member as applied; total
   # training iterations can include skipped AMP steps and are not updates.
   if out['progress']['optimizer_steps_executed'] is None and type(v.get('applied')) is int and v['applied']>=0:
    out['progress']['optimizer_steps_executed']=v['applied']
   out['progress']['age_s']=round(max(0,time.time()-p.stat().st_mtime),1)
   contract=p.parent/'EXECUTION_CONTRACT.json'
   if contract.is_file() and contract.stat().st_size<=262144:
    total=json.loads(contract.read_text()).get('updates_per_epoch')
    if type(total) is int and total>0:out['progress']['steps_per_epoch']=total
   # Some legacy workers only persist the global and in-epoch counters.  Once
   # the second epoch has started, recover the constant epoch length without
   # guessing from wall time or dataset size.  Reject inconsistent counters.
   if out['progress'].get('steps_per_epoch') is None:
    epoch=out['progress'].get('epoch');step=out['progress'].get('step_in_epoch')
    updates=out['progress'].get('optimizer_steps_executed')
    if type(epoch) is int and epoch>1 and type(step) is int and step>0 and type(updates) is int and updates>=step:
     prior=updates-step
     if prior>0 and prior%(epoch-1)==0:
      inferred=prior//(epoch-1)
      if inferred>=step:out['progress']['steps_per_epoch']=inferred
   out['progress'].update(legacy_posterior_phase(root,out['progress'],time.time()))
  else:
   marker=next((root/n for n in ['PROGRESS.json','evaluation/PROGRESS.json','eval/PROGRESS.json','diagnostics/PROGRESS.json'] if (root/n).is_file()),None)
   p=root/'CHILD_STATE.json'
   if marker is not None:
    assert marker.stat().st_size<=131072
    v=json.loads(marker.read_text());out['progress']={k:v.get(k) for k in ['completed_cells','planned_cells','completed_images','planned_images','phase','batches','planned_batches','config']}
    contract=root/'PROGRESS_CONTRACT.json'
    if out['progress'].get('planned_batches') is None and contract.is_file() and contract.stat().st_size<=131072:
     total=json.loads(contract.read_text()).get('planned_batches')
     if type(total) is int and total>0:out['progress']['planned_batches']=total
    out['progress']['age_s']=round(max(0,time.time()-marker.stat().st_mtime),1)
   elif p.is_file():
    assert p.stat().st_size<=131072
    out['progress']={'phase':json.loads(p.read_text()).get('phase')}
   else:
    # Legacy Paddle eval workers may expose only their bounded stdout log.
    # Report observed batches and the two audited fp32/quantized phases without
    # inventing throughput or a batch denominator.
    log=root/'stdout.log'
    if log.is_file():
     with log.open('rb') as stream:
      stream.seek(max(0,log.stat().st_size-131072));text=stream.read(131072).decode('utf-8','replace')
     seen=re.findall(r'Eval iter:\s*([0-9]+)',text)
     base=root/'evaluation';done=sum((base/name/'bbox.json').is_file() for name in ('fp32','quantized'))
     if seen and (base/'fp32').exists():
      phase='complete' if done>=2 else 'quantized' if (base/'fp32'/'bbox.json').is_file() else 'fp32'
      out['progress']={'completed_cells':done,'planned_cells':2,'phase':phase,
                       'batches':int(seen[-1]),'age_s':round(max(0,time.time()-log.stat().st_mtime),1)}
     else:out['progress']={'unavailable':'no progress marker'}
    else:out['progress']={'unavailable':'no progress marker'}
 except Exception as e:out['progress']={'error':str(e)}
 print(json.dumps(out))
'''
    def one(item):
        name, selected = item
        n = nodes[name]
        argv = [n['python'], '-', json.dumps([[r['id'],r['attempt_dir']] for r in selected])]
        if n['transport'] == 'ssh':
            argv = ['ssh', '-T', '-o', 'BatchMode=yes', '-o', 'ConnectTimeout=4', n['target'], shlex.join(argv)]
        try:
            result = subprocess.run(argv, input=code, text=True, capture_output=True, timeout=timeout)
            if result.returncode: raise RuntimeError(result.stderr[-300:])
            values = {x['id']:x['progress'] for line in result.stdout.splitlines() if (x:=json.loads(line))}
            for row in selected: row['progress'] = values.get(row['id'], {'error':'missing response'})
        except Exception as exc:
            for row in selected: row['progress'] = {'error':type(exc).__name__ + ': ' + str(exc)}
    with concurrent.futures.ThreadPoolExecutor(max_workers=10) as pool:
        list(pool.map(one, grouped.items()))


def markdown(data, view='all', include_waiting=False):
    from .registration import campaign_label
    def cell(value): return str(value if value is not None else '—').replace('|', '\\|').replace('\n', ' ')
    lines = ['# Research overview', '', data['time_kst'], '']
    if view in ('all','campaign'):
        lines += ['## Campaigns', '', '| Campaign | Type | Success | Running | Starting | 자원 대기 | 선행 대기 | 검증 대기 | 검증 실패 | Blocked/failed/unknown |', '|---|---|---:|---:|---:|---:|---:|---:|---:|---|']
        for g in data['campaigns']:
            c = g.get('external_counts') or g['counts']
            typed=g.get('counts_by_type') if not g.get('external') else None
            for kind,c in (typed or {'external' if g.get('external') else 'unclassified':c}).items():
                lines.append('| ' + ' | '.join(map(cell,[campaign_label(g),kind,c.get('succeeded',c.get('complete',0)),c.get('running',0),c.get('starting',0),c.get('resource_wait',0),c.get('dependency_wait',0),c.get('validation_wait',0),c.get('validation_failed',0),f"{c.get('blocked',0)}/{c.get('failed',0)}/{c.get('unknown',0)}"])) + ' |')
        if data['hardware']:
            lines += ['', '## Hardware campaigns', '', '| Campaign | Phase | build | test: RTL | test: board |', '|---|---|---|---|---|']
            for h in data['hardware']:
                stale = ' STALE' if h['age_s'] is None or h['age_s'] > 180 else ''
                lines.append(f"| {cell(campaign_label(h))} | {cell(h['phase'])}{stale} | {cell(h.get('build_status'))} | {cell(h.get('validation'))} | {cell((h.get('board') or {}).get('status'))} |")
    if view in ('all','gpu'):
        lines += ['', '## GPU assignments', '', '| Node:GPU | Enabled | °C | Used MiB | Snapshot age s | Jobs / campaigns |', '|---|---|---:|---:|---:|---|']
        for g in data['gpus']:
            names = '; '.join(x['job'] + ' [' + ','.join(x['campaigns']) + '] (' + x['status'] + ')' for x in g['jobs']) or 'No scheduler assignment'
            lines.append('| ' + ' | '.join(map(cell,[f"{g['node']}:{g['index']}",g['enabled'],g['temperature_c'],g['used_mib'],str(g['snapshot_age_s']) + (' STALE' if g['stale'] else ''),names])) + ' |')
    lines += ['', '## Active / waiting jobs', '']
    indexes = {(g['node'], g['uuid']):g['index'] for g in data['gpus']}
    for r in data['jobs']:
        if r['status'] not in ACTIVE | {'queued','blocked','failed'}: continue
        if r['status'] not in ACTIVE and not include_waiting and not data['filters']['job']: continue
        p = r.get('plan') or {}
        detail = r.get('progress') or (r['waiting']['dependencies'] if r.get('waiting') and r['waiting']['category']=='dependency_wait' else None) or p.get('reasons') or p.get('reason') or r.get('reason')
        label = r['waiting']['label'] if r.get('waiting') else r['status']
        location = (r.get('node','') + ':' + ','.join(str(indexes.get((r.get('node'),g),g)) for g in r.get('gpus',[]))) if r.get('node') else ''
        lines.append(f"- {r['id']}: {label} {location} [{','.join(r['campaigns'])}]" + (f" — {json.dumps(detail, ensure_ascii=False)}" if detail else ''))
        if data['filters']['job']:
            profiles = [r['resources'], *r['resource_variants']]
            lines.append('  Resources: ' + ' OR '.join(f"{x['gpu_count']} GPU × {x['vram_mib']} MiB VRAM; {x['ram_mib']} MiB RAM" for x in profiles))
    lines += ['', *['Note: ' + x for x in data['notes']], *['WARNING: ' + x for x in data['warnings']]]
    return '\n'.join(lines)


def add_arguments(p):
    p.add_argument('--view', choices=['all','gpu','campaign'], default='all')
    p.add_argument('--format', choices=['json','markdown'], default='markdown')
    for key in ('node','campaign','job','hardware-index'): p.add_argument('--' + key)
    p.add_argument('--live-progress', action='store_true', help='bounded SSH reads of exact attempt progress files; no probe or scheduling')
    p.add_argument('--include-waiting', action='store_true', help='expand pending/failed job reasons in Markdown (always present in JSON)')
    p.add_argument('--timeout', type=float, default=12)


def execute(args):
    if not 1 <= args.timeout <= 30: raise ValueError('timeout must be 1..30 seconds')
    result = collect(args.db, node=args.node, campaign=args.campaign, job=args.job,
                     hardware_index=args.hardware_index, live_progress=args.live_progress, timeout=args.timeout)
    print(json.dumps(result, ensure_ascii=False, indent=2) if args.format == 'json' else markdown(result,args.view,args.include_waiting))
