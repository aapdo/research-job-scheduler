"""Campaign-level durable notifications with an optional secret webhook file."""
import hashlib
import fcntl
import json
import os
import stat
import time
import urllib.request
from pathlib import Path
from datetime import datetime, timedelta, timezone

from .schema import check, fields, identifier
from .store import dumps
from .registration import registration_fields


ALERT_STATES = {"complete", "error"}
ACTIVE_JOB_STATES = {"queued", "starting", "running", "unknown"}
ERROR_JOB_STATES = {"failed", "blocked", "unknown"}
FAILED_CAMPAIGN_RETENTION_S = 24 * 60 * 60


def campaign_processing_due(store, spec, now=None):
    """Keep history visible while retiring terminal campaigns from hot loops."""
    now = time.time() if now is None else now
    if not spec.get('enabled', True):
        return False
    row = store.db.execute(
        'SELECT state,details,updated FROM campaign_runtime WHERE id=?',
        (spec['id'],)).fetchone()
    if not row:
        return True
    if row['state'] in {'complete', 'cancelled'}:
        return False
    if row['state'] != 'error':
        return True
    details = json.loads(row['details'])
    since = details.get('_error_since')
    if not isinstance(since, (int, float)):
        alert = store.db.execute(
            "SELECT MAX(created) FROM notification_outbox WHERE campaign=? AND state='error'",
            (spec['id'],)).fetchone()[0]
        since = alert if isinstance(alert, (int, float)) else row['updated']
    return now - since < FAILED_CAMPAIGN_RETENTION_S


def ensure_tables(store):
    store.db.executescript("""
        CREATE TABLE IF NOT EXISTS campaigns(
            id TEXT PRIMARY KEY, spec TEXT NOT NULL, created REAL NOT NULL);
        CREATE TABLE IF NOT EXISTS campaign_runtime(
            id TEXT PRIMARY KEY, state TEXT NOT NULL, generation INTEGER NOT NULL,
            details TEXT NOT NULL, updated REAL NOT NULL);
        CREATE TABLE IF NOT EXISTS notification_outbox(
            id TEXT PRIMARY KEY, campaign TEXT NOT NULL, state TEXT NOT NULL,
            payload TEXT NOT NULL, status TEXT NOT NULL, attempts INTEGER NOT NULL DEFAULT 0,
            next_attempt REAL NOT NULL, created REAL NOT NULL, sent REAL, error TEXT NOT NULL DEFAULT '');
    """)


def campaign_spec(raw):
    value = json.loads(dumps(raw))
    fields(value, "id name rq projects experiments external enabled hf")
    if value.get("hf") is not None:
        from .artifacts import hf_spec
        value["hf"] = hf_spec(value["hf"])
    identifier(value["id"])
    for key in ("name", "rq"):
        check(isinstance(value.get(key), str) and value[key].strip(), key + " is required")
    for key in ("projects", "experiments"):
        value.setdefault(key, [])
        check(isinstance(value[key], list) and all(isinstance(v, str) and v for v in value[key]),
              key + " must contain strings")
        check(len(value[key]) == len(set(value[key])), "duplicate " + key)
    value.setdefault("external", False)
    value.setdefault("enabled", True)
    check(isinstance(value["external"], bool) and isinstance(value["enabled"], bool),
          "external/enabled must be boolean")
    if value["external"]:
        check(not value["projects"] and not value["experiments"],
              "external campaign cannot also select scheduler experiments")
        check(not value.get('hf'), 'external campaign must import its jobs before enabling automatic HF publication')
    else:
        check(value["projects"] or value["experiments"],
              "campaign needs a project or experiment")
    return value


def register_campaign(store, raw):
    value = campaign_spec(raw)
    with store.lock(), store.db:
        ensure_tables(store)
        known = set(store.specs("experiments"))
        check(set(value["experiments"]) <= known, "unknown explicit experiment")
        if value.get('hf'):
            experiments = store.specs('experiments')
            def selected(c):
                return set(c['experiments']) | {k for k,e in experiments.items() if e['project'] in c['projects']}
            for other in campaign_specs(store).values():
                if other['id'] != value['id'] and other.get('hf') and other['enabled']:
                    overlap = set(other['projects']) & set(value['projects']) or selected(other) & selected(value)
                    check(not overlap or other['hf'] == value['hf'],
                          'overlapping HF campaigns require the same destination')
        row = store.db.execute("SELECT spec,created FROM campaigns WHERE id=?", (value["id"],)).fetchone()
        if row is not None:
            old = json.loads(row[0])
            check({k:v for k,v in old.items() if k != "hf"} ==
                  {k:v for k,v in value.items() if k != "hf"},
                  "campaign already exists with another specification")
            store.db.execute("UPDATE campaigns SET spec=? WHERE id=?", (dumps(value), value["id"]))
            if old != value:
                store.event("campaign_hf_changed", value["id"], {"hf": value.get("hf")})
            return dict(value, **registration_fields(row[1]))
        created = time.time()
        store.db.execute("INSERT INTO campaigns VALUES(?,?,?)",
                         (value["id"], dumps(value), created))
        store.event("campaign_registered", value["id"],
                    {"projects": value["projects"], "experiments": value["experiments"],
                     "external": value["external"], **registration_fields(created)})
    return dict(value, **registration_fields(created))


def campaign_specs(store):
    ensure_tables(store)
    return {row["id"]: json.loads(row["spec"])
            for row in store.db.execute("SELECT * FROM campaigns ORDER BY id")}


def _job_observation(store, spec, snapshot=None):
    from .observation_snapshot import ObservationSnapshot
    snapshot = snapshot if snapshot is not None else ObservationSnapshot(store)
    experiments = snapshot.experiments
    selected = set(spec["experiments"])
    selected.update(key for key, value in experiments.items()
                    if value.get("project", "general") in spec["projects"])
    from .recovery import current_campaign_jobs
    from .waiting import summarize
    all_jobs = snapshot.jobs
    jobs = current_campaign_jobs(all_jobs, experiments, spec)
    counts = summarize(jobs, snapshot.jobs_by_id)
    errors = [dict(job=job["id"], status=job["status"], reason=job["reason"])
              for job in jobs if job["status"] in ERROR_JOB_STATES]
    if errors:
        state = "error"
    elif any(job["status"] in ACTIVE_JOB_STATES for job in jobs):
        state = "running"
    elif jobs and all(job["status"] in {"succeeded", "cancelled"} for job in jobs):
        state = "complete"
    else:
        state = "pending"
    publication = None
    if spec.get('hf'):
        from .artifacts import publication_summary
        publication = publication_summary(store, spec, snapshot=snapshot)
        if publication['errors']:
            errors.extend(publication['errors'])
            state = 'error'
        elif publication['counts']['pending'] and state == 'complete':
            state = 'running'
    attempts = snapshot.attempts
    ready = {a['job'] for a in attempts
             if a['status'] == 'running' and a.get('report', {}).get('ready')}
    chosen = {j['id'] for j in jobs}
    replacements = {j['id']: j['spec']['metadata']['recovery_replacement']['job']
                    for j in all_jobs if j['experiment'] in selected and j['id'] not in chosen
                    and j['spec'].get('metadata', {}).get('recovery_replacement', {}).get('job') in chosen}
    # Individual training recovery is independent of campaign-wide recovery.
    # Use the newest attempt only; a historical ready flag is not a live retry.
    train_recoveries = []
    for job in jobs:
        if (job['spec']['kind'] != 'train' or job['spec'].get('config', {}).get('smoke_only')
                or job['status'] != 'running'):
            continue
        history = snapshot.attempts_by_job.get(job['id'], [])
        if not history:
            continue
        latest = max(history, key=lambda a: (a.get('created', 0), a.get('id', '')))
        if not (latest.get('id') and latest['status'] == 'running' and latest.get('report', {}).get('ready')):
            continue
        sources = {job['id']} | {old for old, new in replacements.items() if new == job['id']}
        failures = [a for source in sources for a in snapshot.attempts_by_job.get(source, []) if a['status'] == 'failed'
                    and a.get('created', 0) < latest.get('created', 0)]
        if not failures:
            continue
        failed = max(failures, key=lambda a: (a.get('created', 0), a.get('id', '')))
        train_recoveries.append(dict(job=job['id'], attempt=latest['id'], node=latest.get('node'),
                                     failed_job=failed['job'], failed_attempt=failed.get('id')))
    return {"state": state, "counts": counts, "jobs": len(jobs),
            'train_recoveries': train_recoveries,
            'replacement_jobs': replacements,
            'recovery_ready': bool(ready & chosen) and not errors,
            "failed_jobs": [j['id'] for j in jobs if j['status'] == 'failed'],
            "confirmed_jobs": [j['id'] for j in jobs if j['status'] == 'succeeded'
                               or (j['status'] == 'running' and j['id'] in ready)],
            "experiments": len(selected), "errors": errors[:8], 'publication': publication}


def _message(spec, observation, occurred_at):
    icon = {"started": "🚀", "complete": "✅", "recovered": "🔄"}.get(observation['state'], "🚨")
    title = {"started": "시작", "complete": "완료", "recovered": "복구 · 정상 실행 재개"}.get(observation['state'], "오류 발생")
    labels = dict(succeeded='성공', complete='완료', failed='실패', blocked='차단',
                  queued='대기', resource_wait='자원 대기', dependency_wait='선행 대기',
                  starting='시작 중', running='실행 중', unknown='상태 불명',
                  cancelled='취소', artifact_error='결과물 전송 오류', published='업로드 완료',
                  pending='전송 대기', error='오류')
    counts = ", ".join(f"{labels.get(key, key)} {value}개" for key, value in sorted(observation.get("counts", {}).items()))
    timestamp = datetime.fromtimestamp(occurred_at, timezone(timedelta(hours=9))).strftime('%Y-%m-%d %H:%M:%S KST')
    lines = [f"{icon} 실험 캠페인 {title}", f"캠페인: {spec['name']} (`{spec['id']}`)",
             '발생 시각(감지 기준): ' + timestamp,
             "연구 질문: " + spec["rq"], "작업 현황: " + (counts or "외부 캠페인")]
    if observation['state'] == 'recovered':
        lines.append('이전 실패 작업의 실행 준비 완료 또는 성공을 확인했습니다. 전체 실험 완료 알림은 아닙니다.')
    if spec.get('hf'):
        h = spec['hf']
        lines.append('HF: https://huggingface.co/' + ('datasets/' if h['repo_type']=='dataset' else '') + h['repo_id'])
        publication = (observation.get('publication') or {}).get('counts', {})
        lines.append('결과물 업로드: ' + ', '.join(f'{labels.get(k,k)} {v}개' for k,v in publication.items()))
    for error in observation.get("errors", [])[:5]:
        reason = error.get('reason') or '상세 원인 미기록'
        reason = {'upstream failed; no evaluation result': '선행 작업 실패로 후속 작업 차단',
                  'HF Python/auth paths not configured on source node': '원본 서버에 HF 실행 환경·인증 경로가 등록되지 않음',
                  'HF upload retry budget exhausted; training remains successful': 'HF 업로드 재시도 소진(학습은 성공)'}.get(reason, reason)
        reason = reason.replace('\n', ' ')[:240]
        lines.append(f"- {error.get('job', '외부 작업')}: {labels.get(error.get('status'), '오류')} — {reason}")
    return "\n".join(lines)


def _record_observation(store, spec, observation, now):
    check(observation.get("state") in {"pending", "running", "complete", "error"},
          "invalid campaign observation")
    row = store.db.execute("SELECT state,generation,details FROM campaign_runtime WHERE id=?",
                           (spec["id"],)).fetchone()
    previous, generation = (row[0], row[1]) if row else (None, 0)
    old = json.loads(row[2]) if row else {}
    observation = dict(observation)
    train_recovery_notices = _record_train_recoveries(store, spec, observation, old, now)
    def activity(value):
        return any(value.get('counts', {}).get(k, 0) for k in ('starting','running','succeeded','failed','cancelled','complete'))
    # Legacy active/terminal campaigns are baselined, not announced retroactively.
    start_seen = old.get('_start_observed', activity(old))
    started = (not start_seen and observation['state']=='running'
               and any(observation.get('counts', {}).get(k,0) for k in ('starting','running')))
    # Reused successful prerequisites do not mean this new campaign has started.
    observation['_start_observed'] = bool(start_seen or started or observation['state'] in ('complete','error'))
    fingerprint = hashlib.sha256(dumps(sorted(observation.get('errors', []), key=dumps)).encode()).hexdigest()
    new_error = (observation['state']=='error' and previous=='error'
                 and old.get('_error_fingerprint', fingerprint) != fingerprint)
    observation['_error_fingerprint'] = fingerprint if observation['state']=='error' else None
    if observation['state'] == 'error':
        same_error = previous == 'error' and old.get('_error_fingerprint', fingerprint) == fingerprint
        since = old.get('_error_since') if same_error else None
        if not isinstance(since, (int, float)) and same_error:
            alert = store.db.execute(
                "SELECT MAX(created) FROM notification_outbox WHERE campaign=? AND state='error'",
                (spec['id'],)).fetchone()[0]
            since = alert if isinstance(alert, (int, float)) else now
        observation['_error_since'] = since if isinstance(since, (int, float)) else now
    else:
        observation['_error_since'] = None
    recovery = old.get('_pending_recovery')
    if observation['state'] == 'error':
        targets = set((recovery or {}).get('jobs', [])) | set(observation.get('failed_jobs', []))
        targets.update(e['job'] for e in observation.get('errors', [])
                       if e.get('job') and e.get('status') in ('failed','unknown','blocked'))
        recovery = dict(jobs=sorted(targets), generation=generation + int(previous != 'error'))
    elif previous == 'error' and recovery is None:
        # Upgrade an existing runtime without inventing a recovery from normal startup.
        recovery = dict(jobs=[e['job'] for e in old.get('errors', [])
                              if e.get('status') in ('failed','unknown','blocked') and e.get('job')], generation=generation)
    recovery_targets = {observation.get('replacement_jobs', {}).get(key, key)
                        for key in (recovery or {}).get('jobs', [])}
    recovered = bool(recovery and observation['state'] == 'running' and
                     ((recovery['jobs'] and recovery_targets <= set(observation.get('confirmed_jobs', [])))
                      or (not recovery['jobs'] and observation.get('recovery_ready') is True)))
    recovery_generation = recovery['generation'] if recovery else None
    if recovered or observation['state'] == 'complete':
        recovery = None
    observation['_pending_recovery'] = recovery
    if previous != observation["state"]:
        generation += 1
    store.db.execute("INSERT OR REPLACE INTO campaign_runtime VALUES(?,?,?,?,?)",
                     (spec["id"], observation["state"], generation, dumps(observation), now))
    alerts = ['started'] if started else []
    if recovered and not train_recovery_notices:
        alerts.append('recovered')
    if new_error or (previous != observation['state'] and observation['state'] in ALERT_STATES):
        alerts.append(observation['state'])
    if observation['state'] != 'error':
        store.db.execute("UPDATE notification_outbox SET status='superseded',error='campaign no longer in error' "
                         "WHERE campaign=? AND state='error' AND status IN ('pending','sending')", (spec['id'],))
    for alert in alerts:
        if alert=='complete' and store.db.execute(
                "SELECT 1 FROM notification_outbox WHERE campaign=? AND state='complete' LIMIT 1",
                (spec['id'],)).fetchone():
            # Keep the existing delivery/retry record even if observations briefly
            # leave complete and return. A new campaign uses a new campaign ID.
            continue
        token = ('first-execution' if alert=='started' else recovery_generation if recovered else
                 str(generation)+(':'+fingerprint if alert=='error' else ''))
        key = hashlib.sha256(f"{spec['id']}\0{token}\0{alert}".encode()).hexdigest()
        payload = {"text": _message(spec, dict(observation, state=alert), now)}
        store.db.execute("INSERT OR IGNORE INTO notification_outbox "
                         "(id,campaign,state,payload,status,next_attempt,created) VALUES(?,?,?,?,?,?,?)",
                         (key, spec["id"], alert, dumps(payload), "pending", now, now))
        store.event("campaign_alert_queued", spec["id"],
                    {"state": alert, "generation": generation, "notification": key})


def _record_train_recoveries(store, spec, observation, old, now):
    """One error-route message per campaign's newly confirmed recovery batch.

    At upgrade, catch up only the currently pending recovery episode. Older
    running retries are baselined, not replayed. Later failed/restarted attempts
    are detected even if a short failure fell between campaign observation polls.
    """
    candidates = observation.get('train_recoveries', [])
    seen = set(old.get('_train_recovery_seen', []))
    queued = 0
    if '_train_recovery_seen' not in old:
        pending = set((old.get('_pending_recovery') or {}).get('jobs', []))
        pending.update(e.get('job') for e in old.get('errors', []))
        seen.update(c['attempt'] for c in candidates
                    if c['job'] not in pending and c['failed_job'] not in pending)
    # Preserve attempt-level deduplication, but group delivery by campaign.
    # Legacy individually queued records must not be sent again on upgrade.
    for item in candidates:
        legacy = hashlib.sha256(f"{spec['id']}\0train-recovered\0{item['attempt']}".encode()).hexdigest()
        if store.db.execute('SELECT 1 FROM notification_outbox WHERE id=?', (legacy,)).fetchone():
            seen.add(item['attempt'])
    fresh = sorted({c['attempt']: c for c in candidates if c['attempt'] not in seen}.values(),
                   key=lambda c: (c['job'], c['attempt']))
    if fresh:
        token = dumps(sorted(item['attempt'] for item in fresh))
        key = hashlib.sha256(f"{spec['id']}\0train-recovery-batch\0{token}".encode()).hexdigest()
        timestamp = datetime.fromtimestamp(now, timezone(timedelta(hours=9))).strftime('%Y-%m-%d %H:%M:%S KST')
        text = '\n'.join([
            '🔄 캠페인 학습 복구 · 정상 실행 재개',
            f"캠페인: {spec['name']} (`{spec['id']}`)",
            f"복구된 학습: {len(fresh)}개",
            '확인 시각(감지 기준): '+timestamp,
            '실패 후 새 학습 실행의 running 및 ready 상태를 확인했습니다.',
            *[f"- 학습 작업: {item['job']} · {item['node'] or '서버 미확인'}" for item in fresh],
            '이번에 확인된 학습 복구를 캠페인별로 묶었습니다. 캠페인 전체 복구·완료 알림은 아닙니다.'])
        inserted = store.db.execute("INSERT OR IGNORE INTO notification_outbox "
            "(id,campaign,state,payload,status,next_attempt,created) VALUES(?,?,?,?,?,?,?)",
            (key, spec['id'], 'recovered', dumps(dict(text=text)),
             'pending', now, now)).rowcount
        if inserted:
            queued += 1
            for item in fresh:
                store.event('train_recovery_alert_queued', item['job'],
                            dict(campaign=spec['id'], notification=key, **item))
        seen.update(item['attempt'] for item in fresh)
    observation['_train_recovery_seen'] = sorted(seen)
    return queued


def slack_disabled():
    """Durable operator opt-out, including explicit legacy webhook overrides."""
    return (Path.home()/'.config/research-scheduler/slack-disabled').exists()


def webhook_path(explicit=None):
    """Persist via the user config directory; explicit empty string disables."""
    if slack_disabled(): return ''
    if explicit is not None:
        return str(explicit)
    if 'RS_SLACK_WEBHOOK_FILE' in os.environ:
        return os.environ['RS_SLACK_WEBHOOK_FILE']
    path = Path.home() / '.config/research-scheduler/slack-webhook'
    return str(path) if path.is_file() else ''


def _webhook(path):
    if not path:
        return None
    file = os.path.abspath(path)
    info = os.stat(file)
    check(info.st_uid == os.getuid(), "webhook file must be owned by the scheduler user")
    check(stat.S_IMODE(info.st_mode) & 0o077 == 0, "webhook file must not be group/world accessible")
    value = Path(file).read_text(encoding="utf-8").strip()
    check(value.startswith("https://hooks.slack.com/services/") and "\n" not in value,
          "invalid Slack webhook file")
    return value


def _send(url, payload, timeout=10):
    if slack_disabled():
        raise RuntimeError('Slack delivery disabled by operator')
    request = urllib.request.Request(url, data=dumps(payload).encode(),
                                     headers={"Content-Type": "application/json",
                                              "User-Agent": "research-job-scheduler"}, method="POST")
    with urllib.request.urlopen(request, timeout=timeout) as response:
        body = response.read(1024).decode(errors="replace").strip()
        if not 200 <= response.status < 300 or body.lower() != "ok":
            raise RuntimeError("Slack webhook returned a non-success response")


def route_webhook_path(event, explicit=None):
    if slack_disabled(): return ''
    if explicit is not None:
        return webhook_path(explicit)
    path = Path(os.environ.get('RS_SLACK_ROUTES_FILE', str(Path.home()/'.config/research-scheduler/slack-routes.json')))
    if not path.is_file():
        return webhook_path()
    info=path.stat()
    check(info.st_uid==os.getuid() and stat.S_IMODE(info.st_mode)&0o077==0,
          'route configuration must be private and owned by scheduler user')
    cfg=json.loads(path.read_text())
    check(cfg.get('version')==1 and isinstance(cfg.get('events'),dict),'invalid route configuration')
    if event == 'recovered':
        event = 'error'
    check(event in cfg['events'],'event route is not configured')
    return cfg['events'][event]


def deliver_outbox(store, webhook_file=None, sender=_send):
    if slack_disabled():
        return dict(enabled=False,queued=0,sent=0,failed=0,reason='user_disabled')
    # Serialize network deliveries separately from the scientific registry lock.
    # A slow sender must not allow another controller to reclaim its 60s lease.
    with store.path.with_suffix(store.path.suffix+'.notifications.lock').open('a') as lock:
        try:fcntl.flock(lock,fcntl.LOCK_EX|fcntl.LOCK_NB)
        except BlockingIOError:return dict(enabled=True,queued=0,sent=0,failed=0,busy=True)
        return _deliver_outbox(store,webhook_file,sender)


def _deliver_outbox(store, webhook_file=None, sender=_send):
    """Resolve each event route privately; failed delivery stays in the outbox."""
    now=time.time()
    with store.lock(),store.db:
        store.db.execute("UPDATE notification_outbox SET status='superseded',error='campaign no longer in error' "
                         "WHERE state='error' AND status IN ('pending','sending') AND campaign IN "
                         "(SELECT id FROM campaign_runtime WHERE state!='error')")
        pending=list(store.db.execute("SELECT * FROM notification_outbox WHERE status IN ('pending','sending') "
                     "AND next_attempt<=? ORDER BY created LIMIT 8",(now,)))
    urls={};config_error=None
    for event in {r['state'] for r in pending}:
        try:urls[event]=_webhook(route_webhook_path(event,webhook_file))
        except (OSError,ValueError,TypeError,KeyError) as exc:
            urls[event]=None;config_error=type(exc).__name__
    with store.lock(),store.db:
        due=[]
        for row in list(store.db.execute("SELECT * FROM notification_outbox WHERE status IN ('pending','sending') "
                                   "AND next_attempt<=? ORDER BY created LIMIT 8",(time.time(),))):
            if not urls.get(row['state']):continue
            store.db.execute("UPDATE notification_outbox SET status='sending',attempts=attempts+1,next_attempt=? WHERE id=?",
                             (time.time()+60,row['id']))
            due.append(row)
    sent=failed=0
    for row in due:
        current=store.db.execute('SELECT status FROM notification_outbox WHERE id=?',(row['id'],)).fetchone()
        if current['status']!='sending':continue
        try:sender(urls[row['state']],json.loads(row['payload']))
        except Exception as exc:
            failed+=1;attempts=int(row['attempts'])+1
            error=type(exc).__name__ + (':HTTP'+str(exc.code) if hasattr(exc,'code') else '')
            # Outbox-only acknowledgment uses SQLite's transaction serialization;
            # a scientific flock collision after HTTP success must not lose the ACK.
            with store.db:
                store.db.execute("UPDATE notification_outbox SET status='pending',next_attempt=?,error=? WHERE id=? AND status='sending'",
                    (time.time()+min(300,10*2**min(attempts,5)),error,row['id']))
                store.event('campaign_alert_failed',row['campaign'],dict(notification=row['id'],error=error,attempt=attempts))
        else:
            sent+=1
            with store.db:
                store.db.execute("UPDATE notification_outbox SET status='sent',sent=?,error='' WHERE id=?",(time.time(),row['id']))
                store.event('campaign_alert_sent',row['campaign'],dict(notification=row['id'],state=row['state']))
    enabled=any(urls.values()) if pending else bool(webhook_path(webhook_file))
    result=dict(enabled=enabled,queued=len(pending),sent=sent,failed=failed)
    if config_error:result['config_error']=config_error
    return result


def poll_campaigns(store, external_observations=None, webhook_file=None, sender=_send, campaign_ids=None):
    """Observe campaign transitions, enqueue alerts, and deliver due outbox rows."""
    external_observations = external_observations or {}
    now = time.time()
    with store.lock(), store.db:
        ensure_tables(store)
        specs = campaign_specs(store)
        if campaign_ids is not None:
            specs = {k:v for k,v in specs.items() if k in campaign_ids}
        specs = {k:v for k,v in specs.items() if campaign_processing_due(store,v,now)}
        from .observation_snapshot import ObservationSnapshot
        snapshot = ObservationSnapshot(store)
        for key, spec in specs.items():
            if not spec["enabled"]:
                continue
            if spec["external"]:
                observation = external_observations.get(key)
                if observation is None:
                    continue
            else:
                observation = _job_observation(store, spec, snapshot=snapshot)
            _record_observation(store, spec, observation, now)
    return deliver_outbox(store,webhook_file,sender)


def status(store):
    ensure_tables(store)
    specs = campaign_specs(store)
    for row in store.db.execute('SELECT id,created FROM campaigns'):
        specs[row['id']].update(registration_fields(row['created']))
    runtime = {row["id"]: {"state": row["state"], "generation": row["generation"],
                            "details": json.loads(row["details"]), "updated": row["updated"]}
               for row in store.db.execute("SELECT * FROM campaign_runtime")}
    outbox = [dict(row) for row in store.db.execute(
        "SELECT id,campaign,state,status,attempts,next_attempt,created,sent,error "
        "FROM notification_outbox ORDER BY created")]
    return {"campaigns": specs, "runtime": runtime, "outbox": outbox,
            "webhook_configured": bool(webhook_path())}
