"""Local read-only research dashboard. Never dispatches or writes scheduler DBs."""
from __future__ import annotations

import argparse
import gzip
import json
import math
import os
import re
import sqlite3
import sys
import threading
import time
from collections import Counter, deque
from datetime import datetime, timezone, timedelta
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path
from urllib.parse import urlsplit, parse_qs

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / 'src'))
from research_scheduler.overview import collect
from research_scheduler.planner import base_health
from research_scheduler.registration import campaign_label
from research_scheduler.waiting import work_type

DB = '/home/jy/experiments/research_scheduler/state.db'
RTL_DB = '/home/jy/experiments/rtl_scheduler_pilot_20260908_1638_r2/state.db'
INDEX = '/home/jy/experiments/hardware_campaigns/INDEX.json'
STATIC = Path(__file__).parent / 'dist'


class GPUAverages:
    """Physical GPU usage regardless of ownership, idle periods or job changes.

    No browser state or scheduler writes; a service restart starts fresh samples.
    """
    def __init__(self, window_s=180):
        self.window_s = window_s
        self.series = {}

    def update(self, gpus, now=None):
        now = time.time() if now is None else now
        present = set()
        for gpu in gpus:
            key = (gpu['node'], gpu['uuid'])
            present.add(key)
            state = self.series.get(key)
            if state is None:
                state = dict(samples=deque(), last_stamp=None)
                self.series[key] = state
            samples = state['samples']
            while samples and samples[0][0] < now-self.window_s:
                samples.popleft()
            # Use probe start, not receipt time; cached probes count only once.
            stamp, value = gpu.get('sample_started_at'), gpu.get('utilization_percent')
            valid = lambda v: isinstance(v, (int, float)) and not isinstance(v, bool) and math.isfinite(v)
            if (valid(stamp) and valid(value) and 0 <= value <= 100
                    and not gpu.get('stale') and now-self.window_s <= stamp <= now
                    and (state['last_stamp'] is None or stamp > state['last_stamp'])):
                samples.append((stamp, value))
                state['last_stamp'] = stamp
            count = len(samples)
            span = samples[-1][0]-samples[0][0] if count > 1 else 0
            gpu['utilization_average'] = dict(
                percent=round(sum(v for _, v in samples)/count, 1) if count >= 2 else None,
                window_s=self.window_s, sample_count=count, sample_span_s=round(span, 1),
                partial=span < self.window_s*0.8,
                state='ready' if count >= 2 else 'collecting',
                last_sample_at=samples[-1][0] if samples else None)
        for key in set(self.series)-present:
            del self.series[key]


class Inbox:
    """Dashboard-owned data only. Never mutate experiment or Slack outbox records."""
    def __init__(self, path):
        self.path=Path(path);self.path.parent.mkdir(parents=True,exist_ok=True)
        with self.connect() as db:
            db.executescript('CREATE TABLE IF NOT EXISTS inbox(id TEXT PRIMARY KEY,category TEXT,created REAL,payload TEXT,read_at REAL);'
                'CREATE TABLE IF NOT EXISTS cursor(category TEXT PRIMARY KEY,created REAL,id TEXT);')

    def connect(self):
        db=sqlite3.connect(self.path,timeout=5);db.row_factory=sqlite3.Row;return db

    def ingest(self, events):
        with self.connect() as db:
            for event in sorted(events,key=lambda e:(e['created'],e['id'])):
                old=db.execute('SELECT created,id FROM cursor WHERE category=?',(event['category'],)).fetchone()
                if old and (event['created'],event['id'])<=(old['created'],old['id']):continue
                db.execute('INSERT OR IGNORE INTO inbox VALUES(?,?,?,?,NULL)',
                           (event['id'],event['category'],event['created'],json.dumps(clean(event),ensure_ascii=False)))
                db.execute('INSERT OR REPLACE INTO cursor VALUES(?,?,?)',
                           (event['category'],event['created'],event['id']))
            db.execute('DELETE FROM inbox WHERE read_at IS NOT NULL AND read_at<=?',(time.time()-86400,))

    def rows(self):
        with self.connect() as db:
            db.execute('DELETE FROM inbox WHERE read_at IS NOT NULL AND read_at<=?',(time.time()-86400,))
            return [dict(json.loads(r['payload']),read_at=r['read_at']) for r in db.execute(
                'SELECT payload,read_at FROM inbox ORDER BY created DESC LIMIT 500')]

    def mark_read(self, key):
        with self.connect() as db:
            return bool(db.execute('UPDATE inbox SET read_at=COALESCE(read_at,?) WHERE id=?',
                                  (time.time(),key)).rowcount)

    def mark_read_many(self, keys):
        if not isinstance(keys,list) or not 1<=len(keys)<=500 or any(not isinstance(k,str) or not k or len(k)>180 for k in keys):
            raise ValueError('Invalid notification IDs')
        keys=list(dict.fromkeys(keys));stamp=time.time();placeholders=','.join('?' for _ in keys)
        with self.connect() as db:
            found=[r['id'] for r in db.execute('SELECT id FROM inbox WHERE id IN ('+placeholders+')',keys)]
            db.execute('UPDATE inbox SET read_at=COALESCE(read_at,?) WHERE id IN ('+placeholders+')',[stamp,*keys])
        return dict(ok=True,ids=found,read_at=stamp)


def database(path):
    db = sqlite3.connect(Path(path).resolve().as_uri() + '?mode=ro', uri=True, timeout=3)
    db.row_factory = sqlite3.Row
    db.execute('PRAGMA query_only=ON')
    return db


def clean(value):
    if isinstance(value, str):
        value = re.sub(r'https://hooks\.slack\.com/\S+', '[redacted webhook]', value)
        value = re.sub(r'\bhf_[A-Za-z0-9]{12,}\b', '[redacted token]', value)
        value = re.sub(r'(?i)(?:Bearer\s+)[A-Za-z0-9._-]+', 'Bearer [redacted]', value)
        return value[:3000]
    if isinstance(value, dict):
        return {k: clean(v) for k, v in value.items()}
    if isinstance(value, list):
        return [clean(v) for v in value]
    if isinstance(value, float) and not math.isfinite(value):
        return None
    return value


def select(value, keys):
    return {key: value.get(key) for key in keys.split()}


def server_health(path, category):
    """Read cached probes only. Do not run refresh()/tick()/recovery actions."""
    now = time.time()
    db = database(path)
    try:
        nodes = {r['id']: json.loads(r['spec']) for r in db.execute('SELECT id,spec FROM nodes')}
        snaps = {r['node']: json.loads(r['data']) for r in db.execute('SELECT node,data FROM snapshots')}
        health = {r['node']: json.loads(r['data']) for r in db.execute('SELECT node,data FROM node_health')}
        active = []
        for r in db.execute("SELECT a.id,a.job,a.node,a.status,a.spec,a.report,j.spec AS job_spec "
                            "FROM attempts a JOIN jobs j ON j.id=a.job "
                            "WHERE a.status IN ('starting','running','unknown')"):
            spec = json.loads(r['spec']); report = json.loads(r['report'])
            kind = work_type(json.loads(r['job_spec']))
            active.append(dict(id=r['id'], job=r['job'], node=r['node'], status=r['status'],
                               work_type=kind, gpu_count=len(spec.get('gpus', [])),
                               heartbeat_age_s=now-report['heartbeat'] if report.get('heartbeat') else None))
        result = []
        for key, node in nodes.items():
            if category == 'model' and not node.get('gpus'): continue
            snap = snaps.get(key, {}); state = health.get(key, {})
            stamp = snap.get('received_at', snap.get('time'))
            age = max(0, now-stamp) if isinstance(stamp, (float, int)) else None
            stale = age is None or age > node['policy']['max_snapshot_age_s']
            reason = base_health(node, snap, now)
            if category=='hardware' and snap.get('memory_pressure_full_avg10') is not None:
                if snap['memory_pressure_full_avg10']>0.5:
                    reason='Memory PSI full avg10 exceeds hardware admission threshold 0.5%'
            if state.get('phase') in ('unavailable', 'ssh_retrying'):
                reason = state.get('reason') or state['phase']
            status = 'disabled' if not node['enabled'] else 'stale' if stale else 'attention' if reason else 'healthy'
            cg = snap.get('memory_cgroup', {})
            row = dict(id=key, category=category, enabled=node['enabled'], status=status,
                       reason=reason or state.get('reason', ''), recovery_phase=state.get('phase'),
                       snapshot_age_s=age, snapshot_at=stamp,
                       max_snapshot_age_s=node['policy']['max_snapshot_age_s'],
                       stable_polls=snap.get('stable_polls'), required_polls=node['policy']['stable_polls'],
                       jobs=[a for a in active if a['node'] == key],
                       max_jobs=node.get('max_jobs'), cpu_limit=node.get('cpu_limit'),
                       cgroup_memory=select(cg, 'current_mib limit_mib raw_headroom_mib'),
                       **select(snap, 'cpu_percent cpu_count ram_available_mib ram_total_mib disk_free_mib '
                                      'd_state read_ok memory_pressure_full_avg10'))
            result.append(row)
        return result, active
    finally:
        db.close()


def lifecycle(path, category):
    db=database(path)
    try:
        notifications=[]
        for r in db.execute("SELECT id,campaign,state,payload,status,created,sent FROM notification_outbox "
                            "WHERE state IN ('started','complete','error','recovered') "
                            "AND status!='superseded' ORDER BY created DESC LIMIT 100"):
            notifications.append(dict(id=category+':'+r['id'],campaign=r['campaign'],kind=r['state'],
                category=category,created=r['created'],delivery=r['status'],text=json.loads(r['payload']).get('text','')))
        recovery={r['campaign']:r['latest'] for r in db.execute(
            "SELECT campaign,MAX(created) AS latest FROM notification_outbox WHERE state='recovered' GROUP BY campaign")}
        failures={r[0] for r in db.execute("SELECT DISTINCT job FROM attempts WHERE status='failed'")}
        return notifications,recovery,failures
    finally:db.close()


def model_resource_waiting(jobs):
    """Count unique queued train/eval jobs, not campaign or GPU memberships."""
    return len({j['id'] for j in jobs if j['work_type'] in ('train', 'eval')
                and j['status'] == 'queued'
                and (j.get('waiting') or {}).get('category') == 'resource_wait'})


def payload(db=DB, rtl_db=RTL_DB, index=INDEX):
    started = time.time()
    # collect() uses query-only SQLite and bounded reads of exact attempt markers.
    view = collect(db, hardware_index=index, live_progress=True, timeout=10)
    notifications,recovery,failed_jobs=lifecycle(db,'model')
    try:
        events,hw_recovery,_=lifecycle(rtl_db,'hardware');notifications+=events
    except (sqlite3.Error,OSError):hw_recovery={}
    model_health, _ = server_health(db, 'model')
    try:
        hardware_health, hardware_jobs = server_health(rtl_db, 'hardware')
    except (OSError, sqlite3.Error, KeyError):
        hardware_health, hardware_jobs = [], []
        view['warnings'].append('Hardware health data unavailable')
    jobs = []
    for job in view['jobs']:
        row = select(job, 'id status work_type campaigns display_status waiting dependencies reason progress '
                          'node heartbeat_age_s')
        plan = job.get('plan') or {}
        row['admission'] = select(plan, 'decision reason reasons node')
        row['recovered']=job['id'] in failed_jobs and job['status'] in ('running','succeeded')
        if row.get('progress'):
            row['progress'] = select(row['progress'], 'epoch planned_epochs optimizer_steps_executed '
                                      'step_in_epoch steps_per_epoch completed_cells planned_cells status phase age_s unavailable error member members training_iterations_executed phase_inferred phase_elapsed_s phase_evidence')
        jobs.append(row)
    by_id = {j['id']: j for j in jobs}
    gpu_rows = []
    for gpu in view['gpus']:
        g = select(gpu, 'node index uuid enabled temperature_c used_mib total_mib utilization_percent '
                         'snapshot_at sample_started_at snapshot_age_s stale held_jobs_total')
        g['assigned_attempts'] = sorted({a['attempt'] for a in gpu['jobs']})
        g['jobs'] = []
        if not g['enabled']:g['gpu_health']='disabled';g['gpu_reason']='신규 배정 금지'
        elif g['stale']:g['gpu_health']='stale';g['gpu_reason']='최근 GPU 측정값 없음'
        elif g.get('temperature_c') is None:g['gpu_health']='attention';g['gpu_reason']='온도 미측정'
        elif g['temperature_c']>=85:g['gpu_health']='attention';g['gpu_reason']='85°C 이상 · 신규 배정 금지'
        elif g['temperature_c']>=80:g['gpu_health']='attention';g['gpu_reason']='80°C 이상 · 추가 공유 제한'
        else:g['gpu_health']='healthy';g['gpu_reason']='온도·측정 정상 · VRAM/예약 조건은 작업별 판정'
        for a in gpu['jobs']:
            j = dict(by_id.get(a['job'], dict(id=a['job'], status=a['status'])))
            j['execution_node'] = a.get('execution_node', gpu['node'])
            g['jobs'].append(j)
        gpu_rows.append(g)
    campaigns = []
    for c in view['campaigns']:
        if c.get('external') or c.get('unregistered_project'): continue
        row = select(c, 'id name counts counts_by_type recorded_state registered_at completed_at')
        row['label'] = campaign_label(c)
        row['recovered_at']=recovery.get(c['id'])
        row['jobs'] = [by_id[k] for k in c.get('job_ids', []) if k in by_id]
        campaigns.append(row)
    hardware = []
    for h in view['hardware']:
        row = select(h, 'id phase validation build_status registered_at completed_at age_s')
        row['label'] = campaign_label(h)
        row['recovered_at']=hw_recovery.get(h['id'])
        row['board_status'] = (h.get('board') or {}).get('status')
        row['jobs'] = [j for j in hardware_jobs if j['job'].startswith(h['id'] + '-')]
        hardware.append(row)
    campaigns.sort(key=lambda c: (c.get('registered_at') or 0, c['id']), reverse=True)
    hardware.sort(key=lambda c: (c.get('registered_at') or 0, c['id']), reverse=True)
    visible = [g for g in gpu_rows if g['enabled']]
    active = [j for j in jobs if j['status'] in ('running', 'starting', 'unknown')]
    data = dict(generated_at=time.time(), generated_kst=datetime.now(timezone(timedelta(hours=9))).isoformat(),
                collected_at=started, collection_seconds=round(time.time()-started, 2), read_only=True,
                refresh_seconds=30, gpus=gpu_rows, campaigns=campaigns, hardware=hardware,
                hardware_jobs=hardware_jobs, health=model_health+hardware_health,
                waiting=[j for j in jobs if j['status'] in ('queued','failed','unknown','blocked')],
                notifications=sorted(notifications,key=lambda n:n['created'],reverse=True)[:100],
                warnings=view['warnings'],
                summary=dict(eligible_gpus=len(visible), assigned_gpus=sum(bool(g['jobs']) for g in visible),
                             active_jobs=len(active), active_by_type=dict(Counter(j['work_type'] for j in active)),
                             model_resource_waiting=model_resource_waiting(jobs),
                             attention_servers=sum(h['enabled'] and h['status']!='healthy' for h in model_health+hardware_health)))
    return clean(data)


class Cache:
    def __init__(self, args):
        self.args = args; self.lock = threading.Lock(); self.data = None; self.error = None
        self.inbox=Inbox(args.inbox)
        self.gpu_averages=GPUAverages()

    def run(self):
        while True:
            started = time.monotonic()
            try:
                data = payload(self.args.db, self.args.rtl_db, self.args.hardware_index)
                self.gpu_averages.update(data['gpus'])
                self.inbox.ingest(data.pop('notifications', []))
                with self.lock: self.data, self.error = data, None
                rss_mib = int(Path('/proc/self/statm').read_text().split()[1])*os.sysconf('SC_PAGE_SIZE')/(1024*1024)
                print(json.dumps(dict(event='dashboard_refresh',seconds=round(time.monotonic()-started,3),
                                      rss_mib=round(rss_mib,1))),flush=True)
            except Exception as exc:
                # Do not expose arbitrary exception contents, paths, or credentials.
                with self.lock: self.error = type(exc).__name__
                print('dashboard refresh failed: '+type(exc).__name__, flush=True)
            # Processing time belongs to the interval, not an extra delay.
            time.sleep(max(1.0,30-(time.monotonic()-started)))

    def snapshot(self):
        with self.lock:
            if self.data is None:
                return dict(status='loading', read_only=True, error=self.error), 503
            data = dict(self.data, served_at=time.time(), refresh_error=self.error)
            data['notifications']=self.inbox.rows()
            return data, 200


class Handler(BaseHTTPRequestHandler):
    server_version = 'ResearchDashboard'
    def setup(self):
        super().setup(); self.connection.settimeout(10)

    def reply(self, status, raw, mime, compress=False):
        compressed=compress and 'gzip' in self.headers.get('Accept-Encoding','').lower()
        if compressed:raw=gzip.compress(raw,compresslevel=1,mtime=0)
        self.send_response(status)
        self.send_header('Content-Type', mime)
        self.send_header('Content-Length', str(len(raw)))
        if compress:self.send_header('Vary','Accept-Encoding')
        if compressed:self.send_header('Content-Encoding','gzip')
        self.send_header('Cache-Control', 'no-store')
        self.send_header('X-Content-Type-Options', 'nosniff')
        self.send_header('Referrer-Policy', 'no-referrer')
        self.send_header('Content-Security-Policy', "default-src 'self'; script-src 'self'; style-src 'self'; "
                         "connect-src 'self'; img-src 'self' data:; object-src 'none'; base-uri 'none'; frame-ancestors 'none'")
        self.end_headers()
        if self.command != 'HEAD': self.wfile.write(raw)

    def do_GET(self):
        path = urlsplit(self.path).path
        if path == '/api/state':
            data, code = self.server.cache.snapshot()
            if parse_qs(urlsplit(self.path).query).get('view')==['dashboard']:
                # UI uses per-campaign/per-GPU jobs, never this duplicate index.
                data={k:v for k,v in data.items() if k!='waiting'}
            return self.reply(code, json.dumps(data, ensure_ascii=False, allow_nan=False,separators=(',',':')).encode(), 'application/json; charset=utf-8',compress=True)
        files = {'/': ('index.html','text/html'), '/app.js': ('app.js','text/javascript'),
                 '/styles.css': ('styles.css','text/css')}
        if path not in files:
            return self.reply(404, b'Not found', 'text/plain')
        name, mime = files[path]
        return self.reply(200, (STATIC/name).read_bytes(), mime+'; charset=utf-8')

    do_HEAD = do_GET
    def readonly(self):
        self.reply(405, b'Read-only service', 'text/plain')
    def do_POST(self):
        if self.path not in ('/api/notifications/read','/api/notifications/read-many'):return self.readonly()
        origin=self.headers.get('Origin')
        if origin and urlsplit(origin).netloc!=self.headers.get('Host'):
            return self.reply(403,b'Origin mismatch','text/plain')
        try:
            length=int(self.headers.get('Content-Length','0'))
            if not 0<length<=(100000 if self.path.endswith('/read-many') else 1024):raise ValueError()
            data=json.loads(self.rfile.read(length))
            if self.path.endswith('/read-many'):
                result=self.server.cache.inbox.mark_read_many(data['ids'])
                return self.reply(200,json.dumps(result).encode(),'application/json')
            key=data['id']
            if not isinstance(key,str) or len(key)>180:raise ValueError()
        except (ValueError,KeyError,TypeError):return self.reply(400,b'Invalid notification','text/plain')
        found=self.server.cache.inbox.mark_read(key)
        return self.reply(200 if found else 404,b'{"ok":true}' if found else b'{"ok":false}', 'application/json')
    do_PUT = do_PATCH = do_DELETE = readonly
    def log_message(self, fmt, *args):
        pass  # No request strings or tokens in service logs.


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('--host', default='0.0.0.0'); parser.add_argument('--port', type=int, default=32767)
    parser.add_argument('--db', default=DB); parser.add_argument('--rtl-db', default=RTL_DB)
    parser.add_argument('--hardware-index', default=INDEX)
    parser.add_argument('--inbox',default='/home/jy/experiments/research_dashboard_20260911/inbox.db')
    args = parser.parse_args()
    from research_scheduler import planner
    print(json.dumps(dict(event='dashboard_planner_loaded',source=planner.__file__,
                          closure_free_lineage=hasattr(planner,'lineage_error'))),flush=True)
    cache = Cache(args)
    server = ThreadingHTTPServer((args.host,args.port),Handler); server.cache=cache
    threading.Thread(target=cache.run, daemon=True).start()
    print(f'Read-only dashboard listening on {args.host}:{args.port}',flush=True)
    server.serve_forever()


if __name__=='__main__': main()
