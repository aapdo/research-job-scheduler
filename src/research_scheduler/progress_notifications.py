"""Ten-minute read-only fleet snapshots, queued durably to the progress route."""
import json
import re
import sqlite3
import time
from pathlib import Path

from .notifications import ensure_tables
from .store import dumps
from .waiting import work_type
from .registration import campaign_label


def hardware_allocations(data):
    databases=set();rows=[]
    for item in data.get('hardware',[]):
        try:databases.add(json.loads(Path(item['state_file']).read_text())['scheduler_database'])
        except (OSError,KeyError,ValueError):continue
    for path in sorted(databases):
        try:
            with sqlite3.connect(Path(path).resolve().as_uri()+'?mode=ro',uri=True) as db:
                db.execute('PRAGMA query_only=ON')
                for job,node,state,spec in db.execute("SELECT a.job,a.node,a.status,j.spec FROM attempts a JOIN jobs j ON j.id=a.job WHERE a.status IN ('running','starting','unknown')"):
                    rows.append(dict(job=job,node=node,status=state,work_type=work_type(json.loads(spec))))
        except sqlite3.Error:
            rows.append(dict(job='조회 실패',node=Path(path).parent.name,status='unknown'))
    return rows


def _legacy_messages(data, assigned_hardware=None, limit=28000):
    a=data['allocation']
    visible_gpus=[g for g in data['gpus'] if g.get('enabled',False)]
    a=dict(a,total_gpus=len(visible_gpus),assigned_gpus=sum(bool(g.get('jobs')) for g in visible_gpus))
    lines=['📊 실험 진행 현황 — '+data['time_kst'],
           f"GPU 배정 {a['assigned_gpus']}/{a['total_gpus']}개 · 실행/초기화/상태확인 중 작업 {a['active_jobs']}개",
           '※ 스케줄러 예약 기준이며 외부 GPU 작업은 별도입니다. 캠페인 수치는 중복될 수 있습니다.',
           '', '*GPU별 배정*']
    for g in sorted(data['gpus'],key=lambda g:(g['node'],g['index'])):
        if not g.get('enabled',False):continue
        jobs='; '.join(j['job']+' ('+j['status']+')' for j in g['jobs']) or '미배정'
        flags=[]
        if not g['enabled']:flags.append('신규배정 금지')
        if g.get('stale'):flags.append('상태 캐시 오래됨')
        if g.get('temperature_c') is not None:flags.append(str(g['temperature_c'])+'°C')
        lines.append(f"• {g['node']} GPU{g['index']}: {jobs}"+(' ['+', '.join(flags)+']' if flags else ''))
    labels=[('succeeded','완료'),('running','실행'),('starting','초기화'),('resource_wait','자원대기'),
            ('dependency_wait','선행대기'),('failed','실패'),('blocked','차단'),('unknown','상태불명'),('cancelled','취소')]
    lines+=['','*캠페인별 진행*']
    for c in data['campaigns']:
        counts=c.get('external_counts') if c.get('external') else c['counts']
        counts=counts or {}
        status=', '.join(f'{label} {counts.get(k,0)}' for k,label in labels if counts.get(k,0)) or c.get('recorded_state') or '대기'
        lines.append('• '+c['id']+': '+status)
    lines+=['','*하드웨어 실제 작업 배정*']
    for row in assigned_hardware or []:lines.append('• '+row['node']+': '+row['job']+' ('+row['status']+')')
    if not assigned_hardware:lines.append('• 현재 실행 중인 하드웨어 작업 없음')
    lines+=['','*하드웨어 캠페인*']
    for h in data.get('hardware',[]):
        lines.append('• '+h['id']+': '+h['phase']+' / RTL '+str(h.get('validation',{}))+
                     ' / 빌드 '+str(h.get('build_status'))+' / 보드 '+str((h.get('board') or {}).get('status')))
    for warning in data.get('warnings',[]):lines.append('⚠ '+warning)
    result=[];part=''
    for line in lines:
        line=re.sub(r'https://hooks\.slack\.com/services/\S+','[webhook redacted]',line)
        if len(part)+len(line)+1>limit:
            result.append({'text':part});part='📊 진행 현황 (계속)\n'
        part+=line+'\n'
    if part:result.append({'text':part})
    return result


def messages(data, assigned_hardware=None, limit=9000):
    """Native Slack tables; split on row boundaries below Slack's 10k-cell limit."""
    from datetime import datetime
    from .visibility import expired_complete
    try: now = datetime.fromisoformat(data['time_kst']).timestamp()
    except ValueError: now = time.time()
    def clean(value):
        return re.sub(r'https://hooks\.slack\.com/services/\S+', '[webhook redacted]', str(value))

    visible_gpus=[g for g in data['gpus'] if g.get('enabled',False)]
    a=dict(data['allocation'],total_gpus=len(visible_gpus),
           assigned_gpus=sum(bool(g.get('jobs')) for g in visible_gpus))
    summary=(f"GPU 배정 {a['assigned_gpus']}/{a['total_gpus']}개 · "
             f"실행/초기화/상태확인 중 작업 {a['active_jobs']}개")
    if a.get('active_by_type') is not None:
        counts=a['active_by_type']
        summary+=(f" (train {counts.get('train',0)} · eval {counts.get('eval',0)} · 보조 {counts.get('support',0)})")
    gpu=[]
    for g in sorted(visible_gpus,key=lambda g:(g['node'],g['index'])):
        flags=[]
        if not g['enabled']:flags.append('신규배정 금지')
        if g.get('stale'):flags.append('상태 캐시 오래됨')
        jobs=g['jobs']
        gpu.append([g['node'],f"GPU{g['index']}",
                    '\n'.join(j['job']+(' [실행: '+j['execution_node']+']'
                        if j.get('execution_node',g['node'])!=g['node'] else '') for j in jobs) or '미배정',
                    '\n'.join(j.get('work_type','job')+' / '+j['status'] for j in jobs) or '—',
                    f"{g['temperature_c']}°C" if g.get('temperature_c') is not None else '미확인',
                    ', '.join(flags) or '—'])
    labels=[('succeeded','완료'),('running','실행'),('starting','초기화'),('resource_wait','자원대기'),
            ('dependency_wait','선행대기'),('failed','실패'),('blocked','차단'),('unknown','상태불명'),('cancelled','취소')]
    campaigns=[]
    auxiliary=[]
    model_statuses=[('succeeded','finish'),('running','run'),('failed','err'),
                    ('resource_wait','res-wait'),('dependency_wait','dep-wait')]
    model_header=['캠페인']+[f'{kind}-{label}' for kind in ('train','eval') for _,label in model_statuses]
    for c in data['campaigns']:
        if c['id'] in {'cssa-main','cssa-anchor','cssa-context','cssa-followup',
                       'picodet-s-cssa60','cssa-recovery'}:
            continue  # User-hidden from periodic Slack only, not execution/history.
        if expired_complete(c.get('recorded_state'), c.get('completed_at'), now):
            continue
        counts=(c.get('external_counts') if c.get('external') else c['counts']) or {}
        typed=c.get('counts_by_type') if not c.get('external') else None
        if typed and any(kind in typed for kind in ('train','eval')):
            campaigns.append([campaign_label(c)]+[typed[kind].get(key,0) if kind in typed else '—'
                              for kind in ('train','eval') for key,_ in model_statuses])
        elif not typed:
            # Preserve visibility without guessing whether legacy/external
            # aggregate counts are train, eval, or support work.
            campaigns.append([campaign_label(c)]+['—']*(len(model_header)-1))
        for kind,row_counts in (typed or {}).items():
            if kind in ('train','eval'):
                # Do not silently fold initialization, unknown, blocked or
                # cancelled states into run/err, or discard them.
                row_counts={k:v for k,v in row_counts.items()
                            if k not in dict(model_statuses) and v}
                if not row_counts:continue
                kind += ' 추가 상태'
            auxiliary.append([campaign_label(c),'보조(smoke·준비·보고서)' if kind=='support' else kind,sum(row_counts.values())]+[row_counts.get(k,0) for k,_ in labels]+[c.get('recorded_state') or '—'])
    def hardware_status(status, kind):
        return ('building' if kind=='build' else 'testing' if kind in ('test','rtl','board','validation')
                or 'test' in kind else status) if status=='running' else status
    hardware=[[r['node'],r.get('work_type','미분류'),r['job'],
               hardware_status(r['status'],r.get('work_type',''))] for r in assigned_hardware or []]
    latest_hardware=sorted(
        [h for h in data.get('hardware',[])
         if not expired_complete(h.get('phase'), h.get('completed_at'), now)],
        key=lambda h:(h.get('registered_at') or 0,h['id']),reverse=True)[:15]
    stages=[[campaign_label(h),{'build_running':'building','board_running':'testing',
                               'validation_running':'testing'}.get(h['phase'],h['phase']),
             hardware_status(h.get('build_status') or '—','build'),
             hardware_status((h.get('board') or {}).get('status') or '—','test')]
            for h in latest_hardware]
    auxiliary_header=['캠페인','유형','합계']+[label for _,label in labels]+['캠페인 상태']
    sections=[('GPU별 배정',['서버','GPU','작업','상태','온도','비고'],gpu),
              ('캠페인별 train / eval 진행',model_header,campaigns)]
    if auxiliary:
        sections.append(('보조 작업 및 추가 상태',auxiliary_header,auxiliary))
    sections += [('하드웨어 실제 작업 배정',['실행 서버','유형','작업','상태'],hardware),
              ('하드웨어 build / test · 최신 15개',['캠페인','단계','build','test: board'],stages)]
    payloads=[]
    for title,header,rows in sections:
        header=list(map(clean,header)); batches=[]; batch=[header]; size=sum(map(len,header))
        for row in rows or [['현재 해당 작업 없음']+['—']*(len(header)-1)]:
            row=list(map(clean,row)); length=sum(map(len,row))
            if length+sum(map(len,header))>min(limit,9000):
                raise ValueError('progress table row exceeds Slack cell budget')
            if len(batch)>=100 or size+length>min(limit,9000):
                batches.append(batch);batch=[header];size=sum(map(len,header))
            batch.append(row);size+=length
        batches.append(batch)
        for index,batch in enumerate(batches):
            heading=clean(f"📊 {title} — {data['time_kst']}"+(f' ({index+1}/{len(batches)})' if len(batches)>1 else ''))
            intro=heading
            if not title.startswith('하드웨어'):
                intro+='\n'+summary+'\n※ 스케줄러 예약 기준 · 외부 GPU 작업 별도 · 캠페인 수치 중복 가능'
            if not payloads:
                intro='========\n'+intro
            blocks=[{'type':'section','text':{'type':'plain_text','text':intro}},
                    {'type':'table','column_settings':[{'is_wrapped':True} for _ in header],
                     'rows':[[{'type':'raw_text','text':cell} for cell in row] for row in batch]}]
            # Full plain-text fallback also supports screen readers and notification previews.
            fallback=intro+'\n'+'\n'.join(' | '.join(row) for row in batch)
            payloads.append({'text':fallback,'blocks':blocks})
    for warning in data.get('warnings',[]):
        payloads.append({'text':clean('⚠ '+warning)})
    return payloads


def initialize(store):
    ensure_tables(store)
    with store.db:
        store.db.execute('CREATE TABLE IF NOT EXISTS progress_clock(id INTEGER PRIMARY KEY, next_due REAL, sequence INTEGER)')
        store.db.execute('INSERT OR IGNORE INTO progress_clock VALUES(1,0,0)')


def enqueue(store, payloads, now, interval=600):
    with store.lock(),store.db:
        next_due,sequence=store.db.execute('SELECT next_due,sequence FROM progress_clock WHERE id=1').fetchone()
        if now<next_due:return False
        sequence+=1
        # A newer snapshot supersedes undelivered old progress, not lifecycle alerts.
        store.db.execute("UPDATE notification_outbox SET status='superseded',error='newer progress snapshot' "
                         "WHERE campaign='fleet-progress' AND status='pending'")
        for i,payload in enumerate(payloads):
            store.db.execute('INSERT INTO notification_outbox(id,campaign,state,payload,status,next_attempt,created) VALUES(?,?,?,?,?,?,?)',
                             (f'progress-{sequence}-{i}','fleet-progress','progress',dumps(payload),'pending',now,now))
        store.db.execute('UPDATE progress_clock SET next_due=?,sequence=? WHERE id=1',(now+interval,sequence))
        return True
