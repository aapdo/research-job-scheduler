"""Derived waiting categories; stored lifecycle states and scheduling stay intact."""
from collections import Counter


def execution_validation_state(job, validation_states=None):
    metadata=job['spec'].get('metadata',{})
    profile=metadata.get('execution_preparation_catalog')
    if not profile or metadata.get('execution_profiles'):
        return None
    states=list((validation_states or {}).get(profile, ()))
    failed={'failed','validation_failed','verification_failed','cancelled'}
    return 'validation_failed' if states and all(state in failed for state in states) else 'validation_wait'


def waiting_detail(job, by_id, validation_states=None):
    if job['status'] != 'queued':
        return None
    unmet = [dict(job=key, status=by_id.get(key, {}).get('status', 'missing'))
             for key in job['spec'].get('depends_on', [])
             if by_id.get(key, {}).get('status') != 'succeeded']
    if unmet:
        return dict(category='dependency_wait',label='선행 대기',dependencies=unmet)
    validation=execution_validation_state(job,validation_states)
    if validation:
        return dict(category=validation,label='검증 실패' if validation=='validation_failed' else '검증 대기',
                    dependencies=[])
    return dict(category='resource_wait',label='자원 대기',dependencies=[])


def display_status(job, by_id, validation_states=None):
    detail = waiting_detail(job, by_id, validation_states)
    return detail['category'] if detail else job['status']


def summarize(jobs, by_id, validation_states=None):
    return dict(Counter(display_status(job, by_id, validation_states) for job in jobs))


def work_type(spec):
    """Presentation categories; never alter lifecycle or scheduling contracts."""
    kind=spec.get('kind')
    if spec.get('config',{}).get('smoke_only'):
        return 'support'
    if kind in ('train','eval'):return kind
    if kind in ('rtl_build','rtl_ooc'):return 'build'
    if kind in ('rtl_sim','board_test'):return 'test'
    return 'support'


def summarize_by_type(jobs, by_id, validation_states=None):
    groups={}
    for job in jobs:
        groups.setdefault(work_type(job['spec']),[]).append(job)
    return {key:summarize(rows,by_id,validation_states) for key,rows in groups.items()}
