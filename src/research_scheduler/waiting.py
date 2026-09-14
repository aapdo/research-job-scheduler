"""Derived waiting categories; stored lifecycle states and scheduling stay intact."""
from collections import Counter


def waiting_detail(job, by_id):
    if job['status'] != 'queued':
        return None
    unmet = [dict(job=key, status=by_id.get(key, {}).get('status', 'missing'))
             for key in job['spec'].get('depends_on', [])
             if by_id.get(key, {}).get('status') != 'succeeded']
    return dict(category='dependency_wait' if unmet else 'resource_wait',
                label='선행 대기' if unmet else '자원 대기', dependencies=unmet)


def display_status(job, by_id):
    detail = waiting_detail(job, by_id)
    return detail['category'] if detail else job['status']


def summarize(jobs, by_id):
    return dict(Counter(display_status(job, by_id) for job in jobs))


def work_type(spec):
    """Presentation categories; never alter lifecycle or scheduling contracts."""
    kind=spec.get('kind')
    if spec.get('config',{}).get('smoke_only'):
        return 'support'
    if kind in ('train','eval'):return kind
    if kind in ('rtl_build','rtl_ooc'):return 'build'
    if kind in ('rtl_sim','board_test'):return 'test'
    return 'support'


def summarize_by_type(jobs, by_id):
    groups={}
    for job in jobs:
        groups.setdefault(work_type(job['spec']),[]).append(job)
    return {key:summarize(rows,by_id) for key,rows in groups.items()}
