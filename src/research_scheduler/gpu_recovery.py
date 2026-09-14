"""Bounded startup failover; runtime failures and unknown workers are never retried."""
import copy


def quarantine_snapshots(snapshots, attempts):
    faults = {}
    for a in attempts:
        report = a.get('report', {})
        snap = snapshots.get(a['node'], {})
        if (a['status'] == 'failed' and a.get('released')
                and report.get('failure_class') == 'gpu_startup_unavailable'
                and report.get('boot_id') and report['boot_id'] == snap.get('boot_id')):
            faults.setdefault(a['node'], set()).update(report.get('failed_gpu_uuids', []))
    return {key: dict(value, gpu_startup_quarantine=sorted(faults.get(key, set())))
            for key, value in snapshots.items()}


def retry_spec(spec, report, attempt_count):
    """At most three extra infrastructure attempts; never change scientific settings."""
    if (report.get('failure_class') == 'experiment_oom'
            and report.get('status') == 'failed' and report.get('termination_verified')
            and spec.get('kind') in ('train', 'eval') and report.get('oom_node')):
        node = report['oom_node']
        updated = copy.deepcopy(spec)
        meta = updated.setdefault('metadata', {})
        excluded = set(meta.get('excluded_hosts', []))
        excluded.add(node)
        meta['excluded_hosts'] = sorted(excluded)
        history = meta.setdefault('oom_failovers', [])
        if any(r.get('attempt_count')==attempt_count and r.get('node')==node for r in history):
            return updated
        if len(history) >= 3:
            updated['max_attempts'] = attempt_count
            return updated
        history.append(dict(node=node, evidence=report.get('failure_evidence'), attempt_count=attempt_count))
        from .model_vram_policy import default_mib
        base=default_mib()
        if base is not None and report.get('oom_memory')=='gpu':
            previous=report.get('oom_reserved_vram_mib')
            if type(previous) is not int or previous<base:previous=base
            previous=max(previous,meta.get('vram_after_oom_mib',base))
            meta['vram_after_oom_mib']=previous+2048
            history[-1]['next_vram_mib']=meta['vram_after_oom_mib']
        # A host-local resume path is not portable. Use the user's approved fresh fallback.
        if updated.get('config', {}).get('resume_from'):
            meta['oom_resume_blocker'] = 'host-local checkpoint; fresh restart required on alternate host'
            # Resume-only wrappers must be explicitly adapted; never launch them with a null path.
            updated['max_attempts'] = attempt_count
            return updated
        if updated.get('hosts') and not (set(updated['hosts']) - excluded):
            meta['oom_retry_blocker'] = 'no remaining permitted host'
            updated['max_attempts'] = attempt_count
            return updated
        updated['max_attempts'] = max(updated['max_attempts'], attempt_count + 1)
        return updated
    if (report.get('failure_class') != 'gpu_startup_unavailable'
            or report.get('status') != 'failed' or report.get('ready')
            or spec.get('kind') != 'train'):
        return None
    used = spec.get('metadata', {}).get('gpu_startup_failovers', 0)
    if type(used) is not int or not 0 <= used < 3:
        return None
    updated = copy.deepcopy(spec)
    updated.setdefault('metadata', {})['gpu_startup_failovers'] = used + 1
    updated['max_attempts'] = max(updated['max_attempts'], attempt_count + 1)
    return updated
