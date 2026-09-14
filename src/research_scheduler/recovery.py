"""Explicit recovery authorities; historical attempts and artifacts stay intact."""
import hashlib
import json


def output_manifest_sha256(outputs):
    content = {name: {'sha256': value['sha256'], 'bytes': value['bytes']}
               for name, value in outputs.items()}
    return hashlib.sha256(json.dumps(content, sort_keys=True, separators=(',', ':')).encode()).hexdigest()


def frozen_initialization_valid(job, attempt):
    """Only an audited completed E0 initialization may become a lineage boundary."""
    authority = job['spec'].get('metadata', {}).get('frozen_initialization_authority')
    if not authority:
        return None
    if (job['status'] != 'succeeded' or not attempt or attempt['status'] != 'succeeded'
            or attempt['id'] != authority.get('attempt') or authority.get('source_epoch') != 0
            or not authority.get('evidence') or attempt.get('report', {}).get('returncode') != 0):
        return False
    outputs = attempt['report'].get('outputs', {})
    if not outputs:
        return False
    try:
        return output_manifest_sha256(outputs) == authority.get('outputs_sha256')
    except (KeyError, TypeError, ValueError):
        return False


def current_campaign_jobs(jobs, experiments, campaign):
    """Count an explicitly linked replacement instead of a historical failure.

    A malformed link remains an error rather than hiding the failed source.
    Artifact upload and publication use their own immutable attempt records.
    """
    by_id = {j['id']: j for j in jobs}
    selected = set(campaign['experiments'])
    selected.update(key for key, value in experiments.items()
                    if value.get('project', 'general') in campaign['projects'])
    result = {}
    for original in jobs:
        if original['experiment'] not in selected:
            continue
        chosen = original
        replacement = original['spec'].get('metadata', {}).get('recovery_replacement', {})
        target = by_id.get(replacement.get('job'))
        if (original['status'] == 'failed' and target and replacement.get('evidence')
                and target['spec'].get('metadata', {}).get('independent_restart', {}).get('original_job') == original['id']
                and original['spec']['kind'] == target['spec']['kind'] == 'train'
                and original['spec']['config'].get('plan_sha256')
                == target['spec']['config'].get('plan_sha256')):
            chosen = target
        result[chosen['id']] = chosen
    return list(result.values())
