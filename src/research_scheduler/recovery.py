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
        kinds = (original['spec']['kind'], target['spec']['kind']) if target else (None, None)
        restart = target['spec'].get('metadata', {}).get('independent_restart', {}) if target else {}
        train_match = (kinds == ('train', 'train')
                       and original['spec']['config'].get('plan_sha256')
                       == target['spec']['config'].get('plan_sha256'))
        eval_keys = ('mode', 'arm', 'epoch', 'beta_override')
        eval_match = (kinds == ('eval', 'eval') and target['status'] == 'succeeded'
                      and original['spec'].get('metadata', {}).get('plan_sha256')
                      == target['spec'].get('metadata', {}).get('plan_sha256')
                      and all(original['spec']['config'].get(key) == target['spec']['config'].get(key)
                              for key in eval_keys)
                      and restart.get('output_sha256'))
        support_match = (kinds == ('analysis', 'analysis') and target['status'] == 'succeeded'
                         and original['spec'].get('config') == target['spec'].get('config')
                         and restart.get('output_sha256'))
        if (original['status'] == 'failed' and target and replacement.get('evidence')
                and restart.get('original_job') == original['id']
                and (train_match or eval_match or support_match)):
            chosen = target
        result[chosen['id']] = chosen
    return list(result.values())
