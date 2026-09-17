"""Narrow publication-only permissions for a user-drained source executor."""
import json


def host_resource_reservations(node, held):
    """Account sibling containers against one host's CPU/RAM headroom."""
    peers=json.loads(node.get('labels',{}).get('reservation_peer_nodes','[]'))
    domain=node.get('physical_host',node['id'])
    return [a for a in held if a['node'] in {node['id'], *peers}
            or a['spec'].get('node_spec',{}).get('physical_host',a['node'])==domain]


def source_upload_allowed(attempt, node, direction):
    frozen=attempt.get('spec',{}).get('node_spec',{})
    labels=node.get('labels',{})
    # A GPU-runtime quarantine must not strand completed results on the source.
    # This grants only CPU publication once ordinary SSH/health gates recover;
    # a user-drained node without explicit upload approval remains excluded.
    publication_only=(labels.get('drain_upload_only')=='approved'
                      or (not node.get('enabled') and bool(labels.get('gpu_runtime_quarantine'))))
    return (direction=='upload' and publication_only
            and attempt.get('node')==node['id'] and frozen.get('id')==node['id']
            and frozen.get('target')==node.get('target'))


def publication_probe_due(node, health, now):
    """Retry a quarantined source without readmitting its scientific GPU jobs."""
    return (not node.get('enabled') and bool(node.get('labels',{}).get('gpu_runtime_quarantine'))
            and health.get('phase') == 'unavailable'
            and now >= health.get('next_publication_probe_at', 0))


def publication_probe_health(node, old, stable_polls, healthy, now):
    """Independent CPU-publication recovery; never changes node.enabled."""
    if node.get('enabled') or not node.get('labels',{}).get('gpu_runtime_quarantine'):
        return None
    if not (publication_probe_due(node, old, now) or old.get('phase') == 'publication_only'):
        return None
    state = dict(old)
    if healthy and (old.get('phase') == 'publication_only'
                    or stable_polls >= node['policy']['stable_polls']):
        state.update(phase='publication_only', last_success_at=now,
                     next_publication_probe_at=now + 60)
    else:
        state.update(phase='unavailable', next_publication_probe_at=now + (2 if healthy else 60))
    return state


def epoch_publication_allowed(job, node):
    config=job.get('config',{});resources=job.get('resources',{})
    try: sources=json.loads(node.get('labels',{}).get('drain_epoch_sources','[]'))
    except (TypeError,ValueError):return False
    return (node.get('labels',{}).get('drain_upload_only')=='approved'
            and job.get('kind')=='prepare' and job.get('id','').startswith('CSSA_EPOCH_')
            and config.get('mode')=='publish_epoch_checkpoint'
            and config.get('source',{}).get('source_attempt') in sources
            and job.get('hosts')==[node['id']] and resources.get('gpu_count')==0
            and resources.get('cpu',999)<=1 and resources.get('ram_mib',9999)<=512)
