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
    return (direction=='upload' and node.get('labels',{}).get('drain_upload_only')=='approved'
            and attempt.get('node')==node['id'] and frozen.get('id')==node['id']
            and frozen.get('target')==node.get('target'))


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
