"""Approved per-job hardware admission budgets; frozen jobs/attempts stay intact."""
import json


def effective_resources(job, node, resources):
    if job.get('kind') != 'rtl_build' or resources.get('gpu_count', 0):
        return resources
    profile = job.get('metadata', {}).get('hardware_resources_by_node', {}).get(node['id'], {})
    if profile:
        if set(profile) - {'ram_mib', 'disk_mib'} or any(type(v) is not int or not 1024 <= v <= 1048576 for v in profile.values()):
            raise ValueError('invalid approved hardware resource profile')
        resources = dict(resources, **profile)
    raw = node.get('labels', {}).get('build_ram_overrides_mib', '{}')
    overrides = json.loads(raw)
    value = overrides.get(job.get('id'))
    if value is None:
        return resources
    if type(value) is not int or not 1024 <= value <= 1048576:
        raise ValueError('invalid approved build RAM budget')
    return dict(resources, ram_mib=value)
