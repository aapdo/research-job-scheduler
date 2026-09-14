"""Spread hardware work across eligible physical hosts before adding more jobs."""
import math


def build_pressure(node, snap, held, resources, now):
    """Projected dominant pressure, including reservations not yet in telemetry.

    Live CPU covers other users too. RAM's available value already includes RSS,
    so reserve only unobserved growth when the attempt heartbeat is fresh.
    Five-percentage-point bands avoid making tiny probe differences authoritative.
    No attempt, snapshot, node or resource dictionary is mutated here.
    """
    domain = node.get('physical_host', node['id'])
    own = [a for a in held if
           a['spec'].get('node_spec', {}).get('physical_host', a['node']) == domain]
    cpu_capacity = min(snap['cpu_count'], node.get('cpu_limit', snap['cpu_count']))
    cpu_reserved = sum(a['spec']['resources']['cpu'] for a in own)
    cpu_live = snap['cpu_percent'] / 100 * snap['cpu_count']
    cpu = (max(cpu_live, cpu_reserved) + resources['cpu']) / max(1, cpu_capacity)
    growth = 0
    for a in own:
        requested = a.get('admission_ram_mib', a['spec']['resources']['ram_mib'])
        report = a.get('report', {})
        rss = report.get('rss_mib')
        fresh = 0 <= now - report.get('heartbeat', 0) <= 120
        growth += (max(0, requested-rss) if fresh and isinstance(rss, (int, float))
                   and math.isfinite(rss) and rss >= 0 else requested)
    ram_available = min(snap['ram_available_mib'], node.get('ram_limit_mib', float('inf')))
    ram = (growth + resources['ram_mib']) / max(1, ram_available)
    # Match the existing work-filesystem reservation scope, not every host disk.
    disk_claims = sum(a['spec']['resources'].get('disk_mib', 0)
                      for a in held if a['node'] == node['id'])
    disk = (disk_claims + resources.get('disk_mib', 0)
            + node['policy']['min_free_disk_mib']) / max(1, snap['disk_free_mib'])
    slots = (sum(a['spec']['resources'].get('build_slots', 0) for a in own)
             + resources.get('build_slots', 0)) / max(1, node.get('rtl_build_slots', 0))
    components = dict(cpu=cpu, ram=ram, disk=disk, build_slots=slots)
    # Invalid telemetry must never make a candidate look exceptionally empty.
    if not all(math.isfinite(v) and v >= 0 for v in components.values()):
        return (float('inf'), float('inf')), components
    bands = [math.floor(v / .05 + 1e-9) for v in components.values()]
    return (max(bands), sum(bands)), components


def hardware_workload(node, held):
    domain=node.get('physical_host',node['id'])
    return len({a.get('job',a.get('id')) for a in held
                if a['spec'].get('node_spec',{}).get('physical_host',a['node'])==domain
                and (a['spec'].get('job_spec',{}).get('kind') in ('rtl_sim','rtl_build','rtl_ooc')
                     or a['spec']['resources'].get('build_slots',0))})


def placement_key(node, snap, held, resources, now, count, index, chosen, kind=None):
    legacy = (-node.get('admission_priority', 0), count, index, node['id'], chosen)
    if kind not in ('rtl_sim','rtl_build','rtl_ooc') and not resources.get('build_slots', 0):
        return (0, 0, *legacy)
    score, _ = build_pressure(node, snap, held, resources, now)
    return (hardware_workload(node,held), -node.get('admission_priority',0), *score, *legacy[1:])
