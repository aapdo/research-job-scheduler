"""LAB4 placement for new reports; frozen legacy attempts retain their contract."""

import ast
import copy

from .schema import check


def is_report_job(job):
    metadata = job.get("metadata", {})
    return (job.get("kind") == "analysis" and (
        metadata.get("report_role") == "report"
        or "REPORT" in job.get("id", "").upper()
        or "report" in job.get("name", "").lower()
    ))


def validate_report_policy(job):
    """Reject new control-host reports unless an explicit exceptional reason is recorded."""
    if not is_report_job(job):
        return job
    metadata = job.get("metadata", {})
    if metadata.get('report_execution') == 'archive_host':
        check(job.get('hosts') == ['lab4'], 'report archive host must be LAB4')
        profiles = metadata.get('execution_profiles', {})
        check(set(profiles) == {'lab4'}, 'LAB4 report execution profile required')
        profile = profiles['lab4']
        check(all(key in profile for key in ('argv', 'cwd', 'resource_contract')),
              'LAB4 report profile must pin argv, cwd and resources')
        check(profile['resource_contract'] == job['resources'], 'report resource contract differs')
        check(job['resources']['gpu_count'] == 0, 'report is a CPU job')
        return job
    exception = metadata.get("control_report_exception")
    if exception is not None:
        check(isinstance(exception, str) and exception.strip(),
              "control_report_exception must contain an operational reason")
        return job
    check(metadata.get("report_execution") == "dependency_host",
          "new report jobs must execute on a dependency host or declare control_report_exception")
    dependency = metadata.get("report_execution_dependency")
    check(isinstance(dependency, str) and dependency in job.get("depends_on", []),
          "report_execution_dependency must name one of the report dependencies")
    check(dependency not in job.get("order_only_dependencies", []),
          "report execution dependency must provide artifacts, not order only")
    profiles = metadata.get("execution_profiles", {})
    check(isinstance(profiles, dict) and profiles,
          "dependency-host report requires verified per-host execution profiles")
    check(set(job.get("hosts", [])) == set(profiles),
          "report hosts must exactly match its execution profiles")
    check(job.get("resources", {}).get("gpu_count") == 0,
          "report-on-execution-host is a CPU job")
    for host, profile in profiles.items():
        check(isinstance(host, str) and isinstance(profile, dict), "invalid report execution profile")
        check(all(key in profile for key in ("argv", "cwd", "resource_contract")),
              "report execution profile must pin argv, cwd and resource_contract")
        check(profile["resource_contract"] == job["resources"],
              "report execution profile resource contract differs from the report")
    return job


def on_archive_host(job, node=None):
    """Use a LAB4 profile, or relocate a self-contained stdlib Python report."""
    if not is_report_job(job):
        return job
    value = copy.deepcopy(job)
    metadata = value.setdefault('metadata', {})
    profile = metadata.get('execution_profiles', {}).get('lab4')
    if profile is None:
        argv = value.get('argv', [])
        check(len(argv) == 3 and argv[1] == '-c' and 'python' in argv[0].split('/')[-1]
              and not value.get('input_files') and not value.get('assets')
              and not value.get('env') and not value.get('dataset') and not value.get('dataset_path'),
              'report requires a prepared LAB4 runtime profile')
        tree = ast.parse(argv[2])
        safe = {'json', 'gzip', 'os', 'pathlib', 'math', 'statistics', 'csv', 'collections',
                'itertools', 'datetime', 're', 'typing'}
        imports = {alias.name.split('.')[0] for item in ast.walk(tree)
                   if isinstance(item, ast.Import) for alias in item.names}
        imports.update((item.module or '').split('.')[0] for item in ast.walk(tree)
                       if isinstance(item, ast.ImportFrom))
        check(imports <= safe, 'report requires a prepared LAB4 runtime profile')
        check(not any(isinstance(item, ast.Constant) and isinstance(item.value, str)
                      and item.value.startswith('/') for item in ast.walk(tree)),
              'report contains host-local literals; prepare a LAB4 profile')
        def absolute_values(obj):
            if isinstance(obj, str):
                return obj.startswith('/')
            if isinstance(obj, dict):
                return any(absolute_values(v) for v in obj.values())
            if isinstance(obj, list):
                return any(absolute_values(v) for v in obj)
            return False
        check(not absolute_values(value.get('config', {})),
              'report inputs must use dependency placeholders or a LAB4 profile')
        profile = dict(argv=[(node or {}).get('python', 'python3'), '-c', argv[2]],
                       cwd=(node or {}).get('work_root', '/tmp'),
                       resource_contract=copy.deepcopy(value['resources']))
    metadata.pop('control_report_exception', None)
    metadata.pop('report_execution_dependency', None)
    metadata.update(report_role='report', report_execution='archive_host',
                    report_storage='lab4-direct-relay',
                    execution_profiles={'lab4': copy.deepcopy(profile)})
    value['hosts'] = ['lab4']
    return validate_report_policy(value)


def on_dependency_host(job, dependency, profiles):
    """Return a report spec whose runtime is pinned for each possible producer host."""
    value = copy.deepcopy(job)
    value.setdefault("metadata", {}).update(
        report_role="report",
        report_execution="dependency_host",
        report_execution_dependency=dependency,
        execution_profiles=copy.deepcopy(profiles),
    )
    value["hosts"] = sorted(profiles)
    return validate_report_policy(value)


def required_dependency_host(job, successful):
    metadata = job.get("metadata", {})
    if metadata.get('report_execution') == 'archive_host':
        return 'lab4'
    if metadata.get("report_execution") != "dependency_host":
        return None
    dependency = metadata.get("report_execution_dependency")
    attempt = successful.get(dependency)
    return attempt.get("node") if attempt else None
