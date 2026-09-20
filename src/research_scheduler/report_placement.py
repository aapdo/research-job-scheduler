"""LAB4 placement for new reports; frozen legacy attempts retain their contract."""

import ast
import copy

from .schema import check


SAFE_REPORT_IMPORTS = {'json', 'gzip', 'os', 'pathlib', 'math', 'statistics', 'csv', 'collections',
                       'itertools', 'datetime', 're', 'typing'}
HOST_PATH_PREFIXES = ('/home/', '/data/', '/tmp/', '/opt/', '/workspace/', '/root/', '/usr/', '/var/')


def portable_inline_report(job, argv):
    """Whether an inline report needs no release working directory."""
    if (job.get('input_files') or job.get('assets') or job.get('env')
            or job.get('dataset') or job.get('dataset_path')):
        return False
    try:
        index = argv.index('-c')
    except (ValueError, AttributeError):
        return False
    if index + 2 != len(argv):
        return False
    try:
        tree = ast.parse(argv[index + 1])
    except (SyntaxError, TypeError):
        return False
    imports = {alias.name.split('.')[0] for item in ast.walk(tree)
               if isinstance(item, ast.Import) for alias in item.names}
    imports.update((item.module or '').split('.')[0] for item in ast.walk(tree)
                   if isinstance(item, ast.ImportFrom))
    if not imports <= SAFE_REPORT_IMPORTS:
        return False
    if any(isinstance(item, ast.Constant) and isinstance(item.value, str)
           and item.value.startswith(HOST_PATH_PREFIXES) for item in ast.walk(tree)):
        return False
    def absolute_values(obj):
        if isinstance(obj, str):
            return obj.startswith('/')
        if isinstance(obj, dict):
            return any(absolute_values(v) for v in obj.values())
        if isinstance(obj, list):
            return any(absolute_values(v) for v in obj)
        return False
    return not absolute_values(job.get('config', {}))


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
        if metadata.get('report_attempt_cwd'):
            check(portable_inline_report(job, profile['argv']),
                  'attempt-local report must be a self-contained inline report')
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
        check(portable_inline_report(value, argv),
              'report requires a prepared LAB4 runtime profile')
        index = argv.index('-c')
        profile = dict(argv=[(node or {}).get('python', 'python3'), '-c', argv[index + 1]],
                       cwd=(node or {}).get('work_root', '/tmp'),
                       resource_contract=copy.deepcopy(value['resources']))
    else:
        profile = copy.deepcopy(profile)
    if portable_inline_report(value, profile.get('argv', [])):
        # The runner creates the attempt directory before launching the child.
        # Inline reports consume only verified dependency paths, so a mutable or
        # retired release directory must never be their launch prerequisite.
        profile['cwd'] = (node or {}).get('work_root', profile['cwd'])
        metadata['report_attempt_cwd'] = True
    else:
        metadata.pop('report_attempt_cwd', None)
    metadata.pop('control_report_exception', None)
    metadata.pop('report_execution_dependency', None)
    metadata.update(report_role='report', report_execution='archive_host',
                    report_storage='lab4-direct-relay',
                    execution_profiles={'lab4': copy.deepcopy(profile)})
    value['hosts'] = ['lab4']
    return validate_report_policy(value)


def attempt_cwd(job, attempt_dir):
    """Resolve the launch cwd for an already validated report contract."""
    if job.get('metadata', {}).get('report_attempt_cwd'):
        # Registration validated portability before the per-node profile was
        # overlaid.  The overlay may add runtime env paths; those do not make
        # the inline program depend on its retired release cwd.
        check(job.get('kind') == 'analysis'
              and job.get('metadata', {}).get('report_execution') == 'archive_host',
              'attempt-local cwd is restricted to archive-host reports')
        return attempt_dir
    return job['cwd']


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
