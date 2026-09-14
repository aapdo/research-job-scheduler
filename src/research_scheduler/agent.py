"""Standalone stdlib remote helper. Read-only probe needs no remote installation.

Launched attempts receive an immutable copy of this file and a detached runner.
The agent is NOT a sandbox: only trusted operators may register commands.
"""
import csv
import contextlib
import fcntl
import hashlib
import io
import json
import math
import os
import re
import shutil
import subprocess
import sys
import time
import traceback
from pathlib import Path


class UncertainExecution(RuntimeError):
    """Keep reservations when a launched process might still own hardware."""


def validate_rtl(directory, kind, contract):
    """Validate real attempt-local artifacts, not a producer's success flag."""
    directory = Path(directory).resolve()

    def artifact(name):
        path = (directory / name).resolve()
        if not path.is_relative_to(directory) or not path.is_file():
            raise ValueError('missing/escaping validation artifact: ' + name)
        return path

    receipt = {'status': 'pass', 'kind': kind, 'artifacts': {}}

    def record(name):
        path = artifact(name)
        receipt['artifacts'][name] = digest(path)
        return path

    if kind in ('rtl_ooc', 'rtl_build'):
        timing = record(contract['timing_report']).read_text()
        match = re.search(r'(?m)^\s*WNS\(ns\)[^\n]*\n\s*-+[^\n]*\n([^\n]+)', timing)
        if not match:
            raise ValueError('Vivado timing summary missing')
        numbers = [float(v) for v in match[1].split()]
        if (len(numbers) != 12 or not all(math.isfinite(v) for v in numbers)
                or min(numbers[i] for i in (0, 4, 8)) < 0
                or any(numbers[i] != 0 for i in (1, 2, 5, 6, 9, 10))
                or numbers[3] <= 0 or numbers[7] <= 0 or numbers[11] <= 0):
            raise ValueError('Vivado setup/hold/pulse-width gate failed')
        receipt.update(wns_ns=numbers[0], whs_ns=numbers[4], wpws_ns=numbers[8])
        route = record(contract['route_report']).read_text()
        counts = []
        for label in ('routable nets', 'fully routed nets', 'nets with routing errors'):
            found = re.search(r'# of ' + label + r'\.*\s*:\s*(\d+)\s*:', route)
            if not found:
                raise ValueError('Vivado routing summary missing: ' + label)
            counts.append(int(found[1]))
        if counts[0] <= 0 or counts[0] != counts[1] or counts[2] != 0:
            raise ValueError('Vivado routing gate failed')
        if contract.get('bitstream') and record(contract['bitstream']).stat().st_size == 0:
            raise ValueError('empty bitstream')
    elif kind == 'rtl_sim':
        log = record(contract['log']).read_text()
        marker = contract['pass_marker']
        if (not any(line.strip() == marker or line.strip().startswith(marker + ' ') for line in log.splitlines())
                or re.search(r'(?m)^\s*(?:FATAL|ERROR)(?::|\s)', log)):
            raise ValueError('simulation PASS/error gate failed')
    elif kind == 'board_test':
        for pair in contract['comparisons']:
            name = pair['capture']
            record(name)
            if receipt['artifacts'][name] != pair['expected_sha256']:
                raise ValueError('board capture mismatch: ' + name)
    else:
        raise ValueError('unknown RTL validation kind')
    return receipt


def atomic_json(path, value):
    path = Path(path)
    temp = path.with_name(path.name + ".tmp." + str(os.getpid()))
    with temp.open("x") as f:
        json.dump(value, f, ensure_ascii=False, allow_nan=False)
        f.flush()
        os.fsync(f.fileno())
    os.replace(temp, path)


def digest(path):
    h = hashlib.sha256()
    with Path(path).open("rb") as stream:
        for block in iter(lambda: stream.read(1024 * 1024), b""):
            h.update(block)
    return h.hexdigest()


def process(pid, proc_root='/proc'):
    try:
        text = Path(proc_root, str(pid), "stat").read_text()
        parts = text[text.rfind(")") + 2:].split()
        return {"state": parts[0], "ppid": int(parts[1]), "pgrp": int(parts[2]), "start": parts[19]}
    except (OSError, ValueError, IndexError):
        return None


def gpu_process_owners(registrations, pids, proc_root='/proc'):
    """Attribute visible GPU PIDs only to a live, boot/start-verified runner tree."""
    root=Path(proc_root)
    boot=(root/'sys/kernel/random/boot_id').read_text().strip()
    anchors={}
    for row in registrations:
        try:
            state=json.loads((Path(row['attempt_dir'])/'state.json').read_text())
            info=process(state['runner_pid'],proc_root)
            if (state['attempt']==row['id'] and state['boot_id']==boot and info
                    and info['start']==state['runner_start'] and info['state']!='Z'):
                anchors[state['runner_pid']]=row['id']
        except (OSError,ValueError,KeyError):pass
    result={}
    for pid in pids:
        original=process(pid,proc_root);cursor=pid;seen=set()
        while cursor and cursor not in seen:
            seen.add(cursor)
            if cursor in anchors:
                current=process(pid,proc_root)
                if original and current and original['start']==current['start']:
                    result[pid]=anchors[cursor]
                break
            info=process(cursor,proc_root)
            if not info:break
            cursor=info['ppid']
    return result


def registered_d_processes(registrations, proc_root='/proc'):
    """Only registered runners/descendants; PID reuse never establishes ownership."""
    root = Path(proc_root)
    infos = {int(p.name): info for p in root.iterdir() if p.name.isdigit()
             and (info := process(p.name, proc_root))}
    blocked = [pid for pid, info in infos.items() if info['state'] == 'D']
    if not blocked: return [], 0
    boot = (root/'sys/kernel/random/boot_id').read_text().strip()
    known = {r['id']: r['attempt_dir'] for r in registrations}
    anchors = {}
    for key, directory in known.items():
        try:
            state = json.loads((Path(directory)/'state.json').read_text())
            pid = state['runner_pid']
            if (state['attempt'] == key and state['boot_id'] == boot and pid in infos
                    and infos[pid]['start'] == state['runner_start']):
                anchors[pid] = key
        except (OSError, ValueError, KeyError):
            pass
    owned = []
    for pid in blocked:
        cursor, visited, owner = pid, set(), None
        while cursor in infos and cursor not in visited:
            visited.add(cursor)
            if cursor in anchors:
                owner = anchors[cursor]; break
            # Also recognize orphaned descendants retaining the immutable
            # attempt ID/path. No environment values are returned or logged.
            try:
                env = dict(x.split(b'=', 1) for x in (root/str(cursor)/'environ').read_bytes().split(b'\0') if b'=' in x)
                key = env.get(b'RS_ATTEMPT_ID', b'').decode()
                if key in known and env.get(b'RS_ATTEMPT_DIR', b'').decode() == known[key]:
                    owner = key; break
            except (OSError, UnicodeError):
                pass
            cursor = infos[cursor]['ppid']
        current = process(pid, proc_root)
        if owner and current and current['state'] == 'D' and current['start'] == infos[pid]['start']:
            owned.append(dict(pid=pid, start=current['start'], attempt=owner))
    return owned, len(blocked)-len(owned)


def dstate_observation(candidates, boot_id, previous, now):
    """Three minutes of consecutive valid observations of the same process."""
    prior = {(r['pid'], r['start'], r['attempt']): r for r in previous.get('d_state_tracks', [])}
    continuous = (previous.get('boot_id') == boot_id and
                  0 < now-previous.get('d_state_sample_at', -1e30) <= 60)
    tracks = []
    for candidate in candidates:
        key = (candidate['pid'], candidate['start'], candidate['attempt'])
        since = prior[key]['since'] if continuous and key in prior else now
        tracks.append(dict(candidate, since=since, duration_s=max(0, now-since)))
    sustained = [r for r in tracks if r['duration_s'] >= 180]
    return dict(d_state=len(sustained), d_state_pids=[r['pid'] for r in sustained],
                d_state_observed=len(tracks), d_state_tracks=tracks,
                d_state_sample_at=now, boot_id=boot_id,
                d_state_policy='registered-process-180s-v1')


def group_alive(pgid):
    return any((p := process(x.name)) and p["pgrp"] == pgid and p["state"] != "Z"
               for x in Path("/proc").iterdir() if x.name.isdigit())


def process_tree_rss_mib(root_pid):
    """RSS of a scheduler-owned child and descendants across nested groups."""
    infos = {}
    for entry in Path("/proc").iterdir():
        if entry.name.isdigit() and (info := process(entry.name)):
            infos[int(entry.name)] = info
    tree = {root_pid}
    changed = True
    while changed:
        before = len(tree)
        tree.update(pid for pid, info in infos.items() if info["ppid"] in tree)
        changed = len(tree) != before
    total_kib = 0
    for pid in tree:
        info = infos.get(pid)
        if not info or info["state"] == "Z":
            continue
        try:
            for line in Path("/proc", str(pid), "status").read_text().splitlines():
                if line.startswith("VmRSS:"):
                    total_kib += int(line.split()[1])
                    break
        except (OSError, ValueError, IndexError):
            # A process may exit between /proc reads. Missing attribution makes
            # admission more conservative at the controller, never less.
            return None
    return total_kib / 1024


def cpu_sample():
    ticks = [int(x) for x in Path("/proc/stat").read_text().splitlines()[0].split()[1:9]]
    return sum(ticks), ticks[3] + ticks[4]


def dataset_status(path):
    """Lightweight path/access check, not a dataset content or split audit."""
    result = {"path": path, "available": False}
    try:
        entry = Path(path)
        directory = entry.is_dir()
        result["available"] = ((directory or entry.is_file())
                               and os.access(path, os.R_OK | (os.X_OK if directory else 0)))
        if not result["available"]:
            result["reason"] = "path missing or not readable/searchable"
    except OSError as exc:
        result["reason"] = str(exc)
    return result


def cgroup_memory_headroom(host_available_mib, maximum, current, stat, active_file_fraction=0):
    """Admission estimate including clean inactive cache, capped by both limits.

    By default only clean inactive cache is counted. An explicitly opted-in local
    node may count up to half its clean unmapped active file cache. Anonymous,
    dirty/writeback and mapped active pages are never included in that allowance.
    Missing memory.stat information falls back to raw cgroup headroom.
    """
    limit, used = max(0, int(maximum)), max(0, int(current))
    inactive = max(0, int(stat.get('inactive_file', 0)))
    excluded = max(0, int(stat.get('file_dirty', 0))) + max(0, int(stat.get('file_writeback', 0)))
    if not 0 <= active_file_fraction <= .5:
        raise ValueError('active file allowance must be between zero and one half')
    active_clean = max(0, int(stat.get('active_file', 0)) - excluded
                       - max(0, int(stat.get('file_mapped', 0))))
    inactive_clean = max(0, inactive - excluded)
    active_allowance = int(active_clean * active_file_fraction)
    reclaimable = min(used, inactive_clean + active_allowance)
    potential = max(0, limit - used + reclaimable) / 1024**2
    return min(host_available_mib, limit / 1024**2, potential), {
        'limit_mib': limit / 1024**2, 'current_mib': used / 1024**2,
        'raw_headroom_mib': max(0, limit-used) / 1024**2,
        'clean_inactive_file_estimate_mib': min(used, inactive_clean) / 1024**2,
        'clean_active_file_allowance_mib': active_allowance / 1024**2,
        'active_file_fraction': active_file_fraction,
    }


def active_cache_allowance_fraction(node, pressure):
    """Opt-in local-cache accounting only while cgroup memory PSI is healthy."""
    if node.get('filesystem') != 'local':
        return 0
    try:
        requested = float(node.get('labels', {}).get('clean_active_cache_fraction', '0'))
        full = next(line for line in pressure.splitlines() if line.startswith('full '))
        avg10 = float(dict(part.split('=') for part in full.split()[1:])['avg10'])
        if 0 <= requested <= .5 and 0 <= avg10 < .5:
            return requested
    except (ValueError, KeyError, StopIteration):
        pass
    return 0


def probe(node):
    start = time.time()
    before = cpu_sample()
    time.sleep(0.15)
    after = cpu_sample()
    cpu = 100 * (1 - (after[1] - before[1]) / max(1, after[0] - before[0]))
    mem = {line.split(":")[0]: int(line.split()[1]) / 1024
           for line in Path("/proc/meminfo").read_text().splitlines()}
    cpu_count = len(os.sched_getaffinity(0))
    available = mem["MemAvailable"]
    # cgroup v2 root-relative limits; /proc/affinity remains the fallback on v1.
    try:
        quota, period = Path("/sys/fs/cgroup/cpu.max").read_text().split()
        if quota != "max":
            cpu_count = min(cpu_count, float(quota) / float(period))
    except (OSError, ValueError):
        pass
    memory_cgroup = None
    try:
        maximum = Path("/sys/fs/cgroup/memory.max").read_text().strip()
        if maximum != "max":
            current = int(Path("/sys/fs/cgroup/memory.current").read_text())
            try:
                stat = {k: int(v) for k, v in (line.split() for line in
                        Path('/sys/fs/cgroup/memory.stat').read_text().splitlines())}
            except (OSError, ValueError):
                stat = {}
            fraction = 0
            # This is an accounting estimate, never a kernel reclaim request or
            # cgroup limit change. Missing/stressed PSI falls back to inactive-only.
            if node.get('labels', {}).get('clean_active_cache_fraction'):
                try:
                    fraction = active_cache_allowance_fraction(
                        node, Path('/sys/fs/cgroup/memory.pressure').read_text())
                except OSError:
                    pass
            available, memory_cgroup = cgroup_memory_headroom(available, maximum, current, stat, fraction)
    except (OSError, ValueError):
        pass
    owned_d, unmanaged_d = registered_d_processes(node.get('_registered_attempts', []))
    d_status = dstate_observation(owned_d, Path('/proc/sys/kernel/random/boot_id').read_text().strip(),
                                node.get('_d_state_previous', {}), start)
    d_pids = d_status['d_state_pids']
    root = Path(node["work_root"])
    while not root.exists():
        root = root.parent
    result = dict(time=start, hostname=os.uname().nodename, boot_id=Path("/proc/sys/kernel/random/boot_id").read_text().strip(),
                  cpu_percent=cpu, cpu_count=cpu_count, ram_available_mib=available,
                  ram_total_mib=mem["MemTotal"], disk_free_mib=shutil.disk_usage(root).free / 1024**2,
                  d_state=len(d_pids), d_state_pids=d_pids, gpus=[], assets={}, datasets={}, read_ok=True)
    result.update(d_status, d_state_unmanaged_count=unmanaged_d)
    # Fresh PSI is admission telemetry, not a persistent node-disable decision.
    result['memory_pressure_full_avg10'] = None
    try:
        for line in Path('/proc/pressure/memory').read_text().splitlines():
            if line.startswith('full '):
                values = dict(item.split('=', 1) for item in line.split()[1:])
                result['memory_pressure_full_avg10'] = float(values['avg10'])
    except (OSError, ValueError, KeyError):
        pass
    if memory_cgroup is not None:
        result['memory_cgroup'] = memory_cgroup
    try:
        query_result = subprocess.run(["nvidia-smi", "--query-gpu=index,uuid,name,memory.total,memory.used,utilization.gpu,temperature.gpu",
                                      "--format=csv,noheader,nounits"], text=True, capture_output=True, timeout=8)
        query = query_result.stdout
        apps = subprocess.check_output(["nvidia-smi", "--query-compute-apps=gpu_uuid,pid,used_gpu_memory",
                                        "--format=csv,noheader,nounits"], text=True, stderr=subprocess.PIPE, timeout=8)
        processes = {}
        for row in csv.reader(io.StringIO(apps), skipinitialspace=True):
            if len(row) == 3:
                processes.setdefault(row[0], []).append({"pid": int(row[1]), "used_mib": row[2]})
        owners=gpu_process_owners(node.get('_registered_attempts',[]),
            [p['pid'] for rows in processes.values() for p in rows])
        for rows in processes.values():
            for p in rows:
                if p['pid'] in owners:p['attempt']=owners[p['pid']]
        for row in csv.reader(io.StringIO(query), skipinitialspace=True):
            try:
                if len(row) != 7 or not row[1].startswith('GPU-'):
                    continue
                result["gpus"].append(dict(index=int(row[0]), uuid=row[1], name=row[2], memory_mib=float(row[3]),
                                           used_mib=float(row[4]), util_percent=float(row[5]), temperature_c=float(row[6]),
                                           processes=processes.get(row[1], [])))
            except (ValueError, IndexError):
                continue
        result['gpu_unavailable_uuids'] = sorted(
            {g['uuid'] for g in node['gpus']} - {g['uuid'] for g in result['gpus']})
    except (OSError, subprocess.SubprocessError, ValueError) as exc:
        result["gpus"] = []  # fail closed for GPU admission, still report CPU health
        result["gpu_error"] = str(exc)
    for key, contract in node["assets"].items():
        try:
            sha = digest(contract["path"])
            if sha == contract["sha256"]:
                result["assets"][key] = sha
        except OSError:
            pass
    if not d_pids:
        result["datasets"] = {name: dataset_status(path) for name, path in node.get("datasets", {}).items()}
    policy = node["policy"]
    if policy.get("read_probe_path"):
        if d_pids:
            result["read_ok"] = False
            result["read_skipped"] = "D-state already present; do not pile up read probes"
            return result
        # A separate timed subprocess prevents a hung filesystem read from blocking
        # the control plane. A timed-out D-state child is NOT assumed terminated.
        code = "import sys; f=open(sys.argv[1],'rb'); b=f.read(int(sys.argv[2])); sys.exit(0 if len(b)==int(sys.argv[2]) else 2)"
        t = time.monotonic()
        try:
            child = subprocess.Popen([sys.executable, "-c", code, policy["read_probe_path"], str(policy["read_probe_bytes"])],
                                     stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
            result["read_ok"] = child.wait(timeout=policy["read_probe_timeout_s"]) == 0
        except subprocess.TimeoutExpired:
            child.kill()
            # Do not unbounded-wait for an uninterruptible task.
            result["read_ok"] = False
            result["read_timeout_pid"] = child.pid
        result["read_seconds"] = time.monotonic() - t
    return result


def oom_owned_processes(request, proc_root='/proc'):
    """Exact registered environment and UID; never infer ownership from GPU use."""
    owned=[]
    known={p['pid'] for p in request.get('_oom_previous',{}).get('processes',[])}
    try:group=json.loads((Path(request['attempt_dir'])/'state.json').read_text()).get('child_pgid')
    except (OSError,ValueError):group=None
    for path in Path(proc_root).iterdir():
        if not path.name.isdigit() or int(path.name)==os.getpid():continue
        try:
            if path.stat().st_uid!=os.getuid():continue
            before=process(path.name,proc_root)
            if not before or before['state']=='Z':continue
            env=(path/'environ').read_bytes().split(b'\0')
            if (('RS_ATTEMPT_ID='+request['id']).encode() not in env or
                    ('RS_ATTEMPT_DIR='+request['attempt_dir']).encode() not in env):continue
            after=process(path.name,proc_root)
            if after and after['start']==before['start']:
                owned.append(dict(pid=int(path.name),**after))
        except (FileNotFoundError,ProcessLookupError):continue
        except PermissionError:
            # SSH agents and unrelated protected processes share this UID. They
            # are not evidence of a surviving experiment. Known/group/path-bound
            # inaccessible processes, however, must block termination confirmation.
            info=process(path.name,proc_root)
            try:args=(path/'cmdline').read_bytes().split(b'\0')
            except OSError:args=[]
            root=request['attempt_dir'].encode()
            if (int(path.name) in known or (group and info and info['pgrp']==group)
                    or any(v==root or v.startswith(root+b'/') for v in args)):
                raise UncertainExecution('cannot verify a protected attempt process')
    if len(owned)>256:raise UncertainExecution('OOM process inventory exceeds safety bound')
    return owned


def match_kernel_oom(records, previous, state, now, current=None):
    """Bind kernel PID to a recent same-boot registered PID/start observation."""
    current=current or {}
    if (previous.get('boot_id')!=state.get('boot_id') or
            not 0<=now-previous.get('time',0)<=60):return None
    identities={p['pid']:p for p in previous.get('processes',[])}
    for row in records:
        try:
            event=float(row['__REALTIME_TIMESTAMP'])/1e6
            mono=float(row['__MONOTONIC_TIMESTAMP'])/1e6
            if row['_BOOT_ID'].replace('-','')!=state['boot_id'].replace('-',''):continue
            if not previous['time']<=event<=state.get('finished',now)+1:continue
            found=re.search(r'Out of memory: Killed process (\d+) ',row.get('MESSAGE',''),re.I)
            if not found:continue
            pid=int(found[1]);identity=identities.get(pid)
            if not identity:continue
            if mono<float(identity['start'])/os.sysconf('SC_CLK_TCK'):continue
            if pid in current and current[pid]['start']!=identity['start']:continue
            return dict(source='kernel_oom',pid=pid,start=identity['start'],boot_id=state['boot_id'],time=event,attempt=state.get('attempt'))
        except (KeyError,TypeError,ValueError):continue
    return None


def kernel_oom_evidence(request,state):
    previous=request.get('_oom_previous',{})
    if (Path('/.dockerenv').exists() or not state.get('boot_id') or
            previous.get('boot_id')!=state['boot_id'] or
            not 0<=time.time()-previous.get('time',0)<=60):return None
    # Host journal PIDs must not be compared to container-local PIDs.
    base=['journalctl','-k','-b',state['boot_id'].replace('-',''),'--since','@'+str(int(previous['time'])),
          '--no-pager','-o','json','-n','200']
    for argv in (base,['sudo','-n',*base]):
        try:
            result=subprocess.run(argv,text=True,capture_output=True,timeout=4)
            if result.returncode:continue
            records=[json.loads(line) for line in result.stdout.splitlines() if line.startswith('{')]
            current={p['pid']:info for p in previous.get('processes',[])
                     if (info:=process(p['pid'])) is not None}
            evidence=match_kernel_oom(records,previous,state,time.time(),current)
            if evidence:return evidence
        except (OSError,ValueError,subprocess.TimeoutExpired):continue
    return None


def finish_oom_classification(request,state,evidence):
    survivors=oom_owned_processes(request)
    group_live=bool(state.get('child_pgid') and group_alive(state['child_pgid']))
    verified=not survivors and not group_live
    return dict(state,status='failed' if verified else 'unknown',failure_class='experiment_oom',
                oom_node=request['node_spec']['id'],termination_verified=verified,
                failure_evidence=evidence,oom_survivors=survivors,oom_observation=request.get('_oom_previous',{}),
                reason='Experiment OOM; '+('termination verified; alternate host required' if verified else 'owned processes await cleanup'))


def recover_oom(request):
    """Explicit execute-only RPC. Read/status never sends process signals."""
    import signal
    state=json.loads((Path(request['attempt_dir'])/'state.json').read_text())
    boot=Path('/proc/sys/kernel/random/boot_id').read_text().strip()
    if state.get('attempt')!=request['id'] or state.get('boot_id')!=boot:return read_status(request)
    report=oom_failure(request,state)
    if report.get('failure_class')!='experiment_oom' or report.get('termination_verified'):return report
    if not hasattr(os,'pidfd_open') or not hasattr(signal,'pidfd_send_signal'):
        return dict(report,reason='OOM cleanup requires PID-safe signalling support')
    handles=[]
    try:
        for row in report['oom_survivors']:
            try:fd=os.pidfd_open(row['pid'])
            except ProcessLookupError:continue
            actual=next((p for p in oom_owned_processes(request) if p['pid']==row['pid']),None)
            if not actual or actual['start']!=row['start']:
                os.close(fd);continue
            handles.append(fd)
            signal.pidfd_send_signal(fd,signal.SIGTERM)
        deadline=time.monotonic()+2
        while oom_owned_processes(request) and time.monotonic()<deadline:time.sleep(.1)
        for fd in handles:
            try:signal.pidfd_send_signal(fd,signal.SIGKILL)
            except ProcessLookupError:pass
        deadline=time.monotonic()+1
        while oom_owned_processes(request) and time.monotonic()<deadline:time.sleep(.1)
    finally:
        for fd in handles:os.close(fd)
    result=finish_oom_classification(request,state,report['failure_evidence'])
    for key in ('oom_memory','oom_reserved_vram_mib'):
        if key in report:result[key]=report[key]
    atomic_json(Path(request['attempt_dir'])/'OOM_RECOVERY.json',dict(time=time.time(),
                evidence=report['failure_evidence'],owned_pids=report['oom_survivors'],termination_verified=result['termination_verified']))
    return result


def oom_failure(request, state):
    """Only explicit OOM evidence after process-group termination, never exit 137 alone."""
    if state.get('status') != 'failed' or request.get('job_spec', {}).get('kind') not in ('train', 'eval'):
        return state
    cached=request.get('_oom_verified_evidence')
    if (isinstance(cached,dict) and cached.get('source')=='kernel_oom' and
            cached.get('attempt')==request['id'] and cached.get('boot_id')==state.get('boot_id')):
        return finish_oom_classification(request,state,cached)
    root = Path(request['attempt_dir'])
    import re
    pattern = re.compile(r'CUDA out of memory|ResourceExhaustedError|OutOfMemoryError|MemoryError:|CUDA_ERROR_OUT_OF_MEMORY|cudaErrorMemoryAllocation|CUDNN_STATUS_ALLOC_FAILED', re.I)
    for name in ('stderr.log', 'stdout.log', 'run/rank0.stdout', 'run/training.stdout',
                 'run/stdout.log', 'smoke/rank0.stdout', 'evaluation/evaluation.stdout'):
        path = root / name
        try:
            if not path.resolve().is_relative_to(root.resolve()):
                continue
            with path.open('rb') as stream:
                stream.seek(max(0, path.stat().st_size - 65536))
                tail = stream.read(65536).decode(errors='replace')
            match = pattern.search(tail)
            if match:
                classified=finish_oom_classification(request,state,name)
                if re.search(r'CUDA out of memory|CUDA_ERROR_OUT_OF_MEMORY|cudaErrorMemoryAllocation|CUDNN_STATUS_ALLOC_FAILED|GPU.*out of memory|out of memory.*GPU',tail,re.I):
                    classified['oom_memory']='gpu'
                    classified['oom_reserved_vram_mib']=request.get('resources',{}).get('vram_mib')
                return classified
        except OSError:
            pass
    evidence=kernel_oom_evidence(request,state)
    if evidence:return finish_oom_classification(request,state,evidence)
    return state


def gpu_startup_failure(request, state):
    """Classify only terminated pre-training CUDA visibility failures, not model errors."""
    directory = Path(request['attempt_dir'])
    if (state.get('status')=='failed' and request.get('job','').startswith('EXEC_VERIFY_')
            and request.get('job_spec',{}).get('kind')=='prepare'):
        # Validation can finish some methods before the device disappears.
        for path in sorted(directory.glob('*/stdout.log'))[:32]:
            try:
                with path.open('rb') as stream:
                    stream.seek(max(0,path.stat().st_size-16384))
                    tail=stream.read(16384).decode(errors='replace')
                if ('Expected exactly one visible GPU' in tail and
                        'CUDA device is not set properly' in tail):
                    return dict(state,failure_class='gpu_validation_unavailable',
                        failure_evidence=str(path.relative_to(directory)),
                        reason='CUDA device unavailable during execution validation')
            except OSError:
                continue
    if (state.get('status') != 'failed' or state.get('ready')
            or request.get('job_spec', {}).get('kind') != 'train'
            or len(request.get('gpus', [])) != 1
            or not state.get('boot_id')
            or (directory / 'startup.ready').exists()
            or (directory / 'run/TRAIN_PROGRESS.json').exists()):
        return state
    try:
        path = directory / 'smoke/rank0.stdout'
        with path.open('rb') as stream:
            stream.seek(max(0, path.stat().st_size - 32768))
            tail = stream.read(32768).decode(errors='replace')
        if ('Expected exactly one visible GPU' not in tail
                or 'CUDA device is not set properly' not in tail):
            return state
    except OSError:
        return state
    return dict(state, failure_class='gpu_startup_unavailable',
                failed_gpu_uuids=list(request['gpus']),
                reason='CUDA GPU unavailable in pre-training smoke; choose another GPU',
                failure_evidence='smoke/rank0.stdout')


def diagnostic_startup_ready(request, state):
    """Recognize completed R18 diagnostic cells in legacy workers without READY."""
    if (state.get('ready') or state.get('status') != 'running'
            or not request.get('job', '').startswith('R18_')
            or request.get('config', {}).get('mode') != 'diagnose'):
        return state
    root = Path(request['attempt_dir'])
    try:
        progress = json.loads((root/'diagnostics/PROGRESS.json').read_text())
        count = progress.get('completed_cells', 0)
        if type(count) is not int or count < 1 or count > 19:
            return state
        cells = sorted((root/'diagnostics').glob('*/DIAGNOSTIC_CELL.json'))
        if len(cells) < count or len(cells) > 19:
            return state
        for path in cells:
            if path.is_symlink() or not path.resolve().is_relative_to(root.resolve()):
                continue
            if path.stat().st_mtime < state.get('started', float('inf')):
                continue
            value = json.loads(path.read_text())
            spec = json.loads((path.parent/'REQUEST.json').read_text())
            if (value.get('status') != 'complete' or value.get('cell') != spec.get('cell')
                    or not value.get('checkpoint_sha256')
                    or value['checkpoint_sha256'] != spec.get('checkpoint_sha256')):
                continue
            checkpoint = Path(spec['checkpoint'])
            digest = hashlib.sha256()
            with checkpoint.open('rb') as stream:
                for chunk in iter(lambda: stream.read(1024*1024), b''):
                    digest.update(chunk)
            if digest.hexdigest() != value['checkpoint_sha256']:
                continue
            return dict(state, ready=True, readiness_evidence=dict(
                kind='completed_diagnostic_cell', path=str(path.relative_to(root)),
                completed_cells=count, checkpoint_sha256=value['checkpoint_sha256']))
    except (OSError, ValueError, KeyError, TypeError):
        pass
    return state


def read_status(request):
    directory = Path(request["attempt_dir"])
    if not directory.exists():
        return {"status": "unknown", "reason": "attempt directory absent; no automatic relaunch"}
    try:
        state = json.loads((directory / "state.json").read_text())
        if state["attempt"] != request["id"]:
            return {"status": "unknown", "reason": "attempt identity mismatch"}
        boot = Path("/proc/sys/kernel/random/boot_id").read_text().strip()
        old_boot = state.get('boot_id')
        # An authenticated reply from a new boot proves that the old local
        # processes ended. A missing runner on the SAME boot does not.
        local_compute = (request.get('job_spec', {}).get('kind') in ('train', 'eval', 'prepare')
                         and not request.get('resources', {}).get('tokens'))
        if local_compute and old_boot and boot and old_boot != boot:
            if state['status'] in ('succeeded', 'failed'):
                return state  # Old process-group IDs may already have been reused.
            return dict(state, status='failed', ready=False,
                        reason='host reboot confirmed; previous execution terminated',
                        termination_cause='host_reboot', current_boot_id=boot,
                        termination_observed_at=time.time())
        if state["status"] in ("succeeded", "failed"):
            classified=oom_failure(request,state)
            if classified.get('failure_class')=='experiment_oom':return classified
            if state.get("child_pgid") and group_alive(state["child_pgid"]):
                return dict(state, status="unknown", reason="descendant processes still alive")
            return gpu_startup_failure(request, state)
        runner = process(state.get("runner_pid", 0))
        if (runner and runner["start"] == state.get("runner_start") and runner["state"] != "Z"
                and boot == state.get("boot_id")):
            state = diagnostic_startup_ready(request, state)
            rss = process_tree_rss_mib(state.get("child_pid", 0))
            observation={}
            if request.get('job_spec',{}).get('kind') in ('train','eval'):
                observation['oom_observation']=dict(boot_id=boot,time=time.time(),processes=oom_owned_processes(request))
            return dict(state,**observation, **({"rss_mib": rss} if rss is not None else {}))
        return dict(state, status="unknown", reason="runner absent/rebooted; preserve reservation for reconciliation")
    except (OSError, ValueError, KeyError, UncertainExecution) as exc:
        return {"status": "unknown", "reason": "unreadable attempt receipt: " + str(exc)}


def launch(request, source):
    directory = Path(request["attempt_dir"])
    directory.parent.mkdir(parents=True, exist_ok=True)
    try:
        directory.mkdir(mode=0o700)
    except FileExistsError:
        # Lost SSH acknowledgement must never create a second scientific child.
        return read_status(request)
    for name, text in (("spec.json", json.dumps(request)), ("runner.py", source)):
        with (directory / name).open("x") as f:
            f.write(text)
            f.flush()
            os.fsync(f.fileno())
    with (directory / "runner.log").open("xb") as log:
        child = subprocess.Popen([sys.executable, str(directory / "runner.py"), "run", str(directory / "spec.json")],
                                 stdin=subprocess.DEVNULL, stdout=log, stderr=subprocess.STDOUT,
                                 start_new_session=True, close_fds=True)
    return {"status": "starting", "runner_pid": child.pid, "attempt": request["id"]}


def run(request):
    directory = Path(request["attempt_dir"])
    with (directory / "runner.lock").open("a") as lock, contextlib.ExitStack() as hardware_locks:
        fcntl.flock(lock, fcntl.LOCK_EX | fcntl.LOCK_NB)
        # Even a manual repeated runner invocation must not rerun an attempt.
        claim = directory / "execution.claim"
        with claim.open("x") as f:
            f.write(str(os.getpid()))
        state = dict(attempt=request["id"], status="starting", runner_pid=os.getpid(),
                     runner_start=process(os.getpid())["start"],
                     boot_id=Path("/proc/sys/kernel/random/boot_id").read_text().strip(),
                     started=time.time(), ready=False)
        atomic_json(directory / "state.json", state)
        try:
            if digest(directory / "runner.py") != request["runner_sha256"]:
                raise ValueError("immutable runner source hash mismatch")
            frozen = {k: v for k, v in request.items() if k != "spec_sha256"}
            encoded = json.dumps(frozen, sort_keys=True, ensure_ascii=False, allow_nan=False).encode()
            if hashlib.sha256(encoded).hexdigest() != request["spec_sha256"]:
                raise ValueError("immutable attempt specification hash mismatch")
            if request.get("dataset") and not dataset_status(request["dataset_path"])["available"]:
                raise ValueError("dataset path became unavailable before execution: " + request["dataset_path"])
            for contract in request["input_files"]:
                if digest(contract["path"]) != contract["sha256"]:
                    raise ValueError("input hash mismatch: " + contract["path"])
            atomic_json(directory / "config.json", request["config"])
            env = os.environ.copy()
            env.update(request["env"])
            filesystem = request.get(
                "filesystem", "nfs" if request.get("node_spec", {}).get("startup_group") else "local")
            env.update(CUDA_VISIBLE_DEVICES=",".join(request["gpus"]),
                       RS_ATTEMPT_ID=request["id"], RS_ATTEMPT_DIR=str(directory),
                       RS_CONFIG_PATH=str(directory / "config.json"), RS_READY_PATH=str(directory / "startup.ready"),
                       RS_DATASET_PATH=request.get("dataset_path", ""),
                       RS_FILESYSTEM=filesystem)
            pass_fds = ()
            if request.get('board_lock'):
                # One gateway per physical board. Use the SAME lock path as
                # non-scheduler board tooling; never unlink or steal this lock.
                board_lock = hardware_locks.enter_context(Path(request['board_lock']).open('a'))
                while True:
                    try:
                        fcntl.flock(board_lock, fcntl.LOCK_EX | fcntl.LOCK_NB)
                        break
                    except BlockingIOError:
                        state.update(heartbeat=time.time(), reason='waiting for external board lock')
                        atomic_json(directory / 'state.json', state)
                        time.sleep(0.5)
                pass_fds = (board_lock.fileno(),)
                env['RS_BOARD_LOCK_FD'] = str(board_lock.fileno())
            if request.get('preflight_argv'):
                with (directory / 'preflight.log').open('xb') as log:
                    child = subprocess.Popen(request['preflight_argv'], cwd=request['cwd'], env=env,
                                             stdin=subprocess.DEVNULL, stdout=log, stderr=subprocess.STDOUT,
                                             start_new_session=True, pass_fds=pass_fds)
                    state.update(child_pid=child.pid, child_pgid=child.pid)
                    deadline = time.monotonic() + 60
                    while child.poll() is None:
                        state.update(heartbeat=time.time(), reason='RTL preflight')
                        atomic_json(directory / 'state.json', state)
                        if time.monotonic() >= deadline:
                            raise UncertainExecution('preflight timed out; process not killed, reservation retained')
                        time.sleep(0.5)
                    if group_alive(child.pid):
                        raise UncertainExecution('preflight left live descendants')
                    if child.returncode:
                        raise ValueError('RTL preflight failed; workload not launched')
            with (directory / "stdout.log").open("xb") as out, (directory / "stderr.log").open("xb") as err:
                child = subprocess.Popen(request["argv"], cwd=request["cwd"], env=env,
                                         stdin=subprocess.DEVNULL, stdout=out, stderr=err, start_new_session=True,
                                         pass_fds=pass_fds)
                state.update(status="running", child_pid=child.pid, child_pgid=child.pid, reason='')
                while child.poll() is None:
                    state.update(heartbeat=time.time(), ready=(directory / "startup.ready").is_file())
                    atomic_json(directory / "state.json", state)
                    time.sleep(0.5)
                code = child.wait()
            if request.get('validation') and group_alive(child.pid):
                raise UncertainExecution('RTL command left live descendants; reservation retained')
            outputs = {}
            if code == 0:
                if request.get('validation'):
                    state['validation'] = validate_rtl(directory, request['job_spec']['kind'], request['validation'])
                for name in request["outputs"]:
                    path = directory / name
                    if not path.resolve().is_relative_to(directory.resolve()) or not path.is_file():
                        raise ValueError("missing/escaping output file: " + name)
                    outputs[name] = {"path": str(path), "sha256": digest(path), "bytes": path.stat().st_size}
            state.update(status="succeeded" if code == 0 else "failed", returncode=code, outputs=outputs,
                         reason='' if code == 0 else f'workload exited with code {code}')
        except Exception as exc:
            state.update(status="unknown" if isinstance(exc, UncertainExecution) else "failed",
                         error=str(exc), reason=str(exc), traceback=traceback.format_exc())
        state.update(finished=time.time(), heartbeat=time.time())
        atomic_json(directory / "state.json", state)
        return state


def main():
    if len(sys.argv) > 1 and sys.argv[1] == "run":
        run(json.loads(Path(sys.argv[2]).read_text()))
        return
    payload = json.load(sys.stdin)
    action, request = payload["action"], payload["request"]
    if action == "probe":
        result = probe(request)
    elif action == "status":
        result = read_status(request)
    elif action == 'recover_oom':
        result = recover_oom(request)
    elif action == "artifact_status":
        result = read_status(request)
        retry_path = Path(request['attempt_dir'])/'HF_RETRY.json'
        if result.get('status') == 'failed' and retry_path.is_file():
            retry = json.loads(retry_path.read_text())
            if (retry.get('attempt') == request['id'] and retry.get('http_status') == 429
                    and 0 <= retry['retry_at']-retry['created'] <= 21600):
                result['rate_limit'] = retry
        if result.get('status') == 'succeeded':
            receipt = result['outputs']['HF_RECEIPT.json']
            if digest(receipt['path']) != receipt['sha256']:
                raise ValueError('artifact receipt changed')
            result['artifact'] = json.loads(Path(receipt['path']).read_text())
    elif action == 'dataset_receipt':
        result = read_status(request)
        if result.get('status') == 'succeeded':
            receipt = result['outputs']['DATASET_READY.json']
            if digest(receipt['path']) != receipt['sha256']:
                raise ValueError('dataset receipt changed')
            result['dataset_receipt'] = json.loads(Path(receipt['path']).read_text())
    elif action == "launch":
        result = launch(request, payload["source"])
    else:
        raise ValueError("unknown agent action")
    print(json.dumps(result, ensure_ascii=False, allow_nan=False))


if __name__ == "__main__":
    main()
