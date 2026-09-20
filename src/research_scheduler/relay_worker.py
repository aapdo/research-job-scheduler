"""Controller-local, checksum-verified dependency artifact relay worker."""
import hashlib
import json
import os
import shlex
import shutil
import subprocess
import sys
import contextlib
import tempfile
import time
from pathlib import Path, PurePosixPath


def digest(path):
    h = hashlib.sha256()
    with Path(path).open('rb') as stream:
        for chunk in iter(lambda: stream.read(4 * 1024 * 1024), b''):
            h.update(chunk)
    return h.hexdigest()


def relative(value):
    path = PurePosixPath(value)
    if not value or path.is_absolute() or '..' in path.parts or '\\' in value or '\x00' in value:
        raise ValueError('unsafe relay artifact path')
    return path


def run(argv, *, input_text=None):
    return subprocess.run(argv, input=input_text, text=True, check=True,
                          stdout=subprocess.PIPE, stderr=subprocess.PIPE)


def local_attempt_manifest(root, attempt, spec_sha256):
    root = Path(root).resolve()
    state = json.loads((root / 'state.json').read_text())
    if state.get('attempt') != attempt or state.get('status') not in ('succeeded', 'failed'):
        raise ValueError('attempt archive source is not terminal')
    spec = json.loads((root / 'spec.json').read_text())
    frozen = {key: value for key, value in spec.items() if key != 'spec_sha256'}
    actual_spec_sha = hashlib.sha256(json.dumps(
        frozen, sort_keys=True, ensure_ascii=False, allow_nan=False).encode()).hexdigest()
    if spec.get('id') != attempt or actual_spec_sha != spec_sha256:
        raise ValueError('attempt archive specification changed')
    files = {}
    for path in sorted(root.rglob('*')):
        if path.is_symlink():
            raise ValueError('attempt archive contains a symlink: ' + str(path.relative_to(root)))
        if path.is_dir():
            continue
        if not path.is_file() or not path.resolve().is_relative_to(root):
            raise ValueError('attempt archive contains a non-regular file')
        name = path.relative_to(root).as_posix()
        files[name] = {'bytes': path.stat().st_size, 'sha256': digest(path)}
    if not files or 'spec.json' not in files or 'state.json' not in files:
        raise ValueError('attempt archive is missing immutable receipts')
    return files


def discover_attempt_manifest(config):
    root = config['source_root']
    attempt = config['source_attempt']
    spec_sha256 = config['source_spec_sha256']
    if config['source_target'] == '@local':
        return local_attempt_manifest(root, attempt, spec_sha256)
    script = (
        'import hashlib,json,pathlib,sys\n'
        'root=pathlib.Path(sys.argv[1]).resolve();attempt=sys.argv[2];spec_sha=sys.argv[3]\n'
        'state=json.loads((root/"state.json").read_text())\n'
        'assert state.get("attempt")==attempt and state.get("status") in ("succeeded","failed")\n'
        'spec=json.loads((root/"spec.json").read_text());frozen={k:v for k,v in spec.items() if k!="spec_sha256"}\n'
        'actual=hashlib.sha256(json.dumps(frozen,sort_keys=True,ensure_ascii=False,allow_nan=False).encode()).hexdigest()\n'
        'assert spec.get("id")==attempt and actual==spec_sha\n'
        'files={}\n'
        'for p in sorted(root.rglob("*")):\n'
        ' assert not p.is_symlink(),"attempt archive contains a symlink"\n'
        ' if p.is_dir():continue\n'
        ' assert p.is_file() and p.resolve().is_relative_to(root),"non-regular attempt file"\n'
        ' h=hashlib.sha256()\n'
        ' with p.open("rb") as f:\n'
        '  for chunk in iter(lambda:f.read(4194304),b""):h.update(chunk)\n'
        ' files[p.relative_to(root).as_posix()]={"bytes":p.stat().st_size,"sha256":h.hexdigest()}\n'
        'assert files and "spec.json" in files and "state.json" in files\n'
        'print(json.dumps(files,sort_keys=True,separators=(",",":")))\n')
    result = run(['ssh', '-o', 'BatchMode=yes', config['source_target'],
                  'python3', '-', root, attempt, spec_sha256], input_text=script)
    files = json.loads(result.stdout)
    if not files:
        raise ValueError('empty attempt archive manifest')
    return files


def stream_from_remote(target, source, destination):
    script = ('import pathlib,shutil,sys\n'
              'p=pathlib.Path(sys.argv[1])\n'
              'assert p.is_file() and not p.is_symlink()\n'
              'with p.open("rb") as f:shutil.copyfileobj(f,sys.stdout.buffer,4194304)\n')
    command = ['ssh', '-o', 'BatchMode=yes', target,
               shlex.join(['python3', '-c', script, str(source)])]
    try:
        with destination.open('xb') as output:
            subprocess.run(command, check=True, stdout=output, stderr=subprocess.PIPE)
    except Exception:
        destination.unlink(missing_ok=True)
        raise


def stream_to_remote(target, root, name, source):
    script = ('import pathlib,shutil,sys\n'
              'root=pathlib.Path(sys.argv[1]).resolve();p=(root/sys.argv[2]).resolve()\n'
              'assert p.is_relative_to(root) and not p.exists()\n'
              'p.parent.mkdir(parents=True,exist_ok=True)\n'
              'with p.open("xb") as f:shutil.copyfileobj(sys.stdin.buffer,f,4194304)\n')
    command = ['ssh', '-o', 'BatchMode=yes', target,
               shlex.join(['python3', '-c', script, str(root), name])]
    with source.open('rb') as input_stream:
        subprocess.run(command, check=True, stdin=input_stream,
                       stdout=subprocess.PIPE, stderr=subprocess.PIPE)


def stream_between_remotes(source_target, source_root, destination_target, destination_root, name):
    source_script = ('import pathlib,shutil,sys\n'
                     'p=(pathlib.Path(sys.argv[1])/sys.argv[2]).resolve()\n'
                     'root=pathlib.Path(sys.argv[1]).resolve()\n'
                     'assert p.is_relative_to(root) and p.is_file() and not p.is_symlink()\n'
                     'with p.open("rb") as f:shutil.copyfileobj(f,sys.stdout.buffer,4194304)\n')
    destination_script = ('import pathlib,shutil,sys\n'
                          'root=pathlib.Path(sys.argv[1]).resolve();p=(root/sys.argv[2]).resolve()\n'
                          'assert p.is_relative_to(root) and not p.exists()\n'
                          'p.parent.mkdir(parents=True,exist_ok=True)\n'
                          'with p.open("xb") as f:shutil.copyfileobj(sys.stdin.buffer,f,4194304)\n')
    producer = subprocess.Popen(
        ['ssh', '-o', 'BatchMode=yes', source_target,
         shlex.join(['python3', '-c', source_script, str(source_root), name])],
        stdout=subprocess.PIPE, stderr=subprocess.PIPE)
    consumer = subprocess.Popen(
        ['ssh', '-o', 'BatchMode=yes', destination_target,
         shlex.join(['python3', '-c', destination_script, str(destination_root), name])],
        stdin=producer.stdout, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
    producer.stdout.close()
    _, destination_error = consumer.communicate()
    source_error = producer.stderr.read()
    source_code = producer.wait()
    if source_code or consumer.returncode:
        raise subprocess.CalledProcessError(
            source_code or consumer.returncode, 'direct remote attempt stream',
            stderr=(source_error + destination_error).decode(errors='replace'))


def _pipe_processes(producer, consumer, payload=None):
    if payload is not None:
        producer.stdin.write(payload)
        producer.stdin.close()
    producer.stdout.close()
    _, destination_error = consumer.communicate()
    source_error = producer.stderr.read()
    source_code = producer.wait()
    if source_code or consumer.returncode:
        raise subprocess.CalledProcessError(
            source_code or consumer.returncode, 'uncompressed tar relay stream',
            stderr=source_error + destination_error)


def stream_tar_tree(source_target, source_root, destination_target, destination_root, names):
    """Send many regular files through one uncompressed tar/SSH stream."""
    source_script = (
        'import json,pathlib,sys,tarfile\n'
        'root=pathlib.Path(sys.argv[1]).resolve();names=json.load(sys.stdin)\n'
        'with tarfile.open(fileobj=sys.stdout.buffer,mode="w|") as out:\n'
        ' for name in names:\n'
        '  rel=pathlib.PurePosixPath(name);p=(root/name).resolve()\n'
        '  assert name and not rel.is_absolute() and ".." not in rel.parts and p.is_relative_to(root)\n'
        '  assert p.is_file() and not p.is_symlink()\n'
        '  out.add(p,arcname=name,recursive=False)\n')
    extract_script = (
        'import pathlib,shutil,sys,tarfile\n'
        'root=pathlib.Path(sys.argv[1]).resolve();root.mkdir(parents=True,exist_ok=False);seen=set()\n'
        'with tarfile.open(fileobj=sys.stdin.buffer,mode="r|") as src:\n'
        ' for item in src:\n'
        '  name=item.name;rel=pathlib.PurePosixPath(name);p=(root/name).resolve()\n'
        '  assert name and not rel.is_absolute() and ".." not in rel.parts and p.is_relative_to(root)\n'
        '  assert item.isfile() and name not in seen;seen.add(name);p.parent.mkdir(parents=True,exist_ok=True)\n'
        '  incoming=src.extractfile(item);assert incoming is not None\n'
        '  with p.open("xb") as out:shutil.copyfileobj(incoming,out,4194304)\n')
    source_command = ([sys.executable, '-c', source_script, str(source_root)]
                      if source_target == '@local' else
                      ['ssh', '-o', 'BatchMode=yes', source_target,
                       shlex.join(['python3', '-c', source_script, str(source_root)])])
    producer = subprocess.Popen(source_command, stdin=subprocess.PIPE, stdout=subprocess.PIPE,
                                stderr=subprocess.PIPE)
    consumer = subprocess.Popen(
        ['ssh', '-o', 'BatchMode=yes', destination_target,
         shlex.join(['python3', '-c', extract_script, str(destination_root)])],
        stdin=producer.stdout, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
    _pipe_processes(producer, consumer, json.dumps(sorted(names)).encode())


def stream_tar_bundle(source_target, source_root, destination_target, bundle_path, names):
    """Create one uncompressed tar file on a remote staging node."""
    source_script = (
        'import json,pathlib,sys,tarfile\n'
        'root=pathlib.Path(sys.argv[1]).resolve();names=json.load(sys.stdin)\n'
        'with tarfile.open(fileobj=sys.stdout.buffer,mode="w|") as out:\n'
        ' for name in names:\n'
        '  rel=pathlib.PurePosixPath(name);p=(root/name).resolve()\n'
        '  assert name and not rel.is_absolute() and ".." not in rel.parts and p.is_relative_to(root)\n'
        '  assert p.is_file() and not p.is_symlink();out.add(p,arcname=name,recursive=False)\n')
    writer_script = (
        'import pathlib,shutil,sys\n'
        'p=pathlib.Path(sys.argv[1]);assert not p.exists();p.parent.mkdir(parents=True,exist_ok=True)\n'
        'with p.open("xb") as out:shutil.copyfileobj(sys.stdin.buffer,out,4194304)\n')
    source_command = ([sys.executable, '-c', source_script, str(source_root)]
                      if source_target == '@local' else
                      ['ssh', '-o', 'BatchMode=yes', source_target,
                       shlex.join(['python3', '-c', source_script, str(source_root)])])
    producer = subprocess.Popen(source_command, stdin=subprocess.PIPE, stdout=subprocess.PIPE,
                                stderr=subprocess.PIPE)
    consumer = subprocess.Popen(
        ['ssh', '-o', 'BatchMode=yes', destination_target,
         shlex.join(['python3', '-c', writer_script, str(bundle_path)])],
        stdin=producer.stdout, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
    _pipe_processes(producer, consumer, json.dumps(sorted(names)).encode())


def extract_remote_tar(source_target, bundle_path, destination_target, destination_root):
    source_script = ('import pathlib,shutil,sys\n'
                     'p=pathlib.Path(sys.argv[1]);assert p.is_file() and not p.is_symlink()\n'
                     'with p.open("rb") as src:shutil.copyfileobj(src,sys.stdout.buffer,4194304)\n')
    extract_script = (
        'import pathlib,shutil,sys,tarfile\n'
        'root=pathlib.Path(sys.argv[1]).resolve();root.mkdir(parents=True,exist_ok=False);seen=set()\n'
        'with tarfile.open(fileobj=sys.stdin.buffer,mode="r|") as src:\n'
        ' for item in src:\n'
        '  name=item.name;rel=pathlib.PurePosixPath(name);p=(root/name).resolve()\n'
        '  assert name and not rel.is_absolute() and ".." not in rel.parts and p.is_relative_to(root)\n'
        '  assert item.isfile() and name not in seen;seen.add(name);p.parent.mkdir(parents=True,exist_ok=True)\n'
        '  incoming=src.extractfile(item);assert incoming is not None\n'
        '  with p.open("xb") as out:shutil.copyfileobj(incoming,out,4194304)\n')
    producer = subprocess.Popen(
        ['ssh', '-o', 'BatchMode=yes', source_target,
         shlex.join(['python3', '-c', source_script, str(bundle_path)])],
        stdout=subprocess.PIPE, stderr=subprocess.PIPE)
    consumer = subprocess.Popen(
        ['ssh', '-o', 'BatchMode=yes', destination_target,
         shlex.join(['python3', '-c', extract_script, str(destination_root)])],
        stdin=producer.stdout, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
    _pipe_processes(producer, consumer)


@contextlib.contextmanager
def source_access_from_lab4(config):
    """Forward a short-lived local agent to LAB4; never copy a private key."""
    options = run(['ssh', '-G', config['source_target']]).stdout.splitlines()
    settings = {}
    for line in options:
        key, _, value = line.partition(' ')
        settings.setdefault(key, []).append(value)
    if any(settings.get(k, ['none'])[0] != 'none' for k in ('proxycommand', 'proxyjump')):
        raise ValueError('direct archive source requires an explicitly reachable SSH endpoint')
    host = settings['hostname'][0]
    port = settings.get('port', ['22'])[0]
    identity = host if port == '22' else '[' + host + ']:' + port
    public_keys = run(['ssh-keygen', '-F', identity]).stdout
    if not any(line and not line.startswith('#') for line in public_keys.splitlines()):
        raise ValueError('direct source has no trusted host key')
    configured_agent = settings.get('identityagent', ['none'])[0]
    if configured_agent != 'none':
        configured_agent = os.path.expandvars(os.path.expanduser(configured_agent))
        if Path(configured_agent).is_socket():
            yield configured_agent, dict(host=host, port=port, user=settings['user'][0],
                                          known_hosts=public_keys)
            return
    keys = [Path(path).expanduser() for path in settings.get('identityfile', [])]
    keys = [path for path in keys if path.is_file()]
    if not keys:
        raise ValueError('direct archive source has no local authentication identity')
    with tempfile.TemporaryDirectory(prefix='archive-agent-') as folder:
        socket = str(Path(folder) / 'agent.sock')
        process = subprocess.Popen(['ssh-agent', '-D', '-a', socket],
                                   stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
        try:
            for _ in range(50):
                if Path(socket).exists():
                    break
                if process.poll() is not None:
                    raise ValueError('archive authentication agent failed')
                time.sleep(.02)
            env = dict(os.environ, SSH_AUTH_SOCK=socket, SSH_ASKPASS_REQUIRE='never')
            added = False
            for key in keys:
                result = subprocess.run(['ssh-add', '-t', '3600', str(key)], env=env,
                                        stdin=subprocess.DEVNULL, stdout=subprocess.PIPE,
                                        stderr=subprocess.PIPE, timeout=10)
                added = added or result.returncode == 0
            if not added:
                raise ValueError('direct archive source authentication identity is unavailable')
            yield socket, dict(host=host, port=port, user=settings['user'][0], known_hosts=public_keys)
        finally:
            process.terminate()
            try:
                process.wait(timeout=5)
            except subprocess.TimeoutExpired:
                process.kill()
                process.wait()


def pull_on_lab4(config, temporary):
    """The data connection runs from LAB4 to source, not through the controller."""
    script = '''import json,pathlib,shlex,subprocess,sys,tempfile
c=json.load(sys.stdin)
root=pathlib.Path(c['destination'])
with tempfile.TemporaryDirectory(prefix='archive-transport-',dir=root.parent) as td:
 p=pathlib.Path(td);known=p/'known_hosts';listing=p/'files'
 known.write_text(c['connection']['known_hosts'])
 listing.write_bytes(b''.join(n.encode()+b'\\0' for n in c['files']))
 endpoint=c['connection'];ssh=['ssh','-F','/dev/null','-o','BatchMode=yes','-o','ConnectTimeout=10','-o','ServerAliveInterval=15','-o','ServerAliveCountMax=3','-o','StrictHostKeyChecking=yes','-o','UserKnownHostsFile='+str(known),'-p',endpoint['port']]
 remote=endpoint['user']+'@'+endpoint['host']+':'+c['source'].rstrip('/')+'/'
 subprocess.run(['rsync','-r','--protect-args','--from0','--files-from='+str(listing),'-e',shlex.join(ssh),'--',remote,str(root)+'/'],check=True)
'''
    with source_access_from_lab4(config) as (socket, endpoint):
        payload = dict(connection=endpoint, files=sorted(config['files']),
                       source=config['source_root'], destination=str(temporary))
        command = ['ssh', '-A', '-o', 'IdentityAgent=' + socket, '-o', 'BatchMode=yes',
                   config['destination_target'], shlex.join(['python3', '-c', script])]
        subprocess.run(command, input=json.dumps(payload), text=True, check=True,
                       stdout=subprocess.PIPE, stderr=subprocess.PIPE)


def copy_from_source(config, staging):
    source_root = Path(config['source_root'])
    target = config['source_target']
    for name, expected in config['files'].items():
        rel = relative(name)
        local = staging / str(rel)
        local.parent.mkdir(parents=True, exist_ok=True)
        if target == '@local':
            source = source_root / str(rel)
            if source.is_symlink() or not source.is_file() or not source.resolve().is_relative_to(source_root.resolve()):
                raise ValueError('relay source is not a regular file: ' + name)
            shutil.copyfile(source, local)
        else:
            stream_from_remote(target, source_root / str(rel), local)
        if local.stat().st_size != expected['bytes'] or digest(local) != expected['sha256']:
            raise ValueError('relay source verification failed: ' + name)


def filesystem_free_bytes(target, path):
    if target == '@local':
        root = Path(path)
        while not root.exists():
            root = root.parent
        return shutil.disk_usage(root).free
    script = ('import pathlib,shutil,sys\n'
              'p=pathlib.Path(sys.argv[1])\n'
              'while not p.exists():p=p.parent\n'
              'print(shutil.disk_usage(p).free)\n')
    return int(run(['ssh', '-o', 'BatchMode=yes', target, 'python3', '-', str(path)],
                   input_text=script).stdout.strip())


def verify_archive_capacity(config, attempt_dir):
    total = sum(item['bytes'] for item in config['files'].values())
    destination_free = filesystem_free_bytes(
        config['destination_target'], Path(config['destination_root']).parent)
    if destination_free < total + config.get('destination_min_free_bytes', 0):
        raise ValueError('insufficient LAB4 archive filesystem space')
    if config.get('transport_route') == 'controller-local-staging':
        local_free = filesystem_free_bytes('@local', attempt_dir)
        if local_free < total + config.get('controller_min_free_bytes', 0):
            raise ValueError('insufficient controller relay filesystem space')


def reuse_verified_archive(config):
    """Recover a lost acknowledgement without replacing an existing archive."""
    target = config['destination_target']
    root = config['destination_root']
    script = '''import hashlib,json,pathlib,sys
c=json.load(sys.stdin);root=pathlib.Path(c['root'])
if not root.exists():
 print('absent');sys.exit(0)
assert not root.is_symlink() and root.is_dir()
files={}
for p in root.rglob('*'):
 assert not p.is_symlink()
 if p.is_dir():continue
 assert p.is_file() and p.resolve().is_relative_to(root.resolve())
 h=hashlib.sha256()
 with p.open('rb') as f:
  for block in iter(lambda:f.read(4194304),b''):h.update(block)
 files[p.relative_to(root).as_posix()]={'bytes':p.stat().st_size,'sha256':h.hexdigest()}
assert files==c['files'],'existing archive differs; source must be retained'
print('verified')
'''
    command = ['python3', '-c', script]
    if target != '@local':
        command = ['ssh', '-o', 'BatchMode=yes', target, shlex.join(command)]
    result = run(command, input_text=json.dumps(dict(root=root, files=config['files'])))
    return result.stdout.strip() == 'verified'


def publish(config, staging):
    target = config['destination_target']
    destination = Path(config['destination_root'])
    temporary = Path(str(destination) + '.staging')
    if target == '@local':
        if destination.exists() or temporary.exists():
            raise ValueError('relay destination already exists')
        temporary.parent.mkdir(parents=True, exist_ok=True)
        shutil.copytree(staging, temporary)
        temporary.rename(destination)
    else:
        script = 'set -eu; test ! -e "$1"; test ! -e "$2"; mkdir -p "$2"'
        run(['ssh', '-o', 'BatchMode=yes', target, 'sh', '-s', '--',
             str(destination), str(temporary)], input_text=script)
        try:
            for name in config['files']:
                stream_to_remote(target, temporary, name, staging / str(relative(name)))
            checks = []
            for name, expected in config['files'].items():
                checks.append((str(temporary / str(relative(name))), expected['bytes'], expected['sha256']))
            verify = (
                'import hashlib,pathlib\n'
                'checks=' + repr(checks) + '\n'
                'for name,size,sha in checks:\n'
                ' p=pathlib.Path(name); h=hashlib.sha256()\n'
                ' with p.open("rb") as f:\n'
                '  for chunk in iter(lambda:f.read(4194304),b""):h.update(chunk)\n'
                ' assert p.is_file() and p.stat().st_size==size and h.hexdigest()==sha\n')
            run(['ssh', '-o', 'BatchMode=yes', target, 'python3', '-'], input_text=verify)
            run(['ssh', '-o', 'BatchMode=yes', target, 'sh', '-s', '--',
                 str(temporary), str(destination)], input_text='set -eu; mv -- "$1" "$2"')
        except Exception:
            subprocess.run(['ssh', '-o', 'BatchMode=yes', target, 'sh', '-s', '--', str(temporary)],
                           input='rm -rf -- "$1"', text=True,
                           stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
            raise
    return {name: dict(path=str(destination / str(relative(name))), **expected)
            for name, expected in config['files'].items()}


def verify_and_commit_remote(config, temporary, destination):
    checks = [(str(temporary / str(relative(name))), expected['bytes'], expected['sha256'])
              for name, expected in config['files'].items()]
    verify = (
        'import hashlib,pathlib\n'
        'checks=' + repr(checks) + '\n'
        'root=pathlib.Path(' + repr(str(temporary)) + ').resolve();expected=set()\n'
        'for name,size,sha in checks:\n'
        ' p=pathlib.Path(name);expected.add(p.resolve().relative_to(root).as_posix());h=hashlib.sha256()\n'
        ' with p.open("rb") as f:\n'
        '  for chunk in iter(lambda:f.read(4194304),b""):h.update(chunk)\n'
        ' assert p.is_file() and p.stat().st_size==size and h.hexdigest()==sha\n'
        'actual={p.resolve().relative_to(root).as_posix() for p in root.rglob("*") if p.is_file()}\n'
        'assert actual==expected\n')
    run(['ssh', '-o', 'BatchMode=yes', config['destination_target'], 'python3', '-'], input_text=verify)
    run(['ssh', '-o', 'BatchMode=yes', config['destination_target'], 'sh', '-s', '--',
         str(temporary), str(destination)], input_text='set -eu; mv -- "$1" "$2"')


def publish_tar_direct(config, bundle_target=None, bundle_path=None):
    """Publish a manifest as one uncompressed tar stream and verify every file."""
    target = config['destination_target']
    if target == '@local':
        raise ValueError('tar relay destination must be remote')
    destination = Path(config['destination_root'])
    temporary = Path(str(destination) + '.staging')
    check = 'set -eu; test ! -e "$1"; test ! -e "$2"; mkdir -p "$(dirname "$2")"'
    run(['ssh', '-o', 'BatchMode=yes', target, 'sh', '-s', '--',
         str(destination), str(temporary)], input_text=check)
    try:
        if bundle_target is None:
            stream_tar_tree(config['source_target'], config['source_root'],
                            target, temporary, config['files'])
        else:
            extract_remote_tar(bundle_target, bundle_path, target, temporary)
        verify_and_commit_remote(config, temporary, destination)
    except Exception:
        subprocess.run(['ssh', '-o', 'BatchMode=yes', target, 'sh', '-s', '--', str(temporary)],
                       input='rm -rf -- "$1"', text=True,
                       stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
        raise
    return {name: dict(path=str(destination / str(relative(name))), **expected)
            for name, expected in config['files'].items()}


def publish_direct(config):
    """Stream source files to LAB4 without a controller-local disk copy."""
    if config.get('transport_route') == 'direct-stream' and len(config['files']) > 1:
        return publish_tar_direct(config)
    target = config['destination_target']
    if target == '@local':
        raise ValueError('direct archive destination must be remote')
    destination = Path(config['destination_root'])
    temporary = Path(str(destination) + '.staging')
    script = 'set -eu; test ! -e "$1"; test ! -e "$2"; mkdir -p "$2"'
    run(['ssh', '-o', 'BatchMode=yes', target, 'sh', '-s', '--',
         str(destination), str(temporary)], input_text=script)
    try:
        direct = config.get('transport_route') in ('server-direct', 'lab4-local')
        if config.get('transport_route') == 'server-direct':
            pull_on_lab4(config, temporary)
        elif config.get('transport_route') == 'lab4-local':
            script = ('import json,pathlib,shutil,sys\n'
                      'c=json.load(sys.stdin);src=pathlib.Path(c["source"]);dst=pathlib.Path(c["destination"])\n'
                      'for n in c["files"]:\n'
                      ' s=src/n;d=dst/n;assert s.is_file() and not s.is_symlink()\n'
                      ' d.parent.mkdir(parents=True,exist_ok=True);shutil.copyfile(s,d)\n')
            run(['ssh', '-o', 'BatchMode=yes', target, shlex.join(['python3', '-c', script])],
                input_text=json.dumps(dict(source=config['source_root'], destination=str(temporary),
                                          files=list(config['files']))))
        for name in ([] if direct else config['files']):
            if config['source_target'] == '@local':
                source_root = Path(config['source_root']).resolve()
                source = source_root / str(relative(name))
                if source.is_symlink() or not source.is_file() or not source.resolve().is_relative_to(source_root):
                    raise ValueError('direct archive source is not a regular file: ' + name)
                stream_to_remote(target, temporary, name, source)
            else:
                stream_between_remotes(config['source_target'], config['source_root'],
                                       target, temporary, name)
        checks = [(str(temporary / str(relative(name))), expected['bytes'], expected['sha256'])
                  for name, expected in config['files'].items()]
        verify = (
            'import hashlib,pathlib\n'
            'checks=' + repr(checks) + '\n'
            'for name,size,sha in checks:\n'
            ' p=pathlib.Path(name); h=hashlib.sha256()\n'
            ' with p.open("rb") as f:\n'
            '  for chunk in iter(lambda:f.read(4194304),b""):h.update(chunk)\n'
            ' assert p.is_file() and p.stat().st_size==size and h.hexdigest()==sha\n')
        run(['ssh', '-o', 'BatchMode=yes', target, 'python3', '-'], input_text=verify)
        run(['ssh', '-o', 'BatchMode=yes', target, 'sh', '-s', '--',
             str(temporary), str(destination)], input_text='set -eu; mv -- "$1" "$2"')
    except Exception:
        subprocess.run(['ssh', '-o', 'BatchMode=yes', target, 'sh', '-s', '--', str(temporary)],
                       input='rm -rf -- "$1"', text=True,
                       stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
        raise
    return {name: dict(path=str(destination / str(relative(name))), **expected)
            for name, expected in config['files'].items()}


def publish_via_cps1(config):
    """Use CPS1 disk as the bounded bridge across the FARM/LAB firewall."""
    staging_target = config['staging_target']
    staging = Path(config['staging_root'])
    total = sum(item['bytes'] for item in config['files'].values())
    prepare = (
        'import pathlib,shutil,sys\n'
        'p=pathlib.Path(sys.argv[1]);need=int(sys.argv[2])\n'
        'assert p.name.startswith("relay-") and p.parent.name=="artifact-relay-staging"\n'
        'assert not p.exists() and shutil.disk_usage(p.parent.parent).free>need\n'
        'p.mkdir(parents=True)\n')
    run(['ssh', '-o', 'BatchMode=yes', staging_target, 'python3', '-',
         str(staging), str(config['staging_min_free_bytes'] + total)], input_text=prepare)
    try:
        bundle = staging / 'payload.tar'
        stream_tar_bundle(config['source_target'], config['source_root'],
                          staging_target, bundle, config['files'])
        return publish_tar_direct(config, staging_target, bundle)
    finally:
        cleanup = (
            'import pathlib,shutil,sys\n'
            'p=pathlib.Path(sys.argv[1])\n'
            'assert p.name.startswith("relay-") and p.parent.name=="artifact-relay-staging"\n'
            'shutil.rmtree(p,ignore_errors=True)\n')
        subprocess.run(['ssh', '-o', 'BatchMode=yes', staging_target, 'python3', '-', str(staging)],
                       input=cleanup, text=True, stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)


def main():
    config = json.loads(Path(os.environ['RS_CONFIG_PATH']).read_text())
    attempt_dir = Path(os.environ['RS_ATTEMPT_DIR'])
    if config.get('mode') == 'attempt-archive':
        files = discover_attempt_manifest(config)
        config['files'] = files
        config['manifest_sha256'] = hashlib.sha256(
            json.dumps(files, sort_keys=True, ensure_ascii=False,
                       separators=(',', ':')).encode()).hexdigest()
        verify_archive_capacity(config, attempt_dir)
    staging = None
    if config.get('mode') == 'attempt-archive' and reuse_verified_archive(config):
        files = {name: dict(path=str(Path(config['destination_root']) / name), **value)
                 for name, value in config['files'].items()}
    elif config.get('transport_route') == 'cps1-staging':
        files = publish_via_cps1(config)
    elif config.get('transport_route') in ('direct-stream', 'server-direct', 'lab4-local'):
        files = publish_direct(config)
    else:
        staging = attempt_dir / 'relay-staging'
        staging.mkdir(parents=True, exist_ok=False)
        copy_from_source(config, staging)
        files = publish(config, staging)
    receipt = dict(root=config['destination_root'], attempt=config['source_attempt'],
                   revision='local-relay-' + config['manifest_sha256'], files=files,
                   manifest_sha256=config['manifest_sha256'], transport='controller-local-relay')
    if config.get('mode') == 'attempt-archive':
        receipt.update(attempt_archive=True, complete_attempt=True,
                       source_node=config['source_node'], campaigns=config['campaigns'],
                       experiment=config['experiment'], job=config['job'],
                       transport_route=config['transport_route'])
    receipt_path = attempt_dir / 'HF_RECEIPT.json'
    temporary_receipt = receipt_path.with_suffix('.json.tmp')
    with temporary_receipt.open('w') as stream:
        stream.write(json.dumps(receipt, sort_keys=True) + '\n')
        stream.flush()
        os.fsync(stream.fileno())
    os.replace(temporary_receipt, receipt_path)
    # Persist destination verification before removing the controller copy.
    if staging is not None:
        shutil.rmtree(staging)


if __name__ == '__main__':
    main()
