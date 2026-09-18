"""Controller-local, checksum-verified dependency artifact relay worker."""
import hashlib
import json
import os
import shlex
import shutil
import subprocess
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


def main():
    config = json.loads(Path(os.environ['RS_CONFIG_PATH']).read_text())
    attempt_dir = Path(os.environ['RS_ATTEMPT_DIR'])
    staging = attempt_dir / 'relay-staging'
    staging.mkdir(parents=True, exist_ok=False)
    copy_from_source(config, staging)
    files = publish(config, staging)
    receipt = dict(root=config['destination_root'], attempt=config['source_attempt'],
                   revision='local-relay-' + config['manifest_sha256'], files=files,
                   manifest_sha256=config['manifest_sha256'], transport='controller-local-relay')
    # Destination contents and hashes were verified by publish().  The bounded
    # controller-local staging copy is no longer authoritative or needed.
    shutil.rmtree(staging)
    (attempt_dir / 'HF_RECEIPT.json').write_text(json.dumps(receipt, sort_keys=True) + '\n')


if __name__ == '__main__':
    main()
