"""Standalone HF transfer worker; credentials remain on the executing node."""
import hashlib
import json
import os
import re
import shutil
import stat
from pathlib import Path, PurePosixPath


def digest(path):
    h = hashlib.sha256()
    with Path(path).open('rb') as f:
        for chunk in iter(lambda: f.read(4 * 1024 * 1024), b''):
            h.update(chunk)
    return h.hexdigest()


def relative(value):
    p = PurePosixPath(value)
    if not value or p.is_absolute() or '..' in p.parts or '\\' in value or '\x00' in value:
        raise ValueError('unsafe artifact path')
    return p


def token(config):
    path = config.get('token_file')
    if not path:
        return None  # huggingface_hub uses the node's existing login.
    p = Path(path)
    s = p.stat()
    if s.st_uid != os.getuid() or stat.S_IMODE(s.st_mode) & 0o077:
        raise ValueError('HF token file must be private and owned by executing user')
    return p.read_text().strip()


def collect(config):
    root = Path(config['source_root']).absolute()
    files = {}
    for pattern in config['patterns']:
        relative(pattern)
        matches = sorted(root.glob(pattern))
        if not matches:
            raise ValueError('artifact pattern matched no files: ' + pattern)
        for p in matches:
            if p.is_dir():
                raise ValueError('declare file globs, not directories')
            if p.is_symlink() or not p.resolve().is_relative_to(root.resolve()) or not p.is_file():
                raise ValueError('artifact escapes attempt directory')
            name = p.relative_to(root).as_posix()
            if name == 'HF_MANIFEST.json':
                raise ValueError('reserved manifest filename')
            files[name] = {'sha256': digest(p), 'bytes': p.stat().st_size}
    for name, expected in config['outputs'].items():
        if name not in files or files[name]['sha256'] != expected['sha256']:
            raise ValueError('successful output changed before upload: ' + name)
    relocations = []
    for pattern in config.get('relocate_json', []):
        relative(pattern)
        matches = [p.relative_to(root).as_posix() for p in root.glob(pattern)]
        if not matches or any(p not in files or not p.endswith('.json') for p in matches):
            raise ValueError('JSON relocation must select exported JSON files')
        relocations.extend(matches)
    def strings(value):
        if isinstance(value, str):
            yield value
        elif isinstance(value, list):
            for v in value:
                yield from strings(v)
        elif isinstance(value, dict):
            for v in value.values():
                yield from strings(v)
    for name in relocations:
        for value in strings(json.loads((root/name).read_text())):
            if value.startswith(str(root) + '/'):
                reference = value[len(str(root)) + 1:]
                relative(reference)
                if reference not in files and not any(f.startswith(reference + '/') for f in files):
                    raise ValueError('descriptor references an unexported attempt artifact: ' + reference)
    return dict(version=1, attempt=config['attempt'], job=config['job'],
                source_root=str(root), files=files, outputs=list(config['outputs']),
                relocate_json=sorted(set(relocations)))


def upload(config, api=None):
    from huggingface_hub import HfApi, CommitOperationAdd
    api = api or HfApi(token=token(config))
    hf = config['hf']
    # Existing repository required: no implicit public repository creation.
    manifest = collect(config)
    data = json.dumps(manifest, sort_keys=True, ensure_ascii=False).encode()
    prefix = '/'.join(x for x in [hf['path_prefix'], config['campaign'], config['job'], config['attempt']] if x)
    ops = [CommitOperationAdd(path_in_repo=prefix + '/' + name,
                              path_or_fileobj=str(Path(config['source_root']) / name))
           for name in manifest['files']]
    ops.append(CommitOperationAdd(path_in_repo=prefix + '/HF_MANIFEST.json', path_or_fileobj=data))
    info = api.create_commit(repo_id=hf['repo_id'], repo_type=hf['repo_type'], revision=hf['revision'],
                             operations=ops, commit_message='Scheduler artifacts: ' + config['attempt'])
    # Detect mutation during a transfer; never publish a usable receipt for it.
    if collect(config) != manifest:
        raise ValueError('source artifacts changed during upload')
    revision = info.oid
    if not re.fullmatch('[0-9a-f]{40}', revision):
        raise ValueError('Hub did not return an immutable commit SHA')
    base = 'https://huggingface.co/' + ('datasets/' if hf['repo_type'] == 'dataset' else '') + hf['repo_id']
    return dict(repo_id=hf['repo_id'], repo_type=hf['repo_type'], revision=revision, path=prefix,
                url=base + '/tree/' + revision + '/' + prefix,
                manifest_sha256=hashlib.sha256(data).hexdigest(), attempt=config['attempt'],
                files=manifest['files'], source_root=manifest['source_root'])


def relocate(value, old, new):
    if isinstance(value, str):
        return new + value[len(old):] if value == old or value.startswith(old + '/') else value
    if isinstance(value, list):
        return [relocate(v, old, new) for v in value]
    if isinstance(value, dict):
        return {k: relocate(v, old, new) for k, v in value.items()}
    return value


def download(config, fetch=None):
    from huggingface_hub import hf_hub_download
    fetch = fetch or hf_hub_download
    receipt = config['receipt']
    if not re.fullmatch('[0-9a-f]{40}', receipt['revision']):
        raise ValueError('downloads require full immutable commit SHA')
    dest = Path(config['destination'])
    dest.mkdir(parents=True, exist_ok=True)
    common = dict(repo_id=receipt['repo_id'], repo_type=receipt['repo_type'],
                  revision=receipt['revision'], token=token(config),
                  cache_dir=str(dest.parent / 'hub-cache'))
    def get(name):
        relative(name)
        return Path(fetch(filename=receipt['path'] + '/' + name, **common))
    manifest_path = get('HF_MANIFEST.json')
    if digest(manifest_path) != receipt['manifest_sha256']:
        raise ValueError('download manifest hash mismatch')
    manifest = json.loads(manifest_path.read_text())
    if manifest['attempt'] != receipt['attempt'] or manifest['files'] != receipt['files']:
        raise ValueError('download manifest identity mismatch')
    for name, entry in manifest['files'].items():
        source = get(name)
        if source.stat().st_size != entry['bytes'] or digest(source) != entry['sha256']:
            raise ValueError('download artifact hash mismatch: ' + name)
        target = dest / str(relative(name))
        if not target.resolve().is_relative_to(dest.resolve()):
            raise ValueError('download destination escape')
        target.parent.mkdir(parents=True, exist_ok=True)
        shutil.copyfile(source, target)
    # Only declared descriptors are rewritten. Originals stay in the Hub cache.
    derived = {}
    for name in manifest['relocate_json']:
        path = dest / str(relative(name))
        value = relocate(json.loads(path.read_text()), manifest['source_root'], str(dest))
        path.write_text(json.dumps(value, sort_keys=True, ensure_ascii=False) + '\n')
        derived[name] = dict(original_sha256=manifest['files'][name]['sha256'], sha256=digest(path))
    files = {name: dict(path=str(dest/name), sha256=digest(dest/name), bytes=(dest/name).stat().st_size)
             for name in manifest['files']}
    return dict(root=str(dest), attempt=receipt['attempt'], revision=receipt['revision'],
                url=receipt['url'], files=files, derived=derived,
                manifest_sha256=receipt['manifest_sha256'])


def main():
    config = json.loads(Path(os.environ['RS_CONFIG_PATH']).read_text())
    try:
        result = upload(config) if config['direction'] == 'upload' else download(config)
        Path(os.environ['RS_ATTEMPT_DIR'], 'HF_RECEIPT.json').write_text(json.dumps(result))
    except Exception as exc:
        # HTTP errors may contain signed URLs or credentials; never print str(exc).
        print('HF transfer failed: ' + type(exc).__name__, flush=True)
        raise SystemExit(1)


if __name__ == '__main__':
    main()
