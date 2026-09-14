"""Standalone HF transfer worker; credentials remain on the executing node."""
import hashlib
import json
import os
import re
import shutil
import stat
import tarfile
import tempfile
import time
from email.utils import parsedate_to_datetime
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
    if config.get('archive_payload', config.get('hf', {}).get('archive_payload', False)):
        return archive_upload(config, api)
    hf = config['hf']
    # Existing repository required: no implicit public repository creation.
    manifest = collect(config)
    data = json.dumps(manifest, sort_keys=True, ensure_ascii=False).encode()
    prefix = '/'.join(x for x in [hf['path_prefix'], config['campaign'], config['job'], config['attempt']] if x)
    ops = [CommitOperationAdd(path_in_repo=prefix + '/' + name,
                              path_or_fileobj=str(Path(config['source_root']) / name))
           for name in manifest['files']]
    ops.append(CommitOperationAdd(path_in_repo=prefix + '/HF_MANIFEST.json', path_or_fileobj=data))
    # Keep commits bounded. Only the last commit includes the manifest; no
    # usable receipt is returned for partial publication or a changed source.
    offset, batch_size = 0, 50
    while offset < len(ops):
        batch = ops[offset:offset + batch_size]
        if offset + batch_size >= len(ops) and collect(config) != manifest:
            raise ValueError('source artifacts changed during upload')
        try:
            info = commit_with_backoff(api, config, repo_id=hf['repo_id'], repo_type=hf['repo_type'], revision=hf['revision'],
                                     operations=batch, commit_message='Scheduler artifacts: ' + config['attempt'])
        except Exception as exc:
            # Some endpoints impose a lower file cap. Shrink only on this
            # explicit rejection, at most 50 -> 25 -> 12 -> 6 -> 3 -> 1.
            if (getattr(getattr(exc, 'response', None), 'status_code', None) != 400
                    or 'too many files' not in str(exc).lower() or len(batch) <= 1
                    or 'git repo would contain' in str(exc).lower()
                    or config.get('diagnostic_no_adaptive', False)):
                raise
            batch_size = max(1, len(batch) // 2)
            print('HF commit file limit: retrying batch with ' + str(batch_size) + ' files', flush=True)
            continue
        offset += len(batch)
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


def rate_limit_delay(exc, now=None):
    now = time.time() if now is None else now
    headers = {k.lower(): v for k,v in (getattr(getattr(exc, 'response', None), 'headers', {}) or {}).items()}
    delays = []
    retry = headers.get('retry-after')
    if retry:
        try: delays.append(float(retry))
        except ValueError:
            try: delays.append(parsedate_to_datetime(retry).timestamp()-now)
            except (TypeError, ValueError, OverflowError): pass
    match = re.search(r'\bt\s*=\s*(\d+)', headers.get('ratelimit', ''))
    if match: delays.append(float(match.group(1)))
    # Repository commit quotas can use a longer window than generic API headers.
    if 'per hour' in str(exc).lower(): delays.append(3600)
    delay = max([60] + delays) if delays else 3600
    if not 0 <= delay <= 21600:
        raise ValueError('HF retry deadline exceeds six-hour safety bound')
    return delay


_last_commit_at = None


class RateLimitDeferred(RuntimeError):
    def __init__(self, delay):
        self.delay = delay
        super().__init__('HF rate limit; retry deferred')


def commit_with_backoff(api, config, **kwargs):
    global _last_commit_at
    interval = config.get('hf', {}).get('commit_interval_s', 0)
    if _last_commit_at is not None and interval:
        pause = interval-(time.monotonic()-_last_commit_at)
        if pause > 0: time.sleep(pause)
    _last_commit_at = time.monotonic()
    try:
        return api.create_commit(**kwargs)
    except Exception as exc:
        if getattr(getattr(exc, 'response', None), 'status_code', None) == 429:
            raise RateLimitDeferred(rate_limit_delay(exc)) from None
        raise


def archive_upload(config, api):
    """Two Hub files, but original per-file identity and download layout."""
    from huggingface_hub import CommitOperationAdd
    manifest = collect(config)
    hf = config['hf']
    prefix = '/'.join(x for x in [hf['path_prefix'], config['campaign'], config['job'], config['attempt']] if x)
    with tempfile.TemporaryDirectory(prefix='hf-archive-', dir=os.environ.get('RS_ATTEMPT_DIR')) as temporary:
        archive = Path(temporary)/'HF_PAYLOAD.tar.gz'
        with tarfile.open(archive, 'w:gz', compresslevel=1) as bundle:
            for name in sorted(manifest['files']):
                source = Path(config['source_root'])/str(relative(name))
                info = bundle.gettarinfo(str(source), arcname=name)
                if not info.isfile():
                    raise ValueError('archive source must be a regular file')
                info.uid = info.gid = info.mtime = 0
                info.uname = info.gname = ''
                with source.open('rb') as stream:
                    bundle.addfile(info, stream)
        if collect(config) != manifest:
            raise ValueError('source artifacts changed during archive creation')
        packed = dict(manifest, archive=dict(file=archive.name, sha256=digest(archive), bytes=archive.stat().st_size))
        data = json.dumps(packed, sort_keys=True, ensure_ascii=False).encode()
        operations = [
            CommitOperationAdd(path_in_repo=prefix+'/'+archive.name, path_or_fileobj=str(archive)),
            CommitOperationAdd(path_in_repo=prefix+'/HF_MANIFEST.json', path_or_fileobj=data)]
        result = commit_with_backoff(api, config, repo_id=hf['repo_id'], repo_type=hf['repo_type'], revision=hf['revision'],
                                   operations=operations, commit_message='Scheduler archived artifacts: '+config['attempt'])
        if collect(config) != manifest:
            raise ValueError('source artifacts changed during upload')
        if not re.fullmatch('[0-9a-f]{40}', result.oid):
            raise ValueError('Hub did not return an immutable commit SHA')
        base = 'https://huggingface.co/' + ('datasets/' if hf['repo_type']=='dataset' else '') + hf['repo_id']
        return dict(repo_id=hf['repo_id'], repo_type=hf['repo_type'], revision=result.oid, path=prefix,
                    url=base+'/tree/'+result.oid+'/'+prefix, manifest_sha256=hashlib.sha256(data).hexdigest(),
                    attempt=config['attempt'], files=manifest['files'], source_root=manifest['source_root'],
                    archive=packed['archive'])


def unpack_archive(source, destination, entries):
    """No extractall: only declared regular files with exact sizes and SHA."""
    seen = set()
    with tarfile.open(source, 'r:gz') as bundle:
        for member in bundle:
            name = member.name
            relative(name)
            if name not in entries or name in seen or not member.isfile() or member.size != entries[name]['bytes']:
                raise ValueError('undeclared, duplicate or invalid archive member')
            target = destination/str(relative(name))
            if not target.resolve().is_relative_to(destination.resolve()):
                raise ValueError('download destination escape')
            target.parent.mkdir(parents=True, exist_ok=True)
            with bundle.extractfile(member) as stream, target.open('wb') as output:
                shutil.copyfileobj(stream, output, 1024*1024)
            if digest(target) != entries[name]['sha256']:
                raise ValueError('download artifact hash mismatch: '+name)
            seen.add(name)
    if seen != set(entries):
        raise ValueError('archive is missing declared artifacts')


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
    archive = manifest.get('archive')
    if archive:
        source = get(str(relative(archive['file'])))
        if source.stat().st_size != archive['bytes'] or digest(source) != archive['sha256']:
            raise ValueError('download archive hash mismatch')
        unpack_archive(source, dest, manifest['files'])
    for name, entry in ([] if archive else manifest['files'].items()):
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


def safe_error_details(exc, secrets=()):
    def redact(value):
        value = str(value)
        for secret in secrets:
            if secret: value = value.replace(secret, '[REDACTED]')
        value = re.sub(r'https?://\S+', '[URL]', value)
        value = re.sub(r'\bhf_[A-Za-z0-9_-]+', '[TOKEN]', value)
        value = re.sub(r'(?i)Bearer\s+\S+', 'Bearer [REDACTED]', value)
        return value[:2000]
    response = getattr(exc, 'response', None)
    headers = getattr(response, 'headers', {}) or {}
    allowed = {'retry-after','ratelimit','ratelimit-policy','x-ratelimit-limit',
               'x-ratelimit-remaining','x-ratelimit-reset','x-error-message','x-error-code'}
    return dict(message=redact(exc), headers={k:redact(v) for k,v in headers.items() if k.lower() in allowed})


def main():
    config = json.loads(Path(os.environ['RS_CONFIG_PATH']).read_text())
    try:
        result = upload(config) if config['direction'] == 'upload' else download(config)
        Path(os.environ['RS_ATTEMPT_DIR'], 'HF_RECEIPT.json').write_text(json.dumps(result))
    except RateLimitDeferred as exc:
        now = time.time()
        retry = dict(attempt=os.environ['RS_ATTEMPT_ID'], http_status=429, created=now, retry_at=now+exc.delay)
        Path(os.environ['RS_ATTEMPT_DIR'], 'HF_RETRY.json').write_text(json.dumps(retry))
        print(json.dumps(dict(event='hf_retry_deferred', **retry)), flush=True)
        raise SystemExit(75)
    except Exception as exc:
        if config.get('capture_safe_error_details'):
            print('HF error details: ' + json.dumps(safe_error_details(exc, [token(config)])), flush=True)
        # HTTP errors may contain signed URLs or credentials; never print str(exc).
        response = getattr(exc, 'response', None)
        code = getattr(response, 'status_code', None)
        # Classify without exposing response bodies, signed URLs, or credentials.
        detail = str(exc).lower()
        categories = [term for term in ('rate limit', 'quota', 'too many', 'too many files', 'too many operations', 'too many requests', 'too many commits', 'too many lfs', 'storage',
                       'conflict', 'precondition', 'unauthorized', 'expired', 'xet') if term in detail]
        vocabulary = {'too','many','files','file','commit','commits','folder','folders','repository','repositories',
                      'regular','lfs','limit','maximum','exceeded','per','payload','rate','requests','minute','hour','day',
                      'operation','operations','non','large','small','binary','text'}
        context = [w for w in re.findall(r'[a-z]+', re.sub(r'https?://\S+', '', detail)) if w in vocabulary][:60]
        print('HF transfer failed: ' + type(exc).__name__ + ' ' +
              json.dumps(dict(http_status=code, categories=categories, limit_context=context)), flush=True)
        raise SystemExit(1)


if __name__ == '__main__':
    main()
