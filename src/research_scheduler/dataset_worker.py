"""Standalone dataset prepare/verify worker. All paths and commands are registered."""
import hashlib
import json
import os
from pathlib import Path
import subprocess


def sha(path):
    h=hashlib.sha256()
    with Path(path).open('rb') as f:
        for block in iter(lambda:f.read(4<<20),b''):h.update(block)
    return h.hexdigest()


def main():
    config=json.loads(Path(os.environ['RS_CONFIG_PATH']).read_text())
    contract=config['contract'];recipe=config['recipe'];root=Path(recipe['path'])
    marker=root/contract['identity_file']
    reused=marker.is_file()
    if reused and sha(marker)!=contract['identity_sha256']:
        raise ValueError('existing dataset identity differs; refuse overwrite')
    if not reused:
        subprocess.run(recipe['prepare_argv'],cwd=recipe['cwd'],check=True)
    if not marker.is_file() or sha(marker)!=contract['identity_sha256']:
        raise ValueError('dataset identity missing or mismatched after preparation')
    subprocess.run(recipe['verify_argv'],cwd=recipe['cwd'],check=True)
    # Bind the configured metadata after domain-specific validation succeeds.
    evidence={}
    for name in contract['metadata_files']:
        p=root/name
        if not p.is_file() or not p.resolve().is_relative_to(root.resolve()):
            raise ValueError('metadata file missing/escaping dataset root')
        evidence[name]=sha(p)
    if sha(marker)!=contract['identity_sha256']:
        raise ValueError('dataset changed during verification')
    result=dict(status='complete',dataset=contract['id'],version=contract['version'],
                path=str(root),identity_sha256=sha(marker),metadata_sha256=evidence,
                verification='registered verifier exited successfully',reused=reused)
    Path(os.environ['RS_ATTEMPT_DIR'],'DATASET_READY.json').write_text(json.dumps(result))


if __name__=='__main__':main()
