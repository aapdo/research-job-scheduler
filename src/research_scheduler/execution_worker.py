"""Controller-local preparation worker: pinned SSH copies, verify, then receipt."""
import hashlib
import json
import os
from pathlib import Path, PurePosixPath
import shlex
import subprocess
import tempfile


REMOTE = r'''
import hashlib,json,os,shutil,subprocess,sys,tempfile
from pathlib import Path
x=json.load(sys.stdin);root=Path(x['root']);action=x['action'];files=x['files']
assert root.is_absolute() and len(root.parts)>=4
def digest(p):
 h=hashlib.sha256()
 with p.open('rb') as f:
  for b in iter(lambda:f.read(4<<20),b''):h.update(b)
 return h.hexdigest()
def checked(base,name,expected):
 p=base/name
 assert p.resolve().is_relative_to(base.resolve()),name
 if not p.exists():return False
 assert p.is_file() and not p.is_symlink() and p.stat().st_uid==os.getuid(),name
 assert p.stat().st_size==expected['bytes'] and digest(p)==expected['sha256'],name
 return True
if action=='inspect':
 root.mkdir(parents=True,exist_ok=True)
 missing=[name for name,expected in files.items() if not checked(root,name,expected)]
 need=sum(files[name]['bytes'] for name in missing)
 usage=shutil.disk_usage(root)
 assert usage.free>need+(8<<30),'insufficient preparation disk reserve'
 if x.get('farm_local_cap') and missing:
  fs=subprocess.check_output(['stat','-f','-c','%T',str(root)],text=True).strip()
  if fs not in ('nfs','nfs4'):
   assert usage.free-need-(8<<30)>=usage.total*.15,'Farm local filesystem 85% usage cap'
 stage=tempfile.mkdtemp(prefix='.execution-stage-',dir=root) if missing else None
 print(json.dumps(dict(missing=missing,stage=stage)))
elif action=='verify_source':
 assert all(checked(root,n,e) for n,e in files.items()),'source files missing'
 print('{}')
elif action=='publish':
 stage=Path(x['stage']);assert stage.parent==root and stage.name.startswith('.execution-stage-') and not stage.is_symlink()
 assert all(checked(stage,n,e) for n,e in files.items()),'staged bytes differ'
 for name,expected in files.items():
  target=root/name
  assert target.resolve().is_relative_to(root.resolve())
  target.parent.mkdir(parents=True,exist_ok=True)
  try:os.link(stage/name,target)
  except FileExistsError:assert checked(root,name,expected),'refuse overwrite'
 assert all(checked(root,n,e) for n,e in files.items())
 shutil.rmtree(stage) # exact mkdtemp-owned staging tree, never the destination root
 print('{}')
'''


def sha(path):
    return hashlib.sha256(Path(path).read_bytes()).hexdigest()


def remote(target, data):
    if target=='@local':
        import sys
        result=subprocess.run([sys.executable,'-c',REMOTE],input=json.dumps(data),text=True,stdout=subprocess.PIPE,stderr=subprocess.PIPE,timeout=600)
        if result.returncode:raise RuntimeError('local frozen source verification failed')
        return json.loads(result.stdout)
    if not target or target.startswith('-') or any(c.isspace() for c in target):
        raise ValueError('invalid registered SSH target')
    result=subprocess.run(['ssh','-T','-o','BatchMode=yes','-o','ConnectTimeout=10',target,shlex.join(['python3','-c',REMOTE])],input=json.dumps(data),text=True,stdout=subprocess.PIPE,stderr=subprocess.PIPE,timeout=600)
    if result.returncode:
        raise RuntimeError('registered remote preparation verification failed on '+target)
    return json.loads(result.stdout)


def copy_set(target, recipe, output):
    if sha(recipe['manifest_file'])!=recipe['manifest_sha256']:
        raise ValueError('registered copy manifest changed')
    files=json.loads(Path(recipe['manifest_file']).read_text())['files']
    for name,row in files.items():
        path=PurePosixPath(name)
        if path.is_absolute() or '..' in path.parts or chr(0) in name or not name:
            raise ValueError('unsafe manifest path')
        if set(row)!={'bytes','sha256'} or row['bytes']<0 or len(row['sha256'])!=64:
            raise ValueError('invalid manifest entry')
    result=remote(target,dict(action='inspect',root=recipe['destination_root'],files=files,farm_local_cap=target.startswith('farm')))
    if not result['missing']:return dict(files=len(files),copied=0)
    missing={name:files[name] for name in result['missing']}
    remote(recipe['source_target'],dict(action='verify_source',root=recipe['source_root'],files=missing))
    tar=['tar','-C',recipe['source_root'],'--no-recursion','--null','-T','-','-cf','-']
    source=tar if recipe['source_target']=='@local' else ['ssh','-T',recipe['source_target'],shlex.join(tar)]
    destination=['ssh','-T',target,shlex.join(['tar','-C',result['stage'],'--keep-old-files','--no-same-owner','-xf','-'])]
    with tempfile.TemporaryFile() as names, (output/'copy.log').open('ab') as log:
        names.write(b''.join(name.encode()+b'\0' for name in missing));names.seek(0)
        sender=subprocess.Popen(source,stdin=names,stdout=subprocess.PIPE,stderr=log)
        receiver=subprocess.Popen(destination,stdin=sender.stdout,stdout=log,stderr=log)
        sender.stdout.close()
        try:
            destination_code=receiver.wait(timeout=3600);source_code=sender.wait(timeout=30)
            if destination_code or source_code:raise RuntimeError('pinned copy failed; destination originals preserved')
        finally:
            for child in (sender,receiver):
                if child.poll() is None:child.terminate()
    remote(target,dict(action='publish',root=recipe['destination_root'],stage=result['stage'],files=missing))
    return dict(files=len(files),copied=len(missing))


def main():
    config=json.loads(Path(os.environ['RS_CONFIG_PATH']).read_text())
    recipe=config['recipe'];output=Path(os.environ['RS_ATTEMPT_DIR'])
    encoded=json.dumps(recipe,sort_keys=True,ensure_ascii=False,allow_nan=False).encode()
    if hashlib.sha256(encoded).hexdigest()!=config['recipe_sha256']:
        raise ValueError('preparation recipe identity changed')
    evidence=[copy_set(recipe['target'],item,output) for item in recipe['copies']]
    command=['ssh','-T','-o','BatchMode=yes','-o','ConnectTimeout=10',recipe['target'],shlex.join(recipe['verify_argv'])]
    with (output/'verification.log').open('wb') as log:
        result=subprocess.run(command,stdout=log,stderr=subprocess.STDOUT,timeout=600)
    if result.returncode:raise RuntimeError('registered execution/data/framework verifier failed')
    receipt=dict(status='complete',profile=config['profile'],node=config['node'],recipe_sha256=config['recipe_sha256'],copies=evidence,verification='registered verifier succeeded; no scientific training claimed')
    (output/'EXECUTION_READY.json').write_text(json.dumps(receipt,sort_keys=True)+'\n')


if __name__=='__main__':main()
