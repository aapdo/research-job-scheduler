"""Bounded controller-disk chunk relay with source fallback and atomic publish."""
import hashlib
import json
import os
from pathlib import Path,PurePosixPath
import shlex
import shutil
import subprocess
import sys
import tempfile

from research_scheduler.execution_worker import remote

CHUNK_BYTES=64<<20

RECEIVER=r'''
import hashlib,json,subprocess,sys
from pathlib import Path
stage=Path(sys.argv[1]);assert stage.name.startswith('.execution-stage-') and not stage.is_symlink()
tar=subprocess.Popen(['tar','-C',str(stage),'--no-same-owner','--keep-old-files','-xf','-'],stdin=subprocess.PIPE)
chunks=[]
try:
 while True:
  line=sys.stdin.buffer.readline(200);size,expected=line.decode().strip().split(' ');size=int(size)
  assert 0<=size<=64<<20
  if size==0:break
  data=sys.stdin.buffer.read(size);assert len(data)==size
  actual=hashlib.sha256(data).hexdigest();assert actual==expected,'relay chunk SHA mismatch'
  tar.stdin.write(data);chunks.append(actual)
 tar.stdin.close();assert tar.wait(timeout=120)==0
 print(json.dumps(dict(chunks=chunks)))
finally:
 if tar.poll() is None:tar.terminate()
'''

MARKER=r'''
import hashlib,json,os,sys,tempfile
from pathlib import Path
x=json.load(sys.stdin);root=Path(x['root']);p=root/'.resource-ready'/x['resource']
assert p.resolve().is_relative_to(root.resolve())
p.parent.mkdir(exist_ok=True);data=x['content'].encode()
assert hashlib.sha256(data).hexdigest()==x['sha256']
if p.exists():assert p.is_file() and not p.is_symlink() and p.read_bytes()==data
else:
 fd,name=tempfile.mkstemp(prefix='.marker-',dir=p.parent)
 with os.fdopen(fd,'wb') as f:f.write(data);f.flush();os.fsync(f.fileno())
 try:os.link(name,p)
 except FileExistsError:assert p.read_bytes()==data
 os.unlink(name)
print('{}')
'''

def command(target,args):
    if target=='@local':return args
    if not target or target.startswith('-') or any(c.isspace() for c in target):raise ValueError('invalid registered SSH target')
    return ['ssh','-T','-o','BatchMode=yes','-o','ConnectTimeout=8',target,shlex.join(args)]

def relay(source,target,stage,files,out):
    """Retain bounded relay chunks until destination readback succeeds."""
    relay_root=Path('/home/jy/tmp');relay_root.mkdir(exist_ok=True)
    needed=sum(v['bytes'] for v in files.values())+len(files)*2048+(8<<30)
    assert shutil.disk_usage(relay_root).free>needed,'insufficient local relay disk reserve'
    # Only this exact mkdtemp tree is removed, and only after success.
    directory=Path(tempfile.mkdtemp(prefix='resource-relay-',dir=relay_root))
    chunks=[]
    with tempfile.TemporaryFile() as names,(out/'transfer.log').open('ab') as log:
        names.write(b''.join(n.encode()+b'\0' for n in files));names.seek(0)
        sender=subprocess.Popen(command(source['target'],['tar','-C',source['root'],'--no-recursion','--null','-T','-','-cf','-']),stdin=names,stdout=subprocess.PIPE,stderr=log)
        receiver=subprocess.Popen(command(target,['python3','-c',RECEIVER,stage]),stdin=subprocess.PIPE,stdout=subprocess.PIPE,stderr=log)
        try:
            while True:
                data=sender.stdout.read(CHUNK_BYTES)
                if not data:break
                digest=hashlib.sha256(data).hexdigest()
                chunk=directory/(str(len(chunks)).zfill(8)+'.chunk');chunk.write_bytes(data)
                # Local disk readback, then receiver checks each framed chunk.
                payload=chunk.read_bytes();assert hashlib.sha256(payload).hexdigest()==digest
                receiver.stdin.write((str(len(payload))+' '+digest+'\n').encode());receiver.stdin.write(payload);receiver.stdin.flush()
                chunks.append(dict(index=len(chunks),bytes=len(payload),sha256=digest))
                (directory/'CHUNKS.json').write_text(json.dumps(chunks))
            assert sender.wait(timeout=30)==0,'source stream failed'
            receiver.stdin.write(b'0 end\n');receiver.stdin.close()
            result=json.loads(receiver.stdout.read());assert receiver.wait(timeout=120)==0
            assert result['chunks']==[r['sha256'] for r in chunks]
        finally:
            for child in (sender,receiver):
                if child.poll() is None:child.terminate()
                if child.stdin and not child.stdin.closed:child.stdin.close()
                if child.stdout:child.stdout.close()
                try:child.wait(timeout=10)
                except subprocess.TimeoutExpired:child.kill();child.wait(timeout=10)
    return directory,chunks

def run(config,out):
    path=Path(config['manifest_file']);raw=path.read_bytes()
    assert hashlib.sha256(raw).hexdigest()==config['manifest_sha256'],'resource manifest changed'
    files=json.loads(raw)['files'];assert files,'empty resource'
    for name,row in files.items():
        p=PurePosixPath(name)
        assert name and not p.is_absolute() and '..' not in p.parts and '\x00' not in name
        assert set(row)=={'bytes','sha256'} and type(row['bytes']) is int and row['bytes']>=0
        assert len(row['sha256'])==64 and all(c in '0123456789abcdef' for c in row['sha256'])
    target=config['target'];root=config['root']
    observed=remote(target,dict(action='inspect',root=root,files=files,farm_local_cap=target.startswith('farm')))
    missing={n:files[n] for n in observed['missing']};chosen=None;failures=[];chunks=[];relay_dir=None
    if missing:
        for source in config['sources']:
            try:
                remote(source['target'],dict(action='verify_source',root=source['root'],files=missing))
                chosen=source;break
            except Exception as exc:
                failures.append(dict(node=source['node'],error=type(exc).__name__))
        if chosen is None:raise RuntimeError('No registered source passed byte verification: '+json.dumps(failures))
        relay_dir,chunks=relay(chosen,target,observed['stage'],missing,out)
        remote(target,dict(action='publish',root=root,stage=observed['stage'],files=missing))
    marker=config['marker']
    request=dict(root=root,resource=config['resource'],content=marker['content'],sha256=marker['sha256'])
    subprocess.run(command(target,['python3','-c',MARKER]),input=json.dumps(request),text=True,capture_output=True,check=True,timeout=60)
    receipt=dict(status='complete',resource=config['resource'],manifest_sha256=config['manifest_sha256'],node=config['node'],root=root,
        source=chosen['node'] if chosen else config['node'],files=len(files),copied=len(missing),copied_bytes=sum(v['bytes'] for v in missing.values()),
        source_failures=failures,chunks=chunks,verified_at=__import__('time').time())
    (out/'RESOURCE_READY.json').write_text(json.dumps(receipt,sort_keys=True)+'\n')
    if relay_dir:
        # Destination file SHA and chunk readbacks are confirmed above.
        shutil.rmtree(relay_dir)
    return receipt

def main():
    import signal
    def timeout(*args):raise TimeoutError('resource worker exceeded bounded transfer budget')
    signal.signal(signal.SIGALRM,timeout);signal.alarm(3600)
    run(json.loads(Path(os.environ['RS_CONFIG_PATH']).read_text()),Path(os.environ['RS_ATTEMPT_DIR']))

if __name__=='__main__':main()
