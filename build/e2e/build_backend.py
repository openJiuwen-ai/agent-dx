#!/usr/bin/env python3
"""Build the pinned external backend independently of the ADX product package."""
import argparse
import hashlib
import json
import os
from pathlib import Path
import platform
import re
import shutil
import subprocess
import tempfile
import urllib.request
ROOT=Path(__file__).resolve().parents[2]
def run(args,**kw):return subprocess.check_output(list(map(str,args)),text=True,**kw).strip()
def sha(p):return hashlib.sha256(p.read_bytes()).hexdigest()
def release_checksum(manifest, filename):
    matches=[]
    for line in manifest.splitlines():
        parts=line.split()
        if len(parts)==2 and parts[1].lstrip('*')==filename:
            if not re.fullmatch(r'[0-9a-fA-F]{64}',parts[0]):
                raise ValueError('invalid release checksum for '+filename)
            matches.append(parts[0].lower())
    if len(matches)!=1:
        raise ValueError('expected one release checksum for '+filename)
    return matches[0]

def main():
    p=argparse.ArgumentParser();p.add_argument('--output',type=Path,required=True);p.add_argument('--source',type=Path);p.add_argument('--redis-cli',type=Path,required=True);p.add_argument('--jobs',type=int,default=2);a=p.parse_args()
    if platform.system()!='Linux' or a.jobs<1:raise ValueError('native Linux builder and positive jobs required')
    arch={'x86_64':'amd64','aarch64':'arm64'}.get(platform.machine())
    if not arch:raise ValueError('unsupported architecture')
    target={'amd64':'x86_64-unknown-linux-gnu','arm64':'aarch64-unknown-linux-gnu'}[arch]
    pinned=json.loads((ROOT/'third_party/sandboxd/source.json').read_text())
    a.output.mkdir(parents=True,exist_ok=False)
    with tempfile.TemporaryDirectory(prefix='adx-sandboxd-build-') as tmp:
        source=a.source.resolve() if a.source else Path(tmp)/'source'
        if a.source is None:
            run(['git','init',source]);run(['git','-C',source,'remote','add','origin',pinned['repository']]);run(['git','-C',source,'fetch','--depth=1','origin',pinned['revision']]);run(['git','-C',source,'checkout','--detach','FETCH_HEAD'])
        if run(['git','-C',source,'rev-parse','HEAD'])!=pinned['revision'] or run(['git','-C',source,'status','--porcelain']):raise ValueError('sandboxd source must match the clean pinned revision')
        for file in pinned['files']:
            if sha(source/file['source'])!=file['sha256']:raise ValueError('sandboxd source integrity mismatch')
        env={**os.environ,'GOFLAGS':'-p='+str(a.jobs),'GOMAXPROCS':str(a.jobs)}
        subprocess.run(['make','RELEASE_GOARCH='+arch,'release-binary','release-cli','runc-shim','sandbox-logger'],cwd=source,env=env,check=True)
        for name in ('sandboxd','sbox','runc-shim','sandbox-logger'):shutil.copy2(source/'output'/name,a.output/name)
        versions=dict(line.split('=',1) for line in (source/'third_party/runtime-versions.env').read_text().splitlines() if line and not line.startswith('#') and '=' in line)
        url=versions['RUNC_RELEASE_BASE_URL']+'/v'+versions['RUNC_VERSION']+'/'
        with urllib.request.urlopen(url+'runc.sha256sum',timeout=60) as r:sums=r.read().decode()
        expected=release_checksum(sums,'runc.'+arch)
        if arch=='amd64' and expected!=versions['RUNC_AMD64_SHA256']:raise ValueError('runc release manifest mismatch')
        with urllib.request.urlopen(url+'runc.'+arch,timeout=120) as r:(a.output/'runc').write_bytes(r.read())
        if sha(a.output/'runc')!=expected:raise ValueError('runc checksum mismatch')
        (a.output/'runc').chmod(0o755)
        if '7.2.5' not in run([a.redis_cli.resolve(),'--version']):raise ValueError('Redis CLI version mismatch')
        shutil.copy2(a.redis_cli,a.output/'redis-cli')
        files={p.name:sha(p) for p in a.output.iterdir() if p.is_file()}
        (a.output/'manifest.json').write_text(json.dumps({'sandboxd_revision':pinned['revision'],'target':target,'runc_version':versions['RUNC_VERSION'],'files':files},indent=2)+'\n')
if __name__=='__main__':main()
