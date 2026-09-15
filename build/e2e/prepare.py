#!/usr/bin/env python3
"""Build portable E2E images from a verified release; deployment never compiles."""
import argparse
import hashlib
import importlib.util
import json
import os
from pathlib import Path
import shutil
import subprocess
import tempfile
import uuid

ROOT = Path(__file__).resolve().parents[2]
spec = importlib.util.spec_from_file_location('release_package', ROOT/'build/release/package.py')
package = importlib.util.module_from_spec(spec);spec.loader.exec_module(package)
BACKEND_BINARIES = ('sandboxd','sbox','runc','runc-shim','sandbox-logger','redis-cli')

def sha(path):
    h=hashlib.sha256()
    with path.open('rb') as f:
        for data in iter(lambda:f.read(1024*1024),b''):h.update(data)
    return h.hexdigest()

def main():
    p=argparse.ArgumentParser()
    for name in ('package','backend','output'):p.add_argument('--'+name,type=Path,required=True)
    p.add_argument('--runtime-base',required=True);p.add_argument('--rrt-base',required=True)
    a=p.parse_args();a.output.mkdir(parents=True,exist_ok=False)
    manifest=package.verify(a.package)
    if os.getenv('BUILDKITE'):
        if manifest['dirty'] or manifest['commit'] != os.environ['BUILDKITE_COMMIT']:
            raise ValueError('CI requires a clean package from the current commit')
        if any('@sha256:' not in x for x in (a.runtime_base,a.rrt_base)):
            raise ValueError('CI base images must be digest pinned')
    backend=json.loads((a.backend/'manifest.json').read_text())
    pinned=json.loads((ROOT/'third_party/sandboxd/source.json').read_text())
    if backend['sandboxd_revision'] != pinned['revision'] or backend['target'] != manifest['target']:
        raise ValueError('backend revision or architecture mismatch')
    for name in BACKEND_BINARIES:
        path=a.backend/name
        if path.is_symlink() or sha(path) != backend['files'][name]:raise ValueError('backend integrity mismatch')
    uid=uuid.uuid4().hex[:12];tags={'node':f'adx-e2e-node:{uid}','rrt':f'adx-e2e-rrt:{uid}'}
    with tempfile.TemporaryDirectory(prefix='adx-e2e-image-') as d:
        context=Path(d)
        shutil.copytree(a.package,context/'package')
        # Artifact transport may drop Unix mode bits; identity was verified above.
        for name in package.BINARIES:
            (context/'package/bin'/name).chmod(0o755)
        (context/'package/bin/redis-server').chmod(0o755)
        (context/'package/runtime/rrt-runtime').chmod(0o755)
        (context/'backend').mkdir()
        for name in BACKEND_BINARIES:
            shutil.copy2(a.backend/name,context/'backend'/name)
            (context/'backend'/name).chmod(0o755)
        shutil.copytree(ROOT/'build/e2e',context/'e2e',ignore=shutil.ignore_patterns('__pycache__','tests'))
        shutil.copy2(ROOT/'build/ci/rpc_certificates.py',context/'e2e/rpc_certificates.py')
        (context/'Dockerfile.node').write_text('ARG BASE\nFROM ${BASE}\nCOPY package /opt/adx/package\nCOPY backend /usr/local/bin\nCOPY e2e /opt/adx/e2e\nRUN python3 -m venv /opt/adx/client && /opt/adx/client/bin/pip install /opt/adx/package/sdk/*.whl\nWORKDIR /opt/adx\nCMD ["sleep", "infinity"]\n')
        (context/'Dockerfile.rrt').write_text('ARG BASE\nFROM ${BASE}\nCOPY package/runtime/rrt-runtime /usr/local/bin/rrt-runtime\nENTRYPOINT ["/usr/local/bin/rrt-runtime"]\n')
        for role,base in [('node',a.runtime_base),('rrt',a.rrt_base)]:
            subprocess.run(['docker','build','--progress=plain','--provenance=false','--build-arg','BASE='+base,'-f',str(context/f'Dockerfile.{role}'),'-t',tags[role],str(context)],stderr=subprocess.STDOUT,check=True,timeout=900)
        images={role:json.loads(subprocess.check_output(['docker','image','inspect',tag]))[0] for role,tag in tags.items()}
        subprocess.run(['docker','save','-o',str(a.output/'images.tar'),*tags.values()],stderr=subprocess.STDOUT,check=True,timeout=600)
        subprocess.run(['docker','save','-o',str(a.output/'rrt.tar'),tags['rrt']],stderr=subprocess.STDOUT,check=True,timeout=300)
    result={'schema_version':1,'package':manifest,'backend':backend,'image_ids':{k:v['Id'] for k,v in images.items()},'architecture':images['node']['Architecture'],'archive_sha256':sha(a.output/'images.tar'),'rrt_archive_sha256':sha(a.output/'rrt.tar'),'base_images':{'node':a.runtime_base,'rrt':a.rrt_base}}
    (a.output/'bundle.json').write_text(json.dumps(result,indent=2)+'\n')
    print('E2E bundle prepared:',a.output)

if __name__=='__main__':main()
