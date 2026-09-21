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
    p.add_argument('--firecracker-kit',type=Path)
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
    collector=json.loads((ROOT/'build/observability/source.json').read_text())
    collector_image=os.getenv('ADX_COLLECTOR_IMAGE',collector['image'])
    if '@sha256:' not in collector_image:raise ValueError('digest-pinned Collector image required')
    fc_kit=None
    if a.firecracker_kit:
        from firecracker.kit import verify as verify_kit
        fc_kit=verify_kit(a.firecracker_kit,backend)
    uid=uuid.uuid4().hex[:12];tags={
        'node':f'adx-e2e-node:{uid}',
        'rrt':f'adx-e2e-rrt:{uid}',
        'entrypoint':f'adx-e2e-entrypoint:{uid}',
    }
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
        shutil.copytree(ROOT/'build/observability',context/'observability')
        shutil.copytree(ROOT/'build/e2e',context/'e2e',ignore=shutil.ignore_patterns('__pycache__','tests'))
        shutil.copy2(ROOT/'build/ci/rpc_certificates.py',context/'e2e/rpc_certificates.py')
        shutil.copy2(ROOT/'build/release/package.py',context/'e2e/package.py')
        if fc_kit:
            for name in fc_kit['files']:
                output=context/'fc-kit'/name;output.parent.mkdir(parents=True,exist_ok=True)
                shutil.copy2(a.firecracker_kit/name,output)
                if not name.startswith('artifacts/'):output.chmod(0o755)
            (context/'fc-kit/manifest.json').write_text(json.dumps(fc_kit,indent=2))
        (context/'Dockerfile.node').write_text('ARG BASE\nARG COLLECTOR\nFROM ${COLLECTOR} AS collector\nFROM ${BASE}\nCOPY --from=collector /otelcol-contrib /usr/local/bin/otelcol-contrib\nCOPY observability /opt/adx/observability\nCOPY package /opt/adx/package\nCOPY backend /usr/local/bin\nCOPY e2e /opt/adx/e2e\nRUN python3 -m venv /opt/adx/client && /opt/adx/client/bin/pip install /opt/adx/package/sdk/*.whl\nWORKDIR /opt/adx\nCMD ["sleep", "infinity"]\n')
        if fc_kit:
            with (context/'Dockerfile.node').open('a') as dockerfile:
                dockerfile.write('COPY fc-kit /opt/adx-fc\nCOPY fc-kit/tools /opt/adx/tools\n')
        (context/'Dockerfile.rrt').write_text(
            'ARG BASE\nFROM ${BASE}\n'
            'COPY package/runtime/rrt-runtime /usr/local/bin/rrt-runtime\n'
            'RUN mkdir -p /__adx && ln -s /usr /__adx/usr && ln -s /bin /__adx/bin '
            '&& ln -s /sbin /__adx/sbin && ln -s /etc /__adx/etc\n'
            'ENTRYPOINT ["/usr/local/bin/rrt-runtime"]\n')
        (context/'Dockerfile.entrypoint').write_text(
            'ARG BASE\nFROM ${BASE}\n'
            # The inherited process must outlive Firecracker boot, RRT
            # readiness and route publication. Exiting during that window is
            # correctly treated as a failed Capsule start.
            'ENTRYPOINT ["/bin/sh", "-c", "sleep 30; echo adx-entrypoint-stderr >&2; exit 7"]\n')
        for role,base in [('node',a.runtime_base),('rrt',a.rrt_base),('entrypoint',a.rrt_base)]:
            subprocess.run(['docker','build','--progress=plain','--provenance=false','--build-arg','BASE='+base,'--build-arg','COLLECTOR='+collector_image,'-f',str(context/f'Dockerfile.{role}'),'-t',tags[role],str(context)],stderr=subprocess.STDOUT,check=True,timeout=900)
        images={role:json.loads(subprocess.check_output(['docker','image','inspect',tag]))[0] for role,tag in tags.items()}
        subprocess.run(['docker','save','-o',str(a.output/'images.tar'),*tags.values()],stderr=subprocess.STDOUT,check=True,timeout=600)
        subprocess.run(['docker','save','-o',str(a.output/'rrt.tar'),tags['rrt']],stderr=subprocess.STDOUT,check=True,timeout=300)
        subprocess.run(['docker','save','-o',str(a.output/'entrypoint.tar'),tags['entrypoint']],stderr=subprocess.STDOUT,check=True,timeout=300)
    result={'schema_version':1,'package':manifest,'backend':backend,'image_ids':{k:v['Id'] for k,v in images.items()},'architecture':images['node']['Architecture'],'archive_sha256':sha(a.output/'images.tar'),'rrt_archive_sha256':sha(a.output/'rrt.tar'),'entrypoint_archive_sha256':sha(a.output/'entrypoint.tar'),'base_images':{'node':a.runtime_base,'rrt':a.rrt_base}}
    result['collector']={**collector,'image':collector_image}
    if fc_kit:result['firecracker_kit']=fc_kit
    (a.output/'bundle.json').write_text(json.dumps(result,indent=2)+'\n')
    print('E2E bundle prepared:',a.output)

if __name__=='__main__':main()
