#!/usr/bin/env python3
"""Build portable E2E images from a verified release; deployment never compiles."""
import argparse
import hashlib
import importlib.util
import json
import os
from pathlib import Path
import shutil
import stat
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


def verify_sdk(wheel, candidate_path, expected_commit):
    candidate = json.loads(candidate_path.read_text())
    files = candidate.get('files')
    if candidate.get('schema_version') != 1 or not isinstance(files, dict):
        raise ValueError('invalid SDK candidate manifest')
    if candidate.get('commit') != expected_commit:
        raise ValueError('SDK and platform commits differ')
    if wheel.name not in files or files[wheel.name] != sha(wheel):
        raise ValueError('SDK artifact digest mismatch')
    if (Path(wheel.name).name != wheel.name or not wheel.name.startswith('adx_sandbox-')
            or not wheel.name.endswith('-py3-none-any.whl')):
        raise ValueError('invalid SDK wheel name')
    return candidate


def verify_runsc(binary, target):
    """Pin an optional native runsc used only by the heterogeneous E2E case."""
    machines = {'x86_64-unknown-linux-gnu': 62,
                'aarch64-unknown-linux-gnu': 183}
    if target not in machines:
        raise ValueError('unsupported release architecture')
    if binary.is_symlink() or not binary.is_file() or not binary.stat().st_mode & stat.S_IXUSR:
        raise ValueError('runsc must be a regular executable file')
    with binary.open('rb') as stream:
        header = stream.read(20)
    if len(header) != 20 or header[:6] != b'\x7fELF\x02\x01':
        raise ValueError('runsc must be a little-endian ELF64 binary')
    if int.from_bytes(header[18:20], 'little') != machines[target]:
        raise ValueError('runsc architecture differs from the release')
    return sha(binary)

def main():
    p=argparse.ArgumentParser()
    for name in ('package','backend','sdk-wheel','sdk-candidate','output'):
        p.add_argument('--'+name,type=Path,required=True)
    p.add_argument('--runtime-base',required=True);p.add_argument('--execd-base',required=True)
    p.add_argument('--firecracker-kit',type=Path)
    p.add_argument('--runsc-bin',type=Path,
                   help='optional verified native runsc for the heterogeneous runtime E2E')
    a=p.parse_args();a.output.mkdir(parents=True,exist_ok=False)
    manifest=package.verify(a.package)
    runsc_sha256 = verify_runsc(a.runsc_bin, manifest['target']) if a.runsc_bin else None
    expected_commit=os.getenv('ADX_E2E_ARTIFACT_COMMIT',os.getenv('BUILDKITE_COMMIT',manifest['commit']))
    sdk=verify_sdk(a.sdk_wheel,a.sdk_candidate,expected_commit)
    if os.getenv('BUILDKITE'):
        if manifest['dirty'] or manifest['commit'] != expected_commit:
            raise ValueError('CI requires a clean package from the current commit')
        if any('@sha256:' not in x for x in (a.runtime_base,a.execd_base)):
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
        'execd':f'adx-e2e-execd:{uid}',
        'entrypoint':f'adx-e2e-entrypoint:{uid}',
    }
    with tempfile.TemporaryDirectory(prefix='adx-e2e-image-') as d:
        context=Path(d)
        shutil.copytree(a.package,context/'package')
        (context/'sdk').mkdir()
        shutil.copy2(a.sdk_wheel,context/'sdk'/a.sdk_wheel.name)
        shutil.copy2(a.sdk_candidate,context/'sdk'/'sdk-candidate.json')
        # Artifact transport may drop Unix mode bits; identity was verified above.
        for name in package.BINARIES:
            (context/'package/bin'/name).chmod(0o755)
        (context/'package/bin/redis-server').chmod(0o755)
        (context/'package/runtime/adx-execd').chmod(0o755)
        (context/'backend').mkdir()
        for name in BACKEND_BINARIES:
            shutil.copy2(a.backend/name,context/'backend'/name)
            (context/'backend'/name).chmod(0o755)
        if a.runsc_bin:
            shutil.copy2(a.runsc_bin, context/'runsc')
            (context/'runsc').chmod(0o755)
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
        (context/'Dockerfile.node').write_text('ARG BASE\nARG COLLECTOR\nFROM ${COLLECTOR} AS collector\nFROM ${BASE}\nCOPY --from=collector /otelcol-contrib /usr/local/bin/otelcol-contrib\nCOPY observability /opt/adx/observability\nCOPY package /opt/adx/package\nCOPY sdk /opt/adx/sdk\nCOPY backend /usr/local/bin\nCOPY e2e /opt/adx/e2e\nRUN python3 -m venv /opt/adx/client && /opt/adx/client/bin/pip install /opt/adx/sdk/*.whl\nWORKDIR /opt/adx\nCMD ["sleep", "infinity"]\n')
        if a.runsc_bin:
            with (context/'Dockerfile.node').open('a') as dockerfile:
                dockerfile.write('COPY runsc /usr/local/bin/runsc\n')
        if fc_kit:
            with (context/'Dockerfile.node').open('a') as dockerfile:
                dockerfile.write('COPY fc-kit /opt/adx-fc\nCOPY fc-kit/tools /opt/adx/tools\n')
        (context/'Dockerfile.execd').write_text(
            'ARG BASE\nFROM ${BASE}\n'
            'COPY package/runtime/adx-execd /usr/local/bin/adx-execd\n'
            'RUN mkdir -p /__adx && ln -s /usr /__adx/usr && ln -s /bin /__adx/bin '
            '&& ln -s /sbin /__adx/sbin && ln -s /etc /__adx/etc\n'
            'ENTRYPOINT ["/usr/local/bin/adx-execd"]\n')
        (context/'Dockerfile.entrypoint').write_text(
            'ARG BASE\nFROM ${BASE}\n'
            # The inherited process must outlive Firecracker boot, EXECD
            # readiness and route publication. Exiting during that window is
            # correctly treated as a failed Environment start.
            'ENTRYPOINT ["/bin/sh", "-c", "sleep 30; echo adx-entrypoint-stderr >&2; exit 7"]\n')
        for role,base in [('node',a.runtime_base),('execd',a.execd_base),('entrypoint',a.execd_base)]:
            subprocess.run(['docker','build','--progress=plain','--provenance=false','--build-arg','BASE='+base,'--build-arg','COLLECTOR='+collector_image,'-f',str(context/f'Dockerfile.{role}'),'-t',tags[role],str(context)],stderr=subprocess.STDOUT,check=True,timeout=900)
        images={role:json.loads(subprocess.check_output(['docker','image','inspect',tag]))[0] for role,tag in tags.items()}
        subprocess.run(['docker','save','-o',str(a.output/'images.tar'),*tags.values()],stderr=subprocess.STDOUT,check=True,timeout=600)
        subprocess.run(['docker','save','-o',str(a.output/'execd.tar'),tags['execd']],stderr=subprocess.STDOUT,check=True,timeout=300)
        subprocess.run(['docker','save','-o',str(a.output/'entrypoint.tar'),tags['entrypoint']],stderr=subprocess.STDOUT,check=True,timeout=300)
    result={'schema_version':1,'package':manifest,'sdk':sdk,'backend':backend,'image_ids':{k:v['Id'] for k,v in images.items()},'architecture':images['node']['Architecture'],'archive_sha256':sha(a.output/'images.tar'),'execd_archive_sha256':sha(a.output/'execd.tar'),'entrypoint_archive_sha256':sha(a.output/'entrypoint.tar'),'base_images':{'node':a.runtime_base,'execd':a.execd_base}}
    result['collector']={**collector,'image':collector_image}
    if runsc_sha256:result['runsc']={'sha256':runsc_sha256,'target':manifest['target']}
    if fc_kit:result['firecracker_kit']=fc_kit
    (a.output/'bundle.json').write_text(json.dumps(result,indent=2)+'\n')
    print('E2E bundle prepared:',a.output)

if __name__=='__main__':main()
