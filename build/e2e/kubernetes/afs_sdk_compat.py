"""Install a separately pinned Environment SDK for the AFS SDK case only."""

import hashlib
import json
import os
from pathlib import Path, PurePosixPath
import re
import shutil
import subprocess
import tempfile

BUILD_ENV = 'AFS_E2E_SDK_CLIENT_BUILD'
COMMIT_ENV = 'AFS_E2E_SDK_CLIENT_COMMIT'
CLIENT = PurePosixPath('/tmp/adx-e2e/afs-sdk-compat-client')
PYTHON = '/opt/adx/client/bin/python'


def client_candidate(directory, build_id, commit):
    if not re.fullmatch(r'[0-9a-f]{8}(?:-[0-9a-f]{4}){3}-[0-9a-f]{12}', build_id):
        raise ValueError(BUILD_ENV + ' must be the SDK artifact Buildkite UUID')
    if not re.fullmatch(r'[0-9a-f]{40}', commit):
        raise ValueError(COMMIT_ENV + ' must pin the SDK commit')
    agent = shutil.which('buildkite-agent')
    if not agent:
        raise ValueError('independent SDK download requires buildkite-agent')
    for pattern in ('sdk-candidate.json', 'adx_environment-*.whl'):
        subprocess.run(
            [str(Path(agent).resolve()), 'artifact', 'download', 'out/buildkite/sdk/' + pattern, '.',
             '--step', 'sdk-package', '--build', build_id],
            cwd=directory, check=True, timeout=150,
        )
    downloaded = directory / 'out/buildkite/sdk'
    manifest = json.loads((downloaded / 'sdk-candidate.json').read_text())
    files = manifest.get('files')
    if (manifest.get('schema_version') != 1 or manifest.get('name') != 'adx-environment'
            or manifest.get('build_id') != build_id or manifest.get('commit') != commit
            or not isinstance(files, dict)):
        raise ValueError('independent SDK artifact identity differs from requested build and commit')
    candidates = list(downloaded.glob('adx_environment-*.whl'))
    name = 'adx_environment-{}-py3-none-any.whl'.format(manifest.get('version'))
    if len(candidates) != 1 or candidates[0].name != name or not re.fullmatch(
        r'adx_environment-[0-9A-Za-z_.]+-py3-none-any\.whl', name
    ):
        raise ValueError('independent SDK build must have exactly its declared wheel')
    wheel = candidates[0]
    digest = hashlib.sha256(wheel.read_bytes()).hexdigest()
    if files.get(name) != digest:
        raise ValueError('independent SDK wheel does not match its SHA256')
    return wheel, {'build_id': build_id, 'commit': commit, 'version': manifest['version'],
                   'wheel_sha256': digest, 'wheel': str(CLIENT / name), 'site': str(CLIENT / 'site')}


def stage_client(run, service):
    build_id = os.getenv(BUILD_ENV, '')
    commit = os.getenv(COMMIT_ENV, '')
    if not (build_id or commit):
        return None
    receipt_path = run.output / 'afs-sdk-client-setup.json'
    report = {'status': 'failed', 'phase': 'client-artifact-setup', 'service': service,
              'requested_client': {'build_id': build_id, 'commit': commit}}
    try:
        with tempfile.TemporaryDirectory(prefix='adx-afs-sdk-client-') as directory:
            wheel, client = client_candidate(Path(directory), build_id, commit)
            # Reuse dependency versions already locked in the service venv. The
            # probe checks both SDK releases declare identical requirements.
            client['native_requirements'] = json.loads(run.execute(
                'node1', PYTHON, '-B', '-c', 'import importlib.metadata as m,json;'
                'print(json.dumps(sorted(m.requires("adx-environment") or [])))'
            ))
            run.execute('node1', 'mkdir', '-m', '0700', str(CLIENT))
            run.kube('-n', run.id, 'cp', str(wheel), 'node1:' + client['wheel'], '-c', 'platform', timeout=120)
            run.execute('node1', 'python3', '-B', '-c',
                        'from pathlib import Path;import hashlib,sys;'
                        'if_bad=hashlib.sha256(Path(sys.argv[1]).read_bytes()).hexdigest()!=sys.argv[2];'
                        'sys.exit("transferred SDK wheel SHA256 mismatch" if if_bad else 0)',
                        client['wheel'], client['wheel_sha256'])
            run.execute('node1', PYTHON, '-B', '-m', 'pip', 'install', '--no-index', '--no-deps',
                        '--no-compile', '--target', client['site'], client['wheel'], timeout=120)
            # Classify import, requirements, and origin failures as setup;
            # do not mistake an unavailable client environment for a product
            # API incompatibility.
            result = run.execute(
                'node1', 'env', 'PYTHONPATH=' + client['site'], PYTHON, '-B', '-c',
                'import json,runpy,sys;'
                'probe=runpy.run_path("/opt/adx/e2e/afs_sdk.py")["verify_client"];'
                'print(json.dumps(probe(json.loads(sys.argv[1]))))',
                json.dumps(client),
            )
        report.update(status='passed', client=json.loads(result))
        return client
    except Exception as exc:
        report['error'] = f'{type(exc).__name__}: {exc}'
        raise
    finally:
        receipt_path.write_text(json.dumps(report, indent=2) + '\n')
