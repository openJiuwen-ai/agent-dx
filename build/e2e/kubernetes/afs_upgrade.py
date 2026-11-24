"""AFS-only Buildkite binding for the offline version-switch tests."""

import importlib.util
import json
import os
from pathlib import Path
from posixpath import join as remote_join
import re
import shutil
import signal
import subprocess
import sys
import tempfile

HERE = Path(__file__).resolve().parent
ROOT = HERE.parents[2]
BUILD_ENV = 'AFS_E2E_BASELINE_BUILD'
COMMIT_ENV = 'AFS_E2E_BASELINE_COMMIT'
TARGET = 'afs-component-upgrade-rollback'
UPGRADE_CASE_ENV = 'AFS_E2E_UPGRADE_CASE'
PROTOCOL_TARGET = 'afs-component-mixed-protocols'
PROTOCOL_CASES_ENV = 'AFS_E2E_PROTOCOL_CASES'
PROTOCOL_ADMISSION_ENV = 'AFS_E2E_PROTOCOL_ADMISSION'
PROTOCOL_VERSIONS_ENV = 'AFS_E2E_PROTOCOL_VERSIONS'
BASELINE_POD = 'afs-baseline-input'


def baseline_requested():
    return bool(os.getenv(BUILD_ENV) or os.getenv(COMMIT_ENV))


def download_baseline(directory):
    build_id = os.environ.get(BUILD_ENV, '')
    commit = os.environ.get(COMMIT_ENV, '')
    if not re.fullmatch(r'[0-9a-f]{8}(?:-[0-9a-f]{4}){3}-[0-9a-f]{12}', build_id):
        raise ValueError(BUILD_ENV + ' must be a Buildkite build UUID')
    if not re.fullmatch(r'[0-9a-f]{40}', commit):
        raise ValueError(COMMIT_ENV + ' must be the full baseline SHA')
    agent = shutil.which('buildkite-agent')
    if not agent:
        raise ValueError('baseline download requires buildkite-agent')
    subprocess.run(
        [str(Path(agent).resolve()), 'artifact', 'download', 'out/buildkite/bundle/*.json', '.',
         '--step', 'platform-images', '--build', build_id],
        cwd=directory, check=True, timeout=150,
    )
    bundle = directory / 'out/buildkite/bundle'
    spec = importlib.util.spec_from_file_location('afs_upgrade_candidate', HERE.parent / 'candidate.py')
    candidate = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(candidate)
    manifest, registry = candidate.identity(bundle / 'bundle.json', bundle / 'registry-images.json', commit, True)
    if not candidate.afs_handoff.enabled(manifest['package']):
        raise ValueError('baseline must be an AFS ON build')
    return manifest, registry, candidate.afs_handoff.artifact_identity(manifest['package'], commit)


def collect(component, node, directory, identity):
    directory.mkdir()
    for name in ('afs-meta', 'afs-node'):
        component.copy_from(node, remote_join('/opt/adx/package/bin', name), directory / name)
        (directory / name).chmod(0o755)
    (directory / 'artifact-identity.json').write_text(json.dumps(identity, indent=2) + '\n')


def scenarios(component, checks):
    from manifest import LABEL

    run = component.run
    mixed = component.check_name == PROTOCOL_TARGET
    with run.case(component.check_name, checks) as record, tempfile.TemporaryDirectory(prefix='afs-upgrade-') as temp:
        inputs = Path(temp)
        baseline, registry, old = download_baseline(inputs)
        same_commit = old['commit'] == component.package['commit']
        if old['target'] != component.package['target'] or (mixed and same_commit):
            raise ValueError('version switch requires the same target and mixed protocols require distinct commits')
        image = registry['references']['node']
        pod = {
            'apiVersion': 'v1', 'kind': 'Pod',
            'metadata': {'name': BASELINE_POD, 'namespace': run.id, 'labels': {LABEL: run.id}},
            'spec': {
                'restartPolicy': 'Never', 'automountServiceAccountToken': False,
                'nodeSelector': {'kubernetes.io/os': 'linux', 'kubernetes.io/arch': baseline['architecture']},
                'containers': [{
                    'name': 'platform', 'image': image, 'command': ['sleep', '3600'],
                    'resources': {'requests': {'cpu': '50m', 'memory': '64Mi'},
                                  'limits': {'cpu': '250m', 'memory': '256Mi'}},
                }],
            },
        }
        if component.registry_auth:
            pod['spec']['imagePullSecrets'] = [{'name': 'adx-test-registry'}]
        run.apply(pod)
        run.kube('-n', run.id, 'wait', 'pod/' + BASELINE_POD, '--for=condition=Ready',
                 '--timeout=180s', timeout=200)
        old_inputs = inputs / 'baseline'
        collect(component, BASELINE_POD, old_inputs, old)
        new_inputs = inputs / 'current'
        identity_path = inputs / 'new-identity.json'
        component.copy_from('afs-m', '/opt/adx/e2e/afs-tests/inputs/artifact-identity.json', identity_path)
        new = json.loads(identity_path.read_text())
        if new['commit'] != component.package['commit'] or new['target'] != old['target']:
            raise ValueError('current executable identity differs from its bundle')
        collect(component, 'afs-m', new_inputs, new)
        (run.output / 'baseline.json').write_text(json.dumps({
            'build': os.environ[BUILD_ENV], 'image': image, 'commit': old['commit'], 'target': old['target'],
        }, indent=2) + '\n')

        test_root = ROOT / 'afs/tests'
        path = str(test_root)
        path_added = path not in sys.path
        if path_added:
            sys.path.append(path)
        handlers = {sig: signal.getsignal(sig) for sig in (signal.SIGALRM, signal.SIGTERM, signal.SIGINT)}
        output = run.output / 'afs'
        component.attempted_cases = True
        try:
            module = 'mixed_protocols' if mixed else 'version_switch'
            spec = importlib.util.spec_from_file_location('afs_' + module, test_root / 'tools' / (module + '.py'))
            runner = importlib.util.module_from_spec(spec)
            spec.loader.exec_module(runner)
            signal.signal(signal.SIGALRM, _timeout)
            if mixed:
                admission = os.getenv(PROTOCOL_ADMISSION_ENV)
                versions = os.getenv(PROTOCOL_VERSIONS_ENV)
                requested = os.getenv(PROTOCOL_CASES_ENV)
                if admission is not None and requested is not None:
                    raise ValueError('choose the protocol matrix or one admission direction')
                if (admission is None) != (versions is None):
                    raise ValueError(f'specify {PROTOCOL_ADMISSION_ENV} and {PROTOCOL_VERSIONS_ENV} together')
                selection = (runner.selected_cases(requested) if admission is None
                             else runner.admission_cases(admission))
                expected = len(selection)
                entries, cleanup_errors = runner.exercise(
                    component.config, old_inputs, new_inputs, output, selection,
                    admission=admission, admission_versions=versions,
                )
            else:
                selection = runner.selected_cases(os.getenv(UPGRADE_CASE_ENV), same_commit=same_commit)
                expected = len(selection)
                entries, cleanup_errors = runner.exercise(
                    component.config, old_inputs, new_inputs, output, selection=selection,
                )
        finally:
            for sig, handler in handlers.items():
                signal.signal(sig, handler)
            if path_added:
                sys.path.remove(path)
            cleanup = output / 'cleanup.json'
            if cleanup.is_file():
                component.normal_cleanup = json.loads(cleanup.read_text())
        record['subcases'] = [
            {'id': entry['id'], 'status': entry['status'], 'error': entry.get('error'),
             'phases': entry['phases']}
            for entry in entries
        ]
        if len(entries) != expected or cleanup_errors or any(item['status'] != 'passed' for item in entries):
            if mixed:
                raise ValueError('AFS mixed-protocol assertions failed; see afs/result.json and per-case logs')
            raise ValueError('AFS upgrade/rollback assertions failed; see afs/result.json and per-case logs')


def _timeout(_signum, _frame):
    raise TimeoutError('AFS version-switch case exceeded its time budget')
