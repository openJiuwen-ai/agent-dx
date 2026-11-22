"""Test an independently selected SDK candidate against the deployed AFS candidate."""

import hashlib
from contextlib import contextmanager
import importlib.util
import json
import os
from pathlib import Path
from posixpath import join as remote_join
import re
import shutil
import subprocess
import sys
import tempfile
import uuid


HERE = Path(__file__).resolve().parent
ROOT = HERE.parents[2]
BUILD_ENV = 'AFS_E2E_SDK_COMPAT_BUILD'
COMMIT_ENV = 'AFS_E2E_SDK_COMPAT_COMMIT'
DIRECTORY_ENV = 'AFS_E2E_SDK_COMPAT_DIR'
INSTALL_DIR = '/opt/adx/client/afs-compat'
PYTHON = '/opt/adx/client/bin/python'
REPLICA_SEED = 'isolated SDK replica'
SDK_MANIFEST = 'sdk-candidate.json'


def requested():
    return bool(os.getenv(BUILD_ENV) or os.getenv(COMMIT_ENV) or os.getenv(DIRECTORY_ENV))


@contextmanager
def wheel_inputs(build):
    directory = os.getenv(DIRECTORY_ENV)
    if directory:
        yield Path(directory).resolve(strict=True), None
        return
    agent = shutil.which('buildkite-agent')
    if not agent:
        raise ValueError('provide a frozen wheel directory with ' + DIRECTORY_ENV + ' or use buildkite-agent')
    with tempfile.TemporaryDirectory(prefix='afs-sdk-compat-') as temp:
        def download(name):
            subprocess.run([str(Path(agent).resolve()), 'artifact', 'download', 'out/buildkite/sdk/' + name,
                            '.', '--step', 'sdk-package', '--build', build], cwd=temp, timeout=120, check=True)

        download(SDK_MANIFEST)
        yield Path(temp) / 'out/buildkite/sdk', download


def candidate_wheel(candidate, build, commit):
    if not isinstance(candidate, dict) or candidate.get('schema_version') != 1:
        raise ValueError('invalid SDK candidate manifest')
    if candidate.get('build_id') != build or candidate.get('commit') != commit:
        raise ValueError('SDK candidate manifest does not match the requested build UUID and commit')
    version, files = candidate.get('version'), candidate.get('files')
    if candidate.get('name') != 'adx-environment' or not isinstance(version, str) or not isinstance(files, dict):
        raise ValueError('invalid SDK package identity in candidate manifest')
    if not re.fullmatch(r'[a-zA-Z0-9][a-zA-Z0-9_.+!]*', version):
        raise ValueError('invalid SDK version in candidate manifest')
    wheel = f'adx_environment-{version}-py3-none-any.whl'
    digest = files.get(wheel)
    if not isinstance(digest, str) or not re.fullmatch(r'[0-9a-f]{64}', digest):
        raise ValueError('SDK candidate manifest must include the versioned wheel and its SHA256')
    return wheel, version, digest


def install_wheel(product):
    run = product.run
    build = os.environ.get(BUILD_ENV, '')
    commit = os.environ.get(COMMIT_ENV, '')
    if not re.fullmatch(r'[0-9a-f]{8}(?:-[0-9a-f]{4}){3}-[0-9a-f]{12}', build):
        raise ValueError(BUILD_ENV + ' must be a Buildkite build UUID')
    if not re.fullmatch(r'[0-9a-f]{40}', commit):
        raise ValueError(COMMIT_ENV + ' must be the full SDK commit SHA')
    server_commit = run.harness['product_commit']
    script = importlib.util.spec_from_file_location('afs_sdk_prepare', HERE.parent / 'prepare.py')
    prepare = importlib.util.module_from_spec(script)
    script.loader.exec_module(prepare)
    with wheel_inputs(build) as (sdk, download):
        manifest_path = sdk / SDK_MANIFEST
        manifest = json.loads(manifest_path.read_text())
        wheel_name, version, digest = candidate_wheel(manifest, build, commit)
        if download is not None:
            download(wheel_name)
        wheel = sdk / wheel_name
        if prepare.verify_sdk(wheel, manifest_path, commit) != manifest:
            raise ValueError('SDK candidate manifest changed during verification')
        remote = remote_join('/tmp', wheel_name)
        product.copy_to('node1', wheel, remote)
        try:
            if run.execute('node1', 'sha256sum', remote).split()[0] != digest:
                raise ValueError('SDK wheel changed in transit')
            run.execute('node1', PYTHON, '-m', 'pip', '--isolated', 'install', '--no-index', '--no-deps',
                        '--no-compile', '--target', INSTALL_DIR, remote, timeout=60)
        finally:
            run.execute('node1', 'rm', '-f', remote)
    identity = {'build': build, 'commit': commit, 'version': version, 'wheel': wheel_name,
                'sha256': digest, 'install_dir': INSTALL_DIR, 'server_commit': server_commit}
    (run.output / 'afs-sdk-compat-identity.json').write_text(json.dumps(identity, indent=2) + '\n')
    return identity


def execute_probe(run, identity, config, destination):
    input_path, output_path = '/evidence/afs-sdk-compat-input.json', '/evidence/afs-sdk-compat-output.json'
    run.execute('node1', 'python3', '-c', 'from pathlib import Path;import sys;'
                'Path(sys.argv[1]).write_text(sys.argv[2])', input_path, json.dumps({**config, 'sdk': identity}))
    run.execute('node1', 'rm', '-f', output_path)
    run.execute('node1', 'env', 'PYTHONPATH=' + INSTALL_DIR, PYTHON, '-B',
                '/opt/adx/e2e/afs_sdk_compat_probe.py', '--input', input_path, '--output', output_path, timeout=600)
    report = json.loads(run.execute('node1', 'cat', output_path))
    destination.write_text(json.dumps(report, indent=2) + '\n')
    for node in run.nodes:
        run.helper(node, 'empty', node)
    if report['status'] != 'passed' or report['cleanup_errors']:
        raise ValueError('SDK probe failed; see ' + str(destination))
    return report


def mount_config(cluster):
    identities = {}
    for role in ('A', 'B'):
        visible = cluster.python(role,
            'from pathlib import Path\npid=int(Path("/tmp/adx-e2e/sandboxd.pid").read_text());'
            'assert os.readlink("/proc/self/ns/mnt")==os.readlink(f"/proc/{pid}/ns/mnt");'
            's=os.stat(sys.argv[1]);print(json.dumps([s.st_dev,s.st_ino]))',
            cluster.workspace(role), owner=False)
        identities[role] = json.loads(visible['stdout'])
    cluster.record('sandboxd.mount_namespace_visible', identities)
    return {'paths': {role: cluster.workspace(role) for role in identities}, 'identities': identities}


def exercise(product, identity, cluster, output, record):
    from tools.mixed_protocols import owner_payload
    from tools.protocol_admission import placement
    from tools.version_switch import owner_identity

    run = product.run
    cluster.prepare()
    config = {'mode': cluster.mode, 'phase': 'files', **mount_config(cluster)}
    if cluster.mode == 'ownerfs':
        home = owner_identity(cluster)
        if home['home_node_id'] != 'node-a':
            raise AssertionError(home)
        before = {op: owner_payload(cluster, 'B', 'client', op) for op in ('read', 'write')}
    else:
        config['replica_seed'] = REPLICA_SEED
    report = execute_probe(run, identity, config, output / 'sdk-result.json')
    if cluster.mode == 'ownerfs':
        if owner_identity(cluster) != home:
            raise AssertionError(home)
        cluster.record('sdk.ownerfs.home', home)
        for op, old in before.items():
            current = owner_payload(cluster, 'B', 'client', op)
            cluster.record('sdk.ownerfs.grpc.' + op, {'before': old, 'after': current})
            if current - old < report['cross_node']['bytes_each_way']:
                raise AssertionError((op, old, current))
    else:
        body = REPLICA_SEED.encode() * 2048
        placement(cluster, body, ('node-a', 'node-b'), timeout=30)
        cluster.stop('A')
        stopped = cluster.channel('A').call('snapshot')
        writer_mounted = any(line.split()[4] == cluster.mount('A') for line in stopped['mounts'])
        if 'A' in stopped['processes'] or writer_mounted:
            raise AssertionError(stopped)
        cluster.record('sdk.replica.writer_stopped', 'A')
        execute_probe(run, identity, {**config, 'phase': 'replica', 'replica_sha256': hashlib.sha256(body).hexdigest()},
                      output / 'sdk-replica.json')
    record['subcases'] = [{'id': 'common-sdk-and-afs', 'status': 'passed'}]
    feature = report.get('recursive_false')
    if feature:
        record['subcases'].append({'id': 'recursive-false', **feature})
    return feature


def scenarios(product, checks):
    run = product.run
    identity = install_wheel(product)
    current = str(ROOT / 'afs/tests')
    path_added = current not in sys.path
    if path_added:
        sys.path.append(current)
    failures = []
    retired = False
    try:
        from fixtures.common import Case, DfsProfile
        from fixtures.multivm import Cluster

        execute_probe(run, identity, {'mode': 'warm', 'phase': 'warm'}, run.output / 'afs-sdk-compat-warm.json')
        with tempfile.TemporaryDirectory(prefix='adx-sdk-afs-') as temp:
            inputs = Path(temp)
            for name in ('afs-meta', 'afs-node', 'artifact-identity.json'):
                source = '/opt/adx/e2e/afs-tests/inputs/' if name.endswith('.json') else '/opt/adx/package/bin/'
                product.copy_from('node1', source + name, inputs / name)
                if not name.endswith('.json'):
                    (inputs / name).chmod(0o755)
            for mode in ('ownerfs', 'dfs'):
                output = run.output / 'afs-sdk' / mode
                output.mkdir(parents=True)
                profile = DfsProfile(copies=2, replica_failure='strict') if mode == 'dfs' else None
                case = Case('afs.sdk.compat.' + mode, ('cluster-sdk',), mode, (), None, profile=profile)
                cluster = Cluster(product.config, inputs, output, case, 'afs-test-' + uuid.uuid4().hex,
                                  owner_bind_backing=None)
                product.active = cluster
                try:
                    with run.case('afs-sdk-' + mode, checks) as record:
                        try:
                            feature = exercise(product, identity, cluster, output, record)
                        except Exception:
                            for node in reversed(run.nodes):
                                try:
                                    run.helper(node, 'empty', node)
                                except Exception:
                                    retired = True
                                    try:
                                        run.helper(node, 'cleanup', node, timeout=180)
                                    except Exception as exc:
                                        product.cleanup_errors.append('runtime cleanup: ' + str(exc))
                            raise
                        finally:
                            cleanup = cluster.cleanup()
                            product.active = None
                            if cleanup['status'] != 'PASS':
                                product.cleanup_errors.extend(cleanup['errors'] or ['AFS cleanup unconfirmed'])
                            if product.cleanup_errors:
                                raise ValueError('; '.join(product.cleanup_errors))
                        if feature and feature['status'] != 'passed':
                            raise ValueError('SDK recursive=False contract failed; see sdk-result.json')
                except Exception as exc:
                    failures.append(f'{mode}: {exc}')
                    if retired or product.cleanup_errors:
                        break
        if failures:
            raise ValueError('; '.join(failures))
    finally:
        if path_added:
            sys.path.remove(current)
        run.execute('node1', 'rm', '-rf', INSTALL_DIR)
