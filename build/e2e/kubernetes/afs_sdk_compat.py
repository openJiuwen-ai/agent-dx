"""Optional frozen-wheel matrix for the existing AFS SDK target."""

import hashlib
import importlib.util
import json
import os
from pathlib import Path
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
INSTALL_DIR = '/opt/adx/client/afs-compat'
PYTHON = '/opt/adx/client/bin/python'
REPLICA_SEED = 'isolated SDK replica'
CANDIDATES = {
    '18f9c673812dbc988185136011023ea28bdd38f7': (
        '0.1.0rc8', 'f6cd599430cde1d2ac3264c328b21e53e9bdfd57c4f2c241063a253c262c03c9'),
    'be16d72463bb30ae355178795fb4476adacf4c6d': (
        '0.1.0rc12', '2fc551baaf684b37c8283b9337ba5c550fa16382ee4b5d07795b2cb8c5bf6b77'),
}
OLD_SERVER = '18f9c673812dbc988185136011023ea28bdd38f7'


def requested():
    return bool(os.getenv(BUILD_ENV) or os.getenv(COMMIT_ENV))


def install_wheel(run):
    build = os.environ.get(BUILD_ENV, '')
    commit = os.environ.get(COMMIT_ENV, '')
    if not re.fullmatch(r'[0-9a-f]{8}(?:-[0-9a-f]{4}){3}-[0-9a-f]{12}', build):
        raise ValueError(BUILD_ENV + ' must be a Buildkite build UUID')
    if commit not in CANDIDATES:
        raise ValueError(COMMIT_ENV + ' must identify the frozen #280 or #294 wheel')
    product = run.harness['product_commit']
    if product not in CANDIDATES:
        raise ValueError('SDK matrix requires a frozen #280 or #294 server')
    version, digest = CANDIDATES[commit]
    wheel_name = 'adx_environment-' + version + '-py3-none-any.whl'
    script = importlib.util.spec_from_file_location('afs_sdk_prepare', HERE.parent / 'prepare.py')
    prepare = importlib.util.module_from_spec(script)
    script.loader.exec_module(prepare)
    agent = shutil.which('buildkite-agent')
    if not agent:
        raise ValueError('SDK compatibility requires buildkite-agent')
    with tempfile.TemporaryDirectory(prefix='afs-sdk-compat-') as temp:
        for name in (wheel_name, 'sdk-candidate.json'):
            subprocess.run([str(Path(agent).resolve()), 'artifact', 'download', 'out/buildkite/sdk/' + name,
                            '.', '--step', 'sdk-package', '--build', build], cwd=temp, timeout=120, check=True)
        sdk = Path(temp) / 'out/buildkite/sdk'
        wheel = sdk / wheel_name
        manifest = prepare.verify_sdk(wheel, sdk / 'sdk-candidate.json', commit)
        if (manifest['build_id'] != build or manifest['version'] != version
                or manifest['files'][wheel_name] != digest):
            raise ValueError('SDK manifest does not match the frozen wheel')
        remote = '/tmp/' + wheel_name
        run.kube('-n', run.id, 'cp', str(wheel), 'node1:' + remote, '-c', 'platform', timeout=60)
        try:
            if run.execute('node1', 'sha256sum', remote).split()[0] != digest:
                raise ValueError('SDK wheel changed in transit')
            run.execute('node1', PYTHON, '-m', 'pip', '--isolated', 'install', '--no-index', '--no-deps',
                        '--no-compile', '--target', INSTALL_DIR, remote, timeout=60)
        finally:
            run.execute('node1', 'rm', '-f', remote)
    identity = {'build': build, 'commit': commit, 'version': version, 'wheel': wheel_name,
                'sha256': digest, 'install_dir': INSTALL_DIR, 'server_commit': product}
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
    config = {'mode': cluster.mode, 'phase': 'files', **mount_config(cluster),
              'explicit_mkdir': identity['version'] == '0.1.0rc12'}
    if cluster.mode == 'ownerfs':
        home = owner_identity(cluster)
        assert home['home_node_id'] == 'node-a', home
        before = {op: owner_payload(cluster, 'B', 'client', op) for op in ('read', 'write')}
    else:
        config['replica_seed'] = REPLICA_SEED
    report = execute_probe(run, identity, config, output / 'sdk-result.json')
    if cluster.mode == 'ownerfs':
        assert owner_identity(cluster) == home, home
        cluster.record('sdk.ownerfs.home', home)
        for op, old in before.items():
            current = owner_payload(cluster, 'B', 'client', op)
            cluster.record('sdk.ownerfs.grpc.' + op, {'before': old, 'after': current})
            assert current - old >= report['cross_node']['bytes_each_way'], (op, old, current)
    else:
        body = REPLICA_SEED.encode() * 2048
        placement(cluster, body, ('node-a', 'node-b'), timeout=30)
        cluster.stop('A')
        stopped = cluster.channel('A').call('snapshot')
        assert 'A' not in stopped['processes'] and not any(
            line.split()[4] == cluster.mount('A') for line in stopped['mounts']), stopped
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
    identity = install_wheel(run)
    current = str(ROOT / 'afs/tests')
    sys.path.insert(0, current)
    failures = []
    # #280 still puts the workspace in a per-root-generation directory.
    old_backing = 'root-' + 'workspace'.encode().hex() + '-e*'
    backing = old_backing if identity['server_commit'] == OLD_SERVER else 'workspace'
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
                                  owner_bind_backing=backing)
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
        sys.path.remove(current)
        run.execute('node1', 'rm', '-rf', INSTALL_DIR)
