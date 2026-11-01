"""Run existing AFS component assertions in owned local and cross-Pod environments."""

import base64
from copy import deepcopy
import importlib.util
import json
from pathlib import Path
import signal
import sys
import tempfile

HERE = Path(__file__).resolve().parent
ROOT = HERE.parents[2]
if str(HERE) not in sys.path:
    sys.path.insert(0, str(HERE))
from manifest import LABEL, resources as platform_resources

CASES = ('ownerfs.cluster.read_write', 'ownerfs.cluster.permissions',
         'dfs.cluster.read_write', 'dfs.cluster.visibility', 'cluster.topology_cleanup')
scope_spec = importlib.util.spec_from_file_location('afs_ci_full_scope', ROOT / 'afs/tests/suites/ci_full.py')
scope = importlib.util.module_from_spec(scope_spec)
scope_spec.loader.exec_module(scope)
FULL_CASES = scope.CASE_IDS
ROLES = {'M': 'afs-m', 'A': 'afs-a', 'B': 'afs-b'}


def resources(namespace, image, architecture, registry_auth=False, node_names=(), distinct_workers=False, full=False):
    # Reuse the public placement, registry and namespace/image validation contract.
    template = next(item for item in platform_resources(namespace, image, architecture, registry_auth, node_names)
                    if item['kind'] == 'Pod')
    objects = []
    roles = dict(ROLES, C='afs-c') if full else ROLES
    for role, name in roles.items():
        pod = deepcopy(template)
        pod['metadata'] = {'name': name, 'namespace': namespace, 'labels': {LABEL: namespace, 'afs.e2e.role': role}}
        spec = pod['spec']
        spec['affinity'].pop('podAntiAffinity', None)
        container = spec['containers'][0]
        container['env'] = [
            {'name': 'PYTHONDONTWRITEBYTECODE', 'value': '1'},
            {'name': 'AFS_TEST_POD_UID', 'valueFrom': {'fieldRef': {'fieldPath': 'metadata.uid'}}},
        ]
        container['volumeMounts'] = [{'name': 'state', 'mountPath': '/a'},
                                     {'name': 'fuse', 'mountPath': '/dev/fuse'}]
        if role == 'M':
            # This Pod runs only Meta, rather than the full ADX platform stack.
            container['resources'] = {'requests': {'cpu': '250m', 'memory': '256Mi'},
                                      'limits': {'cpu': '1', 'memory': '1Gi'}}
        if full and role == 'M':
            container['resources'] = {'requests': {'cpu': '1', 'memory': '2Gi'},
                                      'limits': {'cpu': '4', 'memory': '4Gi'}}
        spec['containers'] = [container]
        spec['volumes'] = [{'name': 'state', 'emptyDir': {}},
                           {'name': 'fuse', 'hostPath': {'path': '/dev/fuse', 'type': 'CharDevice'}}]
        if full and role == 'M':
            # /sys/class/bdi links here. Cache probes change only their own FUSE BDI
            # and restore its value; no host block-device or global sysctl writes.
            spec['volumes'].append({'name': 'bdi', 'hostPath': {
                'path': '/sys/devices/virtual/bdi', 'type': 'Directory'}})
            container['volumeMounts'].append({'name': 'bdi', 'mountPath': '/sys/devices/virtual/bdi',
                                              'readOnly': False})
        if role in ('A', 'B') and distinct_workers:
            pod['metadata']['labels']['afs.e2e.data'] = 'true'
            pod['metadata']['labels']['afs.e2e.primary-data'] = 'true'
            spec['affinity']['podAntiAffinity'] = {'requiredDuringSchedulingIgnoredDuringExecution': [{
                'labelSelector': {'matchLabels': {LABEL: namespace, 'afs.e2e.primary-data': 'true'}},
                'topologyKey': 'kubernetes.io/hostname',
            }]}
        objects.append(pod)
    return objects


def validate_result(report, cleanup, full=False):
    expected = FULL_CASES if full else CASES
    records = report.get('cases', [])
    actual = [item.get('id') for item in records]
    if (report.get('status') != 'PASS' or report.get('cleanup_errors') or cleanup.get('status') != 'PASS'
            or cleanup.get('errors') or set(report.get('selected_cases', [])) != set(expected)
            or len(actual) != len(expected) or set(actual) != set(expected)
            or any(item.get('status') != 'PASS' for item in records)):
        raise ValueError('AFS required component assertions or normal cleanup did not pass completely')
    if full and (report.get('suite') != 'ci-full' or report.get('partial') is not False):
        raise ValueError('complete declared ci-full suite required')
    if len(report.get('selected_cases', [])) != len(expected):
        raise ValueError('duplicate selected AFS cases')


class Component:
    def __init__(self, run, full=False):
        self.run = run
        self.full = full
        self.roles = dict(ROLES, C='afs-c') if full else ROLES
        self.attempted_cases = False
        self.normal_cleanup = None
        self.config = None
        self.package = None

    def deploy(self, bundle, refs, registry_auth, node_names, distinct_workers, harness_commit):
        run = self.run
        self.package = bundle['package']
        if self.full:
            if not bundle.get('afs_contracts'):
                raise ValueError('AFS Full requires frozen Rust contract assets')
            self.contract_manifest_sha = bundle['afs_contracts']['sha256']
        if not self.package.get('with_afs'):
            raise ValueError('AFS component acceptance requires an AFS ON candidate')
        if sys.version_info < (3, 8):
            raise ValueError('AFS controller requires Python 3.8 or newer')
        objects = resources(run.id, refs['node'], bundle['architecture'], registry_auth is not None,
                            node_names, distinct_workers, full=self.full)
        run.kube('version', '-o', 'json', timeout=30)
        run.namespace_attempted = True
        run.apply({'apiVersion': 'v1', 'kind': 'Namespace', 'metadata': {'name': run.id, 'labels': {
            LABEL: run.id, 'pod-security.kubernetes.io/enforce': 'privileged',
        }}})
        run.namespace_uid = json.loads(run.kube('get', 'namespace', run.id, '-o', 'json'))['metadata']['uid']
        if registry_auth:
            auth = json.loads(registry_auth.read_text())
            if not isinstance(auth.get('auths'), dict):
                raise ValueError('registry auth must contain auths')
            run.apply({'apiVersion': 'v1', 'kind': 'Secret', 'type': 'kubernetes.io/dockerconfigjson',
                       'metadata': {'name': 'adx-test-registry', 'namespace': run.id},
                       'data': {'.dockerconfigjson': base64.b64encode(registry_auth.read_bytes()).decode()}})
        (run.output / 'resources.json').write_text(json.dumps({'kind': 'List', 'items': objects}, indent=2))
        for obj in objects:
            run.apply(obj)
            run.nodes.append(obj['metadata']['name'])
        run.kube('-n', run.id, 'wait', 'pod', '--all', '--for=condition=Ready', '--timeout=300s', timeout=320)
        inventory = json.loads(run.kube('-n', run.id, 'get', 'pods', '-o', 'json'))['items']
        pods = {p['metadata']['name']: p for p in inventory}
        placement = [{'pod': name, 'uid': pods[name]['metadata']['uid'], 'host': pods[name]['spec']['nodeName'],
                      'ip': pods[name]['status']['podIP'], 'role': role} for role, name in self.roles.items()]
        (run.output / 'placement.json').write_text(json.dumps(placement, indent=2))
        if distinct_workers and len({p['host'] for p in placement if p['role'] in ('A', 'B')}) != 2:
            raise ValueError('AFS A/B roles require two distinct Kubernetes workers')
        run.sync_harness(harness_commit, self.package['commit'], self.package)
        # Tool contracts share the worker's Python; controller remains compatible with Python 3.8.
        run.execute(ROLES['M'], 'python3', '-B', '-m', 'unittest', 'discover',
                    '-s', '/opt/adx/e2e/afs-tests/runner_tests', '-v', timeout=180)
        self.config = {
            'kind': 'kubernetes', 'uid': 10000, 'gid': 10000, 'other_uid': 10001, 'other_gid': 10001,
            'roles': {item['role']: {'host': item['role'].lower(), 'address': item['ip']} for item in placement},
            'hosts': {item['role'].lower(): {
                'transport': 'kubernetes', 'base': '/a', 'kubectl': run.kubectl,
                'namespace': run.id, 'namespace_uid': run.namespace_uid, 'pod': item['pod'],
                'pod_uid': item['uid'], 'pod_ip': item['ip'], 'container': 'platform',
            } for item in placement},
        }
        (run.output / 'afs-environment.json').write_text(json.dumps(self.config, indent=2))
        if self.full:
            local = dict(self.config, topology='local', roles={
                role: {'host': 'local', 'address': '127.0.0.' + str(i)} for i, role in enumerate('MABC', 2)},
                hosts={'local': self.config['hosts']['m']})
            self.config = {'kind': 'matrix', 'environments': {'local': local, 'cluster': self.config}}
            (run.output / 'afs-environment.json').write_text(json.dumps(self.config, indent=2))

    def scenarios(self, checks):
        run = self.run
        with run.case('afs-component-full' if self.full else 'afs-component', checks) as record, \
                tempfile.TemporaryDirectory(prefix='adx-afs-inputs-') as temp:
            inputs = Path(temp)
            for name in ('afs-meta', 'afs-node'):
                run.kube('-n', run.id, 'cp', ROLES['M'] + ':/opt/adx/package/bin/' + name,
                         str(inputs / name), '-c', 'platform', timeout=120)
                (inputs / name).chmod(0o755)
            run.kube('-n', run.id, 'cp', ROLES['M'] + ':/opt/adx/e2e/afs-tests/inputs/artifact-identity.json',
                     str(inputs / 'artifact-identity.json'), '-c', 'platform', timeout=60)
            if self.full:
                contract_dir = inputs / 'contracts'
                contract_dir.mkdir()
                for name in ('fuse_contract', 'manifest.json'):
                    run.kube('-n', run.id, 'cp', ROLES['M'] + ':/opt/adx/e2e/afs-contracts/' + name,
                             str(contract_dir / name), '-c', 'platform', timeout=120)
                from run import common
                manifest = common.afs_contracts.installed(contract_dir, self.package)
                if common.afs_contracts.sha(contract_dir / 'manifest.json') != self.contract_manifest_sha:
                    raise ValueError('runtime test asset manifest differs from bundle')
                (contract_dir / 'fuse_contract').chmod(0o755)
                (contract_dir / 'fuse_contract').rename(inputs / 'fuse_contract')
                identity_path = inputs / 'artifact-identity.json'
                identity = json.loads(identity_path.read_text())
                identity['test_elf'] = manifest['test_elf']
                identity_path.write_text(json.dumps(identity, indent=2))
            path = str(ROOT / 'afs/tests')
            sys.path.insert(0, path)
            try:
                spec = importlib.util.spec_from_file_location('afs_component_runner', ROOT / 'afs/tests/run.py')
                runner = importlib.util.module_from_spec(spec)
                spec.loader.exec_module(runner)
                args = ['run', '--env', str(run.output / 'afs-environment.json'), '--binary-dir', str(inputs),
                        '--work-root', str(run.output / 'afs'), '--case-timeout', '300']
                if self.full:
                    args += ['--suite', 'ci-full']
                else:
                    for name in CASES:
                        args += ['--case', name]
                handlers = {sig: signal.getsignal(sig) for sig in (signal.SIGTERM, signal.SIGINT, signal.SIGALRM)}
                self.attempted_cases = True
                try:
                    code = runner.main(args)
                finally:
                    for sig, handler in handlers.items():
                        signal.signal(sig, handler)
            finally:
                sys.path.remove(path)
            report = json.loads((run.output / 'afs/result.json').read_text())
            self.normal_cleanup = json.loads((run.output / 'afs/cleanup.json').read_text())
            record['subcases'] = [{'id': entry['id'], 'status': 'passed' if entry['status'] == 'PASS' else 'failed',
                                    'seconds': entry.get('seconds', 0), 'error': entry.get('error')}
                                   for entry in report.get('cases', [])]
            if code:
                raise ValueError('AFS component runner failed or was BLOCKED; see afs/result.json')
            validate_result(report, self.normal_cleanup, full=self.full)

    def cleanup(self):
        run = self.run
        errors = []
        if self.attempted_cases and (not self.normal_cleanup or self.normal_cleanup.get('status') != 'PASS'):
            errors.append('AFS normal process/mount cleanup failed or is unconfirmed; Pod deletion is not a substitute')
        if run.namespace_attempted:
            try:
                item = run.kube('get', 'namespace', run.id, '--ignore-not-found', '-o', 'json', timeout=20)
                if item.strip():
                    meta = json.loads(item)['metadata']
                    if meta.get('labels', {}).get(LABEL) != run.id or (
                        run.namespace_uid and meta.get('uid') != run.namespace_uid
                    ):
                        raise RuntimeError('namespace ownership changed; refusing cleanup')
                    try:
                        run.kube('-n', run.id, 'get', 'pods', '-o', 'wide', timeout=20)
                        run.kube('-n', run.id, 'get', 'events', '--sort-by=.lastTimestamp', timeout=20)
                    except Exception as exc:
                        run.event('AFS diagnostic collection failed: ' + type(exc).__name__)
                    run.kube('delete', 'namespace', run.id, '--wait=true', '--timeout=180s', timeout=200)
                    if run.kube('get', 'namespace', run.id, '--ignore-not-found', '-o', 'name').strip():
                        raise RuntimeError('AFS test namespace remains')
            except Exception as exc:
                errors.append(str(exc))
        (run.output / 'kubernetes-cleanup.json').write_text(json.dumps({
            'status': 'FAIL' if errors else 'PASS', 'errors': errors, 'normal_afs_cleanup': self.normal_cleanup,
        }, indent=2))
        return errors
