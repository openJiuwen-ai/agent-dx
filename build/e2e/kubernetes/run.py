#!/usr/bin/env python3
"""Buildkite Kubernetes acceptance, sharing business scenarios with local E2E."""
import argparse
import base64
import hashlib
import importlib.util
import json
import ipaddress
import os
from pathlib import Path
import re
import secrets
import shutil
import signal
import subprocess
import sys
import tempfile
import xml.etree.ElementTree as ET

HERE = Path(__file__).resolve().parent
ROOT = HERE.parents[2]
spec = importlib.util.spec_from_file_location('e2e_common', HERE.parent / 'run.py')
common = importlib.util.module_from_spec(spec)
spec.loader.exec_module(common)
sys.path.insert(0, str(HERE))
from manifest import LABEL, resources

def validate_physical_placement(placement, required):
    hosts={item['host'] for item in placement}
    if required and len(hosts) < 2:
        raise RuntimeError('full profile requires two distinct Kubernetes workers')


def source_commit():
    commit = os.getenv('BUILDKITE_COMMIT')
    if commit is None:
        commit = subprocess.check_output(
            ['git', '-C', str(ROOT), 'rev-parse', 'HEAD'], text=True,
        ).strip()
    if not re.fullmatch(r'[0-9a-f]{40}', commit):
        raise ValueError('test harness commit must be a 40-character Git commit')
    return commit


def stage_harness(destination):
    shutil.copytree(
        ROOT / 'build/e2e', destination,
        ignore=shutil.ignore_patterns('__pycache__', 'tests', '*.pyc'),
    )
    shutil.copy2(ROOT / 'build/ci/rpc_certificates.py', destination / 'rpc_certificates.py')
    shutil.copy2(ROOT / 'build/release/package.py', destination / 'package.py')
    files = {}
    for path in sorted(item for item in destination.rglob('*') if item.is_file()):
        files[str(path.relative_to(destination))] = hashlib.sha256(path.read_bytes()).hexdigest()
    return files


def identity(bundle, registry, commit=None, ci=False):
    m = json.loads(bundle.read_text())
    r = json.loads(registry.read_text())
    if m.get('schema_version') != 1 or r.get('schema_version') != 1:
        raise ValueError('invalid Kubernetes artifact manifest')
    if r.get('bundle_sha256') != common.sha(bundle) or r.get('image_ids') != m['image_ids']:
        raise ValueError('registry images do not match the build bundle')
    if set(r.get('references', {})) != {'node', 'execd', 'entrypoint'}:
        raise ValueError('node, EXECD and entrypoint test images required')
    for image in r['references'].values():
        if not re.fullmatch(r'[^\s]+@sha256:[0-9a-f]{64}', image):
            raise ValueError('immutable image references required')
    common.validate_identity(m['package'], m['architecture'], commit, ci)
    return m, r


def credentials(directory, image, runtime_image):
    subprocess.run([sys.executable, str(ROOT / 'build/ci/rpc_certificates.py'), str(directory / 'tls')], check=True)
    tls = directory / 'tls'
    def openssl(*args):
        subprocess.run(['openssl', *map(str, args)], check=True,
                       stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
    openssl('req', '-newkey', 'rsa:2048', '-nodes', '-keyout', tls / 'node2.key',
            '-out', tls / 'node2.csr', '-subj', '/CN=ADX test node2')
    openssl('x509', '-req', '-in', tls / 'node2.csr', '-CA', tls / 'ca.pem',
            '-CAkey', tls / 'ca.key', '-CAcreateserial', '-out', tls / 'node2.pem',
            '-days', '2', '-extfile', tls / 'extensions.cnf')
    openssl('x509', '-in', tls / 'node2.pem', '-outform', 'DER', '-out', tls / 'node2.der')
    for name in ('api-key', 'other-key', 'admin-key', 'redis-key'):
        path = directory / name
        path.write_text(secrets.token_hex(32))
        path.chmod(0o600)
    (directory / 'redis-acl').write_text(
        'user default on >' + (directory / 'redis-key').read_text() + ' ~* &* +@all\n'
    )
    (directory / 'redis-acl').chmod(0o600)
    (directory / 'image').write_text(image)
    (directory / 'runtime-image').write_text(runtime_image)
    data = {p.name: base64.b64encode(p.read_bytes()).decode() for p in tls.iterdir()
            if p.suffix in ('.pem', '.key', '.der') and p.name != 'ca.key'}
    data.update({name: base64.b64encode((directory / name).read_bytes()).decode()
                 for name in ('api-key', 'other-key', 'admin-key', 'redis-key', 'redis-acl',
                              'image', 'runtime-image')})
    return data


def selected_checks(profile, selected_case):
    if selected_case == 'redis-pod-restart':
        if profile != 'full':
            raise ValueError('redis-pod-restart requires the full Kubernetes profile')
        return ('redis-pod-restart',)
    return common.selected_checks(profile, selected_case)


def default_redis_storage_class(value):
    """Choose one cluster-default dynamic class before creating test resources."""
    classes = value.get('items') if isinstance(value, dict) else None
    if not isinstance(classes, list):
        raise ValueError('Kubernetes StorageClass list is invalid')
    defaults = [item['metadata']['name'] for item in classes
                if item.get('provisioner') not in (None, 'kubernetes.io/no-provisioner')
                and item.get('metadata', {}).get('annotations', {}).get(
                    'storageclass.kubernetes.io/is-default-class',
                    item.get('metadata', {}).get('annotations', {}).get(
                        'storageclass.beta.kubernetes.io/is-default-class', 'false')) == 'true']
    if len(defaults) != 1:
        raise ValueError('redis-pod-restart requires one default dynamic StorageClass '
                         'or an explicit --redis-storage-class')
    return defaults[0]


class KubernetesRun(common.Run):
    def __init__(self, output, kubeconfig, context=None, profile="k8s-basic",
                 selected_case=None, redis_storage_class=None):
        super().__init__(output)
        self.kubectl = ['kubectl', '--kubeconfig', str(kubeconfig.resolve())]
        if context:
            self.kubectl += ['--context', context]
        self.namespace_uid = None
        self.namespace_attempted = False
        self.harness = None
        self.profile = profile
        self.selected_case = selected_case
        self.redis_storage_class = redis_storage_class
        self.redis_pod_manifest = None
        self.stop_evidence = 'stop' in selected_checks(profile, selected_case)

    def kube(self, *args, timeout=180):
        return self.command([*self.kubectl, *args], timeout, stream=not any(
            args[i:i+2] == ('-o', 'json') for i in range(len(args)-1)))

    def apply(self, value):
        # Secret bodies stay on stdin; redact any API validation error that echoes values.
        if value['kind'] == 'Secret':
            for encoded in value.get('data', {}).values():
                self.redactions.add(encoded)
                decoded = base64.b64decode(encoded).decode(errors='replace')
                self.redactions.update(line for line in decoded.splitlines() if len(line) >= 8)
                self.redactions.add(decoded)
            self.redactions.discard('')
        self.command([*self.kubectl, 'create', '-f', '-'], timeout=60,
                     input_data=json.dumps(value),
                     label='kubectl create ' + value['kind'] + '/' + value['metadata']['name'])

    def execute(self, node, *args, timeout=180):
        return self.kube('-n', self.id, 'exec', node, '-c', 'platform', '--', *args, timeout=timeout)

    def sdk_instances(self):
        return json.loads(self.execute('node1', 'cat', '/evidence/sdk/sdk-result.json'))['instances']

    def sync_harness(self, commit, product_commit=None):
        with tempfile.TemporaryDirectory(prefix='adx-e2e-harness-') as directory:
            harness = Path(directory) / 'e2e'
            files = stage_harness(harness)
            self.harness = {
                'schema_version': 1,
                'commit': commit,
                'product_commit': product_commit,
                'files': files,
            }
            manifest = json.dumps(self.harness, indent=2) + '\n'
            (harness / 'harness.json').write_text(manifest)
            (self.output / 'harness.json').write_text(manifest)
            self.event('[DEPLOY] Syncing E2E harness commit=' + commit)
            verify = (
                "import hashlib,json,pathlib;"
                "root=pathlib.Path('/opt/adx/e2e');"
                "manifest=json.loads((root/'harness.json').read_text());"
                "bad=[name for name,digest in manifest['files'].items() "
                "if hashlib.sha256((root/name).read_bytes()).hexdigest()!=digest];"
                "assert not bad,bad"
            )
            for node in self.nodes:
                self.kube('-n', self.id, 'cp', str(harness) + '/.',
                          node + ':/opt/adx/e2e', '-c', 'platform', timeout=60)
                self.execute(node, 'python3', '-c', verify, timeout=30)
            self.event('[PASS] Current E2E harness synchronized and verified')

    def deploy(self, m, refs, data, registry_auth=None, node_names=(), require_distinct_workers=False,
               harness_commit=None):
        print('--- Kubernetes deployment', flush=True)
        self.event('[DEPLOY] namespace=' + self.id + '; eligible nodes=' + ','.join(node_names))
        # Check credentials/connectivity before creating any test resource.
        self.kube('version', '-o', 'json', timeout=30)
        if self.selected_case == 'redis-pod-restart' and self.redis_storage_class is None:
            classes = json.loads(self.kube('get', 'storageclasses', '-o', 'json', timeout=30))
            self.redis_storage_class = default_redis_storage_class(classes)
        if self.redis_storage_class is not None:
            (self.output / 'redis-storage-class.json').write_text(
                json.dumps({'name': self.redis_storage_class}, indent=2) + '\n'
            )
            self.event('[DEPLOY] Redis StorageClass=' + self.redis_storage_class)
        self.namespace_attempted = True
        self.apply({'apiVersion': 'v1', 'kind': 'Namespace', 'metadata': {
            'name': self.id, 'labels': {LABEL: self.id, 'pod-security.kubernetes.io/enforce': 'privileged'}}})
        namespace = json.loads(self.kube('get', 'namespace', self.id, '-o', 'json'))
        self.namespace_uid = namespace['metadata']['uid']
        self.apply({'apiVersion': 'v1', 'kind': 'Secret', 'type': 'Opaque',
                    'metadata': {'name': 'adx-test-credentials', 'namespace': self.id}, 'data': data})
        if registry_auth:
            auth = json.loads(registry_auth.read_text())
            if not isinstance(auth.get('auths'), dict):
                raise ValueError('registry auth must be a Docker config with auths')
            self.apply({'apiVersion': 'v1', 'kind': 'Secret', 'type': 'kubernetes.io/dockerconfigjson',
                        'metadata': {'name': 'adx-test-registry', 'namespace': self.id},
                        'data': {'.dockerconfigjson': base64.b64encode(registry_auth.read_bytes()).decode()}})
        objects = resources(self.id, refs['node'], m['architecture'], registry_auth is not None,
                            node_names, self.redis_storage_class)
        # Public manifest contains Secret references, never Secret contents.
        (self.output / 'resources.json').write_text(json.dumps({'apiVersion': 'v1', 'kind': 'List', 'items': objects}, indent=2))
        for obj in objects:
            self.apply(obj)
            if obj['kind'] == 'Pod' and obj['metadata']['name'] in ('node1', 'node2'):
                self.nodes.append(obj['metadata']['name'])
            if obj['kind'] == 'Pod' and obj['metadata']['name'] == 'redis':
                self.redis_pod_manifest = obj
        self.kube('-n', self.id, 'get', 'pods', '-o', 'wide')
        self.event('[DEPLOY] Waiting for Pod readiness and image pulls')
        self.kube('-n', self.id, 'wait', 'pod', '--all', '--for=condition=Ready', '--timeout=300s', timeout=320)
        pods = json.loads(self.kube('-n', self.id, 'get', 'pods', '-o', 'json'))['items']
        placement = [{'pod': p['metadata']['name'], 'host': p['spec']['nodeName'],
                      'ip': p['status']['podIP']} for p in pods]
        validate_physical_placement(
            [item for item in placement if item['pod'] in self.nodes], require_distinct_workers,
        )
        (self.output / 'placement.json').write_text(json.dumps(placement, indent=2) + '\n')
        print('Kubernetes placement: ' + json.dumps(placement), flush=True)
        ingress_pod = next(p for p in pods if p['metadata']['name'] == 'node1')
        ingress_ip = str(ipaddress.ip_address(ingress_pod['status']['podIP']))
        self.sync_harness(harness_commit or source_commit(), m['package']['commit'])
        self.event('[DEPLOY] Checking OCI runtime and bridge netfilter prerequisites')
        for node in self.nodes:
            self.execute(node, 'python3', '-u', '/opt/adx/e2e/preflight.py',
                         '--runtime-source', 'image')
        self.event('[DEPLOY] Configuring nodes and starting sandboxd')
        for node in self.nodes:
            self.execute(node, 'env', 'ADX_E2E_INGRESS_IP=' + ingress_ip,
                         *common.setup_environment(self.selected_case),
                         'python3', '/opt/adx/e2e/node.py', 'setup', node)
            self.execute(node, 'sh', '-c', 'python3 /opt/adx/e2e/node.py services ' + node +
                         ' > /evidence/services-' + node + '.log 2>&1 &')
        self.event('[DEPLOY] Waiting for sandboxd; starting ADX supervisor and services')
        for node in self.nodes:
            self.helper(node, 'backend-ready', timeout=40)
            self.execute(node, 'sh', '-c', '/opt/adx/package/bin/adxctl run --config /tmp/adx-e2e/deployment.yaml'
                         ' > /evidence/supervisor-' + node + '.log 2>&1 &')
        # Pod Ready only means the fixture is available; platform readiness is a separate gate.
        self.event('[DEPLOY] Waiting for Coordinator registration, reconciliation and routes')
        self.helper('node1', 'ready', timeout=150)
        self.event('[PASS] Kubernetes deployment and platform readiness')
        self.kube('-n', self.id, 'get', 'pods', '-o', 'wide')

    def restart_redis_pod(self):
        if self.redis_pod_manifest is None:
            raise ValueError('persistent Redis Pod is not deployed')
        old = json.loads(self.kube('-n', self.id, 'get', 'pod', 'redis', '-o', 'json'))
        claim = json.loads(self.kube('-n', self.id, 'get', 'pvc', 'redis-data', '-o', 'json'))
        pod_uid = old['metadata']['uid']
        claim_uid = claim['metadata']['uid']
        (self.output / 'redis-pod-before.log').write_text(
            self.kube('-n', self.id, 'logs', 'redis', '-c', 'redis', timeout=20)
        )
        self.kube('-n', self.id, 'delete', 'pod', 'redis', '--wait=true',
                  '--timeout=180s', timeout=200)
        self.apply(self.redis_pod_manifest)
        self.kube('-n', self.id, 'wait', 'pod/redis', '--for=condition=Ready',
                  '--timeout=300s', timeout=320)
        new = json.loads(self.kube('-n', self.id, 'get', 'pod', 'redis', '-o', 'json'))
        retained = json.loads(self.kube('-n', self.id, 'get', 'pvc', 'redis-data', '-o', 'json'))
        assert new['metadata']['uid'] != pod_uid, 'Redis Pod was not replaced'
        assert retained['metadata']['uid'] == claim_uid, 'Redis PVC identity changed'
        evidence = {'pod_before': pod_uid, 'pod_after': new['metadata']['uid'],
                    'pvc_uid': claim_uid}
        (self.output / 'redis-pod-identity.json').write_text(json.dumps(evidence, indent=2) + '\n')
        return evidence

    def scenarios(self, checks, required):
        if required != ('redis-pod-restart',):
            return super().scenarios(checks, required)
        with self.case('redis-pod-restart', checks) as record:
            self.event('Create live instances on both workers, replace only the Redis Pod, '
                       'and verify PVC, ownership, backend identities and public SDK access')
            self.execute('node1', '/opt/adx/client/bin/python', '-u',
                         '/opt/adx/e2e/scenarios.py', 'create-marker', timeout=300)
            for node in self.nodes:
                self.helper(node, 'capture-backend', node)
            self.helper('node1', 'redis-pod-before', timeout=20)
            self.restart_redis_pod()
            self.helper('node1', 'ready', timeout=150)
            self.helper('node1', 'redis-pod-after', timeout=45)
            for node in self.nodes:
                self.helper(node, 'unchanged', node)
            output = self.execute('node1', '/opt/adx/client/bin/python', '-u',
                                  '/opt/adx/e2e/scenarios.py', 'recovered-marker', timeout=90)
            record['subcases'] = common.sdk_subcases_from_output(output)
            self.execute('node1', '/opt/adx/client/bin/python', '-u',
                         '/opt/adx/e2e/scenarios.py', 'cleanup-live-redis', timeout=90)
            for node in self.nodes:
                self.helper(node, 'empty', node)

    def cleanup(self):
        print('--- Kubernetes cleanup', flush=True)
        self.event('[CLEANUP] Collect diagnostics, stop services and delete ' + self.id)
        errors = []
        if not self.namespace_attempted:
            return errors
        try:
            text = self.kube('get', 'namespace', self.id, '--ignore-not-found', '-o', 'json', timeout=30)
            if not text.strip():
                return errors
            namespace = json.loads(text)
            meta = namespace['metadata']
            if meta.get('labels', {}).get(LABEL) != self.id or (
                self.namespace_uid and meta['uid'] != self.namespace_uid
            ):
                raise RuntimeError('namespace ownership changed; refusing cleanup')
            # Collect before deleting pods, even when only part of deployment succeeded.
            for args in [('get', 'pods,services', '-o', 'wide'), ('get', 'events', '--sort-by=.lastTimestamp')]:
                try:
                    self.kube('-n', self.id, *args, timeout=20)
                except Exception:
                    pass
            for node in reversed(self.nodes):
                try:
                    # If the scenario already stopped it, skip the unavailable supervisor.
                    action = 'stop' if self.stop_evidence else 'cleanup'
                    self.execute(node, 'sh', '-c', 'test -f /evidence/stop-' + node + '.json || '
                                 'python3 /opt/adx/e2e/node.py ' + action + ' ' + node, timeout=180)
                except Exception as e:
                    errors.append(f'{node} stop: {e}')
                try:
                    # The selected stop case writes final observability evidence.
                    # Always copy after teardown so the summary sees final diagnostics.
                    self.helper(node, 'collect', node, timeout=20)
                    self.kube('-n', self.id, 'cp', node + ':/evidence/.', str(self.output / node), '-c', 'platform', timeout=60)
                except Exception as e:
                    errors.append(f'{node} diagnostics: {e}')
            if self.redis_pod_manifest is not None:
                try:
                    (self.output / 'redis-pod-after.log').write_text(
                        self.kube('-n', self.id, 'logs', 'redis', '-c', 'redis', timeout=20)
                    )
                except Exception as e:
                    errors.append(f'redis diagnostics: {e}')
            self.kube('delete', 'namespace', self.id, '--wait=true', '--timeout=180s', timeout=200)
            if self.kube('get', 'namespace', self.id, '--ignore-not-found', '-o', 'name').strip():
                raise RuntimeError('test namespace remains')
            self.event('[CLEANUP] Namespace deletion verified')
        except Exception as e:
            errors.append(str(e))
        self.event('[FAIL] cleanup: ' + '; '.join(errors) if errors else '[PASS] Kubernetes cleanup')
        return errors


def write_junit(path, report):
    common.write_junit(path, report, 'platform-kubernetes-e2e')


def main():
    p = argparse.ArgumentParser()
    for arg in ('bundle', 'registry-images', 'kubeconfig', 'output'):
        p.add_argument('--' + arg, type=Path, required=True)
    p.add_argument('--context')
    p.add_argument('--node-name', action='append', default=[], help='eligible Kubernetes node; repeat for a pool')
    p.add_argument('--registry-auth', type=Path)
    p.add_argument('--profile', choices=('l0','k8s-basic','full'), default='k8s-basic')
    p.add_argument('--case', help='run one case as a targeted diagnostic, not a Full gate')
    p.add_argument('--redis-storage-class',
                   help='StorageClass for the targeted Redis Pod/PVC recovery case')
    a = p.parse_args()
    required=selected_checks(a.profile,a.case)
    if a.redis_storage_class and a.case != 'redis-pod-restart':
        p.error('--redis-storage-class is only used by --case redis-pod-restart')
    output = a.output.resolve()
    output.mkdir(parents=True, exist_ok=False)
    checks = []
    run = KubernetesRun(output, a.kubeconfig, a.context, a.profile, a.case,
                        a.redis_storage_class if a.case == 'redis-pod-restart' else None)
    error = None
    def cancel(signum, frame):
        raise InterruptedError(f'canceled by signal {signum}')
    for sig in (signal.SIGTERM, signal.SIGINT):
        signal.signal(sig, cancel)
    with tempfile.TemporaryDirectory(prefix='adx-k8s-secrets-') as private:
        try:
            if not a.kubeconfig.is_file():
                raise ValueError('target kubeconfig does not exist')
            expected_commit = os.getenv('ADX_E2E_ARTIFACT_COMMIT', os.getenv('BUILDKITE_COMMIT'))
            m, published = identity(a.bundle, a.registry_images, expected_commit, bool(os.getenv('BUILDKITE')))
            (output / 'bundle.json').write_text(json.dumps(m, indent=2))
            (output / 'registry-images.json').write_text(json.dumps(published, indent=2))
            user_image = m['base_images']['execd']
            if not re.fullmatch(r'[^\s]+@sha256:[0-9a-f]{64}', user_image):
                raise ValueError('immutable custom user image required')
            data = credentials(Path(private), user_image, published['references']['execd'])
            run.deploy(m, published['references'], data, a.registry_auth, a.node_name,
                       require_distinct_workers=a.profile == 'full', harness_commit=source_commit())
            run.scenarios(checks,required)
        except Exception as e:
            error = f'{type(e).__name__}: {e}'
            run.event('[FAIL] Kubernetes acceptance: ' + error)
        finally:
            for sig in (signal.SIGTERM, signal.SIGINT):
                signal.signal(sig, signal.SIG_IGN)
            cleanup_errors = run.cleanup()
    report = common.finish_report(error, cleanup_errors, checks,required)
    report.update(run_id=run.id, deployment='kubernetes', profile='targeted' if a.case else a.profile,
                  source_profile=a.profile, selected_case=a.case,
                  harness=run.harness, cases=run.case_results)
    (output / 'result.json').write_text(json.dumps(report, indent=2) + '\n')
    write_junit(output / 'junit.xml', report)
    print('--- Kubernetes acceptance result', flush=True)
    for case in run.case_results:
        run.event(f"[{'PASS' if case['status'] == 'passed' else 'FAIL'}] {case['name']} ({case['seconds']:.3f}s)")
    for name in report['missing_checks']:
        run.event('[NOT RUN] ' + name)
    run.event(f"[RESULT] {report['status'].upper()}: {len(checks)}/{len(required)} cases passed; cleanup_errors={len(cleanup_errors)}")
    print(json.dumps(report), flush=True)
    return 0 if report['status'] == 'passed' else 1

if __name__ == '__main__':
    raise SystemExit(main())
