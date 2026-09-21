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
    if set(r.get('references', {})) != {'node', 'rrt', 'entrypoint'}:
        raise ValueError('node, RRT and entrypoint test images required')
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
    (directory / 'image').write_text(image)
    (directory / 'runtime-image').write_text(runtime_image)
    data = {p.name: base64.b64encode(p.read_bytes()).decode() for p in tls.iterdir()
            if p.suffix in ('.pem', '.key', '.der') and p.name != 'ca.key'}
    data.update({name: base64.b64encode((directory / name).read_bytes()).decode()
                 for name in ('api-key', 'other-key', 'admin-key', 'redis-key', 'image', 'runtime-image')})
    return data


class KubernetesRun(common.Run):
    def __init__(self, output, kubeconfig, context=None):
        super().__init__(output)
        self.kubectl = ['kubectl', '--kubeconfig', str(kubeconfig.resolve())]
        if context:
            self.kubectl += ['--context', context]
        self.namespace_uid = None
        self.namespace_attempted = False
        self.harness = None

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
        objects = resources(self.id, refs['node'], m['architecture'], registry_auth is not None, node_names)
        # Public manifest contains Secret references, never Secret contents.
        (self.output / 'resources.json').write_text(json.dumps({'apiVersion': 'v1', 'kind': 'List', 'items': objects}, indent=2))
        for obj in objects:
            self.apply(obj)
            if obj['kind'] == 'Pod':
                self.nodes.append(obj['metadata']['name'])
        self.kube('-n', self.id, 'get', 'pods', '-o', 'wide')
        self.event('[DEPLOY] Waiting for Pod readiness and image pulls')
        self.kube('-n', self.id, 'wait', 'pod', '--all', '--for=condition=Ready', '--timeout=300s', timeout=320)
        pods = json.loads(self.kube('-n', self.id, 'get', 'pods', '-o', 'json'))['items']
        placement = [{'pod': p['metadata']['name'], 'host': p['spec']['nodeName'],
                      'ip': p['status']['podIP']} for p in pods]
        validate_physical_placement(placement, require_distinct_workers)
        (self.output / 'placement.json').write_text(json.dumps(placement, indent=2) + '\n')
        print('Kubernetes placement: ' + json.dumps(placement), flush=True)
        edge_pod = next(p for p in pods if p['metadata']['name'] == 'node1')
        edge_ip = str(ipaddress.ip_address(edge_pod['status']['podIP']))
        self.sync_harness(harness_commit or source_commit(), m['package']['commit'])
        self.event('[DEPLOY] Checking OCI runtime and bridge netfilter prerequisites')
        for node in self.nodes:
            self.execute(node, 'python3', '-u', '/opt/adx/e2e/preflight.py',
                         '--runtime-source', 'image')
        self.event('[DEPLOY] Configuring nodes and starting sandboxd')
        for node in self.nodes:
            self.execute(node, 'env', 'ADX_E2E_EDGE_IP=' + edge_ip,
                         'python3', '/opt/adx/e2e/node.py', 'setup', node)
            self.execute(node, 'sh', '-c', 'python3 /opt/adx/e2e/node.py services ' + node +
                         ' > /evidence/services-' + node + '.log 2>&1 &')
        self.event('[DEPLOY] Waiting for sandboxd; starting ADX supervisor and services')
        for node in self.nodes:
            self.helper(node, 'backend-ready', timeout=40)
            self.execute(node, 'sh', '-c', '/opt/adx/package/bin/adxctl run --config /tmp/adx-e2e/deployment.yaml'
                         ' > /evidence/supervisor-' + node + '.log 2>&1 &')
        # Pod Ready only means the fixture is available; platform readiness is a separate gate.
        self.event('[DEPLOY] Waiting for Master registration, reconciliation and routes')
        self.helper('node1', 'ready', timeout=150)
        self.event('[PASS] Kubernetes deployment and platform readiness')
        self.kube('-n', self.id, 'get', 'pods', '-o', 'wide')

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
                    self.execute(node, 'sh', '-c', 'test -f /evidence/stop-' + node + '.json || '
                                 'python3 /opt/adx/e2e/node.py stop ' + node, timeout=180)
                except Exception as e:
                    errors.append(f'{node} stop: {e}')
                try:
                    # Stop writes the final metrics, trace, collection and logging results.
                    # Copy after it completes so the build summary validates final evidence.
                    self.helper(node, 'collect', node, timeout=20)
                    self.kube('-n', self.id, 'cp', node + ':/evidence/.', str(self.output / node), '-c', 'platform', timeout=60)
                except Exception as e:
                    errors.append(f'{node} diagnostics: {e}')
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
    a = p.parse_args()
    output = a.output.resolve()
    output.mkdir(parents=True, exist_ok=False)
    run = KubernetesRun(output, a.kubeconfig, a.context)
    checks = [];required=common.required_for_profile(a.profile)
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
            user_image = m['base_images']['rrt']
            if not re.fullmatch(r'[^\s]+@sha256:[0-9a-f]{64}', user_image):
                raise ValueError('immutable custom user image required')
            data = credentials(Path(private), user_image, published['references']['rrt'])
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
    report.update(run_id=run.id, deployment='kubernetes', profile=a.profile,
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
