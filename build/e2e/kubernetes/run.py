#!/usr/bin/env python3
"""Buildkite Kubernetes acceptance, sharing business scenarios with local E2E."""
import argparse
import base64
import importlib.util
import json
import ipaddress
import os
from pathlib import Path
import re
import secrets
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


def identity(bundle, registry, commit=None, ci=False):
    m = json.loads(bundle.read_text())
    r = json.loads(registry.read_text())
    if m.get('schema_version') != 1 or r.get('schema_version') != 1:
        raise ValueError('invalid Kubernetes artifact manifest')
    if r.get('bundle_sha256') != common.sha(bundle) or r.get('image_ids') != m['image_ids']:
        raise ValueError('registry images do not match the build bundle')
    if set(r.get('references', {})) != {'node', 'rrt'}:
        raise ValueError('both node and RRT images required')
    for image in r['references'].values():
        if not re.fullmatch(r'[^\s]+@sha256:[0-9a-f]{64}', image):
            raise ValueError('immutable image references required')
    common.validate_identity(m['package'], m['architecture'], commit, ci)
    return m, r


def credentials(directory, image):
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
    for name in ('api-key', 'other-key', 'redis-key'):
        path = directory / name
        path.write_text(secrets.token_hex(32))
        path.chmod(0o600)
    (directory / 'image').write_text(image)
    data = {p.name: base64.b64encode(p.read_bytes()).decode() for p in tls.iterdir()
            if p.suffix in ('.pem', '.key', '.der') and p.name != 'ca.key'}
    data.update({name: base64.b64encode((directory / name).read_bytes()).decode()
                 for name in ('api-key', 'other-key', 'redis-key', 'image')})
    return data


class KubernetesRun(common.Run):
    def __init__(self, output, kubeconfig, context=None):
        super().__init__(output)
        self.kubectl = ['kubectl', '--kubeconfig', str(kubeconfig.resolve())]
        if context:
            self.kubectl += ['--context', context]
        self.namespace_uid = None
        self.namespace_attempted = False

    def kube(self, *args, timeout=180):
        return self.command([*self.kubectl, *args], timeout)

    def apply(self, value):
        # Secret payload travels only on stdin. Do not write it to command logs.
        self.commands += 1
        with (self.output / f'{self.commands:03d}.log').open('w') as log:
            p = subprocess.run([*self.kubectl, 'create', '-f', '-'],
                               input=json.dumps(value).encode(), stdout=log,
                               stderr=subprocess.STDOUT, timeout=60)
        if p.returncode:
            raise RuntimeError(f'Kubernetes create failed; see {self.commands:03d}.log')

    def execute(self, node, *args, timeout=180):
        return self.kube('-n', self.id, 'exec', node, '-c', 'platform', '--', *args, timeout=timeout)

    def deploy(self, m, refs, data, registry_auth=None, node_names=()):
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
        self.kube('-n', self.id, 'wait', 'pod', '--all', '--for=condition=Ready', '--timeout=300s', timeout=320)
        edge_pod = json.loads(self.kube('-n', self.id, 'get', 'pod', 'node1', '-o', 'json'))
        edge_ip = str(ipaddress.ip_address(edge_pod['status']['podIP']))
        for node in self.nodes:
            self.execute(node, 'python3', '/opt/adx/e2e/preflight.py')
        for node in self.nodes:
            self.execute(node, 'env', 'ADX_E2E_EDGE_IP=' + edge_ip,
                         'python3', '/opt/adx/e2e/node.py', 'setup', node)
            self.execute(node, 'sh', '-c', 'python3 /opt/adx/e2e/node.py services ' + node +
                         ' > /evidence/services-' + node + '.log 2>&1 &')
        for node in self.nodes:
            self.helper(node, 'backend-ready', timeout=40)
            self.execute(node, 'sh', '-c', '/opt/adx/package/bin/adxctl run --config /tmp/adx-e2e/deployment.json'
                         ' > /evidence/supervisor-' + node + '.log 2>&1 &')
        # Pod Ready only means the fixture is available; platform readiness is a separate gate.
        self.helper('node1', 'ready', timeout=150)
        self.kube('-n', self.id, 'get', 'pods', '-o', 'wide')

    def cleanup(self):
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
                    self.helper(node, 'collect', node, timeout=20)
                    self.kube('-n', self.id, 'cp', node + ':/evidence/.', str(self.output / node), '-c', 'platform', timeout=60)
                except Exception as e:
                    errors.append(f'{node} diagnostics: {e}')
                try:
                    # If the scenario already stopped it, skip the unavailable supervisor.
                    self.execute(node, 'sh', '-c', 'test -f /evidence/stop-' + node + '.json || '
                                 'python3 /opt/adx/e2e/node.py stop ' + node, timeout=180)
                except Exception as e:
                    errors.append(f'{node} stop: {e}')
            self.kube('delete', 'namespace', self.id, '--wait=true', '--timeout=180s', timeout=200)
            if self.kube('get', 'namespace', self.id, '--ignore-not-found', '-o', 'name').strip():
                raise RuntimeError('test namespace remains')
        except Exception as e:
            errors.append(str(e))
        return errors


def main():
    p = argparse.ArgumentParser()
    for arg in ('bundle', 'registry-images', 'kubeconfig', 'output'):
        p.add_argument('--' + arg, type=Path, required=True)
    p.add_argument('--context')
    p.add_argument('--node-name', action='append', default=[], help='eligible Kubernetes node; repeat for a pool')
    p.add_argument('--registry-auth', type=Path)
    a = p.parse_args()
    output = a.output.resolve()
    output.mkdir(parents=True, exist_ok=False)
    run = KubernetesRun(output, a.kubeconfig, a.context)
    checks = []
    error = None
    def cancel(signum, frame):
        raise InterruptedError(f'canceled by signal {signum}')
    for sig in (signal.SIGTERM, signal.SIGINT):
        signal.signal(sig, cancel)
    with tempfile.TemporaryDirectory(prefix='adx-k8s-secrets-') as private:
        try:
            if not a.kubeconfig.is_file():
                raise ValueError('target kubeconfig does not exist')
            m, published = identity(a.bundle, a.registry_images, os.getenv('BUILDKITE_COMMIT'), bool(os.getenv('BUILDKITE')))
            (output / 'bundle.json').write_text(json.dumps(m, indent=2))
            (output / 'registry-images.json').write_text(json.dumps(published, indent=2))
            data = credentials(Path(private), published['references']['rrt'])
            run.deploy(m, published['references'], data, a.registry_auth, a.node_name)
            run.scenarios(checks)
        except Exception as e:
            error = f'{type(e).__name__}: {e}'
        finally:
            for sig in (signal.SIGTERM, signal.SIGINT):
                signal.signal(sig, signal.SIG_IGN)
            cleanup_errors = run.cleanup()
    report = common.finish_report(error, cleanup_errors, checks)
    report.update(run_id=run.id, deployment='kubernetes')
    (output / 'result.json').write_text(json.dumps(report, indent=2) + '\n')
    suite = ET.Element('testsuite', name='platform-kubernetes-e2e', tests='1', failures=str(int(report['status'] != 'passed')))
    case = ET.SubElement(suite, 'testcase', name='public-sdk-two-node-pods')
    if report['status'] != 'passed':
        ET.SubElement(case, 'failure').text = json.dumps(report)
    ET.ElementTree(suite).write(output / 'junit.xml', encoding='utf-8', xml_declaration=True)
    print(json.dumps(report))
    return 0 if report['status'] == 'passed' else 1

if __name__ == '__main__':
    raise SystemExit(main())
