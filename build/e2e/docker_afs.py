"""Docker topology adapters; business assertions remain the existing AFS cases."""

import importlib.util
import json
from pathlib import Path
import sys

ROOT = Path(__file__).resolve().parents[2]
KUBE = ROOT / 'build/e2e/kubernetes'
if str(KUBE) not in sys.path:
    sys.path.insert(0, str(KUBE))


def load(name, path):
    spec = importlib.util.spec_from_file_location(name, path)
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


component_core = load('docker_component_assertions', KUBE / 'afs_component.py')
product_core = load('docker_product_assertions', KUBE / 'afs_product.py')
specials = load('docker_special_assertions', ROOT / 'build/e2e/afs_specials.py')


def topology(run, roles, sdk=False):
    hosts, addresses = {}, {}
    for role, node in roles.items():
        item = json.loads(run.docker('inspect', run.id + '-' + node, stream=False))[0]
        network = item['NetworkSettings']['Networks'][run.id]
        host = role.lower()
        hosts[host] = {'transport': 'docker', 'base': '/a', 'docker': run.docker_argv,
                       'run_id': run.id, 'container': run.id + '-' + node, 'container_id': item['Id'],
                       'network_id': network['NetworkID'], 'address': network['IPAddress'],
                       'mount_namespace': 'container' if sdk and role in ('A', 'B') else 'private'}
        addresses[role] = {'host': host, 'address': network['IPAddress']}
    result = {'kind': 'docker', 'uid': 10000, 'gid': 10000, 'other_uid': 10001, 'other_gid': 10001,
              'hosts': hosts, 'roles': addresses}
    return result


class Component(component_core.Component):
    def deploy(self, bundle, _refs=None, _auth=None, _nodes=(), _distinct=False, harness_commit=None):
        self.package = bundle['package']
        if not self.package.get('with_afs'):
            raise ValueError('AFS component acceptance requires an AFS ON candidate')
        if self.full:
            self.contract_manifest_sha = bundle['afs_contracts']['sha256']
        for node in self.roles.values():
            self.run.role_container(node, bundle, bdi=self.full and node == 'afs-m')
        self.run.sync_harness(harness_commit, self.package)
        config = topology(self.run, self.roles)
        if self.full:
            local = dict(config, topology='local', hosts={'local': config['hosts']['m']},
                         roles={r: {'host': 'local', 'address': '127.0.0.' + str(i)}
                                for i, r in enumerate('MABC', 2)})
            config = {'kind': 'matrix', 'environments': {'local': local, 'cluster': config}}
        self.config = config
        (self.run.output / 'afs-environment.json').write_text(json.dumps(config, indent=2))
        self.run.execute('afs-m', 'python3', '-B', '-m', 'unittest', 'discover',
                         '-s', '/opt/adx/e2e/afs-tests/runner_tests', '-v', timeout=180)

    def copy_from(self, node, source, destination, timeout=120):
        self.run.copy_from(node, source, destination, timeout)

    def cleanup(self):
        if self.attempted_cases and (not self.normal_cleanup or self.normal_cleanup.get('status') != 'PASS'):
            return ['AFS normal process/mount cleanup failed or is unconfirmed; container removal is not a substitute']
        return []


class Product(product_core.Product):
    special = None

    def scenarios(self, checks):
        if self.special:
            return specials.scenarios(self, checks, self.special)
        return super().scenarios(checks)

    def configure(self, package, _inventory=None, harness_commit=None):
        if not package.get('with_afs'):
            raise ValueError('SDK-on-AFS requires an AFS ON candidate')
        self.run.role_container('afs-m', self.run.bundle_manifest)
        self.run.sync_harness(harness_commit, package, nodes=['afs-m'])
        self.config = topology(self.run, {'M': 'afs-m', 'A': 'node1', 'B': 'node2'}, sdk=True)
        (self.run.output / 'afs-sdk-environment.json').write_text(json.dumps(self.config, indent=2))
        self.run.execute('afs-m', 'python3', '-B', '-m', 'unittest', 'discover',
                         '-s', '/opt/adx/e2e/afs-tests/runner_tests', '-v', timeout=180)

    def copy_from(self, node, source, destination, timeout=120):
        self.run.copy_from(node, source, destination, timeout)
