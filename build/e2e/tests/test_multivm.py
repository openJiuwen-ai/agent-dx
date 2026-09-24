import importlib.util
from pathlib import Path
import hashlib
import io
import json
import tarfile
import tempfile
from types import SimpleNamespace
import unittest

ROOT = Path(__file__).resolve().parents[1]
spec = importlib.util.spec_from_file_location('multivm_contract', ROOT / 'multivm/contract.py')
contract = importlib.util.module_from_spec(spec)
spec.loader.exec_module(contract)
from build.e2e.multivm.sdk_accept import inspect_release, run_acceptance


def inventory():
    value = {
        'schema_version': 1,
        'machines': [
            {'role': 'control', 'machine_id': 'm-control', 'hostname': 'control', 'address': '10.0.0.10', 'ssh_target': 'control'},
            {'role': 'worker-1', 'machine_id': 'm-worker-1', 'hostname': 'worker-1', 'address': '10.0.0.11', 'ssh_target': 'worker-1', 'node_id': 'node1'},
            {'role': 'worker-2', 'machine_id': 'm-worker-2', 'hostname': 'worker-2', 'address': '10.0.0.12', 'ssh_target': 'worker-2', 'node_id': 'node2'},
        ],
        'artifacts': {'commit': 'a' * 40, 'release_sha256': 'b' * 64,
                      'target': 'x86_64-unknown-linux-gnu'},
    }
    return value


def result():
    return {
        'status': 'passed', 'profile': 'multi-vm', 'deployment': 'process',
        'required_checks': list(contract.REQUIRED), 'checks': list(contract.REQUIRED),
        'missing_checks': [], 'cleanup_errors': [], 'error': None,
        'inventory_sha256': contract.inventory_digest(inventory()),
        'placement': [
            {'instance_id': 'one', 'machine_role': 'worker-1'},
            {'instance_id': 'two', 'machine_role': 'worker-2'},
        ],
        'final_state': {'backend_instances': {'worker-1': 0, 'worker-2': 0},
                        'published_routes': 0},
    }


class MultiVmContractTests(unittest.TestCase):
    def test_complete_three_vm_result_passes(self):
        self.assertEqual(contract.verify_result(result(), inventory())['status'], 'passed')

    def test_duplicate_machine_identity_is_rejected(self):
        value = inventory()
        value['machines'][2]['machine_id'] = value['machines'][1]['machine_id']
        with self.assertRaisesRegex(ValueError, 'machine_id'):
            contract.verify_inventory(value)

    def test_missing_second_worker_placement_is_rejected(self):
        value = result()
        value['placement'] = value['placement'][:1]
        with self.assertRaisesRegex(ValueError, 'both worker'):
            contract.verify_result(value, inventory())

    def test_residual_backend_is_rejected(self):
        value = result()
        value['final_state']['backend_instances']['worker-2'] = 1
        with self.assertRaisesRegex(ValueError, 'backend inventories'):
            contract.verify_result(value, inventory())

    def test_missing_case_or_cleanup_error_is_rejected(self):
        value = result()
        value['checks'].pop()
        value['missing_checks'] = ['stop']
        value['cleanup_errors'] = ['worker-2 remains']
        with self.assertRaisesRegex(ValueError, 'all multi-VM checks'):
            contract.verify_result(value, inventory())


class MultiVmSdkAcceptanceTests(unittest.TestCase):
    def test_release_archive_hash_and_manifest_are_bound(self):
        with tempfile.TemporaryDirectory() as directory:
            archive = Path(directory) / 'adx-release.tar.gz'
            payload = json.dumps({'commit': 'a' * 40, 'target': 'x86_64-unknown-linux-gnu'}).encode()
            with tarfile.open(archive, 'w:gz') as bundle:
                entry = tarfile.TarInfo('./manifest.json')
                entry.size = len(payload)
                bundle.addfile(entry, io.BytesIO(payload))
            digest = hashlib.sha256(archive.read_bytes()).hexdigest()
            self.assertEqual(inspect_release(archive, digest), (digest, json.loads(payload)))
            with self.assertRaisesRegex(AssertionError, 'SHA256'):
                inspect_release(archive, '0' * 64)

    def test_live_backends_on_both_vms_are_required_and_cleaned(self):
        state = {'actual': {}, 'deleted': []}

        class FakeSandbox:
            def __init__(self, *, node_id, **_options):
                self.id = 'sandbox-' + node_id
                state['actual'][self.id] = node_id
                self.commands = SimpleNamespace(run=lambda _command: SimpleNamespace(
                    stdout='adx-three-vm', stderr='stderr', exit_code=7))
                self.files = SimpleNamespace(write=lambda _path, payload: setattr(self, 'payload', payload),
                                             read=lambda _path, **_kwargs: self.payload)

            def is_running(self):
                return True

            def kill(self):
                state['deleted'].append(self.id)
                state['actual'].pop(self.id)

            def close(self):
                pass

        def remote(machine, *command):
            if command == ('cat', '/etc/machine-id'):
                return machine['machine_id'] + '\n'
            if command == ('hostname',):
                return machine['hostname'] + '\n'
            if command == ('ip', '-j', 'address', 'show'):
                return json.dumps([{'addr_info': [{'local': machine['address']}]}])
            if command == ('cat', '/opt/adx/current/manifest.json'):
                return json.dumps({'commit': inventory()['artifacts']['commit'],
                                   'target': inventory()['artifacts']['target']})
            if command[:5] == ('/opt/adx/current/bin/adx-inspect', '-c',
                                '/opt/adx/config/deployment.yaml', 'environment', 'get'):
                instance_id = command[5]
                return json.dumps({'id': instance_id, 'node_id': instance_id.removeprefix('sandbox-'),
                                   'state': 'Running', 'resources_held': True})
            if command[:4] == ('sbox', '-a', '/run/sandboxd/sandboxd.sock', 'list'):
                instance_id = command[5].split('=', 1)[1]
                actual = state['actual'].get(instance_id)
                return 'ID STATE\n' + (instance_id + '-backend running\n' if actual == machine.get('node_id') else '')
            raise AssertionError(command)

        def resources(**_kwargs):
            return [SimpleNamespace(id=node_id, status=0) for node_id in ('node1', 'node2')]

        with tempfile.TemporaryDirectory() as directory:
            report = run_acceptance(inventory(), object(), 'image', '/run/sandboxd/sandboxd.sock',
                                    Path(directory), FakeSandbox, resources, remote, placement_timeout=0)
            self.assertEqual(report['status'], 'passed')
            self.assertEqual(len(report['backends']), 2)
            self.assertEqual(len(report['assignments']), 2)
            self.assertEqual(set(state['deleted']), {'sandbox-node1', 'sandbox-node2'})
            self.assertFalse(state['actual'])

        state['actual'].clear()

        class MisplacedSandbox(FakeSandbox):
            def __init__(self, *, node_id, **options):
                super().__init__(node_id=node_id, **options)
                state['actual'][self.id] = 'node2' if node_id == 'node1' else 'node1'

        with tempfile.TemporaryDirectory() as directory:
            with self.assertRaisesRegex(AssertionError, 'physical backend placement mismatch'):
                run_acceptance(inventory(), object(), 'image', '/run/sandboxd/sandboxd.sock',
                               Path(directory), MisplacedSandbox, resources, remote, placement_timeout=0)
            self.assertEqual(json.loads((Path(directory) / 'sdk-accept-result.json').read_text())['status'], 'failed')
            self.assertFalse(state['actual'])
