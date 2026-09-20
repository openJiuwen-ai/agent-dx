import importlib.util
from pathlib import Path
import unittest

ROOT = Path(__file__).resolve().parents[1]
spec = importlib.util.spec_from_file_location('multivm_contract', ROOT / 'multivm/contract.py')
contract = importlib.util.module_from_spec(spec)
spec.loader.exec_module(contract)


def inventory():
    value = {
        'schema_version': 1,
        'machines': [
            {'role': 'control', 'machine_id': 'm-control', 'hostname': 'control', 'address': '10.0.0.10'},
            {'role': 'worker-1', 'machine_id': 'm-worker-1', 'hostname': 'worker-1', 'address': '10.0.0.11'},
            {'role': 'worker-2', 'machine_id': 'm-worker-2', 'hostname': 'worker-2', 'address': '10.0.0.12'},
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
