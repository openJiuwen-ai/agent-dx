"""Non-K8s adapters preserve the existing SDK, component and Full contracts."""

import importlib.util
import json
from pathlib import Path
import tempfile
import unittest
from unittest.mock import Mock

ROOT = Path(__file__).resolve().parents[1]


def load(name, path):
    spec = importlib.util.spec_from_file_location(name, path)
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


class DockerAfsTests(unittest.TestCase):
    def test_component_and_sdk_full_use_existing_required_counts(self):
        module = load('docker_full_counts', ROOT / 'docker_run.py')
        self.assertEqual(module.selected_checks('standalone', 'afs-component-full'), ('afs-component-full',))
        self.assertEqual(module.selected_checks('standalone', 'afs-sdk-full'),
                         ('afs-sdk-ownerfs-full', 'afs-sdk-dfs-full'))
        self.assertEqual(len(module.selected_checks('standalone', None)), 11)
        with self.assertRaises(ValueError):
            module.selected_checks('l0', 'afs-component-full')

    def test_off_candidate_cannot_create_resources(self):
        module = load('docker_off_candidate', ROOT / 'docker_run.py')
        with tempfile.TemporaryDirectory() as temporary:
            run = module.DockerRun(Path(temporary), selected_case='afs-sdk', registry={})
            run.docker = Mock()
            with self.assertRaisesRegex(ValueError, 'AFS ON'):
                run.deploy({'package': {'with_afs': False}}, Path('/fixture'), Path('/secret'))
            run.docker.assert_not_called()

    def test_native_product_reuses_sdk_assertions_and_runtime_capability_contract(self):
        module = load('docker_sdk_reuse', ROOT / 'docker_afs.py')
        run = Mock()
        module.Product(run).configure_runtime('node1')
        self.assertIn('/opt/adx/e2e/afs_runc_spec.json', run.execute.call_args.args[-1])
        self.assertEqual(len(module.product_core.SDK_CASES), 6)
        self.assertEqual(len(module.product_core.SDK_FULL_EXTRA), 2)

    def test_local_docker_full_plan_is_public_component_sdk_without_new_business_cases(self):
        gate = load('docker_gate_plan', ROOT / 'kubernetes/afs_gate.py')
        plan = gate.plans('full', deployment='local-docker')
        self.assertEqual(plan, [('public', 'standalone', None),
                                ('afs-component-full', 'standalone', 'afs-component-full'),
                                ('afs-sdk-full', 'standalone', 'afs-sdk-full')])

    def test_cleanup_refuses_a_replaced_container_instead_of_deleting_it(self):
        module = load('docker_cleanup_owner', ROOT / 'docker_run.py')
        with tempfile.TemporaryDirectory() as temporary:
            run = module.DockerRun(Path(temporary))
            run.owned = {'node1': 'a' * 64}
            run.docker = Mock(return_value=json.dumps([{
                'Id': 'b' * 64, 'Config': {'Labels': {'adx.e2e.run': run.id}},
            }]))
            self.assertTrue(run.cleanup())
            self.assertFalse(any(call.args[0] == 'rm' for call in run.docker.call_args_list))

    def test_container_removal_cannot_replace_normal_afs_cleanup(self):
        module = load('docker_cleanup_normal', ROOT / 'docker_run.py')
        with tempfile.TemporaryDirectory() as temporary:
            run = module.DockerRun(Path(temporary), selected_case='afs-component-full')
            run.afs_component.attempted_cases = True
            run.docker = Mock()
            self.assertTrue(run.cleanup())
            self.assertEqual(json.loads((Path(temporary) / 'docker-cleanup.json').read_text())['status'], 'FAIL')

    def test_native_full_still_requires_all_six_public_stop_receipts(self):
        gate = load('native_stop_contract', ROOT / 'kubernetes/afs_gate.py')
        with tempfile.TemporaryDirectory() as temporary:
            directory = Path(temporary)
            (directory / 'result.json').write_text(json.dumps({
                'deployment': 'local-docker', 'status': 'passed', 'cleanup_errors': [], 'missing_checks': [],
                'checks': ['stop'], 'required_checks': ['stop'], 'cases': [{'name': 'stop', 'status': 'passed'}],
            }))
            (directory / 'docker-cleanup.json').write_text(json.dumps({'status': 'PASS', 'errors': []}))
            (directory / 'bundle.json').write_text(json.dumps({'collector': {'pinned': True}}))
            for node in ('node1', 'node2'):
                for kind in ('collection', 'gateway-metrics', 'traces'):
                    (directory / f'{kind}-{node}.json').write_text(json.dumps({'status': 'passed'}))
            gate.verify_part(directory, ('stop',))
            (directory / 'traces-node2.json').unlink()
            with self.assertRaises(FileNotFoundError):
                gate.verify_part(directory, ('stop',))

