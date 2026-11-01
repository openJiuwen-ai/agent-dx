"""CI Full selects a declared functional set without widening Base smoke."""

import importlib.util
from pathlib import Path
import sys
import unittest
from unittest.mock import Mock

ROOT = Path(__file__).resolve().parents[3]
sys.path.append(str(ROOT / 'afs/tests'))
from fixtures.common import discover, select
from fixtures.common import Blocked
from fixtures.multivm import validate


def load(name, path):
    spec = importlib.util.spec_from_file_location(name, ROOT / path)
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


class FullTests(unittest.TestCase):
    def test_full_registry_is_complete_functional_and_includes_smoke(self):
        component = load('full_selection_component', 'build/e2e/kubernetes/afs_component.py')
        cases = select(discover(), suite='ci-full')
        self.assertEqual(len(cases), 24)
        self.assertEqual({c.id for c in cases}, set(component.FULL_CASES))
        self.assertTrue(set(component.CASES) <= set(component.FULL_CASES))
        self.assertTrue(all(c.purpose == 'functional' for c in cases))
        self.assertEqual(sum(c.rust for c in cases), 3)

    def test_full_needs_c_but_only_primary_pair_requires_distinct_workers(self):
        component = load('full_resources_component', 'build/e2e/kubernetes/afs_component.py')
        pods = component.resources('adx-e2e-full', 'reg/node@sha256:' + 'a' * 64,
                                   'amd64', distinct_workers=True, full=True)
        self.assertEqual([p['metadata']['name'] for p in pods], ['afs-m', 'afs-a', 'afs-b', 'afs-c'])
        for pod in pods:
            terms = pod['spec']['affinity'].get('podAntiAffinity', {}).get(
                'requiredDuringSchedulingIgnoredDuringExecution', [])
            if pod['metadata']['name'] in ('afs-a', 'afs-b'):
                self.assertEqual(terms[0]['labelSelector']['matchLabels']['afs.e2e.primary-data'], 'true')
            else:
                self.assertFalse(terms)

    def test_standalone_full_rejects_a_missing_extended_case(self):
        cases = discover()
        for name in ('ownerfs.extended.namespace_identity', 'dfs.visibility.real_cross_handle_cache'):
            with self.subTest(case=name), self.assertRaisesRegex(ValueError, 'incomplete ci-full'):
                select([c for c in cases if c.id != name], suite='ci-full')

    def test_only_full_local_pod_has_writable_owned_fuse_bdi_access(self):
        component = load('full_bdi_resources', 'build/e2e/kubernetes/afs_component.py')
        for full in (False, True):
            pods = component.resources('adx-e2e-bdi', 'reg/node@sha256:' + 'a' * 64, 'amd64', full=full)
            for pod in pods:
                volumes = [v for v in pod['spec']['volumes'] if v['name'] == 'bdi']
                mounts = [m for m in pod['spec']['containers'][0]['volumeMounts'] if m['name'] == 'bdi']
                if full and pod['metadata']['name'] == 'afs-m':
                    self.assertEqual(volumes, [{'name': 'bdi', 'hostPath': {
                        'path': '/sys/devices/virtual/bdi', 'type': 'Directory'}}])
                    self.assertEqual(mounts, [{'name': 'bdi', 'mountPath': '/sys/devices/virtual/bdi',
                                               'readOnly': False}])
                else:
                    self.assertFalse(volumes)
                    self.assertFalse(mounts)

    def test_kubernetes_local_records_one_pod_without_claiming_a_vm(self):
        config = {'kind': 'kubernetes', 'topology': 'local', 'uid': 10000, 'gid': 10000,
                  'other_uid': 10001, 'other_gid': 10001,
                  'roles': {r: {'host': 'local', 'address': '127.0.0.' + str(i)}
                            for i, r in enumerate('MABC', 2)},
                  'hosts': {'local': {'transport': 'kubernetes', 'base': '/a'}}}
        cases = [c for c in discover() if c.id == 'fuse.contracts.ordering']
        self.assertEqual(validate(config, cases), ['local'])
        config['topology'] = 'cluster'
        with self.assertRaises(Blocked):
            validate(config, cases)

    def test_missing_elf_blocks_full_before_kubernetes_mutation(self):
        component = load('full_admission_component', 'build/e2e/kubernetes/afs_component.py')
        run = Mock()
        with self.assertRaisesRegex(ValueError, 'Rust contract assets'):
            component.Component(run, full=True).deploy({'package': {'with_afs': True}}, {}, None, (), True, 'a' * 40)
        run.kube.assert_not_called()
        run.apply.assert_not_called()

    def test_sdk_full_requires_extra_assertions_and_explicit_scope(self):
        product = load('full_sdk_result', 'build/e2e/kubernetes/afs_product.py')
        records = [{'id': c, 'status': 'passed'} for c in product.SDK_CASES]
        report = {'status': 'passed', 'mode': 'dfs', 'cases': records, 'cleanup_errors': []}
        product.validate_sdk_result(report, 'dfs')
        with self.assertRaises(ValueError):
            product.validate_sdk_result(report, 'dfs', full=True)
        report.update(full=True, cases=records + [{'id': c, 'status': 'passed'} for c in product.SDK_FULL_EXTRA])
        product.validate_sdk_result(report, 'dfs', full=True)

    def test_full_result_rejects_missing_duplicate_or_blocked_case(self):
        component = load('full_result_component', 'build/e2e/kubernetes/afs_component.py')
        records = [{'id': c, 'status': 'PASS'} for c in component.FULL_CASES]
        report = {'status': 'PASS', 'selected_cases': list(component.FULL_CASES),
                  'cases': records, 'cleanup_errors': [], 'suite': 'ci-full', 'partial': False}
        component.validate_result(report, {'status': 'PASS'}, full=True)
        for bad in (records[:-1], records + [records[0]], records[:-1] + [dict(records[-1], status='BLOCKED')]):
            with self.assertRaises(ValueError):
                component.validate_result(dict(report, cases=bad), {'status': 'PASS'}, full=True)

    def test_full_gate_expands_only_full_profile(self):
        gate = load('full_plan_gate', 'build/e2e/kubernetes/afs_gate.py')
        self.assertEqual([p[0] for p in gate.plans('l0')], ['public', 'afs-component', 'afs-sdk'])
        self.assertEqual([p[0] for p in gate.plans('full')], ['public', 'afs-component-full', 'afs-sdk-full'])
        runner = load('full_target_runner', 'build/e2e/kubernetes/run.py')
        self.assertEqual(runner.selected_checks('full', 'afs-component-full'), ('afs-component-full',))
        with self.assertRaises(ValueError):
            runner.selected_checks('l0', 'afs-component-full')


if __name__ == '__main__':
    unittest.main()
