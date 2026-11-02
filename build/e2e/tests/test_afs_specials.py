"""Special admission must reject missing work, weakened scope and cleanup gaps."""

import importlib.util
import json
from pathlib import Path
import tempfile
import unittest
from unittest.mock import Mock, patch
from types import ModuleType
import sys

HERE = Path(__file__).resolve().parents[1]


def load(name, path):
    spec = importlib.util.spec_from_file_location(name, path)
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


class SpecialContracts(unittest.TestCase):
    def test_native_specials_are_explicit_and_do_not_change_full_or_smoke(self):
        driver = load('special_driver', HERE / 'docker_run.py')
        self.assertEqual(driver.selected_checks('standalone', 'afs-component-specials'),
                         ('afs-component-specials',))
        for special in ('restart', 'pressure'):
            self.assertEqual(driver.selected_checks('standalone', 'afs-sdk-' + special),
                             tuple('afs-sdk-' + mode + '-' + special for mode in ('ownerfs', 'dfs')))
            with self.assertRaises(ValueError):
                driver.selected_checks('l0', 'afs-sdk-' + special)
        self.assertEqual(len(driver.selected_checks('standalone')), 11)
        self.assertEqual(driver.selected_checks('standalone', 'afs-sdk-full'),
                         ('afs-sdk-ownerfs-full', 'afs-sdk-dfs-full'))

    def test_component_specials_reuse_registered_recovery_and_bounded_write_cases(self):
        scope = load('special_scope', HERE / 'afs_specials.py')
        self.assertEqual(scope.COMPONENT_CASES, (
            'ownerfs.cluster.restart', 'dfs.cluster.restart',
            'dfs.write.cluster.bounded_sync_progress', 'dfs.write.cluster.required_replica_recovery',
            'dfs.write.cluster.failed_close_retirement',
        ))
        component = load('special_component', HERE / 'kubernetes/afs_component.py')
        report = {'status': 'PASS', 'cleanup_errors': [], 'selected_cases': list(scope.COMPONENT_CASES),
                  'cases': [{'id': name, 'status': 'PASS'} for name in scope.COMPONENT_CASES]}
        component.validate_result(report, {'status': 'PASS', 'errors': []}, case_ids=scope.COMPONENT_CASES)
        report['cases'].pop()
        with self.assertRaises(ValueError):
            component.validate_result(report, {'status': 'PASS', 'errors': []}, case_ids=scope.COMPONENT_CASES)

    def test_pressure_success_requires_all_operations_and_real_expected_errno(self):
        scope = load('special_pressure_contract', HERE / 'afs_specials.py')
        report = {'status': 'passed', 'mode': 'dfs', 'special': 'pressure', 'cleanup_errors': [],
                  'peer_reads': {'before': 1.0, 'after': 2.0},
                  'cases': [{'id': n, 'status': 'passed'} for n in scope.SDK_CHECKS['pressure']],
                  'pressure': {'workers': 4, 'operations': 32, 'payload_bytes': 65536,
                               'verified_bytes': 2097152, 'errors': [], 'exclusive_errno': 17}}
        scope.validate_sdk(report, 'dfs', 'pressure')
        for change in ({'operations': 31}, {'exclusive_errno': 13}, {'verified_bytes': 0}, {'errors': ['lost write']}):
            bad = dict(report, pressure=dict(report['pressure'], **change))
            with self.assertRaises(ValueError):
                scope.validate_sdk(bad, 'dfs', 'pressure')
        for bad in (dict(report, cases=report['cases'][:-1]), dict(report, cleanup_errors=['mount remains']),
                    dict(report, mode='ownerfs'), dict(report, peer_reads={'before': 1, 'after': 1})):
            with self.assertRaises(ValueError):
                scope.validate_sdk(bad, 'dfs', 'pressure')

    def test_restart_requires_process_session_backend_and_during_io_evidence(self):
        scope = load('special_restart_contract', HERE / 'afs_specials.py')
        restart = {n: {'before_pid': 10, 'after_pid': 11, 'before_session': 'old',
                       'after_session': 'new', 'before_backend': ['same'], 'after_backend': ['same']}
                   for n in ('node1', 'node2')}
        report = {'status': 'passed', 'mode': 'ownerfs', 'special': 'restart', 'cleanup_errors': [],
                  'cases': [{'id': n, 'status': 'passed'} for n in scope.SDK_CHECKS['restart']],
                  'restart': restart, 'workload': {'paused_state': 'T', 'paused_pid': 10,
                                                  'before_restart': 2, 'at_ready': 2, 'after_restart': 3}}
        scope.validate_sdk(report, 'ownerfs', 'restart')
        for field, value in (('after_pid', 10), ('after_session', 'old'), ('after_backend', ['replacement'])):
            bad = dict(report, restart=dict(restart, node1=dict(restart['node1'], **{field: value})))
            with self.assertRaises(ValueError):
                scope.validate_sdk(bad, 'ownerfs', 'restart')
        for change in ({'paused_state': 'R'}, {'before_restart': 0}, {'after_restart': 2}, {'at_ready': 3}):
            with self.assertRaises(ValueError):
                scope.validate_sdk(dict(report, workload=dict(report['workload'], **change)), 'ownerfs', 'restart')

    def test_native_campaign_requires_an_explicit_cleanup_receipt(self):
        suite = load('special_suite_cleanup', HERE / 'kubernetes/targeted_suite.py')

        def execute(case, command, _log, _timeout, _deadline):
            path = Path(command[-1])
            path.mkdir()
            (path / 'result.json').write_text(json.dumps({
                'status': 'passed', 'checks': [case], 'required_checks': [case], 'missing_checks': [],
                'cleanup_errors': [], 'harness': {'commit': 'a' * 40},
                'cases': [{'name': case, 'status': 'passed'}],
            }))
            return 0, False
        with tempfile.TemporaryDirectory() as directory:
            report = suite.run_suite(('afs-component-specials',), Path(directory) / 'run',
                                     (), 1800, deployment='local-docker', execute=execute)
            self.assertEqual(report['status'], 'failed')
            self.assertTrue(report['cleanup_errors'])

    def test_detached_handles_are_deleted_explicitly_and_catalog_absence_is_checked(self):
        sdk = load('special_explicit_delete', HERE / 'afs_special_sdk.py')
        records = {'environment:a': {}, 'environment:b': {}}
        environment = Mock()

        def delete_record(identity, **_kw):
            return records.pop('environment:' + identity)

        environment.delete.side_effect = delete_record
        sdk.cleanup_environments(None, {'instances': {'A': 'a', 'B': 'b'}}, environment, lambda: records)
        self.assertEqual(environment.delete.call_count, 2)
        environment.kill.assert_not_called()
        self.assertEqual(records, {})

    def test_partial_attach_closes_first_local_client(self):
        sdk = load('special_partial_attach', HERE / 'afs_special_sdk.py')
        module = ModuleType('adx_environment')
        first = Mock()
        module.Environment = Mock()
        module.Environment.from_id.side_effect = [first, RuntimeError('second attach failed')]
        helpers = ModuleType('afs_sdk')
        helpers.command, helpers.perl = Mock(), Mock()
        with patch.dict(sys.modules, {'adx_environment': module, 'afs_sdk': helpers}):
            with self.assertRaisesRegex(RuntimeError, 'second attach'):
                sdk.pressure_worker(None, {'A': 'a', 'B': 'b'}, 0)
        first.close.assert_called_once()

    def test_pressure_corruption_is_not_retried_or_counted_as_success(self):
        sdk = load('special_corruption', HERE / 'afs_special_sdk.py')
        module = ModuleType('adx_environment')
        first, second = Mock(), Mock()
        second.files.read.return_value = b'corrupted'
        module.Environment = Mock()
        module.Environment.from_id.side_effect = [first, second]
        helpers = ModuleType('afs_sdk')
        helpers.command, helpers.perl = Mock(), Mock()
        with patch.dict(sys.modules, {'adx_environment': module, 'afs_sdk': helpers}):
            with self.assertRaisesRegex(AssertionError, 'content mismatch'):
                sdk.pressure_worker(None, {'A': 'a', 'B': 'b'}, 0)
        second.files.read.assert_called_once()
        first.close.assert_called_once()
        second.close.assert_called_once()

    def test_foreign_environment_state_cannot_be_reused_for_cleanup(self):
        sdk = load('special_foreign_state', HERE / 'afs_special_sdk.py')
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / 'state.json'
            path.write_text(json.dumps({'owner': 'other', 'mode': 'dfs', 'special': 'restart',
                                        'instances': {'A': 'do-not-delete'}}))
            with self.assertRaisesRegex(ValueError, 'different run'):
                sdk.owned_state({'owner': 'mine', 'mode': 'dfs', 'special': 'restart'}, path)

    def test_partial_sdk_creation_deletes_the_created_environment(self):
        sdk = load('special_partial_creation', HERE / 'afs_special_sdk.py')
        records, closed = {}, []

        class Environment:
            def __init__(self, **options):
                if records:
                    raise RuntimeError('second create failed')
                self.id = 'created-first'
                records['environment:' + self.id] = json.dumps({'assignment': {'node_id': options['node_id']}})

            def close(self):
                closed.append(self.id)

            @classmethod
            def delete(cls, identity, **_kwargs):
                records.pop('environment:' + identity)

        module, helpers, node = ModuleType('adx_environment'), ModuleType('afs_sdk'), ModuleType('node')
        module.Environment, module.Mount = Environment, Mock()
        module.__file__ = '/opt/adx/client/lib/python3.12/site-packages/adx_environment/__init__.py'

        def perl(code, *_args, **_kwargs):
            return code

        def command(_handle, code):
            if 'print $>' in code:
                return '10000'
            return '1 0 0:1 / /afs rw - ext4 /dev/x rw' if code.startswith('cat ') else '1 2'

        def catalog():
            return records

        helpers.perl = perl
        helpers.command = command
        helpers.write_synced = Mock()
        node.catalog = catalog
        with tempfile.TemporaryDirectory() as directory, \
                patch.dict(sys.modules, {'adx_environment': module, 'afs_sdk': helpers, 'node': node}), \
                patch.object(sdk.importlib.metadata, 'version', return_value='candidate'):
            root = Path(directory)
            report = sdk.run_phase(None, 'fixed-image', {
                'owner': 'mine', 'mode': 'ownerfs', 'special': 'restart',
                'paths': {'A': '/fixture/a', 'B': '/fixture/b'}, 'identities': {'A': [1, 2], 'B': [3, 4]},
            }, 'prepare', root / 'state.json', root / 'result.json')
            self.assertEqual(report['status'], 'failed')
            self.assertIn('second create failed', report['error'])
            self.assertEqual(records, {})
            self.assertEqual(closed, ['created-first'])
            self.assertEqual(report['cleanup_errors'], [])


if __name__ == '__main__':
    unittest.main()
