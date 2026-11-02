"""Storage observation counts hard links once and never follows foreign data."""

import importlib.util
import os
import json
import sys
from pathlib import Path
import tempfile
import unittest
from unittest.mock import patch
from unittest.mock import Mock
from types import ModuleType


class SoakStorageProbe(unittest.TestCase):
    def module(self):
        spec = importlib.util.spec_from_file_location('soak_storage_probe',
                                                    Path(__file__).resolve().parents[1] / 'soak_probe.py')
        value = importlib.util.module_from_spec(spec)
        spec.loader.exec_module(value)
        return value

    def test_external_symlink_and_duplicate_hardlink_do_not_inflate_data(self):
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            owned = root / 'owned'
            owned.mkdir()
            (root / 'foreign.log').write_bytes(b'foreign' * 10000)
            (owned / 'one.log').write_bytes(b'a' * 100)
            os.link(owned / 'one.log', owned / 'two.log')
            (owned / 'outside.log').symlink_to(root / 'foreign.log')
            total, logs = self.module().storage(owned)
            self.assertEqual(logs, 0)
            self.assertEqual(total, owned.lstat().st_size + 100 + (owned / 'outside.log').lstat().st_size)

    def test_concurrent_retirement_is_skipped_but_permission_error_is_not(self):
        probe = self.module()
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            (root / 'retired.log').write_bytes(b'x')
            original = Path.lstat

            def disappearing(path):
                if path.name == 'retired.log':
                    raise FileNotFoundError(str(path))
                return original(path)
            with patch.object(Path, 'lstat', disappearing):
                self.assertEqual(probe.storage(root), (root.stat().st_size, 0))
            with patch.object(Path, 'lstat', side_effect=PermissionError('cannot inspect owned directory')):
                with self.assertRaises(PermissionError):
                    probe.storage(root)

    def test_only_declared_fixture_stdout_is_excluded_from_persistent_data(self):
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            fixture = root / 'afs-test-owned'
            state = fixture / 'M/state'
            state.mkdir(parents=True)
            (fixture / 'M.log').write_bytes(b'x' * 50)
            (state / 'meta.log').write_bytes(b'x' * 100)
            total, logs = self.module().storage(root)
            self.assertEqual(logs, 50)
            self.assertGreaterEqual(total - logs, 100)

    def test_anchor_lookup_uses_existing_label_and_bounded_runc_queries(self):
        probe = self.module()
        node = ModuleType('node')
        node.P = Path('/tmp/adx-e2e')
        node.command = Mock(side_effect=['ID STATUS\nphysical-1 running\n',
                                         json.dumps({'id': 'physical-1', 'status': 'running', 'pid': 33})])
        with patch.dict(sys.modules, {'node': node}), patch.object(probe, 'process_info', return_value={
                'pid': 33, 'start_ticks': 9, 'rss_bytes': 100, 'fds': 3}):
            value = probe.anchor_runtime('environment-a')
        self.assertEqual(value['runtime_id'], 'physical-1')
        self.assertEqual(value['environment_id'], 'environment-a')
        self.assertEqual(value['pid'], 33)
        self.assertIn('adx.environment_id=environment-a', node.command.call_args_list[0].args[0])
        self.assertTrue(all(call.kwargs['timeout'] == 5 for call in node.command.call_args_list))

    def test_missing_or_ambiguous_backend_and_stopped_runc_are_not_accepted(self):
        probe = self.module()
        for replies in (['ID STATUS\n'], ['ID STATUS\na running\nb running\n'],
                        ['ID STATUS\na running\n', json.dumps({'id': 'a', 'status': 'stopped', 'pid': 33})]):
            node = ModuleType('node')
            node.P, node.command = Path('/tmp/adx-e2e'), Mock(side_effect=replies)
            with patch.dict(sys.modules, {'node': node}), self.assertRaises(ValueError):
                probe.anchor_runtime('environment-a')

    def test_afs_probe_uses_one_census_count_and_refuses_replaced_process(self):
        probe = self.module()
        info = {'pid': 22, 'start_ticks': 90, 'rss_bytes': 100, 'fds': 99}
        census = {'pid': 22, 'start_ticks': 90, 'entries': [1, 2, 3]}
        with patch.object(probe, 'process_info', return_value=info.copy()), patch.object(
                probe.fd.inventory, 'census', return_value=census) as collect:
            result = probe.persistent_process(22, 'afs-node', 'dfs', '/owned/chunks')
            self.assertEqual(result['fds'], 3)
            self.assertEqual(result['fd_census'], census)
            collect.assert_called_once_with(22, '/owned/chunks')
        census['start_ticks'] += 1
        with patch.object(probe, 'process_info', return_value=info.copy()), patch.object(
                probe.fd.inventory, 'census', return_value=census), self.assertRaisesRegex(ValueError, 'changed'):
            probe.persistent_process(22, 'afs-node', 'ownerfs')
        with patch.object(probe, 'process_info', return_value=info.copy()), patch.object(
                probe.fd.inventory, 'census') as collect:
            self.assertNotIn('fd_census', probe.persistent_process(22, 'adxlet', 'dfs'))
            collect.assert_not_called()


if __name__ == '__main__':
    unittest.main()
