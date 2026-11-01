"""Storage observation counts hard links once and never follows foreign data."""

import importlib.util
import os
from pathlib import Path
import tempfile
import unittest
from unittest.mock import patch


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


if __name__ == '__main__':
    unittest.main()
