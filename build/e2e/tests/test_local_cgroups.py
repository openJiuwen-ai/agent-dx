import importlib.util
from pathlib import Path
import tempfile
import unittest

ROOT = Path(__file__).resolve().parents[1]
spec = importlib.util.spec_from_file_location('local_cgroups_driver', ROOT / 'run.py')
driver = importlib.util.module_from_spec(spec)
spec.loader.exec_module(driver)


class LocalCgroupTests(unittest.TestCase):
    def test_cgroup_mode_is_explicit_and_validated(self):
        self.assertEqual(driver.Run(Path('/tmp/result')).cgroupns, 'private')
        self.assertEqual(driver.Run(Path('/tmp/result'), cgroupns='host').cgroupns, 'host')
        with self.assertRaises(ValueError):
            driver.Run(Path('/tmp/result'), cgroupns='invalid')

    def test_cleanup_only_removes_empty_owned_cgroup(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            owned = root / 'adx-e2e-owned-node1'
            (owned / 'child').mkdir(parents=True)
            foreign = root / 'unrelated'
            foreign.mkdir()
            driver.cleanup_cgroup(owned)
            self.assertFalse(owned.exists())
            self.assertTrue(foreign.exists())
            owned.mkdir()
            (owned / 'occupied').write_text('do not delete')
            with self.assertRaises(OSError):
                driver.cleanup_cgroup(owned)
            self.assertEqual((owned / 'occupied').read_text(), 'do not delete')

    def test_limits_follow_container_membership_in_host_namespace(self):
        spec = importlib.util.spec_from_file_location('cgroup_limits', ROOT / 'cgroup_limits.py')
        limits = importlib.util.module_from_spec(spec)
        spec.loader.exec_module(limits)
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            own = root / 'system.slice/docker-test.scope'
            own.mkdir(parents=True)
            (own / 'cpu.max').write_text('300000 100000')
            (own / 'memory.max').write_text('4294967296')
            self.assertEqual(limits.v2_directory(root, '0::/system.slice/docker-test.scope\n'), own)
            with self.assertRaises(ValueError):
                limits.v2_directory(root, '0::/../../escape\n')
            (root / 'cpu.max').write_text('300000 100000')
            (root / 'memory.max').write_text('4294967296')
            self.assertEqual(limits.v2_directory(root, '0::/\n'), root)
