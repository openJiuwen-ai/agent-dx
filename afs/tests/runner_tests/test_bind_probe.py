"""Probe reporting/ownership contracts; no native mounts occur in these tests."""

import errno
import importlib
from pathlib import Path
import sys
import tempfile
import unittest
from unittest.mock import Mock

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))


class Operations:
    def __init__(self, private_error=None, detach_error=None):
        self.private_error = private_error
        self.detach_error = detach_error
        self.calls = []

    def clone(self, _source):
        return 999999  # a fake fd, closed only through this fake operations object

    def policy(self, _tree, private):
        self.calls.append(('policy', private))
        if private and self.private_error:
            raise OSError(self.private_error, 'private policy rejected')

    def attach(self, _tree, _target):
        self.calls.append(('attach',))

    def close_tree(self, _tree):
        self.calls.append(('close-tree',))

    def detach(self, _target):
        self.calls.append(('detach',))
        if self.detach_error:
            raise OSError(self.detach_error, 'unmount rejected')


class BindProbeTests(unittest.TestCase):
    def setUp(self):
        self.module = importlib.import_module('fixtures.bind_probe')
        self.temp = tempfile.TemporaryDirectory()
        self.addCleanup(self.temp.cleanup)
        self.root = Path(self.temp.name)
        (self.root / '.afs-test-owner').write_text('owned-token')

    def test_private_policy_failure_is_not_hidden_by_successful_control(self):
        ops = Operations(private_error=errno.EINVAL)
        report = self.module.probe(self.root, 'owned-token', ops)
        self.assertEqual(report['status'], 'FAILED')
        self.assertEqual(report['first_error']['step'], 'mount_setattr_private')
        self.assertEqual(report['first_error']['errno'], errno.EINVAL)
        self.assertEqual(report['control_without_private']['status'], 'OK')
        self.assertNotIn(('attach',), ops.calls)
        self.assertEqual(report['cleanup']['status'], 'PASS')
        self.assertEqual(sorted(p.name for p in self.root.iterdir()), ['.afs-test-owner'])

    def test_success_closes_tree_before_normal_unmount(self):
        ops = Operations()
        report = self.module.probe(self.root, 'owned-token', ops)
        self.assertEqual(report['status'], 'CAPABLE')
        self.assertLess(ops.calls.index(('close-tree',)), ops.calls.index(('detach',)))
        self.assertEqual(report['cleanup']['status'], 'PASS')

    def test_cleanup_failure_preserves_owned_probe_and_cannot_report_capable(self):
        report = self.module.probe(self.root, 'owned-token', Operations(detach_error=errno.EBUSY))
        self.assertEqual(report['status'], 'CLEANUP_FAILED')
        self.assertEqual(report['cleanup']['status'], 'FAIL')
        self.assertTrue(Path(report['probe_directory']).is_dir())

    def test_wrong_owner_is_rejected_before_any_operations(self):
        ops = Operations()
        with self.assertRaises(ValueError):
            self.module.probe(self.root, 'wrong-token', ops)
        self.assertFalse(ops.calls)

    def test_legacy_failure_and_bind_remount_control_are_reported_separately(self):
        class LegacyOperations(Operations):
            def clone(self, _source):
                raise OSError(errno.ENOSYS, 'new mount API absent')

            def legacy_bind(self, _source, _target):
                self.calls.append(('legacy-bind',))

            def legacy_private(self, _target):
                self.calls.append(('legacy-private',))

            def legacy_policy(self, _target, bind_remount=False):
                self.calls.append(('legacy-policy', bind_remount))
                if not bind_remount:
                    raise OSError(errno.EINVAL, 'filesystem remount rejected')

        report = self.module.probe(self.root, 'owned-token', LegacyOperations())
        self.assertEqual(report['first_error']['errno'], errno.ENOSYS)
        self.assertEqual(report['legacy']['first_error']['step'], 'remount_without_bind')
        self.assertEqual(report['legacy']['control_bind_remount']['status'], 'OK')
        self.assertEqual(report['status'], 'FAILED')
        self.assertEqual(report['cleanup']['status'], 'PASS')

    def test_missing_setattr_follows_product_fallback_and_tests_attached_tree_control(self):
        class PartialApi(Operations):
            def policy(self, _tree, _private):
                raise OSError(errno.ENOSYS, 'mount_setattr absent')

            def legacy_private(self, _target):
                self.calls.append(('private-tree',))

            def legacy_policy(self, target, bind_remount=False):
                if str(target).endswith('/999999') and bind_remount:
                    return
                raise OSError(errno.EINVAL, 'held covered target')

        report = self.module.probe(self.root, 'owned-token', PartialApi())
        self.assertEqual(report['first_error']['step'], 'legacy_remount_held_target')
        self.assertEqual(report['first_error']['errno'], errno.EINVAL)
        self.assertEqual(report['control_held_target_bind']['errno'], errno.EINVAL)
        self.assertEqual(report['control_attached_tree']['status'], 'OK')
        self.assertEqual(report['status'], 'FAILED')
        self.assertEqual(report['cleanup']['status'], 'PASS')

    def test_diagnostic_cannot_replace_the_original_topology_failure(self):
        from fixtures.multivm import Cluster
        cluster = object.__new__(Cluster)
        cluster.mode, cluster.roles, cluster.token = 'ownerfs', ['M', 'A'], 'owned-token'
        cluster.workspace = Mock(return_value='/owned/egress/workspace')
        cluster.root = Mock(return_value='/owned')
        cluster.command = Mock(side_effect=AssertionError('original missing bind'))
        cluster.python = Mock(return_value={'returncode': 0, 'stdout': '{"status":"CAPABLE"}'})
        cluster.record = Mock()
        with self.assertRaisesRegex(AssertionError, 'original missing bind'):
            cluster.topology()
        cluster.python.assert_called_once()
        self.assertEqual(cluster.record.call_args.args[0], 'ownerfs.bind_diagnostic')


if __name__ == '__main__':
    unittest.main()
