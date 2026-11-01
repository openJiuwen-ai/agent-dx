"""Backing allocation must exclude mounted views, sparse holes and duplicate links."""

import importlib.util
import os
from pathlib import Path
import tempfile
import unittest


def module():
    spec = importlib.util.spec_from_file_location('backing_probe_tests',
                                                Path(__file__).resolve().parents[1] / 'soak_probe.py')
    value = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(value)
    return value


class BackingAllocation(unittest.TestCase):
    def test_sparse_length_is_not_allocated_disk(self):
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            with (root / 'sparse').open('wb') as stream:
                stream.truncate(1024**3)
            result = module().backing_storage(root, set())
            self.assertLess(result['bytes'], 1024**2)

    def test_even_same_device_bind_view_is_skipped_and_real_upper_bytes_remain(self):
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            view = root / 'rootfs'
            upper = root / 'upper'
            view.mkdir()
            upper.mkdir()
            (view / 'shared-image').write_bytes(b'x' * 65536)
            (upper / 'real-write').write_bytes(b'y' * 4096)
            result = module().backing_storage(root, {str(view)})
            expected = sum(path.lstat().st_blocks * 512 for path in (root, upper, upper / 'real-write'))
            self.assertEqual(result['bytes'], expected)
            self.assertIn(str(view), result['excluded_mounts'])

    def test_hardlinks_count_once_and_symlink_targets_are_not_followed(self):
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            (root / 'data').write_bytes(b'x' * 4096)
            os.link(root / 'data', root / 'same')
            (root / 'outside').symlink_to('/does-not-belong-to-this-run')
            result = module().backing_storage(root, set())
            self.assertEqual(result['bytes'], sum(path.lstat().st_blocks * 512
                                                 for path in (root, root / 'data', root / 'outside')))

    def test_mountinfo_escapes_are_decoded_without_resolving_foreign_paths(self):
        info = '5 1 0:2 / /tmp/sandbox\u005c040data/rootfs rw - overlay overlay rw\n'
        self.assertIn('/tmp/sandbox data/rootfs', module().mount_paths(info))


if __name__ == '__main__':
    unittest.main()
