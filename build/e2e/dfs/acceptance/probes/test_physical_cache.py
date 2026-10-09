"""Real Linux ext4 checks for range boundaries, identity and no-touch mincore."""
import os
from pathlib import Path
import platform
import tempfile
import unittest

import physical_cache


@unittest.skipUnless(platform.system() == "Linux", "Linux integration only")
class PhysicalCache(unittest.TestCase):
    def setUp(self):
        self.scratch = tempfile.TemporaryDirectory(prefix="afs-cache-", dir="/var/tmp")
        self.addCleanup(self.scratch.cleanup)
        self.path = Path(self.scratch.name) / "payload"
        self.page = os.sysconf("SC_PAGE_SIZE")
        self.path.write_bytes(b"X" * self.page * 3)

    def test_hot_unaligned_payload_excludes_header_and_tail(self):
        before = physical_cache.identity(self.path.stat())
        result = physical_cache.observe(self.path, self.page + 17, self.page + 3, before)
        self.assertEqual(result["resident_bytes"], self.page + 3)
        self.assertEqual(result["pages"], 2)
        self.assertTrue(result["fully_resident"])
        self.assertEqual(result["identity"], before)
        self.assertEqual(physical_cache.identity(self.path.stat()), before)

    def test_reject_changed_file_identity(self):
        before = physical_cache.identity(self.path.stat())
        replacement = self.path.with_name("replacement")
        replacement.write_bytes(b"Y" * self.page * 3)
        replacement.replace(self.path)
        with self.assertRaisesRegex(ValueError, "identity changed"):
            physical_cache.observe(self.path, 0, self.page, before)

    def test_reject_symlink_directory_relative_and_invalid_ranges(self):
        link = self.path.with_name("link")
        link.symlink_to(self.path)
        with self.assertRaises(OSError):
            physical_cache.observe(link, 0, 1)
        with self.assertRaises(ValueError):
            physical_cache.observe(self.path.parent, 0, 1)
        with self.assertRaises(ValueError):
            physical_cache.observe(Path("relative"), 0, 1)
        for offset, length in [(-1, 1), (0, 0), (True, 1), (0, False), (0, self.page * 3 + 1)]:
            with self.subTest(offset=offset, length=length), self.assertRaises(ValueError):
                physical_cache.observe(self.path, offset, length)

    def test_failed_observations_do_not_leak_descriptors_or_change_content(self):
        before = sorted(os.listdir("/proc/self/fd"))
        ident = physical_cache.identity(self.path.stat())
        for _ in range(20):
            with self.assertRaises(ValueError):
                physical_cache.observe(self.path, 0, self.page * 4)
        self.assertEqual(sorted(os.listdir("/proc/self/fd")), before)
        self.assertEqual(physical_cache.identity(self.path.stat()), ident)
        self.assertEqual(self.path.read_bytes(), b"X" * self.page * 3)

    def test_observing_evicted_pages_does_not_prefault_them(self):
        with self.path.open("rb") as handle:
            os.fdatasync(handle.fileno())
            os.posix_fadvise(handle.fileno(), 0, 0, os.POSIX_FADV_DONTNEED)
        first = physical_cache.observe(self.path, 0, self.page * 3)
        second = physical_cache.observe(self.path, 0, self.page * 3, first["identity"])
        self.assertEqual(first["resident_bytes"], 0)
        self.assertEqual(second["resident_bytes"], 0)
        self.assertFalse(second["fully_resident"])


if __name__ == "__main__":
    unittest.main()
