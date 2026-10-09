"""Scoped static/dynamic dependency admission for the test-only rootfs."""
import importlib.util
from pathlib import Path
import struct
import subprocess
import tempfile
import unittest

spec = importlib.util.spec_from_file_location('prepare_rootfs', Path(__file__).with_name('prepare-workspace-rootfs-linux.py'))
prepare = importlib.util.module_from_spec(spec)
spec.loader.exec_module(prepare)


class LinkageAdmission(unittest.TestCase):
    def elf(self, root, machine=183, program_type=1):
        data = bytearray(120)
        data[:7] = b'\x7fELF\x02\x01\x01'
        struct.pack_into('<HH', data, 16, 2, machine)
        struct.pack_into('<Q', data, 32, 64)
        struct.pack_into('<HH', data, 54, 56, 1)
        struct.pack_into('<I', data, 64, program_type)
        path = Path(root) / 'elf'
        path.write_bytes(data)
        return path

    def test_static_busybox_requires_valid_arm64_table_without_interpreter(self):
        done = subprocess.CompletedProcess(['ldd'], 1, '', '\tnot a dynamic executable\n')
        with tempfile.TemporaryDirectory() as root:
            self.assertEqual(prepare.admit_ldd(self.elf(root), done), 'static-arm64-elf')
            for machine, program in [(62, 1), (183, 3)]:
                with self.assertRaises(RuntimeError):
                    prepare.admit_ldd(self.elf(root, machine, program), done)

    def test_corrupt_and_truncated_elf_are_not_static_fallback(self):
        with tempfile.TemporaryDirectory() as root:
            path = self.elf(root)
            for data in [b'not ELF', path.read_bytes()[:119]]:
                path.write_bytes(data)
                self.assertFalse(prepare.static_arm64_elf(path))

    def test_missing_library_and_unexpected_ldd_failure_still_reject(self):
        with tempfile.TemporaryDirectory() as root:
            path = self.elf(root)
            for code, output in [(0, 'libc.so => not found'), (1, 'permission denied'), (2, 'not a dynamic executable')]:
                with self.assertRaises(RuntimeError):
                    prepare.admit_ldd(path, subprocess.CompletedProcess(['ldd'], code, output, ''))

    def test_resolved_dynamic_output_is_admitted(self):
        self.assertEqual(prepare.admit_ldd('/unused', subprocess.CompletedProcess(['ldd'], 0, 'libc.so => /lib/libc.so (0x123)', '')), 'dynamic')


if __name__ == '__main__':
    unittest.main()
