"""The immutable runtime payload must be traversable by a non-root workload."""
import importlib.util
import os
from pathlib import Path
import stat
import tempfile
import unittest
from unittest.mock import patch

SPEC = importlib.util.spec_from_file_location("runtime_rootfs", Path(__file__).parents[1] / "rootfs.py")
ROOTFS = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(ROOTFS)


class RuntimePayloadPermissions(unittest.TestCase):
    def test_non_root_can_traverse_payload_to_execd(self):
        for mask in (0o022, 0o027, 0o077):
            with self.subTest(umask=oct(mask)):
                previous = os.umask(mask)
                try:
                    self.check_payload_permissions()
                finally:
                    os.umask(previous)

    def check_payload_permissions(self):
        observed = []

        def inspect_image_input(command, **_kwargs):
            if command[0] == "mkfs.erofs":
                root = Path(command[-1])
                # EROFS copies these modes into /__adx; root-owned 0700 blocks
                # every non-root execution even when adx-execd itself is 0755.
                for directory, _, _ in os.walk(root, followlinks=False):
                    path = Path(directory)
                    mode = stat.S_IMODE(path.stat().st_mode)
                    expected = 0o1777 if path in (root / "tmp", root / "var/tmp") else 0o755
                    observed.append((str(path.relative_to(root)), mode, expected))

        with tempfile.TemporaryDirectory() as temporary:
            base = Path(temporary)
            binary = base / "execd"
            binary.write_bytes(b"runtime")
            busybox = base / "busybox"
            busybox.write_bytes(b"busybox")
            with (
                patch.object(ROOTFS, "static_executable"),
                patch.object(ROOTFS, "_executable", side_effect=lambda name: name),
                patch.object(ROOTFS.subprocess, "check_output", return_value="sh\n"),
                patch.object(ROOTFS.subprocess, "run", side_effect=inspect_image_input),
            ):
                ROOTFS.build(binary, base / "runtime.img", busybox=busybox)
        self.assertTrue(observed)
        for path, mode, expected in observed:
            self.assertEqual(mode, expected, f"incorrect runtime directory permissions: {path}")


if __name__ == "__main__":
    unittest.main()
