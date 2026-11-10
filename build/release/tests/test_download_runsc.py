"""The qualified gVisor release must travel with every adjacent helper."""
import hashlib
import importlib.util
import io
from pathlib import Path
import sys
import tarfile
import tempfile
import unittest
from unittest.mock import patch

RELEASE = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(RELEASE))
SPEC = importlib.util.spec_from_file_location("download_runsc", RELEASE / "download_runsc.py")
runsc = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(runsc)

FILES = (
    "runsc", "containerd-shim-runsc-v1", "gvisor-bin/checkpointgofer",
    "gvisor-bin/gvisor-sentry-prewarmer", "gvisor-bin/gvisor_sentry",
    "gvisor-bin/runsc-fd-parking", "gvisor-bin/runsc-metric-server",
)


class DownloadTests(unittest.TestCase):
    def download(self, root, *, missing=None, wrong_machine=None, extra=None, bad_checksum=False):
        archive = root / "fixture.tar.bz2"
        elf = b"\x7fELF\x02\x01" + bytes(12) + (62).to_bytes(2, "little")
        with tarfile.open(archive, "w:bz2") as bundle:
            for name in (*FILES, *((extra,) if extra else ())):
                if name == missing:
                    continue
                data = elf if name != wrong_machine else elf[:18] + (183).to_bytes(2, "little")
                item = tarfile.TarInfo(name)
                item.size = len(data)
                bundle.addfile(item, io.BytesIO(data))
        digest = hashlib.sha512(archive.read_bytes()).hexdigest()
        pins = {"fixture": (
            "amd64", hashlib.sha256(archive.read_bytes()).hexdigest(), "0" * 128 if bad_checksum else digest, 62,
        )}
        destination = root / "runtime/runsc"
        destination.parent.mkdir()

        def curl(args, **_kwargs):
            Path(args[args.index("--output") + 1]).write_bytes(archive.read_bytes())

        with patch.object(runsc, "ARTIFACTS", pins), patch.object(runsc.subprocess, "run", side_effect=curl):
            runsc.download("fixture", destination)
        return destination

    def test_complete_release_is_installed_with_executable_helpers(self):
        with tempfile.TemporaryDirectory() as directory:
            output = self.download(Path(directory))
            for name in FILES:
                path = output.parent / name
                self.assertTrue(path.is_file(), name)
                self.assertEqual(path.stat().st_mode & 0o777, 0o755)

    def test_missing_helper_is_rejected_before_publication(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            with self.assertRaisesRegex(ValueError, "file set"):
                self.download(root, missing="gvisor-bin/runsc-fd-parking")
            self.assertEqual(list((root / "runtime").iterdir()), [])

    def test_helper_architecture_is_verified(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            with self.assertRaisesRegex(ValueError, "architecture"):
                self.download(root, wrong_machine="gvisor-bin/gvisor_sentry")
            self.assertEqual(list((root / "runtime").iterdir()), [])

    def test_archive_traversal_is_rejected_before_publication(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            with self.assertRaisesRegex(ValueError, "archive entry"):
                self.download(root, extra="../escape")
            self.assertFalse((root / "escape").exists())
            self.assertEqual(list((root / "runtime").iterdir()), [])

    def test_archive_checksum_is_verified_before_unpacking(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            with self.assertRaisesRegex(ValueError, "SHA512"):
                self.download(root, bad_checksum=True)
            self.assertEqual(list((root / "runtime").iterdir()), [])


if __name__ == "__main__":
    unittest.main()
