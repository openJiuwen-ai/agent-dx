import hashlib
import importlib.util
import tarfile
import tempfile
import unittest
from pathlib import Path
from unittest import mock


spec = importlib.util.spec_from_file_location(
    "download_runsc", Path(__file__).resolve().parents[1] / "download_runsc.py"
)
runsc_release = importlib.util.module_from_spec(spec)
spec.loader.exec_module(runsc_release)


class DownloadRunscTests(unittest.TestCase):
    target = "x86_64-unknown-linux-gnu"
    machine = 62

    def runtime_files(self, root):
        runsc = root / "launcher"
        runsc.write_bytes(b"\x7fELF\x02\x01" + bytes(12) + self.machine.to_bytes(2, "little"))
        runsc.chmod(0o755)
        helpers = root / "gvisor-bin"
        helpers.mkdir()
        for name in runsc_release.HELPERS:
            helper = helpers / name
            helper.write_bytes(b"\x7fELF\x02\x01" + bytes(12) + self.machine.to_bytes(2, "little"))
            helper.chmod(0o755)
        return runsc

    def artifact(self, runsc):
        files = {"runsc": hashlib.sha256(runsc.read_bytes()).hexdigest()}
        files.update(
            {
                f"gvisor-bin/{name}": hashlib.sha256(
                    (runsc.parent / "gvisor-bin" / name).read_bytes()
                ).hexdigest()
                for name in runsc_release.HELPERS
            }
        )
        return {
            self.target: {
                "url": "https://example.invalid/kit", "sha512": "0", "machine": self.machine, "files": files
            }
        }

    def test_extracts_and_verifies_complete_runtime_kit(self):
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            source = root / "source"
            source.mkdir()
            launcher = self.runtime_files(source)
            archive = root / "kit.tar.bz2"
            with tarfile.open(archive, "w:bz2") as bundle:
                bundle.add(launcher, "runsc")
                bundle.add(source / "gvisor-bin", "gvisor-bin")
                bundle.add(launcher, "containerd-shim-runsc-v1")
            destination = root / "runtime"
            with mock.patch.object(runsc_release, "ARTIFACTS", self.artifact(launcher)):
                runsc_release.extract_runtime(archive, destination, self.target)
                verified = runsc_release.verify_runtime(destination / "runsc", self.target)
            self.assertEqual(set(verified), {"runsc", *(f"gvisor-bin/{name}" for name in runsc_release.HELPERS)})
            self.assertFalse((destination / "containerd-shim-runsc-v1").exists())
            for name in runsc_release.HELPERS:
                self.assertTrue((destination / "gvisor-bin" / name).stat().st_mode & 0o100)

    def test_rejects_unsafe_member_before_extraction(self):
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            source = root / "source"
            source.mkdir()
            launcher = self.runtime_files(source)
            archive = root / "unsafe.tar.bz2"
            with tarfile.open(archive, "w:bz2") as bundle:
                bundle.add(launcher, "../runsc")
            with mock.patch.object(runsc_release, "ARTIFACTS", self.artifact(launcher)):
                with self.assertRaisesRegex(ValueError, "unsafe gVisor archive member"):
                    runsc_release.extract_runtime(archive, root / "runtime", self.target)


if __name__ == "__main__":
    unittest.main()
