import io
import json
import shutil
import subprocess
import sys
import tarfile
import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch

HERE = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(HERE))
import download_backend as backend

sys.path.pop(0)

TARGET = "x86_64-unknown-linux-gnu"


class BackendObsTests(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.addCleanup(self.tmp.cleanup)
        self.root = Path(self.tmp.name)
        self.payload = self.root / "payload"
        self.payload.mkdir()
        self.output = self.root / "installed"
        self.archive = self.root / "backend.tar.gz"
        self.pin = self.root / "artifacts.json"
        pinned = json.loads((backend.ROOT / "third_party/sandboxd/source.json").read_text())
        self.revision = pinned["revision"]
        for name in backend.BACKEND_BINARIES:
            self.payload.joinpath(name).write_bytes(
                b"\x7fELF\x02\x01"
                + bytes(12)
                + (62).to_bytes(2, "little")
                + name.encode()
            )
        self.manifest = {
            "sandboxd_revision": self.revision,
            "sandboxd_patches": [
                {"path": patch["path"], "sha256": patch["sha256"]}
                for patch in pinned["patches"]
            ],
            "target": TARGET,
            "files": {
                name: backend.sha(self.payload / name)
                for name in backend.BACKEND_BINARIES
            },
        }
        self.pack()
        self.fetch = patch.object(
            backend,
            "fetch_archive",
            side_effect=lambda url, path: shutil.copyfile(self.archive, path),
        )
        self.fetch.start()
        self.addCleanup(self.fetch.stop)

    def pack(self, extra=None, omit=None):
        (self.payload / "manifest.json").write_text(json.dumps(self.manifest))
        with tarfile.open(self.archive, "w:gz") as archive:
            for path in self.payload.iterdir():
                if path.name != omit:
                    archive.add(path, arcname="./" + path.name)
            if extra:
                member, data = extra
                archive.addfile(member, io.BytesIO(data) if data else None)
        self.pin.write_text(
            json.dumps(
                {
                    "schema_version": 1,
                    "sandboxd_revision": self.revision,
                    "artifacts": {
                        TARGET: {
                            "url": "https://example.obs.cn-southwest-2.myhuaweicloud.com/backend.tar.gz",
                            "bytes": self.archive.stat().st_size,
                            "sha256": backend.sha(self.archive),
                        }
                    },
                }
            )
        )

    def install(self):
        backend.install(self.output, TARGET, self.pin)

    def test_verified_bundle_is_installed_with_executable_modes(self):
        self.install()
        self.assertEqual(
            {p.name for p in self.output.iterdir()},
            set(backend.BACKEND_BINARIES) | {"manifest.json"},
        )
        for name in backend.BACKEND_BINARIES:
            self.assertEqual(
                backend.sha(self.output / name), self.manifest["files"][name]
            )
            self.assertEqual((self.output / name).stat().st_mode & 0o777, 0o755)

    def test_corrupt_archive_never_publishes_output(self):
        self.archive.write_bytes(self.archive.read_bytes() + b"corrupt")
        with self.assertRaisesRegex(ValueError, "archive integrity"):
            self.install()
        self.assertFalse(self.output.exists())

    def test_corrupt_binary_is_rejected_even_with_valid_archive_hash(self):
        (self.payload / "sandboxd").write_bytes(b"corrupt")
        self.pack()
        with self.assertRaisesRegex(ValueError, "integrity"):
            self.install()
        self.assertFalse(self.output.exists())

    def test_wrong_revision_and_manifest_architecture_are_rejected(self):
        for field, value in [
            ("sandboxd_revision", "0" * 40),
            ("target", "aarch64-unknown-linux-gnu"),
        ]:
            with self.subTest(field=field):
                original = self.manifest[field]
                self.manifest[field] = value
                self.pack()
                with self.assertRaisesRegex(ValueError, "revision or architecture"):
                    self.install()
                self.assertFalse(self.output.exists())
                self.manifest[field] = original

    def test_actual_binary_architecture_is_checked(self):
        path = self.payload / "sandboxd"
        path.write_bytes(b"\x7fELF\x02\x01" + bytes(12) + (183).to_bytes(2, "little"))
        self.manifest["files"]["sandboxd"] = backend.sha(path)
        self.pack()
        with self.assertRaisesRegex(ValueError, "ELF architecture"):
            self.install()
        self.assertFalse(self.output.exists())

    def test_incomplete_duplicate_and_unsafe_entries_are_rejected(self):
        for name in ["../escaped", "/escaped", "nested/file", "./sandboxd"]:
            with self.subTest(name=name):
                info = tarfile.TarInfo(name)
                info.size = 4
                self.pack(extra=(info, b"evil"))
                with self.assertRaises(ValueError):
                    self.install()
                self.assertFalse(self.output.exists())
                self.assertFalse((self.root / "escaped").exists())
        link = tarfile.TarInfo("link")
        link.type = tarfile.SYMTYPE
        link.linkname = "../escaped"
        self.pack(extra=(link, b""))
        with self.assertRaises(ValueError):
            self.install()
        self.pack(omit="sbox")
        with self.assertRaisesRegex(ValueError, "file set"):
            self.install()
        self.assertFalse(self.output.exists())

    def test_failed_download_does_not_publish_a_partial_backend(self):
        with patch.object(
            backend,
            "fetch_archive",
            side_effect=subprocess.CalledProcessError(28, "curl"),
        ), self.assertRaises(subprocess.CalledProcessError):
            self.install()
        self.assertFalse(self.output.exists())
        self.assertFalse(list(self.root.glob(".backend-*")))

    def test_malformed_pin_is_rejected_before_download(self):
        original = json.loads(self.pin.read_text())
        for field, value in [
            ("url", "http://example.invalid/backend.tar.gz"),
            ("bytes", True),
            ("sha256", "bad"),
        ]:
            with self.subTest(field=field):
                pin = json.loads(json.dumps(original))
                pin["artifacts"][TARGET][field] = value
                self.pin.write_text(json.dumps(pin))
                with patch.object(backend, "fetch_archive") as fetch:
                    with self.assertRaisesRegex(ValueError, "artifact pin"):
                        self.install()
                    fetch.assert_not_called()

    def test_existing_output_is_preserved(self):
        self.output.mkdir()
        (self.output / "marker").write_text("keep")
        with self.assertRaisesRegex(ValueError, "already exists"):
            self.install()
        self.assertEqual((self.output / "marker").read_text(), "keep")

    def test_unavailable_target_and_bad_pin_are_rejected_before_download(self):
        with self.assertRaisesRegex(ValueError, "target"):
            backend.install(self.output, "aarch64-unknown-linux-gnu", self.pin)
        pin = json.loads(self.pin.read_text())
        pin["sandboxd_revision"] = "0" * 40
        self.pin.write_text(json.dumps(pin))
        with self.assertRaisesRegex(ValueError, "pin revision"):
            self.install()
        self.assertFalse(self.output.exists())

    def test_default_pin_and_ci_consumers_use_obs(self):
        pin = json.loads(
            (backend.ROOT / "third_party/sandboxd/artifacts.json").read_text()
        )
        self.assertEqual(pin["sandboxd_revision"], self.revision)
        self.assertIn(".obs.", pin["artifacts"][TARGET]["url"])
        for name in ["package-components.sh", "build-e2e.sh"]:
            text = (backend.ROOT / ".buildkite" / name).read_text()
            self.assertIn("build/e2e/download_backend.py", text)
            self.assertNotIn("ADX_BACKEND_ARTIFACT_BUILD", text)
        pipeline = (backend.ROOT / ".buildkite/pipeline-package.yml").read_text()
        self.assertNotIn("ADX_BACKEND_ARTIFACT_BUILD", pipeline)


if __name__ == "__main__":
    unittest.main()
