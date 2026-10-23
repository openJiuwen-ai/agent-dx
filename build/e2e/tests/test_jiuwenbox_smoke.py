import importlib.util
import json
from pathlib import Path
import tempfile
import unittest
import zipfile

ROOT = Path(__file__).resolve().parents[3]
SMOKE_SPEC = importlib.util.spec_from_file_location("jiuwenbox_smoke", ROOT / "build/e2e/jiuwenbox/smoke.py")
smoke = importlib.util.module_from_spec(SMOKE_SPEC)
SMOKE_SPEC.loader.exec_module(smoke)


class JiuwenboxSmokeTests(unittest.TestCase):
    def test_rejects_stale_or_dirty_release_before_starting_backends(self):
        self.assertEqual(len(smoke.EXPECTED_CASES), 8)
        self.assertFalse(any("conch" in name for name in smoke.EXPECTED_CASES))
        manifest = {"commit": "a" * 40, "dirty": False, "target": "x86_64-unknown-linux-gnu"}
        smoke.verify_identity(manifest, "a" * 40)
        with self.assertRaisesRegex(ValueError, "commit"):
            smoke.verify_identity(manifest, "b" * 40)
        with self.assertRaisesRegex(ValueError, "dirty"):
            smoke.verify_identity(dict(manifest, dirty=True), "a" * 40)
        with self.assertRaisesRegex(ValueError, "x86_64"):
            smoke.verify_identity(dict(manifest, target="aarch64-unknown-linux-gnu"), None)

    def test_installed_backend_payload_must_match_pinned_wheel(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            site = root / "site"
            (site / "jiuwenbox").mkdir(parents=True)
            installed = site / "jiuwenbox/runtime.py"
            installed.write_bytes(b"original")
            wheel = root / "backend.whl"
            with zipfile.ZipFile(wheel, "w") as archive:
                archive.writestr("jiuwenbox/runtime.py", b"original")
            self.assertEqual(smoke.verify_wheel(wheel, site, ("jiuwenbox/",)), 1)
            installed.write_bytes(b"modified")
            with self.assertRaisesRegex(ValueError, "wheel"):
                smoke.verify_wheel(wheel, site, ("jiuwenbox/",))

    def test_green_requires_all_cases_and_empty_physical_inventory(self):
        with tempfile.TemporaryDirectory() as directory:
            run = Path(directory)
            (run / "runtime").mkdir()
            cases = [{"case": name, "status": "passed"} for name in smoke.EXPECTED_CASES]
            result = {"status": "passed", "cases": cases}
            (run / "results.json").write_text(json.dumps(result))
            (run / "host-audit.json").write_text(json.dumps({"leftovers": [], "conch_bridge_ports": []}))
            (run / "runtime/audit.json").write_text(json.dumps({"leftovers": []}))
            smoke.verify_results(run)
            result["cases"] = cases[:-1]
            (run / "results.json").write_text(json.dumps(result))
            with self.assertRaisesRegex(ValueError, "cases"):
                smoke.verify_results(run)
            result["cases"] = cases
            (run / "results.json").write_text(json.dumps(result))
            (run / "host-audit.json").write_text(
                json.dumps({"leftovers": ["orphan"], "conch_bridge_ports": []})
            )
            with self.assertRaisesRegex(ValueError, "cleanup"):
                smoke.verify_results(run)

    def test_shared_mounts_are_retained_but_credentials_are_not_exported(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            run = root / "run"
            run.mkdir()
            public = root / "public"
            public.mkdir()
            secret = b"secret-value-12345678901234567890"
            (run / "api-key").write_bytes(secret)
            (run / "deployment.yaml").write_bytes(secret)
            (run / "sdk-client.json").write_bytes(secret)
            (run / "tls").mkdir()
            (run / "tls/ca.key").write_bytes(b"PRIVATE KEY")
            (run / "runtime").mkdir()
            (run / "runtime/config.json").write_bytes(secret)
            (run / "service.log").write_bytes(b"failed with " + secret)
            (run / "host-audit.json").write_text('{"retained_bootstrap_loops":["loop2"],"leftovers":[]}')
            smoke.export_evidence(run, public)
            self.assertNotIn(secret, (public / "service.log").read_bytes())
            self.assertTrue((public / "host-audit.json").exists())
            self.assertFalse((public / "sdk-client.json").exists())
            self.assertFalse((public / "deployment.yaml").exists())
            self.assertFalse((public / "runtime/config.json").exists())
            self.assertFalse((public / "tls").exists())

    def test_teardown_only_unmounts_its_private_fixture(self):
        from types import SimpleNamespace
        from unittest.mock import patch

        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            private = root / "private"
            image = private / "package-sdk/runtime/adx-runtime-rootfs.img"
            image.parent.mkdir(parents=True)
            image.write_bytes(b"fixture")
            protected = root / "protected"
            protected.write_text("keep")
            target = private / "runs/smoke/runtime/state/bootstrap" / smoke.sha(image)
            listing = {"filesystems": [{"target": str(target), "fstype": "erofs"}]}
            with (
                patch.object(smoke.shutil, "which", side_effect=lambda tool: f"/tools/{tool}"),
                patch.object(
                    smoke.subprocess, "run", return_value=SimpleNamespace(stdout=json.dumps(listing))
                ) as command,
            ):
                result = smoke.cleanup_fixture(private)
            self.assertTrue(result["fixture_mount_unmounted"])
            self.assertFalse(private.exists())
            self.assertEqual(protected.read_text(), "keep")
            self.assertEqual(command.call_args_list[1].args[0], ["/tools/umount", "--", str(target)])

    def test_teardown_refuses_an_unexpected_mount(self):
        from types import SimpleNamespace
        from unittest.mock import patch

        with tempfile.TemporaryDirectory() as directory:
            private = Path(directory) / "private"
            image = private / "package-sdk/runtime/adx-runtime-rootfs.img"
            image.parent.mkdir(parents=True)
            image.write_bytes(b"fixture")
            listing = {"filesystems": [{"target": "/other-node/bootstrap", "fstype": "erofs"}]}
            with (
                patch.object(smoke.shutil, "which", side_effect=lambda tool: f"/tools/{tool}"),
                patch.object(
                    smoke.subprocess, "run", return_value=SimpleNamespace(stdout=json.dumps(listing))
                ) as command,
            ):
                with self.assertRaisesRegex(ValueError, "unexpected fixture mount"):
                    smoke.cleanup_fixture(private)
            self.assertEqual(command.call_count, 1)
            self.assertTrue(private.exists())

    def test_host_preflight_accepts_both_backend_cgroup_layouts(self):
        from unittest.mock import patch

        with tempfile.TemporaryDirectory() as directory:
            cgroup = Path(directory)
            with (
                patch.object(smoke.sys, "platform", "linux"),
                patch.object(smoke.platform, "machine", return_value="x86_64"),
                patch.object(smoke.os, "geteuid", return_value=0),
                patch.object(smoke.shutil, "which", return_value="/tool"),
                patch.object(smoke, "Path", return_value=cgroup),
            ):
                with self.assertRaisesRegex(ValueError, "cgroup"):
                    smoke.preflight()
                for controller in ("memory", "cpu", "pids"):
                    (cgroup / controller).mkdir()
                smoke.preflight()
                for controller in ("memory", "cpu", "pids"):
                    (cgroup / controller).rmdir()
                (cgroup / "cgroup.controllers").write_text("memory cpu pids")
                smoke.preflight()

    def test_native_audit_detects_v1_orphan_and_clears_after_removal(self):
        import sys
        from unittest.mock import MagicMock, patch

        module_spec = importlib.util.spec_from_file_location(
            "accept_fixture", ROOT / "build/e2e/jiuwenbox/accept.py"
        )
        backend = importlib.util.module_from_spec(module_spec)
        with patch.dict(sys.modules, {"httpx": MagicMock(), "yaml": MagicMock()}):
            module_spec.loader.exec_module(backend)
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            empty = root / "empty"
            empty.mkdir()
            roots = backend.cgroup_roots(root / "cgroup")
            orphan = roots[1] / "adx-orphan"
            orphan.mkdir(parents=True)
            acceptance = backend.Acceptance.__new__(backend.Acceptance)
            acceptance.root, acceptance.run, acceptance.backends = root, root, ("supervisor",)
            with (
                patch.object(backend, "cgroup_roots", return_value=roots),
                patch.object(
                    backend, "Path", side_effect=lambda p: empty if p in ("/proc", "/run/netns") else Path(p)
                ),
                patch.object(backend, "command", return_value=""),
            ):
                self.assertEqual(acceptance.host_audit()["leftovers"], [str(orphan)])
                orphan.rmdir()
                self.assertEqual(acceptance.host_audit()["leftovers"], [])


class JiuwenboxArtifactTests(unittest.TestCase):
    @staticmethod
    def loader():
        module_spec = importlib.util.spec_from_file_location(
            "prepare_supervisor", ROOT / "build/e2e/jiuwenbox/prepare_supervisor.py"
        )
        module = importlib.util.module_from_spec(module_spec)
        module_spec.loader.exec_module(module)
        return module

    def test_extracts_only_the_pinned_wheel_and_rejects_tampering(self):
        import hashlib
        import io
        import tarfile

        prepare = self.loader()
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            artifact = {
                "file": "jiuwenswarm.whl",
                "size": 8,
                "sha256": hashlib.sha256(b"original").hexdigest(),
            }
            archive = root / "archive.tar.gz"
            with tarfile.open(archive, "w:gz") as bundle:
                member = tarfile.TarInfo("archive/jiuwenswarm.whl")
                member.size = 8
                bundle.addfile(member, io.BytesIO(b"original"))
            wheel = root / "wheel"
            prepare.extract_wheel(archive, wheel, artifact)
            self.assertEqual(wheel.read_bytes(), b"original")
            wheel.write_bytes(b"modified")
            with self.assertRaisesRegex(ValueError, "pinned artifact"):
                prepare.verify(wheel, artifact)

    def test_duplicate_wheel_members_are_rejected(self):
        import hashlib
        import io
        import tarfile

        prepare = self.loader()
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            artifact = {"file": "jiuwenswarm.whl", "size": 1, "sha256": hashlib.sha256(b"x").hexdigest()}
            archive = root / "archive.tar.gz"
            with tarfile.open(archive, "w:gz") as bundle:
                for name in ("one/jiuwenswarm.whl", "two/jiuwenswarm.whl"):
                    member = tarfile.TarInfo(name)
                    member.size = 1
                    bundle.addfile(member, io.BytesIO(b"x"))
            with self.assertRaisesRegex(ValueError, "exactly one"):
                prepare.extract_wheel(archive, root / "wheel", artifact)


if __name__ == "__main__":
    unittest.main()
