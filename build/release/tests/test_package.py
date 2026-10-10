import json
import importlib.util
import platform
import subprocess
import sys
import tempfile
import unittest
from pathlib import Path

s = importlib.util.spec_from_file_location("package", Path(__file__).resolve().parents[1] / "package.py")
pkg = importlib.util.module_from_spec(s)
s.loader.exec_module(pkg)


class PackageTests(unittest.TestCase):
    def test_inspector_is_part_of_the_platform_release(self):
        self.assertIn("adx-inspect", pkg.BINARIES)

    def test_complete_package_verifies_and_tampering_fails(self):
        with tempfile.TemporaryDirectory() as t:
            root = Path(t)
            binaries = root / "bin"
            binaries.mkdir()
            for name in pkg.BINARIES + ("adx-execd",):
                (binaries / name).write_bytes(b"fixture")
            (binaries / "adxctl").write_text("#!/bin/sh\nexit 0\n")
            redis = root / "redis"
            redis.write_text("#!/bin/sh\necho 'Redis server v=7.2.5 sha=fixture'\n")
            redis.chmod(0o700)
            redis_cli = root / "redis-cli"
            redis_cli.write_text("#!/bin/sh\necho 'redis-cli 7.2.5'\n")
            redis_cli.chmod(0o700)
            wheel = root / "adx_sandbox-1-py3-none-any.whl"
            wheel.write_bytes(b"fixture wheel")
            out = root / "package"
            m = pkg.assemble(binaries, redis, redis_cli, wheel, out, "a" * 40, True, "test-fixture", "debug")
            self.assertTrue(m["dirty"])
            pkg.verify(out)
            self.assertIn("bin/redis-cli", m["files"])
            self.assertTrue((out / "bin/redis-cli").stat().st_mode & 0o111)
            self.assertTrue((out / "install.sh").stat().st_mode & 0o111)
            (out / "bin/adxctl").write_bytes(b"changed")
            with self.assertRaises(ValueError):
                pkg.verify(out)
            with self.assertRaises(ValueError):
                pkg.assemble(binaries, redis, redis_cli, wheel, out, "a" * 40, True, "test-fixture", "debug")

    def test_legacy_manifest_cannot_silently_disable_afs(self):
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            for mode in (True, False):
                (root / "manifest.json").write_text(json.dumps({
                    "schema_version": 1, "files": {}, "with_dfs": mode,
                }))
                with self.assertRaisesRegex(ValueError, "legacy with_dfs"):
                    pkg.verify(root)

    def test_package_ships_optional_split_process_binaries(self):
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            binaries = root / "bin"
            binaries.mkdir()
            shipped = {
                "adxctl",
                "adx-inspect",
                "adx-coordinator",
                "adxlet",
                "adx-apiserver",
                "adx-ingress",
                "adx-relay",
            }
            for name in shipped | {"adx-data-plane-forward", "adx-execd"}:
                (binaries / name).write_bytes(b"fixture")
            redis = root / "redis"
            redis.write_text("#!/bin/sh\necho 'Redis server v=7.2.5 sha=fixture'\n")
            redis.chmod(0o700)
            redis_cli = root / "redis-cli"
            redis_cli.write_text("#!/bin/sh\necho 'redis-cli 7.2.5'\n")
            redis_cli.chmod(0o700)
            wheel = root / "adx_sandbox-1-py3-none-any.whl"
            wheel.write_bytes(b"fixture")
            output = root / "package"
            manifest = pkg.assemble(binaries, redis, redis_cli, wheel, output, "a" * 40, True, "test-fixture", "debug")
            self.assertEqual({p.name for p in (output / "bin").iterdir()}, shipped | {"redis-server", "redis-cli"})
            self.assertNotIn("bin/adx-data-plane-forward", manifest["files"])
            self.assertNotIn("bin/afs-meta", manifest["files"])
            pkg.verify(output)

    def test_afs_artifacts_are_only_shipped_when_enabled(self):
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            binaries = root / "bin"
            binaries.mkdir()
            for name in pkg.BINARIES + pkg.AFS_BINARIES + ("adx-execd",):
                (binaries / name).write_bytes(b"fixture")
            redis = root / "redis"
            redis.write_text("#!/bin/sh\necho 'Redis server v=7.2.5 sha=fixture'\n")
            redis.chmod(0o700)
            redis_cli = root / "redis-cli"
            redis_cli.write_text("#!/bin/sh\necho 'redis-cli 7.2.5'\n")
            redis_cli.chmod(0o700)
            wheel = root / "adx_sandbox-1-py3-none-any.whl"
            wheel.write_bytes(b"fixture")

            default_output = root / "default-package"
            default_manifest = pkg.assemble(
                binaries, redis, redis_cli, wheel, default_output, "a" * 40, True, "test-fixture", "debug"
            )
            self.assertNotIn("with_afs", default_manifest)
            self.assertNotIn("bin/afs-meta", default_manifest["files"])
            self.assertFalse((default_output / "etc/examples/afs").exists())
            pkg.verify(default_output)

            afs_output = root / "afs-package"
            afs_manifest = pkg.assemble(
                binaries, redis, redis_cli, wheel, afs_output, "a" * 40, True, "test-fixture", "debug", True
            )
            self.assertTrue(afs_manifest["with_afs"])
            self.assertIn("bin/afs-meta", afs_manifest["files"])
            self.assertIn("bin/afs-node", afs_manifest["files"])
            self.assertIn("etc/examples/afs/meta.toml", afs_manifest["files"])
            self.assertIn("third_party/afs-source/LICENSE", afs_manifest["files"])
            self.assertIn("third_party/afs-source/NOTICE", afs_manifest["files"])
            pkg.verify(afs_output)

            (afs_output / "bin/afs-node").write_bytes(b"tampered")
            with self.assertRaises(ValueError):
                pkg.verify(afs_output)

    def test_cli_assemble_accepts_with_afs_flag(self):
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            binaries = root / "bin"
            binaries.mkdir()
            for name in pkg.BINARIES + pkg.AFS_BINARIES + ("adx-execd",):
                (binaries / name).write_bytes(b"fixture")
            redis = root / "redis"
            redis.write_text("#!/bin/sh\necho 'Redis server v=7.2.5 sha=fixture'\n")
            redis.chmod(0o700)
            redis_cli = root / "redis-cli"
            redis_cli.write_text("#!/bin/sh\necho 'redis-cli 7.2.5'\n")
            redis_cli.chmod(0o700)
            wheel = root / "adx_sandbox-1-py3-none-any.whl"
            wheel.write_bytes(b"fixture")
            output = root / "package"

            result = subprocess.run(
                [
                    sys.executable,
                    str(Path(__file__).resolve().parents[1] / "package.py"),
                    "assemble",
                    "--binary-dir",
                    str(binaries),
                    "--redis",
                    str(redis),
                    "--redis-cli",
                    str(redis_cli),
                    "--wheel",
                    str(wheel),
                    "--output",
                    str(output),
                    "--target",
                    "test-fixture",
                    "--profile",
                    "debug",
                    "--with-afs",
                ],
                text=True,
                capture_output=True,
            )

            self.assertEqual(result.returncode, 0, result.stderr)
            manifest = pkg.verify(output)
            self.assertTrue(manifest["with_afs"])
            self.assertIn("bin/afs-meta", manifest["files"])

    def test_afs_package_requires_both_runtime_binaries(self):
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            binaries = root / "bin"
            binaries.mkdir()
            for name in pkg.BINARIES + ("afs-meta", "adx-execd"):
                (binaries / name).write_bytes(b"fixture")
            redis = root / "redis"
            redis.write_text("#!/bin/sh\necho 'Redis server v=7.2.5 sha=fixture'\n")
            redis.chmod(0o700)
            redis_cli = root / "redis-cli"
            redis_cli.write_text("#!/bin/sh\necho 'redis-cli 7.2.5'\n")
            redis_cli.chmod(0o700)
            wheel = root / "adx_sandbox-1-py3-none-any.whl"
            wheel.write_bytes(b"fixture")
            with self.assertRaises(ValueError):
                pkg.assemble(
                    binaries,
                    redis,
                    redis_cli,
                    wheel,
                    root / "afs-package",
                    "a" * 40,
                    True,
                    "test-fixture",
                    "debug",
                    True,
                )

    def test_missing_artifact_never_creates_package(self):
        with tempfile.TemporaryDirectory() as t:
            root = Path(t)
            out = root / "package"
            with self.assertRaises(ValueError):
                pkg.assemble(
                    root,
                    root / "redis",
                    root / "redis-cli",
                    root / "adx_sandbox-1.whl",
                    out,
                    "a" * 40,
                    True,
                    "fixture",
                    "debug",
                )
            self.assertFalse(out.exists())

    @unittest.skipUnless(sys.platform.startswith("linux"), "installer targets Linux hosts")
    def test_installer_verifies_and_installs_without_overwriting_host_state(self):
        with tempfile.TemporaryDirectory() as t:
            root = Path(t)
            binaries = root / "bin"
            binaries.mkdir()
            for name in pkg.BINARIES + ("adx-execd",):
                (binaries / name).write_bytes(b"fixture")
            (binaries / "adxctl").write_text("#!/bin/sh\nexit 0\n")
            redis = root / "redis"
            redis.write_text("#!/bin/sh\necho 'Redis server v=7.2.5 sha=fixture'\n")
            redis.chmod(0o700)
            redis_cli = root / "redis-cli"
            redis_cli.write_text("#!/bin/sh\necho 'redis-cli 7.2.5'\n")
            redis_cli.chmod(0o700)
            wheel = root / "adx_sandbox-1-py3-none-any.whl"
            wheel.write_bytes(b"fixture wheel")
            package = root / "package"
            target = f"{platform.machine()}-unknown-linux-gnu"
            pkg.assemble(binaries, redis, redis_cli, wheel, package, "a" * 40, False, target, "debug")
            prefix = root / "opt/adx"
            bin_dir = root / "usr/local/bin"
            command = [str(package / "install.sh"), "--prefix", str(prefix), "--bin-dir", str(bin_dir)]
            bin_dir.mkdir(parents=True)
            unmanaged = bin_dir / "adxctl"
            unmanaged.write_text("unmanaged\n")
            self.assertNotEqual(subprocess.run(command, capture_output=True).returncode, 0)
            self.assertFalse(prefix.exists())
            unmanaged.unlink()
            result = subprocess.run(command, text=True, capture_output=True)
            self.assertEqual(result.returncode, 0, result.stderr)
            release = prefix / "releases" / ("a" * 40)
            self.assertEqual((prefix / "current").resolve(), release)
            self.assertEqual((bin_dir / "adxctl").resolve(), release / "bin/adxctl")
            self.assertEqual(
                subprocess.check_output([release / "bin/redis-cli", "--version"], text=True).strip(), "redis-cli 7.2.5"
            )
            self.assertEqual(subprocess.run([bin_dir / "adxctl"]).returncode, 0)
            pkg.verify(release)
            config = prefix / "config/deployment.yaml"
            config.write_text("user config\n")
            state = prefix / "data/user-state"
            state.write_text("user state\n")
            runtime = prefix / "run/supervisor.sock"
            runtime.write_text("runtime state\n")
            upgraded_package = root / "upgraded-package"
            pkg.assemble(binaries, redis, redis_cli, wheel, upgraded_package, "b" * 40, False, target, "debug")
            upgraded_command = [
                str(upgraded_package / "install.sh"),
                "--prefix",
                str(prefix),
                "--bin-dir",
                str(bin_dir),
            ]
            upgraded = subprocess.run(upgraded_command, text=True, capture_output=True)
            self.assertEqual(upgraded.returncode, 0, upgraded.stderr)
            upgraded_release = prefix / "releases" / ("b" * 40)
            self.assertEqual((prefix / "current").resolve(), upgraded_release)
            self.assertEqual((bin_dir / "adxctl").resolve(), upgraded_release / "bin/adxctl")
            pkg.verify(upgraded_release)
            self.assertTrue(release.is_dir())
            self.assertNotEqual(subprocess.run(upgraded_command, capture_output=True).returncode, 0)
            replaced = subprocess.run(upgraded_command + ["--replace"], text=True, capture_output=True)
            self.assertEqual(replaced.returncode, 0, replaced.stderr)
            self.assertEqual((prefix / "current").resolve(), upgraded_release)
            self.assertEqual(config.read_text(), "user config\n")
            self.assertEqual(state.read_text(), "user state\n")
            self.assertEqual(runtime.read_text(), "runtime state\n")


if __name__ == "__main__":
    unittest.main()
