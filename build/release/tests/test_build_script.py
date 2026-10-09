import os
import subprocess
import tempfile
import unittest
from pathlib import Path

ROOT = Path(__file__).resolve().parents[3]
SCRIPT = ROOT / "build/release/build.sh"


class BuildScriptPreflightTests(unittest.TestCase):
    def write_tool(self, root: Path, name: str, body: str) -> Path:
        path = root / name
        path.write_text("#!/usr/bin/env bash\nset -euo pipefail\n" + body)
        path.chmod(0o755)
        return path

    def write_redis(self, root: Path, name: str, version: str = "7.2.5") -> Path:
        output = f"Redis server v={version} sha=00000000:0 malloc=libc bits=64 build=test" if name == "redis-server" else f"redis-cli {version}"
        return self.write_tool(root, name, f"echo '{output}'\n")

    def prepare_erofs_cache(self, root: Path) -> Path:
        erofs = root / "erofs-cache" / "erofs-utils-1.8.10" / "bin"
        erofs.mkdir(parents=True)
        self.write_tool(erofs, "mkfs.erofs", "echo 'mkfs.erofs 1.8.10'\n")
        self.write_tool(erofs, "fsck.erofs", "echo 'fsck.erofs 1.8.10'\n")
        return erofs.parent.parent

    def run_build(self, tmp: Path, *, redis_version="7.2.5", rustup_installed=True, erofs_cache=True, python_ok=True):
        bin_dir = tmp / "bin"
        bin_dir.mkdir()
        cargo_marker = tmp / "cargo-called"
        self.write_tool(bin_dir, "rustc", "printf 'host: x86_64-unknown-linux-gnu\\n'\n")
        rustup_body = "echo 'x86_64-unknown-linux-musl'\n" if rustup_installed else "true\n"
        self.write_tool(bin_dir, "rustup", rustup_body)
        self.write_tool(bin_dir, "cargo", f"echo cargo >> {cargo_marker}\nexit 88\n")
        self.write_tool(bin_dir, "curl", "echo unexpected curl >&2\nexit 43\n")
        py_body = "cat >/dev/null\n" if python_ok else "cat >/dev/null\necho python missing build backend >&2\nexit 2\n"
        python = self.write_tool(bin_dir, "python-fixture", py_body)
        redis_server = self.write_redis(tmp, "redis-server", redis_version)
        redis_cli = self.write_redis(tmp, "redis-cli", redis_version)
        cache = self.prepare_erofs_cache(tmp) if erofs_cache else tmp / "missing-erofs-cache"
        env = dict(
            os.environ,
            PATH=str(bin_dir) + os.pathsep + os.environ["PATH"],
            CARGO_TARGET_DIR=str(tmp / "target"),
            ADX_REDIS_SERVER=str(redis_server),
            ADX_REDIS_CLI=str(redis_cli),
            ADX_RELEASE_TARGET="x86_64-unknown-linux-gnu",
            ADX_RELEASE_OUTPUT=str(tmp / "release"),
            ADX_EROFS_CACHE=str(cache),
            PYTHON=str(python),
        )
        result = subprocess.run(["bash", str(SCRIPT)], cwd=ROOT, env=env, capture_output=True, text=True)
        return result, cargo_marker

    def test_invalid_redis_version_stops_before_cargo(self):
        for version in ("7.2.4", "7.2.50"):
            with self.subTest(version=version), tempfile.TemporaryDirectory() as directory:
                result, marker = self.run_build(Path(directory), redis_version=version)
                self.assertEqual(result.returncode, 2, result.stderr)
                self.assertIn("Redis 7.2.5", result.stderr)
                self.assertFalse(marker.exists())

    def test_missing_musl_target_stops_before_cargo(self):
        with tempfile.TemporaryDirectory() as directory:
            result, marker = self.run_build(Path(directory), rustup_installed=False)
            self.assertEqual(result.returncode, 2, result.stderr)
            self.assertIn("missing Rust musl target", result.stderr)
            self.assertFalse(marker.exists())

    def test_missing_erofs_tools_stop_before_cargo(self):
        with tempfile.TemporaryDirectory() as directory:
            result, marker = self.run_build(Path(directory), erofs_cache=False)
            self.assertNotEqual(result.returncode, 0, result.stderr)
            self.assertIn("unexpected curl", result.stderr)
            self.assertFalse(marker.exists())

    def test_missing_python_build_backend_stops_before_cargo(self):
        with tempfile.TemporaryDirectory() as directory:
            result, marker = self.run_build(Path(directory), python_ok=False)
            self.assertEqual(result.returncode, 2, result.stderr)
            self.assertIn("python missing build backend", result.stderr)
            self.assertFalse(marker.exists())

    def test_complete_preflight_reaches_first_cargo_build(self):
        with tempfile.TemporaryDirectory() as directory:
            result, marker = self.run_build(Path(directory))
            self.assertEqual(result.returncode, 88, result.stderr)
            self.assertIn("--- :rust: Compile control plane", result.stdout)
            self.assertTrue(marker.exists())


if __name__ == "__main__":
    unittest.main()
