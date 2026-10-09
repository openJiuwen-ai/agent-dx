"""Exercise only the optional component boundary added by the AFS import."""
import os
import subprocess
import tempfile
import unittest
from pathlib import Path

ROOT = Path(__file__).resolve().parents[3]
SCRIPT = ROOT / "build/release/build.sh"


class AfsBuildBoundaryTests(unittest.TestCase):
    def run_build(self, flag, host="x86_64-unknown-linux-gnu", legacy=None):
        with tempfile.TemporaryDirectory() as temp:
            root = Path(temp)
            tools = root / "tools"
            tools.mkdir()
            marker = root / "cargo-called"
            for name, body in {
                "rustc": f"printf 'host: {host}\\n'\n",
                "cargo": f"echo cargo >> '{marker}'\nexit 88\n",
            }.items():
                tool = tools / name
                tool.write_text("#!/bin/sh\n" + body)
                tool.chmod(0o755)
            env = dict(os.environ, PATH=str(tools) + os.pathsep + os.environ["PATH"],
                       CARGO_TARGET_DIR=str(root / "target"),
                       ADX_REDIS_SERVER=str(root / "redis-server"),
                       ADX_REDIS_CLI=str(root / "redis-cli"),
                       ADX_RELEASE_TARGET=host, ADX_RELEASE_OUTPUT=str(root / "output"),
                       ADX_WITH_AFS=flag)
            if legacy is not None:
                env[legacy] = "1"
            result = subprocess.run(["bash", str(SCRIPT)], cwd=ROOT, env=env,
                                    capture_output=True, text=True)
            return result, marker.exists()

    def test_invalid_component_flag_stops_before_build(self):
        result, built = self.run_build("yes")
        self.assertEqual(result.returncode, 2)
        self.assertIn("ADX_WITH_AFS must be 0 or 1", result.stderr)
        self.assertFalse(built)

    def test_legacy_flags_are_rejected_before_build(self):
        for legacy in ("ADX_WITH_DFS", "ADX_DFS_ALL_FEATURES"):
            with self.subTest(legacy=legacy):
                result, built = self.run_build("1", legacy=legacy)
                self.assertEqual(result.returncode, 2)
                self.assertIn(legacy + " was replaced by", result.stderr)
                self.assertFalse(built)

    def test_on_rejects_non_linux_builder_before_build(self):
        result, built = self.run_build("1", "aarch64-apple-darwin")
        self.assertEqual(result.returncode, 2)
        self.assertIn("requires a Linux release builder", result.stderr)
        self.assertFalse(built)

    def test_default_off_keeps_original_first_build_entry(self):
        result, built = self.run_build("0")
        self.assertEqual(result.returncode, 88, result.stderr)
        self.assertTrue(built)


if __name__ == "__main__":
    unittest.main()
