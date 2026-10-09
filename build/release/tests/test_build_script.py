"""Exercise only the optional component boundary added by the DFS import."""
import os
import subprocess
import tempfile
import unittest
from pathlib import Path

ROOT = Path(__file__).resolve().parents[3]
SCRIPT = ROOT / "build/release/build.sh"


class DfsBuildBoundaryTests(unittest.TestCase):
    def run_build(self, flag, host="x86_64-unknown-linux-gnu"):
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
                       ADX_WITH_DFS=flag)
            result = subprocess.run(["bash", str(SCRIPT)], cwd=ROOT, env=env,
                                    capture_output=True, text=True)
            return result, marker.exists()

    def test_invalid_component_flag_stops_before_build(self):
        result, built = self.run_build("yes")
        self.assertEqual(result.returncode, 2)
        self.assertIn("ADX_WITH_DFS must be 0 or 1", result.stderr)
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
