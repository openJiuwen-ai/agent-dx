"""Exercise only the optional component boundary added by the AFS import."""

import os
import shlex
import subprocess
import tempfile
import unittest
from pathlib import Path

ROOT = Path(__file__).resolve().parents[3]
SCRIPT = ROOT / "build/release/build.sh"


class AfsBuildBoundaryTests(unittest.TestCase):
    def run_build(self, flag, host="x86_64-unknown-linux-gnu", legacy=None,
                  hixl="0", stop_at_afs=False):
        with tempfile.TemporaryDirectory() as temp:
            root = Path(temp)
            tools = root / "tools"
            tools.mkdir()
            marker = root / "cargo-called"
            cargo_body = f"echo cargo >> '{marker}'\nexit 88\n"
            if stop_at_afs:
                cargo_body = (f"echo cargo >> '{marker}'\n"
                              "case \" $* \" in\n"
                              "  *\" -p afs \"*) printf 'AFS_COMMAND'; printf ' %s' \"$@\"; printf '\\n'; exit 88 ;;\n"
                              "  *) exit 0 ;;\n"
                              "esac\n")
                binaries = root / "target" / "release"
                binaries.mkdir(parents=True)
                for name in ("adx-activator", "adx-apiserver", "adx-ingress", "adx-relay", "adxctl",
                             "adx-inspect", "adx-coordinator", "adxlet", "adx-execd"):
                    (binaries / name).write_text("fixture")
            for name, body in {
                "rustc": f"printf 'host: {host}\\n'\n",
                "cargo": cargo_body,
            }.items():
                tool = tools / name
                tool.write_text("#!/bin/sh\n" + body)
                tool.chmod(0o755)
            env = dict(
                os.environ,
                PATH=str(tools) + os.pathsep + os.environ["PATH"],
                CARGO_TARGET_DIR=str(root / "target"),
                ADX_REDIS_SERVER=str(root / "redis-server"),
                ADX_REDIS_CLI=str(root / "redis-cli"),
                ADX_RELEASE_TARGET=host,
                ADX_RELEASE_OUTPUT=str(root / "output"),
                ADX_WITH_AFS=flag,
                ADX_AFS_WITH_HIXL=hixl,
            )
            if legacy is not None:
                env[legacy] = "1"
            result = subprocess.run(["bash", str(SCRIPT)], cwd=ROOT, env=env, capture_output=True, text=True)
            return result, marker.exists()

    def test_hixl_requires_explicit_afs_component_before_build(self):
        result, built = self.run_build("0", hixl="1")
        self.assertEqual(result.returncode, 2, result.stderr)
        self.assertIn("ADX_AFS_WITH_HIXL=1 requires ADX_WITH_AFS=1", result.stderr)
        self.assertFalse(built)

    def test_invalid_hixl_flag_stops_before_build(self):
        result, built = self.run_build("1", hixl="yes")
        self.assertEqual(result.returncode, 2, result.stderr)
        self.assertIn("ADX_AFS_WITH_HIXL must be 0 or 1", result.stderr)
        self.assertFalse(built)

    def test_hixl_feature_is_added_only_to_explicit_afs_build(self):
        for enabled in ("0", "1"):
            with self.subTest(hixl=enabled):
                result, built = self.run_build("1", hixl=enabled, stop_at_afs=True)
                self.assertEqual(result.returncode, 88, result.stderr)
                self.assertTrue(built)
                command = next(line for line in result.stdout.splitlines()
                               if line.startswith("AFS_COMMAND"))
                arguments = shlex.split(command)[1:]
                self.assertEqual("--features" in arguments, enabled == "1")
                if enabled == "1":
                    self.assertEqual(arguments[arguments.index("--features") + 1], "hixl")

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
