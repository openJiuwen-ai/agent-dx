"""Local checks preserve failures and their diagnostic evidence."""
import importlib.util
import json
import os
from pathlib import Path
import sys
import tempfile
import unittest
from unittest import mock

SCRIPT = Path(__file__).resolve().parents[1] / "run.py"


class RunnerTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        spec = importlib.util.spec_from_file_location("ci_runner", SCRIPT)
        cls.runner = importlib.util.module_from_spec(spec)
        spec.loader.exec_module(cls.runner)

    def execute(self, commands, timeout=5):
        temp = tempfile.TemporaryDirectory()
        self.addCleanup(temp.cleanup)
        output = Path(temp.name)
        code = self.runner.execute(commands, output, timeout, {"suite": "fixture"})
        return code, json.loads((output / "result.json").read_text()), output

    def test_failure_stops_following_commands_and_preserves_exit_code(self):
        code, report, output = self.execute([
            [sys.executable, "-c", "print('failure evidence'); raise SystemExit(7)"],
            [sys.executable, "-c", "raise SystemExit(0)"],
        ])
        self.assertEqual(code, 7)
        self.assertEqual(report["status"], "failed")
        self.assertEqual(len(report["commands"]), 1)
        self.assertIn("failure evidence", (output / "01.log").read_text())

    def test_missing_executable_is_failure_with_report(self):
        code, report, _ = self.execute([["/nonexistent/adx-ci-tool"]])
        self.assertEqual(code, 127)
        self.assertEqual(report["status"], "failed")

    def test_timeout_is_failure(self):
        code, report, _ = self.execute([[sys.executable, "-c", "import time; time.sleep(30)"]], .1)
        self.assertEqual(code, 124)
        self.assertTrue(report["commands"][0]["timed_out"])

    def test_success_reports_all_commands(self):
        code, report, _ = self.execute([[sys.executable, "-c", "print('ok')"]])
        self.assertEqual(code, 0)
        self.assertEqual(report["status"], "passed")

    def test_reusing_output_directory_does_not_overwrite_evidence(self):
        with tempfile.TemporaryDirectory() as temp:
            output = Path(temp)
            (output / "result.json").write_text("previous")
            with self.assertRaises(FileExistsError):
                self.runner.execute([[sys.executable, "-c", "print('new')"]], output, 5, {})
            self.assertEqual((output / "result.json").read_text(), "previous")

    def test_unknown_suite_is_not_a_successful_empty_run(self):
        with self.assertRaises(ValueError):
            self.runner.commands_for("control-plane-e2e", Path("/tmp/report"), 2)

    def test_empty_suite_is_rejected(self):
        with self.assertRaises(ValueError):
            self.execute([])

    def test_explicit_cargo_target_is_preserved(self):
        previous = os.environ.get("CARGO_TARGET_DIR")
        os.environ["CARGO_TARGET_DIR"] = "/tmp/explicit-cargo-target"
        try:
            self.runner.configure_local_cargo_cache()
            self.assertEqual(os.environ["CARGO_TARGET_DIR"], "/tmp/explicit-cargo-target")
        finally:
            if previous is None:
                os.environ.pop("CARGO_TARGET_DIR", None)
            else:
                os.environ["CARGO_TARGET_DIR"] = previous

    def test_local_runner_loads_shared_cache_environment(self):
        keys = ("CARGO_TARGET_DIR", "CARGO_INCREMENTAL", "SCCACHE_DIR", "BUILDKITE")
        previous = {key: os.environ.get(key) for key in keys}
        for key in keys:
            os.environ.pop(key, None)
        values = {
            "CARGO_TARGET_DIR": "/tmp/shared-cargo-target",
            "CARGO_INCREMENTAL": "0",
            "SCCACHE_DIR": "/tmp/shared-sccache",
        }
        try:
            with mock.patch.object(self.runner.subprocess, "check_output", return_value=json.dumps(values)):
                self.runner.configure_local_cargo_cache()
            for key, value in values.items():
                self.assertEqual(os.environ[key], value)
        finally:
            for key in keys:
                if previous[key] is None:
                    os.environ.pop(key, None)
                else:
                    os.environ[key] = previous[key]


if __name__ == "__main__":
    unittest.main()
