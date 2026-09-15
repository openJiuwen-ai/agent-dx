"""Local checks preserve failures and their diagnostic evidence."""
import importlib.util
import json
from pathlib import Path
import sys
import tempfile
import unittest

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


if __name__ == "__main__":
    unittest.main()
