#!/usr/bin/env python3
import importlib.util
import json
import os
import platform
import subprocess
import sys
import tempfile
import time
import unittest
from pathlib import Path


PROBE = Path(__file__).resolve().parent / "probes" / "owner_restart.py"


def load_probe_module():
    spec = importlib.util.spec_from_file_location("owner_restart_for_test", PROBE)
    module = importlib.util.module_from_spec(spec)
    assert spec.loader is not None
    spec.loader.exec_module(module)
    return module


def ext4_or_skip(testcase: unittest.TestCase, path: Path) -> None:
    module = load_probe_module()
    fstype = module._mount_fstype(path)
    if fstype != "ext4":
        testcase.skipTest(f"owner restart protocol selftest requires ext4, got {fstype}")


class OwnerRestartHelpTests(unittest.TestCase):
    def test_help_available_without_linux_runtime(self):
        result = subprocess.run([sys.executable, str(PROBE), "--help"], text=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE, timeout=10)
        self.assertEqual(result.returncode, 0, result.stderr)
        self.assertIn("writer", result.stdout)
        self.assertIn("check", result.stdout)


@unittest.skipUnless(platform.system() == "Linux", "owner restart probe selftests run only on Linux")
class OwnerRestartProtocolTests(unittest.TestCase):
    def run_writer(self, root: Path, target: Path):
        ready = root / "ready.json"
        trigger = root / "trigger"
        result = root / "writer-result.json"
        proc = subprocess.Popen(
            [
                sys.executable,
                str(PROBE),
                "writer",
                "--target",
                str(target),
                "--state-dir",
                str(root),
                "--ready-file",
                str(ready),
                "--trigger-file",
                str(trigger),
                "--result-file",
                str(result),
                "--payload-seed",
                "owner-restart-unittest",
                "--trigger-timeout",
                "10",
                "--close-deadline",
                "5",
            ],
            text=True,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
        )
        deadline = time.monotonic() + 5
        while not ready.exists() and time.monotonic() < deadline:
            time.sleep(0.05)
        self.assertTrue(ready.exists(), "writer did not create READY")
        trigger.write_text("close\n", encoding="utf-8")
        stdout, stderr = proc.communicate(timeout=10)
        self.assertEqual(proc.returncode, 0, stderr + stdout)
        self.assertTrue(result.exists())
        return ready, result

    def test_writer_and_checker_preserve_payload_on_ext4_regular_file(self):
        with tempfile.TemporaryDirectory() as temp_dir:
            root = Path(temp_dir)
            ext4_or_skip(self, root)
            target = root / "target.bin"
            target.write_bytes(b"existing-from-node-a\n")
            ready, writer_result = self.run_writer(root, target)
            ready_json = json.loads(ready.read_text(encoding="utf-8"))
            writer_json = json.loads(writer_result.read_text(encoding="utf-8"))
            self.assertEqual(ready_json["phase"], "READY")
            self.assertEqual(ready_json["durable_watermark"]["length"], 64 * 1024)
            self.assertEqual(ready_json["payload"]["sha256"], ready_json["durable_watermark"]["read_own_fd_sha256"])
            self.assertEqual(writer_json["status"], "CLOSE_OK")
            check_result = root / "check-result.json"
            result = subprocess.run(
                [
                    sys.executable,
                    str(PROBE),
                    "check",
                    "--target",
                    str(target),
                    "--ready-file",
                    str(ready),
                    "--result-file",
                    str(check_result),
                    "--open-timeout",
                    "5",
                ],
                text=True,
                stdout=subprocess.PIPE,
                stderr=subprocess.PIPE,
                timeout=10,
            )
            self.assertEqual(result.returncode, 0, result.stderr + result.stdout)
            check_json = json.loads(check_result.read_text(encoding="utf-8"))
            self.assertEqual(check_json["status"], "PASS")
            self.assertEqual(check_json["actual"]["length"], 64 * 1024)

    def test_checker_reports_first_open_error_without_retry(self):
        with tempfile.TemporaryDirectory() as temp_dir:
            root = Path(temp_dir)
            ext4_or_skip(self, root)
            ready = root / "ready.json"
            ready.write_text(json.dumps({"payload": {"size": 3, "sha256": "unused"}}))
            output = root / "read-error.json"
            started = time.monotonic()
            result = subprocess.run([
                sys.executable, str(PROBE), "check", "--target", str(root / "missing"),
                "--ready-file", str(ready), "--result-file", str(output), "--open-timeout", "5",
            ], text=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE, timeout=10)
            self.assertEqual(result.returncode, 1, result.stderr)
            self.assertLess(time.monotonic() - started, 2)
            report = json.loads(output.read_text())
            self.assertEqual(report["status"], "READ_ERROR")
            self.assertEqual(len(report["attempts"]), 1)
            self.assertEqual(report["attempts"][0]["errno_name"], "ENOENT")

    def test_checker_fails_immediately_on_stale_or_wrong_bytes(self):
        with tempfile.TemporaryDirectory() as temp_dir:
            root = Path(temp_dir)
            ext4_or_skip(self, root)
            target = root / "target.bin"
            target.write_bytes(b"existing-from-node-a\n")
            ready, _writer_result = self.run_writer(root, target)
            target.write_bytes(b"stale")
            check_result = root / "check-mismatch.json"
            started = time.monotonic()
            result = subprocess.run(
                [
                    sys.executable,
                    str(PROBE),
                    "check",
                    "--target",
                    str(target),
                    "--ready-file",
                    str(ready),
                    "--result-file",
                    str(check_result),
                    "--open-timeout",
                    "5",
                ],
                text=True,
                stdout=subprocess.PIPE,
                stderr=subprocess.PIPE,
                timeout=10,
            )
            elapsed = time.monotonic() - started
            self.assertEqual(result.returncode, 1, result.stderr + result.stdout)
            self.assertLess(elapsed, 2.0)
            check_json = json.loads(check_result.read_text(encoding="utf-8"))
            self.assertEqual(check_json["status"], "MISMATCH")
            self.assertEqual(check_json["actual"]["length"], len(b"stale"))


if __name__ == "__main__":
    unittest.main()
