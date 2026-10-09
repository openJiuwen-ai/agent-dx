#!/usr/bin/env python3
import errno
import importlib.util
import json
import platform
import subprocess
import sys
import time
import unittest
from pathlib import Path


PROBE = Path(__file__).resolve().parent / "probes" / "locks_smoke.py"


def load_probe_module():
    spec = importlib.util.spec_from_file_location("locks_smoke_for_test", PROBE)
    module = importlib.util.module_from_spec(spec)
    assert spec.loader is not None
    spec.loader.exec_module(module)
    return module


@unittest.skipUnless(platform.system() == "Linux", "locks_smoke guard tests run only on Linux")
class LocksSmokeGuardTests(unittest.TestCase):
    def test_conflict_errno_predicates_reject_unsupported_results(self):
        probe = load_probe_module()
        self.assertTrue(probe._fcntl_conflict_errno_ok(errno.EACCES))
        self.assertTrue(probe._fcntl_conflict_errno_ok(errno.EAGAIN))
        self.assertFalse(probe._fcntl_conflict_errno_ok(errno.ENOLCK))
        self.assertFalse(probe._fcntl_conflict_errno_ok(errno.ENOSYS))
        self.assertFalse(probe._fcntl_conflict_errno_ok(errno.EOPNOTSUPP))
        self.assertTrue(probe._flock_conflict_errno_ok(errno.EAGAIN))
        self.assertFalse(probe._flock_conflict_errno_ok(errno.ENOLCK))
        self.assertFalse(probe._flock_conflict_errno_ok(errno.ENOSYS))
        self.assertFalse(probe._flock_conflict_errno_ok(errno.EOPNOTSUPP))

    def test_read_event_timeout_is_bounded_without_output(self):
        probe = load_probe_module()
        proc = subprocess.Popen(
            [sys.executable, "-c", "import time; time.sleep(2)"],
            text=True,
            stdin=subprocess.PIPE,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
        )
        child = probe.Child(proc, "silent", 0.1)
        started = time.monotonic()
        try:
            with self.assertRaises(TimeoutError):
                child.read_event("READY")
            self.assertLess(time.monotonic() - started, 1.0)
        finally:
            child.proc.kill()
            child.wait()

    def test_no_event_guard_rejects_alive_child_that_already_acquired(self):
        probe = load_probe_module()
        child_code = (
            "import json, sys\n"
            "print(json.dumps({'event': 'ACQUIRED', 'pid': 123}), flush=True)\n"
            "sys.stdin.readline()\n"
        )
        proc = subprocess.Popen(
            [sys.executable, "-c", child_code],
            text=True,
            stdin=subprocess.PIPE,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
        )
        child = probe.Child(proc, "early-acquired", 1.0)
        try:
            with self.assertRaises(AssertionError) as context:
                child.assert_no_event("ACQUIRED", 0.5)
            self.assertIn("too early", str(context.exception))
            self.assertEqual(child.events[0]["event"], "ACQUIRED")
        finally:
            child.release()

    def test_no_event_guard_rejects_acquired_buffered_after_waiting(self):
        probe = load_probe_module()
        child_code = (
            "import os, sys\n"
            "os.write(sys.stdout.fileno(), b'{\"event\":\"WAITING\"}\\n{\"event\":\"ACQUIRED\"}\\n')\n"
            "sys.stdin.readline()\n"
        )
        proc = subprocess.Popen(
            [sys.executable, "-c", child_code],
            text=True,
            stdin=subprocess.PIPE,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
        )
        child = probe.Child(proc, "buffered-acquired", 1.0)
        try:
            self.assertEqual(child.read_event("WAITING")["event"], "WAITING")
            with self.assertRaises(AssertionError) as context:
                child.assert_no_event("ACQUIRED", 0.5)
            self.assertIn("too early", str(context.exception))
        finally:
            child.release()

    def test_partial_line_without_newline_times_out_without_blocking(self):
        probe = load_probe_module()
        child_code = (
            "import os, sys, time\n"
            "os.write(sys.stdout.fileno(), b'{\"event\":\"WAITING\"')\n"
            "time.sleep(2)\n"
        )
        proc = subprocess.Popen(
            [sys.executable, "-c", child_code],
            text=True,
            stdin=subprocess.PIPE,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
        )
        child = probe.Child(proc, "partial-line", 0.1)
        started = time.monotonic()
        try:
            with self.assertRaises(TimeoutError):
                child.read_event("WAITING")
            self.assertLess(time.monotonic() - started, 1.0)
        finally:
            child.proc.kill()
            child.wait()

    def test_no_event_guard_rejects_child_exit_before_blocking_window(self):
        probe = load_probe_module()
        child_code = "import json; print(json.dumps({'event': 'WAITING'}), flush=True)"
        proc = subprocess.Popen(
            [sys.executable, "-c", child_code],
            text=True,
            stdin=subprocess.PIPE,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
        )
        child = probe.Child(proc, "early-exit", 1.0)
        self.assertEqual(child.read_event("WAITING")["event"], "WAITING")
        with self.assertRaises(AssertionError) as context:
            child.assert_no_event("ACQUIRED", 0.5)
        details = json.loads(str(context.exception))
        self.assertIn("exited before", details["message"])
        child.wait()


if __name__ == "__main__":
    unittest.main()
