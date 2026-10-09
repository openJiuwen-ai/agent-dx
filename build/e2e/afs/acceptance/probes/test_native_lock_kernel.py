#!/usr/bin/env python3
"""Guard tests for native_lock_kernel.py.

These tests are Linux-only because they exercise real fcntl ABI shape and a
short ext4 primitive run.  They intentionally assert PRIMITIVE_PASS wording so
future callers do not mistake the probe for product or full POSIX evidence.
"""
from __future__ import annotations

import importlib.util
import json
import os
import platform
import shutil
import subprocess
import sys
import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch


PROBE = Path(__file__).resolve().with_name("native_lock_kernel.py")


def load_probe_module():
    spec = importlib.util.spec_from_file_location("native_lock_kernel_for_test", PROBE)
    module = importlib.util.module_from_spec(spec)
    assert spec.loader is not None
    spec.loader.exec_module(module)
    return module


@unittest.skipUnless(platform.system() == "Linux", "native_lock_kernel tests run only on Linux")
class NativeLockKernelTests(unittest.TestCase):
    def test_conflict_errno_rejects_unsupported_results(self) -> None:
        probe = load_probe_module()
        self.assertTrue(probe.conflict_errno_ok(11))
        self.assertTrue(probe.conflict_errno_ok(13))
        self.assertFalse(probe.conflict_errno_ok(37))
        self.assertFalse(probe.conflict_errno_ok(38))
        self.assertFalse(probe.conflict_errno_ok(95))
        self.assertTrue(probe.unsupported_errno(37))
        self.assertTrue(probe.unsupported_errno(38))
        self.assertTrue(probe.unsupported_errno(95))

    def test_missing_ofd_abi_is_rejected(self) -> None:
        probe = load_probe_module()
        with tempfile.TemporaryDirectory() as td:
            work = Path(td).resolve()
            out = work / "out"
            with patch.object(probe.platform, "machine", return_value="aarch64"), \
                    patch.object(probe.os, "geteuid", return_value=0), \
                    patch.object(probe, "F_OFD_GETLK", None):
                with self.assertRaisesRegex(RuntimeError, "missing F_OFD"):
                    probe.run_probe(work, out)

    def test_existing_output_is_rejected_and_preserved(self) -> None:
        probe = load_probe_module()
        with tempfile.TemporaryDirectory() as td:
            work = Path(td).resolve()
            out = work / "out"
            out.mkdir()
            sentinel = out / "sentinel.txt"
            sentinel.write_text("keep\n", encoding="utf-8")
            with patch.object(probe.platform, "machine", return_value="aarch64"), \
                    patch.object(probe.os, "geteuid", return_value=0):
                with self.assertRaisesRegex(RuntimeError, "fresh"):
                    probe.run_probe(work, out)
            self.assertEqual(sentinel.read_text(encoding="utf-8"), "keep\n")

    def test_other_architecture_is_rejected_before_creating_output(self) -> None:
        probe = load_probe_module()
        with tempfile.TemporaryDirectory() as td:
            work = Path(td).resolve()
            out = work / "out"
            with patch.object(probe.platform, "machine", return_value="x86_64"), \
                    patch.object(probe.subprocess, "check_output") as command:
                with self.assertRaisesRegex(RuntimeError, "requires aarch64"):
                    probe.run_probe(work, out)
                command.assert_not_called()
            self.assertEqual(list(work.iterdir()), [])

    def test_silent_child_timeout_is_bounded_and_killed(self) -> None:
        probe = load_probe_module()
        child = probe.Child([sys.executable, "-c", "import time; time.sleep(60)"], timeout=0.2)
        with self.assertRaises(TimeoutError) as context:
            child.read_event("RESULT")
        details = json.loads(str(context.exception))
        self.assertIn("timed out", details["message"])
        self.assertIsNotNone(details["child"]["returncode"])
        self.assertNotEqual(details["child"]["returncode"], 0)
        self.assertEqual(details["child"]["events"], [])
        self.assertIsNotNone(child.proc.poll())

    def test_child_result_followed_by_exit_one_is_rejected(self) -> None:
        probe = load_probe_module()
        fake = (
            "import json, sys\n"
            "print(json.dumps({'event':'RESULT','result':{'ok':True}}), flush=True)\n"
            "sys.exit(1)\n"
        )
        with patch.object(probe, "child_argv", return_value=[sys.executable, "-c", fake]):
            with self.assertRaisesRegex(RuntimeError, "child exited nonzero"):
                probe.run_child(Path("/tmp/unused"), "try", 1, 0, 1)

    def test_getlk_guard_requires_exact_type_range_and_pid(self) -> None:
        probe = load_probe_module()
        good = {"ok": True, "result": {"l_type_name": "F_WRLCK", "l_start": 0, "l_len": 100, "l_pid": -1}}
        probe.assert_lock_seen(good, "guard", lock_type="F_WRLCK", start=0, length=100, pid=-1)
        bad_range = {"ok": True, "result": {"l_type_name": "F_WRLCK", "l_start": 1, "l_len": 99, "l_pid": -1}}
        with self.assertRaisesRegex(RuntimeError, "exact start"):
            probe.assert_lock_seen(bad_range, "guard", lock_type="F_WRLCK", start=0, length=100, pid=-1)
        bad_pid = {"ok": True, "result": {"l_type_name": "F_WRLCK", "l_start": 0, "l_len": 100, "l_pid": 0}}
        with self.assertRaisesRegex(RuntimeError, "exact owner pid"):
            probe.assert_lock_seen(bad_pid, "guard", lock_type="F_WRLCK", start=0, length=100, pid=-1)

    @unittest.skipUnless(platform.machine() == "aarch64" and os.geteuid() == 0 and shutil.which("findmnt"), "full primitive requires root aarch64 Linux")
    def test_ext4_primitive_pass_records_raw_lock_facts(self) -> None:
        with tempfile.TemporaryDirectory(dir="/var/tmp") as td:
            root = Path(td)
            output = root / "out"
            cmd = [sys.executable, str(PROBE), "--work-dir", str(root), "--output", str(output)]
            result = subprocess.run(cmd, text=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE, timeout=30)
            self.assertEqual(result.returncode, 0, result.stderr + result.stdout)
            self.assertIn("PRIMITIVE_PASS", result.stdout)
            report = json.loads((output / "report.json").read_text(encoding="utf-8"))
            self.assertEqual(report["status"], "PRIMITIVE_PASS")
            self.assertTrue(report["primitive_interop"])
            self.assertFalse(report["transparent_classic_posix"])
            self.assertTrue(report["primitive_only"])
            self.assertTrue(report["not_product_pass"])
            self.assertTrue(any("l_pid=-1" in item for item in report["limits"]))
            names = {case["name"] for case in report["cases"]}
            self.assertIn("ofd_parent_vs_child_posix", names)
            self.assertIn("posix_owner_any_close_control", names)
            self.assertIn("ofd_dup_final_close", names)
            first = next(case for case in report["cases"] if case["name"] == "ofd_parent_vs_child_posix")
            getlk = first["payload"]["getlk"]["primary_event"]["result"]
            self.assertEqual(getlk["result"]["l_pid"], -1)
            child_identity = first["payload"]["try"]["primary_event"]["identity"]
            self.assertIsInstance(child_identity["pid"], int)
            self.assertGreater(child_identity["pid"], 1)


if __name__ == "__main__":
    unittest.main()
