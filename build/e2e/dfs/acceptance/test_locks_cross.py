#!/usr/bin/env python3
import errno
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


PROBE = Path(__file__).resolve().parent / "probes" / "locks_cross.py"


def load_probe_module():
    spec = importlib.util.spec_from_file_location("locks_cross_for_test", PROBE)
    module = importlib.util.module_from_spec(spec)
    assert spec.loader is not None
    spec.loader.exec_module(module)
    return module


@unittest.skipUnless(platform.system() == "Linux", "locks_cross selftests run only on Linux")
class LocksCrossTests(unittest.TestCase):
    def test_errno_predicates_reject_unsupported_and_capacity_errors(self):
        probe = load_probe_module()
        self.assertTrue(probe._fcntl_conflict_errno_ok(13))
        self.assertTrue(probe._fcntl_conflict_errno_ok(11))
        self.assertFalse(probe._fcntl_conflict_errno_ok(35))
        self.assertFalse(probe._fcntl_conflict_errno_ok(errno.ENOLCK))
        self.assertFalse(probe._fcntl_conflict_errno_ok(errno.ENOSYS))
        self.assertFalse(probe._fcntl_conflict_errno_ok(errno.EOPNOTSUPP))
        self.assertTrue(probe._flock_conflict_errno_ok(11))
        self.assertFalse(probe._flock_conflict_errno_ok(35))
        self.assertFalse(probe._flock_conflict_errno_ok(errno.ENOLCK))
        self.assertFalse(probe._flock_conflict_errno_ok(errno.ENOSYS))
        self.assertFalse(probe._flock_conflict_errno_ok(errno.EOPNOTSUPP))

    def test_getlk_predicate_prefers_worker_lock_type_name(self):
        probe = load_probe_module()
        self.assertTrue(probe._getlk_reports_conflict({"ok": True, "l_type_name": "F_WRLCK", "l_type": -1}))
        self.assertFalse(probe._getlk_reports_conflict({"ok": True, "l_type_name": "F_UNLCK", "l_type": -1}))
        self.assertFalse(probe._getlk_reports_conflict({"ok": False, "l_type_name": "F_WRLCK"}))
        self.assertFalse(probe._getlk_reports_conflict({"ok": True, "l_type_name": "UNKNOWN"}))
        self.assertFalse(probe._getlk_reports_conflict({"ok": True}))
        self.assertFalse(probe._getlk_reports_conflict({"ok": True, "l_type": 2}))
        self.assertTrue(probe._getlk_reports_conflict({"ok": True, "l_type": 1}))

    def test_worker_read_event_timeout_is_bounded_without_output(self):
        probe = load_probe_module()
        argv = [sys.executable, "-c", "import time; time.sleep(2)"]
        proc = subprocess.Popen(argv, text=True, stdin=subprocess.PIPE, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
        child = probe.WorkerProcess("silent", argv, proc, 0.1)
        started = time.monotonic()
        try:
            with self.assertRaises(TimeoutError):
                child.read_event("READY")
            self.assertLess(time.monotonic() - started, 1)
        finally:
            child.proc.kill()
            child.wait()

    def run_probe(self, temp: Path, *, extra=None, path_a=None, path_b=None):
        path_a = path_a or temp / "lock-target.bin"
        path_b = path_b or path_a
        evidence = temp / "evidence"
        worker = json.dumps([sys.executable, str(PROBE)])
        cmd = [
            sys.executable,
            str(PROBE),
            "host",
            "--worker-a-json",
            worker,
            "--worker-b-json",
            worker,
            "--path-a",
            str(path_a),
            "--path-b",
            str(path_b),
            "--evidence",
            str(evidence),
            "--peer-request-timeout",
            "0.2",
            "--peer-request-timeout-source",
            "selftest explicit tiny bound",
            "--min-wait-seconds",
            "0.35",
            "--child-timeout",
            "8",
        ]
        if extra:
            cmd.extend(extra)
        result = subprocess.run(cmd, text=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE, timeout=60)
        return result, evidence

    def test_ext4_workers_pass_but_are_reference_only(self):
        with tempfile.TemporaryDirectory() as temp_dir:
            temp = Path(temp_dir)
            result, evidence = self.run_probe(temp)
            self.assertEqual(result.returncode, 0, result.stderr + result.stdout)
            report = json.loads((evidence / "report.json").read_text(encoding="utf-8"))
            self.assertEqual(report["status"], "PASS")
            self.assertFalse(report["cross_mount_qualified"])
            self.assertTrue(report["same_kernel_reference_only"])
            self.assertEqual(report["timeout_config"]["peer_request_timeout_seconds"], 0.2)
            self.assertEqual(report["timeout_config"]["peer_request_timeout_source"], "selftest explicit tiny bound")
            self.assertEqual(report["summary"]["failed"], 0)
            self.assertGreaterEqual(report["summary"]["commands"], 1)
            step_names = {step["name"] for step in report["steps"]}
            self.assertIn("posix_overlap_conflict_getlk", step_names)
            self.assertIn("setlkw_wake_interrupt_timeout_bound", step_names)
            self.assertIn("rename_unlink_open_inode_recreated_path", step_names)
            self.assertIn("readonly_fd_write_lock_ebadf", step_names)

    def test_require_cross_mount_rejects_same_kernel_workers(self):
        with tempfile.TemporaryDirectory() as temp_dir:
            temp = Path(temp_dir)
            result, evidence = self.run_probe(temp, extra=["--require-cross-mount"])
            self.assertNotEqual(result.returncode, 0)
            report = json.loads((evidence / "report.json").read_text(encoding="utf-8"))
            self.assertEqual(report["status"], "FAIL")
            self.assertFalse(report["cross_mount_qualified"])
            self.assertIn("workers are qualified cross-mount evidence", report["failures"][0]["message"])

    def test_worker_readonly_write_lock_returns_ebadf(self):
        with tempfile.TemporaryDirectory() as temp_dir:
            path = Path(temp_dir) / "target.bin"
            path.write_bytes(b"readonly\n")
            result = subprocess.run(
                [
                    sys.executable,
                    str(PROBE),
                    "worker",
                    "--op",
                    "try_readonly_write_lock",
                    "--path",
                    str(path),
                    "--start",
                    "0",
                    "--length",
                    "1",
                ],
                text=True,
                stdout=subprocess.PIPE,
                stderr=subprocess.PIPE,
                timeout=20,
            )
            self.assertEqual(result.returncode, 0, result.stderr)
            event = json.loads(result.stdout)
            self.assertFalse(event["ok"])
            self.assertEqual(event["errno_name"], "EBADF")

    def test_worker_pid_file_identity_records_product_and_meta(self):
        with tempfile.TemporaryDirectory() as temp_dir:
            path = Path(temp_dir) / "target.bin"
            path.write_bytes(b"identity\n")
            pid_file = Path(temp_dir) / "self.pid"
            pid_file.write_text(f"{os.getpid()}\n", encoding="utf-8")
            result = subprocess.run(
                [
                    sys.executable,
                    str(PROBE),
                    "worker",
                    "--op",
                    "identity",
                    "--path",
                    str(path),
                    "--process-pid-file",
                    f"node={pid_file}",
                    "--meta-process-pid-file",
                    f"meta={pid_file}",
                ],
                text=True,
                stdout=subprocess.PIPE,
                stderr=subprocess.PIPE,
                timeout=20,
            )
            self.assertEqual(result.returncode, 0, result.stderr)
            event = json.loads(result.stdout)
            self.assertIn("node", event["identity"]["product_processes"])
            self.assertIn("meta", event["identity"]["meta_processes"])
            self.assertRegex(event["identity"]["product_processes"]["node"]["sha256"], r"^[0-9a-f]{64}$")

    def test_fake_different_kernel_non_afs_fuse_is_not_qualified(self):
        with tempfile.TemporaryDirectory() as temp_dir:
            temp = Path(temp_dir)
            fake = temp / "fake_worker.py"
            fake.write_text(
                "import json, sys\n"
                "args=sys.argv\n"
                "op=args[args.index('--op')+1]\n"
                "boot='boot-a' if '--path' in args and 'a.bin' in args[args.index('--path')+1] else 'boot-b'\n"
                "event={'event':'RESULT','op':op,'identity':{'boot_id':boot,'product_processes':{'node':{'sha256':'0'*64}},'meta_processes':{'meta':{'sha256':'1'*64}},'mount':{'returncode':0,'stdout':json.dumps({'filesystems':[{'fstype':'ext4','source':'/dev/vda1','options':'rw'}]}),'stderr':''}},'stat':{'sha256':'same'}}\n"
                "print(json.dumps(event))\n",
                encoding="utf-8",
            )
            worker = json.dumps([sys.executable, str(fake)])
            evidence = temp / "evidence"
            cmd = [
                sys.executable,
                str(PROBE),
                "host",
                "--worker-a-json",
                worker,
                "--worker-b-json",
                worker,
                "--path-a",
                str(temp / "a.bin"),
                "--path-b",
                str(temp / "b.bin"),
                "--evidence",
                str(evidence),
                "--peer-request-timeout",
                "0.2",
                "--min-wait-seconds",
                "0.35",
                "--require-cross-mount",
            ]
            result = subprocess.run(cmd, text=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE, timeout=20)
            self.assertNotEqual(result.returncode, 0)
            report = json.loads((evidence / "report.json").read_text(encoding="utf-8"))
            self.assertFalse(report["cross_mount_qualified"])
            self.assertFalse(report["identity"]["qualification"]["worker_a_afs_fuse"])
            self.assertFalse(report["identity"]["qualification"]["worker_b_afs_fuse"])

    def test_fake_enolck_fcntl_conflict_fails_host_step(self):
        with tempfile.TemporaryDirectory() as temp_dir:
            temp = Path(temp_dir)
            fake = temp / "fake_enolck_worker.py"
            fake.write_text(
                "import json, sys\n"
                "op=sys.argv[sys.argv.index('--op')+1]\n"
                "base={'identity':{'boot_id':'same','mount':{'returncode':0,'stdout':json.dumps({'filesystems':[{'fstype':'ext4','source':'/dev/vda1','options':'rw'}]}),'stderr':''}}}\n"
                "if op in ('init','read_stat','identity'):\n"
                "  print(json.dumps({'event':'RESULT','op':op,**base,'stat':{'sha256':'same'}})); sys.exit(0)\n"
                "if op == 'hold_fcntl':\n"
                "  print(json.dumps({'event':'READY','op':op,**base}), flush=True); sys.stdin.readline(); sys.exit(0)\n"
                "if op == 'try_fcntl':\n"
                "  print(json.dumps({'event':'RESULT','op':op,**base,'ok':False,'errno':37,'errno_name':'ENOLCK'})); sys.exit(1)\n"
                "if op == 'getlk':\n"
                "  print(json.dumps({'event':'RESULT','op':op,**base,'result':{'ok':True,'l_type':1}})); sys.exit(0)\n"
                "print(json.dumps({'event':'RESULT','op':op,**base,'ok':True})); sys.exit(0)\n",
                encoding="utf-8",
            )
            worker = json.dumps([sys.executable, str(fake)])
            evidence = temp / "evidence"
            cmd = [
                sys.executable,
                str(PROBE),
                "host",
                "--worker-a-json",
                worker,
                "--worker-b-json",
                worker,
                "--path-a",
                str(temp / "target.bin"),
                "--path-b",
                str(temp / "target.bin"),
                "--evidence",
                str(evidence),
                "--peer-request-timeout",
                "0.2",
                "--min-wait-seconds",
                "0.35",
            ]
            result = subprocess.run(cmd, text=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE, timeout=20)
            self.assertNotEqual(result.returncode, 0)
            report = json.loads((evidence / "report.json").read_text(encoding="utf-8"))
            self.assertEqual(report["status"], "FAIL")
            self.assertEqual(report["failures"][0]["name"], "posix_overlap_conflict_getlk")
            self.assertIn("fcntl conflict errno is EACCES/EAGAIN", report["failures"][0]["message"])

    def test_bad_fake_worker_preserves_command_and_failure_accounting(self):
        with tempfile.TemporaryDirectory() as temp_dir:
            temp = Path(temp_dir)
            fake = temp / "fake_worker.py"
            fake.write_text(
                "import json, sys\n"
                "print(json.dumps({'event':'ERROR','op':sys.argv[sys.argv.index('--op')+1],'message':'controlled fake failure'}))\n"
                "sys.exit(1)\n",
                encoding="utf-8",
            )
            worker = json.dumps([sys.executable, str(fake)])
            evidence = temp / "evidence"
            cmd = [
                sys.executable,
                str(PROBE),
                "host",
                "--worker-a-json",
                worker,
                "--worker-b-json",
                worker,
                "--path-a",
                str(temp / "a.bin"),
                "--path-b",
                str(temp / "a.bin"),
                "--evidence",
                str(evidence),
            ]
            result = subprocess.run(cmd, text=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE, timeout=20)
            self.assertNotEqual(result.returncode, 0)
            report = json.loads((evidence / "report.json").read_text(encoding="utf-8"))
            self.assertEqual(report["status"], "FAIL")
            self.assertEqual(report["summary"]["failed"], 1)
            self.assertEqual(report["commands"][0]["json_events"][0]["message"], "controlled fake failure")


if __name__ == "__main__":
    unittest.main()
