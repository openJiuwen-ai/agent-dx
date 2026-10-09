#!/usr/bin/env python3
import argparse
import hashlib
import importlib.util
import os
import platform
import signal
import subprocess
import sys
import tempfile
import unittest
from pathlib import Path


PROBE = Path(__file__).resolve().parent / "probes" / "locks_renewal_fault.py"


def load_probe_module():
    spec = importlib.util.spec_from_file_location("locks_renewal_fault_for_test", PROBE)
    module = importlib.util.module_from_spec(spec)
    assert spec.loader is not None
    spec.loader.exec_module(module)
    return module


def sha256_file(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for chunk in iter(lambda: handle.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def process_state(pid: int) -> str:
    for line in (Path("/proc") / str(pid) / "status").read_text(encoding="utf-8").splitlines():
        if line.startswith("State:"):
            return line
    return ""


@unittest.skipUnless(platform.system() == "Linux", "renewal fault selftests run only on Linux")
class RenewalFaultControlTests(unittest.TestCase):
    def start_target(self, temp: Path) -> tuple[subprocess.Popen[str], Path, str]:
        proc = subprocess.Popen([sys.executable, "-c", "import time; time.sleep(120)"], text=True)
        pid_file = temp / f"target-{proc.pid}.pid"
        pid_file.write_text(f"{proc.pid}\n", encoding="utf-8")
        return proc, pid_file, sha256_file(Path("/proc") / str(proc.pid) / "exe")

    def stop_process(self, proc: subprocess.Popen[str]) -> None:
        if proc.poll() is None:
            try:
                os.kill(proc.pid, signal.SIGCONT)
            except ProcessLookupError:
                pass
            proc.terminate()
            try:
                proc.wait(timeout=5)
            except subprocess.TimeoutExpired:
                proc.kill()
                proc.wait(timeout=5)

    def make_probe(self, module, temp: Path, pid_file: Path, sha: str):
        args = argparse.Namespace(
            worker_a_json=[],
            worker_b_json=[],
            path_a=str(temp / "a"),
            path_b=str(temp / "b"),
            renamed_a=None,
            renamed_b=None,
            evidence=str(temp / "evidence"),
            meta_control_json=[],
            meta_pid_file=str(pid_file),
            meta_executable_sha256=sha,
            command_timeout=5.0,
            control_timeout=5.0,
            child_timeout=5.0,
            peer_request_timeout=1.0,
            peer_request_timeout_source="selftest",
            min_wait_seconds=2.0,
            stop_seconds=0.2,
            worker_a_process_pid_file=[],
            worker_b_process_pid_file=[],
            worker_a_meta_process_pid_file=[],
            worker_b_meta_process_pid_file=[],
            keep_fixture=True,
            allow_host_non_linux=False,
            require_cross_mount=True,
        )
        return module.RenewalFaultProbe(args)

    def test_wrong_sha_rejected_before_signal(self):
        module = load_probe_module()
        with tempfile.TemporaryDirectory() as temp_dir:
            temp = Path(temp_dir)
            proc, pid_file, _sha = self.start_target(temp)
            try:
                probe = self.make_probe(module, temp, pid_file, "0" * 64)
                with self.assertRaises(AssertionError):
                    probe.run_control("validate")
                self.assertIsNone(proc.poll())
                self.assertNotIn("stopped", process_state(proc.pid).lower())
            finally:
                self.stop_process(proc)

    def test_changed_pid_rejected_after_binding_same_binary(self):
        module = load_probe_module()
        with tempfile.TemporaryDirectory() as temp_dir:
            temp = Path(temp_dir)
            proc_a, pid_file, sha = self.start_target(temp)
            proc_b, _pid_file_b, _sha_b = self.start_target(temp)
            try:
                probe = self.make_probe(module, temp, pid_file, sha)
                bound = probe.bind_meta_identity()
                self.assertEqual(bound["pid"], proc_a.pid)
                pid_file.write_text(f"{proc_b.pid}\n", encoding="utf-8")
                with self.assertRaises(AssertionError):
                    probe.run_control("state")
                self.assertIsNone(proc_a.poll())
                self.assertIsNone(proc_b.poll())
            finally:
                self.stop_process(proc_a)
                self.stop_process(proc_b)

    def test_prestopped_target_rejected_and_manual_cont_restores(self):
        module = load_probe_module()
        with tempfile.TemporaryDirectory() as temp_dir:
            temp = Path(temp_dir)
            proc, pid_file, sha = self.start_target(temp)
            try:
                os.kill(proc.pid, signal.SIGSTOP)
                deadline = __import__("time").time() + 5
                while "stopped" not in process_state(proc.pid).lower() and __import__("time").time() < deadline:
                    __import__("time").sleep(0.05)
                self.assertIn("stopped", process_state(proc.pid).lower())
                probe = self.make_probe(module, temp, pid_file, sha)
                with self.assertRaises(AssertionError):
                    probe.run_control("validate")
            finally:
                os.kill(proc.pid, signal.SIGCONT)
                self.stop_process(proc)

    def test_stop_then_cont_finally_restores_bound_target(self):
        module = load_probe_module()
        with tempfile.TemporaryDirectory() as temp_dir:
            temp = Path(temp_dir)
            proc, pid_file, sha = self.start_target(temp)
            probe = self.make_probe(module, temp, pid_file, sha)
            try:
                probe.bind_meta_identity()
                probe.meta_was_stopped = True
                stopped = probe.run_control("stop")
                self.assertTrue(stopped["ok"])
                self.assertIn("stopped", process_state(proc.pid).lower())
            finally:
                if probe.meta_was_stopped:
                    probe.run_control("cont")
                    probe.meta_was_stopped = False
                self.stop_process(proc)
            self.assertNotIn("stopped", process_state(proc.pid).lower() if proc.poll() is None else "")


if __name__ == "__main__":
    unittest.main()
