#!/usr/bin/env python3
"""Bounded Meta SIGSTOP renewal fault probe for AFS cross-mount locks.

The host may run on macOS as an orchestrator.  File IO and advisory-lock
syscalls still run inside the Linux workers through locks_cross.py worker
commands.  Meta fault injection is allowed only through an explicit control
prefix, PID file and executable SHA check; the probe validates the exact target
before sending any signal and sends SIGCONT in a finally block after SIGSTOP.
"""
from __future__ import annotations

import argparse
import hashlib
import json
import os
import platform
import random
import signal
import subprocess
import sys
import time
import traceback
from pathlib import Path
from typing import Any

SCRIPT_DIR = Path(__file__).resolve().parent
if str(SCRIPT_DIR) not in sys.path:
    sys.path.insert(0, str(SCRIPT_DIR))

import locks_cross  # noqa: E402


DEFAULT_STOP_SECONDS = 15.0
DEFAULT_COMMAND_TIMEOUT_SECONDS = 10.0
DEFAULT_CHILD_TIMEOUT_SECONDS = 35.0


def _now() -> str:
    return time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime())


def _expect(condition: bool, message: str, details: dict[str, Any] | None = None) -> None:
    if not condition:
        raise AssertionError(json.dumps({"message": message, "details": details or {}}, sort_keys=True))


def _json_argv(value: str) -> list[str]:
    parsed = json.loads(value)
    if not isinstance(parsed, list) or not all(isinstance(item, str) and item for item in parsed):
        raise argparse.ArgumentTypeError("expected JSON array of non-empty argv strings")
    return parsed


def _sha256_path(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for chunk in iter(lambda: handle.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def _read_text_optional(path: Path) -> str | None:
    try:
        return path.read_text(encoding="utf-8").strip()
    except OSError:
        return None


class RenewalFaultProbe(locks_cross.CrossProbe):
    def __init__(self, args: argparse.Namespace) -> None:
        super().__init__(args)
        self.meta_control = args.meta_control_json
        self.fault_records: list[dict[str, Any]] = []
        self.meta_was_stopped = False
        self.bound_meta_identity: dict[str, Any] | None = None
        self.run_id = f"locks-renewal-fault-{time.strftime('%Y%m%dT%H%M%SZ', time.gmtime())}-{os.getpid()}-{random.randrange(1_000_000):06d}"

    def run_control(self, action: str) -> dict[str, Any]:
        code = r'''
import hashlib, json, os, signal, sys, time
pid_file, expected_sha, action, expected_pid, expected_start_ticks, expected_boot_id = sys.argv[1:7]
def read_pid(path):
    raw = open(path, "r", encoding="utf-8").read().strip()
    if not raw.isdecimal() or int(raw) <= 0:
        raise RuntimeError(f"invalid pid file {path}: {raw!r}")
    return int(raw)
def sha256_file(path):
    h = hashlib.sha256()
    with open(path, "rb") as f:
        for chunk in iter(lambda: f.read(1024 * 1024), b""):
            h.update(chunk)
    return h.hexdigest()
def proc_state(pid):
    status = {}
    with open(f"/proc/{pid}/status", "r", encoding="utf-8") as f:
        for line in f:
            if ":" in line:
                key, value = line.split(":", 1)
                status[key] = value.strip()
    stat_text = open(f"/proc/{pid}/stat", "r", encoding="utf-8").read()
    close = stat_text.rfind(")")
    fields = stat_text[close + 2:].split()
    start_ticks = int(fields[19]) if len(fields) > 19 else None
    return {"status_state": status.get("State"), "stat_state": fields[0] if fields else None, "start_ticks": start_ticks}
def boot_id():
    with open("/proc/sys/kernel/random/boot_id", "r", encoding="utf-8") as f:
        return f.read().strip()
def stopped(state):
    return "stopped" in str(state.get("status_state", "")).lower() or state.get("stat_state") == "T"
pid = read_pid(pid_file)
exe_path = os.readlink(f"/proc/{pid}/exe")
actual_sha = sha256_file(f"/proc/{pid}/exe")
before = proc_state(pid)
actual_boot_id = boot_id()
if actual_sha != expected_sha:
    print(json.dumps({"event": "CONTROL", "action": action, "ok": False, "pid": pid, "exe_path": exe_path, "actual_sha256": actual_sha, "expected_sha256": expected_sha, "boot_id": actual_boot_id, "before": before, "error": "sha256 mismatch"}, sort_keys=True))
    sys.exit(2)
if expected_pid != "-" and pid != int(expected_pid):
    print(json.dumps({"event": "CONTROL", "action": action, "ok": False, "pid": pid, "expected_pid": int(expected_pid), "exe_path": exe_path, "sha256": actual_sha, "boot_id": actual_boot_id, "before": before, "error": "pid mismatch"}, sort_keys=True))
    sys.exit(2)
if expected_start_ticks != "-" and before.get("start_ticks") != int(expected_start_ticks):
    print(json.dumps({"event": "CONTROL", "action": action, "ok": False, "pid": pid, "exe_path": exe_path, "sha256": actual_sha, "boot_id": actual_boot_id, "before": before, "expected_start_ticks": int(expected_start_ticks), "error": "start_ticks mismatch"}, sort_keys=True))
    sys.exit(2)
if expected_boot_id != "-" and actual_boot_id != expected_boot_id:
    print(json.dumps({"event": "CONTROL", "action": action, "ok": False, "pid": pid, "exe_path": exe_path, "sha256": actual_sha, "boot_id": actual_boot_id, "expected_boot_id": expected_boot_id, "before": before, "error": "boot_id mismatch"}, sort_keys=True))
    sys.exit(2)
if action == "validate":
    if stopped(before):
        print(json.dumps({"event": "CONTROL", "action": action, "ok": False, "pid": pid, "pid_file": pid_file, "exe_path": exe_path, "sha256": actual_sha, "boot_id": actual_boot_id, "before": before, "after": before, "error": "target already stopped"}, sort_keys=True))
        sys.exit(1)
    after = before
elif action == "stop":
    os.kill(pid, signal.SIGSTOP)
    deadline = time.time() + 5.0
    after = proc_state(pid)
    while not stopped(after) and time.time() < deadline:
        time.sleep(0.05)
        after = proc_state(pid)
elif action == "cont":
    os.kill(pid, signal.SIGCONT)
    deadline = time.time() + 5.0
    after = proc_state(pid)
    while stopped(after) and time.time() < deadline:
        time.sleep(0.05)
        after = proc_state(pid)
elif action == "state":
    after = before
else:
    raise RuntimeError(f"unknown action {action}")
ok = True
if action == "stop":
    ok = stopped(after)
elif action == "cont":
    ok = not stopped(after)
print(json.dumps({"event": "CONTROL", "action": action, "ok": ok, "pid": pid, "pid_file": pid_file, "exe_path": exe_path, "sha256": actual_sha, "boot_id": actual_boot_id, "before": before, "after": after}, sort_keys=True))
sys.exit(0 if ok else 1)
'''
        bound = self.bound_meta_identity or {}
        argv = list(self.meta_control) + [
            "python3",
            "-c",
            code,
            self.args.meta_pid_file,
            self.args.meta_executable_sha256,
            action,
            str(bound.get("pid", "-")),
            str(bound.get("start_ticks", "-")),
            str(bound.get("boot_id", "-")),
        ]
        started = time.time()
        proc = subprocess.run(
            argv,
            text=True,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            timeout=self.args.control_timeout,
            check=False,
        )
        events = [json.loads(line) for line in proc.stdout.splitlines() if line.strip()]
        record = {
            "action": action,
            "argv": argv,
            "returncode": proc.returncode,
            "stdout": proc.stdout,
            "stderr": proc.stderr,
            "json_events": events,
            "duration_ms": round((time.time() - started) * 1000, 3),
        }
        self.fault_records.append(record)
        _expect(proc.returncode == 0 and events and events[-1].get("ok") is True, f"Meta control {action} succeeded", record)
        return events[-1]

    def bind_meta_identity(self) -> dict[str, Any]:
        event = self.run_control("validate")
        before = event.get("before", {})
        identity = {
            "pid": event.get("pid"),
            "start_ticks": before.get("start_ticks"),
            "boot_id": event.get("boot_id"),
            "sha256": event.get("sha256"),
            "exe_path": event.get("exe_path"),
            "pid_file": event.get("pid_file"),
        }
        _expect(isinstance(identity["pid"], int) and identity["pid"] > 0, "Meta PID identity is bound", identity)
        _expect(isinstance(identity["start_ticks"], int) and identity["start_ticks"] > 0, "Meta start_ticks identity is bound", identity)
        _expect(isinstance(identity["boot_id"], str) and bool(identity["boot_id"]), "Meta boot_id identity is bound", identity)
        _expect(identity["sha256"] == self.args.meta_executable_sha256, "Meta executable SHA identity is bound", identity)
        self.bound_meta_identity = identity
        return event

    def assert_worker_a_meta_matches_bound_target(self) -> None:
        _expect(self.bound_meta_identity is not None, "Meta identity has been bound")
        target = self.bound_meta_identity
        meta_processes = self.identity.get("worker_a", {}).get("identity", {}).get("meta_processes", {})
        matches = [
            {"role": role, **process}
            for role, process in meta_processes.items()
            if process.get("pid") == target.get("pid")
            and process.get("start_ticks") == target.get("start_ticks")
            and process.get("sha256") == target.get("sha256")
        ]
        _expect(bool(matches), "bound Meta target matches worker A collected Meta identity", {"target": target, "worker_a_meta_processes": meta_processes})

    def setup(self) -> None:
        super().setup()
        _expect(self.identity.get("cross_mount_qualified") is True, "different-kernel AFS cross-mount identity is required", self.identity)
        _expect(self.args.stop_seconds > 0 and self.args.stop_seconds < 30, "stop duration stays below the 30 second lease bound", {"stop_seconds": self.args.stop_seconds})
        self.bind_meta_identity()
        self.assert_worker_a_meta_matches_bound_target()

    def sleep_with_liveness(self, holder: locks_cross.WorkerProcess, waiter: locks_cross.WorkerProcess) -> dict[str, Any]:
        started = time.monotonic()
        deadline = started + self.args.stop_seconds
        samples: list[dict[str, Any]] = []
        while time.monotonic() < deadline:
            samples.append({
                "elapsed_seconds": round(time.monotonic() - started, 3),
                "holder_returncode": holder.proc.poll(),
                "waiter_returncode": waiter.proc.poll(),
            })
            _expect(holder.proc.poll() is None, "holder remains alive while Meta is stopped", samples[-1])
            _expect(waiter.proc.poll() is None, "SETLKW waiter remains pending while Meta is stopped", samples[-1])
            time.sleep(min(1.0, max(0.0, deadline - time.monotonic())))
        elapsed = time.monotonic() - started
        return {"requested_seconds": self.args.stop_seconds, "elapsed_seconds": round(elapsed, 3), "samples": samples}

    def test_meta_stop_waiter_survives(self) -> dict[str, Any]:
        holder = self.start_worker("A-lock-holder", "A", "hold_fcntl", self.path_a, mode="exclusive", start=0, length=10)
        waiter: locks_cross.WorkerProcess | None = None
        acquired: dict[str, Any] | None = None
        holder_rc: int | None = None
        waiter_rc: int | None = None
        try:
            ready = holder.read_event("READY")
            waiter = self.start_worker("B-lock-waiter", "B", "wait_fcntl", self.path_b, mode="exclusive", start=0, length=10)
            waiting = waiter.read_event("WAITING")
            self.meta_was_stopped = True
            stopped = self.run_control("stop")
            during_stop = self.sleep_with_liveness(holder, waiter)
        finally:
            if self.meta_was_stopped:
                try:
                    self.run_control("cont")
                    self.meta_was_stopped = False
                except Exception as exc:  # noqa: BLE001 - preserve primary failure but keep evidence
                    self.failures.append({"name": "meta_cont_finally", "ok": False, "exception": type(exc).__name__, "message": str(exc), "traceback": traceback.format_exc()})
        state_after_cont = self.run_control("state")
        holder_rc = holder.release()
        _expect(waiter is not None, "waiter was started")
        acquired = waiter.read_event("ACQUIRED", timeout=self.args.child_timeout)
        waiter_rc = waiter.release()
        _expect(holder_rc == 0, "holder exits cleanly after unlock", {"returncode": holder_rc, "stderr": holder.stderr})
        _expect(waiter_rc == 0, "waiter exits cleanly after acquiring lock", {"returncode": waiter_rc, "stderr": waiter.stderr, "acquired": acquired})
        _expect(acquired.get("event") == "ACQUIRED", "waiter acquired after Meta resumes without EIO", acquired)
        _expect(float(acquired.get("waited_seconds", 0.0)) >= self.args.stop_seconds, "waiter remained blocked through stopped Meta interval", acquired)
        return {
            "ready": ready,
            "waiting": waiting,
            "stopped": stopped,
            "during_stop": during_stop,
            "state_after_cont": state_after_cont,
            "acquired": acquired,
            "holder_returncode": holder_rc,
            "waiter_returncode": waiter_rc,
        }

    def run(self) -> None:
        self.step("fixture_identity", self.test_fixture_identity)
        self.step("meta_sigstop_waiter_survives_and_acquires", self.test_meta_stop_waiter_survives)

    def write_report(self) -> None:
        report = {
            "schema": "afs.locks_renewal_fault.v1",
            "created_at": _now(),
            "run_id": self.run_id,
            "status": "PASS" if not self.failures else "FAIL",
            "path_a": self.path_a,
            "path_b": self.path_b,
            "fixture_kept": self.fixture_kept,
            "worker_commands": {"A": self.worker_a, "B": self.worker_b},
            "meta_control_command": self.meta_control,
            "meta_pid_file": self.args.meta_pid_file,
            "meta_executable_sha256": self.args.meta_executable_sha256,
            "bound_meta_identity": self.bound_meta_identity,
            "host": {
                "system": platform.system(),
                "release": platform.release(),
                "machine": platform.machine(),
                "python": platform.python_version(),
                "uid": os.geteuid(),
                "gid": os.getegid(),
                "boot_id": _read_text_optional(Path("/proc/sys/kernel/random/boot_id")),
            },
            "identity": self.identity,
            "timeout_config": self.timeout_config() | {
                "control_timeout_seconds": self.args.control_timeout,
                "stop_seconds": self.args.stop_seconds,
            },
            "summary": {
                "steps": len(self.records),
                "passed": sum(1 for record in self.records if record.get("ok")),
                "failed": len(self.failures),
                "commands": len(self.commands),
                "fault_commands": len(self.fault_records),
            },
            "steps": self.records,
            "failures": self.failures,
            "commands": self.commands,
            "fault_commands": self.fault_records,
            "notes": [
                "All lock syscalls run in Linux worker processes through locks_cross.py worker commands.",
                "The host may be macOS only as an orchestrator when --allow-host-non-linux is supplied.",
                "Meta STOP/CONT is sent only after PID file and executable SHA identity validation.",
                "Any unqualified identity, skipped fault, control failure, worker error, or waiter EIO makes the report FAIL.",
            ],
        }
        self.evidence.mkdir(parents=True, exist_ok=True)
        (self.evidence / "report.json").write_text(json.dumps(report, indent=2, sort_keys=True) + "\n", encoding="utf-8")
        (self.evidence / "summary.txt").write_text(
            f"status={report['status']} cross_mount_qualified={self.identity.get('cross_mount_qualified', False)} "
            f"steps={report['summary']['steps']} failed={report['summary']['failed']} "
            f"fault_commands={report['summary']['fault_commands']} evidence={self.evidence}\n",
            encoding="utf-8",
        )


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Run bounded Meta SIGSTOP renewal fault check for cross-mount locks")
    parser.add_argument("--worker-a-json", required=True, type=_json_argv, help="JSON argv prefix ending in locks_cross.py for worker A")
    parser.add_argument("--worker-b-json", required=True, type=_json_argv, help="JSON argv prefix ending in locks_cross.py for worker B")
    parser.add_argument("--path-a", required=True, help="worker A path for the logical DFS test file")
    parser.add_argument("--path-b", required=True, help="worker B path for the same logical DFS test file")
    parser.add_argument("--renamed-a", help=argparse.SUPPRESS)
    parser.add_argument("--renamed-b", help=argparse.SUPPRESS)
    parser.add_argument("--evidence", required=True)
    parser.add_argument("--meta-control-json", required=True, type=_json_argv, help="JSON command prefix used to run Python control snippets next to the Meta process, e.g. [\"limactl\",\"shell\",\"afs-accept-a\",\"--\",\"sudo\"]")
    parser.add_argument("--meta-pid-file", required=True, help="PID file path as seen by the Meta control prefix")
    parser.add_argument("--meta-executable-sha256", required=True, help="expected SHA256 of /proc/PID/exe for the Meta process")
    parser.add_argument("--command-timeout", type=float, default=DEFAULT_COMMAND_TIMEOUT_SECONDS)
    parser.add_argument("--control-timeout", type=float, default=DEFAULT_COMMAND_TIMEOUT_SECONDS)
    parser.add_argument("--child-timeout", type=float, default=DEFAULT_CHILD_TIMEOUT_SECONDS)
    parser.add_argument("--peer-request-timeout", type=float, default=locks_cross.DEFAULT_PEER_REQUEST_TIMEOUT_SECONDS)
    parser.add_argument("--peer-request-timeout-source", default="default common transport gRPC request timeout")
    parser.add_argument("--min-wait-seconds", type=float, default=locks_cross.DEFAULT_MIN_WAIT_SECONDS)
    parser.add_argument("--stop-seconds", type=float, default=DEFAULT_STOP_SECONDS)
    parser.add_argument("--worker-a-process-pid-file", action="append", type=locks_cross._parse_named_path, default=[], metavar="ROLE=PID_FILE")
    parser.add_argument("--worker-b-process-pid-file", action="append", type=locks_cross._parse_named_path, default=[], metavar="ROLE=PID_FILE")
    parser.add_argument("--worker-a-meta-process-pid-file", action="append", type=locks_cross._parse_named_path, default=[], metavar="ROLE=PID_FILE")
    parser.add_argument("--worker-b-meta-process-pid-file", action="append", type=locks_cross._parse_named_path, default=[], metavar="ROLE=PID_FILE")
    parser.add_argument("--keep-fixture", action="store_true")
    parser.add_argument("--allow-host-non-linux", action="store_true")
    parser.set_defaults(require_cross_mount=True)
    return parser


def main(argv: list[str] | None = None) -> int:
    args = build_parser().parse_args(argv)
    probe = RenewalFaultProbe(args)
    try:
        probe.setup()
        probe.run()
    except Exception as exc:  # noqa: BLE001
        probe.failures.append({"name": "probe_setup_or_run", "ok": False, "exception": type(exc).__name__, "message": str(exc), "traceback": traceback.format_exc()})
        if probe.meta_was_stopped:
            try:
                probe.run_control("cont")
                probe.meta_was_stopped = False
            except Exception as cont_exc:  # noqa: BLE001
                probe.failures.append({"name": "meta_cont_outer_finally", "ok": False, "exception": type(cont_exc).__name__, "message": str(cont_exc), "traceback": traceback.format_exc()})
    finally:
        try:
            probe.cleanup()
        finally:
            probe.write_report()
    return 0 if not probe.failures else 1


if __name__ == "__main__":
    raise SystemExit(main())
