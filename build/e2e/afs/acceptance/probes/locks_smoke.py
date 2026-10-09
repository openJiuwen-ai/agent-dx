#!/usr/bin/env python3
"""Short real Linux advisory-lock smoke probe for AFS acceptance.

The probe exercises Linux fcntl byte-range locks and BSD flock whole-file
locks using distinct child processes. It can run on ordinary ext4 as a known
reference and on an AFS FUSE mount as a local-mount probe. Optional second
path/mount arguments let the same cases run against two paths that should name
one logical file; callers must decide whether that is same-node or cross-node.
"""
from __future__ import annotations

import argparse
import ctypes
import errno
import fcntl
import hashlib
import json
import os
import platform
import random
import selectors
import shutil
import signal
import stat
import struct
import subprocess
import sys
import time
import traceback
from pathlib import Path
from typing import Any, Callable


_LIBC = ctypes.CDLL(None, use_errno=True)


class CFlock(ctypes.Structure):
    _fields_ = [
        ("l_type", ctypes.c_short),
        ("l_whence", ctypes.c_short),
        ("l_start", ctypes.c_longlong),
        ("l_len", ctypes.c_longlong),
        ("l_pid", ctypes.c_int),
    ]


_LIBC.fcntl.argtypes = [ctypes.c_int, ctypes.c_int, ctypes.POINTER(CFlock)]
_LIBC.fcntl.restype = ctypes.c_int

PROCESS_NAME_RE = __import__("re").compile(r"^[A-Za-z0-9_.-]+$")


def _now() -> str:
    return time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime())


def _errno_name(value: int | None) -> str | None:
    if value is None:
        return None
    return errno.errorcode.get(value, f"ERRNO_{value}")


def _expect(condition: bool, message: str, details: dict[str, Any] | None = None) -> None:
    if not condition:
        raise AssertionError(json.dumps({"message": message, "details": details or {}}, sort_keys=True))


def _sha256_file(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for chunk in iter(lambda: handle.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def _parse_start_ticks(stat_text: str) -> int:
    close = stat_text.rfind(")")
    if close == -1:
        raise RuntimeError("/proc stat is malformed: missing comm terminator")
    tail = stat_text[close + 2 :].split()
    if len(tail) <= 19:
        raise RuntimeError("/proc stat is malformed: missing starttime")
    return int(tail[19])


def _process_fingerprint(pid: int) -> dict[str, Any]:
    proc = Path("/proc") / str(pid)
    stat_text = (proc / "stat").read_text(encoding="utf-8")
    exe = proc / "exe"
    exe_target = os.readlink(exe)
    exe_stat = exe.stat()
    return {
        "pid": pid,
        "start_ticks": _parse_start_ticks(stat_text),
        "exe_path": exe_target,
        "exe_dev": exe_stat.st_dev,
        "exe_inode": exe_stat.st_ino,
    }


def _process_identity(pid: int, name: str, source: str) -> dict[str, Any]:
    before = _process_fingerprint(pid)
    digest = _sha256_file(Path("/proc") / str(pid) / "exe")
    after = _process_fingerprint(pid)
    for key in ("pid", "start_ticks", "exe_dev", "exe_inode"):
        if before[key] != after[key]:
            raise RuntimeError(f"process {name} changed while hashing: {key} {before[key]!r} -> {after[key]!r}")
    return {
        "name": name,
        "pid": pid,
        "pid_source": source,
        "exe_path": before["exe_path"],
        "sha256": digest,
        "start_ticks": before["start_ticks"],
        "exe_dev": before["exe_dev"],
        "exe_inode": before["exe_inode"],
    }


def _parse_process_assignment(value: str) -> tuple[str, int, str]:
    if "=" not in value:
        raise argparse.ArgumentTypeError("expected NAME=PID")
    name, pid_text = value.split("=", 1)
    if not PROCESS_NAME_RE.fullmatch(name):
        raise argparse.ArgumentTypeError(f"invalid process name {name!r}")
    if not pid_text.isdecimal() or int(pid_text) <= 0:
        raise argparse.ArgumentTypeError(f"invalid pid {pid_text!r}")
    return name, int(pid_text), "argv"


def _fcntl_call(fd: int, cmd: int, lock_type: int, start: int, length: int) -> tuple[int, CFlock, int | None]:
    lock = CFlock(lock_type, os.SEEK_SET, start, length, 0)
    ctypes.set_errno(0)
    rc = _LIBC.fcntl(fd, cmd, ctypes.byref(lock))
    err = ctypes.get_errno() if rc < 0 else None
    return rc, lock, err


def _fcntl_set(fd: int, lock_type: int, start: int, length: int, blocking: bool) -> tuple[bool, int | None]:
    cmd = fcntl.F_SETLKW if blocking else fcntl.F_SETLK
    rc, _lock, err = _fcntl_call(fd, cmd, lock_type, start, length)
    return rc == 0, err


def _fcntl_getlk(fd: int, lock_type: int, start: int, length: int) -> dict[str, Any]:
    rc, lock, err = _fcntl_call(fd, fcntl.F_GETLK, lock_type, start, length)
    if rc != 0:
        return {"ok": False, "errno": err, "errno_name": _errno_name(err)}
    return {
        "ok": True,
        "l_type": int(lock.l_type),
        "l_type_name": _lock_type_name(lock.l_type),
        "l_whence": int(lock.l_whence),
        "l_start": int(lock.l_start),
        "l_len": int(lock.l_len),
        "l_pid": int(lock.l_pid),
    }


def _lock_type_name(value: int) -> str:
    if value == fcntl.F_RDLCK:
        return "F_RDLCK"
    if value == fcntl.F_WRLCK:
        return "F_WRLCK"
    if value == fcntl.F_UNLCK:
        return "F_UNLCK"
    return f"LOCK_TYPE_{value}"


def _flock_op(mode: str) -> int:
    if mode == "shared":
        return fcntl.LOCK_SH
    if mode == "exclusive":
        return fcntl.LOCK_EX
    if mode == "unlock":
        return fcntl.LOCK_UN
    raise ValueError(mode)


def _fcntl_conflict_errno_ok(value: int | None) -> bool:
    return value in (errno.EACCES, errno.EAGAIN)


def _flock_conflict_errno_ok(value: int | None) -> bool:
    return value in (errno.EACCES, errno.EAGAIN)


def _emit(obj: dict[str, Any]) -> None:
    print(json.dumps(obj, sort_keys=True), flush=True)


def _child_main(args: argparse.Namespace) -> int:
    path = Path(args.path)
    op = args.child_op
    try:
        if op in {"hold_fcntl", "wait_fcntl", "try_fcntl", "getlk", "fcntl_close_other_then_hold"}:
            fd = os.open(path, os.O_RDWR)
            try:
                lock_type = fcntl.F_RDLCK if args.mode == "shared" else fcntl.F_WRLCK
                if op == "hold_fcntl":
                    ok, err = _fcntl_set(fd, lock_type, args.start, args.length, blocking=False)
                    if not ok:
                        _emit({"event": "ERROR", "errno": err, "errno_name": _errno_name(err), "pid": os.getpid()})
                        return 1
                    _emit({"event": "READY", "pid": os.getpid()})
                    sys.stdin.readline()
                    _fcntl_set(fd, fcntl.F_UNLCK, args.start, args.length, blocking=False)
                    return 0
                if op == "wait_fcntl":
                    _emit({"event": "WAITING", "pid": os.getpid()})
                    ok, err = _fcntl_set(fd, lock_type, args.start, args.length, blocking=True)
                    if not ok:
                        _emit({"event": "ERROR", "errno": err, "errno_name": _errno_name(err), "pid": os.getpid()})
                        return 1
                    _emit({"event": "ACQUIRED", "pid": os.getpid()})
                    sys.stdin.readline()
                    _fcntl_set(fd, fcntl.F_UNLCK, args.start, args.length, blocking=False)
                    return 0
                if op == "try_fcntl":
                    ok, err = _fcntl_set(fd, lock_type, args.start, args.length, blocking=False)
                    _emit({"event": "RESULT", "ok": ok, "errno": err, "errno_name": _errno_name(err), "pid": os.getpid()})
                    if ok:
                        _fcntl_set(fd, fcntl.F_UNLCK, args.start, args.length, blocking=False)
                    return 0 if ok or _fcntl_conflict_errno_ok(err) else 1
                if op == "getlk":
                    _emit({"event": "RESULT", "result": _fcntl_getlk(fd, lock_type, args.start, args.length), "pid": os.getpid()})
                    return 0
                if op == "fcntl_close_other_then_hold":
                    fd2 = os.open(path, os.O_RDWR)
                    try:
                        ok, err = _fcntl_set(fd, fcntl.F_WRLCK, args.start, args.length, blocking=False)
                        if not ok:
                            _emit({"event": "ERROR", "errno": err, "errno_name": _errno_name(err), "pid": os.getpid()})
                            return 1
                        os.close(fd2)
                        fd2 = -1
                        _emit({"event": "READY", "pid": os.getpid(), "closed_other_fd": True})
                        sys.stdin.readline()
                        return 0
                    finally:
                        if fd2 >= 0:
                            os.close(fd2)
            finally:
                os.close(fd)
        if op in {"hold_flock", "wait_flock", "try_flock", "flock_dup_close_then_hold"}:
            fd = os.open(path, os.O_RDWR)
            try:
                operation = _flock_op(args.mode)
                if op == "hold_flock":
                    fcntl.flock(fd, operation | fcntl.LOCK_NB)
                    _emit({"event": "READY", "pid": os.getpid()})
                    sys.stdin.readline()
                    fcntl.flock(fd, fcntl.LOCK_UN)
                    return 0
                if op == "wait_flock":
                    _emit({"event": "WAITING", "pid": os.getpid()})
                    fcntl.flock(fd, operation)
                    _emit({"event": "ACQUIRED", "pid": os.getpid()})
                    sys.stdin.readline()
                    fcntl.flock(fd, fcntl.LOCK_UN)
                    return 0
                if op == "try_flock":
                    try:
                        fcntl.flock(fd, operation | fcntl.LOCK_NB)
                        ok = True
                        err = None
                    except OSError as exc:
                        ok = False
                        err = exc.errno
                    _emit({"event": "RESULT", "ok": ok, "errno": err, "errno_name": _errno_name(err), "pid": os.getpid()})
                    if ok:
                        fcntl.flock(fd, fcntl.LOCK_UN)
                    return 0 if ok or _flock_conflict_errno_ok(err) else 1
                if op == "flock_dup_close_then_hold":
                    fcntl.flock(fd, fcntl.LOCK_EX | fcntl.LOCK_NB)
                    dup_fd = os.dup(fd)
                    os.close(fd)
                    fd = dup_fd
                    _emit({"event": "READY", "pid": os.getpid(), "closed_original_fd": True})
                    sys.stdin.readline()
                    fcntl.flock(fd, fcntl.LOCK_UN)
                    return 0
            finally:
                os.close(fd)
        raise RuntimeError(f"unknown child op {op}")
    except OSError as exc:
        _emit({"event": "ERROR", "exception": type(exc).__name__, "message": str(exc), "errno": exc.errno, "errno_name": _errno_name(exc.errno), "pid": os.getpid()})
        return 1
    except Exception as exc:  # noqa: BLE001
        _emit({"event": "ERROR", "exception": type(exc).__name__, "message": str(exc), "traceback": traceback.format_exc(), "pid": os.getpid()})
        return 1


class Child:
    def __init__(self, proc: subprocess.Popen[str], role: str, timeout: float) -> None:
        self.proc = proc
        self.role = role
        self.timeout = timeout
        self.events: list[dict[str, Any]] = []
        self.identity: dict[str, Any] | None = None
        self.stderr = ""
        self._stdout_buffer = b""

    def read_event(self, expected: str | None = None, timeout: float | None = None) -> dict[str, Any]:
        deadline = time.monotonic() + (timeout if timeout is not None else self.timeout)
        while time.monotonic() < deadline:
            line = self._readline(deadline)
            if line:
                event = json.loads(line)
                self.events.append(event)
                if expected is None or event.get("event") == expected:
                    return event
                if event.get("event") == "ERROR":
                    raise RuntimeError(json.dumps({"role": self.role, "event": event}, sort_keys=True))
                continue
            if self.proc.poll() is not None:
                break
            time.sleep(0.02)
        raise TimeoutError(f"child {self.role} did not emit {expected or 'event'} within timeout")

    def assert_no_event(self, forbidden: str, duration: float, *, require_alive: bool = True) -> dict[str, Any]:
        deadline = time.monotonic() + duration
        observed: list[dict[str, Any]] = []
        while time.monotonic() < deadline:
            line = self._readline(deadline)
            if line:
                event = json.loads(line)
                self.events.append(event)
                observed.append(event)
                if event.get("event") == forbidden:
                    raise AssertionError(json.dumps({"message": f"child {self.role} emitted {forbidden} too early", "event": event}, sort_keys=True))
                if event.get("event") == "ERROR":
                    raise RuntimeError(json.dumps({"role": self.role, "event": event}, sort_keys=True))
                continue
            if self.proc.poll() is not None:
                if require_alive:
                    raise AssertionError(json.dumps({"message": f"child {self.role} exited before {forbidden} check finished", "returncode": self.proc.returncode}, sort_keys=True))
                break
            time.sleep(0.02)
        if require_alive:
            _expect(self.proc.poll() is None, f"child {self.role} remains alive while {forbidden} is absent")
        return {"forbidden": forbidden, "duration_seconds": duration, "observed": observed, "pid_alive": self.proc.poll() is None}

    def _readline(self, deadline: float) -> str:
        if self.proc.stdout is None:
            return ""
        while b"\n" not in self._stdout_buffer:
            remaining = max(0.0, deadline - time.monotonic())
            if remaining <= 0:
                return ""
            with selectors.DefaultSelector() as selector:
                selector.register(self.proc.stdout.fileno(), selectors.EVENT_READ)
                if not selector.select(remaining):
                    return ""
            chunk = os.read(self.proc.stdout.fileno(), 4096)
            if not chunk:
                return ""
            self._stdout_buffer += chunk
        line, self._stdout_buffer = self._stdout_buffer.split(b"\n", 1)
        return line.decode("utf-8")

    def release(self) -> int:
        if self.proc.poll() is None:
            try:
                assert self.proc.stdin is not None
                self.proc.stdin.write("release\n")
                self.proc.stdin.flush()
            except BrokenPipeError:
                pass
        return self.wait()

    def wait(self) -> int:
        try:
            _out, err = self.proc.communicate(timeout=self.timeout)
            self.stderr += err or ""
        except subprocess.TimeoutExpired:
            self.proc.terminate()
            try:
                _out, err = self.proc.communicate(timeout=2)
                self.stderr += err or ""
            except subprocess.TimeoutExpired:
                self.proc.kill()
                _out, err = self.proc.communicate(timeout=2)
                self.stderr += err or ""
        return int(self.proc.returncode if self.proc.returncode is not None else -signal.SIGKILL)


class Probe:
    def __init__(self, args: argparse.Namespace) -> None:
        self.args = args
        # Preserve procfs magic links: resolving their text can discard the
        # container namespace or held descriptor through which files are opened.
        self.mount = Path(args.mount).absolute() if args.mount else None
        self.evidence = Path(args.evidence).resolve()
        self.records: list[dict[str, Any]] = []
        self.failures: list[dict[str, Any]] = []
        self.children: list[Child] = []
        self.run_id = f"locks-smoke-{time.strftime('%Y%m%dT%H%M%SZ', time.gmtime())}-{os.getpid()}-{random.randrange(1_000_000):06d}"
        self.fixture_root: Path | None = None
        self.primary_path: Path | None = Path(args.path).absolute() if args.path else None
        self.secondary_path: Path | None = Path(args.second_path).absolute() if args.second_path else None
        self.created_fixture = False
        self.fixture_kept = True

    def setup(self) -> None:
        self.evidence.mkdir(parents=True, exist_ok=True)
        if platform.system() != "Linux":
            raise RuntimeError("locks_smoke requires Linux")
        if self.primary_path is not None:
            self.primary_path.parent.mkdir(parents=True, exist_ok=True)
            self.primary_path.write_bytes(b"afs-locks-smoke\n" + (b"x" * 4096))
            if self.secondary_path is None:
                self.secondary_path = self.primary_path
            self.created_fixture = False
            return
        if self.mount is None:
            raise RuntimeError("--mount or --path is required")
        _expect(self.mount.is_dir(), "mount path is a directory", {"mount": str(self.mount)})
        self.fixture_root = self.mount / f".afs-locks-smoke-{self.run_id}"
        self.fixture_root.mkdir(mode=0o700)
        self.primary_path = self.fixture_root / "lock-target.bin"
        self.primary_path.write_bytes(b"afs-locks-smoke\n" + (b"x" * 4096))
        if self.args.second_mount:
            second_mount = Path(self.args.second_mount).absolute()
            self.secondary_path = second_mount / self.fixture_root.name / "lock-target.bin"
        else:
            self.secondary_path = self.primary_path
        self.created_fixture = True
        if self.secondary_path != self.primary_path:
            for _ in range(50):
                if self.secondary_path.exists():
                    break
                time.sleep(0.1)
            _expect(self.secondary_path.exists(), "second mount sees primary fixture path", {"primary": str(self.primary_path), "secondary": str(self.secondary_path)})
            _expect(self.secondary_path.read_bytes().startswith(b"afs-locks-smoke"), "second mount reads fixture data")

    def cleanup(self) -> None:
        for child in self.children:
            if child.proc.poll() is None:
                child.wait()
        if self.failures or self.args.keep_fixture:
            self.fixture_kept = True
            return
        if self.created_fixture and self.fixture_root is not None:
            shutil.rmtree(self.fixture_root, ignore_errors=True)
        self.fixture_kept = False

    def write_report(self) -> None:
        self.evidence.mkdir(parents=True, exist_ok=True)
        findmnt_primary = self._findmnt(self.primary_path) if self.primary_path else None
        findmnt_secondary = self._findmnt(self.secondary_path) if self.secondary_path else None
        processes: dict[str, Any] = {}
        for assignment in self.args.process:
            name, pid, source = assignment
            processes[name] = _process_identity(pid, name, source)
        report = {
            "schema": "afs.locks_smoke.v1",
            "created_at": _now(),
            "run_id": self.run_id,
            "fixture_root": str(self.fixture_root) if self.fixture_root else None,
            "fixture_kept": self.fixture_kept,
            "primary_path": str(self.primary_path) if self.primary_path else None,
            "secondary_path": str(self.secondary_path) if self.secondary_path else None,
            "same_path": self.primary_path == self.secondary_path,
            "cross_path_probe": self.primary_path != self.secondary_path,
            "platform": {
                "system": platform.system(),
                "release": platform.release(),
                "machine": platform.machine(),
                "python": platform.python_version(),
                "uid": os.geteuid(),
                "gid": os.getegid(),
            },
            "probe_process": _process_identity(os.getpid(), "locks_smoke", "self"),
            "observed_processes": processes,
            "findmnt": {"primary": findmnt_primary, "secondary": findmnt_secondary},
            "summary": {
                "status": "PASS" if not self.failures else "FAIL",
                "steps": len(self.records),
                "passed": sum(1 for record in self.records if record.get("ok")),
                "failed": len(self.failures),
            },
            "steps": self.records,
            "failures": self.failures,
            "notes": [
                "This is a short real Linux advisory-lock probe, not a complete POSIX lock suite.",
                "Failures with ENOSYS/EOPNOTSUPP indicate missing filesystem/FUSE lock support for this probe.",
                "When primary and secondary paths differ, this script proves only those two paths' observed lock behavior; the caller supplies the cross-mount fixture identity.",
            ],
        }
        (self.evidence / "report.json").write_text(json.dumps(report, indent=2, sort_keys=True) + "\n")
        (self.evidence / "summary.txt").write_text(
            f"status={report['summary']['status']} steps={report['summary']['steps']} failed={report['summary']['failed']} "
            f"primary={self.primary_path} secondary={self.secondary_path} kept={self.fixture_kept}\n"
        )

    def _findmnt(self, path: Path | None) -> dict[str, Any] | None:
        if path is None:
            return None
        proc = subprocess.run(
            ["findmnt", "-T", str(path), "-o", "TARGET,SOURCE,FSTYPE,OPTIONS", "--json"],
            text=True,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            check=False,
            timeout=10,
        )
        return {"argv": proc.args, "returncode": proc.returncode, "stdout": proc.stdout, "stderr": proc.stderr}

    def child(self, role: str, op: str, path: Path, mode: str = "exclusive", start: int = 0, length: int = 10) -> Child:
        cmd = [
            sys.executable,
            str(Path(__file__).resolve()),
            "--child-op",
            op,
            "--path",
            str(path),
            "--mode",
            mode,
            "--start",
            str(start),
            "--length",
            str(length),
        ]
        proc = subprocess.Popen(cmd, text=True, stdin=subprocess.PIPE, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
        child = Child(proc, role, timeout=self.args.child_timeout)
        child.identity = _process_identity(proc.pid, role, "subprocess")
        self.children.append(child)
        return child

    def run_try(self, op: str, path: Path, mode: str = "exclusive", start: int = 0, length: int = 10) -> dict[str, Any]:
        child = self.child(f"{op}-{mode}-{start}-{length}", op, path, mode, start, length)
        event = child.read_event("RESULT")
        rc = child.wait()
        return {"event": event, "returncode": rc, "identity": child.identity, "stderr": child.stderr}

    def step(self, name: str, func: Callable[[], dict[str, Any]]) -> None:
        started = time.time()
        record: dict[str, Any] = {"name": name, "started_at": _now()}
        try:
            details = func()
            record.update({"ok": True, "exit_status": 0, "details": details})
        except Exception as exc:  # noqa: BLE001 - probe must serialize raw failure
            record.update({
                "ok": False,
                "exit_status": 1,
                "exception": type(exc).__name__,
                "message": str(exc),
                "traceback": traceback.format_exc(),
            })
            self.failures.append(record)
        finally:
            record["duration_ms"] = round((time.time() - started) * 1000, 3)
            self.records.append(record)

    def paths(self) -> tuple[Path, Path]:
        assert self.primary_path is not None and self.secondary_path is not None
        return self.primary_path, self.secondary_path

    def test_fcntl_conflict_nonoverlap_getlk(self) -> dict[str, Any]:
        primary, secondary = self.paths()
        holder = self.child("fcntl-holder", "hold_fcntl", primary, "exclusive", 0, 10)
        ready = holder.read_event("READY")
        conflict = self.run_try("try_fcntl", secondary, "exclusive", 0, 10)
        nonoverlap = self.run_try("try_fcntl", secondary, "exclusive", 10, 10)
        getlk = self.run_try("getlk", secondary, "exclusive", 0, 10)
        rc = holder.release()
        _expect(conflict["event"].get("ok") is False, "overlapping fcntl exclusive lock conflicts", conflict)
        _expect(_fcntl_conflict_errno_ok(conflict["event"].get("errno")), "conflict errno proves a held-lock conflict", conflict)
        _expect(nonoverlap["event"].get("ok") is True, "non-overlapping fcntl range succeeds", nonoverlap)
        result = getlk["event"].get("result", {})
        _expect(result.get("ok") is True and result.get("l_type") != fcntl.F_UNLCK, "F_GETLK reports conflicting lock", getlk)
        _expect(rc == 0, "holder exits cleanly", {"returncode": rc})
        return {"holder_ready": ready, "holder_identity": holder.identity, "conflict": conflict, "nonoverlap": nonoverlap, "getlk": getlk, "holder_returncode": rc}

    def test_fcntl_shared_exclusive(self) -> dict[str, Any]:
        primary, secondary = self.paths()
        holder = self.child("fcntl-shared-holder", "hold_fcntl", primary, "shared", 40, 10)
        ready = holder.read_event("READY")
        shared = self.run_try("try_fcntl", secondary, "shared", 40, 10)
        exclusive = self.run_try("try_fcntl", secondary, "exclusive", 40, 10)
        rc = holder.release()
        _expect(shared["event"].get("ok") is True, "second shared fcntl lock succeeds", shared)
        _expect(exclusive["event"].get("ok") is False, "exclusive fcntl lock conflicts with shared holder", exclusive)
        _expect(_fcntl_conflict_errno_ok(exclusive["event"].get("errno")), "exclusive fcntl conflict errno proves a held-lock conflict", exclusive)
        _expect(rc == 0, "shared holder exits cleanly", {"returncode": rc})
        return {"holder_ready": ready, "holder_identity": holder.identity, "shared": shared, "exclusive": exclusive, "holder_returncode": rc}

    def test_fcntl_blocking_waiter_wakeup(self) -> dict[str, Any]:
        primary, secondary = self.paths()
        holder = self.child("fcntl-block-holder", "hold_fcntl", primary, "exclusive", 80, 10)
        ready = holder.read_event("READY")
        waiter = self.child("fcntl-block-waiter", "wait_fcntl", secondary, "exclusive", 80, 10)
        waiting = waiter.read_event("WAITING")
        before_unlock = waiter.assert_no_event("ACQUIRED", 0.25)
        holder_rc = holder.release()
        acquired = waiter.read_event("ACQUIRED", timeout=self.args.child_timeout)
        waiter_rc = waiter.release()
        _expect(holder_rc == 0, "blocking holder exits cleanly", {"returncode": holder_rc})
        _expect(waiter_rc == 0, "blocking waiter exits cleanly", {"returncode": waiter_rc})
        return {"holder_ready": ready, "waiting": waiting, "before_unlock": before_unlock, "acquired": acquired, "holder_identity": holder.identity, "waiter_identity": waiter.identity}

    def test_fcntl_owner_exit_release(self) -> dict[str, Any]:
        primary, secondary = self.paths()
        holder = self.child("fcntl-exit-holder", "hold_fcntl", primary, "exclusive", 120, 10)
        ready = holder.read_event("READY")
        holder.proc.terminate()
        holder_rc = holder.wait()
        acquired = self.run_try("try_fcntl", secondary, "exclusive", 120, 10)
        _expect(acquired["event"].get("ok") is True, "fcntl lock released after owner process exits", acquired)
        return {"holder_ready": ready, "holder_identity": holder.identity, "holder_returncode": holder_rc, "post_exit_acquire": acquired}

    def test_fcntl_close_other_fd_releases_process_locks(self) -> dict[str, Any]:
        primary, secondary = self.paths()
        holder = self.child("fcntl-close-other", "fcntl_close_other_then_hold", primary, "exclusive", 160, 10)
        ready = holder.read_event("READY")
        acquired = self.run_try("try_fcntl", secondary, "exclusive", 160, 10)
        rc = holder.release()
        _expect(acquired["event"].get("ok") is True, "closing another fd releases POSIX process locks for that file", acquired)
        _expect(rc == 0, "close-other child exits cleanly", {"returncode": rc})
        return {"holder_ready": ready, "holder_identity": holder.identity, "post_close_acquire": acquired, "holder_returncode": rc}

    def test_flock_conflict_shared_and_dup(self) -> dict[str, Any]:
        primary, secondary = self.paths()
        ex_holder = self.child("flock-exclusive-holder", "hold_flock", primary, "exclusive")
        ex_ready = ex_holder.read_event("READY")
        ex_conflict = self.run_try("try_flock", secondary, "exclusive")
        ex_rc = ex_holder.release()
        _expect(ex_conflict["event"].get("ok") is False, "flock exclusive conflicts with exclusive holder", ex_conflict)
        _expect(_flock_conflict_errno_ok(ex_conflict["event"].get("errno")), "flock exclusive conflict errno proves a held-lock conflict", ex_conflict)
        _expect(ex_rc == 0, "flock exclusive holder exits cleanly", {"returncode": ex_rc})

        sh_holder = self.child("flock-shared-holder", "hold_flock", primary, "shared")
        sh_ready = sh_holder.read_event("READY")
        sh_ok = self.run_try("try_flock", secondary, "shared")
        sh_conflict = self.run_try("try_flock", secondary, "exclusive")
        sh_rc = sh_holder.release()
        _expect(sh_ok["event"].get("ok") is True, "second shared flock succeeds", sh_ok)
        _expect(sh_conflict["event"].get("ok") is False, "exclusive flock conflicts with shared holder", sh_conflict)
        _expect(_flock_conflict_errno_ok(sh_conflict["event"].get("errno")), "flock shared/exclusive conflict errno proves a held-lock conflict", sh_conflict)
        _expect(sh_rc == 0, "flock shared holder exits cleanly", {"returncode": sh_rc})

        dup_holder = self.child("flock-dup-holder", "flock_dup_close_then_hold", primary, "exclusive")
        dup_ready = dup_holder.read_event("READY")
        dup_conflict = self.run_try("try_flock", secondary, "exclusive")
        dup_rc = dup_holder.release()
        after_release = self.run_try("try_flock", secondary, "exclusive")
        _expect(dup_conflict["event"].get("ok") is False, "flock survives closing original fd while dup remains", dup_conflict)
        _expect(_flock_conflict_errno_ok(dup_conflict["event"].get("errno")), "flock dup conflict errno proves a held-lock conflict", dup_conflict)
        _expect(after_release["event"].get("ok") is True, "flock released after final duplicate closes", after_release)
        _expect(dup_rc == 0, "flock dup holder exits cleanly", {"returncode": dup_rc})

        return {
            "exclusive": {"ready": ex_ready, "holder_identity": ex_holder.identity, "conflict": ex_conflict, "returncode": ex_rc},
            "shared": {"ready": sh_ready, "holder_identity": sh_holder.identity, "shared_ok": sh_ok, "exclusive_conflict": sh_conflict, "returncode": sh_rc},
            "dup_close": {"ready": dup_ready, "holder_identity": dup_holder.identity, "conflict_while_dup_open": dup_conflict, "after_release": after_release, "returncode": dup_rc},
        }

    def test_flock_blocking_waiter_owner_exit(self) -> dict[str, Any]:
        primary, secondary = self.paths()
        holder = self.child("flock-block-holder", "hold_flock", primary, "exclusive")
        ready = holder.read_event("READY")
        waiter = self.child("flock-block-waiter", "wait_flock", secondary, "exclusive")
        waiting = waiter.read_event("WAITING")
        before_unlock = waiter.assert_no_event("ACQUIRED", 0.25)
        holder_rc = holder.release()
        acquired = waiter.read_event("ACQUIRED", timeout=self.args.child_timeout)
        waiter_rc = waiter.release()
        _expect(holder_rc == 0, "flock blocking holder exits cleanly", {"returncode": holder_rc})
        _expect(waiter_rc == 0, "flock blocking waiter exits cleanly", {"returncode": waiter_rc})

        exit_holder = self.child("flock-exit-holder", "hold_flock", primary, "exclusive")
        exit_ready = exit_holder.read_event("READY")
        exit_holder.proc.terminate()
        exit_rc = exit_holder.wait()
        after_exit = self.run_try("try_flock", secondary, "exclusive")
        _expect(after_exit["event"].get("ok") is True, "flock released after owner process exits", after_exit)
        return {
            "blocking": {"ready": ready, "waiting": waiting, "before_unlock": before_unlock, "acquired": acquired, "holder_identity": holder.identity, "waiter_identity": waiter.identity},
            "owner_exit": {"ready": exit_ready, "holder_identity": exit_holder.identity, "holder_returncode": exit_rc, "after_exit": after_exit},
        }

    def run(self) -> None:
        self.step("fixture_identity", self.test_fixture_identity)
        self.step("fcntl_conflict_nonoverlap_getlk", self.test_fcntl_conflict_nonoverlap_getlk)
        self.step("fcntl_shared_exclusive", self.test_fcntl_shared_exclusive)
        self.step("fcntl_blocking_waiter_wakeup", self.test_fcntl_blocking_waiter_wakeup)
        self.step("fcntl_owner_exit_release", self.test_fcntl_owner_exit_release)
        self.step("fcntl_close_other_fd_releases_process_locks", self.test_fcntl_close_other_fd_releases_process_locks)
        self.step("flock_conflict_shared_and_dup", self.test_flock_conflict_shared_and_dup)
        self.step("flock_blocking_waiter_owner_exit", self.test_flock_blocking_waiter_owner_exit)

    def test_fixture_identity(self) -> dict[str, Any]:
        primary, secondary = self.paths()
        p_stat = primary.stat()
        s_stat = secondary.stat()
        _expect(primary.exists() and secondary.exists(), "primary and secondary paths exist", {"primary": str(primary), "secondary": str(secondary)})
        _expect(primary.read_bytes() == secondary.read_bytes(), "primary and secondary paths read same fixture bytes")
        return {
            "primary": str(primary),
            "secondary": str(secondary),
            "same_path": primary == secondary,
            "primary_stat": {"dev": p_stat.st_dev, "ino": p_stat.st_ino, "size": p_stat.st_size, "mode": oct(p_stat.st_mode & 0o7777), "type_regular": stat.S_ISREG(p_stat.st_mode)},
            "secondary_stat": {"dev": s_stat.st_dev, "ino": s_stat.st_ino, "size": s_stat.st_size, "mode": oct(s_stat.st_mode & 0o7777), "type_regular": stat.S_ISREG(s_stat.st_mode)},
        }


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Run short Linux advisory lock smoke probe")
    parser.add_argument("--mount", help="mount/root directory where the probe creates an isolated fixture")
    parser.add_argument("--second-mount", help="optional second mount that should expose the same relative fixture path")
    parser.add_argument("--path", help="explicit primary file path; parent is created and file overwritten")
    parser.add_argument("--second-path", help="explicit secondary file path naming the same logical file")
    parser.add_argument("--evidence", required=False, default="/tmp/afs-locks-smoke-evidence")
    parser.add_argument("--child-timeout", type=float, default=5.0)
    parser.add_argument("--keep-fixture", action="store_true")
    parser.add_argument("--process", action="append", type=_parse_process_assignment, default=[], metavar="NAME=PID", help="record live process identity without reading cmdline/environ")
    parser.add_argument("--child-op", choices=["hold_fcntl", "wait_fcntl", "try_fcntl", "getlk", "fcntl_close_other_then_hold", "hold_flock", "wait_flock", "try_flock", "flock_dup_close_then_hold"], help=argparse.SUPPRESS)
    parser.add_argument("--mode", choices=["shared", "exclusive", "unlock"], default="exclusive", help=argparse.SUPPRESS)
    parser.add_argument("--start", type=int, default=0, help=argparse.SUPPRESS)
    parser.add_argument("--length", type=int, default=10, help=argparse.SUPPRESS)
    return parser


def main(argv: list[str]) -> int:
    parser = build_parser()
    args = parser.parse_args(argv)
    if args.child_op:
        return _child_main(args)
    probe = Probe(args)
    try:
        probe.setup()
        probe.run()
    except Exception as exc:  # noqa: BLE001
        probe.failures.append({
            "name": "setup_or_runner",
            "started_at": _now(),
            "ok": False,
            "exit_status": 1,
            "exception": type(exc).__name__,
            "message": str(exc),
            "traceback": traceback.format_exc(),
        })
    finally:
        probe.cleanup()
        probe.write_report()
    status = "PASS" if not probe.failures else "FAIL"
    print(f"status={status} evidence={probe.evidence} primary={probe.primary_path} secondary={probe.secondary_path}")
    return 0 if not probe.failures else 1


if __name__ == "__main__":
    raise SystemExit(main(sys.argv[1:]))
