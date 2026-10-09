#!/usr/bin/env python3
"""Bounded Linux cross-mount advisory-lock probe for AFS delivery.

The host side starts two worker command prefixes. Each worker prefix receives a
``worker`` subcommand plus operation arguments, so the same driver can run local
Linux ext4 selftests or route work through commands such as ``limactl shell``.
All real file IO and advisory-lock syscalls happen inside worker processes.
"""
from __future__ import annotations

import argparse
import fcntl
import hashlib
import json
import os
import platform
import random
import selectors
import signal
import subprocess
import sys
import time
import traceback
from pathlib import Path
from typing import Any, Callable

SCRIPT_DIR = Path(__file__).resolve().parent
if str(SCRIPT_DIR) not in sys.path:
    sys.path.insert(0, str(SCRIPT_DIR))

import locks_smoke  # noqa: E402


DEFAULT_PEER_REQUEST_TIMEOUT_SECONDS = 5.0
DEFAULT_MIN_WAIT_SECONDS = 6.0
DEFAULT_CHILD_TIMEOUT_SECONDS = 20.0
LINUX_EACCES = 13
LINUX_EAGAIN = 11
LINUX_EBADF = 9
LOCK_BYTES = b"afs-locks-cross\n" + (b"x" * 4096)


def _now() -> str:
    return time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime())


def _expect(condition: bool, message: str, details: dict[str, Any] | None = None) -> None:
    if not condition:
        raise AssertionError(json.dumps({"message": message, "details": details or {}}, sort_keys=True))


def _json_dumps(value: object) -> str:
    return json.dumps(value, sort_keys=True, separators=(",", ":"))


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


def _parse_named_path(value: str) -> tuple[str, str]:
    if "=" not in value:
        raise argparse.ArgumentTypeError("expected ROLE=PID_FILE")
    name, file_path = value.split("=", 1)
    if not locks_smoke.PROCESS_NAME_RE.fullmatch(name):
        raise argparse.ArgumentTypeError(f"invalid process role {name!r}")
    if not file_path:
        raise argparse.ArgumentTypeError("pid file path must not be empty")
    return name, file_path


def _collect_pid_file_identities(assignments: list[tuple[str, str]], kind: str) -> dict[str, Any]:
    identities: dict[str, Any] = {}
    for role, pid_file in assignments:
        raw = Path(pid_file).read_text(encoding="utf-8").strip()
        if not raw.isdecimal() or int(raw) <= 0:
            raise RuntimeError(f"{kind} pid file {pid_file} for {role} does not contain a positive pid")
        identities[role] = locks_smoke._process_identity(int(raw), role, pid_file)
    return identities


def _stat_json(path: Path) -> dict[str, Any]:
    stat_result = path.stat()
    return {
        "path": str(path),
        "dev": stat_result.st_dev,
        "ino": stat_result.st_ino,
        "mode": oct(stat_result.st_mode & 0o7777),
        "size": stat_result.st_size,
        "mtime_ns": stat_result.st_mtime_ns,
        "sha256": _sha256_path(path) if path.is_file() else None,
    }


def _stat_meta_json(path: Path) -> dict[str, Any]:
    stat_result = path.stat()
    return {
        "path": str(path),
        "dev": stat_result.st_dev,
        "ino": stat_result.st_ino,
        "mode": oct(stat_result.st_mode & 0o7777),
        "size": stat_result.st_size,
        "mtime_ns": stat_result.st_mtime_ns,
    }


def _mount_identity(path: Path) -> dict[str, Any]:
    proc = subprocess.run(
        ["findmnt", "-T", str(path), "-o", "TARGET,SOURCE,FSTYPE,OPTIONS", "--json"],
        text=True,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        timeout=10,
        check=False,
    )
    return {"argv": proc.args, "returncode": proc.returncode, "stdout": proc.stdout, "stderr": proc.stderr}


def _worker_identity(
    path: Path | None,
    process_pid_files: list[tuple[str, str]] | None = None,
    meta_process_pid_files: list[tuple[str, str]] | None = None,
) -> dict[str, Any]:
    identity: dict[str, Any] = {
        "created_at": _now(),
        "platform": {
            "system": platform.system(),
            "release": platform.release(),
            "machine": platform.machine(),
            "python": platform.python_version(),
            "uid": os.geteuid(),
            "gid": os.getegid(),
            "hostname": platform.node(),
        },
        "boot_id": _read_text_optional(Path("/proc/sys/kernel/random/boot_id")),
        "machine_id": _read_text_optional(Path("/etc/machine-id")),
        "process": locks_smoke._process_identity(os.getpid(), "locks_cross_worker", "self"),
    }
    if path is not None:
        identity["mount"] = _mount_identity(path)
    identity["product_processes"] = _collect_pid_file_identities(process_pid_files or [], "process")
    identity["meta_processes"] = _collect_pid_file_identities(meta_process_pid_files or [], "meta")
    return identity


def _emit(value: dict[str, Any]) -> None:
    print(json.dumps(value, sort_keys=True), flush=True)


def _fcntl_type(mode: str) -> int:
    if mode == "shared":
        return fcntl.F_RDLCK
    if mode == "exclusive":
        return fcntl.F_WRLCK
    raise ValueError(mode)


def _fcntl_conflict_errno_ok(value: int | None) -> bool:
    return value in (LINUX_EACCES, LINUX_EAGAIN)


def _flock_conflict_errno_ok(value: int | None) -> bool:
    return value == LINUX_EAGAIN


def _getlk_reports_conflict(result: dict[str, Any]) -> bool:
    if result.get("ok") is not True:
        return False
    l_type_name = result.get("l_type_name")
    if l_type_name is not None:
        return l_type_name in ("F_WRLCK", "F_RDLCK")
    return result.get("l_type") in (0, 1)  # Linux F_RDLCK / F_WRLCK


def _worker_result(op: str, path: Path | None, **extra: Any) -> dict[str, Any]:
    return {"event": "RESULT", "op": op, "identity": _worker_identity(path, extra.pop("process_pid_files", None), extra.pop("meta_process_pid_files", None)), **extra}


def _worker_main(args: argparse.Namespace) -> int:
    path = Path(args.path).resolve() if args.path else None
    op = args.op
    identity_kwargs = {
        "process_pid_files": args.process_pid_file,
        "meta_process_pid_files": args.meta_process_pid_file,
    }

    def safe_identity() -> dict[str, Any]:
        try:
            return _worker_identity(path, **identity_kwargs)
        except Exception as exc:  # noqa: BLE001 - preserve original worker failure
            return {"identity_error": {"exception": type(exc).__name__, "message": str(exc)}}

    try:
        if op == "identity":
            _emit(_worker_result(op, path, stat=_stat_json(path) if path and path.exists() else None, **identity_kwargs))
            return 0
        if op == "init":
            assert path is not None
            path.parent.mkdir(parents=True, exist_ok=True)
            fd = os.open(path, os.O_CREAT | os.O_TRUNC | os.O_RDWR, 0o666)
            try:
                os.write(fd, LOCK_BYTES)
                os.fsync(fd)
            finally:
                os.close(fd)
            _emit(_worker_result(op, path, stat=_stat_json(path), **identity_kwargs))
            return 0
        if op == "read_stat":
            assert path is not None
            data = path.read_bytes()
            _emit(_worker_result(op, path, stat=_stat_json(path), prefix=data[:32].hex(), **identity_kwargs))
            return 0
        if op == "unlink_path":
            assert path is not None
            existed = path.exists()
            if existed:
                path.unlink()
            _emit(_worker_result(op, path, existed=existed, removed=not path.exists(), **identity_kwargs))
            return 0
        if op == "try_fcntl":
            assert path is not None
            fd = os.open(path, os.O_RDWR)
            try:
                ok, err = locks_smoke._fcntl_set(fd, _fcntl_type(args.mode), args.start, args.length, blocking=False)
                if ok:
                    locks_smoke._fcntl_set(fd, fcntl.F_UNLCK, args.start, args.length, blocking=False)
                _emit(_worker_result(op, path, ok=ok, errno=err, errno_name=locks_smoke._errno_name(err), stat=_stat_json(path), **identity_kwargs))
                return 0 if ok or _fcntl_conflict_errno_ok(err) else 1
            finally:
                os.close(fd)
        if op == "try_readonly_write_lock":
            assert path is not None
            fd = os.open(path, os.O_RDONLY)
            try:
                ok, err = locks_smoke._fcntl_set(fd, fcntl.F_WRLCK, args.start, args.length, blocking=False)
                if ok:
                    locks_smoke._fcntl_set(fd, fcntl.F_UNLCK, args.start, args.length, blocking=False)
                _emit(_worker_result(op, path, ok=ok, errno=err, errno_name=locks_smoke._errno_name(err), stat=_stat_json(path), **identity_kwargs))
                return 0
            finally:
                os.close(fd)
        if op == "getlk":
            assert path is not None
            fd = os.open(path, os.O_RDWR)
            try:
                result = locks_smoke._fcntl_getlk(fd, _fcntl_type(args.mode), args.start, args.length)
                _emit(_worker_result(op, path, result=result, stat=_stat_json(path), **identity_kwargs))
                return 0
            finally:
                os.close(fd)
        if op == "hold_fcntl":
            assert path is not None
            fd = os.open(path, os.O_RDWR)
            try:
                ok, err = locks_smoke._fcntl_set(fd, _fcntl_type(args.mode), args.start, args.length, blocking=False)
                if not ok:
                    _emit({"event": "ERROR", "op": op, "errno": err, "errno_name": locks_smoke._errno_name(err), "identity": _worker_identity(path, **identity_kwargs)})
                    return 1
                _emit({"event": "READY", "op": op, "identity": _worker_identity(path, **identity_kwargs), "stat": _stat_meta_json(path)})
                sys.stdin.readline()
                locks_smoke._fcntl_set(fd, fcntl.F_UNLCK, args.start, args.length, blocking=False)
                return 0
            finally:
                os.close(fd)
        if op == "wait_fcntl":
            assert path is not None
            fd = os.open(path, os.O_RDWR)
            interrupted = False

            def _handle_alarm(_signum: int, _frame: object) -> None:
                nonlocal interrupted
                interrupted = True
                raise InterruptedError("alarm interrupted F_SETLKW")

            try:
                if args.alarm_after > 0:
                    signal.signal(signal.SIGALRM, _handle_alarm)
                    signal.setitimer(signal.ITIMER_REAL, args.alarm_after)
                started = time.monotonic()
                _emit({"event": "WAITING", "op": op, "identity": _worker_identity(path, **identity_kwargs), "stat": _stat_meta_json(path)})
                ok, err = locks_smoke._fcntl_set(fd, _fcntl_type(args.mode), args.start, args.length, blocking=True)
                waited = time.monotonic() - started
                signal.setitimer(signal.ITIMER_REAL, 0)
                if not ok:
                    _emit({"event": "ERROR", "op": op, "errno": err, "errno_name": locks_smoke._errno_name(err), "waited_seconds": waited, "identity": _worker_identity(path, **identity_kwargs)})
                    return 1
                _emit({"event": "ACQUIRED", "op": op, "waited_seconds": waited, "identity": _worker_identity(path, **identity_kwargs), "stat": _stat_meta_json(path)})
                sys.stdin.readline()
                locks_smoke._fcntl_set(fd, fcntl.F_UNLCK, args.start, args.length, blocking=False)
                return 0
            except InterruptedError as exc:
                signal.setitimer(signal.ITIMER_REAL, 0)
                _emit({"event": "INTERRUPTED", "op": op, "message": str(exc), "interrupted": interrupted, "identity": _worker_identity(path, **identity_kwargs)})
                return 0
            finally:
                os.close(fd)
        if op == "fcntl_close_other_then_hold":
            assert path is not None
            fd = os.open(path, os.O_RDWR)
            fd2 = os.open(path, os.O_RDWR)
            try:
                ok, err = locks_smoke._fcntl_set(fd, fcntl.F_WRLCK, args.start, args.length, blocking=False)
                if not ok:
                    _emit({"event": "ERROR", "op": op, "errno": err, "errno_name": locks_smoke._errno_name(err), "identity": _worker_identity(path, **identity_kwargs)})
                    return 1
                os.close(fd2)
                fd2 = -1
                _emit({"event": "READY", "op": op, "closed_other_fd": True, "identity": _worker_identity(path, **identity_kwargs), "stat": _stat_meta_json(path)})
                sys.stdin.readline()
                return 0
            finally:
                if fd2 >= 0:
                    os.close(fd2)
                os.close(fd)
        if op == "hold_flock":
            assert path is not None
            fd = os.open(path, os.O_RDWR)
            try:
                fcntl.flock(fd, locks_smoke._flock_op(args.mode) | fcntl.LOCK_NB)
                _emit({"event": "READY", "op": op, "identity": _worker_identity(path, **identity_kwargs), "stat": _stat_json(path)})
                sys.stdin.readline()
                fcntl.flock(fd, fcntl.LOCK_UN)
                return 0
            finally:
                os.close(fd)
        if op == "try_flock":
            assert path is not None
            fd = os.open(path, os.O_RDWR)
            try:
                try:
                    fcntl.flock(fd, locks_smoke._flock_op(args.mode) | fcntl.LOCK_NB)
                    ok = True
                    err = None
                except OSError as exc:
                    ok = False
                    err = exc.errno
                if ok:
                    fcntl.flock(fd, fcntl.LOCK_UN)
                _emit(_worker_result(op, path, ok=ok, errno=err, errno_name=locks_smoke._errno_name(err), stat=_stat_json(path)))
                return 0 if ok or _flock_conflict_errno_ok(err) else 1
            finally:
                os.close(fd)
        if op == "flock_dup_close_then_hold":
            assert path is not None
            fd = os.open(path, os.O_RDWR)
            try:
                fcntl.flock(fd, fcntl.LOCK_EX | fcntl.LOCK_NB)
                dup_fd = os.dup(fd)
                os.close(fd)
                fd = dup_fd
                _emit({"event": "READY", "op": op, "closed_original_fd": True, "identity": _worker_identity(path, **identity_kwargs), "stat": _stat_json(path)})
                sys.stdin.readline()
                fcntl.flock(fd, fcntl.LOCK_UN)
                return 0
            finally:
                os.close(fd)
        if op == "fcntl_rename_unlink_recreate":
            assert path is not None
            renamed = Path(args.renamed_path).resolve() if args.renamed_path else path.with_name(path.name + ".held")
            fd = os.open(path, os.O_RDWR)
            try:
                ok, err = locks_smoke._fcntl_set(fd, fcntl.F_WRLCK, args.start, args.length, blocking=False)
                if not ok:
                    _emit({"event": "ERROR", "op": op, "errno": err, "errno_name": locks_smoke._errno_name(err), "identity": _worker_identity(path, **identity_kwargs)})
                    return 1
                if renamed.exists():
                    renamed.unlink()
                path.rename(renamed)
                with path.open("wb") as handle:
                    handle.write(b"afs-locks-cross-recreated\n")
                    handle.flush()
                    os.fsync(handle.fileno())
                _emit({
                    "event": "READY",
                    "op": op,
                    "identity": _worker_identity(path, **identity_kwargs),
                    "held_fd_stat": os.fstat(fd).st_ino,
                    "renamed_stat": _stat_meta_json(renamed),
                    "recreated_stat": _stat_json(path),
                })
                line = sys.stdin.readline().strip()
                unlinked = False
                if line == "unlink":
                    renamed.unlink()
                    unlinked = True
                    _emit({"event": "UNLINKED", "op": op, "renamed_path": str(renamed), "identity": _worker_identity(path, **identity_kwargs)})
                    sys.stdin.readline()
                locks_smoke._fcntl_set(fd, fcntl.F_UNLCK, args.start, args.length, blocking=False)
                return 0 if unlinked or line in ("release", "") else 1
            finally:
                os.close(fd)
        raise RuntimeError(f"unknown worker op {op}")
    except OSError as exc:
        _emit({"event": "ERROR", "op": op, "exception": type(exc).__name__, "message": str(exc), "errno": exc.errno, "errno_name": locks_smoke._errno_name(exc.errno), "identity": safe_identity()})
        return 1
    except Exception as exc:  # noqa: BLE001
        _emit({"event": "ERROR", "op": op, "exception": type(exc).__name__, "message": str(exc), "traceback": traceback.format_exc(), "identity": safe_identity()})
        return 1


class WorkerProcess:
    def __init__(self, name: str, argv: list[str], proc: subprocess.Popen[str], timeout: float) -> None:
        self.name = name
        self.argv = argv
        self.proc = proc
        self.timeout = timeout
        self.events: list[dict[str, Any]] = []
        self.stderr = ""

    def read_event(self, expected: str | None = None, timeout: float | None = None) -> dict[str, Any]:
        deadline = time.time() + (timeout if timeout is not None else self.timeout)
        while time.time() < deadline:
            if self.proc.stdout is None:
                break
            with selectors.DefaultSelector() as selector:
                selector.register(self.proc.stdout, selectors.EVENT_READ)
                if not selector.select(max(0, deadline - time.time())):
                    break
            line = self.proc.stdout.readline()
            if line:
                event = json.loads(line)
                self.events.append(event)
                if expected is None or event.get("event") == expected:
                    return event
                if event.get("event") == "ERROR":
                    raise RuntimeError(_json_dumps({"worker": self.name, "event": event}))
                continue
            if self.proc.poll() is not None:
                break
            time.sleep(0.02)
        raise TimeoutError(f"{self.name} did not emit {expected or 'event'} within {timeout or self.timeout}s")

    def send(self, line: str) -> None:
        if self.proc.poll() is None:
            assert self.proc.stdin is not None
            self.proc.stdin.write(line + "\n")
            self.proc.stdin.flush()

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

    def release(self) -> int:
        self.send("release")
        return self.wait()


class CrossProbe:
    def __init__(self, args: argparse.Namespace) -> None:
        self.args = args
        self.worker_a = args.worker_a_json
        self.worker_b = args.worker_b_json
        self.path_a = args.path_a
        self.path_b = args.path_b
        self.renamed_a = args.renamed_a or (args.path_a + ".held")
        self.renamed_b = args.renamed_b or (args.path_b + ".held")
        self.evidence = Path(args.evidence).resolve()
        self.records: list[dict[str, Any]] = []
        self.failures: list[dict[str, Any]] = []
        self.children: list[WorkerProcess] = []
        self.commands: list[dict[str, Any]] = []
        self.run_id = f"locks-cross-{time.strftime('%Y%m%dT%H%M%SZ', time.gmtime())}-{os.getpid()}-{random.randrange(1_000_000):06d}"
        self.fixture_kept = True
        self.identity: dict[str, Any] = {}

    def pid_file_args(self, worker: str) -> list[str]:
        if worker == "A":
            process_files = self.args.worker_a_process_pid_file
            meta_files = self.args.worker_a_meta_process_pid_file
        else:
            process_files = self.args.worker_b_process_pid_file
            meta_files = self.args.worker_b_meta_process_pid_file
        args: list[str] = []
        for role, pid_file in process_files:
            args += ["--process-pid-file", f"{role}={pid_file}"]
        for role, pid_file in meta_files:
            args += ["--meta-process-pid-file", f"{role}={pid_file}"]
        return args

    def argv(self, worker: str, op: str, path: str | None = None, **kwargs: Any) -> list[str]:
        prefix = self.worker_a if worker == "A" else self.worker_b
        cmd = list(prefix) + ["worker", "--op", op] + self.pid_file_args(worker)
        if path is not None:
            cmd += ["--path", path]
        for key, value in kwargs.items():
            option = "--" + key.replace("_", "-")
            if value is None:
                continue
            cmd += [option, str(value)]
        return cmd

    def run_cmd(self, worker: str, op: str, path: str | None = None, **kwargs: Any) -> dict[str, Any]:
        argv = self.argv(worker, op, path, **kwargs)
        started = time.time()
        proc = subprocess.run(
            argv,
            text=True,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            timeout=self.args.command_timeout,
            check=False,
        )
        events = [json.loads(line) for line in proc.stdout.splitlines() if line.strip()]
        record = {
            "worker": worker,
            "op": op,
            "argv": argv,
            "returncode": proc.returncode,
            "stdout": proc.stdout,
            "stderr": proc.stderr,
            "json_events": events,
            "duration_ms": round((time.time() - started) * 1000, 3),
        }
        self.commands.append(record)
        if proc.returncode != 0 and not events:
            raise RuntimeError(_json_dumps({"worker": worker, "op": op, "returncode": proc.returncode, "stderr": proc.stderr}))
        if events and events[-1].get("event") == "ERROR":
            raise RuntimeError(_json_dumps({"worker": worker, "op": op, "event": events[-1]}))
        return record

    def start_worker(self, name: str, worker: str, op: str, path: str, **kwargs: Any) -> WorkerProcess:
        argv = self.argv(worker, op, path, **kwargs)
        proc = subprocess.Popen(argv, text=True, stdin=subprocess.PIPE, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
        child = WorkerProcess(name, argv, proc, self.args.child_timeout)
        self.children.append(child)
        self.commands.append({"worker": worker, "op": op, "argv": argv, "streaming": True, "name": name})
        return child

    def setup(self) -> None:
        self.evidence.mkdir(parents=True, exist_ok=True)
        if platform.system() != "Linux" and not self.args.allow_host_non_linux:
            raise RuntimeError("locks_cross host selftests require Linux; use --allow-host-non-linux only for orchestration")
        _expect(
            self.args.min_wait_seconds > self.args.peer_request_timeout,
            "--min-wait-seconds must be greater than --peer-request-timeout",
            self.timeout_config(),
        )
        self.run_cmd("A", "init", self.path_a)
        self.run_cmd("B", "read_stat", self.path_b)
        a = self.run_cmd("A", "identity", self.path_a)["json_events"][-1]
        b = self.run_cmd("B", "identity", self.path_b)["json_events"][-1]
        self.identity = {
            "worker_a": a,
            "worker_b": b,
            "paths_differ": self.path_a != self.path_b,
            "same_kernel": a.get("identity", {}).get("boot_id") == b.get("identity", {}).get("boot_id"),
            "timeout_config": self.timeout_config(),
        }
        self.identity["qualification"] = self.qualification(a, b)
        self.identity["cross_mount_qualified"] = all(self.identity["qualification"].values())
        if self.args.require_cross_mount:
            _expect(self.identity["cross_mount_qualified"], "workers are qualified cross-mount evidence", self.identity)

    def timeout_config(self) -> dict[str, Any]:
        return {
            "peer_request_timeout_seconds": self.args.peer_request_timeout,
            "peer_request_timeout_source": self.args.peer_request_timeout_source,
            "min_wait_seconds": self.args.min_wait_seconds,
            "child_timeout_seconds": self.args.child_timeout,
            "command_timeout_seconds": self.args.command_timeout,
        }

    def mount_is_afs_fuse(self, event: dict[str, Any]) -> bool:
        mount = event.get("identity", {}).get("mount", {})
        if mount.get("returncode") != 0:
            return False
        try:
            filesystems = json.loads(mount.get("stdout") or "{}").get("filesystems", [])
        except json.JSONDecodeError:
            return False
        if not filesystems:
            return False
        fs = filesystems[0]
        fstype = str(fs.get("fstype", "")).lower()
        source = str(fs.get("source", "")).lower()
        options = str(fs.get("options", "")).lower()
        return fstype.startswith("fuse") and ("afs" in fstype or "afs" in source or "afs" in options)

    def has_product_process_identity(self, event: dict[str, Any]) -> bool:
        identities = event.get("identity", {})
        return bool(identities.get("product_processes"))

    def has_meta_identity(self, *events: dict[str, Any]) -> bool:
        return any(bool(event.get("identity", {}).get("meta_processes")) for event in events)

    def qualification(self, a: dict[str, Any], b: dict[str, Any]) -> dict[str, bool]:
        return {
            "paths_differ": self.path_a != self.path_b,
            "different_worker_kernels": a.get("identity", {}).get("boot_id") != b.get("identity", {}).get("boot_id"),
            "worker_a_afs_fuse": self.mount_is_afs_fuse(a),
            "worker_b_afs_fuse": self.mount_is_afs_fuse(b),
            "worker_a_product_process_identity": self.has_product_process_identity(a),
            "worker_b_product_process_identity": self.has_product_process_identity(b),
            "meta_process_identity_present": self.has_meta_identity(a, b),
        }

    def cleanup(self) -> None:
        for child in self.children:
            if child.proc.poll() is None:
                child.wait()
        if self.failures or self.args.keep_fixture:
            self.fixture_kept = True
            return
        for worker, path in (("A", self.path_a), ("A", self.renamed_a)):
            try:
                self.run_cmd(worker, "unlink_path", path)
            except Exception:
                pass
        self.fixture_kept = False

    def write_report(self) -> None:
        report = {
            "schema": "afs.locks_cross.v1",
            "created_at": _now(),
            "run_id": self.run_id,
            "status": "PASS" if not self.failures else "FAIL",
            "cross_mount_qualified": self.identity.get("cross_mount_qualified", False),
            "same_kernel_reference_only": bool(self.identity and not self.identity.get("cross_mount_qualified", False)),
            "path_a": self.path_a,
            "path_b": self.path_b,
            "renamed_a": self.renamed_a,
            "renamed_b": self.renamed_b,
            "fixture_kept": self.fixture_kept,
            "worker_commands": {"A": self.worker_a, "B": self.worker_b},
            "timeout_config": self.timeout_config(),
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
            "summary": {
                "steps": len(self.records),
                "passed": sum(1 for record in self.records if record.get("ok")),
                "failed": len(self.failures),
                "commands": len(self.commands),
            },
            "steps": self.records,
            "failures": self.failures,
            "commands": self.commands,
            "children": [
                {"name": child.name, "events": child.events, "stderr": child.stderr,
                 "returncode": child.proc.returncode}
                for child in self.children
            ],
            "notes": [
                "PASS means the requested behavior checks passed for the observed worker/path pair.",
                "cross_mount_qualified=false is reference evidence only and must not be counted as cross-mount AFS PASS.",
                "Product qualification requires different worker kernels, AFS FUSE mount identity on both workers, product process identity on both workers and at least one Meta process identity.",
                "All file IO and advisory-lock syscalls are executed by worker subprocesses.",
            ],
        }
        self.evidence.mkdir(parents=True, exist_ok=True)
        (self.evidence / "report.json").write_text(json.dumps(report, indent=2, sort_keys=True) + "\n", encoding="utf-8")
        (self.evidence / "summary.txt").write_text(
            f"status={report['status']} cross_mount_qualified={report['cross_mount_qualified']} "
            f"same_kernel_reference_only={report['same_kernel_reference_only']} steps={report['summary']['steps']} "
            f"failed={report['summary']['failed']} evidence={self.evidence}\n",
            encoding="utf-8",
        )

    def step(self, name: str, func: Callable[[], dict[str, Any]]) -> None:
        started = time.time()
        record: dict[str, Any] = {"name": name, "started_at": _now()}
        try:
            record.update({"ok": True, "details": func()})
        except Exception as exc:  # noqa: BLE001
            record.update({"ok": False, "exception": type(exc).__name__, "message": str(exc), "traceback": traceback.format_exc()})
            self.failures.append(record)
        finally:
            record["duration_ms"] = round((time.time() - started) * 1000, 3)
            self.records.append(record)

    def event(self, record: dict[str, Any]) -> dict[str, Any]:
        events = record["json_events"]
        _expect(bool(events), "worker emitted JSON", record)
        return events[-1]

    def test_fixture_identity(self) -> dict[str, Any]:
        a = self.event(self.run_cmd("A", "read_stat", self.path_a))
        b = self.event(self.run_cmd("B", "read_stat", self.path_b))
        _expect(a["stat"]["sha256"] == b["stat"]["sha256"], "workers read same logical fixture bytes", {"a": a, "b": b})
        return {"a": a, "b": b, "cross_mount_qualified": self.identity.get("cross_mount_qualified", False)}

    def test_posix_overlap_getlk(self) -> dict[str, Any]:
        holder = self.start_worker("A-fcntl-holder", "A", "hold_fcntl", self.path_a, mode="exclusive", start=0, length=10)
        ready = holder.read_event("READY")
        conflict = self.event(self.run_cmd("B", "try_fcntl", self.path_b, mode="exclusive", start=0, length=10))
        nonoverlap = self.event(self.run_cmd("B", "try_fcntl", self.path_b, mode="exclusive", start=10, length=10))
        getlk = self.event(self.run_cmd("B", "getlk", self.path_b, mode="exclusive", start=0, length=10))
        rc = holder.release()
        _expect(conflict.get("ok") is False, "overlapping POSIX fcntl lock conflicts", conflict)
        _expect(_fcntl_conflict_errno_ok(conflict.get("errno")), "fcntl conflict errno is EACCES/EAGAIN", conflict)
        _expect(nonoverlap.get("ok") is True, "non-overlapping POSIX range succeeds", nonoverlap)
        _expect(_getlk_reports_conflict(getlk.get("result", {})), "F_GETLK reports conflicting lock", getlk)
        _expect(rc == 0, "holder exits cleanly", {"returncode": rc, "stderr": holder.stderr})
        return {"ready": ready, "conflict": conflict, "nonoverlap": nonoverlap, "getlk": getlk, "holder_returncode": rc}

    def test_setlkw_wake_and_interrupt(self) -> dict[str, Any]:
        holder = self.start_worker("A-fcntl-wake-holder", "A", "hold_fcntl", self.path_a, mode="exclusive", start=40, length=10)
        ready = holder.read_event("READY")
        waiter = self.start_worker("B-fcntl-wake-waiter", "B", "wait_fcntl", self.path_b, mode="exclusive", start=40, length=10)
        waiting = waiter.read_event("WAITING")
        time.sleep(self.args.min_wait_seconds)
        _expect(waiter.proc.poll() is None, "SETLKW waiter remains blocked past generic peer timeout", {"min_wait_seconds": self.args.min_wait_seconds})
        holder_rc = holder.release()
        acquired = waiter.read_event("ACQUIRED", timeout=self.args.child_timeout)
        waiter_rc = waiter.release()
        _expect(acquired.get("waited_seconds", 0) >= self.args.peer_request_timeout, "SETLKW waited longer than configured peer request timeout", {"acquired": acquired, "timeout_config": self.timeout_config()})
        _expect(holder_rc == 0 and waiter_rc == 0, "SETLKW holder and waiter exit cleanly", {"holder": holder_rc, "waiter": waiter_rc})

        interrupt_holder = self.start_worker("A-fcntl-interrupt-holder", "A", "hold_fcntl", self.path_a, mode="exclusive", start=60, length=10)
        interrupt_ready = interrupt_holder.read_event("READY")
        interruptee = self.start_worker("B-fcntl-interrupt-waiter", "B", "wait_fcntl", self.path_b, mode="exclusive", start=60, length=10, alarm_after=0.2)
        interrupted = interruptee.read_event("INTERRUPTED", timeout=self.args.child_timeout)
        interrupt_waiter_rc = interruptee.wait()
        interrupt_holder_rc = interrupt_holder.release()
        _expect(interrupt_waiter_rc == 0 and interrupted.get("interrupted") is True, "SETLKW waiter reports signal interruption", interrupted)
        _expect(interrupt_holder_rc == 0, "interrupt holder exits cleanly", {"returncode": interrupt_holder_rc})
        return {
            "wake": {"ready": ready, "waiting": waiting, "acquired": acquired, "holder_returncode": holder_rc, "waiter_returncode": waiter_rc},
            "interrupt": {"ready": interrupt_ready, "interrupted": interrupted, "holder_returncode": interrupt_holder_rc, "waiter_returncode": interrupt_waiter_rc},
        }

    def test_any_fd_close_releases_posix(self) -> dict[str, Any]:
        holder = self.start_worker("A-fcntl-close-other", "A", "fcntl_close_other_then_hold", self.path_a, mode="exclusive", start=80, length=10)
        ready = holder.read_event("READY")
        acquired = self.event(self.run_cmd("B", "try_fcntl", self.path_b, mode="exclusive", start=80, length=10))
        rc = holder.release()
        _expect(acquired.get("ok") is True, "closing any fd for the inode releases POSIX process locks", acquired)
        _expect(rc == 0, "close-other holder exits cleanly", {"returncode": rc})
        return {"ready": ready, "acquired_after_other_fd_close": acquired, "holder_returncode": rc}

    def test_flock_dup_final_close_and_posix_independence(self) -> dict[str, Any]:
        dup = self.start_worker("A-flock-dup", "A", "flock_dup_close_then_hold", self.path_a, mode="exclusive")
        dup_ready = dup.read_event("READY")
        conflict = self.event(self.run_cmd("B", "try_flock", self.path_b, mode="exclusive"))
        fcntl_independent = self.event(self.run_cmd("B", "try_fcntl", self.path_b, mode="exclusive", start=120, length=10))
        dup_rc = dup.release()
        after = self.event(self.run_cmd("B", "try_flock", self.path_b, mode="exclusive"))
        _expect(conflict.get("ok") is False, "flock survives closing original fd while duplicate remains", conflict)
        _expect(_flock_conflict_errno_ok(conflict.get("errno")), "flock conflict errno is EAGAIN/EWOULDBLOCK", conflict)
        _expect(fcntl_independent.get("ok") is True, "POSIX fcntl lock remains independent of BSD flock", fcntl_independent)
        _expect(after.get("ok") is True, "flock releases after final duplicate fd closes", after)
        _expect(dup_rc == 0, "flock dup holder exits cleanly", {"returncode": dup_rc})

        posix = self.start_worker("A-posix-independence", "A", "hold_fcntl", self.path_a, mode="exclusive", start=140, length=10)
        posix_ready = posix.read_event("READY")
        flock_independent = self.event(self.run_cmd("B", "try_flock", self.path_b, mode="exclusive"))
        posix_rc = posix.release()
        _expect(flock_independent.get("ok") is True, "BSD flock remains independent of POSIX fcntl locks", flock_independent)
        _expect(posix_rc == 0, "POSIX independence holder exits cleanly", {"returncode": posix_rc})
        return {
            "flock_dup": {"ready": dup_ready, "conflict": conflict, "fcntl_independent": fcntl_independent, "after_final_close": after, "returncode": dup_rc},
            "posix_independence": {"ready": posix_ready, "flock_independent": flock_independent, "returncode": posix_rc},
        }

    def test_rename_unlink_open_inode_recreated_path(self) -> dict[str, Any]:
        holder = self.start_worker(
            "A-rename-unlink-holder",
            "A",
            "fcntl_rename_unlink_recreate",
            self.path_a,
            mode="exclusive",
            start=180,
            length=10,
            renamed_path=self.renamed_a,
        )
        ready = holder.read_event("READY")
        new_path = self.event(self.run_cmd("B", "try_fcntl", self.path_b, mode="exclusive", start=180, length=10))
        renamed_path = self.event(self.run_cmd("B", "try_fcntl", self.renamed_b, mode="exclusive", start=180, length=10))
        holder.send("unlink")
        unlinked = holder.read_event("UNLINKED")
        recreated_after_unlink = self.event(self.run_cmd("B", "try_fcntl", self.path_b, mode="exclusive", start=180, length=10))
        holder_rc = holder.release()
        _expect(new_path.get("ok") is True, "recreated path is a new inode and does not inherit old open-inode lock", new_path)
        _expect(renamed_path.get("ok") is False, "renamed path still names locked open inode before unlink", renamed_path)
        _expect(recreated_after_unlink.get("ok") is True, "recreated path remains independent after locked inode is unlinked", recreated_after_unlink)
        _expect(holder_rc == 0, "rename/unlink holder exits cleanly", {"returncode": holder_rc, "stderr": holder.stderr})
        return {"ready": ready, "new_path": new_path, "renamed_path": renamed_path, "unlinked": unlinked, "recreated_after_unlink": recreated_after_unlink}

    def test_readonly_fd_write_lock_ebadf(self) -> dict[str, Any]:
        result = self.event(self.run_cmd("B", "try_readonly_write_lock", self.path_b, start=220, length=10))
        _expect(result.get("ok") is False and result.get("errno") == LINUX_EBADF, "write lock through readonly fd fails with EBADF", result)
        return {"readonly_write_lock": result}

    def run(self) -> None:
        self.step("fixture_identity", self.test_fixture_identity)
        self.step("posix_overlap_conflict_getlk", self.test_posix_overlap_getlk)
        self.step("setlkw_wake_interrupt_timeout_bound", self.test_setlkw_wake_and_interrupt)
        self.step("any_fd_close_releases_posix", self.test_any_fd_close_releases_posix)
        self.step("flock_dup_final_close_posix_independence", self.test_flock_dup_final_close_and_posix_independence)
        self.step("rename_unlink_open_inode_recreated_path", self.test_rename_unlink_open_inode_recreated_path)
        self.step("readonly_fd_write_lock_ebadf", self.test_readonly_fd_write_lock_ebadf)


def _json_argv(value: str) -> list[str]:
    parsed = json.loads(value)
    if not isinstance(parsed, list) or not all(isinstance(item, str) and item for item in parsed):
        raise argparse.ArgumentTypeError("expected JSON array of non-empty argv strings")
    return parsed


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Run bounded cross-mount Linux advisory-lock probe")
    sub = parser.add_subparsers(dest="command")

    host = sub.add_parser("host", help="orchestrate two worker command prefixes")
    host.add_argument("--worker-a-json", required=True, type=_json_argv, help="JSON argv prefix for worker A, for example [\"python3\",\".../locks_cross.py\"]")
    host.add_argument("--worker-b-json", required=True, type=_json_argv, help="JSON argv prefix for worker B")
    host.add_argument("--path-a", required=True, help="worker A path for the logical test file")
    host.add_argument("--path-b", required=True, help="worker B path for the same logical test file")
    host.add_argument("--renamed-a", help="worker A path used for rename/open-inode case")
    host.add_argument("--renamed-b", help="worker B view of --renamed-a")
    host.add_argument("--evidence", required=True)
    host.add_argument("--command-timeout", type=float, default=10.0)
    host.add_argument("--child-timeout", type=float, default=DEFAULT_CHILD_TIMEOUT_SECONDS)
    host.add_argument("--peer-request-timeout", type=float, default=DEFAULT_PEER_REQUEST_TIMEOUT_SECONDS)
    host.add_argument("--peer-request-timeout-source", default="default common transport gRPC request timeout")
    host.add_argument("--min-wait-seconds", type=float, default=DEFAULT_MIN_WAIT_SECONDS)
    host.add_argument("--worker-a-process-pid-file", action="append", type=_parse_named_path, default=[], metavar="ROLE=PID_FILE")
    host.add_argument("--worker-b-process-pid-file", action="append", type=_parse_named_path, default=[], metavar="ROLE=PID_FILE")
    host.add_argument("--worker-a-meta-process-pid-file", action="append", type=_parse_named_path, default=[], metavar="ROLE=PID_FILE")
    host.add_argument("--worker-b-meta-process-pid-file", action="append", type=_parse_named_path, default=[], metavar="ROLE=PID_FILE")
    host.add_argument("--keep-fixture", action="store_true")
    host.add_argument("--require-cross-mount", action="store_true", help="fail unless workers are different-kernel cross-mount evidence")
    host.add_argument("--allow-host-non-linux", action="store_true", help="allow macOS host orchestration; workers must still be Linux")

    worker = sub.add_parser("worker", help=argparse.SUPPRESS)
    worker.add_argument("--op", required=True)
    worker.add_argument("--path")
    worker.add_argument("--renamed-path")
    worker.add_argument("--mode", choices=["shared", "exclusive"], default="exclusive")
    worker.add_argument("--start", type=int, default=0)
    worker.add_argument("--length", type=int, default=10)
    worker.add_argument("--alarm-after", type=float, default=0.0)
    worker.add_argument("--process-pid-file", action="append", type=_parse_named_path, default=[], metavar="ROLE=PID_FILE")
    worker.add_argument("--meta-process-pid-file", action="append", type=_parse_named_path, default=[], metavar="ROLE=PID_FILE")
    return parser


def main(argv: list[str]) -> int:
    parser = build_parser()
    args = parser.parse_args(argv)
    if args.command == "worker":
        if platform.system() != "Linux":
            _emit({"event": "ERROR", "op": args.op, "message": "locks_cross worker requires Linux", "identity": {"platform": platform.system()}})
            return 1
        return _worker_main(args)
    if args.command != "host":
        parser.print_help(sys.stderr)
        return 2
    probe = CrossProbe(args)
    try:
        probe.setup()
        probe.run()
    except Exception as exc:  # noqa: BLE001
        probe.failures.append({
            "name": "setup_or_runner",
            "started_at": _now(),
            "ok": False,
            "exception": type(exc).__name__,
            "message": str(exc),
            "traceback": traceback.format_exc(),
        })
    finally:
        probe.cleanup()
        probe.write_report()
    status = "PASS" if not probe.failures else "FAIL"
    print(f"status={status} cross_mount_qualified={probe.identity.get('cross_mount_qualified', False)} evidence={probe.evidence}")
    return 0 if not probe.failures else 1


if __name__ == "__main__":
    raise SystemExit(main(sys.argv[1:]))
