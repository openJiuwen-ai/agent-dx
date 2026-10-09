#!/usr/bin/env python3
"""Bounded Linux A/B consistency probe for AFS OwnerFs and DFS.

The host/controller starts two explicit worker command prefixes. Workers perform
all filesystem IO on Linux. The controller only orchestrates ordering, records
raw command output, and writes evidence. It does not restart, signal, or mutate
AFS runtime processes.
"""
from __future__ import annotations

import argparse
import hashlib
import json
import os
import platform
import selectors
import random
import subprocess
import sys
import tempfile
import time
import traceback
from pathlib import Path
from typing import Any


SCHEMA = "afs.consistency_cross.v1"
DEFAULT_IDLE_SECONDS = 35.0
DEFAULT_COMMAND_TIMEOUT_SECONDS = 60.0
DEFAULT_CHILD_TIMEOUT_SECONDS = 90.0


def _now() -> str:
    return time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime())


def _sha256_bytes(data: bytes) -> str:
    return hashlib.sha256(data).hexdigest()


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


def _parse_expected_process(value: str) -> tuple[str, int, str]:
    # ROLE=PID:SHA256
    if "=" not in value or ":" not in value:
        raise argparse.ArgumentTypeError("expected ROLE=PID:SHA256")
    role, rest = value.split("=", 1)
    pid_text, sha = rest.split(":", 1)
    if not role or not all(ch.isalnum() or ch in "_.-" for ch in role):
        raise argparse.ArgumentTypeError(f"invalid process role {role!r}")
    if not pid_text.isdecimal() or int(pid_text) <= 0:
        raise argparse.ArgumentTypeError(f"invalid pid {pid_text!r}")
    if len(sha) != 64 or any(ch not in "0123456789abcdefABCDEF" for ch in sha):
        raise argparse.ArgumentTypeError(f"invalid sha256 {sha!r}")
    return role, int(pid_text), sha.lower()


def _parse_json_argv(value: str) -> list[str]:
    try:
        parsed = json.loads(value)
    except json.JSONDecodeError as exc:
        raise argparse.ArgumentTypeError(f"invalid JSON argv: {exc}") from exc
    if not isinstance(parsed, list) or not parsed or not all(isinstance(item, str) and item for item in parsed):
        raise argparse.ArgumentTypeError("expected non-empty JSON string array")
    return parsed


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


def _process_identity(role: str, pid: int, expected_sha256: str) -> dict[str, Any]:
    before = _process_fingerprint(pid)
    actual_sha = _sha256_path(Path("/proc") / str(pid) / "exe")
    after = _process_fingerprint(pid)
    for key in ("pid", "start_ticks", "exe_dev", "exe_inode"):
        if before[key] != after[key]:
            raise RuntimeError(f"process {role} changed while hashing: {key} {before[key]!r} -> {after[key]!r}")
    ok = actual_sha == expected_sha256.lower()
    return {
        "role": role,
        "pid": pid,
        "expected_sha256": expected_sha256.lower(),
        "sha256": actual_sha,
        "sha256_ok": ok,
        "exe_path": before["exe_path"],
        "start_ticks": before["start_ticks"],
        "exe_dev": before["exe_dev"],
        "exe_inode": before["exe_inode"],
    }


def _stat_json(path: Path) -> dict[str, Any]:
    st = path.stat()
    return {
        "path": str(path),
        "dev": st.st_dev,
        "ino": st.st_ino,
        "mode": oct(st.st_mode & 0o7777),
        "size": st.st_size,
        "mtime_ns": st.st_mtime_ns,
        "sha256": _sha256_path(path) if path.is_file() else None,
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


def _worker_identity(path: Path | None, expected_processes: list[tuple[str, int, str]]) -> dict[str, Any]:
    identities = {role: _process_identity(role, pid, sha) for role, pid, sha in expected_processes}
    result: dict[str, Any] = {
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
        "expected_processes": identities,
        "all_expected_processes_ok": all(item.get("sha256_ok") for item in identities.values()),
    }
    if path is not None:
        result["mount"] = _mount_identity(path)
    return result


def _emit(value: dict[str, Any]) -> None:
    print(json.dumps(value, sort_keys=True), flush=True)


def _read_all(path: Path) -> bytes:
    with path.open("rb") as handle:
        return handle.read()


def _write_json(path: Path, value: dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(value, indent=2, sort_keys=True) + "\n", encoding="utf-8")


def _expected_after_resize(payload: bytes, resize_to: int | None) -> bytes:
    if resize_to is None:
        return payload
    if resize_to <= len(payload):
        return payload[:resize_to]
    return payload + (b"\x00" * (resize_to - len(payload)))


def _result_event(op: str, path: Path | None, expected_processes: list[tuple[str, int, str]], **extra: Any) -> dict[str, Any]:
    return {"event": "RESULT", "op": op, "identity": _worker_identity(path, expected_processes), **extra}


def _same_mount_visible(path: Path) -> dict[str, Any]:
    started = time.time()
    path.parent.mkdir(parents=True, exist_ok=True)
    initial = b"0123456789abcdef\n"
    overwrite = b"HELLO"
    append = b"::APPEND::"
    final_size = 9
    with path.open("wb") as handle:
        handle.write(initial)
        handle.flush()
        os.fsync(handle.fileno())
    fd_ro = os.open(path, os.O_RDONLY)
    close_error = None
    try:
        before = os.pread(fd_ro, 4096, 0)
        fd_wr = os.open(path, os.O_WRONLY)
        try:
            os.pwrite(fd_wr, overwrite, 0)
            os.lseek(fd_wr, 0, os.SEEK_END)
            os.write(fd_wr, append)
            os.ftruncate(fd_wr, final_size)
            observed = os.pread(fd_ro, 4096, 0)
            stat_mid = os.stat(path)
        finally:
            try:
                os.close(fd_wr)
            except OSError as exc:
                close_error = {"errno": exc.errno, "strerror": exc.strerror}
        after_close = _read_all(path)
    finally:
        os.close(fd_ro)
    expected = (overwrite + initial[len(overwrite) :])[:final_size]
    return {
        "ok": observed == expected and after_close == expected and len(observed) == final_size and close_error is None,
        "before_hex": before.hex(),
        "observed_hex": observed.hex(),
        "after_close_hex": after_close.hex(),
        "expected_hex": expected.hex(),
        "observed_sha256": _sha256_bytes(observed),
        "expected_sha256": _sha256_bytes(expected),
        "stat_mid_size": stat_mid.st_size,
        "close_error": close_error,
        "duration_seconds": round(time.time() - started, 6),
    }


def _write_close(path: Path, payload: bytes, resize_to: int | None, fsync_before_close: bool) -> dict[str, Any]:
    started = time.time()
    path.parent.mkdir(parents=True, exist_ok=True)
    fd = os.open(path, os.O_CREAT | os.O_TRUNC | os.O_WRONLY, 0o644)
    close_error = None
    try:
        os.write(fd, payload)
        if resize_to is not None:
            os.ftruncate(fd, resize_to)
        if fsync_before_close:
            os.fsync(fd)
    finally:
        try:
            os.close(fd)
        except OSError as exc:
            close_error = {"errno": exc.errno, "strerror": exc.strerror}
    data = _read_all(path)
    st = os.stat(path)
    expected = _expected_after_resize(payload, resize_to)
    return {
        "ok": close_error is None and data == expected and st.st_size == len(expected),
        "fsync_before_close": fsync_before_close,
        "resize_to": resize_to,
        "close_error": close_error,
        "size": st.st_size,
        "expected_size": len(expected),
        "content_hex": data.hex(),
        "expected_hex": expected.hex(),
        "content_sha256": _sha256_bytes(data),
        "expected_sha256": _sha256_bytes(expected),
        "duration_seconds": round(time.time() - started, 6),
    }


def _fresh_verify(path: Path, expected: bytes) -> dict[str, Any]:
    started = time.time()
    data = _read_all(path)
    st = os.stat(path)
    return {
        "ok": data == expected and st.st_size == len(expected),
        "size": st.st_size,
        "expected_size": len(expected),
        "content_hex": data.hex(),
        "expected_hex": expected.hex(),
        "content_sha256": _sha256_bytes(data),
        "expected_sha256": _sha256_bytes(expected),
        "duration_seconds": round(time.time() - started, 6),
    }


def _os_error(exc: OSError) -> dict[str, Any]:
    return {"errno": exc.errno, "strerror": exc.strerror}


def _safe_read_all(path: Path) -> tuple[bytes | None, dict[str, Any] | None]:
    try:
        return _read_all(path), None
    except OSError as exc:
        return None, _os_error(exc)


def _safe_stat_size(path: Path) -> tuple[int | None, dict[str, Any] | None]:
    try:
        return os.stat(path).st_size, None
    except OSError as exc:
        return None, _os_error(exc)


def _rename_overwrite_hardlink(path: Path) -> dict[str, Any]:
    started = time.time()
    src_payload = b"rename-src-bytes"
    dst_payload = b"rename-dst-original"
    path.mkdir(parents=True, exist_ok=False)
    src = path / "src"
    dst = path / "dst"
    dst_link = path / "dst.lnk"
    src.write_bytes(src_payload)
    dst.write_bytes(dst_payload)
    os.link(dst, dst_link)

    def stat_record(target: Path) -> dict[str, Any]:
        st = os.stat(target)
        return {
            "path": str(target),
            "dev": st.st_dev,
            "ino": st.st_ino,
            "nlink": st.st_nlink,
            "size": st.st_size,
            "mode": oct(st.st_mode & 0o7777),
            "mtime_ns": st.st_mtime_ns,
        }

    src_before = stat_record(src)
    dst_before = stat_record(dst)
    link_before = stat_record(dst_link)
    os.rename(src, dst)
    # This stat must observe the surviving hardlink before reopening the file.
    link_after = stat_record(dst_link)
    dst_after = stat_record(dst)
    src_exists_after = src.exists()
    link_data = _read_all(dst_link)
    dst_data = _read_all(dst)
    ok = (
        dst_before["dev"] == link_before["dev"]
        and dst_before["ino"] == link_before["ino"]
        and dst_before["nlink"] == 2
        and link_before["nlink"] == 2
        and link_after["dev"] == dst_before["dev"]
        and link_after["ino"] == dst_before["ino"]
        and link_after["nlink"] == 1
        and link_data == dst_payload
        and dst_after["dev"] == src_before["dev"]
        and dst_after["ino"] == src_before["ino"]
        and dst_after["nlink"] == 1
        and dst_data == src_payload
        and not src_exists_after
    )
    return {
        "ok": ok,
        "src_before": src_before,
        "dst_before": dst_before,
        "link_before": link_before,
        "link_after": link_after,
        "dst_after": dst_after,
        "src_exists_after": src_exists_after,
        "link_content_hex": link_data.hex(),
        "dst_content_hex": dst_data.hex(),
        "expected_link_content_hex": dst_payload.hex(),
        "expected_dst_content_hex": src_payload.hex(),
        "link_content_sha256": _sha256_bytes(link_data),
        "dst_content_sha256": _sha256_bytes(dst_data),
        "expected_link_content_sha256": _sha256_bytes(dst_payload),
        "expected_dst_content_sha256": _sha256_bytes(src_payload),
        "duration_seconds": round(time.time() - started, 6),
    }


def _provider_resize_write_visible(path: Path, payload: bytes, temporary_size: int) -> dict[str, Any]:
    started = time.time()
    fd: int | None = None
    open_error = None
    truncate_error = None
    write_error = None
    close_error = None

    before, before_error = _safe_read_all(path)
    try:
        fd = os.open(path, os.O_WRONLY)
    except OSError as exc:
        open_error = _os_error(exc)

    if fd is not None:
        try:
            os.truncate(path, temporary_size)
        except OSError as exc:
            truncate_error = _os_error(exc)
        try:
            os.pwrite(fd, payload, 0)
        except OSError as exc:
            write_error = _os_error(exc)

    stat_after_truncate_size, stat_after_truncate_error = _safe_stat_size(path)
    # One same-worker, same-mount readonly path read before fsync or close.
    observed_before_close, observed_before_close_error = _safe_read_all(path)
    stat_before_close_size, stat_before_close_error = _safe_stat_size(path)

    if fd is not None:
        try:
            os.close(fd)
        except OSError as exc:
            close_error = _os_error(exc)

    after_close, after_close_error = _safe_read_all(path)
    stat_after_close_size, stat_after_close_error = _safe_stat_size(path)
    expected = payload
    ok = (
        open_error is None
        and truncate_error is None
        and write_error is None
        and close_error is None
        and observed_before_close_error is None
        and after_close_error is None
        and observed_before_close == expected
        and after_close == expected
        and stat_before_close_size == len(expected)
        and stat_after_close_size == len(expected)
    )
    return {
        "ok": ok,
        "temporary_size": temporary_size,
        "before_hex": None if before is None else before.hex(),
        "before_error": before_error,
        "open_error": open_error,
        "truncate_error": truncate_error,
        "stat_after_truncate_size": stat_after_truncate_size,
        "stat_after_truncate_error": stat_after_truncate_error,
        "write_error": write_error,
        "observed_before_close_hex": None if observed_before_close is None else observed_before_close.hex(),
        "observed_before_close_error": observed_before_close_error,
        "stat_before_close_size": stat_before_close_size,
        "stat_before_close_error": stat_before_close_error,
        "close_error": close_error,
        "after_close_hex": None if after_close is None else after_close.hex(),
        "after_close_error": after_close_error,
        "stat_after_close_size": stat_after_close_size,
        "stat_after_close_error": stat_after_close_error,
        "expected_hex": expected.hex(),
        "observed_before_close_sha256": None if observed_before_close is None else _sha256_bytes(observed_before_close),
        "after_close_sha256": None if after_close is None else _sha256_bytes(after_close),
        "expected_sha256": _sha256_bytes(expected),
        "expected_size": len(expected),
        "duration_seconds": round(time.time() - started, 6),
    }


def _hold_old_then_fresh(path: Path, expected: bytes, expected_processes: list[tuple[str, int, str]]) -> int:
    started = time.time()
    fd = None
    try:
        fd = os.open(path, os.O_RDONLY)
        old = os.pread(fd, 4096, 0)
        _emit({"event": "READY", "op": "hold-old-then-fresh", "path": str(path), "identity": _worker_identity(path, expected_processes), "old_fd_initial_hex": old.hex(), "old_fd_initial_size": len(old)})
        line = sys.stdin.readline()
        if line.strip() != "GO":
            raise RuntimeError(f"expected GO on stdin, got {line!r}")
        result = _fresh_verify(path, expected)
        result.update({"old_fd_initial_hex": old.hex(), "old_fd_initial_size": len(old), "duration_seconds": round(time.time() - started, 6)})
        _emit({"event": "RESULT", "op": "hold-old-then-fresh", "path": str(path), "identity": _worker_identity(path, expected_processes), **result})
        return 0 if result.get("ok") else 1
    except Exception as exc:  # noqa: BLE001 - preserve worker failure as evidence
        _emit({"event": "ERROR", "op": "hold-old-then-fresh", "path": str(path), "exception": type(exc).__name__, "message": str(exc), "identity": _worker_identity(path, expected_processes), "duration_seconds": round(time.time() - started, 6)})
        return 1
    finally:
        if fd is not None:
            os.close(fd)


def _worker_main(args: argparse.Namespace) -> int:
    if platform.system() != "Linux":
        _emit({"event": "ERROR", "op": args.op, "message": "consistency_cross worker requires Linux", "platform": platform.system()})
        return 1
    path = Path(args.path).resolve() if args.path else None
    expected_processes = args.expected_process or []
    try:
        if args.op == "identity":
            _emit(_result_event(args.op, path, expected_processes, stat=_stat_json(path) if path and path.exists() else None))
            return 0
        if args.op == "same-mount":
            assert path is not None
            result = _same_mount_visible(path)
            _emit(_result_event(args.op, path, expected_processes, **result, stat=_stat_json(path)))
            return 0 if result.get("ok") else 1
        if args.op == "write-close":
            assert path is not None
            result = _write_close(path, args.payload.encode(), args.resize_to, args.fsync_before_close)
            _emit(_result_event(args.op, path, expected_processes, **result, stat=_stat_json(path)))
            return 0 if result.get("ok") else 1
        if args.op == "fresh-verify":
            assert path is not None
            result = _fresh_verify(path, args.expected.encode())
            _emit(_result_event(args.op, path, expected_processes, **result, stat=_stat_json(path)))
            return 0 if result.get("ok") else 1
        if args.op == "provider-resize-write-visible":
            assert path is not None
            result = _provider_resize_write_visible(path, args.payload.encode(), 0 if args.resize_to is None else args.resize_to)
            _emit(_result_event(args.op, path, expected_processes, **result, stat=_stat_json(path)))
            return 0 if result.get("ok") else 1
        if args.op == "rename-overwrite-hardlink":
            assert path is not None
            result = _rename_overwrite_hardlink(path)
            _emit(_result_event(args.op, path, expected_processes, **result, stat=_stat_json(path)))
            return 0 if result.get("ok") else 1
        if args.op == "hold-old-then-fresh":
            assert path is not None
            return _hold_old_then_fresh(path, args.expected.encode(), expected_processes)
        raise RuntimeError(f"unknown worker op {args.op!r}")
    except Exception as exc:  # noqa: BLE001 - preserve raw worker error
        _emit({"event": "ERROR", "op": args.op, "path": str(path) if path else None, "exception": type(exc).__name__, "message": str(exc), "identity": _worker_identity(path, expected_processes) if path else None})
        return 1


class StreamingWorker:
    def __init__(self, name: str, argv: list[str], timeout: float) -> None:
        self.name = name
        self.argv = argv
        self.timeout = timeout
        self.proc = subprocess.Popen(argv, text=True, stdin=subprocess.PIPE, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
        self.events: list[dict[str, Any]] = []
        self.stderr = ""

    def read_event(self, event: str) -> dict[str, Any]:
        assert self.proc.stdout is not None
        selector = selectors.DefaultSelector()
        selector.register(self.proc.stdout, selectors.EVENT_READ)
        deadline = time.monotonic() + self.timeout
        try:
            while True:
                remaining = deadline - time.monotonic()
                if remaining <= 0:
                    self._terminate_after_timeout()
                    raise TimeoutError(f"{self.name} did not emit {event}; events={self.events}; stderr={self.stderr!r}")
                ready = selector.select(remaining)
                if not ready:
                    self._terminate_after_timeout()
                    raise TimeoutError(f"{self.name} did not emit {event}; events={self.events}; stderr={self.stderr!r}")
                line = self.proc.stdout.readline()
                if not line:
                    if self.proc.poll() is not None:
                        _, err = self.proc.communicate(timeout=1)
                        self.stderr += err or ""
                        break
                    continue
                parsed = json.loads(line)
                self.events.append(parsed)
                if parsed.get("event") == event:
                    return parsed
                if parsed.get("event") == "ERROR":
                    raise RuntimeError(json.dumps(parsed, sort_keys=True))
            raise TimeoutError(f"{self.name} exited before {event}; events={self.events}; stderr={self.stderr!r}")
        finally:
            selector.close()

    def _terminate_after_timeout(self) -> None:
        if self.proc.poll() is not None:
            return
        self.proc.terminate()
        try:
            _out, err = self.proc.communicate(timeout=2)
            self.stderr += err or ""
        except subprocess.TimeoutExpired:
            self.proc.kill()
            _out, err = self.proc.communicate(timeout=2)
            self.stderr += err or ""

    def send_go(self) -> None:
        assert self.proc.stdin is not None
        self.proc.stdin.write("GO\n")
        self.proc.stdin.flush()

    def finish(self) -> tuple[int, str]:
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
        return int(self.proc.returncode if self.proc.returncode is not None else -9), self.stderr


class ConsistencyController:
    def __init__(self, args: argparse.Namespace) -> None:
        self.args = args
        self.evidence = Path(args.evidence).resolve()
        self.run_id = args.run_id or f"consistency-cross-{time.strftime('%Y%m%dT%H%M%SZ', time.gmtime())}-{os.getpid()}-{random.randrange(1_000_000):06d}"
        self.commands: list[dict[str, Any]] = []
        self.steps: list[dict[str, Any]] = []
        self.failures: list[dict[str, Any]] = []
        self.identity: dict[str, Any] = {}
        self.identity_baseline: dict[str, dict[str, dict[str, Any]]] = {}

    def worker_prefix(self, worker: str) -> list[str]:
        return list(self.args.worker_a_json if worker == "A" else self.args.worker_b_json)

    def expected_args(self, worker: str) -> list[str]:
        expected = self.args.worker_a_expected_process if worker == "A" else self.args.worker_b_expected_process
        result: list[str] = []
        for role, pid, sha in expected:
            result += ["--expected-process", f"{role}={pid}:{sha}"]
        return result

    def argv(self, worker: str, op: str, path: str | None = None, **kwargs: Any) -> list[str]:
        cmd = self.worker_prefix(worker) + ["worker", "--op", op] + self.expected_args(worker)
        if path is not None:
            cmd += ["--path", path]
        for key, value in kwargs.items():
            if value is None:
                continue
            option = "--" + key.replace("_", "-")
            if isinstance(value, bool):
                if value:
                    cmd.append(option)
            else:
                cmd += [option, str(value)]
        return cmd

    def run_cmd(self, worker: str, op: str, path: str | None = None, **kwargs: Any) -> dict[str, Any]:
        argv = self.argv(worker, op, path, **kwargs)
        started = time.time()
        proc = subprocess.run(argv, text=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE, timeout=self.args.command_timeout, check=False)
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
        if events and events[-1].get("event") == "ERROR":
            raise RuntimeError(json.dumps(events[-1], sort_keys=True))
        if events:
            self.validate_event_identity(worker, events[-1])
        return record

    def start_worker(self, name: str, worker: str, op: str, path: str, **kwargs: Any) -> StreamingWorker:
        argv = self.argv(worker, op, path, **kwargs)
        self.commands.append({"worker": worker, "op": op, "argv": argv, "streaming": True, "name": name})
        return StreamingWorker(name, argv, self.args.child_timeout)

    def event(self, record: dict[str, Any]) -> dict[str, Any]:
        events = record.get("json_events") or []
        if not events:
            raise RuntimeError(json.dumps({"message": "worker emitted no JSON", "record": record}, sort_keys=True))
        return events[-1]

    def remember_identity_baseline(self, worker: str, event: dict[str, Any]) -> None:
        processes = event.get("identity", {}).get("expected_processes") or {}
        self.identity_baseline[worker] = {role: {
            "pid": value.get("pid"),
            "start_ticks": value.get("start_ticks"),
            "sha256": value.get("sha256"),
            "exe_dev": value.get("exe_dev"),
            "exe_inode": value.get("exe_inode"),
        } for role, value in processes.items()}

    def validate_event_identity(self, worker: str, event: dict[str, Any]) -> None:
        if not self.identity_baseline:
            return
        expected = self.identity_baseline.get(worker) or {}
        observed = event.get("identity", {}).get("expected_processes") or {}
        if set(observed) != set(expected):
            raise RuntimeError(json.dumps({"message": "worker identity roles changed", "worker": worker, "expected_roles": sorted(expected), "observed_roles": sorted(observed), "event": event}, sort_keys=True))
        for role, want in expected.items():
            got = observed.get(role) or {}
            current = {
                "pid": got.get("pid"),
                "start_ticks": got.get("start_ticks"),
                "sha256": got.get("sha256"),
                "exe_dev": got.get("exe_dev"),
                "exe_inode": got.get("exe_inode"),
            }
            if current != want or got.get("sha256_ok") is not True:
                raise RuntimeError(json.dumps({"message": "worker process identity changed or mismatched", "worker": worker, "role": role, "expected": want, "observed": got, "event_op": event.get("op")}, sort_keys=True))

    def checked_stream_event(self, worker: str, child: StreamingWorker, expected_event: str) -> dict[str, Any]:
        event = child.read_event(expected_event)
        self.validate_event_identity(worker, event)
        return event

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

    def expected_processes_ok(self, event: dict[str, Any]) -> bool:
        return bool(event.get("identity", {}).get("expected_processes")) and bool(event.get("identity", {}).get("all_expected_processes_ok"))

    def setup(self) -> None:
        self.evidence.mkdir(parents=True, exist_ok=True)
        if platform.system() != "Linux" and not self.args.allow_host_non_linux:
            raise RuntimeError("controller host must be Linux unless --allow-host-non-linux is set for orchestration")
        owner_a = self.event(self.run_cmd("A", "identity", self.args.owner_root_a))
        owner_b = self.event(self.run_cmd("B", "identity", self.args.owner_root_b))
        self.remember_identity_baseline("A", owner_a)
        self.remember_identity_baseline("B", owner_b)
        dfs_a = self.event(self.run_cmd("A", "identity", self.args.dfs_root_a))
        dfs_b = self.event(self.run_cmd("B", "identity", self.args.dfs_root_b))
        all_expected_roles = [role for role, _pid, _sha in (self.args.worker_a_expected_process + self.args.worker_b_expected_process)]
        boot_a = owner_a.get("identity", {}).get("boot_id")
        boot_b = owner_b.get("identity", {}).get("boot_id")
        qualification = {
            "worker_boot_ids_nonempty": bool(boot_a) and bool(boot_b),
            "different_worker_kernels": bool(boot_a) and bool(boot_b) and boot_a != boot_b,
            "worker_a_owner_afs_fuse": self.mount_is_afs_fuse(owner_a),
            "worker_b_owner_afs_fuse": self.mount_is_afs_fuse(owner_b),
            "worker_a_dfs_afs_fuse": self.mount_is_afs_fuse(dfs_a),
            "worker_b_dfs_afs_fuse": self.mount_is_afs_fuse(dfs_b),
            "worker_a_expected_processes_ok": self.expected_processes_ok(owner_a) and self.expected_processes_ok(dfs_a),
            "worker_b_expected_processes_ok": self.expected_processes_ok(owner_b) and self.expected_processes_ok(dfs_b),
            "worker_a_node_identity_present": any(role.startswith("node") for role, _pid, _sha in self.args.worker_a_expected_process),
            "worker_b_node_identity_present": any(role.startswith("node") for role, _pid, _sha in self.args.worker_b_expected_process),
            "meta_identity_present": any(role.startswith("meta") for role in all_expected_roles),
        }
        self.identity = {"worker_a_owner": owner_a, "worker_b_owner": owner_b, "worker_a_dfs": dfs_a, "worker_b_dfs": dfs_b, "qualification": qualification, "cross_mount_qualified": all(qualification.values())}
        if not self.args.allow_same_kernel_reference:
            self.expect(qualification["worker_boot_ids_nonempty"] and qualification["different_worker_kernels"], "workers must have nonempty distinct kernel boot IDs", self.identity)
        if self.args.require_cross_mount:
            self.expect(self.identity["cross_mount_qualified"], "workers are not qualified cross-mount AFS evidence", self.identity)

    def expect(self, condition: bool, message: str, details: dict[str, Any] | None = None) -> None:
        if not condition:
            raise AssertionError(json.dumps({"message": message, "details": details or {}}, sort_keys=True))

    def step(self, name: str, func) -> None:  # type: ignore[no-untyped-def]
        started = time.time()
        record: dict[str, Any] = {"name": name, "started_at": _now()}
        try:
            details = func()
            record.update({"ok": True, "details": details})
        except Exception as exc:  # noqa: BLE001 - preserve failures and continue
            record.update({"ok": False, "exception": type(exc).__name__, "message": str(exc), "traceback": traceback.format_exc()})
            self.failures.append(record)
        finally:
            record["duration_ms"] = round((time.time() - started) * 1000, 3)
            self.steps.append(record)

    def backend_path(self, backend: str, worker: str, name: str) -> str:
        if backend == "owner":
            root = self.args.owner_root_a if worker == "A" else self.args.owner_root_b
        elif backend == "dfs":
            root = self.args.dfs_root_a if worker == "A" else self.args.dfs_root_b
        else:
            raise ValueError(backend)
        return str(Path(root) / self.run_id / name)

    def same_mount(self, backend: str) -> dict[str, Any]:
        path = self.backend_path(backend, "A", f"{backend}-same-mount.txt")
        event = self.event(self.run_cmd("A", "same-mount", path))
        self.expect(event.get("ok") is True, f"{backend} same-mount visible failed", event)
        return event

    def close_to_open(self, backend: str) -> dict[str, Any]:
        path_a = self.backend_path(backend, "A", f"{backend}-close-to-open.txt")
        path_b = self.backend_path(backend, "B", f"{backend}-close-to-open.txt")
        old_payload = f"{backend}-old-before-close"
        final_payload = f"{backend}-close-to-open-final"
        initial = self.event(self.run_cmd("A", "write-close", path_a, payload=old_payload))
        self.expect(initial.get("ok") is True, f"{backend} initial close failed", initial)
        precheck = self.event(self.run_cmd("B", "fresh-verify", path_b, expected=old_payload))
        self.expect(precheck.get("ok") is True, f"{backend} B precheck did not see initial content", precheck)
        holder = self.start_worker(f"{backend}-B-hold-old", "B", "hold-old-then-fresh", path_b, expected=final_payload)
        ready = self.checked_stream_event("B", holder, "READY")
        final_close = self.event(self.run_cmd("A", "write-close", path_a, payload=final_payload))
        self.expect(final_close.get("ok") is True, f"{backend} A final close failed", final_close)
        holder.send_go()
        result = self.checked_stream_event("B", holder, "RESULT")
        rc, stderr = holder.finish()
        self.expect(rc == 0 and result.get("ok") is True, f"{backend} B one-shot fresh open did not observe close-to-open content", {"result": result, "returncode": rc, "stderr": stderr})
        return {"initial": initial, "precheck": precheck, "ready": ready, "final_close": final_close, "b_fresh_once": result, "holder_returncode": rc}

    def remote_owner(self, backend: str) -> dict[str, Any]:
        path_a = self.backend_path(backend, "A", f"{backend}-remote-owner.txt")
        path_b = self.backend_path(backend, "B", f"{backend}-remote-owner.txt")
        initial_payload = f"{backend}-remote-initial"
        if backend == "owner":
            final_payload = "owner-remote-final-payload"
            expected = "owner-remote-final"
        else:
            final_payload = "dfs-remote-final-payload"
            expected = "dfs-remote-final"
        initial = self.event(self.run_cmd("A", "write-close", path_a, payload=initial_payload))
        self.expect(initial.get("ok") is True, f"{backend} remote initial close failed", initial)
        b_write = self.event(self.run_cmd("B", "write-close", path_b, payload=final_payload, resize_to=len(expected), fsync_before_close=True))
        self.expect(b_write.get("ok") is True, f"{backend} B write/resize/fsync/close failed", b_write)
        a_verify = self.event(self.run_cmd("A", "fresh-verify", path_a, expected=expected))
        self.expect(a_verify.get("ok") is True, f"{backend} A fresh open did not observe B remote-owner write", a_verify)
        return {"initial": initial, "b_write": b_write, "a_verify": a_verify}

    def owner_rename_overwrite_hardlink(self, worker: str, label: str) -> dict[str, Any]:
        path = self.backend_path("owner", worker, f"owner-rename-overwrite-hardlink-{label}")
        event = self.event(self.run_cmd(worker, "rename-overwrite-hardlink", path))
        self.expect(event.get("ok") is True, f"OwnerFs {label} rename overwrite hardlink regression failed", event)
        return event

    def dfs_handover_after_idle(self) -> dict[str, Any]:
        path_a = self.backend_path("dfs", "A", "dfs-handover-after-idle.txt")
        path_b = self.backend_path("dfs", "B", "dfs-handover-after-idle.txt")
        initial = self.event(self.run_cmd("A", "write-close", path_a, payload="dfs-handover-initial"))
        self.expect(initial.get("ok") is True, "DFS handover initial close failed", initial)
        time.sleep(self.args.handover_idle_seconds)
        b_write = self.event(self.run_cmd("B", "write-close", path_b, payload="dfs-handover-final-payload", resize_to=len("dfs-handover-final"), fsync_before_close=True))
        self.expect(b_write.get("ok") is True, "DFS B handover write/resize/fsync/close failed", b_write)
        a_verify = self.event(self.run_cmd("A", "fresh-verify", path_a, expected="dfs-handover-final"))
        self.expect(a_verify.get("ok") is True, "DFS A fresh open after B handover close saw stale content", a_verify)
        a_provider = self.event(self.run_cmd("A", "provider-resize-write-visible", path_a, payload="dfs-handover-provider-final", resize_to=0))
        self.expect(a_provider.get("ok") is True, "DFS A retained local state shadowed provider write after B handover", a_provider)
        b_verify_after_provider = self.event(self.run_cmd("B", "fresh-verify", path_b, expected="dfs-handover-provider-final"))
        self.expect(b_verify_after_provider.get("ok") is True, "DFS B fresh open after A post-handover provider close saw stale content", b_verify_after_provider)
        return {
            "initial": initial,
            "idle_seconds": self.args.handover_idle_seconds,
            "b_write": b_write,
            "a_verify": a_verify,
            "a_provider_after_handover": a_provider,
            "b_verify_after_provider": b_verify_after_provider,
        }

    def dfs_provider_existing_handle_after_remote_resize(self) -> dict[str, Any]:
        path_a = self.backend_path("dfs", "A", "dfs-provider-existing-handle.txt")
        path_b = self.backend_path("dfs", "B", "dfs-provider-existing-handle.txt")
        initial = self.event(self.run_cmd("A", "write-close", path_a, payload="dfs-provider-initial"))
        self.expect(initial.get("ok") is True, "DFS provider initial A close failed", initial)
        b_provider = self.event(self.run_cmd("B", "provider-resize-write-visible", path_b, payload="dfs-provider-final", resize_to=0))
        self.expect(b_provider.get("ok") is True, "DFS B existing handle write after handleless resize was not same-mount visible before close", b_provider)
        a_verify = self.event(self.run_cmd("A", "fresh-verify", path_a, expected="dfs-provider-final"))
        self.expect(a_verify.get("ok") is True, "DFS A fresh open after provider close did not observe final bytes", a_verify)
        return {"initial": initial, "b_provider": b_provider, "a_verify": a_verify}

    def run(self) -> None:
        self.step("owner_same_mount", lambda: self.same_mount("owner"))
        self.step("dfs_same_mount", lambda: self.same_mount("dfs"))
        self.step("owner_close_to_open", lambda: self.close_to_open("owner"))
        self.step("dfs_close_to_open", lambda: self.close_to_open("dfs"))
        self.step("owner_remote_owner", lambda: self.remote_owner("owner"))
        self.step("owner_rename_overwrite_hardlink_local", lambda: self.owner_rename_overwrite_hardlink("A", "local"))
        self.step("owner_rename_overwrite_hardlink_remote_home", lambda: self.owner_rename_overwrite_hardlink("B", "remote-home"))
        self.step("dfs_remote_owner", lambda: self.remote_owner("dfs"))
        self.step("dfs_handover_after_idle", self.dfs_handover_after_idle)
        self.step("dfs_provider_existing_handle_after_remote_resize", self.dfs_provider_existing_handle_after_remote_resize)

    def write_report(self) -> None:
        report = {
            "schema": SCHEMA,
            "created_at": _now(),
            "run_id": self.run_id,
            "status": "PASS" if not self.failures else "FAIL",
            "identity": self.identity,
            "worker_commands": {"A": self.args.worker_a_json, "B": self.args.worker_b_json},
            "mount_roots": {
                "owner_root_a": self.args.owner_root_a,
                "owner_root_b": self.args.owner_root_b,
                "dfs_root_a": self.args.dfs_root_a,
                "dfs_root_b": self.args.dfs_root_b,
            },
            "host": {
                "system": platform.system(),
                "release": platform.release(),
                "machine": platform.machine(),
                "python": platform.python_version(),
                "boot_id": _read_text_optional(Path("/proc/sys/kernel/random/boot_id")),
            },
            "summary": {"steps": len(self.steps), "passed": sum(1 for step in self.steps if step.get("ok")), "failed": len(self.failures), "commands": len(self.commands)},
            "steps": self.steps,
            "failures": self.failures,
            "commands": self.commands,
            "notes": [
                "All product file IO is performed by Linux worker subprocesses.",
                "Close-to-open checks use one fresh open after an explicit stdin GO orchestration point; no read retry is used to hide stale results.",
                "The controller does not restart, signal, pause, or reconfigure product runtime processes.",
                "The DFS handover case intentionally idles before B writable open, performs one A fresh-open verification after B close, then verifies the former local owner can perform a provider write without retained stale state shadowing it.",
                "The OwnerFs rename regression overwrites dst with src while a dst hardlink survives, then checks the surviving hardlink inode, nlink, and content without retry.",
                "The DFS provider regression keeps a B writable handle open, performs handleless truncate(path), writes through the original handle without sync, checks same-B readonly visibility before close, then checks A fresh-open visibility after close.",
            ],
        }
        _write_json(self.evidence / "report.json", report)
        (self.evidence / "summary.txt").write_text(
            f"status={report['status']} cross_mount_qualified={self.identity.get('cross_mount_qualified', False)} "
            f"steps={report['summary']['steps']} failed={report['summary']['failed']} evidence={self.evidence}\n",
            encoding="utf-8",
        )


def _host_main(args: argparse.Namespace) -> int:
    controller = ConsistencyController(args)
    try:
        controller.setup()
        controller.run()
    except Exception as exc:  # noqa: BLE001 - preserve setup/runner failure
        controller.failures.append({"name": "setup_or_runner", "ok": False, "exception": type(exc).__name__, "message": str(exc), "traceback": traceback.format_exc()})
    finally:
        controller.write_report()
    status = "PASS" if not controller.failures else "FAIL"
    print(f"status={status} cross_mount_qualified={controller.identity.get('cross_mount_qualified', False)} evidence={controller.evidence}")
    return 0 if not controller.failures else 1


def _selftest_main() -> int:
    if platform.system() != "Linux":
        print(json.dumps({"status": "SKIP", "reason": "Linux-only selftest", "platform": platform.system()}, sort_keys=True))
        return 0

    results: list[dict[str, Any]] = []

    def record(name: str, ok: bool, **details: Any) -> None:
        results.append({"name": name, "ok": ok, **details})

    # Regression: a silent streaming worker must not block forever.
    silent = StreamingWorker("silent-selftest", [sys.executable, "-c", "import time; time.sleep(5)"], 0.2)
    started = time.monotonic()
    try:
        try:
            silent.read_event("READY")
            record("silent_worker_timeout", False, message="read_event unexpectedly returned")
        except TimeoutError as exc:
            elapsed = time.monotonic() - started
            record("silent_worker_timeout", elapsed < 2.0 and silent.proc.poll() is not None, elapsed_seconds=round(elapsed, 3), message=str(exc), returncode=silent.proc.returncode)
    finally:
        if silent.proc.poll() is None:
            silent.proc.kill()
            silent.finish()

    with tempfile.TemporaryDirectory() as temp_dir:
        temp = Path(temp_dir)
        script = Path(__file__).resolve()
        worker = json.dumps([sys.executable, str(script)])
        self_sha = _sha256_path(Path("/proc") / str(os.getpid()) / "exe")
        expected_node = f"node={os.getpid()}:{self_sha}"
        expected_meta = f"meta={os.getpid()}:{self_sha}"

        # Positive local reference run. It is same-kernel and non-AFS by design,
        # so it must use the explicit reference-only switch and not require cross mount.
        evidence = temp / "evidence-pass"
        argv = [
            sys.executable, str(script), "host",
            "--worker-a-json", worker, "--worker-b-json", worker,
            "--owner-root-a", str(temp / "owner-a"), "--owner-root-b", str(temp / "owner-a"),
            "--dfs-root-a", str(temp / "dfs-a"), "--dfs-root-b", str(temp / "dfs-a"),
            "--worker-a-expected-process", expected_node, "--worker-a-expected-process", expected_meta,
            "--worker-b-expected-process", expected_node, "--worker-b-expected-process", expected_meta,
            "--evidence", str(evidence),
            "--handover-idle-seconds", "0.01",
            "--allow-same-kernel-reference",
        ]
        proc = subprocess.run(argv, text=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE, timeout=60, check=False)
        report = json.loads((evidence / "report.json").read_text(encoding="utf-8"))
        summary = report["summary"]
        record(
            "local_reference_positive",
            proc.returncode == 0 and report["status"] == "PASS" and summary["steps"] == 10 and summary["passed"] == 10 and summary["failed"] == 0,
            returncode=proc.returncode,
            summary=summary,
            stderr=proc.stderr,
        )

        # Regression: wrong expected SHA must be visible in worker identity.
        wrong_sha_path = temp / "wrong-sha-target"
        wrong_sha_path.write_text("identity\n", encoding="utf-8")
        wrong_sha = "0" * 64
        wrong_proc = subprocess.run(
            [sys.executable, str(script), "worker", "--op", "identity", "--path", str(wrong_sha_path), "--expected-process", f"node={os.getpid()}:{wrong_sha}"],
            text=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE, timeout=20, check=False,
        )
        wrong_event = json.loads(wrong_proc.stdout)
        record("wrong_sha_detected", wrong_event.get("identity", {}).get("all_expected_processes_ok") is False and wrong_event["identity"]["expected_processes"]["node"]["sha256_ok"] is False, returncode=wrong_proc.returncode, event=wrong_event)

        # Regression: --require-cross-mount must reject ordinary ext4/non-AFS DFS roots.
        evidence_reject = temp / "evidence-reject-nonafs"
        reject_argv = [
            sys.executable, str(script), "host",
            "--worker-a-json", worker, "--worker-b-json", worker,
            "--owner-root-a", str(temp / "owner-r"), "--owner-root-b", str(temp / "owner-r"),
            "--dfs-root-a", str(temp / "dfs-r"), "--dfs-root-b", str(temp / "dfs-r"),
            "--worker-a-expected-process", expected_node, "--worker-a-expected-process", expected_meta,
            "--worker-b-expected-process", expected_node, "--worker-b-expected-process", expected_meta,
            "--evidence", str(evidence_reject),
            "--handover-idle-seconds", "0.01",
            "--allow-same-kernel-reference",
            "--require-cross-mount",
        ]
        reject_proc = subprocess.run(reject_argv, text=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE, timeout=30, check=False)
        reject_report = json.loads((evidence_reject / "report.json").read_text(encoding="utf-8"))
        q = reject_report.get("identity", {}).get("qualification", {})
        record(
            "nonafs_dfs_rejected_for_cross_mount",
            reject_proc.returncode != 0 and reject_report["status"] == "FAIL" and q.get("worker_a_dfs_afs_fuse") is False and q.get("worker_b_dfs_afs_fuse") is False,
            returncode=reject_proc.returncode, qualification=q, failures=reject_report.get("failures"),
        )

    ok = all(item["ok"] for item in results)
    print(json.dumps({"status": "PASS" if ok else "FAIL", "results": results}, sort_keys=True))
    return 0 if ok else 1


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description=__doc__)
    sub = parser.add_subparsers(dest="command")

    host = sub.add_parser("host", help="run cross-worker consistency probes")
    host.add_argument("--worker-a-json", required=True, type=_parse_json_argv)
    host.add_argument("--worker-b-json", required=True, type=_parse_json_argv)
    host.add_argument("--owner-root-a", required=True, help="OwnerFs fixture root as seen by worker A")
    host.add_argument("--owner-root-b", required=True, help="same OwnerFs logical fixture root as seen by worker B")
    host.add_argument("--dfs-root-a", required=True, help="DFS fixture root as seen by worker A")
    host.add_argument("--dfs-root-b", required=True, help="same DFS logical fixture root as seen by worker B")
    host.add_argument("--worker-a-expected-process", action="append", type=_parse_expected_process, default=[], metavar="ROLE=PID:SHA256")
    host.add_argument("--worker-b-expected-process", action="append", type=_parse_expected_process, default=[], metavar="ROLE=PID:SHA256")
    host.add_argument("--evidence", required=True)
    host.add_argument("--run-id")
    host.add_argument("--handover-idle-seconds", type=float, default=DEFAULT_IDLE_SECONDS)
    host.add_argument("--command-timeout", type=float, default=DEFAULT_COMMAND_TIMEOUT_SECONDS)
    host.add_argument("--child-timeout", type=float, default=DEFAULT_CHILD_TIMEOUT_SECONDS)
    host.add_argument("--require-cross-mount", action="store_true")
    host.add_argument("--allow-same-kernel-reference", action="store_true", help="allow same-kernel reference/selftest runs; product A/B evidence must omit this")
    host.add_argument("--allow-host-non-linux", action="store_true", help="allow macOS host orchestration; workers must still be Linux")

    worker = sub.add_parser("worker", help=argparse.SUPPRESS)
    worker.add_argument("--op", required=True)
    worker.add_argument("--path")
    worker.add_argument("--payload", default="")
    worker.add_argument("--expected", default="")
    worker.add_argument("--resize-to", type=int)
    worker.add_argument("--fsync-before-close", action="store_true")
    worker.add_argument("--expected-process", action="append", type=_parse_expected_process, default=[], metavar="ROLE=PID:SHA256")

    sub.add_parser("selftest", help="run local Linux selftest; skips on non-Linux")
    return parser


def main(argv: list[str]) -> int:
    parser = build_parser()
    args = parser.parse_args(argv)
    if args.command == "worker":
        return _worker_main(args)
    if args.command == "host":
        return _host_main(args)
    if args.command == "selftest":
        return _selftest_main()
    parser.print_help(sys.stderr)
    return 2


if __name__ == "__main__":
    raise SystemExit(main(sys.argv[1:]))
