#!/usr/bin/env python3
"""Linux DFS owner-restart probe worker.

This probe is intentionally split into two small CLIs.  The writer keeps one
real O_RDWR file descriptor open across the fault window and records a durable
watermark before waiting for an external trigger.  The checker runs only after
the orchestrator has restarted the owner and validates the final file bytes once
they are readable.

The script does not signal, restart, or inspect AFS processes.  Runtime identity
and fault injection remain the orchestrator's responsibility.
"""
from __future__ import annotations

import argparse
import errno
import hashlib
import json
import os
import platform
import signal
import sys
import time
import traceback
from pathlib import Path
from typing import Any


SCHEMA = "afs.owner_restart.v1"
DEFAULT_PAYLOAD_SIZE = 64 * 1024
DEFAULT_TRIGGER_TIMEOUT_SECONDS = 120.0
DEFAULT_CLOSE_DEADLINE_SECONDS = 30.0
DEFAULT_CHECK_TIMEOUT_SECONDS = 60.0
EXT4_FSTYPE = "ext4"


class ProbeError(RuntimeError):
    pass


class CloseTimeout(TimeoutError):
    pass


def _now() -> str:
    return time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime())


def _sha256_bytes(data: bytes) -> str:
    return hashlib.sha256(data).hexdigest()


def _read_text_optional(path: Path) -> str | None:
    try:
        return path.read_text(encoding="utf-8").strip()
    except OSError:
        return None


def _decode_mount_path(value: str) -> str:
    return value.replace("\\040", " ").replace("\\011", "\t").replace("\\012", "\n").replace("\\134", "\\")


def _mount_fstype(path: Path) -> str:
    existing = path
    while not existing.exists():
        parent = existing.parent
        if parent == existing:
            raise ProbeError(f"no existing parent for {path}")
        existing = parent
    target = existing.resolve()
    best_len = -1
    best_fstype: str | None = None
    with Path("/proc/self/mountinfo").open("r", encoding="utf-8") as handle:
        for line in handle:
            parts = line.rstrip("\n").split(" ")
            if " - " not in line:
                continue
            sep = parts.index("-")
            if sep + 1 >= len(parts) or len(parts) < 5:
                continue
            mount_point = Path(_decode_mount_path(parts[4]))
            try:
                resolved_mount = mount_point.resolve()
            except OSError:
                resolved_mount = mount_point
            try:
                target.relative_to(resolved_mount)
            except ValueError:
                continue
            mount_len = len(str(resolved_mount))
            if mount_len > best_len:
                best_len = mount_len
                best_fstype = parts[sep + 1]
    if best_fstype is None:
        raise ProbeError(f"could not resolve filesystem type for {path}")
    return best_fstype


def _require_linux() -> None:
    if platform.system() != "Linux":
        raise ProbeError(f"owner restart probe is Linux-only, got {platform.system()}")


def _require_ext4(path: Path, role: str) -> None:
    fstype = _mount_fstype(path)
    if fstype != EXT4_FSTYPE:
        raise ProbeError(f"{role} must be on ext4, got {fstype!r} for {path}")


def _fsync_dir(path: Path) -> None:
    fd = os.open(path, os.O_RDONLY | getattr(os, "O_DIRECTORY", 0))
    try:
        os.fsync(fd)
    finally:
        os.close(fd)


def _write_json_atomic(path: Path, value: dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    tmp = path.with_name(f".{path.name}.tmp-{os.getpid()}")
    data = json.dumps(value, indent=2, sort_keys=True) + "\n"
    fd = os.open(tmp, os.O_WRONLY | os.O_CREAT | os.O_TRUNC, 0o644)
    try:
        with os.fdopen(fd, "w", encoding="utf-8") as handle:
            handle.write(data)
            handle.flush()
            os.fsync(handle.fileno())
    except Exception:
        try:
            tmp.unlink()
        except OSError:
            pass
        raise
    os.replace(tmp, path)
    _fsync_dir(path.parent)


def _read_json(path: Path) -> dict[str, Any]:
    with path.open("r", encoding="utf-8") as handle:
        value = json.load(handle)
    if not isinstance(value, dict):
        raise ProbeError(f"{path} did not contain a JSON object")
    return value


def _payload(size: int, seed: str) -> bytes:
    if size <= 0:
        raise ProbeError("payload size must be positive")
    output = bytearray()
    counter = 0
    seed_bytes = seed.encode("utf-8")
    while len(output) < size:
        output.extend(hashlib.sha256(seed_bytes + counter.to_bytes(8, "big")).digest())
        counter += 1
    return bytes(output[:size])


def _stat_record(path: Path) -> dict[str, Any]:
    st = path.stat()
    return {
        "path": str(path),
        "dev": st.st_dev,
        "ino": st.st_ino,
        "mode": oct(st.st_mode & 0o7777),
        "size": st.st_size,
        "mtime_ns": st.st_mtime_ns,
    }


def _process_record() -> dict[str, Any]:
    return {
        "pid": os.getpid(),
        "hostname": platform.node(),
        "system": platform.system(),
        "release": platform.release(),
        "machine": platform.machine(),
        "python": platform.python_version(),
        "boot_id": _read_text_optional(Path("/proc/sys/kernel/random/boot_id")),
    }


def _pread_exact(fd: int, length: int) -> bytes:
    chunks: list[bytes] = []
    offset = 0
    remaining = length
    while remaining > 0:
        chunk = os.pread(fd, remaining, offset)
        if not chunk:
            break
        chunks.append(chunk)
        offset += len(chunk)
        remaining -= len(chunk)
    return b"".join(chunks)


def _read_file_once(path: Path) -> bytes:
    fd = os.open(path, os.O_RDONLY)
    try:
        chunks: list[bytes] = []
        while True:
            chunk = os.read(fd, 1024 * 1024)
            if not chunk:
                break
            chunks.append(chunk)
        return b"".join(chunks)
    finally:
        os.close(fd)


def _wait_for_trigger(path: Path, timeout_seconds: float) -> dict[str, Any]:
    deadline = time.monotonic() + timeout_seconds
    polls = 0
    while time.monotonic() < deadline:
        polls += 1
        if path.exists():
            return {
                "observed": True,
                "path": str(path),
                "polls": polls,
                "elapsed_ms": round((timeout_seconds - max(0.0, deadline - time.monotonic())) * 1000, 3),
                "stat": _stat_record(path),
            }
        time.sleep(0.1)
    return {"observed": False, "path": str(path), "polls": polls, "timeout_seconds": timeout_seconds}


def _close_with_deadline(fd: int, deadline_seconds: float) -> dict[str, Any]:
    started = time.monotonic()

    def on_alarm(_signum: int, _frame: Any) -> None:
        raise CloseTimeout(f"os.close exceeded {deadline_seconds} seconds")

    previous = signal.getsignal(signal.SIGALRM)
    signal.signal(signal.SIGALRM, on_alarm)
    signal.setitimer(signal.ITIMER_REAL, deadline_seconds)
    try:
        os.close(fd)
        return {"completed": True, "ok": True, "duration_ms": round((time.monotonic() - started) * 1000, 3)}
    except CloseTimeout as exc:
        return {
            "completed": False,
            "ok": False,
            "timed_out": True,
            "duration_ms": round((time.monotonic() - started) * 1000, 3),
            "error": str(exc),
        }
    except OSError as exc:
        return {
            "completed": True,
            "ok": False,
            "errno": exc.errno,
            "errno_name": errno.errorcode.get(exc.errno, "UNKNOWN"),
            "error": str(exc),
            "duration_ms": round((time.monotonic() - started) * 1000, 3),
        }
    finally:
        signal.setitimer(signal.ITIMER_REAL, 0)
        signal.signal(signal.SIGALRM, previous)


def writer_main(args: argparse.Namespace) -> int:
    _require_linux()
    state_dir = Path(args.state_dir)
    ready_file = Path(args.ready_file)
    trigger_file = Path(args.trigger_file)
    result_file = Path(args.result_file)
    target = Path(args.target)
    _require_ext4(state_dir, "state directory")
    _require_ext4(ready_file.parent, "ready file directory")
    _require_ext4(trigger_file.parent, "trigger file directory")
    _require_ext4(result_file.parent, "result file directory")
    if ready_file == result_file:
        raise ProbeError("ready file and result file must be distinct")
    if not target.exists():
        raise ProbeError(f"target must already exist: {target}")

    payload = _payload(args.payload_size, args.payload_seed)
    fd = os.open(target, os.O_RDWR)
    close_record: dict[str, Any] | None = None
    status = "UNKNOWN"
    ready: dict[str, Any] | None = None
    try:
        started = time.monotonic()
        os.ftruncate(fd, 0)
        written = os.pwrite(fd, payload, 0)
        if written != len(payload):
            raise ProbeError(f"short pwrite: {written} of {len(payload)}")
        os.ftruncate(fd, len(payload))
        os.fsync(fd)
        read_back = _pread_exact(fd, len(payload))
        if read_back != payload:
            raise ProbeError("read-own-fd bytes differ from written payload")
        stat_after = os.fstat(fd)
        ready = {
            "schema": SCHEMA,
            "phase": "READY",
            "created_at": _now(),
            "target": str(target),
            "state_dir": str(state_dir),
            "process": _process_record(),
            "payload": {
                "size": len(payload),
                "seed": args.payload_seed,
                "sha256": _sha256_bytes(payload),
            },
            "durable_watermark": {
                "fsync_completed": True,
                "length": stat_after.st_size,
                "sha256": _sha256_bytes(read_back),
                "read_own_fd_sha256": _sha256_bytes(read_back),
                "target_stat": _stat_record(target),
                "write_fsync_elapsed_ms": round((time.monotonic() - started) * 1000, 3),
            },
        }
        _write_json_atomic(ready_file, ready)
        trigger = _wait_for_trigger(trigger_file, args.trigger_timeout)
        if not trigger["observed"]:
            status = "TRIGGER_TIMEOUT"
            close_record = {"completed": False, "ok": False, "skipped": True, "reason": "trigger timeout"}
        else:
            close_record = _close_with_deadline(fd, args.close_deadline)
            fd = -1
            if close_record.get("timed_out"):
                status = "CLOSE_TIMEOUT"
            elif close_record.get("ok"):
                status = "CLOSE_OK"
            else:
                status = "CLOSE_ERROR"
        result = {
            "schema": SCHEMA,
            "phase": "WRITER_RESULT",
            "status": status,
            "created_at": _now(),
            "target": str(target),
            "ready_file": str(ready_file),
            "trigger_file": str(trigger_file),
            "result_file": str(result_file),
            "ready": ready,
            "trigger": trigger,
            "close": close_record,
        }
        _write_json_atomic(result_file, result)
        return 0 if status in {"CLOSE_OK", "CLOSE_ERROR"} else 2
    finally:
        if fd >= 0:
            try:
                os.close(fd)
            except OSError:
                pass


def check_main(args: argparse.Namespace) -> int:
    _require_linux()
    expected = _read_json(Path(args.ready_file))
    result_file = Path(args.result_file)
    _require_ext4(result_file.parent, "result file directory")
    target = Path(args.target)
    expected_payload = expected.get("payload", {})
    expected_size = int(expected_payload.get("size", -1))
    expected_sha = str(expected_payload.get("sha256", ""))
    started = time.monotonic()
    previous = signal.getsignal(signal.SIGALRM)
    def on_alarm(_signum: int, _frame: Any) -> None:
        raise TimeoutError(f"recovery read exceeded {args.open_timeout} seconds")
    signal.signal(signal.SIGALRM, on_alarm)
    signal.setitimer(signal.ITIMER_REAL, args.open_timeout)
    result = {
        "schema": SCHEMA,
        "phase": "RECOVERY_CHECK",
        "created_at": _now(),
        "target": str(target),
        "ready_file": str(args.ready_file),
        "result_file": str(result_file),
        "expected": {"length": expected_size, "sha256": expected_sha},
    }
    try:
        data = _read_file_once(target)
        actual = {"length": len(data), "sha256": _sha256_bytes(data)}
        ok = actual["length"] == expected_size and actual["sha256"] == expected_sha
        result.update(status="PASS" if ok else "MISMATCH", actual=actual,
                      attempts=[{"event": "read", "ok": True, "target_stat": _stat_record(target)}])
    except TimeoutError as exc:
        result.update(status="READ_TIMEOUT", error=str(exc), attempts=[{"event": "timeout"}])
    except OSError as exc:
        result.update(status="READ_ERROR", attempts=[{
            "event": "open_or_read_error", "errno": exc.errno,
            "errno_name": errno.errorcode.get(exc.errno, "UNKNOWN"), "message": str(exc),
        }])
    finally:
        signal.setitimer(signal.ITIMER_REAL, 0)
        signal.signal(signal.SIGALRM, previous)
    result["elapsed_ms"] = round((time.monotonic() - started) * 1000, 3)
    _write_json_atomic(result_file, result)
    return 0 if result["status"] == "PASS" else 1


def selftest_main(args: argparse.Namespace) -> int:
    _require_linux()
    root = Path(args.root)
    root.mkdir(parents=True, exist_ok=True)
    _require_ext4(root, "selftest root")
    target = root / "owner-restart-target.bin"
    target.write_bytes(b"preexisting\n")
    os.sync()
    ready = root / "ready.json"
    trigger = root / "trigger"
    writer_result = root / "writer-result.json"
    check_result = root / "check-result.json"
    writer_args = argparse.Namespace(
        target=str(target),
        state_dir=str(root),
        ready_file=str(ready),
        trigger_file=str(trigger),
        result_file=str(writer_result),
        payload_size=DEFAULT_PAYLOAD_SIZE,
        payload_seed="owner-restart-selftest",
        trigger_timeout=2.0,
        close_deadline=5.0,
    )
    pid = os.fork()
    if pid == 0:
        try:
            os._exit(writer_main(writer_args))
        except Exception:
            traceback.print_exc()
            os._exit(99)
    ready_deadline = time.monotonic() + 5.0
    while not ready.exists() and time.monotonic() < ready_deadline:
        time.sleep(0.05)
    if not ready.exists():
        os.kill(pid, signal.SIGKILL)
        os.waitpid(pid, 0)
        raise ProbeError("selftest writer did not create READY")
    trigger.write_text("close\n", encoding="utf-8")
    _fsync_dir(root)
    waited_pid, status = os.waitpid(pid, 0)
    if waited_pid != pid or os.WEXITSTATUS(status) != 0:
        raise ProbeError(f"selftest writer failed with status {status}")
    check_args = argparse.Namespace(target=str(target), ready_file=str(ready), result_file=str(check_result), open_timeout=5.0)
    check_rc = check_main(check_args)
    report = {
        "schema": SCHEMA,
        "phase": "SELFTEST",
        "status": "PASS" if check_rc == 0 else "FAIL",
        "root": str(root),
        "writer_result": _read_json(writer_result),
        "check_result": _read_json(check_result),
    }
    print(json.dumps(report, sort_keys=True))
    return check_rc


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Run DFS owner restart probe workers")
    sub = parser.add_subparsers(dest="command", required=True)

    writer = sub.add_parser("writer", help="write, fsync, hold fd, wait for close trigger, and close")
    writer.add_argument("--target", required=True, help="existing DFS path opened O_RDWR by the worker")
    writer.add_argument("--state-dir", required=True, help="ext4 directory used for orchestration state")
    writer.add_argument("--ready-file", required=True, help="ext4 JSON file written after fsync watermark")
    writer.add_argument("--trigger-file", required=True, help="ext4 file whose creation triggers close")
    writer.add_argument("--result-file", required=True, help="ext4 JSON file receiving close result")
    writer.add_argument("--payload-size", "--size", dest="payload_size", type=int, default=DEFAULT_PAYLOAD_SIZE)
    writer.add_argument("--payload-seed", default="afs-owner-restart-v1")
    writer.add_argument("--trigger-timeout", type=float, default=DEFAULT_TRIGGER_TIMEOUT_SECONDS)
    writer.add_argument("--close-deadline", type=float, default=DEFAULT_CLOSE_DEADLINE_SECONDS)

    checker = sub.add_parser("check", help="after owner restart, read target once and validate READY bytes")
    checker.add_argument("--target", required=True, help="DFS path to open after restart")
    checker.add_argument("--ready-file", required=True, help="READY JSON produced by writer")
    checker.add_argument("--result-file", required=True, help="ext4 JSON file receiving recovery result")
    checker.add_argument("--open-timeout", type=float, default=DEFAULT_CHECK_TIMEOUT_SECONDS, help="deadline for one fresh open/read; errors are not retried")

    selftest = sub.add_parser("selftest", help="run a Linux ext4 protocol selftest on an ordinary file")
    selftest.add_argument("--root", required=True, help="ext4 directory for selftest files")
    return parser


def main(argv: list[str] | None = None) -> int:
    parser = build_parser()
    args = parser.parse_args(argv)
    try:
        if args.command == "writer":
            return writer_main(args)
        if args.command == "check":
            return check_main(args)
        if args.command == "selftest":
            return selftest_main(args)
        parser.error(f"unknown command {args.command}")
    except Exception as exc:  # noqa: BLE001 - CLI must serialize failures cleanly.
        error = {
            "schema": SCHEMA,
            "phase": "ERROR",
            "status": "ERROR",
            "created_at": _now(),
            "error_type": type(exc).__name__,
            "message": str(exc),
            "traceback": traceback.format_exc(),
        }
        print(json.dumps(error, sort_keys=True), file=sys.stderr)
        return 1
    return 1


if __name__ == "__main__":
    raise SystemExit(main())
