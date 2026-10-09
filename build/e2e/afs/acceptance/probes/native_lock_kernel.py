#!/usr/bin/env python3
"""Validate Linux kernel byte-range lock primitives on a fresh ext4 file.

Plan and scope:
- This is a primitive probe for the native-workspace lock bridge.  It prints
  ``PRIMITIVE_PASS`` only when the local Linux kernel preserves the lock
  properties that the bridge depends on; it is not an AFS, FUSE, or full POSIX
  qualification.
- The parent process owns F_OFD_SETLK/F_OFD_GETLK locks.  A child process owns
  classic process locks through F_SETLK/F_GETLK so the two lock owners are real
  independent kernel owners.
- Evidence records raw syscall command, range, lock type, errno, pid, child
  identity, and exit status.  Unsupported lock APIs or unsupported errno values
  fail the primitive instead of being treated as conflicts.
"""
from __future__ import annotations

import argparse
import ctypes
import errno
import fcntl
import json
import os
import platform
import selectors
import shutil
import subprocess
import sys
import tempfile
import time
from pathlib import Path
from typing import Any


F_OFD_GETLK = getattr(fcntl, "F_OFD_GETLK", None)
F_OFD_SETLK = getattr(fcntl, "F_OFD_SETLK", None)

LIBC = ctypes.CDLL(None, use_errno=True)


class Flock(ctypes.Structure):
    _fields_ = [
        ("l_type", ctypes.c_short),
        ("l_whence", ctypes.c_short),
        ("l_start", ctypes.c_longlong),
        ("l_len", ctypes.c_longlong),
        ("l_pid", ctypes.c_int),
    ]


LIBC.fcntl.argtypes = [ctypes.c_int, ctypes.c_int, ctypes.POINTER(Flock)]
LIBC.fcntl.restype = ctypes.c_int


def errno_name(value: int | None) -> str | None:
    return None if value is None else errno.errorcode.get(value, f"ERRNO_{value}")


def lock_name(value: int) -> str:
    if value == fcntl.F_RDLCK:
        return "F_RDLCK"
    if value == fcntl.F_WRLCK:
        return "F_WRLCK"
    if value == fcntl.F_UNLCK:
        return "F_UNLCK"
    return f"LOCK_{value}"


def cmd_name(value: int) -> str:
    mapping = {
        fcntl.F_SETLK: "F_SETLK",
        fcntl.F_GETLK: "F_GETLK",
        F_OFD_SETLK: "F_OFD_SETLK",
        F_OFD_GETLK: "F_OFD_GETLK",
    }
    return mapping.get(value, f"FCNTL_{value}")


def conflict_errno_ok(value: int | None) -> bool:
    return value in (errno.EACCES, errno.EAGAIN)


def unsupported_errno(value: int | None) -> bool:
    return value in (errno.ENOLCK, errno.ENOSYS, errno.EOPNOTSUPP, errno.EINVAL)


def require(condition: bool, message: str, detail: Any | None = None) -> None:
    if not condition:
        raise RuntimeError(json.dumps({"message": message, "detail": detail}, sort_keys=True))


def fcntl_lock(fd: int, cmd: int, lock_type: int, start: int, length: int) -> dict[str, Any]:
    lock = Flock(lock_type, os.SEEK_SET, start, length, 0)
    ctypes.set_errno(0)
    rc = LIBC.fcntl(fd, cmd, ctypes.byref(lock))
    err = ctypes.get_errno() if rc < 0 else None
    return {
        "ok": rc == 0,
        "rc": rc,
        "cmd": cmd_name(cmd),
        "lock_type": lock_name(lock_type),
        "start": start,
        "length": length,
        "errno": err,
        "errno_name": errno_name(err),
        "result": {
            "l_type": int(lock.l_type),
            "l_type_name": lock_name(lock.l_type),
            "l_whence": int(lock.l_whence),
            "l_start": int(lock.l_start),
            "l_len": int(lock.l_len),
            "l_pid": int(lock.l_pid),
        },
    }


def setlk(fd: int, cmd: int, lock_type: int, start: int, length: int) -> dict[str, Any]:
    return fcntl_lock(fd, cmd, lock_type, start, length)


def getlk(fd: int, cmd: int, lock_type: int, start: int, length: int) -> dict[str, Any]:
    return fcntl_lock(fd, cmd, lock_type, start, length)


def identity() -> dict[str, Any]:
    return {
        "pid": os.getpid(),
        "ppid": os.getppid(),
        "uid": os.getuid(),
        "gid": os.getgid(),
        "platform": platform.platform(),
        "machine": platform.machine(),
        "python": sys.version.split()[0],
    }


def emit(obj: dict[str, Any]) -> None:
    print(json.dumps(obj, sort_keys=True), flush=True)


def child_main(args: argparse.Namespace) -> int:
    path = Path(args.path)
    fd = os.open(path, os.O_RDWR)
    keep: list[int] = [fd]
    try:
        if args.op == "try":
            result = setlk(fd, fcntl.F_SETLK, args.lock_type, args.start, args.length)
            emit({"event": "RESULT", "op": args.op, "identity": identity(), "result": result})
            if result["ok"]:
                setlk(fd, fcntl.F_SETLK, fcntl.F_UNLCK, args.start, args.length)
            return 0 if result["ok"] or conflict_errno_ok(result["errno"]) else 1
        if args.op == "getlk":
            result = getlk(fd, fcntl.F_GETLK, args.lock_type, args.start, args.length)
            emit({"event": "RESULT", "op": args.op, "identity": identity(), "result": result})
            return 0 if result["ok"] else 1
        if args.op == "hold":
            result = setlk(fd, fcntl.F_SETLK, args.lock_type, args.start, args.length)
            if not result["ok"]:
                emit({"event": "ERROR", "op": args.op, "identity": identity(), "result": result})
                return 1
            emit({"event": "READY", "op": args.op, "identity": identity(), "result": result})
            sys.stdin.readline()
            unlock = setlk(fd, fcntl.F_SETLK, fcntl.F_UNLCK, args.start, args.length)
            emit({"event": "EXIT", "op": args.op, "identity": identity(), "unlock": unlock})
            return 0
        if args.op == "posix-close-control":
            fd2 = os.open(path, os.O_RDWR)
            keep.append(fd2)
            first = setlk(fd, fcntl.F_SETLK, fcntl.F_WRLCK, args.start, args.length)
            if not first["ok"]:
                emit({"event": "ERROR", "op": args.op, "identity": identity(), "result": first})
                return 1
            emit({"event": "LOCKED", "op": args.op, "identity": identity(), "result": first})
            sys.stdin.readline()
            os.close(fd2)
            keep.remove(fd2)
            emit({"event": "CLOSED_OTHER_FD", "op": args.op, "identity": identity(), "closed_fd": fd2})
            sys.stdin.readline()
            return 0
        raise RuntimeError(f"unknown child op {args.op}")
    finally:
        for descriptor in reversed(keep):
            try:
                os.close(descriptor)
            except OSError:
                pass


class Child:
    def __init__(self, argv: list[str], timeout: float = 5.0) -> None:
        self.argv = argv
        self.proc = subprocess.Popen(
            argv,
            stdin=subprocess.PIPE,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
        )
        self.timeout = timeout
        self.events: list[dict[str, Any]] = []
        self._stdout_buffer = b""
        self._stdout_eof = False

    def snapshot(self) -> dict[str, Any]:
        stderr = b""
        if self.proc.stderr is not None and self.proc.poll() is not None:
            stderr = self.proc.stderr.read() or b""
        return {
            "argv": self.argv,
            "returncode": self.proc.poll(),
            "stderr": stderr.decode("utf-8", "replace"),
            "events": self.events,
        }

    def terminate_owned(self) -> dict[str, Any]:
        if self.proc.poll() is None:
            self.proc.kill()
        return self.wait()

    def _readline_bounded(self, deadline: float) -> str | None:
        if b"\n" in self._stdout_buffer:
            line, self._stdout_buffer = self._stdout_buffer.split(b"\n", 1)
            return line.decode("utf-8", "replace")
        require(self.proc.stdout is not None, "child stdout missing")
        selector = selectors.DefaultSelector()
        selector.register(self.proc.stdout.fileno(), selectors.EVENT_READ)
        try:
            while time.monotonic() < deadline:
                if self.proc.poll() is not None and not self._stdout_buffer and self._stdout_eof:
                    return None
                wait = max(0.0, deadline - time.monotonic())
                events = selector.select(wait)
                if not events:
                    return None
                chunk = os.read(self.proc.stdout.fileno(), 4096)
                if not chunk:
                    self._stdout_eof = True
                    if self._stdout_buffer:
                        line, self._stdout_buffer = self._stdout_buffer, b""
                        return line.decode("utf-8", "replace")
                    return None
                self._stdout_buffer += chunk
                if b"\n" in self._stdout_buffer:
                    line, self._stdout_buffer = self._stdout_buffer.split(b"\n", 1)
                    return line.decode("utf-8", "replace")
            return None
        finally:
            selector.close()

    def read_event(self, expected: str) -> dict[str, Any]:
        deadline = time.monotonic() + self.timeout
        while time.monotonic() < deadline:
            line = self._readline_bounded(deadline)
            if not line:
                if self.proc.poll() is not None:
                    raise RuntimeError(json.dumps({"message": f"child exited before {expected}", "child": self.snapshot()}, sort_keys=True))
                continue
            event = json.loads(line)
            self.events.append(event)
            if event.get("event") == expected:
                return event
        child = self.terminate_owned()
        raise TimeoutError(json.dumps({"message": f"timed out waiting for child event {expected}", "child": child}, sort_keys=True))

    def send(self) -> None:
        require(self.proc.stdin is not None, "child stdin missing")
        self.proc.stdin.write(b"\n")
        self.proc.stdin.flush()

    def wait(self) -> dict[str, Any]:
        stdout, stderr = self.proc.communicate(timeout=self.timeout)
        combined_stdout = self._stdout_buffer + stdout
        self._stdout_buffer = b""
        for line in combined_stdout.splitlines():
            if line.strip():
                self.events.append(json.loads(line.decode("utf-8", "replace")))
        return {
            "argv": self.argv,
            "returncode": self.proc.returncode,
            "stderr": stderr.decode("utf-8", "replace"),
            "events": self.events,
        }

    def close(self) -> dict[str, Any]:
        if self.proc.poll() is None:
            self.send()
        return self.wait()


def child_argv(path: Path, op: str, lock_type: int, start: int, length: int) -> list[str]:
    return [
        sys.executable,
        str(Path(__file__).resolve()),
        "--child",
        "--path",
        str(path),
        "--op",
        op,
        "--lock-type",
        str(lock_type),
        "--start",
        str(start),
        "--length",
        str(length),
    ]


def run_child(path: Path, op: str, lock_type: int, start: int, length: int) -> dict[str, Any]:
    child = Child(child_argv(path, op, lock_type, start, length))
    try:
        event = child.read_event("RESULT")
        proc = child.wait()
        proc["primary_event"] = event
        require(proc["returncode"] == 0, "child exited nonzero after RESULT", proc)
        return proc
    except Exception:
        child.terminate_owned()
        raise


def assert_success(result: dict[str, Any], label: str) -> None:
    require(result["ok"], f"{label} succeeds", result)


def assert_conflict(result: dict[str, Any], label: str) -> None:
    require(not result["ok"], f"{label} conflicts", result)
    require(conflict_errno_ok(result["errno"]), f"{label} uses EACCES/EAGAIN, not unsupported errno", result)


def assert_unlocked(result: dict[str, Any], label: str) -> None:
    require(result["ok"] and result["result"]["l_type_name"] == "F_UNLCK", f"{label} reports unlocked", result)


def assert_lock_seen(
    result: dict[str, Any],
    label: str,
    *,
    lock_type: str,
    start: int,
    length: int,
    pid: int,
) -> None:
    require(result["ok"] and result["result"]["l_type_name"] in {"F_RDLCK", "F_WRLCK"}, f"{label} reports conflicting lock", result)
    observed = result["result"]
    require(observed["l_type_name"] == lock_type, f"{label} reports exact lock type", result)
    require(observed["l_start"] == start, f"{label} reports exact start", result)
    require(observed["l_len"] == length, f"{label} reports exact length", result)
    require(observed["l_pid"] == pid, f"{label} reports exact owner pid", result)


def child_result(record: dict[str, Any]) -> dict[str, Any]:
    return record["primary_event"]["result"]


def record_case(cases: list[dict[str, Any]], name: str, payload: dict[str, Any]) -> None:
    cases.append({"name": name, "status": "PASS", "payload": payload})


def run_probe(work_dir: Path, output: Path) -> dict[str, Any]:
    require(platform.system() == "Linux", "native_lock_kernel requires Linux")
    require(platform.machine() == "aarch64", "native_lock_kernel requires aarch64", platform.machine())
    require(os.geteuid() == 0, "native_lock_kernel requires root")
    require(F_OFD_GETLK is not None and F_OFD_SETLK is not None, "missing F_OFD_* constants")
    require(work_dir.is_absolute() and work_dir.is_dir(), "--work-dir must be an existing absolute directory", str(work_dir))
    require(output.is_absolute(), "--output must be absolute", str(output))
    require(output.parent.exists(), "--output parent must exist", str(output.parent))
    require(not output.exists() and not output.is_symlink(), "--output must be fresh", str(output))

    mount = json.loads(subprocess.check_output(["findmnt", "-T", str(work_dir), "-J"], text=True))
    fs = mount["filesystems"][0]
    require(fs.get("fstype") == "ext4", "--work-dir must resolve to ext4", fs)

    scratch = Path(tempfile.mkdtemp(prefix="native-lock-kernel-", dir=work_dir))
    target = scratch / "target.bin"
    target.write_bytes(b"native-lock-kernel\n" + b"\0" * 4096)
    os.chown(scratch, 0, 0)
    os.chown(target, 0, 0)

    cases: list[dict[str, Any]] = []
    failures: list[dict[str, Any]] = []

    try:
        fd = os.open(target, os.O_RDWR)
        try:
            # OFD lock blocks a true child POSIX owner, and child F_GETLK sees pid=-1.
            ofd = setlk(fd, F_OFD_SETLK, fcntl.F_WRLCK, 0, 100)
            assert_success(ofd, "OFD exclusive lock")
            child_try = run_child(target, "try", fcntl.F_WRLCK, 0, 100)
            assert_conflict(child_result(child_try), "child POSIX overlap against OFD")
            child_getlk = run_child(target, "getlk", fcntl.F_WRLCK, 0, 100)
            seen = child_result(child_getlk)
            assert_lock_seen(seen, "child POSIX GETLK sees parent OFD", lock_type="F_WRLCK", start=0, length=100, pid=-1)
            nonoverlap = run_child(target, "try", fcntl.F_WRLCK, 128, 16)
            assert_success(child_result(nonoverlap), "non-overlap child POSIX lock")
            record_case(cases, "ofd_parent_vs_child_posix", {"set": ofd, "try": child_try, "getlk": child_getlk, "nonoverlap": nonoverlap})
            setlk(fd, F_OFD_SETLK, fcntl.F_UNLCK, 0, 100)

            # Child POSIX lock blocks parent OFD on the same inode.
            holder = Child(child_argv(target, "hold", fcntl.F_WRLCK, 200, 50))
            try:
                ready = holder.read_event("READY")
                parent_getlk = getlk(fd, F_OFD_GETLK, fcntl.F_WRLCK, 200, 50)
                assert_lock_seen(
                    parent_getlk,
                    "parent OFD GETLK sees child POSIX",
                    lock_type="F_WRLCK",
                    start=200,
                    length=50,
                    pid=ready["identity"]["pid"],
                )
                parent_try = setlk(fd, F_OFD_SETLK, fcntl.F_WRLCK, 200, 50)
                assert_conflict(parent_try, "parent OFD overlap against child POSIX")
                holder_exit = holder.close()
                require(holder_exit["returncode"] == 0, "child POSIX holder exits cleanly", holder_exit)
            except Exception:
                holder.terminate_owned()
                raise
            record_case(cases, "child_posix_vs_parent_ofd", {"ready": ready, "getlk": parent_getlk, "try": parent_try, "child_exit": holder_exit})

            # Shared/shared succeeds, shared/exclusive rejects.
            shared = setlk(fd, F_OFD_SETLK, fcntl.F_RDLCK, 300, 50)
            assert_success(shared, "parent OFD shared lock")
            shared_child = run_child(target, "try", fcntl.F_RDLCK, 300, 50)
            assert_success(child_result(shared_child), "child POSIX shared/shared")
            exclusive_child = run_child(target, "try", fcntl.F_WRLCK, 300, 50)
            assert_conflict(child_result(exclusive_child), "child POSIX exclusive against shared")
            record_case(cases, "shared_shared_and_shared_exclusive", {"shared": shared, "shared_child": shared_child, "exclusive_child": exclusive_child})
            setlk(fd, F_OFD_SETLK, fcntl.F_UNLCK, 300, 50)

            # Partial unlock leaves the remaining range protected.
            whole = setlk(fd, F_OFD_SETLK, fcntl.F_WRLCK, 400, 100)
            left_unlock = setlk(fd, F_OFD_SETLK, fcntl.F_UNLCK, 400, 50)
            left_child = run_child(target, "try", fcntl.F_WRLCK, 400, 50)
            right_child = run_child(target, "try", fcntl.F_WRLCK, 450, 50)
            assert_success(whole, "whole OFD range lock")
            assert_success(left_unlock, "left half unlock")
            assert_success(child_result(left_child), "unlocked left half")
            assert_conflict(child_result(right_child), "still-locked right half")
            record_case(cases, "partial_unlock", {"whole": whole, "left_unlock": left_unlock, "left_child": left_child, "right_child": right_child})
            setlk(fd, F_OFD_SETLK, fcntl.F_UNLCK, 450, 50)

            # Classic POSIX locks are process-owned: closing any fd for the inode releases them.
            control = Child(child_argv(target, "posix-close-control", fcntl.F_WRLCK, 600, 50))
            try:
                locked = control.read_event("LOCKED")
                before = setlk(fd, F_OFD_SETLK, fcntl.F_WRLCK, 600, 50)
                assert_conflict(before, "POSIX control lock is initially present")
                control.send()
                closed = control.read_event("CLOSED_OTHER_FD")
                after = setlk(fd, F_OFD_SETLK, fcntl.F_WRLCK, 600, 50)
                assert_success(after, "arbitrary POSIX close released process lock")
                setlk(fd, F_OFD_SETLK, fcntl.F_UNLCK, 600, 50)
                control.send()
                control_exit = control.wait()
                require(control_exit["returncode"] == 0, "POSIX close control child exits cleanly", control_exit)
            except Exception:
                control.terminate_owned()
                raise
            record_case(cases, "posix_owner_any_close_control", {"locked": locked, "before": before, "closed": closed, "after": after, "child_exit": control_exit})

            # OFD locks survive dup/original close and release only after the final close.
            fd2 = os.dup(fd)
            try:
                dup_lock = setlk(fd, F_OFD_SETLK, fcntl.F_WRLCK, 700, 50)
                os.close(fd)
                fd = -1
                conflict = run_child(target, "try", fcntl.F_WRLCK, 700, 50)
                assert_conflict(child_result(conflict), "OFD lock survives original close while dup is open")
            finally:
                try:
                    os.close(fd2)
                except OSError:
                    pass
            final = run_child(target, "try", fcntl.F_WRLCK, 700, 50)
            assert_success(child_result(final), "OFD lock releases after final close")
            record_case(cases, "ofd_dup_final_close", {"lock": dup_lock, "conflict_after_original_close": conflict, "success_after_final_close": final})
        finally:
            if fd >= 0:
                os.close(fd)
    except Exception as exc:
        failures.append({"message": str(exc)})

    status = "PRIMITIVE_PASS" if not failures else "FAIL"
    report = {
        "schema": "afs.native_lock_kernel.v1",
        "status": status,
        "primitive_interop": status == "PRIMITIVE_PASS",
        "transparent_classic_posix": False,
        "primitive_only": True,
        "not_product_pass": True,
        "limits": [
            "OFD locks are not transparent classic POSIX locks.",
            "Linux F_GETLK reports a conflicting OFD lock with raw l_pid=-1; this probe records that fact and does not reinterpret it as a correct process pid.",
            "Blocking, deadlock detection, namespace pid translation, and product lock bridging are outside this non-blocking primitive probe.",
        ],
        "work_dir": str(work_dir),
        "output": str(output),
        "scratch": str(scratch),
        "target": str(target),
        "identity": identity(),
        "mount": fs,
        "cases": cases,
        "summary": {"passed": len(cases), "failed": len(failures)},
        "failures": failures,
    }
    output.mkdir(mode=0o700)
    (output / "report.json").write_text(json.dumps(report, indent=2, sort_keys=True) + "\n", encoding="utf-8")
    if failures:
        (output / "failure.json").write_text(json.dumps(failures, indent=2, sort_keys=True) + "\n", encoding="utf-8")
    return report


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Validate Linux kernel OFD/POSIX byte-range lock primitives")
    parser.add_argument("--work-dir", type=Path, help="existing absolute ext4 parent for a fresh root-owned temp file")
    parser.add_argument("--output", type=Path, help="fresh absolute output directory")
    parser.add_argument("--child", action="store_true", help=argparse.SUPPRESS)
    parser.add_argument("--path", type=Path, help=argparse.SUPPRESS)
    parser.add_argument("--op", choices=("try", "getlk", "hold", "posix-close-control"), help=argparse.SUPPRESS)
    parser.add_argument("--lock-type", type=int, default=fcntl.F_WRLCK, help=argparse.SUPPRESS)
    parser.add_argument("--start", type=int, default=0, help=argparse.SUPPRESS)
    parser.add_argument("--length", type=int, default=1, help=argparse.SUPPRESS)
    return parser


def main(argv: list[str] | None = None) -> int:
    args = build_parser().parse_args(argv)
    if args.child:
        return child_main(args)
    require(args.work_dir is not None and args.output is not None, "--work-dir and --output are required")
    report = run_probe(args.work_dir.resolve(), args.output)
    print(report["status"])
    return 0 if report["status"] == "PRIMITIVE_PASS" else 1


if __name__ == "__main__":
    try:
        raise SystemExit(main())
    except Exception as exc:
        print(json.dumps({"status": "FAIL", "message": str(exc)}, sort_keys=True), file=sys.stderr)
        raise SystemExit(1)
