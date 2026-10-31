"""Owned background writer with native control files, never progress I/O in FUSE."""

import errno
import hashlib
import json
import os
from pathlib import Path
import sys
import time


def emit(directory, name, value):
    temporary = directory / (name + ".pending")
    temporary.write_text(json.dumps(value))
    temporary.replace(directory / (name + ".json"))


def await_signal(directory, name):
    deadline = time.monotonic() + 180
    while not (directory / (name + ".json")).exists():
        assert time.monotonic() < deadline, "missing controller signal: " + name
        time.sleep(0.05)


def main():
    state, name, action = sys.argv[1:]
    directory, path = Path(state), Path(name)
    fd = os.open(path, os.O_CREAT | os.O_TRUNC | os.O_RDWR, 0o600)
    try:
        emit(directory, "opened", {"fd": fd, "pid": os.getpid()})
        await_signal(directory, "write")
        body = b"q" * (16 * 1024**2)
        accepted = os.pwrite(fd, body, 0)
        assert accepted > 0, "no admitted data before the required replica failed"
        error = None
        try:
            os.fsync(fd)
        except OSError as exc:
            error = exc.errno
        assert error is not None, "fsync reported durability without its required replica"
        probe = None
        try:
            os.getxattr(fd, "security.capability")
        except OSError as exc:
            probe = exc.errno
        assert probe == errno.EOPNOTSUPP, (probe, error)
        emit(directory, "failed", {"accepted_bytes": accepted, "fsync_errno": error, "xattr_errno": probe})
        if action == "close":
            closed_fd, fd = fd, None
            close_errno = None
            try:
                os.close(closed_fd)
            except OSError as exc:
                close_errno = exc.errno
            try:
                os.fstat(closed_fd)
                raise AssertionError("closed FD remained live")
            except OSError as exc:
                assert exc.errno == errno.EBADF
            emit(directory, "done", {
                "closed": True, "close_errno": close_errno, "failed_data_durability_claimed": False,
            })
            return
        await_signal(directory, "recover")
        attempts = []
        for _ in range(6):
            try:
                os.fsync(fd)
                attempts.append({"passed": True})
                break
            except OSError as exc:
                attempts.append({"passed": False, "errno": exc.errno})
                time.sleep(0.2)
        assert attempts[-1]["passed"], attempts
        os.close(fd)
        fd = None
        with path.open("rb", buffering=0) as stream:
            data = stream.read()
        assert data == body[:accepted], (len(data), accepted)
        emit(directory, "done", {"bytes": accepted, "sha256": hashlib.sha256(data).hexdigest(), "attempts": attempts})
    finally:
        if fd is not None:
            os.close(fd)


if __name__ == "__main__":
    main()
