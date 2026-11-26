#!/usr/bin/env python3
"""Version-1 identity-aware PTY adapter carried by Execd /pty WebSocket."""
import base64
import ctypes
import errno
import fcntl
import json
import os
import select
import signal
import struct
import sys
import termios
import time
import tty

MAX_FRAME = 262144


def emit(kind, **fields):
    data = (json.dumps({"v": 1, "type": kind, **fields}, ensure_ascii=True) + "\n").encode()
    while data:
        written = os.write(1, data)
        data = data[written:]


def dimensions(frame):
    cols, rows = frame.get("cols"), frame.get("rows")
    if type(cols) is not int or type(rows) is not int or not (0 < cols <= 65535 and 0 < rows <= 65535):
        raise ValueError("invalid_terminal_size")
    return struct.pack("HHHH", rows, cols, 0, 0)


def identity(frame):
    uid, gid, groups = frame.get("uid"), frame.get("gid"), frame.get("groups")
    if not isinstance(groups, list):
        raise ValueError("terminal_identity_mismatch")
    if len(groups) > 256:
        raise ValueError("terminal_identity_mismatch")
    for value in (uid, gid, *groups):
        if type(value) is not int or not (0 <= value < 2**32 - 1):
            raise ValueError("terminal_identity_mismatch")
    return uid, gid, sorted(set(groups))


def init_child(frame, error_fd):
    code = "terminal_identity_mismatch"
    try:
        os.chdir("/")
        uid, gid, groups = identity(frame)
        if os.geteuid() == 0:
            os.setgroups(groups)
            os.setresgid(gid, gid, gid)
            os.setresuid(uid, uid, uid)
        if (os.getresuid() != (uid, uid, uid) or os.getresgid() != (gid, gid, gid)
                or sorted(set(os.getgroups())) != groups):
            raise ValueError(code)
        # Refuse inherited privilege changes across a shell exec.
        libc = ctypes.CDLL(None, use_errno=True)
        if libc.prctl(38, 1, 0, 0, 0) != 0:  # PR_SET_NO_NEW_PRIVS
            raise ValueError(code)
        code = "terminal_home_unavailable"
        home = frame["home"]
        if not isinstance(home, str) or not home.startswith("/"):
            raise ValueError(code)
        if not os.path.isdir(home) or not os.access(home, os.R_OK | os.X_OK):
            raise ValueError(code)
        code = "terminal_cwd_unavailable"
        cwd = frame["cwd"]
        if not isinstance(cwd, str) or not cwd.startswith("/"):
            raise ValueError(code)
        os.chdir(cwd)
        code = "terminal_adapter_unavailable"
        shell = frame["shell"]
        if not isinstance(shell, str) or not shell.startswith("/"):
            raise ValueError(code)
        if not os.path.isfile(shell) or not os.access(shell, os.X_OK):
            raise ValueError(code)
        environment = {"HOME": home, "USER": str(uid), "LOGNAME": str(uid), "SHELL": shell,
                       "TERM": "xterm-256color", "PATH": "/usr/local/bin:/usr/bin:/bin", "LANG": "C.UTF-8"}
        ready = {"type": "ready", "uid": os.getuid(), "gid": os.getgid(),
                 "groups": os.getgroups(), "home": home, "cwd": os.getcwd()}
        os.write(error_fd, (json.dumps(ready) + "\n").encode())
        os.execve(shell, [shell, "-i"], environment)
    except (OSError, ValueError, KeyError, TypeError):
        os.write(error_fd, (json.dumps({"type": "error", "code": code}) + "\n").encode())
        # Public fork-child exit API: do not run inherited cleanup or flush buffers.
        os._exit(126)


def main():
    pid = None
    master = None
    try:
        if sys.argv[1:] == ["--pty"]:
            tty.setraw(0, termios.TCSANOW)
            emit("transport_ready")
        elif sys.argv[1:]:
            raise ValueError("terminal_protocol_invalid")
        os.chdir("/")
        raw = b""
        pending = b""
        while b"\n" not in raw:
            chunk = os.read(0, 32768)
            if not chunk:
                raise ValueError("terminal_protocol_invalid")
            raw += chunk
            if len(raw) > MAX_FRAME:
                raise ValueError("terminal_protocol_invalid")
        raw, pending = raw.split(b"\n", 1)
        raw += b"\n"
        if not raw.endswith(b"\n") or len(raw) > MAX_FRAME:
            raise ValueError("terminal_protocol_invalid")
        frame = json.loads(raw)
        if frame.get("v") != 1 or frame.get("type") != "init":
            raise ValueError("terminal_protocol_invalid")
        identity(frame)
        size = dimensions(frame)
        error_read, error_write = os.pipe2(os.O_CLOEXEC)
        pid, master = os.forkpty()
        if pid == 0:
            os.close(error_read)
            init_child(frame, error_write)
        os.close(error_write)
        status = b""
        with os.fdopen(error_read, "rb") as stream:
            status = stream.read(MAX_FRAME)
        messages = [json.loads(line) for line in status.splitlines()]
        if not messages or messages[-1].get("type") != "ready":
            emit("error", code=(messages[-1].get("code", "terminal_adapter_unavailable")
                                if messages else "terminal_adapter_unavailable"))
            return 1
        fcntl.ioctl(master, termios.TIOCSWINSZ, size)
        ready = messages[-1]
        ready.pop("type")
        emit("ready", **ready)
        while True:
            readable, _, _ = select.select([0, master], [], [], 0 if b"\n" in pending else 0.2)
            if master in readable:
                try:
                    output = os.read(master, 32768)
                except OSError as error:
                    if error.errno != errno.EIO:
                        raise
                    output = b""
                if output:
                    emit("output", data=base64.b64encode(output).decode())
                else:
                    _, status = os.waitpid(pid, 0)
                    pid = None
                    emit("exited", exit_code=os.waitstatus_to_exitcode(status))
                    return 0
            if 0 in readable or b"\n" in pending:
                if 0 in readable:
                    chunk = os.read(0, 32768)
                    if not chunk:
                        break
                    pending += chunk
                if len(pending) > MAX_FRAME:
                    raise ValueError("terminal_protocol_invalid")
                while b"\n" in pending:
                    line, pending = pending.split(b"\n", 1)
                    command = json.loads(line)
                    if command.get("v") != 1:
                        raise ValueError("terminal_protocol_invalid")
                    kind = command.get("type")
                    if kind == "input":
                        data = base64.b64decode(command["data"], validate=True)
                        while data:
                            written = os.write(master, data)
                            data = data[written:]
                    elif kind == "resize":
                        fcntl.ioctl(master, termios.TIOCSWINSZ, dimensions(command))
                    elif kind == "eof":
                        os.write(master, b"\x04")
                    elif kind == "close":
                        return 0
                    else:
                        raise ValueError("terminal_protocol_invalid")
        return 0
    except (OSError, ValueError, KeyError, TypeError):
        emit("error", code="terminal_protocol_invalid")
        return 1
    finally:
        if pid is not None:
            try:
                os.killpg(pid, signal.SIGHUP)
            except ProcessLookupError:
                pass
            deadline = time.monotonic() + 1
            while time.monotonic() < deadline:
                ended, _ = os.waitpid(pid, os.WNOHANG)
                if ended:
                    break
                time.sleep(0.02)
            else:
                try:
                    os.killpg(pid, signal.SIGKILL)
                except ProcessLookupError:
                    pass
                os.waitpid(pid, 0)
        if master is not None:
            os.close(master)


if __name__ == "__main__":
    def terminate(_signum, _frame):
        raise SystemExit(0)
    signal.signal(signal.SIGHUP, terminate)
    signal.signal(signal.SIGTERM, terminate)
    sys.exit(main())
