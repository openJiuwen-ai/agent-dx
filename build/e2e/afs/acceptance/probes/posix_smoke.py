#!/usr/bin/env python3
"""Short real POSIX smoke probe for AFS acceptance.

This probe intentionally covers a small set of user-visible filesystem contracts.
It is not a replacement for pjdfstest, LTP, FSx, or differential random suites.
"""
from __future__ import annotations

import argparse
import base64
import ctypes
import errno
import json
import os
import platform
import random
import shutil
import stat as stat_module
import subprocess
import sys
import time
import traceback
from pathlib import Path
from typing import Any, Callable


_LIBC = ctypes.CDLL(None, use_errno=True)
_LIBC.getxattr.argtypes = [ctypes.c_char_p, ctypes.c_char_p, ctypes.c_void_p, ctypes.c_size_t]
_LIBC.getxattr.restype = ctypes.c_ssize_t


def _now() -> str:
    return time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime())


def _errno_name(value: int | None) -> str | None:
    if value is None:
        return None
    return errno.errorcode.get(value, f"ERRNO_{value}")


def _b64(value: bytes) -> str:
    return base64.b64encode(value).decode("ascii")


def _write(path: Path, data: bytes) -> None:
    path.write_bytes(data)


def _read(path: Path) -> bytes:
    return path.read_bytes()


def _stat_dict(path: Path) -> dict[str, Any]:
    st = path.lstat()
    file_type = "regular" if stat_module.S_ISREG(st.st_mode) else (
        "directory" if stat_module.S_ISDIR(st.st_mode) else (
            "symlink" if stat_module.S_ISLNK(st.st_mode) else "other"
        )
    )
    return {
        "mode_octal": oct(st.st_mode & 0o7777),
        "file_type": file_type,
        "is_regular": stat_module.S_ISREG(st.st_mode),
        "uid": st.st_uid,
        "gid": st.st_gid,
        "size": st.st_size,
        "nlink": st.st_nlink,
        "blocks_512b": getattr(st, "st_blocks", None),
        "ino": st.st_ino,
    }


def _expect(condition: bool, message: str, details: dict[str, Any] | None = None) -> dict[str, Any]:
    if not condition:
        raise AssertionError(json.dumps({"message": message, "details": details or {}}, sort_keys=True))
    return {"ok": True, "message": message, "details": details or {}}


def _raw_getxattr(path: Path, name: str, size: int | None = None) -> dict[str, Any]:
    path_b = os.fsencode(path)
    name_b = name.encode("utf-8")
    ctypes.set_errno(0)
    if size is None or size == 0:
        result = _LIBC.getxattr(path_b, name_b, None, 0)
        err = ctypes.get_errno() if result < 0 else None
        return {
            "result": int(result),
            "errno": err,
            "errno_name": _errno_name(err),
            "value_b64": None,
            "size": 0,
        }
    buf = ctypes.create_string_buffer(size)
    result = _LIBC.getxattr(path_b, name_b, buf, size)
    err = ctypes.get_errno() if result < 0 else None
    value = bytes(buf.raw[:result]) if result >= 0 else None
    return {
        "result": int(result),
        "errno": err,
        "errno_name": _errno_name(err),
        "value_b64": _b64(value) if value is not None else None,
        "size": size,
    }


class Probe:
    def __init__(self, mount: Path, evidence: Path, base_dir: Path | None = None) -> None:
        self.mount = mount.resolve()
        self.evidence = evidence.resolve()
        self.base_dir = self._resolve_base_dir(base_dir)
        self.records: list[dict[str, Any]] = []
        self.failures: list[dict[str, Any]] = []
        self.run_id = f"posix-smoke-{time.strftime('%Y%m%dT%H%M%SZ', time.gmtime())}-{os.getpid()}-{random.randrange(1_000_000):06d}"
        self.root = self.base_dir / f".afs-posix-smoke-{self.run_id}"
        self.fixture_kept = True

    def _resolve_base_dir(self, base_dir: Path | None) -> Path:
        if base_dir is None:
            return self.mount
        if base_dir.is_absolute():
            return base_dir.resolve()
        return (self.mount / base_dir).resolve()

    def step(self, name: str, func: Callable[[], dict[str, Any]]) -> None:
        started = time.time()
        record: dict[str, Any] = {
            "name": name,
            "started_at": _now(),
            "fixture_root": str(self.root),
        }
        try:
            details = func()
            record.update({
                "ok": True,
                "exit_status": 0,
                "errno": None,
                "errno_name": None,
                "details": details,
            })
        except OSError as exc:
            record.update({
                "ok": False,
                "exit_status": 1,
                "errno": exc.errno,
                "errno_name": _errno_name(exc.errno),
                "exception": type(exc).__name__,
                "message": str(exc),
                "traceback": traceback.format_exc(),
            })
            self.failures.append(record)
        except Exception as exc:  # noqa: BLE001 - probe must serialize raw failure
            record.update({
                "ok": False,
                "exit_status": 1,
                "errno": None,
                "errno_name": None,
                "exception": type(exc).__name__,
                "message": str(exc),
                "traceback": traceback.format_exc(),
            })
            self.failures.append(record)
        finally:
            record["duration_ms"] = round((time.time() - started) * 1000, 3)
            self.records.append(record)

    def setup(self) -> None:
        self.evidence.mkdir(parents=True, exist_ok=True)
        _expect(self.mount.is_dir(), "mount path is a directory", {"mount": str(self.mount)})
        self.base_dir.mkdir(mode=0o755, parents=True, exist_ok=True)
        _expect(self.base_dir.is_dir(), "base directory is a directory", {"base_dir": str(self.base_dir)})
        try:
            self.root.mkdir(mode=0o700)
        except FileExistsError:
            raise RuntimeError(f"exclusive fixture already exists: {self.root}")

    def cleanup_or_keep(self) -> None:
        if self.failures:
            self.fixture_kept = True
            return
        try:
            shutil.rmtree(self.root)
            self.fixture_kept = False
        except OSError as exc:
            self.fixture_kept = True
            record = {
                "name": "cleanup_fixture",
                "started_at": _now(),
                "fixture_root": str(self.root),
                "ok": False,
                "exit_status": 1,
                "errno": exc.errno,
                "errno_name": _errno_name(exc.errno),
                "exception": type(exc).__name__,
                "message": str(exc),
                "traceback": traceback.format_exc(),
                "details": {"cleanup_target": str(self.root)},
                "duration_ms": 0.0,
            }
            self.records.append(record)
            self.failures.append(record)

    def write_report(self) -> None:
        self.evidence.mkdir(parents=True, exist_ok=True)
        mount_info = subprocess.run(
            ["findmnt", "-T", str(self.mount), "-o", "TARGET,SOURCE,FSTYPE,OPTIONS", "--json"],
            text=True,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            check=False,
        )
        report = {
            "schema": "afs.posix_smoke.v1",
            "run_id": self.run_id,
            "created_at": _now(),
            "mount": str(self.mount),
            "evidence": str(self.evidence),
            "base_dir": str(self.base_dir),
            "fixture_root": str(self.root),
            "fixture_kept": self.fixture_kept,
            "platform": {
                "system": platform.system(),
                "release": platform.release(),
                "machine": platform.machine(),
                "python": platform.python_version(),
                "uid": os.geteuid(),
                "gid": os.getegid(),
            },
            "findmnt": {
                "exit_status": mount_info.returncode,
                "stdout": mount_info.stdout,
                "stderr": mount_info.stderr,
            },
            "summary": {
                "status": "PASS" if not self.failures else "FAIL",
                "steps": len(self.records),
                "failed": len(self.failures),
                "passed": len(self.records) - len(self.failures),
            },
            "steps": self.records,
            "notes": [
                "Short targeted probe only; not a complete POSIX suite.",
                "Unsupported behavior is reported as failure for this probe.",
                "xattr length query, ERANGE, exact-size and empty values use libc getxattr through ctypes to preserve raw Linux errno.",
            ],
        }
        (self.evidence / "report.json").write_text(json.dumps(report, indent=2, sort_keys=True) + "\n")
        (self.evidence / "summary.txt").write_text(
            f"status={report['summary']['status']} steps={report['summary']['steps']} "
            f"failed={report['summary']['failed']} fixture={self.root} kept={self.fixture_kept}\n"
        )

    def test_mkdir_readdir_rmdir(self) -> dict[str, Any]:
        d = self.root / "dir-basic"
        d.mkdir(mode=0o755)
        child = d / "child.txt"
        _write(child, b"child")
        listing = sorted(os.listdir(d))
        _expect(listing == ["child.txt"], "readdir sees created child", {"listing": listing})
        child.unlink()
        d.rmdir()
        _expect(not d.exists(), "rmdir removes empty directory")
        return {"directory": str(d), "listing": listing}


    def test_regular_mknod_readwrite(self) -> dict[str, Any]:
        p = self.root / "mknod-regular"
        os.mknod(p, stat_module.S_IFREG | 0o640)
        st_empty = _stat_dict(p)
        _expect(st_empty["is_regular"], "mknod S_IFREG creates a regular file", st_empty)
        _expect(st_empty["mode_octal"] == "0o640", "mknod regular file mode is visible", st_empty)
        _expect(st_empty["uid"] == os.geteuid(), "mknod regular file uid is effective uid", st_empty)
        _expect(st_empty["size"] == 0, "mknod regular file starts empty", st_empty)
        fd = os.open(p, os.O_RDWR)
        try:
            os.write(fd, b"mknod-data")
            os.fsync(fd)
            os.lseek(fd, 0, os.SEEK_SET)
            data = os.read(fd, 10)
            _expect(data == b"mknod-data", "mknod regular file supports read/write/fsync", {"data_b64": _b64(data)})
        finally:
            os.close(fd)
        st_final = _stat_dict(p)
        return {"path": str(p), "empty_stat": st_empty, "final_stat": st_final, "data_b64": _b64(_read(p))}

    def test_mode_uid(self) -> dict[str, Any]:
        p = self.root / "mode-uid.bin"
        _write(p, b"mode")
        os.chmod(p, 0o640)
        st = _stat_dict(p)
        _expect(st["mode_octal"] == "0o640", "chmod mode is visible", st)
        _expect(st["uid"] == os.geteuid(), "created file uid is effective uid", st)
        return {"path": str(p), "stat": st}

    def test_hardlink_nlink(self) -> dict[str, Any]:
        a = self.root / "hardlink-a"
        b = self.root / "hardlink-b"
        _write(a, b"hardlink-data")
        os.link(a, b)
        st_a = _stat_dict(a)
        st_b = _stat_dict(b)
        _expect(st_a["nlink"] == 2 and st_b["nlink"] == 2, "hardlink nlink is 2", {"a": st_a, "b": st_b})
        _expect(_read(a) == _read(b) == b"hardlink-data", "hardlink content aliases")
        return {"a": str(a), "b": str(b), "stat_a": st_a, "stat_b": st_b}

    def test_symlink_readlink(self) -> dict[str, Any]:
        target = self.root / "symlink-target"
        link = self.root / "symlink-link"
        _write(target, b"target")
        os.symlink(target.name, link)
        value = os.readlink(link)
        _expect(value == target.name, "readlink returns stored relative target", {"readlink": value})
        _expect(_read(link) == b"target", "symlink follows to target")
        return {"target": str(target), "link": str(link), "readlink": value}

    def test_rename_replacement(self) -> dict[str, Any]:
        src = self.root / "rename-src"
        dst = self.root / "rename-dst"
        _write(src, b"replacement")
        _write(dst, b"old")
        os.replace(src, dst)
        _expect(not src.exists(), "source is gone after replacement rename")
        _expect(_read(dst) == b"replacement", "destination contains replacement data")
        return {"src": str(src), "dst": str(dst), "dst_data_b64": _b64(_read(dst))}

    def test_open_unlink_fd(self) -> dict[str, Any]:
        p = self.root / "open-unlink"
        fd = os.open(p, os.O_CREAT | os.O_RDWR | os.O_TRUNC, 0o600)
        try:
            os.write(fd, b"abc")
            os.fsync(fd)
            os.unlink(p)
            _expect(not p.exists(), "path removed while fd stays open")
            os.lseek(fd, 0, os.SEEK_SET)
            first = os.read(fd, 3)
            os.lseek(fd, 0, os.SEEK_END)
            os.write(fd, b"def")
            os.lseek(fd, 0, os.SEEK_SET)
            final = os.read(fd, 6)
            _expect(first == b"abc" and final == b"abcdef", "unlinked fd remains readable and writable", {"first": _b64(first), "final": _b64(final)})
            return {"path": str(p), "first_b64": _b64(first), "final_b64": _b64(final)}
        finally:
            os.close(fd)

    def test_xattr_user(self) -> dict[str, Any]:
        p = self.root / "xattr-file"
        _write(p, b"xattr")
        name = "user.afs_posix_smoke"
        missing_errno = None
        try:
            os.getxattr(p, name)
            raise AssertionError("missing xattr unexpectedly existed")
        except OSError as exc:
            missing_errno = exc.errno
            _expect(exc.errno in {getattr(errno, "ENODATA", 61), getattr(errno, "ENOATTR", 93)}, "missing xattr returns ENODATA/ENOATTR", {"errno": exc.errno, "errno_name": _errno_name(exc.errno)})
        binary = b"\x00afs\xffvalue"
        os.setxattr(p, name, binary, os.XATTR_CREATE)
        got = os.getxattr(p, name)
        _expect(got == binary, "binary xattr roundtrips", {"value_b64": _b64(got)})
        length_query = _raw_getxattr(p, name)
        _expect(length_query["result"] == len(binary), "libc getxattr length query returns binary value size", length_query)
        erange = _raw_getxattr(p, name, 1)
        _expect(erange["result"] == -1 and erange["errno"] == errno.ERANGE, "libc getxattr small buffer returns ERANGE", erange)
        exact = _raw_getxattr(p, name, len(binary))
        _expect(exact["result"] == len(binary) and exact["value_b64"] == _b64(binary), "libc getxattr exact buffer returns full binary value", exact)
        create_errno = None
        try:
            os.setxattr(p, name, b"again", os.XATTR_CREATE)
            raise AssertionError("XATTR_CREATE unexpectedly replaced existing value")
        except OSError as exc:
            create_errno = exc.errno
            _expect(exc.errno == errno.EEXIST, "XATTR_CREATE existing returns EEXIST", {"errno": exc.errno})
        os.setxattr(p, name, b"", os.XATTR_REPLACE)
        empty = os.getxattr(p, name)
        empty_query = _raw_getxattr(p, name)
        _expect(empty == b"", "empty xattr value roundtrips")
        _expect(empty_query["result"] == 0, "libc getxattr empty value length query returns zero", empty_query)
        os.removexattr(p, name)
        replace_errno = None
        try:
            os.setxattr(p, name, b"missing", os.XATTR_REPLACE)
            raise AssertionError("XATTR_REPLACE unexpectedly created missing value")
        except OSError as exc:
            replace_errno = exc.errno
            _expect(exc.errno in {getattr(errno, "ENODATA", 61), getattr(errno, "ENOATTR", 93)}, "XATTR_REPLACE missing returns ENODATA/ENOATTR", {"errno": exc.errno, "errno_name": _errno_name(exc.errno)})
        return {
            "path": str(p),
            "name": name,
            "missing_errno": missing_errno,
            "create_existing_errno": create_errno,
            "replace_missing_errno": replace_errno,
            "length_query": length_query,
            "erange": erange,
            "exact": exact,
            "empty_query": empty_query,
        }

    def test_sparse_high_offset(self) -> dict[str, Any]:
        p = self.root / "sparse-file"
        fd = os.open(p, os.O_CREAT | os.O_RDWR | os.O_TRUNC, 0o600)
        offset = 4 * 1024 * 1024
        payload = b"S" * 4096
        try:
            written = os.pwrite(fd, payload, offset)
            os.fsync(fd)
            st = _stat_dict(p)
            hole = os.pread(fd, 4096, 0)
            data = os.pread(fd, 4096, offset)
            allocated = None if st["blocks_512b"] is None else st["blocks_512b"] * 512
            _expect(written == len(payload), "pwrite wrote full payload", {"written": written})
            _expect(st["size"] == offset + len(payload), "sparse file size includes high offset", st)
            _expect(hole == b"\x00" * 4096, "hole reads as zero")
            _expect(data == payload, "high-offset payload reads back")
            if allocated is not None:
                _expect(allocated < st["size"], "st_blocks shows sparse allocation smaller than size", {"allocated_bytes": allocated, "size": st["size"], "stat": st})
            return {"path": str(p), "offset": offset, "stat": st, "allocated_bytes": allocated, "hole_zero_bytes": len(hole)}
        finally:
            os.close(fd)

    def test_shrink_grow(self) -> dict[str, Any]:
        p = self.root / "shrink-grow"
        fd = os.open(p, os.O_CREAT | os.O_RDWR | os.O_TRUNC, 0o600)
        try:
            os.write(fd, b"A" * 8192)
            os.ftruncate(fd, 1024)
            st_shrink = os.fstat(fd)
            _expect(st_shrink.st_size == 1024, "ftruncate shrink updates size", {"size": st_shrink.st_size})
            os.ftruncate(fd, 4096)
            st_grow = os.fstat(fd)
            os.lseek(fd, 1024, os.SEEK_SET)
            grown = os.read(fd, 3072)
            _expect(st_grow.st_size == 4096, "ftruncate grow updates size", {"size": st_grow.st_size})
            _expect(grown == b"\x00" * 3072, "grown region reads as zero")
            return {"path": str(p), "shrink_size": st_shrink.st_size, "grow_size": st_grow.st_size, "grown_zero_bytes": len(grown)}
        finally:
            os.close(fd)

    def test_fsync_dirfsync(self) -> dict[str, Any]:
        d = self.root / "sync-dir"
        d.mkdir()
        p = d / "sync-file"
        fd = os.open(p, os.O_CREAT | os.O_RDWR | os.O_TRUNC, 0o600)
        dirfd = None
        try:
            os.write(fd, b"sync-data")
            os.fsync(fd)
            dirfd = os.open(d, os.O_RDONLY | getattr(os, "O_DIRECTORY", 0))
            os.fsync(dirfd)
            _expect(_read(p) == b"sync-data", "file remains readable after fsync and dirfsync")
            return {"directory": str(d), "file": str(p), "file_fsync": 0, "dir_fsync": 0}
        finally:
            os.close(fd)
            if dirfd is not None:
                os.close(dirfd)

    def run(self) -> int:
        self.setup()
        self.step("mkdir_readdir_rmdir", self.test_mkdir_readdir_rmdir)
        self.step("regular_mknod_readwrite", self.test_regular_mknod_readwrite)
        self.step("mode_uid", self.test_mode_uid)
        self.step("hardlink_nlink", self.test_hardlink_nlink)
        self.step("symlink_readlink", self.test_symlink_readlink)
        self.step("rename_replacement", self.test_rename_replacement)
        self.step("open_unlink_fd", self.test_open_unlink_fd)
        self.step("xattr_user", self.test_xattr_user)
        self.step("sparse_4k_high_offset", self.test_sparse_high_offset)
        self.step("shrink_grow", self.test_shrink_grow)
        self.step("fsync_dirfsync", self.test_fsync_dirfsync)
        self.cleanup_or_keep()
        self.write_report()
        return 1 if self.failures else 0


def parse_args(argv: list[str]) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Run a short targeted POSIX smoke probe on a mounted filesystem.")
    parser.add_argument("--mount", required=True, type=Path, help="Mounted filesystem path to test.")
    parser.add_argument("--evidence", required=True, type=Path, help="Directory where report.json and summary.txt will be written.")
    parser.add_argument("--base-dir", type=Path, help="Optional directory under which the exclusive fixture is created. Relative paths are resolved under --mount.")
    return parser.parse_args(argv)


def main(argv: list[str]) -> int:
    args = parse_args(argv)
    probe = Probe(args.mount, args.evidence, args.base_dir)
    try:
        return probe.run()
    except Exception as exc:  # setup/write_report failure path
        args.evidence.mkdir(parents=True, exist_ok=True)
        failure = {
            "schema": "afs.posix_smoke.v1",
            "created_at": _now(),
            "mount": str(args.mount),
            "base_dir": str((args.mount / args.base_dir).resolve() if args.base_dir and not args.base_dir.is_absolute() else args.base_dir.resolve()) if args.base_dir else str(args.mount.resolve()),
            "evidence": str(args.evidence),
            "summary": {"status": "FAIL", "steps": 0, "failed": 1, "passed": 0},
            "setup_failure": {
                "exception": type(exc).__name__,
                "message": str(exc),
                "traceback": traceback.format_exc(),
            },
        }
        (args.evidence / "report.json").write_text(json.dumps(failure, indent=2, sort_keys=True) + "\n")
        (args.evidence / "summary.txt").write_text(f"status=FAIL setup_exception={type(exc).__name__} message={exc}\n")
        return 1


if __name__ == "__main__":
    raise SystemExit(main(sys.argv[1:]))
