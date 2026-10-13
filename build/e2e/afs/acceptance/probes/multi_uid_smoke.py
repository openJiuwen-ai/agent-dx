#!/usr/bin/env python3
"""Multi-UID POSIX permission smoke probe.

Runs only as Linux root. It creates one mode-0777 fixture directory under the
provided mount and executes child processes with numeric uid/gid through
setpriv. This exercises kernel default_permissions and backend caller-context
handling; root-only checks are not a substitute for these non-root subcases.
"""

from __future__ import annotations

import argparse
import base64
import errno
import json
import os
import platform
import random
import shutil
import stat
import subprocess
import sys
import time
import traceback
from pathlib import Path
from typing import Any, Callable


def _now() -> str:
    return time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime())


def _errno_name(value: int | None) -> str | None:
    if value is None:
        return None
    return errno.errorcode.get(value, f"ERRNO_{value}")


def _b64(value: bytes) -> str:
    return base64.b64encode(value).decode("ascii")


def _stat_dict(path: Path) -> dict[str, Any]:
    st = path.lstat()
    return {
        "mode_octal": oct(st.st_mode & 0o7777),
        "uid": st.st_uid,
        "gid": st.st_gid,
        "size": st.st_size,
        "nlink": st.st_nlink,
        "is_regular": stat.S_ISREG(st.st_mode),
        "ino": st.st_ino,
    }


def _expect(condition: bool, message: str, details: dict[str, Any] | None = None) -> dict[str, Any]:
    if not condition:
        raise AssertionError(json.dumps({"message": message, "details": details or {}}, sort_keys=True))
    return {"ok": True, "message": message, "details": details or {}}


_CHILD = (
    "import base64, errno, json, os, pathlib, sys, traceback\n"
    "\n"
    "\n"
    "def b64(v):\n"
    '    return base64.b64encode(v).decode("ascii")\n'
    "\n"
    "\n"
    "def emit(obj):\n"
    "    print(json.dumps(obj, sort_keys=True))\n"
    "\n"
    "\n"
    "def main():\n"
    "    op = sys.argv[1]\n"
    "    p = pathlib.Path(sys.argv[2])\n"
    '    out = {"op": op, "uid": os.geteuid(), "gid": os.getegid(), "g'
    'roups": os.getgroups()}\n'
    "    try:\n"
    '        if op == "create_rw":\n'
    "            fd = os.open(p, os.O_CREAT | os.O_EXCL | os.O_RDWR, 0"
    "o664)\n"
    "            try:\n"
    '                os.write(fd, b"uid60001-data")\n'
    "                os.fsync(fd)\n"
    "                os.lseek(fd, 0, os.SEEK_SET)\n"
    "                data = os.read(fd, 64)\n"
    "            finally:\n"
    "                os.close(fd)\n"
    "            st = p.stat()\n"
    "            out.update(\n"
    "                {\n"
    '                    "ok": True,\n'
    '                    "data_b64": b64(data),\n'
    '                    "stat": {\n'
    '                        "uid": st.st_uid,\n'
    '                        "gid": st.st_gid,\n'
    '                        "mode": oct(st.st_mode & 0o7777),\n'
    '                        "size": st.st_size,\n'
    "                    },\n"
    "                }\n"
    "            )\n"
    '        elif op == "rw_existing":\n'
    "            fd = os.open(p, os.O_RDWR)\n"
    "            try:\n"
    "                data = os.read(fd, 64)\n"
    "                os.lseek(fd, 0, os.SEEK_END)\n"
    '                os.write(fd, b"+group")\n'
    "                os.fsync(fd)\n"
    "            finally:\n"
    "                os.close(fd)\n"
    '            out.update({"ok": True, "read_b64": b64(data)})\n'
    '        elif op == "xattr_cycle":\n'
    '            name = "user.afs_multi_uid"\n'
    '            value = b"group-xattr\\x00value"\n'
    "            os.setxattr(p, name, value)\n"
    "            got = os.getxattr(p, name)\n"
    "            os.removexattr(p, name)\n"
    "            try:\n"
    "                os.getxattr(p, name)\n"
    "                missing_errno = None\n"
    "            except OSError as exc:\n"
    "                missing_errno = exc.errno\n"
    '            out.update({"ok": True, "value_b64": b64(got), "missi'
    'ng_errno": missing_errno})\n'
    '        elif op == "expect_denied_rw":\n'
    "            try:\n"
    "                os.open(p, os.O_RDWR)\n"
    '                out.update({"ok": False, "error": "open unexpecte'
    'dly succeeded"})\n'
    "                emit(out)\n"
    "                return 1\n"
    "            except OSError as exc:\n"
    "                out.update(\n"
    "                    {\n"
    '                        "ok": exc.errno in (errno.EACCES, errno.E'
    "PERM),\n"
    '                        "errno": exc.errno,\n'
    '                        "errno_name": errno.errorcode.get(exc.err'
    "no),\n"
    "                    }\n"
    "                )\n"
    "                emit(out)\n"
    '                return 0 if out["ok"] else 1\n'
    '        elif op == "expect_denied_xattr":\n'
    "            try:\n"
    '                os.setxattr(p, "user.afs_multi_uid_denied", b"den'
    'ied")\n'
    '                out.update({"ok": False, "error": "setxattr unexp'
    'ectedly succeeded"})\n'
    "                emit(out)\n"
    "                return 1\n"
    "            except OSError as exc:\n"
    "                out.update(\n"
    "                    {\n"
    '                        "ok": exc.errno in (errno.EACCES, errno.E'
    "PERM),\n"
    '                        "errno": exc.errno,\n'
    '                        "errno_name": errno.errorcode.get(exc.err'
    "no),\n"
    "                    }\n"
    "                )\n"
    "                emit(out)\n"
    '                return 0 if out["ok"] else 1\n'
    '        elif op == "expect_nonowner_chmod_chown_denied":\n'
    "            results = {}\n"
    "            try:\n"
    "                os.chmod(p, 0o777)\n"
    '                results["chmod"] = {"ok": False, "error": "chmod '
    'unexpectedly succeeded"}\n'
    "            except OSError as exc:\n"
    '                results["chmod"] = {\n'
    '                    "ok": exc.errno in (errno.EPERM, errno.EACCES'
    "),\n"
    '                    "errno": exc.errno,\n'
    '                    "errno_name": errno.errorcode.get(exc.errno),'
    "\n"
    "                }\n"
    "            try:\n"
    "                os.chown(p, os.geteuid(), os.getegid())\n"
    '                results["chown"] = {"ok": False, "error": "chown '
    'unexpectedly succeeded"}\n'
    "            except OSError as exc:\n"
    '                results["chown"] = {\n'
    '                    "ok": exc.errno in (errno.EPERM, errno.EACCES'
    "),\n"
    '                    "errno": exc.errno,\n'
    '                    "errno_name": errno.errorcode.get(exc.errno),'
    "\n"
    "                }\n"
    '            out.update({"ok": all(v.get("ok") for v in results.va'
    'lues()), "results": results})\n'
    "            emit(out)\n"
    '            return 0 if out["ok"] else 1\n'
    "        else:\n"
    '            out.update({"ok": False, "error": "unknown op"})\n'
    "            emit(out)\n"
    "            return 2\n"
    "        emit(out)\n"
    '        return 0 if out.get("ok") else 1\n'
    "    except Exception as exc:\n"
    "        out.update(\n"
    "            {\n"
    '                "ok": False,\n'
    '                "exception": type(exc).__name__,\n'
    '                "message": str(exc),\n'
    '                "traceback": traceback.format_exc(),\n'
    "            }\n"
    "        )\n"
    "        emit(out)\n"
    "        return 1\n"
    "\n"
    "\n"
    "raise SystemExit(main())\n"
)


class Probe:
    def __init__(self, mount: Path, evidence: Path, setpriv: str, base_dir: Path | None = None) -> None:
        self.mount = mount.resolve()
        self.evidence = evidence.resolve()
        self.setpriv = setpriv
        self.base_dir = self._resolve_base_dir(base_dir)
        self.run_id = (
            f"multi-uid-smoke-"
            f"{time.strftime('%Y%m%dT%H%M%SZ', time.gmtime())}"
            f"-"
            f"{os.getpid()}"
            f"-"
            f"{random.randrange(1000000):06d}"
        )
        self.root = self.base_dir / f".afs-multi-uid-smoke-{self.run_id}"
        self.child_script = self.root / "child.py"
        self.records: list[dict[str, Any]] = []
        self.failures: list[dict[str, Any]] = []
        self.fixture_kept = True

    def _resolve_base_dir(self, base_dir: Path | None) -> Path:
        if base_dir is None:
            return self.mount
        if base_dir.is_absolute():
            return base_dir.resolve()
        return (self.mount / base_dir).resolve()

    def setup(self) -> None:
        self.evidence.mkdir(parents=True, exist_ok=True)
        _expect(os.geteuid() == 0, "multi_uid_smoke must run as root", {"uid": os.geteuid()})
        _expect(self.mount.is_dir(), "mount path is a directory", {"mount": str(self.mount)})
        self.base_dir.mkdir(mode=0o755, parents=True, exist_ok=True)
        _expect(self.base_dir.is_dir(), "base directory is a directory", {"base_dir": str(self.base_dir)})
        self.root.mkdir(mode=0o777)
        os.chmod(self.root, 0o777)
        self.child_script.write_text(_CHILD)
        os.chmod(self.child_script, 0o755)

    def step(self, name: str, func: Callable[[], dict[str, Any]]) -> None:
        started = time.time()
        record: dict[str, Any] = {"name": name, "started_at": _now(), "fixture_root": str(self.root)}
        try:
            details = func()
            record.update({"ok": True, "exit_status": 0, "errno": None, "errno_name": None, "details": details})
        except OSError as exc:
            record.update(
                {
                    "ok": False,
                    "exit_status": 1,
                    "errno": exc.errno,
                    "errno_name": _errno_name(exc.errno),
                    "exception": type(exc).__name__,
                    "message": str(exc),
                    "traceback": traceback.format_exc(),
                }
            )
            self.failures.append(record)
        except Exception as exc:  # noqa: BLE001
            record.update(
                {
                    "ok": False,
                    "exit_status": 1,
                    "errno": None,
                    "errno_name": None,
                    "exception": type(exc).__name__,
                    "message": str(exc),
                    "traceback": traceback.format_exc(),
                }
            )
            self.failures.append(record)
        finally:
            record["duration_ms"] = round((time.time() - started) * 1000, 3)
            self.records.append(record)

    def child(self, uid: int, gid: int, groups: list[int], op: str, path: Path) -> dict[str, Any]:
        group_arg = ",".join(str(g) for g in groups) if groups else ""
        cmd = [
            self.setpriv,
            f"--reuid={uid}",
            f"--regid={gid}",
            "--clear-groups" if not groups else f"--groups={group_arg}",
            sys.executable,
            str(self.child_script),
            op,
            str(path),
        ]
        proc = subprocess.run(cmd, text=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE, check=False)
        parsed = None
        if proc.stdout.strip():
            parsed = json.loads(proc.stdout.strip().splitlines()[-1])
        result = {
            "cmd": cmd,
            "returncode": proc.returncode,
            "stdout": proc.stdout,
            "stderr": proc.stderr,
            "parsed": parsed,
        }
        _expect(proc.returncode == 0 and parsed and parsed.get("ok"), f"child op {op} as uid {uid} succeeded", result)
        return result

    def test_create_as_uid(self) -> dict[str, Any]:
        p = self.root / "uid60001-created"
        child = self.child(60001, 60001, [60002], "create_rw", p)
        st = _stat_dict(p)
        _expect(st["uid"] == 60001 and st["gid"] == 60001, "created file has child uid/gid", st)
        _expect(p.read_bytes() == b"uid60001-data", "created file data persisted")
        return {"path": str(p), "child": child, "stat": st, "data_b64": _b64(p.read_bytes())}

    def test_owner_chgrp_supplementary(self) -> dict[str, Any]:
        q = self.root / "uid60001-chgrp-source"
        child = self.child(60001, 60001, [60002], "create_rw", q)
        self.child(60001, 60001, [60002], "rw_existing", q)
        subprocess.run(
            [self.setpriv, "--reuid=60001", "--regid=60001", "--groups=60002", "chgrp", "60002", str(q)],
            check=True,
            text=True,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
        )
        st = _stat_dict(q)
        _expect(st["gid"] == 60002, "owner can chgrp to supplementary group", st)
        return {"path": str(q), "stat": st, "create_child": child}

    def test_group_xattr_allowed_and_other_denied(self) -> dict[str, Any]:
        p = self.root / "root-owned-group660"
        p.write_bytes(b"root-group")
        os.chown(p, 0, 60002)
        os.chmod(p, 0o660)
        group_rw = self.child(60003, 60003, [60002], "rw_existing", p)
        xattr = self.child(60003, 60003, [60002], "xattr_cycle", p)
        denied_rw = self.child(60004, 60004, [], "expect_denied_rw", p)
        denied_xattr = self.child(60004, 60004, [], "expect_denied_xattr", p)
        st = _stat_dict(p)
        _expect(
            st["uid"] == 0 and st["gid"] == 60002 and st["mode_octal"] == "0o660",
            "root-owned group file retains mode/owner",
            st,
        )
        return {
            "path": str(p),
            "stat": st,
            "group_rw": group_rw,
            "xattr": xattr,
            "denied_rw": denied_rw,
            "denied_xattr": denied_xattr,
        }

    def test_nonowner_chmod_chown_denied(self) -> dict[str, Any]:
        p = self.root / "root-owned-deny-metadata"
        p.write_bytes(b"metadata")
        os.chown(p, 0, 60002)
        os.chmod(p, 0o660)
        denied = self.child(60003, 60003, [60002], "expect_nonowner_chmod_chown_denied", p)
        return {"path": str(p), "stat": _stat_dict(p), "denied": denied}

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
        mount_info = subprocess.run(
            ["findmnt", "-T", str(self.mount), "-o", "TARGET,SOURCE,FSTYPE,OPTIONS", "--json"],
            text=True,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            check=False,
        )
        report = {
            "schema": "afs.multi_uid_smoke.v1",
            "run_id": self.run_id,
            "created_at": _now(),
            "mount": str(self.mount),
            "evidence": str(self.evidence),
            "base_dir": str(self.base_dir),
            "fixture_root": str(self.root),
            "fixture_kept": self.fixture_kept,
            "ok": not self.failures,
            "step_count": len(self.records),
            "failure_count": len(self.failures),
            "platform": {
                "system": platform.system(),
                "release": platform.release(),
                "machine": platform.machine(),
                "python": platform.python_version(),
                "uid": os.geteuid(),
                "gid": os.getegid(),
            },
            "findmnt": {"exit_status": mount_info.returncode, "stdout": mount_info.stdout, "stderr": mount_info.stderr},
            "summary": {
                "status": "PASS" if not self.failures else "FAIL",
                "steps": len(self.records),
                "failed": len(self.failures),
                "passed": len(self.records) - len(self.failures),
            },
            "steps": self.records,
            "notes": [
                "Short targeted multi-UID probe only; not a complete POSIX suite.",
                "Requires Linux root and util-linux setpriv.",
                (
                    "Exercises both kernel default_permissions style checks and "
                    "backend CallerContext propagation; root-only execution is not a "
                    "substitute for these child uid/gid cases."
                ),
            ],
        }
        self.evidence.mkdir(parents=True, exist_ok=True)
        summary_line = (
            f"status="
            f"{report['summary']['status']}"
            f" steps="
            f"{report['summary']['steps']}"
            f" failed="
            f"{report['summary']['failed']}"
            f" fixture="
            f"{self.root}"
            f" kept="
            f"{self.fixture_kept}"
        )
        (self.evidence / "report.json").write_text(json.dumps(report, indent=2, sort_keys=True) + "\n")
        (self.evidence / "summary.txt").write_text(summary_line + "\n")
        print(summary_line)

    def run(self) -> int:
        self.setup()
        self.step("create_as_uid60001", self.test_create_as_uid)
        self.step("owner_chgrp_to_supplementary_group", self.test_owner_chgrp_supplementary)
        self.step("group_xattr_allowed_and_other_denied", self.test_group_xattr_allowed_and_other_denied)
        self.step("nonowner_chmod_chown_denied", self.test_nonowner_chmod_chown_denied)
        self.cleanup_or_keep()
        self.write_report()
        return 1 if self.failures else 0


def parse_args(argv: list[str]) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Run root-only multi-UID POSIX smoke probe on a mounted filesystem.")
    parser.add_argument("--mount", required=True, type=Path)
    parser.add_argument("--evidence", required=True, type=Path)
    parser.add_argument("--setpriv", default="setpriv", help="Path to util-linux setpriv")
    parser.add_argument(
        "--base-dir",
        type=Path,
        help=(
            "Optional directory under which the exclusive fixture is "
            "created. Relative paths are resolved under --mount."
        ),
    )
    return parser.parse_args(argv)


def main(argv: list[str]) -> int:
    args = parse_args(argv)
    probe = Probe(args.mount, args.evidence, args.setpriv, args.base_dir)
    try:
        return probe.run()
    except Exception as exc:
        args.evidence.mkdir(parents=True, exist_ok=True)
        base_dir = (
            (args.mount / args.base_dir).resolve()
            if args.base_dir and not args.base_dir.is_absolute()
            else (args.base_dir.resolve() if args.base_dir else args.mount.resolve())
        )
        failure = {
            "schema": "afs.multi_uid_smoke.v1",
            "created_at": _now(),
            "mount": str(args.mount),
            "base_dir": str(base_dir),
            "evidence": str(args.evidence),
            "ok": False,
            "step_count": 0,
            "failure_count": 1,
            "summary": {"status": "FAIL", "steps": 0, "failed": 1, "passed": 0},
            "setup_failure": {
                "exception": type(exc).__name__,
                "message": str(exc),
                "traceback": traceback.format_exc(),
            },
        }
        summary_line = f"status=FAIL setup_exception={type(exc).__name__} message={exc}"
        (args.evidence / "report.json").write_text(json.dumps(failure, indent=2, sort_keys=True) + "\n")
        (args.evidence / "summary.txt").write_text(summary_line + "\n")
        print(summary_line)
        return 1


if __name__ == "__main__":
    raise SystemExit(main(sys.argv[1:]))
