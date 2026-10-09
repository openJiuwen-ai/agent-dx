"""Host-side OwnerFs workspace bind probe.

The CLI is intentionally small: the runner supplies an absolute host path and
the uid/gid to execute as.  The probe drops credentials, performs one concrete
filesystem operation, and prints a JSON record that can be compared with Node
and metrics evidence.
"""
from __future__ import annotations

import argparse
import errno
import hashlib
import importlib.util
import json
import os
from pathlib import Path
import platform
import sys


PAYLOAD = bytes(range(256)) * 256
PAYLOAD_SHA256 = hashlib.sha256(PAYLOAD).hexdigest()
SELECTED_CALLBACKS = {"read", "write", "create", "unlink", "mkdir", "rmdir"}


def _load_counts_module():
    path = Path(__file__).resolve().with_name("fuse_callback_counts.py")
    spec = importlib.util.spec_from_file_location("fuse_callback_counts", path)
    module = importlib.util.module_from_spec(spec)
    assert spec.loader is not None
    spec.loader.exec_module(module)
    return module


def verify_window(before: dict[str, int], after: dict[str, int], native: bool) -> dict[str, int]:
    """Verify selected callback deltas for FUSE positive or native bypass windows."""
    counts = _load_counts_module()
    observed = counts.delta(before, after)
    missing = SELECTED_CALLBACKS - observed.keys()
    if missing:
        raise ValueError(f"selected callback series not observed: {sorted(missing)}")
    selected = {name: observed[name] for name in sorted(SELECTED_CALLBACKS)}
    if native:
        leaked = {name: value for name, value in selected.items() if value != 0}
        if leaked:
            raise ValueError(f"native host path entered FUSE callbacks: {leaked}")
    else:
        incomplete = {name: value for name, value in selected.items() if value < 1}
        if incomplete:
            raise ValueError(f"FUSE positive control missing callbacks: {incomplete}")
    return observed


def _namespace_identity() -> dict[str, int]:
    stat = os.stat("/proc/self/ns/mnt")
    return {"dev": stat.st_dev, "ino": stat.st_ino}


def _identity() -> dict[str, object]:
    return {
        "pid": os.getpid(),
        "uid": os.getuid(),
        "gid": os.getgid(),
        "euid": os.geteuid(),
        "egid": os.getegid(),
        "mount_namespace": _namespace_identity(),
    }


def _drop_to(uid: int, gid: int) -> None:
    os.setgroups([])
    os.setgid(gid)
    os.setuid(uid)


def _fsync_dir(path: Path) -> None:
    fd = os.open(path, os.O_RDONLY | os.O_DIRECTORY)
    try:
        os.fsync(fd)
    finally:
        os.close(fd)


def _fresh_read(path: Path) -> bytes:
    with path.open("rb", buffering=0) as stream:
        return stream.read()


def do_write(path: Path) -> dict[str, object]:
    path.mkdir()
    proof = path / "proof"
    fd = os.open(proof, os.O_WRONLY | os.O_CREAT | os.O_EXCL, 0o600)
    try:
        os.write(fd, PAYLOAD)
        os.fsync(fd)
    finally:
        os.close(fd)
    actual = _fresh_read(proof)
    if actual != PAYLOAD:
        raise RuntimeError("fresh read after write did not match payload")
    _fsync_dir(path)
    return {
        "action": "write",
        "path": str(path),
        "proof": str(proof),
        "size": len(actual),
        "sha256": hashlib.sha256(actual).hexdigest(),
    }


def do_read(path: Path, expected_sha256: str = PAYLOAD_SHA256) -> dict[str, object]:
    proof = path / "proof"
    actual = _fresh_read(proof)
    actual_sha256 = hashlib.sha256(actual).hexdigest()
    if len(actual) != len(PAYLOAD) or actual_sha256 != expected_sha256:
        raise RuntimeError("fresh read did not match expected payload")
    return {
        "action": "read",
        "path": str(path),
        "proof": str(proof),
        "size": len(actual),
        "sha256": actual_sha256,
    }


def do_delete(path: Path) -> dict[str, object]:
    proof = path / "proof"
    proof.unlink()
    path.rmdir()
    return {
        "action": "delete",
        "path": str(path),
        "proof": str(proof),
        "exists": path.exists() or proof.exists(),
    }


def do_deny(path: Path) -> dict[str, object]:
    try:
        fd = os.open(path, os.O_WRONLY | os.O_CREAT | os.O_EXCL, 0o600)
    except OSError as error:
        if error.errno != errno.EACCES:
            raise
        return {
            "action": "deny",
            "path": str(path),
            "errno": error.errno,
            "created": False,  # The denied exclusive-open created nothing; root runner checks absence.
        }
    else:
        os.close(fd)
        raise RuntimeError("deny probe unexpectedly created target")


def run(action: str, path: Path, uid: int, gid: int, expected_sha256: str = PAYLOAD_SHA256) -> dict[str, object]:
    if not path.is_absolute():
        raise ValueError("PATH must be absolute")
    if platform.system() != "Linux":
        raise RuntimeError("workspace_host probe requires Linux")
    _drop_to(uid, gid)
    if action == "write":
        result = do_write(path)
    elif action == "read":
        result = do_read(path, expected_sha256)
    elif action == "delete":
        result = do_delete(path)
    elif action == "deny":
        result = do_deny(path)
    else:
        raise ValueError(f"unknown action: {action}")
    result.update(_identity())
    return result


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("action", choices=["write", "read", "delete", "deny"])
    parser.add_argument("path", type=Path)
    parser.add_argument("uid", type=int)
    parser.add_argument("gid", type=int)
    parser.add_argument("--expected-sha256", default=PAYLOAD_SHA256)
    args = parser.parse_args(argv)
    try:
        result = run(args.action, args.path, args.uid, args.gid, args.expected_sha256)
    except Exception as error:
        print(json.dumps({"status": "FAIL", "error": str(error)}, sort_keys=True), file=sys.stderr)
        return 1
    print(json.dumps({"status": "PASS", **result}, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
