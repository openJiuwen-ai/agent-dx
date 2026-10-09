#!/usr/bin/env python3
"""STD-04 deterministic differential random filesystem driver.

This is an AFS-owned differential driver inspired by the JuiceFS fsrand /
Hypothesis acceptance shape. It does not claim to be an upstream JuiceFS suite.
It generates deterministic operation streams, applies each operation to an ext4
reference fixture and a target fixture, and records the first semantic mismatch
with a raw failing prefix trace.
"""
from __future__ import annotations

import argparse
import datetime as dt
import errno
import hashlib
import importlib.util
import json
import os
import platform
import random
import shutil
import stat
import sys
import time
import traceback
import uuid
from pathlib import Path
from typing import Any

from target_identity import target_checks

FULL_SEEDS = list(range(1, 11))
FULL_OPERATIONS = 10_000
SMOKE_SEEDS = [1]
SMOKE_OPERATIONS = 100
RESULT_VALUES = {"PASS", "FAIL", "BLOCKED", "INCONCLUSIVE", "NOT_RUN"}
HYPOTHESIS_MIN_VERSION = "6"
OP_NAMES = [
    "create",
    "open_read",
    "write",
    "pwrite",
    "append",
    "read",
    "truncate",
    "sparse",
    "rename",
    "hardlink",
    "symlink",
    "unlink",
    "stat",
]


def utc() -> str:
    return dt.datetime.now(dt.timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")


def write_json(path: Path, value: Any) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(value, indent=2, sort_keys=True) + "\n", encoding="utf-8")


def run_text(argv: list[str]) -> dict[str, Any]:
    import subprocess

    try:
        proc = subprocess.run(argv, shell=False, text=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE, timeout=10, check=False)
        return {"argv": argv, "returncode": proc.returncode, "stdout": proc.stdout, "stderr": proc.stderr}
    except Exception as exc:  # noqa: BLE001
        return {"argv": argv, "returncode": None, "exception": type(exc).__name__, "message": str(exc)}


def rel(path: Path, base: Path) -> str:
    return str(path.resolve().relative_to(base.resolve()))


def build_check(name: str, status: str, evidence: Any, artifact: str | None = None) -> dict[str, Any]:
    result = {"name": name, "status": status, "evidence": evidence}
    if artifact:
        result["artifact"] = artifact
    return result


def parse_seed_list(value: str | None, default: list[int]) -> list[int]:
    if not value:
        return list(default)
    seeds = [int(part.strip(), 10) for part in value.split(",") if part.strip()]
    if not seeds:
        raise ValueError("at least one seed is required")
    if any(seed < 0 for seed in seeds):
        raise ValueError("seeds must be non-negative")
    return seeds


def is_under(child: Path, parent: Path) -> bool:
    try:
        child.resolve().relative_to(parent.resolve())
        return True
    except ValueError:
        return False


def resolve_base_dir(mount: Path, base_dir: Path | None) -> Path:
    if base_dir is None:
        return mount
    return base_dir if base_dir.is_absolute() else mount / base_dir


def mount_identity(path: Path) -> dict[str, Any]:
    return run_text(["findmnt", "-T", str(path), "-o", "TARGET,SOURCE,FSTYPE,OPTIONS", "--json"])


def sha256_bytes(data: bytes) -> str:
    return hashlib.sha256(data).hexdigest()


def sha256_file(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for chunk in iter(lambda: handle.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def findmnt_first(identity: dict[str, Any]) -> dict[str, Any]:
    if identity.get("returncode") != 0:
        return {}
    try:
        filesystems = json.loads(identity.get("stdout") or "{}").get("filesystems") or []
    except json.JSONDecodeError:
        return {}
    return filesystems[0] if filesystems else {}


def process_identity(pid: str | None) -> dict[str, Any] | None:
    if not pid:
        return None
    proc = Path("/proc") / str(pid)
    result: dict[str, Any] = {"pid": str(pid), "exists": proc.exists()}
    try:
        exe = proc.joinpath("exe").resolve()
        result["exe"] = str(exe)
        if exe.is_file():
            result["exe_sha256"] = sha256_file(exe)
    except Exception as exc:  # noqa: BLE001
        result["exe_error"] = f"{type(exc).__name__}: {exc}"
    try:
        result["cmdline"] = proc.joinpath("cmdline").read_bytes().replace(b"\0", b" ").decode(errors="replace")
    except Exception as exc:  # noqa: BLE001
        result["cmdline_error"] = f"{type(exc).__name__}: {exc}"
    return result


def is_product_backend(name: str) -> bool:
    normalized = name.lower()
    return normalized in {"dfs", "ownerfs"} or "afs" in normalized


def data_for(seed: int, index: int, label: str, max_len: int = 257) -> bytes:
    size_seed = hashlib.sha256(f"{seed}:{index}:{label}:size".encode()).digest()
    size = 1 + (int.from_bytes(size_seed[:8], "big") % max_len)
    out = bytearray()
    counter = 0
    while len(out) < size:
        out.extend(hashlib.sha256(f"{seed}:{index}:{label}:{counter}".encode()).digest())
        counter += 1
    return bytes(out[:size])


def errno_result(exc: BaseException) -> dict[str, Any]:
    err = getattr(exc, "errno", None)
    return {"ok": False, "errno": err, "errno_name": errno.errorcode.get(err, str(err)) if err is not None else type(exc).__name__}


def ok_result(extra: dict[str, Any] | None = None) -> dict[str, Any]:
    result = {"ok": True, "errno": None}
    if extra:
        result.update(extra)
    return result


def safe_join(root: Path, rel_path: str) -> Path:
    rel = Path(rel_path)
    if rel.is_absolute() or ".." in rel.parts:
        raise ValueError(f"unsafe relative path: {rel_path}")
    return root / rel


def path_pool() -> list[str]:
    files = [f"f{i}" for i in range(12)] + [f"d{i}/f{j}" for i in range(4) for j in range(3)]
    links = [f"l{i}" for i in range(6)]
    return files + links


def dir_pool() -> list[str]:
    return [f"d{i}" for i in range(4)]


def generate_operation(seed: int, index: int, rng: random.Random) -> dict[str, Any]:
    op = rng.choice(OP_NAMES)
    paths = path_pool()
    dirs = dir_pool()
    event: dict[str, Any] = {"index": index, "op": op}
    if op == "create":
        event.update({"path": rng.choice(paths), "mode": 0o644})
    elif op == "open_read":
        event.update({"path": rng.choice(paths)})
    elif op == "write":
        event.update({"path": rng.choice(paths), "data_hex": data_for(seed, index, "write", 128).hex()})
    elif op == "pwrite":
        event.update({"path": rng.choice(paths), "offset": rng.randrange(0, 2048), "data_hex": data_for(seed, index, "pwrite", 192).hex()})
    elif op == "append":
        event.update({"path": rng.choice(paths), "data_hex": data_for(seed, index, "append", 96).hex()})
    elif op == "read":
        event.update({"path": rng.choice(paths), "offset": rng.randrange(0, 2048), "length": rng.randrange(0, 256)})
    elif op == "truncate":
        event.update({"path": rng.choice(paths), "size": rng.randrange(0, 4096)})
    elif op == "sparse":
        event.update({"path": rng.choice(paths), "offset": rng.randrange(4096, 16384), "data_hex": data_for(seed, index, "sparse", 32).hex()})
    elif op == "rename":
        event.update({"src": rng.choice(paths), "dst": rng.choice(paths)})
    elif op == "hardlink":
        event.update({"src": rng.choice(paths), "dst": rng.choice(paths)})
    elif op == "symlink":
        event.update({"target": rng.choice(paths), "path": rng.choice(paths)})
    elif op == "unlink":
        event.update({"path": rng.choice(paths)})
    elif op == "stat":
        event.update({"path": rng.choice(paths + dirs)})
    return event


def prepare_root(root: Path) -> None:
    root.parent.mkdir(parents=True, exist_ok=True)
    root.mkdir(mode=0o755, exist_ok=False)
    for directory in dir_pool():
        (root / directory).mkdir(exist_ok=False)


def normalize_result(result: dict[str, Any]) -> dict[str, Any]:
    normalized = dict(result)
    # These fields are stable semantic comparison keys. Raw messages and paths
    # are intentionally omitted because reference/target roots differ.
    return normalized


def path_snapshot(root: Path, rel_path: str) -> dict[str, Any]:
    path = safe_join(root, rel_path)
    try:
        st = os.lstat(path)
    except OSError as exc:
        return {"exists": False, "errno": exc.errno, "errno_name": errno.errorcode.get(exc.errno, str(exc.errno))}
    mode_type = stat.S_IFMT(st.st_mode)
    result: dict[str, Any] = {
        "exists": True,
        "mode": stat.S_IMODE(st.st_mode),
        "type": "dir" if stat.S_ISDIR(st.st_mode) else "symlink" if stat.S_ISLNK(st.st_mode) else "file" if stat.S_ISREG(st.st_mode) else str(mode_type),
        "nlink": st.st_nlink,
    }
    if stat.S_ISLNK(st.st_mode):
        result["size"] = st.st_size
        result["target"] = os.readlink(path)
    elif stat.S_ISREG(st.st_mode):
        result["size"] = st.st_size
        with path.open("rb") as handle:
            data = handle.read()
        result["sha256"] = sha256_bytes(data)
    elif stat.S_ISDIR(st.st_mode):
        # Directory st_size is filesystem-specific (for example ext4 commonly
        # reports block-sized directories while FUSE filesystems may report 0).
        # Compare observable directory semantics instead.
        result["entries"] = sorted(p.name for p in path.iterdir())
    return result


def tree_digest(root: Path) -> dict[str, Any]:
    entries: dict[str, Any] = {}
    for path in sorted(root.rglob("*"), key=lambda item: str(item.relative_to(root))):
        rel_path = str(path.relative_to(root))
        entries[rel_path] = path_snapshot(root, rel_path)
    encoded = json.dumps(entries, sort_keys=True, separators=(",", ":")).encode()
    return {"sha256": sha256_bytes(encoded), "entry_count": len(entries), "entries": entries}


def compare_payload(a: dict[str, Any], b: dict[str, Any]) -> bool:
    return normalize_result(a) == normalize_result(b)


def close_fd(fd: int | None) -> None:
    if fd is not None:
        try:
            os.close(fd)
        except OSError:
            pass


def apply_operation(root: Path, op: dict[str, Any]) -> dict[str, Any]:
    name = op["op"]
    fd: int | None = None
    try:
        if name == "create":
            fd = os.open(safe_join(root, op["path"]), os.O_CREAT | os.O_EXCL | os.O_WRONLY, int(op["mode"]))
            return ok_result()
        if name == "open_read":
            fd = os.open(safe_join(root, op["path"]), os.O_RDONLY)
            return ok_result()
        if name == "write":
            data = bytes.fromhex(op["data_hex"])
            fd = os.open(safe_join(root, op["path"]), os.O_CREAT | os.O_WRONLY, 0o644)
            written = os.write(fd, data)
            return ok_result({"written": written})
        if name == "pwrite":
            data = bytes.fromhex(op["data_hex"])
            fd = os.open(safe_join(root, op["path"]), os.O_CREAT | os.O_RDWR, 0o644)
            written = os.pwrite(fd, data, int(op["offset"]))
            return ok_result({"written": written})
        if name == "append":
            data = bytes.fromhex(op["data_hex"])
            fd = os.open(safe_join(root, op["path"]), os.O_CREAT | os.O_WRONLY | os.O_APPEND, 0o644)
            written = os.write(fd, data)
            return ok_result({"written": written})
        if name == "read":
            fd = os.open(safe_join(root, op["path"]), os.O_RDONLY)
            data = os.pread(fd, int(op["length"]), int(op["offset"]))
            return ok_result({"read_len": len(data), "sha256": sha256_bytes(data), "eof": len(data) < int(op["length"])})
        if name == "truncate":
            os.truncate(safe_join(root, op["path"]), int(op["size"]))
            return ok_result()
        if name == "sparse":
            data = bytes.fromhex(op["data_hex"])
            fd = os.open(safe_join(root, op["path"]), os.O_CREAT | os.O_RDWR, 0o644)
            written = os.pwrite(fd, data, int(op["offset"]))
            return ok_result({"written": written})
        if name == "rename":
            os.replace(safe_join(root, op["src"]), safe_join(root, op["dst"]))
            return ok_result()
        if name == "hardlink":
            os.link(safe_join(root, op["src"]), safe_join(root, op["dst"]))
            return ok_result()
        if name == "symlink":
            os.symlink(op["target"], safe_join(root, op["path"]))
            return ok_result()
        if name == "unlink":
            os.unlink(safe_join(root, op["path"]))
            return ok_result()
        if name == "stat":
            return ok_result({"snapshot": path_snapshot(root, op["path"])})
        raise ValueError(f"unknown op {name}")
    except OSError as exc:
        return errno_result(exc)
    finally:
        close_fd(fd)


def affected_paths(op: dict[str, Any]) -> list[str]:
    paths: list[str] = []
    for key in ("path", "src", "dst"):
        value = op.get(key)
        if isinstance(value, str):
            paths.append(value)
    return sorted(set(paths))


def hypothesis_identity() -> dict[str, Any]:
    spec = importlib.util.find_spec("hypothesis")
    if spec is None:
        return {"available": False, "module": None}
    try:
        import hypothesis  # type: ignore

        return {"available": True, "version": getattr(hypothesis, "__version__", "unknown"), "module": spec.origin}
    except Exception as exc:  # noqa: BLE001
        return {"available": False, "module": spec.origin, "exception": type(exc).__name__, "message": str(exc)}


def mismatch_fingerprint(mismatch: dict[str, Any] | None) -> dict[str, Any] | None:
    if mismatch is None:
        return None
    return {
        "reason": mismatch.get("reason"),
        "operation": mismatch.get("operation"),
        "reference_result": mismatch.get("reference_result"),
        "target_result": mismatch.get("target_result"),
        "reference_tree_sha256": (mismatch.get("reference_tree") or {}).get("sha256"),
        "target_tree_sha256": (mismatch.get("target_tree") or {}).get("sha256"),
    }


def replay_operations(
    reference_root: Path,
    target_root: Path,
    operations: list[dict[str, Any]],
    artifacts: Path,
    label: str,
    inject_target_fault_at: int | None = None,
    keep_trace: bool = False,
) -> dict[str, Any]:
    replay_dir = artifacts / "replay" / label
    replay_dir.mkdir(parents=True, exist_ok=True)
    ref_fixture = reference_root / f"replay-{label}-ref-{uuid.uuid4().hex[:8]}"
    target_fixture = target_root / f"replay-{label}-target-{uuid.uuid4().hex[:8]}"
    prepare_root(ref_fixture)
    prepare_root(target_fixture)
    trace: list[dict[str, Any]] = []
    mismatch: dict[str, Any] | None = None
    try:
        for index, op in enumerate(operations):
            ref_result = apply_operation(ref_fixture, op)
            target_result = apply_operation(target_fixture, op)
            if inject_target_fault_at is not None and op.get("index") == inject_target_fault_at:
                target_result = dict(target_result)
                target_result["selftest_injected_fault"] = True
            ref_tree = tree_digest(ref_fixture)
            target_tree = tree_digest(target_fixture)
            event = {
                "index": index,
                "operation": op,
                "reference_result": ref_result,
                "target_result": target_result,
                "reference_tree_sha256": ref_tree["sha256"],
                "target_tree_sha256": target_tree["sha256"],
            }
            if keep_trace:
                trace.append(event)
            if not compare_payload(ref_result, target_result) or ref_tree["sha256"] != target_tree["sha256"]:
                mismatch = {
                    "index": index,
                    "failing_prefix_operations": index + 1,
                    "minimized": False,
                    "reason": "result mismatch" if not compare_payload(ref_result, target_result) else "tree digest mismatch",
                    "operation": op,
                    "reference_result": ref_result,
                    "target_result": target_result,
                    "reference_tree": ref_tree,
                    "target_tree": target_tree,
                }
                break
        if keep_trace:
            write_json(replay_dir / "trace.json", trace)
        cleanup_ok, cleanup = cleanup_fixtures(replay_dir, [("reference", ref_fixture), ("target", target_fixture)])
        return {"status": "FAIL" if mismatch else "PASS", "mismatch": mismatch, "fingerprint": mismatch_fingerprint(mismatch), "cleanup_ok": cleanup_ok, "cleanup": cleanup, "trace": rel(replay_dir / "trace.json", artifacts) if keep_trace else None}
    except Exception as exc:  # noqa: BLE001
        cleanup_ok, cleanup = cleanup_fixtures(replay_dir, [("reference", ref_fixture), ("target", target_fixture)])
        return {"status": "INCONCLUSIVE", "exception": type(exc).__name__, "message": str(exc), "cleanup_ok": cleanup_ok, "cleanup": cleanup}


def shrink_with_hypothesis(
    reference_root: Path,
    target_root: Path,
    trace: list[dict[str, Any]],
    original_fingerprint: dict[str, Any],
    artifacts: Path,
    seed_dir: Path,
    seed: int,
    inject_target_fault_at: int | None,
) -> dict[str, Any]:
    info = hypothesis_identity()
    database_dir = seed_dir / "hypothesis-db"
    database_dir.mkdir(parents=True, exist_ok=True)
    if not info.get("available"):
        result = {"status": "BLOCKED", "hypothesis": info, "database_dir": rel(database_dir, artifacts), "reason": "Hypothesis is not installed"}
        write_json(seed_dir / "hypothesis-shrink.json", result)
        return result
    try:
        from hypothesis import find, settings
        from hypothesis.database import DirectoryBasedExampleDatabase
        from hypothesis import strategies as st

        operations = [event["operation"] for event in trace]
        attempts: list[dict[str, Any]] = []

        def reproduces(prefix_len: int) -> bool:
            label = f"seed-{seed}-candidate-{len(attempts)}-{prefix_len}"
            replay = replay_operations(reference_root, target_root, operations[:prefix_len], artifacts, label, inject_target_fault_at, keep_trace=False)
            candidate = {"prefix_len": prefix_len, "status": replay.get("status"), "fingerprint": replay.get("fingerprint"), "cleanup_ok": replay.get("cleanup_ok")}
            attempts.append(candidate)
            return replay.get("status") == "FAIL" and replay.get("fingerprint") == original_fingerprint and replay.get("cleanup_ok") is True

        minimal_len = find(
            st.integers(min_value=1, max_value=len(operations)),
            reproduces,
            settings=settings(database=DirectoryBasedExampleDatabase(str(database_dir)), max_examples=max(50, min(1000, len(operations) * 4)), deadline=None),
        )
        final = replay_operations(reference_root, target_root, operations[:minimal_len], artifacts, f"seed-{seed}-minimal-{minimal_len}", inject_target_fault_at, keep_trace=True)
        reproduced = final.get("status") == "FAIL" and final.get("fingerprint") == original_fingerprint and final.get("cleanup_ok") is True
        result = {
            "status": "REPRODUCED" if reproduced else "INCONCLUSIVE",
            "kind": "hypothesis-prefix-shrink-replay",
            "hypothesis": info,
            "database_dir": rel(database_dir, artifacts),
            "seed": seed,
            "original_prefix_operations": len(operations),
            "minimal_failing_prefix_operations": minimal_len,
            "replay": final,
            "attempt_count": len(attempts),
            "attempts": attempts[-100:],
            "fingerprint": original_fingerprint,
            "note": "Hypothesis shrank the deterministic state-machine operation prefix and replayed it on clean fixtures. The operation generator remains the fixed seeded STD-04 stream.",
        }
    except Exception as exc:  # noqa: BLE001
        result = {"status": "INCONCLUSIVE", "kind": "hypothesis-prefix-shrink-replay", "hypothesis": info, "database_dir": rel(database_dir, artifacts), "exception": type(exc).__name__, "message": str(exc), "traceback": traceback.format_exc()}
    write_json(seed_dir / "hypothesis-shrink.json", result)
    return result


def cleanup_fixtures(seed_dir: Path, fixtures: list[tuple[str, Path]], skip_target_cleanup: bool = False) -> tuple[bool, dict[str, Any]]:
    cleanup: dict[str, Any] = {}
    for label, fixture in fixtures:
        if skip_target_cleanup and label == "target":
            cleanup[label] = {"path": str(fixture), "removed": False, "selftest_skipped": True}
            continue
        try:
            shutil.rmtree(fixture)
            cleanup[label] = {"path": str(fixture), "removed": not fixture.exists()}
        except Exception as exc:  # noqa: BLE001
            cleanup[label] = {"path": str(fixture), "removed": False, "exception": type(exc).__name__, "message": str(exc)}
    cleanup_ok = all(item.get("removed") for item in cleanup.values())
    write_json(seed_dir / "cleanup.json", cleanup)
    if not cleanup_ok:
        write_json(seed_dir / "cleanup-error.json", cleanup)
    return cleanup_ok, cleanup


def run_seed(
    reference_root: Path,
    target_root: Path,
    seed: int,
    operations: int,
    artifacts: Path,
    timeout_seconds: float,
    inject_target_fault_at: int | None = None,
    skip_target_cleanup: bool = False,
) -> dict[str, Any]:
    seed_dir = artifacts / "seeds" / f"seed-{seed}"
    seed_dir.mkdir(parents=True, exist_ok=True)
    ref_fixture = reference_root / f"seed-{seed}-ref-{uuid.uuid4().hex[:8]}"
    target_fixture = target_root / f"seed-{seed}-target-{uuid.uuid4().hex[:8]}"
    prepare_root(ref_fixture)
    prepare_root(target_fixture)
    rng = random.Random(seed)
    trace: list[dict[str, Any]] = []
    mismatch: dict[str, Any] | None = None
    timed_out = False
    started_at = time.monotonic()
    for index in range(operations):
        if time.monotonic() - started_at > timeout_seconds:
            timed_out = True
            break
        op = generate_operation(seed, index, rng)
        ref_result = apply_operation(ref_fixture, op)
        target_result = apply_operation(target_fixture, op)
        if inject_target_fault_at is not None and index == inject_target_fault_at:
            target_result = dict(target_result)
            target_result["selftest_injected_fault"] = True
        ref_tree = tree_digest(ref_fixture)
        target_tree = tree_digest(target_fixture)
        event = {
            "index": index,
            "operation": op,
            "reference_result": ref_result,
            "target_result": target_result,
            "affected_paths": {"reference": {path: path_snapshot(ref_fixture, path) for path in affected_paths(op)}, "target": {path: path_snapshot(target_fixture, path) for path in affected_paths(op)}},
            "reference_tree_sha256": ref_tree["sha256"],
            "target_tree_sha256": target_tree["sha256"],
            "reference_entry_count": ref_tree["entry_count"],
            "target_entry_count": target_tree["entry_count"],
        }
        trace.append(event)
        if not compare_payload(ref_result, target_result) or ref_tree["sha256"] != target_tree["sha256"]:
            mismatch = {
                "index": index,
                "failing_prefix_operations": index + 1,
                "minimized": False,
                "reason": "result mismatch" if not compare_payload(ref_result, target_result) else "tree digest mismatch",
                "operation": op,
                "reference_result": ref_result,
                "target_result": target_result,
                "reference_tree": ref_tree,
                "target_tree": target_tree,
            }
            break
    status = "INCONCLUSIVE" if timed_out else "FAIL" if mismatch else "PASS"
    trace_path = seed_dir / "trace.json"
    write_json(trace_path, trace)
    trace_sha = sha256_bytes(json.dumps(trace, sort_keys=True, separators=(",", ":")).encode())
    hypothesis_shrink: dict[str, Any] | None = None
    if mismatch:
        original_fingerprint = mismatch_fingerprint(mismatch)
        hypothesis_shrink = shrink_with_hypothesis(reference_root, target_root, trace, original_fingerprint or {}, artifacts, seed_dir, seed, inject_target_fault_at)
        if hypothesis_shrink.get("status") == "REPRODUCED":
            mismatch["minimized"] = True
            mismatch["minimal_failing_prefix_operations"] = hypothesis_shrink.get("minimal_failing_prefix_operations")
        shrink = {
            "kind": "hypothesis-prefix-shrink-replay",
            "status": hypothesis_shrink.get("status"),
            "seed": seed,
            "failing_prefix_operations": mismatch["failing_prefix_operations"],
            "minimized": hypothesis_shrink.get("status") == "REPRODUCED",
            "minimal_failing_prefix_operations": hypothesis_shrink.get("minimal_failing_prefix_operations"),
            "hypothesis_shrink": rel(seed_dir / "hypothesis-shrink.json", artifacts),
            "mismatch": mismatch,
            "trace_prefix": trace,
            "note": "Hypothesis shrink/replay result is authoritative for minimization. Raw trace_prefix preserves the original failing prefix.",
        }
        write_json(seed_dir / "shrink-corpus.json", shrink)
    else:
        write_json(seed_dir / "shrink-corpus.json", {"kind": "hypothesis-prefix-shrink-replay", "status": "NOT_RUN", "reason": "seed passed"})
    cleanup_ok, cleanup = cleanup_fixtures(seed_dir, [("reference", ref_fixture), ("target", target_fixture)], skip_target_cleanup)
    if not cleanup_ok and status == "PASS":
        status = "INCONCLUSIVE"
    return {
        "seed": seed,
        "status": status,
        "operations_requested": operations,
        "operations_executed": len(trace),
        "trace": rel(trace_path, artifacts),
        "trace_sha256": trace_sha,
        "shrink_corpus": rel(seed_dir / "shrink-corpus.json", artifacts),
        "cleanup": cleanup,
        "cleanup_ok": cleanup_ok,
        "hypothesis_shrink": hypothesis_shrink,
        "mismatch": mismatch,
        "timed_out": timed_out,
        "timeout_seconds": timeout_seconds,
        "reference_fixture": str(ref_fixture),
        "target_fixture": str(target_fixture),
    }


def parse_args(argv: list[str]) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Run AFS STD-04 deterministic differential random driver")
    parser.add_argument("--case-id", default=os.environ.get("AFS_ACCEPTANCE_CASE_ID", "STD-04"))
    parser.add_argument("--profile", choices=["smoke", "full"], default=os.environ.get("AFS_ACCEPTANCE_PROFILE", "smoke"))
    parser.add_argument("--matrix-json", default=os.environ.get("AFS_ACCEPTANCE_MATRIX", "{}"))
    parser.add_argument("--run-dir", type=Path, default=Path(os.environ.get("AFS_ACCEPTANCE_RUN_DIR", "results/std-04-random")))
    parser.add_argument("--mount", type=Path, default=Path(os.environ["AFS_ACCEPTANCE_MOUNT"]) if os.environ.get("AFS_ACCEPTANCE_MOUNT") else None)
    parser.add_argument("--base-dir", type=Path, default=Path(os.environ["AFS_ACCEPTANCE_BASE_DIR"]) if os.environ.get("AFS_ACCEPTANCE_BASE_DIR") else None, help="Target directory under --mount. Relative paths are resolved below --mount.")
    parser.add_argument("--reference-dir", type=Path, default=Path(os.environ["AFS_ACCEPTANCE_REFERENCE_DIR"]) if os.environ.get("AFS_ACCEPTANCE_REFERENCE_DIR") else None)
    parser.add_argument("--seeds", default=None)
    parser.add_argument("--operations", type=int, default=None)
    parser.add_argument("--max-seeds", type=int, default=None)
    parser.add_argument("--per-seed-timeout-seconds", type=float, default=None)
    parser.add_argument("--allow-reference-fixture", action="store_true", help="Allow non-AFS target fixtures for driver selftests and smoke only; full profile remains BLOCKED.")
    parser.add_argument("--backend", default=os.environ.get("AFS_ACCEPTANCE_BACKEND"))
    parser.add_argument("--process-pid", default=os.environ.get("AFS_ACCEPTANCE_PROCESS_PID"))
    parser.add_argument("--meta-process-pid", default=os.environ.get("AFS_ACCEPTANCE_META_PROCESS_PID"))
    parser.add_argument("--selftest-inject-target-fault-at", type=int, default=None, help=argparse.SUPPRESS)
    parser.add_argument("--selftest-skip-target-cleanup", action="store_true", help=argparse.SUPPRESS)
    parser.add_argument("--meta", default=os.environ.get("AFS_ACCEPTANCE_META"))
    return parser.parse_args(argv)


def main(argv: list[str]) -> int:
    args = parse_args(argv)
    run_dir = args.run_dir.resolve()
    artifacts = run_dir / "artifacts" / "std-04-random"
    artifacts.mkdir(parents=True, exist_ok=True)
    checks: list[dict[str, Any]] = []
    matrix = json.loads(args.matrix_json) if args.matrix_json.strip() else {}
    if not isinstance(matrix, dict):
        matrix = {}
    status = "PASS"
    reason = ""
    seed_results: list[dict[str, Any]] = []
    command_summary = {"counts": {value: 0 for value in sorted(RESULT_VALUES)}}

    try:
        if args.case_id != "STD-04":
            raise RuntimeError(f"random_fs.py implements STD-04 only, got {args.case_id}")
        if args.mount is None:
            raise RuntimeError("--mount or AFS_ACCEPTANCE_MOUNT is required")
        if args.reference_dir is None:
            raise RuntimeError("--reference-dir or AFS_ACCEPTANCE_REFERENCE_DIR is required")
        target_base = resolve_base_dir(args.mount, args.base_dir)
        reference_base = args.reference_dir
        expected_seeds = FULL_SEEDS if args.profile == "full" else SMOKE_SEEDS
        seeds = parse_seed_list(args.seeds, expected_seeds)
        cap_applied = False
        if args.max_seeds is not None:
            seeds = seeds[: args.max_seeds]
            cap_applied = True
        operations = args.operations if args.operations is not None else (FULL_OPERATIONS if args.profile == "full" else SMOKE_OPERATIONS)
        timeout_seconds = args.per_seed_timeout_seconds if args.per_seed_timeout_seconds is not None else (900.0 if args.profile == "full" else 60.0)
        full_seed_operation_ok = args.profile != "full" or (seeds == FULL_SEEDS and operations == FULL_OPERATIONS and not cap_applied)
        hyp_identity = hypothesis_identity()
        hypothesis_gate_ok = args.profile != "full" or hyp_identity.get("available") is True
        target_mount_identity = mount_identity(args.mount)
        target_base_identity = mount_identity(target_base)
        reference_identity = mount_identity(reference_base)
        target_fs = findmnt_first(target_base_identity)
        reference_fs = findmnt_first(reference_identity)
        product_backend = args.backend or matrix.get("backend") or ""
        product_target = is_product_backend(str(product_backend))
        node_identity = process_identity(args.process_pid)
        meta_identity = process_identity(args.meta_process_pid)
        observed_checks = target_checks(platform.system(), str(product_backend), target_mount_identity, target_base_identity, node_identity, meta_identity)
        driver_path = Path(__file__).resolve()
        identity = {
            "created_at": utc(),
            "host": run_text(["hostname"]),
            "uname": run_text(["uname", "-a"]),
            "platform": {"system": platform.system(), "release": platform.release(), "machine": platform.machine(), "python": platform.python_version(), "uid": os.geteuid(), "gid": os.getegid()},
            "driver": "AFS custom deterministic differential-random; JuiceFS fsrand/Hypothesis-inspired, not upstream suite",
            "driver_file": {"path": str(driver_path), "sha256": sha256_file(driver_path)},
            "hypothesis": hyp_identity,
            "operation_alphabet": OP_NAMES,
            "selftest_inject_target_fault_at": args.selftest_inject_target_fault_at,
            "selftest_skip_target_cleanup": args.selftest_skip_target_cleanup,
            "allow_reference_fixture": args.allow_reference_fixture,
            "selection": {"profile": args.profile, "full_seeds": FULL_SEEDS, "selected_seeds": seeds, "operations_per_seed": operations, "cap_applied": cap_applied, "per_seed_timeout_seconds": timeout_seconds},
            "product": {"backend": product_backend, "meta": args.meta or matrix.get("meta"), "mount_path": str(args.mount), "target_base": str(target_base), "reference_base": str(reference_base), "process_pid": args.process_pid, "meta_process_pid": args.meta_process_pid, "product_target": product_target},
            "mount": target_mount_identity,
            "target_base_mount": target_base_identity,
            "reference_mount": reference_identity,
            "target_filesystem": target_fs,
            "reference_filesystem": reference_fs,
            "process": node_identity,
            "meta_process": meta_identity,
            "observed_checks": observed_checks,
        }
        write_json(artifacts / "identity.json", identity)
        linux_ok = platform.system() == "Linux"
        mount_ok = args.mount.exists() and target_base.is_dir() and is_under(target_base, args.mount) and identity["target_base_mount"].get("returncode") == 0
        reference_ok = reference_base.is_dir() and identity["reference_mount"].get("returncode") == 0 and reference_fs.get("fstype") == "ext4"
        backend_ok = bool(identity["product"]["backend"])
        target_identity_ok = all(observed_checks.values())
        reference_fixture_ok = (not args.allow_reference_fixture) or args.profile != "full"
        full_contract_ok = full_seed_operation_ok and hypothesis_gate_ok and target_identity_ok and reference_ok and reference_fixture_ok
        checks.extend([
            build_check("linux-host", "PASS" if linux_ok else "BLOCKED", identity["platform"], rel(artifacts / "identity.json", run_dir)),
            build_check("target-mount-scope", "PASS" if mount_ok else "BLOCKED", {"mount": str(args.mount), "target_base": str(target_base), "under_mount": is_under(target_base, args.mount), "exists": target_base.exists(), "filesystem": target_fs}, rel(artifacts / "identity.json", run_dir)),
            build_check("reference-ext4-scope", "PASS" if reference_ok else "BLOCKED", {"reference_base": str(reference_base), "exists": reference_base.exists(), "findmnt_returncode": identity["reference_mount"].get("returncode"), "filesystem": reference_fs}, rel(artifacts / "identity.json", run_dir)),
            build_check("backend-selection", "PASS" if backend_ok else "BLOCKED", identity["product"], rel(artifacts / "identity.json", run_dir)),
            build_check("fixed-seed-operation-contract", "PASS" if full_seed_operation_ok else "BLOCKED", {"profile": args.profile, "required_full_seeds": FULL_SEEDS, "selected_seeds": seeds, "required_operations": FULL_OPERATIONS, "operations_per_seed": operations, "cap_applied": cap_applied}, rel(artifacts / "identity.json", run_dir)),
            build_check("hypothesis-shrink-database", "PASS" if hypothesis_gate_ok else "BLOCKED", {"profile": args.profile, "implemented": True, "hypothesis": hyp_identity, "required_for_full_pass": True}, rel(artifacts / "identity.json", run_dir)),
            build_check("reference-fixture-scope", "PASS" if reference_fixture_ok else "BLOCKED", {"allow_reference_fixture": args.allow_reference_fixture, "profile": args.profile}, rel(artifacts / "identity.json", run_dir)),
        ])
        for name, passed in observed_checks.items():
            checks.append(build_check(name, "PASS" if passed else "BLOCKED", {"observed": passed}, rel(artifacts / "identity.json", run_dir)))
        if not linux_ok or not mount_ok or not reference_ok or not backend_ok or not target_identity_ok or not reference_fixture_ok or not hypothesis_gate_ok:
            status = "BLOCKED"
            reason = "STD-04 preflight failed"
        else:
            for seed in seeds:
                result = run_seed(reference_base, target_base, seed, operations, artifacts, timeout_seconds, args.selftest_inject_target_fault_at, args.selftest_skip_target_cleanup)
                seed_results.append(result)
                command_summary["counts"][result["status"]] += 1
                if result["status"] in {"FAIL", "INCONCLUSIVE"}:
                    break
            write_json(artifacts / "seed-results.json", seed_results)
            if command_summary["counts"].get("FAIL", 0):
                status = "FAIL"
                reason = "differential mismatch found; Hypothesis shrink/replay artifacts preserved"
            elif command_summary["counts"].get("INCONCLUSIVE", 0):
                status = "INCONCLUSIVE"
                reason = "seed execution timed out or cleanup could not prove a clean result"
            elif args.profile == "full" and not full_contract_ok:
                status = "BLOCKED"
                reason = "full STD-04 requires 10 fixed seeds x 10,000 operations, qualified AFS target identity, ext4 reference, and Hypothesis shrink/replay database availability"
            else:
                status = "PASS"
                reason = ""
            checks.append(build_check("seed-execution", "PASS" if seed_results else "BLOCKED", {"selected": len(seeds), "executed": len(seed_results), "counts": command_summary["counts"]}, rel(artifacts / "seed-results.json", run_dir)))
            checks.append(build_check("differential-results", "PASS" if status == "PASS" else "FAIL" if status == "FAIL" else "INCONCLUSIVE" if status == "INCONCLUSIVE" else "BLOCKED", {"counts": command_summary["counts"], "first_failure": next((item for item in seed_results if item["status"] == "FAIL"), None), "first_inconclusive": next((item for item in seed_results if item["status"] == "INCONCLUSIVE"), None)}, rel(artifacts / "seed-results.json", run_dir)))
    except Exception as exc:  # noqa: BLE001
        status = "BLOCKED"
        reason = f"driver setup failed: {type(exc).__name__}: {exc}"
        write_json(artifacts / "setup-error.json", {"exception": type(exc).__name__, "message": str(exc), "traceback": traceback.format_exc()})
        checks.append(build_check("driver-setup", "BLOCKED", reason, rel(artifacts / "setup-error.json", run_dir)))

    accounting = {
        "profile": args.profile,
        "fixed_full_seeds": FULL_SEEDS,
        "selected_seeds": [item.get("seed") for item in seed_results] or (parse_seed_list(args.seeds, FULL_SEEDS if args.profile == "full" else SMOKE_SEEDS)[: args.max_seeds] if args.max_seeds else parse_seed_list(args.seeds, FULL_SEEDS if args.profile == "full" else SMOKE_SEEDS)),
        "executed": len(seed_results),
        "operations_per_seed": args.operations if args.operations is not None else (FULL_OPERATIONS if args.profile == "full" else SMOKE_OPERATIONS),
        "required_full_operations_per_seed": FULL_OPERATIONS,
        "result_counts": command_summary["counts"],
        "raw_trace_per_seed": True,
        "hypothesis_database_per_failure": True,
        "shrink_corpus_per_seed": True,
        "no_post_failure_filtering": True,
    }
    write_json(artifacts / "accounting.json", accounting)
    coverage_axes = {
        "reference": {"values": [str(matrix.get("reference", "ext4"))], "checks": {str(matrix.get("reference", "ext4")): "reference-ext4-scope"}},
        "seeds": {"values": [str(len(accounting["selected_seeds"]))], "checks": {str(len(accounting["selected_seeds"])): "fixed-seed-operation-contract"}},
        "operations_per_seed": {"values": [str(accounting["operations_per_seed"])], "checks": {str(accounting["operations_per_seed"]): "fixed-seed-operation-contract"}},
    }
    coverage_axes[f"seeds_{args.profile}"] = coverage_axes["seeds"]
    coverage_axes[f"operations_per_seed_{args.profile}"] = coverage_axes["operations_per_seed"]
    proof = {
        "case_id": args.case_id,
        "profile": args.profile,
        "matrix": matrix,
        "status": status,
        "reason": reason,
        "checks": checks,
        "coverage": {"profile": args.profile, "axes": coverage_axes},
        "artifacts": {"root": rel(artifacts, run_dir)},
        "identity": {"artifact": rel(artifacts / "identity.json", run_dir)},
        "accounting": accounting,
        "command_summary": command_summary,
        "notes": [
            "This is an AFS custom deterministic differential driver inspired by JuiceFS fsrand/Hypothesis; it is not an upstream JuiceFS suite.",
            "Smoke runs use reduced seeds/operation counts and cannot satisfy full STD-04 release PASS.",
            "On failure the original mismatch trace is preserved and Hypothesis shrink/replay writes hypothesis-shrink.json plus a DirectoryBasedExampleDatabase.",
            "Full STD-04 release PASS requires 10 fixed seeds x 10,000 operations on qualified AFS target identity with Hypothesis available.",
        ],
    }
    write_json(artifacts / "proof.json", proof)
    print(json.dumps(proof, sort_keys=True))
    return 0 if status == "PASS" else 1


if __name__ == "__main__":
    raise SystemExit(main(sys.argv[1:]))
