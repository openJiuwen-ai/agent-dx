#!/usr/bin/env python3
"""Small OwnerFs remote write vs MooseFS write diagnostic.

This records bounded 64 MiB write samples only. It is diagnostic data, not a
MooseFS parity or durable-ACK qualification. The parent owns cross-node B mount
readback and environment capacity checks.
"""
from __future__ import annotations

import argparse
import hashlib
import json
import os
from pathlib import Path
import platform
import stat
import subprocess
import time
from typing import Any

DATA_BYTES = 64 * 1024 * 1024
BLOCK_BYTES = 1024 * 1024
CONCURRENCY = 1
PATTERN_BYTE = 97
EXPECTED_SHA256 = "fae972222d455a2eaee1661ad9625502ec3bfc5ec38b87a6eec5afd5107331b5"
IO_TOOL_SHA256 = "70ac97c7634d406a177a74c783d446b62a2014ba198586132882a1d9228e55e8"
PRODUCT_SOURCE_COMMIT = "6d51aeb45c1ed8669d80f612b3817e6d1bdabe04"
COMPILER_INPUT_MAP = "66dbbe3e0071fcec1efc9cf370c709a99f025d39acd4f37ba57582c874429304"
WARMUP_ROUNDS = 1
MEASUREMENT_ROUNDS = 5


def sha256_file(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for chunk in iter(lambda: handle.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def write_json(path: Path, value: Any) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(value, indent=2, sort_keys=True) + "\n", encoding="utf-8")


def require_absolute_path(value: str, name: str) -> Path:
    path = Path(value)
    if not path.is_absolute():
        raise ValueError(f"{name} must be absolute: {value}")
    return path


def reject_symlink_components(path: Path, name: str) -> None:
    probe = Path(path.anchor)
    parts = path.parts[1:] if path.is_absolute() else path.parts
    for part in parts:
        probe = probe / part
        if probe.is_symlink():
            raise ValueError(f"{name} contains symlink component: {probe}")


def require_existing_directory(path: Path, name: str) -> None:
    reject_symlink_components(path, name)
    if not path.is_dir():
        raise ValueError(f"{name} must be an existing directory: {path}")


def validate_output_path(path: Path) -> None:
    if path.is_symlink() or path.exists():
        raise ValueError(f"output must be fresh and non-symlink: {path}")
    if not path.parent.is_dir():
        raise ValueError(f"output parent must exist: {path.parent}")


def stat_identity(path: Path) -> dict[str, Any]:
    st = path.stat()
    return {"path": str(path), "device": st.st_dev, "inode": st.st_ino, "mode": stat.S_IMODE(st.st_mode), "uid": st.st_uid, "gid": st.st_gid}


def find_mount(path: Path) -> dict[str, Any]:
    result = subprocess.run(
        ["findmnt", "-J", "-T", str(path), "-o", "TARGET,SOURCE,FSTYPE,OPTIONS,ID"],
        text=True,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        check=True,
    )
    filesystems = json.loads(result.stdout).get("filesystems") or []
    if not filesystems:
        raise ValueError(f"no mount found for {path}")
    return filesystems[0]


def require_owner_mount(mount: dict[str, Any]) -> None:
    fstype = str(mount.get("fstype", ""))
    source = str(mount.get("source", ""))
    if not (fstype == "fuse" or fstype.startswith("fuse.")) or source != "afs-ownerfs":
        raise ValueError(f"owner root must be exact FUSE source afs-ownerfs: {mount}")


def require_moose_mount(mount: dict[str, Any]) -> None:
    source = str(mount.get("source", ""))
    fstype = str(mount.get("fstype", ""))
    if fstype not in ("fuse", "fuse.mfs") or source != "mfs#192.168.109.11:23042":
        raise ValueError(f"moose root must be standard FUSE with exact one-copy MooseFS source mfs#192.168.109.11:23042: {mount}")


def fsync_directory(path: Path) -> None:
    descriptor = os.open(path, os.O_RDONLY | getattr(os, "O_DIRECTORY", 0))
    try:
        os.fsync(descriptor)
    finally:
        os.close(descriptor)


def verify_linux_aarch64_root() -> dict[str, Any]:
    identity = {"system": platform.system(), "machine": platform.machine(), "euid": os.geteuid()}
    if identity != {"system": "Linux", "machine": "aarch64", "euid": 0}:
        raise RuntimeError(f"requires Linux aarch64 root: {identity}")
    return identity


def validate_io_tool(path: Path) -> dict[str, str]:
    reject_symlink_components(path, "io-tool")
    if not path.is_file():
        raise ValueError(f"io tool must be an existing file: {path}")
    digest = sha256_file(path)
    if digest != IO_TOOL_SHA256:
        raise ValueError(f"io tool sha256 mismatch: {digest}")
    return {"path": str(path), "sha256": digest}


def validate_write_result(record: dict[str, Any]) -> None:
    expected = {
        "operation": "seq-write",
        "file_bytes": DATA_BYTES,
        "io_bytes": DATA_BYTES,
        "block_bytes": BLOCK_BYTES,
        "concurrency": CONCURRENCY,
        "barrier": "fdatasync",
        "pattern_byte": PATTERN_BYTE,
        "operations": DATA_BYTES // BLOCK_BYTES,
        "cache_requested": "unobserved",
        "content_ok": True,
    }
    for key, value in expected.items():
        if record.get(key) != value:
            raise ValueError(f"unexpected write result {key}: {record.get(key)!r}")
    if record.get("residency_observed") is not False:
        raise ValueError("cache residency must remain unobserved")
    if not isinstance(record.get("wall_ns"), int) or record["wall_ns"] <= 0:
        raise ValueError("write result missing positive wall_ns")


def run_io_write(io_tool: Path, payload: Path) -> dict[str, Any]:
    argv = [
        str(io_tool), str(payload), "seq-write", str(DATA_BYTES), str(BLOCK_BYTES), str(CONCURRENCY),
        "fdatasync", str(DATA_BYTES), str(PATTERN_BYTE), "create", "unobserved",
    ]
    began = time.monotonic_ns()
    completed = subprocess.run(argv, text=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
    record: dict[str, Any] = {"argv": argv, "rc": completed.returncode, "stdout": completed.stdout, "stderr": completed.stderr, "elapsed_ns": time.monotonic_ns() - began, "status": "FAIL"}
    try:
        result = json.loads(completed.stdout)
        record["result"] = result
        if completed.returncode != 0:
            raise ValueError(f"io exited {completed.returncode}")
        validate_write_result(result)
        record["verify"] = {"status": "PASS"}
        record["status"] = "PASS"
    except Exception as error:
        record["verify"] = {"status": "FAIL", "error": repr(error)}
    return record


def verify_content(path: Path) -> dict[str, Any]:
    with path.open("rb") as handle:
        data = handle.read()
        tail = handle.read(1)
    digest = hashlib.sha256(data).hexdigest()
    status = "PASS" if len(data) == DATA_BYTES and tail == b"" and digest == EXPECTED_SHA256 else "FAIL"
    return {"status": status, "path": str(path), "bytes": len(data), "sha256": digest, "expected_sha256": EXPECTED_SHA256, "tail_empty": tail == b""}


def paired_order(round_index: int) -> list[str]:
    return ["owner", "moose"] if round_index % 2 == 0 else ["moose", "owner"]


def sample_name(target: str, round_index: int) -> str:
    return f"{target}-write-r{round_index:02d}-payload64m.bin"


def run_sample(root: Path, target: str, round_index: int, measured: bool, io_tool: Path, output: Path) -> dict[str, Any]:
    payload = root / sample_name(target, round_index)
    record: dict[str, Any] = {"target": target, "round": round_index, "measured": measured, "path": str(payload), "status": "FAIL"}
    try:
        if payload.exists():
            raise FileExistsError(str(payload))
        command = run_io_write(io_tool, payload)
        record["write"] = command
        fsync_directory(root)
        content = verify_content(payload)
        record["content_verify"] = content
        record["dir_fsync"] = True
        record["status"] = "PASS" if command.get("status") == "PASS" and content.get("status") == "PASS" else "FAIL"
    except Exception as error:
        record["error"] = repr(error)
    finally:
        sample_path = output / "samples" / f"round-{round_index:02d}-{target}.json"
        write_json(sample_path, record)
        record["artifact"] = str(sample_path)
    return record


def run_rounds(owner_root: Path, moose_root: Path, io_tool: Path, output: Path) -> list[dict[str, Any]]:
    rounds: list[dict[str, Any]] = []
    for index in range(WARMUP_ROUNDS + MEASUREMENT_ROUNDS):
        samples: list[dict[str, Any]] = []
        for target in paired_order(index):
            root = owner_root if target == "owner" else moose_root
            samples.append(run_sample(root, target, index, index >= WARMUP_ROUNDS, io_tool, output))
        round_record = {"round": index, "measured": index >= WARMUP_ROUNDS, "order": paired_order(index), "samples": samples}
        write_json(output / f"round-{index:02d}.json", round_record)
        rounds.append(round_record)
    return rounds


def write_status(rounds: list[dict[str, Any]]) -> str:
    if len(rounds) != WARMUP_ROUNDS + MEASUREMENT_ROUNDS:
        return "FAIL"
    for index, round_record in enumerate(rounds):
        if round_record.get("order") != paired_order(index):
            return "FAIL"
        samples = round_record.get("samples")
        if not isinstance(samples, list) or len(samples) != 2:
            return "FAIL"
        for sample in samples:
            if sample.get("status") != "PASS" or sample.get("write", {}).get("verify", {}).get("status") != "PASS" or sample.get("content_verify", {}).get("status") != "PASS" or sample.get("dir_fsync") is not True:
                return "FAIL"
    return "DATA_RECORDED"


def run(args: argparse.Namespace) -> dict[str, Any]:
    result: dict[str, Any] = {
        "status": "BLOCKED",
        "scope": "small OwnerFs remote write vs MooseFS write diagnostic; not parity or durability qualification",
        "cache_state": "unobserved",
        "moose_strong_durable_ack": "BLOCKED_PARENT_BASELINE",
        "product_source_commit": PRODUCT_SOURCE_COMMIT,
        "source6d": PRODUCT_SOURCE_COMMIT[:7],
        "compiler_input_map": COMPILER_INPUT_MAP,
        "map66": COMPILER_INPUT_MAP[:8],
        "shape": {"bytes": DATA_BYTES, "block_bytes": BLOCK_BYTES, "concurrency": CONCURRENCY, "pattern_byte": PATTERN_BYTE, "warmups": WARMUP_ROUNDS, "measurements": MEASUREMENT_ROUNDS},
    }
    output: Path | None = None
    output_created = False
    try:
        owner_root = require_absolute_path(args.owner_root, "owner-root")
        moose_root = require_absolute_path(args.moose_root, "moose-root")
        io_tool = require_absolute_path(args.io_tool, "io-tool")
        output = require_absolute_path(args.output, "output")
        validate_output_path(output)
        output.mkdir(mode=0o700)
        output_created = True
        platform_identity = verify_linux_aarch64_root()
        require_existing_directory(owner_root, "owner-root")
        require_existing_directory(moose_root, "moose-root")
        owner_mount = find_mount(owner_root)
        moose_mount = find_mount(moose_root)
        require_owner_mount(owner_mount)
        require_moose_mount(moose_mount)
        io_identity = validate_io_tool(io_tool)
        preflight = {
            "platform": platform_identity,
            "io_tool": io_identity,
            "roots": {"owner": stat_identity(owner_root), "moose": stat_identity(moose_root)},
            "mounts": {"owner": owner_mount, "moose": moose_mount},
        }
        result["preflight"] = preflight
        write_json(output / "preflight.json", preflight)
        result["status"] = "FAIL"
        rounds = run_rounds(owner_root, moose_root, io_tool, output)
        result["rounds"] = rounds
        write_json(output / "rounds.json", rounds)
        result["status"] = write_status(rounds)
    except Exception as error:
        result["error"] = repr(error)
    finally:
        if output_created and output is not None and output.exists() and output.is_dir():
            write_json(output / "summary.json", result)
    return result


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--owner-root", required=True)
    parser.add_argument("--moose-root", required=True)
    parser.add_argument("--io-tool", required=True)
    parser.add_argument("--output", required=True)
    return parser.parse_args()


def main() -> int:
    result = run(parse_args())
    print(json.dumps(result, indent=2, sort_keys=True))
    return 0 if result.get("status") == "DATA_RECORDED" else 1


if __name__ == "__main__":
    raise SystemExit(main())
