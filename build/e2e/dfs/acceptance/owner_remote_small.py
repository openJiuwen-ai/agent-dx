#!/usr/bin/env python3
"""Small OwnerFs remote-vs-MooseFS diagnostic slice.

This tool records a bounded 100-file unlink diagnostic and, optionally, a fixed
64 MiB sequential-read C-probe diagnostic. It is evidence collection only: cache
state is unobserved unless explicitly requested, and the output must not be
interpreted as a performance gate.
"""
from __future__ import annotations

import argparse
import hashlib
import json
import os
from pathlib import Path
import platform
import shutil
import stat
import subprocess
import time
from typing import Any

DELETE_FILE_COUNT = 100
DELETE_FILE_BYTES = 4096
WARMUP_ROUNDS = 1
MEASUREMENT_ROUNDS = 5
READ_BYTES = 64 * 1024 * 1024
READ_BLOCK_BYTES = 1024 * 1024
READ_CONCURRENCY = 1
READ_PATTERN_BYTE = 97
IO_TOOL_SHA256 = "70ac97c7634d406a177a74c783d446b62a2014ba198586132882a1d9228e55e8"
RAW_LATENCY_IO_TOOL_SHA256 = "0d4346c99ad5ed3a7af0306c7db965eb1d6ea57595a89d639216a2be15c9fa8e"
READ_DEFAULT_CACHE = "unobserved"
READ_RAW_CACHE_CHOICES = ("unobserved", "repeat")
READ_LATENCY_INTERVAL = "pread+count-check"
READ_LATENCY_ORDER = "sorted"


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


def require_existing_directory(path: Path, name: str) -> None:
    if path.is_symlink():
        raise ValueError(f"{name} must not be a symlink: {path}")
    if not path.is_dir():
        raise ValueError(f"{name} must be an existing directory: {path}")


def validate_output_path(path: Path) -> None:
    if path.is_symlink() or path.exists():
        raise ValueError(f"output must not already exist or be a symlink: {path}")
    if not path.parent.is_dir():
        raise ValueError(f"output parent must exist: {path.parent}")


def stat_identity(path: Path) -> dict[str, Any]:
    st = path.stat()
    return {
        "path": str(path),
        "device": st.st_dev,
        "inode": st.st_ino,
        "mode": stat.S_IMODE(st.st_mode),
        "uid": st.st_uid,
        "gid": st.st_gid,
    }


def find_mount(path: Path) -> dict[str, Any]:
    result = subprocess.run(
        ["findmnt", "-J", "-T", str(path), "-o", "TARGET,SOURCE,FSTYPE,OPTIONS,ID"],
        text=True,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        check=True,
    )
    data = json.loads(result.stdout)
    filesystems = data.get("filesystems") or []
    if not filesystems:
        raise ValueError(f"no mount found for {path}")
    return filesystems[0]


def require_fuse_mount(mount: dict[str, Any], name: str) -> None:
    fstype = str(mount.get("fstype", ""))
    if not (fstype == "fuse" or fstype.startswith("fuse.")):
        raise ValueError(f"{name} must be on a FUSE mount, got {fstype}")


def fsync_directory(path: Path) -> None:
    descriptor = os.open(path, os.O_RDONLY | getattr(os, "O_DIRECTORY", 0))
    try:
        os.fsync(descriptor)
    finally:
        os.close(descriptor)


def expected_bytes(label: str, index: int) -> bytes:
    seed = f"owner-remote-small:{label}:{index:04d}\n".encode("ascii")
    return (seed * ((DELETE_FILE_BYTES // len(seed)) + 1))[:DELETE_FILE_BYTES]


def prepare_delete_files(directory: Path, label: str) -> dict[str, Any]:
    files: list[str] = []
    for index in range(DELETE_FILE_COUNT):
        path = directory / f"entry-{index:04d}.bin"
        flags = os.O_WRONLY | os.O_CREAT | os.O_EXCL
        descriptor = os.open(path, flags, 0o600)
        try:
            payload = expected_bytes(label, index)
            written = os.write(descriptor, payload)
            if written != DELETE_FILE_BYTES:
                raise OSError(f"short write for {path}: {written}")
            os.fdatasync(descriptor)
        finally:
            os.close(descriptor)
        files.append(path.name)
    fsync_directory(directory)
    return {"files": files, "fdatasync_per_file": True, "parent_fsync": True}


def verify_delete_file_contents(directory: Path, label: str) -> dict[str, Any]:
    checked = 0
    for index in range(DELETE_FILE_COUNT):
        path = directory / f"entry-{index:04d}.bin"
        with path.open("rb") as handle:
            data = handle.read()
            tail = handle.read(1)
        if data != expected_bytes(label, index):
            raise ValueError(f"content mismatch for {path}")
        if tail != b"":
            raise ValueError(f"unexpected trailing read for {path}")
        checked += 1
    return {"status": "PASS", "files_checked": checked, "bytes_per_file": DELETE_FILE_BYTES}


def timed_unlink(directory: Path) -> dict[str, Any]:
    names = [f"entry-{index:04d}.bin" for index in range(DELETE_FILE_COUNT)]
    began = time.monotonic_ns()
    for name in names:
        os.unlink(directory / name)
    elapsed = time.monotonic_ns() - began
    return {"status": "PASS", "files": DELETE_FILE_COUNT, "wall_ns": elapsed}


def check_deleted_on_home(owner_home_root: Path, sample_name: str) -> dict[str, Any]:
    home_dir = owner_home_root / sample_name
    checked = 0
    present: list[str] = []
    for index in range(DELETE_FILE_COUNT):
        path = home_dir / f"entry-{index:04d}.bin"
        if path.exists():
            present.append(str(path))
        checked += 1
    return {"status": "PASS" if not present else "FAIL", "checked": checked, "present": present, "directory": str(home_dir)}


def run_delete_sample(root: Path, label: str, round_index: int, measured: bool, owner_home_root: Path | None) -> dict[str, Any]:
    sample_name = f".afs-owner-remote-small-{label}-r{round_index:02d}"
    directory = root / sample_name
    record: dict[str, Any] = {
        "target": label,
        "round": round_index,
        "measured": measured,
        "directory": str(directory),
        "status": "FAIL",
    }
    cleanup: dict[str, Any] = {"status": "NOT_RUN"}
    try:
        directory.mkdir(mode=0o700)
        record["prepare"] = prepare_delete_files(directory, f"{label}:r{round_index}")
        record["fresh_open_verify"] = verify_delete_file_contents(directory, f"{label}:r{round_index}")
        record["unlink_timer"] = timed_unlink(directory)
        fsync_directory(directory)
        remaining = sorted(path.name for path in directory.iterdir())
        record["post_unlink"] = {"parent_fsync": True, "remaining": remaining, "status": "PASS" if not remaining else "FAIL"}
        if owner_home_root is not None:
            record["owner_home_deleted_check"] = check_deleted_on_home(owner_home_root, sample_name)
            record["owner_home_deleted_check"]["scope"] = "same-host local namespace check only; external Home/B validation is collected by the parent layer"
        else:
            record["external_home_absence"] = "PENDING_PARENT_B_MOUNT_CHECK"
        directory.rmdir()
        cleanup = {"status": "PASS", "removed_sample_dir": True}
        checks = [record["fresh_open_verify"], record["unlink_timer"], record["post_unlink"], cleanup]
        if "owner_home_deleted_check" in record:
            checks.append(record["owner_home_deleted_check"])
        record["status"] = "PASS" if all(item.get("status") == "PASS" for item in checks) else "FAIL"
    except Exception as error:
        record["error"] = repr(error)
        if directory.exists():
            cleanup["left_in_place"] = str(directory)
    finally:
        record["cleanup"] = cleanup
    return record


def paired_order(round_index: int) -> list[str]:
    return ["owner", "moose"] if round_index % 2 == 0 else ["moose", "owner"]


def run_delete_rounds(owner_root: Path, owner_home_root: Path, moose_root: Path, output: Path) -> list[dict[str, Any]]:
    rounds: list[dict[str, Any]] = []
    samples_dir = output / "delete-samples"
    total = WARMUP_ROUNDS + MEASUREMENT_ROUNDS
    for round_index in range(total):
        samples = []
        for label in paired_order(round_index):
            if label == "owner":
                sample = run_delete_sample(owner_root, label, round_index, round_index >= WARMUP_ROUNDS, owner_home_root)
            else:
                sample = run_delete_sample(moose_root, label, round_index, round_index >= WARMUP_ROUNDS, None)
            sample_path = samples_dir / f"round-{round_index:02d}-{label}.json"
            write_json(sample_path, sample)
            sample["artifact"] = str(sample_path)
            samples.append(sample)
            if read_inputs.get("raw_latency", False) and sample.get("status") != "PASS":
                # Keep the first failure; do not repeat an unsupported cache or
                # filesystem operation through the rest of a raw-latency case.
                round_record = {"round": round_index, "measured": round_index >= WARMUP_ROUNDS,
                                "order": paired_order(round_index), "samples": samples,
                                "stopped_after_failure": True}
                write_json(output / f"read-round-{round_index:02d}.json", round_record)
                rounds.append(round_record)
                return rounds
        round_record = {"round": round_index, "measured": round_index >= WARMUP_ROUNDS, "order": paired_order(round_index), "samples": samples}
        write_json(output / f"delete-round-{round_index:02d}.json", round_record)
        rounds.append(round_record)
    return rounds


def percentile_from_sorted(samples: list[int], percentile: int) -> int:
    return samples[len(samples) * percentile // 100]


def validate_latency_samples(record: dict[str, Any]) -> None:
    operations = READ_BYTES // READ_BLOCK_BYTES
    samples = record.get("latency_samples_ns")
    if not isinstance(samples, list) or len(samples) != operations:
        raise ValueError(f"latency_samples_ns must contain {operations} intervals")
    if not all(type(value) is int and value > 0 for value in samples):
        raise ValueError("latency_samples_ns must contain only positive integer intervals")
    if samples != sorted(samples):
        raise ValueError("latency_samples_ns must be sorted")
    if record.get("latency_order") != READ_LATENCY_ORDER:
        raise ValueError(f"unexpected latency_order: {record.get('latency_order')!r}")
    if record.get("latency_interval") != READ_LATENCY_INTERVAL:
        raise ValueError(f"unexpected latency_interval: {record.get('latency_interval')!r}")
    for percentile in (50, 95, 99):
        expected = percentile_from_sorted(samples, percentile)
        actual = record.get(f"p{percentile}_ns")
        if type(actual) is not int or actual != expected:
            raise ValueError(f"p{percentile}_ns does not match latency_samples_ns")


def validate_io_result(record: dict[str, Any], *, raw_latency: bool = False, read_cache: str = READ_DEFAULT_CACHE) -> None:
    expected = {
        "operation": "seq-read",
        "file_bytes": READ_BYTES,
        "io_bytes": READ_BYTES,
        "block_bytes": READ_BLOCK_BYTES,
        "concurrency": READ_CONCURRENCY,
        "barrier": "close",
        "pattern_byte": READ_PATTERN_BYTE,
        "operations": READ_BYTES // READ_BLOCK_BYTES,
        "cache_requested": read_cache,
        "content_ok": True,
    }
    for key, value in expected.items():
        actual = record.get(key)
        if raw_latency and type(value) is int and type(actual) is not int:
            raise ValueError(f"unexpected io result {key}: {actual!r}")
        if actual != value:
            raise ValueError(f"unexpected io result {key}: {record.get(key)!r}")
    if type(record.get("wall_ns")) is not int or record["wall_ns"] <= 0:
        raise ValueError("io result missing positive wall_ns")
    if record.get("residency_observed") is not (read_cache == "repeat"):
        raise ValueError("read diagnostic residency observation must match --read-cache repeat")
    if raw_latency:
        validate_latency_samples(record)
    elif "latency_samples_ns" in record:
        raise ValueError("latency samples require explicit --raw-latency")


def run_io_read(
    io_tool: Path,
    read_file: Path,
    label: str,
    round_index: int,
    measured: bool,
    *,
    raw_latency: bool = False,
    read_cache: str = READ_DEFAULT_CACHE,
) -> dict[str, Any]:
    argv = [
        str(io_tool),
        str(read_file),
        "seq-read",
        str(READ_BYTES),
        str(READ_BLOCK_BYTES),
        str(READ_CONCURRENCY),
        "close",
        str(READ_BYTES),
        str(READ_PATTERN_BYTE),
        "existing",
        read_cache,
    ]
    if raw_latency:
        argv.append("samples")
    began = time.monotonic_ns()
    completed = subprocess.run(argv, text=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
    record: dict[str, Any] = {
        "target": label,
        "round": round_index,
        "measured": measured,
        "argv": argv,
        "rc": completed.returncode,
        "stdout": completed.stdout,
        "stderr": completed.stderr,
        "elapsed_ns": time.monotonic_ns() - began,
        "status": "FAIL",
    }
    try:
        payload = json.loads(completed.stdout)
        record["result"] = payload
        if completed.returncode != 0:
            raise ValueError(f"io tool exited {completed.returncode}")
        validate_io_result(payload, raw_latency=raw_latency, read_cache=read_cache)
        record["verify"] = {"status": "PASS"}
        record["status"] = "PASS"
    except Exception as error:
        record["verify"] = {"status": "FAIL", "error": repr(error)}
    return record



def run_read_rounds(read_inputs: dict[str, Any], output: Path) -> list[dict[str, Any]]:
    rounds: list[dict[str, Any]] = []
    samples_dir = output / "read-samples"
    total = WARMUP_ROUNDS + MEASUREMENT_ROUNDS
    for round_index in range(total):
        samples = []
        for label in paired_order(round_index):
            read_file = read_inputs["owner_read_file"] if label == "owner" else read_inputs["moose_read_file"]
            sample = run_io_read(
                read_inputs["io_tool"],
                read_file,
                label,
                round_index,
                round_index >= WARMUP_ROUNDS,
                raw_latency=read_inputs.get("raw_latency", False),
                read_cache=read_inputs.get("read_cache", READ_DEFAULT_CACHE),
            )
            sample_path = samples_dir / f"round-{round_index:02d}-{label}.json"
            write_json(sample_path, sample)
            sample["artifact"] = str(sample_path)
            samples.append(sample)
            if read_inputs.get("raw_latency", False) and sample.get("status") != "PASS":
                # Keep the first failure; do not repeat an unsupported cache or
                # filesystem operation through the rest of a raw-latency case.
                round_record = {"round": round_index, "measured": round_index >= WARMUP_ROUNDS,
                                "order": paired_order(round_index), "samples": samples,
                                "stopped_after_failure": True}
                write_json(output / f"read-round-{round_index:02d}.json", round_record)
                rounds.append(round_record)
                return rounds
        round_record = {"round": round_index, "measured": round_index >= WARMUP_ROUNDS, "order": paired_order(round_index), "samples": samples}
        write_json(output / f"read-round-{round_index:02d}.json", round_record)
        rounds.append(round_record)
    return rounds

def expect_round_count(rounds: list[dict[str, Any]]) -> bool:
    return len(rounds) == WARMUP_ROUNDS + MEASUREMENT_ROUNDS


def delete_status(rounds: list[dict[str, Any]]) -> str:
    if not expect_round_count(rounds):
        return "FAIL"
    for round_record in rounds:
        samples = round_record.get("samples", [])
        if len(samples) != 2:
            return "FAIL"
        for sample in samples:
            if sample.get("status") != "PASS" or sample.get("cleanup", {}).get("status") != "PASS":
                return "FAIL"
    return "DATA_RECORDED"


def read_status(read_rounds: list[dict[str, Any]]) -> str:
    if not expect_round_count(read_rounds):
        return "FAIL"
    for round_record in read_rounds:
        samples = round_record.get("samples", [])
        if len(samples) != 2:
            return "FAIL"
        for sample in samples:
            if sample.get("status") != "PASS" or sample.get("verify", {}).get("status") != "PASS":
                return "FAIL"
    return "DATA_RECORDED"


def compute_status(rounds: list[dict[str, Any]], read_rounds: list[dict[str, Any]], selected_case: str) -> str:
    statuses: list[str] = []
    if selected_case in ("all", "delete"):
        statuses.append(delete_status(rounds))
    if selected_case in ("all", "read"):
        statuses.append(read_status(read_rounds))
    if not statuses:
        return "FAIL"
    return "DATA_RECORDED" if all(status == "DATA_RECORDED" for status in statuses) else "FAIL"


def require_read_inputs_for_case(selected_case: str, read_inputs: dict[str, Path] | None) -> None:
    if selected_case in ("all", "read") and read_inputs is None:
        raise ValueError(f"--case {selected_case} requires --io-tool, --owner-read-file, and --moose-read-file")


def validate_read_inputs(args: argparse.Namespace, owner_root: Path, moose_root: Path) -> dict[str, Any] | None:
    supplied = [args.io_tool, args.owner_read_file, args.moose_read_file]
    if not any(supplied):
        if args.raw_latency or args.read_cache != READ_DEFAULT_CACHE:
            raise ValueError("--raw-latency and --read-cache require --io-tool, --owner-read-file, and --moose-read-file")
        return None
    if not all(supplied):
        raise ValueError("--io-tool, --owner-read-file, and --moose-read-file must be supplied together")
    if args.read_cache not in READ_RAW_CACHE_CHOICES:
        raise ValueError(f"--read-cache must be one of {READ_RAW_CACHE_CHOICES}")
    if args.read_cache != READ_DEFAULT_CACHE and not args.raw_latency:
        raise ValueError("--read-cache repeat requires explicit --raw-latency")
    io_tool = require_absolute_path(args.io_tool, "io-tool")
    owner_file = require_absolute_path(args.owner_read_file, "owner-read-file")
    moose_file = require_absolute_path(args.moose_read_file, "moose-read-file")
    for path, name in ((io_tool, "io-tool"), (owner_file, "owner-read-file"), (moose_file, "moose-read-file")):
        if path.is_symlink():
            raise ValueError(f"{name} must not be a symlink: {path}")
        if not path.is_file():
            raise ValueError(f"{name} must be an existing file: {path}")
    expected_sha256 = RAW_LATENCY_IO_TOOL_SHA256 if args.raw_latency else IO_TOOL_SHA256
    actual_sha256 = sha256_file(io_tool)
    if actual_sha256 != expected_sha256:
        raise ValueError(f"io-tool sha256 mismatch: {actual_sha256}")
    if owner_root not in (owner_file, *owner_file.parents):
        raise ValueError("owner-read-file must be under --owner-root")
    if moose_root not in (moose_file, *moose_file.parents):
        raise ValueError("moose-read-file must be under --moose-root")
    return {
        "io_tool": io_tool,
        "owner_read_file": owner_file,
        "moose_read_file": moose_file,
        "io_tool_sha256": actual_sha256,
        "raw_latency": args.raw_latency,
        "read_cache": args.read_cache,
    }


def preflight(args: argparse.Namespace, output: Path) -> tuple[Path, Path, Path, Path, dict[str, Any], dict[str, Any] | None]:
    owner_root = require_absolute_path(args.owner_root, "owner-root")
    owner_home_root = require_absolute_path(args.owner_home_root, "owner-home-root") if args.owner_home_root else None
    moose_root = require_absolute_path(args.moose_root, "moose-root")
    for path, name in ((owner_root, "owner-root"), (moose_root, "moose-root")):
        require_existing_directory(path, name)
    if owner_home_root is not None:
        require_existing_directory(owner_home_root, "owner-home-root")
    if platform.system() != "Linux" or platform.machine() != "aarch64" or os.geteuid() != 0:
        raise RuntimeError(f"requires Linux aarch64 root, got {platform.system()} {platform.machine()} uid={os.geteuid()}")
    deps = {name: shutil.which(name) for name in ("findmnt",)}
    if not all(deps.values()):
        raise RuntimeError(f"missing dependencies: {deps}")
    mounts = {"owner_root": find_mount(owner_root), "moose_root": find_mount(moose_root)}
    if owner_home_root is not None:
        mounts["owner_home_root"] = find_mount(owner_home_root)
    for name, mount in mounts.items():
        require_fuse_mount(mount, name)
    read_inputs = validate_read_inputs(args, owner_root, moose_root)
    evidence = {
        "platform": {"system": platform.system(), "machine": platform.machine(), "euid": os.geteuid()},
        "dependencies": deps,
        "identity": {
            "owner_root": stat_identity(owner_root),
            "owner_home_root": stat_identity(owner_home_root) if owner_home_root is not None else None,
            "moose_root": stat_identity(moose_root),
        },
        "mounts": mounts,
        "read_inputs": {
            key: str(value) if isinstance(value, Path) else value
            for key, value in (read_inputs or {}).items()
        },
        "external_home_absence": "LOCAL_OWNER_HOME_CHECK" if owner_home_root is not None else "PENDING_PARENT_B_MOUNT_CHECK",
        "output": str(output),
    }
    return owner_root, owner_home_root, moose_root, output, evidence, read_inputs


def run(args: argparse.Namespace) -> dict[str, Any]:
    result: dict[str, Any] = {
        "status": "BLOCKED",
        "scope": "small OwnerFs remote-vs-MooseFS diagnostic; not a performance acceptance pass",
        "cache_state": "unobserved",
        "external_home_absence": "PENDING_PARENT_B_MOUNT_CHECK",
        "delete_shape": {"files": DELETE_FILE_COUNT, "file_bytes": DELETE_FILE_BYTES, "warmups": WARMUP_ROUNDS, "measurements": MEASUREMENT_ROUNDS},
        "read_shape": {"bytes": READ_BYTES, "block_bytes": READ_BLOCK_BYTES, "concurrency": READ_CONCURRENCY, "pattern_byte": READ_PATTERN_BYTE},
        "read_latency_mode": {
            "raw_latency": args.raw_latency,
            "read_cache": args.read_cache,
            "legacy_io_tool_sha256": IO_TOOL_SHA256,
            "raw_latency_io_tool_sha256": RAW_LATENCY_IO_TOOL_SHA256,
            "latency_interval": READ_LATENCY_INTERVAL,
            "latency_order": READ_LATENCY_ORDER,
            "judging_percentile": "p95",
        },
        "case": args.case,
        "driver_sha256": sha256_file(Path(__file__)),
    }
    output: Path | None = None
    try:
        output = require_absolute_path(args.output, "output")
        validate_output_path(output)
        output.mkdir(mode=0o700)
        owner_root, owner_home_root, moose_root, output, preflight_record, read_inputs = preflight(args, output)
        result["preflight"] = preflight_record
        result["external_home_absence"] = preflight_record["external_home_absence"]
        write_json(output / "preflight.json", preflight_record)
        result["status"] = "FAIL"
        require_read_inputs_for_case(args.case, read_inputs)
        rounds: list[dict[str, Any]] = []
        if args.case in ("all", "delete"):
            rounds = run_delete_rounds(owner_root, owner_home_root, moose_root, output)
            result["delete_rounds"] = rounds
            write_json(output / "delete-rounds.json", rounds)
            result["delete_status"] = delete_status(rounds)
        read_rounds: list[dict[str, Any]] = []
        if args.case in ("all", "read"):
            read_rounds = run_read_rounds(read_inputs, output)
            result["read_rounds"] = read_rounds
            write_json(output / "read-rounds.json", read_rounds)
            result["read_status"] = read_status(read_rounds)
        result["status"] = compute_status(rounds, read_rounds, args.case)
    except Exception as error:
        result["error"] = repr(error)
    finally:
        if output is not None and output.exists() and output.is_dir():
            write_json(output / "summary.json", result)
    return result


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--owner-root", required=True)
    parser.add_argument("--owner-home-root")
    parser.add_argument("--moose-root", required=True)
    parser.add_argument("--output", required=True)
    parser.add_argument("--case", choices=("all", "read", "delete"), default="all")
    parser.add_argument("--io-tool")
    parser.add_argument("--owner-read-file")
    parser.add_argument("--moose-read-file")
    parser.add_argument("--raw-latency", action="store_true", help="require samples-enabled IO probe and validate raw latency intervals")
    parser.add_argument("--read-cache", choices=READ_RAW_CACHE_CHOICES, default=READ_DEFAULT_CACHE)
    return parser.parse_args()


def main() -> int:
    result = run(parse_args())
    print(json.dumps(result, indent=2, sort_keys=True))
    return 0 if result.get("status") == "DATA_RECORDED" else 1


if __name__ == "__main__":
    raise SystemExit(main())
