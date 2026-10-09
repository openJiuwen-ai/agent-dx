#!/usr/bin/env python3
"""Small DFS one-writer/many-reader diagnostic helper.

The parent orchestrates writer on one node and readers on other nodes. This tool
only prepares or reads a fixed DFS payload and records evidence; it does not
manage cluster lifecycle and does not qualify DFS against 3FS.
"""
from __future__ import annotations

import argparse
import hashlib
import importlib.util
import json
import os
import select
from pathlib import Path
import platform
import shutil
import stat
import subprocess
import sys
import time
from typing import Any, TextIO

DATA_BYTES = 64 * 1024 * 1024
BLOCK_BYTES = 1024 * 1024
CONCURRENCY = 1
PATTERN_BYTE = 97
IO_TOOL_SHA256 = "70ac97c7634d406a177a74c783d446b62a2014ba198586132882a1d9228e55e8"
PRODUCT_SOURCE_COMMIT = "6d51aeb45c1ed8669d80f612b3817e6d1bdabe04"
COMPILER_INPUT_MAP = "66dbbe3e0071fcec1efc9cf370c709a99f025d39acd4f37ba57582c874429304"
WARMUP_ROUNDS = 1
MEASUREMENT_ROUNDS = 5
PAYLOAD_NAME = "payload64m.bin"
DELETE_FILE_COUNT = 100
DELETE_FILE_BYTES = 4096
OWNER_REMOTE_SMALL_SHA256 = "a28a1bee8092705298a7fcb03311152d574917a0176d6fcbd056d40e10405480"


def sha256_file(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for chunk in iter(lambda: handle.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def expected_payload_sha() -> str:
    digest = hashlib.sha256()
    block = bytes([PATTERN_BYTE]) * BLOCK_BYTES
    for _ in range(DATA_BYTES // BLOCK_BYTES):
        digest.update(block)
    return digest.hexdigest()


def write_json(path: Path, value: Any) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(value, indent=2, sort_keys=True) + "\n", encoding="utf-8")


def read_json(path: Path) -> Any:
    return json.loads(path.read_text(encoding="utf-8"))


def require_absolute_path(value: str, name: str) -> Path:
    path = Path(value)
    if not path.is_absolute():
        raise ValueError(f"{name} must be absolute: {value}")
    return path


def require_existing_directory(path: Path, name: str) -> None:
    if path.is_symlink() or not path.is_dir():
        raise ValueError(f"{name} must be an existing non-symlink directory: {path}")


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


def require_dfs_mount(mount: dict[str, Any], dfs_root: Path) -> None:
    fstype = str(mount.get("fstype", ""))
    source = str(mount.get("source", ""))
    target = str(mount.get("target", ""))
    if not (fstype == "fuse" or fstype.startswith("fuse.")):
        raise ValueError(f"DFS root must be on FUSE, got {fstype}")
    if source != "afs-dfs" or target != str(dfs_root):
        raise ValueError(f"DFS mount must be exact source=afs-dfs target={dfs_root}: {mount}")


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


def validate_io_tool(path: Path) -> dict[str, Any]:
    if path.is_symlink() or not path.is_file():
        raise ValueError(f"io tool must be an existing non-symlink file: {path}")
    digest = sha256_file(path)
    if digest != IO_TOOL_SHA256:
        raise ValueError(f"io tool sha256 mismatch: {digest}")
    return {"path": str(path), "sha256": digest}


def validate_io_result(record: dict[str, Any], operation: str, barrier: str) -> None:
    expected = {
        "operation": operation,
        "file_bytes": DATA_BYTES,
        "io_bytes": DATA_BYTES,
        "block_bytes": BLOCK_BYTES,
        "concurrency": CONCURRENCY,
        "barrier": barrier,
        "pattern_byte": PATTERN_BYTE,
        "operations": DATA_BYTES // BLOCK_BYTES,
        "cache_requested": "unobserved",
        "content_ok": True,
    }
    for key, value in expected.items():
        if record.get(key) != value:
            raise ValueError(f"unexpected io result {key}: {record.get(key)!r}")
    if record.get("residency_observed") is not False:
        raise ValueError("cache residency must remain unobserved")
    if not isinstance(record.get("wall_ns"), int) or record["wall_ns"] <= 0:
        raise ValueError("io result missing positive wall_ns")


def run_io(io_tool: Path, payload: Path, operation: str, barrier: str, mode: str, timeout: float | None = None) -> dict[str, Any]:
    argv = [
        str(io_tool), str(payload), operation, str(DATA_BYTES), str(BLOCK_BYTES), str(CONCURRENCY),
        barrier, str(DATA_BYTES), str(PATTERN_BYTE), mode, "unobserved",
    ]
    began = time.monotonic_ns()
    try:
        completed = subprocess.run(argv, text=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE, timeout=timeout)
        record: dict[str, Any] = {
            "argv": argv,
            "rc": completed.returncode,
            "stdout": completed.stdout,
            "stderr": completed.stderr,
            "elapsed_ns": time.monotonic_ns() - began,
            "status": "FAIL",
        }
    except subprocess.TimeoutExpired as error:
        record = {
            "argv": argv,
            "rc": None,
            "stdout": decode_process_text(error.stdout),
            "stderr": decode_process_text(error.stderr),
            "elapsed_ns": time.monotonic_ns() - began,
            "status": "FAIL",
            "timed_out": True,
            "verify": {"status": "FAIL", "error": f"io timeout after {timeout} seconds"},
            "child_reaped": True,
        }
        return record
    try:
        payload_json = json.loads(completed.stdout)
        record["result"] = payload_json
        if completed.returncode != 0:
            raise ValueError(f"io exited {completed.returncode}")
        validate_io_result(payload_json, operation, barrier)
        record["verify"] = {"status": "PASS"}
        record["status"] = "PASS"
    except Exception as error:
        record["verify"] = {"status": "FAIL", "error": repr(error)}
    return record


def verify_payload(path: Path, expected_sha256: str) -> dict[str, Any]:
    st = path.stat()
    digest = sha256_file(path)
    status = "PASS" if st.st_size == DATA_BYTES and digest == expected_sha256 else "FAIL"
    return {"status": status, "path": str(path), "bytes": st.st_size, "sha256": digest, "expected_sha256": expected_sha256}


def verify_eof(path: Path) -> dict[str, Any]:
    with path.open("rb") as handle:
        handle.seek(DATA_BYTES)
        tail = handle.read(1)
    return {"status": "PASS" if tail == b"" else "FAIL", "offset": DATA_BYTES, "extra_bytes": len(tail)}


def emit_json_line(stream: TextIO, event: dict[str, Any]) -> None:
    stream.write(json.dumps(event, sort_keys=True) + "\n")
    stream.flush()


def decode_process_text(value: str | bytes | None) -> str:
    if value is None:
        return ""
    if isinstance(value, bytes):
        return value.decode("utf-8", errors="replace")
    return value


class JsonLineReader:
    def __init__(self, stream: TextIO):
        self.fd = stream.fileno()
        self.buffer = bytearray()

    def read_line(self, timeout: float) -> str:
        deadline = time.monotonic() + timeout
        while True:
            newline = self.buffer.find(b"\n")
            if newline >= 0:
                raw = bytes(self.buffer[:newline + 1])
                del self.buffer[:newline + 1]
                return raw.decode("utf-8", errors="replace")
            remaining = deadline - time.monotonic()
            if remaining <= 0:
                raise TimeoutError("timeout waiting for complete JSON line")
            ready, _, _ = select.select([self.fd], [], [], remaining)
            if not ready:
                raise TimeoutError("timeout waiting for complete JSON line")
            chunk = os.read(self.fd, 4096)
            if chunk == b"":
                if self.buffer:
                    raise EOFError("EOF after partial JSON line")
                raise EOFError("EOF waiting for JSON line")
            self.buffer.extend(chunk)


def validate_sync_reader_id(reader_id: str) -> str:
    if reader_id not in ("B", "C"):
        raise ValueError("sync reader-id must be B or C")
    return reader_id


def parse_start_event(line: str, expected_round: int, reader_id: str | None = None, session_token: str | None = None) -> dict[str, Any]:
    try:
        event = json.loads(line)
    except json.JSONDecodeError as error:
        raise ValueError(f"malformed START json: {error}") from error
    if not isinstance(event, dict) or event.get("event") != "START":
        raise ValueError("expected START event")
    if reader_id is not None and event.get("reader_id") != reader_id:
        raise ValueError(f"unexpected START reader_id: {event.get('reader_id')!r}")
    if session_token is not None and event.get("session_token") != session_token:
        raise ValueError("unexpected START session_token")
    if event.get("round") != expected_round:
        raise ValueError(f"unexpected START round: {event.get('round')!r}")
    token = event.get("round_token") or event.get("token")
    if not isinstance(token, str) or not token:
        raise ValueError("START round_token must be a nonempty string")
    return {"round": expected_round, "round_token": token, "token": token}


def read_start_event(input_reader: JsonLineReader, expected_round: int, timeout: float, reader_id: str | None = None, session_token: str | None = None) -> dict[str, Any]:
    line = input_reader.read_line(timeout)
    return parse_start_event(line, expected_round, reader_id=reader_id, session_token=session_token)


def parse_ack_event(line: str, expected_round: int, reader_id: str | None = None, session_token: str | None = None, round_token: str | None = None) -> dict[str, Any]:
    try:
        event = json.loads(line)
    except json.JSONDecodeError as error:
        raise ValueError(f"malformed ACK json: {error}") from error
    if not isinstance(event, dict) or event.get("event") != "ACK":
        raise ValueError("expected ACK event")
    if reader_id is not None and event.get("reader_id") != reader_id:
        raise ValueError(f"unexpected ACK reader_id: {event.get('reader_id')!r}")
    if session_token is not None and event.get("session_token") != session_token:
        raise ValueError("unexpected ACK session_token")
    if event.get("round") != expected_round:
        raise ValueError(f"unexpected ACK round: {event.get('round')!r}")
    if round_token is not None and event.get("round_token") != round_token:
        raise ValueError("unexpected ACK round_token")
    return event


def read_ack_event(input_reader: JsonLineReader, expected_round: int, timeout: float, reader_id: str | None = None,
                   session_token: str | None = None, round_token: str | None = None) -> dict[str, Any]:
    line = input_reader.read_line(timeout)
    return parse_ack_event(line, expected_round, reader_id=reader_id, session_token=session_token, round_token=round_token)


def validate_ready_pair(events: list[dict[str, Any]], expected_round: int | None = None, session_token: str | None = None) -> dict[str, Any]:
    if len(events) != 2:
        raise ValueError(f"READY pair must contain exactly two events: {len(events)}")
    readers: dict[str, dict[str, Any]] = {}
    for event in events:
        if not isinstance(event, dict) or event.get("event") != "READY":
            continue
        reader = event.get("reader_id")
        if reader not in ("B", "C"):
            raise ValueError(f"unexpected READY reader: {reader!r}")
        if reader in readers:
            raise ValueError(f"duplicate READY reader: {reader}")
        if expected_round is not None and event.get("round") != expected_round:
            raise ValueError(f"unexpected READY round: {event.get('round')!r}")
        if session_token is not None and event.get("session_token") != session_token:
            raise ValueError("unexpected READY session_token")
        readers[reader] = event
    if set(readers) != {"B", "C"}:
        raise ValueError(f"READY pair must contain B and C exactly: {sorted(readers)}")
    return {"status": "PASS", "readers": sorted(readers)}


def sync_read_status(rounds: list[dict[str, Any]], final_content: dict[str, Any], final_eof: dict[str, Any]) -> str:
    if read_status(rounds) != "DATA_RECORDED":
        return "FAIL"
    measured = [row.get("round") for row in rounds if row.get("measured") is True]
    if measured != list(range(1, MEASUREMENT_ROUNDS + 1)):
        return "FAIL"
    if final_content.get("status") != "PASS" or final_eof.get("status") != "PASS":
        return "FAIL"
    return "DATA_RECORDED"


def load_owner_delete_helper() -> Any:
    helper_path = Path(__file__).with_name("owner_remote_small.py")
    digest = sha256_file(helper_path)
    if digest != OWNER_REMOTE_SMALL_SHA256:
        raise ValueError(f"owner_remote_small.py sha256 mismatch: {digest}")
    spec = importlib.util.spec_from_file_location("afs_owner_remote_small_delete_core", helper_path)
    if spec is None or spec.loader is None:
        raise RuntimeError(f"cannot load owner delete helper: {helper_path}")
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    for name in ("prepare_delete_files", "verify_delete_file_contents", "timed_unlink"):
        if not callable(getattr(module, name, None)):
            raise RuntimeError(f"owner delete helper missing {name}")
    if getattr(module, "DELETE_FILE_COUNT", None) != DELETE_FILE_COUNT or getattr(module, "DELETE_FILE_BYTES", None) != DELETE_FILE_BYTES:
        raise RuntimeError("owner delete helper shape differs")
    return module


def safe_manifest_name(value: Any) -> str:
    if not isinstance(value, str) or value in ("", ".", "..") or value.startswith("/") or "/" in value or ".." in Path(value).parts:
        raise ValueError(f"unsafe sample name: {value!r}")
    return value


def validate_delete_run_id(value: Any) -> str:
    if not isinstance(value, str):
        raise ValueError("delete run_id must be a string")
    parts = value.split("-", 1)
    if len(parts) != 2 or not all(part and part.isdigit() for part in parts):
        raise ValueError(f"delete run_id must match digits-digits: {value!r}")
    return value


def validate_delete_writer_root(manifest: dict[str, Any]) -> str:
    if manifest.get("role") != "delete-writer":
        raise ValueError("delete manifest role must be delete-writer")
    fs = manifest.get("fs")
    if not isinstance(fs, dict):
        raise ValueError("delete manifest missing fs")
    dfs_root_identity = fs.get("dfs_root")
    if not isinstance(dfs_root_identity, dict):
        raise ValueError("delete manifest missing dfs_root identity")
    dfs_root_path = dfs_root_identity.get("path")
    if not isinstance(dfs_root_path, str) or not Path(dfs_root_path).is_absolute() or ".." in Path(dfs_root_path).parts:
        raise ValueError(f"delete manifest dfs_root path must be absolute plain text: {dfs_root_path!r}")
    for key in ("device", "inode", "mode", "uid", "gid"):
        if not isinstance(dfs_root_identity.get(key), int):
            raise ValueError(f"delete manifest dfs_root identity missing integer {key}")
    mount = manifest.get("mount")
    if not isinstance(mount, dict):
        raise ValueError("delete manifest missing mount identity")
    fstype = str(mount.get("fstype", ""))
    if mount.get("source") != "afs-dfs" or mount.get("target") != dfs_root_path or not (fstype == "fuse" or fstype.startswith("fuse.")):
        raise ValueError(f"delete manifest DFS mount identity mismatch: {mount!r}")
    return dfs_root_path


def expected_delete_files() -> list[str]:
    return [f"entry-{index:04d}.bin" for index in range(DELETE_FILE_COUNT)]


def delete_sample_name(run_id: str, round_index: int) -> str:
    return f".afs-dfs-delete-{run_id}-r{round_index:02d}"


def delete_status(rounds: list[dict[str, Any]]) -> str:
    if len(rounds) != WARMUP_ROUNDS + MEASUREMENT_ROUNDS:
        return "FAIL"
    expected_rounds = list(range(WARMUP_ROUNDS + MEASUREMENT_ROUNDS))
    if [row.get("round") for row in rounds] != expected_rounds:
        return "FAIL"
    for round_record in rounds:
        if round_record.get("status") != "PASS" or round_record.get("cleanup", {}).get("status") != "PASS":
            return "FAIL"
    return "DATA_RECORDED"


def run_delete_sample(dfs_root: Path, run_id: str, round_index: int, helper: Any) -> dict[str, Any]:
    sample_name = delete_sample_name(run_id, round_index)
    directory = dfs_root / sample_name
    record: dict[str, Any] = {
        "round": round_index,
        "measured": round_index >= WARMUP_ROUNDS,
        "sample_name": sample_name,
        "directory": str(directory),
        "files": expected_delete_files(),
        "status": "FAIL",
    }
    cleanup: dict[str, Any] = {"status": "NOT_RUN"}
    try:
        if directory.exists() or directory.is_symlink():
            raise FileExistsError(str(directory))
        directory.mkdir(mode=0o700)
        fsync_directory(dfs_root)
        label = f"dfs-delete:r{round_index}"
        record["prepare"] = helper.prepare_delete_files(directory, label)
        record["fresh_open_verify"] = helper.verify_delete_file_contents(directory, label)
        record["unlink_timer"] = helper.timed_unlink(directory)
        fsync_directory(directory)
        remaining = sorted(path.name for path in directory.iterdir())
        record["post_unlink"] = {"status": "PASS" if not remaining else "FAIL", "remaining": remaining, "parent_fsync": True}
        directory.rmdir()
        cleanup = {"status": "PASS", "removed_sample_dir": True}
        checks = [record["fresh_open_verify"], record["unlink_timer"], record["post_unlink"], cleanup]
        record["status"] = "PASS" if all(item.get("status") == "PASS" for item in checks) else "FAIL"
    except Exception as error:
        record["error"] = repr(error)
        if directory.exists():
            cleanup["left_in_place"] = str(directory)
    finally:
        record["cleanup"] = cleanup
    return record


def validate_delete_manifest(manifest: dict[str, Any]) -> list[dict[str, Any]]:
    if manifest.get("status") != "DATA_RECORDED":
        raise ValueError("delete writer manifest is not DATA_RECORDED")
    if manifest.get("product_source_commit") != PRODUCT_SOURCE_COMMIT or manifest.get("compiler_input_map") != COMPILER_INPUT_MAP:
        raise ValueError("delete writer manifest source/map mismatch")
    if manifest.get("source6d") != PRODUCT_SOURCE_COMMIT[:7] or manifest.get("map66") != COMPILER_INPUT_MAP[:8]:
        raise ValueError("delete writer manifest display source/map mismatch")
    run_id = validate_delete_run_id(manifest.get("run_id"))
    dfs_root_path = validate_delete_writer_root(manifest)
    helper = manifest.get("owner_delete_helper")
    if not isinstance(helper, dict) or helper.get("sha256") != OWNER_REMOTE_SMALL_SHA256:
        raise ValueError("delete writer helper identity mismatch")
    shape = manifest.get("delete_shape")
    expected_shape = {"files": DELETE_FILE_COUNT, "file_bytes": DELETE_FILE_BYTES, "warmups": WARMUP_ROUNDS, "measurements": MEASUREMENT_ROUNDS}
    if shape != expected_shape:
        raise ValueError(f"delete shape mismatch: {shape!r}")
    rounds = manifest.get("delete_rounds")
    if not isinstance(rounds, list) or delete_status(rounds) != "DATA_RECORDED":
        raise ValueError("delete rounds incomplete or failed")
    names: set[str] = set()
    for index, sample in enumerate(rounds):
        if sample.get("round") != index or sample.get("measured") != (index >= WARMUP_ROUNDS):
            raise ValueError("delete sample round/measured mismatch")
        name = safe_manifest_name(sample.get("sample_name"))
        expected_name = delete_sample_name(run_id, index)
        if name != expected_name:
            raise ValueError(f"delete sample name mismatch: {name!r} != {expected_name!r}")
        directory = sample.get("directory")
        expected_directory = str(Path(dfs_root_path) / expected_name)
        if directory != expected_directory:
            raise ValueError(f"delete sample directory mismatch: {directory!r} != {expected_directory!r}")
        if name in names:
            raise ValueError("duplicate delete sample name")
        names.add(name)
        if sample.get("files") != expected_delete_files():
            raise ValueError("delete sample file list mismatch")
        if sample.get("status") != "PASS" or sample.get("cleanup", {}).get("status") != "PASS":
            raise ValueError("delete sample did not pass")
        prepare = sample.get("prepare")
        if not isinstance(prepare, dict) or prepare.get("files") != expected_delete_files() or prepare.get("fdatasync_per_file") is not True or prepare.get("parent_fsync") is not True:
            raise ValueError("delete prepare proof mismatch")
        fresh = sample.get("fresh_open_verify")
        if not isinstance(fresh, dict) or fresh.get("status") != "PASS" or fresh.get("files_checked") != DELETE_FILE_COUNT or fresh.get("bytes_per_file") != DELETE_FILE_BYTES:
            raise ValueError("delete fresh content proof mismatch")
        unlink = sample.get("unlink_timer")
        if not isinstance(unlink, dict) or unlink.get("status") != "PASS" or unlink.get("files") != DELETE_FILE_COUNT or not isinstance(unlink.get("wall_ns"), int) or unlink.get("wall_ns") <= 0:
            raise ValueError("delete unlink proof mismatch")
        post = sample.get("post_unlink")
        if not isinstance(post, dict) or post.get("status") != "PASS" or post.get("remaining") != [] or post.get("parent_fsync") is not True:
            raise ValueError("delete post-unlink proof mismatch")
    return rounds


def check_deleted_paths(dfs_root: Path, rounds: list[dict[str, Any]]) -> dict[str, Any]:
    failures: list[dict[str, Any]] = []
    checked = 0
    for sample in rounds:
        sample_name = safe_manifest_name(sample.get("sample_name"))
        for file_name in expected_delete_files():
            path = dfs_root / sample_name / file_name
            try:
                os.lstat(path)
                failures.append({"path": str(path), "status": "PRESENT"})
            except FileNotFoundError as error:
                if error.errno != 2:
                    failures.append({"path": str(path), "status": "WRONG_ERRNO", "errno": error.errno})
            except OSError as error:
                failures.append({"path": str(path), "status": "WRONG_ERRNO", "errno": error.errno, "error": repr(error)})
            checked += 1
    return {"status": "PASS" if not failures else "FAIL", "checked": checked, "expected_errno": "ENOENT", "failures": failures}


def delete_writer(args: argparse.Namespace) -> dict[str, Any]:
    result: dict[str, Any] = {"role": "delete-writer", "status": "BLOCKED", "scope": "small DFS delete diagnostic; candidate correctness/data collection only"}
    output: Path | None = None
    output_created = False
    rounds: list[dict[str, Any]] = []
    try:
        dfs_root = require_absolute_path(args.dfs_root, "dfs-root")
        output = require_absolute_path(args.output, "output")
        validate_output_path(output)
        output.mkdir(mode=0o700)
        output_created = True
        platform_identity = verify_linux_aarch64_root()
        require_existing_directory(dfs_root, "dfs-root")
        mount = find_mount(dfs_root)
        require_dfs_mount(mount, dfs_root)
        helper = load_owner_delete_helper()
        run_id = f"{time.time_ns()}-{os.getpid()}"
        result.update({
            "status": "FAIL",
            "product_source_commit": PRODUCT_SOURCE_COMMIT,
            "source6d": PRODUCT_SOURCE_COMMIT[:7],
            "compiler_input_map": COMPILER_INPUT_MAP,
            "map66": COMPILER_INPUT_MAP[:8],
            "platform": platform_identity,
            "fs": {"dfs_root": stat_identity(dfs_root)},
            "mount": mount,
            "run_id": run_id,
            "delete_shape": {"files": DELETE_FILE_COUNT, "file_bytes": DELETE_FILE_BYTES, "warmups": WARMUP_ROUNDS, "measurements": MEASUREMENT_ROUNDS},
            "owner_delete_helper": {"path": str(Path(__file__).with_name("owner_remote_small.py")), "sha256": OWNER_REMOTE_SMALL_SHA256},
            "timer_scope": "unlink_timer covers exactly 100 unlink syscalls; prepare, fdatasync, fresh-open verification, directory fsync, namespace checks, and cleanup are outside timer",
            "cache_state": "UNOBSERVED",
            "comparison_status": "CANDIDATE_ONLY_3FS_COMPARATOR_NOT_RUN",
        })
        write_json(output / "preflight.json", {k: result[k] for k in ("product_source_commit", "compiler_input_map", "platform", "fs", "mount", "delete_shape", "owner_delete_helper")})
        for index in range(WARMUP_ROUNDS + MEASUREMENT_ROUNDS):
            sample = run_delete_sample(dfs_root, run_id, index, helper)
            sample_path = output / "delete-samples" / f"round-{index:02d}.json"
            write_json(sample_path, sample)
            sample["artifact"] = str(sample_path)
            rounds.append(sample)
            if sample.get("status") != "PASS":
                break
        result["delete_rounds"] = rounds
        write_json(output / "delete-rounds.json", rounds)
        result["status"] = delete_status(rounds)
    except Exception as error:
        result["error"] = repr(error)
        if rounds:
            result["delete_rounds"] = rounds
    finally:
        if output_created and output is not None and output.exists() and output.is_dir():
            write_json(output / "summary.json", result)
            write_json(output / "confirmed.json", result)
    return result


def delete_checker(args: argparse.Namespace) -> dict[str, Any]:
    checker_id = args.checker_id
    result: dict[str, Any] = {"role": "delete-checker", "checker_id": checker_id, "status": "BLOCKED", "scope": "independent DFS mount ENOENT check for delete-writer manifest"}
    output: Path | None = None
    output_created = False
    try:
        dfs_root = require_absolute_path(args.dfs_root, "dfs-root")
        manifest_path = require_absolute_path(args.manifest, "manifest")
        output = require_absolute_path(args.output, "output")
        validate_output_path(output)
        output.mkdir(mode=0o700)
        output_created = True
        platform_identity = verify_linux_aarch64_root()
        require_existing_directory(dfs_root, "dfs-root")
        if manifest_path.is_symlink() or not manifest_path.is_file():
            raise ValueError(f"manifest must be an existing non-symlink file: {manifest_path}")
        mount = find_mount(dfs_root)
        require_dfs_mount(mount, dfs_root)
        manifest = read_json(manifest_path)
        rounds = validate_delete_manifest(manifest)
        check = check_deleted_paths(dfs_root, rounds)
        write_json(output / "enoent-check.json", check)
        result.update({
            "status": "DATA_RECORDED" if check.get("status") == "PASS" else "FAIL",
            "product_source_commit": PRODUCT_SOURCE_COMMIT,
            "source6d": PRODUCT_SOURCE_COMMIT[:7],
            "compiler_input_map": COMPILER_INPUT_MAP,
            "map66": COMPILER_INPUT_MAP[:8],
            "platform": platform_identity,
            "fs": {"dfs_root": stat_identity(dfs_root)},
            "mount": mount,
            "manifest": {"path": str(manifest_path), "run_id": manifest.get("run_id"), "delete_shape": manifest.get("delete_shape")},
            "enoent_check": check,
        })
    except Exception as error:
        result["error"] = repr(error)
    finally:
        if output_created and output is not None and output.exists() and output.is_dir():
            write_json(output / "summary.json", result)
    return result


def writer_directory_name() -> str:
    return f"dfs-manyread-small-{int(time.time_ns())}-{os.getpid()}"


def writer(args: argparse.Namespace) -> dict[str, Any]:
    result: dict[str, Any] = {"role": "writer", "status": "BLOCKED", "scope": "small DFS one-write/many-read diagnostic"}
    output: Path | None = None
    output_created = False
    try:
        dfs_root = require_absolute_path(args.dfs_root, "dfs-root")
        io_tool = require_absolute_path(args.io_tool, "io-tool")
        output = require_absolute_path(args.output, "output")
        validate_output_path(output)
        output.mkdir(mode=0o700)
        output_created = True
        platform_identity = verify_linux_aarch64_root()
        require_existing_directory(dfs_root, "dfs-root")
        mount = find_mount(dfs_root)
        require_dfs_mount(mount, dfs_root)
        io_identity = validate_io_tool(io_tool)
        sample_dir = dfs_root / writer_directory_name()
        payload = sample_dir / PAYLOAD_NAME
        if sample_dir.exists() or payload.exists():
            raise FileExistsError(str(sample_dir))
        sample_dir.mkdir(mode=0o700)
        fsync_directory(dfs_root)
        if payload.exists():
            raise FileExistsError(str(payload))
        write_record = run_io(io_tool, payload, "seq-write", "fdatasync", "create")
        write_json(output / "write-command.json", write_record)
        fsync_directory(sample_dir)
        expected_sha = expected_payload_sha()
        content = verify_payload(payload, expected_sha)
        result.update({
            "status": "DATA_RECORDED" if write_record.get("status") == "PASS" and content.get("status") == "PASS" else "FAIL",
            "product_source_commit": PRODUCT_SOURCE_COMMIT,
            "source6d": PRODUCT_SOURCE_COMMIT[:7],
            "compiler_input_map": COMPILER_INPUT_MAP,
            "map66": COMPILER_INPUT_MAP[:8],
            "io_tool": io_identity,
            "fs": {"dfs_root": stat_identity(dfs_root), "sample_dir": stat_identity(sample_dir), "payload": stat_identity(payload)},
            "mount": mount,
            "platform": platform_identity,
            "payload": {"relative_dir": sample_dir.name, "name": PAYLOAD_NAME, "bytes": DATA_BYTES, "pattern_byte": PATTERN_BYTE, "sha256": expected_sha},
            "write": write_record,
            "content_verify": content,
            "dir_fsync": True,
            "parent_dir_fsync": True,
        })
    except Exception as error:
        result["error"] = repr(error)
    finally:
        if output_created and output is not None and output.exists() and output.is_dir():
            write_json(output / "confirmed.json", result)
            write_json(output / "summary.json", result)
    return result


def validate_manifest_shape(manifest: dict[str, Any]) -> dict[str, Any]:
    if manifest.get("status") != "DATA_RECORDED":
        raise ValueError("writer manifest is not DATA_RECORDED")
    if manifest.get("product_source_commit") != PRODUCT_SOURCE_COMMIT:
        raise ValueError("writer manifest full source identity mismatch")
    if manifest.get("compiler_input_map") != COMPILER_INPUT_MAP:
        raise ValueError("writer manifest full compiler input map mismatch")
    if manifest.get("source6d") != PRODUCT_SOURCE_COMMIT[:7] or manifest.get("map66") != COMPILER_INPUT_MAP[:8]:
        raise ValueError("writer manifest display source/map identity mismatch")
    io_tool = manifest.get("io_tool")
    if not isinstance(io_tool, dict) or io_tool.get("sha256") != IO_TOOL_SHA256:
        raise ValueError("writer manifest io tool identity mismatch")
    payload = manifest.get("payload")
    if not isinstance(payload, dict):
        raise ValueError("writer manifest missing payload")
    if payload.get("name") != PAYLOAD_NAME or payload.get("bytes") != DATA_BYTES or payload.get("pattern_byte") != PATTERN_BYTE:
        raise ValueError("writer manifest payload shape mismatch")
    rel_dir = payload.get("relative_dir")
    digest = payload.get("sha256")
    if not isinstance(rel_dir, str) or rel_dir.startswith("/") or "/" in rel_dir or rel_dir in ("", ".", ".."):
        raise ValueError("writer manifest relative_dir must be one safe path component")
    if digest != expected_payload_sha():
        raise ValueError("writer manifest payload sha mismatch")
    write = manifest.get("write")
    if not isinstance(write, dict) or write.get("rc") != 0 or write.get("status") != "PASS":
        raise ValueError("writer manifest write command did not pass")
    result = write.get("result")
    if not isinstance(result, dict):
        raise ValueError("writer manifest missing write C result")
    validate_io_result(result, "seq-write", "fdatasync")
    verify = write.get("verify")
    if not isinstance(verify, dict) or verify.get("status") != "PASS":
        raise ValueError("writer manifest write verify did not pass")
    content = manifest.get("content_verify")
    if not isinstance(content, dict) or content.get("status") != "PASS" or content.get("bytes") != DATA_BYTES or content.get("sha256") != digest:
        raise ValueError("writer manifest content verification mismatch")
    if manifest.get("dir_fsync") is not True or manifest.get("parent_dir_fsync") is not True:
        raise ValueError("writer manifest missing directory fsync proof")
    return payload


def read_status(rounds: list[dict[str, Any]]) -> str:
    if len(rounds) != WARMUP_ROUNDS + MEASUREMENT_ROUNDS:
        return "FAIL"
    for round_record in rounds:
        samples = round_record.get("samples")
        if not isinstance(samples, list) or len(samples) != 1:
            return "FAIL"
        sample = samples[0]
        if sample.get("status") != "PASS" or sample.get("verify", {}).get("status") != "PASS":
            return "FAIL"
    return "DATA_RECORDED"


def reader_rounds(io_tool: Path, payload: Path, output: Path) -> list[dict[str, Any]]:
    rounds: list[dict[str, Any]] = []
    for index in range(WARMUP_ROUNDS + MEASUREMENT_ROUNDS):
        sample = run_io(io_tool, payload, "seq-read", "close", "existing")
        sample["round"] = index
        sample["measured"] = index >= WARMUP_ROUNDS
        sample_path = output / "read-samples" / f"round-{index:02d}.json"
        write_json(sample_path, sample)
        sample["artifact"] = str(sample_path)
        round_record = {"round": index, "measured": index >= WARMUP_ROUNDS, "samples": [sample]}
        write_json(output / f"read-round-{index:02d}.json", round_record)
        rounds.append(round_record)
    return rounds


def sync_reader(args: argparse.Namespace) -> dict[str, Any]:
    reader_id = validate_sync_reader_id(args.reader_id)
    timeout = args.round_timeout
    session_token = args.session_token
    result: dict[str, Any] = {"role": "reader", "reader_id": reader_id, "status": "BLOCKED", "sync_stdio": True,
                              "session_token": session_token,
                              "scope": "small DFS synchronized many-reader diagnostic; parent owns common window"}
    output: Path | None = None
    output_created = False
    rounds: list[dict[str, Any]] = []
    emit_json_line(sys.stdout, {"event": "HELLO", "reader_id": reader_id, "session_token": session_token,
                                "protocol": "afs.dfs_manyread.sync_stdio.v1", "rounds": MEASUREMENT_ROUNDS})
    control_reader = JsonLineReader(sys.stdin)
    try:
        read_ack_event(control_reader, 0, timeout, reader_id=reader_id, session_token=session_token)
        dfs_root = require_absolute_path(args.dfs_root, "dfs-root")
        io_tool = require_absolute_path(args.io_tool, "io-tool")
        manifest_path = require_absolute_path(args.manifest, "manifest")
        output = require_absolute_path(args.output, "output")
        validate_output_path(output)
        output.mkdir(mode=0o700)
        output_created = True
        platform_identity = verify_linux_aarch64_root()
        require_existing_directory(dfs_root, "dfs-root")
        if manifest_path.is_symlink() or not manifest_path.is_file():
            raise ValueError(f"manifest must be an existing non-symlink file: {manifest_path}")
        manifest = read_json(manifest_path)
        payload_shape = validate_manifest_shape(manifest)
        mount = find_mount(dfs_root)
        require_dfs_mount(mount, dfs_root)
        io_identity = validate_io_tool(io_tool)
        payload = dfs_root / payload_shape["relative_dir"] / PAYLOAD_NAME
        pre_content = verify_payload(payload, payload_shape["sha256"])
        write_json(output / "manifest-precheck.json", {"manifest": str(manifest_path), "content_verify": pre_content})
        result.update({
            "platform": platform_identity,
            "manifest": {"path": str(manifest_path), "payload": payload_shape, "product_source_commit": manifest.get("product_source_commit"), "compiler_input_map": manifest.get("compiler_input_map"), "source6d": manifest.get("source6d"), "map66": manifest.get("map66")},
            "io_tool": io_identity,
            "fs": {"dfs_root": stat_identity(dfs_root), "payload": stat_identity(payload)},
            "mount": mount,
            "content_verify": pre_content,
            "timer_scope": "READY is emitted per round after manifest/content precheck and warmup; START..DONE rounds 1..5 are parent common-window inputs; final content/EOF check is after the measured rounds.",
        })
        if pre_content.get("status") != "PASS":
            raise ValueError("reader payload precheck failed")
        warmup = run_io(io_tool, payload, "seq-read", "close", "existing", timeout=timeout)
        warmup["round"] = 0
        warmup["measured"] = False
        warmup_path = output / "read-samples" / "round-00.json"
        write_json(warmup_path, warmup)
        warmup["artifact"] = str(warmup_path)
        warmup_round = {"round": 0, "measured": False, "samples": [warmup]}
        write_json(output / "read-round-00.json", warmup_round)
        rounds.append(warmup_round)
        if warmup.get("status") != "PASS" or warmup.get("verify", {}).get("status") != "PASS":
            raise ValueError("reader warmup failed")
        for index in range(1, MEASUREMENT_ROUNDS + 1):
            emit_json_line(sys.stdout, {"event": "READY", "reader_id": reader_id, "session_token": session_token,
                                        "round": index, "payload_sha256": payload_shape["sha256"], "output": str(output)})
            start = read_start_event(control_reader, index, timeout, reader_id=reader_id, session_token=session_token)
            sample = run_io(io_tool, payload, "seq-read", "close", "existing", timeout=timeout)
            sample["round"] = index
            sample["measured"] = True
            sample["round_token"] = start["round_token"]
            sample_path = output / "read-samples" / f"round-{index:02d}.json"
            write_json(sample_path, sample)
            sample["artifact"] = str(sample_path)
            round_record = {"round": index, "measured": True, "round_token": start["round_token"], "samples": [sample]}
            write_json(output / f"read-round-{index:02d}.json", round_record)
            rounds.append(round_record)
            done = {"event": "DONE", "reader_id": reader_id, "session_token": session_token, "round": index,
                    "round_token": start["round_token"], "status": sample.get("status"), "rc": sample.get("rc"),
                    "result": sample.get("result"), "artifact": str(sample_path)}
            emit_json_line(sys.stdout, done)
            if sample.get("status") != "PASS" or sample.get("verify", {}).get("status") != "PASS":
                raise RuntimeError(f"reader round {index} failed")
            read_ack_event(control_reader, index, timeout, reader_id=reader_id, session_token=session_token,
                           round_token=start["round_token"])
        final_content = verify_payload(payload, payload_shape["sha256"])
        final_eof = verify_eof(payload)
        result["read_rounds"] = rounds
        result["final_content_verify"] = final_content
        result["final_eof_verify"] = final_eof
        write_json(output / "read-rounds.json", rounds)
        result["status"] = sync_read_status(rounds, final_content, final_eof)
    except Exception as error:
        result["error"] = repr(error)
    finally:
        if output_created and output is not None and output.exists() and output.is_dir():
            write_json(output / "summary.json", result)
        emit_json_line(sys.stdout, {"event": "FINAL", "reader_id": reader_id, "session_token": session_token,
                                    "status": result.get("status"), "error": result.get("error"),
                                    "output": str(output) if output is not None else None})
    return result


def reader(args: argparse.Namespace) -> dict[str, Any]:
    result: dict[str, Any] = {"role": "reader", "status": "BLOCKED", "scope": "small DFS many-reader diagnostic; parent owns concurrency timing"}
    output: Path | None = None
    output_created = False
    try:
        dfs_root = require_absolute_path(args.dfs_root, "dfs-root")
        io_tool = require_absolute_path(args.io_tool, "io-tool")
        manifest_path = require_absolute_path(args.manifest, "manifest")
        output = require_absolute_path(args.output, "output")
        validate_output_path(output)
        output.mkdir(mode=0o700)
        output_created = True
        platform_identity = verify_linux_aarch64_root()
        require_existing_directory(dfs_root, "dfs-root")
        if manifest_path.is_symlink() or not manifest_path.is_file():
            raise ValueError(f"manifest must be an existing non-symlink file: {manifest_path}")
        manifest = read_json(manifest_path)
        payload_shape = validate_manifest_shape(manifest)
        mount = find_mount(dfs_root)
        require_dfs_mount(mount, dfs_root)
        io_identity = validate_io_tool(io_tool)
        payload = dfs_root / payload_shape["relative_dir"] / PAYLOAD_NAME
        content = verify_payload(payload, payload_shape["sha256"])
        write_json(output / "manifest-precheck.json", {"manifest": str(manifest_path), "content_verify": content})
        result.update({
            "platform": platform_identity,
            "manifest": {"path": str(manifest_path), "payload": payload_shape, "product_source_commit": manifest.get("product_source_commit"), "compiler_input_map": manifest.get("compiler_input_map"), "source6d": manifest.get("source6d"), "map66": manifest.get("map66")},
            "io_tool": io_identity,
            "fs": {"dfs_root": stat_identity(dfs_root), "payload": stat_identity(payload)},
            "mount": mount,
            "content_verify": content,
        })
        if content.get("status") != "PASS":
            raise ValueError("reader payload precheck failed")
        rounds = reader_rounds(io_tool, payload, output)
        result["read_rounds"] = rounds
        write_json(output / "read-rounds.json", rounds)
        result["status"] = read_status(rounds)
    except Exception as error:
        result["error"] = repr(error)
    finally:
        if output_created and output is not None and output.exists() and output.is_dir():
            write_json(output / "summary.json", result)
    return result


def read_json_line(input_reader: JsonLineReader, timeout: float) -> dict[str, Any]:
    line = input_reader.read_line(timeout)
    try:
        event = json.loads(line)
    except json.JSONDecodeError as error:
        raise ValueError(f"malformed routed json: {error}") from error
    if not isinstance(event, dict):
        raise ValueError("routed event must be an object")
    return event


def require_routed_event(event: dict[str, Any], expected_event: str, readers: set[str], session_token: str,
                         seen: set[str] | None = None, expected_round: int | None = None,
                         expected_token: str | None = None) -> dict[str, Any]:
    if event.get("event") != expected_event:
        raise ValueError(f"expected {expected_event}, got {event.get('event')!r}")
    reader = event.get("reader_id")
    if reader not in readers:
        raise ValueError(f"unexpected reader_id: {reader!r}")
    if seen is not None:
        if reader in seen:
            raise ValueError(f"duplicate {expected_event} from {reader}")
        seen.add(reader)
    if event.get("session_token") != session_token:
        raise ValueError("unexpected session_token")
    if expected_round is not None and event.get("round") != expected_round:
        raise ValueError(f"unexpected round: {event.get('round')!r}")
    if expected_token is not None and event.get("round_token") != expected_token:
        raise ValueError("unexpected round_token")
    return event


def validate_done_event_io(done: dict[str, Any]) -> None:
    if done.get("status") != "PASS":
        raise RuntimeError(f"reader {done.get('reader_id')} round {done.get('round')} status {done.get('status')}")
    if done.get("rc") != 0:
        raise RuntimeError(f"reader {done.get('reader_id')} round {done.get('round')} rc {done.get('rc')}")
    result = done.get("result")
    if not isinstance(result, dict):
        raise RuntimeError(f"reader {done.get('reader_id')} round {done.get('round')} missing C result")
    try:
        validate_io_result(result, "seq-read", "close")
    except Exception as error:
        raise RuntimeError(f"reader {done.get('reader_id')} round {done.get('round')} invalid C result: {error}") from error


def coordinator(args: argparse.Namespace) -> dict[str, Any]:
    output = require_absolute_path(args.output, "output")
    validate_output_path(output)
    output.mkdir(mode=0o700)
    readers = set(args.readers)
    result: dict[str, Any] = {"role": "coordinator", "status": "BLOCKED", "session_token": args.session_token,
                              "readers": sorted(readers), "round_timeout": args.round_timeout,
                              "timer_scope": "ctl Linux monotonic_ns; per-round elapsed starts before START emit and ends after both DONE events, including host relay/Lima control delay plus reader result write/emit overhead."}
    raw_events: list[dict[str, Any]] = []
    rounds: list[dict[str, Any]] = []
    try:
        platform_identity = verify_linux_aarch64_root()
        result["platform"] = platform_identity
        result["monotonic_clock"] = {k: getattr(time.get_clock_info("monotonic"), k) for k in ("implementation", "monotonic", "adjustable", "resolution")}
        if readers != {"B", "C"}:
            raise ValueError("coordinator readers must be B and C")
        input_reader = JsonLineReader(sys.stdin)
        hello_seen: set[str] = set()
        while hello_seen != readers:
            event = read_json_line(input_reader, args.round_timeout)
            raw_events.append(event)
            require_routed_event(event, "HELLO", readers, args.session_token, seen=hello_seen)
        for reader in args.readers:
            emit_json_line(sys.stdout, {"event": "ACK", "reader_id": reader, "session_token": args.session_token,
                                        "round": 0, "next_round": 1, "rounds": MEASUREMENT_ROUNDS})
        for round_index in range(1, MEASUREMENT_ROUNDS + 1):
            ready_seen: set[str] = set()
            ready_events: list[dict[str, Any]] = []
            while ready_seen != readers:
                event = read_json_line(input_reader, args.round_timeout)
                raw_events.append(event)
                ready_events.append(require_routed_event(event, "READY", readers, args.session_token, seen=ready_seen, expected_round=round_index))
            round_token = f"{args.session_token}:round:{round_index}"
            start_before_emit_ns = time.monotonic_ns()
            for reader in args.readers:
                emit_json_line(sys.stdout, {"event": "START", "reader_id": reader, "session_token": args.session_token,
                                            "round": round_index, "round_token": round_token})
            done_seen: set[str] = set()
            done_events: list[dict[str, Any]] = []
            while done_seen != readers:
                event = read_json_line(input_reader, args.round_timeout)
                raw_events.append(event)
                done = require_routed_event(event, "DONE", readers, args.session_token, seen=done_seen,
                                            expected_round=round_index, expected_token=round_token)
                validate_done_event_io(done)
                done_events.append(done)
            end_after_both_done_ns = time.monotonic_ns()
            row = {"round": round_index, "round_token": round_token, "ready": ready_events, "done": done_events,
                   "start_before_emit_ns": start_before_emit_ns, "end_after_both_done_ns": end_after_both_done_ns,
                   "elapsed_ns": end_after_both_done_ns - start_before_emit_ns}
            write_json(output / f"coordinator-round-{round_index:02d}.json", row)
            rounds.append(row)
            for reader in args.readers:
                emit_json_line(sys.stdout, {"event": "ACK", "reader_id": reader, "session_token": args.session_token,
                                            "round": round_index, "round_token": round_token,
                                            "next_round": round_index + 1 if round_index < MEASUREMENT_ROUNDS else None})
        final_seen: set[str] = set()
        final_events: list[dict[str, Any]] = []
        while final_seen != readers:
            event = read_json_line(input_reader, args.round_timeout)
            raw_events.append(event)
            final = require_routed_event(event, "FINAL", readers, args.session_token, seen=final_seen)
            final_events.append(final)
            if final.get("status") != "DATA_RECORDED":
                raise RuntimeError(f"reader {final.get('reader_id')} final status {final.get('status')}")
        result.update({"status": "DATA_RECORDED", "rounds": rounds, "final": final_events})
    except Exception as error:
        result["error"] = repr(error)
        result["rounds"] = rounds
    finally:
        write_json(output / "coordinator-events.json", raw_events)
        write_json(output / "summary.json", result)
    return result


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    sub = parser.add_subparsers(dest="role", required=True)
    for name in ("writer", "reader"):
        item = sub.add_parser(name)
        item.add_argument("--dfs-root", required=True)
        item.add_argument("--io-tool", required=True)
        item.add_argument("--output", required=True)
    delete_writer_parser = sub.add_parser("delete-writer")
    delete_writer_parser.add_argument("--dfs-root", required=True)
    delete_writer_parser.add_argument("--output", required=True)
    delete_checker_parser = sub.add_parser("delete-checker")
    delete_checker_parser.add_argument("--dfs-root", required=True)
    delete_checker_parser.add_argument("--manifest", required=True)
    delete_checker_parser.add_argument("--output", required=True)
    delete_checker_parser.add_argument("--checker-id", choices=("B", "C"), required=True)
    coordinator_parser = sub.add_parser("coordinator")
    coordinator_parser.add_argument("--output", required=True)
    coordinator_parser.add_argument("--session-token", required=True)
    coordinator_parser.add_argument("--round-timeout", type=float, default=30.0)
    coordinator_parser.add_argument("--readers", nargs=2, default=["B", "C"])
    sub.choices["reader"].add_argument("--manifest", required=True)
    sub.choices["reader"].add_argument("--sync-stdio", action="store_true", help="emit HELLO/READY/DONE/FINAL JSON lines and wait for START JSON lines")
    sub.choices["reader"].add_argument("--reader-id", choices=("B", "C"), default="B")
    sub.choices["reader"].add_argument("--session-token", default="")
    sub.choices["reader"].add_argument("--round-timeout", type=float, default=30.0)
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    if args.role == "writer":
        result = writer(args)
        print(json.dumps(result, indent=2, sort_keys=True))
    elif args.role == "delete-writer":
        result = delete_writer(args)
        print(json.dumps(result, indent=2, sort_keys=True))
    elif args.role == "delete-checker":
        result = delete_checker(args)
        print(json.dumps(result, indent=2, sort_keys=True))
    elif args.role == "coordinator":
        result = coordinator(args)
        print(json.dumps(result, indent=2, sort_keys=True), file=sys.stderr)
    elif args.sync_stdio:
        result = sync_reader(args)
    else:
        result = reader(args)
        print(json.dumps(result, indent=2, sort_keys=True))
    return 0 if result.get("status") == "DATA_RECORDED" else 1


if __name__ == "__main__":
    raise SystemExit(main())
