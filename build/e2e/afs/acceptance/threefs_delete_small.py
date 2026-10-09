#!/usr/bin/env python3
"""Small 3FS namespace delete diagnostic adapter.

This first-party adapter reuses the fixed Owner delete primitives for an hf3fs
mount. It records same-shape 100x4KiB unlink evidence only; the parent owns the
3FS lifecycle, cross-node orchestration, and comparative report.
"""
from __future__ import annotations

import argparse
import hashlib
import importlib.util
import json
import os
from pathlib import Path
import platform
import stat
import subprocess
import sys
import time
from typing import Any

DELETE_FILE_COUNT = 100
DELETE_FILE_BYTES = 4096
WARMUP_ROUNDS = 1
MEASUREMENT_ROUNDS = 5
THREEFS_UPSTREAM_COMMIT = "22fca04564c7cc230fd8b9523b8b92864e1dad47"
THREEFS_ARM64_PATCH_SHA256 = "6c460875c6c098e0b4aacad309e099657a5daf70bdc3937060d402a5e59aad95"
OWNER_REMOTE_SMALL_SHA256 = "a28a1bee8092705298a7fcb03311152d574917a0176d6fcbd056d40e10405480"
EXPECTED_HF3FS_SOURCE = "hf3fs.afs_3fs_delete_v84_20261007_r1"
EXPECTED_MOUNT_SUFFIX = "/afs-delivery/threefs-delete-v84-20261007-r1/mount"
EXPECTED_ROOT_RELATIVE = "test"


def sha256_file(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for chunk in iter(lambda: handle.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def write_json(path: Path, value: Any) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(value, indent=2, sort_keys=True) + "\n", encoding="utf-8")


def read_json(path: Path) -> Any:
    return json.loads(path.read_text(encoding="utf-8"))


def require_absolute_path(value: str, name: str) -> Path:
    path = Path(value)
    if not path.is_absolute() or ".." in path.parts:
        raise ValueError(f"{name} must be absolute without literal '..': {value}")
    return path


def reject_symlink_ancestors(path: Path, name: str) -> None:
    current = Path(path.anchor)
    for part in path.parts[1:]:
        current = current / part
        if current.is_symlink():
            raise ValueError(f"{name} must not include symlink ancestors: {current}")


def require_existing_directory(path: Path, name: str) -> None:
    reject_symlink_ancestors(path, name)
    if path.is_symlink() or not path.is_dir():
        raise ValueError(f"{name} must be an existing non-symlink directory: {path}")


def validate_output_path(path: Path) -> None:
    if path.is_symlink() or path.exists():
        raise ValueError(f"output must be fresh and non-symlink: {path}")
    if not path.parent.is_dir():
        raise ValueError(f"output parent must exist: {path.parent}")


def verify_linux_aarch64_root() -> dict[str, Any]:
    identity = {"system": platform.system(), "machine": platform.machine(), "euid": os.geteuid()}
    if identity != {"system": "Linux", "machine": "aarch64", "euid": 0}:
        raise RuntimeError(f"requires Linux/aarch64/root: {identity}")
    return identity


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


def mount_covers_path(mount_target: str, path: Path) -> bool:
    try:
        path.relative_to(Path(mount_target))
    except ValueError:
        return False
    return True


def mount_target_matches_fixture(mount_target: str) -> bool:
    return mount_target.endswith(EXPECTED_MOUNT_SUFFIX)


def mount_relative_root(mount_target: str, path: Path) -> str:
    try:
        relative = path.relative_to(Path(mount_target))
    except ValueError as error:
        raise ValueError(f"root is not covered by mount target {mount_target}: {path}") from error
    return "." if str(relative) == "." else relative.as_posix()


def require_expected_relative_root(mount_target: str, path: Path) -> None:
    relative = mount_relative_root(mount_target, path)
    if relative != EXPECTED_ROOT_RELATIVE:
        raise ValueError(f"3FS delete root must be mount-relative {EXPECTED_ROOT_RELATIVE!r}, got {relative!r}")


def require_hf3fs_mount(mount: dict[str, Any], threefs_root: Path) -> None:
    fstype = str(mount.get("fstype", ""))
    source = str(mount.get("source", ""))
    target = str(mount.get("target", ""))
    if not (fstype == "fuse.hf3fs" or fstype.startswith("fuse.hf3fs")):
        raise ValueError(f"3FS root must be on fuse.hf3fs, got {fstype}")
    if source != EXPECTED_HF3FS_SOURCE or not mount_target_matches_fixture(target) or not mount_covers_path(target, threefs_root):
        raise ValueError(f"3FS mount must be exact source={EXPECTED_HF3FS_SOURCE} fixture target suffix={EXPECTED_MOUNT_SUFFIX} covering {threefs_root}: {mount}")
    require_expected_relative_root(target, threefs_root)


def fsync_directory(path: Path) -> None:
    descriptor = os.open(path, os.O_RDONLY | getattr(os, "O_DIRECTORY", 0))
    try:
        os.fsync(descriptor)
    finally:
        os.close(descriptor)


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


def validate_run_id(value: Any) -> str:
    if not isinstance(value, str):
        raise ValueError("run_id must be a string")
    parts = value.split("-", 1)
    if len(parts) != 2 or not all(part and part.isdigit() for part in parts):
        raise ValueError(f"run_id must match digits-digits: {value!r}")
    return value


def expected_delete_files() -> list[str]:
    return [f"entry-{index:04d}.bin" for index in range(DELETE_FILE_COUNT)]


def sample_name(run_id: str, round_index: int) -> str:
    return f".afs-threefs-delete-{run_id}-r{round_index:02d}"


def delete_shape() -> dict[str, int]:
    return {"files": DELETE_FILE_COUNT, "file_bytes": DELETE_FILE_BYTES, "warmups": WARMUP_ROUNDS, "measurements": MEASUREMENT_ROUNDS}


def delete_status(rounds: list[dict[str, Any]]) -> str:
    if len(rounds) != WARMUP_ROUNDS + MEASUREMENT_ROUNDS:
        return "FAIL"
    if [row.get("round") for row in rounds] != list(range(WARMUP_ROUNDS + MEASUREMENT_ROUNDS)):
        return "FAIL"
    for row in rounds:
        if row.get("status") != "PASS" or row.get("cleanup", {}).get("status") != "PASS":
            return "FAIL"
    return "DATA_RECORDED"


def run_delete_sample(threefs_root: Path, run_id: str, round_index: int, helper: Any) -> dict[str, Any]:
    name = sample_name(run_id, round_index)
    directory = threefs_root / name
    record: dict[str, Any] = {
        "round": round_index,
        "measured": round_index >= WARMUP_ROUNDS,
        "sample_name": name,
        "directory": str(directory),
        "files": expected_delete_files(),
        "status": "FAIL",
    }
    cleanup: dict[str, Any] = {"status": "NOT_RUN"}
    try:
        if directory.exists() or directory.is_symlink():
            raise FileExistsError(str(directory))
        directory.mkdir(mode=0o700)
        fsync_directory(threefs_root)
        label = f"threefs-delete:r{round_index}"
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


def validate_writer_root(manifest: dict[str, Any]) -> str:
    fs = manifest.get("fs")
    if not isinstance(fs, dict):
        raise ValueError("manifest missing fs")
    root_identity = fs.get("threefs_root")
    if not isinstance(root_identity, dict):
        raise ValueError("manifest missing threefs_root identity")
    root_path = root_identity.get("path")
    if not isinstance(root_path, str) or not Path(root_path).is_absolute() or ".." in Path(root_path).parts:
        raise ValueError(f"manifest threefs_root path must be absolute plain text: {root_path!r}")
    for key in ("device", "inode", "mode", "uid", "gid"):
        if not isinstance(root_identity.get(key), int):
            raise ValueError(f"manifest threefs_root identity missing integer {key}")
    mount = manifest.get("mount")
    if not isinstance(mount, dict):
        raise ValueError("manifest missing mount identity")
    fstype = str(mount.get("fstype", ""))
    target = str(mount.get("target", ""))
    if mount.get("source") != EXPECTED_HF3FS_SOURCE or not mount_target_matches_fixture(target) or not mount_covers_path(target, Path(root_path)) or not (fstype == "fuse.hf3fs" or fstype.startswith("fuse.hf3fs")):
        raise ValueError(f"manifest hf3fs mount identity mismatch: {mount!r}")
    require_expected_relative_root(target, Path(root_path))
    return root_path


def validate_manifest(manifest: dict[str, Any]) -> list[dict[str, Any]]:
    if manifest.get("role") != "threefs-delete-writer":
        raise ValueError("manifest role must be threefs-delete-writer")
    if manifest.get("status") != "DATA_RECORDED":
        raise ValueError("writer manifest is not DATA_RECORDED")
    if manifest.get("threefs_upstream_commit") != THREEFS_UPSTREAM_COMMIT:
        raise ValueError("3FS upstream identity mismatch")
    if manifest.get("threefs_arm64_patch_sha256") != THREEFS_ARM64_PATCH_SHA256:
        raise ValueError("3FS ARM64 patch identity mismatch")
    run_id = validate_run_id(manifest.get("run_id"))
    root_path = validate_writer_root(manifest)
    helper = manifest.get("owner_delete_helper")
    if not isinstance(helper, dict) or helper.get("sha256") != OWNER_REMOTE_SMALL_SHA256:
        raise ValueError("owner helper identity mismatch")
    if manifest.get("delete_shape") != delete_shape():
        raise ValueError(f"delete shape mismatch: {manifest.get('delete_shape')!r}")
    rounds = manifest.get("delete_rounds")
    if not isinstance(rounds, list) or delete_status(rounds) != "DATA_RECORDED":
        raise ValueError("delete rounds incomplete or failed")
    names: set[str] = set()
    for index, sample in enumerate(rounds):
        if sample.get("round") != index or sample.get("measured") != (index >= WARMUP_ROUNDS):
            raise ValueError("sample round/measured mismatch")
        name = safe_manifest_name(sample.get("sample_name"))
        expected_name = sample_name(run_id, index)
        if name != expected_name:
            raise ValueError(f"sample name mismatch: {name!r} != {expected_name!r}")
        expected_directory = str(Path(root_path) / expected_name)
        if sample.get("directory") != expected_directory:
            raise ValueError(f"sample directory mismatch: {sample.get('directory')!r} != {expected_directory!r}")
        if name in names:
            raise ValueError("duplicate sample name")
        names.add(name)
        if sample.get("files") != expected_delete_files():
            raise ValueError("sample file list mismatch")
        if sample.get("status") != "PASS" or sample.get("cleanup", {}).get("status") != "PASS":
            raise ValueError("sample did not pass")
        prepare = sample.get("prepare")
        if not isinstance(prepare, dict) or prepare.get("files") != expected_delete_files() or prepare.get("fdatasync_per_file") is not True or prepare.get("parent_fsync") is not True:
            raise ValueError("prepare proof mismatch")
        fresh = sample.get("fresh_open_verify")
        if not isinstance(fresh, dict) or fresh.get("status") != "PASS" or fresh.get("files_checked") != DELETE_FILE_COUNT or fresh.get("bytes_per_file") != DELETE_FILE_BYTES:
            raise ValueError("fresh content proof mismatch")
        unlink = sample.get("unlink_timer")
        if not isinstance(unlink, dict) or unlink.get("status") != "PASS" or unlink.get("files") != DELETE_FILE_COUNT or not isinstance(unlink.get("wall_ns"), int) or unlink.get("wall_ns") <= 0:
            raise ValueError("unlink proof mismatch")
        post = sample.get("post_unlink")
        if not isinstance(post, dict) or post.get("status") != "PASS" or post.get("remaining") != [] or post.get("parent_fsync") is not True:
            raise ValueError("post-unlink proof mismatch")
    return rounds


def check_deleted_paths(threefs_root: Path, rounds: list[dict[str, Any]]) -> dict[str, Any]:
    failures: list[dict[str, Any]] = []
    checked = 0
    for sample in rounds:
        name = safe_manifest_name(sample.get("sample_name"))
        for file_name in expected_delete_files():
            path = threefs_root / name / file_name
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


def writer(args: argparse.Namespace) -> dict[str, Any]:
    result: dict[str, Any] = {"role": "threefs-delete-writer", "status": "BLOCKED", "scope": "small 3FS namespace delete diagnostic; parent owns lifecycle/comparison"}
    output: Path | None = None
    output_created = False
    rounds: list[dict[str, Any]] = []
    try:
        threefs_root = require_absolute_path(args.threefs_root, "threefs-root")
        output = require_absolute_path(args.output, "output")
        validate_output_path(output)
        output.mkdir(mode=0o700)
        output_created = True
        platform_identity = verify_linux_aarch64_root()
        require_existing_directory(threefs_root, "threefs-root")
        mount = find_mount(threefs_root)
        require_hf3fs_mount(mount, threefs_root)
        helper = load_owner_delete_helper()
        run_id = f"{time.time_ns()}-{os.getpid()}"
        result.update({
            "status": "FAIL",
            "threefs_upstream_commit": THREEFS_UPSTREAM_COMMIT,
            "threefs_arm64_patch_sha256": THREEFS_ARM64_PATCH_SHA256,
            "platform": platform_identity,
            "fs": {"threefs_root": stat_identity(threefs_root)},
            "mount": mount,
            "run_id": run_id,
            "delete_shape": delete_shape(),
            "owner_delete_helper": {"path": str(Path(__file__).with_name("owner_remote_small.py")), "sha256": OWNER_REMOTE_SMALL_SHA256},
            "timer_scope": "unlink_timer covers exactly 100 Python-loop unlink syscalls; prepare, fdatasync, fresh-open verification, directory fsync, namespace checks, and cleanup are outside timer",
            "cache_state": "UNOBSERVED",
            "comparison_status": "REFERENCE_ONLY_PARENT_COMPARISON_NOT_REPORTED_BY_THIS_TOOL",
        })
        write_json(output / "preflight.json", {k: result[k] for k in ("threefs_upstream_commit", "threefs_arm64_patch_sha256", "platform", "fs", "mount", "delete_shape", "owner_delete_helper")})
        for index in range(WARMUP_ROUNDS + MEASUREMENT_ROUNDS):
            sample = run_delete_sample(threefs_root, run_id, index, helper)
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


def checker(args: argparse.Namespace) -> dict[str, Any]:
    checker_id = args.checker_id
    result: dict[str, Any] = {"role": "threefs-delete-checker", "checker_id": checker_id, "status": "BLOCKED", "scope": "independent hf3fs mount ENOENT check for threefs-delete-writer manifest"}
    output: Path | None = None
    output_created = False
    try:
        threefs_root = require_absolute_path(args.threefs_root, "threefs-root")
        manifest_path = require_absolute_path(args.manifest, "manifest")
        output = require_absolute_path(args.output, "output")
        validate_output_path(output)
        output.mkdir(mode=0o700)
        output_created = True
        platform_identity = verify_linux_aarch64_root()
        require_existing_directory(threefs_root, "threefs-root")
        if manifest_path.is_symlink() or not manifest_path.is_file():
            raise ValueError(f"manifest must be an existing non-symlink file: {manifest_path}")
        mount = find_mount(threefs_root)
        require_hf3fs_mount(mount, threefs_root)
        manifest = read_json(manifest_path)
        rounds = validate_manifest(manifest)
        check = check_deleted_paths(threefs_root, rounds)
        write_json(output / "enoent-check.json", check)
        result.update({
            "status": "DATA_RECORDED" if check.get("status") == "PASS" else "FAIL",
            "threefs_upstream_commit": THREEFS_UPSTREAM_COMMIT,
            "threefs_arm64_patch_sha256": THREEFS_ARM64_PATCH_SHA256,
            "platform": platform_identity,
            "fs": {"threefs_root": stat_identity(threefs_root)},
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


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Small 3FS namespace delete diagnostic adapter")
    subparsers = parser.add_subparsers(dest="command", required=True)
    writer_parser = subparsers.add_parser("writer", help="create/delete 100x4KiB samples on one hf3fs mount")
    writer_parser.add_argument("--threefs-root", required=True)
    writer_parser.add_argument("--output", required=True)
    writer_parser.set_defaults(func=writer)
    checker_parser = subparsers.add_parser("checker", help="check 600 ENOENT paths from a writer manifest on an independent hf3fs mount")
    checker_parser.add_argument("--threefs-root", required=True)
    checker_parser.add_argument("--manifest", required=True)
    checker_parser.add_argument("--output", required=True)
    checker_parser.add_argument("--checker-id", required=True, choices=("B", "C"))
    checker_parser.set_defaults(func=checker)
    return parser


def main(argv: list[str] | None = None) -> int:
    args = build_parser().parse_args(argv)
    result = args.func(args)
    print(json.dumps(result, indent=2, sort_keys=True))
    return 0 if result.get("status") == "DATA_RECORDED" else 1


if __name__ == "__main__":
    raise SystemExit(main())
