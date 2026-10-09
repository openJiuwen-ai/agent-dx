#!/usr/bin/env python3
"""FUN-02/03/04 live visibility acceptance driver.

The acceptance runner owns case/profile/matrix/run-dir selection. This driver
binds that fixed context to already-running Linux AFS FUSE mounts and records
proof for three visibility contracts:

* FUN-02: an existing read-only fd on one mount sees accepted overwrite,
  append and resize from a separate writer process before writer sync/close.
* FUN-03: A writes and closes successfully, then B fresh-opens without a sleep
  barrier and observes the new content and size.
* FUN-04: A fdatasync/fsync separately, then B fresh-opens and observes data,
  length/head and inode attributes. Any old B fd observation is recorded only.
"""
from __future__ import annotations

import argparse
import datetime as dt
import hashlib
import json
import os
import platform
import shlex
import select
import signal
import stat
import subprocess
import sys
import time
import tomllib
import traceback
import uuid
from pathlib import Path
from typing import Any


CASE_IDS = {"FUN-02", "FUN-03", "FUN-04"}
STATUS_VALUES = {"PASS", "FAIL", "BLOCKED", "INCONCLUSIVE", "EXCLUDED"}
EXPECTED_SOURCES = {"OwnerFs": "afs-ownerfs", "DFS": "afs-dfs"}
MAC_PATH_PREFIXES = ("/Users/", "/Volumes/", "/private/", "/var/folders/")
DEFAULT_TIMEOUT = 30


class DriverError(RuntimeError):
    status = "BLOCKED"


class DriverFail(DriverError):
    status = "FAIL"


class BindingError(DriverError):
    pass


def utc() -> str:
    return dt.datetime.now(dt.timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")


def write_json(path: Path, value: Any) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(value, indent=2, sort_keys=True) + "\n", encoding="utf-8")


def emit(value: dict[str, Any]) -> None:
    print(json.dumps(value, sort_keys=True), flush=True)


def sha256_bytes(data: bytes) -> str:
    return hashlib.sha256(data).hexdigest()


def sha256_file(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for chunk in iter(lambda: handle.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def run_text(argv: list[str], timeout: int = 10) -> dict[str, Any]:
    try:
        proc = subprocess.run(argv, shell=False, text=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE, timeout=timeout, check=False)
        return {"argv": argv, "returncode": proc.returncode, "stdout": proc.stdout, "stderr": proc.stderr}
    except Exception as exc:  # noqa: BLE001 - exact failure is evidence
        return {"argv": argv, "returncode": None, "exception": type(exc).__name__, "message": str(exc)}


def mount_identity(path: Path) -> dict[str, Any]:
    return run_text(["findmnt", "-T", str(path), "-o", "TARGET,SOURCE,FSTYPE,OPTIONS", "--json"])


def mount_record(command: dict[str, Any]) -> dict[str, Any]:
    if command.get("returncode") != 0:
        return {}
    try:
        records = json.loads(command.get("stdout", "")).get("filesystems", [])
    except (TypeError, json.JSONDecodeError):
        return {}
    return records[0] if len(records) == 1 else {}


def artifact_manifest(root: Path, run_dir: Path, exclude: set[Path] | None = None) -> list[dict[str, Any]]:
    exclude = {p.resolve() for p in (exclude or set())}
    entries: list[dict[str, Any]] = []
    for path in sorted(p for p in root.rglob("*") if p.is_file()):
        if path.resolve() in exclude:
            continue
        entries.append({"path": str(path.relative_to(run_dir)), "bytes": path.stat().st_size, "sha256": sha256_file(path)})
    return entries


def build_check(name: str, status: str, evidence: Any, artifact: str | None = None) -> dict[str, Any]:
    check = {"name": name, "status": status, "evidence": evidence}
    if artifact:
        check["artifact"] = artifact
    return check


def rel(path: Path, run_dir: Path) -> str:
    try:
        return str(path.relative_to(run_dir))
    except ValueError:
        return str(path)


def parse_json_tail(text: str) -> Any:
    stripped = text.strip()
    if not stripped:
        return None
    candidates = [stripped]
    candidates.extend(line.strip() for line in reversed(stripped.splitlines()) if line.strip())
    seen: set[str] = set()
    for candidate in candidates:
        if candidate in seen:
            continue
        seen.add(candidate)
        try:
            return json.loads(candidate)
        except json.JSONDecodeError:
            continue
    return None


def safe_rel_path(value: str | None = None) -> str:
    if value is None:
        return f".afs-visibility-{uuid.uuid4().hex}.dat"
    path = Path(value)
    if path.is_absolute() or ".." in path.parts or not value:
        raise BindingError(f"unsafe relative path: {value!r}")
    return value


def join_base(base_dir: Path, rel_path: str) -> Path:
    return base_dir / safe_rel_path(rel_path)


def validate_abs_path(value: Any, name: str) -> str:
    if not isinstance(value, str) or not value.startswith("/"):
        raise BindingError(f"{name} must be an absolute Linux path")
    if value.startswith(MAC_PATH_PREFIXES):
        raise BindingError(f"{name} must not be a macOS host path")
    return value


def validate_sha(value: Any, name: str) -> str:
    if not isinstance(value, str) or len(value) != 64 or any(ch not in "0123456789abcdef" for ch in value):
        raise BindingError(f"{name} must be 64 lowercase hex characters")
    return value


def validate_positive_int(value: Any, name: str) -> int:
    if isinstance(value, str) and value.isdecimal():
        value = int(value, 10)
    if not isinstance(value, int) or value <= 0:
        raise BindingError(f"{name} must be a positive integer")
    return value


def validate_boot_id(value: Any, name: str) -> str:
    if not isinstance(value, str):
        raise BindingError(f"{name} must be a Linux boot_id string")
    parts = value.split("-")
    if [len(part) for part in parts] != [8, 4, 4, 4, 12] or any(ch not in "0123456789abcdef" for part in parts for ch in part):
        raise BindingError(f"{name} must be a lowercase Linux boot_id UUID")
    return value


def validate_ssh_host(value: Any, name: str) -> str:
    if not isinstance(value, str) or not value or value.startswith("-") or any(ch.isspace() for ch in value):
        raise BindingError(f"{name} must be an ssh host alias without whitespace/options")
    return value


def validate_timeout(value: Any, name: str) -> int:
    if value is None:
        return DEFAULT_TIMEOUT
    return validate_positive_int(value, name)


def read_boot_id() -> str | None:
    try:
        return Path("/proc/sys/kernel/random/boot_id").read_text(encoding="utf-8").strip()
    except OSError:
        return None


def read_start_ticks(proc: Path) -> int | None:
    try:
        stat_text = proc.joinpath("stat").read_text(encoding="utf-8")
        after_comm = stat_text.rsplit(")", 1)[1].strip().split()
        return int(after_comm[19])
    except Exception:  # noqa: BLE001 - captured as identity mismatch
        return None


def config_path_from_cmdline(items: list[str]) -> str | None:
    for index, item in enumerate(items):
        if item == "--config" and index + 1 < len(items):
            return items[index + 1]
        if item.startswith("--config="):
            return item.split("=", 1)[1]
    return None


def process_identity(pid: int, role: str, expected_sha256: str, expected_boot_id: str, expected_start_ticks: int) -> dict[str, Any]:
    proc = Path("/proc") / str(pid)
    result: dict[str, Any] = {
        "role": role,
        "pid": pid,
        "exists": proc.exists(),
        "expected_sha256": expected_sha256,
        "boot_id": read_boot_id(),
        "expected_boot_id": expected_boot_id,
        "start_ticks": read_start_ticks(proc) if proc.exists() else None,
        "expected_start_ticks": expected_start_ticks,
    }
    result["boot_id_ok"] = bool(result["boot_id"] == expected_boot_id)
    result["start_ticks_ok"] = bool(result["start_ticks"] == expected_start_ticks)
    try:
        exe = proc.joinpath("exe").resolve()
        result["exe_path"] = str(exe)
        result["sha256"] = sha256_file(exe) if exe.is_file() else None
    except Exception as exc:  # noqa: BLE001
        result["exe_error"] = f"{type(exc).__name__}: {exc}"
    try:
        result["cmdline_items"] = [item.decode(errors="replace") for item in proc.joinpath("cmdline").read_bytes().split(b"\0") if item]
    except Exception as exc:  # noqa: BLE001
        result["cmdline_error"] = f"{type(exc).__name__}: {exc}"
    result["sha256_ok"] = bool(result.get("sha256") == expected_sha256)
    return result


def attach_config_identity(identity: dict[str, Any], expected_path: str | None, expected_sha256: str | None) -> dict[str, Any]:
    observed_path = config_path_from_cmdline(identity.get("cmdline_items") if isinstance(identity.get("cmdline_items"), list) else [])
    config: dict[str, Any] = {"observed_path": observed_path, "expected_path": expected_path, "expected_sha256": expected_sha256}
    if observed_path:
        path = Path(observed_path)
        config["exists"] = path.is_file()
        if path.is_file():
            config["sha256"] = sha256_file(path)
            try:
                parsed = tomllib.loads(path.read_text(encoding="utf-8"))
                config["facts"] = {
                    key: str(parsed[key])
                    for key in ("meta_store", "data_mode", "rdma_device", "meta_endpoint", "grpc_listen", "rest_listen")
                    if key in parsed
                }
            except Exception as exc:  # noqa: BLE001 - malformed config is preflight evidence
                config["parse_error"] = f"{type(exc).__name__}: {exc}"
    config["path_ok"] = bool(expected_path and observed_path == expected_path)
    config["sha256_ok"] = bool(expected_sha256 and config.get("sha256") == expected_sha256)
    identity["config_identity"] = config
    return identity


def process_identity_ok(identity: dict[str, Any], executable: str) -> bool:
    if identity.get("exists") is not True:
        return False
    if Path(str(identity.get("exe_path", ""))).name != executable:
        return False
    if identity.get("sha256_ok") is not True or identity.get("boot_id_ok") is not True or identity.get("start_ticks_ok") is not True:
        return False
    config = identity.get("config_identity")
    return config is None or (config.get("path_ok") is True and config.get("sha256_ok") is True)


def expected_process(record: Any, role: str, name: str) -> dict[str, Any]:
    if not isinstance(record, dict):
        raise BindingError(f"{name} must be an object")
    unknown = sorted(set(record) - {"role", "pid", "sha256", "boot_id", "start_ticks", "config_path", "config_sha256"})
    if unknown:
        raise BindingError(f"unsupported {name} field(s): {', '.join(unknown)}")
    if record.get("role") != role:
        raise BindingError(f"{name}.role must be {role}")
    result = {
        "role": role,
        "pid": validate_positive_int(record.get("pid"), f"{name}.pid"),
        "sha256": validate_sha(record.get("sha256"), f"{name}.sha256"),
        "boot_id": validate_boot_id(record.get("boot_id"), f"{name}.boot_id"),
        "start_ticks": validate_positive_int(record.get("start_ticks"), f"{name}.start_ticks"),
    }
    if record.get("config_path") is not None or record.get("config_sha256") is not None:
        result["config_path"] = validate_abs_path(record.get("config_path"), f"{name}.config_path")
        result["config_sha256"] = validate_sha(record.get("config_sha256"), f"{name}.config_sha256")
    return result


def worker_record(record: Any, role: str, name: str) -> dict[str, Any]:
    if not isinstance(record, dict):
        raise BindingError(f"{name} must be an object")
    unknown = sorted(set(record) - {"transport", "host", "ssh_config", "python", "driver", "mount", "base_dir", "worker_run_dir", "expected_process"})
    if unknown:
        raise BindingError(f"unsupported {name} field(s): {', '.join(unknown)}")
    if record.get("transport") != "ssh":
        raise BindingError(f"{name}.transport must be ssh")
    result = {
        "transport": "ssh",
        "host": validate_ssh_host(record.get("host"), f"{name}.host"),
        "python": validate_abs_path(record.get("python"), f"{name}.python"),
        "driver": validate_abs_path(record.get("driver"), f"{name}.driver"),
        "mount": Path(validate_abs_path(record.get("mount"), f"{name}.mount")),
        "base_dir": Path(validate_abs_path(record.get("base_dir"), f"{name}.base_dir")),
        "worker_run_dir": Path(validate_abs_path(record.get("worker_run_dir"), f"{name}.worker_run_dir")),
        "expected_process": expected_process(record.get("expected_process"), role, f"{name}.expected_process"),
    }
    if record.get("ssh_config") is not None:
        result["ssh_config"] = validate_abs_path(record.get("ssh_config"), f"{name}.ssh_config")
    return result


def matrix_from_env(env: dict[str, str]) -> dict[str, str]:
    raw = env.get("AFS_ACCEPTANCE_MATRIX", "{}")
    try:
        parsed = json.loads(raw)
    except json.JSONDecodeError as exc:
        raise BindingError(f"AFS_ACCEPTANCE_MATRIX is not valid JSON: {exc}") from exc
    if not isinstance(parsed, dict):
        raise BindingError("AFS_ACCEPTANCE_MATRIX must decode to an object")
    return {str(key): str(value) for key, value in parsed.items()}


def required_context(env: dict[str, str]) -> tuple[str, str, dict[str, str], Path, Path]:
    case_id = env.get("AFS_ACCEPTANCE_CASE_ID")
    profile = env.get("AFS_ACCEPTANCE_PROFILE")
    run_dir = env.get("AFS_ACCEPTANCE_RUN_DIR")
    binding = env.get("AFS_ACCEPTANCE_VISIBILITY_BINDINGS")
    if case_id not in CASE_IDS:
        raise BindingError(f"unsupported visibility case {case_id!r}")
    if profile not in {"smoke", "full"}:
        raise BindingError(f"unsupported profile {profile!r}")
    if not run_dir:
        raise BindingError("AFS_ACCEPTANCE_RUN_DIR is missing")
    if not binding:
        raise BindingError("AFS_ACCEPTANCE_VISIBILITY_BINDINGS is missing")
    return case_id, profile, matrix_from_env(env), Path(run_dir), Path(binding)


def normalized_matrix(binding: dict[str, Any]) -> dict[str, str]:
    matrix = binding.get("matrix")
    if not isinstance(matrix, dict):
        raise BindingError("binding matrix must be an object")
    return {str(key): str(value) for key, value in matrix.items()}


def select_binding(document: Any, case_id: str, profile: str, matrix: dict[str, str]) -> dict[str, Any]:
    if not isinstance(document, dict) or document.get("schema_version") != 1:
        raise BindingError("visibility binding file must be a schema_version 1 object")
    bindings = document.get("bindings")
    if not isinstance(bindings, list):
        raise BindingError("visibility binding file must contain bindings")
    matches: list[dict[str, Any]] = []
    for item in bindings:
        if not isinstance(item, dict):
            raise BindingError("each binding entry must be an object")
        if item.get("case_id") != case_id:
            continue
        item_profile = item.get("profile")
        if item_profile is not None and item_profile != profile:
            continue
        if normalized_matrix(item) == matrix:
            matches.append(item)
    if not matches:
        raise BindingError(f"no binding for {case_id} with exact matrix {json.dumps(matrix, sort_keys=True)}")
    if len(matches) > 1:
        raise BindingError(f"ambiguous bindings for {case_id} with exact matrix {json.dumps(matrix, sort_keys=True)}")
    return matches[0]


def target_record(record: Any, name: str) -> dict[str, Any]:
    if not isinstance(record, dict):
        raise BindingError(f"{name} must be an object")
    unknown = sorted(set(record) - {"label", "mount", "base_dir", "rel_path", "worker"})
    if unknown:
        raise BindingError(f"unsupported {name} field(s): {', '.join(unknown)}")
    worker = record.get("worker")
    if worker is not None and worker not in {"writer", "reader"}:
        raise BindingError(f"{name}.worker must be writer or reader")
    label = record.get("label")
    if not isinstance(label, str) or not label:
        raise BindingError(f"{name}.label is required")
    return {
        "label": label,
        "mount": Path(validate_abs_path(record.get("mount"), f"{name}.mount")),
        "base_dir": Path(validate_abs_path(record.get("base_dir"), f"{name}.base_dir")),
        "rel_path": safe_rel_path(record.get("rel_path")),
        "worker": worker,
    }


def validate_binding(binding: dict[str, Any], case_id: str, profile: str, matrix: dict[str, str]) -> dict[str, Any]:
    allowed = {
        "case_id", "profile", "matrix", "mode", "timeout", "backend", "meta_process", "writer_process", "reader_process",
        "mount", "base_dir", "writer_mount", "writer_base_dir", "reader_mount", "reader_base_dir", "rel_path",
        "fun02_targets", "ctl_worker", "writer_worker", "reader_worker", "expected_meta_endpoint", "old_reader_fd_observation",
    }
    unknown = sorted(set(binding) - allowed)
    if unknown:
        raise BindingError(f"unsupported binding field(s): {', '.join(unknown)}")
    if binding.get("profile") not in {None, profile}:
        raise BindingError("binding profile conflicts with runner profile")
    if normalized_matrix(binding) != matrix:
        raise BindingError("binding matrix does not exactly match runner matrix")
    backend = str(binding.get("backend") or matrix.get("backend") or "")
    if backend not in EXPECTED_SOURCES:
        raise BindingError("visibility binding backend must be OwnerFs or DFS")
    mode = binding.get("mode", "local")
    if mode not in {"local", "remote-host"}:
        raise BindingError("binding mode must be local or remote-host")
    result: dict[str, Any] = {"case_id": case_id, "profile": profile, "matrix": matrix, "mode": mode, "backend": backend, "timeout": validate_timeout(binding.get("timeout"), "timeout"), "old_reader_fd_observation": bool(binding.get("old_reader_fd_observation", False))}
    if mode == "remote-host":
        result["ctl_worker"] = worker_record(binding.get("ctl_worker"), "meta", "ctl_worker")
        result["writer_worker"] = worker_record(binding.get("writer_worker"), "node", "writer_worker")
        result["reader_worker"] = worker_record(binding.get("reader_worker"), "node", "reader_worker")
        endpoint = binding.get("expected_meta_endpoint")
        if not isinstance(endpoint, str) or not endpoint:
            raise BindingError("expected_meta_endpoint is required for remote-host visibility binding")
        result["expected_meta_endpoint"] = endpoint
        if case_id == "FUN-02":
            targets = binding.get("fun02_targets")
            if not isinstance(targets, list) or not targets:
                raise BindingError("FUN-02 remote-host binding requires fun02_targets")
            result["fun02_targets"] = [target_record(target, f"fun02_targets[{idx}]") for idx, target in enumerate(targets)]
        else:
            result["rel_path"] = safe_rel_path(binding.get("rel_path"))
        return result

    result["meta_process"] = expected_process(binding.get("meta_process"), "meta", "meta_process")
    result["writer_process"] = expected_process(binding.get("writer_process"), "node", "writer_process")
    result["reader_process"] = expected_process(binding.get("reader_process"), "node", "reader_process")
    if case_id == "FUN-02":
        targets = binding.get("fun02_targets")
        if targets is None:
            targets = [{"label": "same-mount", "mount": binding.get("mount"), "base_dir": binding.get("base_dir"), "rel_path": binding.get("rel_path")}]
        if not isinstance(targets, list) or not targets:
            raise BindingError("FUN-02 binding requires non-empty fun02_targets")
        result["fun02_targets"] = [target_record(target, f"fun02_targets[{idx}]") for idx, target in enumerate(targets)]
    else:
        result["writer_mount"] = Path(validate_abs_path(binding.get("writer_mount"), "writer_mount"))
        result["writer_base_dir"] = Path(validate_abs_path(binding.get("writer_base_dir"), "writer_base_dir"))
        result["reader_mount"] = Path(validate_abs_path(binding.get("reader_mount"), "reader_mount"))
        result["reader_base_dir"] = Path(validate_abs_path(binding.get("reader_base_dir"), "reader_base_dir"))
        result["rel_path"] = safe_rel_path(binding.get("rel_path"))
    return result


def check_mount(path: Path, backend: str) -> tuple[bool, dict[str, Any]]:
    command = mount_identity(path)
    record = mount_record(command)
    expected = EXPECTED_SOURCES[backend]
    ok = bool(record) and record.get("source") == expected and str(record.get("fstype", "")).startswith("fuse")
    return ok, {"path": str(path), "expected_source": expected, "command": command, "record": record}


def check_base_dir(base_dir: Path, mount: Path) -> tuple[bool, dict[str, Any]]:
    try:
        base_resolved = base_dir.resolve()
        mount_resolved = mount.resolve()
        under = base_resolved == mount_resolved or mount_resolved in base_resolved.parents
    except OSError:
        under = False
    return base_dir.is_dir() and under, {"base_dir": str(base_dir), "mount": str(mount), "exists": base_dir.exists(), "is_dir": base_dir.is_dir(), "under_mount": under}


def collect_local_identity(binding: dict[str, Any]) -> dict[str, Any]:
    identities: dict[str, Any] = {}
    for key, executable in (("meta_process", "afs-meta"), ("writer_process", "afs-node"), ("reader_process", "afs-node")):
        expected = binding[key]
        observed = process_identity(expected["pid"], expected["role"], expected["sha256"], expected["boot_id"], expected["start_ticks"])
        if expected.get("config_path") and expected.get("config_sha256"):
            attach_config_identity(observed, expected["config_path"], expected["config_sha256"])
        observed["ok"] = process_identity_ok(observed, executable)
        identities[key] = observed
    return identities


def stat_digest(path: Path) -> dict[str, Any]:
    st = os.stat(path)
    with path.open("rb") as handle:
        data = handle.read()
    return {
        "path": str(path),
        "size": st.st_size,
        "mode": stat.S_IMODE(st.st_mode),
        "uid": st.st_uid,
        "gid": st.st_gid,
        "mtime_ns": st.st_mtime_ns,
        "ctime_ns": st.st_ctime_ns,
        "head": data[:32].hex(),
        "sha256": sha256_bytes(data),
    }


def read_exact(path: Path) -> bytes:
    with path.open("rb") as handle:
        return handle.read()


def wait_for_line(fd: int, expected: bytes, timeout: int) -> bytes:
    deadline = time.monotonic() + timeout
    data = b""
    while not data.endswith(b"\n"):
        if time.monotonic() > deadline:
            raise DriverError(f"timed out waiting for writer event {expected!r}; got {data!r}")
        chunk = os.read(fd, 1)
        if not chunk:
            raise DriverError(f"writer pipe closed waiting for {expected!r}; got {data!r}")
        data += chunk
    line = data.rstrip(b"\n")
    if line != expected:
        raise DriverError(f"unexpected writer event: expected {expected!r}, got {line!r}")
    return line


def send_line(fd: int, line: bytes) -> None:
    os.write(fd, line + b"\n")


def fun02_writer_main(argv: list[str]) -> int:
    parser = argparse.ArgumentParser(description="FUN-02 writer process")
    parser.add_argument("--path", type=Path, required=True)
    parser.add_argument("--event-fd", type=int, required=True)
    parser.add_argument("--command-fd", type=int, required=True)
    args = parser.parse_args(argv)
    fd = os.open(args.path, os.O_RDWR)
    cmd_r = args.command_fd
    evt_w = args.event_fd
    try:
        send_line(evt_w, b"READY")
        wait_for_line(cmd_r, b"OVERWRITE", DEFAULT_TIMEOUT)
        os.pwrite(fd, b"bravo", 0)
        send_line(evt_w, b"OVERWROTE")
        wait_for_line(cmd_r, b"APPEND", DEFAULT_TIMEOUT)
        os.lseek(fd, 0, os.SEEK_END)
        os.write(fd, b"-append")
        send_line(evt_w, b"APPENDED")
        wait_for_line(cmd_r, b"RESIZE", DEFAULT_TIMEOUT)
        os.ftruncate(fd, 7)
        send_line(evt_w, b"RESIZED")
        wait_for_line(cmd_r, b"RELEASE", DEFAULT_TIMEOUT)
    finally:
        os.close(fd)
    return 0


def run_fun02_target(base_dir: Path, rel_path: str, timeout: int) -> dict[str, Any]:
    path = join_base(base_dir, rel_path)
    path.parent.mkdir(parents=True, exist_ok=True)
    initial = b"alpha-0000"
    path.write_bytes(initial)
    ro_fd = os.open(path, os.O_RDONLY)
    evt_r, evt_w = os.pipe()
    cmd_r, cmd_w = os.pipe()
    proc: subprocess.Popen[str] | None = None
    observations: dict[str, Any] = {"path": str(path), "rel_path": rel_path, "initial_sha256": sha256_bytes(initial)}
    try:
        proc = subprocess.Popen(
            [sys.executable, str(Path(__file__).resolve()), "fun02-writer", "--path", str(path), "--event-fd", str(evt_w), "--command-fd", str(cmd_r)],
            pass_fds=(evt_w, cmd_r), text=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE, shell=False,
        )
        os.close(evt_w); os.close(cmd_r)
        wait_for_line(evt_r, b"READY", timeout)
        observations["initial_from_old_fd"] = os.pread(ro_fd, 64, 0).hex()
        send_line(cmd_w, b"OVERWRITE")
        wait_for_line(evt_r, b"OVERWROTE", timeout)
        after_overwrite = os.pread(ro_fd, 64, 0)
        observations["overwrite_from_old_fd"] = after_overwrite.hex()
        send_line(cmd_w, b"APPEND")
        wait_for_line(evt_r, b"APPENDED", timeout)
        after_append = os.pread(ro_fd, 64, 0)
        observations["append_from_old_fd"] = after_append.hex()
        send_line(cmd_w, b"RESIZE")
        wait_for_line(evt_r, b"RESIZED", timeout)
        after_resize = os.pread(ro_fd, 64, 0)
        observations["resize_from_old_fd"] = after_resize.hex()
        observations["old_fd_size_after_resize"] = os.fstat(ro_fd).st_size
        ok = after_overwrite.startswith(b"bravo") and after_append.startswith(b"bravo") and after_append.endswith(b"-append") and after_resize == b"bravo-0" and observations["old_fd_size_after_resize"] == 7
        observations["accepted_dirty_visible_before_close_or_sync"] = ok
        send_line(cmd_w, b"RELEASE")
        stdout, stderr = proc.communicate(timeout=timeout)
        observations["writer_returncode"] = proc.returncode
        observations["writer_stdout"] = stdout
        observations["writer_stderr"] = stderr
        observations["final"] = stat_digest(path)
        if proc.returncode != 0:
            raise DriverError(f"FUN-02 writer exited {proc.returncode}")
        if not ok:
            raise DriverFail("FUN-02 old read-only fd did not observe all accepted dirty states before close/sync")
        return observations
    finally:
        try:
            os.close(ro_fd)
        except OSError:
            pass
        for fd in (evt_r, cmd_w, evt_w, cmd_r):
            try:
                os.close(fd)
            except OSError:
                pass
        if proc is not None and proc.poll() is None:
            proc.terminate()
            try:
                proc.wait(timeout=2)
            except subprocess.TimeoutExpired:
                proc.kill(); proc.wait()


def write_close(path: Path, payload: bytes) -> dict[str, Any]:
    path.parent.mkdir(parents=True, exist_ok=True)
    fd = os.open(path, os.O_WRONLY | os.O_CREAT | os.O_TRUNC, 0o644)
    close_ok = False
    try:
        os.write(fd, payload)
    finally:
        os.close(fd)
        close_ok = True
    record = stat_digest(path)
    record["close_success"] = close_ok
    return record


def write_sync(path: Path, payload: bytes, sync_kind: str) -> dict[str, Any]:
    path.parent.mkdir(parents=True, exist_ok=True)
    fd = os.open(path, os.O_WRONLY | os.O_CREAT | os.O_TRUNC, 0o644)
    try:
        os.write(fd, payload)
        if sync_kind == "fdatasync":
            os.fdatasync(fd)
        elif sync_kind == "fsync":
            os.fsync(fd)
        else:
            raise ValueError(sync_kind)
    finally:
        os.close(fd)
    record = stat_digest(path)
    record["sync_kind"] = sync_kind
    return record


def fresh_read_check(base_dir: Path, rel_path: str, expected_sha256: str, expected_size: int, expected_head: str | None = None) -> dict[str, Any]:
    path = join_base(base_dir, rel_path)
    record = stat_digest(path)
    record["expected_sha256"] = expected_sha256
    record["expected_size"] = expected_size
    if expected_head is not None:
        record["expected_head"] = expected_head
    record["ok"] = record["sha256"] == expected_sha256 and record["size"] == expected_size and (expected_head is None or record["head"] == expected_head)
    return record


OLD_FD_CONTRACT = "observed-only; not required to remain an old permanent version"


def fd_digest(fd: int) -> dict[str, Any]:
    st = os.fstat(fd)
    digest = hashlib.sha256()
    remaining = st.st_size
    offset = 0
    head = b""
    while remaining > 0:
        chunk = os.pread(fd, min(1024 * 1024, remaining), offset)
        if not chunk:
            break
        if len(head) < 64:
            head += chunk[: 64 - len(head)]
        digest.update(chunk)
        offset += len(chunk)
        remaining -= len(chunk)
    return {
        "size": st.st_size,
        "mode": stat.S_IMODE(st.st_mode),
        "uid": st.st_uid,
        "gid": st.st_gid,
        "mtime_ns": st.st_mtime_ns,
        "ctime_ns": st.st_ctime_ns,
        "head": head.hex(),
        "sha256": digest.hexdigest(),
    }


def old_fd_observation(base_dir: Path, rel_path: str) -> dict[str, Any]:
    path = join_base(base_dir, rel_path)
    if not path.exists():
        return {"enabled": True, "opened": False, "reason": "path did not exist before writer operation", "contract": OLD_FD_CONTRACT}
    fd = os.open(path, os.O_RDONLY)
    try:
        before = fd_digest(fd)
        after = fd_digest(fd)
        return {"enabled": True, "opened": True, "path": str(path), "before": before, "after": after, "contract": OLD_FD_CONTRACT}
    finally:
        os.close(fd)


def run_with_held_old_fd(base_dir: Path, rel_path: str, mutator) -> dict[str, Any]:
    path = join_base(base_dir, rel_path)
    fd = os.open(path, os.O_RDONLY)
    try:
        before = fd_digest(fd)
        mutator()
        after = fd_digest(fd)
        return {"enabled": True, "opened": True, "path": str(path), "before": before, "after": after, "contract": OLD_FD_CONTRACT}
    finally:
        os.close(fd)


def run_local(case_id: str, profile: str, matrix: dict[str, str], binding: dict[str, Any], artifacts: Path, run_dir: Path) -> dict[str, Any]:
    checks: list[dict[str, Any]] = []
    records: dict[str, Any] = {"started_at": utc(), "mode": "local", "case_id": case_id}
    if platform.system() != "Linux":
        checks.append(build_check("linux-runtime", "BLOCKED", {"system": platform.system()}))
        return finish_proof(case_id, profile, matrix, "BLOCKED", "visibility driver requires Linux", checks, records, artifacts, run_dir)
    identity = collect_local_identity(binding)
    write_json(artifacts / "identity.json", identity)
    identity_ok = all(item.get("ok") is True for item in identity.values())
    checks.append(build_check("process-identity", "PASS" if identity_ok else "BLOCKED", identity, rel(artifacts / "identity.json", run_dir)))

    mount_checks: list[dict[str, Any]] = []
    base_checks: list[dict[str, Any]] = []
    mounts: list[Path]
    if case_id == "FUN-02":
        mounts = [target["mount"] for target in binding["fun02_targets"]]
        bases = [(target["base_dir"], target["mount"]) for target in binding["fun02_targets"]]
    else:
        mounts = [binding["writer_mount"], binding["reader_mount"]]
        bases = [(binding["writer_base_dir"], binding["writer_mount"]), (binding["reader_base_dir"], binding["reader_mount"])]
    for mount in mounts:
        ok, evidence = check_mount(mount, binding["backend"])
        mount_checks.append({"ok": ok, **evidence})
    for base, mount in bases:
        ok, evidence = check_base_dir(base, mount)
        base_checks.append({"ok": ok, **evidence})
    write_json(artifacts / "mounts.json", {"mounts": mount_checks, "base_dirs": base_checks})
    checks.append(build_check("mount-identity", "PASS" if all(item["ok"] for item in mount_checks) else "BLOCKED", mount_checks, rel(artifacts / "mounts.json", run_dir)))
    checks.append(build_check("base-dir-scope", "PASS" if all(item["ok"] for item in base_checks) else "BLOCKED", base_checks, rel(artifacts / "mounts.json", run_dir)))
    if not identity_ok or not all(item["ok"] for item in mount_checks) or not all(item["ok"] for item in base_checks):
        return finish_proof(case_id, profile, matrix, "BLOCKED", "visibility local preflight failed", checks, records, artifacts, run_dir)

    if case_id == "FUN-02":
        target_records = []
        for target in binding["fun02_targets"]:
            target_records.append({"label": target["label"], **run_fun02_target(target["base_dir"], target["rel_path"], binding["timeout"])})
        records["fun02_targets"] = target_records
        write_json(artifacts / "fun02.json", target_records)
        ok = all(item["accepted_dirty_visible_before_close_or_sync"] for item in target_records)
        checks.append(build_check("fun02-same-mount-visible", "PASS" if ok else "FAIL", target_records, rel(artifacts / "fun02.json", run_dir)))
        return finish_proof(case_id, profile, matrix, "PASS" if ok else "FAIL", "" if ok else "FUN-02 dirty visibility check failed", checks, records, artifacts, run_dir)

    rel_path = binding["rel_path"]
    if case_id == "FUN-03":
        payload = f"fun03-close-to-open-{uuid.uuid4().hex}".encode()
        writer = write_close(join_base(binding["writer_base_dir"], rel_path), payload)
        reader = fresh_read_check(binding["reader_base_dir"], rel_path, writer["sha256"], writer["size"])
        records["fun03"] = {"writer": writer, "reader": reader, "barrier": "reader starts only after writer close_success"}
        write_json(artifacts / "fun03.json", records["fun03"])
        ok = writer["close_success"] is True and reader["ok"] is True
        checks.append(build_check("fun03-close-to-open", "PASS" if ok else "FAIL", records["fun03"], rel(artifacts / "fun03.json", run_dir)))
        return finish_proof(case_id, profile, matrix, "PASS" if ok else "FAIL", "" if ok else "FUN-03 close-to-open visibility failed", checks, records, artifacts, run_dir)

    lanes: list[dict[str, Any]] = []
    for sync_kind in ("fdatasync", "fsync"):
        lane_rel = safe_rel_path(f"{rel_path}.{sync_kind}")
        initial_payload = f"fun04-initial-{sync_kind}-{uuid.uuid4().hex}".encode()
        initial_writer = write_sync(join_base(binding["writer_base_dir"], lane_rel), initial_payload, "fsync")
        initial_reader = fresh_read_check(binding["reader_base_dir"], lane_rel, initial_writer["sha256"], initial_writer["size"], initial_writer["head"])
        payload = f"fun04-{sync_kind}-{uuid.uuid4().hex}".encode()
        writer_box: dict[str, Any] = {}

        def mutate() -> None:
            writer_box["writer"] = write_sync(join_base(binding["writer_base_dir"], lane_rel), payload, sync_kind)

        if binding.get("old_reader_fd_observation"):
            old_fd = run_with_held_old_fd(binding["reader_base_dir"], lane_rel, mutate)
            writer = writer_box["writer"]
        else:
            old_fd = {"enabled": False, "contract": "old remote fd observation disabled"}
            writer = write_sync(join_base(binding["writer_base_dir"], lane_rel), payload, sync_kind)
        reader = fresh_read_check(binding["reader_base_dir"], lane_rel, writer["sha256"], writer["size"], writer["head"])
        lane_ok = initial_reader["ok"] and reader["ok"] and (old_fd.get("opened") is True if binding.get("old_reader_fd_observation") else True)
        lanes.append({"sync_kind": sync_kind, "rel_path": lane_rel, "initial_writer": initial_writer, "initial_reader": initial_reader, "writer": writer, "reader": reader, "old_fd_observation": old_fd, "ok": lane_ok})
    records["fun04"] = lanes
    write_json(artifacts / "fun04.json", lanes)
    ok = all(lane["ok"] for lane in lanes)
    checks.append(build_check("fun04-sync-to-reopen", "PASS" if ok else "FAIL", lanes, rel(artifacts / "fun04.json", run_dir)))
    return finish_proof(case_id, profile, matrix, "PASS" if ok else "FAIL", "" if ok else "FUN-04 sync-to-reopen visibility failed", checks, records, artifacts, run_dir)


def worker_prefix(worker: dict[str, Any]) -> list[str]:
    prefix = ["ssh", "-o", "BatchMode=yes"]
    if worker.get("ssh_config"):
        prefix.extend(["-F", worker["ssh_config"]])
    prefix.extend(["--", worker["host"], worker["python"], worker["driver"]])
    return prefix


def worker_argv(worker: dict[str, Any], args: list[str]) -> list[str]:
    prefix = worker_prefix(worker)
    if "--" not in prefix:
        raise BindingError("SSH worker requires an explicit -- host separator")
    remote_start = prefix.index("--") + 2
    if remote_start >= len(prefix):
        raise BindingError("SSH worker requires a host and remote program")
    return prefix[:remote_start] + [shlex.join(prefix[remote_start:] + args)]


def parse_worker_events(stdout: str) -> tuple[list[dict[str, Any]], list[dict[str, Any]]]:
    events: list[dict[str, Any]] = []
    parse_errors: list[dict[str, Any]] = []
    for line_number, line in enumerate(stdout.splitlines(), 1):
        if not line.strip():
            continue
        try:
            events.append(json.loads(line))
        except json.JSONDecodeError as exc:
            parse_errors.append({"line": line_number, "error": str(exc), "text": line})
    return events, parse_errors


def terminate_worker(proc: subprocess.Popen[str]) -> tuple[str, str, int | None]:
    try:
        os.killpg(proc.pid, signal.SIGTERM)
    except ProcessLookupError:
        pass
    try:
        stdout, stderr = proc.communicate(timeout=2)
    except subprocess.TimeoutExpired:
        try:
            os.killpg(proc.pid, signal.SIGKILL)
        except ProcessLookupError:
            pass
        stdout, stderr = proc.communicate()
    return stdout, stderr, proc.returncode


def run_worker_json(worker: dict[str, Any], args: list[str], timeout: int) -> dict[str, Any]:
    argv = worker_argv(worker, args)
    started = time.time()
    proc = subprocess.Popen(argv, shell=False, text=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE, start_new_session=True)
    timed_out = False
    try:
        stdout, stderr = proc.communicate(timeout=timeout)
        returncode = proc.returncode
    except subprocess.TimeoutExpired:
        timed_out = True
        stdout, stderr, returncode = terminate_worker(proc)
    events, parse_errors = parse_worker_events(stdout)
    return {"argv": argv, "returncode": returncode, "stdout": stdout, "stderr": stderr, "json_events": events, "json_parse_errors": parse_errors, "timed_out": timed_out, "timeout_seconds": timeout, "duration_seconds": round(time.time() - started, 3)}


def start_worker_json(worker: dict[str, Any], args: list[str]) -> dict[str, Any]:
    argv = worker_argv(worker, args)
    proc = subprocess.Popen(argv, shell=False, text=True, stdin=subprocess.PIPE, stdout=subprocess.PIPE, stderr=subprocess.PIPE, start_new_session=True)
    return {"argv": argv, "proc": proc, "started": time.time(), "ready_events": [], "json_parse_errors": []}


def read_worker_event(started_record: dict[str, Any], event: str, timeout: int) -> dict[str, Any]:
    proc = started_record["proc"]
    assert proc.stdout is not None
    deadline = time.monotonic() + timeout
    while True:
        remaining = deadline - time.monotonic()
        if remaining <= 0:
            stdout, stderr, returncode = terminate_worker(proc)
            started_record.update({"returncode": returncode, "stdout_after_timeout": stdout, "stderr": stderr, "timed_out": True, "timeout_seconds": timeout, "duration_seconds": round(time.time() - started_record["started"], 3)})
            raise DriverError(f"worker timed out before emitting {event}")
        ready, _, _ = select.select([proc.stdout], [], [], remaining)
        if not ready:
            continue
        line = proc.stdout.readline()
        if line == "":
            stderr = proc.stderr.read() if proc.stderr is not None else ""
            started_record.update({"returncode": proc.poll(), "stderr": stderr})
            raise DriverError(f"worker exited before emitting {event}: {stderr}")
        try:
            parsed = json.loads(line)
        except json.JSONDecodeError as exc:
            started_record.setdefault("json_parse_errors", []).append({"line": line.rstrip("\n"), "error": str(exc)})
            raise DriverError(f"worker emitted malformed JSON before {event}: {line.rstrip()}") from exc
        started_record.setdefault("ready_events", []).append(parsed)
        if parsed.get("event") == event:
            return parsed


def finish_started_worker(started_record: dict[str, Any], command: str, timeout: int) -> dict[str, Any]:
    proc = started_record["proc"]
    assert proc.stdin is not None
    started = started_record["started"]
    proc.stdin.write(command + "\n")
    proc.stdin.flush()
    proc.stdin.close()
    proc.stdin = None
    timed_out = False
    try:
        stdout, stderr = proc.communicate(timeout=timeout)
        returncode = proc.returncode
    except subprocess.TimeoutExpired:
        timed_out = True
        stdout, stderr, returncode = terminate_worker(proc)
    events, parse_errors = parse_worker_events(stdout)
    all_events = list(started_record.get("ready_events", [])) + events
    return {"argv": started_record["argv"], "returncode": returncode, "stdout": stdout, "stderr": stderr, "json_events": all_events, "json_parse_errors": list(started_record.get("json_parse_errors", [])) + parse_errors, "timed_out": timed_out, "timeout_seconds": timeout, "duration_seconds": round(time.time() - started, 3)}


def latest_event(record: dict[str, Any], event: str) -> dict[str, Any]:
    if record.get("timed_out"):
        raise DriverError(f"worker timed out before emitting {event}")
    if record.get("json_parse_errors"):
        raise DriverError(f"worker emitted malformed JSON before {event}: {record['json_parse_errors']!r}")
    events = record.get("json_events") or []
    if not events or events[-1].get("event") != event:
        raise DriverError(json.dumps({"message": f"worker did not emit {event}", "returncode": record.get("returncode"), "stderr": record.get("stderr")}, sort_keys=True))
    return events[-1]


def identity_args(worker: dict[str, Any]) -> list[str]:
    expected = worker["expected_process"]
    args = [
        "identity", "--role", expected["role"], "--pid", str(expected["pid"]), "--expected-sha256", expected["sha256"],
        "--expected-boot-id", expected["boot_id"], "--expected-start-ticks", str(expected["start_ticks"]),
        "--path", str(worker["mount"]), "--base-dir", str(worker["base_dir"]), "--worker-run-dir", str(worker["worker_run_dir"]),
    ]
    if expected.get("config_path") and expected.get("config_sha256"):
        args.extend(["--expected-config-path", expected["config_path"], "--expected-config-sha256", expected["config_sha256"]])
    return args


def remote_identity_ok(event: dict[str, Any], executable: str, backend: str, require_afs_mount: bool = True) -> bool:
    identity = event.get("process_identity") or {}
    mount = mount_record(event.get("mount_identity") or {})
    base = event.get("base_dir") or {}
    mount_ok = bool(mount) and mount.get("source") == EXPECTED_SOURCES[backend] and str(mount.get("fstype", "")).startswith("fuse")
    return process_identity_ok(identity, executable) and (mount_ok if require_afs_mount else True) and base.get("ok") is True and event.get("platform", {}).get("system") == "Linux"


def normalize_meta_store(value: Any) -> str | None:
    aliases = {
        "memory": "memory",
        "mem": "memory",
        "in-memory": "memory",
        "in_memory": "memory",
        "etcd": "etcd",
        "redis": "redis",
        "local-file": "local-file",
        "local_file": "local-file",
        "file": "local-file",
    }
    if value is None:
        return None
    return aliases.get(str(value).strip().lower())


def config_facts(event: dict[str, Any]) -> dict[str, str]:
    identity = event.get("process_identity") or {}
    config = identity.get("config_identity") if isinstance(identity, dict) else None
    facts = config.get("facts") if isinstance(config, dict) else None
    return facts if isinstance(facts, dict) else {}


def matrix_identity_event(label: str, ok: bool, expected: str | None, observed: str | None, source: str) -> dict[str, Any]:
    return {"label": label, "ok": ok, "expected": expected, "observed": observed, "source": source}


def remote_matrix_identity(matrix: dict[str, str], pre_events: dict[str, dict[str, Any]]) -> dict[str, Any]:
    expected_meta = normalize_meta_store(matrix.get("meta"))
    observed_meta = normalize_meta_store(config_facts(pre_events["ctl_worker"]).get("meta_store"))
    requested_transport = matrix.get("transport")
    writer_mode = config_facts(pre_events["writer_worker"]).get("data_mode")
    reader_mode = config_facts(pre_events["reader_worker"]).get("data_mode")
    checks = [
        matrix_identity_event("meta-store", expected_meta == observed_meta and expected_meta is not None, expected_meta, observed_meta, "ctl_worker config meta_store"),
        matrix_identity_event("writer-data-mode", requested_transport == writer_mode and requested_transport not in {None, "", "auto"}, requested_transport, writer_mode, "writer_worker config data_mode"),
        matrix_identity_event("reader-data-mode", requested_transport == reader_mode and requested_transport not in {None, "", "auto"}, requested_transport, reader_mode, "reader_worker config data_mode"),
    ]
    return {"ok": all(item["ok"] for item in checks), "checks": checks, "config_facts": {label: config_facts(event) for label, event in pre_events.items()}}


def run_remote_host(case_id: str, profile: str, matrix: dict[str, str], binding: dict[str, Any], artifacts: Path, run_dir: Path) -> dict[str, Any]:
    checks: list[dict[str, Any]] = []
    records: dict[str, Any] = {"started_at": utc(), "mode": "remote-host", "case_id": case_id, "expected_meta_endpoint": binding["expected_meta_endpoint"]}
    timeout = binding["timeout"]
    pre: dict[str, Any] = {}
    post: dict[str, Any] = {}
    for label in ("ctl_worker", "writer_worker", "reader_worker"):
        worker = binding[label]
        pre[label] = run_worker_json(worker, identity_args(worker), timeout)
    records["identity_pre"] = pre
    pre_events = {label: latest_event(record, "IDENTITY") for label, record in pre.items()}
    identity_ok = (
        remote_identity_ok(pre_events["ctl_worker"], "afs-meta", binding["backend"], require_afs_mount=False)
        and remote_identity_ok(pre_events["writer_worker"], "afs-node", binding["backend"])
        and remote_identity_ok(pre_events["reader_worker"], "afs-node", binding["backend"])
    )
    checks.append(build_check("remote-identity-preflight", "PASS" if identity_ok else "BLOCKED", pre_events))
    matrix_identity = remote_matrix_identity(matrix, pre_events)
    records["matrix_identity"] = matrix_identity
    checks.append(build_check("remote-matrix-identity", "PASS" if matrix_identity["ok"] else "BLOCKED", matrix_identity))
    if not identity_ok or not matrix_identity["ok"]:
        write_json(artifacts / "remote-worker-records.json", records)
        return finish_proof(case_id, profile, matrix, "BLOCKED", "remote visibility identity preflight failed", checks, records, artifacts, run_dir)

    if case_id == "FUN-02":
        fun02_records = []
        for target in binding["fun02_targets"]:
            worker = binding["writer_worker"] if target.get("worker") == "writer" else binding["reader_worker"]
            record = run_worker_json(worker, ["worker-fun02", "--label", target["label"], "--base-dir", str(target["base_dir"]), "--rel-path", target["rel_path"], "--timeout", str(timeout)], timeout + 5)
            event = latest_event(record, "FUN02")
            fun02_records.append({"label": target["label"], "worker": target.get("worker"), "record": record, "event": event})
        records["fun02"] = fun02_records
        ok = all(item["event"].get("result", {}).get("accepted_dirty_visible_before_close_or_sync") is True for item in fun02_records)
        checks.append(build_check("fun02-same-mount-visible", "PASS" if ok else "FAIL", fun02_records))
    elif case_id == "FUN-03":
        payload_hex = f"fun03-remote-{uuid.uuid4().hex}".encode().hex()
        writer_record = run_worker_json(binding["writer_worker"], ["worker-write-close", "--base-dir", str(binding["writer_worker"]["base_dir"]), "--rel-path", binding["rel_path"], "--payload-hex", payload_hex], timeout)
        writer_event = latest_event(writer_record, "WRITE_CLOSE")
        writer_result = writer_event["result"]
        reader_record = run_worker_json(binding["reader_worker"], ["worker-read-check", "--base-dir", str(binding["reader_worker"]["base_dir"]), "--rel-path", binding["rel_path"], "--expected-sha256", writer_result["sha256"], "--expected-size", str(writer_result["size"])], timeout)
        reader_event = latest_event(reader_record, "READ_CHECK")
        records["fun03"] = {"writer": writer_event, "reader": reader_event, "barrier": "reader worker invoked only after writer close event returned"}
        ok = writer_result.get("close_success") is True and reader_event.get("result", {}).get("ok") is True
        checks.append(build_check("fun03-close-to-open", "PASS" if ok else "FAIL", records["fun03"]))
    else:
        lanes = []
        for sync_kind in ("fdatasync", "fsync"):
            lane_rel = safe_rel_path(f"{binding['rel_path']}.{sync_kind}")
            initial_hex = f"fun04-initial-{sync_kind}-remote-{uuid.uuid4().hex}".encode().hex()
            initial_record = run_worker_json(binding["writer_worker"], ["worker-sync", "--base-dir", str(binding["writer_worker"]["base_dir"]), "--rel-path", lane_rel, "--payload-hex", initial_hex, "--sync-kind", "fsync"], timeout)
            initial_event = latest_event(initial_record, "SYNC_WRITE")
            initial_result = initial_event["result"]
            initial_reader_record = run_worker_json(binding["reader_worker"], ["worker-read-check", "--base-dir", str(binding["reader_worker"]["base_dir"]), "--rel-path", lane_rel, "--expected-sha256", initial_result["sha256"], "--expected-size", str(initial_result["size"]), "--expected-head", initial_result["head"]], timeout)
            initial_reader_event = latest_event(initial_reader_record, "READ_CHECK")
            payload_hex = f"fun04-{sync_kind}-remote-{uuid.uuid4().hex}".encode().hex()
            hold_record = None
            old_ready = None
            if binding.get("old_reader_fd_observation"):
                hold_record = start_worker_json(binding["reader_worker"], ["worker-oldfd-hold", "--base-dir", str(binding["reader_worker"]["base_dir"]), "--rel-path", lane_rel])
                old_ready = read_worker_event(hold_record, "OLD_FD_READY", timeout)
            writer_record = run_worker_json(binding["writer_worker"], ["worker-sync", "--base-dir", str(binding["writer_worker"]["base_dir"]), "--rel-path", lane_rel, "--payload-hex", payload_hex, "--sync-kind", sync_kind], timeout)
            writer_event = latest_event(writer_record, "SYNC_WRITE")
            writer_result = writer_event["result"]
            old_fd = None
            if hold_record is not None:
                old_record = finish_started_worker(hold_record, "READ", timeout)
                old_fd = latest_event(old_record, "OLD_FD_OBSERVATION")
            reader_record = run_worker_json(binding["reader_worker"], ["worker-read-check", "--base-dir", str(binding["reader_worker"]["base_dir"]), "--rel-path", lane_rel, "--expected-sha256", writer_result["sha256"], "--expected-size", str(writer_result["size"]), "--expected-head", writer_result["head"]], timeout)
            reader_event = latest_event(reader_record, "READ_CHECK")
            initial_ok = initial_reader_event.get("result", {}).get("ok") is True
            old_ok = old_fd is None or old_fd.get("result", {}).get("opened") is True
            lanes.append({"sync_kind": sync_kind, "rel_path": lane_rel, "initial_writer": initial_event, "initial_reader": initial_reader_event, "old_fd_ready": old_ready, "writer": writer_event, "reader": reader_event, "old_fd_observation": old_fd, "ok": initial_ok and old_ok and reader_event.get("result", {}).get("ok") is True})
        records["fun04"] = lanes
        ok = all(lane["ok"] for lane in lanes)
        checks.append(build_check("fun04-sync-to-reopen", "PASS" if ok else "FAIL", lanes))

    for label in ("ctl_worker", "writer_worker", "reader_worker"):
        worker = binding[label]
        post[label] = run_worker_json(worker, identity_args(worker), timeout)
    records["identity_post"] = post
    post_events = {label: latest_event(record, "IDENTITY") for label, record in post.items()}
    post_ok = (
        remote_identity_ok(post_events["ctl_worker"], "afs-meta", binding["backend"], require_afs_mount=False)
        and remote_identity_ok(post_events["writer_worker"], "afs-node", binding["backend"])
        and remote_identity_ok(post_events["reader_worker"], "afs-node", binding["backend"])
    )
    checks.append(build_check("remote-identity-postflight", "PASS" if post_ok else "BLOCKED", post_events))
    status = "PASS" if ok and post_ok else ("BLOCKED" if not post_ok else "FAIL")
    reason = "" if status == "PASS" else ("remote visibility identity postflight failed" if not post_ok else f"{case_id} visibility check failed")
    write_json(artifacts / "remote-worker-records.json", records)
    return finish_proof(case_id, profile, matrix, status, reason, checks, records, artifacts, run_dir)


def finish_proof(case_id: str, profile: str, matrix: dict[str, str], status: str, reason: str, checks: list[dict[str, Any]], records: dict[str, Any], artifacts: Path, run_dir: Path) -> dict[str, Any]:
    write_json(artifacts / "records.json", records)
    proof = {
        "case_id": case_id,
        "profile": profile,
        "matrix": matrix,
        "status": status,
        "reason": reason,
        "checks": checks,
        "coverage": {"profile": profile, "visibility_contracts": {case_id: "real FUSE/path operations when bound to mounted AFS targets"}},
        "artifacts": {"root": rel(artifacts, run_dir), "manifest": []},
        "notes": ["FUN-04 old reader fd is observed-only when enabled; this driver does not claim a permanent old-version contract."],
    }
    proof_path = artifacts / "proof.json"
    proof["artifacts"]["manifest"] = artifact_manifest(artifacts, run_dir, exclude={proof_path})
    write_json(proof_path, proof)
    return proof


def blocked_proof(case_id: str, profile: str, matrix: dict[str, str], reason: str) -> dict[str, Any]:
    return {"case_id": case_id, "profile": profile, "matrix": matrix, "status": "BLOCKED", "reason": reason, "checks": [{"name": "visibility-driver", "status": "BLOCKED", "evidence": reason}]}


def identity_main(argv: list[str]) -> int:
    parser = argparse.ArgumentParser(description="collect visibility driver strict identity")
    parser.add_argument("--role", choices=["meta", "node"], required=True)
    parser.add_argument("--pid", type=int, required=True)
    parser.add_argument("--expected-sha256", required=True)
    parser.add_argument("--expected-boot-id", required=True)
    parser.add_argument("--expected-start-ticks", type=int, required=True)
    parser.add_argument("--expected-config-path")
    parser.add_argument("--expected-config-sha256")
    parser.add_argument("--path", type=Path, required=True)
    parser.add_argument("--base-dir", type=Path, required=True)
    parser.add_argument("--worker-run-dir", type=Path, required=True)
    args = parser.parse_args(argv)
    identity = process_identity(args.pid, args.role, args.expected_sha256, args.expected_boot_id, args.expected_start_ticks)
    if args.expected_config_path and args.expected_config_sha256:
        attach_config_identity(identity, args.expected_config_path, args.expected_config_sha256)
    executable = "afs-meta" if args.role == "meta" else "afs-node"
    identity["ok"] = process_identity_ok(identity, executable)
    base_ok, base_evidence = check_base_dir(args.base_dir, args.path)
    event = {
        "event": "IDENTITY",
        "platform": {"system": platform.system(), "machine": platform.machine(), "python": platform.python_version()},
        "process_identity": identity,
        "mount_identity": mount_identity(args.path),
        "base_dir": {"ok": base_ok, **base_evidence},
        "worker_run_dir": str(args.worker_run_dir),
        "worker_run_dir_mount": mount_identity(args.worker_run_dir),
    }
    emit(event)
    return 0 if identity["ok"] and base_ok else 1


def worker_fun02_main(argv: list[str]) -> int:
    parser = argparse.ArgumentParser(description="run FUN-02 on one worker")
    parser.add_argument("--label", required=True)
    parser.add_argument("--base-dir", type=Path, required=True)
    parser.add_argument("--rel-path", required=True)
    parser.add_argument("--timeout", type=int, default=DEFAULT_TIMEOUT)
    args = parser.parse_args(argv)
    result = run_fun02_target(args.base_dir, safe_rel_path(args.rel_path), args.timeout)
    emit({"event": "FUN02", "label": args.label, "result": result})
    return 0


def worker_write_close_main(argv: list[str]) -> int:
    parser = argparse.ArgumentParser(description="write and close")
    parser.add_argument("--base-dir", type=Path, required=True)
    parser.add_argument("--rel-path", required=True)
    parser.add_argument("--payload-hex", required=True)
    args = parser.parse_args(argv)
    result = write_close(join_base(args.base_dir, args.rel_path), bytes.fromhex(args.payload_hex))
    emit({"event": "WRITE_CLOSE", "result": result})
    return 0


def worker_sync_main(argv: list[str]) -> int:
    parser = argparse.ArgumentParser(description="write and sync")
    parser.add_argument("--base-dir", type=Path, required=True)
    parser.add_argument("--rel-path", required=True)
    parser.add_argument("--payload-hex", required=True)
    parser.add_argument("--sync-kind", choices=["fdatasync", "fsync"], required=True)
    args = parser.parse_args(argv)
    result = write_sync(join_base(args.base_dir, args.rel_path), bytes.fromhex(args.payload_hex), args.sync_kind)
    emit({"event": "SYNC_WRITE", "result": result})
    return 0


def worker_read_check_main(argv: list[str]) -> int:
    parser = argparse.ArgumentParser(description="fresh open/read/stat check")
    parser.add_argument("--base-dir", type=Path, required=True)
    parser.add_argument("--rel-path", required=True)
    parser.add_argument("--expected-sha256", required=True)
    parser.add_argument("--expected-size", type=int, required=True)
    parser.add_argument("--expected-head")
    args = parser.parse_args(argv)
    result = fresh_read_check(args.base_dir, safe_rel_path(args.rel_path), args.expected_sha256, args.expected_size, args.expected_head)
    emit({"event": "READ_CHECK", "result": result})
    return 0 if result["ok"] else 1


def worker_oldfd_main(argv: list[str]) -> int:
    parser = argparse.ArgumentParser(description="observe old fd contract")
    parser.add_argument("--base-dir", type=Path, required=True)
    parser.add_argument("--rel-path", required=True)
    args = parser.parse_args(argv)
    result = old_fd_observation(args.base_dir, safe_rel_path(args.rel_path))
    emit({"event": "OLD_FD_OBSERVATION", "result": result})
    return 0 if result.get("opened") else 1


def worker_oldfd_hold_main(argv: list[str]) -> int:
    parser = argparse.ArgumentParser(description="hold an existing old fd until the controller releases the application barrier")
    parser.add_argument("--base-dir", type=Path, required=True)
    parser.add_argument("--rel-path", required=True)
    args = parser.parse_args(argv)
    path = join_base(args.base_dir, safe_rel_path(args.rel_path))
    fd = os.open(path, os.O_RDONLY)
    try:
        before = fd_digest(fd)
        emit({"event": "OLD_FD_READY", "result": {"enabled": True, "opened": True, "path": str(path), "before": before, "contract": OLD_FD_CONTRACT}})
        command = sys.stdin.readline().strip()
        if command != "READ":
            emit({"event": "OLD_FD_OBSERVATION", "result": {"enabled": True, "opened": True, "path": str(path), "before": before, "error": f"unexpected command {command!r}", "contract": OLD_FD_CONTRACT}})
            return 2
        after = fd_digest(fd)
        emit({"event": "OLD_FD_OBSERVATION", "result": {"enabled": True, "opened": True, "path": str(path), "before": before, "after": after, "contract": OLD_FD_CONTRACT}})
        return 0
    finally:
        os.close(fd)


def run(env: dict[str, str]) -> int:
    case_id, profile, matrix, run_dir, binding_path = required_context(env)
    document = json.loads(binding_path.read_text(encoding="utf-8"))
    binding = validate_binding(select_binding(document, case_id, profile, matrix), case_id, profile, matrix)
    artifacts = run_dir / "artifacts" / f"{case_id.lower()}-visibility"
    artifacts.mkdir(parents=True, exist_ok=True)
    proof = run_remote_host(case_id, profile, matrix, binding, artifacts, run_dir) if binding["mode"] == "remote-host" else run_local(case_id, profile, matrix, binding, artifacts, run_dir)
    emit(proof)
    return 0 if proof.get("status") == "PASS" else 1


def main(argv: list[str] | None = None) -> int:
    argv = list(sys.argv[1:] if argv is None else argv)
    try:
        if argv and argv[0] == "fun02-writer":
            return fun02_writer_main(argv[1:])
        if argv and argv[0] == "identity":
            return identity_main(argv[1:])
        if argv and argv[0] == "worker-fun02":
            return worker_fun02_main(argv[1:])
        if argv and argv[0] == "worker-write-close":
            return worker_write_close_main(argv[1:])
        if argv and argv[0] == "worker-sync":
            return worker_sync_main(argv[1:])
        if argv and argv[0] == "worker-read-check":
            return worker_read_check_main(argv[1:])
        if argv and argv[0] == "worker-oldfd-observe":
            return worker_oldfd_main(argv[1:])
        if argv and argv[0] == "worker-oldfd-hold":
            return worker_oldfd_hold_main(argv[1:])
        return run(os.environ)
    except (DriverError, OSError, json.JSONDecodeError, ValueError) as exc:
        case_id = os.environ.get("AFS_ACCEPTANCE_CASE_ID", "UNKNOWN")
        profile = os.environ.get("AFS_ACCEPTANCE_PROFILE", "unknown")
        try:
            matrix = matrix_from_env(os.environ)
        except BindingError:
            matrix = {}
        status = getattr(exc, "status", "BLOCKED")
        proof = blocked_proof(case_id, profile, matrix, f"visibility driver {status.lower()}: {type(exc).__name__}: {exc}")
        proof["status"] = status if status in STATUS_VALUES else "BLOCKED"
        proof["traceback"] = traceback.format_exc()
        emit(proof)
        return 1


if __name__ == "__main__":
    raise SystemExit(main())
