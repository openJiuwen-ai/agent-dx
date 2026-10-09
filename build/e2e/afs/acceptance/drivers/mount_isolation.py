#!/usr/bin/env python3
"""FUN-13 live mount isolation driver.

The acceptance runner owns case/profile/matrix/run-dir selection.  This driver
binds that fixed context to two already-mounted AFS FUSE targets on one node
and proves that identical relative paths, fd operations, permissions and
errors remain scoped to the selected mount identity.
"""
from __future__ import annotations

import argparse
import datetime as dt
import errno
import hashlib
import json
import os
import platform
import pwd
import stat
import subprocess
import sys
import traceback
import uuid
from pathlib import Path
from typing import Any


STATUS_VALUES = {"PASS", "FAIL", "BLOCKED", "INCONCLUSIVE", "EXCLUDED"}
EXPECTED_SOURCES = {"OwnerFs": "afs-ownerfs", "DFS": "afs-dfs"}
PEER_BACKEND = {"OwnerFs": "DFS", "DFS": "OwnerFs"}


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


def read_boot_id() -> str | None:
    try:
        return Path("/proc/sys/kernel/random/boot_id").read_text(encoding="utf-8").strip()
    except Exception:  # noqa: BLE001 - absence is captured as identity mismatch
        return None


def read_start_ticks(proc: Path) -> int | None:
    try:
        stat_text = proc.joinpath("stat").read_text(encoding="utf-8")
        after_comm = stat_text.rsplit(")", 1)[1].strip().split()
        return int(after_comm[19])
    except Exception:  # noqa: BLE001 - absence is captured as identity mismatch
        return None


def process_identity(
    pid: int,
    role: str,
    expected_sha256: str | None = None,
    expected_boot_id: str | None = None,
    expected_start_ticks: int | None = None,
) -> dict[str, Any]:
    proc = Path("/proc") / str(pid)
    observed_boot_id = read_boot_id()
    observed_start_ticks = read_start_ticks(proc) if proc.exists() else None
    result: dict[str, Any] = {
        "role": role,
        "pid": pid,
        "exists": proc.exists(),
        "expected_sha256": expected_sha256,
        "boot_id": observed_boot_id,
        "expected_boot_id": expected_boot_id,
        "boot_id_ok": bool(expected_boot_id and observed_boot_id == expected_boot_id),
        "start_ticks": observed_start_ticks,
        "expected_start_ticks": expected_start_ticks,
        "start_ticks_ok": bool(expected_start_ticks is not None and observed_start_ticks == expected_start_ticks),
    }
    try:
        exe = proc.joinpath("exe").resolve()
        result["exe_path"] = str(exe)
        result["sha256"] = sha256_file(exe) if exe.is_file() else None
    except Exception as exc:  # noqa: BLE001
        result["exe_error"] = f"{type(exc).__name__}: {exc}"
    try:
        cmdline_items = [item.decode(errors="replace") for item in proc.joinpath("cmdline").read_bytes().split(b"\0") if item]
        result["cmdline"] = " ".join(cmdline_items)
        result["cmdline_items"] = cmdline_items
    except Exception as exc:  # noqa: BLE001
        result["cmdline_error"] = f"{type(exc).__name__}: {exc}"
    result["sha256_ok"] = bool(expected_sha256 and result.get("sha256") == expected_sha256)
    return result


def config_path_from_cmdline(items: list[str]) -> str | None:
    for index, item in enumerate(items):
        if item == "--config" and index + 1 < len(items):
            return items[index + 1]
        if item.startswith("--config="):
            return item.split("=", 1)[1]
    return None


def attach_config_identity(identity: dict[str, Any], expected_path: str | None, expected_sha256: str | None) -> dict[str, Any]:
    if expected_path is None and expected_sha256 is None:
        identity.pop("config_identity", None)
        return identity
    items = identity.get("cmdline_items")
    observed_path = config_path_from_cmdline(items if isinstance(items, list) else [])
    config: dict[str, Any] = {"observed_path": observed_path, "expected_path": expected_path, "expected_sha256": expected_sha256}
    if observed_path:
        path = Path(observed_path)
        config["exists"] = path.is_file()
        if path.is_file():
            config["sha256"] = sha256_file(path)
    config["path_ok"] = bool(expected_path and observed_path == expected_path)
    config["sha256_ok"] = bool(expected_sha256 and config.get("sha256") == expected_sha256)
    identity["config_identity"] = config
    return identity


def process_identity_ok(identity: dict[str, Any], executable: str) -> bool:
    if identity.get("sha256_ok") is not True:
        return False
    if identity.get("boot_id_ok") is not True or identity.get("start_ticks_ok") is not True:
        return False
    if Path(str(identity.get("exe_path", ""))).name != executable:
        return False
    config = identity.get("config_identity")
    if config is None:
        return True
    return config.get("path_ok") is True and config.get("sha256_ok") is True


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


def validate_pid(value: Any, name: str) -> int:
    return validate_positive_int(value, name)


def validate_boot_id(value: Any, name: str) -> str:
    if not isinstance(value, str):
        raise BindingError(f"{name} must be a Linux boot_id string")
    parts = value.split("-")
    if [len(part) for part in parts] != [8, 4, 4, 4, 12] or any(ch not in "0123456789abcdef" for part in parts for ch in part):
        raise BindingError(f"{name} must be a lowercase Linux boot_id UUID")
    return value


def validate_abs_path(value: Any, name: str) -> str:
    if not isinstance(value, str) or not value.startswith("/"):
        raise BindingError(f"{name} must be an absolute Linux path")
    if value.startswith(("/Users/", "/Volumes/", "/private/", "/var/folders/")):
        raise BindingError(f"{name} must not be a macOS host path")
    return value


def path_within(path: Path, root: Path) -> bool:
    try:
        path.relative_to(root)
        return True
    except ValueError:
        return False


def validate_base_dir(base: Path, mount: Path, name: str, mount_name: str) -> Path:
    if not path_within(base, mount):
        raise BindingError(f"{name} must be {mount_name} or a descendant of {mount_name}")
    return base


def validate_ssh_host(value: Any, name: str) -> str:
    if not isinstance(value, str) or not value or value.startswith("-") or any(ch.isspace() for ch in value):
        raise BindingError(f"{name} must be an ssh host alias without whitespace or options")
    return value


def local_expected_process(record: Any, role: str, name: str) -> dict[str, Any]:
    if not isinstance(record, dict):
        raise BindingError(f"{name} must be an object")
    unknown = sorted(set(record) - {"role", "pid", "sha256", "boot_id", "start_ticks", "config_path", "config_sha256"})
    if unknown:
        raise BindingError(f"unsupported {name} field(s): {', '.join(unknown)}")
    if record.get("role") != role:
        raise BindingError(f"{name}.role must be {role}")
    result = {
        "role": role,
        "pid": validate_pid(record.get("pid"), f"{name}.pid"),
        "sha256": validate_sha(record.get("sha256"), f"{name}.sha256"),
        "boot_id": validate_boot_id(record.get("boot_id"), f"{name}.boot_id"),
        "start_ticks": validate_positive_int(record.get("start_ticks"), f"{name}.start_ticks"),
    }
    if record.get("config_path") is not None or record.get("config_sha256") is not None:
        result["config_path"] = validate_abs_path(record.get("config_path"), f"{name}.config_path")
        result["config_sha256"] = validate_sha(record.get("config_sha256"), f"{name}.config_sha256")
    return result


def remote_worker(record: Any, name: str) -> dict[str, Any]:
    if not isinstance(record, dict):
        raise BindingError(f"{name} must be an object")
    unknown = sorted(set(record) - {"transport", "host", "ssh_config", "python", "driver", "expected_process"})
    if unknown:
        raise BindingError(f"unsupported {name} field(s): {', '.join(unknown)}")
    if record.get("transport") != "ssh":
        raise BindingError(f"{name}.transport must be ssh")
    expected = local_expected_process(record.get("expected_process"), "meta", f"{name}.expected_process")
    worker = {
        "transport": "ssh",
        "host": validate_ssh_host(record.get("host"), f"{name}.host"),
        "python": validate_abs_path(record.get("python"), f"{name}.python"),
        "driver": validate_abs_path(record.get("driver"), f"{name}.driver"),
        "expected_process": expected,
    }
    if record.get("ssh_config") is not None:
        worker["ssh_config"] = validate_abs_path(record.get("ssh_config"), f"{name}.ssh_config")
    return worker


def remote_identity(worker: dict[str, Any]) -> dict[str, Any]:
    expected = worker["expected_process"]
    prefix = ["ssh", "-o", "BatchMode=yes"]
    if worker.get("ssh_config"):
        prefix.extend(["-F", worker["ssh_config"]])
    prefix.extend(["--", worker["host"], worker["python"], worker["driver"]])
    argv = prefix + [
        "identity",
        "--role", "meta",
        "--pid", str(expected["pid"]),
        "--expected-sha256", expected["sha256"],
        "--expected-boot-id", expected["boot_id"],
        "--expected-start-ticks", str(expected["start_ticks"]),
    ]
    if expected.get("config_path") and expected.get("config_sha256"):
        argv.extend(["--expected-config-path", expected["config_path"], "--expected-config-sha256", expected["config_sha256"]])
    proc = subprocess.run(argv, shell=False, text=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE, timeout=15, check=False)
    result = {"argv": argv, "returncode": proc.returncode, "stdout": proc.stdout, "stderr": proc.stderr}
    proof = parse_json_tail(proc.stdout)
    if isinstance(proof, dict):
        result["identity"] = proof
    return result


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


def matrix_from_env() -> dict[str, str]:
    raw = os.environ.get("AFS_ACCEPTANCE_MATRIX", "{}")
    try:
        parsed = json.loads(raw)
    except json.JSONDecodeError as exc:
        raise BindingError(f"AFS_ACCEPTANCE_MATRIX is not valid JSON: {exc}") from exc
    if not isinstance(parsed, dict):
        raise BindingError("AFS_ACCEPTANCE_MATRIX must decode to an object")
    return {str(key): str(value) for key, value in parsed.items()}


def load_binding(path: Path, case_id: str, profile: str, matrix: dict[str, str]) -> dict[str, Any]:
    document = json.loads(path.read_text(encoding="utf-8"))
    if not isinstance(document, dict) or document.get("schema_version") != 1:
        raise BindingError("mount isolation binding file must be a schema_version 1 object")
    bindings = document.get("bindings")
    if not isinstance(bindings, list):
        raise BindingError("mount isolation binding file must contain bindings")
    matches: list[dict[str, Any]] = []
    for item in bindings:
        if not isinstance(item, dict):
            raise BindingError("each binding entry must be an object")
        if item.get("case_id") != case_id:
            continue
        item_profile = item.get("profile")
        if item_profile is not None and item_profile != profile:
            continue
        item_matrix = item.get("matrix")
        if isinstance(item_matrix, dict) and {str(k): str(v) for k, v in item_matrix.items()} == matrix:
            matches.append(item)
    if not matches:
        raise BindingError(f"no FUN-13 binding for exact matrix {json.dumps(matrix, sort_keys=True)}")
    if len(matches) > 1:
        raise BindingError(f"ambiguous FUN-13 bindings for exact matrix {json.dumps(matrix, sort_keys=True)}")
    return validate_binding(matches[0], matrix)


def validate_binding(binding: dict[str, Any], matrix: dict[str, str]) -> dict[str, Any]:
    allowed = {
        "case_id", "profile", "matrix", "owner_mount", "dfs_mount", "owner_base_dir", "dfs_base_dir",
        "node_process", "meta_process", "meta_worker", "access_uid", "access_gid",
    }
    unknown = sorted(set(binding) - allowed)
    if unknown:
        raise BindingError(f"unsupported binding field(s): {', '.join(unknown)}")
    backend = matrix.get("backend")
    if backend not in EXPECTED_SOURCES:
        raise BindingError("FUN-13 matrix backend must be OwnerFs or DFS")
    if matrix.get("meta") not in {"etcd", "Redis", "memory"}:
        raise BindingError("FUN-13 matrix meta must be etcd, Redis or memory")
    owner_mount = Path(validate_abs_path(binding.get("owner_mount"), "owner_mount"))
    dfs_mount = Path(validate_abs_path(binding.get("dfs_mount"), "dfs_mount"))
    if owner_mount == dfs_mount:
        raise BindingError("owner_mount and dfs_mount must be different paths")
    node = local_expected_process(binding.get("node_process"), "node", "node_process")
    meta_process = binding.get("meta_process")
    meta_worker = binding.get("meta_worker")
    if (meta_process is None) == (meta_worker is None):
        raise BindingError("exactly one of meta_process or meta_worker is required")
    owner_base = Path(validate_abs_path(binding.get("owner_base_dir"), "owner_base_dir")) if binding.get("owner_base_dir") else owner_mount
    dfs_base = Path(validate_abs_path(binding.get("dfs_base_dir"), "dfs_base_dir")) if binding.get("dfs_base_dir") else dfs_mount
    result = {
        "backend": backend,
        "owner_mount": owner_mount,
        "dfs_mount": dfs_mount,
        "owner_base_dir": validate_base_dir(owner_base, owner_mount, "owner_base_dir", "owner_mount"),
        "dfs_base_dir": validate_base_dir(dfs_base, dfs_mount, "dfs_base_dir", "dfs_mount"),
        "node_process": node,
        "meta_process": local_expected_process(meta_process, "meta", "meta_process") if meta_process is not None else None,
        "meta_worker": remote_worker(meta_worker, "meta_worker") if meta_worker is not None else None,
    }
    if binding.get("access_uid") is None:
        nobody = pwd.getpwnam("nobody")
        result["access_uid"] = nobody.pw_uid
        result["access_gid"] = nobody.pw_gid
    else:
        result["access_uid"] = validate_pid(binding.get("access_uid"), "access_uid")
        result["access_gid"] = validate_pid(binding.get("access_gid", binding.get("access_uid")), "access_gid")
    return result


def check_mount(record: dict[str, Any], backend: str, expected_target: Path) -> tuple[bool, str]:
    source = EXPECTED_SOURCES[backend]
    if not record:
        return False, "findmnt did not return exactly one mount record"
    if record.get("target") != str(expected_target):
        return False, f"mount target mismatch: expected {expected_target}, observed {record.get('target')}"
    if record.get("source") != source:
        return False, f"mount source mismatch: expected {source}, observed {record.get('source')}"
    if not str(record.get("fstype", "")).startswith("fuse"):
        return False, f"mount fstype is not FUSE: {record.get('fstype')}"
    return True, "observed expected AFS FUSE mount"


def path_is_within_mount(path: Path, mount: Path) -> tuple[bool, str]:
    try:
        resolved_path = path.resolve(strict=True)
        resolved_mount = mount.resolve(strict=True)
    except FileNotFoundError as exc:
        return False, f"base or mount path does not exist: {exc.filename}"
    except Exception as exc:  # noqa: BLE001
        return False, f"base or mount path cannot be resolved: {type(exc).__name__}: {exc}"
    if resolved_path == resolved_mount or resolved_mount in resolved_path.parents:
        return True, "base resolves under expected mount target"
    return False, f"base path escapes mount target after resolving symlinks: base={resolved_path} mount={resolved_mount}"


def check_base(record: dict[str, Any], backend: str, expected_mount: Path, base_dir: Path) -> tuple[bool, str]:
    mount_ok, mount_reason = check_mount(record, backend, expected_mount)
    if not mount_ok:
        return False, f"base findmnt mismatch for {base_dir}: {mount_reason}"
    path_ok, path_reason = path_is_within_mount(base_dir, expected_mount)
    if not path_ok:
        return False, path_reason
    return True, "base is inside the expected AFS FUSE mount and resolves to the same mount source/fstype"


def safe_join(root: Path, rel_path: str) -> Path:
    rel = Path(rel_path)
    if rel.is_absolute() or ".." in rel.parts:
        raise DriverFail(f"unsafe relative test path: {rel_path}")
    return root / rel


def stat_record(path: Path) -> dict[str, Any]:
    st = os.stat(path)
    return {
        "path": str(path),
        "dev": st.st_dev,
        "ino": st.st_ino,
        "mode": stat.S_IMODE(st.st_mode),
        "uid": st.st_uid,
        "gid": st.st_gid,
        "size": st.st_size,
    }


def read_all(path: Path) -> bytes:
    with path.open("rb") as handle:
        return handle.read()


def errno_name(value: int | None) -> str | None:
    return errno.errorcode.get(value, str(value)) if value is not None else None


def child_access(path_private: Path, path_public: Path, uid: int, gid: int) -> dict[str, Any]:
    code = (
        "import errno,json,os,sys\n"
        "uid=int(sys.argv[1]); gid=int(sys.argv[2]); private=sys.argv[3]; public=sys.argv[4]\n"
        "os.setgid(gid); os.setuid(uid)\n"
        "def read(path):\n"
        "    try:\n"
        "        data=open(path,'rb').read()\n"
        "        return {'ok': True, 'errno': None, 'errno_name': None, 'sha256': __import__('hashlib').sha256(data).hexdigest(), 'length': len(data)}\n"
        "    except OSError as exc:\n"
        "        return {'ok': False, 'errno': exc.errno, 'errno_name': errno.errorcode.get(exc.errno, str(exc.errno))}\n"
        "print(json.dumps({'uid': os.getuid(), 'gid': os.getgid(), 'private': read(private), 'public': read(public)}, sort_keys=True))\n"
    )
    proc = subprocess.run([sys.executable, "-c", code, str(uid), str(gid), str(path_private), str(path_public)], shell=False, text=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE, check=False, timeout=10)
    return {"argv": [sys.executable, "-c", "<uid-access>", str(uid), str(gid), str(path_private), str(path_public)], "returncode": proc.returncode, "stdout": proc.stdout, "stderr": proc.stderr, "result": parse_json_tail(proc.stdout)}


def artifact_manifest(root: Path, run_dir: Path) -> list[dict[str, Any]]:
    entries: list[dict[str, Any]] = []
    for path in sorted(p for p in root.rglob("*") if p.is_file()):
        entries.append({
            "path": str(path.relative_to(run_dir)),
            "bytes": path.stat().st_size,
            "sha256": sha256_file(path),
        })
    return entries


def build_check(name: str, status: str, evidence: Any, artifact: str | None = None) -> dict[str, Any]:
    check = {"name": name, "status": status, "evidence": evidence}
    if artifact:
        check["artifact"] = artifact
    return check


def run_operations(args: argparse.Namespace, binding: dict[str, Any], matrix: dict[str, str], artifacts: Path, run_dir: Path) -> tuple[str, str, list[dict[str, Any]], dict[str, Any]]:
    backend = matrix["backend"]
    peer = PEER_BACKEND[backend]
    if not path_within(binding["owner_base_dir"], binding["owner_mount"]):
        raise BindingError("owner_base_dir must be owner_mount or a descendant of owner_mount")
    if not path_within(binding["dfs_base_dir"], binding["dfs_mount"]):
        raise BindingError("dfs_base_dir must be dfs_mount or a descendant of dfs_mount")
    primary_mount = binding["owner_mount"] if backend == "OwnerFs" else binding["dfs_mount"]
    peer_mount = binding["dfs_mount"] if backend == "OwnerFs" else binding["owner_mount"]
    primary_base = binding["owner_base_dir"] if backend == "OwnerFs" else binding["dfs_base_dir"]
    peer_base = binding["dfs_base_dir"] if backend == "OwnerFs" else binding["owner_base_dir"]

    owner_mnt_cmd = mount_identity(binding["owner_mount"])
    dfs_mnt_cmd = mount_identity(binding["dfs_mount"])
    owner_base_mnt_cmd = mount_identity(binding["owner_base_dir"])
    dfs_base_mnt_cmd = mount_identity(binding["dfs_base_dir"])
    owner_record = mount_record(owner_mnt_cmd)
    dfs_record = mount_record(dfs_mnt_cmd)
    owner_base_record = mount_record(owner_base_mnt_cmd)
    dfs_base_record = mount_record(dfs_base_mnt_cmd)
    owner_ok, owner_reason = check_mount(owner_record, "OwnerFs", binding["owner_mount"])
    dfs_ok, dfs_reason = check_mount(dfs_record, "DFS", binding["dfs_mount"])
    owner_base_ok, owner_base_reason = check_mount(owner_base_record, "OwnerFs", binding["owner_mount"])
    dfs_base_ok, dfs_base_reason = check_mount(dfs_base_record, "DFS", binding["dfs_mount"])

    node_identity = attach_config_identity(
        process_identity(
            binding["node_process"]["pid"],
            "node",
            binding["node_process"]["sha256"],
            binding["node_process"]["boot_id"],
            binding["node_process"]["start_ticks"],
        ),
        binding["node_process"].get("config_path"),
        binding["node_process"].get("config_sha256"),
    )
    if binding["meta_process"] is not None:
        meta_identity = attach_config_identity(
            process_identity(
                binding["meta_process"]["pid"],
                "meta",
                binding["meta_process"]["sha256"],
                binding["meta_process"]["boot_id"],
                binding["meta_process"]["start_ticks"],
            ),
            binding["meta_process"].get("config_path"),
            binding["meta_process"].get("config_sha256"),
        )
        meta_identity_ok = process_identity_ok(meta_identity, "afs-meta")
        meta_identity_record: Any = meta_identity
    else:
        remote = remote_identity(binding["meta_worker"])
        meta_identity_record = remote
        identity = remote.get("identity") if isinstance(remote.get("identity"), dict) else {}
        meta_identity_ok = remote.get("returncode") == 0 and process_identity_ok(identity, "afs-meta")

    identity_checks = {
        "system": platform.system(),
        "owner_mount": owner_record,
        "dfs_mount": dfs_record,
        "owner_base_mount": owner_base_record,
        "dfs_base_mount": dfs_base_record,
        "owner_mount_command": owner_mnt_cmd,
        "dfs_mount_command": dfs_mnt_cmd,
        "owner_base_mount_command": owner_base_mnt_cmd,
        "dfs_base_mount_command": dfs_base_mnt_cmd,
        "owner_mount_ok": owner_ok,
        "owner_mount_reason": owner_reason,
        "dfs_mount_ok": dfs_ok,
        "dfs_mount_reason": dfs_reason,
        "owner_base_mount_ok": owner_base_ok,
        "owner_base_mount_reason": owner_base_reason,
        "dfs_base_mount_ok": dfs_base_ok,
        "dfs_base_mount_reason": dfs_base_reason,
        "node_identity": node_identity,
        "meta_identity": meta_identity_record,
        "node_identity_ok": process_identity_ok(node_identity, "afs-node"),
        "meta_identity_ok": meta_identity_ok,
    }

    if platform.system() != "Linux":
        raise BindingError("FUN-13 driver must run on Linux")
    if os.geteuid() != 0:
        raise BindingError("FUN-13 requires root to perform real UID access checks")
    if not (owner_ok and dfs_ok and owner_base_ok and dfs_base_ok):
        raise BindingError("owner/dfs mount identity did not match expected AFS FUSE mounts")
    if not identity_checks["node_identity_ok"]:
        raise BindingError("node process identity did not match expected afs-node pid and sha256")
    if not meta_identity_ok:
        raise BindingError("meta process identity did not match expected afs-meta pid and sha256")

    run_id = uuid.uuid4().hex
    rel_dir = f".afs-fun13-{run_id}"
    rel_file = f"{rel_dir}/same/path/data.bin"
    rel_error = f"{rel_dir}/same/path/error-target"

    primary_path = safe_join(primary_base, rel_file)
    peer_path = safe_join(peer_base, rel_file)
    primary_error = safe_join(primary_base, rel_error)
    peer_error = safe_join(peer_base, rel_error)
    for path in sorted({primary_path.parent, peer_path.parent, primary_error.parent, peer_error.parent}):
        path.mkdir(parents=True, exist_ok=False)
        path.chmod(0o755)

    primary_initial = (f"FUN13:{backend}:primary:{run_id}:".encode() + b"A" * 97)
    peer_initial = (f"FUN13:{peer}:peer:{run_id}:".encode() + b"B" * 131)
    primary_patch = b"primary-pwrite-patch"
    peer_patch = b"peer-pwrite-patch-longer"
    operations: list[dict[str, Any]] = []

    fd_primary = os.open(primary_path, os.O_CREAT | os.O_EXCL | os.O_RDWR, 0o600)
    fd_peer = os.open(peer_path, os.O_CREAT | os.O_EXCL | os.O_RDWR, 0o644)
    try:
        os.write(fd_primary, primary_initial)
        os.write(fd_peer, peer_initial)
        os.fchmod(fd_primary, 0o600)
        os.fchmod(fd_peer, 0o644)
        os.pwrite(fd_primary, primary_patch, 16)
        os.pwrite(fd_peer, peer_patch, 16)
        os.fsync(fd_primary)
        os.fsync(fd_peer)
        primary_fd_data = os.pread(fd_primary, 4096, 0)
        peer_fd_data = os.pread(fd_peer, 4096, 0)
        operations.append({"op": "fd-pwrite-pread", "primary_fd": fd_primary, "peer_fd": fd_peer, "primary_sha256": sha256_bytes(primary_fd_data), "peer_sha256": sha256_bytes(peer_fd_data)})
    finally:
        os.close(fd_primary)
        os.close(fd_peer)

    primary_reopen = read_all(primary_path)
    peer_reopen = read_all(peer_path)
    primary_stat = stat_record(primary_path)
    peer_stat = stat_record(peer_path)

    primary_error.mkdir(mode=0o755)
    peer_error.write_bytes(b"peer error path remains a file\n")
    error_primary_errno = None
    error_peer_errno = None
    try:
        os.open(primary_error, os.O_WRONLY)
    except OSError as exc:
        error_primary_errno = exc.errno
    else:
        raise DriverFail("opening primary directory as file unexpectedly succeeded")
    fd = None
    try:
        fd = os.open(peer_error, os.O_WRONLY)
        os.write(fd, b"ok")
    except OSError as exc:
        error_peer_errno = exc.errno
    finally:
        if fd is not None:
            os.close(fd)

    access = child_access(primary_path, peer_path, binding["access_uid"], binding["access_gid"])
    access_result = access.get("result") if isinstance(access.get("result"), dict) else {}

    content_ok = (
        primary_reopen == primary_fd_data
        and peer_reopen == peer_fd_data
        and primary_reopen != peer_reopen
        and primary_patch in primary_reopen
        and peer_patch in peer_reopen
        and peer_patch not in primary_reopen
        and primary_patch not in peer_reopen
    )
    mode_ok = primary_stat["mode"] == 0o600 and peer_stat["mode"] == 0o644
    error_ok = error_primary_errno == errno.EISDIR and error_peer_errno is None
    uid_ok = (
        access.get("returncode") == 0
        and access_result.get("uid") == binding["access_uid"]
        and access_result.get("gid") == binding["access_gid"]
        and access_result.get("private", {}).get("ok") is False
        and access_result.get("private", {}).get("errno") == errno.EACCES
        and access_result.get("public", {}).get("ok") is True
        and access_result.get("public", {}).get("sha256") == sha256_bytes(peer_reopen)
    )
    identity_tuple_primary = {
        "backend": backend,
        "mount_target": str(primary_mount),
        "mount_source": EXPECTED_SOURCES[backend],
        "dev": primary_stat["dev"],
        "ino": primary_stat["ino"],
    }
    identity_tuple_peer = {
        "backend": peer,
        "mount_target": str(peer_mount),
        "mount_source": EXPECTED_SOURCES[peer],
        "dev": peer_stat["dev"],
        "ino": peer_stat["ino"],
    }
    inode_collision = primary_stat["ino"] == peer_stat["ino"]
    collision_identity_ok = (
        inode_collision
        and identity_tuple_primary != identity_tuple_peer
        and identity_tuple_primary["mount_target"] != identity_tuple_peer["mount_target"]
        and identity_tuple_primary["mount_source"] != identity_tuple_peer["mount_source"]
        and primary_stat["mode"] != peer_stat["mode"]
        and sha256_bytes(primary_reopen) != sha256_bytes(peer_reopen)
    )

    records = {
        "created_at": utc(),
        "profile": args.profile,
        "matrix": matrix,
        "primary_backend": backend,
        "peer_backend": peer,
        "relative_path": rel_file,
        "relative_error_path": rel_error,
        "mount_identity": identity_checks,
        "path_identity": {
            "primary": identity_tuple_primary,
            "peer": identity_tuple_peer,
            "inode_numbers_collided": inode_collision,
            "collision_covered_by_mount_content_and_mode_identity": bool(collision_identity_ok),
            "requires_equal_inode_numbers": True,
        },
        "content": {
            "primary_sha256": sha256_bytes(primary_reopen),
            "peer_sha256": sha256_bytes(peer_reopen),
            "primary_len": len(primary_reopen),
            "peer_len": len(peer_reopen),
        },
        "stat": {"primary": primary_stat, "peer": peer_stat},
        "fd_operations": operations,
        "error_isolation": {
            "primary_errno": error_primary_errno,
            "primary_errno_name": errno_name(error_primary_errno),
            "peer_errno": error_peer_errno,
            "peer_errno_name": errno_name(error_peer_errno),
        },
        "uid_access": access,
    }
    write_json(artifacts / "operations.json", records)

    checks = [
        build_check("live-owner-dfs-mount-identity", "PASS" if owner_ok and dfs_ok and owner_base_ok and dfs_base_ok else "BLOCKED", identity_checks, "artifacts/fun-13-mount-isolation/operations.json"),
        build_check("live-node-meta-process-identity", "PASS" if identity_checks["node_identity_ok"] and meta_identity_ok else "BLOCKED", {"node": node_identity, "meta": meta_identity_record}, "artifacts/fun-13-mount-isolation/operations.json"),
        build_check("same-relative-path-divergent-content-and-fd", "PASS" if content_ok else "FAIL", records["content"] | {"fd_operations": operations}, "artifacts/fun-13-mount-isolation/operations.json"),
        build_check("mount-scoped-inode-identity", "PASS" if collision_identity_ok else "INCONCLUSIVE" if not inode_collision else "FAIL", records["path_identity"], "artifacts/fun-13-mount-isolation/operations.json"),
        build_check("mode-and-real-uid-access-isolation", "PASS" if mode_ok and uid_ok else "FAIL", {"mode_ok": mode_ok, "uid_ok": uid_ok, "uid_access": access_result}, "artifacts/fun-13-mount-isolation/operations.json"),
        build_check("errno-isolation-same-relative-path", "PASS" if error_ok else "FAIL", records["error_isolation"], "artifacts/fun-13-mount-isolation/operations.json"),
    ]
    if any(check["status"] == "FAIL" for check in checks):
        status = "FAIL"
    elif any(check["status"] == "BLOCKED" for check in checks):
        status = "BLOCKED"
    elif any(check["status"] == "INCONCLUSIVE" for check in checks):
        status = "INCONCLUSIVE"
    else:
        status = "PASS"
    if status == "PASS":
        reason = "FUN-13 live mount isolation checks passed with equal inode numbers and mount-scoped identity"
    elif status == "INCONCLUSIVE":
        reason = "FUN-13 semantic checks ran but did not observe equal inode numbers required by the case wording"
    else:
        reason = "one or more FUN-13 semantic checks failed or were blocked"
    return status, reason, checks, records


def proof_template(case_id: str, profile: str, matrix: dict[str, str], status: str, reason: str, checks: list[dict[str, Any]], artifacts: Path, run_dir: Path, records: dict[str, Any] | None = None) -> dict[str, Any]:
    return {
        "case_id": case_id,
        "profile": profile,
        "matrix": matrix,
        "status": status,
        "reason": reason,
        "checks": checks,
        "artifacts": {"root": str(artifacts), "files": artifact_manifest(artifacts, run_dir) if artifacts.exists() else []},
        "records": records or {},
    }


def parse_args(argv: list[str]) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Run AFS FUN-13 mount isolation driver")
    sub = parser.add_subparsers(dest="command")
    identity = sub.add_parser("identity", help="emit local process identity for remote worker use")
    identity.add_argument("--role", choices=["node", "meta"], required=True)
    identity.add_argument("--pid", type=int, required=True)
    identity.add_argument("--expected-sha256", required=True)
    identity.add_argument("--expected-boot-id", required=True)
    identity.add_argument("--expected-start-ticks", type=int, required=True)
    identity.add_argument("--expected-config-path")
    identity.add_argument("--expected-config-sha256")

    parser.add_argument("--case-id", default=os.environ.get("AFS_ACCEPTANCE_CASE_ID", "FUN-13"))
    parser.add_argument("--profile", default=os.environ.get("AFS_ACCEPTANCE_PROFILE", "smoke"), choices=["smoke", "full"])
    parser.add_argument("--matrix-json", default=os.environ.get("AFS_ACCEPTANCE_MATRIX", "{}"))
    parser.add_argument("--run-dir", type=Path, default=Path(os.environ["AFS_ACCEPTANCE_RUN_DIR"]) if os.environ.get("AFS_ACCEPTANCE_RUN_DIR") else None)
    parser.add_argument("--bindings", type=Path, default=Path(os.environ["AFS_ACCEPTANCE_MOUNT_ISOLATION_BINDINGS"]) if os.environ.get("AFS_ACCEPTANCE_MOUNT_ISOLATION_BINDINGS") else None)
    return parser.parse_args(argv)


def run_driver(args: argparse.Namespace) -> int:
    if args.case_id != "FUN-13":
        raise BindingError(f"mount isolation driver only supports FUN-13, got {args.case_id}")
    if args.run_dir is None:
        raise BindingError("--run-dir or AFS_ACCEPTANCE_RUN_DIR is required")
    if args.bindings is None:
        raise BindingError("--bindings or AFS_ACCEPTANCE_MOUNT_ISOLATION_BINDINGS is required")
    matrix = matrix_from_env() if args.matrix_json == os.environ.get("AFS_ACCEPTANCE_MATRIX", "{}") else {str(k): str(v) for k, v in json.loads(args.matrix_json).items()}
    artifacts = args.run_dir / "artifacts" / "fun-13-mount-isolation"
    artifacts.mkdir(parents=True, exist_ok=True)
    try:
        binding = load_binding(args.bindings.resolve(), args.case_id, args.profile, matrix)
        status, reason, checks, records = run_operations(args, binding, matrix, artifacts, args.run_dir)
    except DriverError as exc:
        status = exc.status
        reason = str(exc)
        checks = [build_check("mount-isolation-driver", status, reason)]
        records = {"exception": type(exc).__name__, "traceback": traceback.format_exc()}
    except Exception as exc:  # noqa: BLE001 - unexpected driver failure is fail evidence
        status = "FAIL"
        reason = f"unexpected mount isolation driver failure: {type(exc).__name__}: {exc}"
        checks = [build_check("mount-isolation-driver", "FAIL", reason)]
        records = {"exception": type(exc).__name__, "traceback": traceback.format_exc()}
    proof = proof_template(args.case_id, args.profile, matrix, status, reason, checks, artifacts, args.run_dir, records)
    write_json(artifacts / "proof.json", proof)
    print(json.dumps(proof, sort_keys=True))
    return 0 if status == "PASS" else 1


def main(argv: list[str] | None = None) -> int:
    args = parse_args(argv or sys.argv[1:])
    if args.command == "identity":
        identity = process_identity(
            args.pid,
            args.role,
            validate_sha(args.expected_sha256, "--expected-sha256"),
            validate_boot_id(args.expected_boot_id, "--expected-boot-id"),
            validate_positive_int(args.expected_start_ticks, "--expected-start-ticks"),
        )
        if args.expected_config_path or args.expected_config_sha256:
            identity = attach_config_identity(
                identity,
                validate_abs_path(args.expected_config_path, "--expected-config-path"),
                validate_sha(args.expected_config_sha256, "--expected-config-sha256"),
            )
        print(json.dumps(identity, sort_keys=True))
        return 0 if process_identity_ok(identity, "afs-meta" if args.role == "meta" else "afs-node") else 1
    return run_driver(args)


if __name__ == "__main__":
    raise SystemExit(main())
