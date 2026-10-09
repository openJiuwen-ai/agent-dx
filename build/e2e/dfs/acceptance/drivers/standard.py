#!/usr/bin/env python3
"""Standard-suite acceptance drivers.

Currently implements STD-01 (pjdfstest) only. The driver runs the pinned
upstream pjdfstest root harness against a caller supplied mount directory,
records raw TAP/prove output, and emits the runner proof JSON as the final
stdout object.
"""
from __future__ import annotations

import argparse
import datetime as dt
import hashlib
import json
import os
import platform
import re
import shlex
import shutil
import signal
import subprocess
import uuid
import sys
import time
import traceback
import tomllib
from pathlib import Path
from typing import Any

from target_identity import mount_record, remote_target_checks, target_checks

PJDFS_REV = "d25636a227606f8960e5179741d8f4ad7030ef41"
DEFAULT_SUITE_ROOT = Path("/mnt/lima-afsctlstate/afs-acceptance/suites-reference/src/pjdfstest")
SMOKE_TESTS = ["open/00.t", "mkdir/00.t", "rename/00.t", "mknod/00.t"]


def utc() -> str:
    return dt.datetime.now(dt.timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")


def sha256_file(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for chunk in iter(lambda: handle.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def write_json(path: Path, value: Any) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(value, indent=2, sort_keys=True) + "\n", encoding="utf-8")


def run_text(argv: list[str], timeout: int = 10) -> dict[str, Any]:
    try:
        proc = subprocess.run(argv, shell=False, text=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE, timeout=timeout, check=False)
        return {"argv": argv, "returncode": proc.returncode, "stdout": proc.stdout, "stderr": proc.stderr}
    except Exception as exc:  # noqa: BLE001 - evidence path records exact failure
        return {"argv": argv, "returncode": None, "exception": type(exc).__name__, "message": str(exc)}


def run_bounded(argv: list[str], cwd: Path, timeout: int, stdout_path: Path, stderr_path: Path) -> dict[str, Any]:
    started = time.time()
    stdout_path.parent.mkdir(parents=True, exist_ok=True)
    stderr_path.parent.mkdir(parents=True, exist_ok=True)
    timed_out = False
    with stdout_path.open("wb") as out, stderr_path.open("wb") as err:
        process = subprocess.Popen(argv, cwd=str(cwd), shell=False, stdout=out, stderr=err, start_new_session=True)
        try:
            returncode = process.wait(timeout=timeout)
        except subprocess.TimeoutExpired:
            timed_out = True
            try:
                os.killpg(process.pid, signal.SIGTERM)
            except ProcessLookupError:
                pass
            try:
                returncode = process.wait(timeout=2)
            except subprocess.TimeoutExpired:
                try:
                    os.killpg(process.pid, signal.SIGKILL)
                except ProcessLookupError:
                    pass
                returncode = process.wait()
            err.write(f"pjdfstest driver timed out after {timeout}s\n".encode())
    return {"argv": argv, "cwd": str(cwd), "returncode": returncode, "timeout_seconds": timeout, "timed_out": timed_out, "duration_seconds": round(time.time() - started, 3)}


def discover_tests(suite_root: Path) -> dict[str, Any]:
    tests_root = suite_root / "tests"
    tests = sorted(str(path.relative_to(tests_root)) for path in tests_root.rglob("*.t") if path.is_file())
    uid_drop_tests: list[str] = []
    todo_source_files: list[str] = []
    for rel in tests:
        text = (tests_root / rel).read_text(errors="replace")
        if re.search(r"\s-u\s+\d+", text) or re.search(r"\s-g\s+\d+", text):
            uid_drop_tests.append(rel)
        if "TODO" in text:
            todo_source_files.append(rel)
    misc = tests_root / "misc.sh"
    misc_text = misc.read_text(errors="replace") if misc.exists() else ""
    return {
        "tests_root": str(tests_root),
        "discovered_files": len(tests),
        "tests": tests,
        "uid_drop_test_files": uid_drop_tests,
        "uid_drop_test_file_count": len(uid_drop_tests),
        "todo_source_files": todo_source_files,
        "todo_source_file_count": len(todo_source_files),
        "misc_defines_todo": "TODO" in misc_text,
    }


def tap_file_progress(text: str, selected_tests: list[str]) -> dict[str, Any]:
    """Infer pjdfstest file progress from prove -rv TAP output.

    prove emits a file header before each .t script and an unnumbered
    per-file result when that file finishes.  A timeout can leave a current
    file with normal TAP subtests but without that final result.  Suite
    accounting must therefore distinguish selected files from files actually
    started and completed.
    """
    selected_set = set(selected_tests)
    started: list[str] = []
    completed: list[str] = []
    seen_started: set[str] = set()
    seen_completed: set[str] = set()
    current: str | None = None
    current_has_output = False
    current_has_result = False

    def header_rel(line: str) -> str | None:
        stripped = line.strip()
        if ".t" not in stripped:
            return None
        first = stripped.split(None, 1)[0]
        if not first.endswith(".t"):
            return None
        candidate = first.split("/tests/", 1)[1] if "/tests/" in first else first
        if candidate in selected_set:
            return candidate
        for rel_test in selected_tests:
            if first.endswith("/" + rel_test):
                return rel_test
        return None

    def finish_current(completed_by_header: bool) -> None:
        nonlocal current, current_has_output, current_has_result
        if current is not None and current not in seen_completed and (completed_by_header or current_has_result):
            seen_completed.add(current)
            completed.append(current)
        current = None
        current_has_output = False
        current_has_result = False

    for line in text.splitlines():
        rel_test = header_rel(line)
        if rel_test:
            finish_current(completed_by_header=True)
            current = rel_test
            current_has_output = False
            current_has_result = False
            if rel_test not in seen_started:
                seen_started.add(rel_test)
                started.append(rel_test)
            continue

        stripped = line.strip()
        if current is not None:
            if stripped:
                current_has_output = True
            if re.match(r"^(?:not\s+)?ok$", stripped):
                current_has_result = True

    truncated_current = current if current is not None and current not in seen_completed and current_has_output and not current_has_result else None
    finish_current(completed_by_header=False)
    unobserved = [rel_test for rel_test in selected_tests if rel_test not in seen_started]
    observed_incomplete = [rel_test for rel_test in started if rel_test not in seen_completed]
    return {
        "observed_started_files": len(started),
        "executed_files": len(started),
        "observed_completed_files": len(completed),
        "completed_files": len(completed),
        "unobserved_files": len(unobserved),
        "observed_incomplete_files": len(observed_incomplete),
        "incomplete_files": len(observed_incomplete),
        "observed_started_tests": started,
        "observed_completed_tests": completed,
        "unobserved_tests": unobserved,
        "observed_incomplete_tests": observed_incomplete,
        "truncated_current_test": truncated_current,
        "file_progress_complete": len(completed) == len(selected_tests),
        "file_progress_note": "unobserved files may have been unstarted or buffered by prove before timeout",
    }


def parse_tap_and_prove(stdout_path: Path, stderr_path: Path, selected_tests: list[str]) -> dict[str, Any]:
    text = stdout_path.read_text(errors="replace") if stdout_path.exists() else ""
    stderr = stderr_path.read_text(errors="replace") if stderr_path.exists() else ""
    tap_ok = tap_not_ok = tap_skip = tap_todo = tap_unexpected_fail = 0
    todo_not_ok = 0
    planned = 0
    not_ok_lines: list[str] = []
    unexpected_fail_lines: list[str] = []
    todo_lines: list[str] = []
    skip_lines: list[str] = []
    for line in text.splitlines():
        stripped = line.strip()
        if re.match(r"^1\.\.\d+", stripped):
            try:
                planned += int(stripped.split("..", 1)[1].split()[0])
            except Exception:  # noqa: BLE001
                pass
        if re.match(r"^ok\s+\d+", stripped):
            tap_ok += 1
            if re.search(r"#\s*SKIP", stripped, re.I):
                tap_skip += 1
                skip_lines.append(stripped)
            if re.search(r"#\s*TODO", stripped, re.I):
                tap_todo += 1
                todo_lines.append(stripped)
        elif re.match(r"^not ok\s+\d+", stripped):
            tap_not_ok += 1
            not_ok_lines.append(stripped)
            if re.search(r"#\s*TODO", stripped, re.I):
                tap_todo += 1
                todo_not_ok += 1
                todo_lines.append(stripped)
            else:
                tap_unexpected_fail += 1
                unexpected_fail_lines.append(stripped)
    prove_files = prove_tests = None
    prove_result = None
    match = re.search(r"Files=(\d+),\s+Tests=(\d+).+?Result:\s+(\w+)", text, re.S)
    if match:
        prove_files = int(match.group(1))
        prove_tests = int(match.group(2))
        prove_result = match.group(3)
    file_progress = tap_file_progress(text, selected_tests)
    return {
        "selected_files": len(selected_tests),
        "selected_tests": selected_tests,
        "stdout_bytes": len(text.encode()),
        "stderr_bytes": len(stderr.encode()),
        "tap_ok": tap_ok,
        "tap_not_ok": tap_not_ok,
        "tap_skip": tap_skip,
        "tap_todo": tap_todo,
        "tap_todo_not_ok": todo_not_ok,
        "tap_unexpected_fail": tap_unexpected_fail,
        "tap_planned": planned,
        "not_ok_lines": not_ok_lines,
        "unexpected_fail_lines": unexpected_fail_lines,
        "todo_lines": todo_lines,
        "skip_lines": skip_lines,
        "prove_files": prove_files,
        "prove_tests": prove_tests,
        "prove_result": prove_result,
        **file_progress,
        "accounting_identity": {
            "tap_total_observed": tap_ok + tap_not_ok,
            "tap_total_accounted": tap_ok + tap_not_ok,
            "file_total_selected": len(selected_tests),
            "file_total_observed_started": file_progress["observed_started_files"],
            "file_total_observed_completed": file_progress["observed_completed_files"],
            "no_post_failure_filtering": True,
            "todo_is_counted_not_filtered": True,
        },
    }


def mount_identity(mount: Path) -> dict[str, Any]:
    return run_text(["findmnt", "-T", str(mount), "-o", "TARGET,SOURCE,FSTYPE,OPTIONS", "--json"])


def complete_accounting(accounting: dict[str, Any], selected_files: int) -> bool:
    observed = accounting["tap_ok"] + accounting["tap_not_ok"]
    file_progress_complete = (
        accounting.get("observed_completed_files", accounting.get("completed_files", selected_files)) == selected_files
        and accounting.get("observed_incomplete_files", accounting.get("incomplete_files", 0)) == 0
        and accounting.get("unobserved_files", 0) == 0
    )
    return (
        selected_files > 0
        and accounting["prove_files"] == selected_files
        and observed > 0
        and accounting["prove_tests"] == observed
        and accounting["tap_planned"] == observed
        and file_progress_complete
    )


def resolve_base_dir(mount: Path, base_dir: Path | None) -> Path:
    if base_dir is None:
        return mount
    return base_dir if base_dir.is_absolute() else mount / base_dir


def is_under(child: Path, parent: Path) -> bool:
    try:
        child.resolve().relative_to(parent.resolve())
        return True
    except ValueError:
        return False


def process_identity(pid: str | None) -> dict[str, Any] | None:
    if not pid:
        return None
    proc = Path("/proc") / pid
    result: dict[str, Any] = {"pid": pid, "exists": proc.exists()}
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


def suite_identity(suite_root: Path) -> dict[str, Any]:
    # The root POSIX harness reads the explicitly selected, pinned suite that
    # was prepared by another user. Trust only this path for these read-only
    # commands; do not change global Git configuration or accept any revision.
    root = suite_root.resolve()
    git = ["git", "-c", f"safe.directory={root}", "-C", str(root)]
    head = run_text(git + ["rev-parse", "HEAD"])
    status = run_text(git + ["status", "--porcelain"])
    exe = suite_root / "pjdfstest"
    return {
        "suite_root": str(suite_root),
        "expected_revision": PJDFS_REV,
        "git_head": head.get("stdout", "").strip(),
        "git_head_command": head,
        "git_status_porcelain": status.get("stdout", ""),
        "git_status_command": status,
        "executable": str(exe),
        "executable_exists": exe.is_file() and os.access(exe, os.X_OK),
        "executable_sha256": sha256_file(exe) if exe.is_file() else None,
    }


def rel(path: Path, base: Path) -> str:
    return str(path.resolve().relative_to(base.resolve()))


def build_check(name: str, status: str, evidence: Any, artifact: str | None = None) -> dict[str, Any]:
    check = {"name": name, "status": status, "evidence": evidence}
    if artifact:
        check["artifact"] = artifact
    return check


def artifact_file_manifest(root: Path, base: Path, exclude: set[Path] | None = None) -> list[dict[str, Any]]:
    if not root.exists():
        return []
    excluded = {item.resolve() for item in (exclude or set())}
    files: list[dict[str, Any]] = []
    for path in sorted(item for item in root.rglob("*") if item.is_file()):
        if path.resolve() in excluded:
            continue
        files.append({"path": rel(path, base), "bytes": path.stat().st_size, "sha256": sha256_file(path)})
    return files


def remote_check_artifacts(checks: list[dict[str, Any]], manifest: list[dict[str, Any]], host_artifact: str) -> list[dict[str, Any]]:
    manifest_by_path = {entry.get("path"): entry for entry in manifest if isinstance(entry, dict)}
    rewritten: list[dict[str, Any]] = []
    for check in checks:
        copy = dict(check)
        if "artifact" in copy:
            remote_artifact = copy["artifact"]
            evidence = copy.get("evidence") if isinstance(copy.get("evidence"), dict) else {"value": copy.get("evidence")}
            evidence = dict(evidence)
            evidence["remote_artifact"] = remote_artifact
            evidence["remote_manifest_entry"] = manifest_by_path.get(remote_artifact)
            evidence["remote_artifact_manifest_matched"] = remote_artifact in manifest_by_path
            evidence["remote_artifact_note"] = "Remote path is relative to worker --run-dir on B; host artifact preserves the worker proof and manifest."
            copy["evidence"] = evidence
            copy["artifact"] = host_artifact
        rewritten.append(copy)
    return rewritten



def parse_expected_process(value: str) -> tuple[str, int, str]:
    if "=" not in value or ":" not in value:
        raise argparse.ArgumentTypeError("expected ROLE=PID:SHA256")
    role, rest = value.split("=", 1)
    pid_text, sha = rest.split(":", 1)
    if role not in {"node", "meta"}:
        raise argparse.ArgumentTypeError("role must be node or meta")
    if not pid_text.isdecimal() or int(pid_text) <= 0:
        raise argparse.ArgumentTypeError("pid must be positive decimal")
    sha = sha.lower()
    if len(sha) != 64 or any(ch not in "0123456789abcdef" for ch in sha):
        raise argparse.ArgumentTypeError("sha256 must be 64 lowercase hex characters")
    return role, int(pid_text), sha


def parse_json_argv(value: str) -> list[str]:
    try:
        parsed = json.loads(value)
    except json.JSONDecodeError as exc:
        raise argparse.ArgumentTypeError(f"invalid JSON argv: {exc}") from exc
    if not isinstance(parsed, list) or not parsed or not all(isinstance(item, str) and item for item in parsed):
        raise argparse.ArgumentTypeError("expected non-empty JSON string array")
    return parsed


def parse_start_ticks(stat_text: str) -> int:
    close = stat_text.rfind(")")
    if close == -1:
        raise RuntimeError("/proc stat is malformed: missing comm terminator")
    tail = stat_text[close + 2 :].split()
    if len(tail) <= 19:
        raise RuntimeError("/proc stat is malformed: missing starttime")
    return int(tail[19])


def process_fingerprint(pid: int) -> dict[str, Any]:
    proc = Path("/proc") / str(pid)
    stat_text = (proc / "stat").read_text(encoding="utf-8")
    exe = proc / "exe"
    exe_target = os.readlink(exe)
    exe_stat = exe.stat()
    return {"pid": pid, "start_ticks": parse_start_ticks(stat_text), "exe_path": exe_target, "exe_dev": exe_stat.st_dev, "exe_inode": exe_stat.st_ino}


def cmdline_for_pid(pid: int) -> list[str]:
    raw = (Path("/proc") / str(pid) / "cmdline").read_bytes()
    return [part.decode(errors="replace") for part in raw.split(b"\0") if part]


def config_path_from_cmdline(cmdline: list[str]) -> Path | None:
    for idx, item in enumerate(cmdline):
        if item == "--config" and idx + 1 < len(cmdline):
            return Path(cmdline[idx + 1])
        if item.startswith("--config="):
            return Path(item.split("=", 1)[1])
    return None


def flattened_toml_values(value: Any, prefix: str = "") -> dict[str, Any]:
    flattened: dict[str, Any] = {}
    if isinstance(value, dict):
        for key, child in value.items():
            child_prefix = f"{prefix}.{key}" if prefix else str(key)
            flattened.update(flattened_toml_values(child, child_prefix))
    else:
        flattened[prefix] = value
    return flattened


def first_toml_value(values: dict[str, Any], key: str) -> Any:
    if key in values:
        return values[key]
    suffix = f".{key}"
    for name, value in values.items():
        if name.endswith(suffix):
            return value
    return None


def string_toml_value(values: dict[str, Any], key: str) -> str | None:
    value = first_toml_value(values, key)
    return value if isinstance(value, str) and value else None


def string_map_toml_value(values: dict[str, Any], key: str) -> dict[str, str]:
    value = first_toml_value(values, key)
    if isinstance(value, dict):
        return {str(name): str(path) for name, path in value.items() if isinstance(path, str) and path}
    if isinstance(value, list):
        return {str(index): item for index, item in enumerate(value) if isinstance(item, str) and item}
    if isinstance(value, str) and value:
        return {key: value}
    prefix = f"{key}."
    children = {name[len(prefix):]: child for name, child in values.items() if name.startswith(prefix) and isinstance(child, str) and child}
    return children


def file_digest(path_text: str | None) -> dict[str, Any] | None:
    if not path_text:
        return None
    path = Path(path_text)
    try:
        return {"path": str(path), "sha256": sha256_file(path), "exists": True}
    except OSError as exc:
        return {"path": str(path), "exists": False, "error": f"{type(exc).__name__}: {exc}"}


def config_summary(path: Path) -> dict[str, Any]:
    text = path.read_text(encoding="utf-8")
    parsed = tomllib.loads(text)
    values = flattened_toml_values(parsed)
    tls_names = ("tls_ca_certificate", "tls_identity_certificate", "tls_identity_private_key")
    tls = {name: file_digest(string_toml_value(values, name)) for name in tls_names if string_toml_value(values, name)}
    trusted_node_certs = {name: file_digest(cert_path) for name, cert_path in string_map_toml_value(values, "trusted_node_certs").items()}
    tls_required = bool(any(string_toml_value(values, name) for name in tls_names) or string_toml_value(values, "tls_server_name") or trusted_node_certs)
    tls_missing = [name for name in tls_names if tls_required and not string_toml_value(values, name)]
    return {
        "path": str(path),
        "sha256": sha256_file(path),
        "grpc_listen": string_toml_value(values, "grpc_listen"),
        "rest_listen": string_toml_value(values, "rest_listen"),
        "meta_endpoint": string_toml_value(values, "meta_endpoint"),
        "tls_server_name": string_toml_value(values, "tls_server_name"),
        "tls_required": tls_required,
        "tls_missing": tls_missing,
        "tls": tls,
        "trusted_node_certs": trusted_node_certs,
    }


def strict_process_identity(role: str, pid: int, expected_sha256: str) -> dict[str, Any]:
    before = process_fingerprint(pid)
    actual_sha = sha256_file(Path("/proc") / str(pid) / "exe")
    after = process_fingerprint(pid)
    for key in ("pid", "start_ticks", "exe_dev", "exe_inode"):
        if before[key] != after[key]:
            raise RuntimeError(f"process {role} changed while hashing: {key} {before[key]!r} -> {after[key]!r}")
    cmdline = cmdline_for_pid(pid)
    cfg_path = config_path_from_cmdline(cmdline)
    if cfg_path is None:
        raise RuntimeError(f"process {role} cmdline does not contain --config")
    cfg = config_summary(cfg_path)
    return {
        "role": role,
        "pid": pid,
        "exists": True,
        "boot_id": read_text_optional(Path("/proc/sys/kernel/random/boot_id")),
        "machine_id": read_text_optional(Path("/etc/machine-id")),
        "exe_path": before["exe_path"],
        "exe_dev": before["exe_dev"],
        "exe_inode": before["exe_inode"],
        "start_ticks": before["start_ticks"],
        "sha256": actual_sha,
        "expected_sha256": expected_sha256,
        "sha256_ok": actual_sha == expected_sha256,
        "cmdline_args": cmdline,
        "config": cfg,
        "network": process_network_identity(pid),
    }


def read_text_optional(path: Path) -> str | None:
    try:
        return path.read_text(encoding="utf-8").strip()
    except OSError:
        return None


def process_socket_inodes(pid: int) -> set[str]:
    inodes: set[str] = set()
    fd_dir = Path("/proc") / str(pid) / "fd"
    try:
        entries = list(fd_dir.iterdir())
    except OSError:
        return inodes
    for fd in entries:
        try:
            target = os.readlink(fd)
        except OSError:
            continue
        if target.startswith("socket:[") and target.endswith("]"):
            inodes.add(target[len("socket:[") : -1])
    return inodes


def parse_ipv4_hex(value: str) -> str:
    raw = bytes.fromhex(value)
    return ".".join(str(part) for part in raw[::-1])


def parse_ipv6_hex(value: str) -> str:
    # /proc/net/tcp6 stores each 32-bit word little-endian.
    raw = bytes.fromhex(value)
    words = [raw[index : index + 4][::-1] for index in range(0, 16, 4)]
    data = b"".join(words)
    groups = [data[index : index + 2].hex() for index in range(0, 16, 2)]
    return ":".join(groups)


def parse_proc_net(path: Path, family: str, owned_inodes: set[str]) -> list[dict[str, Any]]:
    sockets: list[dict[str, Any]] = []
    try:
        lines = path.read_text(encoding="utf-8").splitlines()[1:]
    except OSError:
        return sockets
    for line in lines:
        fields = line.split()
        if len(fields) < 10:
            continue
        local, state, inode = fields[1], fields[3], fields[9]
        if state != "0A" or inode not in owned_inodes:
            continue
        host_hex, port_hex = local.split(":", 1)
        ip = parse_ipv4_hex(host_hex) if family == "tcp" else parse_ipv6_hex(host_hex)
        sockets.append({"family": family, "ip": ip, "port": int(port_hex, 16), "inode": inode})
    return sockets


def local_interface_ips() -> list[str]:
    ips: set[str] = set()
    record = run_text(["hostname", "-I"])
    if record.get("returncode") == 0:
        ips.update(part for part in record.get("stdout", "").split() if part)
    return sorted(ips)


def process_network_identity(pid: int) -> dict[str, Any]:
    owned = process_socket_inodes(pid)
    sockets = parse_proc_net(Path("/proc/net/tcp"), "tcp", owned)
    sockets.extend(parse_proc_net(Path("/proc/net/tcp6"), "tcp6", owned))
    return {"interface_ips": local_interface_ips(), "listen_sockets": sorted(sockets, key=lambda item: (item["family"], item["ip"], item["port"], item["inode"]))}


def emit(value: dict[str, Any]) -> None:
    print(json.dumps(value, sort_keys=True), flush=True)


def worker_identity_main(argv: list[str]) -> int:
    parser = argparse.ArgumentParser(description="collect strict standard driver process identity")
    parser.add_argument("--path", type=Path, required=True)
    parser.add_argument("--base-path", type=Path)
    parser.add_argument("--worker-run-dir", type=Path)
    parser.add_argument("--expected-process", action="append", type=parse_expected_process, required=True)
    args = parser.parse_args(argv)
    identities = {role: strict_process_identity(role, pid, sha) for role, pid, sha in args.expected_process}
    base_path = args.base_path or args.path
    event = {"event": "IDENTITY", "platform": {"system": platform.system(), "machine": platform.machine(), "python": platform.python_version()}, "mount": mount_identity(args.path), "base_mount": mount_identity(base_path), "processes": identities}
    if args.worker_run_dir is not None:
        event["worker_run_dir"] = str(args.worker_run_dir)
        event["worker_run_dir_mount"] = mount_identity(args.worker_run_dir)
    emit(event)
    return 0


def run_pjdfstest_worker(args: argparse.Namespace) -> dict[str, Any]:
    run_dir = args.run_dir.resolve()
    artifacts = run_dir / "artifacts" / "std-01-pjdfstest"
    artifacts.mkdir(parents=True, exist_ok=True)
    matrix = json.loads(args.matrix_json) if args.matrix_json.strip() else {}
    if not isinstance(matrix, dict):
        matrix = {}
    matrix.setdefault("reference", "ext4")
    matrix.setdefault("suite_sha", f"sanwan/pjdfstest {PJDFS_REV}")
    checks: list[dict[str, Any]] = []
    status = "PASS"
    reason = ""
    accounting: dict[str, Any] = {}
    command_result: dict[str, Any] | None = None
    fixture: Path | None = None
    fixture_kept: bool | None = None
    try:
        if args.case_id != "STD-01":
            raise RuntimeError(f"standard.py currently implements STD-01 only, got {args.case_id}")
        suite = suite_identity(args.suite_root)
        discovery = discover_tests(args.suite_root)
        base_dir = resolve_base_dir(args.mount, args.base_dir)
        mnt = mount_identity(args.mount)
        base_mnt = mount_identity(base_dir)
        identity = {"created_at": utc(), "host": run_text(["hostname"]), "uname": run_text(["uname", "-a"]), "platform": {"system": platform.system(), "release": platform.release(), "machine": platform.machine(), "python": platform.python_version(), "uid": os.geteuid(), "gid": os.getegid()}, "suite": suite, "mount": mnt, "base_mount": base_mnt, "product": {"backend": args.backend, "mount_path": str(args.mount), "base_dir": str(base_dir)}}
        write_json(artifacts / "identity.json", identity)
        write_json(artifacts / "discovery.json", discovery)
        suite_ok = suite["git_head"] == PJDFS_REV and suite["executable_exists"]
        checks.append(build_check("pinned-suite-identity", "PASS" if suite_ok else "FAIL", {"expected": PJDFS_REV, "observed": suite.get("git_head"), "executable_exists": suite.get("executable_exists")}, rel(artifacts / "identity.json", run_dir)))
        root_ok = os.geteuid() == 0
        checks.append(build_check("root-harness", "PASS" if root_ok else "BLOCKED", {"euid": os.geteuid(), "uid_drop_test_file_count": discovery["uid_drop_test_file_count"]}, rel(artifacts / "discovery.json", run_dir)))
        mount_ok = mnt.get("returncode") == 0 and bool(mnt.get("stdout", "").strip())
        base_dir_ok = base_dir.is_dir() and is_under(base_dir, args.mount)
        checks.append(build_check("mount-identity", "PASS" if mount_ok else "BLOCKED", {"mount": str(args.mount), "findmnt_returncode": mnt.get("returncode")}, rel(artifacts / "identity.json", run_dir)))
        checks.append(build_check("base-dir-scope", "PASS" if base_dir_ok else "BLOCKED", {"mount": str(args.mount), "base_dir": str(base_dir), "exists": base_dir.exists(), "is_dir": base_dir.is_dir(), "under_mount": is_under(base_dir, args.mount)}, rel(artifacts / "identity.json", run_dir)))
        discovery_ok = discovery["discovered_files"] > 0
        checks.append(build_check("discovery", "PASS" if discovery_ok else "BLOCKED", {"discovered_files": discovery["discovered_files"]}, rel(artifacts / "discovery.json", run_dir)))
        if not suite_ok or not root_ok or not mount_ok or not base_dir_ok or not discovery_ok:
            status = "BLOCKED"
            reason = "pjdfstest worker preflight failed"
        else:
            tests_root = args.suite_root / "tests"
            selected_tests = SMOKE_TESTS if args.profile == "smoke" else discovery["tests"]
            timeout = args.timeout or (180 if args.profile == "smoke" else 1800)
            test_paths = [str(tests_root / rel_test) for rel_test in selected_tests]
            fixture = base_dir / f".afs-std01-pjdfstest-{dt.datetime.now(dt.timezone.utc).strftime('%Y%m%dT%H%M%SZ')}-{uuid.uuid4().hex[:8]}"
            fixture.mkdir(mode=0o755)
            os.chmod(fixture, 0o755)
            stdout = artifacts / "pjdfstest.stdout.tap"
            stderr = artifacts / "pjdfstest.stderr.log"
            command_result = run_bounded(["prove", "-e", "/bin/sh", "-rv", *test_paths], cwd=fixture, timeout=timeout, stdout_path=stdout, stderr_path=stderr)
            write_json(artifacts / "command.json", command_result)
            accounting = parse_tap_and_prove(stdout, stderr, selected_tests)
            accounting.update({"profile": args.profile, "discovered_files": discovery["discovered_files"], "not_selected_files": max(0, discovery["discovered_files"] - len(selected_tests)), "upstream_todo_source_file_count": discovery["todo_source_file_count"], "uid_drop_test_file_count": discovery["uid_drop_test_file_count"]})
            write_json(artifacts / "tap-accounting.json", accounting)
            checks.append(build_check("subprocess-bound", "PASS" if not command_result["timed_out"] else "BLOCKED", {"timeout_seconds": command_result["timeout_seconds"], "timed_out": command_result["timed_out"], "returncode": command_result["returncode"]}, rel(artifacts / "command.json", run_dir)))
            accounting_ok = complete_accounting(accounting, len(selected_tests))
            checks.append(build_check("tap-accounting", "PASS" if accounting_ok else "FAIL", accounting, rel(artifacts / "tap-accounting.json", run_dir)))
            result_ok = accounting_ok and command_result["returncode"] == 0 and not command_result["timed_out"] and accounting["tap_unexpected_fail"] == 0 and accounting.get("prove_result") == "PASS"
            checks.append(build_check("pjdfstest-result", "PASS" if result_ok else "FAIL", {"returncode": command_result["returncode"], "prove_result": accounting.get("prove_result"), "tap_unexpected_fail": accounting.get("tap_unexpected_fail"), "tap_todo": accounting.get("tap_todo")}, rel(artifacts / "pjdfstest.stdout.tap", run_dir)))
            if command_result["timed_out"]:
                status = "BLOCKED"; reason = f"pjdfstest timed out after {command_result['timeout_seconds']}s"
            elif not result_ok:
                status = "FAIL"; reason = "pjdfstest reported failures; raw TAP was preserved without filtering"
            if status == "PASS" and fixture.exists():
                shutil.rmtree(fixture); fixture_kept = False
            else:
                fixture_kept = bool(fixture and fixture.exists())
    except Exception as exc:
        status = "BLOCKED"; reason = f"driver setup failed: {type(exc).__name__}: {exc}"
        write_json(artifacts / "setup-error.json", {"exception": type(exc).__name__, "message": str(exc), "traceback": traceback.format_exc()})
        checks.append(build_check("driver-setup", "BLOCKED", reason, rel(artifacts / "setup-error.json", run_dir)))
    proof = {"case_id": args.case_id, "profile": args.profile, "matrix": matrix, "status": status, "reason": reason, "checks": checks, "coverage": {"profile": args.profile, "axes": {"reference": {"values": [str(matrix.get("reference", "ext4"))]}, "suite_sha": {"values": [str(matrix.get("suite_sha", f"sanwan/pjdfstest {PJDFS_REV}"))]}}}, "artifacts": {"root": rel(artifacts, run_dir), "manifest": []}, "fixture": {"path": str(fixture) if fixture else None, "base_dir": str(resolve_base_dir(args.mount, args.base_dir)), "kept": fixture_kept}, "identity": {"artifact": rel(artifacts / "identity.json", run_dir), "product": {"backend": args.backend, "mount_path": str(args.mount), "base_dir": str(resolve_base_dir(args.mount, args.base_dir))}}, "accounting": accounting, "command": command_result, "notes": ["Worker suite execution is qualified by standard.py host strict Node/Meta identity checks."]}
    write_json(artifacts / "proof.json", proof)
    proof["artifacts"]["manifest"] = artifact_file_manifest(artifacts, run_dir, exclude={artifacts / "proof.json"})
    write_json(artifacts / "proof.json", proof)
    return proof


def worker_pjdfstest_main(argv: list[str]) -> int:
    parser = argparse.ArgumentParser(description="run pjdfstest worker under host-qualified identity")
    parser.add_argument("--case-id", default="STD-01")
    parser.add_argument("--profile", choices=["smoke", "full"], default="smoke")
    parser.add_argument("--matrix-json", default="{}")
    parser.add_argument("--run-dir", type=Path, required=True)
    parser.add_argument("--mount", type=Path, required=True)
    parser.add_argument("--base-dir", type=Path)
    parser.add_argument("--suite-root", type=Path, default=DEFAULT_SUITE_ROOT)
    parser.add_argument("--timeout", type=int)
    parser.add_argument("--backend", required=True)
    args = parser.parse_args(argv)
    proof = run_pjdfstest_worker(args)
    emit({"event": "PJDFSTEST", "proof": proof})
    return 0 if proof.get("status") == "PASS" else 1


def read_child_proof(stdout: str) -> dict[str, Any] | None:
    text = stdout.strip()
    if not text:
        return None
    candidates = [text]
    candidates.extend(line.strip() for line in reversed(text.splitlines()) if line.strip())
    seen: set[str] = set()
    for candidate in candidates:
        if candidate in seen:
            continue
        seen.add(candidate)
        try:
            parsed = json.loads(candidate)
        except json.JSONDecodeError:
            continue
        if isinstance(parsed, dict):
            return parsed
    return None


def worker_driver_main(argv: list[str]) -> int:
    parser = argparse.ArgumentParser(description="run a fixed acceptance driver under host-qualified identity")
    parser.add_argument("--driver", type=Path, required=True)
    parser.add_argument("driver_args", nargs=argparse.REMAINDER)
    args = parser.parse_args(argv)
    driver_args = list(args.driver_args)
    if driver_args and driver_args[0] == "--":
        driver_args = driver_args[1:]
    env = dict(os.environ)
    env["AFS_ACCEPTANCE_REMOTE_HOST_QUALIFIED"] = "1"
    proc = subprocess.run([sys.executable, str(args.driver), *driver_args], shell=False, text=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE, check=False, env=env)
    if proc.stderr:
        sys.stderr.write(proc.stderr)
    proof = read_child_proof(proc.stdout)
    if proof is None:
        proof = {
            "case_id": "UNKNOWN",
            "profile": "unknown",
            "matrix": {},
            "status": "BLOCKED",
            "reason": "worker driver did not emit structured JSON proof",
            "checks": [{"name": "worker-driver", "status": "BLOCKED", "evidence": {"driver": str(args.driver), "returncode": proc.returncode}}],
        }
    emit({"event": "DRIVER", "driver": str(args.driver), "returncode": proc.returncode, "proof": proof})
    return proc.returncode if proof.get("status") == "PASS" else 1


def run_worker_json(prefix: list[str], args: list[str], timeout: int) -> dict[str, Any]:
    argv = prefix + args
    if prefix and Path(prefix[0]).name == 'ssh':
        # SSH concatenates remote argv before invoking the login shell. Quote
        # both the fixed worker program and every later fixture/identity arg.
        # The adapter supplies an explicit separator and host; reject opaque
        # prefixes rather than guessing where SSH options end.
        if '--' not in prefix:
            raise ValueError('SSH worker requires an explicit -- host separator')
        remote_start = prefix.index('--') + 2
        if remote_start >= len(prefix):
            raise ValueError('SSH worker requires a host and remote program')
        argv = prefix[:remote_start] + [shlex.join(prefix[remote_start:] + args)]
    started = time.time()
    timed_out = False
    proc = subprocess.Popen(argv, text=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE, start_new_session=True)
    try:
        stdout, stderr = proc.communicate(timeout=timeout)
        returncode = proc.returncode
    except subprocess.TimeoutExpired:
        timed_out = True
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
        returncode = proc.returncode
    events: list[dict[str, Any]] = []
    parse_errors: list[dict[str, Any]] = []
    for line_number, line in enumerate(stdout.splitlines(), 1):
        if not line.strip():
            continue
        try:
            events.append(json.loads(line))
        except json.JSONDecodeError as exc:
            parse_errors.append({"line": line_number, "error": str(exc), "text": line})
    return {"argv": argv, "returncode": returncode, "stdout": stdout, "stderr": stderr, "json_events": events, "json_parse_errors": parse_errors, "timed_out": timed_out, "timeout_seconds": timeout, "duration_seconds": round(time.time() - started, 3)}


def latest_event(record: dict[str, Any], event: str) -> dict[str, Any]:
    events = record.get("json_events") or []
    if record.get("timed_out"):
        raise RuntimeError(f"worker timed out before emitting {event}")
    if record.get("json_parse_errors"):
        raise RuntimeError(f"worker emitted malformed JSON before {event}: {record['json_parse_errors']!r}")
    if not events or events[-1].get("event") != event:
        raise RuntimeError(json.dumps({"message": f"worker did not emit {event}", "returncode": record.get("returncode"), "stderr": record.get("stderr")}, sort_keys=True))
    return events[-1]


def validate_worker_run_dir_b(path: Path) -> str | None:
    text = str(path)
    if not path.is_absolute():
        return "--worker-run-dir-b must be an absolute Linux guest path"
    if text.startswith("/Users/") or text.startswith("/Volumes/") or text.startswith("/private/") or text.startswith("/var/folders/"):
        return "--worker-run-dir-b must not be a macOS host path"
    return None


def worker_run_dir_ext4_check(identity_event: dict[str, Any], expected_path: Path) -> dict[str, Any]:
    record = mount_record(identity_event.get("worker_run_dir_mount") or {})
    return {
        "path": str(expected_path),
        "worker_reported_path": identity_event.get("worker_run_dir"),
        "path_matches": identity_event.get("worker_run_dir") == str(expected_path),
        "findmnt": record,
        "is_ext4": bool(record) and record.get("fstype") == "ext4",
    }


def mount_records_stable(before: dict[str, Any], after: dict[str, Any]) -> bool:
    return bool(before) and bool(after) and all(before.get(key) == after.get(key) for key in ("target", "source", "fstype"))


def worker_run_dir_stability(pre_event: dict[str, Any], post_event: dict[str, Any], expected_path: Path) -> dict[str, Any]:
    pre = worker_run_dir_ext4_check(pre_event, expected_path)
    post = worker_run_dir_ext4_check(post_event, expected_path)
    return {
        "pre": pre,
        "post": post,
        "stable": pre["path_matches"] and post["path_matches"] and pre["is_ext4"] and post["is_ext4"] and mount_records_stable(pre.get("findmnt") or {}, post.get("findmnt") or {}),
    }


def worker_manifest_entry_valid(entry: dict[str, Any]) -> bool:
    return (
        isinstance(entry.get("path"), str)
        and bool(entry.get("path"))
        and isinstance(entry.get("bytes"), int)
        and entry.get("bytes") >= 0
        and isinstance(entry.get("sha256"), str)
        and len(entry.get("sha256")) == 64
        and all(char in "0123456789abcdef" for char in entry.get("sha256"))
    )


def validate_worker_artifacts(worker_proof: dict[str, Any] | None) -> dict[str, Any]:
    if not worker_proof:
        return {"valid": False, "reason": "missing worker proof"}
    artifacts = worker_proof.get("artifacts") or {}
    manifest = artifacts.get("manifest") or []
    root = artifacts.get("root")
    if root == "artifacts/std-01-pjdfstest":
        required_paths = {
            "artifacts/std-01-pjdfstest/identity.json",
            "artifacts/std-01-pjdfstest/command.json",
            "artifacts/std-01-pjdfstest/tap-accounting.json",
            "artifacts/std-01-pjdfstest/pjdfstest.stdout.tap",
        }
    else:
        identity_artifact = ((worker_proof.get("identity") or {}).get("artifact") if isinstance(worker_proof.get("identity"), dict) else None)
        required_paths = {identity_artifact} if manifest and isinstance(identity_artifact, str) and identity_artifact else set()
    paths = {entry.get("path") for entry in manifest if isinstance(entry, dict)}
    missing_paths = sorted(required_paths - paths)
    invalid_entries = [entry for entry in manifest if not isinstance(entry, dict) or not worker_manifest_entry_valid(entry)]
    embedded = {
        "accounting": bool(worker_proof.get("accounting")),
        "command": isinstance(worker_proof.get("command"), dict),
        "identity": bool(worker_proof.get("identity")),
    }
    check_artifacts = []
    manifest_by_path = {entry.get("path"): entry for entry in manifest if isinstance(entry, dict)}
    for check in worker_proof.get("checks") or []:
        remote_artifact = check.get("artifact")
        if remote_artifact:
            check_artifacts.append({"check": check.get("name"), "artifact": remote_artifact, "manifest_entry": manifest_by_path.get(remote_artifact)})
    unmatched_check_artifacts = [item for item in check_artifacts if manifest and item.get("manifest_entry") is None]
    if root == "artifacts/std-01-pjdfstest":
        valid = (
            bool(manifest)
            and not missing_paths
            and not invalid_entries
            and all(embedded.values())
            and not unmatched_check_artifacts
        )
    else:
        valid = (
            embedded["accounting"]
            and embedded["identity"]
            and not missing_paths
            and not invalid_entries
            and not unmatched_check_artifacts
        )
    return {
        "valid": valid,
        "manifest_file_count": len(manifest),
        "required_paths": sorted(required_paths),
        "missing_paths": missing_paths,
        "invalid_entry_count": len(invalid_entries),
        "embedded": embedded,
        "check_artifacts": check_artifacts,
        "unmatched_check_artifacts": unmatched_check_artifacts,
    }


def worker_artifact_manifest(worker_proof: dict[str, Any] | None) -> dict[str, Any]:
    if not worker_proof:
        return {"available": False}
    artifacts = worker_proof.get("artifacts") or {}
    manifest = artifacts.get("manifest") or []
    return {
        "available": True,
        "location": "worker-b",
        "root": artifacts.get("root"),
        "manifest": manifest,
        "manifest_file_count": len(manifest),
        "proof_embedded_in_host_proof": True,
        "accounting_embedded_in_host_proof": bool(worker_proof.get("accounting")),
        "command_embedded_in_host_proof": bool(worker_proof.get("command")),
        "note": "Worker artifact paths are relative to --worker-run-dir-b on B; host artifacts/root contains host-local proof and remote-worker-records only.",
    }


def host_proof_template(args: argparse.Namespace, artifacts: Path, run_dir: Path, status: str, reason: str, checks: list[dict[str, Any]], records: dict[str, Any], qualification: dict[str, bool] | None = None, worker_proof: dict[str, Any] | None = None) -> dict[str, Any]:
    matrix = json.loads(args.matrix_json) if args.matrix_json.strip() else {}
    if not isinstance(matrix, dict):
        matrix = {}
    matrix.setdefault("reference", "ext4")
    if args.case_id == "STD-01":
        matrix.setdefault("suite_sha", f"sanwan/pjdfstest {PJDFS_REV}")
    coverage_axes = {"reference": {"values": [str(matrix["reference"])]}}
    if args.case_id == "STD-01":
        coverage_axes["suite_sha"] = {"values": [str(matrix["suite_sha"])]}
    proof: dict[str, Any] = {}
    proof.update({
        "case_id": args.case_id,
        "profile": args.profile,
        "matrix": matrix,
        "status": status,
        "reason": reason,
        "checks": checks,
        "coverage": (worker_proof or {}).get("coverage", {"profile": args.profile, "axes": coverage_axes}),
        "artifacts": {"root": rel(artifacts, run_dir), "manifest": artifact_file_manifest(artifacts, run_dir, exclude={artifacts / "proof.json"}), "remote_worker": worker_artifact_manifest(worker_proof)},
        "worker_run_dir_b": str(args.worker_run_dir_b),
        "fixture": (worker_proof or {}).get("fixture", {"path": None, "base_dir": str(args.base_dir_b or args.mount_b), "kept": None}),
        "identity": {"artifact": None, "product": {"backend": args.backend, "mount_path": str(args.mount_b), "base_dir": str(args.base_dir_b or args.mount_b)}},
        "accounting": (worker_proof or {}).get("accounting", {}),
        "command": (worker_proof or {}).get("command"),
        "remote_identity": {"qualification": qualification or {}, "records_artifact": rel(artifacts / "remote-worker-records.json", run_dir), "records": records, "suite_invoked": "suite" in records, "worker_proof": worker_proof},
    })
    return proof


def host_main(argv: list[str]) -> int:
    parser = argparse.ArgumentParser(description="run a standard suite on B with strict remote A Meta identity")
    parser.add_argument("--worker-a-json", required=True, type=parse_json_argv)
    parser.add_argument("--worker-b-json", required=True, type=parse_json_argv)
    parser.add_argument("--worker-a-expected-process", action="append", type=parse_expected_process, required=True)
    parser.add_argument("--worker-b-expected-process", action="append", type=parse_expected_process, required=True)
    parser.add_argument("--expected-meta-endpoint", required=True)
    parser.add_argument("--meta-identity-path-a", type=Path, default=Path("/"))
    parser.add_argument("--mount-b", type=Path, required=True)
    parser.add_argument("--base-dir-b", type=Path)
    parser.add_argument("--suite-root-b", type=Path, default=DEFAULT_SUITE_ROOT)
    parser.add_argument("--backend", default=os.environ.get("AFS_ACCEPTANCE_BACKEND"), required=os.environ.get("AFS_ACCEPTANCE_BACKEND") is None)
    parser.add_argument("--profile", choices=["smoke", "full"], default=os.environ.get("AFS_ACCEPTANCE_PROFILE", "smoke"))
    parser.add_argument("--case-id", default=os.environ.get("AFS_ACCEPTANCE_CASE_ID", "STD-01"))
    parser.add_argument("--matrix-json", default=os.environ.get("AFS_ACCEPTANCE_MATRIX", "{}"))
    parser.add_argument("--run-dir", type=Path, default=Path(os.environ.get("AFS_ACCEPTANCE_RUN_DIR", "results/std-01-driver-host")), help="Host-local evidence directory for host proof and worker records.")
    parser.add_argument("--worker-run-dir-b", type=Path, required=True, help="B guest ext4 absolute directory for pjdfstest worker artifacts.")
    parser.add_argument("--worker-suite-event", default="PJDFSTEST")
    parser.add_argument("--worker-suite-args-json", type=parse_json_argv)
    parser.add_argument("--timeout", type=int)
    parser.add_argument("--command-timeout", type=int, default=3600)
    parser.add_argument("--require-cross-worker", action="store_true", default=True, help=argparse.SUPPRESS)
    parser.add_argument("--allow-same-worker", action="store_true", help="Diagnostic-only escape hatch; product remote suite qualification requires distinct worker boot and machine IDs by default.")
    args = parser.parse_args(argv)
    args.require_cross_worker = not args.allow_same_worker
    run_dir = args.run_dir.resolve()
    artifacts = run_dir / "artifacts" / ("std-01-pjdfstest" if args.case_id == "STD-01" else f"{args.case_id.lower()}-remote")
    artifacts.mkdir(parents=True, exist_ok=True)
    records: dict[str, Any] = {}
    checks: list[dict[str, Any]] = []
    qualification: dict[str, bool] | None = None

    def finish(status: str, reason: str, worker_proof: dict[str, Any] | None = None) -> int:
        write_json(artifacts / "remote-worker-records.json", records)
        proof = host_proof_template(args, artifacts, run_dir, status, reason, checks, records, qualification, worker_proof)
        write_json(artifacts / "proof.json", proof)
        print(json.dumps(proof, sort_keys=True))
        return 0 if status == "PASS" else 1

    def exp_args(items: list[tuple[str, int, str]]) -> list[str]:
        out: list[str] = []
        for role, pid, sha in items:
            out += ["--expected-process", f"{role}={pid}:{sha}"]
        return out

    try:
        roles_a = {role for role, _pid, _sha in args.worker_a_expected_process}
        roles_b = {role for role, _pid, _sha in args.worker_b_expected_process}
        if "meta" not in roles_a:
            checks.append(build_check("host-arguments", "BLOCKED", "--worker-a-expected-process must include meta=PID:SHA256"))
            return finish("BLOCKED", "host argument validation failed")
        if "node" not in roles_b:
            checks.append(build_check("host-arguments", "BLOCKED", "--worker-b-expected-process must include node=PID:SHA256"))
            return finish("BLOCKED", "host argument validation failed")
        worker_dir_error = validate_worker_run_dir_b(args.worker_run_dir_b)
        if worker_dir_error:
            checks.append(build_check("worker-run-dir-b", "BLOCKED", {"path": str(args.worker_run_dir_b), "error": worker_dir_error}))
            return finish("BLOCKED", "host argument validation failed")

        meta_identity_args = ["worker-identity", "--path", str(args.meta_identity_path_a), *exp_args(args.worker_a_expected_process)]
        node_identity_args = ["worker-identity", "--path", str(args.mount_b), *( ["--base-path", str(args.base_dir_b)] if args.base_dir_b else [] ), "--worker-run-dir", str(args.worker_run_dir_b), *exp_args(args.worker_b_expected_process)]
        records["meta_pre"] = run_worker_json(args.worker_a_json, meta_identity_args, args.command_timeout)
        records["node_pre"] = run_worker_json(args.worker_b_json, node_identity_args, args.command_timeout)
        meta_pre = latest_event(records["meta_pre"], "IDENTITY")["processes"].get("meta")
        node_pre_event = latest_event(records["node_pre"], "IDENTITY")
        node_pre = node_pre_event["processes"].get("node")
        expected_node_sha = next(sha for role, _pid, sha in args.worker_b_expected_process if role == "node")
        expected_meta_sha = next(sha for role, _pid, sha in args.worker_a_expected_process if role == "meta")
        worker_system = (node_pre_event.get("platform") or {}).get("system", "")
        pre_qualification = remote_target_checks(worker_system, args.backend, node_pre_event["mount"], node_pre_event["base_mount"], node_pre, node_pre, meta_pre, meta_pre, expected_node_sha, expected_meta_sha, args.expected_meta_endpoint, args.require_cross_worker)
        worker_dir_check = worker_run_dir_ext4_check(node_pre_event, args.worker_run_dir_b)
        worker_dir_pre_ok = worker_dir_check["is_ext4"] and worker_dir_check["path_matches"]
        checks.append(build_check("worker-run-dir-b-preflight", "PASS" if worker_dir_pre_ok else "BLOCKED", worker_dir_check))
        checks.append(build_check("remote-target-preflight", "PASS" if all(pre_qualification.values()) else "BLOCKED", pre_qualification))
        if not all(pre_qualification.values()) or not worker_dir_pre_ok:
            qualification = pre_qualification
            return finish("BLOCKED", "remote target identity preflight failed")

        suite_args = args.worker_suite_args_json or ["worker-pjdfstest", "--case-id", args.case_id, "--profile", args.profile, "--matrix-json", args.matrix_json, "--run-dir", str(args.worker_run_dir_b), "--mount", str(args.mount_b), "--backend", args.backend, "--suite-root", str(args.suite_root_b), *( ["--base-dir", str(args.base_dir_b)] if args.base_dir_b else [] ), *( ["--timeout", str(args.timeout)] if args.timeout else [] )]
        records["suite"] = run_worker_json(args.worker_b_json, suite_args, args.command_timeout)
        records["node_post"] = run_worker_json(args.worker_b_json, node_identity_args, args.command_timeout)
        records["meta_post"] = run_worker_json(args.worker_a_json, meta_identity_args, args.command_timeout)
        meta_post = latest_event(records["meta_post"], "IDENTITY")["processes"].get("meta")
        node_post_event = latest_event(records["node_post"], "IDENTITY")
        node_post = node_post_event["processes"].get("node")
        suite_event = latest_event(records["suite"], args.worker_suite_event)
        worker_proof = suite_event["proof"]
        post_system = (node_post_event.get("platform") or {}).get("system", worker_system)
        qualification = remote_target_checks(post_system, args.backend, node_post_event["mount"], node_post_event["base_mount"], node_pre, node_post, meta_pre, meta_post, expected_node_sha, expected_meta_sha, args.expected_meta_endpoint, args.require_cross_worker)
        post_mount_stability = {
            "mount_stable": mount_records_stable(mount_record(node_pre_event["mount"]), mount_record(node_post_event["mount"])),
            "base_mount_stable": mount_records_stable(mount_record(node_pre_event["base_mount"]), mount_record(node_post_event["base_mount"])),
            "pre_mount": mount_record(node_pre_event["mount"]),
            "post_mount": mount_record(node_post_event["mount"]),
            "pre_base_mount": mount_record(node_pre_event["base_mount"]),
            "post_base_mount": mount_record(node_post_event["base_mount"]),
        }
        run_dir_stability = worker_run_dir_stability(node_pre_event, node_post_event, args.worker_run_dir_b)
        artifact_validation = validate_worker_artifacts(worker_proof)
        if worker_proof.get("status") == "PASS" and not artifact_validation["valid"]:
            qualification = dict(qualification)
            qualification["worker-artifacts-complete"] = False
        post_checks = [
            build_check("remote-target-identity", "PASS" if all(qualification.values()) else "BLOCKED", qualification),
            build_check("remote-mount-stability", "PASS" if post_mount_stability["mount_stable"] and post_mount_stability["base_mount_stable"] else "BLOCKED", post_mount_stability),
            build_check("worker-run-dir-b-stability", "PASS" if run_dir_stability["stable"] else "BLOCKED", run_dir_stability),
            build_check("remote-worker-artifacts", "PASS" if artifact_validation["valid"] or worker_proof.get("status") != "PASS" else "BLOCKED", artifact_validation),
        ]
        worker_manifest = ((worker_proof.get("artifacts") or {}).get("manifest") or [])
        host_records_artifact = rel(artifacts / "remote-worker-records.json", run_dir)
        checks = checks + remote_check_artifacts(list(worker_proof.get("checks") or []), worker_manifest, host_records_artifact) + post_checks
        qualified = all(qualification.values()) and post_mount_stability["mount_stable"] and post_mount_stability["base_mount_stable"] and run_dir_stability["stable"] and (artifact_validation["valid"] or worker_proof.get("status") != "PASS")
        status = worker_proof.get("status") if qualified else "BLOCKED"
        reason = worker_proof.get("reason") if qualified else "remote target identity or artifact qualification failed"
        return finish(status, reason, worker_proof)
    except Exception as exc:  # noqa: BLE001 - preserve structured proof for bad/missing worker evidence
        checks.append(build_check("host-orchestration", "BLOCKED", {"exception": type(exc).__name__, "message": str(exc)}))
        return finish("BLOCKED", f"host orchestration failed: {type(exc).__name__}: {exc}")

def parse_args(argv: list[str]) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Run AFS STD-01 pjdfstest driver")
    parser.add_argument("--case-id", default=os.environ.get("AFS_ACCEPTANCE_CASE_ID", "STD-01"))
    parser.add_argument("--profile", choices=["smoke", "full"], default=os.environ.get("AFS_ACCEPTANCE_PROFILE", "smoke"))
    parser.add_argument("--matrix-json", default=os.environ.get("AFS_ACCEPTANCE_MATRIX", "{}"))
    parser.add_argument("--run-dir", type=Path, default=Path(os.environ.get("AFS_ACCEPTANCE_RUN_DIR", "results/std-01-driver")))
    parser.add_argument("--mount", type=Path, default=Path(os.environ["AFS_ACCEPTANCE_MOUNT"]) if os.environ.get("AFS_ACCEPTANCE_MOUNT") else None)
    parser.add_argument("--base-dir", type=Path, default=Path(os.environ["AFS_ACCEPTANCE_BASE_DIR"]) if os.environ.get("AFS_ACCEPTANCE_BASE_DIR") else None, help="Directory under --mount that contains the temporary pjdfstest fixture. Relative paths are resolved below --mount.")
    parser.add_argument("--suite-root", type=Path, default=DEFAULT_SUITE_ROOT)
    parser.add_argument("--timeout", type=int, default=None, help="pjdfstest subprocess timeout in seconds")
    parser.add_argument("--process-pid", default=os.environ.get("AFS_ACCEPTANCE_PROCESS_PID"))
    parser.add_argument("--meta-process-pid", default=os.environ.get("AFS_ACCEPTANCE_META_PROCESS_PID"))
    parser.add_argument("--backend", default=os.environ.get("AFS_ACCEPTANCE_BACKEND"), help="Observed product backend label, for proof identity only.")
    parser.add_argument("--meta", default=os.environ.get("AFS_ACCEPTANCE_META"), help="Observed Meta backend label, for proof identity only.")
    return parser.parse_args(argv)


def legacy_main(argv: list[str]) -> int:
    args = parse_args(argv)
    run_dir = args.run_dir.resolve()
    artifacts = run_dir / "artifacts" / "std-01-pjdfstest"
    artifacts.mkdir(parents=True, exist_ok=True)
    matrix = json.loads(args.matrix_json) if args.matrix_json.strip() else {}
    if not isinstance(matrix, dict):
        matrix = {}
    matrix.setdefault("reference", "ext4")
    matrix.setdefault("suite_sha", f"sanwan/pjdfstest {PJDFS_REV}")

    checks: list[dict[str, Any]] = []
    status = "PASS"
    reason = ""
    selected_tests: list[str] = []
    accounting: dict[str, Any] = {}
    command_result: dict[str, Any] | None = None
    fixture: Path | None = None
    fixture_kept: bool | None = None

    try:
        if args.case_id != "STD-01":
            raise RuntimeError(f"standard.py currently implements STD-01 only, got {args.case_id}")
        if args.mount is None:
            raise RuntimeError("--mount or AFS_ACCEPTANCE_MOUNT is required")

        suite = suite_identity(args.suite_root)
        discovery = discover_tests(args.suite_root)
        base_dir = resolve_base_dir(args.mount, args.base_dir)
        mnt = mount_identity(args.mount)
        base_mnt = mount_identity(base_dir)
        proc = process_identity(args.process_pid)
        meta_proc = process_identity(args.meta_process_pid)
        product_identity = {
            "backend": args.backend or matrix.get("backend"),
            "meta": args.meta or matrix.get("meta"),
            "transport": matrix.get("transport"),
            "single_backend_selection": bool(args.backend or matrix.get("backend")),
            "mount_path": str(args.mount),
            "base_dir": str(base_dir),
            "process_pid": args.process_pid,
            "meta_process_pid": args.meta_process_pid,
        }
        identity = {
            "created_at": utc(),
            "host": run_text(["hostname"]),
            "uname": run_text(["uname", "-a"]),
            "platform": {"system": platform.system(), "release": platform.release(), "machine": platform.machine(), "python": platform.python_version(), "uid": os.geteuid(), "gid": os.getegid()},
            "suite": suite,
            "mount": mnt,
            "base_mount": base_mnt,
            "process": proc,
            "meta_process": meta_proc,
            "product": product_identity,
            "lock_state_note": "acceptance.lock.json may remain PREPARING; driver readiness is not a release PASS.",
        }
        write_json(artifacts / "identity.json", identity)
        write_json(artifacts / "discovery.json", discovery)

        suite_ok = suite["git_head"] == PJDFS_REV and suite["executable_exists"]
        checks.append(build_check("pinned-suite-identity", "PASS" if suite_ok else "FAIL", {"expected": PJDFS_REV, "observed": suite.get("git_head"), "executable_exists": suite.get("executable_exists")}, rel(artifacts / "identity.json", run_dir)))
        root_ok = os.geteuid() == 0
        checks.append(build_check("root-harness", "PASS" if root_ok else "BLOCKED", {"euid": os.geteuid(), "uid_drop_test_file_count": discovery["uid_drop_test_file_count"], "note": "pjdfstest must run as root; internal -u/-g tests provide non-root coverage."}, rel(artifacts / "discovery.json", run_dir)))
        mount_ok = mnt.get("returncode") == 0 and bool(mnt.get("stdout", "").strip())
        base_dir_ok = base_dir.is_dir() and is_under(base_dir, args.mount)
        checks.append(build_check("mount-identity", "PASS" if mount_ok else "BLOCKED", {"mount": str(args.mount), "findmnt_returncode": mnt.get("returncode")}, rel(artifacts / "identity.json", run_dir)))
        checks.append(build_check("base-dir-scope", "PASS" if base_dir_ok else "BLOCKED", {"mount": str(args.mount), "base_dir": str(base_dir), "exists": base_dir.exists(), "is_dir": base_dir.is_dir(), "under_mount": is_under(base_dir, args.mount)}, rel(artifacts / "identity.json", run_dir)))
        backend_ok = bool(product_identity["backend"])
        checks.append(build_check("backend-selection", "PASS" if backend_ok else "BLOCKED", product_identity, rel(artifacts / "identity.json", run_dir)))
        observed_checks = target_checks(platform.system(), product_identity["backend"], mnt, base_mnt, proc, meta_proc)
        for name, passed in observed_checks.items():
            checks.append(build_check(name, "PASS" if passed else "BLOCKED", {"observed": passed}, rel(artifacts / "identity.json", run_dir)))
        target_ok = all(observed_checks.values())
        discovery_ok = discovery["discovered_files"] > 0
        checks.append(build_check("discovery", "PASS" if discovery_ok else "BLOCKED", {"discovered_files": discovery["discovered_files"], "uid_drop_test_file_count": discovery["uid_drop_test_file_count"], "todo_source_file_count": discovery["todo_source_file_count"]}, rel(artifacts / "discovery.json", run_dir)))

        if not suite_ok or not root_ok or not mount_ok or not base_dir_ok or not backend_ok or not discovery_ok or not target_ok:
            status = "BLOCKED" if not (root_ok and mount_ok and base_dir_ok and backend_ok and discovery_ok and target_ok) else "FAIL"
            reason = "pjdfstest preflight failed"
        else:
            tests_root = args.suite_root / "tests"
            if args.profile == "smoke":
                selected_tests = SMOKE_TESTS
                timeout = args.timeout or 180
            else:
                selected_tests = discovery["tests"]
                timeout = args.timeout or 1800
            test_paths = [str(tests_root / rel_test) for rel_test in selected_tests]
            fixture = base_dir / f".afs-std01-pjdfstest-{dt.datetime.now(dt.timezone.utc).strftime('%Y%m%dT%H%M%SZ')}-{uuid.uuid4().hex[:8]}"
            fixture.mkdir(mode=0o755)
            # pjdfstest runs as root but many upstream checks drop to numeric
            # non-root users through the harness. Those child processes must be
            # able to traverse the harness root; a 0700 root-owned fixture makes
            # valid non-root relative-path cases fail with EACCES on ext4.
            os.chmod(fixture, 0o755)
            stdout = artifacts / "pjdfstest.stdout.tap"
            stderr = artifacts / "pjdfstest.stderr.log"
            command = ["prove", "-e", "/bin/sh", "-rv", *test_paths]
            command_result = run_bounded(command, cwd=fixture, timeout=timeout, stdout_path=stdout, stderr_path=stderr)
            write_json(artifacts / "command.json", command_result)
            accounting = parse_tap_and_prove(stdout, stderr, selected_tests)
            accounting.update({
                "profile": args.profile,
                "discovered_files": discovery["discovered_files"],
                "not_selected_files": max(0, discovery["discovered_files"] - len(selected_tests)),
                "upstream_todo_source_file_count": discovery["todo_source_file_count"],
                "uid_drop_test_file_count": discovery["uid_drop_test_file_count"],
            })
            write_json(artifacts / "tap-accounting.json", accounting)
            checks.append(build_check("subprocess-bound", "PASS" if not command_result["timed_out"] else "BLOCKED", {"timeout_seconds": command_result["timeout_seconds"], "timed_out": command_result["timed_out"], "returncode": command_result["returncode"]}, rel(artifacts / "command.json", run_dir)))
            accounting_ok = complete_accounting(accounting, len(selected_tests))
            checks.append(build_check("tap-accounting", "PASS" if accounting_ok else "FAIL", accounting, rel(artifacts / "tap-accounting.json", run_dir)))
            uid_drop_executed = bool(set(selected_tests) & set(discovery["uid_drop_test_files"]))
            uid_drop_status = "PASS" if uid_drop_executed or args.profile == "smoke" else "BLOCKED"
            checks.append(build_check("uid-drop-coverage", uid_drop_status, {"executed_uid_drop_tests": sorted(set(selected_tests) & set(discovery["uid_drop_test_files"])), "total_uid_drop_test_files": discovery["uid_drop_test_file_count"], "profile": args.profile, "note": "smoke verifies harness wiring; full profile must include upstream uid/gid drop tests."}, rel(artifacts / "tap-accounting.json", run_dir)))
            result_ok = accounting_ok and command_result["returncode"] == 0 and not command_result["timed_out"] and accounting["tap_unexpected_fail"] == 0 and accounting.get("prove_result") == "PASS"
            checks.append(build_check("pjdfstest-result", "PASS" if result_ok else "FAIL", {"returncode": command_result["returncode"], "prove_result": accounting.get("prove_result"), "tap_unexpected_fail": accounting.get("tap_unexpected_fail"), "tap_todo": accounting.get("tap_todo")}, rel(artifacts / "pjdfstest.stdout.tap", run_dir)))
            if command_result["timed_out"]:
                status = "BLOCKED"
                reason = f"pjdfstest timed out after {command_result['timeout_seconds']}s"
            elif not result_ok:
                status = "FAIL"
                reason = "pjdfstest reported failures; raw TAP was preserved without filtering"
            if status == "PASS" and fixture.exists():
                try:
                    shutil.rmtree(fixture)
                    fixture_kept = False
                    checks.append(build_check("cleanup-fixture", "PASS", {"fixture": str(fixture), "kept": False}))
                except Exception as cleanup_exc:  # noqa: BLE001 - preserve suite accounting and report cleanup separately
                    fixture_kept = True
                    cleanup_artifact = artifacts / "cleanup-error.json"
                    write_json(cleanup_artifact, {"fixture": str(fixture), "exception": type(cleanup_exc).__name__, "message": str(cleanup_exc), "traceback": traceback.format_exc()})
                    checks.append(build_check("cleanup-fixture", "FAIL", {"fixture": str(fixture), "kept": True, "error": f"{type(cleanup_exc).__name__}: {cleanup_exc}"}, rel(cleanup_artifact, run_dir)))
                    status = "FAIL"
                    reason = "pjdfstest passed but fixture cleanup failed; accounting was preserved"
            else:
                fixture_kept = bool(fixture and fixture.exists())
    except Exception as exc:  # noqa: BLE001 - proof must preserve setup failure
        status = "BLOCKED"
        reason = f"driver setup failed: {type(exc).__name__}: {exc}"
        write_json(artifacts / "setup-error.json", {"exception": type(exc).__name__, "message": str(exc), "traceback": traceback.format_exc()})
        checks.append(build_check("driver-setup", "BLOCKED", reason, rel(artifacts / "setup-error.json", run_dir)))

    coverage_axes = {
        "reference": {"values": [str(matrix.get("reference", "ext4"))], "checks": {str(matrix.get("reference", "ext4")): "mount-identity"}},
        "suite_sha": {"values": [str(matrix.get("suite_sha", f"sanwan/pjdfstest {PJDFS_REV}"))], "checks": {str(matrix.get("suite_sha", f"sanwan/pjdfstest {PJDFS_REV}")): "pinned-suite-identity"}},
    }
    proof = {
        "case_id": args.case_id,
        "profile": args.profile,
        "matrix": matrix,
        "status": status,
        "reason": reason,
        "checks": checks,
        "coverage": {"profile": args.profile, "axes": coverage_axes},
        "artifacts": {"root": rel(artifacts, run_dir)},
        "fixture": {"path": str(fixture) if fixture else None, "base_dir": str(resolve_base_dir(args.mount, args.base_dir)) if args.mount else None, "kept": fixture_kept},
        "identity": {"artifact": rel(artifacts / "identity.json", run_dir), "product": (identity.get("product") if "identity" in locals() else None)},
        "accounting": accounting,
        "command": command_result,
        "notes": [
            "STD-01 driver READY means the pjdfstest harness can run and report proof; it is not a release acceptance PASS by itself.",
            "No failures are filtered after execution. TODO and SKIP are counted in TAP accounting.",
        ],
    }
    write_json(artifacts / "proof.json", proof)
    print(json.dumps(proof, sort_keys=True))
    return 0 if status == "PASS" else 1


def main(argv: list[str]) -> int:
    if argv and argv[0] == "host":
        return host_main(argv[1:])
    if argv and argv[0] == "worker-identity":
        return worker_identity_main(argv[1:])
    if argv and argv[0] == "worker-pjdfstest":
        return worker_pjdfstest_main(argv[1:])
    if argv and argv[0] == "worker-driver":
        return worker_driver_main(argv[1:])
    return legacy_main(argv)


if __name__ == "__main__":
    raise SystemExit(main(sys.argv[1:]))
