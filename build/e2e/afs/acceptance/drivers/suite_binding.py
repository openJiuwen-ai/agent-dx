#!/usr/bin/env python3
"""Bind acceptance runner STD cases to existing standard-suite drivers.

The acceptance runner owns case/profile/matrix/run-dir selection.  This helper
only supplies per-run runtime inputs such as mounts, suite roots and observed
process ids to the fixed STD-01..STD-04 driver CLIs.
"""
from __future__ import annotations

import json
import os
import subprocess
import sys
from pathlib import Path
from typing import Any


STATUS_VALUES = {"PASS", "FAIL", "BLOCKED", "INCONCLUSIVE", "EXCLUDED"}
DRIVER_DIR = Path(__file__).resolve().parent
REMOTE_COMMON_KEYS = {
    "mode",
    "worker_a",
    "worker_b",
    "driver_b",
    "expected_meta_endpoint",
    "meta_identity_path_a",
    "mount_b",
    "base_dir_b",
    "suite_root_b",
    "worker_run_dir_b",
    "timeout",
    "command_timeout",
}

COMMON_KEYS = {
    "case_id",
    "profile",
    "matrix",
    "driver_matrix",
    "mount",
    "base_dir",
    "process_pid",
    "meta_process_pid",
}
CASE_CONFIG: dict[str, dict[str, Any]] = {
    "STD-01": {
        "driver": "standard.py",
        "extra_keys": {
            "suite_root",
            "timeout",
        } | REMOTE_COMMON_KEYS,
        "path_flags": {"mount": "--mount", "base_dir": "--base-dir", "suite_root": "--suite-root"},
        "value_flags": {"process_pid": "--process-pid", "meta_process_pid": "--meta-process-pid", "timeout": "--timeout"},
        "driver_matrix_keys": {"reference", "suite_sha"},
    },
    "STD-02": {
        "driver": "ltp.py",
        "extra_keys": {
            "suite_root",
            "ltp_install",
            "expanded_tsv",
            "per_test_timeout",
            "applicability_manifest",
            "ltp_install_b",
            "expanded_tsv_b",
            "applicability_manifest_b",
        } | REMOTE_COMMON_KEYS,
        "path_flags": {
            "mount": "--mount",
            "base_dir": "--base-dir",
            "suite_root": "--suite-root",
            "ltp_install": "--ltp-install",
            "expanded_tsv": "--expanded-tsv",
            "applicability_manifest": "--applicability-manifest",
        },
        "value_flags": {"process_pid": "--process-pid", "meta_process_pid": "--meta-process-pid", "per_test_timeout": "--per-test-timeout"},
        "driver_matrix_keys": {"reference", "suite"},
    },
    "STD-03": {
        "driver": "fsx.py",
        "extra_keys": {
            "suite_root",
            "fsx_binary",
            "per_seed_timeout",
            "failure_replay_attempts",
            "failure_replay_timeout",
            "fsx_binary_b",
        } | REMOTE_COMMON_KEYS,
        "path_flags": {"mount": "--mount", "base_dir": "--base-dir", "suite_root": "--suite-root", "fsx_binary": "--fsx-binary"},
        "value_flags": {
            "process_pid": "--process-pid",
            "meta_process_pid": "--meta-process-pid",
            "per_seed_timeout": "--per-seed-timeout",
            "failure_replay_attempts": "--failure-replay-attempts",
            "failure_replay_timeout": "--failure-replay-timeout",
        },
        "driver_matrix_keys": {"reference", "suite_sha", "seeds"},
    },
    "STD-04": {
        "driver": "random_fs.py",
        "extra_keys": {"reference_dir", "per_seed_timeout_seconds", "reference_dir_b"} | REMOTE_COMMON_KEYS,
        "path_flags": {"mount": "--mount", "base_dir": "--base-dir", "reference_dir": "--reference-dir"},
        "value_flags": {
            "process_pid": "--process-pid",
            "meta_process_pid": "--meta-process-pid",
            "per_seed_timeout_seconds": "--per-seed-timeout-seconds",
        },
        "driver_matrix_keys": {"reference", "seeds", "operations_per_seed"},
    },
}
RESERVED_MATRIX_KEYS = {"backend", "meta", "transport"}
PROCESS_ROLES = {"node", "meta"}


class BindingError(RuntimeError):
    pass


def load_json(path: Path) -> Any:
    with path.open("r", encoding="utf-8") as handle:
        return json.load(handle)


def matrix_from_env(env: dict[str, str]) -> dict[str, str]:
    raw = env.get("AFS_ACCEPTANCE_MATRIX", "{}")
    try:
        parsed = json.loads(raw)
    except json.JSONDecodeError as exc:
        raise BindingError(f"AFS_ACCEPTANCE_MATRIX is not valid JSON: {exc}") from exc
    if not isinstance(parsed, dict):
        raise BindingError("AFS_ACCEPTANCE_MATRIX must decode to an object")
    return {str(key): str(value) for key, value in parsed.items()}


def required_context(env: dict[str, str]) -> tuple[str, str, dict[str, str], Path]:
    case_id = env.get("AFS_ACCEPTANCE_CASE_ID")
    profile = env.get("AFS_ACCEPTANCE_PROFILE")
    run_dir = env.get("AFS_ACCEPTANCE_RUN_DIR")
    if not case_id:
        raise BindingError("AFS_ACCEPTANCE_CASE_ID is missing")
    if not profile:
        raise BindingError("AFS_ACCEPTANCE_PROFILE is missing")
    if not run_dir:
        raise BindingError("AFS_ACCEPTANCE_RUN_DIR is missing")
    if profile not in {"smoke", "full"}:
        raise BindingError(f"unsupported profile {profile!r}")
    if case_id not in CASE_CONFIG:
        raise BindingError(f"unsupported suite binding case {case_id!r}")
    return case_id, profile, matrix_from_env(env), Path(run_dir)


def normalized_binding_matrix(binding: dict[str, Any]) -> dict[str, str]:
    matrix = binding.get("matrix")
    if not isinstance(matrix, dict):
        raise BindingError("binding matrix must be an object")
    return {str(key): str(value) for key, value in matrix.items()}


def binding_matches(binding: dict[str, Any], case_id: str, profile: str, matrix: dict[str, str]) -> bool:
    if binding.get("case_id") != case_id:
        return False
    binding_profile = binding.get("profile")
    if binding_profile is not None and binding_profile != profile:
        return False
    return normalized_binding_matrix(binding) == matrix


def select_binding(document: Any, case_id: str, profile: str, matrix: dict[str, str]) -> dict[str, Any]:
    if not isinstance(document, dict):
        raise BindingError("suite binding file must contain a JSON object")
    if document.get("schema_version") != 1:
        raise BindingError("suite binding file schema_version must be 1")
    bindings = document.get("bindings")
    if not isinstance(bindings, list):
        raise BindingError("suite binding file must contain a bindings list")
    matches: list[dict[str, Any]] = []
    for item in bindings:
        if not isinstance(item, dict):
            raise BindingError("each binding entry must be an object")
        if binding_matches(item, case_id, profile, matrix):
            matches.append(item)
    if not matches:
        raise BindingError(f"no binding for {case_id} with exact matrix {json.dumps(matrix, sort_keys=True)}")
    if len(matches) > 1:
        raise BindingError(f"ambiguous bindings for {case_id} with exact matrix {json.dumps(matrix, sort_keys=True)}")
    return matches[0]


def validate_binding(binding: dict[str, Any], case_id: str, profile: str, matrix: dict[str, str]) -> None:
    config = CASE_CONFIG[case_id]
    allowed = COMMON_KEYS | set(config["extra_keys"])
    unknown = sorted(set(binding) - allowed)
    if unknown:
        raise BindingError(f"unsupported binding field(s): {', '.join(unknown)}")
    if binding.get("profile") not in {None, profile}:
        raise BindingError("binding profile conflicts with runner profile")
    if normalized_binding_matrix(binding) != matrix:
        raise BindingError("binding matrix does not exactly match runner matrix")
    driver_matrix = binding.get("driver_matrix", {})
    if driver_matrix is None:
        driver_matrix = {}
    if not isinstance(driver_matrix, dict):
        raise BindingError("driver_matrix must be an object when present")
    reserved = sorted(set(driver_matrix) & RESERVED_MATRIX_KEYS)
    if reserved:
        raise BindingError(f"driver_matrix may not override runner matrix field(s): {', '.join(reserved)}")
    unsupported_axes = sorted(set(driver_matrix) - set(config["driver_matrix_keys"]))
    if unsupported_axes:
        raise BindingError(f"unsupported driver_matrix field(s) for {case_id}: {', '.join(unsupported_axes)}")
    remote_mode = binding.get("mode") == "remote-host"
    if binding.get("mode") not in {None, "local", "remote-host"}:
        raise BindingError("binding mode must be local or remote-host")
    if remote_mode:
        if not matrix.get("backend"):
            raise BindingError(f"remote-host {case_id} requires runner matrix backend")
        validate_remote_binding(binding, case_id)
    elif not binding.get("mount"):
        raise BindingError("binding mount is required")
    if case_id == "STD-04" and not remote_mode and not binding.get("reference_dir"):
        raise BindingError("STD-04 binding reference_dir is required")


def validate_absolute_path(value: Any, name: str) -> str:
    if not isinstance(value, str) or not value:
        raise BindingError(f"{name} is required")
    if not value.startswith("/"):
        raise BindingError(f"{name} must be an absolute Linux path")
    if value.startswith(("/Users/", "/Volumes/", "/private/", "/var/folders/")):
        raise BindingError(f"{name} must not be a macOS host path")
    return value


def validate_ssh_host(value: Any, name: str) -> str:
    if not isinstance(value, str) or not value:
        raise BindingError(f"{name} is required")
    if value.startswith("-") or any(ch.isspace() for ch in value):
        raise BindingError(f"{name} must be an ssh config host or user@host without whitespace/options")
    return value


def validate_process_record(value: Any, expected_role: str, name: str) -> tuple[str, int, str]:
    if not isinstance(value, dict):
        raise BindingError(f"{name} must be an object")
    role = value.get("role")
    if role not in PROCESS_ROLES:
        raise BindingError(f"{name}.role must be node or meta")
    if role != expected_role:
        raise BindingError(f"{name}.role must be {expected_role}")
    pid = value.get("pid")
    if isinstance(pid, str) and pid.isdecimal():
        pid = int(pid, 10)
    if not isinstance(pid, int) or pid <= 0:
        raise BindingError(f"{name}.pid must be a positive guest pid")
    sha = value.get("sha256")
    if not isinstance(sha, str) or len(sha) != 64 or any(ch not in "0123456789abcdef" for ch in sha):
        raise BindingError(f"{name}.sha256 must be 64 lowercase hex characters")
    return role, pid, sha


def validate_worker(value: Any, expected_role: str, name: str) -> dict[str, Any]:
    if not isinstance(value, dict):
        raise BindingError(f"{name} must be an object")
    unknown = sorted(set(value) - {"transport", "host", "ssh_config", "python", "standard_driver", "expected_processes"})
    if unknown:
        raise BindingError(f"unsupported {name} field(s): {', '.join(unknown)}")
    if value.get("transport") != "ssh":
        raise BindingError(f"{name}.transport must be ssh")
    validate_ssh_host(value.get("host"), f"{name}.host")
    if value.get("ssh_config") is not None:
        validate_absolute_path(value.get("ssh_config"), f"{name}.ssh_config")
    validate_absolute_path(value.get("python"), f"{name}.python")
    validate_absolute_path(value.get("standard_driver"), f"{name}.standard_driver")
    records = value.get("expected_processes")
    if not isinstance(records, list) or not records:
        raise BindingError(f"{name}.expected_processes must be a non-empty list")
    parsed = [validate_process_record(record, expected_role, f"{name}.expected_processes[{idx}]") for idx, record in enumerate(records)]
    roles = {role for role, _pid, _sha in parsed}
    if roles != {expected_role}:
        raise BindingError(f"{name}.expected_processes must contain only {expected_role}")
    return value


def validate_remote_binding(binding: dict[str, Any], case_id: str) -> None:
    local_keys = {"mount", "base_dir", "suite_root", "process_pid", "meta_process_pid"}
    if case_id == "STD-02":
        local_keys |= {"ltp_install", "expanded_tsv", "applicability_manifest"}
    elif case_id == "STD-03":
        local_keys |= {"fsx_binary"}
    elif case_id == "STD-04":
        local_keys |= {"reference_dir"}
    forbidden = sorted(key for key in local_keys if binding.get(key) is not None)
    if forbidden:
        raise BindingError(f"remote-host binding may not use local {case_id} field(s): {', '.join(forbidden)}")
    validate_worker(binding.get("worker_a"), "meta", "worker_a")
    worker_b = validate_worker(binding.get("worker_b"), "node", "worker_b")
    endpoint = binding.get("expected_meta_endpoint")
    if not isinstance(endpoint, str) or not endpoint:
        raise BindingError("expected_meta_endpoint is required")
    validate_absolute_path(binding.get("mount_b"), "mount_b")
    validate_absolute_path(binding.get("worker_run_dir_b"), "worker_run_dir_b")
    remote_driver = binding.get("driver_b") or str(Path(worker_b["standard_driver"]).with_name(CASE_CONFIG[case_id]["driver"]))
    validate_absolute_path(remote_driver, "driver_b")
    if binding.get("meta_identity_path_a") is not None:
        validate_absolute_path(binding.get("meta_identity_path_a"), "meta_identity_path_a")
    for key in ("base_dir_b", "suite_root_b"):
        if binding.get(key) is not None:
            validate_absolute_path(binding.get(key), key)
    for key in ("ltp_install_b", "expanded_tsv_b", "applicability_manifest_b", "fsx_binary_b", "reference_dir_b"):
        if binding.get(key) is not None:
            validate_absolute_path(binding.get(key), key)
    if case_id == "STD-04":
        validate_absolute_path(binding.get("reference_dir_b"), "reference_dir_b")
    for key in ("timeout", "command_timeout"):
        if binding.get(key) is not None and (not isinstance(binding.get(key), int) or binding.get(key) <= 0):
            raise BindingError(f"{key} must be a positive integer")


def child_matrix(binding: dict[str, Any], matrix: dict[str, str]) -> dict[str, Any]:
    merged: dict[str, Any] = dict(matrix)
    for key, value in (binding.get("driver_matrix") or {}).items():
        merged[str(key)] = value
    return merged


def add_flag(argv: list[str], flag: str, value: Any) -> None:
    if value is None:
        return
    text = str(value)
    if text:
        argv.extend([flag, text])


def child_env(parent: dict[str, str], matrix: dict[str, str]) -> dict[str, str]:
    env = dict(parent)
    for axis, name in (("backend", "AFS_ACCEPTANCE_BACKEND"), ("meta", "AFS_ACCEPTANCE_META")):
        selected = matrix.get(axis)
        existing = env.get(name)
        if existing and selected and existing != selected:
            raise BindingError(f"{name}={existing!r} conflicts with runner matrix {axis}={selected!r}")
        if selected:
            env[name] = selected
    return env


def build_child_argv(binding: dict[str, Any], case_id: str, profile: str, matrix: dict[str, str], run_dir: Path) -> list[str]:
    if binding.get("mode") == "remote-host":
        return build_remote_argv(binding, case_id, profile, matrix, run_dir)
    config = CASE_CONFIG[case_id]
    argv = [
        sys.executable,
        str(DRIVER_DIR / config["driver"]),
        "--case-id",
        case_id,
        "--profile",
        profile,
        "--matrix-json",
        json.dumps(child_matrix(binding, matrix), sort_keys=True),
        "--run-dir",
        str(run_dir),
    ]
    for key, flag in config["path_flags"].items():
        add_flag(argv, flag, binding.get(key))
    for key, flag in config["value_flags"].items():
        add_flag(argv, flag, binding.get(key))
    for axis, flag in (("backend", "--backend"), ("meta", "--meta")):
        if matrix.get(axis):
            argv.extend([flag, matrix[axis]])
    return argv


def worker_prefix(worker: dict[str, Any]) -> list[str]:
    prefix = ["ssh", "-o", "BatchMode=yes"]
    if worker.get("ssh_config"):
        prefix.extend(["-F", worker["ssh_config"]])
    prefix.extend(["--", worker["host"], worker["python"], worker["standard_driver"]])
    return prefix


def expected_process_args(worker: dict[str, Any], flag: str) -> list[str]:
    args: list[str] = []
    for index, record in enumerate(worker["expected_processes"]):
        role, pid, sha = validate_process_record(record, record["role"], f"expected_processes[{index}]")
        args.extend([flag, f"{role}={pid}:{sha}"])
    return args


def worker_process_pid(worker: dict[str, Any], expected_role: str) -> int:
    pids: list[int] = []
    for index, record in enumerate(worker["expected_processes"]):
        role, pid, _sha = validate_process_record(record, expected_role, f"expected_processes[{index}]")
        if role == expected_role:
            pids.append(pid)
    if len(pids) != 1:
        raise BindingError(f"remote {expected_role} worker must bind exactly one expected process")
    return pids[0]


def remote_driver_path(binding: dict[str, Any], case_id: str, worker_b: dict[str, Any]) -> str:
    return str(binding.get("driver_b") or Path(worker_b["standard_driver"]).with_name(CASE_CONFIG[case_id]["driver"]))


def remote_suite_args(binding: dict[str, Any], case_id: str, profile: str, matrix: dict[str, str], worker_b: dict[str, Any]) -> tuple[str, list[str]]:
    child = child_matrix(binding, matrix)
    if case_id == "STD-01":
        args = [
            "worker-pjdfstest",
            "--case-id", case_id,
            "--profile", profile,
            "--matrix-json", json.dumps(child, sort_keys=True),
            "--run-dir", str(binding["worker_run_dir_b"]),
            "--mount", str(binding["mount_b"]),
            "--backend", matrix["backend"],
        ]
        event = "PJDFSTEST"
    else:
        args = [
            "worker-driver",
            "--driver", remote_driver_path(binding, case_id, worker_b),
            "--",
            "--case-id", case_id,
            "--profile", profile,
            "--matrix-json", json.dumps(child, sort_keys=True),
            "--run-dir", str(binding["worker_run_dir_b"]),
            "--mount", str(binding["mount_b"]),
            "--backend", matrix["backend"],
            "--process-pid", str(worker_process_pid(worker_b, "node")),
        ]
        event = "DRIVER"
        for axis, flag in (("meta", "--meta"),):
            if matrix.get(axis):
                args.extend([flag, matrix[axis]])
    if binding.get("base_dir_b"):
        args.extend(["--base-dir", str(binding["base_dir_b"])])
    if binding.get("suite_root_b"):
        args.extend(["--suite-root", str(binding["suite_root_b"])])
    if case_id == "STD-01" and binding.get("timeout"):
        args.extend(["--timeout", str(binding["timeout"])])
    elif case_id == "STD-02":
        for key, flag in (("ltp_install_b", "--ltp-install"), ("expanded_tsv_b", "--expanded-tsv"), ("applicability_manifest_b", "--applicability-manifest"), ("per_test_timeout", "--per-test-timeout")):
            add_flag(args, flag, binding.get(key))
    elif case_id == "STD-03":
        for key, flag in (("fsx_binary_b", "--fsx-binary"), ("per_seed_timeout", "--per-seed-timeout"), ("failure_replay_attempts", "--failure-replay-attempts"), ("failure_replay_timeout", "--failure-replay-timeout")):
            add_flag(args, flag, binding.get(key))
    elif case_id == "STD-04":
        args.extend(["--reference-dir", str(binding["reference_dir_b"])])
        add_flag(args, "--per-seed-timeout-seconds", binding.get("per_seed_timeout_seconds"))
    return event, args


def build_remote_argv(binding: dict[str, Any], case_id: str, profile: str, matrix: dict[str, str], run_dir: Path) -> list[str]:
    worker_a = validate_worker(binding["worker_a"], "meta", "worker_a")
    worker_b = validate_worker(binding["worker_b"], "node", "worker_b")
    suite_event, suite_args = remote_suite_args(binding, case_id, profile, matrix, worker_b)
    argv = [
        sys.executable,
        str(DRIVER_DIR / "standard.py"),
        "host",
        "--worker-a-json",
        json.dumps(worker_prefix(worker_a)),
        "--worker-b-json",
        json.dumps(worker_prefix(worker_b)),
        "--expected-meta-endpoint",
        str(binding["expected_meta_endpoint"]),
        "--mount-b",
        str(binding["mount_b"]),
        "--worker-run-dir-b",
        str(binding["worker_run_dir_b"]),
        "--worker-suite-event",
        suite_event,
        "--worker-suite-args-json",
        json.dumps(suite_args),
        "--backend",
        matrix["backend"],
        "--profile",
        profile,
        "--case-id",
        case_id,
        "--matrix-json",
        json.dumps(child_matrix(binding, matrix), sort_keys=True),
        "--run-dir",
        str(run_dir),
    ]
    argv.extend(expected_process_args(worker_a, "--worker-a-expected-process"))
    argv.extend(expected_process_args(worker_b, "--worker-b-expected-process"))
    for key, flag in (
        ("meta_identity_path_a", "--meta-identity-path-a"),
        ("base_dir_b", "--base-dir-b"),
        ("suite_root_b", "--suite-root-b"),
        ("timeout", "--timeout"),
        ("command_timeout", "--command-timeout"),
    ):
        add_flag(argv, flag, binding.get(key))
    return argv


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


def child_proof_error(proof: dict[str, Any], case_id: str, profile: str, matrix: dict[str, str]) -> str | None:
    status = str(proof.get("status", "")).upper()
    if status not in STATUS_VALUES:
        return "child proof is missing a valid status"
    if proof.get("case_id") != case_id:
        return f"child proof case_id {proof.get('case_id')!r} does not match {case_id!r}"
    if proof.get("profile") != profile:
        return f"child proof profile {proof.get('profile')!r} does not match {profile!r}"
    proof_matrix = proof.get("matrix")
    if not isinstance(proof_matrix, dict):
        return "child proof is missing matrix"
    for key, value in matrix.items():
        if str(proof_matrix.get(key)) != value:
            return f"child proof matrix {key} {proof_matrix.get(key)!r} does not match runner value {value!r}"
    checks = proof.get("checks")
    if not isinstance(checks, list) or not checks:
        return "child proof is missing non-empty checks"
    return None


def blocked_proof(case_id: str, profile: str, matrix: dict[str, str], reason: str) -> dict[str, Any]:
    return {
        "case_id": case_id,
        "profile": profile,
        "matrix": matrix,
        "status": "BLOCKED",
        "reason": reason,
        "checks": [{"name": "suite-binding", "status": "BLOCKED", "evidence": reason}],
    }


def emit_blocked(case_id: str, profile: str, matrix: dict[str, str], reason: str) -> int:
    print(json.dumps(blocked_proof(case_id, profile, matrix, reason), sort_keys=True))
    return 1


def run(env: dict[str, str]) -> int:
    case_id, profile, matrix, run_dir = required_context(env)
    binding_path = env.get("AFS_ACCEPTANCE_SUITE_BINDINGS")
    if not binding_path:
        raise BindingError("AFS_ACCEPTANCE_SUITE_BINDINGS is missing")
    binding = select_binding(load_json(Path(binding_path).resolve()), case_id, profile, matrix)
    validate_binding(binding, case_id, profile, matrix)
    argv = build_child_argv(binding, case_id, profile, matrix, run_dir)
    proc = subprocess.run(
        argv,
        cwd=str(DRIVER_DIR),
        env=child_env(env, matrix),
        shell=False,
        text=True,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        check=False,
    )
    sys.stdout.write(proc.stdout)
    sys.stderr.write(proc.stderr)
    proof = read_child_proof(proc.stdout)
    if proof is None:
        return emit_blocked(case_id, profile, matrix, "child driver did not emit structured JSON proof")
    error = child_proof_error(proof, case_id, profile, matrix)
    if error is not None:
        return emit_blocked(case_id, profile, matrix, error)
    return int(proc.returncode)


def main() -> int:
    case_id = os.environ.get("AFS_ACCEPTANCE_CASE_ID", "UNKNOWN")
    profile = os.environ.get("AFS_ACCEPTANCE_PROFILE", "unknown")
    try:
        matrix = matrix_from_env(os.environ)
    except BindingError:
        matrix = {}
    try:
        return run(os.environ)
    except (OSError, BindingError, json.JSONDecodeError) as exc:
        return emit_blocked(case_id, profile, matrix, f"suite binding blocked: {exc}")


if __name__ == "__main__":
    raise SystemExit(main())
