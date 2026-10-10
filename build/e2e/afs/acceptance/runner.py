#!/usr/bin/env python3
"""AFS acceptance runner skeleton.

This runner is intentionally conservative: a case cannot pass unless a
registered driver returns structured proof for the selected matrix. The current
manifest only has TODO drivers, so real acceptance invocations are BLOCKED.
"""

from __future__ import annotations

import argparse
import datetime as _dt
import hashlib
import json
import os
import platform
import signal
import subprocess
import sys
import time
import uuid
from dataclasses import dataclass
from pathlib import Path
from typing import Any
from xml.etree import ElementTree as ET

import environment


STATUS_VALUES = {"PASS", "FAIL", "BLOCKED", "INCONCLUSIVE", "EXCLUDED"}
NON_PASS_STATUSES = ("FAIL", "BLOCKED", "INCONCLUSIVE", "EXCLUDED")
DEFAULT_TIMEOUT_SECONDS = 30
def find_repo_root(start: Path) -> Path:
    for parent in [start, *start.parents]:
        if (parent / "Cargo.toml").is_file() and (parent / "build" / "e2e").is_dir():
            return parent
    raise AcceptanceError(f"cannot find Agent DX repository root from {start}")


REPO_ROOT = find_repo_root(Path(__file__).resolve())
DEFAULT_CASES = Path(__file__).with_name("cases.json")
DEFAULT_RESULTS = REPO_ROOT / ".local" / "acceptance"
DEFAULT_CONTRACT = REPO_ROOT / "docs" / "testing" / "afs.md"
STRUCTURED_PASS_CHECK_STATUSES = {"PASS", "EXCLUDED"}
CONTROLLED_MATRIX_AXES = {
    "backends": "backend",
    "backend": "backend",
    "meta": "meta",
    "transport": "transport",
    "transport_modes": "transport",
}
PROOF_TAIL_BYTES = 1024 * 1024
IDENTITY_COMPARE_KEYS = ("sha256", "sha", "git_commit")
RELEASE_IDENTITY_COMPONENTS = ("runner", "manifest", "source", "binary", "contract")


@dataclass(frozen=True)
class MatrixSelection:
    backend: str | None = None
    meta: str | None = None
    transport: str | None = None

    def as_dict(self) -> dict[str, str]:
        selected: dict[str, str] = {}
        if self.backend is not None:
            selected["backend"] = self.backend
        if self.meta is not None:
            selected["meta"] = self.meta
        if self.transport is not None:
            selected["transport"] = self.transport
        return selected


class AcceptanceError(RuntimeError):
    pass


def load_json(path: Path) -> Any:
    with path.open("r", encoding="utf-8") as handle:
        return json.load(handle)


def sha256_file(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for chunk in iter(lambda: handle.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def utc_run_id() -> str:
    stamp = _dt.datetime.now(tz=_dt.timezone.utc).strftime("%Y%m%dT%H%M%SZ")
    return f"{stamp}-{uuid.uuid4().hex[:12]}"


def create_run_dir(results_root: Path, requested_run_id: str | None = None) -> Path:
    run_id = requested_run_id or utc_run_id()
    if "/" in run_id or run_id in {"", ".", ".."}:
        raise AcceptanceError(f"invalid run id: {run_id!r}")
    run_dir = results_root / run_id
    run_dir.mkdir(parents=True, exist_ok=False)
    (run_dir / "stdout").mkdir()
    (run_dir / "stderr").mkdir()
    return run_dir


def is_linux_arm64() -> bool:
    machine = platform.machine().lower()
    return sys.platform.startswith("linux") and machine in {"aarch64", "arm64"}


def collect_path_identity(path_value: Any, component: str) -> dict[str, Any]:
    if not isinstance(path_value, str) or not path_value:
        return {}
    path = Path(path_value).expanduser().resolve()
    identity: dict[str, Any] = {"path": str(path)}
    if not path.exists():
        identity["identity_error"] = f"{component} path does not exist"
        return identity
    if not os.access(path, os.R_OK):
        identity["identity_error"] = f"{component} path is not readable"
        return identity
    if path.is_file():
        identity["sha256"] = sha256_file(path)
    elif path.is_dir():
        git_dir = path / ".git"
        if git_dir.exists() and component == "source":
            try:
                commit = subprocess.run(
                    ["git", "-C", str(path), "rev-parse", "HEAD"],
                    shell=False,
                    check=True,
                    capture_output=True,
                    text=True,
                    timeout=10,
                ).stdout.strip()
                dirty = subprocess.run(
                    ["git", "-C", str(path), "status", "--porcelain"],
                    shell=False,
                    check=True,
                    capture_output=True,
                    text=True,
                    timeout=10,
                ).stdout
                identity["git_commit"] = commit
                identity["git_dirty"] = bool(dirty.strip())
            except (OSError, subprocess.SubprocessError) as exc:
                identity["git_error"] = str(exc)
        else:
            identity["identity_error"] = f"{component} path is a directory without supported identity"
    else:
        identity["identity_error"] = f"{component} path is neither file nor supported git directory"
    return identity


def load_identity_attestation(path: str | None) -> dict[str, Any]:
    if path is None:
        return {}
    attestation = load_json(Path(path).resolve())
    if not isinstance(attestation, dict):
        raise AcceptanceError("--identity-attestation must be a JSON object")
    observed: dict[str, Any] = {}
    for component in ("source", "binary"):
        record = attestation.get(component, {})
        if not isinstance(record, dict):
            record = {}
        observed[component] = collect_path_identity(record.get("path"), component)
    return observed


def observed_release_identity(
    manifest_path: Path, attestation: dict[str, Any], contract_path: Path,
) -> dict[str, Any]:
    observed = {
        "runner": {"path": str(Path(__file__).resolve()), "sha256": sha256_file(Path(__file__).resolve())},
        "manifest": {"path": str(manifest_path), "sha256": sha256_file(manifest_path)},
        "source": attestation.get("source", {}),
        "binary": attestation.get("binary", {}),
        "contract": collect_path_identity(str(contract_path), "contract"),
    }
    return observed


def comparable_identity_errors(component: str, expected: Any, observed: Any) -> list[str]:
    if not isinstance(expected, dict) or not expected:
        return [f"lock {component} identity is missing"]
    if not isinstance(observed, dict) or not observed:
        return [f"observed {component} identity is missing"]
    if observed.get("identity_error"):
        return [f"observed {component} identity invalid: {observed['identity_error']}"]
    if observed.get("git_error"):
        return [f"observed {component} git identity failed: {observed['git_error']}"]
    if component == "source" and observed.get("git_commit") and observed.get("git_dirty") is not False:
        return ["observed source git tree is dirty"]
    expected_keys = [key for key in IDENTITY_COMPARE_KEYS if expected.get(key)]
    if not expected_keys:
        return [f"lock {component} identity has no comparable digest or git_commit"]
    errors: list[str] = []
    for key in expected_keys:
        if not observed.get(key):
            errors.append(f"observed {component} identity missing {key}")
        elif str(observed[key]) != str(expected[key]):
            errors.append(f"{component} {key} mismatch: lock={expected[key]!r} observed={observed[key]!r}")
    return errors


def release_lock_validation(lock: dict[str, Any], observed: dict[str, Any]) -> tuple[bool, list[str]]:
    errors: list[str] = []
    state = str(lock.get("state", "")).upper()
    if state != "FROZEN":
        errors.append("lock state is not FROZEN")
    verification = lock.get("verification", {})
    if not isinstance(verification, dict) or str(verification.get("status", "")).upper() != "PASS":
        errors.append("lock verification.status is not PASS")
    for component in RELEASE_IDENTITY_COMPONENTS:
        expected = lock.get(component)
        if component == "source" and expected is None:
            expected = lock.get("source_tree")
        errors.extend(comparable_identity_errors(component, expected, observed.get(component)))
    return not errors, errors


def normalize_command(command: Any) -> list[str] | None:
    if command is None:
        return None
    if not isinstance(command, list) or not command:
        return None
    if not all(isinstance(part, str) and part for part in command):
        return None
    return command


def active_cases(manifest: dict[str, Any]) -> list[dict[str, Any]]:
    return [case for case in manifest.get("cases", []) if case.get("active", True)]


def find_cases(
    manifest: dict[str, Any],
    case_ids: list[str],
    category: str | None,
) -> list[dict[str, Any]]:
    cases = active_cases(manifest)
    if case_ids:
        wanted = set(case_ids)
        known = {case["id"] for case in cases}
        missing = sorted(wanted - known)
        if missing:
            raise AcceptanceError(f"unknown or inactive case id(s): {', '.join(missing)}")
        cases = [case for case in cases if case["id"] in wanted]
    if category is not None:
        cases = [case for case in cases if case.get("category") == category]
    return cases


def matrix_values(case: dict[str, Any], key: str) -> list[str]:
    matrix = case.get("matrix", {})
    if not isinstance(matrix, dict):
        return []
    candidate_keys = [key]
    if key == "transport":
        candidate_keys.append("transport_modes")
    if key == "backend":
        candidate_keys.append("backends")
    for candidate in candidate_keys:
        values = matrix.get(candidate)
        if isinstance(values, list):
            return [str(value) for value in values]
        if isinstance(values, str):
            return [values]
    return []


def axis_applies_to_profile(axis: str, profile: str) -> bool:
    if axis.endswith("_full"):
        return profile == "full"
    if axis.endswith("_smoke"):
        return profile == "smoke"
    return True


def manifest_matrix_axes(case: dict[str, Any], profile: str) -> dict[str, list[str]]:
    matrix = case.get("matrix", {})
    if not isinstance(matrix, dict):
        return {}
    axes: dict[str, list[str]] = {}
    for key, value in matrix.items():
        if not axis_applies_to_profile(key, profile):
            continue
        if isinstance(value, list):
            axes[key] = [str(item) for item in value]
        elif isinstance(value, (str, int, float, bool)):
            axes[key] = [str(value)]
    return axes


def select_matrix(case: dict[str, Any], requested: MatrixSelection, profile: str) -> tuple[list[MatrixSelection], dict[str, Any]]:
    expected = manifest_matrix_axes(case, profile)
    selected_values: dict[str, list[str]] = {}
    driver_owned_axes: dict[str, list[str]] = {}

    missing: dict[str, list[str]] = {}
    for key, expected_values in expected.items():
        controlled = CONTROLLED_MATRIX_AXES.get(key)
        requested_value = getattr(requested, controlled) if controlled is not None else None
        if requested_value is None:
            selected = expected_values if controlled is not None else []
        else:
            selected = [requested_value]
        selected_values[key] = selected
        invalid = sorted(set(selected) - set(expected_values))
        if invalid:
            raise AcceptanceError(f"{case['id']} does not support {key}: {', '.join(invalid)}")
        missing_values = sorted(set(expected_values) - set(selected))
        if missing_values:
            missing[key] = missing_values
        if controlled is None:
            driver_owned_axes[key] = expected_values

    backend_expected = matrix_values(case, "backend")
    meta_expected = matrix_values(case, "meta")
    transport_expected = matrix_values(case, "transport")
    backends = [requested.backend] if requested.backend is not None else backend_expected or [None]
    metas = [requested.meta] if requested.meta is not None else meta_expected or [None]
    transports = [requested.transport] if requested.transport is not None else transport_expected or [None]
    points = [
        MatrixSelection(backend=backend, meta=meta, transport=transport)
        for backend in backends
        for meta in metas
        for transport in transports
    ]
    coverage = {
        "expected": expected,
        "selected": {
            key: [value for value in values if value is not None]
            for key, values in selected_values.items()
        },
        "missing_full_coverage": missing,
        "full_matrix_covered": not missing,
        "driver_owned_axes": driver_owned_axes,
        "driver_axis_coverage": {},
    }
    return points, coverage


def blocked_result(
    case: dict[str, Any],
    profile: str,
    matrix: MatrixSelection,
    reason: str,
    run_dir: Path,
    coverage: dict[str, Any],
) -> dict[str, Any]:
    stdout_path = run_dir / "stdout" / f"{case['id']}-{uuid.uuid4().hex[:8]}.stdout"
    stderr_path = run_dir / "stderr" / f"{case['id']}-{uuid.uuid4().hex[:8]}.stderr"
    stdout_path.write_text("", encoding="utf-8")
    stderr_path.write_text(reason + "\n", encoding="utf-8")
    return {
        "case_id": case["id"],
        "case_name": case.get("name"),
        "category": case.get("category"),
        "profile": profile,
        "matrix": matrix.as_dict(),
        "coverage": coverage,
        "status": "BLOCKED",
        "reason": reason,
        "checks": [{"name": "runner-preflight", "status": "BLOCKED", "evidence": reason}],
        "stdout_path": str(stdout_path),
        "stderr_path": str(stderr_path),
        "artifacts_dir": str(run_dir),
    }


def failure_result(
    case: dict[str, Any],
    profile: str,
    matrix: MatrixSelection,
    reason: str,
    run_dir: Path,
    coverage: dict[str, Any],
    stdout_path: Path,
    stderr_path: Path,
) -> dict[str, Any]:
    return {
        "case_id": case["id"],
        "case_name": case.get("name"),
        "category": case.get("category"),
        "profile": profile,
        "matrix": matrix.as_dict(),
        "coverage": coverage,
        "status": "FAIL",
        "reason": reason,
        "checks": [{"name": "runner-driver-execution", "status": "FAIL", "evidence": reason}],
        "stdout_path": str(stdout_path),
        "stderr_path": str(stderr_path),
        "artifacts_dir": str(run_dir),
    }


def read_tail_text(path: Path, limit: int = PROOF_TAIL_BYTES) -> str:
    with path.open("rb") as handle:
        handle.seek(0, os.SEEK_END)
        size = handle.tell()
        handle.seek(max(0, size - limit))
        return handle.read().decode("utf-8", errors="replace")


def parse_driver_proof_from_file(stdout_path: Path) -> dict[str, Any] | None:
    stdout = read_tail_text(stdout_path).strip()
    if not stdout:
        return None
    candidates = [stdout]
    candidates.extend(line.strip() for line in reversed(stdout.splitlines()) if line.strip())
    seen: set[str] = set()
    for candidate in candidates:
        if candidate in seen:
            continue
        seen.add(candidate)
        try:
            proof = json.loads(candidate)
        except json.JSONDecodeError:
            continue
        if isinstance(proof, dict):
            return proof
    return None


def proof_identity_error(proof: dict[str, Any], case: dict[str, Any], profile: str, matrix: MatrixSelection) -> str | None:
    proof_case_id = proof.get("case_id")
    if proof_case_id is None and isinstance(proof.get("case"), dict):
        proof_case_id = proof["case"].get("id")
    if proof_case_id != case["id"]:
        return f"driver proof case_id {proof_case_id!r} does not match requested {case['id']!r}"
    if proof.get("profile") != profile:
        return f"driver proof profile {proof.get('profile')!r} does not match requested {profile!r}"
    proof_matrix = proof.get("matrix")
    if not isinstance(proof_matrix, dict):
        return "driver proof is missing matrix identity"
    requested_matrix = matrix.as_dict()
    for key, value in requested_matrix.items():
        if proof_matrix.get(key) != value:
            return f"driver proof matrix {key} {proof_matrix.get(key)!r} does not match requested {value!r}"
    proof_mode = proof.get("mode")
    if proof_mode is not None and proof_mode != profile:
        return f"driver proof mode {proof_mode!r} does not match requested profile {profile!r}"
    return None


def approved_exclusion_ids(case: dict[str, Any]) -> set[str]:
    approved: set[str] = set()
    for key in ("approved_exclusions", "exclusions"):
        records = case.get(key, [])
        if isinstance(records, dict):
            records = [records]
        if not isinstance(records, list):
            continue
        for record in records:
            if isinstance(record, str):
                approved.add(record)
            elif isinstance(record, dict) and (record.get("approved") is True or record.get("pre_reviewed") is True):
                exclusion_id = record.get("id") or record.get("name")
                if exclusion_id:
                    approved.add(str(exclusion_id))
    return approved


def proof_exclusion_error(proof: dict[str, Any], case: dict[str, Any]) -> str | None:
    if str(proof.get("status", "")).upper() != "EXCLUDED":
        return None
    exclusion_id = proof.get("exclusion_id")
    if not exclusion_id:
        return "EXCLUDED proof is missing exclusion_id"
    if str(exclusion_id) not in approved_exclusion_ids(case):
        return f"EXCLUDED proof references unapproved exclusion {exclusion_id!r}"
    return None


def artifact_paths(check: dict[str, Any], run_dir: Path) -> list[Path]:
    raw = check.get("artifact") or check.get("artifacts") or check.get("artifact_path")
    values = raw if isinstance(raw, list) else [raw]
    paths: list[Path] = []
    for value in values:
        if not isinstance(value, str) or not value:
            continue
        candidate = Path(value)
        if not candidate.is_absolute():
            candidate = run_dir / candidate
        try:
            resolved = candidate.resolve()
            resolved.relative_to(run_dir.resolve())
        except (OSError, ValueError):
            continue
        paths.append(resolved)
    return paths


def check_has_evidence(check: dict[str, Any], run_dir: Path) -> bool:
    evidence = check.get("evidence")
    if isinstance(evidence, str) and evidence.strip():
        return True
    if isinstance(evidence, (dict, list)) and evidence:
        return True
    for path in artifact_paths(check, run_dir):
        try:
            if path.is_file() and path.stat().st_size > 0:
                return True
        except OSError:
            continue
    return False


def check_validation_error(check: dict[str, Any], case: dict[str, Any], run_dir: Path) -> str | None:
    name = check.get("name")
    if not isinstance(name, str) or not name.strip():
        return "driver proof contains a check without non-empty name"
    check_status = str(check.get("status", "")).upper()
    if check_status not in STATUS_VALUES:
        return f"check {name!r} is missing a valid status"
    if check_status == "PASS" and not check_has_evidence(check, run_dir):
        return f"PASS check {name!r} has no evidence or run-local artifact"
    if check_status == "EXCLUDED":
        exclusion_id = check.get("exclusion_id")
        if not exclusion_id:
            return f"EXCLUDED check {name!r} is missing exclusion_id"
        if str(exclusion_id) not in approved_exclusion_ids(case):
            return f"EXCLUDED check {name!r} references unapproved exclusion {exclusion_id!r}"
    return None


def build_check_accounting(checks: list[dict[str, Any]]) -> dict[str, Any]:
    counts = {status: 0 for status in sorted(STATUS_VALUES)}
    exclusions: list[dict[str, Any]] = []
    for check in checks:
        status = str(check.get("status", "")).upper()
        if status in counts:
            counts[status] += 1
        if status == "EXCLUDED":
            exclusions.append({"name": check.get("name"), "exclusion_id": check.get("exclusion_id")})
    return {"check_counts": counts, "excluded_checks": exclusions}


def validate_driver_axis_coverage(
    proof: dict[str, Any],
    coverage: dict[str, Any],
    checks: list[dict[str, Any]],
    profile: str,
) -> tuple[str | None, dict[str, Any]]:
    expected_axes = coverage.get("driver_owned_axes", {})
    if not expected_axes:
        return None, {}
    proof_coverage = proof.get("coverage")
    if not isinstance(proof_coverage, dict):
        return "driver proof is missing coverage for driver-owned matrix axes", {}
    if proof_coverage.get("profile") != profile:
        return f"driver coverage profile {proof_coverage.get('profile')!r} does not match requested {profile!r}", {}
    proof_axes = proof_coverage.get("axes")
    if not isinstance(proof_axes, dict):
        return "driver coverage is missing axes object", {}

    checks_by_name = {check.get("name"): check for check in checks if isinstance(check.get("name"), str)}
    accepted_statuses = {"PASS", "EXCLUDED"}
    proven: dict[str, dict[str, str]] = {}
    for axis, expected_values in expected_axes.items():
        axis_record = proof_axes.get(axis)
        if not isinstance(axis_record, dict):
            return f"driver coverage missing axis {axis!r}", proven
        values = axis_record.get("values")
        if not isinstance(values, list):
            return f"driver coverage axis {axis!r} missing values list", proven
        normalized_values = [str(value) for value in values]
        unexpected = sorted(set(normalized_values) - set(expected_values))
        if unexpected:
            return f"driver coverage axis {axis!r} has unexpected values {unexpected!r}", proven
        missing = sorted(set(expected_values) - set(normalized_values))
        if missing:
            return f"driver coverage axis {axis!r} missing values {missing!r}", proven
        check_refs = axis_record.get("checks")
        if not isinstance(check_refs, dict):
            return f"driver coverage axis {axis!r} missing checks map", proven
        proven[axis] = {}
        for value in expected_values:
            check_name = check_refs.get(value)
            if not isinstance(check_name, str) or not check_name:
                return f"driver coverage axis {axis!r} value {value!r} has no check reference", proven
            check = checks_by_name.get(check_name)
            if check is None:
                return f"driver coverage axis {axis!r} value {value!r} references unknown check {check_name!r}", proven
            if str(check.get("status", "")).upper() not in accepted_statuses:
                return f"driver coverage axis {axis!r} value {value!r} references non-passing check {check_name!r}", proven
            proven[axis][value] = check_name
    return None, proven


def proof_status(proof: dict[str, Any], returncode: int, case: dict[str, Any], run_dir: Path) -> tuple[str, str]:
    status = str(proof.get("status", "")).upper()
    if status not in STATUS_VALUES:
        return "BLOCKED", "driver proof is missing a valid contract status"
    checks = proof.get("checks")
    if not isinstance(checks, list) or not checks:
        return "BLOCKED", "driver proof is missing non-empty structured checks"
    for check in checks:
        if not isinstance(check, dict):
            return "BLOCKED", "driver proof contains a non-object check"
        validation_error = check_validation_error(check, case, run_dir)
        if validation_error is not None:
            return "BLOCKED", validation_error
    if status == "PASS":
        bad_checks = [
            check
            for check in checks
            if str(check.get("status", "")).upper() not in STRUCTURED_PASS_CHECK_STATUSES
        ]
        if bad_checks:
            return "FAIL", "driver reported PASS but at least one structured check did not pass"
        if returncode != 0:
            return "FAIL", "driver reported PASS but exited non-zero"
    return status, str(proof.get("reason") or "")


def run_driver(
    case: dict[str, Any],
    profile: str,
    matrix: MatrixSelection,
    run_dir: Path,
    coverage: dict[str, Any],
    timeout_seconds: int,
) -> dict[str, Any]:
    driver = case.get("driver")
    if not isinstance(driver, dict):
        return blocked_result(case, profile, matrix, "case has no registered driver object", run_dir, coverage)
    state = str(driver.get("state", "TODO")).upper()
    if state != "READY":
        return blocked_result(
            case,
            profile,
            matrix,
            f"driver state is {state}; registered READY driver required",
            run_dir,
            coverage,
        )
    command = normalize_command(driver.get("command"))
    if command is None:
        return blocked_result(case, profile, matrix, "driver command must be a non-empty argv list", run_dir, coverage)

    env = os.environ.copy()
    env.update(
        {
            "AFS_ACCEPTANCE_CASE_ID": case["id"],
            "AFS_ACCEPTANCE_PROFILE": profile,
            "AFS_ACCEPTANCE_MATRIX": json.dumps(matrix.as_dict(), sort_keys=True),
            "AFS_ACCEPTANCE_RUN_DIR": str(run_dir),
        }
    )
    stdout_path = run_dir / "stdout" / f"{case['id']}-{uuid.uuid4().hex[:8]}.stdout"
    stderr_path = run_dir / "stderr" / f"{case['id']}-{uuid.uuid4().hex[:8]}.stderr"
    returncode: int | None = None
    with stdout_path.open("wb") as stdout_file, stderr_path.open("wb") as stderr_file:
        try:
            process = subprocess.Popen(
                command,
                cwd=Path(__file__).resolve().parent,
                env=env,
                shell=False,
                stdout=stdout_file,
                stderr=stderr_file,
                start_new_session=True,
            )
        except OSError as exc:
            stderr_file.write(f"failed to start driver: {exc}\n".encode("utf-8", errors="replace"))
            return failure_result(case, profile, matrix, f"failed to start driver: {exc}", run_dir, coverage, stdout_path, stderr_path)

        deadline = time.monotonic() + timeout_seconds
        while True:
            returncode = process.poll()
            if returncode is not None:
                break
            if time.monotonic() >= deadline:
                try:
                    os.killpg(process.pid, signal.SIGTERM)
                except ProcessLookupError:
                    pass
                try:
                    process.wait(timeout=2)
                except subprocess.TimeoutExpired:
                    try:
                        os.killpg(process.pid, signal.SIGKILL)
                    except ProcessLookupError:
                        pass
                    process.wait()
                stderr_file.write(f"driver timed out after {timeout_seconds}s; process group terminated\n".encode("utf-8"))
                result = blocked_result(case, profile, matrix, f"driver timed out after {timeout_seconds}s", run_dir, coverage)
                result["stdout_path"] = str(stdout_path)
                result["stderr_path"] = str(stderr_path)
                return result
            time.sleep(0.05)

    proof = parse_driver_proof_from_file(stdout_path)
    if proof is None:
        result = blocked_result(
            case,
            profile,
            matrix,
            "driver did not emit a structured JSON proof on stdout",
            run_dir,
            coverage,
        )
    else:
        identity_error = proof_identity_error(proof, case, profile, matrix)
        exclusion_error = proof_exclusion_error(proof, case)
        checks = proof.get("checks", [])
        if not isinstance(checks, list):
            checks = []
        axis_error: str | None = None
        proven_axis_coverage: dict[str, Any] = {}
        if identity_error is not None:
            status, reason = "BLOCKED", identity_error
        elif exclusion_error is not None:
            status, reason = "BLOCKED", exclusion_error
        else:
            status, reason = proof_status(proof, returncode or 0, case, run_dir)
            if status != "BLOCKED":
                axis_error, proven_axis_coverage = validate_driver_axis_coverage(proof, coverage, checks, profile)
                if axis_error is not None:
                    status, reason = "BLOCKED", axis_error
                elif status == "PASS":
                    coverage["missing_full_coverage"] = {
                        key: value
                        for key, value in coverage.get("missing_full_coverage", {}).items()
                        if key not in proven_axis_coverage
                    }
                    coverage["driver_axis_coverage"] = proven_axis_coverage
                    coverage["full_matrix_covered"] = not coverage["missing_full_coverage"]
        result = {
            "case_id": case["id"],
            "case_name": case.get("name"),
            "category": case.get("category"),
            "profile": profile,
            "matrix": matrix.as_dict(),
            "coverage": coverage,
            "status": status,
            "reason": reason,
            "checks": checks,
            "accounting": build_check_accounting(checks),
            "driver_proof": proof,
            "driver_returncode": returncode,
            "stdout_path": str(stdout_path),
            "stderr_path": str(stderr_path),
            "artifacts_dir": str(run_dir),
        }
    return result


def aggregate_status(results: list[dict[str, Any]]) -> str:
    statuses = [result["status"] for result in results]
    for status in NON_PASS_STATUSES:
        if status in statuses:
            return status
    return "PASS" if statuses else "BLOCKED"


def build_identity(
    manifest_path: Path,
    lock_path: Path,
    manifest: dict[str, Any],
    lock: dict[str, Any],
    observed: dict[str, Any],
    release_identity_errors: list[str],
) -> dict[str, Any]:
    return {
        "runner": {
            "path": str(Path(__file__).resolve()),
            "sha256": sha256_file(Path(__file__).resolve()),
            "argv": sys.argv,
            "python": sys.version,
            "platform": platform.platform(),
        },
        "contract": {
            "source_contract": manifest.get("source_contract"),
            "contract_sha": lock.get("contract_sha"),
            "observed": observed.get("contract"),
        },
        "manifest": {
            "path": str(manifest_path),
            "sha256": sha256_file(manifest_path),
            "schema_version": manifest.get("schema_version"),
        },
        "lock": {
            "path": str(lock_path),
            "sha256": sha256_file(lock_path),
            "state": lock.get("state"),
            "verified": lock.get("verified"),
            "binary": lock.get("binary"),
            "source": lock.get("source") or lock.get("source_tree"),
        },
        "observed": observed,
        "release_identity_errors": release_identity_errors,
    }


def write_junit(run_dir: Path, results: list[dict[str, Any]]) -> None:
    suite = ET.Element(
        "testsuite",
        {
            "name": "afs-acceptance",
            "tests": str(len(results)),
            "failures": str(sum(1 for result in results if result["status"] == "FAIL")),
            "skipped": str(sum(1 for result in results if result["status"] in {"BLOCKED", "EXCLUDED"})),
            "errors": str(sum(1 for result in results if result["status"] == "INCONCLUSIVE")),
        },
    )
    for result in results:
        name = result["case_id"]
        if result.get("matrix"):
            name += " " + json.dumps(result["matrix"], sort_keys=True)
        testcase = ET.SubElement(
            suite,
            "testcase",
            {
                "classname": result.get("category") or "afs",
                "name": name,
            },
        )
        status = result["status"]
        message = result.get("reason") or status
        if status == "FAIL":
            ET.SubElement(testcase, "failure", {"message": message}).text = json.dumps(result, sort_keys=True)
        elif status in {"BLOCKED", "EXCLUDED"}:
            ET.SubElement(testcase, "skipped", {"message": message}).text = json.dumps(result, sort_keys=True)
        elif status == "INCONCLUSIVE":
            ET.SubElement(testcase, "error", {"message": message}).text = json.dumps(result, sort_keys=True)
    ET.ElementTree(suite).write(run_dir / "junit.xml", encoding="utf-8", xml_declaration=True)


def run_acceptance(args: argparse.Namespace) -> dict[str, Any]:
    manifest_path = Path(args.cases).resolve()
    lock_path = Path(args.lock).resolve()
    results_root = Path(args.results_dir).resolve()
    manifest = load_json(manifest_path)
    lock = load_json(lock_path)
    attestation = load_identity_attestation(args.identity_attestation)
    contract_path = Path(getattr(args, "contract", None) or DEFAULT_CONTRACT).resolve()
    observed_identity = observed_release_identity(manifest_path, attestation, contract_path)
    full_lock_ready, release_identity_errors = release_lock_validation(lock, observed_identity)
    environment_errors = environment.qualification_errors(lock, lock_path)
    release_identity_errors.extend(environment_errors)
    full_lock_ready = full_lock_ready and not environment_errors
    all_active = active_cases(manifest)
    all_active_ids = {case["id"] for case in all_active}
    cases = find_cases(manifest, args.case, args.category)
    if not cases:
        raise AcceptanceError("no active cases selected")

    run_dir = create_run_dir(results_root, args.run_id)
    profile = args.profile
    linux_arm64 = is_linux_arm64()
    selected_filter = MatrixSelection(args.backend, args.meta, args.transport)
    results: list[dict[str, Any]] = []
    case_coverages: dict[str, Any] = {}

    for case in cases:
        points, coverage = select_matrix(case, selected_filter, profile)
        coverage["profile"] = profile
        coverage["smoke_is_release_gate"] = False
        coverage["full_profile_lock_ready"] = full_lock_ready
        coverage["linux_arm64"] = linux_arm64
        case_coverages[case["id"]] = coverage
        for point in points:
            if not linux_arm64:
                result = blocked_result(
                    case,
                    profile,
                    point,
                    "acceptance execution is Linux ARM64 only",
                    run_dir,
                    coverage,
                )
            elif profile == "full" and not full_lock_ready:
                result = blocked_result(
                    case,
                    profile,
                    point,
                    "full profile requires frozen verified lock identity: " + "; ".join(release_identity_errors),
                    run_dir,
                    coverage,
                )
            else:
                result = run_driver(case, profile, point, run_dir, coverage, args.timeout)
            results.append(result)

    summary_status = aggregate_status(results)
    selected_active_ids = {case["id"] for case in cases}
    missing_active_case_ids = sorted(all_active_ids - selected_active_ids)
    full_release_gate_pass = (
        profile == "full"
        and full_lock_ready
        and linux_arm64
        and summary_status == "PASS"
        and not missing_active_case_ids
        and all(coverage["full_matrix_covered"] for coverage in case_coverages.values())
    )
    report = {
        "schema_version": 1,
        "run_id": run_dir.name,
        "run_dir": str(run_dir),
        "created_at": _dt.datetime.now(tz=_dt.timezone.utc).isoformat(),
        "profile": profile,
        "selection": {
            "cases": [case["id"] for case in cases],
            "category": args.category,
            "backend": args.backend,
            "meta": args.meta,
            "transport": args.transport,
        },
        "identity": build_identity(manifest_path, lock_path, manifest, lock, observed_identity, release_identity_errors),
        "summary": {
            "status": summary_status,
            "full_release_gate_pass": full_release_gate_pass,
            "smoke_is_release_gate": False,
            "required_active_case_count": len(all_active_ids),
            "selected_active_case_count": len(selected_active_ids),
            "missing_active_case_ids": missing_active_case_ids,
            "result_counts": {
                status: sum(1 for result in results if result["status"] == status)
                for status in sorted(STATUS_VALUES)
            },
            "case_coverage": case_coverages,
        },
        "results": results,
    }
    (run_dir / "result.json").write_text(json.dumps(report, indent=2, sort_keys=True), encoding="utf-8")
    write_junit(run_dir, results)
    return report


def list_cases(args: argparse.Namespace) -> int:
    manifest = load_json(Path(args.cases).resolve())
    cases = find_cases(manifest, args.case, args.category)
    for case in cases:
        driver = case.get("driver", {})
        state = driver.get("state") if isinstance(driver, dict) else "MISSING"
        print(
            "\t".join(
                [
                    case["id"],
                    str(case.get("category", "")),
                    str(case.get("stage", "")),
                    f"driver={state}",
                    f"active={case.get('active', True)}",
                ]
            )
        )
    return 0


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="AFS Linux ARM64 acceptance runner")
    parser.add_argument("--cases", default=str(DEFAULT_CASES), help="case manifest JSON")
    parser.add_argument("--lock", help="explicit run-specific acceptance lock JSON (required except with --list)")
    parser.add_argument("--contract", default=str(DEFAULT_CONTRACT), help="actual AFS acceptance contract to hash against lock.contract")
    parser.add_argument("--identity-attestation", help="observed Linux source/binary identity JSON for full release")
    parser.add_argument("--results-dir", default=str(DEFAULT_RESULTS), help="immutable results root")
    parser.add_argument("--run-id", help="optional unique run id for tests/repro")
    parser.add_argument("--case", action="append", default=[], help="case id to run; repeatable")
    parser.add_argument("--category", help="case category filter")
    parser.add_argument("--backend", choices=["OwnerFs", "DFS"], help="matrix backend selection")
    parser.add_argument("--meta", choices=["etcd", "Redis"], help="matrix meta backend selection")
    parser.add_argument("--transport", help="matrix transport selection")
    parser.add_argument("--profile", choices=["smoke", "full"], default="smoke")
    parser.add_argument("--timeout", type=int, default=DEFAULT_TIMEOUT_SECONDS, help="per-driver timeout seconds")
    parser.add_argument("--list", action="store_true", help="list selected cases without executing")
    return parser


def main(argv: list[str] | None = None) -> int:
    parser = build_parser()
    args = parser.parse_args(argv)
    try:
        if args.timeout <= 0:
            raise AcceptanceError("--timeout must be positive")
        if args.list:
            return list_cases(args)
        if not args.lock:
            parser.error("--lock is required for execution; provide a run-specific acceptance lock")
        report = run_acceptance(args)
    except AcceptanceError as exc:
        print(f"runner error: {exc}", file=sys.stderr)
        return 2
    print(json.dumps({"run_dir": report["run_dir"], "summary": report["summary"]}, indent=2, sort_keys=True))
    return 0 if report["summary"]["status"] == "PASS" else 1


if __name__ == "__main__":
    raise SystemExit(main())
