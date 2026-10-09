#!/usr/bin/env python3
"""STD-05 fail-closed suite-accounting aggregator.

The runner owns the STD-05 case/profile/matrix/run directory.  This driver only
reads a fixed manifest of STD-01..STD-04 proof files and their raw artifacts,
then checks that no discovered suite item disappeared or was reclassified after
execution.
"""
from __future__ import annotations

import argparse
import datetime as dt
import hashlib
import importlib.util
import json
import random
import os
import sys
import traceback
from pathlib import Path
from typing import Any


STATUS_VALUES = {"PASS", "FAIL", "BLOCKED", "INCONCLUSIVE", "EXCLUDED"}
SUITE_IDS = ("STD-01", "STD-02", "STD-03", "STD-04")
FULL_LTP_COMMANDS = 657
FULL_FSX_SEEDS = 3
FULL_FSX_SECONDS = 900
FULL_RANDOM_SEEDS = 10
FULL_RANDOM_OPS = 10_000
RAW_ID_ARTIFACTS = {
    "STD-01": ("identity.json", "discovery.json", "tap-accounting.json", "command.json", "pjdfstest.stdout.tap", "pjdfstest.stderr.log"),
    "STD-02": ("identity.json", "discovery.json", "commands.json", "accounting.json"),
    "STD-03": ("identity.json", "commands.json", "accounting.json"),
    "STD-04": ("identity.json", "seed-results.json", "accounting.json"),
}
FORMAL_PASS_BLOCKERS: tuple[str, ...] = ()


class AccountingError(RuntimeError):
    def __init__(self, status: str, message: str):
        super().__init__(message)
        self.status = status


def utc() -> str:
    return dt.datetime.now(dt.timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")


def load_json(path: Path) -> Any:
    with path.open("r", encoding="utf-8") as handle:
        return json.load(handle)


def write_json(path: Path, value: Any) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(value, indent=2, sort_keys=True) + "\n", encoding="utf-8")


def sha256_file(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for chunk in iter(lambda: handle.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def is_sha256(value: Any) -> bool:
    return isinstance(value, str) and len(value) == 64 and all(ch in "0123456789abcdef" for ch in value.lower())


def rel(path: Path, base: Path) -> str:
    return str(path.resolve().relative_to(base.resolve()))


def is_under(child: Path, parent: Path) -> bool:
    try:
        child.resolve().relative_to(parent.resolve())
        return True
    except ValueError:
        return False


def resolve_manifest_path(value: Any, base: Path) -> Path:
    if not isinstance(value, str) or not value:
        raise AccountingError("BLOCKED", "manifest path is missing or not a string")
    path = Path(value)
    return (path if path.is_absolute() else base / path).resolve()


def parse_matrix(raw: str | None) -> dict[str, str]:
    if not raw:
        return {}
    parsed = json.loads(raw)
    if not isinstance(parsed, dict):
        raise AccountingError("BLOCKED", "AFS_ACCEPTANCE_MATRIX must decode to an object")
    return {str(key): str(value) for key, value in parsed.items()}


def build_check(name: str, status: str, evidence: Any, artifact: str | None = None) -> dict[str, Any]:
    check = {"name": name, "status": status, "evidence": evidence}
    if artifact:
        check["artifact"] = artifact
    return check


def status_from_checks(checks: list[dict[str, Any]]) -> tuple[str, str]:
    statuses = {str(check.get("status", "")).upper() for check in checks}
    if "FAIL" in statuses:
        return "FAIL", "one or more suite-accounting checks failed"
    if "BLOCKED" in statuses:
        return "BLOCKED", "one or more suite-accounting prerequisites are missing or incomplete"
    if "INCONCLUSIVE" in statuses:
        return "INCONCLUSIVE", "one or more suite-accounting checks are inconclusive"
    return "PASS", ""


def validate_manifest_shape(manifest: Any) -> list[dict[str, Any]]:
    if not isinstance(manifest, dict):
        raise AccountingError("BLOCKED", "accounting manifest must be a JSON object")
    if manifest.get("schema_version") != 1:
        raise AccountingError("BLOCKED", "accounting manifest schema_version must be 1")
    suites = manifest.get("suites")
    if not isinstance(suites, list):
        raise AccountingError("BLOCKED", "accounting manifest suites must be a list")
    ids: list[str] = []
    for item in suites:
        if not isinstance(item, dict):
            raise AccountingError("BLOCKED", "each suite manifest entry must be an object")
        ids.append(str(item.get("case_id")))
    missing = sorted(set(SUITE_IDS) - set(ids))
    duplicates = sorted({case_id for case_id in ids if ids.count(case_id) > 1})
    extra = sorted(set(ids) - set(SUITE_IDS))
    if missing or duplicates or extra:
        raise AccountingError("BLOCKED", f"manifest must contain exactly STD-01..STD-04 once; missing={missing} duplicates={duplicates} extra={extra}")
    return sorted(suites, key=lambda item: SUITE_IDS.index(str(item["case_id"])))


def is_reference_preparation_only(doc: dict[str, Any]) -> bool:
    kind = str(doc.get("kind", "")).upper()
    accounting = doc.get("accounting") if isinstance(doc.get("accounting"), dict) else {}
    return doc.get("reference_preparation_only") is True or accounting.get("reference_preparation_only") is True or "REFERENCE_PREPARATION_ONLY" in kind


def load_bound_proof(entry: dict[str, Any], manifest_dir: Path) -> tuple[Path, Path, dict[str, Any]]:
    artifact_root = resolve_manifest_path(entry.get("artifact_root"), manifest_dir)
    proof_path = resolve_manifest_path(entry.get("proof_path"), manifest_dir)
    if not is_under(proof_path, artifact_root):
        raise AccountingError("BLOCKED", f"{entry.get('case_id')} proof_path is outside artifact_root")
    if not proof_path.is_file():
        raise AccountingError("BLOCKED", f"{entry.get('case_id')} proof_path does not exist")
    expected = entry.get("proof_sha256")
    observed = sha256_file(proof_path)
    if not is_sha256(expected):
        raise AccountingError("BLOCKED", f"{entry.get('case_id')} proof_sha256 is required and must be a sha256 hex digest")
    if observed != expected:
        raise AccountingError("BLOCKED", f"{entry.get('case_id')} proof sha256 mismatch")
    proof = load_json(proof_path)
    if not isinstance(proof, dict):
        raise AccountingError("BLOCKED", f"{entry.get('case_id')} proof must be a JSON object")
    if is_reference_preparation_only(proof):
        raise AccountingError("BLOCKED", f"{entry.get('case_id')} target proof is reference-preparation-only, not a formal suite proof")
    return artifact_root, proof_path, proof


def validate_raw_artifacts(entry: dict[str, Any], artifact_root: Path) -> dict[str, dict[str, Any]]:
    case_id = str(entry.get("case_id"))
    raw = entry.get("raw_artifacts")
    if not isinstance(raw, list) or not raw:
        raise AccountingError("BLOCKED", f"{case_id} raw_artifacts must be a non-empty list")
    observed: dict[str, dict[str, Any]] = {}
    for item in raw:
        if not isinstance(item, dict):
            raise AccountingError("BLOCKED", f"{case_id} raw artifact entry must be an object")
        rel_path = item.get("path")
        if not isinstance(rel_path, str) or not rel_path or Path(rel_path).is_absolute() or ".." in Path(rel_path).parts:
            raise AccountingError("BLOCKED", f"{case_id} raw artifact path is not a safe relative path")
        if rel_path in observed:
            raise AccountingError("BLOCKED", f"{case_id} duplicate raw artifact binding: {rel_path}")
        path = (artifact_root / rel_path).resolve()
        if not is_under(path, artifact_root):
            raise AccountingError("BLOCKED", f"{case_id} raw artifact escapes artifact_root")
        if not path.is_file():
            raise AccountingError("BLOCKED", f"{case_id} raw artifact is missing: {rel_path}")
        expected = item.get("sha256")
        actual = sha256_file(path)
        if not is_sha256(expected) or actual != expected:
            raise AccountingError("BLOCKED", f"{case_id} raw artifact sha256 mismatch: {rel_path}")
        observed[rel_path] = {"path": rel_path, "sha256": actual, "bytes": path.stat().st_size}
    for rel_path in RAW_ID_ARTIFACTS.get(case_id, ()):
        if rel_path not in observed:
            raise AccountingError("BLOCKED", f"{case_id} required raw artifact is not hash-bound: {rel_path}")
        if not (artifact_root / rel_path).is_file():
            raise AccountingError("BLOCKED", f"{case_id} required raw artifact is missing: {rel_path}")
    return observed


def load_raw_json(case_id: str, artifact_root: Path, raw_artifacts: dict[str, dict[str, Any]], rel_path: str) -> Any:
    if rel_path not in raw_artifacts:
        raise AccountingError("BLOCKED", f"{case_id} raw artifact is not hash-bound: {rel_path}")
    value = load_json((artifact_root / rel_path).resolve())
    if value is None:
        raise AccountingError("BLOCKED", f"{case_id} raw artifact is empty: {rel_path}")
    return value


def require_bound_artifact(case_id: str, artifact_root: Path, raw_artifacts: dict[str, dict[str, Any]], rel_path: Any) -> Path:
    if not isinstance(rel_path, str) or not rel_path:
        raise AccountingError("BLOCKED", f"{case_id} original raw artifact path is missing")
    if rel_path not in raw_artifacts:
        raise AccountingError("BLOCKED", f"{case_id} original raw artifact is not hash-bound: {rel_path}")
    path = (artifact_root / rel_path).resolve()
    if not is_under(path, artifact_root) or not path.is_file():
        raise AccountingError("BLOCKED", f"{case_id} original raw artifact is missing: {rel_path}")
    return path


_DRIVER_MODULES: dict[str, Any] = {}


def driver_module(name: str) -> Any:
    module = _DRIVER_MODULES.get(name)
    if module is not None:
        return module
    drivers_dir = Path(__file__).resolve().parent
    path = drivers_dir / f"{name}.py"
    if str(drivers_dir) not in sys.path:
        sys.path.insert(0, str(drivers_dir))
    spec = importlib.util.spec_from_file_location(f"afs_acceptance_{name}", path)
    if spec is None or spec.loader is None:
        raise AccountingError("BLOCKED", f"cannot load {name}.py parser")
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    _DRIVER_MODULES[name] = module
    return module


def load_bound_raw_artifacts(case_id: str, binding: dict[str, Any], manifest_dir: Path) -> tuple[Path, dict[str, dict[str, Any]]]:
    artifact_root = resolve_manifest_path(binding.get("artifact_root"), manifest_dir)
    if not artifact_root.is_dir():
        raise AccountingError("BLOCKED", f"{case_id} reference artifact_root is missing")
    raw = binding.get("raw_artifacts")
    if not isinstance(raw, list) or not raw:
        raise AccountingError("BLOCKED", f"{case_id} reference raw_artifacts must be a non-empty list")
    observed: dict[str, dict[str, Any]] = {}
    for item in raw:
        if not isinstance(item, dict):
            raise AccountingError("BLOCKED", f"{case_id} reference raw artifact entry must be an object")
        rel_path = item.get("path")
        if not isinstance(rel_path, str) or not rel_path or Path(rel_path).is_absolute() or ".." in Path(rel_path).parts:
            raise AccountingError("BLOCKED", f"{case_id} reference raw artifact path is not a safe relative path")
        path = (artifact_root / rel_path).resolve()
        if not is_under(path, artifact_root) or not path.is_file():
            raise AccountingError("BLOCKED", f"{case_id} reference raw artifact is missing: {rel_path}")
        expected = item.get("sha256")
        actual = sha256_file(path)
        if not is_sha256(expected) or actual != expected:
            raise AccountingError("BLOCKED", f"{case_id} reference raw artifact sha256 mismatch: {rel_path}")
        observed[rel_path] = {"path": rel_path, "sha256": actual, "bytes": path.stat().st_size}
    for rel_path in RAW_ID_ARTIFACTS.get(case_id, ()):
        if rel_path not in observed:
            raise AccountingError("BLOCKED", f"{case_id} reference required raw artifact is not hash-bound: {rel_path}")
    return artifact_root, observed


def check_stable_process_identity(case_id: str, identity_doc: dict[str, Any]) -> dict[str, Any]:
    result: dict[str, Any] = {}
    for logical_name, key in (("node", "process"), ("meta", "meta_process")):
        proc = identity_doc.get(key)
        if proc is None and logical_name == "node":
            proc = identity_doc.get("node_process")
        if not isinstance(proc, dict):
            raise AccountingError("BLOCKED", f"{case_id} original stable {logical_name} process identity is missing")
        sha = proc.get("sha256") or proc.get("exe_sha256") or proc.get("expected_sha256")
        if not is_sha256(sha):
            raise AccountingError("BLOCKED", f"{case_id} original stable {logical_name} sha256 is missing")
        pid = proc.get("pid")
        if pid in (None, ""):
            raise AccountingError("BLOCKED", f"{case_id} original stable {logical_name} pid is missing")
        stable_tokens = [name for name in ("boot_id", "machine_id", "start_ticks", "exe_dev", "exe_inode", "cmdline", "exe") if proc.get(name) not in (None, "")]
        if len(stable_tokens) < 2:
            raise AccountingError("BLOCKED", f"{case_id} original stable {logical_name} identity lacks stable fields")
        result[logical_name] = {"sha256": str(sha), "pid": str(pid), "stable_fields": stable_tokens}
    return result


def stable_command_key(item: dict[str, Any]) -> str:
    test_id = item.get("test_id") or item.get("name") or item.get("case")
    command = item.get("command") or item.get("argv")
    if not test_id or not command:
        return ""
    return json.dumps({"test_id": test_id, "command": command}, sort_keys=True, separators=(",", ":"))


def stable_item_evidence(case_id: str, artifact_root: Path, raw_artifacts: dict[str, dict[str, Any]], proof: dict[str, Any]) -> dict[str, Any]:
    if case_id == "STD-01":
        discovery = load_raw_json(case_id, artifact_root, raw_artifacts, "discovery.json")
        tap = load_raw_json(case_id, artifact_root, raw_artifacts, "tap-accounting.json")
        if not isinstance(discovery, dict) or not isinstance(tap, dict):
            raise AccountingError("BLOCKED", "STD-01 discovery/tap accounting artifacts must be objects")
        selected = tap.get("selected_tests")
        completed = tap.get("observed_completed_tests")
        if not isinstance(selected, list) or not selected or len(selected) != len(set(map(str, selected))):
            raise AccountingError("BLOCKED", "STD-01 original stable selected TAP test IDs are missing or duplicated")
        if not isinstance(completed, list):
            raise AccountingError("BLOCKED", "STD-01 original observed TAP stable IDs are missing")
        selected_ids = [str(item) for item in selected]
        completed_ids = [str(item) for item in completed]
        missing = sorted(set(selected_ids) - set(completed_ids))
        extra = sorted(set(completed_ids) - set(selected_ids))
        if (proof.get("status") == "PASS" or (proof.get("accounting") or {}).get("tap_todo", 0) == 0) and (missing or extra):
            raise AccountingError("BLOCKED", f"STD-01 observed TAP IDs do not match selected IDs: missing={missing} extra={extra}")
        return {"selected_tests": len(selected), "observed_completed_tests": len(completed), "missing_selected_ids": missing, "unexpected_observed_ids": extra, "discovered_files": discovery.get("discovered_files")}
    if case_id == "STD-02":
        discovery = load_raw_json(case_id, artifact_root, raw_artifacts, "discovery.json")
        commands = load_raw_json(case_id, artifact_root, raw_artifacts, "commands.json")
        if not isinstance(discovery, dict) or not isinstance(commands, list):
            raise AccountingError("BLOCKED", "STD-02 discovery/commands artifacts must use the LTP driver schema")
        selected = discovery.get("selected")
        if not isinstance(selected, list) or not selected:
            raise AccountingError("BLOCKED", "STD-02 frozen LTP selection is missing")
        stable_ids = [stable_command_key(item) for item in selected if isinstance(item, dict)]
        if len(stable_ids) != len(selected) or len(set(stable_ids)) != len(stable_ids) or any(not item for item in stable_ids):
            raise AccountingError("BLOCKED", "STD-02 original stable LTP command IDs are missing or duplicated")
        command_ids = [stable_command_key(item) for item in commands if isinstance(item, dict)]
        if len(command_ids) != len(commands) or any(not item for item in command_ids):
            raise AccountingError("BLOCKED", "STD-02 executed LTP command IDs are missing")
        missing = sorted(set(stable_ids) - set(command_ids))
        extra = sorted(set(command_ids) - set(stable_ids))
        if proof.get("status") == "PASS" and (missing or extra):
            raise AccountingError("BLOCKED", f"STD-02 executed LTP command IDs do not match frozen selection: missing={len(missing)} extra={len(extra)}")
        return {"selected_commands": len(selected), "executed_commands": len(commands), "missing_selected_ids": len(missing), "unexpected_executed_ids": len(extra), "total_commands": discovery.get("total_commands")}
    if case_id == "STD-03":
        commands = load_raw_json(case_id, artifact_root, raw_artifacts, "commands.json")
        if not isinstance(commands, list) or not commands:
            raise AccountingError("BLOCKED", "STD-03 FSx seed command records are missing")
        seeds = [item.get("seed") for item in commands if isinstance(item, dict)]
        if len(seeds) != len(commands) or len(set(map(str, seeds))) != len(seeds) or any(seed is None for seed in seeds):
            raise AccountingError("BLOCKED", "STD-03 original stable FSx seed IDs are missing or duplicated")
        selected = (proof.get("accounting") or {}).get("selected_seeds", []) if isinstance(proof.get("accounting"), dict) else []
        missing = sorted(set(map(str, selected)) - set(map(str, seeds)))
        extra = sorted(set(map(str, seeds)) - set(map(str, selected)))
        if proof.get("status") == "PASS" and (missing or extra):
            raise AccountingError("BLOCKED", f"STD-03 FSx observed seed IDs do not match selected IDs: missing={missing} extra={extra}")
        return {"seed_commands": len(commands), "seeds": seeds, "missing_selected_ids": missing, "unexpected_seed_ids": extra}
    if case_id == "STD-04":
        seeds = load_raw_json(case_id, artifact_root, raw_artifacts, "seed-results.json")
        if not isinstance(seeds, list) or not seeds:
            raise AccountingError("BLOCKED", "STD-04 random seed results are missing")
        seed_ids = [item.get("seed") for item in seeds if isinstance(item, dict)]
        if len(seed_ids) != len(seeds) or len(set(map(str, seed_ids))) != len(seed_ids) or any(seed is None for seed in seed_ids):
            raise AccountingError("BLOCKED", "STD-04 original stable random seed IDs are missing or duplicated")
        missing_ops = [seed for seed, item in zip(seed_ids, seeds) if not isinstance(item, dict) or item.get("operations_executed") in (None, "")]
        if missing_ops:
            raise AccountingError("BLOCKED", "STD-04 per-seed operation counts are missing")
        selected = (proof.get("accounting") or {}).get("selected_seeds", []) if isinstance(proof.get("accounting"), dict) else []
        missing = sorted(set(map(str, selected)) - set(map(str, seed_ids)))
        extra = sorted(set(map(str, seed_ids)) - set(map(str, selected)))
        if proof.get("status") == "PASS" and (missing or extra):
            raise AccountingError("BLOCKED", f"STD-04 random observed seed IDs do not match selected IDs: missing={missing} extra={extra}")
        return {"seeds": len(seeds), "seed_ids": seed_ids, "missing_selected_ids": missing, "unexpected_seed_ids": extra}
    raise AssertionError(case_id)


def validate_proof_identity(case_id: str, proof: dict[str, Any], profile: str, matrix: dict[str, str]) -> None:
    if proof.get("case_id") != case_id:
        raise AccountingError("BLOCKED", f"{case_id} proof case_id mismatch")
    if proof.get("profile") != profile:
        raise AccountingError("BLOCKED", f"{case_id} proof profile mismatch")
    if str(proof.get("status", "")).upper() not in STATUS_VALUES:
        raise AccountingError("BLOCKED", f"{case_id} proof status is invalid")
    proof_matrix = proof.get("matrix")
    if not isinstance(proof_matrix, dict):
        raise AccountingError("BLOCKED", f"{case_id} proof matrix is missing")
    for key, value in matrix.items():
        if str(proof_matrix.get(key)) != value:
            raise AccountingError("BLOCKED", f"{case_id} proof matrix {key} mismatch")


def validate_candidate(entry: dict[str, Any], proof: dict[str, Any], identity_doc: dict[str, Any], profile: str, matrix: dict[str, str]) -> dict[str, Any]:
    candidate = entry.get("candidate")
    case_id = str(entry.get("case_id"))
    if not isinstance(candidate, dict):
        raise AccountingError("BLOCKED", f"{case_id} candidate binding is missing")
    required = ("afs_candidate", "backend", "meta", "profile", "node_sha256", "meta_sha256")
    result: dict[str, Any] = {key: str(candidate.get(key, "")) for key in required}
    if not all(result.values()):
        raise AccountingError("BLOCKED", f"{case_id} candidate binding must include afs_candidate/backend/meta/profile/node_sha256/meta_sha256")
    if not is_sha256(result["node_sha256"]) or not is_sha256(result["meta_sha256"]):
        raise AccountingError("BLOCKED", f"{case_id} candidate Node/Meta SHA bindings must be sha256 hex digests")
    if result["profile"] != profile:
        raise AccountingError("BLOCKED", f"{case_id} candidate profile mismatch")
    for key in ("backend", "meta"):
        if matrix.get(key) and result[key] != matrix[key]:
            raise AccountingError("BLOCKED", f"{case_id} candidate {key} mismatch")
    product = identity_doc.get("product") if isinstance(identity_doc.get("product"), dict) else {}
    proof_product = ((proof.get("identity") or {}).get("product") or {}) if isinstance(proof.get("identity"), dict) else {}
    for source_name, observed in (("identity", product), ("proof", proof_product)):
        for key in ("backend", "meta"):
            if observed.get(key) is not None and str(observed.get(key)) != result[key]:
                raise AccountingError("BLOCKED", f"{case_id} {source_name} product {key} conflicts with manifest candidate")
    process = check_stable_process_identity(case_id, identity_doc)
    if process["node"]["sha256"] != result["node_sha256"]:
        raise AccountingError("BLOCKED", f"{case_id} candidate node_sha256 does not match original identity")
    if process["meta"]["sha256"] != result["meta_sha256"]:
        raise AccountingError("BLOCKED", f"{case_id} candidate meta_sha256 does not match original identity")
    result["process_identity"] = process
    return result


def check_named_pass(proof: dict[str, Any], name: str) -> bool:
    return any(isinstance(check, dict) and check.get("name") == name and check.get("status") == "PASS" for check in proof.get("checks", []))


def load_reference_proof(entry: dict[str, Any], manifest_dir: Path, case_id: str, profile: str) -> dict[str, Any]:
    reference = entry.get("reference")
    if not isinstance(reference, dict):
        raise AccountingError("BLOCKED", f"{case_id} reference binding is missing")
    if reference.get("filesystem") != "ext4":
        raise AccountingError("BLOCKED", f"{case_id} reference filesystem must be ext4")
    proof_path_value = reference.get("proof_path")
    proof_sha = reference.get("proof_sha256")
    if not proof_path_value or not is_sha256(proof_sha):
        raise AccountingError("BLOCKED", f"{case_id} hash-bound ext4 reference proof is required")
    proof_path = resolve_manifest_path(proof_path_value, manifest_dir)
    if not proof_path.is_file():
        raise AccountingError("BLOCKED", f"{case_id} ext4 reference proof is missing")
    if sha256_file(proof_path) != proof_sha:
        raise AccountingError("BLOCKED", f"{case_id} ext4 reference proof sha256 mismatch")
    proof = load_json(proof_path)
    if not isinstance(proof, dict):
        raise AccountingError("BLOCKED", f"{case_id} ext4 reference proof must be a JSON object")
    if proof.get("case_id") != case_id:
        raise AccountingError("BLOCKED", f"{case_id} ext4 reference proof case_id mismatch")
    if proof.get("profile") != profile:
        raise AccountingError("BLOCKED", f"{case_id} ext4 reference proof profile mismatch")
    if proof.get("status") != "PASS":
        raise AccountingError("BLOCKED", f"{case_id} ext4 reference proof is not PASS")
    proof_matrix = proof.get("matrix") if isinstance(proof.get("matrix"), dict) else {}
    if str(proof_matrix.get("reference", "ext4")) != "ext4":
        raise AccountingError("BLOCKED", f"{case_id} ext4 reference proof does not bind ext4")
    return {"filesystem": "ext4", "proof_path": str(proof_path), "proof_sha256": str(proof_sha), "status": "PASS"}


def validate_reference(entry: dict[str, Any], proof: dict[str, Any], identity_doc: dict[str, Any], manifest_dir: Path, case_id: str, profile: str) -> dict[str, Any]:
    reference = entry.get("reference")
    if not isinstance(reference, dict):
        raise AccountingError("BLOCKED", f"{case_id} reference binding is missing")
    if reference.get("filesystem") != "ext4":
        raise AccountingError("BLOCKED", f"{case_id} reference filesystem must be ext4")
    if case_id == "STD-04":
        if reference.get("internal_pairing") is not True:
            raise AccountingError("BLOCKED", "STD-04 must bind the driver's internal ext4/AFS operation pairing")
        ref_fs = identity_doc.get("reference_filesystem") if isinstance(identity_doc.get("reference_filesystem"), dict) else {}
        if ref_fs.get("fstype") != "ext4" or not check_named_pass(proof, "reference-ext4-scope"):
            raise AccountingError("BLOCKED", "STD-04 internal reference pairing lacks ext4 scope proof")
        return {"filesystem": "ext4", "internal_pairing": True, "check": "reference-ext4-scope"}
    return load_reference_proof(entry, manifest_dir, case_id, profile)


def normalize_status(value: Any) -> str:
    status = str(value or "").upper()
    if status in {"OK", "TPASS"}:
        return "PASS"
    if status in {"SKIP", "TCONF", "TODO"}:
        return "SKIP"
    if status in {"TBROK", "BROK", "BLOCKED"}:
        return "BLOCKED"
    if status in {"TIMEOUT"}:
        return "TIMEOUT"
    if status in {"PASS", "FAIL", "INCONCLUSIVE"}:
        return status
    return "PASS" if value is True else "FAIL" if value is False else status or "UNKNOWN"


def errno_name(value: Any) -> str | None:
    if value in (None, ""):
        return None
    text = str(value)
    if text.isdigit():
        return text
    return text.upper()


def raw_event(item_id: str, status: Any, *, errno_value: Any = None, skip_reason: Any = None, detail: Any = None) -> dict[str, Any]:
    normalized = normalize_status(status)
    event = {"id": str(item_id), "status": normalized, "errno": errno_name(errno_value), "skip_reason": str(skip_reason) if skip_reason not in (None, "") else None}
    if detail not in (None, ""):
        event["detail"] = detail
    return event


def explicit_raw_events(doc: Any) -> list[dict[str, Any]]:
    if not isinstance(doc, dict):
        return []
    events = doc.get("raw_semantics") or doc.get("semantic_results") or doc.get("events")
    if not isinstance(events, list):
        return []
    if "raw_semantics" in doc or "semantic_results" in doc:
        raise AccountingError("BLOCKED", "derived raw_semantics/semantic_results receipts are not original raw replay evidence")
    parsed: list[dict[str, Any]] = []
    for index, item in enumerate(events):
        if not isinstance(item, dict):
            continue
        item_id = item.get("id") or item.get("test_id") or item.get("seed") or item.get("name") or f"event-{index}"
        status = item.get("status") or item.get("result") or ("PASS" if item.get("ok") is True else "FAIL" if item.get("ok") is False else "UNKNOWN")
        parsed.append(raw_event(str(item_id), status, errno_value=item.get("errno_name", item.get("errno")), skip_reason=item.get("skip_reason", item.get("reason")), detail=item.get("detail")))
    return parsed


def reject_derived_semantics(case_id: str, doc: Any) -> None:
    if isinstance(doc, dict) and ("raw_semantics" in doc or "semantic_results" in doc):
        raise AccountingError("BLOCKED", f"{case_id} derived raw_semantics/semantic_results receipts are not original raw replay evidence")


def replay_std01_raw(artifact_root: Path, raw_artifacts: dict[str, dict[str, Any]]) -> list[dict[str, Any]]:
    discovery = load_raw_json("STD-01", artifact_root, raw_artifacts, "discovery.json")
    tap_receipt = load_raw_json("STD-01", artifact_root, raw_artifacts, "tap-accounting.json")
    if not isinstance(discovery, dict) or not isinstance(tap_receipt, dict):
        raise AccountingError("BLOCKED", "STD-01 discovery/tap accounting artifacts must be objects")
    reject_derived_semantics("STD-01", tap_receipt)
    stdout = require_bound_artifact("STD-01", artifact_root, raw_artifacts, "pjdfstest.stdout.tap")
    stderr = require_bound_artifact("STD-01", artifact_root, raw_artifacts, "pjdfstest.stderr.log")
    selected = tap_receipt.get("selected_tests") or discovery.get("selected_tests") or discovery.get("tests")
    if not isinstance(selected, list) or not selected:
        raise AccountingError("BLOCKED", "STD-01 selected TAP test list is missing for original replay")
    parser = driver_module("standard")
    parsed = parser.parse_tap_and_prove(stdout, stderr, [str(item) for item in selected])
    receipt_keys = ("tap_ok", "tap_not_ok", "tap_skip", "tap_todo", "tap_unexpected_fail", "tap_planned")
    mismatched = {key: {"receipt": tap_receipt.get(key), "original": parsed.get(key)} for key in receipt_keys if tap_receipt.get(key) != parsed.get(key)}
    if mismatched:
        raise AccountingError("BLOCKED", f"STD-01 TAP receipt contradicts original replay: {mismatched}")
    events: list[dict[str, Any]] = []
    selected = [str(item) for item in parsed.get("selected_tests", []) if item not in (None, "")]
    completed = {str(item) for item in parsed.get("observed_completed_tests", [])}
    for item_id in selected:
        events.append(raw_event(item_id, "PASS" if item_id in completed else "BLOCKED", detail="selected TAP file completion"))
    for index, line in enumerate(parsed.get("skip_lines", []) if isinstance(parsed.get("skip_lines", []), list) else []):
        events.append(raw_event(f"tap-skip-{index}:{line}", "SKIP", skip_reason=line))
    for index, line in enumerate(parsed.get("todo_lines", []) if isinstance(parsed.get("todo_lines", []), list) else []):
        events.append(raw_event(f"tap-todo-{index}:{line}", "SKIP", skip_reason=line))
    for index, line in enumerate(parsed.get("unexpected_fail_lines", []) if isinstance(parsed.get("unexpected_fail_lines", []), list) else []):
        events.append(raw_event(f"tap-fail-{index}:{line}", "FAIL", detail=line))
    if not events:
        raise AccountingError("BLOCKED", "STD-01 raw TAP replay found no events")
    return events


def replay_std02_raw(artifact_root: Path, raw_artifacts: dict[str, dict[str, Any]]) -> list[dict[str, Any]]:
    commands = load_raw_json("STD-02", artifact_root, raw_artifacts, "commands.json")
    if not isinstance(commands, list):
        raise AccountingError("BLOCKED", "STD-02 raw commands must be a list")
    parser = driver_module("ltp")
    events: list[dict[str, Any]] = []
    for index, item in enumerate(commands):
        if not isinstance(item, dict):
            raise AccountingError("BLOCKED", "STD-02 raw command entry must be an object")
        reject_derived_semantics("STD-02", item)
        artifacts = item.get("artifacts")
        if not isinstance(artifacts, dict):
            raise AccountingError("BLOCKED", "STD-02 command original artifact map is missing")
        stdout = require_bound_artifact("STD-02", artifact_root, raw_artifacts, artifacts.get("stdout"))
        stderr = require_bound_artifact("STD-02", artifact_root, raw_artifacts, artifacts.get("stderr"))
        report_rel = artifacts.get("kirk_report")
        report = require_bound_artifact("STD-02", artifact_root, raw_artifacts, report_rel) if isinstance(report_rel, str) else artifact_root / "__missing-kirk-report.json"
        command_result = {"returncode": item.get("returncode"), "timed_out": bool(item.get("timed_out"))}
        classification = parser.classify_result(command_result, stdout, stderr, report)
        recorded = normalize_status(item.get("result") or item.get("status"))
        replayed = normalize_status(classification.get("result"))
        if recorded != replayed:
            raise AccountingError("BLOCKED", f"STD-02 command receipt contradicts original replay for {item.get('test_id')}: receipt={recorded} original={replayed}")
        item_id = stable_command_key(item) or item.get("test_id") or item.get("name") or f"command-{index}"
        events.append(raw_event(str(item_id), replayed, detail={"ltp_token_counts": classification.get("ltp_token_counts")}))
        for event_index, event in enumerate(classification.get("ltp_events", []) if isinstance(classification.get("ltp_events"), list) else []):
            if isinstance(event, dict):
                event_id = event.get("id") or event.get("test_id") or f"{item_id}:event-{event_index}"
                events.append(raw_event(str(event_id), event.get("status") or event.get("result"), skip_reason=event.get("message")))
    if not events:
        raise AccountingError("BLOCKED", "STD-02 raw replay found no command events")
    return events


def replay_std03_raw(artifact_root: Path, raw_artifacts: dict[str, dict[str, Any]]) -> list[dict[str, Any]]:
    commands = load_raw_json("STD-03", artifact_root, raw_artifacts, "commands.json")
    if not isinstance(commands, list):
        raise AccountingError("BLOCKED", "STD-03 raw commands must be a list")
    parser = driver_module("fsx")
    events: list[dict[str, Any]] = []
    for index, item in enumerate(commands):
        if not isinstance(item, dict):
            raise AccountingError("BLOCKED", "STD-03 raw command entry must be an object")
        reject_derived_semantics("STD-03", item)
        artifacts = item.get("artifacts")
        if not isinstance(artifacts, dict):
            raise AccountingError("BLOCKED", "STD-03 seed original artifact map is missing")
        stdout = require_bound_artifact("STD-03", artifact_root, raw_artifacts, artifacts.get("stdout"))
        stderr = require_bound_artifact("STD-03", artifact_root, raw_artifacts, artifacts.get("stderr"))
        command_result = item.get("process") if isinstance(item.get("process"), dict) else {"returncode": item.get("returncode"), "timed_out": bool(item.get("timed_out"))}
        classification = parser.classify_seed(command_result, stdout, stderr)
        recorded = normalize_status(item.get("result") or item.get("status"))
        replayed = normalize_status(classification.get("result"))
        if recorded != replayed:
            raise AccountingError("BLOCKED", f"STD-03 seed receipt contradicts original replay for seed {item.get('seed')}: receipt={recorded} original={replayed}")
        seed = item.get("seed", index)
        events.append(raw_event(f"seed-{seed}", replayed, skip_reason=classification.get("reason"), detail={"operations_completed": classification.get("operations_completed")}))
    if not events:
        raise AccountingError("BLOCKED", "STD-03 raw replay found no seed events")
    return events


def audit_std04_trace(seed: int, trace: Any, seed_result: dict[str, Any]) -> dict[str, Any]:
    if not isinstance(trace, list):
        return {"status": "FAIL", "reason": "trace is not a list", "seed": seed}
    failures: list[dict[str, Any]] = []
    executed = seed_result.get("operations_executed")
    if executed not in (None, len(trace)):
        failures.append({"kind": "seed-result-count", "recorded": executed, "observed": len(trace)})
    observed_sha = hashlib.sha256(json.dumps(trace, sort_keys=True, separators=(",", ":")).encode()).hexdigest()
    if seed_result.get("trace_sha256") and seed_result.get("trace_sha256") != observed_sha:
        failures.append({"kind": "trace-sha256", "recorded": seed_result.get("trace_sha256"), "observed": observed_sha})
    parser = driver_module("random_fs")
    rng = random.Random(seed)
    for index, event in enumerate(trace):
        if not isinstance(event, dict):
            failures.append({"kind": "event-shape", "index": index})
            break
        expected_op = parser.generate_operation(seed, index, rng)
        if event.get("index") != index:
            failures.append({"kind": "event-index", "index": index, "observed": event.get("index")})
        if event.get("operation") != expected_op:
            failures.append({"kind": "operation-stream", "index": index, "expected": expected_op, "observed": event.get("operation")})
        reference_result = event.get("reference_result")
        target_result = event.get("target_result")
        if not isinstance(reference_result, dict) or not isinstance(target_result, dict):
            failures.append({"kind": "result-payload-shape", "index": index, "reference_type": type(reference_result).__name__, "target_type": type(target_result).__name__})
        elif reference_result != target_result:
            failures.append({"kind": "result-divergence", "index": index, "reference_result": reference_result, "target_result": target_result})
        reference_tree = event.get("reference_tree_sha256")
        target_tree = event.get("target_tree_sha256")
        if not is_sha256(reference_tree) or not is_sha256(target_tree):
            failures.append({"kind": "tree-digest-shape", "index": index, "reference_tree_sha256": reference_tree, "target_tree_sha256": target_tree})
        elif reference_tree != target_tree:
            failures.append({"kind": "tree-digest-divergence", "index": index, "reference_tree_sha256": reference_tree, "target_tree_sha256": target_tree})
        reference_count = event.get("reference_entry_count")
        target_count = event.get("target_entry_count")
        if (
            isinstance(reference_count, bool)
            or isinstance(target_count, bool)
            or not isinstance(reference_count, int)
            or not isinstance(target_count, int)
            or reference_count < 0
            or target_count < 0
        ):
            failures.append({"kind": "entry-count-shape", "index": index, "reference_entry_count": reference_count, "target_entry_count": target_count})
        elif reference_count != target_count:
            failures.append({"kind": "entry-count-divergence", "index": index, "reference_entry_count": reference_count, "target_entry_count": target_count})
        affected_paths = event.get("affected_paths")
        if not isinstance(affected_paths, dict) or not isinstance(affected_paths.get("reference"), dict) or not isinstance(affected_paths.get("target"), dict):
            failures.append({"kind": "affected-paths-shape", "index": index, "affected_paths": affected_paths})
        if failures:
            break
    return {
        "status": "PASS" if not failures else "FAIL",
        "seed": seed,
        "events": len(trace),
        "trace_sha256": observed_sha,
        "checked_contract": "deterministic op stream + per-step result equality + tree digest equality + canonical trace hash",
        "failures": failures[:5],
    }


def replay_std04_raw(artifact_root: Path, raw_artifacts: dict[str, dict[str, Any]]) -> list[dict[str, Any]]:
    seeds = load_raw_json("STD-04", artifact_root, raw_artifacts, "seed-results.json")
    if not isinstance(seeds, list):
        raise AccountingError("BLOCKED", "STD-04 raw seed results must be a list")
    events: list[dict[str, Any]] = []
    for index, item in enumerate(seeds):
        if not isinstance(item, dict):
            raise AccountingError("BLOCKED", "STD-04 raw seed entry must be an object")
        reject_derived_semantics("STD-04", item)
        seed = int(item.get("seed", index))
        trace_path = require_bound_artifact("STD-04", artifact_root, raw_artifacts, item.get("trace"))
        trace = load_json(trace_path)
        audit = audit_std04_trace(seed, trace, item)
        if audit.get("status") != "PASS":
            failures = audit.get("failures") if isinstance(audit.get("failures"), list) else []
            if any(isinstance(failure, dict) and failure.get("kind") == "trace-sha256" for failure in failures):
                raise AccountingError("BLOCKED", f"STD-04 seed {seed} trace sha256 contradicts seed-results receipt")
            if any(isinstance(failure, dict) and str(failure.get("kind", "")).startswith("result-") for failure in failures):
                raise AccountingError("BLOCKED", f"STD-04 seed {seed} trace result payload semantic audit failed: {failures}")
            if any(isinstance(failure, dict) and str(failure.get("kind", "")).startswith("tree-digest") for failure in failures):
                raise AccountingError("BLOCKED", f"STD-04 seed {seed} trace tree digest semantic audit failed: {failures}")
            raise AccountingError("BLOCKED", f"STD-04 seed {seed} trace semantic audit failed: {failures or audit.get('reason')}")
        mismatch = item.get("first_mismatch") if isinstance(item.get("first_mismatch"), dict) else {}
        target = mismatch.get("target_result") if isinstance(mismatch.get("target_result"), dict) else {}
        replayed_status = "FAIL" if mismatch else "INCONCLUSIVE" if item.get("timed_out") else "PASS"
        recorded = normalize_status(item.get("status") or item.get("result"))
        if recorded != normalize_status(replayed_status):
            raise AccountingError("BLOCKED", f"STD-04 seed receipt contradicts original trace for seed {seed}: receipt={recorded} original={replayed_status}")
        events.append(raw_event(f"seed-{seed}", replayed_status, errno_value=target.get("errno_name", target.get("errno")), skip_reason=item.get("reason"), detail={"operations_executed": len(trace), "trace_audit": audit}))
    if not events:
        raise AccountingError("BLOCKED", "STD-04 raw replay found no seed events")
    return events


def replay_raw_suite(case_id: str, artifact_root: Path, raw_artifacts: dict[str, dict[str, Any]]) -> list[dict[str, Any]]:
    if case_id == "STD-01":
        return replay_std01_raw(artifact_root, raw_artifacts)
    if case_id == "STD-02":
        return replay_std02_raw(artifact_root, raw_artifacts)
    if case_id == "STD-03":
        return replay_std03_raw(artifact_root, raw_artifacts)
    if case_id == "STD-04":
        return replay_std04_raw(artifact_root, raw_artifacts)
    raise AssertionError(case_id)


def index_raw_events(case_id: str, events: list[dict[str, Any]], label: str) -> dict[str, dict[str, Any]]:
    indexed: dict[str, dict[str, Any]] = {}
    for event in events:
        item_id = event.get("id")
        if not item_id:
            raise AccountingError("BLOCKED", f"{case_id} {label} raw event lacks stable id")
        if item_id in indexed:
            raise AccountingError("BLOCKED", f"{case_id} {label} raw event id is duplicated: {item_id}")
        indexed[str(item_id)] = event
    return indexed


def policy_covers(difference: dict[str, Any], policy: dict[str, Any]) -> bool:
    field = "skip_differences" if difference["kind"] == "skip" else "errno_differences"
    for item in policy.get(field, []):
        if not isinstance(item, dict):
            continue
        if item.get("pre_reviewed") is not True or item.get("post_run") is True or item.get("reference_only") is True or not item.get("explanation"):
            continue
        item_id = item.get("id") or item.get("test_id") or item.get("seed")
        if item_id not in (None, "", difference["id"]):
            continue
        expected = item.get("reference_errno")
        observed = item.get("target_errno")
        if expected not in (None, "", difference.get("reference_errno")):
            continue
        if observed not in (None, "", difference.get("target_errno")):
            continue
        return True
    return False


def raw_semantic_replay(case_id: str, target_root: Path, target_raw: dict[str, dict[str, Any]], reference_root: Path, reference_raw: dict[str, dict[str, Any]], policy: dict[str, Any]) -> tuple[str, dict[str, Any]]:
    target_events = replay_raw_suite(case_id, target_root, target_raw)
    reference_events = replay_raw_suite(case_id, reference_root, reference_raw)
    target_index = index_raw_events(case_id, target_events, "target")
    reference_index = index_raw_events(case_id, reference_events, "reference")
    missing_reference = sorted(set(target_index) - set(reference_index))
    missing_target = sorted(set(reference_index) - set(target_index))
    differences: list[dict[str, Any]] = []
    raw_target_failures: list[dict[str, Any]] = []
    raw_target_skips: list[dict[str, Any]] = []
    for item_id in sorted(set(target_index) & set(reference_index)):
        target = target_index[item_id]
        reference = reference_index[item_id]
        target_status = str(target.get("status"))
        reference_status = str(reference.get("status"))
        if target_status in {"FAIL", "TIMEOUT", "INCONCLUSIVE", "BLOCKED", "UNKNOWN"}:
            raw_target_failures.append({"id": item_id, "target_status": target_status, "target_errno": target.get("errno"), "reference_status": reference_status})
        if target_status == "SKIP":
            raw_target_skips.append({"id": item_id, "target_status": target_status, "reference_status": reference_status, "target_skip_reason": target.get("skip_reason")})
        if target_status == "SKIP" or reference_status == "SKIP":
            if target_status != reference_status or target.get("skip_reason") != reference.get("skip_reason"):
                differences.append({"kind": "skip", "id": item_id, "reference_status": reference_status, "target_status": target_status, "reference_skip_reason": reference.get("skip_reason"), "target_skip_reason": target.get("skip_reason")})
        if target.get("errno") != reference.get("errno") or (target_status != reference_status and "SKIP" not in {target_status, reference_status}):
            differences.append({"kind": "errno", "id": item_id, "reference_status": reference_status, "target_status": target_status, "reference_errno": reference.get("errno"), "target_errno": target.get("errno")})
    uncovered = [difference for difference in differences if not policy_covers(difference, policy)]
    evidence = {
        "target_events": len(target_events),
        "reference_events": len(reference_events),
        "missing_reference_ids": missing_reference,
        "missing_target_ids": missing_target,
        "differences": differences,
        "uncovered_differences": uncovered,
        "raw_target_failures": raw_target_failures,
        "raw_target_skips": raw_target_skips,
        "policy_explained_difference_count": len(differences) - len(uncovered),
    }
    if missing_reference or missing_target:
        return "BLOCKED", evidence
    if uncovered:
        return "BLOCKED", evidence
    if raw_target_failures:
        return "FAIL", evidence
    if raw_target_skips and not all(policy_covers({"kind": "skip", **item}, policy) for item in raw_target_skips):
        return "BLOCKED", evidence
    return "PASS", evidence


def validate_frozen_policy(entry: dict[str, Any], manifest_dir: Path, case_id: str) -> dict[str, Any]:
    frozen = entry.get("pre_review_policy")
    if not isinstance(frozen, dict):
        raise AccountingError("BLOCKED", f"{case_id} frozen pre-review policy artifact is required")
    policy_path = resolve_manifest_path(frozen.get("path"), manifest_dir)
    expected = frozen.get("sha256")
    if not is_sha256(expected) or not policy_path.is_file() or sha256_file(policy_path) != expected:
        raise AccountingError("BLOCKED", f"{case_id} frozen pre-review policy artifact is missing or hash mismatch")
    doc = load_json(policy_path)
    if not isinstance(doc, dict) or doc.get("case_id") != case_id or doc.get("status") != "FROZEN" or doc.get("reviewed_before_run") is not True or doc.get("post_run") is True:
        raise AccountingError("BLOCKED", f"{case_id} frozen pre-review policy artifact is invalid")
    return {"path": str(policy_path), "sha256": str(expected), "status": "FROZEN"}


def validate_difference_policy(entry: dict[str, Any], case_id: str, manifest_dir: Path) -> dict[str, Any]:
    policy = entry.get("difference_policy")
    if not isinstance(policy, dict):
        raise AccountingError("BLOCKED", f"{case_id} difference_policy is missing")
    problems: list[str] = []
    has_policy_items = False
    for name in ("errno_differences", "skip_differences"):
        items = policy.get(name, [])
        if not isinstance(items, list):
            problems.append(f"{name} must be a list")
            continue
        has_policy_items = has_policy_items or bool(items)
        for item in items:
            if not isinstance(item, dict):
                problems.append(f"{name} item must be an object")
                continue
            if item.get("pre_reviewed") is not True:
                problems.append(f"{name} has unreviewed difference")
            if item.get("post_run") is True:
                problems.append(f"{name} contains post-run difference")
            if item.get("reference_only") is True:
                problems.append(f"{name} contains reference-only difference")
            if not item.get("explanation"):
                problems.append(f"{name} item lacks explanation")
    exclusions = entry.get("pre_run_exclusions", [])
    if not isinstance(exclusions, list):
        problems.append("pre_run_exclusions must be a list")
    has_policy_items = has_policy_items or bool(exclusions if isinstance(exclusions, list) else [])
    for item in exclusions if isinstance(exclusions, list) else []:
        if not isinstance(item, dict):
            problems.append("pre_run_exclusion item must be an object")
            continue
        if item.get("reviewed_before_run") is not True:
            problems.append("pre_run_exclusion was not reviewed before run")
        if item.get("post_run") is True:
            problems.append("post-run exclusion is not allowed")
        if item.get("reference_only") is True:
            problems.append("reference-only exclusion cannot waive product accounting")
    frozen_policy = None
    if has_policy_items:
        frozen_policy = validate_frozen_policy(entry, manifest_dir, case_id)
    if problems:
        raise AccountingError("BLOCKED", f"{case_id} difference/exclusion policy invalid: {'; '.join(problems)}")
    return {"errno_differences": policy.get("errno_differences", []), "skip_differences": policy.get("skip_differences", []), "pre_run_exclusions": exclusions, "frozen_policy": frozen_policy}


def check_coverage_axes(proof: dict[str, Any]) -> tuple[bool, dict[str, Any]]:
    checks = {check.get("name"): check for check in proof.get("checks", []) if isinstance(check, dict)}
    axes = ((proof.get("coverage") or {}).get("axes") or {}) if isinstance(proof.get("coverage"), dict) else {}
    missing: list[dict[str, Any]] = []
    if not isinstance(axes, dict) or not axes:
        return False, {"reason": "coverage axes missing"}
    for axis_name, axis in axes.items():
        if not isinstance(axis, dict):
            missing.append({"axis": axis_name, "reason": "axis is not an object"})
            continue
        axis_checks = axis.get("checks")
        if not axis_checks:
            missing.append({"axis": axis_name, "reason": "axis checks missing"})
            continue
        names = axis_checks.values() if isinstance(axis_checks, dict) else axis_checks
        for name in names:
            check = checks.get(name)
            if not check or check.get("status") != "PASS":
                missing.append({"axis": axis_name, "check": name, "observed_status": check.get("status") if check else None})
    return not missing, {"axes": sorted(axes), "missing_or_nonpass": missing}


def result_counts(accounting: dict[str, Any]) -> dict[str, int]:
    counts = accounting.get("result_counts", {})
    if not isinstance(counts, dict):
        return {}
    parsed: dict[str, int] = {}
    for key, value in counts.items():
        try:
            parsed[str(key).upper()] = int(value)
        except (TypeError, ValueError):
            continue
    return parsed


def suite_conservation(case_id: str, proof: dict[str, Any]) -> tuple[str, dict[str, Any]]:
    accounting = proof.get("accounting")
    if not isinstance(accounting, dict):
        return "BLOCKED", {"reason": "accounting object missing"}
    status = str(proof.get("status", "")).upper()
    counts = result_counts(accounting)
    if case_id == "STD-01":
        discovered = int(accounting.get("tap_planned") or 0)
        passed = int(accounting.get("tap_ok") or 0)
        failed = int(accounting.get("tap_unexpected_fail") or 0)
        todo = int(accounting.get("tap_todo") or 0)
        skip = int(accounting.get("tap_skip") or 0)
        incomplete = int(accounting.get("observed_incomplete_files") or 0) + int(accounting.get("unobserved_files") or 0)
        complete = discovered > 0 and passed + int(accounting.get("tap_not_ok") or 0) == discovered and incomplete == 0
        return ("PASS" if complete and status == "PASS" and failed == 0 and todo == 0 and skip == 0 else "FAIL" if status == "FAIL" or failed else "BLOCKED"), {
            "unit": "TAP subtests",
            "discovered": discovered,
            "pass": passed,
            "fail": failed,
            "todo_requires_policy": todo,
            "skip_requires_policy": skip,
            "incomplete_files": incomplete,
            "complete": complete,
            "note": "TODO/SKIP are counted but not automatically pre-reviewed exclusions.",
        }
    if case_id == "STD-02":
        discovered = int(accounting.get("discovered") or 0)
        executed = int(accounting.get("executed") or 0)
        incomplete = int(accounting.get("incomplete") or 0)
        timeout = counts.get("TIMEOUT", 0)
        fail = counts.get("FAIL", 0)
        applicability = proof.get("applicability") if isinstance(proof.get("applicability"), dict) else {}
        raw_applicable_nonpass = counts.get("TBROK", 0) + counts.get("TCONF", 0)
        applicability_ok = raw_applicable_nonpass == 0 or applicability.get("status") == "PASS"
        pass_ok = status == "PASS" and discovered and executed and incomplete == 0 and timeout == 0 and fail == 0 and applicability_ok
        return ("PASS" if pass_ok else "FAIL" if fail else "BLOCKED"), {
            "unit": "LTP commands/events",
            "discovered": discovered,
            "executed": executed,
            "incomplete": incomplete,
            "result_counts": counts,
            "raw_applicable_nonpass": raw_applicable_nonpass,
            "applicability_status": applicability.get("status", "NOT_APPLIED"),
            "note": "TCONF/TBROK can pass only through the originating driver's event-level applicability proof; STD-05 does not reclassify raw summaries.",
        }
    if case_id == "STD-03":
        selected = int(accounting.get("selected_seed_count") or len(accounting.get("selected_seeds") or []))
        executed = int(accounting.get("executed") or 0)
        nonpass = sum(counts.get(name, 0) for name in ("FAIL", "TIMEOUT", "INCONCLUSIVE", "BLOCKED"))
        return ("PASS" if status == "PASS" and selected > 0 and selected == executed and nonpass == 0 else "FAIL" if counts.get("FAIL", 0) else "BLOCKED"), {
            "unit": "FSx seeds",
            "selected": selected,
            "executed": executed,
            "result_counts": counts,
            "duration_seconds_per_seed": accounting.get("duration_seconds_per_seed"),
            "cap_applied": accounting.get("cap_applied"),
        }
    if case_id == "STD-04":
        selected = len(accounting.get("selected_seeds") or [])
        executed = int(accounting.get("executed") or 0)
        nonpass = sum(counts.get(name, 0) for name in ("FAIL", "TIMEOUT", "INCONCLUSIVE", "BLOCKED"))
        return ("PASS" if status == "PASS" and selected > 0 and selected == executed and nonpass == 0 else "FAIL" if counts.get("FAIL", 0) else "BLOCKED"), {
            "unit": "random seeds/operations",
            "selected_seeds": selected,
            "executed": executed,
            "operations_per_seed": accounting.get("operations_per_seed"),
            "result_counts": counts,
        }
    raise AssertionError(case_id)


def full_contract_check(case_id: str, proof: dict[str, Any], profile: str) -> tuple[bool, dict[str, Any]]:
    accounting = proof.get("accounting") if isinstance(proof.get("accounting"), dict) else {}
    if profile != "full":
        return True, {"profile": profile, "full_contract_required": False}
    if case_id == "STD-01":
        return accounting.get("not_selected_files") == 0 and int(accounting.get("discovered_files") or 0) > 0, {"required": "all discovered pjdfstest files", "accounting": accounting}
    if case_id == "STD-02":
        return int(accounting.get("discovered") or 0) == FULL_LTP_COMMANDS and int(accounting.get("executed") or 0) == FULL_LTP_COMMANDS, {"required_commands": FULL_LTP_COMMANDS, "accounting": accounting}
    if case_id == "STD-03":
        return (
            int(accounting.get("selected_seed_count") or 0) == FULL_FSX_SEEDS
            and int(accounting.get("executed") or 0) == FULL_FSX_SEEDS
            and int(accounting.get("duration_seconds_per_seed") or 0) >= FULL_FSX_SECONDS
            and accounting.get("cap_applied") is not True
        ), {"required_seeds": FULL_FSX_SEEDS, "required_duration_seconds": FULL_FSX_SECONDS, "accounting": accounting}
    if case_id == "STD-04":
        return (
            len(accounting.get("selected_seeds") or []) == FULL_RANDOM_SEEDS
            and int(accounting.get("executed") or 0) == FULL_RANDOM_SEEDS
            and int(accounting.get("operations_per_seed") or 0) == FULL_RANDOM_OPS
        ), {"required_seeds": FULL_RANDOM_SEEDS, "required_operations_per_seed": FULL_RANDOM_OPS, "accounting": accounting}
    raise AssertionError(case_id)


def analyze_suite(entry: dict[str, Any], manifest_dir: Path, run_dir: Path, profile: str, matrix: dict[str, str]) -> dict[str, Any]:
    case_id = str(entry["case_id"])
    artifact_root, proof_path, proof = load_bound_proof(entry, manifest_dir)
    validate_proof_identity(case_id, proof, profile, matrix)
    raw_artifacts = validate_raw_artifacts(entry, artifact_root)
    identity_doc = load_raw_json(case_id, artifact_root, raw_artifacts, "identity.json")
    if not isinstance(identity_doc, dict):
        raise AccountingError("BLOCKED", f"{case_id} identity.json must be a JSON object")
    candidate = validate_candidate(entry, proof, identity_doc, profile, matrix)
    reference = validate_reference(entry, proof, identity_doc, manifest_dir, case_id, profile)
    policy = validate_difference_policy(entry, case_id, manifest_dir)
    stable_items = stable_item_evidence(case_id, artifact_root, raw_artifacts, proof)
    reference_binding = entry.get("reference")
    if not isinstance(reference_binding, dict):
        raise AccountingError("BLOCKED", f"{case_id} reference binding is missing")
    reference_root, reference_raw_artifacts = load_bound_raw_artifacts(case_id, reference_binding, manifest_dir)

    checks: list[dict[str, Any]] = []
    checks.append(build_check("binding-identity", "PASS", {"candidate": candidate, "reference": reference}))
    checks.append(build_check("hash-bound-raw-artifacts", "PASS", {"artifact_root": str(artifact_root), "raw_artifacts": list(raw_artifacts.values())}))
    checks.append(build_check("original-stable-item-ids", "PASS", stable_items))
    coverage_ok, coverage_evidence = check_coverage_axes(proof)
    checks.append(build_check("coverage-axis-pass-checks", "PASS" if coverage_ok else "BLOCKED", coverage_evidence))
    conservation_status, conservation = suite_conservation(case_id, proof)
    checks.append(build_check("suite-conservation", conservation_status, conservation))
    full_ok, full_evidence = full_contract_check(case_id, proof, profile)
    checks.append(build_check("full-profile-contract", "PASS" if full_ok else "BLOCKED", full_evidence))
    suite_status = str(proof.get("status", "")).upper()
    checks.append(build_check("suite-proof-status", "PASS" if suite_status == "PASS" else suite_status, {"status": suite_status, "reason": proof.get("reason")}))
    checks.append(build_check("pre-run-difference-policy", "PASS", policy))
    raw_status, raw_evidence = raw_semantic_replay(case_id, artifact_root, raw_artifacts, reference_root, reference_raw_artifacts, policy)
    checks.append(build_check("raw-semantic-replay", raw_status, raw_evidence))
    status, reason = status_from_checks(checks)
    if status == "PASS" and suite_status != "PASS":
        status = suite_status if suite_status in {"FAIL", "BLOCKED", "INCONCLUSIVE"} else "BLOCKED"
        reason = "suite proof is not PASS"
    return {
        "case_id": case_id,
        "status": status,
        "reason": reason,
        "proof": {"path": rel(proof_path, run_dir) if is_under(proof_path, run_dir) else str(proof_path), "sha256": sha256_file(proof_path)},
        "artifact_root": rel(artifact_root, run_dir) if is_under(artifact_root, run_dir) else str(artifact_root),
        "candidate": candidate,
        "reference": reference,
        "checks": checks,
        "accounting": conservation,
    }


def analyze_manifest(manifest_path: Path, run_dir: Path, profile: str, matrix: dict[str, str]) -> tuple[list[dict[str, Any]], list[dict[str, Any]]]:
    manifest = load_json(manifest_path)
    suites = validate_manifest_shape(manifest)
    observed: list[dict[str, Any]] = []
    checks: list[dict[str, Any]] = [build_check("manifest-shape", "PASS", {"suites": list(SUITE_IDS)})]
    candidates: list[dict[str, Any]] = []
    for entry in suites:
        result = analyze_suite(entry, manifest_path.parent, run_dir, profile, matrix)
        observed.append(result)
        candidates.append(result["candidate"])
    first = candidates[0]
    same_candidate = all(candidate == first for candidate in candidates)
    checks.append(build_check("same-candidate-binding", "PASS" if same_candidate else "BLOCKED", {"candidate": first, "all_candidates": candidates}))
    suite_statuses = {item["case_id"]: item["status"] for item in observed}
    all_pass = all(status == "PASS" for status in suite_statuses.values())
    checks.append(build_check("all-suite-accounting-pass", "PASS" if all_pass else "BLOCKED", {"suite_statuses": suite_statuses, "note": "Counts-only conservation is insufficient without each suite proof and coverage checks passing."}))
    checks.append(build_check("formal-std05-pass-readiness", "PASS" if all_pass else "BLOCKED", {"required_evidence": "all STD-01..STD-04 suite proofs passed with hash-bound target/reference raw replay and pre-run difference policy checks", "mode": "raw-replay-checker", "suite_statuses": suite_statuses}))
    return observed, checks


def blocked_proof(case_id: str, profile: str, matrix: dict[str, str], reason: str, status: str = "BLOCKED") -> dict[str, Any]:
    return {
        "case_id": case_id,
        "profile": profile,
        "matrix": matrix,
        "status": status,
        "reason": reason,
        "checks": [build_check("suite-accounting-driver", status, reason)],
        "generated_at": utc(),
    }


def parse_args(argv: list[str]) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Aggregate AFS STD-05 suite accounting")
    parser.add_argument("--case-id", default=os.environ.get("AFS_ACCEPTANCE_CASE_ID", "STD-05"))
    parser.add_argument("--profile", choices=["smoke", "full"], default=os.environ.get("AFS_ACCEPTANCE_PROFILE", "smoke"))
    parser.add_argument("--matrix-json", default=os.environ.get("AFS_ACCEPTANCE_MATRIX", "{}"))
    parser.add_argument("--run-dir", type=Path, default=Path(os.environ.get("AFS_ACCEPTANCE_RUN_DIR", "results/std-05-accounting")))
    parser.add_argument("--manifest", type=Path, default=Path(os.environ["AFS_ACCEPTANCE_ACCOUNTING_MANIFEST"]) if os.environ.get("AFS_ACCEPTANCE_ACCOUNTING_MANIFEST") else None)
    return parser.parse_args(argv)


def main(argv: list[str]) -> int:
    args = parse_args(argv)
    matrix = parse_matrix(args.matrix_json)
    run_dir = args.run_dir.resolve()
    artifacts = run_dir / "artifacts" / "std-05-accounting"
    artifacts.mkdir(parents=True, exist_ok=True)
    if args.case_id != "STD-05":
        proof = blocked_proof(args.case_id, args.profile, matrix, "accounting.py implements STD-05 only")
        print(json.dumps(proof, sort_keys=True))
        return 1
    if args.manifest is None:
        proof = blocked_proof(args.case_id, args.profile, matrix, "AFS_ACCEPTANCE_ACCOUNTING_MANIFEST or --manifest is required")
        print(json.dumps(proof, sort_keys=True))
        return 1
    try:
        suites, checks = analyze_manifest(args.manifest.resolve(), run_dir, args.profile, matrix)
        status, reason = status_from_checks(checks)
        if status == "PASS" and not all(suite["status"] == "PASS" for suite in suites):
            status = "BLOCKED"
            reason = "not all suite accounting entries passed"
        proof = {
            "case_id": "STD-05",
            "profile": args.profile,
            "matrix": matrix,
            "status": status,
            "reason": reason,
            "checks": checks,
            "coverage": {
                "profile": args.profile,
                "axes": {
                    "suites": {"values": list(SUITE_IDS), "checks": {case_id: "all-suite-accounting-pass" for case_id in SUITE_IDS}},
                    "candidate": {"values": [suites[0]["candidate"]["afs_candidate"]] if suites else [], "checks": {"candidate": "same-candidate-binding"}},
                },
            },
            "artifacts": {"root": rel(artifacts, run_dir), "manifest": rel(args.manifest.resolve(), run_dir) if is_under(args.manifest.resolve(), run_dir) else str(args.manifest.resolve())},
            "suites": suites,
            "accounting": {
                "suite_units_are_not_summed": True,
                "suites": {suite["case_id"]: suite["accounting"] for suite in suites},
                "note": "pjdfstest TAP subtests, LTP commands/events, FSx seeds and differential-random seeds/operations are distinct units.",
            },
            "generated_at": utc(),
            "notes": [
                "STD-05 is a strict read-only accounting checker. It never turns suite failures into PASS through counts-only conservation.",
                "Formal STD-05 PASS requires each child suite proof, hash-bound target/reference raw replay and pre-run difference policy checks to pass.",
                "TAP TODO/SKIP, LTP TCONF/TBROK/TFAIL/TIMEOUT, FSx timeout and random inconclusive cleanup remain non-PASS unless the originating suite proof already passed with pre-run policy evidence.",
            ],
        }
    except AccountingError as exc:
        proof = blocked_proof("STD-05", args.profile, matrix, str(exc), exc.status)
    except Exception as exc:  # noqa: BLE001
        error_path = artifacts / "setup-error.json"
        write_json(error_path, {"exception": type(exc).__name__, "message": str(exc), "traceback": traceback.format_exc()})
        proof = blocked_proof("STD-05", args.profile, matrix, f"driver setup failed: {type(exc).__name__}: {exc}")
        proof["checks"][0]["artifact"] = rel(error_path, run_dir)
    write_json(artifacts / "proof.json", proof)
    print(json.dumps(proof, sort_keys=True))
    return 0 if proof.get("status") == "PASS" else 1


if __name__ == "__main__":
    raise SystemExit(main(sys.argv[1:]))
