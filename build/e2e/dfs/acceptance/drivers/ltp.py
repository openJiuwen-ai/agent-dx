#!/usr/bin/env python3
"""LTP filesystem acceptance driver for STD-02.

The driver executes the frozen LTP filesystem command inventory prepared by
``suites/prepare_ctl_reference.sh``.  It preserves every command's raw output
and emits the acceptance runner proof JSON as the final stdout object.
"""
from __future__ import annotations

import argparse
import csv
import datetime as dt
import hashlib
import json
import os
import platform
import re
import shutil
import signal
import subprocess
import sys
import time
import traceback
import uuid
from pathlib import Path
from typing import Any

try:
    from target_identity import target_checks
except ModuleNotFoundError:
    import importlib.util

    _target_identity_path = Path(__file__).resolve().with_name("target_identity.py")
    _target_identity_spec = importlib.util.spec_from_file_location("target_identity", _target_identity_path)
    if _target_identity_spec is None or _target_identity_spec.loader is None:
        raise
    _target_identity_module = importlib.util.module_from_spec(_target_identity_spec)
    _target_identity_spec.loader.exec_module(_target_identity_module)
    target_checks = _target_identity_module.target_checks

LTP_REV = "3a64d78f58bdceba93ed321e91215fb969a047ed"
LTP_TAG = "20260529"
DEFAULT_REFERENCE_ROOT = Path("/mnt/lima-afsctlstate/afs-acceptance/suites-reference")
DEFAULT_SUITE_ROOT = DEFAULT_REFERENCE_ROOT / "src" / "ltp"
DEFAULT_INSTALL_ROOT = Path("/home/lzc.guest/afs-tools/ltp-install")
DEFAULT_EXPANDED_TSV = DEFAULT_REFERENCE_ROOT / "evidence" / "inventory" / "ltp-filesystem-expanded.tsv"
SMOKE_TEST_IDS = ["open01", "read01", "write01", "stat01", "chmod01", "fcntl14"]
RESULT_VALUES = {"PASS", "FAIL", "TBROK", "TCONF", "TWARN", "TIMEOUT", "NOT_RUN"}
LTP_EVENT_TOKENS = ("TPASS", "TFAIL", "TBROK", "TCONF", "TWARN", "TINFO")
# Some legacy LTP tests keep paths in small fixed C buffers.  Keep runner-created
# paths short while still placing test data under the requested mount/base dir.
MAX_LEGACY_LTP_TMPDIR_LEN = 96


def utc() -> str:
    return dt.datetime.now(dt.timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")


def write_json(path: Path, value: Any) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(value, indent=2, sort_keys=True) + "\n", encoding="utf-8")


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
    except Exception as exc:  # noqa: BLE001 - evidence records exact failure
        return {"argv": argv, "returncode": None, "exception": type(exc).__name__, "message": str(exc)}


def rel(path: Path, base: Path) -> str:
    return str(path.resolve().relative_to(base.resolve()))


def build_check(name: str, status: str, evidence: Any, artifact: str | None = None) -> dict[str, Any]:
    check = {"name": name, "status": status, "evidence": evidence}
    if artifact:
        check["artifact"] = artifact
    return check


def is_under(child: Path, parent: Path) -> bool:
    try:
        child.resolve().relative_to(parent.resolve())
        return True
    except ValueError:
        return False


def resolve_base_dir(mount: Path, base_dir: Path | None) -> Path:
    if base_dir is None:
        return mount
    return base_dir if base_dir.is_absolute() else mount / base_dir


def mount_identity(mount: Path) -> dict[str, Any]:
    return run_text(["findmnt", "-T", str(mount), "-o", "TARGET,SOURCE,FSTYPE,OPTIONS", "--json"])


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


def ltp_identity(suite_root: Path, install_root: Path, expanded_tsv: Path) -> dict[str, Any]:
    root = suite_root.resolve()
    git = ["git", "-c", f"safe.directory={root}", "-C", str(root)]
    head = run_text(git + ["rev-parse", "HEAD"])
    status = run_text(git + ["status", "--porcelain"])
    kirk = install_root / "kirk"
    kirk_version = run_text([str(kirk), "--version"]) if kirk.exists() else {"returncode": None, "stdout": "", "stderr": "missing kirk"}
    return {
        "suite_root": str(suite_root),
        "install_root": str(install_root),
        "expected_tag": LTP_TAG,
        "expected_revision": LTP_REV,
        "git_head": head.get("stdout", "").strip(),
        "git_head_command": head,
        "git_status_porcelain": status.get("stdout", ""),
        "git_status_command": status,
        "kirk": str(kirk),
        "kirk_exists": kirk.is_file() and os.access(kirk, os.X_OK),
        "kirk_sha256": sha256_file(kirk) if kirk.is_file() else None,
        "kirk_version": kirk_version,
        "expanded_tsv": str(expanded_tsv),
        "expanded_tsv_sha256": sha256_file(expanded_tsv) if expanded_tsv.is_file() else None,
    }


def read_commands(expanded_tsv: Path) -> list[dict[str, str]]:
    records: list[dict[str, str]] = []
    with expanded_tsv.open(newline="", encoding="utf-8") as handle:
        reader = csv.DictReader(handle, delimiter="\t")
        for row in reader:
            selector = (row.get("selector") or "").strip()
            test_id = (row.get("test_id") or "").strip()
            command = (row.get("command") or "").strip()
            if not selector or not test_id or not command:
                continue
            records.append({"selector": selector, "test_id": test_id, "command": command})
    return records


def select_commands(records: list[dict[str, str]], profile: str, max_tests: int | None) -> tuple[list[dict[str, str]], dict[str, Any]]:
    if profile == "smoke":
        by_id = {record["test_id"]: record for record in records}
        selected = [by_id[test_id] for test_id in SMOKE_TEST_IDS if test_id in by_id]
        selection = {"mode": "smoke", "requested_test_ids": SMOKE_TEST_IDS, "missing_smoke_test_ids": [test_id for test_id in SMOKE_TEST_IDS if test_id not in by_id]}
    else:
        selected = list(records)
        selection = {"mode": "full", "requested_test_ids": "all"}
    if max_tests is not None:
        selected = selected[:max_tests]
        selection["max_tests"] = max_tests
    selection["total_commands"] = len(records)
    selection["selected_count"] = len(selected)
    return selected, selection


def shell_quote(value: str) -> str:
    return "'" + value.replace("'", "'\"'\"'") + "'"


def ltp_shell_command(command: str, test_dir: Path, tmp_dir: Path, install_root: Path) -> str:
    ltp_bin_path = f"{install_root / 'testcases' / 'bin'}:{install_root / 'bin'}"
    return " && ".join(
        [
            f"cd {shell_quote(str(test_dir))}",
            f"export LTPROOT={shell_quote(str(install_root))}",
            f"export PATH={shell_quote(ltp_bin_path)}:\"$PATH\"",
            f"export TMPDIR={shell_quote(str(tmp_dir))}",
            f"export TMP={shell_quote(str(tmp_dir))}",
            f"export TEMP={shell_quote(str(tmp_dir))}",
            command,
        ]
    )


def execution_payload(record: dict[str, str]) -> str:
    parts = record["command"].split(maxsplit=1)
    if len(parts) == 2 and parts[0] == record["test_id"]:
        return parts[1]
    return record["command"]


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
            err.write(f"LTP command timed out after {timeout}s\n".encode())
    return {"argv": argv, "cwd": str(cwd), "returncode": returncode, "timed_out": timed_out, "timeout_seconds": timeout, "duration_seconds": round(time.time() - started, 3)}


def load_json_if_present(path: Path) -> Any | None:
    if not path.exists() or path.stat().st_size == 0:
        return None
    try:
        return json.loads(path.read_text(errors="replace"))
    except Exception:  # noqa: BLE001
        return None


def count_ltp_tokens(text: str) -> dict[str, int]:
    counts = {key: 0 for key in LTP_EVENT_TOKENS}
    for token in counts:
        counts[token] = len(re.findall(rf"\b{token}\b", text))
    return counts


def extract_ltp_events(text: str) -> list[dict[str, Any]]:
    events: list[dict[str, Any]] = []
    pattern = re.compile(r"\b(?P<status>TPASS|TFAIL|TBROK|TCONF|TWARN|TINFO)\b\s*:?")
    for line_no, line in enumerate(text.splitlines(), start=1):
        match = pattern.search(line)
        if not match:
            continue
        prefix = line[: match.start()].strip()
        message = line[match.end() :].strip()
        case_index = None
        index_match = re.search(r"(?:^|\s)(\d+)\s*$", prefix)
        if index_match:
            case_index = int(index_match.group(1))
        source = extract_event_source(line, message)
        events.append({
            "line": line_no,
            "status": match.group("status"),
            "case_index": case_index,
            "prefix": prefix,
            "message": message,
            "source": source,
            "raw": line,
        })
    return events


def extract_event_source(raw: str, message: str) -> str | None:
    source_match = re.search(r"(?P<source>(?:[A-Za-z0-9_.+/-]+/)?[A-Za-z0-9_.+-]+\.(?:c|h):\d+)", raw)
    if source_match:
        source = source_match.group("source")
        # Keep file:line stable across suite relocation.  The pinned suite revision
        # and expanded TSV hash bind the source tree identity separately.
        return "/".join(source.split("/")[-1:])
    source_match = re.search(r"(?P<source>(?:[A-Za-z0-9_.+/-]+/)?[A-Za-z0-9_.+-]+\.(?:c|h):\d+)", message)
    if source_match:
        return "/".join(source_match.group("source").split("/")[-1:])
    return None


def short_fixture_name() -> str:
    return f".l{uuid.uuid4().hex[:8]}"


def command_case_dir(fixture: Path, index: int) -> Path:
    return fixture / f"{index:04d}"


def ltp_path_budget_ok(tmp_dir: Path) -> bool:
    return len(str(tmp_dir)) <= MAX_LEGACY_LTP_TMPDIR_LEN



APPLICABLE_BLOCKING_STATUSES = {"TCONF", "TBROK", "TFAIL"}
PRE_REVIEWABLE_STATUSES = {"TCONF", "TBROK"}
REFERENCE_BACKENDS = {"reference", "ext4"}
REFERENCE_ONLY_SCOPES = {"alternateFS-check"}


def manifest_binding_value(manifest: dict[str, Any], *names: str) -> Any | None:
    binding = manifest.get("binding") if isinstance(manifest.get("binding"), dict) else {}
    for name in names:
        if name in manifest:
            return manifest[name]
        if name in binding:
            return binding[name]
    return None




def load_json_no_duplicate_keys(path: Path) -> Any:
    def reject_duplicates(pairs: list[tuple[str, Any]]) -> dict[str, Any]:
        result: dict[str, Any] = {}
        for key, value in pairs:
            if key in result:
                raise RuntimeError(f"duplicate JSON object key {key!r} in {path}")
            result[key] = value
        return result

    try:
        return json.loads(path.read_text(encoding="utf-8"), object_pairs_hook=reject_duplicates)
    except json.JSONDecodeError as exc:
        raise RuntimeError(f"invalid JSON in {path}: {exc}") from exc


def timeout_policy_binding_value(policy: dict[str, Any], *names: str) -> Any | None:
    binding = policy.get("binding") if isinstance(policy.get("binding"), dict) else {}
    for name in names:
        if name in binding:
            return binding[name]
        if name in policy:
            return policy[name]
    return None


def load_timeout_policy(path: Path, identity: dict[str, Any], selected: list[dict[str, str]], per_test_timeout: int) -> dict[str, Any]:
    raw = load_json_no_duplicate_keys(path)
    if not isinstance(raw, dict):
        raise RuntimeError("timeout policy must be a JSON object")
    allowed_top = {"schema", "binding", "timeouts"}
    extra_top = sorted(set(raw) - allowed_top)
    if extra_top:
        raise RuntimeError("invalid timeout policy: unknown top-level fields: " + ", ".join(extra_top))
    if raw.get("schema") != 1:
        raise RuntimeError("invalid timeout policy: schema must be 1")
    binding = raw.get("binding")
    if not isinstance(binding, dict):
        raise RuntimeError("invalid timeout policy: binding must be an object")
    allowed_binding = {"suite_revision", "expanded_tsv_sha256", "kernel_release", "machine"}
    extra_binding = sorted(set(binding) - allowed_binding)
    if extra_binding:
        raise RuntimeError("invalid timeout policy: unknown binding fields: " + ", ".join(extra_binding))
    suite = identity["suite"]
    expected = {
        "suite_revision": suite.get("git_head"),
        "expanded_tsv_sha256": suite.get("expanded_tsv_sha256"),
        "kernel_release": identity.get("platform", {}).get("release"),
        "machine": identity.get("platform", {}).get("machine"),
    }
    errors: list[str] = []
    for key, expected_value in expected.items():
        observed = timeout_policy_binding_value(raw, key)
        if observed != expected_value:
            errors.append(f"{key} mismatch: policy={observed!r} observed={expected_value!r}")
    timeouts = raw.get("timeouts")
    if not isinstance(timeouts, list):
        errors.append("timeouts must be a list")
        timeouts = []
    selected_ids = {record["test_id"] for record in selected}
    seen: set[str] = set()
    normalized: dict[str, dict[str, int]] = {}
    allowed_entry = {"test_id", "exec_timeout_seconds", "suite_timeout_seconds"}
    for index, entry in enumerate(timeouts):
        if not isinstance(entry, dict):
            errors.append(f"timeouts[{index}] must be an object")
            continue
        extra_entry = sorted(set(entry) - allowed_entry)
        if extra_entry:
            errors.append(f"timeouts[{index}] unknown fields: {', '.join(extra_entry)}")
        missing = sorted(allowed_entry - set(entry))
        if missing:
            errors.append(f"timeouts[{index}] missing fields: {', '.join(missing)}")
            continue
        test_id = entry.get("test_id")
        if not isinstance(test_id, str) or not test_id:
            errors.append(f"timeouts[{index}].test_id must be a non-empty string")
            continue
        if test_id in seen:
            errors.append(f"duplicate timeout policy test_id {test_id!r}")
        seen.add(test_id)
        if test_id not in selected_ids:
            errors.append(f"unknown timeout policy test_id {test_id!r}")
        exec_timeout = entry.get("exec_timeout_seconds")
        suite_timeout = entry.get("suite_timeout_seconds")
        if not isinstance(exec_timeout, int) or isinstance(exec_timeout, bool) or exec_timeout <= 0:
            errors.append(f"timeouts[{index}].exec_timeout_seconds must be a positive integer")
            continue
        if not isinstance(suite_timeout, int) or isinstance(suite_timeout, bool) or suite_timeout <= 0:
            errors.append(f"timeouts[{index}].suite_timeout_seconds must be a positive integer")
            continue
        if exec_timeout < per_test_timeout:
            errors.append(f"timeouts[{index}] exec_timeout_seconds {exec_timeout} is below per-test baseline {per_test_timeout}")
        if suite_timeout < 240:
            errors.append(f"timeouts[{index}] suite_timeout_seconds {suite_timeout} is below baseline 240")
        if suite_timeout <= exec_timeout:
            errors.append(f"timeouts[{index}] suite_timeout_seconds {suite_timeout} must be greater than exec_timeout_seconds {exec_timeout}")
        normalized[test_id] = {"exec_timeout_seconds": exec_timeout, "suite_timeout_seconds": suite_timeout}
    if errors:
        raise RuntimeError("invalid timeout policy: " + "; ".join(errors))
    policy_bytes = path.read_bytes()
    return {
        "enabled": True,
        "path": str(path),
        "sha256": hashlib.sha256(policy_bytes).hexdigest(),
        "size_bytes": len(policy_bytes),
        "schema": 1,
        "binding": expected,
        "timeouts": normalized,
        "default": {"exec_timeout_seconds": per_test_timeout, "suite_timeout_seconds": max(240, per_test_timeout + 10, per_test_timeout * 2)},
    }


def command_timeouts(test_id: str, per_test_timeout: int, timeout_policy: dict[str, Any] | None) -> dict[str, Any]:
    default = {"exec_timeout_seconds": per_test_timeout, "suite_timeout_seconds": max(240, per_test_timeout + 10, per_test_timeout * 2), "source": "default"}
    if not timeout_policy:
        return default
    override = timeout_policy.get("timeouts", {}).get(test_id)
    if not override:
        return default
    return {"exec_timeout_seconds": override["exec_timeout_seconds"], "suite_timeout_seconds": override["suite_timeout_seconds"], "source": "timeout-policy"}

def load_applicability_manifest(path: Path, identity: dict[str, Any]) -> dict[str, Any]:
    manifest = json.loads(path.read_text(encoding="utf-8"))
    if not isinstance(manifest, dict):
        raise RuntimeError("applicability manifest must be a JSON object")
    suite = identity["suite"]
    expected = {
        "suite_revision": suite.get("git_head"),
        "expanded_tsv_sha256": suite.get("expanded_tsv_sha256"),
        "machine": identity.get("platform", {}).get("machine"),
    }
    optional_expected = {"kernel_release": identity.get("platform", {}).get("release")}
    errors: list[str] = []
    for key, expected_value in expected.items():
        observed = manifest_binding_value(manifest, key)
        if observed != expected_value:
            errors.append(f"{key} mismatch: manifest={observed!r} observed={expected_value!r}")
    for key, expected_value in optional_expected.items():
        observed = manifest_binding_value(manifest, key)
        if observed is not None and observed != expected_value:
            errors.append(f"{key} mismatch: manifest={observed!r} observed={expected_value!r}")
    policy = manifest.get("policy") if isinstance(manifest.get("policy"), dict) else {}
    raw_commands_required = policy.get("raw_commands_required")
    if raw_commands_required is not None:
        observed_selected = identity.get("selection", {}).get("selected_count")
        observed_total = identity.get("selection", {}).get("total_commands")
        if observed_selected != raw_commands_required or observed_total != raw_commands_required:
            errors.append(f"raw_commands_required mismatch: manifest={raw_commands_required!r} selected={observed_selected!r} total={observed_total!r}")
    entries = manifest.get("entries")
    if not isinstance(entries, list) or not entries:
        errors.append("entries must be a non-empty list")
        entries = []
    normalized_entries: list[dict[str, Any]] = []
    for idx, raw_entry in enumerate(entries):
        if not isinstance(raw_entry, dict):
            errors.append(f"entries[{idx}] must be an object")
            continue
        matcher = raw_entry.get("event_match") if isinstance(raw_entry.get("event_match"), dict) else raw_entry
        test_id = raw_entry.get("test_id") or matcher.get("test_id")
        status = matcher.get("status") or raw_entry.get("status")
        source = matcher.get("source") or raw_entry.get("source")
        message_regex = matcher.get("message_regex") or raw_entry.get("message_regex")
        disposition = raw_entry.get("disposition")
        rationale = raw_entry.get("rationale")
        scope = raw_entry.get("scope")
        ordinary_required = bool(raw_entry.get("ordinary_subtests_required", False))
        coverage_link = raw_entry.get("ordinary_coverage_link") or raw_entry.get("ordinary_coverage_test_ids")
        fields = {"test_id": test_id, "status": status, "source": source, "message_regex": message_regex, "disposition": disposition, "rationale": rationale, "scope": scope}
        missing = [name for name, value in fields.items() if not value]
        if missing:
            errors.append(f"entries[{idx}] missing required fields: {', '.join(missing)}")
            continue
        if status not in PRE_REVIEWABLE_STATUSES:
            errors.append(f"entries[{idx}] status {status!r} is not pre-reviewable")
        if disposition != "pre_reviewed_not_applicable":
            errors.append(f"entries[{idx}] disposition must be pre_reviewed_not_applicable")
        try:
            re.compile(str(message_regex))
        except re.error as exc:
            errors.append(f"entries[{idx}] message_regex is invalid: {exc}")
        normalized_entries.append({
            "id": raw_entry.get("id") or f"{test_id}:{status}:{source}:{idx}",
            "test_id": str(test_id),
            "status": str(status),
            "source": str(source),
            "message_regex": str(message_regex),
            "scope": str(scope),
            "disposition": str(disposition),
            "rationale": str(rationale),
            "ordinary_subtests_required": ordinary_required,
            "ordinary_coverage_link": coverage_link,
        })
    if errors:
        raise RuntimeError("invalid applicability manifest: " + "; ".join(errors))
    digest = hashlib.sha256(path.read_bytes()).hexdigest()
    reference_only_scopes = policy.get("reference_only_scopes", sorted(REFERENCE_ONLY_SCOPES))
    if not isinstance(reference_only_scopes, list) or not all(isinstance(item, str) for item in reference_only_scopes):
        raise RuntimeError("invalid applicability manifest: policy.reference_only_scopes must be a list of strings")
    return {
        "path": str(path),
        "sha256": digest,
        "binding": {**expected, **{k: v for k, v in optional_expected.items() if manifest_binding_value(manifest, k) is not None}},
        "policy": {"reference_only_scopes": sorted(set(reference_only_scopes))},
        "entries": normalized_entries,
    }


def event_matches_entry(event: dict[str, Any], entry: dict[str, Any]) -> bool:
    if event.get("status") != entry["status"]:
        return False
    entry_source = entry["source"]
    event_source = event.get("source")
    if entry_source == "__none__":
        if event_source is not None:
            return False
    elif event_source != entry_source:
        return False
    return re.search(entry["message_regex"], event.get("message", "")) is not None or re.search(entry["message_regex"], event.get("raw", "")) is not None


def applicability_target_context(identity: dict[str, Any] | None) -> dict[str, Any]:
    product = identity.get("product", {}) if identity else {}
    matrix = identity.get("matrix", {}) if identity else {}
    backend = product.get("backend") or matrix.get("backend") or matrix.get("reference") or "reference"
    backend_text = str(backend).lower()
    return {
        "backend": backend,
        "is_reference_backend": backend_text in REFERENCE_BACKENDS,
    }


def entry_allowed_for_target(entry: dict[str, Any], manifest: dict[str, Any] | None, target: dict[str, Any]) -> bool:
    policy = manifest.get("policy", {}) if manifest else {}
    reference_only_scopes = set(policy.get("reference_only_scopes", sorted(REFERENCE_ONLY_SCOPES)))
    return target.get("is_reference_backend", False) or entry.get("scope") not in reference_only_scopes


def apply_applicability(command_records: list[dict[str, Any]], manifest: dict[str, Any] | None, identity: dict[str, Any] | None = None) -> dict[str, Any]:
    raw_blocking_events: list[dict[str, Any]] = []
    pre_reviewed_events: list[dict[str, Any]] = []
    unmatched_events: list[dict[str, Any]] = []
    missing_entries: list[dict[str, Any]] = []
    ordinary_coverage_failures: list[dict[str, Any]] = []
    target_context_failures: list[dict[str, Any]] = []
    matched_entry_ids: set[str] = set()
    selected_test_ids = {record["test_id"] for record in command_records}
    target_context = applicability_target_context(identity)
    entries = manifest.get("entries", []) if manifest else []
    entries_by_test: dict[str, list[dict[str, Any]]] = {}
    for entry in entries:
        entries_by_test.setdefault(entry["test_id"], []).append(entry)

    tpass_by_test = {record["test_id"]: record.get("ltp_event_counts", {}).get("TPASS", 0) for record in command_records}
    for record in command_records:
        tpass_count = record.get("ltp_event_counts", {}).get("TPASS", 0)
        for event in record.get("ltp_events", []):
            if event.get("status") not in APPLICABLE_BLOCKING_STATUSES:
                continue
            event_ref = {"index": record["index"], "test_id": record["test_id"], "status": event.get("status"), "source": event.get("source"), "line": event.get("line"), "message": event.get("message"), "raw": event.get("raw")}
            raw_blocking_events.append(event_ref)
            matched_entry = None
            for entry in entries_by_test.get(record["test_id"], []):
                if event_matches_entry(event, entry):
                    matched_entry = entry
                    break
            if not matched_entry:
                unmatched_events.append(event_ref)
                continue
            allowed_for_target = entry_allowed_for_target(matched_entry, manifest, target_context)
            if allowed_for_target:
                matched_entry_ids.add(matched_entry["id"])
            else:
                target_context_failures.append({
                    **event_ref,
                    "entry_id": matched_entry["id"],
                    "scope": matched_entry["scope"],
                    "rationale": matched_entry["rationale"],
                    "reason": "pre-reviewed event is scoped to the reference filesystem and cannot waive a product backend result",
                    "target": target_context,
                })
                continue
            if matched_entry["ordinary_subtests_required"] and tpass_count <= 0:
                coverage_link = matched_entry.get("ordinary_coverage_link")
                if isinstance(coverage_link, str):
                    coverage_ids = [coverage_link]
                elif isinstance(coverage_link, list):
                    coverage_ids = [str(item) for item in coverage_link]
                else:
                    coverage_ids = []
                if not any(tpass_by_test.get(test_id, 0) > 0 for test_id in coverage_ids):
                    ordinary_coverage_failures.append({"entry_id": matched_entry["id"], "test_id": record["test_id"], "coverage_link": coverage_link, "reason": "ordinary_subtests_required but neither this command nor linked commands have TPASS coverage"})
            event_ref = {**event_ref, "entry_id": matched_entry["id"], "scope": matched_entry["scope"], "rationale": matched_entry["rationale"]}
            pre_reviewed_events.append(event_ref)

    for entry in entries:
        if entry["test_id"] in selected_test_ids and entry["id"] not in matched_entry_ids and entry_allowed_for_target(entry, manifest, target_context):
            missing_entries.append({"entry_id": entry["id"], "test_id": entry["test_id"], "status": entry["status"], "source": entry["source"], "message_regex": entry["message_regex"], "reason": "selected command did not emit the expected pre-reviewed event"})

    enabled = manifest is not None
    errors = []
    if unmatched_events:
        errors.append("unmatched blocking events remain")
    if missing_entries:
        errors.append("manifest expected events were not observed")
    if ordinary_coverage_failures:
        errors.append("ordinary TPASS coverage requirement failed")
    if target_context_failures:
        errors.append("pre-reviewed reference filesystem events cannot waive product backend results")
    if any(event["status"] == "TFAIL" for event in raw_blocking_events):
        errors.append("TFAIL events are never pre-reviewable")
    effective_blocking_events = list(unmatched_events)
    if ordinary_coverage_failures:
        effective_blocking_events.extend(ordinary_coverage_failures)
    if missing_entries:
        effective_blocking_events.extend(missing_entries)
    if target_context_failures:
        effective_blocking_events.extend(target_context_failures)
    return {
        "enabled": enabled,
        "manifest": {"path": manifest.get("path"), "sha256": manifest.get("sha256"), "binding": manifest.get("binding"), "policy": manifest.get("policy"), "entries": len(entries)} if manifest else None,
        "target": target_context,
        "raw_blocking_event_count": len(raw_blocking_events),
        "pre_reviewed_event_count": len(pre_reviewed_events),
        "unmatched_event_count": len(unmatched_events),
        "missing_entry_count": len(missing_entries),
        "ordinary_coverage_failure_count": len(ordinary_coverage_failures),
        "target_context_failure_count": len(target_context_failures),
        "raw_blocking_events": raw_blocking_events,
        "pre_reviewed_events": pre_reviewed_events,
        "unmatched_events": unmatched_events,
        "missing_entries": missing_entries,
        "ordinary_coverage_failures": ordinary_coverage_failures,
        "target_context_failures": target_context_failures,
        "effective_blocking_events": effective_blocking_events,
        "status": "PASS" if enabled and not errors else ("BLOCKED" if enabled else "NOT_APPLIED"),
        "errors": errors,
    }


def classify_result(command_result: dict[str, Any], stdout: Path, stderr: Path, report: Path) -> dict[str, Any]:
    combined = ""
    if stdout.exists():
        combined += stdout.read_text(errors="replace")
    if stderr.exists():
        combined += "\n" + stderr.read_text(errors="replace")
    token_counts = count_ltp_tokens(combined)
    ltp_events = extract_ltp_events(combined)
    ltp_event_counts = {key: 0 for key in LTP_EVENT_TOKENS}
    for event in ltp_events:
        ltp_event_counts[event["status"]] += 1
    kirk_json = load_json_if_present(report)
    if command_result["timed_out"]:
        result = "TIMEOUT"
    elif re.search(r"\b(Command timeout|timed out after|TimeoutExpired)\b", combined, re.I):
        result = "TIMEOUT"
    elif token_counts["TFAIL"] > 0:
        result = "FAIL"
    elif token_counts["TBROK"] > 0:
        result = "TBROK"
    elif token_counts["TCONF"] > 0:
        result = "TCONF"
    elif command_result["returncode"] == 0:
        result = "PASS"
    else:
        result = "FAIL"
    return {
        "result": result,
        "ltp_token_counts": token_counts,
        "ltp_event_counts": ltp_event_counts,
        "ltp_events": ltp_events,
        "kirk_json_present": kirk_json is not None,
        "kirk_json_top_keys": sorted(kirk_json.keys()) if isinstance(kirk_json, dict) else None,
    }


def run_ltp_commands(
    selected: list[dict[str, str]],
    fixture: Path,
    install_root: Path,
    artifacts: Path,
    per_test_timeout: int,
    timeout_policy: dict[str, Any] | None = None,
) -> tuple[list[dict[str, Any]], dict[str, Any]]:
    command_records: list[dict[str, Any]] = []
    counts = {value: 0 for value in sorted(RESULT_VALUES)}
    total_duration = 0.0
    for index, record in enumerate(selected, start=1):
        safe_test = re.sub(r"[^A-Za-z0-9_.-]+", "_", record["test_id"])[:80]
        case_dir = command_case_dir(fixture, index)
        test_dir = case_dir / "w"
        tmp_dir = case_dir / "t"
        out_dir = artifacts / "commands" / f"{index:04d}-{safe_test}"
        kirk_tmp = out_dir / "kirk-tmp"
        test_dir.mkdir(parents=True, exist_ok=True)
        tmp_dir.mkdir(parents=True, exist_ok=True)
        out_dir.mkdir(parents=True, exist_ok=True)
        kirk_tmp.mkdir(parents=True, exist_ok=True)
        os.chmod(tmp_dir, 0o1777)
        if not ltp_path_budget_ok(tmp_dir):
            raise RuntimeError(f"LTP tmpdir path is too long for legacy fixed-buffer tests: {tmp_dir} ({len(str(tmp_dir))} > {MAX_LEGACY_LTP_TMPDIR_LEN})")
        stdout = out_dir / "stdout.log"
        stderr = out_dir / "stderr.log"
        report = out_dir / "kirk-report.json"
        payload = execution_payload(record)
        command = ltp_shell_command(payload, test_dir, tmp_dir, install_root)
        timeout_fact = command_timeouts(record["test_id"], per_test_timeout, timeout_policy)
        exec_timeout = timeout_fact["exec_timeout_seconds"]
        suite_timeout = timeout_fact["suite_timeout_seconds"]
        process_timeout = suite_timeout + 5 if timeout_fact["source"] == "timeout-policy" else max(per_test_timeout + 5, per_test_timeout * 2 + 5)
        timeout_fact = {**timeout_fact, "process_timeout_seconds": process_timeout}
        argv = [
            str(install_root / "kirk"),
            "--no-colors",
            "--tmp-dir",
            str(kirk_tmp),
            "--json-report",
            str(report),
            "--run-command",
            command,
            "--exec-timeout",
            str(exec_timeout),
            "--suite-timeout",
            str(suite_timeout),
            "--workers",
            "1",
        ]
        command_result = run_bounded(argv, cwd=install_root, timeout=process_timeout, stdout_path=stdout, stderr_path=stderr)
        classification = classify_result(command_result, stdout, stderr, report)
        counts[classification["result"]] += 1
        total_duration += float(command_result["duration_seconds"])
        command_record = {
            "index": index,
            "selector": record["selector"],
            "test_id": record["test_id"],
            "command": record["command"],
            "result": classification["result"],
            "returncode": command_result["returncode"],
            "timed_out": command_result["timed_out"],
            "duration_seconds": command_result["duration_seconds"],
            "timeout": timeout_fact,
            "ltp_token_counts": classification["ltp_token_counts"],
            "kirk_json_present": classification["kirk_json_present"],
            "kirk_json_top_keys": classification["kirk_json_top_keys"],
            "ltp_event_counts": classification["ltp_event_counts"],
            "ltp_events": classification["ltp_events"],
            "artifacts": {
                "stdout": str(stdout.relative_to(artifacts)),
                "stderr": str(stderr.relative_to(artifacts)),
                "kirk_report": str(report.relative_to(artifacts)),
                "command": str((out_dir / "command.json").relative_to(artifacts)),
            },
        }
        write_json(out_dir / "command.json", {"record": record, "execution_payload": payload, "argv": argv, "shell_command": command, "timeout": timeout_fact, "result": command_result, "classification": classification})
        command_records.append(command_record)
    return command_records, {"counts": counts, "duration_seconds": round(total_duration, 3)}


def parse_args(argv: list[str]) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Run AFS STD-02 LTP filesystem driver")
    parser.add_argument("--case-id", default=os.environ.get("AFS_ACCEPTANCE_CASE_ID", "STD-02"))
    parser.add_argument("--profile", choices=["smoke", "full"], default=os.environ.get("AFS_ACCEPTANCE_PROFILE", "smoke"))
    parser.add_argument("--matrix-json", default=os.environ.get("AFS_ACCEPTANCE_MATRIX", "{}"))
    parser.add_argument("--run-dir", type=Path, default=Path(os.environ.get("AFS_ACCEPTANCE_RUN_DIR", "results/std-02-ltp")))
    parser.add_argument("--mount", type=Path, default=Path(os.environ["AFS_ACCEPTANCE_MOUNT"]) if os.environ.get("AFS_ACCEPTANCE_MOUNT") else None)
    parser.add_argument("--base-dir", type=Path, default=Path(os.environ["AFS_ACCEPTANCE_BASE_DIR"]) if os.environ.get("AFS_ACCEPTANCE_BASE_DIR") else None)
    parser.add_argument("--suite-root", type=Path, default=DEFAULT_SUITE_ROOT)
    parser.add_argument("--ltp-install", type=Path, default=DEFAULT_INSTALL_ROOT)
    parser.add_argument("--expanded-tsv", type=Path, default=DEFAULT_EXPANDED_TSV)
    parser.add_argument("--per-test-timeout", type=int, default=120)
    parser.add_argument("--max-tests", type=int, default=None, help="Development-only cap; any full run with this set is BLOCKED, never PASS.")
    parser.add_argument("--process-pid", default=os.environ.get("AFS_ACCEPTANCE_PROCESS_PID"))
    parser.add_argument("--meta-process-pid", default=os.environ.get("AFS_ACCEPTANCE_META_PROCESS_PID"))
    parser.add_argument("--backend", default=os.environ.get("AFS_ACCEPTANCE_BACKEND"))
    parser.add_argument("--meta", default=os.environ.get("AFS_ACCEPTANCE_META"))
    parser.add_argument("--allow-nonroot-fixture", action="store_true", help="Only for driver self-tests with a fake kirk; real STD-02 must run as root.")
    parser.add_argument("--allow-unpinned-suite-fixture", action="store_true", help="Only for driver self-tests with a fake LTP tree; real STD-02 must match the pinned revision.")
    parser.add_argument("--applicability-manifest", type=Path, default=Path(os.environ["AFS_ACCEPTANCE_LTP_APPLICABILITY_MANIFEST"]) if os.environ.get("AFS_ACCEPTANCE_LTP_APPLICABILITY_MANIFEST") else None, help="Pre-run event-level applicability manifest. Raw command results remain preserved; unmatched nonpass events fail closed.")
    parser.add_argument("--timeout-policy", type=Path, default=Path(os.environ["AFS_ACCEPTANCE_LTP_TIMEOUT_POLICY"]) if os.environ.get("AFS_ACCEPTANCE_LTP_TIMEOUT_POLICY") else None, help="Optional pre-run schema-1 per-test timeout policy bound to the selected LTP suite identity. Defaults remain unchanged when omitted.")
    return parser.parse_args(argv)


def proof_status_from_accounting(accounting: dict[str, Any], command_summary: dict[str, Any], profile: str, max_tests: int | None, applicability: dict[str, Any] | None = None) -> tuple[str, str]:
    counts = command_summary["counts"]
    if max_tests is not None and profile == "full":
        return "BLOCKED", "full LTP run was capped by --max-tests; incomplete accounting preserved"
    if accounting["executed"] == 0:
        return "BLOCKED", "no LTP commands were selected"
    if counts["TIMEOUT"] > 0:
        return "BLOCKED", "one or more LTP commands timed out"
    if counts["FAIL"] > 0:
        return "FAIL", "LTP reported FAIL; raw command evidence was preserved"
    if accounting["incomplete"] > 0 and profile == "full":
        return "BLOCKED", "full LTP run did not execute every frozen command"
    raw_nonpass = counts["TBROK"] > 0 or counts["TCONF"] > 0
    if applicability and applicability.get("enabled"):
        if raw_nonpass and applicability.get("status") == "PASS":
            return "PASS", "all raw TCONF/TBROK events were pre-reviewed by the applicability manifest; raw command results were preserved"
        if applicability.get("status") != "PASS":
            return "BLOCKED", "applicability manifest did not match the selected run exactly"
    if counts["TBROK"] > 0:
        return "FAIL", "LTP reported TBROK without a pre-reviewed applicability manifest; raw command evidence was preserved"
    if counts["TCONF"] > 0:
        return "BLOCKED", "LTP reported TCONF without a pre-reviewed applicability manifest"
    if counts["PASS"] == accounting["executed"]:
        return "PASS", ""
    return "INCONCLUSIVE", "LTP results did not fit PASS/FAIL/BLOCKED classification"


def main(argv: list[str]) -> int:
    args = parse_args(argv)
    run_dir = args.run_dir.resolve()
    artifacts = run_dir / "artifacts" / "std-02-ltp"
    artifacts.mkdir(parents=True, exist_ok=True)
    matrix = json.loads(args.matrix_json) if args.matrix_json.strip() else {}
    if not isinstance(matrix, dict):
        matrix = {}
    matrix.setdefault("reference", "ext4")
    matrix.setdefault("suite", f"LTP {LTP_TAG}")

    checks: list[dict[str, Any]] = []
    status = "PASS"
    reason = ""
    accounting: dict[str, Any] = {}
    command_summary: dict[str, Any] = {"counts": {value: 0 for value in sorted(RESULT_VALUES)}}
    applicability_manifest: dict[str, Any] | None = None
    applicability: dict[str, Any] | None = None
    timeout_policy: dict[str, Any] | None = None
    fixture: Path | None = None
    fixture_kept: bool | None = None
    identity: dict[str, Any] = {}

    try:
        if args.case_id != "STD-02":
            raise RuntimeError(f"ltp.py implements STD-02 only, got {args.case_id}")
        if args.mount is None:
            raise RuntimeError("--mount or AFS_ACCEPTANCE_MOUNT is required")

        base_dir = resolve_base_dir(args.mount, args.base_dir)
        ltp = ltp_identity(args.suite_root, args.ltp_install, args.expanded_tsv)
        commands = read_commands(args.expanded_tsv)
        selected, selection = select_commands(commands, args.profile, args.max_tests)
        mnt = mount_identity(args.mount)
        base_mnt = mount_identity(base_dir)
        proc = process_identity(args.process_pid)
        meta_proc = process_identity(args.meta_process_pid)
        product_identity = {
            "backend": args.backend or matrix.get("backend"),
            "meta": args.meta or matrix.get("meta"),
            "transport": matrix.get("transport"),
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
            "suite": ltp,
            "mount": mnt,
            "base_mount": base_mnt,
            "process": proc,
            "meta_process": meta_proc,
            "product": product_identity,
            "selection": selection,
            "lock_state_note": "acceptance.lock.json may remain PREPARING; driver readiness is not a release PASS.",
        }
        write_json(artifacts / "identity.json", identity)
        write_json(artifacts / "discovery.json", {"total_commands": len(commands), "selection": selection, "selected": selected})
        if args.timeout_policy is not None:
            timeout_policy = load_timeout_policy(args.timeout_policy, identity, selected, args.per_test_timeout)
            write_json(artifacts / "timeout-policy.normalized.json", timeout_policy)
        if args.applicability_manifest is not None:
            applicability_manifest = load_applicability_manifest(args.applicability_manifest, identity)
            write_json(artifacts / "applicability-manifest.normalized.json", applicability_manifest)

        suite_ok = (ltp["git_head"] == LTP_REV or args.allow_unpinned_suite_fixture) and ltp["kirk_exists"]
        checks.append(build_check("pinned-suite-identity", "PASS" if suite_ok else "FAIL", {"expected": LTP_REV, "observed": ltp.get("git_head"), "kirk_exists": ltp.get("kirk_exists"), "allow_unpinned_suite_fixture": args.allow_unpinned_suite_fixture}, rel(artifacts / "identity.json", run_dir)))
        root_ok = os.geteuid() == 0 or args.allow_nonroot_fixture
        checks.append(build_check("root-harness", "PASS" if root_ok else "BLOCKED", {"euid": os.geteuid(), "allow_nonroot_fixture": args.allow_nonroot_fixture, "note": "real STD-02 runs as root; nonroot is allowed only for fake-kirk driver self-tests."}, rel(artifacts / "identity.json", run_dir)))
        mount_ok = mnt.get("returncode") == 0 and bool(mnt.get("stdout", "").strip())
        base_dir_ok = base_dir.is_dir() and is_under(base_dir, args.mount)
        checks.append(build_check("mount-identity", "PASS" if mount_ok else "BLOCKED", {"mount": str(args.mount), "findmnt_returncode": mnt.get("returncode")}, rel(artifacts / "identity.json", run_dir)))
        checks.append(build_check("base-dir-scope", "PASS" if base_dir_ok else "BLOCKED", {"mount": str(args.mount), "base_dir": str(base_dir), "exists": base_dir.exists(), "is_dir": base_dir.is_dir(), "under_mount": is_under(base_dir, args.mount)}, rel(artifacts / "identity.json", run_dir)))
        target_result = target_checks(identity["platform"]["system"], product_identity["backend"], mnt, base_mnt, proc, meta_proc)
        target_ok = bool(target_result) and all(target_result.values())
        checks.append(build_check("target-identity", "PASS" if target_ok else "BLOCKED", {"backend": product_identity["backend"], "target_checks": target_result, "note": "Reference runs require observed ext4. Product runs require observed afs-ownerfs/afs-dfs FUSE plus live afs-node and afs-meta identities."}, rel(artifacts / "identity.json", run_dir)))
        discovery_ok = len(commands) == 657 or args.allow_unpinned_suite_fixture
        checks.append(build_check("frozen-command-discovery", "PASS" if discovery_ok else "BLOCKED", {"expected": 657, "observed": len(commands), "selected": len(selected), "selection": selection}, rel(artifacts / "discovery.json", run_dir)))

        if not suite_ok or not root_ok or not mount_ok or not base_dir_ok or not target_ok or not discovery_ok:
            status = "BLOCKED" if not (mount_ok and base_dir_ok and target_ok and discovery_ok and root_ok) else "FAIL"
            reason = "LTP preflight failed"
        else:
            fixture = base_dir / short_fixture_name()
            fixture.mkdir(mode=0o755)
            os.chmod(fixture, 0o755)
            command_records, command_summary = run_ltp_commands(selected, fixture, args.ltp_install, artifacts, args.per_test_timeout, timeout_policy)
            write_json(artifacts / "commands.json", command_records)
            applicability = apply_applicability(command_records, applicability_manifest, identity)
            if applicability_manifest is not None:
                write_json(artifacts / "applicability.json", applicability)
            selector_counts: dict[str, int] = {}
            for record in commands:
                selector_counts[record["selector"]] = selector_counts.get(record["selector"], 0) + 1
            accounting = {
                "profile": args.profile,
                "discovered": len(commands),
                "selected": len(selected),
                "executed": len(command_records),
                "incomplete": max(0, len(commands) - len(command_records)),
                "pre_reviewed_excluded": 0,
                "pre_reviewed_events": applicability.get("pre_reviewed_event_count", 0) if applicability else 0,
                "selector_counts": selector_counts,
                "result_counts": command_summary["counts"],
                "duration_seconds": command_summary["duration_seconds"],
                "no_post_failure_filtering": True,
                "raw_output_per_command": True,
            }
            write_json(artifacts / "accounting.json", accounting)
            checks.append(build_check("command-accounting", "PASS", accounting, rel(artifacts / "accounting.json", run_dir)))
            checks.append(build_check("timeout-policy", "PASS", timeout_policy or {"enabled": False, "default": command_timeouts("__default__", args.per_test_timeout, None)}, rel(artifacts / "timeout-policy.normalized.json", run_dir) if timeout_policy else None))
            all_commands_executed = accounting["executed"] == accounting["selected"]
            checks.append(build_check("complete-selected-execution", "PASS" if all_commands_executed else "BLOCKED", {"selected": accounting["selected"], "executed": accounting["executed"]}, rel(artifacts / "commands.json", run_dir)))
            result_check_status = "PASS" if command_summary["counts"].get("PASS", 0) == accounting["executed"] and accounting["executed"] > 0 else "FAIL"
            if command_summary["counts"].get("TIMEOUT", 0) > 0 or command_summary["counts"].get("TCONF", 0) > 0:
                result_check_status = "BLOCKED"
            if applicability_manifest is not None and command_summary["counts"].get("FAIL", 0) == 0 and command_summary["counts"].get("TIMEOUT", 0) == 0 and applicability and applicability.get("status") == "PASS":
                result_check_status = "PASS"
            checks.append(build_check("ltp-results", result_check_status, {"counts": command_summary["counts"], "applicability": {"enabled": bool(applicability_manifest), "status": applicability.get("status") if applicability else "NOT_APPLIED", "pre_reviewed_event_count": applicability.get("pre_reviewed_event_count", 0) if applicability else 0, "unmatched_event_count": applicability.get("unmatched_event_count", 0) if applicability else 0}, "note": "Raw TCONF/TBROK remain counted. They can be effective PASS only through a pre-run event-level applicability manifest."}, rel(artifacts / "commands.json", run_dir)))
            if applicability_manifest is not None:
                checks.append(build_check("ltp-applicability", "PASS" if applicability and applicability.get("status") == "PASS" else "BLOCKED", applicability, rel(artifacts / "applicability.json", run_dir)))
            status, reason = proof_status_from_accounting(accounting, command_summary, args.profile, args.max_tests, applicability)
            if status == "PASS" and fixture.exists():
                try:
                    shutil.rmtree(fixture)
                    fixture_kept = False
                    checks.append(build_check("cleanup-fixture", "PASS", {"fixture": str(fixture), "kept": False}))
                except Exception as cleanup_exc:  # noqa: BLE001
                    fixture_kept = True
                    cleanup_artifact = artifacts / "cleanup-error.json"
                    write_json(cleanup_artifact, {"fixture": str(fixture), "exception": type(cleanup_exc).__name__, "message": str(cleanup_exc), "traceback": traceback.format_exc()})
                    checks.append(build_check("cleanup-fixture", "FAIL", {"fixture": str(fixture), "kept": True, "error": f"{type(cleanup_exc).__name__}: {cleanup_exc}"}, rel(cleanup_artifact, run_dir)))
                    status = "FAIL"
                    reason = "LTP passed but fixture cleanup failed; accounting was preserved"
            else:
                fixture_kept = bool(fixture and fixture.exists())
    except Exception as exc:  # noqa: BLE001
        status = "BLOCKED"
        reason = f"driver setup failed: {type(exc).__name__}: {exc}"
        write_json(artifacts / "setup-error.json", {"exception": type(exc).__name__, "message": str(exc), "traceback": traceback.format_exc()})
        checks.append(build_check("driver-setup", "BLOCKED", reason, rel(artifacts / "setup-error.json", run_dir)))

    coverage_axes = {
        "reference": {"values": [str(matrix.get("reference", "ext4"))], "checks": {str(matrix.get("reference", "ext4")): "target-identity"}},
        "suite": {"values": [str(matrix.get("suite", f"LTP {LTP_TAG}"))], "checks": {str(matrix.get("suite", f"LTP {LTP_TAG}")): "pinned-suite-identity"}},
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
        "identity": {"artifact": rel(artifacts / "identity.json", run_dir), "product": identity.get("product") if identity else None},
        "accounting": accounting,
        "command_summary": command_summary,
        "applicability": applicability or {"enabled": False, "status": "NOT_APPLIED"},
        "timeout_policy": timeout_policy or {"enabled": False},
        "notes": [
            "STD-02 driver READY means the fixed LTP filesystem subset can run and report proof; it is not a release acceptance PASS by itself.",
            "No tests are filtered after execution. TCONF/TBROK/TIMEOUT are counted and block or fail according to the contract.",
        ],
    }
    write_json(artifacts / "proof.json", proof)
    print(json.dumps(proof, sort_keys=True))
    return 0 if status == "PASS" else 1


if __name__ == "__main__":
    raise SystemExit(main(sys.argv[1:]))
