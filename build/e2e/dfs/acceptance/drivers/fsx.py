#!/usr/bin/env python3
"""FSx acceptance driver for STD-03.

Runs the pinned secfs.test ``tools/bin/fsx`` binary against a caller supplied
mount directory, preserves per-seed raw logs and command accounting, and emits
the acceptance runner proof JSON as the final stdout object.
"""
from __future__ import annotations

import argparse
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

from target_identity import target_checks

SECFS_REV = "edf5eb4a108bfb41073f765aef0cdd32bb3ee1ed"
DEFAULT_SUITE_ROOT = Path("/mnt/lima-afsctlstate/afs-acceptance/suites-reference/src/secfs.test")
DEFAULT_FSX_BINARY = DEFAULT_SUITE_ROOT / "tools" / "bin" / "fsx"
FULL_SEEDS = [1, 2, 3]
FULL_DURATION_SECONDS = 900
SMOKE_SEEDS = [1]
SMOKE_OPERATIONS = 1000
RESULT_VALUES = {"PASS", "FAIL", "TIMEOUT", "BLOCKED", "NOT_RUN"}


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


def fsx_identity(suite_root: Path, fsx_binary: Path) -> dict[str, Any]:
    root = suite_root.resolve()
    git = ["git", "-c", f"safe.directory={root}", "-C", str(root)]
    head = run_text(git + ["rev-parse", "HEAD"])
    status = run_text(git + ["status", "--porcelain"])
    help_output = run_text([str(fsx_binary), "-h"]) if fsx_binary.exists() else {"returncode": None, "stdout": "", "stderr": "missing fsx"}
    source = suite_root / "fstools" / "src" / "fsx" / "fsx.c"
    return {
        "suite_root": str(suite_root),
        "expected_revision": SECFS_REV,
        "git_head": head.get("stdout", "").strip(),
        "git_head_command": head,
        "git_status_porcelain": status.get("stdout", ""),
        "git_status_command": status,
        "fsx_binary": str(fsx_binary),
        "fsx_binary_exists": fsx_binary.is_file() and os.access(fsx_binary, os.X_OK),
        "fsx_binary_sha256": sha256_file(fsx_binary) if fsx_binary.is_file() else None,
        "fsx_source": str(source),
        "fsx_source_sha256": sha256_file(source) if source.is_file() else None,
        "fsx_help": help_output,
    }


def parse_seed_list(value: str | None, default: list[int]) -> list[int]:
    if value is None or not value.strip():
        return list(default)
    seeds: list[int] = []
    for part in value.split(","):
        part = part.strip()
        if not part:
            continue
        seed = int(part, 10)
        if seed < 0:
            raise ValueError("seed must be non-negative")
        seeds.append(seed)
    if not seeds:
        raise ValueError("at least one seed is required")
    return seeds


def duration_arg(seconds: int) -> str:
    return f"{seconds}s"


def duration_timeout(seconds: int, override: int | None) -> int:
    if override is not None:
        return override
    return max(seconds + 30, int(seconds * 1.2) + 10)


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
            err.write(f"fsx driver timed out after {timeout}s\n".encode())
    return {
        "argv": argv,
        "cwd": str(cwd),
        "returncode": returncode,
        "timed_out": timed_out,
        "timeout_seconds": timeout,
        "duration_seconds": round(time.time() - started, 3),
    }


def classify_seed(command_result: dict[str, Any], stdout_path: Path, stderr_path: Path) -> dict[str, Any]:
    stdout = stdout_path.read_text(errors="replace") if stdout_path.exists() else ""
    stderr = stderr_path.read_text(errors="replace") if stderr_path.exists() else ""
    combined = stdout + "\n" + stderr
    operation_match = re.search(r"All operations\s+-\s+(\d+)\s+-\s+completed A-OK!", combined)
    if command_result.get("timed_out"):
        result = "TIMEOUT"
        reason = "process exceeded driver timeout and was terminated"
    elif command_result.get("returncode") != 0:
        result = "FAIL"
        reason = f"fsx exited {command_result.get('returncode')}"
    elif not operation_match:
        result = "FAIL"
        reason = "fsx exited 0 without completion marker"
    else:
        result = "PASS"
        reason = "fsx completed with A-OK marker"
    return {
        "result": result,
        "reason": reason,
        "operations_completed": int(operation_match.group(1)) if operation_match else None,
        "stdout_bytes": len(stdout.encode()),
        "stderr_bytes": len(stderr.encode()),
        "completion_marker": bool(operation_match),
    }




def parse_failure_operation(stdout_path: Path, stderr_path: Path) -> int | None:
    """Best-effort extraction of the operation number associated with an FSx failure."""
    text = ""
    for path in (stdout_path, stderr_path):
        if path.exists():
            text += "\n" + path.read_text(errors="replace")
    patterns = [
        r"(?:operation|op|opnum)\D{0,40}(\d+)",
        r"(?:testcalls|calls)\D{0,40}(\d+)",
        r"All operations\s+-\s+(\d+)\s+-",
        r"\bop\s*#?\s*(\d+)\b",
    ]
    candidates: list[int] = []
    for pattern in patterns:
        for match in re.finditer(pattern, text, flags=re.IGNORECASE):
            try:
                value = int(match.group(1), 10)
            except ValueError:
                continue
            if value > 0:
                candidates.append(value)
    return min(candidates) if candidates else None


def replace_or_append_option(command: list[str], option: str, value: str) -> list[str]:
    updated = list(command)
    if option in updated:
        idx = updated.index(option)
        if idx + 1 < len(updated):
            updated[idx + 1] = value
        else:
            updated.append(value)
        return updated
    # Insert before the final positional file name.
    if updated:
        return updated[:-1] + [option, value] + updated[-1:]
    return [option, value]


def remove_option_with_value(command: list[str], option: str) -> list[str]:
    updated: list[str] = []
    idx = 0
    while idx < len(command):
        if command[idx] == option:
            idx += 2
            continue
        updated.append(command[idx])
        idx += 1
    return updated


def build_prefix_replay_command(original_command: list[str], prefix_ops: int, artifacts_dir: Path, test_file: Path) -> list[str]:
    command = remove_option_with_value(original_command, "-d")
    command = replace_or_append_option(command, "-N", str(prefix_ops))
    command = replace_or_append_option(command, "-P", str(artifacts_dir))
    if command:
        command[-1] = test_file.name
    return command


def command_sha256(command: list[str]) -> str:
    return hashlib.sha256(json.dumps(command, sort_keys=False, separators=(",", ":")).encode()).hexdigest()




def read_combined_output(stdout_path: Path, stderr_path: Path) -> str:
    text = ""
    for path in (stdout_path, stderr_path):
        if path.exists():
            text += "\n" + path.read_text(errors="replace")
    return text


def failure_fingerprint(command_result: dict[str, Any], stdout_path: Path, stderr_path: Path) -> dict[str, Any]:
    text = read_combined_output(stdout_path, stderr_path)
    lines = [line.strip() for line in text.splitlines() if line.strip()]
    signature_line = ""
    for line in lines:
        lower = line.lower()
        if "using file" in lower or "seed set" in lower or "all operations" in lower:
            continue
        signature_line = re.sub(r"\s+", " ", line)[:240]
        break
    return {
        "returncode": command_result.get("returncode"),
        "timed_out": bool(command_result.get("timed_out")),
        "signature_line": signature_line,
        "signature_sha256": hashlib.sha256(signature_line.encode()).hexdigest() if signature_line else None,
    }


def fingerprints_match(original: dict[str, Any], replay: dict[str, Any]) -> bool:
    if original.get("timed_out") or replay.get("timed_out"):
        return original.get("timed_out") == replay.get("timed_out")
    return original.get("returncode") == replay.get("returncode") and bool(original.get("signature_sha256")) and original.get("signature_sha256") == replay.get("signature_sha256")


def attempt_for_result(result: dict[str, Any], artifacts: Path) -> dict[str, Any]:
    converted = dict(result)
    converted["artifacts"] = {k: rel(Path(v), artifacts) for k, v in result["artifacts"].items()}
    return converted

def run_prefix_replay_attempt(
    original_command: list[str],
    seed: int,
    prefix_ops: int,
    attempt_root: Path,
    tested_fixture_parent: Path,
    timeout: int,
    original_fingerprint: dict[str, Any],
) -> dict[str, Any]:
    artifacts_dir = attempt_root / "artifacts"
    fixture = tested_fixture_parent / f"prefix-replay-seed-{seed}-N{prefix_ops}-{uuid.uuid4().hex[:8]}"
    artifacts_dir.mkdir(parents=True, exist_ok=True)
    fixture.mkdir(parents=True, exist_ok=False)
    test_file = fixture / "fsxfile"
    stdout = artifacts_dir / "stdout.log"
    stderr = artifacts_dir / "stderr.log"
    command = build_prefix_replay_command(original_command, prefix_ops, artifacts_dir, test_file)
    findmnt_before = mount_identity(fixture)
    result = run_bounded(command, cwd=fixture, timeout=timeout, stdout_path=stdout, stderr_path=stderr)
    classification = classify_seed(result, stdout, stderr)
    replay_fingerprint = failure_fingerprint(result, stdout, stderr) if classification["result"] == "FAIL" else None
    cleanup: dict[str, Any] = {"attempted": True, "removed": False, "path": str(fixture)}
    try:
        shutil.rmtree(fixture)
        cleanup["removed"] = not fixture.exists()
    except Exception as exc:  # noqa: BLE001 - evidence preserves cleanup failure
        cleanup.update({"exception": type(exc).__name__, "message": str(exc)})
    record = {
        "seed": seed,
        "prefix_operations": prefix_ops,
        "command": command,
        "command_sha256": command_sha256(command),
        "fixture": str(fixture),
        "tested_fixture_parent": str(tested_fixture_parent),
        "findmnt": findmnt_before,
        "artifacts": {"stdout": str(stdout), "stderr": str(stderr)},
        "process": result,
        "classification": classification,
        "failure_fingerprint": replay_fingerprint,
        "matches_original_failure": fingerprints_match(original_fingerprint, replay_fingerprint or {}) if classification["result"] == "FAIL" else False,
        "cleanup": cleanup,
    }
    write_json(attempt_root / "attempt.json", record)
    return record

def minimize_failed_seed_prefix(
    seed_record: dict[str, Any],
    artifacts: Path,
    max_attempts: int,
    timeout_seconds: int,
) -> dict[str, Any]:
    """Find a deterministic minimal failing prefix, if FSx can reproduce one.

    This is intentionally prefix-only. It does not do arbitrary ddmin over the
    operation log, so the result is labelled as a minimal failing prefix rather
    than a minimal failure-inducing operation set.
    """
    seed = int(seed_record["seed"])
    seed_artifacts = artifacts / "seeds" / f"{int(seed_record['index']):02d}-seed-{seed}"
    stdout = artifacts / seed_record["artifacts"]["stdout"]
    stderr = artifacts / seed_record["artifacts"]["stderr"]
    original_fingerprint = failure_fingerprint(seed_record["process"], stdout, stderr)
    tested_fixture = Path(seed_record["fixture"])
    tested_fixture_parent = tested_fixture.parent
    original = {
        "seed": seed,
        "command": seed_record["command"],
        "command_sha256": command_sha256(seed_record["command"]),
        "stdout": seed_record["artifacts"]["stdout"],
        "stderr": seed_record["artifacts"]["stderr"],
        "stdout_sha256": sha256_file(stdout) if stdout.exists() else None,
        "stderr_sha256": sha256_file(stderr) if stderr.exists() else None,
        "classification": seed_record["classification"],
        "failure_fingerprint": original_fingerprint,
        "tested_fixture": str(tested_fixture),
        "tested_fixture_parent": str(tested_fixture_parent),
        "tested_fixture_parent_findmnt": mount_identity(tested_fixture_parent),
    }
    upper = parse_failure_operation(stdout, stderr) or seed_record["classification"].get("operations_completed")
    if upper is None:
        for idx, item in enumerate(seed_record["command"]):
            if item == "-N" and idx + 1 < len(seed_record["command"]):
                try:
                    upper = int(seed_record["command"][idx + 1], 10)
                except ValueError:
                    upper = None
                break
    result: dict[str, Any] = {
        "kind": "prefix-replay",
        "status": "INCONCLUSIVE",
        "reason": "no bounded operation count was available for replay",
        "original_failure": original,
        "attempts": [],
        "max_attempts": max_attempts,
        "timeout_seconds": timeout_seconds,
        "label": "minimal failing prefix when status is REPRODUCED; not arbitrary ddmin",
    }
    if upper is None or upper <= 0:
        write_json(seed_artifacts / "minimization.json", result)
        return result

    replay_root = seed_artifacts / "prefix-replay"
    high_attempt = run_prefix_replay_attempt(seed_record["command"], seed, int(upper), replay_root / f"attempt-001-N{int(upper)}", tested_fixture_parent, timeout_seconds, original_fingerprint)
    result["attempts"].append(attempt_for_result(high_attempt, artifacts))
    if high_attempt["classification"]["result"] not in {"PASS", "FAIL"}:
        result.update({
            "status": "INCONCLUSIVE",
            "reason": "prefix replay hit a non PASS/FAIL result at the upper bound; original case remains FAIL",
            "candidate_upper_bound": int(upper),
        })
        write_json(seed_artifacts / "minimization.json", result)
        return result
    if high_attempt["classification"]["result"] != "FAIL" or not high_attempt.get("matches_original_failure"):
        result.update({
            "status": "INCONCLUSIVE",
            "reason": "original failure did not reproduce with the same failure fingerprint at the parsed prefix bound; original case remains FAIL",
            "candidate_upper_bound": int(upper),
        })
        write_json(seed_artifacts / "minimization.json", result)
        return result

    low = 1
    high = int(upper)
    attempts_used = 1
    while low < high and attempts_used < max_attempts:
        mid = (low + high) // 2
        attempts_used += 1
        attempt = run_prefix_replay_attempt(seed_record["command"], seed, mid, replay_root / f"attempt-{attempts_used:03d}-N{mid}", tested_fixture_parent, timeout_seconds, original_fingerprint)
        result["attempts"].append(attempt_for_result(attempt, artifacts))
        attempt_result = attempt["classification"]["result"]
        if attempt_result not in {"PASS", "FAIL"}:
            result.update({
                "status": "INCONCLUSIVE",
                "reason": "prefix replay produced a non PASS/FAIL result during search; original case remains FAIL",
                "inconclusive_prefix_operations": mid,
                "inconclusive_result": attempt_result,
                "attempts_used": attempts_used,
            })
            write_json(seed_artifacts / "minimization.json", result)
            return result
        if attempt_result == "FAIL" and attempt.get("matches_original_failure"):
            high = mid
        elif attempt_result == "FAIL":
            result.update({
                "status": "INCONCLUSIVE",
                "reason": "prefix replay failed with a different fingerprint during search; original case remains FAIL",
                "inconclusive_prefix_operations": mid,
                "attempts_used": attempts_used,
            })
            write_json(seed_artifacts / "minimization.json", result)
            return result
        else:
            low = mid + 1

    if low == high:
        final_prefix = low
        if result["attempts"][-1]["prefix_operations"] != final_prefix:
            if attempts_used < max_attempts:
                attempts_used += 1
                final_attempt = run_prefix_replay_attempt(seed_record["command"], seed, final_prefix, replay_root / f"attempt-{attempts_used:03d}-N{final_prefix}", tested_fixture_parent, timeout_seconds, original_fingerprint)
                result["attempts"].append(attempt_for_result(final_attempt, artifacts))
        status = "REPRODUCED" if any(a["prefix_operations"] == final_prefix and a["classification"]["result"] == "FAIL" and a.get("matches_original_failure") for a in result["attempts"]) else "INCONCLUSIVE"
        result.update({
            "status": status,
            "reason": "minimal failing prefix found" if status == "REPRODUCED" else "attempt budget ended before final prefix confirmation; original case remains FAIL",
            "minimal_failing_prefix_operations": final_prefix if status == "REPRODUCED" else None,
            "attempts_used": attempts_used,
        })
    else:
        result.update({
            "status": "INCONCLUSIVE",
            "reason": "attempt budget exhausted during prefix search; original case remains FAIL",
            "search_low": low,
            "search_high": high,
            "attempts_used": attempts_used,
        })
    write_json(seed_artifacts / "minimization.json", result)
    return result


def minimize_failed_seeds(
    seed_records: list[dict[str, Any]],
    artifacts: Path,
    max_attempts: int,
    timeout_seconds: int,
) -> list[dict[str, Any]]:
    minimizations: list[dict[str, Any]] = []
    for record in seed_records:
        if record.get("classification", {}).get("result") == "FAIL":
            minimizations.append(minimize_failed_seed_prefix(record, artifacts, max_attempts, timeout_seconds))
    write_json(artifacts / "failure-minimization.json", minimizations)
    return minimizations

def preflight_fixture(base_dir: Path, artifacts: Path) -> dict[str, Any]:
    probe = base_dir / f".afs-fsx-write-probe-{uuid.uuid4().hex[:8]}"
    record: dict[str, Any] = {"path": str(probe), "created": False, "removed": False}
    try:
        probe.write_bytes(b"afs-fsx-probe")
        record["created"] = probe.exists()
        record["readback_ok"] = probe.read_bytes() == b"afs-fsx-probe"
        probe.unlink()
        record["removed"] = not probe.exists()
        record["writable"] = bool(record["created"] and record["readback_ok"] and record["removed"])
    except Exception as exc:  # noqa: BLE001
        record.update({"writable": False, "exception": type(exc).__name__, "message": str(exc)})
        try:
            if probe.exists():
                probe.unlink()
        except OSError:
            pass
    write_json(artifacts / "fixture-preflight.json", record)
    return record


def build_fsx_command(
    fsx_binary: Path,
    profile: str,
    duration_seconds: int,
    operations: int | None,
    seed: int,
    seed_artifacts: Path,
    test_file: Path,
) -> list[str]:
    command = [str(fsx_binary)]
    if profile == "smoke":
        if operations is None:
            raise ValueError("smoke FSx requires an operation count")
        command.extend(["-N", str(operations)])
    else:
        command.extend(["-d", duration_arg(duration_seconds)])
    command.extend(["-S", str(seed), "-P", str(seed_artifacts), test_file.name])
    return command


def run_fsx_seeds(
    fsx_binary: Path,
    profile: str,
    fixture: Path,
    artifacts: Path,
    seeds: list[int],
    duration_seconds: int,
    operations: int | None,
    per_seed_timeout: int | None,
) -> tuple[list[dict[str, Any]], dict[str, Any]]:
    records: list[dict[str, Any]] = []
    counts = {value: 0 for value in sorted(RESULT_VALUES)}
    total_duration = 0.0
    for index, seed in enumerate(seeds, start=1):
        seed_name = f"seed-{seed}"
        seed_fixture = fixture / seed_name
        seed_artifacts = artifacts / "seeds" / f"{index:02d}-{seed_name}"
        seed_fixture.mkdir(parents=True, exist_ok=True)
        seed_artifacts.mkdir(parents=True, exist_ok=True)
        test_file = seed_fixture / "fsxfile"
        stdout = seed_artifacts / "stdout.log"
        stderr = seed_artifacts / "stderr.log"
        timeout = duration_timeout(duration_seconds, per_seed_timeout) if profile == "full" else (per_seed_timeout or 60)
        command = build_fsx_command(fsx_binary, profile, duration_seconds, operations, seed, seed_artifacts, test_file)
        command_result = run_bounded(command, cwd=seed_fixture, timeout=timeout, stdout_path=stdout, stderr_path=stderr)
        classification = classify_seed(command_result, stdout, stderr)
        counts[classification["result"]] += 1
        total_duration += float(command_result.get("duration_seconds", 0.0))
        record = {
            "index": index,
            "seed": seed,
            "profile": profile,
            "command": command,
            "fixture": str(seed_fixture),
            "test_file": str(test_file),
            "artifacts": {"stdout": rel(stdout, artifacts), "stderr": rel(stderr, artifacts)},
            "process": command_result,
            "classification": classification,
        }
        write_json(seed_artifacts / "command.json", record)
        records.append(record)
    summary = {"counts": counts, "duration_seconds": round(total_duration, 3), "executed": len(records)}
    return records, summary


def proof_status_from_accounting(accounting: dict[str, Any], profile: str, cap_applied: bool) -> tuple[str, str]:
    counts = accounting["result_counts"]
    if counts.get("TIMEOUT", 0) > 0:
        return "BLOCKED", "one or more FSx seeds hit the driver timeout before intentional duration completion"
    if counts.get("FAIL", 0) > 0:
        return "FAIL", "FSx reported failure or missing completion marker; raw logs were preserved"
    if counts.get("BLOCKED", 0) > 0:
        return "BLOCKED", "one or more FSx seeds were blocked before execution"
    if profile == "full" and cap_applied:
        return "BLOCKED", "full FSx run was capped and did not execute the fixed 900s x 3 seed contract"
    if profile == "full" and (accounting["duration_seconds_per_seed"] < FULL_DURATION_SECONDS or accounting["selected_seeds"] != FULL_SEEDS):
        return "BLOCKED", "full FSx run did not match the fixed 900s x 3 seed contract"
    if counts.get("PASS", 0) == accounting["executed"] and accounting["executed"] > 0:
        return "PASS", ""
    return "INCONCLUSIVE", "FSx results did not fit PASS/FAIL/BLOCKED classification"


def parse_args(argv: list[str]) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Run AFS STD-03 secfs.test FSx driver")
    parser.add_argument("--case-id", default=os.environ.get("AFS_ACCEPTANCE_CASE_ID", "STD-03"))
    parser.add_argument("--profile", choices=["smoke", "full"], default=os.environ.get("AFS_ACCEPTANCE_PROFILE", "smoke"))
    parser.add_argument("--matrix-json", default=os.environ.get("AFS_ACCEPTANCE_MATRIX", "{}"))
    parser.add_argument("--run-dir", type=Path, default=Path(os.environ.get("AFS_ACCEPTANCE_RUN_DIR", "results/std-03-driver")))
    parser.add_argument("--mount", type=Path, default=Path(os.environ["AFS_ACCEPTANCE_MOUNT"]) if os.environ.get("AFS_ACCEPTANCE_MOUNT") else None)
    parser.add_argument("--base-dir", type=Path, default=Path(os.environ["AFS_ACCEPTANCE_BASE_DIR"]) if os.environ.get("AFS_ACCEPTANCE_BASE_DIR") else None, help="Directory under --mount that contains the temporary FSx fixture. Relative paths are resolved below --mount.")
    parser.add_argument("--suite-root", type=Path, default=DEFAULT_SUITE_ROOT)
    parser.add_argument("--fsx-binary", type=Path, default=DEFAULT_FSX_BINARY)
    parser.add_argument("--duration-seconds", type=int, default=None, help="Override per-seed FSx -d duration for smoke/full driver testing. Full overrides below 900s block release PASS.")
    parser.add_argument("--operations", type=int, default=SMOKE_OPERATIONS, help="Smoke profile FSx -N operation count")
    parser.add_argument("--seeds", default=None, help="Comma-separated seed list. Full overrides different from 1,2,3 block release PASS.")
    parser.add_argument("--max-seeds", type=int, default=None, help="Cap selected seeds for bounded driver tests. Full caps block release PASS.")
    parser.add_argument("--per-seed-timeout", type=int, default=None, help="Per-seed process timeout in seconds")
    parser.add_argument("--failure-replay-attempts", type=int, default=12, help="Maximum attempts per failed seed for deterministic prefix replay minimization")
    parser.add_argument("--failure-replay-timeout", type=int, default=30, help="Timeout in seconds for each prefix replay attempt")
    parser.add_argument("--skip-failure-replay", action="store_true", help="Preserve original failure only; mark minimization as not attempted")
    parser.add_argument("--process-pid", default=os.environ.get("AFS_ACCEPTANCE_PROCESS_PID"))
    parser.add_argument("--meta-process-pid", default=os.environ.get("AFS_ACCEPTANCE_META_PROCESS_PID"))
    parser.add_argument("--backend", default=os.environ.get("AFS_ACCEPTANCE_BACKEND"), help="Observed product backend label, for proof identity only.")
    parser.add_argument("--meta", default=os.environ.get("AFS_ACCEPTANCE_META"), help="Observed Meta backend label, for proof identity only.")
    parser.add_argument("--allow-unpinned-suite-fixture", action="store_true", help="Only for driver self-tests with fake secfs.test fixtures")
    return parser.parse_args(argv)


def main(argv: list[str]) -> int:
    args = parse_args(argv)
    run_dir = args.run_dir.resolve()
    artifacts = run_dir / "artifacts" / "std-03-fsx"
    artifacts.mkdir(parents=True, exist_ok=True)
    matrix = json.loads(args.matrix_json) if args.matrix_json.strip() else {}
    if not isinstance(matrix, dict):
        matrix = {}
    matrix.setdefault("reference", "ext4")
    matrix.setdefault("suite_sha", f"secfs.test {SECFS_REV}")
    matrix.setdefault("seeds", 3)

    checks: list[dict[str, Any]] = []
    status = "PASS"
    reason = ""
    accounting: dict[str, Any] = {}
    command_summary: dict[str, Any] = {"counts": {value: 0 for value in sorted(RESULT_VALUES)}}
    failure_minimization: list[dict[str, Any]] = []
    fixture: Path | None = None
    fixture_kept: bool | None = None
    identity: dict[str, Any] = {}

    try:
        if args.case_id != "STD-03":
            raise RuntimeError(f"fsx.py implements STD-03 only, got {args.case_id}")
        if args.mount is None:
            raise RuntimeError("--mount or AFS_ACCEPTANCE_MOUNT is required")

        base_dir = resolve_base_dir(args.mount, args.base_dir)
        expected_seeds = FULL_SEEDS if args.profile == "full" else SMOKE_SEEDS
        selected_seeds = parse_seed_list(args.seeds, expected_seeds)
        cap_applied = False
        if args.max_seeds is not None:
            selected_seeds = selected_seeds[: args.max_seeds]
            cap_applied = True
        duration_seconds = args.duration_seconds if args.duration_seconds is not None else (FULL_DURATION_SECONDS if args.profile == "full" else 0)
        operations = args.operations if args.profile == "smoke" else None

        fsx = fsx_identity(args.suite_root, args.fsx_binary)
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
            "suite": fsx,
            "mount": mnt,
            "base_mount": base_mnt,
            "process": proc,
            "meta_process": meta_proc,
            "product": product_identity,
            "selection": {"profile": args.profile, "full_seeds": FULL_SEEDS, "selected_seeds": selected_seeds, "duration_seconds_per_seed": duration_seconds, "smoke_operations": operations, "cap_applied": cap_applied},
            "lock_state_note": "acceptance.lock.json may remain PREPARING; driver readiness is not a release PASS.",
        }
        write_json(artifacts / "identity.json", identity)

        suite_ok = ((fsx["git_head"] == SECFS_REV) or args.allow_unpinned_suite_fixture) and fsx["fsx_binary_exists"]
        checks.append(build_check("pinned-suite-identity", "PASS" if suite_ok else "FAIL", {"expected": SECFS_REV, "observed": fsx.get("git_head"), "binary_exists": fsx.get("fsx_binary_exists"), "allow_unpinned_suite_fixture": args.allow_unpinned_suite_fixture}, rel(artifacts / "identity.json", run_dir)))
        help_ok = "-S seed" in str(fsx.get("fsx_help", {}).get("stdout", "") + fsx.get("fsx_help", {}).get("stderr", "")) and "-P dirpath" in str(fsx.get("fsx_help", {}).get("stdout", "") + fsx.get("fsx_help", {}).get("stderr", ""))
        checks.append(build_check("fsx-help-contract", "PASS" if help_ok else "BLOCKED", {"requires": ["-S seed", "-P dirpath", "-d duration", "-N numops"], "returncode": fsx.get("fsx_help", {}).get("returncode")}, rel(artifacts / "identity.json", run_dir)))
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
        full_contract_ok = args.profile != "full" or (selected_seeds == FULL_SEEDS and duration_seconds >= FULL_DURATION_SECONDS and not cap_applied)
        checks.append(build_check("fixed-seed-duration-contract", "PASS" if full_contract_ok else "BLOCKED", {"profile": args.profile, "full_seeds": FULL_SEEDS, "selected_seeds": selected_seeds, "required_duration_seconds": FULL_DURATION_SECONDS, "duration_seconds_per_seed": duration_seconds, "cap_applied": cap_applied}, rel(artifacts / "identity.json", run_dir)))

        if not suite_ok or not help_ok or not mount_ok or not base_dir_ok or not backend_ok or not target_ok:
            status = "BLOCKED" if not (help_ok and mount_ok and base_dir_ok and backend_ok and target_ok) else "FAIL"
            reason = "FSx preflight failed"
        else:
            fixture = base_dir / f".afs-std03-fsx-{dt.datetime.now(dt.timezone.utc).strftime('%Y%m%dT%H%M%SZ')}-{uuid.uuid4().hex[:8]}"
            fixture.mkdir(mode=0o755)
            os.chmod(fixture, 0o755)
            preflight = preflight_fixture(fixture, artifacts)
            checks.append(build_check("fixture-writable", "PASS" if preflight.get("writable") else "BLOCKED", preflight, rel(artifacts / "fixture-preflight.json", run_dir)))
            if not preflight.get("writable"):
                status = "BLOCKED"
                reason = "FSx fixture is not writable"
            else:
                command_records, command_summary = run_fsx_seeds(args.fsx_binary, args.profile, fixture, artifacts, selected_seeds, duration_seconds, operations, args.per_seed_timeout)
                write_json(artifacts / "commands.json", command_records)
                accounting = {
                    "profile": args.profile,
                    "fixed_full_seeds": FULL_SEEDS,
                    "selected_seeds": selected_seeds,
                    "selected_seed_count": len(selected_seeds),
                    "executed": len(command_records),
                    "required_full_seed_count": len(FULL_SEEDS),
                    "duration_seconds_per_seed": duration_seconds,
                    "smoke_operations": operations,
                    "cap_applied": cap_applied,
                    "result_counts": command_summary["counts"],
                    "duration_seconds": command_summary["duration_seconds"],
                    "raw_output_per_seed": True,
                    "raw_command_per_seed": True,
                    "no_post_failure_filtering": True,
                }
                write_json(artifacts / "accounting.json", accounting)
                if command_summary["counts"].get("FAIL", 0) > 0:
                    if args.skip_failure_replay:
                        failure_minimization = [{"status": "NOT_RUN", "reason": "--skip-failure-replay was set; original case remains FAIL"}]
                        write_json(artifacts / "failure-minimization.json", failure_minimization)
                    else:
                        failure_minimization = minimize_failed_seeds(command_records, artifacts, args.failure_replay_attempts, args.failure_replay_timeout)
                checks.append(build_check("command-accounting", "PASS", accounting, rel(artifacts / "accounting.json", run_dir)))
                complete_execution = accounting["executed"] == accounting["selected_seed_count"] and accounting["executed"] > 0
                checks.append(build_check("complete-selected-execution", "PASS" if complete_execution else "BLOCKED", {"selected": accounting["selected_seed_count"], "executed": accounting["executed"]}, rel(artifacts / "commands.json", run_dir)))
                result_check_status = "PASS" if command_summary["counts"].get("PASS", 0) == accounting["executed"] and accounting["executed"] > 0 else "FAIL"
                if command_summary["counts"].get("TIMEOUT", 0) > 0:
                    result_check_status = "BLOCKED"
                checks.append(build_check("fsx-results", result_check_status, {"counts": command_summary["counts"], "note": "TIMEOUT blocks because the process did not complete the intentional FSx duration; nonzero or missing A-OK marker fails."}, rel(artifacts / "commands.json", run_dir)))
                if command_summary["counts"].get("FAIL", 0) > 0:
                    reproduced = sum(1 for item in failure_minimization if item.get("status") == "REPRODUCED")
                    inconclusive = sum(1 for item in failure_minimization if item.get("status") == "INCONCLUSIVE")
                    checks.append(build_check("failure-prefix-replay-evidence", "PASS", {"reproduced": reproduced, "inconclusive": inconclusive, "total": len(failure_minimization), "note": "Prefix replay supplements raw FSx failure evidence. INCONCLUSIVE minimization is recorded inside failure-minimization.json and never hides the original FAIL."}, rel(artifacts / "failure-minimization.json", run_dir)))
                status, reason = proof_status_from_accounting(accounting, args.profile, cap_applied)
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
                        reason = "FSx passed but fixture cleanup failed; accounting was preserved"
                else:
                    fixture_kept = bool(fixture and fixture.exists())
    except Exception as exc:  # noqa: BLE001
        status = "BLOCKED"
        reason = f"driver setup failed: {type(exc).__name__}: {exc}"
        write_json(artifacts / "setup-error.json", {"exception": type(exc).__name__, "message": str(exc), "traceback": traceback.format_exc()})
        checks.append(build_check("driver-setup", "BLOCKED", reason, rel(artifacts / "setup-error.json", run_dir)))

    suite_sha_value = str(matrix.get("suite_sha", f"secfs.test {SECFS_REV}"))
    seeds_value = str(len(accounting.get("selected_seeds", [])))
    reference_value = str(matrix.get("reference", "ext4"))
    coverage_axes = {
        "reference": {"values": [reference_value], "checks": {reference_value: "mount-identity"}},
        "suite_sha": {"values": [suite_sha_value], "checks": {suite_sha_value: "pinned-suite-identity"}},
        "seeds": {"values": [seeds_value], "checks": {seeds_value: "fixed-seed-duration-contract"}},
    }
    # Profile-specific axes describe what ran, rather than copying a full-mode
    # seed count into a reduced smoke proof. Keep the legacy axis for callers.
    coverage_axes[f"seeds_{args.profile}"] = coverage_axes["seeds"]
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
        "failure_minimization": {"artifact": rel(artifacts / "failure-minimization.json", run_dir), "items": failure_minimization} if failure_minimization else None,
        "notes": [
            "STD-03 driver READY means the pinned FSx binary can run and report proof; it is not a release acceptance PASS by itself.",
            "Smoke uses a reduced fixed seed and operation count. Full requires exactly seeds 1,2,3 at >=900s each.",
            "No failures are filtered after execution. Per-seed stdout/stderr and raw argv are preserved.",
            "Failed seeds get bounded deterministic prefix replay when possible. A replay result is a minimal failing prefix, not arbitrary ddmin; unreproduced minimization is INCONCLUSIVE while the original case remains FAIL.",
        ],
    }
    write_json(artifacts / "proof.json", proof)
    print(json.dumps(proof, sort_keys=True))
    return 0 if status == "PASS" else 1


if __name__ == "__main__":
    raise SystemExit(main(sys.argv[1:]))
