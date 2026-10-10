#!/usr/bin/env python3
"""Fail-closed boundary for full AFS environment qualification.

The former evaluator recognized one historical lab only and could not grant
full qualification. Its raw observations and implementation are archived
outside the source tree. A portable full verifier remains a separate task.
"""
from __future__ import annotations

import argparse
import hashlib
import json
from pathlib import Path
import re


FULL_BLOCKER = "portable full environment verifier is not implemented; full acceptance remains BLOCKED"


def qualification_errors(lock: dict, lock_path: Path) -> list[str]:
    """Validate the bound bundle, then keep full qualification blocked.

    A self-reported PASS, a matching hash, or a historical bundle cannot replace
    an implemented verifier of the current environment.
    """
    if not isinstance(lock, dict):
        return ["environment lock must be an object"]
    evidence = lock.get("environment_evidence")
    if not isinstance(evidence, dict):
        return ["lock.environment_evidence is missing"]
    rel, expected_sha = evidence.get("path"), evidence.get("sha256")
    if not isinstance(rel, str) or not rel or "\x00" in rel or Path(rel).is_absolute() or ".." in Path(rel).parts:
        return ["environment evidence path escapes evidence root"]
    if not isinstance(expected_sha, str) or not re.fullmatch(r"[0-9a-f]{64}", expected_sha):
        return ["lock.environment_evidence requires a valid sha256"]
    try:
        root = lock_path.parent.resolve()
        bundle_path = (root / rel).resolve()
        if not bundle_path.is_relative_to(root):
            return ["environment evidence path escapes evidence root"]
        if not bundle_path.is_file():
            return ["environment evidence bundle is missing"]
        data = bundle_path.read_bytes()
        actual_sha = hashlib.sha256(data).hexdigest()
        if actual_sha != expected_sha:
            return [f"environment evidence bundle sha256 mismatch: {actual_sha}"]
        bundle = json.loads(data)
        if not isinstance(bundle, dict):
            return ["environment evidence bundle root must be an object"]
    except (OSError, ValueError, RuntimeError) as exc:
        return [f"environment evidence bundle is malformed: {exc}"]
    return [FULL_BLOCKER]


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description="Report full AFS environment qualification as BLOCKED")
    parser.add_argument("--lock", required=True, type=Path)
    parser.add_argument("--output", required=True, type=Path)
    args = parser.parse_args(argv)
    try:
        lock = json.loads(args.lock.read_bytes())
        errors = qualification_errors(lock, args.lock)
    except (OSError, ValueError) as exc:
        errors = [f"environment lock is malformed: {exc}"]
    report = {"schema_version": 1, "status": "BLOCKED", "errors": errors, "full_release_gate_pass": False}
    args.output.parent.mkdir(parents=True, exist_ok=True)
    # Do not overwrite an earlier result or failure record.
    with args.output.open("x", encoding="utf-8") as handle:
        json.dump(report, handle, indent=2, sort_keys=True)
        handle.write("\n")
    return 2


if __name__ == "__main__":
    raise SystemExit(main())
