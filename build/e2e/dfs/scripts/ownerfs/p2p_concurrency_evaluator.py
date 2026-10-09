#!/usr/bin/env python3
"""Evaluate the declared OwnerFs P2P concurrency goal from raw Linux receipts."""

from __future__ import annotations

import argparse
import json
from math import ceil
from pathlib import Path
from typing import Any


READ_STAGES = ("first_read", "repeat_read")
ALL_STAGES = (*READ_STAGES, "remote_overwrite", "owner_read_after")


def read_json(path: Path) -> dict[str, Any]:
    with path.open(encoding="utf-8") as stream:
        value = json.load(stream)
    if not isinstance(value, dict):
        raise ValueError(f"expected JSON object: {path}")
    return value


def p50(values: list[float]) -> float:
    if not values:
        raise ValueError("empty performance sample set")
    ordered = sorted(values)
    return ordered[ceil(len(ordered) / 2) - 1]


def validate_w2(data: dict[str, Any], workers: int, label: str, failures: list[str]) -> None:
    if data.get("status") != "PASS":
        failures.append(f"{label}: runner did not pass")
    params = data.get("params", {})
    if (params.get("workers"), params.get("count"), params.get("size")) != (workers, 200, 4096):
        failures.append(f"{label}: expected workers={workers}, count=200, size=4096")
    rounds = data.get("rounds", [])
    if len(rounds) < 6 or params.get("rounds") != len(rounds):
        failures.append(f"{label}: requires at least six complete rounds")
    for round_number, row in enumerate(rounds, 1):
        for backend in ("ownerfs", "moosefs"):
            case = row.get("cases", {}).get(backend, {})
            if not case.get("prepare", {}).get("correctness") or not case.get("cleanup", {}).get("correctness"):
                failures.append(f"{label} round {round_number} {backend}: prepare/cleanup failed")
            for stage in ALL_STAGES:
                if not case.get("stages", {}).get(stage, {}).get("correctness"):
                    failures.append(f"{label} round {round_number} {backend} {stage}: correctness failed")


def read_wall_samples(data: dict[str, Any], backend: str) -> list[float]:
    return [
        sum(float(row["cases"][backend]["stages"][stage]["phase_ledger"]["wall_us"]) for stage in READ_STAGES)
        for row in data["rounds"]
    ]


def mount_contract(data: dict[str, Any]) -> dict[str, Any]:
    """Keep mount type/options/source, excluding the intentionally unique AFS run path."""
    normalized: dict[str, Any] = {}
    for role in ("A", "B"):
        roots = data["root_validation"][role]
        owner = roots["ownerfs"]
        moose = roots["moosefs"]
        normalized[role] = {
            "owner_mount": {key: owner["findmnt"][key] for key in ("source", "fstype", "options")},
            "owner_namespace": owner["namespace_relative"].split("/", 1)[0],
            "moosefs": moose,
        }
    return normalized


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    for name in ("baseline", "candidate", "acceptance", "w1", "w2"):
        parser.add_argument(f"--{name}", required=True, type=Path)
    parser.add_argument("--min-read-improvement", type=float, default=0.15)
    parser.add_argument("--total-ratio-max", type=float)
    args = parser.parse_args()
    baseline = read_json(args.baseline)
    candidate = read_json(args.candidate)
    acceptance = read_json(args.acceptance)
    w1 = read_json(args.w1)
    w2 = read_json(args.w2)
    failures: list[str] = []

    validate_w2(baseline, 8, "baseline", failures)
    validate_w2(candidate, 8, "candidate", failures)
    validate_w2(w2, 1, "sequential W2", failures)
    for field in ("hosts", "timing_contract"):
        if baseline.get(field) != candidate.get(field):
            failures.append(f"concurrent runs differ in {field}")
    for field in ("count", "size", "seed", "workers"):
        if baseline.get("params", {}).get(field) != candidate.get("params", {}).get(field):
            failures.append(f"concurrent runs differ in {field}")
    for field in ("runner", "distributed_workload", "posix_workload"):
        if baseline.get("sha256", {}).get(field) != candidate.get("sha256", {}).get(field):
            failures.append(f"concurrent runs differ in {field} hash")
    if mount_contract(baseline) != mount_contract(candidate):
        failures.append("concurrent runs use different mount/root validation")

    baseline_read = p50(read_wall_samples(baseline, "ownerfs"))
    candidate_read = p50(read_wall_samples(candidate, "ownerfs"))
    moosefs_baseline = p50(read_wall_samples(baseline, "moosefs"))
    moosefs_candidate = p50(read_wall_samples(candidate, "moosefs"))
    improvement = 1.0 - candidate_read / baseline_read
    moosefs_drift = moosefs_candidate / moosefs_baseline
    if improvement < args.min_read_improvement:
        failures.append(
            f"concurrent B read improved {improvement:.1%}; "
            f"requires >={args.min_read_improvement:.1%}"
        )
    if not 0.90 <= moosefs_drift <= 1.10:
        failures.append(f"MooseFS reference drifted by {moosefs_drift:.3f}x between concurrent runs")
    total_ratio = candidate.get("summary", {}).get("ownerfs_to_moosefs_p50_total_wall")
    if args.total_ratio_max is not None and (total_ratio is None or total_ratio > args.total_ratio_max):
        failures.append(f"concurrent total OwnerFs/MooseFS must be <={args.total_ratio_max:.3f}")

    w1_ratios = w1.get("ratios", [])
    if w1.get("status") != "PASS" or len(w1_ratios) != 2 or any(ratio > 0.80 for ratio in w1_ratios):
        failures.append("W1 requires two passing sessions, each OwnerFs/MooseFS p50 <=0.80")
    w2_ratio = w2.get("summary", {}).get("ownerfs_to_moosefs_p50_total_wall")
    if w2_ratio is None or w2_ratio > 1.10:
        failures.append("sequential W2 OwnerFs/MooseFS p50 must be <=1.10")
    if not (acceptance.get("status") == "PASS" and acceptance.get("passed") == 15
            and acceptance.get("failed") == 0 and not acceptance.get("gaps") and not acceptance.get("cleanup_errors")):
        failures.append("three-VM OwnerFs acceptance must pass 15/15 with no gaps or cleanup errors")

    result = {
        "status": "PASS" if not failures else "FAIL",
        "concurrent_read_p50_us": {"baseline": baseline_read, "candidate": candidate_read},
        "concurrent_read_improvement": improvement,
        "moosefs_reference_drift": moosefs_drift,
        "w1_ratios": w1_ratios,
        "sequential_w2_ratio": w2_ratio,
        "concurrent_total_ratio": total_ratio,
        "failures": failures,
    }
    print(json.dumps(result, indent=2, sort_keys=True))
    return 0 if not failures else 1


if __name__ == "__main__":
    raise SystemExit(main())
