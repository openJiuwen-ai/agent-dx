"""Validate physical evidence and summarize matched storage benchmark samples."""

import math
import statistics

CASES = (
    "durable capture and direct filesystem path",
    "concurrent traffic wake preserves guest state",
    "node failure retains execution inputs across Coordinator restart",
    "Coordinator reclaims after source reconciliation",
    "anonymous checkpoint follows Environment",
    "Coordinator reclaims stopped incomplete capture",
)


def source_invalidated(record, source):
    # Recovery can reserve the replacement and clear invalidated between polls.
    return record["invalidated"] or record["assignment"]["node_id"] != source


def summarize(samples, repetitions):
    groups = {}
    for row in samples:
        if row["request_count"] != 12 or row["restore_count"] != 1 or row["continuity"] is not True:
            raise ValueError("missing successful physical traffic evidence")
        if row["artifact_bytes"] <= 0:
            raise ValueError("missing successful physical traffic evidence")
        groups.setdefault(row["mode"] + "/" + row["cache"], []).append(row)
    if not groups or repetitions < 3:
        raise ValueError("at least three repetitions are required")
    modes = {row["mode"] for row in samples}
    if len(modes) != 1 or set(groups) != {
        next(iter(modes)) + "/" + cache for cache in ("cold", "warm")
    }:
        raise ValueError("one backend with complete cold and warm groups is required")
    result = {}
    for key, rows in groups.items():
        if len(rows) != repetitions:
            raise ValueError("incomplete benchmark group: " + key)
        result[key] = {"samples": len(rows)}
        for field in ("pause_seconds", "first_http_seconds", "touch_seconds", "artifact_bytes"):
            values = [r[field] for r in rows]
            if not all(math.isfinite(v) and v >= 0 for v in values):
                raise ValueError("invalid timing or byte measurement")
            result[key][field] = {
                "median": statistics.median(values),
                "min": min(values),
                "max": max(values),
            }
    return result


def _verify_completion(result, expected_cases, message):
    if (
        result.get("status") != "passed"
        or result.get("cleanup_errors") != []
        or result.get("inventories") != {"node1": 0, "node2": 0}
    ):
        raise ValueError(message)
    if [case.get("name") for case in result.get("cases", [])] != list(expected_cases):
        raise ValueError(message)
    if any(case.get("passed") is not True for case in result["cases"]):
        raise ValueError(message)


def _verify_benchmark(result):
    repetitions = result["repetitions"]
    samples = result["samples"]
    if repetitions < 1 or len(samples) != repetitions * 2:
        raise ValueError("missing benchmark samples")
    for cache in ("cold", "warm"):
        if len([sample for sample in samples if sample["cache"] == cache]) != repetitions:
            raise ValueError("incomplete benchmark group")
    workload = result["workload"]
    if workload.get("name") != "deterministic-512m-v1":
        raise ValueError("matched full-memory workload required")
    for row in samples:
        if (
            row["request_count"] != 12
            or row["restore_count"] != 1
            or row.get("full_memory_verified") is not True
        ):
            raise ValueError("missing full-memory or single-restore traffic proof")
        if (
            row["continuity"] is not True
            or row.get("checked_bytes") != workload["payload_bytes"]
            or row.get("sha256") != workload["expected_full_sha256"]
        ):
            raise ValueError("missing full-memory or single-restore traffic proof")


def _verify_diagnostic(result):
    trace = result.get("restore_trace", {})
    message = "incomplete restore diagnostic evidence"
    if (
        result.get("storage", {}).get("kind") != "afs"
        or result["repetitions"] != 1
        or len(result["samples"]) != 2
    ):
        raise ValueError(message)
    if (
        trace.get("metadata_cow_confirmed") is not True
        or trace.get("full_source_read_before_first_touch") is not False
        or trace.get("late_touch_remote_read_bytes", 0) <= 0
    ):
        raise ValueError(message)
    if any(
        sample["request_count"] != 12 or sample["restore_count"] != 1 or not sample["continuity"]
        for sample in result["samples"]
    ):
        raise ValueError(message)


def verify(result):
    if result.get("benchmark_only"):
        _verify_completion(result, CASES[:2], "incomplete storage benchmark evidence")
        _verify_benchmark(result)
        return
    _verify_completion(result, CASES, "incomplete storage Firecracker lifecycle evidence")
    if result.get("diagnostic_only"):
        _verify_diagnostic(result)
    else:
        summarize(result["samples"], result["repetitions"])
