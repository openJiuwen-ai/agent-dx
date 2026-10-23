#!/usr/bin/env python3
"""Join restore stages by Environment/runtime/request identity; timings are nested."""

import argparse
import json
import math
from pathlib import Path
import re
import statistics

ANSI = re.compile(r"\x1b\[[0-9;]*m")


def fields(line):
    line = ANSI.sub("", line)
    return {key: value.strip('"') for key, value in re.findall(r'(\w+)=("[^"\n]*"|[^\s]+)', line)}


def event(lines, name, identity, value):
    matches = []
    for line in lines:
        parsed = fields(line)
        if parsed.get("event") == name and parsed.get(identity) == value:
            matches.append(parsed)
    if len(matches) != 1:
        raise ValueError(f"{name}: expected one {identity}={value}, found {len(matches)}")
    result = {key: float(value) for key, value in matches[0].items() if key.endswith("_ms")}
    if not result or not all(math.isfinite(value) and value >= 0 for value in result.values()):
        raise ValueError("missing or invalid phase timings")
    return result


def backend_phase(line):
    values = re.findall(r"(\w+)=(\d+)ms", line.split("phases: ", 1)[1])
    if not values or "total" not in dict(values):
        raise ValueError("missing sandboxd phase timings")
    return {key + "_ms": int(value) for key, value in values}


def cow_values(path):
    result = {}
    for line in path.read_text().splitlines():
        match = re.fullmatch(
            r"afs_fuse_copy_file_range_duration_seconds_(sum|count)\{([^}]+)\} (\S+)", line
        )
        if match:
            labels = dict(re.findall(r'(\w+)="([^"]+)"', match[2]))
            if labels.get("filesystem") == "dfs":
                result[labels["stage"] + "_" + match[1]] = float(match[3])
    if set(result) != {"queue_sum", "queue_count", "execute_sum", "execute_count"}:
        raise ValueError("missing copy_file_range metrics")
    return result


def cow_delta(before, after):
    result = {key: after[key] - value for key, value in before.items()}
    if any(not math.isfinite(value) or value < 0 for value in result.values()):
        raise ValueError("copy_file_range counter reset")
    if result["queue_count"] != result["execute_count"]:
        raise ValueError("incomplete copy_file_range timing")
    return {
        "calls": result["execute_count"],
        "queue_ms": result["queue_sum"] * 1000,
        "execute_ms": result["execute_sum"] * 1000,
    }


def meta_summary(path):
    groups = {"afs_meta_store_profile": [], "afs_meta_local_file_profile": []}
    for line in path.read_text().splitlines():
        try:
            value = json.loads(line)
        except ValueError:
            continue
        group = groups.get(value.get("event"))
        if group is not None:
            group.append(value)
    result = {}
    for kind, values in groups.items():
        if not values:
            continue
        keys = [key for key in values[0] if key.endswith("_ms")]
        result[kind] = {
            "commits": len(values),
            "encoded_bytes_sum": sum(v["bytes"] for v in values),
            "phase_ms_sum": {key: sum(v[key] for v in values) for key in keys},
            "first": values[0],
            "last": values[-1],
            "first_100_mean": {key: statistics.mean(v[key] for v in values[:100]) for key in keys},
            "last_100_mean": {key: statistics.mean(v[key] for v in values[-100:]) for key in keys},
        }
        result[kind]["size_bins"] = []
        for low, high in ((0, 1), (1, 8), (8, 16), (16, 32), (32, 64), (64, math.inf)):
            rows = [v for v in values if low * 1024**2 <= v["bytes"] < high * 1024**2]
            if rows:
                result[kind]["size_bins"].append(
                    {
                        "min_mib": low,
                        "max_mib": high if math.isfinite(high) else None,
                        "commits": len(rows),
                        "mean_bytes": statistics.mean(v["bytes"] for v in rows),
                        "phase_ms_mean": {
                            key: statistics.mean(v[key] for v in rows) for key in keys
                        },
                    }
                )
    return result


def report(root):
    run = json.loads((root / "result.json").read_text())
    logs = {
        node: (root / f"{node}-sandboxd-service.log").read_text().replace('\\"', '"').splitlines()
        for node in ("node1", "node2")
    }
    managers = [
        line for path in root.glob("node*-logs/*.log") for line in path.read_text().splitlines()
    ]
    ingress_paths = [
        *root.glob("ingress*.log"),
        root.parent / "state/logs/api.log",
        *root.parent.glob("ingress2/state/logs/*.log"),
    ]
    ingress = [line for path in ingress_paths for line in path.read_text().splitlines()]
    samples = []
    for sample in run["samples"]:
        identity = sample["environment_id"]
        physical = {}
        runtime = {}
        for node, lines in logs.items():
            physical[node] = []
            for line in lines:
                if identity in line and "Type:create" in line:
                    backend = re.search(r"SandboxID:(sbox-[^ ]+)", line)[1]
                    if backend not in physical[node]:
                        physical[node].append(backend)
                    runtime[backend] = re.search(r'key:"adx.runtime_id" value:"([^"]+)"', line)[1]
        source_lines = logs.get("node1")
        source_backends = physical.get("node1")
        if source_lines is None or not source_backends:
            raise ValueError("missing capture source in benchmark evidence")
        capture_marker = "checkpointed sandbox " + source_backends[0]
        capture = next(line for line in source_lines if capture_marker in line)
        target = sample["target"]
        target_lines = logs.get(target)
        target_backends = physical.get(target)
        if target_lines is None or target_backends is None:
            raise ValueError("unknown restore target in benchmark evidence")
        restores = []
        for line in target_lines:
            for backend in target_backends:
                if "restored sandbox " + backend in line:
                    restores.append((backend, line))
        if len(restores) != 1:
            raise ValueError("benchmark must have exactly one backend restore")
        backend, restore = restores[0]
        runtime_id = runtime.get(backend)
        if runtime_id is None:
            raise ValueError("restore backend has no runtime identity")
        row = {
            "environment_id": identity,
            "runtime_id": runtime_id,
            "cache": sample["cache"],
            "index": sample["index"],
            "pause_ms": sample["pause_seconds"] * 1000,
            "first_http_ms": sample["first_http_seconds"] * 1000,
            "sandboxd_capture": backend_phase(capture),
            "sandboxd_restore": backend_phase(restore),
        }
        row["environment_resume_stages"] = event(
            managers, "environment_resume_stages", "runtime_id", runtime_id
        )
        row["environment_restore_execution_stages"] = event(
            managers, "environment_restore_execution_stages", "runtime_id", runtime_id
        )
        row["environment_backend_restore_stages"] = event(
            managers, "environment_backend_restore_stages", "runtime_id", runtime_id
        )
        row["sandboxd_rpc_outside_restore_ms"] = (
            row["environment_backend_restore_stages"]["start_rpc_ms"]
            - row["sandboxd_restore"]["total_ms"]
        )
        reply = min(sample["replies"], key=lambda value: value["seconds"])
        row["ingress_wake"] = event(
            ingress, "environment_ingress_wake_stages", "request_id", reply["nonce"]
        )
        row["outside_ingress_wake_ms"] = row["first_http_ms"] - row["ingress_wake"]["total_ms"]
        if "client_auth_seconds" in reply:
            row["client_auth_ms"] = reply["client_auth_seconds"] * 1000
            row["client_http_ms"] = reply["client_http_seconds"] * 1000
            row["http_roundtrip_outside_wake_ms"] = (
                row["client_http_ms"] - row["ingress_wake"]["total_ms"]
            )
        if run["storage"]["kind"] == "afs":
            prefix = f"afs-{sample['cache']}-{sample['index']}"
            row["cow"] = {
                node: cow_delta(
                    cow_values(root / f"{prefix}-before-{node}.prom"),
                    cow_values(root / f"{prefix}-first-{node}.prom"),
                )
                for node in ("node-a", "node-b", "node-c")
            }
        samples.append(row)
    result = {
        "storage": run["storage"],
        "samples": samples,
        "timing_contract": "nested wall times; do not add parent and child stages; COW is a subset of sandboxd phases",
    }
    meta = root / "afs/afs-meta-0.log"
    if meta.exists():
        result["meta_profile"] = meta_summary(meta)
    return result


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("evidence", type=Path)
    parser.add_argument("--output", type=Path, required=True)
    args = parser.parse_args()
    args.output.write_text(json.dumps(report(args.evidence), indent=2) + "\n")
