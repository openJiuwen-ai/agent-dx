#!/usr/bin/env python3
"""Separate guest agent process CPU from RPC wall time in explicit diagnostic logs."""

import argparse
import json
import math
import hashlib
from datetime import datetime, timezone, timedelta
from pathlib import Path
import re

FIELDS = re.compile(r'(\w+)=("[^"\n]*"|[^\s]+)')


def parse_line(line):
    return {k: v.strip('"') for k, v in FIELDS.findall(line)}


def host_time_ns(value):
    delta = datetime.fromisoformat(value).replace(tzinfo=timezone(timedelta(hours=8))) - datetime(
        1970, 1, 1, tzinfo=timezone.utc
    )
    return ((delta.days * 86400 + delta.seconds) * 1000000 + delta.microseconds) * 1000


def summarize(logs):
    rows = []
    for path in logs:
        for line in path.read_text().splitlines():
            if "firecracker: guest agent CPU profile" not in line:
                continue
            fields = parse_line(line)
            timing_names = (
                "guest_handler_wall_ms",
                "guest_process_user_ms",
                "guest_process_system_ms",
                "host_response_ms",
            )
            row = {key: float(fields[key]) for key in timing_names}
            if any(not math.isfinite(v) or v < 0 for v in row.values()):
                raise ValueError("invalid guest CPU timing")
            row.update(
                log=path.name,
                vsock=fields["vsock"],
                message_type=int(fields["message_type"]),
                time=fields["time"],
            )
            row["guest_process_cpu_ms"] = (
                row["guest_process_user_ms"] + row["guest_process_system_ms"]
            )
            row["response_outside_handler_ms"] = (
                row["host_response_ms"] - row["guest_handler_wall_ms"]
            )
            rows.append(row)
    if not rows:
        raise ValueError("guest agent CPU instrumentation missing")
    return {
        "scope": (
            "guest process CPU accounting excludes sandbox children; concurrent agent goroutines may contribute. "
            "It is not physical host CPU: observed shared-file restore accounting can exceed host vCPU execution. "
            "Correlate host schedstat and waits. Response outside handler includes "
            "scheduling/transport/serialization, not exclusively storage"
        ),
        "samples": rows,
    }


def wait_window(points, start_ns, end_ns):
    """Integrate observed intervals, excluding gaps larger than 200 ms."""
    covered = blocked = dirty = page = 0
    for left, right in zip(points, points[1:]):
        a, b = left["time_ns"], right["time_ns"]
        if b - a > 200_000_000:
            continue
        duration = max(0, min(b, end_ns) - max(a, start_ns))
        covered += duration
        if "fuse" in left["wchan"] or left["wchan"] == "request_wait_answer":
            blocked += duration
        elif left["wchan"] == "balance_dirty_pages":
            dirty += duration
        elif left["wchan"] == "folio_wait_bit_common" and "filemap_fault" in left.get("stack", ""):
            page += duration
    return {
        "covered_ms": covered / 1e6,
        "fuse_wait_estimate_ms": blocked / 1e6,
        "dirty_throttle_estimate_ms": dirty / 1e6,
        "file_page_fault_wait_estimate_ms": page / 1e6,
    }


def cpu_window(points, start_ns, end_ns):
    covered = run = queue = 0
    for left, right in zip(points, points[1:]):
        a, b = left["time_ns"], right["time_ns"]
        if not 0 < b - a <= 200_000_000 or "run_ns" not in left or "run_ns" not in right:
            continue
        duration = max(0, min(b, end_ns) - max(a, start_ns))
        active = right["run_ns"] - left["run_ns"]
        waiting = right["runqueue_ns"] - left["runqueue_ns"]
        if active < 0 or waiting < 0:
            raise ValueError("vCPU accounting reset")
        covered += duration
        run += active * duration / (b - a)
        queue += waiting * duration / (b - a)
    return {
        "cpu_covered_ms": covered / 1e6,
        "vcpu_run_estimate_ms": run / 1e6,
        "runqueue_estimate_ms": queue / 1e6,
    }


def restore_phases(logs, points, cpu):
    """Join explicit restore windows to guest replies and independently sampled waits."""
    groups = {}
    for point in points:
        groups.setdefault((point["backend_id"], point["pid"], point["tid"]), []).append(point)
    rows = []
    kinds = {"agent_health": 1, "agent_network": 7, "agent_release": 10}
    for path in logs:
        for line in path.read_text().splitlines():
            if "firecracker: restore agent phase" not in line:
                continue
            fields = parse_line(line)
            end = host_time_ns(fields["time"])
            wall = float(fields["elapsed_ms"])
            start = end - int(wall * 1e6)
            backend = fields["sandbox_id"]
            tag = hashlib.sha256(backend.encode()).hexdigest()[:24]
            phase_id = kinds.get(fields.get("phase"))
            if phase_id is None:
                raise ValueError("unknown restore agent phase")
            replies = []
            segment = f"/{tag}/"
            for reply in cpu:
                if reply["log"] != path.name or segment not in reply["vsock"]:
                    continue
                if reply["message_type"] != phase_id:
                    continue
                if start - 2_000_000 <= host_time_ns(reply["time"]) <= end + 2_000_000:
                    replies.append(reply)
            waits = []
            for (identity, pid, tid), samples in groups.items():
                if identity != backend:
                    continue
                window = wait_window(samples, start, end)
                if window["covered_ms"]:
                    waits.append(
                        {"pid": pid, "tid": tid, **window, **cpu_window(samples, start, end)}
                    )
            rows.append(
                {
                    "log": path.name,
                    "sandbox_id": backend,
                    "phase": fields["phase"],
                    "wall_ms": wall,
                    "guest_cpu_replies": replies,
                    "vcpu_waits": waits,
                }
            )
    return rows


if __name__ == "__main__":
    p = argparse.ArgumentParser(description=__doc__)
    p.add_argument("logs", nargs="+", type=Path)
    p.add_argument("--output", required=True, type=Path)
    p.add_argument("--vcpu", type=Path)
    a = p.parse_args()
    result = summarize(a.logs)
    if a.vcpu:
        points = [json.loads(line) for line in a.vcpu.read_text().splitlines()]
        result["restore_phases"] = restore_phases(a.logs, points, result["samples"])
        result["log_timezone"] = "Asia/Shanghai; dedicated experiment host"
    a.output.write_text(json.dumps(result, indent=2) + "\n")
