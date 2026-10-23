#!/usr/bin/env python3
"""Opt-in host observer for a caller-owned FC trial; no application payload."""

import argparse
import json
import os
from pathlib import Path
import time


def cpu_stat(text, ticks):
    fields = text.rsplit(") ", 1)[1].split()
    return {
        "state": fields[0],
        "parent": int(fields[1]),
        "cpu_ns": (int(fields[11]) + int(fields[12])) * 1_000_000_000 // ticks,
    }


def observe(trial, status, output, period):
    trial = trial.resolve()
    ticks = os.sysconf("SC_CLK_TCK")
    output.mkdir(parents=True, exist_ok=False)
    end = time.monotonic() + 3600
    known = {}
    next_scan = 0
    count = 0
    with (output / "vcpu.jsonl").open("w") as log:
        while time.monotonic() < end:
            if status.exists() and json.loads(status.read_text())["status"] != "running":
                break
            if time.monotonic() >= next_scan:
                known = {}
                for proc in Path("/proc").glob("[0-9]*"):
                    try:
                        if proc.joinpath("comm").read_text().strip() != "firecracker":
                            continue
                        stat = cpu_stat(proc.joinpath("stat").read_text(), ticks)
                        if (
                            str(trial).encode()
                            not in (Path("/proc") / str(stat["parent"]) / "cmdline").read_bytes()
                        ):
                            continue
                        args = proc.joinpath("cmdline").read_bytes().decode().split("\0")
                        backend = args[args.index("--id") + 1]
                        for task in proc.joinpath("task").iterdir():
                            if "vcpu" in task.joinpath("comm").read_text():
                                known[task] = (int(proc.name), backend)
                    except (OSError, ValueError, IndexError):
                        continue
                next_scan = time.monotonic() + 0.1
            for task, (pid, backend) in known.items():
                try:
                    row = {
                        "time_ns": time.time_ns(),
                        "pid": pid,
                        "tid": int(task.name),
                        "backend_id": backend,
                        **cpu_stat(task.joinpath("stat").read_text(), ticks),
                        "wchan": task.joinpath("wchan").read_text().strip(),
                    }
                    sched = task.joinpath("schedstat").read_text().split()
                    row.update(run_ns=int(sched[0]), runqueue_ns=int(sched[1]))
                    if row["wchan"] != "0":
                        row["stack"] = task.joinpath("stack").read_text()
                    log.write(json.dumps(row) + "\n")
                    count += 1
                except OSError:
                    continue
            log.flush()
            time.sleep(period)
    (output / "observer.json").write_text(
        json.dumps(
            {
                "samples": count,
                "period_seconds": period,
                "cpu_clock_ticks": ticks,
                "scope": "all vCPU execution, not exclusive guest agent CPU",
                "trial": str(trial),
            },
            indent=2,
        )
        + "\n"
    )
    if not count:
        raise RuntimeError("no owned vCPU samples captured")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--trial", type=Path, required=True)
    parser.add_argument("--status", type=Path, required=True)
    parser.add_argument("--output", type=Path, required=True)
    parser.add_argument("--period", type=float, default=0.02)
    args = parser.parse_args()
    if not 0.02 <= args.period <= 1:
        parser.error("period must be 20..1000 ms")
    observe(args.trial, args.status, args.output, args.period)
