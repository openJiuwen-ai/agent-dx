#!/usr/bin/env python3
# Copyright (c) Huawei Technologies Co., Ltd. 2026. All rights reserved.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
# http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

import argparse
import json
import os
import time
from pathlib import Path


def process_sample(pid_file: str):
    pid = int(Path(pid_file).read_text().strip())
    fields = Path(f"/proc/{pid}/stat").read_text().split()
    status = Path(f"/proc/{pid}/status").read_text().splitlines()
    rss = next(int(line.split()[1]) for line in status if line.startswith("VmRSS:"))
    return pid, int(fields[13]) + int(fields[14]), rss


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--process", action="append", required=True, metavar="NAME=PID_FILE")
    parser.add_argument("--output", required=True)
    parser.add_argument("--stop-file", required=True)
    parser.add_argument("--interval", type=float, default=0.05)
    args = parser.parse_args()
    processes = dict(item.split("=", 1) for item in args.process)
    samples = {name: [] for name in processes}
    started = time.monotonic()
    while not Path(args.stop_file).exists():
        now = time.monotonic()
        for name, pid_file in processes.items():
            try:
                samples[name].append((now, *process_sample(pid_file)))
            except (FileNotFoundError, ProcessLookupError, ValueError, StopIteration):
                pass
        time.sleep(args.interval)
    ended = time.monotonic()
    clock_ticks = os.sysconf("SC_CLK_TCK")
    summary = {"elapsed_seconds": ended - started, "clock_ticks": clock_ticks, "processes": {}}
    for name, values in samples.items():
        if not values:
            summary["processes"][name] = {"samples": 0}
            continue
        cpu_seconds = max(0, values[-1][2] - values[0][2]) / clock_ticks
        summary["processes"][name] = {
            "pid": values[-1][1],
            "samples": len(values),
            "cpu_seconds": cpu_seconds,
            "cpu_cores_avg": cpu_seconds / max(ended - started, 1e-9),
            "rss_start_kib": values[0][3],
            "rss_end_kib": values[-1][3],
            "rss_peak_kib": max(value[3] for value in values),
        }
    Path(args.output).write_text(json.dumps(summary, sort_keys=True) + "\n")


if __name__ == "__main__":
    main()
