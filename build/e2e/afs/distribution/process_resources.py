#!/usr/bin/env python3
"""Read-only resource snapshot of experiment services in this node namespace."""

import json
import os
from pathlib import Path

results = []
for entry in Path("/proc").iterdir():
    if not entry.name.isdigit():
        continue
    try:
        name = (entry / "comm").read_text().strip()
        if name not in {"afs-meta", "afs-node", "registry", "dfdaemon", "scheduler"}:
            continue
        fields = (entry / "stat").read_text().rsplit(")", 1)[1].split()
        status = {}
        for line in (entry / "status").read_text().splitlines():
            if line.startswith(("VmRSS:", "VmHWM:", "Threads:")):
                key, value = line.split(":", 1)
                status[key] = value.strip()
        results.append(
            {
                "pid": int(entry.name),
                "name": name,
                "start_ticks": int(fields[19]),
                "cpu_seconds": (int(fields[11]) + int(fields[12])) / os.sysconf("SC_CLK_TCK"),
                "status": status,
                "fds": len(list((entry / "fd").iterdir())),
            }
        )
    except FileNotFoundError:
        continue
print(json.dumps(results))
