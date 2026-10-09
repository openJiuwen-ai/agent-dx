#!/usr/bin/env python3
"""Read-only Linux observation of protected AFS processes and FUSE mounts."""
import hashlib
import json
import os
import platform
import subprocess
import sys
import time
from pathlib import Path

if platform.system() != "Linux":
    raise SystemExit("Linux only")

def digest(path):
    h = hashlib.sha256()
    with open(path, "rb") as f:
        for block in iter(lambda: f.read(1024 * 1024), b""):
            h.update(block)
    return h.hexdigest()

processes = []
for p in Path("/proc").iterdir():
    if not p.name.isdigit():
        continue
    try:
        exe = os.readlink(p / "exe")
        if Path(exe).name not in {"afs-node", "afs-meta"}:
            continue
        stat = (p / "stat").read_text().rsplit(")", 1)[1].split()
        argv = (p / "cmdline").read_bytes().rstrip(b"\0").decode().split("\0")
        configs = {}
        for i, value in enumerate(argv):
            if value in {"--config", "-c"} and i + 1 < len(argv):
                config = Path(argv[i + 1])
                configs[str(config)] = digest(config)
        processes.append({"pid": int(p.name), "startticks": int(stat[19]), "exe": exe,
                          "binary_sha256": digest(p / "exe"), "argv": argv,
                          "configs": configs})
    except (FileNotFoundError, ProcessLookupError):
        continue

mounts = [line for line in Path("/proc/self/mountinfo").read_text().splitlines()
          if " - fuse" in line and "afs" in line]
commands = {}
for name, argv in {"rdma_link": ["rdma", "link"], "qp": ["rdma", "-j", "resource", "show", "qp"],
                   "cm_id": ["rdma", "-j", "resource", "show", "cm_id"],
                   "statistics": ["rdma", "statistic", "show", "link", "rxe0/1"]}.items():
    run = subprocess.run(argv, capture_output=True, timeout=5)
    commands[name] = {"argv": argv, "returncode": run.returncode,
                      "stdout": run.stdout.decode(), "stderr": run.stderr.decode()}
result = {"schema_version": 1, "observed_unix_ns": time.time_ns(),
          "hostname": platform.node(), "kernel": platform.release(),
          "boot_id": Path("/proc/sys/kernel/random/boot_id").read_text().strip(),
          "machine_id": Path("/etc/machine-id").read_text().strip(),
          "processes": sorted(processes, key=lambda item: item["pid"]),
          "afs_mounts": mounts, "commands": commands}
out = Path(sys.argv[1])
out.parent.mkdir(parents=True, exist_ok=True)
with out.open("x") as f:
    json.dump(result, f, indent=2)
    f.write("\n")
