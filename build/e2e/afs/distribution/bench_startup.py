#!/usr/bin/env python3
"""Guest-side Nydus startup measurement. Every case has a fresh native cache."""

import argparse
import hashlib
import json
import os
import pathlib
import shutil
import subprocess
import time

p = argparse.ArgumentParser(description=__doc__)
p.add_argument("--backend", choices=["registry", "registry-direct", "afs", "dragonfly"], required=True)
p.add_argument("--ip", required=True)
p.add_argument("--case", required=True)
p.add_argument("--manifest", required=True)
p.add_argument("--full", action="store_true")
a = p.parse_args()
umount = shutil.which("umount")
if umount is None:
    raise SystemExit("umount is required")
if pathlib.PurePosixPath(a.case).name != a.case:
    raise SystemExit("case must be a basename")
root = pathlib.Path("/workspace/nydus/bench") / a.case
root.mkdir(parents=True, exist_ok=False)
(root / "cache").mkdir()
(root / "mnt").mkdir()
if a.backend == "afs":
    backend = {
        "type": "http-proxy",
        "http-proxy": {
            "addr": f"http://{a.ip}:7401",
            "path": f"/nydus/lab/{a.manifest}/blobs",
            "timeout": 30,
            "retry_limit": 0,
        },
    }
else:
    host = {"registry": "10.203.0.10:5001", "registry-direct": "10.203.0.10:5000"}.get(a.backend, f"{a.ip}:4001")
    backend = {"type": "registry", "registry": {"scheme": "http", "host": host, "repo": "lab/nydus", "retry_limit": 0}}
config = {
    "version": 2,
    "backend": backend,
    "cache": {
        "type": "blobcache",
        "validate": True,
        "filecache": {"work_dir": str(root / "cache")},
        "prefetch": {"enable": False},
    },
    "rafs": {"mode": "direct", "validate": False, "batch_size": 1048576, "prefetch": {"enable": False}},
}
(root / "config.json").write_text(json.dumps(config))
expected = json.loads(pathlib.Path("/output/fixture/expected-files.json").read_text())
result = {"backend": a.backend, "case": a.case, "manifest": a.manifest, "files": {}}
log = open(f"/output/logs/bench-{a.case}.log", "w")
start = time.monotonic()
process = subprocess.Popen(
    [
        "/artifacts/tools/nydusd",
        "--config",
        str(root / "config.json"),
        "--mountpoint",
        str(root / "mnt"),
        "--bootstrap",
        "/output/fixture/bootstrap",
        "--log-level",
        "warn",
    ],
    stdout=log,
    stderr=log,
)
try:
    deadline = start + 35
    while not os.path.ismount(root / "mnt"):
        if process.poll() is not None or time.monotonic() > deadline:
            raise RuntimeError("Nydus mount did not become ready")
        time.sleep(0.002)
    result["mount_seconds"] = time.monotonic() - start
    for name in ["startup.txt", "hot.bin"] + (["cold.bin"] if a.full else []):
        begin = time.monotonic()
        body = (root / "mnt" / name).read_bytes()
        digest = hashlib.sha256(body).hexdigest()
        assert digest == expected[name]["sha256"]
        result["files"][name] = {"bytes": len(body), "seconds": time.monotonic() - begin, "sha256": digest}
        if name == "hot.bin":
            result["usable_seconds"] = time.monotonic() - start
    result["total_seconds"] = time.monotonic() - start
    fields = pathlib.Path(f"/proc/{process.pid}/stat").read_text().rsplit(")", 1)[1].split()
    result["nydus_cpu_seconds"] = (int(fields[11]) + int(fields[12])) / os.sysconf("SC_CLK_TCK")
    result["nydus_status"] = [
        line
        for line in pathlib.Path(f"/proc/{process.pid}/status").read_text().splitlines()
        if line.startswith(("VmRSS:", "VmHWM:"))
    ]
    result["passed"] = True
finally:
    if os.path.ismount(root / "mnt"):
        subprocess.run([umount, str(root / "mnt")], check=True)
    process.terminate()
    process.wait(timeout=10)
    log.close()
    # Only this exited case's own disposable cache and generated configuration.
    shutil.rmtree(root)
pathlib.Path(f"/output/bench-{a.case}.json").write_text(json.dumps(result, indent=2))
print(json.dumps(result))
