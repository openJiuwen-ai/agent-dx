#!/usr/bin/env python3
"""Read an ordinary file with its original durable node paused; always resume it."""

import argparse
import json
import pathlib
import subprocess
import time

p = argparse.ArgumentParser(description=__doc__)
p.add_argument("--workspace", type=pathlib.Path, required=True)
p.add_argument("--adx-vm", required=True)
p.add_argument("--file", default="distribution-one.bin")
p.add_argument("--sha256", default="fbbab289f7f94b25736c58be46a994c441fd02552cc6022352e3d86d2fab7c83")
p.add_argument("--multi-source", action="store_true")
args = p.parse_args()
root = args.workspace.resolve()
session = json.loads((root / "environment/open.json").read_text())["session"]
if pathlib.PurePosixPath(args.file).name != args.file:
    raise SystemExit("file must be a basename")


def execute(target_node, *command):
    completed = subprocess.run(
        [args.adx_vm, "exec", "--session", session, "--node", target_node, "--", *command],
        capture_output=True,
        text=True,
        timeout=55,
    )
    if completed.returncode:
        raise RuntimeError(f"{target_node}: {completed.stdout}\n{completed.stderr}")
    return completed.stdout


def metrics(node, address):
    return execute(
        node,
        "python3",
        "-c",
        f"import urllib.request; print(urllib.request.urlopen('http://{address}:7401/metrics').read().decode())",
    )


signal = (
    'test "$(cat /proc/$(cat /workspace/afs/run/pid)/comm)" = afs-node; kill -SIGNAL "$(cat /workspace/afs/run/pid)"'
)
result = {"file": args.file, "started": time.time(), "original_node": "node-a", "reader": "node-c"}
before = metrics("worker-2", "10.203.0.21")
other_before = (
    {n: metrics(n, ip) for n, ip in [("worker-1", "10.203.0.20"), ("worker-3", "10.203.0.22")]}
    if args.multi_source
    else {}
)
try:
    if not args.multi_source:
        execute("worker-1", "bash", "-euc", signal.replace("SIGNAL", "STOP"))
    code = (
        "import pathlib,hashlib,json,time; t=time.monotonic(); "
        f"b=pathlib.Path('/workspace/afs/mount/{args.file}').read_bytes(); "
        "print(json.dumps({'bytes':len(b),'sha256':hashlib.sha256(b).hexdigest(),'seconds':time.monotonic()-t}))"
    )
    result["read"] = json.loads(execute("worker-4" if args.multi_source else "worker-3", "python3", "-c", code))
    assert result["read"]["sha256"] == args.sha256, result["read"]
except Exception as exc:
    result["error"] = str(exc)
finally:
    if not args.multi_source:
        execute("worker-1", "bash", "-euc", signal.replace("SIGNAL", "CONT"))
        result["original_resumed"] = True
after = metrics("worker-2", "10.203.0.21")
evidence = root / "evidence"
if args.multi_source:
    evidence = evidence / "multi-source"
    evidence.mkdir(exist_ok=True)
    result["reader"] = "node-d"
    result["original_paused"] = False
    metric = 'afs_dfs_payload_bytes_total{direction="send",operation="read",transport="grpc"} '

    def sent(metric_text):
        return int(next(line[len(metric) :] for line in metric_text.splitlines() if line.startswith(metric)))

    deltas = {"worker-2": sent(after) - sent(before)}
    for n, ip in [("worker-1", "10.203.0.20"), ("worker-3", "10.203.0.22")]:
        snapshot = metrics(n, ip)
        deltas[n] = sent(snapshot) - sent(other_before[n])
        (evidence / f"{n}-before.prom").write_text(other_before[n])
        (evidence / f"{n}-after.prom").write_text(snapshot)
    result["source_bytes"] = deltas
    if sum(value > 0 for value in deltas.values()) < 2:
        result["error"] = "fewer than two actual serving nodes"
(evidence / "replica-b-before.prom").write_text(before)
(evidence / "replica-b-after.prom").write_text(after)
(evidence / "replica-probe.json").write_text(json.dumps(result, indent=2))
print(json.dumps(result, indent=2))
if "error" in result:
    raise SystemExit(1)
