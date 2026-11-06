#!/usr/bin/env python3
"""Matched native Nydus cold/peer/local starts; explicitly a single-VM experiment."""

import argparse
import json
import pathlib
import subprocess
import uuid
import sys

p = argparse.ArgumentParser(description=__doc__)
p.add_argument("--workspace", type=pathlib.Path, required=True)
p.add_argument("--adx-vm", required=True)
p.add_argument("--rounds", type=int, default=3)
p.add_argument(
    "--backends", nargs="+", choices=["registry", "afs", "dragonfly"], default=["registry", "afs", "dragonfly"]
)
p.add_argument("--dragonfly-policy", choices=["always", "auto"], default="always")
p.add_argument("--output", default="startup-benchmark.json")
a = p.parse_args()
root = a.workspace.resolve()
env = json.loads((root / "environment/open.json").read_text())
shared = pathlib.Path(env["output"])
results = []
run_id = uuid.uuid4().hex[:8]
if pathlib.PurePosixPath(a.output).name != a.output:
    raise SystemExit("output must be a basename")
for config in (root / "artifacts/dragonfly").glob("client-[1-4].yaml"):
    text = (
        config.read_text()
        .replace("schedulingPolicy: always", f"schedulingPolicy: {a.dragonfly_policy}")
        .replace("schedulingPolicy: auto", f"schedulingPolicy: {a.dragonfly_policy}")
    )
    config.write_text(text)


def execute(target_node, *command):
    r = subprocess.run(
        [a.adx_vm, "exec", "--session", env["session"], "--node", target_node, "--", *command],
        capture_output=True,
        text=True,
        timeout=90,
    )
    if r.returncode:
        raise RuntimeError(f"{target_node}: {r.stdout}\n{r.stderr}")
    return r.stdout


def rows():
    return (shared / "logs/origin.jsonl").read_text().splitlines()


def resource_snapshot():
    return {
        target: json.loads(execute(target, "python3", "/artifacts/scripts/process_resources.py"))
        for target in ["mgmt", "worker-1", "worker-2", "worker-3", "worker-4"]
    }


for iteration in range(a.rounds):
    backends = a.backends
    backends = backends[iteration % len(backends) :] + backends[: iteration % len(backends)]
    for backend in backends:
        if backend == "dragonfly":
            subprocess.run(
                [
                    sys.executable,
                    str(pathlib.Path(__file__).with_name("dragonfly.py")),
                    "--workspace",
                    str(root),
                    "--adx-vm",
                    a.adx_vm,
                    "reset",
                ],
                check=True,
            )
        manifest = execute(
            "mgmt",
            "python3",
            "-c",
            "import sys; sys.path.append('/artifacts/scripts'); import check_http as c; "
            f"print(c.publish_variant('bench-{run_id}-{iteration}-{backend}'))",
        ).strip()
        for state, node, ip in [
            ("cold", "worker-2", "10.203.0.21"),
            ("peer", "worker-3", "10.203.0.22"),
            ("local", "worker-2", "10.203.0.21"),
        ]:
            resources_before = resource_snapshot()
            begin = len(rows())
            case = f"{run_id}-{iteration}-{backend}-{state}"
            result = json.loads(
                execute(
                    node,
                    sys.executable,
                    "/artifacts/scripts/bench_startup.py",
                    "--backend",
                    backend,
                    "--ip",
                    ip,
                    "--case",
                    case,
                    "--manifest",
                    manifest,
                )
            )
            requests = [json.loads(line) for line in rows()[begin:]]
            blob_gets = [r for r in requests if r.get("method") == "GET" and "/blobs/" in r.get("path", "")]
            result.update(
                round=iteration,
                state=state,
                origin_requests=requests,
                origin_blob_bytes=sum(r.get("bytes", 0) for r in blob_gets),
                origin_blob_gets=len(blob_gets),
            )
            result["resources_before"] = resources_before
            result["resources_after"] = resource_snapshot()
            results.append(result)
            (root / "evidence" / a.output).write_text(
                json.dumps(
                    {
                        "run_id": run_id,
                        "dragonfly_policy": a.dragonfly_policy,
                        "scope": (
                            "four namespaces on one VM, no prefetch, native Nydus cache fresh each start, "
                            "bootstrap preinstalled"
                        ),
                        "results": results,
                    },
                    indent=2,
                )
            )
            print(
                json.dumps(
                    {
                        k: result[k]
                        for k in [
                            "backend",
                            "round",
                            "state",
                            "usable_seconds",
                            "origin_blob_bytes",
                            "origin_blob_gets",
                        ]
                    }
                ),
                flush=True,
            )
