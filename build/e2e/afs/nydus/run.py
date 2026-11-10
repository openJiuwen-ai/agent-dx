#!/usr/bin/env python3
"""Host orchestration. Only touches this project's registered session."""

import argparse
import json
import logging
import os
import pathlib
import shutil
import subprocess
import sys
import time

parser = argparse.ArgumentParser(description=__doc__)
parser.add_argument("--workspace", required=True, type=pathlib.Path)
parser.add_argument("--adx-vm", default=os.environ.get("ADX_VM", "adx-vm"))
options, actions = parser.parse_known_args()
ROOT = options.workspace.resolve()
CLI = shutil.which(options.adx_vm)
if CLI is None:
    parser.error("adx-vm executable not found")
SESSION = json.loads((ROOT / "environment/open.json").read_text())["session"]
SHARED = pathlib.Path(json.loads((ROOT / "environment/open.json").read_text())["output"])


def execute(node, *args):
    r = subprocess.run(
        [CLI, "exec", "--session", SESSION, "--node", node, "--", *map(str, args)], capture_output=True, text=True
    )
    if r.returncode:
        raise RuntimeError(f"{node} exit={r.returncode}\n{r.stdout}\n{r.stderr}")
    return r.stdout


def start(node):
    guest = {"meta": "mgmt", "node-a": "worker-1", "node-b": "worker-2", "node-c": "worker-3", "node-d": "worker-4"}[
        node
    ]
    binary = "afs/afs-meta" if node == "meta" else "afs/afs-node"
    execute(
        guest,
        "bash",
        "-euc",
        f"""mkdir -p /workspace/afs/run /workspace/afs/mount /output/logs; test ! -f /workspace/afs/run/pid; \
nohup /artifacts/{binary} --config /artifacts/afs-configs/{node}.toml >>/output/logs/{node}.log 2>&1 \
</dev/null & echo $! >/workspace/afs/run/pid""",
    )
    time.sleep(1)
    if node != "meta":
        execute(
            guest,
            "bash",
            "-euc",
            "for i in $(seq 1 40); do mountpoint -q /workspace/afs/mount && exit 0; sleep 1; done; exit 1",
        )


def stop(node):
    guest = {"meta": "mgmt", "node-a": "worker-1", "node-b": "worker-2", "node-c": "worker-3", "node-d": "worker-4"}[
        node
    ]
    execute(
        guest,
        "bash",
        "-euc",
        (
            'if test -f /workspace/afs/run/pid; then kill -TERM "$(cat /workspace/afs/run/pid)"; for i '
            'in $(seq 1 40); do if ! kill -0 "$(cat /workspace/afs/run/pid)" 2>/dev/null; then rm '
            "/workspace/afs/run/pid; exit 0; fi; sleep 1; done; exit 1; fi"
        ),
    )


def mount(guest, case, backend):
    cfgpath = ROOT / "artifacts/nydus-configs"
    cfgpath.mkdir(exist_ok=True)
    active_manifest = SHARED / "fixture/active-manifest.txt"
    backendcfg = (
        {
            "type": "http-proxy",
            "http-proxy": {
                "addr": "http://127.0.0.1:7401",
                "path": f"/nydus/lab/{active_manifest.read_text().strip()}/blobs",
                "timeout": 30,
                "retry_limit": 0,
            },
        }
        if backend == "afs"
        else {
            "type": "registry",
            "registry": {"scheme": "http", "host": "10.203.0.10:5000", "repo": "lab/nydus", "retry_limit": 0},
        }
    )
    cfg = {
        "version": 2,
        "backend": backendcfg,
        "cache": {
            "type": "blobcache",
            "validate": True,
            "filecache": {"work_dir": f"/workspace/nydus/{case}/cache"},
            "prefetch": {"enable": False},
        },
        "rafs": {"mode": "direct", "validate": False, "batch_size": 1048576, "prefetch": {"enable": False}},
    }
    (cfgpath / (case + ".json")).write_text(json.dumps(cfg, indent=2))
    logging.info(
        "%s",
        execute(
            guest,
            "bash",
            "-euc",
            f"""test ! -e /workspace/nydus/{case}; mkdir -p /workspace/nydus/{case}/cache \
/workspace/nydus/{case}/mnt; test -z "$(ls -A /workspace/nydus/{case}/cache)"; nohup \
/artifacts/tools/nydusd --config /artifacts/nydus-configs/{case}.json --mountpoint \
/workspace/nydus/{case}/mnt --bootstrap /output/fixture/bootstrap --log-level debug \
>/output/logs/nydus-{case}.log 2>&1 </dev/null & echo $! >/workspace/nydus/{case}/pid; for i in \
$(seq 1 30); do mountpoint -q /workspace/nydus/{case}/mnt && exit 0; sleep 1; done; tail -60 \
/output/logs/nydus-{case}.log; exit 1""",
        ).rstrip(),
    )


def probe(guest, case):
    code = """import hashlib, json, pathlib, subprocess
p = pathlib.Path('/workspace/nydus/CASE/mnt')
expected = json.loads(pathlib.Path('/output/fixture/expected-files.json').read_text())
results = {}
for name in ['startup.txt', 'hot.bin']:
    data = (p / name).read_bytes()
    h = hashlib.sha256(data).hexdigest()
    if h != expected[name]['sha256']:
        raise AssertionError(f'Checksum mismatch for {name}')
    results[name] = {'size': len(data), 'sha256': h}
results['mount'] = subprocess.check_output(['findmnt', '-rn', '-T', str(p), '-o', 'FSTYPE,SOURCE'], text=True).strip()
print(json.dumps(results, indent=2))
""".replace("CASE", case)
    result = execute(guest, "python3", "-c", code)
    (ROOT / "evidence" / f"{case}-read.json").write_text(result)
    logging.info("%s", result.rstrip())


def unmount(guest, case):
    logging.info(
        "%s",
        execute(
            guest,
            "bash",
            "-euc",
            f'umount /workspace/nydus/{case}/mnt; kill -TERM "$(cat /workspace/nydus/{case}/pid)" 2>/dev/null || true',
        ).rstrip(),
    )


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO, format="%(message)s", stream=sys.stdout)
    action = actions[0]
    action_args = actions[1:]
    if action == "start":
        start(*action_args)
    elif action == "stop":
        stop(*action_args)
    elif action == "mount":
        mount(*action_args)
    elif action == "probe":
        probe(*action_args)
    elif action == "unmount":
        unmount(*action_args)
