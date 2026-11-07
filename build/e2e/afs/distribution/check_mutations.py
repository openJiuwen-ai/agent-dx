#!/usr/bin/env python3
"""Ordinary file updates must supersede cached versions, including truncate holes."""

import argparse
import json
import pathlib
import subprocess

p = argparse.ArgumentParser(description=__doc__)
p.add_argument("--workspace", type=pathlib.Path, required=True)
p.add_argument("--adx-vm", required=True)
a = p.parse_args()
session = json.loads((a.workspace / "environment/open.json").read_text())["session"]


def execute(target_node, code):
    completed = subprocess.run(
        [a.adx_vm, "exec", "--session", session, "--node", target_node, "--", "python3", "-c", code],
        capture_output=True,
        text=True,
        timeout=55,
    )
    if completed.returncode:
        raise RuntimeError(f"{target_node}: {completed.stdout}\n{completed.stderr}")
    return completed.stdout


setup = "import os,pathlib; p='/workspace/afs/mount/distribution-one.bin'; "
execute(
    "worker-1",
    setup + "f=os.open(p,os.O_RDWR); assert os.pwrite(f,b'updated-version',12345)==15; os.fsync(f); os.close(f)",
)
for node in ["worker-2", "worker-3", "worker-4"]:
    execute(node, setup + "f=os.open(p,os.O_RDONLY); assert os.pread(f,15,12345)==b'updated-version'; os.close(f)")
execute(
    "worker-1",
    setup
    + "f=os.open(p,os.O_RDWR); os.ftruncate(f,65536); os.fsync(f); os.ftruncate(f,1048576); os.fsync(f); os.close(f)",
)
for node in ["worker-2", "worker-3", "worker-4"]:
    execute(
        node,
        setup + "f=os.open(p,os.O_RDONLY); assert os.fstat(f).st_size==1048576; "
        "assert os.pread(f,1048576-65536,65536)==bytes(1048576-65536); "
        "assert os.pread(f,15,12345)==b'updated-version'; os.close(f)",
    )
execute("worker-1", setup + "os.unlink(p)")
for node in ["worker-2", "worker-3", "worker-4"]:
    execute(node, setup + "assert not pathlib.Path(p).exists()")
result = {"updated_range": "pass", "truncate_and_sparse_extension": "pass", "unlink": "pass", "reader_nodes": 3}
(a.workspace / "evidence/ordinary-mutations.json").write_text(json.dumps(result, indent=2))
print(json.dumps(result, indent=2))
