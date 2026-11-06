#!/usr/bin/env python3
"""Corrupt/remove only this run's newly created disposable copy; require fallback."""

import argparse
import hashlib
import json
import pathlib
import subprocess
import time

p = argparse.ArgumentParser(description=__doc__)
p.add_argument("--workspace", type=pathlib.Path, required=True)
p.add_argument("--adx-vm", required=True)
p.add_argument("--mode", choices=["corrupt", "missing"], default="corrupt")
a = p.parse_args()
root = a.workspace.resolve()
env = json.loads((root / "environment/open.json").read_text())
session = env["session"]
name = f"distribution-{a.mode}.bin"


def run(node, code):
    r = subprocess.run(
        [a.adx_vm, "exec", "--session", session, "--node", node, "--", "python3", "-c", code],
        capture_output=True,
        text=True,
        timeout=55,
    )
    if r.returncode:
        raise RuntimeError(f"{node}: {r.stdout}\n{r.stderr}")
    return r.stdout


inventory = """import pathlib,json
r=pathlib.Path('/workspace/afs/state-replicated/dfs'); records={}; revision=0
if (r/'catalog.snapshot').exists():
 s=json.loads((r/'catalog.snapshot').read_text()); revision=s['revision']
 records={x['chunk']['id']:x for x in s['records']}
for line in (r/'catalog.wal').read_text().splitlines():
 t=json.loads(line)
 if t['revision'] <= revision: continue
 for k in t.get('removed',[]): records.pop(k,None)
 for x in t['records']: records[x['chunk']['id']]=x
print(json.dumps(records))
"""
before = json.loads(run("worker-2", inventory))
run(
    "worker-1",
    f"import os; p='/workspace/afs/mount/{name}'; f=open(p,'xb'); "
    "f.write(bytes(range(256))*8192); f.flush(); os.fsync(f.fileno()); f.close()",
)
reader = (
    f"import pathlib,hashlib,json; b=pathlib.Path('/workspace/afs/mount/{name}').read_bytes(); "
    "print(json.dumps({'bytes':len(b),'sha256':hashlib.sha256(b).hexdigest()}))"
)
expected = hashlib.sha256(bytes(range(256)) * 8192).hexdigest()
assert json.loads(run("worker-2", reader))["sha256"] == expected
after = json.loads(run("worker-2", inventory))
new = [
    v
    for k, v in after.items()
    if k not in before and v.get("read_cache_epoch") and v["stored_length"] == 2 * 1024 * 1024
]
assert len(new) == 1, new
record = new[0]
chunk = record["chunk"]["id"]
assert pathlib.PurePosixPath(chunk).name == chunk
physical = "/workspace/afs/state-replicated/dfs/chunks/" + chunk
if a.mode == "corrupt":
    run(
        "worker-2",
        f"import os; f=os.open({physical!r},os.O_WRONLY); "
        "assert os.pwrite(f,bytes(2097152),0)==2097152; os.fsync(f); os.close(f)",
    )
else:
    run("worker-2", f"import os; os.unlink({physical!r})")
start = time.monotonic()
result = json.loads(run("worker-3", reader))
assert result["sha256"] == expected, result
log = pathlib.Path(env["output"]) / "logs/node-b.log"
deadline = time.monotonic() + 15
reported = False
while time.monotonic() < deadline:
    reported = any(chunk in line and "dfs.local_corruption_reported" in line for line in log.read_text().splitlines())
    if reported:
        break
    time.sleep(0.2)
result.update(mode=a.mode, chunk=chunk, fallback_seconds=time.monotonic() - start, withdrawn=reported)
(root / f"evidence/cache-{a.mode}.json").write_text(json.dumps(result, indent=2))
print(json.dumps(result, indent=2))
assert reported, "bad disposable copy was not withdrawn by the existing maintenance worker"
