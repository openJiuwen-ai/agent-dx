#!/usr/bin/env python3
"""Fill an ordinary file beyond the disposable-copy budget and check recovery."""

import argparse
import json
import pathlib
import subprocess
import uuid


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--workspace", type=pathlib.Path, required=True)
    parser.add_argument("--adx-vm", required=True)
    args = parser.parse_args()
    session = json.loads((args.workspace / "environment/open.json").read_text())["session"]

    def execute(node, code):
        completed = subprocess.run(
            [args.adx_vm, "exec", "--session", session, "--node", node, "--", "python3", "-c", code],
            capture_output=True,
            text=True,
            check=True,
            timeout=180,
        )
        return completed.stdout

    filename = "distribution-pressure-" + uuid.uuid4().hex + ".bin"
    path = "/workspace/afs/mount/" + filename
    writer = f"""import os,hashlib,json
digest=hashlib.sha256()
with open({path!r},'xb',buffering=0) as output:
 for index in range(80):
  body=os.urandom(1048576); assert output.write(body)==len(body); digest.update(body)
 os.fsync(output.fileno())
print(json.dumps({{'sha256':digest.hexdigest()}}))
"""
    # The workload retains only one block in memory; no host-side body copy.
    expected = json.loads(execute("worker-1", writer))["sha256"]
    reader = f"""import hashlib,json,time
start=time.monotonic(); digest=hashlib.sha256()
with open({path!r},'rb',buffering=0) as source:
 while True:
  body=source.read(1048576)
  if not body: break
  digest.update(body)
print(json.dumps({{'sha256':digest.hexdigest(),'seconds':time.monotonic()-start}}))
"""
    read = json.loads(execute("worker-2", reader))
    assert read["sha256"] == expected
    inventory = """import pathlib,json
root=pathlib.Path('/workspace/afs/state-replicated/dfs'); records={}; revision=0
if (root/'catalog.snapshot').exists():
 snapshot=json.loads((root/'catalog.snapshot').read_text()); revision=snapshot['revision']
 records={item['chunk']['id']:item for item in snapshot['records']}
for line in (root/'catalog.wal').read_text().splitlines():
 txn=json.loads(line)
 if txn['revision']<=revision: continue
 for key in txn.get('removed',[]): records.pop(key,None)
 for item in txn['records']: records[item['chunk']['id']]=item
cached=[item for item in records.values() if item.get('read_cache_epoch',0)>0]
print(json.dumps({'cache_charge':sum(item['stored_length']+4096 for item in cached),'copies':len(cached)}))
"""
    state = json.loads(execute("worker-2", inventory))
    assert 0 < state["cache_charge"] <= 64 * 1048576, state
    # A fresh reader avoids mistaking B's FUSE page cache for chunk-store recovery.
    reread = json.loads(execute("worker-3", reader))
    assert reread["sha256"] == expected
    execute("worker-1", f"import os; os.unlink({path!r})")
    result = {"bytes": 80 * 1048576, "first": read, "after": state, "reread": reread, "passed": True}
    (args.workspace / "evidence/cache-pressure.json").write_text(json.dumps(result, indent=2))
    print(json.dumps(result, indent=2))


if __name__ == "__main__":
    main()
