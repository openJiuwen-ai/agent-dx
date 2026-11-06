#!/usr/bin/env python3
"""Read-only test probe of catalog checkpoint plus complete WAL transactions."""

import json, pathlib

p = pathlib.Path("/workspace/afs/state-replicated/dfs")
s = json.loads((p / "catalog.snapshot").read_text()) if (p / "catalog.snapshot").exists() else {}
records = {r["chunk"]["id"]: r for r in s.get("records", [])}
intents = {i["copy"]["chunk_id"]: i for i in s.get("cache_intents", [])}
for line in (p / "catalog.wal").read_text().splitlines():
    try:
        t = json.loads(line)
    except json.JSONDecodeError:
        continue
    if t["revision"] <= s.get("revision", 0):
        continue
    for i in t.get("cache_acks", []):
        if intents.get(i["copy"]["chunk_id"]) == i:
            intents.pop(i["copy"]["chunk_id"], None)
    for i in t.get("cache_updates", []):
        intents[i["copy"]["chunk_id"]] = i
    for c in t.get("removed", []):
        records.pop(c, None)
    for r in t.get("records", []):
        records[r["chunk"]["id"]] = r
print(json.dumps({"records": records, "intents": intents}))
