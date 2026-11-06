#!/usr/bin/env python3
"""Expose the all-copies-unavailable boundary; this is not a recovery PASS gate.

Requires the isolated one-durable-copy fixture. It always restarts node A.
"""

import importlib.util
import json
import pathlib
import time

spec = importlib.util.spec_from_file_location("lab", pathlib.Path(__file__).parents[1] / "nydus/run.py")
lab = importlib.util.module_from_spec(spec)
spec.loader.exec_module(lab)

setup = """import sys,json
sys.path.append('/artifacts/scripts'); import check_http as c
manifest=c.publish_variant('last-copy-unavailable'); c.control()
assert c.request(c.url('a',manifest),'HEAD')[0]==200
assert c.read_range('a',0,4096,manifest)[0]==206
(c.OUT/'origin-recovery-manifest.txt').write_text(manifest)
print(manifest)
"""
manifest_id = lab.execute("mgmt", "python3", "-c", setup).strip()
lab.stop("node-a")
result = {"manifest": manifest_id, "durable_copies": 1, "origin_online": True, "attempts": []}
started = time.monotonic()
try:
    fresh = """import sys,json
sys.path.append('/artifacts/scripts'); import check_http as c
manifest=(c.OUT/'origin-recovery-manifest.txt').read_text()
response=c.read_range('b',1048576,4096,manifest)
print(json.dumps({'status':response[0],'length':len(response[2]),
 'error':response[2][:200].decode(errors='replace') if response[0]!=206 else ''}))
"""
    while time.monotonic() - started < 60:
        response = json.loads(lab.execute("mgmt", "python3", "-c", fresh))
        result["attempts"].append({"seconds": time.monotonic() - started, **response})
        if response["status"] == 206:
            break
        time.sleep(2)
    result["new_missing_range_available"] = response["status"] == 206
    old = fresh.replace("1048576,4096", "0,4096")
    result["committed_range_after_takeover"] = json.loads(lab.execute("mgmt", "python3", "-c", old))
    result["automatically_recovered"] = result["committed_range_after_takeover"]["status"] == 206
finally:
    lab.start("node-a")
    result["original_restarted"] = True
    (lab.ROOT / "evidence/origin-recovery-gap.json").write_text(json.dumps(result, indent=2))
print(json.dumps(result, indent=2))
