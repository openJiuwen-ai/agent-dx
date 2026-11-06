#!/usr/bin/env python3
"""Last-copy loss must recover committed bytes through the ordinary owner hook.

Uses the isolated one-copy fixture; always restarts its original node. No
whole-image download or separate integrity manifest is required.
"""

import importlib.util
import json
import pathlib
import time

spec = importlib.util.spec_from_file_location("lab", pathlib.Path(__file__).parents[1] / "nydus/run.py")
lab = importlib.util.module_from_spec(spec)
spec.loader.exec_module(lab)

CATALOG = pathlib.Path(__file__).with_name("catalog_probe.py").read_text()
before_records = json.loads(lab.execute("worker-1", "python3", "-c", CATALOG))["records"]

setup = """import sys,json
sys.path.append('/artifacts/scripts'); import check_http as c
manifest=c.publish_variant('recovery-last-copy'); c.control()
assert c.request(c.url('a',manifest),'HEAD')[0]==200
reference=c.request(c.BASE+'/blobs/'+c.BLOB['digest'])[2]
assert c.read_range('a',0,4096,manifest)[2]==reference[:4096]
(c.OUT/'origin-recovery-manifest.txt').write_text(manifest)
print(manifest)
"""
manifest = lab.execute("mgmt", "python3", "-c", setup).strip()
original_records = json.loads(lab.execute("worker-1", "python3", "-c", CATALOG))["records"]
original = {
    k: v["chunk"] for k, v in original_records.items() if k not in before_records and v["chunk"]["length"] == 4096
}
assert len(original) == 1, original
lab.stop("node-a")
result = {"manifest": manifest, "durable_copies": 1, "origin_online": True, "attempts": []}
started = time.monotonic()
try:
    probe = """import sys,json,hashlib
sys.path.append('/artifacts/scripts'); import check_http as c
manifest=(c.OUT/'origin-recovery-manifest.txt').read_text()
reference=c.request(c.BASE+'/blobs/'+c.BLOB['digest'])[2]
response=c.read_range('b',0,4096,manifest)
print(json.dumps({'status':response[0],'length':len(response[2]),
 'hash_matches':response[2]==reference[:4096],
 'error':response[2][:200].decode(errors='replace') if response[0]!=206 else ''}))
"""
    while time.monotonic() - started < 70:
        response = json.loads(lab.execute("mgmt", "python3", "-c", probe))
        result["attempts"].append({"seconds": time.monotonic() - started, **response})
        if response["status"] == 206:
            break
        time.sleep(2)
    assert response["status"] == 206 and response["hash_matches"], response
    result["committed_range_recovered"] = True
    restored = json.loads(lab.execute("worker-2", "python3", "-c", CATALOG))["records"]
    assert all(restored[k]["chunk"] == value for k, value in original.items())
    result["original_chunk_identity_preserved"] = original
    follow = """import sys,json
sys.path.append('/artifacts/scripts'); import check_http as c
manifest=(c.OUT/'origin-recovery-manifest.txt').read_text()
reference=c.request(c.BASE+'/blobs/'+c.BLOB['digest'])[2]
r=c.read_range('b',1048576,4096,manifest)
assert r[0]==206 and r[2]==reference[1048576:1052672], (r[0],r[2][:200])
before=len(c.bodies());c.control(mode='503')
try:
 r=c.read_range('c',0,4096,manifest)
 assert r[0]==206 and r[2]==reference[:4096],(r[0],r[2][:200])
 assert len(c.bodies())==before
 print(json.dumps({'new_range_after_restore':True,'cold_C_reuses_without_origin':True}))
finally:c.control()
"""
    result.update(json.loads(lab.execute("mgmt", "python3", "-c", follow)))
    result["passed"] = True
finally:
    lab.start("node-a")
    result["original_restarted"] = True
    (lab.ROOT / "evidence/origin-recovery.json").write_text(json.dumps(result, indent=2))
print(json.dumps(result, indent=2))
