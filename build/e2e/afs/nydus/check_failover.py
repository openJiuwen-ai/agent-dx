import importlib.util
import json
import logging
import pathlib
import sys
import time

spec = importlib.util.spec_from_file_location("lab", pathlib.Path(__file__).with_name("run.py"))
lab = importlib.util.module_from_spec(spec)
spec.loader.exec_module(lab)
SETUP = """import sys, json
sys.path.insert(0, '/artifacts/scripts')
import check_http as c
manifest = c.publish_variant('owner-failover')
(c.OUT / 'failover-manifest.txt').write_text(manifest)
r = c.request(c.url('a', manifest), 'HEAD')
c.require(r[0] == 200, r)
r = c.read_range('a', 0, 4096, manifest)
c.require(r[0] == 206, (r[0], r[2][:300]))
print(manifest)
"""
logging.basicConfig(level=logging.INFO, format="%(message)s", stream=sys.stdout)
logging.info("%s", lab.execute("mgmt", "python3", "-c", SETUP).rstrip())
started = time.monotonic()
lab.stop("node-a")
try:
    probe = """import sys, json
sys.path.insert(0, '/artifacts/scripts')
import check_http as c
manifest = (c.OUT / 'failover-manifest.txt').read_text()
before = len(c.bodies())
c.control(mode='503')
try:
    r = c.read_range('b', 0, 4096, manifest)
finally:
    c.control()
c.require(r[0] == 206, (r[0], r[2][:300]))
c.require(len(c.bodies()) == before, "Committed range contacted Registry")
print('committed range survives owner stop without Registry GET')
"""
    logging.info("%s", lab.execute("mgmt", "python3", "-c", probe).rstrip())
    probe = """import sys, json
sys.path.insert(0, '/artifacts/scripts')
import check_http as c
manifest = (c.OUT / 'failover-manifest.txt').read_text()
r = c.read_range('b', 1048576, 4096, manifest)
body = r[2][:200].decode(errors='replace') if r[0] != 206 else ''
print(json.dumps({'status': r[0], 'length': len(r[2]), 'body': body}))
"""
    attempts = []
    while time.monotonic() - started < 100:
        r = json.loads(lab.execute("mgmt", "python3", "-c", probe))
        attempts.append({"elapsed": round(time.monotonic() - started, 2), **r})
        logging.info("%s", attempts[-1])
        if r["status"] == 206:
            break
        time.sleep(2)
    if not attempts or attempts[-1]["status"] != 206:
        raise AssertionError(attempts)
    VERIFY = """import sys, json, urllib.request
sys.path.insert(0, '/artifacts/scripts')
import check_http as c
manifest = (c.OUT / 'failover-manifest.txt').read_text()
r = c.read_range('b', 1048576, 4096, manifest)
expected = c.request(c.BASE + '/blobs/' + c.BLOB['digest'], headers={'Range': 'bytes=1048576-1052671'})[2]
c.require(r[2] == expected, "Recovered range returned different bytes")
recent = [r for r in c.bodies() if r.get('range') == 'bytes=1048576-1052671']
c.require(recent and recent[-1]['node'] == 'node-b', recent)
print(json.dumps({'passed': True, 'new_owner_origin': recent[-1]}))
"""
    result = json.loads(lab.execute("mgmt", "python3", "-c", VERIFY))
    result["attempts"] = attempts
    (lab.ROOT / "evidence/owner-failover.json").write_text(json.dumps(result, indent=2))
    logging.info("FAILOVER PASS")
finally:
    lab.start("node-a")
