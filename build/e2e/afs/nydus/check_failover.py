import importlib.util, json, time, pathlib

spec = importlib.util.spec_from_file_location("lab", pathlib.Path(__file__).with_name("run.py"))
lab = importlib.util.module_from_spec(spec)
spec.loader.exec_module(lab)
setup = """import sys, json
sys.path.insert(0, '/artifacts/scripts')
import check_http as c
manifest = c.publish_variant('owner-failover')
(c.OUT / 'failover-manifest.txt').write_text(manifest)
r = c.request(c.url('a', manifest), 'HEAD')
assert r[0] == 200, r
r = c.read_range('a', 0, 4096, manifest)
assert r[0] == 206, (r[0], r[2][:300])
print(manifest)
"""
print(lab.execute("mgmt", "python3", "-c", setup), flush=True)
started = time.monotonic()
lab.stop("node-a")
try:
    probe = """import sys, json
sys.path.insert(0, '/artifacts/scripts')
import check_http as c
manifest = (c.OUT / 'failover-manifest.txt').read_text()
before = len(c.bodies())
c.control(mode='503')
r = c.read_range('b', 0, 4096, manifest)
c.control()
assert r[0] == 206, (r[0], r[2][:300])
assert len(c.bodies()) == before
print('committed range survives owner stop without Registry GET')
"""
    print(lab.execute("mgmt", "python3", "-c", probe), flush=True)
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
        print(attempts[-1], flush=True)
        if r["status"] == 206:
            break
        time.sleep(2)
    assert attempts[-1]["status"] == 206, attempts
    verify = """import sys, json, urllib.request
sys.path.insert(0, '/artifacts/scripts')
import check_http as c
manifest = (c.OUT / 'failover-manifest.txt').read_text()
r = c.read_range('b', 1048576, 4096, manifest)
expected = c.request(c.BASE + '/blobs/' + c.BLOB['digest'], headers={'Range': 'bytes=1048576-1052671'})[2]
assert r[2] == expected
recent = [r for r in c.bodies() if r.get('range') == 'bytes=1048576-1052671']
assert recent[-1]['node'] == 'node-b', recent
print(json.dumps({'passed': True, 'new_owner_origin': recent[-1]}))
"""
    result = json.loads(lab.execute("mgmt", "python3", "-c", verify))
    result["attempts"] = attempts
    (lab.ROOT / "evidence/owner-failover.json").write_text(json.dumps(result, indent=2))
    print("FAILOVER PASS", flush=True)
finally:
    lab.start("node-a")
