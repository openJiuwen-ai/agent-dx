#!/usr/bin/env python3
"""Host-run owner fault: stop A mid-origin, B/C compete, resume stale A.

Requires the normal fixture and three running Nodes, with connector and DFS read
budgets set to 60 seconds for this case. Keep production lease settings unchanged.
"""

import concurrent.futures
import importlib.util
import json
import pathlib
import time

spec = importlib.util.spec_from_file_location("lab", pathlib.Path(__file__).with_name("run.py"))
lab = importlib.util.module_from_spec(spec)
spec.loader.exec_module(lab)
PREFIX = """import sys, json, time
sys.path.insert(0, '/artifacts/scripts')
import check_http as c
"""


def mgmt(code):
    return lab.execute("mgmt", "python3", "-c", PREFIX + code)


def signal_a(sig):
    lab.execute("worker-1", "bash", "-euc", f'kill -{sig} "$(cat /workspace/afs/run/pid)"')


manifest = mgmt(
    """c.control()
(c.OUT / 'race-origin-start-count.txt').write_text(str(len(c.bodies())))
m = c.publish_variant('paused-owner-race')
(c.OUT / 'race-manifest.txt').write_text(m)
assert c.request(c.url('a', m), 'HEAD')[0] == 200
r = c.read_range('a', 0, 4096, m)
assert r[0] == 206, r[0]
c.control(mode='hold', node='node-a')
(c.OUT / 'release-origin').unlink(missing_ok=True)
print(m)
"""
).strip()
started_rows = len((lab.SHARED / "logs/origin-start.jsonl").read_text().splitlines())
paused = False
with concurrent.futures.ThreadPoolExecutor(3) as pool:
    old = pool.submit(
        mgmt,
        """m = (c.OUT / 'race-manifest.txt').read_text()
status, _, body = c.request(c.url('a', m), headers={'Range': 'bytes=1048576-1052671'}, timeout=85)
print(json.dumps({'status': status, 'length': len(body), 'body': body[:200].decode(errors='replace')}))
""",
    )
    try:
        until = time.monotonic() + 10
        while time.monotonic() < until:
            rows = [
                json.loads(line)
                for line in (lab.SHARED / "logs/origin-start.jsonl").read_text().splitlines()[started_rows:]
            ]
            if any(r["node"] == "node-a" and r["range"] == "bytes=1048576-1052671" for r in rows):
                break
            time.sleep(0.05)
        else:
            raise AssertionError("A never entered the held origin request")
        signal_a("STOP")
        paused = True
        start = time.monotonic()
        print("A paused inside origin; B and C now race for expired ownership", flush=True)

        def contender(node):
            return json.loads(
                mgmt(
                    f"""m = (c.OUT / 'race-manifest.txt').read_text()
attempts = []
end = time.monotonic() + 75
while time.monotonic() < end:
    t = time.monotonic()
    r = c.read_range('{node}', 1048576, 4096, m)
    attempts.append({{'status': r[0], 'seconds': time.monotonic() - t}})
    if r[0] == 206:
        break
    time.sleep(0.5)
assert r[0] == 206, attempts
expected = c.request(c.BASE + '/blobs/' + c.BLOB['digest'], headers={{'Range': 'bytes=1048576-1052671'}})[2]
assert r[2] == expected
print(json.dumps({{'node': '{node}', 'attempts': attempts}}))
"""
                )
            )

        b = pool.submit(contender, "b")
        c = pool.submit(contender, "c")
        results = [b.result(), c.result()]
        takeover_seconds = time.monotonic() - start
        assert takeover_seconds < 55, takeover_seconds
        print("Both contenders read valid committed bytes; resuming old A", flush=True)
        (lab.SHARED / "release-origin").touch()
        signal_a("CONT")
        paused = False
        old_result = json.loads(old.result())
        assert old_result["status"] not in (200, 206), old_result
        verification = json.loads(
            mgmt(
                """m = (c.OUT / 'race-manifest.txt').read_text()
before = len(c.bodies())
c.control(mode='503')
expected = c.request(c.BASE + '/blobs/' + c.BLOB['digest'], headers={'Range': 'bytes=1048576-1052671'})[2]
for n in ['b', 'c']:
    r = c.read_range(n, 1048576, 4096, m)
    assert r[0] == 206 and r[2] == expected, (n, r[0])
assert len(c.bodies()) == before
first = int((c.OUT / 'race-origin-start-count.txt').read_text())
rows = [r for r in c.bodies()[first:] if r.get('range') == 'bytes=1048576-1052671' and r.get('mode') != 'hold']
assert len(rows) == 1 and rows[0]['node'] in ['node-b', 'node-c'], rows
print(json.dumps({'new_owner': rows[0]['node'], 'new_owner_fetches': len(rows), 'reuse_origin_fetches': 0}))
"""
            )
        )
        report = dict(
            passed=True,
            manifest=manifest,
            takeover_seconds=takeover_seconds,
            contenders=results,
            old_owner_result=old_result,
            **verification,
        )
        (lab.ROOT / "evidence/owner-race.json").write_text(json.dumps(report, indent=2))
        print(json.dumps(report, indent=2), flush=True)
    finally:
        (lab.SHARED / "release-origin").touch()
        if paused:
            signal_a("CONT")
        mgmt("c.control()")
