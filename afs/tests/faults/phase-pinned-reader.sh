#!/usr/bin/env bash
# R81 F2 scenario: node-a keeps one file open and commits it 60 times (each round rewrites one 1 MiB region and
# fsyncs), node-b holds a reader opened after round 10 (a remote reader: one replica, on node-a), and GC runs all
# along. The reader's later reads must return exactly what it saw at open; a fresh open must see the newest data;
# Meta must not have kept every version.
set -u
python3 - "$ROOT/mnt" "$ROOT/mnt-b" <<'PY'
import os, sys, time, hashlib, json, urllib.request
a, b = sys.argv[1], sys.argv[2]
MiB = 1 << 20
def drop(): open("/proc/sys/vm/drop_caches", "w").write("3\n")
def whole(fd):
    out = bytearray(); off = 0
    while True:
        chunk = os.pread(fd, MiB, off)
        if not chunk: return bytes(out)
        out += chunk; off += len(chunk)
def store(): return json.loads(urllib.request.urlopen("http://127.0.0.1:7401/v1/store").read())["entities"]
w = os.open(a + "/pinned", os.O_CREAT | os.O_RDWR, 0o644)
os.write(w, os.urandom(8 * MiB)); os.fsync(w)
reader = None; expected = None; ok = True
for r in range(60):
    os.pwrite(w, os.urandom(MiB), (r % 8) * MiB); os.fsync(w)
    if r == 10:
        time.sleep(1)
        reader = os.open(b + "/pinned", os.O_RDONLY); drop()
        expected = hashlib.sha256(whole(reader)).hexdigest()
        print("reader opened on node-b after round 10", flush=True)
    if r % 10 == 9:
        s = store(); print(f"round {r+1}: versions={s.get('dfs_file_version')} chunks={s.get('dfs_chunk')}", flush=True)
    time.sleep(0.5)
time.sleep(15)  # several GC passes after the last commit
drop()
again = hashlib.sha256(whole(reader)).hexdigest()
print("pinned reader after 50 more commits and GC:", "same as at open" if again == expected else "DIFFERENT", flush=True)
ok &= again == expected
latest = hashlib.sha256(whole(w)).hexdigest()
fresh = os.open(b + "/pinned", os.O_RDONLY); drop()
seen = hashlib.sha256(whole(fresh)).hexdigest()
print("fresh open on node-b:", "newest data" if seen == latest else "STALE", flush=True)
ok &= seen == latest
s = store(); print(f"end: versions={s.get('dfs_file_version')} layouts={s.get('dfs_layout_root')} chunks={s.get('dfs_chunk')}", flush=True)
os.close(fresh); os.close(reader); os.close(w)
time.sleep(15)
s = store(); print(f"after close: versions={s.get('dfs_file_version')} chunks={s.get('dfs_chunk')}", flush=True)
sys.exit(0 if ok else 1)
PY
