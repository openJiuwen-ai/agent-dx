#!/usr/bin/env bash
# Round 2 C-1 (MODE=native): a file unlinked while open and reopened through /proc/self/fd (an open by inode of
# an inode with no links) is pinned with a durable reference, as before C-1: Meta refuses to register it in
# memory. GC passes while it is open must keep its content; closing every descriptor lets GC collect it.
set -u
python3 -u - "$ROOT" <<'PY'
import glob, os, sys, time

root = sys.argv[1]

def reader_pins(port):
    import urllib.request
    text = urllib.request.urlopen(f"http://127.0.0.1:{port}/metrics", timeout=5).read().decode()
    out = {}
    for line in text.splitlines():
        if line.startswith("afs_dfs_reader_references_total{"):
            kind = line.split('kind="', 1)[1].split('"', 1)[0]
            out[kind] = float(line.rsplit(" ", 1)[1])
    return out
path = os.path.join(root, "mnt", "unlinked")
size = 1 << 20
ok = True

def check(label, condition):
    global ok
    print(f"[{'ok' if condition else 'FAIL'}] {label}", flush=True)
    ok &= bool(condition)

def chunks():
    return {p for p in glob.glob(f"{root}/node/state/**/*", recursive=True)
            if "/chunks/" in p and os.path.isfile(p) and os.stat(p).st_size >= 256 * 1024}

before = chunks()
with open(path, "wb", buffering=0) as f:
    f.write(b"u" * size); os.fsync(f.fileno())
data = chunks() - before
check("the file's chunks identified", bool(data))
first = os.open(path, os.O_RDONLY)
pins = reader_pins(7501)
os.unlink(path)
again = os.open(f"/proc/self/fd/{first}", os.O_RDONLY)
after = reader_pins(7501)
print("open pins by kind around the reopen:", pins, "->", after, flush=True)
check("the reopen of the unlinked inode was pinned durably", after.get("durable", 0) > pins.get("durable", 0))
os.close(first)
time.sleep(15)  # GC passes with only the reopened descriptor left
check("the unlinked file's chunks survive while it is open", all(os.path.exists(p) for p in data))
check("the reopened descriptor reads the content", os.pread(again, size, 0) == b"u" * size)
os.close(again)
end = time.monotonic() + 60
while time.monotonic() < end and any(os.path.exists(p) for p in data):
    time.sleep(0.5)
check("closing it lets GC collect the file", not any(os.path.exists(p) for p in data))
sys.exit(0 if ok else 1)
PY
