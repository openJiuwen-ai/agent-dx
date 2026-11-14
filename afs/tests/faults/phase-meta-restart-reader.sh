#!/usr/bin/env bash
# Round 2 C-1 E3 (MODE=r2, COPIES_R2=1): node-b holds a handle on V1 (registered in Meta's memory only); node-a
# writes V2; Meta restarts, which loses every in-memory registration, and GC passes start at once. Until each
# live session has reported its readers to the new Meta, GC keeps every version (the restart gate); node-b's
# report restores its reader, so V1 survives every later pass and node-b reads it back unchanged. Closing the
# handle then lets GC collect V1.
set -u
python3 -u - "$ROOT" "$OUT" "$BIN" <<'PY'
import glob, json, mmap, os, signal, subprocess, sys, time, urllib.request

root, out, bin_dir = sys.argv[1:4]
a, b = (os.path.join(root, d, "restart") for d in ("mnt", "mnt-b"))
size = 1 << 20
ok = True

def check(label, condition):
    global ok
    print(f"[{'ok' if condition else 'FAIL'}] {label}", flush=True)
    ok &= bool(condition)

def chunks():
    return {p for p in glob.glob(f"{root}/node/state/**/*", recursive=True)
            if "/chunks/" in p and os.path.isfile(p) and os.stat(p).st_size >= 256 * 1024}

def read(fd):
    with mmap.mmap(-1, size) as buffer:
        n = os.preadv(fd, [buffer], 0)
        return bytes(buffer[:n])

def gate():
    try:
        text = urllib.request.urlopen("http://127.0.0.1:7401/metrics", timeout=5).read().decode()
    except OSError:
        return None
    for line in text.splitlines():
        if line.startswith("afs_meta_dfs_gc_reader_gate_closed "):
            return float(line.split()[1])
    return None

before = chunks()
with open(a, "wb", buffering=0) as f:
    f.write(b"1" * size); os.fsync(f.fileno())
v1 = chunks() - before
check("V1's chunks identified on node-a", bool(v1))
time.sleep(2)
reader = os.open(b, os.O_RDONLY | os.O_DIRECT)
check("node-b reads V1", read(reader) == b"1" * size)
with open(a, "r+b", buffering=0) as f:
    f.write(b"2" * size); os.fsync(f.fileno())
time.sleep(12)  # GC passes while the original Meta still knows the reader
check("V1 kept by the in-memory reader before the restart", all(os.path.exists(p) for p in v1))
pid = int(open(os.path.join(root, "meta.pid")).read())
os.kill(pid, signal.SIGTERM)
for _ in range(240):
    try:
        os.kill(pid, 0); time.sleep(0.25)
    except ProcessLookupError:
        break
log = open(os.path.join(out, "meta.log"), "ab")
meta = subprocess.Popen([os.path.join(bin_dir, "afs-meta"), "--config", os.path.join(root, "meta.toml")],
                        stdout=log, stderr=subprocess.STDOUT)
open(os.path.join(root, "meta.pid"), "w").write(str(meta.pid))
for _ in range(120):
    try:
        urllib.request.urlopen("http://127.0.0.1:7401/health", timeout=2); break
    except OSError:
        time.sleep(0.25)
print("Meta restarted", flush=True)
seen_closed = False
end = time.monotonic() + 30
while time.monotonic() < end:
    seen_closed |= gate() == 1.0
    time.sleep(0.25)
print(f"restart gate seen closed: {seen_closed}; now: {gate()}", flush=True)
check("V1 survives GC after the restart (node-b reported its reader)", all(os.path.exists(p) for p in v1))
check("node-b reads V1 unchanged after the restart", read(reader) == b"1" * size)
os.close(reader)
end = time.monotonic() + 60
while time.monotonic() < end and any(os.path.exists(p) for p in v1):
    time.sleep(0.5)
check("closing the handle lets GC collect V1", not any(os.path.exists(p) for p in v1))
with open(b, "rb") as f:
    check("a fresh open on node-b reads V2", f.read() == b"2" * size)
sys.exit(0 if ok else 1)
PY
