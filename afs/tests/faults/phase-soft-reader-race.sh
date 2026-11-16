#!/usr/bin/env bash
# Round 2 C-1 E1 (MODE=r2, COPIES_R2=1): node-a rewrites one file in place ~5 times a second while node-b opens
# it read-only (O_DIRECT), holds it a moment and reads it whole, about 400 times; GC runs all along (grace 0).
# Every read must return one version's bytes in full (the file is one byte value throughout), never an error:
# the version an open was answered with stays protected by its in-memory reader registration until close.
# Meta may widen the registration window with AFS_DIAG_READER_REGISTER_DELAY_MS (diagnostics builds); with
# AFS_DIAG_READER_REGISTER_AFTER_VIEW=1 Meta takes the validation view first (the unsafe order): that control
# arm is expected to fail, and a run of it that passes is reported as "not reproduced".
set -u
python3 -u - "$ROOT" "$OUT" <<'PY'
import glob, mmap, os, random, sys, threading, time

root, out = sys.argv[1], sys.argv[2]

def reader_pins(port):
    import urllib.request
    text = urllib.request.urlopen(f"http://127.0.0.1:{port}/metrics", timeout=5).read().decode()
    out = {}
    for line in text.splitlines():
        if line.startswith("afs_dfs_reader_references_total{"):
            kind = line.split('kind="', 1)[1].split('"', 1)[0]
            out[kind] = float(line.rsplit(" ", 1)[1])
    return out
a, b = (os.path.join(root, d, "soft-race") for d in ("mnt", "mnt-b"))
size = 1 << 20
stop = threading.Event()
rounds = [0]

def writer():
    fd = os.open(a, os.O_CREAT | os.O_RDWR, 0o644)
    r = 0
    while not stop.is_set():
        r += 1
        assert os.pwrite(fd, bytes([r % 251 + 1]) * size, 0) == size
        os.fsync(fd)
        rounds[0] = r
        time.sleep(0.2)
    os.close(fd)

with open(a, "wb") as f:
    f.write(b"\x01" * size)
time.sleep(2)
t = threading.Thread(target=writer, daemon=True)
t.start()
opens = errors = torn = 0
deadline = time.monotonic() + 90
buffer = mmap.mmap(-1, size)
while time.monotonic() < deadline and opens < 400:
    try:
        fd = os.open(b, os.O_RDONLY | os.O_DIRECT)
    except OSError as error:
        errors += 1
        print(f"open {opens}: {error!r}", flush=True)
        continue
    opens += 1
    time.sleep(random.uniform(0, 0.4))  # GC passes may run while the file is open
    try:
        n = os.preadv(fd, [buffer], 0)
        body = bytes(buffer[:n])
        if n != size or body.count(body[:1]) != size:
            torn += 1
            print(f"open {opens}: {n} bytes, {len(set(body))} distinct values", flush=True)
    except OSError as error:
        errors += 1
        print(f"read {opens}: {error!r}", flush=True)
    finally:
        os.close(fd)
stop.set()
t.join()
logs = "".join(open(p, errors="replace").read() for p in glob.glob(os.path.join(out, "node*.log")))
missing = logs.count("has no readable copy")
print(f"writer rounds={rounds[0]} reader opens={opens} errors={errors} torn={torn} no-readable-copy={missing}")
print("node-b open pins by kind:", reader_pins(7511), flush=True)
correct = errors == 0 and torn == 0 and missing == 0
enough = opens >= int(os.environ.get("E1_MIN_OPENS", "100")) and rounds[0] >= 50
if os.environ.get("AFS_DIAG_READER_REGISTER_AFTER_VIEW") == "1":
    print("control arm (view before registration):", "not reproduced" if correct else "reproduced",
          "" if enough else "(too few opens to count)")
    sys.exit(0)
ok = correct and enough
print("[ok]" if ok else "[FAIL]", "every read returned one whole version")
sys.exit(0 if ok else 1)
PY
