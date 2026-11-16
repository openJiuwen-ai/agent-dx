#!/usr/bin/env bash
# Round 2 C-1 E2 (MODE=r2, COPIES_R2=1), and E4 with READER_LAPSE=partition: node-b opens V1 read-only and reads
# it; node-b then loses Meta for longer than its 30 s session lease, either stopped (SIGSTOP) or cut off from
# Meta by a stalled TCP proxy (node-b is restarted to dial Meta through it). Meanwhile node-a writes V2 and GC
# runs: once node-b's session is past its deadline plus grace, its in-memory reader no longer protects V1, and
# V1's chunks go. node-b must then never deliver bytes from the old handle: a read fails (it must not return
# other content), and node-b fails closed (its session cannot be renewed).
set -u
mode=${READER_LAPSE:-stop}
if [ "$mode" = partition ]; then
  python3 "$FAULTS/freeze_proxy.py" 7409 7400 "$ROOT/meta-freeze" >"$OUT/meta-freeze-proxy.log" 2>&1 &
  echo $! > "$ROOT/meta-freeze-proxy.pid"
  kill -TERM "$(cat "$ROOT/node-b.pid")"
  for _ in $(seq 1 120); do kill -0 "$(cat "$ROOT/node-b.pid")" 2>/dev/null || break; sleep 0.25; done
  findmnt -n "$ROOT/mnt-b" >/dev/null 2>&1 && fusermount3 -u "$ROOT/mnt-b"
  sed -i 's#^meta_endpoint = "https://127.0.0.1:7400"#meta_endpoint = "https://127.0.0.1:7409"#' "$ROOT/node-b.toml"
  "$BIN/afs-node" --config "$ROOT/node-b.toml" >>"$OUT/node-b.log" 2>&1 &
  echo $! > "$ROOT/node-b.pid"
  for _ in $(seq 1 240); do findmnt -n "$ROOT/mnt-b" >/dev/null 2>&1 && break; sleep 0.25; done
fi
python3 -u - "$ROOT" "$mode" <<'PY'
import glob, mmap, os, signal, sys, time

root, mode = sys.argv[1], sys.argv[2]
a, b = (os.path.join(root, d, "lapse") for d in ("mnt", "mnt-b"))
pid = int(open(os.path.join(root, "node-b.pid")).read())
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

before = chunks()
with open(a, "wb", buffering=0) as f:
    f.write(b"1" * size); os.fsync(f.fileno())
v1 = chunks() - before
check("V1's chunks identified on node-a", bool(v1))
time.sleep(2)
reader = os.open(b, os.O_RDONLY | os.O_DIRECT)
check("node-b reads V1", read(reader) == b"1" * size)
freeze = os.path.join(root, "meta-freeze")
if mode == "partition":
    open(freeze, "w").close()
else:
    os.kill(pid, signal.SIGSTOP)
cut = time.monotonic()
print(f"node-b cut off from Meta ({mode})", flush=True)
with open(a, "r+b", buffering=0) as f:
    f.write(b"2" * size); os.fsync(f.fileno())
end = time.monotonic() + 75
while time.monotonic() < end and any(os.path.exists(p) for p in v1):
    time.sleep(0.5)
gone_after = time.monotonic() - cut
collected = not any(os.path.exists(p) for p in v1)
check(f"V1 collected once node-b's session lapsed ({gone_after:.0f} s after the cut)", collected)
# The session deadline is the last heartbeat (every 10 s) plus 30 s: 20 to 30 s after the cut.
check("not before node-b's session could have lapsed", gone_after >= 19)
if mode == "partition":
    os.unlink(freeze)
else:
    os.kill(pid, signal.SIGCONT)
try:
    body = read(reader)
    check("the old handle never returns other content", body == b"1" * size and not collected)
except OSError as error:
    print(f"old handle read: {error!r}", flush=True)
    check("the old handle fails instead of delivering", True)
end = time.monotonic() + 60
while time.monotonic() < end:
    try:
        os.kill(pid, 0)
    except ProcessLookupError:
        break
    time.sleep(0.5)
alive = True
try:
    os.kill(pid, 0)
except ProcessLookupError:
    alive = False
check("node-b failed closed (its expired session cannot be renewed)", not alive)
try:
    os.close(reader)
except OSError:
    pass
sys.exit(0 if ok else 1)
PY
rc=$?
[ -f "$ROOT/meta-freeze-proxy.pid" ] && { rm -f "$ROOT/meta-freeze"; kill -TERM "$(cat "$ROOT/meta-freeze-proxy.pid")" 2>/dev/null; }
exit $rc
