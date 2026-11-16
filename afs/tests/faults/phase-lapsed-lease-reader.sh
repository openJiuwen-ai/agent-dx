#!/usr/bin/env bash
# R94, MR186 recheck (MODE=r2, COPIES_R2=1): node-a's write lease lapses while its writer is still open.
#  1. node-a commits V1, keeps a reader (O_DIRECT) and a writer open, commits V2 through the writer; the reader
#     reads V2 (through node-a's write state).
#  2. node-a's Node process is stopped (SIGSTOP) until its write lease has expired; node-b then commits V3 on
#     its own (its new chunk lands on node-b, so the write was not proxied back to the stopped node-a).
#  3. node-a resumes with both descriptors still open; GC runs. V2 must survive: the reader keeps reading it.
#  4. Closing both descriptors drops the last reference: GC then collects V2's chunks.
# An open writer is no evidence of a valid lease; the version a read selected stays referenced regardless.
set -u
python3 -u - "$ROOT" <<'PY'
import glob, json, mmap, os, signal, sys, time, urllib.request

root = sys.argv[1]
a, b = (os.path.join(root, d, "lapsed-lease") for d in ("mnt", "mnt-b"))
pid = int(open(os.path.join(root, "node.pid")).read())
size = 2 * 1024 * 1024
ok = True

def check(label, condition):
    global ok
    print(f"[{'ok' if condition else 'FAIL'}] {label}", flush=True)
    ok &= bool(condition)

def write(fd, value):
    assert os.pwrite(fd, value * size, 0) == size
    os.fsync(fd)

def read(fd, offset=0):
    with mmap.mmap(-1, 65536) as buffer:
        n = os.preadv(fd, [buffer], offset)
        return bytes(buffer[:n])

def chunks(node):
    return {p for p in glob.glob(f"{root}/{node}/state/**/*", recursive=True)
            if "/chunks/" in p and os.path.isfile(p) and os.stat(p).st_size >= 256 * 1024}

def versions():
    with urllib.request.urlopen("http://127.0.0.1:7401/v1/store", timeout=5) as response:
        return json.load(response)["entities"].get("dfs_file_version", 0)

def wait_gone(paths, seconds):
    end = time.monotonic() + seconds
    while time.monotonic() < end and any(os.path.exists(p) for p in paths):
        time.sleep(0.5)
    return not any(os.path.exists(p) for p in paths)

time.sleep(4)  # past node-a's first heartbeat refresh after mount
start = time.monotonic()
with open(a, "wb", buffering=0) as f:
    write(f.fileno(), b"A")
reader = os.open(a, os.O_RDONLY | os.O_DIRECT)
writer = os.open(a, os.O_RDWR)
stopped = False
try:
    check("V1 through the reader", read(reader) == b"A" * 65536)
    before = chunks("node")
    write(writer, b"B")
    v2_chunks = chunks("node") - before
    check("V2 through the reader while the writer is open", read(reader) == b"B" * 65536)
    check("V2's chunks identified on node-a", bool(v2_chunks))
    if time.monotonic() > start + 8:
        raise SystemExit("missed the planned stop point")
    time.sleep(max(0, start + 8 - time.monotonic()))
    os.kill(pid, signal.SIGSTOP)
    stopped = True
    print(f"node-a stopped at {time.monotonic() - start:.1f}s with both descriptors open", flush=True)
    time.sleep(max(0, start + 30.5 - time.monotonic()))  # past the 30 s write lease
    before_b = chunks("node-b")
    with open(b, "r+b", buffering=0) as f:
        write(f.fileno(), b"C")
    check("node-b committed V3 itself (a new chunk on node-b) while node-a was stopped",
          bool(chunks("node-b") - before_b))
    os.kill(pid, signal.SIGCONT)
    stopped = False
    print(f"node-a resumed at {time.monotonic() - start:.1f}s; versions={versions()}", flush=True)
    collected = wait_gone(v2_chunks, 30)  # several GC passes
    check("GC keeps V2's chunks while the reader is open", not collected)
    for offset in (0, size - 65536):
        try:
            body = read(reader, offset)
        except OSError as error:
            body = b""
            print(f"reader at {offset}: {error!r}", flush=True)
        check(f"the original reader still reads V2 at {offset}", body == b"B" * 65536)
finally:
    if stopped:
        os.kill(pid, signal.SIGCONT)
    os.close(reader)
    os.close(writer)
check("closing both descriptors lets GC collect V2's chunks", wait_gone(v2_chunks, 90))
print(f"versions after close: {versions()}", flush=True)
with open(b, "rb") as f:
    check("a fresh open on node-b reads V3", f.read() == b"C" * size)
sys.exit(0 if ok else 1)
PY
