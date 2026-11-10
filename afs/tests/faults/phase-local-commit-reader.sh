#!/usr/bin/env bash
# R94 scenario (MODE=r2, COPIES_R2=1): node-a keeps a reader open across its own commit and another Node's.
#  1. node-a writes V1, opens it O_DIRECT and keeps the descriptor; node-a commits V2: the held reader sees V2
#     (decision 4a, this Node's commits are visible to its open handles).
#  2. After node-a's write lease has expired, node-b commits V3 on its own (its chunk lands on node-b, nothing
#     on node-a: the write was not proxied back to node-a). The held reader still reads V2.
#  3. GC runs while the reader is open: V1, which nothing references any more, is collected before V3 is
#     written; after V3, GC keeps exactly V3 and the held V2, and every range read through the held descriptor
#     after GC returns V2.
#  4. Closing the descriptor drops the last reference: GC then collects V2, leaving only the head V3.
set -u
python3 -u - "$ROOT" <<'PY'
import hashlib, json, mmap, os, sys, time, urllib.request
root = sys.argv[1]
a, b = os.path.join(root, "mnt", "r94"), os.path.join(root, "mnt-b", "r94")
size = 2 * 1024 * 1024
ok = True

def check(label, condition):
    global ok
    print(f"[{'ok' if condition else 'FAIL'}] {label}", flush=True)
    ok &= bool(condition)

def write(path, byte):
    with open(path, "wb", buffering=0) as f:
        assert f.write(byte * size) == size
        os.fsync(f.fileno())

def versions():
    with urllib.request.urlopen("http://127.0.0.1:7401/v1/store", timeout=5) as response:
        return json.load(response)["entities"].get("dfs_file_version", 0)

def direct_read(fd, offset, length):
    with mmap.mmap(-1, length) as buffer:
        count = os.preadv(fd, [buffer], offset)
        return bytes(buffer[:count])

def drop():
    with open("/proc/sys/vm/drop_caches", "w") as f:
        f.write("3\n")

def big_files(state):
    found = set()
    for base, _, names in os.walk(state):
        for name in names:
            path = os.path.join(base, name)
            try:
                if os.path.getsize(path) >= 256 * 1024:
                    found.add(path)
            except OSError:
                pass
    return found

def wait_versions(target, seconds):
    end = time.monotonic() + seconds
    seen = None
    while time.monotonic() < end:
        seen = versions()
        if seen == target:
            return seen
        time.sleep(1)
    return seen

write(a, b"A")
fd = os.open(a, os.O_RDONLY | os.O_DIRECT)
try:
    check("V1 read through the held descriptor", direct_read(fd, 0, 4096) == b"A" * 4096)
    write(a, b"B")
    check("node-a's own commit V2 is visible to the held descriptor", direct_read(fd, 0, 4096) == b"B" * 4096)
    # node-a's last writer is closed; past its write lease node-b gets its own lease instead of proxying.
    # Meanwhile GC runs: nothing references V1 any more (the reader moved to V2), so it is collected.
    after_v2 = versions()
    time.sleep(35)
    before_v3 = wait_versions(1, 60)
    print(f"versions: {after_v2} right after V2, {before_v3} before V3", flush=True)
    check("GC ran while the reader was open and collected V1, keeping the held V2", before_v3 == 1)
    before_a = big_files(os.path.join(root, "node", "state"))
    before_b = big_files(os.path.join(root, "node-b", "state"))
    write(b, b"C")
    with open(b, "rb") as f:
        check("node-b reads back its V3", f.read() == b"C" * size)
    new_a = big_files(os.path.join(root, "node", "state")) - before_a
    new_b = big_files(os.path.join(root, "node-b", "state")) - before_b
    print(f"new chunk files: node-a {len(new_a)}, node-b {len(new_b)}", flush=True)
    check("V3 was committed by node-b itself (its chunk on node-b, none on node-a)", new_b and not new_a)
    check("held descriptor before GC still reads V2", direct_read(fd, size // 2, 4096) == b"B" * 4096)
    seen = wait_versions(2, 90)
    print(f"versions after V3: {seen}", flush=True)
    # A count: which two versions remain is checked by the reads below.
    check("GC keeps two versions (the head V3 and the held V2)", seen == 2)
    time.sleep(12)  # more GC passes with the descriptor still open
    check("GC keeps V2 while the descriptor is open", versions() == 2)
    drop()
    for offset in (0, size // 4, size - 65536):
        try:
            body = direct_read(fd, offset, 65536)
        except OSError as error:
            body = b""
            print(f"held descriptor after GC, offset {offset}: {error!r}", flush=True)
        print(f"held descriptor after GC, offset {offset}: {len(body)} bytes {hashlib.sha256(body).hexdigest()[:16]}",
              flush=True)
        check(f"held descriptor after GC reads V2 at {offset}", body == b"B" * 65536)
finally:
    os.close(fd)
seen = wait_versions(1, 90)
print(f"versions after the descriptor closed: {seen}", flush=True)
check("closing the last reference lets GC collect V2", seen == 1)
drop()
with open(a, "rb") as f:
    check("a fresh open on node-a sees V3", f.read() == b"C" * size)
sys.exit(0 if ok else 1)
PY
