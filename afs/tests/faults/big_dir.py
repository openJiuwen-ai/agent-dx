#!/usr/bin/env python3
"""R72: create N empty files in DIR (T threads), then list with `ls -f | wc -l`, check duplicates, time it.
Usage: r72-bigdir.py <dir> <n> [threads] [--concurrent]   --concurrent: while listing, create+unlink extra names
and verify every unmodified name appears exactly once."""
import os, subprocess, sys, threading, time
d, n = sys.argv[1], int(sys.argv[2]); threads = int(sys.argv[3]) if len(sys.argv) > 3 and sys.argv[3].isdigit() else 8
concurrent = '--concurrent' in sys.argv
os.makedirs(d, exist_ok=True)
def make(lo, hi):
    for i in range(lo, hi):
        os.close(os.open(f"{d}/f{i:08d}", os.O_CREAT | os.O_WRONLY, 0o644))
t0 = time.time(); step = (n + threads - 1) // threads
ts = [threading.Thread(target=make, args=(i * step, min(n, (i + 1) * step))) for i in range(threads)]
[t.start() for t in ts]; [t.join() for t in ts]
print(f"created {n} in {time.time()-t0:.1f}s")
stop = threading.Event()
def churn():
    # Names sort before ("a") and after ("z") every listed "f" name; some are
    # created and kept, some created and removed while the listing runs.
    k = 0
    while not stop.is_set():
        for pre in ("a", "z"):
            p = f"{d}/{pre}{k:08d}"; os.close(os.open(p, os.O_CREAT | os.O_WRONLY, 0o644))
            if k % 2: os.unlink(p)
        k += 1
    print(f"churn ops={k}")
if concurrent:
    c = threading.Thread(target=churn); c.start()
t0 = time.time()
if concurrent:
    # A slow lister (one directory handle, getdents in batches) so the churn
    # overlaps many ReadDir pages.
    listed = []
    with os.scandir(d) as it:
        for i, entry in enumerate(it):
            listed.append(entry.name)
            if i % 200 == 0: time.sleep(0.01)
    class R: pass
    r = R(); r.returncode = 0; r.stderr = b""; r.stdout = ("\n".join(listed) + "\n").encode()
else:
    r = subprocess.run(["ls", "-f", d], capture_output=True)
dt = time.time() - t0
if concurrent:
    stop.set(); c.join()
names = r.stdout.decode(errors="replace").split("\n")[:-1]
base = [x for x in names if x.startswith("f")]
dups = len(base) - len(set(base))
missing = n - len(set(base))
print(f"ls rc={r.returncode} stderr={r.stderr.decode().strip()[:200]!r} listed={len(names)} f_entries={len(base)} dups={dups} missing={missing} secs={dt:.2f}")
sys.exit(0 if r.returncode == 0 and dups == 0 and missing == 0 else 1)
