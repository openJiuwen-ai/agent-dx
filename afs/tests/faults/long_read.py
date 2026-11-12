#!/usr/bin/env python3
"""R75: N threads each keep ONE O_DIRECT handle on a file and read it sequentially in a loop for SECS seconds.
Usage: r75-longread.py <file> <threads> <secs> [block]. Prints per-thread bytes and every error (errno), exits 1 on any error."""
import mmap, os, sys, threading, time
path, threads, secs = sys.argv[1], int(sys.argv[2]), float(sys.argv[3])
block = int(sys.argv[4]) if len(sys.argv) > 4 else 1 << 20
size = os.stat(path).st_size
errors, totals = [], [0] * threads
def run(i):
    fd = os.open(path, os.O_RDONLY | os.O_DIRECT)
    buf = mmap.mmap(-1, block)
    off, end = (i * size // threads) // block * block, time.time() + secs
    try:
        while time.time() < end:
            try:
                n = os.preadv(fd, [buf], off)
            except OSError as e:
                errors.append((round(time.time() - (end - secs), 1), i, e.errno, e.strerror)); n = 0
                if len(errors) > 50: return
            totals[i] += n
            off = off + block if n == block and off + block < size else 0
    finally:
        os.close(fd)
ts = [threading.Thread(target=run, args=(i,)) for i in range(threads)]
t0 = time.time(); [t.start() for t in ts]; [t.join() for t in ts]
dt = time.time() - t0
print(f"threads={threads} secs={dt:.0f} read_MiB={sum(totals)/2**20:.0f} MiBps={sum(totals)/2**20/dt:.0f} errors={len(errors)}")
for e in errors[:10]: print("error t=%ss thread=%d errno=%d %s" % e)
sys.exit(1 if errors else 0)
