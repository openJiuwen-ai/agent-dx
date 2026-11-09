#!/usr/bin/env python3
"""Owner read ETIMEDOUT isolation, part 3: what triggers it. Usage: phase-owner-read3.py <mnt-a> <mnt-b>"""
import errno, os, sys, time
a, b = sys.argv[1], sys.argv[2]
def tryit(label, f):
    t = time.time()
    try:
        r = f(); print(f"{label}: ok {len(r)}B ({time.time()-t:.1f}s)", flush=True)
    except OSError as e:
        print(f"{label}: {errno.errorcode.get(e.errno, e.errno)} ({time.time()-t:.1f}s)", flush=True)
for n, (wa, wb, fsync_b) in enumerate([(10, 10, True), (100, 0, True), (0, 100, True), (100, 100, True), (100, 100, False), (60, 60, True)]):
    p = f"t{n}"; open(f"{a}/{p}", "w").close(); time.sleep(0.3)
    fa = os.open(f"{a}/{p}", os.O_RDWR | os.O_APPEND); fb = os.open(f"{b}/{p}", os.O_RDWR | os.O_APPEND)
    if wa: os.write(fa, b"A" * wa); os.fsync(fa)
    if wb:
        os.write(fb, b"B" * wb)
        if fsync_b: os.fsync(fb)
    tryit(f"case{n} A={wa} B={wb} fsyncB={fsync_b}: B pread", lambda: os.pread(fb, 4096, 0))
    os.close(fa); os.close(fb)
