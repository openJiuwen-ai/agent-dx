#!/usr/bin/env python3
"""S4-6 (R20) probes on a two-Node DFS mount pair. Usage: phase-r20.py <mnt-a> <mnt-b>
1. rename flags: RENAME_EXCHANGE then RENAME_NOREPLACE on the same mount (does an ENOSYS reply disable RENAME_NOREPLACE?).
2. pre-1970 timestamps: utime to 1960-01-01, read back.
3. multi-Node O_APPEND: A and B alternately append through their own O_RDWR|O_APPEND fds; each reads the file back
   through its fd (pread) and compares with an independent fresh read."""
import ctypes, errno, os, sys, time
a, b = sys.argv[1], sys.argv[2]
libc = ctypes.CDLL(None, use_errno=True)
SYS_renameat2 = 316 if os.uname().machine == "x86_64" else 276
AT_FDCWD = -100
def renameat2(src, dst, flags):
    r = libc.syscall(SYS_renameat2, AT_FDCWD, src.encode(), AT_FDCWD, dst.encode(), flags)
    return 0 if r == 0 else ctypes.get_errno()
RC = 0
print("== 1 rename flags")
for n in ("x1", "x2", "y1", "c1"): open(f"{a}/{n}", "w").write(n)
e0 = renameat2(f"{a}/c1", f"{a}/c2", 1)   # control: RENAME_NOREPLACE before any EXCHANGE
print(f"control NOREPLACE (free target) first -> {errno.errorcode.get(e0, e0) if e0 else 'ok'}")
e1 = renameat2(f"{a}/x1", f"{a}/x2", 2)   # RENAME_EXCHANGE
print(f"EXCHANGE -> {errno.errorcode.get(e1, e1) if e1 else 'ok'}")
e2 = renameat2(f"{a}/y1", f"{a}/y2", 1)   # RENAME_NOREPLACE onto a free name
print(f"NOREPLACE (free target) after EXCHANGE -> {errno.errorcode.get(e2, e2) if e2 else 'ok'}")
e3 = renameat2(f"{a}/x1", f"{a}/x2", 1)   # NOREPLACE onto an existing name
print(f"NOREPLACE (existing target) -> {errno.errorcode.get(e3, e3) if e3 else 'ok'}")
print(f"R20-1 {'NOREPLACE disabled by the earlier EXCHANGE' if e2 else 'NOREPLACE still works'}")
print("== 2 pre-1970 timestamp")
p = f"{a}/old"; open(p, "w").close()
t = -315619200  # 1960-01-01T00:00:00Z
try:
    os.utime(p, (t, t)); st = os.stat(p)
    print(f"utime(1960) ok; read back mtime={st.st_mtime:.0f} ({time.strftime('%Y-%m-%d', time.gmtime(st.st_mtime))})")
except OSError as e:
    print(f"utime(1960) -> {errno.errorcode.get(e.errno, e.errno)}")
print("== 3 multi-Node O_APPEND")
pa, pb = f"{a}/app", f"{b}/app"
# Warm the B->A peer path first: the first remote-writer read after start can
# time out (separate new issue, see phase-owner-read3.py).
open(f"{a}/warm", "w").write("w"); time.sleep(0.5); open(f"{b}/warm", "rb").read()
open(pa, "w").close(); time.sleep(0.5)
fa = os.open(pa, os.O_RDWR | os.O_APPEND); fb = os.open(pb, os.O_RDWR | os.O_APPEND)
expected = b""
for i in range(6):
    fd, tag = (fa, b"A") if i % 2 == 0 else (fb, b"B")
    chunk = tag * 100 + b"%d\n" % i
    os.write(fd, chunk); os.fsync(fd); expected += chunk; time.sleep(0.3)
time.sleep(1)
bad = 0
def attempt(f):
    try:
        return f()
    except OSError as e:
        return f"ERR {errno.errorcode.get(e.errno, e.errno)}"
for name, fd, path in (("A", fa, pa), ("B", fb, pb)):
    via_fd = attempt(lambda: os.pread(fd, 1 << 20, 0)); size = attempt(lambda: os.fstat(fd).st_size)
    fresh = attempt(lambda: open(path, "rb").read())
    if isinstance(via_fd, str) or isinstance(fresh, str):
        bad += 1; print(f"{name}: fd-read {via_fd if isinstance(via_fd, str) else len(via_fd)} fstat={size} fresh {fresh if isinstance(fresh, str) else len(fresh)}"); continue
    ok = via_fd == expected and fresh == expected and size == len(expected)
    bad += not ok
    print(f"{name}: fd-read {len(via_fd)}B match={via_fd == expected} fstat={size} fresh {len(fresh)}B match={fresh == expected}")
os.close(fa); os.close(fb)
print(f"R20-3 {'MISMATCH (page cache vs persisted append offset)' if bad else 'consistent'}")
