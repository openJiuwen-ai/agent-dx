#!/usr/bin/env python3
"""New issue 3: rename right after creating files. Usage: phase-rename-race.py <mnt> [rounds]
Each round creates 4 files with open/write/close, then immediately renames one with RENAME_NOREPLACE and one plainly."""
import ctypes, errno, os, sys
mnt = sys.argv[1]; rounds = int(sys.argv[2]) if len(sys.argv) > 2 else 100
libc = ctypes.CDLL(None, use_errno=True)
SYS = 316 if os.uname().machine == "x86_64" else 276
def renameat2(a, b, f):
    return 0 if libc.syscall(SYS, -100, a.encode(), -100, b.encode(), f) == 0 else ctypes.get_errno()
fails = {}
for r in range(rounds):
    d = f"{mnt}/r{r}"; os.mkdir(d)
    for n in ("x1", "x2", "y1", "c1"): open(f"{d}/{n}", "w").write(n)
    e = renameat2(f"{d}/c1", f"{d}/c2", 1)
    if e: fails.setdefault(("noreplace", errno.errorcode.get(e, e)), []).append(r)
    try:
        os.rename(f"{d}/y1", f"{d}/y2")
    except OSError as x:
        fails.setdefault(("rename", errno.errorcode.get(x.errno, x.errno)), []).append(r)
print(f"rounds={rounds} failures={ {k: len(v) for k, v in fails.items()} }")
sys.exit(1 if fails else 0)
