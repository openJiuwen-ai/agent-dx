"""S0 semantic characterization: assert the CURRENT DFS file semantics.

These cases record today's behavior, including behavior that later refactor
steps are decided to change. A case that is expected to change names the
decision in its docstring and in the recorded evidence; when that step lands,
the assertion is updated in the same change.

Roles: A writes, B and C read through their own DFS Node and FUSE mount.
On a single VM the roles are separate processes and mounts on one kernel, so
this proves cross-client semantics, not cross-VM isolation.
"""

import json
import posixpath
import time

from fixtures.common import Case, DfsProfile
from suites.operations import uncached_path_visibility

HOLDER = """import os,sys,time,json
d=sys.argv[1];os.setgroups([]);os.setgid(int(sys.argv[4]));os.setuid(int(sys.argv[3]))
f=open(sys.argv[2],"rb");first=f.read()
open(d+"/opened.json","w").write(json.dumps({"bytes":len(first)}))
deadline=time.monotonic()+60
while not os.path.exists(d+"/go.json"):
 if time.monotonic()>deadline:raise TimeoutError("controller did not release")
 time.sleep(.02)
f.seek(0);second=f.read();f.close()
open(d+"/done.json","w").write(json.dumps({"first":len(first),"second":len(second),
 "second_head":second[:8].hex()}))
"""


def _hold_open(c, role, name, path):
    holder = c.role_dir(role) + "/run/sem-" + name
    c.python(role, "os.mkdir(sys.argv[1]);os.chown(sys.argv[1],int(sys.argv[2]),int(sys.argv[3]))",
             holder, c.uid, c.gid, owner=False)
    c.channel(role).call("start", role=name,
                         argv=["python3", "-c", HOLDER, holder, path, str(c.uid), str(c.gid)],
                         log=c.root(role) + "/" + name + ".log")
    _wait_file(c, role, holder + "/opened.json")
    return holder


def _release(c, role, name, holder):
    c.python(role, 'open(sys.argv[1],"w").write("{}")', holder + "/go.json")
    _wait_file(c, role, holder + "/done.json")
    c.channel(role).call("wait", role=name)
    return json.loads(c.python(role, 'print(open(sys.argv[1]).read())', holder + "/done.json")["stdout"])


def _wait_file(c, role, path, timeout=30):
    end = time.monotonic() + timeout
    while time.monotonic() < end:
        if c.python(role, "print(os.path.exists(sys.argv[1]))", path)["stdout"].strip() == "True":
            return
        time.sleep(0.05)
    raise TimeoutError("missing " + path)


def committed_write_visible_to_new_open(c):
    """v2 §5.2 row 2: after fsync+close on A, a NEW open on B and C sees the data."""
    for data in (b"first-version", b"second-version-longer" * 100):
        c.write("A", "sem-visible", data)
        c.read("B", "sem-visible", data)
        c.read("C", "sem-visible", data)
    c.record("semantics.visible_after_commit", {"row": "v2 5.2 visibility", "status": "current == decided"})


def open_reader_keeps_open_time_version(c):
    """Decision 4a (S1a-6): an already-open reader is pinned to its open-time version.

    A version committed on another node after the reader opened becomes visible only to a
    new open; the open reader keeps reading the old bytes until close.
    """
    c.write("A", "sem-pinned", b"old" * 10)
    path = c.workspace("B") + "/sem-pinned"
    holder = _hold_open(c, "B", "sem-pinned-holder", path)
    c.write("A", "sem-pinned", b"NEW" * 20)
    time.sleep(6)  # beyond the removed 5 s refresh, so a regression would show
    result = _release(c, "B", "sem-pinned-holder", holder)
    saw_new = result["second"] == 60 and result["second_head"] == b"NEWNEWNE".hex()
    c.record("semantics.open_reader_after_commit", {
        "first_bytes": result["first"], "second_bytes": result["second"], "saw_new_version": saw_new,
        "changed_by": "decision 4a (open_to_close pin), S1a-6", "row": "v2 5.2 visibility (open readers)",
    })
    assert result["first"] == 30, result
    assert result["second"] == 30 and not saw_new, result
    c.read("B", "sem-pinned", b"NEW" * 20)


def unlink_open_file_keeps_data(c):
    """v2 §5.2 last row: unlink of a file that a reader holds open keeps the open data readable."""
    c.write("A", "sem-unlinked", b"keep-me" * 100)
    path = c.workspace("B") + "/sem-unlinked"
    holder = _hold_open(c, "B", "sem-unlink-holder", path)
    c.python("A", "os.unlink(sys.argv[1])", c.workspace("A") + "/sem-unlinked")
    result = _release(c, "B", "sem-unlink-holder", holder)
    c.record("semantics.unlink_open", result)
    assert result["first"] == 700 and result["second"] == 700, result
    # Path-only query: may lag A by one kernel entry TTL.
    c.poll_python("B", "not os.path.exists(a[0])", path, label="unlink.B")


def close_mtime_visibility_today(c):
    """Decision 5 keeps close DataOnly. Q03 asked whether another client's stat lags; S0 measured no lag.

    The FUSE close path updates size and mtime. A fresh OPEN on another node sees them at once
    (close-to-open, asserted strictly through fstat); a path stat may lag by one kernel attribute TTL.
    """
    c.write("A", "sem-mtime", b"x")
    time.sleep(1.2)
    path = posixpath.join(c.workspace("B"), "sem-mtime")
    before = float(c.python("B", "print(os.stat(sys.argv[1]).st_mtime)", path)["stdout"])
    # write without fsync, then close: FUSE flush commits DataOnly.
    c.python("A", 'f=open(sys.argv[1],"ab");f.write(b"y");f.close()', c.workspace("A") + "/sem-mtime")
    opened = json.loads(c.python(
        "B",
        'fd=os.open(sys.argv[1],os.O_RDONLY);s=os.fstat(fd);os.close(fd);'
        'print(json.dumps({"mtime":s.st_mtime,"size":s.st_size}))',
        path,
    )["stdout"])
    stat_delay = c.poll_python(
        "B", "os.stat(a[0]).st_size==2 and os.stat(a[0]).st_mtime>float(a[1])", path, repr(before),
        label="close_mtime_path_stat.B",
    )
    after_close = float(c.python("B", "print(os.stat(sys.argv[1]).st_mtime)", path)["stdout"])
    size_after_close = int(c.python("B", "print(os.stat(sys.argv[1]).st_size)", path)["stdout"])
    time.sleep(6)  # background commit period is 5 s
    after_background = float(c.python("B", "print(os.stat(sys.argv[1]).st_mtime)", path)["stdout"])
    evidence = {"mtime_before": before, "open_fstat_mtime": opened["mtime"], "open_fstat_size": opened["size"],
                "path_stat_delay_ms": stat_delay["ms"], "mtime_after_close": after_close,
                "size_after_close": size_after_close, "mtime_after_background": after_background,
                "question": "Q03", "decision": "5 (close stays DataOnly)"}
    c.record("semantics.close_mtime", evidence)
    assert opened["size"] == 2 and opened["mtime"] > before, evidence
    assert size_after_close == 2, evidence
    assert after_close > before, evidence


def fsync_error_free_path(c):
    """v2 §5.2 fsync row: fsync on a healthy mount succeeds and makes data visible to new opens."""
    c.python("A", 'f=open(sys.argv[1],"wb");f.write(b"z"*4096);os.fsync(f.fileno());'
             'os.fdatasync(f.fileno());f.close()', c.workspace("A") + "/sem-fsync")
    c.read("B", "sem-fsync", b"z" * 4096)
    c.record("semantics.fsync", {"fsync": "ok", "fdatasync": "ok"})


def file_locks_are_local_only(c):
    """Decision 11: flock/fcntl locks are not forwarded across nodes; each node only sees its own locks."""
    c.write("A", "sem-lock", b"l")
    code = ('import fcntl\nf=open(sys.argv[1],"r+b")\n'
            'try:\n fcntl.lockf(f,fcntl.LOCK_EX|fcntl.LOCK_NB);r="ok"\n'
            'except OSError as e:\n r=e.errno\nprint(json.dumps(r))')
    a = c.python("A", code, c.workspace("A") + "/sem-lock")["stdout"].strip()
    b = c.python("B", code, c.workspace("B") + "/sem-lock")["stdout"].strip()
    c.record("semantics.locks", {"node_a": a, "node_b": b, "decision": "11 (not supported across nodes)"})
    # Both nodes obtain an exclusive lock on the same file: there is no cross-node mutual exclusion.
    assert a == '"ok"' and b == '"ok"', (a, b)


ATOMIC_SAVE = """import os,sys
d,name,data=sys.argv[1],sys.argv[2],bytes.fromhex(sys.argv[3])
tmp=d+"/."+name+".tmp"
f=open(tmp,"wb");f.write(data);f.close()
os.rename(tmp,d+"/"+name)
"""

WRITE_RENAME_OPEN = """import os,sys
d,name,first,second=sys.argv[1],sys.argv[2],bytes.fromhex(sys.argv[3]),bytes.fromhex(sys.argv[4])
tmp=d+"/."+name+".tmp"
f=open(tmp,"wb");f.write(first);f.flush()
os.rename(tmp,d+"/"+name)
f.write(second);f.close()
"""


def atomic_save_visible_to_other_node(c):
    """An editor's atomic save on A (write a temporary file, close, rename over the target) is seen
    whole by a new open on B right after the rename returns. Close-to-open across the rename relies on
    FLUSH: it replies only after the inode's acknowledged bodies ran and their data was committed to
    Meta, so the renamed inode's head already holds them; B's open of the target resolves the new name
    (a cached dentry of the replaced file fails the open-time name check and is looked up again).
    """
    rounds = []
    for index in range(8):
        data = (b"save-%02d-" % index) * (64 + 512 * index)
        c.python("A", ATOMIC_SAVE, c.workspace("A"), "sem-atomic-save", data.hex())
        c.read("B", "sem-atomic-save", data)
        rounds.append(len(data))
    c.record("semantics.atomic_save_cross_node", {"rounds": rounds})


def write_then_rename_before_close(c):
    """Writing, renaming the still-open file over its target, writing again and only then closing:
    POSIX gives no ordering between the writes and the rename, so this only requires no error and no
    corruption; the descriptor keeps writing the same inode, which the target name now addresses.
    """
    first, second = b"before-rename-" * 300, b"after-rename-" * 300
    c.write("A", "sem-write-rename", b"old")
    c.python("A", WRITE_RENAME_OPEN, c.workspace("A"), "sem-write-rename", first.hex(), second.hex())
    c.read("B", "sem-write-rename", first + second)
    c.record("semantics.write_then_rename_before_close", {"bytes": len(first) + len(second)})


CASES = (
    Case("dfs.semantics.visible_after_commit", ("s0-semantics",), "dfs", (), committed_write_visible_to_new_open),
    Case("dfs.semantics.open_reader_after_commit", ("s0-semantics",), "dfs", (), open_reader_keeps_open_time_version),
    Case("dfs.semantics.unlink_open_file", ("s0-semantics",), "dfs", (), unlink_open_file_keeps_data),
    Case("dfs.semantics.close_mtime", ("s0-semantics",), "dfs", (), close_mtime_visibility_today),
    Case("dfs.semantics.uncached_path_visibility", ("s0-semantics",), "dfs", (), uncached_path_visibility,
         profile=DfsProfile(kernel_cache_ttl_ms=0)),
    Case("dfs.semantics.fsync", ("s0-semantics",), "dfs", (), fsync_error_free_path),
    Case("dfs.semantics.locks_local_only", ("s0-semantics",), "dfs", (), file_locks_are_local_only),
    Case("dfs.semantics.atomic_save_cross_node", ("s0-semantics",), "dfs", (), atomic_save_visible_to_other_node),
    Case("dfs.semantics.write_then_rename_before_close", ("s0-semantics",), "dfs", (),
         write_then_rename_before_close),
)
