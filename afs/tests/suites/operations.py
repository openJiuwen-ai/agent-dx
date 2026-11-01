"""Filesystem assertions shared across single-VM and real multi-VM fixtures."""

from concurrent.futures import ThreadPoolExecutor
import hashlib
import json
import time
from fixtures.common import Blocked, is_single_host


def peer_counter(cluster, role):
    text = cluster.health(role, "/metrics")
    cluster.record("peer_metrics." + role, text)
    # This is a client invocation count, never a server success count. The
    # separate fresh-reader content assertions prove successful data delivery.
    values = [
        float(line.rsplit(" ", 1)[1])
        for line in text.splitlines()
        if line.startswith('afs_dfs_read_rpc_calls_total{method="peer_read_ranges"} ')
    ]
    if len(values) != 1:
        raise AssertionError("exact DFS peer read RPC counter missing/duplicated for " + role)
    import math

    if not math.isfinite(values[0]) or values[0] < 0:
        raise AssertionError("invalid DFS peer RPC counter")
    return values[0]


def read_write(c):
    readers = c.roles[2:]
    before = {r: peer_counter(c, r) for r in readers} if c.mode == "dfs" else {}
    for generation, char in enumerate((b"A", b"B", b"C")):
        data = char * 65536
        c.write("A", "cross.bin", data)
        # The private worker executes same-host readers concurrently too.
        requests = {}
        for role in readers:
            code = (
                'import sys,hashlib\nd=open(sys.argv[1],"rb").read();assert len(d)==65536;'
                "assert hashlib.sha256(d).hexdigest()==sys.argv[2]"
            )
            req = {
                "action": "command",
                "argv": ["python3", "-c", code, c.workspace(role) + "/cross.bin", hashlib.sha256(data).hexdigest()],
                "uid": c.uid,
                "gid": c.gid,
                "timeout": 30,
            }
            requests.setdefault(c.config["roles"][role]["host"], []).append(req)
        with ThreadPoolExecutor(max_workers=len(requests)) as pool:
            futures = [
                pool.submit(c.channels[h].call, "parallel", requests=reqs, timeout=40) for h, reqs in requests.items()
            ]
            for future in futures:
                for result in future.result():
                    assert result["returncode"] == 0, result
        c.record(
            "generation." + str(generation),
            {"readers": readers, "bytes": len(data), "sha256": hashlib.sha256(data).hexdigest()},
        )
        if c.mode == "ownerfs":
            c.write("B", "cross.bin", data + b"REMOTE")
            c.read("A", "cross.bin", data + b"REMOTE")
    if c.mode == "dfs":
        for role in readers:
            after = peer_counter(c, role)
            assert after > before[role], (role, before[role], after)
            c.record("actual_peer_path." + role, {"before": before[role], "after": after})
    c.python(
        "A",
        "os.unlink(sys.argv[1]);fd=os.open(sys.argv[2],os.O_RDONLY|os.O_DIRECTORY);os.fsync(fd);os.close(fd)",
        c.workspace("A") + "/cross.bin",
        c.workspace("A"),
    )
    for role in readers:
        c.python(role, "assert not os.path.exists(sys.argv[1])", c.workspace(role) + "/cross.bin")


def namespace(c):
    a, b = c.workspace("A"), c.workspace("B")
    c.python("A", 'os.mkdir(sys.argv[1]);assert "dir" in os.listdir(sys.argv[2])', a + "/dir", a)
    c.write("A", "dir/old", b"namespace")
    c.read("B", "dir/old", b"namespace")
    c.python(
        "B", "assert os.stat(sys.argv[1]).st_size==9;os.rename(sys.argv[1],sys.argv[2])", b + "/dir/old", b + "/dir/new"
    )
    c.python(
        "A",
        (
            'assert not os.path.exists(sys.argv[1]);f=open(sys.argv[2],"r+b");f.t'
            "runcate(3);f.flush();os.fsync(f.fileno());f.close()"
        ),
        a + "/dir/old",
        a + "/dir/new",
    )
    c.read("B", "dir/new", b"nam")
    c.python("B", "os.unlink(sys.argv[1]);os.rmdir(sys.argv[2])", b + "/dir/new", b + "/dir")
    c.python(
        "A",
        (
            'assert not os.path.exists(sys.argv[1])\ntry:open(sys.argv[2],"rb")\nex'
            "cept OSError as e:assert e.errno==errno.ENOENT\nelse:raise AssertionE"
            'rror("ENOENT expected")'
        ),
        a + "/dir",
        a + "/missing",
    )


def permissions(c):
    c.write("A", "private", b"permissions")
    denied = (
        'import os,sys,errno\ntry:open(sys.argv[1],"rb")\nexcept OSError as e:a'
        'ssert e.errno==errno.EACCES\nelse:raise AssertionError("access unexpe'
        'ctedly allowed")'
    )
    c.command(
        "B",
        ["python3", "-c", denied, c.workspace("B") + "/private"],
        uid=c.config["other_uid"],
        gid=c.config["other_gid"],
    )
    c.python("A", "os.chmod(sys.argv[1],0)", c.workspace("A") + "/private")
    c.python("B", denied, c.workspace("B") + "/private")
    c.python("A", "os.chmod(sys.argv[1],0o600)", c.workspace("A") + "/private")
    c.read("B", "private", b"permissions")
    if c.mode == "ownerfs":
        c.python(
            "A",
            'open(sys.argv[1],"wb").write(b"initial");os.chmod(sys.argv[1],0o6777)',
            c.workspace("A") + "/setid",
            owner=False,
        )
        c.write("B", "setid", b"nonroot")
        c.python("A", "assert os.stat(sys.argv[1]).st_mode&0o7777==0o777", c.workspace("A") + "/setid", owner=False)
        c.python("A", "os.chmod(sys.argv[1],0o6777)", c.workspace("A") + "/setid", owner=False)
        c.python(
            "B",
            'f=open(sys.argv[1],"wb");f.write(b"root");f.flush();os.fsync(f.fileno());f.close()',
            c.workspace("B") + "/setid",
            owner=False,
        )
        c.python(
            "A",
            "assert os.stat(sys.argv[1]).st_mode&0o7777==0o6777;os.chmod(sys.argv[1],0o2666)",
            c.workspace("A") + "/setid",
            owner=False,
        )
        c.write("B", "setid", b"non-executable-sgid")
        c.python("A", "assert os.stat(sys.argv[1]).st_mode&0o7777==0o2666", c.workspace("A") + "/setid", owner=False)
    c.record("permissions", {"owner_uid": c.uid, "other_uid": c.config["other_uid"], "mode": c.mode})


def visibility(c):
    for data in (b"a", b"longer" * 10000, b"short"):
        c.write("A", "visible", data)
        c.read("B", "visible", data)
        # Same-version fresh OPEN must also retain the exact data.
        c.read("B", "visible", data)
    permissions(c)


def recovery(c):
    data = b"confirmed-before-stop" * 4096
    c.write("A", "durable", data)
    before = json.loads(
        c.python(
            "A",
            "s=os.stat(sys.argv[1]);print(json.dumps([s.st_ino,s.st_uid,s.st_gid,s.st_mode,s.st_size]))",
            c.workspace("A") + "/durable",
        )["stdout"]
    )
    c.restart(c.roles)
    c.read("B", "durable", data)
    after = json.loads(
        c.python(
            "A",
            "s=os.stat(sys.argv[1]);print(json.dumps([s.st_ino,s.st_uid,s.st_gid,s.st_mode,s.st_size]))",
            c.workspace("A") + "/durable",
        )["stdout"]
    )
    assert before == after, (before, after)
    if not is_single_host(c.config):
        c.restart(["M"])
        wait_read(c, "B", "durable", data)
        # Verify an unprimed name fails while the one R1 Home is unavailable.
        c.write("A", "uncached", b"uncached")
        c.stop("A")
        failed = c.python("B", 'open(sys.argv[1],"rb").read()', c.workspace("B") + "/uncached", check=False)
        assert failed["returncode"] != 0, failed
        c.record("home_unavailable_error", failed)
        c.start("A")
        wait_read(c, "B", "uncached", b"uncached")
    c.record("normal_recovery", {"before": before, "after": after})


def wait_read(c, role, name, expected):
    """Bounded session/cache convergence; never retry content corruption."""
    deadline = time.monotonic() + 30
    attempts = []
    code = (
        'try:\n d=open(sys.argv[1],"rb").read();'
        'print(json.dumps({"bytes":len(d),"sha256":hashlib.sha256(d).hexdigest()}))'
        '\nexcept OSError as e:print(json.dumps({"errno":e.errno,"error":str(e)}))'
    )
    while time.monotonic() < deadline:
        result = json.loads(c.python(role, code, c.workspace(role) + "/" + name)["stdout"])
        if "errno" not in result:
            assert result["bytes"] == len(expected) and result["sha256"] == hashlib.sha256(expected).hexdigest(), result
            c.record("recovery_convergence." + role, {"transient_errors": attempts, "final": result})
            return
        attempts.append(result)
        time.sleep(0.1)
    c.record("recovery_convergence_timeout." + role, attempts)
    raise AssertionError("confirmed data did not recover within 30 seconds")


def old_fd(c):
    for operation in ("rename", "unlink"):
        c.write("A", "old", b"old-object")
        holder = c.role_dir("B") + "/run/holder-" + operation
        c.python(
            "B",
            "os.mkdir(sys.argv[1]);os.chown(sys.argv[1],int(sys.argv[2]),int(sys.argv[3]))",
            holder,
            c.uid,
            c.gid,
            owner=False,
        )
        code = """import os,sys,time,json
os.setgroups([]);os.setgid(int(sys.argv[4]));os.setuid(int(sys.argv[3]))
f=open(sys.argv[1],"rb");open(sys.argv[2]+"/ready","w").close()
deadline=time.monotonic()+20
while not os.path.exists(sys.argv[2]+"/go"):
 if time.monotonic()>deadline:raise TimeoutError("controller did not release fd holder")
 time.sleep(.02)
data=f.read();f.close();assert data==b"old-object",data
open(sys.argv[2]+"/done","w").write("passed")
"""
        role = "holder-" + operation
        c.channel("B").call(
            "start",
            role=role,
            argv=["python3", "-c", code, c.workspace("B") + "/old", holder, str(c.uid), str(c.gid)],
            log=c.root("B") + "/" + role + ".log",
        )
        _wait(c, holder + "/ready")
        if operation == "rename":
            c.python(
                "A", "os.rename(sys.argv[1],sys.argv[2])", c.workspace("A") + "/old", c.workspace("A") + "/renamed"
            )
        else:
            c.python("A", "os.unlink(sys.argv[1])", c.workspace("A") + "/old")
        c.write("A", "old", b"new-object")
        c.python("B", 'open(sys.argv[1],"w").close()', holder + "/go")
        _wait(c, holder + "/done")
        c.channel("B").call("wait", role=role)
        c.read("B", "old", b"new-object")
    c.record("old_fd", "rename and unlink/name reuse retain original object")


def _wait(c, path):
    deadline = time.monotonic() + 20
    while time.monotonic() < deadline:
        result = c.python("B", "assert os.path.exists(sys.argv[1])", path, check=False)
        if result["returncode"] == 0:
            return
        time.sleep(0.05)
    raise AssertionError("holder handshake timed out: " + path)


def owner_namespace(c):
    namespace(c)
    old_fd(c)
    root = "race-" + c.token[-8:]
    requests = {}
    for role in ("A", "B"):
        req = {
            "action": "command",
            "argv": [
                "python3",
                "-c",
                (
                    'import os,sys,json\ntry:os.mkdir(sys.argv[1]);v={"created":True}\nexce'
                    'pt OSError as e:v={"created":False,"errno":e.errno,"error":str(e)}\np'
                    "rint(json.dumps(v))"
                ),
                c.mount(role) + "/" + root,
            ],
            "timeout": 15,
        }
        requests.setdefault(c.config["roles"][role]["host"], []).append(req)
    with ThreadPoolExecutor(max_workers=len(requests)) as pool:
        futures = [pool.submit(c.channels[h].call, "parallel", requests=v) for h, v in requests.items()]
        responses = [r for f in futures for r in f.result()]
        c.record("root_create_race_responses", responses)
        winners = [json.loads(r["stdout"]) for r in responses if r["returncode"] == 0]
    assert len(winners) == 2 and sum(v["created"] for v in winners) == 1, winners
    c.record(
        "root_create_race_outcome",
        {"results": winners, "scope": "single Home election; loser errno is observed, not POSIX-certified"},
    )
    location = json.loads(c.health("M", "/v1/roots/root-" + root.encode().hex()))
    assert location["home_node_id"] in ("node-a", "node-b") and location["status"] == "serving", location
    c.record("single_home", location)
    count = 0
    for role in ("A", "B"):
        result = c.python(
            role,
            "from pathlib import Path\nprint(len(list(Path(sys.argv[1]).glob(sys.argv[2]))))",
            c.role_dir(role) + "/state/ownerfs",
            "root-" + root.encode().hex() + "-e*",
            owner=False,
        )
        count += int(result["stdout"])
    assert count == 1, count
    c.python("B", "os.mkdir(sys.argv[1])", c.mount("B") + "/second", owner=False)
    c.write("A", "cross-root", b"x")
    c.python(
        "B",
        (
            "try:os.rename(sys.argv[1],sys.argv[2])\nexcept OSError as e:assert e."
            'errno==errno.EXDEV\nelse:raise AssertionError("EXDEV required")'
        ),
        c.workspace("B") + "/cross-root",
        c.mount("B") + "/second/x",
        owner=False,
    )


def rust_contract(c, selectors):
    binary = c.root("A") + "/bin/fuse_contract"
    for selector in selectors:
        listed = c.command("A", [binary, "--list", "--exact", selector])["stdout"]
        if selector + ": test" not in listed:
            raise Blocked("selected Rust test ELF does not contain exact selector: " + selector)
        env = {}
        uid = None
        if selector.startswith("real_dfs_keep_cache_contract::"):
            assert is_single_host(c.config)
            c.python(
                "A", "os.chown(sys.argv[1],int(sys.argv[2]),int(sys.argv[3]))", c.mount("A"), c.uid, c.gid, owner=False
            )
            env = {
                "AFS_DFS_FUSE_WRITER_MOUNT": c.mount("A"),
                "AFS_DFS_FUSE_READER_MOUNT": c.mount("B"),
                "AFS_DFS_FUSE_READER_METRICS": c.address("B") + ":" + str(c.ports("B")[1]),
            }
            uid = c.uid
            code = (
                'd=os.stat(sys.argv[1]).st_dev;p="/sys/class/bdi/%s:%s/read_ahead_kb"'
                '%(os.major(d),os.minor(d));print(open(p).read().strip());open(p,"w")'
                '.write("0")'
            )
            before = c.python("B", code, c.mount("B"), owner=False)["stdout"].strip()
        try:
            result = c.command(
                "A",
                [binary, "--ignored", "--exact", selector, "--nocapture", "--test-threads=1"],
                uid=uid,
                gid=c.gid if uid else None,
                env=env,
                timeout=180,
            )
            assert "1 passed" in result["stdout"] and "0 failed" in result["stdout"], result
            c.record("rust." + selector, result)
        finally:
            if uid is not None:
                c.python(
                    "B",
                    (
                        'd=os.stat(sys.argv[1]).st_dev;p="/sys/class/bdi/%s:%s/read_ahead_kb"'
                        '%(os.major(d),os.minor(d));open(p,"w").write(sys.argv[2])'
                    ),
                    c.mount("B"),
                    before,
                    owner=False,
                )
