"""R20: the DFS POSIX support boundary, one case per item group of the support list
(`docs/architecture/afs-posix-boundary.md`). Every case asserts what an application
observes through a real kernel mount, including behavior the kernel decides (cached
ENOSYS, local lock tables, copy fallback). Item numbers (#n) follow that list.

Roles: A and B are two DFS Nodes with their own FUSE mounts. On a single VM they
share one kernel, so "cross-node" here proves cross-mount behavior through two
Nodes, not cross-VM isolation.
"""

import json

from fixtures.common import Case

TAGS = ("posix-boundary",)

PRELUDE = """import ctypes,ctypes.util,fcntl,select,socket,time
libc=ctypes.CDLL(ctypes.util.find_library("c"),use_errno=True)
def name(n):
    # Linux: ENOTSUP is EOPNOTSUPP and EWOULDBLOCK is EAGAIN; report one name.
    raw=errno.errorcode.get(n,str(n))
    return {"ENOTSUP":"EOPNOTSUPP","EWOULDBLOCK":"EAGAIN"}.get(raw,raw)
def err(f,*a):
    try:
        r=f(*a)
        return ["ok",r if isinstance(r,(int,str,bool,type(None))) else None]
    except OSError as e:
        return ["err",name(e.errno)]
def cerr(r):
    return ["ok",r] if r>=0 else ["err",name(ctypes.get_errno())]
"""

# Helpers run without PRELUDE; the fixture imports only os,sys,json,hashlib,errno,stat.
HELPER_IMPORTS = "import time\n"


def run(c, role, code, *args, owner=True, timeout=60):
    value = c.python(role, PRELUDE + code, *map(str, args), owner=owner, timeout=timeout)
    return json.loads(value["stdout"].strip().splitlines()[-1])


def path(c, role, name):
    return c.workspace(role) + "/" + name


def let_others_traverse(c):
    """Cases that act as the other UID need search permission on the workspace."""
    c.python("A", "os.chmod(sys.argv[1],0o711)", c.workspace("A"), owner=False)


def fallocate_is_not_supported(c):
    """#1: every fallocate mode returns EOPNOTSUPP (the kernel caches the first ENOSYS)."""
    c.write("A", "px-falloc", b"x" * 8192)
    result = run(c, "A", """fd=os.open(sys.argv[1],os.O_RDWR)
out={}
for label,mode in (("default",0),("keep_size",1),("punch_hole",3),("collapse",8),("zero_range",16),("insert",32)):
    out[label]=cerr(libc.fallocate(fd,mode,ctypes.c_long(0),ctypes.c_long(4096)))
print(json.dumps(out))""", path(c, "A", "px-falloc"))
    c.record("posix.fallocate", result)
    assert all(v == ["err", "EOPNOTSUPP"] for v in result.values()), result


def seek_data_hole_treat_the_file_as_data(c):
    """#2: SEEK_DATA/SEEK_HOLE are served by the kernel fallback: the whole file is data."""
    result = run(c, "A", """fd=os.open(sys.argv[1],os.O_CREAT|os.O_RDWR,0o644)
os.pwrite(fd,b"d"*4096,0);os.ftruncate(fd,1<<20);os.fsync(fd)
print(json.dumps({"data0":os.lseek(fd,0,os.SEEK_DATA),"data5000":os.lseek(fd,5000,os.SEEK_DATA),
 "hole0":os.lseek(fd,0,os.SEEK_HOLE),"size":os.fstat(fd).st_size,
 "data_past_end":err(os.lseek,fd,2<<20,os.SEEK_DATA)}))""", path(c, "A", "px-seek"))
    c.record("posix.seek_hole", result)
    assert result["data0"] == 0 and result["data5000"] == 5000, result
    assert result["hole0"] == result["size"] == 1 << 20, result
    assert result["data_past_end"] == ["err", "ENXIO"], result


def readdir_lists_every_entry_with_its_type(c):
    """#3, #37: readdir returns every entry once with its type; READDIRPLUS is not used."""
    result = run(c, "A", """d=sys.argv[1];os.mkdir(d)
for i in range(40): open(d+"/f%02d"%i,"w").close()
os.mkdir(d+"/sub");os.symlink("f00",d+"/ln")
items=sorted((e.name,"d" if e.is_dir(follow_symlinks=False) else "l" if e.is_symlink() else "f") for e in os.scandir(d))
print(json.dumps({"names":[n for n,_ in items],"types":dict(items),"has_dots":"." in os.listdir(d)}))""",
                 path(c, "A", "px-readdir"))
    c.record("posix.readdir", {"count": len(result["names"])})
    assert len(result["names"]) == 42 and len(set(result["names"])) == 42, result
    assert result["types"]["sub"] == "d" and result["types"]["ln"] == "l" and result["types"]["f00"] == "f"
    assert not result["has_dots"]
    listed = run(c, "B", "print(json.dumps(sorted(os.listdir(sys.argv[1]))))", path(c, "B", "px-readdir"))
    assert listed == result["names"], listed


LOCK_HOLDER = HELPER_IMPORTS + """import fcntl
f=open(sys.argv[2],"r+b")
kind=sys.argv[3]
if kind=="flock": fcntl.flock(f,fcntl.LOCK_EX)
elif kind=="posix": fcntl.lockf(f,fcntl.LOCK_EX)
else:
    import struct
    fcntl.fcntl(f,37,struct.pack("hhqqi",fcntl.F_WRLCK,0,0,0,0))  # F_OFD_SETLK
open(sys.argv[1]+"/held.json","w").write("{}")
end=time.monotonic()+120
while not os.path.exists(sys.argv[1]+"/release.json") and time.monotonic()<end: time.sleep(.05)
"""

LOCK_PROBE = """import fcntl,struct
f=open(sys.argv[1],"r+b");kind=sys.argv[2]
def take():
    if kind=="flock": fcntl.flock(f,fcntl.LOCK_EX|fcntl.LOCK_NB)
    elif kind=="posix": fcntl.lockf(f,fcntl.LOCK_EX|fcntl.LOCK_NB)
    else: fcntl.fcntl(f,37,struct.pack("hhqqi",fcntl.F_WRLCK,0,0,0,0))
print(json.dumps(err(take)))"""


def file_locks_exclude_on_one_node_only(c):
    """#4 (decision 11): flock, POSIX and OFD locks exclude other processes of the same
    node, and never exclude a process on another node."""
    c.write("A", "px-lock", b"l")
    out = {}
    for kind in ("flock", "posix", "ofd"):
        name = "lock-" + kind
        c.spawn_python("A", name, LOCK_HOLDER, path(c, "A", "px-lock"), kind)
        c.await_helper_signal(name, "held")
        same = run(c, "A", LOCK_PROBE, path(c, "A", "px-lock"), kind)
        other = run(c, "B", LOCK_PROBE, path(c, "B", "px-lock"), kind)
        c.helper_signal(name, "release", {})
        c.wait_helper(name)
        out[kind] = {"same_node": same, "other_node": other}
    c.record("posix.locks", out)
    for kind, value in out.items():
        assert value["same_node"] == ["err", "EAGAIN"], (kind, value)
        assert value["other_node"] == ["ok", None], (kind, value)


def user_xattrs_round_trip(c):
    """#5: user.* get/set/list/remove, CREATE/REPLACE flags and size probes."""
    c.write("A", "px-xattr", b"x")
    result = run(c, "A", """p=sys.argv[1]
os.setxattr(p,"user.a",b"v1")
out={"get":os.getxattr(p,"user.a").decode(),"list":sorted(os.listxattr(p))}
out["create_existing"]=err(os.setxattr,p,"user.a",b"v2",os.XATTR_CREATE)
out["replace_missing"]=err(os.setxattr,p,"user.b",b"v2",os.XATTR_REPLACE)
out["get_missing"]=err(os.getxattr,p,"user.b")
out["long_name"]=err(os.setxattr,p,"user."+"n"*251,b"v")
os.setxattr(p,"user.big",b"z"*65536);out["big_len"]=len(os.getxattr(p,"user.big"))
os.removexattr(p,"user.a");out["after_remove"]=sorted(os.listxattr(p))
print(json.dumps(out))""", path(c, "A", "px-xattr"))
    c.record("posix.xattr_user", {k: v for k, v in result.items() if k != "list"})
    assert result["get"] == "v1" and result["list"] == ["user.a"], result
    assert result["create_existing"] == ["err", "EEXIST"], result
    assert result["replace_missing"] == ["err", "ENODATA"], result
    assert result["get_missing"] == ["err", "ENODATA"], result
    assert result["long_name"] == ["err", "ERANGE"], result
    assert result["big_len"] == 65536 and result["after_remove"] == ["user.big"], result
    seen = run(c, "B", "print(json.dumps(os.getxattr(sys.argv[1],'user.big').hex()[:8]))", path(c, "B", "px-xattr"))
    assert seen == "7a7a7a7a", seen


def other_xattr_namespaces_are_not_supported(c):
    """#6 security.*, #7 trusted.*, #8 system.posix_acl_*: EOPNOTSUPP (or EPERM from the
    kernel for an unprivileged trusted.* write)."""
    c.write("A", "px-xattr-ns", b"x")
    code = """p=sys.argv[1]
print(json.dumps({"security_set":err(os.setxattr,p,"security.test",b"1"),
 "security_capability_get":err(os.getxattr,p,"security.capability"),
 "trusted_set":err(os.setxattr,p,"trusted.t",b"1"),
 "acl_set":err(os.setxattr,p,"system.posix_acl_access",bytes.fromhex(sys.argv[2])),
 "acl_get":err(os.getxattr,p,"system.posix_acl_access")}))"""
    acl = "0200000001000600ffffffff04000400ffffffff10000400ffffffff20000400ffffffff"
    root = run(c, "A", code, path(c, "A", "px-xattr-ns"), acl, owner=False)
    user = run(c, "A", code, path(c, "A", "px-xattr-ns"), acl)
    c.record("posix.xattr_namespaces", {"root": root, "owner": user})
    assert root["security_set"] == ["err", "EOPNOTSUPP"], root
    assert root["security_capability_get"] == ["err", "EOPNOTSUPP"], root
    assert root["trusted_set"] == ["err", "EOPNOTSUPP"], root
    assert user["trusted_set"] == ["err", "EPERM"], user
    assert user["acl_set"] == ["err", "EOPNOTSUPP"], user
    assert user["acl_get"][0] == "err" and user["acl_get"][1] in ("ENODATA", "EOPNOTSUPP"), user


def mknod_regular_and_device_nodes(c):
    """#9 mknod S_IFREG creates a writable regular file; #11 device nodes keep their rdev,
    and the mount's nodev refuses opening them."""
    mount_options = c.python("A", "print(open('/proc/self/mounts').read())", owner=False)["stdout"]
    result = run(c, "A", """p=sys.argv[1];os.mknod(p,stat.S_IFREG|0o644)
st=os.stat(p);open(p,"wb").write(b"m")
d=sys.argv[2];os.mknod(d,stat.S_IFCHR|0o666,os.makedev(1,3));sd=os.stat(d)
print(json.dumps({"regular":stat.S_ISREG(st.st_mode),"size":st.st_size,"written":os.stat(p).st_size,
 "chr":stat.S_ISCHR(sd.st_mode),"rdev":[os.major(sd.st_rdev),os.minor(sd.st_rdev)],
 "open_device":err(lambda:os.close(os.open(d,os.O_WRONLY)))}))""",
                 path(c, "A", "px-mknod"), path(c, "A", "px-chr"), owner=False)
    nodev = any(" nodev" in line or ",nodev" in line for line in mount_options.splitlines() if "afs-dfs" in line)
    c.record("posix.mknod", {"result": result, "mount_nodev": nodev})
    assert result["regular"] and result["size"] == 0 and result["written"] == 1, result
    assert result["chr"] and result["rdev"] == [1, 3], result
    assert result["open_device"] == (["err", "EACCES"] if nodev else ["ok", None]), result


FIFO_READER = HELPER_IMPORTS + """fd=os.open(sys.argv[2],os.O_RDONLY|os.O_NONBLOCK)
open(sys.argv[1]+"/held.json","w").write("{}")
end=time.monotonic()+120
while not os.path.exists(sys.argv[1]+"/release.json") and time.monotonic()<end: time.sleep(.05)
"""


def fifos_and_sockets_are_node_local(c):
    """#10: FIFO and Unix-socket inodes are shared, their pipe and socket objects are not:
    they work between processes of one node and never connect two nodes."""
    run(c, "A", "os.mkfifo(sys.argv[1]);print(json.dumps(0))", path(c, "A", "px-fifo"))
    c.spawn_python("B", "fifo-reader", FIFO_READER, path(c, "B", "px-fifo"))
    c.await_helper_signal("fifo-reader", "held")
    other = run(c, "A", "print(json.dumps(err(os.open,sys.argv[1],os.O_WRONLY|os.O_NONBLOCK)))",
                path(c, "A", "px-fifo"))
    c.helper_signal("fifo-reader", "release", {})
    c.wait_helper("fifo-reader")
    same = run(c, "A", """p=sys.argv[1];r=os.open(p,os.O_RDONLY|os.O_NONBLOCK);w=os.open(p,os.O_WRONLY)
os.write(w,b"pipe");print(json.dumps(os.read(r,4).decode()))""", path(c, "A", "px-fifo"))
    # Relative names: a workspace path exceeds sun_path's 108 bytes.
    sock = run(c, "A", """os.chdir(os.path.dirname(sys.argv[1]));p=os.path.basename(sys.argv[1])
s=socket.socket(socket.AF_UNIX);s.bind(p);s.listen(1)
k=socket.socket(socket.AF_UNIX);k.connect(p);a,_=s.accept();k.send(b"sock")
print(json.dumps({"same":a.recv(4).decode(),"mode":stat.S_ISSOCK(os.stat(p).st_mode)}))""",
               path(c, "A", "px-sock"), timeout=30)
    remote = run(c, "B", """os.chdir(os.path.dirname(sys.argv[1]));p=os.path.basename(sys.argv[1])
k=socket.socket(socket.AF_UNIX)
print(json.dumps({"connect":err(k.connect,p),"mode":stat.S_ISSOCK(os.stat(p).st_mode)}))""",
                 path(c, "B", "px-sock"))
    c.record("posix.fifo_socket", {"fifo_other_node": other, "fifo_same": same, "sock": sock, "sock_remote": remote})
    assert other == ["err", "ENXIO"], other
    assert same == "pipe", same
    assert sock == {"same": "sock", "mode": True}, sock
    assert remote["mode"] and remote["connect"] == ["err", "ECONNREFUSED"], remote


def hard_links_and_symlinks(c):
    """#12 link and #13 symlink/readlink, seen from another node. Targets are limited by
    the kernel's PATH_MAX (4095 bytes plus NUL) before AFS's own 4096-byte bound."""
    c.write("A", "px-link-src", b"linked")
    result = run(c, "A", """a=sys.argv[1];b=sys.argv[2];s=sys.argv[3]
os.link(a,b);os.symlink("t"*4095,s)
print(json.dumps({"nlink":os.stat(a).st_nlink,"same_ino":os.stat(a).st_ino==os.stat(b).st_ino,
 "readlink_len":len(os.readlink(s)),"too_long":err(os.symlink,"t"*4096,s+"2"),
 "empty":err(os.symlink,"",s+"3")}))""",
                 path(c, "A", "px-link-src"), path(c, "A", "px-link-dst"), path(c, "A", "px-symlink"))
    c.record("posix.links", result)
    assert result["nlink"] == 2 and result["same_ino"] and result["readlink_len"] == 4095, result
    assert result["too_long"] == ["err", "ENAMETOOLONG"] and result["empty"] == ["err", "ENOENT"], result
    other = run(c, "B", """a=sys.argv[1];b=sys.argv[2];os.unlink(a)
print(json.dumps({"nlink":os.stat(b).st_nlink,"data":open(b).read(),"link":len(os.readlink(sys.argv[3]))}))""",
                path(c, "B", "px-link-src"), path(c, "B", "px-link-dst"), path(c, "B", "px-symlink"))
    assert other == {"nlink": 1, "data": "linked", "link": 4095}, other


def rename_flags(c):
    """#14 RENAME_NOREPLACE works; #15 EXCHANGE and WHITEOUT return EINVAL and do not
    disable NOREPLACE for the mount."""
    for name in ("px-ren-a", "px-ren-b"):
        c.write("A", name, name.encode())
    result = run(c, "A", """d=sys.argv[1]
def ren(a,b,flags): return cerr(libc.renameat2(-100,(d+"/"+a).encode(),-100,(d+"/"+b).encode(),flags))
out={"noreplace_existing":ren("px-ren-a","px-ren-b",1),"exchange":ren("px-ren-a","px-ren-b",2),
 "whiteout":ren("px-ren-a","px-ren-c",4),"noreplace_after":ren("px-ren-a","px-ren-c",1),
 "plain_over":ren("px-ren-c","px-ren-b",0)}
out["b"]=open(d+"/px-ren-b").read();print(json.dumps(out))""", c.workspace("A"))
    c.record("posix.rename_flags", result)
    assert result["noreplace_existing"] == ["err", "EEXIST"], result
    assert result["exchange"] == ["err", "EINVAL"] and result["whiteout"] == ["err", "EINVAL"], result
    assert result["noreplace_after"] == ["ok", 0] and result["plain_over"] == ["ok", 0], result
    assert result["b"] == "px-ren-a", result


def direct_io_needs_no_alignment(c):
    """#16: O_DIRECT reads and writes at unaligned offsets and sizes succeed."""
    result = run(c, "A", """fd=os.open(sys.argv[1],os.O_CREAT|os.O_RDWR|os.O_DIRECT,0o644)
os.pwrite(fd,b"abc",1);os.pwrite(fd,b"q"*5000,3);os.fsync(fd)
print(json.dumps({"read":os.pread(fd,6,0).hex(),"size":os.fstat(fd).st_size}))""", path(c, "A", "px-direct"))
    c.record("posix.o_direct", result)
    # "abc" at 1, then 5000 x "q" from 3 over the "c".
    assert result == {"read": "006162717171", "size": 5003}, result


def append_on_one_and_two_nodes(c):
    """#17 O_APPEND from two descriptors of one node; #18 alternating appends from two
    nodes, each re-reading through its own descriptor (S4a: no stale page placement)."""
    single = run(c, "A", """p=sys.argv[1];a=os.open(p,os.O_CREAT|os.O_WRONLY|os.O_APPEND,0o644)
b=os.open(p,os.O_WRONLY|os.O_APPEND)
os.write(a,b"1");os.write(b,b"22");os.write(a,b"333");os.fsync(a);os.fsync(b)
print(json.dumps(open(p).read()))""", path(c, "A", "px-append1"))
    assert single == "122333", single
    c.write("A", "px-append2", b"")
    for step in range(4):
        role = "A" if step % 2 == 0 else "B"
        got = run(c, role, """p=sys.argv[1];fd=os.open(p,os.O_RDWR|os.O_APPEND)
os.write(fd,sys.argv[2].encode()*4);os.fsync(fd);d=os.pread(fd,64,0);os.close(fd)
print(json.dumps(d.decode()))""", path(c, role, "px-append2"), str(step))
        assert got == "".join(str(i) * 4 for i in range(step + 1)), (step, got)
    c.record("posix.o_append", {"single": single, "two_nodes": got})


def mmap_shared_write_is_close_to_open(c):
    """#19: a MAP_SHARED write on a cached handle reaches the file; another node sees it
    on a new open after msync and close."""
    result = run(c, "A", """import mmap
p=sys.argv[1];open(p,"wb").write(b"\\0"*8192)
f=open(p,"r+b");m=mmap.mmap(f.fileno(),8192);m[100:105]=b"mmapd";m.flush();m.close();f.close()
print(json.dumps(open(p,"rb").read()[100:105].decode()))""", path(c, "A", "px-mmap"))
    other = run(c, "B", "print(json.dumps(open(sys.argv[1],'rb').read()[100:105].decode()))", path(c, "B", "px-mmap"))
    c.record("posix.mmap_shared", {"local": result, "other_node": other})
    assert result == "mmapd" and other == "mmapd", (result, other)


def copy_file_range_copies_or_falls_back(c):
    """#20: copy_file_range gives the source bytes (COW clone or kernel fallback);
    nonzero flags are EINVAL."""
    c.write("A", "px-cfr-src", bytes(range(256)) * 64)
    result = run(c, "A", """s=os.open(sys.argv[1],os.O_RDONLY);d=os.open(sys.argv[2],os.O_CREAT|os.O_WRONLY,0o644)
n=os.copy_file_range(s,d,16384);os.fsync(d);os.close(d)
flags=cerr(libc.copy_file_range(s,None,os.open(sys.argv[2],os.O_WRONLY),None,ctypes.c_size_t(10),1))
print(json.dumps({"copied":n,"flags":flags}))""", path(c, "A", "px-cfr-src"), path(c, "A", "px-cfr-dst"))
    c.record("posix.copy_file_range", result)
    assert result == {"copied": 16384, "flags": ["err", "EINVAL"]}, result
    c.read("B", "px-cfr-dst", bytes(range(256)) * 64)


def unsupported_ioctls(c):
    """#21, #36: FS_IOC_GETFLAGS is ENOTTY; FICLONE and FIEMAP are refused by the kernel."""
    c.write("A", "px-ioctl", b"i" * 4096)
    result = run(c, "A", """import array
fd=os.open(sys.argv[1],os.O_RDWR);dst=os.open(sys.argv[2],os.O_CREAT|os.O_RDWR,0o644)
buf=array.array("i",[0])
print(json.dumps({"getflags":err(fcntl.ioctl,fd,0x80086601,buf),"ficlone":err(fcntl.ioctl,dst,0x40049409,fd),
 "fiemap":err(fcntl.ioctl,fd,0xC020660B,bytearray(32))}))""", path(c, "A", "px-ioctl"), path(c, "A", "px-ioctl-dst"))
    c.record("posix.ioctl", result)
    assert result["getflags"] == ["err", "ENOTTY"], result
    assert result["ficlone"][0] == "err" and result["ficlone"][1] in ("EOPNOTSUPP", "EXDEV", "EINVAL"), result
    assert result["fiemap"][0] == "err" and result["fiemap"][1] in ("EOPNOTSUPP", "ENOTTY"), result


def timestamps(c):
    """#22: millisecond precision, UTIME_OMIT/UTIME_NOW, pre-1970 refused (D-S4-2)."""
    c.write("A", "px-time", b"t")
    result = run(c, "A", """p=sys.argv[1]
os.utime(p,ns=(1_700_000_000_123_456_789,1_700_000_001_987_654_321));st=os.stat(p)
out={"atime_ns":st.st_atime_ns,"mtime_ns":st.st_mtime_ns}
UTIME_OMIT=(1<<30)-2
class TS(ctypes.Structure): _fields_=[("s",ctypes.c_long),("ns",ctypes.c_long)]
t=(TS*2)(TS(0,UTIME_OMIT),TS(1_800_000_000,0))
out["omit"]=cerr(libc.utimensat(-100,p.encode(),t,0));out["atime_after_omit"]=os.stat(p).st_atime_ns
before=time.time();os.utime(p,None);out["now_delta"]=abs(os.stat(p).st_mtime-before)
out["pre1970"]=err(os.utime,p,(-86400,-86400))
print(json.dumps(out))""", path(c, "A", "px-time"))
    c.record("posix.timestamps", result)
    assert result["atime_ns"] == 1_700_000_000_123_000_000, result
    assert result["mtime_ns"] == 1_700_000_001_987_000_000, result
    assert result["omit"] == ["ok", 0] and result["atime_after_omit"] == 1_700_000_000_123_000_000, result
    assert result["now_delta"] < 5, result
    assert result["pre1970"] == ["err", "EINVAL"], result


def ownership_and_privilege_bits(c):
    """#23 chmod/chown permission checks; #24/#25 a non-root write or truncate clears
    suid and group-executable sgid (kernel-driven, HANDLE_KILLPRIV_V2 not negotiated);
    a root write keeps them."""
    let_others_traverse(c)
    c.write("A", "px-priv", b"p" * 10)
    result = run(c, "A", """p=sys.argv[1];os.chmod(p,0o6755)
out={"chown_other":err(os.chown,p,int(sys.argv[2]),-1),"chgrp_nonmember":err(os.chown,p,-1,int(sys.argv[3]))}
f=os.open(p,os.O_WRONLY);os.write(f,b"w");os.close(f);out["after_write"]=oct(os.stat(p).st_mode&0o7777)
os.chmod(p,0o6755);os.truncate(p,3);out["after_truncate"]=oct(os.stat(p).st_mode&0o7777)
print(json.dumps(out))""", path(c, "A", "px-priv"), c.config["other_uid"], c.config["other_gid"])
    other = run(c, "A", "print(json.dumps(err(os.chmod,sys.argv[1],0o600)))", path(c, "A", "px-priv"))
    rooted = run(c, "A", """p=sys.argv[1];os.chmod(p,0o6755);f=os.open(p,os.O_WRONLY);os.write(f,b"r");os.close(f)
print(json.dumps(oct(os.stat(p).st_mode&0o7777)))""", path(c, "A", "px-priv"), owner=False)
    c.record("posix.privilege", {"owner": result, "root_write": rooted})
    assert result["chown_other"] == ["err", "EPERM"] and result["chgrp_nonmember"] == ["err", "EPERM"], result
    assert result["after_write"] == "0o755" and result["after_truncate"] == "0o755", result
    assert other == ["ok", None], other
    assert rooted == "0o6755", rooted
    denied = c.command("A", ["python3", "-c", "import os,sys;os.chmod(sys.argv[1],0o600)", path(c, "A", "px-priv")],
                       uid=c.config["other_uid"], gid=c.config["other_gid"], check=False)
    assert denied["returncode"] != 0 and "Operation not permitted" in denied["stderr"], denied


def statfs_reports_the_local_disk(c):
    """#26: statfs reports the Node's own chunk-store disk, not cluster capacity."""
    data = c.role_dir("A") + "/state"
    result = run(c, "A", """a=os.statvfs(sys.argv[1]);b=os.statvfs(sys.argv[2])
print(json.dumps({"blocks":[a.f_blocks,b.f_blocks],"bsize":[a.f_bsize,b.f_bsize],"namemax":a.f_namemax}))""",
                 c.workspace("A"), data, owner=False)
    c.record("posix.statfs", result)
    assert result["blocks"][0] == result["blocks"][1] and result["bsize"][0] == result["bsize"][1], result
    assert result["namemax"] == 255, result


def truncate_extends_with_zeros(c):
    """#27: truncate beyond the end reads back zeros on every node."""
    c.write("A", "px-trunc", b"abc")
    run(c, "A", "os.truncate(sys.argv[1],70000);print(json.dumps(0))", path(c, "A", "px-trunc"))
    c.read("B", "px-trunc", b"abc" + b"\0" * (70000 - 3))


def name_and_path_limits(c):
    """#28: 255-byte names, ENAMETOOLONG at 256; paths are limited by the kernel."""
    result = run(c, "A", """d=sys.argv[1]
print(json.dumps({"n255":err(lambda:open(d+"/"+"n"*255,"w").close()),
 "n256":err(lambda:open(d+"/"+"n"*256,"w").close()),
 "path5000":err(os.stat,d+"/"+"/".join(["x"*200]*25))}))""", c.workspace("A"))
    c.record("posix.name_limits", result)
    assert result["n255"] == ["ok", None] and result["n256"] == ["err", "ENAMETOOLONG"], result
    assert result["path5000"] == ["err", "ENAMETOOLONG"], result


def large_sparse_offsets(c):
    """#29: a write at 1 TiB and at 2^50 creates a sparse file of that size."""
    result = run(c, "A", """out={}
for off in (1<<40,1<<50):
    p=sys.argv[1]+str(off);fd=os.open(p,os.O_CREAT|os.O_RDWR,0o644)
    w=err(os.pwrite,fd,b"E",off);os.fsync(fd)
    out[str(off)]={"write":w,"size":os.fstat(fd).st_size,"read":os.pread(fd,1,off).hex(),
                   "zero":os.pread(fd,4,off-4).hex()}
    os.close(fd)
print(json.dumps(out))""", path(c, "A", "px-sparse-"))
    c.record("posix.large_offsets", result)
    for off in (1 << 40, 1 << 50):
        value = result[str(off)]
        assert value == {"write": ["ok", 1], "size": off + 1, "read": "45", "zero": "00000000"}, (off, value)


def directory_fsync_and_tmpfile_and_poll_and_access(c):
    """#30 fsync on a directory succeeds; #31 O_TMPFILE is EOPNOTSUPP; #32 poll reports a
    regular file ready; #33 access follows the mode bits (kernel default_permissions);
    #34 O_PATH opens without I/O."""
    let_others_traverse(c)
    c.write("A", "px-mode", b"m")
    result = run(c, "A", """d=sys.argv[1];p=d+"/px-mode";os.chmod(p,0o600)
fd=os.open(d,os.O_RDONLY|os.O_DIRECTORY)
out={"fsync_dir":err(os.fsync,fd),"tmpfile":err(os.open,d,os.O_TMPFILE|os.O_WRONLY,0o600)}
f=os.open(p,os.O_RDONLY);po=select.poll();po.register(f,select.POLLIN|select.POLLOUT)
out["poll"]=po.poll(0)[0][1]&(select.POLLIN|select.POLLOUT)
out["access_owner"]=os.access(p,os.R_OK|os.W_OK)
o=os.open(p,os.O_PATH);out["opath_stat"]=os.fstat(o).st_size;out["opath_read"]=err(os.read,o,1)
print(json.dumps(out))""", c.workspace("A"))
    probe = "import os,sys;print(os.access(sys.argv[1],os.F_OK),os.access(sys.argv[1],os.R_OK))"
    other = c.command("A", ["python3", "-c", probe, path(c, "A", "px-mode")],
                      uid=c.config["other_uid"], gid=c.config["other_gid"])
    c.record("posix.misc", result)
    assert result["fsync_dir"] == ["ok", None] and result["tmpfile"] == ["err", "EOPNOTSUPP"], result
    assert result["poll"] == 5 and result["access_owner"] is True, result
    assert result["opath_stat"] == 1 and result["opath_read"] == ["err", "EBADF"], result
    assert other["stdout"].strip() == "True False", other


def xattrs_on_special_files(c):
    """#35: user.* xattrs on a symlink or FIFO are refused by the kernel (EPERM)."""
    result = run(c, "A", """d=sys.argv[1];os.symlink("x",d+"/px-xl");os.mkfifo(d+"/px-xf")
print(json.dumps({"symlink":err(lambda:os.setxattr(d+"/px-xl","user.a",b"1",0,follow_symlinks=False)),
 "fifo":err(os.setxattr,d+"/px-xf","user.a",b"1")}))""", c.workspace("A"))
    c.record("posix.xattr_special", result)
    assert result == {"symlink": ["err", "EPERM"], "fifo": ["err", "EPERM"]}, result


HOLDER = HELPER_IMPORTS + """f=open(sys.argv[2],"rb");first=f.read()
open(sys.argv[1]+"/held.json","w").write(json.dumps({"first":first.decode()}))
end=time.monotonic()+120
while not os.path.exists(sys.argv[1]+"/release.json") and time.monotonic()<end: time.sleep(.05)
f.seek(0);open(sys.argv[1]+"/done.json","w").write(json.dumps({"second":f.read().decode()}))
"""


def open_file_survives_replacement(c):
    """#38: a descriptor opened before the file is replaced by rename (or unlinked) keeps
    reading the opened version (decision 4a)."""
    c.write("A", "px-held", b"old")
    c.spawn_python("B", "replace-holder", HOLDER, path(c, "B", "px-held"))
    c.await_helper_signal("replace-holder", "held")
    c.write("A", "px-new", b"newer")
    run(c, "A", "os.rename(sys.argv[1],sys.argv[2]);print(json.dumps(0))",
        path(c, "A", "px-new"), path(c, "A", "px-held"))
    c.helper_signal("replace-holder", "release", {})
    done = c.await_helper_signal("replace-holder", "done")
    c.wait_helper("replace-holder")
    c.record("posix.replaced_open", done)
    assert done == {"second": "old"}, done
    c.read("B", "px-held", b"newer")


CASES = tuple(
    Case("dfs.posix." + fn.__name__, TAGS, "dfs", (), fn)
    for fn in (
        fallocate_is_not_supported,
        seek_data_hole_treat_the_file_as_data,
        readdir_lists_every_entry_with_its_type,
        file_locks_exclude_on_one_node_only,
        user_xattrs_round_trip,
        other_xattr_namespaces_are_not_supported,
        mknod_regular_and_device_nodes,
        fifos_and_sockets_are_node_local,
        hard_links_and_symlinks,
        rename_flags,
        direct_io_needs_no_alignment,
        append_on_one_and_two_nodes,
        mmap_shared_write_is_close_to_open,
        copy_file_range_copies_or_falls_back,
        unsupported_ioctls,
        timestamps,
        ownership_and_privilege_bits,
        statfs_reports_the_local_disk,
        truncate_extends_with_zeros,
        name_and_path_limits,
        large_sparse_offsets,
        directory_fsync_and_tmpfile_and_poll_and_access,
        xattrs_on_special_files,
        open_file_survives_replacement,
    )
)
