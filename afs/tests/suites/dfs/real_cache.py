"""Real DFS H1/H2/H3 contract, using one non-root process and two real mounts.

This tests the same externally observable assertions as fuse_contract.rs. Keeping
this runtime probe in Python makes it runnable with a release package; the Rust
version remains an optional independent cross-check of the supplied test ELF.
"""

import json

# Executed inside the selected Linux worker, never on the macOS controller.
PROBE = (
    "\nimport contextlib, errno, hashlib, json, os, pathlib, sys, tempfile"
    ", urllib.request\nwriter, reader, address = pathlib.Path(sys.argv[1])"
    ", pathlib.Path(sys.argv[2]), sys.argv[3]\nassert os.geteuid() != 0\nas"
    "sert writer.stat().st_dev != reader.stat().st_dev\n\ndef counter():\n  "
    "  with urllib.request.urlopen('http://' + address + '/metrics', time"
    "out=3) as response:\n        raw = response.read(2 * 1024 * 1024 + 1)"
    "\n    assert len(raw) <= 2 * 1024 * 1024\n    values = [int(line.rspli"
    "t(' ',1)[1]) for line in raw.decode().splitlines()\n              if "
    "line.startswith('afs_fuse_callbacks_total{') and 'filesystem=\"dfs\"' "
    "in line and 'operation=\"read\"' in line]\n    assert len(values) == 1,"
    " values\n    return values[0]\n\ndef durable(path, body, create):\n    w"
    "ith path.open('wb' if create else 'r+b') as stream:\n        stream.w"
    "rite(body); stream.flush(); os.fsync(stream.fileno())\n\ndef full(stre"
    "am, body):\n    stream.seek(0); actual = stream.read()\n    assert len"
    "(actual) == len(body)\n    assert actual == body and stream.read(1) ="
    "= b''\n    return hashlib.sha256(actual).hexdigest()\n\nexpected = byte"
    "s(range(256)) * 65536\nwith tempfile.TemporaryDirectory(prefix='afs-h"
    "123-', dir=writer) as folder, contextlib.ExitStack() as stack:\n    a"
    " = pathlib.Path(folder) / 'data'\n    b = reader / a.relative_to(writ"
    "er)\n    durable(a, expected, True)\n    inode = a.stat().st_ino\n    h"
    "1 = stack.enter_context(b.open('rb', buffering=0))\n    assert h1.rea"
    "d(4096) == expected[:4096]\n    before_v2 = counter()\n    expected = "
    "expected.translate(bytes(255 - i for i in range(256)))\n    durable(a"
    ", expected, False)\n    assert a.stat().st_ino == inode\n    h2 = stac"
    "k.enter_context(b.open('rb', buffering=0))\n    assert h2.read(4096) "
    "== expected[:4096]\n    after_h2 = counter(); assert after_h2 > befor"
    "e_v2\n    assert os.pread(h1.fileno(), 4096, 12 * 1024 * 1024) == exp"
    "ected[12 * 1024 * 1024:12 * 1024 * 1024 + 4096]\n    after_h1 = count"
    "er(); assert after_h1 > after_h2\n    digest = full(h2, expected)\n   "
    " before_h3 = counter()\n    h3 = stack.enter_context(b.open('rb', buf"
    "fering=0))\n    assert full(h3, expected) == digest\n    after_h3 = co"
    "unter(); assert after_h3 == before_h3\n    os.chmod(a, 0)\n    denied "
    "= None\n    try:\n        with b.open('rb'):\n            pass\n    exce"
    "pt OSError as exc:\n        denied = exc.errno\n    finally:\n        o"
    "s.chmod(a, 0o600)\n    assert denied == errno.EACCES, denied\n    with"
    " b.open('rb', buffering=0) as restored:\n        assert full(restored"
    ", expected) == digest\n    print(json.dumps({'bytes': len(expected), "
    "'sha256': digest, 'same_inode': inode,\n                      'uid': "
    "os.geteuid(), 'eof': True, 'new_open_mode_000_denied': True,\n       "
    "               'old_handle_snapshot_promised': False,\n              "
    "        'read_callbacks': {'before_v2': before_v2, 'after_h2': after"
    "_h2,\n                                        'after_h1_tail': after_"
    "h1, 'before_h3': before_h3, 'after_h3': after_h3}}))\n"
)


def run(c):
    assert c.config["kind"] == "singlevm", "both real mounts must be visible to this worker"
    c.python("A", "os.chown(sys.argv[1],int(sys.argv[2]),int(sys.argv[3]))", c.mount("A"), c.uid, c.gid, owner=False)
    code = (
        'd=os.stat(sys.argv[1]).st_dev;p="/sys/class/bdi/%s:%s/read_ahead_kb"'
        '%(os.major(d),os.minor(d));v=open(p).read().strip();open(p,"w").writ'
        'e("0");print(v)'
    )
    before = c.python("B", code, c.mount("B"), owner=False)["stdout"].strip()
    try:
        result = c.command(
            "A",
            ["python3", "-c", PROBE, c.mount("A"), c.mount("B"), c.address("B") + ":" + str(c.ports("B")[1])],
            uid=c.uid,
            gid=c.gid,
            timeout=180,
        )
        c.record("real_dfs_cross_handle_cache", json.loads(result["stdout"]))
    finally:
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
