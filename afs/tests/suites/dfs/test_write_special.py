"""Migrated ordinary-write semantics; no throughput or elapsed-time acceptance."""

import json
from pathlib import Path

from fixtures.common import Case, DfsProfile

HERE = Path(__file__).resolve().parent


def verify_files(c, files):
    for name, expected in files.items():
        if not isinstance(expected, dict) or "sha256" not in expected:
            continue
        result = c.python(
            "B",
            'p=sys.argv[1];h=hashlib.sha256();n=0\nwith open(p,"rb",buffering=0) as f:\n'
            ' for b in iter(lambda:f.read(1024**2),b""):h.update(b);n+=len(b)\n'
            'assert n==int(sys.argv[2]) and h.hexdigest()==sys.argv[3];'
            'print(json.dumps({"bytes":n,"sha256":h.hexdigest()}))',
            c.workspace("B") + "/" + name, expected["bytes"], expected["sha256"], timeout=180,
        )
        c.record("write.remote_verify." + name, json.loads(result["stdout"]))


def physical_replicas(c):
    bodies = {}
    for role in ("A", "B"):
        value = c.python(
            role,
            'from pathlib import Path\np=Path(sys.argv[1]);r={}\n'
            'for f in sorted(p.iterdir()):\n'
            ' if not f.is_file():continue\n h=hashlib.sha256()\n'
            ' with f.open("rb") as s:\n  for b in iter(lambda:s.read(1024**2),b""):h.update(b)\n'
            ' r[f.name]={"bytes":f.stat().st_size,"sha256":h.hexdigest()}\n'
            'assert r,"no physical replica bodies";print(json.dumps(r))',
            c.role_dir(role) + "/state/dfs/chunks", owner=False,
        )
        bodies[role] = json.loads(value["stdout"])
    assert bodies["A"] == bodies["B"], bodies
    c.record("write.physical_replicas_after_recovery", bodies)


def probe(c, action):
    result = c.python("A", (HERE / "write_probe.py").read_text(), action, c.workspace("A"), timeout=240)
    value = json.loads(result["stdout"])
    assert value["passed"]
    c.record("write." + action, value)
    verify_files(c, value["files"])


def order(c):
    probe(c, "order")


def barriers(c):
    probe(c, "barriers")


def bounded(c):
    probe(c, "bounded")


def failed_replica(c, action):
    name = "retained-writer"
    c.spawn_python("A", name, (HERE / "held_writer.py").read_text(), c.workspace("A") + "/retained", action)
    c.await_helper_signal(name, "opened")
    c.stop("B")
    c.helper_signal(name, "write", {})
    failed = c.await_helper_signal(name, "failed", timeout=120)
    assert failed["accepted_bytes"] > 0 and failed["fsync_errno"] is not None
    if action == "close":
        c.await_helper_signal(name, "done", timeout=120)
        c.wait_helper(name)
        c.start("B")
        c.write("A", "healthy-after-close", b"healthy" * 4096)
        c.read("B", "healthy-after-close", b"healthy" * 4096)
    else:
        c.start("B")
        c.helper_signal(name, "recover", {})
        done = c.await_helper_signal(name, "done", timeout=120)
        c.wait_helper(name)
        verify_files(c, {"retained": done})
        physical_replicas(c)


def recovery(c):
    failed_replica(c, "recover")


def failed_close(c):
    failed_replica(c, "close")


def cases():
    result = []
    for prefix, tag in [("local", "local-write-special"), ("cluster", "cluster-write-special")]:
        for name, function, copies in [
            ("cross_handle_order", order, 1), ("sync_append_close", barriers, 1),
            ("bounded_sync_progress", bounded, 2), ("required_replica_recovery", recovery, 2),
            ("failed_close_retirement", failed_close, 2),
        ]:
            result.append(Case(
                "dfs.write." + prefix + "." + name, (tag,), "dfs", (), function,
                profile=DfsProfile(copies=copies, node_count=2, write_queue_bytes=64 * 1024**2),
            ))
    return tuple(result)


CASES = cases()
