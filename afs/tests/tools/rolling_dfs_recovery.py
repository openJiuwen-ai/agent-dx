"""Verify a DFS write acknowledged while its replica is offline during rollout."""

from dataclasses import dataclass
from hashlib import sha256
import json
from posixpath import join as remote_join
import time

from suites.operations import wait_read
from tools import version_switch as offline

NAME = "rollout-dfs-repair"
BEFORE = sha256(b"replica data before version switch").digest() * 2048
AFTER = sha256(b"replica data acknowledged while B is offline").digest() * 2048

FIND_CHUNKS = """from pathlib import Path

path = Path(sys.argv[1])
size, digest = int(sys.argv[2]), sys.argv[3]
matching = []
for item in path.iterdir():
    try:
        if item.is_file() and item.stat().st_size == size and hashlib.sha256(item.read_bytes()).hexdigest() == digest:
            matching.append(item.name)
    except FileNotFoundError:
        # A concurrent store cleanup can remove an unrelated chunk.
        continue
print(json.dumps(sorted(matching)))
"""

OVERWRITE = """path, inode = sys.argv[1], int(sys.argv[2])
body = bytes.fromhex(sys.stdin.read())
with open(path, "r+b", buffering=0) as stream:
    assert os.fstat(stream.fileno()).st_ino == inode, "established file identity changed"
    size = stream.write(body)
    assert size == len(body), "short write while replica is offline"
    os.fsync(stream.fileno())
    print(json.dumps({"inode": inode, "bytes": size, "sha256": hashlib.sha256(body).hexdigest()}), flush=True)
"""


@dataclass
class RecoveryState:
    inode: int
    original_chunk: str
    acknowledged_chunk: str | None = None


def matching_chunks(cluster, role, body):
    value = cluster.python(
        role, FIND_CHUNKS, remote_join(cluster.role_dir(role), "state/dfs/chunks"),
        len(body), sha256(body).hexdigest(), owner=False,
    )
    return json.loads(value["stdout"])


def wait_for_chunk(cluster, role, body, name=None):
    deadline = time.monotonic() + 60
    while True:
        matches = matching_chunks(cluster, role, body)
        if len(matches) > 1:
            raise AssertionError(f"{role}: more than one chunk matches the unique rollout payload: {matches}")
        if matches and (name is None or matches == [name]):
            return matches[0]
        if time.monotonic() >= deadline:
            raise AssertionError(f"{role}: durable rollout chunk did not appear: expected={name}, found={matches}")
        time.sleep(0.5)


def assert_stopped(cluster, role, checkpoint):
    snapshot = cluster.channel(role).call("snapshot")
    mount = cluster.mount(role)
    if role in snapshot["processes"] or any(line.split()[4] == mount for line in snapshot["mounts"]):
        raise AssertionError(f"{role}: process or mount still active during {checkpoint}")
    cluster.record("rollout.dfs." + checkpoint, {"role": role, "running": False, "mounted": False})


def seed(cluster):
    if cluster.roles != ["M", "A", "B"]:
        raise ValueError("replica-source isolation needs exactly two DFS Nodes")
    for role in ("A", "B"):
        if cluster.effective[role].get("dfs_external_backend") is not None:
            raise ValueError("replica-source isolation requires native DFS storage")
    meta = cluster.effective["M"]
    policy_keys = ("dfs_desired_copies", "dfs_sync_required_copies", "dfs_replica_failure_policy",
                   "dfs_min_degraded_copies")
    policy = {key: meta.get(key) for key in policy_keys}
    expected = dict(zip(policy_keys, (2, 2, "degrade", 1)))
    if policy != expected:
        raise ValueError(f"unexpected DFS replica policy: {policy}")
    cluster.record("rollout.dfs.policy", policy)
    cluster.write("A", NAME, BEFORE)
    wait_read(cluster, "B", NAME, BEFORE)
    original_chunk = wait_for_chunk(cluster, "A", BEFORE)
    wait_for_chunk(cluster, "B", BEFORE, original_chunk)
    result = cluster.python(
        "A", "print(os.stat(sys.argv[1]).st_ino)", remote_join(cluster.workspace("A"), NAME),
    )
    inode = int(result["stdout"])
    cluster.record("rollout.dfs.established", {
        "path": NAME, "inode": inode, "chunk": original_chunk, "nodes": ["A", "B"],
        "bytes": len(BEFORE), "sha256": sha256(BEFORE).hexdigest(),
    })
    return RecoveryState(inode, original_chunk)


def write_while_offline(cluster, state):
    assert_stopped(cluster, "B", "before_write")
    value = cluster.python(
        "A", OVERWRITE, remote_join(cluster.workspace("A"), NAME), state.inode, stdin=AFTER.hex(),
        check=False, timeout=60,
    )
    receipt = json.loads(value["stdout"]) if value["stdout"].strip() else None
    cluster.record("rollout.dfs.offline_write", {
        "path": NAME, "fsync": receipt, "returncode": value["returncode"], "stderr": value["stderr"],
    })
    assert_stopped(cluster, "B", "after_write")
    expected = {"inode": state.inode, "bytes": len(AFTER), "sha256": sha256(AFTER).hexdigest()}
    if value["returncode"] != 0 or receipt != expected:
        raise AssertionError("established file write was not acknowledged while B was offline")
    state.acknowledged_chunk = wait_for_chunk(cluster, "A", AFTER)
    if state.acknowledged_chunk == state.original_chunk:
        raise AssertionError("the acknowledged write did not create a distinct DFS chunk")
    if matching_chunks(cluster, "B", AFTER):
        raise AssertionError("new content was already on B before replacement start")
    cluster.record("rollout.dfs.expected_repair", {
        "path": NAME, "chunk": state.acknowledged_chunk, "source": "A", "target": "B",
    })
    return {NAME: AFTER}


def verify_recovered(cluster, state, active_identities):
    if state.acknowledged_chunk is None:
        raise AssertionError("no acknowledged write to recover")
    offline.running_identity(cluster, "B", active_identities["B"])
    wait_for_chunk(cluster, "B", AFTER, state.acknowledged_chunk)
    cluster.stop("A")
    try:
        assert_stopped(cluster, "A", "before_replica_read")
        # This is the first open of this file on the new B FUSE mount. The
        # only other Node is stopped and this fixture has no external store.
        wait_read(cluster, "B", NAME, AFTER)
        assert_stopped(cluster, "A", "after_replica_read")
        cluster.record("rollout.dfs.recovered_replica_read", {
            "path": NAME, "source": "B", "other_node": "A", "other_node_stopped": True,
            "chunk": state.acknowledged_chunk, "bytes": len(AFTER), "sha256": sha256(AFTER).hexdigest(),
        })
    finally:
        offline.start_role(cluster, "A", active_identities["A"])
        cluster.topology()


def verify_snapshot(cluster, state):
    wait_for_chunk(cluster, "B", BEFORE, state.original_chunk)
    wait_read(cluster, "B", NAME, BEFORE)
