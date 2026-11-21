"""Measure an old OwnerFs Home starting against a newly adopted workspace."""

import json
from posixpath import join as remote_join

from tools import rolling_ownerfs_mixed as mixed
from tools import version_switch as offline

REJECTION_TEXT = "local root catalog identity is not canonical"
REJECTION_CODE = "ErrorCode(34275329)"
META_IDENTITY_KEYS = ("root_id", "root_epoch", "home_node_id")
FINGERPRINT = """
from pathlib import Path

def inspect(path):
    try:
        info = path.lstat()
    except FileNotFoundError:
        return None
    value = {"inode": info.st_ino, "mode": stat.S_IMODE(info.st_mode),
             "uid": info.st_uid, "gid": info.st_gid}
    if stat.S_ISREG(info.st_mode):
        body = path.read_bytes()
        value.update(bytes=len(body), sha256=hashlib.sha256(body).hexdigest())
    elif stat.S_ISDIR(info.st_mode):
        value["entries"] = sorted(entry.name for entry in path.iterdir())
    else:
        value["file_type"] = stat.S_IFMT(info.st_mode)
    return value

print(json.dumps({key: inspect(Path(path)) for key, path in zip(
    ("catalog", "workspace", "original", "mutated"), sys.argv[1:],
)}))
"""


def native_fingerprint(cluster):
    store = mixed.native_store(cluster)
    workspace = remote_join(store, mixed.WORKSPACE)
    result = cluster.python(
        "B", FINGERPRINT,
        remote_join(store, ".ownerfs-roots", mixed.ROOT_ID + ".json"),
        workspace,
        remote_join(workspace, mixed.SEED_FILE),
        remote_join(workspace, mixed.MUTATED_FILE),
        owner=False,
    )
    return json.loads(result["stdout"])


def retained_file():
    return {"mode": 0o600, **mixed.checksum(mixed.SEED_BODY + mixed.APPEND_BODY)}


def assert_mutated_file(actual):
    expected = retained_file()
    if actual is None or any(actual.get(key) != value for key, value in expected.items()):
        raise AssertionError(f"retained OwnerFs file differs from the acknowledged write: {actual}")


def check_retained_workspace(cluster, token):
    """Verify ordinary FUSE reads, local contents and the adopted Home identity."""
    identity = mixed.observe_adoption(cluster)
    if identity != token["retained_identity"]:
        raise AssertionError(f"retained OwnerFs root identity changed: {identity}")
    views = {}
    for role in ("A", "B"):
        path = remote_join(cluster.mount(role), mixed.WORKSPACE, mixed.MUTATED_FILE)
        cluster.poll_python(role, "os.path.exists(a[0])", path, label="retained-ownerfs-" + role.lower())
        views[role] = mixed.read_file(cluster, role, path)
        assert_mutated_file(views[role])
    native = native_fingerprint(cluster)
    assert_mutated_file(native["mutated"])
    if native["mutated"]["inode"] != token["seed"]["inode"] or native["original"] is not None:
        raise AssertionError("the retained native workspace lost its renamed inode")
    cluster.record("rollout.ownerfs.retained_workspace", {"identity": identity, "views": views})


def reap_home(cluster):
    channel = cluster.channel("B")
    state = channel.call("status").get("B")
    if state is not None:
        try:
            cluster.stop("B")
        except RuntimeError:
            if state["returncode"] in (None, 0) or "B" in channel.call("status"):
                raise
            cluster.record("exited_before_stop.B", state)
    snapshot = channel.call("snapshot")
    if "B" in snapshot["processes"] or any(
        row.split()[4] == cluster.mount("B") for row in snapshot["mounts"]
    ):
        raise AssertionError("OwnerFs downgrade attempt left Node B running or mounted")


def start_on_retained_state(cluster, old, log_path, log_offset):
    try:
        offline.start_role(cluster, "B", old)
    except RuntimeError:
        state = cluster.channel("B").call("status").get("B")
        if state is None or state["returncode"] in (None, 0):
            raise
        output = cluster.python(
            "B", 'with open(sys.argv[1],"rb") as stream:\n'
            ' stream.seek(int(sys.argv[2]))\n sys.stdout.write(stream.read().decode(errors="replace"))',
            log_path, log_offset, owner=False,
        )["stdout"]
        reason = next((line for line in output.splitlines() if REJECTION_TEXT in line and REJECTION_CODE in line), "")
        cluster.record("rollout.ownerfs.retained_downgrade_exit", {
            **state, "log_offset": log_offset, "reason": reason, "commit": old["commit"],
        })
        if not reason:
            raise
        return "incompatible_rejected"
    return "safe_takeover"


def attempt_retained_downgrade(cluster, old_inputs, old, token):
    """Try the old B binary before changing any persisted Home state."""
    token["retained_identity"] = mixed.observe_adoption(cluster)
    cluster.stop("B")
    before = native_fingerprint(cluster)
    cluster.record("rollout.ownerfs.retained_before_downgrade", before)
    assert_mutated_file(before["mutated"])
    if not all(before[key] for key in ("catalog", "workspace")) or before["original"] is not None:
        raise AssertionError(f"expected a durable adopted root and renamed file before downgrade: {before}")
    if before["mutated"]["inode"] != token["seed"]["inode"]:
        raise AssertionError("the adopted file inode changed before the retained-state attempt")
    mixed.restore_settings(cluster, token)
    offline.install(cluster, old_inputs, old, roles=("B",))
    binary = remote_join(cluster.root("B"), "bin/afs-node")
    observed = cluster.channel("B").call("hash", path=binary)["sha256"]
    if observed != old["binaries"]["afs-node"]:
        raise AssertionError("wrong stored Node B binary at retained-state downgrade")
    cluster.record("rollout.ownerfs.retained_downgrade_binary", {"commit": old["commit"], "sha256": observed})
    log_path = remote_join(cluster.root("B"), "B.log")
    log_offset = int(cluster.python("B", 'print(os.path.getsize(sys.argv[1]))', log_path, owner=False)["stdout"])
    try:
        outcome = start_on_retained_state(cluster, old, log_path, log_offset)
        if outcome == "safe_takeover":
            check_retained_workspace(cluster, token)
    finally:
        reap_home(cluster)
        after = native_fingerprint(cluster)
        cluster.record("rollout.ownerfs.retained_after_downgrade", after)
        if before != after:
            cluster.record("rollout.ownerfs.retained_downgrade_result", "state_changed")
            raise AssertionError("starting the old Node changed the adopted catalog or workspace on disk")
    metadata = offline.root_location(cluster, mixed.WORKSPACE)
    stable_identity = {key: token["retained_identity"][key] for key in META_IDENTITY_KEYS}
    observed_identity = {key: metadata[key] for key in META_IDENTITY_KEYS}
    cluster.record("rollout.ownerfs.retained_downgrade_meta", {
        "before": {**stable_identity, "status": "serving"},
        "after": {**observed_identity, "status": metadata["status"]},
    })
    if observed_identity != stable_identity:
        cluster.record("rollout.ownerfs.retained_downgrade_result", "state_changed")
        raise AssertionError("starting the old Node changed the adopted root identity in Meta")
    cluster.record("rollout.ownerfs.retained_downgrade_result", outcome)
    return outcome
