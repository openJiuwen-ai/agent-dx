"""Test documented offline AFS upgrades and rollback on test-owned stores."""

import json
from pathlib import Path
import signal
import traceback
import uuid

from fixtures.common import Blocked, Case, sha256, write_json
from fixtures.multivm import Cluster, validate
from suites.operations import wait_read

SEED = b"old build: confirmed before switch\n" * 8192
AFTER = b"new build: written after switch\n" * 8192


def quiesce(cluster):
    for role in reversed(cluster.roles):
        channel = cluster.channel(role)
        state = channel.call("status").get(role)
        if state is None:
            continue
        try:
            cluster.stop(role)
        except RuntimeError:
            if state["returncode"] is None or role in channel.call("status"):
                raise
            cluster.record("exited_before_stop." + role, state)
    for role in cluster.roles:
        if cluster.channel(role).call("status") or cluster.channel(role).call("snapshot")["mounts"]:
            raise AssertionError("processes or mounts remain before touching persistent state: " + role)


def recovery_point(cluster, restore=False):
    for role in cluster.roles:
        state = Path(cluster.role_dir(role)) / "state"
        backup = state.with_name("state-before-upgrade")
        if restore:
            cluster.command(role, ["mv", "-T", state, state.with_name("state-after-upgrade")])
            cluster.command(role, ["cp", "-aT", backup, state], timeout=60)
        else:
            cluster.command(role, ["cp", "-aT", state, backup], timeout=60)
        cluster.record(("restore." if restore else "backup.") + role, str(backup))


def install(cluster, inputs, identity):
    for role in cluster.roles:
        name = "afs-meta" if role == "M" else "afs-node"
        destination = cluster.root(role) + "/bin/" + name
        channel = cluster.channel(role)
        received = channel.upload(inputs / name, destination + ".next", 0o755)
        if received != identity["binaries"][name]:
            raise AssertionError("binary changed during transfer: " + role)
        cluster.command(role, ["mv", "-T", destination + ".next", destination])


def running_identity(cluster, role, identity):
    pid = cluster.channel(role).call("status")[role]["pid"]
    binary = "afs-meta" if role == "M" else "afs-node"
    running = cluster.command(role, ["sha256sum", f"/proc/{pid}/exe"])["stdout"].split()[0]
    if running != identity["binaries"][binary]:
        raise AssertionError("wrong running binary: " + role)
    cluster.record("running." + role, {"pid": pid, "sha256": running, "commit": identity["commit"]})


def start_role(cluster, role, identity):
    cluster.start(role)
    running_identity(cluster, role, identity)


def start_all(cluster, identity):
    for role in cluster.roles:
        start_role(cluster, role, identity)
    cluster.topology()


def file_state(cluster):
    value = cluster.python(
        "B",
        "s=os.stat(sys.argv[1]);print(json.dumps([s.st_uid,s.st_gid,stat.S_IMODE(s.st_mode),s.st_size]))",
        cluster.workspace("B") + "/seed",
    )["stdout"]
    return json.loads(value)


def root_location(cluster, name):
    root_id = "root-" + name.encode().hex()
    return json.loads(cluster.health("M", "/v1/roots/" + root_id))


def owner_identity(cluster):
    location = root_location(cluster, "workspace")
    if location["status"] != "serving":
        raise AssertionError(location)
    return {key: location[key] for key in ("root_id", "root_epoch", "home_node_id")}


def check_seed(cluster, expected, root_identity):
    wait_read(cluster, "B", "seed", SEED)
    if file_state(cluster) != expected:
        raise AssertionError("pre-upgrade file length, owner or mode changed")
    if root_identity is not None:
        actual = owner_identity(cluster)
        cluster.record("ownerfs.workspace_identity", actual)
        if actual != root_identity:
            raise AssertionError(f"workspace identity changed: {root_identity} -> {actual}")
        cluster.command(
            "B", ["python3", "-c",
                  "import errno,sys\ntry:open(sys.argv[1],'rb').close()\n"
                  "except OSError as exc:assert exc.errno==errno.EACCES,exc\n"
                  "else:raise AssertionError('non-owner unexpectedly opened workspace file')",
                  cluster.workspace("B") + "/seed"],
            uid=cluster.config["other_uid"], gid=cluster.config["other_gid"],
        )


def run_phase(result, name, operation):
    try:
        operation()
    except (InterruptedError, TimeoutError):
        raise
    except Exception as exc:
        result[name] = {"status": "failed", "error": f"{type(exc).__name__}: {exc}",
                        "traceback": traceback.format_exc()}
    else:
        result[name] = {"status": "passed"}


def exercise_switch(cluster, old_inputs, new_inputs, old, new, phases):
    cluster.write("A", "seed", SEED)
    cluster.python("A", "os.chmod(sys.argv[1],0o640)", cluster.workspace("A") + "/seed")
    wait_read(cluster, "B", "seed", SEED)
    expected = file_state(cluster)
    location = owner_identity(cluster) if cluster.mode == "ownerfs" else None
    quiesce(cluster)
    recovery_point(cluster)

    def upgrade():
        install(cluster, new_inputs, new)
        start_all(cluster, new)
        check_seed(cluster, expected, location)
        cluster.write("B", "after-upgrade", AFTER)
        wait_read(cluster, "A", "after-upgrade", AFTER)

    def rollback():
        quiesce(cluster)
        recovery_point(cluster, restore=True)
        install(cluster, old_inputs, old)
        start_all(cluster, old)
        check_seed(cluster, expected, location)
        value = cluster.python(
            "B", "try:open(sys.argv[1],'rb').close()\n"
            "except OSError as exc:assert exc.errno==errno.ENOENT,exc\n"
            "else:raise AssertionError('post-snapshot file is present after rollback')",
            cluster.workspace("B") + "/after-upgrade",
        )
        cluster.record("post_snapshot_file_absent", value["returncode"] == 0)
        cluster.write("B", "after-rollback", SEED)
        wait_read(cluster, "A", "after-rollback", SEED)

    run_phase(phases, "upgrade", upgrade)
    run_phase(phases, "rollback", rollback)


def command_counts(cluster, label):
    store = json.loads(cluster.health("M", "/v1/store"))
    entities = store["entities"]
    counts = {"commands": entities.get("root_command", 0), "acks": entities.get("root_command_ack", 0)}
    cluster.record(label, {**counts, "revision": store["revision"], "watch_floor": store["watch_floor"]})
    return counts


def remove_workspace(cluster, name):
    path = cluster.mount("A") + "/" + name
    cluster.python("A", "os.rmdir(sys.argv[1])", path, owner=False)
    root_id = "root-" + name.encode().hex()
    value = cluster.python(
        "M", "import urllib.error,urllib.request\n"
        "opener=urllib.request.build_opener(urllib.request.ProxyHandler({}))\n"
        "try:opener.open(sys.argv[1],timeout=5).close()\n"
        "except urllib.error.HTTPError as exc:assert exc.code==404,exc\n"
        "else:raise AssertionError('removed root is still registered')",
        f"http://{cluster.address('M')}:{cluster.ports('M')[1]}/v1/roots/{root_id}",
        owner=False,
    )
    cluster.record("removed_workspace", {"name": name, "root_id": root_id, "returncode": value["returncode"]})


def retained_command(cluster, new_inputs, new, phases):
    if command_counts(cluster, "old_before_removal") != {"commands": 0, "acks": 0}:
        raise AssertionError("root command seed must start from an empty test store")
    original = root_location(cluster, "workspace")
    remove_workspace(cluster, "workspace")
    if command_counts(cluster, "old_after_removal") != {"commands": 1, "acks": 0}:
        raise AssertionError("old release did not produce exactly one unacknowledged command")
    quiesce(cluster)

    def verify_after_upgrade():
        install(cluster, new_inputs, new)
        start_role(cluster, "M", new)
        after_upgrade = command_counts(cluster, "new_meta_before_nodes")
        for role in cluster.roles[1:]:
            start_role(cluster, role, new)
        last_epoch = original["root_epoch"]
        for index in range(3):
            cluster.python("A", "os.mkdir(sys.argv[1])", cluster.mount("A") + "/workspace", owner=False)
            current = root_location(cluster, "workspace")
            if current["root_epoch"] <= last_epoch:
                raise AssertionError("recreated workspace did not advance its epoch")
            last_epoch = current["root_epoch"]
            cluster.record("recreated_workspace." + str(index), current)
            remove_workspace(cluster, "workspace")
            command_counts(cluster, "new_after_removal." + str(index))
        quiesce(cluster)
        start_role(cluster, "M", new)
        counts = command_counts(cluster, "new_meta_after_restart")
        if counts["commands"]:
            raise AssertionError(f"old unacknowledged RootCommand survived upgrade and restart: "
                                 f"after_upgrade={after_upgrade}, after_restart={counts}")

    run_phase(phases, "root_command_after_upgrade", verify_after_upgrade)


def exercise(config, old_inputs, new_inputs, output):
    """Run all scenarios, retaining an independent result and cleanup per fixture."""
    output.mkdir()
    identities = [json.loads((inputs / "artifact-identity.json").read_text()) for inputs in (old_inputs, new_inputs)]
    old, new = identities
    if old["commit"] == new["commit"] or old["target"] != new["target"]:
        raise ValueError("expected distinct commits of the same target")
    for inputs, identity in zip((old_inputs, new_inputs), identities):
        for binary in ("afs-meta", "afs-node"):
            if sha256(inputs / binary) != identity["binaries"][binary]:
                raise ValueError("binary differs from pinned package manifest: " + binary)
    write_json(output / "versions.json", {"old": old, "new": new})
    entries = []
    cleanup_errors = []
    for mode, suffix in (("ownerfs", "upgrade_rollback"), ("dfs", "upgrade_rollback"),
                         ("ownerfs", "old_root_command")):
        case = Case(f"{mode}.version.{suffix}", (), mode, (), None, observe_cache=True)
        case_output = output / case.id
        case_output.mkdir()
        result = {"id": case.id, "status": "failed", "phases": {}}
        entries.append(result)
        validate(config, [case])
        cluster = Cluster(config, old_inputs, case_output, case, "afs-test-" + uuid.uuid4().hex)
        signal.alarm(480)
        try:
            if cluster.meta_store_toml != 'meta_store = "local-file"\n':
                raise Blocked("version switching requires a private local-file Meta store")
            cluster.prepare(ownerfs_bind=False)
            for role in cluster.roles:
                running_identity(cluster, role, old)
            if suffix == "old_root_command":
                retained_command(cluster, new_inputs, new, result["phases"])
            else:
                exercise_switch(cluster, old_inputs, new_inputs, old, new, result["phases"])
            if all(phase["status"] == "passed" for phase in result["phases"].values()):
                result["status"] = "passed"
        except (InterruptedError, TimeoutError):
            raise
        except Exception as exc:
            result["error"] = f"{type(exc).__name__}: {exc}"
            result["traceback"] = traceback.format_exc()
            if isinstance(exc, Blocked):
                result["status"] = "blocked"
        finally:
            signal.alarm(0)
            cleanup = cluster.cleanup()
            if cleanup["status"] != "PASS":
                result["status"] = "failed"
                cleanup_errors.extend(cleanup["errors"])
            write_json(output / "result.json", {"cases": entries, "cleanup_errors": cleanup_errors})
            write_json(output / "cleanup.json", {"status": "FAIL" if cleanup_errors else "PASS",
                                                  "errors": cleanup_errors})
        if cleanup_errors:
            break
    return entries, cleanup_errors
