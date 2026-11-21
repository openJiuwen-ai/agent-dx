"""Verify a legacy client mutates a workspace adopted by the upgraded Home."""

import hashlib
import json
import math
from posixpath import join as remote_join
import time

WORKSPACE = "rollout-adopted-workspace"
ROOT_ID = "root-" + WORKSPACE.encode().hex()
SEED_FILE = "adopted-before-switch"
MUTATED_FILE = "renamed-by-old-node"
SEED_BODY = b"ordinary data on the future Home before the upgrade\n" * 128
APPEND_BODY = b"fsynced through the old Node while versions were mixed\n" * 1024
ADOPT_SETTING = "\nownerfs_adopt_on_startup = true\n"


def native_store(cluster):
    return remote_join(cluster.role_dir("B"), "state", "ownerfs")


def require_stopped_home(cluster):
    if "B" in cluster.channel("B").call("status"):
        raise AssertionError("Node B must be stopped before changing its adoption fixture")


def node_config(cluster):
    return remote_join(cluster.role_dir("B"), "config.toml")


def prepare_adoption(cluster):
    """Seed ordinary files on stopped B and return its original settings for rollback."""
    require_stopped_home(cluster)
    settings = cluster.python(
        "B",
        'from pathlib import Path\nsys.stdout.write(Path(sys.argv[1]).read_text())',
        node_config(cluster),
        owner=False,
    )["stdout"]
    if "ownerfs_adopt_on_startup" in settings:
        raise AssertionError("baseline fixture already sets OwnerFs startup adoption")
    prepared = cluster.python(
        "B",
        'from pathlib import Path\n'
        'store=Path(sys.argv[1]); workspace=store/sys.argv[2]; uid,gid=map(int,sys.argv[4:6])\n'
        'assert not list((store/".ownerfs-roots").glob("*.json")), "B unexpectedly owns a legacy catalog record"\n'
        'workspace.mkdir(mode=0o700); os.chown(workspace,uid,gid)\n'
        'seed=workspace/sys.argv[3]; data=bytes.fromhex(sys.stdin.read())\n'
        'with seed.open("xb") as stream:\n'
        ' os.fchown(stream.fileno(),uid,gid); os.fchmod(stream.fileno(),0o640)\n'
        ' stream.write(data); stream.flush(); os.fsync(stream.fileno())\n'
        'for directory in (workspace,store):\n'
        ' fd=os.open(directory,os.O_RDONLY|os.O_DIRECTORY,0)\n'
        ' try: os.fsync(fd)\n'
        ' finally: os.close(fd)\n'
        'print(json.dumps({"inode":seed.stat().st_ino,"bytes":len(data),"sha256":hashlib.sha256(data).hexdigest()}))',
        native_store(cluster),
        WORKSPACE,
        SEED_FILE,
        cluster.uid,
        cluster.gid,
        stdin=SEED_BODY.hex(),
        owner=False,
    )
    seed = json.loads(prepared["stdout"])
    cluster.record("rollout.ownerfs.adoption_input", seed)
    token = {"settings": settings, "seed": seed}
    try:
        cluster.python(
            "B",
            'with open(sys.argv[1],"a") as stream:stream.write(sys.stdin.read())',
            node_config(cluster),
            stdin=ADOPT_SETTING,
            owner=False,
        )
    except Exception:
        restore_settings(cluster, token)
        raise
    return token


def restore_settings(cluster, token):
    """Remove the new-version-only setting before starting an old B binary."""
    require_stopped_home(cluster)
    cluster.python(
        "B",
        'from pathlib import Path\nPath(sys.argv[1]).write_text(sys.stdin.read())',
        node_config(cluster),
        stdin=token["settings"],
        owner=False,
    )


def checksum(body):
    return {"bytes": len(body), "sha256": hashlib.sha256(body).hexdigest()}


def read_file(cluster, role, path, *, owner=True):
    data = cluster.python(
        role,
        'with open(sys.argv[1],"rb") as stream: data=stream.read()\n'
        'info=os.stat(sys.argv[1])\n'
        'print(json.dumps({"inode":info.st_ino,"mode":stat.S_IMODE(info.st_mode),'
        '"bytes":len(data),"sha256":hashlib.sha256(data).hexdigest()}))',
        path,
        owner=owner,
    )
    return json.loads(data["stdout"])


def observe_adoption(cluster):
    """Observe both the advertised root and the new Home's durable record."""
    location = json.loads(cluster.health("M", "/v1/roots/" + ROOT_ID))
    if location["root_id"] != ROOT_ID or location["status"] != "serving" or location["home_node_id"] != "node-b":
        raise AssertionError(f"adopted workspace is not serving from the upgraded Home: {location}")
    catalog_path = remote_join(native_store(cluster), ".ownerfs-roots", ROOT_ID + ".json")
    catalog = json.loads(
        cluster.python(
            "B", 'from pathlib import Path\nsys.stdout.write(Path(sys.argv[1]).read_text())',
            catalog_path, owner=False,
        )["stdout"],
    )
    if catalog["adopted"] is not True or catalog["data_dir"] != WORKSPACE:
        raise AssertionError(f"new Home did not persist the expected adoption state: {catalog}")
    if catalog["root_id"] != ROOT_ID or catalog["epoch"] != location["root_epoch"] or catalog["epoch"] <= 0:
        raise AssertionError(f"catalog identity differs from the advertised root: {catalog}, {location}")
    return {
        "root_id": ROOT_ID,
        "root_epoch": location["root_epoch"],
        "home_node_id": location["home_node_id"],
        "adopted": catalog["adopted"],
        "data_dir": catalog["data_dir"],
    }


def await_old_view(cluster, path):
    """Bound lookup propagation before allowing a single legacy mutation."""
    deadline = time.monotonic() + 30
    while time.monotonic() < deadline:
        result = cluster.python(
            "A",
            'try:\n with open(sys.argv[1],"rb") as stream: data=stream.read()\n'
            'except FileNotFoundError: print("null")\n'
            'else: print(json.dumps({"bytes":len(data),"sha256":hashlib.sha256(data).hexdigest()}))',
            path,
        )
        actual = json.loads(result["stdout"])
        if actual is not None:
            if actual != checksum(SEED_BODY):
                raise AssertionError(f"old Node returned the wrong adopted contents: {actual}")
            return actual
        time.sleep(0.2)
    raise AssertionError("old Node never exposed the newly adopted workspace")


def grpc_writes(cluster, role, side):
    metric = "afs_ownerfiles_payload_bytes_total{"
    expected = {f'side="{side}"', 'direction="write"', 'plane="grpc"'}
    values = []
    for line in cluster.health(role, "/metrics").splitlines():
        if line.startswith(metric):
            labels, _, sample = line[len(metric):].partition("}")
            if set(labels.split(",")) == expected:
                values.append(float(sample.strip()))
    if len(values) != 1 or not math.isfinite(values[0]) or values[0] < 0:
        raise AssertionError(f"expected one successful OwnerFiles gRPC write counter for {role}/{side}")
    return values[0]


def check_mixed_workspace(cluster, token):
    """Check the old A FUSE → new B Home path, with the new catalog untouched by the probe."""
    before = observe_adoption(cluster)
    native = remote_join(native_store(cluster), WORKSPACE)
    original = read_file(cluster, "B", remote_join(native, SEED_FILE), owner=False)
    if original != {"mode": 0o640, **token["seed"]}:
        raise AssertionError(f"adoption altered the staged native file: {original}")
    cluster.record("rollout.ownerfs.adopted_before_legacy_update", {"identity": before, "file": original})
    old_path = remote_join(cluster.mount("A"), WORKSPACE, SEED_FILE)
    old_view = await_old_view(cluster, old_path)
    cluster.record("rollout.ownerfs.legacy_read", old_view)
    client_before = grpc_writes(cluster, "A", "client")
    server_before = grpc_writes(cluster, "B", "server")
    cluster.python(
        "A",
        'os.rename(sys.argv[1],sys.argv[2]); data=bytes.fromhex(sys.stdin.read())\n'
        'with open(sys.argv[2],"r+b") as stream:\n'
        ' stream.seek(0,os.SEEK_END); stream.write(data); stream.flush(); os.fsync(stream.fileno())\n'
        'os.chmod(sys.argv[2],0o600)',
        old_path,
        remote_join(cluster.mount("A"), WORKSPACE, MUTATED_FILE),
        stdin=APPEND_BODY.hex(),
    )
    evidence = {
        "client": grpc_writes(cluster, "A", "client") - client_before,
        "server": grpc_writes(cluster, "B", "server") - server_before,
        "expected_bytes": len(APPEND_BODY),
    }
    cluster.record("rollout.ownerfs.legacy_grpc_write", evidence)
    if evidence["client"] < len(APPEND_BODY) or evidence["server"] < len(APPEND_BODY):
        raise AssertionError(f"old Node's write did not traverse the actual upgraded Home: {evidence}")
    home_mount = remote_join(cluster.mount("B"), WORKSPACE)
    cluster.python(
        "B",
        'try: os.stat(sys.argv[1])\n'
        'except FileNotFoundError: pass\n'
        'else: raise AssertionError("legacy rename left the old name visible")',
        remote_join(home_mount, SEED_FILE),
    )
    mounted = read_file(cluster, "B", remote_join(home_mount, MUTATED_FILE))
    physical = read_file(cluster, "B", remote_join(native, MUTATED_FILE), owner=False)
    after = observe_adoption(cluster)
    cluster.record("rollout.ownerfs.adopted_after_legacy_update", {
        "identity": after, "mounted": mounted, "physical": physical,
    })
    if after != before or physical["inode"] != original["inode"]:
        raise AssertionError("legacy FUSE update replaced the adopted root or its original file")
    expected = {"mode": 0o600, **checksum(SEED_BODY + APPEND_BODY)}
    for actual in (mounted, physical):
        if {key: actual[key] for key in expected} != expected:
            raise AssertionError(f"the upgraded Home did not retain the legacy update: {actual}")


def check_snapshot_restored(cluster, token):
    """The snapshot precedes adoption, so the old view must not contain the new workspace."""
    for role in ("A", "B"):
        cluster.python(
            role,
            'try: os.stat(sys.argv[1])\n'
            'except FileNotFoundError: pass\n'
            'else: raise AssertionError("post-snapshot adopted workspace survived rollback")',
            remote_join(cluster.mount(role), WORKSPACE),
        )
    cluster.python(
        "B",
        'from pathlib import Path\n'
        'assert Path(sys.argv[1]).read_text()==sys.stdin.read(), "old Node B config differs from the input"\n'
        'for path in sys.argv[2:]: assert not os.path.lexists(path),path',
        node_config(cluster),
        remote_join(native_store(cluster), WORKSPACE),
        remote_join(native_store(cluster), ".ownerfs-roots", ROOT_ID + ".json"),
        stdin=token["settings"],
        owner=False,
    )
    cluster.python(
        "M",
        'import urllib.error,urllib.request\n'
        'opener=urllib.request.build_opener(urllib.request.ProxyHandler({}))\n'
        'try:\n with opener.open(sys.argv[1],timeout=2): pass\n'
        'except urllib.error.HTTPError as error: assert error.code==404,error\n'
        'else: raise AssertionError("adopted root is still registered after snapshot restore")',
        f"http://{cluster.address('M')}:{cluster.ports('M')[1]}/v1/roots/{ROOT_ID}",
        owner=False,
    )
    cluster.record("rollout.ownerfs.adopted_workspace_absent_after_snapshot", True)
