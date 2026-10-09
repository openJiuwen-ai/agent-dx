#!/usr/bin/env python3
"""Named small 3FS fixture; fixed public legacy CLI, first-party wait ownership."""
import argparse
import hashlib
import importlib.util
import json
import os
from pathlib import Path
import platform
import resource
import signal
import subprocess
import sys
import time
import tomllib

FIXTURE = "threefs-delete-v84-20261007-r1"
CLUSTER = "afs_3fs_delete_v84_20261007_r1"
LEGACY_SHA = "16a25b5e1623e1030b631994a6799544a776828909512de4c191f06549b38f24"
MANIFEST_SHA = "1608f7ba7f3fa46e9aabfef0f9fe034db827e749e1c9a147a9f75d152f84a241"
PREFIX = Path("/opt/afs-3fs-round3-v84")
VOLUMES = {r: Path("/mnt/lima-" + v) for r, v in
           {"ctl": "afsctlstate", "a": "afsadata", "b": "afsbdata", "c": "afscdata"}.items()}
PORTS = {"fdb": 24100, "mgmtd": 24101, "meta": 24102, "storage": 24103}
SERVICES = {"fdb": "fdbserver", "mgmtd": "mgmtd_main", "meta": "meta_main",
            "storage": "storage_main", "fuse": "hf3fs_fuse_main"}
TARGETS = ((10000, 1000001001, 1), (10001, 1000101001, 1),
           (10000, 1000002001, 2), (10002, 1000202001, 2))


def require(ok, message):
    if not ok:
        raise RuntimeError(message)


def digest(path):
    result = hashlib.sha256()
    with Path(path).open("rb") as stream:
        for block in iter(lambda: stream.read(1024 * 1024), b""):
            result.update(block)
    return result.hexdigest()


def write_json(path, value):
    path = Path(path)
    temporary = path.with_name(path.name + ".publishing-" + str(os.getpid()) + "-" + str(time.monotonic_ns()))
    try:
        with temporary.open("x") as f:
            json.dump(value, f, indent=2)
            f.write("\n")
        # link is atomic and refuses an existing destination. Readers observe
        # either no destination or the complete, closed JSON inode.
        os.link(temporary, path)
    finally:
        if temporary.exists():
            temporary.unlink()


def safe(path, root):
    path, root = Path(path), Path(root)
    require(path.is_absolute() and ".." not in path.parts and path.is_relative_to(root), "path escapes owned root")
    require(not root.is_symlink(), "owned root symlink")
    for p in (path, *path.parents):
        require(not p.is_symlink(), "owned path symlink")
        if p == root:
            break
    return path


def proc_identity(pid):
    p = Path("/proc") / str(pid)
    fields = (p / "stat").read_text().rsplit(")", 1)[1].split()
    st = (p / "exe").stat()
    return {"pid": pid, "state": fields[0], "start_ticks": fields[19],
            "exe": os.readlink(p / "exe"), "exe_dev": st.st_dev, "exe_ino": st.st_ino,
            "exe_sha256": digest(p / "exe"),
            "argv": (p / "cmdline").read_bytes().split(bytes([0]))[:-1],
            "boot_id": Path("/proc/sys/kernel/random/boot_id").read_text().strip()}


def public_identity(value):
    return {**value, "argv": [v.decode() for v in value["argv"]]}


def validate_child(saved, observed, launch):
    require(observed["state"] != "Z", "exited child needs actual wait receipt")
    for key in ("pid", "start_ticks", "exe", "exe_dev", "exe_ino", "exe_sha256", "boot_id"):
        require(saved[key] == observed[key], "child identity differs: " + key)
    require([v.decode() for v in observed["argv"]] == launch["argv"], "foreign child argv")
    require(saved["config_sha256"] == launch["config_sha256"] and saved["script_sha256"] == launch["script_sha256"], "child launch binding differs")


class Fixture:
    def __init__(self, role, legacy_path):
        require(role in VOLUMES, "unknown role")
        self.role, self.legacy_path = role, Path(legacy_path)
        self.root = VOLUMES[role] / "afs-delivery" / FIXTURE
        self.admission = self.root / "inputs/admission.json"

    def linux(self):
        require(platform.system() == "Linux" and platform.machine() == "aarch64" and os.geteuid() == 0, "Linux ARM64 root required")
        require(platform.node() == "lima-afs-accept-" + self.role, "locked VM role differs")
        resource.setrlimit(resource.RLIMIT_NOFILE, (1048576, 1048576))
        require(hasattr(os, "pidfd_open") and hasattr(signal, "pidfd_send_signal"), "pidfd API missing")

    def capacity(self, admission=False):
        st = os.statvfs(self.root)
        floor = (512 if self.role == "ctl" else 1024) * 1024**2
        free = st.f_bavail * st.f_frsize
        allocated = 0
        for directory, dirs, files in os.walk(self.root, followlinks=False):
            if Path(directory) == self.root / "mount":
                dirs[:] = []
                files = []
            allocated += Path(directory).stat().st_blocks * 512
            allocated += sum((Path(directory) / n).lstat().st_blocks * 512 for n in files)
        require(allocated <= 1024**3 and free >= floor + (1024**3 if admission else 0), "named disk budget/floor failed")
        return {"allocated": allocated, "available": free, "budget": 1024**3, "floor": floor}

    def legacy(self):
        require(self.legacy_path.is_absolute() and not self.legacy_path.is_symlink() and digest(self.legacy_path) == LEGACY_SHA, "fixed legacy SHA/path differs")
        spec = importlib.util.spec_from_file_location("fixed_threefs_legacy", self.legacy_path)
        mod = importlib.util.module_from_spec(spec)
        spec.loader.exec_module(mod)
        mod.ROOT_NAME = "afs-delivery/" + FIXTURE
        mod.CLUSTER_ID, mod.FDB_KEY = CLUSTER, "afs3fsdelete20261007r1"
        mod.PORTS = dict(PORTS)
        mod.TEMPLATE = VOLUMES[self.role] / "3fs-round3-v84/config"
        original = mod.rewrite_template
        mod.rewrite_template = lambda text, role, name: original(self.rewrite(text, name), role, name)
        mod.guard_volume = lambda role: self.capacity(admission=True)
        mod.start_service = lambda role, service, argv, env=None: self.start_service(service, argv, env)
        return mod

    def rewrite(self, text, name):
        old = VOLUMES[self.role] / "3fs-round3-v84"
        text = text.replace(str(old), str(self.root)).replace("afs_3fs_round3_v84", CLUSTER)
        for old_port, new_port in zip((19000, 19001, 19002, 19003), (24100, 24101, 24102, 24103)):
            text = text.replace(":" + str(old_port), ":" + str(new_port)).replace("listen_port = " + str(old_port), "listen_port = " + str(new_port))
        if name == "meta_main.toml":
            text = text.replace("externalClientPath = '/lib/libfdb_c.so'", "externalClientPath = '" + str(PREFIX / "lib/libfdb_c.so") + "'")
        require(str(old) not in text and "afs_3fs_round3_v84" not in text, "stale fixture configuration")
        return text

    def config_hashes(self):
        result = {p.name: digest(safe(p, self.root)) for p in sorted((self.root / "config").iterdir()) if p.is_file()}
        cluster = self.root / "data/foundationdb/fdb.cluster"
        if cluster.exists():
            result["fdb.cluster"] = digest(safe(cluster, self.root))
        return result

    def validate_config(self):
        cfg = {p.name: tomllib.loads(p.read_text()) for p in (self.root / "config").glob("*.toml")}
        require(cfg["meta_main.toml"]["server"]["fdb"]["externalClientPath"] == str(PREFIX / "lib/libfdb_c.so"), "FDB dlopen path not fixed owned library")
        for name, port in (("mgmtd_main.toml", 24101), ("meta_main.toml", 24102), ("storage_main.toml", 24103)):
            require(cfg[name]["server"]["base"]["groups"][0]["listener"]["listen_port"] == port, "listener differs")
        for name in ("hf3fs_fuse_main.toml", "hf3fs_fuse_main_launcher.toml"):
            c = cfg[name]
            mgmtd = c["mgmtd"] if name == "hf3fs_fuse_main.toml" else c["mgmtd_client"]
            require(mgmtd["mgmtd_server_addresses"] == ["RDMA://192.168.109.11:24101"], "mgmtd address differs")
        fuse = cfg["hf3fs_fuse_main.toml"]
        require(fuse["fdatasync_update_length"] is True and fuse["fsync_length_hint"] is False, "FUSE sync settings differ")
        require(cfg["hf3fs_fuse_main_launcher.toml"]["cluster_id"] == CLUSTER, "cluster differs")
        require(cfg["storage_main.toml"]["server"]["targets"]["target_paths"] == [str(self.root / "data/storage/data1")], "foreign storage path")
        for name in ("admin_cli.toml", "mgmtd_main_launcher.toml", "meta_main_launcher.toml", "storage_main_launcher.toml"):
            require(cfg[name]["cluster_id"] == CLUSTER, "foreign cluster identity")
        for name, node in (("mgmtd_main_app.toml", 1), ("meta_main_app.toml", 50), ("storage_main_app.toml", {"ctl": 10000, "a": 10000, "b": 10001, "c": 10002}[self.role])):
            require(cfg[name] == {"allow_empty_node_id": False, "node_id": node}, "foreign role node identity")
        for name, client in (("admin_cli.toml", "mgmtd_client"), ("meta_main_launcher.toml", "mgmtd_client"), ("storage_main_launcher.toml", "mgmtd_client")):
            require(cfg[name][client]["mgmtd_server_addresses"] == ["RDMA://192.168.109.11:24101"], "foreign management endpoint")
        require(cfg["storage_main.toml"]["server"]["mgmtd"]["mgmtd_server_addresses"] == ["RDMA://192.168.109.11:24101"], "foreign storage management endpoint")
        for name, section in (("admin_cli.toml", "fdb"), ("mgmtd_main_launcher.toml", "kv_engine"), ("meta_main.toml", "server")):
            client = cfg[name][section]
            if section != "fdb":
                client = client["fdb"]
            require(client["clusterFile"] == str(self.root / "data/foundationdb/fdb.cluster"), "foreign FDB cluster path")
        require((self.root / "data/foundationdb/fdb.cluster").read_text() == "round3:afs3fsdelete20261007r1@192.168.109.11:24100\n", "FDB cluster differs")
        return self.config_hashes()

    def prepare(self):
        require(not (self.root / "run/config-prepared.json").exists(), "exclusive prepare")
        self.capacity(admission=True)
        original_index = json.loads(safe(self.root / "inputs/config-index.json", self.root).read_text())
        old = VOLUMES[self.role] / "3fs-round3-v84/config"
        for name, sha in original_index.items():
            require(Path(name).name == name and digest(old / name) == sha, "retained config changed")
        mod = self.legacy()
        mod.ensure_dirs(self.root)
        value = mod.prepare(self.role)
        config = self.validate_config()
        write_json(self.root / "run/config-prepared.json", {"role": self.role, "fixture": FIXTURE, "config_sha256": config, "legacy_sha256": LEGACY_SHA})
        return {"status": "PASS_PREPARED_CONFIG_ONLY", "config_sha256": config, "legacy": value}

    def prepared(self):
        saved = json.loads(safe(self.root / "run/config-prepared.json", self.root).read_text())
        require(saved["role"] == self.role and saved["fixture"] == FIXTURE and saved["legacy_sha256"] == LEGACY_SHA, "prepared identity differs")
        # Chain CSVs are bootstrap outputs, not immutable executable configs.
        current = {k: v for k, v in self.config_hashes().items() if not k.endswith(".csv")}
        require(saved["config_sha256"] == current, "prepared configuration changed")
        return saved

    def preflight(self):
        self.prepared()
        self.capacity(admission=True)
        require(digest(PREFIX / "manifest.json") == MANIFEST_SHA, "fixed manifest differs")
        return {"status": "PASS_FIXED_CONFIG_READY", "manifest": self.legacy().verify_manifest(), "config": self.validate_config(), "capacity": self.capacity(admission=True)}

    def service_dir(self, service):
        require(service in ("fdb", "mgmtd", "meta") if self.role == "ctl" else service in ("storage", "fuse"), "foreign role service")
        return safe(self.root / "run" / service, self.root)

    def expected_argv(self, service):
        binary, cfg = str(PREFIX / "bin" / SERVICES[service]), self.root / "config"
        if service == "fdb":
            return [binary, "-p", "192.168.109.11:24100", "-m", "1GiB", "--cache-memory", "128MiB", "--storage-memory", "128MiB", "--data-filesystem", str(VOLUMES["ctl"]), "-d", str(self.root / "data/foundationdb"), "-L", str(self.root / "log"), "-C", str(self.root / "data/foundationdb/fdb.cluster")]
        if service == "fuse":
            return [binary, "--launcher_cfg", str(cfg / "hf3fs_fuse_main_launcher.toml"), "--cfg", str(cfg / "hf3fs_fuse_main.toml"), "--launcher_config.mountpoint=" + str(self.root / "mount")]
        name = SERVICES[service]
        return [binary, "--app_cfg", str(cfg / (name + "_app.toml")), "--launcher_cfg", str(cfg / (name + "_launcher.toml")), "--cfg", str(cfg / (name + ".toml"))]

    def launch(self, service, argv, env):
        self.service_dir(service)
        require(argv == self.expected_argv(service), "foreign executable or config argv")
        expected_env = {"LD_PRELOAD": str(PREFIX / "lib/libjemalloc.so.2")} if service in ("fuse", "mgmtd", "meta") else {}
        require((env or {}) == expected_env, "foreign service environment")
        return {"role": self.role, "fixture": FIXTURE, "root": str(self.root), "service": service,
                "argv": argv, "env": env or {}, "config_sha256": self.prepared()["config_sha256"],
                "script_sha256": digest(Path(__file__)), "legacy_sha256": LEGACY_SHA,
                "boot_id": Path("/proc/sys/kernel/random/boot_id").read_text().strip()}

    def start_service(self, service, argv, env=None):
        lifecycle = self.service_dir(service)
        require(not lifecycle.exists(), "exclusive service launch; no retry")
        launch = self.launch(service, argv, env)
        lifecycle.mkdir()
        write_json(lifecycle / "launch.json", launch)
        supervisor_argv = [sys.executable, str(Path(__file__).resolve()), "__supervise", self.role,
                           "--legacy-path", str(self.legacy_path), "--service", service]
        with (lifecycle / "supervisor.log").open("xb") as log:
            sup = subprocess.Popen(supervisor_argv, stdin=subprocess.DEVNULL, stdout=log, stderr=log, start_new_session=True)
        write_json(lifecycle / "supervisor.json", {"pid": sup.pid, "argv": supervisor_argv})
        deadline = time.monotonic() + 15
        while not (lifecycle / "identity.json").exists():
            require(not (lifecycle / "exit.json").exists() and sup.poll() is None and time.monotonic() < deadline, "supervisor/child startup failed")
            time.sleep(0.05)
        identity = json.loads((lifecycle / "identity.json").read_text())
        validate_child(identity, proc_identity(identity["pid"]), launch)
        return {"service": service, "identity": identity, "lifecycle": str(lifecycle)}

    def supervise(self, service):
        lifecycle = self.service_dir(service)
        launch = json.loads((lifecycle / "launch.json").read_text())
        require(launch == self.launch(service, launch["argv"], launch["env"]), "supervisor launch modified")
        env = os.environ.copy()
        env["LD_LIBRARY_PATH"] = str(PREFIX / "lib")
        env.update(launch["env"])
        with (lifecycle / "stdout").open("xb") as out, (lifecycle / "stderr").open("xb") as err:
            child = subprocess.Popen(launch["argv"], stdin=subprocess.DEVNULL, stdout=out, stderr=err, env=env)
            identity = None
            try:
                deadline = time.monotonic() + 10
                while child.poll() is None:
                    observed = proc_identity(child.pid)
                    if observed["exe"] == launch["argv"][0]:
                        identity = {**launch, **public_identity(observed), "supervisor_pid": os.getpid()}
                        write_json(lifecycle / "identity.json", identity)
                        break
                    require(time.monotonic() < deadline, "child exec identity timeout")
                    time.sleep(0.01)
            finally:
                code = child.wait()
                write_json(lifecycle / "exit.json", {"exit_code": code, "pid": child.pid, "supervisor_pid": os.getpid(), "identity": identity, "launch": launch})

    def mount(self):
        require(self.role != "ctl", "ctl does not mount")
        self.prepared()
        self.capacity()
        value = self.start_service("fuse", [str(PREFIX / "bin/hf3fs_fuse_main"), "--launcher_cfg", str(self.root / "config/hf3fs_fuse_main_launcher.toml"), "--cfg", str(self.root / "config/hf3fs_fuse_main.toml"), "--launcher_config.mountpoint=" + str(self.root / "mount")], {"LD_PRELOAD": str(PREFIX / "lib/libjemalloc.so.2")})
        deadline = time.monotonic() + 45
        while not (self.root / "mount/test").is_dir():
            require(time.monotonic() < deadline, "fresh mount/test readiness timeout")
            time.sleep(0.2)
        mount = self.exact_mount()
        write_json(self.service_dir("fuse") / "mount.json", mount)
        return {**value, "mount": mount}

    def exact_mount(self):
        q = subprocess.run(["findmnt", "-J", "--mountpoint", str(self.root / "mount"), "-o", "TARGET,SOURCE,FSTYPE,ID,OPTIONS"], capture_output=True, text=True)
        if q.returncode == 1:
            return None
        require(q.returncode == 0, "findmnt failed")
        rows = json.loads(q.stdout)["filesystems"]
        require(len(rows) == 1 and rows[0]["target"] == str(self.root / "mount") and rows[0]["source"] == "hf3fs." + CLUSTER and rows[0]["fstype"] == "fuse.hf3fs", "foreign mount")
        return rows[0]

    def validate_mount(self, saved, current):
        require(current is not None and saved == current, "mount incarnation differs")

    def stop_service(self, service):
        life = self.service_dir(service)
        if not life.exists():
            return {"service": service, "not_started": True}
        launch = json.loads((life / "launch.json").read_text())
        require(launch == self.launch(service, launch["argv"], launch["env"]), "stop launch changed")
        receipt = life / "exit.json"
        if not receipt.exists():
            identity = json.loads((life / "identity.json").read_text())
            fd = os.pidfd_open(identity["pid"])
            try:
                validate_child(identity, proc_identity(identity["pid"]), launch)
                if service == "fuse":
                    saved = json.loads((life / "mount.json").read_text())
                    self.validate_mount(saved, self.exact_mount())
                    old = json.loads(self.admission.read_text())["fusermount3"]
                    require(digest(old["path"]) == old["sha256"], "unmount tool changed")
                    q = subprocess.run([old["path"], "-u", str(self.root / "mount")], capture_output=True, text=True)
                    write_json(life / "unmount.json", {"argv": q.args, "exit": q.returncode, "stdout": q.stdout, "stderr": q.stderr})
                    require(q.returncode == 0, "normal unmount failed; no fallback")
                    if not receipt.exists():
                        try:
                            validate_child(identity, proc_identity(identity["pid"]), launch)
                            signal.pidfd_send_signal(fd, signal.SIGTERM)
                        except (FileNotFoundError, ProcessLookupError):
                            pass  # Only the actual wait receipt below proves closure.
                else:
                    signal.pidfd_send_signal(fd, signal.SIGTERM)
            finally:
                os.close(fd)
        deadline = time.monotonic() + 30
        while not receipt.exists():
            require(time.monotonic() < deadline, "actual wait receipt timeout; no KILL")
            time.sleep(0.05)
        value = json.loads(receipt.read_text())
        require(value["launch"] == launch and value["exit_code"] == 0 and value["identity"] is not None, "real child wait not normal0")
        saved = json.loads((life / "identity.json").read_text())
        require(value["identity"] == saved and value.get("pid") == saved["pid"] and value.get("supervisor_pid") == saved["supervisor_pid"], "actual wait identity differs")
        if service == "fuse":
            require(self.exact_mount() is None, "owned mount remains")
        return value

    def chains(self):
        require(self.role == "ctl", "chains ctl only")
        mod = self.legacy()
        records = []
        for node, target, chain in TARGETS:
            records.append(mod.admin(self.root, "create-r2-target-" + str(target), ["create-target", "--node-id", str(node), "--disk-index", "0", "--target-id", str(target), "--chain-id", str(chain)]))
        csv, table = self.root / "config/chains.csv", self.root / "config/chain-table.csv"
        with csv.open("x") as f:
            f.write("ChainId,TargetId,TargetId\n1,1000001001,1000101001\n2,1000002001,1000202001\n")
        with table.open("x") as f:
            f.write("ChainId\n1\n2\n")
        for label, argv in (("upload-r2-chains", ["upload-chains", str(csv)]), ("upload-r2-table", ["upload-chain-table", "1", str(table), "--desc", "delete-small-R2-localA-AB-AC"]), ("r2-list-nodes", ["list-nodes"]), ("r2-list-targets", ["list-targets"]), ("r2-list-chains", ["list-chains"]), ("r2-list-tables", ["list-chain-tables"]), ("mkdir-test", ["mkdir", "--perm", "0755", "test"]), ("set-test-perm", ["set-perm", "--uid", "0", "--gid", "0", "test"])):
            records.append(mod.admin(self.root, label, argv))
        return {"status": "R2_PUBLIC_COMMANDS_RECORDED_NEEDS_ACTUAL_SERVING_PROOF", "records": records, "targets": TARGETS}

    def postcheck(self):
        values = {s: self.stop_service(s) for s in (("meta", "mgmtd", "fdb") if self.role == "ctl" else ("fuse", "storage"))}
        old = json.loads(self.admission.read_text())
        unchanged = []
        for saved in old["protected_processes"]:
            live = proc_identity(saved["pid"])
            for key in ("start_ticks", "exe", "exe_dev", "exe_ino"):
                require(live[key] == saved[key], "protected process changed")
            require(live["boot_id"] == old["boot_id"], "protected boot changed")
            unchanged.append(saved["pid"])
        require(self.exact_mount() is None, "mount remains")
        current = subprocess.check_output(["findmnt", "-J", "-o", "TARGET,SOURCE,FSTYPE,ID,OPTIONS"], text=True)
        def rows(entries):
            for row in entries:
                yield {k: v for k, v in row.items() if k != "children"}
                yield from rows(row.get("children", []))
        old_mounts = list(rows(json.loads(old["mount_inventory"]["stdout"])["filesystems"]))
        now_mounts = {r["target"]: r for r in rows(json.loads(current)["filesystems"])}
        for row in old_mounts:
            require(now_mounts.get(row["target"]) == row, "protected mount changed")
        require(not any(Path(r["target"]).is_relative_to(self.root) for r in now_mounts.values()), "owned mount remains")
        deadline = time.monotonic() + 5
        while True:
            alive = []
            for p in Path("/proc").iterdir():
                if not p.name.isdigit() or int(p.name) == os.getpid():
                    continue
                try:
                    argv = (p / "cmdline").read_bytes().split(bytes([0]))
                    if (b"__supervise" in argv and str(self.legacy_path).encode() in argv) or any(v == str(self.root / "tools/threefs_namespace_fixture.py").encode() for v in argv):
                        # Exclude this postcheck and its sudo observer by exact command.
                        if b"postcheck" not in argv:
                            alive.append(int(p.name))
                except FileNotFoundError:
                    continue
            if not alive:
                break
            require(time.monotonic() < deadline, "owned supervisor remains")
            time.sleep(0.05)
        return {"status": "PASS_ACTUAL_NORMAL_WAIT_CLOSURE", "receipts": values, "protected_pids_unchanged": unchanged, "protected_mount_count": len(old_mounts), "owned_supervisors_alive": [], "capacity": self.capacity()}


def main():
    p = argparse.ArgumentParser()
    p.add_argument("action", choices=("prepare", "preflight", "start", "chains", "mount", "stop", "postcheck", "__supervise"))
    p.add_argument("role", choices=tuple(VOLUMES))
    p.add_argument("--legacy-path", required=True)
    p.add_argument("--service", choices=tuple(SERVICES))
    args = p.parse_args()
    fixture = Fixture(args.role, args.legacy_path)
    try:
        fixture.linux()
        if args.action == "__supervise":
            require(args.service is not None, "supervisor service required")
            fixture.supervise(args.service)
            return 0
        if args.action == "start":
            fixture.preflight()
            mod = fixture.legacy()
            value = mod.start_control() if args.role == "ctl" else mod.start_storage(args.role)
        elif args.action == "stop":
            value = {s: fixture.stop_service(s) for s in (("meta", "mgmtd", "fdb") if args.role == "ctl" else ("fuse", "storage"))}
        else:
            value = getattr(fixture, args.action)()
        print(json.dumps(value, indent=2))
        return 0
    except Exception as error:
        print(json.dumps({"status": "FAIL_OR_BLOCKED_STOP_WITHOUT_REPAIR", "role": args.role, "action": args.action, "error": repr(error)}))
        return 1


if __name__ == "__main__":
    raise SystemExit(main())
