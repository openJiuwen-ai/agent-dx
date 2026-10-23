"""Real AFS mounts for FC: distinct clients for each executor and the reclaimer."""

import importlib.util
import json
import re
import shutil
import socket
import subprocess
import urllib.request
from pathlib import Path

from host_tools import executable


def storage_config(root, role, run_name):
    import os

    if os.environ.get("ADX_STORAGE_NATIVE_XFS_ROOT"):
        from native_xfs import storage_config as native_config

        return native_config(os.environ["ADX_STORAGE_NATIVE_XFS_ROOT"], role, run_name)
    if os.environ.get("ADX_FC_JUICEFS_CONFIG"):
        from juicefs_fixture import load_config, storage_config as juicefs_storage_config

        return juicefs_storage_config(
            load_config(os.environ["ADX_FC_JUICEFS_CONFIG"]), role, run_name
        )
    if not re.fullmatch(r"[A-Za-z0-9_-]+", run_name):
        raise ValueError("a single run name is required")
    node = {"node1": "node-a", "node2": "node-b", "coordinator": "node-c"}[role]
    return {
        "kind": "distributed_fs",
        "alias": "afs",
        "mount_root": str(Path(root) / node / "mount/dfs"),
        "prefix": "adx/" + run_name,
    }


class FilesystemFixture:
    def __init__(self, repo, binary_dir, root, evidence, trace_cow=False):
        script = Path(repo) / "build/e2e/afs/scripts/dfs/three_node_core.py"
        spec = importlib.util.spec_from_file_location("fc_dfs_fixture", script)
        module = importlib.util.module_from_spec(spec)
        spec.loader.exec_module(module)
        owner = module.load_owner_helpers(Path(repo))
        # Fresh ports avoid mistaking a previous completed run's TIME_WAIT
        # sockets for a live service. The existing preflight still checks all binds.
        sockets = []
        try:
            for key in ("META_GRPC", "META_REST", "NODE_GRPC", "NODE_REST"):
                sock = socket.socket()
                sock.bind(("127.0.0.1", 0))
                sockets.append(sock)
                setattr(owner, key, sock.getsockname()[1])
        finally:
            for sock in sockets:
                sock.close()
        self.meta_port = owner.META_REST
        self.metrics_port = owner.NODE_REST
        self.trace_cow = trace_cow
        self.core = module.DfsCoreRun(owner, Path(repo), Path(binary_dir), Path(root))
        self.evidence = Path(evidence)
        self.evidence.mkdir(parents=True, exist_ok=True)

    def start(self):
        self.core.prepare()
        import os

        if os.environ.get("ADX_STORAGE_GC") == "1":
            config = self.core.run.root / "etc/meta.toml"
            config.write_text(
                config.read_text()
                + "\nmeta_request_retention_ms = 30000\nmeta_watch_events_max = 16\n"
            )
        if self.trace_cow:
            # Only the traced diagnostic needs the COW operation witness.
            # Ordinary timing runs retain the fixture's WARN logging level.
            config = self.core.run.root / "etc/node-b.toml"
            text = config.read_text()
            if 'log_level = "warn"' not in text:
                raise ValueError("unexpected AFS diagnostic log configuration")
            config.write_text(text.replace('log_level = "warn"', 'log_level = "info"'))
        for name in ("meta", "node-a", "node-b", "node-c"):
            self.core.run.start(name)
        mounts = {}
        for role in ("node1", "node2", "coordinator"):
            root = Path(storage_config(self.core.run.root, role, "preflight")["mount_root"])
            value = json.loads(
                self.core.run.command(
                    ["findmnt", "-J", "--mountpoint", root, "-o", "TARGET,SOURCE,FSTYPE"]
                )
            )["filesystems"][0]
            self.core.run.check(
                "real AFS mount " + role,
                value["source"] == "afs-dfs" and value["fstype"].startswith("fuse"),
                value,
            )
            mounts[role] = value
        (self.evidence / "afs-mounts.json").write_text(json.dumps(mounts, indent=2))

    def verify_gc(self):
        """COW/open-unlink stay readable through collection and an AFS client crash."""
        import hashlib
        import os
        import time
        from storage_gc import GcProbe, reclaimed, close_fenced_descriptor

        probe = GcProbe(self.core.run.root, self.meta_port, self.evidence)
        mounts = {
            role: Path(storage_config(self.core.run.root, role, "unused")["mount_root"])
            for role in ("node1", "node2", "coordinator")
        }
        # Getattr synthesizes the root; a namespace mutation persists it.
        bootstrap = mounts["node1"] / ".gc-namespace-init"
        bootstrap.mkdir()
        bootstrap.rmdir()
        baseline = probe.settle(
            "file-baseline",
            lambda row: row["meta"]["entities"].get("dfs_inode", 0) == 1
            and row["meta"]["entities"].get("dfs_dentry", 0) == 0
            and row["meta"]["entities"].get("dfs_reference", 0) == 0,
        )
        payload = os.urandom(2 * 1024**2)
        expected = hashlib.sha256(payload).hexdigest()
        source, clone = "gc-source", "gc-clone"
        with (mounts["node1"] / source).open("wb") as file:
            file.write(payload)
            file.flush()
            os.fsync(file.fileno())
        source_fd = os.open(mounts["node2"] / source, os.O_RDONLY)
        clone_fd = None
        try:
            destination = os.open(
                mounts["node2"] / clone, os.O_CREAT | os.O_EXCL | os.O_WRONLY, 0o600
            )
            try:
                copied = os.copy_file_range(source_fd, destination, len(payload))
                if copied != len(payload):
                    raise AssertionError("COW clone was short")
                os.fsync(destination)
            finally:
                os.close(destination)
            clone_fd = os.open(mounts["coordinator"] / clone, os.O_RDONLY)
            os.unlink(mounts["node1"] / source)
            os.unlink(mounts["node1"] / clone)
            before = probe.capture("unlinked-open")
            time.sleep(75)
            for descriptor in (source_fd, clone_fd):
                if hashlib.sha256(os.pread(descriptor, len(payload), 0)).hexdigest() != expected:
                    raise AssertionError("GC corrupted a COW/open-unlink reader")
            protected = probe.capture("protected-through-grace")
            if protected["meta"].get("gc", {}).get("generation", 0) <= baseline["meta"].get(
                "gc", {}
            ).get("generation", 0):
                raise AssertionError("no real collection ran during protected reads")
            old = self.core.run.processes["node-b"]
            old.kill()
            old.wait(10)
            subprocess.run(
                [executable("fusermount3"), "-uz", "--", str(mounts["node2"])],
                check=True,
                timeout=15,
            )
            self.core.run.start("node-b")
            descriptor, source_fd = source_fd, None
            close_fenced_descriptor(descriptor)
            # Old session deadline (up to30s) plus native reference/read-grant grace.
            time.sleep(100)
            if hashlib.sha256(os.pread(clone_fd, len(payload), 0)).hexdigest() != expected:
                raise AssertionError("COW reader did not survive source AFS session replacement")
            recovered = probe.capture("afs-session-replaced")
            os.close(clone_fd)
            clone_fd = None
            final = probe.settle("all-readers-closed", lambda row: reclaimed(baseline, row))
            receipt = {
                "status": "passed",
                "payload_sha256": expected,
                "copied_bytes": copied,
                "unlinked_open": before,
                "protected": protected,
                "recovered": recovered,
                "final": final,
                "afs_client_restart": True,
                "old_session_fenced": True,
            }
            (self.evidence / "gc-file-lifecycle.json").write_text(json.dumps(receipt, indent=2))
        finally:
            if source_fd is not None:
                close_fenced_descriptor(source_fd)
            if clone_fd is not None:
                os.close(clone_fd)

    def metrics(self, phase):
        values = {}
        for name, address in [
            ("node-a", "127.0.0.2"),
            ("node-b", "127.0.0.3"),
            ("node-c", "127.0.0.4"),
        ]:
            opener = urllib.request.build_opener(urllib.request.ProxyHandler({}))
            with opener.open(f"http://{address}:{self.metrics_port}/metrics", timeout=5) as reply:
                values[name] = reply.read().decode()
            (self.evidence / f"afs-{phase}-{name}.prom").write_text(values[name])
        return values

    def stop(self):
        errors = []
        # Flush and release every FUSE client while all inode owners and Meta
        # are still reachable. Sequentially stopping RPC servers first strands
        # the remaining clients' deferred close operations.
        for role in ("node2", "node1", "coordinator"):
            mount = storage_config(self.core.run.root, role, "cleanup")["mount_root"]
            if not any(
                line.split(" - ", 1)[0].split()[4] == mount
                for line in Path("/proc/self/mountinfo").read_text().splitlines()
            ):
                continue
            try:
                reply = subprocess.run(
                    [executable("fusermount3"), "-u", "--", mount],
                    capture_output=True,
                    text=True,
                    timeout=30,
                )
                if reply.returncode:
                    errors.append(f"normal unmount {role}: {reply.stderr}")
            except Exception as error:
                errors.append(f"normal unmount {role}: {error!r}")
        for name in ("node-c", "node-b", "node-a", "meta"):
            if name not in self.core.run.processes:
                continue
            try:
                self.core.run.stop(name)
            except Exception as error:
                errors.append(f"{name}: {error!r}")
        for log in self.core.run.logs:
            log.close()
        for path in self.core.run.root.glob("*.log"):
            shutil.copyfile(path, self.evidence / ("afs-" + path.name))
        remaining = [
            line
            for line in Path("/proc/self/mountinfo").read_text().splitlines()
            if str(self.core.run.root) + "/" in line
        ]
        if remaining:
            errors.append("AFS mounts still active: " + repr(remaining))
        (self.evidence / "afs-cleanup.json").write_text(json.dumps({"errors": errors}, indent=2))
        return errors
