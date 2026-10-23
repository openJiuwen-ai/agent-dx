"""Independent JuiceFS clients against deployment-owned PostgreSQL and Ceph RGW."""

import hashlib
import json
import os
from pathlib import Path
import socket
import subprocess
import time
import urllib.request

from host_tools import executable

ROLES = ("node1", "node2", "coordinator")


def load_config(path):
    path = Path(path)
    if path.is_symlink() or not path.is_file() or path.stat().st_mode & 0o077:
        raise ValueError("JuiceFS configuration must be a private regular file")
    value = json.loads(path.read_text())
    if not value["metadata_url"].startswith("postgres://") or "@" not in value["metadata_url"]:
        raise ValueError("PostgreSQL metadata URL required")
    from urllib.parse import urlsplit

    if urlsplit(value["metadata_url"]).password:
        raise ValueError("use metadata_password_file instead of credentials in the URL")
    for key in ("binary", "metadata_password_file", "mount_root", "identity_file"):
        if not Path(value[key]).is_absolute():
            raise ValueError(f"{key} must be absolute")
    if not value["alias"] or value["alias"] == "local":
        raise ValueError("shared storage alias required")
    mount_options(value)
    return value


def mount_options(config):
    value = config.get("max_readahead_mib", 32)
    if type(value) is not int or not 0 <= value <= 32:
        raise ValueError("max_readahead_mib must be an integer from 0 to 32")
    return (
        ["--max-readahead", str(value)] if "max_readahead_mib" in config else [],
        "max_readahead_mib" not in config,
        value,
    )


def storage_config(config, role, run_name):
    if role not in ROLES:
        raise ValueError("unknown filesystem client role")
    import re

    if not re.fullmatch(r"[A-Za-z0-9_-]+", run_name):
        raise ValueError("a single run name is required")
    return {
        "kind": "distributed_fs",
        "alias": config["alias"],
        "mount_root": str(Path(config["mount_root"]) / role),
        "prefix": "adx/" + run_name,
    }


class JuicefsFixture:
    def __init__(self, config_path, evidence):
        self.config = load_config(config_path)
        self.evidence = Path(evidence)
        self.evidence.mkdir(parents=True, exist_ok=True)
        self.processes = {}
        self.logs = {}
        self.ports = {}
        self.access_readers = []
        self.password = Path(self.config["metadata_password_file"]).read_text().strip()

    def start(self):
        binary = Path(self.config["binary"])
        identity = json.loads(Path(self.config["identity_file"]).read_text())
        if identity.get("postgres_synchronous_commit") != "on":
            raise ValueError("durable PostgreSQL acknowledgements required")
        mounts = {}
        extra, defaults, readahead = mount_options(self.config)
        try:
            for role in ROLES:
                mount = Path(self.config["mount_root"]) / role
                mount.mkdir(parents=True, exist_ok=True)
                if os.path.ismount(mount) or any(mount.iterdir()):
                    raise ValueError("JuiceFS fixture requires an unused empty mountpoint")
                with socket.socket() as listener:
                    listener.bind(("127.0.0.1", 0))
                    port = listener.getsockname()[1]
                self.ports[role] = port
                log = self.evidence / (role + ".private.log")
                env = {**os.environ, "META_PASSWORD": self.password}
                args = [
                    str(binary),
                    "mount",
                    self.config["metadata_url"],
                    str(mount),
                    "--cache-size",
                    "0",
                    "--cache-dir",
                    "memory",
                    "--no-usage-report",
                    "--metrics",
                    f"127.0.0.1:{port}",
                    *extra,
                ]
                with log.open("w") as handle:
                    self.logs[role] = log
                    self.processes[role] = subprocess.Popen(
                        args, env=env, stdout=handle, stderr=handle
                    )
                deadline = time.monotonic() + 60
                while not os.path.ismount(mount):
                    if self.processes[role].poll() is not None or time.monotonic() >= deadline:
                        raise RuntimeError("JuiceFS client did not mount: " + role)
                    time.sleep(0.1)
                value = json.loads(
                    subprocess.check_output(
                        [
                            executable("findmnt"),
                            "-J",
                            "--mountpoint",
                            str(mount),
                            "-o",
                            "TARGET,SOURCE,FSTYPE",
                        ],
                        text=True,
                    )
                )["filesystems"][0]
                if (
                    value["fstype"] != "fuse.juicefs"
                    or value["source"] != "JuiceFS:" + self.config["volume"]
                ):
                    raise ValueError("unexpected JuiceFS mount identity")
                mounts[role] = value
                if os.getenv("ADX_STORAGE_FILESYSTEM_TRACE") == "1":
                    with (self.evidence / (role + "-access.log")).open("w") as output:
                        self.access_readers.append(
                            subprocess.Popen(
                                [executable("cat"), str(mount / ".accesslog")],
                                stdout=output,
                                stderr=subprocess.DEVNULL,
                            )
                        )
            identity.update(
                binary_sha256=hashlib.sha256(binary.read_bytes()).hexdigest(),
                mounts=mounts,
                cache_size_mib=0,
                cache_dir="memory",
                writeback=False,
                io_defaults_preserved=defaults,
                max_readahead_mib=readahead,
            )
            (self.evidence / "juicefs-identities.json").write_text(json.dumps(identity, indent=2))
        except BaseException:
            self.stop()
            raise

    def verify_filesystem(self):
        """Witness metadata cloning and linked-clone retention with real object counters."""
        import uuid
        from filesystem_metrics import juicefs_read_bytes

        mounts = {role: Path(self.config["mount_root"]) / role for role in ROLES}
        name = ".adx-cow-probe-" + uuid.uuid4().hex
        for role in ROLES:
            (mounts[role] / name).mkdir(exist_ok=True)
        source = mounts["node1"] / name / "source"
        payload = os.urandom(16 * 1024**2)
        with source.open("wb") as file:
            file.write(payload)
            file.flush()
            os.fsync(file.fileno())
        before = self.metrics("cow-before")
        destination = mounts["node2"] / name / "clone"
        started = time.monotonic()
        with (
            (mounts["node2"] / name / "source").open("rb") as reader,
            destination.open("xb") as writer,
        ):
            copied = 0
            while copied < len(payload):
                count = os.copy_file_range(reader.fileno(), writer.fileno(), len(payload) - copied)
                if count <= 0:
                    raise AssertionError("filesystem clone was short")
                copied += count
            os.fsync(writer.fileno())
        elapsed = time.monotonic() - started
        after = self.metrics("cow-after")
        read_bytes = juicefs_read_bytes(after["node2"]) - juicefs_read_bytes(before["node2"])
        if read_bytes != 0:
            raise AssertionError("filesystem clone downloaded source object data")
        # Independent clone writes must leave the published source unchanged.
        with destination.open("r+b") as file:
            file.write(b"COW")
            file.flush()
            os.fsync(file.fileno())
        expected = b"COW" + payload[3:]
        if source.read_bytes() != payload:
            raise AssertionError("clone modified its source")
        # Coordinator must retain the clone's directory entry while a consumer runs.
        # JuiceFS promises open-unlink only within the same mount, so an FD opened
        # through a different client is not a replacement for that retention rule.
        source.unlink()
        with (mounts["coordinator"] / name / "clone").open("rb") as reader:
            data = reader.read()
        if data != expected:
            raise AssertionError("deleting the COW source corrupted a retained clone")
        destination.unlink()
        (mounts["node1"] / name).rmdir()
        proof = {
            "status": "passed",
            "source_bytes": len(payload),
            "copy_file_range_bytes": copied,
            "clone_seconds": elapsed,
            "clone_object_read_bytes": read_bytes,
            "source_sha256": hashlib.sha256(payload).hexdigest(),
            "clone_sha256": hashlib.sha256(expected).hexdigest(),
            "clone_write_isolated": True,
            "linked_clone_survived_source_delete": True,
            "running_reference_requires_linked_file": True,
        }
        (self.evidence / "filesystem-proof.json").write_text(json.dumps(proof, indent=2))

    def metrics(self, phase):
        result = {}
        opener = urllib.request.build_opener(urllib.request.ProxyHandler({}))
        for role, port in self.ports.items():
            with opener.open(f"http://127.0.0.1:{port}/metrics", timeout=10) as reply:
                text = reply.read().decode()
            (self.evidence / f"juicefs-{phase}-{role}.prom").write_text(text)
            result[role] = text
        return result

    def stop(self):
        errors = []
        for process in self.access_readers:
            if process.poll() is None:
                process.terminate()
                try:
                    process.wait(10)
                except subprocess.TimeoutExpired:
                    process.kill()
                    process.wait()
                    errors.append("access-log reader did not stop")
        self.access_readers.clear()
        for role in reversed(ROLES):
            proc = self.processes.get(role)
            try:
                if proc is not None and proc.poll() is None:
                    proc.terminate()
                    proc.wait(30)
                mount = Path(self.config["mount_root"]) / role
                if os.path.ismount(mount):
                    subprocess.run(
                        [executable("fusermount3"), "-u", "--", str(mount)], check=True, timeout=30
                    )
            except Exception as error:
                errors.append(role + ": " + repr(error))
        for path in self.logs.values():
            text = path.read_text().replace(self.password, "[REDACTED]")
            path.with_name(path.name.replace(".private", "")).write_text(text)
            path.unlink()
        self.logs.clear()
        return errors
