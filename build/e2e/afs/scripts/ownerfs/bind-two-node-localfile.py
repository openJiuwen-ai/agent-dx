#!/usr/bin/env python3
"""Run one bounded OwnerFs workspace bind mount scenario on Linux.

The driver uses target-built afs-meta/afs-node binaries directly. It generates
transient local-file Meta/Node configs and mTLS materials under --work-root; it
intentionally does not reuse the source repository's trial installer.
"""

from __future__ import annotations

import argparse
import hashlib
import http.client
import json
import os
import platform
import shutil
import socket
import subprocess
import sys
import time
from pathlib import Path
from subprocess import TimeoutExpired

META_IP = "127.0.0.1"
NODE_A_IP = "127.0.0.2"
NODE_B_IP = "127.0.0.3"
META_GRPC = 26400
META_REST = 26401
NODE_GRPC = 26500
NODE_REST = 26501
WORKSPACE = "workspace"
WORKLOAD_UID = 501
WORKLOAD_GID = 501




class PreflightError(RuntimeError):
    pass


def require_fresh_linux_root(work_root: Path) -> None:
    if platform.system() != "Linux" or os.geteuid() != 0:
        raise PreflightError("bind two-node local-file scenario requires Linux root")
    if not work_root.is_absolute():
        raise PreflightError(f"work root must be absolute: {work_root}")
    if work_root.exists():
        raise PreflightError(f"work root must not already exist: {work_root}")


def health_ready(status: int, body: str) -> tuple[bool, object]:
    try:
        parsed = json.loads(body)
    except json.JSONDecodeError as exc:
        return False, {"status": status, "body": body[:1000], "error": str(exc)}
    ready = isinstance(parsed, dict) and status == 200 and parsed.get("status") == "ready" and parsed.get("ready", True) is not False
    return ready, {"status": status, "body": parsed}


class Scenario:
    def __init__(self, repo: Path, binary_dir: Path, work_root: Path, keep_going: bool) -> None:
        self.repo = repo
        self.binary_dir = binary_dir
        self.root = work_root
        self.keep_going = keep_going
        self.checks: list[dict] = []
        self.receipts: list[dict] = []
        self.processes: dict[str, subprocess.Popen[bytes]] = {}
        self.logs = []

    def check(self, name: str, condition: bool, detail: object | None = None) -> None:
        self.checks.append({"name": name, "passed": bool(condition), "detail": detail})
        self._write_json("checks.json", self.checks)
        if not condition:
            raise AssertionError((name, detail))

    def command(
        self,
        argv: list[object],
        *,
        input_text: str | None = None,
        timeout: int = 30,
        check_name: str | None = None,
    ) -> str:
        printable = [str(item) for item in argv]
        proc = subprocess.run(
            printable,
            input=input_text,
            capture_output=True,
            text=True,
            timeout=timeout,
        )
        self.check(
            check_name or f"command {printable[0]}",
            proc.returncode == 0,
            {"argv": printable, "stdout": proc.stdout, "stderr": proc.stderr, "returncode": proc.returncode},
        )
        return proc.stdout

    def _write_json(self, name: str, value: object) -> None:
        (self.root / name).write_text(json.dumps(value, indent=2, sort_keys=True), encoding="utf-8")

    def preflight(self) -> None:
        require_fresh_linux_root(self.root)
        self.root.mkdir(mode=0o755)
        self.check("work root initialized", True, str(self.root))
        self.check("repo root", (self.repo / "Cargo.toml").is_file() and (self.repo / "afs/Cargo.toml").is_file(), str(self.repo))
        free = shutil.disk_usage(self.root.parent).free
        self.check("work root parent has at least 1GiB free", free >= 1024**3, {"free_bytes": free})
        required_tools = ["bash", "python3", "openssl", "findmnt", "fusermount3"]
        self.check("host tools", all(shutil.which(tool) for tool in required_tools), {tool: shutil.which(tool) for tool in required_tools})
        for name in ["afs-meta", "afs-node"]:
            exe = self.binary_dir / name
            self.check(f"{name} executable", exe.is_file() and os.access(exe, os.X_OK), str(exe))
        for address, port in [
            (META_IP, META_GRPC),
            (META_IP, META_REST),
            (NODE_A_IP, NODE_GRPC),
            (NODE_A_IP, NODE_REST),
            (NODE_B_IP, NODE_GRPC),
            (NODE_B_IP, NODE_REST),
        ]:
            with socket.socket() as sock:
                sock.bind((address, port))

    def write_identity(self) -> None:
        source_files = [
            "Cargo.toml",
            "Cargo.lock",
            "afs/Cargo.toml",
            "afs/src/node.rs",
            "afs/src/meta.rs",
            "afs/src/node/fuse.rs",
            "afs/src/node/vfs/ownerfs.rs",
            "afs/src/node/vfs/ownerfs/bind_mount.rs",
            "afs/src/node/vfs/types.rs",
        ]
        identity = {
            "uname": platform.uname()._asdict(),
            "repo": str(self.repo),
            "binaries": {
                name: hashlib.sha256((self.binary_dir / name).read_bytes()).hexdigest()
                for name in ["afs-meta", "afs-node"]
            },
            "source_files": {
                rel: hashlib.sha256((self.repo / rel).read_bytes()).hexdigest()
                for rel in source_files
                if (self.repo / rel).is_file()
            },
            "scope": "one Linux VM, local-file Meta, two distinct Node processes over mTLS TCP, OwnerFs only, functional 64KiB checks; no performance or distributed lock claim",
        }
        self._write_json("identity.json", identity)

    def generate_tls(self) -> None:
        tls = self.root / "tls"
        tls.mkdir(parents=True)
        sans = f"DNS:afs-meta,DNS:meta,IP:{META_IP},DNS:node-a,IP:{NODE_A_IP},DNS:node-b,IP:{NODE_B_IP},DNS:127.0.0.1,IP:127.0.0.1"
        self.command(["openssl", "genrsa", "-out", tls / "ca-key.pem", "2048"], check_name="generate CA key")
        self.command(
            [
                "openssl",
                "req",
                "-x509",
                "-new",
                "-nodes",
                "-key",
                tls / "ca-key.pem",
                "-sha256",
                "-days",
                "3650",
                "-subj",
                "/CN=adx-dfs-e2e-ca",
                "-out",
                tls / "ca.pem",
            ],
            check_name="generate CA certificate",
        )
        os.chmod(tls / "ca-key.pem", 0o600)
        os.chmod(tls / "ca.pem", 0o644)
        for name, cn in [("meta", "afs-meta"), ("node-a", "node-a"), ("node-b", "node-b")]:
            self.command(["openssl", "genrsa", "-out", tls / f"{name}-key.pem", "2048"], check_name=f"generate {name} key")
            self.command(
                ["openssl", "req", "-new", "-key", tls / f"{name}-key.pem", "-subj", f"/CN={cn}", "-out", tls / f"{name}.csr"],
                check_name=f"generate {name} csr",
            )
            (tls / f"{name}.ext").write_text(f"subjectAltName = {sans}\nextendedKeyUsage = serverAuth,clientAuth\n", encoding="utf-8")
            self.command(
                [
                    "openssl",
                    "x509",
                    "-req",
                    "-in",
                    tls / f"{name}.csr",
                    "-CA",
                    tls / "ca.pem",
                    "-CAkey",
                    tls / "ca-key.pem",
                    "-CAcreateserial",
                    "-out",
                    tls / f"{name}.pem",
                    "-days",
                    "3650",
                    "-sha256",
                    "-extfile",
                    tls / f"{name}.ext",
                ],
                check_name=f"sign {name} certificate",
            )
            (tls / f"{name}.csr").unlink()
            (tls / f"{name}.ext").unlink()
            os.chmod(tls / f"{name}-key.pem", 0o600)
            os.chmod(tls / f"{name}.pem", 0o644)
            self.command(["openssl", "verify", "-CAfile", tls / "ca.pem", tls / f"{name}.pem"], check_name=f"verify {name} certificate")

    def write_configs(self) -> None:
        etc = self.root / "etc"
        tls = self.root / "tls"
        for name in ["meta", "node-a", "node-b"]:
            (self.root / name / "state").mkdir(parents=True)
            (self.root / name / "run").mkdir(parents=True)
            (self.root / name / "mount" / "ownerfs").mkdir(parents=True)
        trusted = f'{{ node-a = "{tls / "node-a.pem"}", node-b = "{tls / "node-b.pem"}" }}'
        etc.mkdir()
        (etc / "meta.toml").write_text(
            f'''id = "meta-ctl"
fs = "all"
meta_store = "local-file"
data_dir = "{self.root / "meta/state"}"
grpc_listen = "{META_IP}:{META_GRPC}"
rest_listen = "{META_IP}:{META_REST}"
log_level = "warn"
trace_enabled = false
dfs_desired_copies = 1
dfs_sync_required_copies = 1
dfs_min_distinct_nodes = 1
dfs_min_distinct_failure_domains = 1
dfs_local_copy = "required"

tls_ca_certificate = "{tls / "ca.pem"}"
tls_identity_certificate = "{tls / "meta.pem"}"
tls_identity_private_key = "{tls / "meta-key.pem"}"
tls_server_name = "afs-meta"
trusted_node_certs = {trusted}
''',
            encoding="utf-8",
        )
        for node, ip in [("node-a", NODE_A_IP), ("node-b", NODE_B_IP)]:
            (etc / f"{node}.toml").write_text(
                f'''id = "{node}"
fs = "ownerfs"
meta_endpoint = "https://{META_IP}:{META_GRPC}"
grpc_listen = "{ip}:{NODE_GRPC}"
rest_listen = "{ip}:{NODE_REST}"
advertise_endpoint = "https://{ip}:{NODE_GRPC}"
data_dir = "{self.root / node / "state"}"
uds_path = "{self.root / node / "run/node.sock"}"
ownerfs_mount = "{self.root / node / "mount/ownerfs"}"
data_mode = "grpc"
log_level = "warn"
trace_enabled = false

tls_ca_certificate = "{tls / "ca.pem"}"
tls_identity_certificate = "{tls / f"{node}.pem"}"
tls_identity_private_key = "{tls / f"{node}-key.pem"}"
tls_server_name = "afs-meta"
trusted_node_certs = {trusted}
''',
                encoding="utf-8",
            )
        for role, cfg in [("meta", etc / "meta.toml"), ("node", etc / "node-a.toml"), ("node", etc / "node-b.toml")]:
            self.command([self.binary_dir / f"afs-{role}", "--config", cfg, "--print-config"], timeout=20, check_name=f"print-config {cfg.name}")

    def enable_bind_for_node_a(self) -> None:
        cfg = self.root / "etc/node-a.toml"
        with cfg.open("a", encoding="utf-8") as handle:
            handle.write(
                f'''
experimental_ownerfs_workspace_bind = true
[ownerfs_workspace_bind]
workspace = "{WORKSPACE}"
'''
            )
        self.command([self.binary_dir / "afs-node", "--config", cfg, "--print-config"], timeout=20, check_name="print-config node-a bind ON")

    def start(self, name: str) -> None:
        role = "meta" if name == "meta" else "node"
        cfg = self.root / "etc" / ("meta.toml" if name == "meta" else f"{name}.toml")
        log = open(self.root / f"{name}-{len(self.receipts)}.log", "ab")
        self.logs.append(log)
        proc = subprocess.Popen([str(self.binary_dir / f"afs-{role}"), "--config", str(cfg)], stdout=log, stderr=log, start_new_session=True)
        self.processes[name] = proc
        ip, port = {"meta": (META_IP, META_REST), "node-a": (NODE_A_IP, NODE_REST), "node-b": (NODE_B_IP, NODE_REST)}[name]
        deadline = time.monotonic() + 30
        last_error = None
        while time.monotonic() < deadline:
            if proc.poll() is not None:
                raise AssertionError((name, "early exit", proc.returncode, (self.root / f"{name}-{len(self.receipts)}.log").read_text(errors="replace")[-4000:]))
            try:
                conn = http.client.HTTPConnection(ip, port, timeout=1)
                conn.request("GET", "/health")
                response = conn.getresponse()
                body = response.read().decode("utf-8", errors="replace")
                conn.close()
                ready, detail = health_ready(response.status, body)
                if ready:
                    self.check(f"{name} healthy", True, {"pid": proc.pid, "health": detail})
                    return
                last_error = detail
            except OSError as exc:
                last_error = repr(exc)
            time.sleep(0.1)
        raise AssertionError((name, "not healthy", last_error))

    def stop(self, name: str) -> None:
        proc = self.processes[name]
        proc.terminate()
        try:
            status = proc.wait(timeout=30)
        except TimeoutExpired:
            receipt = {
                "name": name,
                "pid": proc.pid,
                "wait_status": "timeout",
                "gone": not Path(f"/proc/{proc.pid}").exists(),
                "tracked": True,
            }
            self.receipts.append(receipt)
            self._write_json("waits.json", self.receipts)
            self.check(f"{name} actual wait0", False, receipt)
            return
        receipt = {"name": name, "pid": proc.pid, "wait_status": status, "gone": not Path(f"/proc/{proc.pid}").exists(), "tracked": False}
        self.receipts.append(receipt)
        self._write_json("waits.json", self.receipts)
        if status == 0:
            self.processes.pop(name, None)
        self.check(f"{name} actual wait0", status == 0, receipt)

    def user(self, script: str, *paths: object, uid: int = WORKLOAD_UID, gid: int = WORKLOAD_GID, input_text: str | None = None) -> None:
        code = f"import os,sys\nos.setgroups([]);os.setgid({gid});os.setuid({uid})\n" + script
        self.command(["python3", "-c", code, *paths], input_text=input_text, check_name="workload user command")

    def write_payload(self, path: Path, payload: bytes) -> None:
        self.user(
            "p=sys.argv[1];data=bytes.fromhex(sys.stdin.read());f=open(p,'wb');f.write(data);f.flush();os.fsync(f.fileno());f.close()",
            path,
            input_text=payload.hex(),
        )

    def read_payload(self, path: Path, payload: bytes) -> None:
        self.user(
            "import hashlib;data=open(sys.argv[1],'rb').read();assert len(data)==int(sys.argv[2]);assert hashlib.sha256(data).hexdigest()==sys.argv[3]",
            path,
            len(payload),
            hashlib.sha256(payload).hexdigest(),
        )

    def run(self) -> None:
        self.preflight()
        self.write_identity()
        self.generate_tls()
        self.write_configs()
        try:
            self.start("meta")
            self.start("node-a")
            home = self.root / "node-a/mount/ownerfs" / WORKSPACE
            remote = self.root / "node-b/mount/ownerfs" / WORKSPACE
            home.mkdir(mode=0o700)
            os.chown(home, WORKLOAD_UID, WORKLOAD_GID)
            fd = os.open(home.parent, os.O_RDONLY | os.O_DIRECTORY)
            try:
                os.fsync(fd)
            finally:
                os.close(fd)
            self.stop("node-a")
            self.enable_bind_for_node_a()
            self.start("node-a")
            self.start("node-b")

            physical = list((self.root / "node-a/state/ownerfs").glob("root-776f726b7370616365-e*"))
            self.check("one physical Home epoch", len(physical) == 1, [str(path) for path in physical])
            found = json.loads(self.command(["findmnt", "-J", "--mountpoint", home, "-o", "TARGET,SOURCE,FSTYPE,ID"], check_name="find bind mount"))["filesystems"][0]
            home_stat, physical_stat = home.stat(), physical[0].stat()
            self.check(
                "real ext4 source bind",
                found.get("fstype") == "ext4" and (home_stat.st_dev, home_stat.st_ino) == (physical_stat.st_dev, physical_stat.st_ino),
                found,
            )
            remote_mount = json.loads(self.command(["findmnt", "-J", "--mountpoint", remote.parent, "-o", "TARGET,SOURCE,FSTYPE,ID"], check_name="find remote fuse mount"))["filesystems"][0]
            self.check("remote FUSE", remote_mount.get("source") == "afs-ownerfs", remote_mount)

            last_reverse = b""
            for payload in [b"A" * 65536, b"B" * 65536, b"CC" * 32768]:
                local_file = home / "cross.bin"
                remote_file = remote / "cross.bin"
                self.write_payload(local_file, payload)
                self.read_payload(remote_file, payload)
                last_reverse = payload[::-1] + b"REMOTE"
                self.write_payload(remote_file, last_reverse)
                self.read_payload(local_file, last_reverse)

            privileged = home / "privilege.bin"
            remote_privileged = remote / "privilege.bin"
            privileged.write_bytes(b"initial")
            os.chmod(privileged, 0o6777)
            self.user("f=os.open(sys.argv[1],os.O_WRONLY);os.write(f,b'nonowner direct IO');os.fsync(f);os.close(f)", remote_privileged)
            self.check("remote direct IO clears suid executable sgid", privileged.stat().st_mode & 0o7777 == 0o777)
            os.chmod(privileged, 0o6777)
            with open(remote_privileged, "wb") as handle:
                handle.write(b"root direct IO")
                handle.flush()
                os.fsync(handle.fileno())
            self.check("remote root CAP_FSETID preserves setid", privileged.stat().st_mode & 0o7777 == 0o6777)
            os.chmod(privileged, 0o2666)
            self.write_payload(remote_privileged, b"nonexec sgid")
            self.check("remote non executable sgid preserved", privileged.stat().st_mode & 0o7777 == 0o2666)

            for path in [home / "cross.bin", remote / "cross.bin"]:
                self.user("\ntry: os.open(sys.argv[1],os.O_RDONLY)\nexcept PermissionError: pass\nelse: raise AssertionError('uid502 unexpectedly read workspace')", path, uid=502, gid=502)
            self.user("\ntry: os.open(sys.argv[1],os.O_RDONLY)\nexcept FileNotFoundError: pass\nelse: raise AssertionError('missing file returned success')", remote / "missing")
            for folder in [home, remote]:
                self.user("f=os.open(sys.argv[1],os.O_RDONLY|os.O_DIRECTORY);os.fsync(f);os.close(f)", folder)

            self.stop("node-b")
            self.stop("node-a")
            self.stop("meta")
            for name in ["meta", "node-a", "node-b"]:
                self.start(name)
            self.read_payload(home / "cross.bin", last_reverse)
            self.read_payload(remote / "cross.bin", last_reverse)
            self.user("os.unlink(sys.argv[1]);d=os.open(sys.argv[2],os.O_RDONLY|os.O_DIRECTORY);os.fsync(d);os.close(d)", remote / "cross.bin", remote)
            self.check("delete visible at bind", not (home / "cross.bin").exists())
            self.stop("node-b")
            self.stop("node-a")
            self.stop("meta")
            mountinfo = Path("/proc/self/mountinfo").read_text(encoding="utf-8")
            self.check("normal unmount no owned mounts", not any(str(self.root) in line for line in mountinfo.splitlines()))
            self._write_json(
                "result.json",
                {
                    "status": "PASS",
                    "checks": len(self.checks),
                    "waits": self.receipts,
                    "limits": [
                        "one Linux VM with two Node processes over mTLS TCP",
                        "64KiB functional data only",
                        "local-file Meta normal full-stop recovery only",
                        "no distributed-lock guarantee",
                        "no performance claim",
                    ],
                },
            )
        finally:
            for name in ["node-b", "node-a", "meta"]:
                if name in self.processes:
                    try:
                        self.stop(name)
                    except Exception as exc:  # keep cleanup best-effort evidence without masking the first failure
                        print(f"cleanup retained {name}: {exc!r}", file=sys.stderr, flush=True)
            for log in self.logs:
                log.close()


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--repo", type=Path, default=Path.cwd(), help="target repository root; default: cwd")
    parser.add_argument("--binary-dir", type=Path, required=True, help="directory containing target-built afs-meta and afs-node")
    parser.add_argument("--work-root", type=Path, required=True, help="fresh absolute work/evidence directory on Linux ext4")
    parser.add_argument("--keep-going", action="store_true", help="reserved for future multi-case drivers; current scenario remains fail-fast")
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    scenario = Scenario(args.repo.resolve(), args.binary_dir.resolve(), args.work_root.resolve(), args.keep_going)
    scenario.run()
    print(scenario.root / "result.json")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
