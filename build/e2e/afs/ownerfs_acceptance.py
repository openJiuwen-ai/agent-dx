#!/usr/bin/env python3
"""OwnerFs three-node Linux acceptance runner.

This runner is intentionally an executable acceptance spec rather than a
production feature. It starts real afs-meta/afs-node processes on three Linux
hosts, mounts OwnerFs on A and B, and verifies the same high-level semantics as
S5 HomeFs acceptance against the new /ownerfs namespace.

Expected hosts:
  A: local shell where this script runs
  B/C: reachable over ssh

The script prints one machine-readable JSON document followed by PASS/FAIL.
"""

from __future__ import annotations

import argparse
import json
import os
import shlex
import socket
import subprocess
import sys
import tempfile
import time
import urllib.error
import urllib.request
import uuid
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, Callable


DEFAULT_TIMEOUT = 20.0


@dataclass
class CmdResult:
    role: str
    cmd: str
    returncode: int
    stdout: str
    stderr: str


@dataclass
class ProcessRef:
    role: str
    name: str
    pid: int
    log: str


@dataclass
class StepResult:
    name: str
    status: str
    detail: dict[str, Any] = field(default_factory=dict)
    error: str | None = None


class RunnerError(RuntimeError):
    pass


def find_repo_root(start: Path) -> Path:
    for parent in [start, *start.parents]:
        if (parent / "Cargo.toml").is_file() and (parent / "afs" / "common" / "protocol" / "proto").is_dir():
            return parent
    raise RunnerError(f"cannot find Agent DX repository root from {start}")


class RemoteShell:
    def __init__(self, role: str, host: str, user: str | None, ssh_opts: list[str] | None = None):
        self.role = role
        self.host = host
        self.user = user
        self.ssh_opts = ssh_opts or []

    @property
    def is_local(self) -> bool:
        return self.role == "A" or self.host in ("localhost", "127.0.0.1", "::1", socket.gethostname())

    def _argv(self, script: str) -> list[str]:
        if self.is_local:
            return ["bash", "-lc", script]
        target = f"{self.user}@{self.host}" if self.user else self.host
        return ["ssh", *self.ssh_opts, target, "bash", "-lc", shlex.quote(script)]

    def run(self, script: str, timeout: float = DEFAULT_TIMEOUT, check: bool = False) -> CmdResult:
        try:
            proc = subprocess.run(
                self._argv(script),
                text=True,
                stdout=subprocess.PIPE,
                stderr=subprocess.PIPE,
                timeout=timeout,
            )
            result = CmdResult(self.role, script, proc.returncode, proc.stdout, proc.stderr)
        except subprocess.TimeoutExpired as exc:
            stdout = exc.stdout if isinstance(exc.stdout, str) else (exc.stdout or b"").decode("utf-8", errors="replace")
            stderr = exc.stderr if isinstance(exc.stderr, str) else (exc.stderr or b"").decode("utf-8", errors="replace")
            result = CmdResult(self.role, script, 124, stdout, stderr + f"\ncommand timed out after {timeout}s")
        if check and result.returncode != 0:
            raise RunnerError(
                f"{self.role} command failed ({result.returncode}): {script}\n"
                f"stdout={result.stdout[-2000:]}\nstderr={result.stderr[-2000:]}"
            )
        return result

    def start(self, name: str, script: str, log: str) -> ProcessRef:
        quoted_log = shlex.quote(log)
        wrapped = (
            "set -euo pipefail\n"
            f"mkdir -p {shlex.quote(str(Path(log).parent))}\n"
            f"nohup bash -lc {shlex.quote(script)} >{quoted_log} 2>&1 & echo $!"
        )
        res = self.run(wrapped, timeout=DEFAULT_TIMEOUT, check=True)
        pid_text = res.stdout.strip().splitlines()[-1]
        return ProcessRef(self.role, name, int(pid_text), log)

    def kill(self, proc: ProcessRef) -> None:
        self.run(f"kill {proc.pid} >/dev/null 2>&1 || true", timeout=5)

    def tail(self, log: str, lines: int = 120) -> str:
        res = self.run(f"tail -n {int(lines)} {shlex.quote(log)} 2>/dev/null || true", timeout=10)
        return res.stdout[-12000:]


class OwnerFsAcceptance:
    def __init__(self, args: argparse.Namespace):
        self.args = args
        self.run_id = args.run_id or f"ownerfs-{int(time.time())}-{os.getpid()}"
        self.a = RemoteShell("A", args.host_a, args.ssh_user, args.ssh_option)
        self.b = RemoteShell("B", args.host_b, args.ssh_user, args.ssh_option)
        self.c = RemoteShell("C", args.host_c, args.ssh_user, args.ssh_option)
        self.roles = {"A": self.a, "B": self.b, "C": self.c}
        self.procs: list[ProcessRef] = []
        self.steps: list[StepResult] = []
        self.meta_grpc = args.meta_grpc_port
        self.meta_rest = args.meta_rest_port
        self.node_a_grpc = args.node_a_grpc_port
        self.node_a_rest = args.node_a_rest_port
        self.node_b_grpc = args.node_b_grpc_port
        self.node_b_rest = args.node_b_rest_port
        self.root_base = args.remote_tmp.rstrip("/") + "/" + self.run_id
        self.paths = {
            "A_ROOT": f"{self.root_base}/A",
            "B_ROOT": f"{self.root_base}/B",
            "C_ROOT": f"{self.root_base}/C",
        }
        self.mount_a = f"{self.paths['A_ROOT']}/mnt"
        self.mount_b = f"{self.paths['B_ROOT']}/mnt"
        self.owner_a = self.mount_a
        self.owner_b = self.mount_b
        self.workspace = args.workspace
        self.endpoint_scheme = args.endpoint_scheme or ("https" if args.tls_ca_certificate else "http")
        self.result: dict[str, Any] = {
            "run_id": self.run_id,
            "backend": "p2p",
            "endpoint_scheme": self.endpoint_scheme,
            "hosts": {"A": args.host_a, "B": args.host_b, "C": args.host_c},
            "steps": [],
            "logs": {},
            "gaps": [],
            "cleanup_errors": [],
        }


    def binary_args(self, role: str) -> str:
        parts: list[str] = []
        config = {
            "meta": self.args.meta_config,
            "A": self.args.node_a_config,
            "B": self.args.node_b_config,
        }.get(role)
        if config:
            parts.extend(["--config", config])
        for flag, value in (
            ("--tls-ca-certificate", self.args.tls_ca_certificate),
            ("--tls-identity-certificate", self.args.tls_identity_certificate),
            ("--tls-identity-private-key", self.args.tls_identity_private_key),
            ("--tls-server-name", self.args.tls_server_name),
        ):
            if value:
                parts.extend([flag, value])
        return " ".join(shlex.quote(part) for part in parts)

    def endpoint(self, host: str, port: int) -> str:
        return f"{self.endpoint_scheme}://{host}:{port}"

    def add_step(self, name: str, status: str, detail: dict[str, Any] | None = None, error: str | None = None) -> None:
        step = StepResult(name, status, detail or {}, error)
        self.steps.append(step)
        self.result["steps"].append({"name": name, "status": status, "detail": step.detail, "error": error})

    def run_step(self, name: str, fn: Callable[[], dict[str, Any] | None]) -> bool:
        start = time.time()
        try:
            detail = fn() or {}
            detail.setdefault("elapsed_ms", round((time.time() - start) * 1000, 3))
            self.add_step(name, "PASS", detail)
            return True
        except Exception as exc:  # acceptance runner must report, then clean up
            self.add_step(name, "FAIL", {"elapsed_ms": round((time.time() - start) * 1000, 3)}, str(exc))
            return False

    def wait_http(self, url: str, timeout: float = 20.0) -> dict[str, Any]:
        deadline = time.time() + timeout
        last = None
        while time.time() < deadline:
            try:
                with urllib.request.urlopen(url, timeout=2) as resp:
                    body = resp.read().decode("utf-8", errors="replace")
                    return {"status": resp.status, "body": body[:500]}
            except Exception as exc:  # noqa: BLE001 - preserve last readiness error
                last = exc
                time.sleep(0.25)
        raise RunnerError(f"timed out waiting for {url}: {last}")

    def remote_wait_http(self, role: RemoteShell, url: str, timeout: float = 20.0) -> dict[str, Any]:
        quoted = shlex.quote(url)
        script = (
            "python3 - <<'PY'\n"
            "import sys, time, urllib.request\n"
            f"url={quoted!r}\n"
            f"deadline=time.time()+{timeout!r}\n"
            "last=None\n"
            "while time.time()<deadline:\n"
            "    try:\n"
            "        with urllib.request.urlopen(url, timeout=2) as r:\n"
            "            print(r.status)\n"
            "            print(r.read().decode('utf-8','replace')[:500])\n"
            "            sys.exit(0)\n"
            "    except Exception as e:\n"
            "        last=e; time.sleep(0.25)\n"
            "print(last, file=sys.stderr)\n"
            "sys.exit(1)\n"
            "PY"
        )
        res = role.run(script, timeout=timeout + 5, check=True)
        lines = res.stdout.splitlines()
        return {"status": int(lines[0]) if lines else None, "body": "\n".join(lines[1:])[:500]}

    def preflight(self) -> dict[str, Any]:
        detail: dict[str, Any] = {}
        for role in (self.a, self.b, self.c):
            linux = role.run("uname -s", check=True).stdout.strip()
            if linux != "Linux":
                raise RunnerError(f"role {role.role} is {linux}; acceptance must run on Linux")
            meta_extra = self.binary_args("meta")
            node_extra = self.binary_args(role.role) if role.role in ("A", "B") else ""
            bin_checks = [
                f"test -x {shlex.quote(self.args.bin_dir)}/afs-meta",
                f"test -x {shlex.quote(self.args.bin_dir)}/afs-node",
                f"{shlex.quote(self.args.bin_dir)}/afs-meta --print-config {meta_extra} >/dev/null",
                f"{shlex.quote(self.args.bin_dir)}/afs-node --print-config --fs ownerfs --ownerfs-mount /tmp/nonexistent {node_extra} >/dev/null",
                "command -v python3 >/dev/null",
            ]
            if role.role in ("A", "B"):
                bin_checks.extend(["test -e /dev/fuse", "command -v fusermount3 >/dev/null || command -v fusermount >/dev/null", "command -v mountpoint >/dev/null"])
            role.run(" && ".join(bin_checks), timeout=20, check=True)
            detail[role.role] = {"kernel": linux, "bin_dir": self.args.bin_dir}
        if self.args.etcd_endpoint:
            # afs-meta is responsible for the real etcd connection; this only records intent.
            detail["etcd_endpoint"] = self.args.etcd_endpoint
        elif self.args.allow_missing_etcd_for_scaffold:
            self.result["gaps"].append("no --etcd-endpoint supplied; MetaStore durability cannot be accepted")
        else:
            raise RunnerError("--etcd-endpoint is required for OwnerFs acceptance; use --allow-missing-etcd-for-scaffold only for harness debugging")
        if self.args.location_api == "grpcurl":
            self.c.run(f"command -v {shlex.quote(self.args.grpcurl)} >/dev/null", timeout=10, check=True)
        return detail

    def prepare_hosts(self) -> dict[str, Any]:
        for role, root_key in ((self.a, "A_ROOT"), (self.b, "B_ROOT"), (self.c, "C_ROOT")):
            root = self.paths[root_key]
            role.run(
                "set -euo pipefail\n"
                f"mkdir -p {shlex.quote(root)} {shlex.quote(root + '/logs')} {shlex.quote(root + '/data')} {shlex.quote(root + '/mnt')}\n"
                f"if mountpoint -q {shlex.quote(root + '/mnt')}; then (fusermount3 -uz {shlex.quote(root + '/mnt')} || fusermount -uz {shlex.quote(root + '/mnt')} || umount -l {shlex.quote(root + '/mnt')} || true); fi",
                timeout=15,
                check=True,
            )
        return {"root_base": self.root_base}

    def start_cluster(self) -> dict[str, Any]:
        meta_endpoint = self.endpoint(self.args.host_c, self.meta_grpc)
        etcd = self.args.etcd_endpoint or "memory://acceptance-missing-etcd"
        meta_extra = self.binary_args("meta")
        meta_cmd = (
            f"exec {shlex.quote(self.args.bin_dir)}/afs-meta "
            f"--id meta-{shlex.quote(self.run_id)} "
            f"--grpc-listen 0.0.0.0:{self.meta_grpc} "
            f"--rest-listen 0.0.0.0:{self.meta_rest} "
            f"--etcd-endpoint {shlex.quote(etcd)} "
            f"{meta_extra}"
        )
        self.procs.append(self.c.start("afs-meta", meta_cmd, f"{self.paths['C_ROOT']}/logs/afs-meta.log"))
        self.remote_wait_http(self.c, f"http://127.0.0.1:{self.meta_rest}/health", timeout=20)

        node_a_extra = self.binary_args("A")
        node_b_extra = self.binary_args("B")
        node_a_advertise = self.args.node_a_advertise_endpoint or self.endpoint(self.args.host_a, self.node_a_grpc)
        node_b_advertise = self.args.node_b_advertise_endpoint or self.endpoint(self.args.host_b, self.node_b_grpc)
        node_a_cmd = (
            f"exec {shlex.quote(self.args.bin_dir)}/afs-node "
            f"--id node-a-{shlex.quote(self.run_id)} "
            f"--grpc-listen 0.0.0.0:{self.node_a_grpc} "
            f"--rest-listen 0.0.0.0:{self.node_a_rest} "
            f"--meta-endpoint {shlex.quote(meta_endpoint)} "
            f"--peer-endpoint {shlex.quote(node_a_advertise)} "
            f"--advertise-endpoint {shlex.quote(node_a_advertise)} "
            f"--fs ownerfs --data-mode grpc "
            f"--data-dir {shlex.quote(self.paths['A_ROOT'] + '/data')} "
            f"--uds-path {shlex.quote(self.paths['A_ROOT'] + '/node.sock')} "
            f"--ownerfs-mount {shlex.quote(self.mount_a)} "
            f"{node_a_extra}"
        )
        node_b_cmd = (
            f"exec {shlex.quote(self.args.bin_dir)}/afs-node "
            f"--id node-b-{shlex.quote(self.run_id)} "
            f"--grpc-listen 0.0.0.0:{self.node_b_grpc} "
            f"--rest-listen 0.0.0.0:{self.node_b_rest} "
            f"--meta-endpoint {shlex.quote(meta_endpoint)} "
            f"--peer-endpoint {shlex.quote(node_a_advertise)} "
            f"--advertise-endpoint {shlex.quote(node_b_advertise)} "
            f"--fs ownerfs --data-mode grpc "
            f"--data-dir {shlex.quote(self.paths['B_ROOT'] + '/data')} "
            f"--uds-path {shlex.quote(self.paths['B_ROOT'] + '/node.sock')} "
            f"--ownerfs-mount {shlex.quote(self.mount_b)} "
            f"{node_b_extra}"
        )
        self.procs.append(self.a.start("afs-node-a", node_a_cmd, f"{self.paths['A_ROOT']}/logs/afs-node-a.log"))
        self.procs.append(self.b.start("afs-node-b", node_b_cmd, f"{self.paths['B_ROOT']}/logs/afs-node-b.log"))
        self.wait_mount(self.a, self.mount_a)
        self.wait_mount(self.b, self.mount_b)
        self.remote_wait_http(self.a, f"http://127.0.0.1:{self.node_a_rest}/health", timeout=20)
        self.remote_wait_http(self.b, f"http://127.0.0.1:{self.node_b_rest}/health", timeout=20)
        return {"meta_endpoint": meta_endpoint, "mount_a": self.mount_a, "mount_b": self.mount_b}

    def wait_mount(self, role: RemoteShell, mount: str, timeout: float = 20.0) -> None:
        deadline = time.time() + timeout
        while time.time() < deadline:
            if role.run(f"mountpoint -q {shlex.quote(mount)}", timeout=3).returncode == 0:
                return
            time.sleep(0.25)
        raise RunnerError(f"{role.role} mount did not become ready: {mount}\n{role.tail(self.log_for(role.role))}")

    def log_for(self, role: str) -> str:
        for proc in reversed(self.procs):
            if proc.role == role:
                return proc.log
        return "/dev/null"

    def namespace_ready(self) -> dict[str, Any]:
        out_a = self.a.run(f"ls -la {shlex.quote(self.mount_a)}", timeout=10, check=True).stdout
        out_b = self.b.run(f"ls -la {shlex.quote(self.mount_b)}", timeout=10, check=True).stdout
        if "ownerfs" not in out_a or "ownerfs" not in out_b:
            raise RunnerError(f"/ownerfs namespace missing: A={out_a!r} B={out_b!r}")
        return {"A_ls": out_a[:300], "B_ls": out_b[:300]}

    def install_meta_proto(self) -> None:
        proto = Path(self.args.proto_root) / "meta.proto"
        if not proto.exists():
            raise RunnerError(f"meta.proto not found under --proto-root: {proto}")
        remote_dir = f"{self.root_base}/proto"
        remote_proto = f"{remote_dir}/meta.proto"
        content = proto.read_text(encoding="utf-8")
        script = (
            f"mkdir -p {shlex.quote(remote_dir)} && "
            f"cat > {shlex.quote(remote_proto)} <<'OWNERFS_META_PROTO'\n"
            f"{content}\n"
            "OWNERFS_META_PROTO"
        )
        self.c.run(script, timeout=10, check=True)

    def location_lookup(self, root: str) -> dict[str, Any]:
        # OwnerFs names are arbitrary Linux bytes; the Meta key is the stable
        # encoded RootId, not the display name used in the mount path.
        root_id = "root-" + os.fsencode(root).hex()
        if self.args.location_api == "rest":
            path = self.args.location_path_template.format(root=root_id)
            url = f"http://127.0.0.1:{self.meta_rest}{path}"
            try:
                response = self.remote_wait_http(self.c, url, timeout=5)
                payload = json.loads(response["body"])
                expected = self.args.expected_home_node_id or "node-a-" + self.run_id
                if payload.get("root_id") != root_id or payload.get("home_node_id") != expected:
                    raise RunnerError(f"root location mismatch for {root}: {payload}")
                return payload
            except Exception as exc:
                raise RunnerError(f"root location REST API unavailable or not authoritative at {path}: {exc}") from exc
        return self.location_lookup_grpcurl(root_id)

    def location_lookup_grpcurl(self, root: str) -> dict[str, Any]:
        self.install_meta_proto()
        remote_proto_dir = f"{self.root_base}/proto"
        remote_request = f"{self.root_base}/lookup-{uuid.uuid4().hex}.json"
        remote_output = f"{self.root_base}/lookup-{uuid.uuid4().hex}.out"
        request = {"requestId": f"accept-{uuid.uuid4().hex}", "rootId": root}
        request_json = json.dumps(request, separators=(",", ":"))
        tls_args: list[str] = []
        if self.endpoint_scheme == "http":
            tls_args.append("-plaintext")
        else:
            if self.args.tls_ca_certificate:
                tls_args.extend(["-cacert", self.args.tls_ca_certificate])
            if self.args.tls_identity_certificate and self.args.tls_identity_private_key:
                tls_args.extend(["-cert", self.args.tls_identity_certificate, "-key", self.args.tls_identity_private_key])
            if self.args.tls_server_name:
                tls_args.extend(["-authority", self.args.tls_server_name])
        grpcurl_args = " ".join(shlex.quote(part) for part in [self.args.grpcurl, *tls_args, "-import-path", remote_proto_dir, "-proto", "meta.proto", "-d", f"@{remote_request}", f"127.0.0.1:{self.meta_grpc}", "afs.meta.v1.OwnerRoots/LookupRoot"])
        script = (
            f"cat > {shlex.quote(remote_request)} <<'OWNERFS_LOOKUP_JSON'\n"
            f"{request_json}\n"
            "OWNERFS_LOOKUP_JSON\n"
            f"{grpcurl_args} > {shlex.quote(remote_output)}\n"
            f"cat {shlex.quote(remote_output)}"
        )
        res = self.c.run(script, timeout=10, check=True)
        try:
            payload = json.loads(res.stdout)
        except json.JSONDecodeError as exc:
            raise RunnerError(f"LookupRoot grpcurl returned non-JSON: {res.stdout!r}") from exc
        if not payload.get("found"):
            raise RunnerError(f"LookupRoot did not find {root}: {payload}")
        location = payload.get("location") or {}
        if location.get("homeNodeId") not in {"node-a-" + self.run_id, self.args.expected_home_node_id or "node-a-" + self.run_id}:
            raise RunnerError(f"LookupRoot returned unexpected home node for {root}: {payload}")
        return payload

    def step_root_create_and_location(self) -> dict[str, Any]:
        path = f"{self.owner_a}/{self.workspace}"
        self.a.run(f"mkdir -p {shlex.quote(path)}", timeout=10, check=True)
        lookup = self.location_lookup(self.workspace)
        return {"root": self.workspace, "location": lookup}

    def step_close_to_open(self) -> dict[str, Any]:
        file_a = f"{self.owner_a}/{self.workspace}/dir/file.txt"
        file_b = f"{self.owner_b}/{self.workspace}/dir/file.txt"
        self.a.run(f"mkdir -p {shlex.quote(str(Path(file_a).parent))} && printf 'alpha' > {shlex.quote(file_a)} && sync -f {shlex.quote(file_a)}", timeout=10, check=True)
        out = self.b.run(f"cat {shlex.quote(file_b)}", timeout=10, check=True).stdout
        if out != "alpha":
            raise RunnerError(f"B reopen read mismatch: {out!r}")
        return {"bytes": out}

    def step_remote_reopen_after_rewrite(self) -> dict[str, Any]:
        file_a = f"{self.owner_a}/{self.workspace}/dir/file.txt"
        file_b = f"{self.owner_b}/{self.workspace}/dir/file.txt"
        self.a.run(f"printf 'bravo-longer' > {shlex.quote(file_a)} && sync -f {shlex.quote(file_a)}", timeout=10, check=True)
        out = self.b.run(f"cat {shlex.quote(file_b)}", timeout=10, check=True).stdout
        if out != "bravo-longer":
            raise RunnerError(f"B reopen after A rewrite mismatch: {out!r}")
        return {"bytes": out}

    def step_rename_unlink_rmdir(self) -> dict[str, Any]:
        base_a = f"{self.owner_a}/{self.workspace}/mut"
        base_b = f"{self.owner_b}/{self.workspace}/mut"
        self.a.run(f"mkdir -p {shlex.quote(base_a)} && printf 'rename' > {shlex.quote(base_a + '/old')} && mv {shlex.quote(base_a + '/old')} {shlex.quote(base_a + '/new')}", timeout=10, check=True)
        out = self.b.run(f"cat {shlex.quote(base_b + '/new')}", timeout=10, check=True).stdout
        if out != "rename":
            raise RunnerError(f"rename visibility mismatch: {out!r}")
        self.b.run(f"rm {shlex.quote(base_b + '/new')} && rmdir {shlex.quote(base_b)}", timeout=10, check=True)
        chk = self.a.run(f"test ! -e {shlex.quote(base_a)}", timeout=10)
        if chk.returncode != 0:
            raise RunnerError("unlink/rmdir from B not visible on A")
        return {"renamed_bytes": out}

    def step_concurrent_root_create_single_home(self) -> dict[str, Any]:
        root = f"race-{self.run_id}-{self.workspace}"
        cmd_a = f"(mkdir {shlex.quote(self.owner_a + '/' + root)} && echo A || echo A_FAIL) > {shlex.quote(self.paths['A_ROOT'] + '/logs/race.out')} 2>&1 &"
        cmd_b = f"(mkdir {shlex.quote(self.owner_b + '/' + root)} && echo B || echo B_FAIL) > {shlex.quote(self.paths['B_ROOT'] + '/logs/race.out')} 2>&1 &"
        self.a.run(cmd_a, timeout=5, check=True)
        self.b.run(cmd_b, timeout=5, check=True)
        time.sleep(1.0)
        out_a = self.a.run(f"cat {shlex.quote(self.paths['A_ROOT'] + '/logs/race.out')} 2>/dev/null || true", timeout=5).stdout.strip()
        out_b = self.b.run(f"cat {shlex.quote(self.paths['B_ROOT'] + '/logs/race.out')} 2>/dev/null || true", timeout=5).stdout.strip()
        if (out_a.endswith("A")) == (out_b.endswith("B")):
            raise RunnerError(f"concurrent mkdir requires exactly one winner: A={out_a!r}, B={out_b!r}")
        lookup = self.location_lookup(root)
        return {"A": out_a, "B": out_b, "location": lookup}

    def step_old_fd_after_rename(self) -> dict[str, Any]:
        base_a = f"{self.owner_a}/{self.workspace}/fd"
        base_b = f"{self.owner_b}/{self.workspace}/fd"
        self.a.run(f"mkdir -p {shlex.quote(base_a)} && printf 'oldfd' > {shlex.quote(base_a + '/x')}", timeout=10, check=True)
        script_path = f"{self.paths['B_ROOT']}/logs/oldfd-reader.py"
        script_log = f"{self.paths['B_ROOT']}/logs/oldfd-reader.log"
        script = (
            f"cat > {shlex.quote(script_path)} <<'PY'\n"
            "import os, time\n"
            f"p={str(base_b + '/x')!r}\n"
            f"out={str(self.paths['B_ROOT'] + '/logs/oldfd.out')!r}\n"
            "fd=os.open(p, os.O_RDONLY)\n"
            "time.sleep(1.5)\n"
            "data=os.read(fd, 100)\n"
            "os.close(fd)\n"
            "open(out,'wb').write(data)\n"
            "PY\n"
            f"nohup python3 {shlex.quote(script_path)} > {shlex.quote(script_log)} 2>&1 & echo $!"
        )
        self.b.run(script, timeout=5, check=True)
        time.sleep(0.4)
        self.a.run(f"mv {shlex.quote(base_a + '/x')} {shlex.quote(base_a + '/y')} && printf 'newname' > {shlex.quote(base_a + '/x')}", timeout=10, check=True)
        time.sleep(2.0)
        out = self.b.run(f"cat {shlex.quote(self.paths['B_ROOT'] + '/logs/oldfd.out')} 2>/dev/null", timeout=5, check=True).stdout
        if out != "oldfd":
            raise RunnerError(f"old fd after rename read {out!r}")
        return {"old_fd_bytes": out}

    def step_unlinked_fd_survives_name_reuse(self) -> dict[str, Any]:
        base_a = f"{self.owner_a}/{self.workspace}/unlinkfd"
        base_b = f"{self.owner_b}/{self.workspace}/unlinkfd"
        self.a.run(f"mkdir -p {shlex.quote(base_a)} && printf 'survive' > {shlex.quote(base_a + '/x')}", timeout=10, check=True)
        script_path = f"{self.paths['B_ROOT']}/logs/unlinkfd-reader.py"
        script_log = f"{self.paths['B_ROOT']}/logs/unlinkfd-reader.log"
        script = (
            f"cat > {shlex.quote(script_path)} <<'PY'\n"
            "import os, time\n"
            f"p={str(base_b + '/x')!r}\n"
            f"out={str(self.paths['B_ROOT'] + '/logs/unlinkfd.out')!r}\n"
            "fd=os.open(p, os.O_RDONLY)\n"
            "time.sleep(1.5)\n"
            "data=os.read(fd, 100)\n"
            "os.close(fd)\n"
            "open(out,'wb').write(data)\n"
            "PY\n"
            f"nohup python3 {shlex.quote(script_path)} > {shlex.quote(script_log)} 2>&1 & echo $!"
        )
        self.b.run(script, timeout=5, check=True)
        time.sleep(0.4)
        self.a.run(f"rm {shlex.quote(base_a + '/x')} && printf 'reuse' > {shlex.quote(base_a + '/x')}", timeout=10, check=True)
        time.sleep(2.0)
        out = self.b.run(f"cat {shlex.quote(self.paths['B_ROOT'] + '/logs/unlinkfd.out')} 2>/dev/null", timeout=5, check=True).stdout
        if out != "survive":
            raise RunnerError(f"unlinked fd read {out!r}")
        return {"old_fd_bytes": out}

    def step_alternating_lengths(self) -> dict[str, Any]:
        file_a = f"{self.owner_a}/{self.workspace}/lengths.txt"
        file_b = f"{self.owner_b}/{self.workspace}/lengths.txt"
        seen: list[int] = []
        for payload in ("x", "yyyyyyyy", "zz", "qqqqqqqqqqqqqqq"):
            self.a.run(f"printf {shlex.quote(payload)} > {shlex.quote(file_a)} && sync -f {shlex.quote(file_a)}", timeout=10, check=True)
            out = self.b.run(f"cat {shlex.quote(file_b)}", timeout=10, check=True).stdout
            if out != payload:
                raise RunnerError(f"length payload mismatch: expected {payload!r}, got {out!r}")
            seen.append(len(out))
        return {"lengths": seen}

    def step_meta_restart(self) -> dict[str, Any]:
        metas = [p for p in self.procs if p.name == "afs-meta"]
        if not metas:
            raise RunnerError("meta process ref missing")
        self.c.kill(metas[-1])
        time.sleep(1.0)
        self.procs.remove(metas[-1])
        etcd = self.args.etcd_endpoint or "memory://acceptance-missing-etcd"
        meta_extra = self.binary_args("meta")
        meta_cmd = (
            f"exec {shlex.quote(self.args.bin_dir)}/afs-meta "
            f"--id meta-{shlex.quote(self.run_id)}-restart "
            f"--grpc-listen 0.0.0.0:{self.meta_grpc} "
            f"--rest-listen 0.0.0.0:{self.meta_rest} "
            f"--etcd-endpoint {shlex.quote(etcd)} "
            f"{meta_extra}"
        )
        self.procs.append(self.c.start("afs-meta", meta_cmd, f"{self.paths['C_ROOT']}/logs/afs-meta-restart.log"))
        self.remote_wait_http(self.c, f"http://127.0.0.1:{self.meta_rest}/health", timeout=20)
        lookup = self.location_lookup(self.workspace)
        out = self.b.run(f"cat {shlex.quote(self.owner_b + '/' + self.workspace + '/dir/file.txt')}", timeout=10, check=True).stdout
        if out != "bravo-longer":
            raise RunnerError(f"post-meta-restart read mismatch: {out!r}")
        return {"location": lookup, "bytes": out}

    def step_home_restart_failure_return(self) -> dict[str, Any]:
        node_a = [p for p in self.procs if p.name == "afs-node-a"]
        if not node_a:
            raise RunnerError("node A process ref missing")
        self.a.kill(node_a[-1])
        time.sleep(1.0)
        miss = self.b.run(f"cat {shlex.quote(self.owner_b + '/' + self.workspace + '/dir/file.txt')}", timeout=10)
        failure_observed = miss.returncode != 0
        self.procs.remove(node_a[-1])
        node_a_advertise = self.args.node_a_advertise_endpoint or self.endpoint(self.args.host_a, self.node_a_grpc)
        node_a_cmd = (
            f"exec {shlex.quote(self.args.bin_dir)}/afs-node "
            f"--id node-a-{shlex.quote(self.run_id)} "
            f"--grpc-listen 0.0.0.0:{self.node_a_grpc} "
            f"--rest-listen 0.0.0.0:{self.node_a_rest} "
            f"--meta-endpoint {shlex.quote(self.endpoint(self.args.host_c, self.meta_grpc))} "
            f"--peer-endpoint {shlex.quote(node_a_advertise)} "
            f"--advertise-endpoint {shlex.quote(node_a_advertise)} "
            f"--fs ownerfs --data-mode grpc "
            f"--data-dir {shlex.quote(self.paths['A_ROOT'] + '/data')} "
            f"--uds-path {shlex.quote(self.paths['A_ROOT'] + '/node.sock')} "
            f"--ownerfs-mount {shlex.quote(self.mount_a)} "
            f"{self.binary_args('A')}"
        )
        self.procs.append(self.a.start("afs-node-a", node_a_cmd, f"{self.paths['A_ROOT']}/logs/afs-node-a-restart.log"))
        self.wait_mount(self.a, self.mount_a)
        out = self.b.run(f"cat {shlex.quote(self.owner_b + '/' + self.workspace + '/dir/file.txt')}", timeout=10, check=True).stdout
        if out != "bravo-longer":
            raise RunnerError(f"post-home-restart read mismatch: {out!r}")
        return {"failure_observed": failure_observed, "bytes_after_restart": out, "failure_stderr": miss.stderr[-500:]}

    def step_cross_root_exdev(self) -> dict[str, Any]:
        r1 = f"{self.owner_a}/{self.workspace}"
        r2 = f"{self.owner_a}/exdev-{self.run_id}-{self.workspace}"
        self.a.run(f"mkdir -p {shlex.quote(r1)} {shlex.quote(r2)} && printf x > {shlex.quote(r1 + '/move-me')}", timeout=10, check=True)
        res = self.a.run(f"mv {shlex.quote(r1 + '/move-me')} {shlex.quote(r2 + '/move-me')}", timeout=10)
        # mv may hide EXDEV by copy+unlink. Use Python os.rename to require kernel errno.
        py = (
            "python3 - <<'PY'\n"
            "import errno, os, sys\n"
            f"src={str(r1 + '/move-me2')!r}\n"
            f"dst={str(r2 + '/move-me2')!r}\n"
            "open(src,'w').write('x')\n"
            "try:\n"
            "    os.rename(src,dst)\n"
            "except OSError as e:\n"
            "    print(e.errno)\n"
            "    sys.exit(0 if e.errno == errno.EXDEV else 2)\n"
            "sys.exit(1)\n"
            "PY"
        )
        chk = self.a.run(py, timeout=10)
        if chk.returncode != 0:
            raise RunnerError(f"cross-root rename did not return EXDEV: rc={chk.returncode} stdout={chk.stdout!r} stderr={chk.stderr!r}; mv_rc={res.returncode}")
        return {"errno": chk.stdout.strip()}

    def note_cleanup_error(self, action: str, role: str, error: str) -> None:
        self.result.setdefault("cleanup_errors", []).append({"action": action, "role": role, "error": error[-2000:]})

    def cleanup_run(self, role: RemoteShell, action: str, script: str, timeout: float = 8.0) -> None:
        try:
            result = role.run(script, timeout=timeout, check=False)
            if result.returncode != 0:
                self.note_cleanup_error(action, role.role, f"rc={result.returncode} stdout={result.stdout[-500:]} stderr={result.stderr[-1000:]}")
        except Exception as exc:  # cleanup must not hide the acceptance result
            self.note_cleanup_error(action, role.role, repr(exc))

    def cleanup(self) -> None:
        for role, mount in ((self.a, self.mount_a), (self.b, self.mount_b)):
            self.cleanup_run(
                role,
                f"unmount {mount}",
                f"(mountpoint -q {shlex.quote(mount)} || grep -qs ' {shlex.quote(mount)} ' /proc/mounts) && "
                f"(fusermount3 -uz {shlex.quote(mount)} || fusermount -uz {shlex.quote(mount)} || umount -l {shlex.quote(mount)} || true) || true",
                timeout=8,
            )
        for proc in reversed(self.procs):
            try:
                self.roles[proc.role].kill(proc)
            except Exception as exc:
                self.note_cleanup_error(f"kill {proc.name}", proc.role, repr(exc))
        if not self.args.keep_tmp:
            for role in (self.a, self.b, self.c):
                self.cleanup_run(role, f"remove {self.root_base}", f"rm -rf {shlex.quote(self.root_base)}", timeout=8)

    def collect_logs(self) -> None:
        for proc in self.procs:
            try:
                self.result["logs"][f"{proc.role}:{proc.name}"] = self.roles[proc.role].tail(proc.log)
            except Exception as exc:
                self.note_cleanup_error(f"tail {proc.name}", proc.role, repr(exc))

    def execute(self) -> dict[str, Any]:
        try:
            if not self.run_step("preflight binaries, Linux, FUSE, etcd intent", self.preflight):
                return self.finish(False)
            if not self.run_step("prepare temporary dirs and stale mounts", self.prepare_hosts):
                return self.finish(False)
            if self.args.preflight_only:
                return self.finish(True)
            if not self.run_step("start persistent meta and A/B OwnerFs nodes", self.start_cluster):
                return self.finish(False)
            for name, fn in (
                ("/ownerfs namespace exists on A and B", self.namespace_ready),
                ("root mkdir on A and management location API", self.step_root_create_and_location),
                ("A close then B reopen reads exact bytes", self.step_close_to_open),
                ("A rewrite then B reopen sees new bytes", self.step_remote_reopen_after_rewrite),
                ("rename/unlink/rmdir are visible cross-node", self.step_rename_unlink_rmdir),
                ("concurrent root create has one authority", self.step_concurrent_root_create_single_home),
                ("remote old fd remains bound after rename/name reuse", self.step_old_fd_after_rename),
                ("remote unlinked fd survives name reuse", self.step_unlinked_fd_survives_name_reuse),
                ("alternating cross-node lengths are exact", self.step_alternating_lengths),
                ("meta restart preserves root ownership and read", self.step_meta_restart),
                ("home P2P service restart exposes failure then recovers", self.step_home_restart_failure_return),
                ("cross-root rename returns EXDEV", self.step_cross_root_exdev),
            ):
                if not self.run_step(name, fn):
                    return self.finish(False)
            return self.finish(True)
        finally:
            try:
                self.collect_logs()
            except Exception as exc:
                self.note_cleanup_error("collect logs", "local", repr(exc))
            try:
                self.cleanup()
            except Exception as exc:
                self.note_cleanup_error("cleanup", "local", repr(exc))

    def finish(self, ok: bool) -> dict[str, Any]:
        self.result["status"] = "PASS" if ok else "FAIL"
        self.result["passed"] = sum(1 for s in self.steps if s.status == "PASS")
        self.result["failed"] = sum(1 for s in self.steps if s.status == "FAIL")
        return self.result


def parse_args(argv: list[str]) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Run OwnerFs three-Linux-VM acceptance against afs-meta/afs-node.")
    parser.add_argument("--host-a", default="127.0.0.1", help="node A address; A is executed locally")
    parser.add_argument("--host-b", required=True, help="node B ssh address")
    parser.add_argument("--host-c", required=True, help="meta node C ssh address")
    parser.add_argument("--ssh-user", default=os.environ.get("USER"), help="ssh user for B/C")
    parser.add_argument("--ssh-option", action="append", default=["-o", "BatchMode=yes", "-o", "StrictHostKeyChecking=no"], help="extra ssh option token; repeat for multiple tokens")
    parser.add_argument("--bin-dir", required=True, help="directory containing afs-meta and afs-node on every host")
    parser.add_argument("--etcd-endpoint", default=os.environ.get("AFS_ETCD_ENDPOINT"), help="etcd endpoint for afs-meta, e.g. http://127.0.0.1:2379")
    parser.add_argument("--allow-missing-etcd-for-scaffold", action="store_true", help="only for harness debugging; full acceptance requires etcd")
    parser.add_argument("--endpoint-scheme", choices=["http", "https"], help="gRPC URI scheme; defaults to https when TLS CA is supplied, else http")
    parser.add_argument("--meta-config", help="afs-meta TOML config path on C")
    parser.add_argument("--node-a-config", help="afs-node TOML config path on A")
    parser.add_argument("--node-b-config", help="afs-node TOML config path on B")
    parser.add_argument("--node-a-advertise-endpoint", help="published node A gRPC URI")
    parser.add_argument("--node-b-advertise-endpoint", help="published node B gRPC URI")
    parser.add_argument("--tls-ca-certificate", help="CA certificate path available on every host")
    parser.add_argument("--tls-identity-certificate", help="identity certificate path available on every host")
    parser.add_argument("--tls-identity-private-key", help="identity private key path available on every host")
    parser.add_argument("--tls-server-name", help="TLS server name/authority")
    parser.add_argument("--remote-tmp", default="/tmp/afs-ownerfs-acceptance", help="base temp dir on all hosts")
    parser.add_argument("--run-id", default=None)
    parser.add_argument("--workspace", default="job-42")
    parser.add_argument("--port-base", type=int, help="first port for automatic allocation; defaults to a run-specific high port")
    parser.add_argument("--meta-grpc-port", type=int)
    parser.add_argument("--meta-rest-port", type=int)
    parser.add_argument("--node-a-grpc-port", type=int)
    parser.add_argument("--node-a-rest-port", type=int)
    parser.add_argument("--node-b-grpc-port", type=int)
    parser.add_argument("--node-b-rest-port", type=int)
    parser.add_argument("--location-api", choices=["grpcurl", "rest"], default="rest", help="how to verify Meta root location")
    parser.add_argument("--grpcurl", default="grpcurl", help="grpcurl binary on meta host C for OwnerRoots.LookupRoot")
    parser.add_argument("--proto-root", default=str(find_repo_root(Path(__file__).resolve()) / "afs" / "common" / "protocol" / "proto"), help="directory containing meta.proto on node A; copied to C for grpcurl")
    parser.add_argument("--expected-home-node-id", help="override expected home node id in LookupRoot")
    parser.add_argument("--location-path-template", default="/v1/roots/{root}", help="meta REST path expected to return root ownership when --location-api=rest")
    parser.add_argument("--preflight-only", action="store_true")
    parser.add_argument("--keep-tmp", action="store_true")
    parser.add_argument("--output", help="write JSON result to path")
    args = parser.parse_args(argv)
    base = args.port_base if args.port_base is not None else 17000 + ((os.getpid() + int(time.time())) % 900) * 30
    args.meta_grpc_port = args.meta_grpc_port or base
    args.meta_rest_port = args.meta_rest_port or base + 1
    args.node_a_grpc_port = args.node_a_grpc_port or base + 10
    args.node_a_rest_port = args.node_a_rest_port or base + 11
    args.node_b_grpc_port = args.node_b_grpc_port or base + 20
    args.node_b_rest_port = args.node_b_rest_port or base + 21
    return args


def main(argv: list[str]) -> int:
    args = parse_args(argv)
    runner = OwnerFsAcceptance(args)
    result = runner.execute()
    text = json.dumps(result, ensure_ascii=False, indent=2, sort_keys=True)
    print(text)
    print(result["status"])
    if args.output:
        Path(args.output).write_text(text + "\n", encoding="utf-8")
    return 0 if result["status"] == "PASS" else 1


if __name__ == "__main__":
    raise SystemExit(main(sys.argv[1:]))
