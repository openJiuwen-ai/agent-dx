#!/usr/bin/env python3
"""Run a bounded DFS one-writer/two-reader smoke on Linux.

This migration driver starts one local-file Meta process and three independent
DFS Node processes on one Linux VM. It proves only the migrated target binaries
can mount DFS, accept one writer on A, serve independent readers on B/C, reflect
deletion, and stop cleanly. It is not a performance, cross-host, three-copy, or
crash-recovery qualification.
"""

from __future__ import annotations

import argparse
import concurrent.futures
import hashlib
import http.client
import importlib.util
import json
import os
from pathlib import Path
import subprocess
import sys
import time
from typing import Any

NODE_C_IP = "127.0.0.4"
PAYLOAD_SIZE = 64 * 1024
GENERATIONS = 3


def load_owner_helpers(repo: Path) -> Any:
    script = repo / "build/e2e/afs/scripts/ownerfs/bind-two-node-localfile.py"
    spec = importlib.util.spec_from_file_location("migration_owner_helpers", script)
    if spec is None or spec.loader is None:
        raise RuntimeError(f"cannot load owner helper: {script}")
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


def rewrite_trusted_line(text: str, trusted: str) -> str:
    return "\n".join(trusted if line.startswith("trusted_node_certs =") else line for line in text.splitlines()) + "\n"


def read_with_oracle(path: Path, expected_sha: str) -> subprocess.CompletedProcess[str]:
    code = "import hashlib,sys;d=open(sys.argv[1],'rb').read();assert len(d)==65536;assert hashlib.sha256(d).hexdigest()==sys.argv[2]"
    return subprocess.run(["python3", "-c", code, str(path), expected_sha], capture_output=True, text=True, timeout=30, check=False)


class DfsCoreRun:
    def __init__(self, owner: Any, repo: Path, binary_dir: Path, work_root: Path) -> None:
        self.owner = owner
        self.run = self._new_run(repo, binary_dir, work_root)

    def _new_run(self, repo: Path, binary_dir: Path, work_root: Path):
        owner = self.owner

        class Run(owner.Scenario):
            def start(self, name: str) -> None:
                if name != "node-c":
                    return super().start(name)
                log = open(self.root / f"node-c-{len(self.receipts)}.log", "ab")
                self.logs.append(log)
                proc = subprocess.Popen(
                    [str(self.binary_dir / "afs-node"), "--config", str(self.root / "etc/node-c.toml")],
                    stdout=log,
                    stderr=log,
                    start_new_session=True,
                )
                self.processes[name] = proc
                deadline = time.monotonic() + 30
                last: object = None
                while time.monotonic() < deadline:
                    if proc.poll() is not None:
                        raise AssertionError(("node-c early exit", proc.returncode))
                    try:
                        conn = http.client.HTTPConnection(NODE_C_IP, owner.NODE_REST, timeout=1)
                        conn.request("GET", "/health")
                        response = conn.getresponse()
                        ready, last = owner.health_ready(response.status, response.read().decode("utf-8", errors="replace"))
                        conn.close()
                        if ready:
                            self.check("node-c healthy", True, {"pid": proc.pid, "health": last})
                            return
                    except OSError as exc:
                        last = repr(exc)
                    time.sleep(0.1)
                raise AssertionError(("node-c not healthy", last))

        return Run(repo.resolve(), binary_dir.resolve(), work_root.resolve(), False)

    def prepare(self) -> None:
        owner = self.owner
        run = self.run
        run.preflight()
        run.check("/dev/fuse available", Path("/dev/fuse").exists())
        for port in [owner.NODE_GRPC, owner.NODE_REST]:
            with owner.socket.socket() as sock:
                sock.bind((NODE_C_IP, port))
        run.write_identity()
        run.generate_tls()
        self._generate_node_c_tls()
        run.write_configs()
        self._rewrite_configs()

    def _generate_node_c_tls(self) -> None:
        run = self.run
        tls = run.root / "tls"
        run.command(["openssl", "genrsa", "-out", tls / "node-c-key.pem", "2048"], check_name="generate node-c key")
        run.command(["openssl", "req", "-new", "-key", tls / "node-c-key.pem", "-subj", "/CN=node-c", "-out", tls / "node-c.csr"], check_name="generate node-c csr")
        (tls / "node-c.ext").write_text(f"subjectAltName=DNS:afs-meta,DNS:node-c,IP:{NODE_C_IP}\nextendedKeyUsage=serverAuth,clientAuth\n", encoding="utf-8")
        run.command(
            [
                "openssl",
                "x509",
                "-req",
                "-in",
                tls / "node-c.csr",
                "-CA",
                tls / "ca.pem",
                "-CAkey",
                tls / "ca-key.pem",
                "-CAcreateserial",
                "-out",
                tls / "node-c.pem",
                "-days",
                "3650",
                "-sha256",
                "-extfile",
                tls / "node-c.ext",
            ],
            check_name="sign node-c certificate",
        )
        (tls / "node-c.csr").unlink()
        (tls / "node-c.ext").unlink()
        os.chmod(tls / "node-c-key.pem", 0o600)
        os.chmod(tls / "node-c.pem", 0o644)
        run.command(["openssl", "verify", "-CAfile", tls / "ca.pem", tls / "node-c.pem"], check_name="verify node-c certificate")

    def _rewrite_configs(self) -> None:
        owner = self.owner
        run = self.run
        tls = run.root / "tls"
        for name in ["meta", "node-a", "node-b"]:
            cfg = run.root / "etc" / f"{name}.toml"
            text = cfg.read_text(encoding="utf-8")
            text = text.replace("trusted_node_certs = {", f'trusted_node_certs = {{ node-c = "{tls / "node-c.pem"}",')
            if name != "meta":
                text = text.replace('fs = "ownerfs"', 'fs = "dfs"')
                text = "\n".join(line for line in text.splitlines() if not line.startswith("ownerfs_mount =")) + "\n"
                (run.root / name / "mount/dfs").mkdir(parents=True)
                text += f'dfs_mount = "{run.root / name / "mount/dfs"}"\n'
            cfg.write_text(text, encoding="utf-8")
        node_b = (run.root / "etc/node-b.toml").read_text(encoding="utf-8")
        trusted = next(line for line in node_b.splitlines() if line.startswith("trusted_node_certs ="))
        node_c = node_b.replace("node-b", "node-c").replace(owner.NODE_B_IP, NODE_C_IP)
        node_c = rewrite_trusted_line(node_c, trusted)
        for rel in ["state", "run", "mount/dfs"]:
            (run.root / "node-c" / rel).mkdir(parents=True, exist_ok=True)
        (run.root / "etc/node-c.toml").write_text(node_c, encoding="utf-8")
        for name in ["meta", "node-a", "node-b", "node-c"]:
            role = "meta" if name == "meta" else "node"
            run.command([run.binary_dir / f"afs-{role}", "--config", run.root / "etc" / f"{name}.toml", "--print-config"], check_name=f"print-config {name}")

    def run_scenario(self) -> None:
        run = self.run
        owner = self.owner
        try:
            for name in ["meta", "node-a", "node-b", "node-c"]:
                run.start(name)
            roots = [run.root / name / "mount/dfs" for name in ["node-a", "node-b", "node-c"]]
            for root in roots:
                mount = json.loads(run.command(["findmnt", "-J", "--mountpoint", root, "-o", "TARGET,SOURCE,FSTYPE"], check_name=f"find DFS mount {root}"))["filesystems"][0]
                run.check(f"actual DFS mount {root}", mount["source"] == "afs-dfs" and mount["fstype"].startswith("fuse"), mount)
            writer = roots[0] / "migration-manyread.bin"
            for generation in range(GENERATIONS):
                payload = bytes([(generation + 1) * 37]) * PAYLOAD_SIZE
                with writer.open("wb") as handle:
                    handle.write(payload)
                    handle.flush()
                    os.fsync(handle.fileno())
                descriptor = os.open(roots[0], os.O_RDONLY | os.O_DIRECTORY)
                try:
                    os.fsync(descriptor)
                finally:
                    os.close(descriptor)
                expected = hashlib.sha256(payload).hexdigest()
                with concurrent.futures.ThreadPoolExecutor(max_workers=2) as pool:
                    results = list(pool.map(lambda root: read_with_oracle(root / writer.name, expected), roots[1:]))
                for name, result in zip(["node-b", "node-c"], results):
                    run.check(f"generation {generation} reader {name}", result.returncode == 0, {"returncode": result.returncode, "stderr": result.stderr, "sha256": expected})
            writer.unlink()
            descriptor = os.open(roots[0], os.O_RDONLY | os.O_DIRECTORY)
            try:
                os.fsync(descriptor)
            finally:
                os.close(descriptor)
            for root in roots[1:]:
                run.check(f"remote deletion visible {root}", not (root / writer.name).exists())
            for name in ["node-c", "node-b", "node-a", "meta"]:
                run.stop(name)
            run.check("normal unmount no owned mounts", not any(str(run.root) in line for line in Path("/proc/self/mountinfo").read_text(encoding="utf-8").splitlines()))
            run._write_json(
                "result.json",
                {
                    "status": "PASS",
                    "scope": "one Linux VM, local-file Meta, three distinct DFS Node processes over mTLS TCP; one writer A, concurrent readers B+C, three 64KiB generations, fsync+close-to-open, deletion and normal stop; one required durable copy; no performance, 3FS parity, three-copy or cross-host claim",
                    "checks": run.checks,
                    "waits": run.receipts,
                },
            )
        finally:
            nodes_stopped = True
            for name in ["node-c", "node-b", "node-a"]:
                if name in run.processes:
                    try:
                        run.stop(name)
                    except Exception as exc:
                        nodes_stopped = False
                        print(f"retained owned process {name}: {exc!r}", file=sys.stderr, flush=True)
            if nodes_stopped and "meta" in run.processes:
                run.stop("meta")
            for log in run.logs:
                log.close()


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--repo", type=Path, required=True, help="target repository root")
    parser.add_argument("--binary-dir", type=Path, required=True, help="directory containing afs-meta and afs-node")
    parser.add_argument("--work-root", type=Path, required=True, help="fresh absolute work/evidence directory on Linux ext4")
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    owner = load_owner_helpers(args.repo.resolve())
    run = DfsCoreRun(owner, args.repo, args.binary_dir, args.work_root)
    run.prepare()
    run.run_scenario()
    print(run.run.root / "result.json")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
