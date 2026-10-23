#!/usr/bin/env python3
"""Bounded 1GiB native CRC E2E: two synchronous copies, repair to three, remote reader.

One Linux VM, four isolated Node processes over authenticated mTLS TCP. This is
functional/durability evidence, not physical failure-domain or throughput proof.
"""
from __future__ import annotations

import argparse
import hashlib
import http.client
import importlib.util
import json
import os
import re
import shutil
import socket
import subprocess
import time
from pathlib import Path

SIZE = 1024**3
BLOCK = 1024**2
NODES = ("node-a", "node-b", "node-c", "node-d")
IPS = dict(zip(NODES, ("127.0.0.2", "127.0.0.3", "127.0.0.4", "127.0.0.5")))


def load(path: Path, name: str):
    spec = importlib.util.spec_from_file_location(name, path)
    if spec is None or spec.loader is None:
        raise RuntimeError(f"cannot load maintained driver: {path}")
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


def http_text(ip: str, port: int, path: str) -> str:
    conn = http.client.HTTPConnection(ip, port, timeout=5)
    try:
        conn.request("GET", path)
        response = conn.getresponse()
        body = response.read().decode()
        if response.status != 200:
            raise RuntimeError((path, response.status, body[:1000]))
        return body
    finally:
        conn.close()


def counter(text: str, marker: str, *required: str) -> float:
    return sum(float(line.rsplit(" ", 1)[1]) for line in text.splitlines()
               if not line.startswith("#") and marker in line and all(token in line for token in required))


def physical(root: Path, name: str) -> dict[str, dict]:
    chunks = root / name / "state/dfs/chunks"
    return {path.name: {"length": path.stat().st_size}
            for path in chunks.iterdir() if path.is_file() and path.name.startswith("crc32c-v2-")}


def add_reader(core) -> None:
    run = core.run
    tls = run.root / "tls"
    for port in (core.owner.NODE_GRPC, core.owner.NODE_REST):
        with socket.socket() as sock:
            sock.bind((IPS["node-d"], port))
    run.command(["openssl", "genrsa", "-out", tls / "node-d-key.pem", "2048"])
    run.command(["openssl", "req", "-new", "-key", tls / "node-d-key.pem", "-subj", "/CN=node-d", "-out", tls / "node-d.csr"])
    (tls / "node-d.ext").write_text("subjectAltName=DNS:afs-meta,DNS:node-d,IP:127.0.0.5\nextendedKeyUsage=serverAuth,clientAuth\n")
    run.command(["openssl", "x509", "-req", "-in", tls / "node-d.csr", "-CA", tls / "ca.pem", "-CAkey", tls / "ca-key.pem",
                 "-CAcreateserial", "-out", tls / "node-d.pem", "-days", "3650", "-sha256", "-extfile", tls / "node-d.ext"])
    os.chmod(tls / "node-d-key.pem", 0o600)
    for suffix in ("csr", "ext"):
        (tls / f"node-d.{suffix}").unlink()
    for name in ("meta", *NODES[:3]):
        cfg = run.root / "etc" / f"{name}.toml"
        text = cfg.read_text().replace("trusted_node_certs = {", f'trusted_node_certs = {{ node-d = "{tls / "node-d.pem"}",')
        cfg.write_text(text)
    original = (run.root / "etc/node-b.toml").read_text()
    trusted = next(line for line in original.splitlines() if line.startswith("trusted_node_certs ="))
    text = original.replace("node-b", "node-d").replace(IPS["node-b"], IPS["node-d"])
    text = "\n".join(trusted if line.startswith("trusted_node_certs =") else line for line in text.splitlines()) + "\n"
    for rel in ("state", "run", "mount/dfs"):
        (run.root / "node-d" / rel).mkdir(parents=True, exist_ok=True)
    (run.root / "etc/node-d.toml").write_text(text)


def start_reader(core) -> None:
    run = core.run
    log = (run.root / "node-d.log").open("ab")
    run.logs.append(log)
    proc = subprocess.Popen([str(run.binary_dir / "afs-node"), "--config", str(run.root / "etc/node-d.toml")],
                            stdout=log, stderr=log, start_new_session=True)
    run.processes["node-d"] = proc
    deadline = time.monotonic() + 30
    while time.monotonic() < deadline:
        if proc.poll() is not None:
            raise RuntimeError(("node-d early exit", proc.returncode, (run.root / "node-d.log").read_text()[-4000:]))
        try:
            ready, detail = core.owner.health_ready(200, http_text(IPS["node-d"], core.owner.NODE_REST, "/health"))
            if ready:
                run.check("late remote reader healthy", True, detail)
                return
        except (OSError, RuntimeError):
            pass
        time.sleep(0.1)
    raise RuntimeError("node-d did not become ready")


def write_gib(path: Path) -> dict:
    body = bytearray(bytes(range(256)) * (BLOCK // 256))
    oracle = hashlib.sha256()
    for index in range(SIZE // BLOCK):
        body[:8] = index.to_bytes(8, "little")
        oracle.update(body)
    fd = os.open(path, os.O_CREAT | os.O_EXCL | os.O_RDWR, 0o600)
    try:
        start = time.monotonic_ns()
        for index in range(SIZE // BLOCK):
            body[:8] = index.to_bytes(8, "little")
            if os.pwrite(fd, body, index * BLOCK) != BLOCK:
                raise RuntimeError("short 1MiB application write")
        written = time.monotonic_ns()
        os.fsync(fd)
        synced = time.monotonic_ns()
        if os.fstat(fd).st_size != SIZE:
            raise RuntimeError("incorrect actual file length")
    finally:
        os.close(fd)
    return {"bytes": SIZE, "writes": SIZE // BLOCK, "sha256": oracle.hexdigest(),
            "write_ms": (written - start) / 1e6, "fullsync_ms": (synced - written) / 1e6,
            "write_fullsync_ms": (synced - start) / 1e6,
            "timing_scope": "all pwrite plus same-fd real fsync; data oracle and readback excluded; not a comparative benchmark"}


def run_case(args) -> dict:
    core_module = load(args.repo / "build/e2e/afs/scripts/dfs/three_node_core.py", "crc_core")
    owner = core_module.load_owner_helpers(args.repo)
    core = core_module.DfsCoreRun(owner, args.repo, args.binary_dir, args.work_root)
    core.prepare()
    run = core.run
    result: dict = {"status": "FAIL", "scope": __doc__, "defaults_required": args.require_defaults}
    try:
        run.check("space for three real copies", shutil.disk_usage(run.root).free >= 5 * SIZE)
        add_reader(core)
        for name in ("meta", *NODES):
            cfg = run.root / "etc" / f"{name}.toml"
            text = cfg.read_text().replace('log_level = "warn"', 'log_level = "info"')
            values = {"dfs_desired_copies": 3, "dfs_sync_required_copies": 2, "dfs_min_distinct_nodes": 2,
                      "dfs_min_distinct_failure_domains": 1, "dfs_background_commit_ms": 60000}
            if not args.require_defaults:
                values.update(dfs_crc32c_writes=True, dfs_optimistic_gc_commit=True)
            for key, value in values.items():
                encoded = str(value).lower()
                if re.search(rf"^{key}\s*=", text, re.M):
                    text = re.sub(rf"^{key}\s*=.*$", f"{key} = {encoded}", text, flags=re.M)
                else:
                    text += f"{key} = {encoded}\n"
            cfg.write_text(text)
            role = "meta" if name == "meta" else "node"
            config = json.loads(run.command([run.binary_dir / f"afs-{role}", "--config", cfg, "--print-config"]))
            if name != "meta":
                run.check(f"CRC and generation protection enabled {name}",
                          config["dfs_crc32c_writes"] is True and config["dfs_optimistic_gc_commit"] is True)
        for name in ("meta", *NODES[:3]):
            run.start(name)
        before = http_text(owner.META_IP, owner.META_REST, "/metrics")
        (run.root / "meta-before.prom").write_text(before)
        result["application"] = write_gib(run.root / "node-a/mount/dfs/crc-general.bin")
        run.write_json("application.json", result["application"])
        after_write = http_text(owner.META_IP, owner.META_REST, "/metrics")
        (run.root / "meta-after-write.prom").write_text(after_write)
        for name in NODES[:3]:
            (run.root / f"{name}-after-write.prom").write_text(http_text(IPS[name], owner.NODE_REST, "/metrics"))
        # M=2 writes initially create two copies. N=3 can only become satisfied
        # through the normal repair worker; do not pause GC or add manual copies.
        deadline = time.monotonic() + 180
        inventories = {}
        while time.monotonic() < deadline:
            inventories = {name: physical(run.root, name) for name in NODES[:3]}
            ids = [set(rows) for rows in inventories.values()]
            completed = sum(line.count("dfs.replication_task_reported") for log in run.root.glob("node-*.log") for line in log.read_text().splitlines() if "Completed" in line)
            if completed >= 256 and all(len(ids_) == 256 for ids_ in ids) and ids[0] == ids[1] == ids[2] \
                    and all(sum(row["length"] for row in rows.values()) == SIZE for rows in inventories.values()):
                break
            time.sleep(0.2)
        else:
            raise RuntimeError(("repair did not produce three complete copies", {name: len(rows) for name, rows in inventories.items()}))
        run.check("normal repair reached three physical 1GiB copies", True, {"copies": {name: len(rows) for name, rows in inventories.items()}, "completed_repair_reports": completed})
        start_reader(core)
        run.check("reader has no durable local replicas", not physical(run.root, "node-d"))
        reader_before = {name: http_text(IPS[name], owner.NODE_REST, "/metrics") for name in NODES}
        path = run.root / "node-d/mount/dfs/crc-general.bin"
        digest = hashlib.sha256()
        count = 0
        with path.open("rb", buffering=0) as handle:
            while data := handle.read(BLOCK):
                digest.update(data)
                count += len(data)
        run.check("remote full 1GiB content", count == SIZE and digest.hexdigest() == result["application"]["sha256"])
        with path.open("rb", buffering=0) as handle:
            for offset, length in ((32759, 31), (SIZE - 17, 17)):
                expected = bytearray(bytes(range(256)) * (BLOCK // 256))
                index = offset // BLOCK
                expected[:8] = index.to_bytes(8, "little")
                run.check(f"remote boundary range {offset}", os.pread(handle.fileno(), length, offset) == expected[offset % BLOCK:offset % BLOCK + length])
        reader_after = {name: http_text(IPS[name], owner.NODE_REST, "/metrics") for name in NODES}
        for name in NODES:
            (run.root / f"{name}-after-read.prom").write_text(reader_after[name])
        sent = sum(counter(reader_after[name], "afs_dfs_payload_bytes_total{", 'operation="read"', 'direction="send"')
                   - counter(reader_before[name], "afs_dfs_payload_bytes_total{", 'operation="read"', 'direction="send"') for name in NODES[:3])
        run.check("remote reader caused real data-plane traffic", sent >= SIZE, {"payload_delta_bytes": sent})
        result["meta_write_metrics_delta"] = {method: counter(after_write, "afs_requests_total{", f'"{method}"') - counter(before, "afs_requests_total{", f'"{method}"')
            for method in ("dfs_gc_capture", "dfs_gc_register", "dfs_commit_file_version", "dfs_get_xattr")}
        run.check("no per-chunk GC registration or capability Meta calls", result["meta_write_metrics_delta"]["dfs_gc_register"] == 0
                  and result["meta_write_metrics_delta"]["dfs_get_xattr"] == 0, result["meta_write_metrics_delta"])
        final_meta = http_text(owner.META_IP, owner.META_REST, "/metrics")
        (run.root / "meta-final.prom").write_text(final_meta)
        run.check("normal GC ran", counter(final_meta, "afs_requests_total{", '"dfs_gc_collect"') > counter(before, "afs_requests_total{", '"dfs_gc_collect"'))
        for name in reversed(("meta", *NODES)):
            run.stop(name)
        # Compare every physical immutable body after normal process shutdown.
        manifests = {}
        catalogs = {}
        for name in NODES[:3]:
            manifests[name] = {cid: {**row, "sha256": hashlib.sha256((run.root / name / "state/dfs/chunks" / cid).read_bytes()).hexdigest()}
                               for cid, row in physical(run.root, name).items()}
            state = run.root / name / "state/dfs"
            records = {}
            checkpoint = state / "catalog.snapshot"
            if checkpoint.exists():
                records.update((row["chunk"]["id"], row) for row in json.loads(checkpoint.read_text())["records"])
            wal = state / "catalog.wal"
            if wal.exists():
                for line in wal.read_text().splitlines():
                    records.update((row["chunk"]["id"], row) for row in json.loads(line)["records"])
            catalogs[name] = records
            run.check(f"persisted CRC catalog {name}", set(records) == set(manifests[name]) and all(
                row["state"] == "Durable" and row["stored_length"] == 4 * BLOCK
                and row["chunk"]["content_digest"]["algorithm"] == "Crc32cSegmentsV1"
                and row["segment_crc"]["segment_bytes"] == 32768
                and len(row["segment_crc"]["checksums"]) == 128 for row in records.values()))
        run.check("all three physical copies match", manifests["node-a"] == manifests["node-b"] == manifests["node-c"])
        run.check("normal unmount no owned mounts", not any(str(run.root) in line for line in Path("/proc/self/mountinfo").read_text().splitlines()))
        run.write_json("physical.json", manifests)
        run.write_json("catalogs.json", catalogs)
        result["status"] = "PASS"
    except BaseException as error:
        result["error"] = repr(error)
        raise
    finally:
        for name in reversed(NODES):
            if name in run.processes:
                try:
                    run.stop(name)
                except Exception as error:
                    result.setdefault("cleanup_errors", []).append(repr(error))
        if not any(name in run.processes for name in NODES) and "meta" in run.processes:
            try:
                run.stop("meta")
            except Exception as error:
                result.setdefault("cleanup_errors", []).append(repr(error))
        result.update(checks=run.checks, waits=run.receipts, remaining_processes=list(run.processes))
        if result.get("cleanup_errors") or run.processes:
            result["status"] = "FAIL"
        run.write_json("result.json", result)
        for log in run.logs:
            log.close()
    return result


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--repo", type=Path, required=True)
    parser.add_argument("--binary-dir", type=Path, required=True)
    parser.add_argument("--work-root", type=Path, required=True)
    parser.add_argument("--require-defaults", action="store_true")
    args = parser.parse_args()
    args.repo = args.repo.resolve()
    args.binary_dir = args.binary_dir.resolve()
    result = run_case(args)
    if result["status"] != "PASS":
        raise SystemExit(1)
    print(json.dumps({"status": result["status"], "root": str(args.work_root)}))


if __name__ == "__main__":
    main()
