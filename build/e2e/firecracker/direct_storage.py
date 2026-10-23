#!/usr/bin/env python3
"""Direct sandboxd full/incremental restoration before ADX creates environments."""

import json
import logging
import ipaddress
import hashlib
import os
from pathlib import Path
import shutil
import subprocess
import sys
import time
import uuid
from urllib.request import ProxyHandler, Request, build_opener

from filesystem_fixture import storage_config
from filesystem_metrics import juicefs_read_bytes
from storage_fixture import public as public_storage
from storage_workload import GUEST, identity, verify_checksum
from filesystem_profile import file_layouts


def main():
    root = Path(sys.argv[1])
    image = sys.argv[2]
    evidence = root / "evidence"
    storage = public_storage(root)
    if storage["kind"] not in ("afs", "juicefs", "native_xfs"):
        raise ValueError("direct capture probe requires shared filesystem storage")
    probe = Path(os.environ["ADX_STORAGE_PROBE"])
    tag = "direct-" + uuid.uuid4().hex
    owned = root / tag
    owned.mkdir()
    config = {
        node: storage_config(os.getenv("ADX_FC_AFS_ROOT", ""), node, root.name)
        for node in ("node1", "node2")
    }
    paths = {
        node: Path(value["mount_root"]) / value["prefix"] / tag for node, value in config.items()
    }
    paths["node1"].mkdir(parents=True)
    request_file = owned / "start.json"
    request_file.write_text(
        json.dumps(
            {
                "runtime": "firecracker",
                "rootfs": {"type": "IMAGE", "image_url": image},
                "command": [
                    "python3",
                    "-c",
                    "open('/tmp/storage-file','w').write('direct')\n" + GUEST,
                ],
                "cwd": "/",
                "envs": {"PATH": "/usr/local/bin:/usr/bin:/bin", "PYTHONDONTWRITEBYTECODE": "1"},
                "resources": {"CPU": 1000, "Memory": 1024},
                "network": "sandbox",
                "writable_layer_limit_bytes": str(32 * 1024**2),
            }
        )
    )
    request_file.chmod(0o600)
    active = set()
    addresses = {}
    opener = build_opener(ProxyHandler({}))
    direct_http = os.getenv("ADX_STORAGE_DIRECT_HTTP") == "1"
    result = {
        "status": "failed",
        "workload": identity(os.environ),
        "image": image,
        "samples": [],
        "api": "direct sandboxd RPC; no ADX lifecycle or traffic wake",
        "storage": storage,
        "first_response_probe": (
            "host HTTP directly to SandboxIP; setup alone uses sbox exec"
            if direct_http
            else "sbox exec plus internal HTTP, includes exec control path"
        ),
        "cleanup_errors": [],
    }
    with probe.open("rb") as stream:
        result["probe_sha256"] = hashlib.file_digest(stream, "sha256").hexdigest()

    def afs_metrics(phase):
        if storage["kind"] != "afs":
            return
        port = os.environ["ADX_FC_AFS_METRICS_PORT"]
        for name, address in [
            ("node-a", "127.0.0.2"),
            ("node-b", "127.0.0.3"),
            ("node-c", "127.0.0.4"),
        ]:
            with opener.open(f"http://{address}:{port}/metrics", timeout=10) as reply:
                (evidence / f"direct-{phase}-{name}.prom").write_bytes(reply.read())

    def save():
        (evidence / "direct-storage.json").write_text(json.dumps(result, indent=2))

    def sbox(node, *args):
        return [
            "/opt/adx-fc/bin/sbox",
            "-a",
            str(root / node / "sandboxd/sandboxd.sock"),
            *map(str, args),
        ]

    def call(args):
        return subprocess.check_output(list(map(str, args)), text=True, timeout=1800).strip()

    def rpc(node, action, sandbox, capture=None, snapshot=None):
        args = [
            probe,
            "-socket",
            root / node / "sandboxd/sandboxd.sock",
            "-action",
            action,
            "-timeout",
            "1800s",
            "-request-file",
            request_file,
            "-target-id" if action == "restore" else "-sandbox-id",
            sandbox,
        ]
        if capture:
            args += ["-checkpoint-dir", paths[node] / capture]
        if action == "checkpoint":
            args += [
                "-compress=false",
                "-leave-running=false",
                "-checkpoint-timeout-seconds",
                "1800",
                "-snapshot-type",
                snapshot,
            ]
        response_file = owned / (sandbox + "-response.json")
        if action == "restore" and direct_http:
            args += ["-response-file", response_file]
        started = time.monotonic()
        call(args)
        if action == "restore" and direct_http:
            response = json.loads(response_file.read_text())
            address = ipaddress.IPv4Address(response["sandboxIp"])
            network = ipaddress.IPv4Network(
                "10.231.16.0/20" if node == "node1" else "10.231.32.0/20"
            )
            if response["id"] != sandbox or address not in network:
                raise AssertionError("direct restore returned an unexpected target address")
            addresses[sandbox] = str(address)
        return time.monotonic() - started

    def remove(node, sandbox):
        rpc(node, "delete", sandbox)
        active.discard((node, sandbox))

    def request(node, sandbox, touch=False, dirty=False, expect_dirty=False):
        nonce = uuid.uuid4().hex
        query = "?nonce=" + nonce + ("&touch=1" if touch else "") + ("&dirty=1" if dirty else "")
        command = (
            "import urllib.request; print(urllib.request.urlopen("
            + repr("http://127.0.0.1:8088/" + query)
            + ",timeout=1800).read().decode())"
        )
        started = time.monotonic()
        if sandbox in addresses:
            with opener.open(
                Request("http://" + addresses[sandbox] + ":8088/" + query), timeout=330
            ) as reply:
                answer = json.load(reply)
        else:
            answer = json.loads(
                call(
                    sbox(
                        node,
                        "exec",
                        "--containers-root",
                        root / node / "sandboxd/root/containers",
                        sandbox,
                        "python3",
                        "-c",
                        command,
                    )
                )
            )
        answer["seconds"] = time.monotonic() - started
        if answer["nonce"] != nonce or answer["count"] != 1 or answer["file"] != "direct":
            raise AssertionError("direct guest response identity mismatch")
        if touch:
            verify_checksum(answer, dirty or expect_dirty)
        return answer

    def metrics():
        if storage["kind"] != "juicefs":
            return None
        import urllib.request

        opener = urllib.request.build_opener(urllib.request.ProxyHandler({}))
        readings = {}
        for node, port in json.loads(os.environ["ADX_FC_JUICEFS_METRICS_PORTS"]).items():
            with opener.open(f"http://127.0.0.1:{port}/metrics", timeout=10) as reply:
                readings[node] = juicefs_read_bytes(reply.read().decode())
        return readings

    try:
        source = "sbox-" + tag + "-source"
        # Register cleanup before sending Start: a lost reply must not leak a VM.
        active.add(("node1", source))
        args = sbox(
            "node1",
            "start",
            "-q",
            "--sandbox-id",
            source,
            "--runtime",
            "firecracker",
            "--image-url",
            image,
            "--cpu-millicores",
            "1000",
            "--memory-mb",
            "1024",
            "--storage-mb",
            "32",
            "--env",
            "PYTHONDONTWRITEBYTECODE=1",
            "--env",
            "PATH=/usr/local/bin:/usr/bin:/bin",
            *json.loads(request_file.read_text())["command"],
        )
        call(args)
        until = time.monotonic() + 120
        while True:
            try:
                baseline = request("node1", source, touch=True)
                break
            except subprocess.SubprocessError:
                if time.monotonic() > until:
                    raise
                time.sleep(0.25)
        for index, (source_node, target, flavor, dirty) in enumerate(
            (("node1", "node2", "Full", False), ("node2", "node1", "Incremental", True))
        ):
            capture = "full" if index == 0 else "incremental"
            if dirty:
                changed = request(source_node, source, touch=True, dirty=True)
                if changed["pid"] != baseline["pid"]:
                    raise AssertionError("guest restarted before dirty capture")
            afs_metrics(capture + "-before-capture")
            pause = rpc(source_node, "checkpoint", source, capture, flavor)
            afs_metrics(capture + "-after-capture")
            file_layouts(
                evidence,
                "direct-" + capture + "-capture",
                {"artifact": paths[source_node] / capture},
            )
            remove(source_node, source)
            restored = "sbox-" + tag + "-" + capture
            active.add((target, restored))
            before = metrics()
            start_ns = time.time_ns()
            elapsed = rpc(target, "restore", restored, capture)
            rpc_ns = time.time_ns()
            first = request(target, restored)
            first_ns = time.time_ns()
            after_first = metrics()
            full = request(target, restored, touch=True, expect_dirty=dirty)
            after_full = metrics()
            runtime = Path(config[target]["mount_root"]) / "runtime" / root.name / target
            file_layouts(evidence, "direct-" + capture + "-restored", {"runtime": runtime})
            if (
                first["pid"] != baseline["pid"]
                or full["pid"] != baseline["pid"]
                or full["seen"].get(baseline["nonce"]) != 1
            ):
                raise AssertionError("direct restore lost process or application memory")
            manifest = paths[target] / capture / "manifest.json"
            manifest_value = json.loads(manifest.read_text())
            (evidence / ("direct-" + capture + "-manifest.json")).write_text(
                json.dumps(manifest_value, indent=2)
            )
            if (
                manifest_value["snapshot_type"] != flavor
                or manifest_value["memory_size"] != 1024 * 1024**2
            ):
                raise AssertionError(
                    "runtime did not produce the requested full/incremental snapshot"
                )
            row = {
                "source": source_node,
                "target": target,
                "snapshot_type": flavor,
                "pause_seconds": pause,
                "rpc_seconds": elapsed,
                "first_response_seconds": (first_ns - start_ns) / 1e9,
                "checksum_seconds": full["seconds"],
                "checked_bytes": full["checked_bytes"],
                "sha256": full["sha256"],
                "pid": full["pid"],
                "continuity": True,
                "started_unix_ns": start_ns,
                "rpc_finished_unix_ns": rpc_ns,
                "first_finished_unix_ns": first_ns,
                "checksum_finished_unix_ns": time.time_ns(),
                "get_bytes": {"before": before, "first": after_first, "checksum": after_full},
                "manifest": manifest_value,
            }
            result["samples"].append(row)
            save()
            logging.info("DIRECT SAMPLE %s", json.dumps(row))
            source = restored
        remove("node1", source)
        result["status"] = "passed"
    except BaseException as error:
        result["error"] = repr(error)
        raise
    finally:
        for node, sandbox in sorted(active):
            try:
                remove(node, sandbox)
            except Exception as error:
                result["cleanup_errors"].append(repr(error))
        if not result["cleanup_errors"]:
            try:
                shutil.rmtree(paths["node1"])
            except Exception as error:
                result["cleanup_errors"].append("artifact cleanup: " + repr(error))
        if result["cleanup_errors"]:
            result["status"] = "failed"
        save()
    return 0 if result["status"] == "passed" else 1


if __name__ == "__main__":
    sys.exit(main())
