#!/usr/bin/env python3
"""Real shared filesystem/S3 + FC traffic, durable references, reclamation and matched timings."""

from concurrent.futures import ThreadPoolExecutor
from contextlib import nullcontext
import json
import logging
import os
from pathlib import Path, PurePosixPath
import re
import signal
import ssl
import subprocess
import sys
import threading
import time
from urllib.error import HTTPError
from urllib.parse import urlsplit, urlunsplit
from urllib.request import HTTPSHandler, ProxyHandler, Request, build_opener
import uuid

from storage_contract import CASES, source_invalidated, summarize
from storage_fixture import public as public_storage
from physical_restore import restores as physical_restores, serialized_restores
from storage_faults import ReadinessBarrier, TrafficFailure, TrafficFailures
from host_tools import executable
from storage_workload import GUEST, identity as workload_identity, verify_checksum
from filesystem_profile import file_layouts

root = Path(sys.argv[1])
image = sys.argv[2]
base = Path(os.environ["ADX_FC_BASE"])
evidence = root / "evidence"
os.environ["SSL_CERT_FILE"] = str(root / "secrets/tls/ca.pem")
from adx_environment import (
    CheckpointStorage,
    ConnectionConfig,
    Environment,
    Mount,
    PausePolicy,
    RetentionPolicy,
)

connection = ConnectionConfig(
    server_address="127.0.0.1:8443",
    token=(root / "secrets/api-key").read_text().strip(),
    use_tls=True,
    verify_tls=True,
)
redis_env = {**os.environ, "REDISCLI_AUTH": (root / "secrets/redis-key").read_text().strip()}
storage = public_storage(root)
mode = storage["kind"]
result = {
    "status": "failed",
    "cases": [],
    "samples": [],
    "storage": storage,
    "cleanup_errors": [],
    "repetitions": int(os.getenv("ADX_STORAGE_REPETITIONS", "3")),
}
result["diagnostic_only"] = os.getenv("ADX_STORAGE_TRACE_RESTORE") == "1"
result["meta_profile_enabled"] = os.getenv("AFS_META_PROFILE") == "1"
result["filesystem_profile_enabled"] = os.getenv("ADX_STORAGE_FILESYSTEM_TRACE") == "1"
result["benchmark_only"] = os.getenv("ADX_STORAGE_BENCHMARK") == "1"
environments = []
stopped = set()
tracers = []
traffic_failures = TrafficFailures(evidence)

result["workload"] = workload_identity(os.environ)


def redis(*args):
    raw = subprocess.check_output(
        [str(base / "tools/redis-cli"), "--json", *args], env=redis_env, text=True, timeout=10
    )
    return json.loads(raw)


def catalog():
    return {
        key: json.loads(value)
        for key, value in redis("HGETALL", "adx:{acceptance}:control:v1").items()
    }


def record(environment):
    return catalog().get("environment:" + environment.id)


def holds():
    return [
        json.loads(value) for value in redis("HVALS", "adx:{acceptance}:control:v1:input-holds")
    ]


def wait(predicate, seconds=240):
    until = time.monotonic() + seconds
    while time.monotonic() < until:
        value = predicate()
        if value:
            return value
        time.sleep(0.25)
    raise TimeoutError("storage fixture condition timed out")


def save(name, value):
    (evidence / (name + ".json")).write_text(json.dumps(value, indent=2))


def passed(index, **details):
    result["cases"].append({"name": CASES[index], "passed": True, **details})
    save("sdk-storage", result)
    logging.info("PASS %s %s", CASES[index], json.dumps(details))


def pid(folder, role):
    status = json.loads(
        subprocess.check_output(
            [
                str(base / "package/bin/adxctl"),
                "status",
                "--config",
                str(folder / "deployment.yaml"),
            ],
            text=True,
        )
    )
    return next(item["pid"] for item in status["services"] if item["role"] == role)


def inventory(node):
    raw = subprocess.check_output(
        ["/opt/adx-fc/bin/sbox", "-a", str(root / node / "sandboxd/sandboxd.sock"), "list"],
        text=True,
        timeout=10,
    )
    return raw.strip().splitlines()[1:]


def command(environment, text):
    answer = environment.commands.run(text)
    if answer.exit_code:
        raise AssertionError(("guest command failed", answer.stderr))
    return answer.stdout.strip()


def create():
    environment = Environment(
        image=image,
        runtime="firecracker",
        cpu=1000,
        memory=result["workload"]["memory_mib"],
        storage_mb=32,
        idle_timeout=0,
        env={"PYTHONDONTWRITEBYTECODE": "1"},
        pause_policy=PausePolicy(
            retention=RetentionPolicy.forever(),
            timeout_seconds=1800,
            storage=CheckpointStorage.SHARED,
        ),
        port_forwardings=[8088],
        connection=connection,
        create_timeout=180,
        schedule_affinities=[
            {
                "kind": 0,
                "affinity": 0,
                "weight": 1000,
                "labelOps": [{"type": 0, "labelKey": "NODE_ID", "labelValues": ["node1"]}],
            }
        ],
    )
    environments.append(environment)
    if record(environment)["assignment"]["node_id"] != "node1":
        raise AssertionError("initial node preference failed")
    environment.files.write("/tmp/storage-server.py", GUEST)
    environment.files.write("/tmp/storage-file", environment.id)
    command(
        environment,
        "nohup python3 /tmp/storage-server.py >/tmp/storage-server.log 2>&1 </dev/null &",
    )
    command(
        environment,
        "for i in $(seq 1 100); do test ! -f /tmp/storage-ready || break; sleep .1; done; cat /tmp/storage-ready",
    )
    return environment


def request(environment, replica=0, touch=False, dirty=False):
    nonce = uuid.uuid4().hex
    parts = urlsplit(environment.get_port_url(8088))
    url = urlunsplit(
        (
            parts.scheme,
            parts.hostname + (":8443" if replica == 0 else ":9443"),
            parts.path,
            "nonce=" + nonce + ("&touch=1" if touch else "") + ("&dirty=1" if dirty else ""),
            "",
        )
    )
    opener = build_opener(
        ProxyHandler({}),
        HTTPSHandler(context=ssl.create_default_context(cafile=str(root / "secrets/tls/ca.pem"))),
    )
    started = time.monotonic()
    deadline = started + 1800
    retries = []
    auth_seconds = 0.0
    http_seconds = 0.0
    while True:
        try:
            auth_started = time.monotonic()
            headers = {**environment.get_port_auth_headers(), "x-adx-request-id": nonce}
            auth_seconds += time.monotonic() - auth_started
            http_started = time.monotonic()
            try:
                with opener.open(Request(url, headers=headers), timeout=330) as reply:
                    answer = json.loads(reply.read())
            finally:
                http_seconds += time.monotonic() - http_started
            break
        except HTTPError as error:
            body = error.read().decode()
            if error.code != 503 or "ENV_RESUMING" not in body or time.monotonic() >= deadline:

                def probe():
                    current = record(environment)
                    node = current["assignment"]["node_id"]
                    namespace = json.loads((evidence / "topology.json").read_text())["namespaces"][
                        node
                    ]
                    if not re.fullmatch(r"axt-[a-f0-9]+-node[12]", namespace):
                        raise ValueError("unexpected fixture namespace")
                    backend = physical_restores(
                        (evidence / (node + "-sandboxd-service.log")).read_text(), environment.id
                    )
                    output = {
                        "runtime": current["result"]["runtime"],
                        "assignment": current["assignment"],
                    }
                    if backend:
                        backend_id = backend[-1]["backend"]
                        args = [
                            "/opt/adx-fc/bin/sbox",
                            "-a",
                            str(root / node / "sandboxd/sandboxd.sock"),
                            "exec",
                            "--containers-root",
                            str(root / node / "sandboxd/root/containers"),
                            backend_id,
                            "sh",
                            "-c",
                            "ip -4 addr; ip route; cat /proc/net/tcp; cat /proc/net/netstat; "
                            "cat /tmp/storage-server.log; ps -ef",
                        ]
                        try:
                            reply = subprocess.run(args, capture_output=True, text=True, timeout=15)
                            output["backend_guest"] = dict(
                                exit_code=reply.returncode, stdout=reply.stdout, stderr=reply.stderr
                            )
                        except Exception as backend_error:
                            output["backend_probe_error"] = repr(backend_error)
                    reply = subprocess.run(
                        [
                            executable("ip"),
                            "netns",
                            "exec",
                            namespace,
                            "sh",
                            "-c",
                            "ip -4 addr; ip route; ip neigh; iptables -t nat -S",
                        ],
                        capture_output=True,
                        text=True,
                        timeout=10,
                    )
                    output["node_network"] = dict(
                        exit_code=reply.returncode, stdout=reply.stdout, stderr=reply.stderr
                    )
                    try:
                        answer = environment.commands.run(
                            "cat /tmp/storage-server.log; cat /proc/net/tcp; cat /proc/meminfo; ps -ef",
                            timeout=10,
                        )
                        output["sdk_guest"] = dict(
                            stdout=answer.stdout, stderr=answer.stderr, exit_code=answer.exit_code
                        )
                    except Exception as sdk_error:
                        output["sdk_probe_error"] = repr(sdk_error)
                    return output

                traffic_failures.capture(
                    TrafficFailure(
                        environment.id, nonce, replica, error.code, body, time.monotonic() - started
                    ),
                    probe,
                )
                raise
            retries.append(body)
            time.sleep(0.25)
    if answer["nonce"] != nonce or answer["count"] != 1 or answer["file"] != environment.id:
        raise AssertionError(("invalid or duplicated HTTP execution", answer))
    elapsed = time.monotonic() - started
    if touch:
        verify_checksum(answer, bool(answer["dirty_bytes"]))
    answer.update(
        seconds=elapsed,
        replica=replica,
        retries=retries,
        client_auth_seconds=auth_seconds,
        client_http_seconds=http_seconds,
    )
    return answer


def continuity(before, after):
    if before["pid"] != after["pid"] or after["seen"].get(before["nonce"]) != 1:
        raise AssertionError(("guest execution continuity lost", before, after))


def restore_count(environment):
    count = 0
    for node in ("node1", "node2"):
        text = (evidence / (node + "-sandboxd-service.log")).read_text()
        count += len(physical_restores(text, environment.id))
    return count


def afs_metrics(phase):
    if mode == "juicefs":
        from filesystem_metrics import juicefs_read_bytes

        values = {}
        opener = build_opener(ProxyHandler({}))
        for role, port in json.loads(os.environ["ADX_FC_JUICEFS_METRICS_PORTS"]).items():
            with opener.open(f"http://127.0.0.1:{port}/metrics", timeout=10) as reply:
                text = reply.read().decode()
            (evidence / f"juicefs-{phase}-{role}.prom").write_text(text)
            values[role] = juicefs_read_bytes(text)
        return values
    if mode != "afs":
        return None
    values = {}
    opener = build_opener(ProxyHandler({}))
    for name, address in [
        ("node-a", "127.0.0.2"),
        ("node-b", "127.0.0.3"),
        ("node-c", "127.0.0.4"),
    ]:
        with opener.open(
            f"http://{address}:{os.environ['ADX_FC_AFS_METRICS_PORT']}/metrics", timeout=10
        ) as reply:
            text = reply.read().decode()
        (evidence / f"afs-{phase}-{name}.prom").write_text(text)
        read_bytes = 0.0
        for line in text.splitlines():
            if (
                line.startswith("afs_dfs_payload_bytes_total{")
                and 'direction="send"' in line
                and 'operation="read"' in line
            ):
                read_bytes += float(line.split()[-1])
        values[name] = read_bytes
    return values


def memory_mappings(environment, phase):
    """Record resident memory and mapped file paths without reading their contents."""
    target = record(environment)["assignment"]["node_id"]
    text = (evidence / (target + "-sandboxd-service.log")).read_text()
    matches = physical_restores(text, environment.id)
    if not matches:
        raise AssertionError("cannot identify restored VMM for memory proof")
    process = matches[-1]["pid"]
    smaps = Path(f"/proc/{process}/smaps").read_text()
    (evidence / f"{phase}-vmm-smaps.txt").write_text(smaps)
    mappings = []
    current = None
    for line in smaps.splitlines():
        if re.match(r"^[0-9a-f]+-[0-9a-f]+ ", line):
            current = {"mapping": line}
            mappings.append(current)
        elif current is not None and line.split(":", 1)[0] in (
            "Size",
            "Rss",
            "Shared_Dirty",
            "Private_Dirty",
        ):
            current[line.split(":", 1)[0] + "_kib"] = int(line.split()[1])
    return {"pid": process, "mappings": [m for m in mappings if m.get("Size_kib", 0) >= 32768]}


def artifact_present(artifact):
    if mode in ("afs", "juicefs"):
        from filesystem_fixture import storage_config

        config = storage_config(os.environ.get("ADX_FC_AFS_ROOT", ""), "coordinator", root.name)
        path = Path(config["mount_root"]) / config["prefix"]
        return (path / "artifacts" / artifact["location"]).exists() or (
            path / "manifests" / (artifact["location"] + ".json")
        ).exists()
    from s3_client import Client

    client = Client(root)
    try:
        client.request(
            "HEAD",
            str(
                PurePosixPath("/")
                / client.config["bucket"]
                / client.config["prefix"]
                / artifact["location"]
                / "manifest.json"
            ),
        )
        return True
    except HTTPError as error:
        if error.code == 404:
            return False
        raise


def suspend(environment):
    source = record(environment)["assignment"]["node_id"]
    manager = pid(root / source, "adxlet")
    os.kill(manager, signal.SIGSTOP)
    stopped.add(manager)
    wait(lambda: source_invalidated(record(environment), source))
    return source, manager


def return_source(source, manager):
    os.kill(manager, signal.SIGCONT)
    stopped.remove(manager)
    wait(lambda: catalog()["node:" + source]["session"]["routable"] and not inventory(source))


def delete(environment):
    environment.kill()
    wait(lambda: record(environment) is None)
    environments.remove(environment)
    environment.close()


def sample(cache, index):
    environment = create()
    baseline = request(environment, touch=True)
    afs_metrics(f"{cache}-{index}-before-capture")
    started = time.monotonic()
    environment.pause(retention=RetentionPolicy.forever(), timeout_seconds=1800)
    pause_seconds = time.monotonic() - started
    afs_metrics(f"{cache}-{index}-after-capture")
    before = record(environment)
    artifact = before["result"]["checkpoint"]["artifact"]
    if not artifact_present(artifact):
        raise AssertionError("durable artifact unavailable through reclaimer client")
    if mode in ("afs", "juicefs"):
        from filesystem_fixture import storage_config

        config = storage_config(os.environ.get("ADX_FC_AFS_ROOT", ""), "node1", root.name)
        source = Path(config["mount_root"]) / config["prefix"] / "artifacts" / artifact["location"]
        files = [
            (str(p.relative_to(source)), p.stat().st_size) for p in source.rglob("*") if p.is_file()
        ]
        save(
            f"{cache}-{index}-capture",
            {
                "record": before,
                "path": str(source),
                "files": files,
                "local_checkpoint_dir_exists": (root / "node1/checkpoints").exists(),
            },
        )
        file_layouts(evidence, f"{cache}-{index}-capture", {"artifact": source})
    manager = None
    if cache == "cold":
        source, manager = suspend(environment)
        dormant = record(environment)
        if dormant["result"]["state"] != "Paused" or dormant.get("resume") or inventory("node2"):
            raise AssertionError("node loss woke a dormant Environment before demand")
        save(f"{cache}-{index}-dormant", dormant)
    metrics_before = afs_metrics(f"{cache}-{index}-before")
    restore_trace = None
    trace_sample = result["diagnostic_only"] and mode == "afs"
    if trace_sample and cache == "cold" and index == 0:
        from filesystem_fixture import storage_config

        config = storage_config(os.environ.get("ADX_FC_AFS_ROOT", ""), "node2", root.name)
        memory = (
            Path(config["mount_root"])
            / config["prefix"]
            / "artifacts"
            / artifact["location"]
            / "memory"
        )
        cow_log = Path(os.environ.get("ADX_FC_AFS_ROOT", "")) / "node-b-0.log"
        cow_log_offset = cow_log.stat().st_size
        backend_pid = json.loads((root / "backend-pids.json").read_text())["node2"]
        with (evidence / "memory-source-strace.log").open("w") as trace_log:
            restore_trace = subprocess.Popen(
                [
                    executable("strace"),
                    "-ff",
                    "-yy",
                    "-s",
                    "0",
                    "-p",
                    str(backend_pid),
                    "-e",
                    "trace=read,pread64,copy_file_range,ioctl,lseek,mmap",
                    "-P",
                    str(memory),
                    "-o",
                    str(evidence / "memory-source.trace"),
                ],
                stdout=subprocess.DEVNULL,
                stderr=trace_log,
            )
        tracers.append(restore_trace)
        time.sleep(0.5)
        if restore_trace.poll() is not None:
            raise AssertionError("cannot trace checkpoint memory source")
    restore_started_ns = time.time_ns()
    with ThreadPoolExecutor(max_workers=12) as executor:
        barrier = threading.Barrier(13)

        def wake(i):
            barrier.wait(15)
            return request(environment, replica=i % 2)

        futures = [executor.submit(wake, i) for i in range(12)]
        barrier.wait(15)
        replies = [future.result(2100) for future in futures]
    metrics_first = afs_metrics(f"{cache}-{index}-first")
    first_finished_ns = time.time_ns()
    mapped_first = memory_mappings(environment, f"{cache}-{index}-first")
    for reply in replies:
        continuity(baseline, reply)
    if restore_trace:
        restore_trace.terminate()
        restore_trace.wait(10)
        tracers.remove(restore_trace)
        source_read_bytes = 0
        copy_bytes = 0
        clone_attempts = []
        for path in evidence.glob("memory-source.trace*"):
            for line in path.read_text().splitlines():
                value = re.search(r"\)\s+=\s+(\d+)\s*$", line)
                operation = re.match(
                    r"^(?:(read|pread64|copy_file_range)\(|<\.\.\. (read|pread64|copy_file_range) resumed>)",
                    line,
                )
                name = (operation[1] or operation[2]) if operation else None
                if value and name in ("read", "pread64"):
                    source_read_bytes += int(value[1])
                if value and name == "copy_file_range":
                    copy_bytes += int(value[1])
                if "FICLONE" in line:
                    clone_attempts.append(line)
        cow_events = []
        for line in cow_log.read_bytes()[cow_log_offset:].decode().splitlines():
            try:
                event = json.loads(line)
            except ValueError:
                continue
            if event.get("msg") != "dfs.cow_range_committed":
                continue
            if (
                int(event.get("length", 0)) == memory.stat().st_size
                and int(event.get("source_offset", -1)) == 0
                and int(event.get("destination_offset", -1)) == 0
            ):
                cow_events.append(event)
        cow_confirmed = (
            len(cow_events) == 1 and source_read_bytes == 0 and copy_bytes == memory.stat().st_size
        )
        result["restore_trace"] = {
            "source_path": str(memory),
            "source_bytes": memory.stat().st_size,
            "read_bytes": source_read_bytes,
            "copy_file_range_bytes": copy_bytes,
            "clone_attempts": clone_attempts,
            "metadata_cow_events": cow_events,
            "metadata_cow_confirmed": cow_confirmed,
            "full_source_read_before_first_touch": source_read_bytes >= memory.stat().st_size
            or (copy_bytes >= memory.stat().st_size and not cow_confirmed),
        }
        save("restore-trace-summary", result["restore_trace"])
    touched = request(environment, touch=True)
    checksum_finished_ns = time.time_ns()
    continuity(baseline, touched)
    metrics_touch = afs_metrics(f"{cache}-{index}-touch")
    if restore_trace:
        late_bytes = sum(metrics_touch.values()) - sum(metrics_first.values())
        result["restore_trace"]["late_touch_remote_read_bytes"] = late_bytes
        save("restore-trace-summary", result["restore_trace"])
        if not result["restore_trace"]["metadata_cow_confirmed"] or late_bytes <= 0:
            raise AssertionError(
                ("AFS metadata COW and deferred remote reads are unproven", result["restore_trace"])
            )
    mapped_touch = memory_mappings(environment, f"{cache}-{index}-touch")
    after = record(environment)
    if mode in ("afs", "juicefs"):
        config = storage_config(
            os.environ.get("ADX_FC_AFS_ROOT", ""), after["assignment"]["node_id"], root.name
        )
        file_layouts(
            evidence,
            f"{cache}-{index}-restored",
            {
                "runtime": Path(config["mount_root"])
                / "runtime"
                / root.name
                / after["assignment"]["node_id"]
            },
        )
    target = "node2" if cache == "cold" else "node1"
    if after["assignment"]["node_id"] != target:
        raise AssertionError(("unexpected resume node", after["assignment"]))
    restores = restore_count(environment)
    if restores != 1 or len(inventory(target)) != 1:
        raise AssertionError(
            ("concurrent traffic did not share one physical restore", restores, inventory(target))
        )
    row = {
        "mode": mode,
        "cache": cache,
        "index": index,
        "environment_id": environment.id,
        "pause_seconds": pause_seconds,
        "first_http_seconds": min(r["seconds"] for r in replies),
        "restore_started_unix_ns": restore_started_ns,
        "first_finished_unix_ns": first_finished_ns,
        "checksum_finished_unix_ns": checksum_finished_ns,
        "all_http_seconds": max(r["seconds"] for r in replies),
        "touch_seconds": touched["seconds"],
        "artifact_bytes": artifact["size_bytes"],
        "request_count": 12,
        "restore_count": restores,
        "continuity": True,
        "full_memory_verified": True,
        "checked_bytes": touched["checked_bytes"],
        "sha256": touched["sha256"],
        "source": "node1",
        "target": target,
        "remote_read_bytes": {
            "before": metrics_before,
            "first": metrics_first,
            "touch": metrics_touch,
        },
        "afs_read_payload": {
            "before": metrics_before,
            "first": metrics_first,
            "touch": metrics_touch,
        }
        if mode == "afs"
        else None,
        "memory_access": {"first": mapped_first, "touch": mapped_touch},
        "assignment_before": before["assignment"],
        "assignment_after": after["assignment"],
        "replies": replies,
    }
    result["samples"].append(row)
    save("sdk-storage", result)
    logging.info("SAMPLE %s", json.dumps({k: v for k, v in row.items() if k != "replies"}))
    if manager:
        return_source(source, manager)
    delete(environment)
    wait(lambda: not artifact_present(artifact))
    return row


def fault_case():
    environment = create()
    baseline = request(environment)
    environment.pause(retention=RetentionPolicy.forever(), timeout_seconds=1800)
    artifact = record(environment)["result"]["checkpoint"]["artifact"]
    environment.resume()
    continuity(baseline, request(environment))
    before = record(environment)
    target = "node2" if before["assignment"]["node_id"] == "node1" else "node1"
    progress = []

    def recovering():
        snapshot = catalog()
        current = snapshot.get("environment:" + environment.id)
        state = {
            "environment": current,
            "nodes": {
                node: {key: snapshot["node:" + node][key] for key in ("node", "session")}
                for node in ("node1", "node2")
            },
        }
        if not progress or progress[-1]["state"] != state:
            progress.append({"unix_seconds": time.time(), "state": state})
            save("fault-progress", progress)
        if current["assignment"]["node_id"] == target and current["invalidated"]:
            raise AssertionError(
                "healthy recovery target was invalidated during Coordinator restart"
            )
        return (
            current["assignment"]["node_id"] == target and current["result"]["state"] == "Running"
        )

    target_pid = json.loads((root / "backend-pids.json").read_text())[target]
    with (evidence / "recovery-runtime-strace.log").open("w") as trace_log:
        recovery_trace = subprocess.Popen(
            [
                executable("strace"),
                "-ff",
                "-ttt",
                "-p",
                str(target_pid),
                "-e",
                "trace=process,kill",
                "-o",
                str(evidence / "recovery-runtime.trace"),
            ],
            stdout=subprocess.DEVNULL,
            stderr=trace_log,
        )
    tracers.append(recovery_trace)
    time.sleep(0.5)
    if recovery_trace.poll() is not None:
        raise AssertionError("cannot trace recovery VMM lifetime")
    target_log_sizes = {
        path: path.stat().st_size for path in (root / target / "state/logs").glob("*.log")
    }

    def target_drain_heartbeats():
        heartbeats = []
        for path in (root / target / "state/logs").glob("*.log"):
            offset = target_log_sizes.get(path, 0)
            for line in path.read_bytes()[offset:].decode().splitlines():
                if "node_admission_drain_heartbeat" in line:
                    heartbeats.append(line)
        return heartbeats

    barrier = ReadinessBarrier(root, target) if mode == "real_obs" else nullcontext()
    with barrier:
        source, manager = suspend(environment)
        saved = wait(
            lambda: [hold for hold in holds() if hold["assignment"] == before["assignment"]]
        )
        save("fault-input-holds", saved)
        if mode == "real_obs":
            # Observe an actual restored VMM while the accepted lifecycle call
            # is held at Execd readiness. Restart must overlap accepted work.
            wait(
                lambda: record(environment)["assignment"]["node_id"] == target
                and physical_restores(
                    (evidence / (target + "-sandboxd-service.log")).read_text(), environment.id
                )
            )
            blocked = record(environment)
            if (
                not blocked.get("recovery", {}).get("pending")
                or blocked["result"]["state"] == "Running"
            ):
                raise AssertionError("readiness barrier did not hold an admitted restore")
            save("fault-readiness-barrier-before-restart", blocked)
        epoch = catalog()["header"]["epoch"]
        os.kill(pid(root, "coordinator"), signal.SIGKILL)
        wait(lambda: catalog()["header"]["epoch"] > epoch)
        if not artifact_present(artifact) or holds() != saved:
            raise AssertionError("Coordinator restart lost retired execution protection")
        if mode == "real_obs":
            wait(lambda: len(target_drain_heartbeats()) >= 2, seconds=60)
            recovering()  # Enforce target liveness while readiness is blocked.
            save(
                "fault-readiness-barrier-release",
                {
                    "target": target,
                    "epoch_after": catalog()["header"]["epoch"],
                    "drain_heartbeats": target_drain_heartbeats(),
                },
            )
    # Demand may have recovered the failed live execution already; no extra
    # client-side start is issued. Delete its authoritative owner while the old
    # execution still exists, proving the hold alone blocks physical GC.
    wait(recovering)
    continuity(baseline, request(environment))
    identifier = environment.id
    fault_restores = restore_count(environment)
    recovery_trace.terminate()
    recovery_trace.wait(10)
    tracers.remove(recovery_trace)
    process_traces = {
        int(path.suffix[1:]): path.read_text() for path in evidence.glob("recovery-runtime.trace.*")
    }
    target_restores = serialized_restores(
        (evidence / (target + "-sandboxd-service.log")).read_text(), identifier, process_traces
    )
    if fault_restores != len(target_restores) + 1 or len(inventory(target)) != 1:
        raise AssertionError(
            ("recovery execution inventory differs", fault_restores, target_restores)
        )
    recovery_replays = []
    drain_heartbeats = []
    for path in (root / record(environment)["assignment"]["node_id"] / "state/logs").glob("*.log"):
        offset = target_log_sizes.get(path, 0)
        for line in path.read_bytes()[offset:].decode().splitlines():
            if "node_admission_drain_heartbeat" in line:
                drain_heartbeats.append(line)
            try:
                event = json.loads(line)
            except ValueError:
                continue
            if (
                event.get("event") == "checkpoint recovery operation reused"
                and event.get("environment_id") == identifier
            ):
                recovery_replays.append(event)
    if mode == "real_obs" and len(drain_heartbeats) < 2:
        raise AssertionError(
            "OBS Coordinator restart did not exercise heartbeat reporting while draining"
        )
    with (evidence / "coordinator-gc-strace.log").open("w") as trace_log:
        trace = subprocess.Popen(
            [
                executable("strace"),
                "-ff",
                "-p",
                str(pid(root, "coordinator")),
                "-e",
                "trace=unlink,unlinkat,rmdir",
                "-o",
                str(evidence / "coordinator-gc.trace"),
            ],
            stdout=subprocess.DEVNULL,
            stderr=trace_log,
        )
    tracers.append(trace)
    time.sleep(0.5)
    if trace.poll() is not None:
        raise AssertionError("cannot trace Coordinator reclamation")
    delete(environment)
    time.sleep(3)
    if not artifact_present(artifact) or not holds():
        raise AssertionError("live retired execution input was reclaimed")
    passed(
        2,
        environment_id=identifier,
        hold_persisted=True,
        epoch_before=epoch,
        source_node=source,
        target_node=target,
        epoch_after=catalog()["header"]["epoch"],
        protected_after_environment_delete=True,
        artifact=artifact,
        recovery_progress_replays=recovery_replays,
        physical_restores=fault_restores,
        drain_heartbeats=drain_heartbeats,
        healthy_target_not_invalidated=True,
        target_vmm_lifetimes=target_restores,
        replacements_nonoverlapping=True,
    )
    return_source(source, manager)
    wait(lambda: not holds())
    wait(lambda: not artifact_present(artifact))
    trace.terminate()
    trace.wait(10)
    tracers.remove(trace)
    deleted_paths = []
    for path in evidence.glob("coordinator-gc.trace*"):
        for line in path.read_text().splitlines():
            if artifact["location"] in line and "= 0" in line:
                deleted_paths.append(line)
    if mode in ("afs", "juicefs") and not deleted_paths:
        raise AssertionError("no successful Coordinator filesystem deletion syscall")
    passed(
        3,
        source_reconciled=True,
        old_backend_removed=not inventory(source),
        coordinator_mount=mode in ("afs", "juicefs"),
        coordinator_delete_syscalls=deleted_paths,
        reclaimed=True,
        artifact=artifact,
    )


def repeated_gc_cycles():
    from storage_gc import GcProbe, bootstrap_only, plateau, reclaimed

    probe = GcProbe(
        os.environ.get("ADX_FC_AFS_ROOT", ""), os.environ["ADX_FC_AFS_META_PORT"], evidence
    )
    # Wait for prior VM/capture data and leases; retain the linked store-id.
    baseline = probe.settle("baseline", bootstrap_only)
    environment = create()
    baseline_reply = request(environment, touch=True)
    rounds = []
    artifacts = []
    for index in range(6):
        restores_before = restore_count(environment)
        started = time.monotonic()
        environment.pause(retention=RetentionPolicy.forever(), timeout_seconds=1800)
        artifact = record(environment)["result"]["checkpoint"]["artifact"]
        artifacts.append(artifact)
        with ThreadPoolExecutor(max_workers=12) as executor:
            barrier = threading.Barrier(13)

            def wake(replica, rendezvous=barrier):
                rendezvous.wait(15)
                return request(environment, replica=replica % 2, touch=True)

            futures = [executor.submit(wake, replica) for replica in range(12)]
            barrier.wait(15)
            replies = [future.result(2100) for future in futures]
        for reply in replies:
            continuity(baseline_reply, reply)
        if restore_count(environment) - restores_before != 1:
            raise AssertionError("repeated wake created duplicate physical restores")
        rounds.append(
            {
                "cycle": index + 1,
                "seconds": time.monotonic() - started,
                "artifact": artifact,
                "requests": 12,
                "restores": 1,
            }
        )
        probe.capture("cycle-" + str(index + 1))
        if index in (2, 5):
            # All superseded anonymous artifacts must be removed by Coordinator.
            wait(lambda: all(not artifact_present(old) for old in artifacts[:-1]))
            time.sleep(75)  # native grant/reference grace plus the 30s test result window
            settled = probe.settle(
                "settled-" + str(index + 1), lambda row: True if index == 2 else plateau(first, row)
            )
            if index == 2:
                first = settled
            else:
                second = settled
    touched = request(environment, touch=True)
    continuity(baseline_reply, touched)
    delete(environment)
    wait(lambda: all(not artifact_present(old) for old in artifacts))
    final = probe.settle("deleted", lambda row: reclaimed(baseline, row))
    result["gc"] = {
        "status": "passed",
        "cycles": rounds,
        "baseline": baseline,
        "first_batch": first,
        "second_batch": second,
        "final": final,
        "same_environment": True,
        "requests_per_cycle": 12,
        "ingress_replicas": 2,
        "protected_live_cow": True,
    }
    save("sdk-storage", result)


def abandoned_capture():
    """Seed only an unowned, stopped writer reservation; production Coordinator deletes it."""
    location = str(uuid.uuid4())
    artifact = {"storage": storage["alias"], "location": location, "size_bytes": 0}
    if mode in ("afs", "juicefs"):
        from filesystem_fixture import storage_config

        config = storage_config(os.environ.get("ADX_FC_AFS_ROOT", ""), "node1", root.name)
        path = Path(config["mount_root"]) / config["prefix"] / "artifacts" / location
        path.mkdir()
        with (path / "partial-memory").open("wb") as file:
            file.write(b"stopped-incomplete-capture")
            file.flush()
            os.fsync(file.fileno())

        def present():
            return artifact_present(artifact)
    else:
        from s3_client import Client

        client = Client(root)
        key = str(
            PurePosixPath("/")
            / client.config["bucket"]
            / client.config["prefix"]
            / location
            / "partial-memory"
        )
        client.request("PUT", key, b"stopped-incomplete-capture")

        def present():
            try:
                client.request("HEAD", key)
                return True
            except HTTPError as error:
                if error.code == 404:
                    return False
                raise

    if not present():
        raise AssertionError("partial capture was not created")
    redis(
        "HSET",
        "adx:{acceptance}:control:v1:artifacts",
        artifact["storage"] + ":" + location,
        json.dumps(artifact),
    )
    wait(lambda: not present())
    wait(
        lambda: not redis(
            "HEXISTS", "adx:{acceptance}:control:v1:artifacts", artifact["storage"] + ":" + location
        )
    )
    passed(
        5,
        injection="stopped incomplete writer plus durable unowned inventory entry",
        artifact=artifact,
        coordinator_reclaimed=True,
    )


def shared_mount_case():
    if mode not in ("afs", "juicefs"):
        return
    from filesystem_fixture import storage_config

    mounts = {
        role: Path(
            storage_config(os.environ.get("ADX_FC_AFS_ROOT", ""), role, root.name)["mount_root"]
        )
        for role in ("node1", "node2", "coordinator")
    }
    shared_name = root.name + "-workspace"
    shared = mounts["coordinator"] / "shares" / shared_name
    shared.mkdir(parents=True)

    def write_shared(value):
        with (shared / "state").open("w") as file:
            file.write(value)
            file.flush()
            os.fsync(file.fileno())

    # The Firecracker fixture exercises read-only host paths. The writer
    # is external to the sleeping Environment, as it is for live code/model data.
    write_shared("before")
    peers = []
    for node in ("node1", "node2"):
        environment = Environment(
            image=image,
            runtime="firecracker",
            cpu=1000,
            memory=256,
            storage_mb=32,
            idle_timeout=0,
            connection=connection,
            create_timeout=180,
            pause_policy=PausePolicy(
                retention=RetentionPolicy.forever(),
                timeout_seconds=1800,
                storage=CheckpointStorage.SHARED,
            ),
            mounts=[
                Mount(
                    target="/shared-workspace",
                    host_path=str(mounts[node] / "shares" / shared_name),
                    options=("ro",),
                )
            ],
            schedule_affinities=[
                {
                    "kind": 0,
                    "affinity": 0,
                    "weight": 1000,
                    "labelOps": [{"type": 0, "labelKey": "NODE_ID", "labelValues": [node]}],
                }
            ],
        )
        environments.append(environment)
        peers.append(environment)
        if record(environment)["assignment"]["node_id"] != node:
            raise AssertionError("shared mount node placement failed")
        if environment.files.read("/shared-workspace/state") != "before":
            raise AssertionError("shared mount did not observe external writer")
        readonly = environment.commands.run("printf forbidden >/shared-workspace/state")
        if readonly.exit_code == 0 or (shared / "state").read_text() != "before":
            raise AssertionError("read-only shared mount allowed a write")
    consumer, other = peers
    consumer.files.write("/tmp/private-state", "private-before")
    consumer.pause(retention=RetentionPolicy.forever(), timeout_seconds=1800)
    write_shared("after")
    if (
        consumer.files.read("/shared-workspace/state") != "after"
        or consumer.files.read("/tmp/private-state") != "private-before"
    ):
        raise AssertionError("restore rolled back business shares or lost private state")
    if other.files.read("/shared-workspace/state") != "after":
        raise AssertionError("peer did not observe external writer")
    for environment in peers:
        delete(environment)
    if (shared / "state").read_text() != "after":
        raise AssertionError("Environment deletion removed business shares")
    result["shared_mounts"] = {
        "status": "passed",
        "writer": "external filesystem client",
        "ro_readers": ["node1", "node2"],
        "private_state_restored": True,
        "shared_state_not_rolled_back": True,
        "preserved_after_environment_delete": True,
    }
    save("shared-mounts", result["shared_mounts"])
    (shared / "state").unlink()
    shared.rmdir()


def main():
    try:
        for cache in ("cold", "warm"):
            for index in range(result["repetitions"]):
                sample(cache, index)
        passed(0, storage=mode, direct_path=mode in ("afs", "juicefs"), durable=True)
        passed(
            1,
            requests_per_sample=12,
            ingress_replicas=2,
            physical_restore_per_sample=1,
            continuity=True,
        )
        if not result["benchmark_only"]:
            fault_case()
            passed(4, anonymous_artifacts_collected_after_delete=True)
            abandoned_capture()
            shared_mount_case()
            if mode == "afs" and os.getenv("ADX_STORAGE_GC") == "1":
                repeated_gc_cycles()
        result["performance"] = (
            {"excluded": "diagnostic or individual fresh-client trial"}
            if result["diagnostic_only"]
            or result["filesystem_profile_enabled"]
            or result["repetitions"] < 3
            else summarize(result["samples"], result["repetitions"])
        )
        result["status"] = "passed"
    except BaseException as error:
        result["error"] = repr(error)
        logging.error("FAIL %r", error)
        raise
    finally:
        for trace in tracers:
            trace.terminate()
            trace.wait(10)
        for manager in list(stopped):
            try:
                os.kill(manager, signal.SIGCONT)
            except ProcessLookupError:
                pass
        for environment in list(environments):
            try:
                delete(environment)
            except Exception as error:
                result["cleanup_errors"].append(repr(error))
        if result["cleanup_errors"]:
            result["status"] = "failed"
        save("sdk-storage", result)


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO, format="%(message)s")
    main()
