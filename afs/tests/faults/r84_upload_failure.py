from collections import Counter
import errno
import json
import os
from pathlib import Path
import shutil
import signal
import subprocess
import sys
import time

root = Path(os.environ["ROOT"])
out = Path(os.environ["OUT"])
fault = os.environ["R84_FAULT"]

def event(**fields):
    print(json.dumps(dict(t=time.monotonic(), wall=time.time(), **fields)), flush=True)

def call(path, name, fn, *args):
    started = time.monotonic()
    event(file=path.name, op=name, phase="start")
    try:
        value, error = fn(*args), None
    except OSError as exc:
        value, error = None, exc.errno
    event(file=path.name, op=name, phase="end", seconds=time.monotonic() - started,
          errno=error, value=value)
    return error


def worker(prefix, count, first_delay):
    for index in range(count):
        path = root / "mnt" / f"{prefix}-{index}"
        event(file=path.name, op="open", phase="start")
        try:
            fd = os.open(path, os.O_CREAT | os.O_RDWR, 0o600)
        except OSError as exc:
            event(file=path.name, op="open", phase="end", errno=exc.errno, seconds=0)
            continue
        event(file=path.name, op="open", phase="end", errno=None, seconds=0)
        if index == 0:
            time.sleep(first_delay)
        payload = os.urandom(4 << 20)
        call(path, "write", os.write, fd, payload)
        call(path, "fsync", os.fsync, fd)
        if prefix == "new-writer" and index == 0:
            marker = root / "fault-cleared"
            deadline = time.monotonic() + 80
            while not marker.exists() and time.monotonic() < deadline:
                time.sleep(0.1)
            retry_deadline = time.monotonic() + 20
            while time.monotonic() < retry_deadline:
                if call(path, "retry-fsync", os.fsync, fd) is None:
                    call(path, "close", os.close, fd)
                    event(file=path.name, op="readback", match=path.read_bytes() == payload)
                    break
                time.sleep(0.5)
            else:
                call(path, "close", os.close, fd)
        else:
            call(path, "close", os.close, fd)
        time.sleep(1.5)

if len(sys.argv) > 1:
    worker(sys.argv[1], int(sys.argv[2]), float(sys.argv[3]))
    sys.exit(0)

def run_tool(name, *args):
    tool = shutil.which(name)
    if tool is None:
        raise RuntimeError(f"R84 requires {name}")
    subprocess.run([tool, *args], check=True)


def start(prefix, count, first_delay=0):
    with (out / f"{prefix}.jsonl").open("w") as stream:
        return subprocess.Popen([sys.executable, __file__, prefix, str(count), str(first_delay)], stdout=stream,
                                stderr=subprocess.STDOUT)

chunk_dir = root / "node/state/dfs/staging"
peer = int((root / "node-b.pid").read_text()) if fault == "strict" else None
writer = start("writer", 60)
probe = start("new-writer", 15, 40 if peer is not None else 35)
mounted = False
try:
    time.sleep(6)
    if peer is not None:
        os.kill(peer, signal.SIGKILL)
    else:
        run_tool("mount", "-t", "tmpfs", "-o", "size=1M", "tmpfs", chunk_dir)
        mounted = True
        run_tool("dd", "if=/dev/zero", f"of={chunk_dir}/fill", "bs=1M", "count=1")
        run_tool("df", "-B1", chunk_dir)
    fault_started = time.time()
    event(fault=fault, phase="start")
    time.sleep(34 if peer else 29)
    time.sleep(36)
    fault_ended = time.time()
    event(fault=fault, phase="end", writer_exit=writer.poll(), new_writer_exit=probe.poll())
finally:
    if mounted:
        run_tool("umount", chunk_dir)
    elif peer is not None:
        run_tool("fusermount3", "-u", root / "mnt-b")
        # Leave the original peer down. The replacement has its own identity
        # and storage and must be selected under current Meta placement.
        (root / "node-b.pid").unlink()
        original = (root / "node-b.toml").read_text()
        config = []
        for line in original.splitlines():
            if not line.startswith("trusted_node_certs"):
                line = line.replace("node-b", "node-c").replace("mnt-b", "mnt-c")
                line = line.replace("7510", "7520").replace("7511", "7521")
            config.append(line)
        (root / "node-c.toml").write_text("\n".join(config) + "\n")
        with (out / "node-c.log").open("w") as log:
            replacement = subprocess.Popen([str(Path(os.environ["BIN"]) / "afs-node"),
                                            "--config", "node-c.toml"], stdout=log, stderr=log)
        (root / "node-c.pid").write_text(str(replacement.pid))
    (root / "fault-cleared").touch()
    time.sleep(20)
    event(phase="post_recovery", writer_exit=writer.poll(),
          new_writer_exit=probe.poll() if probe else None)
    for child in (writer, probe):
        if child and child.poll() is None:
            child.kill()
        if child:
            child.wait(timeout=10)

def load_events(path):
    records = []
    with path.open() as stream:
        for record_line in stream:
            if record_line.endswith("\n"):
                records.append(json.loads(record_line))
    return records


def registered_peer_failure():
    """B remains live at Meta but cannot persist. C is an authorized spare."""
    with (out / "node-b.log").open("a") as peer_log:
        live_peer = subprocess.Popen(
            [str(Path(os.environ["BIN"]) / "afs-node"), "--config", "node-b.toml"],
            stdout=peer_log, stderr=peer_log,
        )
    (root / "node-b.pid").write_text(str(live_peer.pid))
    mount_deadline = time.monotonic() + 20
    while not (root / "mnt-b").is_mount():
        if live_peer.poll() is not None or time.monotonic() >= mount_deadline:
            raise RuntimeError("replacement test peer did not mount")
        time.sleep(0.1)
    staging = root / "node-b/state/dfs/staging"
    previous_events = len(load_events(out / "node.log"))
    run_tool("mount", "-t", "tmpfs", "-o", "size=1M", "tmpfs", staging)
    try:
        run_tool("dd", "if=/dev/zero", f"of={staging}/fill", "bs=1M", "count=1")
        for index in range(32):
            path = root / "mnt" / f"live-peer-failure-{index}"
            payload = os.urandom(128 << 10)
            started = time.monotonic()
            with path.open("wb", buffering=0) as stream:
                assert stream.write(payload) == len(payload)
                os.fsync(stream.fileno())
            assert time.monotonic() - started <= 32
            assert path.read_bytes() == payload
            assert live_peer.poll() is None, "faulty peer exited"
            entries = load_events(out / "node.log")[previous_events:]
            if any(item.get("msg") == "dfs.replica_write_replaced"
                   and item.get("failed_node") == "node-b"
                   and item.get("replacement_node") == "node-c" for item in entries):
                event(op="registered-peer-replacement", passed=True, attempts=index + 1)
                return True
        return False
    finally:
        run_tool("umount", staging)


registered_replacement = fault != "strict" or registered_peer_failure()

events = load_events(out / "new-writer.jsonl")
first = [item for item in events if item["file"] == "new-writer-0"]
writes = [item for item in first if item["op"] == "write" and item["phase"] == "end"]
syncs = [item for item in first if item["op"] == "fsync" and item["phase"] == "end"]
expected = {errno.ENOSPC, errno.ETIMEDOUT} if fault == "diskfull" else {
    errno.EHOSTUNREACH, errno.ETIMEDOUT,
}
counts = Counter()
with (out / "node.log").open() as log:
    for line in log:
        if not line.endswith("\n"):
            continue
        record = json.loads(line)
        if record.get("level") == "ERRO":
            counts[(record.get("op"), record.get("errno"), record.get("msg"))] += 1
        elif record.get("msg") == "dfs.background_error_recorded":
            counts[("background", None, record["error"])] += 1
background = sum(count for key, count in counts.items() if key[0] == "background")
recovery = [item for item in load_events(out / "writer.jsonl") + events
            if item["op"] == "fsync" and item["phase"] == "end"
            and item["wall"] > fault_ended and item["errno"] is None
            and item["file"] != "new-writer-0"]
checks = {
    "write_completed_during_fault": len(writes) == 1 and fault_started <= writes[0]["wall"] < fault_ended
        and writes[0]["value"] == 4 << 20,
    "fsync_returned_expected_error_within_deadline": len(syncs) == 1
        and syncs[0]["errno"] in expected and syncs[0]["wall"] < fault_ended and syncs[0]["seconds"] <= 32,
    "background_errors_bounded": 0 < background <= 120,
    "new_file_synced_after_recovery": bool(recovery),
    "retained_file_synced_and_read_back": any(
        item.get("op") == "retry-fsync" and item.get("phase") == "end" and item["errno"] is None
        for item in first
    ) and any(item.get("op") == "readback" and item["match"] for item in first),
    "registered_faulty_peer_replaced": registered_replacement,
    "alternate_peer_received_replica": fault != "strict" or any(
        path.is_file() and path.stat().st_size > 0
        for path in (root / "node-c/state/dfs/chunks").iterdir()
    ),
}
event(checks=checks, log_counts=[(*key, count) for key, count in counts.items()])
if not all(checks.values()):
    sys.exit(1)
