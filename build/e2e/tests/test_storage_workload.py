import hashlib
import copy
from pathlib import Path
import sys
import json
import os
import signal
import socket
import subprocess
import time
import unittest
from tempfile import TemporaryDirectory
from urllib.request import urlopen
from unittest.mock import patch

sys.path.append(str(Path(__file__).resolve().parents[1] / "firecracker"))
import storage_workload as workload


class StorageWorkloadTests(unittest.TestCase):
    @unittest.skipUnless(hasattr(signal, "SIGSTOP"), "requires process suspension")
    def test_wake_burst_fits_listen_queue_while_guest_is_not_scheduled(self):
        # Reproduce the resume boundary: the listener survives, but guest accept
        # has not run yet. All twelve SYNs must fit without a TCP retry timer.
        with TemporaryDirectory() as directory:
            root = Path(directory)
            ready = root / "ready"
            state = root / "state"
            state.write_text("test-environment")
            source = (
                workload.GUEST.replace("512*1024*1024", "2*1024*1024")
                .replace("/tmp/storage-file", str(state))
                .replace("/tmp/storage-ready", str(ready))
                .replace("('0.0.0.0',8088)", "('127.0.0.1',0)")
                .replace("write(str(os.getpid()))", "write(str(server.server_address[1]))")
            )
            process = subprocess.Popen(
                [sys.executable, "-c", source], stdout=subprocess.DEVNULL, stderr=subprocess.PIPE
            )
            sockets = []
            try:
                deadline = time.monotonic() + 10
                while not ready.exists():
                    if process.poll() is not None or time.monotonic() > deadline:
                        raise AssertionError("workload listener did not start")
                    time.sleep(0.02)
                os.kill(process.pid, signal.SIGSTOP)
                for _ in range(12):
                    try:
                        sockets.append(
                            socket.create_connection(("127.0.0.1", int(ready.read_text())), 0.2)
                        )
                    except socket.timeout:
                        pass
                self.assertEqual(
                    len(sockets), 12, "wake burst overflowed the workload listen queue"
                )
            finally:
                for connection in sockets:
                    connection.close()
                os.kill(process.pid, signal.SIGCONT)
                process.terminate()
                process.wait(10)
                process.stderr.close()

    def test_comparison_combines_fresh_trials_and_rejects_corrupt_or_mixed_direct_probes(self):
        from compare_storage import aggregate

        guest = dict(
            payload_bytes=512 * 1024**2, expected_full_sha256="full", expected_dirty_sha256="dirty"
        )
        trials = []
        for index in range(3):
            samples = [
                dict(
                    mode="juicefs",
                    cache=cache,
                    request_count=12,
                    restore_count=1,
                    continuity=True,
                    artifact_bytes=1024,
                    pause_seconds=1,
                    first_http_seconds=index + 1,
                    touch_seconds=2,
                )
                for cache in ("cold", "warm")
            ]
            direct = dict(
                status="passed",
                cleanup_errors=[],
                workload=guest,
                first_response_probe="host HTTP",
                probe_sha256="probe",
                samples=[],
            )
            for flavor, digest in [("Full", "full"), ("Incremental", "dirty")]:
                direct["samples"].append(
                    dict(
                        snapshot_type=flavor,
                        sha256=digest,
                        checked_bytes=512 * 1024**2,
                        continuity=True,
                        pause_seconds=1,
                        rpc_seconds=1,
                        first_response_seconds=index + 1,
                        checksum_seconds=2,
                    )
                )
            trials.append(
                (
                    dict(
                        storage=dict(kind="juicefs", prefix=str(index)),
                        benchmark_only=True,
                        repetitions=1,
                        samples=samples,
                        cases=[],
                        fixture_cleanup=dict(exit_code=0, errors=[]),
                        workload=guest,
                        direct_baseline=direct,
                    ),
                    {},
                )
            )
        result = aggregate(trials)
        self.assertEqual(len(result), 1)
        self.assertEqual(result[0]["summary"]["juicefs/cold"]["first_http_seconds"]["median"], 2)
        self.assertEqual(result[0]["direct_baseline"]["summary"]["Full"]["samples"], 3)
        with self.assertRaisesRegex(ValueError, "three repetitions"):
            aggregate(trials[:2])
        bad = copy.deepcopy(trials)
        bad[1][0]["direct_baseline"]["samples"][1]["sha256"] = "corrupt"
        with self.assertRaisesRegex(ValueError, "full-memory"):
            aggregate(bad)
        bad = copy.deepcopy(trials)
        bad[1][0]["direct_baseline"]["probe_sha256"] = "different"
        with self.assertRaisesRegex(ValueError, "probe identities"):
            aggregate(bad)

    def test_real_http_workload_preserves_full_digest_and_dirty_state_without_rewriting(self):
        with TemporaryDirectory() as directory:
            root = Path(directory)
            ready = root / "ready"
            state = root / "state"
            state.write_text("test-environment")
            source = (
                workload.GUEST.replace("512*1024*1024", "2*1024*1024")
                .replace("64*1024*1024", "1*1024*1024")
                .replace("/tmp/storage-file", str(state))
                .replace("/tmp/storage-ready", str(ready))
                .replace("('0.0.0.0',8088)", "('127.0.0.1',0)")
                .replace("write(str(os.getpid()))", "write(str(server.server_address[1]))")
            )
            process = subprocess.Popen(
                [sys.executable, "-c", source], stdout=subprocess.DEVNULL, stderr=subprocess.PIPE
            )
            try:
                deadline = time.monotonic() + 10
                while not ready.exists():
                    if process.poll() is not None or time.monotonic() > deadline:
                        raise AssertionError("workload HTTP server did not start")
                    time.sleep(0.02)
                url = "http://127.0.0.1:" + ready.read_text() + "/?nonce="

                def get(nonce, suffix=""):
                    with urlopen(url + nonce + suffix, timeout=5) as reply:
                        return json.load(reply)

                first = get("first")
                self.assertIsNone(first["sha256"])
                self.assertEqual(first["checked_bytes"], 0)
                full = get("full", "&touch=1")
                self.assertEqual(
                    full["sha256"],
                    hashlib.sha256(
                        workload.data_block(0) + workload.data_block(1024**2)
                    ).hexdigest(),
                )
                dirty = get("dirty", "&dirty=1&touch=1")
                expected = hashlib.sha256(
                    workload.data_block(0, True) + workload.data_block(1024**2)
                ).hexdigest()
                self.assertEqual(dirty["sha256"], expected)
                after = get("after", "&touch=1")
                self.assertEqual(after["sha256"], expected)
                self.assertEqual(after["dirty_bytes"], 1024**2)
                self.assertEqual(after["pid"], first["pid"])
                self.assertEqual(after["seen"]["first"], 1)
            finally:
                process.terminate()
                process.wait(10)
                process.stderr.close()

    def test_checksum_covers_all_bytes_and_rejects_corruption_outside_dirty_range(self):
        # Small instantiation of the same contract exercises every byte, including
        # a corrupted byte in a page that the old stride-based probe missed.
        with patch.multiple(workload, PAYLOAD_BYTES=8192, DIRTY_BYTES=4096, MIB=4096):
            memory = bytearray(workload.data_block(0) + workload.data_block(4096))
            answer = {
                "checked_bytes": len(memory),
                "dirty_bytes": 0,
                "sha256": hashlib.sha256(memory).hexdigest(),
            }
            workload.verify_checksum(answer)
            memory[-2] ^= 1
            answer["sha256"] = hashlib.sha256(memory).hexdigest()
            with self.assertRaises(AssertionError):
                workload.verify_checksum(answer)
            memory = bytearray(workload.data_block(0, True) + workload.data_block(4096))
            answer.update(sha256=hashlib.sha256(memory).hexdigest(), dirty_bytes=4096)
            workload.verify_checksum(answer, True)
            answer["checked_bytes"] = 4096
            with self.assertRaises(AssertionError):
                workload.verify_checksum(answer, True)

    def test_io_profile_rejects_unmatched_or_unknown_engine(self):
        self.assertEqual(workload.io_engine({}), "Sync")
        self.assertEqual(workload.io_engine({"ADX_STORAGE_IO_ENGINE": "Async"}), "Async")
        for engine in ("AsyncDirect", "", "unknown"):
            with self.assertRaises(ValueError):
                workload.io_engine({"ADX_STORAGE_IO_ENGINE": engine})
        self.assertEqual(workload.checkpoint_mode({}), "full")
        self.assertEqual(
            workload.checkpoint_mode({"ADX_STORAGE_CHECKPOINT_MODE": "incremental"}), "incremental"
        )
        with self.assertRaises(ValueError):
            workload.checkpoint_mode({"ADX_STORAGE_CHECKPOINT_MODE": "unknown"})

    def test_benchmark_cannot_pass_without_full_memory_and_both_ingresses(self):
        from storage_contract import CASES, verify

        result = dict(
            status="passed",
            benchmark_only=True,
            cleanup_errors=[],
            inventories={"node1": 0, "node2": 0},
            repetitions=1,
            cases=[dict(name=name, passed=True) for name in CASES[:2]],
            workload=dict(
                name="deterministic-512m-v1",
                payload_bytes=512 * 1024**2,
                expected_full_sha256="trusted-host-digest",
            ),
            samples=[],
        )
        for cache in ("cold", "warm"):
            result["samples"].append(
                dict(
                    cache=cache,
                    request_count=12,
                    restore_count=1,
                    full_memory_verified=True,
                    continuity=True,
                    checked_bytes=512 * 1024**2,
                    sha256="trusted-host-digest",
                )
            )
        verify(result)
        for field, value in [
            ("checked_bytes", 4096),
            ("sha256", "wrong"),
            ("restore_count", 2),
            ("full_memory_verified", False),
        ]:
            failed = copy.deepcopy(result)
            failed["samples"][0][field] = value
            with self.assertRaisesRegex(ValueError, "proof"):
                verify(failed)
