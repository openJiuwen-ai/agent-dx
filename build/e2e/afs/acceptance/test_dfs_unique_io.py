"""Linux qualification tests for the fixed dfs_unique_io C probe."""
from __future__ import annotations

import hashlib
import json
import os
import platform
import shutil
import subprocess
import tempfile
import unittest
from pathlib import Path


DATASET = "counter-1m-v1"
DATA_BYTES = 64 * 1024 * 1024
BLOCK_BYTES = 1024 * 1024
OPERATIONS = DATA_BYTES // BLOCK_BYTES
PATTERN_BYTE = 0x61
CHUNK_BYTES = 4 * 1024 * 1024


def expected_block(index: int, generation: int = 0) -> bytes:
    block = bytearray([PATTERN_BYTE]) * BLOCK_BYTES
    block[:8] = ((generation << 32) | index).to_bytes(8, "little")
    return bytes(block)


def expected_payload_sha256() -> str:
    digest = hashlib.sha256()
    for index in range(OPERATIONS):
        digest.update(expected_block(index))
    return digest.hexdigest()


EXPECTED_SHA256 = expected_payload_sha256()


def compile_probe(root: Path) -> Path:
    source = Path(__file__).parent / "probes" / "dfs_unique_io.c"
    binary = root / "dfs_unique_io"
    command = ["cc", "-std=c11", "-O2", "-Wall", "-Wextra", "-Werror", str(source), "-o", str(binary)]
    subprocess.run(command, check=True, text=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
    return binary


def parse_success(completed: subprocess.CompletedProcess[str]) -> dict[str, object]:
    if completed.returncode != 0:
        raise AssertionError(f"probe failed rc={completed.returncode} stderr={completed.stderr!r}")
    if completed.stderr:
        raise AssertionError(f"successful probe wrote stderr: {completed.stderr!r}")
    lines = completed.stdout.splitlines()
    if len(lines) != 1:
        raise AssertionError(f"expected one JSON stdout line, got {len(lines)}")
    return json.loads(lines[0])


def validate_io_result(record: dict[str, object], operation: str, barrier: str) -> None:
    expected: dict[str, object] = {
        "dataset": DATASET,
        "operation": operation,
        "file_bytes": DATA_BYTES,
        "io_bytes": DATA_BYTES,
        "block_bytes": BLOCK_BYTES,
        "concurrency": 1,
        "pattern_byte": PATTERN_BYTE,
        "operations": OPERATIONS,
        "barrier": barrier,
        "cache_requested": "unobserved",
        "residency_observed": False,
        "content_ok": True,
    }
    for key, value in expected.items():
        if record.get(key) != value:
            raise AssertionError(f"unexpected {key}: {record.get(key)!r}")
    for key in ("wall_ns", "client_cpu_ns", "barrier_ns"):
        if not isinstance(record.get(key), int) or record[key] <= 0:
            raise AssertionError(f"expected positive integer {key}: {record.get(key)!r}")


@unittest.skipUnless(platform.system() == "Linux", "dfs_unique_io qualification is Linux-only")
class DfsUniqueIoQualificationTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls) -> None:
        if shutil.which("cc") is None:
            raise unittest.SkipTest("cc is required")
        cls.tempdir = tempfile.TemporaryDirectory(prefix="dfs-unique-io-", dir="/var/tmp")
        cls.root = Path(cls.tempdir.name)
        cls.probe = compile_probe(cls.root)

    @classmethod
    def tearDownClass(cls) -> None:
        cls.tempdir.cleanup()

    def run_probe(self, *args: str) -> subprocess.CompletedProcess[str]:
        return subprocess.run([str(self.probe), *args], text=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE)

    def test_write_and_read_emit_compatible_shape_and_expected_unique_content(self) -> None:
        payload = self.root / "payload.bin"
        write_record = parse_success(self.run_probe("write", str(payload)))
        validate_io_result(write_record, "seq-write", "fdatasync")
        read_record = parse_success(self.run_probe("read", str(payload)))
        validate_io_result(read_record, "seq-read", "close")

        digest = hashlib.sha256()
        chunk_digests: list[str] = []
        with payload.open("rb") as handle:
            for chunk_index in range(DATA_BYTES // CHUNK_BYTES):
                chunk = handle.read(CHUNK_BYTES)
                self.assertEqual(len(chunk), CHUNK_BYTES)
                digest.update(chunk)
                chunk_digests.append(hashlib.sha256(chunk).hexdigest())
                for block_in_chunk in range(CHUNK_BYTES // BLOCK_BYTES):
                    block_index = chunk_index * (CHUNK_BYTES // BLOCK_BYTES) + block_in_chunk
                    begin = block_in_chunk * BLOCK_BYTES
                    self.assertEqual(chunk[begin : begin + 16], expected_block(block_index)[:16])
            self.assertEqual(handle.read(1), b"")
        self.assertEqual(digest.hexdigest(), EXPECTED_SHA256)
        self.assertEqual(len(set(chunk_digests)), 16)

    def test_single_byte_corruption_fails_read(self) -> None:
        payload = self.root / "corrupt.bin"
        parse_success(self.run_probe("write", str(payload)))
        with payload.open("r+b") as handle:
            handle.seek(BLOCK_BYTES + 128)
            handle.write(b"Z")
            handle.flush()
            os.fsync(handle.fileno())
        failed = self.run_probe("read", str(payload))
        self.assertNotEqual(failed.returncode, 0)
        self.assertEqual(failed.stdout, "")

    def test_complete_read_intervals_are_measured_separately_from_oracle(self) -> None:
        payload = self.root / 'read-intervals.bin'
        write = parse_success(self.run_probe('write', str(payload)))
        self.assertNotIn('read_timing', write)
        read = parse_success(self.run_probe('read', str(payload)))
        validate_io_result(read, 'seq-read', 'close')
        timing = read['read_timing']
        self.assertEqual(timing['schema'], 'complete-read-v1')
        self.assertEqual(timing['clock'], 'CLOCK_MONOTONIC')
        self.assertEqual(timing['boundary'], 'full_read_1MiB_excluding_content_oracle')
        self.assertEqual(timing['task_end_ns'] - timing['task_begin_ns'], read['wall_ns'])
        self.assertEqual(len(timing['reads']), OPERATIONS)
        spans = [timing['open'], timing['fstat'], *timing['reads'], timing['eof'], timing['close']]
        previous = timing['task_begin_ns']
        for start, end in spans:
            self.assertGreaterEqual(start, previous)
            self.assertGreater(end, start)
            self.assertLessEqual(end, timing['task_end_ns'])
            previous = end
        self.assertEqual(timing['close'][1] - timing['close'][0], read['barrier_ns'])
        # Real C output must also satisfy the maintained receiver contract.
        import dfs_r3_small
        dfs_r3_small.validate_sample(read, 'read')

    def test_appended_eof_byte_fails_read(self) -> None:
        payload = self.root / "appended.bin"
        parse_success(self.run_probe("write", str(payload)))
        with payload.open("ab") as handle:
            handle.write(b"x")
            handle.flush()
            os.fsync(handle.fileno())
        failed = self.run_probe("read", str(payload))
        self.assertNotEqual(failed.returncode, 0)
        self.assertEqual(failed.stdout, "")

    def test_write_existing_fails_without_overwrite(self) -> None:
        payload = self.root / "existing.bin"
        payload.write_bytes(b"sentinel")
        failed = self.run_probe("write", str(payload))
        self.assertNotEqual(failed.returncode, 0)
        self.assertEqual(failed.stdout, "")
        self.assertEqual(payload.read_bytes(), b"sentinel")

    def test_invalid_cli_fails(self) -> None:
        cases = [
            (),
            ("write", "relative/path"),
            ("bad-op", str(self.root / "target")),
        ]
        for args in cases:
            with self.subTest(args=args):
                failed = self.run_probe(*args)
                self.assertNotEqual(failed.returncode, 0)
                self.assertEqual(failed.stdout, "")

    def test_six_generations_have_distinct_physical_contents_and_correct_readback(self) -> None:
        all_chunks = []
        for generation in range(1, 7):
            payload = self.root / f'generation-{generation}.bin'
            record = parse_success(self.run_probe('write', str(payload), str(generation)))
            self.assertEqual(record['dataset'], 'counter-generation-1m-v1')
            self.assertEqual(record['generation'], generation)
            self.assertEqual(record['barrier'], 'fdatasync')
            self.assertEqual(record['io_bytes'], DATA_BYTES)
            read = parse_success(self.run_probe('read', str(payload), str(generation)))
            self.assertEqual(read['generation'], generation)
            with payload.open('rb') as stream:
                for chunk_index in range(16):
                    chunk = stream.read(CHUNK_BYTES)
                    expected = b''.join(expected_block(i, generation)
                                        for i in range(chunk_index * 4, chunk_index * 4 + 4))
                    self.assertEqual(chunk, expected)
                    all_chunks.append(hashlib.sha256(chunk).hexdigest())
                self.assertEqual(stream.read(1), b'')
        self.assertEqual(len(set(all_chunks)), 96)

    def test_generation_mismatch_and_corruption_reject_without_success_json(self) -> None:
        payload = self.root / 'generation-invalid-read.bin'
        parse_success(self.run_probe('write', str(payload), '1'))
        for args in (('read', str(payload)), ('read', str(payload), '2')):
            failed = self.run_probe(*args)
            self.assertNotEqual(failed.returncode, 0)
            self.assertEqual(failed.stdout, '')
        with payload.open('r+b') as stream:
            stream.seek(BLOCK_BYTES + 512)
            stream.write(b'Z')
            stream.flush()
            os.fsync(stream.fileno())
        failed = self.run_probe('read', str(payload), '1')
        self.assertNotEqual(failed.returncode, 0)
        self.assertEqual(failed.stdout, '')

    def test_invalid_generation_fails_before_file_creation(self) -> None:
        for generation in ('', '0', '7', '-1', '+1', '01', '1x', ' 1', 'true'):
            payload = self.root / 'generation-invalid-cli.bin'
            failed = self.run_probe('write', str(payload), generation)
            self.assertNotEqual(failed.returncode, 0)
            self.assertEqual(failed.stdout, '')
            self.assertFalse(payload.exists())


if __name__ == "__main__":
    unittest.main()
