"""Real syscall assertions, invoked inside a fixture-owned Linux worker."""

import hashlib
import json
import os
from pathlib import Path
import sys
import threading
import time

MIB = 1024**2


def verify(path, digest, size):
    actual = hashlib.sha256()
    count = 0
    with path.open("rb", buffering=0) as stream:
        for data in iter(lambda: stream.read(MIB), b""):
            actual.update(data)
            count += len(data)
    assert count == size and actual.hexdigest() == digest, (count, size, actual.hexdigest(), digest)
    return {"bytes": count, "sha256": actual.hexdigest()}


def order(root):
    path = root / "cross-handle"
    first = os.open(path, os.O_CREAT | os.O_TRUNC | os.O_RDWR, 0o600)
    second = os.open(path, os.O_RDWR)
    try:
        expected = bytearray(b"a" * (12 * MIB))
        assert os.pwrite(first, expected, 0) == len(expected)
        expected[MIB - 4096 : MIB + 4096] = b"b" * 8192
        assert os.pwrite(second, b"b" * 8192, MIB - 4096) == 8192
        assert os.pread(first, len(expected), 0) == expected
        os.ftruncate(second, 6 * MIB)
        os.ftruncate(first, 10 * MIB)
        expected = expected[: 6 * MIB] + b"\0" * (4 * MIB)
        expected[8 * MIB : 9 * MIB] = b"c" * MIB
        assert os.pwrite(second, b"c" * MIB, 8 * MIB) == MIB
        os.fsync(first)
        assert os.fstat(second).st_size == len(expected)
        assert os.pread(second, len(expected), 0) == expected
    finally:
        os.close(second)
        os.close(first)
    return {"cross-handle": verify(path, hashlib.sha256(expected).hexdigest(), len(expected))}


def barriers(root):
    results = {}
    for name, flags in [("sync", os.O_SYNC), ("dsync", os.O_DSYNC), ("close-only", 0)]:
        path = root / name
        data = bytes([len(name)]) * MIB
        fd = os.open(path, os.O_CREAT | os.O_TRUNC | os.O_RDWR | flags, 0o600)
        try:
            assert os.write(fd, data) == len(data)
        finally:
            os.close(fd)
        # Reopen after close; no extra fsync may repair the tested boundary.
        results[name] = verify(path, hashlib.sha256(data).hexdigest(), len(data))
    path = root / "append"
    fd = os.open(path, os.O_CREAT | os.O_TRUNC | os.O_RDWR | os.O_APPEND, 0o600)
    errors = []

    def append(value):
        try:
            for _ in range(32):
                assert os.write(fd, bytes([value]) * 4096) == 4096
        except BaseException as exc:
            errors.append(repr(exc))

    threads = [threading.Thread(target=append, args=(value,), daemon=True) for value in range(1, 5)]
    try:
        for thread in threads:
            thread.start()
        for thread in threads:
            thread.join(60)
        assert not errors and not any(t.is_alive() for t in threads), errors
        os.fsync(fd)
    finally:
        os.close(fd)
    body = path.read_bytes()
    records = [body[n : n + 4096] for n in range(0, len(body), 4096)]
    assert len(records) == 128 and all(len(set(record)) == 1 for record in records)
    assert all(sum(record[0] == value for record in records) == 32 for value in range(1, 5))
    results["append"] = {"bytes": len(body), "sha256": hashlib.sha256(body).hexdigest(), "records": 128}
    return results


def bounded(root):
    """256MiB through a 64MiB profile; test progress, not a fairness or latency SLA."""
    path, independent = root / "bounded", root / "independent"
    total = 256 * MIB
    fd = os.open(path, os.O_CREAT | os.O_TRUNC | os.O_RDWR, 0o600)
    other = os.open(path, os.O_RDWR)
    peer = os.open(independent, os.O_CREAT | os.O_TRUNC | os.O_RDWR, 0o600)
    assert os.write(peer, b"i" * 65536) == 65536
    os.fsync(peer)
    progress = {"bytes": 0}
    errors = []
    prefix = threading.Event()
    observations = {}

    def writer():
        try:
            for offset in range(0, total, MIB):
                assert os.pwrite(fd, b"p" * MIB, offset) == MIB
                progress["bytes"] += MIB
                if progress["bytes"] >= 96 * MIB:
                    prefix.set()
        except BaseException as exc:
            errors.append(repr(exc))
            prefix.set()

    def sync(handle, label):
        try:
            assert prefix.wait(120), "writer did not advance to the required prefix"
            observations[label + "_begin_bytes"] = progress["bytes"]
            os.fsync(handle)
            observations[label + "_end_bytes"] = progress["bytes"]
        except BaseException as exc:
            errors.append(repr(exc))

    threads = [
        threading.Thread(target=writer, daemon=True),
        threading.Thread(target=sync, args=(other, "same_file"), daemon=True),
        threading.Thread(target=sync, args=(peer, "other_file"), daemon=True),
    ]
    try:
        for thread in threads:
            thread.start()
        deadline = time.monotonic() + 180
        for thread in threads:
            thread.join(max(0, deadline - time.monotonic()))
        assert not errors and not any(t.is_alive() for t in threads), errors
        assert progress["bytes"] == total
        os.fsync(fd)
    finally:
        os.close(other)
        os.close(fd)
        os.close(peer)
    digest = hashlib.sha256()
    for _ in range(total // MIB):
        digest.update(b"p" * MIB)
    return {
        "bounded": verify(path, digest.hexdigest(), total),
        "independent": verify(independent, hashlib.sha256(b"i" * 65536).hexdigest(), 65536),
        "progress": observations,
        "exact_queue_occupancy_measured": False,
        "strict_fairness_promised": False,
    }


def main():
    action, directory = sys.argv[1:]
    root = Path(directory)
    result = {"order": order, "barriers": barriers, "bounded": bounded}[action](root)
    print(json.dumps({"passed": True, "files": result}))


if __name__ == "__main__":
    main()
