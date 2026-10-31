"""Explicit, bounded I/O workload. Functional mode emits no performance numbers.

This is a measurement asset, not a throughput acceptance gate. Read SHA work is
inside its timer; write readback verification is outside. These two definitions
must not be compared to historical cohorts with different timer boundaries.
"""

import argparse
import hashlib
import json
import os
from pathlib import Path
import resource
import time


def integer(value):
    return isinstance(value, int) and not isinstance(value, bool)


def run(operation, path, size, request, measure=False):
    if operation not in ("read", "write"):
        raise ValueError("operation must be read or write")
    if not integer(size) or not 4096 <= size <= 1024**3 or size % 4096:
        raise ValueError("bytes must be 4KiB..1GiB and page aligned")
    if not integer(request) or not 4096 <= request <= 1024**2 or request % 4096:
        raise ValueError("request must be 4KiB..1MiB and page aligned")
    body = b"w" * request
    expected = hashlib.sha256()
    for offset in range(0, size, request):
        expected.update(body[: min(request, size - offset)])
    usage = resource.getrusage(resource.RUSAGE_SELF) if measure else None
    start = time.monotonic_ns() if measure else None
    clock = {}
    calls = count = 0
    digest = hashlib.sha256()
    flags = os.O_RDONLY if operation == "read" else os.O_CREAT | os.O_EXCL | os.O_WRONLY
    fd = os.open(path, flags, 0o600)
    try:
        while count < size:
            wanted = min(request, size - count)
            if operation == "write":
                returned = os.write(fd, body[:wanted])
            else:
                data = os.read(fd, wanted)
                returned = len(data)
                digest.update(data)
            assert returned == wanted, ("short I/O", count, wanted, returned)
            count += returned
            calls += 1
        if measure:
            clock["delivery_ns"] = time.monotonic_ns() - start
        if operation == "write":
            os.fsync(fd)
        else:
            assert os.read(fd, 1) == b"", "unexpected trailing data"
            assert digest.hexdigest() == expected.hexdigest()
        if measure:
            clock["data_sync_or_read_verified_ns"] = time.monotonic_ns() - start
    finally:
        os.close(fd)
    if operation == "write":
        parent = os.open(Path(path).parent, os.O_RDONLY | os.O_DIRECTORY)
        try:
            os.fsync(parent)
        finally:
            os.close(parent)
    if measure:
        clock["complete_ns"] = time.monotonic_ns() - start
        current = resource.getrusage(resource.RUSAGE_SELF)
        clock["client_cpu_user_seconds"] = current.ru_utime - usage.ru_utime
        clock["client_cpu_system_seconds"] = current.ru_stime - usage.ru_stime
        clock["client_process_peak_rss_kib"] = current.ru_maxrss
    validation_bytes = 0
    if operation == "write":
        with open(path, "rb", buffering=0) as stream:
            for data in iter(lambda: stream.read(request), b""):
                digest.update(data)
                validation_bytes += len(data)
        assert validation_bytes == size and digest.hexdigest() == expected.hexdigest()
    result = {
        "purpose": "measurement" if measure else "functional helper qualification",
        "operation": operation, "bytes": count, "request_bytes": request, "payload_calls": calls,
        "sha256": digest.hexdigest(), "validation_read_bytes_outside_timing": validation_bytes,
        "progress_disk_writes_in_timing": 0,
        "read_sha_inside_timing": operation == "read" and measure,
        "performance_acceptance": "NOT_EVALUATED",
    }
    if measure:
        result["measurement"] = clock
        result["environment_boundary"] = "client only; Node/resource metrics are separate snapshots"
    return result


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("operation", choices=("read", "write"))
    parser.add_argument("path", type=Path)
    parser.add_argument("--bytes", type=int, required=True)
    parser.add_argument("--request-bytes", type=int, default=1024**2)
    parser.add_argument("--measure", action="store_true", help="explicitly enable measurement, not an SLA check")
    args = parser.parse_args()
    print(json.dumps(run(args.operation, args.path, args.bytes, args.request_bytes, args.measure)))


if __name__ == "__main__":
    main()
