"""Barrier-synchronized, single-reader-node OwnerFs A3 capacity workload."""

import argparse
from concurrent.futures import ProcessPoolExecutor
import hashlib
import json
import multiprocessing
import os
import re
import sys
import time

from a3_fabricmem_worker import WINDOW, mounted, payload, require

GROUP = 8 * WINDOW
GROUPS = 3
STEM = re.compile(r"afs-cap-[0-9a-f]{12}")


def set_gate(barrier):
    global gate
    gate = barrier


def capacity_one(job):
    directory, action, index = job
    filename = directory / f"worker-{index:02}.bin"
    kind = "updated" if action in ("write", "verify") else "original"
    measured = action in ("read", "write")
    writing = action in ("seed", "write")
    expected_sha = hashlib.sha256()
    write_groups = []
    # Prepare distinct groups before the barrier. Readers only need the expected
    # digest, so release each generated group before allocating the read buffer.
    for group in range(GROUPS):
        expected = payload(directory.name, index, f"{kind}/group-{group}", GROUP)
        expected_sha.update(expected)
        if writing:
            write_groups.append(memoryview(expected))
        del expected
    if not writing:
        buf = memoryview(bytearray(GROUP * GROUPS))
    if measured:
        gate.wait(timeout=60)
    clock = time.monotonic_ns
    started = clock()
    flags = os.O_WRONLY if writing else os.O_RDONLY
    if action == "seed":
        flags |= os.O_CREAT | os.O_EXCL
    fd = os.open(filename, flags | os.O_NOFOLLOW, 0o600)
    opened = clock()
    samples = []
    try:
        for group in range(GROUPS):
            group_buf = (
                write_groups[group]
                if writing
                else buf[group * GROUP : (group + 1) * GROUP]
            )
            begin = clock()
            for offset in range(0, GROUP, WINDOW):
                view = group_buf[offset : offset + WINDOW]
                actual = os.write(fd, view) if writing else os.readv(fd, [view])
                require(actual == WINDOW, f"{filename}: short {action} ({actual})")
            if writing:
                os.fsync(fd)
            samples.append({"begin_ns": begin, "end_ns": clock()})
        if not writing:
            require(os.read(fd, 1) == b"", f"{filename}: expected EOF")
    finally:
        os.close(fd)
    ended = clock()
    sha = expected_sha.hexdigest() if writing else hashlib.sha256(buf).hexdigest()
    require(sha == expected_sha.hexdigest(), f"{filename}: digest mismatch on {action}")
    return {
        "index": index,
        "pid": os.getpid(),
        "bytes": GROUP * len(samples),
        "sha256": sha,
        "started_ns": started,
        "opened_ns": opened,
        "ended_ns": ended,
        "groups": samples,
    }


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("action", choices=("seed", "read", "write", "verify"))
    parser.add_argument("--mount", required=True)
    parser.add_argument("--mount-id", required=True)
    parser.add_argument("--stem", required=True)
    parser.add_argument("--processes", type=int, choices=(1, 32, 64), required=True)
    args = parser.parse_args()
    require(STEM.fullmatch(args.stem), "invalid capacity directory name")
    directory = mounted(args) / args.stem
    if args.action == "seed":
        directory.mkdir(mode=0o700)
    require(
        directory.is_dir() and not directory.is_symlink(), "invalid capacity directory"
    )
    jobs = [(directory, args.action, index) for index in range(args.processes)]
    ctx = multiprocessing.get_context("fork")
    workers = (
        args.processes if args.action in ("read", "write") else min(4, args.processes)
    )
    with ProcessPoolExecutor(
        workers, mp_context=ctx, initializer=set_gate, initargs=(ctx.Barrier(workers),)
    ) as pool:
        rows = list(pool.map(capacity_one, jobs))
    require(
        args.action not in ("read", "write")
        or len({row["pid"] for row in rows}) == workers,
        "expected one distinct process per measured worker",
    )
    sys.stdout.write(json.dumps(rows) + "\n")


if __name__ == "__main__":
    main()
