"""Linux-side worker for the staged two-host OwnerFs FabricMem verifier."""

import argparse
from concurrent.futures import ProcessPoolExecutor
import hashlib
import json
import multiprocessing
import os
from pathlib import Path
import re
import sys
import time
from urllib.parse import urlsplit
from urllib.request import HTTPRedirectHandler, ProxyHandler, build_opener

MIB = 1024 * 1024
WINDOW = 4 * MIB
SIZE = 2 * WINDOW + 123
OFFSET = 4099
TAIL = SIZE - 17
STEM = re.compile(r"afs-fm-[0-9a-f]{32}-[ab]-(1|8|32|64)")


def require(condition, message):
    if not condition:
        raise ValueError(message)


def mount_entry(path):
    encoded = re.sub(rb"[ \t\n\\]", lambda m: b"\\%03o" % m[0][0], os.fsencode(path))
    matches = []
    for line in Path("/proc/self/mountinfo").read_bytes().splitlines():
        left, right = line.split(b" - ", 1)
        fields = left.split()
        if fields[4] == encoded:
            matches.append((fields[0].decode(), *right.split()[:2]))
    if not matches:
        return None
    mount_id, fs_type, source = matches[-1]
    return {
        "id": mount_id,
        "ownerfs": (fs_type == b"fuse" or fs_type.startswith(b"fuse."))
        and source == b"afs-ownerfs",
    }


def mounted(args):
    mount = Path(args.mount)
    require(
        mount.is_absolute()
        and mount != Path("/")
        and mount.resolve(strict=True) == mount,
        "use an absolute, canonical mount path",
    )
    entry = mount_entry(mount)
    require(
        entry == {"id": args.mount_id, "ownerfs": True},
        f"unexpected or replaced mount at {mount}: {entry}",
    )
    return mount


class NoRedirect(HTTPRedirectHandler):
    def redirect_request(self, req, fp, code, msg, headers, newurl):
        return None


def snapshot(args):
    # Node's private diagnostic endpoint: no proxy, redirects, credentials or
    # remote HTTP. SSH already authenticates the host and protects the hop.
    url = urlsplit(args.rest)
    require(
        url.scheme == "http"
        and url.hostname in ("127.0.0.1", "::1")
        and url.port
        and url.path in ("", "/")
        and not (url.username or url.password or url.query or url.fragment),
        "REST must be http://127.0.0.1:PORT or http://[::1]:PORT",
    )
    opener = build_opener(ProxyHandler({}), NoRedirect())
    opener.addheaders = []

    def get(path):
        endpoint = args.rest.rstrip("/") + path
        with opener.open(endpoint, timeout=8) as result:
            require(result.geturl() == endpoint, "unexpected diagnostic redirect")
            return result.read().decode()

    host = Path("/proc/sys/kernel/random/boot_id").read_text().strip()
    return {
        "health": json.loads(get("/health")),
        "metrics": get("/metrics"),
        "boot_id": host,
        "mount": mount_entry(args.mount),
    }


def payload(stem, index, kind, size):
    return hashlib.shake_256(f"{stem}/{index}/{kind}".encode()).digest(size)


def digest(value):
    return hashlib.sha256(value).hexdigest()


def exact_read(fd):
    out = bytearray()
    while part := os.read(fd, MIB):
        out.extend(part)
        require(len(out) <= SIZE + MIB, "file larger than expected")
    return out


def read_file(path, expected, probes=False):
    fd = os.open(path, os.O_RDONLY | os.O_NOFOLLOW)
    try:
        if probes:
            require(
                os.pread(fd, 128 * 1024, OFFSET)
                == expected[OFFSET : OFFSET + 128 * 1024],
                f"{path}: nonzero-offset read mismatch",
            )
            require(
                os.pread(fd, 4096, TAIL) == expected[TAIL:],
                f"{path}: short read mismatch",
            )
            require(os.pread(fd, 4096, len(expected)) == b"", f"{path}: EOF mismatch")
        actual = exact_read(fd)
    finally:
        os.close(fd)
    actual_digest = digest(actual)
    require(
        len(actual) == len(expected) and actual_digest == digest(expected),
        f"{path}: digest/length mismatch",
    )
    return actual_digest


def write_file(path, data, flags, offset=None):
    fd = os.open(path, os.O_WRONLY | os.O_NOFOLLOW | flags, 0o600)
    try:
        for start in range(0, len(data), MIB):
            chunk = data[start : start + MIB]
            written = (
                os.write(fd, chunk)
                if offset is None
                else os.pwrite(fd, chunk, offset + start)
            )
            require(
                written == len(chunk),
                f"{path}: short write ({written}/{len(chunk)}); do not retry",
            )
        os.fsync(fd)
    finally:
        os.close(fd)


def exercise(job):
    directory, index = job
    source = directory / f"source-{index}"
    target = directory / f"target-{index}"
    seed = payload(directory.name, index, "seed", SIZE)
    replacement = payload(directory.name, index, "replace", MIB)
    suffix = payload(directory.name, index, "append", MIB - 11)
    start = WINDOW + OFFSET
    expected = seed[:start] + replacement + seed[start + MIB :] + suffix
    # All N workers reach the same gate before any measured IO.
    gate.wait(timeout=30)
    started = time.monotonic()
    source_sha = read_file(source, seed, probes=True)
    source_done = time.monotonic()
    write_file(target, replacement, 0, offset=start)
    overwrite_done = time.monotonic()
    write_file(target, suffix, os.O_APPEND)
    append_done = time.monotonic()
    target_sha = read_file(target, expected)
    done = time.monotonic()
    read_bytes = len(seed) + len(expected) + 128 * 1024 + 17
    write_bytes = len(replacement) + len(suffix)
    read_seconds = (source_done - started) + (done - append_done)
    write_seconds = append_done - source_done
    return {
        "index": index,
        "pid": os.getpid(),
        "source_sha256": source_sha,
        "target_sha256": target_sha,
        "read_bytes": read_bytes,
        "write_bytes": write_bytes,
        "seconds": done - started,
        "source_read_seconds": source_done - started,
        "overwrite_fsync_seconds": overwrite_done - source_done,
        "append_fsync_seconds": append_done - overwrite_done,
        "reader_readback_seconds": done - append_done,
        "read_mib_per_second": read_bytes / MIB / read_seconds,
        "write_mib_per_second": write_bytes / MIB / write_seconds,
    }


def init_worker(barrier):
    global gate
    gate = barrier


def files(args):
    require(
        STEM.fullmatch(args.stem) and args.stem.endswith(f"-{args.processes}"),
        "invalid test directory name",
    )
    directory = mounted(args) / args.stem
    if args.action == "seed":
        directory.mkdir(mode=0o700)
        for index in range(args.processes):
            seed = payload(directory.name, index, "seed", SIZE)
            for kind in ("source", "target"):
                write_file(directory / f"{kind}-{index}", seed, os.O_CREAT | os.O_EXCL)
        return {"seeded": args.processes}
    require(
        directory.is_dir() and not directory.is_symlink(),
        "test directory missing or replaced",
    )
    if args.action == "work":
        ctx = multiprocessing.get_context("fork")
        jobs = [(directory, index) for index in range(args.processes)]
        with ProcessPoolExecutor(
            args.processes,
            mp_context=ctx,
            initializer=init_worker,
            initargs=(ctx.Barrier(args.processes),),
        ) as pool:
            return list(pool.map(exercise, jobs))
    results = []
    for index in range(args.processes):
        seed = payload(directory.name, index, "seed", SIZE)
        start = WINDOW + OFFSET
        expected = (
            seed[:start]
            + payload(directory.name, index, "replace", MIB)
            + seed[start + MIB :]
            + payload(directory.name, index, "append", MIB - 11)
        )
        results.append(
            {
                "index": index,
                "sha256": read_file(directory / f"target-{index}", expected),
            }
        )
    return results


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("action", choices=("snapshot", "seed", "work", "verify"))
    parser.add_argument("--mount", required=True)
    parser.add_argument("--rest")
    parser.add_argument("--mount-id")
    parser.add_argument("--stem")
    parser.add_argument("--processes", type=int, choices=(1, 8, 32, 64))
    args = parser.parse_args()
    value = snapshot(args) if args.action == "snapshot" else files(args)
    sys.stdout.write(json.dumps(value) + "\n")


if __name__ == "__main__":
    main()
