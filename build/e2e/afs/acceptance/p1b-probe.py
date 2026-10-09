#!/usr/bin/env python3
"""Short real-mount probes; this is not a full acceptance driver."""
import argparse
import json
import mmap
import os
from pathlib import Path
import subprocess


def exact_mount(path, expected):
    result = subprocess.run(["findmnt", "-rn", "--mountpoint", str(path), "-o", "SOURCE,FSTYPE"], check=True, capture_output=True, text=True)
    source, fstype = result.stdout.strip().split()
    if source != expected or not (fstype == "fuse" or fstype.startswith("fuse.")):
        raise RuntimeError(f"unexpected mount {source}/{fstype}")


def file_probe(root, suffix):
    path = root / f"close-{suffix}.txt"
    fd = os.open(path, os.O_CREAT | os.O_EXCL | os.O_RDWR, 0o600)
    try:
        os.write(fd, b"close without explicit sync")
    finally:
        os.close(fd)
    assert path.read_bytes() == b"close without explicit sync"
    return {"path": str(path), "bytes": path.stat().st_size}


def cache_probe(root, suffix):
    path = root / f"cached-{suffix}.txt"
    writer = os.open(path, os.O_CREAT | os.O_EXCL | os.O_RDWR, 0o600)
    reader = os.open(path, os.O_RDONLY)
    try:
        # Warm the reader after both handles exist, before the next write.
        os.write(writer, b"a" * 8192)
        assert os.pread(reader, 8192, 0) == b"a" * 8192
        with mmap.mmap(reader, 8192, flags=mmap.MAP_SHARED, prot=mmap.PROT_READ) as view:
            assert view[:] == b"a" * 8192
            in_mapping = False
            resident_kib = 0
            for line in Path("/proc/self/smaps").read_text().splitlines():
                if line.endswith(str(path)):
                    in_mapping = True
                elif in_mapping and line.startswith("Rss:"):
                    resident_kib = int(line.split()[1])
                    break
            assert resident_kib > 0, "warm shared mapping has no resident cached pages"
            os.pwrite(writer, b"HELLO", 4094)
            assert os.pread(reader, 5, 4094) == b"HELLO"
            assert view[4094:4099] == b"HELLO"
        os.pwrite(writer, b"tail", 8192)
        assert os.fstat(reader).st_size == 8196
        assert os.pread(reader, 4, 8192) == b"tail"
        os.ftruncate(writer, 3)
        assert os.fstat(reader).st_size == 3
        assert os.pread(reader, 8192, 0) == b"aaa"
        os.ftruncate(writer, 8192)
        assert os.fstat(reader).st_size == 8192
        assert os.pread(reader, 8189, 3) == bytes(8189)
    finally:
        os.close(reader)
        os.close(writer)
    return {"path": str(path), "warm_readonly": True, "shared_readonly_mapping": True, "resident_kib": resident_kib, "overwrite_append_resize_before_sync": True}


def mapping_write_probe(root, suffix):
    path = root / f"mapping-write-{suffix}.txt"
    fd = os.open(path, os.O_CREAT | os.O_EXCL | os.O_RDWR, 0o600)
    try:
        os.write(fd, b"a" * 8192)
        os.fsync(fd)
        with mmap.mmap(fd, 8192, flags=mmap.MAP_SHARED, prot=mmap.PROT_READ | mmap.PROT_WRITE) as view:
            view[4094:4101] = b"MAPPING"
            view.flush()
            os.fsync(fd)
        assert os.pread(fd, 7, 4094) == b"MAPPING"
    finally:
        os.close(fd)
    expected = b"a" * 4094 + b"MAPPING" + b"a" * (8192 - 4101)
    assert path.read_bytes() == expected
    return {"path": str(path), "shared_write_mapping": True, "msync_fsync_close_reopen": True}


parser = argparse.ArgumentParser()
parser.add_argument("--dfs", type=Path, required=True)
parser.add_argument("--ownerfs", type=Path, required=True)
parser.add_argument("--suffix", required=True)
parser.add_argument("--cache", action="store_true")
parser.add_argument("--mmap-write", action="store_true")
args = parser.parse_args()
if not args.suffix.replace("-", "").isalnum():
    raise SystemExit("invalid probe suffix")
exact_mount(args.dfs, "afs-dfs")
exact_mount(args.ownerfs, "afs-ownerfs")
workspace = args.ownerfs / f"probe-{args.suffix}"
workspace.mkdir()
result = {"kind": "development_slice", "full_acceptance": False, "dfs": file_probe(args.dfs, args.suffix), "ownerfs": file_probe(workspace, args.suffix)}
if args.cache:
    result["dfs_cache"] = cache_probe(args.dfs, args.suffix)
if args.mmap_write:
    result["dfs_mapping_write"] = mapping_write_probe(args.dfs, args.suffix)
print(json.dumps(result, indent=2))
