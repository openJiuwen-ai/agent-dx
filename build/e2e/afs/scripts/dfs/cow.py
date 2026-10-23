#!/usr/bin/env python3
"""Real independent AFS mounts: metadata-only copy and isolated COW writes."""

import argparse
import hashlib
import json
import logging
import os
from pathlib import Path
import sys
import time
from urllib.request import ProxyHandler, build_opener

REPO = Path(__file__).resolve().parents[5]
sys.path.append(str(REPO / "build/e2e/firecracker"))
from filesystem_fixture import FilesystemFixture


def read_payload(port):
    total = 0
    opener = build_opener(ProxyHandler({}))
    for address in ("127.0.0.2", "127.0.0.3", "127.0.0.4"):
        with opener.open(f"http://{address}:{port}/metrics", timeout=10) as reply:
            for line in reply.read().decode().splitlines():
                if (
                    line.startswith("afs_dfs_payload_bytes_total{")
                    and 'direction="send"' in line
                    and 'operation="read"' in line
                ):
                    total += int(float(line.split()[-1]))
    return total


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--binary-dir", type=Path, required=True)
    parser.add_argument("--root", type=Path, required=True)
    args = parser.parse_args()
    root = args.root.resolve()
    evidence = root / "evidence"
    fixture = FilesystemFixture(REPO, args.binary_dir, root / "services", evidence)
    result = {"status": "failed"}
    try:
        fixture.start()
        a = root / "services/node-a/mount/dfs"
        b = root / "services/node-b/mount/dfs"
        payload = b"".join(hashlib.sha256(str(i).encode()).digest() * 128 for i in range(8192))
        source = a / "cow-source"
        with source.open("wb") as output:
            output.write(payload)
            output.flush()
            os.fsync(output.fileno())
        before = read_payload(fixture.metrics_port)
        with (b / "cow-source").open("rb") as original, (b / "cow-target").open("w+b") as target:
            # SharedFile may create a target of the final logical size first.
            target.truncate(len(payload))
            copied = os.copy_file_range(original.fileno(), target.fileno(), 1024**3)
            if not (copied == len(payload)):
                raise AssertionError(copied)
            if not (os.copy_file_range(original.fileno(), target.fileno(), 1024**3) == 0):
                raise AssertionError("COW validation failed")
            # Remap a nonzero source range over an existing destination layout.
            if not (
                os.copy_file_range(
                    original.fileno(),
                    target.fileno(),
                    8192,
                    offset_src=5 * 4096,
                    offset_dst=9 * 4096,
                )
                == 8192
            ):
                raise AssertionError("COW validation failed")
            after = read_payload(fixture.metrics_port)
            result.update(
                source_bytes=len(payload),
                copied_bytes=copied,
                copy_remote_read_bytes=after - before,
            )
            (evidence / "cow-result.json").write_text(json.dumps(result, indent=2))
            if not (after == before):
                raise AssertionError(("copy eagerly read source file data", after - before))
            target.seek(9 * 4096)
            if not (target.read(8192) == payload[5 * 4096 : 7 * 4096]):
                raise AssertionError("COW validation failed")
            target.seek(4096)
            target.write(b"private-cow-page")
            target.flush()
            os.fsync(target.fileno())
            if not (
                hashlib.sha256(source.read_bytes()).digest() == hashlib.sha256(payload).digest()
            ):
                raise AssertionError("COW validation failed")
            source.unlink()
            target.seek(1024**2)
            if not (target.read(4096) == payload[1024**2 : 1024**2 + 4096]):
                raise AssertionError("COW validation failed")
            target.seek(4096)
            if not (target.read(16) == b"private-cow-page"):
                raise AssertionError("COW validation failed")
        (b / "cow-target").unlink()
        size = 4 * 1024**3 + 123
        with (a / "large-sparse-source").open("wb") as output:
            output.truncate(size)
            output.seek(size - 16)
            output.write(b"large-cow-tail!!")
            output.flush()
            os.fsync(output.fileno())
        before = read_payload(fixture.metrics_port)
        with (
            (b / "large-sparse-source").open("rb") as original,
            (b / "large-sparse-target").open("w+b") as target,
        ):
            copied = 0
            while copied < size:
                count = os.copy_file_range(original.fileno(), target.fileno(), 1024**3)
                if not (count > 0):
                    raise AssertionError("COW validation failed")
                copied += count
            if not (copied == size):
                raise AssertionError("COW validation failed")
            after = read_payload(fixture.metrics_port)
            if not (after == before):
                raise AssertionError(("large range clone eagerly read data", after - before))
            target.seek(size - 16)
            if not (target.read(16) == b"large-cow-tail!!"):
                raise AssertionError("COW validation failed")
        (a / "large-sparse-source").unlink()
        (b / "large-sparse-target").unlink()
        result["large_sparse_cloned_bytes"] = size
        with (a / "idle-writer").open("w+b") as output:
            output.write(b"before-idle")
            output.flush()
            os.fsync(output.fileno())
            # Exceed the actual 30-second write lease while retaining the fd.
            time.sleep(35)
            output.seek(0)
            output.write(b"after--idle")
            output.flush()
            os.fsync(output.fileno())
        if not ((b / "idle-writer").read_bytes() == b"after--idle"):
            raise AssertionError("COW validation failed")
        (a / "idle-writer").unlink()
        result["idle_writer_seconds"] = 35
        result["status"] = "passed"
    finally:
        result["cleanup_errors"] = fixture.stop()
        if result["cleanup_errors"]:
            result["status"] = "failed"
        evidence.mkdir(parents=True, exist_ok=True)
        (evidence / "cow-result.json").write_text(json.dumps(result, indent=2))
    if not (result["status"] == "passed"):
        raise AssertionError(result)
    logging.info("%s", json.dumps(result))


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO, format="%(message)s")
    main()
