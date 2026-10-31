"""Bounded real-read correctness probes; this file makes no timing comparison."""

import hashlib
import json
import mmap
import os
from pathlib import Path
import sys

MIB = 1024**2


def pattern(offset, size):
    parts = []
    while size:
        length = min(size, MIB - offset % MIB)
        parts.append(bytes([offset // MIB % 251]) * length)
        offset += length
        size -= length
    return b"".join(parts)


def create(path, size):
    digest = hashlib.sha256()
    fd = os.open(path, os.O_CREAT | os.O_EXCL | os.O_WRONLY, 0o600)
    try:
        for offset in range(0, size, MIB):
            body = pattern(offset, min(MIB, size - offset))
            assert os.write(fd, body) == len(body)
            digest.update(body)
        os.fsync(fd)
    finally:
        os.close(fd)
    return {"bytes": size, "sha256": digest.hexdigest()}


def ranges(path, size):
    fd = os.open(path, os.O_RDONLY)
    checked = []
    try:
        for offset, count in [(0, 1), (4093, 8199), (4 * MIB - 13, 8197), (size - 3, 99), (size, 4096)]:
            data = os.pread(fd, count, offset)
            assert data == pattern(offset, max(0, min(count, size - offset)))
            checked.append({"offset": offset, "requested": count, "returned": len(data)})
        digest = hashlib.sha256()
        offset = 0
        while offset < size:
            data = os.pread(fd, MIB, offset)
            assert data and data == pattern(offset, len(data))
            digest.update(data)
            offset += len(data)
        assert not os.pread(fd, 4096, size)
    finally:
        os.close(fd)
    return {"bytes": offset, "sha256": digest.hexdigest(), "ranges": checked, "eof": True}


def direct(path, size):
    """Explicit O_DIRECT, page-aligned preadv input; partial EOF is checked."""
    fd = os.open(path, os.O_RDONLY | os.O_DIRECT)
    buffer = mmap.mmap(-1, MIB)
    digest = hashlib.sha256()
    try:
        offset = 0
        while offset < size:
            count = os.preadv(fd, [buffer], offset)
            assert count > 0 and buffer[:count] == pattern(offset, count)
            digest.update(buffer[:count])
            offset += count
        assert os.preadv(fd, [buffer], size) == 0
        assert os.preadv(fd, [buffer], 4 * MIB) > 0
        assert buffer[:4096] == pattern(4 * MIB, 4096)
    finally:
        buffer.close()
        os.close(fd)
    return {"bytes": offset, "sha256": digest.hexdigest(), "explicit_direct": True, "eof": True, "seek_checked": True}


def main():
    action, name, count = sys.argv[1:]
    path, size = Path(name), int(count)
    result = {"create": create, "ranges": ranges, "direct": direct}[action](path, size)
    print(json.dumps(result))


if __name__ == "__main__":
    main()
