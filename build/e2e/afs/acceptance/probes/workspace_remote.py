#!/usr/bin/env python3
"""Bounded guest file operations for a real bind/Home and remote-FUSE case."""
import argparse
import hashlib
import json
import os
from pathlib import Path
import platform


def payload(size, byte):
    if size not in (4096, 65536) or not 0 <= byte <= 255:
        raise ValueError('only frozen 4KiB/64KiB payloads are supported')
    return bytes([byte]) * size


def operate(action, path, size=4096, byte=97, destination=None, mode=None, expected_errno=None):
    path = Path(path)
    data = payload(size, byte)
    result = {'action': action, 'path': str(path), 'uid': os.geteuid(), 'gid': os.getegid()}
    if action == 'write':
        fd = os.open(path, os.O_WRONLY | os.O_CREAT | os.O_TRUNC, 0o600)
        try:
            view = memoryview(data)
            while view:
                written = os.write(fd, view)
                if written <= 0:
                    raise OSError('write made no progress')
                view = view[written:]
            os.fsync(fd)
        finally:
            os.close(fd)
        parent = os.open(path.parent, os.O_RDONLY | os.O_DIRECTORY)
        try:
            os.fsync(parent)
        finally:
            os.close(parent)
        result.update(bytes=len(data), sha256=hashlib.sha256(data).hexdigest(), barrier='fsync+close+parent-fsync')
    elif action == 'read':
        fd = os.open(path, os.O_RDONLY)
        try:
            actual = bytearray()
            while len(actual) < size:
                block = os.read(fd, size - len(actual))
                if not block:
                    break
                actual.extend(block)
            extra = os.read(fd, 1)
            length = os.fstat(fd).st_size
        finally:
            os.close(fd)
        if actual != data or extra or length != size:
            raise ValueError(f'fresh-open content/length/EOF mismatch: {len(actual)}/{length}, extra={extra!r}')
        result.update(bytes=len(actual), length=length, sha256=hashlib.sha256(actual).hexdigest(), eof=True)
    elif action in ('open-error', 'write-open-error'):
        if expected_errno not in (2, 13, 17):
            raise ValueError('predeclared ENOENT/EACCES/EEXIST required')
        flags = os.O_WRONLY | os.O_CREAT | os.O_EXCL if expected_errno == 17 else (
            os.O_WRONLY if action == 'write-open-error' else os.O_RDONLY)
        try:
            fd = os.open(path, flags, 0o600)
        except OSError as error:
            if error.errno != expected_errno:
                raise
            result['errno'] = error.errno
        else:
            os.close(fd)
            raise ValueError('expected open error, got success')
    elif action == 'chmod':
        if mode not in (0, 0o600):
            raise ValueError('only frozen permission transitions supported')
        os.chmod(path, mode)
        result['mode'] = os.stat(path).st_mode & 0o777
        if result['mode'] != mode:
            raise ValueError('chmod not visible')
    elif action in ('rename', 'unlink'):
        if action == 'rename':
            if destination is None or Path(destination).parent != path.parent:
                raise ValueError('rename must remain in the same selected directory')
            os.rename(path, destination)
            result['destination'] = str(destination)
        else:
            os.unlink(path)
        fd = os.open(path.parent, os.O_RDONLY | os.O_DIRECTORY)
        try:
            os.fsync(fd)
        finally:
            os.close(fd)
    else:
        raise ValueError('unknown action')
    return dict(status='PASS', **result)


def main():
    p = argparse.ArgumentParser(description=__doc__)
    p.add_argument('action', choices=['write', 'read', 'open-error', 'write-open-error', 'chmod', 'rename', 'unlink'])
    p.add_argument('--root', type=Path, required=True)
    p.add_argument('--name', required=True)
    p.add_argument('--uid', type=int, default=501)
    p.add_argument('--gid', type=int, default=501)
    p.add_argument('--size', type=int, default=4096)
    p.add_argument('--byte', type=int, default=97)
    p.add_argument('--destination')
    p.add_argument('--mode', type=lambda v: int(v, 8))
    p.add_argument('--expected-errno', type=int)
    a = p.parse_args()
    if platform.system() != 'Linux' or os.geteuid() != 0:
        raise RuntimeError('Linux guest root required for explicit credential drop')
    for name in [a.name, *([a.destination] if a.destination else [])]:
        if name in ('', '.', '..') or Path(name).name != name:
            raise ValueError('one filename within the selected directory required')
    if not a.root.is_absolute() or '..' in a.root.parts or a.uid not in (501, 502) or a.gid != a.uid:
        raise ValueError('absolute selected root and fixed nonprivileged credentials required')
    os.setgroups([])
    os.setgid(a.gid)
    os.setuid(a.uid)
    result = operate(a.action, a.root / a.name, a.size, a.byte,
                     a.root / a.destination if a.destination else None, a.mode, a.expected_errno)
    print(json.dumps(result))


if __name__ == '__main__':
    main()
