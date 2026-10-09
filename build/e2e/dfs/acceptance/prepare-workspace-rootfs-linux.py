#!/usr/bin/env python3
"""Prepare a fresh test-only rootfs from an explicitly built Cargo example.

No Cargo build or dependency installation occurs here. Build the example with
`cargo build --release --locked --offline -p afs --example afs-workspace-probe` first.
The supplied helper/busybox digests bind inputs; resolved ldd libraries are
recorded and copied as regular files. Nothing is added to the product package.
"""
import argparse
import hashlib
import json
import os
from pathlib import Path
import platform
import re
import shutil
import subprocess
import struct


def sha(path):
    with Path(path).open('rb') as stream:
        return hashlib.file_digest(stream, 'sha256').hexdigest()


def static_arm64_elf(path):
    """Verify the ELF header/table, not ldd's text alone, before static admission."""
    data = Path(path).read_bytes()
    if len(data) < 64 or data[:7] != b'\x7fELF\x02\x01\x01':
        return False
    kind, machine = struct.unpack_from('<HH', data, 16)
    offset = struct.unpack_from('<Q', data, 32)[0]
    entry_bytes, count = struct.unpack_from('<HH', data, 54)
    if (kind not in (2, 3) or machine != 183 or entry_bytes != 56
            or not 0 < count < 256 or offset < 64 or offset + count * entry_bytes > len(data)):
        return False
    return all(struct.unpack_from('<I', data, offset + index * entry_bytes)[0] != 3
               for index in range(count))


def admit_ldd(binary, done):
    output = done.stdout + done.stderr
    if 'not found' in output:
        raise RuntimeError('unresolved dynamic dependency: ' + str(binary))
    if done.returncode == 0:
        return 'dynamic'
    if (done.returncode == 1 and output.strip() in ('not a dynamic executable', 'statically linked')
            and static_arm64_elf(binary)):
        return 'static-arm64-elf'
    raise RuntimeError('unresolved ELF dependency observation: ' + str(binary))


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    for name in ('helper', 'busybox', 'output', 'manifest'):
        parser.add_argument('--' + name, type=Path, required=True)
    for name in ('helper-sha256', 'busybox-sha256'):
        parser.add_argument('--' + name, required=True)
    args = parser.parse_args()
    if (platform.system(), platform.machine(), os.geteuid()) != ('Linux', 'aarch64', 0):
        raise RuntimeError('requires Linux ARM64 root')
    for path in (args.helper, args.busybox, args.output, args.manifest):
        if not path.is_absolute() or '..' in path.parts or any(p.is_symlink() for p in (path, *path.parents)):
            raise ValueError('absolute non-symlink paths required: ' + str(path))
    if args.output.exists() or args.manifest.exists() or args.manifest.is_relative_to(args.output):
        raise ValueError('fresh rootfs and external fresh manifest required')
    for parent in (args.output.parent, *args.output.parent.parents):
        metadata = parent.stat()
        if metadata.st_uid != 0 or metadata.st_mode & 0o022:
            raise ValueError('rootfs ancestors must be root owned and not writable by others')
    for path, digest in ((args.helper, args.helper_sha256), (args.busybox, args.busybox_sha256)):
        if not path.is_file() or sha(path) != digest or not os.access(path, os.X_OK):
            raise ValueError('input identity/executable mismatch: ' + str(path))
    if not shutil.which('ldd'):
        raise RuntimeError('missing ldd; stop, do not install')
    files = {'afs-workspace-probe': args.helper, 'bin/busybox': args.busybox, 'bin/sh': args.busybox}
    commands = []
    for binary in (args.helper, args.busybox):
        argv = ['ldd', str(binary)]
        done = subprocess.run(argv, capture_output=True, text=True, timeout=20)
        commands.append(dict(argv=argv, rc=done.returncode, stdout=done.stdout, stderr=done.stderr))
        commands[-1]['linkage'] = admit_ldd(binary, done)
        for absolute in re.findall(r'(?:=>\s+|^\s*)(/\S+)\s+\(', done.stdout, re.M):
            files[absolute.lstrip('/')] = Path(absolute).resolve(strict=True)
    # Admit every dependency before creating the fixture; no incomplete tree is
    # silently promoted to a usable rootfs on failure.
    inputs = {relative: dict(source=str(source), sha256=sha(source)) for relative, source in files.items()}
    args.output.mkdir(mode=0o755)
    for relative, source in files.items():
        target = args.output / relative
        target.parent.mkdir(parents=True, exist_ok=True, mode=0o755)
        shutil.copyfile(source, target)
        target.chmod(0o755)
        if sha(target) != inputs[relative]['sha256']:
            raise RuntimeError('copied input changed: ' + relative)
        inputs[relative].update(bytes=target.stat().st_size, mode='0o755')
    for relative in ('proc', 'workspace'):
        (args.output / relative).mkdir(mode=0o755)
    args.manifest.parent.mkdir(parents=True, exist_ok=True)
    with args.manifest.open('x') as stream:
        json.dump(inputs, stream, indent=2)
        stream.write('\n')
    print(json.dumps(dict(status='PREPARED_TEST_ROOTFS_ONLY', rootfs=str(args.output),
                         manifest=str(args.manifest), files=inputs, commands=commands,
                         idle_command=['/afs-workspace-probe', 'idle'],
                         identity_command=['/afs-workspace-probe', 'identity']), indent=2))


if __name__ == '__main__':
    main()
