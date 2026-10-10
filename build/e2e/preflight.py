#!/usr/bin/env python3
"""Read-only host prerequisites for the sandboxd runc acceptance fixture."""

import argparse
import json
import os
import shutil
import subprocess
import tempfile
from pathlib import Path


def _executable(name, environment=None, cwd=None):
    """Resolve an external command using the child's execution environment."""
    directory = os.getcwd() if cwd is None else os.path.abspath(cwd)
    search_path = os.pathsep.join(
        os.path.abspath(os.path.join(directory, entry)) for entry in os.get_exec_path(environment)
    )
    executable = shutil.which(name, path=search_path)
    if executable is None:
        raise FileNotFoundError(f"required executable not found: {name}")
    return os.path.abspath(executable)


RUNTIME_IMAGE = Path('/opt/adx/package/runtime/adx-runtime-rootfs.img')


def verify_erofs_mount(image=RUNTIME_IMAGE):
    if not image.is_file():
        raise FileNotFoundError(f'EROFS runtime image is missing: {image}')
    with tempfile.TemporaryDirectory(prefix='adx-erofs-preflight-') as target:
        mounted = False
        try:
            result = subprocess.run(
                [_executable('mount'), '-t', 'erofs', '-o', 'loop,ro', str(image), target],
                text=True,
                capture_output=True,
            )
            if result.returncode:
                detail = (result.stderr or result.stdout).strip()
                raise OSError(detail or f'mount exited {result.returncode}')
            mounted = True
        finally:
            if mounted:
                subprocess.run([_executable('umount'), target], check=True)


def check(proc=Path('/proc'), erofs_image=RUNTIME_IMAGE, mount_probe=verify_erofs_mount, runtime_source='erofs'):
    filesystems = {line.split()[-1] for line in (proc / 'filesystems').read_text().splitlines() if line.strip()}
    bridge = proc / 'sys/net/bridge/bridge-nf-call-iptables'
    result = {}
    if runtime_source == 'erofs':
        result['erofs'] = 'erofs' in filesystems
    elif runtime_source != 'image':
        raise ValueError('runtime_source must be erofs or image')
    result['bridge_netfilter'] = bridge.is_file() and bridge.read_text().strip() == '1'
    missing = [name for name, available in result.items() if not available]
    if missing:
        raise RuntimeError('sandboxd host prerequisites unavailable: ' + ', '.join(missing))
    if runtime_source == 'erofs':
        try:
            mount_probe(erofs_image)
        except (OSError, subprocess.SubprocessError) as error:
            raise RuntimeError(f'sandboxd host prerequisite unavailable: erofs_mount: {error}') from error
        result['erofs_mount'] = True
    return result


if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument('--runtime-source', choices=('erofs', 'image'), default='erofs')
    args = parser.parse_args()
    print(json.dumps(check(runtime_source=args.runtime_source)), flush=True)
