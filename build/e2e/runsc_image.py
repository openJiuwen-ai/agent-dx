"""Extract and verify a pinned complete gVisor runtime kit from an OCI carrier."""

import argparse
import hashlib
import importlib.util
import logging
import os
import re
import shutil
import subprocess
import sys
import tempfile
from pathlib import Path

ROOT = Path(__file__).resolve().parents[2]
runsc_spec = importlib.util.spec_from_file_location('runsc_release', ROOT / 'build/release/download_runsc.py')
runsc_release = importlib.util.module_from_spec(runsc_spec)
runsc_spec.loader.exec_module(runsc_release)
IMAGE_REF = re.compile(r'[^\s@]+@sha256:[0-9a-f]{64}\Z')
SHA512 = re.compile(r'[0-9a-fA-F]{128}\Z')
CONTAINER_ID = re.compile(r'[0-9a-f]{64}\Z')
PLATFORMS = {'x86_64-unknown-linux-gnu': 'linux/amd64', 'aarch64-unknown-linux-gnu': 'linux/arm64'}


def digest(path, algorithm):
    value = hashlib.new(algorithm)
    with path.open('rb') as source:
        for chunk in iter(lambda: source.read(1024 * 1024), b''):
            value.update(chunk)
    return value.hexdigest()


def _remove_container(docker, cidfile, run, operation_failed):
    """Attempt one controlled cleanup without hiding an earlier operation error."""
    created = 'unavailable'
    try:
        if cidfile.is_symlink() or not cidfile.exists():
            return
        created = cidfile.read_text(encoding='utf-8').strip()
        if not CONTAINER_ID.fullmatch(created):
            logging.error('Container cleanup cannot use an invalid carrier identity')
            return
        run([docker, 'rm', '-f', created], check=True, stdout=sys.stderr)
    except (OSError, UnicodeError, subprocess.SubprocessError) as error:
        if not operation_failed:
            raise
        logging.error('Container cleanup failed for %s: %s', created, error)


def stage(image, expected_sha512, output, run=subprocess.run, *, target='x86_64-unknown-linux-gnu'):
    """Publish the verified six-file kit without replacing existing output paths."""
    if not IMAGE_REF.fullmatch(image):
        raise ValueError('runsc source image must use an immutable digest')
    if expected_sha512 is not None and not SHA512.fullmatch(expected_sha512):
        raise ValueError('runsc SHA512 must be a 128-digit hexadecimal value')
    platform = PLATFORMS.get(target)
    if platform is None:
        raise ValueError('unsupported runsc target: ' + target)
    output = Path(output)
    helpers = output.parent / 'gvisor-bin'
    if any(path.is_symlink() or path.exists() for path in (output, helpers)):
        raise ValueError('runsc output and helper paths must not exist')
    docker = shutil.which('docker')
    if docker is None:
        raise FileNotFoundError('docker is required to extract the runsc runtime kit')
    docker = os.path.abspath(docker)
    output.parent.mkdir(parents=True, exist_ok=True)
    with tempfile.TemporaryDirectory(prefix='.runsc-image-', dir=output.parent) as directory:
        extracted = Path(directory) / 'extracted'
        extracted.mkdir()
        run([docker, 'pull', '--platform', platform, image], check=True, stdout=sys.stderr)
        # The carrier has no default command; docker create never executes it.
        cidfile = Path(directory) / 'carrier.cid'
        try:
            run(
                [docker, 'create', '--platform', platform, '--cidfile', str(cidfile),
                 image, '/runsc', '--version'],
                check=True, stdout=sys.stderr, stderr=sys.stderr,
            )
            if cidfile.is_symlink():
                raise ValueError('docker create returned a symlinked container identity file')
            created = cidfile.read_text(encoding='utf-8').strip()
            if not CONTAINER_ID.fullmatch(created):
                raise ValueError('docker create returned an invalid container identity')
            run([docker, 'cp', created + ':/runsc', str(extracted / 'runsc')], check=True, stdout=sys.stderr)
            run([docker, 'cp', created + ':/gvisor-bin', str(extracted / 'gvisor-bin')], check=True, stdout=sys.stderr)
            verified = runsc_release.verify_runtime(extracted / 'runsc', target)
            if expected_sha512 is not None and digest(extracted / 'runsc', 'sha512') != expected_sha512.lower():
                raise ValueError('runsc SHA512 does not match the pinned binary')
        finally:
            _remove_container(docker, cidfile, run, operation_failed=sys.exc_info()[0] is not None)
        # Only verified members are published, never additional carrier files.
        # Exclusive directory creation and hard links cannot replace an existing kit.
        helpers.mkdir()
        try:
            for name in verified:
                source = extracted / name
                source.chmod(0o755)
                if name != 'runsc':
                    os.link(source, output.parent / name)
            os.link(extracted / 'runsc', output)
        except Exception:
            shutil.rmtree(helpers)
            raise
        return verified['runsc']


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('--image', required=True)
    parser.add_argument('--sha512', help='optional additional SHA512 check for the launcher')
    parser.add_argument('--target', choices=PLATFORMS, default='x86_64-unknown-linux-gnu')
    parser.add_argument('--output', type=Path, required=True)
    args = parser.parse_args()
    print(stage(args.image, args.sha512, args.output, target=args.target), flush=True)


if __name__ == '__main__':
    main()
