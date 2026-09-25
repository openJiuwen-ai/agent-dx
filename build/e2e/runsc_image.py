"""Extract a SHA512-pinned runsc binary from an immutable OCI image."""

import argparse
import hashlib
import os
from pathlib import Path
import re
import subprocess


IMAGE_REF = re.compile(r'[^\s@]+@sha256:[0-9a-f]{64}\Z')
SHA512 = re.compile(r'[0-9a-fA-F]{128}\Z')
CONTAINER_ID = re.compile(r'[0-9a-f]{64}\Z')


def digest(path, algorithm):
    value = hashlib.new(algorithm)
    with path.open('rb') as source:
        for chunk in iter(lambda: source.read(1024 * 1024), b''):
            value.update(chunk)
    return value.hexdigest()


def stage(image, expected_sha512, output, run=subprocess.run):
    if not IMAGE_REF.fullmatch(image):
        raise ValueError('runsc source image must use an immutable digest')
    if not SHA512.fullmatch(expected_sha512):
        raise ValueError('runsc SHA512 must be a 128-digit hexadecimal value')
    output = Path(output)
    if output.is_symlink() or output.exists():
        raise ValueError('runsc output path must not exist')
    output.parent.mkdir(parents=True, exist_ok=True)
    run(['docker', 'pull', '--platform', 'linux/amd64', image], check=True)
    # The carrier image intentionally has no default command; it is never run.
    created = run(['docker', 'create', image, '/runsc', '--version'], check=True, text=True,
                  capture_output=True).stdout.strip()
    if not CONTAINER_ID.fullmatch(created):
        raise ValueError('docker create returned an invalid container identity')
    try:
        try:
            run(['docker', 'cp', created + ':/runsc', str(output)], check=True)
            if digest(output, 'sha512') != expected_sha512.lower():
                raise ValueError('runsc SHA512 does not match the pinned binary')
            os.chmod(output, 0o755)
            return digest(output, 'sha256')
        except Exception:
            output.unlink(missing_ok=True)
            raise
    finally:
        run(['docker', 'rm', '-f', created], check=True,
            stdout=subprocess.DEVNULL)


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('--image', required=True)
    parser.add_argument('--sha512', required=True)
    parser.add_argument('--output', type=Path, required=True)
    args = parser.parse_args()
    print(stage(args.image, args.sha512, args.output), flush=True)


if __name__ == '__main__':
    main()
