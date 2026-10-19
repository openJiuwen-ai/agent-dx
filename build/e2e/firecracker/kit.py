"""Verify an externally built, immutable Firecracker runtime kit before packaging."""

import hashlib
import json
from pathlib import Path

ROOT = Path(__file__).resolve().parents[3]
SOURCE = ROOT / 'third_party/sandboxd/source.json'


def expected_sandboxd_source():
    return json.loads(SOURCE.read_text())


def expected_sandboxd_revision():
    return expected_sandboxd_source()['revision']


def expected_sandboxd_patches():
    return expected_sandboxd_source().get('patches', [])


REQUIRED = {
    'bin/sandboxd',
    'bin/sbox',
    'bin/checkpoint-restore',
    'bin/firecracker',
    'artifacts/Image',
    'artifacts/initrd.img',
    'tools/virtiofsd',
    'tools/distill_fs',
    'tools/minio',
    'tools/docker-registry',
    'tools/redis-cli',
}


def verify(directory, backend):
    root = Path(directory)
    if root.is_symlink() or (root / 'manifest.json').is_symlink():
        raise ValueError('runtime kit symlink rejected')
    manifest = json.loads((root / 'manifest.json').read_text())
    revision = expected_sandboxd_revision()
    patches = expected_sandboxd_patches()
    if manifest.get('schema_version') != 1 or manifest.get('target') != backend['target']:
        raise ValueError('runtime kit architecture or sandboxd revision mismatch')
    if manifest.get('sandboxd_revision') != revision or backend.get('sandboxd_revision') != revision:
        raise ValueError('runtime kit architecture or sandboxd revision mismatch')
    if manifest.get('sandboxd_patches') != patches or backend.get('sandboxd_patches') != patches:
        raise ValueError('runtime kit sandboxd patch identity mismatch')
    initrd_provenance = manifest.get('initrd_provenance')
    if initrd_provenance != {
        'source_revision': revision,
        'patches': patches,
        'sha256': manifest.get('files', {}).get('artifacts/initrd.img'),
    }:
        raise ValueError('runtime kit initrd provenance mismatch')
    if set(manifest.get('files', {})) != REQUIRED:
        raise ValueError('runtime kit file inventory mismatch')
    for name, expected in manifest['files'].items():
        path = root / name
        if any(p.is_symlink() for p in (path, path.parent)) or not path.is_file():
            raise ValueError('runtime kit file missing or symlinked: ' + name)
        h = hashlib.sha256()
        with path.open('rb') as f:
            for chunk in iter(lambda: f.read(1024 * 1024), b''):
                h.update(chunk)
        if h.hexdigest() != expected:
            raise ValueError('runtime kit checksum mismatch: ' + name)
    for name in ('sandboxd', 'sbox', 'redis-cli'):
        prefix = 'tools/' if name == 'redis-cli' else 'bin/'
        if manifest['files'][prefix + name] != backend['files'][name]:
            raise ValueError('runtime kit and backend artifact differ: ' + name)
    return manifest
