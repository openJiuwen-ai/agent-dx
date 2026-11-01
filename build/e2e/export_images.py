"""Deliver fixed test images to an offline Docker host without exporting credentials."""

import argparse
import gzip
import hashlib
import json
from pathlib import Path
import re
import shutil
import subprocess
import tempfile


def sha(path):
    digest = hashlib.sha256()
    with path.open('rb') as stream:
        for block in iter(lambda: stream.read(1024 * 1024), b''):
            digest.update(block)
    return digest.hexdigest()


def verify_image(image, expected, architecture, reference):
    if image['Id'] != expected or image['Architecture'] != architecture or image['Os'] != 'linux':
        raise ValueError('exported image differs from the fixed candidate')
    if reference not in image.get('RepoDigests', []):
        raise ValueError('exported image lacks the fixed registry digest')


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--bundle', type=Path, required=True)
    parser.add_argument('--registry-images', type=Path, required=True)
    parser.add_argument('--output', type=Path, required=True)
    args = parser.parse_args()
    manifest = json.loads(args.bundle.read_text())
    registry = json.loads(args.registry_images.read_text())
    if registry['bundle_sha256'] != sha(args.bundle) or registry['image_ids'] != manifest['image_ids']:
        raise ValueError('registry binding differs from the fixed bundle')
    if set(registry['references']) != {'node', 'execd', 'entrypoint'}:
        raise ValueError('exactly the three fixed test images required')
    docker = shutil.which('docker')
    if docker is None:
        raise FileNotFoundError('Docker executable required')
    args.output.mkdir(parents=True, exist_ok=False)
    for role, reference in registry['references'].items():
        if not re.fullmatch(r'[^\s]+@sha256:[0-9a-f]{64}', reference):
            raise ValueError('immutable image reference required')
        subprocess.run([docker, 'pull', reference], check=True, timeout=600)
        image = json.loads(subprocess.check_output([docker, 'image', 'inspect', reference]))[0]
        verify_image(image, manifest['image_ids'][role], manifest['architecture'], reference)
    archive = args.output / 'images.tar.gz'
    with tempfile.TemporaryDirectory(prefix='adx-image-export-', dir=args.output) as temporary:
        raw = Path(temporary) / 'images.tar'
        subprocess.run([docker, 'save', '-o', str(raw), *manifest['image_ids'].values()], check=True, timeout=600)
        with raw.open('rb') as source, gzip.open(archive, 'wb', compresslevel=1) as destination:
            shutil.copyfileobj(source, destination)
    result = {'schema_version': 1, 'product_commit': manifest['package']['commit'],
              'bundle_sha256': sha(args.bundle), 'registry_sha256': sha(args.registry_images),
              'architecture': manifest['architecture'], 'image_ids': manifest['image_ids'],
              'references': registry['references'],
              'archive': {'name': archive.name, 'bytes': archive.stat().st_size, 'sha256': sha(archive)},
              'scope': 'derived offline image transport; original bundle and product bytes unchanged'}
    (args.output / 'image-export.json').write_text(json.dumps(result, indent=2) + '\n')
    print(json.dumps(result), flush=True)


if __name__ == '__main__':
    main()
