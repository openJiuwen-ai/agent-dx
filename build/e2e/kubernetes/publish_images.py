#!/usr/bin/env python3
"""Publish the verified build images and record immutable pull references."""

import argparse
import json
import os
import re
import shutil
import subprocess
import sys
import uuid
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


sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
from run import sha, verify_bundle


def main():
    p = argparse.ArgumentParser()
    p.add_argument('--bundle', type=Path, required=True)
    p.add_argument('--repository', required=True)
    a = p.parse_args()
    if not re.fullmatch(r'[a-zA-Z0-9.:/_-]+', a.repository) or a.repository.endswith('/'):
        raise ValueError('registry repository required without scheme or trailing slash')
    output = a.bundle / 'registry-images.json'
    if output.exists():
        raise FileExistsError(output)
    m = verify_bundle(a.bundle)
    subprocess.run([_executable('docker'), 'load', '-i', str(a.bundle / 'images.tar')], check=True)
    refs = {}
    for role, identity in m['image_ids'].items():
        tag = a.repository + ':' + m['package']['commit'][:12] + '-' + uuid.uuid4().hex[:12] + '-' + role
        subprocess.run([_executable('docker'), 'tag', identity, tag], check=True)
        try:
            subprocess.run([_executable('docker'), 'push', tag], check=True, timeout=600)
            image = json.loads(subprocess.check_output([_executable('docker'), 'image', 'inspect', tag]))[0]
            if image['Id'] != identity:
                raise ValueError('published image differs from verified build')
            refs[role] = next(r for r in image['RepoDigests'] if r.startswith(a.repository + '@sha256:'))
        finally:
            subprocess.run([_executable('docker'), 'image', 'rm', tag], check=True)
    output.write_text(
        json.dumps(
            {
                'schema_version': 1,
                'bundle_sha256': sha(a.bundle / 'bundle.json'),
                'image_ids': m['image_ids'],
                'references': refs,
            },
            indent=2,
        )
        + '\n'
    )
    print('Kubernetes image references recorded')


if __name__ == '__main__':
    main()
