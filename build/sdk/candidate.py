#!/usr/bin/env python3
"""Create and verify the immutable Python SDK candidate manifest."""
import argparse
import email.parser
import hashlib
import json
from pathlib import Path
import re
import zipfile


def sha(path):
    return hashlib.sha256(path.read_bytes()).hexdigest()


def wheel_metadata(wheel):
    with zipfile.ZipFile(wheel) as archive:
        names = [name for name in archive.namelist() if name.endswith('.dist-info/METADATA')]
        if len(names) != 1:
            raise ValueError('wheel must contain exactly one METADATA file')
        return email.parser.Parser().parsestr(archive.read(names[0]).decode())


def create(directory, commit, build_id=None):
    if not re.fullmatch(r'[0-9a-f]{40}', commit):
        raise ValueError('SDK candidate requires a 40-character Git commit')
    wheel = sorted(directory.glob('adx_sandbox-*-py3-none-any.whl'))
    source = sorted(directory.glob('adx_sandbox-*.tar.gz'))
    if len(wheel) != 1 or len(source) != 1:
        raise ValueError('SDK candidate requires exactly one wheel and one source archive')
    metadata = wheel_metadata(wheel[0])
    if metadata['Name'] != 'adx-sandbox' or not metadata['Version']:
        raise ValueError('unexpected SDK package metadata')
    result = {
        'schema_version': 1,
        'name': metadata['Name'],
        'version': metadata['Version'],
        'requires_python': metadata['Requires-Python'],
        'commit': commit,
        'build_id': build_id,
        'files': {path.name: sha(path) for path in (wheel[0], source[0])},
    }
    (directory / 'sdk-candidate.json').write_text(json.dumps(result, indent=2) + '\n')
    return result


def verify(directory):
    candidate = json.loads((directory / 'sdk-candidate.json').read_text())
    files = candidate.get('files')
    if candidate.get('schema_version') != 1 or not isinstance(files, dict):
        raise ValueError('invalid SDK candidate manifest')
    if not re.fullmatch(r'[0-9a-f]{40}', candidate.get('commit', '')):
        raise ValueError('invalid SDK candidate commit')
    actual = {path.name for path in directory.iterdir() if path.is_file() and path.name != 'sdk-candidate.json'
              and (path.suffix == '.whl' or path.name.endswith('.tar.gz'))}
    if actual != set(files):
        raise ValueError('SDK candidate file list mismatch')
    for name, digest in files.items():
        if Path(name).name != name or sha(directory / name) != digest:
            raise ValueError('SDK candidate artifact digest mismatch')
    wheel = next(directory / name for name in files if name.endswith('.whl'))
    metadata = wheel_metadata(wheel)
    if metadata['Name'] != candidate.get('name') or metadata['Version'] != candidate.get('version'):
        raise ValueError('SDK candidate metadata mismatch')
    return candidate


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('--verify', action='store_true')
    parser.add_argument('--directory', type=Path, required=True)
    parser.add_argument('--commit')
    parser.add_argument('--build-id')
    args = parser.parse_args()
    if args.verify:
        result = verify(args.directory)
    else:
        if not args.commit:
            parser.error('--commit is required when creating a candidate')
        result = create(args.directory, args.commit, args.build_id)
    print(json.dumps(result))


if __name__ == '__main__':
    main()
