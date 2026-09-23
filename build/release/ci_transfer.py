#!/usr/bin/env python3
"""Transfer intermediate CI artifacts via authenticated OBS, with identity and SHA checks."""
import argparse
import json
import os
from pathlib import Path
import tempfile

from obs_upload import COMMIT, component, digest, checked_upload, obs_client, DEFAULT_BUCKET, DEFAULT_ENDPOINT


def prefix(build_id, commit, group):
    component(build_id, 'build ID')
    component(group, 'artifact group')
    if not COMMIT.fullmatch(commit):
        raise ValueError('invalid commit')
    return f'adx/ci/{build_id}/{commit}/{group}'


def upload(client, bucket, build_id, commit, group, files):
    root = prefix(build_id, commit, group)
    entries = {}
    for path in map(Path, files):
        component(path.name, 'filename')
        if path.is_symlink() or not path.is_file() or path.name == 'manifest.json' or path.name in entries:
            raise ValueError('invalid or duplicate artifact')
        entries[path.name] = {'bytes': path.stat().st_size, 'sha256': digest(path)}
    if not entries:
        raise ValueError('empty artifact group')
    for path in map(Path, files):
        checked_upload(client, bucket, f'{root}/{path.name}', path)
    manifest = {'schema_version': 1, 'build_id': build_id, 'commit': commit, 'group': group, 'files': entries}
    with tempfile.TemporaryDirectory() as tmp:
        path = Path(tmp) / 'manifest.json'
        path.write_text(json.dumps(manifest) + '\n')
        checked_upload(client, bucket, f'{root}/manifest.json', path)


def download(client, bucket, build_id, commit, group, output):
    root = prefix(build_id, commit, group)
    output = Path(output)
    output.mkdir(parents=True, exist_ok=True)
    with tempfile.TemporaryDirectory(dir=output) as tmp:
        staging = Path(tmp)
        def fetch(name):
            response = client.getObject(bucket, f'{root}/{name}', downloadPath=str(staging / name))
            if response.status >= 300:
                raise RuntimeError(f'OBS download failed: {group}/{name}, status={response.status}')
        fetch('manifest.json')
        manifest = json.loads((staging / 'manifest.json').read_text())
        for name, expected in [('schema_version', 1), ('build_id', build_id), ('commit', commit), ('group', group)]:
            if manifest.get(name) != expected:
                raise ValueError(f'artifact {name} mismatch')
        entries = manifest.get('files')
        if not isinstance(entries, dict) or not entries:
            raise ValueError('missing artifact files')
        for name, entry in entries.items():
            component(name, 'filename')
            if name == 'manifest.json':
                raise ValueError('reserved filename')
            fetch(name)
            path = staging / name
            if path.stat().st_size != entry['bytes'] or digest(path) != entry['sha256']:
                raise ValueError(f'artifact digest mismatch: {name}')
        # No partially verified group is installed on failure.
        for name in entries:
            (staging / name).replace(output / name)


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('action', choices=['upload', 'download'])
    parser.add_argument('group')
    parser.add_argument('paths', nargs='+')
    args = parser.parse_args()
    endpoint = os.getenv('ADX_OBS_ENDPOINT', DEFAULT_ENDPOINT)
    bucket = os.getenv('ADX_OBS_BUCKET', DEFAULT_BUCKET)
    client = obs_client(os.environ['OBS_ACCESS_KEY_ID'], os.environ['OBS_SECRET_ACCESS_KEY'], endpoint)
    identity = (client, bucket, os.environ['BUILDKITE_BUILD_ID'], os.environ['BUILDKITE_COMMIT'], args.group)
    try:
        if args.action == 'upload':
            upload(*identity, args.paths)
        else:
            if len(args.paths) != 1:
                parser.error('download requires one output directory')
            download(*identity, args.paths[0])
    finally:
        client.close()


if __name__ == '__main__':
    main()
