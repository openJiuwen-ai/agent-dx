#!/usr/bin/env python3
"""Build the pinned upstream envd on Linux, retaining source/toolchain provenance."""
import argparse
import hashlib
import json
import os
import platform
import shutil
import subprocess
import tempfile
from pathlib import Path

ROOT = Path(__file__).resolve().parents[2]


def digest(path):
    return hashlib.sha256(path.read_bytes()).hexdigest()


def git_output(source, *args):
    """Read a copied checkout without changing global Git safety configuration."""
    git = shutil.which('git')
    if git is None:
        raise FileNotFoundError('Git executable is required')
    return subprocess.check_output(
        [git, '-c', f'safe.directory={source}', '-C', str(source), *args],
        text=True,
    ).strip()


def build(source, output, jobs=4):
    if platform.system() != 'Linux' or jobs < 1:
        raise ValueError('native Linux builder and positive jobs required')
    pin = json.loads((ROOT / 'third_party/e2b/source.json').read_text())
    revision = git_output(source, 'rev-parse', 'HEAD')
    dirty = git_output(source, 'status', '--porcelain')
    if revision != pin['revision'] or dirty:
        raise ValueError('envd source must match the clean pinned revision')
    for entry in pin['files']:
        if digest(source / entry['source']) != entry['sha256']:
            raise ValueError('envd pinned source checksum mismatch')
    output.mkdir(parents=True, exist_ok=False)
    env = {**os.environ, 'CGO_ENABLED': '0', 'GOWORK': 'off',
           'GOTOOLCHAIN': pin['go_toolchain'], 'GOFLAGS': f'-p={jobs}',
           'GOMAXPROCS': str(jobs)}
    go = shutil.which('go')
    if go is None:
        raise FileNotFoundError('Go toolchain launcher is required')
    with tempfile.TemporaryDirectory(prefix='adx-envd-') as tmp:
        packages = Path(tmp)
        for name in ('envd', 'shared'):
            shutil.copytree(source / 'packages' / name, packages / name,
                            ignore=shutil.ignore_patterns('bin', '.git'))
        subprocess.run([go, 'build', '-trimpath', '-buildvcs=false', '-ldflags',
                        f'-X=main.commitSHA={revision} -s -w -buildid=', '-o',
                        str((output / 'envd').resolve()), '.'], cwd=packages / 'envd', env=env, check=True)
    binary = output / 'envd'
    version = subprocess.check_output([str(binary.resolve()), '--version'], text=True).strip()
    commit = subprocess.check_output([str(binary.resolve()), '--commit'], text=True).strip()
    if version != pin['envd_version'] or commit != revision:
        raise ValueError('built envd provenance differs from the pin')
    (output / 'LICENSE').write_bytes((ROOT / 'third_party/e2b/LICENSE').read_bytes())
    (output / 'manifest.json').write_text(json.dumps({
        'repository': pin['repository'], 'revision': revision, 'version': version,
        'toolchain': subprocess.check_output([go, 'version'], env=env, text=True).strip(),
        'architecture': platform.machine(), 'sha256': digest(binary),
    }, indent=2) + '\n')


if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument('--source', type=Path, required=True)
    parser.add_argument('--output', type=Path, required=True)
    parser.add_argument('--jobs', type=int, default=4)
    args = parser.parse_args()
    build(args.source.resolve(), args.output.resolve(), args.jobs)
