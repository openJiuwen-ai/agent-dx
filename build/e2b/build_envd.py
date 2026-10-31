#!/usr/bin/env python3
"""Build the pinned upstream envd on Linux, retaining source/toolchain provenance."""
import argparse
import hashlib
import json
import os
import platform
import shutil
import signal
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


def run_git(command, *, timeout):
    """Bound the complete Git process group, including its HTTPS transport."""
    environment = {**os.environ, 'GIT_TERMINAL_PROMPT': '0'}
    process = subprocess.Popen(command, env=environment, start_new_session=True)
    try:
        code = process.wait(timeout=timeout)
    finally:
        if process.poll() is None:
            try:
                os.killpg(process.pid, signal.SIGKILL)
            except ProcessLookupError:
                pass
            process.wait()
    if code:
        raise subprocess.CalledProcessError(code, command)


def build(source, output, jobs=4):
    """Build an explicit checkout, or fetch the pin into a temporary checkout."""
    if source is not None:
        return _build(source, output, jobs)
    pin = json.loads((ROOT / 'third_party/e2b/source.json').read_text())
    git = shutil.which('git')
    if git is None:
        raise FileNotFoundError('Git executable is required')
    with tempfile.TemporaryDirectory(prefix='adx-envd-source-') as directory:
        source = Path(directory)
        run_git([git, 'init', str(source)], timeout=30)
        run_git([git, '-C', str(source), 'remote', 'add', 'origin', pin['repository']], timeout=30)
        # Git 2.17 requires an explicit partial-clone repository before fetch
        # accepts --filter. Keep checkout patterns compatible with pre-2.25 Git.
        for key, value in (('core.repositoryformatversion', '1'), ('extensions.partialClone', 'origin'),
                           ('core.partialCloneFilter', 'blob:none'), ('remote.origin.promisor', 'true'),
                           ('remote.origin.partialCloneFilter', 'blob:none'), ('core.sparseCheckout', 'true')):
            run_git([git, '-C', str(source), 'config', key, value], timeout=30)
        (source / '.git/info/sparse-checkout').write_text('/*\n!/*/\n/packages/\n!/packages/*/\n'
                                                       '/packages/envd/\n/packages/shared/\n')
        run_git([git, '-C', str(source), 'fetch', '--depth=1', '--filter=blob:none', 'origin', pin['revision']],
                timeout=300)
        run_git([git, '-C', str(source), 'checkout', '--detach', 'FETCH_HEAD'], timeout=60)
        return _build(source, output, jobs)


def _build(source, output, jobs):
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
    parser.add_argument('--source', type=Path, help='clean pinned checkout; omitted: fetch the pin temporarily')
    parser.add_argument('--output', type=Path, required=True)
    parser.add_argument('--jobs', type=int, default=4)
    parsed_args = parser.parse_args()
    build(parsed_args.source.resolve() if parsed_args.source else None, parsed_args.output.resolve(), parsed_args.jobs)
