#!/usr/bin/env python3
"""Build the pinned upstream envd on Linux, retaining source/toolchain provenance."""
import argparse
import hashlib
import json
import logging
import os
import platform
import shutil
import signal
import subprocess
import tarfile
import tempfile
from pathlib import Path, PurePosixPath
from urllib.parse import urlsplit

ROOT = Path(__file__).resolve().parents[2]
LOGGER = logging.getLogger(__name__)


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


def unpack_archive(archive, source, pin):
    """Verify the complete pinned archive before extracting required regular files."""
    artifact = pin['archive']
    if archive.stat().st_size != artifact['bytes'] or digest(archive) != artifact['sha256']:
        raise ValueError('envd source archive integrity mismatch')
    prefix = 'runtime-' + pin['revision']
    with tarfile.open(archive, 'r:gz') as bundle:
        seen = set()
        for member in bundle:
            path = PurePosixPath(member.name)
            if path.is_absolute() or '..' in path.parts or not path.parts or path.parts[0] != prefix:
                raise ValueError('unsafe envd archive path')
            relative = PurePosixPath(*path.parts[1:])
            selected = relative == PurePosixPath('LICENSE') or relative.parts[:2] in (
                ('packages', 'envd'), ('packages', 'shared'))
            if not selected or member.isdir():
                continue
            if not member.isfile() or relative in seen:
                raise ValueError('unsafe or duplicate envd archive entry')
            seen.add(relative)
            destination = source.joinpath(*relative.parts)
            destination.parent.mkdir(parents=True, exist_ok=True)
            with bundle.extractfile(member) as src, destination.open('wb') as dst:
                shutil.copyfileobj(src, dst)


def build(source, output, jobs=4, git_protocol=None, obs_source=False):
    """Build an explicit checkout, or fetch the pin into a temporary checkout."""
    if git_protocol not in (None, '2'):
        raise ValueError('git_protocol must be omitted or 2')
    if source is not None:
        return _build(source, output, jobs)
    pin = json.loads((ROOT / 'third_party/e2b/source.json').read_text())
    if obs_source:
        artifact = pin['archive']
        url = urlsplit(artifact['url'])
        if (url.scheme != 'https' or not url.hostname or '.obs.' not in url.hostname
                or not url.hostname.endswith('.myhuaweicloud.com') or url.username or url.password
                or url.query or url.fragment):
            raise ValueError('invalid envd OBS source URL')
        with tempfile.TemporaryDirectory(prefix='adx-envd-obs-') as directory:
            root = Path(directory)
            archive = root / 'source.tar.gz'
            LOGGER.info('Download pinned envd source archive from OBS')
            curl = shutil.which('curl')
            if curl is None:
                raise FileNotFoundError('curl executable is required')
            subprocess.run([curl, '--fail', '--location', '--silent', '--show-error', '--proto', '=https',
                            '--proto-redir', '=https', '--retry', '3', '--connect-timeout', '15',
                            '--max-time', '180', '--retry-max-time', '360', '--output', str(archive),
                            artifact['url']], check=True)
            source = root / 'source'
            unpack_archive(archive, source, pin)
            return _build(source, output, jobs, archive_revision=pin['revision'])
    git = shutil.which('git')
    if git is None:
        raise FileNotFoundError('Git executable is required')
    with tempfile.TemporaryDirectory(prefix='adx-envd-source-') as directory:
        source = Path(directory)
        run_git([git, 'init', str(source)], timeout=30)
        run_git([git, '-C', str(source), 'remote', 'add', 'origin', pin['repository']], timeout=30)
        # Git 2.17 requires an explicit partial-clone repository before fetch
        # accepts --filter. Keep checkout patterns compatible with pre-2.25 Git.
        # CI opts into v2 locally: both fetch and checkout's lazy blob fetches
        # inherit it, without changing other callers or global Git settings.
        if git_protocol:
            run_git([git, '-C', str(source), 'config', 'protocol.version', git_protocol], timeout=30)
        for key, value in (('core.repositoryformatversion', '1'), ('extensions.partialClone', 'origin'),
                           ('core.partialCloneFilter', 'blob:none'), ('remote.origin.promisor', 'true'),
                           ('remote.origin.partialCloneFilter', 'blob:none'), ('core.sparseCheckout', 'true')):
            run_git([git, '-C', str(source), 'config', key, value], timeout=30)
        (source / '.git/info/sparse-checkout').write_text('/*\n!/*/\n/packages/\n!/packages/*/\n'
                                                       '/packages/envd/\n/packages/shared/\n')
        LOGGER.info('Fetch pinned envd source %s (Git protocol %s)', pin['revision'], git_protocol or 'default')
        run_git([git, '-C', str(source), 'fetch', '--depth=1', '--filter=blob:none', 'origin', pin['revision']],
                timeout=300)
        LOGGER.info('Materialize pinned envd checkout, including sparse blobs')
        run_git([git, '-C', str(source), 'checkout', '--detach', 'FETCH_HEAD'], timeout=60)
        LOGGER.info('Pinned envd checkout complete; verifying source before compilation')
        return _build(source, output, jobs)


def _build(source, output, jobs, archive_revision=None):
    if platform.system() != 'Linux' or jobs < 1:
        raise ValueError('native Linux builder and positive jobs required')
    pin = json.loads((ROOT / 'third_party/e2b/source.json').read_text())
    revision = archive_revision if archive_revision is not None else git_output(source, 'rev-parse', 'HEAD')
    dirty = '' if archive_revision is not None else git_output(source, 'status', '--porcelain')
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
    logging.basicConfig(level=logging.INFO, format='%(message)s')
    parser = argparse.ArgumentParser()
    parser.add_argument('--source', type=Path, help='clean pinned checkout; omitted: fetch the pin temporarily')
    parser.add_argument('--output', type=Path, required=True)
    parser.add_argument('--jobs', type=int, default=4)
    parser.add_argument('--git-protocol', choices=('2',),
                        help='opt into protocol v2 in the temporary checkout (Git 2.18+)')
    parser.add_argument('--obs-source', action='store_true', help='download the checksum-pinned OBS source archive')
    parsed_args = parser.parse_args()
    if parsed_args.source and parsed_args.obs_source:
        parser.error('--source and --obs-source are mutually exclusive')
    build(parsed_args.source.resolve() if parsed_args.source else None, parsed_args.output.resolve(), parsed_args.jobs,
          git_protocol=parsed_args.git_protocol, obs_source=parsed_args.obs_source)
