#!/usr/bin/env python3
"""Compose the pinned openEuler filesystem and static runtime into local EROFS."""

import argparse
from dataclasses import dataclass
import hashlib
import inspect
import json
import logging
import os
from pathlib import Path, PurePosixPath
import platform
import re
import shutil
import subprocess
import sys
import tarfile
import tempfile
from urllib.parse import urlsplit

LOG = logging.getLogger(__name__)
REPO = Path(__file__).resolve().parents[2]
SOURCE = REPO / 'third_party/runtime-rootfs/source.json'


def _executable(name, environment=None, cwd=None):
    """Resolve an external command using the child's execution environment."""
    working_directory = os.getcwd() if cwd is None else os.path.abspath(cwd)
    search_path = os.pathsep.join(
        os.path.abspath(os.path.join(working_directory, entry)) for entry in os.get_exec_path(environment)
    )
    executable = shutil.which(name, path=search_path)
    if executable is None:
        raise FileNotFoundError(f"required executable not found: {name}")
    return os.path.abspath(executable)


def sha256(path):
    digest = hashlib.sha256()
    with path.open('rb') as stream:
        for block in iter(lambda: stream.read(1024 * 1024), b''):
            digest.update(block)
    return digest.hexdigest()


def download(url, archive, *, timeout=None):
    subprocess.run([_executable('curl'), '--fail', '--silent', '--show-error', '--location',
                    '--proto', '=https', '--proto-redir', '=https', '--connect-timeout', '15',
                    '--max-time', '600', '--retry', '2', '--output', str(archive), '--', url],
                   check=True, timeout=timeout)


def reuse_image(stage, url, digest, expected):
    archive = stage / 'previous-execd.tar.gz'
    download(url, archive, timeout=120)
    if sha256(archive) != digest:
        raise ValueError('previous Execd archive checksum mismatch')

    component = stage / 'previous-execd'
    component.mkdir()
    payload = {'manifest.json', 'adx-execd', 'adx-runtime-rootfs.img', 'envd-manifest.json', 'envd-LICENSE'}
    with tarfile.open(archive, 'r:gz') as source:
        for member in source:
            path = PurePosixPath(member.name)
            if path == PurePosixPath('.') and member.isdir():
                continue
            if path.as_posix() not in payload or not member.isfile():
                raise ValueError('unexpected previous Execd archive member')
            with source.extractfile(member) as stream, (component / path).open('xb') as target:
                shutil.copyfileobj(stream, target)
            payload.remove(path.as_posix())
    if payload:
        raise ValueError('previous Execd archive is incomplete')

    manifest = json.loads((component / 'manifest.json').read_text())
    commit = manifest.get('commit') if isinstance(manifest, dict) else None
    if not isinstance(commit, str) or not re.fullmatch(r'[0-9a-f]{40}', commit):
        raise ValueError('previous Execd commit is invalid')
    metadata = manifest.get('runtime_rootfs')
    if not isinstance(metadata, dict):
        raise ValueError('previous EROFS lacks provenance')
    for key, value in expected.items():
        if metadata.get(key) != value:
            raise ValueError(f'previous EROFS has missing or different {key}')
    subprocess.run([sys.executable, str(REPO / 'build/release/component.py'), 'verify',
                    '--component', 'execd', '--directory', str(component), '--commit', commit,
                    '--target', expected['target']], check=True)
    envd_manifest = json.loads((component / 'envd-manifest.json').read_text())
    if not isinstance(envd_manifest, dict) or envd_manifest.get('sha256') != expected['envd_sha256']:
        raise ValueError('previous envd manifest does not match current envd')

    image = component / 'adx-runtime-rootfs.img'
    guest = stage / 'previous-rootfs'
    subprocess.run([_executable('fsck.erofs'), '--no-preserve', f'--extract={guest}', str(image)],
                   check=True, timeout=300)
    guest_bin = guest / 'usr/local/bin'
    if any(path.is_symlink() for path in (guest, guest / 'usr', guest / 'usr/local', guest_bin)):
        raise ValueError('unexpected symlink in previous EROFS binary path')
    for name, key in (('adx-execd', 'execd_sha256'), ('envd', 'envd_sha256')):
        binary = guest_bin / name
        if binary.is_symlink() or not binary.is_file() or sha256(binary) != expected[key]:
            raise ValueError(f'previous EROFS contains a different {name}')
    return image, metadata['rootfs_sha256'], commit


def static_executable(path, target):
    with path.open('rb') as stream:
        header = stream.read(20)
    machine = {'x86_64-unknown-linux-gnu': 62, 'aarch64-unknown-linux-gnu': 183}[target]
    if header[:6] != b'\x7fELF\x02\x01' or int.from_bytes(header[18:20], 'little') != machine:
        raise ValueError(f'runtime executable architecture mismatch: {path.name}')
    result = subprocess.run([_executable('readelf'), '-l', str(path)], text=True, capture_output=True, check=True)
    if 'INTERP' in result.stdout:
        raise ValueError(f'{path.name} requires a guest dynamic linker; a static executable is required')


def archive_name(name):
    path = PurePosixPath(name)
    if path.is_absolute() or '..' in path.parts or not path.parts:
        raise ValueError(f'invalid base archive path: {name}')
    return path


def extract_base(archive, root):
    """Preserve guest links and metadata without writing through guest symlinks."""
    with tarfile.open(archive) as source:
        members = source.getmembers()
        indexed = {}
        for member in members:
            name = archive_name(member.name)
            if name in indexed or not (member.isfile() or member.isdir() or member.issym() or member.islnk()):
                raise ValueError(f'unsupported or duplicate base archive member: {name}')
            indexed[name] = member
        links = {name for name, member in indexed.items() if member.issym()}
        for name, member in indexed.items():
            if any(parent in links for parent in name.parents):
                raise ValueError(f'base archive writes through a symlink: {name}')
            if member.islnk():
                target = archive_name(member.linkname)
                visited = {name}
                target_member = indexed.get(target)
                while target_member is not None and target_member.islnk() and target not in visited:
                    visited.add(target)
                    target = archive_name(target_member.linkname)
                    target_member = indexed.get(target)
                if target_member is None or not target_member.isfile():
                    raise ValueError(f'invalid base archive hardlink: {name}')
                if any(parent in links for parent in target.parents):
                    raise ValueError(f'base archive hardlink traverses a symlink: {name}')
        # Validation above allows absolute guest symlinks, but no writes through
        # them. Python's data filter would reject valid distribution links.
        options = {'filter': 'fully_trusted'} if 'filter' in inspect.signature(source.extractall).parameters else {}
        source.extractall(root, **options)


def directory(root, name, mode=0o755):
    """Create runtime directories; keep existing distribution directory modes."""
    path = root
    for part in PurePosixPath(name).parts:
        path = path / part
        if path.is_symlink():
            raise ValueError(f'runtime directory is a symlink: {name}')
        if not path.exists():
            path.mkdir(mode=mode)
            path.chmod(mode)
    return path


def write_file(path, contents):
    if path.is_symlink():
        raise ValueError(f'runtime output is a symlink: {path.name}')
    path.write_text(contents)
    path.chmod(0o644)


@dataclass(frozen=True)
class EnvdConfig:
    """Optional static envd using the distribution's bash and shared libraries."""

    binary: Path
    default_user: str = 'user'


def install_envd(root, config):
    target = root / 'usr/local/bin/envd'
    if target.is_symlink():
        raise ValueError('envd output is a symlink')
    shutil.copyfile(config.binary, target)
    target.chmod(0o755)
    if config.default_user != 'root':
        passwd, group = root / 'etc/passwd', root / 'etc/group'
        entries = [line.split(':') for line in passwd.read_text().splitlines()]
        if any(entry[0] == config.default_user or entry[2] == '1000' for entry in entries):
            raise ValueError('envd user conflicts with the base filesystem')
        home = directory(root, f'home/{config.default_user}')
        os.chown(home, 1000, 1000)
        with passwd.open('a') as stream:
            stream.write(f'{config.default_user}:x:1000:1000:E2B user:/home/{config.default_user}:/bin/bash\n')
        with group.open('a') as stream:
            stream.write(f'{config.default_user}:x:1000:\n')
    write_file(root / 'etc/adx-envd-manifest.json', json.dumps({
        'path': '/usr/local/bin/envd', 'sha256': sha256(config.binary),
        'shell': '/bin/bash', 'default_user': config.default_user,
        'dependencies': {'usr/local/bin/envd': sha256(config.binary),
                         'usr/bin/bash': sha256(root / 'usr/bin/bash')},
    }, sort_keys=True) + '\n')


def build(binary, output, *, base_archive=None, target=None, envd=None, reuse_url=None, reuse_sha256=None):
    if output.exists() or output.with_suffix('.json').exists():
        raise ValueError('output already exists')
    target = target or platform.machine() + '-unknown-linux-gnu'
    pin = json.loads(SOURCE.read_text())
    if target not in pin['artifacts']:
        raise ValueError(f'unsupported rootfs target: {target}')
    base = pin['artifacts'][target]
    static_executable(binary, target)
    if envd is not None:
        if not re.fullmatch(r'[a-z_][a-z0-9_-]{0,31}', envd.default_user):
            raise ValueError('invalid envd default user')
        static_executable(envd.binary, target)
    if reuse_url is not None or reuse_sha256 is not None:
        url = urlsplit(reuse_url or '')
        valid_host = url.scheme == 'https' and url.hostname and url.username is None
        valid_digest = re.fullmatch(r'[0-9a-f]{64}', reuse_sha256 or '')
        if envd is None or not valid_digest or not valid_host:
            raise ValueError('EROFS reuse requires envd, an HTTPS archive URL and its lowercase SHA-256')
        if url.query or url.fragment:
            raise ValueError('EROFS reuse archive URL must not contain a query or fragment')
    if base_archive is not None and (base_archive.stat().st_size != base['bytes']
                                     or sha256(base_archive) != base['sha256']):
        raise ValueError('base rootfs checksum mismatch')

    mkfs = _executable('mkfs.erofs')
    metadata = {key: pin[key] for key in ('schema_version', 'distribution', 'version', 'image_reference')}
    metadata.update(target=target, manifest_digest=base['manifest_digest'],
                    base_sha256=base['sha256'], execd_sha256=sha256(binary))
    provenance = {}
    if envd is not None:
        # The guest metadata stays independent of this release-side reuse fingerprint.
        recipe = [sha256(Path(__file__)), sha256(Path(mkfs)), os.getenv('SOURCE_DATE_EPOCH'), envd.default_user]
        provenance = {'envd_sha256': sha256(envd.binary),
                      'recipe_sha256': hashlib.sha256(json.dumps(recipe).encode()).hexdigest()}
    output.parent.mkdir(parents=True, exist_ok=True)
    with tempfile.TemporaryDirectory(prefix='adx-runtime-') as temporary:
        stage = Path(temporary)
        if reuse_url:
            with tempfile.TemporaryDirectory(dir=stage) as candidate:
                try:
                    image, digest, commit = reuse_image(Path(candidate), reuse_url, reuse_sha256, metadata | provenance)
                except (OSError, ValueError, EOFError, tarfile.TarError, subprocess.SubprocessError) as error:
                    LOG.warning('EROFS reuse skipped: %s', error)
                else:
                    shutil.copyfile(image, output)
                    write_file(output.with_suffix('.json'), json.dumps(metadata | provenance | {
                        'rootfs_sha256': digest, 'reused_from': {'commit': commit, 'archive_sha256': reuse_sha256},
                    }, indent=2) + '\n')
                    sys.stdout.write(f'Reused EROFS {digest} from ADX {commit}\n')
                    return
        archive = base_archive or stage / 'base.tar.gz'
        if base_archive is None:
            download(base['url'], archive)
            if archive.stat().st_size != base['bytes'] or sha256(archive) != base['sha256']:
                raise ValueError('base rootfs checksum mismatch')
        root = stage / 'root'
        root.mkdir()
        root.chmod(0o755)
        extract_base(archive, root)
        for name in ('usr/local/bin', 'etc', 'tmp', 'var/tmp', 'home', 'proc', 'sys', 'dev', '__adx'):
            directory(root, name)
        (root / 'tmp').chmod(0o1777)
        (root / 'var/tmp').chmod(0o1777)
        executable = root / 'usr/local/bin/adx-execd'
        if executable.is_symlink():
            raise ValueError('Execd output is a symlink')
        shutil.copyfile(binary, executable)
        executable.chmod(0o755)
        if envd is not None:
            install_envd(root, envd)
        for name in ('usr', 'bin', 'sbin', 'etc', 'home', 'root', 'lib', 'lib64'):
            (root / '__adx' / name).symlink_to('/' + name)
        write_file(root / 'etc/adx-runtime-rootfs.json', json.dumps(metadata, sort_keys=True) + '\n')
        image = stage / 'rootfs.img'
        subprocess.run([mkfs, '-E', 'noinline_data', str(image), str(root)], check=True)
        subprocess.run([_executable('fsck.erofs'), str(image)], check=True)
        metadata['rootfs_sha256'] = sha256(image)
        shutil.copyfile(image, output)
        write_file(output.with_suffix('.json'), json.dumps(metadata | provenance, indent=2) + '\n')


if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument('--binary', type=Path, required=True)
    parser.add_argument('--envd-binary', type=Path)
    parser.add_argument('--envd-default-user', default='user')
    parser.add_argument('--base-archive', type=Path, help='offline copy of the pinned base archive')
    parser.add_argument('--reuse-url', help='HTTPS URL of a previous published adx-execd.tar.gz')
    parser.add_argument('--reuse-sha256', help='SHA-256 of the previous Execd archive')
    parser.add_argument('--target', help='native Linux release target')
    parser.add_argument('--output', type=Path, required=True)
    args = parser.parse_args()
    envd_config = EnvdConfig(args.envd_binary, args.envd_default_user) if args.envd_binary else None
    build(args.binary, args.output, base_archive=args.base_archive, target=args.target, envd=envd_config,
          reuse_url=args.reuse_url, reuse_sha256=args.reuse_sha256)
