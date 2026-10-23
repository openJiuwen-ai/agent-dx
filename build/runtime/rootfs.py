#!/usr/bin/env python3
"""Build a local EROFS runtime payload from verified native static executables."""

import argparse
from dataclasses import dataclass
import hashlib
import json
import os
from pathlib import Path
import re
import shutil
import subprocess
import tempfile


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


def static_executable(path):
    result = subprocess.run([_executable('readelf'), '-l', str(path)], text=True, capture_output=True, check=True)
    if 'INTERP' in result.stdout:
        raise ValueError(f'{path.name} requires a guest dynamic linker; a static executable is required')


def shell_dependencies(shell):
    """Resolve a trusted native bash and its ELF dependencies for the guest."""
    result = subprocess.run([_executable('ldd'), str(shell)], text=True, capture_output=True, check=True)
    if 'not found' in result.stdout:
        raise ValueError('bash has an unresolved shared library')
    paths = set()
    for line in result.stdout.splitlines():
        match = re.search(r'(?:=>\s+)?(/\S+)\s+\(', line)
        if match:
            path = Path(match.group(1))
            if not path.is_file():
                raise ValueError(f'bash dependency is missing: {path}')
            paths.add(path)
    if not paths:
        static_executable(shell)
    return sorted(paths)


@dataclass(frozen=True)
class EnvdConfig:
    """Guest envd executable and the shell identity it requires."""

    binary: Path
    bash: Path = Path('/bin/bash')
    default_user: str = 'user'


def build(binary, output, busybox=Path('/usr/bin/busybox'), envd=None):
    if output.exists():
        raise ValueError('output already exists')
    artifacts = [binary, busybox]
    if envd is not None:
        artifacts.append(envd.binary)
    for path in artifacts:
        static_executable(path)
    output.parent.mkdir(parents=True, exist_ok=True)
    with tempfile.TemporaryDirectory(prefix='adx-runtime-') as temporary:
        root = Path(temporary)
        # EROFS preserves the source root mode; mkdtemp defaults to 0700.
        # The immutable payload must also be executable by non-root workloads.
        root.chmod(0o755)
        for name in (
            'usr/bin',
            'usr/local/bin',
            'bin',
            'sbin',
            'etc',
            'tmp',
            'var/tmp',
            'root',
            'home',
            'proc',
            'sys',
            'dev',
            '__adx',
        ):
            (root / name).mkdir(parents=True, exist_ok=True)
        payloads = [(binary, 'usr/local/bin/adx-execd'), (busybox, 'bin/busybox')]
        if envd is not None:
            payloads.append((envd.binary, 'usr/local/bin/envd'))
            if not re.fullmatch(r'[a-z_][a-z0-9_-]{0,31}', envd.default_user):
                raise ValueError('invalid envd default user')
            payloads.append((envd.bash, 'bin/bash'))
            payloads.extend((path, str(path).lstrip('/')) for path in shell_dependencies(envd.bash))
        for source, target in payloads:
            (root / target).parent.mkdir(parents=True, exist_ok=True)
            shutil.copyfile(source, root / target)
            (root / target).chmod(0o755)
        if envd is not None:
            digest = hashlib.sha256(envd.binary.read_bytes()).hexdigest()
            (root / 'etc/adx-envd-manifest.json').write_text(json.dumps({
                'path': '/usr/local/bin/envd',
                'sha256': digest,
                'shell': '/bin/bash',
                'default_user': envd.default_user,
                'dependencies': {target: hashlib.sha256(source.read_bytes()).hexdigest()
                                 for source, target in payloads},
            }, sort_keys=True) + '\n')
        for app in subprocess.check_output([str(busybox), '--list'], text=True).splitlines():
            if app != 'busybox' and not (root / 'bin' / app).exists():
                (root / 'bin' / app).symlink_to('busybox')
        for name in ('usr', 'bin', 'sbin', 'etc', 'home', 'root', 'lib', 'lib64'):
            (root / '__adx' / name).symlink_to('/' + name)
        (root / 'tmp').chmod(0o1777)
        (root / 'var/tmp').chmod(0o1777)
        (root / 'etc/passwd').write_text('root:x:0:0:root:/root:/bin/sh\n')
        (root / 'etc/group').write_text('root:x:0:\n')
        if envd is not None and envd.default_user != 'root':
            home = root / 'home' / envd.default_user
            home.mkdir()
            os.chown(home, 1000, 1000)
            with (root / 'etc/passwd').open('a') as f:
                f.write(f'{envd.default_user}:x:1000:1000:E2B user:/home/{envd.default_user}:/bin/bash\n')
            with (root / 'etc/group').open('a') as f:
                f.write(f'{envd.default_user}:x:1000:\n')
        certs = Path('/etc/ssl/certs/ca-certificates.crt')
        if certs.is_file():
            (root / 'etc/ssl/certs').mkdir(parents=True)
            shutil.copyfile(certs, root / 'etc/ssl/certs/ca-certificates.crt')
        # mkdir applies the builder's umask to intermediate directories too.
        # Normalize the staged tree without following its absolute guest symlinks.
        for directory, _, _ in os.walk(root, followlinks=False):
            path = Path(directory)
            path.chmod(0o1777 if path in (root / 'tmp', root / 'var/tmp') else 0o755)
        subprocess.run([_executable('mkfs.erofs'), '-E', 'noinline_data', str(output), str(root)], check=True)
        subprocess.run([_executable('fsck.erofs'), str(output)], check=True)


if __name__ == '__main__':
    p = argparse.ArgumentParser()
    p.add_argument('--binary', type=Path, required=True)
    p.add_argument('--envd-binary', type=Path)
    p.add_argument('--bash', type=Path, default=Path('/bin/bash'))
    p.add_argument('--envd-default-user', default='user')
    p.add_argument('--output', type=Path, required=True)
    a = p.parse_args()
    envd = EnvdConfig(a.envd_binary, a.bash, a.envd_default_user) if a.envd_binary else None
    build(a.binary, a.output, envd=envd)
