"""Package frozen Linux FUSE test ELFs as test assets, outside product releases."""

import argparse
import hashlib
import json
import platform
from pathlib import Path
import shutil
import subprocess
import tarfile

ROOT = Path(__file__).resolve().parents[2]
SELECTORS = (
    'fuse_ftruncate_on_same_handle_waits_for_prior_write',
    'dfs_buffered_reopen_refreshes_growth_and_truncation_with_new_heads',
    'fuse_normal_join_rejects_busy_mount_without_detaching',
    'real_dfs_keep_cache_contract::dfs_real_nodes_cross_handle_keep_cache_and_permission',
)


def sha(path):
    digest = hashlib.sha256()
    with Path(path).open('rb') as stream:
        for block in iter(lambda: stream.read(1024 * 1024), b''):
            digest.update(block)
    return digest.hexdigest()


def executable(name):
    value = shutil.which(name)
    if value is None:
        raise FileNotFoundError('required tool: ' + name)
    # Rustup dispatches by argv[0]; resolving cargo's symlink invokes rustup itself.
    return str(Path(value).absolute())


def elf(path, target):
    with Path(path).open('rb') as stream:
        header = stream.read(20)
    machine = {'x86_64-unknown-linux-gnu': 62, 'aarch64-unknown-linux-gnu': 183}.get(target)
    if (len(header) != 20 or header[:6] != b'\x7fELF\x02\x01'
            or int.from_bytes(header[18:20], 'little') != machine):
        raise ValueError('test ELF architecture differs from candidate')


def validate(manifest, package):
    expected = {name: package.get('files', {}).get('bin/' + name) for name in ('afs-meta', 'afs-node')}
    matching = (
        package.get('with_afs') is True, manifest.get('schema_version') == 1,
        manifest.get('commit') == package.get('commit'), manifest.get('target') == package.get('target'),
        manifest.get('binaries') == expected, all(expected.values()),
    )
    if not all(matching):
        raise ValueError('test contracts belong to another product source/target/binary')
    if tuple(manifest.get('selectors', ())) != SELECTORS:
        raise ValueError('incomplete exact Rust contract selectors')
    if manifest.get('test_elf', {}).get('name') != 'fuse_contract':
        raise ValueError('unexpected test ELF name')
    if manifest.get('archive', {}).get('name') != 'fuse-contract.tar.gz':
        raise ValueError('unexpected test archive name')
    for item in (manifest.get('test_elf', {}), manifest.get('archive', {})):
        digest = item.get('sha256', '')
        if len(digest) != 64 or any(c not in '0123456789abcdef' for c in digest):
            raise ValueError('test asset digest is invalid')
    return manifest


def pack(source, destination, package):
    source, destination = Path(source), Path(destination)
    elf(source, package['target'])
    destination.mkdir(parents=True, exist_ok=False)
    archive = destination / 'fuse-contract.tar.gz'
    with tarfile.open(archive, 'w:gz') as output:
        info = tarfile.TarInfo('fuse_contract')
        info.size, info.mode = source.stat().st_size, 0o755
        with source.open('rb') as stream:
            output.addfile(info, stream)
    manifest = {'schema_version': 1, 'commit': package['commit'], 'target': package['target'],
                'binaries': {n: package['files']['bin/' + n] for n in ('afs-meta', 'afs-node')},
                'selectors': list(SELECTORS), 'test_elf': {'name': 'fuse_contract', 'sha256': sha(source)},
                'archive': {'name': archive.name, 'sha256': sha(archive), 'bytes': archive.stat().st_size}}
    validate(manifest, package)
    (destination / 'manifest.json').write_text(json.dumps(manifest, indent=2) + '\n')
    return manifest


def unpack(directory, package, destination):
    directory, destination = Path(directory), Path(destination)
    manifest = validate(json.loads((directory / 'manifest.json').read_text()), package)
    archive = directory / 'fuse-contract.tar.gz'
    if sha(archive) != manifest['archive']['sha256'] or archive.stat().st_size != manifest['archive']['bytes']:
        raise ValueError('test contract archive corrupted')
    destination.mkdir(parents=True, exist_ok=False)
    with tarfile.open(archive, 'r:gz') as stream:
        entries = stream.getmembers()
        if (len(entries) != 1 or entries[0].name != 'fuse_contract' or not entries[0].isfile()
                or entries[0].size > 512 * 1024**2):
            raise ValueError('unexpected test archive member')
        with stream.extractfile(entries[0]) as source, (destination / 'fuse_contract').open('wb') as output:
            shutil.copyfileobj(source, output)
    (destination / 'fuse_contract').chmod(0o755)
    (destination / 'manifest.json').write_text(json.dumps(manifest, indent=2) + '\n')
    return installed(destination, package)


def installed(directory, package):
    directory = Path(directory)
    if any(path.is_symlink() for path in (directory, directory / 'fuse_contract', directory / 'manifest.json')):
        raise ValueError('test asset symlinks are not accepted')
    manifest = validate(json.loads((directory / 'manifest.json').read_text()), package)
    elf(directory / 'fuse_contract', package['target'])
    if sha(directory / 'fuse_contract') != manifest['test_elf']['sha256']:
        raise ValueError('installed Rust test ELF differs from manifest')
    return manifest


def verify_bundle(directory, bundle, required=False):
    path = Path(directory) / 'afs-contracts.json'
    record = bundle.get('afs_contracts')
    if record is None:
        if path.exists() or path.is_symlink():
            raise ValueError('unbound Rust test manifest')
        if required:
            raise ValueError('AFS Full requires frozen fuse_contract assets; rebuild the candidate with CI4 tooling')
        return None
    if not isinstance(record, dict) or record.get('manifest') != 'afs-contracts.json':
        raise ValueError('invalid Rust test asset handoff')
    if path.is_symlink() or sha(path) != record.get('sha256'):
        raise ValueError('Rust test manifest handoff corrupted')
    return validate(json.loads(path.read_text()), bundle['package'])


def build(arguments):
    if platform.system() != 'Linux':
        raise ValueError('Rust test ELF compilation requires the Linux CI builder')
    head = subprocess.check_output([executable('git'), 'rev-parse', 'HEAD'], cwd=ROOT, text=True).strip()
    if head != arguments.commit or subprocess.check_output(
            [executable('git'), 'status', '--porcelain'], cwd=ROOT, text=True).strip():
        raise ValueError('test ELF compilation needs the exact clean product source')
    result = subprocess.run([executable('cargo'), 'test', '--locked', '-p', 'afs', '--test', 'fuse_contract',
                             '--no-run', '--message-format=json'], cwd=ROOT, check=True, stdout=subprocess.PIPE,
                            text=True, timeout=1800)
    records = [json.loads(line) for line in result.stdout.splitlines() if line.startswith('{')]
    matches = [Path(r['executable']) for r in records if r.get('reason') == 'compiler-artifact'
               and r.get('target', {}).get('name') == 'fuse_contract' and r.get('executable')]
    if len(matches) != 1:
        raise ValueError('one exact fuse_contract compiler artifact required')
    import tempfile
    with tempfile.TemporaryDirectory(prefix='adx-fuse-contract-') as temp:
        binary = Path(temp) / 'fuse_contract'
        shutil.copyfile(matches[0], binary)
        binary.chmod(0o755)
        subprocess.run([executable('strip'), '--strip-debug', str(binary)], check=True, timeout=120)
        for selector in SELECTORS:
            listed = subprocess.check_output([str(binary), '--list', '--exact', selector], text=True, timeout=30)
            if selector + ': test' not in listed:
                raise ValueError('missing exact test selector: ' + selector)
        package = {'with_afs': True, 'commit': head, 'target': arguments.target,
                   'files': {'bin/afs-meta': sha(arguments.meta), 'bin/afs-node': sha(arguments.node)}}
        manifest = pack(binary, arguments.output, package)
    print(json.dumps(manifest), flush=True)


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--commit', required=True)
    parser.add_argument('--target', required=True)
    parser.add_argument('--meta', required=True, type=Path)
    parser.add_argument('--node', required=True, type=Path)
    parser.add_argument('--output', required=True, type=Path)
    build(parser.parse_args())


if __name__ == '__main__':
    main()
