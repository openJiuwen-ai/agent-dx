"""Default rootfs preserves the pinned distribution and non-root runtime access."""
import hashlib
import importlib.util
import io
import json
import os
from pathlib import Path
import shutil
import stat
import subprocess
import sys
import tarfile
import tempfile
import unittest
from unittest.mock import patch
import zlib

SPEC = importlib.util.spec_from_file_location("runtime_rootfs", Path(__file__).parents[1] / "rootfs.py")
ROOTFS = importlib.util.module_from_spec(SPEC)
sys.modules[SPEC.name] = ROOTFS
SPEC.loader.exec_module(ROOTFS)
TARGET = 'x86_64-unknown-linux-gnu'


def fixture(base):
    archive = base / 'base.tar.gz'
    with tarfile.open(archive, 'w:gz') as tar:
        for name in ['usr', 'usr/bin', 'usr/local', 'usr/local/bin', 'etc', 'root']:
            entry = tarfile.TarInfo(name)
            entry.type = tarfile.DIRTYPE
            entry.mode = 0o700 if name == 'root' else 0o755
            entry.uid, entry.gid = os.getuid(), os.getgid()
            tar.addfile(entry)
        for name, data in [('etc/os-release', b'ID="openEuler"\nVERSION_ID="24.03"\n'),
                           ('etc/passwd', b'root:x:0:0:root:/root:/bin/bash\n'),
                           ('etc/group', b'root:x:0:\n'), ('usr/bin/bash', b'distribution-bash'),
                           ('usr/bin/chown', b'distribution-chown')]:
            entry = tarfile.TarInfo(name)
            entry.size = len(data)
            entry.mode = 0o755 if name.startswith('usr/bin') else 0o644
            entry.uid, entry.gid = os.getuid(), os.getgid()
            tar.addfile(entry, io.BytesIO(data))
        entry = tarfile.TarInfo('bin')
        entry.type, entry.linkname = tarfile.SYMTYPE, 'usr/bin'
        entry.uid, entry.gid = os.getuid(), os.getgid()
        tar.addfile(entry)
    pin = {'schema_version': 1, 'distribution': 'openEuler', 'version': '24.03 LTS',
           'image_reference': 'docker.io/openeuler/openeuler:24.03-lts',
           'artifacts': {TARGET: {'url': 'https://example.invalid/base.tar.gz',
               'bytes': archive.stat().st_size, 'sha256': hashlib.sha256(archive.read_bytes()).hexdigest(),
               'manifest_digest': 'sha256:' + '1' * 64}}}
    source = base / 'source.json'
    source.write_text(json.dumps(pin))
    binary = base / 'execd'
    binary.write_bytes(b'\x7fELF\x02\x01' + bytes(12) + (62).to_bytes(2, 'little'))
    return source, archive, binary


class RuntimeRootfsTests(unittest.TestCase):
    def test_reuse_checks_inputs_and_previous_payload(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            source, archive, binary = fixture(root)
            envd_binary = root / 'envd'
            envd_binary.write_bytes(b'original envd')
            envd = ROOTFS.EnvdConfig(envd_binary, default_user='root')
            mkfs = root / 'mkfs.erofs'
            mkfs.write_bytes(b'pinned formatter')
            component = root / 'execd-component'
            component.mkdir()
            first = component / 'adx-runtime-rootfs.img'
            candidate = root / 'adx-execd.tar.gz'
            release = root / 'first-rootfs.json'
            reference = 'https://example.invalid/adx-execd.tar.gz'
            previous_commit, current_commit = 'a' * 40, 'b' * 40
            formatter_runs = []
            extract_runs = []
            candidate_timeout = False
            real_run = subprocess.run

            def package(commit):
                real_run([sys.executable, str(ROOTFS.REPO / 'build/release/component.py'), 'create',
                          '--component', 'execd', '--directory', str(component), '--commit', commit,
                          '--target', TARGET, '--rootfs-metadata', str(release)], check=True)
                with tarfile.open(candidate, 'w:gz') as tar:
                    tar.add(component, arcname='.')

            guest = {'adx-execd': binary.read_bytes(), 'envd': envd_binary.read_bytes()}
            guest_modes = {'adx-execd': 0o755, 'envd': 0o755}

            def run(command, **kwargs):
                if command[0] == str(mkfs):
                    formatter_runs.append(command)
                    Path(command[-2]).write_bytes(f'EROFS {len(formatter_runs)}'.encode())
                elif command[0] == str(root / 'fsck.erofs'):
                    if command[-2].startswith('--extract='):
                        extract_runs.append(command)
                        guest_bin = Path(command[-2].split('=', 1)[1]) / 'usr/local/bin'
                        guest_bin.mkdir(parents=True)
                        for name, contents in guest.items():
                            mode = guest_modes.get(name)
                            self.assertIsNotNone(mode, f'missing test permissions for {name}')
                            (guest_bin / name).write_bytes(contents)
                            (guest_bin / name).chmod(mode)
                elif command[0] == str(root / 'curl'):
                    self.assertEqual(kwargs.get('timeout'), 120)
                    if candidate_timeout:
                        raise subprocess.TimeoutExpired(command, kwargs['timeout'])
                    shutil.copyfile(candidate, Path(command[command.index('--output') + 1]))
                else:
                    real_run(command, **kwargs)

            def build_image(name, digest, config=envd):
                image = root / f'{name}.img'
                ROOTFS.build(binary, image, base_archive=archive, target=TARGET, envd=config,
                             reuse_url=reference, reuse_sha256=digest)
                return image, json.loads(image.with_suffix('.json').read_text())

            with patch.object(ROOTFS, 'SOURCE', source), patch.object(ROOTFS, 'static_executable'), \
                 patch.object(ROOTFS, '_executable', side_effect=lambda name: str(root / name)), \
                 patch.object(ROOTFS.subprocess, 'run', side_effect=run):
                ROOTFS.build(binary, first, base_archive=archive, target=TARGET, envd=envd)
                first.with_suffix('.json').rename(release)
                shutil.copyfile(binary, component / 'adx-execd')
                (component / 'envd-LICENSE').write_text('test license')
                (component / 'envd-manifest.json').write_text(json.dumps({'sha256': ROOTFS.sha256(envd_binary)}))
                package(previous_commit)
                digest = ROOTFS.sha256(candidate)
                reused, metadata = build_image('reused', digest)
                self.assertEqual(reused.read_bytes(), first.read_bytes())
                self.assertEqual(len(formatter_runs), 1)
                self.assertEqual(len(extract_runs), 1)
                self.assertEqual(metadata['reused_from'], {'commit': previous_commit, 'archive_sha256': digest})
                candidate_timeout = True
                image, metadata = build_image('timed-out-download', digest)
                self.assertNotEqual(image.read_bytes(), first.read_bytes())
                self.assertNotIn('reused_from', metadata)
                candidate_timeout = False

                valid_archive = candidate.read_bytes()
                malformed_tar = io.BytesIO()
                with tarfile.open(fileobj=malformed_tar, mode='w') as tar:
                    entry = tarfile.TarInfo('adx-execd')
                    entry.size = 40960
                    tar.addfile(entry, io.BytesIO(b'x' * entry.size))
                compressor = zlib.compressobj(level=0, wbits=31)
                prefix = compressor.compress(malformed_tar.getvalue()[:20480])
                # A valid gzip prefix opens successfully, then an invalid deflate block fails during extraction.
                candidate.write_bytes(prefix + compressor.flush(zlib.Z_SYNC_FLUSH) + b'\x07')
                self.assertNotIn('reused_from', build_image('corrupt-deflate', ROOTFS.sha256(candidate))[1])
                candidate.write_bytes(valid_archive)

                for name, path in (('execd', binary), ('envd', envd_binary), ('recipe', mkfs)):
                    with self.subTest(changed=name):
                        old = path.read_bytes()
                        path.write_bytes(old + b'changed')
                        image, metadata = build_image(name, digest)
                        self.assertNotEqual(image.read_bytes(), first.read_bytes())
                        self.assertNotIn('reused_from', metadata)
                        path.write_bytes(old)

                for name, sha, config in [('bad-checksum', '0' * 64, envd),
                                          ('different-user', digest, ROOTFS.EnvdConfig(envd_binary, 'other'))]:
                    with self.subTest(candidate=name), patch.object(ROOTFS, 'install_envd'):
                        self.assertNotIn('reused_from', build_image(name, sha, config)[1])
                raw_pin = source.read_text()
                pin = json.loads(raw_pin)
                pin['artifacts'][TARGET]['manifest_digest'] = 'sha256:' + '2' * 64
                source.write_text(json.dumps(pin))
                self.assertNotIn('reused_from', build_image('different-base', digest)[1])
                source.write_text(raw_pin)

                guest['envd'] = b'different embedded envd'
                self.assertNotIn('reused_from', build_image('bad-guest', digest)[1])
                guest['envd'] = envd_binary.read_bytes()
                for name in guest_modes:
                    with self.subTest(non_executable=name):
                        guest_modes[name] = 0o644
                        self.assertNotIn('reused_from', build_image(f'wrong-mode-{name}', digest)[1])
                        guest_modes[name] = 0o755

                shutil.copyfile(reused, first)
                reused.with_suffix('.json').rename(release)
                package(current_commit)
                relabeled, metadata = build_image('reused-again', ROOTFS.sha256(candidate))
                self.assertEqual(relabeled.read_bytes(), reused.read_bytes())
                self.assertEqual(metadata['reused_from']['commit'], current_commit)

                metadata.pop('recipe_sha256')
                release.write_text(json.dumps(metadata))
                package(previous_commit)
                self.assertNotIn('reused_from', build_image('legacy', ROOTFS.sha256(candidate))[1])

    def test_reuse_requires_both_pins(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            source, archive, binary = fixture(root)
            envd = ROOTFS.EnvdConfig(binary, default_user='root')
            with patch.object(ROOTFS, 'SOURCE', source), patch.object(ROOTFS, 'static_executable'):
                with self.assertRaisesRegex(ValueError, 'HTTPS archive URL and its lowercase SHA-256'):
                    ROOTFS.build(binary, root / 'out.img', base_archive=archive, target=TARGET,
                                 envd=envd, reuse_url='https://example.invalid/adx-execd.tar.gz')

    def test_distribution_and_non_root_permissions_survive_builder_umask(self):
        for mask in (0o022, 0o027, 0o077):
            with self.subTest(umask=oct(mask)):
                self.check_builder_umask(mask)

    def check_builder_umask(self, mask):
        with tempfile.TemporaryDirectory() as directory:
            base = Path(directory)
            source, archive, binary = fixture(base)
            observed = []

            def inspect_image(command, **_kwargs):
                if command[0] != '/mock/mkfs.erofs':
                    return
                root = Path(command[-1])
                self.assertEqual((root / 'bin/bash').read_bytes(), b'distribution-bash')
                self.assertEqual((root / 'bin/chown').read_bytes(), b'distribution-chown')
                self.assertEqual(stat.S_IMODE((root / 'root').stat().st_mode), 0o700)
                for name in ['.', 'usr', 'usr/local', 'usr/local/bin', '__adx']:
                    self.assertEqual(stat.S_IMODE((root / name).stat().st_mode), 0o755, name)
                self.assertEqual(stat.S_IMODE((root / 'tmp').stat().st_mode), 0o1777)
                self.assertEqual((root / '__adx/usr').readlink(), Path('/usr'))
                self.assertEqual((root / 'usr/local/bin/adx-execd').read_bytes(), binary.read_bytes())
                observed.append(True)
                Path(command[-2]).write_bytes(b'EROFS fixture')

            old = os.umask(mask)
            try:
                with patch.object(ROOTFS, 'SOURCE', source), patch.object(ROOTFS, 'static_executable'), \
                     patch.object(ROOTFS, '_executable', side_effect=lambda name: str(Path('/mock') / name)), \
                     patch.object(ROOTFS.subprocess, 'run', side_effect=inspect_image):
                    ROOTFS.build(binary, base / 'runtime.img', base_archive=archive, target=TARGET)
            finally:
                os.umask(old)
            self.assertTrue(observed)
            metadata = json.loads((base / 'runtime.json').read_text())
            self.assertEqual(metadata['distribution'], 'openEuler')
            self.assertEqual(metadata['rootfs_sha256'], hashlib.sha256(b'EROFS fixture').hexdigest())

    def test_base_checksum_is_checked_before_extracting(self):
        with tempfile.TemporaryDirectory() as directory:
            base = Path(directory)
            source, archive, binary = fixture(base)
            archive.write_bytes(archive.read_bytes()[:-1] + b'x')
            with patch.object(ROOTFS, 'SOURCE', source), patch.object(ROOTFS, 'static_executable'):
                with self.assertRaisesRegex(ValueError, 'checksum'):
                    ROOTFS.build(binary, base / 'rootfs.img', base_archive=archive, target=TARGET)
            self.assertFalse((base / 'rootfs.img').exists())

    def test_archive_cannot_write_through_an_absolute_guest_symlink(self):
        with tempfile.TemporaryDirectory() as directory:
            base = Path(directory)
            archive = base / 'unsafe.tar'
            with tarfile.open(archive, 'w') as tar:
                link = tarfile.TarInfo('etc')
                link.type, link.linkname = tarfile.SYMTYPE, str(base / 'escaped')
                tar.addfile(link)
                child = tarfile.TarInfo('etc/owned')
                child.size = 1
                tar.addfile(child, io.BytesIO(b'x'))
            root = base / 'root'
            root.mkdir()
            with self.assertRaisesRegex(ValueError, 'symlink'):
                ROOTFS.extract_base(archive, root)
            self.assertFalse((base / 'escaped').exists())

    def test_archive_parent_traversal_is_rejected(self):
        with tempfile.TemporaryDirectory() as directory:
            base = Path(directory)
            archive = base / 'unsafe.tar'
            with tarfile.open(archive, 'w') as tar:
                entry = tarfile.TarInfo('../escaped')
                entry.size = 1
                tar.addfile(entry, io.BytesIO(b'x'))
            with self.assertRaisesRegex(ValueError, 'archive'):
                ROOTFS.extract_base(archive, base)

    def test_archive_hardlink_chain_preserves_file_contents(self):
        with tempfile.TemporaryDirectory() as directory:
            base = Path(directory)
            archive = base / 'hardlinks.tar'
            with tarfile.open(archive, 'w') as tar:
                entry = tarfile.TarInfo('file')
                entry.size = 4
                tar.addfile(entry, io.BytesIO(b'data'))
                for name, target in [('alias', 'file'), ('chain', 'alias')]:
                    entry = tarfile.TarInfo(name)
                    entry.type, entry.linkname = tarfile.LNKTYPE, target
                    tar.addfile(entry)
            root = base / 'root'
            ROOTFS.extract_base(archive, root)
            self.assertEqual((root / 'chain').read_bytes(), b'data')
            self.assertEqual((root / 'chain').stat().st_ino, (root / 'file').stat().st_ino)

    def test_archive_invalid_hardlink_targets_are_rejected_before_extraction(self):
        cases = [('missing', [('alias', 'absent')]),
                 ('cycle', [('alias', 'other'), ('other', 'alias')]),
                 ('self', [('alias', 'alias')])]
        for name, links in cases:
            with self.subTest(name=name), tempfile.TemporaryDirectory() as directory:
                base = Path(directory)
                archive = base / 'hardlinks.tar'
                with tarfile.open(archive, 'w') as tar:
                    for link, target in links:
                        entry = tarfile.TarInfo(link)
                        entry.type, entry.linkname = tarfile.LNKTYPE, target
                        tar.addfile(entry)
                root = base / 'root'
                with self.assertRaisesRegex(ValueError, 'invalid base archive hardlink'):
                    ROOTFS.extract_base(archive, root)
                self.assertFalse(root.exists())
