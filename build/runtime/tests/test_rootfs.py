"""Default rootfs preserves the pinned distribution and non-root runtime access."""
import hashlib
import importlib.util
import io
import json
import os
from pathlib import Path
import stat
import sys
import tarfile
import tempfile
import unittest
from unittest.mock import patch

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
    def test_distribution_and_non_root_permissions_survive_builder_umask(self):
        for mask in (0o022, 0o027, 0o077):
            with self.subTest(umask=oct(mask)), tempfile.TemporaryDirectory() as directory:
                base = Path(directory)
                source, archive, binary = fixture(base)
                observed = []

                def inspect_image(command, binary=binary, observed=observed, **_kwargs):
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
                         patch.object(ROOTFS, '_executable', side_effect=lambda name: '/mock/' + name), \
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
