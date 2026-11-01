"""Frozen test ELF transport; no Rust compilation or FUSE execution here."""

import hashlib
import importlib.util
import json
from pathlib import Path
import tarfile
import tempfile
import unittest
from unittest.mock import patch

ROOT = Path(__file__).resolve().parents[3]


class ContractAssetsTests(unittest.TestCase):
    def setUp(self):
        spec = importlib.util.spec_from_file_location('contract_assets', ROOT / 'build/e2e/afs_contracts.py')
        self.assets = importlib.util.module_from_spec(spec)
        spec.loader.exec_module(self.assets)
        self.temp = tempfile.TemporaryDirectory()
        self.addCleanup(self.temp.cleanup)
        self.root = Path(self.temp.name)
        self.elf = self.root / 'test-elf'
        header = bytearray(64)
        header[:6] = b'\x7fELF\x02\x01'
        header[18:20] = (62).to_bytes(2, 'little')
        self.elf.write_bytes(header)
        self.package = {'with_afs': True, 'commit': 'a' * 40, 'target': 'x86_64-unknown-linux-gnu',
                        'files': {'bin/afs-meta': 'b' * 64, 'bin/afs-node': 'c' * 64}}
        self.directory = self.root / 'assets'
        self.manifest = self.assets.pack(self.elf, self.directory, self.package)

    def test_tool_lookup_preserves_multicall_proxy_name(self):
        proxy = self.root / 'cargo'
        proxy.symlink_to(self.elf)
        with patch.object(self.assets.shutil, 'which', return_value=str(proxy)):
            self.assertEqual(self.assets.executable('cargo'), str(proxy))

    def test_transport_is_separate_and_preserves_frozen_elf(self):
        dest = self.root / 'unpacked'
        self.assets.unpack(self.directory, self.package, dest)
        self.assertEqual((dest / 'fuse_contract').read_bytes(), self.elf.read_bytes())
        self.assertTrue((dest / 'fuse_contract').stat().st_mode & 0o111)
        self.assertEqual(set(p.name for p in self.directory.iterdir()), {'manifest.json', 'fuse-contract.tar.gz'})

    def test_foreign_product_source_binary_or_target_cannot_be_accepted(self):
        for package in (dict(self.package, commit='d' * 40),
                        dict(self.package, target='aarch64-unknown-linux-gnu'),
                        dict(self.package, files=dict(self.package['files'], **{'bin/afs-node': 'd' * 64})),
                        dict(self.package, with_afs=False)):
            with self.subTest(package=package), self.assertRaises(ValueError):
                self.assets.validate(self.manifest, package)

    def test_corrupt_archive_is_rejected_before_extraction(self):
        (self.directory / 'fuse-contract.tar.gz').write_bytes(b'corrupt')
        with self.assertRaises(ValueError):
            self.assets.unpack(self.directory, self.package, self.root / 'bad')

    def test_archive_rejects_missing_extra_or_unsafe_members(self):
        regular = tarfile.TarInfo('fuse_contract')
        other = tarfile.TarInfo('../outside')
        link = tarfile.TarInfo('fuse_contract')
        link.type, link.linkname = tarfile.SYMTYPE, '../outside'
        archive = self.directory / 'fuse-contract.tar.gz'
        for index, entries in enumerate(([], [regular, regular], [other], [link])):
            with self.subTest(index=index):
                with tarfile.open(archive, 'w:gz') as stream:
                    for entry in entries:
                        stream.addfile(entry)
                manifest = dict(self.manifest, archive={
                    'name': archive.name, 'sha256': self.assets.sha(archive), 'bytes': archive.stat().st_size})
                (self.directory / 'manifest.json').write_text(json.dumps(manifest))
                with self.assertRaises(ValueError):
                    self.assets.unpack(self.directory, self.package, self.root / str(index))
                self.assertFalse((self.root / str(index) / 'fuse_contract').exists())
                self.assertFalse((self.root / 'outside').exists())

    def test_cargo_artifact_selection_rejects_missing_and_duplicate_matches(self):
        artifact = {'reason': 'compiler-artifact', 'target': {'name': 'fuse_contract'},
                    'executable': '/target/fuse_contract-123'}
        unrelated = [dict(artifact, reason='compiler-message'),
                     dict(artifact, target={'name': 'another_test'}), dict(artifact, executable=None)]
        for count in (0, 1, 2):
            with self.subTest(count=count):
                stdout = 'cargo progress\n' + '\n'.join(json.dumps(item) for item in unrelated + [artifact] * count)
                if count == 1:
                    self.assertEqual(self.assets.compiler_artifact(stdout), Path(artifact['executable']))
                else:
                    with self.assertRaises(ValueError):
                        self.assets.compiler_artifact(stdout)

    def test_full_requires_an_explicit_small_bundle_handoff(self):
        bundle = {'package': self.package}
        self.assertIsNone(self.assets.verify_bundle(self.root, bundle))
        with self.assertRaises(ValueError):
            self.assets.verify_bundle(self.root, bundle, required=True)
        path = self.root / 'afs-contracts.json'
        path.write_text(json.dumps(self.manifest))
        bundle['afs_contracts'] = {'manifest': path.name, 'sha256': hashlib.sha256(path.read_bytes()).hexdigest()}
        self.assertEqual(self.assets.verify_bundle(self.root, bundle, required=True), self.manifest)

    def test_actual_installed_elf_must_match_small_manifest(self):
        dest = self.root / 'unpacked'
        self.assets.unpack(self.directory, self.package, dest)
        (dest / 'fuse_contract').write_bytes(b'foreign')
        with self.assertRaises(ValueError):
            self.assets.installed(dest, self.package)


if __name__ == '__main__':
    unittest.main()
