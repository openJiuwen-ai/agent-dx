import importlib.util
from pathlib import Path
import unittest
spec=importlib.util.spec_from_file_location('backend_builder',Path(__file__).resolve().parents[1]/'build_backend.py')
backend=importlib.util.module_from_spec(spec);spec.loader.exec_module(backend)

class ReleaseChecksumTests(unittest.TestCase):
    def test_blank_lines_comments_and_other_architectures(self):
        digest='a'*64
        manifest='\n# release checksums\n\n'+'b'*64+'  runc.arm64\n\n'+digest+' *runc.amd64\n'
        self.assertEqual(backend.release_checksum(manifest,'runc.amd64'),digest)
    def test_missing_malformed_or_duplicate_checksum_is_rejected(self):
        for manifest in ('\n','bad  runc.amd64\n',('a'*64+'  runc.amd64\n')*2):
            with self.subTest(manifest=manifest), self.assertRaises(ValueError):
                backend.release_checksum(manifest,'runc.amd64')
