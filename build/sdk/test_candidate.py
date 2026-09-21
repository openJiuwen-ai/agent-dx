import importlib.util
from pathlib import Path
import tempfile
import unittest
import zipfile


MODULE_PATH = Path(__file__).with_name('candidate.py')
SPEC = importlib.util.spec_from_file_location('sdk_candidate', MODULE_PATH)
candidate = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(candidate)


class CandidateTests(unittest.TestCase):
    def test_candidate_records_and_verifies_both_distributions(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            wheel = root / 'adx_sandbox-0.1.0-py3-none-any.whl'
            with zipfile.ZipFile(wheel, 'w') as archive:
                archive.writestr(
                    'adx_sandbox-0.1.0.dist-info/METADATA',
                    'Name: adx-sandbox\nVersion: 0.1.0\nRequires-Python: >=3.10\n',
                )
            (root / 'adx_sandbox-0.1.0.tar.gz').write_bytes(b'source')
            created = candidate.create(root, 'a' * 40, 'build-id')
            self.assertEqual(created['version'], '0.1.0')
            self.assertEqual(candidate.verify(root), created)
            wheel.write_bytes(b'changed')
            with self.assertRaisesRegex(ValueError, 'digest'):
                candidate.verify(root)

    def test_candidate_rejects_incomplete_or_wrong_package(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            with self.assertRaisesRegex(ValueError, 'one wheel'):
                candidate.create(root, 'a' * 40)


if __name__ == '__main__':
    unittest.main()
