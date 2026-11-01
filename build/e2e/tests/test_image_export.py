import importlib.util
from pathlib import Path
import unittest

SPEC = importlib.util.spec_from_file_location('fixed_export', Path(__file__).resolve().parents[1] / 'export_images.py')
EXPORT = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(EXPORT)

class ImageExportTests(unittest.TestCase):
    def test_exact_image_id_architecture_and_registry_digest_are_all_required(self):
        ref = 'registry.example/image@sha256:' + 'a' * 64
        image = {'Id': 'sha256:' + 'b' * 64, 'Architecture': 'amd64', 'Os': 'linux', 'RepoDigests': [ref]}
        EXPORT.verify_image(image, image['Id'], 'amd64', ref)
        for replacement in ({'Id': 'wrong'}, {'Architecture': 'arm64'}, {'RepoDigests': []}, {'Os': 'other'}):
            with self.subTest(replacement=replacement), self.assertRaises(ValueError):
                EXPORT.verify_image(dict(image, **replacement), image['Id'], 'amd64', ref)
