import importlib.util
from pathlib import Path
import unittest


MODULE_PATH = Path(__file__).with_name("verify_index.py")
SPEC = importlib.util.spec_from_file_location("verify_index", MODULE_PATH)
verify_index = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(verify_index)


class IndexVerificationTests(unittest.TestCase):
    def test_requires_every_candidate_file_with_the_exact_digest(self):
        candidate = {
            "name": "adxadmin",
            "version": "0.1.0",
            "commit": "c" * 40,
            "files": {"adxadmin-0.1.0.whl": "a" * 64},
        }
        payload = {
            "info": {"name": "adxadmin", "version": "0.1.0"},
            "urls": [
                {
                    "filename": "adxadmin-0.1.0.whl",
                    "digests": {"sha256": "a" * 64},
                    "url": "https://files.example/adxadmin.whl",
                }
            ],
        }
        result = verify_index.verify_payload(payload, candidate, "pypi")
        self.assertEqual(result["status"], "published")
        payload["urls"][0]["digests"]["sha256"] = "b" * 64
        with self.assertRaisesRegex(ValueError, "digest"):
            verify_index.verify_payload(payload, candidate, "pypi")

    def test_rejects_files_outside_the_candidate(self):
        candidate = {
            "name": "adxadmin",
            "version": "0.1.0",
            "commit": "c" * 40,
            "files": {"adxadmin-0.1.0.tar.gz": "a" * 64},
        }
        payload = {
            "info": {"name": "adxadmin", "version": "0.1.0"},
            "urls": [
                {
                    "filename": "adxadmin-0.1.0.tar.gz",
                    "digests": {"sha256": "a" * 64},
                },
                {
                    "filename": "unexpected.whl",
                    "digests": {"sha256": "b" * 64},
                },
            ],
        }
        with self.assertRaisesRegex(ValueError, "file set"):
            verify_index.verify_payload(payload, candidate, "pypi")


if __name__ == "__main__":
    unittest.main()
