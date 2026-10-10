"""Fail-closed full qualification without a developer-specific lab profile."""
from __future__ import annotations

import hashlib
import json
from pathlib import Path
import tempfile
import unittest

import environment


class EnvironmentQualificationTests(unittest.TestCase):
    def make_evidence(self, root: Path, value: object = None) -> tuple[dict, Path]:
        data = json.dumps({} if value is None else value).encode()
        (root / "bundle.json").write_bytes(data)
        lock = {"environment_evidence": {"path": "bundle.json", "sha256": hashlib.sha256(data).hexdigest()}}
        return lock, root / "lock.json"

    def test_missing_and_malformed_lock_are_blocked(self):
        for value in (None, [], {}, {"environment_evidence": []}):
            with self.subTest(value=value):
                self.assertTrue(environment.qualification_errors(value, Path("lock.json")))

    def test_valid_bundle_cannot_grant_full_qualification(self):
        with tempfile.TemporaryDirectory() as tmp:
            lock, path = self.make_evidence(Path(tmp), {"status": "PASS", "checks": []})
            errors = environment.qualification_errors(lock, path)
            self.assertEqual(errors, ["portable full environment verifier is not implemented; full acceptance remains BLOCKED"])

    def test_missing_or_tampered_bundle_is_rejected(self):
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            lock, path = self.make_evidence(root)
            (root / "bundle.json").write_text("changed")
            self.assertIn("sha256 mismatch", environment.qualification_errors(lock, path)[0])
            (root / "bundle.json").unlink()
            self.assertIn("missing", environment.qualification_errors(lock, path)[0])

    def test_absolute_traversal_and_symlink_escape_are_rejected(self):
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            run = root / "run"
            run.mkdir()
            lock, _ = self.make_evidence(root)
            (run / "outside.json").symlink_to(root / "bundle.json")
            for value in ("../bundle.json", str(root / "bundle.json"), "outside.json", "\x00"):
                with self.subTest(value=value):
                    lock["environment_evidence"]["path"] = value
                    self.assertIn("escapes", environment.qualification_errors(lock, run / "lock.json")[0])

    def test_bad_hash_and_non_object_json_are_rejected(self):
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            lock, path = self.make_evidence(root)
            for value in ("z" * 64, "0" * 63, 123):
                with self.subTest(value=value):
                    lock["environment_evidence"]["sha256"] = value
                    self.assertIn("sha256", environment.qualification_errors(lock, path)[0])
            for value in ([], "PASS", None):
                data = json.dumps(value).encode()
                (root / "bundle.json").write_bytes(data)
                lock["environment_evidence"]["sha256"] = hashlib.sha256(data).hexdigest()
                self.assertIn("object", environment.qualification_errors(lock, path)[0])

    def test_cli_blocks_and_preserves_an_existing_result(self):
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            lock, path = self.make_evidence(root)
            path.write_text(json.dumps(lock))
            output = root / "result.json"
            args = ["--lock", str(path), "--output", str(output)]
            self.assertEqual(environment.main(args), 2)
            result = json.loads(output.read_text())
            self.assertEqual(result["status"], "BLOCKED")
            self.assertFalse(result["full_release_gate_pass"])
            before = output.read_bytes()
            with self.assertRaises(FileExistsError):
                environment.main(args)
            self.assertEqual(output.read_bytes(), before)

    def test_malformed_json_is_rejected(self):
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            lock, path = self.make_evidence(root)
            data = b'{broken'
            (root / "bundle.json").write_bytes(data)
            lock["environment_evidence"]["sha256"] = hashlib.sha256(data).hexdigest()
            self.assertIn("malformed", environment.qualification_errors(lock, path)[0])


if __name__ == "__main__":
    unittest.main()
