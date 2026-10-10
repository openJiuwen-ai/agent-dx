#!/usr/bin/env python3
"""Regression coverage for compact fixture restoration."""

from __future__ import annotations

import json
import tempfile
import unittest
from pathlib import Path

import fixture_store


class FixtureStoreTests(unittest.TestCase):
    def test_manifest_restores_every_original_path_with_expected_hash(self):
        manifest = fixture_store.load_manifest()
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            network = fixture_store.restore("network-preparation", root / "network-preparation")
            verbs = fixture_store.restore("verbs-preparation", root / "verbs-preparation")
            self.assertEqual(len(network) + len(verbs), len(manifest["files"]))
            for entry in manifest["files"]:
                restored = root / entry["path"]
                self.assertTrue(restored.exists(), entry["path"])
                data = restored.read_bytes()
                self.assertEqual(fixture_store.sha256(data), entry["sha256"], entry["path"])
                self.assertEqual(len(data), entry["bytes"], entry["path"])

    def test_store_keeps_only_unique_content_objects(self):
        manifest = fixture_store.load_manifest()
        hashes = {entry["sha256"] for entry in manifest["files"]}
        stored = {path.name for path in fixture_store.STORE.iterdir() if path.is_file()}
        self.assertEqual(stored, hashes)
        self.assertLess(len(stored), len(manifest["files"]))

    def test_missing_prefix_is_rejected(self):
        with tempfile.TemporaryDirectory() as temporary:
            with self.assertRaises(ValueError):
                fixture_store.restore("missing", Path(temporary))

    def test_tampered_store_object_is_rejected(self):
        manifest = fixture_store.load_manifest()
        entry = manifest["files"][0]
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            fixtures = root / "fixtures"
            store = fixtures / "store"
            store.mkdir(parents=True)
            manifest_path = fixtures / "manifest.json"
            manifest_path.write_text(json.dumps(manifest) + "\n", encoding="utf-8")
            for digest in {item["sha256"] for item in manifest["files"]}:
                source = fixture_store.STORE / digest
                target = store / digest
                target.write_bytes(source.read_bytes())
            (store / entry["sha256"]).write_bytes(b"tampered")
            with self.assertRaises(ValueError):
                fixture_store.restore(entry["path"].split("/", 1)[0], root / "out", manifest_path=manifest_path)


if __name__ == "__main__":
    unittest.main()
