#!/usr/bin/env python3
from __future__ import annotations

import importlib.util
import json
import os
import tempfile
import unittest
from pathlib import Path
from unittest import mock

SCRIPT = Path(__file__).with_name("bind-two-node-localfile.py")
spec = importlib.util.spec_from_file_location("bind_two_node_localfile", SCRIPT)
module = importlib.util.module_from_spec(spec)
assert spec.loader is not None
spec.loader.exec_module(module)


class BindTwoNodeLocalFileTests(unittest.TestCase):
    def test_fresh_root_records_preflight_dependency_failure(self) -> None:
        with tempfile.TemporaryDirectory() as td:
            base = Path(td)
            repo = base / "repo"
            (repo / "afs").mkdir(parents=True)
            (repo / "Cargo.toml").write_text("[workspace]\n")
            (repo / "afs/Cargo.toml").write_text("[package]\nname='afs'\nversion='0.0.0'\nedition='2024'\n")
            binaries = base / "bin"
            binaries.mkdir()
            for name in ("afs-meta", "afs-node"):
                exe = binaries / name
                exe.write_text("#!/bin/sh\nexit 0\n")
                exe.chmod(0o755)
            root = base / "fresh-root"
            scenario = module.Scenario(repo, binaries, root, False)
            with mock.patch.object(module.platform, "system", return_value="Linux"), \
                 mock.patch.object(module.os, "geteuid", return_value=0), \
                 mock.patch.object(module.shutil, "which", side_effect=lambda tool: None if tool == "fusermount3" else f"/usr/bin/{tool}"):
                with self.assertRaises(AssertionError):
                    scenario.preflight()
            checks = json.loads((root / "checks.json").read_text())
            self.assertEqual(checks[-1]["name"], "host tools")
            self.assertFalse(checks[-1]["passed"])
            self.assertIsNone(checks[-1]["detail"]["fusermount3"])

    def test_successful_preflight_creates_root_and_records_checks(self) -> None:
        with tempfile.TemporaryDirectory() as td:
            base = Path(td)
            repo = base / "repo"
            (repo / "afs").mkdir(parents=True)
            (repo / "Cargo.toml").write_text("[workspace]\n")
            (repo / "afs/Cargo.toml").write_text("[package]\nname='afs'\nversion='0.0.0'\nedition='2024'\n")
            binaries = base / "bin"
            binaries.mkdir()
            for name in ("afs-meta", "afs-node"):
                exe = binaries / name
                exe.write_text("#!/bin/sh\nexit 0\n")
                exe.chmod(0o755)
            root = base / "fresh-root"
            scenario = module.Scenario(repo, binaries, root, False)

            class FakeSocket:
                def __enter__(self):
                    return self

                def __exit__(self, exc_type, exc, tb):
                    return False

                def bind(self, address):
                    self.address = address

            with mock.patch.object(module.platform, "system", return_value="Linux"), \
                 mock.patch.object(module.os, "geteuid", return_value=0), \
                 mock.patch.object(module.shutil, "which", side_effect=lambda tool: f"/usr/bin/{tool}"), \
                 mock.patch.object(module.shutil, "disk_usage", return_value=mock.Mock(free=2 * 1024**3)), \
                 mock.patch.object(module.socket, "socket", side_effect=lambda: FakeSocket()):
                scenario.preflight()

            checks = json.loads((root / "checks.json").read_text())
            self.assertTrue(root.is_dir())
            self.assertEqual(checks[-1]["name"], "afs-node executable")
            self.assertTrue(all(item["passed"] for item in checks))

    def test_existing_root_is_rejected_without_overwriting_sentinel(self) -> None:
        with tempfile.TemporaryDirectory() as td:
            base = Path(td)
            root = base / "existing"
            root.mkdir()
            sentinel = root / "checks.json"
            sentinel.write_text("sentinel")
            scenario = module.Scenario(base / "repo", base / "bin", root, False)
            with mock.patch.object(module.platform, "system", return_value="Linux"), \
                 mock.patch.object(module.os, "geteuid", return_value=0):
                with self.assertRaises(module.PreflightError):
                    scenario.preflight()
            self.assertEqual(sentinel.read_text(), "sentinel")

    def test_health_requires_ready_json_not_only_http_200(self) -> None:
        self.assertEqual(module.health_ready(200, '{"status":"starting"}')[0], False)
        self.assertEqual(module.health_ready(503, '{"status":"ready"}')[0], False)
        self.assertEqual(module.health_ready(200, '{"status":"ready"}')[0], True)
        self.assertEqual(module.health_ready(200, 'null')[0], False)
        self.assertEqual(module.health_ready(200, '[{"status":"ready"}]')[0], False)


if __name__ == "__main__":
    unittest.main()
