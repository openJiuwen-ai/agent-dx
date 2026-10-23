#!/usr/bin/env python3
"""Pure command/config tests; no SSH, services, mounts or filesystem acceptance."""

import contextlib
import importlib.util
import io
import shlex
import subprocess
import sys
import unittest
from pathlib import Path

RUNNER = Path(__file__).resolve().parents[1] / "ownerfs_acceptance.py"
SPEC = importlib.util.spec_from_file_location("ownerfs_meta_storage_under_test", RUNNER)
MODULE = importlib.util.module_from_spec(SPEC)
sys.modules[SPEC.name] = MODULE
SPEC.loader.exec_module(MODULE)


class OwnerFsMetaStorageTests(unittest.TestCase):
    def args(self, *extra):
        return MODULE.parse_args([
            "--host-b", "unused-b", "--host-c", "unused-c",
            "--bin-dir", "/tools with space", "--run-id", "storage-test",
            "--remote-tmp", "/tmp/ownerfs test", *extra,
        ])

    def test_start_and_restart_share_a_dedicated_local_file_directory(self):
        runner = MODULE.OwnerFsAcceptance(self.args("--meta-config", "/config with space/meta.toml"))
        for restart in [False, True]:
            argv = shlex.split(runner.meta_arguments(restart=restart))
            self.assertEqual(argv[argv.index("--meta-store") + 1], "local-file")
            self.assertEqual(argv[argv.index("--data-dir") + 1], runner.paths["C_ROOT"] + "/meta-data")
            self.assertEqual(argv[argv.index("--config") + 1], "/config with space/meta.toml")
            self.assertNotIn("--etcd-endpoint", argv)

    def test_preflight_checks_the_same_managed_local_file_arguments(self):
        runner = MODULE.OwnerFsAcceptance(self.args())
        commands = []

        def run(command, **_kwargs):
            commands.append(command)
            return subprocess.CompletedProcess(command, 0, "Linux\n" if command == "uname -s" else "", "")

        for role in runner.roles.values():
            role.run = run
        runner.preflight()
        checks = [command for command in commands if "--print-config" in command]
        self.assertEqual(len(checks), 3)
        for command in checks:
            argv = shlex.split(command)
            self.assertEqual(argv[argv.index("--meta-store") + 1], "local-file")
            self.assertEqual(argv[argv.index("--data-dir") + 1], runner.paths["C_ROOT"] + "/meta-data")
        self.assertEqual(runner.result["gaps"], [])

    def test_retired_etcd_runner_options_are_rejected(self):
        for args in [("--etcd-endpoint", "http://127.0.0.1:2379"), ("--allow-missing-etcd-for-scaffold",)]:
            with self.subTest(args=args), contextlib.redirect_stderr(io.StringIO()):
                with self.assertRaises(SystemExit) as failure:
                    self.args(*args)
                self.assertEqual(failure.exception.code, 2)


if __name__ == "__main__":
    unittest.main()
