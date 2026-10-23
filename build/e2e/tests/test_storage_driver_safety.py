"""Failure-path contracts that do not require KVM or a mounted filesystem."""

import json
import os
from pathlib import Path
import subprocess
import sys
from tempfile import TemporaryDirectory
import unittest
from unittest.mock import patch

REPO = Path(__file__).resolve().parents[3]
sys.path.append(str(REPO / "build/e2e/firecracker"))

SHORT_COPY_PROBE = """
import os
from pathlib import Path
import runpy
import sys
from types import SimpleNamespace

class Fixture:
    metrics_port = 1

    def __init__(self, repo, binaries, root, evidence):
        self.root = root
        self.evidence = evidence

    def start(self):
        self.evidence.mkdir(parents=True)
        shared = self.root / "shared"
        shared.mkdir(parents=True)
        for node in ("node-a", "node-b"):
            parent = self.root / node / "mount"
            parent.mkdir(parents=True)
            (parent / "dfs").symlink_to(shared, target_is_directory=True)

    def stop(self):
        return []

calls = 0

def short_copy(*args, **kwargs):
    global calls
    calls += 1
    if calls > 1:
        raise RuntimeError("probe accepted the first short copy")
    return 0

sys.modules["filesystem_fixture"] = SimpleNamespace(FilesystemFixture=Fixture)
os.copy_file_range = short_copy
source, root = sys.argv[1:]
probe = runpy.run_path(source)
probe["main"].__globals__["read_payload"] = lambda port: 0
sys.argv = [source, "--binary-dir", root, "--root", root]
try:
    probe["main"]()
except AssertionError:
    if calls != 1:
        raise RuntimeError("probe did not reject the first short copy")
else:
    raise RuntimeError("optimized probe accepted corrupt capture evidence")
"""


class StorageDriverSafetyTests(unittest.TestCase):
    def test_cow_probe_rejects_short_copy_with_python_optimization(self):
        source = REPO / "build/e2e/afs/scripts/dfs/cow.py"
        with TemporaryDirectory() as directory:
            reply = subprocess.run(
                [sys.executable, "-O", "-c", SHORT_COPY_PROBE, str(source), directory],
                capture_output=True,
                text=True,
                timeout=30,
            )
            self.assertEqual(reply.returncode, 0, reply.stdout + reply.stderr)

    def test_host_executable_preserves_multicall_name_and_rejects_missing_tool(self):
        from host_tools import executable

        with TemporaryDirectory() as directory:
            program = Path(directory) / "fusermount3"
            program.write_text("#!/bin/sh\nexit 0\n")
            program.chmod(0o700)
            with patch.dict(os.environ, {"PATH": directory}):
                self.assertEqual(executable("fusermount3"), str(program))
                with self.assertRaises(FileNotFoundError):
                    executable("missing-tool")

    def test_juicefs_failed_spawn_closes_and_redacts_parent_log(self):
        from juicefs_fixture import JuicefsFixture

        with TemporaryDirectory() as directory:
            root = Path(directory)
            secret = root / "password"
            secret.write_text("private-password")
            identity = root / "identity.json"
            identity.write_text(json.dumps({"postgres_synchronous_commit": "on"}))
            config = {
                "binary": str(root / "juicefs"),
                "metadata_url": "postgres://test@127.0.0.1/test",
                "metadata_password_file": str(secret),
                "identity_file": str(identity),
                "mount_root": str(root / "mounts"),
            }
            streams = []

            def fail_spawn(*args, **kwargs):
                output = kwargs["stdout"]
                streams.append(output)
                output.write("private-password")
                raise OSError("spawn rejected")

            with patch("juicefs_fixture.load_config", return_value=config):
                fixture = JuicefsFixture("unused-config", root / "evidence")
            with patch("juicefs_fixture.subprocess.Popen", side_effect=fail_spawn):
                with patch("juicefs_fixture.os.path.ismount", return_value=False):
                    with self.assertRaisesRegex(OSError, "spawn rejected"):
                        fixture.start()
            self.assertTrue(streams[0].closed)
            self.assertEqual((root / "evidence/node1.log").read_text(), "[REDACTED]")
            self.assertFalse((root / "evidence/node1.private.log").exists())
