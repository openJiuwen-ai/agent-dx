"""Cleanup failures must remain visible in acceptance evidence."""

import hashlib
import importlib.util
import subprocess
import sys
import tempfile
import unittest
from pathlib import Path
from types import SimpleNamespace
from unittest import mock


class CleanupReportingTests(unittest.TestCase):
    def test_unlink_failure_retains_fixture_and_fails_acceptance(self):
        path = Path(__file__).parent / "probes" / "locks_cross.py"
        spec = importlib.util.spec_from_file_location("cleanup_probe", path)
        module = importlib.util.module_from_spec(spec)
        spec.loader.exec_module(module)
        probe = module.CrossProbe(
            SimpleNamespace(
                worker_a_json=["worker-a"],
                worker_b_json=["worker-b"],
                path_a="/fixture/lock",
                path_b="/fixture/lock",
                renamed_a=None,
                renamed_b=None,
                evidence="/unused",
                keep_fixture=False,
            )
        )
        with mock.patch.object(probe, "run_cmd", side_effect=[OSError("unlink denied"), {}]) as unlink:
            probe.cleanup()
        self.assertEqual(unlink.call_count, 2)
        self.assertTrue(probe.fixture_kept)
        self.assertEqual(len(probe.failures), 1)
        self.assertIn("unlink denied", probe.failures[0]["message"])

    def test_owner_payload_corruption_is_rejected_with_python_optimization(self):
        path = Path(__file__).parents[1] / "scripts" / "ownerfs" / "bind-two-node-localfile.py"
        spec = importlib.util.spec_from_file_location("optimized_payload_probe", path)
        module = importlib.util.module_from_spec(spec)
        spec.loader.exec_module(module)
        scenario = module.Scenario(Path("/unused"), Path("/unused"), Path("/unused"), False)
        expected = b"expected"
        with tempfile.TemporaryDirectory() as directory:
            payload = Path(directory) / "payload"
            payload.write_bytes(b"corrupt!")
            with mock.patch.object(scenario, "user") as user:
                scenario.read_payload(payload, expected)
            code = user.call_args.args[0]
            result = subprocess.run(
                [
                    sys.executable,
                    "-O",
                    "-c",
                    "import os, sys\n" + code,
                    str(payload),
                    str(len(expected)),
                    hashlib.sha256(expected).hexdigest(),
                ],
                capture_output=True,
                text=True,
                timeout=5,
            )
        self.assertNotEqual(result.returncode, 0, result.stdout)
