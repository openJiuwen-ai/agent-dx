#!/usr/bin/env python3
import json
import os
import subprocess
import sys
import tempfile
import unittest
from pathlib import Path

DRIVER = Path(__file__).resolve().parent / "drivers" / "deploy.py"


class DeployDriverTests(unittest.TestCase):
    def run_driver(self, package: Path, profile="smoke", case_id="DEP-01"):
        with tempfile.TemporaryDirectory() as td:
            run_dir = Path(td) / "run"
            env = os.environ.copy()
            env.update(
                {
                    "AFS_ACCEPTANCE_CASE_ID": case_id,
                    "AFS_ACCEPTANCE_PROFILE": profile,
                    "AFS_ACCEPTANCE_MATRIX": json.dumps({"backend": "DFS", "meta": "local-file"}),
                    "AFS_ACCEPTANCE_RUN_DIR": str(run_dir),
                }
            )
            proc = subprocess.run(
                [sys.executable, str(DRIVER), "--package", str(package), "--work-root", str(Path(td) / "work")],
                shell=False,
                text=True,
                stdout=subprocess.PIPE,
                stderr=subprocess.PIPE,
                env=env,
                check=False,
            )
            lines = [line for line in proc.stdout.splitlines() if line.strip()]
            self.assertTrue(lines, proc.stderr)
            proof = json.loads(lines[-1])
            return proc, proof

    def test_missing_package_is_blocked_not_pass(self):
        proc, proof = self.run_driver(Path("/definitely/missing/afs.tar.gz"))
        self.assertNotEqual(proc.returncode, 0)
        self.assertEqual(proof["status"], "BLOCKED")
        self.assertEqual(proof["checks"][0]["status"], "BLOCKED")
        self.assertIn("package does not exist", proof["reason"])

    def test_full_profile_is_blocked_until_full_matrix_driver_exists(self):
        with tempfile.TemporaryDirectory() as td:
            package = Path(td) / "fake.tar.gz"
            package.write_bytes(b"not a real package")
            proc, proof = self.run_driver(package, profile="full")
        self.assertNotEqual(proc.returncode, 0)
        self.assertEqual(proof["status"], "BLOCKED")
        self.assertIn("full DEP matrix remains TODO", proof["reason"])

    def test_dep02_is_blocked_in_single_node_driver(self):
        with tempfile.TemporaryDirectory() as td:
            package = Path(td) / "fake.tar.gz"
            package.write_bytes(b"not a real package")
            proc, proof = self.run_driver(package, case_id="DEP-02")
        self.assertNotEqual(proc.returncode, 0)
        self.assertEqual(proof["status"], "BLOCKED")
        self.assertIn("multi-node topology", proof["reason"])
        self.assertEqual(proof["case_id"], "DEP-02")

    def test_unknown_case_is_blocked(self):
        with tempfile.TemporaryDirectory() as td:
            package = Path(td) / "fake.tar.gz"
            package.write_bytes(b"not a real package")
            proc, proof = self.run_driver(package, case_id="DEP-99")
        self.assertNotEqual(proc.returncode, 0)
        self.assertEqual(proof["status"], "BLOCKED")
        self.assertIn("unsupported deployment case", proof["reason"])

    def test_blocked_proof_keeps_requested_identity(self):
        _proc, proof = self.run_driver(Path("/definitely/missing/afs.tar.gz"))
        self.assertEqual(proof["case_id"], "DEP-01")
        self.assertEqual(proof["profile"], "smoke")
        self.assertEqual(proof["matrix"], {"backend": "DFS", "meta": "local-file"})
        self.assertTrue(proof["checks"][0]["evidence"])
        self.assertIn("limitations", proof)


if __name__ == "__main__":
    unittest.main()
