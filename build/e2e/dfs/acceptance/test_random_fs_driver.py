#!/usr/bin/env python3
import json
import platform
import subprocess
import sys
import tempfile
import unittest
from pathlib import Path

DRIVER = Path(__file__).resolve().parent / "drivers" / "random_fs.py"


class RandomFsDriverTests(unittest.TestCase):
    def setUp(self):
        if platform.system() != "Linux":
            self.skipTest("STD-04 driver tests run in Linux so findmnt and errno behavior match acceptance")

    def run_driver(self, *, profile="smoke", operations=None, seeds=None, max_seeds=None, fault_at=None, backend="ext4", allow_fixture=True, reference_under_tmpfs=False, timeout_seconds=None, skip_cleanup=False, process_pid=None, meta_process_pid=None):
        root = Path(tempfile.mkdtemp(prefix="afs-random-fs-driver-"))
        self.addCleanup(lambda: subprocess.run(["rm", "-rf", str(root)], check=False))
        mount = root / "target-mount"
        target = mount / "workspace"
        reference = (Path("/dev/shm") / f"afs-random-fs-driver-ref-{root.name}") if reference_under_tmpfs else root / "reference-ext4"
        target.mkdir(parents=True)
        reference.mkdir()
        if reference_under_tmpfs:
            self.addCleanup(lambda: subprocess.run(["rm", "-rf", str(reference)], check=False))
        run_dir = root / "run"
        matrix = {"reference": "ext4", "backend": backend, "meta": "memory", "seeds": 10, "operations_per_seed": 10000}
        argv = [
            sys.executable,
            str(DRIVER),
            "--profile",
            profile,
            "--matrix-json",
            json.dumps(matrix),
            "--run-dir",
            str(run_dir),
            "--mount",
            str(mount),
            "--base-dir",
            str(target),
            "--reference-dir",
            str(reference),
            "--backend",
            backend,
            "--meta",
            "memory",
        ]
        if process_pid is not None:
            argv.extend(["--process-pid", str(process_pid)])
        if meta_process_pid is not None:
            argv.extend(["--meta-process-pid", str(meta_process_pid)])
        if allow_fixture:
            argv.append("--allow-reference-fixture")
        if operations is not None:
            argv.extend(["--operations", str(operations)])
        if seeds is not None:
            argv.extend(["--seeds", seeds])
        if max_seeds is not None:
            argv.extend(["--max-seeds", str(max_seeds)])
        if timeout_seconds is not None:
            argv.extend(["--per-seed-timeout-seconds", str(timeout_seconds)])
        if fault_at is not None:
            argv.extend(["--selftest-inject-target-fault-at", str(fault_at)])
        if skip_cleanup:
            argv.append("--selftest-skip-target-cleanup")
        proc = subprocess.run(argv, text=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE, check=False)
        lines = [line for line in proc.stdout.splitlines() if line.strip()]
        self.assertTrue(lines, proc.stderr)
        return proc, json.loads(lines[-1]), run_dir

    def test_smoke_pass_records_trace_and_accounting(self):
        proc, proof, run_dir = self.run_driver(operations=25)
        self.assertEqual(proc.returncode, 0, proof)
        self.assertEqual(proof["status"], "PASS")
        self.assertEqual(proof["accounting"]["executed"], 1)
        self.assertEqual(proof["coverage"]["axes"]["seeds_smoke"]["values"], ["1"])
        self.assertEqual(proof["coverage"]["axes"]["operations_per_seed_smoke"]["values"], ["25"])
        self.assertNotIn("seeds_full", proof["coverage"]["axes"])
        seed_results = run_dir / proof["artifacts"]["root"] / "seed-results.json"
        self.assertTrue(seed_results.exists())
        raw = json.loads(seed_results.read_text())
        self.assertEqual(raw[0]["operations_executed"], 25)
        trace = run_dir / proof["artifacts"]["root"] / raw[0]["trace"]
        self.assertTrue(trace.exists())
        self.assertEqual(len(json.loads(trace.read_text())), 25)

    def test_full_with_cap_is_blocked_not_pass(self):
        proc, proof, _run_dir = self.run_driver(profile="full", operations=40, max_seeds=1, backend="ext4", allow_fixture=False)
        self.assertNotEqual(proc.returncode, 0)
        self.assertEqual(proof["status"], "BLOCKED")
        self.assertIn("10 fixed seeds", proof["reason"])

    def test_failure_preserves_raw_prefix(self):
        proc, proof, run_dir = self.run_driver(operations=20, fault_at=7)
        self.assertEqual(proof["status"], "FAIL")
        first_failure = next(item for item in proof["checks"] if item["name"] == "differential-results")["evidence"]["first_failure"]
        self.assertEqual(first_failure["mismatch"]["failing_prefix_operations"], 8)
        self.assertTrue(first_failure["mismatch"]["minimized"])
        self.assertEqual(first_failure["mismatch"]["minimal_failing_prefix_operations"], 8)
        shrink = run_dir / proof["artifacts"]["root"] / first_failure["shrink_corpus"]
        self.assertTrue(shrink.exists())
        shrink_raw = json.loads(shrink.read_text())
        self.assertEqual(shrink_raw["failing_prefix_operations"], 8)
        self.assertTrue(shrink_raw["minimized"])
        self.assertEqual(shrink_raw["minimal_failing_prefix_operations"], 8)
        self.assertEqual(len(shrink_raw["trace_prefix"]), 8)
        hyp = json.loads((run_dir / proof["artifacts"]["root"] / shrink_raw["hypothesis_shrink"]).read_text())
        self.assertEqual(hyp["status"], "REPRODUCED")
        self.assertEqual(hyp["minimal_failing_prefix_operations"], 8)
        self.assertEqual(hyp["replay"]["fingerprint"], hyp["fingerprint"])

    def test_missing_backend_blocks(self):
        proc, proof, _run_dir = self.run_driver(backend="")
        self.assertNotEqual(proc.returncode, 0)
        self.assertEqual(proof["status"], "BLOCKED")
        self.assertTrue(any(c["name"] == "backend-selection" and c["status"] == "BLOCKED" for c in proof["checks"]))

    def test_product_backend_on_ext4_is_blocked_without_fixture_override(self):
        proc, proof, _run_dir = self.run_driver(backend="dfs", allow_fixture=False)
        self.assertNotEqual(proc.returncode, 0)
        self.assertEqual(proof["status"], "BLOCKED")
        self.assertTrue(any(c["name"] == "observed-target-backend" and c["status"] == "BLOCKED" for c in proof["checks"]))

    def test_product_backend_on_ext4_is_blocked_even_with_fixture_override(self):
        proc, proof, _run_dir = self.run_driver(backend="dfs", allow_fixture=True)
        self.assertNotEqual(proc.returncode, 0)
        self.assertEqual(proof["status"], "BLOCKED")
        self.assertTrue(any(c["name"] == "observed-target-backend" and c["status"] == "BLOCKED" for c in proof["checks"]))

    def test_product_backend_with_mismatched_pids_is_blocked(self):
        proc, proof, _run_dir = self.run_driver(backend="dfs", allow_fixture=True, process_pid=1, meta_process_pid=1)
        self.assertNotEqual(proc.returncode, 0)
        self.assertEqual(proof["status"], "BLOCKED")
        self.assertTrue(any(c["name"] == "product-process-identity" and c["status"] == "BLOCKED" for c in proof["checks"]))

    def test_full_reference_fixture_is_blocked(self):
        proc, proof, _run_dir = self.run_driver(profile="full", operations=40, max_seeds=1, backend="ext4", allow_fixture=True)
        self.assertNotEqual(proc.returncode, 0)
        self.assertEqual(proof["status"], "BLOCKED")
        self.assertTrue(any(c["name"] == "reference-fixture-scope" and c["status"] == "BLOCKED" for c in proof["checks"]))
        self.assertTrue(any(c["name"] == "hypothesis-shrink-database" and c["status"] == "PASS" for c in proof["checks"]))

    def test_wrong_reference_filesystem_is_blocked(self):
        if not Path("/dev/shm").is_dir():
            self.skipTest("/dev/shm tmpfs is not available")
        proc, proof, _run_dir = self.run_driver(reference_under_tmpfs=True)
        self.assertNotEqual(proc.returncode, 0)
        self.assertEqual(proof["status"], "BLOCKED")
        self.assertTrue(any(c["name"] == "reference-ext4-scope" and c["status"] == "BLOCKED" for c in proof["checks"]))

    def test_timeout_is_inconclusive_not_pass(self):
        proc, proof, _run_dir = self.run_driver(operations=10, timeout_seconds=0)
        self.assertNotEqual(proc.returncode, 0)
        self.assertEqual(proof["status"], "INCONCLUSIVE")
        self.assertTrue(any(c["name"] == "differential-results" and c["status"] == "INCONCLUSIVE" for c in proof["checks"]))

    def test_cleanup_error_is_inconclusive_not_pass(self):
        proc, proof, _run_dir = self.run_driver(operations=1, skip_cleanup=True)
        self.assertNotEqual(proc.returncode, 0)
        self.assertEqual(proof["status"], "INCONCLUSIVE")
        first_inconclusive = next(item for item in proof["checks"] if item["name"] == "differential-results")["evidence"]["first_inconclusive"]
        self.assertFalse(first_inconclusive["cleanup_ok"])


if __name__ == "__main__":
    unittest.main()
