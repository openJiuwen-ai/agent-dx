#!/usr/bin/env python3
import json
import subprocess
import sys
import tempfile
import time
import unittest
from argparse import Namespace
from pathlib import Path
from unittest import mock

import runner


def write_json(path: Path, value: object) -> None:
    path.write_text(json.dumps(value, indent=2, sort_keys=True), encoding="utf-8")


class RunnerTests(unittest.TestCase):
    def make_workspace(self):
        temp = tempfile.TemporaryDirectory()
        root = Path(temp.name)
        manifest = {
            "schema_version": 1,
            "source_contract": "docs/testing/dfs.md",
            "cases": [
                {
                    "id": "FUN-01",
                    "name": "basic-io",
                    "category": "functional",
                    "stage": "short",
                    "active": True,
                    "matrix": {
                        "backends": ["OwnerFs", "DFS"],
                        "meta": ["etcd", "Redis"],
                        "transport": ["TCP", "RXE"],
                    },
                    "driver": {"state": "TODO", "command": None},
                }
            ],
        }
        lock = {"state": "PREPARING", "contract_sha": "abc123"}
        cases_path = root / "cases.json"
        lock_path = root / "acceptance.lock.json"
        results = root / "results"
        (root / "acceptance.md").write_text("accepted requirements\n", encoding="utf-8")
        write_json(cases_path, manifest)
        write_json(lock_path, lock)
        return temp, root, manifest, lock, cases_path, lock_path, results


    def test_default_paths_are_independent_clone_safe(self):
        repo_root = runner.REPO_ROOT
        self.assertEqual(repo_root / "docs" / "testing" / "dfs.md", runner.DEFAULT_CONTRACT)
        self.assertEqual(repo_root / ".local" / "acceptance", runner.DEFAULT_RESULTS)
        self.assertTrue(runner.DEFAULT_CONTRACT.is_file())
        self.assertEqual((repo_root / "docs" / "testing" / "dfs.md").resolve(), runner.DEFAULT_CONTRACT.resolve())
        self.assertNotIn("tests/acceptance/results", str(runner.DEFAULT_RESULTS))

    def args(self, cases_path: Path, lock_path: Path, results: Path, **overrides):
        values = {
            "cases": str(cases_path),
            "lock": str(lock_path),
            "results_dir": str(results),
            "run_id": None,
            "case": ["FUN-01"],
            "category": None,
            "backend": None,
            "meta": None,
            "transport": None,
            "profile": "smoke",
            "timeout": 5,
            "identity_attestation": None,
            "contract": str(cases_path.parent / "acceptance.md"),
        }
        values.update(overrides)
        return Namespace(**values)

    def write_attestation_and_lock(self, root: Path, cases_path: Path, lock_path: Path, wrong_runner_sha=False):
        source = root / "source.bin"
        binary = root / "binary.bin"
        source.write_text("source identity\n", encoding="utf-8")
        binary.write_text("binary identity\n", encoding="utf-8")
        attestation_path = root / "identity.json"
        write_json(attestation_path, {"source": {"path": str(source)}, "binary": {"path": str(binary)}})
        runner_sha = runner.sha256_file(Path(runner.__file__).resolve())
        if wrong_runner_sha:
            runner_sha = "0" * 64
        lock = {
            "state": "FROZEN",
            "verification": {"status": "PASS"},
            "source": {"sha256": runner.sha256_file(source)},
            "binary": {"sha256": runner.sha256_file(binary)},
            "runner": {"sha256": runner_sha},
            "manifest": {"sha256": runner.sha256_file(cases_path)},
            "contract_sha": "abc123",
            "contract": {"sha256": runner.sha256_file(root / "acceptance.md")},
        }
        write_json(lock_path, lock)
        return attestation_path

    def write_full_passing_case(self, root: Path, manifest: dict, cases_path: Path):
        passing = self.write_driver(
            root,
            "passing.py",
            self.pass_proof(profile="full", matrix={"backend": "DFS", "meta": "etcd", "transport": "TCP"}),
        )
        manifest["cases"][0]["driver"] = {"state": "READY", "command": [sys.executable, str(passing)]}
        write_json(cases_path, manifest)

    def write_driver(self, root: Path, name: str, proof: dict):
        path = root / name
        path.write_text(
            "import json\n"
            f"print(json.dumps({proof!r}))\n",
            encoding="utf-8",
        )
        return path

    def pass_proof(self, case_id="FUN-01", profile="smoke", matrix=None):
        return {
            "case_id": case_id,
            "profile": profile,
            "matrix": matrix or {"backend": "DFS", "meta": "etcd", "transport": "TCP"},
            "status": "PASS",
            "checks": [{"name": "digest", "status": "PASS", "evidence": "ok"}],
        }

    def proof_with_coverage(self, profile, matrix, axes):
        checks = []
        coverage_axes = {}
        for axis, values in axes.items():
            coverage_axes[axis] = {"values": [str(value) for value in values], "checks": {}}
            for value in values:
                name = f"{axis}-{value}"
                coverage_axes[axis]["checks"][str(value)] = name
                checks.append({"name": name, "status": "PASS", "evidence": f"covered {axis}={value}"})
        return {
            "case_id": "FUN-01",
            "profile": profile,
            "matrix": matrix,
            "status": "PASS",
            "checks": checks or [{"name": "digest", "status": "PASS", "evidence": "ok"}],
            "coverage": {"profile": profile, "axes": coverage_axes},
        }

    def test_todo_driver_is_blocked_not_pass(self):
        temp, _root, _manifest, _lock, cases_path, lock_path, results = self.make_workspace()
        with temp, mock.patch("runner.is_linux_arm64", return_value=True):
            report = runner.run_acceptance(self.args(cases_path, lock_path, results))
            self.assertEqual(report["summary"]["status"], "BLOCKED")
            self.assertEqual(report["summary"]["result_counts"]["BLOCKED"], 8)
            self.assertFalse(report["summary"]["full_release_gate_pass"])
            self.assertIn("driver state is TODO", report["results"][0]["reason"])
            self.assertTrue((Path(report["run_dir"]) / "result.json").exists())
            self.assertTrue((Path(report["run_dir"]) / "junit.xml").exists())

    def test_timeout_is_blocked_and_captures_logs(self):
        temp, root, manifest, _lock, cases_path, lock_path, results = self.make_workspace()
        sleeper = root / "sleeper.py"
        sleeper.write_text("import time\nprint('starting')\ntime.sleep(3)\n", encoding="utf-8")
        manifest["cases"][0]["driver"] = {"state": "READY", "command": [sys.executable, str(sleeper)]}
        write_json(cases_path, manifest)
        with temp, mock.patch("runner.is_linux_arm64", return_value=True):
            report = runner.run_acceptance(
                self.args(cases_path, lock_path, results, backend="DFS", meta="etcd", transport="TCP", timeout=1)
            )
            result = report["results"][0]
            self.assertEqual(result["status"], "BLOCKED")
            self.assertIn("timed out", result["reason"])
            self.assertTrue(Path(result["stdout_path"]).exists())
            self.assertTrue(Path(result["stderr_path"]).exists())

    def test_timeout_terminates_process_group_children(self):
        temp, root, manifest, _lock, cases_path, lock_path, results = self.make_workspace()
        marker = root / "grandchild_survived"
        child_code = f"import pathlib, time; time.sleep(4); pathlib.Path({str(marker)!r}).write_text('alive')"
        spawner = root / "spawner.py"
        spawner.write_text(
            "import subprocess, sys, time\n"
            f"subprocess.Popen([sys.executable, '-c', {child_code!r}])\n"
            "time.sleep(30)\n",
            encoding="utf-8",
        )
        manifest["cases"][0]["driver"] = {"state": "READY", "command": [sys.executable, str(spawner)]}
        write_json(cases_path, manifest)
        with temp, mock.patch("runner.is_linux_arm64", return_value=True):
            report = runner.run_acceptance(
                self.args(cases_path, lock_path, results, backend="DFS", meta="etcd", transport="TCP", timeout=1)
            )
            self.assertEqual(report["results"][0]["status"], "BLOCKED")
            time.sleep(5)
            self.assertFalse(marker.exists())

    def test_structured_failure_remains_fail(self):
        temp, root, manifest, _lock, cases_path, lock_path, results = self.make_workspace()
        failing = self.write_driver(
            root,
            "failing.py",
            {
                "case_id": "FUN-01",
                "profile": "smoke",
                "matrix": {"backend": "DFS", "meta": "etcd", "transport": "TCP"},
                "status": "FAIL",
                "reason": "digest mismatch",
                "checks": [{"name": "digest", "status": "FAIL", "evidence": "bad hash"}],
            },
        )
        manifest["cases"][0]["driver"] = {"state": "READY", "command": [sys.executable, str(failing)]}
        write_json(cases_path, manifest)
        with temp, mock.patch("runner.is_linux_arm64", return_value=True):
            report = runner.run_acceptance(
                self.args(cases_path, lock_path, results, backend="DFS", meta="etcd", transport="TCP")
            )
        self.assertEqual(report["summary"]["status"], "FAIL")
        self.assertEqual(report["results"][0]["status"], "FAIL")
        self.assertIn("digest mismatch", report["results"][0]["reason"])

    def test_exit_zero_without_structured_proof_is_blocked(self):
        temp, root, manifest, _lock, cases_path, lock_path, results = self.make_workspace()
        empty_success = root / "empty_success.py"
        empty_success.write_text("print('ok')\n", encoding="utf-8")
        manifest["cases"][0]["driver"] = {"state": "READY", "command": [sys.executable, str(empty_success)]}
        write_json(cases_path, manifest)
        with temp, mock.patch("runner.is_linux_arm64", return_value=True):
            report = runner.run_acceptance(
                self.args(cases_path, lock_path, results, backend="DFS", meta="etcd", transport="TCP")
            )
        self.assertEqual(report["summary"]["status"], "BLOCKED")
        self.assertIn("structured JSON proof", report["results"][0]["reason"])

    def test_smoke_pass_is_not_full_release_pass_and_records_missing_coverage(self):
        temp, root, manifest, _lock, cases_path, lock_path, results = self.make_workspace()
        passing = self.write_driver(root, "passing.py", self.pass_proof())
        manifest["cases"][0]["driver"] = {"state": "READY", "command": [sys.executable, str(passing)]}
        write_json(cases_path, manifest)
        with temp, mock.patch("runner.is_linux_arm64", return_value=True):
            report = runner.run_acceptance(
                self.args(cases_path, lock_path, results, backend="DFS", meta="etcd", transport="TCP")
            )
        coverage = report["summary"]["case_coverage"]["FUN-01"]
        self.assertEqual(report["summary"]["status"], "PASS")
        self.assertFalse(report["summary"]["full_release_gate_pass"])
        self.assertFalse(coverage["full_matrix_covered"])
        self.assertEqual(coverage["missing_full_coverage"]["backends"], ["OwnerFs"])
        self.assertEqual(coverage["missing_full_coverage"]["meta"], ["Redis"])
        self.assertEqual(coverage["missing_full_coverage"]["transport"], ["RXE"])

    def test_unimplemented_matrix_axes_are_missing_full_coverage(self):
        temp, root, manifest, _lock, cases_path, lock_path, results = self.make_workspace()
        passing = self.write_driver(root, "passing.py", self.pass_proof(matrix={"backend": "DFS", "meta": "etcd"}))
        manifest["cases"][0]["matrix"] = {
            "backends": ["DFS"],
            "meta": ["etcd"],
            "replicas": [1, 2, 3],
            "seeds": [11, 22],
            "topology": ["A", "B"],
        }
        manifest["cases"][0]["driver"] = {"state": "READY", "command": [sys.executable, str(passing)]}
        write_json(cases_path, manifest)
        with temp, mock.patch("runner.is_linux_arm64", return_value=True):
            report = runner.run_acceptance(self.args(cases_path, lock_path, results, backend="DFS", meta="etcd"))
        coverage = report["summary"]["case_coverage"]["FUN-01"]
        self.assertEqual(report["summary"]["status"], "BLOCKED")
        self.assertIn("missing coverage", report["results"][0]["reason"])
        self.assertFalse(coverage["full_matrix_covered"])
        self.assertEqual(coverage["missing_full_coverage"]["replicas"], ["1", "2", "3"])
        self.assertEqual(coverage["missing_full_coverage"]["seeds"], ["11", "22"])
        self.assertEqual(coverage["missing_full_coverage"]["topology"], ["A", "B"])

    def test_driver_owned_axis_requires_every_value(self):
        temp, root, manifest, _lock, cases_path, lock_path, results = self.make_workspace()
        proof = self.proof_with_coverage("smoke", {"backend": "DFS", "meta": "etcd"}, {"seeds": [11]})
        passing = self.write_driver(root, "partial_axis.py", proof)
        manifest["cases"][0]["matrix"] = {"backends": ["DFS"], "meta": ["etcd"], "seeds": [11, 22]}
        manifest["cases"][0]["driver"] = {"state": "READY", "command": [sys.executable, str(passing)]}
        write_json(cases_path, manifest)
        with temp, mock.patch("runner.is_linux_arm64", return_value=True):
            report = runner.run_acceptance(self.args(cases_path, lock_path, results, backend="DFS", meta="etcd"))
        self.assertEqual(report["summary"]["status"], "BLOCKED")
        self.assertIn("missing values", report["results"][0]["reason"])

    def test_driver_owned_axis_rejects_wrong_coverage_profile(self):
        temp, root, manifest, _lock, cases_path, lock_path, results = self.make_workspace()
        proof = self.proof_with_coverage("smoke", {"backend": "DFS", "meta": "etcd"}, {"sizes_smoke": ["1"]})
        proof["coverage"]["profile"] = "full"
        passing = self.write_driver(root, "wrong_profile_axis.py", proof)
        manifest["cases"][0]["matrix"] = {"backends": ["DFS"], "meta": ["etcd"], "sizes_smoke": ["1"]}
        manifest["cases"][0]["driver"] = {"state": "READY", "command": [sys.executable, str(passing)]}
        write_json(cases_path, manifest)
        with temp, mock.patch("runner.is_linux_arm64", return_value=True):
            report = runner.run_acceptance(self.args(cases_path, lock_path, results, backend="DFS", meta="etcd"))
        self.assertEqual(report["summary"]["status"], "BLOCKED")
        self.assertIn("coverage profile", report["results"][0]["reason"])

    def test_pass_check_requires_evidence_or_artifact(self):
        temp, root, manifest, _lock, cases_path, lock_path, results = self.make_workspace()
        proof = self.pass_proof()
        proof["checks"] = [{"name": "empty-pass", "status": "PASS"}]
        passing = self.write_driver(root, "no_evidence.py", proof)
        manifest["cases"][0]["driver"] = {"state": "READY", "command": [sys.executable, str(passing)]}
        write_json(cases_path, manifest)
        with temp, mock.patch("runner.is_linux_arm64", return_value=True):
            report = runner.run_acceptance(
                self.args(cases_path, lock_path, results, backend="DFS", meta="etcd", transport="TCP")
            )
        self.assertEqual(report["summary"]["status"], "BLOCKED")
        self.assertIn("no evidence", report["results"][0]["reason"])

    def test_check_level_excluded_requires_approved_exclusion(self):
        temp, root, manifest, _lock, cases_path, lock_path, results = self.make_workspace()
        proof = self.pass_proof()
        proof["checks"] = [{"name": "skipped-subcase", "status": "EXCLUDED", "evidence": "not applicable", "exclusion_id": "E1"}]
        passing = self.write_driver(root, "bad_excluded_check.py", proof)
        manifest["cases"][0]["driver"] = {"state": "READY", "command": [sys.executable, str(passing)]}
        write_json(cases_path, manifest)
        with temp, mock.patch("runner.is_linux_arm64", return_value=True):
            report = runner.run_acceptance(
                self.args(cases_path, lock_path, results, backend="DFS", meta="etcd", transport="TCP")
            )
        self.assertEqual(report["summary"]["status"], "BLOCKED")
        self.assertIn("unapproved exclusion", report["results"][0]["reason"])

    def test_scalar_or_empty_artifact_is_not_evidence(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            (root / "empty.log").touch()
            (root / "directory").mkdir()
            for evidence in (False, True, 0, 1, 1.25, "", "   ", [], {}):
                with self.subTest(evidence=evidence):
                    self.assertFalse(runner.check_has_evidence({"evidence": evidence}, root))
            for artifact in ("empty.log", "directory", "missing.log"):
                with self.subTest(artifact=artifact):
                    self.assertFalse(runner.check_has_evidence({"artifact": artifact}, root))
            (root / "result.log").write_text("observed digest matched\n")
            self.assertTrue(runner.check_has_evidence({"artifact": "result.log"}, root))
            self.assertTrue(runner.check_has_evidence({"evidence": {"observed": 4}}, root))

    def test_wrong_proof_identity_is_blocked(self):
        temp, root, manifest, _lock, cases_path, lock_path, results = self.make_workspace()
        passing = self.write_driver(root, "wrong_identity.py", self.pass_proof(case_id="FUN-02"))
        manifest["cases"][0]["driver"] = {"state": "READY", "command": [sys.executable, str(passing)]}
        write_json(cases_path, manifest)
        with temp, mock.patch("runner.is_linux_arm64", return_value=True):
            report = runner.run_acceptance(
                self.args(cases_path, lock_path, results, backend="DFS", meta="etcd", transport="TCP")
            )
        self.assertEqual(report["summary"]["status"], "BLOCKED")
        self.assertIn("does not match requested", report["results"][0]["reason"])

    def test_missing_ready_command_is_blocked(self):
        temp, _root, manifest, _lock, cases_path, lock_path, results = self.make_workspace()
        manifest["cases"][0]["driver"] = {"state": "READY", "command": None}
        write_json(cases_path, manifest)
        with temp, mock.patch("runner.is_linux_arm64", return_value=True):
            report = runner.run_acceptance(
                self.args(cases_path, lock_path, results, backend="DFS", meta="etcd", transport="TCP")
            )
        self.assertEqual(report["summary"]["status"], "BLOCKED")
        self.assertIn("argv list", report["results"][0]["reason"])

    def test_driver_start_oserror_generates_failure_artifact(self):
        temp, _root, manifest, _lock, cases_path, lock_path, results = self.make_workspace()
        manifest["cases"][0]["driver"] = {"state": "READY", "command": ["/definitely/missing/afs-driver"]}
        write_json(cases_path, manifest)
        with temp, mock.patch("runner.is_linux_arm64", return_value=True):
            report = runner.run_acceptance(
                self.args(cases_path, lock_path, results, backend="DFS", meta="etcd", transport="TCP")
            )
            result = report["results"][0]
            self.assertEqual(result["status"], "FAIL")
            self.assertIn("failed to start driver", result["reason"])
            self.assertTrue(Path(result["stdout_path"]).exists())
            self.assertTrue(Path(result["stderr_path"]).exists())

    def test_full_selected_pass_does_not_release_pass_when_active_case_missing(self):
        temp, root, manifest, _lock, cases_path, lock_path, results = self.make_workspace()
        passing = self.write_driver(
            root,
            "passing.py",
            self.pass_proof(profile="full", matrix={"backend": "DFS", "meta": "etcd", "transport": "TCP"}),
        )
        manifest["cases"][0]["driver"] = {"state": "READY", "command": [sys.executable, str(passing)]}
        second = dict(manifest["cases"][0])
        second["id"] = "FUN-02"
        manifest["cases"].append(second)
        write_json(cases_path, manifest)
        attestation_path = self.write_attestation_and_lock(root, cases_path, lock_path)
        # Isolate dispatch/coverage semantics; this fixture is not qualified ENV evidence.
        with temp, mock.patch("runner.is_linux_arm64", return_value=True), mock.patch(
            "runner.environment.qualification_errors", return_value=[],
        ):
            report = runner.run_acceptance(
                self.args(
                    cases_path,
                    lock_path,
                    results,
                    backend="DFS",
                    meta="etcd",
                    transport="TCP",
                    profile="full",
                    identity_attestation=str(attestation_path),
                )
            )
        self.assertEqual(report["summary"]["status"], "PASS")
        self.assertFalse(report["summary"]["full_release_gate_pass"])
        self.assertEqual(report["summary"]["missing_active_case_ids"], ["FUN-02"])

    def test_full_requires_frozen_verified_lock(self):
        temp, root, manifest, _lock, cases_path, lock_path, results = self.make_workspace()
        manifest["cases"][0]["driver"] = {"state": "READY", "command": [sys.executable, "-c", "print('{}')"]}
        write_json(cases_path, manifest)
        with temp, mock.patch("runner.is_linux_arm64", return_value=True):
            report = runner.run_acceptance(
                self.args(cases_path, lock_path, results, backend="DFS", meta="etcd", transport="TCP", profile="full")
            )
        self.assertEqual(report["summary"]["status"], "BLOCKED")
        self.assertIn("frozen verified lock identity", report["results"][0]["reason"])

    def test_full_flags_and_four_hashes_without_environment_cannot_pass(self):
        temp, root, manifest, _lock, cases_path, lock_path, results = self.make_workspace()
        self.write_full_passing_case(root, manifest, cases_path)
        attestation = self.write_attestation_and_lock(root, cases_path, lock_path)
        contract = root / "acceptance.md"
        contract.write_text("accepted requirements\n", encoding="utf-8")
        lock = runner.load_json(lock_path)
        lock["contract"] = {"sha256": runner.sha256_file(contract)}
        write_json(lock_path, lock)
        with temp, mock.patch("runner.is_linux_arm64", return_value=True):
            report = runner.run_acceptance(self.args(
                cases_path, lock_path, results, backend="DFS", meta="etcd", transport="TCP",
                profile="full", identity_attestation=str(attestation), contract=str(contract),
            ))
        self.assertEqual(report["summary"]["status"], "BLOCKED")
        self.assertFalse(report["summary"]["full_release_gate_pass"])
        self.assertIn("environment", report["results"][0]["reason"])

    def test_changed_actual_contract_cannot_pass_with_old_contract_hash(self):
        temp, root, manifest, _lock, cases_path, lock_path, results = self.make_workspace()
        self.write_full_passing_case(root, manifest, cases_path)
        attestation = self.write_attestation_and_lock(root, cases_path, lock_path)
        contract = root / "acceptance.md"
        contract.write_text("accepted requirements\n", encoding="utf-8")
        lock = runner.load_json(lock_path)
        lock["contract"] = {"sha256": runner.sha256_file(contract)}
        write_json(lock_path, lock)
        contract.write_text("altered requirements\n", encoding="utf-8")
        # Isolate contract validation; the environment guard is tested separately.
        with temp, mock.patch("runner.is_linux_arm64", return_value=True), mock.patch(
            "runner.environment.qualification_errors", return_value=[],
        ):
            report = runner.run_acceptance(self.args(
                cases_path, lock_path, results, backend="DFS", meta="etcd", transport="TCP",
                profile="full", identity_attestation=str(attestation), contract=str(contract),
            ))
        self.assertEqual(report["summary"]["status"], "BLOCKED")
        self.assertFalse(report["summary"]["full_release_gate_pass"])
        self.assertIn("contract sha256 mismatch", report["results"][0]["reason"])

    def test_missing_actual_contract_blocks_full_but_not_local_smoke(self):
        temp, root, manifest, _lock, cases_path, lock_path, results = self.make_workspace()
        self.write_full_passing_case(root, manifest, cases_path)
        attestation = self.write_attestation_and_lock(root, cases_path, lock_path)
        (root / "acceptance.md").unlink()
        with temp, mock.patch("runner.is_linux_arm64", return_value=True), mock.patch(
            "runner.environment.qualification_errors", return_value=[],
        ):
            full = runner.run_acceptance(self.args(
                cases_path, lock_path, results, backend="DFS", meta="etcd", transport="TCP",
                profile="full", identity_attestation=str(attestation),
            ))
            self.assertEqual(full["summary"]["status"], "BLOCKED")
            self.assertIn("contract path does not exist", full["results"][0]["reason"])
            passing = self.write_driver(root, "smoke.py", self.pass_proof())
            manifest["cases"][0]["driver"]["command"] = [sys.executable, str(passing)]
            write_json(cases_path, manifest)
            smoke = runner.run_acceptance(self.args(
                cases_path, lock_path, results, backend="DFS", meta="etcd", transport="TCP",
            ))
            self.assertEqual(smoke["summary"]["status"], "PASS")
            self.assertFalse(smoke["summary"]["full_release_gate_pass"])

    def test_contract_provenance_commit_does_not_replace_actual_contract_digest(self):
        temp, root, manifest, _lock, cases_path, lock_path, results = self.make_workspace()
        self.write_full_passing_case(root, manifest, cases_path)
        attestation = self.write_attestation_and_lock(root, cases_path, lock_path)
        lock = runner.load_json(lock_path)
        del lock["contract"]
        write_json(lock_path, lock)
        with temp, mock.patch("runner.is_linux_arm64", return_value=True), mock.patch(
            "runner.environment.qualification_errors", return_value=[],
        ):
            report = runner.run_acceptance(self.args(
                cases_path, lock_path, results, backend="DFS", meta="etcd", transport="TCP",
                profile="full", identity_attestation=str(attestation),
            ))
        self.assertEqual(report["summary"]["status"], "BLOCKED")
        self.assertIn("lock contract identity is missing", report["results"][0]["reason"])

    def test_lock_verified_boolean_alone_is_not_release_ready(self):
        temp, root, manifest, _lock, cases_path, lock_path, results = self.make_workspace()
        passing = self.write_driver(
            root,
            "passing.py",
            self.pass_proof(profile="full", matrix={"backend": "DFS", "meta": "etcd", "transport": "TCP"}),
        )
        manifest["cases"][0]["driver"] = {"state": "READY", "command": [sys.executable, str(passing)]}
        write_json(cases_path, manifest)
        write_json(lock_path, {"state": "FROZEN", "verified": True, "contract_sha": "abc123"})
        with temp, mock.patch("runner.is_linux_arm64", return_value=True):
            report = runner.run_acceptance(
                self.args(cases_path, lock_path, results, backend="DFS", meta="etcd", transport="TCP", profile="full")
            )
        self.assertEqual(report["summary"]["status"], "BLOCKED")
        self.assertFalse(report["summary"]["full_release_gate_pass"])

    def test_frozen_lock_runner_sha_mismatch_blocks_full(self):
        temp, root, manifest, _lock, cases_path, lock_path, results = self.make_workspace()
        passing = self.write_driver(
            root,
            "passing.py",
            self.pass_proof(profile="full", matrix={"backend": "DFS", "meta": "etcd", "transport": "TCP"}),
        )
        manifest["cases"][0]["driver"] = {"state": "READY", "command": [sys.executable, str(passing)]}
        write_json(cases_path, manifest)
        attestation_path = self.write_attestation_and_lock(root, cases_path, lock_path, wrong_runner_sha=True)
        with temp, mock.patch("runner.is_linux_arm64", return_value=True):
            report = runner.run_acceptance(
                self.args(
                    cases_path,
                    lock_path,
                    results,
                    backend="DFS",
                    meta="etcd",
                    transport="TCP",
                    profile="full",
                    identity_attestation=str(attestation_path),
                )
            )
        self.assertEqual(report["summary"]["status"], "BLOCKED")
        self.assertIn("runner sha256 mismatch", report["results"][0]["reason"])

    def test_attestation_without_paths_cannot_fake_observed_identity(self):
        temp, root, manifest, _lock, cases_path, lock_path, results = self.make_workspace()
        self.write_full_passing_case(root, manifest, cases_path)
        source_sha = "1" * 64
        binary_sha = "2" * 64
        attestation_path = root / "identity.json"
        write_json(attestation_path, {"source": {"sha256": source_sha}, "binary": {"sha256": binary_sha}})
        write_json(
            lock_path,
            {
                "state": "FROZEN",
                "verification": {"status": "PASS"},
                "source": {"sha256": source_sha},
                "binary": {"sha256": binary_sha},
                "runner": {"sha256": runner.sha256_file(Path(runner.__file__).resolve())},
                "manifest": {"sha256": runner.sha256_file(cases_path)},
            },
        )
        with temp, mock.patch("runner.is_linux_arm64", return_value=True):
            report = runner.run_acceptance(
                self.args(
                    cases_path,
                    lock_path,
                    results,
                    backend="DFS",
                    meta="etcd",
                    transport="TCP",
                    profile="full",
                    identity_attestation=str(attestation_path),
                )
            )
        self.assertEqual(report["summary"]["status"], "BLOCKED")
        self.assertIn("observed source identity is missing", report["results"][0]["reason"])

    def test_attestation_declared_sha_is_ignored_actual_file_hash_wins(self):
        temp, root, manifest, _lock, cases_path, lock_path, results = self.make_workspace()
        self.write_full_passing_case(root, manifest, cases_path)
        source = root / "source.tar"
        binary = root / "binary.pkg"
        source.write_text("actual source\n", encoding="utf-8")
        binary.write_text("actual binary\n", encoding="utf-8")
        fake_source_sha = "1" * 64
        fake_binary_sha = "2" * 64
        attestation_path = root / "identity.json"
        write_json(
            attestation_path,
            {
                "source": {"path": str(source), "sha256": fake_source_sha},
                "binary": {"path": str(binary), "sha256": fake_binary_sha},
            },
        )
        write_json(
            lock_path,
            {
                "state": "FROZEN",
                "verification": {"status": "PASS"},
                "source": {"sha256": fake_source_sha},
                "binary": {"sha256": fake_binary_sha},
                "runner": {"sha256": runner.sha256_file(Path(runner.__file__).resolve())},
                "manifest": {"sha256": runner.sha256_file(cases_path)},
            },
        )
        with temp, mock.patch("runner.is_linux_arm64", return_value=True):
            report = runner.run_acceptance(
                self.args(
                    cases_path,
                    lock_path,
                    results,
                    backend="DFS",
                    meta="etcd",
                    transport="TCP",
                    profile="full",
                    identity_attestation=str(attestation_path),
                )
            )
        self.assertEqual(report["summary"]["status"], "BLOCKED")
        self.assertIn("source sha256 mismatch", report["results"][0]["reason"])

    def test_dirty_git_source_tree_blocks_full_release(self):
        temp, root, manifest, _lock, cases_path, lock_path, results = self.make_workspace()
        self.write_full_passing_case(root, manifest, cases_path)
        source_dir = root / "source"
        source_dir.mkdir()
        subprocess.run(["git", "init"], cwd=source_dir, check=True, capture_output=True, text=True)
        subprocess.run(["git", "config", "user.email", "test@example.invalid"], cwd=source_dir, check=True)
        subprocess.run(["git", "config", "user.name", "Test"], cwd=source_dir, check=True)
        (source_dir / "tracked.txt").write_text("clean\n", encoding="utf-8")
        subprocess.run(["git", "add", "tracked.txt"], cwd=source_dir, check=True)
        subprocess.run(["git", "commit", "-m", "initial"], cwd=source_dir, check=True, capture_output=True, text=True)
        commit = subprocess.run(["git", "rev-parse", "HEAD"], cwd=source_dir, check=True, capture_output=True, text=True).stdout.strip()
        (source_dir / "tracked.txt").write_text("dirty\n", encoding="utf-8")
        binary = root / "binary.pkg"
        binary.write_text("actual binary\n", encoding="utf-8")
        attestation_path = root / "identity.json"
        write_json(attestation_path, {"source": {"path": str(source_dir)}, "binary": {"path": str(binary)}})
        write_json(
            lock_path,
            {
                "state": "FROZEN",
                "verification": {"status": "PASS"},
                "source": {"git_commit": commit},
                "binary": {"sha256": runner.sha256_file(binary)},
                "runner": {"sha256": runner.sha256_file(Path(runner.__file__).resolve())},
                "manifest": {"sha256": runner.sha256_file(cases_path)},
            },
        )
        with temp, mock.patch("runner.is_linux_arm64", return_value=True):
            report = runner.run_acceptance(
                self.args(
                    cases_path,
                    lock_path,
                    results,
                    backend="DFS",
                    meta="etcd",
                    transport="TCP",
                    profile="full",
                    identity_attestation=str(attestation_path),
                )
            )
        self.assertEqual(report["summary"]["status"], "BLOCKED")
        self.assertIn("source git tree is dirty", report["results"][0]["reason"])


if __name__ == "__main__":
    unittest.main()
