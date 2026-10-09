#!/usr/bin/env python3
import importlib.util
import json
import random
import tempfile
import unittest
from pathlib import Path


DRIVER = Path(__file__).resolve().parent / "drivers" / "accounting.py"


def load_driver():
    spec = importlib.util.spec_from_file_location("afs_accounting_driver", DRIVER)
    module = importlib.util.module_from_spec(spec)
    assert spec.loader is not None
    spec.loader.exec_module(module)
    return module


accounting = load_driver()


def write_json(path: Path, value: object) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(value, indent=2, sort_keys=True) + "\n", encoding="utf-8")


def sha256_file(path: Path) -> str:
    return accounting.sha256_file(path)


class AccountingFixture:
    def __init__(self, root: Path, *, profile: str = "full"):
        self.root = root
        self.profile = profile
        self.matrix = {"backend": "DFS", "meta": "Redis"}
        self.node_sha = "a" * 64
        self.meta_sha = "b" * 64
        self.candidate = {"afs_candidate": f"node:{self.node_sha}:meta:{self.meta_sha}", "backend": "DFS", "meta": "Redis", "profile": profile, "node_sha256": self.node_sha, "meta_sha256": self.meta_sha}
        self.entries: list[dict[str, object]] = []

    def add_suite(self, case_id: str, *, proof_updates: dict[str, object] | None = None, entry_updates: dict[str, object] | None = None) -> None:
        artifact_root = self.root / case_id.lower() / "artifacts" / self.artifact_name(case_id)
        artifact_root.mkdir(parents=True)
        proof = self.default_proof(case_id)
        if proof_updates:
            self.deep_update(proof, proof_updates)
        self.write_raw_artifacts(case_id, artifact_root, proof)
        proof_path = artifact_root / "proof.json"
        write_json(proof_path, proof)
        reference = self.reference_binding(case_id)
        if case_id == "STD-04":
            reference["internal_pairing"] = True
        entry = {
            "case_id": case_id,
            "candidate": dict(self.candidate),
            "reference": reference,
            "difference_policy": {"errno_differences": [], "skip_differences": []},
            "pre_run_exclusions": [],
            "artifact_root": str(artifact_root),
            "proof_path": str(proof_path),
            "proof_sha256": sha256_file(proof_path),
            "raw_artifacts": self.raw_bindings(artifact_root),
        }
        if entry_updates:
            self.deep_update(entry, entry_updates)
        self.entries.append(entry)

    def raw_bindings(self, artifact_root: Path) -> list[dict[str, str]]:
        return [{"path": str(path.relative_to(artifact_root)), "sha256": sha256_file(path)} for path in sorted(artifact_root.rglob("*")) if path.is_file() and path.name != "proof.json"]

    def reference_binding(self, case_id: str) -> dict[str, object]:
        reference_root = self.root / "references" / case_id
        reference_root.mkdir(parents=True, exist_ok=True)
        proof = self.default_proof(case_id)
        proof["matrix"] = {"reference": "ext4"}
        proof["identity"] = {"product": {"reference": "ext4"}}
        self.write_raw_artifacts(case_id, reference_root, proof)
        path = reference_root / "proof.json"
        write_json(path, proof)
        return {"filesystem": "ext4", "proof_path": str(path), "proof_sha256": sha256_file(path), "artifact_root": str(reference_root), "raw_artifacts": self.raw_bindings(reference_root)}

    def write_raw_artifacts(self, case_id: str, artifact_root: Path, proof: dict[str, object]) -> None:
        identity = {
            "product": dict(self.candidate),
            "process": {"pid": "101", "exe": "/usr/bin/afs-node", "cmdline": "afs-node", "exe_sha256": self.node_sha},
            "meta_process": {"pid": "102", "exe": "/usr/bin/afs-meta", "cmdline": "afs-meta", "exe_sha256": self.meta_sha},
        }
        if case_id == "STD-04":
            identity["reference_filesystem"] = {"fstype": "ext4"}
        write_json(artifact_root / "identity.json", identity)
        if case_id == "STD-01":
            write_json(artifact_root / "discovery.json", {"discovered_files": 3, "tests": ["chmod/00.t", "open/00.t", "rename/00.t"]})
            tap = dict(proof["accounting"])  # type: ignore[index]
            tap.setdefault("selected_tests", ["chmod/00.t", "open/00.t", "rename/00.t"]); tap.setdefault("observed_completed_tests", ["chmod/00.t", "open/00.t", "rename/00.t"])
            write_json(artifact_root / "tap-accounting.json", tap)
            write_json(artifact_root / "command.json", {"returncode": 0, "timed_out": False})
            tap_lines: list[str] = []
            for rel_test in tap["selected_tests"]:
                tap_lines.extend([f"{rel_test} ..", "1..1", f"ok 1 - {rel_test}", "ok"])
            tap_lines.append(f"Files={len(tap['selected_tests'])}, Tests={tap['tap_ok'] + tap['tap_not_ok']}, Result: PASS")
            (artifact_root / "pjdfstest.stdout.tap").write_text("\n".join(tap_lines) + "\n", encoding="utf-8")
            (artifact_root / "pjdfstest.stderr.log").write_text("", encoding="utf-8")
        elif case_id == "STD-02":
            selected = [{"test_id": f"ltp-{idx}", "command": ["kirk", str(idx)]} for idx in range(657)]
            write_json(artifact_root / "discovery.json", {"total_commands": 657, "selection": {"profile": self.profile}, "selected": selected})
            executed = int(proof["accounting"].get("executed", 657))  # type: ignore[index,union-attr]
            records = []
            for idx in range(executed):
                command_dir = artifact_root / "commands" / f"{idx + 1:04d}-ltp-{idx}"
                command_dir.mkdir(parents=True, exist_ok=True)
                (command_dir / "stdout.log").write_text(f"ltp-{idx} 1 TPASS: ok\n", encoding="utf-8")
                (command_dir / "stderr.log").write_text("", encoding="utf-8")
                write_json(command_dir / "kirk-report.json", {"test_id": f"ltp-{idx}", "result": "PASS"})
                write_json(command_dir / "command.json", {"result": {"returncode": 0, "timed_out": False}})
                records.append({"index": idx + 1, "test_id": f"ltp-{idx}", "command": ["kirk", str(idx)], "result": "PASS", "returncode": 0, "timed_out": False, "artifacts": {"stdout": str((command_dir / "stdout.log").relative_to(artifact_root)), "stderr": str((command_dir / "stderr.log").relative_to(artifact_root)), "kirk_report": str((command_dir / "kirk-report.json").relative_to(artifact_root)), "command": str((command_dir / "command.json").relative_to(artifact_root))}})
            write_json(artifact_root / "commands.json", records)
            write_json(artifact_root / "accounting.json", proof["accounting"])
        elif case_id == "STD-03":
            seeds = proof["accounting"].get("selected_seeds", [1, 2, 3])  # type: ignore[index,union-attr]
            executed = int(proof["accounting"].get("executed", len(seeds)))  # type: ignore[index,union-attr,arg-type]
            records = []
            for seed in list(seeds)[:executed]:
                seed_dir = artifact_root / "seeds" / f"seed-{seed}"
                seed_dir.mkdir(parents=True, exist_ok=True)
                (seed_dir / "stdout.log").write_text("All operations - 100 - completed A-OK!\n", encoding="utf-8")
                (seed_dir / "stderr.log").write_text("", encoding="utf-8")
                records.append({"seed": seed, "argv": ["fsx", str(seed)], "result": "PASS", "process": {"returncode": 0, "timed_out": False}, "artifacts": {"stdout": str((seed_dir / "stdout.log").relative_to(artifact_root)), "stderr": str((seed_dir / "stderr.log").relative_to(artifact_root))}})
            write_json(artifact_root / "commands.json", records)
            write_json(artifact_root / "accounting.json", proof["accounting"])
        elif case_id == "STD-04":
            seeds = proof["accounting"].get("selected_seeds", list(range(1, 11)))  # type: ignore[index,union-attr]
            executed = int(proof["accounting"].get("executed", len(seeds)))  # type: ignore[index,union-attr,arg-type]
            operations = proof["accounting"].get("operations_per_seed", 10000)  # type: ignore[index,union-attr]
            records = []
            random_driver = accounting.driver_module("random_fs")
            for seed in list(seeds)[:executed]:
                rng = random.Random(int(seed))
                trace = []
                for idx in range(int(operations)):
                    trace.append({
                        "index": idx,
                        "operation": random_driver.generate_operation(int(seed), idx, rng),
                        "reference_result": {"ok": True},
                        "target_result": {"ok": True},
                        "affected_paths": {"reference": {}, "target": {}},
                        "reference_tree_sha256": "0" * 64,
                        "target_tree_sha256": "0" * 64,
                        "reference_entry_count": 0,
                        "target_entry_count": 0,
                    })
                trace_path = artifact_root / "seeds" / f"seed-{seed}" / "trace.json"
                write_json(trace_path, trace)
                trace_sha = accounting.hashlib.sha256(json.dumps(trace, sort_keys=True, separators=(",", ":")).encode()).hexdigest()
                records.append({"seed": seed, "status": "PASS", "operations_executed": operations, "trace": str(trace_path.relative_to(artifact_root)), "trace_sha256": trace_sha})
            write_json(artifact_root / "seed-results.json", records)
            write_json(artifact_root / "accounting.json", proof["accounting"])

    @staticmethod
    def artifact_name(case_id: str) -> str:
        return {"STD-01": "std-01-pjdfstest", "STD-02": "std-02-ltp", "STD-03": "std-03-fsx", "STD-04": "std-04-random"}[case_id]

    def default_proof(self, case_id: str) -> dict[str, object]:
        proof: dict[str, object] = {
            "case_id": case_id,
            "profile": self.profile,
            "matrix": dict(self.matrix),
            "status": "PASS",
            "reason": "",
            "checks": [{"name": "identity-check", "status": "PASS", "evidence": {}}],
            "coverage": {"profile": self.profile, "axes": {"reference": {"values": ["ext4"], "checks": {"ext4": "identity-check"}}}},
            "identity": {"artifact": f"artifacts/{self.artifact_name(case_id)}/identity.json", "product": {"backend": "DFS", "meta": "Redis"}},
        }
        if case_id == "STD-01":
            proof["accounting"] = {
                "tap_planned": 3,
                "tap_ok": 3,
                "tap_not_ok": 0,
                "tap_unexpected_fail": 0,
                "tap_todo": 0,
                "tap_skip": 0,
                "observed_incomplete_files": 0,
                "unobserved_files": 0,
                "discovered_files": 2,
                "not_selected_files": 0,
            }
        elif case_id == "STD-02":
            proof["accounting"] = {"discovered": 657, "executed": 657, "incomplete": 0, "result_counts": {"PASS": 657, "FAIL": 0, "TBROK": 0, "TCONF": 0, "TIMEOUT": 0}}
        elif case_id == "STD-03":
            proof["accounting"] = {"selected_seed_count": 3, "selected_seeds": [1, 2, 3], "executed": 3, "duration_seconds_per_seed": 900, "cap_applied": False, "result_counts": {"PASS": 3, "FAIL": 0, "TIMEOUT": 0}}
        elif case_id == "STD-04":
            proof["checks"].append({"name": "reference-ext4-scope", "status": "PASS", "evidence": {"filesystem": {"fstype": "ext4"}}})
            proof["coverage"] = {"profile": self.profile, "axes": {"reference": {"values": ["ext4"], "checks": {"ext4": "reference-ext4-scope"}}}}
            proof["accounting"] = {"selected_seeds": list(range(1, 11)), "executed": 10, "operations_per_seed": 10000, "result_counts": {"PASS": 10, "FAIL": 0, "INCONCLUSIVE": 0}}
        return proof

    @staticmethod
    def deep_update(target: dict[str, object], updates: dict[str, object]) -> None:
        for key, value in updates.items():
            if isinstance(value, dict) and isinstance(target.get(key), dict):
                AccountingFixture.deep_update(target[key], value)  # type: ignore[index]
            else:
                target[key] = value

    def write_manifest(self) -> Path:
        manifest = self.root / "manifest.json"
        write_json(manifest, {"schema_version": 1, "suites": self.entries})
        return manifest

    def analyze(self):
        manifest = self.write_manifest()
        return accounting.analyze_manifest(manifest, self.root, self.profile, self.matrix)


class AccountingDriverTests(unittest.TestCase):
    def make_fixture(self, *, profile: str = "full") -> tuple[tempfile.TemporaryDirectory, AccountingFixture]:
        tmp = tempfile.TemporaryDirectory(prefix="afs-accounting-driver-")
        fixture = AccountingFixture(Path(tmp.name), profile=profile)
        for case_id in accounting.SUITE_IDS:
            fixture.add_suite(case_id)
        return tmp, fixture

    def assertBlocked(self, fixture: AccountingFixture, text: str | None = None) -> None:
        with self.assertRaises(accounting.AccountingError) as ctx:
            fixture.analyze()
        self.assertEqual(ctx.exception.status, "BLOCKED")
        if text is not None:
            self.assertIn(text, str(ctx.exception))


    def rewrite_std04_trace(self, fixture: AccountingFixture, mutate) -> None:
        artifact_root = Path(fixture.entries[3]["artifact_root"])
        seeds_path = artifact_root / "seed-results.json"
        seeds = json.loads(seeds_path.read_text())
        trace_path = artifact_root / seeds[0]["trace"]
        trace = json.loads(trace_path.read_text())
        mutate(trace)
        write_json(trace_path, trace)
        seeds[0]["trace_sha256"] = accounting.hashlib.sha256(json.dumps(trace, sort_keys=True, separators=(",", ":")).encode()).hexdigest()
        write_json(seeds_path, seeds)
        for item in fixture.entries[3]["raw_artifacts"]:  # type: ignore[index]
            if item["path"] == seeds[0]["trace"]:
                item["sha256"] = sha256_file(trace_path)
            if item["path"] == "seed-results.json":
                item["sha256"] = sha256_file(seeds_path)

    def test_complete_full_manifest_is_ready_after_original_raw_replay(self):
        tmp, fixture = self.make_fixture()
        self.addCleanup(tmp.cleanup)
        suites, checks = fixture.analyze()
        self.assertEqual({suite["case_id"]: suite["status"] for suite in suites}, {case_id: "PASS" for case_id in accounting.SUITE_IDS})
        self.assertTrue(any(check["name"] == "same-candidate-binding" and check["status"] == "PASS" for check in checks))
        formal = next(check for check in checks if check["name"] == "formal-std05-pass-readiness")
        self.assertEqual(formal["status"], "PASS")
        self.assertIn("hash-bound target/reference raw replay", formal["evidence"]["required_evidence"])
        self.assertEqual(accounting.status_from_checks(checks)[0], "PASS")
        self.assertNotEqual(suites[0]["accounting"]["unit"], suites[1]["accounting"]["unit"])

    def test_missing_and_duplicate_suites_block_exact_manifest_shape(self):
        tmp, fixture = self.make_fixture()
        self.addCleanup(tmp.cleanup)
        fixture.entries = fixture.entries[:-1]
        self.assertBlocked(fixture, "missing")
        tmp2, fixture2 = self.make_fixture()
        self.addCleanup(tmp2.cleanup)
        fixture2.entries.append(dict(fixture2.entries[0]))
        self.assertBlocked(fixture2, "duplicates")

    def test_proof_hash_and_path_containment_are_enforced(self):
        tmp, fixture = self.make_fixture()
        self.addCleanup(tmp.cleanup)
        fixture.entries[0]["proof_sha256"] = "0" * 64
        self.assertBlocked(fixture, "proof sha256 mismatch")
        tmp2, fixture2 = self.make_fixture()
        self.addCleanup(tmp2.cleanup)
        outside = Path(tmp2.name) / "outside-proof.json"
        write_json(outside, {"case_id": "STD-01"})
        fixture2.entries[0]["proof_path"] = str(outside)
        fixture2.entries[0]["proof_sha256"] = sha256_file(outside)
        self.assertBlocked(fixture2, "outside artifact_root")

    def test_raw_artifact_hash_and_relative_paths_are_enforced(self):
        tmp, fixture = self.make_fixture()
        self.addCleanup(tmp.cleanup)
        fixture.entries[1]["raw_artifacts"] = [{"path": "../escape.log", "sha256": "x"}]
        self.assertBlocked(fixture, "safe relative path")
        tmp2, fixture2 = self.make_fixture()
        self.addCleanup(tmp2.cleanup)
        fixture2.entries[1]["raw_artifacts"][0]["sha256"] = "0" * 64  # type: ignore[index]
        self.assertBlocked(fixture2, "raw artifact sha256 mismatch")

    def test_candidate_meta_and_profile_mismatch_block(self):
        tmp, fixture = self.make_fixture()
        self.addCleanup(tmp.cleanup)
        fixture.entries[2]["candidate"]["meta"] = "etcd"  # type: ignore[index]
        self.assertBlocked(fixture, "candidate meta mismatch")
        tmp2, fixture2 = self.make_fixture(profile="smoke")
        self.addCleanup(tmp2.cleanup)
        fixture2.entries[2]["candidate"]["profile"] = "full"  # type: ignore[index]
        self.assertBlocked(fixture2, "candidate profile mismatch")

    def test_tap_todo_skip_and_lost_items_do_not_pass(self):
        tmp = tempfile.TemporaryDirectory(prefix="afs-accounting-driver-")
        self.addCleanup(tmp.cleanup)
        fixture = AccountingFixture(Path(tmp.name))
        fixture.add_suite("STD-01", proof_updates={"accounting": {"tap_todo": 1}})
        for case_id in ("STD-02", "STD-03", "STD-04"):
            fixture.add_suite(case_id)
        self.assertBlocked(fixture, "TAP receipt contradicts original replay")
        tmp2 = tempfile.TemporaryDirectory(prefix="afs-accounting-driver-")
        self.addCleanup(tmp2.cleanup)
        fixture2 = AccountingFixture(Path(tmp2.name))
        fixture2.add_suite("STD-01", proof_updates={"accounting": {"unobserved_files": 1}})
        for case_id in ("STD-02", "STD-03", "STD-04"):
            fixture2.add_suite(case_id)
        suites2, _checks2 = fixture2.analyze()
        self.assertEqual(next(s for s in suites2 if s["case_id"] == "STD-01")["status"], "BLOCKED")

    def test_ltp_nonpass_timeout_and_full_selection_caps_are_nonpass(self):
        tmp = tempfile.TemporaryDirectory(prefix="afs-accounting-driver-")
        self.addCleanup(tmp.cleanup)
        fixture = AccountingFixture(Path(tmp.name))
        fixture.add_suite("STD-01")
        fixture.add_suite("STD-02", proof_updates={"accounting": {"result_counts": {"PASS": 656, "TCONF": 1}, "executed": 657}})
        fixture.add_suite("STD-03")
        fixture.add_suite("STD-04")
        suites, _checks = fixture.analyze()
        self.assertEqual(next(s for s in suites if s["case_id"] == "STD-02")["status"], "BLOCKED")
        tmp2 = tempfile.TemporaryDirectory(prefix="afs-accounting-driver-")
        self.addCleanup(tmp2.cleanup)
        fixture2 = AccountingFixture(Path(tmp2.name))
        fixture2.add_suite("STD-01")
        fixture2.add_suite("STD-02", proof_updates={"accounting": {"discovered": 657, "executed": 1, "incomplete": 656, "result_counts": {"PASS": 1}}})
        fixture2.add_suite("STD-03")
        fixture2.add_suite("STD-04")
        self.assertBlocked(fixture2, "executed LTP command IDs do not match frozen selection")

    def test_ltp_event_applicability_can_explain_tconf_without_reclassifying_raw_summary(self):
        tmp = tempfile.TemporaryDirectory(prefix="afs-accounting-driver-")
        self.addCleanup(tmp.cleanup)
        fixture = AccountingFixture(Path(tmp.name))
        fixture.add_suite("STD-01")
        fixture.add_suite(
            "STD-02",
            proof_updates={
                "accounting": {"result_counts": {"PASS": 656, "TCONF": 1}, "executed": 657, "incomplete": 0},
                "applicability": {"enabled": True, "status": "PASS", "pre_reviewed_event_count": 1, "unmatched_event_count": 0},
            },
        )
        fixture.add_suite("STD-03")
        fixture.add_suite("STD-04")
        suites, _checks = fixture.analyze()
        std02 = next(s for s in suites if s["case_id"] == "STD-02")
        self.assertEqual(std02["status"], "PASS")
        self.assertEqual(std02["accounting"]["applicability_status"], "PASS")

    def test_fsx_and_random_full_caps_block(self):
        tmp = tempfile.TemporaryDirectory(prefix="afs-accounting-driver-")
        self.addCleanup(tmp.cleanup)
        fixture = AccountingFixture(Path(tmp.name))
        fixture.add_suite("STD-01")
        fixture.add_suite("STD-02")
        fixture.add_suite("STD-03", proof_updates={"accounting": {"selected_seed_count": 1, "selected_seeds": [1], "executed": 1, "duration_seconds_per_seed": 1, "cap_applied": True, "result_counts": {"PASS": 1}}})
        fixture.add_suite("STD-04")
        suites, _checks = fixture.analyze()
        self.assertEqual(next(s for s in suites if s["case_id"] == "STD-03")["status"], "BLOCKED")
        tmp2 = tempfile.TemporaryDirectory(prefix="afs-accounting-driver-")
        self.addCleanup(tmp2.cleanup)
        fixture2 = AccountingFixture(Path(tmp2.name))
        fixture2.add_suite("STD-01")
        fixture2.add_suite("STD-02")
        fixture2.add_suite("STD-03")
        fixture2.add_suite("STD-04", proof_updates={"accounting": {"selected_seeds": [1], "executed": 1, "operations_per_seed": 500, "result_counts": {"PASS": 1}}})
        suites2, _checks2 = fixture2.analyze()
        self.assertEqual(next(s for s in suites2 if s["case_id"] == "STD-04")["status"], "BLOCKED")

    def test_unreviewed_postrun_reference_only_policy_blocks(self):
        tmp, fixture = self.make_fixture()
        self.addCleanup(tmp.cleanup)
        fixture.entries[0]["difference_policy"] = {"errno_differences": [{"errno": "EIO"}], "skip_differences": []}
        self.assertBlocked(fixture, "frozen pre-review policy artifact is required")
        tmp2, fixture2 = self.make_fixture()
        self.addCleanup(tmp2.cleanup)
        fixture2.entries[0]["pre_run_exclusions"] = [{"id": "skip", "reviewed_before_run": True, "post_run": True}]
        self.assertBlocked(fixture2, "frozen pre-review policy artifact is required")
        tmp3, fixture3 = self.make_fixture()
        self.addCleanup(tmp3.cleanup)
        fixture3.entries[0]["difference_policy"] = {"errno_differences": [], "skip_differences": [{"skip": "x", "pre_reviewed": True, "reference_only": True, "explanation": "ext4 only"}]}
        self.assertBlocked(fixture3, "frozen pre-review policy artifact is required")

    def test_errno_and_skip_differences_need_explanations(self):
        tmp, fixture = self.make_fixture()
        self.addCleanup(tmp.cleanup)
        fixture.entries[0]["difference_policy"] = {"errno_differences": [{"pre_reviewed": True}], "skip_differences": []}
        self.assertBlocked(fixture, "frozen pre-review policy artifact is required")

    def test_random_requires_internal_reference_pairing(self):
        tmp, fixture = self.make_fixture()
        self.addCleanup(tmp.cleanup)
        fixture.entries[3]["reference"] = {"filesystem": "ext4", "compatible": True}
        self.assertBlocked(fixture, "internal ext4/AFS operation pairing")

    def test_target_raw_failure_overrides_child_pass_receipt(self):
        tmp, fixture = self.make_fixture()
        self.addCleanup(tmp.cleanup)
        artifact_root = Path(fixture.entries[2]["artifact_root"])
        commands_path = artifact_root / "commands.json"
        commands = json.loads(commands_path.read_text())
        stdout_path = artifact_root / commands[0]["artifacts"]["stdout"]
        stdout_path.write_text("fsx failed before completion\n", encoding="utf-8")
        for item in fixture.entries[2]["raw_artifacts"]:  # type: ignore[index]
            if item["path"] == commands[0]["artifacts"]["stdout"]:
                item["sha256"] = sha256_file(stdout_path)
        self.assertBlocked(fixture, "receipt contradicts original replay")

    def test_unreviewed_raw_errno_difference_blocks(self):
        tmp, fixture = self.make_fixture()
        self.addCleanup(tmp.cleanup)
        artifact_root = Path(fixture.entries[1]["artifact_root"])
        commands_path = artifact_root / "commands.json"
        commands = json.loads(commands_path.read_text())
        stdout_path = artifact_root / commands[0]["artifacts"]["stdout"]
        stdout_path.write_text("ltp-0 1 TFAIL: raw target failure\n", encoding="utf-8")
        for item in fixture.entries[1]["raw_artifacts"]:  # type: ignore[index]
            if item["path"] == commands[0]["artifacts"]["stdout"]:
                item["sha256"] = sha256_file(stdout_path)
        self.assertBlocked(fixture, "receipt contradicts original replay")

    def test_pre_reviewed_errno_difference_is_explained_but_raw_failure_still_fails(self):
        tmp, fixture = self.make_fixture()
        self.addCleanup(tmp.cleanup)
        policy = {"case_id": "STD-02", "status": "FROZEN", "reviewed_before_run": True, "post_run": False}
        policy_path = Path(tmp.name) / "std02-policy.json"
        write_json(policy_path, policy)
        artifact_root = Path(fixture.entries[1]["artifact_root"])
        commands_path = artifact_root / "commands.json"
        commands = json.loads(commands_path.read_text())
        commands[0]["result"] = "FAIL"
        stdout_path = artifact_root / commands[0]["artifacts"]["stdout"]
        stdout_path.write_text("ltp-0 1 TFAIL: unsupported Linux extension\n", encoding="utf-8")
        test_id = accounting.stable_command_key(commands[0])
        write_json(commands_path, commands)
        for item in fixture.entries[1]["raw_artifacts"]:  # type: ignore[index]
            if item["path"] == "commands.json":
                item["sha256"] = sha256_file(commands_path)
            if item["path"] == commands[0]["artifacts"]["stdout"]:
                item["sha256"] = sha256_file(stdout_path)
        fixture.entries[1]["difference_policy"] = {"errno_differences": [
            {"test_id": test_id, "pre_reviewed": True, "post_run": False, "explanation": "unsupported Linux extension returns a documented failure"},
            {"test_id": f"{test_id}:event-0", "pre_reviewed": True, "post_run": False, "explanation": "same parsed LTP event identity for the command failure"},
        ], "skip_differences": []}
        fixture.entries[1]["pre_review_policy"] = {"path": str(policy_path), "sha256": sha256_file(policy_path)}
        suites, _checks = fixture.analyze()
        raw_check = next(check for check in next(s for s in suites if s["case_id"] == "STD-02")["checks"] if check["name"] == "raw-semantic-replay")
        self.assertEqual(raw_check["status"], "FAIL")
        self.assertEqual(raw_check["evidence"]["policy_explained_difference_count"], 2)

    def test_unreviewed_raw_skip_difference_blocks(self):
        tmp, fixture = self.make_fixture()
        self.addCleanup(tmp.cleanup)
        artifact_root = Path(fixture.entries[0]["artifact_root"])
        tap_path = artifact_root / "tap-accounting.json"
        tap = json.loads(tap_path.read_text())
        tap["tap_ok"] = 3
        tap["tap_skip"] = 1
        tap["skip_lines"] = ["ok 1 - open/00.t # SKIP target skipped"]
        stdout_path = artifact_root / "pjdfstest.stdout.tap"
        stdout_path.write_text("chmod/00.t ..\n1..1\nok 1 - chmod/00.t\nok\nopen/00.t ..\n1..1\nok 1 - open/00.t # SKIP target skipped\nok\nrename/00.t ..\n1..1\nok 1 - rename/00.t\nok\nFiles=3, Tests=3, Result: PASS\n", encoding="utf-8")
        write_json(tap_path, tap)
        for item in fixture.entries[0]["raw_artifacts"]:  # type: ignore[index]
            if item["path"] == "tap-accounting.json":
                item["sha256"] = sha256_file(tap_path)
            if item["path"] == "pjdfstest.stdout.tap":
                item["sha256"] = sha256_file(stdout_path)
        suites, _checks = fixture.analyze()
        raw_check = next(check for check in next(s for s in suites if s["case_id"] == "STD-01")["checks"] if check["name"] == "raw-semantic-replay")
        self.assertEqual(raw_check["status"], "BLOCKED")
        self.assertTrue(raw_check["evidence"].get("missing_reference_ids"))

    def test_injected_raw_semantics_receipts_are_rejected(self):
        tmp, fixture = self.make_fixture()
        self.addCleanup(tmp.cleanup)
        artifact_root = Path(fixture.entries[0]["artifact_root"])
        tap_path = artifact_root / "tap-accounting.json"
        tap = json.loads(tap_path.read_text())
        tap["raw_semantics"] = [
            {"id": "chmod/00.t", "status": "PASS"},
            {"id": "open/00.t", "status": "PASS"},
            {"id": "rename/00.t", "status": "PASS"},
        ]
        write_json(tap_path, tap)
        for item in fixture.entries[0]["raw_artifacts"]:  # type: ignore[index]
            if item["path"] == "tap-accounting.json":
                item["sha256"] = sha256_file(tap_path)
        self.assertBlocked(fixture, "derived raw_semantics")

    def test_coverage_axes_must_reference_pass_checks(self):
        tmp = tempfile.TemporaryDirectory(prefix="afs-accounting-driver-")
        self.addCleanup(tmp.cleanup)
        fixture = AccountingFixture(Path(tmp.name))
        fixture.add_suite("STD-01", proof_updates={"coverage": {"axes": {"reference": {"values": ["ext4"], "checks": {"ext4": "missing-check"}}}}})
        for case_id in ("STD-02", "STD-03", "STD-04"):
            fixture.add_suite(case_id)
        suites, _checks = fixture.analyze()
        self.assertEqual(next(s for s in suites if s["case_id"] == "STD-01")["status"], "BLOCKED")

    def test_suite_fail_cannot_be_turned_pass_by_counts_only_conservation(self):
        tmp = tempfile.TemporaryDirectory(prefix="afs-accounting-driver-")
        self.addCleanup(tmp.cleanup)
        fixture = AccountingFixture(Path(tmp.name))
        fixture.add_suite("STD-01", proof_updates={"status": "FAIL", "reason": "actual suite failure", "accounting": {"tap_ok": 2, "tap_not_ok": 1, "tap_unexpected_fail": 1}})
        artifact_root = Path(fixture.entries[0]["artifact_root"])
        stdout_path = artifact_root / "pjdfstest.stdout.tap"
        stdout_path.write_text("chmod/00.t ..\n1..1\nok 1 - chmod/00.t\nok\nopen/00.t ..\n1..1\nnot ok 1 - open/00.t\nnot ok\nrename/00.t ..\n1..1\nok 1 - rename/00.t\nok\nFiles=3, Tests=3, Result: FAIL\n", encoding="utf-8")
        for item in fixture.entries[0]["raw_artifacts"]:  # type: ignore[index]
            if item["path"] == "pjdfstest.stdout.tap":
                item["sha256"] = sha256_file(stdout_path)
        for case_id in ("STD-02", "STD-03", "STD-04"):
            fixture.add_suite(case_id)
        suites, _checks = fixture.analyze()
        std01 = next(s for s in suites if s["case_id"] == "STD-01")
        self.assertEqual(std01["status"], "FAIL")

    def test_ltp_stable_identity_includes_command_not_only_test_id(self):
        tmp, fixture = self.make_fixture()
        self.addCleanup(tmp.cleanup)
        artifact_root = Path(fixture.entries[1]["artifact_root"])
        discovery_path = artifact_root / "discovery.json"
        commands_path = artifact_root / "commands.json"
        discovery = json.loads(discovery_path.read_text())
        discovery["selected"] = [
            {"test_id": "same-name", "command": ["kirk", "a"]},
            {"test_id": "same-name", "command": ["kirk", "b"]},
        ]
        commands = json.loads(commands_path.read_text())[:2]
        commands[0]["test_id"] = "same-name"; commands[0]["command"] = ["kirk", "a"]
        commands[1]["test_id"] = "same-name"; commands[1]["command"] = ["kirk", "b"]
        write_json(discovery_path, discovery)
        write_json(commands_path, commands)
        for item in fixture.entries[1]["raw_artifacts"]:  # type: ignore[index]
            if item["path"] == "discovery.json":
                item["sha256"] = sha256_file(discovery_path)
            if item["path"] == "commands.json":
                item["sha256"] = sha256_file(commands_path)
        suites, _checks = fixture.analyze()
        self.assertEqual(next(s for s in suites if s["case_id"] == "STD-02")["checks"][2]["status"], "PASS")

    def test_ltp_selected_observed_command_mismatch_blocks(self):
        tmp, fixture = self.make_fixture()
        self.addCleanup(tmp.cleanup)
        artifact_root = Path(fixture.entries[1]["artifact_root"])
        commands_path = artifact_root / "commands.json"
        commands = json.loads(commands_path.read_text())
        commands[0]["command"] = ["kirk", "different"]
        write_json(commands_path, commands)
        for item in fixture.entries[1]["raw_artifacts"]:  # type: ignore[index]
            if item["path"] == "commands.json":
                item["sha256"] = sha256_file(commands_path)
        self.assertBlocked(fixture, "executed LTP command IDs do not match frozen selection")

    def test_missing_proof_sha_blocks(self):
        tmp, fixture = self.make_fixture()
        self.addCleanup(tmp.cleanup)
        fixture.entries[0].pop("proof_sha256")
        self.assertBlocked(fixture, "proof_sha256 is required")

    def test_candidate_sha_must_match_original_identity(self):
        tmp, fixture = self.make_fixture()
        self.addCleanup(tmp.cleanup)
        fixture.entries[0]["candidate"]["node_sha256"] = "c" * 64  # type: ignore[index]
        self.assertBlocked(fixture, "node_sha256 does not match original identity")

    def test_ext4_compatible_boolean_without_hash_bound_reference_blocks(self):
        tmp, fixture = self.make_fixture()
        self.addCleanup(tmp.cleanup)
        fixture.entries[0]["reference"] = {"filesystem": "ext4", "compatible": True}
        self.assertBlocked(fixture, "hash-bound ext4 reference proof is required")

    def test_missing_original_stable_ids_block(self):
        tmp, fixture = self.make_fixture()
        self.addCleanup(tmp.cleanup)
        artifact_root = Path(fixture.entries[0]["artifact_root"])
        tap_path = artifact_root / "tap-accounting.json"
        tap = json.loads(tap_path.read_text())
        tap.pop("selected_tests", None)
        write_json(tap_path, tap)
        for item in fixture.entries[0]["raw_artifacts"]:  # type: ignore[index]
            if item["path"] == "tap-accounting.json":
                item["sha256"] = sha256_file(tap_path)
        self.assertBlocked(fixture, "original stable selected TAP test IDs")


    def test_missing_std01_original_tap_artifact_blocks_even_with_receipt(self):
        tmp, fixture = self.make_fixture()
        self.addCleanup(tmp.cleanup)
        artifact_root = Path(fixture.entries[0]["artifact_root"])
        stdout_path = artifact_root / "pjdfstest.stdout.tap"
        stdout_path.unlink()
        fixture.entries[0]["raw_artifacts"] = [item for item in fixture.entries[0]["raw_artifacts"] if item["path"] != "pjdfstest.stdout.tap"]  # type: ignore[index]
        self.assertBlocked(fixture, "raw artifact is not hash-bound")

    def test_changed_std01_original_tap_blocks_before_receipt_replay(self):
        tmp, fixture = self.make_fixture()
        self.addCleanup(tmp.cleanup)
        artifact_root = Path(fixture.entries[0]["artifact_root"])
        stdout_path = artifact_root / "pjdfstest.stdout.tap"
        stdout_path.write_text("chmod/00.t ..\n1..1\nnot ok 1 - chmod/00.t\nnot ok\nFiles=1, Tests=1, Result: FAIL\n", encoding="utf-8")
        for item in fixture.entries[0]["raw_artifacts"]:  # type: ignore[index]
            if item["path"] == "pjdfstest.stdout.tap":
                item["sha256"] = sha256_file(stdout_path)
        self.assertBlocked(fixture, "TAP receipt contradicts original replay")


    def test_reference_preparation_only_packet_cannot_be_target_proof(self):
        tmp, fixture = self.make_fixture()
        self.addCleanup(tmp.cleanup)
        artifact_root = Path(fixture.entries[3]["artifact_root"])
        proof_path = artifact_root / "proof.json"
        proof = json.loads(proof_path.read_text())
        proof["kind"] = "STD04_EXT4_REFERENCE_PREPARATION_ONLY"
        proof["reference_preparation_only"] = True
        write_json(proof_path, proof)
        fixture.entries[3]["proof_sha256"] = sha256_file(proof_path)
        self.assertBlocked(fixture, "reference-preparation-only")


    def test_std04_trace_requires_explicit_result_dicts(self):
        tmp, fixture = self.make_fixture()
        self.addCleanup(tmp.cleanup)
        def mutate(trace):
            trace[0].pop("reference_result", None)
            trace[0].pop("target_result", None)
        self.rewrite_std04_trace(fixture, mutate)
        self.assertBlocked(fixture, "result payload")

    def test_std04_trace_requires_valid_tree_hashes_and_entry_counts(self):
        tmp, fixture = self.make_fixture()
        self.addCleanup(tmp.cleanup)
        def mutate(trace):
            trace[0].pop("reference_tree_sha256", None)
            trace[0].pop("target_tree_sha256", None)
            trace[0]["reference_entry_count"] = True
            trace[0]["target_entry_count"] = True
        self.rewrite_std04_trace(fixture, mutate)
        self.assertBlocked(fixture, "tree digest")

    def test_std04_trace_operation_stream_is_semantically_replayed(self):
        tmp, fixture = self.make_fixture()
        self.addCleanup(tmp.cleanup)
        artifact_root = Path(fixture.entries[3]["artifact_root"])
        seeds_path = artifact_root / "seed-results.json"
        seeds = json.loads(seeds_path.read_text())
        trace_path = artifact_root / seeds[0]["trace"]
        trace = json.loads(trace_path.read_text())
        trace[0]["operation"] = {"index": 0, "op": "stat", "path": "forged-counts-only"}
        write_json(trace_path, trace)
        seeds[0]["trace_sha256"] = accounting.hashlib.sha256(json.dumps(trace, sort_keys=True, separators=(",", ":")).encode()).hexdigest()
        write_json(seeds_path, seeds)
        for item in fixture.entries[3]["raw_artifacts"]:  # type: ignore[index]
            if item["path"] == seeds[0]["trace"]:
                item["sha256"] = sha256_file(trace_path)
            if item["path"] == "seed-results.json":
                item["sha256"] = sha256_file(seeds_path)
        self.assertBlocked(fixture, "trace semantic audit failed")

    def test_std04_trace_hash_and_length_are_enforced(self):
        tmp, fixture = self.make_fixture()
        self.addCleanup(tmp.cleanup)
        artifact_root = Path(fixture.entries[3]["artifact_root"])
        seeds_path = artifact_root / "seed-results.json"
        seeds = json.loads(seeds_path.read_text())
        trace_path = artifact_root / seeds[0]["trace"]
        trace = json.loads(trace_path.read_text())
        trace.append({"index": len(trace), "operation": {"op": "stat", "path": "extra"}, "reference_result": {"ok": True}, "target_result": {"ok": True}})
        write_json(trace_path, trace)
        for item in fixture.entries[3]["raw_artifacts"]:  # type: ignore[index]
            if item["path"] == seeds[0]["trace"]:
                item["sha256"] = sha256_file(trace_path)
        self.assertBlocked(fixture, "trace sha256 contradicts seed-results receipt")

    def test_std04_missing_trace_binding_blocks_receipt_only_promotion(self):
        tmp, fixture = self.make_fixture()
        self.addCleanup(tmp.cleanup)
        artifact_root = Path(fixture.entries[3]["artifact_root"])
        seeds = json.loads((artifact_root / "seed-results.json").read_text())
        missing_rel = seeds[0]["trace"]
        fixture.entries[3]["raw_artifacts"] = [item for item in fixture.entries[3]["raw_artifacts"] if item["path"] != missing_rel]  # type: ignore[index]
        self.assertBlocked(fixture, "raw artifact is not hash-bound")

    def test_frozen_fixture_identity_without_node_meta_process_is_rejected(self):
        repo = Path(__file__).resolve().parents[2]
        fixture_path = repo / "evidence" / "afs-delivery" / "suite-binding-v93" / "ctl" / "ext4-reference" / "STD-01" / "artifacts" / "std-01-pjdfstest" / "identity.json"
        if not fixture_path.exists():
            self.skipTest("frozen STD-01 identity fixture not present")
        identity = json.loads(fixture_path.read_text())
        with self.assertRaises(accounting.AccountingError) as ctx:
            accounting.check_stable_process_identity("STD-01", identity)
        self.assertEqual(ctx.exception.status, "BLOCKED")
        self.assertIn("original stable node process identity is missing", str(ctx.exception))


if __name__ == "__main__":
    unittest.main()
