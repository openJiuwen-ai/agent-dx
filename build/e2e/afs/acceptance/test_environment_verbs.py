#!/usr/bin/env python3
"""Cross-VM verbs preparation predicate, seeded by retained Linux observations."""

from __future__ import annotations

import json
import shutil
import tempfile
import unittest
from pathlib import Path

import environment
import test_environment


FIXTURE = Path(__file__).resolve().parent / "fixtures/verbs-preparation"


class VerbsEnvironmentTests(unittest.TestCase):
    def make_bundle(self, root: Path) -> dict:
        bundle = test_environment.EnvironmentEvaluatorTests().make_bundle(root, initial_available=120 * test_environment.GIB)
        verbs = root / "verbs"
        shutil.copytree(FIXTURE, verbs)
        bundle["verbs"] = {"prefix": "verbs"}
        for path in verbs.rglob("*"):
            if path.is_file():
                bundle["artifact_references"][path.relative_to(root).as_posix()] = test_environment.sha(path)
        return bundle

    def outcome(self, root: Path, bundle: dict) -> dict:
        report = environment.evaluate_environment(test_environment.lock(bundle["contract"]["sha256"]), bundle, root)
        self.assertNotEqual("PASS", report["status"], "verbs preparation cannot qualify complete ENV")
        return next(item for item in report["checks"] if item["name"] == "cross-vm-verbs")

    def rehash(self, root: Path, bundle: dict, rel: str, *, update_manifest: bool = True) -> None:
        bundle["artifact_references"][rel] = test_environment.sha(root / rel)
        if update_manifest and rel.startswith("verbs/") and rel != "verbs/artifacts-manifest.json":
            manifest_path = root / "verbs/artifacts-manifest.json"
            manifest = json.loads(manifest_path.read_text())
            subrel = rel.removeprefix("verbs/")
            if subrel in manifest["files"]:
                manifest["files"][subrel] = bundle["artifact_references"][rel]
                test_environment.write_json(manifest_path, manifest)
                bundle["artifact_references"]["verbs/artifacts-manifest.json"] = test_environment.sha(manifest_path)

    def edit_json(self, root: Path, bundle: dict, rel: str, edit, *, update_manifest: bool = True) -> None:
        path = root / rel
        value = json.loads(path.read_text())
        edit(value)
        test_environment.write_json(path, value)
        self.rehash(root, bundle, rel, update_manifest=update_manifest)

    def write_jsonl(self, root: Path, bundle: dict, rel: str, rows: list[dict], *, update_manifest: bool = True) -> None:
        path = root / rel
        path.write_text("".join(json.dumps(row) + "\n" for row in rows), encoding="utf-8")
        self.rehash(root, bundle, rel, update_manifest=update_manifest)

    def commands(self, root: Path) -> list[dict]:
        return [json.loads(line) for line in (root / "verbs/commands.jsonl").read_text().splitlines() if line.strip()]

    def test_retained_valid_verbs_observations_pass_only_verbs_predicate(self):
        with tempfile.TemporaryDirectory() as td:
            root = Path(td); bundle = self.make_bundle(root)
            self.assertEqual("PASS", self.outcome(root, bundle)["status"])

    def test_generic_pass_receipt_is_not_verbs_evidence(self):
        with tempfile.TemporaryDirectory() as td:
            root = Path(td); bundle = self.make_bundle(root)
            bundle["verbs"] = {"status": "PASS", "summary": {"PASS": 127}}
            self.assertNotEqual("PASS", self.outcome(root, bundle)["status"])

    def test_missing_artifacts_are_blocked(self):
        rels = (
            "verbs/runs.jsonl",
            "verbs/matrix.json",
            "verbs/commands.jsonl",
            "verbs/inputs/env_verbs-checker.py",
            "verbs/runtime/a/pairs/a-b-256/142edaab-04ea-4d80-82c1-0ef5e184225d-client.json",
            "verbs/runtime/a/pairs/a-b-256/142edaab-04ea-4d80-82c1-0ef5e184225d-client.raw.log",
        )
        for rel in rels:
            with self.subTest(rel=rel), tempfile.TemporaryDirectory() as td:
                root = Path(td); bundle = self.make_bundle(root)
                del bundle["artifact_references"][rel]
                self.assertEqual("BLOCKED", self.outcome(root, bundle)["status"])

    def test_tampered_hash_bound_artifact_fails(self):
        with tempfile.TemporaryDirectory() as td:
            root = Path(td); bundle = self.make_bundle(root)
            (root / "verbs/runtime/a/pairs/a-b-256/142edaab-04ea-4d80-82c1-0ef5e184225d-client.json").write_text("{}\n", encoding="utf-8")
            self.assertEqual("FAIL", self.outcome(root, bundle)["status"])

    def test_rehashed_bad_endpoint_semantics_fail(self):
        rel = "verbs/runtime/a/pairs/a-b-256/142edaab-04ea-4d80-82c1-0ef5e184225d-client.json"
        cases = (
            ("wrong-role", lambda v: v.update(role="server")),
            ("wrong-bind", lambda v: v.update(bind="127.0.0.1")),
            ("bool-pid", lambda v: v["process"].update(pid=True)),
            ("missing-mr", lambda v: v.update(raw_log=v["raw_log"].replace("RDMA addr", "RDMA bogus", 1))),
            ("wrong-size", lambda v: v.update(size=128)),
        )
        for name, edit in cases:
            with self.subTest(name=name), tempfile.TemporaryDirectory() as td:
                root = Path(td); bundle = self.make_bundle(root)
                self.edit_json(root, bundle, rel, edit)
                self.assertEqual("FAIL", self.outcome(root, bundle)["status"])

    def test_duplicate_or_missing_direction_fails(self):
        for mutation in ("duplicate", "missing"):
            with self.subTest(mutation=mutation), tempfile.TemporaryDirectory() as td:
                root = Path(td); bundle = self.make_bundle(root)
                rows = [json.loads(line) for line in (root / "verbs/runs.jsonl").read_text().splitlines()]
                if mutation == "duplicate":
                    rows[-1] = dict(rows[0])
                else:
                    rows.pop()
                self.write_jsonl(root, bundle, "verbs/runs.jsonl", rows)
                self.assertEqual("FAIL", self.outcome(root, bundle)["status"])

    def test_command_identity_is_bound(self):
        mutations = ("wrong-guest", "failed-call", "wrong-bind", "missing-listen", "echo-listen", "argv-none")
        for mutation in mutations:
            with self.subTest(mutation=mutation), tempfile.TemporaryDirectory() as td:
                root = Path(td); bundle = self.make_bundle(root)
                rows = self.commands(root)
                pair_rows = [row for row in rows if any("a-b-256" in str(arg) for arg in row.get("argv", []))]
                if mutation == "wrong-guest":
                    pair_rows[0]["argv"] = ["afs-accept-c" if arg == "afs-accept-b" else arg for arg in pair_rows[0]["argv"]]
                elif mutation == "failed-call":
                    pair_rows[1]["returncode"] = 1
                elif mutation == "wrong-bind":
                    pair_rows[1]["argv"] = ["127.0.0.1" if arg == "192.168.109.12" else arg for arg in pair_rows[1]["argv"]]
                elif mutation == "missing-listen":
                    rows.remove(pair_rows[0])
                elif mutation == "echo-listen":
                    pair_rows[0]["argv"][-1] = "echo " + pair_rows[0]["argv"][-1]
                else:
                    pair_rows[0]["argv"] = None
                self.write_jsonl(root, bundle, "verbs/commands.jsonl", rows)
                self.assertEqual("FAIL", self.outcome(root, bundle)["status"])

    def test_cleanup_and_protected_state_are_required(self):
        cases = (
            ("process-changed", lambda v: v.update(processes=[])),
            ("mount-changed", lambda v: v.update(afs_mounts=[])),
            ("cm-leftover", lambda v: v["commands"]["cm_id"].update(stdout='[{"comm":"rping"}]\n')),
        )
        for name, edit in cases:
            with self.subTest(name=name), tempfile.TemporaryDirectory() as td:
                root = Path(td); bundle = self.make_bundle(root)
                self.edit_json(root, bundle, "verbs/runtime/a/protected-after.json", edit)
                self.assertEqual("FAIL", self.outcome(root, bundle)["status"])

    def test_negative_no_listener_cannot_be_faked(self):
        cases = (
            ("rc0", lambda v: v.update(returncode=0)),
            ("success-log", lambda v: v.update(raw_log="ping data: fake\nrdma read completion\n", raw_log_sha256="0" * 64)),
            ("wrong-pair", lambda v: v.update(peer="192.168.109.14")),
        )
        for name, edit in cases:
            with self.subTest(name=name), tempfile.TemporaryDirectory() as td:
                root = Path(td); bundle = self.make_bundle(root)
                rel = "verbs/runtime/a/negative-no-listener/dab656d8-8d0c-4f50-b58b-c5eb675ad9b3-client.json"
                self.edit_json(root, bundle, rel, edit)
                self.assertEqual("FAIL", self.outcome(root, bundle)["status"])

    def test_checker_collector_and_observer_identity_are_fixed(self):
        for rel in ("verbs/inputs/env_verbs-checker.py", "verbs/inputs/env_verbs.py", "verbs/inputs/observe.py", "verbs/runtime/a/env_verbs.py", "verbs/runtime/a/observe.py"):
            with self.subTest(rel=rel), tempfile.TemporaryDirectory() as td:
                root = Path(td); bundle = self.make_bundle(root)
                (root / rel).write_text("print('PASS')\n", encoding="utf-8")
                self.rehash(root, bundle, rel)
                self.assertEqual("FAIL", self.outcome(root, bundle)["status"])

    def test_malformed_shapes_return_named_failure_without_exception(self):
        cases = (
            ("verbs/commands.jsonl", "jsonl-none-argv"),
            ("verbs/commands.jsonl", "jsonl-bad-time"),
            ("verbs/runtime/a/pairs/a-b-256/142edaab-04ea-4d80-82c1-0ef5e184225d-client.json", "json-array"),
        )
        for rel, mutation in cases:
            with self.subTest(mutation=mutation), tempfile.TemporaryDirectory() as td:
                root = Path(td); bundle = self.make_bundle(root)
                if mutation == "json-array":
                    (root / rel).write_text("[]\n", encoding="utf-8")
                    self.rehash(root, bundle, rel)
                else:
                    rows = self.commands(root)
                    if mutation == "jsonl-none-argv":
                        rows[0]["argv"] = None
                    else:
                        rows[0]["started"] = None
                    self.write_jsonl(root, bundle, rel, rows)
                self.assertEqual("FAIL", self.outcome(root, bundle)["status"])

    def test_resource_observation_must_be_complete_and_parsed(self):
        for name in ("missing-qp", "invalid-qp-json", "failed-qp", "missing-endpoint-resources"):
            with self.subTest(name=name), tempfile.TemporaryDirectory() as td:
                root = Path(td); bundle = self.make_bundle(root)
                if name == "missing-endpoint-resources":
                    rel = "verbs/runtime/a/pairs/a-b-256/142edaab-04ea-4d80-82c1-0ef5e184225d-client.json"
                    self.edit_json(root, bundle, rel, lambda v: v.update(resources_after={}))
                else:
                    rel = "verbs/runtime/c/protected-after.json"
                    def edit(value):
                        if name == "missing-qp":
                            value["commands"].pop("qp")
                        elif name == "invalid-qp-json":
                            value["commands"]["qp"]["stdout"] = "not JSON"
                        else:
                            value["commands"]["qp"]["returncode"] = 1
                    self.edit_json(root, bundle, rel, edit)
                self.assertEqual("FAIL", self.outcome(root, bundle)["status"])

    def test_negative_endpoint_has_same_identity_contract(self):
        rel = "verbs/runtime/a/negative-no-listener/dab656d8-8d0c-4f50-b58b-c5eb675ad9b3-client.json"
        cases = (
            ("run", lambda v: v.update(run_id="3d082f36-9130-4a67-82f4-c0f4289852bb")),
            ("process", lambda v: v.update(process={})),
            ("host", lambda v: v.update(host={})),
            ("resources", lambda v: v.update(resources_after={})),
        )
        for name, edit in cases:
            with self.subTest(name=name), tempfile.TemporaryDirectory() as td:
                root = Path(td); bundle = self.make_bundle(root)
                self.edit_json(root, bundle, rel, edit)
                self.assertEqual("FAIL", self.outcome(root, bundle)["status"])

    def test_observer_fields_and_endpoint_boot_are_bound(self):
        for name in ("missing-processes", "missing-mounts", "different-boot"):
            with self.subTest(name=name), tempfile.TemporaryDirectory() as td:
                root = Path(td); bundle = self.make_bundle(root)
                if name == "different-boot":
                    rel = "verbs/runtime/a/pairs/a-b-256/142edaab-04ea-4d80-82c1-0ef5e184225d-client.json"
                    self.edit_json(root, bundle, rel, lambda v: v["host"].update(boot_id="3d082f36-9130-4a67-82f4-c0f4289852bb"))
                else:
                    field = "processes" if name == "missing-processes" else "afs_mounts"
                    for phase in ("before", "after"):
                        rel = f"verbs/runtime/c/protected-{phase}.json"
                        self.edit_json(root, bundle, rel, lambda v: v.pop(field))
                self.assertEqual("FAIL", self.outcome(root, bundle)["status"])

    def test_command_timestamps_are_parsed_and_ordered(self):
        for name in ("nonsense", "backwards"):
            with self.subTest(name=name), tempfile.TemporaryDirectory() as td:
                root = Path(td); bundle = self.make_bundle(root)
                rows = self.commands(root)
                if name == "nonsense":
                    rows[0]["started"] = "nonsense"
                else:
                    rows[0]["started"], rows[0]["ended"] = rows[0]["ended"], rows[0]["started"]
                self.write_jsonl(root, bundle, "verbs/commands.jsonl", rows)
                self.assertEqual("FAIL", self.outcome(root, bundle)["status"])

    def test_manifest_and_matrix_are_semantic_inputs(self):
        for rel, edit in (
            ("verbs/matrix.json", lambda v: v.update(probe_sha256="0" * 64)),
            ("verbs/audit.json", lambda v: v["summary"].update(FAIL=1)),
            ("verbs/protected-files.json", lambda v: v.pop("files", None)),
        ):
            with self.subTest(rel=rel), tempfile.TemporaryDirectory() as td:
                root = Path(td); bundle = self.make_bundle(root)
                self.edit_json(root, bundle, rel, edit)
                self.assertEqual("FAIL", self.outcome(root, bundle)["status"])

    def test_exact_probe_command_must_cover_endpoint_time(self):
        for role in ("client", "server", "negative"):
            with self.subTest(role=role), tempfile.TemporaryDirectory() as td:
                root = Path(td); bundle = self.make_bundle(root)
                rows = self.commands(root)
                run_id = "dab656d8-8d0c-4f50-b58b-c5eb675ad9b3" if role == "negative" else "142edaab-04ea-4d80-82c1-0ef5e184225d"
                expected_role = "client" if role == "negative" else role
                row = next(r for r in rows if run_id in r["argv"] and expected_role in r["argv"])
                row["started"] = "2026-09-01T00:00:00.000Z"
                row["ended"] = "2026-09-01T00:00:01.000Z"
                self.write_jsonl(root, bundle, "verbs/commands.jsonl", rows)
                self.assertEqual("FAIL", self.outcome(root, bundle)["status"])


if __name__ == "__main__":
    unittest.main()
