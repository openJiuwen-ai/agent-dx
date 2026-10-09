"""Guard tests for the bounded DFS R3 multi-node cohort driver."""
from __future__ import annotations

import json
from pathlib import Path
import subprocess
import sys
import tempfile
from types import SimpleNamespace
import unittest
from unittest.mock import patch

import dfs_multinode_small as probe
import dfs_r3_write_small as write_small


def good_identity() -> dict[str, str]:
    return {
        "product_source_commit": "7" * 40,
        "compiler_input_map": "1" * 64,
        "afs_meta_sha256": "2" * 64,
        "afs_node_sha256": "3" * 64,
        "io_sha256": "4" * 64,
    }


def good_c_result(generation: int = 1, operation: str = "seq-write", wall_ns: int = 100_000_000) -> dict[str, object]:
    return {
        "dataset": write_small.DATASET,
        "generation": generation,
        "operation": operation,
        "file_bytes": 64 * 2**20,
        "io_bytes": 64 * 2**20,
        "block_bytes": 2**20,
        "concurrency": 1,
        "pattern_byte": 97,
        "operations": 64,
        "barrier": "fdatasync" if operation == "seq-write" else "close",
        "cache_requested": "unobserved",
        "residency_observed": False,
        "content_ok": True,
        "wall_ns": wall_ns,
        "client_cpu_ns": 1,
        "barrier_ns": 1,
    }


def tuple_for(operation: str, batch: int, member: str, route: str | None) -> dict[str, object]:
    return probe.write_tuple(batch, member) if operation == "write" else probe.read_tuple(batch, route or "r1", member)


def content_verify(generation: int) -> dict[str, object]:
    return {"status": "PASS", "bytes": 64 * 2**20,
            "sha256": write_small.expected_content(generation)["sha256"],
            "eof": {"status": "PASS", "offset": 64 * 2**20, "extra_bytes": 0}}


def coordinator_payload(operation: str = "write", route: str | None = None, failed_final: bool = False,
                        duplicate_ready: bool = False, bad_result: bool = False,
                        mono_raw_elapsed: int | None = 100_000_000,
                        interleaved_final: bool = False, missing_fsync: bool = False,
                        missing_pre_read: bool = False, bad_token: bool = False,
                        bad_c_done_token: bool = False,
                        bool_index: bool = False) -> str:
    lines: list[dict[str, object]] = []
    batch = 1
    identity = good_identity()
    for member in probe.MEMBERS:
        lines.append({"event": "READY", "reader_id": member, "session_token": "s",
                      "cohort_token": "c", "operation": operation, "identity": identity,
                      **tuple_for(operation, batch, member, route)})
    if duplicate_ready:
        lines[1] = dict(lines[0])
    tail: list[dict[str, object]] = []
    for member in probe.MEMBERS:
        row = tuple_for(operation, batch, member, route)
        if bool_index and member == "A":
            row = dict(row, index=True)
        generation = row["generation"]
        result = good_c_result(generation, "seq-write" if operation == "write" else "seq-read")
        if bad_result and member == "A":
            result["io_bytes"] = 1
        sample = {"result": result}
        start_token = "wrong" if bad_c_done_token and member == "A" else "s:c:start"
        c_done = {"event": "C_DONE", "reader_id": member, "session_token": "s",
                  "cohort_token": "c", "operation": operation, "start_token": start_token,
                  "status": "PASS", "rc": 0, "result": result, "identity": identity,
                  "py_monotonic_elapsed_ns": 100_000_000,
                  "py_monotonic_raw_elapsed_ns": mono_raw_elapsed, **row}
        final = {"event": "FINAL", "reader_id": member, "session_token": "s",
                 "cohort_token": "c", "operation": operation, "start_token": start_token,
                 "status": "BLOCKED" if failed_final and member == "B" else "DATA_RECORDED",
                 "identity": identity, "sample": sample, "content": write_small.expected_content(generation),
                 "content_verify": content_verify(generation),
                 "parent_dir_fsync": False if missing_fsync and member == "A" else operation == "write",
                 "root_dir_fsync": False if missing_fsync and member == "A" else operation == "write",
                 "pre_read_content_verify": None if missing_pre_read and member == "A" else content_verify(generation),
                 "source_member": row.get("source_member"), **row}
        if bad_token and member == "A":
            final["start_token"] = "wrong"
        if interleaved_final and member == "A":
            tail.extend([c_done, final])
        else:
            tail.append(c_done)
            tail.append(final)
    if not interleaved_final:
        c_done_events = [event for event in tail if event["event"] == "C_DONE"]
        final_events = [event for event in tail if event["event"] == "FINAL"]
        tail = c_done_events + final_events
    lines.extend(tail)
    return "\n".join(json.dumps(line) for line in lines) + "\n"


class MultiNodeSmallGuards(unittest.TestCase):
    def test_write_and_read_tuple_mapping_is_fixed(self) -> None:
        self.assertEqual(probe.write_tuple(0, "A")["index"], 0)
        self.assertEqual(probe.write_tuple(1, "C")["generation"], 6)
        self.assertEqual(probe.read_tuple(1, "1", "A")["source_member"], "B")
        self.assertEqual(probe.read_tuple(1, "1", "A")["member"], "A")
        self.assertEqual(probe.read_tuple(1, "r2", "B")["source_member"], "A")
        for bad in (-1, 2, True):
            with self.assertRaises(ValueError):
                probe.write_index(bad, "A")
        with self.assertRaises(ValueError):
            probe.member_index("D")

    def writer_summary(self, batch: int, member: str) -> dict[str, object]:
        identity = good_identity()
        row = probe.write_tuple(batch, member)
        result = good_c_result(row["generation"])
        return {"event": "FINAL", "operation": "write", "status": "DATA_RECORDED",
                "identity": identity, "sample": {"result": result},
                "content": write_small.expected_content(row["generation"]),
                "content_verify": content_verify(row["generation"]),
                "parent_dir_fsync": True, "root_dir_fsync": True, **row}

    def test_single_writer_manifest_accepts_warmup_and_rejects_wrong_source(self) -> None:
        identity = good_identity()
        with tempfile.TemporaryDirectory() as tmp:
            path = Path(tmp) / "manifest.json"
            expected = probe.read_tuple(0, "r1", "A")
            path.write_text(json.dumps(self.writer_summary(0, "B")), encoding="utf-8")
            self.assertEqual(probe.validate_single_write_manifest(str(path), identity, expected)["record"]["member"], "B")
            path.write_text(json.dumps(self.writer_summary(1, "B")), encoding="utf-8")
            with self.assertRaises(ValueError):
                probe.validate_single_write_manifest(str(path), identity, expected)
            path.write_text(json.dumps(self.writer_summary(0, "C")), encoding="utf-8")
            with self.assertRaises(ValueError):
                probe.validate_single_write_manifest(str(path), identity, expected)
            bad_index = dict(self.writer_summary(0, "B"), index=True)
            path.write_text(json.dumps(bad_index), encoding="utf-8")
            with self.assertRaises(ValueError):
                probe.validate_single_write_manifest(str(path), identity, expected)

    def run_coordinator(self, payload: str, operation: str = "write", route: str | None = None) -> tuple[subprocess.CompletedProcess[str], dict[str, object]]:
        with tempfile.TemporaryDirectory() as tmp:
            out = Path(tmp) / "coord"
            command = [sys.executable, str(Path(probe.__file__)), "coordinator",
                       "--operation", operation, "--batch", "1", "--session-token", "s",
                       "--cohort-token", "c", "--round-timeout", "0.2", "--output", str(out)]
            if route:
                command.extend(["--route", route])
            completed = subprocess.run(command, input=payload, text=True, stdout=subprocess.PIPE,
                                       stderr=subprocess.PIPE, timeout=5)
            summary_path = out / "summary.json"
            summary = json.loads(summary_path.read_text(encoding="utf-8")) if summary_path.exists() else {"error": completed.stderr}
            return completed, summary

    def test_coordinator_accepts_three_members_and_waits_for_final_postcheck(self) -> None:
        completed, summary = self.run_coordinator(coordinator_payload())
        self.assertEqual(completed.returncode, 0, completed.stderr)
        self.assertEqual(summary["status"], "DATA_RECORDED")
        self.assertEqual(len(summary["ready"]), 3)
        self.assertEqual(len(summary["c_done"]), 3)
        self.assertEqual(len(summary["final"]), 3)
        self.assertFalse(summary["qualified_threefs_parity"])
        stdout_events = [json.loads(line) for line in completed.stdout.splitlines()]
        self.assertTrue(all(event["event"] in {"START", "SUMMARY"} for event in stdout_events))
        self.assertTrue(completed.stderr.startswith("{"))

        completed, summary = self.run_coordinator(coordinator_payload(failed_final=True))
        self.assertNotEqual(completed.returncode, 0)
        self.assertEqual(summary["status"], "BLOCKED")
        self.assertIn("FINAL failed", summary["error"])

    def test_coordinator_rejects_duplicate_member_bad_result_and_missing_route(self) -> None:
        for payload, expected in [
            (coordinator_payload(duplicate_ready=True), "duplicate"),
            (coordinator_payload(bad_result=True), "unexpected io result"),
        ]:
            completed, summary = self.run_coordinator(payload)
            self.assertNotEqual(completed.returncode, 0)
            self.assertIn(expected, summary["error"])
        completed, summary = self.run_coordinator(coordinator_payload(operation="read", route="r1"), operation="read")
        self.assertNotEqual(completed.returncode, 0)
        self.assertIn("read requires --route", summary["error"])
        completed, summary = self.run_coordinator(coordinator_payload(bool_index=True))
        self.assertNotEqual(completed.returncode, 0)
        self.assertIn("index mismatch", summary["error"])

        args = SimpleNamespace(operation="write", batch=0, route=None, session_token="s", cohort_token="c")
        row = probe.write_tuple(0, "B")
        event = {"event": "READY", "session_token": "s", "cohort_token": "c", "operation": "write",
                 **dict(row, index=True)}
        with self.assertRaises(ValueError):
            probe.require_event(event, "READY", args)

    def test_coordinator_accepts_interleaved_final_after_matching_c_done(self) -> None:
        completed, summary = self.run_coordinator(coordinator_payload(interleaved_final=True))
        self.assertEqual(completed.returncode, 0, completed.stderr)
        self.assertEqual(summary["status"], "DATA_RECORDED")
        self.assertEqual([event["member"] for event in summary["final"]][0], "A")

    def test_final_requires_token_fsync_and_read_precheck(self) -> None:
        for payload, operation, route, expected in [
            (coordinator_payload(missing_fsync=True), "write", None, "fsync"),
            (coordinator_payload(bad_token=True), "write", None, "start token"),
            (coordinator_payload(bad_c_done_token=True), "write", None, "C_DONE start token"),
            (coordinator_payload(operation="read", route="r1", missing_pre_read=True), "read", "r1", "content verification"),
        ]:
            completed, summary = self.run_coordinator(payload, operation=operation, route=route)
            self.assertNotEqual(completed.returncode, 0)
            self.assertIn(expected, summary["error"])

    def test_overlap_diagnostic_never_scores_without_raw_rate(self) -> None:
        completed, summary = self.run_coordinator(coordinator_payload(mono_raw_elapsed=None))
        self.assertEqual(completed.returncode, 0, completed.stderr)
        diag = summary["overlap_diagnostic"]
        self.assertEqual(diag["status"], "OBSERVED_NOT_PROVED")
        self.assertFalse(diag["proved_overlap"])
        self.assertFalse(diag["rate_observation_ok"])
        self.assertFalse(diag["proved_common_inner_overlap"])

    def test_worker_does_not_pass_when_postcheck_fails_after_c_done(self) -> None:
        args = SimpleNamespace(operation="write", member="A", batch=0, route=None, session_token="s",
                               cohort_token="c", round_timeout=1, output="/unused", dfs_root="/mnt",
                               io_tool="/tool", identity="/id", candidate=None, manifest=None)
        with tempfile.TemporaryDirectory() as tmp:
            out = Path(tmp) / "out"
            out.mkdir()
            root = Path(tmp) / "mnt"
            root.mkdir()
            sample = {"status": "PASS", "rc": 0, "result": good_c_result(1),
                      "py_monotonic_elapsed_ns": 100, "py_monotonic_raw_elapsed_ns": 100,
                      "clock_info": {}}
            start = {"event": "START", "member": "A", "session_token": "s", "operation": "write",
                     "batch": 0, "cohort_token": "c", "start_token": "t"}
            with patch.object(probe, "prepare_common", return_value=(out, root, Path(tmp) / "tool", good_identity(), {})), \
                 patch.object(probe, "read_control", return_value=start), \
                 patch.object(probe, "run_timed", return_value=sample), \
                 patch.object(probe.sync, "fsync_directory"), \
                 patch.object(probe.write_small, "verify_content", side_effect=ValueError("bad sha")), \
                 patch.object(probe, "emit"):
                result = probe.worker(args)
        self.assertEqual(result["status"], "BLOCKED")
        self.assertIn("bad sha", result["error"])

    def test_failed_probe_sample_survives_error_summary(self) -> None:
        args = SimpleNamespace(operation="write", member="A", batch=0, route=None, session_token="s",
                               cohort_token="c", round_timeout=1, output="/unused", dfs_root="/mnt",
                               io_tool="/tool", identity="/id", candidate=None, manifest=None)
        with tempfile.TemporaryDirectory() as tmp:
            out = Path(tmp) / "out"
            out.mkdir()
            root = Path(tmp) / "mnt"
            root.mkdir()
            sample = {"status": "FAIL", "rc": 2, "stdout": "partial diagnostic\n",
                      "stderr": "create: Device or resource busy\n", "result": None,
                      "error": "ValueError('probe exit 2')", "py_monotonic_elapsed_ns": 100,
                      "py_monotonic_raw_elapsed_ns": 100, "clock_info": {}}
            start = {"event": "START", "member": "A", "session_token": "s", "operation": "write",
                     "batch": 0, "cohort_token": "c", "start_token": "t"}
            with patch.object(probe, "prepare_common", return_value=(out, root, Path(tmp) / "tool", good_identity(), {})), \
                 patch.object(probe, "read_control", return_value=start), \
                 patch.object(probe, "run_timed", return_value=sample), \
                 patch.object(probe, "emit"):
                result = probe.worker(args)
            self.assertEqual(result["status"], "BLOCKED")
            self.assertEqual(result["sample"], sample)
            self.assertEqual(json.loads((out / "summary.json").read_text())["sample"], sample)
            self.assertEqual(json.loads((out / "probe-sample.json").read_text()), sample)

    def test_read_worker_prechecks_before_ready_and_postchecks_after_c_done(self) -> None:
        args = SimpleNamespace(operation="read", member="A", batch=0, route="r1", session_token="s",
                               cohort_token="c", round_timeout=1, output="/unused", dfs_root="/mnt",
                               io_tool="/tool", identity="/id", candidate=None, manifest="/manifest")
        with tempfile.TemporaryDirectory() as tmp:
            out = Path(tmp) / "out"
            out.mkdir()
            root = Path(tmp) / "mnt"
            payload = root / write_small.relative_path(1)
            payload.parent.mkdir(parents=True)
            payload.write_bytes(b"")
            content = write_small.expected_content(2)
            sample = {"status": "PASS", "rc": 0, "result": good_c_result(2, "seq-read"),
                      "py_monotonic_elapsed_ns": 100, "py_monotonic_raw_elapsed_ns": 100,
                      "clock_info": {}}
            start = {"event": "START", "member": "A", "session_token": "s", "operation": "read",
                     "batch": 0, "cohort_token": "c", "start_token": "t",
                     "route": "r1", "source_member": "B"}
            calls: list[str] = []

            def fake_verify(_payload: Path, expected_content: dict[str, object]) -> dict[str, object]:
                self.assertEqual(expected_content, content)
                calls.append("verify")
                return content_verify(2)

            def fake_emit(event: dict[str, object]) -> None:
                calls.append(str(event["event"]))

            with patch.object(probe, "prepare_common", return_value=(out, root, Path(tmp) / "tool", good_identity(), {})), \
                 patch.object(probe, "validate_single_write_manifest",
                              return_value={"record": {"content": content}, "path": "/manifest"}), \
                 patch.object(probe, "read_control", return_value=start), \
                 patch.object(probe, "run_timed", return_value=sample), \
                 patch.object(probe.write_small, "verify_content", side_effect=fake_verify), \
                 patch.object(probe, "emit", side_effect=fake_emit):
                result = probe.worker(args)

        self.assertEqual(result["status"], "DATA_RECORDED")
        self.assertEqual(calls, ["verify", "READY", "C_DONE", "verify", "FINAL"])


if __name__ == "__main__":
    unittest.main()
