"""Guard tests for owner_remote_small diagnostic evidence shape."""
from __future__ import annotations

import importlib.util
from pathlib import Path
import subprocess
import tempfile
from types import SimpleNamespace
import unittest
from unittest import mock

spec = importlib.util.spec_from_file_location("owner_remote_small", Path(__file__).with_name("owner_remote_small.py"))
owner_remote_small = importlib.util.module_from_spec(spec)
spec.loader.exec_module(owner_remote_small)


def latency_values() -> list[int]:
    return list(range(1000, 1000 + owner_remote_small.READ_BYTES // owner_remote_small.READ_BLOCK_BYTES))


def with_latency(record: dict[str, object], samples: list[int] | None = None) -> dict[str, object]:
    values = samples if samples is not None else latency_values()
    record.update(
        {
            "latency_samples_ns": values,
            "latency_order": owner_remote_small.READ_LATENCY_ORDER,
            "latency_interval": owner_remote_small.READ_LATENCY_INTERVAL,
            "p50_ns": values[len(values) * 50 // 100],
            "p95_ns": values[len(values) * 95 // 100],
            "p99_ns": values[len(values) * 99 // 100],
        }
    )
    return record


def read_result(**overrides: object) -> dict[str, object]:
    record: dict[str, object] = {
        "operation": "seq-read",
        "file_bytes": owner_remote_small.READ_BYTES,
        "io_bytes": owner_remote_small.READ_BYTES,
        "block_bytes": owner_remote_small.READ_BLOCK_BYTES,
        "concurrency": owner_remote_small.READ_CONCURRENCY,
        "barrier": "close",
        "pattern_byte": owner_remote_small.READ_PATTERN_BYTE,
        "operations": owner_remote_small.READ_BYTES // owner_remote_small.READ_BLOCK_BYTES,
        "wall_ns": 1000,
        "residency_observed": False,
        "cache_requested": "unobserved",
        "content_ok": True,
    }
    record.update(overrides)
    return record


class OwnerRemoteSmallGuardTests(unittest.TestCase):
    def test_raw_case_stops_at_first_failed_probe_and_keeps_evidence(self) -> None:
        failure = {"target": "owner", "round": 0, "measured": False,
                   "status": "FAIL", "rc": 2, "stdout": "", "stderr": "residency mapping: No such device"}
        with tempfile.TemporaryDirectory() as tmp:
            inputs = {"io_tool": Path("/io"), "owner_read_file": Path("/owner"),
                      "moose_read_file": Path("/moose"), "raw_latency": True, "read_cache": "repeat"}
            with mock.patch.object(owner_remote_small, "run_io_read", return_value=failure) as probe:
                rounds = owner_remote_small.run_read_rounds(inputs, Path(tmp))
            self.assertEqual(probe.call_count, 1)
            self.assertEqual(len(rounds), 1)
            self.assertTrue(rounds[0]["stopped_after_failure"])
            self.assertEqual(rounds[0]["samples"][0]["stderr"], failure["stderr"])
            self.assertTrue((Path(tmp) / "read-samples/round-00-owner.json").is_file())
            self.assertEqual(owner_remote_small.read_status(rounds), "FAIL")

    def test_output_admission_rejects_missing_parent_before_timers(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            parent = Path(tmp) / "results"
            output = parent / "read-r1"
            with self.assertRaisesRegex(ValueError, "output parent must exist"):
                owner_remote_small.validate_output_path(output)
            self.assertFalse(parent.exists())
            parent.mkdir()
            owner_remote_small.validate_output_path(output)
            output.mkdir()
            with self.assertRaisesRegex(ValueError, "output must not already exist"):
                owner_remote_small.validate_output_path(output)

    def test_validate_io_result_rejects_wrong_content_and_short_read(self) -> None:
        owner_remote_small.validate_io_result(read_result())
        with self.assertRaises(ValueError):
            owner_remote_small.validate_io_result(read_result(content_ok=False))
        with self.assertRaises(ValueError):
            owner_remote_small.validate_io_result(read_result(io_bytes=owner_remote_small.READ_BYTES - 1))
        with self.assertRaises(ValueError):
            owner_remote_small.validate_io_result(read_result(cache_requested="hot"))

    def test_raw_latency_requires_exact_sorted_sample_shape_and_percentiles(self) -> None:
        owner_remote_small.validate_io_result(
            with_latency(read_result(cache_requested="repeat", residency_observed=True)),
            raw_latency=True,
            read_cache="repeat",
        )
        cases = [
            with_latency(read_result(cache_requested="repeat", residency_observed=True), latency_values()[:-1]),
            with_latency(read_result(cache_requested="repeat", residency_observed=True), list(reversed(latency_values()))),
            with_latency(read_result(cache_requested="repeat", residency_observed=True), [True] + latency_values()[1:]),
            with_latency(read_result(cache_requested="repeat", residency_observed=True), [0] + latency_values()[1:]),
            {
                **with_latency(read_result(cache_requested="repeat", residency_observed=True)),
                "latency_interval": "pread-only",
            },
            {
                **with_latency(read_result(cache_requested="repeat", residency_observed=True)),
                "p95_ns": 1,
            },
        ]
        for case in cases:
            with self.subTest(case=case), self.assertRaises(ValueError):
                owner_remote_small.validate_io_result(case, raw_latency=True, read_cache="repeat")
        with self.assertRaises(ValueError):
            owner_remote_small.validate_io_result(with_latency(read_result()))

    def test_raw_latency_rejects_bool_and_float_scalar_fields(self) -> None:
        for mutated in (
            {"operations": float(owner_remote_small.READ_BYTES // owner_remote_small.READ_BLOCK_BYTES)},
            {"wall_ns": True},
            {"p95_ns": True, "latency_samples_ns": [1] * (owner_remote_small.READ_BYTES // owner_remote_small.READ_BLOCK_BYTES)},
        ):
            record = with_latency(read_result(cache_requested="repeat", residency_observed=True))
            record.update(mutated)
            with self.subTest(mutated=mutated), self.assertRaises(ValueError):
                owner_remote_small.validate_io_result(record, raw_latency=True, read_cache="repeat")

    def test_run_read_rounds_accepts_legacy_read_inputs_without_raw_keys(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            output = Path(tmp) / "out"
            read_inputs = {
                "io_tool": Path("/probe"),
                "owner_read_file": Path("/owner/payload"),
                "moose_read_file": Path("/moose/payload"),
            }
            calls: list[dict[str, object]] = []

            def fake_run_io_read(io_tool: Path, read_file: Path, label: str, round_index: int, measured: bool, **kwargs: object) -> dict[str, object]:
                calls.append({"label": label, "kwargs": kwargs, "read_file": read_file})
                return {"status": "PASS", "verify": {"status": "PASS"}}

            with mock.patch.object(owner_remote_small, "run_io_read", side_effect=fake_run_io_read):
                rounds = owner_remote_small.run_read_rounds(read_inputs, output)

            self.assertEqual(len(rounds), owner_remote_small.WARMUP_ROUNDS + owner_remote_small.MEASUREMENT_ROUNDS)
            self.assertEqual(calls[0]["kwargs"], {"raw_latency": False, "read_cache": owner_remote_small.READ_DEFAULT_CACHE})

    def test_repeat_cache_requires_raw_latency_and_raw_tool_identity(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            owner_root = root / "owner"
            moose_root = root / "moose"
            owner_root.mkdir()
            moose_root.mkdir()
            owner_file = owner_root / "payload"
            moose_file = moose_root / "payload"
            io_tool = root / "probe"
            for path in (owner_file, moose_file, io_tool):
                path.write_bytes(b"x")

            args = SimpleNamespace(
                io_tool=str(io_tool),
                owner_read_file=str(owner_file),
                moose_read_file=str(moose_file),
                raw_latency=False,
                read_cache="unobserved",
            )

            with mock.patch.object(owner_remote_small, "sha256_file", return_value=owner_remote_small.IO_TOOL_SHA256):
                result = owner_remote_small.validate_read_inputs(args, owner_root, moose_root)
            self.assertFalse(result["raw_latency"])
            self.assertEqual(result["read_cache"], "unobserved")
            self.assertEqual(result["io_tool_sha256"], owner_remote_small.IO_TOOL_SHA256)

            args.read_cache = "repeat"
            with self.assertRaises(ValueError):
                owner_remote_small.validate_read_inputs(args, owner_root, moose_root)

            args.raw_latency = True
            with mock.patch.object(owner_remote_small, "sha256_file", return_value=owner_remote_small.RAW_LATENCY_IO_TOOL_SHA256):
                result = owner_remote_small.validate_read_inputs(args, owner_root, moose_root)
            self.assertTrue(result["raw_latency"])
            self.assertEqual(result["read_cache"], "repeat")
            self.assertEqual(result["io_tool_sha256"], owner_remote_small.RAW_LATENCY_IO_TOOL_SHA256)

            with mock.patch.object(owner_remote_small, "sha256_file", return_value=owner_remote_small.IO_TOOL_SHA256):
                with self.assertRaises(ValueError):
                    owner_remote_small.validate_read_inputs(args, owner_root, moose_root)

    def test_run_io_read_raw_mode_adds_samples_arg_and_preserves_failed_probe_output(self) -> None:
        completed = subprocess.CompletedProcess(args=["probe"], returncode=7, stdout="{bad", stderr="probe stderr")
        with mock.patch.object(owner_remote_small.subprocess, "run", return_value=completed) as run:
            sample = owner_remote_small.run_io_read(
                Path("/probe"),
                Path("/payload"),
                "owner",
                1,
                True,
                raw_latency=True,
                read_cache="repeat",
            )
        argv = run.call_args.args[0]
        self.assertEqual(argv[-2:], ["repeat", "samples"])
        self.assertEqual(sample["rc"], 7)
        self.assertEqual(sample["stdout"], "{bad")
        self.assertEqual(sample["stderr"], "probe stderr")
        self.assertEqual(sample["status"], "FAIL")
        self.assertEqual(sample["verify"]["status"], "FAIL")

    def test_existing_output_directory_is_rejected(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            existing = Path(tmp) / "out"
            existing.mkdir()
            with self.assertRaises(ValueError):
                owner_remote_small.validate_output_path(existing)

    def test_failed_cleanup_prevents_data_recorded(self) -> None:
        rounds = [
            {
                "samples": [
                    {"status": "PASS", "cleanup": {"status": "PASS"}},
                    {"status": "PASS", "cleanup": {"status": "FAIL", "left_in_place": "/tmp/sample"}},
                ]
            }
        ]
        self.assertEqual(owner_remote_small.compute_status(rounds, [], "delete"), "FAIL")

    def test_failed_syscall_prevents_data_recorded(self) -> None:
        rounds = [
            {
                "samples": [
                    {"status": "PASS", "cleanup": {"status": "PASS"}},
                    {"status": "FAIL", "cleanup": {"status": "PASS"}, "error": "OSError('unlink')"},
                ]
            }
        ]
        self.assertEqual(owner_remote_small.compute_status(rounds, [], "delete"), "FAIL")


    def test_missing_rounds_do_not_record_data(self) -> None:
        one_round = [{"samples": [{"status": "PASS", "cleanup": {"status": "PASS"}}, {"status": "PASS", "cleanup": {"status": "PASS"}}]}]
        self.assertEqual(owner_remote_small.delete_status(one_round), "FAIL")
        self.assertEqual(owner_remote_small.read_status(one_round), "FAIL")


    def test_all_requires_read_rounds_before_data_recorded(self) -> None:
        rounds = [
            {"samples": [{"status": "PASS", "cleanup": {"status": "PASS"}}, {"status": "PASS", "cleanup": {"status": "PASS"}}]}
            for _ in range(owner_remote_small.WARMUP_ROUNDS + owner_remote_small.MEASUREMENT_ROUNDS)
        ]
        self.assertEqual(owner_remote_small.delete_status(rounds), "DATA_RECORDED")
        self.assertEqual(owner_remote_small.compute_status(rounds, [], "all"), "FAIL")

    def test_all_missing_read_inputs_is_rejected(self) -> None:
        with self.assertRaises(ValueError):
            owner_remote_small.require_read_inputs_for_case("all", None)

    def test_read_missing_read_inputs_is_rejected(self) -> None:
        with self.assertRaises(ValueError):
            owner_remote_small.require_read_inputs_for_case("read", None)
        owner_remote_small.require_read_inputs_for_case("delete", None)


if __name__ == "__main__":
    unittest.main()
