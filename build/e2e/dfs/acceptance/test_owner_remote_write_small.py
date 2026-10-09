"""Guard tests for owner_remote_write_small diagnostic helpers."""
from __future__ import annotations

import importlib.util
from pathlib import Path
import tempfile
import unittest

spec = importlib.util.spec_from_file_location("owner_remote_write_small", Path(__file__).with_name("owner_remote_write_small.py"))
owner_remote_write_small = importlib.util.module_from_spec(spec)
spec.loader.exec_module(owner_remote_write_small)


def good_write_result(**overrides: object) -> dict[str, object]:
    record: dict[str, object] = {
        "operation": "seq-write",
        "file_bytes": owner_remote_write_small.DATA_BYTES,
        "io_bytes": owner_remote_write_small.DATA_BYTES,
        "block_bytes": owner_remote_write_small.BLOCK_BYTES,
        "concurrency": owner_remote_write_small.CONCURRENCY,
        "barrier": "fdatasync",
        "pattern_byte": owner_remote_write_small.PATTERN_BYTE,
        "operations": owner_remote_write_small.DATA_BYTES // owner_remote_write_small.BLOCK_BYTES,
        "cache_requested": "unobserved",
        "content_ok": True,
        "residency_observed": False,
        "wall_ns": 1000,
    }
    record.update(overrides)
    return record


def sample(target: str, round_index: int, status: str = "PASS") -> dict[str, object]:
    return {
        "target": target,
        "round": round_index,
        "status": status,
        "write": {"verify": {"status": "PASS"}},
        "content_verify": {"status": "PASS", "bytes": owner_remote_write_small.DATA_BYTES, "sha256": owner_remote_write_small.EXPECTED_SHA256},
        "dir_fsync": True,
    }


def full_rounds() -> list[dict[str, object]]:
    rounds = []
    for index in range(owner_remote_write_small.WARMUP_ROUNDS + owner_remote_write_small.MEASUREMENT_ROUNDS):
        rounds.append({"round": index, "order": owner_remote_write_small.paired_order(index), "samples": [sample(target, index) for target in owner_remote_write_small.paired_order(index)]})
    return rounds


class OwnerRemoteWriteSmallGuardTests(unittest.TestCase):
    def test_write_result_rejects_failed_c_short_write_and_content(self) -> None:
        owner_remote_write_small.validate_write_result(good_write_result())
        with self.assertRaises(ValueError):
            owner_remote_write_small.validate_write_result(good_write_result(content_ok=False))
        with self.assertRaises(ValueError):
            owner_remote_write_small.validate_write_result(good_write_result(io_bytes=owner_remote_write_small.DATA_BYTES - 1))
        with self.assertRaises(ValueError):
            owner_remote_write_small.validate_write_result(good_write_result(cache_requested="unchecked"))

    def test_missing_rounds_and_wrong_order_do_not_record_data(self) -> None:
        self.assertEqual(owner_remote_write_small.write_status(full_rounds()[:1]), "FAIL")
        rounds = full_rounds()
        rounds[1]["order"] = ["owner", "moose"]
        self.assertEqual(owner_remote_write_small.write_status(rounds), "FAIL")

    def test_full_rounds_record_data(self) -> None:
        self.assertEqual(owner_remote_write_small.write_status(full_rounds()), "DATA_RECORDED")

    def test_output_existing_is_rejected_and_not_overwritten(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            out = Path(tmp) / "out"
            out.mkdir()
            sentinel = out / "summary.json"
            sentinel.write_text("sentinel", encoding="utf-8")
            with self.assertRaises(ValueError):
                owner_remote_write_small.validate_output_path(out)

            class Args:
                owner_root = "/missing-owner"
                moose_root = "/missing-moose"
                io_tool = "/missing-io"
                output = str(out)

            result = owner_remote_write_small.run(Args())
            self.assertEqual(result["status"], "BLOCKED")
            self.assertEqual(sentinel.read_text(encoding="utf-8"), "sentinel")

    def test_foreign_mounts_are_rejected(self) -> None:
        with self.assertRaises(ValueError):
            owner_remote_write_small.require_owner_mount({"fstype": "fuse", "source": "afs-dfs"})
        owner_remote_write_small.require_owner_mount({"fstype": "fuse", "source": "afs-ownerfs"})
        with self.assertRaises(ValueError):
            owner_remote_write_small.require_moose_mount({"fstype": "fuse.mfs", "source": "mfs#other"})
        with self.assertRaises(ValueError):
            owner_remote_write_small.require_moose_mount({"fstype": "fuse", "source": "mfs#other"})
        owner_remote_write_small.require_moose_mount({"fstype": "fuse.mfs", "source": "mfs#192.168.109.11:23042"})
        owner_remote_write_small.require_moose_mount({"fstype": "fuse", "source": "mfs#192.168.109.11:23042"})


if __name__ == "__main__":
    unittest.main()
