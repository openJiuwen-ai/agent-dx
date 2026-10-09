"""Guard tests for threefs_delete_small."""
from __future__ import annotations

import importlib.util
import os
from pathlib import Path
import tempfile
import unittest
from unittest.mock import patch

spec = importlib.util.spec_from_file_location("threefs_delete_small", Path(__file__).with_name("threefs_delete_small.py"))
threefs_delete_small = importlib.util.module_from_spec(spec)
spec.loader.exec_module(threefs_delete_small)

RUN_ID = "123456789-4242"
THREEFS_MOUNT = "/mnt/lima-afsadata/afs-delivery/threefs-delete-v84-20261007-r1/mount"
THREEFS_ROOT = THREEFS_MOUNT + "/test"


def good_sample(round_index: int = 0, *, run_id: str = RUN_ID, root: str = THREEFS_ROOT, **overrides: object) -> dict[str, object]:
    name = threefs_delete_small.sample_name(run_id, round_index)
    sample: dict[str, object] = {
        "round": round_index,
        "measured": round_index >= threefs_delete_small.WARMUP_ROUNDS,
        "sample_name": name,
        "directory": str(Path(root) / name),
        "files": threefs_delete_small.expected_delete_files(),
        "status": "PASS",
        "prepare": {"files": threefs_delete_small.expected_delete_files(), "fdatasync_per_file": True, "parent_fsync": True},
        "fresh_open_verify": {"status": "PASS", "files_checked": threefs_delete_small.DELETE_FILE_COUNT, "bytes_per_file": threefs_delete_small.DELETE_FILE_BYTES},
        "unlink_timer": {"status": "PASS", "files": threefs_delete_small.DELETE_FILE_COUNT, "wall_ns": 1000},
        "post_unlink": {"status": "PASS", "remaining": [], "parent_fsync": True},
        "cleanup": {"status": "PASS", "removed_sample_dir": True},
    }
    sample.update(overrides)
    return sample


def good_manifest() -> dict[str, object]:
    rounds = [good_sample(index) for index in range(threefs_delete_small.WARMUP_ROUNDS + threefs_delete_small.MEASUREMENT_ROUNDS)]
    return {
        "role": "threefs-delete-writer",
        "status": "DATA_RECORDED",
        "threefs_upstream_commit": threefs_delete_small.THREEFS_UPSTREAM_COMMIT,
        "threefs_arm64_patch_sha256": threefs_delete_small.THREEFS_ARM64_PATCH_SHA256,
        "fs": {"threefs_root": {"path": THREEFS_ROOT, "device": 1, "inode": 2, "mode": 0o755, "uid": 0, "gid": 0}},
        "mount": {"source": threefs_delete_small.EXPECTED_HF3FS_SOURCE, "target": THREEFS_MOUNT, "fstype": "fuse.hf3fs", "options": "rw", "id": "10"},
        "run_id": RUN_ID,
        "owner_delete_helper": {"sha256": threefs_delete_small.OWNER_REMOTE_SMALL_SHA256},
        "delete_shape": threefs_delete_small.delete_shape(),
        "delete_rounds": rounds,
    }


class ThreefsDeleteSmallGuardTests(unittest.TestCase):
    def test_changed_helper_is_rejected_before_import(self) -> None:
        with patch.object(threefs_delete_small, "sha256_file", return_value="0" * 64):
            with self.assertRaisesRegex(ValueError, "sha256 mismatch"):
                threefs_delete_small.load_owner_delete_helper()

    def test_owner_helper_loads_fixed_sha_and_shape(self) -> None:
        helper = threefs_delete_small.load_owner_delete_helper()
        self.assertEqual(helper.DELETE_FILE_COUNT, threefs_delete_small.DELETE_FILE_COUNT)
        self.assertEqual(helper.DELETE_FILE_BYTES, threefs_delete_small.DELETE_FILE_BYTES)
        self.assertTrue(callable(helper.prepare_delete_files))
        self.assertTrue(callable(helper.verify_delete_file_contents))
        self.assertTrue(callable(helper.timed_unlink))

    def test_mount_identity_requires_exact_hf3fs_source_and_target(self) -> None:
        threefs_delete_small.require_hf3fs_mount({"fstype": "fuse.hf3fs", "source": threefs_delete_small.EXPECTED_HF3FS_SOURCE, "target": THREEFS_MOUNT}, Path(THREEFS_ROOT))
        with self.assertRaises(ValueError):
            threefs_delete_small.require_hf3fs_mount({"fstype": "fuse", "source": "afs-dfs", "target": THREEFS_ROOT}, Path(THREEFS_ROOT))
        with self.assertRaises(ValueError):
            threefs_delete_small.require_hf3fs_mount({"fstype": "ext4", "source": threefs_delete_small.EXPECTED_HF3FS_SOURCE, "target": THREEFS_MOUNT}, Path(THREEFS_ROOT))
        with self.assertRaises(ValueError):
            threefs_delete_small.require_hf3fs_mount({"fstype": "fuse.hf3fs", "source": threefs_delete_small.EXPECTED_HF3FS_SOURCE, "target": "/mnt/other"}, Path(THREEFS_ROOT))
        with self.assertRaises(ValueError):
            threefs_delete_small.require_hf3fs_mount({"fstype": "fuse.hf3fs", "source": threefs_delete_small.EXPECTED_HF3FS_SOURCE, "target": "/mnt/other/afs-delivery/other-run/mount"}, Path("/mnt/other/afs-delivery/other-run/mount/test"))
        with self.assertRaises(ValueError):
            threefs_delete_small.require_hf3fs_mount({"fstype": "fuse.hf3fs", "source": threefs_delete_small.EXPECTED_HF3FS_SOURCE, "target": THREEFS_MOUNT}, Path(THREEFS_MOUNT + "/other-existing-empty"))

    def test_manifest_binds_identity_runid_names_directories_and_shape(self) -> None:
        rounds = threefs_delete_small.validate_manifest(good_manifest())
        self.assertEqual(len(rounds), threefs_delete_small.WARMUP_ROUNDS + threefs_delete_small.MEASUREMENT_ROUNDS)
        cases = []
        missing_run = good_manifest(); missing_run.pop("run_id"); cases.append(missing_run)
        bad_run = good_manifest(); bad_run["run_id"] = "run-x"; cases.append(bad_run)
        bad_role = good_manifest(); bad_role["role"] = "delete-writer"; cases.append(bad_role)
        bad_patch = good_manifest(); bad_patch["threefs_arm64_patch_sha256"] = "0" * 64; cases.append(bad_patch)
        bad_helper = good_manifest(); bad_helper["owner_delete_helper"] = {"sha256": "0" * 64}; cases.append(bad_helper)
        wrong_name = good_manifest(); wrong_name["delete_rounds"][0] = {**wrong_name["delete_rounds"][0], "sample_name": ".afs-threefs-delete-123456789-9999-r00"}; cases.append(wrong_name)
        wrong_dir = good_manifest(); wrong_dir["delete_rounds"][0] = {**wrong_dir["delete_rounds"][0], "directory": "/mnt/hf3fs/.afs-threefs-delete-123456789-4242-r99"}; cases.append(wrong_dir)
        traversal = good_manifest(); traversal["delete_rounds"][0] = {**traversal["delete_rounds"][0], "sample_name": "../escape"}; cases.append(traversal)
        wrong_root = good_manifest(); wrong_root["fs"]["threefs_root"]["path"] = "/mnt/other"; cases.append(wrong_root)
        wrong_relative = good_manifest(); wrong_relative["fs"]["threefs_root"]["path"] = THREEFS_MOUNT + "/other-existing-empty"; cases.append(wrong_relative)
        wrong_source = good_manifest(); wrong_source["mount"] = {**wrong_source["mount"], "source": "afs-dfs"}; cases.append(wrong_source)
        short_rounds = good_manifest(); short_rounds["delete_rounds"] = short_rounds["delete_rounds"][:-1]; cases.append(short_rounds)
        bad_shape = good_manifest(); bad_shape["delete_shape"] = {"files": 1}; cases.append(bad_shape)
        for manifest in cases:
            with self.subTest(manifest=manifest), self.assertRaises(ValueError):
                threefs_delete_small.validate_manifest(manifest)

    def test_manifest_rejects_content_unlink_cleanup_and_fsync_failures(self) -> None:
        for mutated in (
            {"prepare": {"files": threefs_delete_small.expected_delete_files(), "fdatasync_per_file": False, "parent_fsync": True}},
            {"fresh_open_verify": {"status": "FAIL", "files_checked": threefs_delete_small.DELETE_FILE_COUNT, "bytes_per_file": threefs_delete_small.DELETE_FILE_BYTES}},
            {"unlink_timer": {"status": "FAIL", "files": threefs_delete_small.DELETE_FILE_COUNT, "wall_ns": 1000}},
            {"post_unlink": {"status": "FAIL", "remaining": ["entry-0000.bin"], "parent_fsync": True}},
            {"cleanup": {"status": "FAIL", "left_in_place": "/tmp/x"}},
        ):
            manifest = good_manifest()
            manifest["delete_rounds"][0] = {**manifest["delete_rounds"][0], **mutated}
            with self.subTest(mutated=mutated), self.assertRaises(ValueError):
                threefs_delete_small.validate_manifest(manifest)

    def test_deleted_path_checker_requires_600_actual_lstat_enoent_and_wrong_errno_fails(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            rounds = threefs_delete_small.validate_manifest(good_manifest())
            result = threefs_delete_small.check_deleted_paths(root, rounds)
            self.assertEqual(result["status"], "PASS")
            self.assertEqual(result["checked"], threefs_delete_small.DELETE_FILE_COUNT * (threefs_delete_small.WARMUP_ROUNDS + threefs_delete_small.MEASUREMENT_ROUNDS))
            present_dir = root / rounds[0]["sample_name"]
            present_dir.mkdir()
            (present_dir / "entry-0000.bin").write_bytes(b"present")
            present = threefs_delete_small.check_deleted_paths(root, rounds)
            self.assertEqual(present["status"], "FAIL")
            self.assertEqual(present["failures"][0]["status"], "PRESENT")

            original_lstat = threefs_delete_small.os.lstat
            try:
                def denied_lstat(path: Path) -> object:
                    if str(path).endswith("entry-0000.bin"):
                        raise PermissionError(13, "denied", str(path))
                    return original_lstat(path)
                threefs_delete_small.os.lstat = denied_lstat
                wrong_errno = threefs_delete_small.check_deleted_paths(root, rounds)
            finally:
                threefs_delete_small.os.lstat = original_lstat
            self.assertEqual(wrong_errno["status"], "FAIL")
            self.assertEqual(wrong_errno["failures"][0]["status"], "WRONG_ERRNO")
            self.assertEqual(wrong_errno["failures"][0]["errno"], 13)

    def test_absolute_paths_reject_literal_dotdot(self) -> None:
        with self.assertRaises(ValueError):
            threefs_delete_small.require_absolute_path(THREEFS_MOUNT + "/../mount/test", "threefs-root")

    def test_writer_existing_output_sentinel_is_not_overwritten(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            out = Path(tmp) / "out"
            out.mkdir()
            sentinel = out / "summary.json"
            sentinel.write_text("sentinel", encoding="utf-8")

            class Args:
                threefs_root = "/missing-threefs"
                output = str(out)

            result = threefs_delete_small.writer(Args())
            self.assertEqual(result["status"], "BLOCKED")
            self.assertEqual(sentinel.read_text(encoding="utf-8"), "sentinel")


if __name__ == "__main__":
    unittest.main()
