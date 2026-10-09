import importlib.util
import json
import tempfile
import unittest
from argparse import Namespace
from pathlib import Path
from unittest import mock


DRIVER = Path(__file__).resolve().parent / "drivers" / "mount_isolation.py"
SPEC = importlib.util.spec_from_file_location("mount_isolation", DRIVER)
mount_isolation = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(mount_isolation)


NODE_SHA = "a" * 64
META_SHA = "b" * 64
CFG_SHA = "c" * 64
BOOT_ID = "11111111-2222-3333-4444-555555555555"
NODE_START_TICKS = 123456
META_START_TICKS = 234567


def write_json(path: Path, value: object) -> None:
    path.write_text(json.dumps(value, indent=2, sort_keys=True), encoding="utf-8")


def mount_command(source: str, target: Path) -> dict[str, object]:
    return {
        "returncode": 0,
        "stdout": json.dumps({"filesystems": [{"target": str(target), "source": source, "fstype": "fuse.afs", "options": "rw"}]}),
        "stderr": "",
    }


def identity(role: str, sha: str, executable: str = "afs-node", config_path: str = "/etc/afs/node.toml") -> dict[str, object]:
    start_ticks = NODE_START_TICKS if role == "node" else META_START_TICKS
    return {
        "role": role,
        "pid": 100 if role == "node" else 200,
        "exists": True,
        "exe_path": "/opt/afs/bin/" + executable,
        "sha256": sha,
        "expected_sha256": sha,
        "sha256_ok": True,
        "boot_id": BOOT_ID,
        "expected_boot_id": BOOT_ID,
        "boot_id_ok": True,
        "start_ticks": start_ticks,
        "expected_start_ticks": start_ticks,
        "start_ticks_ok": True,
        "cmdline_items": [executable, "--config", config_path],
        "config_identity": {
            "observed_path": config_path,
            "expected_path": config_path,
            "sha256": CFG_SHA,
            "expected_sha256": CFG_SHA,
            "path_ok": True,
            "sha256_ok": True,
        },
    }


class MountIsolationBindingTests(unittest.TestCase):
    def test_binding_requires_exact_matrix_and_process_identity(self):
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            binding = {
                "schema_version": 1,
                "bindings": [
                    {
                        "case_id": "FUN-13",
                        "profile": "smoke",
                        "matrix": {"backend": "DFS", "meta": "memory"},
                        "owner_mount": "/mnt/owner",
                        "dfs_mount": "/mnt/dfs",
                        "node_process": {"role": "node", "pid": 10, "sha256": NODE_SHA, "boot_id": BOOT_ID, "start_ticks": NODE_START_TICKS, "config_path": "/etc/afs/node.toml", "config_sha256": CFG_SHA},
                        "meta_process": {"role": "meta", "pid": 20, "sha256": META_SHA, "boot_id": BOOT_ID, "start_ticks": META_START_TICKS},
                    }
                ],
            }
            write_json(root / "bindings.json", binding)
            selected = mount_isolation.load_binding(root / "bindings.json", "FUN-13", "smoke", {"backend": "DFS", "meta": "memory"})
            self.assertEqual(selected["backend"], "DFS")
            self.assertEqual(selected["node_process"]["config_sha256"], CFG_SHA)
            self.assertEqual(selected["node_process"]["boot_id"], BOOT_ID)
            self.assertEqual(selected["node_process"]["start_ticks"], NODE_START_TICKS)
            with self.assertRaises(mount_isolation.BindingError):
                mount_isolation.load_binding(root / "bindings.json", "FUN-13", "smoke", {"backend": "OwnerFs", "meta": "memory"})

    def test_process_identity_ok_requires_config_and_incarnation_when_supplied(self):
        good = identity("node", NODE_SHA, "afs-node")
        self.assertTrue(mount_isolation.process_identity_ok(good, "afs-node"))
        wrong_config = dict(good)
        wrong_config["config_identity"] = dict(good["config_identity"], sha256_ok=False)
        self.assertFalse(mount_isolation.process_identity_ok(wrong_config, "afs-node"))
        wrong_exe = dict(good, exe_path="/opt/afs/bin/python3")
        self.assertFalse(mount_isolation.process_identity_ok(wrong_exe, "afs-node"))
        wrong_boot = dict(good, boot_id_ok=False)
        self.assertFalse(mount_isolation.process_identity_ok(wrong_boot, "afs-node"))
        wrong_start = dict(good, start_ticks_ok=False)
        self.assertFalse(mount_isolation.process_identity_ok(wrong_start, "afs-node"))

    def test_binding_requires_process_incarnation_fields(self):
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            binding = {
                "schema_version": 1,
                "bindings": [
                    {
                        "case_id": "FUN-13",
                        "profile": "smoke",
                        "matrix": {"backend": "DFS", "meta": "memory"},
                        "owner_mount": "/mnt/owner",
                        "dfs_mount": "/mnt/dfs",
                        "node_process": {"role": "node", "pid": 10, "sha256": NODE_SHA},
                        "meta_process": {"role": "meta", "pid": 20, "sha256": META_SHA, "boot_id": BOOT_ID, "start_ticks": META_START_TICKS},
                    }
                ],
            }
            write_json(root / "bindings.json", binding)
            with self.assertRaises(mount_isolation.BindingError):
                mount_isolation.load_binding(root / "bindings.json", "FUN-13", "smoke", {"backend": "DFS", "meta": "memory"})

    def test_binding_rejects_operation_bases_outside_verified_mounts(self):
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            binding = {
                "schema_version": 1,
                "bindings": [
                    {
                        "case_id": "FUN-13",
                        "profile": "smoke",
                        "matrix": {"backend": "OwnerFs", "meta": "memory"},
                        "owner_mount": "/mnt/owner",
                        "dfs_mount": "/mnt/dfs",
                        "owner_base_dir": "/mnt/sibling-owner",
                        "dfs_base_dir": "/mnt/dfs/subdir",
                        "node_process": {"role": "node", "pid": 10, "sha256": NODE_SHA, "boot_id": BOOT_ID, "start_ticks": NODE_START_TICKS},
                        "meta_process": {"role": "meta", "pid": 20, "sha256": META_SHA, "boot_id": BOOT_ID, "start_ticks": META_START_TICKS},
                    }
                ],
            }
            write_json(root / "bindings.json", binding)
            with self.assertRaisesRegex(mount_isolation.BindingError, "owner_base_dir"):
                mount_isolation.load_binding(root / "bindings.json", "FUN-13", "smoke", {"backend": "OwnerFs", "meta": "memory"})


class MountIsolationOperationTests(unittest.TestCase):
    def test_run_operations_proves_content_errno_uid_and_identity(self):
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            owner = root / "owner"
            dfs = root / "dfs"
            owner.mkdir()
            dfs.mkdir()
            node_cfg = root / "node.toml"
            meta_cfg = root / "meta.toml"
            node_cfg.write_text("node\n", encoding="utf-8")
            meta_cfg.write_text("meta\n", encoding="utf-8")
            node_cfg_sha = mount_isolation.sha256_file(node_cfg)
            meta_cfg_sha = mount_isolation.sha256_file(meta_cfg)
            run_dir = root / "run"
            artifacts = run_dir / "artifacts" / "fun-13-mount-isolation"
            artifacts.mkdir(parents=True)
            binding = {
                "owner_mount": owner,
                "dfs_mount": dfs,
                "owner_base_dir": owner,
                "dfs_base_dir": dfs,
                "node_process": {"pid": 10, "sha256": NODE_SHA, "boot_id": BOOT_ID, "start_ticks": NODE_START_TICKS, "config_path": str(node_cfg), "config_sha256": node_cfg_sha},
                "meta_process": {"pid": 20, "sha256": META_SHA, "boot_id": BOOT_ID, "start_ticks": META_START_TICKS, "config_path": str(meta_cfg), "config_sha256": meta_cfg_sha},
                "meta_worker": None,
                "access_uid": 65534,
                "access_gid": 65534,
            }

            def fake_mount(path: Path) -> dict[str, object]:
                if path == owner:
                    return mount_command("afs-ownerfs", owner)
                return mount_command("afs-dfs", dfs)

            real_stat_record = mount_isolation.stat_record

            def fake_stat(path: Path) -> dict[str, object]:
                record = real_stat_record(path)
                if path.name == "data.bin":
                    record["ino"] = 777
                return record

            def fake_process(pid: int, role: str, expected_sha256: str, expected_boot_id: str, expected_start_ticks: int) -> dict[str, object]:
                observed = identity(role, expected_sha256, "afs-node" if role == "node" else "afs-meta", str(node_cfg if role == "node" else meta_cfg))
                observed["expected_boot_id"] = expected_boot_id
                observed["boot_id_ok"] = observed["boot_id"] == expected_boot_id
                observed["expected_start_ticks"] = expected_start_ticks
                observed["start_ticks_ok"] = observed["start_ticks"] == expected_start_ticks
                return observed

            def fake_access(private: Path, public: Path, uid: int, gid: int) -> dict[str, object]:
                data = public.read_bytes()
                return {
                    "returncode": 0,
                    "stdout": "{}",
                    "stderr": "",
                    "result": {
                        "uid": uid,
                        "gid": gid,
                        "private": {"ok": False, "errno": 13, "errno_name": "EACCES"},
                        "public": {"ok": True, "errno": None, "errno_name": None, "sha256": mount_isolation.sha256_bytes(data), "length": len(data)},
                    },
                }

            real_stat_record = mount_isolation.stat_record

            def fake_stat(path: Path) -> dict[str, object]:
                record = real_stat_record(path)
                if path.name == "data.bin":
                    record["ino"] = 424242
                return record

            args = Namespace(profile="smoke")
            matrix = {"backend": "OwnerFs", "meta": "memory"}
            with mock.patch.object(mount_isolation.platform, "system", return_value="Linux"), \
                mock.patch.object(mount_isolation.os, "geteuid", return_value=0), \
                mock.patch.object(mount_isolation, "mount_identity", side_effect=fake_mount), \
                mock.patch.object(mount_isolation, "process_identity", side_effect=fake_process), \
                mock.patch.object(mount_isolation, "child_access", side_effect=fake_access), \
                mock.patch.object(mount_isolation, "stat_record", side_effect=fake_stat):
                status, reason, checks, records = mount_isolation.run_operations(args, binding, matrix, artifacts, run_dir)

            self.assertEqual(status, "PASS", reason)
            self.assertTrue(all(check["status"] == "PASS" for check in checks))
            self.assertEqual(records["primary_backend"], "OwnerFs")
            self.assertEqual(records["peer_backend"], "DFS")
            self.assertNotEqual(records["content"]["primary_sha256"], records["content"]["peer_sha256"])
            self.assertTrue(records["path_identity"]["inode_numbers_collided"])
            self.assertIn("owner_mount_command", records["mount_identity"])
            self.assertIn("stdout", records["mount_identity"]["owner_mount_command"])
            self.assertEqual(records["error_isolation"]["primary_errno_name"], "EISDIR")
            self.assertTrue((artifacts / "operations.json").exists())
            self.assertTrue(records["path_identity"]["inode_numbers_collided"])
            self.assertTrue(records["path_identity"]["collision_covered_by_mount_content_and_mode_identity"])

    def test_run_operations_blocks_base_outside_mount(self):
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            owner_mount = root / "mnt" / "owner"
            dfs_mount = root / "mnt" / "dfs"
            owner_base = root / "sibling" / "owner"
            dfs_base = dfs_mount
            for path in [owner_mount, dfs_mount, owner_base]:
                path.mkdir(parents=True)
            binding = {
                "owner_mount": owner_mount,
                "dfs_mount": dfs_mount,
                "owner_base_dir": owner_base,
                "dfs_base_dir": dfs_base,
                "node_process": {"pid": 10, "sha256": NODE_SHA, "boot_id": BOOT_ID, "start_ticks": NODE_START_TICKS},
                "meta_process": {"pid": 20, "sha256": META_SHA, "boot_id": BOOT_ID, "start_ticks": META_START_TICKS},
                "meta_worker": None,
                "access_uid": 65534,
                "access_gid": 65534,
            }

            def fake_mount(path: Path) -> dict[str, object]:
                if path in {owner_mount, owner_base}:
                    return mount_command("afs-ownerfs", owner_mount)
                return mount_command("afs-dfs", dfs_mount)

            with mock.patch.object(mount_isolation.platform, "system", return_value="Linux"),                 mock.patch.object(mount_isolation.os, "geteuid", return_value=0),                 mock.patch.object(mount_isolation, "mount_identity", side_effect=fake_mount),                 mock.patch.object(mount_isolation, "process_identity", side_effect=lambda pid, role, sha, boot, ticks: identity(role, sha, "afs-node" if role == "node" else "afs-meta")):
                with self.assertRaises(mount_isolation.BindingError):
                    mount_isolation.run_operations(Namespace(profile="smoke"), binding, {"backend": "OwnerFs", "meta": "memory"}, root / "artifacts", root / "run")

    def test_run_operations_blocks_pseudo_fuse_source(self):
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            owner = root / "owner"
            dfs = root / "dfs"
            owner.mkdir()
            dfs.mkdir()
            binding = {
                "owner_mount": owner,
                "dfs_mount": dfs,
                "owner_base_dir": owner,
                "dfs_base_dir": dfs,
                "node_process": {"pid": 10, "sha256": NODE_SHA, "boot_id": BOOT_ID, "start_ticks": NODE_START_TICKS},
                "meta_process": {"pid": 20, "sha256": META_SHA, "boot_id": BOOT_ID, "start_ticks": META_START_TICKS},
                "meta_worker": None,
                "access_uid": 65534,
                "access_gid": 65534,
            }

            def fake_mount(path: Path) -> dict[str, object]:
                if path == owner:
                    return mount_command("not-afs-ownerfs", owner)
                return mount_command("afs-dfs", dfs)

            with mock.patch.object(mount_isolation.platform, "system", return_value="Linux"),                 mock.patch.object(mount_isolation.os, "geteuid", return_value=0),                 mock.patch.object(mount_isolation, "mount_identity", side_effect=fake_mount),                 mock.patch.object(mount_isolation, "process_identity", side_effect=lambda pid, role, sha, boot, ticks: identity(role, sha, "afs-node" if role == "node" else "afs-meta")):
                with self.assertRaises(mount_isolation.BindingError):
                    mount_isolation.run_operations(Namespace(profile="smoke"), binding, {"backend": "OwnerFs", "meta": "memory"}, root / "artifacts", root / "run")

    def test_run_operations_inconclusive_without_inode_collision(self):
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            owner = root / "owner"
            dfs = root / "dfs"
            owner.mkdir()
            dfs.mkdir()
            binding = {
                "owner_mount": owner,
                "dfs_mount": dfs,
                "owner_base_dir": owner,
                "dfs_base_dir": dfs,
                "node_process": {"pid": 10, "sha256": NODE_SHA, "boot_id": BOOT_ID, "start_ticks": NODE_START_TICKS},
                "meta_process": {"pid": 20, "sha256": META_SHA, "boot_id": BOOT_ID, "start_ticks": META_START_TICKS},
                "meta_worker": None,
                "access_uid": 65534,
                "access_gid": 65534,
            }

            def fake_access(private: Path, public: Path, uid: int, gid: int) -> dict[str, object]:
                data = public.read_bytes()
                return {"returncode": 0, "stdout": "{}", "stderr": "", "result": {"uid": uid, "gid": gid, "private": {"ok": False, "errno": 13}, "public": {"ok": True, "sha256": mount_isolation.sha256_bytes(data)}}}

            with mock.patch.object(mount_isolation.platform, "system", return_value="Linux"),                 mock.patch.object(mount_isolation.os, "geteuid", return_value=0),                 mock.patch.object(mount_isolation, "mount_identity", side_effect=lambda path: mount_command("afs-ownerfs" if path == owner else "afs-dfs", path)),                 mock.patch.object(mount_isolation, "process_identity", side_effect=lambda pid, role, sha, boot, ticks: identity(role, sha, "afs-node" if role == "node" else "afs-meta")),                 mock.patch.object(mount_isolation, "child_access", side_effect=fake_access):
                status, reason, checks, records = mount_isolation.run_operations(Namespace(profile="smoke"), binding, {"backend": "OwnerFs", "meta": "memory"}, root / "artifacts", root / "run")
            self.assertEqual(status, "INCONCLUSIVE", reason)
            self.assertIn(("mount-scoped-inode-identity", "INCONCLUSIVE"), [(check["name"], check["status"]) for check in checks])
            self.assertFalse(records["path_identity"]["inode_numbers_collided"])

    def test_run_operations_fails_substituted_uid_result(self):
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            owner = root / "owner"
            dfs = root / "dfs"
            owner.mkdir()
            dfs.mkdir()
            binding = {
                "owner_mount": owner,
                "dfs_mount": dfs,
                "owner_base_dir": owner,
                "dfs_base_dir": dfs,
                "node_process": {"pid": 10, "sha256": NODE_SHA, "boot_id": BOOT_ID, "start_ticks": NODE_START_TICKS},
                "meta_process": {"pid": 20, "sha256": META_SHA, "boot_id": BOOT_ID, "start_ticks": META_START_TICKS},
                "meta_worker": None,
                "access_uid": 65534,
                "access_gid": 65534,
            }

            real_stat_record = mount_isolation.stat_record

            def fake_stat(path: Path) -> dict[str, object]:
                record = real_stat_record(path)
                if path.name == "data.bin":
                    record["ino"] = 999
                return record

            def fake_access(private: Path, public: Path, uid: int, gid: int) -> dict[str, object]:
                data = public.read_bytes()
                return {"returncode": 0, "stdout": "{}", "stderr": "", "result": {"uid": uid + 1, "gid": gid, "private": {"ok": False, "errno": 13}, "public": {"ok": True, "sha256": mount_isolation.sha256_bytes(data)}}}

            with mock.patch.object(mount_isolation.platform, "system", return_value="Linux"),                 mock.patch.object(mount_isolation.os, "geteuid", return_value=0),                 mock.patch.object(mount_isolation, "mount_identity", side_effect=lambda path: mount_command("afs-ownerfs" if path == owner else "afs-dfs", path)),                 mock.patch.object(mount_isolation, "process_identity", side_effect=lambda pid, role, sha, boot, ticks: identity(role, sha, "afs-node" if role == "node" else "afs-meta")),                 mock.patch.object(mount_isolation, "stat_record", side_effect=fake_stat),                 mock.patch.object(mount_isolation, "child_access", side_effect=fake_access):
                status, reason, checks, records = mount_isolation.run_operations(Namespace(profile="smoke"), binding, {"backend": "OwnerFs", "meta": "memory"}, root / "artifacts", root / "run")
            self.assertEqual(status, "FAIL", reason)
            self.assertIn(("mode-and-real-uid-access-isolation", "FAIL"), [(check["name"], check["status"]) for check in checks])

    def test_run_operations_blocks_pid_reuse_tamper(self):
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            owner = root / "owner"
            dfs = root / "dfs"
            owner.mkdir()
            dfs.mkdir()
            run_dir = root / "run"
            artifacts = run_dir / "artifacts" / "fun-13-mount-isolation"
            artifacts.mkdir(parents=True)
            binding = {
                "owner_mount": owner,
                "dfs_mount": dfs,
                "owner_base_dir": owner,
                "dfs_base_dir": dfs,
                "node_process": {"pid": 10, "sha256": NODE_SHA, "boot_id": BOOT_ID, "start_ticks": NODE_START_TICKS},
                "meta_process": {"pid": 20, "sha256": META_SHA, "boot_id": BOOT_ID, "start_ticks": META_START_TICKS},
                "meta_worker": None,
                "access_uid": 65534,
                "access_gid": 65534,
            }

            def fake_mount(path: Path) -> dict[str, object]:
                return mount_command("afs-ownerfs" if path == owner else "afs-dfs", path)

            def fake_process(pid: int, role: str, expected_sha256: str, expected_boot_id: str, expected_start_ticks: int) -> dict[str, object]:
                observed = identity(role, expected_sha256, "afs-node" if role == "node" else "afs-meta")
                observed["expected_boot_id"] = expected_boot_id
                observed["boot_id_ok"] = observed["boot_id"] == expected_boot_id
                observed["expected_start_ticks"] = expected_start_ticks
                observed["start_ticks"] = expected_start_ticks + 1 if role == "node" else expected_start_ticks
                observed["start_ticks_ok"] = observed["start_ticks"] == expected_start_ticks
                return observed

            args = Namespace(profile="smoke")
            matrix = {"backend": "OwnerFs", "meta": "memory"}
            with mock.patch.object(mount_isolation.platform, "system", return_value="Linux"),                 mock.patch.object(mount_isolation.os, "geteuid", return_value=0),                 mock.patch.object(mount_isolation, "mount_identity", side_effect=fake_mount),                 mock.patch.object(mount_isolation, "process_identity", side_effect=fake_process):
                with self.assertRaises(mount_isolation.BindingError):
                    mount_isolation.run_operations(args, binding, matrix, artifacts, run_dir)

    def test_run_operations_inconclusive_without_same_inode_collision(self):
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            owner = root / "owner"
            dfs = root / "dfs"
            owner.mkdir()
            dfs.mkdir()
            run_dir = root / "run"
            artifacts = run_dir / "artifacts" / "fun-13-mount-isolation"
            artifacts.mkdir(parents=True)
            binding = {
                "owner_mount": owner,
                "dfs_mount": dfs,
                "owner_base_dir": owner,
                "dfs_base_dir": dfs,
                "node_process": {"pid": 10, "sha256": NODE_SHA, "boot_id": BOOT_ID, "start_ticks": NODE_START_TICKS},
                "meta_process": {"pid": 20, "sha256": META_SHA, "boot_id": BOOT_ID, "start_ticks": META_START_TICKS},
                "meta_worker": None,
                "access_uid": 65534,
                "access_gid": 65534,
            }

            def fake_mount(path: Path) -> dict[str, object]:
                return mount_command("afs-ownerfs" if path == owner else "afs-dfs", path)

            def fake_process(pid: int, role: str, expected_sha256: str, expected_boot_id: str, expected_start_ticks: int) -> dict[str, object]:
                observed = identity(role, expected_sha256, "afs-node" if role == "node" else "afs-meta")
                observed["expected_boot_id"] = expected_boot_id
                observed["boot_id_ok"] = observed["boot_id"] == expected_boot_id
                observed["expected_start_ticks"] = expected_start_ticks
                observed["start_ticks_ok"] = observed["start_ticks"] == expected_start_ticks
                return observed

            def fake_access(private: Path, public: Path, uid: int, gid: int) -> dict[str, object]:
                data = public.read_bytes()
                return {
                    "returncode": 0,
                    "stdout": "{}",
                    "stderr": "",
                    "result": {
                        "uid": uid,
                        "gid": gid,
                        "private": {"ok": False, "errno": 13, "errno_name": "EACCES"},
                        "public": {"ok": True, "errno": None, "errno_name": None, "sha256": mount_isolation.sha256_bytes(data), "length": len(data)},
                    },
                }

            args = Namespace(profile="smoke")
            matrix = {"backend": "OwnerFs", "meta": "memory"}
            with mock.patch.object(mount_isolation.platform, "system", return_value="Linux"), \
                mock.patch.object(mount_isolation.os, "geteuid", return_value=0), \
                mock.patch.object(mount_isolation, "mount_identity", side_effect=fake_mount), \
                mock.patch.object(mount_isolation, "process_identity", side_effect=fake_process), \
                mock.patch.object(mount_isolation, "child_access", side_effect=fake_access):
                status, reason, checks, records = mount_isolation.run_operations(args, binding, matrix, artifacts, run_dir)

            self.assertEqual(status, "INCONCLUSIVE")
            inode_check = next(check for check in checks if check["name"] == "mount-scoped-inode-identity")
            self.assertEqual(inode_check["status"], "INCONCLUSIVE")
            self.assertFalse(records["path_identity"]["inode_numbers_collided"])

    def test_run_operations_fails_when_child_uid_gid_do_not_match_binding(self):
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            owner = root / "owner"
            dfs = root / "dfs"
            owner.mkdir()
            dfs.mkdir()
            run_dir = root / "run"
            artifacts = run_dir / "artifacts" / "fun-13-mount-isolation"
            artifacts.mkdir(parents=True)
            binding = {
                "owner_mount": owner,
                "dfs_mount": dfs,
                "owner_base_dir": owner,
                "dfs_base_dir": dfs,
                "node_process": {"pid": 10, "sha256": NODE_SHA, "boot_id": BOOT_ID, "start_ticks": NODE_START_TICKS},
                "meta_process": {"pid": 20, "sha256": META_SHA, "boot_id": BOOT_ID, "start_ticks": META_START_TICKS},
                "meta_worker": None,
                "access_uid": 65534,
                "access_gid": 65534,
            }

            def fake_mount(path: Path) -> dict[str, object]:
                return mount_command("afs-ownerfs" if path == owner else "afs-dfs", path)

            def fake_process(pid: int, role: str, expected_sha256: str, expected_boot_id: str, expected_start_ticks: int) -> dict[str, object]:
                observed = identity(role, expected_sha256, "afs-node" if role == "node" else "afs-meta")
                observed["expected_boot_id"] = expected_boot_id
                observed["boot_id_ok"] = observed["boot_id"] == expected_boot_id
                observed["expected_start_ticks"] = expected_start_ticks
                observed["start_ticks_ok"] = observed["start_ticks"] == expected_start_ticks
                return observed

            def fake_access(private: Path, public: Path, uid: int, gid: int) -> dict[str, object]:
                data = public.read_bytes()
                return {
                    "returncode": 0,
                    "stdout": "{}",
                    "stderr": "",
                    "result": {
                        "uid": 0,
                        "gid": 0,
                        "private": {"ok": False, "errno": 13, "errno_name": "EACCES"},
                        "public": {"ok": True, "errno": None, "errno_name": None, "sha256": mount_isolation.sha256_bytes(data), "length": len(data)},
                    },
                }

            real_stat_record = mount_isolation.stat_record

            def fake_stat(path: Path) -> dict[str, object]:
                record = real_stat_record(path)
                if path.name == "data.bin":
                    record["ino"] = 424242
                return record

            args = Namespace(profile="smoke")
            matrix = {"backend": "OwnerFs", "meta": "memory"}
            with mock.patch.object(mount_isolation.platform, "system", return_value="Linux"), \
                mock.patch.object(mount_isolation.os, "geteuid", return_value=0), \
                mock.patch.object(mount_isolation, "mount_identity", side_effect=fake_mount), \
                mock.patch.object(mount_isolation, "process_identity", side_effect=fake_process), \
                mock.patch.object(mount_isolation, "child_access", side_effect=fake_access), \
                mock.patch.object(mount_isolation, "stat_record", side_effect=fake_stat):
                status, reason, checks, records = mount_isolation.run_operations(args, binding, matrix, artifacts, run_dir)

            self.assertEqual(status, "FAIL")
            access_check = next(check for check in checks if check["name"] == "mode-and-real-uid-access-isolation")
            self.assertEqual(access_check["status"], "FAIL")
            self.assertEqual(records["uid_access"]["result"]["uid"], 0)


class MountIsolationManifestTests(unittest.TestCase):
    def test_fun13_registered_ready_without_promoting_status(self):
        manifest = json.loads((DRIVER.parent.parent / "cases.json").read_text())
        cases = {case["id"]: case for case in manifest["cases"]}
        self.assertEqual(cases["FUN-13"]["driver"]["state"], "READY")
        self.assertEqual(cases["FUN-13"]["driver"]["command"], ["python3", "drivers/mount_isolation.py"])
        self.assertEqual(cases["FUN-13"]["status"], "NOT_RUN")


if __name__ == "__main__":
    unittest.main()
