import importlib.util
import json
import os
import tempfile
import unittest
from pathlib import Path
from unittest import mock


DRIVER = Path(__file__).resolve().parent / "drivers" / "visibility.py"
SPEC = importlib.util.spec_from_file_location("visibility", DRIVER)
visibility = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(visibility)

NODE_SHA = "a" * 64
META_SHA = "b" * 64
CFG_SHA = "c" * 64
BOOT_ID = "11111111-2222-3333-4444-555555555555"
NODE_START = 123456
META_START = 234567


def write_json(path: Path, value: object) -> None:
    path.write_text(json.dumps(value, indent=2, sort_keys=True), encoding="utf-8")


def process(role: str, *, pid: int = 10, sha: str | None = None, start: int | None = None) -> dict[str, object]:
    return {
        "role": role,
        "pid": pid,
        "sha256": sha or (META_SHA if role == "meta" else NODE_SHA),
        "boot_id": BOOT_ID,
        "start_ticks": start or (META_START if role == "meta" else NODE_START),
        "config_path": "/etc/afs/meta.toml" if role == "meta" else "/etc/afs/node.toml",
        "config_sha256": CFG_SHA,
    }


def observed_identity(role: str, executable: str) -> dict[str, object]:
    facts = {"meta_store": "memory"} if role == "meta" else {"data_mode": "rdma", "rdma_device": "rxe0"}
    return {
        "role": role,
        "pid": 20 if role == "meta" else 10,
        "exists": True,
        "exe_path": "/opt/afs/bin/" + executable,
        "sha256": META_SHA if role == "meta" else NODE_SHA,
        "expected_sha256": META_SHA if role == "meta" else NODE_SHA,
        "sha256_ok": True,
        "boot_id": BOOT_ID,
        "expected_boot_id": BOOT_ID,
        "boot_id_ok": True,
        "start_ticks": META_START if role == "meta" else NODE_START,
        "expected_start_ticks": META_START if role == "meta" else NODE_START,
        "start_ticks_ok": True,
        "cmdline_items": [executable, "--config", "/etc/afs/meta.toml" if role == "meta" else "/etc/afs/node.toml"],
        "config_identity": {"observed_path": "/etc/afs/meta.toml" if role == "meta" else "/etc/afs/node.toml", "expected_path": "/etc/afs/meta.toml" if role == "meta" else "/etc/afs/node.toml", "sha256": CFG_SHA, "expected_sha256": CFG_SHA, "path_ok": True, "sha256_ok": True, "facts": facts},
    }


def mount_command(source: str, target: Path) -> dict[str, object]:
    return {"returncode": 0, "stdout": json.dumps({"filesystems": [{"target": str(target), "source": source, "fstype": "fuse.afs", "options": "rw"}]}), "stderr": ""}


class VisibilityBindingTests(unittest.TestCase):
    def local_binding(self, root: Path, case_id: str = "FUN-03", **extra) -> dict[str, object]:
        mount = root / "mnt"
        mount.mkdir(exist_ok=True)
        binding = {
            "case_id": case_id,
            "profile": "smoke",
            "matrix": {"backend": "OwnerFs", "meta": "memory"},
            "backend": "OwnerFs",
            "writer_mount": str(mount),
            "writer_base_dir": str(mount),
            "reader_mount": str(mount),
            "reader_base_dir": str(mount),
            "rel_path": "vis/file.dat",
            "meta_process": process("meta", pid=20),
            "writer_process": process("node", pid=10),
            "reader_process": process("node", pid=11),
        }
        if case_id == "FUN-02":
            binding.pop("writer_mount"); binding.pop("writer_base_dir"); binding.pop("reader_mount"); binding.pop("reader_base_dir")
            binding["fun02_targets"] = [
                {"label": "home-a-local", "mount": str(mount), "base_dir": str(mount), "rel_path": "vis/fun02-local.dat"},
                {"label": "b-remote-home-a", "mount": str(mount), "base_dir": str(mount), "rel_path": "vis/fun02-remote.dat"},
            ]
        binding.update(extra)
        return binding

    def test_binding_requires_exact_case_profile_matrix_and_full_identity(self):
        with tempfile.TemporaryDirectory(dir="/tmp") as tmp:
            root = Path(tmp)
            doc = {"schema_version": 1, "bindings": [self.local_binding(root)]}
            write_json(root / "bindings.json", doc)
            selected = visibility.validate_binding(visibility.select_binding(doc, "FUN-03", "smoke", {"backend": "OwnerFs", "meta": "memory"}), "FUN-03", "smoke", {"backend": "OwnerFs", "meta": "memory"})
            self.assertEqual(selected["rel_path"], "vis/file.dat")
            with self.assertRaises(visibility.BindingError):
                visibility.select_binding(doc, "FUN-03", "smoke", {"backend": "DFS", "meta": "memory"})
            bad = self.local_binding(root)
            del bad["writer_process"]["boot_id"]
            with self.assertRaises(visibility.BindingError):
                visibility.validate_binding(bad, "FUN-03", "smoke", {"backend": "OwnerFs", "meta": "memory"})

    def test_ssh_worker_rejects_option_injection_and_mac_paths(self):
        worker = {
            "transport": "ssh",
            "host": "-oProxyCommand=bad",
            "python": "/usr/bin/python3",
            "driver": "/opt/afs/visibility.py",
            "mount": "/mnt/afs",
            "base_dir": "/mnt/afs",
            "worker_run_dir": "/tmp/afs-run",
            "expected_process": process("node"),
        }
        with self.assertRaises(visibility.BindingError):
            visibility.worker_record(worker, "node", "worker")
        worker["host"] = "node-b"
        worker["driver"] = "/Users/lzc/visibility.py"
        with self.assertRaises(visibility.BindingError):
            visibility.worker_record(worker, "node", "worker")


class VisibilityOperationTests(unittest.TestCase):
    def patch_preflight(self, mount: Path):
        def fake_mount(path: Path) -> dict[str, object]:
            return mount_command("afs-ownerfs", path if path == mount else mount)

        def fake_process(pid: int, role: str, expected_sha256: str, expected_boot_id: str, expected_start_ticks: int) -> dict[str, object]:
            exe = "afs-meta" if role == "meta" else "afs-node"
            ident = observed_identity(role, exe)
            ident["pid"] = pid
            ident["expected_sha256"] = expected_sha256
            ident["sha256"] = expected_sha256
            ident["expected_start_ticks"] = expected_start_ticks
            ident["start_ticks"] = expected_start_ticks
            return ident

        return mock.patch.multiple(visibility, mount_identity=mock.DEFAULT, process_identity=mock.DEFAULT), fake_mount, fake_process

    def test_fun02_uses_separate_writer_process_and_existing_ro_fd_sees_dirty_states(self):
        with tempfile.TemporaryDirectory(dir="/tmp") as tmp:
            root = Path(tmp)
            result = visibility.run_fun02_target(root, "dir/fun02.dat", 10)
        self.assertTrue(result["accepted_dirty_visible_before_close_or_sync"])
        self.assertEqual(bytes.fromhex(result["resize_from_old_fd"]), b"bravo-0")
        self.assertEqual(result["old_fd_size_after_resize"], 7)
        self.assertEqual(result["writer_returncode"], 0)

    def test_local_fun03_close_to_open_has_no_sleep_and_fresh_reader_sees_close(self):
        with tempfile.TemporaryDirectory(dir="/tmp") as tmp:
            root = Path(tmp)
            mount = root / "mnt"; mount.mkdir()
            run_dir = root / "run"
            artifacts = run_dir / "artifacts" / "fun03"
            binding = VisibilityBindingTests().local_binding(root, "FUN-03")
            validated = visibility.validate_binding(binding, "FUN-03", "smoke", {"backend": "OwnerFs", "meta": "memory"})

            def fake_mount(path: Path) -> dict[str, object]:
                return mount_command("afs-ownerfs", mount)

            def fake_process(pid: int, role: str, expected_sha256: str, expected_boot_id: str, expected_start_ticks: int) -> dict[str, object]:
                return observed_identity(role, "afs-meta" if role == "meta" else "afs-node")

            with mock.patch.object(visibility.platform, "system", return_value="Linux"), \
                mock.patch.object(visibility, "mount_identity", side_effect=fake_mount), \
                mock.patch.object(visibility, "process_identity", side_effect=fake_process), \
                mock.patch.object(visibility, "attach_config_identity", side_effect=lambda ident, path, sha: ident), \
                mock.patch.object(visibility.time, "sleep", side_effect=AssertionError("sleep barrier forbidden")):
                proof = visibility.run_local("FUN-03", "smoke", {"backend": "OwnerFs", "meta": "memory"}, validated, artifacts, run_dir)
        self.assertEqual(proof["status"], "PASS", proof.get("reason"))
        self.assertEqual(proof["checks"][-1]["status"], "PASS")
        self.assertIn("reader starts only after writer close_success", json.dumps(proof))

    def test_local_fun04_fdatasync_and_fsync_fresh_reader_checks_attributes_not_oldfd_permanent(self):
        with tempfile.TemporaryDirectory(dir="/tmp") as tmp:
            root = Path(tmp)
            mount = root / "mnt"; mount.mkdir()
            run_dir = root / "run"
            artifacts = run_dir / "artifacts" / "fun04"
            binding = VisibilityBindingTests().local_binding(root, "FUN-04", old_reader_fd_observation=True)
            validated = visibility.validate_binding(binding, "FUN-04", "smoke", {"backend": "OwnerFs", "meta": "memory"})

            def fake_mount(path: Path) -> dict[str, object]:
                return mount_command("afs-ownerfs", mount)

            def fake_process(pid: int, role: str, expected_sha256: str, expected_boot_id: str, expected_start_ticks: int) -> dict[str, object]:
                return observed_identity(role, "afs-meta" if role == "meta" else "afs-node")

            with mock.patch.object(visibility.platform, "system", return_value="Linux"), \
                mock.patch.object(visibility, "mount_identity", side_effect=fake_mount), \
                mock.patch.object(visibility, "process_identity", side_effect=fake_process), \
                mock.patch.object(visibility, "attach_config_identity", side_effect=lambda ident, path, sha: ident), \
                mock.patch.object(visibility.os, "fdatasync", side_effect=visibility.os.fsync, create=True):
                proof = visibility.run_local("FUN-04", "smoke", {"backend": "OwnerFs", "meta": "memory"}, validated, artifacts, run_dir)
        self.assertEqual(proof["status"], "PASS", proof.get("reason"))
        lanes = proof["checks"][-1]["evidence"]
        self.assertEqual([lane["sync_kind"] for lane in lanes], ["fdatasync", "fsync"])
        self.assertTrue(all(lane["initial_reader"]["ok"] for lane in lanes))
        self.assertTrue(all(lane["reader"]["ok"] for lane in lanes))
        self.assertTrue(all(lane["old_fd_observation"]["opened"] for lane in lanes))
        self.assertTrue(all("before" in lane["old_fd_observation"] and "after" in lane["old_fd_observation"] for lane in lanes))
        self.assertIn("observed-only", json.dumps(lanes))

    def test_pid_reuse_start_ticks_tamper_blocks_before_operations(self):
        with tempfile.TemporaryDirectory(dir="/tmp") as tmp:
            root = Path(tmp)
            mount = root / "mnt"; mount.mkdir()
            run_dir = root / "run"
            artifacts = run_dir / "artifacts" / "fun03"
            binding = visibility.validate_binding(VisibilityBindingTests().local_binding(root, "FUN-03"), "FUN-03", "smoke", {"backend": "OwnerFs", "meta": "memory"})

            def fake_mount(path: Path) -> dict[str, object]:
                return mount_command("afs-ownerfs", mount)

            def fake_process(pid: int, role: str, expected_sha256: str, expected_boot_id: str, expected_start_ticks: int) -> dict[str, object]:
                ident = observed_identity(role, "afs-meta" if role == "meta" else "afs-node")
                ident["start_ticks"] = expected_start_ticks + 1 if role == "node" else expected_start_ticks
                ident["expected_start_ticks"] = expected_start_ticks
                ident["start_ticks_ok"] = ident["start_ticks"] == expected_start_ticks
                return ident

            with mock.patch.object(visibility.platform, "system", return_value="Linux"), \
                mock.patch.object(visibility, "mount_identity", side_effect=fake_mount), \
                mock.patch.object(visibility, "process_identity", side_effect=fake_process), \
                mock.patch.object(visibility, "attach_config_identity", side_effect=lambda ident, path, sha: ident):
                proof = visibility.run_local("FUN-03", "smoke", {"backend": "OwnerFs", "meta": "memory"}, binding, artifacts, run_dir)
        self.assertEqual(proof["status"], "BLOCKED")
        self.assertIn("preflight", proof["reason"])


class VisibilityRemoteTests(unittest.TestCase):
    def remote_binding(self) -> dict[str, object]:
        def worker(name: str, role: str) -> dict[str, object]:
            return {
                "transport": "ssh",
                "host": name,
                "python": "/usr/bin/python3",
                "driver": "/opt/afs/visibility.py",
                "mount": "/mnt/afs",
                "base_dir": "/mnt/afs/home-a",
                "worker_run_dir": "/var/tmp/afs-acceptance",
                "expected_process": process(role, pid=20 if role == "meta" else 10),
            }
        return {
            "case_id": "FUN-03",
            "profile": "smoke",
            "matrix": {"backend": "OwnerFs", "meta": "memory", "transport": "rdma"},
            "backend": "OwnerFs",
            "mode": "remote-host",
            "expected_meta_endpoint": "10.0.0.10:50051",
            "ctl_worker": worker("ctl", "meta"),
            "writer_worker": worker("node-a", "node"),
            "reader_worker": worker("node-b", "node"),
            "rel_path": "vis/fun03.dat",
        }

    def test_remote_host_builds_structured_ssh_identity_and_a_then_b_without_sleep(self):
        binding = visibility.validate_binding(self.remote_binding(), "FUN-03", "smoke", {"backend": "OwnerFs", "meta": "memory", "transport": "rdma"})
        calls = []

        def fake_run(worker: dict[str, object], args: list[str], timeout: int) -> dict[str, object]:
            calls.append((worker["host"], args))
            if args[0] == "identity":
                role = "meta" if worker["host"] == "ctl" else "node"
                event = {
                    "event": "IDENTITY",
                    "platform": {"system": "Linux"},
                    "process_identity": observed_identity(role, "afs-meta" if role == "meta" else "afs-node"),
                    "mount_identity": mount_command("afs-ownerfs", Path("/mnt/afs")),
                    "base_dir": {"ok": True},
                }
            elif args[0] == "worker-write-close":
                event = {"event": "WRITE_CLOSE", "result": {"sha256": "d" * 64, "size": 12, "head": "68656164", "close_success": True}}
            elif args[0] == "worker-read-check":
                event = {"event": "READ_CHECK", "result": {"ok": True}}
            else:
                raise AssertionError(args)
            return {"returncode": 0, "json_events": [event], "json_parse_errors": [], "timed_out": False, "stdout": json.dumps(event), "stderr": "", "argv": ["ssh", "--", worker["host"], "quoted"]}

        with tempfile.TemporaryDirectory(dir="/tmp") as tmp, mock.patch.object(visibility, "run_worker_json", side_effect=fake_run):
            root = Path(tmp)
            proof = visibility.run_remote_host("FUN-03", "smoke", {"backend": "OwnerFs", "meta": "memory", "transport": "rdma"}, binding, root / "artifacts", root)
        self.assertEqual(proof["status"], "PASS", proof.get("reason"))
        write_index = calls.index(("node-a", mock.ANY)) if False else [i for i, call in enumerate(calls) if call[0] == "node-a" and call[1][0] == "worker-write-close"][0]
        read_index = [i for i, call in enumerate(calls) if call[0] == "node-b" and call[1][0] == "worker-read-check"][0]
        self.assertLess(write_index, read_index)
        prefix = visibility.worker_prefix(binding["reader_worker"])
        self.assertEqual(prefix[:3], ["ssh", "-o", "BatchMode=yes"])
        self.assertIn("--", prefix)

    def test_remote_fun04_creates_initial_fixture_then_holds_old_fd_before_sync(self):
        raw_binding = self.remote_binding()
        raw_binding["case_id"] = "FUN-04"
        raw_binding["rel_path"] = "vis/fun04.dat"
        raw_binding["old_reader_fd_observation"] = True
        binding = visibility.validate_binding(raw_binding, "FUN-04", "smoke", {"backend": "OwnerFs", "meta": "memory", "transport": "rdma"})
        calls = []

        def event_record(event: dict[str, object]) -> dict[str, object]:
            return {"returncode": 0, "json_events": [event], "json_parse_errors": [], "timed_out": False, "stdout": json.dumps(event), "stderr": "", "argv": ["ssh"]}

        def fake_run(worker: dict[str, object], args: list[str], timeout: int) -> dict[str, object]:
            calls.append(("run", worker["host"], args))
            if args[0] == "identity":
                role = "meta" if worker["host"] == "ctl" else "node"
                return event_record({"event": "IDENTITY", "platform": {"system": "Linux"}, "process_identity": observed_identity(role, "afs-meta" if role == "meta" else "afs-node"), "mount_identity": mount_command("afs-ownerfs", Path("/mnt/afs")), "base_dir": {"ok": True}})
            if args[0] == "worker-sync":
                payload_marker = "initial" if "initial" in bytes.fromhex(args[args.index("--payload-hex") + 1]).decode() else "final"
                return event_record({"event": "SYNC_WRITE", "result": {"sha256": ("a" if payload_marker == "initial" else "d") * 64, "size": 13 if payload_marker == "initial" else 11, "head": "68656164", "sync_kind": args[args.index("--sync-kind") + 1]}})
            if args[0] == "worker-read-check":
                return event_record({"event": "READ_CHECK", "result": {"ok": True}})
            raise AssertionError(args)

        def fake_start(worker: dict[str, object], args: list[str]) -> dict[str, object]:
            calls.append(("start", worker["host"], args))
            return {"argv": ["ssh"], "proc": object(), "started": 0.0, "ready_events": []}

        def fake_read(started_record: dict[str, object], event: str, timeout: int) -> dict[str, object]:
            calls.append(("ready", "node-b", [event]))
            return {"event": "OLD_FD_READY", "result": {"opened": True, "before": {"sha256": "a" * 64}}}

        def fake_finish(started_record: dict[str, object], command: str, timeout: int) -> dict[str, object]:
            calls.append(("finish", "node-b", [command]))
            return event_record({"event": "OLD_FD_OBSERVATION", "result": {"opened": True, "before": {"sha256": "a" * 64}, "after": {"sha256": "d" * 64}, "contract": "observed-only"}})

        with tempfile.TemporaryDirectory(dir="/tmp") as tmp, \
            mock.patch.object(visibility, "run_worker_json", side_effect=fake_run), \
            mock.patch.object(visibility, "start_worker_json", side_effect=fake_start), \
            mock.patch.object(visibility, "read_worker_event", side_effect=fake_read), \
            mock.patch.object(visibility, "finish_started_worker", side_effect=fake_finish):
            root = Path(tmp)
            proof = visibility.run_remote_host("FUN-04", "smoke", {"backend": "OwnerFs", "meta": "memory", "transport": "rdma"}, binding, root / "artifacts", root)
        self.assertEqual(proof["status"], "PASS", proof.get("reason"))
        def is_final_sync(call, sync_kind: str) -> bool:
            if call[0] != "run" or call[2][0] != "worker-sync" or call[2][-1] != sync_kind:
                return False
            payload = bytes.fromhex(call[2][call[2].index("--payload-hex") + 1]).decode()
            return "initial" not in payload

        for sync_kind in ("fdatasync", "fsync"):
            sync_indices = [i for i, call in enumerate(calls) if is_final_sync(call, sync_kind)]
            self.assertEqual(len(sync_indices), 1)
            final_sync = sync_indices[0]
            ready = max(i for i, call in enumerate(calls) if i < final_sync and call[0] == "ready")
            finish = next(i for i, call in enumerate(calls) if i > final_sync and call[0] == "finish")
            final_read = next(i for i, call in enumerate(calls) if i > finish and call[0] == "run" and call[2][0] == "worker-read-check")
            self.assertLess(ready, final_sync)
            self.assertLess(final_sync, finish)
            self.assertLess(finish, final_read)
        lanes = proof["checks"][-2]["evidence"]
        self.assertTrue(all(lane["old_fd_observation"]["result"]["opened"] for lane in lanes))

    def test_remote_matrix_identity_blocks_stale_meta_or_transport_claim(self):
        raw_binding = self.remote_binding()
        raw_binding["matrix"] = {"backend": "OwnerFs", "meta": "etcd", "transport": "tcp"}
        binding = visibility.validate_binding(raw_binding, "FUN-03", "smoke", {"backend": "OwnerFs", "meta": "etcd", "transport": "tcp"})
        calls = []

        def event_record(event: dict[str, object]) -> dict[str, object]:
            return {"returncode": 0, "json_events": [event], "json_parse_errors": [], "timed_out": False, "stdout": json.dumps(event), "stderr": "", "argv": ["ssh"]}

        def fake_run(worker: dict[str, object], args: list[str], timeout: int) -> dict[str, object]:
            calls.append((worker["host"], args))
            if args[0] != "identity":
                raise AssertionError("fixture operation must not run after matrix mismatch")
            role = "meta" if worker["host"] == "ctl" else "node"
            return event_record({"event": "IDENTITY", "platform": {"system": "Linux"}, "process_identity": observed_identity(role, "afs-meta" if role == "meta" else "afs-node"), "mount_identity": mount_command("afs-ownerfs", Path("/mnt/afs")), "base_dir": {"ok": True}})

        with tempfile.TemporaryDirectory(dir="/tmp") as tmp, mock.patch.object(visibility, "run_worker_json", side_effect=fake_run):
            root = Path(tmp)
            proof = visibility.run_remote_host("FUN-03", "smoke", {"backend": "OwnerFs", "meta": "etcd", "transport": "tcp"}, binding, root / "artifacts", root)
        self.assertEqual(proof["status"], "BLOCKED")
        matrix_check = next(check for check in proof["checks"] if check["name"] == "remote-matrix-identity")
        self.assertEqual(matrix_check["status"], "BLOCKED")
        self.assertTrue(all(call[1][0] == "identity" for call in calls))


if __name__ == "__main__":
    unittest.main()
