"""Small first-party 3FS identity/config/normal-wait guards; Linux only."""
import json
import os
from pathlib import Path
import tempfile
from types import SimpleNamespace
import unittest
from unittest.mock import patch

from threefs_namespace_fixture import CLUSTER, Fixture, PREFIX, TARGETS, safe, validate_child, digest, write_json


class FixtureTests(unittest.TestCase):
    def test_json_publication_is_complete_when_destination_appears(self):
        with tempfile.TemporaryDirectory() as d:
            destination = Path(d) / "receipt.json"
            value = {"identity": "large-value" * 10000, "exit_code": 0}
            original_link = os.link
            def publish(source, target):
                self.assertFalse(destination.exists())
                original_link(source, target)
                self.assertEqual(json.loads(destination.read_text()), value)
            with patch("threefs_namespace_fixture.os.link", side_effect=publish):
                write_json(destination, value)
            self.assertEqual(list(Path(d).iterdir()), [destination])

    def test_json_publication_never_overwrites_existing_sentinel(self):
        with tempfile.TemporaryDirectory() as d:
            destination = Path(d) / "receipt.json"
            destination.write_text("immutable failure sentinel")
            with self.assertRaises(FileExistsError):
                write_json(destination, {"exit_code": 0})
            self.assertEqual(destination.read_text(), "immutable failure sentinel")
            self.assertEqual(list(Path(d).iterdir()), [destination])

    def test_rewrite_all_roles_isolates_paths_ports_and_dlopen(self):
        for role in ("ctl", "a", "b", "c"):
            f = Fixture(role, "/fixed/legacy.py")
            old = str(f.root.parent.parent / "3fs-round3-v84")
            text = "file_path = '" + old + "/log/meta.log'\ncluster_id = 'afs_3fs_round3_v84'\nlisten_port = 19002\naddress = 'RDMA://192.168.109.11:19001'\nexternalClientPath = '/lib/libfdb_c.so'\n"
            new = f.rewrite(text, "meta_main.toml")
            self.assertNotIn(old, new)
            self.assertNotIn("1900", new)
            self.assertIn(CLUSTER, new)
            self.assertIn(str(PREFIX / "lib/libfdb_c.so"), new)
            self.assertIn(str(f.root), new)

    def test_r2_target_map_has_distinct_local_a_targets(self):
        by_chain = {c: [(n, t) for n, t, chain in TARGETS if chain == c] for c in (1, 2)}
        self.assertEqual([n for n, _ in by_chain[1]], [10000, 10001])
        self.assertEqual([n for n, _ in by_chain[2]], [10000, 10002])
        self.assertEqual(len({t for _, t, _ in TARGETS}), 4)

    def test_foreign_config_argv_rejected_even_with_owned_root_argument(self):
        f = Fixture("a", "/fixed/legacy.py")
        argv = f.expected_argv("storage")
        argv[-1] = "/protected/old-config.toml"
        argv.append(str(f.root))
        with self.assertRaisesRegex(RuntimeError, "foreign executable or config argv"):
            f.launch("storage", argv, {})
        with self.assertRaisesRegex(RuntimeError, "foreign role service"):
            f.service_dir("meta")

    def test_legacy_sha_rejects_private_patch_before_import(self):
        with tempfile.TemporaryDirectory() as d:
            p = Path(d) / "legacy.py"
            p.write_text("raise RuntimeError('must not import')\n")
            with self.assertRaisesRegex(RuntimeError, "fixed legacy SHA"):
                Fixture("a", str(p)).legacy()

    def test_owned_path_escape_and_symlink_rejected(self):
        with tempfile.TemporaryDirectory() as d:
            root = Path(d)
            (root / "link").symlink_to("/tmp")
            for path in (root / "../old", root / "link/data", root.parent / "other"):
                with self.assertRaisesRegex(RuntimeError, "path|symlink"):
                    safe(path, root)

    def fixture_life(self, directory, service="fuse"):
        f = Fixture("a", "/fixed/legacy.py")
        f.root = Path(directory)
        f.admission = f.root / "inputs/admission.json"
        f.admission.parent.mkdir()
        life = f.root / "run" / service
        life.mkdir(parents=True)
        argv = f.expected_argv(service)
        launch = {"argv": argv, "env": {}, "config_sha256": {"node": "sha"}, "script_sha256": "script"}
        observed = {"pid": 123, "state": "S", "start_ticks": "1234", "exe": argv[0], "exe_dev": 1, "exe_ino": 2, "exe_sha256": "elf", "boot_id": "boot", "argv": [v.encode() for v in argv]}
        identity = {**launch, **observed, "argv": argv, "supervisor_pid": 122}
        (life / "launch.json").write_text(json.dumps(launch))
        (life / "identity.json").write_text(json.dumps(identity))
        return f, life, launch, observed, identity

    def test_foreign_child_refused_before_unmount_or_signal(self):
        with tempfile.TemporaryDirectory() as d:
            f, life, launch, observed, saved = self.fixture_life(d)
            observed["start_ticks"] = "foreign"
            with patch.object(f, "launch", return_value=launch), patch("threefs_namespace_fixture.os.pidfd_open", return_value=42), patch("threefs_namespace_fixture.os.close"), patch("threefs_namespace_fixture.proc_identity", return_value=observed), patch("threefs_namespace_fixture.subprocess.run") as unmount, patch("threefs_namespace_fixture.signal.pidfd_send_signal") as send:
                with self.assertRaisesRegex(RuntimeError, "child identity differs"):
                    f.stop_service("fuse")
                unmount.assert_not_called()
                send.assert_not_called()

    def test_foreign_mount_refused_before_unmount_or_signal(self):
        with tempfile.TemporaryDirectory() as d:
            f, life, launch, observed, saved = self.fixture_life(d)
            (life / "mount.json").write_text(json.dumps({"id": 9, "source": "owned"}))
            with patch.object(f, "launch", return_value=launch), patch("threefs_namespace_fixture.os.pidfd_open", return_value=42), patch("threefs_namespace_fixture.os.close"), patch("threefs_namespace_fixture.proc_identity", return_value=observed), patch.object(f, "exact_mount", return_value={"id": 10, "source": "foreign"}), patch("threefs_namespace_fixture.subprocess.run") as unmount, patch("threefs_namespace_fixture.signal.pidfd_send_signal") as send:
                with self.assertRaisesRegex(RuntimeError, "mount incarnation differs"):
                    f.stop_service("fuse")
                unmount.assert_not_called()
                send.assert_not_called()

    def test_unmount_failure_preserved_without_term_fallback(self):
        with tempfile.TemporaryDirectory() as d:
            f, life, launch, observed, saved = self.fixture_life(d)
            mount = {"id": 9, "source": "owned"}
            (life / "mount.json").write_text(json.dumps(mount))
            helper = f.root / "unmount-tool"
            helper.write_text("fixed tool")
            f.admission.write_text(json.dumps({"fusermount3": {"path": str(helper), "sha256": digest(helper)}}))
            failed = SimpleNamespace(args=[str(helper), "-u", str(f.root / "mount")], returncode=1, stdout="", stderr="busy")
            with patch.object(f, "launch", return_value=launch), patch("threefs_namespace_fixture.os.pidfd_open", return_value=42), patch("threefs_namespace_fixture.os.close"), patch("threefs_namespace_fixture.proc_identity", return_value=observed), patch.object(f, "exact_mount", return_value=mount), patch("threefs_namespace_fixture.subprocess.run", return_value=failed), patch("threefs_namespace_fixture.signal.pidfd_send_signal") as send:
                with self.assertRaisesRegex(RuntimeError, "normal unmount failed"):
                    f.stop_service("fuse")
                self.assertEqual(json.loads((life / "unmount.json").read_text())["exit"], 1)
                send.assert_not_called()

    def test_actual_wait0_required_not_process_disappearance(self):
        with tempfile.TemporaryDirectory() as d:
            f, life, launch, observed, saved = self.fixture_life(d, "storage")
            receipt = {"launch": launch, "identity": saved, "exit_code": -15, "pid": 123, "supervisor_pid": 122}
            (life / "exit.json").write_text(json.dumps(receipt))
            with patch.object(f, "launch", return_value=launch), self.assertRaisesRegex(RuntimeError, "real child wait not normal0"):
                f.stop_service("storage")
            receipt["exit_code"] = 0
            (life / "exit.json").write_text(json.dumps(receipt))
            with patch.object(f, "launch", return_value=launch), patch("threefs_namespace_fixture.os.pidfd_open") as open_pid:
                self.assertEqual(f.stop_service("storage")["exit_code"], 0)
                open_pid.assert_not_called()

    def test_wait_receipt_from_foreign_child_is_rejected(self):
        with tempfile.TemporaryDirectory() as d:
            f, life, launch, observed, saved = self.fixture_life(d, "storage")
            receipt = {"launch": launch, "identity": {**saved, "pid": 999}, "exit_code": 0, "pid": 999, "supervisor_pid": 122}
            (life / "exit.json").write_text(json.dumps(receipt))
            with patch.object(f, "launch", return_value=launch), self.assertRaisesRegex(RuntimeError, "actual wait identity differs"):
                f.stop_service("storage")

    def test_supervisor_rejects_changed_launch_before_popen(self):
        with tempfile.TemporaryDirectory() as d:
            f, life, launch, observed, saved = self.fixture_life(d, "storage")
            changed = {**launch, "script_sha256": "modified"}
            with patch.object(f, "launch", return_value=changed), patch("threefs_namespace_fixture.subprocess.Popen") as popen:
                with self.assertRaisesRegex(RuntimeError, "supervisor launch modified"):
                    f.supervise("storage")
                popen.assert_not_called()


if __name__ == "__main__":
    unittest.main()
