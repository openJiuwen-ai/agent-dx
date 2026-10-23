#!/usr/bin/env python3
"""R-OWN2 evidence boundaries; synthetic checks and real Linux child waits."""

import dataclasses
import http.server
import importlib.util
import json
import shlex
import shutil
import sys
import tempfile
import threading
import time
import unittest
from pathlib import Path
from types import SimpleNamespace

RUNNER = Path(__file__).resolve().parents[1] / "ownerfs_acceptance.py"
SPEC = importlib.util.spec_from_file_location("ownerfs_lifecycle_under_test", RUNNER)
MODULE = importlib.util.module_from_spec(SPEC)
sys.modules[SPEC.name] = MODULE
SPEC.loader.exec_module(MODULE)


class OwnerFsRunnerEvidenceTests(unittest.TestCase):
    def runner(self, root="/unused-test"):
        args = MODULE.parse_args([
            "--host-b", "127.0.0.1", "--host-c", "127.0.0.1",
            "--bin-dir", "/unused-tools", "--run-id", "lifecycle",
            "--remote-tmp", root,
        ])
        return MODULE.OwnerFsAcceptance(args)

    def test_direct_workspace_fuse_root_needs_no_ownerfs_child(self):
        runner = self.runner()
        for role, mount in [(runner.a, runner.mount_a), (runner.b, runner.mount_b)]:
            def command(script, *, target=mount, **_kwargs):
                if "findmnt" in script:
                    out = json.dumps({"filesystems": [{
                        "target": target, "source": "afs-ownerfs", "fstype": "fuse",
                    }]})
                else:
                    out = "total 0\n"
                return MODULE.CmdResult("test", script, 0, out, "")
            role.run = command
        result = runner.namespace_ready()
        self.assertIn("A_mount", result)
        self.assertIn("B_mount", result)

    def test_directory_name_is_not_proof_of_ownerfs_mount(self):
        runner = self.runner()
        for role in [runner.a, runner.b]:
            role.run = lambda script, **_kwargs: MODULE.CmdResult(
                "test", script, 0,
                json.dumps({"filesystems": [{"target": "/ownerfs", "source": "/dev/vdb1", "fstype": "ext4"}]}), "",
            )
        with self.assertRaises(MODULE.RunnerError):
            runner.namespace_ready()

    def test_race_location_is_checked_against_the_actual_winner(self):
        runner = self.runner()
        root = "race-root"
        root_id = "root-" + root.encode().hex()
        for winner in ("node-a-", "node-b-"):
            expected = winner + runner.run_id
            runner.remote_wait_http = lambda *_args, **_kwargs: {
                "body": json.dumps({"root_id": root_id, "home_node_id": expected}),
            }
            self.assertEqual(runner.location_lookup(root, expected)["home_node_id"], expected)
            with self.assertRaises(MODULE.RunnerError):
                runner.location_lookup(root, "wrong-winner")

    def test_cleanup_failure_cannot_leave_overall_pass(self):
        runner = self.runner()
        runner.run_step = lambda _name, _fn: True
        runner.collect_logs = lambda: None
        runner.cleanup = lambda: runner.note_cleanup_error("normal unmount", "B", "busy")
        result = runner.execute()
        self.assertEqual(result["status"], "FAIL")
        self.assertTrue(result["cleanup_errors"])

    def test_unmount_failure_preserves_run_data_and_has_no_lazy_fallback(self):
        runner = self.runner()
        runner.prepared_roles.update(["A", "B", "C"])
        commands = []
        def command(script, **_kwargs):
            commands.append(script)
            return MODULE.CmdResult("test", script, 1 if "fusermount" in script else 0, "", "busy")
        for role in runner.roles.values():
            role.run = command
        runner.cleanup()
        self.assertTrue(runner.result["cleanup_errors"])
        self.assertFalse(any("rm -rf" in script for script in commands))
        self.assertFalse(any("-uz" in script or "umount -l" in script for script in commands))

    def test_unprepared_directories_are_never_unmounted_or_removed(self):
        runner = self.runner()
        commands = []
        for role in runner.roles.values():
            role.run = lambda script, **_kwargs: commands.append(script)
        runner.cleanup()
        self.assertEqual(commands, [])

    def test_cleanup_stops_nodes_before_a_restarted_meta(self):
        runner = self.runner()
        runner.procs = [SimpleNamespace(name="afs-node-b", role="B"),
                        SimpleNamespace(name="afs-meta", role="C"),
                        SimpleNamespace(name="afs-node-a", role="A")]
        stopped = []
        def wait_process(proc, **_kwargs):
            stopped.append(proc.name)
            runner.procs.remove(proc)
        runner.wait_process = wait_process
        runner.cleanup()
        self.assertEqual(stopped, ["afs-node-a", "afs-node-b", "afs-meta"])

    def test_failed_run_is_retained_even_after_successful_cleanup(self):
        runner = self.runner()
        runner.prepared_roles.update(["A", "B", "C"])
        runner.result["status"] = "FAIL"
        commands = []
        def command(script, **_kwargs):
            commands.append(script)
            return MODULE.CmdResult("test", script, 0, "", "")
        for role in runner.roles.values():
            role.run = command
        runner.cleanup()
        self.assertFalse(any("rm -rf" in script for script in commands))

    @unittest.skipUnless(sys.platform == "linux", "Linux actual command boundary")
    def test_existing_run_directory_is_rejected_without_unmount_or_delete(self):
        with tempfile.TemporaryDirectory(prefix="ownerfs-evidence-") as root:
            runner = self.runner(root)
            old = Path(runner.paths["A_ROOT"])
            old.mkdir(parents=True)
            marker = old / "preserve"
            marker.write_bytes(b"old evidence")
            with self.assertRaises(MODULE.RunnerError):
                runner.prepare_hosts()
            self.assertEqual(marker.read_bytes(), b"old evidence")

    @unittest.skipUnless(sys.platform == "linux", "Linux actual remote path binding")
    def test_alias_inputs_bind_each_role_to_its_canonical_data_path(self):
        with tempfile.TemporaryDirectory(prefix="ownerfs-paths-") as root:
            physical = Path(root) / "physical data"
            physical.mkdir()
            alias = Path(root) / "alias data"
            alias.symlink_to(physical, target_is_directory=True)
            runner = self.runner(str(alias))
            result = runner.prepare_hosts()
            for role in runner.roles:
                expected = str(physical / runner.run_id / role)
                self.assertEqual(runner.paths[role + "_ROOT"], expected)
                self.assertEqual(result["path_bindings"][role]["canonical"], expected)
            self.assertEqual(runner.mount_a, runner.paths["A_ROOT"] + "/mnt")
            self.assertEqual(runner.mount_b, runner.paths["B_ROOT"] + "/mnt")
            self.assertEqual(runner.owner_a, runner.mount_a)
            self.assertEqual(runner.owner_b, runner.mount_b)


@unittest.skipUnless(sys.platform == "linux", "Linux actual HTTP subprocess boundary")
class OwnerFsRestBodyTests(unittest.TestCase):
    def setUp(self):
        self.payload = {
            "diagnostic": "完整响应" * 256,
            "root_id": "root-" + b"race-root".hex(),
            "home_node_id": "node-a-rest-body",
        }
        self.body = json.dumps(self.payload, ensure_ascii=False)
        test = self

        class Handler(http.server.BaseHTTPRequestHandler):
            def do_GET(self):
                body = test.body.encode()
                self.send_response(200)
                self.send_header("Content-Type", "application/json")
                self.send_header("Content-Length", str(len(body)))
                self.end_headers()
                self.wfile.write(body)

            def log_message(self, _format, *_args):
                pass

        self.server = http.server.ThreadingHTTPServer(("127.0.0.1", 0), Handler)
        self.thread = threading.Thread(
            target=lambda: self.server.serve_forever(poll_interval=0.05), daemon=True,
        )
        self.thread.start()
        args = MODULE.parse_args([
            "--host-b", "127.0.0.1", "--host-c", "127.0.0.1",
            "--bin-dir", "/unused-tools", "--run-id", "rest-body",
            "--meta-rest-port", str(self.server.server_port),
        ])
        self.runner = MODULE.OwnerFsAcceptance(args)
        self.url = f"http://127.0.0.1:{self.server.server_port}/v1/roots/{self.payload['root_id']}"

    def tearDown(self):
        self.server.shutdown()
        self.server.server_close()
        self.thread.join(timeout=2)
        self.assertFalse(self.thread.is_alive())

    def test_local_http_retains_complete_body(self):
        self.assertGreater(len(self.body), 500)
        self.assertEqual(self.runner.wait_http(self.url)["body"], self.body)

    def test_remote_subprocess_retains_complete_body_and_parses_authority(self):
        response = self.runner.remote_wait_http(self.runner.c, self.url)
        self.assertEqual(response["status"], 200)
        self.assertEqual(response["body"], self.body)
        self.assertEqual(self.runner.location_lookup("race-root"), self.payload)

    def test_long_response_with_wrong_authority_is_still_rejected(self):
        with self.assertRaisesRegex(MODULE.RunnerError, "root location mismatch"):
            self.runner.location_lookup("race-root", "another-node")

    def test_invalid_json_is_still_rejected(self):
        self.body = self.body[:-1]
        with self.assertRaisesRegex(MODULE.RunnerError, "not authoritative"):
            self.runner.location_lookup("race-root")


@unittest.skipUnless(sys.platform == "linux", "Linux /proc and actual child ownership")
class OwnerFsRealChildTests(unittest.TestCase):
    def setUp(self):
        self.tmp = Path(tempfile.mkdtemp(prefix="ownerfs-child-"))
        self.shell = MODULE.RemoteShell("A", "127.0.0.1", None)
        self.children = []

    def tearDown(self):
        clean = True
        for proc in self.children:
            try:
                self.shell.kill(proc)
            except (MODULE.RunnerError, ProcessLookupError):
                clean = clean and not Path(f"/proc/{proc.pid}").exists()
        if clean:
            shutil.rmtree(self.tmp)
        else:
            print(f"retained failed child evidence: {self.tmp}", file=sys.stderr)

    def start(self, code):
        proc = self.shell.start(
            "unit-child", "exec python3 -u -c " + shlex.quote(code),
            str(self.tmp / f"child-{len(self.children)}.log"),
        )
        self.children.append(proc)
        return proc

    def ready(self, proc):
        deadline = time.monotonic() + 5
        while time.monotonic() < deadline:
            if "ready" in Path(proc.log).read_text():
                return
            time.sleep(0.01)
        self.fail("child did not publish ready marker")

    def test_stop_returns_actual_zero_wait_receipt_and_is_idempotent(self):
        proc = self.start("import signal,time; signal.signal(signal.SIGTERM,lambda *_:exit(0)); print('ready'); time.sleep(30)")
        self.ready(proc)
        receipt = self.shell.kill(proc)
        self.assertEqual(receipt["returncode"], 0)
        self.assertEqual(receipt["pid"], proc.pid)
        self.assertEqual(self.shell.kill(proc), receipt)
        self.assertFalse(Path(f"/proc/{proc.pid}").exists())

    def test_nonzero_exit_is_preserved_and_rejected(self):
        proc = self.start("raise SystemExit(7)")
        with self.assertRaisesRegex(MODULE.RunnerError, "7"):
            self.shell.wait(proc)
        receipt = json.loads(Path(proc.wait_file).read_text())
        self.assertEqual(receipt["returncode"], 7)

    def test_mismatched_birth_identity_is_never_signalled(self):
        proc = self.start("import signal,time; signal.signal(signal.SIGTERM,lambda *_:exit(0)); print('ready'); time.sleep(30)")
        self.ready(proc)
        mismatch = dataclasses.replace(proc, start_ticks=proc.start_ticks + 1)
        with self.assertRaises(MODULE.RunnerError):
            self.shell.kill(mismatch)
        self.assertTrue(Path(f"/proc/{proc.pid}").exists())
        self.assertEqual(self.shell.kill(proc)["returncode"], 0)


if __name__ == "__main__":
    unittest.main()
