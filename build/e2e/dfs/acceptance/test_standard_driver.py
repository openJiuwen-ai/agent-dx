import contextlib
import importlib.util
import io
import json
import os
import shlex
import subprocess
import sys
import tempfile
import unittest
from unittest import mock
from pathlib import Path

drivers = Path(__file__).parent / "drivers"
sys.path.insert(0, str(drivers))
spec = importlib.util.spec_from_file_location("standard_driver", drivers / "standard.py")
standard = importlib.util.module_from_spec(spec)
spec.loader.exec_module(standard)


class StandardAccountingTest(unittest.TestCase):
    def setUp(self):
        self.counts = {"tap_ok": 7, "tap_not_ok": 1, "prove_files": 2, "prove_tests": 8, "tap_planned": 8}

    def test_complete(self):
        self.assertTrue(standard.complete_accounting(self.counts, 2))

    def test_lost_file(self):
        self.assertFalse(standard.complete_accounting(self.counts, 3))

    def test_lost_subtest(self):
        self.counts["tap_planned"] = 9
        self.assertFalse(standard.complete_accounting(self.counts, 2))

    def test_missing_summary(self):
        self.counts["prove_tests"] = None
        self.assertFalse(standard.complete_accounting(self.counts, 2))

    def test_empty_pass(self):
        self.counts.update(tap_ok=0, tap_not_ok=0, prove_tests=0, tap_planned=0)
        self.assertFalse(standard.complete_accounting(self.counts, 2))


class StandardTapFileProgressTest(unittest.TestCase):
    def parse_text(self, text, selected):
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            stdout = root / "pjdfstest.stdout.tap"
            stderr = root / "pjdfstest.stderr.log"
            stdout.write_text(text, encoding="utf-8")
            stderr.write_text("", encoding="utf-8")
            return standard.parse_tap_and_prove(stdout, stderr, selected)

    def test_complete_verbose_tap_counts_completed_files_and_todo(self):
        accounting = self.parse_text(
            "\n".join([
                "/suite/tests/open/00.t .......... ",
                "1..2",
                "ok 1",
                "not ok 2 # TODO upstream behavior",
                "ok",
                "/suite/tests/mkdir/00.t .......... ",
                "1..1",
                "ok 1 # SKIP environment",
                "ok",
                "Files=2, Tests=3,  0 wallclock secs",
                "Result: PASS",
                "",
            ]),
            ["open/00.t", "mkdir/00.t"],
        )
        self.assertEqual(accounting["observed_started_files"], 2)
        self.assertEqual(accounting["observed_completed_files"], 2)
        self.assertEqual(accounting["unobserved_files"], 0)
        self.assertEqual(accounting["observed_incomplete_files"], 0)
        self.assertEqual(accounting["truncated_current_test"], None)
        self.assertEqual(accounting["tap_todo"], 1)
        self.assertEqual(accounting["tap_todo_not_ok"], 1)
        self.assertEqual(accounting["tap_skip"], 1)
        self.assertTrue(standard.complete_accounting(accounting, 2))

    def test_timeout_partial_tap_keeps_unobserved_distinct_from_incomplete(self):
        accounting = self.parse_text(
            "\n".join([
                "/suite/tests/open/00.t .......... ",
                "1..1",
                "ok 1",
                "ok",
                "/suite/tests/mkdir/00.t .......... ",
                "1..2",
                "ok 1",
                "",
            ]),
            ["open/00.t", "mkdir/00.t", "rename/00.t"],
        )
        self.assertEqual(accounting["observed_started_files"], 2)
        self.assertEqual(accounting["executed_files"], 2)
        self.assertEqual(accounting["observed_completed_files"], 1)
        self.assertEqual(accounting["observed_incomplete_files"], 1)
        self.assertEqual(accounting["unobserved_files"], 1)
        self.assertEqual(accounting["observed_incomplete_tests"], ["mkdir/00.t"])
        self.assertEqual(accounting["unobserved_tests"], ["rename/00.t"])
        self.assertEqual(accounting["truncated_current_test"], "mkdir/00.t")
        self.assertFalse(standard.complete_accounting(accounting, 3))

    def test_timeout_between_files_reports_unobserved_without_incomplete_current(self):
        accounting = self.parse_text(
            "\n".join([
                "/suite/tests/open/00.t .......... ",
                "1..1",
                "ok 1",
                "ok",
                "",
            ]),
            ["open/00.t", "mkdir/00.t"],
        )
        self.assertEqual(accounting["observed_started_files"], 1)
        self.assertEqual(accounting["observed_completed_files"], 1)
        self.assertEqual(accounting["observed_incomplete_files"], 0)
        self.assertEqual(accounting["unobserved_files"], 1)
        self.assertEqual(accounting["truncated_current_test"], None)
        self.assertFalse(standard.complete_accounting(accounting, 2))


class StandardRemoteIdentityHelpersTest(unittest.TestCase):
    def test_suite_git_trust_is_scoped_to_the_selected_repository(self):
        with tempfile.TemporaryDirectory() as td:
            root = Path(td).resolve()
            with mock.patch.object(standard, 'run_text', return_value={'returncode': 0, 'stdout': '', 'stderr': ''}) as run:
                standard.suite_identity(root)
            expected = ['git', '-c', f'safe.directory={root}', '-C', str(root)]
            self.assertEqual(run.call_args_list, [mock.call(expected + ['rev-parse', 'HEAD']), mock.call(expected + ['status', '--porcelain'])])

    def test_suite_git_failure_remains_explicit_in_identity(self):
        with tempfile.TemporaryDirectory() as td:
            failed = {'returncode': 128, 'stdout': '', 'stderr': 'fatal: invalid repository'}
            with mock.patch.object(standard, 'run_text', return_value=failed):
                identity = standard.suite_identity(Path(td))
            self.assertEqual(identity['git_head'], '')
            self.assertEqual(identity['git_head_command'], failed)

    def test_structured_ssh_quotes_all_remote_arguments(self):
        prefix = ['ssh', '-o', 'BatchMode=yes', '-F', '/tmp/ssh config', '--', 'node-b', '/usr/bin/python3', '/opt/standard.py']
        args = ['worker-identity', '--path', "/tmp/a; touch /tmp/unwanted", '--base-path', "$(touch /tmp/also-unwanted)", 'a\nb', "single'quote", '']
        proc = mock.Mock(returncode=0)
        proc.communicate.return_value = ('{"event":"identity"}\n', '')
        with mock.patch.object(standard.subprocess, 'Popen', return_value=proc) as launch:
            record = standard.run_worker_json(prefix, args, timeout=2)
        actual = launch.call_args.args[0]
        self.assertEqual(actual[:-1], prefix[:7])
        self.assertEqual(shlex.split(actual[-1]), prefix[7:] + args)
        self.assertEqual(record['argv'], actual)

    def test_unstructured_ssh_is_rejected_before_launch(self):
        with mock.patch.object(standard.subprocess, 'Popen') as launch:
            with self.assertRaises(ValueError):
                standard.run_worker_json(['ssh', 'node-b', '/usr/bin/python3', '/opt/standard.py'], ['worker-identity'], timeout=2)
        launch.assert_not_called()

    def test_local_worker_keeps_argument_vector(self):
        prefix = [sys.executable, '/opt/standard.py']
        args = ['worker-identity', '--path', '/tmp/a; touch /tmp/unwanted']
        proc = mock.Mock(returncode=0)
        proc.communicate.return_value = ('{"event":"identity"}\n', '')
        with mock.patch.object(standard.subprocess, 'Popen', return_value=proc) as launch:
            standard.run_worker_json(prefix, args, timeout=2)
        self.assertEqual(launch.call_args.args[0], prefix + args)

    def test_parse_start_ticks_handles_comm_with_spaces(self):
        stat_text = "123 (python worker) S " + " ".join(str(i) for i in range(1, 25))
        self.assertEqual(standard.parse_start_ticks(stat_text), 19)

    def test_config_path_from_cmdline_accepts_split_and_equals_forms(self):
        self.assertEqual(standard.config_path_from_cmdline(["afs-node", "--config", "/tmp/node.toml"]), Path("/tmp/node.toml"))
        self.assertEqual(standard.config_path_from_cmdline(["afs-meta", "--config=/tmp/meta.toml"]), Path("/tmp/meta.toml"))

    def test_parse_expected_process_requires_role_pid_sha(self):
        role, pid, sha = standard.parse_expected_process("node=123:" + "a" * 64)
        self.assertEqual((role, pid, sha), ("node", 123, "a" * 64))
        with self.assertRaises(Exception):
            standard.parse_expected_process("node=abc:" + "a" * 64)
        with self.assertRaises(Exception):
            standard.parse_expected_process("other=123:" + "a" * 64)

    def test_config_summary_parses_real_single_quote_toml_and_tls_digests(self):
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            for name in ("ca.pem", "node.pem", "node-key.pem", "peer-a.pem", "peer-b.pem"):
                (root / name).write_text(f"material-{name}", encoding="utf-8")
            cfg_path = root / "node.toml"
            cfg_path.write_text(
                "\n".join([
                    "meta_endpoint = 'https://192.168.109.12:17880/path#fragment'",
                    "grpc_listen = '0.0.0.0:17884'",
                    "rest_listen = '0.0.0.0:17885'",
                    f"tls_ca_certificate = '{root / 'ca.pem'}'",
                    f"tls_identity_certificate = '{root / 'node.pem'}'",
                    f"tls_identity_private_key = '{root / 'node-key.pem'}'",
                    "tls_server_name = 'afs-cluster'",
                    f"trusted_node_certs = {{ memory-node-a = '{root / 'peer-a.pem'}', memory-node-b = '{root / 'peer-b.pem'}' }}",
                ]),
                encoding="utf-8",
            )
            summary = standard.config_summary(cfg_path)
            self.assertEqual(summary["meta_endpoint"], "https://192.168.109.12:17880/path#fragment")
            self.assertEqual(summary["grpc_listen"], "0.0.0.0:17884")
            self.assertTrue(summary["tls_required"])
            self.assertEqual(summary["tls_missing"], [])
            self.assertEqual(set(summary["tls"]), {"tls_ca_certificate", "tls_identity_certificate", "tls_identity_private_key"})
            self.assertEqual(set(summary["trusted_node_certs"]), {"memory-node-a", "memory-node-b"})
            self.assertNotIn("material-node-key", json.dumps(summary))

    def test_config_summary_fail_closed_on_partial_tls(self):
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            (root / "ca.pem").write_text("ca", encoding="utf-8")
            cfg_path = root / "meta.toml"
            cfg_path.write_text(f"grpc_listen = '0.0.0.0:17880'\ntls_ca_certificate = '{root / 'ca.pem'}'\n", encoding="utf-8")
            summary = standard.config_summary(cfg_path)
            self.assertTrue(summary["tls_required"])
            self.assertEqual(set(summary["tls_missing"]), {"tls_identity_certificate", "tls_identity_private_key"})

    def test_run_worker_json_times_out_and_terminates_silent_child(self):
        record = standard.run_worker_json([sys.executable, "-c", "import time; time.sleep(30)"], [], timeout=1)
        self.assertTrue(record["timed_out"])
        self.assertNotEqual(record["returncode"], 0)


    def test_artifact_manifest_excludes_proof_and_hashes_raw_files(self):
        with tempfile.TemporaryDirectory() as tmp:
            run_dir = Path(tmp)
            artifacts = run_dir / "artifacts" / "std-01-pjdfstest"
            artifacts.mkdir(parents=True)
            raw = artifacts / "pjdfstest.stdout.tap"
            raw.write_text("ok 1\n", encoding="utf-8")
            proof = artifacts / "proof.json"
            proof.write_text('{"old": true}\n', encoding="utf-8")
            manifest = standard.artifact_file_manifest(artifacts, run_dir, exclude={proof})
            self.assertEqual([entry["path"] for entry in manifest], ["artifacts/std-01-pjdfstest/pjdfstest.stdout.tap"])
            self.assertEqual(manifest[0]["bytes"], raw.stat().st_size)
            self.assertEqual(manifest[0]["sha256"], standard.sha256_file(raw))


def mount_event(source="afs-dfs", fstype="fuse.afs", target="/mnt/dfs"):
    return {"returncode": 0, "stdout": json.dumps({"filesystems": [{"source": source, "fstype": fstype, "target": target}]})}


def strict_event(role, sha, boot, machine, endpoint="http://10.0.0.1:7400", listen="0.0.0.0:7400", tls=None, exists=True, sha_ok=True):
    cfg = {"sha256": "c" * 64, "tls": tls or {}}
    if role == "node":
        cfg["meta_endpoint"] = endpoint
        network = {"interface_ips": ["10.0.0.2"], "listen_sockets": []}
    else:
        cfg["grpc_listen"] = listen
        network = {"interface_ips": ["10.0.0.1"], "listen_sockets": [{"family": "tcp", "ip": "0.0.0.0", "port": 7400, "inode": "1"}]}
    return {
        "role": role,
        "pid": 10 if role == "node" else 20,
        "exists": exists,
        "boot_id": boot,
        "machine_id": machine,
        "exe_path": "/opt/afs/bin/" + ("afs-node" if role == "node" else "afs-meta"),
        "exe_dev": 1,
        "exe_inode": 2 if role == "node" else 3,
        "start_ticks": 123,
        "sha256": sha,
        "expected_sha256": sha,
        "sha256_ok": sha_ok,
        "config": cfg,
        "network": network,
    }


def worker_record(event):
    return {"argv": ["worker"], "returncode": 0, "stdout": json.dumps(event) + "\n", "stderr": "", "json_events": [event], "json_parse_errors": [], "timed_out": False}


def complete_worker_proof(status="PASS", manifest_override=None, case_id="STD-01", artifact_root="artifacts/std-01-pjdfstest"):
    stdout_name = "pjdfstest.stdout.tap" if artifact_root == "artifacts/std-01-pjdfstest" else "stdout.log"
    manifest = manifest_override if manifest_override is not None else [
        {"path": f"{artifact_root}/identity.json", "bytes": 100, "sha256": "b" * 64},
        {"path": f"{artifact_root}/command.json", "bytes": 100, "sha256": "c" * 64},
        {"path": f"{artifact_root}/tap-accounting.json", "bytes": 100, "sha256": "d" * 64},
        {"path": f"{artifact_root}/{stdout_name}", "bytes": 10, "sha256": "e" * 64},
    ]
    return {
        "case_id": case_id,
        "profile": "smoke",
        "status": status,
        "reason": "",
        "checks": [{"name": "worker-check", "status": "PASS", "evidence": {}, "artifact": f"{artifact_root}/{stdout_name}"}],
        "artifacts": {"root": artifact_root, "manifest": manifest},
        "accounting": {"tap_ok": 1},
        "command": {"returncode": 0},
        "identity": {"artifact": f"{artifact_root}/identity.json"},
        "coverage": {"profile": "smoke", "axes": {}},
    }


class StandardHostRemoteGuardTest(unittest.TestCase):
    def host_args(self, run_dir):
        return [
            "--worker-a-json", '["worker-a"]',
            "--worker-b-json", '["worker-b"]',
            "--worker-a-expected-process", "meta=20:" + "b" * 64,
            "--worker-b-expected-process", "node=10:" + "a" * 64,
            "--expected-meta-endpoint", "http://10.0.0.1:7400",
            "--mount-b", "/mnt/dfs",
            "--backend", "DFS",
            "--run-dir", str(run_dir),
            "--worker-run-dir-b", "/mnt/guest-ext4/std01",
        ]

    def run_host_with_fake_worker(self, fake_worker, run_dir, extra_args=None):
        original = standard.run_worker_json
        standard.run_worker_json = fake_worker
        try:
            out = io.StringIO()
            with contextlib.redirect_stdout(out):
                args = self.host_args(run_dir)
                if extra_args:
                    args.extend(extra_args)
                rc = standard.host_main(args)
            return rc, json.loads(out.getvalue())
        finally:
            standard.run_worker_json = original

    def test_host_timeout_emits_blocked_proof(self):
        with tempfile.TemporaryDirectory() as tmp:
            run_dir = Path(tmp)
            def fake_worker(prefix, args, timeout):
                return {"argv": prefix + args, "returncode": -15, "stdout": "", "stderr": "", "json_events": [], "json_parse_errors": [], "timed_out": True}
            rc, proof = self.run_host_with_fake_worker(fake_worker, run_dir)
            self.assertEqual(rc, 1)
            self.assertEqual(proof["status"], "BLOCKED")
            self.assertFalse(proof["remote_identity"]["suite_invoked"])
            self.assertTrue((run_dir / "artifacts/std-01-pjdfstest/proof.json").exists())

    def test_host_malformed_json_emits_blocked_proof(self):
        with tempfile.TemporaryDirectory() as tmp:
            run_dir = Path(tmp)
            def fake_worker(prefix, args, timeout):
                return {"argv": prefix + args, "returncode": 0, "stdout": "not-json\n", "stderr": "", "json_events": [], "json_parse_errors": [{"line": 1, "error": "bad", "text": "not-json"}], "timed_out": False}
            rc, proof = self.run_host_with_fake_worker(fake_worker, run_dir)
            self.assertEqual(rc, 1)
            self.assertEqual(proof["status"], "BLOCKED")
            self.assertFalse(proof["remote_identity"]["suite_invoked"])


    def test_host_rejects_macos_worker_run_dir(self):
        with tempfile.TemporaryDirectory() as tmp:
            run_dir = Path(tmp)
            calls = []
            def fake_worker(prefix, args, timeout):
                calls.append(args[0])
                raise AssertionError("worker must not run when worker-run-dir-b is a host path")
            rc, proof = self.run_host_with_fake_worker(fake_worker, run_dir, ["--worker-run-dir-b", "/Users/lzc/evidence/std01"])
            self.assertEqual(rc, 1)
            self.assertEqual(proof["status"], "BLOCKED")
            self.assertFalse(calls)


    def test_host_rejects_non_ext4_worker_run_dir_before_suite(self):
        with tempfile.TemporaryDirectory() as tmp:
            run_dir = Path(tmp)
            calls = []
            meta = {"event": "IDENTITY", "platform": {"system": "Linux"}, "mount": mount_event(), "base_mount": mount_event(), "processes": {"meta": strict_event("meta", "b" * 64, "boot-a", "machine-a")}}
            node = {"event": "IDENTITY", "platform": {"system": "Linux"}, "mount": mount_event(), "base_mount": mount_event(), "worker_run_dir": "/mnt/not-ext4/std01", "worker_run_dir_mount": mount_event("tmpfs", "tmpfs", "/mnt/not-ext4"), "processes": {"node": strict_event("node", "a" * 64, "boot-b", "machine-b")}}
            def fake_worker(prefix, args, timeout):
                calls.append(args[0])
                if args[0] == "worker-pjdfstest":
                    raise AssertionError("suite must not execute before worker run dir qualifies as ext4")
                return worker_record(meta if prefix == ["worker-a"] else node)
            rc, proof = self.run_host_with_fake_worker(fake_worker, run_dir, ["--worker-run-dir-b", "/mnt/not-ext4/std01"])
            self.assertEqual(rc, 1)
            self.assertEqual(proof["status"], "BLOCKED")
            self.assertEqual(calls, ["worker-identity", "worker-identity"])
            self.assertFalse(proof["remote_identity"]["suite_invoked"])

    def test_host_passes_worker_run_dir_to_suite_and_marks_remote_artifacts(self):
        with tempfile.TemporaryDirectory() as tmp:
            run_dir = Path(tmp)
            calls = []
            meta = {"event": "IDENTITY", "platform": {"system": "Linux"}, "mount": mount_event(), "base_mount": mount_event(), "processes": {"meta": strict_event("meta", "b" * 64, "boot-a", "machine-a")}}
            node = {"event": "IDENTITY", "platform": {"system": "Linux"}, "mount": mount_event(), "base_mount": mount_event(), "worker_run_dir": "/mnt/guest-ext4/std01", "worker_run_dir_mount": mount_event("/dev/vdb", "ext4", "/mnt/guest-ext4"), "processes": {"node": strict_event("node", "a" * 64, "boot-b", "machine-b")}}
            worker_proof = complete_worker_proof()
            def fake_worker(prefix, args, timeout):
                calls.append(args)
                if args[0] == "worker-pjdfstest":
                    self.assertIn("--run-dir", args)
                    self.assertEqual(args[args.index("--run-dir") + 1], "/mnt/guest-ext4/std01")
                    return worker_record({"event": "PJDFSTEST", "proof": worker_proof})
                return worker_record(meta if prefix == ["worker-a"] else node)
            rc, proof = self.run_host_with_fake_worker(fake_worker, run_dir)
            self.assertEqual(rc, 0)
            self.assertEqual(proof["status"], "PASS")
            self.assertEqual(proof["artifacts"]["root"], "artifacts/std-01-pjdfstest")
            self.assertEqual(proof["artifacts"]["remote_worker"]["location"], "worker-b")
            self.assertEqual(proof["artifacts"]["remote_worker"]["manifest_file_count"], 4)
            self.assertEqual(proof["worker_run_dir_b"], "/mnt/guest-ext4/std01")
            self.assertTrue(proof["remote_identity"]["suite_invoked"])
            self.assertEqual(proof["checks"][2]["artifact"], "artifacts/std-01-pjdfstest/remote-worker-records.json")
            self.assertEqual(proof["checks"][2]["evidence"]["remote_artifact"], "artifacts/std-01-pjdfstest/pjdfstest.stdout.tap")
            self.assertTrue(proof["checks"][2]["evidence"]["remote_artifact_manifest_matched"])

    def test_host_accepts_generic_worker_driver_event(self):
        with tempfile.TemporaryDirectory() as tmp:
            run_dir = Path(tmp)
            meta = {"event": "IDENTITY", "platform": {"system": "Linux"}, "mount": mount_event(), "base_mount": mount_event(), "processes": {"meta": strict_event("meta", "b" * 64, "boot-a", "machine-a")}}
            node = {"event": "IDENTITY", "platform": {"system": "Linux"}, "mount": mount_event(), "base_mount": mount_event(), "worker_run_dir": "/mnt/guest-ext4/std03", "worker_run_dir_mount": mount_event("/dev/vdb", "ext4", "/mnt/guest-ext4"), "processes": {"node": strict_event("node", "a" * 64, "boot-b", "machine-b")}}
            worker_proof = complete_worker_proof(case_id="STD-03", artifact_root="artifacts/std-03-fsx")
            seen_suite_args = []

            def fake_worker(prefix, args, timeout):
                if args[0] == "worker-driver":
                    seen_suite_args.extend(args)
                    return worker_record({"event": "DRIVER", "proof": worker_proof})
                return worker_record(meta if prefix == ["worker-a"] else node)

            rc, proof = self.run_host_with_fake_worker(
                fake_worker,
                run_dir,
                [
                    "--case-id", "STD-03",
                    "--worker-run-dir-b", "/mnt/guest-ext4/std03",
                    "--worker-suite-event", "DRIVER",
                    "--worker-suite-args-json", json.dumps(["worker-driver", "--driver", "/opt/afs-acceptance/drivers/fsx.py", "--", "--case-id", "STD-03"]),
                ],
            )
            self.assertEqual(rc, 0)
            self.assertEqual(proof["case_id"], "STD-03")
            self.assertEqual(proof["status"], "PASS")
            self.assertEqual(proof["artifacts"]["root"], "artifacts/std-03-remote")
            self.assertEqual(proof["artifacts"]["remote_worker"]["manifest_file_count"], 4)
            self.assertEqual(seen_suite_args[:3], ["worker-driver", "--driver", "/opt/afs-acceptance/drivers/fsx.py"])
            self.assertTrue((run_dir / "artifacts/std-03-remote/proof.json").exists())

    def test_generic_worker_artifact_validation_accepts_embedded_proof_without_manifest(self):
        worker_proof = complete_worker_proof(
            case_id="STD-04",
            artifact_root="artifacts/std-04-random",
            manifest_override=[],
        )
        worker_proof.pop("command")
        validation = standard.validate_worker_artifacts(worker_proof)
        self.assertTrue(validation["valid"])
        self.assertEqual(validation["manifest_file_count"], 0)
        self.assertTrue(validation["embedded"]["accounting"])
        self.assertTrue(validation["embedded"]["identity"])

    def test_worker_driver_marks_child_as_remote_host_qualified(self):
        proof = complete_worker_proof(case_id="STD-03")
        captured = {}

        def fake_run(argv, **kwargs):
            captured["argv"] = argv
            captured["env"] = kwargs["env"]
            return subprocess.CompletedProcess(argv, 0, stdout=json.dumps(proof) + "\n", stderr="")

        with mock.patch.object(standard.subprocess, "run", side_effect=fake_run):
            out = io.StringIO()
            with contextlib.redirect_stdout(out):
                rc = standard.worker_driver_main(["--driver", "/opt/afs-acceptance/drivers/fsx.py", "--", "--case-id", "STD-03"])
        self.assertEqual(rc, 0)
        self.assertEqual(captured["env"]["AFS_ACCEPTANCE_REMOTE_HOST_QUALIFIED"], "1")
        self.assertEqual(captured["argv"][-2:], ["--case-id", "STD-03"])
        self.assertEqual(json.loads(out.getvalue())["event"], "DRIVER")

    def test_host_mirrors_legacy_acceptance_environment_defaults(self):
        with tempfile.TemporaryDirectory() as tmp:
            run_dir = Path(tmp) / "env-run"
            old_env = {name: os.environ.get(name) for name in ("AFS_ACCEPTANCE_BACKEND", "AFS_ACCEPTANCE_PROFILE", "AFS_ACCEPTANCE_CASE_ID", "AFS_ACCEPTANCE_MATRIX", "AFS_ACCEPTANCE_RUN_DIR")}
            os.environ.update({
                "AFS_ACCEPTANCE_BACKEND": "DFS",
                "AFS_ACCEPTANCE_PROFILE": "full",
                "AFS_ACCEPTANCE_CASE_ID": "STD-01",
                "AFS_ACCEPTANCE_MATRIX": '{"backend":"DFS","meta":"memory"}',
                "AFS_ACCEPTANCE_RUN_DIR": str(run_dir),
            })
            original = standard.run_worker_json
            try:
                meta = {"event": "IDENTITY", "platform": {"system": "Linux"}, "mount": mount_event(), "base_mount": mount_event(), "processes": {"meta": strict_event("meta", "b" * 64, "boot-a", "machine-a")}}
                node = {"event": "IDENTITY", "platform": {"system": "Linux"}, "mount": mount_event(), "base_mount": mount_event(), "worker_run_dir": "/mnt/guest-ext4/std01", "worker_run_dir_mount": mount_event("/dev/vdb", "ext4", "/mnt/guest-ext4"), "processes": {"node": strict_event("node", "a" * 64, "boot-b", "machine-b")}}
                worker_proof = complete_worker_proof()
                def fake_worker(prefix, args, timeout):
                    if args[0] == "worker-pjdfstest":
                        self.assertEqual(args[args.index("--profile") + 1], "full")
                        self.assertEqual(args[args.index("--backend") + 1], "DFS")
                        self.assertEqual(json.loads(args[args.index("--matrix-json") + 1])["meta"], "memory")
                        return worker_record({"event": "PJDFSTEST", "proof": worker_proof})
                    return worker_record(meta if prefix == ["worker-a"] else node)
                standard.run_worker_json = fake_worker
                out = io.StringIO()
                with contextlib.redirect_stdout(out):
                    rc = standard.host_main([
                        "--worker-a-json", '["worker-a"]',
                        "--worker-b-json", '["worker-b"]',
                        "--worker-a-expected-process", "meta=20:" + "b" * 64,
                        "--worker-b-expected-process", "node=10:" + "a" * 64,
                        "--expected-meta-endpoint", "http://10.0.0.1:7400",
                        "--mount-b", "/mnt/dfs",
                        "--worker-run-dir-b", "/mnt/guest-ext4/std01",
                    ])
                proof = json.loads(out.getvalue())
                self.assertEqual(rc, 0)
                self.assertEqual(proof["profile"], "full")
                self.assertEqual(proof["matrix"]["meta"], "memory")
                self.assertTrue((run_dir / "artifacts/std-01-pjdfstest/proof.json").exists())
            finally:
                standard.run_worker_json = original
                for name, value in old_env.items():
                    if value is None:
                        os.environ.pop(name, None)
                    else:
                        os.environ[name] = value

    def test_host_blocks_pass_with_incomplete_worker_artifacts(self):
        with tempfile.TemporaryDirectory() as tmp:
            run_dir = Path(tmp)
            meta = {"event": "IDENTITY", "platform": {"system": "Linux"}, "mount": mount_event(), "base_mount": mount_event(), "processes": {"meta": strict_event("meta", "b" * 64, "boot-a", "machine-a")}}
            node = {"event": "IDENTITY", "platform": {"system": "Linux"}, "mount": mount_event(), "base_mount": mount_event(), "worker_run_dir": "/mnt/guest-ext4/std01", "worker_run_dir_mount": mount_event("/dev/vdb", "ext4", "/mnt/guest-ext4"), "processes": {"node": strict_event("node", "a" * 64, "boot-b", "machine-b")}}
            incomplete = complete_worker_proof(manifest_override=[{"path": "artifacts/std-01-pjdfstest/pjdfstest.stdout.tap", "bytes": 10, "sha256": "e" * 64}])
            def fake_worker(prefix, args, timeout):
                if args[0] == "worker-pjdfstest":
                    return worker_record({"event": "PJDFSTEST", "proof": incomplete})
                return worker_record(meta if prefix == ["worker-a"] else node)
            rc, proof = self.run_host_with_fake_worker(fake_worker, run_dir)
            self.assertEqual(rc, 1)
            self.assertEqual(proof["status"], "BLOCKED")
            artifact_check = next(check for check in proof["checks"] if check["name"] == "remote-worker-artifacts")
            self.assertEqual(artifact_check["status"], "BLOCKED")
            self.assertIn("identity.json", " ".join(artifact_check["evidence"]["missing_paths"]))

    def test_host_preflight_invalid_never_invokes_suite(self):
        with tempfile.TemporaryDirectory() as tmp:
            run_dir = Path(tmp)
            calls = []
            meta = {"event": "IDENTITY", "platform": {"system": "Linux"}, "mount": mount_event(), "base_mount": mount_event(), "processes": {"meta": strict_event("meta", "b" * 64, "boot-a", "machine-a")}}
            node = {"event": "IDENTITY", "platform": {"system": "Linux"}, "mount": mount_event(), "base_mount": mount_event(), "worker_run_dir": "/mnt/guest-ext4/std01", "worker_run_dir_mount": mount_event("/dev/vdb", "ext4", "/mnt/guest-ext4"), "processes": {"node": strict_event("node", "a" * 64, "boot-b", "machine-b", endpoint="http://10.0.0.9:7400")}}
            def fake_worker(prefix, args, timeout):
                calls.append(args[0])
                if args[0] == "worker-pjdfstest":
                    raise AssertionError("suite must not execute after failed preflight")
                return worker_record(meta if prefix == ["worker-a"] else node)
            rc, proof = self.run_host_with_fake_worker(fake_worker, run_dir)
            self.assertEqual(rc, 1)
            self.assertEqual(proof["status"], "BLOCKED")
            self.assertEqual(calls, ["worker-identity", "worker-identity"])
            self.assertFalse(proof["remote_identity"]["suite_invoked"])


if __name__ == "__main__":
    unittest.main()
