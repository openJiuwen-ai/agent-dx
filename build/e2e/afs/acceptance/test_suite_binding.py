import contextlib
import importlib.util
import io
import json
import subprocess
import sys
import tempfile
import unittest
from pathlib import Path
from unittest import mock


DRIVERS = Path(__file__).resolve().parent / "drivers"
SPEC = importlib.util.spec_from_file_location("suite_binding", DRIVERS / "suite_binding.py")
suite_binding = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(suite_binding)


def write_json(path: Path, value: object) -> None:
    path.write_text(json.dumps(value, indent=2, sort_keys=True), encoding="utf-8")


class SuiteBindingTests(unittest.TestCase):
    def remote_worker(self, role="meta", host="afs-a", pid=1234, ssh_config=None):
        return {
            "transport": "ssh",
            "host": host,
            **({"ssh_config": ssh_config} if ssh_config is not None else {}),
            "python": "/usr/bin/python3",
            "standard_driver": "/opt/afs-acceptance/drivers/standard.py",
            "expected_processes": [
                {
                    "role": role,
                    "pid": pid,
                    "sha256": "a" * 64 if role == "meta" else "b" * 64,
                }
            ],
        }

    def remote_binding(self, **extra):
        result = self.binding(
            case_id="STD-01",
            driver_matrix={"reference": "ext4", "suite_sha": "sanwan/pjdfstest current"},
            mode="remote-host",
            worker_a=self.remote_worker("meta", "afs-meta-a", 1234),
            worker_b=self.remote_worker("node", "afs-node-b", 2345),
            expected_meta_endpoint="https://10.0.0.10:17880",
            mount_b="/mnt/afs",
            base_dir_b="/mnt/afs/acceptance",
            suite_root_b="/opt/afs-acceptance/suites-reference/src/pjdfstest",
            worker_run_dir_b="/var/tmp/afs-acceptance/std01-worker",
            timeout=1800,
            command_timeout=3600,
        )
        result.pop("mount", None)
        result.pop("process_pid", None)
        result.pop("meta_process_pid", None)
        result.update(extra)
        return result

    def test_std01_remote_host_builds_fixed_standard_host_argv(self):
        binding = self.remote_binding()
        suite_binding.validate_binding(binding, "STD-01", "full", binding["matrix"])
        argv = suite_binding.build_child_argv(binding, "STD-01", "full", binding["matrix"], Path("/run/std01"))
        self.assertEqual(argv[:3], [sys.executable, str(DRIVERS / "standard.py"), "host"])
        self.assertEqual(json.loads(argv[argv.index("--worker-a-json") + 1]), ["ssh", "-o", "BatchMode=yes", "--", "afs-meta-a", "/usr/bin/python3", "/opt/afs-acceptance/drivers/standard.py"])
        self.assertEqual(json.loads(argv[argv.index("--worker-b-json") + 1]), ["ssh", "-o", "BatchMode=yes", "--", "afs-node-b", "/usr/bin/python3", "/opt/afs-acceptance/drivers/standard.py"])
        self.assertIn("--worker-a-expected-process", argv)
        self.assertIn("meta=1234:" + "a" * 64, argv)
        self.assertIn("--worker-b-expected-process", argv)
        self.assertIn("node=2345:" + "b" * 64, argv)
        self.assertEqual(argv[argv.index("--backend") + 1], "DFS")
        self.assertNotIn("--meta", argv)
        self.assertNotIn("--allow-same-worker", argv)

    def test_std03_remote_host_builds_generic_driver_argv(self):
        binding = self.remote_binding(
            case_id="STD-03",
            driver_matrix={"reference": "ext4", "suite_sha": "secfs.test current", "seeds": 3},
            suite_root_b="/opt/afs-acceptance/suites-reference/src/secfs.test",
            fsx_binary_b="/opt/afs-acceptance/suites-reference/src/secfs.test/tools/bin/fsx",
            worker_run_dir_b="/var/tmp/afs-acceptance/std03-worker",
        )
        suite_binding.validate_binding(binding, "STD-03", "full", binding["matrix"])
        argv = suite_binding.build_child_argv(binding, "STD-03", "full", binding["matrix"], Path("/run/std03"))
        self.assertEqual(argv[:3], [sys.executable, str(DRIVERS / "standard.py"), "host"])
        self.assertEqual(argv[argv.index("--worker-suite-event") + 1], "DRIVER")
        suite_args = json.loads(argv[argv.index("--worker-suite-args-json") + 1])
        self.assertEqual(suite_args[:3], ["worker-driver", "--driver", "/opt/afs-acceptance/drivers/fsx.py"])
        self.assertIn("--fsx-binary", suite_args)
        self.assertEqual(suite_args[suite_args.index("--fsx-binary") + 1], "/opt/afs-acceptance/suites-reference/src/secfs.test/tools/bin/fsx")
        self.assertEqual(suite_args[suite_args.index("--case-id") + 1], "STD-03")
        self.assertEqual(suite_args[suite_args.index("--meta") + 1], "etcd")
        self.assertEqual(suite_args[suite_args.index("--process-pid") + 1], "2345")
        self.assertNotIn("--meta-process-pid", suite_args)

    def test_std02_remote_host_rejects_local_suite_fields(self):
        binding = self.remote_binding(
            case_id="STD-02",
            driver_matrix={"reference": "ext4", "suite": "LTP 20260529"},
            ltp_install="/local/ltp",
            ltp_install_b="/opt/ltp",
            expanded_tsv_b="/opt/afs-acceptance/ltp.tsv",
            applicability_manifest_b="/opt/afs-acceptance/ltp-applicability.json",
        )
        with self.assertRaises(suite_binding.BindingError):
            suite_binding.validate_binding(binding, "STD-02", "full", binding["matrix"])

    def test_std04_remote_host_requires_remote_reference_dir(self):
        binding = self.remote_binding(case_id="STD-04", driver_matrix={"reference": "ext4", "seeds": 10, "operations_per_seed": 10000})
        with self.assertRaises(suite_binding.BindingError):
            suite_binding.validate_binding(binding, "STD-04", "full", binding["matrix"])

    def test_std01_remote_host_rejects_local_pid_fields(self):
        binding = self.remote_binding(process_pid=999)
        with self.assertRaises(suite_binding.BindingError):
            suite_binding.validate_binding(binding, "STD-01", "full", binding["matrix"])

    def test_std01_remote_host_rejects_ssh_option_host(self):
        binding = self.remote_binding(worker_a=self.remote_worker("meta", "-oProxyCommand=bad", 1234))
        with self.assertRaises(suite_binding.BindingError):
            suite_binding.validate_binding(binding, "STD-01", "full", binding["matrix"])

    def test_std01_remote_host_rejects_non_absolute_worker_script(self):
        worker = self.remote_worker("node", "afs-node-b", 2345)
        worker["standard_driver"] = "drivers/standard.py"
        binding = self.remote_binding(worker_b=worker)
        with self.assertRaises(suite_binding.BindingError):
            suite_binding.validate_binding(binding, "STD-01", "full", binding["matrix"])

    def test_std01_remote_host_allows_absolute_ssh_config(self):
        binding = self.remote_binding(worker_a=self.remote_worker("meta", "afs-meta-a", 1234, ssh_config="/etc/afs-acceptance/ssh_config"))
        suite_binding.validate_binding(binding, "STD-01", "full", binding["matrix"])
        argv = suite_binding.build_child_argv(binding, "STD-01", "full", binding["matrix"], Path("/run/std01"))
        self.assertEqual(json.loads(argv[argv.index("--worker-a-json") + 1]), ["ssh", "-o", "BatchMode=yes", "-F", "/etc/afs-acceptance/ssh_config", "--", "afs-meta-a", "/usr/bin/python3", "/opt/afs-acceptance/drivers/standard.py"])

    def test_std01_remote_host_rejects_relative_ssh_config(self):
        binding = self.remote_binding(worker_a=self.remote_worker("meta", "afs-meta-a", 1234, ssh_config="ssh_config"))
        with self.assertRaises(suite_binding.BindingError):
            suite_binding.validate_binding(binding, "STD-01", "full", binding["matrix"])

    def test_valid_remote_dispatch_uses_standard_host(self):
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            binding = self.remote_binding()
            write_json(root / "bindings.json", {"schema_version": 1, "bindings": [binding]})
            proof = self.proof(case_id="STD-01", matrix={**binding["matrix"], **binding["driver_matrix"]})
            proc = subprocess.CompletedProcess(["child"], 0, stdout=json.dumps(proof) + "\n", stderr="")
            code, out, _err, run_mock = self.run_main(self.env(root, case_id="STD-01"), proc=proc)
        self.assertEqual(code, 0)
        self.assertEqual(json.loads(out)["case_id"], "STD-01")
        argv = run_mock.call_args.args[0]
        self.assertEqual(argv[:3], [sys.executable, str(DRIVERS / "standard.py"), "host"])

    def test_all_fixed_argv_match_real_driver_parsers(self):
        sys.path.insert(0, str(DRIVERS))
        try:
            for case_id, config in suite_binding.CASE_CONFIG.items():
                with self.subTest(case_id=case_id):
                    spec = importlib.util.spec_from_file_location("binding_cli_" + case_id.replace("-", "_"), DRIVERS / config["driver"])
                    driver = importlib.util.module_from_spec(spec)
                    sys.modules[spec.name] = driver
                    spec.loader.exec_module(driver)
                    binding = self.binding(case_id=case_id)
                    if case_id == "STD-04":
                        binding["reference_dir"] = "/reference/ext4"
                    suite_binding.validate_binding(binding, case_id, "full", binding["matrix"])
                    argv = suite_binding.build_child_argv(binding, case_id, "full", binding["matrix"], Path("/run"))
                    args = driver.parse_args(argv[2:])
                    self.assertEqual(args.case_id, case_id)
                    self.assertEqual(args.profile, "full")
                    self.assertEqual(args.backend, "DFS")
                    self.assertEqual(args.meta, "etcd")
                    self.assertNotIn("--max-seeds", argv)
                    self.assertNotIn("--operations", argv)
        finally:
            sys.path.remove(str(DRIVERS))

    def test_fixture_bypass_fields_are_rejected(self):
        for field in ("allow_reference_fixture", "allow_nonroot_fixture", "allow_unpinned_suite_fixture", "max_tests", "max_seeds", "operations", "seeds", "command", "run_dir"):
            with self.subTest(field=field):
                binding = self.binding(**{field: True})
                with self.assertRaises(suite_binding.BindingError):
                    suite_binding.validate_binding(binding, "STD-03", "full", binding["matrix"])

    def test_child_proof_cannot_change_context(self):
        matrix = {"backend": "DFS", "meta": "etcd"}
        for field, value in (("case_id", "STD-01"), ("profile", "full"), ("matrix", {"backend": "OwnerFs", "meta": "etcd"})):
            with self.subTest(field=field):
                proof = self.proof()
                proof[field] = value
                self.assertIsNotNone(suite_binding.child_proof_error(proof, "STD-03", "smoke", matrix))

    def test_conflicting_inherited_backend_is_rejected(self):
        with self.assertRaises(suite_binding.BindingError):
            suite_binding.child_env({"AFS_ACCEPTANCE_BACKEND": "OwnerFs"}, {"backend": "DFS", "meta": "etcd"})

    def test_manifest_has_profile_specific_workload_counts(self):
        import runner
        manifest = json.loads((DRIVERS.parent / "cases.json").read_text())
        cases = {case["id"]: case for case in manifest["cases"]}
        for case_id, full_seeds in (("STD-03", 3), ("STD-04", 10)):
            smoke = runner.manifest_matrix_axes(cases[case_id], "smoke")
            full = runner.manifest_matrix_axes(cases[case_id], "full")
            self.assertEqual(smoke["seeds_smoke"], ["1"])
            self.assertNotIn("seeds_full", smoke)
            self.assertEqual(full["seeds_full"], [str(full_seeds)])
            self.assertNotIn("seeds_smoke", full)
        self.assertEqual(runner.manifest_matrix_axes(cases["STD-04"], "full")["operations_per_seed_full"], ["10000"])
        self.assertEqual(runner.manifest_matrix_axes(cases["STD-04"], "smoke")["operations_per_seed_smoke"], ["100"])

    def test_registered_suite_cases_do_not_claim_acceptance(self):
        manifest = json.loads((DRIVERS.parent / "cases.json").read_text())
        self.assertEqual(sum(case.get("active", False) for case in manifest["cases"]), 69)
        cases = {case["id"]: case for case in manifest["cases"]}
        for case_id in suite_binding.CASE_CONFIG:
            self.assertEqual(cases[case_id]["driver"]["state"], "READY")
            self.assertEqual(cases[case_id]["driver"]["command"], ["python3", "drivers/suite_binding.py"])
            self.assertEqual(cases[case_id]["status"], "NOT_RUN")
        # Accounting is now implemented; readiness still does not claim a suite pass.
        self.assertEqual(cases["STD-05"]["driver"]["state"], "READY")
        self.assertEqual(cases["STD-05"]["driver"]["command"], ["python3", "drivers/accounting.py"])
        self.assertEqual(cases["STD-05"]["status"], "NOT_RUN")

    def env(self, root: Path, *, case_id="STD-03", matrix=None):
        matrix = matrix or {"backend": "DFS", "meta": "etcd"}
        return {
            "AFS_ACCEPTANCE_CASE_ID": case_id,
            "AFS_ACCEPTANCE_PROFILE": "smoke",
            "AFS_ACCEPTANCE_MATRIX": json.dumps(matrix, sort_keys=True),
            "AFS_ACCEPTANCE_RUN_DIR": str(root / "run"),
            "AFS_ACCEPTANCE_SUITE_BINDINGS": str(root / "bindings.json"),
        }

    def binding(self, *, case_id="STD-03", matrix=None, **extra):
        result = {
            "case_id": case_id,
            "matrix": matrix or {"backend": "DFS", "meta": "etcd"},
            "mount": "/mnt/afs",
            "process_pid": 123,
            "meta_process_pid": 456,
        }
        result.update(extra)
        return result

    def proof(self, *, case_id="STD-03", matrix=None, status="PASS"):
        return {
            "case_id": case_id,
            "profile": "smoke",
            "matrix": matrix or {"backend": "DFS", "meta": "etcd"},
            "status": status,
            "checks": [{"name": "ok", "status": status, "evidence": "child"}],
        }

    def run_main(self, env, proc=None):
        out = io.StringIO()
        err = io.StringIO()
        if proc is None:
            proc = subprocess.CompletedProcess(["child"], 0, stdout=json.dumps(self.proof()) + "\n", stderr="raw err\n")
        with mock.patch.dict("os.environ", env, clear=True), mock.patch.object(suite_binding.subprocess, "run", return_value=proc) as run_mock:
            with contextlib.redirect_stdout(out), contextlib.redirect_stderr(err):
                code = suite_binding.main()
        return code, out.getvalue(), err.getvalue(), run_mock

    def test_valid_dispatch_forwards_child_proof_and_builds_fixed_argv(self):
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            write_json(root / "bindings.json", {"schema_version": 1, "bindings": [self.binding(driver_matrix={"reference": "ext4", "suite_sha": "secfs.test current", "seeds": 3}, suite_root="/s/secfs", fsx_binary="/s/secfs/tools/bin/fsx")]})
            code, out, err, run_mock = self.run_main(self.env(root))
        self.assertEqual(code, 0)
        self.assertEqual(json.loads(out.splitlines()[-1])["status"], "PASS")
        self.assertEqual(err, "raw err\n")
        argv = run_mock.call_args.args[0]
        self.assertEqual(argv[:2], [sys.executable, str(DRIVERS / "fsx.py")])
        self.assertIn("--fsx-binary", argv)
        child_matrix = json.loads(argv[argv.index("--matrix-json") + 1])
        self.assertEqual(child_matrix["backend"], "DFS")
        self.assertEqual(child_matrix["meta"], "etcd")
        self.assertEqual(child_matrix["seeds"], 3)

    def test_missing_binding_blocks_without_child_process(self):
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            write_json(root / "bindings.json", {"schema_version": 1, "bindings": []})
            code, out, _err, run_mock = self.run_main(self.env(root))
        self.assertEqual(code, 1)
        self.assertFalse(run_mock.called)
        proof = json.loads(out)
        self.assertEqual(proof["status"], "BLOCKED")
        self.assertIn("no binding", proof["reason"])

    def test_ambiguous_binding_blocks(self):
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            write_json(root / "bindings.json", {"schema_version": 1, "bindings": [self.binding(), self.binding()]})
            code, out, _err, run_mock = self.run_main(self.env(root))
        self.assertEqual(code, 1)
        self.assertFalse(run_mock.called)
        self.assertIn("ambiguous", json.loads(out)["reason"])

    def test_driver_matrix_cannot_override_runner_axes(self):
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            write_json(root / "bindings.json", {"schema_version": 1, "bindings": [self.binding(driver_matrix={"backend": "OwnerFs"})]})
            code, out, _err, run_mock = self.run_main(self.env(root))
        self.assertEqual(code, 1)
        self.assertFalse(run_mock.called)
        self.assertIn("may not override", json.loads(out)["reason"])

    def test_matrix_mismatch_blocks_as_missing(self):
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            write_json(root / "bindings.json", {"schema_version": 1, "bindings": [self.binding(matrix={"backend": "OwnerFs", "meta": "etcd"})]})
            code, out, _err, run_mock = self.run_main(self.env(root))
        self.assertEqual(code, 1)
        self.assertFalse(run_mock.called)
        self.assertIn("no binding", json.loads(out)["reason"])

    def test_child_nonzero_with_real_proof_is_forwarded(self):
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            write_json(root / "bindings.json", {"schema_version": 1, "bindings": [self.binding()]})
            proc = subprocess.CompletedProcess(["child"], 7, stdout="log\n" + json.dumps(self.proof(status="FAIL")) + "\n", stderr="bad\n")
            code, out, err, run_mock = self.run_main(self.env(root), proc=proc)
        self.assertEqual(code, 7)
        self.assertTrue(run_mock.called)
        self.assertEqual(err, "bad\n")
        self.assertEqual(json.loads(out.splitlines()[-1])["status"], "FAIL")

    def test_child_without_proof_blocks_and_preserves_raw_output(self):
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            write_json(root / "bindings.json", {"schema_version": 1, "bindings": [self.binding()]})
            proc = subprocess.CompletedProcess(["child"], 0, stdout="plain child output\n", stderr="child stderr\n")
            code, out, err, _run_mock = self.run_main(self.env(root), proc=proc)
        self.assertEqual(code, 1)
        self.assertEqual(err, "child stderr\n")
        self.assertIn("plain child output", out)
        self.assertEqual(json.loads(out.splitlines()[-1])["status"], "BLOCKED")
        self.assertIn("did not emit", json.loads(out.splitlines()[-1])["reason"])

    def test_unsupported_command_field_blocks(self):
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            write_json(root / "bindings.json", {"schema_version": 1, "bindings": [self.binding(command=["python3", "anything.py"])]})
            code, out, _err, run_mock = self.run_main(self.env(root))
        self.assertEqual(code, 1)
        self.assertFalse(run_mock.called)
        self.assertIn("unsupported binding field", json.loads(out)["reason"])


if __name__ == "__main__":
    unittest.main()
