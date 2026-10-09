#!/usr/bin/env python3
import hashlib
import importlib.util
import json
import os
import platform
import subprocess
import sys
import tempfile
import textwrap
import unittest
from unittest import mock
from pathlib import Path

DRIVER = Path(__file__).resolve().parent / "drivers" / "ltp.py"


def load_driver_module():
    sys.path.insert(0, str(DRIVER.parent))
    spec = importlib.util.spec_from_file_location("ltp_driver_under_test", DRIVER)
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


def sha256_file(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for chunk in iter(lambda: handle.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def write_fake_ltp(root: Path, outcomes: dict[str, object]) -> tuple[Path, Path, Path]:
    suite = root / "suite"
    install = root / "install"
    bin_dir = install / "testcases" / "bin"
    suite.mkdir()
    install.mkdir()
    bin_dir.mkdir(parents=True)
    subprocess.run(["git", "init", "-q", str(suite)], check=True)
    subprocess.run(["git", "-C", str(suite), "config", "user.email", "test@example.invalid"], check=True)
    subprocess.run(["git", "-C", str(suite), "config", "user.name", "test"], check=True)
    (suite / "README").write_text("fake ltp\n", encoding="utf-8")
    subprocess.run(["git", "-C", str(suite), "add", "README"], check=True)
    subprocess.run(["git", "-C", str(suite), "commit", "-q", "-m", "fake"], check=True)
    for test_id in outcomes:
        path = bin_dir / test_id
        path.write_text("#!/bin/sh\nexit 0\n", encoding="utf-8")
        path.chmod(0o755)
    kirk = install / "kirk"
    kirk.write_text(
        textwrap.dedent(
            f"""\
            #!/usr/bin/env python3
            import json, pathlib, sys, time
            outcomes = {outcomes!r}
            if '--version' in sys.argv:
                print('kirk, fake')
                raise SystemExit(0)
            report = pathlib.Path(sys.argv[sys.argv.index('--json-report') + 1])
            command = sys.argv[sys.argv.index('--run-command') + 1]
            test_id = command.split()[-1]
            outcome = outcomes.get(test_id, 'PASS')
            if isinstance(outcome, dict):
                result = outcome.get('result', 'PASS')
                lines = outcome.get('lines', [])
            else:
                result = outcome
                lines = []
            report.parent.mkdir(parents=True, exist_ok=True)
            report.write_text(json.dumps({{'results': [{{'test': test_id, 'status': result}}]}}))
            if result == 'SLEEP':
                time.sleep(10)
            elif lines:
                for line in lines:
                    print(line)
                raise SystemExit({{'PASS': 0, 'TCONF': 32, 'TBROK': 2, 'FAIL': 1}}.get(result, 1))
            elif result == 'PASS':
                print('TPASS: ' + test_id)
                raise SystemExit(0)
            elif result == 'TCONF':
                print('TCONF: ' + test_id)
                raise SystemExit(32)
            elif result == 'TBROK':
                print('TBROK: ' + test_id)
                raise SystemExit(2)
            else:
                print('TFAIL: ' + test_id)
                raise SystemExit(1)
            """
        ),
        encoding="utf-8",
    )
    kirk.chmod(0o755)
    tsv = root / "ltp-filesystem-expanded.tsv"
    with tsv.open("w", encoding="utf-8") as handle:
        handle.write("selector\ttest_id\tcommand\n")
        for test_id in outcomes:
            handle.write(f"fs\t{test_id}\t{test_id} {test_id}\n")
    return suite, install, tsv


class LtpDriverPureTests(unittest.TestCase):
    def timeout_identity(self) -> dict[str, object]:
        return {
            "suite": {"git_head": "suite-a", "expanded_tsv_sha256": "tsv-a"},
            "platform": {"machine": "aarch64", "release": "6.8.0-142-generic"},
        }

    def write_timeout_policy(self, root: Path, body: dict[str, object]) -> Path:
        policy = root / "timeout-policy.json"
        policy.write_text(json.dumps(body), encoding="utf-8")
        return policy

    def test_git_identity_trust_is_scoped_to_selected_suite_root(self):
        driver = load_driver_module()
        with tempfile.TemporaryDirectory() as td:
            suite = Path(td) / "ltp-src"
            install = Path(td) / "ltp-install"
            expanded = Path(td) / "ltp-filesystem-expanded.tsv"
            suite.mkdir()
            install.mkdir()
            (install / "kirk").write_text("#!/bin/sh\n", encoding="utf-8")
            (install / "kirk").chmod(0o755)
            expanded.write_text("selector\ttest_id\tcommand\n", encoding="utf-8")
            root = suite.resolve()
            responses = [
                {"returncode": 0, "stdout": "3a64\n", "stderr": ""},
                {"returncode": 0, "stdout": "", "stderr": ""},
                {"returncode": 0, "stdout": "kirk\n", "stderr": ""},
            ]
            with mock.patch.object(driver, "run_text", side_effect=responses) as run:
                identity = driver.ltp_identity(suite, install, expanded)
            git = ["git", "-c", f"safe.directory={root}", "-C", str(root)]
            self.assertEqual(run.call_args_list[0], mock.call(git + ["rev-parse", "HEAD"]))
            self.assertEqual(run.call_args_list[1], mock.call(git + ["status", "--porcelain"]))
            self.assertEqual(identity["git_head"], "3a64")

    def test_extracts_result_lines_for_subtest_accounting(self):
        driver = load_driver_module()
        events = driver.extract_ltp_events(
            "rename11    1  TPASS  :  failed as expected\n"
            "fallocate04.c:92: TCONF: fallocate() not supported\n"
            "prot_hsymlinks    2  TBROK  :  Remaining cases broken\n"
        )
        self.assertEqual([event["status"] for event in events], ["TPASS", "TCONF", "TBROK"])
        self.assertEqual(events[0]["case_index"], 1)
        self.assertIsNone(events[1]["case_index"])
        self.assertEqual(events[2]["case_index"], 2)

    def test_extracts_source_file_line_from_ltp_events(self):
        driver = load_driver_module()
        events = driver.extract_ltp_events("/tmp/src/openat02.c:151: TBROK: write failed: EFBIG\n")
        self.assertEqual(events[0]["source"], "openat02.c:151")

    def test_extracts_source_file_line_through_relative_suite_path(self):
        driver = load_driver_module()
        events = driver.extract_ltp_events(
            "/mnt/ltp/testcases/kernel/syscalls/chown/../utils/compat_tst_16.h:153: "
            "TCONF: 16-bit version of chown() is not supported on your platform\n"
        )
        self.assertEqual(events[0]["source"], "compat_tst_16.h:153")

    def test_short_fixture_paths_stay_within_legacy_ltp_budget(self):
        driver = load_driver_module()
        base = Path("/mnt/afs/base")
        fixture = base / driver.short_fixture_name()
        tmp_dir = driver.command_case_dir(fixture, 488) / "t"
        self.assertTrue(driver.ltp_path_budget_ok(tmp_dir), str(tmp_dir))
        self.assertLessEqual(len(str(tmp_dir)), driver.MAX_LEGACY_LTP_TMPDIR_LEN)

    def test_applicability_manifest_binding_mismatch_is_rejected(self):
        driver = load_driver_module()
        identity = {
            "suite": {"git_head": "suite-a", "expanded_tsv_sha256": "tsv-a"},
            "platform": {"machine": "aarch64", "release": "6.8.0-142-generic"},
        }
        with tempfile.TemporaryDirectory(prefix="afs-ltp-manifest-") as tmp:
            manifest = Path(tmp) / "manifest.json"
            manifest.write_text(
                json.dumps({
                    "binding": {
                        "suite_revision": "suite-a",
                        "expanded_tsv_sha256": "tsv-a",
                        "machine": "x86_64",
                        "kernel_release": "6.8.0-142-generic",
                    },
                    "entries": [{
                        "test_id": "t001",
                        "event_match": {"status": "TCONF", "source": "feature.c:10", "message_regex": "missing feature"},
                        "scope": "kernel-feature",
                        "disposition": "pre_reviewed_not_applicable",
                        "rationale": "synthetic event",
                    }],
                }),
                encoding="utf-8",
            )
            with self.assertRaisesRegex(RuntimeError, "machine mismatch"):
                driver.load_applicability_manifest(manifest, identity)

    def test_applicability_manifest_raw_command_count_mismatch_is_rejected(self):
        driver = load_driver_module()
        identity = {
            "suite": {"git_head": "suite-a", "expanded_tsv_sha256": "tsv-a"},
            "platform": {"machine": "aarch64", "release": "6.8.0-142-generic"},
            "selection": {"mode": "full", "total_commands": 657, "selected_count": 2},
        }
        with tempfile.TemporaryDirectory(prefix="afs-ltp-manifest-") as tmp:
            manifest = Path(tmp) / "manifest.json"
            manifest.write_text(
                json.dumps({
                    "binding": {
                        "suite_revision": "suite-a",
                        "expanded_tsv_sha256": "tsv-a",
                        "machine": "aarch64",
                        "kernel_release": "6.8.0-142-generic",
                    },
                    "policy": {"raw_commands_required": 657},
                    "entries": [{
                        "test_id": "t001",
                        "event_match": {"status": "TCONF", "source": "feature.c:10", "message_regex": "missing feature"},
                        "scope": "kernel-feature",
                        "disposition": "pre_reviewed_not_applicable",
                        "rationale": "synthetic event",
                    }],
                }),
                encoding="utf-8",
            )
            with self.assertRaisesRegex(RuntimeError, "raw_commands_required mismatch"):
                driver.load_applicability_manifest(manifest, identity)

    def test_tfail_remains_blocking_even_with_applicability_enabled(self):
        driver = load_driver_module()
        app = driver.apply_applicability(
            [{
                "index": 1,
                "test_id": "t001",
                "ltp_event_counts": {"TPASS": 0, "TFAIL": 1},
                "ltp_events": [{"line": 1, "status": "TFAIL", "source": "feature.c:10", "message": "actual failure", "raw": "feature.c:10: TFAIL: actual failure"}],
            }],
            {"path": "manifest.json", "sha256": "synthetic", "binding": {}, "entries": []},
        )
        self.assertEqual(app["status"], "BLOCKED")
        self.assertEqual(app["unmatched_event_count"], 1)
        self.assertIn("TFAIL events are never pre-reviewable", app["errors"])

    def test_reference_only_scope_cannot_waive_product_backend_tconf(self):
        driver = load_driver_module()
        manifest = {
            "path": "manifest.json",
            "sha256": "synthetic",
            "binding": {},
            "policy": {"reference_only_scopes": ["alternateFS-check"]},
            "entries": [{
                "id": "fallocate04:tconf",
                "test_id": "fallocate04",
                "status": "TCONF",
                "source": "fallocate04.c:92",
                "message_regex": "fallocate\\(\\) not supported",
                "scope": "alternateFS-check",
                "disposition": "pre_reviewed_not_applicable",
                "rationale": "reference ext4 subcase context only",
                "ordinary_subtests_required": True,
                "ordinary_coverage_link": None,
            }],
        }
        command_records = [{
            "index": 1,
            "test_id": "fallocate04",
            "ltp_event_counts": {"TPASS": 1, "TCONF": 1},
            "ltp_events": [
                {"line": 1, "status": "TPASS", "source": "fallocate04.c:80", "message": "ordinary path ok", "raw": "fallocate04 1 TPASS: ordinary path ok"},
                {"line": 2, "status": "TCONF", "source": "fallocate04.c:92", "message": "fallocate() not supported", "raw": "fallocate04.c:92: TCONF: fallocate() not supported"},
            ],
        }]
        reference_app = driver.apply_applicability(command_records, manifest, {"product": {"backend": "reference"}})
        self.assertEqual(reference_app["status"], "PASS", reference_app)
        self.assertEqual(reference_app["pre_reviewed_event_count"], 1)
        product_app = driver.apply_applicability(command_records, manifest, {"product": {"backend": "dfs"}})
        self.assertEqual(product_app["status"], "BLOCKED")
        self.assertEqual(product_app["pre_reviewed_event_count"], 0)
        self.assertEqual(product_app["target_context_failure_count"], 1)
        self.assertIn("pre-reviewed reference filesystem events cannot waive product backend results", product_app["errors"])

    def test_reference_only_missing_events_do_not_block_product_backend_pass(self):
        driver = load_driver_module()
        manifest = {
            "path": "manifest.json",
            "sha256": "synthetic",
            "binding": {},
            "policy": {"reference_only_scopes": ["alternateFS-check"]},
            "entries": [{
                "id": "fallocate04:tconf",
                "test_id": "fallocate04",
                "status": "TCONF",
                "source": "fallocate04.c:92",
                "message_regex": "fallocate\\(\\) not supported",
                "scope": "alternateFS-check",
                "disposition": "pre_reviewed_not_applicable",
                "rationale": "reference ext4 subcase context only",
                "ordinary_subtests_required": True,
                "ordinary_coverage_link": None,
            }],
        }
        app = driver.apply_applicability(
            [{"index": 1, "test_id": "fallocate04", "ltp_event_counts": {"TPASS": 1}, "ltp_events": []}],
            manifest,
            {"product": {"backend": "ownerfs"}},
        )
        self.assertEqual(app["status"], "PASS", app)
        self.assertEqual(app["missing_entry_count"], 0)

    def test_cross_target_scope_still_requires_expected_product_event(self):
        driver = load_driver_module()
        manifest = {
            "path": "manifest.json",
            "sha256": "synthetic",
            "binding": {},
            "policy": {"reference_only_scopes": ["alternateFS-check"]},
            "entries": [{
                "id": "fcntl40:tconf",
                "test_id": "fcntl40",
                "status": "TCONF",
                "source": "tst_test.c:1080",
                "message_regex": "requires kernel 6\\.12",
                "scope": "irrelevantkernel",
                "disposition": "pre_reviewed_not_applicable",
                "rationale": "kernel version is independent of filesystem backend",
                "ordinary_subtests_required": False,
                "ordinary_coverage_link": None,
            }],
        }
        app = driver.apply_applicability(
            [{"index": 1, "test_id": "fcntl40", "ltp_event_counts": {"TPASS": 1}, "ltp_events": []}],
            manifest,
            {"product": {"backend": "dfs"}},
        )
        self.assertEqual(app["status"], "BLOCKED")
        self.assertEqual(app["missing_entry_count"], 1)

    def test_timeout_policy_accepts_bound_positive_overrides_and_defaults(self):
        driver = load_driver_module()
        identity = self.timeout_identity()
        selected = [{"test_id": "gf16"}, {"test_id": "plain01"}]
        with tempfile.TemporaryDirectory(prefix="afs-ltp-timeout-policy-") as tmp:
            policy_path = self.write_timeout_policy(
                Path(tmp),
                {
                    "schema": 1,
                    "binding": {
                        "suite_revision": "suite-a",
                        "expanded_tsv_sha256": "tsv-a",
                        "machine": "aarch64",
                        "kernel_release": "6.8.0-142-generic",
                    },
                    "timeouts": [{"test_id": "gf16", "exec_timeout_seconds": 180, "suite_timeout_seconds": 240}],
                },
            )
            policy = driver.load_timeout_policy(policy_path, identity, selected, 120)
            self.assertEqual(policy["sha256"], hashlib.sha256(policy_path.read_bytes()).hexdigest())
            self.assertEqual(policy["size_bytes"], policy_path.stat().st_size)
        self.assertEqual(driver.command_timeouts("gf16", 120, policy), {"exec_timeout_seconds": 180, "suite_timeout_seconds": 240, "source": "timeout-policy"})
        self.assertEqual(driver.command_timeouts("plain01", 120, policy), {"exec_timeout_seconds": 120, "suite_timeout_seconds": 240, "source": "default"})

    def test_timeout_policy_rejects_binding_mismatch_duplicate_and_unknown_ids(self):
        driver = load_driver_module()
        identity = self.timeout_identity()
        selected = [{"test_id": "gf16"}]
        with tempfile.TemporaryDirectory(prefix="afs-ltp-timeout-policy-") as tmp:
            policy_path = self.write_timeout_policy(
                Path(tmp),
                {
                    "schema": 1,
                    "binding": {
                        "suite_revision": "suite-a",
                        "expanded_tsv_sha256": "tsv-a",
                        "machine": "x86_64",
                        "kernel_release": "6.8.0-142-generic",
                    },
                    "timeouts": [
                        {"test_id": "gf16", "exec_timeout_seconds": 180, "suite_timeout_seconds": 240},
                        {"test_id": "gf16", "exec_timeout_seconds": 181, "suite_timeout_seconds": 241},
                        {"test_id": "missing01", "exec_timeout_seconds": 180, "suite_timeout_seconds": 240},
                    ],
                },
            )
            with self.assertRaisesRegex(RuntimeError, "machine mismatch.*duplicate timeout policy test_id 'gf16'.*unknown timeout policy test_id 'missing01'"):
                driver.load_timeout_policy(policy_path, identity, selected, 120)

    def test_timeout_policy_rejects_unbounded_or_non_integer_budgets(self):
        driver = load_driver_module()
        identity = self.timeout_identity()
        selected = [{"test_id": "gf16"}, {"test_id": "gf17"}, {"test_id": "iogen01"}, {"test_id": "writev03"}]
        with tempfile.TemporaryDirectory(prefix="afs-ltp-timeout-policy-") as tmp:
            policy_path = self.write_timeout_policy(
                Path(tmp),
                {
                    "schema": 1,
                    "binding": {
                        "suite_revision": "suite-a",
                        "expanded_tsv_sha256": "tsv-a",
                        "machine": "aarch64",
                        "kernel_release": "6.8.0-142-generic",
                    },
                    "timeouts": [
                        {"test_id": "gf16", "exec_timeout_seconds": 119, "suite_timeout_seconds": 240},
                        {"test_id": "gf17", "exec_timeout_seconds": 180, "suite_timeout_seconds": 239},
                        {"test_id": "iogen01", "exec_timeout_seconds": 180, "suite_timeout_seconds": 180},
                        {"test_id": "writev03", "exec_timeout_seconds": True, "suite_timeout_seconds": 1320},
                    ],
                },
            )
            with self.assertRaisesRegex(RuntimeError, "below per-test baseline 120.*below baseline 240.*must be greater.*positive integer"):
                driver.load_timeout_policy(policy_path, identity, selected, 120)

    def test_timeout_policy_rejects_duplicate_json_object_keys(self):
        driver = load_driver_module()
        identity = self.timeout_identity()
        selected = [{"test_id": "gf16"}]
        with tempfile.TemporaryDirectory(prefix="afs-ltp-timeout-policy-") as tmp:
            policy_path = Path(tmp) / "timeout-policy.json"
            policy_path.write_text(
                '{"schema":1,"binding":{"suite_revision":"suite-a","suite_revision":"suite-b","expanded_tsv_sha256":"tsv-a","machine":"aarch64","kernel_release":"6.8.0-142-generic"},"timeouts":[]}',
                encoding="utf-8",
            )
            with self.assertRaisesRegex(RuntimeError, "duplicate JSON object key 'suite_revision'"):
                driver.load_timeout_policy(policy_path, identity, selected, 120)


class LtpDriverTests(unittest.TestCase):
    def setUp(self):
        if platform.system() != "Linux":
            self.skipTest("LTP driver self-tests run in Linux so findmnt/proc semantics match acceptance")

    def run_driver(self, outcomes: dict[str, object], *, profile="full", max_tests: int | None = None, timeout=2, manifest_entries: list[dict] | None = None, timeout_entries: list[dict] | None = None, backend: str = "reference"):
        root = Path(tempfile.mkdtemp(prefix="afs-ltp-driver-test-"))
        self.addCleanup(lambda: subprocess.run(["rm", "-rf", str(root)], check=False))
        suite, install, tsv = write_fake_ltp(root, outcomes)
        mount = root / "mount"
        base = mount / "base"
        base.mkdir(parents=True)
        run_dir = root / "run"
        matrix = {"reference": "ext4", "suite": "LTP 20260529"}
        manifest_path = None
        timeout_policy_path = None
        if manifest_entries is not None:
            suite_head = subprocess.run(["git", "-C", str(suite), "rev-parse", "HEAD"], check=True, text=True, stdout=subprocess.PIPE).stdout.strip()
            manifest = {
                "binding": {
                    "suite_revision": suite_head,
                    "expanded_tsv_sha256": sha256_file(tsv),
                    "machine": platform.machine(),
                    "kernel_release": platform.release(),
                },
                "entries": manifest_entries,
            }
            manifest_path = root / "applicability-manifest.json"
            manifest_path.write_text(json.dumps(manifest), encoding="utf-8")
        if timeout_entries is not None:
            suite_head = subprocess.run(["git", "-C", str(suite), "rev-parse", "HEAD"], check=True, text=True, stdout=subprocess.PIPE).stdout.strip()
            timeout_policy = {
                "schema": 1,
                "binding": {
                    "suite_revision": suite_head,
                    "expanded_tsv_sha256": sha256_file(tsv),
                    "machine": platform.machine(),
                    "kernel_release": platform.release(),
                },
                "timeouts": timeout_entries,
            }
            timeout_policy_path = root / "timeout-policy.json"
            timeout_policy_path.write_text(json.dumps(timeout_policy), encoding="utf-8")
        argv = [
            sys.executable,
            str(DRIVER),
            "--profile",
            profile,
            "--matrix-json",
            json.dumps(matrix),
            "--run-dir",
            str(run_dir),
            "--mount",
            str(mount),
            "--base-dir",
            str(base),
            "--suite-root",
            str(suite),
            "--ltp-install",
            str(install),
            "--expanded-tsv",
            str(tsv),
            "--per-test-timeout",
            str(timeout),
            "--backend",
            backend,
            "--meta",
            "memory",
            "--allow-nonroot-fixture",
            "--allow-unpinned-suite-fixture",
        ]
        if max_tests is not None:
            argv.extend(["--max-tests", str(max_tests)])
        if manifest_path is not None:
            argv.extend(["--applicability-manifest", str(manifest_path)])
        if timeout_policy_path is not None:
            argv.extend(["--timeout-policy", str(timeout_policy_path)])
        env = os.environ.copy()
        proc = subprocess.run(argv, shell=False, text=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE, env=env, check=False)
        lines = [line for line in proc.stdout.splitlines() if line.strip()]
        self.assertTrue(lines, proc.stderr)
        return proc, json.loads(lines[-1]), run_dir

    def test_all_pass_can_pass_smoke(self):
        proc, proof, run_dir = self.run_driver({"open01": "PASS", "read01": "PASS", "write01": "PASS", "stat01": "PASS", "chmod01": "PASS", "fcntl14": "PASS"}, profile="smoke")
        self.assertEqual(proc.returncode, 0, proof)
        self.assertEqual(proof["status"], "PASS")
        self.assertEqual(proof["accounting"]["result_counts"]["PASS"], 6)
        target_check = next(check for check in proof["checks"] if check["name"] == "target-identity")
        self.assertEqual(target_check["status"], "PASS")
        commands = run_dir / "artifacts" / "std-02-ltp" / "commands.json"
        self.assertTrue(commands.exists())

    def test_fake_product_label_on_ext4_is_blocked(self):
        proc, proof, _run_dir = self.run_driver({"open01": "PASS", "read01": "PASS", "write01": "PASS", "stat01": "PASS", "chmod01": "PASS", "fcntl14": "PASS"}, profile="smoke", backend="dfs")
        self.assertNotEqual(proc.returncode, 0)
        self.assertEqual(proof["status"], "BLOCKED")
        target_check = next(check for check in proof["checks"] if check["name"] == "target-identity")
        self.assertEqual(target_check["status"], "BLOCKED")
        self.assertFalse(target_check["evidence"]["target_checks"]["observed-target-backend"])
        self.assertFalse(target_check["evidence"]["target_checks"]["product-process-identity"])

    def test_full_cap_is_blocked_not_pass(self):
        outcomes = {f"t{i:03d}": "PASS" for i in range(657)}
        proc, proof, _run_dir = self.run_driver(outcomes, profile="full", max_tests=2)
        self.assertNotEqual(proc.returncode, 0)
        self.assertEqual(proof["status"], "BLOCKED")
        self.assertIn("capped", proof["reason"])
        self.assertEqual(proof["accounting"]["incomplete"], 655)

    def test_failure_is_fail_and_keeps_raw_output(self):
        proc, proof, run_dir = self.run_driver({f"t{i:03d}": ("FAIL" if i == 1 else "PASS") for i in range(657)}, profile="full")
        self.assertNotEqual(proc.returncode, 0)
        self.assertEqual(proof["status"], "FAIL")
        self.assertIn("FAIL", proof["reason"])
        command_logs = list((run_dir / "artifacts" / "std-02-ltp" / "commands").glob("*/stdout.log"))
        self.assertTrue(command_logs)
        self.assertTrue(any("TFAIL" in path.read_text(errors="replace") for path in command_logs))

    def test_tconf_blocks_uncurated_acceptance(self):
        proc, proof, _run_dir = self.run_driver({f"t{i:03d}": ("TCONF" if i == 1 else "PASS") for i in range(657)}, profile="full")
        self.assertNotEqual(proc.returncode, 0)
        self.assertEqual(proof["status"], "BLOCKED")
        self.assertIn("TCONF", proof["reason"])
        self.assertEqual(proof["accounting"]["result_counts"]["TCONF"], 1)

    def test_applicability_manifest_pre_reviews_tconf_without_changing_raw_counts(self):
        outcomes = {f"t{i:03d}": "PASS" for i in range(657)}
        outcomes["t001"] = {"result": "TCONF", "lines": ["feature.c:10: TCONF: optional feature unavailable"]}
        entry = {
            "test_id": "t001",
            "event_match": {"status": "TCONF", "source": "feature.c:10", "message_regex": "optional feature unavailable"},
            "scope": "kernel-feature",
            "disposition": "pre_reviewed_not_applicable",
            "rationale": "synthetic feature is outside the required ordinary path",
        }
        proc, proof, run_dir = self.run_driver(outcomes, profile="full", manifest_entries=[entry])
        self.assertEqual(proc.returncode, 0, proof)
        self.assertEqual(proof["status"], "PASS")
        self.assertEqual(proof["accounting"]["result_counts"]["TCONF"], 1)
        self.assertEqual(proof["accounting"]["pre_reviewed_events"], 1)
        self.assertEqual(proof["applicability"]["status"], "PASS")
        commands = json.loads((run_dir / "artifacts" / "std-02-ltp" / "commands.json").read_text())
        self.assertEqual(commands[1]["result"], "TCONF")

    def test_applicability_manifest_fails_closed_on_unmatched_event(self):
        outcomes = {f"t{i:03d}": "PASS" for i in range(657)}
        outcomes["t001"] = {"result": "TCONF", "lines": ["feature.c:10: TCONF: optional feature unavailable"]}
        entry = {
            "test_id": "t001",
            "event_match": {"status": "TCONF", "source": "feature.c:11", "message_regex": "optional feature unavailable"},
            "scope": "kernel-feature",
            "disposition": "pre_reviewed_not_applicable",
            "rationale": "wrong source line should not match",
        }
        proc, proof, _run_dir = self.run_driver(outcomes, profile="full", manifest_entries=[entry])
        self.assertNotEqual(proc.returncode, 0)
        self.assertEqual(proof["status"], "BLOCKED")
        self.assertEqual(proof["applicability"]["unmatched_event_count"], 1)
        self.assertEqual(proof["applicability"]["missing_entry_count"], 1)

    def test_applicability_manifest_requires_ordinary_tpass_when_requested(self):
        outcomes = {f"t{i:03d}": "PASS" for i in range(657)}
        outcomes["t001"] = {"result": "TBROK", "lines": ["feature.c:12: TBROK: alternate fs broke"]}
        entry = {
            "test_id": "t001",
            "event_match": {"status": "TBROK", "source": "feature.c:12", "message_regex": "alternate fs broke"},
            "scope": "alternate-fs-subcase",
            "disposition": "pre_reviewed_not_applicable",
            "ordinary_subtests_required": True,
            "rationale": "ordinary checks must still be present",
        }
        proc, proof, _run_dir = self.run_driver(outcomes, profile="full", manifest_entries=[entry])
        self.assertNotEqual(proc.returncode, 0)
        self.assertEqual(proof["status"], "BLOCKED")
        self.assertEqual(proof["applicability"]["ordinary_coverage_failure_count"], 1)
        outcomes["t001"] = {"result": "TBROK", "lines": ["t001 1 TPASS: ordinary path ok", "feature.c:12: TBROK: alternate fs broke"]}
        proc, proof, _run_dir = self.run_driver(outcomes, profile="full", manifest_entries=[entry])
        self.assertEqual(proc.returncode, 0, proof)
        self.assertEqual(proof["status"], "PASS")

    def test_applicability_manifest_fails_closed_when_expected_event_disappears(self):
        outcomes = {f"t{i:03d}": "PASS" for i in range(657)}
        entry = {
            "test_id": "t001",
            "event_match": {"status": "TCONF", "source": "feature.c:10", "message_regex": "optional feature unavailable"},
            "scope": "kernel-feature",
            "disposition": "pre_reviewed_not_applicable",
            "rationale": "disappearing expected events must be reviewed",
        }
        proc, proof, _run_dir = self.run_driver(outcomes, profile="full", manifest_entries=[entry])
        self.assertNotEqual(proc.returncode, 0)
        self.assertEqual(proof["status"], "BLOCKED")
        self.assertEqual(proof["applicability"]["missing_entry_count"], 1)

    def test_timeout_policy_records_effective_command_budgets_without_reclassifying_results(self):
        outcomes = {
            "gf16": "PASS",
            "gf17": "PASS",
            "iogen01": "PASS",
            "writev03": "PASS",
            "plain01": "PASS",
        }
        timeout_entries = [
            {"test_id": "gf16", "exec_timeout_seconds": 180, "suite_timeout_seconds": 240},
            {"test_id": "gf17", "exec_timeout_seconds": 180, "suite_timeout_seconds": 240},
            {"test_id": "iogen01", "exec_timeout_seconds": 180, "suite_timeout_seconds": 240},
            {"test_id": "writev03", "exec_timeout_seconds": 1200, "suite_timeout_seconds": 1320},
        ]
        proc, proof, run_dir = self.run_driver(outcomes, profile="full", timeout=120, timeout_entries=timeout_entries)
        self.assertEqual(proc.returncode, 0, proof)
        self.assertEqual(proof["status"], "PASS")
        self.assertEqual(proof["accounting"]["result_counts"]["PASS"], 5)
        self.assertTrue(proof["timeout_policy"]["enabled"])
        self.assertEqual(proof["timeout_policy"]["timeouts"]["writev03"], {"exec_timeout_seconds": 1200, "suite_timeout_seconds": 1320})
        timeout_check = next(check for check in proof["checks"] if check["name"] == "timeout-policy")
        self.assertEqual(timeout_check["status"], "PASS")
        self.assertEqual(timeout_check["evidence"]["sha256"], proof["timeout_policy"]["sha256"])
        normalized = run_dir / "artifacts" / "std-02-ltp" / "timeout-policy.normalized.json"
        self.assertTrue(normalized.exists())
        self.assertEqual(json.loads(normalized.read_text())["size_bytes"], proof["timeout_policy"]["size_bytes"])
        commands = json.loads((run_dir / "artifacts" / "std-02-ltp" / "commands.json").read_text())
        by_id = {command["test_id"]: command for command in commands}
        self.assertEqual(by_id["gf16"]["timeout"], {"exec_timeout_seconds": 180, "suite_timeout_seconds": 240, "source": "timeout-policy", "process_timeout_seconds": 245})
        self.assertEqual(by_id["writev03"]["timeout"], {"exec_timeout_seconds": 1200, "suite_timeout_seconds": 1320, "source": "timeout-policy", "process_timeout_seconds": 1325})
        self.assertEqual(by_id["plain01"]["timeout"], {"exec_timeout_seconds": 120, "suite_timeout_seconds": 240, "source": "default", "process_timeout_seconds": 245})
        gf16_command = json.loads((run_dir / "artifacts" / "std-02-ltp" / by_id["gf16"]["artifacts"]["command"]).read_text())
        self.assertEqual(gf16_command["timeout"], by_id["gf16"]["timeout"])
        self.assertEqual(gf16_command["argv"][gf16_command["argv"].index("--exec-timeout") + 1], "180")
        self.assertEqual(gf16_command["argv"][gf16_command["argv"].index("--suite-timeout") + 1], "240")
        writev03_command = json.loads((run_dir / "artifacts" / "std-02-ltp" / by_id["writev03"]["artifacts"]["command"]).read_text())
        self.assertEqual(writev03_command["argv"][writev03_command["argv"].index("--exec-timeout") + 1], "1200")
        self.assertEqual(writev03_command["argv"][writev03_command["argv"].index("--suite-timeout") + 1], "1320")

    def test_timeout_is_blocked_and_counted(self):
        proc, proof, _run_dir = self.run_driver({f"t{i:03d}": ("SLEEP" if i == 0 else "PASS") for i in range(657)}, profile="full", max_tests=1, timeout=1)
        self.assertNotEqual(proc.returncode, 0)
        self.assertEqual(proof["status"], "BLOCKED")
        self.assertGreaterEqual(proof["accounting"]["result_counts"]["TIMEOUT"], 1)


if __name__ == "__main__":
    unittest.main()
