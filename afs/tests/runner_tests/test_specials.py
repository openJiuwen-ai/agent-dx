"""Contracts for explicit DFS profiles and owned probe lifetimes, not product PASS."""

from pathlib import Path
import importlib.util
import io
import json
import sys
import tempfile
import time
import unittest
from contextlib import redirect_stderr, redirect_stdout

TESTS = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(TESTS))

from fixtures import common


class ProfileTests(unittest.TestCase):
    def test_supported_dfs_profile_is_explicit(self):
        self.assertTrue(hasattr(common, "DfsProfile"), "special cases need an explicit validated DFS profile")
        profile = common.DfsProfile(copies=2, node_count=2, write_queue_bytes=64 * 1024**2)
        self.assertEqual(profile.copies, 2)
        self.assertEqual(profile.node_count, 2)
        self.assertIn("dfs_write_queue_bytes = 67108864", profile.node_config())
        self.assertIn("dfs_sync_required_copies = 2", profile.meta_config())

    def test_invalid_profile_values_cannot_inject_product_configuration(self):
        for values in [
            {"copies": True}, {"copies": 3, "node_count": 2}, {"node_count": 1},
            {"write_queue_bytes": 0}, {"write_queue_bytes": 2**40 + 1},
            {"read_crc_mode": 'off"\ndata_dir="/tmp/other'},
            {"keep_cache": "false"}, {"kernel_readahead_bytes": 2**30},
            {"native_open_authority": "true"},
            {"readahead_max_bytes": -1},
        ]:
            with self.subTest(values=values), self.assertRaises(ValueError):
                common.DfsProfile(**values)

    def test_profile_boolean_and_crc_values_are_valid_toml(self):
        import tomllib

        profile = common.DfsProfile(keep_cache=False, read_crc_mode="receiver", kernel_readahead_bytes=0)
        parsed = tomllib.loads(profile.node_config())
        self.assertIs(parsed["dfs_fuse_keep_cache"], False)
        self.assertEqual(parsed["dfs_read_crc_mode"], "receiver")

    def test_explicit_two_node_case_does_not_require_an_unused_c(self):
        from fixtures.multivm import validate

        config = json.loads((TESTS / "fixtures/singlevm.example.json").read_text())
        del config["roles"]["C"]
        case = common.Case("dfs.write.local.order", ("local-write-special",), "dfs", (), None,
                           profile=common.DfsProfile())
        self.assertEqual(validate(config, [case]), ["guest"])
        with self.assertRaises(common.Blocked):
            validate(config, [common.Case("dfs.basic.read", ("local-basic",), "dfs", (), None)])

    def test_dfs_profile_is_rejected_on_ownerfs(self):
        from fixtures.multivm import validate

        config = json.loads((TESTS / "fixtures/singlevm.example.json").read_text())
        case = common.Case("ownerfs.invalid", ("local-extended",), "ownerfs", (), None,
                           profile=common.DfsProfile())
        with self.assertRaises(ValueError):
            validate(config, [case])

    def test_extended_and_performance_cases_do_not_enlarge_all_basic(self):
        cases = common.discover()
        basic = common.select(cases, suite="all-basic")
        self.assertEqual(len(basic), 22)
        self.assertTrue(all(c.profile is None and c.purpose == "functional" for c in basic))
        self.assertEqual(len(common.select(cases, suite="local-write-special")), 5)
        self.assertEqual(len(common.select(cases, suite="cluster-read-special")), 5)
        specials = common.select(cases, suite="all-special")
        self.assertEqual(len(specials), 20)
        self.assertTrue(all(c.purpose == "functional" for c in specials))


class OwnedHelperTests(unittest.TestCase):
    def initialize(self, directory):
        from fixtures.worker import Worker

        worker = Worker()
        root = str(Path(directory) / "owned-run")
        worker.dispatch({"action": "initialize", "root": root, "token": "unit"})
        return worker, root

    def test_helper_exit_failure_keeps_actual_wait_status(self):
        with tempfile.TemporaryDirectory() as directory:
            worker, root = self.initialize(directory)
            worker.dispatch({
                "action": "helper_start", "name": "failure",
                "argv": [sys.executable, "-c", "raise SystemExit(7)"], "log": root + "/failure.log",
            })
            with self.assertRaises(RuntimeError):
                worker.dispatch({"action": "wait", "role": "H:failure", "timeout": 5})
            self.assertEqual(worker.receipts[-1]["wait_status"], 7)
            self.assertFalse(worker.processes)

    def test_cancelled_helper_is_distinct_from_successfully_completed_helper(self):
        with tempfile.TemporaryDirectory() as directory:
            worker, root = self.initialize(directory)
            worker.dispatch({
                "action": "helper_start", "name": "waiting",
                "argv": [sys.executable, "-c", "import time;time.sleep(60)"], "log": root + "/waiting.log",
            })
            value = worker.dispatch({"action": "stop_helpers", "timeout": 5})
            self.assertEqual(value["status"], "PASS")
            self.assertTrue(value["waits"][0]["cancelled_helper"])
            self.assertNotEqual(value["waits"][0]["wait_status"], 0)
            self.assertFalse(worker.processes)

    def test_bad_helper_does_not_prevent_retiring_other_helpers(self):
        with tempfile.TemporaryDirectory() as directory:
            worker, root = self.initialize(directory)
            for name, code in [("bad", "raise SystemExit(9)"), ("pending", "import time;time.sleep(60)")]:
                worker.dispatch({"action": "helper_start", "name": name, "argv": [sys.executable, "-c", code],
                                 "log": root + "/" + name + ".log"})
            worker.processes["H:bad"].wait(timeout=5)
            result = worker.dispatch({"action": "stop_helpers", "timeout": 5})
            self.assertEqual(result["status"], "FAIL")
            self.assertFalse(worker.processes)
            self.assertTrue(any(r["wait_status"] == 9 for r in worker.receipts))

    def test_helper_name_cannot_impersonate_product_role_or_escape_log_root(self):
        with tempfile.TemporaryDirectory() as directory:
            worker, root = self.initialize(directory)
            for name in ("M", "../escape", "a/b"):
                with self.assertRaises(ValueError):
                    worker.dispatch({"action": "helper_start", "name": name, "argv": [sys.executable, "-c", "pass"],
                                     "log": root + "/helper.log"})
            with self.assertRaises(ValueError):
                worker.dispatch({"action": "helper_start", "name": "valid", "argv": [sys.executable, "-c", "pass"],
                                 "log": directory + "/outside.log"})

    def test_helper_is_cancelled_before_normal_node_stop(self):
        # Controlled processes verify wait ordering; real FUSE FDs are tested by the fault cases.
        with tempfile.TemporaryDirectory() as directory:
            worker, root = self.initialize(directory)
            marker = Path(root) / "node-ready"
            code = ("import signal,sys,time,pathlib;signal.signal(signal.SIGTERM,lambda *_:sys.exit(0));"
                    "pathlib.Path(sys.argv[1]).touch();time.sleep(60)")
            worker.dispatch({"action": "start", "role": "A", "argv": [sys.executable, "-c", code, str(marker)],
                             "log": root + "/node.log"})
            try:
                deadline = time.monotonic() + 5
                while not marker.exists():
                    self.assertLess(time.monotonic(), deadline)
                    time.sleep(0.01)
                worker.dispatch({
                    "action": "helper_start", "name": "fd-holder",
                    "argv": [sys.executable, "-c", "import time;time.sleep(60)"], "log": root + "/holder.log",
                })
                result = worker.dispatch({"action": "cleanup"})
                self.assertEqual(result["status"], "PASS", result)
                self.assertEqual([r["role"] for r in worker.receipts], ["H:fd-holder", "A"])
            finally:
                worker.cleanup()


class PerformanceBoundaryTests(unittest.TestCase):
    def module(self, name, path):
        spec = importlib.util.spec_from_file_location(name, path)
        module = importlib.util.module_from_spec(spec)
        spec.loader.exec_module(module)
        return module

    def test_performance_is_discoverable_but_blocked_without_opt_in_before_environment(self):
        runner = self.module("special_cli_guard", TESTS / "run.py")
        with redirect_stderr(io.StringIO()) as errors:
            self.assertEqual(runner.main(["run", "--suite", "local-performance"]), 2)
        value = json.loads(errors.getvalue())
        self.assertEqual(value["status"], "BLOCKED")
        with redirect_stdout(io.StringIO()) as output:
            self.assertEqual(runner.main(["list", "--suite", "local-performance"]), 0)
        items = json.loads(output.getvalue())
        self.assertEqual(len(items), 2)
        self.assertTrue(all(item["requires_performance_opt_in"] for item in items))

    def test_performance_requires_declared_size_even_after_opt_in(self):
        for config in ({}, {"performance": {}}, {"performance": {"bytes": 4096, "request_bytes": -1}}):
            with self.assertRaises(common.Blocked):
                common.performance_parameters(config)

    def test_functional_io_tool_qualification_emits_no_measurement(self):
        tool = self.module("qualified_io_asset", TESTS / "tools/io_workload.py")
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / "payload"
            first = tool.run("write", path, 12288, 8192)
            second = tool.run("read", path, 12288, 8192)
            self.assertEqual(first["sha256"], second["sha256"])
            self.assertEqual(first["payload_calls"], 2)
            self.assertEqual(first["validation_read_bytes_outside_timing"], 12288)
            self.assertNotIn("measurement", first)
            self.assertNotIn("measurement", second)
            self.assertEqual(first["progress_disk_writes_in_timing"], 0)
            with self.assertRaises(FileExistsError):
                tool.run("write", path, 4096, 4096)
            with path.open("r+b") as stream:
                stream.write(b"x")
            with self.assertRaises(AssertionError):
                tool.run("read", path, 12288, 8192)

    def test_io_asset_rejects_invalid_or_unbounded_requests(self):
        tool = self.module("bounded_io_asset", TESTS / "tools/io_workload.py")
        for size, request in [(True, 4096), (0, 4096), (2**30 + 4096, 4096), (4096, 0), (4096, 2**20 + 4096)]:
            with self.assertRaises(ValueError):
                tool.run("read", "/never-opened", size, request)

    def test_metric_parser_does_not_turn_missing_or_bad_evidence_into_zero(self):
        from suites.dfs.test_read_special import metric

        self.assertEqual(metric('', 'metric', {'role': 'B'}, 0), 0)
        for raw in ['', 'metric{role="B"} NaN', 'metric{role="B"} -1', 'metric{role="B"} 1\nmetric{role="B"} 2']:
            with self.assertRaises(AssertionError):
                metric(raw, 'metric', {'role': 'B'})


if __name__ == "__main__":
    unittest.main()
