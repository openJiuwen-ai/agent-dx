"""Tool contracts only: no fake product PASS or filesystem integration claims."""

import json
from pathlib import Path
import sys
import tempfile
import unittest

TESTS = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(TESTS))
from fixtures.common import Case, Channel, discover, select


class SelectionTests(unittest.TestCase):
    def setUp(self):
        self.cases = [
            Case("ownerfs.basic.read_write", ("local-basic",), "ownerfs", ("LB03",), None),
            Case("dfs.basic.read_write", ("local-basic",), "dfs", ("LB05",), None),
            Case("ownerfs.cluster.read_write", ("cluster-basic",), "ownerfs", ("MV02",), None),
        ]

    def test_glob_union_deduplicates_and_keeps_registry_order(self):
        got = select(self.cases, patterns=["ownerfs.*", "*.basic.*"])
        self.assertEqual([c.id for c in got], [c.id for c in self.cases])

    def test_one_unknown_selector_rejects_whole_union(self):
        with self.assertRaises(ValueError):
            select(self.cases, patterns=["ownerfs.*", "typo.*"])

    def test_selectors_cannot_silently_override_each_other(self):
        with self.assertRaises(ValueError):
            select(self.cases, all_cases=True, patterns=["*"])

    def test_default_and_all_have_different_scopes(self):
        self.assertEqual(len(select(self.cases)), 2)
        self.assertEqual(len(select(self.cases, all_cases=True)), 3)

    def test_unknown_suite_rejected(self):
        with self.assertRaises(ValueError):
            select(self.cases, suite="no-such-suite")

    def test_real_registry_has_complete_groups_without_running_products(self):
        cases = discover()
        self.assertTrue(cases)
        self.assertEqual(len(cases), len({c.id for c in cases}))
        for tag, prefix, count in [("local-basic", "LB", 8), ("cluster-basic", "MV", 7), ("cluster-manyread", "MV", 8)]:
            selected = select(cases, suite=tag)
            groups = {g for c in selected for g in c.groups}
            self.assertGreaterEqual(groups, {f"{prefix}{i:02}" for i in range(1, count + 1)})


class WorkerProtocolTests(unittest.TestCase):
    def test_child_exit_status_is_retained_and_not_inferred_from_pid(self):
        from fixtures.worker import Worker

        worker = Worker()
        with tempfile.TemporaryDirectory() as temp:
            worker.dispatch({"action": "initialize", "root": temp + "/owned-run", "token": "unit"})
            worker.dispatch(
                {
                    "action": "start",
                    "role": "A",
                    "argv": [sys.executable, "-c", "raise SystemExit(7)"],
                    "log": temp + "/owned-run/a.log",
                }
            )
            worker.processes["A"].wait(timeout=5)
            with self.assertRaises(RuntimeError):
                worker.dispatch({"action": "stop", "role": "A"})
            self.assertEqual(worker.receipts[-1]["wait_status"], 7)
            self.assertFalse(worker.processes)

    def test_upload_refuses_traversal_and_unowned_paths(self):
        from fixtures.worker import Worker

        worker = Worker()
        with tempfile.TemporaryDirectory() as temp:
            worker.dispatch({"action": "initialize", "root": temp + "/owned-run", "token": "unit"})
            for path in [temp + "/outside", temp + "/owned-run/../outside"]:
                with self.assertRaises(ValueError):
                    worker.owned(path)

    def test_nonzero_child_command_does_not_break_worker_protocol(self):
        channel = Channel([sys.executable, "-u", str(TESTS / "fixtures/worker.py")])
        try:
            result = channel.call(
                "command", argv=[sys.executable, "-c", 'import sys;print("failure evidence");sys.exit(4)']
            )
            self.assertEqual(result["returncode"], 4)
            self.assertIn("failure evidence", result["stdout"])
            result = channel.call("command", argv=[sys.executable, "-c", 'print("still alive")'])
            self.assertEqual(result["stdout"].strip(), "still alive")
        finally:
            channel.close()

    def test_bulk_payload_uses_stdin_without_linux_argv_limit(self):
        channel = Channel([sys.executable, "-u", str(TESTS / "fixtures/worker.py")])
        try:
            payload = "x" * (256 * 1024)
            result = channel.call(
                "command", argv=[sys.executable, "-c", "import sys;print(len(sys.stdin.read()))"], stdin=payload
            )
            self.assertEqual(result["returncode"], 0)
            self.assertEqual(int(result["stdout"]), len(payload))
        finally:
            channel.close()


class EnvironmentTests(unittest.TestCase):
    def config(self):
        return json.loads((TESTS / "fixtures/multivm-3.example.json").read_text())

    def test_multivm_cannot_alias_roles_to_same_guest(self):
        from fixtures.multivm import validate
        from fixtures.common import Blocked

        cfg = self.config()
        cfg["roles"]["B"]["host"] = cfg["roles"]["A"]["host"]
        case = Case("ownerfs.cluster.read_write", ("cluster-basic",), "ownerfs", ("MV02",), None)
        with self.assertRaises(Blocked):
            validate(cfg, [case])

    def test_all_cannot_silently_drop_incompatible_local_cases(self):
        from fixtures.multivm import validate
        from fixtures.common import Blocked

        with self.assertRaises(Blocked):
            validate(self.config(), [Case("ownerfs.basic.read_write", ("local-basic",), "ownerfs", ("LB03",), None)])

    def test_fourth_reader_requires_fourth_guest(self):
        from fixtures.multivm import validate
        from fixtures.common import Blocked

        case = Case("dfs.cluster.many_readers", ("cluster-manyread",), "dfs", ("MV08",), None)
        with self.assertRaises(Blocked):
            validate(self.config(), [case])

    def test_unsafe_host_names_are_rejected_before_transport(self):
        from fixtures.multivm import validate

        cfg = self.config()
        cfg["hosts"]["../escape"] = cfg["hosts"].pop("vm-a")
        cfg["roles"]["A"]["host"] = "../escape"
        case = Case("ownerfs.cluster.read_write", ("cluster-basic",), "ownerfs", ("MV02",), None)
        with self.assertRaises(ValueError):
            validate(cfg, [case])


class ResultTests(unittest.TestCase):
    def execute(self, case_function, cleanup_status="PASS", blocked=False):
        import importlib.util
        from unittest.mock import patch
        from fixtures.common import Blocked

        spec = importlib.util.spec_from_file_location("integration_cli_under_test", TESTS / "run.py")
        module = importlib.util.module_from_spec(spec)
        spec.loader.exec_module(module)

        class ControlledFixture:
            def __init__(self, *args):
                pass

            def prepare(self):
                if blocked:
                    raise Blocked("missing actual test environment")

            def cleanup(self):
                return {"status": cleanup_status, "errors": ["cleanup evidence"] if cleanup_status != "PASS" else []}

        case = Case("tool.probe", ("local-basic",), "ownerfs", ("LB03",), case_function)
        with tempfile.TemporaryDirectory() as temp:
            env = Path(temp) / "env.json"
            env.write_text((TESTS / "fixtures/singlevm.example.json").read_text())
            result = Path(temp) / "result"
            with patch.object(module, "discover", return_value=[case]), \
                    patch.object(module, "Cluster", ControlledFixture):
                code = module.main(
                    ["run", "--case", case.id, "--env", str(env), "--binary-dir", temp, "--work-root", str(result)]
                )
            return code, json.loads((result / "result.json").read_text())

    def test_cleanup_failure_prevents_false_pass(self):
        code, report = self.execute(lambda _: None, cleanup_status="FAIL")
        self.assertEqual(code, 1)
        self.assertEqual(report["status"], "FAIL")
        self.assertTrue(report["cleanup_errors"])

    def test_original_failure_survives_cleanup_failure(self):
        def fail(_):
            raise AssertionError("original assertion")

        code, report = self.execute(fail, cleanup_status="FAIL")
        self.assertEqual(code, 1)
        self.assertIn("original assertion", report["cases"][0]["error"])
        self.assertTrue(report["cleanup_errors"])

    def test_missing_environment_is_blocked_not_pass(self):
        code, report = self.execute(lambda _: None, blocked=True)
        self.assertEqual(code, 2)
        self.assertEqual(report["status"], "BLOCKED")

    def test_targeted_success_remains_partial(self):
        code, report = self.execute(lambda _: None)
        self.assertEqual(code, 0)
        self.assertTrue(report["partial"])
        self.assertEqual(report["suite"], "targeted")


class ReplyIdentityTests(unittest.TestCase):
    def test_late_response_cannot_be_misattributed_to_cleanup(self):
        import io
        from types import SimpleNamespace
        from collections import deque
        import queue
        import threading

        channel = object.__new__(Channel)
        channel.proc = SimpleNamespace(stdin=io.StringIO())
        channel.lock = threading.Lock()
        channel.sequence = 1
        channel.timeout = 1
        channel.errors = deque()
        channel.replies = queue.Queue()
        channel.replies.put({"id": 1, "ok": True, "value": {"status": "PASS", "wrong": True}})
        channel.replies.put({"id": 2, "ok": True, "value": {"status": "FAIL", "cleanup": "unconfirmed"}})
        result = channel.call("cleanup")
        self.assertEqual(result["status"], "FAIL")
        self.assertNotIn("wrong", result)


class MatrixConfigurationTests(unittest.TestCase):
    def test_all_maps_local_and_cluster_to_explicit_environments(self):
        import importlib.util

        spec = importlib.util.spec_from_file_location("matrix_cli", TESTS / "run.py")
        module = importlib.util.module_from_spec(spec)
        spec.loader.exec_module(module)
        local = Case("ownerfs.basic.read_write", ("local-basic",), "ownerfs", ("LB03",), None)
        cluster = Case("ownerfs.cluster.read_write", ("cluster-basic",), "ownerfs", ("MV02",), None)
        with tempfile.TemporaryDirectory() as directory:
            base = Path(directory)
            (base / "local.json").write_text((TESTS / "fixtures/singlevm.example.json").read_text())
            (base / "cluster.json").write_text((TESTS / "fixtures/namespaces-3.example.json").read_text())
            path = base / "matrix.json"
            path.write_text(
                json.dumps({"kind": "matrix", "environments": {"local": "local.json", "cluster": "cluster.json"}})
            )
            raw, configs = module.configurations(path, [local, cluster])
            self.assertEqual(raw["kind"], "matrix")
            self.assertEqual(configs[local.id]["kind"], "singlevm")
            self.assertEqual(configs[cluster.id]["kind"], "namespaces")

    def test_missing_selected_matrix_environment_does_not_skip_cases(self):
        import importlib.util
        from fixtures.common import Blocked

        spec = importlib.util.spec_from_file_location("matrix_cli_missing", TESTS / "run.py")
        module = importlib.util.module_from_spec(spec)
        spec.loader.exec_module(module)
        case = Case("ownerfs.cluster.read_write", ("cluster-basic",), "ownerfs", ("MV02",), None)
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / "matrix.json"
            path.write_text(json.dumps({"kind": "matrix", "environments": {}}))
            with self.assertRaises(Blocked):
                module.configurations(path, [case])


class NamespaceIdentityTests(unittest.TestCase):
    def exercise(self, alias_pid=False):
        from fixtures.multivm import Cluster
        from unittest.mock import patch
        import fixtures.multivm as fixtures

        config = json.loads((TESTS / "fixtures/namespaces-3.example.json").read_text())
        case = Case("ownerfs.cluster.read_write", ("cluster-basic",), "ownerfs", ("MV02",), None)

        class ObservedWorker:
            def __init__(self, number):
                self.number = number

            def call(self, action, **_):
                if action == "inspect":
                    return {
                        "system": "Linux",
                        "machine": "aarch64",
                        "uid": 0,
                        "fuse": True,
                        "filesystem": "ext4",
                        "free": 2 * 1024**3,
                        "tools": {"python3": "/usr/bin/python3"},
                        "machine_id": "one-vm",
                        "boot_id": "one-boot",
                        "product_uuid": None,
                        "network_namespace": "net-" + str(self.number),
                        "pid_namespace": "pid-" + str(0 if alias_pid else self.number),
                        "mount_namespace": "mnt-" + str(self.number),
                    }
                if action == "status":
                    return {}
                if action == "cleanup":
                    return {"status": "PASS", "errors": [], "waits": []}
                raise AssertionError(action)

            def close(self):
                pass

        with tempfile.TemporaryDirectory() as directory:
            cluster = Cluster(config, directory, directory, case, "test-token")
            with patch.object(fixtures.namespaces, "connect", side_effect=[ObservedWorker(i) for i in range(3)]):
                try:
                    return cluster.connect()
                finally:
                    cluster.cleanup()

    def test_namespaces_accept_one_guest_with_distinct_role_namespaces(self):
        result = self.exercise()
        self.assertEqual(len(result), 3)
        self.assertEqual(len({v["boot_id"] for v in result.values()}), 1)

    def test_aliasing_same_pid_namespace_cannot_claim_node_isolation(self):
        from fixtures.common import Blocked

        with self.assertRaises(Blocked):
            self.exercise(alias_pid=True)


class ArtifactIdentityTests(unittest.TestCase):
    def test_mixed_or_modified_product_binaries_are_blocked_before_deployment(self):
        from fixtures.multivm import Cluster
        from fixtures.common import Blocked, sha256

        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            header = bytearray(20)
            header[:6] = b"\x7fELF\x02\x01"
            header[18:20] = (62).to_bytes(2, "little")
            for name in ("afs-meta", "afs-node"):
                (root / name).write_bytes(header)
            (root / "artifact-identity.json").write_text(
                json.dumps(
                    {
                        "compiled_source_logical": "a" * 64,
                        "binaries": {"afs-meta": sha256(root / "afs-meta"), "afs-node": "0" * 64},
                    }
                )
            )
            config = json.loads((TESTS / "fixtures/singlevm.example.json").read_text())
            case = Case("ownerfs.basic.read_write", ("local-basic",), "ownerfs", ("LB03",), None)
            cluster = Cluster(config, root, root, case, "test")
            with self.assertRaises(Blocked):
                cluster.inputs({"guest": {"machine": "x86_64"}})


if __name__ == "__main__":
    unittest.main()
