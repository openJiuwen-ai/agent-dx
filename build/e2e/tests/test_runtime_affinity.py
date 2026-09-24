"""The heterogeneous case must check placement and execute through the SDK."""

import importlib.util
import json
from pathlib import Path
import sys
import tempfile
import types
import unittest
from unittest.mock import patch


ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))
import runtime_inventory


class RuntimeAffinityScenarioTests(unittest.TestCase):
    def fixture(self, *, runsc_node="node2", assigned_node="node2"):
        state = {"created": 0, "deleted": False, "closed": False, "commands": []}

        class Sandbox:
            def __init__(self, **options):
                state["created"] += 1
                state["options"] = options
                self.id = "default-runtime-affinity"
                self.commands = types.SimpleNamespace(run=self.run)

            def run(self, command):
                state["commands"].append(command)
                return types.SimpleNamespace(exit_code=0, stdout="runtime-affinity-ready")

            def kill(self):
                state["deleted"] = True

            def close(self):
                state["closed"] = True

        def catalog():
            result = {
                f"node:{node_id}": json.dumps({
                    "node": {
                        "id": node_id, "available": True,
                        "runtime_classes": ["runc", "runsc"] if node_id == runsc_node else ["runc"],
                    },
                    "session": {"routable": True},
                })
                for node_id in ("node1", "node2")
            }
            result["environment:default-runtime-affinity"] = json.dumps({
                "spec": {"runtime_class": "runsc"},
                "assignment": {"node_id": assigned_node},
                "result": {
                    "state": "Deleted" if state["deleted"] else "Running",
                    "resources_held": not state["deleted"],
                },
            })
            return result

        modules = {
            "adx_sandbox": types.SimpleNamespace(Sandbox=Sandbox),
            "node": types.SimpleNamespace(catalog=catalog),
            "runtime_inventory": runtime_inventory,
        }
        spec = importlib.util.spec_from_file_location("runtime_affinity_case", ROOT / "runtime_affinity.py")
        scenario = importlib.util.module_from_spec(spec)
        with patch.dict(sys.modules, modules):
            spec.loader.exec_module(scenario)
        return scenario, state

    def test_unique_runtime_node_executes_and_releases(self):
        scenario, state = self.fixture()
        with tempfile.TemporaryDirectory() as directory:
            output = Path(directory) / "result.json"
            scenario.run(object(), "image@sha256:test", output)
            report = json.loads(output.read_text())
        self.assertEqual(report["status"], "passed")
        self.assertEqual(report["cases"][0]["node_id"], "node2")
        self.assertEqual(state["options"]["runtime"], "runsc")
        self.assertNotIn("node_id", state["options"])
        self.assertEqual(state["commands"], ["printf runtime-affinity-ready"])
        self.assertTrue(state["deleted"] and state["closed"])

    def test_runc_only_fixture_fails_before_creation(self):
        scenario, state = self.fixture(runsc_node=None)
        with tempfile.TemporaryDirectory() as directory:
            output = Path(directory) / "result.json"
            with self.assertRaisesRegex(AssertionError, "exactly one"):
                scenario.run(object(), "image@sha256:test", output)
            self.assertEqual(json.loads(output.read_text())["status"], "failed")
        self.assertEqual(state["created"], 0)

    def test_wrong_assignment_is_not_accepted(self):
        scenario, state = self.fixture(assigned_node="node1")
        with tempfile.TemporaryDirectory() as directory:
            output = Path(directory) / "result.json"
            with self.assertRaises(AssertionError):
                scenario.run(object(), "image@sha256:test", output)
            self.assertEqual(json.loads(output.read_text())["status"], "failed")
        self.assertTrue(state["deleted"] and state["closed"])


if __name__ == "__main__":
    unittest.main()
