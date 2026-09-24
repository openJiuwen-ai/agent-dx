"""A live foreground client request must outlast the idle threshold."""

import importlib.util
import json
from pathlib import Path
import sys
import tempfile
import types
import unittest
from unittest.mock import patch


ROOT = Path(__file__).resolve().parents[1]


class IdleActiveScenarioTests(unittest.TestCase):
    def fixture(self, *, running=True):
        state = {"now": 0, "created": 0, "closed": False, "deleted": False,
                 "waited": False, "command": None}

        class Sandbox:
            def __init__(self, **options):
                state["created"] += 1
                state["options"] = options
                self.id = "active-idle-instance"
                self.commands = types.SimpleNamespace(run=self.run)

            def run(self, command, **_options):
                state["command"] = command
                state["now"] += 12
                return types.SimpleNamespace(exit_code=0, stdout="active-request-complete")

            def is_running(self):
                return running

            def close(self):
                state["closed"] = True

            @classmethod
            def delete(cls, instance_id, **_options):
                self.assertEqual(instance_id, "active-idle-instance")
                state["deleted"] = True

        def catalog():
            return {"environment:active-idle-instance": json.dumps({
                "result": {"state": "Deleted" if state["deleted"] else "Running",
                           "resources_held": not state["deleted"]},
            })}

        def wait_deleted(instance_id, _connection, **_options):
            self.assertEqual(instance_id, "active-idle-instance")
            state["waited"] = True
            state["deleted"] = True

        modules = {
            "adx_sandbox": types.SimpleNamespace(Sandbox=Sandbox),
            "node": types.SimpleNamespace(catalog=catalog),
            "functional_lifecycle": types.SimpleNamespace(_wait_deleted=wait_deleted),
        }
        spec = importlib.util.spec_from_file_location("idle_active_case", ROOT / "idle_active.py")
        scenario = importlib.util.module_from_spec(spec)
        with patch.dict(sys.modules, modules):
            spec.loader.exec_module(scenario)
        scenario.monotonic = lambda: state["now"]
        return scenario, state

    def test_active_request_survives_then_idle_reclaims(self):
        scenario, state = self.fixture()
        with tempfile.TemporaryDirectory() as directory:
            output = Path(directory) / "result.json"
            scenario.run(object(), "image@sha256:test", output)
            report = json.loads(output.read_text())
        self.assertEqual(report["status"], "passed")
        self.assertEqual(report["cases"][0]["id"], "lifecycle.active-request-prevents-idle")
        self.assertEqual(state["options"]["idle_timeout"], 6)
        self.assertIn("sleep 12", state["command"])
        self.assertTrue(state["closed"] and state["waited"] and state["deleted"])

    def test_early_reclaim_fails_and_cleans_up(self):
        scenario, state = self.fixture(running=False)
        with tempfile.TemporaryDirectory() as directory:
            output = Path(directory) / "result.json"
            with self.assertRaises(AssertionError):
                scenario.run(object(), "image@sha256:test", output)
            report = json.loads(output.read_text())
        self.assertEqual(report["status"], "failed")
        self.assertTrue(state["closed"] and state["deleted"])
        self.assertFalse(state["waited"])


if __name__ == "__main__":
    unittest.main()
