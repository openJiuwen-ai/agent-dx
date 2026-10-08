"""The fault scenario must reject duplicate runtime identity and unbounded retry."""

import importlib.util
import json
from pathlib import Path
import sys
import tempfile
import types
import unittest
from unittest.mock import patch


ROOT = Path(__file__).resolve().parents[1]


class RuntimeExitScenarioTests(unittest.TestCase):
    def fixture(self, *, reuse_identity=False):
        state = {"records": {}, "backends": {}, "deleted": [], "commands": [],
                 "stale_status_reads": {}}

        class Sandbox:
            def __init__(self, **options):
                self.id = f"instance-{len(state['records']) + 1}"
                self.commands = types.SimpleNamespace(run=self.run)
                state["records"][self.id] = {
                    "result": {"state": "Running", "resources_held": True,
                               "restart_pending": False, "restart_attempts": 0,
                               "runtime": {"id": self.id + "-runtime-0"}},
                    "assignment": {"node_id": "node1", "generation": 1},
                    "policy": options.get("restart_policy"),
                }
                state["backends"][self.id] = [self.id + "-backend-0"]

            def run(self, command):
                state["commands"].append(command)
                return types.SimpleNamespace(exit_code=0, stdout=command.removeprefix("printf "))

            def is_running(self):
                if state["stale_status_reads"].get(self.id, 0):
                    state["stale_status_reads"][self.id] -= 1
                    return True
                return state["records"][self.id]["result"]["state"] == "Running"

            def kill(self):
                state["deleted"].append(self.id)
                state["records"][self.id]["result"]["state"] = "Deleted"
                state["backends"][self.id] = []

            def close(self):
                pass

        modules = {
            "adx_sandbox": types.SimpleNamespace(Sandbox=Sandbox,
                                                  RestartPolicy=lambda **options: options),
            "node": types.SimpleNamespace(
                catalog=lambda: {},
                persisted_runtime_id=lambda result: result["runtime"]["id"]),
        }
        spec = importlib.util.spec_from_file_location("runtime_exit_case", ROOT / "runtime_exit.py")
        scenario = importlib.util.module_from_spec(spec)
        with patch.dict(sys.modules, modules):
            spec.loader.exec_module(scenario)

        scenario._record = lambda instance_id: state["records"][instance_id]
        scenario._backend_ids = lambda instance_id: state["backends"][instance_id]

        def delete_backend(instance_id):
            identities = state["backends"][instance_id]
            self.assertEqual(len(identities), 1)
            old = identities[0]
            state["backends"][instance_id] = []
            record = state["records"][instance_id]
            result = record["result"]
            result["state"] = "Failed"
            result["restart_pending"] = bool(record["policy"] and result["restart_attempts"] < 2)
            if not result["restart_pending"]:
                result["resources_held"] = False
                state["stale_status_reads"][instance_id] = 1
            return old

        def wait(predicate, **_options):
            value = predicate()
            if value:
                return value
            for instance_id, record in state["records"].items():
                result = record["result"]
                if result["state"] == "Failed" and result["restart_pending"]:
                    attempt = result["restart_attempts"] + 1
                    result.update(state="Running", restart_pending=False,
                                  restart_attempts=attempt,
                                  runtime={"id": (result["runtime"]["id"] if reuse_identity
                                          else instance_id + f"-runtime-{attempt}")})
                    state["backends"][instance_id] = [instance_id + f"-backend-{attempt}"]
            value = predicate()
            if not value:
                raise TimeoutError("expected lifecycle transition missing")
            return value

        scenario._delete_backend = delete_backend
        scenario._wait = wait
        return scenario, state

    def test_never_and_bounded_restart_reach_expected_terminal_states(self):
        scenario, state = self.fixture()
        with tempfile.TemporaryDirectory() as directory:
            output = Path(directory) / "result.json"
            scenario.run(object(), "image@sha256:test", output)
            report = json.loads(output.read_text())
        self.assertEqual(report["status"], "passed")
        self.assertEqual([case["id"] for case in report["cases"]], [
            "lifecycle.runtime-exit-never", "lifecycle.runtime-exit-bounded-restart",
        ])
        self.assertEqual(len(report["cases"][1]["attempts"]), 2)
        self.assertEqual(len(state["deleted"]), 2)
        self.assertEqual(state["backends"], {"instance-1": [], "instance-2": []})

    def test_restarted_backend_cannot_reuse_runtime_identity(self):
        scenario, state = self.fixture(reuse_identity=True)
        with tempfile.TemporaryDirectory() as directory:
            output = Path(directory) / "result.json"
            with self.assertRaises(TimeoutError):
                scenario.run(object(), "image@sha256:test", output)
            report = json.loads(output.read_text())
        self.assertEqual(report["status"], "failed")
        self.assertEqual(len(state["deleted"]), 2)


if __name__ == "__main__":
    unittest.main()
