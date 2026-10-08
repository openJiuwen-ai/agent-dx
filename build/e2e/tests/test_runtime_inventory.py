"""Contract checks for the runtime inventory used by placement acceptance."""

import importlib.util
import json
from pathlib import Path
import unittest


ROOT = Path(__file__).resolve().parents[1]
spec = importlib.util.spec_from_file_location("runtime_inventory", ROOT / "runtime_inventory.py")
runtime_inventory = importlib.util.module_from_spec(spec)
spec.loader.exec_module(runtime_inventory)


def records(*, first=("runc",), second=("runc",)):
    return {
        f"node:{node_id}": json.dumps(
            {
                "node": {"id": node_id, "available": True, "runtime_classes": list(classes)},
                "session": {"routable": True},
            }
        )
        for node_id, classes in (("node1", first), ("node2", second))
    }


class RuntimeInventoryTests(unittest.TestCase):
    def test_both_nodes_must_advertise_the_configured_runc_runtime(self):
        self.assertEqual(
            runtime_inventory.require_runc_only_inventory(records()),
            {"node1": ["runc"], "node2": ["runc"]},
        )

    def test_missing_inventory_does_not_count_as_runtime_support(self):
        current = records()
        current["node:node2"] = json.dumps(
            {"node": {"id": "node2", "available": True}, "session": {"routable": True}}
        )
        with self.assertRaisesRegex(AssertionError, "node2.*runtime inventory"):
            runtime_inventory.require_runc_only_inventory(current)

    def test_unexpected_runtime_or_unroutable_node_fails_fixture(self):
        with self.assertRaisesRegex(AssertionError, "node2.*unexpected"):
            runtime_inventory.require_runc_only_inventory(records(second=("runc", "runsc")))
        current = records()
        current["node:node1"] = json.dumps(
            {
                "node": {"id": "node1", "available": True, "runtime_classes": ["runc"]},
                "session": {"routable": False},
            }
        )
        with self.assertRaisesRegex(AssertionError, "node1.*routable"):
            runtime_inventory.require_runc_only_inventory(current)

    def test_rejected_runtime_cannot_leave_a_held_assignment(self):
        current = records()
        runtime_inventory.require_unassigned_create(current, "default-rejected")
        current["environment:default-rejected"] = json.dumps(
            {"result": {"state": "Failed", "resources_held": False}}
        )
        runtime_inventory.require_unassigned_create(current, "default-rejected")
        current["environment:default-rejected"] = json.dumps(
            {
                "assignment": {"node_id": "node1"},
                "result": {"state": "Running", "resources_held": True},
            }
        )
        with self.assertRaisesRegex(AssertionError, "held|assigned"):
            runtime_inventory.require_unassigned_create(current, "default-rejected")

    def test_heterogeneous_runtime_requires_one_live_capable_node(self):
        current = records(first=("runc",), second=("runc", "runsc"))
        self.assertEqual(runtime_inventory.require_unique_runtime_node(current, "runsc"), "node2")
        with self.assertRaisesRegex(AssertionError, "exactly one"):
            runtime_inventory.require_unique_runtime_node(records(), "runsc")
        with self.assertRaisesRegex(AssertionError, "exactly one"):
            runtime_inventory.require_unique_runtime_node(
                records(first=("runc", "runsc"), second=("runc", "runsc")), "runsc"
            )
        current["node:node2"] = json.dumps({
            "node": {"id": "node2", "available": False, "runtime_classes": ["runsc"]},
            "session": {"routable": True},
        })
        with self.assertRaisesRegex(AssertionError, "available"):
            runtime_inventory.require_unique_runtime_node(current, "runsc")


if __name__ == "__main__":
    unittest.main()
