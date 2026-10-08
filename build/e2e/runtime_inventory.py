"""Assertions for the standard two-node, runc-only acceptance fixture."""

import json


def require_runc_only_inventory(records):
    """Require live sandboxd inventories, not a configured or inferred class."""
    inventories = {}
    for node_id in ("node1", "node2"):
        key = f"node:{node_id}"
        assert key in records, f"{node_id} is missing from the control catalog"
        record = json.loads(records[key])
        node = record["node"]
        session = record["session"]
        assert node.get("available") and session.get("routable"), (
            f"{node_id} is not available and routable"
        )
        classes = node.get("runtime_classes")
        assert isinstance(classes, list) and classes, (
            f"{node_id} has no reported sandboxd runtime inventory"
        )
        assert set(classes) == {"runc"}, (
            f"{node_id} reported unexpected runtime classes: {classes}"
        )
        inventories[node_id] = classes
    return inventories


def require_unique_runtime_node(records, runtime_class):
    """Find the one live node advertising a runtime in a heterogeneous fixture."""
    supported = []
    for node_id in ("node1", "node2"):
        key = f"node:{node_id}"
        assert key in records, f"{node_id} is missing from the control catalog"
        record = json.loads(records[key])
        node = record["node"]
        session = record["session"]
        assert node.get("available") and session.get("routable"), (
            f"{node_id} is not available and routable"
        )
        classes = node.get("runtime_classes")
        assert isinstance(classes, list) and classes, (
            f"{node_id} has no reported sandboxd runtime inventory"
        )
        if runtime_class in classes:
            supported.append(node_id)
    assert len(supported) == 1, (
        f"{runtime_class} must be advertised by exactly one live node; got {supported}"
    )
    return supported[0]


def require_unassigned_create(records, instance_id):
    """A runtime rejected by placement cannot retain ownership or resources."""
    raw = records.get(f"environment:{instance_id}")
    if raw is None:
        return
    record = json.loads(raw)
    result = record.get("result") or {}
    assert not result.get("resources_held") and not record.get("assignment"), (
        f"{instance_id} remained assigned or held resources"
    )
