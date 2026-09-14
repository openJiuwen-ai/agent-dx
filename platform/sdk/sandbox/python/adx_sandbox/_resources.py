"""Private resource discovery helpers backed by the scheduler query API."""

from typing import Any, Iterable, List, Mapping, Optional

from ._transport import SandboxClient
from .types import ConnectionConfig, NodeInfo


def _resource_values(value: object) -> Mapping[str, float]:
    if not isinstance(value, Mapping):
        return {}
    resources = value.get("resources")
    if not isinstance(resources, Mapping):
        return {}
    result = {}
    for name, resource in resources.items():
        if not isinstance(resource, Mapping):
            continue
        scalar = resource.get("scalar")
        if not isinstance(scalar, Mapping):
            continue
        raw_value = scalar.get("value")
        if isinstance(raw_value, (int, float)) and not isinstance(raw_value, bool):
            result[str(name)] = float(raw_value)
    return result


def _node_labels(value: object) -> Mapping[str, Any]:
    if not isinstance(value, Mapping):
        return {}
    result = {}
    for name, counter in value.items():
        if not isinstance(counter, Mapping):
            continue
        items = counter.get("items")
        if not isinstance(items, Mapping):
            continue
        keys = [str(item) for item in items]
        result[str(name)] = keys[0] if len(keys) == 1 else dict(items)
    return result


def _coerce_node(node_id: str, item: Mapping[str, object]) -> NodeInfo:
    try:
        raw_status = item.get("status", 0)
        if isinstance(raw_status, bool) or not isinstance(
            raw_status, (int, float, str, bytes, bytearray)
        ):
            raise TypeError("node status must be an integer-compatible value")
        return NodeInfo(
            id=str(item.get("id") or node_id),
            status=int(raw_status),
            capacity=_resource_values(item.get("capacity")),
            allocatable=_resource_values(item.get("allocatable")),
            labels=_node_labels(item.get("nodeLabels")),
        )
    except Exception as exc:
        raise ValueError(f"invalid node item: {item!r}") from exc


def resources(
    *, connection: Optional[ConnectionConfig] = None
) -> List[NodeInfo]:
    """Return tenant-visible schedulable nodes from sandbox v1.

    This function uses ``SandboxClient.resources()`` and decodes the ``items``
    payload into strongly-typed :class:`NodeInfo` objects.
    """

    if connection is not None and not isinstance(connection, ConnectionConfig):
        raise TypeError("connection must be a ConnectionConfig or None")
    if connection is None:
        client = SandboxClient()
    else:
        client = SandboxClient(connection=connection)
    try:
        payload = client.resources()
    finally:
        client.close()

    items: Iterable[tuple[object, object]] = []
    resource = payload.get("resource") if isinstance(payload, dict) else None
    if isinstance(resource, Mapping):
        fragments = resource.get("fragment")
        if isinstance(fragments, Mapping):
            items = fragments.items()

    return [
        _coerce_node(str(node_id), item)
        for node_id, item in items
        if isinstance(item, Mapping)
    ]
