"""Network addresses shared by the ADX E2E node fixtures."""


def sandboxd_gateway_range(node):
    """Return the bridge gateway and prefix for one isolated test node."""
    if node not in ("node1", "node2"):
        raise ValueError(f"unknown E2E node: {node}")
    third_octet = 16 if node == "node1" else 32
    return f"10.231.{third_octet}.1/20"
