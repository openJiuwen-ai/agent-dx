"""Direct sandboxd control on the caller's native XFS; never a shared backend."""

import re
from pathlib import Path


def storage_config(root, role, run_name):
    if role not in ("node1", "node2", "coordinator") or not re.fullmatch(
        r"[A-Za-z0-9_-]+", run_name
    ):
        raise ValueError("owned native control role and run name required")
    root = Path(root)
    if not root.is_absolute():
        raise ValueError("absolute native XFS mount required")
    return {"mount_root": str(root), "prefix": "native-checkpoints/" + run_name}


def verify_mount(root, info):
    rows = info.get("filesystems", [])
    if len(rows) != 1 or rows[0].get("fstype") != "xfs" or rows[0].get("target") != str(root):
        raise ValueError("explicit native XFS mountpoint required")
    return rows[0]


def verify_result(result):
    direct = result.get("direct_baseline", {})
    if (
        result.get("native_control_only") is not True
        or result.get("status") != "passed"
        or result.get("inventories") != {"node1": 0, "node2": 0}
    ):
        raise ValueError("incomplete native direct control")
    if (
        result.get("cleanup_errors") != []
        or direct.get("status") != "passed"
        or direct.get("cleanup_errors") != []
    ):
        raise ValueError("incomplete native direct control")
    samples = direct.get("samples", [])
    work = direct["workload"]
    if [s["snapshot_type"] for s in samples] != ["Full", "Incremental"]:
        raise ValueError("native control needs Full and Incremental")
    for sample, key in zip(samples, ("expected_full_sha256", "expected_dirty_sha256")):
        if (
            sample.get("checked_bytes") != work["payload_bytes"]
            or sample.get("sha256") != work[key]
            or sample.get("continuity") is not True
        ):
            raise ValueError("native control lost full workload or process continuity")
