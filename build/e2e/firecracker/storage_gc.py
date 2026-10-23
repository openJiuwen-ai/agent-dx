"""Observe real Meta authority and chunk files, without inferring GC from directory deletion."""

import errno
import json
import os
import time
from pathlib import Path
from urllib.request import ProxyHandler, build_opener


def file_usage(paths):
    sizes = []
    for path in paths:
        try:
            sizes.append(path.stat().st_size)
        except FileNotFoundError:
            # A concurrent completed collection/rename is ordinary progress.
            continue
    return {"files": len(sizes), "bytes": sum(sizes)}


class GcProbe:
    def __init__(self, root, port, evidence):
        self.root = Path(root)
        self.port = int(port)
        self.evidence = Path(evidence)
        self.history = []

    def capture(self, phase):
        opener = build_opener(ProxyHandler({}))
        with opener.open(f"http://127.0.0.1:{self.port}/v1/store", timeout=10) as response:
            meta = json.loads(response.read())
        physical = {}
        for node in ("node-a", "node-b", "node-c"):
            directory = self.root / node / "state/dfs"
            chunks = file_usage((directory / "chunks").glob("*"))
            physical[node] = {
                "chunks": chunks["files"],
                "bytes": chunks["bytes"],
                "catalog_bytes": file_usage(
                    path for path in directory.glob("catalog.*") if path.is_file()
                )["bytes"],
            }
        meta_disk = file_usage(
            path for path in (self.root / "meta/state").rglob("*") if path.is_file()
        )
        row = {
            "phase": phase,
            "time": time.time(),
            "meta": meta,
            "physical": physical,
            "meta_disk": meta_disk,
        }
        self.history.append(row)
        (self.evidence / "gc-observations.json").write_text(json.dumps(self.history, indent=2))
        return row

    def settle(self, phase, predicate, seconds=240):
        deadline = time.monotonic() + seconds
        consecutive = 0
        while time.monotonic() < deadline:
            row = self.capture(phase)
            gc = row["meta"].get("gc", {})
            ready = (
                not gc.get("pending_chunks", 0)
                and not gc.get("waiting_nodes", [])
                and predicate(row)
            )
            consecutive = consecutive + 1 if ready else 0
            if consecutive >= 3:
                return row
            time.sleep(5)
        raise AssertionError(("GC did not converge", phase, self.history[-1]))


def totals(row):
    return {
        field: sum(node[field] for node in row["physical"].values())
        for field in ("chunks", "bytes", "catalog_bytes")
    }


def plateau(previous, current):
    before, after = totals(previous), totals(current)
    kinds = (
        "dfs_inode",
        "dfs_dentry",
        "dfs_file_version",
        "dfs_layout_root",
        "dfs_reference",
        "dfs_write_lease",
        "dfs_chunk",
        "dfs_copy",
        "dfs_placement",
    )
    return (
        after["bytes"] <= before["bytes"] * 1.25 + 1024**2
        and after["chunks"] <= before["chunks"] * 1.25 + 16
        and after["catalog_bytes"] <= before["catalog_bytes"] * 1.25 + 256 * 1024
        and all(
            current["meta"]["entities"].get(kind, 0)
            <= previous["meta"]["entities"].get(kind, 0) * 1.25 + 16
            for kind in kinds
        )
        and current["meta"]["revision"] - current["meta"]["watch_floor"] <= 80
        and current["meta"]["requests"] <= max(200, previous["meta"]["requests"] * 1.25)
    )


def reclaimed(baseline, current):
    initial, final = totals(baseline), totals(current)
    kinds = (
        "dfs_inode",
        "dfs_dentry",
        "dfs_reference",
        "dfs_write_lease",
        "dfs_file_version",
        "dfs_layout_root",
        "dfs_chunk",
        "dfs_copy",
        "dfs_placement",
    )
    return (
        final["bytes"] <= initial["bytes"] + 1024**2
        and final["chunks"] <= initial["chunks"] + 4
        and final["catalog_bytes"] <= initial["catalog_bytes"] + 64 * 1024
        and all(
            current["meta"]["entities"].get(kind, 0) <= baseline["meta"]["entities"].get(kind, 0)
            for kind in kinds
        )
        and current["meta"]["requests"] <= 200
        and current["meta"]["revision"] - current["meta"]["watch_floor"] <= 80
    )


def close_fenced_descriptor(descriptor):
    # Linux closes this descriptor even when FUSE release reports the dead client.
    # Retrying close could close a subsequently reused descriptor.
    try:
        os.close(descriptor)
    except OSError as error:
        if error.errno not in (errno.ENOTCONN, errno.EIO):
            raise


def bootstrap_only(row):
    # CheckpointStore keeps one linked three-byte store-id file. Everything
    # created by prior VMs/captures must disappear, including writer leases.
    physical = totals(row)
    entities = row["meta"]["entities"]
    return (
        physical["bytes"] <= 9
        and physical["chunks"] <= 3
        and entities.get("dfs_file_version", 0) <= 1
        and entities.get("dfs_layout_root", 0) <= 1
        and entities.get("dfs_reference", 0) == 0
        and entities.get("dfs_write_lease", 0) == 0
    )
