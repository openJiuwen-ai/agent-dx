"""Discoverable opt-in assets; excluded from ordinary correctness suites."""

import json
from pathlib import Path

from fixtures.common import Case, DfsProfile, performance_parameters

TOOL = (Path(__file__).resolve().parents[2] / "tools/io_workload.py").read_text()


def execute(c, operation):
    size, request = performance_parameters(c.config)
    before = {role: c.health(role, "/metrics") for role in ("A", "B")}
    path = c.workspace("A") + "/measurement"
    if operation == "read":
        c.python("A", TOOL, "write", path, "--bytes", size, "--request-bytes", request, timeout=900)
    role = "B" if operation == "read" else "A"
    path = c.workspace(role) + "/measurement"
    output = c.python(role, TOOL, operation, path, "--bytes", size, "--request-bytes", request,
                      "--measure", timeout=900)
    measurement = json.loads(output["stdout"])
    assert measurement["bytes"] == size and measurement["performance_acceptance"] == "NOT_EVALUATED"
    c.record("performance.workload", measurement)
    c.record("performance.node_metric_snapshots", {
        "before": before, "after": {r: c.health(r, "/metrics") for r in ("A", "B")},
        "scope": "includes fixture preparation and validation; not an exclusive timed CPU or I/O counter delta",
        "source_cache_state": "writer-hot", "baseline_comparison_performed": False,
    })


def write(c):
    execute(c, "write")


def read(c):
    execute(c, "read")


CASES = (
    Case("dfs.performance.sequential_write", ("local-performance",), "dfs", (), write,
         profile=DfsProfile(), purpose="performance"),
    Case("dfs.performance.sequential_read", ("local-performance",), "dfs", (), read,
         profile=DfsProfile(), purpose="performance"),
)
