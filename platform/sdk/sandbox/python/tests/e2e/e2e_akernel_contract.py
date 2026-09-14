"""Live AKernel-compatibility regression for the adx sandbox layer.

The test requires a deployed Frontend, RRT sandbox runtime, and an image already
available to the node-side container runtime. It validates the public SDK
contract together with the Frontend wire path, including resource discovery,
required NODE_ID affinity, command/file direct access, real PTY WebSocket
control frames, instance status, and unified ID deletion.

Environment:
  ADX_SERVER_ADDRESS   Frontend/gateway host:port.
  ADX_GATEWAY_ADDRESS  Optional PTY gateway host:port; defaults to the server.
  ADX_TOKEN            Any non-empty value when auth is disabled.
  ADX_TLS              ``0`` for a plain HTTP development cluster.
  ADX_SANDBOX_IMAGE    Node-local sandbox image (default ``aio-adx-runtime:latest``).
  ADX_SANDBOX_MEMORY   Scheduling memory in MB (default ``4096``).
  ADX_EXPECT_NODE_ID   Optional node that the sandbox must run on.
  ADX_E2E_RESULT       Optional JSON result output path.
"""

from __future__ import annotations

import json
import os
import sys
import time
from pathlib import Path

os.environ.setdefault("ADX_TLS", "0")
os.environ.setdefault("ADX_TOKEN", "ci")


def _require(name: str) -> str:
    value = os.environ.get(name, "").strip()
    if not value:
        print(f"ERROR: {name} is required", file=sys.stderr)
        raise SystemExit(2)
    return value


server = _require("ADX_SERVER_ADDRESS")
os.environ.setdefault("ADX_GATEWAY_ADDRESS", server)
image = os.environ.get("ADX_SANDBOX_IMAGE", "aio-adx-runtime:latest").strip()
memory = int(os.environ.get("ADX_SANDBOX_MEMORY", "4096"))
expected_node = os.environ.get("ADX_EXPECT_NODE_ID", "").strip()
result_path = os.environ.get("ADX_E2E_RESULT", "").strip()

from adx_sandbox import Sandbox, resources

passed: list[str] = []
failed: list[str] = []
details: dict[str, object] = {
    "server": server,
    "image": image,
    "memory": memory,
    "expected_node_id": expected_node or None,
}


def check(name: str, condition: bool, detail: object = None) -> None:
    target = passed if condition else failed
    target.append(name)
    if detail is not None:
        details[name] = detail
    suffix = "" if detail is None else f"  {detail}"
    print(f"[{'PASS' if condition else 'FAIL'}] {name}{suffix}")


def finish() -> None:
    result = {
        "passed": passed,
        "failed": failed,
        "details": details,
    }
    rendered = json.dumps(result, ensure_ascii=False, indent=2, sort_keys=True)
    print(rendered)
    if result_path:
        Path(result_path).write_text(rendered + "\n", encoding="utf-8")


sandbox: Sandbox | None = None
try:
    nodes = resources()
    details["resources"] = [
        {
            "id": node.id,
            "status": node.status,
            "capacity": dict(node.capacity),
            "allocatable": dict(node.allocatable),
            "labels": dict(node.labels),
        }
        for node in nodes
    ]
    check("resources returns nodes", bool(nodes), [node.id for node in nodes])
    if not expected_node and nodes:
        expected_node = nodes[0].id
        details["expected_node_id"] = expected_node

    unique_name = f"akernel-e2e-{int(time.time())}"
    sandbox = Sandbox(
        image=image,
        runtime="runsc",
        name=unique_name,
        cwd="/tmp",
        node_id=expected_node or None,
        memory=memory,
        create_timeout=180,
        schedule_timeout=120,
    )
    check("create", bool(sandbox.id), sandbox.id)

    summary = sandbox._client.instance_info(sandbox.id)
    details["instance_summary"] = summary
    actual_node = str(summary.get("node_id") or summary.get("nodeId") or "")
    check("instance status running", summary.get("status") == "running", summary)
    if expected_node:
        check(
            "required NODE_ID affinity",
            actual_node == expected_node,
            {"expected": expected_node, "actual": actual_node},
        )

    info = sandbox.get_info()
    check(
        "get_info",
        info.id == sandbox.id and info.state == "running",
        {"id": info.id, "state": info.state},
    )

    written = sandbox.files.write("/tmp/akernel-e2e.txt", b"direct-bytes")
    check("files.write bytes", written.size == 12, written.size)
    check(
        "files.read bytes",
        sandbox.files.read("/tmp/akernel-e2e.txt", format="bytes")
        == b"direct-bytes",
    )
    check(
        "frontend direct path remains active",
        sandbox._client._direct_disabled is False,
    )

    command = sandbox.commands.run("pwd && printf command-ok", timeout=30)
    check(
        "command cwd inheritance",
        command.exit_code == 0
        and command.stdout.startswith("/tmp")
        and "command-ok" in command.stdout,
        {"exit_code": command.exit_code, "stdout": command.stdout},
    )

    output: list[bytes] = []
    with sandbox.pty.create(
        ["/bin/sh", "-lc", "pwd; printf pty-ok"],
        on_data=output.append,
        timeout=60,
    ) as session:
        session.resize(rows=32, cols=100)
        exit_code = session.wait(timeout=60)
        session_id = session.session_id
    pty_output = b"".join(output)
    check(
        "PTY control/data/exit",
        exit_code == 0 and b"pty-ok" in pty_output,
        {
            "session_id": session_id,
            "exit_code": exit_code,
            "output": pty_output.decode("utf-8", errors="replace"),
        },
    )
except Exception as exc:  # noqa: BLE001
    check("unexpected exception", False, repr(exc))
finally:
    if sandbox is not None:
        sandbox_id = sandbox.id
        try:
            sandbox.kill()
            sandbox.kill()
            check("unified ID delete is idempotent locally", True, sandbox_id)
        except Exception as exc:  # noqa: BLE001
            check("unified ID delete is idempotent locally", False, repr(exc))
    finish()

if failed:
    raise SystemExit(1)
