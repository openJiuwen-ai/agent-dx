"""Installed SDK acceptance for a foreground request spanning the idle threshold."""

import json
from time import monotonic

from adx_sandbox import Sandbox
from functional_lifecycle import _wait_deleted
from node import catalog


def run(connection, image, output):
    sandbox = None
    closed = False
    deleted = False
    report = {"status": "failed", "cases": [], "cleanup_errors": []}
    try:
        sandbox = Sandbox(
            image=image, runtime="runc", cpu=500, memory=512,
            idle_timeout=6, detached=True, node_id="node1",
            connection=connection, create_timeout=150,
        )
        started = monotonic()
        command = sandbox.commands.run(
            "sleep 12; printf active-request-complete", timeout=20,
        )
        elapsed = monotonic() - started
        assert command.exit_code == 0 and command.stdout == "active-request-complete", command
        assert elapsed >= 10, f"foreground request ended too soon: {elapsed:.3f}s"
        assert sandbox.is_running(), "active request did not protect the instance from idle reclaim"
        record = json.loads(catalog()["environment:" + sandbox.id])
        assert record["result"]["state"] == "Running", record["result"]
        assert record["result"]["resources_held"], record["result"]
        sandbox.close()
        closed = True
        _wait_deleted(sandbox.id, connection, timeout=90)
        terminal = json.loads(catalog()["environment:" + sandbox.id])["result"]
        assert terminal["state"] == "Deleted" and not terminal["resources_held"], terminal
        deleted = True
        report["cases"].append({
            "id": "lifecycle.active-request-prevents-idle", "status": "passed",
            "seconds": round(monotonic() - started, 3),
        })
        report["status"] = "passed"
    except Exception as error:
        report["error"] = str(error)
        raise
    finally:
        if sandbox is not None:
            if not closed:
                try:
                    sandbox.close()
                except Exception as error:
                    report["cleanup_errors"].append(str(error))
            if not deleted:
                try:
                    Sandbox.delete(sandbox.id, connection=connection)
                except Exception as error:
                    report["cleanup_errors"].append(str(error))
        if report["cleanup_errors"]:
            report["status"] = "failed"
        output.write_text(json.dumps(report, indent=2) + "\n")
        if report["cleanup_errors"] and "error" not in report:
            raise RuntimeError("idle activity cleanup failed")
