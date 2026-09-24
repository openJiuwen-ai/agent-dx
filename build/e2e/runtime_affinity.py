"""Installed SDK acceptance for placement on a real heterogeneous runtime inventory."""

import json
import time

from adx_sandbox import Sandbox
from node import catalog
from runtime_inventory import require_unique_runtime_node


def run(connection, image, output):
    started = time.monotonic()
    runtime_class = "runsc"
    sandbox = None
    deleted = False
    report = {"status": "failed", "cases": [], "cleanup_errors": []}
    try:
        expected_node = require_unique_runtime_node(catalog(), runtime_class)
        sandbox = Sandbox(
            image=image, runtime=runtime_class, cpu=250, memory=256,
            idle_timeout=0, connection=connection, create_timeout=150,
        )
        record = json.loads(catalog()["environment:" + sandbox.id])
        assert record["result"]["state"] == "Running", record["result"]
        assert record["assignment"]["node_id"] == expected_node, record["assignment"]
        assert record["spec"]["runtime_class"] == runtime_class, record["spec"]
        command = sandbox.commands.run("printf runtime-affinity-ready")
        assert command.exit_code == 0 and command.stdout == "runtime-affinity-ready"
        report["cases"].append({
            "id": "runtime-affinity.unique-runsc-node", "status": "passed",
            "seconds": round(time.monotonic() - started, 3),
            "instance_id": sandbox.id, "node_id": expected_node,
        })
        sandbox.kill()
        deleted = True
        terminal = json.loads(catalog()["environment:" + sandbox.id])["result"]
        assert terminal["state"] == "Deleted" and not terminal["resources_held"], terminal
        report["status"] = "passed"
    except Exception as error:
        report["error"] = str(error)
        raise
    finally:
        if sandbox is not None:
            try:
                if not deleted:
                    sandbox.kill()
            except Exception as error:
                report["cleanup_errors"].append(str(error))
            finally:
                sandbox.close()
        if report["cleanup_errors"]:
            report["status"] = "failed"
        output.write_text(json.dumps(report, indent=2) + "\n")
        if report["cleanup_errors"] and "error" not in report:
            raise RuntimeError("runtime affinity cleanup failed")
