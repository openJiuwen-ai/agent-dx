"""Installed SDK acceptance for real sandboxd backend loss and restart policy."""

import json
from pathlib import Path
import subprocess
import time

from adx_sandbox import RestartPolicy, Sandbox
from node import catalog, persisted_runtime_id


SOCKET = Path("/tmp/adx-e2e/sandboxd/sandboxd.sock")


def _record(instance_id):
    return json.loads(catalog()["environment:" + instance_id])


def _backend_ids(instance_id):
    lines = subprocess.check_output(
        ["sbox", "-a", str(SOCKET), "list", "--label", "adx.environment_id=" + instance_id],
        text=True, timeout=15,
    ).splitlines()
    return [line.split()[0] for line in lines[1:] if line.strip()]


def _delete_backend(instance_id):
    identities = _backend_ids(instance_id)
    assert len(identities) == 1, (instance_id, identities)
    subprocess.run(["sbox", "-a", str(SOCKET), "delete", identities[0]],
                   check=True, timeout=30)
    return identities[0]


def _wait(predicate, timeout=90):
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        value = predicate()
        if value:
            return value
        time.sleep(.2)
    raise TimeoutError("backend loss did not converge before the E2E deadline")


def run(connection, image, output):
    instances = []
    report = {"status": "failed", "cases": [], "cleanup_errors": []}

    def create(**options):
        instance = Sandbox(
            image=image, runtime="runc", cpu=500, memory=512,
            idle_timeout=0, node_id="node1",
            connection=connection, create_timeout=150, **options,
        )
        instances.append(instance)
        assert instance.commands.run("printf runtime-exit-ready").stdout == "runtime-exit-ready"
        assert _record(instance.id)["result"]["state"] == "Running"
        return instance

    try:
        started = time.monotonic()
        never = create()
        old_backend = _delete_backend(never.id)
        failed = _wait(lambda: (
            r if (r := _record(never.id)["result"])["state"] == "Failed"
            and not r["resources_held"] else None
        ))
        assert not failed["restart_pending"], failed
        assert not _backend_ids(never.id), old_backend
        _wait(lambda: not never.is_running(), timeout=15)
        report["cases"].append({
            "id": "lifecycle.runtime-exit-never", "status": "passed",
            "seconds": round(time.monotonic() - started, 3),
            "instance_id": never.id, "old_backend": old_backend,
        })

        started = time.monotonic()
        restarted = create(restart_policy=RestartPolicy(
            max_attempts=2, initial_backoff_seconds=2, max_backoff_seconds=4,
        ))
        original_assignment = _record(restarted.id)["assignment"]
        attempts = []
        for attempt in (1, 2):
            previous = persisted_runtime_id(_record(restarted.id)["result"])
            old_backend = _delete_backend(restarted.id)
            _wait(lambda: (
                r if (r := _record(restarted.id)["result"])["state"] == "Failed"
                and r["restart_pending"] else None
            ))
            running = _wait(lambda: (
                r if (r := _record(restarted.id)["result"])["state"] == "Running"
                and persisted_runtime_id(r) != previous and r["restart_attempts"] == attempt else None
            ))
            record = _record(restarted.id)
            assert record["assignment"] == original_assignment, record["assignment"]
            identities = _backend_ids(restarted.id)
            assert len(identities) == 1 and identities[0] != old_backend, identities
            command = restarted.commands.run("printf runtime-restarted")
            assert command.exit_code == 0 and command.stdout == "runtime-restarted", command
            attempts.append({"attempt": attempt, "runtime_id": persisted_runtime_id(running),
                             "backend_id": identities[0]})
        _delete_backend(restarted.id)
        exhausted = _wait(lambda: (
            r if (r := _record(restarted.id)["result"])["state"] == "Failed"
            and not r["restart_pending"] and not r["resources_held"] else None
        ))
        assert exhausted["restart_attempts"] == 2, exhausted
        assert not _backend_ids(restarted.id), restarted.id
        _wait(lambda: not restarted.is_running(), timeout=15)
        report["cases"].append({
            "id": "lifecycle.runtime-exit-bounded-restart", "status": "passed",
            "seconds": round(time.monotonic() - started, 3),
            "instance_id": restarted.id, "attempts": attempts,
        })
        report["status"] = "passed"
    except Exception as error:
        report["error"] = str(error)
        raise
    finally:
        for instance in instances:
            try:
                instance.kill()
            except Exception as error:
                report["cleanup_errors"].append(f"{instance.id}: {error}")
            try:
                instance.close()
            except Exception as error:
                report["cleanup_errors"].append(f"{instance.id} close: {error}")
        if report["cleanup_errors"]:
            report["status"] = "failed"
        output.write_text(json.dumps(report, indent=2) + "\n")
        if report["cleanup_errors"] and "error" not in report:
            raise RuntimeError("runtime exit cleanup failed")
