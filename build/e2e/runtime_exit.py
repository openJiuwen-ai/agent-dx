"""Installed SDK acceptance for real sandboxd backend loss and restart policy."""

import json
import os
import shutil
import subprocess
import time
from pathlib import Path

from adx_sandbox import RestartPolicy, Sandbox
from node import catalog, persisted_runtime_id


def _executable(name, environment=None, cwd=None):
    """Resolve an external command using the child's execution environment."""
    directory = os.getcwd() if cwd is None else os.path.abspath(cwd)
    search_path = os.pathsep.join(
        os.path.abspath(os.path.join(directory, entry)) for entry in os.get_exec_path(environment)
    )
    executable = shutil.which(name, path=search_path)
    if executable is None:
        raise FileNotFoundError(f"required executable not found: {name}")
    return os.path.abspath(executable)


SOCKET = Path("/tmp/adx-e2e/sandboxd/sandboxd.sock")


def _record(instance_id):
    return json.loads(catalog()["environment:" + instance_id])


def _backend_ids(instance_id):
    lines = subprocess.check_output(
        [_executable("sbox"), "-a", str(SOCKET), "list", "--label", "adx.environment_id=" + instance_id],
        text=True,
        timeout=15,
    ).splitlines()
    return [line.split()[0] for line in lines[1:] if line.strip()]


def _delete_backend(instance_id):
    identities = _backend_ids(instance_id)
    if not (len(identities) == 1):
        raise AssertionError((instance_id, identities))
    subprocess.run([_executable("sbox"), "-a", str(SOCKET), "delete", identities[0]], check=True, timeout=30)
    return identities[0]


def _wait(predicate, timeout=90):
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        value = predicate()
        if value:
            return value
        time.sleep(0.2)
    raise TimeoutError("backend loss did not converge before the E2E deadline")


def run(connection, image, output):
    instances = []
    report = {"status": "failed", "cases": [], "cleanup_errors": []}

    def create(**options):
        instance = Sandbox(
            image=image,
            runtime="runc",
            cpu=500,
            memory=512,
            idle_timeout=0,
            node_id="node1",
            connection=connection,
            create_timeout=150,
            **options,
        )
        instances.append(instance)
        if not (instance.commands.run("printf runtime-exit-ready").stdout == "runtime-exit-ready"):
            raise AssertionError()
        if not (_record(instance.id)["result"]["state"] == "Running"):
            raise AssertionError()
        return instance

    try:
        started = time.monotonic()
        never = create()
        old_backend = _delete_backend(never.id)
        failed = _wait(
            lambda: r if (r := _record(never.id)["result"])["state"] == "Failed" and not r["resources_held"] else None
        )
        if not (not failed["restart_pending"]):
            raise AssertionError(failed)
        if not (not _backend_ids(never.id)):
            raise AssertionError(old_backend)
        _wait(lambda: not never.is_running(), timeout=15)
        report["cases"].append(
            {
                "id": "lifecycle.runtime-exit-never",
                "status": "passed",
                "seconds": round(time.monotonic() - started, 3),
                "instance_id": never.id,
                "old_backend": old_backend,
            }
        )

        started = time.monotonic()
        restarted = create(
            restart_policy=RestartPolicy(
                max_attempts=2,
                initial_backoff_seconds=2,
                max_backoff_seconds=4,
            )
        )
        original_assignment = _record(restarted.id)["assignment"]
        attempts = []
        for attempt in (1, 2):
            previous = persisted_runtime_id(_record(restarted.id)["result"])
            old_backend = _delete_backend(restarted.id)
            _wait(
                lambda: (
                    r if (r := _record(restarted.id)["result"])["state"] == "Failed" and r["restart_pending"] else None
                )
            )
            running = _wait(
                lambda previous=previous, attempt=attempt: (
                    r
                    if (r := _record(restarted.id)["result"])["state"] == "Running"
                    and persisted_runtime_id(r) != previous
                    and r["restart_attempts"] == attempt
                    else None
                )
            )
            record = _record(restarted.id)
            if not (record["assignment"] == original_assignment):
                raise AssertionError(record["assignment"])
            identities = _backend_ids(restarted.id)
            if not (len(identities) == 1 and identities[0] != old_backend):
                raise AssertionError(identities)
            command = restarted.commands.run("printf runtime-restarted")
            if not (command.exit_code == 0 and command.stdout == "runtime-restarted"):
                raise AssertionError(command)
            attempts.append(
                {"attempt": attempt, "runtime_id": persisted_runtime_id(running), "backend_id": identities[0]}
            )
        _delete_backend(restarted.id)
        exhausted = _wait(
            lambda: (
                r
                if (r := _record(restarted.id)["result"])["state"] == "Failed"
                and not r["restart_pending"]
                and not r["resources_held"]
                else None
            )
        )
        if not (exhausted["restart_attempts"] == 2):
            raise AssertionError(exhausted)
        if not (not _backend_ids(restarted.id)):
            raise AssertionError(restarted.id)
        _wait(lambda: not restarted.is_running(), timeout=15)
        report["cases"].append(
            {
                "id": "lifecycle.runtime-exit-bounded-restart",
                "status": "passed",
                "seconds": round(time.monotonic() - started, 3),
                "instance_id": restarted.id,
                "attempts": attempts,
            }
        )
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
