#!/usr/bin/env python3
"""Installed SDK acceptance for ordinary instance lifecycle behavior."""
import json
from pathlib import Path
import time

from adx_sandbox import Sandbox, SandboxNotFound


def _wait_deleted(instance_id, connection, timeout=90):
    deadline = time.monotonic() + timeout
    last_error = None
    while time.monotonic() < deadline:
        try:
            handle = Sandbox.from_id(instance_id, connection=connection)
        except SandboxNotFound:
            return
        except RuntimeError as error:
            if 'is not running' in str(error):
                return
            last_error = error
        except Exception as error:
            last_error = error
        else:
            handle.close()
        time.sleep(1)
    raise TimeoutError(f"instance {instance_id} was not reclaimed: {last_error}")


def run(connection, image, output):
    checks = {}
    cases = []
    remaining = set()

    def begin(case_id):
        print(f'[SDK CASE RUN] {case_id}', flush=True)
        return time.monotonic()

    def passed(case_id, started):
        seconds = round(time.monotonic() - started, 3)
        cases.append({'id': case_id, 'status': 'passed', 'seconds': seconds})
        print(f'[SDK CASE PASS] {case_id} ({seconds:.3f}s)', flush=True)

    try:
        started = begin('lifecycle.detached-close-reattach-delete')
        detached = Sandbox(
            image=image,
            runtime='runc',
            cpu=500,
            memory=512,
            idle_timeout=0,
            detached=True,
            node_id='node1',
            connection=connection,
            create_timeout=150,
        )
        detached_id = detached.id
        remaining.add(detached_id)
        detached.kill()
        attached = Sandbox.from_id(detached_id, connection=connection)
        try:
            result = attached.commands.run("printf 'reattached'")
            assert result.exit_code == 0 and result.stdout == 'reattached'
        finally:
            attached.close()
        Sandbox.delete(detached_id, connection=connection)
        remaining.remove(detached_id)
        _wait_deleted(detached_id, connection)
        checks['detached_reattach_and_delete'] = detached_id
        passed('lifecycle.detached-close-reattach-delete', started)

        started = begin('lifecycle.attached-close-preserves-instance')
        attached = Sandbox(
            image=image,
            runtime='runc',
            cpu=500,
            memory=512,
            idle_timeout=0,
            node_id='node1',
            connection=connection,
            create_timeout=150,
        )
        attached_id = attached.id
        remaining.add(attached_id)
        attached.close()
        reopened = Sandbox.from_id(attached_id, connection=connection)
        try:
            result = reopened.commands.run("printf 'close-preserved'")
            assert result.exit_code == 0 and result.stdout == 'close-preserved'
        finally:
            reopened.close()
        # A handle obtained through from_id is detached from ownership by
        # design. Delete explicitly after proving close preserved the remote.
        Sandbox.delete(attached_id, connection=connection)
        remaining.remove(attached_id)
        _wait_deleted(attached_id, connection)
        passed('lifecycle.attached-close-preserves-instance', started)

        started = begin('lifecycle.context-manager-deletes')
        context_id = ''
        with Sandbox(
            image=image,
            runtime='runc',
            cpu=500,
            memory=512,
            idle_timeout=0,
            node_id='node1',
            connection=connection,
            create_timeout=150,
        ) as context_capsule:
            context_id = context_capsule.id
            remaining.add(context_id)
            assert context_capsule.is_running()
        remaining.remove(context_id)
        _wait_deleted(context_id, connection)
        passed('lifecycle.context-manager-deletes', started)

        started = begin('lifecycle.idle-timeout-reclaims')
        idle = Sandbox(
            image=image,
            runtime='runc',
            cpu=500,
            memory=512,
            idle_timeout=6,
            node_id='node1',
            connection=connection,
            create_timeout=150,
        )
        idle_id = idle.id
        remaining.add(idle_id)
        assert idle.commands.run("printf 'idle-start'").stdout == 'idle-start'
        idle.close()
        _wait_deleted(idle_id, connection)
        remaining.remove(idle_id)
        checks['idle_timeout_reclamation'] = idle_id
        passed('lifecycle.idle-timeout-reclaims', started)

        result = {'status': 'passed', 'checks': checks, 'cases': cases}
        Path(output).write_text(json.dumps(result, indent=2) + '\n')
        print(json.dumps(result), flush=True)
    finally:
        for instance_id in remaining:
            try:
                Sandbox.delete(instance_id, connection=connection)
            except Exception:
                pass
