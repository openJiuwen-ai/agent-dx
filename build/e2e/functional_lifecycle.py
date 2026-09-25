#!/usr/bin/env python3
"""Installed SDK acceptance for ordinary instance lifecycle behavior."""
import json
from pathlib import Path
import subprocess
import sys
import time
import uuid

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

        started = begin('lifecycle.named-create-reuses-running-owner')
        name = f'e2e-named-{uuid.uuid4().hex[:12]}'
        first = Sandbox(
            name=name,
            image=image,
            runtime='runc',
            cpu=500,
            memory=512,
            idle_timeout=0,
            detached=True,
            connection=connection,
            create_timeout=150,
        )
        named_id = first.id
        remaining.add(named_id)
        first.close()
        reopened = Sandbox(
            name=name,
            image=image,
            runtime='runc',
            cpu=500,
            memory=512,
            idle_timeout=0,
            detached=True,
            connection=connection,
            create_timeout=150,
        )
        try:
            assert reopened.id == named_id
            assert reopened.commands.run("printf 'same-owner'").stdout == 'same-owner'
        finally:
            reopened.close()
        Sandbox.delete(named_id, connection=connection)
        remaining.remove(named_id)
        _wait_deleted(named_id, connection)
        passed('lifecycle.named-create-reuses-running-owner', started)

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
        ) as context_environment:
            context_id = context_environment.id
            remaining.add(context_id)
            assert context_environment.is_running()
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

        started = begin('lifecycle.idle-with-background-command-after-client-exit')
        child_evidence = Path(output).parent / 'idle-background-client.json'
        subprocess.run(
            [sys.executable, '/opt/adx/e2e/idle_background_client.py', image, str(child_evidence)],
            check=True,
            timeout=180,
        )
        child = json.loads(child_evidence.read_text())
        background_id = child['instance_id']
        remaining.add(background_id)
        assert child['command_running_before_exit']
        # The separate SDK process has exited, but its 120-second command has
        # not. The 90-second wait proves idle cleanup does not wait for it.
        _wait_deleted(background_id, connection, timeout=90)
        remaining.remove(background_id)
        from node import catalog
        record = json.loads(catalog()['environment:' + background_id])
        assert record['result']['state'] == 'Deleted'
        assert not record['result']['resources_held']
        checks['idle_background_client_exit'] = background_id
        passed('lifecycle.idle-with-background-command-after-client-exit', started)

        result = {'status': 'passed', 'checks': checks, 'cases': cases}
        Path(output).write_text(json.dumps(result, indent=2) + '\n')
        print(json.dumps(result), flush=True)
    finally:
        for instance_id in remaining:
            try:
                Sandbox.delete(instance_id, connection=connection)
            except Exception:
                pass
