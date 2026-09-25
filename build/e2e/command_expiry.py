"""Distinguish an expired Execd command from an ID that never existed."""

import json
import time
import uuid


def run(connection, image, output):
    from adx_sandbox import CommandExpired, CommandNotFound, Sandbox
    from functional_lifecycle import _wait_deleted
    from node import catalog, labeled_backend

    started = time.monotonic()
    report = {'status': 'failed', 'cases': [], 'cleanup_errors': []}
    name = 'command-expiry-' + uuid.uuid4().hex[:12]
    command_id = 'expired-' + uuid.uuid4().hex[:12]
    missing_id = 'never-seen-' + uuid.uuid4().hex[:12]
    sandbox = None
    deleted = False
    try:
        sandbox = Sandbox(
            name=name, image=image, runtime='runc', node_id='node1',
            cpu=500, memory=512, idle_timeout=0, detached=True,
            connection=connection, create_timeout=150,
        )
        initial = json.loads(catalog()['environment:' + sandbox.id])
        backend = labeled_backend(sandbox.id)
        assert len(backend) == 1, backend

        try:
            sandbox.commands.get(missing_id)
        except CommandNotFound as error:
            assert not isinstance(error, CommandExpired), error
            assert error.sandbox_id == sandbox.id and error.command_id == missing_id
        else:
            raise AssertionError('unknown command did not return CommandNotFound')

        finished = sandbox.commands.run(
            'printf expired-command', background=True, command_id=command_id,
        ).wait(timeout=20)
        assert finished.exit_code == 0 and finished.stdout == 'expired-command', finished
        # This case injects a one-second terminal-record TTL on node1 only.
        time.sleep(2)
        try:
            sandbox.commands.get(command_id)
        except CommandExpired as error:
            assert error.sandbox_id == sandbox.id and error.command_id == command_id
        else:
            raise AssertionError('expired command did not return CommandExpired')

        alive = sandbox.commands.run('printf runtime-alive')
        assert alive.exit_code == 0 and alive.stdout == 'runtime-alive', alive
        final = json.loads(catalog()['environment:' + sandbox.id])
        assert final['assignment']['generation'] == initial['assignment']['generation']
        assert labeled_backend(sandbox.id) == backend
        Sandbox.delete(sandbox.id, connection=connection)
        deleted = True
        _wait_deleted(sandbox.id, connection, timeout=60)
        deleted_result = json.loads(catalog()['environment:' + sandbox.id])['result']
        assert deleted_result['state'] == 'Deleted' and not deleted_result['resources_held']
        assert not labeled_backend(sandbox.id)

        report['status'] = 'passed'
        report['cases'].append({
            'id': 'command.expired-result', 'status': 'passed',
            'seconds': round(time.monotonic() - started, 3),
            'instance_id': sandbox.id,
            'expired_command_id': command_id,
            'missing_command_id': missing_id,
            'backend': backend[0],
        })
        return report
    except Exception as error:
        report['error'] = str(error)
        raise
    finally:
        if sandbox is not None:
            if not deleted:
                try:
                    Sandbox.delete(sandbox.id, connection=connection)
                except Exception as error:
                    report['cleanup_errors'].append(str(error))
            sandbox.close()
        if report['cleanup_errors']:
            report['status'] = 'failed'
        output.write_text(json.dumps(report, indent=2) + '\n')
