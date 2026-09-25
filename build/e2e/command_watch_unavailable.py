"""Keep HTTP queries live while command Watch remains unavailable."""

import json
import time
import uuid

from command_response_cut import CommandResponseCutProxy


def run(connection, image, output, secrets, *, query_unavailable=False):
    from adx_sandbox import (
        CommandStatus, CommandUnavailable, ConnectionConfig, Sandbox, SandboxError,
    )
    from functional_lifecycle import _wait_deleted
    from node import catalog, labeled_backend

    started = time.monotonic()
    report = {'status': 'failed', 'cases': [], 'cleanup_errors': []}
    name = 'watch-down-' + uuid.uuid4().hex[:12]
    command_id = 'watch-down-' + uuid.uuid4().hex[:12]
    sandbox = None
    attached = None
    recovered = None
    deleted = False
    proxy = CommandResponseCutProxy(
        'https://127.0.0.1:8443', certificate=secrets / 'tls/ingress.pem',
        private_key=secrets / 'tls/ingress.key', ca=secrets / 'tls/ca.pem',
        cut_start=False, reject_watch=True,
        reject_get_on_watch=query_unavailable,
    )
    try:
        sandbox = Sandbox(
            name=name, image=image, runtime='runc', node_id='node1',
            cpu=500, memory=512, idle_timeout=0, detached=True,
            connection=connection, create_timeout=150,
        )
        initial = json.loads(catalog()['environment:' + sandbox.id])
        backend = labeled_backend(sandbox.id)
        assert len(backend) == 1, backend

        with proxy:
            proxied = ConnectionConfig(
                server_address=f'127.0.0.1:{proxy.port}',
                token=connection.token, use_tls=True, verify_tls=True,
            )
            attached = Sandbox.from_id(sandbox.id, connection=proxied)
            handle = attached.commands.run(
                'sleep 120', background=True, command_id=command_id,
            )
            assert handle.id == command_id
            assert handle.poll() == CommandStatus.RUNNING
            try:
                handle.wait(timeout=45)
            except CommandUnavailable as error:
                assert error.sandbox_id == sandbox.id, error
                assert error.command_id == command_id, error
                assert 'command watch unavailable' in str(error), error
            else:
                raise AssertionError('SDK did not report the exhausted Watch reconnect budget')
            if query_unavailable:
                try:
                    handle.poll()
                except SandboxError as error:
                    assert error.code == 'OUTCOME_UNKNOWN', error
                    assert error.retry == 'same_operation', error
                    assert error.instance_id == sandbox.id, error
                else:
                    raise AssertionError('command query remained available after Watch outage')
                assert proxy.rejected_get_attempts > 0
            else:
                assert handle.poll() == CommandStatus.RUNNING
            assert proxy.watch_attempts > 0, proxy.watch_attempts
            assert len(proxy.start_attempts) == 1, proxy.start_attempts
            assert proxy.start_attempts[0]['command_id'] == command_id
            assert proxy.start_attempts[0]['request_id']
            attached.close()
            attached = None

        recovered = Sandbox.from_id(sandbox.id, connection=connection)
        recovered_command = recovered.commands.get(command_id)
        assert recovered_command.poll() == CommandStatus.RUNNING
        assert recovered_command.kill()
        terminal = recovered_command.wait(timeout=20)
        assert terminal.status not in (CommandStatus.PENDING, CommandStatus.RUNNING), terminal
        recovered.close()
        recovered = None

        final = json.loads(catalog()['environment:' + sandbox.id])
        assert final['assignment']['generation'] == initial['assignment']['generation']
        assert labeled_backend(sandbox.id) == backend
        Sandbox.delete(sandbox.id, connection=connection)
        deleted = True
        _wait_deleted(sandbox.id, connection, timeout=60)
        result = json.loads(catalog()['environment:' + sandbox.id])['result']
        assert result['state'] == 'Deleted' and not result['resources_held'], result
        assert not labeled_backend(sandbox.id)

        report['status'] = 'passed'
        report['cases'].append({
            'id': ('reliability.command-watch-query-unavailable'
                   if query_unavailable else 'reliability.command-watch-unavailable'),
            'status': 'passed',
            'seconds': round(time.monotonic() - started, 3),
            'instance_id': sandbox.id,
            'command_id': command_id,
            'watch_attempts': proxy.watch_attempts,
            'rejected_get_attempts': proxy.rejected_get_attempts,
            'start_attempts': len(proxy.start_attempts),
            'backend': backend[0],
        })
        return report
    except Exception as error:
        report['error'] = str(error)
        raise
    finally:
        if attached is not None:
            attached.close()
        if recovered is not None:
            recovered.close()
        if sandbox is not None:
            if not deleted:
                try:
                    Sandbox.delete(sandbox.id, connection=connection)
                except Exception as error:
                    report['cleanup_errors'].append(str(error))
            sandbox.close()
        if report['cleanup_errors']:
            report['status'] = 'failed'
        report['watch_attempts'] = proxy.watch_attempts
        report['rejected_get_attempts'] = proxy.rejected_get_attempts
        report['start_attempts'] = proxy.start_attempts
        output.write_text(json.dumps(report, indent=2) + '\n')
