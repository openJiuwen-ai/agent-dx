"""Keep HTTP queries live while command Watch remains unavailable."""

import json
import time
import uuid

from command_response_cut import CommandResponseCutProxy


def run(connection, image, output, secrets, *, query_unavailable=False):
    from adx_sandbox import (
        CommandStatus,
        CommandUnavailable,
        ConnectionConfig,
        Sandbox,
        SandboxError,
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
        'https://127.0.0.1:8443',
        certificate=secrets / 'tls/ingress.pem',
        private_key=secrets / 'tls/ingress.key',
        ca=secrets / 'tls/ca.pem',
        cut_start=False,
        reject_watch=True,
        reject_get_on_watch=query_unavailable,
    )
    try:
        sandbox = Sandbox(
            name=name,
            image=image,
            runtime='runc',
            node_id='node1',
            cpu=500,
            memory=512,
            idle_timeout=0,
            detached=True,
            connection=connection,
            create_timeout=150,
        )
        initial = json.loads(catalog()['environment:' + sandbox.id])
        backend = labeled_backend(sandbox.id)
        if not (len(backend) == 1):
            raise AssertionError(backend)

        with proxy:
            proxied = ConnectionConfig(
                server_address=f'127.0.0.1:{proxy.port}',
                token=connection.token,
                use_tls=True,
                verify_tls=True,
            )
            attached = Sandbox.from_id(sandbox.id, connection=proxied)
            handle = attached.commands.run(
                'sleep 120',
                background=True,
                command_id=command_id,
            )
            if not (handle.id == command_id):
                raise AssertionError()
            if not (handle.poll() == CommandStatus.RUNNING):
                raise AssertionError()
            try:
                handle.wait(timeout=45)
            except CommandUnavailable as error:
                if not (error.sandbox_id == sandbox.id):
                    raise AssertionError(error)
                if not (error.command_id == command_id):
                    raise AssertionError(error)
                if 'command watch unavailable' not in str(error):
                    raise AssertionError(error)
            else:
                raise AssertionError('SDK did not report the exhausted Watch reconnect budget')
            if query_unavailable:
                try:
                    handle.poll()
                except SandboxError as error:
                    if not (error.code == 'OUTCOME_UNKNOWN'):
                        raise AssertionError(error)
                    if not (error.retry == 'same_operation'):
                        raise AssertionError(error)
                    if not (error.instance_id == sandbox.id):
                        raise AssertionError(error)
                else:
                    raise AssertionError('command query remained available after Watch outage')
                if not (proxy.rejected_get_attempts > 0):
                    raise AssertionError()
            else:
                if not (handle.poll() == CommandStatus.RUNNING):
                    raise AssertionError()
            if not (proxy.watch_attempts > 0):
                raise AssertionError(proxy.watch_attempts)
            if not (len(proxy.start_attempts) == 1):
                raise AssertionError(proxy.start_attempts)
            if not (proxy.start_attempts[0]['command_id'] == command_id):
                raise AssertionError()
            if not (proxy.start_attempts[0]['request_id']):
                raise AssertionError()
            attached.close()
            attached = None

        recovered = Sandbox.from_id(sandbox.id, connection=connection)
        recovered_command = recovered.commands.get(command_id)
        if not (recovered_command.poll() == CommandStatus.RUNNING):
            raise AssertionError()
        if not (recovered_command.kill()):
            raise AssertionError()
        terminal = recovered_command.wait(timeout=20)
        if not (terminal.status not in (CommandStatus.PENDING, CommandStatus.RUNNING)):
            raise AssertionError(terminal)
        recovered.close()
        recovered = None

        final = json.loads(catalog()['environment:' + sandbox.id])
        if not (final['assignment']['generation'] == initial['assignment']['generation']):
            raise AssertionError()
        if not (labeled_backend(sandbox.id) == backend):
            raise AssertionError()
        Sandbox.delete(sandbox.id, connection=connection)
        deleted = True
        _wait_deleted(sandbox.id, connection, timeout=60)
        if not ('environment:' + sandbox.id not in catalog()):
            raise AssertionError()
        if not (not labeled_backend(sandbox.id)):
            raise AssertionError()

        report['status'] = 'passed'
        report['cases'].append(
            {
                'id': (
                    'reliability.command-watch-query-unavailable'
                    if query_unavailable
                    else 'reliability.command-watch-unavailable'
                ),
                'status': 'passed',
                'seconds': round(time.monotonic() - started, 3),
                'instance_id': sandbox.id,
                'command_id': command_id,
                'watch_attempts': proxy.watch_attempts,
                'rejected_get_attempts': proxy.rejected_get_attempts,
                'start_attempts': len(proxy.start_attempts),
                'backend': backend[0],
            }
        )
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
