"""Reject a missing recovery capability before any real command is started."""

import json
import time
import uuid

from command_response_cut import CommandResponseCutProxy


def run(connection, image, output, secrets):
    from adx_sandbox import ConnectionConfig, Sandbox, UnsupportedFeature
    from functional_lifecycle import _wait_deleted
    from node import catalog, labeled_backend

    started = time.monotonic()
    report = {'status': 'failed', 'cases': [], 'cleanup_errors': []}
    name = 'unsupported-' + uuid.uuid4().hex[:12]
    command_id = 'unsupported-' + uuid.uuid4().hex[:12]
    marker = '/tmp/' + command_id + '.marker'
    sandbox = None
    attached = None
    recovered = None
    deleted = False
    proxy = CommandResponseCutProxy(
        'https://127.0.0.1:8443', certificate=secrets / 'tls/ingress.pem',
        private_key=secrets / 'tls/ingress.key', ca=secrets / 'tls/ca.pem',
        cut_start=False, strip_watch_capability=True,
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
            try:
                attached.commands.run(
                    f'printf x > {marker}', background=True,
                    command_id=command_id,
                )
            except UnsupportedFeature as error:
                assert error.sandbox_id == sandbox.id, error
                assert error.command_id == command_id, error
            else:
                raise AssertionError('SDK started a command without the required capability')
            assert len(proxy.capability_attempts) == 1, proxy.capability_attempts
            assert 'multiplexed-command-watch' in proxy.capability_attempts[0][
                'capabilities'
            ]
            assert proxy.start_attempts == [], proxy.start_attempts
            attached.close()
            attached = None

        recovered = Sandbox.from_id(sandbox.id, connection=connection)
        handle = recovered.commands.run(
            f'printf x > {marker}; printf capability-ok',
            background=True, command_id=command_id,
        )
        result = handle.wait(timeout=20)
        assert result.exit_code == 0 and result.stdout == 'capability-ok', result
        marker_result = recovered.commands.run(f'cat {marker}')
        assert marker_result.exit_code == 0 and marker_result.stdout == 'x', marker_result
        recovered.commands.run(f'rm {marker}')
        recovered.close()
        recovered = None

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
            'id': 'command.unsupported-feature', 'status': 'passed',
            'seconds': round(time.monotonic() - started, 3),
            'instance_id': sandbox.id,
            'command_id': command_id,
            'rejected_start_attempts': len(proxy.start_attempts),
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
        report['capability_attempts'] = proxy.capability_attempts
        report['start_attempts'] = proxy.start_attempts
        output.write_text(json.dumps(report, indent=2) + '\n')
