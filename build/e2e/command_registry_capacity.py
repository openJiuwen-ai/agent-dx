"""Exercise Execd command registry capacity through the installed SDK."""

import json
import time
import uuid


def run(connection, image, output):
    from adx_sandbox import ResourceExhausted, Sandbox
    from functional_lifecycle import _wait_deleted
    from node import catalog, labeled_backend

    started = time.monotonic()
    report = {'status': 'failed', 'cases': [], 'cleanup_errors': []}
    name = 'registry-capacity-' + uuid.uuid4().hex[:12]
    held_id = 'capacity-holder-' + uuid.uuid4().hex[:12]
    rejected_id = 'capacity-rejected-' + uuid.uuid4().hex[:12]
    marker = '/tmp/' + rejected_id + '.marker'
    command = f'printf y >> {marker}; printf capacity-released'
    sandbox = None
    held = None
    deleted = False
    try:
        sandbox = Sandbox(
            name=name, image=image, runtime='runc', node_id='node1',
            cpu=500, memory=512, idle_timeout=0, detached=True,
            connection=connection, create_timeout=150,
        )
        record = json.loads(catalog()['environment:' + sandbox.id])
        backend = labeled_backend(sandbox.id)
        assert len(backend) == 1, backend

        held = sandbox.commands.run('sleep 60', background=True, command_id=held_id)
        try:
            sandbox.commands.run(command, background=True, command_id=rejected_id)
        except ResourceExhausted as error:
            if error.command_id != rejected_id or error.sandbox_id != sandbox.id:
                raise AssertionError(f'capacity error lost command identity: {error}') from error
            report['rejected_command_id'] = error.command_id
        else:
            raise AssertionError('second command was admitted while registry limit was one')

        assert held.kill(), 'long-running holder ended before capacity rejection'
        held.wait(timeout=10)
        held = None
        recovered = sandbox.commands.run(
            command, background=True, command_id=rejected_id,
        ).wait(timeout=30)
        assert recovered.exit_code == 0 and recovered.stdout == 'capacity-released', recovered
        marker_result = sandbox.commands.run(f'cat {marker}')
        assert marker_result.exit_code == 0 and marker_result.stdout == 'y', marker_result
        sandbox.commands.run(f'rm {marker}')

        current = json.loads(catalog()['environment:' + sandbox.id])
        assert current['assignment']['generation'] == record['assignment']['generation']
        assert labeled_backend(sandbox.id) == backend
        Sandbox.delete(sandbox.id, connection=connection)
        deleted = True
        _wait_deleted(sandbox.id, connection, timeout=60)
        final = json.loads(catalog()['environment:' + sandbox.id])['result']
        assert final['state'] == 'Deleted' and not final['resources_held'], final
        assert not labeled_backend(sandbox.id)

        report['status'] = 'passed'
        report['cases'].append({
            'id': 'command.registry-capacity', 'status': 'passed',
            'seconds': round(time.monotonic() - started, 3),
            'instance_id': sandbox.id,
            'holder_command_id': held_id,
            'rejected_command_id': rejected_id,
            'backend': backend[0],
        })
        return report
    except Exception as error:
        report['error'] = str(error)
        raise
    finally:
        if held is not None:
            try:
                held.kill()
            except Exception as error:
                report['cleanup_errors'].append(str(error))
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
