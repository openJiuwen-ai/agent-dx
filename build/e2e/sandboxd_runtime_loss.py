"""Combine a real sandboxd daemon restart with loss of its runtime."""

import json
import time

from adx_sandbox import RestartPolicy, Sandbox
from node import catalog, labeled_backend, persisted_runtime_id


def _record(instance_id):
    return json.loads(catalog()['environment:' + instance_id])


def _wait(predicate, seconds=90):
    deadline = time.monotonic() + seconds
    while time.monotonic() < deadline:
        value = predicate()
        if value:
            return value
        time.sleep(.2)
    raise TimeoutError('sandboxd runtime-loss result did not converge')


def create(connection, image, evidence, policy):
    assert policy in ('never', 'restart'), policy
    options = {}
    if policy == 'restart':
        options['restart_policy'] = RestartPolicy(
            max_attempts=2, initial_backoff_seconds=2, max_backoff_seconds=4,
        )
    sandbox = Sandbox(
        image=image, runtime='runc', node_id='node1', cpu=500, memory=512,
        idle_timeout=0, detached=True, connection=connection, create_timeout=150,
        **options,
    )
    try:
        assert sandbox.commands.run('printf before-daemon-loss').stdout == 'before-daemon-loss'
        record = _record(sandbox.id)
        backends = labeled_backend(sandbox.id)
        assert record['result']['state'] == 'Running' and len(backends) == 1
        payload = {
            'instance_id': sandbox.id,
            'assignment': record['assignment'],
            'runtime_id': persisted_runtime_id(record['result']),
            'backend_id': backends[0],
        }
        (evidence / ('loss-' + policy + '-created.json')).write_text(
            json.dumps(payload, indent=2) + '\n')
        return payload
    except Exception:
        Sandbox.delete(sandbox.id, connection=connection)
        raise
    finally:
        sandbox.close()


def verify(connection, evidence, secrets, policy):
    assert policy in ('never', 'restart'), policy
    before = json.loads((evidence / ('loss-' + policy + '-created.json')).read_text())
    fault = json.loads((evidence / ('sandboxd-runtime-loss-node1-' + policy + '.json')).read_text())
    assert fault['backend_ids_before'] == [before['backend_id']]
    assert not fault['backend_ids_after'] and fault['pid'] != fault['previous_pid']
    instance_id = before['instance_id']
    report = {'status': 'failed', 'cases': [], 'cleanup_errors': []}
    started = time.monotonic()
    deleted = False
    try:
        if policy == 'never':
            record = _wait(lambda: (
                record if (record := _record(instance_id))['result']['state'] == 'Failed'
                and not record['result']['resources_held'] else None
            ))
            assert record['assignment'] == before['assignment']
            assert not record['result']['restart_pending']
            assert not labeled_backend(instance_id)
            from network_partition import withdrawn_route
            route = withdrawn_route(instance_id, secrets)
            case_id = 'reliability.daemon-loss-never'
            extra = {'terminal_state': 'Failed', 'route': route}
        else:
            record = _wait(lambda: (
                record if (record := _record(instance_id))['result']['state'] == 'Running'
                and persisted_runtime_id(record['result']) != before['runtime_id']
                and record['result']['restart_attempts'] == 1 else None
            ), seconds=120)
            assert record['assignment'] == before['assignment']
            backends = labeled_backend(instance_id)
            assert len(backends) == 1 and backends[0] != before['backend_id'], backends
            sandbox = Sandbox.from_id(instance_id, connection=connection)
            try:
                command = sandbox.commands.run('printf after-daemon-loss')
                assert command.exit_code == 0 and command.stdout == 'after-daemon-loss'
            finally:
                sandbox.close()
            case_id = 'reliability.daemon-loss-restart'
            extra = {'backend_id': backends[0], 'restart_attempts': 1}
        Sandbox.delete(instance_id, connection=connection)
        deleted = True
        _wait(lambda: (
            record if (record := _record(instance_id))['result']['state'] == 'Deleted'
            and not record['result']['resources_held'] else None
        ), seconds=60)
        assert not labeled_backend(instance_id)
        report['status'] = 'passed'
        report['cases'] = [{
            'id': case_id, 'status': 'passed',
            'seconds': round(time.monotonic() - started, 3),
            'instance_id': instance_id, 'old_backend_id': before['backend_id'], **extra,
        }]
        if policy == 'never':
            (evidence / 'loss-never-verified.json').write_text(json.dumps(report, indent=2) + '\n')
        else:
            never = json.loads((evidence / 'loss-never-verified.json').read_text())
            report['cases'] = never['cases'] + report['cases']
            (evidence / 'sandboxd-runtime-loss-result.json').write_text(json.dumps(report, indent=2) + '\n')
        return report
    except Exception as error:
        report['error'] = str(error)
        raise
    finally:
        if not deleted:
            try:
                Sandbox.delete(instance_id, connection=connection)
            except Exception as error:
                report['cleanup_errors'].append(str(error))
        if report['cleanup_errors']:
            report['status'] = 'failed'
        if policy == 'restart':
            (evidence / 'sandboxd-runtime-loss-result.json').write_text(json.dumps(report, indent=2) + '\n')
