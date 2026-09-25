"""Public SDK checks for resource-observation expiry without node-session loss."""

import json
import time
import uuid

from adx_sandbox import Sandbox, SandboxError
from functional_lifecycle import _wait_deleted
from node import backend, catalog, labeled_backend, persisted_runtime_id


def create(connection, image, output):
    sandbox = Sandbox(
        image=image, runtime='runc', cpu=250, memory=256,
        idle_timeout=0, detached=True, node_id='node1',
        connection=connection, create_timeout=150,
    )
    try:
        result = sandbox.commands.run('printf resource-observer-live')
        assert result.exit_code == 0 and result.stdout == 'resource-observer-live'
        records = catalog()
        record = json.loads(records['environment:' + sandbox.id])
        node = json.loads(records['node:node1'])
        backends = labeled_backend(sandbox.id)
        assert record['assignment']['node_id'] == 'node1' and len(backends) == 1
        assert record['result']['state'] == 'Running' and node['node']['available']
        output.write_text(json.dumps({
            'instance_id': sandbox.id,
            'session_id': node['session']['id'],
            'runtime_id': persisted_runtime_id(record['result']),
            'backend': backends[0],
        }) + '\n')
    except Exception:
        Sandbox.delete(sandbox.id, connection=connection)
        raise
    finally:
        sandbox.close()


def rejected(connection, image, evidence):
    live = json.loads((evidence / 'resource-live.json').read_text())
    before = catalog()
    held = {
        key for key, value in before.items()
        if key.startswith('environment:')
        and (json.loads(value).get('result') or {}).get('resources_held')
    }
    backends = backend()
    name = 'stale-rejected-' + uuid.uuid4().hex
    started = time.monotonic()
    try:
        unexpected = Sandbox(
            name=name, image=image, runtime='runc', node_id='node1',
            cpu=250, memory=256, idle_timeout=0,
            schedule_timeout=3, create_timeout=40, connection=connection,
        )
    except SandboxError as error:
        assert 'central scheduling queue deadline exceeded' in str(error), str(error)
    else:
        try:
            raise AssertionError('stale node accepted a new instance')
        finally:
            unexpected.kill()
            unexpected.close()
    after = catalog()
    held_after = {
        key for key, value in after.items()
        if key.startswith('environment:')
        and (json.loads(value).get('result') or {}).get('resources_held')
    }
    assert held_after == held and 'environment:' + live['instance_id'] in held_after
    assert backend() == backends and labeled_backend(live['instance_id']) == [live['backend']]
    result = {'name': name, 'node_id': 'node1', 'held_before': len(held),
              'held_after': len(held_after), 'backend': live['backend'],
              'seconds': round(time.monotonic() - started, 3)}
    (evidence / 'resource-rejected.json').write_text(json.dumps(result, indent=2) + '\n')
    return result


def verify(connection, image, evidence, output):
    live = json.loads((evidence / 'resource-live.json').read_text())
    stale = json.loads((evidence / 'resource-stale.json').read_text())
    fresh = json.loads((evidence / 'resource-fresh.json').read_text())
    rejected_result = json.loads((evidence / 'resource-rejected.json').read_text())
    created = None
    deleted = set()
    report = {'status': 'failed', 'cases': [], 'cleanup_errors': []}
    try:
        retained = Sandbox.from_id(live['instance_id'], connection=connection)
        try:
            command = retained.commands.run('printf resource-observer-retained')
            assert command.exit_code == 0 and command.stdout == 'resource-observer-retained'
        finally:
            retained.close()

        created = Sandbox(
            image=image, runtime='runc', cpu=250, memory=256, node_id='node1',
            idle_timeout=0, detached=True, connection=connection, create_timeout=150,
        )
        record = json.loads(catalog()['environment:' + created.id])
        assert record['assignment']['node_id'] == 'node1'
        command = created.commands.run('printf resource-observer-reopened')
        assert command.exit_code == 0 and command.stdout == 'resource-observer-reopened'

        for sid in (created.id, live['instance_id']):
            Sandbox.delete(sid, connection=connection)
            deleted.add(sid)
            _wait_deleted(sid, connection, timeout=60)
            final = json.loads(catalog()['environment:' + sid])['result']
            assert final['state'] == 'Deleted' and not final['resources_held'], final
            assert not labeled_backend(sid)
        report['status'] = 'passed'
        report['cases'] = [
            {'id': 'reliability.resource-stale-admission', 'status': 'passed',
             'seconds': stale['seconds'] + rejected_result['seconds']},
            {'id': 'reliability.resource-observation-recovery', 'status': 'passed',
             'seconds': fresh['seconds']},
        ]
        return report
    except Exception as error:
        report['error'] = str(error)
        raise
    finally:
        if created is not None:
            created.close()
        for sid in (live['instance_id'], created.id if created else None):
            if sid and sid not in deleted:
                try:
                    Sandbox.delete(sid, connection=connection)
                except Exception as error:
                    report['cleanup_errors'].append(str(error))
        if report['cleanup_errors']:
            report['status'] = 'failed'
        output.write_text(json.dumps(report, indent=2) + '\n')
