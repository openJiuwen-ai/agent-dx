"""Public SDK assertions while one worker cannot reach Coordinator."""

import json
import ssl
import time
from urllib.error import HTTPError
from urllib.request import Request, urlopen

def withdrawn_route(instance_id, secrets):
    """Distinguish an Ingress route rejection from an EXECD 404 response."""
    context = ssl.create_default_context(cafile=str(secrets / 'tls/ca.pem'))
    request = Request(
        f'https://127.0.0.1:8443/direct/{instance_id}/50090/status',
        headers={'Authorization': 'Bearer ' + (secrets / 'api-key').read_text().strip()},
    )
    deadline = time.monotonic() + 15
    last = None
    while time.monotonic() < deadline:
        try:
            with urlopen(request, context=context, timeout=2) as response:
                last = f'route unexpectedly served HTTP {response.status}'
        except HTTPError as error:
            body = error.read().decode('utf-8', 'replace')
            if error.code in (404, 409, 503) and (
                'route' in body or 'not connectable' in body
            ):
                return {'http_status': error.code, 'message': body[:250]}
            last = f'HTTP {error.code}: {body[:250]}'
        time.sleep(.2)
    raise AssertionError(f'failed instance route remained usable: {last}')


def run(connection, image, evidence, secrets, output):
    from adx_sandbox import Sandbox, SandboxError, SandboxNotFound
    from functional_lifecycle import _wait_deleted

    observed = json.loads((evidence / 'node-failure-observed.json').read_text())
    failed_id = observed['failed_id']
    healthy_id = observed['healthy_id']
    started = time.monotonic()
    report = {'status': 'failed', 'cases': [], 'cleanup_errors': []}
    extra = None
    deleted = False
    try:
        route = withdrawn_route(failed_id, secrets)
        deadline = time.monotonic() + 15
        while True:
            try:
                unexpected = Sandbox.from_id(failed_id, connection=connection)
            except (SandboxNotFound, SandboxError, RuntimeError):
                break
            else:
                unexpected.close()
                if time.monotonic() >= deadline:
                    raise AssertionError('failed instance remained queryable as Running')
                time.sleep(.2)
        report['cases'].append({
            'id': 'reliability.partition-route-withdrawal', 'status': 'passed',
            'seconds': round(time.monotonic() - started, 3),
            'failed_id': failed_id, **route,
        })

        healthy = Sandbox.from_id(healthy_id, connection=connection)
        try:
            command = healthy.commands.run('printf partition-unaffected')
            assert command.exit_code == 0 and command.stdout == 'partition-unaffected'
        finally:
            healthy.close()
        extra = Sandbox(
            image=image, runtime='runc', node_id='node1', cpu=250, memory=256,
            idle_timeout=0, detached=True, connection=connection, create_timeout=150,
        )
        command = extra.commands.run('printf partition-new-allocation')
        assert command.exit_code == 0 and command.stdout == 'partition-new-allocation'
        Sandbox.delete(extra.id, connection=connection)
        deleted = True
        _wait_deleted(extra.id, connection, timeout=60)
        report['cases'].append({
            'id': 'reliability.partition-healthy-worker-continues',
            'status': 'passed', 'seconds': round(time.monotonic() - started, 3),
            'healthy_id': healthy_id, 'new_id': extra.id,
        })
        report['status'] = 'passed'
        return report
    except Exception as error:
        report['error'] = str(error)
        raise
    finally:
        if extra is not None:
            extra.close()
            if not deleted:
                try:
                    Sandbox.delete(extra.id, connection=connection)
                except Exception as error:
                    report['cleanup_errors'].append(str(error))
        if report['cleanup_errors']:
            report['status'] = 'failed'
        output.write_text(json.dumps(report, indent=2) + '\n')
