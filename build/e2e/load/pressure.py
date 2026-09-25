"""Burst creates against full nodes and verify queue drain and resource accounting."""

from concurrent.futures import ThreadPoolExecutor
import json
import time
import uuid


REQUESTS = 8


def verify_waiting(expected, observed, completed):
    if completed:
        raise AssertionError('a request created before capacity release')
    if not expected <= observed:
        raise AssertionError(f'not all requests queued: {sorted(expected - observed)}')


def run(connection, image, output, ca, admin_key):
    """Saturate both nodes, then release capacity while requests are queued."""
    from adx_sandbox import Sandbox
    from metrics import check as check_metrics
    from node import catalog
    from schedule_deadline import pending_ids

    prefix = 'load-' + uuid.uuid4().hex[:10]
    names = [f'{prefix}-{index}' for index in range(REQUESTS)]
    ids = {'default-' + name for name in names}
    holders = []
    results = []
    failures = []
    started = time.monotonic()
    report = {'status': 'failed', 'profile': 'load-pressure', 'requested': REQUESTS,
              'queue_observed': False, 'completed': 0, 'errors': failures,
              'cases': []}

    def create(name, node_id):
        sandbox = None
        try:
            sandbox = Sandbox(name=name, image=image, runtime='runc', node_id=node_id,
                              cpu=2000, memory=512, idle_timeout=0,
                              connection=connection, schedule_timeout=120,
                              create_timeout=150)
            record = json.loads(catalog()['environment:' + sandbox.id])
            assignment = record['assignment']
            if assignment['node_id'] != node_id or record['result']['state'] != 'Running':
                raise AssertionError(f'wrong pressure assignment: {sandbox.id} {record}')
            command = sandbox.commands.run("printf 'pressure-ready'")
            if command.exit_code != 0 or command.stdout != 'pressure-ready':
                raise AssertionError(f'pressure command failed: {sandbox.id}')
            return {'id': sandbox.id, 'node_id': node_id,
                    'generation': assignment['generation']}
        finally:
            if sandbox is not None:
                try:
                    sandbox.kill()
                finally:
                    sandbox.close()

    try:
        for node_id in ('node1', 'node2'):
            holder = Sandbox(name=prefix + '-holder-' + node_id, image=image,
                             runtime='runc', node_id=node_id, cpu=2000, memory=512,
                             idle_timeout=0, connection=connection, create_timeout=150)
            holders.append(holder)
        check_metrics('pressure-full', running=2, reserved=4000, pending=0)
        with ThreadPoolExecutor(max_workers=REQUESTS) as executor:
            futures = [executor.submit(create, name, 'node1' if index % 2 == 0 else 'node2')
                       for index, name in enumerate(names)]
            try:
                deadline = time.monotonic() + 25
                observed = set()
                while time.monotonic() < deadline:
                    observed = pending_ids(connection.server_address,
                                           admin_key.read_text().strip(), ca)
                    completed = sum(future.done() for future in futures)
                    if ids <= observed or completed:
                        break
                    time.sleep(.1)
                verify_waiting(ids, observed, completed)
                check_metrics('pressure-queued', running=2, reserved=4000,
                              pending=REQUESTS)
                report['queue_observed'] = True
            finally:
                for holder in holders:
                    try:
                        holder.kill()
                    finally:
                        holder.close()
                holders.clear()
            for future in futures:
                results.append(future.result(timeout=150))
        if len({item['id'] for item in results}) != REQUESTS:
            raise AssertionError('pressure requests did not retain unique identities')
        if {item['node_id'] for item in results} != {'node1', 'node2'}:
            raise AssertionError('pressure requests did not use both nodes')
        check_metrics('pressure-drained', running=0, reserved=0, pending=0)
        report['completed'] = len(results)
        report['assignments'] = results
        report['status'] = 'passed'
        report['cases'] = [
            {'id': 'load-pressure.queued', 'status': 'passed', 'seconds': 0},
            {'id': 'load-pressure.drained', 'status': 'passed', 'seconds': 0},
            {'id': 'load-pressure.ledger', 'status': 'passed', 'seconds': 0},
        ]
    except Exception as error:
        failures.append(f'{type(error).__name__}: {error}')
        raise
    finally:
        for holder in holders:
            try:
                holder.kill()
            finally:
                holder.close()
        report['elapsed_seconds'] = round(time.monotonic() - started, 3)
        output.write_text(json.dumps(report, indent=2) + '\n')
    return report
