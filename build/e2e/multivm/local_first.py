#!/usr/bin/env python3
"""Three-VM local-first claim, cross-node fallback, and ownership acceptance."""
from __future__ import annotations

import argparse
from concurrent.futures import ThreadPoolExecutor
import json
import os
from pathlib import Path
import time
import uuid

if __package__:
    from .contract import verify_inventory
    from .sdk_accept import (backend_ids, persisted_assignment, ssh,
                             verify_machines)
else:
    from contract import verify_inventory
    from sdk_accept import (backend_ids, persisted_assignment, ssh,
                            verify_machines)


EVENT_LOG_SCRIPT = '''
import gzip, json, pathlib, sys
directory = pathlib.Path(sys.argv[1])
identities = set(json.loads(sys.argv[2]))
event = sys.argv[3]
found = set()
for path in directory.glob(sys.argv[4]):
    if path.name.endswith('.tmp') or not path.is_file():
        continue
    opener = gzip.open if path.suffix == '.gz' else open
    with opener(path, 'rt', errors='replace') as stream:
        for line in stream:
            if event in line:
                found.update(identity for identity in identities if identity in line)
print(json.dumps(sorted(found)))
'''


def claimed_ids(control, identities, remote=ssh):
    directory = control.get('coordinator_log_dir', '/opt/adx/run/control/logs')
    return set(json.loads(remote(control, 'python3', '-c', EVENT_LOG_SCRIPT,
                                 directory, json.dumps(sorted(identities)),
                                 'local_environment_claim', 'coordinator*.log*', timeout=30)))


def fallback_ids(worker, identities, remote=ssh):
    directory = worker.get('adxlet_log_dir', '/opt/adx/run/node/logs')
    return set(json.loads(remote(worker, 'python3', '-c', EVENT_LOG_SCRIPT,
                                 directory, json.dumps(sorted(identities)),
                                 'local_environment_fallback', 'adxlet*.log*', timeout=30)))


def wait_until(check, description, timeout=12, clock=time.monotonic, sleep=time.sleep):
    deadline = clock() + timeout
    while True:
        value = check()
        if value:
            return value
        if clock() >= deadline:
            raise AssertionError(description)
        sleep(.25)


def run_local_first(inventory, connection, image, socket, output,
                    sandbox_factory=None, sandbox_error=None, resource_reader=None,
                    remote=ssh, claims=claimed_ids, fallbacks=fallback_ids,
                    wait=wait_until):
    verify_inventory(inventory)
    if sandbox_factory is None or sandbox_error is None or resource_reader is None:
        from adx_sandbox import Sandbox, SandboxError, resources
        sandbox_factory = sandbox_factory or Sandbox
        sandbox_error = sandbox_error or SandboxError
        resource_reader = resource_reader or resources
    machines = {machine['role']: machine for machine in inventory['machines']}
    control = machines['control']
    workers = (machines['worker-1'], machines['worker-2'])
    worker_ids = {worker['node_id'] for worker in workers}
    report = {'status': 'failed', 'profile': 'multi-vm-local-first',
              'checks': [], 'instances': [], 'cleanup_errors': []}
    handles = []
    output.mkdir(parents=True, exist_ok=True)
    prefix = 'mv-lf-' + uuid.uuid4().hex[:12]

    def create(name, **options):
        return sandbox_factory(name=name, image=image, runtime='runc',
                               cpu=500, memory=512, idle_timeout=0,
                               connection=connection, create_timeout=150, **options)

    def assigned(sandbox):
        record = persisted_assignment(control, sandbox.id, remote)
        if record.get('node_id') not in worker_ids or record.get('state') != 'Running' \
                or not record.get('resources_held'):
            raise AssertionError(f'invalid local-first assignment: {record}')
        return record

    def physical(sandbox, owner):
        observed = {
            worker['node_id']: backend_ids(worker, socket, sandbox.id, remote)
            for worker in workers
        }
        if len(observed[owner]) != 1 or any(ids for node_id, ids in observed.items() if node_id != owner):
            raise AssertionError(f'duplicate or misplaced physical backend: {sandbox.id} {observed}')
        return observed

    def available_cpu(node_id):
        nodes = {node.id: node for node in resource_reader(connection=connection)}
        return nodes[node_id].allocatable['CPU']

    try:
        report['machines'] = verify_machines(inventory, remote)
        initial = {worker['node_id']: available_cpu(worker['node_id']) for worker in workers}
        if initial[workers[0]['node_id']] < 1000 or initial[workers[1]['node_id']] < 2000:
            raise AssertionError(f'insufficient dedicated CPU for local-first test: {initial}')
        first = create(prefix + '-a')
        handles.append(first)
        second = create(prefix + '-b')
        handles.append(second)
        owners = [assigned(sandbox)['node_id'] for sandbox in (first, second)]
        if set(owners) != worker_ids:
            raise AssertionError(f'local-first entry rotation did not reach both workers: {owners}')
        report['checks'].append('entry-rotation')

        with ThreadPoolExecutor(max_workers=2) as executor:
            futures = [executor.submit(create, prefix + '-race') for _ in range(2)]
            copies = []
            errors = []
            for future in futures:
                try:
                    copy = future.result()
                except Exception as error:
                    errors.append(error)
                else:
                    handles.append(copy)
                    copies.append(copy)
            if errors:
                raise errors[0]
        if copies[0].id != copies[1].id:
            raise AssertionError('same-name concurrent creates received different IDs')
        owner = assigned(copies[0])['node_id']
        physical(copies[0], owner)
        report['checks'].append('same-id-atomic-claim')
        try:
            changed = sandbox_factory(
                name=prefix + '-race', image=image, runtime='runc', cpu=600,
                memory=512, idle_timeout=0, connection=connection, create_timeout=150,
            )
        except sandbox_error as error:
            if error.code != 'CONFLICT' or error.retry != 'never' \
                    or error.outcome != 'not_started':
                raise AssertionError(f'unexpected specification conflict contract: {error}')
        else:
            handles.append(changed)
            raise AssertionError('different specification reused the existing ID')
        report['checks'].append('different-specification-conflict')

        local_ids = {first.id, second.id, copies[0].id}
        wait(lambda: local_ids <= claims(control, local_ids, remote),
             'expected local claims were not recorded')
        report['checks'].append('local-claim-log-evidence')

        reserved = {worker['node_id']: 0 for worker in workers}
        for sandbox in (first, second, copies[0]):
            reserved[assigned(sandbox)['node_id']] += 500
        wait(lambda: all(available_cpu(node_id) == initial[node_id] - amount
                         for node_id, amount in reserved.items()),
             'initial local claims were not visible in scheduler resources')
        before_fallback = available_cpu(workers[1]['node_id'])
        fallback = []
        for index in (1, 2):
            sandbox = create(prefix + f'-fallback-{index}', node_id=workers[1]['node_id'])
            handles.append(sandbox)
            fallback.append(sandbox)
        fallback_ids = {sandbox.id for sandbox in fallback}
        for sandbox in fallback:
            record = assigned(sandbox)
            if record['node_id'] != workers[1]['node_id']:
                raise AssertionError('node constraint was ignored during fallback')
            physical(sandbox, workers[1]['node_id'])
        # With two available entries, two sequential requests constrained to
        # worker 2 must include one local claim and one central fallback.
        def fallback_evidence():
            claimed = claims(control, fallback_ids, remote)
            forwarded = set().union(*(fallbacks(worker, fallback_ids, remote)
                                      for worker in workers))
            if claimed | forwarded == fallback_ids:
                return claimed, forwarded
            return None

        claimed, forwarded = wait(fallback_evidence,
                                  'fallback pair has incomplete claim and forward events')
        if len(claimed) != 1:
            raise AssertionError(f'fallback pair did not split local and central paths: {claimed}')
        if forwarded != fallback_ids - claimed:
            raise AssertionError(f'central fallback event disagrees with local claim: {forwarded}')
        report['fallback'] = {'local_claim': next(iter(claimed)),
                              'central_assignment': next(iter(forwarded))}
        report['checks'].append('constrained-central-fallback')
        def observed_fallback_cpu():
            current = available_cpu(workers[1]['node_id'])
            return current if before_fallback - current >= 1000 else None
        after_fallback = wait(
            observed_fallback_cpu,
            'fallback allocations were not reflected in scheduler resources')
        if before_fallback - after_fallback != 1000:
            raise AssertionError('local and central fallback double-counted worker resources')
        report['fallback']['cpu_millis_delta'] = 1000
        report['checks'].append('single-resource-accounting')

        unique = {sandbox.id: sandbox for sandbox in handles}
        if len(unique) != 5:
            raise AssertionError('duplicate request created another ownership identity')
        for sandbox in unique.values():
            record = assigned(sandbox)
            physical(sandbox, record['node_id'])
            result = sandbox.commands.run('printf local-first-three-vm')
            if result.exit_code != 0 or result.stdout != 'local-first-three-vm':
                raise AssertionError(f'{sandbox.id} command failed after ownership confirmation')
            report['instances'].append({'id': sandbox.id, 'node_id': record['node_id'],
                                        'generation': record['generation']})
        report['checks'].append('one-backend-per-assignment')
    except Exception as error:
        report['error'] = f'{type(error).__name__}: {error}'
        raise
    finally:
        removed = set()
        for sandbox in reversed(handles):
            try:
                if sandbox.id not in removed:
                    sandbox.kill()
                    removed.add(sandbox.id)
            except Exception as error:
                report['cleanup_errors'].append(f'{sandbox.id} delete: {error}')
            finally:
                try:
                    sandbox.close()
                except Exception as error:
                    report['cleanup_errors'].append(f'{sandbox.id} close: {error}')
        try:
            deadline = time.monotonic() + 15
            while removed:
                residual = {
                    instance_id: {
                        'assignment': persisted_assignment(control, instance_id, remote),
                        'backends': {
                            worker['node_id']: backend_ids(worker, socket, instance_id, remote)
                            for worker in workers
                        },
                    }
                    for instance_id in removed
                }
                if all(item['assignment']['state'] == 'Deleted'
                       and not item['assignment']['resources_held']
                       and not any(item['backends'].values()) for item in residual.values()):
                    report['checks'].append('single-release-per-instance')
                    break
                if time.monotonic() >= deadline:
                    raise AssertionError(f'local-first cleanup incomplete: {residual}')
                time.sleep(.2)
        except Exception as error:
            report['cleanup_errors'].append(str(error))
        if not report.get('error') and not report['cleanup_errors']:
            report['status'] = 'passed'
        (output / 'local-first-result.json').write_text(json.dumps(report, indent=2) + '\n')
        if report['cleanup_errors'] and not report.get('error'):
            raise RuntimeError('; '.join(report['cleanup_errors']))
    return report


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--inventory', required=True, type=Path)
    parser.add_argument('--endpoint', required=True)
    parser.add_argument('--token-file', required=True, type=Path)
    parser.add_argument('--ca', required=True, type=Path)
    parser.add_argument('--image', required=True)
    parser.add_argument('--socket', default='/run/sandboxd/sandboxd.sock')
    parser.add_argument('--output', required=True, type=Path)
    args = parser.parse_args()
    os.environ['SSL_CERT_FILE'] = str(args.ca.resolve())
    from adx_sandbox import ConnectionConfig
    connection = ConnectionConfig(
        server_address=args.endpoint, token=args.token_file.read_text().strip(),
        use_tls=True, verify_tls=True,
    )
    result = run_local_first(json.loads(args.inventory.read_text()), connection,
                             args.image, args.socket, args.output)
    print(json.dumps({'status': result['status'], 'checks': result['checks']}), flush=True)


if __name__ == '__main__':
    main()
