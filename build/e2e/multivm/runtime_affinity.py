#!/usr/bin/env python3
"""Place an unpinned Sandbox on the sole worker advertising its runtime."""
from __future__ import annotations

import argparse
import json
import os
from pathlib import Path
import uuid

if __package__:
    from .contract import verify_inventory
    from .sdk_accept import backend_ids, persisted_assignment, ssh, verify_machines
    from .worker_failure import node_record, wait_until
else:
    from contract import verify_inventory
    from sdk_accept import backend_ids, persisted_assignment, ssh, verify_machines
    from worker_failure import node_record, wait_until


def runtime_owner(control, workers, runtime_class, remote=ssh):
    inventories = {}
    supported = []
    for worker in workers:
        node_id = worker['node_id']
        record = node_record(control, node_id, remote)
        session = record.get('session', {})
        classes = record.get('runtime_classes')
        if record.get('id') != node_id or not record.get('available') \
                or not session.get('routable') \
                or not isinstance(classes, list) or not classes:
            raise AssertionError(f'{node_id} lacks a live sandboxd runtime inventory')
        inventories[node_id] = classes
        if runtime_class in classes:
            supported.append(node_id)
    if len(supported) != 1:
        raise AssertionError(f'{runtime_class} must be advertised by exactly one worker; '
                             f'got {supported}')
    return supported[0], inventories


def run_runtime_affinity(inventory, connection, image, socket, output,
                         sandbox_factory=None, remote=ssh, runtime_class='runsc',
                         wait=wait_until):
    verify_inventory(inventory)
    if sandbox_factory is None:
        from adx_sandbox import Sandbox
        sandbox_factory = Sandbox
    machines = {machine['role']: machine for machine in inventory['machines']}
    control = machines['control']
    workers = (machines['worker-1'], machines['worker-2'])
    report = {'status': 'failed', 'profile': 'multi-vm-runtime-affinity',
              'runtime_class': runtime_class, 'checks': [], 'cleanup_errors': []}
    sandbox = None
    output.mkdir(parents=True, exist_ok=True)
    try:
        report['machines'] = verify_machines(inventory, remote)
        expected, report['inventories'] = runtime_owner(control, workers, runtime_class, remote)
        report['checks'].append('live-heterogeneous-runtime-inventory')
        sandbox = sandbox_factory(
            name='mv-runtime-' + uuid.uuid4().hex[:12],
            image=image, runtime=runtime_class, cpu=250, memory=256,
            idle_timeout=0, connection=connection, create_timeout=150,
        )
        report['instance_id'] = sandbox.id
        assignment = persisted_assignment(control, sandbox.id, remote)
        report['assignment'] = assignment
        if assignment.get('node_id') != expected \
                or assignment.get('state') != 'Running' \
                or not assignment.get('resources_held'):
            raise AssertionError(f'runtime assignment differs from {expected}: {assignment}')
        physical = {worker['node_id']: backend_ids(worker, socket, sandbox.id, remote)
                    for worker in workers}
        report['backends'] = physical
        if len(physical[expected]) != 1 or any(ids for node_id, ids in physical.items()
                                               if node_id != expected):
            raise AssertionError(f'runtime physical backend differs from assignment: {physical}')
        result = sandbox.commands.run('printf runtime-ready')
        if (result.exit_code, result.stdout) != (0, 'runtime-ready'):
            raise AssertionError('runtime command failed through public SDK')
        report['checks'].append('unpinned-runtime-placement-and-command')
    except Exception as error:
        report['error'] = f'{type(error).__name__}: {error}'
        raise
    finally:
        if sandbox is not None:
            try:
                sandbox.kill()
            except Exception as error:
                report['cleanup_errors'].append(f'{sandbox.id} delete: {error}')
            finally:
                try:
                    sandbox.close()
                except Exception as error:
                    report['cleanup_errors'].append(f'{sandbox.id} close: {error}')
            try:
                def released():
                    record = persisted_assignment(control, sandbox.id, remote)
                    backends = {worker['node_id']: backend_ids(worker, socket, sandbox.id, remote)
                                for worker in workers}
                    if record.get('state') == 'Deleted' \
                            and not record.get('resources_held') \
                            and not any(backends.values()):
                        return True
                    return None

                wait(released, 'runtime-affinity backend or allocation remained', 20)
                report['checks'].append('runtime-backend-and-allocation-released')
            except Exception as error:
                report['cleanup_errors'].append(str(error))
        if not report.get('error') and not report['cleanup_errors']:
            report['status'] = 'passed'
        (output / 'runtime-affinity-result.json').write_text(json.dumps(report, indent=2) + '\n')
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
    parser.add_argument('--runtime-class', default='runsc')
    parser.add_argument('--output', required=True, type=Path)
    args = parser.parse_args()
    os.environ['SSL_CERT_FILE'] = str(args.ca.resolve())
    from adx_sandbox import ConnectionConfig
    connection = ConnectionConfig(
        server_address=args.endpoint, token=args.token_file.read_text().strip(),
        use_tls=True, verify_tls=True,
    )
    report = run_runtime_affinity(json.loads(args.inventory.read_text()), connection,
                                  args.image, args.socket, args.output,
                                  runtime_class=args.runtime_class)
    print(json.dumps({'status': report['status'], 'checks': report['checks']}), flush=True)


if __name__ == '__main__':
    main()
