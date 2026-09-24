#!/usr/bin/env python3
"""Check Pack or Spread placement on two comparable real worker VMs."""
from __future__ import annotations

import argparse
import json
import os
from pathlib import Path
import re
import uuid

if __package__:
    from .contract import verify_inventory
    from .sdk_accept import backend_ids, persisted_assignment, ssh, verify_machines
    from .worker_failure import wait_until
else:
    from contract import verify_inventory
    from sdk_accept import backend_ids, persisted_assignment, ssh, verify_machines
    from worker_failure import wait_until


def configured_policy(control, remote=ssh):
    config = control.get('deployment_config', '/opt/adx/config/deployment.yaml')
    rendered = remote(control, '/opt/adx/current/bin/adxctl', '--config',
                      config, 'config', 'dump')
    policies = re.findall(r'(?m)^\s+placement:\s*[\'"]?(pack|spread)[\'"]?\s*$', rendered)
    if len(policies) != 1:
        raise AssertionError(f'expected exactly one configured placement policy, got {policies}')
    modes = re.findall(r'(?m)^\s+create_mode:\s*[\'"]?([^\s\'"]+)[\'"]?\s*$',
                       rendered)
    if modes and modes != ['central']:
        raise AssertionError('Pack/Spread acceptance requires central create mode')
    return policies[0]


def comparable_nodes(nodes, worker_ids):
    selected = [nodes[node_id] for node_id in worker_ids]
    scores = []
    impacts = []
    for node in selected:
        if node.status != 0:
            raise AssertionError(f'{node.id} is not accepting allocations')
        for field in ('CPU', 'Memory', 'Disk'):
            if node.capacity.get(field, 0) <= 0:
                raise AssertionError(f'{node.id} has no measured {field} capacity')
        if node.allocatable.get('CPU', 0) < 1000 \
                or node.allocatable.get('Memory', 0) < 1024:
            raise AssertionError(f'{node.id} has insufficient free CPU or memory')
        scores.append(sum(node.allocatable.get(field, 0) / node.capacity[field]
                          for field in ('CPU', 'Memory', 'Disk')))
        impacts.append(500 / node.capacity['CPU'] + 512 / node.capacity['Memory'])
    if abs(scores[0] - scores[1]) >= min(impacts) * .4:
        raise AssertionError('workers are too resource-asymmetric for a deterministic score test')
    return {'free_scores': scores, 'request_impacts': impacts}


def run_placement_policy(inventory, connection, image, socket, output, policy,
                         sandbox_factory=None, resource_reader=None, remote=ssh,
                         wait=wait_until):
    if policy not in ('pack', 'spread'):
        raise ValueError('placement policy must be pack or spread')
    verify_inventory(inventory)
    if sandbox_factory is None or resource_reader is None:
        from adx_sandbox import Sandbox, resources
        sandbox_factory = sandbox_factory or Sandbox
        resource_reader = resource_reader or resources
    machines = {machine['role']: machine for machine in inventory['machines']}
    control = machines['control']
    workers = (machines['worker-1'], machines['worker-2'])
    worker_ids = tuple(worker['node_id'] for worker in workers)
    report = {'status': 'failed', 'profile': 'multi-vm-placement-' + policy,
              'checks': [], 'instances': [], 'cleanup_errors': []}
    handles = []
    output.mkdir(parents=True, exist_ok=True)
    try:
        report['machines'] = verify_machines(inventory, remote)
        actual = configured_policy(control, remote)
        if actual != policy:
            raise AssertionError(f'configured placement {actual} differs from selected {policy} case')
        report['checks'].append('central-placement-config')
        nodes = {node.id: node for node in resource_reader(connection=connection)}
        report['preflight'] = comparable_nodes(nodes, worker_ids)
        initial = {node_id: {field: nodes[node_id].allocatable[field]
                             for field in ('CPU', 'Memory')} for node_id in worker_ids}
        report['checks'].append('comparable-worker-capacity')

        prefix = 'mv-' + policy + '-' + uuid.uuid4().hex[:12]
        for index in range(2):
            sandbox = sandbox_factory(
                name=f'{prefix}-{index}', image=image, runtime='runc',
                cpu=500, memory=512, idle_timeout=0,
                connection=connection, create_timeout=150,
            )
            handles.append(sandbox)
            assignment = persisted_assignment(control, sandbox.id, remote)
            owner = assignment.get('node_id')
            if owner not in worker_ids or assignment.get('state') != 'Running' \
                    or not assignment.get('resources_held'):
                raise AssertionError(f'placement assignment invalid: {assignment}')
            physical = {worker['node_id']: backend_ids(worker, socket, sandbox.id, remote)
                        for worker in workers}
            if len(physical[owner]) != 1 or any(ids for node_id, ids in physical.items()
                                                if node_id != owner):
                raise AssertionError(f'physical placement disagrees with assignment: {physical}')
            result = sandbox.commands.run('printf placement-ok')
            if (result.exit_code, result.stdout) != (0, 'placement-ok'):
                raise AssertionError(f'{sandbox.id} command failed after placement')
            report['instances'].append({'id': sandbox.id, 'node_id': owner,
                                        'generation': assignment['generation'],
                                        'backend_id': physical[owner][0]})
        owners = [instance['node_id'] for instance in report['instances']]
        if policy == 'pack' and owners[0] != owners[1]:
            raise AssertionError(f'Pack did not concentrate two requests: {owners}')
        if policy == 'spread' and owners[0] == owners[1]:
            raise AssertionError(f'Spread did not separate two requests: {owners}')
        report['checks'].append('public-sdk-policy-placement')
    except Exception as error:
        report['error'] = f'{type(error).__name__}: {error}'
        raise
    finally:
        for sandbox in reversed(handles):
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
            if handles:
                def released():
                    nodes = {node.id: node for node in resource_reader(connection=connection)}
                    if any(any(nodes[node_id].allocatable[field] != initial[node_id][field]
                               for field in ('CPU', 'Memory')) for node_id in worker_ids):
                        return None
                    if any((record := persisted_assignment(control, sandbox.id, remote))
                           .get('state') != 'Deleted' or record.get('resources_held')
                           for sandbox in handles):
                        return None
                    if any(backend_ids(worker, socket, sandbox.id, remote)
                           for sandbox in handles for worker in workers):
                        return None
                    return True

                wait(released, 'placement resources or backends remained after cleanup', 20)
                report['checks'].append('all-owned-resources-released')
        except Exception as error:
            report['cleanup_errors'].append(str(error))
        if not report.get('error') and not report['cleanup_errors']:
            report['status'] = 'passed'
        (output / ('placement-' + policy + '-result.json')).write_text(
            json.dumps(report, indent=2) + '\n')
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
    parser.add_argument('--placement', choices=('pack', 'spread'), required=True)
    parser.add_argument('--output', required=True, type=Path)
    args = parser.parse_args()
    os.environ['SSL_CERT_FILE'] = str(args.ca.resolve())
    from adx_sandbox import ConnectionConfig
    connection = ConnectionConfig(
        server_address=args.endpoint, token=args.token_file.read_text().strip(),
        use_tls=True, verify_tls=True,
    )
    report = run_placement_policy(json.loads(args.inventory.read_text()), connection,
                                  args.image, args.socket, args.output, args.placement)
    print(json.dumps({'status': report['status'], 'checks': report['checks']}), flush=True)


if __name__ == '__main__':
    main()
