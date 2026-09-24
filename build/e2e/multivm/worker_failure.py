#!/usr/bin/env python3
"""Three-VM worker heartbeat failure and stale backend reconciliation."""
from __future__ import annotations

import argparse
import json
import os
from pathlib import Path
import time

if __package__:
    from .contract import verify_inventory
    from .sdk_accept import (backend_ids, persisted_assignment, ssh,
                             verify_machines, wait_for_backends)
else:
    from contract import verify_inventory
    from sdk_accept import (backend_ids, persisted_assignment, ssh,
                            verify_machines, wait_for_backends)


def node_record(control, node_id, remote=ssh):
    config = control.get('deployment_config', '/opt/adx/config/deployment.yaml')
    return json.loads(remote(control, '/opt/adx/current/bin/adx-inspect', '-c', config,
                             'node', 'get', node_id))


def manager_pid(worker, remote=ssh):
    config = worker.get('deployment_config', '/opt/adx/config/deployment.yaml')
    status = json.loads(remote(worker, '/opt/adx/current/bin/adxctl', 'status',
                               '--config', config))
    managers = [service for service in status['services'] if service['role'] == 'adxlet']
    if len(managers) != 1 or not isinstance(managers[0].get('pid'), int) \
            or managers[0]['pid'] <= 0:
        raise AssertionError('worker has no unique running adxlet process')
    return managers[0]['pid']


def wait_until(check, description, timeout, clock=time.monotonic, sleep=time.sleep):
    deadline = clock() + timeout
    while True:
        value = check()
        if value:
            return value
        if clock() >= deadline:
            raise TimeoutError(description)
        sleep(.5)


def run_failure(inventory, connection, image, socket, output,
                sandbox_factory=None, sandbox_error=None, remote=ssh,
                failure_timeout=90, recovery_timeout=90,
                wait=wait_until):
    """Freeze only the selected worker's manager; never stop the VM/sandboxd."""
    verify_inventory(inventory)
    if sandbox_factory is None or sandbox_error is None:
        from adx_sandbox import Sandbox, SandboxError
        sandbox_factory = sandbox_factory or Sandbox
        sandbox_error = sandbox_error or SandboxError
    machines = {machine['role']: machine for machine in inventory['machines']}
    control = machines['control']
    healthy_worker, affected_worker = machines['worker-1'], machines['worker-2']
    report = {'status': 'failed', 'profile': 'multi-vm-worker-failure',
              'checks': [], 'instances': [], 'cleanup_errors': []}
    handles = []
    frozen = False
    output.mkdir(parents=True, exist_ok=True)
    try:
        report['machines'] = verify_machines(inventory, remote)
        report['checks'].append('machine-identity')
        for worker in (healthy_worker, affected_worker):
            sandbox = sandbox_factory(
                image=image, runtime='runc', node_id=worker['node_id'],
                cpu=500, memory=512, idle_timeout=0,
                connection=connection, create_timeout=150,
            )
            handles.append(sandbox)
            report['instances'].append({'id': sandbox.id, 'node_id': worker['node_id']})
        expected = {item['id']: item['node_id'] for item in report['instances']}
        report['before_backends'] = wait_for_backends(
            (healthy_worker, affected_worker), socket, expected, remote)
        for sandbox in handles:
            assignment = persisted_assignment(control, sandbox.id, remote)
            if assignment.get('node_id') != expected[sandbox.id] or assignment.get('state') != 'Running':
                raise AssertionError(f'initial assignment mismatch: {assignment}')
            baseline = sandbox.commands.run('printf baseline-route')
            if baseline.exit_code != 0 or baseline.stdout != 'baseline-route':
                raise AssertionError(f'initial route to {sandbox.id} did not work')
        report['checks'].append('both-workers-running')
        old_session = node_record(control, affected_worker['node_id'], remote)['session']['id']
        pid = manager_pid(affected_worker, remote)
        remote(affected_worker, 'sudo', '-n', 'kill', '-STOP', str(pid))
        frozen = True
        report['frozen_pid'] = pid

        affected = handles[1]
        report['failed_assignment'] = wait(
            lambda: _failed_assignment(control, affected_worker, affected.id, remote),
            'worker heartbeat did not invalidate the instance', failure_timeout)
        report['checks'].append('heartbeat-expiry')
        try:
            affected.commands.run('printf stale-route-must-not-execute')
        except sandbox_error:
            report['checks'].append('route-withdrawn')
        else:
            raise AssertionError('failed worker remained reachable through Ingress')
        survivor = handles[0].commands.run('printf healthy-worker')
        if survivor.exit_code != 0 or survivor.stdout != 'healthy-worker':
            raise AssertionError('healthy worker stopped serving during peer failure')
        report['checks'].append('healthy-worker-unaffected')

        remote(affected_worker, 'sudo', '-n', 'kill', '-CONT', str(pid))
        frozen = False
        report['recovered_node'] = wait(
            lambda: _reconciled_node(control, affected_worker, affected.id,
                                     old_session, socket, remote),
            'returning worker did not reconcile stale backend and reopen admission',
            recovery_timeout)
        report['checks'].append('stale-backend-cleanup-before-readmission')
        try:
            affected.commands.run('printf old-route-must-stay-invalid')
        except sandbox_error:
            report['checks'].append('old-route-still-invalid-after-rejoin')
        else:
            raise AssertionError('old instance route revived after worker rejoined')
        replacement = sandbox_factory(
            image=image, runtime='runc', node_id=affected_worker['node_id'],
            cpu=500, memory=512, idle_timeout=0,
            connection=connection, create_timeout=150,
        )
        handles.append(replacement)
        report['instances'].append({'id': replacement.id, 'node_id': affected_worker['node_id']})
        assignment = persisted_assignment(control, replacement.id, remote)
        if assignment.get('node_id') != affected_worker['node_id'] or assignment.get('state') != 'Running':
            raise AssertionError(f'new instance assigned to wrong worker: {assignment}')
        report['replacement_backend'] = wait_for_backends(
            (healthy_worker, affected_worker), socket,
            {replacement.id: affected_worker['node_id']}, remote)
        result = replacement.commands.run('printf resumed-admission')
        if result.exit_code != 0 or result.stdout != 'resumed-admission':
            raise AssertionError('returning worker failed to serve a fresh instance')
        report['checks'].append('new-admission-after-reconciliation')
    except Exception as error:
        report['error'] = f'{type(error).__name__}: {error}'
        raise
    finally:
        if frozen:
            try:
                remote(affected_worker, 'sudo', '-n', 'kill', '-CONT', str(pid))
            except Exception as error:
                report['cleanup_errors'].append(f'resume adxlet: {error}')
        affected_handle = handles[1] if len(handles) > 1 else None
        for sandbox in reversed(handles):
            try:
                sandbox.kill()
            except sandbox_error as error:
                # An invalidated instance may already be terminal. Physical
                # cleanup below remains the authority for this test.
                if sandbox is not affected_handle:
                    report['cleanup_errors'].append(f'{sandbox.id} delete: {error}')
            except Exception as error:
                report['cleanup_errors'].append(f'{sandbox.id} delete: {error}')
            finally:
                try:
                    sandbox.close()
                except Exception as error:
                    report['cleanup_errors'].append(f'{sandbox.id} close: {error}')
        try:
            deadline = time.monotonic() + 15
            while handles:
                residual = {
                    sandbox.id: {
                        worker['node_id']: backend_ids(worker, socket, sandbox.id, remote)
                        for worker in (healthy_worker, affected_worker)
                    }
                    for sandbox in handles
                }
                if not any(ids for nodes in residual.values() for ids in nodes.values()):
                    break
                if time.monotonic() >= deadline:
                    raise AssertionError(f'test-owned backend remains: {residual}')
                time.sleep(.2)
        except Exception as error:
            report['cleanup_errors'].append(str(error))
        if not report.get('error') and not report['cleanup_errors']:
            report['status'] = 'passed'
        (output / 'worker-failure-result.json').write_text(json.dumps(report, indent=2) + '\n')
        if report['cleanup_errors'] and not report.get('error'):
            raise RuntimeError('; '.join(report['cleanup_errors']))
    return report


def _failed_assignment(control, worker, instance_id, remote):
    node = node_record(control, worker['node_id'], remote)
    assignment = persisted_assignment(control, instance_id, remote)
    if node['available'] or node['session']['routable']:
        return None
    if assignment.get('node_id') != worker['node_id']:
        raise AssertionError('failed instance changed owner without checkpoint recovery')
    if assignment.get('state') != 'Failed' or not assignment.get('invalidated') \
            or assignment.get('resources_held'):
        return None
    return assignment


def _reconciled_node(control, worker, instance_id, old_session, socket, remote):
    node = node_record(control, worker['node_id'], remote)
    backends = backend_ids(worker, socket, instance_id, remote)
    if node['available'] and backends:
        raise AssertionError('returning worker reopened admission before stale backend cleanup')
    if not node['available'] or not node['session']['routable'] or backends:
        return None
    if node['session']['id'] == old_session:
        raise AssertionError('returning worker reused the expired node session')
    return node


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
    inventory = json.loads(args.inventory.read_text())
    result = run_failure(inventory, connection, args.image, args.socket, args.output)
    print(json.dumps({'status': result['status'], 'checks': result['checks']}), flush=True)


if __name__ == '__main__':
    main()
