#!/usr/bin/env python3
"""Three-VM quick adxlet restart with preserved sandboxd backend identity."""
from __future__ import annotations

import argparse
import json
import os
from pathlib import Path
import subprocess
import time

if __package__:
    from .contract import verify_inventory
    from .sdk_accept import backend_ids, persisted_assignment, ssh, verify_machines
    from .worker_failure import manager_pid, node_record, wait_until
else:
    from contract import verify_inventory
    from sdk_accept import backend_ids, persisted_assignment, ssh, verify_machines
    from worker_failure import manager_pid, node_record, wait_until


def restarted_state(control, worker, instance_id, old_pid, old_session,
                    old_generation, old_backend, socket, remote):
    try:
        pid = manager_pid(worker, remote)
    except (AssertionError, KeyError):
        return None
    node = node_record(control, worker['node_id'], remote)
    assignment = persisted_assignment(control, instance_id, remote)
    backends = backend_ids(worker, socket, instance_id, remote)
    if pid == old_pid or node['session']['id'] == old_session or not node['available'] \
            or not node['session']['routable']:
        return None
    if assignment.get('state') != 'Running' or assignment.get('node_id') != worker['node_id'] \
            or assignment.get('generation') != old_generation or backends != old_backend:
        raise AssertionError('quick worker restart changed instance ownership or backend identity')
    return {'old_pid': old_pid, 'new_pid': pid, 'old_session': old_session,
            'new_session': node['session']['id'], 'backend_ids': backends,
            'generation': old_generation}


def run_session_probe(executable, control, worker, instance_id, old_session, new_session):
    address = control.get('coordinator_rpc_address')
    if not isinstance(address, str) or not address:
        raise ValueError('control inventory needs coordinator_rpc_address for session fencing')
    command = [str(executable), address, worker['node_id'], instance_id,
               old_session, new_session]
    tls = control.get('session_probe_tls')
    if tls is not None:
        if not isinstance(tls, dict) or any(not tls.get(key)
                                            for key in ('ca', 'cert', 'key', 'server_name')):
            raise ValueError('session_probe_tls needs ca, cert, key and server_name')
        command.extend(str(tls[key]) for key in ('ca', 'cert', 'key', 'server_name'))
    completed = subprocess.run(command, capture_output=True, text=True,
                               timeout=30, check=False)
    if completed.returncode != 0:
        raise AssertionError('stale-session RPC probe failed: ' + completed.stderr[-1000:])
    try:
        result = json.loads(completed.stdout)
    except ValueError as error:
        raise AssertionError('stale-session RPC probe returned invalid JSON') from error
    if result.get('status') != 'passed' \
            or result.get('grpc_code') != 'FailedPrecondition' \
            or result.get('record_unchanged') is not True \
            or result.get('node_id') != worker['node_id'] \
            or result.get('instance_id') != instance_id \
            or result.get('old_session') != old_session \
            or result.get('new_session') != new_session:
        raise AssertionError(f'stale-session RPC probe did not prove fencing: {result}')
    return result


def run_restart(inventory, connection, image, socket, output,
                sandbox_factory=None, remote=ssh, wait=wait_until, probe=None):
    verify_inventory(inventory)
    if sandbox_factory is None:
        from adx_sandbox import Sandbox
        sandbox_factory = Sandbox
    machines = {machine['role']: machine for machine in inventory['machines']}
    control = machines['control']
    workers = (machines['worker-1'], machines['worker-2'])
    restarted = workers[1]
    report = {'status': 'failed', 'profile': 'multi-vm-worker-restart',
              'checks': [], 'instances': [], 'cleanup_errors': []}
    handles = []
    output.mkdir(parents=True, exist_ok=True)
    try:
        report['machines'] = verify_machines(inventory, remote)
        for worker in workers:
            sandbox = sandbox_factory(
                image=image, runtime='runc', node_id=worker['node_id'],
                cpu=500, memory=512, idle_timeout=0,
                connection=connection, create_timeout=150,
            )
            handles.append(sandbox)
            assignment = persisted_assignment(control, sandbox.id, remote)
            if assignment.get('node_id') != worker['node_id'] or assignment.get('state') != 'Running':
                raise AssertionError(f'initial assignment mismatch for {sandbox.id}')
            result = sandbox.commands.run('printf before-restart')
            if result.exit_code != 0 or result.stdout != 'before-restart':
                raise AssertionError(f'initial route failed for {sandbox.id}')
            report['instances'].append({'id': sandbox.id, 'node_id': worker['node_id']})
        report['checks'].append('both-workers-ready')
        affected = handles[1]
        old_assignment = persisted_assignment(control, affected.id, remote)
        old_backends = backend_ids(restarted, socket, affected.id, remote)
        if len(old_backends) != 1:
            raise AssertionError(f'worker has no unique backend for {affected.id}')
        old_session = node_record(control, restarted['node_id'], remote)['session']['id']
        old_pid = manager_pid(restarted, remote)
        remote(restarted, 'sudo', '-n', 'kill', '-TERM', str(old_pid))
        report['restart'] = wait(
            lambda: restarted_state(control, restarted, affected.id, old_pid,
                                    old_session, old_assignment['generation'], old_backends,
                                    socket, remote),
            'adxlet did not restart and reattach before heartbeat expiry', 25)
        report['checks'].append('new-session-same-backend-and-generation')
        if probe is not None:
            fencing = probe(control, restarted, affected.id,
                            old_session, report['restart']['new_session'])
            if fencing.get('status') != 'passed' \
                    or fencing.get('grpc_code') != 'FailedPrecondition':
                raise AssertionError('stale-session probe did not prove fencing')
            report['fencing'] = fencing
            report['checks'].append('old-session-commit-rejected')
        for sandbox in handles:
            result = sandbox.commands.run('printf after-restart')
            if result.exit_code != 0 or result.stdout != 'after-restart':
                raise AssertionError(f'{sandbox.id} route failed after worker restart')
        report['checks'].append('both-workers-still-serving')
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
            deadline = time.monotonic() + 15
            while handles:
                residual = {
                    sandbox.id: {
                        worker['node_id']: backend_ids(worker, socket, sandbox.id, remote)
                        for worker in workers
                    }
                    for sandbox in handles
                }
                if not any(ids for nodes in residual.values() for ids in nodes.values()):
                    break
                if time.monotonic() >= deadline:
                    raise AssertionError(f'backend remains after restart test: {residual}')
                time.sleep(.2)
        except Exception as error:
            report['cleanup_errors'].append(str(error))
        if not report.get('error') and not report['cleanup_errors']:
            report['status'] = 'passed'
        (output / 'worker-restart-result.json').write_text(json.dumps(report, indent=2) + '\n')
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
    parser.add_argument('--session-probe', type=Path)
    args = parser.parse_args()
    if args.session_probe and (not args.session_probe.is_file()
                               or not os.access(args.session_probe, os.X_OK)):
        parser.error('--session-probe must name a built stale_session_probe executable')
    os.environ['SSL_CERT_FILE'] = str(args.ca.resolve())
    from adx_sandbox import ConnectionConfig
    connection = ConnectionConfig(
        server_address=args.endpoint, token=args.token_file.read_text().strip(),
        use_tls=True, verify_tls=True,
    )
    result = run_restart(json.loads(args.inventory.read_text()), connection,
                         args.image, args.socket, args.output,
                         probe=(lambda *probe_args: run_session_probe(args.session_probe, *probe_args))
                         if args.session_probe else None)
    print(json.dumps({'status': result['status'], 'checks': result['checks']}), flush=True)


if __name__ == '__main__':
    main()
