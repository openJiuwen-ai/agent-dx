#!/usr/bin/env python3
"""Final three-VM acceptance: stop workers before the control node."""
from __future__ import annotations

import argparse
import json
import os
from pathlib import Path
import subprocess

if __package__:
    from .contract import verify_inventory
    from .sdk_accept import persisted_assignment, ssh, verify_machines
    from .worker_failure import wait_until
else:
    from contract import verify_inventory
    from sdk_accept import persisted_assignment, ssh, verify_machines
    from worker_failure import wait_until


def service_status(machine, remote=ssh):
    config = machine.get('deployment_config', '/opt/adx/config/deployment.yaml')
    return json.loads(remote(machine, '/opt/adx/current/bin/adxctl', 'status',
                             '--config', config))


def backend_inventory(worker, socket, remote=ssh):
    configured_socket = worker.get('sandboxd_socket', socket)
    lines = remote(worker, 'sbox', '-a', configured_socket, 'list').splitlines()
    return [line.split()[0] for line in lines[1:] if line.strip()]


def processes_gone(machine, pids, remote=ssh):
    for pid in pids:
        try:
            command = remote(machine, 'ps', '-p', str(pid), '-o', 'comm=').strip()
        except subprocess.SubprocessError:
            continue
        if command:
            return None
    return True


def stop_services(machine, remote=ssh, wait=wait_until):
    services = service_status(machine, remote)['services']
    pids = [service['pid'] for service in services if isinstance(service.get('pid'), int)
            and service['pid'] > 0]
    if not pids:
        raise AssertionError(f"{machine['role']} has no supervised child processes")
    config = machine.get('deployment_config', '/opt/adx/config/deployment.yaml')
    remote(machine, 'sudo', '-n', '/opt/adx/current/bin/adxctl', 'stop', '--config', config)
    wait(lambda: processes_gone(machine, pids, remote),
         f"{machine['role']} retained a supervised child", 90)
    return pids


def run_route_probe(executable, control):
    address = control.get('coordinator_rpc_address')
    if not isinstance(address, str) or not address:
        raise ValueError('control inventory needs coordinator_rpc_address for route probing')
    command = [str(executable), address]
    tls = control.get('route_probe_tls')
    if tls is not None:
        if not isinstance(tls, dict) or any(not tls.get(key)
                                            for key in ('ca', 'cert', 'key', 'server_name')):
            raise ValueError('route_probe_tls needs ca, cert, key and server_name')
        command.extend(str(tls[key]) for key in ('ca', 'cert', 'key', 'server_name'))
    completed = subprocess.run(command, capture_output=True, text=True,
                               timeout=30, check=False)
    if completed.returncode != 0:
        raise AssertionError('route snapshot RPC probe failed: ' + completed.stderr[-1000:])
    try:
        result = json.loads(completed.stdout)
    except ValueError as error:
        raise AssertionError('route snapshot RPC probe returned invalid JSON') from error
    if result.get('status') != 'passed' or result.get('reset') is not True \
            or not isinstance(result.get('published_routes'), int) \
            or result['published_routes'] < 0 \
            or not isinstance(result.get('revision'), int):
        raise AssertionError(f'route snapshot RPC probe returned invalid evidence: {result}')
    return result


def run_stop(inventory, connection, image, socket, output, dedicated=False,
             sandbox_factory=None, remote=ssh, wait=wait_until, route_probe=None):
    if not dedicated:
        raise ValueError('stop acceptance requires explicit dedicated-VM confirmation')
    verify_inventory(inventory)
    if sandbox_factory is None:
        from adx_sandbox import Sandbox
        sandbox_factory = Sandbox
    machines = {machine['role']: machine for machine in inventory['machines']}
    control = machines['control']
    workers = (machines['worker-1'], machines['worker-2'])
    report = {'status': 'failed', 'profile': 'multi-vm-stop',
              'checks': [], 'instances': [], 'stop_order': [], 'cleanup_errors': []}
    handles = {}
    output.mkdir(parents=True, exist_ok=True)
    try:
        report['machines'] = verify_machines(inventory, remote)
        for machine in (*workers, control):
            services = service_status(machine, remote)['services']
            if not any(isinstance(service.get('pid'), int) and service['pid'] > 0
                       for service in services):
                raise AssertionError(f"{machine['role']} has no running services")
        for worker in workers:
            if backend_inventory(worker, socket, remote):
                raise AssertionError(f"{worker['role']} is not empty before destructive test")
        report['checks'].append('dedicated-empty-workers')
        for worker in workers:
            sandbox = sandbox_factory(
                image=image, runtime='runc', node_id=worker['node_id'],
                cpu=500, memory=512, idle_timeout=0,
                connection=connection, create_timeout=150,
            )
            handles[worker['role']] = sandbox
            assignment = persisted_assignment(control, sandbox.id, remote)
            if assignment.get('node_id') != worker['node_id'] \
                    or assignment.get('state') != 'Running':
                raise AssertionError(f'initial ownership mismatch: {assignment}')
            if len(backend_inventory(worker, socket, remote)) != 1:
                raise AssertionError(f"{worker['role']} has no unique backend")
            result = sandbox.commands.run('printf still-running')
            if (result.exit_code, result.stdout) != (0, 'still-running'):
                raise AssertionError(f'{sandbox.id} did not serve before stop')
            report['instances'].append({'id': sandbox.id, 'node_id': worker['node_id']})
        report['checks'].append('both-workers-serving')

        for stopped_worker, surviving_worker in ((workers[1], workers[0]),
                                                  (workers[0], None)):
            report['stopped_pids_' + stopped_worker['role']] = stop_services(
                stopped_worker, remote, wait)
            report['stop_order'].append(stopped_worker['role'])
            wait(lambda: not backend_inventory(stopped_worker, socket, remote),
                 f"{stopped_worker['role']} retained a backend", 30)
            affected = handles[stopped_worker['role']]
            wait(lambda: persisted_assignment(control, affected.id, remote).get('state') == 'Deleted'
                 and not persisted_assignment(control, affected.id, remote).get('resources_held'),
                 f'{affected.id} persisted cleanup missing', 30)
            try:
                affected.commands.run('printf should-not-route')
            except Exception:
                pass
            else:
                raise AssertionError(f'{affected.id} remained routable after worker stop')
            if surviving_worker is not None:
                survivor = handles[surviving_worker['role']]
                result = survivor.commands.run('printf still-running')
                if (result.exit_code, result.stdout) != (0, 'still-running'):
                    raise AssertionError('surviving worker did not continue serving')
            report['checks'].append(stopped_worker['role'] + '-drained-and-route-withdrawn')

        report['final_state'] = {
            'backend_instances': {
                worker['role']: len(backend_inventory(worker, socket, remote))
                for worker in workers
            },
        }
        if route_probe is not None:
            snapshot = wait(
                lambda: (result if result['published_routes'] == 0 else None)
                if (result := route_probe(control)) else None,
                'published routes remain after worker shutdown', 30)
            report['route_snapshot'] = snapshot
            report['final_state']['published_routes'] = snapshot['published_routes']
            report['checks'].append('published-route-catalog-empty')
        report['stopped_pids_control'] = stop_services(control, remote, wait)
        report['stop_order'].append('control')
        report['checks'].append('control-stopped-last')
    except Exception as error:
        report['error'] = f'{type(error).__name__}: {error}'
        raise
    finally:
        if report.get('error') and 'control' not in report['stop_order']:
            for role, sandbox in handles.items():
                if role not in report['stop_order']:
                    try:
                        sandbox.kill()
                    except Exception as error:
                        report['cleanup_errors'].append(f'{sandbox.id} delete: {error}')
        for sandbox in handles.values():
            try:
                sandbox.close()
            except Exception as error:
                report['cleanup_errors'].append(f'{sandbox.id} close: {error}')
        if not report.get('error') and not report['cleanup_errors']:
            report['status'] = 'passed'
        (output / 'stop-result.json').write_text(json.dumps(report, indent=2) + '\n')
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
    parser.add_argument('--confirm-dedicated', action='store_true', required=True)
    parser.add_argument('--route-probe', type=Path)
    args = parser.parse_args()
    if args.route_probe and (not args.route_probe.is_file()
                             or not os.access(args.route_probe, os.X_OK)):
        parser.error('--route-probe must name a built route_snapshot_probe executable')
    os.environ['SSL_CERT_FILE'] = str(args.ca.resolve())
    from adx_sandbox import ConnectionConfig
    connection = ConnectionConfig(
        server_address=args.endpoint, token=args.token_file.read_text().strip(),
        use_tls=True, verify_tls=True,
    )
    report = run_stop(json.loads(args.inventory.read_text()), connection,
                      args.image, args.socket, args.output, dedicated=args.confirm_dedicated,
                      route_probe=(lambda control: run_route_probe(args.route_probe, control))
                      if args.route_probe else None)
    print(json.dumps({'status': report['status'], 'checks': report['checks']}), flush=True)


if __name__ == '__main__':
    main()
