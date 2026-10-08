#!/usr/bin/env python3
"""Restart supervised control roles while two worker backends remain live."""
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
    from .worker_failure import node_record, wait_until
else:
    from contract import verify_inventory
    from sdk_accept import backend_ids, persisted_assignment, ssh, verify_machines
    from worker_failure import node_record, wait_until


ROLES = ('coordinator', 'apiserver', 'redis')
OPTIONAL_ROLES = ('ingress',)


def control_status(control, remote=ssh):
    config = control.get('deployment_config', '/opt/adx/config/deployment.yaml')
    return json.loads(remote(control, '/opt/adx/current/bin/adxctl', 'status',
                             '--config', config))


def service_pid(control, role, remote=ssh):
    services = [service for service in control_status(control, remote)['services']
                if service['role'] == role]
    if len(services) != 1 or not isinstance(services[0].get('pid'), int) \
            or services[0]['pid'] <= 0:
        raise AssertionError(f'control node has no unique supervised {role} process')
    return services[0]['pid']


def summary(control, remote=ssh):
    config = control.get('deployment_config', '/opt/adx/config/deployment.yaml')
    result = json.loads(remote(control, '/opt/adx/current/bin/adx-inspect',
                               '-c', config, 'summary'))
    if not isinstance(result.get('epoch'), int) or result['epoch'] < 1:
        raise AssertionError(f'invalid Coordinator epoch: {result}')
    return result


def worker_routable(control, workers, remote=ssh):
    return all((record := node_record(control, worker['node_id'], remote))['available']
               and record['session']['routable'] for worker in workers)


def route_ready(sandboxes):
    try:
        for sandbox in sandboxes:
            result = sandbox.commands.run('printf control-restart')
            if (result.exit_code, result.stdout) != (0, 'control-restart'):
                raise AssertionError(f'{sandbox.id} route returned unexpected data')
            if sandbox.files.read('/tmp/adx-control-restart-marker', format='bytes') != b'marker':
                raise AssertionError(f'{sandbox.id} file changed across control restart')
    except AssertionError:
        raise
    except Exception:
        return None
    return True


def run_control_restarts(inventory, connection, image, socket, output,
                         sandbox_factory=None, remote=ssh, wait=wait_until, roles=None):
    verify_inventory(inventory)
    roles = ROLES if roles is None else tuple(roles)
    if not roles or len(set(roles)) != len(roles) \
            or any(role not in ROLES + OPTIONAL_ROLES for role in roles):
        raise ValueError('select distinct supervised control roles')
    if sandbox_factory is None:
        from adx_sandbox import Sandbox
        sandbox_factory = Sandbox
    machines = {machine['role']: machine for machine in inventory['machines']}
    control = machines['control']
    workers = (machines['worker-1'], machines['worker-2'])
    report = {'status': 'failed', 'profile': 'multi-vm-control-restart',
              'checks': [], 'instances': [], 'restarts': {}, 'cleanup_errors': []}
    handles = []
    output.mkdir(parents=True, exist_ok=True)

    def physical(instance_id):
        return {worker['node_id']: backend_ids(worker, socket, instance_id, remote)
                for worker in workers}

    try:
        report['machines'] = verify_machines(inventory, remote)
        for role in roles:
            service_pid(control, role, remote)
        epoch = summary(control, remote)['epoch']
        report['checks'].append('managed-control-services-ready')
        baseline = {}
        for worker in workers:
            sandbox = sandbox_factory(
                image=image, runtime='runc', node_id=worker['node_id'],
                cpu=500, memory=512, idle_timeout=0,
                connection=connection, create_timeout=150,
            )
            handles.append(sandbox)
            sandbox.files.write('/tmp/adx-control-restart-marker', b'marker')
            assignment = persisted_assignment(control, sandbox.id, remote)
            if assignment.get('node_id') != worker['node_id'] \
                    or assignment.get('state') != 'Running' \
                    or not assignment.get('resources_held'):
                raise AssertionError(f'initial ownership invalid: {assignment}')
            backends = physical(sandbox.id)
            if len(backends[worker['node_id']]) != 1 \
                    or any(ids for node_id, ids in backends.items()
                           if node_id != worker['node_id']):
                raise AssertionError(f'{sandbox.id} has no unique physical backend')
            baseline[sandbox.id] = (assignment['node_id'], assignment['generation'], backends)
            report['instances'].append({'id': sandbox.id, 'node_id': worker['node_id']})
        if not worker_routable(control, workers, remote) or not route_ready(handles):
            raise AssertionError('initial workers or SDK routes are unavailable')
        report['checks'].append('both-workers-running')

        for role in roles:
            old_pid = service_pid(control, role, remote)
            old_epoch = epoch
            remote(control, 'sudo', '-n', 'kill', '-KILL', str(old_pid))

            def restarted():
                try:
                    new_pid = service_pid(control, role, remote)
                    new_epoch = summary(control, remote)['epoch']
                    routable = worker_routable(control, workers, remote)
                except (OSError, subprocess.SubprocessError, AssertionError,
                        KeyError, ValueError):
                    return None
                if new_pid == old_pid or not routable:
                    return None
                if role == 'coordinator' and new_epoch <= old_epoch:
                    return None
                if role in ('apiserver', 'ingress') and new_epoch != old_epoch:
                    raise AssertionError(f'Coordinator epoch changed during {role} restart')
                return new_pid, new_epoch

            new_pid, epoch = wait(restarted, f'{role} did not restart and restore control state', 90)
            for sandbox in handles:
                owner, generation, backends = baseline[sandbox.id]
                assignment = persisted_assignment(control, sandbox.id, remote)
                if assignment.get('node_id') != owner \
                        or assignment.get('generation') != generation \
                        or assignment.get('state') != 'Running' \
                        or not assignment.get('resources_held'):
                    raise AssertionError(f'{sandbox.id} ownership changed after {role} restart')
                if physical(sandbox.id) != backends:
                    raise AssertionError(f'{sandbox.id} backend changed after {role} restart')
            wait(lambda: route_ready(handles), f'public SDK did not recover after {role} restart', 60)
            report['restarts'][role] = {'previous_pid': old_pid, 'pid': new_pid,
                                        'epoch_before': old_epoch, 'epoch_after': epoch}
            report['checks'].append(role + '-restart-preserved-ownership-and-route')
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
                        'assignment': persisted_assignment(control, sandbox.id, remote),
                        'backends': {worker['node_id']: backend_ids(worker, socket, sandbox.id, remote)
                                     for worker in workers},
                    }
                    for sandbox in handles
                }
                if all(item['assignment']['state'] == 'Deleted'
                       and not item['assignment']['resources_held']
                       and not any(item['backends'].values()) for item in residual.values()):
                    report['checks'].append('owned-backends-and-capacity-released')
                    break
                if time.monotonic() >= deadline:
                    raise AssertionError(f'control-restart cleanup incomplete: {residual}')
                time.sleep(.2)
        except Exception as error:
            report['cleanup_errors'].append(str(error))
        if not report.get('error') and not report['cleanup_errors']:
            report['status'] = 'passed'
        (output / 'control-restart-result.json').write_text(json.dumps(report, indent=2) + '\n')
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
    parser.add_argument('--role', dest='roles', action='append',
                        choices=ROLES + OPTIONAL_ROLES)
    args = parser.parse_args()
    os.environ['SSL_CERT_FILE'] = str(args.ca.resolve())
    from adx_sandbox import ConnectionConfig
    connection = ConnectionConfig(
        server_address=args.endpoint, token=args.token_file.read_text().strip(),
        use_tls=True, verify_tls=True,
    )
    report = run_control_restarts(json.loads(args.inventory.read_text()), connection,
                                  args.image, args.socket, args.output, roles=args.roles)
    print(json.dumps({'status': report['status'], 'checks': report['checks']}), flush=True)


if __name__ == '__main__':
    main()
