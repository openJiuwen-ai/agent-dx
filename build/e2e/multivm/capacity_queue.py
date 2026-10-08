#!/usr/bin/env python3
"""Three-VM two-worker capacity, central queue, and release wakeup acceptance."""
from __future__ import annotations

import argparse
from concurrent.futures import ThreadPoolExecutor, TimeoutError as FutureTimeout
import json
import os
from pathlib import Path
import ssl
import time
import urllib.request
import uuid

if __package__:
    from .contract import verify_inventory
    from .sdk_accept import backend_ids, persisted_assignment, ssh, verify_machines
else:
    from contract import verify_inventory
    from sdk_accept import backend_ids, persisted_assignment, ssh, verify_machines


def pending_ids(endpoint, admin_key, ca):
    request = urllib.request.Request(
        'https://' + endpoint + '/global-scheduler/scheduling_queue',
        headers={'Authorization': 'Bearer ' + admin_key},
    )
    with urllib.request.urlopen(request, context=ssl.create_default_context(cafile=str(ca)),
                                timeout=5) as response:
        body = json.load(response)
    if body['count'] != len(body['instanceInfos']):
        raise AssertionError('queue count differs from returned request list')
    return {item['instanceID'] for item in body['instanceInfos']}


def run_capacity(inventory, connection, image, endpoint, admin_key, ca, output,
                 sandbox_factory=None, resource_reader=None, queue_reader=pending_ids,
                 remote=ssh, clock=time.monotonic, sleep=time.sleep,
                 pending_probe_seconds=2, socket='/run/sandboxd/sandboxd.sock'):
    verify_inventory(inventory)
    if sandbox_factory is None or resource_reader is None:
        from adx_sandbox import Sandbox, resources
        sandbox_factory = sandbox_factory or Sandbox
        resource_reader = resource_reader or resources
    control = next(machine for machine in inventory['machines'] if machine['role'] == 'control')
    workers = [machine for machine in inventory['machines'] if machine['role'] != 'control']
    report = {'status': 'failed', 'profile': 'multi-vm-capacity', 'checks': [],
              'holders': [], 'cleanup_errors': []}
    handles = []
    deleted = set()
    output.mkdir(parents=True, exist_ok=True)
    try:
        report['machines'] = verify_machines(inventory, remote)
        nodes = {node.id: node for node in resource_reader(connection=connection)}
        cpu = {}
        for worker in workers:
            node = nodes[worker['node_id']]
            available_cpu = node.allocatable.get('CPU', 0)
            available_memory = node.allocatable.get('Memory', 0)
            if node.status != 0 or available_cpu != int(available_cpu) \
                    or available_cpu < 500 or available_memory < 512:
                raise AssertionError(f"{worker['node_id']} lacks dedicated test capacity")
            cpu[worker['node_id']] = int(available_cpu)
        report['measured_cpu_millis'] = cpu
        for worker in workers:
            sandbox = sandbox_factory(
                image=image, runtime='runc', node_id=worker['node_id'],
                cpu=cpu[worker['node_id']], memory=256, idle_timeout=0,
                connection=connection, create_timeout=150,
            )
            handles.append(sandbox)
            assignment = persisted_assignment(control, sandbox.id, remote)
            if assignment.get('node_id') != worker['node_id'] or assignment.get('state') != 'Running':
                raise AssertionError(f'capacity holder assigned incorrectly: {assignment}')
            report['holders'].append({'id': sandbox.id, 'node_id': worker['node_id'],
                                      'cpu_millis': cpu[worker['node_id']]})
        report['checks'].append('two-worker-capacity-reserved')

        name = 'mv-queue-' + uuid.uuid4().hex[:12]
        def create_pending():
            return sandbox_factory(
                name=name, image=image, runtime='runc', cpu=250, memory=256,
                idle_timeout=0, connection=connection, schedule_timeout=90,
                create_timeout=150,
            )
        released = False
        with ThreadPoolExecutor(max_workers=1) as executor:
            future = executor.submit(create_pending)
            try:
                try:
                    unexpected = future.result(timeout=pending_probe_seconds)
                except FutureTimeout:
                    pass
                else:
                    handles.append(unexpected)
                    raise AssertionError('creation overcommitted full workers')
                deadline = clock() + 10
                while True:
                    matches = [identity for identity in queue_reader(endpoint, admin_key, ca)
                               if identity.endswith(name)]
                    if len(matches) == 1:
                        break
                    if clock() >= deadline:
                        raise AssertionError(f'{name} missing from central waiting queue')
                    sleep(.2)
                report['queued_id'] = matches[0]
                report['checks'].append('central-queue-observed')
                released_node = workers[0]['node_id']
                handles[0].kill()
                deleted.add(handles[0].id)
                released = True
                resumed = future.result(timeout=90)
                handles.append(resumed)
                assignment = persisted_assignment(control, resumed.id, remote)
                if assignment.get('node_id') != released_node or assignment.get('state') != 'Running':
                    raise AssertionError(f'queued request did not use released worker: {assignment}')
                result = resumed.commands.run('printf queue-wakeup')
                if result.exit_code != 0 or result.stdout != 'queue-wakeup':
                    raise AssertionError('resumed instance did not execute')
                report['resumed'] = {'id': resumed.id, 'node_id': assignment['node_id']}
                report['checks'].append('release-wakes-one-queued-request')
            finally:
                # A queue-probe error must not leave a create waiting until its
                # scheduler deadline, then silently materialize a live Sandbox.
                if not released:
                    handles[0].kill()
                    deleted.add(handles[0].id)
                    released = True
                try:
                    candidate = future.result(timeout=90)
                except Exception:
                    pass
                else:
                    if candidate not in handles:
                        handles.append(candidate)
        deadline = clock() + 10
        while report['queued_id'] in queue_reader(endpoint, admin_key, ca):
            if clock() >= deadline:
                raise AssertionError('completed create remained in waiting queue')
            sleep(.2)
        report['checks'].append('queue-drained')
    except Exception as error:
        report['error'] = f'{type(error).__name__}: {error}'
        raise
    finally:
        for sandbox in reversed(handles):
            try:
                if sandbox.id not in deleted:
                    sandbox.kill()
                    deleted.add(sandbox.id)
            except Exception as error:
                report['cleanup_errors'].append(f'{sandbox.id} delete: {error}')
            finally:
                try:
                    sandbox.close()
                except Exception as error:
                    report['cleanup_errors'].append(f'{sandbox.id} close: {error}')
        try:
            deadline = clock() + 15
            while handles:
                residual = {
                    sandbox.id: {
                        'record': persisted_assignment(control, sandbox.id, remote),
                        'backends': {
                            worker['node_id']: backend_ids(worker, socket, sandbox.id, remote)
                            for worker in workers
                        },
                    }
                    for sandbox in handles
                }
                if all(item['record']['state'] == 'Deleted'
                       and not item['record']['resources_held']
                       and not any(item['backends'].values()) for item in residual.values()):
                    report['checks'].append('backend-and-ledger-cleanup')
                    break
                if clock() >= deadline:
                    raise AssertionError(f'capacity cleanup incomplete: {residual}')
                sleep(.2)
        except Exception as error:
            report['cleanup_errors'].append(str(error))
        if not report.get('error') and not report['cleanup_errors']:
            report['status'] = 'passed'
        (output / 'capacity-queue-result.json').write_text(json.dumps(report, indent=2) + '\n')
        if report['cleanup_errors'] and not report.get('error'):
            raise RuntimeError('; '.join(report['cleanup_errors']))
    return report


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--inventory', required=True, type=Path)
    parser.add_argument('--endpoint', required=True)
    parser.add_argument('--token-file', required=True, type=Path)
    parser.add_argument('--admin-token-file', required=True, type=Path)
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
    report = run_capacity(
        json.loads(args.inventory.read_text()), connection, args.image, args.endpoint,
        args.admin_token_file.read_text().strip(), args.ca, args.output,
        socket=args.socket,
    )
    print(json.dumps({'status': report['status'], 'checks': report['checks']}), flush=True)


if __name__ == '__main__':
    main()
