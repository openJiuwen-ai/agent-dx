#!/usr/bin/env python3
"""Run public-SDK placement and data-path checks on three named Linux VMs.

The machines and ADX services must already be provisioned. This program does
not stop shared services; it removes only the Sandboxes it creates.
"""
from __future__ import annotations

import argparse
import hashlib
import ipaddress
import json
import os
from pathlib import Path
import shlex
import subprocess
import tarfile
import time

if __package__:
    from .contract import verify_inventory
else:
    from contract import verify_inventory


def ssh(machine, *command, timeout=20):
    return subprocess.check_output(
        ['ssh', '-o', 'BatchMode=yes', '-o', 'StrictHostKeyChecking=yes',
         '-o', 'ConnectTimeout=5', '--', machine['ssh_target'], shlex.join(command)],
        text=True, timeout=timeout,
    )


def verify_machines(inventory, remote=ssh, release_manifest=None):
    """Bind configured roles to live VM identities before creating anything."""
    observed = []
    for machine in inventory['machines']:
        machine_id = remote(machine, 'cat', '/etc/machine-id').strip()
        hostname = remote(machine, 'hostname').strip()
        addresses = json.loads(remote(machine, 'ip', '-j', 'address', 'show'))
        assigned = {
            item['local'] for interface in addresses
            for item in interface.get('addr_info', []) if 'local' in item
        }
        expected = str(ipaddress.ip_address(machine['address']))
        if (machine_id, hostname) != (machine['machine_id'], machine['hostname']):
            raise AssertionError(f"{machine['role']} identity differs from inventory")
        if expected not in assigned:
            raise AssertionError(f"{machine['role']} address is not assigned: {expected}")
        manifest = json.loads(remote(machine, 'cat', '/opt/adx/current/manifest.json'))
        artifacts = inventory['artifacts']
        if (manifest.get('commit'), manifest.get('target')) != (artifacts['commit'], artifacts['target']):
            raise AssertionError(f"{machine['role']} installed release differs from inventory")
        if release_manifest is not None and manifest != release_manifest:
            raise AssertionError(f"{machine['role']} installed manifest differs from release archive")
        observed.append({'role': machine['role'], 'machine_id': machine_id,
                         'hostname': hostname, 'address': expected,
                         'release_commit': manifest['commit'], 'release_target': manifest['target']})
    return observed


def backend_ids(machine, socket, instance_id, remote=ssh):
    lines = remote(machine, 'sbox', '-a', str(machine.get('sandboxd_socket', socket)), 'list',
                   '--label', 'adx.environment_id=' + instance_id).splitlines()
    return [line.split()[0] for line in lines[1:] if line.strip()]


def persisted_assignment(control, instance_id, remote=ssh):
    config = control.get('deployment_config', '/opt/adx/config/deployment.yaml')
    return json.loads(remote(control, '/opt/adx/current/bin/adx-inspect', '-c', config,
                             'environment', 'get', instance_id))


def inspect_release(release, expected_sha256):
    hasher = hashlib.sha256()
    with release.open('rb') as archive:
        for chunk in iter(lambda: archive.read(1024 * 1024), b''):
            hasher.update(chunk)
    digest = hasher.hexdigest()
    if digest != expected_sha256:
        raise AssertionError('release archive SHA256 differs from inventory')
    with tarfile.open(release, 'r:gz') as archive:
        member = next((item for item in archive if item.name in ('manifest.json', './manifest.json')), None)
        if member is None or not member.isfile():
            raise ValueError('release archive has no manifest.json')
        manifest = json.load(archive.extractfile(member))
    return digest, manifest


def wait_for_backends(workers, socket, expected, remote=ssh, timeout=15):
    deadline = time.monotonic() + timeout
    while True:
        observed = {
            instance_id: {
                worker['node_id']: backend_ids(worker, socket, instance_id, remote)
                for worker in workers
            }
            for instance_id in expected
        }
        if all(len(by_node[node_id]) == 1 and
               all(not ids for other, ids in by_node.items() if other != node_id)
               for instance_id, node_id in expected.items()
               for by_node in (observed[instance_id],)):
            return observed
        if time.monotonic() >= deadline:
            raise AssertionError(f'physical backend placement mismatch: {observed}')
        time.sleep(.2)


def run_acceptance(inventory, connection, image, socket, output,
                   sandbox_factory=None, resource_reader=None, remote=ssh,
                   placement_timeout=15, release=None):
    """Public SDK -> Ingress -> separate worker Relays and Execd processes."""
    verify_inventory(inventory)
    if sandbox_factory is None or resource_reader is None:
        from adx_sandbox import Sandbox, resources
        sandbox_factory = sandbox_factory or Sandbox
        resource_reader = resource_reader or resources
    workers = [machine for machine in inventory['machines'] if machine['role'] != 'control']
    control = next(machine for machine in inventory['machines'] if machine['role'] == 'control')
    report = {'status': 'failed', 'profile': 'multi-vm-sdk', 'checks': [],
              'instances': [], 'cleanup_errors': []}
    handles = []
    output.mkdir(parents=True, exist_ok=True)
    try:
        release_manifest = None
        if release is not None:
            digest, release_manifest = inspect_release(
                release, inventory['artifacts']['release_sha256'])
            report['release_sha256'] = digest
        report['machines'] = verify_machines(inventory, remote, release_manifest)
        report['checks'].append('machine-identity')
        nodes = {node.id: node for node in resource_reader(connection=connection)}
        for worker in workers:
            node = nodes[worker['node_id']]
            if node.status != 0:
                raise AssertionError(f"{worker['node_id']} is not accepting instances")
        report['checks'].append('resource-discovery')
        for worker in workers:
            sandbox = sandbox_factory(
                image=image, runtime='runc', node_id=worker['node_id'],
                cpu=500, memory=512, idle_timeout=0,
                connection=connection, create_timeout=150,
            )
            handles.append(sandbox)
            if not sandbox.is_running():
                raise AssertionError(f'{sandbox.id} did not reach running')
            report['instances'].append({'id': sandbox.id, 'node_id': worker['node_id']})
        expected = {item['id']: item['node_id'] for item in report['instances']}
        report['assignments'] = {}
        for instance_id, node_id in expected.items():
            assignment = persisted_assignment(control, instance_id, remote)
            if assignment.get('node_id') != node_id or assignment.get('state') != 'Running' \
                    or not assignment.get('resources_held'):
                raise AssertionError(f'persisted assignment mismatch for {instance_id}: {assignment}')
            report['assignments'][instance_id] = assignment
        report['backends'] = wait_for_backends(workers, socket, expected, remote,
                                               timeout=placement_timeout)
        report['checks'].append('physical-placement')
        for sandbox in handles:
            command = sandbox.commands.run("printf 'adx-three-vm'; printf 'stderr' >&2; exit 7")
            if (command.stdout, command.stderr, command.exit_code) != ('adx-three-vm', 'stderr', 7):
                raise AssertionError(f'{sandbox.id} command result mismatch')
            payload = b'adx-three-vm\x00\xff\n' * 1024
            sandbox.files.write('/tmp/adx-three-vm.bin', payload)
            if sandbox.files.read('/tmp/adx-three-vm.bin', format='bytes') != payload:
                raise AssertionError(f'{sandbox.id} binary file mismatch')
        report['checks'].append('cross-vm-command-file')
    except Exception as error:
        report['error'] = f'{type(error).__name__}: {error}'
        raise
    finally:
        for sandbox in reversed(handles):
            try:
                sandbox.kill()
            except Exception as error:
                report['cleanup_errors'].append(f'{sandbox.id}: {error}')
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
                    raise AssertionError(f'test-owned backend remains: {residual}')
                time.sleep(.2)
            if handles:
                report['checks'].append('owned-backend-cleanup')
        except Exception as error:
            report['cleanup_errors'].append(str(error))
        if not report.get('error') and not report['cleanup_errors']:
            report['status'] = 'passed'
        (output / 'sdk-accept-result.json').write_text(json.dumps(report, indent=2) + '\n')
        if report['cleanup_errors'] and not report.get('error'):
            raise RuntimeError('; '.join(report['cleanup_errors']))
    return report


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--inventory', required=True, type=Path)
    parser.add_argument('--release', required=True, type=Path,
                        help='the exact release archive installed on all three VMs')
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
    report = run_acceptance(inventory, connection, args.image, args.socket, args.output,
                            release=args.release)
    print(json.dumps({'status': report['status'], 'checks': report['checks']}), flush=True)


if __name__ == '__main__':
    main()
