#!/usr/bin/env python3
"""Verify public API-key authentication and tenant isolation on three VMs."""
from __future__ import annotations

import argparse
import json
import os
from pathlib import Path
import ssl
import time

if __package__:
    from .contract import verify_inventory
    from .sdk_accept import ssh, verify_machines
else:
    from contract import verify_inventory
    from sdk_accept import ssh, verify_machines


TENANT = 'adx-3vm-auth'
MANAGEMENT = '/api/admin/v1/keys'


def require_status(request, method, path, token, expected, label, **options):
    status, payload = request(method, path, token, **options)
    if status != expected:
        raise AssertionError(f'{label}: expected HTTP {expected}, got {status}')
    return payload


def run_acceptance(inventory, connection, image, output, *,
                   owner_token, admin_token, sandbox_factory=None,
                   request, verify=None, now=time.monotonic, sleep=time.sleep):
    verify_inventory(inventory)
    if sandbox_factory is None:
        from adx_sandbox import Sandbox
        sandbox_factory = Sandbox
    verify = verify or (lambda value: verify_machines(value, ssh))
    worker = next(machine for machine in inventory['machines']
                  if machine['role'] == 'worker-1')
    report = {'status': 'failed', 'profile': 'multi-vm-auth', 'checks': [],
              'instances': [], 'cleanup_errors': []}
    output.mkdir(parents=True, exist_ok=True)
    sandbox = None
    key_id = None
    revoked = False
    try:
        report['machines'] = verify(inventory)
        report['checks'].append('machine-identity')
        sandbox = sandbox_factory(
            image=image, runtime='runc', node_id=worker['node_id'],
            cpu=500, memory=512, idle_timeout=0,
            connection=connection, create_timeout=150,
        )
        if not sandbox.is_running():
            raise AssertionError('owner Sandbox did not reach Running')
        report['instances'].append({'id': sandbox.id, 'node_id': worker['node_id']})
        path = '/api/sandbox/v1/sandboxes/' + sandbox.id
        query = {'instance_id': sandbox.id}
        bad = 'invalid-acceptance-key'
        require_status(request, 'GET', '/api/instances', bad, 401,
                       'invalid key read', params=query)
        require_status(request, 'DELETE', path, bad, 401, 'invalid key delete')
        report['checks'].append('invalid-key-denied')

        require_status(request, 'GET', MANAGEMENT, owner_token, 403,
                       'tenant key management')
        created = require_status(request, 'POST', MANAGEMENT, admin_token, 201,
                                 'administrator key create', body={'tenantId': TENANT})
        key_id = created['key']['id']
        other_token = created['apiKey']
        if not key_id or not other_token or other_token == owner_token:
            raise AssertionError('administrator returned an invalid temporary key')
        listed = require_status(request, 'GET', MANAGEMENT, admin_token, 200,
                                'administrator key list', params={'tenantId': TENANT})
        if not any(item.get('id') == key_id for item in listed.get('items', [])):
            raise AssertionError('temporary key missing from administrator list')
        if other_token in json.dumps(listed):
            raise AssertionError('administrator list exposed key material')
        report['checks'].append('key-management-admin-only')

        require_status(request, 'GET', '/api/sandbox/v1/snapshots', other_token,
                       200, 'temporary key authentication')
        require_status(request, 'GET', '/api/instances', other_token, 403,
                       'tenant read', params=query)
        require_status(request, 'DELETE', path, other_token, 403, 'tenant delete')
        report['checks'].append('tenant-isolation')

        require_status(request, 'DELETE', MANAGEMENT + '/' + key_id, admin_token,
                       204, 'administrator key revoke')
        revoked = True
        deadline = now() + 20
        while True:
            status, _ = request('GET', '/api/sandbox/v1/snapshots', other_token)
            if status == 401:
                break
            if status != 200 or now() >= deadline:
                raise AssertionError('revoked key remained accepted beyond cache budget')
            sleep(.25)
        report['checks'].append('temporary-key-revoked')

        result = sandbox.commands.run('printf still-owned')
        if not sandbox.is_running() or (result.exit_code, result.stdout) != (0, 'still-owned'):
            raise AssertionError('owner Sandbox stopped serving after denied requests')
        report['checks'].append('owner-still-serving')
    except Exception as error:
        report['error'] = f'{type(error).__name__}: {error}'
        raise
    finally:
        if key_id and not revoked:
            try:
                require_status(request, 'DELETE', MANAGEMENT + '/' + key_id,
                               admin_token, 204, 'temporary key cleanup')
            except Exception as error:
                report['cleanup_errors'].append(f'temporary key revoke: {error}')
        if sandbox is not None:
            try:
                sandbox.kill()
            except Exception as error:
                report['cleanup_errors'].append(f'{sandbox.id} delete: {error}')
            try:
                sandbox.close()
            except Exception as error:
                report['cleanup_errors'].append(f'{sandbox.id} close: {error}')
        if not report.get('error') and not report['cleanup_errors']:
            report['status'] = 'passed'
        (output / 'auth-result.json').write_text(json.dumps(report, indent=2) + '\n')
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
    parser.add_argument('--socket')
    parser.add_argument('--output', required=True, type=Path)
    args = parser.parse_args()
    os.environ['SSL_CERT_FILE'] = str(args.ca.resolve())
    import httpx
    from adx_sandbox import ConnectionConfig
    owner_token = args.token_file.read_text().strip()
    admin_token = args.admin_token_file.read_text().strip()
    connection = ConnectionConfig(server_address=args.endpoint, token=owner_token,
                                  use_tls=True, verify_tls=True)
    with httpx.Client(base_url='https://' + args.endpoint,
                      verify=ssl.create_default_context(cafile=str(args.ca)),
                      timeout=20) as client:
        def request(method, path, token, body=None, params=None):
            response = client.request(method, path,
                                      headers={'Authorization': 'Bearer ' + token},
                                      json=body, params=params)
            return response.status_code, response.json() if response.content else {}

        report = run_acceptance(json.loads(args.inventory.read_text()), connection,
                                args.image, args.output,
                                owner_token=owner_token, admin_token=admin_token,
                                request=request)
    print(json.dumps({'status': report['status'], 'checks': report['checks']}), flush=True)


if __name__ == '__main__':
    main()
