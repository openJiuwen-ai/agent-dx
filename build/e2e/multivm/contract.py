#!/usr/bin/env python3
"""Machine-readable acceptance contract for a pre-provisioned ADX three-VM run."""
from __future__ import annotations

import re
import hashlib
import json
import ipaddress

REQUIRED = (
    'l0',
    'placement',
    'capacity',
    'local-first',
    'cross-node-data',
    'node-failure',
    'worker-restart',
    'control-restart',
    'stop',
)
ROLES = ('control', 'worker-1', 'worker-2')
SHA256 = re.compile(r'^[0-9a-f]{64}$')


def inventory_digest(inventory):
    payload = json.dumps(inventory, sort_keys=True, separators=(',', ':')).encode()
    return hashlib.sha256(payload).hexdigest()


def verify_inventory(inventory):
    if inventory.get('schema_version') != 1:
        raise ValueError('three-VM inventory schema_version must be 1')
    machines = inventory.get('machines')
    if not isinstance(machines, list) or len(machines) != 3:
        raise ValueError('three-VM inventory requires exactly three machines')
    roles = [machine.get('role') for machine in machines]
    if len(set(roles)) != 3 or set(roles) != set(ROLES):
        raise ValueError('machine roles must be control, worker-1 and worker-2')
    for field in ('machine_id', 'hostname', 'address'):
        values = [machine.get(field) for machine in machines]
        if any(not isinstance(value, str) or not value for value in values) or len(set(values)) != 3:
            raise ValueError(field + ' must be present and unique on all three machines')
    for machine in machines:
        try:
            ipaddress.ip_address(machine['address'])
        except ValueError as error:
            raise ValueError('machine address must be an assigned IP address') from error
        target = machine.get('ssh_target')
        if not isinstance(target, str) or not target or target.startswith('-') or any(c.isspace() for c in target):
            raise ValueError('every machine requires a safe ssh_target')
        if machine['role'] != 'control' and not machine.get('node_id'):
            raise ValueError('each worker requires a node_id')
    node_ids = [machine['node_id'] for machine in machines if machine['role'] != 'control']
    if len(set(node_ids)) != 2:
        raise ValueError('worker node_id values must be unique')
    artifacts = inventory.get('artifacts', {})
    if not re.fullmatch(r'[0-9a-f]{40}', artifacts.get('commit', '')):
        raise ValueError('a full source commit is required')
    if not SHA256.fullmatch(artifacts.get('release_sha256', '')):
        raise ValueError('release_sha256 is required')
    if not artifacts.get('target'):
        raise ValueError('release target is required')
    return inventory


def verify_result(result, inventory):
    verify_inventory(inventory)
    errors = []
    if result.get('profile') != 'multi-vm' or result.get('deployment') != 'process':
        errors.append('result must identify the multi-vm process profile')
    if tuple(result.get('required_checks', ())) != REQUIRED:
        errors.append('required_checks differ from the multi-VM contract')
    if tuple(result.get('checks', ())) != REQUIRED or result.get('missing_checks'):
        errors.append('all multi-VM checks must pass in contract order')
    if result.get('status') != 'passed' or result.get('error'):
        errors.append('result status is not passed')
    if result.get('cleanup_errors'):
        errors.append('cleanup errors are acceptance failures')
    expected_workers = {'worker-1', 'worker-2'}
    placements = result.get('placement', [])
    actual_workers = {item.get('machine_role') for item in placements}
    if not expected_workers <= actual_workers:
        errors.append('instances were not proven on both worker VMs')
    final = result.get('final_state', {})
    if final.get('backend_instances') != {'worker-1': 0, 'worker-2': 0}:
        errors.append('both worker backend inventories must be empty')
    if final.get('published_routes') != 0:
        errors.append('published routes must be empty')
    if result.get('inventory_sha256') != inventory_digest(inventory):
        errors.append('result does not identify the accepted inventory')
    if errors:
        raise ValueError('; '.join(errors))
    return result
