#!/usr/bin/env python3
"""Assemble only observed three-VM case reports into the full result contract."""
from __future__ import annotations

import argparse
import hashlib
import json
from pathlib import Path

if __package__:
    from .contract import REQUIRED, inventory_digest, verify_inventory, verify_result
    from .suite import CASES, MAX_BUDGET_SECONDS, write_json
else:
    from contract import REQUIRED, inventory_digest, verify_inventory, verify_result
    from suite import CASES, MAX_BUDGET_SECONDS, write_json


GROUP_CASES = {
    'l0': ('sdk', 'auth'),
    'placement': ('sdk', 'placement-pack', 'placement-spread', 'node-preferences'),
    'capacity': ('capacity',),
    'local-first': ('local-first',),
    'cross-node-data': ('sdk',),
    'node-failure': ('worker-failure',),
    'worker-restart': ('worker-restart', 'session-fence'),
    'control-restart': ('control-restart', 'ingress-restart'),
    'stop': ('stop',),
}
REQUIRED_CASES = tuple(dict.fromkeys(case for names in GROUP_CASES.values() for case in names))
CASE_CHECKS = {
    'sdk': {'machine-identity', 'resource-discovery', 'physical-placement',
            'cross-vm-command-file', 'owned-backend-cleanup'},
    'auth': {'machine-identity', 'invalid-key-denied', 'key-management-admin-only',
             'tenant-isolation', 'temporary-key-revoked', 'owner-still-serving'},
    'capacity': {'two-worker-capacity-reserved', 'central-queue-observed',
                 'release-wakes-one-queued-request', 'queue-drained',
                 'backend-and-ledger-cleanup'},
    'placement-pack': {'central-placement-config', 'comparable-worker-capacity',
                       'public-sdk-policy-placement', 'all-owned-resources-released'},
    'placement-spread': {'central-placement-config', 'comparable-worker-capacity',
                         'public-sdk-policy-placement', 'all-owned-resources-released'},
    'node-preferences': set(),
    'runtime-affinity': {'live-heterogeneous-runtime-inventory',
                         'unpinned-runtime-placement-and-command',
                         'runtime-backend-and-allocation-released'},
    'local-first': {'entry-rotation', 'same-id-atomic-claim',
                    'different-specification-conflict', 'local-claim-log-evidence',
                    'constrained-central-fallback', 'single-resource-accounting',
                    'one-backend-per-assignment', 'single-release-per-instance'},
    'worker-failure': {'both-workers-running', 'heartbeat-expiry', 'route-withdrawn',
                       'healthy-worker-unaffected',
                       'stale-backend-cleanup-before-readmission',
                       'old-route-still-invalid-after-rejoin',
                       'new-admission-after-reconciliation'},
    'worker-restart': {'both-workers-ready',
                       'new-session-same-backend-and-generation',
                       'both-workers-still-serving'},
    'session-fence': {'both-workers-ready',
                      'new-session-same-backend-and-generation',
                      'old-session-commit-rejected', 'both-workers-still-serving'},
    'control-restart': {'managed-control-services-ready', 'both-workers-running',
                        'coordinator-restart-preserved-ownership-and-route',
                        'apiserver-restart-preserved-ownership-and-route',
                        'redis-restart-preserved-ownership-and-route',
                        'owned-backends-and-capacity-released'},
    'ingress-restart': {'managed-control-services-ready', 'both-workers-running',
                        'ingress-restart-preserved-ownership-and-route',
                        'owned-backends-and-capacity-released'},
    'stop': {'dedicated-empty-workers', 'both-workers-serving',
             'worker-2-drained-and-route-withdrawn',
             'worker-1-drained-and-route-withdrawn',
             'published-route-catalog-empty', 'control-stopped-last'},
}
PREFERENCE_NAMES = {
    'weighted node preference', 'ordered node preference',
    'environment affinity OR', 'environment anti-affinity',
    'explicit node and OR constraints',
}


def sdk_placement(report, inventory):
    artifacts = inventory['artifacts']
    if report.get('release_sha256') != artifacts['release_sha256']:
        raise ValueError('SDK release SHA256 differs from inventory')
    machines = {machine['role']: machine for machine in inventory['machines']}
    observed = {machine['role']: machine for machine in report.get('machines', [])}
    if len(report.get('machines', [])) != len(machines) or set(observed) != set(machines):
        raise ValueError('SDK machine inventory is incomplete')
    for role, expected in machines.items():
        item = observed[role]
        if any(item.get(field) != expected[field] for field in
               ('machine_id', 'hostname', 'address')) \
                or item.get('release_commit') != artifacts['commit'] \
                or item.get('release_target') != artifacts['target']:
            raise ValueError(f'SDK machine identity differs for {role}')
    node_roles = {machine['node_id']: machine['role'] for machine in
                  inventory['machines'] if machine['role'] != 'control'}
    instances = report.get('instances', [])
    if len(instances) != 2 or {item.get('node_id') for item in instances} != set(node_roles):
        raise ValueError('SDK did not place one Sandbox on each worker')
    if len({item.get('id') for item in instances}) != 2:
        raise ValueError('SDK instance IDs are missing or duplicated')
    placements = []
    for item in instances:
        instance_id, owner = item['id'], item['node_id']
        assignment = report.get('assignments', {}).get(instance_id, {})
        physical = report.get('backends', {}).get(instance_id, {})
        if assignment.get('node_id') != owner or assignment.get('state') != 'Running' \
                or assignment.get('resources_held') is not True \
                or set(physical) != set(node_roles) \
                or any(not isinstance(ids, list) for ids in physical.values()) \
                or len(physical[owner]) != 1 \
                or any(physical[node] for node in node_roles if node != owner):
            raise ValueError(f'SDK ownership or physical backend mismatch for {instance_id}')
        placements.append({'machine_role': node_roles[owner], 'node_id': owner,
                           'environment_id': instance_id,
                           'backend_id': physical[owner][0]})
    return placements


def validate_case(name, report, inventory):
    if report.get('status') != 'passed' or report.get('error') \
            or report.get('cleanup_errors'):
        raise ValueError('case did not finish and clean successfully')
    if not CASE_CHECKS[name] <= set(report.get('checks', [])):
        raise ValueError('required case assertions are missing')
    if name == 'sdk':
        return sdk_placement(report, inventory)
    if name in ('placement-pack', 'placement-spread'):
        owners = [item.get('node_id') for item in report.get('instances', [])]
        worker_ids = {machine['node_id'] for machine in inventory['machines']
                      if machine['role'] != 'control'}
        if len(owners) != 2 or not set(owners) <= worker_ids \
                or (owners[0] == owners[1]) != (name == 'placement-pack'):
            raise ValueError('Pack/Spread physical placement evidence disagrees')
    if name == 'node-preferences':
        cases = report.get('cases', [])
        if {case.get('name') for case in cases} != PREFERENCE_NAMES \
                or len(cases) != len(PREFERENCE_NAMES) \
                or any(case.get('actual_node') != case.get('expected_node')
                       or not case.get('backend_id') for case in cases):
            raise ValueError('node preference placement evidence is incomplete')
    if name == 'runtime-affinity':
        runtime = report.get('runtime_class')
        inventories = report.get('inventories', {})
        workers = {machine['node_id'] for machine in inventory['machines']
                   if machine['role'] != 'control'}
        owners = [node for node in workers if runtime in inventories.get(node, [])]
        assignment = report.get('assignment', {})
        physical = report.get('backends', {})
        if not runtime or len(owners) != 1 or assignment.get('node_id') != owners[0] \
                or assignment.get('state') != 'Running' \
                or assignment.get('resources_held') is not True \
                or set(physical) != workers or len(physical[owners[0]]) != 1 \
                or any(physical[node] for node in workers if node != owners[0]):
            raise ValueError('runtime affinity inventory or backend evidence is incomplete')
    if name in ('worker-restart', 'session-fence'):
        restart = report.get('restart', {})
        if not restart.get('old_session') or not restart.get('new_session') \
                or restart['old_session'] == restart['new_session'] \
                or not isinstance(restart.get('generation'), int) \
                or restart['generation'] < 1 \
                or not isinstance(restart.get('backend_ids'), list) \
                or len(restart['backend_ids']) != 1:
            raise ValueError('worker replacement did not prove a new session on one backend')
    if name == 'session-fence':
        fencing = report.get('fencing', {})
        if fencing.get('status') != 'passed' \
                or fencing.get('grpc_code') != 'FailedPrecondition' \
                or fencing.get('record_unchanged') is not True:
            raise ValueError('retired Node session write was not fenced')
    if name in ('control-restart', 'ingress-restart'):
        expected = ('coordinator', 'apiserver', 'redis') if name == 'control-restart' \
            else ('ingress',)
        restarts = report.get('restarts', {})
        if not all(role in restarts for role in expected):
            raise ValueError('control role restart evidence is incomplete')
        if name == 'control-restart' and \
                restarts['coordinator'].get('epoch_after', 0) <= \
                restarts['coordinator'].get('epoch_before', 0):
            raise ValueError('Coordinator epoch did not advance')
    if name == 'stop':
        final = report.get('final_state', {})
        snapshot = report.get('route_snapshot', {})
        if report.get('stop_order') != ['worker-2', 'worker-1', 'control'] \
                or final.get('backend_instances') != {'worker-1': 0, 'worker-2': 0} \
                or final.get('published_routes') != 0 \
                or snapshot.get('status') != 'passed' or snapshot.get('reset') is not True \
                or snapshot.get('published_routes') != 0 \
                or not isinstance(snapshot.get('revision'), int):
            raise ValueError('final backend or publication catalog is not empty')
    return None


def assemble(inventory, root):
    verify_inventory(inventory)
    root = Path(root)
    digest = inventory_digest(inventory)
    errors = []
    try:
        state = json.loads((root / 'budget-state.json').read_text())
    except (OSError, ValueError):
        state = {}
        errors.append('suite budget-state.json is missing or invalid')
    budget = state.get('budget_seconds')
    duration = state.get('runtime_seconds')
    started_at = state.get('started_at')
    finished_at = state.get('finished_at')
    if state.get('schema_version') != 2 or state.get('inventory_sha256') != digest \
            or not isinstance(budget, int) or not 0 < budget <= MAX_BUDGET_SECONDS \
            or not isinstance(duration, (int, float)) or not 0 <= duration <= budget \
            or state.get('active'):
        errors.append('suite inventory or three-hour budget evidence is invalid')
    if not isinstance(started_at, (int, float)) \
            or not isinstance(finished_at, (int, float)) \
            or finished_at < started_at \
            or not isinstance(budget, int) \
            or finished_at - started_at > budget:
        errors.append('suite wall-clock budget evidence exceeds three hours or is missing')
    records = state.get('cases', [])
    if not isinstance(records, list):
        records = []
        errors.append('suite case records are invalid')
    recorded = {}
    seconds = []
    for item in records:
        if not isinstance(item, dict) or item.get('case') in recorded \
                or item.get('case') not in CASES:
            errors.append('suite has duplicate or unknown case records')
            continue
        recorded[item['case']] = item
        elapsed = item.get('seconds')
        if not isinstance(elapsed, (int, float)) or elapsed < 0:
            errors.append(f"{item['case']}: invalid case runtime")
        else:
            seconds.append(elapsed)
        if item.get('status') != 'passed':
            errors.append(f"{item['case']}: suite recorded a failure or interruption")
    if isinstance(duration, (int, float)) and len(seconds) == len(records) \
            and abs(sum(seconds) - duration) > .05:
        errors.append('suite budget runtime differs from summed case runtimes')
    if records and (not isinstance(records[-1], dict)
                    or records[-1].get('case') != 'stop'):
        errors.append('destructive stop was not the final case')
    valid = {}
    evidence = {}
    placements = []
    for name in dict.fromkeys((*REQUIRED_CASES, *recorded)):
        record = recorded.get(name)
        if record is None or record.get('status') != 'passed':
            errors.append(f'{name}: required case was not passed')
            continue
        path = root / name / CASES[name].report
        try:
            content = path.read_bytes()
            report = json.loads(content)
            if not isinstance(report, dict):
                raise ValueError('case report must be an object')
            value = validate_case(name, report, inventory)
        except (OSError, ValueError, TypeError, KeyError) as error:
            errors.append(f'{name}: invalid case evidence: {error}')
            continue
        valid[name] = report
        evidence[name] = {'path': str(path), 'sha256': hashlib.sha256(content).hexdigest()}
        if name == 'sdk':
            placements = value
    checks = [name for name in REQUIRED if all(case in valid for case in GROUP_CASES[name])]
    missing = [name for name in REQUIRED if name not in checks]
    final = valid.get('stop', {}).get('final_state', {})
    result = {
        'schema_version': 1, 'profile': 'multi-vm', 'deployment': 'process',
        'required_checks': list(REQUIRED), 'checks': checks, 'missing_checks': missing,
        'inventory_sha256': digest, 'budget_seconds': budget,
        'runtime_seconds': duration,
        'wall_elapsed_seconds': finished_at - started_at
        if isinstance(started_at, (int, float)) and isinstance(finished_at, (int, float))
        else None,
        'placement': placements,
        'final_state': final, 'case_evidence': evidence,
        'status': 'failed' if errors or missing else 'passed',
        'cleanup_errors': [], 'errors': errors,
    }
    if result['status'] == 'passed':
        try:
            verify_result(result, inventory)
        except ValueError as error:
            result['status'] = 'failed'
            result['errors'].append(str(error))
    write_json(root / 'result.json', result)
    return result


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--inventory', required=True, type=Path)
    parser.add_argument('--suite-output', required=True, type=Path)
    args = parser.parse_args()
    result = assemble(json.loads(args.inventory.read_text()), args.suite_output)
    print(json.dumps({'status': result['status'], 'missing_checks': result['missing_checks'],
                      'errors': result['errors']}), flush=True)
    if result['status'] != 'passed':
        raise SystemExit(1)


if __name__ == '__main__':
    main()
