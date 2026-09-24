import json
from pathlib import Path
import tempfile
import unittest

from e2e.multivm.assemble import assemble
from e2e.multivm.contract import inventory_digest, verify_result
from e2e.tests.test_multivm_local_first import inventory


CASE_CHECKS = {
    'sdk': ['machine-identity', 'resource-discovery', 'physical-placement',
            'cross-vm-command-file', 'owned-backend-cleanup'],
    'auth': ['machine-identity', 'invalid-key-denied', 'key-management-admin-only',
             'tenant-isolation', 'temporary-key-revoked', 'owner-still-serving'],
    'capacity': ['two-worker-capacity-reserved', 'central-queue-observed',
                 'release-wakes-one-queued-request', 'queue-drained',
                 'backend-and-ledger-cleanup'],
    'placement-pack': ['central-placement-config', 'comparable-worker-capacity',
                       'public-sdk-policy-placement', 'all-owned-resources-released'],
    'placement-spread': ['central-placement-config', 'comparable-worker-capacity',
                         'public-sdk-policy-placement', 'all-owned-resources-released'],
    'local-first': ['entry-rotation', 'same-id-atomic-claim',
                    'different-specification-conflict', 'local-claim-log-evidence',
                    'constrained-central-fallback', 'single-resource-accounting',
                    'one-backend-per-assignment', 'single-release-per-instance'],
    'worker-failure': ['machine-identity', 'both-workers-running',
                       'heartbeat-expiry', 'route-withdrawn',
                       'healthy-worker-unaffected',
                       'stale-backend-cleanup-before-readmission',
                       'old-route-still-invalid-after-rejoin',
                       'new-admission-after-reconciliation'],
    'worker-restart': ['both-workers-ready',
                       'new-session-same-backend-and-generation',
                       'both-workers-still-serving'],
    'session-fence': ['both-workers-ready',
                      'new-session-same-backend-and-generation',
                      'old-session-commit-rejected', 'both-workers-still-serving'],
    'control-restart': ['managed-control-services-ready', 'both-workers-running',
                        'coordinator-restart-preserved-ownership-and-route',
                        'apiserver-restart-preserved-ownership-and-route',
                        'redis-restart-preserved-ownership-and-route',
                        'owned-backends-and-capacity-released'],
    'ingress-restart': ['managed-control-services-ready', 'both-workers-running',
                        'ingress-restart-preserved-ownership-and-route',
                        'owned-backends-and-capacity-released'],
    'stop': ['dedicated-empty-workers', 'both-workers-serving',
             'worker-2-drained-and-route-withdrawn',
             'worker-1-drained-and-route-withdrawn',
             'published-route-catalog-empty', 'control-stopped-last'],
}


def full_evidence(root, inv):
    from e2e.multivm.suite import CASES
    ordered = ['sdk', 'auth', 'capacity', 'placement-pack', 'placement-spread',
               'node-preferences', 'local-first', 'worker-failure',
               'worker-restart', 'session-fence', 'control-restart',
               'ingress-restart', 'stop']
    state = {'schema_version': 2, 'inventory_sha256': inventory_digest(inv),
             'budget_seconds': 10800, 'runtime_seconds': len(ordered),
             'scope': 'deployment-and-cases',
             'started_at': 1000, 'finished_at': 1000 + len(ordered),
             'active': None,
             'cases': [{'case': name, 'status': 'passed', 'seconds': 1}
                       for name in ordered]}
    (root / 'budget-state.json').write_text(json.dumps(state))
    for name in ordered:
        report = {'status': 'passed', 'cleanup_errors': [],
                  'checks': CASE_CHECKS.get(name, [])}
        if name == 'sdk':
            report.update({
                'release_sha256': inv['artifacts']['release_sha256'],
                'machines': [{'role': machine['role'], 'machine_id': machine['machine_id'],
                              'hostname': machine['hostname'], 'address': machine['address'],
                              'release_commit': inv['artifacts']['commit'],
                              'release_target': inv['artifacts']['target']}
                             for machine in inv['machines']],
                'instances': [{'id': 'sbox-1', 'node_id': 'node1'},
                              {'id': 'sbox-2', 'node_id': 'node2'}],
                'assignments': {
                    'sbox-1': {'node_id': 'node1', 'state': 'Running', 'resources_held': True},
                    'sbox-2': {'node_id': 'node2', 'state': 'Running', 'resources_held': True},
                },
                'backends': {
                    'sbox-1': {'node1': ['backend-1'], 'node2': []},
                    'sbox-2': {'node1': [], 'node2': ['backend-2']},
                },
            })
        if name == 'node-preferences':
            report['cases'] = [{'name': label, 'expected_node': 'node1',
                                'actual_node': 'node1', 'backend_id': label}
                               for label in ('weighted node preference',
                                             'ordered node preference',
                                             'environment affinity OR',
                                             'environment anti-affinity',
                                             'explicit node and OR constraints')]
        if name in ('placement-pack', 'placement-spread'):
            report['instances'] = [{'node_id': 'node1'},
                                   {'node_id': 'node1' if name == 'placement-pack' else 'node2'}]
        if name == 'session-fence':
            report['fencing'] = {'status': 'passed',
                                 'grpc_code': 'FailedPrecondition',
                                 'record_unchanged': True}
        if name in ('worker-restart', 'session-fence'):
            report['restart'] = {'old_session': 'old', 'new_session': 'new',
                                 'generation': 1, 'backend_ids': ['backend-2']}
        if name == 'control-restart':
            report['restarts'] = {role: {'epoch_before': 1,
                                        'epoch_after': 2 if role == 'coordinator' else 1}
                                  for role in ('coordinator', 'apiserver', 'redis')}
        if name == 'ingress-restart':
            report['restarts'] = {'ingress': {'epoch_before': 2, 'epoch_after': 2}}
        if name == 'stop':
            report['stop_order'] = ['worker-2', 'worker-1', 'control']
            report['final_state'] = {'backend_instances': {
                'worker-1': 0, 'worker-2': 0}, 'published_routes': 0}
            report['route_snapshot'] = {'status': 'passed', 'reset': True,
                                        'revision': 5, 'published_routes': 0}
        destination = root / name
        destination.mkdir()
        (destination / CASES[name].report).write_text(json.dumps(report))
    return state


class AssembleTests(unittest.TestCase):
    def test_complete_observed_evidence_satisfies_contract(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            inv = inventory()
            full_evidence(root, inv)
            result = assemble(inv, root)
            self.assertEqual(result['status'], 'passed')
            self.assertEqual(len(result['checks']), 9)
            self.assertEqual({item['machine_role'] for item in result['placement']},
                             {'worker-1', 'worker-2'})
            verify_result(result, inv)

    def test_missing_auth_cannot_be_assembled_as_full_pass(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            inv = inventory()
            state = full_evidence(root, inv)
            state['cases'] = [item for item in state['cases'] if item['case'] != 'auth']
            (root / 'budget-state.json').write_text(json.dumps(state))
            result = assemble(inv, root)
            self.assertEqual(result['status'], 'failed')
            self.assertIn('l0', result['missing_checks'])

    def test_fake_stop_status_cannot_hide_published_route(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            inv = inventory()
            full_evidence(root, inv)
            path = root / 'stop/stop-result.json'
            report = json.loads(path.read_text())
            report['final_state']['published_routes'] = 1
            path.write_text(json.dumps(report))
            result = assemble(inv, root)
            self.assertEqual(result['status'], 'failed')
            self.assertIn('stop', result['missing_checks'])

    def test_zero_route_count_without_publication_snapshot_is_not_evidence(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            inv = inventory()
            full_evidence(root, inv)
            path = root / 'stop/stop-result.json'
            report = json.loads(path.read_text())
            report.pop('route_snapshot')
            path.write_text(json.dumps(report))
            result = assemble(inv, root)
            self.assertEqual(result['status'], 'failed')
            self.assertIn('stop', result['missing_checks'])

    def test_inconsistent_budget_totals_are_rejected(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            inv = inventory()
            state = full_evidence(root, inv)
            state['runtime_seconds'] = 1
            (root / 'budget-state.json').write_text(json.dumps(state))
            result = assemble(inv, root)
            self.assertEqual(result['status'], 'failed')
            self.assertTrue(any('budget' in error for error in result['errors']))

    def test_wall_clock_over_three_hours_rejects_full_result(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            inv = inventory()
            state = full_evidence(root, inv)
            state['started_at'] = 1000
            state['finished_at'] = 11801
            (root / 'budget-state.json').write_text(json.dumps(state))
            result = assemble(inv, root)
            self.assertEqual(result['status'], 'failed')
            self.assertTrue(any('wall' in error for error in result['errors']))

    def test_case_only_budget_cannot_prove_full_deployment_under_three_hours(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            inv = inventory()
            state = full_evidence(root, inv)
            state['scope'] = 'cases-only'
            (root / 'budget-state.json').write_text(json.dumps(state))
            result = assemble(inv, root)
            self.assertEqual(result['status'], 'failed')
            self.assertTrue(any('deployment' in error for error in result['errors']))

    def test_reused_session_cannot_count_as_worker_restart(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            inv = inventory()
            full_evidence(root, inv)
            path = root / 'worker-restart/worker-restart-result.json'
            report = json.loads(path.read_text())
            report['restart']['new_session'] = report['restart']['old_session']
            path.write_text(json.dumps(report))
            result = assemble(inv, root)
            self.assertEqual(result['status'], 'failed')
            self.assertIn('worker-restart', result['missing_checks'])

    def test_selected_runtime_affinity_must_have_real_report_evidence(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            inv = inventory()
            state = full_evidence(root, inv)
            state['cases'].insert(-1, {'case': 'runtime-affinity', 'status': 'passed',
                                       'seconds': 1})
            state['runtime_seconds'] += 1
            (root / 'budget-state.json').write_text(json.dumps(state))
            path = root / 'runtime-affinity'
            path.mkdir()
            (path / 'runtime-affinity-result.json').write_text(json.dumps({'status': 'passed'}))
            result = assemble(inv, root)
            self.assertEqual(result['status'], 'failed')
            self.assertTrue(any('runtime-affinity' in error for error in result['errors']))
