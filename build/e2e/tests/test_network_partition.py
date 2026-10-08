"""Network fault injection must clean its rule and distinguish route errors."""

import importlib.util
import io
import json
from pathlib import Path
import tempfile
import unittest
from unittest.mock import patch
from urllib.error import HTTPError


ROOT = Path(__file__).resolve().parents[1]


def load(name, filename):
    spec = importlib.util.spec_from_file_location(name, ROOT / filename)
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


class NetworkPartitionTests(unittest.TestCase):
    def test_returning_worker_cannot_reopen_with_its_invalidated_backend(self):
        node = load('partition_recovery_node', 'node.py')
        records = {
            'node:node2': json.dumps({'node': {'available': True},
                                      'session': {'routable': True}}),
            'environment:failed': json.dumps({
                'spec': {'id': 'failed'}, 'assignment': {'node_id': 'node2'},
                'invalidated': True,
            }),
        }
        with self.assertRaisesRegex(AssertionError, 'before stale backend cleanup'):
            node.returning_node_status(records, ['old-backend'])
        self.assertEqual(node.returning_node_status(records, []),
                         ('failed', True, True))

    def test_heal_removes_exact_rule_after_real_blocked_packets(self):
        node = load('partition_node', 'node.py')
        with tempfile.TemporaryDirectory() as directory:
            marker = Path(directory) / 'partition.ip'
            marker.write_text('192.0.2.11\n')
            listing = ('[3:144] -A OUTPUT -d 192.0.2.11/32 -p tcp -m tcp '
                       '--dport 17000 -m comment '
                       '--comment "adx-e2e-network-partition" -j DROP\n')
            with patch.object(node.subprocess, 'check_output', return_value=listing), \
                    patch.object(node.subprocess, 'run') as remove:
                evidence = node.heal_partition(marker, require_packets=True)
            self.assertEqual(evidence, {'coordinator_ip': '192.0.2.11',
                                        'blocked_packets': 3})
            remove.assert_called_once_with(
                ['iptables', '-D', 'OUTPUT', *node.partition_rule('192.0.2.11')],
                check=True, timeout=5)
            self.assertFalse(marker.exists())

    def test_only_ingress_route_failure_counts_as_withdrawal(self):
        scenario = load('network_partition_case', 'network_partition.py')
        with tempfile.TemporaryDirectory() as directory:
            secrets = Path(directory)
            (secrets / 'api-key').write_text('test-only-key')
            route = HTTPError('https://example.test/direct/id/status', 409,
                              'Conflict', {}, io.BytesIO(b'sandbox instance is not connectable'))
            with patch.object(scenario.ssl, 'create_default_context'), \
                    patch.object(scenario, 'urlopen', side_effect=route):
                observed = scenario.withdrawn_route('failed-id', secrets)
            self.assertEqual(observed['http_status'], 409)

            backend_404 = HTTPError('https://example.test/direct/id/status', 404,
                                    'Not Found', {}, io.BytesIO(b'unknown EXECD path'))
            with patch.object(scenario.ssl, 'create_default_context'), \
                    patch.object(scenario, 'urlopen', side_effect=backend_404), \
                    patch.object(scenario.time, 'monotonic', side_effect=[0, 0, 16]), \
                    patch.object(scenario.time, 'sleep'):
                with self.assertRaisesRegex(AssertionError, 'HTTP 404'):
                    scenario.withdrawn_route('failed-id', secrets)


if __name__ == '__main__':
    unittest.main()
