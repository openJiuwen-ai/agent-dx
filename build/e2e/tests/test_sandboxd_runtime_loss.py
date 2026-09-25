"""A lost runtime must lose its public route before the scenario cleans it up."""

import importlib.util
import json
from pathlib import Path
import sys
import tempfile
import types
import unittest
from unittest.mock import patch


ROOT = Path(__file__).resolve().parents[1]


class RuntimeLossTests(unittest.TestCase):
    def test_never_policy_checks_route_withdrawal_before_delete(self):
        instance_id = 'instance-1'
        assignment = {'node_id': 'node1', 'generation': 1}
        record = {
            'assignment': assignment,
            'result': {
                'state': 'Failed', 'resources_held': False,
                'restart_pending': False,
            },
        }
        calls = []

        def withdrawn_route(got_id, _secrets):
            self.assertEqual(got_id, instance_id)
            self.assertEqual(record['result']['state'], 'Failed')
            calls.append('route')
            return {'http_status': 409, 'message': 'not connectable'}

        def delete(got_id, connection):
            self.assertEqual(got_id, instance_id)
            calls.append('delete')
            record['result']['state'] = 'Deleted'

        modules = {
            'adx_sandbox': types.SimpleNamespace(
                RestartPolicy=object,
                Sandbox=types.SimpleNamespace(delete=delete),
            ),
            'node': types.SimpleNamespace(
                catalog=lambda: {'environment:' + instance_id: json.dumps(record)},
                labeled_backend=lambda _id: [],
                persisted_runtime_id=lambda result: result['runtime']['id'],
            ),
            'network_partition': types.SimpleNamespace(withdrawn_route=withdrawn_route),
        }
        with patch.dict(sys.modules, modules), tempfile.TemporaryDirectory() as directory:
            spec = importlib.util.spec_from_file_location(
                'runtime_loss_route_case', ROOT / 'sandboxd_runtime_loss.py')
            scenario = importlib.util.module_from_spec(spec)
            spec.loader.exec_module(scenario)
            evidence = Path(directory)
            (evidence / 'loss-never-created.json').write_text(json.dumps({
                'instance_id': instance_id, 'assignment': assignment,
                'backend_id': 'old-backend', 'runtime_id': 'old-runtime',
            }))
            (evidence / 'sandboxd-runtime-loss-node1-never.json').write_text(json.dumps({
                'backend_ids_before': ['old-backend'], 'backend_ids_after': [],
                'previous_pid': 100, 'pid': 200,
            }))
            result = scenario.verify(object(), evidence, evidence, 'never')

        self.assertEqual(calls, ['route', 'delete'])
        self.assertEqual(result['status'], 'passed')
        self.assertEqual(result['cases'][0]['route']['http_status'], 409)


if __name__ == '__main__':
    unittest.main()
