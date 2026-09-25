"""Central scheduling timeout must leave neither a queued nor owned request."""

import importlib.util
import json
from pathlib import Path
import sys
import tempfile
import time
import types
import unittest
from unittest.mock import patch


ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))
import runtime_inventory


class ScheduleDeadlineTests(unittest.TestCase):
    def test_queue_is_observed_then_drained_without_assignment(self):
        state = {'pending': False, 'created': 0, 'queries': 0}

        class SandboxError(Exception):
            def __init__(self, instance_id):
                super().__init__('central scheduling queue deadline exceeded')
                self.code = 'OUTCOME_UNKNOWN'
                self.retry = 'same_operation'
                self.outcome = 'unknown'
                self.instance_id = instance_id

        class Sandbox:
            def __init__(self, **options):
                state['created'] += 1
                state['options'] = options
                state['pending'] = True
                time.sleep(.08)
                state['pending'] = False
                raise SandboxError('default-' + options['name'])

        nodes = {
            'node:' + node: json.dumps({
                'node': {'available': True, 'runtime_classes': ['runc']},
                'session': {'routable': True},
            })
            for node in ('node1', 'node2')
        }

        def queue_reader(*_args):
            state['queries'] += 1
            return {'default-' + state['options']['name']} if state['pending'] else set()

        spec = importlib.util.spec_from_file_location(
            'schedule_deadline_case', ROOT / 'schedule_deadline.py')
        scenario = importlib.util.module_from_spec(spec)
        modules = {
            'adx_sandbox': types.SimpleNamespace(Sandbox=Sandbox, SandboxError=SandboxError),
            'node': types.SimpleNamespace(catalog=lambda: nodes),
            'runtime_inventory': runtime_inventory,
        }
        with patch.dict(sys.modules, modules):
            spec.loader.exec_module(scenario)

        connection = types.SimpleNamespace(server_address='test:8443', token='admin')
        with tempfile.TemporaryDirectory() as directory:
            output = Path(directory) / 'deadline.json'
            result = scenario.run(connection, 'test-image', output, Path(directory) / 'ca.pem',
                                  queue_reader=queue_reader)
            persisted = json.loads(output.read_text())
        self.assertEqual(result, persisted)
        self.assertEqual(result['status'], 'passed')
        self.assertEqual(result['cases'][0]['id'], 'reliability.central-queue-deadline')
        self.assertTrue(result['queue_observed'] and result['queue_drained'])
        self.assertEqual(state['created'], 1)
        self.assertGreaterEqual(state['queries'], 2)
        self.assertEqual(state['options']['schedule_timeout'], 3)
        self.assertEqual(state['options']['node_id'], 'node1')


if __name__ == '__main__':
    unittest.main()
