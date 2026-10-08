"""Acceptance policy for the optional mixed-service stability scenario."""

import unittest
import json
from pathlib import Path
import sys
import tempfile
import types
from unittest.mock import patch

from e2e.mixed_soak import evaluate, exercise, run, verify_assignment
from e2e import run as driver


class FakeSandbox:
    class Commands:
        def run(self, command):
            marker = command.rsplit("'", 2)[1]
            return type('Result', (), {'exit_code': 0, 'stdout': marker})()

    class Files:
        def __init__(self):
            self.data = {}

        def write(self, path, data):
            self.data[path] = data

        def read(self, path, format):
            assert format == 'bytes'
            return self.data[path]

    def __init__(self):
        self.commands = self.Commands()
        self.files = self.Files()


class MixedSoakTests(unittest.TestCase):
    def test_each_created_sandbox_must_have_the_requested_assignment(self):
        records = {'environment:sid': json.dumps({'assignment': {'node_id': 'node2'}})}
        self.assertEqual(verify_assignment('sid', 'node2', lambda: records), 'node2')
        with self.assertRaisesRegex(AssertionError, 'node assignment'):
            verify_assignment('sid', 'node1', lambda: records)

    def test_failed_live_operation_still_closes_every_created_sandbox(self):
        instances = []

        class FailingSandbox:
            def __init__(self, **_kwargs):
                self.id = 'sid-' + str(len(instances))
                self.node_id = _kwargs['node_id']
                self.killed = False
                self.closed = False
                self.commands = self
                instances.append(self)

            def run(self, _command):
                raise RuntimeError('command unavailable')

            def kill(self):
                self.killed = True

            def close(self):
                self.closed = True

        fake_sdk = types.ModuleType('adx_sandbox')
        fake_sdk.Sandbox = FailingSandbox
        fake_node = types.ModuleType('node')
        fake_node.catalog = lambda: {
            'environment:' + item.id: json.dumps({'assignment': {'node_id': item.node_id}})
            for item in instances
        }
        with tempfile.TemporaryDirectory() as directory:
            output = Path(directory) / 'result.json'
            with patch.dict(sys.modules, {'adx_sandbox': fake_sdk, 'node': fake_node}):
                with self.assertRaisesRegex(AssertionError, 'mixed-load acceptance failed'):
                    run(object(), 'image', output, seconds=1)
            report = json.loads(output.read_text())
            self.assertEqual(report['status'], 'failed')
            self.assertIn('command unavailable', report['errors'][0])
            self.assertGreaterEqual(len(instances), 2)
            self.assertTrue(all(item.killed and item.closed for item in instances))

    def test_soak_is_an_opt_in_full_and_standalone_case(self):
        self.assertEqual(driver.selected_checks('full', 'mixed-soak'), ('mixed-soak',))
        self.assertEqual(driver.selected_checks('standalone', 'mixed-soak'), ('mixed-soak',))
        with self.assertRaises(ValueError):
            driver.selected_checks('k8s-basic', 'mixed-soak')
        with tempfile.TemporaryDirectory() as directory:
            run = driver.Run(Path(directory))
            run.nodes = ['node1', 'node2']
            calls = []
            run.execute = lambda node, *args, **kwargs: (
                calls.append(('execute', node, args[-1]))
                or json.dumps({'cases': [{'id': 'mixed-soak.command', 'status': 'passed'}]})
            )
            run.helper = lambda node, *args, **kwargs: calls.append(('helper', node, args[0]))
            checks = []
            run.scenarios(checks, ('mixed-soak',))
            self.assertEqual(checks, ['mixed-soak'])
            self.assertEqual(calls, [('execute', 'node1', 'mixed-soak'),
                                     ('helper', 'node1', 'empty'),
                                     ('helper', 'node2', 'empty')])

    def test_exercise_checks_real_command_and_binary_file_contract(self):
        sandbox = FakeSandbox()
        exercise(sandbox, 'anchor-1')
        self.assertEqual(sandbox.files.data['/tmp/adx-soak-anchor-1.bin'][-2:], b'\x00\xff')

    def test_evaluation_requires_sustained_mixed_work_and_zero_errors(self):
        samples = {'command': [10.0] * 40, 'file': [20.0] * 40,
                   'create': [100.0] * 5, 'delete': [50.0] * 5}
        report = evaluate(samples, [], elapsed=301, minimum_seconds=300)
        self.assertEqual(report['status'], 'passed')
        self.assertEqual(report['operations']['create']['count'], 5)
        self.assertEqual(report['operations']['command']['p99_ms'], 10.0)
        for failing_samples, errors in ((samples, ['command timeout']),
                                        ({**samples, 'create': [100.0]}, [])):
            self.assertEqual(evaluate(failing_samples, errors, 301, 300)['status'], 'failed')
        short = evaluate(samples, [], 250, 300)
        self.assertEqual(short['status'], 'failed')
        self.assertIn({'id': 'mixed-soak.duration', 'status': 'failed', 'seconds': 0},
                      short['cases'])


if __name__ == '__main__':
    unittest.main()
