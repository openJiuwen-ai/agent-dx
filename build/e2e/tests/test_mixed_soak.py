"""Acceptance policy for the optional mixed-service stability scenario."""

import unittest
import json
from pathlib import Path
import tempfile

from e2e.mixed_soak import evaluate, exercise
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
