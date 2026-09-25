"""The optional load profiles require a real two-worker Full deployment."""

import unittest
import json
from pathlib import Path
import tempfile

from e2e import run as driver


class LoadProfileTests(unittest.TestCase):
    def test_performance_and_pressure_are_full_only(self):
        for case in ('load-performance', 'load-pressure'):
            with self.subTest(case=case):
                self.assertEqual(driver.selected_checks('full', case), (case,))
                with self.assertRaises(ValueError):
                    driver.selected_checks('standalone', case)
                with self.assertRaises(ValueError):
                    driver.selected_checks('k8s-basic', case)

    def test_performance_report_has_per_operation_rates(self):
        from e2e.load.performance import summarize

        result = summarize({
            'status': 'passed', 'elapsed_seconds': 60,
            'operations': {'command': {'count': 120}, 'file': {'count': 60},
                           'create': {'count': 12}, 'delete': {'count': 12}},
            'cases': [{'id': 'mixed-soak.command', 'status': 'passed', 'seconds': 0}],
        })
        self.assertEqual(result['operations']['command']['rate_per_second'], 2.0)
        self.assertEqual(result['operations']['file']['rate_per_second'], 1.0)
        self.assertEqual(result['operations']['create']['rate_per_second'], 0.2)
        self.assertEqual(result['cases'][0]['id'], 'load-performance.command')

    def test_pressure_requires_every_request_waiting_before_release(self):
        from e2e.load.pressure import verify_waiting

        expected = {'default-load-1', 'default-load-2'}
        verify_waiting(expected, expected, completed=0)
        with self.assertRaisesRegex(AssertionError, 'not all requests queued'):
            verify_waiting(expected, {'default-load-1'}, completed=0)
        with self.assertRaisesRegex(AssertionError, 'created before capacity release'):
            verify_waiting(expected, expected, completed=1)

    def test_full_driver_collects_load_subcases_and_physical_cleanup(self):
        with tempfile.TemporaryDirectory() as directory:
            runner = driver.Run(Path(directory))
            runner.nodes = ['node1', 'node2']
            commands = []
            runner.execute = lambda node, *args, **kwargs: (
                commands.append(('execute', node, args[-1]))
                or json.dumps({'cases': [{'id': 'load-pressure.drained',
                                          'status': 'passed', 'seconds': 0}]})
            )
            runner.helper = lambda node, *args, **kwargs: (
                commands.append(('helper', node, args[0]))
            )
            checks = []
            runner.scenarios(checks, ('load-pressure',))
            self.assertEqual(checks, ['load-pressure'])
            self.assertEqual(commands, [
                ('execute', 'node1', 'load-pressure'),
                ('helper', 'node1', 'empty'),
                ('helper', 'node2', 'empty'),
            ])


if __name__ == '__main__':
    unittest.main()
