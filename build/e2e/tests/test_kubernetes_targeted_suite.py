"""A targeted Full campaign keeps case failures for later triage within one budget."""

import importlib.util
import json
from pathlib import Path
import sys
import tempfile
import time
import unittest


ROOT = Path(__file__).resolve().parents[1]
SPEC = importlib.util.spec_from_file_location(
    'kubernetes_targeted_suite', ROOT / 'kubernetes/targeted_suite.py',
)
SUITE = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(SUITE)


class Clock:
    def __init__(self):
        self.value = 1000.0

    def __call__(self):
        return self.value


class TargetedSuiteTests(unittest.TestCase):
    def test_product_failure_does_not_hide_later_case_or_cleanup(self):
        clock = Clock()
        calls = []

        def execute(case, command, log, timeout, deadline):
            calls.append((case, command, timeout, deadline))
            clock.value += 30
            output = log.parent
            status = 'failed' if case == 'command-expiry' else 'passed'
            (output / 'result.json').write_text(json.dumps({
                'status': status, 'harness': {'commit': 'a' * 40},
                'checks': [] if status == 'failed' else [case],
                'cleanup_errors': [], 'error': 'known product gap' if status == 'failed' else None,
                'cases': [{'name': case, 'status': status, 'seconds': 30}],
            }))
            (output / 'placement.json').write_text('[]')
            return (1 if status == 'failed' else 0), False

        with tempfile.TemporaryDirectory() as directory:
            output = Path(directory) / 'suite'
            report = SUITE.run_suite(
                ('command-expiry', 'command-response-cut'), output,
                ('--bundle', '/bundle.json'), 3600, execute=execute, clock=clock,
            )
            self.assertEqual([call[0] for call in calls],
                             ['command-expiry', 'command-response-cut'])
            self.assertEqual(report['status'], 'failed')
            self.assertEqual(report['checks'], ['command-response-cut'])
            self.assertEqual(report['missing_checks'], ['command-expiry'])
            self.assertEqual(report['failed_cases'], ['command-expiry'])
            self.assertLess(report['wall_elapsed_seconds'], 3600)
            self.assertEqual(json.loads((output / 'result.json').read_text())['status'], 'failed')
            self.assertTrue((output / 'junit.xml').is_file())
            self.assertEqual(calls[0][1][-4:-2], ['--case', 'command-expiry'])

    def test_cleanup_failure_stops_before_next_namespace(self):
        clock = Clock()
        calls = []

        def execute(case, _command, log, _timeout, _deadline):
            calls.append(case)
            clock.value += 10
            (log.parent / 'result.json').write_text(json.dumps({
                'status': 'failed', 'harness': {'commit': 'a' * 40},
                'checks': [], 'cleanup_errors': ['namespace remains'],
                'error': 'cleanup failed', 'cases': [],
            }))
            return 1, False

        with tempfile.TemporaryDirectory() as directory:
            report = SUITE.run_suite(
                ('command-expiry', 'command-response-cut'), Path(directory) / 'suite',
                (), 3600, execute=execute, clock=clock,
            )
            self.assertEqual(calls, ['command-expiry'])
            self.assertEqual(report['missing_checks'],
                             ['command-expiry', 'command-response-cut'])
            self.assertEqual(report['cleanup_errors'], ['command-expiry: namespace remains'])

    def test_budget_and_case_selection_are_bounded(self):
        clock = Clock()
        calls = []

        def execute(case, _command, log, _timeout, _deadline):
            calls.append(case)
            clock.value += 200
            (log.parent / 'result.json').write_text(json.dumps({
                'status': 'passed', 'harness': {'commit': 'a' * 40},
                'checks': [case], 'cleanup_errors': [], 'error': None,
                'cases': [{'name': case, 'status': 'passed', 'seconds': 200}],
            }))
            return 0, False

        with tempfile.TemporaryDirectory() as directory:
            report = SUITE.run_suite(
                ('command-response-cut', 'upload-response-cut'), Path(directory) / 'suite',
                (), 850, execute=execute, clock=clock,
            )
            self.assertEqual(calls, ['command-response-cut'])
            self.assertEqual(report['missing_checks'], ['upload-response-cut'])
            self.assertEqual(report['status'], 'failed')
        with tempfile.TemporaryDirectory() as directory:
            with self.assertRaisesRegex(ValueError, 'three hours'):
                SUITE.run_suite(('command-response-cut',), Path(directory) / 'suite',
                                (), 10801, execute=execute, clock=clock)
            with self.assertRaisesRegex(ValueError, 'distinct'):
                SUITE.run_suite(('command-response-cut', 'command-response-cut'),
                                Path(directory) / 'suite', (), 3600,
                                execute=execute, clock=clock)

    def test_missing_report_does_not_start_another_namespace(self):
        calls = []

        def execute(case, _command, _log, _timeout, _deadline):
            calls.append(case)
            return 1, False

        with tempfile.TemporaryDirectory() as directory:
            report = SUITE.run_suite(
                ('command-response-cut', 'upload-response-cut'), Path(directory) / 'suite',
                (), 3600, execute=execute,
            )
            self.assertEqual(calls, ['command-response-cut'])
            self.assertIn('cleanup is unverified', report['error'])
            self.assertEqual(report['missing_checks'],
                             ['command-response-cut', 'upload-response-cut'])

    def test_timeout_stops_even_when_driver_reports_cleanup(self):
        calls = []

        def execute(case, _command, log, _timeout, _deadline):
            calls.append(case)
            (log.parent / 'result.json').write_text(json.dumps({
                'status': 'failed', 'harness': {'commit': 'a' * 40},
                'checks': [], 'cleanup_errors': [], 'error': 'interrupted', 'cases': [],
            }))
            return 1, True

        with tempfile.TemporaryDirectory() as directory:
            report = SUITE.run_suite(
                ('command-response-cut', 'upload-response-cut'), Path(directory) / 'suite',
                (), 3600, execute=execute,
            )
            self.assertEqual(calls, ['command-response-cut'])
            self.assertIn('case timeout', report['error'])

    def test_real_executor_terminates_driver_with_cleanup_time_remaining(self):
        with tempfile.TemporaryDirectory() as directory:
            log = Path(directory) / 'case.log'
            started = time.monotonic()
            exit_code, timed_out = SUITE.execute_case(
                'command-response-cut',
                [sys.executable, '-c', 'import time; time.sleep(10)'], log,
                0.1, started + 2,
            )
            self.assertTrue(timed_out)
            self.assertNotEqual(exit_code, 0)
            self.assertLess(time.monotonic() - started, 2)


if __name__ == '__main__':
    unittest.main()
