import json
from pathlib import Path
import tempfile
import unittest

from e2e.multivm.budget import start_budget
from e2e.multivm.suite import run_plan
from e2e.tests.test_multivm_local_first import inventory
from e2e.tests import test_multivm_suite as suite_test


class BudgetStartTests(unittest.TestCase):
    def test_start_before_deployment_is_used_by_later_cases(self):
        with tempfile.TemporaryDirectory() as directory:
            output = Path(directory) / 'suite'
            state = start_budget(inventory(), output, 25, now=lambda: 1000)
            self.assertEqual(state['started_at'], 1000)
            config = suite_test.SuiteTests().config(directory, ('sdk',), budget=25)
            execute, calls = suite_test.SuiteTests().executor(seconds=1)
            result = run_plan(config, execute=execute, now=lambda: 1026)
            self.assertEqual(calls, [])
            self.assertEqual(result['failed_cases'], ['sdk'])
            self.assertEqual(result['cases'][0]['status'], 'not-run-budget-exhausted')

    def test_existing_budget_cannot_be_reset(self):
        with tempfile.TemporaryDirectory() as directory:
            output = Path(directory) / 'suite'
            original = start_budget(inventory(), output, 10800, now=lambda: 1000)
            with self.assertRaisesRegex(FileExistsError, 'budget-state'):
                start_budget(inventory(), output, 10800, now=lambda: 2000)
            self.assertEqual(json.loads((output / 'budget-state.json').read_text()), original)

    def test_budget_limit_is_three_hours(self):
        with tempfile.TemporaryDirectory() as directory:
            with self.assertRaisesRegex(ValueError, 'three hours'):
                start_budget(inventory(), Path(directory), 10801)
