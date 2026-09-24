import json
from pathlib import Path
import tempfile
import unittest
from unittest.mock import patch

from build.e2e.multivm.suite import RunConfig, case_command, run_plan
from build.e2e.tests.test_multivm_local_first import inventory


class SuiteTests(unittest.TestCase):
    def config(self, directory, cases, budget=10800, dedicated=False):
        root = Path(directory)
        return RunConfig(
            inventory=inventory(), inventory_path=root / 'inventory.json',
            output=root / 'suite', endpoint='control.test:8443',
            token_file=root / 'tenant-key', admin_token_file=root / 'admin-key',
            ca=root / 'ca.pem', image='image', release=root / 'release.tar.gz',
            socket='/run/sandboxd/sandboxd.sock', cases=tuple(cases),
            budget_seconds=budget, confirm_dedicated=dedicated,
        )

    def executor(self, failures=(), seconds=10):
        calls = []

        def run(case, command, log, timeout, report_name):
            calls.append((case, timeout))
            output = Path(command[command.index('--output') + 1])
            output.mkdir(parents=True, exist_ok=True)
            log.write_text('case=' + case + '\n')
            (output / report_name).write_text(json.dumps({
                'status': 'failed' if case in failures else 'passed',
                'cleanup_errors': [],
            }))
            return (124 if seconds > timeout else 1 if case in failures else 0), \
                min(seconds, timeout), seconds > timeout

        return run, calls

    def test_failure_is_recorded_and_later_cases_continue(self):
        with tempfile.TemporaryDirectory() as directory:
            config = self.config(directory, ('sdk', 'worker-restart', 'control-restart'))
            execute, calls = self.executor(failures={'worker-restart'})
            result = run_plan(config, execute=execute)
            self.assertEqual(result['status'], 'failed')
            self.assertEqual([case for case, _timeout in calls], list(config.cases))
            self.assertEqual(result['failed_cases'], ['worker-restart'])
            self.assertEqual(result['runtime_seconds'], 30)
            self.assertTrue((config.output / 'worker-restart' / 'case.log').is_file())

    def test_budget_is_shared_across_deployment_modes(self):
        with tempfile.TemporaryDirectory() as directory:
            config = self.config(directory, ('placement-pack',), budget=25)
            execute, calls = self.executor(seconds=20)
            self.assertEqual(run_plan(config, execute=execute)['status'], 'passed')
            config.cases = ('placement-spread',)
            result = run_plan(config, execute=execute)
            self.assertEqual(calls, [('placement-pack', 25), ('placement-spread', 5)])
            self.assertEqual(result['runtime_seconds'], 25)
            self.assertEqual(result['status'], 'failed')
            self.assertEqual(result['failed_cases'], ['placement-spread'])

    def test_stop_must_be_last_and_explicitly_authorized(self):
        with tempfile.TemporaryDirectory() as directory:
            config = self.config(directory, ('stop', 'sdk'), dedicated=True)
            with self.assertRaisesRegex(ValueError, 'last'):
                run_plan(config, execute=self.executor()[0])
            config.cases = ('sdk', 'stop')
            config.confirm_dedicated = False
            with self.assertRaisesRegex(ValueError, 'dedicated'):
                run_plan(config, execute=self.executor()[0])

    def test_rejects_mismatched_inventory_on_resume(self):
        with tempfile.TemporaryDirectory() as directory:
            config = self.config(directory, ('sdk',))
            run_plan(config, execute=self.executor()[0])
            config.cases = ('worker-restart',)
            config.inventory['machines'][1]['hostname'] = 'other-worker'
            with self.assertRaisesRegex(ValueError, 'inventory'):
                run_plan(config, execute=self.executor()[0])

    def test_does_not_restart_while_previous_case_group_is_alive(self):
        with tempfile.TemporaryDirectory() as directory:
            config = self.config(directory, ('worker-restart',))
            config.output.mkdir()
            state = {'schema_version': 1, 'inventory_sha256': None,
                     'budget_seconds': config.budget_seconds, 'runtime_seconds': 5,
                     'cases': [], 'active': {'case': 'sdk', 'started_at': 1,
                                              'log': str(config.output / 'sdk/case.log')}}
            from build.e2e.multivm.contract import inventory_digest
            state['inventory_sha256'] = inventory_digest(config.inventory)
            (config.output / 'budget-state.json').write_text(json.dumps(state))
            execute, calls = self.executor()
            with patch('build.e2e.multivm.suite.active_process_group', return_value=True):
                with self.assertRaisesRegex(RuntimeError, 'still running'):
                    run_plan(config, execute=execute)
            self.assertFalse(calls)

    def test_exit_zero_without_result_is_a_failure(self):
        with tempfile.TemporaryDirectory() as directory:
            config = self.config(directory, ('sdk',))

            def execute(_case, _command, log, _timeout, _report_name):
                log.write_text('script exited before reporting\n')
                return 0, 1, False

            result = run_plan(config, execute=execute)
            self.assertEqual(result['failed_cases'], ['sdk'])
            self.assertEqual(result['cases'][0]['status'], 'failed')

    def test_stop_ends_suite_across_invocations(self):
        with tempfile.TemporaryDirectory() as directory:
            config = self.config(directory, ('stop',), dedicated=True)
            run_plan(config, execute=self.executor()[0])
            config.cases = ('sdk',)
            with self.assertRaisesRegex(ValueError, 'already ended'):
                run_plan(config, execute=self.executor()[0])

    def test_case_arguments_keep_profile_requirements(self):
        with tempfile.TemporaryDirectory() as directory:
            config = self.config(directory, ('sdk',))
            destination = config.output / 'sdk'
            self.assertIn('--release', case_command(config, 'sdk', destination))
            self.assertIn('--admin-token-file', case_command(config, 'capacity', destination))
            self.assertEqual(case_command(config, 'placement-spread', destination)[-2:],
                             ['--placement', 'spread'])
            self.assertEqual(case_command(config, 'ingress-restart', destination)[-2:],
                             ['--role', 'ingress'])
            self.assertTrue(case_command(config, 'runtime-affinity', destination)[2]
                            .endswith('runtime_affinity.py'))
            self.assertEqual(case_command(config, 'stop', destination)[-1],
                             '--confirm-dedicated')
