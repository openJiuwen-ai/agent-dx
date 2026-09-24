import json
from pathlib import Path
import tempfile
import unittest
from unittest.mock import patch

from e2e.multivm.suite import RunConfig, case_command, run_plan
from e2e.tests.test_multivm_local_first import inventory


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
            self.assertEqual([case for case, _timeout in calls],
                             ['placement-pack', 'placement-spread'])
            self.assertAlmostEqual(calls[0][1], 25, delta=.1)
            self.assertAlmostEqual(calls[1][1], 5, delta=.1)
            self.assertEqual(result['runtime_seconds'], 25)
            self.assertEqual(result['status'], 'failed')
            self.assertEqual(result['failed_cases'], ['placement-spread'])

    def test_budget_includes_time_between_deployment_modes(self):
        with tempfile.TemporaryDirectory() as directory:
            config = self.config(directory, ('placement-pack',), budget=25)
            clock = [1000.0]
            execute, calls = self.executor(seconds=5)

            def timed_execute(*args):
                result = execute(*args)
                clock[0] += result[1]
                return result

            self.assertEqual(run_plan(config, execute=timed_execute,
                                      now=lambda: clock[0])['status'], 'passed')
            clock[0] += 20  # Reconfigure Pack to Spread between suite invocations.
            config.cases = ('placement-spread',)
            result = run_plan(config, execute=timed_execute, now=lambda: clock[0])
            self.assertEqual(calls, [('placement-pack', 25)])
            self.assertEqual(result['failed_cases'], ['placement-spread'])
            self.assertEqual(result['cases'][-1]['status'], 'not-run-budget-exhausted')

    def test_case_finishing_after_wall_deadline_is_not_a_pass(self):
        with tempfile.TemporaryDirectory() as directory:
            config = self.config(directory, ('sdk',), budget=10)
            clock = [1000.0]

            def slow_finalization(_case, command, log, _timeout, report_name):
                destination = Path(command[command.index('--output') + 1])
                destination.mkdir(parents=True, exist_ok=True)
                log.write_text('case completed but reporting was delayed\n')
                (destination / report_name).write_text(json.dumps({
                    'status': 'passed', 'cleanup_errors': [],
                }))
                clock[0] += 11
                return 0, 9, False

            result = run_plan(config, execute=slow_finalization, now=lambda: clock[0])
            self.assertEqual(result['failed_cases'], ['sdk'])
            self.assertEqual(result['cases'][0]['status'], 'failed')

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
            state = {'schema_version': 2, 'inventory_sha256': None,
                     'budget_seconds': config.budget_seconds, 'runtime_seconds': 5,
                     'scope': 'cases-only',
                     'started_at': 1, 'finished_at': None,
                     'cases': [], 'active': {'case': 'sdk', 'started_at': 1,
                                              'log': str(config.output / 'sdk/case.log')}}
            from e2e.multivm.contract import inventory_digest
            state['inventory_sha256'] = inventory_digest(config.inventory)
            (config.output / 'budget-state.json').write_text(json.dumps(state))
            execute, calls = self.executor()
            with patch('e2e.multivm.suite.active_process_group', return_value=True):
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
            self.assertIn('--admin-token-file', case_command(config, 'auth', destination))
            self.assertIn('--admin-token-file', case_command(config, 'capacity', destination))
            self.assertEqual(case_command(config, 'placement-spread', destination)[-2:],
                             ['--placement', 'spread'])
            self.assertEqual(case_command(config, 'ingress-restart', destination)[-2:],
                             ['--role', 'ingress'])
            self.assertTrue(case_command(config, 'runtime-affinity', destination)[2]
                            .endswith('runtime_affinity.py'))
            config.session_probe = destination / 'probe'
            self.assertEqual(case_command(config, 'session-fence', destination)[-2:],
                             ['--session-probe', str(config.session_probe)])
            config.route_probe = destination / 'route-probe'
            self.assertEqual(case_command(config, 'stop', destination)[-3:],
                             ['--confirm-dedicated', '--route-probe', str(config.route_probe)])

    def test_session_fence_requires_executable_probe(self):
        with tempfile.TemporaryDirectory() as directory:
            config = self.config(directory, ('session-fence',))
            execute, calls = self.executor()
            with self.assertRaisesRegex(ValueError, 'session-probe'):
                run_plan(config, execute=execute)
            config.session_probe = Path(directory) / 'probe'
            config.session_probe.write_text('not executable')
            with self.assertRaisesRegex(ValueError, 'session-probe'):
                run_plan(config, execute=execute)
            self.assertFalse(calls)

    def test_auth_uses_shared_budget_and_saves_its_report(self):
        with tempfile.TemporaryDirectory() as directory:
            config = self.config(directory, ('auth',))
            execute, calls = self.executor()
            result = run_plan(config, execute=execute)
            self.assertEqual(result['status'], 'passed')
            self.assertEqual(calls, [('auth', 300)])
            self.assertTrue((config.output / 'auth' / 'auth-result.json').is_file())

    def test_stop_route_probe_must_be_executable_when_configured(self):
        with tempfile.TemporaryDirectory() as directory:
            config = self.config(directory, ('stop',), dedicated=True)
            config.route_probe = Path(directory) / 'probe'
            execute, calls = self.executor()
            with self.assertRaisesRegex(ValueError, 'route-probe'):
                run_plan(config, execute=execute)
            config.route_probe.write_text('not executable')
            with self.assertRaisesRegex(ValueError, 'route-probe'):
                run_plan(config, execute=execute)
            self.assertFalse(calls)
