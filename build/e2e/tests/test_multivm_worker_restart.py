import json
from pathlib import Path
import tempfile
from types import SimpleNamespace
import unittest
from unittest.mock import patch

from build.e2e.multivm.worker_restart import run_restart, run_session_probe


def inventory():
    return {
        'schema_version': 1,
        'machines': [
            {'role': 'control', 'machine_id': 'control-id', 'hostname': 'control',
             'address': '10.0.0.10', 'ssh_target': 'control'},
            {'role': 'worker-1', 'machine_id': 'worker-a-id', 'hostname': 'worker-a',
             'address': '10.0.0.11', 'ssh_target': 'worker-a', 'node_id': 'node1'},
            {'role': 'worker-2', 'machine_id': 'worker-b-id', 'hostname': 'worker-b',
             'address': '10.0.0.12', 'ssh_target': 'worker-b', 'node_id': 'node2'},
        ],
        'artifacts': {'commit': 'a' * 40, 'release_sha256': 'b' * 64,
                      'target': 'x86_64-unknown-linux-gnu'},
    }


class WorkerRestartTests(unittest.TestCase):
    def exercise(self, replacement_backend=False, fence=False, fence_fail=False,
                 invalid_probe=False):
        state = {'live': {}, 'restarted': False, 'signals': [], 'probe_calls': []}

        def probe(_control, worker, instance_id, old_session, new_session):
            state['probe_calls'].append((worker['node_id'], instance_id,
                                         old_session, new_session))
            if fence_fail:
                raise AssertionError('old session commit was accepted')
            if invalid_probe:
                return {'status': 'failed', 'grpc_code': 'OK'}
            return {'status': 'passed', 'grpc_code': 'FailedPrecondition'}

        class Sandbox:
            def __init__(self, *, node_id, **_options):
                self.id = 'sandbox-' + node_id
                state['live'][self.id] = node_id
                self.commands = SimpleNamespace(run=lambda command: SimpleNamespace(
                    stdout=command.removeprefix('printf '), exit_code=0))

            def kill(self):
                state['live'].pop(self.id, None)

            def close(self):
                pass

        def remote(machine, *command):
            if command == ('cat', '/etc/machine-id'):
                return machine['machine_id']
            if command == ('hostname',):
                return machine['hostname']
            if command == ('ip', '-j', 'address', 'show'):
                return json.dumps([{'addr_info': [{'local': machine['address']}]}])
            if command == ('cat', '/opt/adx/current/manifest.json'):
                return json.dumps({'commit': 'a' * 40, 'target': 'x86_64-unknown-linux-gnu'})
            if command[:5] == ('/opt/adx/current/bin/adx-inspect', '-c',
                                '/opt/adx/config/deployment.yaml', 'environment', 'get'):
                instance_id = command[5]
                return json.dumps({'id': instance_id, 'node_id': state['live'].get(instance_id),
                                   'state': 'Running', 'generation': 7})
            if command[:5] == ('/opt/adx/current/bin/adx-inspect', '-c',
                                '/opt/adx/config/deployment.yaml', 'node', 'get'):
                return json.dumps({'available': True, 'session': {
                    'id': 'new' if state['restarted'] else 'old', 'routable': True}})
            if command[:3] == ('/opt/adx/current/bin/adxctl', 'status', '--config'):
                return json.dumps({'services': [{'role': 'adxlet',
                                                  'pid': 235 if state['restarted'] else 234}]})
            if command[:4] == ('sbox', '-a', '/run/sandboxd/sandboxd.sock', 'list'):
                instance_id = command[5].split('=', 1)[1]
                if state['live'].get(instance_id) != machine.get('node_id'):
                    return 'ID STATE\n'
                backend = instance_id + ('-replaced' if replacement_backend and state['restarted'] else '-original')
                return f'ID STATE\n{backend} running\n'
            if command[:4] == ('sudo', '-n', 'kill', '-TERM'):
                state['signals'].append('TERM')
                state['restarted'] = True
                return ''
            raise AssertionError(command)

        def wait(check, message, _timeout):
            value = check()
            if not value:
                raise AssertionError(message)
            return value

        with tempfile.TemporaryDirectory() as directory:
            output = Path(directory)
            if replacement_backend:
                with self.assertRaisesRegex(AssertionError, 'changed instance ownership or backend'):
                    run_restart(inventory(), object(), 'image', '/run/sandboxd/sandboxd.sock',
                                output, Sandbox, remote, wait, probe=probe if fence else None)
                self.assertEqual(json.loads((output / 'worker-restart-result.json').read_text())['status'], 'failed')
            elif fence_fail or invalid_probe:
                expected = 'old session commit was accepted' if fence_fail else 'did not prove fencing'
                with self.assertRaisesRegex(AssertionError, expected):
                    run_restart(inventory(), object(), 'image', '/run/sandboxd/sandboxd.sock',
                                output, Sandbox, remote, wait, probe=probe)
                self.assertEqual(json.loads((output / 'worker-restart-result.json').read_text())['status'], 'failed')
            else:
                report = run_restart(inventory(), object(), 'image', '/run/sandboxd/sandboxd.sock',
                                     output, Sandbox, remote, wait, probe=probe if fence else None)
                self.assertEqual(report['status'], 'passed')
                self.assertEqual(report['restart']['new_session'], 'new')
                self.assertEqual(report['restart']['old_pid'], 234)
                if fence:
                    self.assertEqual(report['fencing']['grpc_code'], 'FailedPrecondition')
                    self.assertIn('old-session-commit-rejected', report['checks'])
            if fence:
                self.assertEqual(state['probe_calls'],
                                 [('node2', 'sandbox-node2', 'old', 'new')])
        self.assertEqual(state['signals'], ['TERM'])
        self.assertFalse(state['live'])

    def test_quick_restart_preserves_backend_and_generation(self):
        self.exercise()

    def test_replaced_backend_is_not_accepted_as_reattachment(self):
        self.exercise(replacement_backend=True)

    def test_old_session_commit_is_rejected_after_quick_replacement(self):
        self.exercise(fence=True)

    def test_accepted_old_session_commit_fails_and_cleans(self):
        self.exercise(fence=True, fence_fail=True)

    def test_probe_must_report_rejection_to_pass(self):
        self.exercise(fence=True, invalid_probe=True)

    def test_probe_requires_failed_precondition_and_unchanged_record(self):
        control = {'coordinator_rpc_address': 'control:19000'}
        worker = {'node_id': 'node2'}
        valid = {'status': 'passed', 'grpc_code': 'FailedPrecondition',
                 'record_unchanged': True, 'node_id': 'node2',
                 'instance_id': 'sandbox-node2', 'old_session': 'old',
                 'new_session': 'new'}
        with patch('build.e2e.multivm.worker_restart.subprocess.run') as command:
            command.return_value = SimpleNamespace(returncode=0, stdout=json.dumps(valid),
                                                   stderr='')
            self.assertEqual(run_session_probe('/tmp/probe', control, worker,
                                               'sandbox-node2', 'old', 'new'), valid)
            self.assertEqual(command.call_args.args[0],
                             ['/tmp/probe', 'control:19000', 'node2',
                              'sandbox-node2', 'old', 'new'])
            command.return_value.stdout = json.dumps({**valid, 'record_unchanged': False})
            with self.assertRaisesRegex(AssertionError, 'did not prove fencing'):
                run_session_probe('/tmp/probe', control, worker,
                                  'sandbox-node2', 'old', 'new')
