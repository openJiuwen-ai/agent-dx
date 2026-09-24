import json
from pathlib import Path
import subprocess
import tempfile
from types import SimpleNamespace
import unittest

from build.e2e.multivm.control_restart import run_control_restarts
from build.e2e.tests.test_multivm_local_first import inventory


class ControlRestartTests(unittest.TestCase):
    def exercise(self, change_owner=False, duplicate_backend=False):
        state = {'epoch': 1, 'pids': {'coordinator': 10, 'apiserver': 11, 'redis': 12},
                 'instances': {}, 'deleted': set(), 'restarts': [], 'outage': 0}

        class Sandbox:
            def __init__(self, *, node_id, **_options):
                self.id = 'sandbox-' + node_id
                self.commands = SimpleNamespace(run=lambda _command: SimpleNamespace(
                    exit_code=0, stdout='control-restart'))
                self.files = SimpleNamespace(write=lambda *_args: None,
                                             read=lambda *_args, **_kwargs: b'marker')
                state['instances'][self.id] = node_id

            def kill(self):
                state['deleted'].add(self.id)

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
                return json.dumps({'commit': 'a' * 40,
                                   'target': 'x86_64-unknown-linux-gnu'})
            if command[:3] == ('/opt/adx/current/bin/adxctl', 'status', '--config'):
                if state['outage']:
                    state['outage'] -= 1
                    raise subprocess.CalledProcessError(1, command)
                return json.dumps({'services': [{'role': role, 'pid': pid}
                                                for role, pid in state['pids'].items()]})
            if command[:4] == ('/opt/adx/current/bin/adx-inspect', '-c',
                                '/opt/adx/config/deployment.yaml', 'summary'):
                return json.dumps({'epoch': state['epoch']})
            if command[:5] == ('/opt/adx/current/bin/adx-inspect', '-c',
                                '/opt/adx/config/deployment.yaml', 'environment', 'get'):
                owner = state['instances'][command[5]]
                if change_owner and state['restarts']:
                    owner = 'node2' if owner == 'node1' else 'node1'
                deleted = command[5] in state['deleted']
                return json.dumps({'node_id': owner,
                                   'generation': 1, 'state': 'Deleted' if deleted else 'Running',
                                   'resources_held': not deleted})
            if command[:5] == ('/opt/adx/current/bin/adx-inspect', '-c',
                                '/opt/adx/config/deployment.yaml', 'node', 'get'):
                return json.dumps({'available': True, 'session': {'routable': True}})
            if command[:4] == ('sbox', '-a', '/run/sandboxd/sandboxd.sock', 'list'):
                instance_id = command[5].split('=', 1)[1]
                live = instance_id in state['instances'] and instance_id not in state['deleted']
                return 'ID STATE\n' + (instance_id + '-backend running\n'
                                       if live and (state['instances'][instance_id] == machine.get('node_id')
                                                    or duplicate_backend and state['restarts'])
                                       else '')
            if command[:4] == ('sudo', '-n', 'kill', '-KILL'):
                old_pid = int(command[4])
                role = next(role for role, pid in state['pids'].items() if pid == old_pid)
                state['pids'][role] += 100
                if role == 'coordinator':
                    state['epoch'] += 1
                state['restarts'].append(role)
                state['outage'] = 1
                return ''
            raise AssertionError(command)

        def wait(check, message, _timeout):
            for _ in range(3):
                value = check()
                if value is not None:
                    return value
            raise AssertionError(message)

        with tempfile.TemporaryDirectory() as directory:
            output = Path(directory)
            if change_owner or duplicate_backend:
                expected = 'ownership changed' if change_owner else 'backend changed'
                with self.assertRaisesRegex(AssertionError, expected):
                    run_control_restarts(inventory(), object(), 'image',
                                         '/run/sandboxd/sandboxd.sock', output,
                                         Sandbox, remote, wait)
                self.assertEqual(json.loads((output / 'control-restart-result.json').read_text())['status'],
                                 'failed')
            else:
                report = run_control_restarts(inventory(), object(), 'image',
                                              '/run/sandboxd/sandboxd.sock', output,
                                              Sandbox, remote, wait)
                self.assertEqual(report['status'], 'passed')
                self.assertEqual(state['restarts'], ['coordinator', 'apiserver', 'redis'])
                self.assertEqual(len(report['instances']), 2)
        self.assertEqual(state['deleted'], set(state['instances']))

    def test_control_role_restarts_preserve_ownership_and_routes(self):
        self.exercise()

    def test_changed_ownership_fails_and_cleans_owned_instances(self):
        self.exercise(change_owner=True)

    def test_duplicate_backend_fails_and_cleans_owned_instances(self):
        self.exercise(duplicate_backend=True)
