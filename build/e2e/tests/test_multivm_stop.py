import json
from pathlib import Path
import subprocess
import tempfile
from types import SimpleNamespace
import unittest

from build.e2e.multivm.stop import run_stop
from build.e2e.tests.test_multivm_local_first import inventory


class StopTests(unittest.TestCase):
    def test_requires_explicit_dedicated_environment(self):
        with tempfile.TemporaryDirectory() as directory:
            with self.assertRaisesRegex(ValueError, 'dedicated'):
                run_stop(inventory(), object(), 'image', '/run/sandboxd/sandboxd.sock',
                         Path(directory))

    def exercise(self, route_leak=False):
        state = {'owners': {}, 'stopped': set(), 'order': [], 'deleted': set()}

        class Sandbox:
            def __init__(self, *, node_id, **_options):
                self.id = 'sandbox-' + node_id
                state['owners'][self.id] = node_id
                self.commands = SimpleNamespace(run=self.run)

            def run(self, _command):
                if 'worker-' + self.id[-1] in state['stopped'] and not route_leak:
                    raise RuntimeError('route unavailable')
                return SimpleNamespace(stdout='still-running', exit_code=0)

            def kill(self):
                state['deleted'].add(self.id)

            def close(self):
                pass

        def remote(machine, *command):
            role = machine['role']
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
                if role in state['stopped']:
                    raise subprocess.CalledProcessError(1, command)
                return json.dumps({'services': [{'role': 'adxlet' if role != 'control' else 'coordinator',
                                                 'pid': {'worker-1': 101, 'worker-2': 102,
                                                         'control': 103}[role]}]})
            if command[:5] == ('/opt/adx/current/bin/adx-inspect', '-c',
                                '/opt/adx/config/deployment.yaml', 'environment', 'get'):
                instance_id = command[5]
                stopped = 'worker-' + state['owners'][instance_id][-1] in state['stopped']
                return json.dumps({'state': 'Deleted' if stopped else 'Running',
                                   'resources_held': not stopped,
                                   'node_id': state['owners'][instance_id]})
            if command[:4] == ('sbox', '-a', '/run/sandboxd/sandboxd.sock', 'list'):
                ids = [instance_id + '-backend' for instance_id, owner in state['owners'].items()
                       if owner == machine.get('node_id') and role not in state['stopped']]
                if len(command) == 4:
                    return 'ID STATE\n' + ''.join(item + ' running\n' for item in ids)
                instance_id = command[5].split('=', 1)[1]
                return 'ID STATE\n' + ''.join(item + ' running\n' for item in ids
                                               if item.startswith(instance_id + '-'))
            if command[:4] == ('sudo', '-n', '/opt/adx/current/bin/adxctl', 'stop'):
                state['stopped'].add(role)
                state['order'].append(role)
                return '{}'
            if command[:2] == ('ps', '-p'):
                return '' if role in state['stopped'] else 'adx-process\n'
            raise AssertionError(command)

        def wait(check, message, _timeout):
            result = check()
            if not result:
                raise AssertionError(message)
            return result

        with tempfile.TemporaryDirectory() as directory:
            output = Path(directory)
            if route_leak:
                with self.assertRaisesRegex(AssertionError, 'remained routable'):
                    run_stop(inventory(), object(), 'image', '/run/sandboxd/sandboxd.sock',
                             output, dedicated=True, sandbox_factory=Sandbox,
                             remote=remote, wait=wait)
                self.assertEqual(state['order'], ['worker-2'])
                self.assertEqual(state['deleted'], {'sandbox-node1'})
                self.assertEqual(json.loads((output / 'stop-result.json').read_text())['status'],
                                 'failed')
            else:
                report = run_stop(inventory(), object(), 'image', '/run/sandboxd/sandboxd.sock',
                                  output, dedicated=True, sandbox_factory=Sandbox,
                                  remote=remote, wait=wait)
                self.assertEqual(report['status'], 'passed')
                self.assertEqual(state['order'], ['worker-2', 'worker-1', 'control'])
                self.assertEqual(len(report['instances']), 2)

    def test_stops_workers_before_control_and_cleans_backends(self):
        self.exercise()

    def test_route_leak_aborts_and_cleans_remaining_owned_instance(self):
        self.exercise(route_leak=True)
