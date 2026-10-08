import json
from pathlib import Path
import tempfile
from types import SimpleNamespace
import unittest

from e2e.multivm.worker_failure import run_failure


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


class FakeSandboxError(Exception):
    pass


class WorkerFailureTests(unittest.TestCase):
    def exercise(self, route_leaks=False):
        state = {'instances': {}, 'frozen': False, 'signals': [], 'next_id': 0}

        class Sandbox:
            def __init__(self, *, node_id, **_options):
                state['next_id'] += 1
                self.id = f'sandbox-{node_id}-{state["next_id"]}'
                self.node_id = node_id
                state['instances'][self.id] = node_id
                self.commands = SimpleNamespace(run=self.command)

            def command(self, command):
                if command == 'printf baseline-route':
                    return SimpleNamespace(stdout='baseline-route', exit_code=0)
                if self.id == 'sandbox-node2-2' and (state['frozen'] or state.get('recovered')) and not route_leaks:
                    raise FakeSandboxError('route withdrawn')
                response = 'healthy-worker' if self.node_id == 'node1' else 'resumed-admission'
                return SimpleNamespace(stdout=response, exit_code=0)

            def kill(self):
                state['instances'].pop(self.id, None)

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
            if command[:4] == ('sbox', '-a', '/run/sandboxd/sandboxd.sock', 'list'):
                instance_id = command[5].split('=', 1)[1]
                return 'ID STATE\n' + (
                    instance_id + '-backend running\n'
                    if state['instances'].get(instance_id) == machine.get('node_id') else '')
            if command[:3] == ('/opt/adx/current/bin/adxctl', 'status', '--config'):
                return json.dumps({'services': [{'role': 'adxlet', 'pid': 234}]})
            if command[:5] == ('/opt/adx/current/bin/adx-inspect', '-c',
                                '/opt/adx/config/deployment.yaml', 'node', 'get'):
                failed = command[5] == 'node2' and state['frozen']
                return json.dumps({'available': not failed,
                                   'session': {'id': 'new' if state.get('recovered') else 'old',
                                               'routable': not failed}})
            if command[:5] == ('/opt/adx/current/bin/adx-inspect', '-c',
                                '/opt/adx/config/deployment.yaml', 'environment', 'get'):
                instance_id = command[5]
                failed = instance_id == 'sandbox-node2-2' and state['frozen']
                return json.dumps({'id': instance_id,
                                   'node_id': state['instances'].get(instance_id, 'node2'),
                                   'state': 'Failed' if failed else 'Running',
                                   'invalidated': failed, 'resources_held': not failed})
            if command[:4] == ('sudo', '-n', 'kill', '-STOP'):
                state['signals'].append('STOP')
                state['frozen'] = True
                return ''
            if command[:4] == ('sudo', '-n', 'kill', '-CONT'):
                state['signals'].append('CONT')
                state['frozen'] = False
                state['recovered'] = True
                state['instances'].pop('sandbox-node2-2', None)
                return ''
            raise AssertionError(command)

        def wait(check, message, _timeout):
            value = check()
            if not value:
                raise AssertionError(message)
            return value

        with tempfile.TemporaryDirectory() as directory:
            output = Path(directory)
            if route_leaks:
                with self.assertRaisesRegex(AssertionError, 'remained reachable'):
                    run_failure(inventory(), object(), 'image', '/run/sandboxd/sandboxd.sock',
                                output, Sandbox, FakeSandboxError, remote, wait=wait)
                self.assertEqual(json.loads((output / 'worker-failure-result.json').read_text())['status'], 'failed')
            else:
                report = run_failure(inventory(), object(), 'image', '/run/sandboxd/sandboxd.sock',
                                     output, Sandbox, FakeSandboxError, remote, wait=wait)
                self.assertEqual(report['status'], 'passed')
                self.assertIn('stale-backend-cleanup-before-readmission', report['checks'])
        self.assertEqual(state['signals'], ['STOP', 'CONT'])
        self.assertFalse(state['frozen'])
        self.assertFalse(state['instances'])

    def test_expired_worker_cleans_stale_backend_before_readmission(self):
        self.exercise()

    def test_route_leak_fails_but_frozen_worker_is_resumed(self):
        self.exercise(route_leaks=True)
