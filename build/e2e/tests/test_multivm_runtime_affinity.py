import json
from pathlib import Path
import tempfile
from types import SimpleNamespace
import unittest

from e2e.multivm.runtime_affinity import run_runtime_affinity
from e2e.tests.test_multivm_local_first import inventory


class RuntimeAffinityTests(unittest.TestCase):
    def exercise(self, classes=None, assigned='node2'):
        classes = classes or {'node1': ['runc'], 'node2': ['runc', 'runsc']}
        state = {'created': 0, 'deleted': False, 'closed': False, 'options': None}

        class Sandbox:
            def __init__(self, **options):
                state['created'] += 1
                state['options'] = options
                self.id = 'runtime-choice'
                self.commands = SimpleNamespace(run=lambda _cmd: SimpleNamespace(
                    exit_code=0, stdout='runtime-ready'))

            def kill(self):
                state['deleted'] = True

            def close(self):
                state['closed'] = True

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
            if command[:5] == ('/opt/adx/current/bin/adx-inspect', '-c',
                                '/opt/adx/config/deployment.yaml', 'node', 'get'):
                node_id = command[5]
                return json.dumps({'id': node_id, 'available': True,
                                   'runtime_classes': classes[node_id],
                                   'session': {'routable': True}})
            if command[:5] == ('/opt/adx/current/bin/adx-inspect', '-c',
                                '/opt/adx/config/deployment.yaml', 'environment', 'get'):
                return json.dumps({'node_id': assigned,
                                   'state': 'Deleted' if state['deleted'] else 'Running',
                                   'resources_held': not state['deleted'], 'generation': 1})
            if command[:4] == ('sbox', '-a', '/run/sandboxd/sandboxd.sock', 'list'):
                live = not state['deleted'] and machine.get('node_id') == assigned
                return 'ID STATE\n' + ('backend running\n' if live else '')
            raise AssertionError(command)

        with tempfile.TemporaryDirectory() as directory:
            output = Path(directory)
            if sum('runsc' in value for value in classes.values()) != 1:
                with self.assertRaisesRegex(AssertionError, 'exactly one'):
                    run_runtime_affinity(inventory(), object(), 'image',
                                         '/run/sandboxd/sandboxd.sock', output,
                                         Sandbox, remote)
                self.assertEqual(state['created'], 0)
                self.assertEqual(json.loads((output / 'runtime-affinity-result.json')
                                            .read_text())['status'], 'failed')
            elif assigned != 'node2':
                with self.assertRaisesRegex(AssertionError, 'assignment'):
                    run_runtime_affinity(inventory(), object(), 'image',
                                         '/run/sandboxd/sandboxd.sock', output,
                                         Sandbox, remote)
                self.assertTrue(state['deleted'] and state['closed'])
            else:
                report = run_runtime_affinity(inventory(), object(), 'image',
                                              '/run/sandboxd/sandboxd.sock', output,
                                              Sandbox, remote)
                self.assertEqual(report['status'], 'passed')
                self.assertEqual(report['assignment']['node_id'], 'node2')
                self.assertEqual(state['options']['runtime'], 'runsc')
                self.assertNotIn('node_id', state['options'])
                self.assertTrue(state['deleted'] and state['closed'])

    def test_unique_runsc_worker_is_chosen_without_node_pin(self):
        self.exercise()

    def test_runc_only_workers_fail_preflight_before_creation(self):
        self.exercise(classes={'node1': ['runc'], 'node2': ['runc']})

    def test_both_workers_advertising_runtime_fail_preflight(self):
        self.exercise(classes={'node1': ['runc', 'runsc'],
                               'node2': ['runc', 'runsc']})

    def test_wrong_worker_assignment_fails_and_cleans(self):
        self.exercise(assigned='node1')
