import json
from pathlib import Path
import tempfile
from types import SimpleNamespace
import unittest

from e2e.multivm.node_preferences import run_preferences
from e2e.tests.test_multivm_local_first import inventory


class NodePreferencesTests(unittest.TestCase):
    def exercise(self, weighted_wrong=False):
        expected = ['node1', 'node2', 'node1' if weighted_wrong else 'node2',
                    'node2', 'node1', 'node2', 'node2']
        state = {'owners': {}, 'deleted': set(), 'options': []}

        class Sandbox:
            def __init__(self, **options):
                index = len(state['options'])
                self.id = 'pref-' + str(index)
                state['options'].append(options)
                state['owners'][self.id] = expected[index]
                self.commands = SimpleNamespace(run=lambda _cmd: SimpleNamespace(
                    exit_code=0, stdout='preference-ok'))

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
            if command[:5] == ('/opt/adx/current/bin/adxctl', '--config',
                                '/opt/adx/config/deployment.yaml', 'config', 'dump'):
                return 'services:\n  - role: coordinator\n    config:\n      placement: pack\n'
            if command[:5] == ('/opt/adx/current/bin/adx-inspect', '-c',
                                '/opt/adx/config/deployment.yaml', 'environment', 'get'):
                identity = command[5]
                live = identity not in state['deleted']
                return json.dumps({'node_id': state['owners'][identity],
                                   'state': 'Running' if live else 'Deleted',
                                   'resources_held': live, 'generation': 1})
            if command[:4] == ('sbox', '-a', '/run/sandboxd/sandboxd.sock', 'list'):
                identity = command[5].split('=', 1)[1]
                matches = identity not in state['deleted'] \
                    and state['owners'][identity] == machine.get('node_id')
                return 'ID STATE\n' + (identity + '-backend running\n' if matches else '')
            raise AssertionError(command)

        def resources(**_kwargs):
            return [SimpleNamespace(id=node_id, status=0,
                    capacity={'CPU': 4000, 'Memory': 8192, 'Disk': 10000},
                    allocatable={'CPU': 4000, 'Memory': 8192, 'Disk': 10000})
                    for node_id in ('node1', 'node2')]

        with tempfile.TemporaryDirectory() as directory:
            output = Path(directory)
            if weighted_wrong:
                with self.assertRaisesRegex(AssertionError, 'weighted node preference'):
                    run_preferences(inventory(), object(), 'image',
                                    '/run/sandboxd/sandboxd.sock', output,
                                    Sandbox, resources, remote)
                self.assertEqual(json.loads((output / 'node-preferences-result.json').read_text())['status'],
                                 'failed')
            else:
                report = run_preferences(inventory(), object(), 'image',
                                         '/run/sandboxd/sandboxd.sock', output,
                                         Sandbox, resources, remote)
                self.assertEqual(report['status'], 'passed')
                self.assertEqual(len(report['cases']), 5)
                self.assertEqual(state['options'][2]['schedule_affinities'][1]['weight'], 9)
                self.assertEqual(state['options'][4]['schedule_affinities'][0]['kind'], 1)
        self.assertEqual(state['deleted'], set(state['owners']))

    def test_preference_and_affinity_cases_prove_real_assignments(self):
        self.exercise()

    def test_wrong_weighted_node_fails_and_cleans_every_created_sandbox(self):
        self.exercise(weighted_wrong=True)
