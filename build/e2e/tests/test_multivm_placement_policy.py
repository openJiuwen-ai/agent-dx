import json
from pathlib import Path
import tempfile
from types import SimpleNamespace
import unittest

from e2e.multivm.placement_policy import run_placement_policy
from e2e.tests.test_multivm_local_first import inventory


class PlacementPolicyTests(unittest.TestCase):
    def exercise(self, policy, forced_owner=None, configured_policy=None):
        state = {'owners': {}, 'deleted': set()}

        class Sandbox:
            def __init__(self, **_options):
                self.id = 'placement-' + str(len(state['owners']) + 1)
                owner = forced_owner or ('node1' if policy == 'pack' or not state['owners']
                                         else 'node2')
                state['owners'][self.id] = owner
                self.commands = SimpleNamespace(run=lambda _cmd: SimpleNamespace(
                    exit_code=0, stdout='placement-ok'))

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
                return ('services:\n  - role: coordinator\n    config:\n      placement: '
                        + (configured_policy or policy) + '\n'
                        '  - role: apiserver\n    config:\n      create_mode: central\n')
            if command[:5] == ('/opt/adx/current/bin/adx-inspect', '-c',
                                '/opt/adx/config/deployment.yaml', 'environment', 'get'):
                identity = command[5]
                live = identity not in state['deleted']
                return json.dumps({'node_id': state['owners'][identity],
                                   'state': 'Running' if live else 'Deleted',
                                   'resources_held': live, 'generation': 1})
            if command[:4] == ('sbox', '-a', '/run/sandboxd/sandboxd.sock', 'list'):
                identity = command[5].split('=', 1)[1]
                live = identity not in state['deleted']
                matches = live and state['owners'][identity] == machine.get('node_id')
                return 'ID STATE\n' + (identity + '-backend running\n' if matches else '')
            raise AssertionError(command)

        def resources(**_kwargs):
            values = []
            for node_id in ('node1', 'node2'):
                held = sum(owner == node_id and identity not in state['deleted']
                           for identity, owner in state['owners'].items())
                values.append(SimpleNamespace(
                    id=node_id, status=0,
                    capacity={'CPU': 4000, 'Memory': 8192, 'Disk': 10000},
                    allocatable={'CPU': 4000 - 500 * held,
                                 'Memory': 8192 - 512 * held, 'Disk': 10000}))
            return values

        with tempfile.TemporaryDirectory() as directory:
            output = Path(directory)
            if configured_policy and configured_policy != policy:
                with self.assertRaisesRegex(AssertionError, 'configured placement'):
                    run_placement_policy(inventory(), object(), 'image',
                                         '/run/sandboxd/sandboxd.sock', output, policy,
                                         Sandbox, resources, remote)
            elif forced_owner and policy == 'spread':
                with self.assertRaisesRegex(AssertionError, 'Spread'):
                    run_placement_policy(inventory(), object(), 'image',
                                         '/run/sandboxd/sandboxd.sock', output, policy,
                                         Sandbox, resources, remote)
                self.assertEqual(state['deleted'], set(state['owners']))
            else:
                report = run_placement_policy(inventory(), object(), 'image',
                                              '/run/sandboxd/sandboxd.sock', output, policy,
                                              Sandbox, resources, remote)
                self.assertEqual(report['status'], 'passed')
                self.assertEqual(len(report['instances']), 2)
                self.assertEqual(state['deleted'], set(state['owners']))

    def test_pack_concentrates_on_one_worker(self):
        self.exercise('pack')

    def test_spread_separates_two_workers(self):
        self.exercise('spread')

    def test_spread_detects_policy_not_applied(self):
        self.exercise('spread', forced_owner='node1')

    def test_configured_mode_must_match_selected_case(self):
        self.exercise('spread', configured_policy='pack')
