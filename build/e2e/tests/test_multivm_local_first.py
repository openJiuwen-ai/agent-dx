import json
from pathlib import Path
import tempfile
import threading
from types import SimpleNamespace
import unittest

from build.e2e.multivm.local_first import run_local_first


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


class Conflict(Exception):
    code = 'CONFLICT'
    retry = 'never'
    outcome = 'not_started'


class LocalFirstTests(unittest.TestCase):
    def exercise(self, double_account=False, missing_fallback=False):
        state = {'live': {}, 'all': {}, 'claimed': set(), 'fallback_count': 0,
                 'deleted': set()}
        mutex = threading.Lock()

        class Sandbox:
            def __init__(self, *, name, cpu, node_id=None, **_options):
                self.id = 'tenant-' + name
                self.commands = SimpleNamespace(run=lambda _command: SimpleNamespace(
                    stdout='local-first-three-vm', exit_code=0))
                with mutex:
                    if self.id in state['all']:
                        if cpu != 500:
                            raise Conflict('different specification')
                    else:
                        if name.endswith('-a'):
                            owner = 'node1'
                        elif name.endswith('-b') or node_id:
                            owner = 'node2'
                        else:
                            owner = 'node1'
                        state['live'][self.id] = owner
                        state['all'][self.id] = owner
                        if 'fallback' in name:
                            state['fallback_count'] += 1
                            if state['fallback_count'] == 1:
                                state['claimed'].add(self.id)
                        else:
                            state['claimed'].add(self.id)

            def kill(self):
                state['deleted'].add(self.id)
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
                live = instance_id in state['live']
                return json.dumps({'id': instance_id, 'node_id': state['all'][instance_id],
                                   'state': 'Running' if live else 'Deleted',
                                   'resources_held': live, 'generation': 1})
            if command[:4] == ('sbox', '-a', '/run/sandboxd/sandboxd.sock', 'list'):
                instance_id = command[5].split('=', 1)[1]
                return 'ID STATE\n' + (
                    instance_id + '-backend running\n'
                    if state['live'].get(instance_id) == machine.get('node_id') else '')
            raise AssertionError(command)

        def resources(**_kwargs):
            counts = {'node1': 0, 'node2': 0}
            for instance_id, owner in state['live'].items():
                counts[owner] += 500
            if double_account and state['fallback_count'] == 2:
                counts['node2'] += 500
            return [SimpleNamespace(id=node_id, allocatable={'CPU': 3000 - counts[node_id]})
                    for node_id in ('node1', 'node2')]

        def claims(_control, identities, _remote):
            return identities & state['claimed']

        def fallbacks(worker, identities, _remote):
            if missing_fallback:
                return set()
            if worker['node_id'] == 'node1':
                return identities - state['claimed']
            return set()

        def wait(check, message):
            result = check()
            if not result:
                raise AssertionError(message)
            return result

        with tempfile.TemporaryDirectory() as directory:
            output = Path(directory)
            if double_account or missing_fallback:
                expected = 'double-counted' if double_account else 'incomplete claim and forward events'
                with self.assertRaisesRegex(AssertionError, expected):
                    run_local_first(inventory(), object(), 'image', '/run/sandboxd/sandboxd.sock',
                                    output, Sandbox, Conflict, resources, remote, claims, fallbacks, wait)
                self.assertEqual(json.loads((output / 'local-first-result.json').read_text())['status'], 'failed')
            else:
                result = run_local_first(inventory(), object(), 'image', '/run/sandboxd/sandboxd.sock',
                                         output, Sandbox, Conflict, resources, remote, claims, fallbacks, wait)
                self.assertEqual(result['status'], 'passed')
                self.assertEqual(result['fallback']['cpu_millis_delta'], 1000)
                self.assertEqual(len(result['instances']), 5)
        self.assertFalse(state['live'])
        self.assertEqual(len(state['deleted']), 5)

    def test_local_claim_and_central_fallback_share_one_resource_ledger(self):
        self.exercise()

    def test_double_accounting_fails_and_all_instances_are_cleaned(self):
        self.exercise(double_account=True)

    def test_missing_forward_event_cannot_be_inferred_from_missing_claim(self):
        self.exercise(missing_fallback=True)
