import json
from pathlib import Path
import tempfile
import threading
from types import SimpleNamespace
import unittest

from build.e2e.multivm.capacity_queue import run_capacity


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


class CapacityQueueTests(unittest.TestCase):
    def test_both_workers_full_then_pending_create_wakes_on_release(self):
        state = {'live': {}, 'pending': None, 'deleted': []}
        released = threading.Event()

        class Sandbox:
            def __init__(self, *, name=None, node_id=None, **_options):
                if name is not None:
                    state['pending'] = name
                    if not released.wait(timeout=3):
                        raise TimeoutError('test worker was never released')
                    node_id = 'node1'
                    self.id = 'tenant-' + name
                else:
                    self.id = 'holder-' + node_id
                self.node_id = node_id
                state['live'][self.id] = node_id
                self.commands = SimpleNamespace(run=lambda _command: SimpleNamespace(
                    stdout='queue-wakeup', exit_code=0))

            def kill(self):
                state['live'].pop(self.id, None)
                state['deleted'].append(self.id)
                if self.id == 'holder-node1':
                    released.set()

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
                return json.dumps({'id': instance_id,
                                   'node_id': state['live'].get(instance_id, 'node1'),
                                   'state': 'Running' if live else 'Deleted',
                                   'resources_held': live})
            if command[:4] == ('sbox', '-a', '/run/sandboxd/sandboxd.sock', 'list'):
                instance_id = command[5].split('=', 1)[1]
                return 'ID STATE\n' + (
                    instance_id + '-backend running\n'
                    if state['live'].get(instance_id) == machine.get('node_id') else '')
            raise AssertionError(command)

        def resources(**_kwargs):
            return [SimpleNamespace(id=node_id, status=0,
                                    allocatable={'CPU': 1000, 'Memory': 2048})
                    for node_id in ('node1', 'node2')]

        def queue(_endpoint, _key, _ca):
            return {'tenant-' + state['pending']} if state['pending'] and not released.is_set() else set()

        with tempfile.TemporaryDirectory() as directory:
            report = run_capacity(inventory(), object(), 'image', 'control:8443',
                                  'admin', 'ca', Path(directory), Sandbox, resources,
                                  queue, remote, pending_probe_seconds=.01)
            self.assertEqual(report['status'], 'passed')
            self.assertEqual(report['resumed']['node_id'], 'node1')
            self.assertEqual(report['checks'][-1], 'backend-and-ledger-cleanup')
        self.assertEqual(len(state['deleted']), 3)
        self.assertFalse(state['live'])
