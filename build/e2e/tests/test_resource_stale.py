"""The resource-expiry SDK scenario keeps one backend and drains it cleanly."""

import importlib.util
import json
from pathlib import Path
import sys
import tempfile
import types
import unittest
from unittest.mock import patch


ROOT = Path(__file__).resolve().parents[1]


class ResourceStaleScenarioTests(unittest.TestCase):
    def test_rejects_stale_allocation_then_reopens_without_replacing_live_backend(self):
        records = {
            'node:node1': {'node': {'available': True},
                           'session': {'id': 'session-one', 'routable': True}},
            'node:node2': {'node': {'available': True},
                           'session': {'id': 'session-two', 'routable': True}},
        }
        backends = {}
        deleted = []

        class SandboxError(Exception):
            pass

        class Sandbox:
            def __init__(self, **options):
                if options.get('name', '').startswith('stale-rejected-'):
                    raise SandboxError('central scheduling queue deadline exceeded')
                self.id = 'instance-' + str(len(backends) + len(deleted) + 1)
                self.commands = types.SimpleNamespace(run=self.run)
                backends[self.id] = 'backend-' + self.id
                records['environment:' + self.id] = {
                    'assignment': {'node_id': 'node1'},
                    'result': {'state': 'Running', 'runtime': {'id': 'runtime-' + self.id},
                               'resources_held': True},
                }

            def run(self, command):
                return types.SimpleNamespace(exit_code=0,
                                             stdout=command.removeprefix('printf '))

            def close(self):
                pass

            @classmethod
            def from_id(cls, instance_id, **_options):
                instance = object.__new__(cls)
                instance.id = instance_id
                instance.commands = types.SimpleNamespace(run=instance.run)
                return instance

            @classmethod
            def delete(cls, instance_id, **_options):
                deleted.append(instance_id)
                backends.pop(instance_id, None)
                records['environment:' + instance_id]['result'].update(
                    state='Deleted', resources_held=False)

        modules = {
            'adx_sandbox': types.SimpleNamespace(Sandbox=Sandbox, SandboxError=SandboxError),
            'functional_lifecycle': types.SimpleNamespace(_wait_deleted=lambda *_a, **_kw: None),
            'node': types.SimpleNamespace(
                catalog=lambda: {key: json.dumps(value) for key, value in records.items()},
                backend=lambda: sorted(backends.values()),
                labeled_backend=lambda instance_id: (
                    [backends[instance_id]] if instance_id in backends else []),
                persisted_runtime_id=lambda result: result['runtime']['id'],
            ),
        }
        spec = importlib.util.spec_from_file_location('resource_stale_case',
                                                      ROOT / 'resource_stale.py')
        scenario = importlib.util.module_from_spec(spec)
        with patch.dict(sys.modules, modules):
            spec.loader.exec_module(scenario)

        with tempfile.TemporaryDirectory() as directory:
            evidence = Path(directory)
            scenario.create(object(), 'test-image', evidence / 'resource-live.json')
            live = json.loads((evidence / 'resource-live.json').read_text())
            self.assertEqual(live['session_id'], 'session-one')
            self.assertEqual(backends[live['instance_id']], live['backend'])
            records['node:node1']['node']['available'] = False
            rejected = scenario.rejected(object(), 'test-image', evidence)
            self.assertEqual(rejected['held_before'], rejected['held_after'])
            self.assertEqual(len(backends), 1)
            records['node:node1']['node']['available'] = True
            (evidence / 'resource-stale.json').write_text(json.dumps({'seconds': 12.0}))
            (evidence / 'resource-fresh.json').write_text(json.dumps({'seconds': 2.0}))
            report = scenario.verify(object(), 'test-image', evidence,
                                     evidence / 'resource-result.json')
            self.assertEqual(report['status'], 'passed')
            self.assertEqual([case['id'] for case in report['cases']], [
                'reliability.resource-stale-admission',
                'reliability.resource-observation-recovery',
            ])
            self.assertEqual(sorted(deleted), ['instance-1', 'instance-2'])
            self.assertEqual(backends, {})


if __name__ == '__main__':
    unittest.main()
