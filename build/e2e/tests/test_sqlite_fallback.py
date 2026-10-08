"""The E2E oracle reads the same SQLite pending-row shape as adxlet."""

import importlib.util
import json
from pathlib import Path
import sqlite3
import sys
import tempfile
import types
import unittest
from unittest.mock import patch


ROOT = Path(__file__).resolve().parents[1]
SPEC = importlib.util.spec_from_file_location("e2e_node_sqlite", ROOT / "node.py")
NODE = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(NODE)


class SqliteFallbackOracleTests(unittest.TestCase):
    def test_restarted_node_keeps_pending_delete_and_original_live_backend(self):
        before = {'pid': 101, 'session_id': 'old-session'}
        status = {'services': [{'role': 'adxlet', 'pid': 202}]}
        journaled = {
            'keep_id': 'live', 'idle_id': 'idle',
            'keep_backend': 'runtime-live', 'keep_runtime_id': 'runtime-live',
        }
        records = {
            'environment:live': json.dumps({
                'result': {'state': 'Running', 'runtime': {'id': 'runtime-live'}},
            }),
            'environment:idle': json.dumps({'result': {'state': 'Running'}}),
        }
        pending = [{'spec': {'id': 'idle'}, 'state': 'Deleted'}]
        evidence = NODE.validated_sqlite_node_restart(
            before, status, records, pending, ['runtime-live'], journaled,
        )
        self.assertEqual(evidence['pid_before'], 101)
        self.assertEqual(evidence['pid_after'], 202)
        self.assertEqual(evidence['pending_records'], 1)
        with self.assertRaisesRegex(AssertionError, 'backend'):
            NODE.validated_sqlite_node_restart(
                before, status, records, pending, [], journaled,
            )
        with self.assertRaisesRegex(AssertionError, 'pending'):
            NODE.validated_sqlite_node_restart(
                before, status, records, [], ['runtime-live'], journaled,
            )
        with self.assertRaisesRegex(AssertionError, 'duplicated'):
            NODE.validated_sqlite_node_restart(
                before, status, records, pending * 2, ['runtime-live'], journaled,
            )

    def test_unopened_healthy_journal_is_not_required(self):
        with tempfile.TemporaryDirectory() as directory:
            old = NODE.P
            NODE.P = Path(directory)
            try:
                self.assertIsNone(NODE.journal_pending())
            finally:
                NODE.P = old

    def test_sdk_scenario_preserves_live_backend_and_rejects_eager_journal(self):
        state = {"created": [], "deleted": [], "waited": [], "commands": []}

        class Sandbox:
            def __init__(self, **options):
                self.id = f"instance-{len(state['created']) + 1}"
                self.commands = types.SimpleNamespace(run=self.run)
                state['created'].append((self.id, options))

            def run(self, command):
                state['commands'].append(command)
                return types.SimpleNamespace(exit_code=0, stdout=command.removeprefix('printf '))

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
                state['deleted'].append(instance_id)

        def wait_deleted(instance_id, _connection, **_options):
            state['waited'].append(instance_id)

        modules = {
            'adx_sandbox': types.SimpleNamespace(Sandbox=Sandbox),
            'functional_lifecycle': types.SimpleNamespace(_wait_deleted=wait_deleted),
        }
        spec = importlib.util.spec_from_file_location('sqlite_sdk_case', ROOT / 'sqlite_fallback.py')
        scenario = importlib.util.module_from_spec(spec)
        with patch.dict(sys.modules, modules):
            spec.loader.exec_module(scenario)

        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            scenario.create(object(), 'image@sha256:test', root / 'sqlite-live.json',
                            journal_path=root / 'absent.sqlite')
            live = json.loads((root / 'sqlite-live.json').read_text())
            self.assertEqual((live['keep_id'], live['idle_id']), ('instance-1', 'instance-2'))
            self.assertEqual([options['idle_timeout'] for _, options in state['created']], [0, 6])
            self.assertTrue(all(options['detached'] for _, options in state['created']))
            (root / 'sqlite-journaled.json').write_text(json.dumps({'seconds': 13.5}))
            (root / 'sqlite-reconciled.json').write_text(json.dumps({'seconds': 2.25}))
            report = scenario.verify(object(), root, root / 'sqlite-result.json')
            self.assertEqual(report['status'], 'passed')
            self.assertEqual([case['seconds'] for case in report['cases']], [13.5, 2.25])
            self.assertEqual(state['deleted'], ['instance-1'])
            self.assertEqual(state['waited'], ['instance-1'])

        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            eager = root / 'eager.sqlite'
            eager.touch()
            with self.assertRaisesRegex(AssertionError, 'healthy node opened'):
                scenario.create(object(), 'image@sha256:test', root / 'sqlite-live.json',
                                journal_path=eager)
        self.assertEqual(state['deleted'][-2:], ['instance-3', 'instance-4'])

    def test_pending_wal_record_is_read_without_mutating_it(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            path = root / "degraded/results.sqlite"
            path.parent.mkdir()
            payload = {"spec": {"id": "idle-capsule"}, "state": "Deleted"}
            with sqlite3.connect(path) as connection:
                connection.executescript("PRAGMA journal_mode=WAL; PRAGMA synchronous=FULL; "
                                         "CREATE TABLE pending (sequence INTEGER PRIMARY KEY, "
                                         "environment TEXT NOT NULL, payload TEXT NOT NULL);")
                connection.execute("INSERT INTO pending(environment,payload) VALUES (?,?)",
                                   ("idle-capsule", json.dumps(payload)))
            old = NODE.P
            NODE.P = root
            try:
                self.assertEqual(NODE.journal_pending(), [payload])
                with sqlite3.connect(path) as connection:
                    self.assertEqual(connection.execute("SELECT COUNT(*) FROM pending")
                                     .fetchone()[0], 1)
            finally:
                NODE.P = old


if __name__ == "__main__":
    unittest.main()
