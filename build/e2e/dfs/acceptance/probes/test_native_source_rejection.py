"""Negative oracles for the bounded live source rejection evidence."""
import copy
import importlib.util
from pathlib import Path
import platform
import tempfile
import unittest

spec = importlib.util.spec_from_file_location('source_rejection',
    Path(__file__).with_name('native_source_rejection.py'))
probe = importlib.util.module_from_spec(spec)
spec.loader.exec_module(probe)


@unittest.skipUnless(platform.system() == 'Linux', 'authoritative tests run on Linux')
class SourceRejectionGuards(unittest.TestCase):
    def good(self):
        return {'status': {'state': 'FinalVerified', 'production_ready': False,
                 'container': 'managed-first', 'root': 'root-1', 'epoch': 2,
                 'access_generation': 2, 'home_node': 'node-a', 'home_session': 'session-a'},
                'runtime': {'id': 'managed-first', 'status': 'running', 'pid': 111},
                'process': {'pid': 111, 'starttick': 123, 'boot_id': 'boot-1'},
                'namespace': {'dev': 4, 'ino': 5}, 'source': {'dev': 6, 'ino': 7},
                'mount': '55 54 8:1 /root /workspace rw - ext4 /dev/vda rw',
                'runtime_commands': {'command-0001.command.json': '1' * 64}}

    def test_source_error_requires_exact_unknown_field(self):
        good = {'status': 'ERROR', 'error': 'unknown field `source`, expected `id` or `workspace`',
                'production_ready': False}
        probe.verify_rejection(good)
        for change in ({'status': 'Started'}, {'status': 'ERROR', 'error': 'busy'},
                       {'error': 'unknown field `workspace`, expected source'},
                       {'error': 'failed path /etc: source denied'},
                       {'production_ready': True}, {'error': None}):
            with self.subTest(change=change), self.assertRaises(ValueError):
                probe.verify_rejection(dict(good, **change))

    def test_identity_change_cannot_pass(self):
        before = self.good()
        probe.verify_unchanged(before, copy.deepcopy(before))
        for group, key, value in [('status', 'epoch', 3), ('status', 'home_session', 'session-b'),
                                 ('status', 'root', 'root-2'), ('status', 'access_generation', 3),
                                 ('process', 'pid', 112), ('process', 'starttick', 124),
                                 ('process', 'boot_id', 'boot-2'), ('namespace', 'ino', 99),
                                 ('source', 'ino', 99), ('runtime', 'id', 'other')]:
            after = copy.deepcopy(before)
            after[group][key] = value
            with self.subTest(group=group, key=key), self.assertRaises(ValueError):
                probe.verify_unchanged(before, after)
        after = copy.deepcopy(before)
        after['mount'] = after['mount'].replace('55 54', '56 54')
        with self.assertRaises(ValueError):
            probe.verify_unchanged(before, after)

    def test_runtime_artifact_add_change_and_removal_fail(self):
        before = self.good()
        for artifacts in ({}, {'command-0001.command.json': 'changed'},
                          dict(before['runtime_commands'], **{'command-0002.stdout': 'new'})):
            after = copy.deepcopy(before)
            after['runtime_commands'] = artifacts
            with self.subTest(artifacts=artifacts), self.assertRaises(ValueError):
                probe.verify_unchanged(before, after)

    def test_missing_unverified_or_mismatched_identity_fails(self):
        for group in ('process', 'namespace', 'source', 'mount', 'runtime_commands'):
            before = self.good()
            del before[group]
            with self.subTest(group=group), self.assertRaises(ValueError):
                probe.verify_unchanged(before, copy.deepcopy(before))
        for group, key, value in [('status', 'state', 'Idle'), ('status', 'epoch', 0),
                                 ('runtime', 'status', 'stopped'), ('runtime', 'pid', 112)]:
            before = self.good()
            before[group][key] = value
            with self.subTest(group=group, key=key), self.assertRaises(ValueError):
                probe.verify_unchanged(before, copy.deepcopy(before))

    def test_complete_artifact_set_includes_output_mutations(self):
        with tempfile.TemporaryDirectory() as tmp:
            directory = Path(tmp)
            (directory / 'command-0001.stdout').write_text('first')
            before = probe.command_artifacts(directory)
            (directory / 'command-0001.stdout').write_text('changed')
            self.assertNotEqual(before, probe.command_artifacts(directory))
            (directory / 'command-0002.exit.json').write_text('{}')
            self.assertEqual(len(probe.command_artifacts(directory)), 2)

    def test_incomplete_or_wrong_identity_types_never_pass(self):
        for group, key, value in [('process', 'pid', True), ('process', 'starttick', True),
                                 ('process', 'starttick', 0), ('process', 'boot_id', None),
                                 ('namespace', 'dev', True), ('namespace', 'ino', '5'),
                                 ('source', 'dev', '6'), ('source', 'ino', False),
                                 ('runtime', 'pid', True), ('status', 'epoch', True)]:
            before = self.good()
            before[group][key] = value
            with self.subTest(group=group, key=key), self.assertRaises(ValueError):
                probe.verify_unchanged(before, copy.deepcopy(before))
        for group, key in [('process', 'starttick'), ('process', 'boot_id'),
                           ('namespace', 'dev'), ('source', 'ino')]:
            before = self.good()
            del before[group][key]
            with self.subTest(group=group, key=key), self.assertRaises(ValueError):
                probe.verify_unchanged(before, copy.deepcopy(before))
        for group, value in [('mount', {'id': 55}), ('mount', 'nonempty but not mountinfo'),
                             ('runtime_commands', ['nonempty']),
                             ('runtime_commands', {'command-0001.stdout': 'not-a-sha'})]:
            before = self.good()
            before[group] = value
            with self.subTest(group=group, value=value), self.assertRaises(ValueError):
                probe.verify_unchanged(before, copy.deepcopy(before))

    def test_legal_exec_requires_complete_exact_bytes(self):
        probe.verify_content(probe.PAYLOAD)
        for content in (b'', probe.PAYLOAD[:-1], probe.PAYLOAD + b'extra', b'wrong'):
            with self.subTest(content=content), self.assertRaises(ValueError):
                probe.verify_content(content)

    @unittest.skipUnless((Path(__file__).parent.parent / "native-workspace-linux.py").exists(), "native-workspace-linux.py runner not migrated in this slice")
    def test_source_mode_refuses_semantics_before_preflight(self):
        driver_spec = importlib.util.spec_from_file_location('source_driver',
            Path(__file__).parent.parent / 'native-workspace-linux.py')
        driver = importlib.util.module_from_spec(driver_spec)
        driver_spec.loader.exec_module(driver)
        from types import SimpleNamespace
        for other in ({'semantics_only': True, 'semantics_probe': None},
                      {'semantics_only': False, 'semantics_probe': Path('/probe.py')}):
            run = object.__new__(driver.Run)
            run.args = SimpleNamespace(source_rejection_only=True, **other)
            with self.subTest(other=other), self.assertRaisesRegex(ValueError, 'mutually exclusive'):
                run.preflight()


if __name__ == '__main__':
    unittest.main()
