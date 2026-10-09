"""Linux negative oracles for the independently bounded control-capacity case."""
import copy
import importlib.util
from pathlib import Path
import platform
import tempfile
from types import SimpleNamespace
import unittest
from unittest.mock import patch


def load(path, name):
    spec = importlib.util.spec_from_file_location(name, path)
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


probe = load(Path(__file__).with_name('native_control_capacity.py'), 'capacity')
identity = load(Path(__file__).with_name('native_source_rejection.py'), 'capacity_identity')


@unittest.skipUnless(platform.system() == 'Linux', 'authoritative guards run on Linux')
class CapacityGuards(unittest.TestCase):
    def status(self):
        return {'state': 'FinalVerified', 'production_ready': False, 'container': 'managed-first',
                'root': 'root-1', 'epoch': 1, 'access_generation': 1,
                'home_node': 'node-a', 'home_session': 'session-a'}

    def good_fill(self):
        initial = {'request': {'operation': 'start', 'id': 'first', 'workspace': 'workspace'},
                   'response': self.status()}
        records = [{'request': {'operation': 'start', 'id': f'capacity-busy-{i:02d}', 'workspace': 'workspace'},
                    'response': {'status': 'ERROR', 'production_ready': False,
                                 'error': probe.ERRORS[16], 'active': self.status()}} for i in range(63)]
        return initial, records

    def before(self):
        return {'status': self.status(), 'runtime': {'id': 'managed-first', 'status': 'running', 'pid': 111},
                'process': {'pid': 111, 'starttick': 123, 'boot_id': 'boot-1'},
                'namespace': {'dev': 4, 'ino': 5}, 'source': {'dev': 6, 'ino': 7},
                'mount': '55 54 8:1 /root /workspace rw - ext4 /dev/vda rw',
                'runtime_commands': {'command-0001.command.json': '1' * 64}}

    def full(self, after=None, response=None, observations=None, statuses=None):
        before = self.before()
        probe.verify_full(before, before if after is None else after,
                          {'status': 'ERROR', 'production_ready': False, 'error': probe.ERRORS[28]}
                          if response is None else response,
                          {'fuse': False, 'native': False} if observations is None else observations,
                          [self.status(), self.status()] if statuses is None else statuses, identity)

    def test_exact_count_and_unique_otherwise_valid_busy_starts(self):
        initial, records = self.good_fill()
        probe.verify_fill(initial, records)
        for bad in (records[:-1], records + [records[-1]]):
            with self.subTest(count=len(bad)), self.assertRaises(ValueError):
                probe.verify_fill(initial, bad)
        for change in ({'id': 'capacity-busy-00'}, {'operation': 'exec'}, {'workspace': '../escape'}):
            bad = copy.deepcopy(records)
            bad[1]['request'].update(change)
            with self.subTest(change=change), self.assertRaises(ValueError):
                probe.verify_fill(initial, bad)

    def test_failed_initial_start_cannot_count_as_a_verified_ledger_entry(self):
        initial, records = self.good_fill()
        for key, value in [('state', 'Idle'), ('production_ready', True)]:
            bad = copy.deepcopy(initial)
            bad['response'][key] = value
            with self.subTest(key=key), self.assertRaises(ValueError):
                probe.verify_fill(bad, records)

    def test_busy_requires_exact_EBUSY_and_current_active_grant(self):
        initial, records = self.good_fill()
        for change in ({'status': 'Started'}, {'error': probe.ERRORS[28]},
                       {'error': 'resource busy'}, {'active': {'state': 'Idle'}}):
            bad = copy.deepcopy(records)
            bad[30]['response'].update(change)
            with self.subTest(change=change), self.assertRaises(ValueError):
                probe.verify_fill(initial, bad)

    def test_full_requires_exact_ENOSPC_not_success_or_unrelated_error(self):
        self.full()
        for change in ({'status': 'Executed'}, {'error': probe.ERRORS[16]},
                       {'error': 'No space left on device'}, {'production_ready': True}):
            response = {'status': 'ERROR', 'production_ready': False, 'error': probe.ERRORS[28]}
            response.update(change)
            with self.subTest(change=change), self.assertRaises(ValueError):
                self.full(response=response)

    def test_full_reuses_physical_identity_and_runtime_artifact_oracle(self):
        for group, key, value in [('process', 'starttick', 124), ('namespace', 'ino', 99),
                                 ('source', 'ino', 99), ('status', 'epoch', 2),
                                 ('runtime_commands', 'command-0002.stdout', '2' * 64)]:
            after = self.before()
            after[group][key] = value
            with self.subTest(group=group, key=key), self.assertRaises(ValueError):
                self.full(after=after)

    def test_denied_file_must_be_absent_in_both_views_with_actual_booleans(self):
        for observations in ({'fuse': True, 'native': False}, {'fuse': False, 'native': True},
                             {'fuse': False}, {'fuse': 0, 'native': False}):
            with self.subTest(observations=observations), self.assertRaises(ValueError):
                self.full(observations=observations)

    def test_two_status_observations_cannot_be_missing_or_changed(self):
        for statuses in ([], [self.status()], [self.status(), dict(self.status(), state='Idle')]):
            with self.subTest(statuses=statuses), self.assertRaises(ValueError):
                self.full(statuses=statuses)

    @unittest.skipUnless((Path(__file__).parent.parent / "native-workspace-linux.py").exists(), "native-workspace-linux.py runner not migrated in this slice")
    def test_capacity_mode_rejects_other_modes_before_runtime(self):
        driver = load(Path(__file__).parent.parent / 'native-workspace-linux.py', 'capacity_driver')
        for change in ({'source_rejection_only': True}, {'semantics_only': True},
                       {'semantics_probe': Path('/probe.py')}):
            args = dict(control_capacity_only=True, source_rejection_only=False,
                        semantics_only=False, semantics_probe=None)
            args.update(change)
            run = object.__new__(driver.Run)
            run.args = SimpleNamespace(**args)
            with self.subTest(change=change), self.assertRaises(ValueError):
                run.preflight()

    @unittest.skipUnless((Path(__file__).parent.parent / "native-workspace-linux.py").exists(), "native-workspace-linux.py runner not migrated in this slice")
    def test_status_side_effect_is_observed_before_final_success(self):
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            before = self.before()
            status = self.status()
            events = []
            def native(ident, *action):
                events.append(ident)
                if ident == 'capacity-full-status-1':
                    before['runtime_commands']['command-unexpected.stdout'] = '2' * 64
                return copy.deepcopy(status)
            def snapshot(run, label, container):
                events.append(label)
                return copy.deepcopy(before)
            def exchange(socket, request):
                code = 16 if request['operation'] == 'start' else 28
                response = {'status': 'ERROR', 'production_ready': False, 'error': probe.ERRORS[code]}
                if code == 16:
                    response['active'] = copy.deepcopy(status)
                return response
            run = SimpleNamespace(root=root, args=SimpleNamespace(controller=root/'client.py'),
                                  native=native, save=lambda *a: None, check=lambda *a: None)
            shared = SimpleNamespace(snapshot=snapshot, verify_unchanged=identity.verify_unchanged)
            client = SimpleNamespace(exchange=exchange)
            with patch.object(probe, 'load', side_effect=[shared, client]), self.assertRaises(ValueError):
                probe.execute(run, status, root/'workspace')
            self.assertLess(events.index('capacity-full-status-1'), events.index('capacity-after-full'))


if __name__ == '__main__':
    unittest.main()
