"""Frozen one-hour acceptance must reject gaps, resource drift, OOM and partial work."""

from copy import deepcopy
from functools import lru_cache
import importlib.util
from pathlib import Path
import unittest
from unittest.mock import patch
import contextlib
import io
import json
import stat


@lru_cache(maxsize=1)
def module():
    spec = importlib.util.spec_from_file_location('soak_acceptance_tests',
                                                Path(__file__).resolve().parents[1] / 'soak_acceptance.py')
    value = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(value)
    return value


def census(count, cached=0):
    return {'scope': 'afs-fd-census-v1', 'comm': 'afs-node', 'pid': 2, 'start_ticks': 100,
            'cache_root': {'path': '/owned/chunks', 'device': 9, 'inode': 2},
            'entries': [{'fd': i, 'target': ('/owned/chunks/' if i >= count-cached else '/outside/') + str(i),
                         'device': 9, 'inode': i+10, 'kind': stat.S_IFREG} for i in range(count)]}


def change_fds(node, noncache=0, cached=0):
    process = node['processes']['afs-node:2']
    process['fds'] = 7 + noncache + cached
    process['fd_census'] = census(process['fds'], cached)
    node['fds'] = process['fds'] + (node['anchor_runtime']['fds'] if node['anchor_runtime'] else 0)
    module().fd.attach(node, 'dfs')


def samples():
    values = []
    for elapsed in range(0, 3601, 15):
        nodes = {}
        for name in ('node1', 'node2', 'afs-m'):
            process = 'afs-meta:2' if name == 'afs-m' else 'afs-node:2'
            nodes[name] = dict.fromkeys(('memory_bytes', 'memory_anon_bytes', 'runtime_memory_bytes',
                                        'runtime_anon_bytes', 'disk_bytes', 'disk_data_bytes', 'log_bytes',
                                        'runtime_disk_bytes', 'runtime_backing_bytes', 'rss_bytes'), 1024**2)
            nodes[name].update(fds=10, mounts=10, free_bytes=8 * 1024**3, container_id=name,
                               storage_scope='allocated-backing-fs-v2',
                               memory_cgroup='/docker/' + name,
                               runtime_cgroup='/runtime/' + name if name != 'afs-m' else None,
                               runtime_disk_present=name != 'afs-m',
                               memory_events={'oom': 0, 'oom_kill': 0},
                               runtime_memory_events={'oom': 0, 'oom_kill': 0},
                               processes={process: {'pid': 2, 'start_ticks': 100, 'rss_bytes': 1024**2,
                                                    'fds': 10 if name == 'afs-m' else 7}})
            nodes[name]['anchor_runtime'] = ({
                'environment_id': 'a' if name == 'node1' else 'b', 'runtime_id': name + '-runc',
                'pid': 5, 'start_ticks': 500, 'rss_bytes': 1024**2, 'fds': 3,
            } if name != 'afs-m' and elapsed < 3600 else None)
            if name != 'afs-m':
                nodes[name]['fds'] = 10 if elapsed < 3600 else 7
                nodes[name]['processes'][process]['fd_census'] = census(7)
            module().fd.attach(nodes[name], 'dfs')
        churn = elapsed // 6
        values.append({'elapsed_seconds': elapsed, 'observed_monotonic_seconds': elapsed,
                       'cycles': elapsed, 'active': elapsed < 3600,
                       'operations': {'command': elapsed + churn, 'file': elapsed + churn,
                                      'create': churn + 2, 'delete': churn + (2 if elapsed == 3600 else 0)},
                       'instances': {'A': 'a', 'B': 'b'}, 'peer_reads': elapsed, 'nodes': nodes})
    return values


def workload():
    return {'status': 'passed', 'reason': 'duration-complete', 'requested_seconds': 3600,
            'elapsed_seconds': 3600, 'cycles': 3600, 'verified_bytes': 3600 * 16384,
            'operations': {'command': 4200, 'file': 4200, 'create': 602, 'delete': 602},
            'errors': [], 'cleanup_errors': [], 'remaining': [], 'instances': {'A': 'a', 'B': 'b'},
            'qualification_only': False, 'injected_failure_after_cycles': 0}


class FormalSoakContracts(unittest.TestCase):
    def setUp(self):
        self.real_workload = workload

    def work(self):
        return dict(self.real_workload(), contract_sha256=module().contract_sha(),
                    payload_transport='sdk-stage-short-copy-fsync-v2', maximum_publish_command_bytes=1000)

    def test_steady_complete_hour_passes_with_ten_five_minute_windows(self):
        policy = module()
        result = policy.evaluate(samples(), self.work(), 'dfs', policy.contract())
        self.assertEqual(result['status'], 'passed')
        self.assertEqual(len(result['windows']), 10)
        self.assertFalse(result['qualification_only'])

    def test_duration_minimum_work_counts_and_corruption_cannot_be_waived(self):
        policy = module()
        for changes in ({'elapsed_seconds': 3599}, {'cycles': 599}, {'verified_bytes': 1},
                        {'qualification_only': True}, {'errors': ['corrupt content']},
                        {'cleanup_errors': ['mount remains']}, {'injected_failure_after_cycles': 3},
                        {'operations': {'command': 4200, 'file': 4200, 'create': 602, 'delete': 601}}):
            with self.subTest(changes=changes), self.assertRaises(ValueError):
                policy.evaluate(samples(), dict(self.work(), **changes), 'dfs', policy.contract())

    def test_resource_gap_missing_metric_and_missing_window_fail(self):
        policy = module()
        missing_metric = samples()
        del missing_metric[40]['nodes']['node1']['runtime_anon_bytes']
        wrong_identity = samples()
        wrong_identity[50]['instances']['A'] = 'replacement'
        for values in (samples()[:-20], samples()[:60] + samples()[65:], missing_metric,
                       [s for s in samples() if not 1500 <= s['elapsed_seconds'] < 1800], wrong_identity):
            with self.assertRaises(ValueError):
                policy.evaluate(values, self.work(), 'dfs', policy.contract())

    def test_mild_but_continuing_fd_growth_fails_below_peak_limit(self):
        policy = module()
        values = samples()
        for value in values:
            change_fds(value['nodes']['node1'], noncache=value['elapsed_seconds'] // 300)
        with self.assertRaisesRegex(ValueError, 'trend'):
            policy.evaluate(values, self.work(), 'dfs', policy.contract())

    def test_one_oom_or_process_replacement_fails_even_if_recovered(self):
        policy = module()
        for kind in ('oom', 'process'):
            values = samples()
            if kind == 'oom':
                values[20]['nodes']['node1']['runtime_memory_events']['oom_kill'] = 1
            else:
                values[20]['nodes']['node1']['processes']['afs-node:2']['start_ticks'] = 200
            with self.assertRaises(ValueError):
                policy.evaluate(values, self.work(), 'dfs', policy.contract())

    def test_transient_peak_during_warmup_is_not_hidden_by_window_medians(self):
        policy = module()
        values = samples()
        values[2]['nodes']['node1']['memory_anon_bytes'] += 256 * 1024**2
        with self.assertRaisesRegex(ValueError, 'peak'):
            policy.evaluate(values, self.work(), 'dfs', policy.contract())

    def test_busy_start_and_finish_do_not_hide_a_quiet_middle(self):
        policy = module()
        values = samples()
        for value in values:
            if 1800 <= value['elapsed_seconds'] < 2400:
                value['cycles'] = 1790
        with self.assertRaises(ValueError):
            policy.evaluate(values, self.work(), 'dfs', policy.contract())

    def test_dfs_requires_peer_progress_in_each_steady_window(self):
        policy = module()
        values = deepcopy(samples())
        for value in values:
            value['peer_reads'] = min(value['peer_reads'], 1200)
        with self.assertRaisesRegex(ValueError, 'peer'):
            policy.evaluate(values, self.work(), 'dfs', policy.contract())

    def test_formal_cli_rejects_changed_duration_sampling_or_weakened_limits(self):
        spec = importlib.util.spec_from_file_location('formal_cli',
                                                    Path(__file__).resolve().parents[1] / 'soak.py')
        cli = importlib.util.module_from_spec(spec)
        spec.loader.exec_module(cli)
        arguments = [value for name in ('bundle', 'registry-images', 'image-export', 'artifact-commit', 'output')
                     for value in ('--' + name, '/fixed/' + name)]
        arguments += ['--mode', 'ownerfs', '--acceptance', 'one-hour']
        for extra in (['--seconds', '60'], ['--seconds', '3600', '--sample-seconds', '5'],
                      ['--seconds', '3600', '--max-fd-growth', '129'],
                      ['--seconds', '3600', '--qualify-fail-after-cycles', '3']):
            with contextlib.redirect_stderr(io.StringIO()), self.assertRaises(SystemExit):
                cli.main(arguments + extra)
        with patch.object(cli.docker_run.common, 'main', return_value=7):
            self.assertEqual(cli.main(arguments + ['--seconds', '3600']), 7)

    def test_wrong_contract_hash_and_missing_external_runtime_cgroup_fail(self):
        policy = module()
        with self.assertRaises(ValueError):
            policy.evaluate(samples(), dict(self.work(), contract_sha256='wrong'), 'dfs', policy.contract())
        values = samples()
        values[10]['nodes']['node1']['runtime_cgroup'] = None
        with self.assertRaisesRegex(ValueError, 'coverage'):
            policy.evaluate(values, self.work(), 'dfs', policy.contract())

    def test_swapped_observer_cgroup_and_unbound_final_counters_fail(self):
        policy = module()
        values = samples()
        values[10]['nodes']['node1']['runtime_cgroup'] = '/another/empty/cgroup'
        with self.assertRaisesRegex(ValueError, 'identity'):
            policy.evaluate(values, self.work(), 'dfs', policy.contract())
        values = samples()
        values[-1]['cycles'] -= 1
        with self.assertRaisesRegex(ValueError, 'coverage'):
            policy.evaluate(values, self.work(), 'dfs', policy.contract())

    def test_sampling_continues_during_cleanup_without_calling_it_active_io(self):
        policy = module()
        values = samples()
        for observed in (3615, 3630, 3645, 3660):
            terminal = deepcopy(values[-1])
            terminal['observed_monotonic_seconds'] = observed
            values.append(terminal)
        report = policy.evaluate(values, self.work(), 'dfs', policy.contract())
        self.assertEqual(report['status'], 'passed')
        self.assertEqual(len(report['windows']), 10)

    def test_same_environment_id_cannot_hide_physical_backend_replacement(self):
        policy = module()
        for field, changed in (('runtime_id', 'new-backend'), ('pid', 6), ('start_ticks', 501)):
            values = samples()
            values[30]['nodes']['node1']['anchor_runtime'][field] = changed
            with self.assertRaisesRegex(ValueError, 'physical anchor'):
                policy.evaluate(values, self.work(), 'dfs', policy.contract())

    def test_v2_changes_measurement_units_without_weakening_any_numeric_or_workload_rule(self):
        policy = module()
        original = json.loads((policy.CONTRACT_PATH.parent / 'one-hour-v1.json').read_text())
        current = json.loads((policy.CONTRACT_PATH.parent / 'one-hour-v2.json').read_text())
        current['id'] = original['id']
        current.pop('measurement')
        for group in ('peak_growth', 'trends'):
            current[group]['runtime_disk_bytes'] = current[group].pop('runtime_backing_bytes')
        self.assertEqual(current, original)

    def test_v3_only_changes_explicit_fd_measurement_and_keeps_other_rules(self):
        policy = module()
        old = json.loads((policy.CONTRACT_PATH.parent / 'one-hour-v2.json').read_text())
        current = policy.contract()
        current['id'] = old['id']
        current.pop('fd_accounting')
        current['measurement'].pop('fd_scope')
        current['measurement'].pop('fds')
        self.assertEqual(current, old)
        self.assertEqual(policy.contract()['fd_accounting']['dfs_reader_capacity_per_afs_node'], policy.fd.CAPACITY)

    def test_bounded_cache_fill_passes_while_raw_drift_is_visible(self):
        policy = module()
        values = samples()
        for value in values:
            change_fds(value['nodes']['node1'], cached=min(256, value['elapsed_seconds']//10))
        result = policy.evaluate(values, self.work(), 'dfs', policy.contract())
        self.assertGreater(result['trends']['node1']['raw_fds_diagnostic']['end_delta'], 4)
        self.assertEqual(result['trends']['node1']['fds']['end_delta'], 0)

    def test_persistent_excess_or_noncache_peak_still_fails(self):
        policy = module()
        for excess in (True, False):
            values = samples()
            for value in values:
                change_fds(value['nodes']['node1'], noncache=0 if excess else 129, cached=257 if excess else 256)
            # Preserve a genuine cold baseline, rather than accepting an already leaking baseline.
            change_fds(values[0]['nodes']['node1'])
            with self.subTest(excess=excess), self.assertRaisesRegex(ValueError, 'excess|peak'):
                policy.evaluate(values, self.work(), 'dfs', policy.contract())

    def test_ownerfs_has_zero_credit_and_original_fd_failure_remains_failure(self):
        policy = module()
        values = samples()
        for value in values:
            change_fds(value['nodes']['node1'], noncache=value['elapsed_seconds']//10)
            for node in ('node1', 'node2'):
                item = value['nodes'][node]
                item['processes']['afs-node:2']['fd_census']['cache_root'] = None
                policy.fd.attach(item, 'ownerfs')
            policy.fd.attach(value['nodes']['afs-m'], 'ownerfs')
        with self.assertRaisesRegex(ValueError, 'peak'):
            policy.evaluate(values, self.work(), 'ownerfs', policy.contract())

    def test_extra_afs_process_cannot_multiply_cache_allowance(self):
        policy = module()
        for role in ('node1', 'afs-m'):
            values = samples()
            node = values[0]['nodes'][role]
            duplicate = deepcopy(values[0]['nodes']['node1']['processes']['afs-node:2'])
            duplicate.update(pid=3, start_ticks=200)
            duplicate['fd_census'].update(pid=3, start_ticks=200)
            node['processes']['afs-node:3'] = duplicate
            node['fds'] += duplicate['fds']
            policy.fd.attach(node, 'dfs')
            with self.assertRaisesRegex(ValueError, 'owned AFS|Meta role'):
                policy.evaluate(values, self.work(), 'dfs', policy.contract())


if __name__ == '__main__':
    unittest.main()
