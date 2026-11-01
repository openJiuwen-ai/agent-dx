"""Frozen one-hour acceptance must reject gaps, resource drift, OOM and partial work."""

from copy import deepcopy
import importlib.util
from pathlib import Path
import unittest
from unittest.mock import patch
import contextlib
import io


def module():
    spec = importlib.util.spec_from_file_location('soak_acceptance_tests',
                                                Path(__file__).resolve().parents[1] / 'soak_acceptance.py')
    value = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(value)
    return value


def samples():
    values = []
    for elapsed in range(0, 3601, 15):
        nodes = {}
        for name in ('node1', 'node2', 'afs-m'):
            process = 'afs-meta:2' if name == 'afs-m' else 'afs-node:2'
            nodes[name] = dict.fromkeys(('memory_bytes', 'memory_anon_bytes', 'runtime_memory_bytes',
                                        'runtime_anon_bytes', 'disk_bytes', 'disk_data_bytes', 'log_bytes',
                                        'runtime_disk_bytes', 'rss_bytes'), 1024**2)
            nodes[name].update(fds=10, mounts=10, free_bytes=8 * 1024**3, container_id=name,
                               memory_cgroup='/docker/' + name,
                               runtime_cgroup='/runtime/' + name if name != 'afs-m' else None,
                               runtime_disk_present=name != 'afs-m',
                               memory_events={'oom': 0, 'oom_kill': 0},
                               runtime_memory_events={'oom': 0, 'oom_kill': 0},
                               processes={process: {'pid': 2, 'start_ticks': 100, 'rss_bytes': 1024**2}})
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
        return dict(self.real_workload(), contract_sha256=module().contract_sha())

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
            value['nodes']['node1']['fds'] += value['elapsed_seconds'] // 300
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


if __name__ == '__main__':
    unittest.main()
