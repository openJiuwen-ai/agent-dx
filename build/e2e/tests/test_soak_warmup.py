"""Independent warmup/measurement boundaries must not hide growth or missing work."""
from copy import deepcopy
import importlib.util
from pathlib import Path
import unittest
from unittest.mock import patch


def load(name, path):
    spec = importlib.util.spec_from_file_location(name, path)
    result = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(result)
    return result


ROOT = Path(__file__).resolve().parents[1]
BASE = load('warmup_test_base', ROOT / 'tests/test_soak_acceptance.py')
A = BASE.module()


def fixture():
    policy = A.contract('warmup-hour')
    values = []
    template = BASE.samples()[0]
    for elapsed in range(0, 7201, 15):
        sample = deepcopy(template)
        sample.update(elapsed_seconds=elapsed, observed_monotonic_seconds=elapsed,
                      cycles=elapsed, active=elapsed < 7200, peer_reads=elapsed)
        churn = elapsed // 6
        sample['operations'] = dict(command=elapsed + churn, file=elapsed + churn,
                                    create=churn + 2, delete=churn + (2 if elapsed == 7200 else 0))
        for node, observed in sample['nodes'].items():
            observed['observation_scope'] = 'persistent-smaps-v4'
            observed['process_census'] = []
            if node != 'afs-m':
                observed['processes']['afs-node:2']['fd_census']['cache_root'] = None
                for key in ('execd-service:7', 'otelcol-contrib:8', 'sandboxd:10', 'adxctl:11', 'adxlet:12',
                            *(['redis-server:13', 'adx-coordinator:14', 'adx-apiserver:15']
                              if node == 'node1' else [])):
                    observed['processes'][key] = dict(pid=int(key.split(':')[1]), start_ticks=100,
                                                      rss_bytes=1024**2, fds=3)
                if node == 'node1':
                    observed['processes']['sdk-worker:9'] = dict(pid=9, start_ticks=100,
                                                               rss_bytes=1024**2, fds=3)
                if elapsed == 7200:
                    observed['anchor_runtime'] = None
                    del observed['processes']['execd-service:7']
                    observed['processes'].pop('sdk-worker:9', None)
            for process in observed['processes'].values():
                process.update(identity={'exe': '/fixed/bin', 'exe_device': 1, 'exe_inode': process['pid'],
                                         'pid_namespace': 'pid:[1]', 'cgroup': '0::/owned\n', 'ppid': 1},
                               smaps_rollup_raw='Rss: 1024 kB\nPss_Anon: 512 kB\nSwap: 0 kB\nSwapPss: 0 kB\n',
                               pss_anon_bytes=512 * 1024, swap_bytes=0, swap_pss_bytes=0)
            if observed['anchor_runtime']:
                observed['anchor_runtime'].update({k: v for k, v in process.items()
                    if k in ('identity', 'smaps_rollup_raw', 'pss_anon_bytes', 'swap_bytes', 'swap_pss_bytes')})
                observed['anchor_runtime']['identity'] = dict(
                    observed['processes']['execd-service:7']['identity'])
                observed['processes']['execd-service:7']['identity']['ppid'] = observed['anchor_runtime']['pid']
            observed['process_census'] = [deepcopy(p) for p in observed['processes'].values()]
            if observed['anchor_runtime']:
                observed['process_census'].append(deepcopy(observed['anchor_runtime']))
            observed['fds'] = sum(p['fds'] for p in observed['processes'].values()) + (
                observed['anchor_runtime']['fds'] if observed['anchor_runtime'] else 0)
            A.fd.attach(observed, 'ownerfs')
        values.append(sample)
    work = BASE.workload()
    work.update(requested_seconds=7200, elapsed_seconds=7200, cycles=7200, verified_bytes=7200 * 16384,
                warmup_seconds=3600, measurement_seconds=3600,
                operations=dict(command=8400, file=8400, create=1202, delete=1202),
                contract_sha256=A.contract_sha(policy), payload_transport='sdk-stage-short-copy-fsync-v2',
                maximum_publish_command_bytes=1000)
    return policy, values, work


class WarmupContract(unittest.TestCase):
    def test_normal_warmup_and_full_hour_measurement(self):
        policy, values, work = fixture()
        for sample in values:
            for observed in sample['nodes'].values():
                observed['runtime_anon_bytes'] += min(sample['elapsed_seconds'], 3600) * 8000
        result = A.evaluate(values, work, 'ownerfs', policy)
        self.assertEqual(len(result['warmup_windows']), 12)
        self.assertEqual(len(result['windows']), 12)
        self.assertEqual(result['measurement_seconds'], 3600)

    def test_growth_during_measurement_is_not_excused_as_warmup(self):
        policy, values, work = fixture()
        for sample in values:
            sample['nodes']['node1']['runtime_anon_bytes'] += sample['elapsed_seconds'] * 8000
        with self.assertRaisesRegex(ValueError, 'trend exceeded'):
            A.evaluate(values, work, 'ownerfs', policy)

    def test_warmup_peak_and_empty_warmup_still_fail(self):
        for change in ('peak', 'idle'):
            policy, values, work = fixture()
            if change == 'peak':
                values[20]['nodes']['afs-m']['memory_anon_bytes'] += 129 * 1024**2
            else:
                for sample in values[1:240]:
                    sample['cycles'] = 0
            with self.assertRaises(ValueError):
                A.evaluate(values, work, 'ownerfs', policy)

    def test_actual_service_identity_and_swap_cannot_hide_growth(self):
        for change in ('identity', 'swap', 'raw'):
            policy, values, work = fixture()
            process = values[300]['nodes']['node1']['processes']['execd-service:7']
            if change == 'identity':
                process['identity']['exe_inode'] += 1
            elif change == 'swap':
                process['smaps_rollup_raw'] = process['smaps_rollup_raw'].replace('Swap: 0', 'Swap: 1')
                process['swap_bytes'] = 1024
            else:
                process['pss_anon_bytes'] += 1024
            with self.assertRaises(ValueError):
                A.evaluate(values, work, 'ownerfs', policy)

    def test_warmup_cannot_supply_measurement_work(self):
        policy, values, work = fixture()
        for sample in values:
            if sample['elapsed_seconds'] >= 3600:
                sample['cycles'] = 3600 + (sample['elapsed_seconds'] - 3600) // 10
        work['cycles'] = values[-1]['cycles']
        work['verified_bytes'] = work['cycles'] * 16384
        work['operations']['command'] = work['operations']['file'] = work['cycles'] + 1200
        values[-1]['operations'] = work['operations'].copy()
        with self.assertRaises(ValueError):
            A.evaluate(values, work, 'ownerfs', policy)

    def test_v3_contract_remains_distinct(self):
        self.assertEqual(A.contract()['id'], 'afs-continuous-1h-v3')
        self.assertEqual(A.contract()['warmup_seconds'], 600)
        self.assertEqual(A.contract()['seconds'], 3600)
        self.assertNotEqual(A.contract_sha(), A.contract_sha(A.contract('warmup-hour')))
        for key in ('peak_growth', 'trends', 'process_rss_trend', 'fd_accounting'):
            self.assertEqual(A.contract()[key], A.contract('warmup-hour')[key])

    def test_normal_retirement_is_not_an_active_worker_replacement(self):
        policy, values, work = fixture()
        runner = load('warmup_cli', ROOT / 'soak.py')
        limits = {key: policy['peak_growth'][key] for key in ('memory_bytes', 'disk_bytes', 'fds', 'mounts')}
        limits['min_free_bytes'] = policy['min_free_bytes']
        runner.validate_samples(values, limits, 15, fd_metric='fds_noncache', allow_retired_workers=True)
        del values[300]['nodes']['node1']['processes']['sdk-worker:9']
        with self.assertRaises(ValueError):
            A.evaluate(values, work, 'ownerfs', policy)

    def test_cli_freezes_mode_duration_and_sampling_without_running_a_product(self):
        runner = load('warmup_cli_admission', ROOT / 'soak.py')
        args = [value for flag in ('bundle', 'registry-images', 'image-export', 'artifact-commit', 'output')
                for value in ('--' + flag, '/not-used')]
        args += ['--mode', 'ownerfs', '--acceptance', 'warmup-hour', '--seconds', '7200']
        with patch.object(runner.docker_run.common, 'main', return_value=0) as run:
            self.assertEqual(runner.main(args), 0)
            run.assert_called_once()
        frozen_error = 'warmup-hour acceptance requires the exact frozen duration, sampling and resource bounds'
        invalid = [(['--seconds', '3600'], frozen_error),
                   (['--mode', 'dfs'], 'mode has not qualified the selected warmup contract'),
                   (['--sample-seconds', '30'], frozen_error),
                   (['--qualify-v4-observer'],
                    'v4 observer qualification is restricted to OwnerFs qualification mode')]
        for extra, message in invalid:
            with (patch.object(runner.argparse.ArgumentParser, 'error',
                               side_effect=ValueError('admission rejected')) as reject,
                  patch.object(runner.docker_run.common, 'main') as run):
                with self.assertRaisesRegex(ValueError, 'admission rejected'):
                    runner.main(args + extra)
            reject.assert_called_once_with(message)
            run.assert_not_called()
        with patch.object(runner.docker_run.common, 'main', return_value=0) as run:
            self.assertEqual(runner.main(args + ['--acceptance', 'qualification',
                                                '--qualify-v4-observer', '--seconds', '60']), 0)
            with patch.object(runner, 'scenarios') as scenarios:
                factory = run.call_args.kwargs['native_factory']
                factory.scenarios(None, [])
                self.assertIsNone(scenarios.call_args.args[-1])
                self.assertTrue(scenarios.call_args.args[2]['qualify_v4_observer'])


if __name__ == '__main__':
    unittest.main()
