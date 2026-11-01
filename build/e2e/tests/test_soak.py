"""Soak admission rejects incomplete duration, stalled work and resource gaps."""

import importlib.util
from pathlib import Path
import unittest


def module():
    spec = importlib.util.spec_from_file_location('soak_contract', Path(__file__).resolve().parents[1] / 'soak.py')
    value = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(value)
    return value


class SoakContracts(unittest.TestCase):
    def test_duration_is_explicit_bounded_and_separate_from_targeted_suite(self):
        value = module()
        for seconds in (0, -1, 86401):
            with self.assertRaises(ValueError):
                value.validate_options({'seconds': seconds, 'sample_seconds': 5})
        value.validate_options({'seconds': 86400, 'sample_seconds': 30})

    def test_progress_duration_cleanup_and_errors_are_all_required(self):
        value = module()
        report = {'status': 'passed', 'elapsed_seconds': 60, 'requested_seconds': 60,
                  'cycles': 20, 'operations': {'command': 20, 'file': 20, 'create': 5, 'delete': 5},
                  'errors': [], 'cleanup_errors': [], 'instances': {'A': 'a', 'B': 'b'},
                  'remaining': [], 'reason': 'duration-complete'}
        value.validate_workload(report, 60)
        for change in ({'elapsed_seconds': 59}, {'requested_seconds': 59}, {'cycles': 0},
                       {'elapsed_seconds': float('nan')}, {'elapsed_seconds': float('inf')},
                       {'errors': ['corruption']}, {'cleanup_errors': ['env remains']},
                       {'reason': 'cancelled'}, {'remaining': ['a']},
                       {'operations': {'command': 20, 'file': 20, 'create': 5, 'delete': 4}}):
            with self.assertRaises(ValueError):
                value.validate_workload(dict(report, **change), 60)

    def test_missing_samples_and_growth_cannot_be_reported_as_qualified(self):
        value = module()
        limits = {'memory_bytes': 100, 'disk_bytes': 100, 'fds': 2, 'mounts': 1, 'min_free_bytes': 100}
        def sample(memory=10, elapsed=0, cycles=1):
            return {'elapsed_seconds': elapsed, 'cycles': cycles, 'nodes': {
                n: {'memory_bytes': memory, 'disk_bytes': 10, 'fds': 2, 'mounts': 1, 'free_bytes': 1000,
                    'processes': {'afs': {'pid': 2, 'start_ticks': 123, 'rss_bytes': 100}}}
                for n in ('node1', 'node2', 'afs-m')}}
        samples = [sample(), sample(elapsed=5, cycles=2)]
        value.validate_samples(samples, limits, 5)
        for bad in ([samples[0]], [samples[0], sample(memory=111, elapsed=5, cycles=2)],
                    [samples[0], sample(elapsed=100, cycles=2)],
                    [samples[0], sample(elapsed=5, cycles=1)]):
            with self.assertRaises(ValueError):
                value.validate_samples(bad, limits, 5)

    def test_stale_worker_clock_does_not_hide_a_stall_from_observer(self):
        value = module()
        limits = {'memory_bytes': 100, 'disk_bytes': 100, 'fds': 2, 'mounts': 1, 'min_free_bytes': 100}
        node = {'memory_bytes': 0, 'disk_bytes': 0, 'fds': 0, 'mounts': 0, 'free_bytes': 1000,
                'processes': {'afs': {'pid': 2, 'start_ticks': 123, 'rss_bytes': 100}}}
        samples = [{'elapsed_seconds': t, 'cycles': c, 'observed_monotonic_seconds': observed,
                    'nodes': {n: node for n in ('node1', 'node2', 'afs-m')}}
                   for t, c, observed in ((1, 1, 1), (2, 2, 2), (2, 2, 20), (2, 2, 34))]
        with self.assertRaisesRegex(ValueError, 'stalled'):
            value.validate_samples(samples, limits, 5)


if __name__ == '__main__':
    unittest.main()
