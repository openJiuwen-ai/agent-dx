"""Unit guards for generation-aware DFS R3 write observation helpers."""
import copy
import json
from pathlib import Path
import tempfile
from types import SimpleNamespace
import unittest
from unittest.mock import patch

import dfs_r3_write_small as probe


class WriteSmallGuards(unittest.TestCase):
    def setUp(self):
        self.identity = dict(product_source_commit='7' * 40, compiler_input_map='1' * 64,
                             afs_meta_sha256='2' * 64, afs_node_sha256='3' * 64,
                             io_sha256='4' * 64)

    def io_result(self, generation=1, operation='seq-write', barrier='fdatasync', wall_ns=1_000_000_000):
        return dict(dataset=probe.DATASET, generation=generation, operation=operation,
                    file_bytes=64 * 2**20, io_bytes=64 * 2**20, block_bytes=2**20,
                    concurrency=1, pattern_byte=97, operations=64, barrier=barrier,
                    cache_requested='unobserved', residency_observed=False,
                    content_ok=True, wall_ns=wall_ns, client_cpu_ns=1, barrier_ns=1)

    def manifest(self, index, **updates):
        generation = index + 1
        result = self.io_result(generation=generation, wall_ns=(index + 1) * 1_000_000_000)
        value = dict(role='write-round', status='DATA_RECORDED', identity=self.identity,
                     relative_path=probe.relative_path(index), generation=generation, round=index,
                     measured=index > 0, content=probe.expected_content(generation),
                     rawargv=['/tool', 'write', '/payload', str(generation)], rc=0,
                     stdout=json.dumps(result) + '\n', stderr='', result=result,
                     parent_dir_fsync=True, root_dir_fsync=True,
                     content_verify=dict(bytes=64 * 2**20,
                                         sha256=probe.expected_content(generation)['sha256'],
                                         eof=dict(status='PASS', offset=64 * 2**20, extra_bytes=0),
                                         status='PASS'))
        value.update(updates)
        return value

    def manifests(self):
        return [self.manifest(index) for index in range(6)]

    def test_generation_distinct_across_sixteen_chunks_and_six_rounds(self):
        all_chunks = []
        full = []
        for generation in range(1, 7):
            shape = probe.expected_content(generation)
            self.assertEqual(shape['bytes'], 64 * 2**20)
            self.assertEqual(shape['dataset'], probe.DATASET)
            self.assertEqual(shape['generation'], generation)
            self.assertEqual(len(set(shape['chunk_sha256'])), 16)
            all_chunks.extend(shape['chunk_sha256'])
            full.append(shape['sha256'])
        self.assertEqual(len(set(all_chunks)), 6 * 16)
        self.assertEqual(len(set(full)), 6)

    def test_malformed_round_generation_and_identity_rejected(self):
        for bad in (-1, 6, True, None, '1'):
            with self.subTest(round=bad), self.assertRaises(ValueError):
                probe.validate_round_index(bad)
        for bad in (0, 7, False, None, '1'):
            with self.subTest(generation=bad), self.assertRaises(ValueError):
                probe.validate_generation(bad)
        with self.assertRaisesRegex(ValueError, 'identity|invalid candidate'):
            probe.validate_write_manifest(self.manifest(0, identity={}), self.identity)

    def test_failed_corrupt_or_false_timer_cannot_score(self):
        cases = []
        failed = self.manifests(); failed[2]['status'] = 'FAIL'; cases.append(failed)
        corrupt = self.manifests(); corrupt[3]['content_verify']['sha256'] = 'bad'; cases.append(corrupt)
        false_timer = self.manifests(); false_timer[4]['result']['wall_ns'] = True; cases.append(false_timer)
        bad_stdout = self.manifests(); bad_stdout[1]['stdout'] = json.dumps(dict(bad_stdout[1]['result'], generation=99)); cases.append(bad_stdout)
        missing_id = self.manifests(); missing_id[0]['identity'] = None; cases.append(missing_id)
        bool_generation = self.manifests(); bool_generation[0]['generation'] = True; cases.append(bool_generation)
        wrong_cli = self.manifests(); wrong_cli[0]['rawargv'][3] = '6'; cases.append(wrong_cli)
        for manifests in cases:
            with self.subTest(case=manifests), self.assertRaises(ValueError):
                probe.write_timings(manifests)

    def test_six_round_index_generation_warmup_and_manifest_match(self):
        result = probe.write_timings(self.manifests())
        self.assertEqual(result['writes_checked'], 6)
        self.assertEqual(result['warmup_rounds'], 1)
        self.assertEqual(result['measured_rounds'], 5)
        self.assertEqual(result['mib_per_second'], [32, 64/3, 16, 12.8, 64/6])
        self.assertEqual(result['median_mib_per_second'], 16)
        self.assertFalse(result['qualified_threefs_parity'])
        bad = self.manifests(); bad[2]['generation'] = 6
        with self.assertRaises(ValueError):
            probe.write_timings(bad)
        bad = self.manifests(); bad[0]['measured'] = True
        with self.assertRaises(ValueError):
            probe.write_timings(bad)
        bad = self.manifests(); bad[4]['relative_path'] = bad[3]['relative_path']
        with self.assertRaises(ValueError):
            probe.write_timings(bad)

    def test_write_round_uses_generation_path_and_verifies_content(self):
        args = SimpleNamespace(round=2, round_timeout=60)
        sample = dict(status='PASS', rawargv=['/tool', 'write', '/payload', '3'], rc=0, stdout='{}', stderr='',
                      result=self.io_result(generation=3))
        verified = dict(status='PASS')
        with tempfile.TemporaryDirectory() as directory:
            out = Path(directory)
            root = out / 'mnt'; root.mkdir()
            with patch.object(probe.base, 'prepare', return_value=(out, root, root/'unused', out/'io', self.identity)), \
                 patch.object(probe, 'run_sample', return_value=sample) as run, \
                 patch.object(probe.sync, 'fsync_directory') as fsync, \
                 patch.object(probe, 'verify_content', return_value=verified):
                result = probe.write_round(args)
        self.assertEqual(result['status'], 'DATA_RECORDED')
        self.assertEqual(result['relative_path'], 'r3-current-counter/round-02.bin')
        self.assertEqual(result['generation'], 3)
        run.assert_called_once_with(out/'io', root/'r3-current-counter/round-02.bin', 'write', 3, 60)
        self.assertEqual(fsync.call_count, 2)

    def test_failed_post_write_verification_cannot_report_pass(self):
        args = SimpleNamespace(round=0, round_timeout=60)
        with tempfile.TemporaryDirectory() as directory:
            out = Path(directory); root = out / 'mnt'; root.mkdir()
            with patch.object(probe.base, 'prepare', return_value=(out, root, root/'unused', out/'io', self.identity)), \
                 patch.object(probe, 'run_sample', return_value=dict(status='PASS')), \
                 patch.object(probe.sync, 'fsync_directory', side_effect=OSError('fsync failed')):
                result = probe.write_round(args)
        self.assertEqual(result['status'], 'BLOCKED')
        self.assertIn('fsync failed', result['error'])

    def test_read_check_uses_manifest_generation_and_never_claims_performance(self):
        manifest = self.manifest(4)
        args = SimpleNamespace(manifest='/manifest.json', round_timeout=60)
        sample = dict(status='PASS', rawargv=['/tool', 'read', '/payload', '5'], rc=0, stdout='{}', stderr='',
                      result=self.io_result(generation=5, operation='seq-read', barrier='close'))
        verified = dict(status='PASS')
        with tempfile.TemporaryDirectory() as directory:
            out = Path(directory)
            root = out / 'mnt'; root.mkdir()
            with patch.object(probe.base, 'prepare', return_value=(out, root, root/'unused', out/'io', self.identity)), \
                 patch.object(probe.sync, 'read_json', return_value=manifest), \
                 patch.object(probe, 'run_sample', return_value=sample) as run, \
                 patch.object(probe, 'verify_content', return_value=verified):
                result = probe.read_check(args)
        self.assertEqual(result['status'], 'DATA_RECORDED')
        self.assertFalse(result['performance_claim'])
        run.assert_called_once_with(out/'io', root/'r3-current-counter/round-04.bin', 'read', 5, 60)


if __name__ == '__main__':
    unittest.main()
