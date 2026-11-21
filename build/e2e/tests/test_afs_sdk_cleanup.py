"""Independent SDK failures may reuse only a verified live runtime."""

from contextlib import contextmanager
import json
from pathlib import Path
import sys
import tempfile
import unittest
from unittest.mock import Mock, call, patch

E2E = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(E2E / 'kubernetes'))
sys.path.insert(0, str(E2E.parents[1] / 'afs/tests'))
from afs_product import Product


CLIENT = {'build_id': 'build', 'commit': 'commit', 'version': '0.1.0rc9', 'wheel_sha256': 'digest', 'site': '/sdk'}
FAILED = {
    'status': 'failed', 'mode': 'ownerfs', 'full': True, 'error': 'AssertionError: changed SDK bytes',
    'cleanup_errors': [], 'cases': [{'id': 'environment-cleanup', 'status': 'passed'}], 'sdk_client': CLIENT,
}


class SdkCleanupTests(unittest.TestCase):
    def run_modes(self, output, owner_report, *, stale_stop=False, afs_cleanup=None):
        run = Mock(output=output, nodes=('node1', 'node2'))
        attempted = []
        clusters = []

        @contextmanager
        def case(name, checks):
            attempted.append(name)
            yield {}

        def execute(node, *args, **kwargs):
            if args[0] == 'cat':
                remote = args[1]
                if remote.endswith('-ownerfs.json'):
                    return json.dumps(owner_report)
                if remote.endswith('-dfs.json'):
                    return json.dumps({**FAILED, 'mode': 'dfs'})
                return '{}'
            if '--output' in args and not args[-1].endswith('-warm.json'):
                raise RuntimeError('SDK workload failed')
            if args[0] == 'test' and node == 'node2' and stale_stop:
                raise RuntimeError('previous runtime already stopped')
            return ''

        def make_cluster(*args):
            cluster = Mock()
            cluster.workspace.side_effect = lambda role: '/a/' + role
            cluster.python.return_value = {'stdout': '[1, 2]'}
            cluster.cleanup.return_value = afs_cleanup or {'status': 'PASS', 'errors': []}
            clusters.append(cluster)
            return cluster

        run.case.side_effect = case
        run.execute.side_effect = execute
        product = Product(run, full=True)
        with (patch('afs_product.stage_client', return_value=CLIENT),
              patch('fixtures.multivm.Cluster', side_effect=make_cluster),
              patch('suites.operations.peer_counter', return_value=0),
              patch.object(product, 'copy_from', side_effect=lambda node, source, path: path.touch())):
            with self.assertRaises((RuntimeError, ValueError)) as error:
                product.scenarios([])
        return attempted, run, product, str(error.exception), clusters

    def test_both_workloads_fail_independently_after_clean_runtime_proof(self):
        with tempfile.TemporaryDirectory() as directory:
            output = Path(directory)
            attempted, run, product, failure, clusters = self.run_modes(output, FAILED)
            self.assertEqual(attempted, ['afs-sdk-ownerfs-full', 'afs-sdk-dfs-full'])
            self.assertEqual(failure, 'independent SDK cases failed: ownerfs: SDK workload failed; '
                                      'dfs: SDK workload failed')
            self.assertEqual(run.helper.call_args_list, [call(node, 'empty', node)
                                                         for node in ('node1', 'node2')] * 3)
            for mode, cluster in zip(('ownerfs', 'dfs'), clusters):
                cluster.cleanup.assert_called_once()
                report = json.loads((output / 'afs-sdk' / mode / 'sdk-result.json').read_text())
                self.assertEqual(report['error'], FAILED['error'])
            self.assertEqual(product.cleanup_errors, [])

    def test_failed_or_missing_sdk_cleanup_evidence_retires_the_runtime(self):
        reports = ({**FAILED, 'cases': []}, {**FAILED, 'cleanup_errors': ['leftover']},
                   {**FAILED, 'sdk_client': {**CLIENT, 'version': 'wrong'}})
        for report in reports:
            with self.subTest(report=report), tempfile.TemporaryDirectory() as directory:
                attempted, run, _, failure, _ = self.run_modes(Path(directory), report)
                self.assertEqual(attempted, ['afs-sdk-ownerfs-full'])
                self.assertEqual(failure, 'SDK workload failed')
                self.assertEqual(run.helper.call_args_list[2:], [call(node, 'cleanup', node, timeout=180)
                                                                 for node in ('node2', 'node1')])

    def test_stopped_runtime_cannot_pass_the_live_empty_probe(self):
        with tempfile.TemporaryDirectory() as directory:
            attempted, run, _, failure, _ = self.run_modes(Path(directory), FAILED, stale_stop=True)
            self.assertEqual(attempted, ['afs-sdk-ownerfs-full'])
            self.assertEqual(failure, 'SDK workload failed')
            self.assertIn(call('node2', 'test', '!', '-e', '/evidence/stop-node2.json'), run.execute.call_args_list)
            self.assertEqual(run.helper.call_args_list[2:], [call('node1', 'empty', 'node1'),
                                                             call('node2', 'cleanup', 'node2', timeout=180),
                                                             call('node1', 'cleanup', 'node1', timeout=180)])

    def test_afs_cleanup_failure_prevents_the_next_mode(self):
        with tempfile.TemporaryDirectory() as directory:
            cleanup = {'status': 'FAIL', 'errors': ['mount held']}
            attempted, _, product, failure, _ = self.run_modes(Path(directory), FAILED, afs_cleanup=cleanup)
            self.assertEqual(attempted, ['afs-sdk-ownerfs-full'])
            self.assertEqual(failure, 'mount held')
            self.assertEqual(product.cleanup_errors, ['mount held'])
