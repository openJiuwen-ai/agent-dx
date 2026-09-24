import importlib.util
import json
from pathlib import Path
import signal
import sys
import tempfile
import unittest
from unittest import mock


ROOT = Path(__file__).resolve().parents[1]
spec = importlib.util.spec_from_file_location('e2e_node', ROOT / 'node.py')
node = importlib.util.module_from_spec(spec)
spec.loader.exec_module(node)


class SandboxdRestartTests(unittest.TestCase):
    def test_coordinator_restart_requires_new_epoch_and_preserved_ownership(self):
        def snapshot(epoch, *, node='node1', routable=True):
            return {
                'header': json.dumps({'epoch':epoch}),
                'environment:env-1': json.dumps({
                    'result':{'state':'Running','resources_held':True},
                    'assignment':{'node_id':node,'generation':7},
                }),
                'node:node1':json.dumps({'node':{'available':True},'session':{'routable':routable}}),
                'node:node2':json.dumps({'node':{'available':True},'session':{'routable':True}}),
            }
        before=snapshot(4)
        self.assertEqual(node.validated_coordinator_recovery(before,snapshot(5),['env-1']),
                         {'epoch_before':4,'epoch_after':5,
                          'ownership':{'env-1':{'node_id':'node1','generation':7}}})
        with self.assertRaises(AssertionError):
            node.validated_coordinator_recovery(before,snapshot(4),['env-1'])
        with self.assertRaises(AssertionError):
            node.validated_coordinator_recovery(before,snapshot(5,node='node2'),['env-1'])
        with self.assertRaises(AssertionError):
            node.validated_coordinator_recovery(before,snapshot(5,routable=False),['env-1'])

    def test_redis_recovery_evidence_requires_running_held_ownership(self):
        record={'result':{'state':'Running','resources_held':True},
                'assignment':{'node_id':'node2','generation':7}}
        records={'environment:env-1':json.dumps(record)}
        self.assertEqual(node.persisted_ownership(records,['env-1']),
                         {'env-1':{'node_id':'node2','generation':7}})
        record['result']['resources_held']=False
        with self.assertRaises(AssertionError):
            node.persisted_ownership({'environment:env-1':json.dumps(record)},['env-1'])

    def test_backend_identity_can_converge_after_daemon_becomes_ready(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            (root / 'sandboxd.pid').write_text('101')
            observed = iter((['backend-1'], [], ['backend-1']))

            def terminate(pid, signum):
                self.assertEqual(pid, 101)
                self.assertEqual(signum, signal.SIGKILL)
                (root / 'sandboxd.pid').write_text('202')

            with mock.patch.object(node, 'P', root), mock.patch.object(node, 'E', root), \
                 mock.patch.object(node, 'backend', side_effect=lambda: next(observed)), \
                 mock.patch.object(node.os, 'kill', side_effect=terminate), \
                 mock.patch.object(node.time, 'sleep'), \
                 mock.patch.object(sys, 'argv', ['node.py', 'restart-sandboxd', 'node1']):
                node.main()

            self.assertEqual(
                json.loads((root / 'sandboxd-restart-node1.json').read_text())['backend_ids_after'],
                ['backend-1'],
            )

    def test_persistent_backend_identity_loss_fails_with_observed_ids(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            (root / 'sandboxd.pid').write_text('101')

            def terminate(pid, signum):
                self.assertEqual(signum, signal.SIGKILL)
                (root / 'sandboxd.pid').write_text('202')

            with mock.patch.object(node, 'P', root), mock.patch.object(node, 'E', root), \
                 mock.patch.object(node, 'backend', side_effect=[['backend-1'], []]), \
                 mock.patch.object(node.os, 'kill', side_effect=terminate), \
                 mock.patch.object(node.time, 'monotonic', side_effect=[0, 66]), \
                 mock.patch.object(sys, 'argv', ['node.py', 'restart-sandboxd', 'node1']):
                with self.assertRaisesRegex(TimeoutError, "before=\\['backend-1'\\], after=\\[\\]"):
                    node.main()
