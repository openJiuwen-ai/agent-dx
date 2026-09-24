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
    def test_reconciliation_barrier_requires_stopped_runtime_and_pending_term(self):
        pending=1 << (signal.SIGTERM - 1)
        status=f'State:\tT (stopped)\nSigPnd:\t{pending:016x}\nShdPnd:\t0000000000000000\n'
        self.assertTrue(node.stopped_with_pending_signal(status,signal.SIGTERM))
        self.assertFalse(node.stopped_with_pending_signal(
            status.replace('T (stopped)','S (sleeping)'),signal.SIGTERM))
        self.assertFalse(node.stopped_with_pending_signal(
            status.replace(f'{pending:016x}','0000000000000000'),signal.SIGTERM))

    def test_reconciliation_crash_waits_for_physical_delete_signal(self):
        pending=1 << (signal.SIGTERM - 1)
        status=f'State:\tT (stopped)\nSigPnd:\t{pending:016x}\nShdPnd:\t0000000000000000\n'
        records={
            'node:node2':json.dumps({'node':{'available':False},
                                     'session':{'id':'old-session','routable':False}}),
            'environment:env-1':json.dumps({
                'spec':{'id':'env-1'},'assignment':{'node_id':'node2'},
                'invalidated':True,'result':{'state':'Failed','resources_held':False},
            }),
        }
        with tempfile.TemporaryDirectory() as directory:
            root=Path(directory)
            (root/'frozen-stale-runtime.json').write_text(json.dumps({
                'runtime_id':'runtime-1','pid':4321,'start_ticks':100,
            }))
            killed=[]
            with mock.patch.object(node,'P',root),mock.patch.object(node,'E',root), \
                 mock.patch.object(node,'catalog',return_value=records), \
                 mock.patch.object(node,'backend',return_value=['runtime-1']), \
                 mock.patch.object(node,'supervisor',return_value={
                     'services':[{'role':'adxlet','pid':1234}],
                 }),mock.patch.object(node,'process_status',return_value=status), \
                 mock.patch.object(node,'process_start_ticks',return_value=100), \
                 mock.patch.object(node.os,'kill',side_effect=lambda *args:killed.append(args)), \
                 mock.patch.object(sys,'argv',['node.py','reconcile-delete-blocked','node2']):
                node.main()
            self.assertEqual(killed,[(1234,signal.SIGKILL)])
            evidence=json.loads((root/'reconcile-delete-blocked.json').read_text())
            self.assertTrue(evidence['delete_signal_pending'])
            self.assertTrue(evidence['admission_closed'])
            self.assertEqual(evidence['failed_id'],'env-1')

    def test_reconciliation_recovery_rejects_admission_before_cleanup(self):
        records={
            'node:node2':json.dumps({'node':{'available':True},
                                     'session':{'id':'new-session','routable':True}}),
            'environment:env-1':json.dumps({
                'invalidated':True,'result':{'state':'Failed','resources_held':False},
            }),
        }
        with tempfile.TemporaryDirectory() as directory:
            root=Path(directory)
            (root/'reconcile-delete-blocked.json').write_text(json.dumps({
                'failed_id':'env-1','old_manager_pid':1234,'old_session_id':'old-session',
            }))
            with mock.patch.object(node,'P',root),mock.patch.object(node,'E',root), \
                 mock.patch.object(node,'catalog',return_value=records), \
                 mock.patch.object(node,'backend',return_value=['stale-backend']), \
                 mock.patch.object(node,'supervisor',return_value={
                     'services':[{'role':'adxlet','pid':5678}],
                 }),mock.patch.object(sys,'argv',['node.py','reconcile-recovered','node2']):
                with self.assertRaisesRegex(AssertionError,'reopened admission'):
                    node.main()

    def test_stale_runtime_thaw_does_not_signal_a_reused_pid(self):
        with tempfile.TemporaryDirectory() as directory:
            root=Path(directory)
            marker=root/'frozen-stale-runtime.json'
            marker.write_text(json.dumps({'pid':4321,'start_ticks':100}))
            with mock.patch.object(node,'P',root), \
                 mock.patch.object(node,'process_start_ticks',return_value=101), \
                 mock.patch.object(node.os,'kill') as kill:
                node.thaw_stale_runtime()
                kill.assert_not_called()
            self.assertFalse(marker.exists())
            marker.write_text(json.dumps({'pid':4321,'start_ticks':100}))
            with mock.patch.object(node,'P',root), \
                 mock.patch.object(node,'process_start_ticks',return_value=100), \
                 mock.patch.object(node.os,'kill') as kill:
                node.thaw_stale_runtime()
                kill.assert_called_once_with(4321,signal.SIGCONT)

    def test_gateway_role_restart_keeps_epoch_and_live_ownership(self):
        def snapshot(epoch, generation=7):
            return {
                'header':json.dumps({'epoch':epoch}),
                'environment:env-1':json.dumps({
                    'result':{'state':'Running','resources_held':True},
                    'assignment':{'node_id':'node1','generation':generation},
                }),
            }
        before=snapshot(4)
        self.assertEqual(node.validated_gateway_recovery(before,snapshot(4),['env-1']),
                         {'coordinator_epoch':4,
                          'ownership':{'env-1':{'node_id':'node1','generation':7}}})
        with self.assertRaises(AssertionError):
            node.validated_gateway_recovery(before,snapshot(5),['env-1'])
        with self.assertRaises(AssertionError):
            node.validated_gateway_recovery(before,snapshot(4,generation=8),['env-1'])

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
