import importlib.util
import json
from pathlib import Path
import sys
import tempfile
import unittest
from unittest import mock


ROOT = Path(__file__).resolve().parents[1]
spec = importlib.util.spec_from_file_location('e2e_node', ROOT / 'node.py')
node = importlib.util.module_from_spec(spec)
spec.loader.exec_module(node)


class SandboxdRestartTests(unittest.TestCase):
    def test_backend_identity_can_converge_after_daemon_becomes_ready(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            (root / 'sandboxd.pid').write_text('101')
            observed = iter((['backend-1'], [], ['backend-1']))

            def terminate(pid, signum):
                self.assertEqual(pid, 101)
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
                (root / 'sandboxd.pid').write_text('202')

            with mock.patch.object(node, 'P', root), mock.patch.object(node, 'E', root), \
                 mock.patch.object(node, 'backend', side_effect=[['backend-1'], []]), \
                 mock.patch.object(node.os, 'kill', side_effect=terminate), \
                 mock.patch.object(node.time, 'monotonic', side_effect=[0, 66]), \
                 mock.patch.object(sys, 'argv', ['node.py', 'restart-sandboxd', 'node1']):
                with self.assertRaisesRegex(TimeoutError, "before=\\['backend-1'\\], after=\\[\\]"):
                    node.main()
