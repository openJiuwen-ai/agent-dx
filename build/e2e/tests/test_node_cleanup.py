import importlib.util
from pathlib import Path
import tempfile
import unittest
from unittest.mock import patch

spec = importlib.util.spec_from_file_location('node_cleanup', Path(__file__).parents[1] / 'node.py')
node = importlib.util.module_from_spec(spec)
spec.loader.exec_module(node)


class NodeCleanupTests(unittest.TestCase):
    def test_stop_still_cleans_backend_when_metrics_probe_fails(self):
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            (root / 'sandboxd.pid').write_text('123')
            with patch.object(node, 'P', root), patch.object(node, 'E', root), \
                 patch.object(node.sys, 'argv', ['node.py', 'stop', 'node1']), \
                 patch.object(node, 'supervisor', return_value={'ok': True, 'services': []}) as supervisor, \
                 patch.object(node, 'collect'), patch.object(node, 'backend', return_value=[]), \
                 patch.object(node.os, 'kill') as kill, \
                 patch.object(node.subprocess, 'run', side_effect=AssertionError('metrics missing')):
                with self.assertRaisesRegex(AssertionError, 'metrics missing'):
                    node.main()
                supervisor.assert_called_once_with('stop')
                kill.assert_called_once_with(123, node.signal.SIGTERM)
                self.assertTrue((root / 'stop-node1.json').exists())

    def test_basic_cleanup_stops_services_and_requires_empty_backend(self):
        for remaining in ([], ['live-runtime']):
            with self.subTest(remaining=remaining), tempfile.TemporaryDirectory() as tmp:
                root = Path(tmp)
                (root / 'sandboxd.pid').write_text('123')
                with patch.object(node, 'P', root), patch.object(node, 'E', root), \
                     patch.object(node.sys, 'argv', ['node.py', 'cleanup', 'node1']), \
                     patch.object(node, 'supervisor', return_value={'ok': True}) as supervisor, \
                     patch.object(node, 'collect') as collect, \
                     patch.object(node, 'backend', return_value=remaining), \
                     patch.object(node.os, 'kill') as kill, \
                     patch.object(node.subprocess, 'run') as telemetry:
                    if remaining:
                        with self.assertRaises(AssertionError):
                            node.main()
                        kill.assert_not_called()
                        self.assertFalse((root / 'stop-node1.json').exists())
                    else:
                        node.main()
                        kill.assert_called_once_with(123, node.signal.SIGTERM)
                        self.assertTrue((root / 'stop-node1.json').exists())
                    supervisor.assert_called_once_with('stop')
                    collect.assert_called_once_with('node1')
                    telemetry.assert_not_called()
