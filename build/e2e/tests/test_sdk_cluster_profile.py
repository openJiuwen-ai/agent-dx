"""The native SDK functional suite also accepts real cluster runtime/node IDs."""
import sys
import tempfile
import types
import unittest
from pathlib import Path
from unittest.mock import patch

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
import functional_data_plane


class AllocationReached(Exception):
    pass


class SDKClusterProfileTest(unittest.TestCase):
    def test_selected_runtime_and_nodes_reach_public_sdk(self):
        allocations = []
        capacity = {'CPU': 1000, 'Memory': 2048, 'Disk': 1024}
        nodes = [types.SimpleNamespace(
            id=name, status=0, capacity=capacity, allocatable=capacity, labels={}
        ) for name in ('worker-a', 'worker-b')]

        def create(**kwargs):
            allocations.append(kwargs)
            raise AllocationReached()

        native = types.SimpleNamespace(
            Sandbox=create, resources=lambda **_kwargs: nodes,
            CommandConflict=Exception, CommandNotFound=Exception,
            CommandStatus=object, DataPlaneSecurityPolicy=lambda **kwargs: kwargs,
        )
        with tempfile.TemporaryDirectory() as directory, patch.dict(
            sys.modules, {'adx_sandbox': native}
        ):
            with self.assertRaises(AllocationReached):
                functional_data_plane.run(
                    object(), 'fixture', Path(directory) / 'result.json',
                    Path(directory) / 'ca.pem', runtime='runsc',
                    node_ids=('worker-a', 'worker-b'),
                )
        self.assertEqual(allocations[0]['runtime'], 'runsc')
        self.assertEqual(allocations[0]['node_id'], 'worker-a')
        self.assertEqual(allocations[0]['data_plane_security'], {'port_forward_mode': 'tls-token'})


if __name__ == '__main__':
    unittest.main()
