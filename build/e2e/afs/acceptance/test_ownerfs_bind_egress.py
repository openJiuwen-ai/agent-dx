"""Pure configuration regression for the maintained OwnerFs bind driver."""

import importlib.util
import tempfile
import tomllib
import unittest
from pathlib import Path


DRIVER = Path(__file__).resolve().parents[1] / "scripts/ownerfs/bind-two-node-localfile.py"
SPEC = importlib.util.spec_from_file_location("ownerfs_bind_egress_driver", DRIVER)
MODULE = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(MODULE)


class OwnerFsBindEgressTests(unittest.TestCase):
    def test_bind_config_uses_a_native_root_outside_fuse_and_state(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            scenario = MODULE.Scenario(root, root / "bin", root, False)
            commands = []
            scenario.command = lambda argv, **kwargs: commands.append(argv) or ""
            scenario.write_configs()
            scenario.enable_bind_for_node_a()
            config = tomllib.loads((root / "etc/node-a.toml").read_text())
            self.assertTrue(config["experimental_ownerfs_workspace_bind"])
            egress = Path(config["ownerfs_workspace_egress_root"])
            self.assertTrue(egress.is_dir())
            self.assertFalse(egress.is_relative_to(Path(config["ownerfs_mount"])))
            self.assertFalse(egress.is_relative_to(Path(config["data_dir"])))
            self.assertEqual(config["ownerfs_workspace_bind"]["workspace"], MODULE.WORKSPACE)
            self.assertEqual(commands[-1][-1], "--print-config")
            peer = tomllib.loads((root / "etc/node-b.toml").read_text())
            self.assertNotIn("experimental_ownerfs_workspace_bind", peer)


if __name__ == "__main__":
    unittest.main()
