"""The sandboxd bridge address must be routable inside runc netns."""

import importlib.util
import ipaddress
from pathlib import Path
import unittest


ROOT = Path(__file__).resolve().parents[1]


class SandboxdNetworkConfigTests(unittest.TestCase):
    def test_each_node_uses_a_host_address_as_its_default_gateway(self):
        spec = importlib.util.spec_from_file_location(
            "e2e_network_config", ROOT / "network_config.py")
        module = importlib.util.module_from_spec(spec)
        spec.loader.exec_module(module)
        for node, subnet in (("node1", "10.231.16.0/20"),
                             ("node2", "10.231.32.0/20")):
            with self.subTest(node=node):
                gateway = ipaddress.ip_interface(module.sandboxd_gateway_range(node))
                self.assertEqual(str(gateway.network), subnet)
                self.assertEqual(gateway.ip, gateway.network.network_address + 1)
                self.assertNotEqual(gateway.ip, gateway.network.network_address)


if __name__ == "__main__":
    unittest.main()
