import shutil
import stat
import subprocess
import tempfile
import unittest
from pathlib import Path


ROOT = Path(__file__).resolve().parents[3]
PACKAGED = ROOT / "build/config/examples/afs"
SOURCE = ROOT / "afs/examples"


class AfsExampleTests(unittest.TestCase):
    def test_packaged_examples_are_secure_and_match_crate_examples(self):
        # Config::resolve is exercised by afs/tests/config_contract.rs.
        # Keep this package-copy contract runnable in ADX's Python 3.9 image.
        for name, identity in (("meta", "meta"), ("node", "node-a")):
            text = (PACKAGED / f"{name}.toml").read_text()
            lines = [line.strip() for line in text.splitlines()
                     if line.strip() and not line.lstrip().startswith("#")]
            source_lines = [line.strip() for line in (SOURCE / f"{name}.toml").read_text().splitlines()
                            if line.strip() and not line.lstrip().startswith("#")]
            self.assertEqual(lines, source_lines)
            expected = {
                "fs": "all",
                "tls_ca_certificate": "/opt/adx/config/afs/tls/ca.pem",
                "tls_identity_certificate": f"/opt/adx/config/afs/tls/{identity}.pem",
                "tls_identity_private_key": f"/opt/adx/config/afs/tls/{identity}-key.pem",
                "tls_server_name": "afs-cluster",
            }
            if name == "node":
                expected.update(meta_endpoint="https://127.0.0.1:7400",
                                peer_endpoint="https://127.0.0.1:7500")
            for field, value in expected.items():
                self.assertIn(f'{field} = "{value}"', lines)
            self.assertIn('[trusted_node_certs]\nnode-a = "/opt/adx/config/afs/tls/node-a.pem"', text)

        deployment = (PACKAGED / "deployment-ownerfs-local.yaml").read_text()
        self.assertIn("with_afs: true", deployment)
        self.assertIn("/opt/adx/config/afs/meta.toml", deployment)
        self.assertIn("/opt/adx/config/afs/node.toml", deployment)

    @unittest.skipUnless(shutil.which("openssl"), "openssl is required")
    def test_local_tls_helper_creates_verified_private_material_and_refuses_overwrite(self):
        script = PACKAGED / "prepare-local-tls.sh"
        with tempfile.TemporaryDirectory() as temporary:
            destination = Path(temporary) / "tls"
            first = subprocess.run(
                [str(script), str(destination)], text=True, capture_output=True
            )
            self.assertEqual(first.returncode, 0, first.stderr)
            for name in ("ca-key.pem", "meta-key.pem", "node-a-key.pem"):
                mode = stat.S_IMODE((destination / name).stat().st_mode)
                self.assertEqual(mode, 0o600)
            for name in ("meta.pem", "node-a.pem"):
                verified = subprocess.run(
                    [
                        "openssl",
                        "verify",
                        "-CAfile",
                        str(destination / "ca.pem"),
                        str(destination / name),
                    ],
                    text=True,
                    capture_output=True,
                )
                self.assertEqual(verified.returncode, 0, verified.stderr)

            second = subprocess.run(
                [str(script), str(destination)], text=True, capture_output=True
            )
            self.assertNotEqual(second.returncode, 0)
            self.assertIn("refusing to replace", second.stderr)


if __name__ == "__main__":
    unittest.main()
