import shutil
import stat
import subprocess
import tempfile
import tomllib
import unittest
from pathlib import Path


ROOT = Path(__file__).resolve().parents[3]
PACKAGED = ROOT / "build/config/examples/afs"
SOURCE = ROOT / "afs/examples"


class AfsExampleTests(unittest.TestCase):
    def test_packaged_examples_are_secure_and_match_crate_examples(self):
        meta = tomllib.loads((PACKAGED / "meta.toml").read_text())
        node = tomllib.loads((PACKAGED / "node.toml").read_text())

        self.assertEqual(meta, tomllib.loads((SOURCE / "meta.toml").read_text()))
        self.assertEqual(node, tomllib.loads((SOURCE / "node.toml").read_text()))
        self.assertEqual(meta["fs"], "all")
        self.assertEqual(node["fs"], "all")
        self.assertTrue(node["meta_endpoint"].startswith("https://"))
        self.assertTrue(node["peer_endpoint"].startswith("https://"))
        for config in (meta, node):
            for field in (
                "tls_ca_certificate",
                "tls_identity_certificate",
                "tls_identity_private_key",
                "tls_server_name",
            ):
                self.assertTrue(config[field])
            self.assertEqual(config["trusted_node_certs"], {
                "node-a": "/opt/adx/config/afs/tls/node-a.pem"
            })

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
