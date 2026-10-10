import json
import os
from pathlib import Path
import tempfile
import unittest

import vm_capacity_manifest as volume


class VolumeManifestTests(unittest.TestCase):
    def test_preserves_bytes_permissions_xattr_and_dangling_symlink(self):
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            data = root / "data"
            data.write_bytes(b"durable data")
            data.chmod(0o640)
            os.setxattr(data, "user.identity", b"original")
            (root / "link").symlink_to("absent")
            before = volume.manifest(root)
            after = volume.manifest(root)
            self.assertEqual(volume.compare(before, after)["status"], "PASS")
            self.assertEqual(before["data"]["mode"], 0o640)
            self.assertEqual(before["data"]["xattrs"]["user.identity"], b"original".hex())
            self.assertEqual(before["link"]["target"], "absent")
            self.assertEqual(json.loads(json.dumps(before)), before)

    def test_rejects_content_or_permission_loss(self):
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            data = root / "data"
            data.write_bytes(b"before")
            before = volume.manifest(root)
            data.write_bytes(b"after!")
            data.chmod(0o600)
            result = volume.compare(before, volume.manifest(root))
            self.assertEqual(result["status"], "FAIL")
            self.assertIn("data", result["changed"])

    def test_does_not_follow_symlink_outside_volume(self):
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            (root / "outside").symlink_to("/etc")
            self.assertEqual(set(volume.manifest(root)), {".", "outside"})


if __name__ == "__main__":
    unittest.main()
