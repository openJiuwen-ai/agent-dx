"""Checks for startup plans and the Registry Range client."""
import hashlib
import io
import json
from pathlib import Path
import sys
import tempfile
import unittest
from unittest.mock import Mock

sys.path.insert(0, str(Path(__file__).resolve().parents[4] / "tools" / "nydus-startup"))

from prewarm import read_range, validate_plan
from sample_ranges import make_plan


class StartupRangesTest(unittest.TestCase):
    def setUp(self):
        self.manifest = {
            "config": {"digest": "sha256:" + "1" * 64, "size": 8},
            "layers": [{"digest": "sha256:" + "2" * 64, "size": 8}],
        }
        self.blob = self.manifest["layers"][0]["digest"]
        self.raw = json.dumps(self.manifest).encode()
        self.image = "sha256:" + hashlib.sha256(self.raw).hexdigest()
        event = {"kind": "body", "in_window": True, "method": "GET", "blob": self.blob}
        self.events = [
            dict(event, offset=1, length=2),
            dict(event, offset=2, length=2),
            dict(event, offset=5, length=2, in_window=False),
            dict(event, offset=6, length=2, kind="bootstrap"),
        ]

    def test_plan_only_includes_union_of_startup_body(self):
        plan = make_plan(self.image, "case", "cat /file", "train", self.events)
        self.assertEqual(plan["ranges"], [{"blob": self.blob, "offset": 1, "length": 3}])

    def test_plan_identity_and_bounds_are_checked(self):
        plan = make_plan(self.image, "case", "cat /file", "train", self.events)
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / "manifest.json"
            path.write_bytes(self.raw)
            validate_plan(path, plan, "case", "cat /file")
            with self.assertRaisesRegex(ValueError, "command"):
                validate_plan(path, plan, "case", "cat /other")
            plan["ranges"][0]["length"] = 8
            with self.assertRaisesRegex(ValueError, "range"):
                validate_plan(path, plan, "case", "cat /file")

    def test_read_range_checks_status_range_and_exact_body(self):
        headers = {
            "Content-Range": "bytes 1-3/8",
            "Content-Length": "3",
            "Docker-Content-Digest": self.blob,
        }
        path = "/v2/test/image/blobs/" + self.blob
        span = {"blob": self.blob, "offset": 1, "length": 3}
        for status, content_range, data, error in (
            (206, "bytes 1-3/8", b"abc", None),
            (200, "bytes 1-3/8", b"abc", "HTTP 200"),
            (206, "bytes 2-4/8", b"abc", "Content-Range"),
            (206, "bytes 1-3/8", b"ab", "short"),
        ):
            with self.subTest(status=status, content_range=content_range, data=data):
                response = Mock(status=status)
                response.getheader.side_effect = dict(headers, **{"Content-Range": content_range}).get
                response.read.side_effect = io.BytesIO(data).read
                connection = Mock()
                connection.getresponse.return_value = response
                if error:
                    with self.assertRaisesRegex(ValueError, error):
                        read_range(connection, path, span, 8, None)
                else:
                    self.assertEqual(read_range(connection, path, span, 8, None), hashlib.sha256(data).hexdigest())
                response.close.assert_called_once()


if __name__ == "__main__":
    unittest.main()
