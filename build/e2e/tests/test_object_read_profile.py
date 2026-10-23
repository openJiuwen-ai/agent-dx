from pathlib import Path
import sys
import unittest

sys.path.append(str(Path(__file__).resolve().parents[1] / "firecracker"))
from object_read_profile import summarize


class ObjectReadProfileTests(unittest.TestCase):
    def test_overlaps_retries_and_cow_owners_do_not_double_count_unique_bytes(self):
        def row(start, stop, at=10):
            return dict(
                method="GET",
                at_unix_ns=at,
                status=206,
                path="/volume/chunks/1",
                response_bytes=stop - start,
                content_range=f"bytes {start}-{stop - 1}/100",
                complete=True,
                query=True,
            )

        rows = [row(0, 40), row(20, 60), row(0, 40), row(0, 100, at=30)]
        result = summarize(
            rows, 0, 20, {"/volume/chunks/1": ["checkpoint/memory", "runtime/memory.live"]}
        )
        self.assertEqual(result["get_bytes"], 120)
        self.assertEqual(result["unique_object_bytes"], 60)
        self.assertEqual(result["repeated_object_bytes"], 60)
        self.assertEqual(result["repeat_factor"], 2)
        self.assertEqual(result["unattributed_bytes"], 0)
        self.assertEqual(len(result["objects"]["/volume/chunks/1"]["files"]), 2)

    def test_cancelled_prefetch_preserves_sent_prefix_and_flags_delivery_uncertainty(self):
        row = dict(
            method="GET",
            at_unix_ns=1,
            status=206,
            path="/volume/chunks/1",
            response_bytes=2,
            content_range="bytes 0-3/10",
            complete=False,
        )
        result = summarize([row])
        self.assertEqual(result["get_bytes"], 2)
        self.assertEqual(result["unique_object_bytes"], 2)
        self.assertEqual(result["aborted_requests"], 1)
        self.assertIn("client-consumed", result["scope"])
        row["response_bytes"] = 4
        row["complete"] = True
        row["content_range"] = ""
        with self.assertRaisesRegex(ValueError, "Content-Range"):
            summarize([row])
        row.update(status=200, response_bytes=10)
        result = summarize([row])
        self.assertEqual(result["unattributed_bytes"], 10)
        self.assertEqual(result["unique_object_bytes"], 10)
