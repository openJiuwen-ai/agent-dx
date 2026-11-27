"""Disk-backed regression tests for the Linux A3 capacity worker."""

from contextlib import ExitStack
import hashlib
import os
from pathlib import Path
import sys
from tempfile import TemporaryDirectory
from types import SimpleNamespace
import unittest
from unittest.mock import Mock, patch

sys.path.append(str(Path(__file__).resolve().parents[1] / "tools"))
import a3_fabricmem_capacity as capacity

WINDOW = 256
GROUP = 8 * WINDOW
GROUPS = 3


@unittest.skipUnless(
    hasattr(os, "readv") and hasattr(os, "O_NOFOLLOW"), "requires Linux file I/O"
)
class CapacityTests(unittest.TestCase):
    def setUp(self):
        stack = ExitStack()
        self.addCleanup(stack.close)
        root = Path(stack.enter_context(TemporaryDirectory()))
        self.directory = root / "afs-cap-123456abcdef"
        self.directory.mkdir()
        self.filename = self.directory / "worker-00.bin"
        self.barrier = Mock()
        stack.enter_context(
            patch.multiple(capacity, WINDOW=WINDOW, GROUP=GROUP, GROUPS=GROUPS)
        )
        stack.enter_context(patch.object(capacity, "gate", self.barrier, create=True))

    def run_action(self, action):
        row = capacity.capacity_one((self.directory, action, 0))
        contents = self.filename.read_bytes()
        self.assertEqual(row["bytes"], len(contents))
        self.assertEqual(row["bytes"], GROUP * GROUPS)
        self.assertEqual(row["sha256"], hashlib.sha256(contents).hexdigest())
        self.assertEqual(len(row["groups"]), GROUPS)
        return contents

    def test_seed_read_write_verify_on_disk(self):
        original = self.run_action("seed")
        self.assertEqual(self.run_action("read"), original)
        updated = self.run_action("write")
        self.assertEqual(self.run_action("verify"), updated)
        windows = {
            data[start : start + WINDOW]
            for data in (original, updated)
            for start in range(0, len(data), WINDOW)
        }
        self.assertEqual(len(windows), 2 * GROUPS * GROUP // WINDOW)
        self.assertEqual(self.barrier.wait.call_count, 2)

    def test_read_and_verify_reject_wrong_offsets_at_the_correct_length(self):
        original = self.run_action("seed")
        updated = self.run_action("write")
        for action, data in (("read", original), ("verify", updated)):
            first, second, third = (
                data[start : start + GROUP] for start in range(0, len(data), GROUP)
            )
            faults = {
                "repeated group": first + first + third,
                "reordered groups": second + first + third,
                "reordered windows": data[WINDOW : 2 * WINDOW]
                + data[:WINDOW]
                + data[2 * WINDOW :],
                "one-byte misalignment": first + second[1:] + second[:1] + third,
            }
            for fault, corrupted in faults.items():
                with self.subTest(action=action, fault=fault):
                    self.assertEqual(len(corrupted), len(data))
                    self.filename.write_bytes(corrupted)
                    with self.assertRaisesRegex(
                        ValueError, f"digest mismatch on {action}"
                    ):
                        capacity.capacity_one((self.directory, action, 0))

    def test_measured_actions_keep_payload_work_outside_io_timings(self):
        self.run_action("seed")
        for action in ("read", "write"):
            with self.subTest(action=action):
                self.assert_measurement_boundaries(action)

    def assert_measurement_boundaries(self, action):
        events = []

        def clock():
            events.append("clock")
            return len(events)

        def record(name, operation):
            def invoke(*args, **kwargs):
                events.append(name)
                return operation(*args, **kwargs)

            return invoke

        with (
            patch.object(capacity, "time", SimpleNamespace(monotonic_ns=clock)),
            patch.object(
                self.barrier,
                "wait",
                side_effect=lambda **_: events.append("gate"),
            ),
            patch.object(
                capacity,
                "payload",
                side_effect=record("payload", capacity.payload),
            ),
            patch.object(os, "fsync", side_effect=record("fsync", os.fsync)),
        ):
            row = capacity.capacity_one((self.directory, action, 0))

        gate = events.index("gate")
        self.assertIn("payload", events[:gate])
        self.assertNotIn("payload", events[gate:])
        self.assertEqual(row["started_ns"], gate + 2)
        self.assertLess(row["started_ns"], row["opened_ns"])
        previous = row["opened_ns"]
        for group in row["groups"]:
            self.assertLess(previous, group["begin_ns"])
            self.assertLess(group["begin_ns"], group["end_ns"])
            inside = events[group["begin_ns"] : group["end_ns"] - 1]
            self.assertEqual(inside.count("fsync"), int(action == "write"))
            previous = group["end_ns"]
        self.assertLess(previous, row["ended_ns"])
