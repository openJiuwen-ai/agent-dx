"""Exercise the FabricMem drain gate with synthetic SSH snapshots and time."""

from contextlib import ExitStack
import json
from pathlib import Path
import subprocess
import sys
import tempfile
import unittest
from unittest.mock import patch

sys.path.append(str(Path(__file__).resolve().parents[1] / "tools"))
import a3_fabricmem as fabricmem

DRAIN_ERROR = "HIXL slots did not drain within 45 seconds"


class FakeClock:
    def __init__(self):
        self.now = 100.0
        self.sleeps = []

    def monotonic(self):
        return self.now

    def sleep(self, seconds):
        self.sleeps.append(seconds)
        self.now += seconds


def snapshot(slots):
    runtime = {"receive_slots_in_use": slots, "export_slots_in_use": 0, "lanes": []}
    return {"health": {"checks": {"hixl": runtime}}, "metrics": ""}


class FabricMemDrainTests(unittest.TestCase):
    def setUp(self):
        self.clock = FakeClock()
        self.timeouts = []
        self.hosts = {
            role: {
                "id": role, "mount": "/mount", "rest": "rest", "ssh_target": role,
                "device_ids": [0],
            }
            for role in ("a", "b")
        }
        self.inventory = {role: {"boot_id": role} for role in self.hosts}
        stack = ExitStack()
        self.addCleanup(stack.close)
        self.output = Path(stack.enter_context(tempfile.TemporaryDirectory()))
        self.assess = stack.enter_context(patch.object(fabricmem, "assess", return_value={}))
        stack.enter_context(patch.object(fabricmem, "time", self.clock))
        stack.enter_context(patch.object(
            fabricmem, "health",
            side_effect=lambda host, value: (host["id"], "mount-id", None, "domain", [host["id"]]),
        ))
        stack.enter_context(patch.object(fabricmem, "counters", return_value={}))
        self.drain_steps = iter(())
        self.running = False
        self.passed = self.output / "a-home-1" / "passed.json"
        stack.enter_context(patch.object(fabricmem.subprocess, "run", side_effect=self.run_ssh))

    def run_ssh(self, argv, *, timeout, **_kwargs):
        action = argv[-1].split()[3]
        if action == "snapshot" and self.running:
            self.timeouts.append((argv[-2], timeout))
            delay, slots = next(self.drain_steps, (0, 0))
            if slots is None:
                self.clock.now += timeout
                raise subprocess.TimeoutExpired(argv, timeout)
            self.clock.now += delay
            # Allow a late result to model a subprocess that returns after its budget.
            value = snapshot(slots)
        elif action == "snapshot":
            value = snapshot(0)
        elif action == "work":
            value = [{"index": 0, "pid": 100, "target_sha256": "target"}]
        elif action == "verify":
            self.running = True
            value = [{"index": 0, "sha256": "target"}]
        else:
            self.running = False
            value = {}
        return subprocess.CompletedProcess(argv, 0, json.dumps(value).encode(), b"")

    def verify(self, *steps):
        self.drain_steps = iter(steps)
        fabricmem.verify(self.hosts, self.inventory, (1,), self.output, "test")

    def assert_drain_fails(self, *steps):
        with self.assertRaisesRegex(ValueError, DRAIN_ERROR):
            self.verify(*steps)
        self.assertFalse(self.passed.exists())
        self.assess.assert_not_called()

    def test_timely_idle_retry_uses_shrinking_ssh_budgets(self):
        self.verify((18, 1), (18, 0), (2, 0), (5, 0))
        self.assertTrue(self.passed.is_file())
        self.assertEqual(self.clock.sleeps, [1])
        self.assertEqual(self.timeouts[:4], [("a", 20), ("b", 20), ("a", 8), ("b", 6)])

    def test_rejects_idle_pair_returned_after_deadline(self):
        self.assert_drain_fails((18, 1), (18, 0), (2, 0), (7, 0))
        self.assertEqual(self.timeouts, [("a", 20), ("b", 20), ("a", 8), ("b", 6)])

    def test_does_not_start_second_ssh_after_deadline(self):
        self.assert_drain_fails((18, 1), (18, 0), (8, 0))
        self.assertEqual(self.timeouts, [("a", 20), ("b", 20), ("a", 8)])

    def test_bounds_retry_sleep_and_does_not_poll_at_deadline(self):
        self.assert_drain_fails((18, 1), (18, 0), (2, 1), (5.5, 0))
        self.assertEqual(self.clock.sleeps, [1, 0.5])
        self.assertEqual(self.timeouts, [("a", 20), ("b", 20), ("a", 8), ("b", 6)])

    def test_rejects_ssh_timeout_on_remaining_budget(self):
        self.assert_drain_fails((18, 1), (18, 0), (2, 0), (0, None))
        self.assertEqual(self.timeouts, [("a", 20), ("b", 20), ("a", 8), ("b", 6)])
        self.assertEqual(self.clock.now, 145)
