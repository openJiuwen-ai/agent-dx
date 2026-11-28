"""One file, two independent monotonic clocks, a prefetch that finishes late."""

import sys
import unittest
from pathlib import Path

sys.path.append(str(Path(__file__).resolve().parents[1] / "tools"))
from hixl_group_timeline import analyze


def event(name, clock_ms, **fields):
    return dict(event=name, mono_ns=clock_ms * 1_000_000, **fields)


def sample():
    home, reader = [], []
    for i in range(2):
        rid = f"read-{i}"
        identity = dict(read_id=rid, offset=i * 4, bytes=4, outcome="ready")
        home.extend(event(name, 5000 + i * 10 + delay, **identity) for name, delay in (
            ("home_source_begin", 0), ("home_source_end", 4), ("home_ready", 5)))
        identity["group_id"] = i + 1
        timeline = [("prepare_rpc_begin", 10), ("prepare_rpc_end", 16),
                    ("fetch_done", 18), ("demand_wait_start", 9 if i == 0 else 17),
                    ("demand_wait_end", 19), ("window_consumed", 25)]
        if i:
            timeline.append(("prefetch_submit", 7))
        reader.extend(event(name, i * 10 + delay, **identity) for name, delay in timeline)
    return home, reader


class TimelineTests(unittest.TestCase):
    def test_computes_delays_within_each_clock(self):
        rows = analyze(*sample(), 8)
        self.assertEqual(rows[1]["home_source_ms"], 4)
        self.assertEqual(rows[1]["submit_to_rpc_ms"], 3)
        self.assertEqual(rows[1]["ready_minus_demand_ms"], 1)
        self.assertEqual(rows[1]["ready_minus_prev_consumed_ms"], 3)

    def test_rejects_wrong_peer_or_incomplete_capture(self):
        home, reader = sample()
        home[-1]["offset"] = 0
        with self.assertRaises(ValueError):
            analyze(home, reader, 8)
        with self.assertRaises(ValueError):
            analyze(*sample(), 9)
