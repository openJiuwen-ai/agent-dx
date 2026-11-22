"""Regression contracts for the !283 review: stripe coverage, per-phase read paths, counter
validity and whole-round OOM coverage. Tool behaviour only; no product or performance claim.
"""

import json
import sys
import unittest
from pathlib import Path
from posixpath import join
from unittest.mock import patch

TESTS = Path(__file__).resolve().parents[1]
sys.path.append(str(TESTS))
from tools import benchlib  # noqa: E402

MIB = 1024**2
SEND = 'afs_dfs_payload_bytes_total{direction="send",operation="read",transport="grpc"}'
RECV = 'afs_dfs_crc32c_read_bytes_total{stage="trusted_segments_receive"}'


def bench(**extra):
    value = {"file_bytes": 64 * MIB, "block_bytes": [MIB], "threads": [1], "rounds": 3,
             "tools": {"fio": "/opt/bench/fio", "elbencho": "/opt/bench/elbencho"}}
    value.update(extra)
    return {"bench": value}


def load_suite():
    import importlib.util

    spec = importlib.util.spec_from_file_location("bench_suite_review", TESTS / "suites/bench/test_bench.py")
    suite = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(suite)
    return suite


def chunks(role, *entries, resident=0):
    """A probe `resident` result: (chunk id, bytes) bodies under the role's state dir."""
    files = []
    for chunk_id, size in entries:
        files.append({"path": join("/r", role, "state", "dfs", "chunks", chunk_id),
                      "bytes": size, "resident_bytes": resident})
    return {"files": files, "bytes": sum(f["bytes"] for f in files),
            "resident_bytes": sum(f["resident_bytes"] for f in files), "errors": [], "page_size": 4096}


def run_evict(results, file_bytes, baseline=None, source="A"):
    suite = load_suite()

    def probe(_c, role, action, *_paths):
        return {"files": 0, "bytes": 0, "errors": []} if action == "evict" else results[role]

    class Cluster:
        @staticmethod
        def role_dir(role):
            return join("/r", role)

    with patch.object(suite, "probe", side_effect=probe):
        return suite.evict_backends(Cluster(), list(results), file_bytes, source, baseline=baseline)


class StripeCoverageTests(unittest.TestCase):
    """Issue 1: coverage by this round's distinct chunks, not by summed bytes."""

    def test_one_stripe_held_twice_does_not_cover_a_two_stripe_file(self):
        evidence, cold = run_evict({"A": chunks("A", ("x", MIB)), "B": chunks("B", ("x", MIB))}, 2 * MIB,
                                   baseline={"A": {}, "B": {}})
        placement = evidence["placement"]
        self.assertEqual(placement["coverage"], "unverified")
        self.assertEqual((placement["distinct_bytes"], placement["replica_bytes"]), (MIB, MIB))
        self.assertFalse(cold)

    def test_stale_chunks_from_earlier_files_are_not_counted(self):
        evidence, cold = run_evict({"A": chunks("A", ("old", 2 * MIB), ("a", MIB)), "B": chunks("B", ("b", MIB))},
                                   2 * MIB, baseline={"A": {"old": 2 * MIB}, "B": {}})
        placement = evidence["placement"]
        self.assertEqual(placement["coverage"], "verified")
        self.assertEqual(placement["by_role"], {"A": MIB, "B": MIB})
        self.assertEqual(placement["stale_by_role"], {"A": 2 * MIB})
        self.assertEqual(placement["replica_bytes"], 0)
        self.assertTrue(cold)

    def test_stale_residue_on_the_source_cannot_stand_in_for_its_share(self):
        # A holds 2 MiB of an earlier file and 1 MiB of this one: summed bytes would say "A holds it all".
        evidence, cold = run_evict({"A": chunks("A", ("old", 2 * MIB), ("a", MIB)), "B": chunks("B")}, 2 * MIB,
                                   baseline={"A": {"old": 2 * MIB}, "B": {}})
        self.assertEqual(evidence["placement"]["coverage"], "unverified")
        self.assertFalse(cold)

    def test_replicated_chunks_count_once_and_their_copies_separately(self):
        evidence, cold = run_evict({"A": chunks("A", ("a", MIB), ("b", MIB)), "B": chunks("B", ("a", MIB), ("b", MIB)),
                                    "C": chunks("C", ("a", MIB))}, 2 * MIB, baseline={"A": {}, "B": {}, "C": {}})
        placement = evidence["placement"]
        self.assertEqual((placement["coverage"], placement["chunks"]), ("verified", 2))
        self.assertEqual((placement["distinct_bytes"], placement["replica_bytes"]), (2 * MIB, 3 * MIB))
        self.assertTrue(cold)

    def test_default_single_copy_source_short_of_the_file_is_layout_drift(self):
        with self.assertRaises(AssertionError) as raised:
            run_evict({"A": chunks("A", ("a", MIB)), "B": chunks("B", ("b", MIB))}, 2 * MIB)
        self.assertIn("payload layout not recognised on A", str(raised.exception))

    def test_default_full_source_evidence_has_no_placement(self):
        evidence, _ = run_evict({"A": chunks("A", ("a", 2 * MIB))}, 2 * MIB)
        self.assertEqual(set(evidence), {"A"})

    def test_listing_keeps_only_chunk_bodies(self):
        suite = load_suite()
        listed = {"files": [{"path": "/r/A/state/dfs/chunks/a", "bytes": 5},
                            {"path": "/r/A/state/dfs/catalog.wal", "bytes": 9}], "errors": []}
        with patch.object(suite, "probe", return_value=listed):
            self.assertEqual(suite.payload_listing(type("C", (), {"role_dir": staticmethod(lambda r: join("/r", r))})(),
                                                   ["A"]), {"A": {"a": 5}})


class FakeFanout:
    """Scripted cluster for fanout_rounds: counters move when an elbencho phase or the gap runs."""

    def __init__(self, cfg, traffic, *, oom=None, scrape_hook=None, recv_series=True):
        self.config, self.traffic, self.oom = cfg, traffic, oom or {}
        self.scrape_hook, self.recv_series = scrape_hook, recv_series
        self.roles = ["M", "A", "B", "C"]
        self.token, self.uid, self.gid = "t", 10001, 10001
        self.counters = {r: {"send": 0, "recv": 0, "oom": 0} for r in self.roles}
        self.recorded, self.scrapes, self.baselines = [], 0, []
        self.last_hosts = ()
        self.params = benchlib.parameters(cfg)

    def record(self, name, value):
        self.recorded.append((name, value))
        return value

    @staticmethod
    def role_dir(role):
        return join("/run", role)

    root = workspace = role_dir

    @staticmethod
    def address(role):
        return role

    @staticmethod
    def channel(role):
        return type("Ch", (), {"call": lambda *a, **k: {"returncode": 0}})()

    def apply(self, key):
        for role, change in self.traffic.get(key, {}).items():
            for name, value in change.items():
                self.counters[role][name] += value
        for role in self.oom.get(key, ()):
            self.counters[role]["oom"] += 1

    def health(self, role, path):
        self.scrapes += 1
        lines = []
        if role != "M":
            lines.append("%s %d" % (SEND, self.counters[role]["send"]))
            if self.recv_series:
                lines.append("%s %d" % (RECV, self.counters[role]["recv"]))
        text = "\n".join(lines) + "\n"
        return self.scrape_hook(self, role, text) if self.scrape_hook else text

    def command(self, role, argv, **kw):
        if "--hosts" in argv and "--quit" not in argv:
            self.last_hosts = tuple(h.split(":")[0] for h in argv[argv.index("--hosts") + 1].split(","))
            self.apply(self.last_hosts)
        return {"stdout": "", "stderr": "", "returncode": 0}

    def python(self, role, code, *args, **kw):
        if "open(sys.argv[1]).read()" in code:
            hosts = len(self.last_hosts)
            phase = {"phase_type": "READ", "config": {"version": "3.3", "hosts": hosts},
                     "last_done": {"bytes": self.params.file_bytes * hosts, "elapsed_time_ms": 1000}}
            return {"stdout": json.dumps(phase) + "\n"}
        if "seek" in code:
            return {"stdout": json.dumps({"size": 0, "rotated": False,
                                          "counts": {e: 0 for e in json.loads(args[2])}})}
        return {"stdout": "x"}

    def snapshot(self, _c, roles):
        snapshots = {}
        for role in roles:
            snapshots[role] = {"time": "t", "diskstats": "", "loadavg": "0 0 0", "psi_cpu": "", "psi_io": "",
                               "vmstat": "oom_kill %d\n" % self.counters[role]["oom"],
                               "meminfo": "MemAvailable: 1 kB\n"}
        return snapshots

    def run(self):
        suite = load_suite()

        def backend_state(_c, _p, _nodes, _source, baseline=None):
            self.baselines.append(baseline)
            return {"medium": "disk"}, "remote_cold"

        with patch.object(suite, "write_file", return_value={"bytes": self.params.file_bytes, "mib_per_s": 1.0}), \
                patch.object(suite, "wait_visible"), patch.object(suite, "host_snapshot", side_effect=self.snapshot), \
                patch.object(suite, "backend_state", side_effect=backend_state), \
                patch.object(suite, "payload_listing", return_value={"A": {}}), \
                patch.object(suite, "boot_id", side_effect=lambda c, r: r), patch.object(suite, "remove"), \
                patch.object(suite, "mkdir_owned"), \
                patch.object(suite.time, "sleep", side_effect=lambda s: self.apply("gap")):
            suite.fanout_rounds(self, self.params, ("B", "C"), "/tmp/l", "/opt/bench/elbencho", {})
        reads = [v for n, v in self.recorded if n == "bench.round" and v["op"] == "read"]
        return reads[:len(reads) // 3]  # first round; every round is scripted alike


S = 64 * MIB
STAGGER = {"mode": "stagger", "seed": "B", "gap_ms": 10}
REPLICAS = {"variant": "P3", "dfs_desired_copies": 3, "dfs_sync_required_copies": 1}


class PhaseLabelTests(unittest.TestCase):
    """Issue 2: each phase is labelled from its own window; the whole-round check is unchanged."""

    def test_remote_seed_and_local_followers_get_different_labels(self):
        traffic = {("B",): {"A": {"send": S}, "B": {"recv": S}}, ("C",): {}}
        reads = FakeFanout(bench(fanout_arrival=STAGGER, **REPLICAS), traffic).run()
        self.assertEqual([r["layer"] for r in reads], ["remote_cold", "remote_post_seed_local_replica"])
        whole = reads[1]["evidence"]["send_check"]
        self.assertEqual((whole["sent_total"], whole["expected"], whole["ok"]), (S, 2 * S, None))
        self.assertEqual(reads[0]["evidence"]["phase_send"]["sent_total"], S)
        self.assertEqual(reads[1]["evidence"]["local_read"]["by_reader"]["C"]["local_bytes"], S)

    def test_followers_record_the_seed_prewarm(self):
        traffic = {("B",): {"A": {"send": S}, "B": {"recv": S}},
                   ("C",): {"A": {"send": S // 4}, "B": {"send": 3 * S // 4}, "C": {"recv": S}}}
        reads = FakeFanout(bench(fanout_arrival=STAGGER, variant="VR", node_overrides={
            "*": {"dfs_read_cache_bytes": 1024 * MIB}}), traffic).run()
        self.assertEqual([r["layer"] for r in reads], ["remote_cold", "remote_post_seed"])
        self.assertEqual(reads[1]["evidence"]["backend_scope"], "before_seed")
        prewarm = reads[1]["evidence"]["prewarm"]
        self.assertEqual(prewarm, {"after": "seed", "seed": "B", "gap_ms": 10, "seed_sent_bytes": 3 * S // 4})
        self.assertNotIn("prewarm", reads[0]["evidence"])
        self.assertTrue(reads[1]["evidence"]["send_check"]["ok"])


class CounterValidityTests(unittest.TestCase):
    """Issue 3: only complete, non-reset counters reconcile; local_replica needs reader evidence."""

    def test_counter_window_distinguishes_zero_missing_and_reset(self):
        ok = benchlib.counter_window(SEND + " 5\n", SEND + " 5\n", benchlib.SEND_SERIES)
        self.assertEqual((ok["status"], ok["delta"]), ("complete", 0))
        self.assertEqual(benchlib.counter_window(SEND + " 5\n", "", benchlib.SEND_SERIES)["status"], "missing")
        self.assertEqual(benchlib.counter_window("", SEND + " 5\n", benchlib.SEND_SERIES)["status"], "missing")
        self.assertEqual(benchlib.counter_window(SEND + " 9\n", SEND + " 2\n", benchlib.SEND_SERIES)["status"],
                         "reset")

    @staticmethod
    def drop_a_after_read(fake, role, text):
        return "" if role == "A" and fake.counters["A"]["send"] else text

    def test_missing_writer_series_is_not_a_local_replica_hit(self):
        traffic = {("B", "C"): {"A": {"send": 2 * S}, "B": {"recv": S}, "C": {"recv": S}}}
        reads = FakeFanout(bench(**REPLICAS), traffic, scrape_hook=self.drop_a_after_read).run()
        check = reads[0]["evidence"]["send_check"]
        self.assertEqual((check["metrics"], check["ok"]), ("missing", None))
        self.assertEqual(check["metrics_issues"], {"A": "missing"})
        self.assertEqual(reads[0]["layer"], "remote_cold_unattributed")

    def test_counter_reset_is_not_a_local_replica_hit(self):
        # A restarted between the scrapes: its cumulative send counter went from 10*S back to 0.
        fake = FakeFanout(bench(**REPLICAS), {("B", "C"): {"B": {"recv": S}, "C": {"recv": S}}},
                          scrape_hook=lambda f, role, text: SEND + " 0\n" if role == "A" and f.counters["B"]["recv"]
                          else text)
        fake.counters["A"]["send"] = 10 * S
        reads = fake.run()
        self.assertEqual(reads[0]["evidence"]["send_check"]["metrics"], "reset")
        self.assertEqual(reads[0]["layer"], "remote_cold_unattributed")

    def test_true_zero_send_with_reader_evidence_is_a_local_replica_read(self):
        reads = FakeFanout(bench(**REPLICAS), {("B", "C"): {}}).run()
        self.assertEqual(reads[0]["evidence"]["send_check"]["metrics"], "complete")
        self.assertEqual(reads[0]["layer"], "remote_cold_local_replica")

    def test_zero_send_without_reader_series_stays_unattributed(self):
        reads = FakeFanout(bench(**REPLICAS), {("B", "C"): {}}, recv_series=False).run()
        self.assertEqual(reads[0]["evidence"]["local_read"]["status"], "missing")
        self.assertEqual(reads[0]["layer"], "remote_cold_unattributed")

    def test_default_single_copy_with_missing_series_voids_the_round(self):
        traffic = {("B", "C"): {"A": {"send": 2 * S}, "B": {"recv": S}, "C": {"recv": S}}}
        with self.assertRaises(AssertionError) as raised:
            FakeFanout(bench(), traffic, scrape_hook=self.drop_a_after_read).run()
        self.assertIn("missing", str(raised.exception))

    def test_default_single_copy_label_is_unchanged(self):
        traffic = {("B", "C"): {"A": {"send": 2 * S}, "B": {"recv": S}, "C": {"recv": S}}}
        reads = FakeFanout(bench(), traffic).run()
        self.assertEqual(reads[0]["layer"], "remote_cold")
        self.assertTrue(reads[0]["evidence"]["send_check"]["ok"])


class RoundOomTests(unittest.TestCase):
    """Issue 4: the whole round's kernels, gap included, are checked for OOM kills."""

    VR = dict(fanout_arrival=STAGGER, variant="VR", node_overrides={"*": {"dfs_read_cache_bytes": 1024 * MIB}})
    TRAFFIC = {("B",): {"A": {"send": S}, "B": {"recv": S}},
               ("C",): {"B": {"send": S}, "C": {"recv": S}}}

    def test_seed_oom_while_serving_followers_voids_a_reconciled_round(self):
        with self.assertRaises(AssertionError) as raised:
            FakeFanout(bench(**self.VR), self.TRAFFIC, oom={("C",): ("B",)}).run()
        self.assertIn("OOM", str(raised.exception))

    def test_oom_during_the_gap_voids_the_round(self):
        with self.assertRaises(AssertionError) as raised:
            FakeFanout(bench(**self.VR), self.TRAFFIC, oom={"gap": ("C",)}).run()
        self.assertIn("OOM", str(raised.exception))

    def test_clean_round_records_every_role_kernel(self):
        reads = FakeFanout(bench(**self.VR), self.TRAFFIC).run()
        self.assertEqual(set(reads[-1]["evidence"]["round_memory"]), {"M", "A", "B", "C"})


class BaselineWiringTests(unittest.TestCase):
    def test_coverage_baseline_only_for_non_default_placement(self):
        traffic = {("B", "C"): {"A": {"send": 2 * S}, "B": {"recv": S}, "C": {"recv": S}}}
        default = FakeFanout(bench(), traffic)
        default.run()
        self.assertEqual(default.baselines, [None] * 3)
        striped = FakeFanout(bench(variant="P2", dfs_local_copy="none"), {("B", "C"): {}})
        striped.run()
        self.assertEqual(striped.baselines, [{"A": {}}] * 3)


if __name__ == "__main__":
    unittest.main()
