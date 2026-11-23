"""Tool contracts for reader-sharing and placement measurement inputs.

They pin the read-cache and source-cache variant keys, topology-level replica settings,
all-Node and Meta metric evidence with the sent-bytes reconciliation, staggered fan-out
arrival and per-round Node log event counts. No product or performance claim.
"""

import json
import subprocess
import sys
import tempfile
import unittest
from pathlib import Path
from posixpath import join
from unittest.mock import patch

TESTS = Path(__file__).resolve().parents[1]
sys.path.append(str(TESTS))
from fixtures.common import Blocked, Case  # noqa: E402
from tools import benchlib  # noqa: E402

MIB = 1024**2
KIB = 1024


def bench(**extra):
    value = {"file_bytes": 64 * MIB, "block_bytes": [MIB], "threads": [1], "rounds": 3,
             "tools": {"fio": "/opt/bench/fio", "elbencho": "/opt/bench/elbencho"}}
    value.update(extra)
    return {"bench": value}


def blocked(test, **extra):
    with test.assertRaises(Blocked):
        benchlib.parameters(bench(**extra))


class ReadCacheKeyTests(unittest.TestCase):
    def test_read_cache_stripe_and_source_cache_keys_are_accepted(self):
        p = benchlib.parameters(bench(variant="VR", node_overrides={"*": {
            "dfs_read_cache_bytes": 4 * MIB + 4 * KIB, "dfs_read_stripe_bytes": 8 * MIB,
            "dfs_source_cache_max_ms": 60000, "dfs_read_parallelism": 8, "dfs_read_pipeline.max_rpcs": 128}}))
        self.assertEqual(p.overrides_for("B")["dfs_read_cache_bytes"], 4 * MIB + 4 * KIB)
        p = benchlib.parameters(bench(variant="VR0", node_overrides={"B": {
            "dfs_read_cache_bytes": 0, "dfs_read_stripe_bytes": 64 * KIB, "dfs_source_cache_max_ms": 0}}))
        self.assertEqual(p.overrides_for("B")["dfs_read_stripe_bytes"], 64 * KIB)

    def test_values_outside_product_limits_are_blocked(self):
        for bad in ({"dfs_read_cache_bytes": 4 * MIB}, {"dfs_read_cache_bytes": 1},
                    {"dfs_read_cache_bytes": 1024**4 + 1}, {"dfs_read_stripe_bytes": 32 * KIB},
                    {"dfs_read_stripe_bytes": 96 * KIB}, {"dfs_read_stripe_bytes": 8 * MIB + 64 * KIB},
                    {"dfs_source_cache_max_ms": 60001}, {"dfs_source_cache_max_ms": -1},
                    {"dfs_read_parallelism": 9}, {"dfs_read_pipeline.max_rpcs": 129},
                    {"dfs_read_cache_bytes": True}):
            with self.subTest(bad=bad):
                blocked(self, variant="Vx", node_overrides={"B": bad})

    def test_read_cache_and_peer_cache_on_one_role_are_refused(self):
        blocked(self, variant="Vx", node_overrides={
            "B": {"dfs_read_cache_bytes": 64 * MIB, "dfs_peer_cache.capacity_bytes": 64 * MIB}})
        # "*" merges into every role, so the conflict is judged on the effective settings.
        blocked(self, variant="Vx", node_overrides={
            "*": {"dfs_read_cache_bytes": 64 * MIB}, "C": {"dfs_peer_cache.capacity_bytes": 64 * MIB}})
        p = benchlib.parameters(bench(variant="Vx", node_overrides={
            "B": {"dfs_read_cache_bytes": 64 * MIB}, "C": {"dfs_peer_cache.capacity_bytes": 64 * MIB}}))
        self.assertEqual(set(p.node_overrides), {"B", "C"})

    def test_new_keys_render_as_top_level_node_settings(self):
        toml = benchlib.node_overrides_toml({"dfs_read_cache_bytes": 64 * MIB, "dfs_read_stripe_bytes": MIB,
                                             "dfs_source_cache_max_ms": 0}, "/unused")
        self.assertEqual(toml.splitlines(), ["dfs_read_cache_bytes = %d" % (64 * MIB),
                                             "dfs_read_stripe_bytes = %d" % MIB, "dfs_source_cache_max_ms = 0"])


class ReadCacheBudgetKeyTests(unittest.TestCase):
    """H2 step 1 keys: staging/partial budgets and the single-source TTL, within product limits."""

    def test_budget_and_single_ttl_keys_are_accepted_at_product_bounds(self):
        for settings in ({"dfs_read_cache_queue_bytes": 4 * MIB}, {"dfs_read_cache_queue_bytes": 1024 * MIB},
                         {"dfs_read_cache_partial_bytes": 0}, {"dfs_read_cache_partial_bytes": 4 * MIB},
                         {"dfs_read_cache_partial_bytes": 1024 * MIB}, {"dfs_source_single_ttl_ms": 0},
                         {"dfs_source_single_ttl_ms": 60000}):
            with self.subTest(settings=settings):
                p = benchlib.parameters(bench(variant="VR", node_overrides={
                    "B": dict(settings, dfs_read_cache_bytes=64 * MIB)}))
                self.assertEqual(p.inert_overrides("B"), [])

    def test_out_of_range_values_are_blocked(self):
        for bad in ({"dfs_read_cache_queue_bytes": 0}, {"dfs_read_cache_queue_bytes": 4 * MIB - 1},
                    {"dfs_read_cache_queue_bytes": 1024 * MIB + 1}, {"dfs_read_cache_partial_bytes": 1},
                    {"dfs_read_cache_partial_bytes": 4 * MIB - 1}, {"dfs_read_cache_partial_bytes": 1024 * MIB + 1},
                    {"dfs_source_single_ttl_ms": 60001}, {"dfs_source_single_ttl_ms": -1},
                    {"dfs_source_single_ttl_ms": True}):
            with self.subTest(bad=bad):
                blocked(self, variant="Vx", node_overrides={"B": dict(bad, dfs_read_cache_bytes=64 * MIB)})

    def test_keys_without_the_read_cache_are_accepted_and_reported_inert(self):
        # The product range-checks these on every Node but uses them only when dfs_read_cache_bytes is
        # non-zero, so the bench records them as having no effect instead of refusing.
        p = benchlib.parameters(bench(variant="Vx", node_overrides={
            "*": {"dfs_read_cache_queue_bytes": 64 * MIB, "dfs_source_single_ttl_ms": 100},
            "B": {"dfs_read_cache_bytes": 64 * MIB}, "C": {"dfs_read_cache_bytes": 0,
                                                          "dfs_read_cache_partial_bytes": 0}}))
        self.assertEqual(p.inert_overrides("B"), [])
        self.assertEqual(p.inert_overrides("A"), ["dfs_read_cache_queue_bytes", "dfs_source_single_ttl_ms"])
        self.assertEqual(p.inert_overrides("C"), ["dfs_read_cache_partial_bytes", "dfs_read_cache_queue_bytes",
                                                  "dfs_source_single_ttl_ms"])

    def test_keys_render_as_top_level_settings(self):
        toml = benchlib.node_overrides_toml({"dfs_read_cache_bytes": 64 * MIB, "dfs_read_cache_queue_bytes": 8 * MIB,
                                             "dfs_read_cache_partial_bytes": 0, "dfs_source_single_ttl_ms": 250},
                                            "/unused")
        self.assertEqual(toml.splitlines(), ["dfs_read_cache_bytes = %d" % (64 * MIB),
                                             "dfs_read_cache_partial_bytes = 0",
                                             "dfs_read_cache_queue_bytes = %d" % (8 * MIB),
                                             "dfs_source_single_ttl_ms = 250"])

    def test_cluster_records_inert_keys_only_when_present(self):
        with tempfile.TemporaryDirectory() as d:
            c = ClusterRenderingTests().cluster(d, {"dfs_read_cache_queue_bytes": 64 * MIB, "dfs_read_cache_bytes": 0},
                                                variant="Vq",
                                                node_overrides={"B": {"dfs_read_cache_queue_bytes": 64 * MIB}})
            c.write_configs()
            records = {e["check"]: e["value"] for e in c.evidence if e["check"].startswith("bench.node_overrides.")}
            self.assertEqual(records["bench.node_overrides.B"]["inert"], ["dfs_read_cache_queue_bytes"])
            self.assertNotIn("inert", records["bench.node_overrides.A"])


class SummaryAttributionTests(unittest.TestCase):
    """bench.summary attributes a variant only to settings that take effect on a started Node."""

    QUEUE, PARTIAL, TTL = "dfs_read_cache_queue_bytes", "dfs_read_cache_partial_bytes", "dfs_source_single_ttl_ms"

    def summary(self, roles, **extra):
        suite = load_suite()
        recorded = {}

        class Cluster:
            config = bench(**extra)

            def record(self, name, value):
                recorded[name] = value
                return value

        cluster = Cluster()
        cluster.roles = list(roles)
        suite.summaries(cluster, [{"op": "read", "layer": "hot", "tool": "fio", "block_bytes": MIB, "threads": 1,
                                   "result": {"mib_per_s": 10.0}}] * 3)
        return recorded["bench.summary"]

    def test_only_inert_keys_are_no_variant(self):
        row = self.summary("MABC", variant="Vq", node_overrides={
            "*": {self.QUEUE: 64 * MIB, self.TTL: 100}, "B": {self.PARTIAL: 0}})
        self.assertEqual(row["node_overrides"], {})
        self.assertFalse(row["variant_applied"])
        # Only the read-cache-only keys are filtered: an explicit dfs_read_cache_bytes = 0 is still declared.
        row = self.summary("MABC", variant="Vq",
                           node_overrides={"B": {"dfs_read_cache_bytes": 0, self.QUEUE: 64 * MIB}})
        self.assertEqual(row["node_overrides"], {"B": {"dfs_read_cache_bytes": 0}})
        # The same effectiveness rule admits such a declaration under the V0 label.
        self.assertEqual(benchlib.parameters(bench(node_overrides={"*": {self.QUEUE: 64 * MIB}})).variant, "V0")

    def test_wildcard_keys_count_only_on_nodes_with_the_read_cache(self):
        row = self.summary("MABC", variant="VR", node_overrides={
            "*": {self.QUEUE: 64 * MIB, self.TTL: 100},
            "A": {"dfs_read_parallelism": 4}, "B": {"dfs_read_cache_bytes": 64 * MIB}})
        self.assertEqual(row["node_overrides"], {
            "A": {"dfs_read_parallelism": 4},
            "B": {"dfs_read_cache_bytes": 64 * MIB, self.QUEUE: 64 * MIB, self.TTL: 100}})
        self.assertTrue(row["variant_applied"])

    def test_overrides_for_roles_not_started_are_no_variant(self):
        row = self.summary("MAB", variant="Vx", node_overrides={"D": {"dfs_read_parallelism": 4}})
        self.assertEqual((row["node_overrides"], row["variant_applied"]), ({}, False))

    def test_replica_settings_keep_the_variant_applied(self):
        row = self.summary("MABC", variant="P3", dfs_desired_copies=3, dfs_sync_required_copies=1,
                           node_overrides={"*": {self.QUEUE: 64 * MIB}})
        self.assertEqual(row["node_overrides"], {})
        self.assertTrue(row["variant_applied"])

    def test_node_evidence_keeps_every_declared_key(self):
        with tempfile.TemporaryDirectory() as d:
            c = ClusterRenderingTests().cluster(d, {self.QUEUE: 64 * MIB, "dfs_read_cache_bytes": 0},
                                                node_overrides={"B": {self.QUEUE: 64 * MIB}})
            c.write_configs()
            record = {e["check"]: e["value"] for e in c.evidence}["bench.node_overrides.B"]
            self.assertEqual(record, {"declared": {self.QUEUE: 64 * MIB}, "mismatches": [], "inert": [self.QUEUE]})

    def test_binary_without_the_new_field_is_still_refused(self):
        with tempfile.TemporaryDirectory() as d:
            c = ClusterRenderingTests().cluster(d, {"dfs_read_cache_bytes": 0},
                                                node_overrides={"B": {self.QUEUE: 64 * MIB}})
            with self.assertRaises(AssertionError):
                c.write_configs()


class SingleTtlAttributionTests(unittest.TestCase):
    """The single-source TTL only acts while the read cache runs and the ordinary source TTL is non-zero."""

    QUEUE, TTL = SummaryAttributionTests.QUEUE, SummaryAttributionTests.TTL
    CACHE, NORMAL = "dfs_read_cache_bytes", "dfs_source_cache_max_ms"
    summary = SummaryAttributionTests.summary

    def test_zero_ordinary_ttl_makes_any_single_ttl_inert(self):
        rows = [self.summary("MABC", variant="VR", node_overrides={
            "*": {self.NORMAL: 0}, "B": {self.CACHE: 64 * MIB, self.TTL: ttl}}) for ttl in (100, 250)]
        self.assertEqual(rows[0]["node_overrides"], rows[1]["node_overrides"])
        self.assertNotIn(self.TTL, rows[0]["node_overrides"]["B"])
        p = benchlib.parameters(bench(variant="VR", node_overrides={
            "*": {self.NORMAL: 0}, "B": {self.CACHE: 64 * MIB, self.TTL: 100}}))
        self.assertEqual(p.inert_overrides("B"), [self.TTL])

    def test_wildcard_and_role_settings_are_merged_before_judging(self):
        overrides = {"*": {self.NORMAL: 0, self.TTL: 100, self.QUEUE: 64 * MIB},
                     "B": {self.CACHE: 64 * MIB}, "C": {self.CACHE: 64 * MIB, self.NORMAL: 1000}}
        p = benchlib.parameters(bench(variant="VR", node_overrides=overrides))
        self.assertEqual(p.inert_overrides("B"), [self.TTL])
        self.assertEqual(p.inert_overrides("C"), [])
        self.assertEqual(p.inert_overrides("A"), [self.QUEUE, self.TTL])
        row = self.summary("MABC", variant="VR", node_overrides=overrides)
        self.assertEqual(row["node_overrides"], {
            "A": {self.NORMAL: 0},
            "B": {self.NORMAL: 0, self.QUEUE: 64 * MIB, self.CACHE: 64 * MIB},
            "C": {self.NORMAL: 1000, self.TTL: 100, self.QUEUE: 64 * MIB, self.CACHE: 64 * MIB}})
        self.assertTrue(row["variant_applied"])

    def test_explicit_zero_single_ttl_keeps_the_ordinary_deadline_and_counts(self):
        # Ordinary TTL left at its product default (non-zero): single TTL 0 means "use the ordinary deadline".
        row = self.summary("MAB", variant="VR", node_overrides={"B": {self.CACHE: 64 * MIB, self.TTL: 0}})
        self.assertEqual(row["node_overrides"], {"B": {self.CACHE: 64 * MIB, self.TTL: 0}})

    def test_only_an_inert_single_ttl_besides_the_variant_settings(self):
        # B's remaining settings still form a variant even though its single TTL is dropped.
        row = self.summary("MAB", variant="VR", node_overrides={
            "B": {self.CACHE: 64 * MIB, self.NORMAL: 0, self.TTL: 100}})
        self.assertEqual(row["node_overrides"], {"B": {self.CACHE: 64 * MIB, self.NORMAL: 0}})
        self.assertTrue(row["variant_applied"])

    def test_node_evidence_lists_the_single_ttl_as_inert_and_still_checks_it(self):
        declared = {self.CACHE: 64 * MIB, self.NORMAL: 0, self.TTL: 100}
        with tempfile.TemporaryDirectory() as d:
            c = ClusterRenderingTests().cluster(d, dict(declared), variant="VR", node_overrides={"B": declared})
            c.write_configs()
            record = {e["check"]: e["value"] for e in c.evidence}["bench.node_overrides.B"]
            self.assertEqual(record, {"declared": declared, "mismatches": [], "inert": [self.TTL]})
        with tempfile.TemporaryDirectory() as d:
            effective = {self.CACHE: 64 * MIB, self.NORMAL: 0}
            c = ClusterRenderingTests().cluster(d, effective, variant="VR", node_overrides={"B": declared})
            with self.assertRaises(AssertionError):
                c.write_configs()


class AbsentRoleTests(unittest.TestCase):
    @staticmethod
    def topology(roles, **extra):
        return dict(bench(**extra), roles={r: {"host": "h-" + r} for r in roles})

    def test_overrides_for_roles_outside_the_topology_do_not_make_a_variant(self):
        p = benchlib.parameters(self.topology("MAB", node_overrides={"E": {"dfs_read_parallelism": 4}}))
        self.assertEqual(p.variant, "V0")
        self.assertEqual(p.applied_overrides(("A", "B")), {})

    def test_overrides_for_present_roles_still_need_a_label(self):
        with self.assertRaises(Blocked):
            benchlib.parameters(self.topology("MAB", node_overrides={"B": {"dfs_read_parallelism": 4}}))
        with self.assertRaises(Blocked):
            benchlib.parameters(self.topology("MAB", node_overrides={"*": {"dfs_read_parallelism": 4}}))

    def test_applied_overrides_keep_only_cluster_roles_with_settings(self):
        p = benchlib.parameters(bench(variant="V1", node_overrides={
            "*": {"dfs_read_crc_mode": "off"}, "E": {"dfs_read_parallelism": 4}}))
        self.assertEqual(p.applied_overrides(("A", "B")),
                         {"A": {"dfs_read_crc_mode": "off"}, "B": {"dfs_read_crc_mode": "off"}})
        self.assertNotIn("E", p.applied_overrides(("A", "B")))

    def test_summary_records_only_overrides_applied_in_the_case_cluster(self):
        suite = load_suite()
        recorded = {}

        class Cluster:
            config = bench(variant="V2", node_overrides={"D": {"dfs_read_parallelism": 4}})
            roles = ["M", "A", "B"]

            @staticmethod
            def record(name, value):
                recorded[name] = value
                return value

        suite.summaries(Cluster(), [{"op": "read", "layer": "hot", "tool": "fio", "block_bytes": MIB, "threads": 1,
                                     "result": {"mib_per_s": 10.0}}] * 3)
        row = recorded["bench.summary"]
        self.assertEqual(row["node_overrides"], {})
        self.assertFalse(row["variant_applied"])


class ReplicationTests(unittest.TestCase):
    def test_absent_replication_keeps_the_fixture_default(self):
        p = benchlib.parameters(bench())
        self.assertEqual(p.replication, {})
        self.assertEqual(benchlib.replication_settings(p.replication), None)

    def test_declared_copies_render_for_meta_and_node(self):
        p = benchlib.parameters(bench(variant="P3", dfs_desired_copies=3, dfs_sync_required_copies=1))
        settings = benchlib.replication_settings(p.replication)
        self.assertEqual(settings, {"dfs_desired_copies": 3, "dfs_sync_required_copies": 1,
                                    "dfs_min_distinct_nodes": 3, "dfs_min_distinct_failure_domains": 1,
                                    "dfs_local_copy": "required"})
        toml = benchlib.replication_toml(settings)
        self.assertIn("dfs_desired_copies = 3\n", toml)
        self.assertIn("dfs_sync_required_copies = 1\n", toml)
        self.assertIn('dfs_local_copy = "required"\n', toml)
        self.assertFalse(any(line.startswith("[") for line in toml.splitlines()))

    def test_sync_defaults_to_desired_and_local_copy_is_declarable(self):
        p = benchlib.parameters(bench(variant="P2", dfs_desired_copies=2, dfs_local_copy="none"))
        settings = benchlib.replication_settings(p.replication)
        self.assertEqual((settings["dfs_sync_required_copies"], settings["dfs_local_copy"]), (2, "none"))

    def test_invalid_replication_is_blocked(self):
        for bad in ({"dfs_desired_copies": 0}, {"dfs_desired_copies": 6}, {"dfs_desired_copies": "3"},
                    {"dfs_desired_copies": 2, "dfs_sync_required_copies": 3},
                    {"dfs_sync_required_copies": 0}, {"dfs_local_copy": "maybe"}):
            with self.subTest(bad=bad):
                blocked(self, variant="Px", **bad)

    def test_replication_is_a_variant(self):
        blocked(self, dfs_desired_copies=3, dfs_sync_required_copies=1)

    def test_copies_beyond_the_case_nodes_are_blocked(self):
        from fixtures.multivm import validate

        seq = Case("bench.dfs.sequential", ("bench-dfs",), "dfs", (), None, purpose="performance")
        roles = {r: {"host": "h-" + r.lower(), "address": "10.0.0.%d" % (i + 10)} for i, r in enumerate("MABC")}
        hosts = {"h-" + r.lower(): {"transport": "exec", "command_prefix": ["true"], "base": join("/b", r)}
                 for r in "MABC"}
        config = dict(bench(variant="P3", dfs_desired_copies=3, dfs_sync_required_copies=1), kind="namespaces",
                      uid=10001, gid=10001, other_uid=10002, other_gid=10002, roles=roles, hosts=hosts)
        with self.assertRaises(Blocked):
            validate(config, [seq])  # sequential runs A and B only
        fanout = Case("bench.dfs.fanout.many_readers", ("bench-dfs",), "dfs", (), None, purpose="performance")
        self.assertEqual(len(validate(config, [fanout])), 4)

    def test_send_reconciliation_applies_only_when_the_writer_holds_the_only_copy(self):
        self.assertTrue(benchlib.single_writer_copy({}))
        self.assertTrue(benchlib.single_writer_copy({"dfs_desired_copies": 1}))
        self.assertFalse(benchlib.single_writer_copy({"dfs_desired_copies": 3, "dfs_sync_required_copies": 1}))
        self.assertFalse(benchlib.single_writer_copy({"dfs_desired_copies": 1, "dfs_local_copy": "none"}))


class FakeChannel:
    def __init__(self, effective):
        self.effective, self.calls = effective, []

    def call(self, action, **request):
        self.calls.append((action, request))
        if action == "command":
            argv = request["argv"]
            if "--print-config" in argv:
                return {"returncode": 0, "stdout": json.dumps(self.effective), "stderr": ""}
            if argv[0] == "findmnt":
                return {"returncode": 0, "stdout": "tmpfs\n", "stderr": ""}
            return {"returncode": 0, "stdout": "", "stderr": ""}
        return {}


class ClusterRenderingTests(unittest.TestCase):
    FANOUT = Case("bench.dfs.fanout.many_readers", ("bench-dfs",), "dfs", (), None, purpose="performance")
    REPLICAS = {"dfs_desired_copies": 3, "dfs_sync_required_copies": 1, "dfs_min_distinct_nodes": 3,
                "dfs_min_distinct_failure_domains": 1, "dfs_local_copy": "required"}

    def cluster(self, directory, effective, **bench_extra):
        from fixtures.multivm import Cluster

        roles = {r: {"host": "h-" + r.lower(), "address": "10.0.0.%d" % (i + 10)} for i, r in enumerate("MABC")}
        config = dict(bench(**bench_extra), kind="namespaces", uid=10001, gid=10001, other_uid=10002,
                      other_gid=10002, roles=roles, hosts={})
        c = Cluster(config, directory, directory, self.FANOUT, "t")
        c.channels = {"h-" + r.lower(): FakeChannel(effective) for r in "MABC"}
        c.roots = {"h-" + r.lower(): join("/run", r) for r in "MABC"}
        return c

    def test_replica_settings_reach_meta_and_every_node_and_are_proven(self):
        with tempfile.TemporaryDirectory() as d:
            c = self.cluster(d, dict(self.REPLICAS, dfs_read_parallelism=4), variant="P3", dfs_desired_copies=3,
                             dfs_sync_required_copies=1, node_overrides={"*": {"dfs_read_parallelism": 4}})
            c.write_configs()
            meta = (Path(d) / "config-M.toml").read_text()
            self.assertIn("dfs_desired_copies = 3\n", meta)
            self.assertIn("dfs_sync_required_copies = 1\n", meta)
            self.assertEqual(meta.count("dfs_desired_copies"), 1)
            for role in "ABC":
                text = (Path(d) / ("config-%s.toml" % role)).read_text()
                self.assertIn("dfs_sync_required_copies = 1\n", text)
                self.assertLess(text.index("dfs_local_copy"), text.index("dfs_read_parallelism"))
            self.assertEqual(c.evidence[-1]["check"], "bench.node_overrides.C")
            replication = [e for e in c.evidence if e["check"].startswith("bench.replication.")]
            self.assertEqual({e["check"] for e in replication},
                             {"bench.replication." + r for r in "MABC"})

    def test_replica_settings_missing_from_print_config_fail(self):
        with tempfile.TemporaryDirectory() as d:
            c = self.cluster(d, dict(self.REPLICAS, dfs_sync_required_copies=3), variant="P3",
                             dfs_desired_copies=3, dfs_sync_required_copies=1)
            with self.assertRaises(AssertionError):
                c.write_configs()

    def test_default_meta_policy_is_unchanged(self):
        with tempfile.TemporaryDirectory() as d:
            c = self.cluster(d, {})
            c.write_configs()
            meta = (Path(d) / "config-M.toml").read_text()
            self.assertIn("dfs_desired_copies = 1\ndfs_sync_required_copies = 1\n", meta)
            self.assertNotIn("dfs_desired_copies", (Path(d) / "config-B.toml").read_text())

    def test_memory_backend_puts_the_peer_cache_on_tmpfs_too(self):
        with tempfile.TemporaryDirectory() as d:
            c = self.cluster(d, {"dfs_peer_cache": {"capacity_bytes": 64 * MIB}}, variant="V4", backend="memory",
                             memory_backend_bytes=1024 * MIB,
                             node_overrides={"B": {"dfs_peer_cache.capacity_bytes": 64 * MIB}})
            c.write_configs()
            mounts = [req["argv"] for action, req in c.channel("B").calls
                      if action == "command" and req["argv"][0] == "mount"]
            self.assertEqual(sorted(m[-1].rsplit("/", 1)[-1] for m in mounts), ["peer-cache", "state"])
            peer = next(m for m in mounts if m[-1].endswith("/peer-cache"))
            self.assertIn("size=%d" % (128 * MIB), peer[4])
            a_mounts = [req["argv"][-1] for action, req in c.channel("A").calls
                        if action == "command" and req["argv"][0] == "mount"]
            self.assertEqual([m.rsplit("/", 1)[-1] for m in a_mounts], ["state"])
            c.unmount_memory_backends([])
            unmounts = []
            for role in "ABC":
                for action, request in c.channel(role).calls:
                    if action == "command" and request["argv"][0] == "umount":
                        unmounts.append(request["argv"][-1])
            self.assertEqual(len(unmounts), 4)
            self.assertEqual(c.memory_mounts, {})

    def test_disk_backend_peer_cache_stays_a_plain_directory(self):
        with tempfile.TemporaryDirectory() as d:
            c = self.cluster(d, {"dfs_peer_cache": {"capacity_bytes": 64 * MIB}}, variant="V4",
                             node_overrides={"B": {"dfs_peer_cache.capacity_bytes": 64 * MIB}})
            c.write_configs()
            self.assertFalse(any(req["argv"][0] == "mount" for action, req in c.channel("B").calls
                                 if action == "command"))


class ArrivalTests(unittest.TestCase):
    def test_default_arrival_is_together(self):
        self.assertEqual(benchlib.parameters(bench()).arrival, {"mode": "together"})

    def test_stagger_names_a_reader_seed_and_a_gap(self):
        p = benchlib.parameters(bench(fanout_arrival={"mode": "stagger", "seed": "C", "gap_ms": 15000}))
        self.assertEqual(p.arrival, {"mode": "stagger", "seed": "C", "gap_ms": 15000})
        self.assertEqual(benchlib.arrival_phases(p.arrival, ("B", "C")),
                         [("seed", ("C",)), ("followers", ("B",))])
        self.assertEqual(benchlib.arrival_phases({"mode": "together"}, ("B", "C")), [("all", ("B", "C"))])

    def test_invalid_arrivals_are_blocked(self):
        for bad in ({"mode": "wave"}, {"mode": "stagger", "gap_ms": 0}, {"mode": "stagger", "seed": "A", "gap_ms": 0},
                    {"mode": "stagger", "seed": "D", "gap_ms": 0}, {"mode": "stagger", "seed": "B", "gap_ms": -1},
                    {"mode": "stagger", "seed": "B", "gap_ms": 600001}, {"mode": "stagger", "seed": "B"},
                    {"mode": "together", "seed": "B"}, "stagger"):
            with self.subTest(bad=bad):
                blocked(self, fanout_arrival=bad)
        blocked(self, readers=1, fanout_arrival={"mode": "stagger", "seed": "B", "gap_ms": 0})


SEND = 'afs_dfs_payload_bytes_total{direction="send",operation="read",transport="%s"}'


def windows(sent):
    """Complete SEND_SERIES windows with the given per-role sent bytes."""
    series = SEND % "grpc"
    result = {}
    for role, value in sent.items():
        result[role] = benchlib.counter_window(series + " 0\n", series + " %d\n" % value, benchlib.SEND_SERIES)
    return result


class MetricsEvidenceTests(unittest.TestCase):
    def test_node_pattern_adds_crc_read_bytes_and_meta_pattern_keeps_read_source_methods(self):
        suite = load_suite()
        before = "afs_dfs_crc32c_read_bytes_total 0\n"
        after = "afs_dfs_crc32c_read_bytes_total 4096\n"
        self.assertEqual(benchlib.metrics_delta(before, after, suite.NODE_METRICS),
                         {"afs_dfs_crc32c_read_bytes_total": 4096.0})
        meta_before = ('afs_meta_rpc_requests_total{method="afs.meta.v1.DfsMeta/PublishReadCache"} 1\n'
                       'afs_meta_rpc_requests_total{method="afs.meta.v1.DfsMeta/GetChunkSources"} 5\n'
                       'afs_meta_rpc_requests_total{method="afs.meta.v1.DfsMeta/Mkdir"} 1\n')
        meta_after = ('afs_meta_rpc_requests_total{method="afs.meta.v1.DfsMeta/PublishReadCache"} 3\n'
                      'afs_meta_rpc_requests_total{method="afs.meta.v1.DfsMeta/GetChunkSources"} 9\n'
                      'afs_meta_rpc_requests_total{method="afs.meta.v1.DfsMeta/Mkdir"} 4\n'
                      'afs_meta_rpc_requests_total{method="afs.meta.v1.DfsMeta/GetPlacementSnapshot"} 2\n')
        delta = benchlib.metrics_delta(meta_before, meta_after, suite.META_METRICS)
        self.assertEqual(set(delta), {'afs_meta_rpc_requests_total{method="afs.meta.v1.DfsMeta/PublishReadCache"}',
                                      'afs_meta_rpc_requests_total{method="afs.meta.v1.DfsMeta/GetChunkSources"}',
                                      'afs_meta_rpc_requests_total{method="afs.meta.v1.DfsMeta/GetPlacementSnapshot"}'})

    def test_sent_read_payload_sums_every_transport(self):
        recv = 'afs_dfs_payload_bytes_total{direction="recv",operation="replica",transport="grpc"}'
        before = "%s 0\n%s 0\n%s 0\n" % (SEND % "grpc", SEND % "rdma", recv)
        after = "%s %d\n%s %d\n%s 9\n" % (SEND % "grpc", 3 * MIB, SEND % "rdma", MIB, recv)
        self.assertEqual(benchlib.counter_window(before, after, benchlib.SEND_SERIES),
                         {"status": "complete", "delta": 4 * MIB})

    def test_reconciliation_passes_when_all_three_agree(self):
        deltas = windows({"A": 96 * MIB, "B": 32 * MIB, "C": 0})
        check = benchlib.send_reconciliation(deltas, readers=2, file_bytes=64 * MIB, tool_bytes=128 * MIB,
                                             applies=True)
        self.assertEqual(check["sent_by_role"], {"A": 96 * MIB, "B": 32 * MIB, "C": 0})
        self.assertEqual((check["sent_total"], check["expected"], check["ok"]), (128 * MIB, 128 * MIB, True))

    def test_reconciliation_flags_any_disagreement(self):
        deltas = windows({"A": 64 * MIB, "B": 0})
        check = benchlib.send_reconciliation(deltas, readers=2, file_bytes=64 * MIB, tool_bytes=128 * MIB,
                                             applies=True)
        self.assertFalse(check["ok"])
        check = benchlib.send_reconciliation(windows({"A": 128 * MIB}), readers=2, file_bytes=64 * MIB,
                                             tool_bytes=96 * MIB, applies=True)
        self.assertFalse(check["ok"])

    def test_reconciliation_is_recorded_but_not_judged_with_reader_replicas(self):
        check = benchlib.send_reconciliation(windows({"A": 64 * MIB}), readers=2, file_bytes=64 * MIB,
                                             tool_bytes=128 * MIB, applies=False)
        self.assertIsNone(check["ok"])
        self.assertEqual(check["sent_total"], 64 * MIB)

    def test_suite_raises_on_a_failed_reconciliation(self):
        suite = load_suite()
        with self.assertRaises(AssertionError):
            suite.require_reconciled({"ok": False, "sent_total": 1, "expected": 2, "tool_bytes": 2,
                                      "applies": True, "metrics": "complete"})
        suite.require_reconciled({"ok": True, "applies": True, "metrics": "complete"})
        suite.require_reconciled({"ok": None, "applies": False, "metrics": "missing"})

    def test_cluster_metrics_scrape_meta_and_every_node(self):
        suite = load_suite()
        scraped = []

        class Cluster:
            roles = ["M", "A", "B", "C"]

            @staticmethod
            def health(role, path):
                scraped.append((role, path))
                return "afs_dfs_crc32c_read_bytes_total %d\n" % len(scraped)

        before = suite.cluster_metrics(Cluster())
        deltas = suite.cluster_metrics_delta(before, suite.cluster_metrics(Cluster()))
        self.assertEqual(sorted({r for r, _ in scraped}), ["A", "B", "C", "M"])
        self.assertTrue(all(p == "/metrics" for _, p in scraped))
        self.assertEqual(deltas["B"], {"afs_dfs_crc32c_read_bytes_total": 4.0})
        self.assertEqual(deltas["M"], {})


class LocalReplicaLayerTests(unittest.TestCase):
    @staticmethod
    def check(sent, applies):
        return benchlib.send_reconciliation(windows({"A": sent}), readers=2, file_bytes=64 * MIB,
                                            tool_bytes=128 * MIB, applies=applies)

    LOCAL = {"status": "complete", "by_reader": {}, "local_total": 64 * MIB}

    def test_default_single_copy_keeps_the_layer_verbatim(self):
        for layer in ("remote_cold", "remote_cold_unverified", "remote_cold_memory_backend"):
            with self.subTest(layer=layer):
                self.assertEqual(benchlib.replica_layer(layer, self.check(128 * MIB, True), self.LOCAL), layer)
                # Even an unreconciled default round keeps its name; the reconciliation fails it instead.
                self.assertEqual(benchlib.replica_layer(layer, self.check(64 * MIB, True), self.LOCAL), layer)

    def test_reader_replicas_with_fewer_bytes_sent_get_a_suffix(self):
        self.assertEqual(benchlib.replica_layer("remote_cold", self.check(0, False), self.LOCAL),
                         "remote_cold_local_replica")
        self.assertEqual(benchlib.replica_layer("remote_cold_unverified", self.check(64 * MIB, False), self.LOCAL),
                         "remote_cold_unverified_local_replica")

    def test_reader_replicas_that_still_crossed_the_network_keep_the_layer(self):
        self.assertEqual(benchlib.replica_layer("remote_cold", self.check(128 * MIB, False), self.LOCAL),
                         "remote_cold")


class OomTests(unittest.TestCase):
    def test_unreadable_oom_counter_voids_the_round(self):
        suite = load_suite()
        suite.require_no_oom({"A": {"oom_kill_delta": 0}, "B": {"oom_kill_delta": 0}})
        with self.assertRaises(AssertionError):
            suite.require_no_oom({"A": {"oom_kill_delta": 0}, "B": {"oom_kill_delta": None}})
        with self.assertRaises(AssertionError):
            suite.require_no_oom({"A": {"oom_kill_delta": 2}})


class LogEventTests(unittest.TestCase):
    @staticmethod
    def run_script(path, offset, events):
        output = subprocess.run([sys.executable, "-c", "import os,sys,json\n" + benchlib.LOG_EVENTS_SCRIPT,
                                 path, str(offset), json.dumps(events)], check=True, capture_output=True, text=True)
        return json.loads(output.stdout)

    def test_default_events_include_the_sharing_failures(self):
        events = benchlib.parameters(bench()).log_events
        for name in ("dfs.read_cache_install_failed", "dfs.read_cache_reconcile_retry",
                     "DFS peer read admission deadline exceeded", "peer stayed busy"):
            self.assertIn(name, events)

    def test_declared_events_are_added_once(self):
        p = benchlib.parameters(bench(log_events=["dfs.custom", "peer stayed busy"]))
        self.assertEqual(p.log_events.count("peer stayed busy"), 1)
        self.assertEqual(p.log_events[-1], "dfs.custom")
        for bad in ("x", [""], [1], ["a\nb"], ["x" * 201]):
            with self.subTest(bad=bad):
                blocked(self, log_events=bad)

    def test_counts_lines_appended_after_the_offset(self):
        with tempfile.TemporaryDirectory() as d:
            log = Path(d) / "B.log"
            log.write_text("WARN dfs.read_cache_install_failed old\n")
            start = self.run_script(str(log), 0, [])["size"]
            with log.open("a") as f:
                f.write("WARN dfs.read_cache_install_failed error=x\nINFO ok\n"
                        "ERROR DFS peer stayed busy until the read deadline: B\n"
                        "WARN dfs.read_cache_install_failed error=y\n")
            value = self.run_script(str(log), start, ["dfs.read_cache_install_failed", "peer stayed busy",
                                                      "dfs.read_cache_reconcile_retry"])
            self.assertEqual(value["counts"], {"dfs.read_cache_install_failed": 2, "peer stayed busy": 1,
                                               "dfs.read_cache_reconcile_retry": 0})
            self.assertEqual(value["size"], log.stat().st_size)
            self.assertFalse(value["rotated"])

    def test_missing_or_truncated_log_is_reported(self):
        with tempfile.TemporaryDirectory() as d:
            self.assertEqual(self.run_script(str(Path(d) / "none.log"), 0, ["x"]), None)
            log = Path(d) / "A.log"
            log.write_text("x\n")
            value = self.run_script(str(log), 100, ["x"])
            self.assertTrue(value["rotated"])
            self.assertEqual(value["counts"], {"x": 1})

    def test_suite_counts_each_node_between_two_marks(self):
        suite = load_suite()

        class Cluster:
            roles = ["M", "A", "B"]
            calls = []

            @staticmethod
            def root(role):
                return join("/run", role)

            def python(self, role, code, *args, owner=True, check=True):
                self.calls.append((role, args[0], args[1], owner))
                return {"stdout": json.dumps({"size": 10, "rotated": False,
                                              "counts": {e: 1 for e in json.loads(args[2])}})}

        c = Cluster()
        marks = suite.log_marks(c)
        self.assertEqual(marks, {"A": 10, "B": 10})
        counts = suite.log_event_counts(c, marks, ["peer stayed busy"])
        self.assertEqual(counts["B"], {"counts": {"peer stayed busy": 1}, "rotated": False, "missing": False})
        self.assertEqual({(r, p) for r, p, _, _ in c.calls}, {("A", "/run/A/A.log"), ("B", "/run/B/B.log")})
        self.assertFalse(any(owner for _, _, _, owner in c.calls))


def load_suite():
    import importlib.util

    spec = importlib.util.spec_from_file_location("bench_suite_sharing", TESTS / "suites/bench/test_bench.py")
    suite = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(suite)
    return suite


MEMORY_BACKEND = ({"medium": "memory"}, "remote_cold_memory_backend")


class StaggeredFanoutTests(unittest.TestCase):
    """Drive fanout_rounds with fakes: phases, gap and whole-round evidence."""

    @staticmethod
    def run_rounds(arrival, sent, **extra):
        suite = load_suite()
        cfg = bench(rounds=3, fanout_arrival=arrival, backend="memory", memory_backend_bytes=256 * MIB, **extra)
        params = benchlib.parameters(cfg)
        recorded, commands, sleeps = [], [], []
        scrape = {"sent": 0}

        class Channel:
            @staticmethod
            def call(action, **request):
                return {"returncode": 0}

        class Cluster:
            config = cfg
            roles = ["M", "A", "B", "C"]
            token = "t"
            uid = gid = 10001

            @staticmethod
            def record(name, value):
                recorded.append((name, value))
                return value

            @staticmethod
            def role_dir(role):
                return join("/run", role)

            root = role_dir

            @staticmethod
            def workspace(role):
                return join("/ws", role)

            @staticmethod
            def address(role):
                return "10.0.0." + role

            @staticmethod
            def channel(role):
                return Channel()

            @staticmethod
            def health(role, path):
                # Cumulative counters: A sends `sent` bytes for every two reader-file transfers.
                return (SEND % "grpc") + " %d\n" % (scrape["sent"] if role == "A" else 0)

            @staticmethod
            def command(role, argv, **kw):
                commands.append(argv)
                if "--hosts" in argv and "--quit" not in argv:
                    hosts = len(argv[argv.index("--hosts") + 1].split(","))
                    scrape["sent"] += sent * hosts // 2
                return {"stdout": "", "stderr": "", "returncode": 0}

            @staticmethod
            def python(role, code, *args, **kw):
                if "open(sys.argv[1]).read()" in code:
                    hosts = len(commands[-1][commands[-1].index("--hosts") + 1].split(","))
                    phase = {"phase_type": "READ", "config": {"version": "3.3", "hosts": hosts},
                             "last_done": {"bytes": params.file_bytes * hosts, "elapsed_time_ms": 1000}}
                    return {"stdout": json.dumps(phase) + "\n"}
                if "seek" in code:
                    return {"stdout": json.dumps({"size": 0, "rotated": False,
                                                  "counts": {e: 0 for e in json.loads(args[2])}})}
                return {"stdout": "x"}

        c = Cluster()
        with patch.object(suite, "write_file", return_value={"bytes": params.file_bytes, "mib_per_s": 1.0}), \
                patch.object(suite, "wait_visible"), patch.object(suite, "host_snapshot", return_value={}), \
                patch.object(suite, "host_evidence", return_value=({}, {})), \
                patch.object(suite, "backend_state", return_value=MEMORY_BACKEND), \
                patch.object(suite, "coverage_baseline", return_value=None), \
                patch.object(suite, "boot_id", side_effect=lambda c, r: r), patch.object(suite, "remove"), \
                patch.object(suite, "mkdir_owned"), patch.object(suite.time, "sleep", side_effect=sleeps.append):
            suite.fanout_rounds(c, params, ("B", "C"), "/tmp/l", "/opt/bench/elbencho", {})
        return recorded, commands, sleeps

    def test_stagger_runs_seed_alone_then_followers_after_the_gap(self):
        recorded, commands, sleeps = self.run_rounds({"mode": "stagger", "seed": "C", "gap_ms": 1500}, 128 * MIB)
        reads = [v for n, v in recorded if n == "bench.round" and v["op"] == "read"]
        self.assertEqual([r["phase"] for r in reads], ["seed", "followers"] * 3)
        self.assertEqual(sleeps, [1.5] * 3)
        runs = [argv for argv in commands if "--hosts" in argv and "--quit" not in argv]
        self.assertEqual([argv[argv.index("--hosts") + 1] for argv in runs[:2]],
                         ["10.0.0.C:26611", "10.0.0.B:26611"])
        self.assertEqual([r["readers"] for r in reads[:2]], [1, 1])
        whole = reads[1]["evidence"]
        self.assertEqual(whole["scope"], "whole_round")
        self.assertTrue(whole["send_check"]["ok"])
        self.assertEqual(whole["send_check"]["tool_bytes"], 128 * MIB)
        self.assertIn("log_events", whole)
        self.assertNotIn("send_check", reads[0]["evidence"])
        summary = [v for n, v in recorded if n == "bench.summary"][0]
        self.assertEqual({row["phase"] for row in summary["rows"] if row["op"] == "read"}, {"seed", "followers"})
        self.assertEqual(summary["arrival"], {"mode": "stagger", "seed": "C", "gap_ms": 1500})

    def test_together_keeps_one_read_per_round_and_fails_on_unreconciled_bytes(self):
        recorded, _, sleeps = self.run_rounds({"mode": "together"}, 128 * MIB)
        reads = [v for n, v in recorded if n == "bench.round" and v["op"] == "read"]
        self.assertEqual([r["phase"] for r in reads], ["all"] * 3)
        self.assertEqual(sleeps, [])
        self.assertTrue(reads[0]["evidence"]["send_check"]["ok"])
        self.assertIn("A", reads[0]["evidence"]["metrics"])
        self.assertEqual(reads[0]["evidence"]["writer_metrics"], reads[0]["evidence"]["metrics"]["A"])
        with self.assertRaises(AssertionError):
            self.run_rounds({"mode": "together"}, 64 * MIB)

    def test_stagger_with_reader_replicas_but_no_reader_evidence_is_unattributed(self):
        # This fake exposes no reader receive counters, so a send shortfall cannot be attributed.
        recorded, _, _ = self.run_rounds({"mode": "stagger", "seed": "B", "gap_ms": 0}, 0, variant="P3",
                                         dfs_desired_copies=3, dfs_sync_required_copies=1)
        reads = [v for n, v in recorded if n == "bench.round" and v["op"] == "read"]
        self.assertEqual([r["layer"] for r in reads], [
            "remote_cold_memory_backend_unattributed", "remote_post_seed_memory_backend_unattributed",
        ] * 3)
        self.assertIsNone(reads[1]["evidence"]["send_check"]["ok"])

    def test_memory_backend_labels_followers_as_post_seed(self):
        recorded, _, _ = self.run_rounds({"mode": "stagger", "seed": "B", "gap_ms": 0}, 128 * MIB)
        reads = [v for n, v in recorded if n == "bench.round" and v["op"] == "read"]
        self.assertEqual([r["layer"] for r in reads], [
            "remote_cold_memory_backend", "remote_post_seed_memory_backend",
        ] * 3)


if __name__ == "__main__":
    unittest.main()
