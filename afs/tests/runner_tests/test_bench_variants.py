"""Tool contracts for verification-measurement inputs: variants, memory backend, readers, evidence.

They pin how a typed bench variant becomes Node configuration and how its effect is proven
(--print-config), how a memory backend is declared, how many fan-out readers are selected,
and how writer metrics and cgroup memory are read. No product or performance claim.
"""

import json
from pathlib import Path
import sys
import unittest

TESTS = Path(__file__).resolve().parents[1]
sys.path.append(str(TESTS))
from fixtures.common import Blocked, Case  # noqa: E402
from tools import benchlib  # noqa: E402

MIB = 1024**2


def bench(**extra):
    value = {"file_bytes": 64 * MIB, "block_bytes": [MIB], "threads": [1], "rounds": 3,
             "tools": {"fio": "/opt/bench/fio"}}
    value.update(extra)
    return {"bench": value}


class VariantParameterTests(unittest.TestCase):
    def test_defaults_keep_existing_env_files_valid(self):
        p = benchlib.parameters(bench())
        self.assertEqual((p.backend, p.variant, p.node_overrides, p.readers), ("disk", "V0", {}, 2))

    def test_typed_overrides_per_role_and_for_all_nodes(self):
        p = benchlib.parameters(bench(variant="V1", node_overrides={
            "*": {"dfs_read_crc_mode": "receiver"}, "B": {"dfs_read_pipeline.max_rpcs": 16}}))
        self.assertEqual(p.overrides_for("B"), {"dfs_read_crc_mode": "receiver", "dfs_read_pipeline.max_rpcs": 16})
        self.assertEqual(p.overrides_for("A"), {"dfs_read_crc_mode": "receiver"})

    def test_overrides_need_an_explicit_non_baseline_label(self):
        with self.assertRaises(Blocked):
            benchlib.parameters(bench(node_overrides={"B": {"dfs_read_parallelism": 4}}))

    def test_unknown_keys_roles_and_out_of_range_values_are_blocked(self):
        for bad in ({"B": {"dfs_write_queue_bytes": 1}}, {"M": {"dfs_read_parallelism": 4}},
                    {"Z": {"dfs_read_parallelism": 4}}, {"B": {"dfs_read_parallelism": 0}},
                    {"B": {"dfs_readahead_max_bytes": 64 * MIB}}, {"B": {"dfs_read_crc_mode": "both"}},
                    {"B": {"dfs_read_pipeline.max_rpcs": True}}, {"B": "x"}):
            with self.subTest(bad=bad), self.assertRaises(Blocked):
                benchlib.parameters(bench(variant="Vx", node_overrides=bad))

    def test_memory_backend_requires_a_size(self):
        with self.assertRaises(Blocked):
            benchlib.parameters(bench(backend="memory"))
        p = benchlib.parameters(bench(backend="memory", memory_backend_bytes=4096 * MIB))
        self.assertEqual((p.backend, p.memory_backend_bytes), ("memory", 4096 * MIB))
        with self.assertRaises(Blocked):
            benchlib.parameters(bench(backend="ssd"))

    def test_readers_are_one_to_four(self):
        self.assertEqual(benchlib.parameters(bench(readers=4)).reader_roles, ("B", "C", "D", "E"))
        self.assertEqual(benchlib.parameters(bench(readers=1)).reader_roles, ("B",))
        for bad in (0, 5, "2"):
            with self.subTest(bad=bad), self.assertRaises(Blocked):
                benchlib.parameters(bench(readers=bad))


class OverrideRenderingTests(unittest.TestCase):
    def test_top_level_keys_precede_tables_and_peer_cache_gets_its_directory(self):
        toml = benchlib.node_overrides_toml({"dfs_read_parallelism": 4, "dfs_read_crc_mode": "off",
                                             "dfs_read_pipeline.max_rpcs": 16,
                                             "dfs_peer_cache.capacity_bytes": 1024 * MIB}, "/r/node-b/peer-cache")
        lines = toml.strip().splitlines()
        first_table = next(i for i, line in enumerate(lines) if line.startswith("["))
        self.assertIn('dfs_read_crc_mode = "off"', lines[:first_table])
        self.assertIn("dfs_read_parallelism = 4", lines[:first_table])
        self.assertIn("[dfs_read_pipeline]", lines)
        self.assertIn("max_rpcs = 16", lines)
        self.assertIn("[dfs_peer_cache]", lines)
        self.assertIn('directory = "/r/node-b/peer-cache"', lines)
        self.assertIn("capacity_bytes = %d" % (1024 * MIB), lines)

    def test_no_overrides_render_nothing(self):
        self.assertEqual(benchlib.node_overrides_toml({}, "/unused"), "")

    def test_effective_configuration_must_show_every_declared_value(self):
        effective = {"dfs_read_parallelism": 4, "dfs_read_pipeline": {"max_rpcs": 16, "max_disk_reads": 2},
                     "dfs_peer_cache": None}
        self.assertEqual(benchlib.effective_mismatches(
            effective, {"dfs_read_parallelism": 4, "dfs_read_pipeline.max_rpcs": 16}), [])
        missing = benchlib.effective_mismatches(effective, {"dfs_peer_cache.capacity_bytes": MIB,
                                                            "dfs_read_parallelism": 8})
        self.assertEqual({m["key"] for m in missing}, {"dfs_peer_cache.capacity_bytes", "dfs_read_parallelism"})


class EvidenceParsingTests(unittest.TestCase):
    def test_metrics_delta_keeps_changed_matching_samples(self):
        before = ("# HELP afs_x x\nafs_dfs_peer_read_rpcs_total 10\n"
                  'afs_dfs_read_slot_rejected_total{reason="wait"} 1\nafs_fuse_requests_total 5\n')
        after = ("afs_dfs_peer_read_rpcs_total 14\n"
                 'afs_dfs_read_slot_rejected_total{reason="wait"} 1\n'
                 "afs_fuse_requests_total 9\nafs_dfs_gc_collected 3\n")
        delta = benchlib.metrics_delta(before, after, r"peer|slot|gc")
        self.assertEqual(delta, {"afs_dfs_peer_read_rpcs_total": 4.0, "afs_dfs_gc_collected": 3.0})

    def test_kernel_memory_and_oom_kills(self):
        # Role namespaces see no cgroup files (A2 smoke 2026-11-19); kernel-wide counters are visible.
        m = benchlib.parse_kernel_memory("pgfault 9\noom_kill 2\n", "MemTotal: 10 kB\nMemAvailable:   300 kB\n")
        self.assertEqual(m, {"mem_available": 300 * 1024, "oom_kill": 2})
        self.assertEqual(benchlib.parse_kernel_memory("", ""), {"mem_available": None, "oom_kill": None})


class ReaderTopologyTests(unittest.TestCase):
    FANOUT = Case("bench.dfs.fanout.many_readers", ("bench-dfs",), "dfs", (), None, purpose="performance")

    def config(self, readers):
        roles = {r: {"host": "h-" + r.lower(), "address": "10.0.0.%d" % (i + 10)} for i, r in enumerate("MABCDE")}
        hosts = {"h-" + r.lower(): {"transport": "exec", "command_prefix": ["true"], "base": "/b" + r}
                 for r in "MABCDE"}
        return dict(bench(readers=readers), kind="namespaces", uid=10001, gid=10001, other_uid=10002,
                    other_gid=10002, roles=roles, hosts=hosts)

    def test_fanout_cluster_uses_exactly_the_declared_readers(self):
        from fixtures.multivm import Cluster

        for readers, expected in ((1, ["M", "A", "B"]), (2, ["M", "A", "B", "C"]), (4, list("MABCDE"))):
            with self.subTest(readers=readers):
                self.assertEqual(Cluster(self.config(readers), "/x", "/x", self.FANOUT, "t").roles, expected)

    def test_validate_requires_the_reader_roles(self):
        from fixtures.multivm import validate

        cfg = self.config(4)
        del cfg["roles"]["E"]
        with self.assertRaises(Blocked):
            validate(cfg, [self.FANOUT])
        self.assertEqual(len(validate(self.config(1), [self.FANOUT])), 3)


class FakeChannel:
    """Records worker calls; answers --print-config with a fixed effective configuration."""

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


class ClusterConfigTests(unittest.TestCase):
    SEQ = Case("bench.dfs.sequential", ("bench-dfs",), "dfs", (), None, purpose="performance")

    def cluster(self, directory, effective, **bench_extra):
        from fixtures.multivm import Cluster

        roles = {r: {"host": "h-" + r.lower(), "address": "10.0.0.%d" % (i + 10)} for i, r in enumerate("MAB")}
        config = dict(bench(**bench_extra), kind="namespaces", uid=10001, gid=10001, other_uid=10002,
                      other_gid=10002, roles=roles, hosts={})
        c = Cluster(config, directory, directory, self.SEQ, "t")
        c.channels = {"h-" + r.lower(): FakeChannel(effective) for r in "MAB"}
        c.roots = {"h-" + r.lower(): "/run/" + r for r in "MAB"}
        return c

    def test_overrides_reach_only_their_role_and_are_proven_by_print_config(self):
        import tempfile

        with tempfile.TemporaryDirectory() as d:
            c = self.cluster(d, {"dfs_read_pipeline": {"max_rpcs": 16}}, variant="V1",
                             node_overrides={"B": {"dfs_read_pipeline.max_rpcs": 16}})
            c.write_configs()
            self.assertIn("max_rpcs = 16", (Path(d) / "config-B.toml").read_text())
            self.assertNotIn("max_rpcs", (Path(d) / "config-A.toml").read_text())

    def test_override_missing_from_effective_configuration_fails(self):
        import tempfile

        with tempfile.TemporaryDirectory() as d:
            c = self.cluster(d, {"dfs_read_pipeline": {"max_rpcs": 4}}, variant="V1",
                             node_overrides={"B": {"dfs_read_pipeline.max_rpcs": 16}})
            with self.assertRaises(AssertionError):
                c.write_configs()

    def test_memory_backend_mounts_tmpfs_on_node_state_and_cleanup_unmounts_it(self):
        import tempfile

        with tempfile.TemporaryDirectory() as d:
            c = self.cluster(d, {}, backend="memory", memory_backend_bytes=1024 * MIB)
            c.write_configs()
            mounts = [(role, req["argv"]) for role in "AB" for action, req in c.channel(role).calls
                      if action == "command" and req["argv"][0] == "mount"]
            self.assertEqual([r for r, _ in mounts], ["A", "B"])
            self.assertEqual(mounts[0][1][:4], ["mount", "-t", "tmpfs", "-o"])
            self.assertTrue(mounts[0][1][-1].endswith("/node-a/state"))
            self.assertFalse(any(req.get("argv", [""])[0] == "mount" for _, req in c.channel("M").calls))
            c.unmount_memory_backends([])
            unmounts = [req["argv"] for role in "AB" for action, req in c.channel(role).calls
                        if action == "command" and req["argv"][0] == "umount"]
            self.assertEqual(len(unmounts), 2)


def load_suite():
    import importlib.util

    spec = importlib.util.spec_from_file_location("bench_suite_v", TESTS / "suites/bench/test_bench.py")
    suite = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(suite)
    return suite


class SuiteMemoryBackendTests(unittest.TestCase):
    def test_memory_backend_is_not_probed_and_is_labelled(self):
        from unittest.mock import patch

        suite = load_suite()
        params = benchlib.parameters(bench(backend="memory", memory_backend_bytes=1024 * MIB))
        with patch.object(suite, "probe", side_effect=AssertionError("must not probe a memory backend")):
            evidence, layer = suite.backend_state(None, params, ["A", "B"], "A")
        self.assertEqual(layer, "remote_cold_memory_backend")
        self.assertEqual(evidence, {"medium": "memory", "roles": ["A", "B"]})

    def test_disk_layers_follow_the_eviction_verdict(self):
        from unittest.mock import patch

        suite = load_suite()
        params = benchlib.parameters(bench())
        with patch.object(suite, "evict_backends", return_value=({"A": {}}, False)):
            self.assertEqual(suite.backend_state(None, params, ["A"], "A")[1], "remote_cold_unverified")
        with patch.object(suite, "evict_backends", return_value=({"A": {}}, True)):
            self.assertEqual(suite.backend_state(None, params, ["A"], "A")[1], "remote_cold")

    def test_local_upper_bound_refuses_a_memory_backend(self):
        suite = load_suite()

        class Cluster:
            config = bench(backend="memory", memory_backend_bytes=1024 * MIB)

        with self.assertRaises(Blocked):
            suite.localfs(Cluster())

    def test_oom_kill_during_a_round_fails_it(self):
        suite = load_suite()
        suite.require_no_oom({"A": {"oom_kill_delta": 0}, "B": {"oom_kill_delta": 0}})
        with self.assertRaises(AssertionError):
            suite.require_no_oom({"A": {"oom_kill_delta": 1}})

    def test_summary_rows_carry_variant_and_backend(self):
        suite = load_suite()
        recorded = {}

        class Cluster:
            config = bench(variant="V2", backend="memory", memory_backend_bytes=1024 * MIB,
                           node_overrides={"B": {"dfs_read_parallelism": 4}})
            roles = ["M", "A", "B"]

            def record(self, name, value):
                recorded[name] = value
                return value

        suite.summaries(Cluster(), [{"op": "read", "layer": "hot", "tool": "fio", "block_bytes": MIB, "threads": 1,
                                     "result": {"mib_per_s": 10.0}}] * 3)
        row = recorded["bench.summary"]
        self.assertEqual((row["variant"], row["backend"]), ("V2", "memory"))
        self.assertEqual(row["node_overrides"], {"B": {"dfs_read_parallelism": 4}})


if __name__ == "__main__":
    unittest.main()
