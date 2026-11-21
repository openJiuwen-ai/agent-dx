"""Tool contracts for the opt-in benchmark suite: parameters, argv, parsing, statistics.

These pin how industry tools (fio, elbencho, iperf3) are invoked and how their own
reports are read. They make no product or performance claim.
"""

import json
import os
from pathlib import Path, PurePosixPath
import platform
import sys
import tempfile
import unittest

TESTS = Path(__file__).resolve().parents[1]
sys.path.append(str(TESTS))
from fixtures.common import Blocked  # noqa: E402
from tools import benchlib  # noqa: E402

MIB = 1024**2


def bench_config(**overrides):
    value = {"file_bytes": 64 * MIB, "block_bytes": [MIB, 4 * MIB], "threads": [1, 4], "rounds": 5,
             "tools": {"fio": "/opt/bench/fio", "elbencho": "/opt/bench/elbencho"}}
    value.update(overrides)
    return {"bench": value}


class ParameterTests(unittest.TestCase):
    def test_declared_parameters_are_returned_typed(self):
        p = benchlib.parameters(bench_config())
        self.assertEqual((p.file_bytes, p.block_bytes, p.threads, p.rounds), (64 * MIB, (MIB, 4 * MIB), (1, 4), 5))
        self.assertEqual(p.tool("fio"), ("/opt/bench/fio", {}))

    def test_missing_section_is_blocked_not_defaulted(self):
        with self.assertRaises(Blocked):
            benchlib.parameters({})

    def test_invalid_values_are_blocked(self):
        for bad in ({"file_bytes": MIB + 1}, {"block_bytes": []}, {"block_bytes": [3000]},
                    {"threads": [0]}, {"rounds": 2}, {"tools": {"fio": "relative/fio"}},
                    {"file_bytes": 4 * MIB, "block_bytes": [8 * MIB]}):
            with self.subTest(bad=bad), self.assertRaises(Blocked):
                benchlib.parameters(bench_config(**bad))

    def test_file_must_split_evenly_across_threads(self):
        with self.assertRaises(Blocked):
            benchlib.parameters(bench_config(file_bytes=64 * MIB, threads=[3]))

    def test_tool_with_environment(self):
        cfg = bench_config(tools={"iperf3": {"path": "/opt/i/iperf3", "env": {"LD_LIBRARY_PATH": "/opt/i"}}})
        self.assertEqual(benchlib.parameters(cfg).tool("iperf3"), ("/opt/i/iperf3", {"LD_LIBRARY_PATH": "/opt/i"}))

    def test_undeclared_tool_is_blocked(self):
        with self.assertRaises(Blocked):
            benchlib.parameters(bench_config()).tool("iperf3")


class ArgvTests(unittest.TestCase):
    def test_fio_sequential_read_uses_one_shared_file_and_group_reporting(self):
        argv = benchlib.fio_argv("/opt/fio", "/m/f", "read", block=MIB, file_bytes=64 * MIB, threads=4)
        self.assertEqual(argv[0], "/opt/fio")
        for flag in ("--rw=read", "--bs=1048576", "--numjobs=4", "--group_reporting",
                     "--output-format=json", "--filename=/m/f", "--ioengine=psync", "--direct=0",
                     "--offset_increment=16777216", "--io_size=16777216", "--invalidate=0"):
            self.assertIn(flag, argv)

    def test_fio_job_regions_tile_the_file_without_passing_its_end(self):
        # fio lays out (writes) a file shorter than offset+size before reading it; each job's
        # region must therefore be exactly its own slice so no job reaches past EOF.
        argv = benchlib.fio_argv("/opt/fio", "/m/f", "read", block=MIB, file_bytes=64 * MIB, threads=4)

        def value(name):
            return int(next(a for a in argv if a.startswith("--" + name + "=")).split("=")[1])

        share = value("size")
        self.assertEqual(share, 16 * MIB)
        self.assertEqual(value("offset_increment"), share)
        self.assertEqual(value("io_size"), share)
        self.assertEqual((value("numjobs") - 1) * value("offset_increment") + share, 64 * MIB)
        self.assertIn("--allow_file_create=0", argv)

    def test_fio_write_may_create_the_file(self):
        argv = benchlib.fio_argv("/f", "/m/x", "write", block=MIB, file_bytes=8 * MIB, threads=1)
        self.assertIn("--size=8388608", argv)
        self.assertNotIn("--allow_file_create=0", argv)

    def test_fio_write_fsync_is_explicit(self):
        with_sync = benchlib.fio_argv("/f", "/m/x", "write", block=MIB, file_bytes=8 * MIB, threads=1, end_fsync=True)
        without = benchlib.fio_argv("/f", "/m/x", "write", block=MIB, file_bytes=8 * MIB, threads=1, end_fsync=False)
        self.assertIn("--end_fsync=1", with_sync)
        self.assertIn("--end_fsync=0", without)

    def test_fio_rejects_unknown_operation(self):
        with self.assertRaises(ValueError):
            benchlib.fio_argv("/f", "/m/x", "randrw", block=MIB, file_bytes=MIB, threads=1)

    def test_elbencho_fanout_reads_full_file_on_every_service_host(self):
        argv = benchlib.elbencho_read_argv("/opt/elbencho", ["10.0.0.2:1611", "10.0.0.3:1611"], "/m/f",
                                           block=4 * MIB, file_bytes=64 * MIB, threads=2, result="/r/out.json")
        self.assertEqual(argv[0], "/opt/elbencho")
        for flag in ("--hosts", "10.0.0.2:1611,10.0.0.3:1611", "--nosvcshare", "-r", "--nolive",
                     "--jsonfile", "/r/out.json"):
            self.assertIn(flag, argv)
        self.assertEqual(argv[-1], "/m/f")
        self.assertNotIn("-w", argv)


class ParseTests(unittest.TestCase):
    def test_fio_report_bytes_and_time_come_from_the_tool(self):
        job = {"read": {"io_bytes": 64 * MIB, "runtime": 500, "bw_bytes": 134217728},
               "write": {"io_bytes": 0, "runtime": 0, "bw_bytes": 0}}
        report = {"fio version": "fio-3.43", "jobs": [job]}
        r = benchlib.parse_fio(json.dumps(report), "read")
        self.assertEqual((r["bytes"], r["seconds"], r["tool_version"]), (64 * MIB, 0.5, "fio-3.43"))
        self.assertAlmostEqual(r["mib_per_s"], 128.0)

    def test_fio_report_without_grouping_is_rejected(self):
        report = {"jobs": [{"read": {"io_bytes": 1, "runtime": 1, "bw_bytes": 1}}] * 2}
        with self.assertRaises(ValueError):
            benchlib.parse_fio(json.dumps(report), "read")

    def test_fio_report_with_leading_noise_is_parsed(self):
        report = {"fio version": "fio-3.43", "jobs": [{"read": {"io_bytes": MIB, "runtime": 1000, "bw_bytes": MIB}}]}
        r = benchlib.parse_fio("note: something\n" + json.dumps(report), "read")
        self.assertEqual(r["bytes"], MIB)

    def test_elbencho_report_uses_last_done_of_the_read_phase(self):
        phases = [
            {"phase_type": "WRITE", "config": {"version": "3.3-3"},
             "last_done": {"bytes": "1", "elapsed_time_ms": "1"}},
            {"phase_type": "READ", "config": {"version": "3.3-3", "hosts": "2"},
             "last_done": {"bytes": str(128 * MIB), "elapsed_time_ms": "1000", "bytes/s": str(128 * MIB)}},
        ]
        # elbencho --jsonfile writes one JSON object per phase per line (JSON Lines), see
        # a real multi-host report captured on 2026-11-17 in features/afs-perf.
        r = benchlib.parse_elbencho("\n".join(json.dumps(p) for p in phases) + "\n", "READ")
        self.assertEqual((r["bytes"], r["seconds"], r["hosts"], r["tool_version"]), (128 * MIB, 1.0, 2, "3.3-3"))
        self.assertAlmostEqual(r["mib_per_s"], 128.0)

    def test_elbencho_single_phase_report_is_one_line(self):
        line = json.dumps({"phase_type": "READ", "config": {"version": "3.3-3", "hosts": "2"},
                           "last_done": {"bytes": "134217728", "elapsed_time_ms": "102", "bytes/s": "1308330763"}})
        r = benchlib.parse_elbencho(line, "READ")
        self.assertEqual((r["bytes"], r["hosts"]), (134217728, 2))

    def test_elbencho_report_without_the_phase_is_rejected(self):
        with self.assertRaises(ValueError):
            benchlib.parse_elbencho("", "READ")

    def test_iperf3_report(self):
        report = {"start": {"version": "iperf 3.16"},
                  "end": {"sum_received": {"bytes": 10 * MIB, "seconds": 10.0, "bits_per_second": 8 * MIB},
                          "sum_sent": {"retransmits": 3}}}
        r = benchlib.parse_iperf3(json.dumps(report))
        self.assertAlmostEqual(r["mib_per_s"], 1.0)
        self.assertEqual(r["retransmits"], 3)


class DiskstatsTests(unittest.TestCase):
    def test_delta_reports_bytes_per_changed_device(self):
        before = "   8       0 vda 10 0 100 0 20 0 200 0 0 0 0\n   8       1 vda1 1 0 8 0 0 0 0 0 0 0 0\n"
        after = "   8       0 vda 12 0 2148 0 20 0 4296 0 0 0 0\n   8       1 vda1 1 0 8 0 0 0 0 0 0 0 0\n"
        delta = benchlib.diskstats_delta(before, after)
        self.assertEqual(delta, {"vda": {"read_bytes": 2048 * 512, "write_bytes": 4096 * 512}})


def measured(*files, page_size=4096):
    entries = [{"path": p, "bytes": b, "resident_bytes": r} for p, b, r in files]
    return {"files": entries, "bytes": sum(f["bytes"] for f in entries),
            "resident_bytes": sum(f["resident_bytes"] for f in entries), "errors": [], "page_size": page_size}


class ResidencyVerdictTests(unittest.TestCase):
    """Verify the 64-page floor for small metadata trees."""

    def test_tiny_tree_with_one_cached_page_is_within_the_absolute_floor(self):
        v = benchlib.residency_verdict(measured(("/s/a", 20, 0), ("/s/b", 134, 134), ("/s/c", 0, 0)))
        self.assertTrue(v["ok"])
        self.assertEqual(v["allowed_bytes"], 64 * 4096)
        self.assertEqual(v["resident_files"], [{"path": "/s/b", "bytes": 134, "resident_bytes": 134}])

    def test_large_tree_uses_the_relative_bound(self):
        tree = measured(("/s/chunk", 3 * 1024**3, 40 * MIB))
        v = benchlib.residency_verdict(tree)
        self.assertFalse(v["ok"])
        self.assertEqual(v["allowed_bytes"], int(0.01 * 3 * 1024**3))

    def test_resident_files_are_listed_largest_first_and_bounded(self):
        files = [("/s/f%02d" % i, 8192, 4096 * (i % 3)) for i in range(30)]
        v = benchlib.residency_verdict(measured(*files))
        self.assertEqual(len(v["resident_files"]), 10)
        sizes = [f["resident_bytes"] for f in v["resident_files"]]
        self.assertEqual(sizes, sorted(sizes, reverse=True))

    def test_page_size_comes_from_the_probe(self):
        v = benchlib.residency_verdict(measured(("/s/a", 10, 10), page_size=65536))
        self.assertEqual(v["allowed_bytes"], 64 * 65536)


class BackendVerdictTests(unittest.TestCase):
    """Verify that the metadata page floor never widens the payload limit."""

    def test_partly_cached_payload_is_not_cold_even_on_a_tiny_tree(self):
        tree = measured(("/s/dfs/chunks/c1", MIB, 256 * 1024), ("/s/dfs/device_epoch", 20, 0))
        v = benchlib.backend_verdict(tree, file_bytes=MIB)
        self.assertFalse(v["payload"]["ok"])
        self.assertTrue(v["metadata"]["ok"])

    def test_large_page_size_does_not_widen_the_payload_bound(self):
        tree = measured(("/s/dfs/chunks/c1", MIB, MIB), page_size=65536)
        v = benchlib.backend_verdict(tree, file_bytes=MIB)
        self.assertFalse(v["payload"]["ok"])
        self.assertEqual(v["payload"]["allowed_bytes"], MIB // 100)

    def test_rewritten_metadata_keeps_its_page_tolerance(self):
        tree = measured(("/s/dfs/catalog.snapshot", 134, 134), ("/s/dfs/chunks/c1", 64 * MIB, 0))
        v = benchlib.backend_verdict(tree, file_bytes=64 * MIB)
        self.assertTrue(v["payload"]["ok"] and v["metadata"]["ok"])
        self.assertEqual(v["metadata"]["resident_files"][0]["path"], "/s/dfs/catalog.snapshot")

    def test_payload_within_one_percent_of_the_file_is_cold(self):
        tree = measured(("/s/dfs/chunks/c1", 1024 * MIB, 8 * MIB))
        self.assertTrue(benchlib.backend_verdict(tree, file_bytes=1024 * MIB)["payload"]["ok"])


class CachedVerdictTests(unittest.TestCase):
    def test_fully_cached_file_is_hot(self):
        self.assertTrue(benchlib.cached_verdict(measured(("/m/f", MIB, MIB)), MIB)["ok"])

    def test_probe_error_or_partial_cache_is_not_hot(self):
        failed = dict(measured(), errors=[{"path": "/m/f", "errno": 19, "error": "No such device"}])
        self.assertFalse(benchlib.cached_verdict(failed, MIB)["ok"])
        self.assertFalse(benchlib.cached_verdict(measured(("/m/f", MIB, MIB // 2)), MIB)["ok"])


def load_suite():
    import importlib.util

    spec = importlib.util.spec_from_file_location("bench_suite", TESTS / "suites/bench/test_bench.py")
    suite = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(suite)
    return suite


class EvictBackendsTests(unittest.TestCase):
    """At most one documented re-evict + re-measure; both attempts are kept as evidence."""

    @staticmethod
    def run_with(results, file_bytes=64 * MIB, source="A"):
        from unittest.mock import patch

        suite = load_suite()
        calls = []

        def probe(_c, role, action, *_paths):
            calls.append((role, action))
            return {"files": 0, "bytes": 0, "errors": []} if action == "evict" else results[role].pop(0)

        class Cluster:

            @staticmethod
            def role_dir(role):
                return str(PurePosixPath("/r") / role)

        with patch.object(suite, "probe", side_effect=probe):
            try:
                return suite.evict_backends(Cluster(), list(results), file_bytes, source), calls, None
            except AssertionError as error:
                return None, calls, error

    @staticmethod
    def source(resident=0):
        return measured(("/r/A/state/dfs/chunks/c1", 64 * MIB, resident))

    def test_clean_eviction_is_cold_without_retry(self):
        (evidence, cold), calls, error = self.run_with({"A": [self.source()],
                                                       "B": [measured(("/r/B/state/dfs/catalog.snapshot", 134, 134))]})
        self.assertIsNone(error)
        self.assertTrue(cold)
        self.assertEqual(calls, [("A", "evict"), ("A", "resident"), ("B", "evict"), ("B", "resident")])
        self.assertEqual(evidence["B"]["attempts"], 1)

    def test_payload_cached_after_retry_marks_the_round_unverified_without_failing(self):
        (evidence, cold), calls, error = self.run_with({"A": [self.source(8 * MIB), self.source(8 * MIB)]})
        self.assertIsNone(error)
        self.assertFalse(cold)
        self.assertEqual(len(calls), 4)
        self.assertEqual(evidence["A"]["attempts"], 2)
        self.assertFalse(evidence["A"]["payload"]["ok"])

    def test_payload_cleared_by_the_retry_is_cold(self):
        (evidence, cold), _, error = self.run_with({"A": [self.source(8 * MIB), self.source(0)]})
        self.assertIsNone(error)
        self.assertTrue(cold)
        self.assertEqual(evidence["A"]["first_attempt"]["payload"]["resident_bytes"], 8 * MIB)

    def test_metadata_still_over_tolerance_after_retry_fails_with_file_names(self):
        big = measured(("/r/A/state/dfs/chunks/c1", 64 * MIB, 0), ("/r/A/state/dfs/catalog.wal", 8 * MIB, 8 * MIB))
        _, calls, error = self.run_with({"A": [big, big]})
        self.assertEqual(len(calls), 4)
        self.assertIn("catalog.wal", str(error))

    def test_source_without_recognised_payload_fails_as_layout_drift(self):
        flat = measured(("/r/A/state/data.bin", 64 * MIB, 0))
        _, _, error = self.run_with({"A": [flat]})
        self.assertIn("payload", str(error))

    @staticmethod
    def vanished(measurement, *paths):
        """A probe result whose listed files vanished (node GC) before they could be opened."""
        return dict(measurement, errors=[{"path": p, "errno": 2, "error": "No such file or directory"} for p in paths])

    def test_files_reclaimed_between_listing_and_open_count_as_absent(self):
        from unittest.mock import patch

        suite = load_suite()
        gone = "/r/A/state/dfs/chunks/old"

        def probe(_c, role, action, *_paths):
            if action == "evict":
                return self.vanished({"files": 1, "bytes": 64 * MIB}, gone)
            return self.vanished(self.source(), gone)

        class Cluster:
            @staticmethod
            def role_dir(role):
                return "/r/" + role

        with patch.object(suite, "probe", side_effect=probe):
            evidence, cold = suite.evict_backends(Cluster(), ["A"], 64 * MIB, "A")
        self.assertTrue(cold)
        self.assertEqual(evidence["A"]["enoent"], {"evict": 1, "resident": 1})
        self.assertEqual(evidence["A"]["payload"]["resident_bytes"], 0)

    def test_other_probe_errors_still_fail(self):
        from unittest.mock import patch

        suite = load_suite()

        def probe(_c, role, action, *_paths):
            result = {"files": 0, "bytes": 0} if action == "evict" else self.source()
            return dict(result, errors=[{"path": "/r/A/state/x", "errno": 13, "error": "Permission denied"}])

        class Cluster:
            @staticmethod
            def role_dir(role):
                return "/r/" + role

        with patch.object(suite, "probe", side_effect=probe), self.assertRaises(AssertionError) as raised:
            suite.evict_backends(Cluster(), ["A"], 64 * MIB, "A")
        self.assertIn("evidence incomplete", str(raised.exception))

    def test_default_evidence_is_unchanged_without_vanished_files(self):
        (evidence, _), _, _ = self.run_with({"A": [self.source()]})
        self.assertEqual(set(evidence), {"A"})
        self.assertEqual(set(evidence["A"]), {"payload", "metadata", "attempts"})

    @staticmethod
    def stripe(role, size, resident=0):
        return measured(("/r/%s/state/dfs/chunks/%s" % (role, role.lower()), size, resident))

    def test_striped_payload_is_judged_across_every_node(self):
        holdings = {"A": 13 * MIB, "B": 17 * MIB, "C": 16 * MIB, "D": 9 * MIB, "E": 9 * MIB}
        (evidence, cold), calls, error = self.run_with({r: [self.stripe(r, b)] for r, b in holdings.items()})
        self.assertIsNone(error)
        self.assertTrue(cold)
        self.assertEqual(evidence["placement"], {"source": "A", "source_bytes": 13 * MIB,
                                                 "total_bytes": 64 * MIB, "by_role": holdings})
        # Every Node evicts and measures its own share.
        self.assertEqual(sorted({r for r, a in calls if a == "evict"}), sorted(holdings))

    def test_a_cached_stripe_on_any_node_makes_the_round_unverified(self):
        results = {"A": [self.stripe("A", 32 * MIB)], "B": [self.stripe("B", 32 * MIB, 8 * MIB)] * 2}
        (_, cold), _, error = self.run_with(results)
        self.assertIsNone(error)
        self.assertFalse(cold)

    def test_stripes_short_of_the_file_still_fail_as_layout_drift(self):
        _, _, error = self.run_with({"A": [self.stripe("A", 16 * MIB)], "B": [self.stripe("B", 16 * MIB)]})
        self.assertIn("payload layout not recognised", str(error))
        self.assertIn("'B': %d" % (16 * MIB), str(error))


class FanoutTopologyTests(unittest.TestCase):
    def test_readers_sharing_one_host_are_blocked_before_any_work(self):
        suite = load_suite()
        config = {"roles": {"B": {"host": "vm"}, "C": {"host": "vm"}}}
        with self.assertRaises(Blocked):
            suite.require_separate_readers(config, ("B", "C"))

    def test_readers_on_separate_hosts_are_accepted_regardless_of_base(self):
        suite = load_suite()
        suite.require_separate_readers({"roles": {"B": {"host": "ns-b"}, "C": {"host": "ns-c"}}}, ("B", "C"))


class HostEvidenceTests(unittest.TestCase):
    def test_pressure_parses_some_and_full_averages(self):
        text = ("some avg10=1.50 avg60=0.75 avg300=0.10 total=123\n"
                "full avg10=0.25 avg60=0.05 avg300=0.00 total=45\n")
        self.assertEqual(benchlib.parse_pressure(text),
                         {"some_avg10": 1.5, "some_avg60": 0.75, "full_avg10": 0.25, "full_avg60": 0.05})

    def test_missing_pressure_is_none_not_zero(self):
        self.assertIsNone(benchlib.parse_pressure(""))

    def test_loadavg_keeps_the_three_averages(self):
        self.assertEqual(benchlib.parse_loadavg("1.18 2.76 2.08 3/900 12345\n"), [1.18, 2.76, 2.08])

    def test_host_window_summarises_before_and_after(self):
        before = {"time": "2026-11-18T01:04:24Z", "loadavg": "0.50 0.40 0.30 1/1 1\n",
                  "psi_cpu": "some avg10=0.00 avg60=0.00 avg300=0.00 total=1\n",
                  "psi_io": ("some avg10=0.00 avg60=0.00 avg300=0.00 total=1\n"
                             "full avg10=0.00 avg60=0.00 avg300=0.00 total=1\n")}
        after = dict(before, time="2026-11-18T01:04:28Z", loadavg="3.00 1.00 0.40 2/1 1\n",
                     psi_io=("some avg10=40.00 avg60=9.00 avg300=1.00 total=9\n"
                             "full avg10=30.00 avg60=7.00 avg300=1.00 total=9\n"))
        w = benchlib.host_window(before, after)
        self.assertEqual((w["start"], w["end"]), ("2026-11-18T01:04:24Z", "2026-11-18T01:04:28Z"))
        self.assertEqual(w["loadavg_after"], [3.0, 1.0, 0.4])
        self.assertEqual(w["psi_io_after"]["some_avg10"], 40.0)
        self.assertEqual(w["psi_cpu_after"]["some_avg10"], 0.0)


class StatisticsTests(unittest.TestCase):
    def test_summary_keeps_every_sample_and_spread(self):
        s = benchlib.summarize([10.0, 12.0, 11.0, 30.0, 11.5])
        self.assertEqual(s["n"], 5)
        self.assertEqual(s["median"], 11.5)
        self.assertEqual((s["min"], s["max"]), (10.0, 30.0))
        self.assertAlmostEqual(s["max_over_min"], 3.0)
        self.assertEqual(s["samples"], [10.0, 12.0, 11.0, 30.0, 11.5])

    def test_empty_summary_is_rejected(self):
        with self.assertRaises(ValueError):
            benchlib.summarize([])


@unittest.skipUnless(platform.system() == "Linux", "page-cache residency needs Linux mincore/posix_fadvise")
class ResidencyProbeTests(unittest.TestCase):
    def test_evict_then_read_changes_measured_residency(self):
        probe = TESTS / "tools/bench_probe.py"
        import subprocess

        with tempfile.TemporaryDirectory(dir="/dev/shm" if Path("/dev/shm").is_dir() else None) as d:
            path = Path(d) / "f"
            path.write_bytes(b"x" * (4 * MIB))

            def run(*args):
                return json.loads(subprocess.check_output([sys.executable, str(probe), *args]))

            evicted = run("evict", d)
            self.assertEqual(evicted["files"], 1)
            resident = run("resident", str(path))
            self.assertEqual(resident["files"][0]["bytes"], 4 * MIB)
            self.assertIn("resident_bytes", resident["files"][0])
            self.assertEqual(resident["page_size"], os.sysconf("SC_PAGE_SIZE"))


if __name__ == "__main__":
    unittest.main()
