"""Tool contracts for concurrent fan-out evidence: who supplied each reader, per-reader completion
and progress from elbencho, and which roles share a kernel.

They pin how existing Node counters and elbencho's own reports are read. No product or performance claim.
"""

import json
import sys
import unittest
from pathlib import Path
from posixpath import join
from unittest.mock import patch

TESTS = Path(__file__).resolve().parents[1]
sys.path.append(str(TESTS))
from fixtures.common import Blocked  # noqa: E402
from tools import benchlib  # noqa: E402

MIB = 1024**2
SOURCE = "afs_dfs_read_source_bytes_total"


def scrape(**roles):
    """One /metrics text with afs_dfs_read_source_bytes_total{role=...} samples."""
    return "".join('%s{role="%s"} %d\n' % (SOURCE, role, value) for role, value in roles.items())


def bench(**extra):
    value = {"file_bytes": 64 * MIB, "block_bytes": [MIB], "threads": [1], "rounds": 3,
             "tools": {"fio": "/opt/bench/fio", "elbencho": "/opt/bench/elbencho"}}
    value.update(extra)
    return {"bench": value}


class LabelledWindowTests(unittest.TestCase):
    def test_deltas_are_kept_per_label_value(self):
        window = benchlib.labelled_counter_window(scrape(local=0, durable=10), scrape(local=4, durable=30),
                                                  SOURCE, "role")
        self.assertEqual(window, {"status": "complete", "by": {"local": 4, "durable": 20}})

    def test_a_missing_or_different_sample_set_is_missing_not_zero(self):
        self.assertEqual(benchlib.labelled_counter_window("", scrape(durable=1), SOURCE, "role")["status"],
                         "missing")
        window = benchlib.labelled_counter_window(scrape(durable=1), scrape(durable=1, peer_holder=3),
                                                  SOURCE, "role")
        self.assertEqual(window["status"], "missing")

    def test_a_sample_of_the_family_without_the_label_makes_the_window_missing(self):
        for unlabelled in ("%s 0\n", '%s{stage="x"} 0\n'):
            with self.subTest(unlabelled=unlabelled):
                before = scrape(durable=0) + unlabelled % SOURCE
                after = scrape(durable=8) + unlabelled.replace(" 0", " 8") % SOURCE
                window = benchlib.labelled_counter_window(before, after, SOURCE, "role")
                self.assertEqual(window["status"], "missing")
        # Other families sharing the prefix are not part of this family.
        other = SOURCE + "_extra 5\n"
        window = benchlib.labelled_counter_window(scrape(durable=0) + other, scrape(durable=8) + other, SOURCE, "role")
        self.assertEqual(window, {"status": "complete", "by": {"durable": 8}})

    def test_a_decreased_sample_is_a_reset(self):
        window = benchlib.labelled_counter_window(scrape(durable=9), scrape(durable=2), SOURCE, "role")
        self.assertEqual(window, {"status": "reset", "samples": ['%s{role="durable"}' % SOURCE]})


class SourceSupplyTests(unittest.TestCase):
    S = 8 * MIB

    def windows(self, **readers):
        return {role: {"status": "complete", "by": by} for role, by in readers.items()}

    def test_writer_share_and_reader_supply_reconcile_with_the_sent_bytes(self):
        windows = self.windows(B={"local": 0, "durable": self.S, "verified_cache": 0, "object": 0},
                               C={"local": 0, "durable": 2 * MIB, "verified_cache": 6 * MIB, "object": 0})
        sent = {"A": self.S + 2 * MIB, "B": 6 * MIB, "C": 0}
        supply = benchlib.source_supply(windows, sent, writer="A", file_bytes=self.S, applies=True)
        self.assertEqual(supply["writer_share"], (self.S + 2 * MIB) / (2 * self.S))
        self.assertEqual(supply["peer_received"], 6 * MIB)
        self.assertEqual(supply["readers_sent"], 6 * MIB)
        self.assertEqual(supply["received_by_role"]["durable"], self.S + 2 * MIB)
        self.assertEqual(supply["received_by_reader"]["C"]["verified_cache"], 6 * MIB)
        self.assertIs(supply["ok"], True)

    def test_roles_other_than_local_durable_object_count_as_peer_supply(self):
        windows = self.windows(B={"durable": 0, "peer_holder": self.S})
        supply = benchlib.source_supply(windows, {"A": 0, "C": self.S}, writer="A", file_bytes=self.S,
                                        applies=True)
        self.assertEqual(supply["peer_received"], self.S)
        self.assertIs(supply["ok"], True)

    def test_mismatch_is_reported_not_raised(self):
        windows = self.windows(B={"durable": self.S, "verified_cache": 0})
        supply = benchlib.source_supply(windows, {"A": self.S - 1, "B": 0}, writer="A", file_bytes=self.S,
                                        applies=True)
        self.assertIs(supply["ok"], False)

    def test_peer_bytes_must_match_what_the_other_nodes_sent(self):
        windows = self.windows(B={"durable": self.S // 2, "verified_cache": self.S // 2})
        supply = benchlib.source_supply(windows, {"A": self.S // 2, "C": self.S}, writer="A",
                                        file_bytes=self.S, applies=True)
        self.assertIs(supply["ok"], False)

    def test_object_bytes_cannot_be_attributed(self):
        windows = self.windows(B={"durable": 0, "object": self.S})
        supply = benchlib.source_supply(windows, {"A": self.S}, writer="A", file_bytes=self.S, applies=True)
        self.assertIsNone(supply["ok"])
        self.assertEqual(supply["unattributed"], "object")

    def test_reader_replicas_or_incomplete_counters_are_recorded_not_judged(self):
        windows = self.windows(B={"durable": self.S})
        self.assertIsNone(benchlib.source_supply(windows, {"A": self.S}, writer="A", file_bytes=self.S,
                                                 applies=False)["ok"])
        windows["C"] = {"status": "missing", "samples": [SOURCE]}
        supply = benchlib.source_supply(windows, {"A": self.S}, writer="A", file_bytes=self.S, applies=True)
        self.assertEqual(supply["status"], "missing")
        self.assertIsNone(supply["ok"])
        self.assertNotIn("C", supply["received_by_reader"])

    def test_no_writer_window_means_no_share(self):
        supply = benchlib.source_supply(self.windows(B={"durable": self.S}), {}, writer="A",
                                        file_bytes=self.S, applies=True)
        self.assertIsNone(supply["writer_share"])
        self.assertIsNone(supply["ok"])


class ServiceCompletionTests(unittest.TestCase):
    SERVICES = {"10.0.0.2:26611": "B", "10.0.0.3:26611": "C", "10.0.0.4:26611": "D"}
    CONSOLE = ("OPERATION   RESULT TYPE        FIRST DONE   LAST DONE\n"
               "=========== ================   ==========   =========\n"
               "READ        Elapsed time     :      529ms       1.204s\n"
               "            Svc compl. time  : [ 10.0.0.3:26611=529ms 10.0.0.2:26611=987ms "
               "10.0.0.4:26611=1.204s ]\n")

    def test_elapsed_strings_follow_elbencho_units(self):
        for text, value in (("529ms", 529), ("1.204s", 1204), ("2m3.004s", 123004), ("1h2m3s", 3723000)):
            with self.subTest(text=text):
                self.assertEqual(benchlib.elbencho_elapsed_ms(text), value)
        with self.assertRaises(ValueError):
            benchlib.elbencho_elapsed_ms("soon")

    def test_completion_of_every_reader_is_mapped_to_its_role(self):
        completion = benchlib.elbencho_service_completion(self.CONSOLE, self.SERVICES)
        self.assertEqual(completion["by_reader"], {"B": 987, "C": 529, "D": 1204})
        self.assertEqual((completion["first_ms"], completion["median_ms"], completion["last_ms"]), (529, 987, 1204))
        self.assertEqual(completion["spread_ms"], 675)

    def test_missing_row_or_unknown_service_is_rejected(self):
        with self.assertRaises(ValueError):
            benchlib.elbencho_service_completion("READ Elapsed time : 1s 1s\n", self.SERVICES)
        with self.assertRaises(ValueError):
            benchlib.elbencho_service_completion(self.CONSOLE, {"10.0.0.2:26611": "B"})


class ProgressTests(unittest.TestCase):
    HEADER = ("ISO Date,Label,Phase,RuntimeMS,Rank,MixType,Done%,DoneBytes,MiB/s,IOPS,Entries,Entries/s,"
              "Lat Ent us,Lat IO us,Active,CPU,Service,\n")
    SERVICES = {"h1:1": "B", "h2:1": "C"}

    def row(self, runtime, rank, done, service=""):
        return "2026-11-23T10:00:00.000+0800,,READ,%d,%s,,0,%d,,,0,,,,1,5,%s,\n" % (runtime, rank, done, service)

    def test_progress_gives_first_progress_completion_and_the_largest_lead(self):
        text = self.HEADER + "".join([
            self.row(100, "Total", 3 * MIB), self.row(100, 0, 3 * MIB, "h1:1"), self.row(100, 1, 0, "h2:1"),
            self.row(200, "Total", 9 * MIB), self.row(200, 0, 6 * MIB, "h1:1"), self.row(200, 1, 3 * MIB, "h2:1"),
            self.row(300, "Total", 16 * MIB), self.row(300, 0, 8 * MIB, "h1:1"), self.row(300, 1, 8 * MIB, "h2:1"),
        ])
        progress = benchlib.elbencho_progress(text, self.SERVICES, 8 * MIB)
        self.assertEqual(progress["by_reader"]["B"], {"first_progress_ms": 100, "done_ms": 300, "samples": 3})
        self.assertEqual(progress["by_reader"]["C"]["first_progress_ms"], 200)
        self.assertEqual(progress["max_lead_bytes"], 3 * MIB)
        self.assertEqual(progress["max_lead_at_ms"], 100)
        self.assertEqual(progress["max_lead_fraction"], 3 / 8)
        self.assertEqual(progress["first_progress_spread_ms"], 100)

    def test_reader_that_never_finishes_in_the_samples_has_no_done_time(self):
        text = self.HEADER + self.row(100, 0, MIB, "h1:1") + self.row(100, 1, MIB, "h2:1")
        progress = benchlib.elbencho_progress(text, self.SERVICES, 8 * MIB)
        self.assertIsNone(progress["by_reader"]["B"]["done_ms"])
        self.assertEqual(progress["max_lead_bytes"], 0)

    def test_a_sample_missing_a_reader_does_not_count_as_a_lead(self):
        services = dict(self.SERVICES, **{"h3:1": "D"})
        text = self.HEADER + "".join([self.row(100, 0, 6 * MIB, "h1:1"), self.row(100, 1, 0, "h2:1"),
                                      self.row(200, 0, 7 * MIB, "h1:1"), self.row(200, 1, 6 * MIB, "h2:1"),
                                      self.row(200, 2, 6 * MIB, "h3:1")])
        progress = benchlib.elbencho_progress(text, services, 8 * MIB)
        self.assertEqual((progress["max_lead_bytes"], progress["max_lead_at_ms"]), (MIB, 200))

    def test_missing_column_or_short_row_is_a_format_error(self):
        good = self.HEADER + self.row(100, 0, MIB, "h1:1") + self.row(100, 1, MIB, "h2:1")
        for bad in (good.replace("DoneBytes", "Bytes"), good + "2026-11-23T10:00:00.000+0800,,READ,200\n",
                    good.replace("h2:1,", "h2:1,extra,")):
            with self.subTest(bad=bad[-40:]), self.assertRaises(ValueError):
                benchlib.elbencho_progress(bad, self.SERVICES, MIB)

    def test_report_without_every_reader_is_rejected(self):
        with self.assertRaises(ValueError):
            benchlib.elbencho_progress(self.HEADER + self.row(100, 0, MIB, "h1:1"), self.SERVICES, 8 * MIB)
        with self.assertRaises(ValueError):
            benchlib.elbencho_progress("", self.SERVICES, 8 * MIB)


class ArgvAndParameterTests(unittest.TestCase):
    def test_completion_row_is_always_requested_and_progress_only_when_declared(self):
        plain = benchlib.elbencho_read_argv("/e", ["h:1"], "/m/f", block=MIB, file_bytes=8 * MIB, threads=1,
                                            result="/r.json")
        self.assertIn("--svcelapsed", plain)
        self.assertNotIn("--livecsv", plain)
        self.assertEqual(plain[-1], "/m/f")
        live = benchlib.elbencho_read_argv("/e", ["h:1"], "/m/f", block=MIB, file_bytes=8 * MIB, threads=1,
                                           result="/r.json", live_csv="/live.csv", live_interval_ms=100)
        index = live.index("--livecsv")
        self.assertEqual(live[index:index + 5], ["--livecsv", "/live.csv", "--livecsvex", "--liveint", "100"])
        self.assertEqual(live[-1], "/m/f")

    def test_progress_interval_is_optional_and_bounded(self):
        self.assertEqual(benchlib.parameters(bench()).progress_interval_ms, 0)
        self.assertEqual(benchlib.parameters(bench(progress_interval_ms=100)).progress_interval_ms, 100)
        for bad in (0, 99, 2001, "100", True):
            with self.subTest(bad=bad), self.assertRaises(Blocked):
                benchlib.parameters(bench(progress_interval_ms=bad))


class KernelGroupTests(unittest.TestCase):
    def test_roles_are_grouped_by_kernel_without_exposing_boot_ids(self):
        groups = benchlib.kernel_groups({"A": "k1", "B": "k2", "C": "k2", "D": "k1"})
        self.assertEqual(groups, [["A", "D"], ["B", "C"]])

    def test_one_kernel_is_one_group(self):
        self.assertEqual(benchlib.kernel_groups({"A": "k", "B": "k"}), [["A", "B"]])


class PeerLocalityTests(unittest.TestCase):
    """Reader-to-reader traffic locality is reported only when the kernel groups determine it."""

    def test_all_readers_on_one_kernel_is_same_kernel(self):
        self.assertEqual(benchlib.reader_peer_locality([["A"], ["B", "C", "D", "E"]], "BCDE"), "same_kernel")

    def test_one_reader_per_kernel_is_cross_kernel(self):
        self.assertEqual(benchlib.reader_peer_locality([["A", "B"], ["C"], ["D"]], "BCD"), "cross_kernel")

    def test_mixed_groups_are_undetermined(self):
        # B<->C, D<->E and B<->D, C<->E give the same groups and the same per-role bytes.
        self.assertEqual(benchlib.reader_peer_locality([["A"], ["B", "C"], ["D", "E"]], "BCDE"), "undetermined")
        self.assertEqual(benchlib.reader_peer_locality([["A", "B", "C"], ["D"]], "BCD"), "undetermined")

    def test_a_single_reader_has_no_peer_traffic(self):
        self.assertEqual(benchlib.reader_peer_locality([["A"], ["B"]], "B"), "no_peer")


class MetricWhitelistTests(unittest.TestCase):
    def test_reader_source_and_read_cache_counters_are_in_the_node_window(self):
        import re  # noqa: PLC0415
        suite = (TESTS / "suites" / "bench" / "test_bench.py").read_text()
        pattern = re.search(r'^NODE_METRICS = WRITER_METRICS \+ r"([^"]*)"', suite, re.M)
        writer = re.search(r'^WRITER_METRICS = r"([^"]*)"', suite, re.M)
        self.assertIsNotNone(pattern)
        compiled = re.compile(writer.group(1) + pattern.group(1))
        # Sent payload (afs_dfs_payload_bytes_total) is read from the raw scrapes (SEND_SERIES), not this window.
        for family in ("afs_dfs_read_source_bytes_total", "afs_dfs_read_cache_events_total",
                       "afs_dfs_read_cache_resources"):
            with self.subTest(family=family):
                self.assertTrue(compiled.search(family))


def load_suite():
    import importlib.util  # noqa: PLC0415

    spec = importlib.util.spec_from_file_location("bench_suite_concurrency", TESTS / "suites/bench/test_bench.py")
    suite = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(suite)
    return suite


MEMORY_BACKEND = ({"medium": "memory"}, "remote_cold_memory_backend")
PAYLOAD = 'afs_dfs_payload_bytes_total{direction="send",operation="read",transport="grpc"}'


class FanoutEvidenceTests(unittest.TestCase):
    """Drive fanout_rounds with fakes: B and C read together, A sends S and B serves C the other S."""

    @staticmethod
    def run_rounds(live="ok", **extra):
        suite = load_suite()
        cfg = bench(backend="memory", memory_backend_bytes=256 * MIB, **extra)
        params = benchlib.parameters(cfg)
        size = params.file_bytes
        recorded, commands = [], []
        state = {"reads": 0}

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
                n = state["reads"]
                sent = {"A": n * size, "B": n * size}.get(role, 0)
                text = "%s %d\n" % (PAYLOAD, sent)
                if role == "B":
                    text += scrape(local=0, durable=n * size, verified_cache=0, object=0)
                if role == "C":
                    text += scrape(local=0, durable=0, verified_cache=n * size, object=0)
                return text

            @staticmethod
            def command(role, argv, **kw):
                commands.append(argv)
                if "--hosts" in argv and "--quit" not in argv:
                    state["reads"] += 1
                    console = ("            Svc compl. time  : [ 10.0.0.C:26611=1.100s 10.0.0.B:26611=900ms ]\n")
                    return {"stdout": console, "stderr": "", "returncode": 0}
                return {"stdout": "", "stderr": "", "returncode": 0}

            @staticmethod
            def python(role, code, *args, **kw):
                if "open(sys.argv[1]).read()" in code and args[0].endswith("-live.csv"):
                    if live == "missing":
                        value = {"stdout": "", "stderr": "FileNotFoundError", "returncode": 1}
                        if kw.get("check", True):
                            raise AssertionError("A: command failed: %r" % value)  # as Cluster.python does
                        return value
                    rows = ProgressTests.HEADER + "".join(
                        "t,,READ,%d,%d,,0,%d,,,0,,,,1,5,%s,\n" % (ms, rank, done, host)
                        for ms, rank, done, host in ((100, 0, size // 2, "10.0.0.B:26611"),
                                                     (100, 1, size // 4, "10.0.0.C:26611"),
                                                     (200, 0, size, "10.0.0.B:26611"),
                                                     (200, 1, size, "10.0.0.C:26611")))
                    if live == "no_column":
                        rows = rows.replace("DoneBytes", "Bytes")
                    if live == "short_row":
                        rows += "t,,READ,300,0\n"
                    return {"stdout": rows, "stderr": "", "returncode": 0}
                if "open(sys.argv[1]).read()" in code:
                    phase = {"phase_type": "READ", "config": {"version": "3.3", "hosts": 2},
                             "last_done": {"bytes": size * 2, "elapsed_time_ms": 1100}}
                    return {"stdout": json.dumps(phase) + "\n"}
                if "seek" in code:
                    return {"stdout": json.dumps({"size": 0, "rotated": False,
                                                  "counts": {e: 0 for e in json.loads(args[2])}})}
                return {"stdout": "x"}

        with patch.object(suite, "write_file", return_value={"bytes": size, "mib_per_s": 1.0}), \
                patch.object(suite, "wait_visible"), patch.object(suite, "host_snapshot", return_value={}), \
                patch.object(suite, "host_evidence", return_value=({}, {})), \
                patch.object(suite, "backend_state", return_value=MEMORY_BACKEND), \
                patch.object(suite, "coverage_baseline", return_value=None), \
                patch.object(suite, "boot_id", side_effect=lambda c, r: "k1" if r == "A" else "k2"), \
                patch.object(suite, "remove"), patch.object(suite, "mkdir_owned"):
            suite.fanout_rounds(Cluster(), params, ("B", "C"), "/tmp/l", "/opt/bench/elbencho", {})
        reads = [v for n, v in recorded if n == "bench.round" and v["op"] == "read"]
        runs = [argv for argv in commands if "--hosts" in argv and "--quit" not in argv]
        assert any(n == "bench.summary" for n, _ in recorded)
        return reads, runs, size

    def test_round_records_supply_completion_and_kernel_groups(self):
        reads, runs, size = self.run_rounds()
        evidence = reads[0]["evidence"]
        self.assertEqual(evidence["kernel_groups"], [["A"], ["B", "C"]])
        self.assertFalse(evidence["same_kernel"])
        for key in ("phase_supply", "supply"):
            with self.subTest(key=key):
                self.assertEqual(evidence[key]["writer_share"], 0.5)
                self.assertEqual(evidence[key]["peer_received"], size)
                self.assertIs(evidence[key]["ok"], True)
        timing = reads[0]["result"]["reader_timing"]
        self.assertEqual(timing["completion"]["by_reader"], {"B": 900, "C": 1100})
        self.assertNotIn("progress", timing)
        self.assertTrue(all("--svcelapsed" in argv and "--livecsv" not in argv for argv in runs))

    def test_declared_progress_interval_samples_every_reader(self):
        reads, runs, size = self.run_rounds(progress_interval_ms=100)
        progress = reads[0]["result"]["reader_timing"]["progress"]
        self.assertEqual(progress["interval_ms"], 100)
        self.assertEqual(progress["max_lead_bytes"], size // 4)
        self.assertEqual(progress["by_reader"]["C"]["done_ms"], 200)
        self.assertTrue(all(argv[argv.index("--liveint") + 1] == "100" for argv in runs))

    def test_unreadable_progress_is_unavailable_and_every_round_completes(self):
        for live in ("missing", "no_column", "short_row"):
            with self.subTest(live=live):
                reads, _, size = self.run_rounds(live=live, progress_interval_ms=100)
                self.assertEqual(len(reads), 3)
                for read in reads:
                    progress = read["result"]["reader_timing"]["progress"]
                    self.assertEqual(progress["status"], "unavailable")
                    self.assertEqual(progress["interval_ms"], 100)
                    self.assertTrue(progress["error"])
                    self.assertEqual(read["result"]["bytes"], 2 * size)

    def test_round_records_reader_peer_locality(self):
        reads, _, _ = self.run_rounds()
        self.assertEqual(reads[0]["evidence"]["peer_locality"], "same_kernel")


if __name__ == "__main__":
    unittest.main()
