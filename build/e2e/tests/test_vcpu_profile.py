import sys
import unittest
from pathlib import Path

sys.path.append(str(Path(__file__).resolve().parents[1] / "firecracker/diagnostics"))
from vcpu_wait import cpu_stat


class VcpuProfileTests(unittest.TestCase):
    def test_proc_stat_spaces_and_cpu_ticks(self):
        fields = ["D", "17"] + ["0"] * 9 + ["3", "5"] + ["0"] * 20
        self.assertEqual(
            cpu_stat("42 (fc_vcpu 0) " + " ".join(fields), 100),
            {"state": "D", "parent": 17, "cpu_ns": 80000000},
        )

    def test_agent_cpu_keeps_unattributed_wait_separate(self):
        from tempfile import TemporaryDirectory
        from agent_cpu import summarize

        with TemporaryDirectory() as d:
            p = Path(d) / "node.log"
            p.write_text(
                'time="2026-10-20 10:00:00.001" level=info msg="firecracker: guest agent CPU profile" '
                "vsock=/owned/vsock message_type=1 guest_handler_wall_ms=2.5 guest_process_user_ms=0.1 "
                "guest_process_system_ms=0.2 host_response_ms=1500\n"
            )
            r = summarize([p])["samples"][0]
            self.assertAlmostEqual(r["guest_process_cpu_ms"], 0.3)
            self.assertEqual(r["response_outside_handler_ms"], 1497.5)
            p.write_text("unrelated log\n")
            with self.assertRaises(ValueError):
                summarize([p])

    def test_wait_estimates_clip_to_phase_and_exclude_sampling_gaps(self):
        from agent_cpu import wait_window

        observations = (
            (0, "fuse_wait_on_page_writeback"),
            (10, "0"),
            (20, "fuse_flush"),
            (1000, "0"),
        )
        points = [{"time_ns": t * 1000000, "wchan": w} for t, w in observations]
        self.assertEqual(
            wait_window(points, 5000000, 25000000),
            {
                "covered_ms": 15,
                "fuse_wait_estimate_ms": 5,
                "dirty_throttle_estimate_ms": 0,
                "file_page_fault_wait_estimate_ms": 0,
            },
        )

    def test_restore_join_excludes_create_and_other_instances(self):
        from agent_cpu import restore_phases
        from datetime import datetime
        import hashlib
        from tempfile import TemporaryDirectory

        with TemporaryDirectory() as d:
            p = Path(d) / "node.log"
            backend = "sbox-owned"
            tag = hashlib.sha256(backend.encode()).hexdigest()[:24]
            p.write_text(
                'time="2026-10-20 10:00:00.050" msg="firecracker: restore agent phase" elapsed_ms=50 '
                "phase=agent_health sandbox_id=" + backend + "\n"
            )
            base = int(datetime.fromisoformat("2026-10-20T10:00:00+08:00").timestamp() * 1e9)
            points = [
                {
                    "time_ns": base + t * 1000000,
                    "backend_id": b,
                    "pid": 1,
                    "tid": 2,
                    "wchan": "fuse_wait_on_page_writeback",
                }
                for b in [backend, "sbox-other"]
                for t in [0, 20, 40, 60]
            ]
            cpu = [
                {
                    "log": p.name,
                    "vsock": str(Path("/run/sandboxd/firecracker") / tag / "vsock.sock"),
                    "message_type": 1,
                    "time": t,
                }
                for t in ["2026-10-20 09:59:00.000", "2026-10-20 10:00:00.049"]
            ]
            row = restore_phases([p], points, cpu)[0]
            self.assertEqual(len(row["guest_cpu_replies"]), 1)
            self.assertEqual(len(row["vcpu_waits"]), 1)
            self.assertEqual(row["vcpu_waits"][0]["fuse_wait_estimate_ms"], 50)

    def test_host_cpu_accounts_active_execution_separately_from_page_fault_sleep(self):
        from agent_cpu import cpu_window, wait_window

        points = [
            {
                "time_ns": t * 1000000,
                "run_ns": r * 1000000,
                "runqueue_ns": q * 1000000,
                "wchan": "folio_wait_bit_common",
                "stack": "filemap_fault",
            }
            for t, r, q in [(0, 10, 0), (20, 12, 1), (40, 15, 2)]
        ]
        self.assertEqual(
            cpu_window(points, 0, 40000000),
            {"cpu_covered_ms": 40, "vcpu_run_estimate_ms": 5, "runqueue_estimate_ms": 2},
        )
        self.assertEqual(wait_window(points, 0, 40000000)["file_page_fault_wait_estimate_ms"], 40)
