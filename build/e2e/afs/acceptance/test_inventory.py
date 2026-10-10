#!/usr/bin/env python3
import json
import os
import pathlib
import platform
import subprocess
import sys
import tempfile
import time
import unittest
from unittest import mock

import inventory


class InventoryTests(unittest.TestCase):
    def test_optional_missing_file_is_unknown(self):
        with tempfile.TemporaryDirectory() as temp:
            result = inventory.file_observation(pathlib.Path(temp) / "missing")
        self.assertEqual(result["status"], "UNKNOWN")
        self.assertIsNone(result["value"])
        self.assertIn("error", result)

    def test_optional_command_timeout_does_not_abort_inventory(self):
        with mock.patch.object(inventory.subprocess, "run", side_effect=subprocess.TimeoutExpired(["timedatectl"], 15)):
            result = inventory.command(["timedatectl"])
        self.assertEqual(result["status"], "UNKNOWN")
        self.assertIsNone(result["returncode"])

    def test_cgroup_includes_parent_limit_when_leaf_has_no_limit(self):
        with tempfile.TemporaryDirectory() as temp:
            root = pathlib.Path(temp)
            proc = root / "proc" / "123"
            proc.mkdir(parents=True)
            (proc / "cgroup").write_text("0::/slice/worker\n", encoding="utf-8")
            cgroups = root / "cgroup"
            leaf = cgroups / "slice" / "worker"
            leaf.mkdir(parents=True)
            (leaf / "memory.max").write_text("max\n", encoding="utf-8")
            (leaf.parent / "memory.max").write_text("1048576\n", encoding="utf-8")
            result = inventory.cgroup_observation(123, root / "proc", cgroups)
        self.assertEqual(result["version"], 2)
        ancestors = result["ancestors"]
        self.assertEqual(ancestors[0]["fields"]["memory.max"]["value"], "max")
        self.assertEqual(ancestors[1]["fields"]["memory.max"]["value"], "1048576")
        self.assertEqual(ancestors[2]["fields"]["memory.max"]["status"], "UNKNOWN")

    def test_cgroup_v1_and_outside_hierarchy_are_unknown(self):
        with tempfile.TemporaryDirectory() as temp:
            root = pathlib.Path(temp)
            proc = root / "proc" / "123"
            proc.mkdir(parents=True)
            membership = proc / "cgroup"
            for value in ("2:memory:/worker\n", "0::/../outside\n"):
                with self.subTest(value=value):
                    membership.write_text(value, encoding="utf-8")
                    result = inventory.cgroup_observation(123, root / "proc", root / "cgroup")
                    self.assertEqual(result["status"], "UNKNOWN")
                    self.assertNotIn("ancestors", result)

    def test_parse_start_ticks_ignores_spaces_in_comm(self):
        fields_after_comm = ["S"] + [str(i) for i in range(4, 53)]
        fields_after_comm[19] = "123456"
        stat = "42 (name with spaces) " + " ".join(fields_after_comm)
        self.assertEqual(inventory.parse_start_ticks(stat), 123456)

    def test_rejects_invalid_process_name_and_pid(self):
        with self.assertRaises(inventory.InventoryError):
            inventory.parse_process_assignment("bad/name=123")
        with self.assertRaises(inventory.InventoryError):
            inventory.parse_process_assignment("meta=not-a-pid")

    def test_collects_current_process_without_cmdline_or_environ(self):
        probe = inventory.ProcessProbe("self", os.getpid(), "test")
        identity = inventory.collect_process_identity(probe)
        self.assertEqual(identity["name"], "self")
        self.assertEqual(identity["pid"], os.getpid())
        self.assertRegex(identity["sha256"], r"^[0-9a-f]{64}$")
        self.assertGreater(identity["start_ticks"], 0)
        self.assertIn("exe_path", identity)
        self.assertNotIn("cmdline", identity)
        self.assertNotIn("environ", identity)

    def test_dead_pid_exits_nonzero(self):
        proc = subprocess.Popen([sys.executable, "-c", "pass"])
        proc.wait(timeout=10)
        result = subprocess.run(
            [sys.executable, str(pathlib.Path(inventory.__file__)), "--process", f"dead={proc.pid}"],
            capture_output=True,
            text=True,
            timeout=20,
        )
        self.assertNotEqual(result.returncode, 0)
        self.assertIn("inventory error", result.stderr)

    def test_cli_rejects_other_architecture_before_collecting(self):
        script = pathlib.Path(inventory.__file__).resolve()
        code = (
            "import platform, sys\n"
            f"sys.path.insert(0, {str(script.parent)!r})\n"
            "import inventory\n"
            "platform.machine = lambda: 'x86_64'\n"
            "def unexpected_collect(_):\n"
            "    raise AssertionError('collected before platform admission')\n"
            "inventory.collect_guest_state = unexpected_collect\n"
            "raise SystemExit(inventory.main([]))\n"
        )
        result = subprocess.run(
            [sys.executable, "-c", code], capture_output=True, text=True, timeout=20,
        )
        self.assertEqual(result.returncode, 2, result.stderr)
        self.assertIn("dedicated ARM64 Linux required", result.stderr)
        self.assertEqual(result.stdout, "")

    def test_pid_file_probe_respects_dedicated_platform(self):
        sleeper = subprocess.Popen([sys.executable, "-c", "import time; time.sleep(30)"])
        try:
            with tempfile.TemporaryDirectory() as temp:
                pid_file = pathlib.Path(temp) / "sleep.pid"
                pid_file.write_text(f"{sleeper.pid}\n", encoding="utf-8")
                result = subprocess.run(
                    [sys.executable, str(pathlib.Path(inventory.__file__)), "--pid-file", f"sleep={pid_file}"],
                    capture_output=True,
                    text=True,
                    timeout=20,
                )
            if platform.system() != "Linux" or platform.machine() != "aarch64":
                self.assertEqual(result.returncode, 2, result.stderr)
                self.assertIn("dedicated ARM64 Linux required", result.stderr)
                self.assertEqual(result.stdout, "")
                return
            self.assertEqual(result.returncode, 0, result.stderr)
            state = json.loads(result.stdout)
            process = state["processes"]["sleep"]
            self.assertEqual(process["pid"], sleeper.pid)
            self.assertEqual(process["pid_source"], str(pid_file))
            self.assertRegex(process["sha256"], r"^[0-9a-f]{64}$")
        finally:
            sleeper.terminate()
            try:
                sleeper.wait(timeout=5)
            except subprocess.TimeoutExpired:
                sleeper.kill()
                sleeper.wait(timeout=5)


if __name__ == "__main__":
    unittest.main()
