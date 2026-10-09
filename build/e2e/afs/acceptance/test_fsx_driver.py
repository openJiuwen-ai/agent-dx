#!/usr/bin/env python3
import importlib.util
import json
import os
import platform
import subprocess
import sys
import tempfile
import textwrap
import unittest
from unittest import mock
from pathlib import Path

DRIVER = Path(__file__).resolve().parent / "drivers" / "fsx.py"


def load_driver_module():
    sys.path.insert(0, str(DRIVER.parent))
    spec = importlib.util.spec_from_file_location("fsx_driver_under_test", DRIVER)
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


def write_fake_secfs(root: Path, behavior: str = "pass") -> tuple[Path, Path]:
    suite = root / "secfs.test"
    bin_dir = suite / "tools" / "bin"
    src_dir = suite / "fstools" / "src" / "fsx"
    bin_dir.mkdir(parents=True)
    src_dir.mkdir(parents=True)
    (src_dir / "fsx.c").write_text("/* fake fsx source */\n", encoding="utf-8")
    subprocess.run(["git", "init", "-q", str(suite)], check=True)
    subprocess.run(["git", "-C", str(suite), "config", "user.email", "test@example.invalid"], check=True)
    subprocess.run(["git", "-C", str(suite), "config", "user.name", "test"], check=True)
    (suite / "README").write_text("fake secfs.test\n", encoding="utf-8")
    subprocess.run(["git", "-C", str(suite), "add", "README", "fstools/src/fsx/fsx.c"], check=True)
    subprocess.run(["git", "-C", str(suite), "commit", "-q", "-m", "fake"], check=True)
    fsx = bin_dir / "fsx"
    fsx.write_text(
        textwrap.dedent(
            f"""\
            #!/usr/bin/env python3
            import pathlib, sys, time
            behavior = {behavior!r}
            if '-h' in sys.argv or '--help' in sys.argv:
                print('usage: fsx [-d duration] [-N numops] [-P dirpath] [-S seed] fname')
                print('-d duration')
                print('-N numops')
                print('-P dirpath')
                print('-S seed')
                raise SystemExit(0)
            pdir = pathlib.Path(sys.argv[sys.argv.index('-P') + 1])
            seed = sys.argv[sys.argv.index('-S') + 1]
            target = pathlib.Path(sys.argv[-1])
            ops = None
            if '-N' in sys.argv:
                ops = int(sys.argv[sys.argv.index('-N') + 1])
            pdir.mkdir(parents=True, exist_ok=True)
            if behavior == 'sleep':
                time.sleep(5)
            if behavior == 'fail':
                print('seed ' + seed + ' mismatch at operation 1000')
                raise SystemExit(9)
            if behavior == 'fail-prefix-37' and ops is not None and ops >= 37:
                print('seed ' + seed + ' mismatch at operation 37')
                raise SystemExit(9)
            if behavior == 'fail-unreproducible':
                replay = 'prefix-replay' in str(pdir)
                if not replay:
                    print('seed ' + seed + ' mismatch at operation 41')
                    raise SystemExit(9)
            if behavior == 'fail-timeout-mid':
                if ops is not None and ops >= 100:
                    print('seed ' + seed + ' mismatch at operation 100')
                    raise SystemExit(9)
                if ops is not None and ops >= 50:
                    time.sleep(5)
            if behavior == 'fail-different-replay':
                replay = 'prefix-replay' in str(pdir)
                if ops is not None and ops >= 37:
                    if replay:
                        print('unknown option at operation 37')
                        raise SystemExit(2)
                    print('seed ' + seed + ' mismatch at operation 37')
                    raise SystemExit(9)
            if behavior == 'false-pass':
                raise SystemExit(0)
            target.write_bytes(('seed=' + seed).encode())
            (pdir / '.fsxlog').write_text('log seed=' + seed)
            (pdir / '.fsxgood').write_text('good seed=' + seed)
            completed = ops if ops is not None else 1073
            print('Using file ' + str(target))
            print('Seed set to ' + seed)
            print('All operations - ' + str(completed) + ' - completed A-OK!')
            raise SystemExit(0)
            """
        ),
        encoding="utf-8",
    )
    fsx.chmod(0o755)
    return suite, fsx


class FsxIdentityTests(unittest.TestCase):
    def test_git_identity_trust_is_scoped_to_selected_suite_root(self):
        fsx_driver = load_driver_module()
        with tempfile.TemporaryDirectory() as td:
            suite = Path(td) / "secfs.test"
            binary = suite / "tools/bin/fsx"
            source = suite / "fstools/src/fsx/fsx.c"
            source.parent.mkdir(parents=True)
            binary.parent.mkdir(parents=True)
            binary.write_text("#!/bin/sh\n", encoding="utf-8")
            binary.chmod(0o755)
            source.write_text("/* fsx */\n", encoding="utf-8")
            root = suite.resolve()
            responses = [
                {"returncode": 0, "stdout": "edf5\n", "stderr": ""},
                {"returncode": 0, "stdout": "", "stderr": ""},
                {"returncode": 0, "stdout": "usage\n", "stderr": ""},
            ]
            with mock.patch.object(fsx_driver, "run_text", side_effect=responses) as run:
                identity = fsx_driver.fsx_identity(suite, binary)
            git = ["git", "-c", f"safe.directory={root}", "-C", str(root)]
            self.assertEqual(run.call_args_list[0], mock.call(git + ["rev-parse", "HEAD"]))
            self.assertEqual(run.call_args_list[1], mock.call(git + ["status", "--porcelain"]))
            self.assertEqual(identity["git_head"], "edf5")


class FsxDriverTests(unittest.TestCase):
    def setUp(self):
        if platform.system() != "Linux":
            self.skipTest("FSx driver self-tests run in Linux so findmnt and mount semantics match acceptance")

    def run_driver(self, behavior="pass", *, profile="smoke", duration=None, seeds=None, max_seeds=None, timeout=None, replay_timeout=None, replay_attempts=None, base_dir: Path | None = None):
        root = Path(tempfile.mkdtemp(prefix="afs-fsx-driver-test-"))
        self.addCleanup(lambda: subprocess.run(["rm", "-rf", str(root)], check=False))
        suite, fsx = write_fake_secfs(root, behavior)
        mount = root / "mount"
        mount.mkdir()
        base = base_dir if base_dir is not None else mount / "base"
        base.mkdir(parents=True, exist_ok=True)
        run_dir = root / "run"
        matrix = {"reference": "ext4", "suite_sha": "secfs.test edf5eb4a108bfb41073f765aef0cdd32bb3ee1ed", "seeds": 3}
        argv = [
            sys.executable,
            str(DRIVER),
            "--profile",
            profile,
            "--matrix-json",
            json.dumps(matrix),
            "--run-dir",
            str(run_dir),
            "--mount",
            str(mount),
            "--base-dir",
            str(base),
            "--suite-root",
            str(suite),
            "--fsx-binary",
            str(fsx),
            "--backend",
            "reference",
            "--meta",
            "memory",
            "--allow-unpinned-suite-fixture",
        ]
        if duration is not None:
            argv.extend(["--duration-seconds", str(duration)])
        if seeds is not None:
            argv.extend(["--seeds", seeds])
        if max_seeds is not None:
            argv.extend(["--max-seeds", str(max_seeds)])
        if timeout is not None:
            argv.extend(["--per-seed-timeout", str(timeout)])
        if replay_timeout is not None:
            argv.extend(["--failure-replay-timeout", str(replay_timeout)])
        if replay_attempts is not None:
            argv.extend(["--failure-replay-attempts", str(replay_attempts)])
        proc = subprocess.run(argv, shell=False, text=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE, check=False)
        lines = [line for line in proc.stdout.splitlines() if line.strip()]
        self.assertTrue(lines, proc.stderr)
        return proc, json.loads(lines[-1]), run_dir

    def test_smoke_pass_records_commands_and_axes(self):
        proc, proof, run_dir = self.run_driver()
        self.assertEqual(proc.returncode, 0, proof)
        self.assertEqual(proof["status"], "PASS")
        self.assertEqual(proof["accounting"]["selected_seeds"], [1])
        self.assertEqual(proof["accounting"]["result_counts"]["PASS"], 1)
        self.assertIn("seeds", proof["coverage"]["axes"])
        self.assertEqual(proof["coverage"]["axes"]["seeds"]["values"], ["1"])
        self.assertEqual(proof["coverage"]["axes"]["seeds_smoke"]["values"], ["1"])
        self.assertNotIn("seeds_full", proof["coverage"]["axes"])
        commands = run_dir / "artifacts" / "std-03-fsx" / "commands.json"
        self.assertTrue(commands.exists())
        raw = json.loads(commands.read_text())
        self.assertEqual(raw[0]["seed"], 1)
        self.assertIn("-S", raw[0]["command"])

    def test_full_short_duration_is_blocked_not_pass(self):
        proc, proof, _run_dir = self.run_driver(profile="full", duration=1, timeout=3)
        self.assertNotEqual(proc.returncode, 0)
        self.assertEqual(proof["status"], "BLOCKED")
        self.assertIn("900s", proof["reason"])
        self.assertEqual(proof["accounting"]["selected_seeds"], [1, 2, 3])

    def test_full_seed_cap_is_blocked_not_pass(self):
        proc, proof, _run_dir = self.run_driver(profile="full", max_seeds=1, duration=900, timeout=2)
        self.assertNotEqual(proc.returncode, 0)
        self.assertEqual(proof["status"], "BLOCKED")
        self.assertIn("capped", proof["reason"])

    def test_failure_is_fail_and_keeps_raw_output(self):
        proc, proof, run_dir = self.run_driver("fail")
        self.assertNotEqual(proc.returncode, 0)
        self.assertEqual(proof["status"], "FAIL")
        stdout_logs = list((run_dir / "artifacts" / "std-03-fsx" / "seeds").glob("*/stdout.log"))
        self.assertTrue(stdout_logs)
        self.assertTrue(any("mismatch" in path.read_text(errors="replace") for path in stdout_logs))

    def test_failure_records_minimal_failing_prefix(self):
        proc, proof, run_dir = self.run_driver("fail-prefix-37")
        self.assertNotEqual(proc.returncode, 0)
        self.assertEqual(proof["status"], "FAIL")
        minimization = proof["failure_minimization"]
        self.assertIsNotNone(minimization)
        self.assertEqual(minimization["items"][0]["status"], "REPRODUCED")
        self.assertEqual(minimization["items"][0]["minimal_failing_prefix_operations"], 37)
        artifact = run_dir / minimization["artifact"]
        self.assertTrue(artifact.exists())
        raw = json.loads(artifact.read_text())
        self.assertEqual(raw[0]["minimal_failing_prefix_operations"], 37)
        self.assertIn("minimal failing prefix", raw[0]["label"])
        original_parent = Path(raw[0]["original_failure"]["tested_fixture_parent"])
        self.assertTrue(str(original_parent).endswith(".afs-std03-fsx-" + str(original_parent).split(".afs-std03-fsx-")[-1]))
        for attempt in raw[0]["attempts"]:
            fixture = Path(attempt["fixture"])
            self.assertEqual(fixture.parent, original_parent)
            self.assertTrue(attempt["cleanup"]["removed"])
            self.assertEqual(attempt["findmnt"]["returncode"], 0)

    def test_unreproduced_minimization_keeps_original_fail(self):
        proc, proof, run_dir = self.run_driver("fail-unreproducible")
        self.assertNotEqual(proc.returncode, 0)
        self.assertEqual(proof["status"], "FAIL")
        item = proof["failure_minimization"]["items"][0]
        self.assertEqual(item["status"], "INCONCLUSIVE")
        self.assertIn("original case remains FAIL", item["reason"])
        stdout_logs = list((run_dir / "artifacts" / "std-03-fsx" / "seeds").glob("*/stdout.log"))
        self.assertTrue(any("mismatch" in path.read_text(errors="replace") for path in stdout_logs))

    def test_timeout_during_prefix_search_is_inconclusive(self):
        proc, proof, _run_dir = self.run_driver("fail-timeout-mid", replay_timeout=1)
        self.assertNotEqual(proc.returncode, 0)
        self.assertEqual(proof["status"], "FAIL")
        item = proof["failure_minimization"]["items"][0]
        self.assertEqual(item["status"], "INCONCLUSIVE")
        self.assertIn("non PASS/FAIL", item["reason"])
        self.assertEqual(item["inconclusive_result"], "TIMEOUT")

    def test_different_replay_failure_signature_is_inconclusive(self):
        proc, proof, _run_dir = self.run_driver("fail-different-replay")
        self.assertNotEqual(proc.returncode, 0)
        self.assertEqual(proof["status"], "FAIL")
        item = proof["failure_minimization"]["items"][0]
        self.assertEqual(item["status"], "INCONCLUSIVE")
        self.assertIn("same failure fingerprint", item["reason"])

    def test_zero_exit_without_completion_marker_fails(self):
        proc, proof, _run_dir = self.run_driver("false-pass")
        self.assertNotEqual(proc.returncode, 0)
        self.assertEqual(proof["status"], "FAIL")
        self.assertIn("completion marker", proof["reason"])

    def test_timeout_is_blocked(self):
        proc, proof, _run_dir = self.run_driver("sleep", timeout=1)
        self.assertNotEqual(proc.returncode, 0)
        self.assertEqual(proof["status"], "BLOCKED")
        self.assertIn("timeout", proof["reason"])


    def test_readonly_base_dir_blocks(self):
        root = Path(tempfile.mkdtemp(prefix="afs-fsx-driver-readonly-"))
        mount = root / "mount"
        base = mount / "base"
        base.mkdir(parents=True)
        base.chmod(0o555)
        def cleanup():
            try:
                base.chmod(0o755)
            except OSError:
                pass
            subprocess.run(["rm", "-rf", str(root)], check=False)
        self.addCleanup(cleanup)
        proc, proof, _run_dir = self.run_driver(base_dir=base)
        self.assertNotEqual(proc.returncode, 0)
        self.assertEqual(proof["status"], "BLOCKED")
        self.assertTrue(any(c["status"] == "BLOCKED" for c in proof["checks"]), proof["checks"])

    def test_base_dir_outside_mount_blocks(self):
        root = Path(tempfile.mkdtemp(prefix="afs-fsx-driver-outside-"))
        self.addCleanup(lambda: subprocess.run(["rm", "-rf", str(root)], check=False))
        outside = root / "outside"
        proc, proof, _run_dir = self.run_driver(base_dir=outside)
        self.assertNotEqual(proc.returncode, 0)
        self.assertEqual(proof["status"], "BLOCKED")
        self.assertEqual(next(c for c in proof["checks"] if c["name"] == "base-dir-scope")["status"], "BLOCKED")


if __name__ == "__main__":
    unittest.main()
