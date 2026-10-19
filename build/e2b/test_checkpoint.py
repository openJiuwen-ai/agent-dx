import importlib.util
import json
import os
from pathlib import Path
import signal
import subprocess
import sys
import tempfile
import time
import unittest

MODULE = Path(__file__).with_name("checkpoint.py")
spec = importlib.util.spec_from_file_location("accept", MODULE)
accept = importlib.util.module_from_spec(spec)
spec.loader.exec_module(accept)


class CheckpointAcceptanceUnitTest(unittest.TestCase):
    def test_observation_requires_memory_counter_schema(self):
        self.assertEqual(
            accept.observation("42 nonce 7 123.5\n"),
            {"pid": "42", "nonce": "nonce", "count": 7, "epoch": 123.5},
        )
        with self.assertRaises(accept.AcceptanceError):
            accept.observation("42 nonce nope 1")

    def test_loop_runs_as_real_child_and_counts(self):
        with tempfile.TemporaryDirectory(prefix="e2b path ") as directory:
            process = subprocess.Popen(
                ["/bin/bash", "-c", accept.loop_command("nonce", directory)],
                start_new_session=True,
            )
            try:
                observed = Path(directory, "e2b-kvm-observe")
                deadline, samples = time.monotonic() + 5, []
                while time.monotonic() < deadline:
                    if observed.exists():
                        samples.append(accept.observation(observed.read_text()))
                        if len(samples) >= 2 and samples[-1]["count"] >= 2:
                            break
                    time.sleep(.2)
                self.assertGreaterEqual(samples[-1]["count"], 2)
                pid = samples[-1]["pid"]
                os.kill(int(pid), 0)
                self.assertEqual(Path(directory, "e2b-kvm-pid").read_text().strip(), pid)
            finally:
                os.killpg(process.pid, signal.SIGTERM)
                process.wait(timeout=5)

    def test_cli_failure_keeps_stdout_and_stderr_as_json(self):
        with tempfile.TemporaryDirectory() as directory:
            base = Path(directory)
            result = subprocess.run(
                [
                    sys.executable,
                    str(MODULE),
                    "--api-url", "https://127.0.0.1",
                    "--key-file", str(base / "missing-key"),
                    "--other-key-file", str(base / "missing-other-key"),
                    "--ca", str(base / "missing-ca"),
                    "--state", str(base / "state.json"),
                    "--output", str(base / "evidence.json"),
                ],
                capture_output=True,
                text=True,
                check=False,
            )
            self.assertEqual(result.returncode, 1)
            self.assertEqual(json.loads(result.stdout)["result"], "failed")
            self.assertEqual(json.loads(result.stderr)["result"], "failed")
            evidence = json.loads((base / "evidence.json").read_text())
            self.assertEqual(evidence["result"], "failed")

    def test_state_rejects_secret_fields(self):
        path = Path(self.id().replace(".", "_"))
        try:
            path.write_text('{"run_id":"r","boxes":{},"token":"secret"}')
            with self.assertRaises(accept.AcceptanceError):
                accept.load_state(path)
        finally:
            path.unlink(missing_ok=True)

    def test_snapshot_list_requires_object_entries(self):
        self.assertEqual(
            accept.snapshot_entries({"snapshots": [{"snapshotID": "s"}]}),
            [{"snapshotID": "s"}],
        )
        self.assertEqual(accept.snapshot_entries([{"snapshotID": "s"}]), [{"snapshotID": "s"}])
        with self.assertRaises(accept.AcceptanceError):
            accept.snapshot_entries([["unexpected"]])

    def test_snapshot_name_is_run_scoped_and_list_shapes_are_poll_safe(self):
        self.assertEqual(accept.snapshot_name("0123456789abcdef"), "kvm-0123456789ab")
        self.assertEqual(accept.snapshot_entries([{"snapshotID": "late"}])[0]["snapshotID"], "late")

    def test_snapshot_timeout_is_single_attempt_without_post_retry(self):
        class Timeout(Exception):
            pass

        calls = []

        def send():
            calls.append(1)
            raise Timeout()

        self.assertEqual(accept.single_snapshot_attempt(send, Timeout, True), (None, True))
        self.assertEqual(calls, [1])
        with self.assertRaises(accept.AcceptanceError):
            accept.single_snapshot_attempt(send, Timeout, False)

    def test_bounded_read_recovers_503_without_any_mutation_retry(self):
        class Timeout(Exception):
            pass

        class Response:
            def __init__(self, status_code):
                self.status_code = status_code

        values, sleeps = iter([Response(503), Response(200)]), []
        response, transient = accept.bounded_read(
            lambda: next(values),
            Timeout,
            10,
            sleep=lambda seconds: sleeps.append(seconds),
            now=lambda: 0,
        )
        self.assertEqual((response.status_code, transient, sleeps), (200, [503], [1]))

    def test_redaction_and_expected_rejection_statuses(self):
        self.assertNotIn("secret", accept.redact("X-API-Key=secret"))

        class Response:
            status_code = 403
            text = "denied"

        response = Response()
        self.assertIs(accept.require_status(response, (401, 403), "isolation"), response)
        with self.assertRaises(accept.AcceptanceError):
            accept.require_status(Response(), (200,), "isolation")

    def test_cleanup_state_never_serializes_credentials(self):
        path = Path(self.id().replace(".", "_") + "-state")
        try:
            state = {"run_id": "run", "boxes": {"source": "sbx"}, "snapshots": ["snap"]}
            accept.save_state(path, state)
            self.assertNotIn("token", path.read_text().lower())
            self.assertEqual(accept.load_state(path), state)
        finally:
            path.unlink(missing_ok=True)


if __name__ == "__main__":
    unittest.main()
