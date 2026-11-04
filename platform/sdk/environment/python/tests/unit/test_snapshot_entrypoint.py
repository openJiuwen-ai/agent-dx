"""Restored process handles discover entrypoint state without restarting it."""
import unittest
from unittest.mock import MagicMock, patch

from adx_environment import Environment, EnvironmentError as SDKEnvironmentError


class SnapshotEntrypointTests(unittest.TestCase):
    def setUp(self):
        self.client = MagicMock()
        self.client.create_info.return_value = {"environmentId": "clone", "newlyCreated": True}
        self.client.environment_info.return_value = {"id": "clone", "status": "running"}
        replacement = patch("adx_environment.environment_api.EnvironmentClient", return_value=self.client)
        replacement.start()
        self.addCleanup(replacement.stop)

    def test_snapshot_handle_polls_saved_entrypoint_until_exit(self):
        self.client.invoke.side_effect = [
            {"status": "running"},
            {"status": "exited", "exit_code": 7, "shell_exit_code": 7},
        ]
        environment = Environment.create(snapshot_id="saved")
        self.addCleanup(environment.close)
        self.assertEqual(environment.wait_entrypoint(), 7)
        self.assertEqual(environment.entrypoint_exit_info["exit_code"], 7)
        self.assertEqual(self.client.invoke.call_count, 2)
        for call in self.client.invoke.call_args_list:
            self.assertEqual(call.args[:2], ("clone", "entrypoint.poll"))
        self.assertNotIn("inheritEntrypoint", self.client.create_info.call_args.args[0])

    def test_snapshot_without_entrypoint_reports_server_error(self):
        self.client.invoke.return_value = {
            "status": "error", "message": "inherit_entrypoint is not enabled for this environment",
        }
        environment = Environment.create(snapshot_id="saved")
        self.addCleanup(environment.close)
        with self.assertRaisesRegex(SDKEnvironmentError, "not enabled"):
            environment.wait_entrypoint()
        self.client.invoke.assert_called_once()

    def test_attached_handle_can_poll_saved_entrypoint(self):
        self.client.invoke.return_value = {"status": "exited", "shell_exit_code": 143}
        environment = Environment.from_id("clone")
        self.addCleanup(environment.close)
        self.assertIsNone(environment.entrypoint_exit_info)
        self.assertEqual(environment.wait_entrypoint(), 143)

    def test_fresh_non_entrypoint_handle_still_rejects_locally(self):
        environment = Environment(image="base")
        self.addCleanup(environment.close)
        with self.assertRaisesRegex(RuntimeError, "not enabled"):
            environment.wait_entrypoint()
        self.client.invoke.assert_not_called()

    def test_closed_snapshot_handle_does_not_poll(self):
        environment = Environment.create(snapshot_id="saved")
        environment.close()
        with self.assertRaisesRegex(RuntimeError, "closed"):
            environment.wait_entrypoint()
        self.client.invoke.assert_not_called()
