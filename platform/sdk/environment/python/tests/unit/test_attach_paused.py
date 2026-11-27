"""Attach handles can represent a paused environment for explicit resume."""
import unittest
from unittest.mock import MagicMock, patch

from adx_environment import Environment


class AttachPausedTests(unittest.TestCase):
    def setUp(self):
        self.client = MagicMock()
        self.client.environment_info.return_value = {"id": "default-tools-x", "status": "paused"}
        replacement = patch("adx_environment.environment_api.EnvironmentClient", return_value=self.client)
        replacement.start()
        self.addCleanup(replacement.stop)

    def test_from_id_can_attach_to_paused_without_running_requirement(self):
        environment = Environment.from_id("default-tools-x", require_running=False)
        self.addCleanup(environment.close)
        self.assertEqual(environment.id, "default-tools-x")

    def test_from_id_still_rejects_paused_by_default(self):
        with self.assertRaisesRegex(RuntimeError, "not running"):
            Environment.from_id("default-tools-x")
