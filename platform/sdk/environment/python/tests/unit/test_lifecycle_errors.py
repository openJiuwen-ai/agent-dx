"""Lifecycle HTTP failures keep the stable error contract fields."""
import unittest
from types import SimpleNamespace
from unittest.mock import MagicMock

from adx_environment._transport import EnvironmentClient, EnvironmentHTTPError


class LifecycleErrorTests(unittest.TestCase):
    def _client(self, status_code, payload):
        response = MagicMock(status_code=status_code, headers={})
        response.json.return_value = payload
        client = EnvironmentClient.__new__(EnvironmentClient)
        client._http = MagicMock()
        client._http.post.return_value = response
        client._base = "http://127.0.0.1:1/api/environment/v1"
        client._connection = SimpleNamespace(communication_margin_seconds=0)
        return client

    def test_resume_invalid_argument_keeps_status_and_retry_fields(self):
        client = self._client(
            400,
            {
                "code": 400,
                "message": "checkpoint expired or missing",
                "error": {
                    "code": "INVALID_ARGUMENT",
                    "retry": "never",
                    "outcome": "not_started",
                    "requestId": "resume-x",
                    "environmentId": "default-tools-x",
                },
            },
        )
        with self.assertRaises(EnvironmentHTTPError) as raised:
            client.resume("default-tools-x")
        error = raised.exception
        self.assertEqual(error.status_code, 400)
        self.assertEqual(error.code, "INVALID_ARGUMENT")
        self.assertEqual(error.retry, "never")
        self.assertEqual(error.outcome, "not_started")
        self.assertEqual(error.environment_id, "default-tools-x")

    def test_resume_conflict_keeps_retry_never(self):
        client = self._client(
            409,
            {
                "code": 409,
                "message": "execution is not resumable",
                "error": {
                    "code": "CONFLICT",
                    "retry": "never",
                    "outcome": "not_started",
                    "requestId": "resume-x",
                    "environmentId": "default-tools-x",
                },
            },
        )
        with self.assertRaises(EnvironmentHTTPError) as raised:
            client.resume("default-tools-x")
        self.assertEqual(raised.exception.status_code, 409)
        self.assertEqual(raised.exception.retry, "never")


if __name__ == "__main__":
    unittest.main()
