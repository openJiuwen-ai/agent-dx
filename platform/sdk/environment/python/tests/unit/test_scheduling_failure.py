"""Scheduling expiry is a service result, distinct from transport loss."""
import json
import unittest
from unittest.mock import patch

import httpx
from adx_environment._transport import EnvironmentClient, EnvironmentHTTPError
from adx_environment.types import ConnectionConfig


class SchedulingFailureTest(unittest.TestCase):
    def client(self, handler):
        http = httpx.Client(transport=httpx.MockTransport(handler))
        self.addCleanup(http.close)
        with patch("adx_environment._transport.acquire_shared_http_client", return_value=http):
            client = EnvironmentClient(connection=ConnectionConfig(
                server_address="localhost:8889", token="test", use_tls=False))
        self.addCleanup(client.close)
        return client

    def test_cancelled_schedule_preserves_diagnosis_without_retry_or_backoff(self):
        for sse in (False, True):
            with self.subTest(sse=sse):
                self.check_cancelled_schedule(sse)

    def check_cancelled_schedule(self, sse):
        calls = []
        diagnosis = {"nodeCount": 3, "rejections": {"insufficient_cpu": 3}}

        def handler(request):
            calls.append(request)
            detail = {"code": "DEADLINE_EXCEEDED", "retry": "never",
                      "outcome": "not_started", "requestId": "create-test",
                      "environmentId": "default-test", "schedulingFailure": diagnosis}
            payload = {"message": "scheduling deadline: insufficient CPU", "error": detail}
            if sse:
                payload.update(status="timeout", errorCode=504)
                return httpx.Response(200, headers={"content-type": "text/event-stream"},
                                      text="event: final\ndata: " + json.dumps(payload) + "\n\n")
            return httpx.Response(504, json=payload)

        with patch("adx_environment._transport.time.sleep") as sleep:
            with self.assertRaises(EnvironmentHTTPError) as caught:
                self.client(handler).create_info({"name": "test", "createTimeoutSeconds": 62})
        error = caught.exception
        self.assertEqual(len(calls), 1)
        sleep.assert_not_called()
        self.assertEqual(error.scheduling_failure, diagnosis)
        self.assertEqual((error.retry, error.outcome), ("never", "not_started"))
        self.assertEqual(error.request_id, "create-test")
        self.assertEqual(error.environment_id, "default-test")
        self.assertNotIn("transport failed", str(error))

    def test_exhausted_service_retries_keep_http_error_and_metadata(self):
        for sse in (False, True):
            with self.subTest(sse=sse):
                self.check_exhausted_service_retries(sse)

    def check_exhausted_service_retries(self, sse):
        calls = []

        def handler(request):
            calls.append((request.headers["x-request-id"], json.loads(request.read())))
            detail = {"code": "DEADLINE_EXCEEDED", "retry": "same_operation",
                      "outcome": "unknown", "requestId": calls[-1][0],
                      "environmentId": "default-test"}
            payload = {"message": "node reply unconfirmed", "error": detail}
            if sse:
                payload.update(status="timeout", errorCode=504)
                return httpx.Response(200, headers={"content-type": "text/event-stream"},
                                      text="event: final\ndata: " + json.dumps(payload) + "\n\n")
            return httpx.Response(504, json=payload)

        with patch("adx_environment._transport.time.sleep"):
            with self.assertRaises(EnvironmentHTTPError) as caught:
                self.client(handler).create_info({"name": "test", "createTimeoutSeconds": 62})
        self.assertEqual(len(calls), 3)
        self.assertTrue(all(call == calls[0] for call in calls))
        self.assertEqual(caught.exception.status_code, 504)
        self.assertEqual(caught.exception.outcome, "unknown")
        self.assertNotIn("transport failed", str(caught.exception))


if __name__ == "__main__":
    unittest.main()
