import json
import threading
import unittest
from concurrent.futures import ThreadPoolExecutor
from functools import partial
from unittest.mock import patch

import httpx

from adx_environment import ConnectionConfig
from adx_environment._transport import EnvironmentClient, EnvironmentError, EnvironmentHTTPError


class ResumeTimeoutTests(unittest.TestCase):
    def client(self, handler, margin=5, attempts=3):
        client = EnvironmentClient(connection=ConnectionConfig(
            server_address="ingress:8888", token="token", use_tls=False,
            communication_margin_seconds=margin,
            request_max_attempts=attempts,
        ))
        self.addCleanup(client.close)
        client._http.close()
        client._http = httpx.Client(transport=httpx.MockTransport(handler))
        return client

    def test_client_timeout_wraps_explicit_server_deadline(self):
        seen = []
        def handler(request):
            seen.append(request)
            return httpx.Response(200, json={"ok": True})
        self.client(handler).invoke("env", "test", timeout=10)
        self.assertEqual(seen[0].headers["X-ADX-Processing-Timeout-Ms"], "10000")
        self.assertGreater(seen[0].extensions["timeout"]["read"], 14)
        self.assertLessEqual(seen[0].extensions["timeout"]["read"], 15)

    def test_resuming_retries_same_request_with_the_remaining_timeout(self):
        seen, sleeps = [], []
        def handler(request):
            seen.append(request)
            if len(seen) == 1:
                clock[0] += 6.0
                return httpx.Response(
                    503, headers={"Retry-After": "1", "X-ADX-Error-Code": "ENV_RESUMING"},
                    json={"error": {
                        "code": "ENV_RESUMING", "retry": "same_operation", "outcome": "not_started",
                        "requestForwarded": False, "requestId": request.headers["X-ADX-Request-ID"],
                    }},
                )
            return httpx.Response(200, json={"ok": True})
        clock = [100.0]
        def sleep(seconds):
            sleeps.append(seconds)
            clock[0] += seconds
        with patch("adx_environment._transport.time.monotonic", side_effect=lambda: clock[0]), patch(
            "adx_environment._transport.time.sleep", side_effect=sleep
        ), patch("adx_environment._transport.random.uniform", return_value=0.0):
            self.assertEqual(self.client(handler).invoke("env", "side_effect", timeout=10), {"ok": True})
        self.assertGreaterEqual(sleeps[0], 1)
        self.assertEqual(int(seen[1].headers["X-ADX-Processing-Timeout-Ms"]), 3000)
        self.assertEqual(seen[0].headers["X-ADX-Request-ID"], seen[1].headers["X-ADX-Request-ID"])
        self.assertEqual(json.loads(seen[0].content), json.loads(seen[1].content))
        self.assertEqual(seen[1].extensions["timeout"]["read"], 8)
        self.assertTrue(all(r.url.path.startswith("/direct/") for r in seen))

    def test_resuming_exhaustion_preserves_known_not_forwarded_outcome(self):
        calls = []
        def handler(request):
            calls.append(request)
            return httpx.Response(
                503, headers={"Retry-After": "1", "X-ADX-Error-Code": "ENV_RESUMING"},
                json={"error": {
                    "code": "ENV_RESUMING", "retry": "same_operation", "outcome": "not_started",
                    "requestForwarded": False, "requestId": request.headers["X-ADX-Request-ID"],
                }},
            )
        with patch("adx_environment._transport.time.sleep"):
            with self.assertRaises(EnvironmentHTTPError) as raised:
                self.client(handler).invoke("env", "side_effect", timeout=10)
        self.assertEqual(raised.exception.code, "ENV_RESUMING")
        self.assertEqual(raised.exception.outcome, "not_started")
        self.assertEqual(len(calls), 3)

    def test_explicit_terminal_error_is_not_retried(self):
        calls = []
        def handler(request):
            calls.append(request)
            return httpx.Response(503, json={"error": {"code": "RESTORE_FAILED", "retry": "never"}})
        with self.assertRaises(EnvironmentHTTPError):
            self.client(handler).invoke("env", "side_effect", timeout=10)
        self.assertEqual(len(calls), 1)

    def test_margin_validation_and_environment(self):
        for value in [-1, True, float("inf"), float("nan"), "5"]:
            with self.assertRaises((TypeError, ValueError)):
                ConnectionConfig(server_address="ingress", token="t", communication_margin_seconds=value)
        with patch.dict("os.environ", {"ADX_SERVER_ADDRESS": "ingress", "ADX_TOKEN": "t",
                                       "ADX_COMMUNICATION_MARGIN_SECONDS": "7.5"}):
            self.assertEqual(ConnectionConfig.from_env().communication_margin_seconds, 7.5)

    def test_existing_idempotent_transport_retry_uses_remaining_timeout(self):
        seen, clock = [], [100.0]
        def handler(request):
            seen.append(request)
            if len(seen) == 1:
                clock[0] += 3
                raise httpx.ReadTimeout("response lost", request=request)
            return httpx.Response(200, json={"ok": True})
        with patch("adx_environment._transport.time.monotonic", side_effect=lambda: clock[0]), patch(
            "adx_environment._transport.time.sleep"
        ):
            self.client(handler).invoke("env", "side_effect", timeout=10)
        self.assertEqual([r.headers["X-ADX-Processing-Timeout-Ms"] for r in seen], ["10000", "7000"])
        self.assertEqual([r.extensions["timeout"]["read"] for r in seen], [15, 12])
        self.assertEqual(seen[0].headers["X-ADX-Request-ID"], seen[1].headers["X-ADX-Request-ID"])

    def test_resuming_cannot_erase_an_earlier_unknown_outcome(self):
        calls = []
        def handler(request):
            calls.append(request)
            if request.method == "GET":
                return httpx.Response(200, json={"status": "running", "failure": None})
            if len(calls) == 1:
                raise httpx.ReadTimeout("response lost", request=request)
            return httpx.Response(503, headers={"X-ADX-Error-Code": "ENV_RESUMING"}, json={"error": {
                "code": "ENV_RESUMING", "retry": "same_operation", "outcome": "not_started",
                "requestForwarded": False, "requestId": request.headers["X-ADX-Request-ID"],
            }})
        with patch("adx_environment._transport.time.sleep"):
            with self.assertRaises(EnvironmentError) as raised:
                self.client(handler).invoke("env", "side_effect", timeout=10)
        self.assertEqual(raised.exception.code, "OUTCOME_UNKNOWN")
        self.assertTrue(all(r.url.path.startswith("/direct/") for r in calls if r.method == "POST"))
        self.assertEqual(sum(r.method == "POST" for r in calls), 3)
        self.assertEqual(sum(r.method == "GET" for r in calls), 1)

    def test_concurrent_resuming_retries_preserve_each_logical_identity(self):
        barrier, lock = threading.Barrier(12), threading.Lock()
        calls, executions = {}, {}
        def handler(request):
            request_id = request.headers["X-ADX-Request-ID"]
            with lock:
                calls.setdefault(request_id, []).append(request)
                first = len(calls[request_id]) == 1
            if first:
                barrier.wait(timeout=5)
                return httpx.Response(
                    503, headers={"X-ADX-Error-Code": "ENV_RESUMING", "Retry-After": "0"},
                    json={"error": {
                        "code": "ENV_RESUMING", "retry": "same_operation", "outcome": "not_started",
                        "requestForwarded": False, "requestId": request_id,
                    }},
                )
            with lock:
                executions[request_id] = executions.get(request_id, 0) + 1
            return httpx.Response(200, json={"requestId": request_id})
        client = self.client(handler)
        with patch("adx_environment._transport.time.sleep"), ThreadPoolExecutor(max_workers=12) as pool:
            results = list(pool.map(lambda _: client.invoke("env", "side_effect", timeout=10), range(12)))
        self.assertEqual(len(results), 12)
        self.assertEqual(len(calls), 12)
        self.assertTrue(all(len(requests) == 2 for requests in calls.values()))
        self.assertTrue(all(count == 1 for count in executions.values()))
        for requests in calls.values():
            self.assertEqual(requests[0].content, requests[1].content)

    def test_binary_upload_retries_only_a_replayable_body(self):
        for replayable in [True, False]:
            calls = []

            def handler(request, recorded):
                recorded.append(request)
                if len(recorded) == 1:
                    return httpx.Response(503, headers={"X-ADX-Error-Code": "ENV_RESUMING"}, json={"error": {
                        "code": "ENV_RESUMING", "retry": "same_operation", "outcome": "not_started",
                        "requestForwarded": False, "requestId": request.headers["X-ADX-Request-ID"],
                    }})
                return httpx.Response(200, json={"ok": True})
            client = self.client(partial(handler, recorded=calls))
            with patch("adx_environment._transport.time.sleep"):
                if replayable:
                    client.upload_bytes_direct("env", b"payload", "/file", 10)
                    self.assertEqual([r.content for r in calls], [b"payload", b"payload"])
                    self.assertEqual(calls[0].headers["X-ADX-Request-ID"], calls[1].headers["X-ADX-Request-ID"])
                    self.assertEqual(calls[1].extensions["timeout"]["read"], 15)
                else:
                    with self.assertRaises(EnvironmentHTTPError) as raised:
                        client.upload_stream_direct("env", iter([b"payload"]), "/file", 10)
                    self.assertFalse(raised.exception.request_forwarded)
                    self.assertEqual(len(calls), 1)

    def test_attempt_limit_is_configurable_and_exhaustion_is_retryable(self):
        for limit in [1, 4]:
            calls = []

            def handler(request, recorded):
                recorded.append(request)
                return httpx.Response(503, headers={"X-ADX-Error-Code": "ENV_RESUMING"}, json={"error": {
                    "code": "ENV_RESUMING", "retry": "same_operation", "outcome": "not_started",
                    "requestForwarded": False, "requestId": request.headers["X-ADX-Request-ID"],
                }})
            with patch("adx_environment._transport.time.sleep"):
                with self.assertRaises(EnvironmentHTTPError) as raised:
                    client = self.client(partial(handler, recorded=calls), attempts=limit)
                    client.invoke("env", "side_effect", timeout=10)
            self.assertEqual(len(calls), limit)
            self.assertEqual(raised.exception.code, "ENV_RESUMING")
            self.assertFalse(raised.exception.request_forwarded)
        for value in [0, -1, True, 1.5, "3"]:
            with self.assertRaises(ValueError):
                ConnectionConfig(server_address="ingress", token="t", request_max_attempts=value)
        with patch.dict(
            "os.environ", {"ADX_SERVER_ADDRESS": "ingress", "ADX_TOKEN": "t", "ADX_REQUEST_MAX_ATTEMPTS": "4"}
        ):
            self.assertEqual(ConnectionConfig.from_env().request_max_attempts, 4)

    def test_forwarded_deadline_exceeded_never_enters_safe_resume_retry(self):
        calls = []
        def handler(request):
            calls.append(request)
            return httpx.Response(504, json={"error": {
                "code": "PROCESSING_DEADLINE_EXCEEDED", "retry": "never", "outcome": "unknown",
                "requestForwarded": True,
            }})
        with self.assertRaises(EnvironmentHTTPError) as raised:
            self.client(handler).invoke("env", "side_effect", timeout=10)
        self.assertEqual(len(calls), 1)
        self.assertTrue(raised.exception.request_forwarded)
