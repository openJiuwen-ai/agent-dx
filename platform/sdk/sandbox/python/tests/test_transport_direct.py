"""Offline tests for the HTTP-direct-via-frontend /direct path in SandboxClient.

No cluster required: an httpx.MockTransport stands in for both the direct route
(raw-result JSON) and the frontend (base64 BuildJobResponse envelope), so we
can assert direct-vs-fallback routing, response parsing, and the sticky
disable. Run: ``python tests/test_transport_direct.py`` (or via pytest).
"""

import ast
import base64
import hashlib
import importlib.util
import inspect
import io
import json
import os
import tarfile
import tempfile
import textwrap
import urllib.request
import uuid
from pathlib import Path
from unittest.mock import patch

import httpx

os.environ.setdefault("ADX_SERVER_ADDRESS", "frontend:8889")
os.environ.setdefault("ADX_TOKEN", "test-token")

from adx_sandbox._transport import SandboxClient, SandboxError  # noqa: E402
from adx_sandbox.filesystem import Filesystem  # noqa: E402


def _check(cond: bool, msg: str) -> None:
    """Repo tests avoid bare ``assert`` (stripped under -O); raise explicitly."""
    if not cond:
        raise AssertionError(msg)


def _envelope(inner: dict) -> dict:
    """frontend BuildJobResponse: data is base64(JSON(inner))."""
    raw = base64.b64encode(json.dumps(inner).encode()).decode()
    return {"code": 200, "message": "", "data": raw}


def _make_client(handler):
    """A SandboxClient with frontend /direct enabled and a mocked transport."""
    os.environ.pop("ADX_GATEWAY_ADDRESS", None)
    os.environ["ADX_TLS"] = "0"
    c = SandboxClient()
    c._http = httpx.Client(
        transport=httpx.MockTransport(handler), headers={"X-Auth": "test-token"}
    )
    return c


def test_create_uses_sse_and_returns_running_final():
    seen = {}

    def handler(request: httpx.Request) -> httpx.Response:
        seen["accept"] = request.headers.get("accept")
        seen["body"] = json.loads(request.read())
        return httpx.Response(
            200,
            headers={"content-type": "text/event-stream"},
            text=(
                'event: accepted\n'
                'data: {"status":"creating"}\n\n'
                ': heartbeat\n\n'
                'event: final\n'
                'data: {"sandboxId":"sandbox-sse","status":"running"}\n\n'
            ),
        )

    c = _make_client(handler)
    result = c.create_info({"name": "sandbox-sse", "createTimeoutSeconds": 7})
    _check(result["sandboxId"] == "sandbox-sse", f"create result: {result}")
    _check(result["status"] == "running", f"create status: {result}")
    _check(seen["accept"] == "text/event-stream", f"Accept header: {seen}")
    _check(seen["body"]["createTimeoutSeconds"] == 7, f"request body: {seen}")
    print("ok: create uses SSE and returns running final")


def test_create_transport_default_covers_default_logical_budget():
    seen = {}

    def handler(request: httpx.Request) -> httpx.Response:
        seen["read_timeout"] = request.extensions["timeout"]["read"]
        return httpx.Response(
            200,
            headers={"content-type": "text/event-stream"},
            text=(
                'event: final\n'
                'data: {"sandboxId":"sandbox-default-timeout","status":"running"}\n\n'
            ),
        )

    c = _make_client(handler)
    result = c.create_info({"name": "sandbox-default-timeout"})
    _check(result["status"] == "running", f"create status: {result}")
    _check(119 < seen["read_timeout"] <= 120, f"request timeout: {seen}")
    print("ok: create transport default covers 90-second logical budget")


def test_create_rejects_timeout_final():
    seen = {}

    def handler(request: httpx.Request) -> httpx.Response:
        seen["request_id"] = request.headers.get("X-Request-Id")
        return httpx.Response(
            200,
            headers={"content-type": "text/event-stream"},
            text=(
                'event: accepted\n'
                'data: {"status":"creating"}\n\n'
                'event: final\n'
                'data: {"sandboxId":"sandbox-timeout","status":"timeout",'
                '"errorCode":3002,"message":"create timed out"}\n\n'
            ),
        )

    c = _make_client(handler)
    try:
        c.create_info({"createTimeoutSeconds": 3})
    except SandboxError as exc:
        _check("create timed out" in str(exc), f"timeout error: {exc}")
        _check("3002" in str(exc), f"timeout code missing: {exc}")
        _check(
            f"requestId={seen['request_id']}" in str(exc),
            f"request id missing: {exc}",
        )
        _check(
            "sandboxId=sandbox-timeout" in str(exc),
            f"sandbox id missing: {exc}",
        )
    else:
        raise AssertionError("timeout final must raise SandboxError")
    print("ok: create rejects timeout final")


def test_create_rejects_stream_without_final():
    def handler(_request: httpx.Request) -> httpx.Response:
        return httpx.Response(
            200,
            headers={"content-type": "text/event-stream"},
            text='event: accepted\ndata: {"status":"creating"}\n\n: heartbeat\n\n',
        )

    c = _make_client(handler)
    try:
        c.create_info({"createTimeoutSeconds": 3})
    except SandboxError as exc:
        _check("final" in str(exc).lower(), f"missing-final error: {exc}")
    else:
        raise AssertionError("stream without final must raise SandboxError")
    print("ok: create rejects stream without final")


def test_create_retries_broken_sse_with_stable_request_identity():
    attempts = []

    class BrokenCreateStream(httpx.SyncByteStream):
        def __iter__(self):
            yield (
                b'event: accepted\n'
                b'data: {"status":"creating"}\n\n'
            )
            raise httpx.ReadError("connection reset during chunked response")

    def handler(request: httpx.Request) -> httpx.Response:
        attempts.append(
            {
                "request_id": request.headers.get("X-Request-Id"),
                "body": json.loads(request.read()),
            }
        )
        if len(attempts) == 1:
            return httpx.Response(
                200,
                headers={"content-type": "text/event-stream"},
                stream=BrokenCreateStream(),
            )
        return httpx.Response(
            200,
            headers={"content-type": "text/event-stream"},
            text=(
                'event: final\n'
                'data: {"sandboxId":"sandbox-retried","status":"running"}\n\n'
            ),
        )

    c = _make_client(handler)
    result = c.create_info({"createTimeoutSeconds": 3})

    _check(result["sandboxId"] == "sandbox-retried", f"create result: {result}")
    _check(len(attempts) == 2, f"create attempts: {attempts}")
    _check(
        attempts[0]["request_id"] == attempts[1]["request_id"],
        f"request id changed across retries: {attempts}",
    )
    _check(
        len({item["body"]["name"] for item in attempts}) == 1,
        f"generated instance name changed across retries: {attempts}",
    )
    _check(
        attempts[0]["body"]["name"]
        == f"sandbox-{attempts[0]['request_id'].removeprefix('create-')}",
        f"generated name is not tied to the request identity: {attempts}",
    )
    print("ok: broken create SSE retries with stable request identity")


def test_create_transport_retry_is_bounded_and_preserves_user_name():
    attempts = []

    def handler(request: httpx.Request) -> httpx.Response:
        attempts.append(
            {
                "request_id": request.headers.get("X-Request-Id"),
                "body": json.loads(request.read()),
            }
        )
        raise httpx.ConnectError("connection reset", request=request)

    c = _make_client(handler)
    try:
        c.create_info(
            {
                "name": "user-sandbox",
                "createTimeoutSeconds": 3,
            }
        )
    except SandboxError as exc:
        _check("transport failed after 3 attempts" in str(exc), f"error: {exc}")
        _check(
            f"requestId={attempts[0]['request_id']}" in str(exc),
            f"request id missing: {exc}",
        )
        _check("name=user-sandbox" in str(exc), f"name missing: {exc}")
    else:
        raise AssertionError("persistent create transport failure must raise")

    _check(len(attempts) == 3, f"create attempts: {attempts}")
    _check(
        len({item["request_id"] for item in attempts}) == 1,
        f"request id changed across retries: {attempts}",
    )
    _check(
        {item["body"]["name"] for item in attempts} == {"user-sandbox"},
        f"user name changed across retries: {attempts}",
    )
    print("ok: create transport retries are bounded")


def test_create_http_error_is_not_retried():
    attempts = []

    def handler(request: httpx.Request) -> httpx.Response:
        attempts.append(request.headers.get("X-Request-Id"))
        return httpx.Response(409, json={"message": "request body conflict"})

    c = _make_client(handler)
    try:
        c.create_info({"createTimeoutSeconds": 3})
    except SandboxError as exc:
        _check("HTTP 409" in str(exc), f"unexpected HTTP error: {exc}")
    else:
        raise AssertionError("HTTP create error must raise")

    _check(len(attempts) == 1, f"HTTP error was retried: {attempts}")
    print("ok: create HTTP errors are not retried")


def test_distinct_logical_creates_use_distinct_request_ids_and_names():
    attempts = []

    def handler(request: httpx.Request) -> httpx.Response:
        body = json.loads(request.read())
        attempts.append(
            {
                "request_id": request.headers.get("X-Request-Id"),
                "body": body,
            }
        )
        return httpx.Response(
            200,
            headers={"content-type": "text/event-stream"},
            text=(
                'event: final\n'
                f'data: {{"sandboxId":"sandbox-{len(attempts)}","status":"running"}}\n\n'
            ),
        )

    c = _make_client(handler)
    c.create_info({"createTimeoutSeconds": 3})
    c.create_info({"createTimeoutSeconds": 3})

    _check(len(attempts) == 2, f"create attempts: {attempts}")
    _check(
        attempts[0]["request_id"] != attempts[1]["request_id"],
        f"logical creates reused a request id: {attempts}",
    )
    for attempt in attempts:
        operation_id = attempt["request_id"].removeprefix("create-")
        _check(
            attempt["body"]["name"] == f"sandbox-{operation_id}",
            f"generated name does not match request identity: {attempt}",
        )
        _check(
            uuid.UUID(operation_id).version == 4,
            f"create identity is not a full UUIDv4: {attempt}",
        )
    _check(
        attempts[0]["body"]["name"] != attempts[1]["body"]["name"],
        f"logical creates reused an instance name: {attempts}",
    )
    print("ok: distinct logical creates use distinct request ids and names")


def test_create_retries_gateway_503_with_stable_request_id():
    attempts = []

    def handler(request: httpx.Request) -> httpx.Response:
        attempts.append(request.headers.get("X-Request-Id"))
        if len(attempts) == 1:
            return httpx.Response(503, text="upstream temporarily unavailable")
        return httpx.Response(
            200,
            headers={"content-type": "text/event-stream"},
            text=(
                'event: final\n'
                'data: {"sandboxId":"sandbox-after-503","status":"running"}\n\n'
            ),
        )

    c = _make_client(handler)
    result = c.create_info({"createTimeoutSeconds": 3})

    _check(result["sandboxId"] == "sandbox-after-503", f"create result: {result}")
    _check(len(attempts) == 2, f"create attempts: {attempts}")
    _check(len(set(attempts)) == 1, f"request id changed across retry: {attempts}")
    print("ok: create retries gateway 503 with stable request id")


def test_delete_retries_transport_and_gateway_failures_with_stable_request_id():
    attempts = []

    def handler(request: httpx.Request) -> httpx.Response:
        attempts.append(request.headers.get("X-Request-Id"))
        if len(attempts) == 1:
            raise httpx.RemoteProtocolError(
                "server disconnected without sending a response",
                request=request,
            )
        if len(attempts) == 2:
            return httpx.Response(502, text="bad gateway")
        return httpx.Response(404, text="already deleted")

    c = _make_client(handler)
    c.delete("default-sandbox-delete")

    _check(len(attempts) == 3, f"delete attempts: {attempts}")
    _check(len(set(attempts)) == 1, f"delete request id changed: {attempts}")
    _check(
        attempts[0].startswith("delete-"),
        f"delete request id missing operation prefix: {attempts}",
    )
    print("ok: delete retries transport and gateway failures")


def test_delete_does_not_retry_non_transient_http_error():
    attempts = []

    def handler(request: httpx.Request) -> httpx.Response:
        attempts.append(request.headers.get("X-Request-Id"))
        return httpx.Response(409, text="delete conflict")

    c = _make_client(handler)
    try:
        c.delete("default-sandbox-conflict")
    except SandboxError as exc:
        _check("HTTP 409 delete conflict" in str(exc), f"delete error: {exc}")
        _check("requestId=delete-" in str(exc), f"request id missing: {exc}")
    else:
        raise AssertionError("non-transient delete error must raise")

    _check(len(attempts) == 1, f"non-transient delete was retried: {attempts}")
    print("ok: delete does not retry non-transient HTTP errors")


def test_delete_transport_retries_are_bounded_and_contextual():
    attempts = []

    def handler(request: httpx.Request) -> httpx.Response:
        attempts.append(request.headers.get("X-Request-Id"))
        raise httpx.ConnectTimeout("TLS handshake timed out", request=request)

    c = _make_client(handler)
    try:
        c.delete("default-sandbox-timeout")
    except SandboxError as exc:
        _check("failed after 3 attempts" in str(exc), f"attempts missing: {exc}")
        _check("requestId=delete-" in str(exc), f"request id missing: {exc}")
        _check("TLS handshake timed out" in str(exc), f"cause missing: {exc}")
    else:
        raise AssertionError("persistent delete transport failure must raise")

    _check(len(attempts) == 3, f"delete attempts: {attempts}")
    _check(len(set(attempts)) == 1, f"request id changed: {attempts}")
    print("ok: delete transport retries are bounded and contextual")


def test_pause_and_resume_retry_with_one_internal_request_id_per_call():
    pause_attempts = []
    resume_attempts = []

    def handler(request: httpx.Request) -> httpx.Response:
        request_id = request.headers.get("X-ADX-Request-ID")
        body = json.loads(request.read())
        if request.url.path.endswith("/pause"):
            pause_attempts.append((request_id, body))
            if len(pause_attempts) == 1:
                raise httpx.ReadError("pause response lost", request=request)
            if len(pause_attempts) == 2:
                return httpx.Response(502, text="bad gateway")
            return httpx.Response(
                200,
                json=_envelope(
                    {
                        "sandboxId": "default-sandbox-1",
                        "snapshotId": request_id,
                        "size": 8192,
                        "state": "paused",
                        "expiresAt": 1_800_000_000,
                    }
                ),
            )
        if request.url.path.endswith("/resume"):
            resume_attempts.append((request_id, body))
            if len(resume_attempts) == 1:
                return httpx.Response(503, text="temporarily unavailable")
            return httpx.Response(
                200,
                json=_envelope(
                    {
                        "sandboxId": "default-sandbox-1",
                        "state": "running",
                        "routeAddress": "10.0.0.8:9000",
                        "functionProxyId": "proxy-a",
                        "nodeId": "node-a",
                        "portMappings": {"8080": 41080},
                    }
                ),
            )
        raise AssertionError(f"unexpected lifecycle path: {request.url.path}")

    client = _make_client(handler)
    _check(callable(getattr(client, "pause", None)), "SandboxClient.pause must exist")
    _check(callable(getattr(client, "resume", None)), "SandboxClient.resume must exist")
    pause = client.pause("default-sandbox-1", 90_000)
    resume = client.resume("default-sandbox-1")

    _check(len(pause_attempts) == 3, f"pause attempts: {pause_attempts}")
    _check(
        len({attempt[0] for attempt in pause_attempts}) == 1,
        f"pause request id changed: {pause_attempts}",
    )
    _check(
        pause_attempts[0][0].startswith("pause-"),
        f"pause request id: {pause_attempts}",
    )
    _check(
        {attempt[1]["ttlSeconds"] for attempt in pause_attempts} == {90_000},
        f"pause ttl changed: {pause_attempts}",
    )
    _check(
        pause["snapshotId"] == pause_attempts[0][0],
        f"snapshot id did not reuse request id: {pause}",
    )
    _check(len(resume_attempts) == 2, f"resume attempts: {resume_attempts}")
    _check(
        len({attempt[0] for attempt in resume_attempts}) == 1,
        f"resume request id changed: {resume_attempts}",
    )
    _check(
        resume_attempts[0][0].startswith("resume-"),
        f"resume request id: {resume_attempts}",
    )
    _check(
        pause_attempts[0][0] != resume_attempts[0][0],
        "distinct lifecycle calls reused one request id",
    )
    _check(resume["state"] == "running", f"resume result: {resume}")
    print("ok: pause and resume retry with stable internal request identity")


def test_pause_rejects_snapshot_identity_not_owned_by_internal_request():
    seen_request_id = []

    def handler(request: httpx.Request) -> httpx.Response:
        seen_request_id.append(request.headers.get("X-ADX-Request-ID"))
        return httpx.Response(
            200,
            json=_envelope(
                {
                    "sandboxId": "default-sandbox-1",
                    "snapshotId": "pause-someone-elses-request",
                    "size": 8192,
                    "state": "paused",
                    "expiresAt": 1_800_000_000,
                }
            ),
        )

    client = _make_client(handler)
    try:
        client.pause("default-sandbox-1", 90_000)
        raise AssertionError("mismatched pause snapshot identity unexpectedly succeeded")
    except SandboxError as exc:
        _check("does not match" in str(exc), f"unexpected pause identity error: {exc}")
    _check(len(seen_request_id) == 1, f"business mismatch was retried: {seen_request_id}")
    print("ok: pause rejects mismatched authoritative snapshot identity")


def test_sandbox_create_timeout_precedence_and_body():
    import adx_sandbox.sandbox_api as sandbox_api

    original_client = sandbox_api.SandboxClient
    original_env = os.environ.get("ADX_SANDBOX_CREATE_TIMEOUT")
    seen = []

    class FakeClient:
        def create_info(self, body):
            seen.append(dict(body))
            return {"sandboxId": f"sandbox-{len(seen)}", "status": "running"}

        def delete(self, _sandbox_id):
            pass

        def close(self):
            pass

    try:
        sandbox_api.SandboxClient = FakeClient
        os.environ["ADX_SANDBOX_CREATE_TIMEOUT"] = "500"
        explicit = sandbox_api.Sandbox(
            image="python:3.12-slim",
            create_timeout=400,
            runtime="kata",
            detached=True,
        )
        schedule_only = sandbox_api.Sandbox(
            image="python:3.12-slim",
            schedule_timeout=45,
            detached=True,
        )
        inherited = sandbox_api.Sandbox(image="python:3.12-slim", detached=True)
        os.environ.pop("ADX_SANDBOX_CREATE_TIMEOUT", None)
        derived = sandbox_api.Sandbox(
            image="python:3.12-slim",
            schedule_timeout=45,
            detached=True,
        )
        legacy = sandbox_api.Sandbox(
            image="python:3.12-slim",
            create_timeout=180,
            schedule_timeout=120,
            detached=True,
        )
        os.environ["ADX_SANDBOX_CREATE_TIMEOUT"] = "60"
        legacy_env = sandbox_api.Sandbox(
            image="python:3.12-slim",
            detached=True,
        )
        explicit.kill()
        schedule_only.kill()
        inherited.kill()
        derived.kill()
        legacy.kill()
        legacy_env.kill()
    finally:
        sandbox_api.SandboxClient = original_client
        if original_env is None:
            os.environ.pop("ADX_SANDBOX_CREATE_TIMEOUT", None)
        else:
            os.environ["ADX_SANDBOX_CREATE_TIMEOUT"] = original_env

    _check(seen[0]["createTimeoutSeconds"] == 400, f"explicit timeout body: {seen[0]}")
    _check(seen[0]["scheduleTimeoutSeconds"] == 30, f"default schedule timeout body: {seen[0]}")
    _check(seen[0]["initCallTimeoutSeconds"] == 30, f"init timeout body: {seen[0]}")
    _check(seen[0]["rootfs"]["runtime"] == "kata", f"runtime body: {seen[0]}")
    _check("runtime" not in seen[0], f"top-level runtime body: {seen[0]}")
    _check(seen[1]["createTimeoutSeconds"] == 500, f"env create timeout body: {seen[1]}")
    _check(seen[1]["scheduleTimeoutSeconds"] == 45, f"explicit schedule timeout body: {seen[1]}")
    _check(seen[2]["createTimeoutSeconds"] == 500, f"env timeout body: {seen[2]}")
    _check(seen[2]["scheduleTimeoutSeconds"] == 30, f"default schedule timeout body: {seen[2]}")
    _check("runtime" not in seen[2]["rootfs"], f"default isolation runtime body: {seen[2]}")
    _check("runtime" not in seen[2], f"top-level runtime body: {seen[2]}")
    _check(seen[3]["createTimeoutSeconds"] == 105, f"derived create timeout body: {seen[3]}")
    _check(seen[3]["scheduleTimeoutSeconds"] == 45, f"derived schedule timeout body: {seen[3]}")
    _check(seen[4]["createTimeoutSeconds"] == 180, f"legacy create timeout body: {seen[4]}")
    _check(seen[4]["scheduleTimeoutSeconds"] == 120, f"legacy schedule timeout body: {seen[4]}")
    _check(seen[5]["createTimeoutSeconds"] == 90, f"legacy env timeout body: {seen[5]}")
    _check(seen[5]["scheduleTimeoutSeconds"] == 30, f"legacy env schedule body: {seen[5]}")
    print("ok: Sandbox create timeout precedence and body")


def test_sandbox_create_timeout_validation():
    import adx_sandbox.sandbox_api as sandbox_api

    invalid = (
        (
            {"create_timeout": 60, "schedule_timeout": 40},
            "create_timeout - schedule_timeout must be at least 30",
        ),
        ({"schedule_timeout": -1}, "schedule_timeout must be a positive integer"),
        ({"schedule_timeout": 0}, "schedule_timeout must be a positive integer"),
        (
            {"create_timeout": 400, "schedule_timeout": 410},
            "schedule_timeout must be less than or equal to create_timeout",
        ),
        (
            {"create_timeout": 100, "schedule_timeout": 80},
            "create_timeout - schedule_timeout must be at least 30",
        ),
    )
    for kwargs, message in invalid:
        try:
            sandbox_api.Sandbox(
                image="python:3.12-slim",
                detached=True,
                **kwargs,
            )
        except ValueError as exc:
            _check(str(exc) == message, f"unexpected validation error: {exc}")
        else:
            raise AssertionError(f"invalid timeout combination accepted: {kwargs}")
    print("ok: Sandbox create timeout validation")


def test_direct_success_no_fallback():
    calls = []

    def handler(request: httpx.Request) -> httpx.Response:
        calls.append(request.url.path)
        if request.url.path.startswith("/api/sandbox/v1"):
            raise AssertionError("frontend must NOT be called on direct success")
        # frontend /direct raw-result path: /direct/{safeID}/invoke
        return httpx.Response(200, json={"exists": True})

    c = _make_client(handler)
    out = c.invoke("sandbox-demo", "file.exists", {"path": "/"})
    _check(out == {"exists": True}, f"direct result: {out}")
    _check(calls == ["/direct/sandbox-demo/invoke"], f"calls: {calls}")
    print("ok: direct success, no fallback ->", calls)


def test_direct_503_retries_with_same_request_id():
    calls = []

    def handler(request: httpx.Request) -> httpx.Response:
        body = json.loads(request.content)
        calls.append(
            (
                request.url.path,
                request.headers.get("x-adx-request-id"),
                body.get("requestId"),
            )
        )
        if request.url.path.startswith("/api/sandbox/v1"):
            raise AssertionError("retryable direct response must not use fallback")
        if len(calls) < 3:
            return httpx.Response(503, json={"error": "route unavailable"})
        return httpx.Response(200, json={"created": True})

    c = _make_client(handler)
    out = c.invoke("sandbox-demo", "process.exec", {"cmd": ["touch", "/tmp/once"]})
    _check(out == {"created": True}, f"retry result: {out}")
    _check(
        len(calls) == 3
        and all(call[0] == "/direct/sandbox-demo/invoke" for call in calls),
        f"expected three direct attempts: {calls}",
    )
    _check(
        len({call[1] for call in calls}) == 1
        and all(call[1] == call[2] for call in calls),
        f"direct retries must reuse request id: {calls}",
    )
    _check(c._direct_disabled is False, "transient 503 must not disable direct")
    print("ok: direct 503 retries with one request id ->", calls)


def test_direct_read_timeout_retries_with_same_request_id():
    calls = []

    def handler(request: httpx.Request) -> httpx.Response:
        body = json.loads(request.content)
        calls.append(
            (
                request.url.path,
                request.headers.get("x-adx-request-id"),
                body.get("requestId"),
            )
        )
        if request.url.path.startswith("/api/sandbox/v1"):
            raise AssertionError("unknown-outcome retry must not use fallback")
        if len(calls) == 1:
            raise httpx.ReadTimeout(
                "response lost after possible execution",
                request=request,
            )
        return httpx.Response(200, json={"started": True})

    c = _make_client(handler)
    out = c.invoke("sandbox-demo", "process.start", {"cmd": ["side-effect"]})
    _check(out == {"started": True}, f"retry result: {out}")
    _check(
        len(calls) == 2
        and all(call[0] == "/direct/sandbox-demo/invoke" for call in calls),
        f"read timeout should retry direct once: {calls}",
    )
    _check(
        calls[0][1] == calls[0][2] == calls[1][1] == calls[1][2],
        f"read timeout retry must reuse request id: {calls}",
    )
    _check(c._direct_disabled is False, "read timeout must not disable direct")
    print("ok: direct read timeout retries with one request id ->", calls)


def test_direct_unknown_outcome_retry_exhaustion_does_not_fallback():
    calls = []

    def handler(request: httpx.Request) -> httpx.Response:
        body = json.loads(request.content)
        calls.append(
            (
                request.url.path,
                request.headers.get("x-adx-request-id"),
                body.get("requestId"),
            )
        )
        if request.url.path.startswith("/api/sandbox/v1"):
            raise AssertionError("unknown outcome must not use RuntimeRPC fallback")
        raise httpx.RemoteProtocolError(
            "server disconnected without a response",
            request=request,
        )

    c = _make_client(handler)
    try:
        c.invoke("sandbox-demo", "shell.run", {"command": "side-effect"})
    except SandboxError as exc:
        _check("outcome is unknown" in str(exc), f"unexpected error: {exc}")
        _check(calls[0][1] in str(exc), f"request id missing from error: {exc}")
    else:
        raise AssertionError("exhausted unknown-outcome retries must fail")
    _check(len(calls) == 3, f"expected three direct attempts: {calls}")
    _check(
        len({call[1] for call in calls}) == 1
        and all(call[1] == call[2] for call in calls),
        f"exhausted retries must reuse request id: {calls}",
    )
    _check(
        c._direct_disabled is False,
        "unknown outcome must not permanently disable direct",
    )
    print("ok: exhausted unknown-outcome retries do not fall back ->", calls)


def test_direct_mixed_failures_preserve_unknown_outcome():
    for first in ("read", "invalid-json", "gateway"):
        for later in ("connect", "pool", "missing"):
            calls = []

            def handler(request):
                body = json.loads(request.content)
                calls.append((request.url.path, body["requestId"],
                              request.headers["x-adx-request-id"]))
                _check(request.url.path.startswith("/direct/"),
                       f"unknown outcome crossed to RuntimeRPC: {calls}")
                if len(calls) == 1:
                    if first == "read":
                        raise httpx.RemoteProtocolError("response lost", request=request)
                    if first == "invalid-json":
                        return httpx.Response(200, content=b"incomplete-json")
                    return httpx.Response(502, text="upstream lost")
                if later == "connect":
                    raise httpx.ConnectError("connection refused", request=request)
                if later == "pool":
                    raise httpx.PoolTimeout("pool exhausted", request=request)
                return httpx.Response(404)

            client = _make_client(handler)
            try:
                with patch("adx_sandbox._transport.time.sleep"):
                    try:
                        client.invoke("sandbox-demo", "process.start", {"command": "append"})
                    except SandboxError as error:
                        _check("outcome is unknown" in str(error), str(error))
                        _check(error.request_id == calls[0][1], "request id lost")
                    else:
                        raise AssertionError("mixed unknown outcome must fail")
                _check(len(calls) == (2 if later == "missing" else 3), str(calls))
                _check(len({call[1] for call in calls}) == 1, str(calls))
                _check(all(call[1] == call[2] for call in calls), str(calls))
                _check(not client._direct_disabled, "mixed failure disabled direct")
            finally:
                client.close()


def test_direct_non_retryable_http_errors_do_not_fallback():
    for status in (400, 401, 403, 409, 429, 500):
        calls = []

        def handler(
            request: httpx.Request,
            status=status,
            calls=calls,
        ) -> httpx.Response:
            calls.append(request.url.path)
            if request.url.path.startswith("/api/sandbox/v1"):
                return httpx.Response(200, json=_envelope({"ok": True}))
            return httpx.Response(status, json={"error": f"status-{status}"})

        c = _make_client(handler)
        try:
            c.invoke("sandbox-demo", "process.exec", {"cmd": ["side-effect"]})
        except SandboxError as exc:
            _check(f"HTTP {status}" in str(exc), f"unexpected {status} error: {exc}")
        else:
            raise AssertionError(f"HTTP {status} must not fall back")
        _check(
            calls == ["/direct/sandbox-demo/invoke"],
            f"HTTP {status} retried through frontend: {calls}",
        )
        _check(c._direct_disabled is False, f"HTTP {status} should not disable direct")
    print("ok: direct non-retryable HTTP responses do not fall back")


def test_direct_invalid_json_retries_with_same_request_id():
    calls = []

    def handler(request: httpx.Request) -> httpx.Response:
        body = json.loads(request.content)
        calls.append(
            (
                request.url.path,
                request.headers.get("x-adx-request-id"),
                body.get("requestId"),
            )
        )
        if request.url.path.startswith("/api/sandbox/v1"):
            raise AssertionError("invalid direct response must not use fallback")
        if len(calls) == 1:
            return httpx.Response(200, content=b"not-json")
        return httpx.Response(200, json={"ok": True})

    c = _make_client(handler)
    out = c.invoke("sandbox-demo", "process.exec", {"cmd": ["side-effect"]})
    _check(out == {"ok": True}, f"invalid JSON retry result: {out}")
    _check(
        len(calls) == 2
        and all(call[0] == "/direct/sandbox-demo/invoke" for call in calls),
        f"invalid response should retry direct: {calls}",
    )
    _check(
        calls[0][1] == calls[0][2] == calls[1][1] == calls[1][2],
        f"invalid response retry must reuse request id: {calls}",
    )
    _check(c._direct_disabled is False, "invalid response must not disable direct")
    print("ok: invalid direct response retries with one request id ->", calls)


def test_direct_connect_error_falls_back():
    calls = []

    def handler(request: httpx.Request) -> httpx.Response:
        body = json.loads(request.content)
        calls.append(
            (
                request.url.path,
                request.headers.get("x-adx-request-id"),
                body.get("requestId"),
            )
        )
        if request.url.path.startswith("/api/sandbox/v1"):
            return httpx.Response(200, json=_envelope({"ok": 1}))
        raise httpx.ConnectError("direct route down")

    c = _make_client(handler)
    out = c.invoke("sandbox-demo", "file.exists", {"path": "/"})
    _check(out == {"ok": 1}, f"connect-error fallback: {out}")
    _check(c._direct_disabled is True, "connect error should sticky-disable")
    _check(
        len(calls) == 4
        and all(
            call[0] == "/direct/sandbox-demo/invoke"
            for call in calls[:3]
        ),
        f"connect error should retry direct three times: {calls}",
    )
    _check(calls[3][0].startswith("/api/sandbox/v1"), f"fallback: {calls}")
    _check(
        len({call[1] for call in calls}) == 1
        and all(call[1] == call[2] for call in calls),
        f"fallback must reuse the logical request id: {calls}",
    )
    print("ok: connect-error fallback ->", calls)


def test_direct_pool_timeout_retries_then_falls_back():
    calls = []

    def handler(request: httpx.Request) -> httpx.Response:
        body = json.loads(request.content)
        calls.append(
            (
                request.url.path,
                request.headers.get("x-adx-request-id"),
                body.get("requestId"),
            )
        )
        if request.url.path.startswith("/api/sandbox/v1"):
            return httpx.Response(200, json=_envelope({"ok": 1}))
        raise httpx.PoolTimeout("connection pool exhausted", request=request)

    c = _make_client(handler)
    out = c.invoke("sandbox-demo", "file.exists", {"path": "/"})
    _check(out == {"ok": 1}, f"pool-timeout fallback: {out}")
    _check(c._direct_disabled is True, "pool timeout should sticky-disable")
    _check(
        len(calls) == 4
        and all(
            call[0] == "/direct/sandbox-demo/invoke"
            for call in calls[:3]
        )
        and calls[3][0].startswith("/api/sandbox/v1"),
        f"pool timeout retry/fallback calls: {calls}",
    )
    _check(
        len({call[1] for call in calls}) == 1
        and all(call[1] == call[2] for call in calls),
        f"pool-timeout fallback must reuse request id: {calls}",
    )
    print("ok: pool timeout retries then falls back ->", calls)


def test_direct_fallback_when_frontend_direct_missing():
    os.environ.pop("ADX_GATEWAY_ADDRESS", None)
    calls = []

    def handler(request: httpx.Request) -> httpx.Response:
        body = json.loads(request.content)
        calls.append(
            (
                request.url.path,
                request.headers.get("x-adx-request-id"),
                body.get("requestId"),
            )
        )
        if request.url.path.startswith("/api/sandbox/v1"):
            return httpx.Response(200, json=_envelope({"exists": False}))
        return httpx.Response(404, json={"error": "direct route missing"})

    os.environ["ADX_TLS"] = "0"
    c = SandboxClient()
    c._http = httpx.Client(
        transport=httpx.MockTransport(handler), headers={"X-Auth": "t"}
    )
    _check(
        c.direct_enabled is True,
        "direct should default on through frontend /direct",
    )
    with patch("adx_sandbox._transport.logger.warning") as warning:
        for invocation in range(1, 5):
            out = c.invoke("sandbox-demo", "file.exists", {"path": "/"})
            _check(out == {"exists": False}, f"frontend-only result: {out}")
            _check(
                c._direct_disabled is (invocation > 3),
                f"unexpected direct state after 404 #{invocation}",
            )
            _check(
                warning.call_count == (1 if invocation > 3 else 0),
                "warning must only be emitted when direct is disabled: "
                f"{warning.call_args_list}",
            )

        # Once the fourth consecutive miss exhausts the failure budget, later
        # invokes use the frontend path without probing direct or warning again.
        out = c.invoke("sandbox-demo", "file.exists", {"path": "/"})
        _check(out == {"exists": False}, f"disabled-direct result: {out}")
        _check(
            warning.call_count == 1,
            f"duplicate disable warning: {warning.call_args_list}",
        )
    _check(
        calls[0][0] == "/direct/sandbox-demo/invoke",
        f"first call direct: {calls}",
    )
    _check(calls[1][0].startswith("/api/sandbox/v1"), f"fallback: {calls}")
    _check(len(calls) == 9, f"unexpected direct/fallback call count: {calls}")
    for offset in range(0, 8, 2):
        direct_call = calls[offset]
        fallback_call = calls[offset + 1]
        _check(
            direct_call[0] == "/direct/sandbox-demo/invoke"
            and fallback_call[0].startswith("/api/sandbox/v1"),
            f"404 must immediately fall back: {calls}",
        )
        _check(
            direct_call[1]
            == direct_call[2]
            == fallback_call[1]
            == fallback_call[2],
            f"404 fallback must reuse the logical request id: {calls}",
        )
    _check(
        calls[-1][0].startswith("/api/sandbox/v1"),
        f"disabled direct should use frontend only: {calls}",
    )
    print("ok: direct disables only after four consecutive 404s ->", calls)


def test_direct_success_resets_route_miss_count():
    direct_statuses = [404, 404, 200, 404, 404, 404, 200]
    direct_calls = 0

    def handler(request: httpx.Request) -> httpx.Response:
        nonlocal direct_calls
        if request.url.path.startswith("/api/sandbox/v1"):
            return httpx.Response(200, json=_envelope({"via": "fallback"}))
        status = direct_statuses[direct_calls]
        direct_calls += 1
        if status == 404:
            return httpx.Response(404, json={"error": "direct route missing"})
        return httpx.Response(200, json={"via": "direct"})

    c = _make_client(handler)
    results = [
        c.invoke("sandbox-demo", "file.exists", {"path": "/"})
        for _ in direct_statuses
    ]

    _check(direct_calls == len(direct_statuses), f"direct calls: {direct_calls}")
    _check(results[2] == {"via": "direct"}, f"first direct success: {results}")
    _check(results[-1] == {"via": "direct"}, f"second direct success: {results}")
    _check(c._direct_route_misses == 0, "direct success must reset route misses")
    _check(
        c._direct_disabled is False,
        "separated route-miss runs must not disable direct",
    )
    print("ok: direct success resets route-miss count ->", results)


def test_direct_binary_upload_success():
    calls = []

    def handler(request: httpx.Request) -> httpx.Response:
        body = request.read()
        calls.append((request.url.path, request.url.params.get("path"), body))
        if request.url.path.startswith("/api/sandbox/v1"):
            raise AssertionError("binary upload must not use frontend invoke")
        return httpx.Response(
            200,
            json={
                "error": None,
                "name": "blob.bin",
                "path": request.url.params.get("path"),
                "type": "file",
                "size": len(body),
            },
        )

    c = _make_client(handler)
    out = c.upload_bytes_direct("sandbox-demo", b"binary-payload", "/tmp/blob.bin")
    _check(out["size"] == len(b"binary-payload"), f"upload result: {out}")
    _check(
        calls == [("/direct/sandbox-demo/upload", "/tmp/blob.bin", b"binary-payload")],
        f"upload calls: {calls}",
    )
    print("ok: direct binary upload ->", calls)


def test_files_write_bytes_uses_direct_upload():
    calls = []

    def handler(request: httpx.Request) -> httpx.Response:
        body = request.read()
        calls.append((request.url.path, request.url.params.get("path"), len(body)))
        _check(
            request.url.path == "/direct/sandbox-demo/upload",
            f"bytes write should use upload, got {request.url.path}",
        )
        return httpx.Response(
            200,
            json={
                "error": None,
                "name": "large.bin",
                "path": request.url.params.get("path"),
                "type": "file",
                "size": len(body),
            },
        )

    c = _make_client(handler)
    info = Filesystem(c, "sandbox-demo").write("/tmp/large.bin", b"x")
    _check(info.size == 1, f"upload-backed write size: {info}")
    _check(calls == [("/direct/sandbox-demo/upload", "/tmp/large.bin", 1)], f"calls: {calls}")
    print("ok: bytes write uses direct upload ->", calls)


def test_files_write_text_uses_direct_upload():
    calls = []

    def handler(request: httpx.Request) -> httpx.Response:
        body = request.read()
        calls.append((request.url.path, request.url.params.get("path"), body))
        _check(
            request.url.path == "/direct/sandbox-demo/upload",
            f"text write should use upload, got {request.url.path}",
        )
        return httpx.Response(
            200,
            json={
                "error": None,
                "name": "text.txt",
                "path": request.url.params.get("path"),
                "type": "file",
                "size": len(body),
            },
        )

    c = _make_client(handler)
    info = Filesystem(c, "sandbox-demo").write("/tmp/text.txt", "héllo")
    _check(info.size == len("héllo".encode("utf-8")), f"upload-backed text size: {info}")
    _check(
        calls == [("/direct/sandbox-demo/upload", "/tmp/text.txt", "héllo".encode("utf-8"))],
        f"calls: {calls}",
    )
    print("ok: text write uses direct upload ->", calls)


def test_copy_from_local_small_file_uses_single_direct_upload():
    calls = []

    def handler(request: httpx.Request) -> httpx.Response:
        body = request.read()
        calls.append(
            (
                request.method,
                request.url.path,
                request.url.params.get("path"),
                request.url.params.get("uploadId"),
                body,
            )
        )
        return httpx.Response(
            200,
            json={
                "error": None,
                "name": "up.dat",
                "path": request.url.params.get("path"),
                "type": "file",
                "size": len(body),
            },
        )

    c = _make_client(handler)
    with tempfile.NamedTemporaryFile("wb", delete=True) as f:
        f.write(b"upload-file")
        f.flush()
        Filesystem(c, "sandbox-demo").copy_from_local(f.name, "/tmp/up.dat")

    _check(
        calls == [
            (
                "POST",
                "/direct/sandbox-demo/upload",
                "/tmp/up.dat",
                None,
                b"upload-file",
            )
        ],
        f"small file should use one upload request: {calls}",
    )
    print("ok: copy_from_local small file uses single upload ->", calls)


def test_copy_from_local_file_uses_resumable_direct_upload_above_threshold():
    calls = []

    def handler(request: httpx.Request) -> httpx.Response:
        body = request.read()
        calls.append(
            (
                request.method,
                request.url.path,
                request.url.params.get("path"),
                request.url.params.get("uploadId") is not None,
                request.url.params.get("offset"),
                body,
            )
        )
        if request.method == "GET" and request.url.path.endswith("/upload/status"):
            return httpx.Response(200, json={"error": None, "offset": 0})
        if request.method == "POST" and request.url.path.endswith("/upload/commit"):
            return httpx.Response(
                200,
                json={
                    "error": None,
                    "name": "up.dat",
                    "path": request.url.params.get("path"),
                    "type": "file",
                    "size": len(b"upload-file"),
                    "committed": True,
                },
            )
        return httpx.Response(
            200,
            json={
                "error": None,
                "path": request.url.params.get("path"),
                "type": "file",
                "offset": len(body),
                "bytes_written": len(body),
            },
        )

    c = _make_client(handler)
    c._resume_min_size = 1
    with tempfile.NamedTemporaryFile("wb", delete=True) as f:
        f.write(b"upload-file")
        f.flush()
        Filesystem(c, "sandbox-demo").copy_from_local(f.name, "/tmp/up.dat")

    _check(
        calls[0][0:3] == ("GET", "/direct/sandbox-demo/upload/status", "/tmp/up.dat"),
        f"status call: {calls}",
    )
    _check(
        calls[1][0:5]
        == ("POST", "/direct/sandbox-demo/upload", "/tmp/up.dat", True, "0"),
        f"chunk call: {calls}",
    )
    _check(calls[1][5] == b"upload-file", f"uploaded body: {calls}")
    _check(
        calls[2][0:3]
        == ("POST", "/direct/sandbox-demo/upload/commit", "/tmp/up.dat"),
        f"commit call: {calls}",
    )
    print("ok: copy_from_local large file uses resumable upload ->", calls)

def test_files_read_uses_direct_download():
    calls = []

    def handler(request: httpx.Request) -> httpx.Response:
        calls.append((request.method, request.url.path, request.url.params.get("path")))
        _check(
            request.url.path == "/direct/sandbox-demo/download",
            f"read should use download, got {request.url.path}",
        )
        return httpx.Response(200, content=b"hello\xff")

    c = _make_client(handler)
    fs = Filesystem(c, "sandbox-demo")
    _check(
        fs.read("/tmp/read.bin", format="bytes") == b"hello\xff",
        "bytes read mismatch",
    )
    _check(fs.read("/tmp/read.bin") == "hello�", "text read mismatch")
    _check(
        calls == [
            ("GET", "/direct/sandbox-demo/download", "/tmp/read.bin"),
            ("GET", "/direct/sandbox-demo/download", "/tmp/read.bin"),
        ],
        f"download calls: {calls}",
    )
    print("ok: files.read uses direct download ->", calls)


def test_copy_to_local_file_uses_direct_download():
    calls = []

    def handler(request: httpx.Request) -> httpx.Response:
        calls.append((request.method, request.url.path, request.url.params.get("path")))
        if request.url.path.endswith("/invoke"):
            return httpx.Response(
                200,
                json={
                    "name": "down.dat",
                    "path": "/tmp/down.dat",
                    "type": "file",
                    "size": 13,
                    "permissions": "",
                    "modified_time": 0,
                },
            )
        return httpx.Response(200, content=b"download-file")

    c = _make_client(handler)
    with tempfile.NamedTemporaryFile(delete=True) as f:
        target = f.name
    Filesystem(c, "sandbox-demo").copy_to_local("/tmp/down.dat", target)
    try:
        with open(target, "rb") as f:
            _check(f.read() == b"download-file", "downloaded file mismatch")
    finally:
        if os.path.exists(target):
            os.unlink(target)

    _check(calls[0][1] == "/direct/sandbox-demo/invoke", f"stat call: {calls}")
    _check(calls[1] == ("GET", "/direct/sandbox-demo/download", "/tmp/down.dat"), f"download call: {calls}")
    print("ok: copy_to_local file uses direct download ->", calls)


def test_copy_from_local_dir_streams_direct_tar_upload():
    calls = []

    def handler(request: httpx.Request) -> httpx.Response:
        body = request.read()
        calls.append(
            (
                request.url.path,
                request.url.params.get("path"),
                request.url.params.get("type"),
                request.headers.get("content-length"),
                request.headers.get("transfer-encoding"),
                body,
            )
        )
        names = []
        with tarfile.open(fileobj=io.BytesIO(body), mode="r") as tar:
            names = sorted(m.name for m in tar.getmembers())
        _check(names == ["a.txt"], f"tar names: {names}")
        return httpx.Response(
            200,
            json={"error": None, "name": "remote-dir", "path": request.url.params.get("path"), "type": "dir", "size": len(body)},
        )

    c = _make_client(handler)
    with tempfile.TemporaryDirectory() as d:
        with open(os.path.join(d, "a.txt"), "wb") as f:
            f.write(b"dir-file")
        Filesystem(c, "sandbox-demo").copy_from_local(d, "/tmp/remote-dir")

    _check(calls[0][0:3] == ("/direct/sandbox-demo/upload", "/tmp/remote-dir", "tar"), f"calls: {calls}")
    _check(calls[0][3] is None, f"streamed tar should not set content-length: {calls}")
    _check(
        calls[0][4] == "chunked",
        f"streamed tar should use transfer-encoding chunked: {calls}",
    )
    print("ok: copy_from_local dir streams direct tar upload ->", calls[0][0:5])


def test_copy_to_local_dir_uses_direct_tar_download():
    calls = []
    tar_buf = io.BytesIO()
    with tarfile.open(fileobj=tar_buf, mode="w") as tar:
        data = b"from-tar"
        info = tarfile.TarInfo("nested.txt")
        info.size = len(data)
        tar.addfile(info, io.BytesIO(data))
    tar_bytes = tar_buf.getvalue()

    def handler(request: httpx.Request) -> httpx.Response:
        calls.append((request.method, request.url.path, request.url.params.get("path"), request.url.params.get("type")))
        if request.url.path.endswith("/invoke"):
            return httpx.Response(
                200,
                json={"name": "remote-dir", "path": "/tmp/remote-dir", "type": "dir", "size": 0, "permissions": "", "modified_time": 0},
            )
        _check(request.url.path == "/direct/sandbox-demo/download", f"dir download path: {request.url.path}")
        return httpx.Response(200, content=tar_bytes)

    c = _make_client(handler)
    with tempfile.TemporaryDirectory() as d:
        Filesystem(c, "sandbox-demo").copy_to_local("/tmp/remote-dir", d)
        with open(os.path.join(d, "nested.txt"), "rb") as f:
            _check(f.read() == b"from-tar", "tar download extract mismatch")

    _check(calls[0][1] == "/direct/sandbox-demo/invoke", f"stat call: {calls}")
    _check(calls[1] == ("GET", "/direct/sandbox-demo/download", "/tmp/remote-dir", "tar"), f"download call: {calls}")
    print("ok: copy_to_local dir uses direct tar download ->", calls)


def test_direct_invoke_sends_request_id_header_and_body():
    seen = {}

    def handler(request: httpx.Request) -> httpx.Response:
        seen["path"] = request.url.path
        seen["header"] = request.headers.get("x-adx-request-id")
        seen["body"] = json.loads(request.content.decode())
        return httpx.Response(200, json={"ok": True})

    c = _make_client(handler)
    out = c.invoke("sandbox-demo", "file.exists", {"path": "/"})
    _check(out == {"ok": True}, f"direct invoke result: {out}")
    _check(seen["path"] == "/direct/sandbox-demo/invoke", f"path: {seen}")
    _check(seen["header"] and seen["header"].startswith("invoke-"), f"header: {seen}")
    _check(seen["body"].get("requestId") == seen["header"], f"request id mismatch: {seen}")
    print("ok: direct invoke carries requestId")


def test_resumable_download_continues_from_part_file():
    calls = []

    def handler(request: httpx.Request) -> httpx.Response:
        calls.append((request.url.path, request.headers.get("range")))
        _check(request.url.path == "/direct/sandbox-demo/download", f"download path: {request.url.path}")
        _check(request.headers.get("range") == "bytes=4-", f"range header: {request.headers}")
        return httpx.Response(206, content=b"ef", headers={"Content-Range": "bytes 4-5/6"})

    c = _make_client(handler)
    with tempfile.TemporaryDirectory() as d:
        target = os.path.join(d, "out.bin")
        with open(target + ".part", "wb") as f:
            f.write(b"abcd")
        c.download_file_direct("sandbox-demo", "/tmp/out.bin", target)
        with open(target, "rb") as f:
            _check(f.read() == b"abcdef", "resumed download mismatch")
    _check(calls == [("/direct/sandbox-demo/download", "bytes=4-")], f"calls: {calls}")
    print("ok: resumable download continues from .part")


def test_tunnel_client_keeps_http_req_dedup_cache():
    import adx_sandbox.tunnel_client as tunnel_client

    source = inspect.getsource(tunnel_client.TunnelClient._proxy_loop)
    _check("inflight" in source and "completed" in source, "TunnelClient should cache in-flight/completed http_req frames")
    _check("if rid and rid in completed" in source, "TunnelClient should replay completed http_req ids")
    _check("if rid and rid in inflight" in source, "TunnelClient should coalesce running http_req ids")
    print("ok: TunnelClient has http_req id dedup/replay cache")


def test_direct_entry_uses_server_and_ignores_plain_gateway_entry():
    old_env = {
        name: os.environ.get(name)
        for name in (
            "ADX_SERVER_ADDRESS",
            "ADX_TLS",
            "ADX_GATEWAY_ADDRESS",
            "ADX_GATEWAY_TLS",
        )
    }
    try:
        os.environ["ADX_SERVER_ADDRESS"] = "edge-tls:8443"
        os.environ["ADX_TLS"] = "1"
        os.environ["ADX_GATEWAY_ADDRESS"] = "edge-plain:8080"
        os.environ["ADX_GATEWAY_TLS"] = "0"
        client = SandboxClient(token="test-token")
        _check(
            client._direct_base == "https://edge-tls:8443/direct",
            f"plain gateway leaked into direct entry: {client._direct_base}",
        )
        client.close()

    finally:
        for name, value in old_env.items():
            if value is None:
                os.environ.pop(name, None)
            else:
                os.environ[name] = value
    print("ok: required direct entry uses ADX_SERVER_ADDRESS, not plain gateway")


def test_reverse_tunnel_url_uses_gateway_tunnel_alias():
    import adx_sandbox.sandbox_api as sandbox_api
    import adx_sandbox.tunnel_client as tunnel_client

    os.environ["ADX_SERVER_ADDRESS"] = "frontend:8888"
    os.environ["ADX_GATEWAY_ADDRESS"] = "router:8080"
    os.environ.pop("ADX_GATEWAY_TLS", None)

    seen = {}
    original_client = sandbox_api.SandboxClient
    original_tunnel = tunnel_client.TunnelClient

    class FakeClient:
        direct_enabled = True
        rrt_port = 50090
        token = "test-token"

        def create(self, body):
            seen["create_ports"] = body.get("ports")
            seen["create_env"] = body.get("env", {})
            seen["create_tunnel"] = body.get("tunnel")
            return "sandbox-demo"

        def invoke(self, *args, **kwargs):
            return {"exists": True}

        def delete(self, sandbox_id):
            seen["deleted"] = sandbox_id

        def close(self):
            seen["closed"] = True

        @staticmethod
        def _safe_id(sandbox_id):
            return original_client._safe_id(sandbox_id)

    class FakeTunnelClient:
        def __init__(self, upstream, token=None):
            seen["upstream"] = upstream
            seen["token"] = token

        def start(self, url, timeout=60):
            seen["url"] = url
            seen["timeout"] = timeout
            return True

        def stop(self):
            seen["stopped"] = True

    try:
        sandbox_api.SandboxClient = FakeClient
        tunnel_client.TunnelClient = FakeTunnelClient
        sb = sandbox_api.Sandbox(
            image="python:3.12-slim",
            upstream="127.0.0.1:8000",
            tunnel_connect_timeout=1,
            detached=True,
        )
        _check(sb.get_tunnel_url() == "http://127.0.0.1:8766", "sandbox-side tunnel URL mismatch")
        sb.kill()
    finally:
        sandbox_api.SandboxClient = original_client
        tunnel_client.TunnelClient = original_tunnel
        os.environ.pop("ADX_GATEWAY_ADDRESS", None)
        os.environ.pop("ADX_GATEWAY_TLS", None)

    _check(
        seen["url"] == "ws://router:8080/tunnel/sandbox-demo",
        f"tunnel URL should use gateway /tunnel alias, got {seen['url']}",
    )
    _check("8765" not in seen["url"], f"tunnel URL leaked control port: {seen['url']}")
    _check(seen["token"] is None, "plaintext tunnel should not carry token by default")
    _check(
        seen["create_ports"] is None,
        f"SDK should not request RRT/tunnel control ports, got {seen['create_ports']}",
    )
    _check(
        seen["create_tunnel"] == {"enabled": True, "proxyPort": 8766},
        f"SDK should ask frontend for tunnel declaratively, got {seen.get('create_tunnel')}",
    )
    _check(
        "RRT_TUNNEL_WS_PORT" not in seen["create_env"]
        and "RRT_TUNNEL_HTTP_PORT" not in seen["create_env"],
        f"SDK must not set RRT tunnel envs, got {seen['create_env']}",
    )
    print("ok: reverse tunnel URL uses gateway alias and hides control port ->", seen["url"])



def test_reverse_tunnel_uses_frontend_returned_tunnel_metadata():
    import adx_sandbox.sandbox_api as sandbox_api
    import adx_sandbox.tunnel_client as tunnel_client

    os.environ["ADX_SERVER_ADDRESS"] = "frontend:8888"
    os.environ["ADX_GATEWAY_ADDRESS"] = "router:8080"
    os.environ["ADX_GATEWAY_TLS"] = "0"

    seen = {}
    original_client = sandbox_api.SandboxClient
    original_tunnel = tunnel_client.TunnelClient

    class FakeClient:
        direct_enabled = True
        rrt_port = 50090
        token = "test-token"

        def create_info(self, body):
            seen["create_ports"] = body.get("ports")
            seen["create_env"] = body.get("env", {})
            seen["create_tunnel"] = body.get("tunnel")
            return {
                "sandboxId": "sandbox-demo",
                "tunnel": {
                    "url": "/tunnel/frontend-returned",
                    "proxyUrl": "http://127.0.0.1:8766",
                },
            }

        def delete(self, sandbox_id):
            seen["deleted"] = sandbox_id

        def close(self):
            seen["closed"] = True

        @staticmethod
        def _safe_id(sandbox_id):
            return original_client._safe_id(sandbox_id)

    class FakeTunnelClient:
        def __init__(self, upstream, token=None):
            seen["token"] = token

        def start(self, url, timeout=60):
            seen["url"] = url
            return True

        def stop(self):
            pass

    try:
        sandbox_api.SandboxClient = FakeClient
        tunnel_client.TunnelClient = FakeTunnelClient
        sb = sandbox_api.Sandbox(
            image="python:3.12-slim",
            upstream="127.0.0.1:8000",
            proxy_port=9876,
            env={"USER_ENV": "ok"},
            tunnel_connect_timeout=1,
            detached=True,
        )
        _check(sb.get_tunnel_url() == "http://127.0.0.1:8766", "frontend proxyUrl mismatch")
        sb.kill()
    finally:
        sandbox_api.SandboxClient = original_client
        tunnel_client.TunnelClient = original_tunnel
        os.environ.pop("ADX_GATEWAY_ADDRESS", None)
        os.environ.pop("ADX_GATEWAY_TLS", None)

    _check(seen["url"] == "ws://router:8080/tunnel/frontend-returned", f"returned tunnel url mismatch: {seen['url']}")
    _check(seen["create_ports"] is None, f"SDK leaked control ports: {seen['create_ports']}")
    _check(seen["create_env"] == {"USER_ENV": "ok"}, f"SDK should not set RRT envs: {seen['create_env']}")
    _check(
        seen["create_tunnel"] == {"enabled": True, "proxyPort": 9876},
        f"declarative tunnel mismatch: {seen['create_tunnel']}",
    )
    _check(seen["token"] is None, "plaintext tunnel should not carry token")
    print("ok: reverse tunnel uses frontend-returned metadata")


def test_reverse_tunnel_example_local_server_serves_owned_ephemeral_port():
    example_path = Path(__file__).resolve().parents[1] / "examples" / "reverse_tunnel.py"
    spec = importlib.util.spec_from_file_location("reverse_tunnel_example", example_path)
    if spec is None or spec.loader is None:
        raise AssertionError(f"cannot load reverse_tunnel example from {example_path}")
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)

    server, thread, temp_dir, port = module.start_local_server(0)
    try:
        _check(port > 0, f"expected ephemeral port, got {port}")
        health = urllib.request.urlopen(
            f"http://127.0.0.1:{port}/health", timeout=2
        ).read().decode()
        index = urllib.request.urlopen(
            f"http://127.0.0.1:{port}/index.html", timeout=2
        ).read().decode()
        _check(health == "OK", f"health body mismatch: {health!r}")
        _check("Hello from local machine!" in index, "index body mismatch")
        print("ok: reverse_tunnel local server uses owned ephemeral port ->", port)
    finally:
        server.shutdown()
        server.server_close()
        thread.join(timeout=5)
        import shutil

        shutil.rmtree(temp_dir, ignore_errors=True)


def test_tunnel_large_response_example_local_server_serves_owned_ephemeral_port():
    example_path = Path(__file__).resolve().parents[1] / "examples" / "tunnel_large_response.py"
    spec = importlib.util.spec_from_file_location("tunnel_large_response_example", example_path)
    if spec is None or spec.loader is None:
        raise AssertionError(f"cannot load tunnel_large_response example from {example_path}")
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)

    server, thread, temp_dir, file_hashes, port = module.start_local_server(0)
    try:
        _check(port > 0, f"expected ephemeral port, got {port}")
        health = urllib.request.urlopen(
            f"http://127.0.0.1:{port}/health", timeout=2
        ).read()
        first_label, first_size = module.TEST_SIZES[0]
        data = urllib.request.urlopen(
            f"http://127.0.0.1:{port}/{first_label}.bin", timeout=2
        ).read()
        _check(health == b"OK", f"health body mismatch: {health!r}")
        _check(len(data) == first_size, f"large fixture size mismatch: {len(data)}")
        _check(
            hashlib.sha256(data).hexdigest() == file_hashes[first_label],
            "large fixture hash mismatch",
        )
        print("ok: tunnel_large_response local server uses owned ephemeral port ->", port)
    finally:
        server.shutdown()
        server.server_close()
        thread.join(timeout=5)
        import shutil

        shutil.rmtree(temp_dir, ignore_errors=True)


def test_tunnel_client_ignores_proxy_env_for_local_upstream():
    import adx_sandbox.tunnel_client as tunnel_client

    source = textwrap.dedent(inspect.getsource(tunnel_client.TunnelClient))
    tree = ast.parse(source)
    clients = [
        node
        for node in ast.walk(tree)
        if isinstance(node, ast.Call)
        and isinstance(node.func, ast.Attribute)
        and node.func.attr == "AsyncClient"
    ]
    _check(clients, "TunnelClient must construct an upstream HTTP client")
    for client in clients:
        trust_env = next(
            (keyword.value for keyword in client.keywords if keyword.arg == "trust_env"),
            None,
        )
        _check(
            isinstance(trust_env, ast.Constant) and trust_env.value is False,
            "TunnelClient upstream HTTP clients must ignore proxy env",
        )
    print("ok: TunnelClient ignores proxy env for local upstream")


def test_safe_id_matches_router_sanitize():
    _check(
        SandboxClient._safe_id("sandbox@v1/foo.bar_baz") == "sandbox-at-v1-foo-bar-baz",
        "safe_id sanitize mismatch",
    )
    _check(
        SandboxClient._safe_id("default-rtt") == "default-rtt", "safe_id passthrough"
    )
    print("ok: safe_id sanitize")


if __name__ == "__main__":
    test_create_uses_sse_and_returns_running_final()
    test_create_transport_default_covers_default_logical_budget()
    test_create_rejects_timeout_final()
    test_create_rejects_stream_without_final()
    test_sandbox_create_timeout_precedence_and_body()
    test_sandbox_create_timeout_validation()
    test_pause_and_resume_retry_with_one_internal_request_id_per_call()
    test_pause_rejects_snapshot_identity_not_owned_by_internal_request()
    test_direct_success_no_fallback()
    test_direct_503_retries_with_same_request_id()
    test_direct_read_timeout_retries_with_same_request_id()
    test_direct_unknown_outcome_retry_exhaustion_does_not_fallback()
    test_direct_mixed_failures_preserve_unknown_outcome()
    test_direct_non_retryable_http_errors_do_not_fallback()
    test_direct_invalid_json_retries_with_same_request_id()
    test_direct_connect_error_falls_back()
    test_direct_pool_timeout_retries_then_falls_back()
    test_direct_fallback_when_frontend_direct_missing()
    test_direct_success_resets_route_miss_count()
    test_direct_binary_upload_success()
    test_files_write_bytes_uses_direct_upload()
    test_files_write_text_uses_direct_upload()
    test_copy_from_local_small_file_uses_single_direct_upload()
    test_copy_from_local_file_uses_resumable_direct_upload_above_threshold()
    test_files_read_uses_direct_download()
    test_copy_to_local_file_uses_direct_download()
    test_direct_invoke_sends_request_id_header_and_body()
    test_resumable_download_continues_from_part_file()
    test_tunnel_client_keeps_http_req_dedup_cache()
    test_direct_entry_uses_server_and_ignores_plain_gateway_entry()
    test_copy_from_local_dir_streams_direct_tar_upload()
    test_copy_to_local_dir_uses_direct_tar_download()
    test_reverse_tunnel_url_uses_gateway_tunnel_alias()
    test_reverse_tunnel_uses_frontend_returned_tunnel_metadata()
    test_reverse_tunnel_example_local_server_serves_owned_ephemeral_port()
    test_tunnel_large_response_example_local_server_serves_owned_ephemeral_port()
    test_tunnel_client_ignores_proxy_env_for_local_upstream()
    test_safe_id_matches_router_sanitize()
    print("\nALL PASS")
