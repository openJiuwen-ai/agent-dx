"""Verify ordinary /direct file download deadlines and Range integrity."""

import json
from dataclasses import dataclass
from types import SimpleNamespace

import httpx
import pytest
from adx_environment import ConnectionConfig, _transport
from adx_environment.filesystem import Filesystem


@dataclass
class Clock:
    now: float = 0

    def advance(self, seconds):
        self.now += seconds


class TimedStream(httpx.SyncByteStream):
    def __init__(self, clock, steps, error=None):
        self.clock = clock
        self.steps = steps
        self.error = error

    def __iter__(self):
        for seconds, chunk in self.steps:
            self.clock.advance(seconds)
            if chunk:
                yield chunk
        if self.error is not None:
            raise self.error


@pytest.fixture(name="clock")
def download_clock(monkeypatch):
    fake = Clock()
    monkeypatch.setattr(_transport, "time", SimpleNamespace(monotonic=lambda: fake.now, sleep=fake.advance))
    return fake


@pytest.fixture(name="client_for")
def download_client(monkeypatch):
    monkeypatch.delenv("ADX_DIRECT_DOWNLOAD_IDLE_TIMEOUT", raising=False)
    monkeypatch.delenv("ADX_DIRECT_DOWNLOAD_TIMEOUT", raising=False)
    monkeypatch.setenv("ADX_RESUME_MAX_RETRIES", "1")
    clients = []

    def make_client(handler):
        http = httpx.Client(transport=httpx.MockTransport(handler))
        clients.append(http)
        monkeypatch.setattr(_transport, "acquire_shared_http_client", lambda *_: http)
        return _transport.EnvironmentClient(connection=ConnectionConfig(server_address="ingress", token="test-token"))

    yield make_client

    for client in clients:
        client.close()


@pytest.mark.parametrize(
    ("configured_total", "configured_idle", "explicit_total", "expected_total", "expected_idle"),
    [
        (None, None, None, "300000", 30),
        ("90", "7", None, "90000", 7),
        ("90", "7", 5, "5000", 5),
        ("90", "7", -1, None, 7),
    ],
)
def test_file_download_uses_independent_processing_and_read_timeouts(
    client_for, clock, monkeypatch, tmp_path, configured_total, configured_idle, explicit_total,
    expected_total, expected_idle,
):
    if configured_total:
        monkeypatch.setenv("ADX_DIRECT_DOWNLOAD_TIMEOUT", configured_total)
        monkeypatch.setenv("ADX_DIRECT_DOWNLOAD_IDLE_TIMEOUT", configured_idle)
    seen = []

    def handler(request):
        seen.append((request.headers.get("x-adx-processing-timeout-ms"), request.extensions["timeout"]["read"]))
        return httpx.Response(200, content=b"finished")

    target = tmp_path / "out.bin"
    client_for(handler).download_file_direct("environment", "/tmp/out.bin", str(target), explicit_total)

    assert seen == [(expected_total, expected_idle)]
    assert target.read_bytes() == b"finished"


def test_copy_to_local_uses_file_budget_instead_of_the_upload_budget(client_for, clock, monkeypatch, tmp_path):
    monkeypatch.setenv("ADX_DIRECT_UPLOAD_TIMEOUT", "123")
    monkeypatch.setenv("ADX_DIRECT_DOWNLOAD_TIMEOUT", "40")
    seen = []

    def handler(request):
        if request.url.path.endswith("/invoke"):
            return httpx.Response(200, json={
                "name": "out.bin", "path": "/tmp/out.bin", "type": "file", "size": 8,
                "permissions": "", "modified_time": 0,
            })
        seen.append((request.headers["x-adx-processing-timeout-ms"], request.extensions["timeout"]["read"]))
        return httpx.Response(200, content=b"finished")

    target = tmp_path / "out.bin"
    Filesystem(client_for(handler), "environment").copy_to_local("/tmp/out.bin", str(target))

    assert seen == [("40000", 30)]
    assert target.read_bytes() == b"finished"


def test_idle_timeout_resumes_from_partial_file_with_remaining_budget(client_for, clock, monkeypatch, tmp_path):
    monkeypatch.setenv("ADX_DIRECT_DOWNLOAD_IDLE_TIMEOUT", "7")
    calls = []

    def handler(request):
        calls.append((
            request.headers.get("range"),
            request.headers["x-adx-processing-timeout-ms"],
            request.extensions["timeout"]["read"],
        ))
        if len(calls) == 1:
            stream = TimedStream(clock, [(0, b"abcd"), (7, b"")], httpx.ReadTimeout("idle", request=request))
            return httpx.Response(200, stream=stream)
        return httpx.Response(206, headers={"Content-Range": "bytes 4-7/8"}, content=b"efgh")

    target = tmp_path / "out.bin"
    client_for(handler).download_file_direct("environment", "/tmp/out.bin", str(target), 10)

    assert calls == [(None, "10000", 7), ("bytes=4-", "3000", 3)]
    assert target.read_bytes() == b"abcdefgh"
    assert not target.with_suffix(".bin.part").exists()


def _resuming_response(request, retry_after, clock):
    payload = {"error": {
        "code": "ENV_RESUMING",
        "retry": "same_operation",
        "outcome": "not_started",
        "requestForwarded": False,
        "requestId": request.headers["x-adx-request-id"],
    }}
    return httpx.Response(
        503,
        headers={"Retry-After": retry_after, "X-ADX-Error-Code": "ENV_RESUMING"},
        stream=TimedStream(clock, [(0, json.dumps(payload).encode())]),
    )


def test_authoritative_deferral_and_backoff_share_file_deadline(client_for, clock, monkeypatch, tmp_path):
    monkeypatch.setattr(_transport.random, "uniform", lambda *_: 0)
    seen = []

    def handler(request):
        seen.append(request)
        if len(seen) == 1:
            clock.advance(6)
            return _resuming_response(request, "1", clock)
        return httpx.Response(200, content=b"finished")

    target = tmp_path / "out.bin"
    client_for(handler).download_file_direct("environment", "/tmp/out.bin", str(target), 10)

    assert [request.headers["x-adx-processing-timeout-ms"] for request in seen] == ["10000", "3000"]
    assert [request.extensions["timeout"]["read"] for request in seen] == [10, 3]
    assert seen[0].headers["x-adx-request-id"] == seen[1].headers["x-adx-request-id"]
    assert target.read_bytes() == b"finished"


def test_authoritative_retry_after_cannot_outlive_remaining_budget(client_for, clock, monkeypatch, tmp_path):
    monkeypatch.setattr(_transport.random, "uniform", lambda *_: 0)
    seen = []

    def handler(request):
        seen.append(request)
        clock.advance(6)
        return _resuming_response(request, "5", clock)

    target = tmp_path / "out.bin"
    with pytest.raises(_transport.EnvironmentError) as error:
        client_for(handler).download_file_direct("environment", "/tmp/out.bin", str(target), 10)

    assert error.value.code == "DOWNLOAD_TIMEOUT"
    assert clock.now == 6
    assert len(seen) == 1
    assert not target.exists()


def test_continuous_progress_cannot_extend_total_download_time(client_for, clock, monkeypatch, tmp_path):
    monkeypatch.setenv("ADX_DIRECT_DOWNLOAD_IDLE_TIMEOUT", "3")
    requests = []

    def handler(request):
        requests.append(request)
        return httpx.Response(200, stream=TimedStream(clock, [(2, b"a"), (2, b"b"), (2, b"c")]))

    target = tmp_path / "out.bin"
    with pytest.raises(_transport.EnvironmentError):
        client_for(handler).download_file_direct("environment", "/tmp/out.bin", str(target), 5)

    assert len(requests) == 1
    assert not target.exists()


def test_streamed_error_body_uses_the_same_download_deadline(client_for, clock, tmp_path):
    def handler(_request):
        return httpx.Response(
            503, stream=TimedStream(clock, [(2, b"a"), (2, b"b"), (2, b"c"), (2, b"d")]),
        )

    target = tmp_path / "out.bin"
    with pytest.raises(_transport.EnvironmentError) as raised:
        client_for(handler).download_file_direct("environment", "/tmp/out.bin", str(target), 5)

    assert raised.value.code == "DOWNLOAD_TIMEOUT"
    assert clock.now == 6
    assert not target.exists()


@pytest.mark.parametrize(
    ("content_range", "body"),
    [
        (None, b"efgh"),
        ("bytes 0-3/8", b"ABCD"),
        ("bytes 4-7/10", b"efgh"),
        ("bytes 4-7/7", b"efgh"),
        ("bytes 4-7/8", b"ef"),
        ("bytes 4-7/8", b"efghij"),
    ],
)
def test_invalid_resume_does_not_publish_or_overwrite_destination(client_for, tmp_path, content_range, body):
    def handler(request):
        assert request.headers["range"] == "bytes=4-"
        headers = {"Content-Range": content_range} if content_range is not None else {}
        return httpx.Response(206, headers=headers, content=body)

    target = tmp_path / "out.bin"
    target.write_bytes(b"previous")
    target.with_suffix(".bin.part").write_bytes(b"abcd")

    with pytest.raises(_transport.EnvironmentError):
        client_for(handler).download_file_direct("environment", "/tmp/out.bin", str(target))

    assert target.read_bytes() == b"previous"


def test_resume_rejects_changed_total_during_same_transfer(client_for, clock, tmp_path):
    ranges = []

    def handler(request):
        ranges.append(request.headers["range"])
        if len(ranges) == 1:
            stream = TimedStream(clock, [(0, b"ef")], httpx.ReadError("disconnected", request=request))
            return httpx.Response(206, headers={"Content-Range": "bytes 4-7/8"}, stream=stream)
        return httpx.Response(206, headers={"Content-Range": "bytes 6-9/10"}, content=b"ghij")

    target = tmp_path / "out.bin"
    target.with_suffix(".bin.part").write_bytes(b"abcd")

    with pytest.raises(_transport.EnvironmentError):
        client_for(handler).download_file_direct("environment", "/tmp/out.bin", str(target))

    assert ranges == ["bytes=4-", "bytes=6-"]
    assert not target.exists()


def test_clean_short_range_stream_restores_partial_file(client_for, clock, tmp_path):
    def handler(request):
        assert request.headers["range"] == "bytes=4-"
        return httpx.Response(
            206, headers={"Content-Range": "bytes 4-7/8"}, stream=TimedStream(clock, [(0, b"ef")]),
        )

    target = tmp_path / "out.bin"
    part = target.with_suffix(".bin.part")
    part.write_bytes(b"abcd")

    with pytest.raises(_transport.EnvironmentError) as error:
        client_for(handler).download_file_direct("environment", "/tmp/out.bin", str(target))

    assert error.value.code == "INVALID_DOWNLOAD_RESPONSE"
    assert not target.exists()
    assert part.read_bytes() == b"abcd"


def test_full_empty_response_replaces_partial_file_when_range_is_ignored(client_for, tmp_path):
    def handler(request):
        assert request.headers["range"] == "bytes=4-"
        return httpx.Response(200, headers={"Content-Length": "0"}, content=b"")

    target = tmp_path / "out.bin"
    part = target.with_suffix(".bin.part")
    part.write_bytes(b"abcd")

    client_for(handler).download_file_direct("environment", "/tmp/out.bin", str(target))

    assert target.read_bytes() == b""
    assert not part.exists()
