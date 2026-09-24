"""Focused reverse-tunnel client protocol tests."""

import asyncio
import base64
import gzip
import http.server
import json
import logging
import os
import ssl
import threading
import unittest
from typing import ClassVar
from unittest import mock

from adx_sandbox import tunnel_client
from adx_sandbox.tunnel_client import TunnelClient
from adx_sandbox.tunnel_protocol import BinaryEnvelope, BinaryKind, hello_frame
from websockets.asyncio.server import serve


class _RecordingHandler(http.server.BaseHTTPRequestHandler):
    requests: ClassVar[list] = []
    block_started: ClassVar[threading.Event] = threading.Event()
    block_release: ClassVar[threading.Event] = threading.Event()

    def do_HEAD(self):
        self.send_response(200)
        self.send_header("Content-Length", "100000")
        self.end_headers()

    def do_GET(self):
        type(self).requests.append((self.path, self.headers, b""))
        if self.path == "/block":
            type(self).block_started.set()
            type(self).block_release.wait(timeout=5)
            response = b"released"
            self.send_response(200)
            self.send_header("Content-Length", str(len(response)))
            self.end_headers()
            self.wfile.write(response)
            return
        self.send_response(200)
        self.send_header("Content-Type", "text/event-stream")
        self.end_headers()
        self.wfile.write(b"data: first\n\n")
        self.wfile.flush()
        self.wfile.write(b"data: second\n\n")
        self.wfile.flush()

    def do_POST(self):
        content_length = int(self.headers.get("Content-Length", "0"))
        body = self.rfile.read(content_length)
        type(self).requests.append((self.path, self.headers, body))
        if self.path == "/gzip":
            response = gzip.compress(b"compressed-response")
            self.send_response(200)
            self.send_header("Content-Encoding", "gzip")
            self.send_header("Content-Length", str(len(response)))
            self.send_header("Set-Cookie", "session=one; Path=/")
            self.send_header("Set-Cookie", "theme=dark; Path=/")
            self.end_headers()
            self.wfile.write(response)
            return
        response = b"ok"
        self.send_response(200)
        if self.path == "/set-cookie":
            self.send_header("Set-Cookie", "session=leaked; Path=/")
        self.send_header("Content-Length", str(len(response)))
        self.end_headers()
        self.wfile.write(response)

    def log_message(self, *_args):
        return


class _FrameWebSocket:
    def __init__(self):
        self._incoming = asyncio.Queue()
        self.sent = asyncio.Queue()
        self.closed = asyncio.Event()

    def __aiter__(self):
        return self

    async def __anext__(self):
        message = await self._incoming.get()
        if message is None:
            raise StopAsyncIteration
        return message if isinstance(message, bytes) else json.dumps(message)

    async def send(self, message):
        await self.sent.put(
            message if isinstance(message, bytes) else json.loads(message)
        )

    async def close(self):
        self.closed.set()
        self.close_input()

    def feed(self, frame):
        self._incoming.put_nowait(frame)

    def close_input(self):
        self._incoming.put_nowait(None)


class TunnelClientRequestTests(unittest.IsolatedAsyncioTestCase):
    async def asyncSetUp(self):
        _RecordingHandler.requests = []
        _RecordingHandler.block_started = threading.Event()
        _RecordingHandler.block_release = threading.Event()
        self.server = http.server.ThreadingHTTPServer(
            ("127.0.0.1", 0),
            _RecordingHandler,
        )
        self.server_thread = threading.Thread(
            target=self.server.serve_forever,
            daemon=True,
        )
        self.server_thread.start()

    async def asyncTearDown(self):
        self.server.shutdown()
        self.server.server_close()
        self.server_thread.join(timeout=2)

    async def test_pair_headers_preserve_duplicates_and_strip_hop_by_hop(self):
        payload = b"request-body"
        frame = {
            "type": "http_req",
            "id": "request-1",
            "method": "POST",
            "path": "/headers",
            "headers": [
                ["Connection", "close, X-First-Hop"],
                ["Connection", "X-Second-Hop"],
                ["X-First-Hop", "drop-one"],
                ["X-Second-Hop", "drop-two"],
                ["Transfer-Encoding", "chunked"],
                ["Content-Length", "999"],
                ["X-Tag", "first"],
                ["X-Tag", "second"],
                ["Authorization", "Bearer test"],
                ["Content-Type", "application/octet-stream"],
            ],
            "body": base64.b64encode(payload).decode("ascii"),
        }
        websocket = _FrameWebSocket()
        websocket.feed(frame)
        client = TunnelClient(
            upstream=f"127.0.0.1:{self.server.server_port}",
        )

        proxy_task = asyncio.create_task(client._proxy_loop(websocket))
        try:
            response = await asyncio.wait_for(websocket.sent.get(), timeout=2)
        finally:
            websocket.close_input()
            await asyncio.wait_for(proxy_task, timeout=2)

        self.assertEqual(response["type"], "http_resp")
        self.assertEqual(len(_RecordingHandler.requests), 1)
        _, headers, body = _RecordingHandler.requests[0]
        self.assertEqual(body, payload)
        self.assertEqual(headers.get_all("X-Tag"), ["first", "second"])
        self.assertIsNone(headers.get("X-First-Hop"))
        self.assertIsNone(headers.get("X-Second-Hop"))
        self.assertIsNone(headers.get("Transfer-Encoding"))
        self.assertEqual(headers.get("Content-Length"), str(len(payload)))
        self.assertEqual(headers.get("Authorization"), "Bearer test")
        self.assertEqual(
            headers.get("Content-Type"),
            "application/octet-stream",
        )

    async def test_legacy_headers_round_trip_and_cached_response(self):
        frame = {
            "type": "http_req", "id": "legacy-request", "method": "POST",
            "path": "/headers", "headers": {
                "Connection": "X-Local", "X-Local": "drop",
                "Authorization": "Bearer legacy", "Content-Length": "999",
            }, "body": base64.b64encode(b"legacy-body").decode("ascii"),
        }
        websocket = _FrameWebSocket()
        client = TunnelClient(upstream=f"127.0.0.1:{self.server.server_port}")
        task = asyncio.create_task(client._proxy_loop(websocket))
        try:
            websocket.feed(frame)
            first = await asyncio.wait_for(websocket.sent.get(), timeout=2)
            self.assertEqual(first["type"], "http_resp")
            self.assertIsInstance(first["headers"], dict)
            self.assertEqual(base64.b64decode(first["body"]), b"ok")
            websocket.feed(frame)
            cached = await asyncio.wait_for(websocket.sent.get(), timeout=2)
            self.assertEqual(cached, first)
            self.assertEqual(len(_RecordingHandler.requests), 1)
            _, headers, body = _RecordingHandler.requests[0]
            self.assertEqual(body, b"legacy-body")
            self.assertEqual(headers["Authorization"], "Bearer legacy")
            self.assertIsNone(headers.get("X-Local"))
            self.assertEqual(headers["Content-Length"], str(len(body)))
        finally:
            websocket.close_input()
            await asyncio.wait_for(task, timeout=2)

    async def test_v2_streams_request_and_response_with_binary_chunks(self):
        request_id = "00112233-4455-6677-8899-aabbccddeeff"
        payload = b"a" * 100_000
        websocket = _FrameWebSocket()
        websocket.feed(hello_frame())
        websocket.feed(
            {
                "type": "http_req_begin",
                "id": request_id,
                "method": "POST",
                "path": "/stream",
                "headers": [["Content-Type", "application/octet-stream"]],
                "content_length": len(payload),
            }
        )
        websocket._incoming.put_nowait(
            BinaryEnvelope(
                request_id=request_id,
                kind=BinaryKind.HTTP_REQUEST_DATA,
                payload=payload[:65536],
                offset=0,
            ).encode()
        )
        websocket._incoming.put_nowait(
            BinaryEnvelope(
                request_id=request_id,
                kind=BinaryKind.HTTP_REQUEST_DATA,
                payload=payload[65536:],
                offset=65536,
            ).encode()
        )
        websocket.feed({"type": "http_req_end", "id": request_id})
        client = TunnelClient(
            upstream=f"127.0.0.1:{self.server.server_port}",
        )

        proxy_task = asyncio.create_task(client._proxy_loop(websocket))
        initial_window = await asyncio.wait_for(websocket.sent.get(), timeout=2)
        self.assertEqual(
            initial_window,
            {
                "type": "window",
                "id": request_id,
                "credits": 16,
                "ack_offset": 0,
            },
        )
        returned_credits = 0
        while True:
            response_begin = await asyncio.wait_for(websocket.sent.get(), timeout=2)
            if response_begin["type"] == "http_resp_begin":
                break
            self.assertEqual(response_begin["type"], "window")
            returned_credits += response_begin["credits"]
        self.assertEqual(returned_credits, 2)
        self.assertEqual(response_begin["type"], "http_resp_begin")
        self.assertEqual(response_begin["status"], 200)
        websocket.feed(
            {
                "type": "window",
                "id": request_id,
                "credits": 16,
                "ack_offset": 0,
            }
        )
        response_data = await asyncio.wait_for(websocket.sent.get(), timeout=2)
        self.assertIsInstance(response_data, bytes)
        response_envelope = BinaryEnvelope.decode(response_data)
        self.assertEqual(response_envelope.kind, BinaryKind.HTTP_RESPONSE_DATA)
        self.assertEqual(response_envelope.payload, b"ok")
        self.assertEqual(response_envelope.offset, 0)
        response_end = await asyncio.wait_for(websocket.sent.get(), timeout=2)
        self.assertEqual(response_end, {"type": "http_resp_end", "id": request_id})

        websocket.close_input()
        await asyncio.wait_for(proxy_task, timeout=2)
        self.assertEqual(len(_RecordingHandler.requests), 1)
        _, headers, body = _RecordingHandler.requests[0]
        self.assertEqual(body, payload)
        self.assertEqual(headers.get("Content-Length"), str(len(payload)))

    async def test_failed_stream_absorbs_late_window_data_without_reconnect(self):
        request_id = "00112233-4455-6677-8899-aabbccddeeff"
        websocket = _FrameWebSocket()
        websocket.feed(hello_frame())
        websocket.feed(
            {
                "type": "http_req_begin",
                "id": request_id,
                "method": "POST",
                "path": "/unreachable",
                "headers": [],
                "content_length": 1,
            }
        )
        client = TunnelClient(upstream="127.0.0.1:1")
        proxy_task = asyncio.create_task(client._proxy_loop(websocket))

        window = await asyncio.wait_for(websocket.sent.get(), timeout=2)
        self.assertEqual(window["type"], "window")
        error = await asyncio.wait_for(websocket.sent.get(), timeout=2)
        self.assertEqual(error["type"], "error")
        self.assertEqual(error["id"], request_id)

        websocket._incoming.put_nowait(
            BinaryEnvelope(
                request_id=request_id,
                kind=BinaryKind.HTTP_REQUEST_DATA,
                payload=b"x",
            ).encode()
        )
        websocket.feed({"type": "http_req_end", "id": request_id})
        websocket.feed({"type": "ping", "id": "still-alive", "timestamp": 1})
        pong = await asyncio.wait_for(websocket.sent.get(), timeout=2)
        self.assertEqual(pong["type"], "pong")
        self.assertEqual(pong["id"], "still-alive")

        websocket.close_input()
        await asyncio.wait_for(proxy_task, timeout=2)

    async def test_negotiated_peer_body_limit_rejects_larger_stream(self):
        request_id = "00112233-4455-6677-8899-aabbccddeeff"
        websocket = _FrameWebSocket()
        websocket.feed(hello_frame(max_body_size=1))
        websocket.feed(
            {
                "type": "http_req_begin",
                "id": request_id,
                "method": "POST",
                "path": "/too-large",
                "headers": [],
                "content_length": 2,
            }
        )
        websocket.feed({"type": "ping", "id": "still-alive", "timestamp": 1})
        client = TunnelClient(upstream=f"127.0.0.1:{self.server.server_port}")
        proxy_task = asyncio.create_task(client._proxy_loop(websocket))

        error = await asyncio.wait_for(websocket.sent.get(), timeout=2)
        self.assertEqual(error["type"], "error")
        self.assertIn("exceeds", error["message"])
        pong = await asyncio.wait_for(websocket.sent.get(), timeout=2)
        self.assertEqual(pong["type"], "pong")

        websocket.close_input()
        await asyncio.wait_for(proxy_task, timeout=2)

    async def test_peer_error_cancels_only_its_stream(self):
        request_id = "00112233-4455-6677-8899-aabbccddeeff"
        websocket = _FrameWebSocket()
        websocket.feed(hello_frame())
        websocket.feed(
            {
                "type": "http_req_begin",
                "id": request_id,
                "method": "POST",
                "path": "/stream",
                "headers": [],
                "content_length": 1,
            }
        )
        client = TunnelClient(upstream=f"127.0.0.1:{self.server.server_port}")
        proxy_task = asyncio.create_task(client._proxy_loop(websocket))
        window = await asyncio.wait_for(websocket.sent.get(), timeout=2)
        self.assertEqual(window["type"], "window")

        websocket.feed(
            {"type": "error", "id": request_id, "message": "downstream closed"}
        )
        websocket.feed({"type": "ping", "id": "still-alive", "timestamp": 2})
        pong = await asyncio.wait_for(websocket.sent.get(), timeout=2)
        self.assertEqual(pong["type"], "pong")
        self.assertEqual(pong["id"], "still-alive")

        websocket.close_input()
        await asyncio.wait_for(proxy_task, timeout=2)

    async def test_v2_small_request_streams_unknown_length_response(self):
        request_id = "00112233-4455-6677-8899-aabbccddeeff"
        websocket = _FrameWebSocket()
        websocket.feed(hello_frame())
        websocket.feed(
            {
                "type": "http_req",
                "id": request_id,
                "method": "GET",
                "path": "/events",
                "headers": [],
                "body": "",
            }
        )
        client = TunnelClient(
            upstream=f"127.0.0.1:{self.server.server_port}",
        )

        proxy_task = asyncio.create_task(client._proxy_loop(websocket))
        response_begin = await asyncio.wait_for(websocket.sent.get(), timeout=2)
        self.assertEqual(response_begin["type"], "http_resp_begin")
        self.assertEqual(response_begin["id"], request_id)
        self.assertIsNone(response_begin["content_length"])
        websocket.feed({"type": "window", "id": request_id, "credits": 16})

        streamed = bytearray()
        while True:
            message = await asyncio.wait_for(websocket.sent.get(), timeout=2)
            if isinstance(message, dict):
                self.assertEqual(
                    message,
                    {"type": "http_resp_end", "id": request_id},
                )
                break
            envelope = BinaryEnvelope.decode(message)
            self.assertEqual(envelope.kind, BinaryKind.HTTP_RESPONSE_DATA)
            streamed.extend(envelope.payload)

        websocket.close_input()
        await asyncio.wait_for(proxy_task, timeout=2)
        self.assertEqual(streamed, b"data: first\n\ndata: second\n\n")

    async def test_v2_head_preserves_content_length_without_waiting_for_body(self):
        request_id = "00112233-4455-6677-8899-aabbccddeeff"
        websocket = _FrameWebSocket()
        websocket.feed(hello_frame())
        websocket.feed(
            {
                "type": "http_req",
                "id": request_id,
                "method": "HEAD",
                "path": "/large-metadata",
                "headers": [],
                "body": "",
            }
        )
        client = TunnelClient(upstream=f"127.0.0.1:{self.server.server_port}")
        proxy_task = asyncio.create_task(client._proxy_loop(websocket))
        response_begin = await asyncio.wait_for(websocket.sent.get(), timeout=2)
        self.assertEqual(response_begin["type"], "http_resp_begin")
        self.assertEqual(response_begin["content_length"], 100000)
        response_end = await asyncio.wait_for(websocket.sent.get(), timeout=2)
        self.assertEqual(
            response_end,
            {"type": "http_resp_end", "id": request_id},
        )
        websocket.close_input()
        await asyncio.wait_for(proxy_task, timeout=2)

    async def test_v2_websocket_binary_roundtrip_uses_raw_chunks(self):
        request_id = "00112233-4455-6677-8899-aabbccddeeff"
        payload = b"z" * 100_000

        async def echo(upstream_websocket):
            message = await upstream_websocket.recv()
            self.assertEqual(message, payload)
            await upstream_websocket.send(message)

        async with serve(echo, "127.0.0.1", 0) as upstream_server:
            upstream_port = upstream_server.sockets[0].getsockname()[1]
            websocket = _FrameWebSocket()
            websocket.feed(hello_frame())
            websocket.feed(
                {
                    "type": "ws_connect",
                    "id": request_id,
                    "path": "/binary",
                    "headers": {},
                }
            )
            client = TunnelClient(upstream=f"127.0.0.1:{upstream_port}")
            proxy_task = asyncio.create_task(client._proxy_loop(websocket))
            connected = await asyncio.wait_for(websocket.sent.get(), timeout=2)
            self.assertEqual(
                connected,
                {"type": "ws_connected", "id": request_id},
            )
            websocket._incoming.put_nowait(
                BinaryEnvelope(
                    request_id=request_id,
                    kind=BinaryKind.WS_BINARY_DATA,
                    payload=payload[:65536],
                ).encode()
            )
            websocket._incoming.put_nowait(
                BinaryEnvelope(
                    request_id=request_id,
                    kind=BinaryKind.WS_BINARY_DATA,
                    payload=payload[65536:],
                    end_of_body=True,
                ).encode()
            )

            echoed = bytearray()
            while True:
                message = await asyncio.wait_for(websocket.sent.get(), timeout=2)
                self.assertIsInstance(message, bytes)
                envelope = BinaryEnvelope.decode(message)
                self.assertEqual(envelope.kind, BinaryKind.WS_BINARY_DATA)
                echoed.extend(envelope.payload)
                if envelope.end_of_body:
                    break
            self.assertEqual(echoed, payload)

            websocket.close_input()
            await asyncio.wait_for(proxy_task, timeout=2)

    async def test_v2_websocket_message_limit_is_independent_and_channel_scoped(self):
        request_id = "00112233-4455-6677-8899-aabbccddeeff"

        async def wait_for_close(upstream_websocket):
            await upstream_websocket.wait_closed()

        async with serve(wait_for_close, "127.0.0.1", 0) as upstream_server:
            upstream_port = upstream_server.sockets[0].getsockname()[1]
            websocket = _FrameWebSocket()
            websocket.feed(hello_frame(max_ws_message_size=1))
            websocket.feed(
                {
                    "type": "ws_connect",
                    "id": request_id,
                    "path": "/oversized",
                    "headers": {},
                }
            )
            client = TunnelClient(upstream=f"127.0.0.1:{upstream_port}")
            proxy_task = asyncio.create_task(client._proxy_loop(websocket))
            self.assertEqual(
                await asyncio.wait_for(websocket.sent.get(), timeout=2),
                {"type": "ws_connected", "id": request_id},
            )
            websocket._incoming.put_nowait(
                BinaryEnvelope(
                    request_id=request_id,
                    kind=BinaryKind.WS_BINARY_DATA,
                    payload=b"ab",
                    end_of_body=True,
                ).encode()
            )
            error = await asyncio.wait_for(websocket.sent.get(), timeout=2)
            self.assertEqual(error["type"], "error")
            self.assertEqual(error["id"], request_id)
            self.assertIn("exceeds tunnel limit", error["message"])

            websocket.feed({"type": "ping", "id": "healthy", "timestamp": 1})
            pong = await asyncio.wait_for(websocket.sent.get(), timeout=2)
            self.assertEqual(pong["type"], "pong")
            self.assertEqual(pong["id"], "healthy")
            websocket.close_input()
            await asyncio.wait_for(proxy_task, timeout=2)

    async def test_max_inflight_rejects_burst_without_spawning_unbounded_tasks(self):
        websocket = _FrameWebSocket()
        websocket.feed(hello_frame(max_inflight=1))
        websocket.feed(
            {
                "type": "http_req",
                "id": "request-1",
                "method": "GET",
                "path": "/block",
                "headers": [],
                "body": "",
            }
        )
        client = TunnelClient(
            upstream=f"127.0.0.1:{self.server.server_port}",
        )
        client._max_inflight = 16
        proxy_task = asyncio.create_task(client._proxy_loop(websocket))
        started = await asyncio.to_thread(_RecordingHandler.block_started.wait, 2)
        self.assertTrue(started)
        websocket.feed(hello_frame(max_inflight=16))
        websocket.feed(
            {
                "type": "http_req",
                "id": "request-2",
                "method": "GET",
                "path": "/block",
                "headers": [],
                "body": "",
            }
        )
        rejected = await asyncio.wait_for(websocket.sent.get(), timeout=2)
        self.assertEqual(rejected["type"], "error")
        self.assertEqual(rejected["id"], "request-2")
        self.assertIn("max_inflight", rejected["message"])

        _RecordingHandler.block_release.set()
        response = await asyncio.wait_for(websocket.sent.get(), timeout=2)
        self.assertEqual(response["type"], "http_resp")
        self.assertEqual(response["id"], "request-1")
        websocket.close_input()
        await asyncio.wait_for(proxy_task, timeout=2)

    async def test_v2_reconnect_keeps_one_upstream_request_for_stable_id(self):
        request_id = "00112233-4455-6677-8899-aabbccddeeff"
        client = TunnelClient(upstream=f"127.0.0.1:{self.server.server_port}")
        client._http_client = tunnel_client.httpx.AsyncClient(
            timeout=tunnel_client._http_timeout_for_tunnel(),
            trust_env=False,
        )

        first = _FrameWebSocket()
        first.feed(hello_frame())
        first.feed(
            {
                "type": "http_req",
                "id": request_id,
                "method": "GET",
                "path": "/block",
                "headers": [],
                "body": "",
            }
        )
        first_task = asyncio.create_task(client._proxy_loop(first))
        self.assertTrue(
            await asyncio.to_thread(_RecordingHandler.block_started.wait, 2)
        )
        first.close_input()
        await asyncio.wait_for(first_task, timeout=2)
        client._resume_state["ws_ready"].clear()

        second = _FrameWebSocket()
        second.feed(hello_frame())
        second.feed(
            {
                "type": "http_req",
                "id": request_id,
                "method": "GET",
                "path": "/block",
                "headers": [],
                "body": "",
            }
        )
        client._ws = second
        client._resume_state["ws_ready"].set()
        second_task = asyncio.create_task(client._proxy_loop(second))
        _RecordingHandler.block_release.set()
        response = await asyncio.wait_for(second.sent.get(), timeout=2)
        self.assertEqual(response["type"], "http_resp")
        self.assertEqual(response["id"], request_id)
        self.assertEqual(len(_RecordingHandler.requests), 1)

        client._stopping.set()
        second.close_input()
        await asyncio.wait_for(second_task, timeout=2)
        await client._close_shared_http_client()

    async def test_response_keeps_raw_gzip_and_duplicate_set_cookie(self):
        websocket = _FrameWebSocket()
        websocket.feed(
            {
                "type": "http_req",
                "id": "gzip-1",
                "method": "POST",
                "path": "/gzip",
                "headers": [],
                "body": "",
            }
        )
        client = TunnelClient(
            upstream=f"127.0.0.1:{self.server.server_port}",
        )

        proxy_task = asyncio.create_task(client._proxy_loop(websocket))
        try:
            response = await asyncio.wait_for(websocket.sent.get(), timeout=2)
        finally:
            websocket.close_input()
            await asyncio.wait_for(proxy_task, timeout=2)

        raw_body = base64.b64decode(response["body"])
        self.assertEqual(gzip.decompress(raw_body), b"compressed-response")
        set_cookies = [
            value for name, value in response["headers"] if name.lower() == "set-cookie"
        ]
        self.assertEqual(
            set_cookies,
            ["session=one; Path=/", "theme=dark; Path=/"],
        )

    async def test_shared_pool_does_not_replay_response_cookies(self):
        websocket = _FrameWebSocket()
        client = TunnelClient(
            upstream=f"127.0.0.1:{self.server.server_port}",
        )
        proxy_task = asyncio.create_task(client._proxy_loop(websocket))
        try:
            websocket.feed(
                {
                    "type": "http_req",
                    "id": "cookie-1",
                    "method": "POST",
                    "path": "/set-cookie",
                    "headers": [],
                    "body": "",
                }
            )
            await asyncio.wait_for(websocket.sent.get(), timeout=2)
            websocket.feed(
                {
                    "type": "http_req",
                    "id": "cookie-2",
                    "method": "POST",
                    "path": "/check-cookie",
                    "headers": [],
                    "body": "",
                }
            )
            await asyncio.wait_for(websocket.sent.get(), timeout=2)
        finally:
            websocket.close_input()
            await asyncio.wait_for(proxy_task, timeout=2)

        self.assertEqual(len(_RecordingHandler.requests), 2)
        _, second_headers, _ = _RecordingHandler.requests[1]
        self.assertIsNone(second_headers.get("Cookie"))

    async def test_explicit_cookie_header_is_forwarded(self):
        websocket = _FrameWebSocket()
        websocket.feed(
            {
                "type": "http_req",
                "id": "cookie-explicit",
                "method": "POST",
                "path": "/check-cookie",
                "headers": [["Cookie", "caller=explicit"]],
                "body": "",
            }
        )
        client = TunnelClient(
            upstream=f"127.0.0.1:{self.server.server_port}",
        )

        proxy_task = asyncio.create_task(client._proxy_loop(websocket))
        try:
            await asyncio.wait_for(websocket.sent.get(), timeout=2)
        finally:
            websocket.close_input()
            await asyncio.wait_for(proxy_task, timeout=2)

        _, headers, _ = _RecordingHandler.requests[0]
        self.assertEqual(headers.get("Cookie"), "caller=explicit")


class TunnelClientRelayCleanupTests(unittest.IsolatedAsyncioTestCase):
    async def _check_relay_cleanup(self, cancel_proxy):
        async def upstream_handler(websocket):
            await websocket.wait_closed()

        async with serve(upstream_handler, "127.0.0.1", 0) as server:
            port = server.sockets[0].getsockname()[1]
            websocket = _FrameWebSocket()
            websocket.feed(hello_frame())
            websocket.feed({
                "type": "ws_connect",
                "id": "00112233-4455-6677-8899-aabbccddeeff",
                "path": "/cleanup",
                "headers": {},
            })
            client = TunnelClient(upstream=f"127.0.0.1:{port}")
            proxy_task = asyncio.create_task(client._proxy_loop(websocket))
            relays = set()
            try:
                connected = await asyncio.wait_for(websocket.sent.get(), timeout=2)
                self.assertEqual(connected["type"], "ws_connected")
                # Keep strong references so an orphan cannot disappear through GC
                # before its lifecycle is checked.
                relays = {
                    task for task in asyncio.all_tasks()
                    if task.get_coro().__qualname__.endswith((
                        "handle_ws_connect.<locals>.from_upstream",
                        "handle_ws_connect.<locals>.from_sandbox",
                    ))
                }
                self.assertEqual(len(relays), 2)
                if cancel_proxy:
                    proxy_task.cancel()
                    with self.assertRaises(asyncio.CancelledError):
                        await asyncio.wait_for(proxy_task, timeout=2)
                else:
                    websocket.close_input()
                    await asyncio.wait_for(proxy_task, timeout=2)
                self.assertTrue(
                    all(task.done() for task in relays),
                    "outer tunnel ended with a live WebSocket relay task",
                )
            finally:
                proxy_task.cancel()
                for task in relays:
                    task.cancel()
                await asyncio.gather(proxy_task, *relays, return_exceptions=True)

    async def test_outer_eof_joins_websocket_relays(self):
        await self._check_relay_cleanup(cancel_proxy=False)

    async def test_proxy_cancellation_joins_websocket_relays(self):
        await self._check_relay_cleanup(cancel_proxy=True)


class TunnelClientHeartbeatTests(unittest.IsolatedAsyncioTestCase):
    async def test_sends_application_ping_and_accepts_matching_pong(self):
        websocket = _FrameWebSocket()
        client = TunnelClient(
            upstream="127.0.0.1:1",
            ping_interval=0.01,
            ping_timeout=0.5,
        )

        proxy_task = asyncio.create_task(client._proxy_loop(websocket))
        ping = await asyncio.wait_for(websocket.sent.get(), timeout=1)
        self.assertEqual(ping["type"], "ping")
        self.assertTrue(ping["id"])
        self.assertIsInstance(ping["timestamp"], int)

        websocket.feed(
            {
                "type": "pong",
                "id": ping["id"],
                "timestamp": ping["timestamp"],
            }
        )
        client._stopping.set()
        websocket.close_input()
        await asyncio.wait_for(proxy_task, timeout=1)
        self.assertFalse(websocket.closed.is_set())

    async def test_missing_application_pong_closes_connection(self):
        websocket = _FrameWebSocket()
        client = TunnelClient(
            upstream="127.0.0.1:1",
            ping_interval=0.01,
            ping_timeout=0.02,
        )

        proxy_task = asyncio.create_task(client._proxy_loop(websocket))
        ping = await asyncio.wait_for(websocket.sent.get(), timeout=1)
        self.assertEqual(ping["type"], "ping")
        websocket.feed(
            {
                "type": "pong",
                "id": "different-ping",
                "timestamp": ping["timestamp"],
            }
        )
        await asyncio.wait_for(websocket.closed.wait(), timeout=1)
        with self.assertRaisesRegex(asyncio.TimeoutError, "heartbeat"):
            await asyncio.wait_for(proxy_task, timeout=1)


class TunnelClientConfigurationTests(unittest.TestCase):
    @staticmethod
    def _capture_http_timeout():
        captured = []

        class RecordingAsyncClient:
            def __init__(self, **kwargs):
                captured.append(kwargs["timeout"])

            async def aclose(self):
                return None

        with mock.patch.object(
            tunnel_client.httpx,
            "AsyncClient",
            RecordingAsyncClient,
        ):
            client = TunnelClient(upstream="127.0.0.1:1")
            client.start("ws://127.0.0.1:1", timeout=0.05)
            client.stop()
        if not captured:
            raise AssertionError("TunnelClient did not construct its HTTP client")
        return captured[0]

    def test_tunnel_limits_are_capped_and_internally_consistent(self):
        with mock.patch.dict(
            os.environ,
            {
                "ADX_TUNNEL_PROTOCOL_VERSION": "99",
                "ADX_TUNNEL_MAX_BODY_SIZE": str(2 * 1024 * 1024 * 1024),
                "ADX_TUNNEL_MAX_WS_MESSAGE_SIZE": str(64 * 1024 * 1024),
                "ADX_TUNNEL_STREAM_CHUNK_BYTES": str(2 * 1024 * 1024),
                "ADX_TUNNEL_MAX_INFLIGHT": "2048",
                "ADX_TUNNEL_STREAM_WINDOW_FRAMES": "2048",
                "ADX_TUNNEL_FAST_PATH_BODY_BYTES": str(2 * 1024 * 1024 * 1024),
            },
            clear=False,
        ):
            client = TunnelClient(upstream="127.0.0.1:1")
        self.assertEqual(client._protocol_version, 2)
        self.assertEqual(client._max_body_size, 1024 * 1024 * 1024)
        self.assertEqual(client._max_ws_message_size, 8 * 1024 * 1024)
        self.assertEqual(client._max_stream_chunk, 64 * 1024)
        self.assertEqual(client._max_inflight, 1024)
        self.assertEqual(client._stream_window_frames, 1024)
        self.assertEqual(client._fast_path_body_bytes, 5 * 1024 * 1024)

    def test_http_timeout_preserves_legacy_environment_contract(self):
        cases = ((None, 600.0), ("7200", 7200.0), ("invalid", 600.0))
        for raw, expected in cases:
            env = {} if raw is None else {"ADX_TUNNEL_HTTP_TIMEOUT": raw}
            with self.subTest(raw=raw), mock.patch.dict(os.environ, env, clear=True):
                timeout = self._capture_http_timeout()
            self.assertEqual(timeout.connect, expected)
            self.assertEqual(timeout.read, expected)
            self.assertEqual(timeout.write, expected)
            self.assertEqual(timeout.pool, expected)

    def test_invalid_tunnel_limit_is_rejected_at_construction(self):
        with mock.patch.dict(
            os.environ,
            {"ADX_TUNNEL_MAX_BODY_SIZE": "not-an-integer"},
            clear=False,
        ):
            with self.assertRaisesRegex(ValueError, "positive integer"):
                TunnelClient(upstream="127.0.0.1:1")


class TunnelClientTlsTests(unittest.IsolatedAsyncioTestCase):
    @staticmethod
    def _rejected_connection(client, status_code):
        from websockets.datastructures import Headers
        from websockets.http11 import Response

        error = tunnel_client.ws_exc.InvalidStatus(
            Response(status_code, "Rejected", Headers(), b"")
        )

        class _RejectedConnection:
            async def __aenter__(self):
                client._stopping.set()
                raise error

            async def __aexit__(self, *_args):
                return False

        return _RejectedConnection()

    async def test_route_not_ready_404_is_debug_only(self):
        client = TunnelClient(upstream="127.0.0.1:1")

        with (
            mock.patch.object(
                tunnel_client.ws_client,
                "connect",
                return_value=self._rejected_connection(client, 404),
            ),
            self.assertLogs(tunnel_client.logger, level="DEBUG") as logs,
        ):
            await client._connect_loop("ws://router.test/tunnel/sandbox")

        output = "\n".join(logs.output)
        self.assertIn("route unavailable", output)
        self.assertNotIn("unexpected error", output)

    async def test_repeated_route_404_emits_bounded_warnings(self):
        client = TunnelClient(upstream="127.0.0.1:1")
        attempts = 0

        class _RepeatedRejection:
            async def __aenter__(self):
                nonlocal attempts
                attempts += 1
                if attempts == 10:
                    client._stopping.set()
                from websockets.datastructures import Headers
                from websockets.http11 import Response

                raise tunnel_client.ws_exc.InvalidStatus(
                    Response(404, "Not Found", Headers(), b"")
                )

            async def __aexit__(self, *_args):
                return False

        with (
            mock.patch.object(
                tunnel_client.ws_client,
                "connect",
                side_effect=lambda *_args, **_kwargs: _RepeatedRejection(),
            ),
            mock.patch.object(
                tunnel_client.asyncio,
                "sleep",
                new=mock.AsyncMock(),
            ),
            self.assertLogs(tunnel_client.logger, level="DEBUG") as logs,
        ):
            await client._connect_loop("ws://router.test/tunnel/sandbox")

        warnings = [
            record for record in logs.records if record.levelno >= logging.WARNING
        ]
        self.assertEqual(attempts, 10)
        self.assertEqual(len(warnings), 2)
        self.assertIn("attempt 5", warnings[0].getMessage())
        self.assertIn("attempt 10", warnings[1].getMessage())

    async def test_non_404_handshake_rejection_stays_visible(self):
        client = TunnelClient(upstream="127.0.0.1:1")

        with (
            mock.patch.object(
                tunnel_client.ws_client,
                "connect",
                return_value=self._rejected_connection(client, 403),
            ),
            self.assertLogs(tunnel_client.logger, level="WARNING") as logs,
        ):
            await client._connect_loop("ws://router.test/tunnel/sandbox")

        output = "\n".join(logs.output)
        self.assertIn("handshake rejected", output)
        self.assertIn("HTTP 403", output)

    async def test_checkpoint_inflight_reconnect_is_quiet_and_nonfatal(self):
        client = TunnelClient(upstream="127.0.0.1:1")
        attempts = 0
        from websockets.datastructures import Headers
        from websockets.http11 import Response

        error = tunnel_client.ws_exc.InvalidStatus(
            Response(503, "Rejected", Headers(), b"")
        )

        class _RepeatedRejection:
            async def __aenter__(self):
                nonlocal attempts
                attempts += 1
                if attempts == 3:
                    client._stopping.set()
                raise error

            async def __aexit__(self, *_args):
                return False

        with (
            mock.patch.object(
                tunnel_client.ws_client,
                "connect",
                side_effect=lambda *_args, **_kwargs: _RepeatedRejection(),
            ),
            mock.patch.object(
                tunnel_client.asyncio,
                "sleep",
                new=mock.AsyncMock(),
            ),
            self.assertLogs(tunnel_client.logger, level="DEBUG") as logs,
            client.checkpoint_inflight(),
        ):
            await client._connect_loop("ws://router.test/tunnel/sandbox")

        reconnect_logs = [
            record
            for record in logs.records
            if "handshake rejected" in record.getMessage()
        ]
        self.assertEqual(attempts, 3)
        self.assertTrue(reconnect_logs)
        self.assertTrue(all(record.levelno < logging.WARNING for record in reconnect_logs))

    async def test_wss_preserves_legacy_auth_and_default_tls_verification(self):
        client = TunnelClient(upstream="127.0.0.1:1", token="sandbox-token", sandbox_id="sandbox-1")
        captured = {}

        class _FailingConnection:
            async def __aenter__(self):
                client._stopping.set()
                raise OSError("stop after inspecting connection arguments")

            async def __aexit__(self, *_args):
                return False

        def fake_connect(_url, **kwargs):
            captured.update(kwargs)
            return _FailingConnection()

        with mock.patch.object(
            tunnel_client.ws_client,
            "connect",
            side_effect=fake_connect,
        ):
            await client._connect_loop("wss://tunnel.example.test/path")

        self.assertEqual(
            captured["additional_headers"],
            {"Authorization": "Bearer sandbox-token", "X-Auth": "sandbox-token", "X-Sandbox-ID": "sandbox-1"},
        )
        context = captured["ssl"]
        self.assertIsInstance(context, ssl.SSLContext)
        self.assertTrue(context.check_hostname)
        self.assertEqual(context.verify_mode, ssl.CERT_REQUIRED)
        self.assertIsNone(captured["ping_interval"])
        self.assertIsNone(captured["ping_timeout"])

    async def test_reconnect_reuses_ssl_contexts_across_http_clients(self):
        client = TunnelClient(upstream="127.0.0.1:1")
        connection_count = 0
        http_client_count = 0
        http_client_close_count = 0
        ssl_contexts = []
        http_verify_values = []
        http_trust_env_values = []

        class _EmptyWebSocket:
            async def send(self, _message):
                return None

            def __aiter__(self):
                async def messages():
                    if False:
                        yield None

                return messages()

        class _Connected:
            async def __aenter__(self):
                if connection_count == 3:
                    client._stopping.set()
                return _EmptyWebSocket()

            async def __aexit__(self, *_args):
                return False

        def fake_connect(_url, **kwargs):
            nonlocal connection_count
            connection_count += 1
            ssl_contexts.append(kwargs["ssl"])
            return _Connected()

        class FakeHttpClient:
            def __init__(self, **kwargs):
                nonlocal http_client_count
                http_client_count += 1
                http_verify_values.append(kwargs["verify"])
                http_trust_env_values.append(kwargs["trust_env"])

            async def __aenter__(self):
                return self

            async def __aexit__(self, *_args):
                nonlocal http_client_close_count
                http_client_close_count += 1
                return False

        expected_context = ssl.create_default_context()
        expected_http_context = object()
        with (
            mock.patch.object(
                tunnel_client.ssl,
                "create_default_context",
                return_value=expected_context,
            ) as create_context,
            mock.patch.object(
                tunnel_client.ws_client,
                "connect",
                side_effect=fake_connect,
            ),
            mock.patch.object(
                tunnel_client.httpx,
                "create_ssl_context",
                return_value=expected_http_context,
            ) as create_http_context,
            mock.patch.object(
                tunnel_client.httpx,
                "AsyncClient",
                FakeHttpClient,
            ),
            mock.patch.object(
                tunnel_client.asyncio,
                "sleep",
                new=mock.AsyncMock(),
            ) as reconnect_sleep,
        ):
            await client._connect_loop("wss://tunnel.example.test/path")

        self.assertEqual(connection_count, 3)
        create_context.assert_called_once_with(cafile=None)
        create_http_context.assert_called_once_with(verify=True, trust_env=False)
        self.assertEqual(http_client_count, 1)
        self.assertEqual(http_client_close_count, 1)
        self.assertEqual(reconnect_sleep.await_count, 2)
        self.assertTrue(all(context is expected_context for context in ssl_contexts))
        self.assertTrue(
            all(context is expected_http_context for context in http_verify_values)
        )
        self.assertTrue(http_trust_env_values)
        self.assertTrue(all(value is False for value in http_trust_env_values))
        self.assertFalse(client._connected.is_set())

    def test_wss_uses_explicit_ca_bundle(self):
        expected = ssl.create_default_context()
        with (
            mock.patch.dict(
                os.environ,
                {"ADX_TUNNEL_CA_BUNDLE": "/tmp/tunnel-ca.pem"},
                clear=False,
            ),
            mock.patch.object(
                tunnel_client.ssl,
                "create_default_context",
                return_value=expected,
            ) as create_context,
        ):
            context = tunnel_client._ssl_context_for_tunnel(
                "wss://tunnel.example.test/path"
            )

        self.assertIs(context, expected)
        create_context.assert_called_once_with(cafile="/tmp/tunnel-ca.pem")

    def test_wss_insecure_mode_requires_explicit_development_switch(self):
        with (
            mock.patch.dict(
                os.environ,
                {"ADX_TUNNEL_SSL_VERIFY": "0"},
                clear=False,
            ),
            self.assertLogs(tunnel_client.logger, level="WARNING") as logs,
        ):
            context = tunnel_client._ssl_context_for_tunnel(
                "wss://tunnel.example.test/path"
            )

        self.assertFalse(context.check_hostname)
        self.assertEqual(context.verify_mode, ssl.CERT_NONE)
        self.assertIn("explicitly disabled", "\n".join(logs.output))


if __name__ == "__main__":
    unittest.main()
