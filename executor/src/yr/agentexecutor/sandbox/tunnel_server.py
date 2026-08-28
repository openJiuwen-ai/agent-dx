# Copyright (c) Huawei Technologies Co., Ltd. 2026. All rights reserved.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
# http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
"""Sandbox-side reverse tunnel server with V1 compatibility and V2 streaming."""

from __future__ import annotations

import asyncio
import base64
import contextlib
import json
import logging
import os
import uuid
from dataclasses import dataclass, field
from typing import Any, Optional

import aiohttp
import websockets
from aiohttp import web
from multidict import CIMultiDict

from yr.agentexecutor.sandbox.tunnel_protocol import (
    DEFAULT_FAST_PATH_BODY_BYTES,
    DEFAULT_MAX_BODY_BYTES,
    DEFAULT_MAX_INFLIGHT,
    DEFAULT_MAX_WS_MESSAGE_BYTES,
    DEFAULT_STREAM_CHUNK_BYTES,
    DEFAULT_STREAM_WINDOW_FRAMES,
    MAX_TUNNEL_FRAME_SIZE,
    MAX_V1_BODY_BYTES,
    MIN_STREAM_CHUNK_BYTES,
    PROTOCOL_VERSION,
    BinaryEnvelope,
    BinaryKind,
    ErrorFrame,
    HttpReqFrame,
    HttpRespFrame,
    ProtocolError,
    WsConnectFrame,
    WsMessageFrame,
    hello_frame,
    make_id,
    parse_frame,
    rebuilt_request_header_items,
    rebuilt_response_header_items,
    _is_valid_window_frame,
)

logger = logging.getLogger(__name__)

_NEGOTIATION_TIMEOUT = 0.25
_MAX_CONFIGURED_BODY_BYTES = 1024 * 1024 * 1024
_MAX_CONFIGURED_WS_MESSAGE_BYTES = 8 * 1024 * 1024
_MAX_CONFIGURED_STREAM_CHUNK_BYTES = 64 * 1024
_MAX_CONFIGURED_INFLIGHT = 1024
_MAX_CONFIGURED_WINDOW_FRAMES = 1024


def _positive_int_env(name: str, default: int, maximum: int) -> int:
    raw = os.environ.get(name)
    try:
        value = default if raw is None else int(raw)
    except ValueError as exc:
        raise ValueError(f"{name} must be a positive integer") from exc
    if value <= 0:
        raise ValueError(f"{name} must be greater than zero")
    return min(value, maximum)


def _http_timeout() -> float:
    try:
        return float(os.environ.get("YR_TUNNEL_HTTP_TIMEOUT", "600"))
    except ValueError:
        return 600.0


def _raw_headers(request: web.Request) -> list[tuple[str, str]]:
    return [
        (name.decode("ascii"), value.decode("latin-1"))
        for name, value in request.raw_headers
    ]


def _body_allowed(method: str, status: int) -> bool:
    return method.upper() != "HEAD" and not (
        100 <= status < 200 or status in (204, 304)
    )


@dataclass
class _Protocol:
    version: int = 1
    stream_chunk: int = DEFAULT_STREAM_CHUNK_BYTES
    max_inflight: int = DEFAULT_MAX_INFLIGHT
    stream_window: int = DEFAULT_STREAM_WINDOW_FRAMES
    max_body: int = DEFAULT_MAX_BODY_BYTES
    max_ws_message: int = DEFAULT_MAX_WS_MESSAGE_BYTES


@dataclass
class _Connection:
    websocket: Any
    generation: int
    protocol: _Protocol
    inflight: asyncio.Semaphore
    hello_received: asyncio.Event = field(default_factory=asyncio.Event)
    send_lock: asyncio.Lock = field(default_factory=asyncio.Lock)
    hello_sent: bool = False
    resumable: bool = False
    session_id: Optional[str] = None


@dataclass
class _HttpExchange:
    generation: int
    result: asyncio.Future
    response_chunks: asyncio.Queue
    request_credits: asyncio.Queue
    streaming_response: bool = False
    response_received: int = 0
    response_complete: bool = False
    session_id: Optional[str] = None
    request_frame: Optional[dict] = None
    streaming_request: bool = False
    request_unacked: list[tuple[int, bytes]] = field(default_factory=list)
    request_next_offset: int = 0
    request_end_sent: bool = False
    upload_ready: asyncio.Event = field(default_factory=asyncio.Event)
    upload_send_lock: asyncio.Lock = field(default_factory=asyncio.Lock)
    upload_resuming: bool = False


class TunnelServer:
    """Expose sandbox-local HTTP/WS and relay it over one reverse WebSocket."""

    def __init__(self, ws_port: int, http_port: int):
        self._ws_port = ws_port
        self._http_port = http_port
        self._max_body_size = _positive_int_env(
            "YR_TUNNEL_MAX_BODY_SIZE",
            DEFAULT_MAX_BODY_BYTES,
            _MAX_CONFIGURED_BODY_BYTES,
        )
        self._max_ws_message_size = _positive_int_env(
            "YR_TUNNEL_MAX_WS_MESSAGE_SIZE",
            DEFAULT_MAX_WS_MESSAGE_BYTES,
            _MAX_CONFIGURED_WS_MESSAGE_BYTES,
        )
        self._stream_chunk = max(
            MIN_STREAM_CHUNK_BYTES,
            _positive_int_env(
                "YR_TUNNEL_STREAM_CHUNK_BYTES",
                DEFAULT_STREAM_CHUNK_BYTES,
                _MAX_CONFIGURED_STREAM_CHUNK_BYTES,
            ),
        )
        self._max_inflight = _positive_int_env(
            "YR_TUNNEL_MAX_INFLIGHT",
            DEFAULT_MAX_INFLIGHT,
            _MAX_CONFIGURED_INFLIGHT,
        )
        self._stream_window = _positive_int_env(
            "YR_TUNNEL_STREAM_WINDOW_FRAMES",
            DEFAULT_STREAM_WINDOW_FRAMES,
            _MAX_CONFIGURED_WINDOW_FRAMES,
        )
        self._fast_path_body = _positive_int_env(
            "YR_TUNNEL_FAST_PATH_BODY_BYTES",
            DEFAULT_FAST_PATH_BODY_BYTES,
            min(self._max_body_size, MAX_V1_BODY_BYTES),
        )
        self._active: Optional[_Connection] = None
        self._generation = 0
        self._pending_http: dict[str, _HttpExchange] = {}
        self._pending_ws: dict[str, tuple[int, asyncio.Queue]] = {}
        self._loop: Optional[asyncio.AbstractEventLoop] = None
        self._ws_server = None
        self._http_runner = None
        self._connection_cleanup_tasks: set[asyncio.Task] = set()
        self._connection_ready = asyncio.Event()

    async def start(self):
        self._loop = asyncio.get_running_loop()
        self._ws_server = await websockets.serve(
            self._handle_tunnel_conn,
            "0.0.0.0",
            self._ws_port,
            reuse_address=True,
            ping_interval=None,
            ping_timeout=None,
            max_size=MAX_TUNNEL_FRAME_SIZE,
        )
        app = web.Application(client_max_size=self._max_body_size)
        app.router.add_route("*", "/{path_info:.*}", self._handle_request)
        self._http_runner = web.AppRunner(app, auto_decompress=False)
        await self._http_runner.setup()
        site = web.TCPSite(self._http_runner, "127.0.0.1", self._http_port)
        await site.start()
        logger.info(
            "TunnelServer started ws=0.0.0.0:%d http=127.0.0.1:%d",
            self._ws_port,
            self._http_port,
        )

    async def stop(self):
        if self._ws_server:
            # Stop accepting reconnects before closing the active generation.
            self._ws_server.close()
        active = self._active
        self._active = None
        self._connection_ready.clear()
        if active is not None:
            with contextlib.suppress(Exception, asyncio.TimeoutError):
                await asyncio.wait_for(active.websocket.close(), timeout=1.0)
        self._fail_generation(
            active.generation if active else None,
            "TunnelServer stopped",
        )
        if self._ws_server:
            with contextlib.suppress(asyncio.TimeoutError):
                await asyncio.wait_for(self._ws_server.wait_closed(), timeout=2.0)
        if self._http_runner:
            await self._http_runner.cleanup()
        for task in list(self._connection_cleanup_tasks):
            task.cancel()
        if self._connection_cleanup_tasks:
            await asyncio.gather(
                *self._connection_cleanup_tasks,
                return_exceptions=True,
            )

    def _local_protocol(self) -> _Protocol:
        return _Protocol(
            stream_chunk=self._stream_chunk,
            max_inflight=self._max_inflight,
            stream_window=self._stream_window,
            max_body=self._max_body_size,
            max_ws_message=self._max_ws_message_size,
        )

    async def _handle_tunnel_conn(self, websocket):
        self._generation += 1
        conn = _Connection(
            websocket,
            self._generation,
            self._local_protocol(),
            asyncio.Semaphore(self._max_inflight),
        )
        logger.info("TunnelClient connected generation=%d", conn.generation)
        request = getattr(websocket, "request", None)
        request_headers = getattr(request, "headers", None) or getattr(
            websocket, "request_headers", None
        )
        advertised_v2 = request_headers is not None and request_headers.get(
            "X-YR-Tunnel-Protocol"
        ) == str(PROTOCOL_VERSION)
        if advertised_v2:
            await self._send_json(
                conn,
                hello_frame(
                    max_stream_chunk=self._stream_chunk,
                    max_inflight=self._max_inflight,
                    stream_window_frames=self._stream_window,
                    max_body_size=self._max_body_size,
                    max_ws_message_size=self._max_ws_message_size,
                ),
            )
            conn.hello_sent = True
        else:
            # A V1 peer has no negotiation header and starts relaying
            # immediately. V2 peers advertise the header, so reconnecting V2
            # sessions are not exposed until their stable session id is known.
            previous = self._active
            self._active = conn
            self._connection_ready.set()
            if previous is not None and previous is not conn:
                self._fail_generation(previous.generation, "TunnelClient replaced")
                cleanup_task = asyncio.create_task(
                    previous.websocket.close(code=1012, reason="replaced")
                )
                self._connection_cleanup_tasks.add(cleanup_task)
                cleanup_task.add_done_callback(self._connection_cleanup_tasks.discard)
        try:
            async for message in websocket:
                if isinstance(message, bytes):
                    await self._dispatch_binary(conn, message)
                else:
                    await self._dispatch_text(conn, message)
        except websockets.ConnectionClosed:
            logger.info("TunnelClient disconnected generation=%d", conn.generation)
        except (ValueError, TypeError, KeyError) as exc:
            logger.warning("Closing malformed tunnel connection: %s", exc)
            with contextlib.suppress(Exception):
                await websocket.close(code=1002, reason="invalid tunnel frame")
        finally:
            if self._active is conn:
                self._active = None
                self._connection_ready.clear()
            if conn.resumable:
                for exchange in self._pending_http.values():
                    if exchange.session_id == conn.session_id:
                        exchange.upload_ready.clear()
                        while not exchange.request_credits.empty():
                            with contextlib.suppress(asyncio.QueueEmpty):
                                exchange.request_credits.get_nowait()
            else:
                self._fail_generation(conn.generation, "TunnelClient disconnected")

    async def _dispatch_text(self, conn: _Connection, raw: str) -> None:
        if len(raw.encode("utf-8")) > MAX_TUNNEL_FRAME_SIZE:
            raise ProtocolError("tunnel control frame exceeds limit")
        data = json.loads(raw)
        if not isinstance(data, dict):
            raise ProtocolError("tunnel control frame must be an object")
        frame_type = data.get("type")
        if frame_type == "hello":
            first_hello = not conn.hello_received.is_set()
            await self._negotiate(conn, data)
            if first_hello and not conn.hello_sent:
                await self._send_json(
                    conn,
                    hello_frame(
                        max_stream_chunk=self._stream_chunk,
                        max_inflight=self._max_inflight,
                        stream_window_frames=self._stream_window,
                        max_body_size=self._max_body_size,
                        max_ws_message_size=self._max_ws_message_size,
                    ),
                )
                conn.hello_sent = True
            return
        if frame_type == "ping":
            await self._send_json(
                conn,
                {
                    "type": "pong",
                    "id": data.get("id", ""),
                    "timestamp": data.get("timestamp", 0),
                },
            )
            return
        if frame_type == "pong":
            return
        request_id = data.get("id", "")
        exchange = self._pending_http.get(request_id)
        if exchange is not None and self._exchange_matches(exchange, conn):
            if frame_type == "window":
                self._grant_credits(
                    exchange,
                    data.get("credits"),
                    data.get("ack_offset"),
                    conn,
                )
                if exchange.upload_resuming:
                    exchange.upload_resuming = False
                    task = asyncio.create_task(self._resume_upload(conn, exchange))
                    self._connection_cleanup_tasks.add(task)
                    task.add_done_callback(self._connection_cleanup_tasks.discard)
                return
            if frame_type in ("http_resp", "http_resp_begin", "http_resp_end", "error"):
                await self._dispatch_http_control(conn, exchange, data)
                return
        channel = self._pending_ws.get(request_id)
        if channel is not None and channel[0] == conn.generation:
            if frame_type in ("ws_connected", "ws_message", "ws_close", "error"):
                try:
                    channel[1].put_nowait(data)
                except asyncio.QueueFull as exc:
                    raise ProtocolError(
                        "WebSocket channel queue exceeded its limit"
                    ) from exc
                return
        if frame_type in {
            "window",
            "http_resp",
            "http_resp_begin",
            "http_resp_end",
            "error",
            "ws_connected",
            "ws_message",
            "ws_close",
        }:
            # Cancellation can race with frames that were already covered by
            # the advertised window. They have no consumer after teardown.
            return
        parse_frame(raw)

    async def _negotiate(self, conn: _Connection, frame: dict) -> None:
        if conn.hello_received.is_set():
            logger.warning("TunnelServer ignored duplicate hello")
            return
        peer_version = frame.get("protocol_version")
        peer_chunk = frame.get("max_stream_chunk")
        peer_inflight = frame.get("max_inflight")
        peer_window = frame.get("stream_window_frames")
        peer_body = frame.get("max_body_size")
        peer_ws = frame.get("max_ws_message_size")
        peer_resume = frame.get("resume") is True
        peer_session_id = frame.get("session_id")
        if not isinstance(peer_version, int) or peer_version <= 0:
            raise ProtocolError("invalid tunnel protocol version")
        if peer_version >= PROTOCOL_VERSION:
            if not peer_resume:
                raise ProtocolError("tunnel V2 peer must support resumable sessions")
            try:
                peer_session_id = str(uuid.UUID(peer_session_id))
            except (ValueError, TypeError, AttributeError) as exc:
                raise ProtocolError(
                    "resumable tunnel V2 peer must provide a UUID session_id"
                ) from exc
            if not (
                isinstance(peer_chunk, int)
                and peer_chunk >= MIN_STREAM_CHUNK_BYTES
                and isinstance(peer_inflight, int)
                and peer_inflight > 0
                and isinstance(peer_window, int)
                and peer_window > 0
                and isinstance(peer_body, int)
                and peer_body > 0
                and isinstance(peer_ws, int)
                and peer_ws > 0
            ):
                raise ProtocolError("invalid tunnel V2 capability advertisement")
            conn.protocol = _Protocol(
                version=PROTOCOL_VERSION,
                stream_chunk=min(self._stream_chunk, peer_chunk),
                max_inflight=min(self._max_inflight, peer_inflight),
                stream_window=min(self._stream_window, peer_window),
                max_body=min(self._max_body_size, peer_body),
                max_ws_message=min(self._max_ws_message_size, peer_ws),
            )
            conn.inflight = asyncio.Semaphore(conn.protocol.max_inflight)
            conn.resumable = True
            conn.session_id = peer_session_id
            logger.info(
                "TunnelServer protocol v2 negotiated chunk=%d inflight=%d "
                "window=%d body=%d ws_message=%d",
                conn.protocol.stream_chunk,
                conn.protocol.max_inflight,
                conn.protocol.stream_window,
                conn.protocol.max_body,
                conn.protocol.max_ws_message,
            )
        previous = self._active
        self._active = conn
        self._connection_ready.set()
        conn.hello_received.set()
        if previous is not None and previous is not conn:
            if not (
                previous.resumable
                and conn.resumable
                and previous.session_id == conn.session_id
            ):
                self._fail_generation(previous.generation, "TunnelClient replaced")
            cleanup_task = asyncio.create_task(
                previous.websocket.close(code=1012, reason="replaced")
            )
            self._connection_cleanup_tasks.add(cleanup_task)
            cleanup_task.add_done_callback(self._connection_cleanup_tasks.discard)
        if conn.resumable:
            for exchange in list(self._pending_http.values()):
                if exchange.session_id != conn.session_id:
                    continue
                exchange.generation = conn.generation
                await self._resume_exchange(conn, exchange)

    async def _dispatch_http_control(
        self,
        conn: _Connection,
        exchange: _HttpExchange,
        frame: dict,
    ) -> None:
        frame_type = frame["type"]
        if frame_type == "http_resp_begin":
            if conn.protocol.version < PROTOCOL_VERSION:
                raise ProtocolError("unexpected streaming response start")
            status = frame.get("status")
            headers = frame.get("headers")
            content_length = frame.get("content_length")
            if not isinstance(status, int) or not 100 <= status <= 599:
                raise ProtocolError("invalid streaming response status")
            if not isinstance(headers, list):
                raise ProtocolError("streaming response headers must be a pair list")
            if content_length is not None and (
                not isinstance(content_length, int) or content_length < 0
            ):
                raise ProtocolError("invalid streaming response content length")
            if content_length is not None and content_length > conn.protocol.max_body:
                raise ProtocolError("response body exceeds tunnel limit")
            if exchange.streaming_response:
                await self._send_response_window(conn, exchange, credits=0)
                return
            if exchange.result.done():
                raise ProtocolError("unexpected streaming response start")
            exchange.streaming_response = True
            exchange.result.set_result(frame)
            await self._send_json(
                conn,
                {
                    "type": "window",
                    "id": frame["id"],
                    "credits": conn.protocol.stream_window,
                    "ack_offset": 0,
                },
            )
        elif frame_type == "http_resp_end":
            if not exchange.streaming_response:
                raise ProtocolError("response end without response start")
            if not exchange.response_complete:
                exchange.response_complete = True
                exchange.response_chunks.put_nowait(None)
        elif frame_type == "http_resp":
            if exchange.result.done():
                raise ProtocolError("duplicate HTTP response")
            if isinstance(frame.get("headers"), list):
                body_text = frame.get("body") or ""
                try:
                    body = base64.b64decode(body_text, validate=True)
                except (ValueError, TypeError) as exc:
                    raise ProtocolError("invalid HTTP response body") from exc
                if len(body) > conn.protocol.max_body:
                    raise ProtocolError("response body exceeds tunnel limit")
                exchange.result.set_result(
                    HttpRespFrame(
                        id=frame.get("id", ""),
                        status=frame.get("status"),
                        headers={},
                        header_items=frame["headers"],
                        body=body,
                    )
                )
            else:
                exchange.result.set_result(parse_frame(json.dumps(frame)))
        elif frame_type == "error":
            error = ErrorFrame(
                id=frame.get("id", ""),
                message=str(frame.get("message", "")),
            )
            if not exchange.result.done():
                exchange.result.set_result(error)
            elif exchange.streaming_response:
                self._put_terminal(exchange.response_chunks, error)

    async def _dispatch_binary(self, conn: _Connection, raw: bytes) -> None:
        if conn.protocol.version < PROTOCOL_VERSION:
            raise ProtocolError("binary data received before V2 negotiation")
        envelope = BinaryEnvelope.decode(raw, conn.protocol.stream_chunk)
        if envelope.kind == BinaryKind.HTTP_RESPONSE_DATA:
            if envelope.end_of_body:
                raise ProtocolError("HTTP response data must end with http_resp_end")
            exchange = self._pending_http.get(envelope.request_id)
            if exchange is None or not self._exchange_matches(exchange, conn):
                raise ProtocolError("response data for unknown stream")
            if not exchange.streaming_response:
                raise ProtocolError("response data before response start")
            if conn.resumable:
                if envelope.offset is None:
                    raise ProtocolError("resumable response chunk is missing offset")
                chunk_end = envelope.offset + len(envelope.payload)
                if envelope.offset < exchange.response_received:
                    if chunk_end <= exchange.response_received:
                        await self._send_response_window(conn, exchange, credits=0)
                        return
                    raise ProtocolError("overlapping response chunk")
                if envelope.offset > exchange.response_received:
                    raise ProtocolError("response chunk gap")
            exchange.response_received += len(envelope.payload)
            if exchange.response_received > conn.protocol.max_body:
                raise ProtocolError("response body exceeds tunnel limit")
            try:
                exchange.response_chunks.put_nowait(envelope.payload)
                if envelope.end_of_body:
                    exchange.response_chunks.put_nowait(None)
            except asyncio.QueueFull as exc:
                raise ProtocolError(
                    "response stream exceeded advertised window"
                ) from exc
            return
        if envelope.kind == BinaryKind.WS_BINARY_DATA:
            channel = self._pending_ws.get(envelope.request_id)
            if channel is None or channel[0] != conn.generation:
                raise ProtocolError("WebSocket data for unknown channel")
            try:
                channel[1].put_nowait(envelope)
            except asyncio.QueueFull as exc:
                raise ProtocolError(
                    "WebSocket channel queue exceeded its limit"
                ) from exc
            return
        raise ProtocolError(f"unexpected binary frame kind: {envelope.kind.name}")

    @staticmethod
    def _exchange_matches(exchange: _HttpExchange, conn: _Connection) -> bool:
        if exchange.session_id is not None:
            return conn.resumable and exchange.session_id == conn.session_id
        return exchange.generation == conn.generation

    @staticmethod
    def _grant_credits(
        exchange: _HttpExchange,
        credit_count: Any,
        ack_offset: Any,
        conn: _Connection,
    ) -> None:
        if not _is_valid_window_frame(credit_count, ack_offset):
            raise ProtocolError("window must carry credits or a valid ack_offset")
        if isinstance(ack_offset, int):
            if ack_offset > exchange.request_next_offset:
                raise ProtocolError("request acknowledgement exceeds sent body")
            exchange.request_unacked = [
                (offset, payload)
                for offset, payload in exchange.request_unacked
                if offset + len(payload) > ack_offset
            ]
        for _ in range(min(credit_count, conn.protocol.stream_window)):
            with contextlib.suppress(asyncio.QueueFull):
                exchange.request_credits.put_nowait(conn.generation)

    async def _send_json(self, conn: _Connection, frame: dict) -> None:
        raw = json.dumps(frame)
        if len(raw.encode("utf-8")) > MAX_TUNNEL_FRAME_SIZE:
            raise ProtocolError("tunnel control frame exceeds limit")
        async with conn.send_lock:
            await conn.websocket.send(raw)

    async def _send_binary(self, conn: _Connection, envelope: BinaryEnvelope) -> None:
        async with conn.send_lock:
            await conn.websocket.send(envelope.encode(conn.protocol.stream_chunk))

    async def _connection_for_exchange(self, exchange: _HttpExchange) -> _Connection:
        while True:
            await self._connection_ready.wait()
            conn = self._active
            if conn is not None and self._exchange_matches(exchange, conn):
                return conn
            await asyncio.sleep(0)

    async def _send_json_exchange(
        self, exchange: _HttpExchange, frame: dict
    ) -> _Connection:
        if exchange.session_id is None:
            conn = self._active
            if conn is None or conn.generation != exchange.generation:
                raise ConnectionResetError("TunnelClient disconnected")
            await self._send_json(conn, frame)
            return conn
        while True:
            conn = await self._connection_for_exchange(exchange)
            try:
                await self._send_json(conn, frame)
                return conn
            except (websockets.ConnectionClosed, ConnectionError, OSError):
                if self._active is conn:
                    self._active = None
                    self._connection_ready.clear()
                exchange.upload_ready.clear()

    async def _send_binary_exchange(
        self,
        exchange: _HttpExchange,
        envelope: BinaryEnvelope,
        *,
        expected_generation: Optional[int] = None,
    ) -> _Connection:
        if exchange.session_id is None:
            conn = self._active
            if conn is None or conn.generation != exchange.generation:
                raise ConnectionResetError("TunnelClient disconnected")
            await self._send_binary(conn, envelope)
            return conn
        while True:
            conn = await self._connection_for_exchange(exchange)
            if (
                expected_generation is not None
                and conn.generation != expected_generation
            ):
                return conn
            try:
                await self._send_binary(conn, envelope)
                return conn
            except (websockets.ConnectionClosed, ConnectionError, OSError):
                if self._active is conn:
                    self._active = None
                    self._connection_ready.clear()
                exchange.upload_ready.clear()
                # The chunk is retained in request_unacked. Reconnection
                # replays it from the peer's cumulative ACK; do not race that
                # replay with a second blind send from the producer task.
                return conn

    async def _send_response_window(
        self,
        conn: _Connection,
        exchange: _HttpExchange,
        *,
        credits: int,
        complete: bool = False,
    ) -> None:
        frame = {
            "type": "window",
            "id": self._request_id_for_exchange(exchange),
            "credits": credits,
            "ack_offset": exchange.response_received,
        }
        if complete:
            frame["complete"] = True
        await self._send_json(conn, frame)

    def _request_id_for_exchange(self, exchange: _HttpExchange) -> str:
        for request_id, pending in self._pending_http.items():
            if pending is exchange:
                return request_id
        raise ProtocolError("HTTP exchange is no longer pending")

    async def _resume_exchange(
        self, conn: _Connection, exchange: _HttpExchange
    ) -> None:
        if exchange.request_frame is not None:
            if exchange.streaming_request:
                exchange.upload_ready.clear()
                exchange.upload_resuming = True
            await self._send_json(conn, exchange.request_frame)
        if exchange.streaming_response:
            queued = exchange.response_chunks.qsize()
            credits = max(0, conn.protocol.stream_window - queued)
            await self._send_response_window(conn, exchange, credits=credits)

    async def _resume_upload(self, conn: _Connection, exchange: _HttpExchange) -> None:
        async with exchange.upload_send_lock:
            for offset, payload in list(exchange.request_unacked):
                if (offset, payload) not in exchange.request_unacked:
                    continue
                while await exchange.request_credits.get() != conn.generation:
                    pass
                await self._send_binary_exchange(
                    exchange,
                    BinaryEnvelope(
                        request_id=self._request_id_for_exchange(exchange),
                        kind=BinaryKind.HTTP_REQUEST_DATA,
                        payload=payload,
                        offset=offset,
                    ),
                )
            if exchange.request_end_sent:
                await self._send_json_exchange(
                    exchange,
                    {
                        "type": "http_req_end",
                        "id": self._request_id_for_exchange(exchange),
                    },
                )
        exchange.upload_ready.set()

    async def _take_upload_credit(self, exchange: _HttpExchange) -> int:
        while True:
            await exchange.upload_ready.wait()
            generation = await exchange.request_credits.get()
            conn = await self._connection_for_exchange(exchange)
            if generation == conn.generation and exchange.upload_ready.is_set():
                return generation

    @staticmethod
    def _put_terminal(queue: asyncio.Queue, item: Any) -> None:
        """Deliver teardown even when all advertised data slots are occupied."""
        if queue.full():
            with contextlib.suppress(asyncio.QueueEmpty):
                queue.get_nowait()
        queue.put_nowait(item)

    def _fail_generation(self, generation: Optional[int], message: str) -> None:
        for request_id, exchange in list(self._pending_http.items()):
            if generation is not None and exchange.generation != generation:
                continue
            error = ErrorFrame(id=request_id, message=message)
            if not exchange.result.done():
                exchange.result.set_result(error)
            else:
                self._put_terminal(exchange.response_chunks, error)
        for request_id, (item_generation, queue) in list(self._pending_ws.items()):
            if generation is None or item_generation == generation:
                self._put_terminal(
                    queue,
                    {
                        "type": "error",
                        "id": request_id,
                        "message": message,
                    },
                )

    async def _negotiated_connection(self) -> Optional[_Connection]:
        conn = self._active
        if conn is None:
            return None
        with contextlib.suppress(asyncio.TimeoutError):
            await asyncio.wait_for(conn.hello_received.wait(), _NEGOTIATION_TIMEOUT)
        return conn if self._active is conn else None

    async def _handle_request(self, request: web.Request):
        if request.headers.get("Upgrade", "").lower() == "websocket":
            return await self._handle_ws(request)
        return await self._handle_http(request)

    async def _handle_http(self, request: web.Request) -> web.StreamResponse:
        conn = await self._negotiated_connection()
        if conn is None:
            return web.Response(status=503, text="No TunnelClient connected")
        async with conn.inflight:
            return await self._proxy_http(conn, request)

    async def _proxy_http(
        self,
        conn: _Connection,
        request: web.Request,
    ) -> web.StreamResponse:
        request_id = make_id()
        exchange = _HttpExchange(
            generation=conn.generation,
            result=self._loop.create_future(),
            response_chunks=asyncio.Queue(maxsize=conn.protocol.stream_window + 1),
            request_credits=asyncio.Queue(maxsize=conn.protocol.stream_window),
            session_id=conn.session_id if conn.resumable else None,
        )
        exchange.upload_ready.set()
        self._pending_http[request_id] = exchange
        upload_task: Optional[asyncio.Task] = None
        disconnect_task = asyncio.create_task(self._wait_downstream_disconnect(request))
        try:
            content_length = request.content_length
            prefetched_body = b""
            unknown_body_complete = False
            if content_length is None:
                prefetched_body, unknown_body_complete = (
                    await self._prefetch_unknown_body(request)
                )
            use_streaming = conn.protocol.version >= PROTOCOL_VERSION and (
                (content_length is None and not unknown_body_complete)
                or (
                    content_length is not None and content_length > self._fast_path_body
                )
            )
            if use_streaming:
                headers = rebuilt_request_header_items(
                    _raw_headers(request),
                    content_length or 0,
                )
                headers = [
                    (name, value)
                    for name, value in headers
                    if name.lower() != "content-length"
                ]
                exchange.streaming_request = True
                exchange.request_frame = {
                    "type": "http_req_begin",
                    "id": request_id,
                    "method": request.method,
                    "path": str(request.rel_url),
                    "headers": headers,
                    "content_length": content_length,
                }
                await self._send_json_exchange(exchange, exchange.request_frame)
                upload_task = asyncio.create_task(
                    self._stream_request_body(
                        conn,
                        request_id,
                        request,
                        exchange,
                        prefetched_body,
                    )
                )
            else:
                limit = (
                    conn.protocol.max_body
                    if conn.protocol.version >= PROTOCOL_VERSION
                    else min(conn.protocol.max_body, MAX_V1_BODY_BYTES)
                )
                if content_length is not None and content_length > limit:
                    return web.Response(
                        status=413,
                        text="Request body exceeds tunnel limit",
                    )
                if content_length is None:
                    body = prefetched_body
                    if not unknown_body_complete:
                        body = await self._read_remaining_limited(
                            request,
                            body,
                            limit,
                        )
                else:
                    body = await request.read()
                if len(body) > limit:
                    return web.Response(
                        status=413,
                        text="Request body exceeds tunnel limit",
                    )
                headers = rebuilt_request_header_items(_raw_headers(request), len(body))
                if conn.protocol.version >= PROTOCOL_VERSION:
                    exchange.request_frame = {
                        "type": "http_req",
                        "id": request_id,
                        "method": request.method,
                        "path": str(request.rel_url),
                        "headers": headers,
                        "body": base64.b64encode(body).decode("ascii"),
                    }
                    await self._send_json_exchange(exchange, exchange.request_frame)
                else:
                    frame = HttpReqFrame(
                        id=request_id,
                        method=request.method,
                        path=str(request.rel_url),
                        headers={},
                        header_items=headers,
                        body=body,
                    )
                    exchange.request_frame = json.loads(frame.to_json())
                    await self._send_json_exchange(exchange, exchange.request_frame)
            if upload_task is not None:
                done, _ = await asyncio.wait(
                    {upload_task, exchange.result, disconnect_task},
                    timeout=_http_timeout(),
                    return_when=asyncio.FIRST_COMPLETED,
                )
                if not done:
                    raise asyncio.TimeoutError
                if upload_task in done:
                    # Surface body-limit, downstream-disconnect, and send
                    # failures immediately instead of waiting for HTTP timeout.
                    upload_task.result()
                if disconnect_task in done:
                    raise ConnectionResetError("downstream HTTP request disconnected")
            done, _ = await asyncio.wait(
                {exchange.result, disconnect_task},
                timeout=_http_timeout(),
                return_when=asyncio.FIRST_COMPLETED,
            )
            if not done:
                raise asyncio.TimeoutError
            if disconnect_task in done:
                raise ConnectionResetError("downstream HTTP request disconnected")
            response = exchange.result.result()
            if isinstance(response, ErrorFrame):
                return web.Response(status=502, text=response.message)
            if isinstance(response, dict):
                return await self._stream_http_response(
                    request,
                    request_id,
                    exchange,
                    response,
                    disconnect_task,
                )
            headers = CIMultiDict()
            for name, value in rebuilt_response_header_items(
                response.header_items,
                request.method,
                response.status,
                len(response.body),
            ):
                headers.add(name, value)
            return web.Response(
                status=response.status,
                headers=headers,
                body=response.body,
            )
        except asyncio.TimeoutError:
            await self._cancel_exchange(conn, request_id, "Tunnel timeout")
            return web.Response(status=504, text="Tunnel timeout")
        except asyncio.CancelledError:
            await self._cancel_exchange(
                conn,
                request_id,
                "downstream HTTP request cancelled",
            )
            raise
        except (web.HTTPException, asyncio.IncompleteReadError):
            await self._cancel_exchange(
                conn,
                request_id,
                "downstream HTTP request failed",
            )
            raise
        except Exception as exc:
            logger.warning("HTTP tunnel request %s failed: %s", request_id, exc)
            await self._cancel_exchange(conn, request_id, str(exc))
            return web.Response(status=502, text=str(exc))
        finally:
            if upload_task is not None and not upload_task.done():
                upload_task.cancel()
                await asyncio.gather(upload_task, return_exceptions=True)
            if not disconnect_task.done():
                disconnect_task.cancel()
                await asyncio.gather(disconnect_task, return_exceptions=True)
            self._pending_http.pop(request_id, None)

    @staticmethod
    async def _wait_downstream_disconnect(request: web.Request) -> None:
        while request.transport is not None and not request.transport.is_closing():
            await asyncio.sleep(0.05)

    async def _prefetch_unknown_body(
        self,
        request: web.Request,
    ) -> tuple[bytes, bool]:
        """Keep small chunked requests on the compatibility fast path."""
        body = bytearray()
        limit = self._fast_path_body + 1
        while len(body) < limit and not request.content.at_eof():
            chunk = await request.content.read(
                min(self._stream_chunk, limit - len(body))
            )
            if not chunk:
                break
            body.extend(chunk)
        return bytes(body), request.content.at_eof()

    async def _read_remaining_limited(
        self,
        request: web.Request,
        prefix: bytes,
        limit: int,
    ) -> bytes:
        body = bytearray(prefix)
        async for chunk in request.content.iter_chunked(self._stream_chunk):
            body.extend(chunk)
            if len(body) > limit:
                raise web.HTTPRequestEntityTooLarge(
                    max_size=limit,
                    actual_size=len(body),
                )
        return bytes(body)

    async def _stream_request_body(
        self,
        conn: _Connection,
        request_id: str,
        request: web.Request,
        exchange: _HttpExchange,
        prefetched_body: bytes = b"",
    ) -> None:
        total = 0
        pending_chunks = [
            prefetched_body[offset : offset + conn.protocol.stream_chunk]
            for offset in range(0, len(prefetched_body), conn.protocol.stream_chunk)
        ]
        for chunk in pending_chunks:
            total += len(chunk)
            if total > conn.protocol.max_body:
                raise ProtocolError("request body exceeds tunnel limit")
            credit_generation = await self._take_upload_credit(exchange)
            offset = exchange.request_next_offset
            exchange.request_next_offset += len(chunk)
            if exchange.session_id is not None:
                exchange.request_unacked.append((offset, chunk))
            async with exchange.upload_send_lock:
                await self._send_binary_exchange(
                    exchange,
                    BinaryEnvelope(
                        request_id=request_id,
                        kind=BinaryKind.HTTP_REQUEST_DATA,
                        payload=chunk,
                        offset=offset if exchange.session_id is not None else None,
                    ),
                    expected_generation=credit_generation,
                )
        async for raw_chunk in request.content.iter_chunked(conn.protocol.stream_chunk):
            for offset in range(0, len(raw_chunk), conn.protocol.stream_chunk):
                chunk = raw_chunk[offset : offset + conn.protocol.stream_chunk]
                total += len(chunk)
                if total > conn.protocol.max_body:
                    raise ProtocolError("request body exceeds tunnel limit")
                credit_generation = await self._take_upload_credit(exchange)
                offset = exchange.request_next_offset
                exchange.request_next_offset += len(chunk)
                if exchange.session_id is not None:
                    exchange.request_unacked.append((offset, chunk))
                async with exchange.upload_send_lock:
                    await self._send_binary_exchange(
                        exchange,
                        BinaryEnvelope(
                            request_id=request_id,
                            kind=BinaryKind.HTTP_REQUEST_DATA,
                            payload=chunk,
                            offset=offset if exchange.session_id is not None else None,
                        ),
                        expected_generation=credit_generation,
                    )
        if request.content_length is not None and total != request.content_length:
            raise ProtocolError("request content length mismatch")
        exchange.request_end_sent = True
        await self._send_json_exchange(
            exchange, {"type": "http_req_end", "id": request_id}
        )

    async def _stream_http_response(
        self,
        request: web.Request,
        request_id: str,
        exchange: _HttpExchange,
        metadata: dict,
        disconnect_task: asyncio.Task,
    ) -> web.StreamResponse:
        status = metadata["status"]
        content_length = metadata.get("content_length")
        headers = CIMultiDict()
        for name, value in rebuilt_response_header_items(
            metadata["headers"],
            request.method,
            status,
            content_length or 0,
        ):
            headers.add(name, value)
        if content_length is None:
            headers.popall("Content-Length", None)
        response = web.StreamResponse(status=status, headers=headers)
        await response.prepare(request)
        received = 0
        while True:
            chunk_task = asyncio.create_task(exchange.response_chunks.get())
            done, _ = await asyncio.wait(
                {chunk_task, disconnect_task},
                timeout=_http_timeout(),
                return_when=asyncio.FIRST_COMPLETED,
            )
            if not done:
                chunk_task.cancel()
                await asyncio.gather(chunk_task, return_exceptions=True)
                raise asyncio.TimeoutError
            if disconnect_task in done:
                chunk_task.cancel()
                await asyncio.gather(chunk_task, return_exceptions=True)
                raise ConnectionResetError("downstream HTTP request disconnected")
            item = chunk_task.result()
            if item is None:
                break
            if isinstance(item, ErrorFrame):
                if request.transport is not None:
                    request.transport.close()
                raise RuntimeError(item.message)
            received += len(item)
            if _body_allowed(request.method, status):
                await response.write(item)
            window = {"type": "window", "id": request_id, "credits": 1}
            if exchange.session_id is not None:
                window["ack_offset"] = exchange.response_received
            await self._send_json_exchange(exchange, window)
        if content_length is not None and _body_allowed(request.method, status):
            if received != content_length:
                if request.transport is not None:
                    request.transport.close()
                raise ProtocolError("response content length mismatch")
        await response.write_eof()
        if exchange.session_id is not None:
            await self._send_json_exchange(
                exchange,
                {
                    "type": "window",
                    "id": request_id,
                    "credits": 0,
                    "ack_offset": exchange.response_received,
                    "complete": True,
                },
            )
        return response

    async def _cancel_exchange(
        self,
        conn: _Connection,
        request_id: str,
        message: str,
    ) -> None:
        with contextlib.suppress(Exception):
            exchange = self._pending_http.get(request_id)
            if exchange is None:
                return
            await self._send_json_exchange(
                exchange,
                {"type": "error", "id": request_id, "message": message},
            )

    async def _handle_ws(self, request: web.Request) -> web.WebSocketResponse:
        response = web.WebSocketResponse(max_msg_size=self._max_ws_message_size)
        await response.prepare(request)
        conn = await self._negotiated_connection()
        if conn is None:
            await response.close(code=1011, message=b"No TunnelClient connected")
            return response
        request_id = make_id()
        message_frames = (
            conn.protocol.max_ws_message + conn.protocol.stream_chunk - 1
        ) // conn.protocol.stream_chunk
        queue: asyncio.Queue = asyncio.Queue(maxsize=message_frames + 2)
        self._pending_ws[request_id] = (conn.generation, queue)
        try:
            headers = {
                name: value
                for name, value in request.headers.items()
                if name.lower() != "host"
            }
            await self._send_json(
                conn,
                json.loads(
                    WsConnectFrame(
                        id=request_id,
                        path=str(request.rel_url),
                        headers=headers,
                    ).to_json()
                ),
            )
            ack = await asyncio.wait_for(queue.get(), timeout=10)
            if ack.get("type") != "ws_connected":
                await response.close(
                    code=1011,
                    message=str(ack.get("message", "tunnel error")).encode(),
                )
                return response

            async def downstream_to_tunnel() -> None:
                async for message in response:
                    if message.type == aiohttp.WSMsgType.TEXT:
                        await self._send_json(
                            conn,
                            {
                                "type": "ws_message",
                                "id": request_id,
                                "data": message.data,
                                "binary": False,
                            },
                        )
                    elif message.type == aiohttp.WSMsgType.BINARY:
                        if len(message.data) > conn.protocol.max_ws_message:
                            raise ProtocolError(
                                "WebSocket message exceeds tunnel limit"
                            )
                        if conn.protocol.version >= PROTOCOL_VERSION:
                            offsets = list(
                                range(
                                    0,
                                    len(message.data),
                                    conn.protocol.stream_chunk,
                                )
                            ) or [0]
                            for offset in offsets:
                                chunk = message.data[
                                    offset : offset + conn.protocol.stream_chunk
                                ]
                                await self._send_binary(
                                    conn,
                                    BinaryEnvelope(
                                        request_id=request_id,
                                        kind=BinaryKind.WS_BINARY_DATA,
                                        payload=chunk,
                                        end_of_body=(
                                            offset + len(chunk) >= len(message.data)
                                        ),
                                    ),
                                )
                        else:
                            await self._send_json(
                                conn,
                                json.loads(
                                    WsMessageFrame(
                                        id=request_id,
                                        data=base64.b64encode(message.data).decode(
                                            "ascii"
                                        ),
                                        binary=True,
                                    ).to_json()
                                ),
                            )
                    elif message.type in (
                        aiohttp.WSMsgType.CLOSE,
                        aiohttp.WSMsgType.ERROR,
                    ):
                        return

            async def tunnel_to_downstream() -> None:
                binary = bytearray()
                while True:
                    item = await queue.get()
                    if isinstance(item, BinaryEnvelope):
                        binary.extend(item.payload)
                        if len(binary) > conn.protocol.max_ws_message:
                            raise ProtocolError(
                                "WebSocket message exceeds tunnel limit"
                            )
                        if item.end_of_body:
                            await response.send_bytes(bytes(binary))
                            binary.clear()
                        continue
                    frame_type = item.get("type")
                    if frame_type == "ws_message":
                        if item.get("binary"):
                            await response.send_bytes(
                                base64.b64decode(item.get("data", ""))
                            )
                        else:
                            await response.send_str(item.get("data", ""))
                    elif frame_type == "ws_close":
                        await response.close(code=item.get("code", 1000))
                        return
                    elif frame_type == "error":
                        await response.close(
                            code=1011,
                            message=str(item.get("message", "tunnel error")).encode(),
                        )
                        return

            tasks = {
                asyncio.create_task(downstream_to_tunnel()),
                asyncio.create_task(tunnel_to_downstream()),
            }
            done, pending = await asyncio.wait(
                tasks,
                return_when=asyncio.FIRST_COMPLETED,
            )
            for task in pending:
                task.cancel()
            await asyncio.gather(*pending, return_exceptions=True)
            for task in done:
                task.result()
        except (asyncio.CancelledError, KeyboardInterrupt):
            raise
        except Exception as exc:
            logger.warning("WS channel %s failed: %s", request_id, exc)
            if not response.closed:
                await response.close(code=1011, message=str(exc).encode())
        finally:
            self._pending_ws.pop(request_id, None)
            if self._active is conn:
                with contextlib.suppress(Exception):
                    await self._send_json(
                        conn,
                        {
                            "type": "ws_close",
                            "id": request_id,
                            "code": response.close_code or 1000,
                            "reason": "",
                        },
                    )
        return response


async def _main(ws_port: int, http_port: int):
    server = TunnelServer(ws_port, http_port)
    await server.start()
    await asyncio.Future()


if __name__ == "__main__":
    import argparse

    parser = argparse.ArgumentParser()
    parser.add_argument("--ws-port", type=int, default=8765)
    parser.add_argument("--http-port", type=int, default=8766)
    arguments = parser.parse_args()
    logging.basicConfig(level=logging.INFO)
    asyncio.run(_main(arguments.ws_port, arguments.http_port))
