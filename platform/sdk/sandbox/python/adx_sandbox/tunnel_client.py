"""TunnelClient: WebSocket-based reverse tunnel for EXECD sandboxes.

Connects to the sandbox's tunnel WebSocket port through the sandbox
router (or frontend gateway) and proxies HTTP and application WebSocket
traffic to services running on the client machine.

Architecture:
  [Local Machine]
    upstream (e.g. 127.0.0.1:8000)
         ^ HTTP
    TunnelClient (control WebSocket + HTTP/WebSocket proxy)
         | WS via router/gateway
         v
  [Sandbox]
    adx-execd tunnel server (Port A:8765 WS, Port B:8766 HTTP)
    sandbox code → http://127.0.0.1:8766 → WS → local upstream
"""

import asyncio
import contextlib
import http.cookiejar
import json
import logging
import os
import ssl
import threading
import time
import uuid
from typing import Any, Optional
from urllib.parse import quote, urljoin, urlsplit, urlunsplit

import httpx
import websockets.asyncio.client as ws_client
import websockets.exceptions as ws_exc

from .tunnel_protocol import (
    BinaryEnvelope,
    BinaryKind,
    DEFAULT_FAST_PATH_BODY_BYTES,
    DEFAULT_MAX_BODY_BYTES,
    DEFAULT_MAX_INFLIGHT,
    DEFAULT_MAX_WS_MESSAGE_BYTES,
    DEFAULT_STREAM_CHUNK_BYTES,
    DEFAULT_STREAM_WINDOW_FRAMES,
    MAX_V1_BODY_BYTES,
    MIN_STREAM_CHUNK_BYTES,
    PROTOCOL_VERSION,
    ProtocolError,
    hello_frame,
)

logger = logging.getLogger(__name__)

_RECONNECT_DELAY = 1.0
_PING_INTERVAL = 20.0
_PING_TIMEOUT = 10.0
_ROUTE_404_WARNING_THRESHOLD = 5
_ROUTE_404_WARNING_INTERVAL = 10
_COMPLETED_FRAME_TTL = 300.0
_COMPLETED_FRAME_LIMIT = 1024
_COMPLETED_FRAME_BYTES_LIMIT = 16 * 1024 * 1024
_WS_CHANNEL_QUEUE_LIMIT = 100
_MAX_CONFIGURED_BODY_BYTES = 1024 * 1024 * 1024
_MAX_CONFIGURED_WS_MESSAGE_BYTES = 8 * 1024 * 1024
_MAX_CONFIGURED_STREAM_CHUNK_BYTES = 64 * 1024
_MAX_CONFIGURED_INFLIGHT = 1024
_MAX_CONFIGURED_WINDOW_FRAMES = 1024
_OUTBOUND_QUEUE_FRAMES = 512
_OUTBOUND_CONTROL_RESERVE = 32
_CONTROL_WS_MESSAGE_BYTES = 8 * 1024 * 1024
_HOP_BY_HOP_HEADERS = frozenset(
    {
        "connection",
        "keep-alive",
        "proxy-authenticate",
        "proxy-authorization",
        "proxy-connection",
        "te",
        "trailer",
        "transfer-encoding",
        "upgrade",
    }
)


def _positive_int_env(name: str, default: int, maximum: int) -> int:
    raw = os.environ.get(name)
    try:
        value = default if raw is None else int(raw)
    except ValueError as exc:
        raise ValueError(f"{name} must be a positive integer") from exc
    if value <= 0:
        raise ValueError(f"{name} must be greater than zero")
    return min(value, maximum)


def _http_timeout_for_tunnel() -> httpx.Timeout:
    """Preserve the legacy tunnel-wide HTTP timeout environment contract."""
    try:
        seconds = float(os.environ.get("ADX_TUNNEL_HTTP_TIMEOUT", "600"))
    except ValueError:
        seconds = 600.0
    return httpx.Timeout(seconds)


def _header_pairs(headers):
    """Decode legacy dictionaries and ordered header pairs."""
    if isinstance(headers, dict):
        headers = headers.items()
    pairs = []
    for item in headers or []:
        if not isinstance(item, (list, tuple)) or len(item) != 2:
            raise ValueError("tunnel headers must be [name, value] pairs")
        name, value = item
        if not isinstance(name, str) or not isinstance(value, str):
            raise TypeError("tunnel header names and values must be strings")
        pairs.append((name, value))
    return pairs


def _headers_for_rebuilt_request(headers):
    """Return ordered second-hop headers after removing connection metadata."""
    pairs = _header_pairs(headers)
    connection_tokens = {
        token.strip().lower()
        for name, value in pairs
        if name.lower() == "connection"
        for token in value.split(",")
        if token.strip()
    }
    excluded = (
        _HOP_BY_HOP_HEADERS
        | connection_tokens
        | {
            "host",
            "content-length",
            "expect",
        }
    )
    return [(name, value) for name, value in pairs if name.lower() not in excluded]


class _NoCookieJar(http.cookiejar.CookieJar):
    """Keep pooled tunnel requests free of ambient HTTP cookie state."""

    def extract_cookies(self, _response, _request):
        return

    def add_cookie_header(self, _request):
        return


def _ssl_context_for_tunnel(tunnel_ws_url):
    if not tunnel_ws_url.startswith("wss://"):
        return None
    ca_bundle = os.environ.get("ADX_TUNNEL_CA_BUNDLE") or None
    context = ssl.create_default_context(cafile=ca_bundle)
    verify = os.environ.get("ADX_TUNNEL_SSL_VERIFY", "1").strip().lower()
    if verify in ("0", "false", "no"):
        logger.warning(
            "TunnelClient TLS certificate verification is explicitly disabled"
        )
        context.check_hostname = False
        context.verify_mode = ssl.CERT_NONE
    return context


class TunnelClient:
    """WebSocket tunnel from sandbox back to a local upstream service.

    Usage::

        tunnel = TunnelClient(upstream="127.0.0.1:8000")
        tunnel.start("ws://router/safeID/8765", timeout=30)
        # sandbox code can now reach local :8000 via its proxy port
        tunnel.stop()

    ``ping_interval`` and ``ping_timeout`` configure JSON application
    heartbeats. WebSocket protocol ping is disabled because some gateways
    don't count control frames when enforcing idle timeouts.
    """

    def __init__(
        self,
        upstream: str,
        token: Optional[str] = None,
        ping_interval: float = _PING_INTERVAL,
        ping_timeout: float = _PING_TIMEOUT,
        *,
        sandbox_id: Optional[str] = None,
    ):
        if ping_interval <= 0:
            raise ValueError("ping_interval must be greater than zero")
        if ping_timeout <= 0:
            raise ValueError("ping_timeout must be greater than zero")
        self._upstream = upstream
        self._token = token
        self._sandbox_id = sandbox_id
        self._ping_interval = ping_interval
        self._ping_timeout = ping_timeout
        self._protocol_version = _positive_int_env(
            "ADX_TUNNEL_PROTOCOL_VERSION", PROTOCOL_VERSION, PROTOCOL_VERSION
        )
        # These process-local knobs are optional. Current sandbox creation does
        # not need to inject them: both peers advertise defaults and negotiate
        # the lower value. Operators can override them independently when the
        # TunnelClient and adx-execd processes need tighter limits.
        self._max_body_size = _positive_int_env(
            "ADX_TUNNEL_MAX_BODY_SIZE",
            DEFAULT_MAX_BODY_BYTES,
            _MAX_CONFIGURED_BODY_BYTES,
        )
        self._max_ws_message_size = _positive_int_env(
            "ADX_TUNNEL_MAX_WS_MESSAGE_SIZE",
            DEFAULT_MAX_WS_MESSAGE_BYTES,
            _MAX_CONFIGURED_WS_MESSAGE_BYTES,
        )
        self._max_stream_chunk = max(
            MIN_STREAM_CHUNK_BYTES,
            _positive_int_env(
                "ADX_TUNNEL_STREAM_CHUNK_BYTES",
                DEFAULT_STREAM_CHUNK_BYTES,
                _MAX_CONFIGURED_STREAM_CHUNK_BYTES,
            ),
        )
        self._max_inflight = _positive_int_env(
            "ADX_TUNNEL_MAX_INFLIGHT", DEFAULT_MAX_INFLIGHT, _MAX_CONFIGURED_INFLIGHT
        )
        self._stream_window_frames = _positive_int_env(
            "ADX_TUNNEL_STREAM_WINDOW_FRAMES",
            DEFAULT_STREAM_WINDOW_FRAMES,
            _MAX_CONFIGURED_WINDOW_FRAMES,
        )
        self._fast_path_body_bytes = _positive_int_env(
            "ADX_TUNNEL_FAST_PATH_BODY_BYTES",
            DEFAULT_FAST_PATH_BODY_BYTES,
            min(self._max_body_size, MAX_V1_BODY_BYTES),
        )
        self._loop: Optional[asyncio.AbstractEventLoop] = None
        self._thread: Optional[threading.Thread] = None
        self._connected = threading.Event()
        self._stopping = threading.Event()
        self._ws: Any = None
        self._session_id = str(uuid.uuid4())
        self._resume_state: Optional[dict[str, Any]] = None
        self._http_client: Optional[httpx.AsyncClient] = None
        self._checkpoint_scope_depth = 0
        self._checkpoint_scope_lock = threading.Lock()

    def start(self, tunnel_ws_url: str, timeout: float = 60) -> bool:
        """Start the tunnel client in a background thread.

        Returns True if the WebSocket connected within *timeout* seconds.
        """
        self._stopping.clear()
        self._connected.clear()
        self._thread = threading.Thread(
            target=self._run_loop,
            args=(tunnel_ws_url,),
            daemon=True,
            name="tunnel-client",
        )
        self._thread.start()
        return self._connected.wait(timeout=timeout)

    def stop(self) -> None:
        """Signal the tunnel client to stop and wait for the thread."""
        self._stopping.set()
        if self._loop is not None and self._loop.is_running() and self._ws is not None:
            fut = asyncio.run_coroutine_threadsafe(self._ws.close(), self._loop)
            try:
                fut.result(timeout=2)
            except Exception:
                pass
        if self._thread is not None and self._thread.is_alive():
            self._thread.join(timeout=5)
        if (
            self._thread is not None
            and self._thread.is_alive()
            and self._loop is not None
        ):
            # Last-resort fallback for a wedged event loop. The normal path
            # closes the WebSocket above, letting _connect_loop exit cleanly.
            self._loop.call_soon_threadsafe(self._loop.stop)
            self._thread.join(timeout=2)

    @contextlib.contextmanager
    def checkpoint_inflight(self):
        """Mark a checkpoint operation while allowing tunnel recovery to continue.

        Checkpointing may briefly interrupt the sandbox-side tunnel while
        runtime listeners are preserved or re-armed. This scope only
        downgrades expected reconnect noise; the reconnect loop and resumable
        in-flight request handling remain unchanged.
        """
        with self._checkpoint_scope_lock:
            self._checkpoint_scope_depth += 1
        try:
            yield
        finally:
            with self._checkpoint_scope_lock:
                self._checkpoint_scope_depth = max(
                    0, self._checkpoint_scope_depth - 1
                )

    def _checkpoint_is_inflight(self) -> bool:
        with self._checkpoint_scope_lock:
            return self._checkpoint_scope_depth > 0

    def _log_reconnect(self, level: int, message: str, *args: Any) -> None:
        if self._checkpoint_is_inflight():
            logger.debug("checkpoint in-flight; " + message, *args)
            return
        logger.log(level, message, *args)

    def _run_loop(self, tunnel_ws_url: str) -> None:
        self._loop = asyncio.new_event_loop()
        asyncio.set_event_loop(self._loop)
        try:
            self._loop.run_until_complete(self._connect_loop(tunnel_ws_url))
        except RuntimeError:
            # Event loop was stopped by the last-resort shutdown path.
            pass
        finally:
            pending = [
                task for task in asyncio.all_tasks(self._loop) if not task.done()
            ]
            for task in pending:
                task.cancel()
            if pending:
                self._loop.run_until_complete(
                    asyncio.gather(*pending, return_exceptions=True)
                )
            if self._http_client is not None:
                close = getattr(self._http_client, "aclose", None)
                if close is not None:
                    self._loop.run_until_complete(close())
                else:
                    self._loop.run_until_complete(
                        self._http_client.__aexit__(None, None, None)
                    )
                self._http_client = None
            self._loop.run_until_complete(self._loop.shutdown_asyncgens())
            self._loop.close()

    async def _close_shared_http_client(self) -> None:
        if self._http_client is None:
            return
        client, self._http_client = self._http_client, None
        close = getattr(client, "aclose", None)
        if close is not None:
            await close()
        else:
            await client.__aexit__(None, None, None)

    async def _connect_loop(self, tunnel_ws_url: str) -> None:
        failures = 0
        # Loading CA certificates is expensive; reuse both contexts across reconnects.
        ssl_context = _ssl_context_for_tunnel(tunnel_ws_url)
        http_ssl_context = httpx.create_ssl_context(
            verify=True,
            trust_env=False,
        )
        self._resume_state = {
            "send_lock": asyncio.Lock(),
            "ws_ready": asyncio.Event(),
            "resume_enabled": True,
            "inflight": {},
            "completed": {},
            "stream_requests": {},
            "response_credits": {},
            "terminated_streams": {},
            "tasks": set(),
            "http_tasks": set(),
        }
        self._http_client = httpx.AsyncClient(
            limits=httpx.Limits(max_keepalive_connections=5, max_connections=10),
            timeout=_http_timeout_for_tunnel(),
            verify=http_ssl_context,
            trust_env=False,
            cookies=_NoCookieJar(),
        )
        while not self._stopping.is_set():
            connected_at = None
            try:
                _extra_headers = {}
                if self._sandbox_id:
                    _extra_headers["X-Sandbox-ID"] = self._sandbox_id
                connect_url = tunnel_ws_url
                if self._token:
                    _extra_headers["Authorization"] = f"Bearer {self._token}"
                    _extra_headers["X-Auth"] = self._token
                    # Keep credentials out of gateway access logs by default.
                    # Operators that sit behind a gateway known to drop custom
                    # WebSocket headers can explicitly enable the query-token
                    # fallback.
                    token_query_fallback = (
                        os.environ.get("ADX_TUNNEL_TOKEN_QUERY_FALLBACK", "0")
                        .strip()
                        .lower()
                    )
                    if token_query_fallback in ("1", "true", "yes"):
                        sep = "&" if "?" in connect_url else "?"
                        connect_url = (
                            f"{connect_url}{sep}token={quote(self._token, safe='')}"
                        )
                async with ws_client.connect(
                    connect_url,
                    max_size=_CONTROL_WS_MESSAGE_BYTES,
                    # Use JSON application frames for the tunnel heartbeat.
                    # Some gateways don't count WebSocket control frames as
                    # activity and close otherwise healthy idle tunnels.
                    ping_interval=None,
                    ping_timeout=None,
                    close_timeout=5,
                    ssl=ssl_context,
                    additional_headers=_extra_headers,
                ) as ws:
                    self._ws = ws
                    await ws.send(
                        json.dumps(
                            hello_frame(
                                protocol_version=self._protocol_version,
                                max_stream_chunk=self._max_stream_chunk,
                                max_inflight=self._max_inflight,
                                stream_window_frames=self._stream_window_frames,
                                max_body_size=self._max_body_size,
                                max_ws_message_size=self._max_ws_message_size,
                                resumable=True,
                                session_id=self._session_id,
                            )
                        )
                    )
                    self._resume_state["ws_ready"].set()
                    # A local send failure raises here before start() observes
                    # `_connected`. Peer-side hello rejection is observable only
                    # when the receive loop closes because V2 has no hello ACK.
                    self._connected.set()
                    connected_at = time.monotonic()
                    logger.info("TunnelClient connected: %s", tunnel_ws_url)
                    try:
                        await self._proxy_loop(ws, http_ssl_context)
                    finally:
                        self._resume_state["ws_ready"].clear()
                        for response_state in list(
                            self._resume_state["response_credits"].values()
                        ):
                            response_state["ready"].clear()
                        self._ws = None
                        self._connected.clear()
                    if self._stopping.is_set():
                        await self._close_shared_http_client()
                        return
                    # Normalize a clean iterator end into the same retry path as
                    # connection errors so accounting and backoff cannot diverge.
                    raise ConnectionError("tunnel peer ended the connection")
            except (ws_exc.ConnectionClosed, OSError, asyncio.TimeoutError) as e:
                self._connected.clear()
                if (
                    connected_at is not None
                    and time.monotonic() - connected_at >= self._ping_interval
                ):
                    failures = 0
                failures += 1
                if failures == 1 or failures % 10 == 0:
                    self._log_reconnect(
                        logging.WARNING,
                        "TunnelClient disconnected (attempt %d): %s", failures, e
                    )
                if self._stopping.is_set():
                    await self._close_shared_http_client()
                    return
                await asyncio.sleep(min(_RECONNECT_DELAY * min(failures, 30), 30))
            except ws_exc.InvalidStatus as e:
                self._connected.clear()
                failures += 1
                status_code = e.response.status_code
                if status_code == 404:
                    # Hide the short route-publication window, but surface a
                    # persistent missing route without logging every second.
                    warn = failures == _ROUTE_404_WARNING_THRESHOLD or (
                        failures > _ROUTE_404_WARNING_THRESHOLD
                        and failures % _ROUTE_404_WARNING_INTERVAL == 0
                    )
                    self._log_reconnect(
                        logging.WARNING if warn else logging.DEBUG,
                        "TunnelClient route unavailable "
                        "(HTTP 404, attempt %d); retrying",
                        failures,
                    )
                else:
                    self._log_reconnect(
                        logging.WARNING,
                        "TunnelClient WebSocket handshake rejected "
                        "(HTTP %d, attempt %d): %s",
                        status_code,
                        failures,
                        e,
                    )
                if self._stopping.is_set():
                    await self._close_shared_http_client()
                    return
                await asyncio.sleep(_RECONNECT_DELAY)
            except Exception as e:
                failures += 1
                self._log_reconnect(
                    logging.ERROR, "TunnelClient unexpected error: %s", e
                )
                if self._stopping.is_set():
                    await self._close_shared_http_client()
                    return
                await asyncio.sleep(_RECONNECT_DELAY)

    @contextlib.asynccontextmanager
    async def _borrow_http_client(self, http_ssl_context=None):
        if self._http_client is not None:
            yield self._http_client
            return
        verify = http_ssl_context if http_ssl_context is not None else True
        async with httpx.AsyncClient(
            limits=httpx.Limits(max_keepalive_connections=5, max_connections=10),
            timeout=_http_timeout_for_tunnel(),
            verify=verify,
            trust_env=False,
            cookies=_NoCookieJar(),
        ) as client:
            yield client

    async def _proxy_loop(self, ws, http_ssl_context=None) -> None:
        """Relay execd tunnel frames to/from the upstream HTTP service.

        EXECD's tunnel server (Port A) keeps metadata and small bodies in JSON
        text frames. After V2 hello negotiation, large HTTP bodies and binary
        WebSocket messages use bounded raw binary envelopes. ``ping`` frames
        are answered with ``pong`` (heartbeat), and HTTP forwarding is capped
        by the negotiated in-flight limit.
        """
        import asyncio
        import base64

        resume_state = self._resume_state
        if resume_state is None:
            resume_state = {
                "send_lock": asyncio.Lock(),
                "ws_ready": asyncio.Event(),
                "resume_enabled": True,
                "inflight": {},
                "completed": {},
                "stream_requests": {},
                "response_credits": {},
                "terminated_streams": {},
                "tasks": set(),
                "http_tasks": set(),
            }
            resume_state["ws_ready"].set()
            self._resume_state = resume_state
        send_lock = resume_state["send_lock"]
        inflight: dict = resume_state["inflight"]
        completed: dict = resume_state["completed"]
        ws_channels: dict[str, asyncio.Queue] = {}
        ws_tasks: dict[str, asyncio.Task] = {}
        stream_requests: dict[str, dict[str, Any]] = resume_state["stream_requests"]
        response_credits: dict[str, dict[str, Any]] = resume_state["response_credits"]
        terminated_streams: dict[str, float] = resume_state["terminated_streams"]
        pong_received = asyncio.Event()
        pending_ping_id: str | None = None
        heartbeat_timed_out = asyncio.Event()
        negotiated_protocol_version = 1
        negotiated_stream_chunk = self._max_stream_chunk
        negotiated_max_inflight = self._max_inflight
        negotiated_stream_window = self._stream_window_frames
        negotiated_max_body_size = self._max_body_size
        negotiated_max_ws_message_size = self._max_ws_message_size
        negotiated_resumable = True

        async def send_frame(obj: dict) -> None:
            # websockets does not allow concurrent send() from multiple tasks;
            # serialize since http_req frames are handled on their own tasks.
            raw = json.dumps(obj)
            if len(raw.encode("utf-8")) > _CONTROL_WS_MESSAGE_BYTES:
                raise ProtocolError("tunnel control frame exceeds 8 MiB limit")
            while not self._stopping.is_set():
                await resume_state["ws_ready"].wait()
                current_ws = self._ws or ws
                try:
                    async with send_lock:
                        if self._ws is not None and current_ws is not self._ws:
                            continue
                        await current_ws.send(raw)
                    return
                except ws_exc.ConnectionClosed:
                    resume_state["ws_ready"].clear()
            raise asyncio.CancelledError

        async def send_binary(envelope: BinaryEnvelope) -> None:
            encoded = envelope.encode(negotiated_stream_chunk)
            while not self._stopping.is_set():
                await resume_state["ws_ready"].wait()
                current_ws = self._ws or ws
                try:
                    async with send_lock:
                        if self._ws is not None and current_ws is not self._ws:
                            continue
                        await current_ws.send(encoded)
                    return
                except ws_exc.ConnectionClosed:
                    resume_state["ws_ready"].clear()
            raise asyncio.CancelledError

        async def heartbeat_loop() -> None:
            nonlocal pending_ping_id
            while not self._stopping.is_set():
                await asyncio.sleep(self._ping_interval)
                if self._stopping.is_set():
                    return

                pending_ping_id = uuid.uuid4().hex
                timestamp = int(time.time() * 1000)
                pong_received.clear()
                await send_frame(
                    {
                        "type": "ping",
                        "id": pending_ping_id,
                        "timestamp": timestamp,
                    }
                )
                try:
                    await asyncio.wait_for(
                        pong_received.wait(),
                        timeout=self._ping_timeout,
                    )
                except asyncio.TimeoutError:
                    heartbeat_timed_out.set()
                    self._log_reconnect(
                        logging.WARNING,
                        "TunnelClient application heartbeat timed out after %.1fs",
                        self._ping_timeout,
                    )
                    await ws.close()
                    return

        def cleanup_completed() -> None:
            def cached_frame_bytes(frame: dict) -> int:
                headers = _header_pairs(frame.get("headers"))
                return len(frame.get("body", "")) + sum(
                    len(name) + len(value) for name, value in headers
                )

            now = time.monotonic()
            expired = [
                rid
                for rid, (_, ts) in completed.items()
                if now - ts > _COMPLETED_FRAME_TTL
            ]
            for rid in expired:
                completed.pop(rid, None)
            cached_bytes = sum(
                cached_frame_bytes(frame) for frame, _ in completed.values()
            )
            while (
                len(completed) > _COMPLETED_FRAME_LIMIT
                or cached_bytes > _COMPLETED_FRAME_BYTES_LIMIT
            ):
                oldest = min(completed.items(), key=lambda item: item[1][1])[0]
                evicted, _ = completed.pop(oldest)
                cached_bytes -= cached_frame_bytes(evicted)

        def remember_terminated(rid: str) -> None:
            now = time.monotonic()
            expired = [
                request_id
                for request_id, timestamp in terminated_streams.items()
                if now - timestamp > _COMPLETED_FRAME_TTL
            ]
            for request_id in expired:
                terminated_streams.pop(request_id, None)
            while len(terminated_streams) >= _COMPLETED_FRAME_LIMIT:
                oldest = min(terminated_streams, key=terminated_streams.get)
                terminated_streams.pop(oldest, None)
            terminated_streams[rid] = now

        def is_terminated(rid: str) -> bool:
            timestamp = terminated_streams.get(rid)
            if timestamp is None:
                return False
            if time.monotonic() - timestamp > _COMPLETED_FRAME_TTL:
                terminated_streams.pop(rid, None)
                return False
            return True

        def response_metadata(resp: httpx.Response, legacy_headers: bool = False):
            response_headers = [
                (name.decode("ascii"), value.decode("latin-1"))
                for name, value in resp.headers.raw
            ]
            content_length = None
            for name, value in response_headers:
                if name.lower() == "content-length":
                    try:
                        content_length = int(value)
                    except ValueError:
                        pass
                    break
            return (dict(response_headers) if legacy_headers else response_headers), content_length

        async def send_streaming_response(
            rid: str,
            resp: httpx.Response,
            body_expected: bool,
            legacy_headers: bool = False,
        ) -> None:
            response_headers, content_length = response_metadata(resp, legacy_headers)
            if (
                body_expected
                and content_length is not None
                and content_length > negotiated_max_body_size
            ):
                raise ProtocolError("response body exceeds tunnel limit")
            credit_queue: asyncio.Queue = asyncio.Queue(
                maxsize=negotiated_stream_window
            )
            response_ready = asyncio.Event()
            response_ready.set()
            begin_frame = {
                "type": "http_resp_begin",
                "id": rid,
                "status": resp.status_code,
                "headers": response_headers,
                "content_length": content_length,
            }
            response_state = {
                "credits": credit_queue,
                "unacked": [],
                "next_offset": 0,
                "ready": response_ready,
                "send_lock": asyncio.Lock(),
                "ended": False,
                "begin_frame": begin_frame,
            }
            response_credits[rid] = response_state
            try:
                await send_frame(begin_frame)
                response_size = 0
                async for raw_chunk in resp.aiter_raw():
                    response_size += len(raw_chunk)
                    if response_size > negotiated_max_body_size:
                        raise ProtocolError("response body exceeds tunnel limit")
                    for offset in range(0, len(raw_chunk), negotiated_stream_chunk):
                        await response_ready.wait()
                        await credit_queue.get()
                        chunk = raw_chunk[offset : offset + negotiated_stream_chunk]
                        chunk_offset = response_state["next_offset"]
                        response_state["next_offset"] += len(chunk)
                        response_state["unacked"].append((chunk_offset, chunk))
                        async with response_state["send_lock"]:
                            await send_binary(
                                BinaryEnvelope(
                                    request_id=rid,
                                    kind=BinaryKind.HTTP_RESPONSE_DATA,
                                    payload=chunk,
                                    offset=chunk_offset,
                                )
                            )
                if (
                    body_expected
                    and content_length is not None
                    and response_size != content_length
                ):
                    raise ProtocolError("response content length mismatch")
                response_state["ended"] = True
                await send_frame({"type": "http_resp_end", "id": rid})
            finally:
                pass

        async def build_http_resp_frame(client, frame: dict) -> dict | None:
            rid = frame.get("id", "")
            method = frame.get("method", "GET")
            path = frame.get("path", "/")
            try:
                req_headers = _headers_for_rebuilt_request(frame.get("headers") or [])
                body_b64 = frame.get("body") or ""
                body = base64.b64decode(body_b64) if body_b64 else b""
                base_url = self._upstream
                if "://" not in base_url:
                    base_url = f"http://{base_url}"
                async with client.stream(
                    method,
                    urljoin(f"{base_url.rstrip('/')}/", path.lstrip("/")),
                    headers=req_headers or None,
                    content=body,
                ) as resp:
                    legacy_headers = isinstance(frame.get("headers"), dict)
                    response_headers, content_length = response_metadata(resp, legacy_headers)
                    response_body_limit = (
                        negotiated_max_body_size
                        if negotiated_protocol_version >= PROTOCOL_VERSION
                        else min(negotiated_max_body_size, MAX_V1_BODY_BYTES)
                    )
                    if (
                        content_length is not None
                        and content_length > response_body_limit
                    ):
                        raise ProtocolError("response body requires tunnel protocol V2")
                    if negotiated_protocol_version >= PROTOCOL_VERSION and (
                        content_length is None
                        or content_length > self._fast_path_body_bytes
                    ):
                        body_expected = method.upper() != "HEAD" and not (
                            100 <= resp.status_code < 200
                            or resp.status_code in (204, 304)
                        )
                        await send_streaming_response(rid, resp, body_expected, legacy_headers)
                        return None
                    raw_body = b"".join([chunk async for chunk in resp.aiter_raw()])
                    if len(raw_body) > response_body_limit:
                        raise ProtocolError("response body exceeds tunnel limit")
                return {
                    "type": "http_resp",
                    "id": rid,
                    "status": resp.status_code,
                    "headers": response_headers,
                    "body": base64.b64encode(raw_body).decode("ascii"),
                }
            except Exception as e:  # upstream unreachable / fetch error
                logger.debug("tunnel http_req upstream error: %s", e)
                return {"type": "error", "id": rid, "message": str(e)}

        async def stream_http_response(
            client: httpx.AsyncClient,
            frame: dict,
            request_state: dict[str, Any],
        ) -> None:
            rid = frame["id"]
            method = frame.get("method", "GET")
            path = frame.get("path", "/")
            request_queue: asyncio.Queue = request_state["queue"]

            async def request_content():
                while True:
                    chunk = await request_queue.get()
                    if chunk is None:
                        return
                    request_state["consumed"] += len(chunk)
                    request_state["pending_credit"] += 1
                    yield chunk
                    try:
                        await send_frame(
                            {
                                "type": "window",
                                "id": rid,
                                "credits": 1,
                                "ack_offset": request_state["consumed"],
                            }
                        )
                    finally:
                        request_state["pending_credit"] -= 1

            try:
                req_headers = _headers_for_rebuilt_request(frame.get("headers") or [])
                content_length = request_state["content_length"]
                if content_length is not None:
                    req_headers.append(("Content-Length", str(content_length)))
                base_url = self._upstream
                if "://" not in base_url:
                    base_url = f"http://{base_url}"
                async with client.stream(
                    method,
                    urljoin(f"{base_url.rstrip('/')}/", path.lstrip("/")),
                    headers=req_headers or None,
                    content=request_content(),
                ) as resp:
                    body_expected = method.upper() != "HEAD" and not (
                        100 <= resp.status_code < 200 or resp.status_code in (204, 304)
                    )
                    await send_streaming_response(
                        rid, resp, body_expected, isinstance(frame.get("headers"), dict)
                    )
            except asyncio.CancelledError:
                raise
            except Exception as exc:
                logger.debug("tunnel streaming request error: %s", exc)
                try:
                    await send_frame({"type": "error", "id": rid, "message": str(exc)})
                except ws_exc.ConnectionClosed:
                    pass
            finally:
                stream_requests.pop(rid, None)
                remember_terminated(rid)

        async def await_and_send(rid: str, task) -> None:
            frame = await task
            if frame is not None:
                await send_frame(frame)

        def handle_http_req(client, frame: dict):
            rid = frame.get("id", "")
            cleanup_completed()
            if rid and rid in response_credits:
                return asyncio.ensure_future(
                    send_frame(response_credits[rid]["begin_frame"])
                )
            if rid and rid in completed:
                cached, _ = completed[rid]
                return asyncio.ensure_future(send_frame(cached))
            if rid and rid in inflight:
                return asyncio.ensure_future(asyncio.sleep(0))

            task = asyncio.ensure_future(build_http_resp_frame(client, frame))
            if rid:
                inflight[rid] = task

                def done_callback(t, request_id=rid):
                    inflight.pop(request_id, None)
                    if not t.cancelled():
                        try:
                            result = t.result()
                            if result is not None:
                                completed[request_id] = (result, time.monotonic())
                                cleanup_completed()
                        except Exception:
                            pass

                task.add_done_callback(done_callback)
            return asyncio.ensure_future(await_and_send(rid, task))

        async def handle_http_req_begin(client, frame: dict):
            rid = frame.get("id")
            if not isinstance(rid, str):
                raise ProtocolError("streaming request id must be a UUID string")
            try:
                uuid.UUID(rid)
            except ValueError as exc:
                raise ProtocolError(
                    "streaming request id must be a UUID string"
                ) from exc
            if rid in stream_requests:
                request_state = stream_requests[rid]
                await send_frame(
                    {
                        "type": "window",
                        "id": rid,
                        "credits": max(
                            0,
                            negotiated_stream_window
                            - request_state["queue"].qsize()
                            - request_state["pending_credit"],
                        ),
                        "ack_offset": request_state["consumed"],
                    }
                )
                return request_state["task"]
            if rid in response_credits:
                await send_frame(response_credits[rid]["begin_frame"])
                return None
            content_length = frame.get("content_length")
            if content_length is not None:
                if not isinstance(content_length, int) or content_length < 0:
                    raise ProtocolError("invalid streaming request content_length")
                if content_length > negotiated_max_body_size:
                    await send_frame(
                        {
                            "type": "error",
                            "id": rid,
                            "message": "request body exceeds tunnel limit",
                        }
                    )
                    return None
            if len(http_tasks) >= negotiated_max_inflight:
                await send_frame(
                    {
                        "type": "error",
                        "id": rid,
                        "message": "tunnel max_inflight limit reached",
                    }
                )
                return None
            request_state: dict[str, Any] = {
                "queue": asyncio.Queue(maxsize=negotiated_stream_window),
                "received": 0,
                "consumed": 0,
                "pending_credit": 0,
                "content_length": content_length,
                "ended": False,
            }
            stream_requests[rid] = request_state
            task = asyncio.create_task(
                stream_http_response(client, frame, request_state)
            )
            request_state["task"] = task
            tasks.add(task)
            http_tasks.add(task)
            task.add_done_callback(tasks.discard)
            task.add_done_callback(http_tasks.discard)
            await send_frame(
                {
                    "type": "window",
                    "id": rid,
                    "credits": negotiated_stream_window,
                    "ack_offset": 0,
                }
            )
            return task

        def websocket_target(path: str) -> str:
            base_url = self._upstream
            if "://" not in base_url:
                base_url = f"http://{base_url}"
            target = urljoin(f"{base_url.rstrip('/')}/", path.lstrip("/"))
            parsed = urlsplit(target)
            if parsed.scheme == "http":
                scheme = "ws"
            elif parsed.scheme == "https":
                scheme = "wss"
            elif parsed.scheme in ("ws", "wss"):
                scheme = parsed.scheme
            else:
                raise ValueError(
                    f"unsupported reverse WebSocket target scheme: {parsed.scheme}"
                )
            return urlunsplit(parsed._replace(scheme=scheme))

        def websocket_options(frame: dict) -> dict[str, Any]:
            headers = frame.get("headers") or {}
            additional_headers: dict[str, str] = {}
            origin: str | None = None
            subprotocols: list[str] | None = None
            ignored = {
                "connection",
                "content-length",
                "host",
                "sec-websocket-extensions",
                "sec-websocket-key",
                "sec-websocket-version",
                "upgrade",
            }
            for key, value in headers.items():
                lower = key.lower()
                if lower == "origin":
                    origin = value
                elif lower == "sec-websocket-protocol":
                    subprotocols = [
                        item.strip() for item in value.split(",") if item.strip()
                    ]
                elif lower not in ignored:
                    additional_headers[key] = value
            options: dict[str, Any] = {
                # Application WebSocket messages are reassembled before being
                # forwarded, so they have an independent bounded limit. HTTP
                # bodies remain streaming and use negotiated_max_body_size.
                "max_size": (
                    negotiated_max_ws_message_size
                    if negotiated_protocol_version >= PROTOCOL_VERSION
                    else min(negotiated_max_body_size, MAX_V1_BODY_BYTES)
                ),
                "additional_headers": additional_headers or None,
            }
            if origin:
                options["origin"] = origin
            if subprotocols:
                options["subprotocols"] = subprotocols
            return options

        async def handle_ws_connect(frame: dict, queue: asyncio.Queue) -> None:
            rid = frame.get("id", "")
            try:
                async with ws_client.connect(
                    websocket_target(frame.get("path", "/")),
                    **websocket_options(frame),
                ) as upstream_ws:
                    await send_frame({"type": "ws_connected", "id": rid})

                    async def from_upstream() -> None:
                        async for upstream_message in upstream_ws:
                            if isinstance(upstream_message, str):
                                await send_frame(
                                    {
                                        "type": "ws_message",
                                        "id": rid,
                                        "data": upstream_message,
                                        "binary": False,
                                    }
                                )
                            elif negotiated_protocol_version >= PROTOCOL_VERSION:
                                if (
                                    len(upstream_message)
                                    > negotiated_max_ws_message_size
                                ):
                                    raise ProtocolError(
                                        "WebSocket binary message exceeds tunnel limit"
                                    )
                                chunks = (
                                    [b""]
                                    if not upstream_message
                                    else (
                                        upstream_message[
                                            offset : offset + negotiated_stream_chunk
                                        ]
                                        for offset in range(
                                            0,
                                            len(upstream_message),
                                            negotiated_stream_chunk,
                                        )
                                    )
                                )
                                chunk_count = max(
                                    1,
                                    (
                                        len(upstream_message)
                                        + negotiated_stream_chunk
                                        - 1
                                    )
                                    // negotiated_stream_chunk,
                                )
                                for index, chunk in enumerate(chunks):
                                    await send_binary(
                                        BinaryEnvelope(
                                            request_id=rid,
                                            kind=BinaryKind.WS_BINARY_DATA,
                                            payload=chunk,
                                            end_of_body=index + 1 == chunk_count,
                                        )
                                    )
                            else:
                                await send_frame(
                                    {
                                        "type": "ws_message",
                                        "id": rid,
                                        "data": base64.b64encode(
                                            upstream_message
                                        ).decode("ascii"),
                                        "binary": True,
                                    }
                                )

                    async def from_sandbox() -> None:
                        incoming_binary = bytearray()
                        while True:
                            channel_frame = await queue.get()
                            if isinstance(channel_frame, BinaryEnvelope):
                                if channel_frame.kind != BinaryKind.WS_BINARY_DATA:
                                    raise ProtocolError(
                                        "unexpected binary kind for WebSocket channel"
                                    )
                                if (
                                    len(incoming_binary) + len(channel_frame.payload)
                                    > negotiated_max_ws_message_size
                                ):
                                    raise ProtocolError(
                                        "WebSocket binary message exceeds tunnel limit"
                                    )
                                incoming_binary.extend(channel_frame.payload)
                                if channel_frame.end_of_body:
                                    await upstream_ws.send(bytes(incoming_binary))
                                    incoming_binary.clear()
                                continue
                            frame_type = channel_frame.get("type")
                            if frame_type == "ws_message":
                                data = channel_frame.get("data", "")
                                if channel_frame.get("binary", False):
                                    data = base64.b64decode(data, validate=True)
                                    ws_body_limit = (
                                        negotiated_max_ws_message_size
                                        if negotiated_protocol_version
                                        >= PROTOCOL_VERSION
                                        else min(
                                            negotiated_max_body_size,
                                            MAX_V1_BODY_BYTES,
                                        )
                                    )
                                    if len(data) > ws_body_limit:
                                        raise ProtocolError(
                                            "WebSocket binary message exceeds tunnel limit"
                                        )
                                await upstream_ws.send(data)
                            elif frame_type == "ws_close":
                                code = channel_frame.get("code", 1000)
                                reason = channel_frame.get("reason", "")
                                await upstream_ws.close(code=code, reason=reason)
                                return

                    upstream_task = asyncio.create_task(from_upstream())
                    sandbox_task = asyncio.create_task(from_sandbox())
                    relay_tasks = (upstream_task, sandbox_task)
                    try:
                        done, _ = await asyncio.wait(
                            relay_tasks,
                            return_when=asyncio.FIRST_COMPLETED,
                        )
                    finally:
                        for task in relay_tasks:
                            if not task.done():
                                task.cancel()
                        await asyncio.gather(*relay_tasks, return_exceptions=True)
                    for task in done:
                        task.result()

                    await send_frame(
                        {
                            "type": "ws_close",
                            "id": rid,
                            "code": upstream_ws.close_code or 1000,
                            "reason": upstream_ws.close_reason or "",
                        }
                    )
            except asyncio.CancelledError:
                raise
            except Exception as exc:
                logger.debug("tunnel ws_connect upstream error: %s", exc)
                try:
                    await send_frame(
                        {
                            "type": "error",
                            "id": rid,
                            "message": str(exc),
                        }
                    )
                except ws_exc.ConnectionClosed:
                    pass
            finally:
                if ws_channels.get(rid) is queue:
                    ws_channels.pop(rid, None)
                if ws_tasks.get(rid) is asyncio.current_task():
                    ws_tasks.pop(rid, None)
                remember_terminated(rid)

        tasks: set = resume_state["tasks"]
        http_tasks: set = resume_state["http_tasks"]
        heartbeat_task = asyncio.create_task(heartbeat_loop())
        try:
            async with self._borrow_http_client(http_ssl_context) as client:
                async for message in ws:
                    if self._stopping.is_set():
                        break
                    if isinstance(message, bytes):
                        envelope = BinaryEnvelope.decode(
                            message,
                            max_payload=negotiated_stream_chunk,
                        )
                        if envelope.kind == BinaryKind.WS_BINARY_DATA:
                            channel = ws_channels.get(envelope.request_id)
                            if channel is None:
                                if is_terminated(envelope.request_id):
                                    continue
                                raise ProtocolError(
                                    "WebSocket data for unknown channel: "
                                    f"{envelope.request_id}"
                                )
                            try:
                                channel.put_nowait(envelope)
                            except asyncio.QueueFull:
                                rid = envelope.request_id
                                task = ws_tasks.pop(rid, None)
                                if task is not None:
                                    task.cancel()
                                ws_channels.pop(rid, None)
                                remember_terminated(rid)
                                await send_frame(
                                    {
                                        "type": "error",
                                        "id": rid,
                                        "message": "WebSocket channel queue limit reached",
                                    }
                                )
                            continue
                        if envelope.kind != BinaryKind.HTTP_REQUEST_DATA:
                            raise ProtocolError(
                                f"unexpected binary frame kind: {envelope.kind.name}"
                            )
                        rid = envelope.request_id
                        request_state = stream_requests.get(rid)
                        if request_state is None:
                            if is_terminated(rid):
                                # The peer may still have an already-granted
                                # window after this stream failed. Absorb only
                                # bounded, recently terminated ids.
                                continue
                            raise ProtocolError(
                                f"request data for unknown stream: {rid}"
                            )
                        chunk_offset = envelope.offset
                        if chunk_offset is None:
                            raise ProtocolError(
                                f"resumable request chunk is missing offset: {rid}"
                            )
                        chunk_end = chunk_offset + len(envelope.payload)
                        if chunk_offset < request_state["received"]:
                            if chunk_end <= request_state["received"]:
                                await send_frame(
                                    {
                                        "type": "window",
                                        "id": rid,
                                        "credits": 0,
                                        "ack_offset": request_state["received"],
                                    }
                                )
                                continue
                            raise ProtocolError(
                                f"overlapping request chunk for stream: {rid}"
                            )
                        if chunk_offset > request_state["received"]:
                            raise ProtocolError(f"request chunk gap for stream: {rid}")
                        if request_state["ended"]:
                            continue
                        total = chunk_end
                        if total > negotiated_max_body_size:
                            await send_frame(
                                {
                                    "type": "error",
                                    "id": rid,
                                    "message": "request body exceeds tunnel limit",
                                }
                            )
                            request_state["task"].cancel()
                            stream_requests.pop(rid, None)
                            remember_terminated(rid)
                            continue
                        try:
                            request_state["queue"].put_nowait(envelope.payload)
                        except asyncio.QueueFull:
                            logger.warning(
                                "request stream window overflow id=%s offset=%d "
                                "received=%d queued=%d",
                                rid,
                                chunk_offset,
                                request_state["received"],
                                request_state["queue"].qsize(),
                            )
                            await send_frame(
                                {
                                    "type": "error",
                                    "id": rid,
                                    "message": "request stream exceeded advertised window",
                                }
                            )
                            request_state["task"].cancel()
                            stream_requests.pop(rid, None)
                            remember_terminated(rid)
                            continue
                        request_state["received"] = total
                        if envelope.end_of_body:
                            request_state["ended"] = True
                            await request_state["queue"].put(None)
                        continue
                    text = message
                    try:
                        frame = json.loads(text)
                    except (ValueError, TypeError):
                        continue
                    ftype = frame.get("type")
                    if ftype == "hello":
                        if negotiated_protocol_version >= PROTOCOL_VERSION:
                            logger.warning("TunnelClient ignored duplicate hello")
                            continue
                        peer_version = frame.get("protocol_version")
                        peer_chunk = frame.get("max_stream_chunk")
                        peer_inflight = frame.get("max_inflight")
                        peer_window = frame.get("stream_window_frames")
                        peer_max_body = frame.get("max_body_size", self._max_body_size)
                        peer_max_ws_message = frame.get(
                            "max_ws_message_size", self._max_ws_message_size
                        )
                        if (
                            self._protocol_version >= PROTOCOL_VERSION
                            and isinstance(peer_version, int)
                            and peer_version >= PROTOCOL_VERSION
                            and isinstance(peer_chunk, int)
                            and peer_chunk >= MIN_STREAM_CHUNK_BYTES
                            and isinstance(peer_inflight, int)
                            and peer_inflight > 0
                            and isinstance(peer_window, int)
                            and peer_window > 0
                            and isinstance(peer_max_body, int)
                            and peer_max_body > 0
                            and isinstance(peer_max_ws_message, int)
                            and peer_max_ws_message > 0
                            and frame.get("resume") is True
                        ):
                            negotiated_protocol_version = PROTOCOL_VERSION
                            negotiated_stream_chunk = min(
                                self._max_stream_chunk, peer_chunk
                            )
                            negotiated_max_inflight = min(
                                self._max_inflight, peer_inflight
                            )
                            negotiated_stream_window = min(
                                self._stream_window_frames, peer_window
                            )
                            negotiated_stream_window = min(
                                negotiated_stream_window,
                                max(
                                    1,
                                    (_OUTBOUND_QUEUE_FRAMES - _OUTBOUND_CONTROL_RESERVE)
                                    // negotiated_max_inflight,
                                ),
                            )
                            negotiated_max_body_size = min(
                                self._max_body_size, peer_max_body
                            )
                            negotiated_max_ws_message_size = min(
                                self._max_ws_message_size, peer_max_ws_message
                            )
                            logger.info(
                                "TunnelClient protocol v%d negotiated "
                                "chunk=%d inflight=%d window=%d body=%d ws_message=%d",
                                negotiated_protocol_version,
                                negotiated_stream_chunk,
                                negotiated_max_inflight,
                                negotiated_stream_window,
                                negotiated_max_body_size,
                                negotiated_max_ws_message_size,
                            )
                    elif ftype == "http_req":
                        rid = frame.get("id", "")
                        is_existing = rid in inflight or rid in completed
                        if (
                            not is_existing
                            and len(http_tasks) >= negotiated_max_inflight
                        ):
                            await send_frame(
                                {
                                    "type": "error",
                                    "id": rid,
                                    "message": "tunnel max_inflight limit reached",
                                }
                            )
                            continue
                        t = handle_http_req(client, frame)
                        tasks.add(t)
                        http_tasks.add(t)
                        t.add_done_callback(tasks.discard)
                        t.add_done_callback(http_tasks.discard)
                    elif ftype == "http_req_begin":
                        if negotiated_protocol_version < PROTOCOL_VERSION:
                            raise ProtocolError(
                                "received streaming request before V2 negotiation"
                            )
                        await handle_http_req_begin(client, frame)
                    elif ftype == "http_req_end":
                        rid = frame.get("id", "")
                        request_state = stream_requests.get(rid)
                        if request_state is None:
                            if is_terminated(rid):
                                continue
                            raise ProtocolError(
                                f"request end for unknown stream: {rid}"
                            )
                        if request_state["ended"]:
                            continue
                        expected = request_state["content_length"]
                        if (
                            expected is not None
                            and expected != request_state["received"]
                        ):
                            await send_frame(
                                {
                                    "type": "error",
                                    "id": rid,
                                    "message": "request content length mismatch",
                                }
                            )
                            request_state["task"].cancel()
                            stream_requests.pop(rid, None)
                            remember_terminated(rid)
                            continue
                        request_state["ended"] = True
                        await request_state["queue"].put(None)
                    elif ftype == "window":
                        rid = frame.get("id", "")
                        credits = frame.get("credits")
                        ack_offset = frame.get("ack_offset")
                        if (
                            not isinstance(credits, int)
                            or credits < 0
                            or (credits == 0 and not isinstance(ack_offset, int))
                            or (
                                ack_offset is not None
                                and (not isinstance(ack_offset, int) or ack_offset < 0)
                            )
                        ):
                            raise ProtocolError(
                                "window must carry credits or a valid ack_offset"
                            )
                        response_state = response_credits.get(rid)
                        if response_state is None:
                            continue
                        if isinstance(ack_offset, int):
                            response_state["unacked"] = [
                                (offset, payload)
                                for offset, payload in response_state["unacked"]
                                if offset + len(payload) > ack_offset
                            ]
                        if frame.get("complete") is True:
                            response_credits.pop(rid, None)
                            continue
                        credit_queue = response_state["credits"]
                        for _ in range(min(credits, negotiated_stream_window)):
                            try:
                                credit_queue.put_nowait(None)
                            except asyncio.QueueFull:
                                break
                        if not response_state["ready"].is_set():
                            async with response_state["send_lock"]:
                                for offset, payload in response_state["unacked"]:
                                    await credit_queue.get()
                                    await send_binary(
                                        BinaryEnvelope(
                                            request_id=rid,
                                            kind=BinaryKind.HTTP_RESPONSE_DATA,
                                            payload=payload,
                                            offset=offset,
                                        )
                                    )
                                if response_state["ended"]:
                                    await send_frame(
                                        {"type": "http_resp_end", "id": rid}
                                    )
                            response_state["ready"].set()
                    elif ftype == "error":
                        rid = frame.get("id", "")
                        response_credits.pop(rid, None)
                        request_state = stream_requests.pop(rid, None)
                        if request_state is not None:
                            request_state["task"].cancel()
                            remember_terminated(rid)
                            continue
                        task = inflight.get(rid)
                        if task is not None:
                            task.cancel()
                            continue
                        task = ws_tasks.pop(rid, None)
                        if task is not None:
                            task.cancel()
                            ws_channels.pop(rid, None)
                            remember_terminated(rid)
                    elif ftype == "ws_connect":
                        rid = frame.get("id", "")
                        if not rid:
                            continue
                        if rid in ws_channels or is_terminated(rid):
                            await send_frame(
                                {
                                    "type": "error",
                                    "id": rid,
                                    "message": "duplicate WebSocket channel id",
                                }
                            )
                            continue
                        if len(ws_channels) >= negotiated_max_inflight:
                            await send_frame(
                                {
                                    "type": "error",
                                    "id": rid,
                                    "message": "tunnel WebSocket channel limit reached",
                                }
                            )
                            continue
                        ws_message_frames = (
                            negotiated_max_ws_message_size + negotiated_stream_chunk - 1
                        ) // negotiated_stream_chunk
                        channel_limit = (
                            ws_message_frames + 2
                            if negotiated_protocol_version >= PROTOCOL_VERSION
                            else _WS_CHANNEL_QUEUE_LIMIT
                        )
                        channel: asyncio.Queue = asyncio.Queue(maxsize=channel_limit)
                        ws_channels[rid] = channel
                        t = asyncio.create_task(handle_ws_connect(frame, channel))
                        ws_tasks[rid] = t
                        tasks.add(t)
                        t.add_done_callback(tasks.discard)
                    elif ftype in ("ws_message", "ws_close"):
                        rid = frame.get("id", "")
                        channel = ws_channels.get(rid)
                        if channel is not None:
                            try:
                                channel.put_nowait(frame)
                            except asyncio.QueueFull:
                                task = ws_tasks.pop(rid, None)
                                if task is not None:
                                    task.cancel()
                                ws_channels.pop(rid, None)
                                remember_terminated(rid)
                                await send_frame(
                                    {
                                        "type": "error",
                                        "id": rid,
                                        "message": "WebSocket channel queue limit reached",
                                    }
                                )
                    elif ftype == "ping":
                        await send_frame(
                            {
                                "type": "pong",
                                "id": frame.get("id", ""),
                                "timestamp": frame.get("timestamp", 0),
                            }
                        )
                    elif (
                        ftype == "pong"
                        and pending_ping_id is not None
                        and frame.get("id") == pending_ping_id
                    ):
                        pong_received.set()
        except ws_exc.ConnectionClosed:
            pass
        finally:
            heartbeat_task.cancel()
            await asyncio.gather(heartbeat_task, return_exceptions=True)
            preserve_http = not self._stopping.is_set()
            cancel_tasks = tasks.difference(http_tasks) if preserve_http else set(tasks)
            for task in list(cancel_tasks):
                task.cancel()
            if cancel_tasks:
                await asyncio.gather(*cancel_tasks, return_exceptions=True)
            ws_channels.clear()
            if not preserve_http:
                stream_requests.clear()
                response_credits.clear()
        if heartbeat_timed_out.is_set():
            raise asyncio.TimeoutError("TunnelClient application heartbeat timed out")
