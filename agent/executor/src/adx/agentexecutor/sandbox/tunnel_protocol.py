# api/python/adx/sandbox/tunnel_protocol.py
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
"""Wire protocol frames for the sandbox reverse tunnel."""

import base64
import json
import os
import re
import struct
import uuid
from dataclasses import dataclass, field
from enum import IntEnum
from typing import Dict, List, Optional, Tuple

_HTTP_METHOD_RE = re.compile(r"^[A-Z]+$")
PROTOCOL_VERSION = 2
BINARY_ENVELOPE_VERSION = 1
BINARY_MAGIC = b"YD"
DEFAULT_STREAM_CHUNK_BYTES = 64 * 1024
MIN_STREAM_CHUNK_BYTES = 1024
DEFAULT_FAST_PATH_BODY_BYTES = 64 * 1024
DEFAULT_MAX_INFLIGHT = 16
DEFAULT_STREAM_WINDOW_FRAMES = 16
DEFAULT_MAX_BODY_BYTES = 512 * 1024 * 1024
DEFAULT_MAX_WS_MESSAGE_BYTES = 8 * 1024 * 1024
MAX_V1_BODY_BYTES = 5 * 1024 * 1024
_DEFAULT_MAX_BODY_SIZE = DEFAULT_MAX_BODY_BYTES
_DEFAULT_MAX_FRAME_SIZE = 8 * 1024 * 1024
_CRLF_RE = re.compile(r"[\r\n]")
_PATH_TRAVERSAL_RE = re.compile(r"(?:^|/)\.\.(?:/|$)")
_HEADER_NAME_RE = re.compile(r"^[!#$%&'*+\-.^_`|~0-9A-Za-z]+$")
_END_OF_BODY = 0x01
_HAS_OFFSET = 0x02
_UUID_BYTES = 16
_BINARY_HEADER = struct.Struct("!2sBBB16sBI")

HeaderItems = List[Tuple[str, str]]
HOP_BY_HOP_HEADERS = frozenset(
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


def _read_size_env(name: str, default: int) -> int:
    try:
        value = int(os.environ.get(name, str(default)))
    except ValueError:
        return default
    return value if value > 0 else default


MAX_TUNNEL_BODY_SIZE = _read_size_env("ADX_TUNNEL_MAX_BODY_SIZE", _DEFAULT_MAX_BODY_SIZE)
MAX_TUNNEL_FRAME_SIZE = _read_size_env(
    "ADX_TUNNEL_MAX_FRAME_SIZE", _DEFAULT_MAX_FRAME_SIZE
)
_MAX_BODY_SIZE = MAX_TUNNEL_BODY_SIZE
_MAX_FRAME_SIZE = MAX_TUNNEL_FRAME_SIZE


class ProtocolError(ValueError):
    """Raised when a tunnel frame violates the negotiated protocol."""


class BinaryKind(IntEnum):
    HTTP_REQUEST_DATA = 0x01
    HTTP_RESPONSE_DATA = 0x02
    WS_BINARY_DATA = 0x03


@dataclass(frozen=True)
class BinaryEnvelope:
    """Compact V2 envelope used for bounded binary body chunks."""

    request_id: str
    kind: BinaryKind
    payload: bytes
    end_of_body: bool = False
    offset: Optional[int] = None

    def encode(self, max_payload: int = DEFAULT_STREAM_CHUNK_BYTES) -> bytes:
        if len(self.payload) > max_payload:
            raise ProtocolError(
                "binary payload exceeds negotiated chunk limit: "
                f"{len(self.payload)} > {max_payload}"
            )
        try:
            request_uuid = uuid.UUID(self.request_id)
        except (ValueError, AttributeError) as exc:
            raise ProtocolError("binary envelope id must be a UUID") from exc
        if self.offset is not None and self.offset < 0:
            raise ProtocolError("binary envelope offset must not be negative")
        flags = _END_OF_BODY if self.end_of_body else 0
        wire_payload = self.payload
        if self.offset is not None:
            flags |= _HAS_OFFSET
            wire_payload = struct.pack("!Q", self.offset) + wire_payload
        return (
            _BINARY_HEADER.pack(
                BINARY_MAGIC,
                BINARY_ENVELOPE_VERSION,
                int(self.kind),
                _UUID_BYTES,
                request_uuid.bytes,
                flags,
                len(wire_payload),
            )
            + wire_payload
        )

    @classmethod
    def decode(
        cls,
        raw: bytes,
        max_payload: int = DEFAULT_STREAM_CHUNK_BYTES,
    ) -> "BinaryEnvelope":
        if len(raw) < _BINARY_HEADER.size:
            raise ProtocolError("binary envelope is shorter than its header")
        magic, version, kind, id_length, raw_id, flags, payload_length = (
            _BINARY_HEADER.unpack_from(raw)
        )
        if magic != BINARY_MAGIC:
            raise ProtocolError("invalid binary envelope magic")
        if version != BINARY_ENVELOPE_VERSION:
            raise ProtocolError(f"unsupported binary envelope version: {version}")
        if id_length != _UUID_BYTES:
            raise ProtocolError(f"invalid binary envelope UUID length: {id_length}")
        try:
            binary_kind = BinaryKind(kind)
        except ValueError as exc:
            raise ProtocolError(f"unknown binary envelope kind: {kind}") from exc
        if flags & ~(_END_OF_BODY | _HAS_OFFSET):
            raise ProtocolError(f"unknown binary envelope flags: {flags:#x}")
        wire_limit = max_payload + (8 if flags & _HAS_OFFSET else 0)
        if payload_length > wire_limit:
            raise ProtocolError(
                "binary payload exceeds negotiated chunk limit: "
                f"{payload_length} > {wire_limit}"
            )
        wire_payload = raw[_BINARY_HEADER.size :]
        if len(wire_payload) != payload_length:
            raise ProtocolError(
                f"binary payload length mismatch: {len(wire_payload)} != {payload_length}"
            )
        offset = None
        payload = wire_payload
        if flags & _HAS_OFFSET:
            if len(wire_payload) < 8:
                raise ProtocolError("offset binary envelope payload is too short")
            offset = struct.unpack("!Q", wire_payload[:8])[0]
            payload = wire_payload[8:]
        if len(payload) > max_payload:
            raise ProtocolError(
                "binary payload exceeds negotiated chunk limit: "
                f"{len(payload)} > {max_payload}"
            )
        return cls(
            request_id=str(uuid.UUID(bytes=raw_id)),
            kind=binary_kind,
            payload=payload,
            end_of_body=bool(flags & _END_OF_BODY),
            offset=offset,
        )


def hello_frame(
    *,
    protocol_version: int = PROTOCOL_VERSION,
    max_stream_chunk: int = DEFAULT_STREAM_CHUNK_BYTES,
    max_inflight: int = DEFAULT_MAX_INFLIGHT,
    stream_window_frames: int = DEFAULT_STREAM_WINDOW_FRAMES,
    max_body_size: int = DEFAULT_MAX_BODY_BYTES,
    max_ws_message_size: int = DEFAULT_MAX_WS_MESSAGE_BYTES,
    resumable: bool = True,
    session_id: Optional[str] = None,
) -> dict:
    """Build and validate the V2 capability advertisement."""
    if protocol_version <= 0:
        raise ValueError("protocol_version must be greater than zero")
    if max_stream_chunk < MIN_STREAM_CHUNK_BYTES:
        raise ValueError("max_stream_chunk must be at least 1024 bytes")
    if max_inflight <= 0:
        raise ValueError("max_inflight must be greater than zero")
    if stream_window_frames <= 0:
        raise ValueError("stream_window_frames must be greater than zero")
    if max_body_size <= 0:
        raise ValueError("max_body_size must be greater than zero")
    if max_ws_message_size <= 0:
        raise ValueError("max_ws_message_size must be greater than zero")
    if resumable and session_id is not None:
        try:
            session_id = str(uuid.UUID(session_id))
        except (ValueError, AttributeError) as exc:
            raise ValueError("resumable hello requires a UUID session_id") from exc
    frame = {
        "type": "hello",
        "protocol_version": protocol_version,
        "max_stream_chunk": max_stream_chunk,
        "max_inflight": max_inflight,
        "stream_window_frames": stream_window_frames,
        "max_body_size": max_body_size,
        "max_ws_message_size": max_ws_message_size,
    }
    if resumable:
        frame["resume"] = True
        if session_id is not None:
            frame["session_id"] = session_id
    return frame


def _validate_path(path: str) -> str:
    if not isinstance(path, str):
        raise ValueError("path must be a string")
    if not path.startswith("/") or path.startswith("//"):
        raise ValueError(f"HTTP request target must use origin-form: {path!r}")
    if _PATH_TRAVERSAL_RE.search(path):
        raise ValueError(f"Path traversal not allowed: {path!r}")
    return path


def _validate_headers(headers: Dict[str, str]) -> Dict[str, str]:
    if not isinstance(headers, dict):
        raise ValueError("headers must be an object")
    for k, v in headers.items():
        if not isinstance(k, str) or not _HEADER_NAME_RE.fullmatch(k):
            raise ValueError(f"Invalid header name: {k!r}")
        if not isinstance(v, str):
            raise ValueError(f"Header value for {k!r} must be a string")
        if _CRLF_RE.search(v):
            raise ValueError(f"CRLF in header value for key {k!r}")
    return headers


def _validate_header_items(header_items) -> HeaderItems:
    if not isinstance(header_items, list):
        raise ValueError("header_items must be an array")
    result = []
    for item in header_items:
        if not isinstance(item, (list, tuple)) or len(item) != 2:
            raise ValueError("header_items entries must be [name, value] pairs")
        name, value = item
        _validate_headers({name: value})
        result.append((name, value))
    return result


def header_items_to_legacy_headers(header_items: HeaderItems) -> Dict[str, str]:
    """Return a deterministic last-value map for legacy tunnel peers."""
    result = {}
    names_by_lower = {}
    for name, value in _validate_header_items(list(header_items)):
        lower = name.lower()
        previous = names_by_lower.get(lower)
        if previous is not None:
            result.pop(previous, None)
        result[name] = value
        names_by_lower[lower] = name
    return result


def _http_headers_from_frame(data: dict) -> Tuple[Dict[str, str], HeaderItems]:
    legacy = _validate_headers(data.get("headers", {}))
    if "header_items" in data:
        items = _validate_header_items(data["header_items"])
    else:
        items = list(legacy.items())
    return header_items_to_legacy_headers(items), items


def _is_valid_window_frame(credit_count, ack_offset) -> bool:
    """Validate a flow-control window frame's credits and ack_offset.

    Replaces a compound boolean ``if`` flagged by G.CTL.03
    (too-many-boolean-expressions) with a plain early-return guard.
    A window frame must either grant credits (non-negative int) or carry a
    valid acknowledgement offset (non-negative int, optional when credits > 0).
    """
    if not isinstance(credit_count, int) or credit_count < 0:
        return False
    if credit_count == 0 and not isinstance(ack_offset, int):
        return False
    if ack_offset is not None:
        if not isinstance(ack_offset, int) or ack_offset < 0:
            return False
    return True


def connection_tokens_from_header_items(header_items: HeaderItems) -> set:
    """Return lowercased Connection-header tokens from a header item list.

    Replaces a multi-clause set comprehension flagged by G.EXP.04
    (complicate-comprehension) with an equivalent plain loop.
    """
    tokens = set()
    for name, value in header_items:
        if name.lower() != "connection":
            continue
        for token in value.split(","):
            token = token.strip().lower()
            if token:
                tokens.add(token)
    return tokens


def filter_hop_by_hop_header_items(
    header_items: HeaderItems,
    excluded=(),
) -> HeaderItems:
    """Remove fixed and Connection-nominated fields from a header list."""
    items = _validate_header_items(list(header_items))
    connection_tokens = connection_tokens_from_header_items(items)
    blocked = (
        HOP_BY_HOP_HEADERS | connection_tokens | {name.lower() for name in excluded}
    )
    return [(name, value) for name, value in items if name.lower() not in blocked]


def rebuilt_request_header_items(
    header_items: HeaderItems,
    body_length: int,
) -> HeaderItems:
    """Build second-hop request headers from decoded first-hop bytes."""
    result = filter_hop_by_hop_header_items(
        header_items,
        excluded=("host", "content-length", "expect"),
    )
    result.append(("Content-Length", str(body_length)))
    return result


def rebuilt_response_header_items(
    header_items: HeaderItems,
    method: str,
    status: int,
    body_length: int,
) -> HeaderItems:
    """Build downstream response headers from actual response bytes."""
    items = _validate_header_items(list(header_items))
    content_lengths = [
        value.strip() for name, value in items if name.lower() == "content-length"
    ]
    representation_length = None
    if content_lengths and len(set(content_lengths)) == 1:
        value = content_lengths[0]
        if value.isdigit():
            representation_length = value

    result = filter_hop_by_hop_header_items(
        items,
        excluded=("content-length",),
    )
    method = method.upper()
    if method == "HEAD" or status == 304:
        if representation_length is not None:
            result.append(("Content-Length", representation_length))
    elif not (100 <= status < 200) and status != 204:
        result.append(("Content-Length", str(body_length)))
    return result


def make_id() -> str:
    """Generate a unique frame ID."""
    return str(uuid.uuid4())


def _decode_body(data: dict) -> bytes:
    body = data.get("body")
    if body in (None, ""):
        return b""
    if not isinstance(body, str):
        raise ValueError("body must be a base64 string or null")
    decoded = base64.b64decode(body, validate=True)
    v1_limit = min(_MAX_BODY_SIZE, MAX_V1_BODY_BYTES)
    if len(decoded) > v1_limit:
        raise ValueError(f"Body exceeds {v1_limit} bytes V1 limit")
    return decoded


def _validate_http_method(method: str) -> str:
    if not isinstance(method, str) or not _HTTP_METHOD_RE.fullmatch(method):
        raise ValueError(f"Invalid HTTP method: {method!r}")
    return method


def _validate_http_status(status: int) -> int:
    if not isinstance(status, int) or not (100 <= status <= 599):
        raise ValueError(f"Invalid HTTP status: {status!r}")
    return status


def _validate_ws_close_code(code: int) -> int:
    if not isinstance(code, int) or not (1000 <= code <= 4999):
        raise ValueError(f"Invalid WebSocket close code: {code!r}")
    return code


@dataclass
class HttpReqFrame:
    id: str
    method: str
    path: str
    headers: Dict[str, str]
    body: bytes
    header_items: Optional[HeaderItems] = None
    type: str = field(default="http_req", init=False)

    def __post_init__(self):
        items = (
            list(_validate_headers(self.headers).items())
            if self.header_items is None
            else _validate_header_items(self.header_items)
        )
        self.header_items = items
        self.headers = header_items_to_legacy_headers(items)

    def to_json(self) -> str:
        return json.dumps(
            {
                "type": self.type,
                "id": self.id,
                "method": self.method,
                "path": self.path,
                "headers": self.headers,
                "header_items": self.header_items,
                "body": base64.b64encode(self.body).decode(),
            }
        )


@dataclass
class HttpRespFrame:
    id: str
    status: int
    headers: Dict[str, str]
    body: bytes
    header_items: Optional[HeaderItems] = None
    type: str = field(default="http_resp", init=False)

    def __post_init__(self):
        items = (
            list(_validate_headers(self.headers).items())
            if self.header_items is None
            else _validate_header_items(self.header_items)
        )
        self.header_items = items
        self.headers = header_items_to_legacy_headers(items)

    def to_json(self) -> str:
        return json.dumps(
            {
                "type": self.type,
                "id": self.id,
                "status": self.status,
                "headers": self.headers,
                "header_items": self.header_items,
                "body": base64.b64encode(self.body).decode(),
            }
        )


@dataclass
class WsConnectFrame:
    id: str
    path: str
    headers: Dict[str, str]
    type: str = field(default="ws_connect", init=False)

    def to_json(self) -> str:
        return json.dumps(
            {
                "type": self.type,
                "id": self.id,
                "path": self.path,
                "headers": self.headers,
            }
        )


@dataclass
class WsConnectedFrame:
    id: str
    type: str = field(default="ws_connected", init=False)

    def to_json(self) -> str:
        return json.dumps({"type": self.type, "id": self.id})


@dataclass
class WsMessageFrame:
    id: str
    data: str
    binary: bool = False
    type: str = field(default="ws_message", init=False)

    def to_json(self) -> str:
        return json.dumps(
            {"type": self.type, "id": self.id, "data": self.data, "binary": self.binary}
        )


@dataclass
class WsCloseFrame:
    id: str
    code: int = 1000
    reason: str = ""
    type: str = field(default="ws_close", init=False)

    def to_json(self) -> str:
        return json.dumps(
            {"type": self.type, "id": self.id, "code": self.code, "reason": self.reason}
        )


@dataclass
class ErrorFrame:
    id: str
    message: str
    type: str = field(default="error", init=False)

    def to_json(self) -> str:
        return json.dumps({"type": self.type, "id": self.id, "message": self.message})


@dataclass
class PingFrame:
    id: str
    timestamp: float
    type: str = field(default="ping", init=False)

    def to_json(self) -> str:
        return json.dumps(
            {"type": self.type, "id": self.id, "timestamp": self.timestamp}
        )


@dataclass
class PongFrame:
    id: str
    timestamp: float
    type: str = field(default="pong", init=False)

    def to_json(self) -> str:
        return json.dumps(
            {"type": self.type, "id": self.id, "timestamp": self.timestamp}
        )


def parse_frame(raw: str):
    """Parse a JSON frame string into the appropriate frame dataclass."""
    if len(raw) > _MAX_FRAME_SIZE:
        raise ValueError(f"Frame exceeds {_MAX_FRAME_SIZE} bytes limit")
    data = json.loads(raw)
    if not isinstance(data, dict):
        raise ValueError("Frame must be a JSON object")
    t = data.get("type")
    if t == "http_req":
        headers, header_items = _http_headers_from_frame(data)
        return HttpReqFrame(
            id=data["id"],
            method=_validate_http_method(data["method"]),
            path=_validate_path(data["path"]),
            headers=headers,
            header_items=header_items,
            body=_decode_body(data),
        )
    if t == "http_resp":
        headers, header_items = _http_headers_from_frame(data)
        return HttpRespFrame(
            id=data["id"],
            status=_validate_http_status(data["status"]),
            headers=headers,
            header_items=header_items,
            body=_decode_body(data),
        )
    if t == "ws_connect":
        return WsConnectFrame(
            id=data["id"],
            path=_validate_path(data["path"]),
            headers=_validate_headers(data.get("headers", {})),
        )
    if t == "ws_connected":
        return WsConnectedFrame(id=data["id"])
    if t == "ws_message":
        return WsMessageFrame(
            id=data["id"], data=data["data"], binary=data.get("binary", False)
        )
    if t == "ws_close":
        return WsCloseFrame(
            id=data["id"],
            code=_validate_ws_close_code(data.get("code", 1000)),
            reason=data.get("reason", ""),
        )
    if t == "error":
        return ErrorFrame(id=data["id"], message=data["message"])
    if t == "ping":
        return PingFrame(id=data["id"], timestamp=data["timestamp"])
    if t == "pong":
        return PongFrame(id=data["id"], timestamp=data["timestamp"])
    raise ValueError(f"Unknown frame type: {t!r}")
