"""Versioned wire primitives for the sandbox reverse tunnel.

Control messages remain JSON. HTTP body chunks and WebSocket binary payloads
use the compact binary envelope defined by the streaming tunnel RFC.
"""

from __future__ import annotations

import struct
import uuid
from dataclasses import dataclass
from enum import IntEnum

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
_END_OF_BODY = 0x01
_HAS_OFFSET = 0x02
_UUID_BYTES = 16
_HEADER = struct.Struct("!2sBBB16sBI")


class ProtocolError(ValueError):
    """Raised when a tunnel frame violates the negotiated protocol."""


class BinaryKind(IntEnum):
    HTTP_REQUEST_DATA = 0x01
    HTTP_RESPONSE_DATA = 0x02
    WS_BINARY_DATA = 0x03


@dataclass(frozen=True)
class BinaryEnvelope:
    request_id: str
    kind: BinaryKind
    payload: bytes
    end_of_body: bool = False
    offset: int | None = None

    def encode(self, max_payload: int = DEFAULT_STREAM_CHUNK_BYTES) -> bytes:
        if len(self.payload) > max_payload:
            raise ProtocolError(
                f"binary payload exceeds negotiated chunk limit: "
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
            _HEADER.pack(
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
        if len(raw) < _HEADER.size:
            raise ProtocolError("binary envelope is shorter than its header")
        magic, version, kind, id_length, raw_id, flags, payload_length = (
            _HEADER.unpack_from(raw)
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
                f"binary payload exceeds negotiated chunk limit: "
                f"{payload_length} > {wire_limit}"
            )
        wire_payload = raw[_HEADER.size :]
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
    session_id: str | None = None,
) -> dict:
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
