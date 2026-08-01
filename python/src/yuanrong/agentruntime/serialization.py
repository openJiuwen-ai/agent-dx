"""Stable JSON serialization used by EventLog and SSE output."""

import json
from typing import Any

from .errors import EventSerializationFailed


def to_json_bytes(value: Any) -> bytes:
    try:
        return json.dumps(
            value,
            ensure_ascii=False,
            separators=(",", ":"),
            sort_keys=True,
        ).encode("utf-8")
    except (TypeError, ValueError, OverflowError) as exc:
        raise EventSerializationFailed(str(exc)) from exc


def from_json_bytes(value: bytes) -> Any:
    try:
        return json.loads(value.decode("utf-8"))
    except (UnicodeDecodeError, json.JSONDecodeError) as exc:
        raise EventSerializationFailed(f"stored EventLog data is invalid: {exc}") from exc


def to_stream_value(value: Any) -> str:
    if isinstance(value, str):
        return value
    return to_json_bytes(value).decode("utf-8")
