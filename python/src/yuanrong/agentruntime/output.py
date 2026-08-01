"""Persist-before-send streaming output."""

from __future__ import annotations

from typing import TYPE_CHECKING, Any, Optional

from .errors import OutputNotActive
from .serialization import to_stream_value

if TYPE_CHECKING:
    from .context import RequestContext
    from .turn_writer import TurnWriter


class OutputWriter:
    def __init__(self, writer: "TurnWriter", stream: object):
        self._writer = writer
        self._stream = stream
        self._request_context: Optional["RequestContext"] = None
        self._turn_id: Optional[str] = None

    def _bind(self, request_context: "RequestContext", turn_id: str) -> None:
        self._request_context = request_context
        self._turn_id = turn_id

    async def write(self, value: Any) -> None:
        context = self._request_context
        if context is None or not context.is_active or self._turn_id is None:
            raise OutputNotActive()
        serialized = to_stream_value(value)
        await self._writer.append_platform(
            self._turn_id,
            "output.message",
            {"message": value},
        )
        self._stream.write(serialized)
