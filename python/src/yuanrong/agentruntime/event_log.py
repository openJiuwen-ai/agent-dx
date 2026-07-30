"""Public EventLog facade."""

from __future__ import annotations

from typing import TYPE_CHECKING, Any, List, Optional

from .errors import EventAppendNotActive
from .event import Event

if TYPE_CHECKING:
    from .context import RequestContext
    from .turn_writer import TurnWriter


class EventLog:
    def __init__(self, writer: "TurnWriter"):
        self._writer = writer
        self._active_request: Optional["RequestContext"] = None
        self._active_turn_id: Optional[str] = None

    async def get(self, *, after_seq: int = 0, limit: Optional[int] = None) -> List[Event]:
        if after_seq < 0:
            raise ValueError("after_seq must be non-negative")
        if limit is not None and limit < 0:
            raise ValueError("limit must be non-negative")
        return await self._writer.get_events(after_seq=after_seq, limit=limit)

    async def append(
        self,
        request_context: "RequestContext",
        event_type: str,
        data: Any,
    ) -> Event:
        if (
            request_context is not self._active_request
            or not request_context.is_active
            or self._active_turn_id is None
        ):
            raise EventAppendNotActive()
        if not event_type or event_type.startswith(
            ("turn.", "input.", "output.", "session.", "runtime.")
        ):
            raise ValueError("event_type is empty or uses a platform-reserved prefix")
        return await self._writer.append_sdk(self._active_turn_id, event_type, data)

    def _activate(self, request_context: "RequestContext", turn_id: str) -> None:
        self._active_request = request_context
        self._active_turn_id = turn_id

    def _deactivate(self, request_context: "RequestContext") -> None:
        if request_context is self._active_request:
            self._active_request = None
            self._active_turn_id = None
