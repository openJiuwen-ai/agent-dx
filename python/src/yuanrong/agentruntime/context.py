"""Agent-facing Session and Request contexts."""

from __future__ import annotations

from dataclasses import dataclass
from typing import TYPE_CHECKING, Any

if TYPE_CHECKING:
    from .event_log import EventLog
    from .output import OutputWriter


@dataclass(frozen=True)
class RequestInput:
    message: Any


class SessionContext:
    def __init__(self, session_context_id: str, event_log: "EventLog"):
        self._id = session_context_id
        self._event_log = event_log

    @property
    def id(self) -> str:
        return self._id

    @property
    def event_log(self) -> "EventLog":
        return self._event_log


class RequestContext:
    __slots__ = (
        "_session_context",
        "__turn_id",
        "_input",
        "_output",
        "_active",
    )

    def __init__(
        self,
        session_context: SessionContext,
        turn_id: str,
        message: Any,
        output: "OutputWriter",
    ):
        self._session_context = session_context
        self.__turn_id = turn_id
        self._input = RequestInput(message)
        self._output = output
        self._active = True

    @property
    def session_context(self) -> SessionContext:
        return self._session_context

    @property
    def turn_id(self) -> str:
        return self.__turn_id

    @property
    def input(self) -> RequestInput:
        return self._input

    @property
    def output(self) -> "OutputWriter":
        return self._output

    @property
    def is_active(self) -> bool:
        return self._active

    def _deactivate(self) -> None:
        self._active = False
