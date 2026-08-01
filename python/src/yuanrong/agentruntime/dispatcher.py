"""One-at-a-time invocation dispatcher."""

from __future__ import annotations

import logging
from typing import Any

from .context import RequestContext, SessionContext
from .errors import AgentRuntimeError, InvalidExecutionResult
from .executor import AgentExecutor
from .output import OutputWriter
from .result import Complete, InputRequired
from .turn_writer import TurnWriter

_LOG = logging.getLogger(__name__)


class Dispatcher:
    def __init__(
        self,
        executor: AgentExecutor,
        session_context: SessionContext,
        writer: TurnWriter,
    ):
        self._executor = executor
        self._session_context = session_context
        self._writer = writer

    async def dispatch(self, message: Any, stream: object) -> Any:
        turn_id = await self._writer.accept_input(message)
        output = OutputWriter(self._writer, stream)
        request = RequestContext(self._session_context, turn_id, message, output)
        output._bind(request, turn_id)
        event_log = self._session_context.event_log
        event_log._activate(request, turn_id)

        try:
            try:
                result = await self._executor.execute(request)
            finally:
                request._deactivate()
                event_log._deactivate(request)
            if not isinstance(result, (Complete, InputRequired)):
                raise InvalidExecutionResult()

            if isinstance(result, Complete):
                await self._writer.append_platform(
                    turn_id,
                    "turn.completed",
                    {"output": result.value},
                )
            else:
                await self._writer.append_platform(
                    turn_id,
                    "turn.input_required",
                    {"output": result.value},
                )
            return result.value
        except Exception as exc:
            if self._writer.state_for(turn_id) not in ("COMPLETED", "FAILED"):
                code = exc.code if isinstance(exc, AgentRuntimeError) else "AGENT_EXECUTION_FAILED"
                await self._writer.append_platform(
                    turn_id,
                    "turn.failed",
                    {"error": {"code": code, "message": "Agent execution failed"}},
                )
            if isinstance(exc, AgentRuntimeError):
                raise
            _LOG.exception("Agent execute() failed")
            raise AgentRuntimeError(
                "AGENT_EXECUTION_FAILED",
                "Agent execution failed; see instance logs",
            ) from exc
