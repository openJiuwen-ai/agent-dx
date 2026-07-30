"""User Agent interface."""

from abc import ABC, abstractmethod
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from .context import RequestContext, SessionContext
    from .result import ExecutionResult


class AgentExecutor(ABC):
    @abstractmethod
    async def init(self, session_context: "SessionContext") -> None:
        """Initialize or recover Agent business state."""

    @abstractmethod
    async def execute(self, request_context: "RequestContext") -> "ExecutionResult":
        """Handle one invocation in the current Turn."""
