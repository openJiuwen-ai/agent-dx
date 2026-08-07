"""Public agent-dx SDK."""

from importlib.metadata import PackageNotFoundError, version
from pathlib import Path

from .context import RequestContext, RequestInput, SessionContext
from .event import Event
from .event_log import EventLog
from .executor import AgentExecutor
from .output import OutputWriter
from .result import Complete, ExecutionResult, InputRequired


def _version() -> str:
    try:
        return version("agent-dx-sdk")
    except PackageNotFoundError:
        return (Path(__file__).resolve().parents[4] / "VERSION").read_text(encoding="utf-8").strip()


__version__ = _version()

__all__ = [
    "AgentExecutor",
    "Complete",
    "Event",
    "EventLog",
    "ExecutionResult",
    "InputRequired",
    "OutputWriter",
    "RequestContext",
    "RequestInput",
    "SessionContext",
    "__version__",
]
