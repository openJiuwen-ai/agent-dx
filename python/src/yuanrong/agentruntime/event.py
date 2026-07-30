"""EventLog public model."""

from dataclasses import dataclass
from datetime import datetime
from typing import Any, Literal


@dataclass(frozen=True)
class Event:
    session_context_id: str
    turn_id: str
    seq: int
    event_id: str
    source: Literal["PLATFORM", "SDK"]
    type: str
    data: Any
    schema_version: int
    created_at: datetime
