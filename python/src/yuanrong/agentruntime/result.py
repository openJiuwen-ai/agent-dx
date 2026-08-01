"""Execution results returned by user Agents."""

from dataclasses import dataclass
from typing import Any, Union


@dataclass(frozen=True)
class Complete:
    value: Any


@dataclass(frozen=True)
class InputRequired:
    value: Any


ExecutionResult = Union[Complete, InputRequired]
