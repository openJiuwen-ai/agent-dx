#!/usr/bin/env python3
# coding=UTF-8
# Copyright (c) Huawei Technologies Co., Ltd. 2026. All rights reserved.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
# http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

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
