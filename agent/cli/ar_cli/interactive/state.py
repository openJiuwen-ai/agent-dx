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

"""State and result types for one interactive adx exec session."""

from dataclasses import dataclass
from enum import Enum
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from ar_cli.api.client import AgentRuntimeClient


@dataclass
class InteractiveSessionState:
    """The locally selected SessionCtx for one interactive run."""

    server_url: str
    agent_urn: str
    session_ctx: str


@dataclass
class InteractiveContext:
    """Dependencies and mutable state passed to slash-command handlers."""

    state: InteractiveSessionState
    client: "AgentRuntimeClient"


class CommandResult(Enum):
    """Control signal returned by a slash-command handler."""

    CONTINUE = "continue"
    EXIT = "exit"


class InteractiveCommandError(Exception):
    """User-facing slash-command syntax or response errors."""
