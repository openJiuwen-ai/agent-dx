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
