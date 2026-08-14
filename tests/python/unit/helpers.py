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

import asyncio
from typing import Any, List

from yuanrong.agentruntime import AgentExecutor, Complete


class FakeStream:
    def __init__(self, operations=None):
        self.values: List[str] = []
        self.operations = operations

    def write(self, value: str) -> None:
        self.values.append(value)
        if self.operations is not None:
            self.operations.append(("stream", value))


class FakeFunctionContext:
    def __init__(self, stream=None):
        self.stream = FakeStream() if stream is None else stream

    @staticmethod
    def getTenantID():
        return "tenant"

    @staticmethod
    def getFunctionName():
        return "agent"

    @staticmethod
    def getVersion():
        return "v1"

    def get_stream(self):
        return self.stream


class SequenceAgent(AgentExecutor):
    def __init__(self, results):
        self.results = list(results)
        self.requests = []
        self.init_calls = 0

    async def init(self, session_context):
        self.init_calls += 1

    async def execute(self, request_context):
        self.requests.append(request_context)
        result = self.results.pop(0)
        if isinstance(result, Exception):
            raise result
        if callable(result):
            return await result(request_context)
        return result


class ConcurrentAgent(AgentExecutor):
    def __init__(self):
        self.active = 0
        self.max_active = 0

    async def init(self, session_context):
        return None

    async def execute(self, request_context):
        self.active += 1
        self.max_active = max(self.max_active, self.active)
        await asyncio.sleep(0.01)
        self.active -= 1
        return Complete(request_context.input.message)
