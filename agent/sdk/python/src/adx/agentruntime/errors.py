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

"""Agent Runtime error types."""

from __future__ import annotations


class AgentRuntimeError(RuntimeError):
    """An error with a stable machine-readable code."""

    def __init__(self, code: str, message: str):
        self.code = code
        self.message = message
        super().__init__(f"{code}: {message}")


class AgentRuntimeNotConfigured(AgentRuntimeError):
    def __init__(self, message: str):
        super().__init__("AGENT_RUNTIME_NOT_CONFIGURED", message)


class SessionContextBindingMismatch(AgentRuntimeError):
    def __init__(self):
        super().__init__(
            "SESSION_CONTEXT_BINDING_MISMATCH",
            "the invocation identity does not match the Agent Runtime instance binding",
        )


class AgentExecutorLoadFailed(AgentRuntimeError):
    def __init__(self, message: str):
        super().__init__("AGENT_EXECUTOR_LOAD_FAILED", message)


class AgentInitFailed(AgentRuntimeError):
    def __init__(self, message: str):
        super().__init__("AGENT_INIT_FAILED", message)


class InvalidExecutionResult(AgentRuntimeError):
    def __init__(self, message: str = "execute() must return Complete or InputRequired"):
        super().__init__("INVALID_EXECUTION_RESULT", message)


class OutputNotActive(AgentRuntimeError):
    def __init__(self):
        super().__init__("OUTPUT_NOT_ACTIVE", "the current execution is no longer active")


class EventAppendNotActive(AgentRuntimeError):
    def __init__(self):
        super().__init__("EVENT_APPEND_NOT_ACTIVE", "events may only be appended during execute()")


class EventSerializationFailed(AgentRuntimeError):
    def __init__(self, message: str):
        super().__init__("EVENT_SERIALIZATION_FAILED", message)


class DataSystemError(AgentRuntimeError):
    def __init__(self, message: str):
        super().__init__("DATASYSTEM_ERROR", message)
