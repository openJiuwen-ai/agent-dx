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

"""FaaS metadata extraction."""

from dataclasses import dataclass
import os
from typing import Mapping, Optional

from .errors import AgentRuntimeNotConfigured


@dataclass(frozen=True)
class RuntimeMetadata:
    tenant_id: str
    function_name: str
    function_version: str
    session_context_id: str

    @classmethod
    def from_function_context(
        cls,
        function_context: object,
        environ: Optional[Mapping[str, str]] = None,
    ) -> "RuntimeMetadata":
        env = os.environ if environ is None else environ
        session_context_id = env.get("YR_SESSION_CTX_ID")
        if session_context_id is None or not session_context_id.strip():
            raise AgentRuntimeNotConfigured("YR_SESSION_CTX_ID is not set or empty")
        try:
            return cls(
                tenant_id=_required_context_value(function_context.getTenantID(), "tenant ID"),
                function_name=_required_context_value(
                    function_context.getFunctionName(), "function name"
                ),
                function_version=_required_context_value(
                    function_context.getVersion(), "function version"
                ),
                session_context_id=session_context_id,
            )
        except AttributeError as exc:
            raise AgentRuntimeNotConfigured(
                "FunctionContext does not expose tenant, function name, and version"
            ) from exc


def _required_context_value(value: object, field: str) -> str:
    if value is None:
        raise AgentRuntimeNotConfigured(f"FunctionContext {field} is not set")
    text = str(value)
    if not text.strip():
        raise AgentRuntimeNotConfigured(f"FunctionContext {field} is empty")
    return text
