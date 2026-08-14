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

"""Persist-before-send streaming output."""

from __future__ import annotations

from typing import TYPE_CHECKING, Any, Optional

from .errors import OutputNotActive
from .serialization import to_stream_value

if TYPE_CHECKING:
    from .context import RequestContext
    from .turn_writer import TurnWriter


class OutputWriter:
    def __init__(self, writer: "TurnWriter", stream: object):
        self._writer = writer
        self._stream = stream
        self._request_context: Optional["RequestContext"] = None
        self._turn_id: Optional[str] = None

    def _bind(self, request_context: "RequestContext", turn_id: str) -> None:
        self._request_context = request_context
        self._turn_id = turn_id

    async def write(self, value: Any) -> None:
        context = self._request_context
        if context is None or not context.is_active or self._turn_id is None:
            raise OutputNotActive()
        serialized = to_stream_value(value)
        await self._writer.append_platform(
            self._turn_id,
            "output.message",
            {"message": value},
        )
        self._stream.write(serialized)
