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

"""Function invocation request encoding and SSE response consumption."""

import json
from dataclasses import dataclass
from typing import Dict, Iterator, Optional

from ar_cli.const import (
    ACCEPT_SSE,
    CONTENT_TYPE_JSON,
    DEFAULT_CONCURRENCY,
    DEFAULT_SESSION_TTL,
    HEADER_ACCEPT,
    HEADER_AGENT_SESSION,
    HEADER_CONTENT_TYPE,
    HEADER_INSTANCE_SESSION,
    SSE_DATA_PREFIX,
    SSE_DONE_MARKER,
)


@dataclass(frozen=True)
class InvocationOptions:
    """Session and request metadata for one function invocation."""

    session_ctx: Optional[str] = None
    session_id: Optional[str] = None
    session_ttl: Optional[int] = None
    concurrency: Optional[int] = None

    def to_headers(self) -> Dict[str, str]:
        headers: Dict[str, str] = {
            HEADER_CONTENT_TYPE: CONTENT_TYPE_JSON,
            HEADER_ACCEPT: ACCEPT_SSE,
        }
        if self.session_ctx is not None:
            headers[HEADER_AGENT_SESSION] = json.dumps({"sessionCtx": self.session_ctx})
        if self.session_id is not None:
            ttl = self.session_ttl if self.session_ttl is not None else DEFAULT_SESSION_TTL
            concurrency = self.concurrency if self.concurrency is not None else DEFAULT_CONCURRENCY
            headers[HEADER_INSTANCE_SESSION] = json.dumps(
                {"sessionID": self.session_id, "sessionTTL": ttl, "concurrency": concurrency}
            )
        return headers


def build_invocation_headers(
    session_ctx: Optional[str] = None,
    session_id: Optional[str] = None,
    session_ttl: Optional[int] = None,
    concurrency: Optional[int] = None,
) -> Dict[str, str]:
    """Compatibility helper for callers that do not construct InvocationOptions."""
    return InvocationOptions(
        session_ctx=session_ctx,
        session_id=session_id,
        session_ttl=session_ttl,
        concurrency=concurrency,
    ).to_headers()


def stream_sse(response) -> Iterator[str]:
    """Yield SSE ``data:`` payloads until the ``[DONE]`` terminator."""
    for raw in response.iter_lines(decode_unicode=True):
        if not raw:
            continue
        if raw.startswith(SSE_DATA_PREFIX):
            payload = raw[len(SSE_DATA_PREFIX):].strip()
            if payload == SSE_DONE_MARKER:
                return
            yield payload


def invoke_stream(client, server: str, urn: str, *, headers: Dict[str, str], body: Optional[str]) -> Iterator[str]:
    """Invoke a function and yield its SSE data frames while the response is open."""
    response = client.invoke(server, urn, headers=headers, body=body)
    with response:
        yield from stream_sse(response)
