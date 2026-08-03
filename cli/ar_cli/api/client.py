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

"""Public Agent Runtime API facade used by CLI commands and interactive mode."""

import json
from typing import Any, Dict, Optional

from ar_cli.api.http import HttpClient, join_url
from ar_cli.api.session_context import SessionContextApi
from ar_cli.const import (
    CONTENT_TYPE_JSON,
    DEFAULT_SESSION_CONTEXT_LIST_LIMIT,
    FUNCTIONS_PATH,
    HEADER_CONTENT_TYPE,
    INVOCATIONS_PATH,
)


class AgentRuntimeClient:
    """Expose Agent Runtime operations without leaking REST implementation details."""

    def __init__(
        self,
        timeout: Optional[float] = None,
        jwt_token: Optional[str] = None,
    ) -> None:
        self._http = HttpClient(timeout, jwt_token=jwt_token)
        self._session_contexts = SessionContextApi(self._http)

    @property
    def _session(self):
        """Compatibility hook for tests that replace the requests session."""
        return self._http.session

    @_session.setter
    def _session(self, value) -> None:
        self._http.session = value

    def register_function(self, server: str, spec: Dict[str, Any]) -> Dict[str, Any]:
        return self._http.request_json(
            "POST",
            join_url(server, FUNCTIONS_PATH),
            action="deploy",
            headers={HEADER_CONTENT_TYPE: CONTENT_TYPE_JSON},
            body=json.dumps(spec),
        )

    def invoke(
        self,
        server: str,
        urn: str,
        *,
        headers: Dict[str, str],
        body: Optional[str],
    ):
        return self._http.post_stream(
            join_url(server, INVOCATIONS_PATH.format(urn=urn)),
            headers=headers,
            body=body,
            action="invoke",
        )

    def list_session_contexts(
        self,
        server: str,
        function_urn: str,
        *,
        limit: int = DEFAULT_SESSION_CONTEXT_LIST_LIMIT,
        page_token: Optional[str] = None,
    ) -> Dict[str, Any]:
        return self._session_contexts.list_session_contexts(
            server,
            function_urn,
            limit=limit,
            page_token=page_token,
        )

    def list_turns(
        self,
        server: str,
        function_urn: str,
        session_ctx_id: str,
        *,
        limit: int = DEFAULT_SESSION_CONTEXT_LIST_LIMIT,
        page_token: Optional[str] = None,
    ) -> Dict[str, Any]:
        return self._session_contexts.list_turns(
            server,
            function_urn,
            session_ctx_id,
            limit=limit,
            page_token=page_token,
        )

    def fork_session_context(
        self,
        server: str,
        function_urn: str,
        session_ctx_id: str,
        *,
        turn_id: str,
        target_session_ctx_id: str,
    ) -> Dict[str, Any]:
        return self._session_contexts.fork_session_context(
            server,
            function_urn,
            session_ctx_id,
            turn_id=turn_id,
            target_session_ctx_id=target_session_ctx_id,
        )

    def delete_session_context(
        self,
        server: str,
        function_urn: str,
        session_ctx_id: str,
    ) -> None:
        return self._session_contexts.delete_session_context(
            server,
            function_urn,
            session_ctx_id,
        )
