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

"""SessionCtx resource operations exposed by the Agent Runtime API."""

import json
from typing import Any, Dict, Optional
from urllib.parse import quote

from ar_cli.api.http import HttpClient, join_url
from ar_cli.const import (
    CONTENT_TYPE_JSON,
    DEFAULT_SESSION_CONTEXT_LIST_LIMIT,
    FUNCTION_SESSION_CONTEXTS_PATH,
    FUNCTION_SESSION_CONTEXT_PATH,
    HEADER_CONTENT_TYPE,
    SESSION_CONTEXT_FORK_PATH,
    SESSION_CONTEXT_TURNS_PATH,
)


class SessionContextApi:
    """REST resource adapter for SessionCtx and Turn operations."""

    def __init__(self, http: HttpClient) -> None:
        self._http = http

    def list_session_contexts(
        self,
        server: str,
        function_urn: str,
        *,
        limit: int = DEFAULT_SESSION_CONTEXT_LIST_LIMIT,
        page_token: Optional[str] = None,
    ) -> Dict[str, Any]:
        params: Dict[str, Any] = {"limit": limit}
        if page_token is not None:
            params["pageToken"] = page_token
        return self._http.request_json(
            "GET",
            join_url(server, _function_path(FUNCTION_SESSION_CONTEXTS_PATH, function_urn)),
            action="list session contexts",
            params=params,
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
        params: Dict[str, Any] = {"limit": limit}
        if page_token is not None:
            params["pageToken"] = page_token
        return self._http.request_json(
            "GET",
            join_url(server, _session_context_path(SESSION_CONTEXT_TURNS_PATH, function_urn, session_ctx_id)),
            action="list turns",
            params=params,
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
        body: Dict[str, Any] = {
            "turnId": turn_id,
            "targetSessionCtxId": target_session_ctx_id,
        }
        return self._http.request_json(
            "POST",
            join_url(server, _session_context_path(SESSION_CONTEXT_FORK_PATH, function_urn, session_ctx_id)),
            action="fork session context",
            headers={HEADER_CONTENT_TYPE: CONTENT_TYPE_JSON},
            body=json.dumps(body),
        )

    def delete_session_context(
        self,
        server: str,
        function_urn: str,
        session_ctx_id: str,
    ) -> None:
        self._http.request_no_content(
            "DELETE",
            join_url(server, _session_context_path(FUNCTION_SESSION_CONTEXT_PATH, function_urn, session_ctx_id)),
            action="delete session context",
        )


def _function_path(template: str, function_urn: str) -> str:
    return template.format(urn=quote(function_urn, safe=""))


def _session_context_path(template: str, function_urn: str, session_ctx_id: str) -> str:
    return template.format(
        urn=quote(function_urn, safe=""),
        session_ctx_id=quote(session_ctx_id, safe=""),
    )
