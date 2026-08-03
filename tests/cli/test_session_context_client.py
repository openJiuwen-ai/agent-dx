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

import json
import logging

import pytest

from ar_cli.api.client import AgentRuntimeClient
from ar_cli.const import HEADER_AUTH
from ar_cli.errors import ApiError


class FakeResponse:
    def __init__(self, payload, *, ok=True, status_code=200, text=""):
        self._payload = payload
        self.ok = ok
        self.status_code = status_code
        self.text = text
        self.closed = False

    def json(self):
        if isinstance(self._payload, Exception):
            raise self._payload
        return self._payload

    def close(self):
        self.closed = True


class FakeSession:
    def __init__(self, response):
        self.response = response
        self.calls = []

    def request(self, method, url, **kwargs):
        self.calls.append((method, url, kwargs))
        return self.response

    def post(self, url, **kwargs):
        self.calls.append(("POST", url, kwargs))
        return self.response


def _client(response, *, jwt_token=None):
    client = AgentRuntimeClient(jwt_token=jwt_token)
    fake_session = FakeSession(response)
    client._session = fake_session
    return client, fake_session


def test_list_session_contexts_uses_function_scoped_resource_path():
    client, session = _client(FakeResponse({"sessionContexts": []}))
    urn = "0@default@demo"

    assert client.list_session_contexts("http://frontend", urn) == {"sessionContexts": []}

    method, url, kwargs = session.calls[0]
    assert method == "GET"
    assert url == (
        "http://frontend/serverless/v1/functions/"
        "0%40default%40demo/session-contexts"
    )
    assert kwargs["params"] == {"limit": 50}
    assert session.response.closed is True


def test_register_function_uses_the_function_resource_and_json_body():
    client, session = _client(FakeResponse({"function": {"functionUrn": "urn"}}))

    result = client.register_function("http://server", {"name": "demo"})

    assert result == {"function": {"functionUrn": "urn"}}
    method, url, kwargs = session.calls[0]
    assert method == "POST"
    assert url == "http://server/serverless/v1/functions"
    assert kwargs["headers"] == {"Content-Type": "application/json"}
    assert json.loads(kwargs["data"]) == {"name": "demo"}
    assert session.response.closed is True


def test_invoke_opens_utf8_stream_for_the_function_invocation_resource():
    response = FakeResponse({}, text="")
    client, session = _client(response)

    actual = client.invoke(
        "http://server",
        "0@default@demo",
        headers={"Accept": "text/event-stream"},
        body='{"message":"hello"}',
    )

    assert actual is response
    assert response.encoding == "utf-8"
    method, url, kwargs = session.calls[0]
    assert method == "POST"
    assert url.endswith("/functions/0@default@demo/invocations")
    assert kwargs["stream"] is True
    assert kwargs["headers"] == {"Accept": "text/event-stream"}
    assert kwargs["data"] == '{"message":"hello"}'


def test_fork_encodes_resource_path_and_sends_required_target():
    client, session = _client(FakeResponse({"sessionContextId": "target"}))

    client.fork_session_context(
        "http://frontend",
        "0@default@demo",
        "source/ctx",
        turn_id="turn-1",
        target_session_ctx_id="target",
    )

    method, url, kwargs = session.calls[0]
    assert method == "POST"
    assert "/functions/0%40default%40demo/" in url
    assert "/session-contexts/source%2Fctx/fork" in url
    assert kwargs["headers"] == {"Content-Type": "application/json"}
    assert json.loads(kwargs["data"]) == {"turnId": "turn-1", "targetSessionCtxId": "target"}


def test_delete_accepts_no_content_response_without_parsing_json():
    response = FakeResponse(ValueError("no JSON"), status_code=204)
    client, session = _client(response)

    assert client.delete_session_context("http://frontend", "urn", "old/ctx") is None

    method, url, kwargs = session.calls[0]
    assert method == "DELETE"
    assert url == "http://frontend/serverless/v1/functions/urn/session-contexts/old%2Fctx"
    assert "data" not in kwargs
    assert response.closed is True


def test_jwt_token_is_sent_and_redacted_from_debug_logs(caplog):
    token = "secret-jwt-token"
    client, session = _client(FakeResponse({"sessionContexts": []}), jwt_token=token)

    with caplog.at_level(logging.DEBUG, logger="ar_cli.api.http"):
        client.list_session_contexts("http://frontend", "urn")

    _, _, kwargs = session.calls[0]
    assert kwargs["headers"][HEADER_AUTH] == token
    assert token not in caplog.text
    assert "<redacted>" in caplog.text


def test_management_api_error_keeps_http_and_service_codes():
    client, _ = _client(
        FakeResponse(
            {"code": "SESSION_CONTEXT_NOT_FOUND", "message": "session context not found"},
            ok=False,
            status_code=404,
        )
    )

    with pytest.raises(ApiError) as captured:
        client.list_turns("http://frontend", "urn", "ctx")

    assert captured.value.status_code == 404
    assert captured.value.service_code == "SESSION_CONTEXT_NOT_FOUND"
