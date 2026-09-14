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

from ar_cli.const import HEADER_AGENT_SESSION, HEADER_INSTANCE_SESSION
from ar_cli.api.function_invocation import build_invocation_headers


def test_no_session_headers_when_unset():
    headers = build_invocation_headers()
    assert HEADER_AGENT_SESSION not in headers
    assert HEADER_INSTANCE_SESSION not in headers
    assert headers["Content-Type"] == "application/json"
    assert headers["Accept"] == "text/event-stream"


def test_agent_session_only():
    headers = build_invocation_headers(session_ctx="ctx1")
    assert json.loads(headers[HEADER_AGENT_SESSION]) == {"sessionCtx": "ctx1"}
    assert HEADER_INSTANCE_SESSION not in headers


def test_instance_session_defaults():
    headers = build_invocation_headers(session_id="id1")
    assert json.loads(headers[HEADER_INSTANCE_SESSION]) == {
        "sessionID": "id1",
        "sessionTTL": 90,
        "concurrency": 1,
    }


def test_instance_session_custom_values():
    headers = build_invocation_headers(session_id="id1", session_ttl=120, concurrency=4)
    assert json.loads(headers[HEADER_INSTANCE_SESSION]) == {
        "sessionID": "id1",
        "sessionTTL": 120,
        "concurrency": 4,
    }
