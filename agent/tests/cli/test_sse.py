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

from ar_cli.api.function_invocation import stream_sse


class FakeResponse:
    """Minimal stand-in exposing iter_lines() like requests.Response."""

    def __init__(self, lines):
        self._lines = lines

    def iter_lines(self, decode_unicode=False):
        yield from self._lines


def test_parses_data_payloads_and_skips_blanks():
    resp = FakeResponse(
        [
            'data: {"func": "hello"}',
            "",
            "data: {}",
            "",
            'data: "OK"',
            "data: [DONE]",
            'data: "after done"',  # must not be yielded
        ]
    )
    assert list(stream_sse(resp)) == ['{"func": "hello"}', "{}", '"OK"']


def test_stops_at_done():
    resp = FakeResponse(["data: a", "data: [DONE]"])
    assert list(stream_sse(resp)) == ["a"]


def test_ignores_non_data_lines():
    resp = FakeResponse(["event: message", "data: x", ": keep-alive comment"])
    assert list(stream_sse(resp)) == ["x"]
