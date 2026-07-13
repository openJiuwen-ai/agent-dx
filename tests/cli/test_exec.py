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

from click.testing import CliRunner

from ar_cli.const import HEADER_AGENT_SESSION, HEADER_INSTANCE_SESSION
from ar_cli.main import cli


class FakeResponse:
    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc, tb):
        return False

    def iter_lines(self, decode_unicode=False):
        yield "data: ok"
        yield "data: [DONE]"


class BrokenStreamResponse(FakeResponse):
    def iter_lines(self, decode_unicode=False):
        raise RuntimeError("stream interrupted")
        yield


def _capture_invocations(monkeypatch):
    captured = []

    def fake_invoke(self, server, urn, *, headers, body):
        captured.append(
            {
                "server": server,
                "urn": urn,
                "headers": headers,
                "body": body,
            }
        )
        return FakeResponse()

    monkeypatch.setattr("ar_cli.client.AgentRuntimeClient.invoke", fake_invoke)
    return captured


def test_exec_with_args_invokes_once_and_preserves_json_body(monkeypatch):
    captured = _capture_invocations(monkeypatch)
    runner = CliRunner()

    result = runner.invoke(
        cli,
        ["exec", "--agent", "0@default@demo", "--server", "frontend:31180", "--args", '"你好"'],
    )

    assert result.exit_code == 0
    assert len(captured) == 1
    assert captured[0]["server"] == "http://frontend:31180"
    assert captured[0]["urn"] == "sn:cn:yrk:default:function:0@default@demo:latest"
    assert captured[0]["body"] == '"你好"'
    assert HEADER_AGENT_SESSION not in captured[0]["headers"]
    assert HEADER_INSTANCE_SESSION not in captured[0]["headers"]


def test_exec_without_args_enters_interactive_mode_and_wraps_messages(monkeypatch):
    captured = _capture_invocations(monkeypatch)
    runner = CliRunner()

    result = runner.invoke(
        cli,
        ["exec", "--agent", "0@default@demo", "--server", "frontend:31180"],
        input="你好\n下一轮\n/quit\n",
    )

    assert result.exit_code == 0
    assert "ar> " in result.output
    assert "yrar> " not in result.output
    assert [json.loads(item["body"]) for item in captured[:2]] == [
        {"message": "你好"},
        {"message": "下一轮"},
    ]
    assert json.loads(captured[2]["body"]) == {}

    session_headers = [json.loads(item["headers"][HEADER_AGENT_SESSION]) for item in captured]
    assert session_headers[0] == session_headers[1]
    assert session_headers[1] == session_headers[2]
    assert session_headers[0]["sessionCtx"].startswith("ar-")

    instance_headers = [json.loads(item["headers"][HEADER_INSTANCE_SESSION]) for item in captured]
    assert instance_headers[0] == {
        "sessionID": instance_headers[0]["sessionID"],
        "sessionTTL": 600,
        "concurrency": 1,
    }
    assert instance_headers[0]["sessionID"].startswith("ar-")
    assert instance_headers[1] == instance_headers[0]
    assert instance_headers[2] == {
        "sessionID": instance_headers[0]["sessionID"],
        "sessionTTL": 0,
        "concurrency": 1,
    }


def test_interactive_mode_uses_user_session_ctx(monkeypatch):
    captured = _capture_invocations(monkeypatch)
    runner = CliRunner()

    result = runner.invoke(
        cli,
        [
            "exec",
            "--agent",
            "0@default@demo",
            "--server",
            "frontend:31180",
            "--session-ctx",
            "ctx-user",
        ],
        input="hello\n/exit\n",
    )

    assert result.exit_code == 0
    assert len(captured) == 2
    assert json.loads(captured[0]["headers"][HEADER_AGENT_SESSION]) == {"sessionCtx": "ctx-user"}
    assert json.loads(captured[0]["headers"][HEADER_INSTANCE_SESSION])["sessionTTL"] == 600
    assert json.loads(captured[1]["headers"][HEADER_INSTANCE_SESSION])["sessionTTL"] == 0


def test_interactive_mode_uses_user_session_id_and_ttl(monkeypatch):
    captured = _capture_invocations(monkeypatch)
    runner = CliRunner()

    result = runner.invoke(
        cli,
        [
            "exec",
            "--agent",
            "0@default@demo",
            "--server",
            "frontend:31180",
            "--session-id",
            "id-user",
            "--session-ttl",
            "120",
        ],
        input="hello\n/exit\n",
    )

    assert result.exit_code == 0
    assert len(captured) == 2
    assert json.loads(captured[0]["headers"][HEADER_INSTANCE_SESSION]) == {
        "sessionID": "id-user",
        "sessionTTL": 120,
        "concurrency": 1,
    }
    assert json.loads(captured[1]["headers"][HEADER_INSTANCE_SESSION]) == {
        "sessionID": "id-user",
        "sessionTTL": 0,
        "concurrency": 1,
    }


def test_interactive_mode_releases_session_when_first_stream_fails(monkeypatch):
    captured = []

    def fake_invoke(self, server, urn, *, headers, body):
        captured.append({"headers": headers, "body": body})
        if len(captured) == 1:
            return BrokenStreamResponse()
        return FakeResponse()

    monkeypatch.setattr("ar_cli.client.AgentRuntimeClient.invoke", fake_invoke)
    runner = CliRunner()

    result = runner.invoke(
        cli,
        ["exec", "--agent", "0@default@demo", "--server", "frontend:31180"],
        input="hello\n",
    )

    assert result.exit_code == 1
    assert isinstance(result.exception, RuntimeError)
    assert len(captured) == 2
    assert json.loads(captured[0]["body"]) == {"message": "hello"}
    assert json.loads(captured[1]["body"]) == {}
    assert json.loads(captured[1]["headers"][HEADER_INSTANCE_SESSION])["sessionTTL"] == 0


def test_interactive_session_release_ignores_stream_failures(monkeypatch):
    captured = []

    def fake_invoke(self, server, urn, *, headers, body):
        captured.append({"headers": headers, "body": body})
        if len(captured) == 2:
            return BrokenStreamResponse()
        return FakeResponse()

    monkeypatch.setattr("ar_cli.client.AgentRuntimeClient.invoke", fake_invoke)
    runner = CliRunner()

    result = runner.invoke(
        cli,
        ["exec", "--agent", "0@default@demo", "--server", "frontend:31180"],
        input="hello\n/exit\n",
    )

    assert result.exit_code == 0
    assert len(captured) == 2
    assert json.loads(captured[1]["body"]) == {}
    assert json.loads(captured[1]["headers"][HEADER_INSTANCE_SESSION])["sessionTTL"] == 0


def test_exec_args_still_requires_valid_json(monkeypatch):
    _capture_invocations(monkeypatch)
    runner = CliRunner()

    result = runner.invoke(
        cli,
        ["exec", "--agent", "0@default@demo", "--server", "frontend:31180", "--args", "not-json"],
    )

    assert result.exit_code == 2


def test_exec_agent_accepts_explicit_version(monkeypatch):
    captured = _capture_invocations(monkeypatch)
    runner = CliRunner()

    result = runner.invoke(
        cli,
        ["exec", "--agent", "0@default@demo:v1", "--server", "frontend:31180", "--args", "{}"],
    )

    assert result.exit_code == 0
    assert captured[0]["urn"] == "sn:cn:yrk:default:function:0@default@demo:v1"


def test_exec_agent_rejects_non_default_prefix(monkeypatch):
    _capture_invocations(monkeypatch)
    runner = CliRunner()

    result = runner.invoke(
        cli,
        ["exec", "--agent", "0@svc@demo", "--server", "frontend:31180", "--args", "{}"],
    )

    assert result.exit_code == 2


def test_session_ttl_without_session_id_aborts_without_sending(monkeypatch):
    captured = _capture_invocations(monkeypatch)
    runner = CliRunner()

    result = runner.invoke(
        cli,
        ["exec", "--agent", "0@default@demo", "--server", "frontend:31180", "--session-ttl", "120"],
    )

    assert result.exit_code == 2
    assert "require --session-id" in result.output
    assert captured == []  # nothing was sent


def test_concurrency_without_session_id_aborts_without_sending(monkeypatch):
    captured = _capture_invocations(monkeypatch)
    runner = CliRunner()

    result = runner.invoke(
        cli,
        ["exec", "--agent", "0@default@demo", "--server", "frontend:31180", "--concurrency", "4"],
    )

    assert result.exit_code == 2
    assert captured == []


def test_session_ctx_over_max_len_aborts_without_sending(monkeypatch):
    captured = _capture_invocations(monkeypatch)
    runner = CliRunner()

    result = runner.invoke(
        cli,
        ["exec", "--agent", "0@default@demo", "--server", "frontend:31180", "--session-ctx", "c" * 64],
    )

    assert result.exit_code == 2
    assert "at most 63 characters" in result.output
    assert captured == []  # nothing was sent


def test_session_id_over_max_len_aborts_without_sending(monkeypatch):
    captured = _capture_invocations(monkeypatch)
    runner = CliRunner()

    result = runner.invoke(
        cli,
        ["exec", "--agent", "0@default@demo", "--server", "frontend:31180", "--session-id", "i" * 64],
    )

    assert result.exit_code == 2
    assert captured == []


def test_session_fields_at_max_len_are_allowed(monkeypatch):
    captured = _capture_invocations(monkeypatch)
    runner = CliRunner()

    result = runner.invoke(
        cli,
        [
            "exec",
            "--agent",
            "0@default@demo",
            "--server",
            "frontend:31180",
            "--session-ctx",
            "c" * 63,
            "--args",
            '"hi"',
        ],
    )

    assert result.exit_code == 0
    assert len(captured) == 1


def test_server_without_port_is_rejected(monkeypatch):
    captured = _capture_invocations(monkeypatch)
    runner = CliRunner()

    result = runner.invoke(cli, ["exec", "--agent", "0@default@demo", "--server", "frontend", "--args", '"hi"'])

    assert result.exit_code == 2
    assert "host:port" in result.output
    assert captured == []


def test_server_with_invalid_port_is_rejected(monkeypatch):
    captured = _capture_invocations(monkeypatch)
    runner = CliRunner()

    result = runner.invoke(
        cli, ["exec", "--agent", "0@default@demo", "--server", "frontend:99999", "--args", '"hi"']
    )

    assert result.exit_code == 2
    assert captured == []


def test_empty_agent_is_rejected(monkeypatch):
    captured = _capture_invocations(monkeypatch)
    runner = CliRunner()

    result = runner.invoke(cli, ["exec", "--agent", "  ", "--server", "frontend:31180", "--args", '"hi"'])

    assert result.exit_code == 2
    assert captured == []


def test_session_ttl_zero_is_rejected(monkeypatch):
    captured = _capture_invocations(monkeypatch)
    runner = CliRunner()

    result = runner.invoke(
        cli,
        ["exec", "--agent", "0@default@demo", "--server", "frontend:31180", "--session-id", "id1", "--session-ttl", "0"],
    )

    assert result.exit_code == 2
    assert captured == []


def test_concurrency_zero_is_rejected(monkeypatch):
    captured = _capture_invocations(monkeypatch)
    runner = CliRunner()

    result = runner.invoke(
        cli,
        ["exec", "--agent", "0@default@demo", "--server", "frontend:31180", "--session-id", "id1", "--concurrency", "0"],
    )

    assert result.exit_code == 2
    assert captured == []


def test_session_ttl_with_session_id_is_allowed(monkeypatch):
    captured = _capture_invocations(monkeypatch)
    runner = CliRunner()

    result = runner.invoke(
        cli,
        [
            "exec",
            "--agent",
            "0@default@demo",
            "--server",
            "frontend:31180",
            "--session-id",
            "id1",
            "--session-ttl",
            "120",
            "--args",
            '"hi"',
        ],
    )

    assert result.exit_code == 0
    assert len(captured) == 1
