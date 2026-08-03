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

import pytest

import ar_cli.interactive as interactive
import ar_cli.interactive.query as query
from ar_cli.errors import ApiError
from ar_cli.interactive import InteractiveCommandError, InteractiveSessionState, handle_session_command
from ar_cli.interactive.state import CommandResult


class FakeClient:
    def __init__(self):
        self.calls = []
        self.sessions_payload = {"sessionContexts": []}
        self.turns_payload = {"turns": []}
        self.turns_error = None
        self.fork_payload = {"sessionContextId": "forked"}

    def list_session_contexts(self, server, function_urn, *, limit):
        self.calls.append(("sessions", server, function_urn, limit))
        return self.sessions_payload

    def list_turns(self, server, function_urn, session_ctx_id, *, limit):
        self.calls.append(("turns", server, function_urn, session_ctx_id, limit))
        if self.turns_error is not None:
            raise self.turns_error
        return self.turns_payload

    def fork_session_context(
        self, server, function_urn, session_ctx_id, *, turn_id, target_session_ctx_id
    ):
        self.calls.append(
            ("fork", server, function_urn, session_ctx_id, turn_id, target_session_ctx_id)
        )
        return self.fork_payload

    def delete_session_context(self, server, function_urn, session_ctx_id):
        self.calls.append(("delete", server, function_urn, session_ctx_id))


def _state(session_ctx="research-main"):
    return InteractiveSessionState(
        server_url="http://frontend:31180",
        agent_urn="0@default@demo",
        session_ctx=session_ctx,
    )


def test_slash_command_completions_include_session_commands_and_exit_commands():
    completions = dict(interactive.SLASH_COMMAND_COMPLETIONS)

    assert set(("/sessions", "/history", "/fork", "/delete", "/new", "/quit")) <= set(completions)
    assert "/exit" not in completions


def test_sessions_filters_by_current_function_and_uses_table_mode_without_tty(monkeypatch):
    client = FakeClient()
    client.sessions_payload = {
        "sessionContexts": [
            {
                "sessionContextId": "research-main",
                "functionVersion": "latest",
                "createdAt": "2026-07-20T10:31:22Z",
            }
        ]
    }
    printed = []
    monkeypatch.setattr(query, "can_select_items", lambda: False)
    monkeypatch.setattr(query.print_logger, "info", lambda message, *args: printed.append(message % args))

    handle_session_command("/sessions", state=_state(), client=client)

    assert client.calls == [
        ("sessions", "http://frontend:31180", "0@default@demo", 50)
    ]
    assert "SESSION CONTEXT" in printed[0]
    assert "research-main" in printed[0]
    assert "latest" in printed[0]


def test_sessions_switches_context_when_linux_selector_confirms(monkeypatch):
    client = FakeClient()
    client.sessions_payload = {
        "sessionContexts": [
            {"sessionContextId": "research-main", "functionVersion": "latest", "createdAt": ""},
            {"sessionContextId": "research-branch", "functionVersion": "latest", "createdAt": ""},
        ]
    }
    state = _state()
    monkeypatch.setattr(query, "can_select_items", lambda: True)
    monkeypatch.setattr(query, "select_item", lambda *args, **kwargs: 1)

    handle_session_command("/sessions", state=state, client=client)

    assert state.session_ctx == "research-branch"


def test_history_404_is_a_friendly_interactive_result(monkeypatch):
    client = FakeClient()
    client.turns_error = ApiError("not found", status_code=404)
    printed = []
    monkeypatch.setattr(query.print_logger, "info", lambda message, *args: printed.append(message % args))

    handle_session_command("/history", state=_state(), client=client)

    assert "has not been created" in printed[0]


def test_history_renders_new_turn_response_fields(monkeypatch):
    client = FakeClient()
    client.turns_payload = {
        "turns": [
            {
                "turnId": "turn-0001",
                "state": "COMPLETED",
                "inputs": [{"text": "hello"}],
                "outputs": [{"kind": "progress"}],
                "result": {"kind": "complete"},
                "createdAt": "2026-07-29T03:02:08Z",
            }
        ],
        "nextPageToken": None,
    }
    printed = []
    monkeypatch.setattr(query.print_logger, "info", lambda message, *args: printed.append(message % args))

    handle_session_command("/history", state=_state(), client=client)

    assert "STATE" in printed[0]
    assert "COMPLETED" in printed[0]
    assert "hello" in printed[0]
    assert "complete" in printed[0]


def test_fork_switches_to_returned_session_context():
    client = FakeClient()
    state = _state()

    handle_session_command("/fork turn-0001 research-alt", state=state, client=client)

    assert client.calls[0] == (
        "fork",
        "http://frontend:31180",
        "0@default@demo",
        "research-main",
        "turn-0001",
        "research-alt",
    )
    assert state.session_ctx == "forked"


def test_fork_requires_a_target_session_context():
    client = FakeClient()

    with pytest.raises(InteractiveCommandError, match="usage: /fork"):
        handle_session_command("/fork turn-0001", state=_state(), client=client)

    assert client.calls == []


def test_fork_rejects_current_session_as_target_without_sending_request():
    client = FakeClient()

    with pytest.raises(InteractiveCommandError, match="must differ"):
        handle_session_command(
            "/fork turn-0001 research-main",
            state=_state(),
            client=client,
        )

    assert client.calls == []


def test_delete_current_session_is_rejected_without_sending_request():
    client = FakeClient()
    state = _state()

    with pytest.raises(InteractiveCommandError, match="cannot delete current SessionCtx"):
        handle_session_command("/delete research-main", state=state, client=client)

    assert client.calls == []
    assert state.session_ctx == "research-main"


def test_delete_non_current_session_keeps_current_context():
    client = FakeClient()
    state = _state()

    handle_session_command("/delete research-old", state=state, client=client)

    assert client.calls[0] == (
        "delete",
        "http://frontend:31180",
        "0@default@demo",
        "research-old",
    )
    assert state.session_ctx == "research-main"


def test_new_is_local_and_does_not_call_client():
    client = FakeClient()
    state = _state()

    handle_session_command("/new research-next", state=state, client=client)

    assert state.session_ctx == "research-next"
    assert client.calls == []


def test_quit_command_returns_exit_result_without_calling_client():
    client = FakeClient()

    result = handle_session_command("/quit", state=_state(), client=client)

    assert result is CommandResult.EXIT
    assert client.calls == []


def test_exit_command_is_not_registered():
    client = FakeClient()

    with pytest.raises(InteractiveCommandError, match="unknown command: /exit"):
        handle_session_command("/exit", state=_state(), client=client)

    assert client.calls == []


@pytest.mark.parametrize(
    "command",
    [
        "/fork " + "t" * 64 + " target",
        "/fork turn-1 " + "c" * 64,
        "/delete " + "c" * 64,
        "/new " + "c" * 64,
    ],
)
def test_resource_ids_longer_than_63_are_rejected_without_requests(command):
    client = FakeClient()

    with pytest.raises(InteractiveCommandError):
        handle_session_command(command, state=_state(), client=client)

    assert client.calls == []
