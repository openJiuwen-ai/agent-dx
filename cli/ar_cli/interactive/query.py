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

"""Read-only SessionCtx interactive commands."""

from typing import Sequence

from ar_cli.const import DEFAULT_SESSION_CONTEXT_LIST_LIMIT
from ar_cli.errors import ApiError
from ar_cli.interactive.lineedit import can_select_items, select_item
from ar_cli.interactive.render import format_row, format_table, objects, session_row, turn_row
from ar_cli.interactive.state import CommandResult, InteractiveCommandError, InteractiveContext
from ar_cli.utils import print_logger


def show_sessions(args: Sequence[str], context: InteractiveContext) -> CommandResult:
    if args:
        raise InteractiveCommandError("usage: /sessions")
    state = context.state
    payload = context.client.list_session_contexts(
        state.server_url,
        state.agent_urn,
        limit=DEFAULT_SESSION_CONTEXT_LIST_LIMIT,
    )
    sessions = objects(payload.get("sessionContexts"), "sessionContexts")
    if not sessions:
        print_logger.info("No SessionCtx found for current agent.")
        return CommandResult.CONTINUE

    selected_index = next(
        (
            index
            for index, item in enumerate(sessions)
            if item.get("sessionContextId") == state.session_ctx
        ),
        0,
    )
    rows = [session_row(item) for item in sessions]
    headers = ["SESSION CONTEXT", "VERSION", "CREATED"]
    widths = [22, 12, 19]
    if can_select_items():
        chosen = select_item(
            [format_row(row, widths) for row in rows],
            selected_index=selected_index,
            heading="Select SessionCtx: Up/Down to move, Enter to switch, Esc/q to cancel",
            header=format_row(headers, widths),
        )
        if chosen is not None:
            state.session_ctx = str(sessions[chosen].get("sessionContextId", ""))
            print_logger.info("Switched to SessionCtx %s", state.session_ctx)
    else:
        print_logger.info("%s", format_table(headers, rows, widths, selected_index))
    if payload.get("nextPageToken") is not None:
        print_logger.info("Only the first %d SessionCtx entries are shown.", DEFAULT_SESSION_CONTEXT_LIST_LIMIT)
    return CommandResult.CONTINUE


def show_history(args: Sequence[str], context: InteractiveContext) -> CommandResult:
    if args:
        raise InteractiveCommandError("usage: /history")
    state = context.state
    try:
        payload = context.client.list_turns(
            state.server_url,
            state.agent_urn,
            state.session_ctx,
            limit=DEFAULT_SESSION_CONTEXT_LIST_LIMIT,
        )
    except ApiError as e:
        if e.status_code == 404:
            print_logger.info("No history: SessionCtx %s has not been created or no longer exists.", state.session_ctx)
            return CommandResult.CONTINUE
        raise

    turns = objects(payload.get("turns"), "turns")
    if not turns:
        print_logger.info("No Turns found for SessionCtx %s.", state.session_ctx)
        return CommandResult.CONTINUE
    headers = ["TURN", "STATE", "INPUTS", "OUTPUTS", "RESULT/ERROR"]
    widths = [16, 14, 24, 24, 24]
    rows = [turn_row(turn) for turn in turns]
    print_logger.info("%s", format_table(headers, rows, widths))
    if payload.get("nextPageToken") is not None:
        print_logger.info("Only the first %d Turns are shown.", DEFAULT_SESSION_CONTEXT_LIST_LIMIT)
    return CommandResult.CONTINUE
