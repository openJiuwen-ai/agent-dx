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

"""SessionCtx mutation and interactive lifecycle commands."""

import uuid
from typing import Sequence

from ar_cli.const import SESSION_FIELD_MAX_LEN
from ar_cli.errors import ApiError
from ar_cli.interactive.state import CommandResult, InteractiveCommandError, InteractiveContext
from ar_cli.utils import print_logger


def fork_session(args: Sequence[str], context: InteractiveContext) -> CommandResult:
    if len(args) != 2:
        raise InteractiveCommandError("usage: /fork <turn-id> <new-session-ctx-id>")
    turn_id = validate_resource_id(args[0], "turn ID")
    target_session_ctx_id = validate_resource_id(args[1], "SessionCtx ID")
    state = context.state
    source_session_ctx = state.session_ctx
    if target_session_ctx_id == source_session_ctx:
        raise InteractiveCommandError(
            "target SessionCtx ID must differ from the current SessionCtx"
        )
    try:
        payload = context.client.fork_session_context(
            state.server_url,
            state.agent_urn,
            source_session_ctx,
            turn_id=turn_id,
            target_session_ctx_id=target_session_ctx_id,
        )
    except ApiError as e:
        if e.status_code == 409:
            raise InteractiveCommandError(
                f"fork conflict: {e.service_code or e}"
            )
        raise

    target_session_ctx = payload.get("sessionContextId")
    if not isinstance(target_session_ctx, str) or not target_session_ctx:
        raise InteractiveCommandError("fork response does not contain sessionContextId")
    state.session_ctx = target_session_ctx
    print_logger.info("Forked %s at %s to %s", source_session_ctx, turn_id, target_session_ctx)
    print_logger.info("Switched to SessionCtx %s", target_session_ctx)
    return CommandResult.CONTINUE


def delete_session(args: Sequence[str], context: InteractiveContext) -> CommandResult:
    if len(args) != 1:
        raise InteractiveCommandError("usage: /delete <session-ctx-id>")
    state = context.state
    session_ctx_id = validate_resource_id(args[0], "SessionCtx ID")
    if session_ctx_id == state.session_ctx:
        raise InteractiveCommandError(
            "cannot delete current SessionCtx; switch with /new or /sessions first"
        )
    context.client.delete_session_context(
        state.server_url,
        state.agent_urn,
        session_ctx_id,
    )
    print_logger.info("Deleted SessionCtx %s", session_ctx_id)
    return CommandResult.CONTINUE


def new_session(args: Sequence[str], context: InteractiveContext) -> CommandResult:
    if len(args) > 1:
        raise InteractiveCommandError("usage: /new [session-ctx-id]")
    state = context.state
    state.session_ctx = validate_resource_id(args[0], "SessionCtx ID") if args else new_session_ctx_id()
    print_logger.info("Current SessionCtx: %s (not created)", state.session_ctx)
    return CommandResult.CONTINUE


def quit_interactive(args: Sequence[str], context: InteractiveContext) -> CommandResult:
    if args:
        raise InteractiveCommandError("usage: /quit")
    return CommandResult.EXIT


def validate_resource_id(value: str, label: str) -> str:
    if not value:
        raise InteractiveCommandError(f"{label} must not be empty")
    if len(value) > SESSION_FIELD_MAX_LEN:
        raise InteractiveCommandError(
            f"{label} must be at most {SESSION_FIELD_MAX_LEN} characters (got {len(value)})"
        )
    return value


def new_session_ctx_id() -> str:
    return f"ar-{uuid.uuid4().hex}"
