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

"""Slash-command registration, parsing, completion, and dispatch."""

import shlex
from dataclasses import dataclass
from typing import Callable, List, Sequence, Tuple

from ar_cli.interactive.lifecycle import delete_session, fork_session, new_session, quit_interactive
from ar_cli.interactive.query import show_history, show_sessions
from ar_cli.interactive.state import (
    CommandResult,
    InteractiveCommandError,
    InteractiveContext,
    InteractiveSessionState,
)


CommandHandler = Callable[[Sequence[str], InteractiveContext], CommandResult]


@dataclass(frozen=True)
class SlashCommand:
    name: str
    summary: str
    handler: CommandHandler


_COMMANDS: Tuple[SlashCommand, ...] = (
    SlashCommand("/sessions", "list and switch SessionCtx", show_sessions),
    SlashCommand("/history", "show Turns in the current SessionCtx", show_history),
    SlashCommand("/fork", "fork a completed Turn into a SessionCtx", fork_session),
    SlashCommand("/delete", "delete a non-current SessionCtx", delete_session),
    SlashCommand("/new", "switch to a new local SessionCtx", new_session),
    SlashCommand("/quit", "exit interactive mode", quit_interactive),
)
_COMMAND_BY_NAME = {command.name: command for command in _COMMANDS}

SLASH_COMMAND_COMPLETIONS: Tuple[Tuple[str, str], ...] = tuple(
    (command.name, command.summary) for command in _COMMANDS
)


def handle_session_command(
    line: str,
    *,
    state: InteractiveSessionState,
    client,
) -> CommandResult:
    """Compatibility entry point for executing one slash command."""
    return dispatch(line, InteractiveContext(state=state, client=client))


def dispatch(line: str, context: InteractiveContext) -> CommandResult:
    tokens = parse_command(line)
    command = _COMMAND_BY_NAME.get(tokens[0].lower())
    if command is None:
        raise InteractiveCommandError(f"unknown command: {tokens[0]}")
    return command.handler(tokens[1:], context)


def parse_command(line: str) -> List[str]:
    try:
        tokens = shlex.split(line)
    except ValueError as e:
        raise InteractiveCommandError(f"invalid command syntax: {e}")
    if not tokens:
        raise InteractiveCommandError("empty command")
    return tokens
