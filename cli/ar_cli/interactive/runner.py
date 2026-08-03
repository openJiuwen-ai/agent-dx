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

"""Interactive ar exec loop."""

import json
import logging
import uuid
from typing import Optional

import click

from ar_cli.api.function_invocation import InvocationOptions, invoke_stream
from ar_cli.const import DEFAULT_INTERACTIVE_SESSION_TTL, RELEASE_SESSION_TTL
from ar_cli.errors import ArError
from ar_cli.interactive.lifecycle import new_session_ctx_id
from ar_cli.interactive.lineedit import read_line
from ar_cli.interactive.registry import SLASH_COMMAND_COMPLETIONS, handle_session_command
from ar_cli.interactive.state import CommandResult, InteractiveCommandError, InteractiveSessionState
from ar_cli.utils import print_logger


logger = logging.getLogger(__name__)


def run_interactive(
    *,
    client,
    server: str,
    agent: str,
    session_ctx: Optional[str],
    session_id: Optional[str],
    session_ttl: Optional[int],
    concurrency: Optional[int],
) -> None:
    if session_id is None:
        session_id = _new_instance_session_id()
    if session_ttl is None:
        session_ttl = DEFAULT_INTERACTIVE_SESSION_TTL
    state = InteractiveSessionState(
        server_url=server,
        agent_urn=agent,
        session_ctx=session_ctx if session_ctx is not None else new_session_ctx_id(),
    )
    click.echo("Entering interactive mode. Type /quit to quit.", err=True)
    click.echo(f"session-ctx: {state.session_ctx}", err=True)
    click.echo(f"session-id: {session_id}", err=True)

    session_invoked = False
    try:
        while True:
            line = read_line(f"[{state.session_ctx}] > ", completions=SLASH_COMMAND_COMPLETIONS)
            if line is None:
                click.echo("", err=True)
                return
            text = line.strip()
            if not text:
                continue
            if text.startswith("/"):
                previous_session_ctx = state.session_ctx
                previous_session_id = session_id
                try:
                    result = handle_session_command(line, state=state, client=client)
                except (InteractiveCommandError, ArError) as e:
                    _print_interactive_error(e)
                    continue
                if state.session_ctx != previous_session_ctx:
                    if session_invoked:
                        _release_interactive_session(
                            client,
                            server,
                            agent,
                            session_ctx=previous_session_ctx,
                            session_id=previous_session_id,
                            concurrency=concurrency,
                        )
                    session_id = _new_instance_session_id()
                    session_invoked = False
                    click.echo(f"session-id: {session_id}", err=True)
                if result is CommandResult.EXIT:
                    return
                continue

            options = InvocationOptions(
                session_ctx=state.session_ctx,
                session_id=session_id,
                session_ttl=session_ttl,
                concurrency=concurrency,
            )
            session_invoked = True
            try:
                for payload in invoke_stream(
                    client,
                    server,
                    agent,
                    headers=options.to_headers(),
                    body=json.dumps({"message": line}, ensure_ascii=False),
                ):
                    print_logger.info("%s", payload)
            except ArError as e:
                _print_interactive_error(e)
    finally:
        if session_invoked:
            _release_interactive_session(
                client,
                server,
                agent,
                session_ctx=state.session_ctx,
                session_id=session_id,
                concurrency=concurrency,
            )


def _new_instance_session_id() -> str:
    return f"ar-{uuid.uuid4().hex}"


def _release_interactive_session(
    client,
    server: str,
    agent: str,
    *,
    session_ctx: str,
    session_id: str,
    concurrency: Optional[int],
) -> None:
    options = InvocationOptions(
        session_ctx=session_ctx,
        session_id=session_id,
        session_ttl=RELEASE_SESSION_TTL,
        concurrency=concurrency,
    )
    try:
        for _ in invoke_stream(
            client,
            server,
            agent,
            headers=options.to_headers(),
            body="{}",
        ):
            pass
    except Exception as e:
        logger.warning("failed to release interactive session %s: %s", session_id, e)


def _print_interactive_error(error: Exception) -> None:
    click.echo(f"Error: {error}", err=True)
