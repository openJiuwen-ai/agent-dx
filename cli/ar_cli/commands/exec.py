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

"""`ar exec` — invoke an agent (function) and stream the SSE response."""

import json
import logging
import uuid
from typing import Optional

import click

from ar_cli.client import AgentRuntimeClient
from ar_cli.const import DEFAULT_INTERACTIVE_SESSION_TTL, RELEASE_SESSION_TTL
from ar_cli.errors import ArError
from ar_cli.lineedit import read_line
from ar_cli.session import build_invocation_headers
from ar_cli.sse import stream_sse
from ar_cli.utils import (
    normalize_addr,
    parse_json_arg,
    print_logger,
    validate_non_empty,
    validate_server,
    validate_session_field,
)

logger = logging.getLogger(__name__)

_INTERACTIVE_EXIT_COMMANDS = {"/exit", "/quit"}


@click.command(
    name="exec",
    help="""Invoke an agent (function) and stream its SSE response.

Only --agent and --server are required. Passing --args invokes once with that
JSON body. Omitting --args enters interactive mode; each line is sent as
{"message": "<line>"}. Session headers are sent only when their options are
provided, except interactive mode auto-generates session values when omitted.

Example:\n
  ar exec --agent <URN> --server <FRONTEND_ADDR> --args '{"param1":"hi"}'
""",
)
@click.option(
    "--agent",
    required=True,
    callback=validate_non_empty,
    help="functionVersionUrn of the agent to invoke.",
)
@click.option(
    "--server",
    required=True,
    callback=validate_server,
    help="frontend address as host:port, e.g. 127.0.0.1:31180 (http is assumed, no scheme needed).",
)
@click.option(
    "--session-ctx",
    default=None,
    callback=validate_session_field,
    help="Agent session context; sets the X-Session-Context header (max 63 chars).",
)
@click.option(
    "--session-id",
    default=None,
    callback=validate_session_field,
    help="Instance session id; sets the X-Instance-Session header (max 63 chars).",
)
@click.option(
    "--session-ttl",
    type=click.IntRange(min=1),
    default=None,
    help="Instance session TTL (default: 90, must be > 0). Only used with --session-id.",
)
@click.option(
    "--concurrency",
    type=click.IntRange(min=1),
    default=None,
    help="Instance session concurrency (default: 1, must be > 0). Only used with --session-id.",
)
@click.option(
    "--args",
    "args",
    default=None,
    help="Handler arguments as a JSON string. Omit to enter interactive mode.",
)
@click.pass_context
def exec_cmd(
    ctx: click.Context,
    agent: str,
    server: str,
    session_ctx: str,
    session_id: str,
    session_ttl: int,
    concurrency: int,
    args: str,
) -> None:
    # session-ttl / concurrency are meaningless without a session id.
    if args is not None and session_id is None and (session_ttl is not None or concurrency is not None):
        raise click.UsageError("--session-ttl/--concurrency require --session-id; nothing was sent")

    client = AgentRuntimeClient()
    server_url = normalize_addr(server)

    try:
        if args is None:
            _run_interactive(
                client=client,
                server=server_url,
                agent=agent,
                session_ctx=session_ctx,
                session_id=session_id,
                session_ttl=session_ttl,
                concurrency=concurrency,
            )
            ctx.exit(0)

        parse_json_arg(args, "--args")  # validate early; exit code 2 on bad JSON
        headers = build_invocation_headers(
            session_ctx=session_ctx,
            session_id=session_id,
            session_ttl=session_ttl,
            concurrency=concurrency,
        )
        _invoke_once(client, server_url, agent, headers=headers, body=args)
    except ArError as e:
        logger.error("%s", e)
        ctx.exit(e.exit_code)
    ctx.exit(0)


def _run_interactive(
    *,
    client: AgentRuntimeClient,
    server: str,
    agent: str,
    session_ctx: Optional[str],
    session_id: Optional[str],
    session_ttl: Optional[int],
    concurrency: Optional[int],
) -> None:
    if session_ctx is None:
        session_ctx = f"ar-{uuid.uuid4().hex}"
    if session_id is None:
        session_id = f"ar-{uuid.uuid4().hex}"
    if session_ttl is None:
        session_ttl = DEFAULT_INTERACTIVE_SESSION_TTL

    click.echo("Entering interactive mode. Type /exit or /quit to quit.", err=True)
    click.echo(f"session-ctx: {session_ctx}", err=True)
    click.echo(f"session-id: {session_id}", err=True)

    invoked = False
    try:
        while True:
            line = _read_interactive_line()
            if line is None:
                click.echo("", err=True)
                return

            text = line.strip()
            if not text:
                continue
            if text.lower() in _INTERACTIVE_EXIT_COMMANDS:
                return

            body = json.dumps({"message": line}, ensure_ascii=False)
            headers = build_invocation_headers(
                session_ctx=session_ctx,
                session_id=session_id,
                session_ttl=session_ttl,
                concurrency=concurrency,
            )
            invoked = True
            _invoke_once(client, server, agent, headers=headers, body=body)
    finally:
        if invoked:
            _release_interactive_session(
                client=client,
                server=server,
                agent=agent,
                session_ctx=session_ctx,
                session_id=session_id,
                concurrency=concurrency,
            )


def _read_interactive_line() -> Optional[str]:
    # read_line() enables cursor/Home/End editing via readline when available,
    # else a built-in raw-mode editor on a TTY, else plain input().
    return read_line("yrar> ")


def _invoke_once(
    client: AgentRuntimeClient,
    server: str,
    agent: str,
    *,
    headers: dict,
    body: str,
) -> None:
    resp = client.invoke(server, agent, headers=headers, body=body)
    with resp:
        for payload in stream_sse(resp):
            print_logger.info("%s", payload)


def _release_interactive_session(
    *,
    client: AgentRuntimeClient,
    server: str,
    agent: str,
    session_ctx: str,
    session_id: str,
    concurrency: Optional[int],
) -> None:
    headers = build_invocation_headers(
        session_ctx=session_ctx,
        session_id=session_id,
        session_ttl=RELEASE_SESSION_TTL,
        concurrency=concurrency,
    )
    try:
        resp = client.invoke(server, agent, headers=headers, body="{}")
        with resp:
            for _ in stream_sse(resp):
                pass
    except Exception as e:
        logger.warning("failed to release interactive session %s: %s", session_id, e)
