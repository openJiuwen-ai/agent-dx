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

"""`adx exec` — invoke an agent (function) and stream the SSE response."""

import logging

import click

from ar_cli.api.client import AgentRuntimeClient
from ar_cli.api.function_invocation import InvocationOptions, invoke_stream
from ar_cli.errors import ArError
from ar_cli.interactive.lifecycle import new_session_ctx_id
from ar_cli.interactive.runner import run_interactive
from ar_cli.urn import public_agent_to_function_version_urn
from ar_cli.utils import (
    normalize_addr,
    parse_json_arg,
    print_logger,
    validate_non_empty,
    validate_server,
    validate_session_field,
)

logger = logging.getLogger(__name__)


@click.command(
    name="exec",
    help="""Invoke an agent (function) and stream its SSE response.

Only --agent and --server are required. Passing --args invokes once with that
JSON body. Omitting --args enters interactive mode; each line is sent as
{"message": "<line>"}. SessionCtx is auto-generated when omitted. Interactive
mode also auto-generates InstanceSession values when omitted.

Example:\n
  adx exec --agent <AGENT> --server <FRONTEND_ADDR> --args '{"param1":"hi"}'
""",
)
@click.option(
    "--agent",
    required=True,
    callback=validate_non_empty,
    help="Agent name as 0@default@funcname[:version].",
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
    help="Instance session TTL (default: 90; 600 in interactive mode, must be > 0). Only used with --session-id.",
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
    # session-ttl / concurrency are meaningless without a user-provided session id.
    if session_id is None and (session_ttl is not None or concurrency is not None):
        raise click.UsageError("--session-ttl/--concurrency require --session-id; nothing was sent")

    client = AgentRuntimeClient(jwt_token=ctx.obj.get("jwt_token"))
    server_url = normalize_addr(server)
    agent_urn = public_agent_to_function_version_urn(agent)

    try:
        if args is None:
            run_interactive(
                client=client,
                server=server_url,
                agent=agent_urn,
                session_ctx=session_ctx,
                session_id=session_id,
                session_ttl=session_ttl,
                concurrency=concurrency,
            )
            ctx.exit(0)

        parse_json_arg(args, "--args")  # validate early; exit code 2 on bad JSON
        invocation_session_ctx = session_ctx if session_ctx is not None else new_session_ctx_id()
        options = InvocationOptions(
            session_ctx=invocation_session_ctx,
            session_id=session_id,
            session_ttl=session_ttl,
            concurrency=concurrency,
        )
        for payload in invoke_stream(
            client,
            server_url,
            agent_urn,
            headers=options.to_headers(),
            body=args,
        ):
            print_logger.info("%s", payload)
    except ArError as e:
        logger.error("%s", e)
        ctx.exit(e.exit_code)
    ctx.exit(0)
