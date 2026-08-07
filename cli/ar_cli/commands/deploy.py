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

"""`adx deploy` — register an agent (function) via meta_service."""

import logging

import click

from ar_cli.api.client import AgentRuntimeClient
from ar_cli.const import ENABLE_SESSION_CTX_KEY
from ar_cli.errors import ArError
from ar_cli.urn import function_urn_to_public_agent
from ar_cli.utils import load_spec, normalize_addr, print_logger, validate_server

logger = logging.getLogger(__name__)


@click.command(
    name="deploy",
    help="""Register an agent (function) with meta_service.

The spec accepts either an inline JSON string or a path to a JSON file. When the
spec omits `enableSessionCtx`, it defaults to true.

Example:\n
  adx deploy -s ./agent.json --server <META_SERVICE_ADDR>
""",
)
@click.option(
    "-s",
    "--spec",
    required=True,
    help="Function definition: an inline JSON string or a path to a JSON file.",
)
@click.option(
    "--server",
    required=True,
    callback=validate_server,
    help="meta_service address as host:port, e.g. 127.0.0.1:31182 (http is assumed, no scheme needed).",
)
@click.pass_context
def deploy(ctx: click.Context, spec: str, server: str) -> None:
    function_spec = load_spec(spec)
    if not isinstance(function_spec, dict):
        raise click.BadParameter("spec must be a JSON object")

    # Inject enableSessionCtx=true only when the user did not set it. Operate on
    # the parsed dict (not the raw string): key presence decides "was it set",
    # so an explicit `false` is preserved.
    if ENABLE_SESSION_CTX_KEY not in function_spec:
        function_spec[ENABLE_SESSION_CTX_KEY] = True

    client = AgentRuntimeClient(jwt_token=ctx.obj.get("jwt_token"))
    try:
        result = client.register_function(normalize_addr(server), function_spec)
    except ArError as e:
        logger.error("%s", e)
        ctx.exit(e.exit_code)

    function = result.get("function", {}) if isinstance(result, dict) else {}
    urn = function.get("functionVersionUrn") or function.get("functionUrn")
    public_agent = function_urn_to_public_agent(urn)
    if public_agent:
        print_logger.info("Deployed. agent: %s", public_agent)
    elif urn:
        logger.warning("Deploy succeeded but function URN is not in public 0@default@funcname format")
        print_logger.info("Deployed.")
    else:
        logger.warning("Deploy succeeded but no functionVersionUrn found in response")
        print_logger.info("Deployed.")
    ctx.exit(0)
