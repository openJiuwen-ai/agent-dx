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

"""Entry point for the `adx` CLI: root group, global options, command wiring."""

from typing import Optional

import click

from ar_cli import __version__
from ar_cli.commands import COMMANDS
from ar_cli.utils import print_logger, setup_logging


def _print_version(ctx: click.Context, param: click.Parameter, value: bool) -> None:
    if not value or ctx.resilient_parsing:
        return
    print_logger.info("adx version: %s", __version__)
    ctx.exit(0)


@click.group(context_settings={"help_option_names": ["-h", "--help"]})
@click.option(
    "-v",
    "--verbose",
    is_flag=True,
    help="Enable verbose logging (DEBUG level, prints request details).",
)
@click.option(
    "--version",
    is_flag=True,
    callback=_print_version,
    expose_value=False,
    is_eager=True,
    help="Show version and exit.",
)
@click.option(
    "--jwt-token",
    envvar="YR_JWT_TOKEN",
    default=None,
    help="JWT token for API authentication (sent in X-Auth header).",
)
@click.pass_context
def cli(ctx: click.Context, verbose: bool, jwt_token: Optional[str]) -> None:
    """Agent Distributed Executor (agent-dx) CLI.

    Deploy and invoke agents (functions). Use `-h/--help` on any command for
    detailed usage.

    Example usage:\n
      - adx deploy -s ./agent.json --server <META_SERVICE_ADDR>\n
      - adx exec --agent <AGENT> --server <FRONTEND_ADDR>
    """
    ctx.ensure_object(dict)
    ctx.obj["verbose"] = verbose
    ctx.obj["jwt_token"] = jwt_token
    setup_logging(verbose)


for _command in COMMANDS:
    cli.add_command(_command)


def main(cmdargs: Optional[list] = None) -> None:
    cli.main(args=cmdargs, prog_name="adx", standalone_mode=True)


if __name__ == "__main__":
    main()
