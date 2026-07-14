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

"""Logging setup and small helpers shared across commands."""

import json
import logging
import os
import re
import sys
from typing import Any
from urllib.parse import urlparse

import click

from ar_cli.const import SESSION_FIELD_MAX_LEN

_SCHEME_RE = re.compile(r"^[a-zA-Z][a-zA-Z0-9+.-]*://")


def validate_session_field(ctx, param, value):
    """Click callback: reject session field values longer than the max length.

    Shared by --session-ctx / --session-id (and future resume/fork commands).
    ``None`` (option not provided) passes through unchanged.
    """
    if value is not None and len(value) > SESSION_FIELD_MAX_LEN:
        raise click.BadParameter(
            f"must be at most {SESSION_FIELD_MAX_LEN} characters (got {len(value)})"
        )
    return value


def validate_server(ctx, param, value):
    """Click callback: require a ``host:port`` address (scheme optional).

    Accepts a bare ``host:port`` or one carrying an http(s) scheme. Rejects a
    missing host or a missing/invalid port. Returns the value unchanged.
    """
    if value is None:
        return value
    parsed = urlparse(normalize_addr(value))
    try:
        port = parsed.port
    except ValueError:
        port = None
    if not parsed.hostname or not port:
        raise click.BadParameter(f"must be in host:port form, e.g. 127.0.0.1:31180 (got {value!r})")
    return value


def validate_non_empty(ctx, param, value):
    """Click callback: reject an empty / whitespace-only value."""
    if value is not None and value.strip() == "":
        raise click.BadParameter("must not be empty")
    return value

# Diagnostic logger -> stderr. The ar CLI never writes log files: it is a
# short-lived, stateless HTTP client. Users who want logs on disk redirect
# stderr (e.g. `ar exec ... 2> ar.log`).
_LOG_FORMAT = "%(asctime)s.%(msecs)03d | %(levelname)-7s | %(name)s:%(funcName)s:%(lineno)d - %(message)s"
_DATE_FORMAT = "%Y-%m-%d %H:%M:%S"

# Clean output logger -> stdout. Carries command results and the streamed SSE
# payloads, with no prefixes, so piping/redirecting stays clean.
print_logger = logging.getLogger("ar.print")
print_logger.setLevel(logging.INFO)
print_logger.propagate = False
_print_handler = logging.StreamHandler(sys.stdout)
_print_handler.setFormatter(logging.Formatter("%(message)s"))
print_logger.addHandler(_print_handler)


def setup_logging(verbose: bool) -> None:
    """Configure root logging to the console (stderr).

    DEBUG when ``verbose`` is set (shows request details), otherwise INFO.
    """
    logging.basicConfig(
        level=logging.DEBUG if verbose else logging.INFO,
        format=_LOG_FORMAT,
        datefmt=_DATE_FORMAT,
        force=True,
    )


def normalize_addr(addr: str) -> str:
    """Normalize a user-supplied address to a URL base.

    Users pass a bare ``host:port`` (no scheme); http is assumed and prepended.
    An address that already carries a scheme is left as-is.
    """
    addr = addr.strip()
    if not _SCHEME_RE.match(addr):
        addr = "http://" + addr
    return addr


def load_spec(spec_arg: str) -> Any:
    """Load a function spec from an inline JSON string or a JSON file path.

    A value that points to an existing file is read from disk; anything else is
    parsed as an inline JSON string. Invalid input raises ``click.BadParameter``
    so the process exits with the standard parameter-error code (2).
    """
    raw = spec_arg
    if os.path.isfile(spec_arg):
        try:
            with open(spec_arg, "r", encoding="utf-8") as f:
                raw = f.read()
        except OSError as e:
            raise click.BadParameter(f"failed to read spec file '{spec_arg}': {e}")
    try:
        return json.loads(raw)
    except json.JSONDecodeError as e:
        if os.path.isfile(spec_arg):
            raise click.BadParameter(f"spec file '{spec_arg}' does not contain valid JSON: {e}")
        raise click.BadParameter(
            f"spec is neither valid JSON nor an existing file: {_truncate(spec_arg)} ({e})"
        )


def parse_json_arg(value: str, label: str) -> Any:
    """Validate that ``value`` is a JSON document, raising ``BadParameter`` if not."""
    try:
        return json.loads(value)
    except json.JSONDecodeError as e:
        raise click.BadParameter(f"{label} is not valid JSON ({_truncate(value)}): {e}")


def _truncate(text: str, limit: int = 120) -> str:
    text = text.strip().replace("\n", " ")
    return text if len(text) <= limit else text[:limit] + "..."
