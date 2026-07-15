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

"""Function URN conversion for the public ar CLI surface."""

from typing import Optional, Tuple

import click

_FULL_URN_PREFIX = "sn:cn:yrk:default:function:"
_DEFAULT_VERSION = "latest"


def public_agent_to_function_version_urn(agent: str) -> str:
    """Convert public `0@namespace@func[:version]` input to a backend version URN."""
    triplet, version = _parse_public_agent(agent)
    return f"{_FULL_URN_PREFIX}{triplet}:{version}"


def function_urn_to_public_agent(urn: str) -> Optional[str]:
    """Convert a backend function/version URN to public `0@namespace@func[:version]`."""
    value = (urn or "").strip()
    if not value.startswith(_FULL_URN_PREFIX):
        return None

    body = value[len(_FULL_URN_PREFIX):]
    triplet, version = _split_version(body)
    if not _is_public_triplet(triplet):
        return None
    if version in (None, "", _DEFAULT_VERSION):
        return triplet
    return f"{triplet}:{version}"


def _parse_public_agent(agent: str) -> Tuple[str, str]:
    value = (agent or "").strip()
    triplet, version = _split_version(value)
    if not _is_public_triplet(triplet):
        raise click.BadParameter(
            "agent must use 0@namespace@funcname or 0@namespace@funcname:version format"
        )
    if version in (None, ""):
        version = _DEFAULT_VERSION
    return triplet, version


def _split_version(value: str) -> Tuple[str, Optional[str]]:
    if ":" not in value:
        return value, None
    triplet, version = value.rsplit(":", 1)
    return triplet, version


def _is_public_triplet(value: str) -> bool:
    parts = value.split("@")
    return len(parts) == 3 and parts[0] == "0" and bool(parts[1]) and bool(parts[2])
