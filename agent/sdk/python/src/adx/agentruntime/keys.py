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

"""Deterministic DataSystem keys."""

import hashlib
import json


def _hash_parts(*parts: str) -> str:
    canonical = json.dumps(parts, ensure_ascii=False, separators=(",", ":"))
    # Go encoding/json always escapes the JavaScript line/paragraph separators,
    # even with HTML escaping disabled. Mirror that behavior without escaping
    # ordinary non-ASCII text.
    canonical = canonical.replace("\u2028", "\\u2028").replace("\u2029", "\\u2029")
    encoded = canonical.encode("utf-8")
    # Keep DataSystem keys compact while retaining 128 bits of collision resistance.
    return hashlib.sha256(encoded).hexdigest()[:32]


class SessionKeys:
    def __init__(
        self,
        tenant_id: str,
        function_name: str,
        function_version: str,
        session_context_id: str,
    ):
        self._prefix = "ar:s:{}:{}".format(
            _hash_parts(tenant_id, function_name, function_version),
            _hash_parts(session_context_id),
        )

    def turn(self, index: int) -> str:
        return f"{self._prefix}:t{index}"

    def event(self, seq: int) -> str:
        return f"{self._prefix}:e{seq}"
