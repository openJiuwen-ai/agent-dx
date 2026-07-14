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

"""HTTP client wrapping the meta_service and frontend endpoints.

A single place for "which service, which URL, which headers", so the
invocation path is shared by exec and the future resume / fork commands.
"""

import json
import logging
from typing import Any, Dict, Optional

import requests

from ar_cli.const import FUNCTIONS_PATH, INVOCATIONS_PATH
from ar_cli.errors import ApiError, NetworkError

logger = logging.getLogger(__name__)


class AgentRuntimeClient:
    """Thin client over the openYuanrong FaaS HTTP APIs (http only)."""

    def __init__(self, timeout: Optional[float] = None) -> None:
        self._session = requests.Session()
        self._timeout = timeout

    def register_function(self, meta_addr: str, spec: Dict[str, Any]) -> Dict[str, Any]:
        """Register (deploy) a function via meta_service.

        POST {meta_addr}/serverless/v1/functions with the spec as the body.
        Returns the parsed JSON response. Raises ``ApiError`` on a non-2xx
        status or a non-zero business ``code``; ``NetworkError`` if unreachable.
        """
        url = _join(meta_addr, FUNCTIONS_PATH)
        body = json.dumps(spec)
        logger.debug("POST %s\nheaders=%s\nbody=%s", url, {"Content-Type": "application/json"}, body)

        try:
            resp = self._session.post(
                url,
                data=body,
                headers={"Content-Type": "application/json"},
                timeout=self._timeout,
            )
        except requests.RequestException as e:
            raise NetworkError(f"failed to reach meta_service at {url}: {e}")

        if not resp.ok:
            raise ApiError(f"deploy failed: HTTP {resp.status_code} {_brief(resp.text)}")

        try:
            payload = resp.json()
        except ValueError:
            raise ApiError(f"deploy returned a non-JSON response: {_brief(resp.text)}")

        code = payload.get("code")
        if code not in (None, 0):
            raise ApiError(f"deploy failed: code={code} message={payload.get('message')}")
        return payload

    def invoke(
        self,
        server: str,
        urn: str,
        *,
        headers: Dict[str, str],
        body: Optional[str],
    ) -> requests.Response:
        """Invoke a function via the frontend, returning a streaming response.

        The response is opened with ``stream=True`` so the caller can consume
        SSE incrementally. Raises ``ApiError`` on a non-2xx status,
        ``NetworkError`` if the server is unreachable.
        """
        url = _join(server, INVOCATIONS_PATH.format(urn=urn))
        logger.debug("POST %s\nheaders=%s\nbody=%s", url, headers, body)

        try:
            resp = self._session.post(
                url,
                data=body,
                headers=headers,
                stream=True,
                timeout=self._timeout,
            )
        except requests.RequestException as e:
            raise NetworkError(f"failed to reach frontend at {url}: {e}")

        # SSE is UTF-8 by spec. requests defaults a text/* response without an
        # explicit charset to ISO-8859-1, which would mojibake non-ASCII (e.g.
        # Chinese) payloads when decoded; force UTF-8 for both streaming and the
        # error-body text below.
        resp.encoding = "utf-8"

        if not resp.ok:
            text = _brief(resp.text)
            resp.close()
            raise ApiError(f"invoke failed: HTTP {resp.status_code} {text}")
        return resp


def _join(addr: str, path: str) -> str:
    """Join a base address and an API path, tolerating a trailing slash."""
    return f"{addr.rstrip('/')}{path}"


def _brief(text: str, limit: int = 300) -> str:
    if text is None:
        return ""
    text = text.strip()
    return text if len(text) <= limit else text[:limit] + "..."
