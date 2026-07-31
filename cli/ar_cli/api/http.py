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

"""Shared HTTP request and response handling for the CLI API adapter."""

import logging
from typing import Any, Dict, Optional

import requests

from ar_cli.const import HEADER_AUTH
from ar_cli.errors import ApiError, NetworkError


logger = logging.getLogger(__name__)


class HttpClient:
    """Small requests wrapper with uniform JSON and streaming error handling."""

    def __init__(
        self,
        timeout: Optional[float] = None,
        *,
        jwt_token: Optional[str] = None,
    ) -> None:
        self._session = requests.Session()
        self._timeout = timeout
        self._default_headers = {HEADER_AUTH: jwt_token} if jwt_token else {}

    @property
    def session(self):
        return self._session

    @session.setter
    def session(self, value) -> None:
        self._session = value

    def request_json(
        self,
        method: str,
        url: str,
        *,
        action: str,
        headers: Optional[Dict[str, str]] = None,
        body: Optional[str] = None,
        params: Optional[Dict[str, Any]] = None,
    ) -> Dict[str, Any]:
        request_headers = self._request_headers(headers)
        logger.debug(
            "%s %s\nheaders=%s\nparams=%s\nbody=%s",
            method,
            url,
            _redact_headers(request_headers),
            params,
            body,
        )
        try:
            response = self._session.request(
                method,
                url,
                params=params,
                data=body,
                headers=request_headers,
                timeout=self._timeout,
            )
        except requests.RequestException as e:
            raise NetworkError(f"failed to reach server at {url}: {e}")

        try:
            if not response.ok:
                raise build_api_error(action, response)
            try:
                payload = response.json()
            except ValueError:
                raise ApiError(
                    f"{action} returned a non-JSON response: {brief(response.text)}",
                    status_code=response.status_code,
                )
            if not isinstance(payload, dict):
                raise ApiError(
                    f"{action} returned a non-object JSON response",
                    status_code=response.status_code,
                )
            code = payload.get("code")
            if code not in (None, 0):
                raise ApiError(
                    f"{action} failed: code={code} message={payload.get('message')}",
                    status_code=response.status_code,
                    service_code=code,
                )
            return payload
        finally:
            response.close()

    def request_no_content(
        self,
        method: str,
        url: str,
        *,
        action: str,
        headers: Optional[Dict[str, str]] = None,
        params: Optional[Dict[str, Any]] = None,
    ) -> None:
        """Send a request whose successful response intentionally has no body."""
        request_headers = self._request_headers(headers)
        logger.debug(
            "%s %s\nheaders=%s\nparams=%s",
            method,
            url,
            _redact_headers(request_headers),
            params,
        )
        try:
            response = self._session.request(
                method,
                url,
                params=params,
                headers=request_headers,
                timeout=self._timeout,
            )
        except requests.RequestException as e:
            raise NetworkError(f"failed to reach server at {url}: {e}")

        try:
            if not response.ok:
                raise build_api_error(action, response)
        finally:
            response.close()

    def post_stream(self, url: str, *, headers: Dict[str, str], body: Optional[str], action: str):
        """Open a UTF-8 streaming response. The caller closes the response."""
        request_headers = self._request_headers(headers)
        logger.debug(
            "POST %s\nheaders=%s\nbody=%s",
            url,
            _redact_headers(request_headers),
            body,
        )
        try:
            response = self._session.post(
                url,
                data=body,
                headers=request_headers,
                stream=True,
                timeout=self._timeout,
            )
        except requests.RequestException as e:
            raise NetworkError(f"failed to reach server at {url}: {e}")

        response.encoding = "utf-8"
        if not response.ok:
            try:
                raise build_api_error(action, response)
            finally:
                response.close()
        return response

    def _request_headers(self, headers: Optional[Dict[str, str]]) -> Dict[str, str]:
        request_headers = dict(self._default_headers)
        request_headers.update(headers or {})
        return request_headers


def join_url(addr: str, path: str) -> str:
    """Join a base address and an API path, tolerating a trailing slash."""
    return f"{addr.rstrip('/')}{path}"


def brief(text: Optional[str], limit: int = 300) -> str:
    if text is None:
        return ""
    value = text.strip()
    return value if len(value) <= limit else value[:limit] + "..."


def _redact_headers(headers: Dict[str, str]) -> Dict[str, str]:
    return {
        key: "<redacted>" if key.lower() == HEADER_AUTH.lower() else value
        for key, value in headers.items()
    }


def build_api_error(action: str, response) -> ApiError:
    """Preserve HTTP and service codes for callers that handle API states."""
    service_code = None
    message = brief(response.text)
    try:
        payload = response.json()
    except ValueError:
        payload = None
    if isinstance(payload, dict):
        service_code = payload.get("code")
        message = payload.get("message") or message
    return ApiError(
        f"{action} failed: HTTP {response.status_code} {message}",
        status_code=response.status_code,
        service_code=service_code,
    )
