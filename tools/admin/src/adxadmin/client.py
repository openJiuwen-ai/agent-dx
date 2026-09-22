"""Typed boundary over the public ADX administration HTTP API."""

from dataclasses import dataclass
import ipaddress
import re
from typing import Any
from urllib.parse import urlsplit
import uuid

import httpx

from .errors import ApiError, InvalidInput


KEY_PATH = "/api/admin/v1/keys"
KEY_ID = re.compile(r"^[0-9a-fA-F]{64}$")


@dataclass
class ClientOptions:
    endpoint: str
    token: str
    timeout_seconds: float = 30
    ca_file: str | None = None
    allow_loopback_http: bool = False


class AdminClient:
    def __init__(
        self,
        options: ClientOptions,
        *,
        transport: httpx.BaseTransport | None = None,
    ) -> None:
        self._endpoint = _validate_endpoint(
            options.endpoint, options.allow_loopback_http
        )
        _validate_secret(options.token, "administrator API Key")
        if options.timeout_seconds <= 0:
            raise InvalidInput("request timeout must be positive")
        self._token = options.token
        self._client = httpx.Client(
            base_url=self._endpoint,
            timeout=options.timeout_seconds,
            verify=options.ca_file or True,
            follow_redirects=False,
            transport=transport,
        )

    def __enter__(self) -> "AdminClient":
        return self

    def __exit__(self, *_unused: object) -> None:
        self.close()

    def close(self) -> None:
        self._client.close()

    def create_key(self, tenant: str, expires_at: int = 0) -> dict[str, Any]:
        if (
            not tenant.strip()
            or len(tenant) > 256
            or any(ord(character) < 32 or ord(character) == 127 for character in tenant)
        ):
            raise InvalidInput("tenant must be nonempty, bounded and contain no controls")
        if expires_at < 0:
            raise InvalidInput("expiry must be a Unix timestamp or zero")
        response = self._request(
            "POST",
            KEY_PATH,
            json={
                "tenantId": tenant,
                "expiresAtUnixSeconds": expires_at,
            },
        )
        value = self._json(response, 201)
        key = _metadata(value.get("key"))
        if key["tenantId"] != tenant:
            raise InvalidInput("create response tenant does not match the request")
        secret = value.get("apiKey")
        if not isinstance(secret, str):
            raise InvalidInput("server returned an invalid API Key secret")
        _validate_secret(secret, "server API Key")
        return {"key": key, "apiKey": secret}

    def list_keys(
        self,
        *,
        tenant: str | None = None,
        page_size: int = 100,
        page_token: str | None = None,
    ) -> dict[str, Any]:
        if not 1 <= page_size <= 1000:
            raise InvalidInput("page size must be between 1 and 1000")
        parameters: dict[str, str | int] = {"pageSize": page_size}
        if tenant:
            parameters["tenantId"] = tenant
        if page_token:
            parameters["pageToken"] = page_token
        value = self._json(self._request("GET", KEY_PATH, params=parameters), 200)
        items = value.get("items")
        next_page_token = value.get("nextPageToken")
        if not isinstance(items, list) or not isinstance(next_page_token, str):
            raise InvalidInput("server returned an invalid Key page")
        metadata = [_metadata(item) for item in items]
        if tenant and any(item["tenantId"] != tenant for item in metadata):
            raise InvalidInput("list response contains another tenant")
        return {"items": metadata, "nextPageToken": next_page_token}

    def revoke_key(self, key_id: str) -> None:
        if not KEY_ID.fullmatch(key_id):
            raise InvalidInput("Key ID must be a 64-character hexadecimal digest")
        self._expect(self._request("DELETE", f"{KEY_PATH}/{key_id}"), 204)

    def _request(self, method: str, path: str, **kwargs: Any) -> httpx.Response:
        headers = {
            "Authorization": f"Bearer {self._token}",
            "X-Request-ID": str(uuid.uuid4()),
        }
        try:
            return self._client.request(method, path, headers=headers, **kwargs)
        except httpx.HTTPError as error:
            raise InvalidInput(f"HTTP request failed: {error}") from error

    @staticmethod
    def _expect(response: httpx.Response, status: int) -> None:
        if response.status_code != status:
            raise _api_error(response)

    @classmethod
    def _json(cls, response: httpx.Response, status: int) -> dict[str, Any]:
        cls._expect(response, status)
        try:
            value = response.json()
        except ValueError as error:
            raise InvalidInput("server returned invalid JSON") from error
        if not isinstance(value, dict):
            raise InvalidInput("server returned invalid JSON")
        return value


def _validate_endpoint(endpoint: str, allow_loopback_http: bool) -> str:
    parsed = urlsplit(endpoint)
    if (
        not parsed.hostname
        or parsed.username
        or parsed.password
        or parsed.query
        or parsed.fragment
        or parsed.path not in ("", "/")
    ):
        raise InvalidInput(
            "endpoint must be an origin URL without credentials, path, query or fragment"
        )
    if parsed.scheme == "https":
        return endpoint.rstrip("/")
    if parsed.scheme == "http" and allow_loopback_http and _is_loopback(parsed.hostname):
        return endpoint.rstrip("/")
    if parsed.scheme == "http":
        raise InvalidInput(
            "plaintext endpoint is permitted only for loopback with --allow-loopback-http"
        )
    raise InvalidInput("endpoint must use https")


def _is_loopback(host: str) -> bool:
    if host.lower() == "localhost":
        return True
    try:
        return ipaddress.ip_address(host).is_loopback
    except ValueError:
        return False


def _metadata(value: object) -> dict[str, Any]:
    if not isinstance(value, dict):
        raise InvalidInput("server returned invalid Key metadata")
    key_id = value.get("id")
    tenant = value.get("tenantId")
    expires_at = value.get("expiresAtUnixSeconds")
    if (
        not isinstance(key_id, str)
        or not KEY_ID.fullmatch(key_id)
        or not isinstance(tenant, str)
        or not tenant.strip()
        or len(tenant) > 256
        or any(ord(character) < 32 or ord(character) == 127 for character in tenant)
        or not isinstance(expires_at, int)
        or isinstance(expires_at, bool)
        or expires_at < 0
    ):
        raise InvalidInput("server returned invalid Key metadata")
    return {
        "id": key_id,
        "tenantId": tenant,
        "expiresAtUnixSeconds": expires_at,
    }


def _validate_secret(secret: str, kind: str) -> None:
    if not 32 <= len(secret) <= 512 or any(
        ord(character) < 33 or ord(character) == 127 for character in secret
    ):
        raise InvalidInput(f"{kind} must contain 32..512 non-whitespace characters")


def _api_error(response: httpx.Response) -> ApiError:
    try:
        body = response.json()
    except ValueError:
        body = {}
    if not isinstance(body, dict):
        body = {}
    detail = body.get("error")
    if not isinstance(detail, dict):
        detail = {}
    request_id = detail.get("requestId") or response.headers.get("x-request-id") or "unknown"
    return ApiError(
        response.status_code,
        str(body.get("message") or "request failed"),
        code=str(detail.get("code") or "UNKNOWN"),
        retry=str(detail.get("retry") or "UNKNOWN"),
        outcome=str(detail.get("outcome") or "UNKNOWN"),
        request_id=str(request_id),
    )
