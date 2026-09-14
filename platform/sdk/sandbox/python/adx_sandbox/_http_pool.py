"""Process-local shared HTTPX clients for sandbox control-plane traffic."""

import atexit
import http.cookiejar
import os
import threading
from collections.abc import Mapping
from dataclasses import dataclass
from typing import Any, Callable, Union
from urllib.parse import urlsplit

import httpx

_DEFAULT_MAX_CONNECTIONS = 256
_DEFAULT_MAX_KEEPALIVE_CONNECTIONS = 128
_DEFAULT_KEEPALIVE_EXPIRY = 30.0


class SandboxClientClosedError(RuntimeError):
    """Raised when a request uses a closed SandboxClient lease."""


class _RejectAllCookiesPolicy(http.cookiejar.DefaultCookiePolicy):
    """Prevent a shared client from persisting response cookies."""

    def set_ok(self, cookie, request) -> bool:
        return False

    def return_ok(self, cookie, request) -> bool:
        return False


@dataclass
class _PoolEntry:
    client: httpx.Client
    references: int = 0


_TargetKey = tuple[str, str, bool]
_PoolKey = tuple[int, str, str, bool]


def _positive_int_env(name: str, default: int) -> int:
    raw = os.environ.get(name, "").strip()
    if not raw:
        return default
    value = int(raw)
    if value <= 0:
        raise ValueError(f"{name} must be a positive integer")
    return value


def _positive_float_env(name: str, default: float) -> float:
    raw = os.environ.get(name, "").strip()
    if not raw:
        return default
    value = float(raw)
    if value <= 0:
        raise ValueError(f"{name} must be greater than zero")
    return value


def _new_http_client(verify_tls: bool) -> httpx.Client:
    max_connections = _positive_int_env(
        "ADX_HTTP_MAX_CONNECTIONS", _DEFAULT_MAX_CONNECTIONS
    )
    max_keepalive_connections = _positive_int_env(
        "ADX_HTTP_MAX_KEEPALIVE_CONNECTIONS",
        _DEFAULT_MAX_KEEPALIVE_CONNECTIONS,
    )
    if max_keepalive_connections > max_connections:
        raise ValueError(
            "ADX_HTTP_MAX_KEEPALIVE_CONNECTIONS must be less than or equal to "
            "ADX_HTTP_MAX_CONNECTIONS"
        )
    keepalive_expiry = _positive_float_env(
        "ADX_HTTP_KEEPALIVE_EXPIRY", _DEFAULT_KEEPALIVE_EXPIRY
    )
    cookie_jar = http.cookiejar.CookieJar(policy=_RejectAllCookiesPolicy())
    return httpx.Client(
        verify=verify_tls,
        timeout=None,
        limits=httpx.Limits(
            max_connections=max_connections,
            max_keepalive_connections=max_keepalive_connections,
            keepalive_expiry=keepalive_expiry,
        ),
        cookies=cookie_jar,
    )


class _SharedHTTPClientRegistry:
    """Own shared HTTP clients without carrying per-Sandbox authentication."""

    def __init__(self) -> None:
        self._pid = os.getpid()
        self._entries: dict[_TargetKey, _PoolEntry] = {}
        self._lock = threading.RLock()

    def acquire(
        self,
        scheme: str,
        server: str,
        verify_tls: bool,
        token: Union[str, Callable[[], str]],
    ) -> "_SharedHTTPClientLease":
        target = (scheme, server, verify_tls)
        pid, client = self._acquire_target(target)
        return _SharedHTTPClientLease(self, target, pid, client, token)

    def _acquire_target(self, target: _TargetKey) -> tuple[int, httpx.Client]:
        with self._lock:
            self._reset_after_fork_locked()
            entry = self._entries.get(target)
            if entry is None:
                entry = _PoolEntry(client=_new_http_client(target[2]))
                self._entries[target] = entry
            entry.references += 1
            return self._pid, entry.client

    def release(self, target: _TargetKey, lease_pid: int) -> None:
        with self._lock:
            self._reset_after_fork_locked()
            if lease_pid != self._pid:
                return
            entry = self._entries.get(target)
            if entry is not None and entry.references > 0:
                entry.references -= 1

    def close_all(self) -> None:
        with self._lock:
            self._reset_after_fork_locked()
            entries = list(self._entries.values())
            self._entries.clear()
        for entry in entries:
            try:
                entry.client.close()
            except (OSError, RuntimeError):
                # Process teardown must continue closing the remaining pools.
                pass

    def snapshot(self) -> dict[_PoolKey, tuple[httpx.Client, int]]:
        """Return pool identities and reference counts for diagnostics/tests."""
        with self._lock:
            self._reset_after_fork_locked()
            return {
                (self._pid, *target): (entry.client, entry.references)
                for target, entry in self._entries.items()
            }

    def _reset_after_fork_locked(self) -> None:
        current_pid = os.getpid()
        if current_pid == self._pid:
            return
        # Never use or close inherited HTTPX clients in the child. Their
        # transports and locks were created in the parent process. Existing
        # leases lazily acquire a child-local client before their next request.
        self._pid = current_pid
        self._entries = {}
        self._lock = threading.RLock()

    def _after_fork_in_child(self) -> None:
        """Reset inherited synchronization and pool state without taking locks."""
        self._pid = os.getpid()
        self._entries = {}
        self._lock = threading.RLock()


class _SharedHTTPClientLease:
    """Per-Sandbox authentication and lifecycle over a shared HTTP client."""

    def __init__(
        self,
        registry: _SharedHTTPClientRegistry,
        target: _TargetKey,
        pid: int,
        client: httpx.Client,
        token: Union[str, Callable[[], str]],
    ) -> None:
        self._registry = registry
        self._target = target
        self._pid = pid
        self._client = client
        self._token = token
        self._closed = False
        self._lock = threading.Lock()

    def get(self, url: str, **kwargs: Any) -> httpx.Response:
        return self.request("GET", url, **kwargs)

    def post(self, url: str, **kwargs: Any) -> httpx.Response:
        return self.request("POST", url, **kwargs)

    def put(self, url: str, **kwargs: Any) -> httpx.Response:
        return self.request("PUT", url, **kwargs)

    def delete(self, url: str, **kwargs: Any) -> httpx.Response:
        return self.request("DELETE", url, **kwargs)

    def request(self, method: str, url: str, **kwargs: Any) -> httpx.Response:
        kwargs["headers"] = self._request_headers(url, kwargs.get("headers"))
        return self._current_client().request(method, url, **kwargs)

    def stream(self, method: str, url: str, **kwargs: Any):
        kwargs["headers"] = self._request_headers(url, kwargs.get("headers"))
        return self._current_client().stream(method, url, **kwargs)

    def close(self) -> None:
        with self._lock:
            if self._closed:
                return
            self._closed = True
            pid = self._pid
        self._registry.release(self._target, pid)

    def _current_client(self) -> httpx.Client:
        with self._lock:
            if self._closed:
                raise SandboxClientClosedError(
                    "Cannot send a request after SandboxClient.close()"
                )
            current_pid = os.getpid()
            if current_pid != self._pid:
                self._pid, self._client = self._registry._acquire_target(self._target)
            return self._client

    def _request_headers(
        self, url: str, headers: Mapping[str, str] | None
    ) -> httpx.Headers:
        request_headers = httpx.Headers(headers)
        token = self._token() if callable(self._token) else self._token
        if not isinstance(token, str) or not token.strip():
            raise ValueError("token provider must return a non-empty string")
        token = token.strip()
        # Keep the legacy Frontend contract alongside Edge bearer authentication.
        request_headers["X-Auth"] = token
        if urlsplit(url).path.startswith("/direct/"):
            request_headers["Authorization"] = f"Bearer {token}"
        return request_headers


_SHARED_HTTP_CLIENT_REGISTRY = _SharedHTTPClientRegistry()
if hasattr(os, "register_at_fork"):
    os.register_at_fork(
        after_in_child=_SHARED_HTTP_CLIENT_REGISTRY._after_fork_in_child
    )
atexit.register(_SHARED_HTTP_CLIENT_REGISTRY.close_all)


def acquire_shared_http_client(
    scheme: str,
    server: str,
    verify_tls: bool,
    token: Union[str, Callable[[], str]],
) -> _SharedHTTPClientLease:
    """Acquire a token-scoped lease for a process-local HTTP connection pool."""
    return _SHARED_HTTP_CLIENT_REGISTRY.acquire(scheme, server, verify_tls, token)
