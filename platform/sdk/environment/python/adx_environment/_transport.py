"""HTTP client for the frontend environment v1 API.

Small control-plane requests use the unified environment action model::

    POST   /api/environment/v1/environments
    DELETE /api/environment/v1/environments/{environmentID}
    POST   /api/environment/v1/environments/{environmentID}/invoke   {"action", "args"}

Connection settings may be passed explicitly with ``ConnectionConfig`` or read
from these environment variables::

    ADX_SERVER_ADDRESS   host:port of the primary service entry (required)
    ADX_GATEWAY_ADDRESS  optional gateway for tunnel/port-forward traffic
    ADX_TOKEN            JWT used by both entries (required)

Response format:
- Frontend control requests use the existing raw ``X-Auth`` contract.
- Direct-route requests include ``Authorization: Bearer <JWT>`` and the
  same raw ``X-Auth`` token for compatibility with existing Frontend deployments.
- Frontend responses use ``{"code", "message", "data"}``; ``data`` is a
  base64-encoded JSON result and is decoded by this client.
"""

import base64
import json
import logging
import math
import os
import random
import time
import uuid
from contextlib import contextmanager, nullcontext
from typing import Any, Dict, Iterable, Optional

import httpx

from ._http_pool import acquire_shared_http_client
from .types import ADX_GET_DEFAULT_TIMEOUT, ConnectionConfig

logger = logging.getLogger(__name__)


class EnvironmentError(RuntimeError):
    """Raised when the frontend returns a non-2xx response or an error body."""

    def __init__(
        self,
        message: str,
        *,
        request_id: Optional[str] = None,
        code: Optional[str] = None,
        retry: Optional[str] = None,
        outcome: Optional[str] = None,
        operation_id: Optional[str] = None,
        environment_id: Optional[str] = None,
        request_forwarded: Optional[bool] = None,
        failure: Optional[Dict[str, Any]] = None,
        scheduling_failure: Optional[Dict[str, Any]] = None,
        probe_failure: Optional[Dict[str, Any]] = None,
    ):
        super().__init__(message)
        self.request_id = request_id
        self.code = code
        self.retry = retry
        self.outcome = outcome
        self.operation_id = operation_id
        self.environment_id = environment_id
        self.request_forwarded = request_forwarded
        self.failure = failure
        self.scheduling_failure = scheduling_failure
        self.probe_failure = probe_failure


class EnvironmentNotFound(EnvironmentError):
    def __init__(self, environment_id: str, message: Optional[str] = None):
        super().__init__(
            message or f"environment {environment_id} was not found",
            code="NOT_FOUND",
            retry="never",
            outcome="terminal",
            environment_id=environment_id,
        )
        self.environment_id = environment_id


class PermissionDenied(EnvironmentError):
    def __init__(
        self,
        environment_id: str,
        message: Optional[str] = None,
        *,
        request_id: Optional[str] = None,
    ):
        super().__init__(
            message or f"permission denied for environment {environment_id}",
            request_id=request_id,
            code="PERMISSION_DENIED",
            retry="never",
            outcome="not_started",
            environment_id=environment_id,
        )
        self.environment_id = environment_id


class EnvironmentHTTPError(EnvironmentError):
    def __init__(
        self,
        status_code: int,
        payload: Dict[str, Any],
        message: str,
        *,
        request_id: Optional[str] = None,
        code: Optional[str] = None,
        retry: Optional[str] = None,
        outcome: Optional[str] = None,
        operation_id: Optional[str] = None,
        environment_id: Optional[str] = None,
        scheduling_failure: Optional[Dict[str, Any]] = None,
        probe_failure: Optional[Dict[str, Any]] = None,
        request_forwarded: Optional[bool] = None,
    ):
        super().__init__(
            message,
            request_id=request_id,
            code=code,
            retry=retry,
            outcome=outcome,
            operation_id=operation_id,
            environment_id=environment_id,
            scheduling_failure=scheduling_failure,
            probe_failure=probe_failure,
            request_forwarded=request_forwarded,
        )
        self.status_code = status_code
        self.payload = payload
        raw_detail = payload.get("error")
        detail = raw_detail if isinstance(raw_detail, dict) else {}
        self.sandbox_ids = tuple(detail.get("sandbox_ids") or ())
        self.template_ids = tuple(detail.get("template_ids") or ())


class _InvokeResult(dict):
    """Action result carrying transport metadata outside its public mapping."""

    def __init__(self, value: Dict[str, Any], request_id: str):
        super().__init__(value)
        self.request_id = request_id


class _RetryableHTTPStatus(EnvironmentHTTPError):
    """Transient gateway response whose create/delete outcome may be unknown."""

    def __init__(self, error: EnvironmentHTTPError):
        super().__init__(
            error.status_code,
            error.payload,
            str(error),
            request_id=error.request_id,
            code=error.code,
            retry=error.retry,
            outcome=error.outcome,
            operation_id=error.operation_id,
            environment_id=error.environment_id,
            scheduling_failure=error.scheduling_failure,
            probe_failure=error.probe_failure,
            request_forwarded=error.request_forwarded,
        )


class _CreateOutcomeUnknown(EnvironmentError):
    """Create stream ended without an authoritative final result."""


_DIRECT_SAFE_FALLBACK_ERRORS = (
    httpx.ConnectError,
    httpx.ConnectTimeout,
    httpx.PoolTimeout,
)
_DIRECT_UNKNOWN_OUTCOME_ERRORS = (
    httpx.ReadError,
    httpx.ReadTimeout,
    httpx.RemoteProtocolError,
    httpx.WriteError,
    httpx.WriteTimeout,
)
_DIRECT_RETRY_BACKOFF_SECONDS = 0.1
_DIRECT_POOL_TIMEOUT_SECONDS = 2.0
# A single missing route is commonly a transient publication race. Keep trying
# direct on later logical invokes and only sticky-disable after this many misses
# have already been tolerated.
_DIRECT_ROUTE_MISS_BUDGET = 3

_CREATE_MAX_ATTEMPTS = 3
_CREATE_RETRY_BACKOFF_SECONDS = 0.1
_DEFAULT_CREATE_LOGICAL_TIMEOUT = 90
_CREATE_RETRYABLE_ERRORS = (
    httpx.ConnectError,
    httpx.ConnectTimeout,
    httpx.ReadError,
    httpx.ReadTimeout,
    httpx.RemoteProtocolError,
    httpx.WriteError,
    httpx.WriteTimeout,
    _RetryableHTTPStatus,
    _CreateOutcomeUnknown,
)
_RETRYABLE_GATEWAY_STATUS_CODES = frozenset((502, 503, 504))
_DELETE_MAX_ATTEMPTS = 3
_DELETE_RETRY_BACKOFF_SECONDS = 0.1
_LIFECYCLE_MAX_ATTEMPTS = 3
_LIFECYCLE_RETRY_BACKOFF_SECONDS = 0.1


class EnvironmentClient:
    """Thin HTTP client over the frontend environment v1 control plane.

    A single client is shared by all sub-resources (files/commands/shells)
    of one :class:`~adx_environment.Environment`. It is also used standalone for
    create/delete before a environment id exists.
    """

    def __init__(
        self,
        server: Optional[str] = None,
        token: Optional[str] = None,
        *,
        verify_tls: bool = False,
        connection: Optional[ConnectionConfig] = None,
    ):
        if connection is not None:
            if not isinstance(connection, ConnectionConfig):
                raise TypeError("connection must be a ConnectionConfig")
            if server is not None or token is not None or verify_tls:
                raise ValueError("connection cannot be combined with server, token, or verify_tls")
        else:
            connection = ConnectionConfig.from_env(
                server_address=server,
                token=token,
                verify_tls=verify_tls,
            )
        self._connection = connection
        self._server = connection.server_address
        self._token = connection.token
        # Production gateways are TLS. Set ADX_TLS=0 for a plain-HTTP dev
        # cluster (e.g. an AIO frontend started with frontend_ssl_enable=false).
        self._tls = connection.use_tls
        scheme = "https" if self._tls else "http"
        self._origin = f"{scheme}://{self._server}"
        self._base = f"{scheme}://{self._server}/api/environment/v1"
        # The process-level client owns TCP/TLS pooling. Authentication remains
        # on this per-Environment lease and is injected into every request.
        self._http = acquire_shared_http_client(
            scheme,
            self._server,
            connection.verify_tls,
            connection.resolved_token,
        )

        # ── HTTP direct via the primary service entry ──────────────────────
        # Required /direct traffic always shares ADX_SERVER_ADDRESS + ADX_TLS.
        # ADX_GATEWAY_ADDRESS is intentionally not consulted here: it can point
        # at the optional plaintext tunnel/port-forward listener.
        self._execd_port = int(os.environ.get("ADX_EXECD_PORT", "50090").strip() or "50090")
        self._direct_enabled = True
        # A 404 falls back immediately for the current invoke. Only a sustained
        # run of route misses sticky-disables direct for this client.
        self._direct_disabled = False
        self._direct_route_misses = 0
        self._direct_base = f"{scheme}://{self._server}/direct"
        self._last_create: Dict[str, Any] = {}
        self._resume_chunk_size = int(os.environ.get("ADX_RESUME_CHUNK_SIZE", str(8 * 1024 * 1024)))
        self._resume_max_retries = int(os.environ.get("ADX_RESUME_MAX_RETRIES", "3"))
        self._resume_min_size = int(os.environ.get("ADX_RESUME_MIN_SIZE", str(64 * 1024 * 1024)))

    # ── lifecycle ──────────────────────────────────────────────────────

    def require_workload_probes(self) -> None:
        """Reject unsupported servers before accepting a probe-dependent create."""
        response = self._http.get(f"{self._base}/capabilities", headers={"X-Auth": self._token}, timeout=10)
        if response.status_code in (404, 405, 501):
            raise EnvironmentError("server does not support workload probes", code="UNSUPPORTED", retry="never", outcome="not_started")
        if response.status_code >= 400:
            raise self._http_error(response)
        try:
            payload = response.json()
        except ValueError as exc:
            raise EnvironmentError("invalid server capabilities response", code="UNSUPPORTED", retry="never", outcome="not_started") from exc
        if not isinstance(payload, dict) or not isinstance(payload.get("capabilities"), list) or "workload-probes-v1" not in payload["capabilities"]:
            raise EnvironmentError("server does not support workload probes", code="UNSUPPORTED", retry="never", outcome="not_started")

    def create(self, body: Dict[str, Any]) -> str:
        """POST /environments — returns the new environmentID."""
        data = self.create_info(body)
        sid = data.get("environmentId")
        if not sid:
            raise EnvironmentError(f"create response missing environmentId: {data}")
        return sid

    def resources(self) -> Dict[str, Any]:
        """Query the public Environment node-resource endpoint."""
        resp = self._http.get(
            f"{self._base}/resources",
            headers={"Accept": "application/json"},
            timeout=60,
        )
        if resp.status_code >= 400:
            raise EnvironmentError(f"resource query failed: HTTP {resp.status_code} {resp.text}")
        try:
            payload = resp.json()
        except ValueError as exc:
            raise EnvironmentError("resource query returned invalid JSON") from exc
        if not isinstance(payload, dict):
            raise EnvironmentError("resource query returned an invalid response")
        return payload

    def create_info(self, body: Dict[str, Any]) -> Dict[str, Any]:
        """POST /environments and return the confirmed-running final SSE result."""
        logical_timeout = int(body.get("createTimeoutSeconds") or _DEFAULT_CREATE_LOGICAL_TIMEOUT)
        request_timeout = logical_timeout + self._connection.communication_margin_seconds
        # A UUIDv4 identifies this logical create across transport retries.
        operation_id = str(uuid.uuid4())
        request_id = f"create-{operation_id}"
        request_body = dict(body)
        request_name = request_body.get("name") or f"environment-{operation_id}"
        request_body["name"] = request_name
        deadline = time.monotonic() + request_timeout
        last_error: Optional[BaseException] = None
        attempts = 0

        for attempt in range(1, _CREATE_MAX_ATTEMPTS + 1):
            remaining = deadline - time.monotonic()
            if remaining <= 0:
                break
            attempts = attempt
            try:
                data = self._create_info_attempt(
                    request_body,
                    request_id=request_id,
                    request_timeout=remaining,
                )
            except _CREATE_RETRYABLE_ERRORS as exc:
                last_error = exc
                if attempt >= _CREATE_MAX_ATTEMPTS:
                    break
                remaining = deadline - time.monotonic()
                backoff = min(
                    _CREATE_RETRY_BACKOFF_SECONDS * (2 ** (attempt - 1)),
                    max(0.0, remaining),
                )
                if backoff <= 0:
                    break
                logger.warning(
                    "environment create transport error; retrying request_id=%s name=%s attempt=%d/%d error=%s",
                    request_id,
                    request_name,
                    attempt,
                    _CREATE_MAX_ATTEMPTS,
                    exc,
                )
                time.sleep(backoff)
                continue

            self._last_create = data
            return data

        if isinstance(last_error, _RetryableHTTPStatus):
            # A complete service response remains a domain/HTTP failure even
            # after retry exhaustion. Preserve payload, status and diagnostics.
            raise EnvironmentHTTPError(
                last_error.status_code, last_error.payload,
                f"environment create service failed after {attempts} attempts: {last_error}",
                request_id=last_error.request_id, code=last_error.code,
                retry=last_error.retry, outcome=last_error.outcome,
                operation_id=last_error.operation_id,
                environment_id=last_error.environment_id,
                scheduling_failure=last_error.scheduling_failure,
                probe_failure=last_error.probe_failure,
                request_forwarded=last_error.request_forwarded,
            ) from last_error
        contract_error = last_error if isinstance(last_error, EnvironmentError) else None
        raise EnvironmentError(
            "environment create transport failed after "
            f"{attempts} attempts "
            f"(requestId={request_id}, name={request_name}): "
            f"{last_error or 'create deadline exhausted'}",
            request_id=(contract_error.request_id if contract_error else None) or request_id,
            code=(contract_error.code if contract_error else None) or "OUTCOME_UNKNOWN",
            retry=(contract_error.retry if contract_error else None) or "same_operation",
            outcome=(contract_error.outcome if contract_error else None) or "unknown",
            operation_id=contract_error.operation_id if contract_error else None,
            environment_id=(contract_error.environment_id if contract_error else None) or request_name,
        ) from last_error

    def _create_info_attempt(
        self,
        body: Dict[str, Any],
        *,
        request_id: str,
        request_timeout: float,
    ) -> Dict[str, Any]:
        """Execute one transport attempt for a logical environment create."""
        final: Optional[Dict[str, Any]] = None
        with self._http.stream(
            "POST",
            f"{self._base}/environments",
            json=body,
            headers={
                "Accept": "text/event-stream",
                "X-Request-Id": request_id,
            },
            timeout=request_timeout,
        ) as resp:
            if resp.status_code in _RETRYABLE_GATEWAY_STATUS_CODES:
                resp.read()
                error = self._http_error(resp, request_id=request_id)
                if error.retry == "never":
                    raise error
                raise _RetryableHTTPStatus(error)
            content_type = resp.headers.get("content-type", "").lower()
            if "text/event-stream" not in content_type:
                resp.read()
                return self._json(resp)
            if resp.status_code >= 400:
                resp.read()
                raise EnvironmentError(f"HTTP {resp.status_code}: {resp.text}")

            event = ""
            data_lines: list[str] = []
            for line in resp.iter_lines():
                if line == "":
                    if event == "final" and data_lines:
                        try:
                            parsed = json.loads("\n".join(data_lines))
                        except json.JSONDecodeError as exc:
                            raise EnvironmentError(f"invalid environment create final event: {exc}") from exc
                        if not isinstance(parsed, dict):
                            raise EnvironmentError("environment create final event must contain a JSON object")
                        final = parsed
                        break
                    event = ""
                    data_lines = []
                    continue
                if line.startswith(":"):
                    continue
                if line.startswith("event:"):
                    event = line[6:].strip()
                elif line.startswith("data:"):
                    data_lines.append(line[5:].lstrip())

        if final is None:
            raise _CreateOutcomeUnknown(
                "environment create stream ended before final event",
                request_id=request_id,
                code="OUTCOME_UNKNOWN",
                retry="same_operation",
                outcome="unknown",
                environment_id=str(body.get("name") or "") or None,
            )
        if final.get("status") != "running":
            status = final.get("status") or "unknown"
            code = final.get("errorCode")
            message = final.get("message") or "environment did not reach running state"
            response_request_id = final.get("requestId") or request_id
            environment_id = final.get("environmentId") or "unknown"
            detail = final.get("error")
            if isinstance(detail, dict):
                error = EnvironmentHTTPError(
                    int(code) if isinstance(code, int) else 503,
                    final,
                    f"environment create {status}: {message}",
                    request_id=detail.get("requestId") or response_request_id,
                    code=detail.get("code"),
                    retry=detail.get("retry"),
                    outcome=detail.get("outcome"),
                    operation_id=detail.get("operationId"),
                    environment_id=detail.get("environmentId") or environment_id,
                    scheduling_failure=detail.get("schedulingFailure"),
                    probe_failure=detail.get("probeFailure"),
                )
                if error.retry == "same_operation" and error.outcome == "unknown":
                    raise _RetryableHTTPStatus(error)
                raise error
            raise EnvironmentError(
                f"environment create {status} "
                f"(errorCode={code}, requestId={response_request_id}, "
                f"environmentId={environment_id}): {message}",
                request_id=response_request_id,
                environment_id=environment_id,
            )
        data = final
        return data

    @property
    def last_create(self) -> Dict[str, Any]:
        """Full decoded response from the most recent create call."""
        return self._last_create

    def delete(self, environment_id: str) -> None:
        """DELETE /environments/{id}."""
        request_id = f"delete-{uuid.uuid4()}"
        last_error: Optional[BaseException] = None
        for attempt in range(1, _DELETE_MAX_ATTEMPTS + 1):
            try:
                resp = self._http.delete(
                    f"{self._base}/environments/{environment_id}",
                    headers={"X-Request-Id": request_id},
                    timeout=60,
                )
            except _CREATE_RETRYABLE_ERRORS as exc:
                last_error = exc
            else:
                # Treat 404 as a successful idempotent teardown.
                if resp.status_code in (200, 202, 204, 404):
                    return
                if resp.status_code == 403:
                    raise PermissionDenied(environment_id, request_id=request_id)
                if resp.status_code not in _RETRYABLE_GATEWAY_STATUS_CODES:
                    raise self._http_error(resp, request_id=request_id)
                error = self._http_error(resp, request_id=request_id)
                if error.retry == "never":
                    raise error
                last_error = _RetryableHTTPStatus(error)

            if attempt >= _DELETE_MAX_ATTEMPTS:
                break
            logger.warning(
                "environment delete transport error; retrying request_id=%s environment_id=%s attempt=%d/%d error=%s",
                request_id,
                environment_id,
                attempt,
                _DELETE_MAX_ATTEMPTS,
                last_error,
            )
            time.sleep(_DELETE_RETRY_BACKOFF_SECONDS * (2 ** (attempt - 1)))

        raise EnvironmentError(
            (
                f"delete "
                f"{environment_id}"
                f" failed after "
                f"{_DELETE_MAX_ATTEMPTS}"
                f" attempts (requestId="
                f"{request_id}"
                f"): "
                f"{last_error}"
            ),
            request_id=request_id,
            code="OUTCOME_UNKNOWN",
            retry="same_operation",
            outcome="unknown",
            environment_id=environment_id,
        ) from last_error

    def create_snapshot(
        self,
        environment_id: str,
        *,
        name: Optional[str] = None,
        timeout_seconds: int = ADX_GET_DEFAULT_TIMEOUT,
    ) -> Dict[str, Any]:
        """Create a reusable Snapshot with one explicit request attempt.

        A broken connection or gateway error is an uncertain outcome: the
        server may already have committed the immutable Snapshot. Do not hide
        that state behind an SDK sleep/retry loop. The request ID in the error
        lets an explicit reconciliation flow replay the same logical action.
        """
        request_id = self._new_request_id("snapshot")
        body: Dict[str, Any] = {"timeoutSeconds": timeout_seconds}
        if name is not None:
            body["name"] = name
        try:
            resp = self._http.post(
                f"{self._base}/environments/{environment_id}/snapshots",
                json=body,
                headers={"X-ADX-Request-ID": request_id},
                timeout=timeout_seconds + self._connection.communication_margin_seconds,
            )
        except _CREATE_RETRYABLE_ERRORS as exc:
            raise EnvironmentError(
                f"environment snapshot result is uncertain (requestId={request_id}): {exc}",
                request_id=request_id,
                code="OUTCOME_UNKNOWN",
                retry="same_operation",
                outcome="unknown",
                operation_id=request_id,
                environment_id=environment_id,
            ) from exc
        if resp.status_code in _RETRYABLE_GATEWAY_STATUS_CODES:
            raise self._http_error(resp, request_id=request_id)
        return self._json(resp)

    def get_snapshot(self, snapshot_id: str) -> Dict[str, Any]:
        resp = self._http.get(
            f"{self._base}/snapshots/{snapshot_id}",
            timeout=ADX_GET_DEFAULT_TIMEOUT + self._connection.communication_margin_seconds,
        )
        return self._json(resp)

    def list_snapshots(
        self,
        *,
        name: Optional[str] = None,
        page_token: Optional[str] = None,
        page_size: Optional[int] = None,
    ) -> Dict[str, Any]:
        params: Dict[str, Any] = {}
        if name is not None:
            params["name"] = name
        if page_token is not None:
            params["pageToken"] = page_token
        if page_size is not None:
            params["pageSize"] = page_size
        resp = self._http.get(
            f"{self._base}/snapshots",
            params=params,
            timeout=ADX_GET_DEFAULT_TIMEOUT + self._connection.communication_margin_seconds,
        )
        return self._json(resp)

    def delete_snapshot(self, snapshot_id: str) -> None:
        request_id = self._new_request_id("delete-snapshot")
        resp = self._http.delete(
            f"{self._base}/snapshots/{snapshot_id}",
            headers={"X-ADX-Request-ID": request_id},
            timeout=ADX_GET_DEFAULT_TIMEOUT + self._connection.communication_margin_seconds,
        )
        self._json(resp)

    def pause(
        self,
        environment_id: str,
        retention: Optional[Dict[str, Any]] = None,
        timeout_seconds: int = ADX_GET_DEFAULT_TIMEOUT,
    ) -> Dict[str, Any]:
        """Synchronously pause one environment using one internal request identity."""
        return self._lifecycle_request(
            environment_id,
            operation="pause",
            body={"timeoutSeconds": timeout_seconds, **({"retention": retention} if retention is not None else {})},
            timeout_seconds=timeout_seconds,
        )

    def resume(self, environment_id: str) -> Dict[str, Any]:
        """Synchronously resume one environment using one internal request identity."""
        return self._lifecycle_request(
            environment_id,
            operation="resume",
            body={},
        )

    def reload(self, environment_id: str) -> Dict[str, Any]:
        """Synchronously reload one environment using one internal request identity."""
        return self._lifecycle_request(
            environment_id,
            operation="reload",
            body={},
        )

    def update_network_policy(self, environment_id: str, policy: Dict[str, Any]) -> Dict[str, Any]:
        """Atomically replace the complete network policy of one environment."""
        return self._lifecycle_request(
            environment_id,
            operation="network",
            body=policy,
            method="put",
        )

    def _lifecycle_request(
        self,
        environment_id: str,
        *,
        operation: str,
        body: Dict[str, Any],
        timeout_seconds: int = ADX_GET_DEFAULT_TIMEOUT,
        method: str = "post",
    ) -> Dict[str, Any]:
        request_id = self._new_request_id(operation)
        last_error: Optional[BaseException] = None
        sender = self._http.put if method == "put" else self._http.post
        for attempt in range(1, _LIFECYCLE_MAX_ATTEMPTS + 1):
            try:
                resp = sender(
                    f"{self._base}/environments/{environment_id}/{operation}",
                    json=body,
                    headers={"X-ADX-Request-ID": request_id},
                    timeout=timeout_seconds + self._connection.communication_margin_seconds,
                )
            except _CREATE_RETRYABLE_ERRORS as exc:
                last_error = exc
            else:
                if resp.status_code not in _RETRYABLE_GATEWAY_STATUS_CODES:
                    try:
                        result = self._json(resp)
                    except EnvironmentError as exc:
                        raise EnvironmentError(f"environment {operation} failed (requestId={request_id}): {exc}") from exc
                    if operation == "pause" and result.get("snapshotId") != request_id:
                        raise EnvironmentError(
                            "environment pause returned a snapshotId that does not match "
                            f"the internal requestId={request_id}"
                        )
                    return result
                error = self._http_error(resp, request_id=request_id)
                if error.retry == "never":
                    raise error
                last_error = _RetryableHTTPStatus(error)

            if attempt >= _LIFECYCLE_MAX_ATTEMPTS:
                break
            logger.warning(
                "environment %s transport error; retrying request_id=%s environment_id=%s attempt=%d/%d error=%s",
                operation,
                request_id,
                environment_id,
                attempt,
                _LIFECYCLE_MAX_ATTEMPTS,
                last_error,
            )
            time.sleep(_LIFECYCLE_RETRY_BACKOFF_SECONDS * (2 ** (attempt - 1)))

        raise EnvironmentError(
            f"environment {operation} failed after {_LIFECYCLE_MAX_ATTEMPTS} attempts "
            f"(requestId={request_id}): {last_error}",
            request_id=request_id,
            code="OUTCOME_UNKNOWN",
            retry="same_operation",
            outcome="unknown",
            operation_id=request_id,
            environment_id=environment_id,
        ) from last_error

    def environment_info(self, environment_id: str, *, timeout: float = 60) -> Dict[str, Any]:
        """Return one Environment through the directory's instance_id query."""
        resp = self._http.get(
            f"{self._origin}/api/instances",
            params={"instance_id": environment_id},
            timeout=timeout,
        )
        if resp.status_code == 404:
            raise EnvironmentNotFound(environment_id)
        if resp.status_code == 403:
            raise PermissionDenied(environment_id)
        if resp.status_code >= 400:
            raise self._http_error(resp, request_id=resp.headers.get("x-request-id"))
        try:
            payload = resp.json()
        except ValueError as exc:
            raise EnvironmentError(f"get instance {environment_id} returned invalid JSON") from exc
        if not isinstance(payload, list):
            raise EnvironmentError(f"get instance {environment_id} returned an invalid response")
        for item in payload:
            if isinstance(item, dict) and item.get("id") == environment_id:
                return item
        raise EnvironmentNotFound(environment_id)

    # ── unified action invoke ──────────────────────────────────────────

    def invoke(
        self,
        environment_id: str,
        action: str,
        args: Optional[Dict[str, Any]] = None,
        *,
        timeout: Optional[int] = None,
    ) -> Dict[str, Any]:
        """POST /environments/{id}/invoke with ``{action, args}``.

        Returns the decoded JSON ``args``/result object. The HTTP timeout is
        derived from the logical *timeout* with a small network buffer, so a slow
        primitive does not trip the client first.
        """
        logical_timeout = ADX_GET_DEFAULT_TIMEOUT if timeout is None else timeout
        if isinstance(logical_timeout, bool) or not isinstance(logical_timeout, (int, float)) or not math.isfinite(logical_timeout):
            raise ValueError("timeout must be finite")
        if logical_timeout == 0 or logical_timeout > 86400:
            raise ValueError("timeout must be positive and at most 86400 seconds, or negative for unbounded calls")
        request_id = self._new_request_id("invoke")
        processing_deadline = None if logical_timeout < 0 else time.monotonic() + logical_timeout
        deadline = None if processing_deadline is None else processing_deadline + self._connection.communication_margin_seconds

        # Prefer the /direct path; fall back to the control-plane invoke path.
        if self._direct_enabled and not self._direct_disabled:
            result, fell_back = self._invoke_direct(
                environment_id,
                action,
                args or {},
                request_id,
                deadline,
                processing_deadline,
                None if logical_timeout < 0 else logical_timeout,
            )
            if not fell_back:
                return _InvokeResult(result, request_id)
            if logical_timeout >= 0:
                processing_deadline = time.monotonic() + logical_timeout
                deadline = processing_deadline + self._connection.communication_margin_seconds

        fallback_timeout = self._remaining_timeout(deadline)
        if fallback_timeout is not None and fallback_timeout <= 0:
            raise EnvironmentError(
                f"invoke deadline exhausted before frontend fallback (requestId={request_id})",
                request_id=request_id,
            )
        try:
            resp = self._http.post(
                f"{self._base}/environments/{environment_id}/invoke",
                json={
                    "action": action,
                    "args": args or {},
                    "requestId": request_id,
                },
                timeout=fallback_timeout,
                headers={"X-ADX-Request-ID": request_id, **self._processing_headers(processing_deadline)},
            )
        except httpx.RequestError as error:
            raise EnvironmentError(
                f"frontend invoke outcome is unknown (requestId={request_id}): {error}",
                request_id=request_id,
            ) from error
        return _InvokeResult(self._json(resp), request_id)

    def _invoke_direct(
        self,
        environment_id: str,
        action: str,
        args: Dict[str, Any],
        request_id: str,
        deadline: Optional[float],
        processing_deadline: Optional[float] = None,
        processing_timeout: Optional[float] = None,
    ) -> "tuple[Dict[str, Any], bool]":
        """Try the /direct path. Returns ``(result, fell_back)``.

        All attempts for one logical action reuse ``request_id``. EXECD deduplicates
        direct requests by that id, so a retry can wait for or replay the first
        execution instead of repeating a side effect. ``fell_back=True`` is
        returned only after connect/pool failures (known not sent) are exhausted,
        or when the server reports that the direct route does not exist. An
        exhausted unknown-outcome failure is surfaced to the caller and is never
        replayed through the Frontend invoke path.

        Unlike Frontend invoke, the EXECD HTTP server returns raw result JSON (no
        base64 ``BuildJobResponse`` envelope); action-level errors live inside
        that object (HTTP 200).
        """
        url = f"{self._direct_base}/{self._safe_id(environment_id)}/invoke"
        last_error: Optional[BaseException] = None
        last_failure_safe = False
        outcome_unknown = False
        attempts = 0

        pending_resume: Optional[EnvironmentHTTPError] = None
        attempt_limit = self._connection.request_max_attempts
        for attempt in range(1, attempt_limit + 1):
            remaining = self._remaining_timeout(deadline)
            if remaining is not None and remaining <= 0:
                break
            processing_remaining = self._remaining_timeout(processing_deadline)
            if processing_remaining is not None and processing_remaining <= 0:
                break
            attempts = attempt
            retry_after = 0.0
            pool_timeout = (
                _DIRECT_POOL_TIMEOUT_SECONDS if remaining is None else min(_DIRECT_POOL_TIMEOUT_SECONDS, remaining)
            )
            request_timeout = httpx.Timeout(
                remaining,
                pool=pool_timeout,
            )
            try:
                resp = self._http.post(
                    url,
                    json={
                        "action": action,
                        "args": args,
                        "requestId": request_id,
                    },
                    timeout=request_timeout,
                    headers={"X-ADX-Request-ID": request_id, **self._processing_headers(processing_deadline)},
                )
            except _DIRECT_SAFE_FALLBACK_ERRORS as exc:
                self._direct_route_misses = 0
                last_error = exc
                last_failure_safe = True
            except _DIRECT_UNKNOWN_OUTCOME_ERRORS as exc:
                self._direct_route_misses = 0
                last_error = exc
                last_failure_safe = False
                outcome_unknown = True
            except httpx.RequestError as exc:
                self._direct_route_misses = 0
                raise EnvironmentError(
                    f"direct invoke outcome is unknown after transport failure (requestId={request_id}): {exc}",
                    request_id=request_id,
                    code="OUTCOME_UNKNOWN",
                    retry="same_operation",
                    outcome="unknown",
                    operation_id=request_id,
                    environment_id=environment_id,
                ) from exc
            else:
                if resp.status_code == 404:
                    try:
                        not_found = resp.json()
                    except ValueError:
                        not_found = None
                    if isinstance(not_found, dict) and not_found.get("error_code"):
                        # EXECD uses 404 for an operation-level missing command.
                        # It is an authoritative result from the resolved
                        # Environment route, not evidence that /direct is absent.
                        self._direct_route_misses = 0
                        raise self._http_error(resp, request_id=request_id)
                    if outcome_unknown:
                        # An earlier attempt may already have executed. Route
                        # disappearance does not make Frontend replay safe.
                        last_error = EnvironmentError(
                            "direct route disappeared after an unknown outcome",
                            request_id=request_id,
                        )
                        break
                    self._direct_route_misses += 1
                    if self._direct_route_misses > _DIRECT_ROUTE_MISS_BUDGET and not self._direct_disabled:
                        self._direct_disabled = True
                        logger.warning(
                            "direct invoke disabled; all subsequent invokes "
                            "will use frontend fallback after consecutive "
                            "route misses "
                            "request_id=%s action=%s route_misses=%d",
                            request_id,
                            action,
                            self._direct_route_misses,
                        )
                    return {}, True
                self._direct_route_misses = 0
                error = self._http_error(resp, request_id=request_id) if resp.status_code >= 400 else None
                if error is not None and error.retry == "never":
                    raise error
                if error is not None and self._is_resuming(resp, error, request_id):
                    pending_resume = error
                    last_error = error
                    last_failure_safe = True
                    retry_after = self._resume_retry_delay(resp)
                elif resp.status_code in _RETRYABLE_GATEWAY_STATUS_CODES:
                    last_error = _RetryableHTTPStatus(self._http_error(resp, request_id=request_id))
                    last_failure_safe = False
                    outcome_unknown = True
                elif resp.status_code >= 400:
                    raise self._http_error(resp, request_id=request_id)
                else:
                    try:
                        parsed = resp.json()
                    except ValueError as exc:
                        last_error = EnvironmentError(
                            f"direct invoke returned invalid JSON: {exc}",
                            request_id=request_id,
                        )
                        last_failure_safe = False
                        outcome_unknown = True
                    else:
                        if not isinstance(parsed, dict):
                            parsed = {"value": parsed}
                        return parsed, False

            if attempt >= attempt_limit:
                break
            backoff = max(retry_after, _DIRECT_RETRY_BACKOFF_SECONDS * (2 ** (attempt - 1)))
            if retry_after:
                backoff += random.uniform(0, 0.1)
            if backoff <= 0:
                break
            logger.warning(
                "direct invoke transport error; retrying request_id=%s action=%s attempt=%d/%d error=%s",
                request_id,
                action,
                attempt,
                attempt_limit,
                last_error,
            )
            time.sleep(backoff)
            if processing_timeout is not None:
                # Every permitted retry is a new HTTP attempt with the same
                # configured timeout, margin and operation identity.
                processing_deadline = time.monotonic() + processing_timeout
                deadline = processing_deadline + self._connection.communication_margin_seconds

        if pending_resume is not None and not outcome_unknown:
            # No attempt reached the application. Keep this authoritative outcome;
            # never turn demand waiting into a control-plane invocation fallback.
            raise pending_resume
        if last_failure_safe and not outcome_unknown and last_error is not None:
            # Every attempt failed before request bytes could reach EXECD.
            self._direct_disabled = True
            return {}, True
        detail = last_error or "invoke deadline exhausted"
        # The failed data connection alone cannot classify OOM. Consult the
        # authenticated catalog only on this recovery path, within the same
        # deadline. A still-running or unavailable record preserves uncertainty.
        remaining = self._remaining_timeout(deadline)
        if remaining is None or remaining > 0:
            try:
                info = self.environment_info(environment_id, timeout=min(2.0, remaining) if remaining is not None else 2.0)
            except (EnvironmentError, httpx.RequestError):
                info = {}
            failure = info.get("failure")
            if info.get("status") == "failed" and isinstance(failure, dict) and failure.get("reason") == "OomKilled":
                raise EnvironmentError(
                    "environment backend was OOM-killed",
                    request_id=request_id, code="ENVIRONMENT_OOM", retry="never", outcome="terminal",
                    operation_id=request_id, environment_id=environment_id, failure=failure,
                ) from last_error
        raise EnvironmentError(
            (f"direct invoke outcome is unknown after {attempts} attempts (requestId={request_id}): {detail}"),
            request_id=request_id,
            code="OUTCOME_UNKNOWN",
            retry="same_operation",
            outcome="unknown",
            operation_id=request_id,
            environment_id=environment_id,
        ) from last_error

    @staticmethod
    def _is_resuming(response: httpx.Response, error: EnvironmentHTTPError, request_id: str) -> bool:
        detail = error.payload.get("error", {})
        return (response.status_code == 503 and response.headers.get("X-ADX-Error-Code") == "ENV_RESUMING"
                and error.code == "ENV_RESUMING" and error.outcome == "not_started"
                and error.retry == "same_operation" and isinstance(detail, dict)
                and detail.get("requestForwarded") is False and detail.get("requestId") == request_id)

    @staticmethod
    def _resume_retry_delay(response: httpx.Response) -> float:
        try:
            seconds = float(response.headers.get("Retry-After", "1"))
        except ValueError:
            seconds = 1.0
        if not math.isfinite(seconds) or seconds < 0:
            seconds = 1.0
        return min(seconds, 30.0)

    @contextmanager
    def _data_response(self, method: str, url: str, timeout: Optional[float], *, stream=False, **kwargs):
        """Retry only an authoritative not-forwarded reply, with a replayable body.

        Generic file transport failures keep the file API's existing recovery
        behavior. An iterator cannot be replayed after HTTPX has consumed it.
        """
        logical = ADX_GET_DEFAULT_TIMEOUT if timeout is None else timeout
        if isinstance(logical, bool) or not isinstance(logical, (int, float)) or not math.isfinite(logical):
            raise ValueError("timeout must be finite")
        if logical == 0 or logical > 86400:
            raise ValueError("timeout must be positive and at most 86400 seconds, or negative")
        headers = dict(kwargs.pop("headers", None) or {})
        request_id = headers.setdefault("X-ADX-Request-ID", self._new_request_id("data"))
        content = kwargs.get("content")
        rewind = None
        replayable = content is None or isinstance(content, (bytes, bytearray, str))
        if hasattr(content, "seek") and hasattr(content, "tell"):
            try:
                rewind = content.tell()
                replayable = True
            except (OSError, ValueError):
                pass
        attempt_limit = self._connection.request_max_attempts
        for attempt in range(attempt_limit):
            if rewind is not None:
                content.seek(rewind)
            processing_deadline = None if logical < 0 else time.monotonic() + logical
            attempt_headers = {**headers, **self._processing_headers(processing_deadline)}
            transport_timeout = None if logical < 0 else logical + self._connection.communication_margin_seconds
            context = (self._http.stream(method, url, timeout=transport_timeout, headers=attempt_headers, **kwargs)
                       if stream else nullcontext(getattr(self._http, method.lower())(
                           url, timeout=transport_timeout, headers=attempt_headers, **kwargs)))
            with context as response:
                if response.status_code < 400:
                    yield response
                    return
                if stream:
                    response.read()
                error = self._http_error(response, request_id=request_id)
                if not self._is_resuming(response, error, request_id) or not replayable or attempt + 1 == attempt_limit:
                    raise error
                delay = self._resume_retry_delay(response) + random.uniform(0, 0.1)
            response.close()
            time.sleep(delay)

    @staticmethod
    def _processing_headers(deadline: Optional[float]) -> Dict[str, str]:
        if deadline is None:
            return {}
        milliseconds = max(1, math.ceil(max(0.0, deadline - time.monotonic()) * 1000))
        return {"X-ADX-Processing-Timeout-Ms": str(milliseconds)}

    @staticmethod
    def _remaining_timeout(deadline: Optional[float]) -> Optional[float]:
        if deadline is None:
            return None
        return max(0.0, deadline - time.monotonic())

    def upload_file_direct(
        self,
        environment_id: str,
        local_path: str,
        remote_path: str,
        rpc_timeout: Optional[float] = None,
        *,
        upload_type: str = "file",
    ) -> Dict[str, Any]:
        """Upload a file/tar over the required /direct binary data path."""
        content_len = os.path.getsize(local_path)
        if upload_type == "file" and content_len >= self._resume_min_size:
            return self._upload_file_resumable(environment_id, local_path, remote_path, rpc_timeout)
        with open(local_path, "rb") as f:
            return self._upload_direct(
                environment_id,
                f,
                remote_path,
                rpc_timeout,
                upload_type=upload_type,
                content_len=content_len,
            )

    def _upload_file_resumable(
        self,
        environment_id: str,
        local_path: str,
        remote_path: str,
        rpc_timeout: Optional[float] = None,
    ) -> Dict[str, Any]:
        """Upload a file via resumable /direct chunks and atomic commit."""
        total = os.path.getsize(local_path)
        upload_id = self._new_request_id("upload")
        offset = self._upload_status(environment_id, remote_path, upload_id, rpc_timeout)
        with open(local_path, "rb") as f:
            while offset < total:
                f.seek(offset)
                chunk = f.read(min(self._resume_chunk_size, total - offset))
                if not chunk:
                    break
                attempts = 0
                while True:
                    try:
                        result = self._upload_direct(
                            environment_id,
                            chunk,
                            remote_path,
                            rpc_timeout,
                            upload_type="file",
                            content_len=len(chunk),
                            extra_params={
                                "uploadId": upload_id,
                                "offset": str(offset),
                                "totalSize": str(total),
                            },
                        )
                        offset = int(result.get("offset", offset + len(chunk)))
                        break
                    except EnvironmentError:
                        attempts += 1
                        if attempts > self._resume_max_retries:
                            raise
                        offset = self._upload_status(environment_id, remote_path, upload_id, rpc_timeout)
            return self._upload_commit(environment_id, remote_path, upload_id, total, rpc_timeout)

    def _upload_status(
        self,
        environment_id: str,
        remote_path: str,
        upload_id: str,
        rpc_timeout: Optional[float],
    ) -> int:
        url = f"{self._direct_base}/{self._safe_id(environment_id)}/upload/status"
        try:
            with self._data_response("GET", url, rpc_timeout,
                params={"path": remote_path, "uploadId": upload_id},

            ) as resp:
                pass
        except httpx.RequestError as e:
            raise EnvironmentError(f"direct upload status {environment_id} failed: {e}") from e
        try:
            parsed = resp.json()
        except ValueError as e:
            raise EnvironmentError("direct upload status returned non-JSON body") from e
        return int(parsed.get("offset", 0))

    def _upload_commit(
        self,
        environment_id: str,
        remote_path: str,
        upload_id: str,
        total: int,
        rpc_timeout: Optional[float],
    ) -> Dict[str, Any]:
        url = f"{self._direct_base}/{self._safe_id(environment_id)}/upload/commit"
        request_id = self._new_request_id("upload-commit")
        try:
            with self._data_response("POST", url, rpc_timeout,
                params={
                    "path": remote_path,
                    "uploadId": upload_id,
                    "totalSize": str(total),
                },
                headers={"X-ADX-Request-ID": request_id},
            ) as resp:
                pass
        except httpx.RequestError as e:
            raise EnvironmentError(f"direct upload commit {environment_id} failed: {e}") from e
        try:
            parsed = resp.json()
        except ValueError as e:
            raise EnvironmentError("direct upload commit returned non-JSON body") from e
        if not isinstance(parsed, dict):
            parsed = {"value": parsed}
        return parsed

    def upload_bytes_direct(
        self,
        environment_id: str,
        data: bytes,
        remote_path: str,
        rpc_timeout: Optional[float] = None,
    ) -> Dict[str, Any]:
        """Upload bytes over the required /direct binary data path."""
        return self._upload_direct(environment_id, data, remote_path, rpc_timeout, content_len=len(data))

    def upload_stream_direct(
        self,
        environment_id: str,
        chunks: Iterable[bytes],
        remote_path: str,
        rpc_timeout: Optional[float] = None,
        *,
        upload_type: str = "file",
    ) -> Dict[str, Any]:
        """Upload an iterator over bytes using HTTP chunked transfer encoding."""
        return self._upload_direct(
            environment_id,
            chunks,
            remote_path,
            rpc_timeout,
            upload_type=upload_type,
        )

    def _upload_direct(
        self,
        environment_id: str,
        content,
        remote_path: str,
        rpc_timeout: Optional[float],
        *,
        upload_type: str = "file",
        content_len: Optional[int] = None,
        extra_params: Optional[Dict[str, str]] = None,
    ) -> Dict[str, Any]:
        url = f"{self._direct_base}/{self._safe_id(environment_id)}/upload"
        content_type = "application/x-tar" if upload_type == "tar" else "application/octet-stream"
        headers = {"Content-Type": content_type}
        if content_len is not None:
            headers["Content-Length"] = str(content_len)
        try:
            params = {"path": remote_path, "type": upload_type}
            if extra_params:
                params.update(extra_params)
            with self._data_response("POST", url, rpc_timeout,
                params=params,
                content=content,
                headers=headers,
            ) as resp:
                pass
        except httpx.RequestError as e:
            raise EnvironmentError(f"direct upload {environment_id} failed: {e}") from e
        try:
            parsed = resp.json()
        except ValueError as e:
            raise EnvironmentError(f"direct upload {environment_id} returned non-JSON body") from e
        if not isinstance(parsed, dict):
            parsed = {"value": parsed}
        return parsed

    def download_file_direct(
        self,
        environment_id: str,
        remote_path: str,
        local_path: str,
        rpc_timeout: Optional[float] = None,
        *,
        download_type: str = "file",
    ) -> None:
        """Download a file/tar over the required /direct binary data path."""
        if download_type == "file":
            self._download_file_resumable(environment_id, remote_path, local_path, rpc_timeout)
            return
        url = f"{self._direct_base}/{self._safe_id(environment_id)}/download"
        try:
            with self._data_response(
                "GET", url, rpc_timeout, stream=True,
                params={"path": remote_path, "type": download_type},
            ) as resp:
                self._write_stream_to_file(resp, local_path, append=False)
        except httpx.RequestError as e:
            raise EnvironmentError(f"direct download {environment_id} failed: {e}") from e

    def _download_file_resumable(
        self,
        environment_id: str,
        remote_path: str,
        local_path: str,
        rpc_timeout: Optional[float] = None,
    ) -> None:
        url = f"{self._direct_base}/{self._safe_id(environment_id)}/download"
        part_path = f"{local_path}.part"
        parent = os.path.dirname(os.path.abspath(local_path))
        if parent:
            os.makedirs(parent, exist_ok=True)
        attempts = 0
        while True:
            offset = os.path.getsize(part_path) if os.path.exists(part_path) else 0
            headers = {"Range": f"bytes={offset}-"} if offset > 0 else None
            try:
                with self._data_response(
                    "GET", url, rpc_timeout, stream=True,
                    params={"path": remote_path, "type": "file"},
                        headers=headers,
                ) as resp:
                    append = offset > 0 and resp.status_code == 206
                    if offset > 0 and resp.status_code != 206:
                        append = False
                    self._write_stream_to_file(resp, part_path, append=append)
                os.replace(part_path, local_path)
                return
            except httpx.RequestError as e:
                attempts += 1
                if attempts > self._resume_max_retries:
                    raise EnvironmentError(f"direct download {environment_id} failed: {e}") from e

    @staticmethod
    def _write_stream_to_file(resp: httpx.Response, path: str, *, append: bool) -> None:
        mode = "ab" if append else "wb"
        with open(path, mode) as f:
            for chunk in resp.iter_bytes():
                if chunk:
                    f.write(chunk)

    def download_bytes_direct(
        self,
        environment_id: str,
        remote_path: str,
        rpc_timeout: Optional[float] = None,
    ) -> bytes:
        """Download bytes over the required /direct binary data path."""
        url = f"{self._direct_base}/{self._safe_id(environment_id)}/download"
        try:
            with self._data_response("GET", url, rpc_timeout,
                params={"path": remote_path, "type": "file"},

            ) as resp:
                pass
        except httpx.RequestError as e:
            raise EnvironmentError(f"direct download {environment_id} failed: {e}") from e
        return resp.content

    @property
    def direct_enabled(self) -> bool:
        """Whether EXECD direct invoke first tries the /direct route."""
        return self._direct_enabled

    def set_direct_enabled(self, enabled: bool) -> None:
        """Select whether operations may use the direct data path."""
        if not isinstance(enabled, bool):
            raise TypeError("enabled must be a boolean")
        self._direct_enabled = enabled
        if not enabled:
            self._direct_disabled = True

    @property
    def execd_port(self) -> int:
        """Internal EXECD HTTP container port requested during environment create."""
        return self._execd_port

    @staticmethod
    def _new_request_id(prefix: str) -> str:
        return f"{prefix}-{uuid.uuid4()}"

    @staticmethod
    def _safe_id(environment_id: str) -> str:
        """Sanitize an instance id the same way the router does (route.SanitizeID):
        ``@`` -> ``-at-`` and ``/`` ``.`` ``_`` -> ``-``."""
        s = environment_id.replace("@", "-at-")
        return "".join("-" if c in "/._" else c for c in s)

    @property
    def connection(self) -> ConnectionConfig:
        return self._connection

    @property
    def token(self) -> str:
        return self._token

    def close(self) -> None:
        self._http.close()

    # ── internal ───────────────────────────────────────────────────────

    @staticmethod
    def _http_error(resp: httpx.Response, *, request_id: Optional[str] = None) -> EnvironmentHTTPError:
        try:
            payload = resp.json()
        except (AttributeError, ValueError):
            payload = {"message": resp.text}
        if not isinstance(payload, dict):
            payload = {"message": str(payload)}
        raw_error = payload.get("error")
        detail = raw_error if isinstance(raw_error, dict) else {}
        message = str(
            payload.get("message")
            or detail.get("message")
            or (raw_error if isinstance(raw_error, str) else "")
            or resp.text
        )
        resolved_request_id = detail.get("requestId") or getattr(resp, "headers", {}).get("x-request-id") or request_id
        identity = f" (requestId={resolved_request_id})" if resolved_request_id else ""
        return EnvironmentHTTPError(
            resp.status_code,
            payload,
            f"HTTP {resp.status_code} {message}{identity}",
            request_id=resolved_request_id,
            code=detail.get("code"),
            retry=detail.get("retry"),
            outcome=detail.get("outcome"),
            operation_id=detail.get("operationId"),
            environment_id=detail.get("environmentId"),
            scheduling_failure=detail.get("schedulingFailure"),
            probe_failure=detail.get("probeFailure"),
            request_forwarded=detail.get("requestForwarded") if isinstance(detail.get("requestForwarded"), bool) else None,
        )

    @staticmethod
    def _json(resp: httpx.Response) -> Dict[str, Any]:
        """Unwrap the job.BuildJobResponse envelope.

        Body: ``{"code": <http-status>, "message": "<err>", "data": "<b64>"}``.
        ``code`` mirrors the HTTP status; HTTP-level failures (>=400) raise.
        Action-level errors are carried *inside* ``data`` (e.g. an ``error``
        key), so they are NOT raised here — the caller's result parsing
        handles them while preserving the requested local file layout.
        """
        if resp.status_code >= 400:
            raise EnvironmentClient._http_error(resp)
        try:
            envelope = resp.json()
        except ValueError:
            return {}

        code = envelope.get("code", resp.status_code)
        if isinstance(code, int) and code >= 400:
            raise EnvironmentError(f"code {code}: {envelope.get('message', '')}")

        raw = envelope.get("data")
        if raw in (None, ""):
            return {}
        # Go marshals []byte as base64; decode then JSON-parse the inner result.
        if isinstance(raw, str):
            try:
                decoded = base64.b64decode(raw)
                parsed = json.loads(decoded)
            except ValueError as e:
                raise EnvironmentError(f"failed to decode response data: {e}") from e
        else:
            # Some deployments may already return a JSON object for data.
            parsed = raw
        return parsed if isinstance(parsed, dict) else {"value": parsed}
