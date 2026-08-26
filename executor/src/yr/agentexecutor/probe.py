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

"""Instance-level startup/liveness probes for the Agent executor."""

from __future__ import annotations

import enum
import json
import logging
import os
import socket
import subprocess
import threading
import time
import urllib.request
from typing import Any, Mapping, Optional
from urllib.error import URLError

_LOG = logging.getLogger(__name__)

PROBE_SPEC_ENV = "YR_RUNTIME_BOOTSTRAP_PROBE"
# Injected by the jiuwenbox process runtime at sandbox create: sandbox veth
# IPv4 (isolated) or egress IPv4 (host). Absent on docker/runtime backends,
# where probes fall back to the loopback below.
SANDBOX_IP_ENV = "SANDBOX_IP"
DEFAULT_PROBE_HOST = "127.0.0.1"

DEFAULT_INITIAL_DELAY_SECONDS = 0
DEFAULT_PERIOD_SECONDS = 10
DEFAULT_TIMEOUT_SECONDS = 2
DEFAULT_FAILURE_THRESHOLD = 3
_MIN_PORT = 1
_MAX_PORT = 65535


class AggregateState(str, enum.Enum):
    HEALTHY = "HEALTHY"
    SUBHEALTH = "SUBHEALTH"
    FAILED = "FAILED"
    DISABLED = "DISABLED"


def _as_timeout(value: Any, fallback: int) -> int:
    try:
        timeout = int(value)
    except (TypeError, ValueError):
        return fallback
    return timeout if timeout >= 1 else fallback


class ProbeSpec:
    __slots__ = (
        "action",
        "initial_delay_seconds",
        "period_seconds",
        "timeout_seconds",
        "failure_threshold",
    )

    def __init__(
        self,
        action: dict,
        *,
        initial_delay_seconds: int = DEFAULT_INITIAL_DELAY_SECONDS,
        period_seconds: int = DEFAULT_PERIOD_SECONDS,
        timeout_seconds: int = DEFAULT_TIMEOUT_SECONDS,
        failure_threshold: int = DEFAULT_FAILURE_THRESHOLD,
    ) -> None:
        self.action = action
        self.initial_delay_seconds = max(0, initial_delay_seconds)
        self.period_seconds = max(1, period_seconds)
        self.timeout_seconds = _as_timeout(timeout_seconds, DEFAULT_TIMEOUT_SECONDS)
        self.failure_threshold = max(1, failure_threshold)

    @classmethod
    def from_mapping(cls, raw: Mapping[str, Any], sandbox_ip: Optional[str] = None) -> Optional["ProbeSpec"]:
        action = _extract_action(raw, sandbox_ip)
        if action is None:
            return None
        return cls(
            action,
            initial_delay_seconds=raw.get("initialDelaySeconds", DEFAULT_INITIAL_DELAY_SECONDS),
            period_seconds=raw.get("periodSeconds", DEFAULT_PERIOD_SECONDS),
            timeout_seconds=raw.get("timeoutSeconds", DEFAULT_TIMEOUT_SECONDS),
            failure_threshold=raw.get("failureThreshold", DEFAULT_FAILURE_THRESHOLD),
        )


def _extract_action(raw: Mapping[str, Any], sandbox_ip: Optional[str] = None) -> Optional[dict]:
    # Probe target precedence: spec host > SANDBOX_IP (sandbox veth IP) > loopback.
    # A spec-provided host lets the caller target an address other than the
    # sandbox's own veth/loopback (mirrors frontend ProbeSpec tcpSocket/httpGet).
    default_host = sandbox_ip or DEFAULT_PROBE_HOST
    actions: list[dict] = []
    tcp = raw.get("tcpSocket")
    if tcp is not None:
        if not isinstance(tcp, dict):
            return None
        port = tcp.get("port")
        if not isinstance(port, int) or not _MIN_PORT <= port <= _MAX_PORT:
            return None
        host = tcp.get("host") or default_host
        if not isinstance(host, str):
            return None
        actions.append({"type": "tcp", "port": port, "host": host})
    http = raw.get("httpGet")
    if http is not None:
        if not isinstance(http, dict):
            return None
        port = http.get("port")
        if not isinstance(port, int) or not _MIN_PORT <= port <= _MAX_PORT:
            return None
        path = http.get("path", "/")
        scheme = http.get("scheme", "http")
        host = http.get("host") or default_host
        if not all(isinstance(v, str) for v in (host, path, scheme)):
            return None
        if scheme not in ("http", "https"):
            return None
        actions.append({"type": "http", "port": port, "host": host, "path": path, "scheme": scheme})
    exec_spec = raw.get("exec")
    if exec_spec is not None:
        if not isinstance(exec_spec, dict):
            return None
        command = exec_spec.get("command")
        if not isinstance(command, list) or not command or not all(isinstance(a, str) for a in command):
            return None
        actions.append({"type": "exec", "command": command})
    if len(actions) != 1:
        return None
    return actions[0]


def _probe_once(action: dict, timeout: int) -> bool:
    kind = action["type"]
    if kind == "tcp":
        return _probe_tcp(action["host"], action["port"], timeout)
    if kind == "http":
        return _probe_http(action, timeout)
    if kind == "exec":
        return _probe_exec(action["command"], timeout)
    return False


def _probe_tcp(host: str, port: int, timeout: int) -> bool:
    try:
        with socket.create_connection((host, port), timeout=timeout):
            return True
    except OSError as exc:
        _LOG.debug("tcp probe %s:%s failed: %s (errno=%s)", host, port, exc, getattr(exc, "errno", "?"))
        return False


def _probe_http(action: dict, timeout: int) -> bool:
    scheme = action["scheme"]
    netloc = action["host"]
    default_port = 443 if scheme == "https" else 80
    if ":" not in netloc and action["port"] != default_port:
        netloc = f"{action['host']}:{action['port']}"
    url = f"{scheme}://{netloc}{action['path']}"
    request = urllib.request.Request(url, headers={"Accept": "application/json"})
    try:
        with urllib.request.urlopen(request, timeout=timeout) as resp:
            code = resp.getcode()
            if 200 <= code < 300:
                return True
            _LOG.debug("http probe %s failed: status=%s", url, code)
            return False
    except (URLError, OSError, ValueError) as exc:
        _LOG.debug("http probe %s failed: %s", url, exc)
        return False


def _probe_exec(command: list[str], timeout: int) -> bool:
    try:
        result = subprocess.run(  # noqa: S603
            command,
            stdout=subprocess.DEVNULL,
            stderr=subprocess.DEVNULL,
            timeout=timeout,
        )
    except (OSError, subprocess.TimeoutExpired) as exc:
        _LOG.debug("exec probe %s failed: %s", command, exc)
        return False
    if result.returncode == 0:
        return True
    _LOG.debug("exec probe %s failed: exit=%s", command, result.returncode)
    return False


def _load_probe_set(
    environ: Optional[Mapping[str, str]] = None,
) -> tuple[Optional[ProbeSpec], Optional[ProbeSpec]]:
    active_env = os.environ if environ is None else environ
    raw = active_env.get(PROBE_SPEC_ENV, "")
    if not raw:
        return None, None
    try:
        payload = json.loads(raw)
    except (TypeError, ValueError) as exc:
        _LOG.warning("invalid %s (not JSON): %s", PROBE_SPEC_ENV, exc)
        return None, None
    if not isinstance(payload, dict):
        _LOG.warning("invalid %s (expected object, got %s)", PROBE_SPEC_ENV, type(payload).__name__)
        return None, None
    sandbox_ip = active_env.get(SANDBOX_IP_ENV)
    startup_spec = payload.get("startup")
    startup = (
        ProbeSpec.from_mapping(startup_spec, sandbox_ip)
        if isinstance(startup_spec, dict)
        else None
    )
    liveness_spec = payload.get("liveness")
    liveness = (
        ProbeSpec.from_mapping(liveness_spec, sandbox_ip)
        if isinstance(liveness_spec, dict)
        else None
    )
    _LOG.info(
        "loaded probe set from %s: sandbox_ip=%s, startup=%s, liveness=%s",
        PROBE_SPEC_ENV,
        sandbox_ip or "(absent, using loopback)",
        _describe_spec(startup),
        _describe_spec(liveness),
    )
    return startup, liveness


def _describe_spec(spec: Optional[ProbeSpec]) -> str:
    if spec is None:
        return "none"
    return (
        f"action={spec.action}, initial_delay={spec.initial_delay_seconds}s, "
        f"period={spec.period_seconds}s, timeout={spec.timeout_seconds}s, "
        f"failure_threshold={spec.failure_threshold}"
    )


_liveness_spec: Optional[ProbeSpec] = None
_liveness_fail = 0
_liveness_lock = threading.Lock()


def _configure_liveness(spec: Optional[ProbeSpec]) -> None:
    global _liveness_spec, _liveness_fail
    with _liveness_lock:
        _liveness_spec = spec
        _liveness_fail = 0
    if spec is None:
        _LOG.info("liveness probe disabled (no spec configured)")
    else:
        _LOG.info(
            "liveness probe configured: action=%s, timeout=%ss, failure_threshold=%s",
            spec.action,
            spec.timeout_seconds,
            spec.failure_threshold,
        )


def probe_liveness() -> str:
    global _liveness_fail
    with _liveness_lock:
        spec = _liveness_spec
        if spec is None:
            _LOG.info("liveness probe invoked but no spec configured -> DISABLED")
            return AggregateState.DISABLED.value
        try:
            passed = _probe_once(spec.action, spec.timeout_seconds)
        except Exception as exc:  # noqa: BLE001
            _LOG.warning("liveness probe raised: %s", exc)
            _liveness_fail += 1
            if _liveness_fail >= spec.failure_threshold:
                _LOG.warning(
                    "liveness probe FAILED (consecutive_fail=%s/%s, reason=exception)",
                    _liveness_fail,
                    spec.failure_threshold,
                )
                return AggregateState.FAILED.value
            _LOG.info(
                "liveness probe SUBHEALTH (consecutive_fail=%s/%s, reason=exception)",
                _liveness_fail,
                spec.failure_threshold,
            )
            return AggregateState.SUBHEALTH.value
        if passed:
            _liveness_fail = 0
            _LOG.info("liveness probe HEALTHY (consecutive_fail=0/%s)", spec.failure_threshold)
            return AggregateState.HEALTHY.value
        _liveness_fail += 1
        if _liveness_fail >= spec.failure_threshold:
            _LOG.warning(
                "liveness probe FAILED (consecutive_fail=%s/%s, reason=action failed)",
                _liveness_fail,
                spec.failure_threshold,
            )
            return AggregateState.FAILED.value
        _LOG.info(
            "liveness probe SUBHEALTH (consecutive_fail=%s/%s, reason=action failed)",
            _liveness_fail,
            spec.failure_threshold,
        )
        return AggregateState.SUBHEALTH.value


def run_startup(startup: ProbeSpec) -> None:
    _LOG.info(
        "startup probe begin: action=%s, initial_delay=%ss, period=%ss, "
        "timeout=%ss, failure_threshold=%s",
        startup.action,
        startup.initial_delay_seconds,
        startup.period_seconds,
        startup.timeout_seconds,
        startup.failure_threshold,
    )
    if startup.initial_delay_seconds > 0:
        _LOG.info("startup probe waiting initial_delay=%ss", startup.initial_delay_seconds)
        time.sleep(startup.initial_delay_seconds)
    fail = 0
    while True:
        passed = _probe_once(startup.action, startup.timeout_seconds)
        if passed:
            _LOG.info("startup probe PASS after %s failed attempt(s)", fail)
            return
        fail += 1
        _LOG.info(
            "startup probe FAIL (attempt=%s, consecutive_fail=%s/%s)",
            fail,
            fail,
            startup.failure_threshold,
        )
        if fail >= startup.failure_threshold:
            _LOG.warning(
                "startup probe FAILED after %s consecutive failures (threshold=%s)",
                fail,
                startup.failure_threshold,
            )
            raise ProbeStartupError(
                f"startup probe failed {fail} consecutive times (threshold={startup.failure_threshold})"
            )
        time.sleep(startup.period_seconds)


class ProbeStartupError(RuntimeError):
    pass
