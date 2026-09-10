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

"""Process-local lifecycle state for the Agent executor."""

from __future__ import annotations

import logging
import os
import threading
from dataclasses import asdict
from typing import Any, Mapping, Optional

from .file_handler import DEFAULT_MAX_FILE_SIZE
from .http_server import ExecutorHTTPServer
from .process_manager import ProcessManager
from .probe import ProbeStartupError, _configure_liveness, _load_probe_set, run_startup
from .sandbox.sandbox import SandboxCreateOptions
from .sandbox_manager import SandboxManager

EXECUTOR_HOST_ENV = "YR_AGENT_EXECUTOR_HOST"
EXECUTOR_PORT_ENV = "YR_AGENT_EXECUTOR_PORT"
EXECUTOR_MAX_FILE_SIZE_ENV = "YR_AGENT_EXECUTOR_MAX_FILE_SIZE"
EXECUTOR_SHUTDOWN_GRACE_ENV = "YR_AGENT_EXECUTOR_SHUTDOWN_GRACE_SECONDS"
PRE_STOP_TIMEOUT_ENV = "PRE_STOP_TIMEOUT"
TRACE_ID_ENV = "YR_TRACE_ID"
INSTANCE_ID_ENV = "INSTANCE_ID"
RUNTIME_ID_ENV = "YR_RUNTIME_ID"
DEFAULT_EXECUTOR_HOST = "0.0.0.0"
DEFAULT_EXECUTOR_PORT = 18093
DEFAULT_PRE_STOP_TIMEOUT = 10.0
SHUTDOWN_CLEANUP_RESERVE_SECONDS = 2.0

SANDBOX_TYPE_ENV = "YR_AGENT_SANDBOX_TYPE"
SANDBOX_IMAGE_ENV = "YR_AGENT_SANDBOX_IMAGE"
SANDBOX_CPU_ENV = "YR_AGENT_SANDBOX_CPU"
SANDBOX_MEMORY_ENV = "YR_AGENT_SANDBOX_MEMORY"
SANDBOX_PORTS_ENV = "YR_AGENT_SANDBOX_PORTS"
SANDBOX_UPSTREAM_ENV = "YR_AGENT_SANDBOX_UPSTREAM"
SANDBOX_PROXY_PORT_ENV = "YR_AGENT_SANDBOX_PROXY_PORT"
SANDBOX_IDLE_TIMEOUT_ENV = "YR_AGENT_SANDBOX_IDLE_TIMEOUT"
DEFAULT_SANDBOX_PROXY_PORT = 8766
DEFAULT_SANDBOX_IDLE_TIMEOUT = 300

_LOG = logging.getLogger(__name__)


def _build_sandbox_options(environ: Mapping[str, str]) -> SandboxCreateOptions:
    """Build SandboxCreateOptions from YR_AGENT_SANDBOX_* env (see 5.5)."""
    sandbox_type = environ.get(SANDBOX_TYPE_ENV, "")
    image = environ.get(SANDBOX_IMAGE_ENV) or None
    cpu_raw = environ.get(SANDBOX_CPU_ENV)
    memory_raw = environ.get(SANDBOX_MEMORY_ENV)
    ports_raw = environ.get(SANDBOX_PORTS_ENV)
    upstream = environ.get(SANDBOX_UPSTREAM_ENV) or None
    proxy_port = _int_env(environ, SANDBOX_PROXY_PORT_ENV, DEFAULT_SANDBOX_PROXY_PORT)
    idle_timeout = _int_env(environ, SANDBOX_IDLE_TIMEOUT_ENV, DEFAULT_SANDBOX_IDLE_TIMEOUT)

    ports = None
    if ports_raw:
        ports = [part.strip() for part in ports_raw.split(",") if part.strip()]

    cpu = int(cpu_raw) if cpu_raw else None
    memory = int(memory_raw) if memory_raw else None

    return SandboxCreateOptions(
        image=image,
        cpu=cpu,
        memory=memory,
        ports=ports,
        upstream=upstream,
        proxy_port=proxy_port,
        idle_timeout=idle_timeout,
        sandbox_type=sandbox_type,
    )


def _int_env(environ: Mapping[str, str], name: str, default: int) -> int:
    raw = environ.get(name)
    if not raw:
        return default
    try:
        return int(raw)
    except (TypeError, ValueError):
        _LOG.warning("invalid %s; using default %s", name, default)
        return default


def resolve_process_shutdown_grace(environ: Optional[Mapping[str, str]] = None) -> float:
    """Fit the child-process grace period inside the FaaS pre-stop timeout."""
    active_env = os.environ if environ is None else environ
    try:
        pre_stop_timeout = float(active_env.get(PRE_STOP_TIMEOUT_ENV, str(DEFAULT_PRE_STOP_TIMEOUT)))
    except (TypeError, ValueError):
        _LOG.warning("invalid %s; using default", PRE_STOP_TIMEOUT_ENV)
        pre_stop_timeout = DEFAULT_PRE_STOP_TIMEOUT
    budget = max(0.0, pre_stop_timeout - SHUTDOWN_CLEANUP_RESERVE_SECONDS)
    configured = active_env.get(EXECUTOR_SHUTDOWN_GRACE_ENV)
    if configured is None:
        return budget
    try:
        return min(max(0.0, float(configured)), budget)
    except (TypeError, ValueError):
        _LOG.warning("invalid %s; using pre-stop budget", EXECUTOR_SHUTDOWN_GRACE_ENV)
        return budget


class AgentExecutorRuntime:
    """Starts the internal HTTP server, user processes, and sandbox manager once."""

    def __init__(self) -> None:
        self._http_server: Optional[ExecutorHTTPServer] = None
        self._process_manager = ProcessManager()
        self._sandbox_manager: Optional[SandboxManager] = None
        self._lock = threading.RLock()

    def start(self) -> None:
        with self._lock:
            if self._http_server is not None:
                return
            trace_id = os.getenv(TRACE_ID_ENV, "")
            instance_id = os.getenv(INSTANCE_ID_ENV, "")
            runtime_id = os.getenv(RUNTIME_ID_ENV, "")
            line = "[agent.start.enter] agentexecutor"
            if trace_id:
                line += f" trace_id={trace_id}"
            if instance_id:
                line += f" instance_id={instance_id}"
            if runtime_id:
                line += f" runtime_id={runtime_id}"
            _LOG.info(line)
            startup, liveness = _load_probe_set()
            import yr

            yr.init()
            host = os.getenv(EXECUTOR_HOST_ENV, DEFAULT_EXECUTOR_HOST)
            port = int(os.getenv(EXECUTOR_PORT_ENV, str(DEFAULT_EXECUTOR_PORT)))
            max_file_size = int(os.getenv(EXECUTOR_MAX_FILE_SIZE_ENV, str(DEFAULT_MAX_FILE_SIZE)))
            self._sandbox_manager = SandboxManager()
            server = ExecutorHTTPServer(
                host, port, max_file_size=max_file_size, sandbox_manager=self._sandbox_manager
            )
            server.start()
            self._http_server = server
            try:
                self._process_manager.start_from_env()
            except BaseException:
                server.stop()
                self._http_server = None
                self._terminate_sandboxes()
                raise
            if startup is not None:
                try:
                    run_startup(startup)
                except ProbeStartupError:
                    self._cleanup_on_startup_failure()
                    raise
            # Liveness is armed only after the startup gate (if any) passes.
            # Until then probe_liveness() returns DISABLED so the yr runtime
            # heartbeat callback keeps the instance in CREATING instead of
            # racing ahead to RUNNING while startup is still blocking.
            _configure_liveness(liveness)

    def stop(self) -> None:
        with self._lock:
            grace = resolve_process_shutdown_grace()
            server = self._http_server
            self._http_server = None
            try:
                if server is not None:
                    server.stop()
            finally:
                self._process_manager.stop(grace)
                self._terminate_sandboxes()

    def _cleanup_on_startup_failure(self) -> None:
        with self._lock:
            server = self._http_server
            self._http_server = None
            grace = resolve_process_shutdown_grace()
            try:
                if server is not None:
                    server.stop()
            finally:
                self._process_manager.stop(grace)

    def status(self) -> dict:
        with self._lock:
            return {
                "ready": self._http_server is not None,
                "processes": self._process_manager.status(),
            }

    def _terminate_sandboxes(self) -> None:
        """Best-effort terminate all managed sandboxes during shutdown."""
        manager = self._sandbox_manager
        self._sandbox_manager = None
        if manager is None:
            return
        try:
            manager.terminate_all()
        except Exception:  # noqa: BLE001 - best-effort during shutdown
            _LOG.debug("sandbox manager terminate_all failed", exc_info=True)


_RUNTIME = AgentExecutorRuntime()


def get_runtime() -> AgentExecutorRuntime:
    return _RUNTIME
