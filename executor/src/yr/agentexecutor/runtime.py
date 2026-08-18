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
from typing import Mapping, Optional

from .file_handler import DEFAULT_MAX_FILE_SIZE
from .http_server import ExecutorHTTPServer
from .process_manager import ProcessManager

EXECUTOR_HOST_ENV = "YR_AGENT_EXECUTOR_HOST"
EXECUTOR_PORT_ENV = "YR_AGENT_EXECUTOR_PORT"
EXECUTOR_MAX_FILE_SIZE_ENV = "YR_AGENT_EXECUTOR_MAX_FILE_SIZE"
EXECUTOR_SHUTDOWN_GRACE_ENV = "YR_AGENT_EXECUTOR_SHUTDOWN_GRACE_SECONDS"
PRE_STOP_TIMEOUT_ENV = "PRE_STOP_TIMEOUT"
DEFAULT_EXECUTOR_HOST = "0.0.0.0"
DEFAULT_EXECUTOR_PORT = 18093
DEFAULT_PRE_STOP_TIMEOUT = 10.0
SHUTDOWN_CLEANUP_RESERVE_SECONDS = 2.0

_LOG = logging.getLogger(__name__)


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
    """Starts the internal HTTP server and user processes exactly once."""

    def __init__(self) -> None:
        self._http_server: Optional[ExecutorHTTPServer] = None
        self._process_manager = ProcessManager()
        self._lock = threading.Lock()

    def start(self) -> None:
        with self._lock:
            if self._http_server is not None:
                return
            host = os.getenv(EXECUTOR_HOST_ENV, DEFAULT_EXECUTOR_HOST)
            port = int(os.getenv(EXECUTOR_PORT_ENV, str(DEFAULT_EXECUTOR_PORT)))
            max_file_size = int(os.getenv(EXECUTOR_MAX_FILE_SIZE_ENV, str(DEFAULT_MAX_FILE_SIZE)))
            server = ExecutorHTTPServer(host, port, max_file_size=max_file_size)
            server.start()
            self._http_server = server
            try:
                self._process_manager.start_from_env()
            except BaseException:
                server.stop()
                self._http_server = None
                raise

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

    def status(self) -> dict:
        with self._lock:
            return {
                "ready": self._http_server is not None,
                "processes": self._process_manager.status(),
            }


_RUNTIME = AgentExecutorRuntime()


def get_runtime() -> AgentExecutorRuntime:
    return _RUNTIME
