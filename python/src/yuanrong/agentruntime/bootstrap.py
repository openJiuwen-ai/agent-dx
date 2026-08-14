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

"""Fixed FaaS initializer and handler for Agent functions."""

from __future__ import annotations

import asyncio
import importlib
import logging
import threading
from enum import Enum
from typing import Any, Callable, Coroutine, Optional

from .context import SessionContext
from .dispatcher import Dispatcher
from .errors import (
    AgentExecutorLoadFailed,
    AgentInitFailed,
    AgentRuntimeError,
    AgentRuntimeNotConfigured,
    SessionContextBindingMismatch,
)
from .event_log import EventLog
from .executor import AgentExecutor
from .keys import SessionKeys
from .metadata import RuntimeMetadata
from .storage import DataSystemKVStore, KVStore
from .turn_writer import TurnWriter

_LOG = logging.getLogger(__name__)


class RuntimeState(str, Enum):
    STARTING = "STARTING"
    READY = "READY"
    START_FAILED = "START_FAILED"


class AgentRuntime:
    """Process-local runtime bound to one SessionContext instance."""

    def __init__(self):
        self._state = RuntimeState.STARTING
        self._start_error: Optional[AgentRuntimeError] = None
        self._dispatcher: Optional[Dispatcher] = None
        self._bound_metadata: Optional[RuntimeMetadata] = None
        self._event_loop: Optional[asyncio.AbstractEventLoop] = None
        self._event_loop_thread: Optional[threading.Thread] = None
        self._lock = threading.Lock()

    @property
    def state(self) -> RuntimeState:
        return self._state

    def initialize(
        self,
        function_context: object,
        *,
        store: Optional[KVStore] = None,
        executor_factory: Optional[Callable[[], AgentExecutor]] = None,
    ) -> None:
        """Initialize once and cache failure without failing FaaS instance startup."""
        with self._lock:
            if self._state is RuntimeState.START_FAILED:
                return
            try:
                metadata = RuntimeMetadata.from_function_context(function_context)
                if self._state is RuntimeState.READY:
                    if metadata != self._bound_metadata:
                        self._start_error = SessionContextBindingMismatch()
                        self._state = RuntimeState.START_FAILED
                    return
                self._bound_metadata = metadata
                self._ensure_event_loop()
                self._run_coroutine(
                    self._initialize_async(
                        metadata,
                        store=store,
                        executor_factory=executor_factory,
                    )
                )
            except Exception as exc:
                _LOG.exception("Agent Runtime initialization failed")
                self._start_error = _startup_error(exc)
                self._state = RuntimeState.START_FAILED

    def invoke(self, message: Any, function_context: object) -> Any:
        """Execute one invocation under the process-local serial lock."""
        with self._lock:
            stream = _require_stream(function_context)
            if self._state is RuntimeState.START_FAILED:
                if self._start_error is None:
                    raise AgentRuntimeError(
                        "AGENT_RUNTIME_START_FAILED",
                        "Agent Runtime initialization failed; see instance logs",
                    )
                raise self._start_error
            if self._state is not RuntimeState.READY or self._dispatcher is None:
                raise AgentRuntimeNotConfigured(
                    "Agent Runtime initializer has not completed; configure "
                    "yuanrong.agentruntime.bootstrap.initialize"
                )
            metadata = RuntimeMetadata.from_function_context(function_context)
            if metadata != self._bound_metadata:
                raise SessionContextBindingMismatch()
            return self._run_coroutine(self._dispatcher.dispatch(message, stream))

    async def _initialize_async(
        self,
        metadata: RuntimeMetadata,
        *,
        store: Optional[KVStore],
        executor_factory: Optional[Callable[[], AgentExecutor]],
    ) -> None:
        if executor_factory is None:
            executor = _load_executor()
        else:
            executor = executor_factory()
            if not isinstance(executor, AgentExecutor):
                raise AgentExecutorLoadFailed("executor factory returned an invalid Agent")

        active_store = store or await DataSystemKVStore.create(metadata.tenant_id)
        keys = SessionKeys(
            metadata.tenant_id,
            metadata.function_name,
            metadata.function_version,
            metadata.session_context_id,
        )
        writer = TurnWriter(active_store, keys, metadata.session_context_id)
        await writer.recover()
        event_log = EventLog(writer)
        session_context = SessionContext(metadata.session_context_id, event_log)
        try:
            await executor.init(session_context)
        except AgentRuntimeError:
            raise
        except Exception as exc:
            raise AgentInitFailed("Agent init() failed; see instance logs") from exc
        self._dispatcher = Dispatcher(executor, session_context, writer)
        self._state = RuntimeState.READY

    def close(self) -> None:
        """Stop the private event loop used by this runtime."""
        with self._lock:
            loop = self._event_loop
            thread = self._event_loop_thread
            self._event_loop = None
            self._event_loop_thread = None
        if loop is not None and loop.is_running():
            loop.call_soon_threadsafe(loop.stop)
        if thread is not None and thread is not threading.current_thread():
            thread.join(timeout=5)

    def _ensure_event_loop(self) -> None:
        if self._event_loop is not None and self._event_loop.is_running():
            return

        ready = threading.Event()
        startup_error: list[BaseException] = []

        def run_event_loop() -> None:
            loop: Optional[asyncio.AbstractEventLoop] = None
            try:
                loop = asyncio.new_event_loop()
                asyncio.set_event_loop(loop)
                self._event_loop = loop
                loop.call_soon(ready.set)
                loop.run_forever()
            except BaseException as exc:
                startup_error.append(exc)
                ready.set()
            finally:
                if loop is not None:
                    pending = asyncio.all_tasks(loop)
                    for task in pending:
                        task.cancel()
                    if pending:
                        loop.run_until_complete(
                            asyncio.gather(*pending, return_exceptions=True)
                        )
                    loop.run_until_complete(loop.shutdown_asyncgens())
                    loop.close()

        thread = threading.Thread(
            target=run_event_loop,
            name="yuanrong-agent-runtime-loop",
            daemon=True,
        )
        self._event_loop_thread = thread
        thread.start()
        ready.wait()
        if startup_error:
            raise RuntimeError("failed to start Agent Runtime event loop") from startup_error[0]
        if self._event_loop is None or not self._event_loop.is_running():
            raise RuntimeError("Agent Runtime event loop did not start")

    def _run_coroutine(self, coroutine: Coroutine[Any, Any, Any]) -> Any:
        loop = self._event_loop
        if loop is None or not loop.is_running():
            raise RuntimeError("Agent Runtime event loop is not running")
        future = asyncio.run_coroutine_threadsafe(coroutine, loop)
        return future.result()


def _load_executor() -> AgentExecutor:
    try:
        module = importlib.import_module("agent")
        agent_class = getattr(module, "Agent")
        if not isinstance(agent_class, type) or not issubclass(agent_class, AgentExecutor):
            raise TypeError("agent.Agent must inherit AgentExecutor")
        executor = agent_class()
    except Exception as exc:
        raise AgentExecutorLoadFailed(
            "failed to load fixed entry agent.Agent; see instance logs"
        ) from exc
    return executor


def _require_stream(function_context: object) -> object:
    try:
        stream = function_context.get_stream()
    except Exception as exc:
        raise AgentRuntimeError(
            "SSE_STREAM_REQUIRED",
            "FunctionContext does not provide an SSE stream",
        ) from exc
    if stream is None or not callable(getattr(stream, "write", None)):
        raise AgentRuntimeError(
            "SSE_STREAM_REQUIRED",
            "Agent functions must be invoked with an SSE stream",
        )
    return stream


def _startup_error(exc: Exception) -> AgentRuntimeError:
    if isinstance(exc, AgentRuntimeError):
        summaries = {
            "AGENT_RUNTIME_NOT_CONFIGURED": (
                "Agent Runtime is not configured; see instance logs"
            ),
            "AGENT_EXECUTOR_LOAD_FAILED": "failed to load agent.Agent; see instance logs",
            "AGENT_INIT_FAILED": "Agent init() failed; see instance logs",
            "DATASYSTEM_ERROR": "DataSystem initialization failed; see instance logs",
        }
        return AgentRuntimeError(
            exc.code,
            summaries.get(exc.code, "Agent Runtime initialization failed; see instance logs"),
        )
    return AgentRuntimeError(
        "AGENT_RUNTIME_START_FAILED",
        "Agent Runtime initialization failed; see instance logs",
    )


_RUNTIME = AgentRuntime()


def initialize(function_context: object) -> None:
    """FaaS initializer entrypoint. Errors are cached, never propagated."""
    _RUNTIME.initialize(function_context)


def handler(event: Any, function_context: object) -> Any:
    """FaaS invocation entrypoint."""
    return _RUNTIME.invoke(event, function_context)
