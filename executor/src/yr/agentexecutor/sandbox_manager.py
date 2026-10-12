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

"""Manage multiple Sandbox instances by id for the executor HTTP API.

The manager is created/owned by ``AgentExecutorRuntime`` and injected into
``ExecutorHTTPServer``; the HTTP server only holds the reference injected here
and never constructs a remote sandbox on its own. ``create`` returns the
platform instance id which callers put in the URL path ({id}) of
subsequent execute/read/write/list/search/delete requests.
"""

from __future__ import annotations

import logging
import threading
from dataclasses import asdict
from typing import Dict, Optional

from .sandbox.sandbox import Sandbox, SandboxCreateOptions

_LOG = logging.getLogger(__name__)


class SandboxManager:
    """Holds Sandbox instances keyed by the platform instance id.

    Thread-safe: a ``threading.Lock`` guards the internal dict. ``create``
    creates the remote child container, records it under its instance_id, and
    rolls back (terminate) on failure so no orphan container is leaked.
    """

    def __init__(self) -> None:
        self._sandboxes: Dict[str, Sandbox] = {}
        self._lock = threading.Lock()

    def create(self, options: SandboxCreateOptions) -> str:
        """Create a child sandbox and return its platform instance id.

        Raises ``RuntimeError`` (wrapped with "create sandbox failed: ...")
        if creation or id retrieval fails; on id-retrieval failure the already
        created sandbox is terminated to avoid leaking a remote container.
        """
        sandbox: Optional[Sandbox] = None
        try:
            sandbox = Sandbox(**asdict(options))
            instance_id = sandbox.get_instance_id()
            if not instance_id:
                raise RuntimeError("create sandbox failed: empty instance id")
            with self._lock:
                self._sandboxes[instance_id] = sandbox
            return instance_id
        except Exception as exc:
            # Roll back the partially-created sandbox so we don't leak a remote
            # container when id retrieval fails after the instance was created.
            if sandbox is not None:
                try:
                    sandbox.terminate()
                except Exception:  # noqa: BLE001 - best-effort rollback
                    _LOG.debug("sandbox rollback terminate failed", exc_info=True)
            if isinstance(exc, RuntimeError) and str(exc).startswith("create sandbox failed"):
                raise
            raise RuntimeError(f"create sandbox failed: {exc}") from exc

    def delete(self, instance_id: str) -> bool:
        """Terminate and remove the sandbox for ``instance_id``.

        Idempotent at the manager level: returns ``False`` if the id is
        unknown (no RPC issued, no exception) — mirrors the runtime SDK where
        ``InstanceProxy.terminate`` no-ops for an inactive instance.
        Re-raises terminate RPC failures so the HTTP layer can map them to
        500 (doc contract: only底层 terminate RPC 异常时返 500).
        """
        with self._lock:
            sandbox = self._sandboxes.pop(instance_id, None)
            if sandbox is None:
                return False
        sandbox.terminate()
        return True

    def get(self, instance_id: str) -> Optional[Sandbox]:
        """Return the sandbox for ``instance_id`` or ``None`` if not found."""
        with self._lock:
            return self._sandboxes.get(instance_id)

    def register(self, instance_id: str, sandbox: Sandbox) -> None:
        """Record an already-created sandbox under ``instance_id``.

        Unlike :meth:`create` this does not spawn anything and issues no RPC —
        it only registers the given ``Sandbox`` object so subsequent
        execute/read/write/list/search/delete calls route to it. Intended for
        recovery-style re-registration of a sandbox whose creation is managed
        outside this manager (and for tests that seed a stub sandbox).
        """
        with self._lock:
            self._sandboxes[instance_id] = sandbox

    def terminate_all(self) -> None:
        """Terminate every managed sandbox. Used by ``AgentExecutorRuntime.stop``."""
        with self._lock:
            sandboxes = list(self._sandboxes.values())
            self._sandboxes.clear()
        for sandbox in sandboxes:
            try:
                sandbox.terminate()
            except Exception:  # noqa: BLE001 - best-effort during shutdown
                _LOG.debug("sandbox terminate failed", exc_info=True)
