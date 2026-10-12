#!/usr/bin/env python3
# coding=UTF-8
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

"""Test-time stubs for the external ``yr`` SDK.

The ``agentexecutor`` package is pure-Python and its unit tests never call into
the real YuanRong runtime. But importing ``yr.agentexecutor`` pulls in
``sandbox.sandbox``, whose top-level imports reference SDK modules
(``yr.config``, ``yr.runtime_holder``, ``yr.config_manager``) that are not
installed in CI and must not be installed there.

This conftest is collected by pytest before any test module is imported. It
sets up ``yr`` so that:

* ``yr`` is a namespace package rooted at ``executor/src/yr`` (it has no
  ``__init__.py`` in the repo), so ``yr.agentexecutor`` keeps resolving from
  the source tree as usual.
* The missing SDK submodules (``yr.config`` etc.) and the few ``yr``
  attributes that ``sandbox.py`` references at import time (the ``yr.instance``
  decorator, plus ``yr.get``/``yr.Config``/``yr.InvokeOptions``/
  ``yr.PortForwarding`` used inside method bodies) are replaced with
  lightweight stand-ins.

The test suite never exercises those method bodies, so the stubs only need to
be importable, not faithful.
"""

from __future__ import annotations

import os
import sys
import types
from unittest.mock import MagicMock

# executor/src is on sys.path via pytest's ``pythonpath`` setting, but that is
# applied after conftest collection begins. Make it available here so the ``yr``
# namespace package below points at the real source tree.
_REPO_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
_EXECUTOR_SRC = os.path.join(_REPO_ROOT, "executor", "src")
if _EXECUTOR_SRC not in sys.path:
    # G.PSL.03: append (not insert(0)) so an installed same-named package on a
    # developer's sys.path is not silently shadowed by the source tree.
    sys.path.append(_EXECUTOR_SRC)

_YR_DIR = os.path.join(_EXECUTOR_SRC, "yr")


def _identity_decorator(cls):
    """Stand-in for ``@yr.instance``: return the class unchanged."""
    return cls


def _passthrough_get(ref):
    """Stand-in for ``yr.get``: return the reference unchanged."""
    return ref


def _noop_init(*args, **kwargs):
    """Stand-in for ``yr.init``: no-op, tests never exercise the real runtime."""
    return None


def _install_yr_stubs() -> None:
    """Ensure ``yr`` and the SDK submodules referenced at import time resolve.

    Idempotent: safe to invoke multiple times (e.g. across conftest reloads).
    """
    existing = sys.modules.get("yr")
    if existing is None or not hasattr(existing, "__path__"):
        # Build a namespace package so ``yr.agentexecutor`` resolves from
        # executor/src. A plain ModuleType without __path__ would make any
        # ``from yr.X import ...`` fail with "'yr' is not a package".
        yr = types.ModuleType("yr")
        yr.__path__ = [_YR_DIR]  # type: ignore[attr-defined]
        yr.__package__ = "yr"
        sys.modules["yr"] = yr
    yr = sys.modules["yr"]
    # If ``yr`` loaded as a real namespace package (installed SDK or another
    # source), make the source tree a participating location too. Namespace
    # packages support multiple __path__ entries, so ``yr.agentexecutor`` still
    # resolves from executor/src without shadowing the installed package.
    if _YR_DIR not in yr.__path__:
        yr.__path__.append(_YR_DIR)

    # Attributes used at module top of sandbox.py (``@yr.instance`` decorator
    # runs at class-definition time) plus a few referenced inside method bodies.
    if not hasattr(yr, "instance"):
        yr.instance = _identity_decorator
    if not hasattr(yr, "init"):
        # runtime.start() calls yr.init() explicitly (idempotent by design).
        yr.init = _noop_init
    if not hasattr(yr, "get"):
        yr.get = _passthrough_get
    if not hasattr(yr, "Config"):
        yr.Config = type("Config", (), {})
    if not hasattr(yr, "InvokeOptions"):
        yr.InvokeOptions = type("InvokeOptions", (), {})

    # yr.config — InvokeOptions / PortForwarding are imported by name at top of
    # sandbox.py. Provide dataclass-like stubs that accept arbitrary kwargs.
    if "yr.config" not in sys.modules or not hasattr(sys.modules["yr.config"], "InvokeOptions"):
        yr_config = types.ModuleType("yr.config")

        class _InvokeOptions:
            def __init__(self, **kwargs: object) -> None:
                self.skip_serialize = False
                self.idle_timeout = 300
                self.cpu = None
                self.memory = None
                self.name = None
                self.namespace = None
                self.env_vars = None
                self.runtime_env: dict = {}
                self.port_forwardings: list = []
                self.custom_extensions: dict = {}
                self.trace_id = ""
                self.recover_retry_times = 3

        class _PortForwarding:
            def __init__(self, port: int = 0, protocol: str = "TCP") -> None:
                self.port = port
                self.protocol = protocol

        yr_config.InvokeOptions = _InvokeOptions
        yr_config.PortForwarding = _PortForwarding
        sys.modules["yr.config"] = yr_config
        yr.config = yr_config

    # yr.runtime_holder — only ``global_runtime`` is imported at top of sandbox.py.
    if "yr.runtime_holder" not in sys.modules or not hasattr(sys.modules["yr.runtime_holder"], "global_runtime"):
        yr_runtime_holder = types.ModuleType("yr.runtime_holder")
        yr_runtime_holder.global_runtime = MagicMock(name="global_runtime")
        sys.modules["yr.runtime_holder"] = yr_runtime_holder
        yr.runtime_holder = yr_runtime_holder

    # yr.config_manager — only ``ConfigManager`` is imported at top of sandbox.py.
    if "yr.config_manager" not in sys.modules or not hasattr(sys.modules["yr.config_manager"], "ConfigManager"):
        yr_config_manager = types.ModuleType("yr.config_manager")
        yr_config_manager.ConfigManager = type("ConfigManager", (), {})
        sys.modules["yr.config_manager"] = yr_config_manager
        yr.config_manager = yr_config_manager


_install_yr_stubs()
