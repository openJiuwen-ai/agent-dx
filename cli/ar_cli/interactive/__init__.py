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

"""Interactive adx exec session management."""

__all__ = [
    "InteractiveCommandError",
    "InteractiveSessionState",
    "SLASH_COMMAND_COMPLETIONS",
    "handle_session_command",
]


def __getattr__(name):
    """Keep terminal-only imports independent from Click-backed command handlers."""
    if name in {"SLASH_COMMAND_COMPLETIONS", "handle_session_command"}:
        from ar_cli.interactive.registry import SLASH_COMMAND_COMPLETIONS, handle_session_command

        return {
            "SLASH_COMMAND_COMPLETIONS": SLASH_COMMAND_COMPLETIONS,
            "handle_session_command": handle_session_command,
        }[name]
    if name in {"InteractiveCommandError", "InteractiveSessionState"}:
        from ar_cli.interactive.state import InteractiveCommandError, InteractiveSessionState

        return {
            "InteractiveCommandError": InteractiveCommandError,
            "InteractiveSessionState": InteractiveSessionState,
        }[name]
    raise AttributeError(f"module {__name__!r} has no attribute {name!r}")
