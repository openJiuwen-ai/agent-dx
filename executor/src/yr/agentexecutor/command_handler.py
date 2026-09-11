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

"""Process-local command execution backing the /v1/exec HTTP endpoint.

Runs commands with subprocess directly inside the executor process
container. Unlike the sandbox API it depends on neither Sandbox nor
SandboxManager and never goes through the sandbox RPC path.
"""

from __future__ import annotations

import os
import signal
import subprocess
from typing import Dict, List, Optional, Union

DEFAULT_EXECUTION_TIMEOUT_SECONDS = 300.0
PROCESS_KILL_WAIT_SECONDS = 2.0


class CommandHandler:
    """Execute commands via subprocess in the executor process.

    ``execute`` always returns a ``{"returncode", "stdout", "stderr"}`` dict
    and never raises; spawn failures and timeouts are reported through the
    returncode/stderr fields.
    """

    def __init__(
        self,
        execution_timeout_seconds: float = DEFAULT_EXECUTION_TIMEOUT_SECONDS,
    ) -> None:
        if execution_timeout_seconds <= 0:
            raise ValueError("execution_timeout_seconds must be greater than zero")
        self._execution_timeout_seconds = execution_timeout_seconds

    def execute(
        self,
        command: Union[str, List[str]],
        working_dir: Optional[str] = None,
        env: Optional[Dict[str, str]] = None,
        timeout: Optional[float] = None,
    ) -> dict:
        """Run ``command`` and return returncode/stdout/stderr without raising.

        A string command is executed through ``/bin/sh -c``; a list or tuple
        is executed argument-vector style. ``working_dir``/``env`` follow
        subprocess semantics (None inherits the executor process state).
        """
        if isinstance(command, str):
            cmd_args = ["/bin/sh", "-c", command]
        elif isinstance(command, (list, tuple)):
            if len(command) == 0:
                return {
                    "returncode": -1,
                    "stdout": "",
                    "stderr": "Error: cmd list cannot be empty",
                }
            if not all(isinstance(arg, str) for arg in command):
                return {
                    "returncode": -1,
                    "stdout": "",
                    "stderr": "Error: All elements in command list must be strings",
                }
            cmd_args = list(command)
        else:
            return {
                "returncode": -1,
                "stdout": "",
                "stderr": f"Error: cmd must be a string or a list of strings, got {type(command).__name__}",
            }

        effective_timeout = (
            self._execution_timeout_seconds if timeout is None else timeout
        )
        try:
            process = subprocess.Popen(
                args=cmd_args,
                shell=False,
                cwd=working_dir,
                env=env,
                stdout=subprocess.PIPE,
                stderr=subprocess.PIPE,
                text=True,
                start_new_session=True,
            )
            stdout, stderr = process.communicate(timeout=effective_timeout)
            return {
                "returncode": process.returncode,
                "stdout": stdout or "",
                "stderr": stderr or "",
            }
        except subprocess.TimeoutExpired:
            drained_stdout = self._kill_and_drain(process)
            return {
                "returncode": -1,
                "stdout": drained_stdout or "",
                "stderr": f"Command timed out after {effective_timeout:g} seconds",
            }
        except Exception as exc:  # noqa: BLE001 - endpoint contract: never raise
            return {"returncode": -1, "stdout": "", "stderr": str(exc)}

    @staticmethod
    def _kill_and_drain(process: subprocess.Popen) -> str:
        """Kill the process group, drain residual output; best effort."""
        try:
            os.killpg(process.pid, signal.SIGKILL)
        except ProcessLookupError:
            pass
        try:
            stdout, _ = process.communicate(timeout=PROCESS_KILL_WAIT_SECONDS)
        except subprocess.TimeoutExpired:
            try:
                process.stdout.close()
            except Exception:  # noqa: BLE001
                pass
            try:
                process.stderr.close()
            except Exception:  # noqa: BLE001
                pass
            try:
                process.wait()
            except Exception:  # noqa: BLE001
                pass
            stdout = ""
        return stdout or ""
