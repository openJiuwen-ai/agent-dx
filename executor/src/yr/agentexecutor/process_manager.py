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

"""Lifecycle management for user processes inside an Agent instance."""

from __future__ import annotations

import json
import logging
import os
import signal
import subprocess
import threading
import time
from pathlib import Path
from typing import Mapping, Optional, Sequence

_LOG = logging.getLogger(__name__)

BOOTSTRAP_COMMAND_ENV = "YR_RUNTIME_BOOTSTRAP_CMD"
PROCESS_LOG_DIR_ENV = "GLOG_log_dir"
DEFAULT_PROCESS_LOG_DIR = "/home/snuser/log/"
MAX_BOOTSTRAP_COMMANDS = 64


def parse_bootstrap_commands(value: str) -> list[list[str]]:
    """Best-effort parse of bootstrap argv arrays, matching yr_runtime_main."""
    if not value:
        return []
    try:
        commands = json.loads(value)
    except (TypeError, ValueError) as exc:
        _LOG.warning("invalid %s (not JSON): %s", BOOTSTRAP_COMMAND_ENV, exc)
        return []
    if not isinstance(commands, list):
        _LOG.warning("invalid %s (expected a list, got %s)", BOOTSTRAP_COMMAND_ENV, type(commands).__name__)
        return []
    if len(commands) > MAX_BOOTSTRAP_COMMANDS:
        _LOG.warning(
            "%s has %d entries, only the first %d will start",
            BOOTSTRAP_COMMAND_ENV,
            len(commands),
            MAX_BOOTSTRAP_COMMANDS,
        )
        commands = commands[:MAX_BOOTSTRAP_COMMANDS]
    validated: list[list[str]] = []
    for index, command in enumerate(commands):
        if not isinstance(command, list) or not command or not all(isinstance(argument, str) for argument in command):
            _LOG.warning("skip invalid bootstrap command %d: %r", index, command)
            continue
        validated.append(command)
    return validated


class ProcessManager:
    """Starts user commands once and terminates their process groups on shutdown."""

    def __init__(self) -> None:
        self._processes: list[subprocess.Popen] = []
        self._log_files = []
        self._lock = threading.Lock()

    def start_from_env(self, environ: Optional[Mapping[str, str]] = None) -> None:
        active_env = os.environ if environ is None else environ
        commands = parse_bootstrap_commands(active_env.get(BOOTSTRAP_COMMAND_ENV, ""))
        log_dir = active_env.get(PROCESS_LOG_DIR_ENV, DEFAULT_PROCESS_LOG_DIR)
        self.start(commands, log_dir=log_dir)

    def start(self, commands: Sequence[Sequence[str]], *, log_dir: str) -> None:
        """Start commands independently; one failure does not fail initialization."""
        with self._lock:
            if self._processes:
                raise RuntimeError("user processes have already been started")
            log_directory_available = True
            try:
                Path(log_dir).mkdir(parents=True, exist_ok=True)
            except OSError as exc:
                _LOG.warning("cannot create bootstrap log directory %s: %s", log_dir, exc)
                log_directory_available = False
            for index, command in enumerate(commands[:MAX_BOOTSTRAP_COMMANDS]):
                log_file = None
                try:
                    if log_directory_available:
                        log_path = Path(log_dir) / f"bootstrap_cmd_{index}.log"
                        try:
                            log_file = log_path.open("ab", buffering=0)
                        except OSError as exc:
                            _LOG.warning("cannot open bootstrap log %s: %s", log_path, exc)
                    output = log_file if log_file is not None else subprocess.DEVNULL
                    process = subprocess.Popen(
                        list(command),
                        stdin=subprocess.DEVNULL,
                        stdout=output,
                        stderr=subprocess.STDOUT,
                        start_new_session=True,
                    )
                    self._processes.append(process)
                    if log_file is not None:
                        self._log_files.append(log_file)
                    _LOG.info("started user process pid=%s argv=%s", process.pid, list(command))
                except (OSError, ValueError) as exc:
                    _LOG.warning("failed to start bootstrap command %r: %s", list(command), exc)
                    if log_file is not None:
                        log_file.close()

    def status(self) -> list[dict]:
        with self._lock:
            return [
                {"pid": process.pid, "returncode": process.poll(), "running": process.poll() is None}
                for process in self._processes
            ]

    def stop(self, grace_period_seconds: float = 10) -> None:
        with self._lock:
            self._stop_locked(grace_period_seconds)

    def _stop_locked(self, grace_period_seconds: float) -> None:
        running = [process for process in self._processes if process.poll() is None]
        for process in running:
            self._signal_process_group(process, signal.SIGTERM)

        deadline = time.monotonic() + max(0, grace_period_seconds)
        for process in running:
            remaining = deadline - time.monotonic()
            if remaining <= 0:
                break
            try:
                process.wait(timeout=remaining)
            except subprocess.TimeoutExpired:
                break

        for process in running:
            if process.poll() is None:
                self._signal_process_group(process, signal.SIGKILL)
        for process in running:
            if process.poll() is None:
                try:
                    process.wait(timeout=1)
                except subprocess.TimeoutExpired:
                    _LOG.warning("user process did not exit after SIGKILL pid=%s", process.pid)

        self._processes.clear()
        for log_file in self._log_files:
            log_file.close()
        self._log_files.clear()

    @staticmethod
    def _signal_process_group(process: subprocess.Popen, sig: signal.Signals) -> None:
        try:
            os.killpg(process.pid, sig)
        except ProcessLookupError:
            return
        except OSError:
            _LOG.exception("failed to signal user process group pid=%s signal=%s", process.pid, sig)
