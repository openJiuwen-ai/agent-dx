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

"""Unified errors carrying their own process exit code."""

from ar_cli.const import EXIT_FAILURE, EXIT_NETWORK_ERROR


class ArError(Exception):
    """Base error for the adx CLI. ``exit_code`` maps to the process exit code."""

    exit_code = EXIT_FAILURE


class ApiError(ArError):
    """Server returned a non-2xx status or a non-zero business ``code``."""

    exit_code = EXIT_FAILURE

    def __init__(self, message: str, status_code: int = None, service_code=None) -> None:
        super().__init__(message)
        self.status_code = status_code
        self.service_code = service_code


class NetworkError(ArError):
    """Could not reach the server (connection refused, timeout, DNS, ...)."""

    exit_code = EXIT_NETWORK_ERROR
