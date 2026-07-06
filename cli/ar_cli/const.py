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

"""Constants shared across the ar CLI."""

# --- API paths ---
FUNCTIONS_PATH = "/serverless/v1/functions"
INVOCATIONS_PATH = "/serverless/v1/functions/{urn}/invocations"

# --- Function spec ---
# Field injected into the function definition on `ar deploy` (defaults to True
# when the user does not set it).
ENABLE_SESSION_CTX_KEY = "enableSessionCtx"

# --- exec session defaults (configurable via CLI options) ---
DEFAULT_SESSION_TTL = 90
DEFAULT_INTERACTIVE_SESSION_TTL = 600
RELEASE_SESSION_TTL = 0
DEFAULT_CONCURRENCY = 1

# --- request headers ---
HEADER_CONTENT_TYPE = "Content-Type"
HEADER_ACCEPT = "Accept"
HEADER_AGENT_SESSION = "X-Session-Context"
HEADER_INSTANCE_SESSION = "X-Instance-Session"
CONTENT_TYPE_JSON = "application/json"
ACCEPT_SSE = "text/event-stream"

# --- SSE protocol ---
SSE_DATA_PREFIX = "data:"
SSE_DONE_MARKER = "[DONE]"

# --- exit codes ---
EXIT_SUCCESS = 0
EXIT_FAILURE = 1  # generic failure: non-2xx HTTP, response code != 0
EXIT_PARAM_ERROR = 2  # bad parameters (click default)
EXIT_NETWORK_ERROR = 3  # connection / timeout
