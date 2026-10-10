#!/usr/bin/env python3
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

"""Prove that an open data-plane stream survives a byte-idle interval."""

import logging
import socket
import sys
import time


# Keep machine-readable results on stdout without diagnostic prefixes.
result_logger = logging.getLogger(__name__ + ".result")
result_logger.setLevel(logging.INFO)
result_logger.propagate = False
result_handler = logging.StreamHandler(sys.stdout)
result_handler.setFormatter(logging.Formatter("%(message)s"))
result_logger.addHandler(result_handler)


host = sys.argv[1]
port = int(sys.argv[2])
idle_seconds = float(sys.argv[3])

with socket.create_connection((host, port), timeout=5) as connection:
    connection.settimeout(5)
    connection.sendall(b"before-idle")
    if connection.recv(11) != b"before-idle":
        raise AssertionError("echo mismatch before the idle interval")
    started = time.monotonic()
    time.sleep(idle_seconds)
    connection.sendall(b"after-idle")
    if connection.recv(10) != b"after-idle":
        raise AssertionError("echo mismatch after the idle interval")
    elapsed = time.monotonic() - started

result_logger.info("idle_stream_survived_seconds=%.3f", elapsed)
