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

import argparse
import http.client
import json
import logging
import math
import ssl
import statistics
import sys
import threading
import time
from collections import defaultdict
from concurrent.futures import ThreadPoolExecutor
from urllib.parse import urlsplit


# Keep machine-readable results on stdout without diagnostic prefixes.
result_logger = logging.getLogger(__name__ + ".result")
result_logger.setLevel(logging.INFO)
result_logger.propagate = False
result_handler = logging.StreamHandler(sys.stdout)
result_handler.setFormatter(logging.Formatter("%(message)s"))
result_logger.addHandler(result_handler)


def percentile(values, quantile):
    return values[min(len(values) - 1, max(0, math.ceil(len(values) * quantile) - 1))]


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("url")
    parser.add_argument("--requests", type=int, default=100)
    parser.add_argument("--concurrency", type=int, default=1)
    parser.add_argument("--warmup", type=int, default=5)
    parser.add_argument("--ca")
    parser.add_argument("--token")
    parser.add_argument("--path-template")
    parser.add_argument("--targets", type=int, default=1)
    args = parser.parse_args()
    if args.requests < 1 or args.concurrency < 1 or args.targets < 1:
        parser.error("requests, concurrency and targets must be positive")

    parsed = urlsplit(args.url)
    if parsed.scheme not in ("http", "https") or not parsed.hostname:
        parser.error("url must use http or https")
    port = parsed.port or (443 if parsed.scheme == "https" else 80)
    path = parsed.path or "/"
    if parsed.query:
        path += "?" + parsed.query
    headers = {}
    if args.token:
        headers["Authorization"] = "Bearer " + args.token
    tls_context = ssl.create_default_context(cafile=args.ca) if parsed.scheme == "https" else None

    lock = threading.Lock()
    samples = []
    target_samples = defaultdict(list)
    bytes_received = 0
    failures = []
    measurement_started = [0.0]
    measurement_ended = [0.0]
    barrier = threading.Barrier(
        args.concurrency, action=lambda: measurement_started.__setitem__(0, time.perf_counter())
    )
    allocations = [args.requests // args.concurrency] * args.concurrency
    for index in range(args.requests % args.concurrency):
        allocations[index] += 1

    def request_path(target):
        if args.path_template:
            return args.path_template.format(target=target)
        return path

    def worker(worker_index, count):
        nonlocal bytes_received
        connection_class = http.client.HTTPSConnection if tls_context else http.client.HTTPConnection
        kwargs = {"context": tls_context} if tls_context else {}
        connection = connection_class(parsed.hostname, port, timeout=10, **kwargs)
        connection.connect()
        try:
            for _ in range(args.warmup):
                connection.request("GET", request_path(worker_index % args.targets), headers=headers)
                response = connection.getresponse()
                response.read()
                if response.status != 200 or response.will_close or connection.sock is None:
                    raise RuntimeError(
                        f"warmup is not reusable: status={response.status} will_close={response.will_close}"
                    )
            barrier.wait()
            local_samples = []
            local_target_samples = defaultdict(list)
            local_bytes = 0
            for request_index in range(count):
                target = (worker_index + request_index * args.concurrency) % args.targets
                started = time.perf_counter()
                connection.request("GET", request_path(target), headers=headers)
                response = connection.getresponse()
                body = response.read()
                elapsed = (time.perf_counter() - started) * 1000.0
                if response.status != 200:
                    raise RuntimeError(f"unexpected HTTP status {response.status}")
                if response.will_close or connection.sock is None:
                    raise RuntimeError("server closed the persistent connection")
                local_samples.append(elapsed)
                local_target_samples[target].append(elapsed)
                local_bytes += len(body)
            with lock:
                samples.extend(local_samples)
                for target, values in local_target_samples.items():
                    target_samples[target].extend(values)
                bytes_received += local_bytes
                measurement_ended[0] = max(measurement_ended[0], time.perf_counter())
        except Exception as error:  # surfaced in the JSON and process status
            barrier.abort()
            with lock:
                failures.append(repr(error))
        finally:
            connection.close()

    with ThreadPoolExecutor(max_workers=args.concurrency) as executor:
        list(executor.map(worker, range(args.concurrency), allocations))
    elapsed = max(0.0, measurement_ended[0] - measurement_started[0])
    samples.sort()
    result = {
        "url": args.url,
        "requests": len(samples),
        "concurrency": args.concurrency,
        "connection_count": args.concurrency,
        "logical_targets": args.targets,
        "errors": len(failures),
        "elapsed_ms": round(elapsed * 1000.0, 3),
        "requests_per_second": round(len(samples) / elapsed, 3) if elapsed else 0,
        "bytes_received": bytes_received,
        "throughput_mib_s": round(bytes_received / 1024 / 1024 / elapsed, 3) if elapsed else 0,
    }
    if samples:
        result.update(
            p50_ms=round(statistics.median(samples), 3),
            p95_ms=round(percentile(samples, 0.95), 3),
            p99_ms=round(percentile(samples, 0.99), 3),
        )
        target_p99 = [percentile(sorted(values), 0.99) for values in target_samples.values()]
        target_counts = [len(values) for values in target_samples.values()]
        result.update(
            target_request_count_min=min(target_counts),
            target_request_count_max=max(target_counts),
            target_p99_ms_min=round(min(target_p99), 3),
            target_p99_ms_max=round(max(target_p99), 3),
        )
    if failures:
        result["failure_samples"] = failures[:3]
    result_logger.info("%s", json.dumps(result, sort_keys=True))
    if failures or len(samples) != args.requests:
        raise SystemExit(1)


if __name__ == "__main__":
    main()
