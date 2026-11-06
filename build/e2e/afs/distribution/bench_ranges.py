#!/usr/bin/env python3
"""Guest HTTP range load probe; warmed backends, fixed bytes, no Nydus cache."""

import concurrent.futures
import hashlib
import json
import math
import pathlib
import sys
import time
import urllib.request

sys.path.append("/artifacts/scripts")
import check_http as lab


def fetch(url, offset):
    started = time.monotonic()
    request = urllib.request.Request(url, headers={"Range": f"bytes={offset}-{offset + 65535}"})
    with urllib.request.urlopen(request, timeout=35) as response:
        if response.status != 206:
            raise RuntimeError(f"unexpected status {response.status}")
        body = response.read()
    return body, time.monotonic() - started


def main():
    manifest = lab.publish_variant("load")
    origin = lab.BASE + "/blobs/" + lab.BLOB["digest"]
    offsets = [index * 65536 for index in range(16)]
    expected = {offset: hashlib.sha256(fetch(origin, offset)[0]).hexdigest() for offset in offsets}
    urls = {
        "registry-direct": origin,
        "registry": origin.replace(":5000/", ":5001/"),
        "afs": lab.url("b", manifest),
        "dragonfly-auto": f"http://10.203.0.21:4001/v2/lab/nydus/blobs/{lab.BLOB['digest']}",
    }
    results = []
    for backend, url in urls.items():
        for offset in offsets:
            body, _ = fetch(url, offset)
            assert hashlib.sha256(body).hexdigest() == expected[offset]
        for concurrency in [1, 8, 32]:
            before = len(lab.logs())

            def sample(index, target=url):
                offset = offsets[index % len(offsets)]
                body, elapsed = fetch(target, offset)
                assert hashlib.sha256(body).hexdigest() == expected[offset]
                return elapsed

            started = time.monotonic()
            with concurrent.futures.ThreadPoolExecutor(concurrency) as pool:
                timings = sorted(pool.map(sample, range(128)))
            elapsed = time.monotonic() - started
            requests = lab.logs()[before:]
            row = {
                "backend": backend,
                "concurrency": concurrency,
                "requests": 128,
                "seconds": elapsed,
                "mib_per_second": 8 / elapsed,
                "p50_ms": timings[63] * 1000,
                "p95_ms": timings[math.ceil(128 * 0.95) - 1] * 1000,
                "p99_ms": timings[math.ceil(128 * 0.99) - 1] * 1000,
                "origin_bytes": sum(item.get("bytes", 0) for item in requests),
                "passed": True,
            }
            if backend == "registry-direct":
                row["origin_bytes"] = 128 * 65536
                row["origin_accounting"] = "validated response bodies read directly from Registry"
            results.append(row)
            print(json.dumps(row), flush=True)
            pathlib.Path("/output/range-benchmark.json").write_text(json.dumps(results, indent=2))


if __name__ == "__main__":
    main()
