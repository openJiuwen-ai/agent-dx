#!/usr/bin/env python3
"""Run inside mgmt after fixture, observing proxy, Meta and three Nodes start."""

import concurrent.futures
import hashlib
import json
import logging
import pathlib
import sys
import time
import urllib.error
import urllib.parse
import urllib.request

OUT = pathlib.Path("/output")
FIXTURE = OUT / "fixture"
MANIFEST = (FIXTURE / "manifest-digest.txt").read_text().strip()
BLOB = json.loads((FIXTURE / "data-blob.json").read_text())
BASE = "http://10.203.0.10:5000/v2/lab/nydus"
RESULTS = []
LOGGER = logging.getLogger(__name__)


def request(request_url, method="GET", headers=None, data=None, *, authenticated=True, timeout=40):
    headers = dict(headers or {})
    if authenticated and urllib.parse.urlsplit(request_url).path.startswith("/nydus/"):
        token = pathlib.Path("/artifacts/afs-tls/nydus-token").read_text().strip()
        headers["Authorization"] = f"Bearer {token}"
    try:
        response = urllib.request.urlopen(
            urllib.request.Request(request_url, data=data, method=method, headers=headers), timeout=timeout
        )
    except urllib.error.HTTPError as response:
        return response.code, {k.lower(): v for k, v in response.headers.items()}, response.read()
    with response:
        return response.status, {k.lower(): v for k, v in response.headers.items()}, response.read()


def url(node, manifest=None, blob=BLOB["digest"]):
    manifest = manifest or MANIFEST
    ip = {"a": 20, "b": 21, "c": 22}[node]
    return f"http://10.203.0.{ip}:7401/nydus/lab/{manifest}/blobs/{blob}"


def logs():
    time.sleep(0.05)
    file = OUT / "logs/origin.jsonl"
    return [json.loads(line) for line in file.read_text().splitlines()] if file.exists() else []


def bodies():
    return [row for row in logs() if "/blobs/" in row["path"]]


def control(**values):
    temporary = OUT / "origin-control.tmp"
    temporary.write_text(json.dumps(values))
    temporary.replace(OUT / "origin-control.json")


def result(case, **details):
    RESULTS.append(dict(case=case, passed=True, **details))
    (OUT / "http-results.json").write_text(json.dumps(RESULTS, indent=2))
    LOGGER.info("%s PASS %s", case, details)


def publish_variant(name):
    manifest = json.loads((FIXTURE / "manifest.raw").read_bytes())
    manifest.setdefault("annotations", {})["afs.e2e.case"] = name + "-" + str(time.time_ns())
    body = json.dumps(manifest).encode()
    digest = "sha256:" + hashlib.sha256(body).hexdigest()
    status, _, _ = request(f"{BASE}/manifests/{digest}", "PUT", {"Content-Type": manifest["mediaType"]}, body)
    require(status == 201, f"Registry manifest upload returned {status}")
    return digest


def read_range(node, start, size, manifest=None):
    return request(url(node, manifest), headers={"Range": f"bytes={start}-{start + size - 1}"})


def source_queries():
    status, _, body = request("http://10.203.0.20:7401/metrics")
    require(status == 200, f"Metrics endpoint returned {status}")
    prefix = 'afs_dfs_read_rpc_calls_total{method="get_chunk_sources"} '
    return sum(float(line[len(prefix):]) for line in body.decode().splitlines() if line.startswith(prefix))


def require(condition, detail):
    if not condition:
        raise AssertionError(detail)


def check_first_access(reference):
    before_requests = len(logs())
    for method in ("HEAD", "GET"):
        status, _, _ = request(url("a"), method, authenticated=False)
        require(status == 403, f"Unauthenticated {method} returned {status}")
    require(len(logs()) == before_requests, "Unauthenticated request contacted Registry")
    result("unauthenticated_requests_do_not_prepare_image")
    before = len(bodies())
    status, headers, body = request(url("a"), "HEAD")
    require(
        status == 200 and int(headers["content-length"]) == len(reference) and not body,
        (status, headers, body),
    )
    require(len(bodies()) == before, "Unexpected Registry body request")
    result("first_HEAD_metadata_only", length=len(reference), origin_body_requests=0)

    status, headers, body = read_range("b", 0, 4096)
    require(status == 206 and body == reference[:4096], (status, body[:300]))
    delta = bodies()[before:]
    require(len(delta) == 1 and delta[0]["node"] == "node-a", delta)
    result("cold_B_owner_A_fills", origin=delta)
    before = len(bodies())
    require(read_range("c", 0, 4096)[2] == reference[:4096], "Node C returned different bytes")
    require(len(bodies()) == before, "Unexpected Registry body request")
    result("cold_C_reuses_A", extra_origin_requests=0)

    direct = publish_variant("direct-get")
    status, _, body = read_range("b", 8192, 4096, direct)
    require(status == 206 and body == reference[8192:12288], (status, body[:300]))
    result("first_GET_without_HEAD_or_prepare", manifest=direct)



def check_sequential_reads(reference):
    sequential = publish_variant("sequential-cold-ranges")
    require(request(url("a", sequential), "HEAD")[0] == 200, "Sequential-read preparation failed")
    size = 4096
    count = 16
    require(len(reference) >= 2 * size * count, "Fixture is too small for the sequential range test")
    before_queries = source_queries()
    before = len(bodies())
    for index in range(count):
        start = 2 * index * size
        status, _, body = read_range("a", start, size, sequential)
        end = start + size
        require(status == 206 and body == reference[start:end], (index, status))
    queries = source_queries() - before_queries
    require(len(bodies()) - before == count, "Unexpected count of cold-range Registry requests")
    require(queries <= 2 * count, queries)
    result("sequential_cold_ranges_bound_source_queries", ranges=count, source_queries=queries)



def check_concurrent_requests(reference):
    racing = publish_variant("prepare-race")
    before = len(bodies())
    with concurrent.futures.ThreadPoolExecutor(6) as pool:
        replies = list(pool.map(lambda node: request(url(node, racing), "HEAD"), ["a", "b", "c"] * 2))
    require(all(reply[0] == 200 for reply in replies), [(reply[0], reply[2]) for reply in replies])
    require(len(bodies()) == before, "Unexpected Registry body request")
    result("concurrent_prepare", requests=6)

    before = len(bodies())
    with concurrent.futures.ThreadPoolExecutor(6) as pool:
        replies = list(pool.map(lambda node: read_range(node, 16384, 4096), ["a", "b", "c"] * 2))
    for status, _, body in replies:
        require(status == 206 and body == reference[16384:20480], (status, body[:100]))
    delta = bodies()[before:]
    require(len(delta) == 1, delta)
    result("same_range_origin_coalesced", requests=6, origin_requests=1)



def check_origin_recovery(reference):
    for index, mode in enumerate(["short", "503", "delay"]):
        start = 65536 + index * 8192
        control(mode=mode, delay=5)
        started = time.monotonic()
        status, _, _ = read_range("b" if mode == "delay" else "a", start, 4096)
        if mode == "delay":
            require(time.monotonic() - started < 4.5, "forwarded read exceeded HTTP budget")
        require(status not in (200, 206), (mode, status))
        control()
        status, _, body = read_range("a", start, 4096)
        end = start + 4096
        require(status == 206 and body == reference[start:end], (mode, status, body[:100]))
        result("origin_" + mode + "_recovery")

    corrupt = publish_variant("manifest-identity")
    control(manifest_mode="corrupt")
    status, _, _ = request(url("c", corrupt), "HEAD")
    require(status not in (200, 206), status)
    control()
    require(request(url("c", corrupt), "HEAD")[0] == 200, "Manifest preparation did not recover")
    result("manifest_digest_mismatch_and_retry")

    unknown = "sha256:" + "0" * 64
    require(request(url("b", blob=unknown), "HEAD")[0] == 404, "Unknown blob was not rejected")
    status, _, body = request(url("a"), headers={"Range": f"bytes={len(reference)}-"})
    require(status == 416, (status, body))
    result("unknown_blob_and_unsatisfiable_range")


def main():
    global MANIFEST
    MANIFEST = publish_variant("cold-head")
    (FIXTURE / "active-manifest.txt").write_text(MANIFEST)
    control()
    reference = request(f"{BASE}/blobs/{BLOB['digest']}")[2]
    require(f"sha256:{hashlib.sha256(reference).hexdigest()}" == BLOB["digest"], "Registry blob hash differs")
    check_first_access(reference)
    check_sequential_reads(reference)
    check_concurrent_requests(reference)
    check_origin_recovery(reference)


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO, format="%(message)s", stream=sys.stdout)
    try:
        main()
    finally:
        control()
