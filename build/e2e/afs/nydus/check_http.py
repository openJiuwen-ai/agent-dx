#!/usr/bin/env python3
"""Run inside mgmt after fixture, observing proxy, Meta and three Nodes start."""

import concurrent.futures
import hashlib
import json
import pathlib
import time
import urllib.error
import urllib.request

OUT = pathlib.Path("/output")
FIXTURE = OUT / "fixture"
MANIFEST = (FIXTURE / "manifest-digest.txt").read_text().strip()
BLOB = json.loads((FIXTURE / "data-blob.json").read_text())
BASE = "http://10.203.0.10:5000/v2/lab/nydus"
RESULTS = []


def request(request_url, method="GET", headers=None, data=None):
    try:
        response = urllib.request.urlopen(
            urllib.request.Request(request_url, data=data, method=method, headers=headers or {}), timeout=40
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
    print(case, "PASS", details, flush=True)


def publish_variant(name):
    manifest = json.loads((FIXTURE / "manifest.raw").read_bytes())
    manifest.setdefault("annotations", {})["afs.e2e.case"] = name + "-" + str(time.time_ns())
    body = json.dumps(manifest).encode()
    digest = "sha256:" + hashlib.sha256(body).hexdigest()
    status, _, _ = request(BASE + "/manifests/" + digest, "PUT", {"Content-Type": manifest["mediaType"]}, body)
    assert status == 201, status
    return digest


def read_range(node, start, size, manifest=None):
    return request(url(node, manifest), headers={"Range": f"bytes={start}-{start + size - 1}"})


def main():
    global MANIFEST
    MANIFEST = publish_variant("cold-head")
    (FIXTURE / "active-manifest.txt").write_text(MANIFEST)
    control()
    reference = request(BASE + "/blobs/" + BLOB["digest"])[2]
    assert "sha256:" + hashlib.sha256(reference).hexdigest() == BLOB["digest"]
    before = len(bodies())
    status, headers, body = request(url("a"), "HEAD")
    assert status == 200 and int(headers["content-length"]) == len(reference) and not body, (status, headers, body)
    assert len(bodies()) == before
    result("first_HEAD_metadata_only", length=len(reference), origin_body_requests=0)

    status, headers, body = read_range("b", 0, 4096)
    assert status == 206 and body == reference[:4096], (status, body[:300])
    delta = bodies()[before:]
    assert len(delta) == 1 and delta[0]["node"] == "node-a", delta
    result("cold_B_owner_A_fills", origin=delta)
    before = len(bodies())
    assert read_range("c", 0, 4096)[2] == reference[:4096]
    assert len(bodies()) == before
    result("cold_C_reuses_A", extra_origin_requests=0)

    direct = publish_variant("direct-get")
    status, _, body = read_range("b", 8192, 4096, direct)
    assert status == 206 and body == reference[8192:12288], (status, body[:300])
    result("first_GET_without_HEAD_or_prepare", manifest=direct)

    racing = publish_variant("prepare-race")
    before = len(bodies())
    with concurrent.futures.ThreadPoolExecutor(6) as pool:
        replies = list(pool.map(lambda node: request(url(node, racing), "HEAD"), ["a", "b", "c"] * 2))
    assert all(r[0] == 200 for r in replies), [(r[0], r[2]) for r in replies]
    assert len(bodies()) == before
    result("concurrent_prepare", requests=6)

    before = len(bodies())
    with concurrent.futures.ThreadPoolExecutor(6) as pool:
        replies = list(pool.map(lambda node: read_range(node, 16384, 4096), ["a", "b", "c"] * 2))
    assert all(r[0] == 206 and r[2] == reference[16384:20480] for r in replies), [(r[0], r[2][:100]) for r in replies]
    assert len(bodies()) - before == 1, bodies()[before:]
    result("same_range_origin_coalesced", requests=6, origin_requests=1)

    for index, mode in enumerate(["short", "503", "delay"]):
        start = 65536 + index * 8192
        control(mode=mode, delay=5)
        started = time.monotonic()
        status, _, _ = read_range("b" if mode == "delay" else "a", start, 4096)
        if mode == "delay":
            assert time.monotonic() - started < 4.5, "forwarded read exceeded HTTP budget"
        assert status not in (200, 206), (mode, status)
        control()
        status, _, body = read_range("a", start, 4096)
        assert status == 206 and body == reference[start : start + 4096], (mode, status, body[:100])
        result("origin_" + mode + "_recovery")

    corrupt = publish_variant("manifest-identity")
    control(manifest_mode="corrupt")
    status, _, _ = request(url("c", corrupt), "HEAD")
    assert status not in (200, 206), status
    control()
    assert request(url("c", corrupt), "HEAD")[0] == 200
    result("manifest_digest_mismatch_and_retry")

    assert request(url("b", blob="sha256:" + "0" * 64), "HEAD")[0] == 404
    status, _, body = request(url("a"), headers={"Range": f"bytes={len(reference)}-"})
    assert status == 416, (status, body)
    result("unknown_blob_and_unsatisfiable_range")


if __name__ == "__main__":
    try:
        main()
    finally:
        control()
