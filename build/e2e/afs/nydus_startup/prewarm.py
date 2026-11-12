#!/usr/bin/env python3
"""Read a sampled startup plan through one worker's AFS Registry endpoint."""
import argparse
from contextlib import closing
import hashlib
from http.client import HTTPConnection, HTTPException, HTTPSConnection
import json
from pathlib import Path
import re
import time
from urllib.parse import quote, urlsplit

from sample_ranges import coverage

DIGEST = re.compile(r"sha256:[0-9a-f]{64}")
REPOSITORY_COMPONENT = re.compile(r"[a-z0-9]+(?:(?:[._]|__|-+)[a-z0-9]+)*")


def validate_plan(manifest_path, plan, workload, command):
    raw = manifest_path.read_bytes()
    image = "sha256:" + hashlib.sha256(raw).hexdigest()
    if plan["schema"] != 1 or plan["manifest"] != image:
        raise ValueError("plan does not match the manifest digest or supported schema")
    if plan["workload"] != workload or plan["command"] != command:
        raise ValueError("plan does not match the workload and command")
    manifest = json.loads(raw)
    metadata = {manifest["config"]["digest"]}
    metadata.update(layer["digest"] for layer in manifest["layers"] if layer.get(
        "annotations", {}).get("containerd.io/snapshot/nydus-bootstrap") == "true")
    sizes = {layer["digest"]: layer["size"] for layer in manifest["layers"] if layer["digest"] not in metadata}
    ranges = plan["ranges"]
    if not ranges:
        raise ValueError("startup plan is empty")
    for span in ranges:
        blob = span["blob"]
        start, length = span["offset"], span["length"]
        if not DIGEST.fullmatch(blob) or blob not in sizes:
            raise ValueError(f"range does not identify a manifest body blob: {blob}")
        if type(start) is not int or type(length) is not int or not 0 <= start < start + length <= sizes[blob]:
            raise ValueError(f"range is outside the manifest body blob: {blob}")
    if coverage(ranges) != ranges:
        raise ValueError("plan ranges must be sorted and merged by blob")
    return sizes


def read_range(connection, path, span, size, token):
    start, length = span["offset"], span["length"]
    end = start + length - 1
    headers = {"Range": f"bytes={start}-{end}"}
    if token:
        headers["Authorization"] = f"Bearer {token}"
    connection.request("GET", path, headers=headers)
    with closing(connection.getresponse()) as response:
        if response.status != 206:
            raise ValueError(f"Range GET returned HTTP {response.status} for {span['blob']}")
        if response.getheader("Content-Range") != f"bytes {start}-{end}/{size}":
            raise ValueError(f"unexpected Content-Range for {span['blob']}")
        if response.getheader("Content-Length") != str(length):
            raise ValueError(f"unexpected Content-Length for {span['blob']}")
        if response.getheader("Docker-Content-Digest") != span["blob"]:
            raise ValueError(f"unexpected Docker-Content-Digest for {span['blob']}")
        digest = hashlib.sha256()
        received = 0
        while chunk := response.read(min(256 * 1024, length - received + 1)):
            received += len(chunk)
            if received > length:
                raise ValueError(f"oversized body for {span['blob']}")
            digest.update(chunk)
        if received != length:
            raise ValueError(f"short body for {span['blob']}: {received} of {length} bytes")
        return digest.hexdigest()


def prewarm(args):
    raw = args.plan.read_bytes()
    plan = json.loads(raw)
    sizes = validate_plan(args.manifest, plan, args.workload, args.command)
    endpoint = urlsplit(args.endpoint)
    if endpoint.scheme not in ("http", "https") or not endpoint.hostname or endpoint.username is not None:
        raise ValueError("endpoint must be an HTTP(S) origin without credentials")
    if endpoint.path not in ("", "/") or endpoint.query or endpoint.fragment:
        raise ValueError("endpoint must be an HTTP(S) origin without path, query or fragment")
    if not all(REPOSITORY_COMPONENT.fullmatch(part) for part in args.repository.split("/")):
        raise ValueError("invalid Registry repository")
    if args.timeout <= 0:
        raise ValueError("timeout must be positive")
    token = args.token_file.read_text().strip() if args.token_file else None
    connection_type = HTTPSConnection if endpoint.scheme == "https" else HTTPConnection
    base_path = "/v2/" + quote(args.repository, safe="/") + "/blobs/"
    result = {"manifest": plan["manifest"], "workload": plan["workload"], "repository": args.repository,
              "endpoint": args.endpoint, "plan_sha256": hashlib.sha256(raw).hexdigest(), "ok": False,
              "started_monotonic_ns": time.monotonic_ns(), "reads": []}
    with args.receipt.open("x") as stream:
        try:
            with closing(connection_type(endpoint.hostname, endpoint.port, timeout=args.timeout)) as connection:
                for span in plan["ranges"]:
                    started = time.monotonic_ns()
                    blob = span["blob"]
                    digest = read_range(connection, base_path + blob, span, sizes[blob], token)
                    result["reads"].append(dict(span, sha256=digest, elapsed_ns=time.monotonic_ns() - started))
            result["ok"] = True
        except (HTTPException, OSError, ValueError) as error:
            result["error"] = str(error)
            raise
        finally:
            result["finished_monotonic_ns"] = time.monotonic_ns()
            json.dump(result, stream, indent=2)
            stream.write("\n")


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--manifest", type=Path, required=True)
    parser.add_argument("--plan", type=Path, required=True)
    parser.add_argument("--workload", required=True)
    parser.add_argument("--command", required=True)
    parser.add_argument("--repository", required=True)
    parser.add_argument("--endpoint", required=True)
    parser.add_argument("--receipt", type=Path, required=True)
    parser.add_argument("--token-file", type=Path)
    parser.add_argument("--timeout", type=float, default=30, help="socket timeout in seconds")
    args = parser.parse_args()
    try:
        prewarm(args)
    except (HTTPException, KeyError, OSError, TypeError, ValueError) as error:
        parser.exit(1, f"prewarm failed: {error}\n")


if __name__ == "__main__":
    main()
