#!/usr/bin/env python3
"""Read back a PyPI release and bind every published file to the candidate."""

import argparse
import json
from pathlib import Path
import time
from urllib.error import HTTPError
from urllib.request import urlopen


INDEXES = {
    "pypi": "https://pypi.org/pypi/{name}/{version}/json",
    "testpypi": "https://test.pypi.org/pypi/{name}/{version}/json",
}


def verify_payload(payload, candidate, repository):
    info = payload.get("info", {})
    if info.get("name", "").lower() != candidate["name"] or info.get(
        "version"
    ) != candidate["version"]:
        raise ValueError("published package identity mismatch")
    urls = {item.get("filename"): item for item in payload.get("urls", [])}
    if set(urls) != set(candidate["files"]):
        raise ValueError("published file set differs from candidate")
    published = {}
    for name, digest in candidate["files"].items():
        item = urls.get(name)
        if not item:
            raise ValueError(f"published file is missing: {name}")
        if item.get("digests", {}).get("sha256") != digest:
            raise ValueError(f"published digest differs: {name}")
        published[name] = {"sha256": digest, "url": item.get("url")}
    return {
        "schema_version": 1,
        "status": "published",
        "repository": repository,
        "name": candidate["name"],
        "version": candidate["version"],
        "commit": candidate["commit"],
        "build_id": candidate.get("build_id"),
        "files": published,
    }


def fetch(candidate, repository, attempts=6):
    url = INDEXES[repository].format(
        name=candidate["name"], version=candidate["version"]
    )
    for attempt in range(attempts):
        try:
            with urlopen(url, timeout=15) as response:
                payload = json.load(response)
            return verify_payload(payload, candidate, repository)
        except HTTPError as error:
            if error.code != 404 or attempt + 1 == attempts:
                raise
            time.sleep(5)
    raise RuntimeError("published release did not become visible")


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--candidate", type=Path, required=True)
    parser.add_argument("--repository", choices=tuple(INDEXES), required=True)
    parser.add_argument("--output", type=Path, required=True)
    arguments = parser.parse_args()
    candidate = json.loads(arguments.candidate.read_text())
    result = fetch(candidate, arguments.repository)
    arguments.output.parent.mkdir(parents=True, exist_ok=True)
    arguments.output.write_text(json.dumps(result, indent=2) + "\n")
    print(json.dumps(result))


if __name__ == "__main__":
    main()
