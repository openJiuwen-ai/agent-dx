"""Attribute observed object GET ranges without treating aggregate bytes as memory I/O."""

import argparse
import json
from pathlib import Path
import re


def union_bytes(ranges):
    total = 0
    end = -1
    for start, stop in sorted(ranges):
        total += max(0, stop - max(start, end))
        end = max(end, stop)
    return total


def summarize(rows, start_ns=0, stop_ns=2**63 - 1, owners=None):
    objects = {}
    failures = []
    for row in rows:
        if row["method"] != "GET" or not start_ns <= row["at_unix_ns"] < stop_ns:
            continue
        if row["status"] not in (200, 206):
            failures.append(row)
            continue
        # S3 listing GETs are metadata, not object ranges.
        if "/chunks/" not in row["path"] or row["path"].endswith("/"):
            continue
        complete = row.get("complete") is True
        count = row["response_bytes"]
        content_range = re.fullmatch(r"bytes (\d+)-(\d+)/(\d+)", row.get("content_range", ""))
        if row["status"] == 206:
            if not content_range:
                raise ValueError("missing or inconsistent Content-Range")
            declared = int(content_range[2]) - int(content_range[1]) + 1
            if count < 0 or count > declared:
                raise ValueError("missing or inconsistent Content-Range")
            if complete and count != declared:
                raise ValueError("missing or inconsistent Content-Range")
            # Speculative readers may cancel a GET early. Preserve the prefix
            # accepted by the observer's socket, without calling it client-consumed.
            start, stop = int(content_range[1]), int(content_range[1]) + count
        else:
            start, stop = 0, count
        value = objects.setdefault(
            row["path"], {"get_bytes": 0, "requests": 0, "aborted_requests": 0, "ranges": []}
        )
        value["get_bytes"] += count
        value["requests"] += 1
        value["aborted_requests"] += int(not complete)
        value["ranges"].append((start, stop))
    for key, value in objects.items():
        value["unique_bytes"] = union_bytes(value["ranges"])
        value["repeated_bytes"] = value["get_bytes"] - value["unique_bytes"]
        # One COW block can belong to several files; preserve every association.
        value["files"] = sorted((owners or {}).get(key, []))
    total = sum(value["get_bytes"] for value in objects.values())
    unique = sum(value["unique_bytes"] for value in objects.values())
    return {
        "get_bytes": total,
        "unique_object_bytes": unique,
        "repeated_object_bytes": total - unique,
        "repeat_factor": total / unique if unique else None,
        "unattributed_bytes": sum(v["get_bytes"] for v in objects.values() if not v["files"]),
        "aborted_requests": sum(v["aborted_requests"] for v in objects.values()),
        "objects": objects,
        "failed_gets": failures,
        "scope": (
            "observer socket bytes and ranges; aborted prefixes can exceed client-consumed bytes; "
            "shared COW chunks retain all file associations"
        ),
    }


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("log", type=Path)
    parser.add_argument("--start-ns", type=int, default=0)
    parser.add_argument("--stop-ns", type=int, default=2**63 - 1)
    parser.add_argument("--owners", type=Path)
    parser.add_argument("--output", type=Path, required=True)
    args = parser.parse_args()
    owners = json.loads(args.owners.read_text()) if args.owners else {}
    rows = [json.loads(line) for line in args.log.read_text().splitlines()]
    args.output.write_text(
        json.dumps(summarize(rows, args.start_ns, args.stop_ns, owners), indent=2)
    )


if __name__ == "__main__":
    main()
