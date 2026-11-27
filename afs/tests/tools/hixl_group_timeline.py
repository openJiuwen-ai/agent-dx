#!/usr/bin/env python3
"""Join one sequential single-file read from two AFS_HIXL_TRACE_GROUP logs."""

import argparse
import csv
import json
import sys
from collections import defaultdict
from pathlib import Path


def events(path):
    with path.open(encoding="utf-8") as stream:
        for line in stream:
            if 'ownerfs.hixl_group' not in line:
                continue
            event = json.loads(line)
            if event.get("msg") == "ownerfs.hixl_group":
                yield event


def groups(events_iter, id_key):
    result = defaultdict(dict)
    for event in events_iter:
        key = event[id_key]
        name = event["event"]
        if name in result[key]:
            raise ValueError(f"duplicate {name} for {key}; isolate one successful read (no retries)")
        result[key][name] = event
    return result


def stamp(event):
    return int(event["mono_ns"])


def duration(start, end):
    delta = stamp(end) - stamp(start)
    if delta < 0:
        raise ValueError("negative duration; check input process identity")
    return delta / 1e6


def analyze(home, reader, expected_bytes):
    homes = groups(home, "read_id")
    reads = groups(reader, "group_id")
    rows = []
    cursor = 0
    previous_consumed = None
    for group in sorted(reads.values(), key=lambda g: int(g["fetch_done"]["offset"])):
        done = group["fetch_done"]
        if done["outcome"] != "ready":
            raise ValueError("incomplete or failed fetch")
        offset, size = int(done["offset"]), int(done["bytes"])
        if not size and offset == expected_bytes:
            continue  # EOF need not install or consume a window.
        if offset != cursor or size <= 0:
            raise ValueError(f"non-contiguous trace at {offset}, expected {cursor}")
        cursor += size
        rid = done["read_id"]
        home_group = homes[rid]
        ready = home_group["home_ready"]
        if int(ready["offset"]) != offset or int(ready["bytes"]) != size:
            raise ValueError(f"inconsistent Home reply for {rid}")
        prepare = group["prepare_rpc_begin"]
        reply = group["prepare_rpc_end"]
        if prepare["read_id"] != rid or reply["read_id"] != rid:
            raise ValueError(f"inconsistent prepare for {rid}")
        rpc_ms = duration(prepare, reply)
        source_ms = duration(home_group["home_source_begin"], home_group["home_source_end"])
        submitted = group.get("prefetch_submit")
        consumed = group["window_consumed"]
        demand = group["demand_wait_start"]
        rows.append(dict(offset=offset, size=size, read_id=rid,
                         prefetch=submitted is not None,
                         submit_to_rpc_ms=duration(submitted, prepare) if submitted else None,
                         prepare_rpc_ms=rpc_ms, home_source_ms=source_ms,
                         after_prepare_to_verified_ms=duration(reply, done),
                         demand_wait_ms=duration(demand, group["demand_wait_end"]),
                         # Signed: positive means the data was late. Both stamps are Reader-local.
                         ready_minus_demand_ms=(stamp(done)-stamp(demand))/1e6,
                         ready_minus_prev_consumed_ms=(stamp(done)-stamp(previous_consumed))/1e6
                         if previous_consumed is not None else None))
        previous_consumed = consumed
    if cursor != expected_bytes or not rows:
        raise ValueError(f"trace covers {cursor} bytes, expected {expected_bytes}")
    return rows


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("home", type=Path)
    parser.add_argument("reader", type=Path)
    parser.add_argument("--bytes", type=int, required=True, help="expected file length")
    args = parser.parse_args()
    try:
        rows = analyze(events(args.home), events(args.reader), args.bytes)
    except (KeyError, OSError, ValueError) as exc:
        parser.exit(1, f"Invalid single-read trace: {exc}\n")
    writer = csv.DictWriter(sys.stdout, fieldnames=rows[0])
    writer.writeheader()
    writer.writerows(rows)


if __name__ == "__main__":
    main()
