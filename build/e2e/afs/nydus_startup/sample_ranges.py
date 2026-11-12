#!/usr/bin/env python3
"""Read the plaintext Registry request capture and compare startup byte ranges."""
import argparse
import hashlib
import json
from pathlib import Path
import re
import struct
from urllib.parse import urlsplit

REQUEST = re.compile(rb"(GET|HEAD|CONNECT) ([^ ]+) HTTP/1\.[01]")
BLOB = re.compile(r"/v2/.+/blobs/(sha256:[0-9a-f]{64})")


def packets(path):
    """Yield untruncated IPv4 TCP payloads from Linux loopback Ethernet pcap."""
    with path.open("rb") as pcap:
        header = pcap.read(24)
        endian, precision = {
            b"\xd4\xc3\xb2\xa1": ("<", 1000), b"\xa1\xb2\xc3\xd4": (">", 1000),
            b"\x4d\x3c\xb2\xa1": ("<", 1), b"\xa1\xb2\x3c\x4d": (">", 1),
        }[header[:4]]
        assert struct.unpack_from(endian + "I", header, 20)[0] == 1, "capture must use Ethernet framing"
        while record := pcap.read(16):
            sec, fraction, size, wire = struct.unpack(endian + "IIII", record)
            data = pcap.read(size)
            assert len(data) == size == wire, "truncated capture packet"
            assert data[12:14] == b"\x08\x00" and data[23] == 6, "expected IPv4 TCP"
            ip_size = int.from_bytes(data[16:18], "big")
            tcp = 14 + (data[14] & 15) * 4
            payload = data[tcp + (data[tcp + 12] >> 4) * 4:14 + ip_size]
            if payload:
                flow = (data[26:34], data[tcp:tcp + 4])
                seq = int.from_bytes(data[tcp + 4:tcp + 8], "big")
                yield sec * 10**9 + fraction * precision, flow, seq, payload


def observe(path, manifest, sdk):
    sample, = sdk["samples"]
    assert sample["ok"] and sample["exit_code"] == 0
    start, end = sample["submit_wall_ns"], sample["command_wall_ns"]
    monotonic = sample["command_ns"] - sample["submit_ns"]
    assert abs(end - start - monotonic) < 10_000_000, "worker realtime changed during the run"
    layers = {layer["digest"]: (layer["size"], "bootstrap" if layer.get("annotations", {}).get(
        "containerd.io/snapshot/nydus-bootstrap") == "true" else "body") for layer in manifest["layers"]}
    config = manifest["config"]
    layers[config["digest"]] = (config["size"], "config")
    events = []
    seen = set()
    for ts, flow, seq, payload in packets(path):
        key = (flow, seq, payload)
        if key in seen:
            continue
        seen.add(key)
        # This captures the short, plaintext Registry requests in the isolated lab.
        # Fail rather than silently dropping a split request or unknown payload.
        while payload:
            headers, marker, payload = payload.partition(b"\r\n\r\n")
            assert marker, "split HTTP header in capture"
            line, *fields = headers.split(b"\r\n")
            match = REQUEST.fullmatch(line)
            assert match, "unknown HTTP request payload"
            method, target = (part.decode("ascii") for part in match.groups())
            match = BLOB.fullmatch(urlsplit(target).path) if method != "CONNECT" else None
            digest = match[1] if match else None
            event = {"at_ns": ts, "method": method, "target": target, "blob": digest,
                     "in_window": start <= ts <= end, "kind": "metadata"}
            if digest:
                size, event["kind"] = layers[digest]
                if method == "GET":
                    ranges = [field.split(b":", 1)[1].strip() for field in fields
                              if field.lower().startswith(b"range:")]
                    if ranges:
                        value, = ranges
                        span = re.fullmatch(rb"bytes=([0-9]+)-([0-9]*)", value)
                        assert span, "unsupported Range"
                        lo, hi = int(span[1]), min(int(span[2]) + 1, size) if span[2] else size
                    else:
                        lo, hi = 0, size
                    assert 0 <= lo < hi <= size
                    event.update(offset=lo, length=hi - lo)
            events.append(event)
    return events


def coverage(entries):
    result = []
    by_blob = {}
    for entry in entries:
        start = entry["offset"]
        by_blob.setdefault(entry["blob"], []).append((start, start + entry["length"]))
    for digest, spans in sorted(by_blob.items()):
        merged = []
        for lo, hi in sorted(spans):
            if merged and lo <= merged[-1][1]:
                merged[-1][1] = max(hi, merged[-1][1])
            else:
                merged.append([lo, hi])
        result.extend({"blob": digest, "offset": lo, "length": hi - lo} for lo, hi in merged)
    return result


def body(events):
    return coverage(event for event in events if event["in_window"] and event["kind"] == "body"
                    and event["method"] == "GET")


def make_plan(image, workload, command, training_run, events):
    ranges = body(events)
    if not ranges:
        raise ValueError("no startup body requests in the sample")
    return {"schema": 1, "manifest": image, "workload": workload, "command": command,
            "training_run": training_run, "ranges": ranges}


def compare(planned, actual):
    assert actual, "no startup body ranges in the validation run"
    plan_bytes = sum(row["length"] for row in planned)
    actual_bytes = sum(row["length"] for row in actual)
    hit = 0
    for a in planned:
        for b in actual:
            if a["blob"] == b["blob"]:
                hit += max(0, min(a["offset"] + a["length"], b["offset"] + b["length"])
                           - max(a["offset"], b["offset"]))
    return {"plan_bytes": plan_bytes, "actual_bytes": actual_bytes, "hit_bytes": hit,
            "missing_bytes": actual_bytes - hit, "extra_bytes": plan_bytes - hit,
            "coverage_pct": hit * 100 / actual_bytes}


def read(path):
    return json.loads(path.read_text())


def dump(path, data):
    with path.open("x") as stream:
        json.dump(data, stream, indent=2)
        stream.write("\n")


def analyze(manifest_path, pcap, sdk):
    raw = manifest_path.read_bytes()
    image = "sha256:" + hashlib.sha256(raw).hexdigest()
    assert sdk["digest"] == image
    return image, observe(pcap, json.loads(raw), sdk)


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("manifest", type=Path)
    parser.add_argument("capture", type=Path)
    parser.add_argument("sdk", type=Path)
    parser.add_argument("events", type=Path)
    parser.add_argument("--plan", type=Path)
    parser.add_argument("--workload")
    parser.add_argument("--command")
    parser.add_argument("--training-run")
    args = parser.parse_args()
    if args.plan and not all((args.workload, args.command, args.training_run)):
        parser.error("--plan requires --workload, --command and --training-run")
    try:
        image, observed = analyze(args.manifest, args.capture, read(args.sdk))
        plan = make_plan(image, args.workload, args.command, args.training_run, observed) if args.plan else None
        dump(args.events, observed)
        if args.plan:
            dump(args.plan, plan)
    except (AssertionError, KeyError, OSError, ValueError, struct.error) as error:
        parser.exit(1, f"sampling failed: {error}\n")


if __name__ == "__main__":
    main()
