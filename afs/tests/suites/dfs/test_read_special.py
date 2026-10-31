"""Remote range/CRC/prefetch lifetime contracts, independent of speed or CPU SLA."""

import json
import math
from pathlib import Path
import re
import time

from fixtures.common import Case, DfsProfile
from suites.operations import peer_counter

PROBE = (Path(__file__).resolve().parent / "read_probe.py").read_text()
MIB = 1024**2


def metric(text, name, labels, default=None):
    values = []
    for line in text.splitlines():
        if not line.startswith(name + "{"):
            continue
        actual = dict(re.findall(r'(\w+)="([^"\\]*)"', line.split("}", 1)[0]))
        if all(actual.get(key) == value for key, value in labels.items()):
            values.append(float(line.rsplit(" ", 1)[1]))
    if not values and default is not None:
        return default
    assert len(values) == 1 and math.isfinite(values[0]) and values[0] >= 0, (name, labels, values)
    return values[0]


def read_probe(c, role, action, size):
    result = c.python(role, PROBE, action, c.workspace(role) + "/read-special", size, timeout=180)
    value = json.loads(result["stdout"])
    return c.record("read." + role + "." + action, value)


def crc_ranges(c):
    size = 8 * MIB + 4096
    expected = read_probe(c, "A", "create", size)
    before = {r: c.health(r, "/metrics") for r in ("A", "B")}
    calls = peer_counter(c, "B")
    actual = read_probe(c, "B", "ranges", size)
    assert actual["bytes"] == expected["bytes"] and actual["sha256"] == expected["sha256"]
    assert peer_counter(c, "B") > calls, "fresh reader did not exercise the remote path"
    after = {r: c.health(r, "/metrics") for r in ("A", "B")}
    stages = ("storage_segments", "range_send", "range_receive", "trusted_segments_receive")
    scans = {
        r: {stage: metric(after[r], "afs_dfs_crc32c_read_bytes_total", {"stage": stage})
            - metric(before[r], "afs_dfs_crc32c_read_bytes_total", {"stage": stage}) for stage in stages}
        for r in ("A", "B")
    }
    if c.case.profile.read_crc_mode == "off":
        assert scans["A"]["range_send"] == 0 and scans["B"]["range_receive"] == 0, scans
    else:
        assert sum(scans["B"].values()) > 0, "selected receiver validation scanned no body"
    c.record("read.crc_scans", {"mode": c.case.profile.read_crc_mode, "bytes": scans,
                                "corruption_injection_claimed": False})


def prefetch_lifetime(c):
    size = 64 * MIB + 4096
    expected = read_probe(c, "A", "create", size)
    before = c.health("B", "/metrics")
    calls = peer_counter(c, "B")
    actual = read_probe(c, "B", "direct", size)
    assert actual["sha256"] == expected["sha256"] and actual["bytes"] == size
    assert peer_counter(c, "B") > calls
    deadline = time.monotonic() + 30
    while True:
        after = c.health("B", "/metrics")
        active = metric(after, "afs_dfs_read_buffer_capacity_bytes", {"role": "b_prefetch", "state": "active"}, 0)
        total = metric(after, "afs_dfs_read_buffer_capacity_bytes", {"role": "b_prefetch", "state": "total"}, 0)
        if active == 0 and total == 0:
            break
        assert time.monotonic() < deadline, ("prefetch retained buffers after close", active, total)
        time.sleep(0.1)
    scheduled = metric(after, "afs_dfs_read_buffer_events_total", {"role": "b_prefetch", "event": "scheduled"})
    scheduled -= metric(before, "afs_dfs_read_buffer_events_total", {"role": "b_prefetch", "event": "scheduled"}, 0)
    assert scheduled > 0, "explicit direct sequential read never activated the configured prefetch"
    c.record("read.prefetch_lifetime", {"scheduled": scheduled, "active_after_close": active,
                                        "total_after_close": total, "performance_acceptance": False})


def cached_reuse(c):
    size = 48 * MIB + 4096
    expected = read_probe(c, "A", "create", size)
    before = c.health("B", "/metrics")
    # The same process retains the same FD for the first repeat, then reopens it.
    code = (
        'p=sys.argv[1];n=int(sys.argv[2]);want=sys.argv[3];checks=[]\n'
        'def full(fd):\n h=hashlib.sha256();seen=0\n while True:\n'
        '  b=os.read(fd,1024**2)\n  if not b:break\n  h.update(b);seen+=len(b)\n'
        ' assert seen==n and h.hexdigest()==want\n return {"bytes":seen,"sha256":h.hexdigest()}\n'
        'fd=os.open(p,os.O_RDONLY)\ntry:\n checks.append(full(fd));os.lseek(fd,0,0);checks.append(full(fd))\n'
        'finally:os.close(fd)\nfd=os.open(p,os.O_RDONLY)\ntry:checks.append(full(fd))\nfinally:os.close(fd)\n'
        'print(json.dumps(checks))'
    )
    value = c.python("B", code, c.workspace("B") + "/read-special", size, expected["sha256"], timeout=180)
    assert len(json.loads(value["stdout"])) == 3
    after = c.health("B", "/metrics")
    scheduled = metric(after, "afs_dfs_read_buffer_events_total", {"role": "b_prefetch", "event": "scheduled"}, 0)
    scheduled -= metric(before, "afs_dfs_read_buffer_events_total", {"role": "b_prefetch", "event": "scheduled"}, 0)
    assert scheduled == 0, "cached requests entered the direct-only native prefetch controller"
    c.record("read.cached_reuse", {"first_same_fd_and_reopen_content": json.loads(value["stdout"]),
                                   "native_prefetch_scheduled": scheduled,
                                   "cache_residency_or_zero_RPC_claimed": False})


def cases():
    result = []
    for prefix, tag in [("local", "local-read-special"), ("cluster", "cluster-read-special")]:
        for mode in ("off", "receiver", "both"):
            result.append(Case("dfs.read." + prefix + ".ranges_crc_" + mode, (tag,), "dfs", (), crc_ranges,
                               profile=DfsProfile(read_crc_mode=mode, kernel_readahead_bytes=0,
                                                  native_open_authority=True)))
        result.append(Case("dfs.read." + prefix + ".prefetch_release", (tag,), "dfs", (), prefetch_lifetime,
                           profile=DfsProfile(read_crc_mode="off", readahead_max_bytes=16 * MIB,
                                              native_open_authority=True)))
        result.append(Case("dfs.read." + prefix + ".cached_reuse", (tag,), "dfs", (), cached_reuse,
                           profile=DfsProfile(keep_cache=True, kernel_readahead_bytes=16 * MIB,
                                              read_crc_mode="off", readahead_max_bytes=16 * MIB,
                                              native_open_authority=True)))
    return tuple(result)


CASES = cases()
