"""Pure helpers for the opt-in benchmark suite (no I/O, no product knowledge).

The suite measures with industry tools (fio, elbencho, iperf3) and reads their own
reports. Everything here is deterministic so it can be pinned by runner_tests.
"""

from dataclasses import dataclass
import json
import statistics
from pathlib import PurePosixPath

from fixtures.common import Blocked, is_integer

MIB = 1024**2
FIO_OPERATIONS = ("read", "write")


@dataclass(frozen=True)
class BenchParameters:
    file_bytes: int
    block_bytes: tuple[int, ...]
    threads: tuple[int, ...]
    rounds: int
    tools: dict

    def tool(self, name):
        """Return (absolute path, extra environment) for a declared tool."""
        if name not in self.tools:
            raise Blocked("benchmark tool not declared in bench.tools: " + name)
        entry = self.tools[name]
        if isinstance(entry, str):
            return entry, {}
        return entry["path"], dict(entry.get("env", {}))


def _tool_entry(name, entry):
    path, env = (entry, {}) if isinstance(entry, str) else (entry.get("path"), entry.get("env", {}))
    if not isinstance(path, str) or not PurePosixPath(path).is_absolute() or "\n" in path:
        raise Blocked("bench tool path must be absolute: " + name)
    if not isinstance(env, dict) or not all(isinstance(k, str) and isinstance(v, str) for k, v in env.items()):
        raise Blocked("bench tool env must map strings to strings: " + name)


def parameters(config):
    """Validate the explicit `bench` section; nothing is defaulted."""
    values = config.get("bench")
    if not isinstance(values, dict):
        raise Blocked("benchmark input requires an explicit bench section; no implicit experiment")
    size = values.get("file_bytes")
    if not is_integer(size) or not MIB <= size <= 1024**4 or size % MIB:
        raise Blocked("bench file_bytes must be 1MiB..1TiB and a whole number of MiB")
    blocks, threads = values.get("block_bytes"), values.get("threads")
    if not isinstance(blocks, list) or not blocks or not all(
            is_integer(b) and 4096 <= b <= 64 * MIB and b % 4096 == 0 and b <= size for b in blocks):
        raise Blocked("bench block_bytes must list page-aligned sizes 4KiB..64MiB not above file_bytes")
    if not isinstance(threads, list) or not threads or not all(is_integer(t) and 1 <= t <= 64 for t in threads):
        raise Blocked("bench threads must list counts 1..64")
    for t in threads:
        if size % t or any((size // t) % b for b in blocks):
            raise Blocked("bench file_bytes must split into whole blocks for every thread count")
    rounds = values.get("rounds")
    if not is_integer(rounds) or not 3 <= rounds <= 50:
        raise Blocked("bench rounds must be 3..50")
    tools = values.get("tools")
    if not isinstance(tools, dict) or not tools:
        raise Blocked("bench tools must declare absolute tool paths")
    for name, entry in tools.items():
        _tool_entry(name, entry)
    return BenchParameters(size, tuple(blocks), tuple(threads), rounds, dict(tools))


def fio_argv(fio, path, operation, *, block, file_bytes, threads, end_fsync=False):
    """One shared file, each job owning an equal contiguous slice; cache state is the caller's."""
    if operation not in FIO_OPERATIONS:
        raise ValueError("unsupported fio operation: " + str(operation))
    share = file_bytes // threads
    # Each job's region is exactly [k*share, (k+1)*share): fio lays out (writes) any file
    # shorter than offset+size before reading, so a region past EOF would turn a read into a write.
    argv = [
        fio, "--name=bench", "--filename=" + path, "--rw=" + operation, "--bs=%d" % block,
        "--size=%d" % share, "--io_size=%d" % share, "--offset_increment=%d" % share,
        "--numjobs=%d" % threads, "--ioengine=psync", "--direct=0", "--invalidate=0",
        "--end_fsync=%d" % (1 if end_fsync else 0), "--group_reporting", "--output-format=json",
    ]
    if operation == "read":
        argv.append("--allow_file_create=0")
    return argv


def diskstats_delta(before, after):
    """Bytes read/written per block device between two /proc/diskstats snapshots (changed devices only)."""
    def parse(text):
        table = {}
        for line in text.splitlines():
            fields = line.split()
            if len(fields) >= 10:
                table[fields[2]] = (int(fields[5]), int(fields[9]))
        return table

    first, second = parse(before), parse(after)
    delta = {}
    for name, (read, written) in second.items():
        old = first.get(name, (read, written))
        change = {"read_bytes": (read - old[0]) * 512, "write_bytes": (written - old[1]) * 512}
        if change["read_bytes"] or change["write_bytes"]:
            delta[name] = change
    return delta


def elbencho_read_argv(elbencho, hosts, path, *, block, file_bytes, threads, result):
    """Every service host reads the whole file (`--nosvcshare`); the master only coordinates."""
    return [
        elbencho, "--hosts", ",".join(hosts), "--nosvcshare", "-r", "-b", str(block), "-s", str(file_bytes),
        "-t", str(threads), "--nolive", "--jsonfile", result, path,
    ]


def _json_document(text):
    start = text.find("{")
    if start < 0:
        raise ValueError("tool report contains no JSON object")
    return json.loads(text[start:])


def parse_fio(text, operation):
    report = _json_document(text)
    jobs = report.get("jobs", [])
    if len(jobs) != 1:
        raise ValueError("fio report must be group_reporting with exactly one job entry")
    side = jobs[0][operation]
    seconds = side["runtime"] / 1000
    if seconds <= 0:
        raise ValueError("fio reported zero runtime")
    return {"tool_version": report.get("fio version"), "bytes": side["io_bytes"], "seconds": seconds,
            "mib_per_s": side["io_bytes"] / seconds / MIB, "tool_bw_bytes": side["bw_bytes"]}


def parse_elbencho(text, phase_type):
    # --jsonfile is JSON Lines: one object per phase per line.
    phases = [json.loads(line) for line in text.splitlines() if line.strip()]
    found = [p for p in phases if p.get("phase_type") == phase_type]
    if len(found) != 1:
        raise ValueError("elbencho report must contain exactly one %s phase" % phase_type)
    phase = found[0]
    done = phase["last_done"]
    data, seconds = int(done["bytes"]), int(done["elapsed_time_ms"]) / 1000
    if seconds <= 0:
        raise ValueError("elbencho reported zero elapsed time")
    return {"tool_version": phase["config"].get("version"), "hosts": int(phase["config"].get("hosts", 1)),
            "bytes": data, "seconds": seconds, "mib_per_s": data / seconds / MIB,
            "tool_bytes_per_s": int(done.get("bytes/s", 0))}


def parse_iperf3(text):
    report = _json_document(text)
    received = report["end"]["sum_received"]
    return {"tool_version": report.get("start", {}).get("version"), "bytes": received["bytes"],
            "seconds": received["seconds"], "mib_per_s": received["bits_per_second"] / 8 / MIB,
            "retransmits": report["end"].get("sum_sent", {}).get("retransmits")}


EVICTED_MAX_FRACTION = 0.01
EVICTED_MAX_PAGES = 64  # A node writing its own state between evict and measure leaves a few pages
                        # resident; A1/A2 saw at most 39 pages (159744 B) on the writer's 3 GiB tree.
RESIDENT_FILES_KEPT = 10


def residency_verdict(measured, fraction=EVICTED_MAX_FRACTION, max_pages=EVICTED_MAX_PAGES):
    """Judge a `bench_probe.py resident` result after eviction.

    Allowed residency is max(fraction of the tree, max_pages pages): a relative bound alone turns
    any single cached page of a tiny metadata-only tree into a failure. Files still resident are
    returned largest first (bounded) so a failure names what stayed cached.
    """
    allowed = max(int(fraction * measured["bytes"]), max_pages * measured["page_size"])
    resident = sorted((f for f in measured["files"] if f["resident_bytes"]), key=lambda f: -f["resident_bytes"])
    return {"ok": measured["resident_bytes"] <= allowed, "allowed_bytes": allowed, "bytes": measured["bytes"],
            "resident_bytes": measured["resident_bytes"], "files": len(measured["files"]),
            "resident_files": resident[:RESIDENT_FILES_KEPT]}


PAYLOAD_MARKER = "/dfs/chunks/"  # DFS chunk bodies under a Node's state/; everything else is metadata
HOT_MIN_FRACTION = 0.99


def _subset(measured, files):
    return {"files": files, "bytes": sum(f["bytes"] for f in files),
            "resident_bytes": sum(f["resident_bytes"] for f in files), "page_size": measured["page_size"]}


def backend_verdict(measured, file_bytes, fraction=EVICTED_MAX_FRACTION):
    """Judge a Node backend tree after eviction, payload and metadata separately.

    Payload (chunk bodies) may keep at most `fraction` of the measured file resident, with no page
    floor: chunk residency cannot be attributed to a file, so any cached body beyond that makes the
    round not provably cold. Metadata a Node rewrites (catalog, epoch) keeps the
    max(fraction of tree, page floor) tolerance of residency_verdict.
    """
    payload = [f for f in measured["files"] if PAYLOAD_MARKER in f["path"]]
    metadata = [f for f in measured["files"] if PAYLOAD_MARKER not in f["path"]]
    body = _subset(measured, payload)
    allowed = int(fraction * file_bytes)
    resident = sorted((f for f in payload if f["resident_bytes"]), key=lambda f: -f["resident_bytes"])
    return {"payload": {"ok": body["resident_bytes"] <= allowed, "allowed_bytes": allowed, "bytes": body["bytes"],
                        "resident_bytes": body["resident_bytes"], "files": len(payload),
                        "resident_files": resident[:RESIDENT_FILES_KEPT]},
            "metadata": residency_verdict(_subset(measured, metadata), fraction)}


def cached_verdict(measured, file_bytes, minimum=HOT_MIN_FRACTION):
    """A hot read needs the probe to succeed and the file to be (almost) fully resident."""
    return {"ok": not measured["errors"] and measured["resident_bytes"] >= minimum * file_bytes,
            "required_bytes": int(minimum * file_bytes), "resident_bytes": measured["resident_bytes"],
            "errors": measured["errors"][:3]}


def parse_pressure(text):
    """Linux PSI (/proc/pressure/*) 10s and 60s averages; None when the kernel has no PSI."""
    values = {}
    for line in text.splitlines():
        fields = line.split()
        if fields and fields[0] in ("some", "full"):
            pairs = dict(f.split("=", 1) for f in fields[1:])
            values[fields[0] + "_avg10"] = float(pairs["avg10"])
            values[fields[0] + "_avg60"] = float(pairs["avg60"])
    return values or None


def parse_loadavg(text):
    return [float(v) for v in text.split()[:3]]


def host_window(before, after):
    """Host contention evidence around one measured tool run (same-kernel roles share it)."""
    return {"start": before["time"], "end": after["time"],
            "loadavg_before": parse_loadavg(before["loadavg"]), "loadavg_after": parse_loadavg(after["loadavg"]),
            "psi_cpu_after": parse_pressure(after["psi_cpu"]), "psi_io_after": parse_pressure(after["psi_io"])}


def summarize(samples):
    if not samples:
        raise ValueError("no samples to summarize")
    low, high = min(samples), max(samples)
    return {"n": len(samples), "median": statistics.median(samples), "min": low, "max": high,
            "max_over_min": high / low if low > 0 else None, "samples": list(samples)}
