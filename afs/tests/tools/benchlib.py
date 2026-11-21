"""Pure helpers for the opt-in benchmark suite (no I/O, no product knowledge).

The suite measures with industry tools (fio, elbencho, iperf3) and reads their own
reports. Everything here is deterministic so it can be pinned by runner_tests.
"""

from dataclasses import dataclass, field
import json
import re
import statistics
from pathlib import PurePosixPath

from fixtures.common import Blocked, is_integer

MIB = 1024**2
FIO_OPERATIONS = ("read", "write")


READER_ROLES = ("B", "C", "D", "E")
NODE_ROLES = ("A",) + READER_ROLES
BACKENDS = ("disk", "memory")
KIB = 1024
# Whitelisted Node settings a bench variant may change, within the product's own limits:
# key -> ("int", lower, upper), ("choice", allowed), ("step", unit, lower, upper) or ("zero_or", lower, upper).
OVERRIDE_KEYS = {
    "dfs_read_parallelism": ("int", 1, 8),
    "dfs_readahead_max_bytes": ("int", 0, 32 * MIB),
    "dfs_read_crc_mode": ("choice", ("receiver", "off")),
    "dfs_read_pipeline.max_rpcs": ("int", 1, 128),
    "dfs_peer_cache.capacity_bytes": ("int", MIB, 1024**4),
    "dfs_read_cache_bytes": ("zero_or", 4 * MIB + 4 * KIB, 1024**4),
    "dfs_read_stripe_bytes": ("step", 64 * KIB, 64 * KIB, 8 * MIB),
    "dfs_source_cache_max_ms": ("int", 0, 60_000),
}
# The serveable read cache and the private peer cache are exclusive on one Node (the product refuses both).
EXCLUSIVE_OVERRIDES = ("dfs_read_cache_bytes", "dfs_peer_cache.")
LOCAL_COPY = ("required", "preferred", "none")
REPLICATION_KEYS = ("dfs_desired_copies", "dfs_sync_required_copies", "dfs_local_copy")
ARRIVAL_GAP_MAX_MS = 600_000
# Node log lines counted every round (substring per line); bench.log_events adds more.
LOG_EVENTS = ("dfs.read_cache_install_failed", "dfs.read_cache_reconcile_retry",
              "DFS peer read admission deadline exceeded", "peer stayed busy")


@dataclass(frozen=True)
class BenchParameters:
    file_bytes: int
    block_bytes: tuple[int, ...]
    threads: tuple[int, ...]
    rounds: int
    tools: dict
    backend: str = "disk"
    memory_backend_bytes: int = 0
    variant: str = "V0"
    node_overrides: dict = field(default_factory=dict)
    readers: int = 2
    replication: dict = field(default_factory=dict)
    arrival: dict = field(default_factory=lambda: {"mode": "together"})
    log_events: tuple[str, ...] = LOG_EVENTS

    @property
    def reader_roles(self):
        return READER_ROLES[:self.readers]

    def overrides_for(self, role):
        """Effective overrides for one Node role: the "*" entries, then the role's own."""
        return {**self.node_overrides.get("*", {}), **self.node_overrides.get(role, {})}

    def applied_overrides(self, roles):
        """Overrides that reach the given Node roles; roles outside the cluster contribute nothing."""
        return {role: self.overrides_for(role) for role in roles if role in NODE_ROLES and self.overrides_for(role)}

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
    present = tuple(r for r in NODE_ROLES if r in config["roles"]) if isinstance(config.get("roles"), dict) \
        else NODE_ROLES
    return BenchParameters(size, tuple(blocks), tuple(threads), rounds, dict(tools),
                           *_variant_inputs(values, present))


def _override_value(key, value):
    spec = OVERRIDE_KEYS.get(key)
    if spec is None:
        raise Blocked("bench node override not allowed: " + str(key))
    if spec[0] == "choice":
        if value not in spec[1]:
            raise Blocked("bench node override %s must be one of %s" % (key, ",".join(spec[1])))
    elif spec[0] == "step":
        if not is_integer(value) or not spec[2] <= value <= spec[3] or value % spec[1]:
            raise Blocked("bench node override %s must be a multiple of %d in %d..%d" % (key, *spec[1:]))
    elif spec[0] == "zero_or":
        if not is_integer(value) or value != 0 and not spec[1] <= value <= spec[2]:
            raise Blocked("bench node override %s must be 0 or %d..%d" % (key, spec[1], spec[2]))
    elif not is_integer(value) or not spec[1] <= value <= spec[2]:
        raise Blocked("bench node override %s must be an integer %d..%d" % (key, spec[1], spec[2]))


def _replication_inputs(values):
    """Topology-level replica settings (Meta and every Node); absent keys keep the fixture default."""
    declared = {k: values[k] for k in REPLICATION_KEYS if k in values}
    desired = declared.get("dfs_desired_copies", 1)
    if not is_integer(desired) or not 1 <= desired <= len(NODE_ROLES):
        raise Blocked("bench dfs_desired_copies must be 1..%d" % len(NODE_ROLES))
    sync = declared.get("dfs_sync_required_copies", desired)
    if not is_integer(sync) or not 1 <= sync <= desired:
        raise Blocked("bench dfs_sync_required_copies must be 1..dfs_desired_copies")
    if declared.get("dfs_local_copy", "required") not in LOCAL_COPY:
        raise Blocked("bench dfs_local_copy must be one of " + ",".join(LOCAL_COPY))
    return declared


def replication_settings(declared):
    """Full Meta/Node replica settings for declared bench replication; None keeps the fixture default.

    Sync copies default to the desired count and every copy needs its own Node, as in the fixture.
    """
    if not declared:
        return None
    desired = declared.get("dfs_desired_copies", 1)
    return {"dfs_desired_copies": desired,
            "dfs_sync_required_copies": declared.get("dfs_sync_required_copies", desired),
            "dfs_min_distinct_nodes": desired, "dfs_min_distinct_failure_domains": 1,
            "dfs_local_copy": declared.get("dfs_local_copy", "required")}


def replication_toml(settings):
    return "".join("%s = %s\n" % (k, json.dumps(v)) for k, v in settings.items())


def single_writer_copy(declared):
    """True when the writer A holds the only replica, so every reader byte must cross the network."""
    settings = replication_settings(declared) or {"dfs_desired_copies": 1, "dfs_local_copy": "required"}
    return settings["dfs_desired_copies"] == 1 and settings["dfs_local_copy"] == "required"


def _arrival_inputs(values, readers):
    arrival = values.get("fanout_arrival", {"mode": "together"})
    if not isinstance(arrival, dict):
        raise Blocked("bench fanout_arrival must be an object with a mode")
    if arrival.get("mode") == "together" and set(arrival) == {"mode"}:
        return {"mode": "together"}
    if arrival.get("mode") != "stagger" or set(arrival) != {"mode", "seed", "gap_ms"}:
        raise Blocked("bench fanout_arrival is {mode: together} or {mode: stagger, seed, gap_ms}")
    if readers < 2 or arrival["seed"] not in READER_ROLES[:readers]:
        raise Blocked("staggered fan-out needs at least two readers and a seed among them")
    gap = arrival["gap_ms"]
    if not is_integer(gap) or not 0 <= gap <= ARRIVAL_GAP_MAX_MS:
        raise Blocked("bench fanout_arrival gap_ms must be 0..%d" % ARRIVAL_GAP_MAX_MS)
    return dict(arrival)


def arrival_phases(arrival, readers):
    """Reader groups in start order: stagger runs the seed alone, then the others together."""
    if arrival["mode"] == "together":
        return [("all", tuple(readers))]
    seed = arrival["seed"]
    return [("seed", (seed,)), ("followers", tuple(r for r in readers if r != seed))]


def _log_event_inputs(values):
    extra = values.get("log_events", [])
    if not isinstance(extra, list) or not all(
            isinstance(e, str) and 1 <= len(e) <= 200 and "\n" not in e for e in extra):
        raise Blocked("bench log_events must list 1..200 character single-line strings")
    return tuple(dict.fromkeys(LOG_EVENTS + tuple(extra)))


def _variant_inputs(values, present=NODE_ROLES):
    """Optional verification inputs; absent keys keep the disk-backed default configuration."""
    backend = values.get("backend", "disk")
    if backend not in BACKENDS:
        raise Blocked("bench backend must be disk or memory")
    memory = values.get("memory_backend_bytes", 0)
    if backend == "memory" and (not is_integer(memory) or not 256 * MIB <= memory <= 64 * 1024**3):
        raise Blocked("memory backend needs memory_backend_bytes 256MiB..64GiB (tmpfs size per Node)")
    overrides = values.get("node_overrides", {})
    if not isinstance(overrides, dict):
        raise Blocked("bench node_overrides must map roles to settings")
    for role, settings in overrides.items():
        if role != "*" and role not in NODE_ROLES or not isinstance(settings, dict):
            raise Blocked("bench node_overrides roles are *, A, B, C, D or E with a settings object")
        for key, value in settings.items():
            _override_value(key, value)
    for role in NODE_ROLES:
        merged = {**overrides.get("*", {}), **overrides.get(role, {})}
        if EXCLUSIVE_OVERRIDES[0] in merged and any(k.startswith(EXCLUSIVE_OVERRIDES[1]) for k in merged):
            raise Blocked("bench node %s cannot use dfs_read_cache_bytes together with dfs_peer_cache" % role)
    replication = _replication_inputs(values)
    variant = values.get("variant", "V0")
    if not isinstance(variant, str) or not 1 <= len(variant) <= 32:
        raise Blocked("bench variant must be a label of 1..32 characters")
    applied = any({**overrides.get("*", {}), **overrides.get(r, {})} for r in present)
    if (applied or replication) and variant == "V0":
        raise Blocked("bench node overrides and replica settings need a variant label other than V0")
    readers = values.get("readers", 2)
    if not is_integer(readers) or not 1 <= readers <= len(READER_ROLES):
        raise Blocked("bench readers must be 1..4")
    return (backend, memory if backend == "memory" else 0, variant, dict(overrides), readers, replication,
            _arrival_inputs(values, readers), _log_event_inputs(values))


def node_overrides_toml(overrides, peer_cache_directory):
    """Render whitelisted overrides as Node TOML: top-level keys first, then one table per prefix."""
    top, tables = [], {}
    for key, value in sorted(overrides.items()):
        rendered = json.dumps(value) if isinstance(value, str) else str(value)
        if "." in key:
            table, name = key.split(".", 1)
            tables.setdefault(table, []).append("%s = %s" % (name, rendered))
        else:
            top.append("%s = %s" % (key, rendered))
    if "dfs_peer_cache" in tables:
        tables["dfs_peer_cache"].insert(0, "directory = " + json.dumps(peer_cache_directory))
    lines = top + ["\n".join(["[%s]" % name] + entries) for name, entries in sorted(tables.items())]
    return "".join(line + "\n" for line in lines)


def effective_mismatches(effective, overrides):
    """Declared overrides that `--print-config` does not show with the same value."""
    mismatches = []
    for key, value in sorted(overrides.items()):
        actual = effective
        for part in key.split("."):
            actual = actual.get(part) if isinstance(actual, dict) else None
        if actual != value:
            mismatches.append({"key": key, "declared": value, "effective": actual})
    return mismatches


def metrics_delta(before, after, pattern):
    """Prometheus samples (name+labels) matching `pattern` whose value changed between two scrapes."""

    def samples(text):
        table = {}
        for line in text.splitlines():
            if line and not line.startswith("#"):
                name, _, value = line.rpartition(" ")
                try:
                    table[name] = float(value)
                except ValueError:
                    continue
        return table

    first, second, wanted = samples(before), samples(after), re.compile(pattern)
    delta = {}
    for name, value in second.items():
        change = value - first.get(name, 0.0)
        if change and wanted.search(name):
            delta[name] = change
    return delta


SENT_READ_PAYLOAD = "afs_dfs_payload_bytes_total{"


def sent_read_payload(delta):
    """Read payload bytes a Node served to peers (every transport) from one metrics_delta."""
    return int(sum(v for name, v in delta.items() if name.startswith(SENT_READ_PAYLOAD)
                   and 'direction="send"' in name and 'operation="read"' in name))


def send_reconciliation(deltas, *, readers, file_bytes, tool_bytes, applies):
    """Sum of read payload sent by every Node against N*S and the tool's own byte count.

    `applies` is False when readers may hold local replicas (they read without any Node sending);
    the sums are still recorded but not judged (ok=None).
    """
    sent = {role: sent_read_payload(delta) for role, delta in deltas.items() if role != "M"}
    expected = readers * file_bytes
    total = sum(sent.values())
    return {"sent_by_role": sent, "sent_total": total, "expected": expected, "tool_bytes": tool_bytes,
            "ok": (total == expected == tool_bytes) if applies else None}


LOCAL_REPLICA_SUFFIX = "_local_replica"


def replica_layer(layer, check):
    """Layer name after the round's send reconciliation.

    Unchanged when the writer holds the only copy (the check is judged) or when the Nodes sent at least
    N*S. With reader replicas (check not judged) and fewer bytes sent, part of the read was served by a
    reader's own replica, so the layer gets LOCAL_REPLICA_SUFFIX and is never pooled with remote reads.
    """
    if check["ok"] is None and check["sent_total"] < check["expected"]:
        return layer + LOCAL_REPLICA_SUFFIX
    return layer


# Runs on a Node host as root: counts lines containing each event after a byte offset of one log.
# A log shorter than the offset was truncated or replaced and is counted from its start.
LOG_EVENTS_SCRIPT = (
    "path,offset,events=sys.argv[1],int(sys.argv[2]),json.loads(sys.argv[3])\n"
    "try:\n f=open(path,'rb')\nexcept OSError:\n print(json.dumps(None));sys.exit(0)\n"
    "with f:\n"
    " size=f.seek(0,2);rotated=size<offset;f.seek(0 if rotated else offset);lines=f.read().splitlines()\n"
    "counts={e:sum(1 for l in lines if e.encode() in l) for e in events}\n"
    "print(json.dumps({'size':size,'rotated':rotated,'counts':counts}))"
)


def parse_kernel_memory(vmstat, meminfo):
    """Kernel-wide oom_kill count (/proc/vmstat) and MemAvailable bytes (/proc/meminfo); None when unreadable.

    Role namespaces do not expose their cgroup files, so OOM is detected for the whole kernel: any OOM
    kill on the host during a round voids it (conservative for shared hosts).
    """
    kills = available = None
    for line in vmstat.splitlines():
        fields = line.split()
        if len(fields) == 2 and fields[0] == "oom_kill":
            kills = int(fields[1])
    for line in meminfo.splitlines():
        fields = line.split()
        if len(fields) >= 2 and fields[0] == "MemAvailable:":
            available = int(fields[1]) * 1024
    return {"mem_available": available, "oom_kill": kills}


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
