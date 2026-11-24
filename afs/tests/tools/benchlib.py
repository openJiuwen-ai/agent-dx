"""Pure helpers for the opt-in benchmark suite (no I/O, no product knowledge).

The suite measures with industry tools (fio, elbencho, iperf3) and reads their own
reports. Everything here is deterministic so it can be pinned by runner_tests.
"""

import csv
import io
import json
import re
import statistics
from dataclasses import dataclass, field
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
    "dfs_read_cache_queue_bytes": ("int", 4 * MIB, 1024 * MIB),
    "dfs_read_cache_partial_bytes": ("zero_or", 4 * MIB, 1024 * MIB),
    "dfs_source_single_ttl_ms": ("int", 0, 60_000),
    "dfs_read_share_concurrent": ("bool",),
    "dfs_holder_ttl_ms": ("int", 50, 5000),
}
# Settings the product range-checks on every Node but uses only while the native read cache runs
# (dfs_read_cache_bytes non-zero): accepted without it, but recorded as inert instead of effective.
READ_CACHE_ONLY = ("dfs_read_cache_queue_bytes", "dfs_read_cache_partial_bytes", "dfs_source_single_ttl_ms",
                   "dfs_read_share_concurrent", "dfs_holder_ttl_ms")
# Product default of dfs_source_cache_max_ms (config/dfs.rs resolve_read_budget) when a role leaves it unset.
SOURCE_CACHE_MAX_MS_DEFAULT = 5000
# The serveable read cache and the private peer cache are exclusive on one Node (the product refuses both).
EXCLUSIVE_OVERRIDES = ("dfs_read_cache_bytes", "dfs_peer_cache.")
LOCAL_COPY = ("required", "preferred", "none")
REPLICATION_KEYS = ("dfs_desired_copies", "dfs_sync_required_copies", "dfs_local_copy")
ARRIVAL_GAP_MAX_MS = 600_000
PROGRESS_INTERVAL_MS = (100, 2000)
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
    progress_interval_ms: int = 0

    @property
    def reader_roles(self):
        return READER_ROLES[:self.readers]

    def overrides_for(self, role):
        """Effective overrides for one Node role: the "*" entries, then the role's own."""
        return {**self.node_overrides.get("*", {}), **self.node_overrides.get(role, {})}

    def inert_overrides(self, role):
        """Declared settings the product ignores on this role because its read cache is off."""
        return inert_keys(self.overrides_for(role))

    def effective_overrides(self, role):
        """The role's merged settings without the ones it ignores at run time (inert_overrides)."""
        return effective_settings(self.overrides_for(role))

    def applied_overrides(self, roles):
        """Return nonempty effective overrides for the given Node roles."""
        return {role: self.effective_overrides(role) for role in roles
                if role in NODE_ROLES and self.effective_overrides(role)}

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
                           **_variant_inputs(values, present))


def inert_keys(merged):
    """Declared settings one role's Node ignores at run time, judged on its merged ("*" + role) settings.

    With the read cache off (dfs_read_cache_bytes absent or 0) every READ_CACHE_ONLY key is inert. With it
    on, the single-source TTL is still inert while the ordinary source TTL is 0: the Node then caches no
    source metadata at all (sources.rs source_deadline yields no deadline, so source_expiry none). An
    explicit single TTL of 0 under a non-zero ordinary TTL keeps the ordinary deadline and is not inert.
    Concurrent sharing (H2 step 2b) runs only with dfs_read_share_concurrent true and the read cache on
    (assembly/dfs.rs read_sharing); dfs_holder_ttl_ms is used only while it runs.
    """
    if not merged.get("dfs_read_cache_bytes", 0):
        return sorted(k for k in merged if k in READ_CACHE_ONLY)
    inert = []
    if "dfs_source_single_ttl_ms" in merged and not merged.get("dfs_source_cache_max_ms",
                                                               SOURCE_CACHE_MAX_MS_DEFAULT):
        inert.append("dfs_source_single_ttl_ms")
    if "dfs_holder_ttl_ms" in merged and merged.get("dfs_read_share_concurrent") is not True:
        inert.append("dfs_holder_ttl_ms")
    return sorted(inert)


def effective_settings(merged):
    inert = inert_keys(merged)
    return {k: v for k, v in merged.items() if k not in inert}


def _override_value(key, value):
    spec = OVERRIDE_KEYS.get(key)
    if spec is None:
        raise Blocked("bench node override not allowed: " + str(key))
    if spec[0] == "bool":
        if not isinstance(value, bool):
            raise Blocked("bench node override %s must be true or false" % key)
    elif spec[0] == "choice":
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
    error = "bench log_events must list 1..200 character single-line strings"
    if not isinstance(extra, list):
        raise Blocked(error)
    for event in extra:
        if not isinstance(event, str) or not 1 <= len(event) <= 200 or "\n" in event:
            raise Blocked(error)
    return tuple(dict.fromkeys(LOG_EVENTS + tuple(extra)))


def _progress_input(values):
    """Optional elbencho live progress sampling interval (ms); 0 (absent) records no progress."""
    interval = values.get("progress_interval_ms", 0)
    if "progress_interval_ms" in values and (not is_integer(interval) or not
                                             PROGRESS_INTERVAL_MS[0] <= interval <= PROGRESS_INTERVAL_MS[1]):
        raise Blocked("bench progress_interval_ms must be %d..%d" % PROGRESS_INTERVAL_MS)
    return interval


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
    applied = any(effective_settings({**overrides.get("*", {}), **overrides.get(r, {})}) for r in present)
    if (applied or replication) and variant == "V0":
        raise Blocked("bench node overrides and replica settings need a variant label other than V0")
    readers = values.get("readers", 2)
    if not is_integer(readers) or not 1 <= readers <= len(READER_ROLES):
        raise Blocked("bench readers must be 1..4")
    return {
        "backend": backend,
        "memory_backend_bytes": memory if backend == "memory" else 0,
        "variant": variant,
        "node_overrides": dict(overrides),
        "readers": readers,
        "replication": replication,
        "arrival": _arrival_inputs(values, readers),
        "log_events": _log_event_inputs(values),
        "progress_interval_ms": _progress_input(values),
    }


def node_overrides_toml(overrides, peer_cache_directory):
    """Render whitelisted overrides as Node TOML: top-level keys first, then one table per prefix."""
    top, tables = [], {}
    for key, value in sorted(overrides.items()):
        rendered = json.dumps(value) if isinstance(value, (str, bool)) else str(value)
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


def _samples(text):
    table = {}
    for line in text.splitlines():
        if line and not line.startswith("#"):
            name, _, value = line.rpartition(" ")
            try:
                table[name] = float(value)
            except ValueError:
                continue
    return table


def metrics_delta(before, after, pattern):
    """Prometheus samples (name+labels) matching `pattern` whose value changed between two scrapes."""
    first, second, wanted = _samples(before), _samples(after), re.compile(pattern)
    delta = {}
    for name, value in second.items():
        change = value - first.get(name, 0.0)
        if change and wanted.search(name):
            delta[name] = change
    return delta


# Counter families a reconciliation depends on: (family, labels every sample must carry, any-of labels).
# Sent read payload of a Node, every transport.
SEND_SERIES = ("afs_dfs_payload_bytes_total", ('direction="send"', 'operation="read"'), ())
# Payload a reader received from peers (CRC-checked receive stages), the reader-side remote evidence.
RECEIVE_SERIES = ("afs_dfs_crc32c_read_bytes_total", (), ('stage="range_receive"', 'stage="trusted_segments_receive"'))


def counter_window(before, after, series):
    """One counter family between two scrapes: `complete` (same samples in both, none decreased),
    `missing` (absent from either scrape or a different sample set) or `reset` (a sample decreased).
    Only a complete window has a delta; a missing sample is never read as zero.
    """
    family, required, any_of = series

    def pick(text):
        selected = {}
        for name, value in _samples(text).items():
            if not name.startswith(family + "{") or not all(label in name for label in required):
                continue
            if any_of and not any(label in name for label in any_of):
                continue
            selected[name] = value
        return selected

    first, second = pick(before), pick(after)
    if not first or not second or set(first) != set(second):
        return {"status": "missing", "samples": sorted(set(first) ^ set(second)) or [family]}
    reset = sorted(n for n in second if second[n] < first[n])
    if reset:
        return {"status": "reset", "samples": reset}
    return {"status": "complete", "delta": int(sum(second[n] - first[n] for n in second))}


def counter_windows(before, after, roles, series):
    return {role: counter_window(before[role], after[role], series) for role in roles}


def send_reconciliation(windows, *, readers, file_bytes, tool_bytes, applies):
    """Sum of read payload sent by every Node (from complete SEND_SERIES windows) against N*S and the tool.

    `applies` is False when readers may hold local replicas (they read without any Node sending);
    the sums are then recorded but not judged (ok=None). With any Node's window missing or reset,
    `metrics` names the problem, the sums cannot be trusted and nothing is judged (ok=None).
    """
    issues = {role: w["status"] for role, w in windows.items() if w["status"] != "complete"}
    sent = {role: w["delta"] for role, w in windows.items() if w["status"] == "complete"}
    expected = readers * file_bytes
    total = sum(sent.values())
    metrics = "complete" if not issues else ("reset" if "reset" in issues.values() else "missing")
    check = {"sent_by_role": sent, "sent_total": total, "expected": expected, "tool_bytes": tool_bytes,
             "applies": applies, "metrics": metrics,
             "ok": (total == expected == tool_bytes) if applies and not issues else None}
    if issues:
        check["metrics_issues"] = issues
    return check


def reader_local_reads(windows, file_bytes):
    """Bytes each reader did not receive from peers while reading the whole file once (RECEIVE_SERIES).

    `status` is complete only when every reader's window is complete; local bytes never go below 0.
    """
    issues, by_reader = {}, {}
    for role, window in windows.items():
        if window["status"] != "complete":
            issues[role] = window["status"]
            continue
        received = window["delta"]
        by_reader[role] = {"received_bytes": received, "local_bytes": max(0, file_bytes - received)}
    value = {"status": "complete" if not issues else ("reset" if "reset" in issues.values() else "missing"),
             "by_reader": by_reader, "local_total": sum(r["local_bytes"] for r in by_reader.values())}
    if issues:
        value["issues"] = issues
    return value


SOURCE_FAMILY = "afs_dfs_read_source_bytes_total"
# Source roles a reader does not receive from another reader: its own disk, the durable copies (the writer's,
# when it holds the only copy) and the whole-object path, which does not say who served it.
NON_PEER_SOURCES = ("local", "durable", "object")


def labelled_counter_window(before, after, family, label):
    """One counter family between two scrapes, kept per value of `label` (same verdicts as counter_window).

    Every sample of the family must carry the label: a sample without it, or a different sample set,
    makes the window `missing`, never zero.
    """
    wanted = re.compile(r'^%s\{(?:.*,)?%s="([^"]*)"' % (re.escape(family), re.escape(label)))
    member = re.compile(r'^%s(?:\{|$)' % re.escape(family))

    def pick(text):
        selected = {}
        for name, value in _samples(text).items():
            if member.match(name):
                selected[name] = value
        return selected

    first, second = pick(before), pick(after)
    unlabelled = sorted(n for n in set(first) | set(second) if not wanted.match(n))
    if unlabelled:
        return {"status": "missing", "samples": unlabelled}
    if not first or not second or set(first) != set(second):
        return {"status": "missing", "samples": sorted(set(first) ^ set(second)) or [family]}
    reset = sorted(n for n in second if second[n] < first[n])
    if reset:
        return {"status": "reset", "samples": reset}
    by = {}
    for name, value in second.items():
        key = wanted.match(name).group(1)
        by[key] = by.get(key, 0) + int(value - first[name])
    return {"status": "complete", "by": by}


def source_supply(windows, sent, *, writer, file_bytes, applies):
    """Who supplied the readers in one read window: received bytes per reader and source role
    (`windows`, labelled_counter_window of SOURCE_FAMILY per reader) against the bytes each Node sent.

    `writer_share` is the writer's sent bytes over N*S (R_A). Peer supply is every source role but
    NON_PEER_SOURCES, so a new reader-to-reader role counts without a framework change. When the writer
    holds the only copy (`applies`), durable bytes received must equal the writer's sent bytes and peer
    bytes received must equal what the other Nodes sent (`ok`). Incomplete counters, a missing writer
    window or whole-object bytes (`unattributed`) leave `ok` None; nothing is raised.
    """
    issues, by_reader, by_role = {}, {}, {}
    for role, window in windows.items():
        if window["status"] != "complete":
            issues[role] = window["status"]
            continue
        by_reader[role] = dict(window["by"])
        for source, value in window["by"].items():
            by_role[source] = by_role.get(source, 0) + value
    peer_received = 0
    for source, value in by_role.items():
        if source not in NON_PEER_SOURCES:
            peer_received += value
    writer_sent = sent.get(writer)
    readers_sent = sum(value for role, value in sent.items() if role != writer)
    expected = len(windows) * file_bytes
    supply = {"status": "complete" if not issues else ("reset" if "reset" in issues.values() else "missing"),
              "received_by_reader": by_reader, "received_by_role": by_role, "peer_received": peer_received,
              "writer_sent": writer_sent, "readers_sent": readers_sent,
              "writer_share": None if writer_sent is None or not expected else writer_sent / expected, "ok": None}
    if issues:
        supply["issues"] = issues
    if by_role.get("object", 0):
        supply["unattributed"] = "object"
    judged = applies and not issues and writer_sent is not None
    if judged and "unattributed" not in supply:
        supply["ok"] = by_role.get("durable", 0) == writer_sent and peer_received == readers_sent
    return supply


def reader_peer_locality(groups, readers):
    """Whether reader-to-reader traffic crosses kernels, as far as the kernel groups alone determine it.

    `same_kernel` when every reader shares one kernel, `cross_kernel` when no two readers share one,
    `no_peer` for a single reader. Any other grouping is `undetermined`: per-role sent and received bytes
    cannot tell which reader served which (no sender/receiver pair counter exists in the product).
    """
    readers = set(readers)
    counts = [len(readers.intersection(group)) for group in groups]
    if len(readers) < 2:
        return "no_peer"
    if max(counts) == len(readers):
        return "same_kernel"
    if max(counts) == 1:
        return "cross_kernel"
    return "undetermined"


def kernel_groups(kernels):
    """Roles grouped by the kernel they run on (`kernels`: role -> boot id), without the boot ids."""
    groups = {}
    for role, kernel in kernels.items():
        groups.setdefault(kernel, []).append(role)
    return sorted(sorted(roles) for roles in groups.values())


LOCAL_REPLICA_SUFFIX = "_local_replica"
UNATTRIBUTED_SUFFIX = "_unattributed"


def replica_layer(layer, check, local):
    """Layer name of one read window after its send check and reader-side evidence.

    Unchanged when the check is judged (the writer holds the only copy), or when complete counters show
    the Nodes sent at least N*S. With reader replicas, a shortfall is `_local_replica` only when every
    send and reader receive window is complete and readers did read bytes they never received;
    otherwise (counters missing or reset, or a shortfall no reader accounts for) `_unattributed`.
    Neither is ever pooled with remote reads.
    """
    if check["applies"]:
        return layer
    if check["metrics"] == "complete" and check["sent_total"] >= check["expected"]:
        return layer
    if check["metrics"] == "complete" and local["status"] == "complete" and local["local_total"] > 0:
        return layer + LOCAL_REPLICA_SUFFIX
    return layer + UNATTRIBUTED_SUFFIX


def placement_coverage(files_by_role, baseline, file_bytes):
    """Whether this round's file is covered by the chunk bodies written for it.

    Chunk bodies already listed before the write on any Node (`baseline`, role -> {chunk id: bytes}) are
    residue of earlier files and are recorded as stale, never counted. The remaining chunk ids are this
    round's; each counts once (`distinct_bytes`), further copies count as `replica_bytes`. Coverage is
    `verified` only when the distinct chunks add up exactly to the file and every copy of a chunk has
    the same size; anything else is `unverified`. The product exposes no per-file layout, so logical
    ranges are not checked: a round's chunks are disjoint by construction (fresh ids per write).
    """
    old = {cid for ids in baseline.values() for cid in ids}
    distinct, by_role, stale, consistent = {}, {}, {}, True
    for role, files in files_by_role.items():
        for f in files:
            if PAYLOAD_MARKER not in f["path"]:
                continue
            cid = PurePosixPath(f["path"]).name
            if cid in old:
                stale[role] = stale.get(role, 0) + f["bytes"]
                continue
            by_role[role] = by_role.get(role, 0) + f["bytes"]
            consistent &= distinct.setdefault(cid, f["bytes"]) == f["bytes"]
    total = sum(distinct.values())
    return {"coverage": "verified" if consistent and total == file_bytes else "unverified",
            "chunks": len(distinct), "distinct_bytes": total, "replica_bytes": sum(by_role.values()) - total,
            "by_role": by_role, "stale_by_role": stale}


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


def elbencho_read_argv(elbencho, hosts, path, *, block, file_bytes, threads, result, live_csv=None,
                       live_interval_ms=0):
    """Every service host reads the whole file (`--nosvcshare`); the master only coordinates.

    `--svcelapsed` adds the completion time of every service to the console results. With `live_csv`,
    per-service progress is sampled every `live_interval_ms` into that CSV (`--livecsvex`).
    """
    live = ["--livecsv", live_csv, "--livecsvex", "--liveint", str(live_interval_ms)] if live_csv else []
    return [
        elbencho, "--hosts", ",".join(hosts), "--nosvcshare", "-r", "-b", str(block), "-s", str(file_bytes),
        "-t", str(threads), "--nolive", "--svcelapsed", *live, "--jsonfile", result, path,
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


ELAPSED_PART = re.compile(r"(\d+(?:\.\d+)?)(ms|h|m|s)")
ELAPSED_UNITS_MS = {"h": 3_600_000, "m": 60_000, "s": 1000, "ms": 1}
SVC_COMPLETION_ROW = "Svc compl. time"


def elbencho_elapsed_ms(text):
    """elbencho's human elapsed time ("529ms", "1.204s", "2m3.004s", "1h2m3s") in milliseconds."""
    parts = ELAPSED_PART.findall(text)
    if not parts or "".join(number + unit for number, unit in parts) != text:
        raise ValueError("not an elbencho elapsed time: %r" % text)
    return round(sum(float(number) * ELAPSED_UNITS_MS[unit] for number, unit in parts))


def _spread(by_reader):
    values = sorted(by_reader.values())
    return {"by_reader": by_reader, "first_ms": values[0], "median_ms": statistics.median(values),
            "last_ms": values[-1], "spread_ms": values[-1] - values[0]}


def elbencho_service_completion(console, services):
    """Completion time of every reader from elbencho's `--svcelapsed` console row.

    `services` maps each elbencho service ("host:port") to its reader role; every service must appear once.
    """
    rows = [line for line in console.splitlines() if SVC_COMPLETION_ROW in line]
    if len(rows) != 1 or "[" not in rows[0] or "]" not in rows[0]:
        raise ValueError("elbencho console must contain exactly one %r row" % SVC_COMPLETION_ROW)
    by_reader = {}
    for token in rows[0][rows[0].index("[") + 1:rows[0].rindex("]")].split():
        service, _, elapsed = token.rpartition("=")
        if service not in services or services[service] in by_reader:
            raise ValueError("unexpected or repeated elbencho service in completion row: %r" % token)
        by_reader[services[service]] = elbencho_elapsed_ms(elapsed)
    if len(by_reader) != len(services):
        raise ValueError("elbencho completion row lacks services: %r" % sorted(set(services) - set(by_reader)))
    return _spread(by_reader)


def elbencho_progress(text, services, file_bytes, phase="READ"):
    """Per-reader progress from elbencho's extended live CSV (`--livecsv --livecsvex`).

    Service rows carry the bytes each service has read so far at the master's RuntimeMS. Reported per
    reader: the first sample with progress, the first sample at the whole file (None if no sample shows
    it) and the sample count; for the readers together, the largest lead in bytes between the fastest
    and the slowest reader at one sample, when it happened and its fraction of the file, and the spread
    of the first-progress times. Resolution is the sampling interval.
    """
    reader = csv.DictReader(io.StringIO(text))
    missing = sorted({"Phase", "RuntimeMS", "DoneBytes", "Service"} - set(reader.fieldnames or ()))
    if missing:
        raise ValueError("elbencho live report lacks columns: %r" % missing)
    samples = {}
    for row in reader:
        # csv fills a short row's missing fields with None and keeps a long row's extra fields under None.
        if None in row or None in row.values():
            raise ValueError("elbencho live report has a malformed row: %r" % row)
        if row.get("Phase") != phase or row.get("Service") not in services:
            continue
        try:
            runtime, done = int(row["RuntimeMS"]), int(row["DoneBytes"])
        except (TypeError, ValueError) as error:
            raise ValueError("elbencho live report has a malformed row: %r" % row) from error
        samples.setdefault(runtime, {})[services[row["Service"]]] = done
    seen = set()
    for values in samples.values():
        seen.update(values)
    if seen != set(services.values()):
        raise ValueError("elbencho live report lacks readers: %r" % sorted(set(services.values()) - seen))
    by_reader = {role: {"first_progress_ms": None, "done_ms": None, "samples": 0} for role in seen}
    lead, lead_at = 0, None
    for runtime in sorted(samples):
        values = samples[runtime]
        for role, done in values.items():
            entry = by_reader[role]
            entry["samples"] += 1
            if done > 0 and entry["first_progress_ms"] is None:
                entry["first_progress_ms"] = runtime
            if done >= file_bytes and entry["done_ms"] is None:
                entry["done_ms"] = runtime
        if len(values) == len(seen) and max(values.values()) - min(values.values()) > lead:
            lead, lead_at = max(values.values()) - min(values.values()), runtime
    first = [entry["first_progress_ms"] for entry in by_reader.values() if entry["first_progress_ms"] is not None]
    return {"by_reader": by_reader, "max_lead_bytes": lead, "max_lead_at_ms": lead_at,
            "max_lead_fraction": lead / file_bytes if file_bytes else None,
            "first_progress_spread_ms": max(first) - min(first) if len(first) == len(seen) else None}


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
