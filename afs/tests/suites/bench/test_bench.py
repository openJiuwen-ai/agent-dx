"""Opt-in industry-standard benchmarks (fio, elbencho, iperf3); excluded from correctness suites.

Every case needs `--allow-performance` and an explicit `bench` section in the env file
(see tools/benchlib.py). Cases record each round with its cache layer and the evidence
that the layer was reached; a round whose cache state cannot be shown fails the case
instead of producing an unlabelled number. Nothing here claims a performance result.

Layers: `cold` = page cache evicted and verified empty before the read;
`hot` = immediate re-read after a full read; `remote_cold` = a reader that never
opened the file reads it after every Node's backend files were evicted and verified.
`same_kernel` marks remote reads whose roles share one kernel (shared page cache).
With declared reader replicas, a cold read whose Nodes sent fewer than N*S bytes is suffixed
`_local_replica`: part of it was served by the readers' own copies.
Fan-out readers start together, or staggered (`bench.fanout_arrival`): a seed reader reads
alone, then the others read together after a gap. Followers use `remote_post_seed`
(`remote_post_seed_memory_backend` for tmpfs); their backend evidence is from before the seed.
"""

import errno
import hashlib
import json
import time
from pathlib import Path, PurePosixPath
from posixpath import join as remote_join

from fixtures.common import Blocked, Case
from tools import benchlib

TOOLS = Path(__file__).resolve().parents[2] / "tools"
PROBE = (TOOLS / "bench_probe.py").read_text()
EVICTED_MAX_FRACTION = benchlib.EVICTED_MAX_FRACTION
HOT_MIN_FRACTION = 0.99
ELBENCHO_PORT = 26611
IPERF3_PORT = 26612


def probe(c, role, action, *paths):
    result = c.command(role, ["python3", "-c", PROBE, action, *paths], timeout=1800)
    return json.loads(result["stdout"])


def run_tool(c, role, params, name, argv_tail, timeout=3600):
    path, env = params.tool(name)
    return c.command(role, [path, *argv_tail], uid=c.uid, gid=c.gid, env=env, timeout=timeout)


def tool_identity(c, params, roles, names):
    """Record version output and SHA-256 of each tool binary as seen by each role."""
    identity = {}
    for role in roles:
        for name in names:
            path, env = params.tool(name)
            digest = c.python(role, "print(hashlib.sha256(open(sys.argv[1],'rb').read()).hexdigest())", path,
                              owner=False)["stdout"].strip()
            version = c.command(role, [path, "--version"], env=env, check=False)
            identity[role + ":" + name] = {"path": path, "sha256": digest,
                                           "version": (version["stdout"] or version["stderr"]).strip()[:400]}
    return c.record("bench.tool_identity", identity)


def boot_id(c, role):
    return c.python(role, "print(open('/proc/sys/kernel/random/boot_id').read().strip())", owner=False)[
        "stdout"].strip()


def backend_dirs(c, roles):
    return {role: remote_join(c.role_dir(role), "state") for role in roles}


def split_vanished(result):
    """Return the remaining probe errors and the count of ENOENT files reclaimed by Node GC."""
    errors = result.get("errors", [])
    gone = [e for e in errors if e.get("errno") == errno.ENOENT]
    return [e for e in errors if e.get("errno") != errno.ENOENT], len(gone)


def payload_listing(c, roles):
    """Chunk bodies (id -> bytes) each Node holds now; taken before a write as the coverage baseline.

    A file that vanished during the listing (Node GC) is simply not listed.
    """
    listed = {}
    for role, directory in backend_dirs(c, roles).items():
        value = probe(c, role, "list", directory)
        errors, _ = split_vanished(value)
        if errors:
            raise AssertionError("backend listing incomplete on %s: %r" % (role, errors[:3]))
        chunks = {}
        for entry in value["files"]:
            if benchlib.PAYLOAD_MARKER in entry["path"]:
                chunks[PurePosixPath(entry["path"]).name] = entry["bytes"]
        listed[role] = chunks
    return listed


def evict_backends(c, roles, file_bytes, source, baseline=None):
    """Evict every Node's backend and judge payload and metadata separately (benchlib.backend_verdict).

    If the first measurement is over a bound, the tree is evicted and measured exactly once more
    (a Node may touch its own files between the two probe calls); both attempts are kept.
    Metadata still over its tolerance fails the case. Payload still cached returns cold=False, so
    the caller labels the round `remote_cold_unverified` instead of `remote_cold`. Files reclaimed
    between listing and opening (ENOENT) count as absent; their number is recorded as `enoent`.
    Without `baseline` (the writer holds the only copy) the source Node must hold the whole file,
    otherwise the backend layout is unrecognised. With `baseline` (chunk bodies listed before the write,
    payload_listing) the file must be covered by this round's distinct chunks (benchlib.placement_coverage,
    recorded as `placement`); an unverified coverage makes the round not provably cold.
    Returns (evidence, cold).
    """
    evidence, cold, holdings, files = {}, True, {}, {}
    for role, directory in backend_dirs(c, roles).items():
        attempts, vanished = [], {"evict": 0, "resident": 0}
        for _ in range(2):
            evict_errors, evict_gone = split_vanished(probe(c, role, "evict", directory))
            measured = probe(c, role, "resident", directory)
            resident_errors, resident_gone = split_vanished(measured)
            vanished["evict"] += evict_gone
            vanished["resident"] += resident_gone
            if evict_errors or resident_errors:
                raise AssertionError("backend eviction evidence incomplete on %s: %r" % (
                    role, (evict_errors + resident_errors)[:3]))
            attempts.append(benchlib.backend_verdict(measured, file_bytes))
            if attempts[-1]["payload"]["ok"] and attempts[-1]["metadata"]["ok"]:
                break
        final = attempts[-1]
        holdings[role] = final["payload"]["bytes"]
        files[role] = measured["files"]
        if not final["metadata"]["ok"]:
            raise AssertionError("backend metadata still resident on %s after re-eviction: %d of %d bytes "
                                 "(allowed %d); resident files %r" % (
                                     role, final["metadata"]["resident_bytes"], final["metadata"]["bytes"],
                                     final["metadata"]["allowed_bytes"], final["metadata"]["resident_files"]))
        cold &= final["payload"]["ok"]
        evidence[role] = dict(final, attempts=len(attempts))
        if len(attempts) == 2:
            evidence[role]["first_attempt"] = attempts[0]
        if vanished["evict"] or vanished["resident"]:
            evidence[role]["enoent"] = vanished
    if baseline is None:
        if holdings.get(source, 0) < file_bytes:
            raise AssertionError("backend payload layout not recognised on %s: %d payload bytes under %r for a "
                                 "%d-byte file" % (source, holdings.get(source, 0), benchlib.PAYLOAD_MARKER,
                                                   file_bytes))
        return evidence, cold
    placement = dict(benchlib.placement_coverage(files, baseline, file_bytes), source=source)
    if not placement["distinct_bytes"]:
        raise AssertionError("backend payload layout not recognised: no new chunk bodies under %r on any Node "
                             "(%r) for a %d-byte file" % (benchlib.PAYLOAD_MARKER, holdings, file_bytes))
    evidence["placement"] = placement
    return evidence, cold and placement["coverage"] == "verified"


def require_separate_readers(config, readers):
    """Fan-out runs one elbencho service per reader on a fixed port inside that reader's worker."""
    hosts = [config["roles"][r]["host"] for r in readers]
    if len(set(hosts)) != len(hosts):
        raise Blocked("fan-out readers %s need separate role hosts; one host cannot run two elbencho services "
                      "on one port or give two readers one shared path" % ",".join(readers))


def mkdir_owned(c, role, path):
    c.python(role, "os.makedirs(sys.argv[1],0o755,exist_ok=True);os.chown(sys.argv[1],int(sys.argv[2]),"
                   "int(sys.argv[3]))", path, c.uid, c.gid, owner=False)


def remove(c, role, path):
    c.python(role, "os.unlink(sys.argv[1])", path)


SNAPSHOT = (
    "import time\n"
    "def read(p):\n"
    " try:\n  return open(p).read()\n"
    " except OSError:\n  return ''\n"
    "print(json.dumps({'time':time.strftime('%Y-%m-%dT%H:%M:%SZ',time.gmtime()),'diskstats':read('/proc/diskstats'),"
    "'loadavg':read('/proc/loadavg'),'psi_cpu':read('/proc/pressure/cpu'),'psi_io':read('/proc/pressure/io'),"
    "'vmstat':read('/proc/vmstat'),'meminfo':read('/proc/meminfo')}))"
)


def host_snapshot(c, roles):
    """Block-device counters, load and PSI as seen by each role's kernel (for cross-checks)."""
    return {r: json.loads(c.python(r, SNAPSHOT, owner=False)["stdout"]) for r in roles}


def memory_window(before, after):
    first = benchlib.parse_kernel_memory(before.get("vmstat", ""), before.get("meminfo", ""))
    second = benchlib.parse_kernel_memory(after.get("vmstat", ""), after.get("meminfo", ""))
    kills = None if None in (first["oom_kill"], second["oom_kill"]) else second["oom_kill"] - first["oom_kill"]
    return {"mem_available_before": first["mem_available"], "mem_available_after": second["mem_available"],
            "oom_kill_delta": kills}


def require_no_oom(memory):
    """Reject rounds with OOM kills or unreadable OOM counters."""
    unknown = sorted(role for role, m in memory.items() if m["oom_kill_delta"] is None)
    if unknown:
        raise AssertionError("oom_kill unreadable on %s, round void" % ",".join(unknown))
    killed = {role: m["oom_kill_delta"] for role, m in memory.items() if m["oom_kill_delta"]}
    if killed:
        raise AssertionError("OOM kill during the round, round void: %r" % killed)


def round_memory(before, after):
    """OOM window over every role's kernel from round start to its end (gaps and serving included)."""
    memory = {role: memory_window(before[role], after[role]) for role in before}
    require_no_oom(memory)
    return memory


def host_evidence(before, after):
    hosts = {}
    for role in before:
        hosts[role] = dict(benchlib.host_window(before[role], after[role]),
                           memory=memory_window(before[role], after[role]))
    require_no_oom({role: value["memory"] for role, value in hosts.items()})
    return ({r: benchlib.diskstats_delta(before[r]["diskstats"], after[r]["diskstats"]) for r in before}, hosts)


WRITER_METRICS = r"read|peer|serve|slot|reject|admission|gc|reclaim|collect|cache"
NODE_METRICS = WRITER_METRICS + r"|crc32c_read_bytes"
# Meta requests that serve read sources, the read cache and replica placement (not namespace traffic).
META_METHODS = ("PublishReadCache", "RetireReadCache", "GetChunkSources", "OpenFileRead", "ValidateReadGrants",
                "RetainReader", "ReleaseReaders", "ReportReaders", "GetPlacementSnapshot", "ValidateReplicaWrite",
                "EnsureReplicaPlacement", "ClaimReplicationTask", "ReportReplicationTask")
META_METRICS = r'^afs_meta_rpc_requests_total\{method="[^"]*/(%s)"' % "|".join(META_METHODS)


def cluster_metrics(c):
    """One /metrics scrape of Meta and every Node of the case cluster."""
    return {role: c.health(role, "/metrics") for role in c.roles}


def cluster_metrics_delta(before, after):
    deltas = {}
    for role, start in before.items():
        pattern = META_METRICS if role == "M" else NODE_METRICS
        deltas[role] = benchlib.metrics_delta(start, after[role], pattern)
    return deltas


def require_reconciled(check):
    """Sum of payload sent by every Node, N*S and the tool's bytes must agree when the check applies.

    When it applies, send counters that are missing or were reset void the round: nothing can be reconciled.
    """
    if check["applies"] and check["metrics"] != "complete":
        raise AssertionError("read bytes cannot be reconciled, send counters %s: %r, round void" % (
            check["metrics"], check.get("metrics_issues")))
    if check.get("ok") is False:
        raise AssertionError("read bytes do not reconcile: Nodes sent %d, expected N*S=%d, tool read %d: %r" % (
            check["sent_total"], check["expected"], check["tool_bytes"], check))


def read_window(params, before, after, readers, tool_bytes):
    """Send check and reader-side local reads for one read window (two cluster_metrics scrapes)."""
    nodes = [r for r in before if r != "M"]
    check = benchlib.send_reconciliation(benchlib.counter_windows(before, after, nodes, benchlib.SEND_SERIES),
                                         readers=len(readers), file_bytes=params.file_bytes, tool_bytes=tool_bytes,
                                         applies=benchlib.single_writer_copy(params.replication))
    local = benchlib.reader_local_reads(benchlib.counter_windows(before, after, readers, benchlib.RECEIVE_SERIES),
                                        params.file_bytes)
    return check, local


def read_round_evidence(c, params, before, readers, tool_bytes, after=None):
    """Metric deltas of every role plus the sent-bytes reconciliation for one remote read window."""
    after = cluster_metrics(c) if after is None else after
    metrics = cluster_metrics_delta(before, after)
    check, local = read_window(params, before, after, readers, tool_bytes)
    return {"metrics": metrics, "writer_metrics": metrics.get("A", {}), "send_check": check, "local_read": local}


def log_path(c, role):
    return remote_join(c.root(role), role + ".log")


def log_marks(c):
    """Current size of every Node log, the start of a round's event window."""
    marks = {}
    for role in c.roles:
        if role != "M":
            value = json.loads(c.python(role, benchlib.LOG_EVENTS_SCRIPT, log_path(c, role), "0", "[]",
                                        owner=False)["stdout"])
            marks[role] = value["size"] if value else 0
    return marks


def log_event_counts(c, marks, events):
    """Lines per event each Node logged since its mark; a missing log is recorded, not skipped."""
    counts = {}
    for role, mark in marks.items():
        value = json.loads(c.python(role, benchlib.LOG_EVENTS_SCRIPT, log_path(c, role), str(mark),
                                    json.dumps(list(events)), owner=False)["stdout"])
        counts[role] = ({"counts": {}, "rotated": False, "missing": True} if value is None else
                        {"counts": value["counts"], "rotated": value["rotated"], "missing": False})
    return counts


def coverage_baseline(c, params, nodes):
    """Chunk listing before the write, only where the writer may not hold the whole file (disk backend)."""
    if params.backend == "memory" or benchlib.single_writer_copy(params.replication):
        return None
    return payload_listing(c, nodes)


def backend_state(c, params, nodes, source, baseline=None):
    """Evidence and layer name for a remote cold read.

    Disk backend: evict and judge (evict_backends). Memory backend: chunks live in tmpfs by declaration,
    so the backend cannot be cold; the layer only says the reader never opened the file.
    """
    if params.backend == "memory":
        return {"medium": "memory", "roles": list(nodes)}, "remote_cold_memory_backend"
    evidence, cold = evict_backends(c, nodes, params.file_bytes, source, baseline=baseline)
    return evidence, "remote_cold" if cold else "remote_cold_unverified"


def fio(c, role, params, path, operation, block, threads, end_fsync=False, watch=()):
    """Run fio and attach block-device byte deltas (from /proc/diskstats) seen by `role` and `watch`."""
    fio_path, _ = params.tool("fio")
    argv = benchlib.fio_argv(fio_path, path, operation, block=block, file_bytes=params.file_bytes,
                             threads=threads, end_fsync=end_fsync)
    roles = tuple(dict.fromkeys((role,) + tuple(watch)))
    before = host_snapshot(c, roles)
    output = run_tool(c, role, params, "fio", argv[1:])
    after = host_snapshot(c, roles)
    result = benchlib.parse_fio(output["stdout"], operation)
    result["disk"], result["host"] = host_evidence(before, after)
    if result["bytes"] != params.file_bytes:
        raise AssertionError("fio transferred %d bytes, expected the declared file" % result["bytes"])
    return result


def write_file(c, role, params, path, block):
    """One writer job with end_fsync: DFS files have a single writer, so writes never fan out.

    Write rows carry threads=1 (the writer) and loop_threads (the read-thread loop they precede),
    so the summary pools every 1-job write of a block size while rounds stay attributable.
    """
    return fio(c, role, params, path, "write", block, 1, end_fsync=True)


def round_record(c, case, **fields):
    entry = {"case": case, **fields}
    c.record("bench.round", entry)
    return entry


def summaries(c, rounds):
    groups = {}
    for entry in rounds:
        key = (entry["op"], entry["layer"], entry["tool"], entry["block_bytes"], entry["threads"],
               entry.get("phase", "all"))
        groups.setdefault(key, []).append(entry["result"]["mib_per_s"])
    table = []
    for (op, layer, tool, block, threads, phase), samples in sorted(groups.items()):
        table.append({"op": op, "layer": layer, "tool": tool, "block_bytes": block, "threads": threads,
                      "phase": phase, "mib_per_s": benchlib.summarize(samples)})
    params = benchlib.parameters(c.config)
    applied = params.applied_overrides([r for r in c.roles if r != "M"])
    return c.record("bench.summary", {"scope": "measurement only; performance acceptance NOT_EVALUATED",
                                      "variant": params.variant, "backend": params.backend,
                                      "node_overrides": applied,
                                      "variant_applied": bool(applied or params.replication),
                                      "replication": benchlib.replication_settings(params.replication),
                                      "arrival": params.arrival, "rows": table})


def localfs(c):
    """Upper bound: the same fio scenarios on the role's native ext4 directory."""
    params = benchlib.parameters(c.config)
    if params.backend != "disk":
        raise Blocked("bench.env.localfs is the disk upper bound; with a memory backend use hot reads instead")
    tool_identity(c, params, ("A",), ("fio",))
    directory = remote_join(c.role_dir("A"), "bench-local")
    mkdir_owned(c, "A", directory)
    rounds = []
    for block in params.block_bytes:
        for threads in params.threads:
            for number in range(params.rounds):
                path = remote_join(directory, "f-%d-%d-%d" % (block, threads, number))
                shape = dict(tool="fio", block_bytes=block, threads=threads, round=number,
                             file_bytes=params.file_bytes, target="localfs")
                result = write_file(c, "A", params, path, block)
                rounds.append(round_record(c, "localfs", op="write_fsync", layer="write", result=result,
                                           **dict(shape, threads=1, loop_threads=threads)))
                probe(c, "A", "evict", path)
                before = probe(c, "A", "resident", path)
                if before["errors"] or before["resident_bytes"] > EVICTED_MAX_FRACTION * params.file_bytes:
                    raise AssertionError("local file not evicted: %r" % before)
                result = fio(c, "A", params, path, "read", block, threads)
                rounds.append(round_record(c, "localfs", op="read", layer="cold", result=result,
                                           evidence={"resident_before": before["resident_bytes"]}, **shape))
                warm = probe(c, "A", "resident", path)
                if warm["errors"] or warm["resident_bytes"] < HOT_MIN_FRACTION * params.file_bytes:
                    raise AssertionError("local file not fully cached before hot read: %r" % warm)
                result = fio(c, "A", params, path, "read", block, threads)
                rounds.append(round_record(c, "localfs", op="read", layer="hot", result=result,
                                           evidence={"resident_before": warm["resident_bytes"]}, **shape))
                remove(c, "A", path)
    summaries(c, rounds)


def wait_visible(c, role, path, size):
    """Path-only wait (stat, never open) so the reader's page cache stays untouched."""
    return c.poll_python(role, "os.path.exists(a[0]) and os.stat(a[0]).st_size==int(a[1])", path, str(size),
                         label="bench.visible")


def dfs_sequential(c):
    """Writer A, reader B: DFS write, remote cold read, then hot re-read on the same reader."""
    params = benchlib.parameters(c.config)
    tool_identity(c, params, ("A", "B"), ("fio",))
    same_kernel = boot_id(c, "A") == boot_id(c, "B")
    nodes = [r for r in c.roles if r != "M"]
    rounds = []
    for block in params.block_bytes:
        for threads in params.threads:
            for number in range(params.rounds):
                name = "bench-%d-%d-%d" % (block, threads, number)
                shape = dict(tool="fio", block_bytes=block, threads=threads, round=number,
                             file_bytes=params.file_bytes, target="dfs")
                marks = log_marks(c)
                baseline = coverage_baseline(c, params, nodes)
                round_start = host_snapshot(c, c.roles)
                result = write_file(c, "A", params, remote_join(c.workspace("A"), name), block)
                rounds.append(round_record(c, "dfs.sequential", op="write_fsync", layer="write",
                                           result=result, **dict(shape, threads=1, loop_threads=threads)))
                reader_path = remote_join(c.workspace("B"), name)
                wait_visible(c, "B", reader_path, params.file_bytes)
                backend, layer = backend_state(c, params, nodes, "A", baseline=baseline)
                metrics_before = cluster_metrics(c)
                result = fio(c, "B", params, reader_path, "read", block, threads, watch=("A",))
                evidence = {"same_kernel": same_kernel, "backend": backend,
                            **read_round_evidence(c, params, metrics_before, ("B",), result["bytes"]),
                            "log_events": log_event_counts(c, marks, params.log_events),
                            "round_memory": round_memory(round_start, host_snapshot(c, c.roles))}
                check = evidence.get("send_check")
                local = evidence.get("local_read")
                require_reconciled(check)
                rounds.append(round_record(c, "dfs.sequential", op="read",
                                           layer=benchlib.replica_layer(layer, check, local),
                                           result=result, evidence=evidence, **shape))
                cached = benchlib.cached_verdict(probe(c, "B", "resident", reader_path), params.file_bytes)
                result = fio(c, "B", params, reader_path, "read", block, threads)
                rounds.append(round_record(c, "dfs.sequential", op="read",
                                           layer="hot" if cached["ok"] else "hot_unverified", result=result,
                                           evidence={"reader_page_cache": cached}, **shape))
                remove(c, "A", remote_join(c.workspace("A"), name))
    summaries(c, rounds)


def dfs_fanout(c):
    """Writer A; readers (bench.readers, B..E) read the whole file concurrently through elbencho services."""
    params = benchlib.parameters(c.config)
    readers = params.reader_roles
    require_separate_readers(c.config, readers)
    tool_identity(c, params, ("A",) + readers, ("fio", "elbencho"))
    # elbencho needs one path on every service host: each reader binds its own workspace onto the same
    # directory inside its worker's private mount namespace (invisible to other roles), whatever its base.
    link = remote_join("/tmp", "afs-bench-" + c.token)
    bound = []
    elbencho, env = params.tool("elbencho")
    try:
        for role in readers:
            c.python(role, "os.makedirs(sys.argv[1],0o755,exist_ok=True)", link, owner=False)
            c.command(role, ["mount", "--bind", c.workspace(role), link])
            bound.append(role)
        for role in readers:
            c.channel(role).call(
                "helper_start", name="elbencho-svc", log=remote_join(c.root(role), "helper-elbencho.log"),
                argv=[elbencho, "--service", "--foreground", "--port", str(ELBENCHO_PORT)],
                uid=c.uid, gid=c.gid, env=env,
            )
        fanout_rounds(c, params, readers, link, elbencho, env)
    finally:
        for role in bound:
            c.record("bench.fanout_unbind." + role, c.command(role, ["umount", link], check=False))
        for role in readers:
            c.python(role, "os.path.isdir(sys.argv[1]) and not os.path.ismount(sys.argv[1]) and os.rmdir(sys.argv[1])",
                     link, owner=False, check=False)


def elbencho_phase(c, params, phase, readers, link, name, reports, elbencho, block, threads):
    """One elbencho read by `readers` (each reads the whole file); returns the parsed result and raw report."""
    hosts = ["%s:%d" % (c.address(r), ELBENCHO_PORT) for r in readers]
    report = remote_join(reports, "elbencho-%s-%s.json" % (hashlib.sha256(name.encode()).hexdigest()[:8], phase))
    argv = benchlib.elbencho_read_argv(elbencho, hosts, remote_join(link, name), block=block,
                                       file_bytes=params.file_bytes, threads=threads, result=report)
    before = host_snapshot(c, ("A",) + tuple(readers))
    console = run_tool(c, "A", params, "elbencho", argv[1:])
    after = host_snapshot(c, ("A",) + tuple(readers))
    c.record("bench.elbencho_console", {"name": name, "phase": phase, "stdout": console["stdout"][-2000:],
                                        "stderr": console["stderr"][-2000:]})
    text = c.python("A", "print(open(sys.argv[1]).read())", report)["stdout"]
    result = benchlib.parse_elbencho(text, "READ")
    result["disk"], result["host"] = host_evidence(before, after)
    if result["hosts"] != len(readers) or result["bytes"] != params.file_bytes * len(readers):
        raise AssertionError("elbencho did not read the full file on every reader: %r" % result)
    return result, [json.loads(line) for line in text.splitlines() if line.strip()]


def fanout_rounds(c, params, readers, link, elbencho, env):
    """Rounds of write-then-fan-out; metrics, the sent-bytes reconciliation and log events cover the round.

    Every phase row carries its own send check and reader-side local reads (`phase_send`, `local_read`) and
    its layer from them. The followers' row identifies the backend evidence as pre-seed and records
    the seed's sent bytes in that window (`prewarm`). The last row adds the whole-round evidence: metrics,
    the judged send check, log events and the OOM window over every role's kernel (`round_memory`).
    """
    hosts = ["%s:%d" % (c.address(r), ELBENCHO_PORT) for r in readers]
    reports = remote_join(c.role_dir("A"), "bench-reports")
    mkdir_owned(c, "A", reports)
    nodes = [r for r in c.roles if r != "M"]
    same_kernel = len({boot_id(c, r) for r in ("A",) + tuple(readers)}) == 1
    phases = benchlib.arrival_phases(params.arrival, readers)
    rounds = []
    try:
        for block in params.block_bytes:
            for threads in params.threads:
                for number in range(params.rounds):
                    name = "fan-%d-%d-%d" % (block, threads, number)
                    shape = dict(block_bytes=block, threads=threads, round=number, file_bytes=params.file_bytes,
                                 target="dfs")
                    marks = log_marks(c)
                    baseline = coverage_baseline(c, params, nodes)
                    round_start = host_snapshot(c, c.roles)
                    result = write_file(c, "A", params, remote_join(c.workspace("A"), name), block)
                    rounds.append(round_record(c, "dfs.fanout", op="write_fsync", layer="write", tool="fio",
                                               result=result, **dict(shape, threads=1, loop_threads=threads,
                                                                     readers=len(readers))))
                    for role in readers:
                        wait_visible(c, role, remote_join(c.workspace(role), name), params.file_bytes)
                    backend, layer = backend_state(c, params, nodes, "A", baseline=baseline)
                    scrapes = [cluster_metrics(c)]
                    total, rows = 0, []
                    for index, (phase, group) in enumerate(phases):
                        if index:
                            time.sleep(params.arrival["gap_ms"] / 1000)
                        result, raw = elbencho_phase(c, params, phase, group, link, name, reports, elbencho,
                                                     block, threads)
                        scrapes.append(cluster_metrics(c))
                        total += result["bytes"]
                        # Each phase is labelled from its own window (seed and followers read differently).
                        check, local = read_window(params, scrapes[-2], scrapes[-1], group, result["bytes"])
                        evidence = {"same_kernel": same_kernel, "backend": backend, "readers": list(group),
                                    "phase_send": check, "local_read": local}
                        phase_layer = layer
                        if phase == "followers":
                            phase_layer = ("remote_post_seed_memory_backend" if params.backend == "memory"
                                           else "remote_post_seed")
                            evidence["backend_scope"] = "before_seed"
                            seed = params.arrival["seed"]
                            evidence["prewarm"] = {"after": "seed", "seed": seed, "gap_ms": params.arrival["gap_ms"],
                                                   "seed_sent_bytes": check["sent_by_role"].get(seed)}
                        rows.append(dict(phase=phase, result=result, evidence=evidence, raw=raw, readers=len(group),
                                         layer=benchlib.replica_layer(phase_layer, check, local)))
                    whole = rows[-1]["evidence"]
                    whole.update(read_round_evidence(c, params, scrapes[0], readers, total, after=scrapes[-1]),
                                 phase_send=whole["phase_send"], local_read=whole["local_read"], scope="whole_round",
                                 log_events=log_event_counts(c, marks, params.log_events),
                                 round_memory=round_memory(round_start, host_snapshot(c, c.roles)))
                    require_reconciled(whole["send_check"])
                    for row in rows:
                        rounds.append(round_record(c, "dfs.fanout", op="read", tool="elbencho",
                                                   arrival=params.arrival, **row, **shape))
                    remove(c, "A", remote_join(c.workspace("A"), name))
    finally:
        c.record("bench.elbencho_quit", c.command("A", [elbencho, "--hosts", ",".join(hosts), "--quit"],
                                                   env=env, check=False, timeout=60))
        for role in readers:
            c.record("bench.elbencho_service." + role,
                     c.channel(role).call("stop", role="H:elbencho-svc", timeout=30))
    summaries(c, rounds)


LISTENING = (
    "port='%04X'%int(sys.argv[1])\n"
    "def up():\n"
    " for t in ('/proc/net/tcp','/proc/net/tcp6'):\n"
    "  for l in open(t).read().splitlines()[1:]:\n"
    "   f=l.split()\n"
    "   if f[1].endswith(':'+port) and f[3]=='0A':return True\n"
    " return False\n"
)


def network(c):
    """Environment ceiling between the writer and a reader host, measured in the same run.

    Each round starts a one-off iperf3 server (`-1`) that exits normally after one test.
    """
    params = benchlib.parameters(c.config)
    tool_identity(c, params, ("A", "B"), ("iperf3",))
    iperf3, env = params.tool("iperf3")
    same_kernel = boot_id(c, "A") == boot_id(c, "B")
    samples = []
    for streams in params.threads:
        for number in range(params.rounds):
            name = "iperf3-p%d-r%d" % (streams, number)
            c.channel("B").call("helper_start", name=name, log=remote_join(c.root("B"), "helper-%s.log" % name),
                                argv=[iperf3, "-s", "-1", "-p", str(IPERF3_PORT)], uid=c.uid, gid=c.gid, env=env)
            c.python("B", "import time\n" + LISTENING + "d=time.monotonic()+10\n"
                     "while not up() and time.monotonic()<d:time.sleep(0.05)\nassert up()", str(IPERF3_PORT),
                     owner=False)
            before = host_snapshot(c, ("A", "B"))
            output = run_tool(c, "A", params, "iperf3", ["-c", c.address("B"), "-p", str(IPERF3_PORT),
                                                         "-t", "10", "-P", str(streams), "-J"], timeout=120)
            after = host_snapshot(c, ("A", "B"))
            c.record("bench.iperf3_server." + name, c.channel("B").call("wait", role="H:" + name, timeout=30))
            result = benchlib.parse_iperf3(output["stdout"])
            _, result["host"] = host_evidence(before, after)
            samples.append(round_record(c, "env.network", op="tcp", layer="network", tool="iperf3",
                                        block_bytes=0, threads=streams, round=number, result=result,
                                        evidence={"same_kernel": same_kernel}))
    summaries(c, samples)


CASES = (
    Case("bench.env.localfs", ("bench-env",), "dfs", (), localfs, purpose="performance"),
    Case("bench.env.network", ("bench-env",), "dfs", (), network, purpose="performance"),
    Case("bench.dfs.sequential", ("bench-dfs",), "dfs", (), dfs_sequential, purpose="performance"),
    Case("bench.dfs.fanout.many_readers", ("bench-dfs",), "dfs", (), dfs_fanout, purpose="performance"),
)
