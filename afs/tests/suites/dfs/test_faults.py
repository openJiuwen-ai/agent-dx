"""DFS fault scenarios (suite local-fault): kills, stops, wiped disks, corrupted replicas, a full disk, lost or
stalled S3 deleters and Meta log backends, each on its own short-lived cluster.

Every case stops the suite cluster's products and runs ``faults/harness.sh`` on the single host with one phase
script from ``faults/``: the harness starts one Meta and up to two DFS Nodes on 127.0.0.1 (ports 7400-7521),
the phase injects the fault and exits non-zero when its acceptance fails. Evidence (phase output, product logs,
ERROR-level lines) is recorded in checks.json and fault-<case>.tgz of the case.

Scope: one Linux host, root (FUSE, drop_caches, tmpfs). Some cases need tools named in the environment file:
``"fault_tools": {"minio": ..., "redis_server": ..., "redis_cli": ..., "etcd": ...}``; a missing tool is
BLOCKED. Cases run up to 25 minutes: use ``--case-timeout 1800``.
"""

import base64
import io
import tarfile
from pathlib import Path

from fixtures.common import Blocked, Case, is_single_host

FAULTS = Path(__file__).resolve().parents[2] / "faults"
SUITE = ("local-fault",)


def _tools(c, names):
    tools = c.config.get("fault_tools") or {}
    env = {}
    for name in names:
        value = tools.get(name)
        if not value:
            raise Blocked(f"local-fault needs fault_tools.{name} in the environment file")
        env[name.upper()] = value
    return env


def run_fault(c, label, phase, *, mode="r2", env=None, tools=(), timeout=900, tmpfs_mib=None):
    if not is_single_host(c.config):
        raise Blocked("local-fault runs every product on one host")
    # The suite cluster is not used: free its mounts and ports first.
    for role in reversed(c.roles):
        c.stop(role)
    base = c.root("M")
    faults = base + "/faults"
    for path in sorted(FAULTS.iterdir()):
        if path.is_file():
            c.channel("M").upload(path, faults + "/" + path.name, 0o755)
    # The suite uploads only the product binaries; the SDK client phases (R76, R18) need the bench tool.
    bench = Path(c.binary_dir) / "afs-bench"
    if not bench.is_file():
        raise Blocked("local-fault needs afs-bench in the binary directory")
    c.channel("M").upload(bench, base + "/bin/afs-bench", 0o755)
    # Short working root: Unix socket paths under it must stay below SUN_LEN (108 bytes).
    host = c.config["roles"]["M"]["host"]
    root = c.config["hosts"][host]["base"].rstrip("/") + "/f-" + base.rsplit("/", 1)[-1][:8]
    out = base + "/fault-out"
    c.command("M", ["bash", "-c", '[ ! -e "$1" ]', "-", root])
    values = {"BIN": base + "/bin", "OUT": out, "ROOT": root, "PHASES": faults + "/" + phase,
              "MODE": mode, "FAULTS": faults}
    values.update(_tools(c, tools))
    values.update(env or {})
    harness = f'bash {faults}/harness.sh'
    if tmpfs_mib:
        # The working directory lives on a small tmpfs so the disk really fills.
        values["ROOT"] = root + "/full/root"
        harness = (f'mkdir -p {root}/full && mount -t tmpfs -o size={tmpfs_mib}m tmpfs {root}/full && '
                   f'{{ {harness}; rc=$?; umount {root}/full; exit $rc; }}')
    try:
        result = c.command("M", ["bash", "-c", harness], env=values, timeout=timeout, check=False)
    finally:
        c.command(
            "M", ["bash", "-c", 'findmnt -rn -o TARGET | grep -F "$1/" | sort -r | xargs -r -n1 umount; rm -rf "$1"',
                  "-", root], timeout=120, check=False,
        )
    summary = c.command(
        "M",
        ["bash", "-c", 'cd "$1" && for f in *.log; do [ -f "$f" ] && printf "%s %s\\n" "$f" '
         '"$(grep -c \'"level":"ERRO"\' "$f")"; done; echo ---; grep -h \'"level":"ERRO"\' *.log | head -40',
         "-", out],
        check=False,
    )
    archive = c.command(
        "M", ["bash", "-c", 'tar czf - -C "$1" . 2>/dev/null | base64 -w0', "-", out], timeout=120, check=False
    )
    if archive["returncode"] == 0 and archive["stdout"]:
        (c.output / f"fault-{label}.tgz").write_bytes(base64.b64decode(archive["stdout"]))
    phases = _member(c.output / f"fault-{label}.tgz", "phases.txt")
    counts, _, lines = summary["stdout"].partition("---\n")
    c.record("fault." + label, {
        "returncode": result["returncode"],
        "stderr": result["stderr"][-2000:],
        "phases_tail": phases[-6000:],
        "error_lines_per_log": dict(line.split() for line in counts.splitlines() if line.strip()),
        "error_lines": lines.splitlines(),
    })
    assert result["returncode"] == 0, f"{label}: harness rc={result['returncode']}\n{phases[-3000:]}"
    return phases


def _member(path, name):
    if not path.exists() or path.stat().st_size == 0:
        return ""
    with tarfile.open(path) as archive:
        try:
            member = archive.extractfile("./" + name)
        except KeyError:
            return ""
        return io.TextIOWrapper(member, errors="replace").read() if member else ""


def scenario(label, phase, **kwargs):
    def function(c):
        run_fault(c, label, phase, **kwargs)

    function.__name__ = "fault_" + label.replace("-", "_")
    return function


def require(text, *needles):
    for needle in needles:
        assert needle in text, f"missing {needle!r}"


def scrub(c):
    run_fault(c, "scrub", "phase-scrub.sh", env={"COPIES_R2": "2", "NODE_EXTRA": (
        "dfs_scrub_start_delay_seconds = 5\\ndfs_scrub_round_interval_seconds = 20\\n")})


def s3_deleter_stuck(c):
    phases = run_fault(c, "s3-deleter-stuck", "phase-s3-deleter.sh", mode="s3", tools=("minio",), timeout=1500,
                       env={"S3_TWO": "1", "FREEZE_PROXY": "1", "NODE_A_S3_PORT": "9100",
                            "S49_VARIANT": "stuck", "S49_LIMIT": "600"})
    require(phases, "[keep readable]")


def s3_deleter_lost(c):
    phases = run_fault(c, "s3-deleter-lost", "phase-s3-deleter.sh", mode="s3", tools=("minio",), timeout=1500,
                       env={"S3_TWO": "1", "S49_VARIANT": "lost"})
    require(phases, "[keep readable]")


def meta_backend(kind):
    tools = ("redis_server", "redis_cli") if kind == "redis" else ("etcd",)

    def function(c):
        run_fault(c, "meta-" + kind, "phase-meta-backend.sh", mode="native", tools=tools,
                  env={"META_BACKEND": kind})

    function.__name__ = "fault_meta_" + kind
    return function


def _case(name, groups, function):
    return Case("dfs.fault." + name, SUITE, "dfs", groups, function)


CASES = (
    # S2: GC with a lost participant, S3 reclaim (R05), a bad replica reported by its reader (R59).
    _case("gc_lost_participant", ("LF01",), scenario("gc-lost", "phase-gc-lost.sh", env={"COPIES_R2": "2"})),
    _case("s3_reclaim", ("LF02",), scenario("s3-reclaim", "phase-s3-reclaim.sh", mode="s3", tools=("minio",))),
    _case("bad_replica_reported", ("LF03",), scenario(
        "bad-replica", "phase-bad-replica.sh",
        env={"COPIES_R2": "1", "NODE_EXTRA": 'dfs_read_crc_mode = "receiver"\\n'})),
    # S3a: repair after a wiped Node and onto a third Node (R61).
    _case("repair_wiped_node", ("LF04",), scenario("repair-wipe", "phase-repair-wipe.sh", env={"COPIES_R2": "2"})),
    _case("repair_third_node", ("LF05",), scenario("repair-third", "phase-repair-third.sh", env={"COPIES_R2": "2"})),
    # S4a: bad replica never delivered (S4-0), R20 POSIX corners, R76 admission, R75 long reads, R72 big
    # directories, a full disk (R71a), rename right after close (R83).
    _case("bad_replica_never_delivered", ("LF06",), scenario(
        "bad-replica-read", "phase-bad-replica-read.sh", env={"COPIES_R2": "2"})),
    _case("posix_rename_append", ("LF07",), scenario("r20-posix", "phase-r20.sh", env={"COPIES_R2": "1"})),
    _case("front_write_admission", ("LF08",), scenario("front-write", "phase-front-write.sh", mode="native")),
    _case("long_direct_reads", ("LF09",), scenario("long-read", "phase-long-read.sh", mode="native")),
    _case("big_directory_listing", ("LF10",), scenario(
        "big-dir", "phase-big-dir.sh", mode="native", timeout=1200)),
    _case("disk_full_enospc", ("LF11",), scenario(
        "disk-full", "phase-diskfull.sh", mode="native", tmpfs_mib=300)),
    _case("rename_after_close", ("LF12",), scenario("rename-race", "phase-rename-race.sh", mode="native")),
    # S4b: fresh owner read (R82), restart during patrol (Q08), degraded writes with a killed or hung replica
    # (R12), SDK client crash (R18), lost or stuck S3 deleter (S4-9), latent bad replica (S4-10), pinned
    # reader under GC (R81).
    _case("fresh_owner_read", ("LF13",), scenario("owner-read", "phase-owner-read.sh", env={"COPIES_R2": "1"})),
    _case("restart_during_patrol", ("LF14",), scenario(
        "q08-wipe", "phase-q08-wipe.sh", env={"COPIES_R2": "2"}, timeout=1200)),
    _case("degraded_write_killed_replica", ("LF15",), scenario(
        "degrade-kill", "phase-r12-degrade.sh", env={"COPIES_R2": "2"}, timeout=1200)),
    _case("degraded_write_hung_replica", ("LF16",), scenario(
        "degrade-stop", "phase-r12-degrade.sh", env={"COPIES_R2": "2", "R12_FAULT": "stop"}, timeout=1200)),
    _case("sdk_client_crash", ("LF17",), scenario("sdk-crash", "phase-r18-crash.sh", mode="native")),
    _case("s3_deleter_lost", ("LF18",), s3_deleter_lost),
    _case("s3_deleter_stuck", ("LF19",), s3_deleter_stuck),
    _case("latent_bad_replica", ("LF20",), scenario(
        "latent-replica", "phase-latent-replica.sh", env={"COPIES_R2": "2"})),
    _case("pinned_reader_under_gc", ("LF21",), scenario(
        "pinned-reader", "phase-pinned-reader.sh", env={"COPIES_R2": "1"})),
    # S4c: background scrub (S4-16), Meta on Redis / etcd killed, fenced and backend lost (R16).
    _case("scrub_restores_bad_replica", ("LF22",), scrub),
    _case("meta_backend_redis", ("LF23",), meta_backend("redis")),
    _case("meta_backend_etcd", ("LF24",), meta_backend("etcd")),
    # R94: a reader held across this Node's commit and another Node's keeps the version it moved to under GC.
    _case("local_commit_reader_under_gc", ("LF25",), scenario(
        "local-commit-reader", "phase-local-commit-reader.sh", env={"COPIES_R2": "1"})),
    # R94 recheck: the same reader while node-a's write lease lapses with its writer still open.
    _case("lapsed_lease_reader_under_gc", ("LF26",), scenario(
        "lapsed-lease-reader", "phase-lapsed-lease-reader.sh", env={"COPIES_R2": "1"})),
    _case("r84_strict_upload_failure", ("LF27",), scenario(
        "r84-strict", "phase-r84-upload-failure.sh",
        env={"R84_FAULT": "strict", "META_EXTRA": 'dfs_replica_failure_policy = "strict"'})),
    _case("r84_local_upload_failure", ("LF28",), scenario(
        "r84-local", "phase-r84-upload-failure.sh", mode="native",
        env={"R84_FAULT": "diskfull"})),
    # Round 2 C-1: in-memory reader registrations. E1 open/rewrite race under GC; E2 a reader whose session
    # lapses (stopped Node), E4 the same through a Meta partition; E3 a reader across a Meta restart; an
    # unlinked file reopened through /proc/self/fd stays pinned durably.
    _case("soft_reader_race", ("LF29",), scenario(
        "soft-reader-race", "phase-soft-reader-race.sh",
        env={"COPIES_R2": "1", "META_EXTRA": "dfs_gc_grace_ms = 0", "AFS_DIAG_READER_REGISTER_DELAY_MS": "20"})),
    _case("reader_lease_lapse", ("LF30",), scenario(
        "reader-lease-lapse", "phase-reader-lease-lapse.sh", env={"COPIES_R2": "1"})),
    _case("meta_restart_reader", ("LF31",), scenario(
        "meta-restart-reader", "phase-meta-restart-reader.sh", env={"COPIES_R2": "1"})),
    _case("reader_partition", ("LF32",), scenario(
        "reader-partition", "phase-reader-lease-lapse.sh", env={"COPIES_R2": "1", "READER_LAPSE": "partition"})),
    _case("unlinked_reopen_reader", ("LF33",), scenario(
        "unlinked-reopen", "phase-unlinked-reopen.sh", mode="native")),
)
