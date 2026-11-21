"""Exercise role-by-role process switches and two explicit rollback policies."""

from hashlib import sha256
from pathlib import Path
from posixpath import join as remote_join

from fixtures.common import Case, DfsProfile
from suites.operations import wait_read
from tools import version_switch as offline

PROBE = Path(__file__).resolve().parents[1] / "suites" / "cluster" / "rolling_probe.py"
FINAL_NAME = "rollout-acked-after-upgrade"
SNAPSHOT_FINAL_NAME = "rollout-acked-after-interruption"
FINAL_BODY = b"committed after version switch\n" * 4096
ORDER = ("M", "B", "A")


def switch_role(cluster, role, inputs, identity, *, reopen):
    peer = "A" if role == "B" else "B"
    suffix = role.lower()
    helper = "rollout-" + suffix
    name = "rollout-during-" + suffix
    payload = ("acknowledged while switching " + role + "\n").encode()
    digest = sha256(offline.SEED).hexdigest()
    cluster.spawn_python(peer, helper, PROBE.read_text(),
                         remote_join(cluster.workspace(peer), "seed"),
                         name, len(offline.SEED), digest, payload.hex(), "reopen" if reopen else "held")
    opened = cluster.await_helper_signal(helper, "opened", timeout=30)
    if not opened["sample"].get("valid"):
        raise AssertionError(f"{role}: pre-switch read has wrong data: {opened}")
    cluster.stop(role)
    cluster.helper_signal(helper, "probe", {})
    issued = cluster.await_helper_signal(helper, "issued", timeout=10)
    if issued["fd"] != opened["fd"]:
        raise AssertionError(f"{role}: peer closed or reopened held FD during switch")
    offline.install(cluster, inputs, identity, roles=(role,))
    offline.start_role(cluster, role, identity)
    cluster.helper_signal(helper, "resume", {})
    during = cluster.await_helper_signal(helper, "during", timeout=65)
    recovered = cluster.await_helper_signal(helper, "recovered", timeout=40)
    cluster.wait_helper(helper)
    outage_read = during["read"]
    if "errno" not in outage_read and not outage_read["valid"]:
        raise AssertionError(f"{role}: read returned corrupt data during switch: {outage_read}")
    if recovered["pid"] != opened["pid"] or recovered["fd"] != opened["fd"]:
        raise AssertionError(f"{role}: post-switch read used a different process or FD: {recovered}")
    if not recovered["sample"].get("valid") or recovered["close_errno"] is not None:
        reader = "fresh reopen" if reopen else "held FD"
        raise AssertionError(f"{role}: {reader} did not recover after switch: {recovered}")
    cluster.topology()
    if during["write"]["acknowledged"]:
        return {name: payload}
    return {}


def verify_acked(cluster, files, phase):
    for name, body in files.items():
        try:
            wait_read(cluster, "B", name, body)
        except AssertionError as error:
            raise AssertionError(f"{phase}: acknowledged file unreadable or changed: {name}") from error
        cluster.record("rollout.ack." + phase + "." + name,
                       {"bytes": len(body), "sha256": sha256(body).hexdigest()})


def assert_absent(cluster, name):
    result = cluster.python(
        "B",
        "try:open(sys.argv[1],'rb').close()\n"
        "except OSError as exc:assert exc.errno==errno.ENOENT,exc\n"
        "else:raise AssertionError('post-snapshot acknowledged file survived restore')",
        remote_join(cluster.workspace("B"), name),
    )
    cluster.record("rollout.snapshot_absent." + name, result["returncode"] == 0)


def exercise_switch(cluster, old_inputs, new_inputs, old, new, phases, policy, *, reopen=False):
    cluster.write("A", "seed", offline.SEED)
    wait_read(cluster, "B", "seed", offline.SEED)
    expected = offline.file_state(cluster)
    identity = offline.owner_identity(cluster) if cluster.mode == "ownerfs" else None
    offline.quiesce(cluster)
    if policy == "snapshot":
        offline.recovery_point(cluster)
    offline.start_all(cluster, old)
    acked = {}

    def roll():
        for index, role in enumerate(ORDER):
            acked.update(switch_role(cluster, role, new_inputs, new, reopen=reopen))
            done = ORDER[:index + 1]
            cluster.record("rollout.checkpoint." + role,
                           {r: new["commit"] if r in done else old["commit"] for r in cluster.roles})
            if policy == "snapshot":
                break
        final_name = SNAPSHOT_FINAL_NAME if policy == "snapshot" else FINAL_NAME
        cluster.write("A", final_name, FINAL_BODY)
        acked[final_name] = FINAL_BODY
        verify_acked(cluster, acked, "before-rollback")

    phase = "interrupt_after_meta" if policy == "snapshot" else "complete_rolling"
    offline.run_phase(phases, phase, roll)

    def rollback():
        for helper, (peer, _) in list(cluster.helpers.items()):
            stopped = cluster.channel(peer).call("stop", role="H:" + helper, timeout=35)
            cluster.record("helper.stop." + helper, stopped)
            del cluster.helpers[helper]
        offline.quiesce(cluster)
        if policy == "snapshot":
            offline.recovery_point(cluster, restore=True)
        offline.install(cluster, old_inputs, old)
        offline.start_all(cluster, old)
        offline.check_seed(cluster, expected, identity)
        if policy == "snapshot":
            for name in acked:
                assert_absent(cluster, name)
        else:
            verify_acked(cluster, acked, "rollback-preserve")
        cluster.write("A", "rollout-post-rollback", offline.SEED)
        wait_read(cluster, "B", "rollout-post-rollback", offline.SEED)

    if policy == "preserve" and phases[phase]["status"] != "passed":
        phases["rollback_preserve"] = {"status": "not_run", "error": "rolling upgrade did not pass"}
        return
    offline.run_phase(phases, "rollback_" + policy, rollback)


def cases(*, reopen=False, mode=None):
    lane = "rolling_ack" if reopen else "rolling"
    modes = ("ownerfs", "dfs") if mode is None else (mode,)
    for selected in modes:
        profile = DfsProfile(copies=2, replica_failure="strict") if selected == "dfs" else None
        for policy in ("snapshot", "preserve"):
            case = Case(f"{selected}.version.{lane}_{policy}", (), selected, (), None, profile=profile)
            yield case, policy
