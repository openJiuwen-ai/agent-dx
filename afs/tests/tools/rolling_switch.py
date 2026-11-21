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
MIDWAY_FINAL_NAME = "rollout-acked-before-midway-rollback"
FINAL_BODY = b"committed after version switch\n" * 4096
ORDER = ("M", "B", "A")


def switch_role(cluster, role, inputs, identity, *, reopen, while_stopped=None):
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
    if while_stopped is not None:
        while_stopped()
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


def exercise_switch(cluster, old_inputs, new_inputs, old, new, phases, policy, *, reopen=False, live_upgrade=False):
    midway = policy in ("midway_preserve", "midway_ownerfs")
    preserve = policy in ("preserve", "midway_preserve")
    dfs_probe = None
    ownerfs_probe = None
    if live_upgrade and policy != "snapshot":
        if cluster.mode == "dfs":
            from tools import rolling_dfs_recovery

            dfs_probe = rolling_dfs_recovery
        else:
            from tools import rolling_ownerfs_mixed

            ownerfs_probe = rolling_ownerfs_mixed
    cluster.write("A", "seed", offline.SEED)
    wait_read(cluster, "B", "seed", offline.SEED)
    expected = offline.file_state(cluster)
    identity = offline.owner_identity(cluster) if cluster.mode == "ownerfs" else None
    if dfs_probe is None:
        offline.quiesce(cluster)
        if not preserve:
            offline.recovery_point(cluster)
        offline.start_all(cluster, old)
    # The live DFS lane starts from the still-serving old sessions. It retains
    # its local data on rollback, so it needs no stopped pre-upgrade snapshot.
    dfs_state = dfs_probe.seed(cluster) if dfs_probe is not None else None
    acked = {}
    dfs_acked = {}
    ownerfs_settings = None

    def stopped_b():
        nonlocal ownerfs_settings
        if dfs_probe is not None:
            dfs_acked.update(dfs_probe.write_while_offline(cluster, dfs_state))
        elif ownerfs_probe is not None:
            ownerfs_settings = ownerfs_probe.prepare_adoption(cluster)

    def roll():
        for index, role in enumerate(ORDER):
            hook = stopped_b if role == "B" and (dfs_probe is not None or ownerfs_probe is not None) else None
            acked.update(switch_role(cluster, role, new_inputs, new, reopen=reopen, while_stopped=hook))
            done = ORDER[:index + 1]
            cluster.record("rollout.checkpoint." + role,
                           {r: new["commit"] if r in done else old["commit"] for r in cluster.roles})
            if role == "B" and dfs_probe is not None:
                dfs_probe.verify_recovered(cluster, dfs_state, {r: new if r in done else old for r in cluster.roles})
                cluster.record("rollout.dfs_recovered_replica", "passed")
            if role == "B" and ownerfs_probe is not None:
                ownerfs_probe.check_mixed_workspace(cluster, ownerfs_settings)
                cluster.record("rollout.ownerfs_mixed_workspace", "passed")
            if policy == "snapshot":
                break
            if midway and role == "B":
                break
        final_name = SNAPSHOT_FINAL_NAME if policy == "snapshot" else FINAL_NAME
        if midway:
            final_name = MIDWAY_FINAL_NAME
            for role in cluster.roles:
                offline.running_identity(cluster, role, old if role == "A" else new)
        cluster.write("A", final_name, FINAL_BODY)
        acked[final_name] = FINAL_BODY
        verify_acked(cluster, {**acked, **dfs_acked}, "before-rollback")
        if midway:
            cluster.record("rollout.midway_abort", {
                "versions": {"M": new["commit"], "A": old["commit"], "B": new["commit"]},
                "confirmed_files": sorted({**acked, **dfs_acked}),
            })

    phase = "interrupt_after_meta" if policy == "snapshot" else "complete_rolling"
    if midway:
        phase = "interrupt_after_meta_and_b"
    offline.run_phase(phases, phase, roll)

    def rollback(restore_snapshot):
        for helper, (peer, _) in list(cluster.helpers.items()):
            stopped = cluster.channel(peer).call("stop", role="H:" + helper, timeout=35)
            cluster.record("helper.stop." + helper, stopped)
            del cluster.helpers[helper]
        offline.quiesce(cluster)
        if ownerfs_settings is not None:
            ownerfs_probe.restore_settings(cluster, ownerfs_settings)
        if restore_snapshot:
            offline.recovery_point(cluster, restore=True)
        offline.install(cluster, old_inputs, old, roles=("M", "B") if midway else None)
        if midway:
            cluster.record("rollout.midway_rollback_policy", {
                "restore_snapshot": restore_snapshot, "replaced_binaries": ["M", "B"],
            })
        offline.start_all(cluster, old)
        offline.check_seed(cluster, expected, identity)
        if restore_snapshot:
            for name in acked:
                assert_absent(cluster, name)
            if ownerfs_settings is not None:
                ownerfs_probe.check_snapshot_restored(cluster, ownerfs_settings)
        else:
            verify_acked(cluster, {**acked, **dfs_acked}, "rollback-preserve")
            if policy == "midway_ownerfs":
                from tools import ownerfs_midway_downgrade

                ownerfs_midway_downgrade.check_retained_workspace(cluster, ownerfs_settings)
        cluster.write("A", "rollout-post-rollback", offline.SEED)
        wait_read(cluster, "B", "rollout-post-rollback", offline.SEED)

    if policy == "midway_ownerfs":
        from tools import ownerfs_midway_downgrade

        verdict = None

        def attempt_retained_downgrade():
            nonlocal verdict
            verdict = ownerfs_midway_downgrade.attempt_retained_downgrade(cluster, old_inputs, old, ownerfs_settings)
            if verdict not in ("safe_takeover", "incompatible_rejected"):
                raise AssertionError(f"unrecognized retained rollback outcome: {verdict}")

        if phases[phase]["status"] == "passed":
            offline.run_phase(phases, "rollback_retained_probe", attempt_retained_downgrade)
        else:
            phases["rollback_retained_probe"] = {"status": "not_run", "error": "mixed-version checkpoint did not pass"}
        preserve = verdict == "safe_takeover"
    elif preserve and phases[phase]["status"] != "passed":
        phases["rollback_preserve"] = {"status": "not_run", "error": "rolling upgrade did not pass"}
        return
    offline.run_phase(phases, "rollback_preserve" if preserve else "rollback_snapshot", lambda: rollback(not preserve))


def cases(*, reopen=False, mode=None, live_upgrade=False, midway_rollback=False):
    lane = "rolling_ack" if reopen else "rolling"
    if live_upgrade:
        lane = "rolling_e2e"
    modes = ("ownerfs", "dfs") if mode is None else (mode,)
    for selected in modes:
        completed_policy = "full_snapshot" if live_upgrade and selected == "ownerfs" else "preserve"
        policies = ("snapshot", completed_policy)
        if midway_rollback:
            policies = ("midway_preserve" if selected == "dfs" else "midway_ownerfs",)
        for policy in policies:
            profile = None
            if selected == "dfs":
                replica_failure = "degrade" if live_upgrade and policy != "snapshot" else "strict"
                profile = DfsProfile(copies=2, replica_failure=replica_failure)
            case_id = f"{selected}.version.{policy}" if midway_rollback else f"{selected}.version.{lane}_{policy}"
            case = Case(case_id, (), selected, (), None, profile=profile)
            yield case, policy
