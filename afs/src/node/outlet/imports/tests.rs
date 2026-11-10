//! `imports` 的决策逻辑测试。
//!
//! 全部脱离内核：动作由 [`plan_imports`] 从状态算出，所以"该做什么"可以在
//! 没有 NFS 服务端的机器上验证。真实 `mount(2)` 只在端到端用例里跑。

use super::*;
use std::time::Duration;

fn homes(names: &[&str]) -> BTreeSet<String> {
    names.iter().map(|name| (*name).to_owned()).collect()
}

#[test]
fn new_home_in_snapshot_produces_one_mount() {
    let table = ImportTable::new();
    let actions = plan_imports(&homes(&["node-a"]), "node-b", &table, Instant::now());
    assert_eq!(
        actions,
        vec![ImportAction::Mount {
            kind: MountKind::Remote,
            home: "node-a".into()
        }]
    );
}

#[test]
fn settled_home_produces_no_action() {
    let mut table = ImportTable::new();
    table.entry("node-a").confirm_mounted();
    let actions = plan_imports(&homes(&["node-a"]), "node-b", &table, Instant::now());
    assert!(actions.is_empty(), "a mounted home must stay untouched");
}

/// 一个 home 有 100 个 root 时快照里会出现 100 次；去重后只能有一个动作，
/// 否则每轮会对同一挂载点并发 mount 100 次。
#[test]
fn duplicate_roots_for_one_home_collapse_to_one_action() {
    let table = ImportTable::new();
    let actions = plan_imports(
        &homes(&["node-a", "node-a", "node-a"]),
        "node-b",
        &table,
        Instant::now(),
    );
    assert_eq!(actions.len(), 1);
}

/// 本机 home 也要挂，但方式不同：bind 而非 NFS。
///
/// `/public/<name>` 的 target 不区分 home 是否本机，本机若跳过挂载，同一台
/// 机器访问自己托管的 workspace 就是断链——消费方要求本机与跨机看到同一个
/// 目录，所以这里必须是"挂，但是 bind"。
#[test]
fn local_home_is_mounted_by_bind() {
    let table = ImportTable::new();
    let actions = plan_imports(&homes(&["node-b"]), "node-b", &table, Instant::now());
    assert_eq!(
        actions,
        vec![ImportAction::Mount {
            home: "node-b".into(),
            kind: MountKind::Local,
        }]
    );
}

/// 同一轮里本机与对端的 kind 必须区分开。
#[test]
fn local_and_remote_kinds_are_distinguished() {
    let table = ImportTable::new();
    let actions = plan_imports(
        &homes(&["node-a", "node-b"]),
        "node-b",
        &table,
        Instant::now(),
    );
    let local = actions
        .iter()
        .find(|action| action.home() == "node-b")
        .map(ImportAction::kind);
    let remote = actions
        .iter()
        .find(|action| action.home() == "node-a")
        .map(ImportAction::kind);
    assert_eq!(local, Some(MountKind::Local));
    assert_eq!(remote, Some(MountKind::Remote));
}

/// 冷却期内不产生动作，且不读文件系统——`plan_imports` 是纯函数，这里通过
/// 时间推进来验证冷却边界。
#[test]
fn cooling_home_produces_no_action() {
    let mut table = ImportTable::new();
    let now = Instant::now();
    let delay = table.entry("node-a").record_failure(now);
    assert_eq!(delay, Duration::from_secs(1));

    assert!(
        plan_imports(&homes(&["node-a"]), "node-b", &table, now).is_empty(),
        "must not retry inside the backoff window"
    );
    // 窗口过后重新尝试。
    let later = now + delay + Duration::from_millis(1);
    assert_eq!(
        plan_imports(&homes(&["node-a"]), "node-b", &table, later),
        vec![ImportAction::Mount {
            kind: MountKind::Remote,
            home: "node-a".into()
        }]
    );
}

/// 退避序列 1s→2s→…→60s 封顶，成功后归零。
#[test]
fn backoff_doubles_to_the_cap_and_resets_on_success() {
    let expected = [1, 2, 4, 8, 16, 32, 60, 60, 60];
    for (index, seconds) in expected.iter().enumerate() {
        let failures = u32::try_from(index + 1).unwrap();
        assert_eq!(
            backoff_for(failures),
            Duration::from_secs(*seconds),
            "failure #{failures} must wait {seconds}s"
        );
    }
    assert_eq!(backoff_for(0), Duration::ZERO);

    let mut state = ImportState::default();
    state.record_failure(Instant::now());
    state.record_failure(Instant::now());
    assert_eq!(state.consecutive_failures(), 2);
    state.confirm_mounted();
    assert_eq!(
        state.consecutive_failures(),
        0,
        "success clears the counter"
    );
    assert_eq!(state.phase(), Phase::Ready);
}

/// 一个 home 失败不能影响其他 home 产生动作。
#[test]
fn one_failing_home_does_not_block_the_others() {
    let mut table = ImportTable::new();
    let now = Instant::now();
    table.entry("node-a").record_failure(now);

    let actions = plan_imports(&homes(&["node-a", "node-c"]), "node-b", &table, now);
    assert_eq!(
        actions,
        vec![ImportAction::Mount {
            kind: MountKind::Remote,
            home: "node-c".into()
        }],
        "node-a is cooling down, node-c must still be mounted"
    );
}

/// mountinfo 说已消失：内存里的 Ready 必须回到 NeedsMount，否则再也不会重挂。
#[test]
fn observed_absence_downgrades_ready_to_needs_mount() {
    let mut table = ImportTable::new();
    table.entry("node-a").confirm_mounted();
    assert_eq!(table.entry("node-a").phase(), Phase::Ready);

    let now = Instant::now();
    table.entry("node-a").observe_absent();
    assert_eq!(table.entry("node-a").phase(), Phase::NeedsMount);
    assert_eq!(
        plan_imports(&homes(&["node-a"]), "node-b", &table, now),
        vec![ImportAction::Mount {
            kind: MountKind::Remote,
            home: "node-a".into()
        }]
    );
}

/// 进程重启但机器未重启：mountinfo 说挂着，内存一片空白，必须认下来而不是重复挂。
#[test]
fn observed_mount_upgrades_needs_mount_to_ready() {
    let mut table = ImportTable::new();
    assert_eq!(table.entry("node-a").phase(), Phase::NeedsMount);

    table.entry("node-a").observe_mounted();
    assert_eq!(table.entry("node-a").phase(), Phase::Ready);
    assert!(
        plan_imports(&homes(&["node-a"]), "node-b", &table, Instant::now()).is_empty(),
        "an already-mounted import must not be mounted again"
    );
}

/// Remount 阶段不能被 mountinfo 收编成 Ready：条目还在但访问失败过。
#[test]
fn suspect_mount_is_not_cleared_by_mountinfo_alone() {
    let mut table = ImportTable::new();
    table.entry("node-a").confirm_mounted();
    table.entry("node-a").observe_access_failure();
    assert_eq!(table.entry("node-a").phase(), Phase::Remount);

    table.entry("node-a").observe_mounted();
    assert_eq!(
        table.entry("node-a").phase(),
        Phase::Remount,
        "a stale entry still needs rebuilding"
    );
    assert_eq!(
        plan_imports(&homes(&["node-a"]), "node-b", &table, Instant::now()),
        vec![ImportAction::Remount {
            kind: MountKind::Remote,
            home: "node-a".into()
        }]
    );
}

/// 动作顺序稳定：Remount 排在 Mount 之前，同阶段按名字排序。
#[test]
fn actions_are_ordered_deterministically() {
    let mut table = ImportTable::new();
    table.entry("node-b").confirm_mounted();
    table.entry("node-b").observe_access_failure();

    let now = Instant::now();
    let actions = plan_imports(
        &homes(&["node-a", "node-b", "node-c"]),
        "node-x",
        &table,
        now,
    );
    let order: Vec<(&str, bool)> = actions
        .iter()
        .map(|action| {
            (
                action.home(),
                matches!(action, ImportAction::Remount { .. }),
            )
        })
        .collect();
    assert_eq!(
        order,
        vec![("node-b", true), ("node-a", false), ("node-c", false)]
    );
}

#[test]
fn mount_point_matches_the_published_target_prefix() {
    let point = mount_point(Path::new("/run/afs/imports"), "node-a");
    assert_eq!(point, Path::new("/run/afs/imports/node-a/users"));
}

#[test]
fn mount_source_uses_the_shared_users_root() {
    assert_eq!(
        mount_source("node-a", Path::new("/home/agentos/users")),
        "node-a:/home/agentos/users"
    );
}

/// 已挂载的 home 即使本轮快照里没有它的 root，也必须继续被维护。
///
/// 真实故障：A 侧快照暂时只含自己托管的 root，于是 node-b 从状态表被裁掉，
/// 之后 A 上的 node-b 挂载被外部卸载就再没人重挂，形成永久断链，还被告警
/// 成孤儿。挂载的生命周期不能跟着"本轮有没有 root"走。
#[test]
fn mounted_home_survives_a_round_without_its_roots() {
    let mut table = ImportTable::new();
    table.entry("node-b").confirm_mounted();

    // 本轮快照里只有 node-a 的 root，node-b 完全没出现。
    table.retain(&homes(&["node-a"]));
    assert!(
        table.state("node-b").is_some(),
        "a mounted home must not be forgotten just because it has no roots right now"
    );

    // 挂载被外部卸载后，仍必须产生重挂动作。
    table.entry("node-b").observe_absent();
    let actions = plan_imports(&homes(&["node-a"]), "node-a", &table, Instant::now());
    let homes_in_actions: Vec<&str> = actions.iter().map(ImportAction::home).collect();
    assert!(
        homes_in_actions.contains(&"node-b"),
        "the forgotten mount would never be repaired, got {homes_in_actions:?}"
    );
}

/// 从未挂载过的 home 不进状态表，避免表无限增长。
#[test]
fn never_mounted_home_is_not_retained() {
    let mut table = ImportTable::new();
    table.entry("node-x").observe_absent();
    table.retain(&homes(&["node-a"]));
    assert!(table.state("node-x").is_none());
}

/// 已挂载的 home 在快照里消失时，状态必须保留——见
/// `mounted_home_survives_a_round_without_its_roots` 的真实故障说明。
#[test]
fn retain_keeps_mounted_homes_even_when_absent_from_snapshot() {
    let mut table = ImportTable::new();
    table.entry("node-a").confirm_mounted();
    table.entry("node-b").confirm_mounted();
    assert_eq!(table.ready_count(), 2);

    table.retain(&homes(&["node-a"]));
    // node-b 仍在：它的挂载还在内核里，仍需要被维护。
    assert_eq!(table.ready_count(), 2);
    assert!(table.state("node-b").is_some());
}
