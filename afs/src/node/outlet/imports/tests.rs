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
    // 上限 10s（见 MAX_BACKOFF 的取值理由）：1→2→4→8→10→10…
    let expected = [1, 2, 4, 8, 10, 10, 10];
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

    // 降级需要连续 ABSENT_ROUNDS_BEFORE_DEMOTE 轮缺失（防抖），且跨过
    // MOUNT_SETTLE 抑制期——单轮缺失不足以判断"被外部卸载"。
    // 这里把观测时刻推后到抑制期之外，模拟"挂好一阵之后被外部卸掉"。
    let later = Instant::now() + MOUNT_SETTLE;
    for _ in 0..ABSENT_ROUNDS_BEFORE_DEMOTE {
        table
            .entry("node-a")
            .observe_absent(later, ABSENT_ROUNDS_BEFORE_DEMOTE);
    }
    assert_eq!(table.entry("node-a").phase(), Phase::NeedsMount);
    assert_eq!(
        plan_imports(&homes(&["node-a"]), "node-b", &table, later),
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

/// 挂载源解析回 host：换 IP 的判断就靠这一段。
#[test]
fn mount_source_host_splits_the_peer_address() {
    assert_eq!(
        mount_source_host("192.0.2.10:/home/agentos/users"),
        Some("192.0.2.10")
    );
    assert_eq!(
        mount_source_host("node-a.test:/home/agentos/users"),
        Some("node-a.test")
    );
    // bind 与块设备的源是纯路径，没有对端地址可读。
    assert_eq!(mount_source_host("/home/agentos/users"), None);
    assert_eq!(mount_source_host("/dev/sda1"), None);
    assert_eq!(mount_source_host(":/empty-host"), None);
    assert_eq!(mount_source_host("host:"), None);
}

/// IPv6 的挂载源带方括号，必须整体取到地址而不是在第一个冒号处截断。
///
/// 截断会得到 `[fe80`，与上报的 `fe80::1` 永远不等 —— 每轮都判"地址变了"，
/// 于是每 250ms 拆挂载重挂一次，把名字层访问拖成间歇失败。
#[test]
fn mount_source_host_handles_bracketed_ipv6() {
    assert_eq!(
        mount_source_host("[fe80::1]:/home/agentos/users"),
        Some("fe80::1")
    );
    assert_eq!(mount_source_host("[fe80::1]:"), None);
    assert_eq!(mount_source_host("[]:/home/agentos/users"), None);
    // 裸 IPv6 不可能出现在 mountinfo（无方括号时无法与路径分隔），读不出即放弃比对。
    assert_eq!(mount_source_host("fe80::1:/home/agentos/users"), None);
}

/// 配置里裸写 IPv6、mountinfo 带方括号时，两者必须判为同一个地址。
#[test]
fn normalize_host_treats_brackets_as_syntax_not_identity() {
    assert_eq!(normalize_host("[fe80::1]"), "fe80::1");
    assert_eq!(normalize_host("fe80::1"), "fe80::1");
    assert_eq!(normalize_host("192.0.2.10"), "192.0.2.10");
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

    // 挂载被外部卸载后，仍必须产生重挂动作（需连续缺失达到防抖阈值，
    // 且观测时刻在 MOUNT_SETTLE 抑制期之外）。
    let later = Instant::now() + MOUNT_SETTLE;
    for _ in 0..ABSENT_ROUNDS_BEFORE_DEMOTE {
        table
            .entry("node-b")
            .observe_absent(later, ABSENT_ROUNDS_BEFORE_DEMOTE);
    }
    let actions = plan_imports(&homes(&["node-a"]), "node-a", &table, later);
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
    table
        .entry("node-x")
        .observe_absent(Instant::now(), ABSENT_ROUNDS_BEFORE_DEMOTE);
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

/// `ready_homes` 是发布门禁的输入：只有 Ready 的 home 才可能出现在结果里。
#[test]
fn ready_homes_lists_only_ready_entries() {
    let mut table = ImportTable::new();
    table.entry("node-a").confirm_mounted();
    // node-b 记一次失败：处于 NeedsMount + 冷却中，不该被算作就绪。
    table.entry("node-b").record_failure(Instant::now());

    let ready = table.ready_homes();
    assert!(ready.contains("node-a"));
    assert!(!ready.contains("node-b"), "a failing home is not ready");
    assert_eq!(ready.len(), 1);
}

/// 未挂载过的 home 不在表里，自然也不在 ready 集合里。
#[test]
fn ready_homes_is_empty_for_a_fresh_table() {
    assert!(ImportTable::new().ready_homes().is_empty());
}
