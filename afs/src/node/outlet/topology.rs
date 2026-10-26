//! 集群拓扑管理（spec §7.2）：传输无关的策略层。
//!
//! 职责是"数据面通路与集群成员表对齐"：周期全量拉取 Meta live 成员表
//! （`ListNodes`，10s，与节点租约节奏对齐），对每个远端成员做后端只读
//! 校验与探活，diff 出告警并发布最新 [`TopologyState`]。成员死活判定归
//! Meta 心跳，传输自愈（连接恢复）不归本组件；对端离场按 D12 保留挂载、
//! 仅移除拓扑记录。
//!
//! 探活按成员隔离（spec §12 故障表）：hard mount 下对端失联时 `stat`
//! 无限阻塞而非报错，因此每个远端成员一个独立探针线程，阻塞只困住该
//! 成员自己的线程，轮询循环只读缓存——单个失联对端无法冻结拓扑对齐。

use std::{
    collections::{BTreeMap, BTreeSet},
    sync::{
        Arc, Mutex,
        atomic::{AtomicBool, Ordering},
    },
    thread,
    time::Duration,
};

use tokio::sync::watch;

use super::transport::{NodeId, PathHealth, TransportBackend, TransportPathState};
use crate::node::rpc::meta::GrpcRootMeta;
use crate::runtime::{BoxError, cancelled};

/// 拓扑对齐周期：与节点 10s 重注册租约节奏一致（spec §7.2）。
const TOPOLOGY_SYNC_INTERVAL: Duration = Duration::from_secs(10);
/// 探针线程的探活节奏：与拓扑对齐同周期。
const PROBE_INTERVAL: Duration = Duration::from_secs(10);
/// 探针睡眠分段：保证 stop 标志在半个周期内被观察到。
const PROBE_STOP_POLL: Duration = Duration::from_millis(500);

/// 集群拓扑快照：live 成员表 × 各远端通路健康。本地节点在 `live` 中，
/// 但不对自身维护 `paths`（本地 home 走物理层 users_root，不经传输通路）。
#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub struct TopologyState {
    pub live: BTreeSet<NodeId>,
    pub paths: BTreeMap<NodeId, PathHealth>,
}

/// 一轮对齐产生的增量事件（由调用方记录日志）。
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum TopologyChange {
    MemberJoined { node: NodeId },
    MemberLeft { node: NodeId },
    PathMissing { node: NodeId },
    PathDrift { node: NodeId, reason: String },
    PathUnhealthy { node: NodeId },
    PathRecovered { node: NodeId },
}

/// 用最新 live 成员表与探针结果推进拓扑状态，返回新状态与增量事件。
///
/// 幂等：同一输入产生同一输出。纯函数——后端校验与探活由调用方在
/// 探针线程 / 阻塞池完成后，以 `probed` 形式传入。
pub(crate) fn advance_topology(
    previous: &TopologyState,
    live: &BTreeSet<NodeId>,
    probed: &BTreeMap<NodeId, PathHealth>,
    local_node_id: &NodeId,
) -> (TopologyState, Vec<TopologyChange>) {
    let mut changes = Vec::new();
    for node in previous
        .live
        .iter()
        .filter(|node| *node != local_node_id && !live.contains(*node))
    {
        changes.push(TopologyChange::MemberLeft { node: node.clone() });
    }
    for node in live
        .iter()
        .filter(|node| *node != local_node_id && !previous.live.contains(*node))
    {
        changes.push(TopologyChange::MemberJoined { node: node.clone() });
    }

    let mut paths = BTreeMap::new();
    for (node, health) in probed {
        if node == local_node_id {
            continue;
        }
        match previous.paths.get(node) {
            Some(previous_health) if previous_health == health => {}
            Some(previous_health) => {
                if health.usable() && !previous_health.usable() {
                    changes.push(TopologyChange::PathRecovered { node: node.clone() });
                } else if !health.usable() {
                    changes.push(degraded_change(node, health));
                }
            }
            // 新成员通路未就绪：补齐前表现为该 home 的 symlink 停发 + 告警
            // （spec §7.2，fail-safe，不阻塞其他流量）。
            None if !health.usable() => changes.push(degraded_change(node, health)),
            None => {}
        }
        paths.insert(node.clone(), health.clone());
    }
    (
        TopologyState {
            live: live.clone(),
            paths,
        },
        changes,
    )
}

fn degraded_change(node: &NodeId, health: &PathHealth) -> TopologyChange {
    match &health.state {
        TransportPathState::Missing => TopologyChange::PathMissing { node: node.clone() },
        TransportPathState::Drift { reason } => TopologyChange::PathDrift {
            node: node.clone(),
            reason: reason.clone(),
        },
        TransportPathState::Present => TopologyChange::PathUnhealthy { node: node.clone() },
    }
}

/// 单个成员的探针线程句柄。stop 只置标志不 join：线程可能正阻塞在
/// 失联对端的 stat 上（hard mount 无限重试），join 会卡住调用方；线程
/// 在 stat 返回后观察到 stop 自行退出。
struct MemberProbe {
    stop: Arc<AtomicBool>,
}

/// 每成员探针注册表（探活隔离的核心）：成员加入起线程、离场停线程。
///
/// 健康语义（spec §12 故障表）：
/// - 探活成功 → 缓存 true；
/// - 探活报错（soft mount / ESTALE）→ 缓存 false → 停发/下线该 home
///   的 symlink，恢复后对账补发；
/// - 探活挂死（hard mount 失联）→ 缓存保留最后成功值：已发布 symlink
///   保留（在跑容器由 kernel 无限重试无缝续上），新发布被阻止（新成员
///   从未探活成功，默认 false）。
///
/// 线程数上界 = 远端 live 成员数（+ 离场时仍阻塞在 stat 上的线程，随
/// stat 返回退出）。
struct ProbeRegistry {
    /// 探活结果缓存：最后成功值即当前判定。
    healthy: Arc<Mutex<BTreeMap<NodeId, bool>>>,
    probes: BTreeMap<NodeId, MemberProbe>,
}

impl ProbeRegistry {
    fn new() -> Self {
        Self {
            healthy: Arc::new(Mutex::new(BTreeMap::new())),
            probes: BTreeMap::new(),
        }
    }

    /// 按 live 成员表对齐探针线程：新成员起线程，离场成员停线程并清除
    /// 其缓存记录（重新加入时由新线程重新探活）。
    fn sync(&mut self, members: &BTreeSet<NodeId>, backend: &Arc<dyn TransportBackend>) {
        let departed: Vec<NodeId> = self
            .probes
            .keys()
            .filter(|node| !members.contains(*node))
            .cloned()
            .collect();
        for node in departed {
            if let Some(probe) = self.probes.remove(&node) {
                probe.stop.store(true, Ordering::Relaxed);
            }
            if let Ok(mut cache) = self.healthy.lock() {
                cache.remove(&node);
            }
        }
        for node in members {
            if self.probes.contains_key(node) {
                continue;
            }
            let stop = Arc::new(AtomicBool::new(false));
            let spawned = thread::Builder::new()
                .name(format!("outlet-probe-{node}"))
                .spawn({
                    let backend = Arc::clone(backend);
                    let node = node.clone();
                    let healthy = Arc::clone(&self.healthy);
                    let stop = Arc::clone(&stop);
                    move || probe_loop(backend.as_ref(), node, healthy, stop)
                });
            match spawned {
                Ok(_) => {
                    self.probes.insert(
                        node.clone(),
                        MemberProbe {
                            stop: Arc::clone(&stop),
                        },
                    );
                }
                Err(error) => {
                    // 线程创建失败（资源耗尽）：该成员保持"从未探活成功"，
                    // 表现为停发 + 下轮重试。
                    afs_logging::warn!(
                        "outlet.topology.probe_spawn_failed";
                        "node" => node.clone(),
                        "error" => error.to_string()
                    );
                }
            }
        }
    }

    /// 停止全部探针（shutdown 路径）。
    fn shutdown(&mut self) {
        for (_, probe) in std::mem::take(&mut self.probes) {
            probe.stop.store(true, Ordering::Relaxed);
        }
    }

    /// 探活缓存快照（轮询循环消费）。
    fn snapshot(&self) -> BTreeMap<NodeId, bool> {
        self.healthy
            .lock()
            .map(|cache| cache.clone())
            .unwrap_or_default()
    }
}

fn probe_loop(
    backend: &dyn TransportBackend,
    node: NodeId,
    healthy: Arc<Mutex<BTreeMap<NodeId, bool>>>,
    stop: Arc<AtomicBool>,
) {
    loop {
        if stop.load(Ordering::Relaxed) {
            return;
        }
        // 可能无限阻塞（hard mount 失联）——只困住本成员线程。
        let probed = backend.healthy(&node);
        if let Ok(mut cache) = healthy.lock() {
            cache.insert(node.clone(), probed);
        }
        // 分段睡眠：stop 及时可见，避免离场线程多跑一轮。
        let mut slept = Duration::ZERO;
        while slept < PROBE_INTERVAL {
            if stop.load(Ordering::Relaxed) {
                return;
            }
            thread::sleep(PROBE_STOP_POLL);
            slept += PROBE_STOP_POLL;
        }
    }
}

/// 拓扑对齐 worker：启动即全量校验一轮（spec §7.2），此后每 10s 拉取
/// Meta live 成员表并推进状态。
///
/// Meta 不可达只影响拓扑校验与告警（spec §7.2）：保留上一状态、下一周期
/// 重试，已建立的通路照常工作——与根命令控制流的 fail-closed 姿态不同。
pub(crate) async fn run_topology_sync(
    meta: Arc<GrpcRootMeta>,
    backend: Arc<dyn TransportBackend>,
    local_node_id: NodeId,
    state: watch::Sender<TopologyState>,
    stop: watch::Receiver<bool>,
) -> Result<(), BoxError> {
    let shutdown = cancelled(stop);
    tokio::pin!(shutdown);
    let mut current = TopologyState::default();
    let mut registry = ProbeRegistry::new();
    let mut delay = Duration::ZERO;
    loop {
        tokio::select! {
            biased;
            _ = &mut shutdown => {
                registry.shutdown();
                return Ok(());
            }
            _ = tokio::time::sleep(delay) => {}
        }
        delay = TOPOLOGY_SYNC_INTERVAL;
        let meta_for_task = meta.clone();
        let listed = tokio::task::spawn_blocking(move || meta_for_task.list_nodes()).await?;
        let (node_ids, _authority_revision) = match listed {
            Ok(listed) => listed,
            Err(error) => {
                afs_logging::warn!(
                    "outlet.topology.list_nodes_failed";
                    "error" => error.to_string()
                );
                continue;
            }
        };
        let live: BTreeSet<NodeId> = node_ids.into_iter().collect();
        let remote: BTreeSet<NodeId> = live
            .iter()
            .filter(|node| **node != local_node_id)
            .cloned()
            .collect();
        registry.sync(&remote, &backend);
        // 健康判定读探针缓存（阻塞只发生在各探针线程内）；mountinfo 校验
        // （本地 /proc 读）在阻塞池批量完成。
        let healthy_cache = registry.snapshot();
        let backend_for_task = backend.clone();
        let previous = current.clone();
        let local = local_node_id.clone();
        let (next, changes) = tokio::task::spawn_blocking(move || {
            let mut probed = BTreeMap::new();
            for node in &live {
                if node == &local {
                    continue;
                }
                let path = backend_for_task.path_state(node);
                let healthy = matches!(path, TransportPathState::Present)
                    && healthy_cache.get(node).copied().unwrap_or(false);
                probed.insert(
                    node.clone(),
                    PathHealth {
                        state: path,
                        healthy,
                    },
                );
            }
            advance_topology(&previous, &live, &probed, &local)
        })
        .await?;
        for change in &changes {
            log_topology_change(change);
        }
        current = next.clone();
        // 发布 worker 尚未订阅时无接收者，忽略发送结果并继续对齐。
        let _ = state.send(next);
    }
}

fn log_topology_change(change: &TopologyChange) {
    match change {
        TopologyChange::MemberJoined { node } => {
            afs_logging::info!("outlet.topology.member_joined"; "node" => node.clone());
        }
        TopologyChange::MemberLeft { node } => {
            afs_logging::warn!("outlet.topology.member_left"; "node" => node.clone());
        }
        TopologyChange::PathMissing { node } => {
            afs_logging::warn!(
                "outlet.topology.path_missing";
                "node" => node.clone(),
                "action" => "deployment must provide the import mount; AFS never mounts"
            );
        }
        TopologyChange::PathDrift { node, reason } => {
            afs_logging::warn!(
                "outlet.topology.path_drift";
                "node" => node.clone(),
                "reason" => reason.clone()
            );
        }
        TopologyChange::PathUnhealthy { node } => {
            afs_logging::warn!("outlet.topology.path_unhealthy"; "node" => node.clone());
        }
        TopologyChange::PathRecovered { node } => {
            afs_logging::info!("outlet.topology.path_recovered"; "node" => node.clone());
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::{ffi::OsStr, path::PathBuf};

    fn live(nodes: &[&str]) -> BTreeSet<NodeId> {
        nodes.iter().map(|node| (*node).to_owned()).collect()
    }

    fn probed(entries: &[((&str, TransportPathState), bool)]) -> BTreeMap<NodeId, PathHealth> {
        entries
            .iter()
            .map(|((node, state), healthy)| {
                (
                    (*node).to_owned(),
                    PathHealth {
                        state: state.clone(),
                        healthy: *healthy,
                    },
                )
            })
            .collect()
    }

    #[test]
    fn local_node_has_no_path_and_no_membership_events() {
        let (state, changes) = advance_topology(
            &TopologyState::default(),
            &live(&["A", "B"]),
            &probed(&[(("B", TransportPathState::Present), true)]),
            &"A".to_owned(),
        );
        assert_eq!(state.live, live(&["A", "B"]));
        assert_eq!(state.paths.keys().collect::<Vec<_>>(), [&"B".to_owned()]);
        assert!(state.paths["B"].usable());
        // 本地节点不产生成员事件；远端成员加入是低频通知。
        assert_eq!(
            changes,
            vec![TopologyChange::MemberJoined {
                node: "B".to_owned()
            }]
        );
    }

    #[test]
    fn missing_mount_reports_path_missing() {
        let (state, changes) = advance_topology(
            &TopologyState::default(),
            &live(&["A", "B"]),
            &probed(&[(("B", TransportPathState::Missing), false)]),
            &"A".to_owned(),
        );
        assert_eq!(state.paths["B"].state, TransportPathState::Missing);
        assert!(!state.paths["B"].usable());
        assert!(changes.contains(&TopologyChange::PathMissing {
            node: "B".to_owned()
        }));
    }

    #[test]
    fn drift_reports_reason() {
        let (_, changes) = advance_topology(
            &TopologyState::default(),
            &live(&["A", "B"]),
            &probed(&[(
                (
                    "B",
                    TransportPathState::Drift {
                        reason: "ext4".to_owned(),
                    },
                ),
                false,
            )]),
            &"A".to_owned(),
        );
        assert!(changes.contains(&TopologyChange::PathDrift {
            node: "B".to_owned(),
            reason: "ext4".to_owned()
        }));
    }

    #[test]
    fn present_but_unreachable_reports_unhealthy() {
        let (state, changes) = advance_topology(
            &TopologyState::default(),
            &live(&["A", "B"]),
            &probed(&[(("B", TransportPathState::Present), false)]),
            &"A".to_owned(),
        );
        assert!(!state.paths["B"].usable());
        assert!(changes.contains(&TopologyChange::PathUnhealthy {
            node: "B".to_owned()
        }));
    }

    #[test]
    fn recovery_after_degradation_reports_recovered() {
        let previous = TopologyState {
            live: live(&["A", "B"]),
            paths: probed(&[(("B", TransportPathState::Missing), false)]),
        };
        let (state, changes) = advance_topology(
            &previous,
            &live(&["A", "B"]),
            &probed(&[(("B", TransportPathState::Present), true)]),
            &"A".to_owned(),
        );
        assert!(state.paths["B"].usable());
        assert_eq!(
            changes,
            vec![TopologyChange::PathRecovered {
                node: "B".to_owned()
            }]
        );
    }

    #[test]
    fn member_departure_prunes_path() {
        let previous = TopologyState {
            live: live(&["A", "B"]),
            paths: probed(&[(("B", TransportPathState::Present), true)]),
        };
        let (state, changes) =
            advance_topology(&previous, &live(&["A"]), &probed(&[]), &"A".to_owned());
        assert!(state.paths.is_empty());
        assert_eq!(
            changes,
            vec![TopologyChange::MemberLeft {
                node: "B".to_owned()
            }]
        );
    }

    #[test]
    fn steady_state_is_silent() {
        let previous = TopologyState {
            live: live(&["A", "B"]),
            paths: probed(&[(("B", TransportPathState::Present), true)]),
        };
        let (state, changes) = advance_topology(
            &previous,
            &live(&["A", "B"]),
            &probed(&[(("B", TransportPathState::Present), true)]),
            &"A".to_owned(),
        );
        assert_eq!(state, previous);
        assert!(changes.is_empty());
    }

    /// 探活挂死语义（spec §12）：advance_topology 只看传入快照，"挂死"
    /// 表现为上一轮的成功值继续传入——已发布 symlink 保留、无降级事件。
    #[test]
    fn hung_probe_keeps_previous_health() {
        let previous = TopologyState {
            live: live(&["A", "B"]),
            paths: probed(&[(("B", TransportPathState::Present), true)]),
        };
        let (state, changes) = advance_topology(
            &previous,
            &live(&["A", "B"]),
            &probed(&[(("B", TransportPathState::Present), true)]),
            &"A".to_owned(),
        );
        assert!(state.paths["B"].usable());
        assert!(changes.is_empty());
    }

    struct ProbeFakeBackend {
        healthy: BTreeSet<NodeId>,
    }

    impl TransportBackend for ProbeFakeBackend {
        fn path_state(&self, _node: &NodeId) -> TransportPathState {
            TransportPathState::Present
        }

        fn healthy(&self, node: &NodeId) -> bool {
            self.healthy.contains(node)
        }

        fn resolve_target(&self, name: &OsStr, home: &NodeId) -> afs_error::Result<PathBuf> {
            Ok(PathBuf::from("/imports").join(home).join(name))
        }
    }

    fn wait_for_cache(
        registry: &ProbeRegistry,
        predicate: impl Fn(&BTreeMap<NodeId, bool>) -> bool,
    ) -> bool {
        for _ in 0..50 {
            if predicate(&registry.snapshot()) {
                return true;
            }
            thread::sleep(Duration::from_millis(100));
        }
        false
    }

    /// 探针线程写入缓存、离场清缓存：健康成员探活成功、离场后记录清除。
    #[test]
    fn probe_registry_updates_cache_and_prunes_departed() {
        let backend: Arc<dyn TransportBackend> = Arc::new(ProbeFakeBackend {
            healthy: ["B".to_owned()].into(),
        });
        let mut registry = ProbeRegistry::new();
        registry.sync(&live(&["B"]), &backend);
        assert!(wait_for_cache(&registry, |cache| {
            cache.get("B") == Some(&true)
        }));
        // 离场：缓存记录清除，探针线程退出（stop 后最多再跑一轮睡眠段）。
        registry.sync(&live(&[]), &backend);
        assert_eq!(registry.snapshot(), BTreeMap::new());
        registry.shutdown();
    }

    /// 从未探活成功的成员（对端失联）无缓存记录 → 轮询侧默认 false。
    #[test]
    fn probe_registry_never_probed_defaults_unhealthy() {
        let backend: Arc<dyn TransportBackend> = Arc::new(ProbeFakeBackend {
            healthy: BTreeSet::new(),
        });
        let mut registry = ProbeRegistry::new();
        registry.sync(&live(&["B"]), &backend);
        // 探活立即返回 false → 缓存为 false（而非无记录）。
        assert!(wait_for_cache(&registry, |cache| {
            cache.get("B") == Some(&false)
        }));
        registry.shutdown();
    }
}
