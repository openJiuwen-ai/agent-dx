//! 表现层发布 worker（spec §9/§12）：单写者维护 symlink 农场。
//!
//! 发布 = P2 暂存（`.pub/<uuid>` symlink）+ P3 rename 原子亮出；下线 =
//! unlink（探测随即干净失败）。发布前置（meta Active + 本地物理目录存在 /
//! 远端通路可用）不满足绝不创建 symlink。worker 只调用 TransportBackend
//! 接口与 TopologyState 内存快照，零 NFS 知识（spec §7.1 模块边界）。

use std::{
    collections::{BTreeMap, BTreeSet},
    ffi::{OsStr, OsString},
    os::unix::fs::symlink,
    path::{Path, PathBuf},
    sync::{
        Arc,
        atomic::{AtomicU64, Ordering},
    },
    time::Duration,
};

use afs_error::Error;
use tokio::sync::watch;

use super::{
    reconcile::reconcile_farm,
    topology::TopologyState,
    transport::{NodeId, PathHealth, TransportBackend},
};
use crate::node::rpc::meta::{GrpcRootMeta, OutletEventBatch, OutletPublicationEvent};
use crate::runtime::{BoxError, cancelled};

pub(crate) const STAGING_DIR: &str = ".pub";
pub(crate) const QUARANTINE_DIR: &str = ".quarantine";

/// 8 字节随机 hex（16 字符），用于 quarantine 目标名：跨重启/跨进程唯一
/// 且无时钟假设。熵源优先 `/dev/urandom`（Linux 上启动早期即可用且永不
/// 阻塞）；极端不可用时回退到 时间戳+pid+进程内计数器 组合，仍保持
/// 跨进程唯一（时钟回拨场景由计数器兜底）。
fn random_hex_suffix() -> String {
    use std::io::Read;
    let mut bytes = [0u8; 8];
    let entropic = std::fs::File::open("/dev/urandom")
        .and_then(|mut file| file.read_exact(&mut bytes))
        .is_ok();
    if entropic {
        return bytes.iter().map(|b| format!("{b:02x}")).collect();
    }
    static FALLBACK: AtomicU64 = AtomicU64::new(0);
    let nanos = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|d| d.as_nanos() as u64)
        .unwrap_or(0);
    let seq = FALLBACK.fetch_add(1, Ordering::Relaxed);
    let value = nanos ^ ((u64::from(std::process::id()) << 32) | seq);
    format!("{value:016x}")
}

/// 与根命令控制流相同的轮询节奏（spec §9 v1：revision 轮询）。
const OUTLET_POLL_INTERVAL: Duration = Duration::from_millis(250);

/// 周期全量对账间隔（spec §12 等式的兜底校验）：事件流是假设性同步，
/// 任何来源的漂移（编号断裂、事件处理缺陷、farm 目录被外部改动）在此
/// 被纠正；对账成本为一次快照读 + tmpfs readdir，可忽略。
const OUTLET_RECONCILE_INTERVAL: Duration = Duration::from_secs(60);

#[derive(Clone, PartialEq)]
pub(crate) struct DesiredRoot {
    pub(crate) name: OsString,
    pub(crate) home: NodeId,
}

/// 发布 worker 的期望态：meta Active 集（root_id → 名字/home）、已亮出集与
/// 摘牌欠账（root_id → 名字，摘牌失败待重试；名字不能丢，否则过期入口
/// 滞留发布面直到重启）。游标不持久化；重启 = 全量对账重建（spec §9）。
#[derive(Default)]
pub(crate) struct FarmState {
    pub(crate) roots: BTreeMap<String, DesiredRoot>,
    pub(crate) published: BTreeSet<String>,
    pub(crate) pending_unpublish: BTreeMap<String, OsString>,
}

/// 表现层单写者。所有方法只做本地 syscall（tmpfs 上的 rename/unlink），
/// 可在阻塞线程直接调用；远端探活不在本类型内（归 TopologyManager）。
pub struct OutletPublisher {
    public_root: PathBuf,
    users_root: PathBuf,
    backend: Arc<dyn TransportBackend>,
    topology: watch::Receiver<TopologyState>,
    local_node_id: NodeId,
    staging_sequence: AtomicU64,
}

impl OutletPublisher {
    #[must_use]
    pub fn new(
        public_root: PathBuf,
        users_root: PathBuf,
        backend: Arc<dyn TransportBackend>,
        topology: watch::Receiver<TopologyState>,
        local_node_id: NodeId,
    ) -> Self {
        Self {
            public_root,
            users_root,
            backend,
            topology,
            local_node_id,
            staging_sequence: AtomicU64::new(1),
        }
    }

    pub(crate) fn public_root(&self) -> &Path {
        &self.public_root
    }

    fn staging_path(&self) -> PathBuf {
        let sequence = self.staging_sequence.fetch_add(1, Ordering::Relaxed);
        self.public_root
            .join(STAGING_DIR)
            .join(format!("{}-{}", std::process::id(), sequence))
    }

    /// P2 + P3：暂存 symlink 后 rename 原子亮出（spec §9 硬规则 1/2）。
    /// 目标被目录实体占据（消费方违约的 ghost 目录）时先隔离再重试；
    /// 目标为既有 symlink/文件时 rename 直接原子替换。
    pub(crate) fn publish(&self, name: &OsStr, target: &Path) -> afs_error::Result<()> {
        let staging = self.staging_path();
        symlink(target, &staging).map_err(Error::from)?;
        let final_path = self.public_root.join(name);
        match std::fs::rename(&staging, &final_path) {
            Ok(()) => Ok(()),
            Err(error) => {
                if Self::is_directory(&final_path) {
                    self.quarantine(&final_path);
                    if let Err(retry_error) = std::fs::rename(&staging, &final_path) {
                        let _ = std::fs::remove_file(&staging);
                        return Err(Error::from(retry_error));
                    }
                    return Ok(());
                }
                let _ = std::fs::remove_file(&staging);
                Err(Error::from(error))
            }
        }
    }

    /// 原子下线：unlink 即完成（spec §9 硬规则 4/5）。幂等容忍 ENOENT；
    /// 其他失败返回错误，由调用方入欠账队列（`FarmState::pending_unpublish`）
    /// 交下轮收敛重试。tmpfs 上唯一现实持久失败 = 名字位被目录实体占据
    /// （消费方违约），其余故障下一轮 250ms 即收敛。
    pub(crate) fn unpublish(&self, name: &OsStr) -> afs_error::Result<()> {
        match std::fs::remove_file(self.public_root.join(name)) {
            Ok(()) => Ok(()),
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => Ok(()),
            Err(error) => Err(Error::from(error)),
        }
    }

    /// 清扫 `.pub/` 全部残留（P2 之后崩溃的遗留；单写者保证无归属争议，
    /// spec §9 硬规则 3）。单个残留清理失败只记录，不阻塞对账。
    pub(crate) fn sweep_staging(&self) -> afs_error::Result<()> {
        let staging_dir = self.public_root.join(STAGING_DIR);
        let entries = std::fs::read_dir(&staging_dir).map_err(Error::from)?;
        for entry in entries {
            let entry = match entry {
                Ok(entry) => entry,
                Err(error) => return Err(Error::from(error)),
            };
            if let Err(error) = std::fs::remove_file(entry.path())
                && error.kind() != std::io::ErrorKind::NotFound
            {
                afs_logging::warn!(
                    "outlet.publisher.staging_sweep_failed";
                    "path" => ?entry.path(), "error" => error.to_string()
                );
            }
        }
        Ok(())
    }

    /// 将 farm 中的非 symlink 实体移入 `.quarantine/` 保留证据（spec §13
    /// ghost-bind 防线 2/3）。失败只记录；残留实体由下轮对账再次尝试。
    ///
    /// 目标名 = 原名截断 + 8 字节随机 hex：序号跨重启归零会与既有隔离项
    /// 撞名（rename 对目录报 EEXIST，该名字的发布位永久卡死）；原名单独
    /// 作前缀则满 NAME_MAX 时必溢出（与 ownerfs catalog 临时名同一教训）。
    /// 截断 + 随机后缀恒满足 NAME_MAX 且全局唯一，前缀保留可溯源性。
    pub(crate) fn quarantine(&self, path: &Path) {
        let Some(name) = path.file_name() else {
            return;
        };
        let suffix = random_hex_suffix();
        let mut stem = name.to_string_lossy().into_owned();
        // NAME_MAX(255) - "-" - 16 hex = 238；留 1 字节余量。回退到字符
        // 边界（String::truncate 落在多字节字符中间会 panic）。
        let mut stem_len = stem.len().min(237);
        while !stem.is_char_boundary(stem_len) {
            stem_len -= 1;
        }
        stem.truncate(stem_len);
        let target = self
            .public_root
            .join(QUARANTINE_DIR)
            .join(format!("{stem}-{suffix}"));
        match std::fs::rename(path, &target) {
            Ok(()) => {
                afs_logging::warn!(
                    "outlet.publisher.quarantined_foreign_entry";
                    "path" => ?path, "quarantine_target" => ?target
                );
            }
            Err(error) => {
                afs_logging::warn!(
                    "outlet.publisher.quarantine_failed";
                    "path" => ?path, "error" => error.to_string()
                );
            }
        }
    }

    fn is_directory(path: &Path) -> bool {
        std::fs::symlink_metadata(path).is_ok_and(|meta| meta.is_dir())
    }

    /// 已亮出 symlink 的保留判据（spec §12 停发/下线）：
    /// - 本地 home：保留（物理缺失不自动下线，A1 违约由对账发现）；
    /// - 远端 home：通路已知不可用（live 成员但缺失/漂移/探活失败）→ false；
    ///   成员离场或拓扑未就绪（无记录）→ 保留（D12，fail-safe）。
    fn symlink_should_stay(&self, home: &NodeId) -> bool {
        if home == &self.local_node_id {
            return true;
        }
        match self.topology.borrow().paths.get(home) {
            Some(health) => health.usable(),
            None => true,
        }
    }

    /// 期望 target 的路径文本（发布与对账的共享单点）：本地 home =
    /// `users_root/<name>`；远端 home = 传输后端布局。纯路径计算，不含
    /// 存在性/拓扑门控（那是 [`Self::try_publish_root`] 的职责），对账
    /// 据此比对已亮出 symlink 的指向。
    pub(crate) fn expected_target(
        &self,
        name: &OsStr,
        home: &NodeId,
    ) -> afs_error::Result<PathBuf> {
        if home == &self.local_node_id {
            Ok(self.users_root.join(name))
        } else {
            self.backend.resolve_target(name, home)
        }
    }

    /// 单个 root 的发布决策 + 执行，返回是否已亮出。失败只记录并返回
    /// false，留在未发布集由下轮收敛重试（fail-safe，不阻塞其他 root）。
    pub(crate) fn try_publish_root(&self, name: &OsStr, home: &NodeId) -> bool {
        let target = match self.expected_target(name, home) {
            Ok(target) => target,
            Err(error) => {
                afs_logging::warn!(
                    "outlet.publisher.resolve_target_failed";
                    "workspace" => ?name, "home" => home.clone(),
                    "error" => error.to_string()
                );
                return false;
            }
        };
        if home == &self.local_node_id {
            // 本地 home：物理目录存在才发布（spec §9 前置；meta Active 但
            // 物理缺失 = A1 违约残留，停发但不触碰）。
            if std::fs::symlink_metadata(&target).is_err() {
                return false;
            }
        } else {
            // 远端 home：只在拓扑正面报告通路可用时发布（成员离场或拓扑
            // 未就绪均停发，spec §7.2/D12）。
            let usable = self
                .topology
                .borrow()
                .paths
                .get(home)
                .is_some_and(PathHealth::usable);
            if !usable {
                return false;
            }
        }
        match self.publish(name, &target) {
            Ok(()) => true,
            Err(error) => {
                afs_logging::warn!(
                    "outlet.publisher.publish_failed";
                    "workspace" => ?name, "home" => home.clone(),
                    "error" => error.to_string()
                );
                false
            }
        }
    }
}

/// 应用一批事件到期望态（spec §9：Activate → 收敛时发布；Delete → 下线）。
/// NodeSessionRegistered 是拓扑输入（TopologyManager 消费），发布面无动作。
pub(crate) fn apply_outlet_events(
    state: &mut FarmState,
    events: Vec<OutletPublicationEvent>,
    publisher: &OutletPublisher,
) {
    for event in events {
        match event {
            OutletPublicationEvent::RootActivated {
                root_id,
                name,
                home_node_id,
            } => {
                state.roots.insert(
                    root_id,
                    DesiredRoot {
                        name,
                        home: home_node_id,
                    },
                );
            }
            OutletPublicationEvent::RootDeleted { root_id } => {
                if let Some(root) = state.roots.remove(&root_id)
                    && state.published.remove(&root_id)
                    && let Err(error) = publisher.unpublish(&root.name)
                {
                    afs_logging::warn!(
                        "outlet.publisher.unpublish_failed";
                        "workspace" => ?root.name, "error" => error.to_string()
                    );
                    // 摘牌失败：名字入欠账队列由下轮收敛重试。root 已
                    // 划出两个集合，名字再丢就无人重试了。
                    state.pending_unpublish.insert(root_id, root.name);
                }
            }
            OutletPublicationEvent::NodeSessionRegistered { .. } => {}
        }
    }
}

/// 收敛一遍 farm（spec §12）：先重试摘牌欠账，再下线通路失效的已发布
/// root（停发/下线），最后补发未发布 root（通路恢复后对账补发）。
/// 幂等，可重复执行。
pub(crate) fn converge_farm(state: &mut FarmState, publisher: &OutletPublisher) {
    // 摘牌欠账重试：成功出队；root 已重新亮出（同名复活后 republish 用
    // rename 原子替换了名字位）则取消欠账——新 symlink 拥有该名字，重试
    // 反而会误摘。republish 前的旧 symlink 重试摘除无害且正确。
    let mut retired: Vec<String> = Vec::new();
    for (root_id, name) in &state.pending_unpublish {
        if state.published.contains(root_id) {
            retired.push(root_id.clone());
            continue;
        }
        match publisher.unpublish(name) {
            Ok(()) => retired.push(root_id.clone()),
            Err(error) => {
                afs_logging::warn!(
                    "outlet.publisher.unpublish_retry_failed";
                    "workspace" => ?name, "error" => error.to_string()
                );
            }
        }
    }
    for root_id in retired {
        state.pending_unpublish.remove(&root_id);
    }

    let degraded: Vec<String> = state
        .roots
        .iter()
        .filter(|(root_id, root)| {
            state.published.contains(*root_id) && !publisher.symlink_should_stay(&root.home)
        })
        .map(|(root_id, _)| root_id.clone())
        .collect();
    for root_id in degraded {
        if let Some(root) = state.roots.get(&root_id)
            && let Err(error) = publisher.unpublish(&root.name)
        {
            afs_logging::warn!(
                "outlet.publisher.unpublish_failed";
                "workspace" => ?root.name, "error" => error.to_string()
            );
            // 通路失效必须停发（从已亮出集移除），但摘牌失败的名字
            // 同样入欠账，由下轮重试。
            state
                .pending_unpublish
                .insert(root_id.clone(), root.name.clone());
        }
        state.published.remove(&root_id);
        afs_logging::info!(
            "outlet.publisher.path_degraded_unpublished";
            "root_id" => root_id.clone()
        );
    }

    let pending: Vec<(String, OsString, NodeId)> = state
        .roots
        .iter()
        .filter(|(root_id, _)| !state.published.contains(*root_id))
        .map(|(root_id, root)| (root_id.clone(), root.name.clone(), root.home.clone()))
        .collect();
    for (root_id, name, home) in pending {
        if publisher.try_publish_root(&name, &home) {
            state.published.insert(root_id);
        }
    }
}

/// 全量对账结果并回运行态：以快照重建的 state 为准，保留旧摘牌欠账
/// （root 已复活的条目由 converge 的 published 判据取消；未复活的由
/// 重试幂等收敛），返回漂移计数供告警。漂移 = 事件流假设被打破的信号
/// （编号断裂、事件处理缺陷、farm 被外部改动），静默纠正会掩盖缺陷。
fn merge_reconciled_state(old: FarmState, mut reconciled: FarmState) -> (FarmState, usize) {
    let mut drift = 0;
    for id in old.roots.keys() {
        if !reconciled.roots.contains_key(id) {
            drift += 1;
        }
    }
    for (id, new_root) in &reconciled.roots {
        match old.roots.get(id) {
            None => drift += 1,
            Some(old_root) if old_root != new_root => drift += 1,
            Some(_) => {}
        }
    }
    drift += old
        .published
        .symmetric_difference(&reconciled.published)
        .count();
    reconciled.pending_unpublish.extend(old.pending_unpublish);
    (reconciled, drift)
}

/// 发布 worker（spec §9 v1：250ms revision 轮询，复制根命令控制流的成熟
/// 模式）。启动 = 全量快照锚点 + farm diff 重建（spec §12）；Compacted
/// 同构恢复。游标不持久化。
///
/// 错误姿态 = fail-closed 停发（spec §9）：poll 失败不推进游标、不执行
/// 发布/下线动作，仅告警并下轮重试——现有 symlink 保持，Meta 恢复后从
/// 同一游标续拉，无事件丢失窗口。
pub(crate) async fn run_outlet_publisher(
    meta: Arc<GrpcRootMeta>,
    publisher: Arc<OutletPublisher>,
    stop: watch::Receiver<bool>,
) -> Result<(), BoxError> {
    let shutdown = cancelled(stop);
    tokio::pin!(shutdown);

    // 启动对账：全量快照 + farm diff。Meta 暂不可达时重试（游标尚未推进，
    // 与 fail-closed 停发同一姿态）。
    let (mut state, mut after_revision) = loop {
        let meta_for_snapshot = meta.clone();
        let snapshot =
            tokio::task::spawn_blocking(move || meta_for_snapshot.list_outlet_roots()).await?;
        match snapshot {
            Ok((active, authority_revision)) => {
                let publisher_for_reconcile = publisher.clone();
                let state = tokio::task::spawn_blocking(move || {
                    reconcile_farm(&publisher_for_reconcile, &active)
                })
                .await??;
                break (state, authority_revision);
            }
            Err(error) => {
                afs_logging::warn!(
                    "outlet.publisher.startup_snapshot_failed";
                    "error" => error.to_string()
                );
                tokio::select! {
                    biased;
                    _ = &mut shutdown => return Ok(()),
                    _ = tokio::time::sleep(OUTLET_POLL_INTERVAL) => {}
                }
            }
        }
    };

    let mut delay = Duration::ZERO;
    let mut last_reconcile = std::time::Instant::now();
    loop {
        tokio::select! {
            biased;
            _ = &mut shutdown => return Ok(()),
            _ = tokio::time::sleep(delay) => {}
        }
        delay = OUTLET_POLL_INTERVAL;

        // 周期全量对账（spec §12 兜底）：事件流是假设性同步，漂移（编号
        // 断裂如 memory meta 重启、事件处理缺陷、farm 被外部改动）在此
        // 纠正并以快照 revision 重新锚定游标。
        if last_reconcile.elapsed() >= OUTLET_RECONCILE_INTERVAL {
            let meta_for_snapshot = meta.clone();
            let snapshot =
                tokio::task::spawn_blocking(move || meta_for_snapshot.list_outlet_roots()).await?;
            match snapshot {
                Ok((active, authority_revision)) => {
                    let publisher_for_reconcile = publisher.clone();
                    let old_state = std::mem::take(&mut state);
                    // reconcile 在阻塞线程执行（含 tmpfs IO）；merge 是纯内存
                    // 合并，可内联。对账失败与快照失败同姿态：不退出，保留
                    // 旧 state 等下个周期重试（spec §12 对账是幂等兜底）。
                    let reconciled = tokio::task::spawn_blocking(move || {
                        reconcile_farm(&publisher_for_reconcile, &active)
                    })
                    .await;
                    match reconciled {
                        Ok(Ok(reconciled)) => {
                            let (merged, drift) = merge_reconciled_state(old_state, reconciled);
                            if drift > 0 {
                                afs_logging::warn!(
                                    "outlet.publisher.reconcile_drift";
                                    "drift" => drift
                                );
                            }
                            state = merged;
                            after_revision = authority_revision;
                        }
                        Ok(Err(error)) => {
                            state = old_state;
                            afs_logging::warn!(
                                "outlet.publisher.periodic_reconcile_failed";
                                "error" => error.to_string()
                            );
                        }
                        Err(error) => {
                            state = old_state;
                            afs_logging::warn!(
                                "outlet.publisher.periodic_reconcile_failed";
                                "error" => error.to_string()
                            );
                        }
                    }
                }
                Err(error) => {
                    afs_logging::warn!(
                        "outlet.publisher.periodic_reconcile_failed";
                        "error" => error.to_string()
                    );
                }
            }
            last_reconcile = std::time::Instant::now();
        }

        let meta_for_poll = meta.clone();
        let batch =
            tokio::task::spawn_blocking(move || meta_for_poll.poll_outlet_events(after_revision))
                .await?;
        match batch {
            Ok(OutletEventBatch::Events {
                next_revision,
                events,
            }) => {
                let publisher_for_converge = publisher.clone();
                let mut state_for_converge = std::mem::take(&mut state);
                state = tokio::task::spawn_blocking(move || {
                    apply_outlet_events(&mut state_for_converge, events, &publisher_for_converge);
                    converge_farm(&mut state_for_converge, &publisher_for_converge);
                    state_for_converge
                })
                .await?;
                // 游标纪律：next_revision 为排他界，下一轮以 next_revision - 1
                // 恢复；空轮次（next = after + 1）游标不动、回到节流间隔。
                let cursor = next_revision - 1;
                if cursor > after_revision {
                    delay = Duration::ZERO;
                }
                after_revision = cursor;
            }
            Ok(OutletEventBatch::Compacted) => {
                // 事件史不可续：全量快照 + farm diff 重建（与启动对账同构），
                // 保留摘牌欠账。恢复失败不退出：游标未重锚，下轮 poll 仍会
                // 返回 Compacted，自然形成重试（例行信号不得升级为进程退出）。
                let meta_for_snapshot = meta.clone();
                let snapshot =
                    tokio::task::spawn_blocking(move || meta_for_snapshot.list_outlet_roots())
                        .await?;
                match snapshot {
                    Ok((active, authority_revision)) => {
                        let publisher_for_reconcile = publisher.clone();
                        let old_state = std::mem::take(&mut state);
                        let reconciled = tokio::task::spawn_blocking(move || {
                            reconcile_farm(&publisher_for_reconcile, &active)
                        })
                        .await;
                        match reconciled {
                            Ok(Ok(reconciled)) => {
                                state = merge_reconciled_state(old_state, reconciled).0;
                                after_revision = authority_revision;
                            }
                            Ok(Err(error)) => {
                                state = old_state;
                                afs_logging::warn!(
                                    "outlet.publisher.compacted_recovery_failed";
                                    "error" => error.to_string()
                                );
                            }
                            Err(error) => {
                                state = old_state;
                                afs_logging::warn!(
                                    "outlet.publisher.compacted_recovery_failed";
                                    "error" => error.to_string()
                                );
                            }
                        }
                    }
                    Err(error) => {
                        afs_logging::warn!(
                            "outlet.publisher.compacted_recovery_failed";
                            "error" => error.to_string()
                        );
                    }
                }
            }
            Err(error) => {
                afs_logging::warn!(
                    "outlet.publisher.poll_failed";
                    "after_revision" => after_revision, "error" => error.to_string()
                );
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::super::transport::TransportPathState;
    use super::*;

    struct FakeBackend;

    impl TransportBackend for FakeBackend {
        fn path_state(&self, _node: &NodeId) -> TransportPathState {
            TransportPathState::Missing
        }

        fn healthy(&self, _node: &NodeId) -> bool {
            false
        }

        fn resolve_target(&self, name: &OsStr, home: &NodeId) -> afs_error::Result<PathBuf> {
            Ok(PathBuf::from("/imports").join(home).join(name))
        }
    }

    struct Fixture {
        root: tempfile::TempDir,
        publisher: OutletPublisher,
        topology_tx: watch::Sender<TopologyState>,
    }

    impl Fixture {
        fn new() -> Self {
            let root = tempfile::tempdir().unwrap();
            let public = root.path().join("public");
            std::fs::create_dir(&public).unwrap();
            std::fs::create_dir(root.path().join("users")).unwrap();
            std::fs::create_dir(public.join(STAGING_DIR)).unwrap();
            std::fs::create_dir(public.join(QUARANTINE_DIR)).unwrap();
            let (topology_tx, topology_rx) = watch::channel(TopologyState::default());
            let publisher = OutletPublisher::new(
                public,
                root.path().join("users"),
                Arc::new(FakeBackend),
                topology_rx,
                "A".to_owned(),
            );
            Self {
                root,
                publisher,
                topology_tx,
            }
        }

        fn public(&self) -> &Path {
            self.publisher.public_root()
        }

        fn users(&self) -> PathBuf {
            self.root.path().join("users")
        }

        fn set_topology(&self, node: &str, state: TransportPathState, healthy: bool) {
            self.topology_tx.send_modify(|topology| {
                topology
                    .paths
                    .insert(node.to_owned(), PathHealth { state, healthy });
                topology.live.insert(node.to_owned());
            });
        }

        fn clear_topology(&self) {
            self.topology_tx.send_modify(|topology| {
                topology.paths.clear();
                topology.live.clear();
            });
        }
    }

    fn usable() -> TransportPathState {
        TransportPathState::Present
    }

    fn degraded() -> TransportPathState {
        TransportPathState::Drift {
            reason: "drifted".to_owned(),
        }
    }

    fn farm_symlink(fx: &Fixture, name: &str) -> bool {
        std::fs::symlink_metadata(fx.public().join(name))
            .is_ok_and(|meta| meta.file_type().is_symlink())
    }

    #[test]
    fn publish_creates_symlink_and_unpublish_is_idempotent() {
        let fx = Fixture::new();
        fx.publisher
            .publish(OsStr::new("ws-a"), Path::new("/users/ws-a"))
            .unwrap();
        assert!(farm_symlink(&fx, "ws-a"));
        assert_eq!(
            std::fs::read_link(fx.public().join("ws-a")).unwrap(),
            Path::new("/users/ws-a")
        );
        // 暂存目录不留残留（P3 rename 已消费）。
        assert_eq!(
            std::fs::read_dir(fx.public().join(STAGING_DIR))
                .unwrap()
                .count(),
            0
        );

        fx.publisher.unpublish(OsStr::new("ws-a")).unwrap();
        assert!(!farm_symlink(&fx, "ws-a"));
        // 幂等：ENOENT 不告警不失败。
        fx.publisher.unpublish(OsStr::new("ws-a")).unwrap();
    }

    #[test]
    fn publish_quarantines_ghost_directory_before_reveal() {
        let fx = Fixture::new();
        // 消费方违约在 farm 中创建的 ghost 目录（spec §13 防线 2）。
        std::fs::create_dir(fx.public().join("ws-a")).unwrap();
        fx.publisher
            .publish(OsStr::new("ws-a"), Path::new("/users/ws-a"))
            .unwrap();
        assert!(farm_symlink(&fx, "ws-a"));
        let quarantine = fx.public().join(QUARANTINE_DIR);
        let quarantined: Vec<_> = std::fs::read_dir(&quarantine)
            .unwrap()
            .map(|entry| entry.unwrap().file_name())
            .collect();
        assert_eq!(quarantined.len(), 1);
        assert!(quarantined[0].to_string_lossy().starts_with("ws-a-"));
    }

    #[test]
    fn quarantine_same_name_twice_produces_distinct_targets() {
        let fx = Fixture::new();
        // 重启撞名场景的进程内等价验证：旧实现的目标名来自跨重启归零的
        // 计数器，同名第二次隔离会 EEXIST 失败、发布位永久卡死。
        for _ in 0..2 {
            std::fs::create_dir(fx.public().join("ws-a")).unwrap();
            fx.publisher.quarantine(&fx.public().join("ws-a"));
            assert!(!fx.public().join("ws-a").exists());
        }
        let quarantine = fx.public().join(QUARANTINE_DIR);
        let mut names: Vec<_> = std::fs::read_dir(&quarantine)
            .unwrap()
            .map(|entry| entry.unwrap().file_name())
            .collect();
        names.sort();
        assert_eq!(names.len(), 2);
        assert!(names[0].to_string_lossy().starts_with("ws-a-"));
        assert_ne!(names[0], names[1]);
    }

    #[test]
    fn quarantine_truncates_names_near_name_max() {
        let fx = Fixture::new();
        // 满 NAME_MAX 的名字加后缀必溢出（ENAMETOOLONG）；截断前缀保证
        // rename 成功且目标名可溯源。
        let long_name = "w".repeat(255);
        let ghost = fx.public().join(&long_name);
        std::fs::create_dir(&ghost).unwrap();
        fx.publisher.quarantine(&ghost);
        let quarantine = fx.public().join(QUARANTINE_DIR);
        let quarantined: Vec<_> = std::fs::read_dir(&quarantine)
            .unwrap()
            .map(|entry| entry.unwrap().file_name())
            .collect();
        assert_eq!(quarantined.len(), 1);
        let name = quarantined[0].to_string_lossy().into_owned();
        assert!(name.starts_with("wwww"), "prefix preserved for tracing");
        assert!(name.len() <= 255, "quarantine target fits NAME_MAX");
        assert!(!ghost.exists(), "ghost removed from the farm");
    }

    #[test]
    fn try_publish_root_local_home_requires_physical_directory() {
        let fx = Fixture::new();
        // 物理目录缺失：停发（spec §9 前置）。
        assert!(
            !fx.publisher
                .try_publish_root(OsStr::new("ws-a"), &"A".to_owned())
        );
        assert!(!farm_symlink(&fx, "ws-a"));
        std::fs::create_dir(fx.users().join("ws-a")).unwrap();
        assert!(
            fx.publisher
                .try_publish_root(OsStr::new("ws-a"), &"A".to_owned())
        );
        assert_eq!(
            std::fs::read_link(fx.public().join("ws-a")).unwrap(),
            fx.users().join("ws-a")
        );
    }

    #[test]
    fn try_publish_root_remote_home_requires_positive_topology() {
        let fx = Fixture::new();
        let remote = "B".to_owned();
        // 拓扑未知（未就绪/成员离场）：停发。
        assert!(!fx.publisher.try_publish_root(OsStr::new("ws-a"), &remote));
        // 通路可用：发布，target 由后端决定（发布面零 NFS 知识）。
        fx.set_topology(&remote, usable(), true);
        assert!(fx.publisher.try_publish_root(OsStr::new("ws-a"), &remote));
        assert_eq!(
            std::fs::read_link(fx.public().join("ws-a")).unwrap(),
            Path::new("/imports/B/ws-a")
        );
        // 通路漂移：停发新 root。
        fx.set_topology(&remote, degraded(), true);
        assert!(!fx.publisher.try_publish_root(OsStr::new("ws-b"), &remote));
    }

    #[test]
    fn converge_unpublishes_degraded_and_republishes_on_recovery() {
        let fx = Fixture::new();
        let remote = "B".to_owned();
        let mut state = FarmState::default();
        state.roots.insert(
            "r1".to_owned(),
            DesiredRoot {
                name: "ws-a".into(),
                home: remote.clone(),
            },
        );
        fx.set_topology(&remote, usable(), true);
        converge_farm(&mut state, &fx.publisher);
        assert!(state.published.contains("r1"));
        assert!(farm_symlink(&fx, "ws-a"));

        // 通路失效：停发/下线（spec §12），root 保留在期望态等待恢复。
        fx.set_topology(&remote, degraded(), false);
        converge_farm(&mut state, &fx.publisher);
        assert!(!state.published.contains("r1"));
        assert!(state.roots.contains_key("r1"));
        assert!(!farm_symlink(&fx, "ws-a"));

        // 通路恢复：对账补发。
        fx.set_topology(&remote, usable(), true);
        converge_farm(&mut state, &fx.publisher);
        assert!(state.published.contains("r1"));
        assert!(farm_symlink(&fx, "ws-a"));
    }

    #[test]
    fn converge_keeps_symlink_for_departed_member() {
        let fx = Fixture::new();
        let mut state = FarmState::default();
        state.roots.insert(
            "r1".to_owned(),
            DesiredRoot {
                name: "ws-a".into(),
                home: "B".to_owned(),
            },
        );
        state.published.insert("r1".to_owned());
        std::os::unix::fs::symlink("/imports/B/ws-a", fx.public().join("ws-a")).unwrap();
        // 成员离场（D12）：保留挂载与 symlink，仅停发新发布。
        fx.clear_topology();
        converge_farm(&mut state, &fx.publisher);
        assert!(state.published.contains("r1"));
        assert!(farm_symlink(&fx, "ws-a"));
    }

    #[test]
    fn apply_events_drive_publish_and_unpublish() {
        let fx = Fixture::new();
        fx.set_topology("B", usable(), true);
        let mut state = FarmState::default();
        apply_outlet_events(
            &mut state,
            vec![OutletPublicationEvent::RootActivated {
                root_id: "r1".into(),
                name: "ws-a".into(),
                home_node_id: "B".into(),
            }],
            &fx.publisher,
        );
        converge_farm(&mut state, &fx.publisher);
        assert!(farm_symlink(&fx, "ws-a"));

        apply_outlet_events(
            &mut state,
            vec![OutletPublicationEvent::RootDeleted {
                root_id: "r1".into(),
            }],
            &fx.publisher,
        );
        assert!(!farm_symlink(&fx, "ws-a"));
        assert!(state.roots.is_empty());
        assert!(state.published.is_empty());

        // 未知 root 的删除事件是幂等 no-op。
        apply_outlet_events(
            &mut state,
            vec![OutletPublicationEvent::RootDeleted {
                root_id: "unknown".into(),
            }],
            &fx.publisher,
        );
    }

    #[test]
    fn unpublish_failure_is_retried_by_converge_until_success() {
        let fx = Fixture::new();
        fx.set_topology("B", usable(), true);
        let mut state = FarmState::default();
        apply_outlet_events(
            &mut state,
            vec![OutletPublicationEvent::RootActivated {
                root_id: "r1".into(),
                name: "ws-a".into(),
                home_node_id: "B".into(),
            }],
            &fx.publisher,
        );
        converge_farm(&mut state, &fx.publisher);
        assert!(state.published.contains("r1"));

        // 消费方违约：名字位被目录实体占据，删除事件摘牌失败（EISDIR）。
        // 名字必须入欠账队列，否则过期入口滞留发布面直到重启。
        std::fs::remove_file(fx.public().join("ws-a")).unwrap();
        std::fs::create_dir(fx.public().join("ws-a")).unwrap();
        apply_outlet_events(
            &mut state,
            vec![OutletPublicationEvent::RootDeleted {
                root_id: "r1".into(),
            }],
            &fx.publisher,
        );
        assert!(state.pending_unpublish.contains_key("r1"));
        assert!(std::fs::symlink_metadata(fx.public().join("ws-a")).is_ok());

        // 违约目录仍在：重试失败，欠账保留。
        converge_farm(&mut state, &fx.publisher);
        assert!(state.pending_unpublish.contains_key("r1"));

        // 违约目录被清理（人工/重启模拟）：重试成功（ENOENT 幂等），欠账出队。
        std::fs::remove_dir(fx.public().join("ws-a")).unwrap();
        converge_farm(&mut state, &fx.publisher);
        assert!(state.pending_unpublish.is_empty());
        assert!(!farm_symlink(&fx, "ws-a"));
    }

    #[test]
    fn revived_root_cancels_pending_unpublish() {
        let fx = Fixture::new();
        fx.set_topology("B", usable(), true);
        let mut state = FarmState::default();
        apply_outlet_events(
            &mut state,
            vec![OutletPublicationEvent::RootActivated {
                root_id: "r1".into(),
                name: "ws-a".into(),
                home_node_id: "B".into(),
            }],
            &fx.publisher,
        );
        converge_farm(&mut state, &fx.publisher);
        assert!(state.published.contains("r1"));

        // 删除摘牌失败（ghost 目录占据），名字入欠账。
        std::fs::remove_file(fx.public().join("ws-a")).unwrap();
        std::fs::create_dir(fx.public().join("ws-a")).unwrap();
        apply_outlet_events(
            &mut state,
            vec![OutletPublicationEvent::RootDeleted {
                root_id: "r1".into(),
            }],
            &fx.publisher,
        );
        assert!(state.pending_unpublish.contains_key("r1"));

        // 同名 root 复活：converge 重试摘牌仍失败（目录占据），但 republish
        // 的 publish 路径隔离 ghost 目录后成功亮出。
        apply_outlet_events(
            &mut state,
            vec![OutletPublicationEvent::RootActivated {
                root_id: "r1".into(),
                name: "ws-a".into(),
                home_node_id: "B".into(),
            }],
            &fx.publisher,
        );
        converge_farm(&mut state, &fx.publisher);
        assert!(state.published.contains("r1"));
        assert!(farm_symlink(&fx, "ws-a"));

        // 下一轮收敛：root 已重新亮出，欠账取消——重试会误摘新 symlink。
        converge_farm(&mut state, &fx.publisher);
        assert!(state.pending_unpublish.is_empty());
        assert!(farm_symlink(&fx, "ws-a"), "revived symlink must survive");
    }

    #[test]
    fn degraded_unpublish_failure_is_queued_and_retried() {
        let fx = Fixture::new();
        let remote = "B".to_owned();
        let mut state = FarmState::default();
        state.roots.insert(
            "r1".to_owned(),
            DesiredRoot {
                name: "ws-a".into(),
                home: remote.clone(),
            },
        );
        fx.set_topology(&remote, usable(), true);
        converge_farm(&mut state, &fx.publisher);
        assert!(state.published.contains("r1"));

        // 通路失效但摘牌失败（ghost 目录）：停发 + 名字入欠账。
        std::fs::remove_file(fx.public().join("ws-a")).unwrap();
        std::fs::create_dir(fx.public().join("ws-a")).unwrap();
        fx.set_topology(&remote, degraded(), false);
        converge_farm(&mut state, &fx.publisher);
        assert!(!state.published.contains("r1"));
        assert!(state.pending_unpublish.contains_key("r1"));

        // 目录清理后重试成功（ENOENT 幂等），欠账出队。
        std::fs::remove_dir(fx.public().join("ws-a")).unwrap();
        converge_farm(&mut state, &fx.publisher);
        assert!(state.pending_unpublish.is_empty());
        assert!(!farm_symlink(&fx, "ws-a"));
    }

    #[test]
    fn sweep_staging_removes_all_residue() {
        let fx = Fixture::new();
        let staging = fx.public().join(STAGING_DIR);
        std::os::unix::fs::symlink("/users/ws-a", staging.join("residue-1")).unwrap();
        std::fs::write(staging.join("residue-2"), b"junk").unwrap();
        fx.publisher.sweep_staging().unwrap();
        assert_eq!(std::fs::read_dir(&staging).unwrap().count(), 0);
    }

    #[test]
    fn merge_reconciled_state_counts_drift_and_preserves_pending_unpublish() {
        let mut old = FarmState::default();
        old.roots.insert(
            "r1".to_owned(),
            DesiredRoot {
                name: "ws-a".into(),
                home: "B".to_owned(),
            },
        );
        old.roots.insert(
            "r2".to_owned(),
            DesiredRoot {
                name: "ws-b".into(),
                home: "B".to_owned(),
            },
        );
        old.published.insert("r1".to_owned());
        old.pending_unpublish
            .insert("r-gone".to_owned(), "ws-gone".into());

        let mut reconciled = FarmState::default();
        // r1 home 变化（漂移 +1）；r2 消失（漂移 +1）；r3 新增（漂移 +1）。
        reconciled.roots.insert(
            "r1".to_owned(),
            DesiredRoot {
                name: "ws-a".into(),
                home: "C".to_owned(),
            },
        );
        reconciled.roots.insert(
            "r3".to_owned(),
            DesiredRoot {
                name: "ws-c".into(),
                home: "B".to_owned(),
            },
        );
        reconciled.published.insert("r1".to_owned());

        let (merged, drift) = merge_reconciled_state(old, reconciled);
        assert_eq!(drift, 3);
        assert!(!merged.roots.contains_key("r2"));
        assert!(merged.roots.contains_key("r3"));
        // 摘牌欠账保留：全量对账不清欠账，未复活的条目继续重试。
        assert_eq!(
            merged.pending_unpublish.get("r-gone"),
            Some(&OsString::from("ws-gone"))
        );
    }

    #[test]
    fn merge_reconciled_state_reports_zero_drift_for_identical_states() {
        let mut state = FarmState::default();
        state.roots.insert(
            "r1".to_owned(),
            DesiredRoot {
                name: "ws-a".into(),
                home: "B".to_owned(),
            },
        );
        state.published.insert("r1".to_owned());
        let clone = FarmState {
            roots: state.roots.clone(),
            published: state.published.clone(),
            pending_unpublish: BTreeMap::new(),
        };
        let (merged, drift) = merge_reconciled_state(state, clone);
        assert_eq!(drift, 0);
        assert!(merged.published.contains("r1"));
    }
}
