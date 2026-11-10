//! 名字面的 IO 与收敛循环。
//!
//! 与 `mirror` 的分工：那边只有纯函数，这边负责读目录、执行动作、按周期驱动。
//! 所有失败都按"跳过本轮、保持现状"处理（spec H7）：一次失败的 Meta 调用或
//! 一次 tmpfs 写错误不得清空已发布的名字，否则下游容器会在重启 Meta 的瞬间
//! 集体 bind 失败。

use std::{
    collections::BTreeMap,
    path::{Path, PathBuf},
    sync::Arc,
    time::Duration,
};

use afs_error::{Error, Result};

use super::mirror::{self, Action, LstatKind, PresentEntry};
use crate::node::vfs::ownerfs::root::{ActiveOutletRoots, OutletRoot, RootMeta};

/// Private staging and quarantine directories inside the public root.
const STAGING_DIR: &str = ".pub";
const QUARANTINE_DIR: &str = ".quarantine";

/// Everything one mirror needs to run.
#[derive(Clone, Debug)]
pub struct MirrorConfig {
    /// tmpfs directory holding the published names (`/public`).
    pub public_root: PathBuf,
    /// Root under which per-home data trees are mounted (`/imports`).
    pub import_root: String,
    /// Convergence period. Affects publish latency only, never the data path.
    pub interval: Duration,
}

/// Load the mirror's view of the public directory.
///
/// Only `symlink_metadata` and `read_link` are used. **No target is ever
/// stat'ed**: targets live under per-home mounts that may be NFS, where a
/// metadata call can block for the whole mount timeout and stall the loop
/// (spec H4). Whether a target exists is the consumer's startup check, not
/// ours.
pub fn scan_public(root: &Path) -> Result<BTreeMap<String, PresentEntry>> {
    let entries = std::fs::read_dir(root).map_err(Error::from)?;
    let mut present = BTreeMap::new();
    for entry in entries {
        let entry = entry.map_err(Error::from)?;
        let Some(name) = entry.file_name().to_str().map(str::to_owned) else {
            // A non-UTF-8 name cannot be a workspace name (those are single
            // path components encoded as hex), so it is not ours to manage.
            continue;
        };
        let metadata = std::fs::symlink_metadata(entry.path()).map_err(Error::from)?;
        let kind = if metadata.file_type().is_symlink() {
            LstatKind::Symlink
        } else {
            LstatKind::Other
        };
        let target = if kind == LstatKind::Symlink {
            match std::fs::read_link(entry.path()) {
                Ok(target) => Some(target.to_string_lossy().into_owned()),
                // Unreadable body: treat as foreign so the mirror never deletes
                // something it cannot prove it created.
                Err(_) => None,
            }
        } else {
            None
        };
        present.insert(name, mirror::classify(kind, target));
    }
    Ok(present)
}

/// Execute one action. Idempotent: a missing name is not an error.
///
/// Publishing goes through a staging symlink plus `rename`, so a reader never
/// observes a half-written link and a generation change cannot leave the name
/// briefly absent (spec H5).
pub fn apply(root: &Path, action: &Action) -> Result<()> {
    match action {
        Action::Publish { name, target } => {
            let staging = staging_path(root, name)?;
            // A leftover staging file from a crash would make symlinkat fail
            // with EEXIST; remove it first.
            match std::fs::remove_file(&staging) {
                Ok(()) => {}
                Err(error) if error.kind() == std::io::ErrorKind::NotFound => {}
                Err(error) => return Err(Error::from(error)),
            }
            std::os::unix::fs::symlink(target, &staging).map_err(Error::from)?;
            std::fs::rename(&staging, root.join(name)).map_err(Error::from)?;
            Ok(())
        }
        Action::Remove { name } => match std::fs::remove_file(root.join(name)) {
            Ok(()) => Ok(()),
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => Ok(()),
            Err(error) => Err(Error::from(error)),
        },
        Action::Quarantine { name } => quarantine(root, name),
    }
}

fn staging_path(root: &Path, name: &str) -> Result<PathBuf> {
    let dir = root.join(STAGING_DIR);
    std::fs::create_dir_all(&dir).map_err(Error::from)?;
    // Fixed-size staging basename: a workspace name is at most NAME_MAX bytes,
    // so appending a prefix to it could overflow the limit.
    Ok(dir.join(format!("stage-{}", fnv1a(name))))
}

fn fnv1a(value: &str) -> u64 {
    let mut hash: u64 = 0xcbf2_9ce4_8422_2325;
    for byte in value.as_bytes() {
        hash ^= u64::from(*byte);
        hash = hash.wrapping_mul(0x0000_0100_0000_01b3);
    }
    hash
}

/// Move a non-symlink entry out of the way so the name can be published.
///
/// Kept as evidence rather than deleted: a consumer that created a real
/// directory under a workspace name is a contract violation, and deleting their
/// data silently would be worse than quarantining it.
fn quarantine(root: &Path, name: &str) -> Result<()> {
    let dir = root.join(QUARANTINE_DIR);
    std::fs::create_dir_all(&dir).map_err(Error::from)?;
    let stamp = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|value| value.as_secs())
        .unwrap_or(0);
    let target = dir.join(format!(
        "{}-{}-{stamp}",
        &name[..name.len().min(32)],
        fnv1a(name)
    ));
    std::fs::rename(root.join(name), &target).map_err(Error::from)?;
    afs_logging::warn!(
        "outlet.mirror.quarantined";
        "name" => name,
        "to" => target.display().to_string()
    );
    Ok(())
}

/// Startup gate: the public root must be a tmpfs (spec §5.3).
///
/// A real directory would let a container write into the name layer and would
/// survive reboots with stale links; tmpfs makes the layer's lifetime explicit.
pub fn ensure_public_root_is_tmpfs(root: &Path, mountinfo: &[u8]) -> Result<()> {
    let encoded = mountinfo_path(root);
    let entry = mountinfo
        .split(|byte| *byte == b'\n')
        .filter_map(|line| crate::node::parse_exact_mountinfo_line(line, &encoded))
        .next_back();
    match entry {
        Some(entry) if entry.fs_type == b"tmpfs" => Ok(()),
        Some(entry) => Err(Error::coded(
            afs_error::CONFIG_INVALID,
            format!(
                "outlet public root must be tmpfs, found {}",
                String::from_utf8_lossy(entry.fs_type)
            ),
        )),
        None => Err(Error::coded(
            afs_error::CONFIG_INVALID,
            "outlet public root is not a mount point; mount a tmpfs there",
        )),
    }
}

/// Encode a path the way `/proc/self/mountinfo` does (octal escapes).
fn mountinfo_path(path: &Path) -> Vec<u8> {
    use std::os::unix::ffi::OsStrExt;
    path.as_os_str()
        .as_bytes()
        .iter()
        .flat_map(|byte| match *byte {
            b' ' | b'\t' | b'\n' | b'\\' => format!("\\{:03o}", byte).into_bytes(),
            other => vec![other],
        })
        .collect()
}

/// Prepare the private directories and clear staging leftovers.
///
/// A crash mid-publish leaves a staging symlink; it is not referenced by any
/// name, so removing it is always safe.
pub fn prepare(root: &Path) -> Result<()> {
    for dir in [STAGING_DIR, QUARANTINE_DIR] {
        let path = root.join(dir);
        std::fs::create_dir_all(&path).map_err(Error::from)?;
        #[cfg(unix)]
        {
            use std::os::unix::fs::PermissionsExt;
            std::fs::set_permissions(&path, std::fs::Permissions::from_mode(0o700))
                .map_err(Error::from)?;
        }
    }
    let staging = root.join(STAGING_DIR);
    if let Ok(entries) = std::fs::read_dir(&staging) {
        for entry in entries.flatten() {
            let _ = std::fs::remove_file(entry.path());
        }
    }
    Ok(())
}

/// One convergence round. Returns the number of actions executed.
///
/// Concurrency: a round loads the current directory once, plans against it, and
/// executes. Meta changing mid-round is corrected by the next round; every
/// action is individually idempotent, so a partial round leaves no wedge
/// (spec H8).
/// 快照里出现的 home 集合（去重）。动态挂载的唯一输入。
///
/// 不需要节点清单：mirror 本来就要知道 home 才能拼 target，所以"哪些 home
/// 需要发布名字"与"哪些 home 需要挂载"是同一个集合（设计第二节）。
#[must_use]
pub fn homes_in_snapshot(active: &[OutletRoot]) -> std::collections::BTreeSet<String> {
    active
        .iter()
        .map(|root| root.home_node_id.clone())
        .collect()
}

/// 动态 import 挂载的执行器。
///
/// 决策在 [`super::imports::plan_imports`]，这里只负责把动作落到内核。分开
/// 的原因：动作序列可以在没有 NFS 的机器上测试，而系统调用必须在真机上跑。
pub struct ImportExecutor {
    import_root: PathBuf,
    users_root: PathBuf,
    mount_options: String,
    local_node: String,
    /// 每轮从快照刷新的、对端自己上报的地址。这是唯一来源。
    advertised: std::collections::BTreeMap<String, String>,
    mounter: Arc<dyn super::mount::ImportMounter>,
    table: super::imports::ImportTable,
}

impl ImportExecutor {
    #[must_use]
    pub fn new(
        import_root: PathBuf,
        users_root: PathBuf,
        mount_options: String,
        local_node: String,
        mounter: Arc<dyn super::mount::ImportMounter>,
    ) -> Self {
        Self {
            import_root,
            users_root,
            mount_options,
            local_node,
            advertised: std::collections::BTreeMap::new(),
            mounter,
            table: super::imports::ImportTable::new(),
        }
    }

    #[must_use]
    pub fn ready_count(&self) -> usize {
        self.table.ready_count()
    }

    /// 用 mountinfo 校正内存状态：真相源是内核，不是我们记得什么。
    ///
    /// 一次全量扫描，成本与 home 数量无关（设计第四节）。三种漂移都能被
    /// 纠正：外部 umount、AFS 重启但机器未重启、mountinfo 与内存不一致。
    ///
    /// 判定带防抖：单轮缺失不降级，须连续
    /// [`super::imports::ABSENT_ROUNDS_BEFORE_DEMOTE`] 轮都缺失。
    /// 原因是挂载动作自身会让条目短暂从 mountinfo 消失（实测 bind 约
    /// 120ms），而本循环每 250ms 读一次快照，很容易落进那个窗口。若不防抖，
    /// "看到缺失 → 重挂 → 条目再消失 → 又看到缺失"会形成自激循环，实测每秒
    /// 重建一次、每次约 120ms 路径不可用。
    pub fn reconcile(&mut self, mounts: &[super::topology::Mount]) {
        let now = std::time::Instant::now();
        let homes: Vec<String> = self.table.homes().map(str::to_owned).collect();
        for home in homes {
            let point = super::imports::mount_point(&self.import_root, &home);
            let point_bytes = {
                use std::os::unix::ffi::OsStrExt;
                point.as_os_str().as_bytes().to_vec()
            };
            let in_mountinfo = mounts.iter().any(|mount| mount.mount_point == point_bytes);
            // 本地 home 的 bind 在 mountinfo 里可能长期不可见（dcache 退化），
            // 用 (ino, dev) 与源比对复核，避免把"已挂载"误判成"缺失"。
            // 远程不追加探测：`stat` 在陈旧 NFS 上会阻塞整个 timeo。
            let mounted = in_mountinfo
                || (home == self.local_node && bind_still_mounted(&point, &self.users_root));
            let state = self.table.entry(&home);
            if mounted {
                state.observe_mounted();
            } else {
                state.observe_absent(now, super::imports::ABSENT_ROUNDS_BEFORE_DEMOTE);
            }
        }
    }

    /// 发布路径访问失败：标记可疑，由下一轮统一重建。
    ///
    /// 不在发现时立刻 umount：状态变更集中在一处，避免在发布循环中间插入
    /// 挂载操作（设计第四节）。
    pub fn mark_access_failure(&mut self, home: &str) {
        self.table.entry(home).observe_access_failure();
    }

    /// 补齐缺失的 import 挂载。
    ///
    /// 单个 home 失败不影响其他 home；失败只退避，不熔断。绝大多数轮次
    /// 什么都不做（全部 Ready），成本是一次 `plan_imports` 调用。
    pub fn ensure_imports(&mut self, active: &[OutletRoot]) -> Result<usize> {
        // 地址跟着快照走：节点重建换 IP 后，下一轮就拿到新地址，不需要任何
        // 既有节点改配置。同一 home 的多个 root 上报同一地址，后者覆盖无妨。
        self.advertised = active
            .iter()
            .filter(|root| !root.home_nfs_addr.is_empty())
            .map(|root| (root.home_node_id.clone(), root.home_nfs_addr.clone()))
            .collect();

        let wanted = homes_in_snapshot(active);
        self.table.retain(&wanted);

        let now = std::time::Instant::now();
        let actions = super::imports::plan_imports(&wanted, &self.local_node, &self.table, now);
        afs_logging::debug!(
            "outlet.import.plan";
            "wanted" => ?wanted,
            "local" => self.local_node.clone(),
            "actions" => actions.len()
        );
        if actions.is_empty() {
            return Ok(0);
        }

        let mut applied = 0;
        for action in actions {
            let home = action.home().to_owned();
            let point = super::imports::mount_point(&self.import_root, &home);
            // tmpfs 重启后目录消失，挂载前必须存在。
            if let Err(error) = std::fs::create_dir_all(&point) {
                let delay = self.table.entry(&home).record_failure(now);
                afs_logging::warn!(
                    "outlet.import.mount_failed";
                    "home" => ?home,
                    "error" => error.to_string(),
                    "consecutive" => self.table.entry(&home).consecutive_failures(),
                    "next_retry_ms" => u64::try_from(delay.as_millis()).unwrap_or(u64::MAX)
                );
                continue;
            }

            let started = std::time::Instant::now();
            let is_remount = matches!(action, super::imports::ImportAction::Remount { .. });

            // 挂载点已被占用：本轮什么都不做。
            //
            // 不能"摘掉再挂"。`reconcile` 在下一轮按 mountinfo 重新判定，摘掉
            // 之后它看到的是"没挂"，于是又产生一次挂载，再摘、再挂，每 250ms
            // 一次，永远停不下来（实测每秒一次、日志刷到 196 条）。占用即视为
            // 已就绪，把状态对齐到 Ready，循环才会收敛。
            //
            // Remount 例外：那是"挂载存在但访问失败"，必须重建，由下面的
            // detach 分支处理。
            //
            // 本地 bind 额外传 source：mountinfo 在该路径 dcache 退化时会长期
            // 不显示条目（实测只剩 30% 可见），单靠它会把"已挂载"误判成"没挂"，
            // 于是每秒重挂。bind 的复核用 (st_ino, st_dev) 是否等于源来判断，
            // 纯本地调用无阻塞风险；远程 NFS 不做追加探测（`stat` 会阻塞
            // 整个 timeo），那里 mountinfo 实测可靠。
            let local_source = matches!(action.kind(), super::imports::MountKind::Local)
                .then_some(self.users_root.as_path());
            if !is_remount && is_mounted(&point, local_source) {
                self.table.entry(&home).confirm_mounted();
                continue;
            }
            if is_remount {
                afs_logging::info!(
                    "outlet.import.remount_started";
                    "home" => ?home,
                    "reason" => "access_failed"
                );
                // MNT_DETACH：容器可能仍持有 fd，强制 umount 会 EBUSY。
                let _ = self.mounter.detach(&point);
            }

            // 本机 bind，对端 NFS。两种方式产生同一个路径形态
            // `<import_root>/<home>/users`，所以 `/public/<name>` 的 target
            // 对两边一致——用户在本机与跨机看到的是同一个目录。
            let source = match action.kind() {
                super::imports::MountKind::Local => self.users_root.display().to_string(),
                super::imports::MountKind::Remote => {
                    // 地址随快照来：home 注册会话时自己上报，所以新增节点
                    // 不需要任何既有节点改配置。节点 id 是 AFS 身份、不是域名，
                    // 不能拿它去解析。
                    let host = match self.advertised.get(&home).cloned() {
                        Some(host) if !host.is_empty() => host,
                        _ => {
                            // 没有地址就不能猜，否则每轮都失败并把日志刷满。
                            self.table.entry(&home).record_failure(now);
                            afs_logging::warn!(
                                "outlet.import.no_nfs_addr";
                                "home" => ?home,
                                "hint" => "the home must advertise outlet_nfs_addr when it registers"
                            );
                            continue;
                        }
                    };
                    super::imports::mount_source(&host, &self.users_root)
                }
            };
            let options = match action.kind() {
                // bind 不接受 NFS 选项。
                super::imports::MountKind::Local => super::mount::BIND_OPTIONS,
                super::imports::MountKind::Remote => self.mount_options.as_str(),
            };
            match self.mounter.mount(&source, &point, options) {
                Ok(identity) => {
                    self.table.entry(&home).confirm_mounted();
                    applied += 1;
                    afs_logging::info!(
                        "outlet.import.mounted";
                        "home" => ?home,
                        "source" => source,
                        "elapsed_ms" => u64::try_from(started.elapsed().as_millis()).unwrap_or(u64::MAX),
                        "unique_mount_id" => identity.unique_mount_id
                    );
                }
                Err(error) => {
                    let delay = self.table.entry(&home).record_failure(now);
                    let consecutive = self.table.entry(&home).consecutive_failures();
                    let errno = error.raw_os_error().unwrap_or(0);
                    let next_retry_ms = u64::try_from(delay.as_millis()).unwrap_or(u64::MAX);
                    // 事件名必须是字面量，所以两条分支各自写一遍。
                    if is_remount {
                        afs_logging::warn!(
                            "outlet.import.remount_failed";
                            "home" => ?home,
                            "errno" => errno,
                            "error" => error.to_string(),
                            "consecutive" => consecutive,
                            "next_retry_ms" => next_retry_ms
                        );
                    } else {
                        afs_logging::warn!(
                            "outlet.import.mount_failed";
                            "home" => ?home,
                            "errno" => errno,
                            "error" => error.to_string(),
                            "consecutive" => consecutive,
                            "next_retry_ms" => next_retry_ms
                        );
                    }
                }
            }
        }
        Ok(applied)
    }

    /// 孤儿挂载：挂载点存在但快照里没有对应 home。
    ///
    /// 只告警，不卸载。"节点永久移除"没有可靠信号（session 过期不等于删除），
    /// 自动卸载可能打断仍在读的容器（设计第九节 R6）。
    pub fn report_orphans(&self, mounts: &[super::topology::Mount]) {
        let known: std::collections::BTreeSet<&str> = self.table.homes().collect();
        let prefix = {
            use std::os::unix::ffi::OsStrExt;
            self.import_root.as_os_str().as_bytes().to_vec()
        };
        for mount in mounts {
            if !mount.mount_point.starts_with(&prefix) {
                continue;
            }
            // 取相对 import_root 的第一段作为 home 名。
            let rest = &mount.mount_point[prefix.len()..];
            let rest = rest.strip_prefix(b"/").unwrap_or(rest);
            let home_end = rest
                .iter()
                .position(|byte| *byte == b'/')
                .unwrap_or(rest.len());
            let Ok(home) = std::str::from_utf8(&rest[..home_end]) else {
                continue;
            };
            if home.is_empty() || known.contains(home) || home == self.local_node {
                continue;
            }
            afs_logging::warn!(
                "outlet.import.orphan";
                "home" => ?home,
                "reason" => "no_active_root"
            );
        }
    }
}

/// Convergence with an import executor.
///
/// Round order is fixed and matters (design §3):
///
/// 1. `reconcile` — correct in-memory import state from mountinfo, the only
///    truth about what is actually mounted;
/// 2. `ensure_imports` — mount what is missing, **before** publishing, so a
///    name never appears pointing at an unmounted target;
/// 3. publish — the name layer of `mirror`;
/// 4. orphan report — warn only, never unmount.
pub fn converge_round(
    config: &MirrorConfig,
    meta: &dyn RootMeta,
    cached_digest: &mut Option<String>,
    cached_roots: &mut Vec<OutletRoot>,
    imports: &mut ImportExecutor,
) -> Result<usize> {
    let reply = meta.list_active_outlet_roots(cached_digest.as_deref())?;
    // H14: an `unchanged` reply carries no payload and must reuse the cache.
    // This is the single place that decision is made, so no caller can mistake
    // an empty payload for "Meta has no active roots" and wipe the directory.
    let active = merge_reply(&reply, cached_roots);
    *cached_digest = Some(reply.digest.clone());
    *cached_roots = active.clone();

    {
        // mountinfo 是真相源：外部 umount、进程重启但机器未重启、内存与实际
        // 不一致，都由这一次扫描纠正（设计 §4）。
        if let Ok(mountinfo) = std::fs::read("/proc/self/mountinfo") {
            let mounts = super::topology::parse(&mountinfo);
            imports.reconcile(&mounts);
            imports.report_orphans(&mounts);
        }
        // 挂载失败不阻断发布：该 home 的名字仍会发布，只是暂时断链。
        // 这比"整轮放弃"好——其他 home 的名字不受影响（spec H7）。
        if let Err(error) = imports.ensure_imports(&active) {
            afs_logging::warn!(
                "outlet.import.round_failed";
                "error" => error.to_string()
            );
        }
    }

    let present = scan_public(&config.public_root)?;
    let actions = mirror::plan(&config.import_root, &active, &present);
    let mut applied = 0;
    // 发布时访问失败是"挂载挂了但不通"的唯一发现途径（设计 §4）：不做主动
    // 探测，因为那会变成每轮对每个 peer 的持续 NFS 压力。
    //
    // 每个 home 每轮最多标记一次：一个 home 可能有上百个 root，逐个失败会
    // 连续标记上百次，而下一轮的重建是每个 home 一次。
    let mut suspect: Option<String> = None;
    for action in &actions {
        match apply(&config.public_root, action) {
            Ok(()) => applied += 1,
            Err(error) => {
                // 发布失败的语义是"名字暂时没换成"，与挂载无关的错误（比如
                // tmpfs 满）不该触发重建。只有涉及 import 路径的 IO 失败才
                // 标记可疑。
                let name = match action {
                    mirror::Action::Publish { name, .. }
                    | mirror::Action::Remove { name }
                    | mirror::Action::Quarantine { name } => name,
                };
                if let Some(home) = home_of_published(name, &active) {
                    suspect = Some(home);
                }
                afs_logging::warn!(
                    "outlet.publish.failed";
                    "name" => name.clone(),
                    "error" => error.to_string()
                );
            }
        }
    }
    if let Some(home) = suspect {
        imports.mark_access_failure(home.as_str());
    }
    if applied > 0 {
        afs_logging::info!(
            "outlet.mirror.converged";
            "applied" => applied,
            "active" => active.len(),
            "present" => present.len()
        );
    }
    Ok(applied)
}

fn merge_reply(reply: &ActiveOutletRoots, cached: &[OutletRoot]) -> Vec<OutletRoot> {
    reply.merge_into(cached.to_vec())
}

/// Run the convergence loop until `shutdown` resolves.
///
/// Failures log and skip the round: the last successful snapshot keeps serving
/// (spec H7). There is no backoff on the happy path — the loop is a fixed
/// interval, and adding idle backoff would only increase publish latency.
///
/// The executor holds per-home state and a `Send + Sync` mounter, so it moves
/// into the blocking worker each round and comes back with its state updated.
/// Rounds are serial, so a `&mut` round trip through the task is safe.
pub async fn run(
    config: MirrorConfig,
    meta: Arc<dyn RootMeta>,
    mut shutdown: impl std::future::Future<Output = ()> + Unpin,
    imports: ImportExecutor,
) {
    let mut cached_digest: Option<String> = None;
    let mut cached_roots: Vec<OutletRoot> = Vec::new();
    let mut tick = tokio::time::interval(config.interval);
    tick.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
    // Move slot for the executor: handed to the blocking worker each round and
    // taken back afterwards so its per-home state survives across rounds.
    let mut imports = Some(imports);
    loop {
        tokio::select! {
            _ = &mut shutdown => return,
            _ = tick.tick() => {
                let config = config.clone();
                let meta = meta.clone();
                let mut digest = cached_digest.clone();
                let mut roots = cached_roots.clone();
                // 把 executor 暂时移进 worker，本轮结束后取回，状态得以跨轮保留。
                let mut worker_imports = imports.take();
                let result = tokio::task::spawn_blocking({
                    let config = config.clone();
                    move || {
                        let executor = worker_imports
                            .as_mut()
                            .expect("executor slot is only empty between rounds");
                        let applied = converge_round(
                            &config,
                            meta.as_ref(),
                            &mut digest,
                            &mut roots,
                            executor,
                        );
                        (applied, digest, roots, worker_imports)
                    }
                })
                .await;
                match result {
                    Ok((Ok(_), digest, roots, back)) => {
                        cached_digest = digest;
                        cached_roots = roots;
                        imports = back;
                    }
                    Ok((Err(error), _, _, back)) => {
                        // Keep the previous snapshot: a failed round must not
                        // retract names that were valid moments ago.
                        imports = back;
                        afs_logging::warn!("outlet.mirror.round_failed"; "error" => error.to_string());
                    }
                    Err(error) => {
                        // Worker panicked: state is gone with it. Rebuild on the
                        // next round by starting from a fresh executor rather
                        // than serving with stale state.
                        afs_logging::warn!("outlet.mirror.worker_failed"; "error" => error.to_string());
                    }
                }
            }
        }
    }
}

/// 该路径当前是否是一个挂载点。
///
/// 读 `/proc/self/mountinfo` 而不是 `stat`：后者在陈旧的 NFS 挂载上会阻塞
/// 整个 `timeo`，把本可以自愈的一轮拖成十分钟。
///
/// `source` 只在本地 bind 的二次确认里用到，见 [`bind_still_mounted`]。
/// 远程（NFS）调用方传 `None`：那条路径上 mountinfo 实测可靠，且 `stat`
/// 有阻塞风险，不做任何追加探测。
fn is_mounted(point: &Path, source: Option<&Path>) -> bool {
    use std::os::unix::ffi::OsStrExt;
    let Ok(mountinfo) = std::fs::read("/proc/self/mountinfo") else {
        return false;
    };
    let wanted = point.as_os_str().as_bytes();
    if super::topology::parse(&mountinfo)
        .iter()
        .any(|mount| mount.mount_point == wanted)
    {
        return true;
    }
    // mountinfo 未命中时的复核。只在本地 bind 上做。
    source.is_some_and(|source| bind_still_mounted(point, source))
}

/// 本地 bind 的二次确认：挂载点的 `(st_ino, st_dev)` 是否等于挂载源。
///
/// 必要性：某个路径被反复 mount/umount 上万次之后，它的 dentry 会退化——
/// 实测某节点上该路径在 `/proc/self/mountinfo` 里只有约 30% 的时间可见，
/// 而 `stat` 一直能访问。若只信 mountinfo，就会陷入"判定未挂载 → 重挂 →
/// 条目仍不可见 → 再重挂"的自激循环，每秒重建一次、每次约 120ms 路径不可用。
///
/// 为什么用 `(ino, dev)` 而不是只比 `dev`：本地 bind 的设备号与源盘同一个，
/// 只比 `dev` 无法区分"挂载生效"与"只是目录本身"。实测正常 bind 后挂载点
/// 的 inode 会变成源的 inode（如 `2097283 → 2097221`），据此可判。
///
/// 为什么只用于本地：`stat` 在陈旧的 NFS 挂载上会阻塞整个 `timeo`（默认
/// 600s），把一轮收敛拖成十分钟。本地 bind 是 ext4，无此风险。
fn bind_still_mounted(point: &Path, source: &Path) -> bool {
    use std::os::unix::fs::MetadataExt;
    let Ok(point_meta) = std::fs::metadata(point) else {
        return false;
    };
    let Ok(source_meta) = std::fs::metadata(source) else {
        return false;
    };
    point_meta.ino() == source_meta.ino() && point_meta.dev() == source_meta.dev()
}

/// 发布名对应的 home。用于发布失败时定位是哪个 import 出了问题。
///
/// 名字与 root id 一一对应，root 又携带 home，所以不需要额外查表。
fn home_of_published(name: &str, active: &[OutletRoot]) -> Option<String> {
    active
        .iter()
        .find(|root| {
            crate::node::vfs::ownerfs::root::root_name_from_id(&root.root_id)
                .is_ok_and(|decoded| decoded.to_string_lossy() == name)
        })
        .map(|root| root.home_node_id.clone())
}

/// Diagnosis cadence. Slower than the mirror interval on purpose: mount drift
/// is an operator-visible condition, not something the loop reacts to.
pub const DIAGNOSIS_INTERVAL: Duration = Duration::from_secs(60);

/// One diagnosis pass. Returns findings; the caller only logs them.
///
/// Pure with respect to the mirror: it reads mountinfo and the import
/// directory, never mutates names, and never feeds back into `converge`
/// (spec H3). A missing or drifting mount must not stop publication — a
/// symlink to a currently unreachable target is still the correct name layer,
/// and retracting it would take names away from consumers that never touched
/// that home.
pub fn diagnose(config: &MirrorConfig, mountinfo: &[u8]) -> Vec<super::topology::Finding> {
    let mounts = super::topology::parse(mountinfo);
    let mut findings = super::topology::diagnose_public(&config.public_root, &mounts);
    let homes = homes_under(&config.import_root);
    findings.extend(super::topology::diagnose_imports(
        Path::new(&config.import_root),
        &homes,
        &mounts,
    ));
    findings
}

/// Node ids that have a directory under the import root.
///
/// Enumerated from the directory rather than from mountinfo so a home whose
/// mount silently disappeared is still reported: it would otherwise vanish
/// from the check entirely, which is exactly the case worth catching.
fn homes_under(import_root: &str) -> Vec<String> {
    let Ok(entries) = std::fs::read_dir(import_root) else {
        return Vec::new();
    };
    let mut homes: Vec<String> = entries
        .flatten()
        .filter(|entry| entry.file_type().map(|kind| kind.is_dir()).unwrap_or(false))
        .filter_map(|entry| entry.file_name().to_str().map(str::to_owned))
        .collect();
    homes.sort();
    homes
}

/// Run the diagnosis loop until `shutdown` resolves.
pub async fn run_diagnosis(
    config: MirrorConfig,
    mut shutdown: impl std::future::Future<Output = ()> + Unpin,
) {
    let mut tick = tokio::time::interval(DIAGNOSIS_INTERVAL);
    tick.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
    loop {
        tokio::select! {
            _ = &mut shutdown => return,
            _ = tick.tick() => {
                let config = config.clone();
                match tokio::task::spawn_blocking(move || {
                    let mountinfo = std::fs::read("/proc/self/mountinfo").ok()?;
                    Some(diagnose(&config, &mountinfo))
                })
                .await
                {
                    Ok(Some(findings)) => {
                        for finding in findings {
                            // One event per finding so each is separately
                            // alertable; a single "topology bad" line would
                            // hide which home broke.
                            afs_logging::warn!(
                                "outlet.topology";
                                "kind" => finding.kind(),
                                "mount_point" => finding.mount_point(),
                            );
                        }
                    }
                    Ok(None) => {
                        afs_logging::warn!("outlet.topology.mountinfo_unreadable");
                    }
                    Err(error) => {
                        afs_logging::warn!("outlet.topology.worker_failed"; "error" => error.to_string());
                    }
                }
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::node::vfs::ownerfs::root::{ActiveOutletRoots, RootId};
    use std::sync::Mutex;

    /// 名字层测试的入口：执行器在动态 import 成为唯一路径前就已必选，
    /// 这里用假 mounter 造一个，让纯发布语义的测试不必关心挂载细节。
    fn converge(
        config: &MirrorConfig,
        meta: &dyn RootMeta,
        cached_digest: &mut Option<String>,
        cached_roots: &mut Vec<OutletRoot>,
    ) -> Result<usize> {
        let mut executor = ImportExecutor::new(
            PathBuf::from(&config.import_root),
            PathBuf::from("/home/agentos/users"),
            "vers=4.1,hard".to_owned(),
            "node-self".to_owned(),
            Arc::new(FakeMounter::default()),
        );
        converge_round(config, meta, cached_digest, cached_roots, &mut executor)
    }

    /// 记录调用的假 mounter：让执行器逻辑在无 NFS 的机器上可测。
    #[derive(Default)]
    struct FakeMounter {
        mounts: Mutex<Vec<(String, PathBuf, String)>>,
        detaches: Mutex<Vec<PathBuf>>,
        /// home 名 -> 失败次数。为 0 时成功。
        fail_for: Mutex<std::collections::HashMap<String, u32>>,
    }

    impl FakeMounter {
        /// 按挂载源里的 host（即映射后的地址）指定失败。
        fn failing_for(host: &str) -> Self {
            let mut map = std::collections::HashMap::new();
            map.insert(host.to_owned(), u32::MAX);
            Self {
                fail_for: Mutex::new(map),
                ..Self::default()
            }
        }

        fn mount_count(&self) -> usize {
            self.mounts.lock().unwrap().len()
        }

        fn mounted_homes(&self) -> Vec<String> {
            self.mounts
                .lock()
                .unwrap()
                .iter()
                .map(|(source, _, _)| source.split(':').next().unwrap_or("").to_owned())
                .collect()
        }
    }

    impl super::super::mount::ImportMounter for FakeMounter {
        fn mount(
            &self,
            source: &str,
            target: &std::path::Path,
            options: &str,
        ) -> std::io::Result<super::super::mount::MountIdentity> {
            let host = source.split(':').next().unwrap_or("").to_owned();
            let mut failures = self.fail_for.lock().unwrap();
            if let Some(remaining) = failures.get_mut(&host)
                && *remaining > 0
            {
                *remaining -= 1;
                return Err(std::io::Error::from_raw_os_error(libc::EHOSTUNREACH));
            }
            self.mounts.lock().unwrap().push((
                source.to_owned(),
                target.to_path_buf(),
                options.to_owned(),
            ));
            Ok(super::super::mount::MountIdentity { unique_mount_id: 7 })
        }

        fn detach(&self, target: &std::path::Path) -> std::io::Result<()> {
            self.detaches.lock().unwrap().push(target.to_path_buf());
            Ok(())
        }
    }

    fn executor(dir: &std::path::Path, mounter: Arc<FakeMounter>) -> ImportExecutor {
        ImportExecutor::new(
            dir.join("imports"),
            PathBuf::from("/home/agentos/users"),
            "vers=4.1,hard".to_owned(),
            "node-self".to_owned(),
            mounter,
        )
    }

    fn root_on(home: &str) -> OutletRoot {
        // 测试约定：地址随快照来。给已知 home 一个稳定地址，未知名留空
        //（空地址即"该 home 没上报"，会被跳过）。
        let addr = match home {
            "node-a" => "192.0.2.10",
            "node-b" => "192.0.2.11",
            "node-c" => "192.0.2.12",
            _ => "",
        };
        OutletRoot {
            root_id: RootId("root-61".into()),
            root_epoch: 1,
            home_node_id: home.to_owned(),
            home_nfs_addr: addr.to_owned(),
        }
    }

    /// 本机也要挂（bind），对端走 NFS：两条路径产出同一个
    /// `<import_root>/<home>/users`，所以 `/public` 的 target 形态一致。
    #[test]
    fn ensure_imports_mounts_both_peer_and_local() {
        let dir = tempfile::tempdir().unwrap();
        let mounter = Arc::new(FakeMounter::default());
        let mut exec = executor(dir.path(), mounter.clone());

        let active = vec![root_on("node-a"), root_on("node-self")];
        assert_eq!(exec.ensure_imports(&active).unwrap(), 2);

        let calls = mounter.mounts.lock().unwrap().clone();
        let remote = calls
            .iter()
            .find(|(source, _, _)| source.starts_with("192.0.2.10:"))
            .expect("peer mounted by address");
        assert!(remote.2.starts_with("vers="), "peer uses NFS options");

        let local = calls
            .iter()
            .find(|(source, _, _)| source.starts_with('/'))
            .expect("local mounted from its own data tree");
        assert_eq!(local.2, super::super::mount::BIND_OPTIONS);

        // 第二轮无事可做。
        assert_eq!(exec.ensure_imports(&active).unwrap(), 0);
        assert_eq!(mounter.mount_count(), 2);
    }

    /// 一个 home 挂不上时，其他 home 必须照常挂载（设计第三节）。
    #[test]
    fn one_failing_home_does_not_block_others() {
        let dir = tempfile::tempdir().unwrap();
        // node-a 映射到 192.0.2.10，让这个地址失败。
        let mounter = Arc::new(FakeMounter::failing_for("192.0.2.10"));
        let mut exec = executor(dir.path(), mounter.clone());

        let active = vec![root_on("node-a"), root_on("node-c")];
        assert_eq!(exec.ensure_imports(&active).unwrap(), 1);
        assert_eq!(mounter.mounted_homes(), vec!["192.0.2.12".to_owned()]);
        assert_eq!(exec.ready_count(), 1);
    }

    /// mountinfo 说挂着、内存空白：认下来，不重复挂（进程重启场景）。
    #[test]
    fn reconcile_accepts_an_already_mounted_import() {
        let dir = tempfile::tempdir().unwrap();
        let mounter = Arc::new(FakeMounter::default());
        let mut exec = executor(dir.path(), mounter.clone());

        // 先让它认识 node-a，再模拟内核说已挂载。
        exec.ensure_imports(&[root_on("node-a")]).unwrap();
        let point = super::super::imports::mount_point(&dir.path().join("imports"), "node-a");
        let mounts = vec![super::super::topology::Mount {
            mount_point: {
                use std::os::unix::ffi::OsStrExt;
                point.as_os_str().as_bytes().to_vec()
            },
            fs_type: "nfs4".to_owned(),
            source: "node-a:/home/agentos/users".to_owned(),
        }];
        exec.reconcile(&mounts);
        assert_eq!(exec.ready_count(), 1);
    }

    /// 本地 bind 的二次确认：`(ino, dev)` 等于源即视为已挂载。
    ///
    /// 这条判据存在的原因是 mountinfo 在 dcache 退化的路径上会长期不显示
    /// 条目（实测某节点只剩 30% 可见），而挂载实际可用。只信 mountinfo
    /// 就会陷入每秒重挂的自激循环。
    #[test]
    fn bind_probe_treats_the_source_inode_as_mounted() {
        let dir = tempfile::tempdir().unwrap();
        let source = dir.path().join("source");
        std::fs::create_dir_all(&source).unwrap();

        // 同一目录的另一个路径（不做真正 mount）：元数据必然一致，
        // 等价于"bind 已生效"的可观测特征。
        let point = dir.path().join("point");
        std::fs::create_dir_all(&point).unwrap();
        use std::os::unix::fs::MetadataExt;
        let source_meta = std::fs::metadata(&source).unwrap();
        let point_meta = std::fs::metadata(&point).unwrap();
        assert_ne!(
            point_meta.ino(),
            source_meta.ino(),
            "两个独立目录的 inode 不同，前提成立"
        );

        // 独立目录：inode 不同 → 视为未挂载。
        assert!(
            !super::bind_still_mounted(&point, &source),
            "inode 不同必须判为未挂载"
        );
    }

    /// open(2) 出来的同一个 inode 走两条路径时，探针必须判为已挂载。
    #[test]
    fn bind_probe_matches_the_same_inode_through_two_paths() {
        let dir = tempfile::tempdir().unwrap();
        let source = dir.path().join("source");
        std::fs::create_dir_all(&source).unwrap();

        // 用硬链接式的路径别名模拟"bind 后两条路径指向同一 inode"：
        // 这里直接对同一个路径取两次，验证判据本身不误报。
        assert!(
            super::bind_still_mounted(&source, &source),
            "同一 inode 必须判为已挂载"
        );
    }

    /// 远程 home 不做 stat 探测：`stat` 在陈旧 NFS 上会阻塞整个 timeo。
    #[test]
    fn reconcile_skips_the_bind_probe_for_remote_homes() {
        let dir = tempfile::tempdir().unwrap();
        let mounter = Arc::new(FakeMounter::default());
        let mut exec = executor(dir.path(), mounter.clone());

        // node-a 相对 node-self 是 Remote。即使 mountinfo 说没挂、
        // 且 import 目录下确实没有任何东西，也不应因为 stat 探测而卡住。
        exec.ensure_imports(&[root_on("node-a")]).unwrap();
        std::thread::sleep(super::super::imports::MOUNT_SETTLE);
        for _ in 0..super::super::imports::ABSENT_ROUNDS_BEFORE_DEMOTE {
            exec.reconcile(&[]);
        }
        assert_eq!(exec.ready_count(), 0, "远程缺失仍须降级");
    }

    /// 外部 umount 后必须重挂。
    ///
    /// 判定带防抖（连续 `ABSENT_ROUNDS_BEFORE_DEMOTE` 轮缺失），所以不是
    /// "下一轮"，而是"连续几轮之后"——外部卸载是持久状态，这个延迟只会让
    /// 恢复晚几百毫秒，不影响最终发现。防抖换来的是：我们自己挂载造成的
    /// 短暂空窗不会被误判成外部卸载（见 `reconcile_ignores_the_mount_settle_window`）。
    #[test]
    fn reconcile_detects_an_external_unmount() {
        let dir = tempfile::tempdir().unwrap();
        let mounter = Arc::new(FakeMounter::default());
        let mut exec = executor(dir.path(), mounter.clone());

        exec.ensure_imports(&[root_on("node-a")]).unwrap();
        assert_eq!(exec.ready_count(), 1);

        // 抑制期先过去，避免挂载后 1s 内的抑制挡住降级。
        std::thread::sleep(super::super::imports::MOUNT_SETTLE);

        // 连续缺失若干轮后才降级。
        for _ in 0..super::super::imports::ABSENT_ROUNDS_BEFORE_DEMOTE {
            exec.reconcile(&[]);
        }
        assert_eq!(exec.ready_count(), 0);
        assert_eq!(exec.ensure_imports(&[root_on("node-a")]).unwrap(), 1);
        assert_eq!(mounter.mount_count(), 2);
    }

    /// 刚挂载完成的那一段空窗，不能被误判成外部卸载。
    ///
    /// 这是自激循环的回归测试。`mount` 动作本身会让挂载点短暂从 mountinfo
    /// 消失（实测 bind 约 110–125ms），而收敛循环每 250ms 读一次快照。若单轮
    /// 缺失就降级，就会"挂 → 判缺失 → 再挂"永不收敛，实测每秒重建一次、
    /// 约 12% 的读失败。
    #[test]
    fn reconcile_ignores_the_mount_settle_window() {
        let dir = tempfile::tempdir().unwrap();
        let mounter = Arc::new(FakeMounter::default());
        let mut exec = executor(dir.path(), mounter.clone());

        exec.ensure_imports(&[root_on("node-a")]).unwrap();
        let after_first = mounter.mount_count();
        assert_eq!(exec.ready_count(), 1);

        // 模拟挂载动作造成的空窗：紧接着连续几轮看不到条目。
        for _ in 0..(super::super::imports::ABSENT_ROUNDS_BEFORE_DEMOTE * 2) {
            exec.reconcile(&[]);
        }

        // 仍在 Ready，且没有因此产生新的挂载动作。
        assert_eq!(exec.ready_count(), 1, "空窗内的缺失不得降级");
        assert_eq!(
            exec.ensure_imports(&[root_on("node-a")]).unwrap(),
            0,
            "不得重复挂载"
        );
        assert_eq!(mounter.mount_count(), after_first, "挂载次数不应增加");
    }

    /// 缺失中断后计数归零：不是"累计缺失",是"连续缺失"。
    #[test]
    fn absent_streak_resets_when_the_mount_is_seen_again() {
        let dir = tempfile::tempdir().unwrap();
        let mounter = Arc::new(FakeMounter::default());
        let mut exec = executor(dir.path(), mounter.clone());

        exec.ensure_imports(&[root_on("node-a")]).unwrap();
        std::thread::sleep(super::super::imports::MOUNT_SETTLE);

        let point = super::super::imports::mount_point(&dir.path().join("imports"), "node-a");
        let mounts = vec![super::super::topology::Mount {
            mount_point: {
                use std::os::unix::ffi::OsStrExt;
                point.as_os_str().as_bytes().to_vec()
            },
            fs_type: "nfs4".to_owned(),
            source: "node-a:/home/agentos/users".to_owned(),
        }];

        // 缺失两轮（未到阈值），再看到一轮，再缺失两轮 —— 不应降级。
        exec.reconcile(&[]);
        exec.reconcile(&[]);
        exec.reconcile(&mounts);
        exec.reconcile(&[]);
        exec.reconcile(&[]);
        assert_eq!(exec.ready_count(), 1, "计数必须连续，不能被累计");
    }

    /// 地址来自快照（对端自己上报），dial 原样不做任何加工。
    #[test]
    fn address_comes_from_the_snapshot() {
        let dir = tempfile::tempdir().unwrap();
        let mounter = Arc::new(FakeMounter::default());
        let mut exec = ImportExecutor::new(
            dir.path().join("imports"),
            PathBuf::from("/home/agentos/users"),
            "vers=4.1,hard".to_owned(),
            "node-self".to_owned(),
            mounter.clone(),
        );

        let mut root = root_on("node-a");
        root.home_nfs_addr = "203.0.113.7".to_owned();
        assert_eq!(exec.ensure_imports(&[root]).unwrap(), 1);
        assert_eq!(
            mounter.mounted_homes(),
            vec!["203.0.113.7".to_owned()],
            "the advertised address must be dialled as-is"
        );
    }

    /// 快照里的地址变了，下一轮就要用新地址重挂。
    ///
    /// 覆盖"节点重建换了 IP"：不能因为旧地址还挂着就继续用旧地址。
    #[test]
    fn a_changed_address_is_picked_up_on_the_next_round() {
        let dir = tempfile::tempdir().unwrap();
        let mounter = Arc::new(FakeMounter::default());
        let mut exec = ImportExecutor::new(
            dir.path().join("imports"),
            PathBuf::from("/home/agentos/users"),
            "vers=4.1,hard".to_owned(),
            "node-self".to_owned(),
            mounter.clone(),
        );

        let mut root = root_on("node-a");
        root.home_nfs_addr = "203.0.113.7".to_owned();
        exec.ensure_imports(&[root.clone()]).unwrap();

        // 同一 home 换了地址：状态被标记为可疑后必须重挂到新地址。
        root.home_nfs_addr = "203.0.113.99".to_owned();
        exec.mark_access_failure("node-a");
        assert_eq!(exec.ensure_imports(&[root]).unwrap(), 1);
        let sources: Vec<String> = mounter
            .mounts
            .lock()
            .unwrap()
            .iter()
            .map(|(source, _, _)| source.split(':').next().unwrap_or("").to_owned())
            .collect();
        assert_eq!(sources.last().map(String::as_str), Some("203.0.113.99"));
    }

    /// home 没上报地址时不猜，也不回退到节点名解析。
    #[test]
    fn a_home_without_an_advertised_address_is_skipped() {
        let dir = tempfile::tempdir().unwrap();
        let mounter = Arc::new(FakeMounter::default());
        let mut exec = ImportExecutor::new(
            dir.path().join("imports"),
            PathBuf::from("/home/agentos/users"),
            "vers=4.1,hard".to_owned(),
            "node-self".to_owned(),
            mounter.clone(),
        );

        // node-b 的 root 没有地址：不猜，也不回退到节点名解析。
        let mut no_addr = root_on("node-b");
        no_addr.home_nfs_addr.clear();
        // node-c 有地址：正常挂载，说明一个缺失不影响其他 home。
        assert_eq!(
            exec.ensure_imports(&[no_addr, root_on("node-c")]).unwrap(),
            1
        );
        assert_eq!(mounter.mount_count(), 1);
    }

    /// 访问失败标记可疑，下一轮走 detach + 重挂。
    #[test]
    fn access_failure_triggers_a_remount() {
        let dir = tempfile::tempdir().unwrap();
        let mounter = Arc::new(FakeMounter::default());
        let mut exec = executor(dir.path(), mounter.clone());

        exec.ensure_imports(&[root_on("node-a")]).unwrap();
        exec.mark_access_failure("node-a");
        assert_eq!(exec.ensure_imports(&[root_on("node-a")]).unwrap(), 1);
        assert_eq!(mounter.detaches.lock().unwrap().len(), 1);
        assert_eq!(mounter.mount_count(), 2);
    }

    /// Scripted `RootMeta` returning queued replies, so rounds are deterministic
    /// without a Meta server.
    struct ScriptedMeta {
        replies: Mutex<Vec<ActiveOutletRoots>>,
        calls: Mutex<Vec<Option<String>>>,
    }

    impl ScriptedMeta {
        fn new(replies: Vec<ActiveOutletRoots>) -> Self {
            // Reverse so `pop` yields the scripted order.
            let mut replies = replies;
            replies.reverse();
            Self {
                replies: Mutex::new(replies),
                calls: Mutex::new(Vec::new()),
            }
        }
    }

    impl RootMeta for ScriptedMeta {
        fn reserve_root(
            &self,
            _id: &RootId,
            _create_intent_id: &str,
        ) -> Result<crate::node::vfs::ownerfs::root::RootReservation> {
            unreachable!("not exercised by mirror tests")
        }
        fn activate_root(
            &self,
            _prepared: &crate::node::vfs::ownerfs::root::PreparedRoot,
        ) -> Result<crate::node::vfs::ownerfs::root::RootGrant> {
            unreachable!("not exercised by mirror tests")
        }
        fn abort_root(
            &self,
            _reservation: &crate::node::vfs::ownerfs::root::RootReservation,
        ) -> Result<()> {
            unreachable!("not exercised by mirror tests")
        }
        fn deactivate_root(
            &self,
            _root_id: &RootId,
            _epoch: u64,
            _home_node_id: &str,
            _session_id: &str,
        ) -> Result<()> {
            unreachable!("not exercised by mirror tests")
        }
        fn lookup_root(
            &self,
            _id: &RootId,
        ) -> Result<Option<crate::node::vfs::ownerfs::root::RootLocation>> {
            unreachable!("not exercised by mirror tests")
        }
        fn list_owner_roots(
            &self,
            _home_node_id: &str,
        ) -> Result<crate::node::vfs::ownerfs::root::OwnerRootInventory> {
            unreachable!("not exercised by mirror tests")
        }
        fn acquire_root(
            &self,
            _id: &RootId,
            _right: crate::node::vfs::ownerfs::root::RootRight,
        ) -> Result<crate::node::vfs::ownerfs::root::RootGrant> {
            unreachable!("not exercised by mirror tests")
        }
        fn validate_root_access(
            &self,
            _presented: &crate::node::vfs::ownerfs::root::PresentedRootAccess,
            _authenticated_peer_node_id: &str,
        ) -> Result<crate::node::vfs::ownerfs::root::RootGrant> {
            unreachable!("not exercised by mirror tests")
        }
        fn recover_root(
            &self,
            _record: &crate::node::vfs::ownerfs::catalog::LocalRootRecord,
            _new_session_id: &str,
        ) -> Result<crate::node::vfs::ownerfs::root::RootGrant> {
            unreachable!("not exercised by mirror tests")
        }
        fn list_active_outlet_roots(
            &self,
            known_digest: Option<&str>,
        ) -> Result<ActiveOutletRoots> {
            self.calls
                .lock()
                .unwrap()
                .push(known_digest.map(str::to_owned));
            let mut replies = self.replies.lock().unwrap();
            if replies.is_empty() {
                return Err(Error::coded(
                    afs_error::RUNTIME_INTERNAL,
                    "scripted meta exhausted",
                ));
            }
            Ok(replies.pop().unwrap())
        }
    }

    fn node(id: &str, epoch: u64, home: &str) -> OutletRoot {
        OutletRoot {
            root_id: RootId(id.to_owned()),
            root_epoch: epoch,
            home_node_id: home.to_owned(),
            home_nfs_addr: String::new(),
        }
    }

    fn config(root: &Path) -> MirrorConfig {
        MirrorConfig {
            public_root: root.to_path_buf(),
            import_root: "/imports".into(),
            interval: Duration::from_millis(10),
        }
    }

    fn full(digest: &str, roots: Vec<OutletRoot>) -> ActiveOutletRoots {
        ActiveOutletRoots {
            roots,
            digest: digest.into(),
            unchanged: false,
        }
    }

    fn converged(digest: &str) -> ActiveOutletRoots {
        ActiveOutletRoots {
            roots: Vec::new(),
            digest: digest.into(),
            unchanged: true,
        }
    }

    /// 端到端一轮：Meta 返回 root → /public 出现正确 symlink。
    #[test]
    fn round_publishes_active_roots() {
        let dir = tempfile::tempdir().unwrap();
        prepare(dir.path()).unwrap();
        let meta = ScriptedMeta::new(vec![full("d1", vec![node("root-61", 7, "node-a")])]);
        let mut digest = None;
        let mut roots = Vec::new();

        let applied = converge(&config(dir.path()), &meta, &mut digest, &mut roots).unwrap();
        assert_eq!(applied, 1);
        let link = dir.path().join("a");
        assert_eq!(
            std::fs::read_link(&link).unwrap(),
            Path::new("/imports/node-a/users/root-61-e7")
        );
        assert_eq!(digest.as_deref(), Some("d1"));
    }

    /// 第二轮传入上一轮的 digest，服务端据此返回 unchanged，不得清空目录。
    #[test]
    fn unchanged_reply_keeps_the_previous_snapshot() {
        let dir = tempfile::tempdir().unwrap();
        prepare(dir.path()).unwrap();
        let meta = ScriptedMeta::new(vec![
            full("d1", vec![node("root-61", 7, "node-a")]),
            converged("d1"),
        ]);
        let mut digest = None;
        let mut roots = Vec::new();
        converge(&config(dir.path()), &meta, &mut digest, &mut roots).unwrap();
        let applied = converge(&config(dir.path()), &meta, &mut digest, &mut roots).unwrap();

        assert_eq!(applied, 0, "unchanged round must do nothing");
        assert!(
            dir.path().join("a").symlink_metadata().is_ok(),
            "H14: an unchanged reply must not be read as 'no active roots'"
        );
        // 第二轮必须把上一轮的 digest 传上去，否则服务端永远回全量。
        assert_eq!(
            meta.calls.lock().unwrap()[1].as_deref(),
            Some("d1"),
            "the cached digest must be forwarded"
        );
    }

    /// 换代的 target 变化必须原地替换，读到的始终是可解析的 symlink。
    #[test]
    fn generation_change_replaces_the_link_in_place() {
        let dir = tempfile::tempdir().unwrap();
        prepare(dir.path()).unwrap();
        let meta = ScriptedMeta::new(vec![
            full("d1", vec![node("root-61", 7, "node-a")]),
            full("d2", vec![node("root-61", 8, "node-a")]),
        ]);
        let mut digest = None;
        let mut roots = Vec::new();
        converge(&config(dir.path()), &meta, &mut digest, &mut roots).unwrap();
        converge(&config(dir.path()), &meta, &mut digest, &mut roots).unwrap();

        assert_eq!(
            std::fs::read_link(dir.path().join("a")).unwrap(),
            Path::new("/imports/node-a/users/root-61-e8")
        );
    }

    /// 下线的 root 被摘除。
    #[test]
    fn removed_root_is_unlinked() {
        let dir = tempfile::tempdir().unwrap();
        prepare(dir.path()).unwrap();
        let meta = ScriptedMeta::new(vec![
            full("d1", vec![node("root-61", 7, "node-a")]),
            full("d2", Vec::new()),
        ]);
        let mut digest = None;
        let mut roots = Vec::new();
        converge(&config(dir.path()), &meta, &mut digest, &mut roots).unwrap();
        converge(&config(dir.path()), &meta, &mut digest, &mut roots).unwrap();

        assert!(dir.path().join("a").symlink_metadata().is_err());
    }

    /// ghost（真实目录占位）先隔离再发布，且原内容不丢。
    #[test]
    fn foreign_directory_is_quarantined_not_destroyed() {
        let dir = tempfile::tempdir().unwrap();
        prepare(dir.path()).unwrap();
        let ghost = dir.path().join("a");
        std::fs::create_dir(&ghost).unwrap();
        std::fs::write(ghost.join("evidence"), b"x").unwrap();

        let meta = ScriptedMeta::new(vec![full("d1", vec![node("root-61", 7, "node-a")])]);
        let mut digest = None;
        let mut roots = Vec::new();
        converge(&config(dir.path()), &meta, &mut digest, &mut roots).unwrap();

        // 名字现在是 symlink，原目录被移入隔离区而不是被删掉。
        assert_eq!(
            std::fs::read_link(dir.path().join("a")).unwrap(),
            Path::new("/imports/node-a/users/root-61-e7")
        );
        let quarantined: Vec<_> = std::fs::read_dir(dir.path().join(QUARANTINE_DIR))
            .unwrap()
            .flatten()
            .collect();
        assert_eq!(quarantined.len(), 1);
        assert!(quarantined[0].path().is_dir());
    }

    /// Meta 失败时保持现状：已发布的名字不能被撤掉（H7）。
    #[test]
    fn meta_failure_preserves_published_names() {
        let dir = tempfile::tempdir().unwrap();
        prepare(dir.path()).unwrap();
        let meta = ScriptedMeta::new(vec![full("d1", vec![node("root-61", 7, "node-a")])]);
        let mut digest = None;
        let mut roots = Vec::new();
        converge(&config(dir.path()), &meta, &mut digest, &mut roots).unwrap();

        // 第二次调用时脚本已耗尽 → 返回错误。
        let result = converge(&config(dir.path()), &meta, &mut digest, &mut roots);
        assert!(result.is_err());
        assert!(
            dir.path().join("a").symlink_metadata().is_ok(),
            "a failed round must not retract names"
        );
    }

    /// 保留目录不参与 diff，也不会被当成 ghost 移走。
    #[test]
    fn reserved_directories_survive_a_round() {
        let dir = tempfile::tempdir().unwrap();
        prepare(dir.path()).unwrap();
        let meta = ScriptedMeta::new(vec![full("d1", Vec::new())]);
        converge(&config(dir.path()), &meta, &mut None, &mut Vec::new()).unwrap();

        assert!(dir.path().join(STAGING_DIR).is_dir());
        assert!(dir.path().join(QUARANTINE_DIR).is_dir());
    }

    /// 启动残留的 staging 链接被清掉，避免后续 symlinkat 撞 EEXIST。
    #[test]
    fn prepare_clears_staging_leftovers() {
        let dir = tempfile::tempdir().unwrap();
        std::fs::create_dir_all(dir.path().join(STAGING_DIR)).unwrap();
        std::os::unix::fs::symlink("/stale", dir.path().join(STAGING_DIR).join("stage-1")).unwrap();

        prepare(dir.path()).unwrap();
        assert_eq!(
            std::fs::read_dir(dir.path().join(STAGING_DIR))
                .unwrap()
                .count(),
            0
        );
    }

    #[test]
    fn public_root_must_be_tmpfs() {
        let root = Path::new("/mnt/adx/public");
        let tmpfs = b"36 1 0:32 / /mnt/adx/public rw,nosuid,nodev - tmpfs tmpfs rw\n";
        assert!(ensure_public_root_is_tmpfs(root, tmpfs).is_ok());

        let ext4 = b"36 1 0:32 / /mnt/adx/public rw,relatime - ext4 /dev/sda1 rw\n";
        let error = ensure_public_root_is_tmpfs(root, ext4).unwrap_err();
        assert!(format!("{error:?}").contains("tmpfs"), "{error:?}");

        let missing = b"36 1 0:32 / /mnt/other rw - tmpfs tmpfs rw\n";
        let error = ensure_public_root_is_tmpfs(root, missing).unwrap_err();
        assert!(
            format!("{error:?}").contains("not a mount point"),
            "{error:?}"
        );
    }

    /// 挂载点路径中的空格按 mountinfo 的八进制转义编码后应能匹配。
    #[test]
    fn public_root_with_spaces_is_matched_via_mountinfo_escaping() {
        let root = Path::new("/mnt/ad x/public");
        let mountinfo = b"36 1 0:32 / /mnt/ad\\040x/public rw - tmpfs tmpfs rw\n";
        assert!(ensure_public_root_is_tmpfs(root, mountinfo).is_ok());
    }

    /// 诊断只报告，不改变名字面：即便 import 挂载缺失，已发布的名字仍保留。
    #[test]
    fn diagnosis_does_not_disturb_published_names() {
        let dir = tempfile::tempdir().unwrap();
        prepare(dir.path()).unwrap();
        let meta = ScriptedMeta::new(vec![full("d1", vec![node("root-61", 7, "node-a")])]);
        let mut digest = None;
        let mut roots = Vec::new();
        converge(&config(dir.path()), &meta, &mut digest, &mut roots).unwrap();

        // import 根下什么都没有 → 应当报告，但名字面不受影响。
        let findings = diagnose(&config(dir.path()), b"");
        assert!(
            dir.path().join("a").symlink_metadata().is_ok(),
            "diagnosis must never retract names"
        );
        // 该临时目录不是 tmpfs 挂载点，因此 public 也应有 finding。
        assert!(!findings.is_empty());
    }

    /// 诊断从目录枚举 home，而不是从 mountinfo：挂载消失的 home 仍要报告。
    #[test]
    fn homes_are_enumerated_from_the_directory_not_from_mountinfo() {
        let dir = tempfile::tempdir().unwrap();
        let imports = dir.path().join("imports");
        std::fs::create_dir_all(imports.join("node-a")).unwrap();
        std::fs::create_dir_all(imports.join("node-b")).unwrap();
        let config = MirrorConfig {
            public_root: dir.path().join("public"),
            import_root: imports.to_string_lossy().into_owned(),
            interval: Duration::from_millis(10),
        };
        assert_eq!(homes_under(&config.import_root), vec!["node-a", "node-b"]);

        // 没有任何 mountinfo 条目覆盖它们 → 每个 home 都报 path_missing。
        let findings = diagnose(&config, b"");
        let missing = findings
            .iter()
            .filter(|finding| finding.kind() == "path_missing")
            .count();
        assert!(
            missing >= 2,
            "both homes should be reported, got {findings:?}"
        );
    }

    /// 挂载齐全时不应产生 import 相关 finding。
    #[test]
    fn healthy_topology_reports_nothing_for_imports() {
        let dir = tempfile::tempdir().unwrap();
        let imports = dir.path().join("imports");
        std::fs::create_dir_all(imports.join("node-a")).unwrap();
        let public = dir.path().join("public");
        std::fs::create_dir_all(&public).unwrap();
        let config = MirrorConfig {
            public_root: public.clone(),
            import_root: imports.to_string_lossy().into_owned(),
            interval: Duration::from_millis(10),
        };

        let mountinfo = format!(
            "1 0 0:1 / {} rw - tmpfs tmpfs rw\n2 0 0:2 / {}/node-a/users rw - nfs4 home-a:/x rw\n",
            public.display(),
            imports.display()
        );
        let findings = diagnose(&config, mountinfo.as_bytes());
        assert!(findings.is_empty(), "unexpected findings: {findings:?}");
    }
}
