//! 动态 import 挂载的状态机与决策。
//!
//! 设计见《Outlet 动态 NFS 挂载》。本模块只做两件事：维护每个 home 的挂载
//! 状态与退避计时，以及把"当前状态 + 快照"算成一组动作。真正的 `mount(2)`
//! 由调用方执行——这样状态迁移可以完全脱离内核测试。
//!
//! 三条刻意的克制（对应设计第四节）：
//!
//! - **不主动探测**。挂载是否可用由发布路径的真实访问失败来发现，不额外起
//!   探测线程；否则每 250ms 一轮的循环会变成持续的 NFS 压力。
//! - **不熔断**。失败只退避，不永久放弃：节点恢复后必须能自动接回。
//! - **不卸载**。"节点永久移除"缺少可靠信号（session 过期不等于删除），
//!   自动卸载可能打断仍持有 fd 的容器，孤儿挂载只告警。

use std::collections::{BTreeMap, BTreeSet};
use std::path::{Path, PathBuf};
use std::time::{Duration, Instant};

/// 退避上限。超过之后按固定间隔重试，避免退避把恢复时间拉得过长。
pub const MAX_BACKOFF: Duration = Duration::from_secs(60);

/// 首次失败的等待时间；此后逐次翻倍直到 [`MAX_BACKOFF`]。
pub const INITIAL_BACKOFF: Duration = Duration::from_secs(1);

/// 连续多少轮读不到挂载条目才认定"真的被外部卸载"。
///
/// `mount` 动作本身有代价：从开始挂到条目重新出现在 `/proc/self/mountinfo`
/// 之间有一段空窗（实测 bind 约 110–125ms，NFS 更长）。而收敛循环每 250ms
/// 读一次快照，很容易正好落进这个窗口。
///
/// 若单轮缺失就降级为 `NeedsMount`，就会形成自我维持的循环：
/// 挂载 → 条目短暂消失 → 判定未挂载 → 再挂载 → 条目再消失。实测该循环会
/// 稳定持续，每秒重建一次，每次造成约 120ms 的路径不可用（约 12% 的读失败）。
///
/// 取 3 轮（≈750ms）足以跨过任何一次自建挂载的空窗，同时对外部 umount
/// 仍是"下一轮就能发现"的响应速度——外部卸载是持久的，多等两轮无副作用。
pub const ABSENT_ROUNDS_BEFORE_DEMOTE: u32 = 3;

/// 刚挂载完成后的抑制期：此期间不因 mountinfo 缺条目而降级。
///
/// 与 [`ABSENT_ROUNDS_BEFORE_DEMOTE`] 互补：连续计数已经能吸收空窗，
/// 这里再加一道时间闸，防止"每轮都读、但恰好每轮都落在窗口内"的极端情况
/// （快照读取与挂载动作的周期若发生共振，单靠计数仍可能连续命中）。
pub const MOUNT_SETTLE: Duration = Duration::from_secs(1);

/// 一个 home 的挂载阶段。
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum Phase {
    /// 已确认可用。常态，绝大多数轮次停在这里。
    Ready,
    /// 未挂载或已消失，需要挂。
    NeedsMount,
    /// 挂载存在但访问失败过，需要重建。
    Remount,
}

impl Phase {
    fn order(self) -> u8 {
        match self {
            // 需要动手的阶段排在前面，让日志与测试的顺序稳定。
            Self::Remount => 0,
            Self::NeedsMount => 1,
            Self::Ready => 2,
        }
    }
}

/// 单个 home 的状态。每个 home 独立计数，互不影响。
#[derive(Clone, Debug)]
pub struct ImportState {
    phase: Phase,
    /// 连续失败次数，决定退避长度。成功即清零。
    consecutive_failures: u32,
    /// 早于此刻的重试被跳过。`Ready` 时无意义。
    next_attempt: Instant,
    /// 连续多少轮 mountinfo 里看不到该挂载点。
    ///
    /// 单轮缺失不足以判定"被外部卸载"——我们自己刚挂完的那一段窗口里也缺失。
    /// 只有连续 [`ABSENT_ROUNDS_BEFORE_DEMOTE`] 轮都缺失才降级。
    consecutive_absent: u32,
    /// 最近一次挂载成功的时刻，用于 [`MOUNT_SETTLE`] 抑制期。
    mounted_at: Option<Instant>,
}

impl Default for ImportState {
    fn default() -> Self {
        Self {
            phase: Phase::NeedsMount,
            consecutive_failures: 0,
            next_attempt: Instant::now(),
            consecutive_absent: 0,
            mounted_at: None,
        }
    }
}

impl ImportState {
    #[must_use]
    pub fn phase(&self) -> Phase {
        self.phase
    }

    #[must_use]
    pub fn consecutive_failures(&self) -> u32 {
        self.consecutive_failures
    }

    /// 是否处于退避期内。冷却中不碰文件系统。
    #[must_use]
    pub fn cooling(&self, now: Instant) -> bool {
        self.phase != Phase::Ready && now < self.next_attempt
    }

    fn mark_ready(&mut self) {
        self.phase = Phase::Ready;
        self.consecutive_failures = 0;
        // Ready 不参与冷却判断，仍清零以便下次失败从 1s 起算。
        self.next_attempt = Instant::now();
        // 已确认挂着，缺失计数归零。
        self.consecutive_absent = 0;
    }

    fn mark_needs_mount(&mut self) {
        // 由 mountinfo 校正触发：外部 umount 或进程重启。
        // 若是从 Ready 掉下来，这不是一次"失败"，不累计退避，
        // 否则一次外部干扰会让恢复被无谓地推迟。
        self.phase = Phase::NeedsMount;
    }

    fn mark_suspect(&mut self) {
        // 只有 Ready -> Remount 有意义；已在 NeedsMount/Remount 时不重复降级。
        if self.phase == Phase::Ready {
            self.phase = Phase::Remount;
        }
    }

    fn mark_failed(&mut self, now: Instant) -> Duration {
        self.consecutive_failures = self.consecutive_failures.saturating_add(1);
        let delay = backoff_for(self.consecutive_failures);
        self.next_attempt = now + delay;
        // 失败后仍处于"需要动手"的状态，只是被冷却挡住。
        if self.phase == Phase::Ready {
            self.phase = Phase::NeedsMount;
        }
        delay
    }

    /// 挂载成功。返回是否发生了状态变化（用于日志节流）。
    pub fn confirm_mounted(&mut self) -> bool {
        let changed = self.phase != Phase::Ready;
        self.mark_ready();
        // 记下时刻：接下来的 MOUNT_SETTLE 内不因 mountinfo 缺条目而降级。
        // 挂载动作本身会让条目短暂消失（实测 bind 约 120ms），这段窗口里
        // 的"缺失"不是外部卸载。
        self.mounted_at = Some(Instant::now());
        changed
    }

    /// 一次挂载尝试失败。
    pub fn record_failure(&mut self, now: Instant) -> Duration {
        self.mark_failed(now)
    }

    /// mountinfo 说这个 home 已挂载。用于校正内存状态。
    pub fn observe_mounted(&mut self) -> bool {
        // 看到了就是看到了，无论如何清零缺失计数。
        self.consecutive_absent = 0;
        let changed = self.phase != Phase::Ready;
        // 已挂载不等于可访问：Remount 阶段说明访问失败过，即使 mountinfo
        // 仍有条目也要重建。所以只在 NeedsMount 时升级为 Ready。
        if self.phase == Phase::NeedsMount {
            self.mark_ready();
        }
        changed && self.phase == Phase::Ready
    }

    /// mountinfo 说这个 home 没有挂载条目。
    ///
    /// 单轮缺失不代表被卸载：刚挂载完成后、条目尚未出现在 mountinfo 的窗口，
    /// 与"被外部 umount"在单次快照上完全无法区分。所以这里累计缺失轮数，
    /// 只在两种防抖都放行后才真正降级：
    ///
    /// 1. 已连续缺失 `rounds` 轮（吸收自建挂载的空窗）；
    /// 2. 距上次挂载已超过 [`MOUNT_SETTLE`]（防住与快照周期的共振）。
    ///
    /// 外部 umount 是持久状态，多等几轮不影响最终发现——这正是原测试
    /// `reconcile_detects_an_external_unmount` 仍然成立的原因。
    pub fn observe_absent(&mut self, now: Instant, rounds: u32) {
        self.consecutive_absent = self.consecutive_absent.saturating_add(1);
        if self.consecutive_absent < rounds {
            return;
        }
        if let Some(mounted_at) = self.mounted_at
            && now.duration_since(mounted_at) < MOUNT_SETTLE
        {
            // 还在抑制期内：把计数压在阈值下方，下一轮再判，避免提前降级。
            self.consecutive_absent = rounds.saturating_sub(1);
            return;
        }
        self.mark_needs_mount();
    }

    /// 发布路径访问失败：标记可疑，下一轮重建。
    pub fn observe_access_failure(&mut self) {
        self.mark_suspect();
    }
}

/// `1s → 2s → 4s → 8s → 16s → 32s → 60s`，之后恒为 60s。
///
/// 位移在 `u32` 上做，`consecutive_failures` 饱和增长，因此不会溢出。
#[must_use]
pub fn backoff_for(consecutive_failures: u32) -> Duration {
    if consecutive_failures == 0 {
        return Duration::ZERO;
    }
    let shift = consecutive_failures.saturating_sub(1).min(6);
    let seconds = INITIAL_BACKOFF.as_secs().saturating_mul(1u64 << shift);
    Duration::from_secs(seconds).min(MAX_BACKOFF)
}

/// 一个 home 需要执行的动作。
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum ImportAction {
    /// 未挂载，直接挂。
    Mount { home: String, kind: MountKind },
    /// 已挂载但访问失败过，先惰性卸载再挂。
    Remount { home: String, kind: MountKind },
}

/// 挂载方式。本机与对端的唯一区别就在这里。
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum MountKind {
    /// 对端 Home：走 NFS，源是 `<address>:<users_root>`。
    Remote,
    /// 本机 Home：bind 到自己的数据目录。
    ///
    /// 本机也必须出现在 `<import_root>/<local>/users`：`/public/<name>` 的
    /// target 公式对所有 home 一视同仁，本机若不挂，同一台机器上访问自己
    /// 托管的 workspace 就是断链。消费方要求"本机与跨机看到同一个目录"，
    /// 所以这里不能因为 home 落在本机就跳过。
    Local,
}

impl ImportAction {
    #[must_use]
    pub fn home(&self) -> &str {
        match self {
            Self::Mount { home, .. } | Self::Remount { home, .. } => home,
        }
    }

    #[must_use]
    pub fn kind(&self) -> MountKind {
        match self {
            Self::Mount { kind, .. } | Self::Remount { kind, .. } => *kind,
        }
    }
}

/// 全体 home 的状态表。
#[derive(Debug, Default)]
pub struct ImportTable {
    states: BTreeMap<String, ImportState>,
}

impl ImportTable {
    #[must_use]
    pub fn new() -> Self {
        Self::default()
    }

    #[must_use]
    pub fn state(&self, home: &str) -> Option<&ImportState> {
        self.states.get(home)
    }

    pub fn entry(&mut self, home: &str) -> &mut ImportState {
        self.states.entry(home.to_owned()).or_default()
    }

    /// 已知的 home 列表，供 mountinfo 校正遍历。
    pub fn homes(&self) -> impl Iterator<Item = &str> {
        self.states.keys().map(String::as_str)
    }

    /// 丢弃快照里不再出现的 home 的状态。
    ///
    /// 只清内存，不动挂载——节点可能只是暂时没有 root，挂载还要继续服务
    /// 已存在的容器 fd。挂载残留由孤儿告警提示人工处理。
    pub fn retain(&mut self, wanted: &BTreeSet<String>) {
        // 只丢弃"从未挂上过"的状态。
        //
        // 一个已挂载的 home 即使本轮快照里没有它的 root，挂载也必须留着：
        // 快照只包含 Active root，而一个 home 可能暂时没有任何 root（刚重启、
        // 刚清空），也可能本轮 Meta 回复被 H14 缓存挡住。若按快照裁剪，
        // 挂载会失去管理——被外部卸载后再也没人重挂，且会被误判成孤儿。
        self.states
            .retain(|home, state| wanted.contains(home) || state.phase == Phase::Ready);
    }

    /// 当前处于 `Ready` 的 home 数量，用于 `imports_up` 指标。
    #[must_use]
    pub fn ready_count(&self) -> usize {
        self.states
            .values()
            .filter(|state| state.phase == Phase::Ready)
            .count()
    }
}

/// 从快照算出一轮要执行的动作。纯函数：无 IO、无时钟。
///
/// 规则（设计第三节）：
///
/// - 去重：一个 home 有 100 个 root 也只产生一个动作；
/// - 排除本机：本机数据走本地路径，不需要挂载；
/// - 冷却中跳过：退避期内不产生动作，不碰文件系统。
#[must_use]
pub fn plan_imports(
    snapshot_homes: &BTreeSet<String>,
    local_node: &str,
    table: &ImportTable,
    now: Instant,
) -> Vec<ImportAction> {
    // 遍历"快照需要的 home"与"已登记状态的 home"的并集。
    //
    // 只用快照会让已挂载但本轮无 root 的 home 失去维护：它被外部卸载后
    // 不会重挂，形成永久断链。状态表里的 home 也必须有动作机会。
    let mut candidates: BTreeSet<&str> = snapshot_homes.iter().map(String::as_str).collect();
    for home in table.homes() {
        candidates.insert(home);
    }

    let mut actions = Vec::new();
    for home in candidates {
        // 本机也参与：`/public` 的 target 不区分 home 是否本机，所以本机必须
        // 同样出现在 `<import_root>/<local>/users`，否则本机访问自己托管的
        // workspace 会断链。挂载方式由 kind 区分（本机 bind，对端 NFS）。
        let kind = if home == local_node {
            MountKind::Local
        } else {
            MountKind::Remote
        };
        let Some(state) = table.state(home) else {
            // 未知 home：新加入的节点。当作 NeedsMount，且不受冷却限制
            // （它还没有失败记录）。
            actions.push(ImportAction::Mount {
                home: home.to_owned(),
                kind,
            });
            continue;
        };
        if state.cooling(now) {
            continue;
        }
        match state.phase {
            Phase::Ready => {}
            Phase::NeedsMount => actions.push(ImportAction::Mount {
                home: home.to_owned(),
                kind,
            }),
            Phase::Remount => actions.push(ImportAction::Remount {
                home: home.to_owned(),
                kind,
            }),
        }
    }
    actions.sort_by(|left, right| {
        let lphase = table
            .state(left.home())
            .map_or(Phase::NeedsMount, |s| s.phase);
        let rphase = table
            .state(right.home())
            .map_or(Phase::NeedsMount, |s| s.phase);
        lphase
            .order()
            .cmp(&rphase.order())
            .then_with(|| left.home().cmp(right.home()))
    });
    actions
}

/// 一个 home 的挂载点：`<import_root>/<home>/users`。
///
/// 与 outlet 发布的 target 前缀一致，所以先挂载再发布时名字一出现就可用。
#[must_use]
pub fn mount_point(import_root: &Path, home: &str) -> PathBuf {
    import_root.join(home).join("users")
}

/// 挂载源：`<home>:<users_root>`。
///
/// `home` 用作主机名而非 IP：设计假定全集群按节点名解析到数据地址（部署
/// 文档要求节点名可达）。这样不需要节点清单，也不需要 Meta 额外提供地址。
#[must_use]
pub fn mount_source(home: &str, users_root: &Path) -> String {
    format!("{}:{}", home, users_root.display())
}

#[cfg(test)]
mod tests;
