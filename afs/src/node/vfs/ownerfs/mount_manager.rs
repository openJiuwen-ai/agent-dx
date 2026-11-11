//! Independent mount lifecycle manager for mkdir-triggered workspace bind mounts.
//!
//! Follows RFC 0001 §5: OwnerFs owns authoritative root state and P2P;
//! MountManager owns native egress creation, validation, and teardown.
//! Per-root entries are independent; same-root operations serialize via
//! the entry state mutex. `request_mount` is non-blocking (RFC 0001 §2).

use std::{
    collections::{HashMap, HashSet},
    ffi::OsString,
    io,
    path::PathBuf,
    sync::{Arc, Condvar, Mutex, Weak},
    thread,
    time::{Duration, Instant},
};

use afs_error::Result;

use super::{OwnerFs, bind_mount, root, root::RootId};

/// Manages per-root native bind mounts triggered by mkdir.
///
/// Each root gets an independent [`MountEntry`]; different roots never
/// block each other. Same-root operations serialize through the entry's
/// state mutex, preventing stacked mounts from repeated requests.
pub(in crate::node) struct MountManager {
    entries: Mutex<HashMap<RootId, Arc<MountEntry>>>,
    egress_root: Option<PathBuf>,
    owner: Mutex<Weak<OwnerFs>>,
    /// Test-only probe for the rmdir fast-fail path: proves the emptiness
    /// pre-check returns before any unmount request is issued.
    #[cfg(test)]
    unmount_requests: std::sync::atomic::AtomicUsize,
}

struct MountEntry {
    state: Mutex<MountState>,
    /// 挂载 worker 收敛（进入 NativeActive / FuseOnly）时唤醒 detach_entry
    /// 的等待者，消除"卸载时 worker 仍在挂载"的竞态（review: Mounting 与
    /// 卸载竞态会泄漏 bind mount）。
    wait: Condvar,
    root_id: RootId,
    name: OsString,
}

enum MountState {
    /// Backing directory ready; shared cache policy enabled.
    FuseReady,
    /// Async worker is binding in the recorded namespace.
    Mounting,
    /// Bind mount live; new paths resolve native.
    #[cfg(target_os = "linux")]
    NativeActive {
        bind: Box<bind_mount::AuthorizedWorkspaceBind>,
    },
    /// Bind failed; root remains fully accessible via FUSE (D2 fallback).
    #[allow(dead_code)]
    FuseOnly { reason: String },
}

fn poisoned() -> afs_error::Error {
    afs_error::Error::coded(afs_error::RUNTIME_INTERNAL, "MountManager state poisoned")
}

impl MountManager {
    /// Create a new MountManager. `egress_root` is the administrator-configured
    /// native path whose children receive workspace bind targets (D1/D6). When
    /// `None`, all requests fall back to FUSE_ONLY.
    pub(in crate::node) fn new(egress_root: Option<PathBuf>) -> Self {
        Self {
            entries: Mutex::new(HashMap::new()),
            egress_root,
            owner: Mutex::new(Weak::new()),
            #[cfg(test)]
            unmount_requests: std::sync::atomic::AtomicUsize::new(0),
        }
    }

    /// Set the weak back-reference to the owning OwnerFs. Called once after
    /// construction so workers can reach `native_home_export_for_current_namespace`.
    pub(in crate::node) fn attach_owner(&self, owner: &Arc<OwnerFs>) {
        let mut slot = self.owner.lock().unwrap_or_else(|e| e.into_inner());
        *slot = Arc::downgrade(owner);
    }

    /// Request an asynchronous native bind mount for the given root.
    ///
    /// Non-blocking: spawns a worker thread that prepares and activates the
    /// bind. Idempotent: repeated requests for an already-mounted or
    /// mounting root are no-ops. Failures only log (D2 fallback to FUSE).
    pub(in crate::node) fn request_mount(&self, root_id: RootId, name: OsString) -> Result<()> {
        let entry = {
            let mut entries = self.entries.lock().map_err(|_| poisoned())?;
            entries
                .entry(root_id.clone())
                .or_insert_with(|| {
                    Arc::new(MountEntry {
                        state: Mutex::new(MountState::FuseReady),
                        wait: Condvar::new(),
                        root_id: root_id.clone(),
                        name: name.clone(),
                    })
                })
                .clone()
        };

        // Check current state under the entry lock; skip if already active or mounting.
        {
            let mut state = entry.state.lock().map_err(|_| poisoned())?;
            match &*state {
                MountState::Mounting => return Ok(()),
                #[cfg(target_os = "linux")]
                MountState::NativeActive { .. } => return Ok(()),
                MountState::FuseReady | MountState::FuseOnly { .. } => {
                    *state = MountState::Mounting;
                }
            }
        }

        self.spawn_mount_worker(entry);
        Ok(())
    }

    /// Test-only probe: whether this root currently has a tracked mount entry.
    #[cfg(test)]
    pub(in crate::node) fn has_entry(&self, root_id: &RootId) -> bool {
        self.entries
            .lock()
            .unwrap_or_else(|error| error.into_inner())
            .contains_key(root_id)
    }

    /// Test-only probe: how many unmount requests reached this manager.
    #[cfg(test)]
    pub(in crate::node) fn unmount_requests(&self) -> usize {
        self.unmount_requests
            .load(std::sync::atomic::Ordering::Relaxed)
    }

    /// Shut down all active mounts: detach every live bind, clean up each
    /// egress target directory, and clear entries.
    ///
    /// Called from the Node shutdown path after the FUSE session ends.
    /// Without this call, active binds keep a strong OwnerFs reference and
    /// prevent clean drop. Detach uses MNT_DETACH so a busy tree (active
    /// container fds) cannot strand the shutdown. Still-mounting workers are
    /// awaited up to `DETACH_MOUNTING_TIMEOUT` so their bind is detached
    /// before teardown; a pathological timeout is covered by the next startup's
    /// `reconcile_egress`.
    pub(in crate::node) fn shutdown(&self) {
        let entries: Vec<Arc<MountEntry>> = {
            let mut entries = self.entries.lock().unwrap_or_else(|e| e.into_inner());
            entries.drain().map(|(_, v)| v).collect()
        };
        for entry in entries {
            // 关闭阶段 detach 失败只记录日志（已在 detach_entry 内 warn）。
            let _ = Self::detach_entry(&entry);
            self.cleanup_egress_target(&entry);
        }
    }

    /// D4: 删除流程的卸载步骤——detach 该 root 的活跃 bind（MNT_DETACH，
    /// 不因活跃 fd 失败），从 entries 移除，再清理出口目标空目录。
    ///
    /// 幂等：root_id 不在 entries 中时返回 Ok(())。若异步 worker 仍在挂载
    /// （Mounting），detach 等待其收敛（至多 `DETACH_MOUNTING_TIMEOUT`），
    /// **期间 entry 保持被跟踪**：worker 收敛为 NativeActive 时仍能被 detach，
    /// 同 root 的 request_mount 也保持幂等，不会派生第二个 worker
    /// （review: Mounting 与卸载竞态会泄漏 bind mount）。
    /// 超时或 detach 失败时保留 entry 供后续重试并记录日志，不阻塞删除流程；
    /// 物理目录删除可能随后失败，由 reconcile_on_startup 在下次启动时重试。
    pub(in crate::node) fn request_unmount(&self, root_id: &RootId) -> Result<()> {
        #[cfg(test)]
        self.unmount_requests
            .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
        let entry = {
            let entries = self.entries.lock().map_err(|_| poisoned())?;
            entries.get(root_id).cloned()
        };
        let Some(entry) = entry else {
            return Ok(());
        };

        if let Err(error) = Self::detach_entry(&entry) {
            afs_logging::warn!(
                "ownerfs.mount_manager.detach_failed_retained";
                "root_id" => ?root_id, "error" => error.to_string()
            );
            return Ok(());
        }

        // 仅移除同一个 entry：若 detach 等待期间同 root 被重新创建（新 entry、
        // 新 worker 正在挂载到同一出口目标），不能误删新建的挂载记录。
        let removed = {
            let mut entries = self.entries.lock().map_err(|_| poisoned())?;
            match entries.get(root_id) {
                Some(current) if Arc::ptr_eq(current, &entry) => entries.remove(root_id).is_some(),
                _ => false,
            }
        };
        if removed {
            self.cleanup_egress_target(&entry);
        }
        Ok(())
    }

    /// Startup reconciliation of residual egress bind mounts (D4 §4.4 / v3 §7.5).
    ///
    /// Scans mountinfo for leftover `<egress_root>/<name>` binds from a previous
    /// run. Every residual bind is detached with MNT_DETACH (never stranded by a
    /// busy tree):
    /// - names with a live recovered root are left as empty target directories,
    ///   so the normal recovery path re-mounts a fresh claim without stacking;
    /// - orphan names (no recovered root) also have their empty target directory
    ///   removed.
    pub(in crate::node) fn reconcile_egress(&self, known_root_ids: &HashSet<RootId>) -> Result<()> {
        let Some(egress_root) = self.egress_root.clone() else {
            return Ok(());
        };
        let names = bind_mount::scan_egress_binds(&egress_root).map_err(afs_error::Error::from)?;
        for name in names {
            if let Err(error) = bind_mount::detach_egress_bind(&egress_root, &name) {
                afs_logging::warn!(
                    "ownerfs.mount_manager.reconcile.detach_failed";
                    "workspace" => ?name, "error" => error.to_string()
                );
                continue;
            }
            let known = root::root_id_from_name(&name)
                .map(|id| known_root_ids.contains(&id))
                .unwrap_or(false);
            if !known {
                let target = egress_root.join(&name);
                if let Err(error) = std::fs::remove_dir(&target)
                    && error.kind() != std::io::ErrorKind::NotFound
                {
                    afs_logging::warn!(
                        "ownerfs.mount_manager.reconcile.target_cleanup_failed";
                        "workspace" => ?name, "target" => ?target,
                        "error" => error.to_string()
                    );
                }
            }
        }
        Ok(())
    }

    /// Best-effort removal of the egress target directory `<egress_root>/<name>`.
    /// Absence is idempotent; other failures only log (the reconcile or the next
    /// unmount retries the cleanup).
    fn cleanup_egress_target(&self, entry: &MountEntry) {
        let Some(egress_root) = &self.egress_root else {
            return;
        };
        let target = egress_root.join(&entry.name);
        if let Err(error) = std::fs::remove_dir(&target)
            && error.kind() != std::io::ErrorKind::NotFound
        {
            afs_logging::warn!(
                "ownerfs.mount_manager.egress_cleanup_failed";
                "root_id" => ?entry.root_id, "target" => ?target,
                "error" => error.to_string()
            );
        }
    }

    /// 卸载在挂载 worker 仍处于 Mounting 时最长等待的时长。超过后返回
    /// 超时错误：`request_unmount` 保留 entry 供重试，`shutdown` 记录日志
    /// 并由下次启动的 reconcile_egress 兜底清理残留 bind。
    #[cfg(target_os = "linux")]
    const DETACH_MOUNTING_TIMEOUT: Duration = Duration::from_secs(5);

    #[cfg(target_os = "linux")]
    fn detach_entry(entry: &MountEntry) -> io::Result<()> {
        Self::detach_entry_with_timeout(entry, Self::DETACH_MOUNTING_TIMEOUT)
    }

    /// detach 活跃 bind；entry 仍在挂载（Mounting）时等待 worker 收敛后再
    /// 处理，而不是对 Mounting 直接返回成功——否则 worker 随后写入
    /// NativeActive，而该 entry 已从 entries 移除，bind 残留且无法再卸载
    /// （review: Mounting 与卸载竞态会泄漏 bind mount）。
    ///
    /// 等待以 `timeout` 为上限：worker 收敛为 NativeActive 则 detach 并转为
    /// FuseOnly；收敛为 FuseOnly 则无事可做；超时返回 Err 让调用方保留 entry
    /// 重试。`timeout` 参数化以便测试覆盖等待与超时两条路径。
    #[cfg(target_os = "linux")]
    fn detach_entry_with_timeout(entry: &MountEntry, timeout: Duration) -> io::Result<()> {
        let mut state = entry
            .state
            .lock()
            .map_err(|_| io::Error::other("mount entry state poisoned"))?;
        let deadline = Instant::now() + timeout;
        loop {
            if let MountState::NativeActive { bind } = &mut *state {
                let result = bind.detach();
                if result.is_ok() {
                    *state = MountState::FuseOnly {
                        reason: "detached for teardown".to_string(),
                    };
                }
                return result;
            }
            match &*state {
                // 等待挂载 worker 收敛（spawn_mount_worker 失败路径或
                // mount_worker_loop 结束块都会 notify_all）。
                MountState::Mounting => {}
                _ => return Ok(()),
            }
            let remaining = deadline.saturating_duration_since(Instant::now());
            if remaining.is_zero() {
                return Err(io::Error::new(
                    io::ErrorKind::TimedOut,
                    "mount worker did not settle before unmount timeout",
                ));
            }
            let (new_state, _wait_result) = entry
                .wait
                .wait_timeout(state, remaining)
                .map_err(|_| io::Error::other("mount entry state poisoned"))?;
            state = new_state;
        }
    }

    #[cfg(not(target_os = "linux"))]
    fn detach_entry(_entry: &MountEntry) -> io::Result<()> {
        Ok(())
    }

    fn spawn_mount_worker(&self, entry: Arc<MountEntry>) {
        let owner = match self
            .owner
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .upgrade()
        {
            Some(o) => o,
            None => {
                Self::set_fuse_only(&entry, "OwnerFs dropped before mount");
                return;
            }
        };
        let egress_root = match &self.egress_root {
            Some(p) => p.clone(),
            None => {
                Self::set_fuse_only(&entry, "no egress root configured");
                return;
            }
        };

        let worker_entry = entry.clone();
        if let Err(error) = thread::Builder::new()
            .name(format!("afs-mount-{}", entry.root_id.0))
            .spawn(move || {
                Self::mount_worker_loop(owner, egress_root, worker_entry);
            })
        {
            Self::note_worker_spawn_failure(&entry, &error);
        }
    }

    /// 挂载 worker 线程创建失败（如资源耗尽 EAGAIN）：收敛为 FuseOnly 并
    /// 唤醒等待中的 detach，避免 entry 永久停留在 Mounting 造成 no-op、
    /// 卸载 5s 阻塞和 shutdown 串行阻塞（review: spawn 失败未收敛）。
    fn note_worker_spawn_failure(entry: &MountEntry, error: &io::Error) {
        afs_logging::warn!(
            "ownerfs.mount_manager.spawn_failed";
            "error" => error.to_string()
        );
        Self::set_fuse_only(entry, format!("mount worker spawn failed: {error}"));
    }

    #[cfg(target_os = "linux")]
    fn mount_worker_loop(owner: Arc<OwnerFs>, egress_root: PathBuf, entry: Arc<MountEntry>) {
        let workspace = entry.name.clone();
        let mut bind = match bind_mount::AuthorizedWorkspaceBind::prepare_egress(
            owner,
            &egress_root,
            &workspace,
        ) {
            Ok(bind) => bind,
            Err(error) => {
                Self::set_fuse_only(&entry, format!("prepare failed: {error}"));
                return;
            }
        };
        if let Err(error) = bind.activate() {
            Self::set_fuse_only(&entry, format!("activate failed: {error}"));
            return;
        }

        afs_logging::info!(
            "ownerfs.mount_manager.native_active";
            "root_id" => ?entry.root_id, "workspace" => ?entry.name
        );

        let mut state = match entry.state.lock() {
            Ok(s) => s,
            Err(e) => e.into_inner(),
        };
        match &*state {
            MountState::Mounting => {
                *state = MountState::NativeActive {
                    bind: Box::new(bind),
                };
            }
            MountState::FuseOnly { .. } => {
                // Shutdown raced ahead; detach the bind we just activated.
                drop(state);
                let _ = bind.detach();
            }
            _ => {
                *state = MountState::NativeActive {
                    bind: Box::new(bind),
                };
            }
        }
        // 唤醒可能正等待本 worker 收敛的 detach_entry（Mounting → 卸载竞态）。
        entry.wait.notify_all();
    }

    #[cfg(not(target_os = "linux"))]
    fn mount_worker_loop(_owner: Arc<OwnerFs>, _egress_root: PathBuf, entry: Arc<MountEntry>) {
        Self::set_fuse_only(&entry, "bind mount is only supported on Linux");
    }

    fn set_fuse_only(entry: &MountEntry, reason: impl Into<String>) {
        let reason = reason.into();
        afs_logging::warn!(
            "ownerfs.mount_manager.fuse_only";
            "root_id" => ?entry.root_id, "reason" => &reason
        );
        let mut state = match entry.state.lock() {
            Ok(s) => s,
            Err(e) => e.into_inner(),
        };
        // Don't overwrite NativeActive if a previous bind succeeded.
        #[cfg(target_os = "linux")]
        {
            if matches!(&*state, MountState::NativeActive { .. }) {
                return;
            }
        }
        *state = MountState::FuseOnly { reason };
        // 唤醒可能正等待 worker 收敛的 detach_entry。
        entry.wait.notify_all();
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::ffi::OsStr;
    use std::time::{Duration, Instant};

    fn root_id(name: &str) -> RootId {
        RootId(name.to_owned())
    }

    fn entry_state(manager: &MountManager, id: &RootId) -> Option<MountState> {
        let entries = manager.entries.lock().unwrap();
        let entry = entries.get(id)?;
        let state = entry.state.lock().unwrap();
        Some(match &*state {
            MountState::FuseReady => MountState::FuseReady,
            MountState::Mounting => MountState::Mounting,
            #[cfg(target_os = "linux")]
            MountState::NativeActive { .. } => {
                panic!("cannot snapshot NativeActive without a live bind")
            }
            MountState::FuseOnly { reason } => MountState::FuseOnly {
                reason: reason.clone(),
            },
        })
    }

    #[test]
    fn request_mount_without_owner_falls_back_to_fuse_only() {
        let manager = MountManager::new(None);
        manager
            .request_mount(root_id("agent1"), OsString::from("agent1"))
            .unwrap();

        let entries = manager.entries.lock().unwrap();
        assert_eq!(entries.len(), 1);
        let entry = entries.get(&root_id("agent1")).unwrap();
        assert_eq!(entry.root_id, root_id("agent1"));
        assert_eq!(entry.name.as_os_str(), OsStr::new("agent1"));
        match &*entry.state.lock().unwrap() {
            MountState::FuseOnly { reason } => assert!(
                reason.contains("OwnerFs dropped before mount"),
                "unexpected reason: {reason}"
            ),
            _ => panic!("expected FuseOnly fallback without an attached OwnerFs"),
        }
    }

    #[test]
    fn request_mount_without_egress_root_falls_back_to_fuse_only() {
        let (_temp, owner, _ctx, _disk) =
            crate::node::vfs::ownerfs::native_home_tests::fixture(true);
        let owner = Arc::new(owner);
        let manager = MountManager::new(None);
        manager.attach_owner(&owner);
        manager
            .request_mount(root_id("agent1"), OsString::from("agent1"))
            .unwrap();

        let entries = manager.entries.lock().unwrap();
        assert_eq!(entries.len(), 1);
        let entry = entries.get(&root_id("agent1")).unwrap();
        match &*entry.state.lock().unwrap() {
            MountState::FuseOnly { reason } => assert!(
                reason.contains("no egress root configured"),
                "unexpected reason: {reason}"
            ),
            _ => panic!("expected FuseOnly fallback without an egress root"),
        }
    }

    #[test]
    fn request_mount_is_idempotent_while_mounting() {
        let manager = MountManager::new(None);
        manager
            .request_mount(root_id("agent1"), OsString::from("agent1"))
            .unwrap();
        let entry = manager
            .entries
            .lock()
            .unwrap()
            .get(&root_id("agent1"))
            .unwrap()
            .clone();
        {
            let mut state = entry.state.lock().unwrap();
            *state = MountState::Mounting;
        }

        // A second request while Mounting must not spawn a second worker.
        manager
            .request_mount(root_id("agent1"), OsString::from("agent1"))
            .unwrap();

        // No owner and no mount path mean any spawned worker would have flipped
        // the state synchronously; it staying Mounting proves the no-op path.
        std::thread::sleep(Duration::from_millis(50));
        assert!(matches!(*entry.state.lock().unwrap(), MountState::Mounting));
    }

    #[test]
    fn request_mount_for_distinct_roots_keeps_independent_entries() {
        let manager = MountManager::new(None);
        for name in ["agent1", "agent2"] {
            manager
                .request_mount(root_id(name), OsString::from(name))
                .unwrap();
        }

        let entries = manager.entries.lock().unwrap();
        assert_eq!(entries.len(), 2);
        for name in ["agent1", "agent2"] {
            let entry = entries.get(&root_id(name)).expect("independent entry");
            assert_eq!(entry.root_id, root_id(name));
            assert_eq!(entry.name.as_os_str(), OsStr::new(name));
            assert!(matches!(
                *entry.state.lock().unwrap(),
                MountState::FuseOnly { .. }
            ));
        }
    }

    #[test]
    fn request_mount_for_same_root_reuses_entry() {
        let manager = MountManager::new(None);
        manager
            .request_mount(root_id("agent1"), OsString::from("agent1"))
            .unwrap();
        manager
            .request_mount(root_id("agent1"), OsString::from("renamed"))
            .unwrap();

        let entries = manager.entries.lock().unwrap();
        assert_eq!(entries.len(), 1);
        let entry = entries.get(&root_id("agent1")).unwrap();
        // The first name wins; a repeated request must not create a new entry.
        assert_eq!(entry.name.as_os_str(), OsStr::new("agent1"));
        assert!(matches!(
            *entry.state.lock().unwrap(),
            MountState::FuseOnly { .. }
        ));
    }

    #[test]
    fn shutdown_drains_all_entries() {
        let manager = MountManager::new(None);
        for name in ["agent1", "agent2"] {
            manager
                .request_mount(root_id(name), OsString::from(name))
                .unwrap();
        }
        assert_eq!(manager.entries.lock().unwrap().len(), 2);

        manager.shutdown();
        assert!(manager.entries.lock().unwrap().is_empty());
    }

    #[test]
    fn reconcile_egress_without_egress_root_is_a_no_op() {
        let manager = MountManager::new(None);
        manager.reconcile_egress(&HashSet::new()).unwrap();
        assert!(manager.entries.lock().unwrap().is_empty());
    }

    #[cfg(target_os = "linux")]
    #[test]
    fn detach_entry_waits_for_mounting_worker_to_settle() {
        let manager = MountManager::new(None);
        manager
            .request_mount(root_id("agent1"), OsString::from("agent1"))
            .unwrap();
        let entry = manager
            .entries
            .lock()
            .unwrap()
            .get(&root_id("agent1"))
            .unwrap()
            .clone();
        // 手工回到 Mounting，模拟异步 worker 正在挂载。
        {
            let mut state = entry.state.lock().unwrap();
            *state = MountState::Mounting;
        }

        // 模拟挂载 worker 稍后收敛（如 bind 失败回退 FuseOnly）。
        let settling_entry = entry.clone();
        let settle = std::thread::spawn(move || {
            std::thread::sleep(Duration::from_millis(50));
            MountManager::set_fuse_only(&settling_entry, "simulated worker settle");
        });

        let start = Instant::now();
        let result = MountManager::detach_entry_with_timeout(&entry, Duration::from_secs(5));
        settle.join().unwrap();
        // detach 必须等到 worker 收敛，而不是对 Mounting 直接返回成功。
        assert!(result.is_ok(), "unexpected detach failure: {result:?}");
        assert!(
            start.elapsed() >= Duration::from_millis(30),
            "detach returned before the mounting worker settled"
        );
        assert!(matches!(
            *entry.state.lock().unwrap(),
            MountState::FuseOnly { .. }
        ));
    }

    #[cfg(target_os = "linux")]
    #[test]
    fn detach_entry_times_out_while_worker_still_mounting() {
        let manager = MountManager::new(None);
        manager
            .request_mount(root_id("agent1"), OsString::from("agent1"))
            .unwrap();
        let entry = manager
            .entries
            .lock()
            .unwrap()
            .get(&root_id("agent1"))
            .unwrap()
            .clone();
        {
            let mut state = entry.state.lock().unwrap();
            *state = MountState::Mounting;
        }

        // 没有 worker 会收敛：detach 必须超时返回 Err，而不是假装成功。
        let err =
            MountManager::detach_entry_with_timeout(&entry, Duration::from_millis(50)).unwrap_err();
        assert_eq!(err.kind(), std::io::ErrorKind::TimedOut);
        // 状态保持 Mounting：request_unmount 会保留 entry 供后续重试。
        assert!(matches!(*entry.state.lock().unwrap(), MountState::Mounting));
    }

    #[cfg(target_os = "linux")]
    #[test]
    fn spawn_failure_converges_entry_to_fuse_only_and_unblocks_detach() {
        let manager = MountManager::new(None);
        manager
            .request_mount(root_id("agent1"), OsString::from("agent1"))
            .unwrap();
        let entry = manager
            .entries
            .lock()
            .unwrap()
            .get(&root_id("agent1"))
            .unwrap()
            .clone();
        // 模拟 spawn 已把状态置为 Mounting、但 OS 线程创建失败。
        {
            let mut state = entry.state.lock().unwrap();
            *state = MountState::Mounting;
        }

        // 删除流程的卸载步骤正阻塞在 detach，等待 worker 收敛。
        let waiting = {
            let entry = entry.clone();
            std::thread::spawn(move || {
                MountManager::detach_entry_with_timeout(&entry, Duration::from_secs(5))
            })
        };
        // 等待观察者进入等待，再触发 spawn 失败处理。
        std::thread::sleep(Duration::from_millis(50));
        MountManager::note_worker_spawn_failure(
            &entry,
            &std::io::Error::new(std::io::ErrorKind::WouldBlock, "simulated EAGAIN"),
        );

        // detach 必须被唤醒并立即成功（FuseOnly 无事可做），而不是等满 5s 超时。
        let result = waiting.join().unwrap();
        assert!(
            result.is_ok(),
            "detach should unblock after spawn failure: {result:?}"
        );
        // 收敛为 FuseOnly：request_mount 可重试，不再是永久 no-op。
        assert!(matches!(
            *entry.state.lock().unwrap(),
            MountState::FuseOnly { .. }
        ));
    }

    #[cfg(target_os = "linux")]
    #[test]
    fn request_unmount_keeps_entry_tracked_while_worker_mounting() {
        let manager = Arc::new(MountManager::new(None));
        manager
            .request_mount(root_id("agent1"), OsString::from("agent1"))
            .unwrap();
        let entry = manager
            .entries
            .lock()
            .unwrap()
            .get(&root_id("agent1"))
            .unwrap()
            .clone();
        // 手工回到 Mounting，模拟异步 worker 正在挂载。
        {
            let mut state = entry.state.lock().unwrap();
            *state = MountState::Mounting;
        }

        // 观察者：在 detach 等待期间确认 entry 仍被跟踪，然后让 worker 收敛。
        let observer = {
            let manager = manager.clone();
            let root_id = root_id("agent1");
            std::thread::spawn(move || {
                std::thread::sleep(Duration::from_millis(50));
                let tracked = manager.entries.lock().unwrap().contains_key(&root_id);
                assert!(
                    tracked,
                    "entry must stay tracked while the mount worker is awaited"
                );
                let entry = manager
                    .entries
                    .lock()
                    .unwrap()
                    .get(&root_id)
                    .unwrap()
                    .clone();
                MountManager::set_fuse_only(&entry, "settle for unmount");
            })
        };

        manager.request_unmount(&root_id("agent1")).unwrap();
        observer.join().unwrap();
        // 卸载完成：entry 已收敛并被移除。
        assert!(manager.entries.lock().unwrap().is_empty());
    }

    #[cfg(target_os = "linux")]
    #[test]
    fn reconcile_egress_scans_empty_egress_root_without_error() {
        let egress = tempfile::tempdir().unwrap();
        let manager = MountManager::new(Some(egress.path().to_path_buf()));
        manager.reconcile_egress(&HashSet::new()).unwrap();
    }

    /// mkdir must return before the bind completes (RFC 0001 §2) and must not
    /// fail when the bind falls back to FUSE_ONLY (D2). The MountManager worker
    /// runs in the background and settles the entry to FuseOnly when the egress
    /// root is unusable.
    #[cfg(target_os = "linux")]
    #[test]
    fn mkdir_triggers_request_mount_and_bind_failure_does_not_fail_mkdir() {
        use super::super::{
            Backend, native_home_tests::fixture_with_mount_manager, root::root_id_from_name,
        };

        // An egress root that will never exist, so bind prepare must fail.
        let egress = tempfile::tempdir().unwrap();
        let egress_root = egress.path().join("egress");
        let (_temp, owner, ctx) = fixture_with_mount_manager(egress_root);
        let owner = Arc::new(owner);
        owner.mount_manager().unwrap().attach_owner(&owner);
        let manager = owner.mount_manager().unwrap().clone();
        let expected_root_id = root_id_from_name(OsStr::new("agent1")).unwrap();

        // mkdir itself must succeed without waiting for the bind.
        let owner_fs: &OwnerFs = &owner;
        let entry = Backend::mkdir(
            owner_fs,
            &ctx,
            owner.root_inode(),
            OsStr::new("agent1"),
            0o755,
        )
        .expect("mkdir must not depend on bind mount success");
        assert!(entry.inode.value != 0);

        // The entry is registered synchronously; it must not be NATIVE_ACTIVE
        // because the FUSE mount path does not exist.
        let initial = entry_state(&manager, &expected_root_id);
        assert!(matches!(
            initial,
            Some(MountState::Mounting | MountState::FuseOnly { .. })
        ));

        // The worker eventually settles the entry to FuseOnly.
        let deadline = Instant::now() + Duration::from_secs(5);
        loop {
            match entry_state(&manager, &expected_root_id) {
                Some(MountState::FuseOnly { .. }) => break,
                Some(MountState::Mounting) if Instant::now() < deadline => {
                    std::thread::sleep(Duration::from_millis(10));
                }
                _state => {
                    assert!(
                        Instant::now() < deadline,
                        "bind worker did not settle to FuseOnly"
                    );
                    std::thread::sleep(Duration::from_millis(10));
                }
            }
        }

        // The fallback must carry the bind failure reason, not a shutdown path.
        match entry_state(&manager, &expected_root_id) {
            Some(MountState::FuseOnly { reason }) => assert!(
                reason.contains("prepare failed"),
                "unexpected fallback reason: {reason}"
            ),
            _ => panic!("expected FuseOnly with prepare failure"),
        }
    }
}
