//! 名字面的 IO 与收敛循环。
//!
//! 与 `mirror` 的分工：那边只有纯函数，这边负责读目录、执行动作、按周期驱动。
//! 所有失败都按"跳过本轮、保持现状"处理（spec H7）：一次失败的 Meta 调用或
//! 一次 tmpfs 写错误不得清空已发布的名字，否则下游容器会在重启 Meta 的瞬间
//! 集体 bind 失败。

use std::{
    collections::BTreeMap,
    ffi::OsString,
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
pub fn converge(
    config: &MirrorConfig,
    meta: &dyn RootMeta,
    cached_digest: &mut Option<String>,
    cached_roots: &mut Vec<OutletRoot>,
) -> Result<usize> {
    let reply = meta.list_active_outlet_roots(cached_digest.as_deref())?;
    // H14: an `unchanged` reply carries no payload and must reuse the cache.
    // This is the single place that decision is made, so no caller can mistake
    // an empty payload for "Meta has no active roots" and wipe the directory.
    let active = merge_reply(&reply, cached_roots);
    *cached_digest = Some(reply.digest.clone());
    *cached_roots = active.clone();

    let present = scan_public(&config.public_root)?;
    let actions = mirror::plan(&config.import_root, &active, &present);
    let mut applied = 0;
    for action in &actions {
        apply(&config.public_root, action)?;
        applied += 1;
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
pub async fn run(
    config: MirrorConfig,
    meta: Arc<dyn RootMeta>,
    mut shutdown: impl std::future::Future<Output = ()> + Unpin,
) {
    let mut cached_digest: Option<String> = None;
    let mut cached_roots: Vec<OutletRoot> = Vec::new();
    let mut tick = tokio::time::interval(config.interval);
    tick.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
    loop {
        tokio::select! {
            _ = &mut shutdown => return,
            _ = tick.tick() => {
                let config = config.clone();
                let meta = meta.clone();
                let mut digest = cached_digest.clone();
                let mut roots = cached_roots.clone();
                match tokio::task::spawn_blocking(move || {
                    let applied = converge(&config, meta.as_ref(), &mut digest, &mut roots);
                    (applied, digest, roots)
                })
                .await
                {
                    Ok((Ok(_), digest, roots)) => {
                        cached_digest = digest;
                        cached_roots = roots;
                    }
                    Ok((Err(error), _, _)) => {
                        // Keep the previous snapshot: a failed round must not
                        // retract names that were valid moments ago.
                        afs_logging::warn!("outlet.mirror.round_failed"; "error" => error.to_string());
                    }
                    Err(error) => {
                        afs_logging::warn!("outlet.mirror.worker_failed"; "error" => error.to_string());
                    }
                }
            }
        }
    }
}

/// Best-effort listing for diagnostics.
pub fn published_names(root: &Path) -> Vec<OsString> {
    let mut names = Vec::new();
    if let Ok(entries) = std::fs::read_dir(root) {
        for entry in entries.flatten() {
            names.push(entry.file_name());
        }
    }
    names.sort();
    names
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::node::vfs::ownerfs::root::{ActiveOutletRoots, RootId};
    use std::sync::Mutex;

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
            unimplemented!("not exercised by mirror tests")
        }
        fn activate_root(
            &self,
            _prepared: &crate::node::vfs::ownerfs::root::PreparedRoot,
        ) -> Result<crate::node::vfs::ownerfs::root::RootGrant> {
            unimplemented!("not exercised by mirror tests")
        }
        fn abort_root(
            &self,
            _reservation: &crate::node::vfs::ownerfs::root::RootReservation,
        ) -> Result<()> {
            unimplemented!("not exercised by mirror tests")
        }
        fn deactivate_root(
            &self,
            _root_id: &RootId,
            _epoch: u64,
            _home_node_id: &str,
            _session_id: &str,
        ) -> Result<()> {
            unimplemented!("not exercised by mirror tests")
        }
        fn lookup_root(
            &self,
            _id: &RootId,
        ) -> Result<Option<crate::node::vfs::ownerfs::root::RootLocation>> {
            unimplemented!("not exercised by mirror tests")
        }
        fn list_owner_roots(
            &self,
            _home_node_id: &str,
        ) -> Result<crate::node::vfs::ownerfs::root::OwnerRootInventory> {
            unimplemented!("not exercised by mirror tests")
        }
        fn acquire_root(
            &self,
            _id: &RootId,
            _right: crate::node::vfs::ownerfs::root::RootRight,
        ) -> Result<crate::node::vfs::ownerfs::root::RootGrant> {
            unimplemented!("not exercised by mirror tests")
        }
        fn validate_root_access(
            &self,
            _presented: &crate::node::vfs::ownerfs::root::PresentedRootAccess,
            _authenticated_peer_node_id: &str,
        ) -> Result<crate::node::vfs::ownerfs::root::RootGrant> {
            unimplemented!("not exercised by mirror tests")
        }
        fn recover_root(
            &self,
            _record: &crate::node::vfs::ownerfs::catalog::LocalRootRecord,
            _new_session_id: &str,
        ) -> Result<crate::node::vfs::ownerfs::root::RootGrant> {
            unimplemented!("not exercised by mirror tests")
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
        let link = dir.path().join("root-61");
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
            dir.path().join("root-61").symlink_metadata().is_ok(),
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
            std::fs::read_link(dir.path().join("root-61")).unwrap(),
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

        assert!(dir.path().join("root-61").symlink_metadata().is_err());
    }

    /// ghost（真实目录占位）先隔离再发布，且原内容不丢。
    #[test]
    fn foreign_directory_is_quarantined_not_destroyed() {
        let dir = tempfile::tempdir().unwrap();
        prepare(dir.path()).unwrap();
        let ghost = dir.path().join("root-61");
        std::fs::create_dir(&ghost).unwrap();
        std::fs::write(ghost.join("evidence"), b"x").unwrap();

        let meta = ScriptedMeta::new(vec![full("d1", vec![node("root-61", 7, "node-a")])]);
        let mut digest = None;
        let mut roots = Vec::new();
        converge(&config(dir.path()), &meta, &mut digest, &mut roots).unwrap();

        // 名字现在是 symlink，原目录被移入隔离区而不是被删掉。
        assert_eq!(
            std::fs::read_link(dir.path().join("root-61")).unwrap(),
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
            dir.path().join("root-61").symlink_metadata().is_ok(),
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
}
