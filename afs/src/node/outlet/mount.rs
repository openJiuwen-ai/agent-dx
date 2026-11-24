//! import 挂载的实际执行：`mount(2)` 与惰性卸载。
//!
//! 与 [`super::imports`] 分开，是因为那一半是决策（可测），这一半是系统调用
//! （需要真机）。两者之间只通过一个 trait 相连，因此决策测试不需要 NFS 服务端。
//!
//! 权限与 namespace 前提由启动门禁检查（见 [`probe_mount_permission`] 与
//! [`shares_host_namespace`]）：不满足时应拒绝启动，而不是每 250ms 失败一次。

use std::{
    ffi::CString,
    fs::{self, File, OpenOptions},
    io,
    os::{fd::AsRawFd, unix::fs::OpenOptionsExt},
    path::{Path, PathBuf},
};

use crate::node::vfs::ownerfs::fuse_policy::OUTLET_FUSE_LABEL;

/// 单次挂载尝试的上限。
///
/// 收敛循环按 `mirror_interval_ms`（默认 50ms）驱动，而一次 NFS 建挂可能因
/// 对端不可达而挂住整个 `timeo`（默认 10 分钟）。把整个循环押在一次建挂上，
/// 等于在对端重启期间连发布和自愈都停摆。这里只限制"尝试"本身：超时就当作
/// 这次失败，退避后重试。
///
/// 取值依据（实测）：成功的 NFS 挂载约 100ms（`outlet.import.mounted` 的
/// `elapsed_ms`），2s 留了 20 倍余量。原先的 10s 意味着对端失联时每轮发布
/// 被推迟 10s——实测点名到 `10069ms` 的发布延迟，正好是 10s 超时叠加正常
/// 开销。缩短到 2s 后，最坏启发式延迟降到 2s，且多个坏 home 并行后不再叠加。
///
/// 失败本身就很快：`ECONNREFUSED` 立即返回，`EHOSTUNREACH` 也快。真正吃满
/// 超时的是"包被静默丢弃"（防火墙 DROP、对端宕机），这种情形等更久也没有
/// 收益——恢复要靠下一轮重试，而不是让当前这轮多等。
pub const MOUNT_ATTEMPT_TIMEOUT: std::time::Duration = std::time::Duration::from_secs(2);

/// 本机 home 的挂载选项标记。执行器与 mounter 共用同一个字面量，避免两边
/// 各写一份后悄悄漂移。
pub const BIND_OPTIONS: &str = "bind";

/// 一次挂载操作的结果。
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct MountIdentity {
    /// 内核分配给这次挂载的 id，用于区分"还是原来那个挂载"与"被换掉了"。
    pub unique_mount_id: u64,
}

/// 执行挂载的抽象。
///
/// 生产实现调用内核；测试实现记录调用序列，让"一次失败不影响其他 home"
/// 这类性质可以在没有 NFS 的机器上验证。
pub trait ImportMounter: Send + Sync {
    /// 把 `source`（`<home>:<path>`）挂到 `target`。已挂载时返回 `EBUSY`。
    fn mount(&self, source: &str, target: &Path, options: &str) -> io::Result<MountIdentity>;

    /// 惰性卸载。`MNT_DETACH` 让仍有 fd 打开的挂载也能立刻从命名空间摘除，
    /// 老 fd 继续指向旧挂载，最后一个引用关闭后目录重新露出来。
    fn detach(&self, target: &Path) -> io::Result<()>;
}

/// 内核实现。
#[derive(Debug, Default)]
pub struct KernelMounter;

impl ImportMounter for KernelMounter {
    fn mount(&self, source: &str, target: &Path, options: &str) -> io::Result<MountIdentity> {
        let result = if options == BIND_OPTIONS {
            // The local home is a bind of our own data tree, not an NFS mount.
            std::process::Command::new("mount")
                .arg("--bind")
                .arg(source)
                .arg(target)
                .status()?
        } else {
            // Run `mount.nfs` rather than calling `mount(2)` directly. The
            // syscall alone is not enough for NFS: the kernel module is loaded
            // on demand, and the helper performs the version negotiation and
            // portmapper queries that a raw `mount(2)` skips. Calling the
            // syscall directly fails with EINVAL on a host that has never
            // mounted NFS, which is exactly the first-boot case this feature
            // exists for.
            // `-o` 里仍带 `hard`：数据面必须坚持，不能静默截断。但**建立
            // 挂载**这一步不能无限等——它跑在收敛线程里，一次卡住就把发布、
            // 自愈、孤儿告警全部冻住（实测 `mount.nfs` 挂了 4 分钟以上，整个
            // mirror 循环停在第一次 ensure_imports）。超时只放弃这次尝试，
            // 下轮由退避重试。
            run_with_timeout(
                std::process::Command::new("mount.nfs")
                    .arg("-o")
                    .arg(options)
                    .arg(source)
                    .arg(target),
                MOUNT_ATTEMPT_TIMEOUT,
            )?
        };
        if !result.success() {
            return Err(io::Error::other(format!(
                "mount helper exited with {result} for {source}"
            )));
        }
        Ok(MountIdentity {
            unique_mount_id: unique_mount_id(target).unwrap_or(0),
        })
    }

    #[allow(unsafe_code)]
    fn detach(&self, target: &Path) -> io::Result<()> {
        let target_c = cstring_path(target)?;
        // SAFETY: target_c is NUL-terminated and live. MNT_DETACH never fails on
        // a busy tree, which is what lets a remount proceed while a container
        // still reads from the old mount.
        let result = unsafe { libc::umount2(target_c.as_ptr(), libc::MNT_DETACH) };
        if result == 0 {
            Ok(())
        } else {
            // 已经不在了不是错误：目标状态就是"没有这个挂载"。
            let error = io::Error::last_os_error();
            if error.raw_os_error() == Some(libc::EINVAL)
                || error.raw_os_error() == Some(libc::ENOENT)
            {
                return Ok(());
            }
            Err(error)
        }
    }
}

fn cstring_path(path: &Path) -> io::Result<CString> {
    use std::os::unix::ffi::OsStrExt;
    CString::new(path.as_os_str().as_bytes())
        .map_err(|_| io::Error::from_raw_os_error(libc::EINVAL))
}

/// Before starting the mirror, recover only the private legacy FUSE mount left
/// by a dead daemon. Never follow the mountpoint itself as a symlink.
pub(crate) fn recover_legacy_outlet_mount(import_root: &Path) -> afs_error::Result<()> {
    let configured = import_root.join(super::mirror::LEGACY_MOUNT_DIR);
    let target = import_root
        .canonicalize()
        .map_err(|error| legacy_outlet_recovery_error(&configured, &error))?
        .join(super::mirror::LEGACY_MOUNT_DIR);
    recover_disconnected_outlet(&target)
        .map_err(|error| legacy_outlet_recovery_error(&target, &error))
}

fn legacy_outlet_recovery_error(target: &Path, error: &io::Error) -> afs_error::Error {
    afs_error::Error::coded(
        afs_error::CONFIG_INVALID,
        format!(
            "legacy OwnerFs outlet mount at {}: {error}. Inspect it with findmnt. After \
             confirming it is no longer in use, run umount -l on that exact path, then \
             restart afs-node",
            target.display()
        ),
    )
}

fn recover_disconnected_outlet(target: &Path) -> io::Result<()> {
    let read_mount_id = || legacy_outlet_mount_id(&fs::read("/proc/self/mountinfo")?, target);
    let Some(id) = read_mount_id()? else {
        return Ok(());
    };

    let (send, receive) = std::sync::mpsc::sync_channel(1);
    let probe_target = target.to_path_buf();
    // A broken or slow FUSE server must not be able to hang startup. This
    // thread only probes; it cannot detach anything, even after the deadline.
    std::thread::Builder::new()
        .name("ownerfs-outlet-probe".into())
        .spawn(move || {
            let _ = send.send(probe_disconnected_outlet(&probe_target, id));
        })?;
    let pinned = receive
        .recv_timeout(MOUNT_ATTEMPT_TIMEOUT)
        .map_err(|error| {
            io::Error::other(format!(
                "could not confirm a disconnected FUSE mount: {error}"
            ))
        })??;
    if read_mount_id()? != Some(id) {
        return Err(io::Error::other("mount identity changed"));
    }
    detach_disconnected_outlet(&pinned)?;
    if read_mount_id()?.is_some() {
        return Err(io::Error::other("mount remains after cleanup"));
    }
    afs_logging::info!(
        "outlet.legacy_mount_recovered";
        "mount_path" => target.display().to_string(),
        "mount_id" => id
    );
    Ok(())
}

fn legacy_outlet_mount_id(mountinfo: &[u8], target: &Path) -> io::Result<Option<u64>> {
    let wanted = target.as_os_str().as_encoded_bytes();
    let expected_source = format!("afs-{OUTLET_FUSE_LABEL}");
    let mut found = None;
    for line in mountinfo.split(|byte| *byte == b'\n') {
        let Some(mount) = super::topology::parse_line(line) else {
            continue;
        };
        if mount.mount_point == wanted {
            if found.is_some()
                || mount.root != b"/"
                || mount.fs_type != "fuse"
                || mount.source != expected_source
            {
                return Err(io::Error::other(
                    "could not confirm a single private OwnerFs outlet FUSE mount",
                ));
            }
            let id =
                std::str::from_utf8(line.split(|byte| *byte == b' ').next().unwrap_or_default())
                    .ok()
                    .and_then(|id| id.parse().ok())
                    .ok_or_else(|| io::Error::other("invalid kernel mount ID"))?;
            found = Some(id);
        } else if mount
            .mount_point
            .strip_prefix(wanted)
            .is_some_and(|rest| rest.starts_with(b"/"))
        {
            return Err(io::Error::other("outlet path contains a nested mount"));
        }
    }
    Ok(found)
}

#[allow(unsafe_code)]
fn probe_disconnected_outlet(target: &Path, id: u64) -> io::Result<File> {
    let mount = OpenOptions::new()
        .read(true)
        .custom_flags(libc::O_PATH | libc::O_NOFOLLOW | libc::O_DIRECTORY)
        .open(target)?;
    let fd = mount.as_raw_fd();
    let info = fs::read_to_string(format!("/proc/self/fdinfo/{fd}"))?;
    let pinned_id = info
        .lines()
        .find_map(|line| line.strip_prefix("mnt_id:"))
        .and_then(|value| value.trim().parse().ok());
    if pinned_id != Some(id) {
        return Err(io::Error::other("mount identity changed during FUSE probe"));
    }

    let mut stats = std::mem::MaybeUninit::<libc::statfs>::uninit();
    // The OwnerFs root's live statfs can return unsupported; only the kernel's
    // disconnected-transport errno is evidence for recovery.
    // SAFETY: mount owns fd and stats points to a writable statfs buffer.
    if unsafe { libc::fstatfs(fd, stats.as_mut_ptr()) } == 0 {
        return Err(io::Error::other("FUSE connection still responds"));
    }
    let error = io::Error::last_os_error();
    if error.raw_os_error() != Some(libc::ENOTCONN) {
        return Err(io::Error::other(format!(
            "could not confirm a disconnected FUSE mount: {error}"
        )));
    }
    Ok(mount)
}

#[allow(unsafe_code)]
fn detach_disconnected_outlet(mount: &File) -> io::Result<()> {
    let target = cstring_path(Path::new(&format!("/proc/self/fd/{}", mount.as_raw_fd())))?;
    // SAFETY: target is NUL-terminated. The procfs link refers to the pinned
    // mount root, so a replacement at the configured path cannot be detached.
    if unsafe { libc::umount2(target.as_ptr(), libc::MNT_DETACH) } != 0 {
        return Err(io::Error::other(format!(
            "cleanup of disconnected FUSE mount failed: {}",
            io::Error::last_os_error()
        )));
    }
    Ok(())
}

/// Disconnect the owned legacy outlet even if consumers still hold binds of
/// its FUSE tree. The private mount at this path is removed without lazy detach.
#[allow(unsafe_code)]
pub(super) fn unmount_legacy_fuse(path: &Path) -> afs_error::Result<()> {
    let target = cstring_path(path).map_err(afs_error::Error::from)?;
    // SAFETY: the CString is live and the caller has verified the mount ID.
    // Linux FUSE aborts the connection on MNT_FORCE, waking its receiver so
    // the daemon can join the callbacks even if a consumer still has a bind.
    if unsafe { libc::umount2(target.as_ptr(), libc::MNT_FORCE) } != 0 {
        return Err(afs_error::Error::from(io::Error::last_os_error()));
    }
    Ok(())
}

/// Run a mount helper with a deadline, killing it if the deadline passes.
///
/// A hung `mount.nfs` holds the convergence thread; killing the helper leaves
/// the kernel mount state alone (nothing was mounted yet), and the caller
/// retries under backoff.
fn run_with_timeout(
    command: &mut std::process::Command,
    timeout: std::time::Duration,
) -> io::Result<std::process::ExitStatus> {
    let mut child = command.spawn()?;
    let deadline = std::time::Instant::now() + timeout;
    loop {
        if let Some(status) = child.try_wait()? {
            return Ok(status);
        }
        if std::time::Instant::now() >= deadline {
            let _ = child.kill();
            let _ = child.wait();
            return Err(io::Error::new(
                io::ErrorKind::TimedOut,
                format!("mount helper did not finish within {timeout:?}"),
            ));
        }
        std::thread::sleep(std::time::Duration::from_millis(50));
    }
}

/// 从 mountinfo 里读挂载点对应的 `unique mount id`。
///
/// 读不到时返回 `None`——这不是错误，只说明这次挂载的 id 无法核对。调用方
/// 不应因此判定挂载失败：挂载本身已经成功了。
fn unique_mount_id(target: &Path) -> io::Result<u64> {
    use std::os::unix::ffi::OsStrExt;
    let mountinfo = std::fs::read("/proc/self/mountinfo")?;
    let wanted = target.as_os_str().as_bytes();
    for line in mountinfo.split(|byte| *byte == b'\n') {
        if line.is_empty() {
            continue;
        }
        let mut fields = line.split(|byte| *byte == b' ');
        // mountinfo: id parent major:minor root mount_point ...
        let Some(_id) = fields.next() else { continue };
        let Some(_parent) = fields.next() else {
            continue;
        };
        let Some(_dev) = fields.next() else { continue };
        let Some(_root) = fields.next() else { continue };
        let Some(point) = fields.next() else { continue };
        if decode_octal(point) != wanted {
            continue;
        }
        // 可选字段后面跟一个 '-' 分隔符，之后是 fs_type source super_options。
        // unique mount id 在挂载选项段里（kernel 自 5.8 起提供）。
        return Ok(parse_unique_id(line).unwrap_or(0));
    }
    Err(io::Error::from_raw_os_error(libc::ENOENT))
}

fn parse_unique_id(line: &[u8]) -> Option<u64> {
    let text = String::from_utf8_lossy(line);
    // `mnt_id` sits inside a comma-separated option list that is itself one
    // space-separated field, so splitting on spaces alone never isolates it.
    text.split([' ', ','])
        .filter_map(|field| field.strip_prefix("mnt_id="))
        .find_map(|value| value.parse::<u64>().ok())
}

/// mountinfo 的八进制转义（空格写成 `\040`）。
fn decode_octal(encoded: &[u8]) -> Vec<u8> {
    let mut out = Vec::with_capacity(encoded.len());
    let mut index = 0;
    while index < encoded.len() {
        if encoded[index] == b'\\' && index + 3 < encoded.len() {
            let digits = &encoded[index + 1..index + 4];
            if digits.iter().all(|byte| (b'0'..=b'7').contains(byte)) {
                let value = digits
                    .iter()
                    .fold(0u32, |acc, byte| acc * 8 + u32::from(byte - b'0'));
                out.push(u8::try_from(value).unwrap_or(b'?'));
                index += 4;
                continue;
            }
        }
        out.push(encoded[index]);
        index += 1;
    }
    out
}

/// 启动门禁：确认本进程与宿主机共享 mount namespace。
///
/// AFS 以 system service 运行时通常满足。若被隔离（`PrivateMounts=yes` 或容
/// 器），挂载对宿主机不可见，容器仍然看不到数据——此时应在启动阶段拒绝，
/// 而不是运行期反复挂载却无人受益。
///
/// 判据是根挂载 `/` 的**传播标志为 `shared:`**，而不是"存在根挂载"：容器
/// namespace 和 `PrivateMounts=yes` 的 mountinfo 里同样有 `/`（前者是
/// overlay 根，后者是克隆宿主得到的副本），单纯的存在性检查区分不出来。
/// 两种隔离方式都会把全部挂载重挂成 private，所以 shared 根是
/// "这里建挂载能传播给宿主"的可靠信号。
#[must_use]
pub fn shares_host_namespace(mountinfo: &[u8]) -> bool {
    let mounts = super::topology::parse(mountinfo);
    mounts
        .iter()
        .any(|mount| mount.mount_point == b"/" && mount.shared)
}

/// 启动门禁：用一次真实挂载试探本进程是否有 `mount(2)` 权限。
///
/// 挂到临时目录再立刻 detach，不留痕迹。失败说明缺少 `CAP_SYS_ADMIN`，
/// 动态导入开不起来。
pub fn probe_mount_permission(scratch_parent: &Path) -> io::Result<()> {
    let scratch = scratch_parent.join(".afs-mount-probe");
    std::fs::create_dir_all(&scratch)?;
    // 自己挂自己是最轻的试探：不需要网络，也不依赖任何服务端。
    let source = cstring_path(&scratch)?;
    let target = cstring_path(&scratch)?;
    #[allow(unsafe_code)]
    // SAFETY: both pointers are NUL-terminated CStrings live for the call.
    // A bind of a path onto itself is a no-op mount used only to test privilege.
    let result = unsafe {
        libc::mount(
            source.as_ptr(),
            target.as_ptr(),
            std::ptr::null(),
            libc::MS_BIND,
            std::ptr::null(),
        )
    };
    let outcome = if result == 0 {
        Ok(())
    } else {
        Err(io::Error::last_os_error())
    };
    let kernel = KernelMounter;
    let _ = kernel.detach(&scratch);
    let _ = std::fs::remove_dir(&scratch);
    outcome
}

/// 一个 home 的挂载点，与决策层的 [`super::imports::mount_point`] 一致。
#[must_use]
pub fn mount_point(import_root: &Path, home: &str) -> PathBuf {
    super::imports::mount_point(import_root, home)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn legacy_recovery_selects_only_the_private_outlet_mount() {
        let target = Path::new("/run/afs imports/.ownerfs-outlet");
        let unrelated =
            b"40 1 0:1 / /run/afs\x5c040imports/other rw - fuse afs-ownerfs-outlet rw\n";
        let outlet =
            b"41 1 0:2 / /run/afs\x5c040imports/.ownerfs-outlet rw - fuse afs-ownerfs-outlet rw\n";
        let foreign = b"42 1 0:3 / /run/afs\x5c040imports/.ownerfs-outlet rw - tmpfs tmpfs rw\n";
        let child =
            b"43 41 0:4 / /run/afs\x5c040imports/.ownerfs-outlet/nested rw - tmpfs tmpfs rw\n";
        let subtree = b"44 1 0:2 /proj /run/afs\x5c040imports/.ownerfs-outlet rw - fuse afs-ownerfs-outlet rw\n";

        assert_eq!(
            legacy_outlet_mount_id(unrelated, target).expect("no outlet mount"),
            None
        );
        assert_eq!(
            legacy_outlet_mount_id(&[unrelated.as_slice(), outlet].concat(), target)
                .expect("private outlet mount"),
            Some(41)
        );
        for mountinfo in [
            foreign.as_slice(),
            subtree,
            &[outlet.as_slice(), foreign].concat(),
            &[outlet.as_slice(), child].concat(),
        ] {
            assert!(legacy_outlet_mount_id(mountinfo, target).is_err());
        }
    }

    #[test]
    fn legacy_recovery_reports_kernel_cleanup_failure_with_operator_instructions() {
        let root = tempfile::tempdir().expect("temporary import root");
        let target = root.path().join(super::super::mirror::LEGACY_MOUNT_DIR);
        // This newly created ordinary file cannot be unmounted. Exercise the
        // real error path without needing FUSE, root or a fault-injection seam.
        let file = File::create_new(&target).expect("ordinary file at the reserved path");
        let error = detach_disconnected_outlet(&file).expect_err("ordinary file is not a mount");
        let diagnostic = legacy_outlet_recovery_error(&target, &error);
        assert_eq!(diagnostic.code(), afs_error::CONFIG_INVALID);
        for expected in [
            target.to_str().expect("UTF-8 temporary path"),
            "cleanup of disconnected FUSE mount failed",
            "findmnt",
            "umount -l on that exact path",
            "restart afs-node",
        ] {
            assert!(diagnostic.message().contains(expected), "{diagnostic}");
        }
    }

    #[test]
    fn octal_escapes_are_decoded() {
        assert_eq!(decode_octal(b"/mnt/a\\040b"), b"/mnt/a b");
        assert_eq!(decode_octal(b"/plain"), b"/plain");
        // 不完整的转义原样保留，避免把普通反斜杠吃掉。
        assert_eq!(decode_octal(b"/a\\04"), b"/a\\04");
    }

    #[test]
    fn unique_id_is_read_from_the_mount_options() {
        let line = b"36 1 0:32 / /mnt/x rw,relatime shared:1 - nfs4 host:/p rw,mnt_id=42";
        assert_eq!(parse_unique_id(line), Some(42));
        assert_eq!(
            parse_unique_id(b"36 1 0:32 / /mnt/x rw - nfs4 h:/p rw"),
            None
        );
    }

    /// 挂载助手必须能被超时打断。
    ///
    /// 真实故障：对端重启期间 `mount.nfs` 挂了 4 分钟以上，把整个镜像循环
    /// （发布、自愈、孤儿告警）全部冻住，因为它们跑在同一条收敛线程上。
    #[test]
    fn run_with_timeout_kills_a_hung_helper() {
        let started = std::time::Instant::now();
        let mut command = std::process::Command::new("sleep");
        command.arg("30");
        let result = run_with_timeout(&mut command, std::time::Duration::from_millis(200));
        let error = result.expect_err("a hung helper must not be waited on");
        assert_eq!(error.kind(), std::io::ErrorKind::TimedOut);
        assert!(
            started.elapsed() < std::time::Duration::from_secs(5),
            "must give up near the deadline, not after the helper's own runtime"
        );
    }

    /// 正常结束的助手不能被误杀。
    #[test]
    fn run_with_timeout_returns_the_real_exit_status() {
        let mut command = std::process::Command::new("true");
        let status = run_with_timeout(&mut command, std::time::Duration::from_secs(5))
            .expect("a fast helper completes");
        assert!(status.success());
    }

    #[test]
    fn host_namespace_is_detected_by_the_shared_root_mount() {
        // 正常宿主服务：systemd 把根挂载设为 shared。
        let with_root =
            b"1 0 8:1 / / rw,relatime shared:1 - ext4 /dev/vda1 rw\n2 1 0:3 / /proc rw shared:1 - proc proc rw\n";
        assert!(shares_host_namespace(with_root));

        // 容器 namespace：有 overlay 根挂载但传播是 private，宿主看不到
        // 这里建的挂载。
        let container =
            b"1 0 0:40 / / rw - overlay overlay rw\n2 1 0:3 / /proc rw - proc proc rw\n";
        assert!(!shares_host_namespace(container));

        // `PrivateMounts=yes`：克隆宿主 namespace（根挂载还在）但全部重挂
        // 成 private。
        let private_mounts = b"1 0 8:1 / / rw,relative - ext4 /dev/vda1 rw\n".to_vec();
        assert!(!shares_host_namespace(&private_mounts));

        // 私有 namespace：没有根挂载。
        let isolated = b"90 80 0:40 / /run/afs rw - tmpfs tmpfs rw\n";
        assert!(!shares_host_namespace(isolated));
    }
}
