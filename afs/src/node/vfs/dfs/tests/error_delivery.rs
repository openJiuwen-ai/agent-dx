// S2 G1：持久相关错误的交付。一次后台失败经同一个入口交付：同一句柄的 fsync/flush/write
// 报告一次；只 write 不 sync 的句柄在 release 时报告；没人接收的错误交给下一个写者
// （Linux errseq 语义：未被任何人看到的错误，新 open 也会看到一次）。交付后的错误不再
// 阻止该 inode 换新租约或刷新已提交视图。

use super::*;

fn create_rw(fs: &DistributedFs, name: &str) -> crate::node::vfs::types::CreatedFile {
    fs.create(
        &context(),
        fs.root_inode(),
        OsStr::new(name),
        0o640,
        libc::O_RDWR,
    )
    .unwrap()
}

fn bump_lease_epoch(meta: &RecordingMeta) -> u64 {
    let mut lease = meta.lease.lock().unwrap();
    lease.lease_epoch = lease.lease_epoch.saturating_add(1);
    lease.expires_at_unix_ms = now_unix_ms().saturating_add(30_000);
    lease.lease_epoch
}

/// 后台提交失败后，同一句柄的 fsync 报告该错误一次，下一次 fsync 重试并成功提交。
#[test]
fn s2_background_commit_failure_reaches_the_same_handle_fsync_once() {
    let (_temp, meta, fs) = test_fs();
    let created = create_rw(&fs, "bg-fsync.bin");
    fs.write(&context(), created.handle, 0, b"data").unwrap();
    meta.fail_next_commit();
    fs.writeback_pending().unwrap();
    assert_eq!(meta.commit_count(), 0);
    let error = fs
        .fsync(&context(), created.handle, SyncMode::Full)
        .unwrap_err();
    assert!(error.to_string().contains("injected test commit failure"));
    fs.fsync(&context(), created.handle, SyncMode::Full)
        .unwrap();
    assert_eq!(meta.commit_count(), 1);
    fs.release(&context(), created.handle).unwrap();
}

/// 只 write 不 sync 的句柄：后台失败在 release（SDK 路径式 close 的最后一步）时返回。
#[test]
fn s2_unobserved_background_error_is_returned_by_release() {
    let (_temp, meta, fs) = test_fs();
    let created = create_rw(&fs, "bg-release.bin");
    fs.write(&context(), created.handle, 0, b"data").unwrap();
    meta.fail_next_commit();
    fs.writeback_pending().unwrap();
    let error = fs.release(&context(), created.handle).unwrap_err();
    assert!(
        error.to_string().contains("injected test commit failure"),
        "{error}"
    );
    // 交付后后台重试照常进行，数据最终提交。
    fs.writeback_pending().unwrap();
    assert_eq!(meta.commit_count(), 1);
}

/// 已经由 flush 交付过的错误，release 不再重复报告。
#[test]
fn s2_flush_then_release_reports_one_background_error_once() {
    let (_temp, meta, fs) = test_fs();
    let created = create_rw(&fs, "bg-flush.bin");
    fs.write(&context(), created.handle, 0, b"data").unwrap();
    meta.fail_next_commit();
    fs.writeback_pending().unwrap();
    assert!(fs.flush(&context(), created.handle).is_err());
    fs.release(&context(), created.handle).unwrap();
}

/// 错误交付后，同 inode 可在新租约下重新打开写并成功提交（修复前：install_write_state 因
/// `background_error` 恒非空返回 stale，直到 Node 重启）。
#[test]
fn s2_delivered_background_error_does_not_block_a_fresh_lease() {
    let (_temp, meta, fs) = test_fs();
    let created = create_rw(&fs, "bg-reopen.bin");
    fs.write(&context(), created.handle, 0, b"data").unwrap();
    meta.fail_next_commit();
    fs.writeback_pending().unwrap();
    assert!(
        fs.fsync(&context(), created.handle, SyncMode::Full)
            .is_err()
    );
    fs.fsync(&context(), created.handle, SyncMode::Full)
        .unwrap();
    fs.release(&context(), created.handle).unwrap();
    fs.writeback_pending().unwrap();

    let fresh_epoch = bump_lease_epoch(&meta);
    let handle = fs
        .open(&context(), created.entry.inode, libc::O_RDWR)
        .unwrap();
    let inode_id = meta.inode.lock().unwrap().inode_id.clone();
    assert_eq!(
        fs.write_state(&inode_id)
            .unwrap()
            .unwrap()
            .lock()
            .unwrap()
            .write_lease
            .lease_epoch,
        fresh_epoch
    );
    fs.write(&context(), handle, 4, b"more").unwrap();
    fs.fsync(&context(), handle, SyncMode::Full).unwrap();
    assert_eq!(meta.commit_count(), 2);
    fs.release(&context(), handle).unwrap();
}

/// 最后写者关闭之后的后台失败没有句柄可交付：它不卡住换租约，并交给下一个写者一次。
#[test]
fn s2_orphan_background_error_reaches_the_next_writer_once_after_lease_change() {
    let (_temp, meta, fs) = test_fs();
    let created = create_rw(&fs, "bg-orphan.bin");
    fs.write(&context(), created.handle, 0, b"data").unwrap();
    fs.release(&context(), created.handle).unwrap();
    meta.fail_next_commit();
    fs.writeback_pending().unwrap();
    assert_eq!(meta.commit_count(), 0);
    // 后台重试成功，只剩一个没人看到的错误。
    fs.writeback_pending().unwrap();
    assert_eq!(meta.commit_count(), 1);

    bump_lease_epoch(&meta);
    let handle = fs
        .open(&context(), created.entry.inode, libc::O_RDWR)
        .unwrap();
    let error = fs.fsync(&context(), handle, SyncMode::Full).unwrap_err();
    assert!(error.to_string().contains("injected test commit failure"));
    fs.fsync(&context(), handle, SyncMode::Full).unwrap();
    let second = fs
        .open(&context(), created.entry.inode, libc::O_RDWR)
        .unwrap();
    fs.fsync(&context(), second, SyncMode::Full).unwrap();
    fs.release(&context(), second).unwrap();
    fs.release(&context(), handle).unwrap();
}

/// 每个已打开的写句柄各自看到一次（errseq）：A 交付后 B 仍会看到，之后都不再看到。
#[test]
fn s2_each_open_writer_sees_a_background_error_once() {
    let (_temp, meta, fs) = test_fs();
    let created = create_rw(&fs, "bg-two.bin");
    let second = fs
        .open(&context(), created.entry.inode, libc::O_RDWR)
        .unwrap();
    fs.write(&context(), created.handle, 0, b"data").unwrap();
    meta.fail_next_commit();
    fs.writeback_pending().unwrap();
    assert!(
        fs.fsync(&context(), created.handle, SyncMode::Full)
            .is_err()
    );
    assert!(fs.release(&context(), second).is_err());
    fs.fsync(&context(), created.handle, SyncMode::Full)
        .unwrap();
    fs.release(&context(), created.handle).unwrap();
}

/// 远端写者：owner 上的句柄释放时，未交付错误经 RPC 返回给调用者；owner 的句柄与 open
/// 身份已清理，重试 release 幂等成功。
#[test]
fn s2_owner_release_returns_undelivered_error_after_finishing_cleanup() {
    use crate::node::vfs::dfs::owner_api::{DfsOwnerFilesHandler, DfsOwnerLifecycleHandler};

    let (_temp, meta, fs) = test_fs();
    let created = create_rw(&fs, "bg-owner.bin");
    Backend::release(&fs, &context(), created.handle).unwrap();
    let lease = meta.lease.lock().unwrap().clone();
    let open = DfsOwnerLifecycleHandler::open(
        &fs,
        "node-b",
        afs_protocol::node_control::DfsOwnerOpenRequest {
            namespace_id: "default".into(),
            inode_id: "inode:test".into(),
            owner_node_id: "node-a".into(),
            owner_session_id: "session-a".into(),
            lease_epoch: lease.lease_epoch,
            caller_session_id: "session-b".into(),
            open_flags: libc::O_RDWR,
            kill_suidgid: false,
            open_seq: 1,
        },
    )
    .unwrap();
    let handle = open.handle.expect("owner open returns handle");
    DfsOwnerFilesHandler::write(
        &fs,
        "node-b",
        afs_protocol::node_data::DfsOwnerWriteRequest {
            handle: Some(data_owner_handle(&handle)),
            operation_id: "owner-bg-write".into(),
            offset: 0,
            data: b"remote".to_vec(),
            append: false,
            kill_suidgid: false,
        },
    )
    .unwrap();
    meta.fail_next_commit();
    fs.writeback_pending().unwrap();
    let release = afs_protocol::node_control::DfsOwnerReleaseRequest {
        handle: Some(handle),
    };
    let error = DfsOwnerLifecycleHandler::release(&fs, "node-b", release.clone()).unwrap_err();
    assert!(error.to_string().contains("injected test commit failure"));
    DfsOwnerLifecycleHandler::release(&fs, "node-b", release).unwrap();
}
