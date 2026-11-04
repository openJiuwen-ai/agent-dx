use super::*;
use std::sync::mpsc;
fn queued_fs() -> AfsFuse {
    let mut fs = AfsFuse::new(Arc::new(EmptyBackend));
    fs.front_write = Some(Arc::new(front_write::FrontWriteQueue::new(32).unwrap()));
    fs
}
fn blocked_body_fs() -> (Arc<AfsFuse>, mpsc::Sender<()>) {
    let fs = queued_fs();
    let (entered_tx, entered_rx) = mpsc::channel();
    let (release_tx, release_rx) = mpsc::channel();
    fs.front_write
        .as_ref()
        .unwrap()
        .submit_with(1, 1, 1, || {
            Ok(Box::new(move || {
                entered_tx.send(()).unwrap();
                release_rx.recv().unwrap();
                Ok(())
            }))
        })
        .unwrap();
    entered_rx.recv_timeout(Duration::from_secs(2)).unwrap();
    (Arc::new(fs), release_tx)
}

struct EmptyBackend;
impl Backend for EmptyBackend {
    fn root_inode(&self) -> BackendInode {
        BackendInode { value: 1 }
    }
}
#[test]
fn synchronous_append_and_writeback_requests_keep_old_ack_boundary() {
    assert!(ordinary_queued_write(
        OpenFlags(libc::O_RDWR),
        WriteFlags::empty()
    ));
    for flags in [libc::O_SYNC, libc::O_DSYNC, libc::O_APPEND] {
        assert!(!ordinary_queued_write(
            OpenFlags(libc::O_RDWR | flags),
            WriteFlags::empty()
        ));
    }
    assert!(!ordinary_queued_write(
        OpenFlags(libc::O_RDWR),
        WriteFlags::FUSE_WRITE_CACHE
    ));
}
#[test]
fn configured_front_write_limit_reaches_the_mount_queue() {
    let options = FuseMountOptions {
        dfs_write_queue_bytes: 64 * 1024 * 1024,
        ..Default::default()
    };
    options.validate().unwrap();
    let mut fs = AfsFuse::with_mount_options(Arc::new(EmptyBackend), options);
    fs.enable_write_queue().unwrap();
    let q = fs.front_write.as_ref().unwrap();
    let (tx, rx) = std::sync::mpsc::channel();
    q.submit_deferred_with(
        1,
        11,
        64 * 1024 * 1024 + 1,
        || panic!("configured limit must reject before allocation"),
        move |r| tx.send(r).unwrap(),
    );
    assert_eq!(
        rx.recv_timeout(std::time::Duration::from_secs(2)).unwrap(),
        Err(libc::E2BIG)
    );
    assert!(
        FuseMountOptions {
            dfs_write_queue_bytes: 0,
            ..Default::default()
        }
        .validate()
        .is_err()
    );
}

#[test]
fn normal_dfs_setup_enables_owned_write_admission_without_an_option() {
    let mut fs = AfsFuse::new(Arc::new(EmptyBackend));
    fs.enable_write_queue().unwrap();
    assert!(fs.front_write.is_some());
    assert_eq!(
        fs.mount_options.dfs_write_queue_bytes,
        2 * 1024 * 1024 * 1024
    );
    assert!(
        !fs.backend
            .accepts_queued_writes(crate::node::vfs::types::FileHandle(1))
    );
}
#[test]
fn last_close_observes_earlier_release_before_retiring_failed_inode() {
    let mut fs = AfsFuse::new(Arc::new(EmptyBackend));
    fs.enable_write_queue().unwrap();
    let fs = Arc::new(fs);
    fs.file_handle_inodes
        .lock()
        .unwrap()
        .extend([(11, 1), (12, 1)]);
    let queue = fs.front_write.as_ref().unwrap();
    queue
        .submit_with(1, 11, 1, || Ok(Box::new(|| Err(libc::ENOSPC))))
        .unwrap();
    assert_eq!(queue.wait(Some(1), false), Err(libc::ENOSPC));
    let handles = fs.file_handle_inodes.clone();
    let (entered_tx, entered_rx) = std::sync::mpsc::channel();
    let (release_tx, release_rx) = std::sync::mpsc::channel();
    let (first_guard, _) = fs.front_callback(FrontOrdering::WriteAdmission, Some(1), false);
    fs.dispatch.submit_keyed(1, move || {
        let _first_guard = first_guard;
        entered_tx.send(()).unwrap();
        release_rx.recv().unwrap();
        handles.lock().unwrap().remove(&11);
    });
    entered_rx.recv_timeout(Duration::from_secs(2)).unwrap();
    let closing = fs.clone();
    let (done_tx, done_rx) = std::sync::mpsc::channel();
    let close = thread::spawn(move || {
        let (_guard, _) = closing.front_callback(FrontOrdering::WriteBarrier, Some(1), true);
        closing.retire_front_handle(1, 12);
        done_tx.send(()).unwrap();
    });
    let blocked = done_rx.recv_timeout(Duration::from_millis(100)).is_err();
    release_tx.send(()).unwrap();
    close.join().unwrap();
    assert!(blocked, "last close raced an earlier handle-table removal");
    assert_eq!(queue.wait(Some(1), false), Ok(()));
    assert!(!queue.has_pending());
}
#[test]
fn no_write_backlog_does_not_serialize_existing_read_dispatch() {
    let mut fs = AfsFuse::new(Arc::new(EmptyBackend));
    fs.enable_write_queue().unwrap();
    let fs = Arc::new(fs);
    let (entered_tx, entered_rx) = std::sync::mpsc::channel();
    let (release_tx, release_rx) = std::sync::mpsc::channel();
    fs.dispatch.submit(move || {
        entered_tx.send(()).unwrap();
        release_rx.recv().unwrap();
    });
    entered_rx.recv_timeout(Duration::from_secs(2)).unwrap();
    let next = fs.clone();
    let (done_tx, done_rx) = std::sync::mpsc::channel();
    let check = thread::spawn(move || {
        let (_guard, result) = next.front_callback(FrontOrdering::WriteBarrier, Some(2), false);
        done_tx.send(result).unwrap();
    });
    let result = done_rx.recv_timeout(Duration::from_millis(100));
    release_tx.send(()).unwrap();
    check.join().unwrap();
    assert_eq!(result.unwrap(), Ok(()));
}
#[test]
fn file_barrier_unrelated_dispatch_does_not_delay_write_admission() {
    let fs = Arc::new(queued_fs());
    let (entered_tx, entered_rx) = mpsc::channel();
    let (release_tx, release_rx) = mpsc::channel();
    fs.dispatch.submit_keyed(2, move || {
        entered_tx.send(()).unwrap();
        release_rx.recv().unwrap();
    });
    entered_rx.recv_timeout(Duration::from_secs(2)).unwrap();
    let other = fs.clone();
    let (done_tx, done_rx) = mpsc::channel();
    let worker = thread::spawn(move || {
        let (_guard, result) = other.front_callback(FrontOrdering::WriteAdmission, Some(1), false);
        done_tx.send(result).unwrap();
    });
    let before = done_rx.recv_timeout(Duration::from_millis(100));
    release_tx.send(()).unwrap();
    worker.join().unwrap();
    assert_eq!(before.unwrap(), Ok(()), "file1 waited for file2 dispatch");
}

#[test]
fn file_barrier_unrelated_body_does_not_delay_other_file() {
    let (fs, release) = blocked_body_fs();
    let other = fs.clone();
    let (done_tx, done_rx) = mpsc::channel();
    let worker = thread::spawn(move || {
        let (_guard, result) = other.front_callback(FrontOrdering::WriteBarrier, Some(2), false);
        done_tx.send(result).unwrap();
    });
    let before = done_rx.recv_timeout(Duration::from_millis(100));
    release.send(()).unwrap();
    worker.join().unwrap();
    assert_eq!(before.unwrap(), Ok(()), "file2 waited for file1 body");
}

#[test]
fn file_barrier_recovery_uses_reserved_admission_when_normal_tasks_are_full() {
    let dispatch = FuseDispatch::with_capacity(2, 1);
    let (entered_tx, entered_rx) = mpsc::channel();
    let (release_tx, release_rx) = mpsc::channel();
    dispatch.submit_keyed(2, move || {
        entered_tx.send(()).unwrap();
        release_rx.recv().unwrap();
    });
    entered_rx.recv_timeout(Duration::from_secs(2)).unwrap();
    let recovery = FuseRecoverySender {
        shared: dispatch.shared.clone(),
        normal_capacity: None,
    };
    let (tx, rx) = mpsc::channel();
    recovery.submit(1, move || tx.send(()).unwrap());
    let before = rx.recv_timeout(Duration::from_secs(2));
    release_tx.send(()).unwrap();
    dispatch.drain();
    before.unwrap();
}
