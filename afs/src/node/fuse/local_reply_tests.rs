use super::*;
use std::sync::mpsc;

struct LocalReplyBackend;
impl Backend for LocalReplyBackend {
    fn root_inode(&self) -> BackendInode {
        BackendInode { value: 1 }
    }
    fn getxattr_is_fixed_local(&self, name: &OsStr) -> bool {
        name.as_encoded_bytes() == b"security.capability"
    }
}
struct ConservativeBackend;
impl Backend for ConservativeBackend {
    fn root_inode(&self) -> BackendInode {
        BackendInode { value: 1 }
    }
}
fn queued_fs() -> AfsFuse {
    let mut fs = AfsFuse::new(Arc::new(LocalReplyBackend));
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
#[test]
fn local_reply_does_not_wait_for_accepted_inflight_body() {
    let (fs, release) = blocked_body_fs();
    let local = fs.clone();
    let (done_tx, done_rx) = mpsc::channel();
    let probe = thread::spawn(move || {
        let (_guard, result) = local.front_callback(
            FrontOrdering::FixedLocalReply,
            GateAccess::Exclusive,
            Some(1),
            false,
        );
        done_tx.send(result).unwrap();
    });
    let before = done_rx.recv_timeout(Duration::from_millis(100));
    let retained = fs.front_write.as_ref().unwrap().has_pending();
    release.send(()).unwrap();
    probe.join().unwrap();
    fs.front_write
        .as_ref()
        .unwrap()
        .wait(Some(1), false)
        .unwrap();
    assert!(
        retained,
        "probe must not drain or release the blocked payload"
    );
    assert_eq!(
        before.unwrap(),
        Ok(()),
        "fixed reply waited for backend body"
    );
}
#[test]
fn local_reply_does_not_consume_retry_or_replace_sticky_write_error() {
    let fs = queued_fs();
    let queue = fs.front_write.as_ref().unwrap();
    let mut first = true;
    queue
        .submit_with(1, 1, 1, || {
            Ok(Box::new(move || {
                if first {
                    first = false;
                    Err(libc::ENOSPC)
                } else {
                    Ok(())
                }
            }))
        })
        .unwrap();
    assert_eq!(queue.wait(Some(1), false), Err(libc::ENOSPC));
    let (_, result) = fs.front_callback(
        FrontOrdering::FixedLocalReply,
        GateAccess::Exclusive,
        Some(1),
        false,
    );
    assert_eq!(queue.wait(Some(1), false), Err(libc::ENOSPC));
    assert_eq!(
        queue.submit_with(1, 1, 1, || Ok(Box::new(|| Ok(())))),
        Err(libc::ENOSPC)
    );
    assert_eq!(result, Ok(()), "unrelated probe inherited the write error");
    queue.wait(Some(1), true).unwrap();
    assert!(!queue.has_pending());
}
#[test]
fn ordered_read_resize_and_sync_fence_still_waits_inflight_body() {
    let (fs, release) = blocked_body_fs();
    let ordered = fs.clone();
    let (done_tx, done_rx) = mpsc::channel();
    let waiter = thread::spawn(move || {
        let (_guard, result) = ordered.front_callback(
            FrontOrdering::WriteBarrier,
            GateAccess::Exclusive,
            Some(1),
            false,
        );
        done_tx.send(result).unwrap();
    });
    let blocked = done_rx.recv_timeout(Duration::from_millis(100)).is_err();
    release.send(()).unwrap();
    waiter.join().unwrap();
    assert!(blocked);
    assert_eq!(
        done_rx.recv_timeout(Duration::from_secs(2)).unwrap(),
        Ok(())
    );
}
#[test]
fn local_reply_does_not_wait_for_unrelated_dispatched_metadata() {
    let fs = Arc::new(queued_fs());
    let (entered_tx, entered_rx) = mpsc::channel();
    let (release_tx, release_rx) = mpsc::channel();
    fs.dispatch.submit(move || {
        entered_tx.send(()).unwrap();
        release_rx.recv().unwrap();
    });
    entered_rx.recv_timeout(Duration::from_secs(2)).unwrap();
    let local = fs.clone();
    let (done_tx, done_rx) = mpsc::channel();
    let probe = thread::spawn(move || {
        let (_guard, result) = local.front_callback(
            FrontOrdering::FixedLocalReply,
            GateAccess::Exclusive,
            Some(1),
            false,
        );
        done_tx.send(result).unwrap();
    });
    let before = done_rx.recv_timeout(Duration::from_millis(100));
    release_tx.send(()).unwrap();
    probe.join().unwrap();
    assert_eq!(before.unwrap(), Ok(()));
}
#[test]
fn xattr_local_classification_requires_backend_opt_in_and_exact_name() {
    assert_eq!(
        xattr_front_ordering(&ConservativeBackend, OsStr::new("security.capability")),
        FrontOrdering::WriteBarrier
    );
    assert_eq!(
        xattr_front_ordering(&LocalReplyBackend, OsStr::new("security.capability")),
        FrontOrdering::FixedLocalReply
    );
    for name in [
        "user.value",
        "security.capability.extra",
        "security.selinux",
    ] {
        assert_eq!(
            xattr_front_ordering(&LocalReplyBackend, OsStr::new(name)),
            FrontOrdering::WriteBarrier
        );
    }
    assert_eq!(
        unsupported_lock_errno(0, 1, libc::F_WRLCK),
        libc::EOPNOTSUPP
    );
    assert_eq!(unsupported_lock_errno(2, 1, libc::F_WRLCK), libc::EINVAL);
    assert_eq!(
        validate_xattr_set(libc::XATTR_CREATE | libc::XATTR_REPLACE, 0),
        Err(libc::EINVAL)
    );
    assert_eq!(validate_xattr_set(0, 1), Err(libc::EINVAL));
}
