//! Performance batch 1 (A): the open-time name check behind positive kernel
//! entry TTLs. `RecordingMeta` holds one inode that every lookup returns;
//! changing its id models another Node recreating (or renaming over) the name.
use super::*;

fn with_check(fs: DistributedFs, ttl: Duration) -> DistributedFs {
    fs.with_open_name_check(ttl, None).unwrap()
}

/// Let a recorded name age past the fresh-lookup window, so opens compare it.
fn age_names() {
    std::thread::sleep(super::super::recent_names::FRESH_LOOKUP * 2);
}

fn replace_named_inode(meta: &RecordingMeta, id: &str) {
    meta.inode.lock().unwrap().inode_id = InodeId::new(id);
}

#[test]
fn open_through_a_replaced_name_is_stale_once_and_the_retry_opens_the_new_inode() {
    let (_temp, meta, fs) = test_fs();
    let fs = with_check(fs, Duration::from_secs(1));
    let old = fs
        .lookup(&context(), fs.root_inode(), OsStr::new("f"))
        .unwrap();
    age_names();
    replace_named_inode(&meta, "recreated");
    let before = meta.get_inode_call_count();
    let error = fs.open(&context(), old.inode, libc::O_RDONLY).unwrap_err();
    assert_eq!(error.code(), afs_error::NODE_DFS_STALE_NAME);
    assert_eq!(crate::error::errno(&error), libc::ESTALE);
    assert_eq!(
        meta.get_inode_call_count(),
        before,
        "the name check replaces GetInode with Lookup"
    );
    // Linux retries once with LOOKUP_REVAL: a fresh LOOKUP, then OPEN.
    let new = fs
        .lookup(&context(), fs.root_inode(), OsStr::new("f"))
        .unwrap();
    assert_ne!(new.inode, old.inode);
    let handle = fs.open(&context(), new.inode, libc::O_RDONLY).unwrap();
    fs.release(&context(), handle).unwrap();
    // Later opens of the new inode are checked by name again, and pass.
    age_names();
    let before = meta.get_inode_call_count();
    let handle = fs.open(&context(), new.inode, libc::O_RDONLY).unwrap();
    assert_eq!(meta.get_inode_call_count(), before);
    fs.release(&context(), handle).unwrap();
}

#[test]
fn open_through_a_current_name_costs_one_lookup_and_no_get_inode() {
    let (_temp, meta, fs) = test_fs();
    let fs = with_check(fs, Duration::from_secs(1));
    let entry = fs
        .lookup(&context(), fs.root_inode(), OsStr::new("f"))
        .unwrap();
    age_names();
    let before = meta.get_inode_call_count();
    let handle = fs.open(&context(), entry.inode, libc::O_RDONLY).unwrap();
    assert_eq!(meta.get_inode_call_count(), before);
    fs.release(&context(), handle).unwrap();
}

#[test]
fn zero_ttl_keeps_the_inode_only_open_check() {
    let (_temp, meta, fs) = test_fs();
    let fs = with_check(fs, Duration::ZERO);
    let old = fs
        .lookup(&context(), fs.root_inode(), OsStr::new("f"))
        .unwrap();
    replace_named_inode(&meta, "recreated");
    let before = meta.get_inode_call_count();
    // Today's behavior: only "does this inode exist" is asked.
    let result = fs.open(&context(), old.inode, libc::O_RDONLY);
    assert_eq!(meta.get_inode_call_count(), before + 1);
    if let Ok(handle) = result {
        fs.release(&context(), handle).unwrap();
    }
}
