//! R97: attribute replies report the length of the version the kernel page
//! cache holds for an inode, never a longer head.
use super::*;
use crate::node::vfs::PageCacheIdentity;
use fuser::{FileAttr, FileType};
use std::time::UNIX_EPOCH;

fn identity(version: &str, length: u64) -> PageCacheIdentity {
    PageCacheIdentity {
        version_id: Some(version.to_owned()),
        length,
    }
}

fn regular(ino: u64, size: u64) -> FileAttr {
    let attributes = FileAttributes {
        kind: FileKind::Regular,
        size,
        blocks: size.div_ceil(512),
        mode: 0o644,
        uid: 0,
        gid: 0,
        nlink: 1,
        atime: UNIX_EPOCH,
        mtime: UNIX_EPOCH,
        ctime: UNIX_EPOCH,
    };
    file_attr(ino, &attributes)
}

#[test]
fn cached_length_follows_the_live_page_cache_view() {
    let mut views = ActiveCachedViews::default();
    assert_eq!(views.cached_length(7), None);
    assert!(views.admit(7, 1, &identity("v0", 1024)));
    assert!(views.admit(7, 2, &identity("v0", 1024)));
    assert!(!views.admit(7, 3, &identity("v1", 9216)));
    assert_eq!(views.cached_length(7), Some(1024));
    views.release(7, 1);
    assert_eq!(views.cached_length(7), Some(1024));
    views.release(7, 2);
    assert_eq!(views.cached_length(7), None);
}

#[test]
fn head_attributes_are_capped_to_the_cached_version_length() {
    let views = Mutex::new(ActiveCachedViews::default());
    assert!(views.lock().unwrap().admit(7, 1, &identity("v0", 1024)));

    let head = regular(7, 9216);
    let replied = with_cached_view_size(&views, 7, head);
    assert_eq!(replied.size, 1024);
    assert_eq!(replied.blocks, 2);
    assert_eq!(replied.perm, head.perm, "only the length follows the view");
    assert_eq!(replied.mtime, head.mtime);

    // No view on another inode, and directories are never rewritten.
    assert_eq!(
        with_cached_view_size(&views, 8, regular(8, 9216)).size,
        9216
    );
    let mut directory = regular(7, 4096);
    directory.kind = FileType::Directory;
    assert_eq!(with_cached_view_size(&views, 7, directory).size, 4096);

    views.lock().unwrap().release(7, 1);
    assert_eq!(with_cached_view_size(&views, 7, head).size, 9216);
}

#[test]
fn a_poisoned_view_table_still_caps_lengths() {
    let views = Arc::new(Mutex::new(ActiveCachedViews::default()));
    assert!(views.lock().unwrap().admit(7, 1, &identity("v0", 1024)));
    let poison = views.clone();
    let _ = std::thread::spawn(move || {
        let _guard = poison.lock().unwrap();
        panic!("poison the view table");
    })
    .join();
    assert!(views.is_poisoned());
    assert_eq!(
        with_cached_view_size(&views, 7, regular(7, 9216)).size,
        1024
    );
}
