//! FUSE-layer decisions driven by a Home-like (OwnerFs-shaped) mount policy.
use super::*;

struct EmptyBackend;
impl Backend for EmptyBackend {
    fn root_inode(&self) -> BackendInode {
        BackendInode { value: 1 }
    }
}

const PRIVATE_ROOT: u64 = 10;
const SHARED_ROOT: u64 = 20;
const REMOTE: u64 = 30;

/// Mirrors OwnerFs: a private Home root caches for 1s with page cache, a
/// shared root does not cache, and only local Home inodes run inline.
#[derive(Default)]
struct HomeLikePolicy {
    remembered: Mutex<Vec<u64>>,
}

impl FuseMountPolicy for HomeLikePolicy {
    fn label(&self) -> &'static str {
        "ownerfs"
    }
    fn remember_inode(&self, ino: u64) {
        self.remembered.lock().unwrap().push(ino);
    }
    fn inline_local(&self, inode: BackendInode) -> bool {
        matches!(inode.value, PRIVATE_ROOT | SHARED_ROOT)
    }
    fn with_cache_policy(&self, inode: BackendInode, reply: &mut dyn FnMut(Duration, bool)) {
        if inode.value == PRIVATE_ROOT {
            reply(Duration::from_secs(1), true);
        } else {
            reply(Duration::ZERO, false);
        }
    }
}

fn home_fs() -> (AfsFuse, Arc<HomeLikePolicy>) {
    let policy = Arc::new(HomeLikePolicy::default());
    let mut fs = AfsFuse::new(Arc::new(EmptyBackend));
    fs.policy = policy.clone();
    (fs, policy)
}

fn create_reply(fs: &AfsFuse, inode: u64) -> (Duration, FopenFlags) {
    let mut out = None;
    fs.with_cache_policy(BackendInode { value: inode }, |ttl, private| {
        out = Some(fs.create_reply_policy(ttl, private));
    });
    out.unwrap()
}

#[test]
fn ownerfs_private_root_keeps_policy_ttl_and_page_cache() {
    let (fs, _) = home_fs();
    assert_eq!(
        create_reply(&fs, PRIVATE_ROOT),
        (Duration::from_secs(1), FopenFlags::empty())
    );
    let mut open_flags = None;
    fs.with_cache_policy(
        BackendInode {
            value: PRIVATE_ROOT,
        },
        |_, private| {
            open_flags = Some(open_reply_flags(!private));
        },
    );
    assert_eq!(open_flags, Some(FopenFlags::empty()));
}

#[test]
fn ownerfs_shared_root_is_uncached_and_direct() {
    let (fs, _) = home_fs();
    assert_eq!(create_reply(&fs, SHARED_ROOT), (Duration::ZERO, DIRECT_IO));
    assert_eq!(create_reply(&fs, REMOTE), (Duration::ZERO, DIRECT_IO));
    // The DFS kernel-cache feature, not the policy, bounds DFS creates.
    let mut dfs = AfsFuse::new(Arc::new(EmptyBackend));
    dfs.policy = Arc::new(DfsFusePolicy::new(Duration::ZERO));
    dfs.cached_io = true;
    assert_eq!(
        create_reply(&dfs, PRIVATE_ROOT),
        (Duration::ZERO, FopenFlags::empty())
    );
}

#[test]
fn ownerfs_inline_local_only_for_read_only_local_opens_and_remembers_inodes() {
    let (fs, policy) = home_fs();
    let local = BackendInode {
        value: PRIVATE_ROOT,
    };
    assert!(fs.inline_local_open(local, libc::O_RDONLY));
    assert!(fs.inline_local_open(BackendInode { value: SHARED_ROOT }, libc::O_RDONLY));
    assert!(!fs.inline_local_open(local, libc::O_RDWR));
    assert!(!fs.inline_local_open(BackendInode { value: REMOTE }, libc::O_RDONLY));
    fs.remember_cached_inode(42);
    assert_eq!(*policy.remembered.lock().unwrap(), vec![42]);

    let mut dfs = AfsFuse::new(Arc::new(EmptyBackend));
    dfs.policy = Arc::new(DfsFusePolicy::new(Duration::from_secs(1)));
    assert!(!dfs.inline_local_open(local, libc::O_RDONLY));
}

/// A: the DFS policy hands its configured kernel TTL to lookup/getattr,
/// to the entry replies the policy does not see per inode (mkdir, mknod,
/// symlink, link, setattr) and, with the kernel cache feature, to create.
#[test]
fn dfs_policy_ttl_bounds_entry_attr_and_create_replies() {
    let ttl = Duration::from_millis(1000);
    let mut dfs = AfsFuse::new(Arc::new(EmptyBackend));
    dfs.policy = Arc::new(DfsFusePolicy::new(ttl));
    let mut seen = None;
    dfs.with_cache_policy(BackendInode { value: 7 }, |ttl, private| {
        seen = Some((ttl, private));
    });
    assert_eq!(seen, Some((ttl, false)));
    assert_eq!(dfs.entry_ttls(), (ttl, ttl));
    dfs.cached_io = true;
    // CREATE makes a regular file whose attributes are never cached, and
    // ReplyCreate carries a single TTL.
    assert_eq!(create_reply(&dfs, 7), (Duration::ZERO, FopenFlags::empty()));
}

fn attributes_of(kind: FileKind) -> FileAttributes {
    FileAttributes {
        kind,
        size: 0,
        blocks: 0,
        mode: 0o644,
        uid: 0,
        gid: 0,
        nlink: 1,
        atime: std::time::UNIX_EPOCH,
        mtime: std::time::UNIX_EPOCH,
        ctime: std::time::UNIX_EPOCH,
    }
}

const ALL_KINDS: [FileKind; 4] = [
    FileKind::Regular,
    FileKind::Directory,
    FileKind::Symlink,
    FileKind::Special(SpecialFileKind::Fifo),
];

/// A (F4): DFS caches names for every kind and attributes only for
/// directories; LOOKUP, MKDIR, MKNOD, SYMLINK, LINK, SETATTR and GETATTR all
/// take their TTLs from these two rules.
#[test]
fn dfs_caches_names_for_all_kinds_and_attributes_only_for_directories() {
    let ttl = Duration::from_secs(1);
    let mut dfs = AfsFuse::new(Arc::new(EmptyBackend));
    dfs.policy = Arc::new(DfsFusePolicy::new(ttl));
    for kind in ALL_KINDS {
        let expected_attr = if kind == FileKind::Directory {
            ttl
        } else {
            Duration::ZERO
        };
        assert_eq!(
            dfs.reply_ttls(&attributes_of(kind)),
            (ttl, expected_attr),
            "{kind:?}"
        );
        assert_eq!(
            dfs.policy.attr_ttl(kind == FileKind::Directory, ttl),
            expected_attr,
            "{kind:?}"
        );
    }
}

#[test]
fn non_dfs_policies_cache_attributes_like_names() {
    let (fs, _) = home_fs();
    let ttl = Duration::from_secs(1);
    for kind in ALL_KINDS {
        assert_eq!(fs.policy.attr_ttl(kind == FileKind::Directory, ttl), ttl);
        assert_eq!(
            fs.reply_ttls(&attributes_of(kind)),
            (Duration::ZERO, Duration::ZERO)
        );
    }
}

#[test]
fn dfs_policy_ttl_zero_restores_uncached_replies() {
    let mut dfs = AfsFuse::new(Arc::new(EmptyBackend));
    dfs.policy = Arc::new(DfsFusePolicy::new(Duration::ZERO));
    dfs.cached_io = true;
    assert_eq!(dfs.entry_ttls(), (Duration::ZERO, Duration::ZERO));
    assert_eq!(create_reply(&dfs, 7), (Duration::ZERO, FopenFlags::empty()));
}

#[test]
fn non_dfs_policies_keep_uncached_entry_replies() {
    let (fs, _) = home_fs();
    assert_eq!(fs.entry_ttls(), (Duration::ZERO, Duration::ZERO));
    let fs = AfsFuse::new(Arc::new(EmptyBackend));
    assert_eq!(fs.entry_ttls(), (Duration::ZERO, Duration::ZERO));
}
