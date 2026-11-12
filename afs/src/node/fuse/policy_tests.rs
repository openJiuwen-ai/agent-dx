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
    dfs.policy = Arc::new(DfsFusePolicy);
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
    dfs.policy = Arc::new(DfsFusePolicy);
    assert!(!dfs.inline_local_open(local, libc::O_RDONLY));
}
