use std::ffi::OsStr;

use afs::node::vfs::{
    Backend,
    types::{BackendInode, RequestContext},
};
use afs_error::ErrorKind;

struct DefaultBackend;

impl Backend for DefaultBackend {
    fn root_inode(&self) -> BackendInode {
        BackendInode { value: 7 }
    }
}

#[test]
fn backend_contract_binds_one_root_and_rejects_unimplemented_operations() {
    let backend = DefaultBackend;
    let root = backend.root_inode();
    assert_eq!(root.value, 7);

    let context = RequestContext {
        uid: 1000,
        gid: 1000,
        pid: 42,
        umask: 0o022,
        supplementary_gids: Vec::new(),
    };
    assert_eq!(
        backend
            .lookup(&context, root, OsStr::new("file"))
            .unwrap_err()
            .kind(),
        ErrorKind::Unimplemented
    );
    assert_eq!(
        backend
            .create(&context, root, OsStr::new("file"), 0o644, 0)
            .unwrap_err()
            .kind(),
        ErrorKind::Unimplemented
    );
}
