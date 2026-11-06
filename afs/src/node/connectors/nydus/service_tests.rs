//! Fault boundary: rename moves the real namespace, then loses its reply.
use super::*;
use crate::node::vfs::types::{CreatedFile, FileAttributes, FileKind, RenameFlags};
use std::{
    collections::HashMap,
    ffi::OsString,
    sync::{
        Mutex,
        atomic::{AtomicBool, Ordering},
    },
    time::UNIX_EPOCH,
};

#[derive(Default)]
struct Namespace {
    next: u64,
    names: HashMap<(BackendInode, OsString), Entry>,
    contents: HashMap<u64, Vec<u8>>,
}
#[derive(Default)]
struct LostReplyBackend {
    state: Mutex<Namespace>,
    lose_reply: AtomicBool,
    fail_confirmation: AtomicBool,
    published: AtomicBool,
}
impl LostReplyBackend {
    fn insert(
        &self,
        parent: BackendInode,
        name: &OsStr,
        kind: FileKind,
        size: u64,
    ) -> Result<Entry> {
        let mut state = self.state.lock().unwrap();
        let key = (parent, name.to_owned());
        if state.names.contains_key(&key) {
            return Err(Error::coded(afs_error::IO_ALREADY_EXISTS, "exists"));
        }
        state.next += 1;
        let entry = Entry {
            inode: BackendInode {
                value: state.next + 1,
            },
            attributes: FileAttributes {
                kind,
                size,
                blocks: 0,
                mode: 0o750,
                uid: 0,
                gid: 0,
                nlink: 1,
                atime: UNIX_EPOCH,
                mtime: UNIX_EPOCH,
                ctime: UNIX_EPOCH,
            },
        };
        state.names.insert(key, entry.clone());
        Ok(entry)
    }
}
impl Backend for LostReplyBackend {
    fn root_inode(&self) -> BackendInode {
        BackendInode { value: 1 }
    }
    fn lookup(&self, _: &RequestContext, parent: BackendInode, name: &OsStr) -> Result<Entry> {
        if self.published.load(Ordering::SeqCst) && self.fail_confirmation.load(Ordering::SeqCst) {
            return Err(unavailable("confirmation unavailable"));
        }
        self.state
            .lock()
            .unwrap()
            .names
            .get(&(parent, name.to_owned()))
            .cloned()
            .ok_or_else(|| Error::coded(afs_error::IO_NOT_FOUND, "absent"))
    }
    fn mkdir(
        &self,
        _: &RequestContext,
        parent: BackendInode,
        name: &OsStr,
        _: u32,
    ) -> Result<Entry> {
        self.insert(parent, name, FileKind::Directory, 0)
    }
    fn prepare_lazy_file(
        &self,
        _: &RequestContext,
        parent: BackendInode,
        name: &OsStr,
        spec: &LazyFileSpec,
    ) -> Result<Entry> {
        self.insert(parent, name, FileKind::Regular, spec.length)
    }
    fn create(
        &self,
        _: &RequestContext,
        parent: BackendInode,
        name: &OsStr,
        _: u32,
        _: i32,
    ) -> Result<CreatedFile> {
        let entry = self.insert(parent, name, FileKind::Regular, 0)?;
        Ok(CreatedFile {
            handle: FileHandle(entry.inode.value),
            entry,
        })
    }
    fn write(
        &self,
        _: &RequestContext,
        handle: FileHandle,
        offset: u64,
        data: &[u8],
    ) -> Result<usize> {
        let mut state = self.state.lock().unwrap();
        let content = state.contents.entry(handle.0).or_default();
        let end = offset as usize + data.len();
        content.resize(end, 0);
        content[offset as usize..end].copy_from_slice(data);
        for entry in state
            .names
            .values_mut()
            .filter(|e| e.inode.value == handle.0)
        {
            entry.attributes.size = end as u64;
        }
        Ok(data.len())
    }
    fn fsync(&self, _: &RequestContext, _: FileHandle, _: SyncMode) -> Result<()> {
        Ok(())
    }
    fn release(&self, _: &RequestContext, _: FileHandle) -> Result<()> {
        Ok(())
    }
    fn open(&self, _: &RequestContext, inode: BackendInode, _: i32) -> Result<FileHandle> {
        Ok(FileHandle(inode.value))
    }
    fn read_owned(
        &self,
        _: &RequestContext,
        handle: FileHandle,
        offset: u64,
        size: usize,
    ) -> Result<Vec<u8>> {
        let state = self.state.lock().unwrap();
        Ok(state.contents[&handle.0][offset as usize..offset as usize + size].to_vec())
    }
    fn rename(
        &self,
        _: &RequestContext,
        from_parent: BackendInode,
        from_name: &OsStr,
        to_parent: BackendInode,
        to_name: &OsStr,
        _: RenameFlags,
    ) -> Result<()> {
        let mut state = self.state.lock().unwrap();
        let target = (to_parent, to_name.to_owned());
        if state.names.contains_key(&target) {
            return Err(Error::coded(afs_error::IO_ALREADY_EXISTS, "exists"));
        }
        let entry = state
            .names
            .remove(&(from_parent, from_name.to_owned()))
            .unwrap();
        state.names.insert(target, entry);
        self.published.store(true, Ordering::SeqCst);
        if self.lose_reply.load(Ordering::SeqCst) {
            Err(unavailable("rename committed, reply lost"))
        } else {
            Ok(())
        }
    }
    fn unlink(&self, _: &RequestContext, parent: BackendInode, name: &OsStr) -> Result<()> {
        self.state
            .lock()
            .unwrap()
            .names
            .remove(&(parent, name.to_owned()))
            .map(|_| ())
            .ok_or_else(|| Error::coded(afs_error::IO_NOT_FOUND, "absent"))
    }
    fn rmdir(&self, ctx: &RequestContext, parent: BackendInode, name: &OsStr) -> Result<()> {
        self.unlink(ctx, parent, name)
    }
}
fn fixture() -> (Arc<LostReplyBackend>, NydusService, ImagePlan) {
    let backend = Arc::new(LostReplyBackend::default());
    backend.lose_reply.store(true, Ordering::SeqCst);
    let config = NydusConfig::default();
    let provider = RegistryProvider::new(&config.sources, "test").unwrap();
    let service = NydusService::new(
        ConnectorContext {
            backend: backend.clone(),
            root: backend.root_inode(),
        },
        config,
        provider,
    );
    let plan = ImagePlan {
        source_key: "test".into(),
        registry: "https://example.com".into(),
        repository: "test".into(),
        manifest_digest: format!("sha256:{}", "a".repeat(64)),
        blobs: vec![registry::RegistrySource {
            source_key: "test".into(),
            registry: "https://example.com".into(),
            repository: "test".into(),
            blob_digest: format!("sha256:{}", "b".repeat(64)),
            size: 8,
        }],
    };
    (backend, service, plan)
}
#[test]
fn publication_lost_reply_keeps_published_tree_and_confirms_identity() {
    let (_, service, plan) = fixture();
    let result = service.publish_plan(plan.clone(), service.deadline());
    assert_eq!(
        service
            .read_plan(&plan.source_key, &plan.manifest_digest)
            .unwrap(),
        Some(plan.clone())
    );
    assert_eq!(result.unwrap(), plan);
}
#[test]
fn publication_unknown_result_never_removes_published_children() {
    let (backend, service, plan) = fixture();
    backend.fail_confirmation.store(true, Ordering::SeqCst);
    assert!(
        service
            .publish_plan(plan.clone(), service.deadline())
            .is_err()
    );
    backend.fail_confirmation.store(false, Ordering::SeqCst);
    assert_eq!(
        service
            .read_plan(&plan.source_key, &plan.manifest_digest)
            .unwrap(),
        Some(plan)
    );
}
