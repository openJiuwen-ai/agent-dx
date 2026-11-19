//! Fault boundary: rename moves the real namespace, then loses its reply.
use super::*;
use crate::node::vfs::types::{CreatedFile, FileAttributes, FileKind, RenameFlags, SyncMode};
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
    plan_bytes: Mutex<Vec<u8>>,
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
        let entry = self.insert(parent, name, FileKind::Regular, spec.length)?;
        if name == OsStr::new("descriptor.json") {
            assert_eq!(
                spec.binding.kind,
                lazy_fill::PLAN_KIND,
                "descriptors must survive the same copy loss as blobs"
            );
            self.state
                .lock()
                .unwrap()
                .contents
                .insert(entry.inode.value, self.plan_bytes.lock().unwrap().clone());
        }
        Ok(entry)
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
            #[cfg(feature = "dfs")]
            admin: None,
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
    *backend.plan_bytes.lock().unwrap() = serde_json::to_vec(&plan).unwrap();
    (backend, service, plan)
}

#[tokio::test]
async fn registry_proxy_ping_accepts_loopback_clients() {
    use axum::{
        body::Body,
        extract::ConnectInfo,
        http::{Request, StatusCode},
    };
    use std::net::SocketAddr;
    use tower::ServiceExt;

    let (_, service, _) = fixture();
    let reply = super::super::http::routes(Arc::new(service), None)
        .oneshot(
            Request::builder()
                .uri("/v2/")
                .extension(ConnectInfo("127.0.0.1:1234".parse::<SocketAddr>().unwrap()))
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(reply.status(), StatusCode::OK);
    assert_eq!(
        reply.headers()["docker-distribution-api-version"],
        "registry/2.0"
    );
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

#[derive(Default)]
struct HttpRangeSourceBackend {
    prepared: Mutex<Vec<std::ops::Range<u64>>>,
    reads: Mutex<Vec<(u64, usize)>>,
    released: AtomicBool,
}
impl Backend for HttpRangeSourceBackend {
    fn root_inode(&self) -> BackendInode {
        BackendInode { value: 1 }
    }
    fn open(&self, _: &RequestContext, _: BackendInode, _: i32) -> Result<FileHandle> {
        Ok(FileHandle(1))
    }
    fn set_read_source_range(
        &self,
        _: &RequestContext,
        _: FileHandle,
        range: std::ops::Range<u64>,
        _: Instant,
    ) -> Result<()> {
        assert!(
            self.reads.lock().unwrap().is_empty(),
            "install metadata range before any body read"
        );
        self.prepared.lock().unwrap().push(range);
        Ok(())
    }
    fn read_owned(
        &self,
        _: &RequestContext,
        _: FileHandle,
        offset: u64,
        size: usize,
    ) -> Result<Vec<u8>> {
        self.reads.lock().unwrap().push((offset, size));
        Ok(vec![42; size])
    }
    fn release(&self, _: &RequestContext, _: FileHandle) -> Result<()> {
        self.released.store(true, Ordering::SeqCst);
        Ok(())
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn http_range_sources_are_prepared_once_before_streaming_256k() {
    let (namespace, mut service, _) = fixture();
    let start = 123_u64;
    let length = (IO_BATCH + 17) as u64;
    let end = start + length;
    let entry = namespace
        .insert(
            BackendInode { value: 1 },
            OsStr::new("blob"),
            FileKind::Regular,
            end + 1024,
        )
        .unwrap();
    let backend = Arc::new(HttpRangeSourceBackend::default());
    service.backend = backend.clone();
    let service = Arc::new(service);
    let permit = service.admit(service.deadline()).await.unwrap();
    let mut receiver = service
        .read_blob(entry, start, length, service.deadline(), permit)
        .await
        .unwrap();
    let mut received = 0;
    while let Some(piece) = receiver.recv().await {
        let piece = piece.unwrap();
        assert!(
            piece.len() <= IO_BATCH,
            "metadata preparation cannot enlarge HTTP body pieces"
        );
        assert!(piece.iter().all(|byte| *byte == 42));
        received += piece.len();
    }
    assert_eq!(received as u64, length);
    assert!(backend.released.load(Ordering::SeqCst));
    assert_eq!(
        *backend.prepared.lock().unwrap(),
        vec![start..end],
        "install exactly the caller's full Range on its open handle before producing its first piece"
    );
    let reads = backend.reads.lock().unwrap();
    assert_eq!(
        *reads,
        vec![(start, IO_BATCH), (start + IO_BATCH as u64, 17)]
    );
}
