// DistributedFs unit tests. Shared fixtures live here; topic modules below
// see them (and every private item of `dfs`) through `use super::*`.

use super::*;

use crate::{
    dfs::{
        AuthorizeCurrentExternalRead, AuthorizeCurrentExternalReadReply,
        AuthorizeExternalVersionRead, AuthorizeExternalVersionReadReply, ChunkSources, CopyId,
        CopyLocation, CopyRole, CopyState, DfsAuthorizedRead, DfsChunkSourcesReply,
        DfsExternalAuthorizedRead, DfsMissingReadSource, DfsReadGrant, DfsReadValidation,
        SourceCandidate,
    },
    node::{
        chunk::LocalChunkStore,
        read::engine::{
            DfsReadConfig, ExternalChunkReader, ExternalReadAuthorizer, ReadSourceProvider,
            UnimplementedChunkTransfer, VerifiedExternalChunk,
        },
    },
};

use std::sync::atomic::AtomicUsize;

struct CommitPause {
    after_successes: usize,
    reached: (Mutex<bool>, std::sync::Condvar),
    release: (Mutex<bool>, std::sync::Condvar),
}

impl CommitPause {
    fn new(after_successes: usize) -> Self {
        Self {
            after_successes,
            reached: (Mutex::new(false), std::sync::Condvar::new()),
            release: (Mutex::new(false), std::sync::Condvar::new()),
        }
    }

    fn wait_until_reached(&self) {
        let (lock, cv) = &self.reached;
        let mut reached = lock.lock().unwrap();
        while !*reached {
            reached = cv.wait(reached).unwrap();
        }
    }

    fn release(&self) {
        let (lock, cv) = &self.release;
        *lock.lock().unwrap() = true;
        cv.notify_all();
    }

    fn was_reached(&self) -> bool {
        *self.reached.0.lock().unwrap()
    }

    fn block(&self) {
        let (reached_lock, reached_cv) = &self.reached;
        *reached_lock.lock().unwrap() = true;
        reached_cv.notify_all();

        let (release_lock, release_cv) = &self.release;
        let mut released = release_lock.lock().unwrap();
        while !*released {
            released = release_cv.wait(released).unwrap();
        }
    }
}

struct RenewPause {
    reached: (Mutex<bool>, std::sync::Condvar),
    release: (Mutex<bool>, std::sync::Condvar),
}

impl RenewPause {
    fn new() -> Self {
        Self {
            reached: (Mutex::new(false), std::sync::Condvar::new()),
            release: (Mutex::new(false), std::sync::Condvar::new()),
        }
    }

    fn block(&self) {
        let (reached_lock, reached_cv) = &self.reached;
        *reached_lock.lock().unwrap() = true;
        reached_cv.notify_all();
        let (release_lock, release_cv) = &self.release;
        let mut release = release_lock.lock().unwrap();
        while !*release {
            release = release_cv.wait(release).unwrap();
        }
    }

    fn release(&self) {
        let (lock, cv) = &self.release;
        *lock.lock().unwrap() = true;
        cv.notify_all();
    }

    fn was_reached(&self) -> bool {
        *self.reached.0.lock().unwrap()
    }
}

struct RecordingMeta {
    inode: Mutex<InodeRecord>,
    lease: Mutex<WriteLease>,
    commits: Mutex<Vec<CommitFileVersion>>,
    failed_commits: Mutex<Vec<CommitFileVersion>>,
    cas_failed_commits: Mutex<Vec<CommitFileVersion>>,
    metadata_syncs: Mutex<Vec<SyncInodeMetadata>>,
    failed_metadata_syncs: Mutex<Vec<SyncInodeMetadata>>,
    next_commit_error: Mutex<Option<Error>>,
    commit_error_after_successes: Mutex<Option<(usize, Error)>>,
    commit_pause: Mutex<Option<Arc<CommitPause>>>,
    renew_pause: Mutex<Option<Arc<RenewPause>>>,
    next_metadata_sync_error: Mutex<Option<Error>>,
    next_renew_error: Mutex<Option<Error>>,
    next_open_write_error: Mutex<Option<Error>>,
    next_get_inode_error: Mutex<Option<Error>>,
    next_get_inode_pause: Mutex<Option<Arc<NativeReadPause>>>,
    next_get_file_version_error: Mutex<Option<Error>>,
    file_version_replies: Mutex<VecDeque<(FileVersion, LayoutRoot)>>,
    next_get_inode_bad_id: Mutex<bool>,
    next_open_write_bad_namespace: Mutex<bool>,
    open_write_results: Mutex<std::collections::VecDeque<Result<(InodeRecord, WriteLease)>>>,
    renew_results: Mutex<std::collections::VecDeque<Result<WriteLease>>>,
    renew_timeouts: Mutex<Vec<Duration>>,
    metadata_timeouts: Mutex<Vec<Duration>>,
    commit_timeouts: Mutex<Vec<Duration>>,
    node_sessions: Mutex<HashMap<String, Option<String>>>,
    next_current_session_error: Mutex<Option<Error>>,
    current_session_calls: AtomicUsize,
    current_session_async_calls: AtomicUsize,
    resolve_write_async_calls: AtomicUsize,
    file_version_loads: AtomicUsize,
    renew_calls: AtomicUsize,
    open_write_calls: AtomicUsize,
    resolve_lock_calls: AtomicUsize,
    resolve_write_calls: AtomicUsize,
    get_inode_calls: AtomicUsize,
    xattr_calls: AtomicUsize,
    next_xattr_error: Mutex<Option<Error>>,
    /// Directory listing served by `read_dir_page`, keyed (and so ordered) by name.
    dir_entries: Mutex<std::collections::BTreeMap<Vec<u8>, DirEntryRecord>>,
    /// Largest page `read_dir_page` returns, standing in for Meta's cap.
    dir_page_cap: AtomicUsize,
    dir_page_calls: AtomicUsize,
    /// Errors `rename` returns, in order, before it succeeds.
    rename_errors: Mutex<VecDeque<Error>>,
    /// Operation ids `rename` was called with.
    rename_operations: Mutex<Vec<OperationId>>,
}

impl RecordingMeta {
    fn new() -> Self {
        Self {
            inode: Mutex::new(InodeRecord {
                namespace_id: NamespaceId::new("default"),
                inode_id: InodeId::new("inode:test"),
                kind: InodeKind::Regular,
                attributes: InodeAttributes {
                    mode: 0o640,
                    uid: 1000,
                    gid: 1000,
                    nlink: 1,
                    atime_unix_ms: 1,
                    mtime_unix_ms: 1,
                    ctime_unix_ms: 1,
                },
                head_version: None,
                symlink_target: None,
                xattrs: std::collections::BTreeMap::new(),
                revision: 1,
            }),
            lease: Mutex::new(WriteLease {
                inode_id: InodeId::new("inode:test"),
                owner_node_id: "node-a".into(),
                owner_session_id: "session-a".into(),
                lease_epoch: 1,
                expires_at_unix_ms: u64::MAX,
            }),
            commits: Mutex::new(Vec::new()),
            failed_commits: Mutex::new(Vec::new()),
            cas_failed_commits: Mutex::new(Vec::new()),
            metadata_syncs: Mutex::new(Vec::new()),
            failed_metadata_syncs: Mutex::new(Vec::new()),
            next_commit_error: Mutex::new(None),
            commit_error_after_successes: Mutex::new(None),
            commit_pause: Mutex::new(None),
            renew_pause: Mutex::new(None),
            next_metadata_sync_error: Mutex::new(None),
            next_renew_error: Mutex::new(None),
            next_open_write_error: Mutex::new(None),
            next_get_inode_error: Mutex::new(None),
            next_get_inode_pause: Mutex::new(None),
            next_get_file_version_error: Mutex::new(None),
            file_version_replies: Mutex::new(VecDeque::new()),
            next_get_inode_bad_id: Mutex::new(false),
            next_open_write_bad_namespace: Mutex::new(false),
            open_write_results: Mutex::new(std::collections::VecDeque::new()),
            renew_results: Mutex::new(std::collections::VecDeque::new()),
            renew_timeouts: Mutex::new(Vec::new()),
            metadata_timeouts: Mutex::new(Vec::new()),
            commit_timeouts: Mutex::new(Vec::new()),
            node_sessions: Mutex::new(HashMap::new()),
            next_current_session_error: Mutex::new(None),
            current_session_calls: AtomicUsize::new(0),
            current_session_async_calls: AtomicUsize::new(0),
            resolve_write_async_calls: AtomicUsize::new(0),
            dir_entries: Mutex::new(std::collections::BTreeMap::new()),
            dir_page_cap: AtomicUsize::new(1024),
            dir_page_calls: AtomicUsize::new(0),
            rename_errors: Mutex::new(VecDeque::new()),
            rename_operations: Mutex::new(Vec::new()),
            file_version_loads: AtomicUsize::new(0),
            renew_calls: AtomicUsize::new(0),
            open_write_calls: AtomicUsize::new(0),
            resolve_lock_calls: AtomicUsize::new(0),
            resolve_write_calls: AtomicUsize::new(0),
            get_inode_calls: AtomicUsize::new(0),
            xattr_calls: AtomicUsize::new(0),
            next_xattr_error: Mutex::new(None),
        }
    }

    fn commit_count(&self) -> usize {
        self.commits.lock().unwrap().len()
    }

    fn advance_namespace_revision(&self) {
        let mut inode = self.inode.lock().unwrap();
        inode.revision = inode.revision.saturating_add(1);
    }

    fn fail_next_commit(&self) {
        self.fail_next_commit_with(unavailable("injected test commit failure"));
    }

    fn fail_next_metadata_sync(&self) {
        self.fail_next_metadata_sync_with(unavailable("injected test metadata sync failure"));
    }

    fn fail_next_commit_with(&self, error: Error) {
        *self.next_commit_error.lock().unwrap() = Some(error);
    }

    fn pause_commit_after_successes(&self, successes: usize) -> Arc<CommitPause> {
        let pause = Arc::new(CommitPause::new(successes));
        *self.commit_pause.lock().unwrap() = Some(pause.clone());
        pause
    }

    fn pause_next_renew(&self) -> Arc<RenewPause> {
        let pause = Arc::new(RenewPause::new());
        *self.renew_pause.lock().unwrap() = Some(pause.clone());
        pause
    }

    fn fail_next_metadata_sync_with(&self, error: Error) {
        *self.next_metadata_sync_error.lock().unwrap() = Some(error);
    }

    fn fail_next_renew_with(&self, error: Error) {
        *self.next_renew_error.lock().unwrap() = Some(error);
    }

    fn fail_next_open_write_with(&self, error: Error) {
        *self.next_open_write_error.lock().unwrap() = Some(error);
    }

    fn fail_next_get_inode_with(&self, error: Error) {
        *self.next_get_inode_error.lock().unwrap() = Some(error);
    }

    fn fail_next_get_file_version_with(&self, error: Error) {
        *self.next_get_file_version_error.lock().unwrap() = Some(error);
    }

    fn file_version_load_count(&self) -> usize {
        self.file_version_loads.load(Ordering::SeqCst)
    }

    fn get_inode_call_count(&self) -> usize {
        self.get_inode_calls.load(Ordering::SeqCst)
    }

    fn return_bad_inode_on_next_get_inode(&self) {
        *self.next_get_inode_bad_id.lock().unwrap() = true;
    }

    fn return_bad_namespace_on_next_open_write(&self) {
        *self.next_open_write_bad_namespace.lock().unwrap() = true;
    }

    fn queue_renew_result(&self, result: Result<WriteLease>) {
        self.renew_results.lock().unwrap().push_back(result);
    }

    fn queue_open_write_result(&self, result: Result<(InodeRecord, WriteLease)>) {
        self.open_write_results.lock().unwrap().push_back(result);
    }

    fn lease_with_expiry(&self, expires_at_unix_ms: u64) -> WriteLease {
        let mut lease = self.lease.lock().unwrap().clone();
        lease.expires_at_unix_ms = expires_at_unix_ms;
        lease
    }

    fn observed_write_authority(&self) -> Result<(InodeRecord, WriteLease)> {
        if let Some(error) = self.next_open_write_error.lock().unwrap().take() {
            return Err(error);
        }
        let mut inode = self.inode.lock().unwrap().clone();
        if *self.next_open_write_bad_namespace.lock().unwrap() {
            *self.next_open_write_bad_namespace.lock().unwrap() = false;
            inode.namespace_id = NamespaceId::new("wrong");
        }
        Ok((inode, self.lease.lock().unwrap().clone()))
    }

    fn renew_call_count(&self) -> usize {
        self.renew_calls.load(Ordering::SeqCst)
    }

    fn commit_timeouts(&self) -> Vec<Duration> {
        self.commit_timeouts.lock().unwrap().clone()
    }

    fn metadata_timeouts(&self) -> Vec<Duration> {
        self.metadata_timeouts.lock().unwrap().clone()
    }

    fn renew_timeouts(&self) -> Vec<Duration> {
        self.renew_timeouts.lock().unwrap().clone()
    }

    fn set_node_session(&self, node_id: &str, session_id: Option<&str>) {
        self.node_sessions
            .lock()
            .unwrap()
            .insert(node_id.to_owned(), session_id.map(ToOwned::to_owned));
    }

    fn fail_next_current_session_with(&self, error: Error) {
        *self.next_current_session_error.lock().unwrap() = Some(error);
    }

    fn current_session_call_count(&self) -> usize {
        self.current_session_calls.load(Ordering::SeqCst)
    }

    fn publish_external_version(&self, length: u64) -> FileVersionId {
        self.publish_external_version_with_receipts(length, Vec::new(), Vec::new())
    }

    fn publish_external_version_with_receipts(
        &self,
        length: u64,
        inline_extents: Vec<Extent>,
        chunk_receipts: Vec<crate::dfs::ChunkReceipt>,
    ) -> FileVersionId {
        let mut inode = self.inode.lock().unwrap();
        let generation = self.commits.lock().unwrap().len() + 1;
        let layout_id = LayoutRootId::new(format!("external-layout-{generation}"));
        let version_id = FileVersionId::new(format!("external-version-{generation}"));
        let now = now_unix_ms();
        let version = FileVersion {
            id: version_id.clone(),
            inode_id: inode.inode_id.clone(),
            parent_version: inode.head_version.clone(),
            length,
            layout_root: layout_id.clone(),
            created_at_unix_ms: now,
        };
        let commit = CommitFileVersion {
            gc_fence: None,
            operation_id: OperationId::new(format!("external-commit-{generation}")),
            inode_id: inode.inode_id.clone(),
            write_lease: self.lease.lock().unwrap().clone(),
            expected_inode_revision: inode.revision,
            expected_head_version: inode.head_version.clone(),
            file_version: version,
            layout_root: LayoutRoot {
                id: layout_id,
                file_length: length,
                inline_extents,
            },
            chunk_receipts,
            clone_source: None,
            metadata_delta: CommitMetadataDelta {
                mode: CommitMetadataMode::Full,
                mtime_unix_ms: Some(now),
                ctime_unix_ms: Some(now),
                kill_suidgid: false,
            },
        };
        inode.revision = inode.revision.saturating_add(1);
        inode.head_version = Some(version_id.clone());
        inode.attributes.mtime_unix_ms = now;
        inode.attributes.ctime_unix_ms = now;
        drop(inode);
        self.commits.lock().unwrap().push(commit);
        version_id
    }
}

impl DfsMeta for RecordingMeta {
    fn lookup(&self, _: &InodeId, _: &[u8]) -> Result<Option<InodeRecord>> {
        Ok(Some(self.inode.lock().unwrap().clone()))
    }

    fn create(
        &self,
        _: &OperationId,
        _: &InodeId,
        _: &[u8],
        _: InodeAttributes,
    ) -> Result<(InodeRecord, WriteLease)> {
        Ok((
            self.inode.lock().unwrap().clone(),
            self.lease.lock().unwrap().clone(),
        ))
    }

    fn get_inode(&self, _: &InodeId) -> Result<InodeRecord> {
        self.get_inode_calls.fetch_add(1, Ordering::SeqCst);
        let error = self.next_get_inode_error.lock().unwrap().take();
        let pause = self.next_get_inode_pause.lock().unwrap().take();
        if let Some(pause) = pause {
            pause.block();
        }
        if let Some(error) = error {
            return Err(error);
        }
        let mut inode = self.inode.lock().unwrap().clone();
        if *self.next_get_inode_bad_id.lock().unwrap() {
            *self.next_get_inode_bad_id.lock().unwrap() = false;
            inode.inode_id = InodeId::new("inode:wrong");
        }
        Ok(inode)
    }

    fn get_file_version(&self, version_id: &FileVersionId) -> Result<(FileVersion, LayoutRoot)> {
        self.file_version_loads.fetch_add(1, Ordering::SeqCst);
        if let Some(error) = self.next_get_file_version_error.lock().unwrap().take() {
            return Err(error);
        }
        if let Some(reply) = self.file_version_replies.lock().unwrap().pop_front() {
            return Ok(reply);
        }
        self.commits
            .lock()
            .unwrap()
            .iter()
            .find(|commit| commit.file_version.id == *version_id)
            .map(|commit| (commit.file_version.clone(), commit.layout_root.clone()))
            .ok_or_else(|| invalid("test FileVersion not found"))
    }

    fn open_write(&self, _: &InodeId) -> Result<(InodeRecord, WriteLease)> {
        self.open_write_calls.fetch_add(1, Ordering::SeqCst);
        if let Some(result) = self.open_write_results.lock().unwrap().pop_front() {
            return result;
        }
        self.observed_write_authority()
    }

    fn resolve_lock_authority(&self, _: &InodeId) -> Result<(InodeRecord, WriteLease)> {
        self.resolve_lock_calls.fetch_add(1, Ordering::SeqCst);
        self.observed_write_authority()
    }

    fn resolve_write_authority(&self, _: &InodeId) -> Result<(InodeRecord, WriteLease)> {
        self.resolve_write_calls.fetch_add(1, Ordering::SeqCst);
        self.observed_write_authority()
    }

    fn renew_write_lease(&self, lease: WriteLease) -> Result<WriteLease> {
        if let Some(remaining) = crate::node::call_deadline::remaining() {
            self.renew_timeouts.lock().unwrap().push(remaining);
        }
        self.renew_calls.fetch_add(1, Ordering::SeqCst);
        if let Some(pause) = self.renew_pause.lock().unwrap().take() {
            pause.block();
        }
        if let Some(result) = self.renew_results.lock().unwrap().pop_front() {
            return result;
        }
        if let Some(error) = self.next_renew_error.lock().unwrap().take() {
            return Err(error);
        }
        let mut stored = self.lease.lock().unwrap();
        stored.expires_at_unix_ms = stored.expires_at_unix_ms.max(lease.expires_at_unix_ms);
        Ok(stored.clone())
    }

    fn sync_inode_metadata(&self, sync: SyncInodeMetadata) -> Result<InodeRecord> {
        if let Some(remaining) = crate::node::call_deadline::remaining() {
            self.metadata_timeouts.lock().unwrap().push(remaining);
        }
        let next_error = self.next_metadata_sync_error.lock().unwrap().take();
        if let Some(error) = next_error {
            self.failed_metadata_syncs.lock().unwrap().push(sync);
            return Err(error);
        }
        let mut inode = self.inode.lock().unwrap();
        if inode.revision != sync.expected_inode_revision
            || inode.head_version != sync.expected_head_version
        {
            return Err(unavailable("test inode metadata CAS failed"));
        }
        inode.revision = inode.revision.saturating_add(1);
        inode.attributes.mtime_unix_ms = sync.metadata_delta.mtime_unix_ms.unwrap();
        inode.attributes.ctime_unix_ms = sync.metadata_delta.ctime_unix_ms.unwrap();
        if sync.metadata_delta.kill_suidgid {
            inode.attributes.mode =
                DistributedFs::clear_kernel_write_privileges(inode.attributes.mode);
        }
        self.metadata_syncs.lock().unwrap().push(sync);
        Ok(inode.clone())
    }

    fn commit_file_version(&self, commit: CommitFileVersion) -> Result<InodeRecord> {
        if let Some(remaining) = crate::node::call_deadline::remaining() {
            self.commit_timeouts.lock().unwrap().push(remaining);
        }
        let next_error = self.next_commit_error.lock().unwrap().take();
        if let Some(error) = next_error {
            self.failed_commits.lock().unwrap().push(commit);
            return Err(error);
        }
        {
            let mut scheduled = self.commit_error_after_successes.lock().unwrap();
            let should_fail = scheduled
                .as_ref()
                .is_some_and(|(successes, _)| self.commits.lock().unwrap().len() >= *successes);
            if should_fail {
                let (_, error) = scheduled.take().unwrap();
                self.failed_commits.lock().unwrap().push(commit);
                return Err(error);
            }
        }
        let pause = self.commit_pause.lock().unwrap().clone();
        if let Some(pause) = pause
            && self.commits.lock().unwrap().len() >= pause.after_successes
        {
            *self.commit_pause.lock().unwrap() = None;
            pause.block();
        }
        let mut inode = self.inode.lock().unwrap();
        if inode.revision != commit.expected_inode_revision
            || inode.head_version != commit.expected_head_version
        {
            self.cas_failed_commits.lock().unwrap().push(commit);
            return Err(unavailable("test inode CAS failed"));
        }
        inode.revision = inode.revision.saturating_add(1);
        inode.head_version = Some(commit.file_version.id.clone());
        if commit.metadata_delta.mode == CommitMetadataMode::Full {
            inode.attributes.mtime_unix_ms = commit.metadata_delta.mtime_unix_ms.unwrap();
            inode.attributes.ctime_unix_ms = commit.metadata_delta.ctime_unix_ms.unwrap();
        }
        if commit.metadata_delta.kill_suidgid {
            inode.attributes.mode =
                DistributedFs::clear_kernel_write_privileges(inode.attributes.mode);
        }
        self.commits.lock().unwrap().push(commit);
        Ok(inode.clone())
    }

    fn mkdir(
        &self,
        _: &OperationId,
        _: &InodeId,
        _: &[u8],
        attributes: InodeAttributes,
        _: CallerContext,
    ) -> Result<InodeRecord> {
        let mut inode = self.inode.lock().unwrap().clone();
        inode.kind = InodeKind::Directory;
        inode.attributes = attributes;
        Ok(inode)
    }

    fn read_dir_page(&self, _: &InodeId, after: &[u8], limit: u32) -> Result<DirPage> {
        self.dir_page_calls.fetch_add(1, Ordering::SeqCst);
        let take = (limit as usize).min(self.dir_page_cap.load(Ordering::SeqCst));
        let entries = self.dir_entries.lock().unwrap();
        let mut after_cursor = entries
            .range::<[u8], _>((std::ops::Bound::Excluded(after), std::ops::Bound::Unbounded))
            .map(|(_, entry)| entry.clone());
        let page: Vec<_> = after_cursor.by_ref().take(take).collect();
        let eof = after_cursor.next().is_none();
        Ok(DirPage { entries: page, eof })
    }

    fn unlink(
        &self,
        _: &OperationId,
        _: &InodeId,
        _: &[u8],
        _: CallerContext,
    ) -> Result<InodeRecord> {
        Ok(self.inode.lock().unwrap().clone())
    }

    fn rmdir(
        &self,
        _: &OperationId,
        _: &InodeId,
        _: &[u8],
        _: CallerContext,
    ) -> Result<InodeRecord> {
        Ok(self.inode.lock().unwrap().clone())
    }

    fn rename(
        &self,
        operation_id: &OperationId,
        _: &InodeId,
        _: &[u8],
        _: &InodeId,
        _: &[u8],
        _: RenameMode,
        _: CallerContext,
    ) -> Result<RenameOutcome> {
        self.rename_operations
            .lock()
            .unwrap()
            .push(operation_id.clone());
        if let Some(error) = self.rename_errors.lock().unwrap().pop_front() {
            return Err(error);
        }
        Ok(RenameOutcome {
            inode: self.inode.lock().unwrap().clone(),
            replaced_inode: None,
        })
    }

    fn link(
        &self,
        _: &OperationId,
        _: &InodeId,
        expected_inode_revision: u64,
        _: &InodeId,
        _: &[u8],
        _: CallerContext,
    ) -> Result<InodeRecord> {
        let mut inode = self.inode.lock().unwrap();
        if inode.revision != expected_inode_revision {
            return Err(unavailable("test link CAS failed"));
        }
        inode.attributes.nlink = inode.attributes.nlink.saturating_add(1);
        inode.revision = inode.revision.saturating_add(1);
        Ok(inode.clone())
    }

    fn symlink(&self, _: SymlinkRequest) -> Result<InodeRecord> {
        Err(unavailable("test meta symlink is not implemented"))
    }

    fn mknod(&self, request: MknodRequest) -> Result<InodeRecord> {
        let mut inode = self.inode.lock().unwrap().clone();
        inode.kind = InodeKind::Special(request.kind);
        inode.attributes = request.attributes;
        inode.head_version = None;
        inode.revision = inode.revision.saturating_add(1);
        *self.inode.lock().unwrap() = inode.clone();
        Ok(inode)
    }

    fn read_link(&self, _: ReadLinkRequest) -> Result<Vec<u8>> {
        Err(unavailable("test meta readlink is not implemented"))
    }

    fn set_inode_attributes(&self, request: SetInodeAttrRequest) -> Result<InodeRecord> {
        let mut inode = self.inode.lock().unwrap();
        if inode.revision != request.expected_inode_revision {
            return Err(unavailable("test setattr CAS failed"));
        }
        if let Some(mode) = request.update.mode {
            inode.attributes.mode = mode;
        }
        if let Some(uid) = request.update.uid {
            inode.attributes.uid = uid;
        }
        if let Some(gid) = request.update.gid {
            inode.attributes.gid = gid;
        }
        if let Some(atime) = request.update.atime_unix_ms {
            inode.attributes.atime_unix_ms = atime;
        }
        if let Some(mtime) = request.update.mtime_unix_ms {
            inode.attributes.mtime_unix_ms = mtime;
        }
        if let Some(ctime) = request.update.ctime_unix_ms {
            inode.attributes.ctime_unix_ms = ctime;
        }
        inode.revision = inode.revision.saturating_add(1);
        Ok(inode.clone())
    }

    fn get_xattr(&self, request: GetXattrRequest) -> Result<Vec<u8>> {
        self.xattr_calls.fetch_add(1, Ordering::SeqCst);
        if let Some(error) = self.next_xattr_error.lock().unwrap().take() {
            return Err(error);
        }
        self.inode
            .lock()
            .unwrap()
            .xattrs
            .get(&request.name)
            .cloned()
            .ok_or_else(|| Error::coded(afs_error::NODE_VFS_NOT_FOUND, "test xattr not found"))
    }

    fn list_xattr(&self, _: ListXattrRequest) -> Result<Vec<Vec<u8>>> {
        Ok(self.inode.lock().unwrap().xattrs.keys().cloned().collect())
    }

    fn set_xattr(&self, request: SetXattrRequest) -> Result<InodeRecord> {
        let mut inode = self.inode.lock().unwrap();
        if inode.revision != request.expected_inode_revision {
            return Err(unavailable("test setxattr CAS failed"));
        }
        let exists = inode.xattrs.contains_key(&request.name);
        match request.mode {
            XattrSetMode::Create if exists => return Err(invalid("test xattr exists")),
            XattrSetMode::Replace if !exists => return Err(invalid("test xattr missing")),
            _ => {}
        }
        inode.xattrs.insert(request.name, request.value);
        inode.revision = inode.revision.saturating_add(1);
        Ok(inode.clone())
    }

    fn remove_xattr(&self, request: RemoveXattrRequest) -> Result<InodeRecord> {
        let mut inode = self.inode.lock().unwrap();
        if inode.revision != request.expected_inode_revision {
            return Err(unavailable("test removexattr CAS failed"));
        }
        inode.xattrs.remove(&request.name);
        inode.revision = inode.revision.saturating_add(1);
        Ok(inode.clone())
    }

    fn lookup_node_location(&self, node_id: &str) -> Result<Option<DfsNodeLocation>> {
        Ok(Some(DfsNodeLocation {
            node_id: node_id.into(),
            node_epoch: 1,
            data_endpoint: "http://127.0.0.1:9".into(),
        }))
    }

    fn current_node_session(&self, node_id: &str) -> Result<Option<String>> {
        self.current_session_calls.fetch_add(1, Ordering::SeqCst);
        if let Some(error) = self.next_current_session_error.lock().unwrap().take() {
            return Err(error);
        }
        if let Some(session) = self.node_sessions.lock().unwrap().get(node_id).cloned() {
            return Ok(session);
        }
        let suffix = node_id.strip_prefix("node-").unwrap_or("a");
        Ok(Some(format!("session-{suffix}")))
    }

    fn current_node_session_async(
        &self,
        node_id: String,
    ) -> crate::node::read::admission::ReadFuture<'_, Option<String>> {
        self.current_session_async_calls
            .fetch_add(1, Ordering::SeqCst);
        Box::pin(async move { self.current_node_session(&node_id) })
    }

    fn resolve_write_authority_async(
        &self,
        inode_id: InodeId,
    ) -> crate::node::read::admission::ReadFuture<'_, (InodeRecord, WriteLease)> {
        self.resolve_write_async_calls
            .fetch_add(1, Ordering::SeqCst);
        Box::pin(async move { self.resolve_write_authority(&inode_id) })
    }
}

fn context() -> RequestContext {
    RequestContext {
        uid: 1000,
        gid: 1000,
        pid: 42,
        umask: 0,
        supplementary_gids: Vec::new(),
    }
}

fn test_fs() -> (tempfile::TempDir, Arc<RecordingMeta>, DistributedFs) {
    let meta = Arc::new(RecordingMeta::new());
    let temp = tempfile::tempdir().unwrap();
    let chunks = Arc::new(LocalChunkStore::open(temp.path(), "node-a").unwrap());
    let store = Arc::new(LocalRequiredChunkStore(chunks.clone()));
    let fs = distributed_fs_with_chunk_store(meta.clone(), chunks, store);
    (temp, meta, fs)
}

/// 生产默认（单副本、`dfs_local_copy = "required"`）下 `DfsChunkSink` 给 native 上传的回执标
/// `BodyRelease::ReleaseAfterUpload`，上传完成即释放内存正文；测试的本地仓同样标记，
/// 否则测的是"非本地副本、正文保留到发布"那一支。
struct LocalRequiredChunkStore(Arc<LocalChunkStore>);

impl ChunkSink for LocalRequiredChunkStore {
    fn put_chunks(&self, staged: Vec<StagedChunk>, intent: PutIntent) -> Result<Vec<StoredChunk>> {
        let mut stored = self.0.put_chunks(staged, intent)?;
        for stored in &mut stored {
            stored.body_release = if intent == PutIntent::PipelineUpload {
                BodyRelease::ReleaseAfterUpload
            } else {
                BodyRelease::RetainUntilPublish
            };
        }
        Ok(stored)
    }
    fn read_uploaded_at(
        &self,
        receipt: &ChunkReceipt,
        offset: u64,
        out: &mut [u8],
    ) -> Result<usize> {
        self.0
            .open_receipt_reader(&receipt.chunk)?
            .read_at(offset, out)
    }
    fn capacity(&self) -> Result<FilesystemCapacity> {
        ChunkSink::capacity(self.0.as_ref())
    }
}

fn distributed_fs_with_chunk_store(
    meta: Arc<RecordingMeta>,
    local: Arc<LocalChunkStore>,
    chunk_store: Arc<dyn ChunkSink>,
) -> DistributedFs {
    let read_engine = Arc::new(crate::node::read::engine::DfsReadEngine::new(
        NamespaceId::new("default"),
        "node-a".into(),
        local,
        Arc::new(crate::node::read::engine::UnimplementedReadSourceProvider),
        Arc::new(crate::node::read::engine::UnimplementedChunkTransfer),
        crate::node::read::engine::DfsReadConfig::default(),
    ));
    test_dfs_new(
        NamespaceId::new("default"),
        "node-a",
        "session-a",
        meta,
        chunk_store,
        read_engine,
    )
}

/// 测试共享的 Node runtime：生产里 native 上传跑在 Node 的 tokio runtime 上。
fn test_node_runtime() -> tokio::runtime::Handle {
    static RUNTIME: std::sync::OnceLock<tokio::runtime::Runtime> = std::sync::OnceLock::new();
    RUNTIME
        .get_or_init(|| {
            tokio::runtime::Builder::new_multi_thread()
                .worker_threads(2)
                .enable_all()
                .build()
                .unwrap()
        })
        .handle()
        .clone()
}

/// 与生产装配一致（`node.rs`）：DFS 存储一律装带 runtime 的 native 写流水线，配置取生产默认值。
fn test_dfs_new(
    namespace_id: NamespaceId,
    node_id: impl Into<String>,
    session_id: impl Into<String>,
    meta: Arc<dyn DfsMeta>,
    chunk_store: Arc<dyn ChunkSink>,
    read_engine: Arc<DfsReadEngine>,
) -> DistributedFs {
    DistributedFs::new(
        namespace_id,
        node_id,
        session_id,
        meta,
        chunk_store,
        read_engine,
    )
    .with_write_pipeline(
        DirtyBudget::new(PRODUCTION_NATIVE_BUDGET_BYTES as usize),
        WritePipelineConfig {
            unpublished_descriptors: PRODUCTION_UNPUBLISHED_DESCRIPTORS,
            upload_inflight_chunks: PRODUCTION_UPLOAD_INFLIGHT_CHUNKS,
        },
        test_node_runtime(),
        None,
    )
    .unwrap()
}

/// 生产 `dfs_dirty_budget_bytes`、`dfs_unpublished_descriptors`、`dfs_upload_inflight_chunks` 的默认值。
const PRODUCTION_NATIVE_BUDGET_BYTES: u64 = 64 * 1024 * 1024;

const PRODUCTION_UNPUBLISHED_DESCRIPTORS: usize = 1024;

const PRODUCTION_UPLOAD_INFLIGHT_CHUNKS: usize = 8;

fn publish_external_version_with_bytes(
    meta: &RecordingMeta,
    fs: &DistributedFs,
    operation_id: &str,
    bytes: &[u8],
) -> FileVersionId {
    let receipt = fs
        .chunk_store
        .put(StagedChunk::new_generational(
            OperationId::new(operation_id),
            bytes.to_vec(),
        ))
        .unwrap();
    meta.publish_external_version_with_receipts(
        bytes.len() as u64,
        vec![Extent {
            file_offset: 0,
            length: bytes.len() as u64,
            chunk_id: receipt.chunk.id.clone(),
            chunk_offset: 0,
        }],
        vec![receipt],
    )
}

#[derive(Clone, Copy)]
enum CurrentExternalFixtureMode {
    Authorized,
    Denied,
}

struct CurrentExternalFixture {
    meta: Arc<RecordingMeta>,
    chunks: Mutex<HashMap<crate::dfs::ChunkId, (ChunkObject, Vec<u8>)>>,
    mode: Mutex<CurrentExternalFixtureMode>,
    current_calls: AtomicUsize,
    legacy_calls: AtomicUsize,
    source_calls: AtomicUsize,
    fetch_calls: AtomicUsize,
}

impl CurrentExternalFixture {
    fn new(meta: Arc<RecordingMeta>) -> Self {
        Self {
            meta,
            chunks: Mutex::new(HashMap::new()),
            mode: Mutex::new(CurrentExternalFixtureMode::Authorized),
            current_calls: AtomicUsize::new(0),
            legacy_calls: AtomicUsize::new(0),
            source_calls: AtomicUsize::new(0),
            fetch_calls: AtomicUsize::new(0),
        }
    }

    fn set_mode(&self, mode: CurrentExternalFixtureMode) {
        *self.mode.lock().unwrap() = mode;
    }

    fn current_call_count(&self) -> usize {
        self.current_calls.load(Ordering::SeqCst)
    }

    fn legacy_call_count(&self) -> usize {
        self.legacy_calls.load(Ordering::SeqCst)
    }

    fn insert_chunk(&self, chunk: ChunkObject, bytes: Vec<u8>) {
        self.chunks
            .lock()
            .unwrap()
            .insert(chunk.id.clone(), (chunk, bytes));
    }

    fn authorization_for(&self, validation: DfsReadValidation) -> DfsExternalAuthorizedRead {
        let (chunk, _) = self
            .chunks
            .lock()
            .unwrap()
            .get(&validation.chunk_id)
            .cloned()
            .expect("test chunk should be known to external fixture");
        let object_key = crate::dfs::external_object_key("test-store", "test-revision", &chunk.id)
            .expect("fixture must return the committed external chunk key");
        DfsExternalAuthorizedRead {
            authorized: DfsAuthorizedRead {
                validation,
                allowed_ranges: vec![(0, chunk.length)],
                expires_at_unix_ms: u64::MAX,
            },
            chunk,
            location: CopyLocation::External {
                store_id: "test-store".into(),
                object_key,
                object_revision: "test-revision".into(),
            },
        }
    }

    fn authorization_for_request(
        &self,
        request: DfsMissingReadSource,
        file_version_id: &FileVersionId,
        layout_root_id: &LayoutRootId,
    ) -> DfsExternalAuthorizedRead {
        let (chunk, _) = self
            .chunks
            .lock()
            .unwrap()
            .get(&request.chunk_id)
            .cloned()
            .expect("test chunk should be known to external fixture");
        self.authorization_for(DfsReadValidation {
            grant: DfsReadGrant {
                namespace_id: NamespaceId::new("default"),
                file_version_id: file_version_id.clone(),
                layout_root_id: layout_root_id.clone(),
                caller_node_id: "node-a".into(),
                caller_node_epoch: 1,
                expires_at_unix_ms: u64::MAX,
                fence: 1,
                token: "test-token".into(),
            },
            chunk_id: chunk.id.clone(),
            copy_id: CopyId::new(format!("external-copy-{}", chunk.id.0)),
            chunk_offset: request.chunk_offset,
            length: request.length,
        })
    }
}

impl ReadSourceProvider for CurrentExternalFixture {
    fn sources_for(
        &self,
        request: crate::dfs::DfsChunkSourcesRequest,
    ) -> Result<DfsChunkSourcesReply> {
        self.source_calls.fetch_add(1, Ordering::SeqCst);
        let chunks = self.chunks.lock().unwrap();
        Ok(DfsChunkSourcesReply {
            revision: 1,
            chunks: request
                .chunk_ids
                .into_iter()
                .filter_map(|chunk_id| {
                    chunks.get(&chunk_id).map(|(chunk, _)| ChunkSources {
                        chunk_id: chunk_id.clone(),
                        chunk: Some(chunk.clone()),
                        allowed_ranges: Vec::new(),
                        sources: vec![SourceCandidate {
                            copy_id: CopyId::new(format!("external-copy-{}", chunk_id.0)),
                            chunk_id: chunk_id.clone(),
                            role: CopyRole::ExternalCommitted,
                            state: CopyState::Ready,
                            location: CopyLocation::External {
                                store_id: "test-store".into(),
                                object_key: crate::dfs::external_object_key(
                                    "test-store",
                                    "test-revision",
                                    &chunk.id,
                                )
                                .expect("fixture must return the committed external chunk key"),
                                object_revision: "test-revision".into(),
                            },
                            data_endpoint: None,
                            load_hint: 0,
                            read_grant: DfsReadGrant {
                                namespace_id: request.namespace_id.clone(),
                                file_version_id: request.file_version_id.clone(),
                                layout_root_id: request.layout_root_id.clone(),
                                caller_node_id: request.caller_id.clone(),
                                caller_node_epoch: 1,
                                expires_at_unix_ms: u64::MAX,
                                fence: 1,
                                token: "test-token".into(),
                            },
                        }],
                    })
                })
                .collect(),
        })
    }
}

impl ExternalReadAuthorizer for CurrentExternalFixture {
    fn authorize_external_version_read(
        &self,
        request: AuthorizeExternalVersionRead,
    ) -> Result<Option<AuthorizeExternalVersionReadReply>> {
        self.current_calls.fetch_add(1, Ordering::SeqCst);
        assert_eq!(request.caller_id, "node-a");
        assert_eq!(request.namespace_id, NamespaceId::new("default"));
        assert!(!request.requests.is_empty());
        match *self.mode.lock().unwrap() {
            CurrentExternalFixtureMode::Authorized => Ok(Some(AuthorizeExternalVersionReadReply {
                authorizations: request
                    .requests
                    .into_iter()
                    .map(|entry| {
                        self.authorization_for_request(
                            entry,
                            &request.file_version_id,
                            &request.layout_root_id,
                        )
                    })
                    .collect(),
                remaining_validity_ms: 5_000,
            })),
            CurrentExternalFixtureMode::Denied => Err(Error::coded(
                afs_error::IO_PERMISSION_DENIED,
                "test external version authorization denied",
            )),
        }
    }

    fn authorize_current_external_read(
        &self,
        request: AuthorizeCurrentExternalRead,
    ) -> Result<Option<AuthorizeCurrentExternalReadReply>> {
        self.current_calls.fetch_add(1, Ordering::SeqCst);
        assert_eq!(request.caller_id, "node-a");
        assert_eq!(request.namespace_id, NamespaceId::new("default"));
        let inode = self.meta.inode.lock().unwrap().clone();
        match *self.mode.lock().unwrap() {
            CurrentExternalFixtureMode::Authorized => {
                assert_eq!(
                    inode.head_version.as_ref(),
                    Some(&request.expected_head_version)
                );
                Ok(Some(AuthorizeCurrentExternalReadReply::Authorized {
                    inode,
                    authorizations: request
                        .validations
                        .into_iter()
                        .map(|validation| self.authorization_for(validation))
                        .collect(),
                }))
            }
            CurrentExternalFixtureMode::Denied => Err(Error::coded(
                afs_error::IO_PERMISSION_DENIED,
                "test current external authorization denied",
            )),
        }
    }

    fn validate_external_read_grants(
        &self,
        caller_id: &str,
        validations: Vec<DfsReadValidation>,
    ) -> Result<Vec<DfsExternalAuthorizedRead>> {
        assert_eq!(caller_id, "node-a");
        self.legacy_calls.fetch_add(1, Ordering::SeqCst);
        Ok(validations
            .into_iter()
            .map(|validation| self.authorization_for(validation))
            .collect())
    }
}

impl ExternalChunkReader for CurrentExternalFixture {
    fn read_chunk(
        &self,
        chunk: &ChunkObject,
        source: &SourceCandidate,
    ) -> Result<VerifiedExternalChunk> {
        assert!(matches!(source.location, CopyLocation::External { .. }));
        self.fetch_calls.fetch_add(1, Ordering::SeqCst);
        let (_, bytes) = self
            .chunks
            .lock()
            .unwrap()
            .get(&chunk.id)
            .cloned()
            .ok_or_else(|| unavailable("test external chunk not found"))?;
        VerifiedExternalChunk::verify(chunk, bytes)
    }
}

/// S3 stand-in for the native pipeline (S1a-5): objects land in the external
/// read fixture, receipts are external commits and never local-durable, like
/// `S3ChunkStore`.
struct FakeS3Store {
    fixture: Arc<CurrentExternalFixture>,
    puts: AtomicUsize,
    fail_next: AtomicBool,
}

impl FakeS3Store {
    fn objects(&self) -> usize {
        self.fixture.chunks.lock().unwrap().len()
    }
}

impl ChunkSink for FakeS3Store {
    fn put_chunks(&self, staged: Vec<StagedChunk>, _: PutIntent) -> Result<Vec<StoredChunk>> {
        if self.fail_next.swap(false, Ordering::SeqCst) {
            return Err(unavailable("injected S3 PUT failure"));
        }
        Ok(staged
            .into_iter()
            .map(|chunk| {
                self.puts.fetch_add(1, Ordering::SeqCst);
                self.fixture
                    .insert_chunk(chunk.chunk.clone(), chunk.bytes().to_vec());
                StoredChunk::unreleased(ChunkReceipt {
                    operation_id: chunk.operation_id.clone(),
                    external_commit: Some(crate::dfs::ExternalCommitReceipt {
                        store_id: "fake-s3".into(),
                        generation: "g1".into(),
                        object_key: format!("fake-s3/g1/{}", chunk.chunk.id.0),
                        persisted_bytes: chunk.chunk.length,
                        verified_digest: chunk.chunk.content_digest.clone(),
                    }),
                    chunk: chunk.chunk,
                    placement_revision: 0,
                    placement_epoch: 0,
                    replica_group_id: crate::dfs::ReplicaGroupId::new(""),
                    durable_acks: Vec::new(),
                })
            })
            .collect())
    }
}

fn test_fs_on_fake_s3() -> (
    tempfile::TempDir,
    Arc<RecordingMeta>,
    DistributedFs,
    Arc<FakeS3Store>,
) {
    let meta = Arc::new(RecordingMeta::new());
    let temp = tempfile::tempdir().unwrap();
    let chunks = Arc::new(LocalChunkStore::open(temp.path(), "node-a").unwrap());
    let fixture = Arc::new(CurrentExternalFixture::new(meta.clone()));
    let read_engine = Arc::new(
        DfsReadEngine::new(
            NamespaceId::new("default"),
            "node-a".into(),
            chunks,
            fixture.clone(),
            Arc::new(UnimplementedChunkTransfer),
            DfsReadConfig::default(),
        )
        .with_external_backend(fixture.clone(), fixture.clone()),
    );
    let store = Arc::new(FakeS3Store {
        fixture,
        puts: AtomicUsize::new(0),
        fail_next: AtomicBool::new(false),
    });
    let fs = test_dfs_new(
        NamespaceId::new("default"),
        "node-a",
        "session-a",
        meta.clone(),
        store.clone(),
        read_engine,
    );
    assert!(
        fs.write_resources.pipeline.is_some(),
        "S3 writes through the native pipeline"
    );
    (temp, meta, fs, store)
}

#[cfg(feature = "dfs")]
struct CountingChunkStore {
    inner: Arc<dyn ChunkSink>,
    put_batches: AtomicUsize,
    capacity_calls: AtomicUsize,
    fail_empty_prepare: AtomicBool,
}

#[cfg(feature = "dfs")]
impl CountingChunkStore {
    fn new(inner: Arc<dyn ChunkSink>) -> Self {
        Self {
            inner,
            put_batches: AtomicUsize::new(0),
            capacity_calls: AtomicUsize::new(0),
            fail_empty_prepare: AtomicBool::new(false),
        }
    }

    fn put_batch_count(&self) -> usize {
        self.put_batches.load(Ordering::SeqCst)
    }

    fn capacity_call_count(&self) -> usize {
        self.capacity_calls.load(Ordering::SeqCst)
    }
}

#[cfg(feature = "dfs")]
impl ChunkSink for CountingChunkStore {
    fn put_chunks(&self, staged: Vec<StagedChunk>, _: PutIntent) -> Result<Vec<StoredChunk>> {
        self.put_batches.fetch_add(1, Ordering::SeqCst);
        if staged.is_empty() && self.fail_empty_prepare.swap(false, Ordering::SeqCst) {
            return Err(unavailable(
                "injected PR93 preparation failure after admission",
            ));
        }
        self.inner.put_chunks(staged, PutIntent::Commit)
    }

    fn capacity(&self) -> Result<FilesystemCapacity> {
        self.capacity_calls.fetch_add(1, Ordering::SeqCst);
        self.inner.capacity()
    }
}

fn expire_native_read_view(fs: &DistributedFs, handle: FileHandle) {
    let opened = fs.handles.get(handle.0).unwrap().unwrap();
    let mut state = opened.state.lock().unwrap();
    if let Some(view) = state.native_read_view.as_mut() {
        view.deadline = Instant::now();
    }
}

#[derive(Default)]
struct NativeReadPause {
    state: Mutex<(bool, bool)>,
    changed: Condvar,
}

impl NativeReadPause {
    fn block(&self) {
        let mut state = self.state.lock().unwrap();
        state.0 = true;
        self.changed.notify_all();
        let (state, timeout) = self
            .changed
            .wait_timeout_while(state, Duration::from_secs(5), |state| !state.1)
            .unwrap();
        assert!(
            state.1 && !timeout.timed_out(),
            "native read pause timed out"
        );
    }

    fn wait_until_reached(&self) {
        let state = self.state.lock().unwrap();
        let (state, timeout) = self
            .changed
            .wait_timeout_while(state, Duration::from_secs(5), |state| !state.0)
            .unwrap();
        assert!(
            state.0 && !timeout.timed_out(),
            "native read did not reach pause"
        );
    }

    fn resume(&self) {
        self.state.lock().unwrap().1 = true;
        self.changed.notify_all();
    }
}

fn p2_byte_cache_version(name: &str, extent_count: usize) -> Arc<ReadOnlyVersion> {
    let id = FileVersionId::new(name);
    let layout_id = LayoutRootId::new(format!("{name}-layout"));
    let length = extent_count as u64 * 2;
    let chunk_id = "c".repeat(512);
    let layout = LayoutRoot {
        id: layout_id.clone(),
        file_length: length,
        inline_extents: (0..extent_count)
            .map(|index| Extent {
                file_offset: index as u64 * 2,
                length: 1,
                chunk_id: crate::dfs::ChunkId::new(chunk_id.clone()),
                chunk_offset: 0,
            })
            .collect(),
    };
    let version = FileVersion {
        id: id.clone(),
        inode_id: InodeId::new("byte-cache-file"),
        parent_version: None,
        length,
        layout_root: layout_id,
        created_at_unix_ms: 0,
    };
    Arc::new(ReadOnlyVersion::checked(&id, version, layout).unwrap())
}

fn lock_owner(session: &str, kernel_owner: u64) -> FileLockOwner {
    FileLockOwner {
        ingress_session_id: session.into(),
        kernel_owner,
    }
}

fn lock_range(start: u64, end: u64) -> FileLockRange {
    FileLockRange { start, end }
}

struct NoopRemoteDfsOwner;

macro_rules! noop_remote_owner_method {
    ($method:ident, $request:ty, $reply:ty) => {
        fn $method(&self, _: $request) -> afs_error::Result<$reply> {
            Err(unavailable("noop remote owner should not be called"))
        }
    };
}

impl RemoteDfsOwner for NoopRemoteDfsOwner {
    noop_remote_owner_method!(
        open,
        afs_protocol::node_control::DfsOwnerOpenRequest,
        afs_protocol::node_control::DfsOwnerOpenReply
    );
    noop_remote_owner_method!(
        getattr,
        afs_protocol::node_control::DfsOwnerGetAttrRequest,
        afs_protocol::node_control::DfsOwnerGetAttrReply
    );
    noop_remote_owner_method!(
        release,
        afs_protocol::node_control::DfsOwnerReleaseRequest,
        afs_protocol::node_control::DfsOwnerReleaseReply
    );
    noop_remote_owner_method!(
        get_lock,
        afs_protocol::node_control::DfsOwnerGetLockRequest,
        afs_protocol::node_control::DfsOwnerGetLockReply
    );
    noop_remote_owner_method!(
        set_lock,
        afs_protocol::node_control::DfsOwnerSetLockRequest,
        afs_protocol::node_control::DfsOwnerSetLockReply
    );
    noop_remote_owner_method!(
        cancel_lock_wait,
        afs_protocol::node_control::DfsOwnerCancelLockWaitRequest,
        afs_protocol::node_control::DfsOwnerCancelLockWaitReply
    );
    noop_remote_owner_method!(
        acknowledge_lock_wait,
        afs_protocol::node_control::DfsOwnerAcknowledgeLockWaitRequest,
        afs_protocol::node_control::DfsOwnerAcknowledgeLockWaitReply
    );
    noop_remote_owner_method!(
        release_locks,
        afs_protocol::node_control::DfsOwnerReleaseLocksRequest,
        afs_protocol::node_control::DfsOwnerReleaseLocksReply
    );
    noop_remote_owner_method!(
        release_lock_session,
        afs_protocol::node_control::DfsOwnerReleaseLockSessionRequest,
        afs_protocol::node_control::DfsOwnerReleaseLockSessionReply
    );
    noop_remote_owner_method!(
        read,
        afs_protocol::node_data::DfsOwnerReadRequest,
        afs_protocol::node_data::DfsOwnerReadReply
    );
    noop_remote_owner_method!(
        write,
        afs_protocol::node_data::DfsOwnerWriteRequest,
        afs_protocol::node_data::DfsOwnerWriteReply
    );
    noop_remote_owner_method!(
        resize,
        afs_protocol::node_data::DfsOwnerResizeRequest,
        afs_protocol::node_data::DfsOwnerResizeReply
    );
    noop_remote_owner_method!(
        sync,
        afs_protocol::node_data::DfsOwnerSyncRequest,
        afs_protocol::node_data::DfsOwnerSyncReply
    );
}

struct StaticRemoteOwnerFactory {
    owner: Arc<dyn RemoteDfsOwner>,
}

impl DfsRemoteOwnerFactory for StaticRemoteOwnerFactory {
    fn connect(&self, _: DfsNodeLocation) -> Result<Arc<dyn RemoteDfsOwner>> {
        Ok(self.owner.clone())
    }
}

struct LoopbackRemoteDfsOwner {
    owner: Arc<DistributedFs>,
    peer: String,
}

impl RemoteDfsOwner for LoopbackRemoteDfsOwner {
    fn open(
        &self,
        request: afs_protocol::node_control::DfsOwnerOpenRequest,
    ) -> Result<afs_protocol::node_control::DfsOwnerOpenReply> {
        <DistributedFs as crate::node::vfs::dfs::owner_api::DfsOwnerLifecycleHandler>::open(
            self.owner.as_ref(),
            &self.peer,
            request,
        )
    }

    fn getattr(
        &self,
        request: afs_protocol::node_control::DfsOwnerGetAttrRequest,
    ) -> Result<afs_protocol::node_control::DfsOwnerGetAttrReply> {
        <DistributedFs as crate::node::vfs::dfs::owner_api::DfsOwnerLifecycleHandler>::getattr(
            self.owner.as_ref(),
            &self.peer,
            request,
        )
    }

    fn release(
        &self,
        request: afs_protocol::node_control::DfsOwnerReleaseRequest,
    ) -> Result<afs_protocol::node_control::DfsOwnerReleaseReply> {
        <DistributedFs as crate::node::vfs::dfs::owner_api::DfsOwnerLifecycleHandler>::release(
            self.owner.as_ref(),
            &self.peer,
            request,
        )
    }

    fn get_lock(
        &self,
        request: afs_protocol::node_control::DfsOwnerGetLockRequest,
    ) -> Result<afs_protocol::node_control::DfsOwnerGetLockReply> {
        <DistributedFs as crate::node::vfs::dfs::owner_api::DfsOwnerLifecycleHandler>::get_lock(
            self.owner.as_ref(),
            &self.peer,
            request,
        )
    }

    fn set_lock(
        &self,
        request: afs_protocol::node_control::DfsOwnerSetLockRequest,
    ) -> Result<afs_protocol::node_control::DfsOwnerSetLockReply> {
        <DistributedFs as crate::node::vfs::dfs::owner_api::DfsOwnerLifecycleHandler>::set_lock(
            self.owner.as_ref(),
            &self.peer,
            request,
        )
    }

    fn cancel_lock_wait(
        &self,
        request: afs_protocol::node_control::DfsOwnerCancelLockWaitRequest,
    ) -> Result<afs_protocol::node_control::DfsOwnerCancelLockWaitReply> {
        <DistributedFs as crate::node::vfs::dfs::owner_api::DfsOwnerLifecycleHandler>::cancel_lock_wait(
            self.owner.as_ref(),
            &self.peer,
            request,
        )
    }

    fn acknowledge_lock_wait(
        &self,
        request: afs_protocol::node_control::DfsOwnerAcknowledgeLockWaitRequest,
    ) -> Result<afs_protocol::node_control::DfsOwnerAcknowledgeLockWaitReply> {
        <DistributedFs as crate::node::vfs::dfs::owner_api::DfsOwnerLifecycleHandler>::acknowledge_lock_wait(
                self.owner.as_ref(),
                &self.peer,
                request,
            )
    }

    fn release_locks(
        &self,
        request: afs_protocol::node_control::DfsOwnerReleaseLocksRequest,
    ) -> Result<afs_protocol::node_control::DfsOwnerReleaseLocksReply> {
        <DistributedFs as crate::node::vfs::dfs::owner_api::DfsOwnerLifecycleHandler>::release_locks(
            self.owner.as_ref(),
            &self.peer,
            request,
        )
    }

    fn release_lock_session(
        &self,
        request: afs_protocol::node_control::DfsOwnerReleaseLockSessionRequest,
    ) -> Result<afs_protocol::node_control::DfsOwnerReleaseLockSessionReply> {
        <DistributedFs as crate::node::vfs::dfs::owner_api::DfsOwnerLifecycleHandler>::release_lock_session(
            self.owner.as_ref(),
            &self.peer,
            request,
        )
    }

    fn read(
        &self,
        request: afs_protocol::node_data::DfsOwnerReadRequest,
    ) -> Result<afs_protocol::node_data::DfsOwnerReadReply> {
        <DistributedFs as crate::node::vfs::dfs::owner_api::DfsOwnerFilesHandler>::read(
            self.owner.as_ref(),
            &self.peer,
            request,
        )
    }

    fn write(
        &self,
        request: afs_protocol::node_data::DfsOwnerWriteRequest,
    ) -> Result<afs_protocol::node_data::DfsOwnerWriteReply> {
        <DistributedFs as crate::node::vfs::dfs::owner_api::DfsOwnerFilesHandler>::write(
            self.owner.as_ref(),
            &self.peer,
            request,
        )
    }

    fn resize(
        &self,
        request: afs_protocol::node_data::DfsOwnerResizeRequest,
    ) -> Result<afs_protocol::node_data::DfsOwnerResizeReply> {
        <DistributedFs as crate::node::vfs::dfs::owner_api::DfsOwnerFilesHandler>::resize(
            self.owner.as_ref(),
            &self.peer,
            request,
        )
    }

    fn sync(
        &self,
        request: afs_protocol::node_data::DfsOwnerSyncRequest,
    ) -> Result<afs_protocol::node_data::DfsOwnerSyncReply> {
        <DistributedFs as crate::node::vfs::dfs::owner_api::DfsOwnerFilesHandler>::sync(
            self.owner.as_ref(),
            &self.peer,
            request,
        )
    }
}

enum ScriptedReleaseAction {
    DelegateThenErr(Error),
    DelegateThenPause(Arc<OwnerOpenFinishPause>),
    ErrWithoutDelegate(Error),
    OkWithoutDelegate,
}

struct ScriptedReleaseRemoteDfsOwner {
    inner: Arc<dyn RemoteDfsOwner>,
    releases: Mutex<std::collections::VecDeque<ScriptedReleaseAction>>,
    calls: Mutex<Vec<afs_protocol::node_control::DfsOwnerHandle>>,
    read_pause: Mutex<Option<Arc<OwnerOpenFinishPause>>>,
}

impl ScriptedReleaseRemoteDfsOwner {
    fn new(inner: Arc<dyn RemoteDfsOwner>, actions: Vec<ScriptedReleaseAction>) -> Self {
        Self {
            inner,
            releases: Mutex::new(actions.into_iter().collect()),
            calls: Mutex::new(Vec::new()),
            read_pause: Mutex::new(None),
        }
    }

    fn release_call_count(&self) -> usize {
        self.calls.lock().unwrap().len()
    }

    fn release_opaque_handles(&self) -> Vec<Vec<u8>> {
        self.calls
            .lock()
            .unwrap()
            .iter()
            .map(|handle| handle.opaque_handle.clone())
            .collect()
    }

    fn set_read_pause(&self, pause: Arc<OwnerOpenFinishPause>) {
        *self.read_pause.lock().unwrap() = Some(pause);
    }
}

impl RemoteDfsOwner for ScriptedReleaseRemoteDfsOwner {
    fn open(
        &self,
        request: afs_protocol::node_control::DfsOwnerOpenRequest,
    ) -> Result<afs_protocol::node_control::DfsOwnerOpenReply> {
        self.inner.open(request)
    }

    fn getattr(
        &self,
        request: afs_protocol::node_control::DfsOwnerGetAttrRequest,
    ) -> Result<afs_protocol::node_control::DfsOwnerGetAttrReply> {
        self.inner.getattr(request)
    }

    fn release(
        &self,
        request: afs_protocol::node_control::DfsOwnerReleaseRequest,
    ) -> Result<afs_protocol::node_control::DfsOwnerReleaseReply> {
        if let Some(handle) = request.handle.as_ref() {
            self.calls.lock().unwrap().push(handle.clone());
        }
        match self.releases.lock().unwrap().pop_front() {
            None => self.inner.release(request),
            Some(ScriptedReleaseAction::DelegateThenErr(error)) => {
                self.inner.release(request)?;
                Err(error)
            }
            Some(ScriptedReleaseAction::DelegateThenPause(pause)) => {
                let reply = self.inner.release(request)?;
                pause.pause();
                Ok(reply)
            }
            Some(ScriptedReleaseAction::ErrWithoutDelegate(error)) => Err(error),
            Some(ScriptedReleaseAction::OkWithoutDelegate) => {
                Ok(afs_protocol::node_control::DfsOwnerReleaseReply {})
            }
        }
    }

    fn get_lock(
        &self,
        request: afs_protocol::node_control::DfsOwnerGetLockRequest,
    ) -> Result<afs_protocol::node_control::DfsOwnerGetLockReply> {
        self.inner.get_lock(request)
    }

    fn set_lock(
        &self,
        request: afs_protocol::node_control::DfsOwnerSetLockRequest,
    ) -> Result<afs_protocol::node_control::DfsOwnerSetLockReply> {
        self.inner.set_lock(request)
    }

    fn cancel_lock_wait(
        &self,
        request: afs_protocol::node_control::DfsOwnerCancelLockWaitRequest,
    ) -> Result<afs_protocol::node_control::DfsOwnerCancelLockWaitReply> {
        self.inner.cancel_lock_wait(request)
    }

    fn acknowledge_lock_wait(
        &self,
        request: afs_protocol::node_control::DfsOwnerAcknowledgeLockWaitRequest,
    ) -> Result<afs_protocol::node_control::DfsOwnerAcknowledgeLockWaitReply> {
        self.inner.acknowledge_lock_wait(request)
    }

    fn release_locks(
        &self,
        request: afs_protocol::node_control::DfsOwnerReleaseLocksRequest,
    ) -> Result<afs_protocol::node_control::DfsOwnerReleaseLocksReply> {
        self.inner.release_locks(request)
    }

    fn release_lock_session(
        &self,
        request: afs_protocol::node_control::DfsOwnerReleaseLockSessionRequest,
    ) -> Result<afs_protocol::node_control::DfsOwnerReleaseLockSessionReply> {
        self.inner.release_lock_session(request)
    }

    fn read(
        &self,
        request: afs_protocol::node_data::DfsOwnerReadRequest,
    ) -> Result<afs_protocol::node_data::DfsOwnerReadReply> {
        let reply = self.inner.read(request)?;
        if let Some(pause) = self.read_pause.lock().unwrap().take() {
            pause.pause();
        }
        Ok(reply)
    }

    fn write(
        &self,
        request: afs_protocol::node_data::DfsOwnerWriteRequest,
    ) -> Result<afs_protocol::node_data::DfsOwnerWriteReply> {
        self.inner.write(request)
    }

    fn resize(
        &self,
        request: afs_protocol::node_data::DfsOwnerResizeRequest,
    ) -> Result<afs_protocol::node_data::DfsOwnerResizeReply> {
        self.inner.resize(request)
    }

    fn sync(
        &self,
        request: afs_protocol::node_data::DfsOwnerSyncRequest,
    ) -> Result<afs_protocol::node_data::DfsOwnerSyncReply> {
        self.inner.sync(request)
    }
}

mod commit;
mod directories;
mod dirty_index;
mod error_delivery;
mod grpc_meta;
mod locks;
mod misc;
mod owner_gateway;
mod read_views;
mod remote_provider;
mod s3;
mod unified;
mod version_cache;
mod write_pipeline_ops;
mod write_semantics;
mod write_state;
