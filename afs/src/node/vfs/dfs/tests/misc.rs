use super::*;

#[test]
fn meta_capability_local_rejection_skips_authority() {
    let (_temp, meta, fs) = test_fs();
    let inode_id = meta.inode.lock().unwrap().inode_id.clone();
    let inode = fs.backend_inode(&inode_id).unwrap();
    for uid in [0, 1000, 2000] {
        let mut caller = context();
        caller.uid = uid;
        let error = fs
            .getxattr(&caller, inode, OsStr::new("security.capability"))
            .unwrap_err();
        assert_eq!(error.code(), afs_error::IO_NOT_SUPPORTED);
        assert_eq!(error.message(), "DFS currently supports only user.* xattrs");
    }
    assert_eq!(meta.xattr_calls.load(Ordering::SeqCst), 0);
}

#[test]
fn meta_capability_other_names_keep_authority_and_errors() {
    let (_temp, meta, fs) = test_fs();
    let inode_id = {
        let mut inode = meta.inode.lock().unwrap();
        inode
            .xattrs
            .insert(b"user.meta-capability".to_vec(), b"value".to_vec());
        inode.inode_id.clone()
    };
    let inode = fs.backend_inode(&inode_id).unwrap();
    assert_eq!(
        fs.getxattr(&context(), inode, OsStr::new("user.meta-capability"))
            .unwrap(),
        b"value"
    );
    *meta.next_xattr_error.lock().unwrap() = Some(Error::coded(
        afs_error::IO_PERMISSION_DENIED,
        "authority denied",
    ));
    let error = fs
        .getxattr(&context(), inode, OsStr::new("user.meta-capability"))
        .unwrap_err();
    assert_eq!(error.code(), afs_error::IO_PERMISSION_DENIED);
    assert_eq!(error.message(), "authority denied");
    for name in [
        "security.selinux",
        "security.capability.extra",
        "user.missing",
    ] {
        assert_eq!(
            fs.getxattr(&context(), inode, OsStr::new(name))
                .unwrap_err()
                .code(),
            afs_error::NODE_VFS_NOT_FOUND
        );
    }
    assert_eq!(meta.xattr_calls.load(Ordering::SeqCst), 5);
}

#[test]
fn meta_capability_invalid_inode_keeps_local_failure_order() {
    let (_temp, meta, fs) = test_fs();
    let inode = BackendInode { value: u64::MAX };
    let expected = fs.inode_id(inode).unwrap_err();
    let actual = fs
        .getxattr(&context(), inode, OsStr::new("security.capability"))
        .unwrap_err();
    assert_eq!(actual.code(), expected.code());
    assert_eq!(actual.message(), expected.message());
    assert_eq!(meta.xattr_calls.load(Ordering::SeqCst), 0);
}

#[derive(Default)]
struct Pr88GcControl {
    captures: AtomicUsize,
}

impl crate::node::gc::GcControl for Pr88GcControl {
    fn retain(&self, _: &str, _: &InodeId, _: Option<&FileVersionId>) -> Result<()> {
        Ok(())
    }
    fn retain_fenced(
        &self,
        _: &str,
        _: &InodeId,
        _: Option<&FileVersionId>,
    ) -> Result<crate::dfs::GcFence> {
        self.captures.fetch_add(1, Ordering::SeqCst);
        Ok(crate::dfs::GcFence::WriteGeneration)
    }
    fn release(&self, _: &[String]) -> Result<()> {
        Ok(())
    }
    fn collect(&self, _: u64, _: Vec<ChunkObject>) -> Result<crate::dfs::GcState> {
        Ok(crate::dfs::GcState::default())
    }
}

#[test]
fn pr88_fix_crc32c_meta_resize_after_fullsync_keeps_fence() {
    let (_temp, meta, fs) = test_fs();
    let control = Arc::new(Pr88GcControl::default());
    let references = crate::node::gc::GcReferences::new(control.clone());
    let fs = fs.with_gc_references(Some(references.clone()));
    fs.hooks
        .create_admits_no_generation
        .store(true, Ordering::SeqCst);
    let file = fs
        .create(
            &context(),
            fs.root_inode(),
            OsStr::new("pr88-resize"),
            0o640,
            libc::O_RDWR,
        )
        .unwrap();
    let mut expected = vec![11; 65549];
    fs.write(&context(), file.handle, 0, &expected).unwrap();
    fs.fsync(&context(), file.handle, SyncMode::Full).unwrap();
    for length in [32775, 70001] {
        fs.setattr(
            &context(),
            file.entry.inode,
            Some(file.handle),
            &AttributeChange {
                size: Some(length),
                ..AttributeChange::default()
            },
        )
        .unwrap();
        fs.fsync(&context(), file.handle, SyncMode::Full).unwrap();
        expected.resize(length as usize, 0);
        let mut got = vec![0; 80000];
        assert_eq!(
            fs.read(&context(), file.handle, 0, &mut got).unwrap(),
            expected.len()
        );
        assert_eq!(&got[..expected.len()], expected.as_slice());
    }
    let commits = meta.commits.lock().unwrap();
    assert_eq!(commits.len(), 3);
    assert!(
        commits
            .iter()
            .all(|c| c.gc_fence == Some(crate::dfs::GcFence::WriteGeneration))
    );
    assert_eq!(commits.last().unwrap().file_version.length, 70001);
    drop(commits);
    assert_eq!(control.captures.load(Ordering::SeqCst), 3);
    fs.release(&context(), file.handle).unwrap();
    references.flush().unwrap();
}

#[test]
fn pr88_fix_crc32c_tail_partial_success_retry_keeps_ids_and_receipts() {
    struct LostSecondReceipt {
        inner: Arc<LocalChunkStore>,
        attempts: Mutex<Vec<ChunkObject>>,
    }
    impl ChunkSink for LostSecondReceipt {
        fn put_chunks(
            &self,
            staged: Vec<StagedChunk>,
            intent: PutIntent,
        ) -> Result<Vec<StoredChunk>> {
            assert_eq!(staged.len(), 1);
            let call = {
                let mut attempts = self.attempts.lock().unwrap();
                attempts.push(staged[0].chunk.clone());
                attempts.len()
            };
            let mut stored = self.inner.put_chunks(staged, PutIntent::Commit)?;
            if call == 2 {
                return Err(unavailable("injected lost second durable receipt"));
            }
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
            self.inner
                .open_receipt_reader(&receipt.chunk)?
                .read_at(offset, out)
        }
    }
    let meta = Arc::new(RecordingMeta::new());
    let temp = tempfile::tempdir().unwrap();
    let local = Arc::new(LocalChunkStore::open(temp.path(), "node-a").unwrap());
    let store = Arc::new(LostSecondReceipt {
        inner: local.clone(),
        attempts: Mutex::new(Vec::new()),
    });
    let control = Arc::new(Pr88GcControl::default());
    let references = crate::node::gc::GcReferences::new(control.clone());
    let fs = distributed_fs_with_chunk_store(meta.clone(), local.clone(), store.clone())
        .with_gc_references(Some(references.clone()));
    fs.hooks
        .create_admits_no_generation
        .store(true, Ordering::SeqCst);
    let file = fs
        .create(
            &context(),
            fs.root_inode(),
            OsStr::new("pr88-tail-retry"),
            0o640,
            libc::O_RDWR,
        )
        .unwrap();
    let offset = 2 * COMMIT_CHUNK_BYTES;
    fs.write(&context(), file.handle, 0, &[17; 65549]).unwrap();
    fs.write(&context(), file.handle, offset, &[29; 65549])
        .unwrap();
    assert!(fs.fsync(&context(), file.handle, SyncMode::Full).is_err());
    assert_eq!(meta.commit_count(), 0);
    assert_eq!(
        local.gc_inventory().unwrap().len(),
        2,
        "both tails reached real durable storage"
    );
    let inode_id = meta.inode.lock().unwrap().inode_id.clone();
    let cell = fs.write_state(&inode_id).unwrap().unwrap();
    {
        let state = cell.lock().unwrap();
        let upload = state.dirty_extents.extents[&0]
            .native_upload
            .as_ref()
            .unwrap();
        assert!(
            upload.state.lock().unwrap().body.is_none(),
            "successful local R1 tail retires its memory body"
        );
    }
    let mut first_tail = vec![0; 65549];
    assert_eq!(
        fs.read(&context(), file.handle, 0, &mut first_tail)
            .unwrap(),
        first_tail.len()
    );
    assert_eq!(
        first_tail,
        vec![17; 65549],
        "failed preparation retains an anchored durable read"
    );
    fs.fsync(&context(), file.handle, SyncMode::Full).unwrap();
    let attempts = store.attempts.lock().unwrap();
    assert_eq!(
        attempts.len(),
        3,
        "successful first tail must not be submitted again"
    );
    assert_eq!(
        attempts[2], attempts[1],
        "lost receipt retry must keep the same CRC object"
    );
    assert_ne!(attempts[0].id, attempts[1].id);
    assert!(attempts.iter().all(ChunkObject::is_segment_crc32c));
    let original = [attempts[0].id.clone(), attempts[1].id.clone()];
    drop(attempts);
    assert_eq!(
        local.gc_inventory().unwrap().len(),
        2,
        "retry must not create duplicate physical objects"
    );
    let commits = meta.commits.lock().unwrap();
    assert_eq!(commits.len(), 1);
    assert_eq!(
        commits[0].gc_fence,
        Some(crate::dfs::GcFence::WriteGeneration)
    );
    assert_eq!(commits[0].chunk_receipts.len(), 2);
    assert!(
        commits[0]
            .chunk_receipts
            .iter()
            .all(|r| original.contains(&r.chunk.id))
    );
    drop(commits);
    let mut expected = vec![0; offset as usize + 65549];
    expected[..65549].fill(17);
    expected[offset as usize..].fill(29);
    let mut got = vec![0; expected.len()];
    for (index, out) in got.chunks_mut(1024 * 1024).enumerate() {
        assert_eq!(
            fs.read(&context(), file.handle, (index * 1024 * 1024) as u64, out)
                .unwrap(),
            out.len()
        );
    }
    assert_eq!(got, expected);
    assert_eq!(
        control.captures.load(Ordering::SeqCst),
        1,
        "retry must not refresh the frozen GC fence"
    );
    fs.release(&context(), file.handle).unwrap();
    references.flush().unwrap();
}

#[test]
fn crc32c_vfs_partial_overwrite_and_truncate() {
    let (_temp, meta, fs) = test_fs();
    let file = fs
        .create(
            &context(),
            fs.root_inode(),
            OsStr::new("crc-range.bin"),
            0o640,
            libc::O_RDWR,
        )
        .unwrap();
    let mut expected = vec![11; 65549];
    fs.write(&context(), file.handle, 0, &expected).unwrap();
    fs.fsync(&context(), file.handle, SyncMode::Full).unwrap();
    fs.write(&context(), file.handle, 32760, &[29; 30]).unwrap();
    expected[32760..32790].fill(29);
    fs.fsync(&context(), file.handle, SyncMode::Full).unwrap();
    let mut got = vec![0; expected.len()];
    assert_eq!(
        fs.read(&context(), file.handle, 0, &mut got).unwrap(),
        expected.len()
    );
    assert_eq!(got, expected);
    fs.setattr(
        &context(),
        file.entry.inode,
        Some(file.handle),
        &AttributeChange {
            size: Some(32775),
            ..AttributeChange::default()
        },
    )
    .unwrap();
    fs.fsync(&context(), file.handle, SyncMode::Full).unwrap();
    expected.truncate(32775);
    let mut got = vec![0; 65549];
    assert_eq!(
        fs.read(&context(), file.handle, 0, &mut got).unwrap(),
        expected.len()
    );
    assert_eq!(&got[..expected.len()], &expected);
    assert!(
        meta.commits
            .lock()
            .unwrap()
            .iter()
            .flat_map(|c| &c.chunk_receipts)
            .all(|r| r.chunk.is_segment_crc32c())
    );
    fs.release(&context(), file.handle).unwrap();
}

#[cfg(feature = "dfs")]
struct UnsupportedCapacityChunkStore {
    inner: Arc<dyn ChunkSink>,
}

#[cfg(feature = "dfs")]
impl ChunkSink for UnsupportedCapacityChunkStore {
    fn put_chunks(&self, staged: Vec<StagedChunk>, _: PutIntent) -> Result<Vec<StoredChunk>> {
        self.inner.put_chunks(staged, PutIntent::Commit)
    }
}

/// S1a-4 characterization: every native commit trigger reaches Meta with a
/// fixed metadata mode. Native commits carry the write generation's operation
/// id, so the trigger itself is not visible to Meta; the refactor must keep
/// these modes.
#[test]
fn s1a_commit_triggers_keep_their_metadata_mode() {
    type Trigger = fn(&DistributedFs, FileHandle);
    let cases: [(&str, Trigger, CommitMetadataMode); 6] = [
        (
            "fsync",
            |fs, handle| fs.fsync(&context(), handle, SyncMode::Full).unwrap(),
            CommitMetadataMode::Full,
        ),
        (
            "fdatasync",
            |fs, handle| fs.fsync(&context(), handle, SyncMode::DataOnly).unwrap(),
            CommitMetadataMode::DataOnly,
        ),
        (
            "flush",
            |fs, handle| fs.flush(&context(), handle).unwrap(),
            CommitMetadataMode::DataOnly,
        ),
        (
            "drain",
            |fs, _| {
                fs.drain().unwrap();
            },
            CommitMetadataMode::Full,
        ),
        (
            "background",
            |fs, _| {
                fs.writeback_pending().unwrap();
            },
            CommitMetadataMode::Full,
        ),
        (
            "last writer",
            |fs, handle| {
                fs.release(&context(), handle).unwrap();
                fs.writeback_pending().unwrap();
            },
            CommitMetadataMode::Full,
        ),
    ];
    for (name, trigger, mode) in cases {
        let (_temp, meta, fs) = test_fs();
        let created = fs
            .create(
                &context(),
                fs.root_inode(),
                OsStr::new("trigger.bin"),
                0o640,
                libc::O_RDWR,
            )
            .unwrap();
        fs.write(&context(), created.handle, 0, &[7; 4096]).unwrap();
        trigger(&fs, created.handle);
        let commits = meta.commits.lock().unwrap();
        let last = commits
            .last()
            .unwrap_or_else(|| panic!("{name}: no commit reached Meta"));
        assert_eq!(last.metadata_delta.mode, mode, "{name}");
        assert_eq!(last.file_version.length, 4096, "{name}");
    }
}

/// S1a-2 structural cost: only lock acquisitions that wait are counted and timed.
#[test]
fn s1a_inode_lock_contention_is_counted_only_when_a_lock_waits() {
    let registry = afs_metrics::Registry::new();
    let (_temp, meta, fs) = test_fs();
    let fs = fs.with_inode_lock_metrics(&registry).unwrap();
    fs.create(
        &context(),
        fs.root_inode(),
        OsStr::new("lock-wait.bin"),
        0o640,
        libc::O_RDWR,
    )
    .unwrap();
    let inode_id = meta.inode.lock().unwrap().inode_id.clone();
    let cell = fs.write_state(&inode_id).unwrap().unwrap();
    let wait = fs.writes.lock_wait.clone().unwrap();
    let before = wait.contended.get();
    drop(cell.lock().unwrap());
    assert_eq!(
        wait.contended.get(),
        before,
        "uncontended lock is not counted"
    );

    let guard = cell.lock().unwrap();
    let waiter_cell = cell.clone();
    let (ready_tx, ready_rx) = std::sync::mpsc::channel();
    let waiter = std::thread::spawn(move || {
        ready_tx.send(()).unwrap();
        drop(waiter_cell.lock().unwrap());
    });
    ready_rx.recv().unwrap();
    std::thread::sleep(Duration::from_millis(50));
    drop(guard);
    waiter.join().unwrap();
    assert_eq!(wait.contended.get(), before + 1);
    assert!(wait.wait_nanoseconds.get() >= 10_000_000);
}

/// R56：生产模式（带 runtime 的 native 流水线）下，一次后台上传失败对同一句柄只报告一次，
/// 经 inode 的后台错误游标交付。之后重试成功；之后打开的句柄不受影响。
#[test]
fn native_upload_failure_is_reported_once_to_the_same_handle() {
    struct FailOnceStore {
        inner: Arc<LocalChunkStore>,
        fail: AtomicBool,
    }
    impl ChunkSink for FailOnceStore {
        fn put_chunks(&self, staged: Vec<StagedChunk>, _: PutIntent) -> Result<Vec<StoredChunk>> {
            if self.fail.swap(false, Ordering::SeqCst) {
                return Err(unavailable("injected upload failure"));
            }
            self.inner.put_chunks(staged, PutIntent::Commit)
        }
    }
    let meta = Arc::new(RecordingMeta::new());
    let temp = tempfile::tempdir().unwrap();
    let local = Arc::new(LocalChunkStore::open(temp.path(), "node-a").unwrap());
    let store = Arc::new(FailOnceStore {
        inner: local.clone(),
        fail: AtomicBool::new(true),
    });
    let fs = distributed_fs_with_chunk_store(meta.clone(), local, store);
    let created = fs
        .create(
            &context(),
            fs.root_inode(),
            OsStr::new("twice.bin"),
            0o640,
            libc::O_RDWR,
        )
        .unwrap();
    let data = vec![5; COMMIT_CHUNK_BYTES as usize];
    let reports = usize::from(fs.write(&context(), created.handle, 0, &data).is_err())
        + (0..2)
            .filter(|_| {
                fs.fsync(&context(), created.handle, SyncMode::Full)
                    .is_err()
            })
            .count();
    assert_eq!(
        reports, 1,
        "one upload failure, reported once to one handle"
    );
    assert_eq!(meta.commit_count(), 1);
    let other = fs
        .open(&context(), created.entry.inode, libc::O_RDWR)
        .unwrap();
    fs.fsync(&context(), other, SyncMode::Full).unwrap();
}

#[test]
fn crc32c_native_preupload_retry_preserves_immutable_id() {
    struct FailOnceStore {
        inner: Arc<LocalChunkStore>,
        fail: AtomicBool,
        attempts: Mutex<Vec<Vec<ChunkObject>>>,
    }
    impl ChunkSink for FailOnceStore {
        fn put_chunks(&self, staged: Vec<StagedChunk>, _: PutIntent) -> Result<Vec<StoredChunk>> {
            self.attempts
                .lock()
                .unwrap()
                .push(staged.iter().map(|chunk| chunk.chunk.clone()).collect());
            if self.fail.swap(false, Ordering::SeqCst) {
                return Err(unavailable("injected prepare put failure"));
            }
            self.inner.put_chunks(staged, PutIntent::Commit)
        }
    }
    let meta = Arc::new(RecordingMeta::new());
    let temp = tempfile::tempdir().unwrap();
    let local = Arc::new(LocalChunkStore::open(temp.path(), "node-a").unwrap());
    let store = Arc::new(FailOnceStore {
        inner: local.clone(),
        fail: AtomicBool::new(true),
        attempts: Mutex::new(Vec::new()),
    });
    let pool = WriteBufferPool::new(0);
    let fs = distributed_fs_with_chunk_store(meta.clone(), local, store.clone())
        .with_write_buffer_pool(pool.clone());
    let created = fs
        .create(
            &context(),
            fs.root_inode(),
            OsStr::new("native-restore-tail.bin"),
            0o640,
            libc::O_RDWR,
        )
        .unwrap();
    let half = COMMIT_CHUNK_BYTES as usize / 2;
    let mut original = vec![5; COMMIT_CHUNK_BYTES as usize];
    original[half..].fill(9);
    // 生产模式（带 runtime）下满块在写入期间就开始后台上传，注入的失败可能先由 write 观察到，
    // 也可能由 fsync 报告；两处都是合法的错误出口。要求的是失败一定被报告且不产生提交。
    if fs.write(&context(), created.handle, 0, &original).is_ok() {
        assert!(
            fs.fsync(&context(), created.handle, SyncMode::Full)
                .is_err()
        );
    }
    assert_eq!(meta.commit_count(), 0);
    let inode_id = meta.inode.lock().unwrap().inode_id.clone();
    let cell = fs.write_state(&inode_id).unwrap().unwrap();
    assert!(cell.lock().unwrap().dirty_extents.builders.is_empty());
    for length in [half as u64, COMMIT_CHUNK_BYTES] {
        fs.setattr(
            &context(),
            created.entry.inode,
            Some(created.handle),
            &AttributeChange {
                size: Some(length),
                ..AttributeChange::default()
            },
        )
        .unwrap();
    }
    fs.write(&context(), created.handle, 8192, &[7; 4096])
        .unwrap();
    fs.fsync(&context(), created.handle, SyncMode::Full)
        .unwrap();
    let attempts = store.attempts.lock().unwrap();
    assert_eq!(attempts.len(), 3);
    assert_eq!(attempts[0][0].length, COMMIT_CHUNK_BYTES);
    // The failed object is retried by the next barrier; the new tail may start first.
    let retry = attempts[1..]
        .iter()
        .find(|attempt| attempt[0].length == COMMIT_CHUNK_BYTES)
        .unwrap();
    let tail = attempts[1..]
        .iter()
        .find(|attempt| attempt[0].length == 4096)
        .unwrap();
    assert_eq!(
        retry, &attempts[0],
        "retry keeps the sealed immutable object; it does not rewrite its hidden tail"
    );
    assert_eq!(tail.len(), 1);
    assert!(
        attempts
            .iter()
            .flatten()
            .all(ChunkObject::is_segment_crc32c)
    );
    for (chunk, body) in [
        (&retry[0], original.as_slice()),
        (&tail[0], [7; 4096].as_slice()),
    ] {
        assert_eq!(
            chunk.content_digest,
            crate::node::chunk_crc32c::CrcTable::build(body)
                .unwrap()
                .descriptor_digest()
        );
    }
    let original_id = attempts[0][0].id.clone();
    drop(attempts);
    let commits = meta.commits.lock().unwrap();
    let commit = commits.last().unwrap();
    assert_eq!(commit.chunk_receipts.len(), 2);
    for extent in &commit.layout_root.inline_extents {
        if extent.chunk_id == original_id {
            assert!(
                extent.chunk_offset + extent.length <= half as u64,
                "layout-authorized ranges never include the original hidden tail"
            );
        }
    }
    drop(commits);
    let mut expected = original;
    expected[half..].fill(0);
    expected[8192..12288].fill(7);
    let mut actual = vec![0; expected.len()];
    assert_eq!(
        fs.read(&context(), created.handle, 0, &mut actual).unwrap(),
        expected.len()
    );
    assert_eq!(actual, expected);
    assert_eq!(pool.snapshot().copied_bytes, COMMIT_CHUNK_BYTES + 4096);
}

#[cfg(feature = "dfs")]
#[test]
fn statfs_validates_meta_inode_and_reads_chunk_store_capacity_without_writer() {
    let meta = Arc::new(RecordingMeta::new());
    let temp = tempfile::tempdir().unwrap();
    let local = Arc::new(LocalChunkStore::open(temp.path(), "node-a").unwrap());
    local
        .put(StagedChunk::new(
            OperationId::new("statfs-capacity"),
            b"bytes".to_vec(),
        ))
        .unwrap();
    let counting = Arc::new(CountingChunkStore::new(local.clone()));
    let fs = distributed_fs_with_chunk_store(meta.clone(), local, counting.clone());
    let inode = fs.backend_inode(&InodeId::new("inode:test")).unwrap();
    let open_writes_before = meta.open_write_calls.load(Ordering::SeqCst);

    let capacity = fs.statfs(&context(), inode).unwrap();

    assert!(capacity.bsize > 0);
    assert!(capacity.frsize > 0);
    assert!(capacity.blocks >= capacity.bfree);
    assert_eq!(counting.capacity_call_count(), 1);
    assert_eq!(
        meta.open_write_calls.load(Ordering::SeqCst),
        open_writes_before
    );
}

#[cfg(feature = "dfs")]
#[test]
fn statfs_preserves_meta_failure_and_rejects_unknown_or_mismatched_inode() {
    let (_temp, meta, fs) = test_fs();
    let inode = fs.backend_inode(&InodeId::new("inode:test")).unwrap();

    meta.fail_next_get_inode_with(unavailable("injected statfs get_inode failure"));
    assert_eq!(
        fs.statfs(&context(), inode).unwrap_err().code(),
        afs_error::NODE_VFS_UNAVAILABLE
    );

    meta.return_bad_inode_on_next_get_inode();
    assert_eq!(
        fs.statfs(&context(), inode).unwrap_err().code(),
        afs_error::NODE_VFS_INVALID
    );

    meta.inode.lock().unwrap().namespace_id = NamespaceId::new("wrong");
    assert_eq!(
        fs.statfs(&context(), inode).unwrap_err().code(),
        afs_error::NODE_VFS_INVALID
    );
    meta.inode.lock().unwrap().namespace_id = NamespaceId::new("default");

    assert_eq!(
        fs.statfs(&context(), BackendInode { value: 999 })
            .unwrap_err()
            .code(),
        afs_error::NODE_DFS_STALE_HANDLE
    );
}

#[cfg(feature = "dfs")]
#[test]
fn statfs_returns_unsupported_when_chunk_backend_has_no_capacity_capability() {
    let meta = Arc::new(RecordingMeta::new());
    let temp = tempfile::tempdir().unwrap();
    let local = Arc::new(LocalChunkStore::open(temp.path(), "node-a").unwrap());
    let unsupported = Arc::new(UnsupportedCapacityChunkStore {
        inner: local.clone(),
    });
    let fs = distributed_fs_with_chunk_store(meta.clone(), local, unsupported);
    let inode = fs.backend_inode(&InodeId::new("inode:test")).unwrap();
    let open_writes_before = meta.open_write_calls.load(Ordering::SeqCst);

    let error = fs.statfs(&context(), inode).unwrap_err();

    assert_eq!(error.code(), afs_error::NODE_VFS_UNIMPLEMENTED);
    assert_eq!(
        meta.open_write_calls.load(Ordering::SeqCst),
        open_writes_before
    );
}

/// D-S4-2: a time before 1970 cannot be stored; setattr refuses it with
/// EINVAL instead of silently storing 1970-01-01, and changes nothing.
#[test]
fn setattr_refuses_times_before_1970_instead_of_storing_the_epoch() {
    let (_temp, meta, fs) = test_fs();
    let inode_id = meta.inode.lock().unwrap().inode_id.clone();
    let inode = fs.backend_inode(&inode_id).unwrap();
    let before = meta.inode.lock().unwrap().attributes.clone();
    let old = std::time::UNIX_EPOCH - Duration::from_secs(10 * 365 * 24 * 3600);
    for change in [
        AttributeChange {
            mtime: Some(old),
            ..AttributeChange::default()
        },
        AttributeChange {
            atime: Some(old),
            ..AttributeChange::default()
        },
    ] {
        let error = fs.setattr(&context(), inode, None, &change).unwrap_err();
        assert_eq!(crate::error::errno(&error), libc::EINVAL);
    }
    assert_eq!(meta.inode.lock().unwrap().attributes, before);
    let epoch = AttributeChange {
        mtime: Some(std::time::UNIX_EPOCH + Duration::from_millis(1)),
        ..AttributeChange::default()
    };
    fs.setattr(&context(), inode, None, &epoch).unwrap();
}

fn rename_conflict() -> Error {
    Error::coded(
        afs_error::META_DFS_CONFLICT,
        "DFS metadata condition changed during rename",
    )
}

/// R83: a rename whose Meta transaction lost a condition race (the source
/// was updated between Meta's read and its commit, for example by the
/// just-closed file's own commit) is retried under the same operation id,
/// so the application sees the rename succeed instead of EBUSY.
#[test]
fn rename_retries_a_meta_condition_conflict_under_the_same_operation() {
    let (_temp, meta, fs) = test_fs();
    let root = fs
        .backend_inode(&meta.inode.lock().unwrap().inode_id)
        .unwrap();
    meta.rename_errors
        .lock()
        .unwrap()
        .extend([rename_conflict(), rename_conflict()]);
    fs.rename(
        &context(),
        root,
        OsStr::new("c1"),
        root,
        OsStr::new("c2"),
        RenameFlags(libc::RENAME_NOREPLACE),
    )
    .unwrap();
    let operations = meta.rename_operations.lock().unwrap().clone();
    assert_eq!(operations.len(), 3);
    assert!(operations.iter().all(|id| id == &operations[0]));
}

/// R83: the retry is bounded; a conflict that persists reaches the caller,
/// and errors other than a condition conflict are never retried.
#[test]
fn rename_conflict_retry_is_bounded_and_specific() {
    let (_temp, meta, fs) = test_fs();
    let root = fs
        .backend_inode(&meta.inode.lock().unwrap().inode_id)
        .unwrap();
    meta.rename_errors
        .lock()
        .unwrap()
        .extend((0..64).map(|_| rename_conflict()));
    let error = fs
        .rename(
            &context(),
            root,
            OsStr::new("a"),
            root,
            OsStr::new("b"),
            RenameFlags(0),
        )
        .unwrap_err();
    assert_eq!(error.code(), afs_error::META_DFS_CONFLICT);
    let attempts = meta.rename_operations.lock().unwrap().len();
    assert!((2..=8).contains(&attempts), "{attempts} attempts");

    meta.rename_errors.lock().unwrap().clear();
    meta.rename_operations.lock().unwrap().clear();
    meta.rename_errors.lock().unwrap().push_back(Error::coded(
        afs_error::IO_ALREADY_EXISTS,
        "DFS rename target already exists",
    ));
    let error = fs
        .rename(
            &context(),
            root,
            OsStr::new("a"),
            root,
            OsStr::new("b"),
            RenameFlags(libc::RENAME_NOREPLACE),
        )
        .unwrap_err();
    assert_eq!(error.code(), afs_error::IO_ALREADY_EXISTS);
    assert_eq!(meta.rename_operations.lock().unwrap().len(), 1);
}
