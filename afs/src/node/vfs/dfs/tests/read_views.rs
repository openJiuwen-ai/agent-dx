use super::*;

fn test_fs_with_native_authority() -> (
    tempfile::TempDir,
    Arc<RecordingMeta>,
    DistributedFs,
    Arc<crate::node::read::engine::LocalReadAuthority>,
) {
    let meta = Arc::new(RecordingMeta::new());
    let temp = tempfile::tempdir().unwrap();
    let local = Arc::new(LocalChunkStore::open(temp.path(), "node-a").unwrap());
    let authority = Arc::new(crate::node::read::engine::LocalReadAuthority::new(
        1,
        Instant::now() + Duration::from_secs(30),
    ));
    let engine = Arc::new(
        DfsReadEngine::new(
            NamespaceId::new("default"),
            "node-a".into(),
            local.clone(),
            Arc::new(crate::node::read::engine::UnimplementedReadSourceProvider),
            Arc::new(crate::node::read::engine::UnimplementedChunkTransfer),
            crate::node::read::engine::DfsReadConfig::default(),
        )
        .with_read_authority(authority.clone()),
    );
    let fs = test_dfs_new(
        NamespaceId::new("default"),
        "node-a",
        "session-a",
        meta.clone(),
        local,
        engine,
    );
    (temp, meta, fs, authority)
}

fn test_fs_with_current_external_fixture() -> (
    tempfile::TempDir,
    Arc<RecordingMeta>,
    DistributedFs,
    Arc<CurrentExternalFixture>,
) {
    let meta = Arc::new(RecordingMeta::new());
    let temp = tempfile::tempdir().unwrap();
    let chunks = Arc::new(LocalChunkStore::open(temp.path(), "node-a").unwrap());
    let fixture = Arc::new(CurrentExternalFixture::new(meta.clone()));
    let read_engine = Arc::new(
        DfsReadEngine::new(
            NamespaceId::new("default"),
            "node-a".into(),
            chunks.clone(),
            fixture.clone(),
            Arc::new(UnimplementedChunkTransfer),
            DfsReadConfig {
                source_cache_ttl: Duration::from_secs(60),
                ..DfsReadConfig::default()
            },
        )
        .with_external_backend(fixture.clone(), fixture.clone()),
    );
    let fs = test_dfs_new(
        NamespaceId::new("default"),
        "node-a",
        "session-a",
        meta.clone(),
        chunks,
        read_engine,
    );
    (temp, meta, fs, fixture)
}

fn publish_external_fixture_version(
    meta: &RecordingMeta,
    fs: &DistributedFs,
    fixture: &CurrentExternalFixture,
    operation_id: &str,
    bytes: &[u8],
) -> FileVersionId {
    let staged = StagedChunk::new(OperationId::new(operation_id), bytes.to_vec());
    fixture.insert_chunk(staged.chunk.clone(), bytes.to_vec());
    let receipt = fs.chunk_store.put(staged).unwrap();
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

fn publish_sparse_external_fixture_version(
    meta: &RecordingMeta,
    fs: &DistributedFs,
    fixture: &CurrentExternalFixture,
    operation_id: &str,
    file_length: u64,
    file_offset: u64,
    bytes: &[u8],
) -> FileVersionId {
    let staged = StagedChunk::new(OperationId::new(operation_id), bytes.to_vec());
    fixture.insert_chunk(staged.chunk.clone(), bytes.to_vec());
    let receipt = fs.chunk_store.put(staged).unwrap();
    meta.publish_external_version_with_receipts(
        file_length,
        vec![Extent {
            file_offset,
            length: bytes.len() as u64,
            chunk_id: receipt.chunk.id.clone(),
            chunk_offset: 0,
        }],
        vec![receipt],
    )
}

#[test]
fn readonly_external_version_repeated_reads_skip_fresh_head() {
    let (_temp, meta, fs, fixture) = test_fs_with_current_external_fixture();
    let inode_id = meta.inode.lock().unwrap().inode_id.clone();
    publish_external_fixture_version(&meta, &fs, &fixture, "version-stable", b"cached-view");
    let inode = fs.backend_inode(&inode_id).unwrap();
    let reader = fs.open(&context(), inode, libc::O_RDONLY).unwrap();

    let mut first = [0; 16];
    assert_eq!(fs.read(&context(), reader, 0, &mut first).unwrap(), 11);
    assert_eq!(&first[..11], b"cached-view");
    assert_eq!(meta.file_version_load_count(), 1);
    let get_inode_after_cache_fill = meta.get_inode_call_count();
    let version_auth_after_cache_fill = fixture.current_call_count();
    let legacy_after_cache_fill = fixture.legacy_call_count();

    meta.fail_next_get_inode_with(unavailable(
        "external version path should not call GetInode for an opened head",
    ));
    let mut second = [0; 16];
    assert_eq!(fs.read(&context(), reader, 0, &mut second).unwrap(), 11);
    assert_eq!(&second[..11], b"cached-view");
    assert!(
        fixture.current_call_count() >= version_auth_after_cache_fill,
        "version authorizations may be reused or renewed, but reads must not refresh head"
    );
    assert_eq!(fixture.legacy_call_count(), legacy_after_cache_fill);
    assert_eq!(meta.get_inode_call_count(), get_inode_after_cache_fill);
    assert_eq!(meta.file_version_load_count(), 1);

    fs.release(&context(), reader).unwrap();
}

// S1a-6 (R04, decision 4a): a reader pins the version committed by other
// nodes at open until close; writes made on this Node are visible to every
// handle on this Node whatever the open order. Native and S3 alike.

fn s1a6_read_all(fs: &DistributedFs, handle: FileHandle) -> Vec<u8> {
    fs.read_owned(&context(), handle, 0, 64).unwrap()
}

fn s1a6_write_and_close(fs: &DistributedFs, inode: BackendInode, bytes: &[u8]) {
    let writer = fs.open(&context(), inode, libc::O_RDWR).unwrap();
    fs.write(&context(), writer, 0, bytes).unwrap();
    fs.fsync(&context(), writer, SyncMode::Full).unwrap();
    fs.release(&context(), writer).unwrap();
}

#[test]
fn s1a6_native_reader_keeps_its_version_across_other_node_commits() {
    let (_temp, meta, fs) = test_fs();
    let inode_id = meta.inode.lock().unwrap().inode_id.clone();
    publish_external_version_with_bytes(&meta, &fs, "s1a6-n1-old", b"old-version");
    let inode = fs.backend_inode(&inode_id).unwrap();
    let reader = fs.open(&context(), inode, libc::O_RDONLY).unwrap();
    publish_external_version_with_bytes(&meta, &fs, "s1a6-n1-new", b"new");

    // Nothing on this Node moves the reader: view expiry, fstat on the
    // reader, a path stat or another open of the same file.
    expire_native_read_view(&fs, reader);
    assert_eq!(s1a6_read_all(&fs, reader), b"old-version");
    let attrs = fs.getattr(&context(), inode, Some(reader)).unwrap();
    assert_eq!(
        attrs.size, 11,
        "fstat on a pinned reader reports its version"
    );
    fs.getattr(&context(), inode, None).unwrap();
    let later = fs.open(&context(), inode, libc::O_RDONLY).unwrap();
    assert_eq!(s1a6_read_all(&fs, later), b"new");
    assert_eq!(s1a6_read_all(&fs, reader), b"old-version");
    fs.release(&context(), later).unwrap();
    fs.release(&context(), reader).unwrap();
}

#[test]
fn s1a6_native_reader_opened_first_sees_local_writes_and_keeps_them() {
    let (_temp, meta, fs) = test_fs();
    let inode_id = meta.inode.lock().unwrap().inode_id.clone();
    publish_external_version_with_bytes(&meta, &fs, "s1a6-n2-base", b"base-data");
    let inode = fs.backend_inode(&inode_id).unwrap();
    let reader = fs.open(&context(), inode, libc::O_RDONLY).unwrap();
    assert_eq!(s1a6_read_all(&fs, reader), b"base-data");

    let writer = fs.open(&context(), inode, libc::O_RDWR).unwrap();
    fs.write(&context(), writer, 0, b"LOCAL").unwrap();
    assert_eq!(
        s1a6_read_all(&fs, reader),
        b"LOCALdata",
        "dirty local write"
    );
    fs.fsync(&context(), writer, SyncMode::Full).unwrap();
    fs.release(&context(), writer).unwrap();
    assert_eq!(s1a6_read_all(&fs, reader), b"LOCALdata", "local commit");

    // Local activity ended: the reader is pinned at this Node's commit.
    publish_external_version_with_bytes(&meta, &fs, "s1a6-n2-other", b"other");
    let later = fs.open(&context(), inode, libc::O_RDONLY).unwrap();
    assert_eq!(s1a6_read_all(&fs, later), b"other");
    expire_native_read_view(&fs, reader);
    fs.getattr(&context(), inode, Some(reader)).unwrap();
    assert_eq!(s1a6_read_all(&fs, reader), b"LOCALdata");
    fs.release(&context(), later).unwrap();
    fs.release(&context(), reader).unwrap();
}

#[test]
fn s1a6_native_reader_opened_after_local_write_sees_it() {
    let (_temp, meta, fs) = test_fs();
    let inode_id = meta.inode.lock().unwrap().inode_id.clone();
    publish_external_version_with_bytes(&meta, &fs, "s1a6-n3-base", b"base-data");
    let inode = fs.backend_inode(&inode_id).unwrap();
    let writer = fs.open(&context(), inode, libc::O_RDWR).unwrap();
    fs.write(&context(), writer, 0, b"LOCAL").unwrap();
    let reader = fs.open(&context(), inode, libc::O_RDONLY).unwrap();
    assert_eq!(s1a6_read_all(&fs, reader), b"LOCALdata");
    fs.release(&context(), writer).unwrap();
    fs.release(&context(), reader).unwrap();
}

#[test]
fn s1a6_native_file_written_here_earlier_still_pins_other_node_commits() {
    let (_temp, meta, fs) = test_fs();
    let inode_id = meta.inode.lock().unwrap().inode_id.clone();
    publish_external_version_with_bytes(&meta, &fs, "s1a6-n4-base", b"base-data");
    let inode = fs.backend_inode(&inode_id).unwrap();
    s1a6_write_and_close(&fs, inode, b"mine");
    let reader = fs.open(&context(), inode, libc::O_RDONLY).unwrap();
    assert_eq!(s1a6_read_all(&fs, reader), b"mine-data");
    publish_external_version_with_bytes(&meta, &fs, "s1a6-n4-other", b"other");
    let later = fs.open(&context(), inode, libc::O_RDONLY).unwrap();
    assert_eq!(s1a6_read_all(&fs, later), b"other");
    expire_native_read_view(&fs, reader);
    assert_eq!(s1a6_read_all(&fs, reader), b"mine-data");
    fs.release(&context(), later).unwrap();
    fs.release(&context(), reader).unwrap();
}

#[test]
fn s1a6_reading_back_a_file_written_here_loads_no_version_from_meta() {
    let (_temp, meta, fs) = test_fs();
    let inode_id = meta.inode.lock().unwrap().inode_id.clone();
    let inode = fs.backend_inode(&inode_id).unwrap();
    s1a6_write_and_close(&fs, inode, b"written-here");
    let loads = meta.file_version_load_count();
    let reader = fs.open(&context(), inode, libc::O_RDONLY).unwrap();
    assert_eq!(s1a6_read_all(&fs, reader), b"written-here");
    assert_eq!(meta.file_version_load_count(), loads);
    fs.release(&context(), reader).unwrap();
}

#[test]
fn s1a6_s3_reader_keeps_its_version_across_other_node_commits() {
    let (_temp, meta, fs, fixture) = test_fs_with_current_external_fixture();
    let inode_id = meta.inode.lock().unwrap().inode_id.clone();
    publish_external_fixture_version(&meta, &fs, &fixture, "s1a6-s1-old", b"old-version");
    let inode = fs.backend_inode(&inode_id).unwrap();
    let reader = fs.open(&context(), inode, libc::O_RDONLY).unwrap();
    publish_external_fixture_version(&meta, &fs, &fixture, "s1a6-s1-new", b"new");
    fs.getattr(&context(), inode, Some(reader)).unwrap();
    fs.getattr(&context(), inode, None).unwrap();
    let later = fs.open(&context(), inode, libc::O_RDONLY).unwrap();
    assert_eq!(s1a6_read_all(&fs, later), b"new");
    assert_eq!(s1a6_read_all(&fs, reader), b"old-version");
    let mut out = [0; 32];
    let n = fs.read(&context(), reader, 0, &mut out).unwrap();
    assert_eq!(&out[..n], b"old-version");
    fs.release(&context(), later).unwrap();
    fs.release(&context(), reader).unwrap();
}

#[test]
fn s1a6_s3_reader_opened_first_keeps_local_writes_after_commit() {
    let (_temp, meta, fs, store) = test_fs_on_fake_s3();
    let inode_id = meta.inode.lock().unwrap().inode_id.clone();
    publish_external_fixture_version(&meta, &fs, &store.fixture, "s1a6-s2-base", b"base-data");
    let inode = fs.backend_inode(&inode_id).unwrap();
    let reader = fs.open(&context(), inode, libc::O_RDONLY).unwrap();
    assert_eq!(s1a6_read_all(&fs, reader), b"base-data");
    let writer = fs.open(&context(), inode, libc::O_RDWR).unwrap();
    fs.write(&context(), writer, 0, b"LOCAL").unwrap();
    assert_eq!(
        s1a6_read_all(&fs, reader),
        b"LOCALdata",
        "dirty local write"
    );
    fs.fsync(&context(), writer, SyncMode::Full).unwrap();
    fs.release(&context(), writer).unwrap();
    assert_eq!(
        s1a6_read_all(&fs, reader),
        b"LOCALdata",
        "after local commit"
    );
    let mut out = [0; 32];
    let n = fs.read(&context(), reader, 0, &mut out).unwrap();
    assert_eq!(&out[..n], b"LOCALdata");

    publish_external_fixture_version(&meta, &fs, &store.fixture, "s1a6-s2-other", b"other");
    let later = fs.open(&context(), inode, libc::O_RDONLY).unwrap();
    assert_eq!(s1a6_read_all(&fs, later), b"other");
    fs.getattr(&context(), inode, Some(reader)).unwrap();
    assert_eq!(s1a6_read_all(&fs, reader), b"LOCALdata");
    fs.release(&context(), later).unwrap();
    fs.release(&context(), reader).unwrap();
}

struct ResumeNativeReadOnDrop(Arc<NativeReadPause>);

impl Drop for ResumeNativeReadOnDrop {
    fn drop(&mut self) {
        self.0.resume();
    }
}

#[test]
fn open_authority_keeps_existing_dirty_reader_path() {
    let (_temp, _meta, fs) = test_fs();
    let created = fs
        .create(
            &context(),
            fs.root_inode(),
            OsStr::new("mixed.txt"),
            0o640,
            libc::O_RDWR,
        )
        .unwrap();
    fs.write(&context(), created.handle, 0, b"hello").unwrap();
    let reader = fs
        .open(&context(), created.entry.inode, libc::O_RDONLY)
        .unwrap();
    assert!(
        fs.handle_snapshot(reader)
            .unwrap()
            .handle
            .dynamic_read_route
            .load(Ordering::Acquire),
        "a reader opened during local write activity reads this Node's view"
    );
    assert_eq!(fs.read_owned(&context(), reader, 0, 16).unwrap(), b"hello");
    fs.release(&context(), reader).unwrap();
    fs.fsync(&context(), created.handle, SyncMode::DataOnly)
        .unwrap();
    fs.release(&context(), created.handle).unwrap();
}

#[test]
fn open_authority_local_stop_rejects_cached_bytes_and_eof() {
    let (_temp, meta, fs, authority) = test_fs_with_native_authority();
    let inode_id = meta.inode.lock().unwrap().inode_id.clone();
    publish_external_version_with_bytes(&meta, &fs, "open-stop", b"data");
    let inode = fs.backend_inode(&inode_id).unwrap();
    let reader = fs.open(&context(), inode, libc::O_RDONLY).unwrap();
    assert_eq!(fs.read_owned(&context(), reader, 0, 16).unwrap(), b"data");
    authority.stop();
    let mut output = [7; 16];
    assert!(fs.read(&context(), reader, 0, &mut output).is_err());
    assert_eq!(output, [7; 16]);
    assert!(fs.read_owned(&context(), reader, 99, 0).is_err());
    fs.release(&context(), reader).unwrap();
}

#[test]
fn fast_open_fixed_eof_and_attributes_survive_new_shorter_open() {
    let (_temp, meta, fs) = test_fs();
    let inode_id = meta.inode.lock().unwrap().inode_id.clone();
    publish_external_version_with_bytes(&meta, &fs, "fixed-old", b"old-open-data");
    let inode = fs.backend_inode(&inode_id).unwrap();
    let old = fs.open(&context(), inode, libc::O_RDONLY).unwrap();
    assert!(fs.fixed_read_view(old));
    publish_external_version_with_bytes(&meta, &fs, "fixed-new", b"new");
    let new = fs.open(&context(), inode, libc::O_RDONLY).unwrap();
    assert_eq!(fs.getattr(&context(), inode, Some(old)).unwrap().size, 13);
    assert_eq!(fs.getattr(&context(), inode, Some(new)).unwrap().size, 3);
    assert_eq!(
        fs.read_reply(&context(), old, 3, 32).unwrap().as_ref(),
        b"-open-data"
    );
    assert!(fs.read_reply(&context(), new, 3, 32).unwrap().is_empty());
    assert!(fs.read_reply(&context(), old, 13, 32).unwrap().is_empty());
    fs.release(&context(), old).unwrap();
    fs.release(&context(), new).unwrap();
}

#[test]
fn open_authority_readonly_keeps_opened_version_after_view_expiry() {
    let (_temp, meta, fs) = test_fs();
    let inode_id = meta.inode.lock().unwrap().inode_id.clone();
    publish_external_version_with_bytes(&meta, &fs, "open-authority-old", b"old-open-data");
    let inode = fs.backend_inode(&inode_id).unwrap();
    let reader = fs.open(&context(), inode, libc::O_RDONLY).unwrap();
    publish_external_version_with_bytes(&meta, &fs, "open-authority-new", b"new-open-data");
    expire_native_read_view(&fs, reader);
    let old = fs.read_owned(&context(), reader, 0, 32).unwrap();
    assert_eq!(
        old, b"old-open-data",
        "open-to-close must retain the opened version"
    );
    let next = fs.open(&context(), inode, libc::O_RDONLY).unwrap();
    assert_eq!(
        fs.read_owned(&context(), next, 0, 32).unwrap(),
        b"new-open-data"
    );
    fs.release(&context(), reader).unwrap();
    assert!(fs.read_owned(&context(), reader, 0, 1).is_err());
    fs.release(&context(), next).unwrap();
}

#[test]
fn p1_readonly_open_uses_one_fresh_get_inode() {
    let (_temp, meta, fs) = test_fs();
    let inode_id = meta.inode.lock().unwrap().inode_id.clone();
    publish_external_version_with_bytes(&meta, &fs, "p1-open-fresh", b"fresh");
    let inode = fs.backend_inode(&inode_id).unwrap();
    let before = meta.get_inode_call_count();
    let reader = fs.open(&context(), inode, libc::O_RDONLY).unwrap();
    let calls = meta.get_inode_call_count() - before;
    let data = fs.read_owned(&context(), reader, 0, 16).unwrap();
    fs.release(&context(), reader).unwrap();
    assert_eq!(
        calls, 1,
        "read-only open must reuse its fresh preflight inode"
    );
    assert_eq!(data, b"fresh");
    assert_eq!(meta.get_inode_call_count() - before, 1);
}

#[test]
fn p1_handle_snapshots_share_immutable_inode_payload() {
    let (_temp, meta, fs) = test_fs();
    let inode_id = {
        let mut inode = meta.inode.lock().unwrap();
        inode
            .xattrs
            .insert(b"user.p1-payload".to_vec(), vec![37; 64 * 1024]);
        inode.inode_id.clone()
    };
    let reader = fs
        .open(
            &context(),
            fs.backend_inode(&inode_id).unwrap(),
            libc::O_RDONLY,
        )
        .unwrap();
    let first = fs.handle_snapshot(reader).unwrap();
    let second = fs.handle_snapshot(reader).unwrap();
    let first_payload = first
        .opened_inode
        .xattrs
        .get(b"user.p1-payload".as_slice())
        .unwrap();
    let second_payload = second
        .opened_inode
        .xattrs
        .get(b"user.p1-payload".as_slice())
        .unwrap();
    fs.release(&context(), reader).unwrap();
    assert_eq!(first_payload, second_payload);
    assert_eq!(
        first_payload.as_ptr(),
        second_payload.as_ptr(),
        "snapshots of one unchanged handle must share its inode payload"
    );
}

#[test]
fn p1_backend_inode_parallel_first_lookup_is_bijective() {
    let (_temp, _meta, fs) = test_fs();
    let inode = InodeId::new("inode:p1-concurrent-first-lookup");
    *fs.hooks.backend_inode_after_missing.lock().unwrap() =
        Some(Arc::new(std::sync::Barrier::new(2)));
    let (first, second) = std::thread::scope(|scope| {
        let first = scope.spawn(|| fs.backend_inode(&inode));
        let second = scope.spawn(|| fs.backend_inode(&inode));
        (
            first.join().unwrap().unwrap(),
            second.join().unwrap().unwrap(),
        )
    });
    *fs.hooks.backend_inode_after_missing.lock().unwrap() = None;
    assert_eq!(
        first, second,
        "one DFS inode must have one backend identity"
    );
    assert_eq!(fs.inode_id(first).unwrap(), inode);
    assert_eq!(fs.inode_id(second).unwrap(), inode);
    assert_eq!(fs.backend_inode(&inode).unwrap(), first);
}

#[test]
fn p1_handle_table_register_snapshot_release_stress_is_bounded() {
    let table = DfsHandleTable::default();
    let inode = RecordingMeta::new().inode.into_inner().unwrap();
    std::thread::scope(|scope| {
        for worker in 0_u64..16 {
            let table = &table;
            let inode = inode.clone();
            scope.spawn(move || {
                for iteration in 0_u64..6250 {
                    let id = worker * 6250 + iteration + 1;
                    table
                        .insert(
                            id,
                            DfsFileHandle {
                                origin_fill: false,
                                gc_pin: None,
                                inode_id: inode.inode_id.clone(),
                                opened_inode: inode.clone(),
                                opened_page_identity: None,
                                native_read_view: None,
                                native_read_generation: 0,
                                native_denial_serial: 0,
                                write_session: None,
                                flags: libc::O_RDONLY,
                                owner_scope: None,
                            },
                            || Ok(false),
                        )
                        .unwrap();
                    let handle = table.get(id).unwrap().unwrap();
                    let snapshot = DfsFileHandleSnapshot::from_handle(&handle).unwrap();
                    assert_eq!(snapshot.inode_id, inode.inode_id);
                    assert!(
                        table
                            .for_inode(&inode.inode_id)
                            .unwrap()
                            .iter()
                            .any(|entry| Arc::ptr_eq(entry, &handle))
                    );
                    let removed = table.remove(id).unwrap().unwrap();
                    assert!(Arc::ptr_eq(&handle, &removed));
                    assert!(table.get(id).unwrap().is_none());
                }
            });
        }
    });
    assert!(table.is_empty());
    assert!(table.for_inode(&inode.inode_id).unwrap().is_empty());
    let fixed = std::mem::size_of::<DfsHandleTable>();
    assert!(
        fixed <= 16 * 1024,
        "empty sharded table metadata exceeds fixed allowance"
    );
    let point_tables = std::mem::size_of::<ShardedMap<InodeId, SharedInodeWriteState>>()
        + std::mem::size_of::<ShardedMap<InodeId, RemoteDfsWriteSession>>()
        + std::mem::size_of::<ShardedMap<InodeId, Arc<DfsLockAuthority>>>()
        + std::mem::size_of::<ShardedMap<InodeId, u64>>()
        + std::mem::size_of::<ShardedMap<u64, InodeId>>();
    assert!(
        fixed + point_tables <= 64 * 1024,
        "fixed sharded metadata exceeds its bounded non-body allowance"
    );
    println!(
        "handle_table_fixed_bytes={fixed} point_tables_fixed_bytes={point_tables} operations=100000 workers=16 live_handles=0 reverse_entries=0"
    );
}

#[test]
fn s1a6_inflight_and_later_reads_keep_the_pinned_version_across_view_renewal() {
    let (_temp, meta, fs) = test_fs();
    let fs = Arc::new(fs);
    let inode_id = meta.inode.lock().unwrap().inode_id.clone();
    publish_external_version_with_bytes(&meta, &fs, "p1-inflight-old", b"old-data");
    let inode = fs.backend_inode(&inode_id).unwrap();
    let reader = fs.open(&context(), inode, libc::O_RDONLY).unwrap();
    let pause = Arc::new(NativeReadPause::default());
    let _resume = ResumeNativeReadOnDrop(pause.clone());
    let read_pause = pause.clone();
    *fs.hooks.native_read_after_committed.lock().unwrap() =
        Some(Box::new(move || read_pause.block()));
    let read_fs = fs.clone();
    let read = std::thread::spawn(move || read_fs.read_owned(&context(), reader, 0, 32));
    pause.wait_until_reached();
    expire_native_read_view(&fs, reader);
    publish_external_version_with_bytes(&meta, &fs, "p1-inflight-new", b"new-data");
    let snapshot = fs.handle_snapshot(reader).unwrap();
    let (pinned, _) = fs.native_inode_for_read(reader, &snapshot).unwrap();
    assert_eq!(pinned.head_version, snapshot.opened_inode.head_version);
    pause.resume();
    let result = read.join().unwrap();
    let next = fs.read_owned(&context(), reader, 0, 32).unwrap();
    fs.release(&context(), reader).unwrap();
    assert_eq!(result.unwrap(), b"old-data");
    assert_eq!(next, b"old-data");
}

#[test]
fn p1_native_known_denial_rejects_inflight_committed_output() {
    let (_temp, meta, fs) = test_fs();
    let fs = Arc::new(fs);
    let inode_id = meta.inode.lock().unwrap().inode_id.clone();
    publish_external_version_with_bytes(&meta, &fs, "p1-inflight-denial", b"data");
    let inode = fs.backend_inode(&inode_id).unwrap();
    let reader = fs.open(&context(), inode, libc::O_RDONLY).unwrap();
    let pause = Arc::new(NativeReadPause::default());
    let _resume = ResumeNativeReadOnDrop(pause.clone());
    let read_pause = pause.clone();
    *fs.hooks.native_read_after_committed.lock().unwrap() =
        Some(Box::new(move || read_pause.block()));
    let read_fs = fs.clone();
    let read = std::thread::spawn(move || {
        let mut output = [0xa5; 16];
        let result = read_fs.read(&context(), reader, 0, &mut output);
        (result, output)
    });
    pause.wait_until_reached();
    meta.fail_next_get_inode_with(Error::coded(afs_error::IO_PERMISSION_DENIED, "revoked"));
    let denial = fs.getattr(&context(), inode, None);
    pause.resume();
    let (result, output) = read.join().unwrap();
    fs.release(&context(), reader).unwrap();
    assert!(denial.is_err());
    assert!(result.is_err());
    assert_eq!(
        output, [0xa5; 16],
        "denied read must leave output unchanged"
    );
}

#[test]
fn open_authority_release_racing_delivery_never_exposes_partial_output() {
    let (_temp, meta, fs) = test_fs();
    let fs = Arc::new(fs);
    let inode_id = meta.inode.lock().unwrap().inode_id.clone();
    publish_external_version_with_bytes(&meta, &fs, "p1-close-race", b"data");
    let reader = fs
        .open(
            &context(),
            fs.backend_inode(&inode_id).unwrap(),
            libc::O_RDONLY,
        )
        .unwrap();
    let pause = Arc::new(NativeReadPause::default());
    let _resume = ResumeNativeReadOnDrop(pause.clone());
    let read_pause = pause.clone();
    *fs.hooks.native_read_after_committed.lock().unwrap() =
        Some(Box::new(move || read_pause.block()));
    let read_fs = fs.clone();
    let read = std::thread::spawn(move || {
        let mut output = [0xa5; 16];
        let result = read_fs.read(&context(), reader, 0, &mut output);
        (result, output)
    });
    pause.wait_until_reached();
    let release_fs = fs.clone();
    let (entered, observed) = std::sync::mpsc::channel();
    let release = std::thread::spawn(move || {
        entered.send(()).unwrap();
        release_fs.release(&context(), reader)
    });
    observed.recv_timeout(Duration::from_secs(5)).unwrap();
    pause.resume();
    let (result, output) = read.join().unwrap();
    release.join().unwrap().unwrap();
    if let Ok(count) = result {
        assert_eq!(count, 4);
        assert_eq!(&output[..4], b"data");
        assert_eq!(&output[4..], &[0xa5; 12]);
    } else {
        assert_eq!(output, [0xa5; 16]);
    }
    assert!(fs.read_owned(&context(), reader, 0, 16).is_err());
}

#[test]
fn p1_native_release_racing_delivery_never_exposes_partial_output() {
    let (_temp, meta, fs) = test_fs();
    let fs = Arc::new(fs);
    let inode_id = meta.inode.lock().unwrap().inode_id.clone();
    publish_external_version_with_bytes(&meta, &fs, "p1-close-race", b"data");
    let reader = fs
        .open(
            &context(),
            fs.backend_inode(&inode_id).unwrap(),
            libc::O_RDONLY,
        )
        .unwrap();
    let pause = Arc::new(NativeReadPause::default());
    let _resume = ResumeNativeReadOnDrop(pause.clone());
    let read_pause = pause.clone();
    *fs.hooks.native_read_after_committed.lock().unwrap() =
        Some(Box::new(move || read_pause.block()));
    let read_fs = fs.clone();
    let read = std::thread::spawn(move || {
        let mut output = [0xa5; 16];
        let result = read_fs.read(&context(), reader, 0, &mut output);
        (result, output)
    });
    pause.wait_until_reached();
    let release_fs = fs.clone();
    let (entered, observed) = std::sync::mpsc::channel();
    let release = std::thread::spawn(move || {
        entered.send(()).unwrap();
        release_fs.release(&context(), reader)
    });
    observed.recv_timeout(Duration::from_secs(5)).unwrap();
    pause.resume();
    let (result, output) = read.join().unwrap();
    release.join().unwrap().unwrap();
    if let Ok(count) = result {
        assert_eq!(count, 4);
        assert_eq!(&output[..4], b"data");
        assert_eq!(&output[4..], &[0xa5; 12]);
    } else {
        assert_eq!(output, [0xa5; 16]);
    }
    assert!(fs.read_owned(&context(), reader, 0, 16).is_err());
}

#[test]
fn p1_native_read_capabilities_bind_exact_opened_head_and_length() {
    let (_temp, meta, fs) = test_fs();
    let inode_id = meta.inode.lock().unwrap().inode_id.clone();
    let head = publish_external_version_with_bytes(&meta, &fs, "p1-open-identity", b"payload");
    let reader = fs
        .open(
            &context(),
            fs.backend_inode(&inode_id).unwrap(),
            libc::O_RDONLY,
        )
        .unwrap();
    let inode_calls = meta.get_inode_call_count();
    let version_calls = meta.file_version_load_count();
    let identity = fs.page_cache_identity(reader);
    let parallel = fs.supports_parallel_reads(reader);
    fs.release(&context(), reader).unwrap();
    assert_eq!(
        identity,
        Some(super::super::super::PageCacheIdentity {
            version_id: Some(head.0),
            length: 7,
        })
    );
    assert!(parallel);
    assert_eq!(meta.get_inode_call_count(), inode_calls);
    assert_eq!(
        meta.file_version_load_count(),
        version_calls,
        "capability hooks cannot issue RPCs"
    );
}

#[test]
fn p1_native_empty_open_has_explicit_zero_length_identity() {
    let (_temp, meta, fs) = test_fs();
    let inode_id = meta.inode.lock().unwrap().inode_id.clone();
    let reader = fs
        .open(
            &context(),
            fs.backend_inode(&inode_id).unwrap(),
            libc::O_RDONLY,
        )
        .unwrap();
    let identity = fs.page_cache_identity(reader);
    let parallel = fs.supports_parallel_reads(reader);
    fs.release(&context(), reader).unwrap();
    assert_eq!(
        identity,
        Some(super::super::super::PageCacheIdentity {
            version_id: None,
            length: 0,
        })
    );
    assert!(parallel);
    assert_eq!(meta.file_version_load_count(), 0);
}

#[test]
fn p1_read_capabilities_exclude_writable_dirty_and_external_paths() {
    let (_temp, meta, fs) = test_fs();
    let inode_id = meta.inode.lock().unwrap().inode_id.clone();
    let inode = fs.backend_inode(&inode_id).unwrap();
    let reader = fs.open(&context(), inode, libc::O_RDONLY).unwrap();
    let writer = fs.open(&context(), inode, libc::O_RDWR).unwrap();
    fs.write(&context(), writer, 0, b"dirty").unwrap();
    assert!(fs.page_cache_identity(writer).is_none());
    assert!(!fs.supports_parallel_reads(writer));
    assert!(fs.page_cache_identity(reader).is_none());
    assert!(!fs.supports_parallel_reads(reader));
    fs.flush(&context(), writer).unwrap();
    fs.release(&context(), reader).unwrap();
    fs.release(&context(), writer).unwrap();

    let (_temp, meta, fs, _fixture) = test_fs_with_current_external_fixture();
    let inode_id = meta.inode.lock().unwrap().inode_id.clone();
    let reader = fs
        .open(
            &context(),
            fs.backend_inode(&inode_id).unwrap(),
            libc::O_RDONLY,
        )
        .unwrap();
    // S3 readers are pinned too, so FUSE admits their kernel pages per
    // version; they never use the native parallel read path.
    assert!(fs.page_cache_identity(reader).is_some());
    assert!(!fs.supports_parallel_reads(reader));
    fs.release(&context(), reader).unwrap();
}

#[test]
fn s1a6_page_cache_identity_stays_with_the_pinned_version() {
    let (_temp, meta, fs) = test_fs();
    let inode_id = meta.inode.lock().unwrap().inode_id.clone();
    publish_external_version_with_bytes(&meta, &fs, "p1-cache-id-old", b"old");
    let inode = fs.backend_inode(&inode_id).unwrap();
    let reader = fs.open(&context(), inode, libc::O_RDONLY).unwrap();
    let identity = fs.page_cache_identity(reader);
    assert!(identity.is_some());
    assert!(fs.fixed_read_view(reader));
    publish_external_version_with_bytes(&meta, &fs, "p1-cache-id-new", b"new-data");
    fs.getattr(&context(), inode, Some(reader)).unwrap();
    fs.getattr(&context(), inode, None).unwrap();
    assert_eq!(fs.page_cache_identity(reader), identity);
    assert_eq!(fs.read_owned(&context(), reader, 0, 16).unwrap(), b"old");
    let fresh = fs.open(&context(), inode, libc::O_RDONLY).unwrap();
    assert_ne!(
        fs.page_cache_identity(fresh),
        identity,
        "a new version never reuses the old version's kernel pages"
    );
    fs.release(&context(), fresh).unwrap();
    fs.release(&context(), reader).unwrap();
}

#[test]
fn p1_cached_native_read_uses_at_most_two_metadata_table_locks() {
    let (_temp, meta, fs) = test_fs();
    let inode_id = meta.inode.lock().unwrap().inode_id.clone();
    publish_external_version_with_bytes(&meta, &fs, "p1-hot-metadata", b"data");
    let reader = fs
        .open(
            &context(),
            fs.backend_inode(&inode_id).unwrap(),
            libc::O_RDONLY,
        )
        .unwrap();
    assert_eq!(fs.read_owned(&context(), reader, 0, 16).unwrap(), b"data");
    let inode_calls = meta.get_inode_call_count();
    DFS_METADATA_TABLE_LOCKS.with(|count| count.set(0));
    let result = fs.read_owned(&context(), reader, 0, 16).unwrap();
    let locks = DFS_METADATA_TABLE_LOCKS.with(std::cell::Cell::get);
    fs.release(&context(), reader).unwrap();
    assert_eq!(result, b"data");
    assert!(
        locks <= 2,
        "cached native read took {locks} metadata table locks"
    );
    assert_eq!(meta.get_inode_call_count(), inode_calls);
    println!("cached_native_metadata_table_locks={locks}");
}

#[test]
fn p1_handle_registration_racing_local_or_proxy_install_preserves_dirty_visibility() {
    for remote in [false, true] {
        let (_owner_temp, meta, owner) = test_fs();
        let owner = Arc::new(owner);
        let reader_temp = tempfile::tempdir().unwrap();
        let fs = if remote {
            let chunks = Arc::new(LocalChunkStore::open(reader_temp.path(), "node-b").unwrap());
            let reads = Arc::new(DfsReadEngine::new(
                NamespaceId::new("default"),
                "node-b".into(),
                chunks.clone(),
                Arc::new(crate::node::read::engine::UnimplementedReadSourceProvider),
                Arc::new(UnimplementedChunkTransfer),
                DfsReadConfig::default(),
            ));
            Arc::new(
                test_dfs_new(
                    NamespaceId::new("default"),
                    "node-b",
                    "session-b",
                    meta.clone(),
                    chunks,
                    reads,
                )
                .with_remote_owner_factory(Arc::new(StaticRemoteOwnerFactory {
                    owner: Arc::new(LoopbackRemoteDfsOwner {
                        owner: owner.clone(),
                        peer: "node-b".into(),
                    }),
                })),
            )
        } else {
            owner.clone()
        };
        let inode_id = meta.inode.lock().unwrap().inode_id.clone();
        let inode = fs.backend_inode(&inode_id).unwrap();
        let pause = Arc::new(NativeReadPause::default());
        let _resume = ResumeNativeReadOnDrop(pause.clone());
        let registration_pause = pause.clone();
        *fs.hooks
            .handle_registration_after_route_check
            .lock()
            .unwrap() = Some(Box::new(move || registration_pause.block()));
        let read_fs = fs.clone();
        let opening = std::thread::spawn(move || read_fs.open(&context(), inode, libc::O_RDONLY));
        pause.wait_until_reached();
        let writer_fs = fs.clone();
        let writer = std::thread::spawn(move || writer_fs.open(&context(), inode, libc::O_RDWR));
        let deadline = Instant::now() + Duration::from_secs(2);
        let mut published = false;
        while Instant::now() < deadline {
            published = if remote {
                fs.write_coordinator
                    .current_remote_provider(&inode_id)
                    .unwrap()
                    .is_some()
            } else {
                fs.write_state(&inode_id).unwrap().is_some()
            };
            if published {
                break;
            }
            std::thread::sleep(Duration::from_millis(1));
        }
        pause.resume();
        let reader = opening.join().unwrap().unwrap();
        let writer = writer.join().unwrap().unwrap();
        assert!(
            published,
            "routing map was not published before registration resumed"
        );
        assert!(
            fs.handle_snapshot(reader)
                .unwrap()
                .handle
                .dynamic_read_route
                .load(Ordering::Acquire)
        );
        fs.write(&context(), writer, 0, b"dirty").unwrap();
        assert_eq!(
            meta.commit_count(),
            0,
            "this check precedes durable publication"
        );
        assert_eq!(fs.read_owned(&context(), reader, 0, 16).unwrap(), b"dirty");
        assert!(fs.page_cache_identity(reader).is_none());
        assert!(!fs.supports_parallel_reads(reader));
        fs.flush(&context(), writer).unwrap();
        fs.release(&context(), reader).unwrap();
        fs.release(&context(), writer).unwrap();
    }
}

#[test]
fn r7_product_sequential_reads_schedule_one_two_four_objects_and_zero_stays_idle() {
    use crate::{
        dfs::*,
        node::{
            read::engine::{
                ChunkTransfer, DfsReadConfig, LocalReadAuthority, PeerReadBatch, PrefetchControl,
                ReadSourceProvider,
            },
            read::peer_cache::{
                CompletedPeerDiskRead, DiskPeerCache, PeerCacheOptions, PeerStreamSink,
            },
        },
    };
    struct Fixture {
        chunks: Vec<(ChunkObject, Vec<u8>)>,
        lookups: AtomicUsize,
        background: AtomicUsize,
    }
    impl ReadSourceProvider for Fixture {
        fn sources_for(&self, request: DfsChunkSourcesRequest) -> Result<DfsChunkSourcesReply> {
            self.lookups.fetch_add(1, Ordering::SeqCst);
            Ok(DfsChunkSourcesReply {
                revision: 1,
                chunks: self
                    .chunks
                    .iter()
                    .filter(|(chunk, _)| request.chunk_ids.contains(&chunk.id))
                    .map(|(chunk, _)| ChunkSources {
                        chunk_id: chunk.id.clone(),
                        chunk: Some(chunk.clone()),
                        allowed_ranges: vec![(0, chunk.length)],
                        sources: vec![SourceCandidate {
                            chunk_id: chunk.id.clone(),
                            copy_id: CopyId::new("copy"),
                            role: CopyRole::DurableReplica,
                            state: CopyState::Ready,
                            location: CopyLocation::Node {
                                node_id: "node-b".into(),
                                node_epoch: 1,
                                device_id: "local-0".into(),
                                device_epoch: 1,
                                catalog_revision: 1,
                            },
                            data_endpoint: Some("http://node-b".into()),
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
                    .collect(),
            })
        }
    }
    impl ChunkTransfer for Fixture {
        fn read_ranges(&self, _: &PeerReadBatch, _: &mut [u8]) -> Result<()> {
            Err(unavailable("R7 fixture must use the verified sink"))
        }
        fn supports_verified_object_sink(&self) -> bool {
            true
        }
        fn read_verified_object_to_sink<'output>(
            &self,
            _: &PeerReadBatch,
            chunk: &ChunkObject,
            offset: u64,
            out: &'output mut [u8],
            sink: &mut PeerStreamSink<'_>,
        ) -> Result<Option<CompletedPeerDiskRead<'output>>> {
            let body = &self
                .chunks
                .iter()
                .find(|(object, _)| object == chunk)
                .unwrap()
                .1;
            crate::node::rpc::peer::complete_peer_disk_read_for_test(chunk, body, offset, out, sink)
                .map(Some)
        }
        fn try_read_verified_object_to_sink<'output>(
            &self,
            batch: &PeerReadBatch,
            chunk: &ChunkObject,
            offset: u64,
            out: &'output mut [u8],
            sink: &mut PeerStreamSink<'_>,
            control: &PrefetchControl,
        ) -> Result<Option<CompletedPeerDiskRead<'output>>> {
            assert!(
                out.is_empty(),
                "background read cannot allocate a fake private object"
            );
            control.check()?;
            self.background.fetch_add(1, Ordering::SeqCst);
            self.read_verified_object_to_sink(batch, chunk, offset, out, sink)
        }
    }
    for maximum in [0, 32 * 1024 * 1024] {
        let runtime = tokio::runtime::Builder::new_multi_thread()
            .worker_threads(2)
            .enable_all()
            .build()
            .unwrap();
        let temp = tempfile::tempdir().unwrap();
        let local = Arc::new(LocalChunkStore::open(temp.path().join("local"), "node-a").unwrap());
        let fixture = Arc::new(Fixture {
            chunks: (1..=5)
                .map(|byte| {
                    let staged = StagedChunk::new(
                        OperationId::new(format!("r7-sequence-{byte}")),
                        vec![byte; 64 * 1024],
                    );
                    (staged.chunk.clone(), staged.bytes().to_vec())
                })
                .collect(),
            lookups: AtomicUsize::new(0),
            background: AtomicUsize::new(0),
        });
        let cache = Arc::new(
            DiskPeerCache::open(PeerCacheOptions {
                directory: temp.path().join("cache"),
                capacity_bytes: 2 * 1024 * 1024,
                namespace_id: NamespaceId::new("default"),
            })
            .unwrap(),
        );
        let engine = {
            let _entered = runtime.enter();
            Arc::new(
                DfsReadEngine::new(
                    NamespaceId::new("default"),
                    "node-a".into(),
                    local.clone(),
                    fixture.clone(),
                    fixture.clone(),
                    DfsReadConfig {
                        readahead_max_bytes: maximum,
                        ..DfsReadConfig::default()
                    },
                )
                .with_read_authority(Arc::new(LocalReadAuthority::new(
                    1,
                    Instant::now() + Duration::from_secs(30),
                )))
                .with_peer_cache(cache.clone()),
            )
        };
        // Window selection and best-effort body admission are separate
        // contracts. Join each test attempt before the next one so that
        // the successful 1/2/4 case does not race its own metadata locks.
        // Production and the real-RPC overlap tests have no such hook.
        let attempts = Arc::new(Mutex::new(Vec::new()));
        let attempt_windows = attempts.clone();
        let fetcher = engine.object_fetcher().unwrap();
        engine.set_prefetch_attempt_hook_for_test(Arc::new(move |window, _| {
            attempt_windows.lock().unwrap().push(window);
            fetcher.join_background_for_test().unwrap();
        }));
        let meta = Arc::new(RecordingMeta::new());
        meta.publish_external_version_with_receipts(
            5 * 64 * 1024,
            fixture
                .chunks
                .iter()
                .enumerate()
                .map(|(index, (chunk, _))| Extent {
                    file_offset: index as u64 * 64 * 1024,
                    length: chunk.length,
                    chunk_id: chunk.id.clone(),
                    chunk_offset: 0,
                })
                .collect(),
            Vec::new(),
        );
        let fs = Arc::new(test_dfs_new(
            NamespaceId::new("default"),
            "node-a",
            "session-a",
            meta.clone(),
            local,
            engine.clone(),
        ));
        let inode = fs
            .backend_inode(&meta.inode.lock().unwrap().inode_id)
            .unwrap();
        let reader = fs.open(&context(), inode, libc::O_RDONLY).unwrap();
        for (index, expected_entries) in [2, 3, 5].into_iter().enumerate() {
            assert_eq!(
                fs.read_owned(&context(), reader, index as u64 * 8, 8)
                    .unwrap(),
                [1; 8]
            );
            if maximum != 0 {
                assert_eq!(cache.stats().unwrap().entries, expected_entries);
            }
        }
        assert_eq!(
            *attempts.lock().unwrap(),
            if maximum == 0 {
                Vec::new()
            } else {
                vec![1, 2, 2, 4, 4, 4, 4]
            }
        );
        if maximum == 0 {
            // Exercise the public borrowed-output boundary with all fetch
            // slots held. The engine must not start a fresh five-second view
            // while queued; since R75 the queued read keeps its own pipeline
            // deadline instead of failing when this handle view expires, and
            // completes once the slots are released.
            let fetcher = engine.object_fetcher().unwrap();
            let permits: Vec<_> = (0..8)
                .map(|_| fetcher.try_reserve_background(1).unwrap().unwrap())
                .collect();
            {
                let opened = fs.handles.get(reader.0).unwrap().unwrap();
                opened
                    .state
                    .lock()
                    .unwrap()
                    .native_read_view
                    .as_mut()
                    .unwrap()
                    .deadline = Instant::now() + Duration::from_secs(1);
            }
            let reading_fs = fs.clone();
            let (done_tx, done_rx) = std::sync::mpsc::channel();
            let reading = std::thread::spawn(move || {
                let mut output = [91; 16];
                let result = reading_fs.read(&context(), reader, 64 * 1024 - 8, &mut output);
                done_tx.send((result.is_err(), output)).unwrap();
            });
            let before_slot_release = done_rx.recv_timeout(Duration::from_secs(2)).ok();
            drop(permits);
            reading.join().unwrap();
            assert_eq!(
                before_slot_release, None,
                "an expired handle view must not fail a read queued within its deadline"
            );
            let (failed, _) = done_rx.recv().unwrap();
            assert!(!failed, "the queued read is delivered once slots free");
        }
        fs.release(&context(), reader).unwrap();
        runtime
            .block_on(engine.object_fetcher().unwrap().shutdown())
            .unwrap();
        assert_eq!(
            fixture.background.load(Ordering::SeqCst),
            if maximum == 0 { 0 } else { 4 }
        );
        // With no read-ahead the queued read above now completes (R75) and
        // caches the next object itself: one more foreground entry.
        assert_eq!(
            cache.stats().unwrap().entries,
            if maximum == 0 { 2 } else { 5 }
        );
        assert_eq!(
            fixture.lookups.load(Ordering::SeqCst),
            1,
            "background work cannot query Meta"
        );
    }
}

#[test]
fn readonly_native_owned_read_reuses_bounded_head_and_reopen_is_fresh() {
    let (_temp, meta, fs) = test_fs();
    let inode_id = meta.inode.lock().unwrap().inode_id.clone();
    publish_external_version_with_bytes(&meta, &fs, "native-owned-old", b"old");
    let inode = fs.backend_inode(&inode_id).unwrap();
    let reader = fs.open(&context(), inode, libc::O_RDONLY).unwrap();
    let before = meta.get_inode_call_count();
    let deadline = fs
        .handle_snapshot(reader)
        .unwrap()
        .native_read_view
        .unwrap()
        .deadline;
    for _ in 0..4 {
        assert_eq!(fs.read_owned(&context(), reader, 0, 16).unwrap(), b"old");
    }
    assert_eq!(meta.get_inode_call_count(), before);
    assert_eq!(
        fs.handle_snapshot(reader)
            .unwrap()
            .native_read_view
            .unwrap()
            .deadline,
        deadline
    );

    publish_external_version_with_bytes(&meta, &fs, "native-owned-new", b"new-data");
    assert_eq!(fs.read_owned(&context(), reader, 0, 16).unwrap(), b"old");
    let fresh = fs.open(&context(), inode, libc::O_RDONLY).unwrap();
    assert_eq!(
        fs.read_owned(&context(), fresh, 0, 16).unwrap(),
        b"new-data"
    );
    expire_native_read_view(&fs, reader);
    assert_eq!(fs.read_owned(&context(), reader, 0, 16).unwrap(), b"old");
    assert!(fs.read_owned(&context(), reader, 6, 16).unwrap().is_empty());
    fs.release(&context(), fresh).unwrap();
    fs.release(&context(), reader).unwrap();
}

#[test]
fn readonly_native_meta_outage_does_not_stop_a_pinned_reader() {
    let (_temp, meta, fs) = test_fs();
    let inode_id = meta.inode.lock().unwrap().inode_id.clone();
    publish_external_version_with_bytes(&meta, &fs, "native-disconnect", b"data");
    let inode = fs.backend_inode(&inode_id).unwrap();
    let reader = fs.open(&context(), inode, libc::O_RDONLY).unwrap();
    meta.fail_next_get_inode_with(unavailable("disconnected Meta"));
    assert_eq!(fs.read_owned(&context(), reader, 0, 16).unwrap(), b"data");
    assert!(fs.open(&context(), inode, libc::O_RDONLY).is_err());
    expire_native_read_view(&fs, reader);
    meta.fail_next_get_inode_with(unavailable("disconnected Meta"));
    let mut output = [9; 16];
    assert_eq!(fs.read(&context(), reader, 0, &mut output).unwrap(), 4);
    assert_eq!(&output[..4], b"data");
    fs.release(&context(), reader).unwrap();
}

#[test]
fn readonly_native_getattr_never_moves_an_open_reader() {
    let (_temp, meta, fs) = test_fs();
    let inode_id = meta.inode.lock().unwrap().inode_id.clone();
    publish_external_version_with_bytes(&meta, &fs, "native-getattr-old", b"old");
    let inode = fs.backend_inode(&inode_id).unwrap();
    let reader = fs.open(&context(), inode, libc::O_RDONLY).unwrap();
    publish_external_version_with_bytes(&meta, &fs, "native-getattr-new", b"new-data");
    assert_eq!(fs.getattr(&context(), inode, Some(reader)).unwrap().size, 3);
    assert_eq!(fs.getattr(&context(), inode, None).unwrap().size, 8);
    assert_eq!(fs.read_owned(&context(), reader, 0, 16).unwrap(), b"old");
    fs.release(&context(), reader).unwrap();
}

#[test]
fn readonly_native_known_stop_and_denial_reject_cached_output() {
    let (_temp, meta, fs, authority) = test_fs_with_native_authority();
    let inode_id = meta.inode.lock().unwrap().inode_id.clone();
    publish_external_version_with_bytes(&meta, &fs, "native-stop", b"data");
    let inode = fs.backend_inode(&inode_id).unwrap();
    let reader = fs.open(&context(), inode, libc::O_RDONLY).unwrap();
    assert_eq!(fs.read_owned(&context(), reader, 0, 16).unwrap(), b"data");
    authority.stop();
    let mut output = [7; 16];
    assert!(fs.read(&context(), reader, 0, &mut output).is_err());
    assert_eq!(output, [7; 16]);
    assert!(fs.read_owned(&context(), reader, 99, 0).is_err());
    fs.release(&context(), reader).unwrap();

    let (_temp, meta, fs, _authority) = test_fs_with_native_authority();
    let inode_id = meta.inode.lock().unwrap().inode_id.clone();
    publish_external_version_with_bytes(&meta, &fs, "native-denial", b"data");
    let inode = fs.backend_inode(&inode_id).unwrap();
    let reader = fs.open(&context(), inode, libc::O_RDONLY).unwrap();
    meta.fail_next_get_inode_with(Error::coded(
        afs_error::IO_PERMISSION_DENIED,
        "known revoked",
    ));
    assert!(fs.open(&context(), inode, libc::O_RDONLY).is_err());
    assert!(
        fs.handle_snapshot(reader)
            .unwrap()
            .native_read_view
            .is_none()
    );
    // A pinned reader needs no Meta round trip after open (decision 4a);
    // the denial only aborts reads that were in flight.
    let before = meta.get_inode_call_count();
    assert_eq!(fs.read_owned(&context(), reader, 0, 16).unwrap(), b"data");
    assert_eq!(meta.get_inode_call_count(), before);
    fs.release(&context(), reader).unwrap();
}

#[test]
fn readonly_native_view_deadline_is_not_extended_by_heartbeat() {
    let (_temp, _meta, fs, authority) = test_fs_with_native_authority();
    let view = fs.capture_native_read_view().unwrap();
    let original = view.deadline;
    authority
        .refresh(1, Instant::now() + Duration::from_secs(60))
        .unwrap();
    assert_eq!(view.deadline, original);
    assert!(view.deadline <= Instant::now() + Duration::from_secs(5));
    let mut expired = view;
    expired.deadline = Instant::now();
    assert!(fs.check_native_read_view(&expired).is_err());
}

#[test]
fn readonly_native_publish_and_truncate_invalidate_existing_view() {
    let (_temp, meta, fs) = test_fs();
    let created = fs
        .create(
            &context(),
            fs.root_inode(),
            OsStr::new("native-local"),
            0o640,
            libc::O_RDWR,
        )
        .unwrap();
    let inode_id = meta.inode.lock().unwrap().inode_id.clone();
    let inode = fs.backend_inode(&inode_id).unwrap();
    let reader = fs.open(&context(), inode, libc::O_RDONLY).unwrap();
    assert!(
        fs.handle_snapshot(reader)
            .unwrap()
            .native_read_view
            .is_some()
    );
    fs.write(&context(), created.handle, 0, b"new-data")
        .unwrap();
    fs.fsync(&context(), created.handle, SyncMode::Full)
        .unwrap();
    assert!(
        fs.handle_snapshot(reader)
            .unwrap()
            .native_read_view
            .is_none()
    );
    assert_eq!(
        fs.read_owned(&context(), reader, 0, 16).unwrap(),
        b"new-data"
    );
    fs.setattr(
        &context(),
        inode,
        Some(created.handle),
        &AttributeChange {
            size: Some(2),
            ..AttributeChange::default()
        },
    )
    .unwrap();
    assert_eq!(fs.read_owned(&context(), reader, 0, 16).unwrap(), b"ne");
    fs.release(&context(), reader).unwrap();
    fs.release(&context(), created.handle).unwrap();
}

#[test]
fn readonly_native_refresh_reuses_winner_without_installing_late_old_snapshot() {
    let (_temp, meta, fs) = test_fs();
    let inode_id = meta.inode.lock().unwrap().inode_id.clone();
    publish_external_version_with_bytes(&meta, &fs, "native-cas-old", b"old");
    let reader = fs
        .open(
            &context(),
            fs.backend_inode(&inode_id).unwrap(),
            libc::O_RDONLY,
        )
        .unwrap();
    expire_native_read_view(&fs, reader);
    let stale_snapshot = fs.handle_snapshot(reader).unwrap();
    let old_inode = meta.inode.lock().unwrap().clone();
    publish_external_version_with_bytes(&meta, &fs, "native-cas-new", b"new-data");
    let (new_inode, new_view) = fs.native_inode_for_read(reader, &stale_snapshot).unwrap();
    assert!(new_view.generation > stale_snapshot.native_read_generation);
    // A stale caller joins/reuses the winning refresh, rather than issuing
    // a second Meta call whose older response could overwrite its head.
    *meta.inode.lock().unwrap() = old_inode;
    let calls = meta.get_inode_call_count();
    let (joined_inode, joined_view) = fs.native_inode_for_read(reader, &stale_snapshot).unwrap();
    assert_eq!(joined_inode.head_version, new_inode.head_version);
    assert_eq!(joined_view.generation, new_view.generation);
    assert_eq!(meta.get_inode_call_count(), calls);
    let current = fs.handle_snapshot(reader).unwrap();
    assert_eq!(current.opened_inode.head_version, new_inode.head_version);
    assert_eq!(current.native_read_generation, new_view.generation);
    fs.release(&context(), reader).unwrap();
}

#[test]
fn readonly_native_path_getattr_denial_aborts_views_but_reader_stays_pinned() {
    let (_temp, meta, fs) = test_fs();
    let inode_id = meta.inode.lock().unwrap().inode_id.clone();
    publish_external_version_with_bytes(&meta, &fs, "native-getattr-denied", b"data");
    let inode = fs.backend_inode(&inode_id).unwrap();
    let reader = fs.open(&context(), inode, libc::O_RDONLY).unwrap();
    meta.fail_next_get_inode_with(Error::coded(afs_error::IO_PERMISSION_DENIED, "revoked"));
    assert!(fs.getattr(&context(), inode, None).is_err());
    assert!(
        fs.handle_snapshot(reader)
            .unwrap()
            .native_read_view
            .is_none()
    );
    let before = meta.get_inode_call_count();
    assert_eq!(fs.read_owned(&context(), reader, 0, 16).unwrap(), b"data");
    assert_eq!(meta.get_inode_call_count(), before);
    assert_eq!(fs.getattr(&context(), inode, Some(reader)).unwrap().size, 4);
    fs.release(&context(), reader).unwrap();
}

#[test]
fn readonly_native_dirty_late_denial_does_not_publish_output() {
    let (_temp, meta, fs) = test_fs();
    let fs = Arc::new(fs);
    let created = fs
        .create(
            &context(),
            fs.root_inode(),
            OsStr::new("late-dirty"),
            0o640,
            libc::O_RDWR,
        )
        .unwrap();
    fs.write(&context(), created.handle, 0, b"dirty").unwrap();
    let inode_id = meta.inode.lock().unwrap().inode_id.clone();
    let inode = fs.backend_inode(&inode_id).unwrap();
    let reader = fs.open(&context(), inode, libc::O_RDONLY).unwrap();
    let hook_fs = fs.clone();
    let hook_meta = meta.clone();
    *fs.hooks.native_read_before_delivery.lock().unwrap() = Some(Box::new(move || {
        hook_meta.fail_next_get_inode_with(Error::coded(
            afs_error::IO_PERMISSION_DENIED,
            "late revoked",
        ));
        assert!(hook_fs.getattr(&context(), inode, Some(reader)).is_err());
    }));
    let mut output = [9; 8];
    assert!(fs.read(&context(), reader, 0, &mut output).is_err());
    assert_eq!(output, [9; 8]);
    // A fresh context can recover; ordinary publication must not act as a
    // known rejection and cancel its own successful dirty read.
    assert_eq!(fs.read_owned(&context(), reader, 0, 8).unwrap(), b"dirty");
    fs.release(&context(), reader).unwrap();
    fs.release(&context(), created.handle).unwrap();
}

#[test]
fn readonly_native_size_gate_precedes_dirty_allocation() {
    let (_temp, _meta, fs) = test_fs();
    let created = fs
        .create(
            &context(),
            fs.root_inode(),
            OsStr::new("size-gate"),
            0o640,
            libc::O_RDWR,
        )
        .unwrap();
    fs.write(&context(), created.handle, 0, b"data").unwrap();
    assert!(
        fs.read_owned(&context(), created.handle, 0, usize::MAX)
            .is_err()
    );
    fs.release(&context(), created.handle).unwrap();
}

#[test]
fn readonly_native_owned_sparse_fallback_preserves_zero_holes() {
    let (_temp, meta, fs) = test_fs();
    let inode_id = meta.inode.lock().unwrap().inode_id.clone();
    let receipt = fs
        .chunk_store
        .put(StagedChunk::new(
            OperationId::new("native-owned-sparse"),
            b"data".to_vec(),
        ))
        .unwrap();
    meta.publish_external_version_with_receipts(
        12,
        vec![Extent {
            file_offset: 4,
            length: 4,
            chunk_id: receipt.chunk.id.clone(),
            chunk_offset: 0,
        }],
        vec![receipt],
    );
    let inode = fs.backend_inode(&inode_id).unwrap();
    let reader = fs.open(&context(), inode, libc::O_RDONLY).unwrap();
    assert_eq!(
        fs.read_owned(&context(), reader, 0, 16).unwrap(),
        b"\0\0\0\0data\0\0\0\0"
    );
    fs.release(&context(), reader).unwrap();
}

#[test]
fn readonly_external_version_owned_returns_private_tail_and_zero_holes() {
    let (_temp, meta, fs, fixture) = test_fs_with_current_external_fixture();
    let inode_id = meta.inode.lock().unwrap().inode_id.clone();
    publish_sparse_external_fixture_version(
        &meta,
        &fs,
        &fixture,
        "version-owned-sparse",
        12,
        4,
        b"data",
    );
    let inode = fs.backend_inode(&inode_id).unwrap();
    let reader = fs.open(&context(), inode, libc::O_RDONLY).unwrap();

    let data = fs.read_owned(&context(), reader, 0, 16).unwrap();
    assert_eq!(data, b"\0\0\0\0data\0\0\0\0");
    assert_eq!(fixture.legacy_call_count(), 0);
    assert_eq!(meta.file_version_load_count(), 1);

    let tail = fs.read_owned(&context(), reader, 6, 16).unwrap();
    assert_eq!(tail, b"ta\0\0\0\0");

    fs.release(&context(), reader).unwrap();
}

#[test]
fn readonly_external_version_owned_denial_does_not_fallback_to_buffered_read() {
    let (_temp, meta, fs, fixture) = test_fs_with_current_external_fixture();
    let inode_id = meta.inode.lock().unwrap().inode_id.clone();
    publish_external_fixture_version(&meta, &fs, &fixture, "version-owned-denied", b"data");
    let inode = fs.backend_inode(&inode_id).unwrap();
    let reader = fs.open(&context(), inode, libc::O_RDONLY).unwrap();

    fixture.set_mode(CurrentExternalFixtureMode::Denied);
    meta.fail_next_get_inode_with(unavailable(
        "denied owned read must not fall back to a fresh-head buffered read",
    ));
    assert_eq!(
        fs.read_owned(&context(), reader, 0, 16)
            .expect_err("external version authorization denial must propagate")
            .code(),
        afs_error::IO_PERMISSION_DENIED
    );
    assert_eq!(fixture.legacy_call_count(), 0);

    fs.release(&context(), reader).unwrap();
}

#[test]
fn readonly_external_version_reopen_sees_new_head_and_old_handle_stays_pinned() {
    let (_temp, meta, fs, fixture) = test_fs_with_current_external_fixture();
    let inode_id = meta.inode.lock().unwrap().inode_id.clone();
    publish_external_fixture_version(&meta, &fs, &fixture, "version-old-head", b"old");
    let inode = fs.backend_inode(&inode_id).unwrap();
    let reader = fs.open(&context(), inode, libc::O_RDONLY).unwrap();
    let mut old = [0; 8];
    assert_eq!(fs.read(&context(), reader, 0, &mut old).unwrap(), 3);
    assert_eq!(&old[..3], b"old");
    let get_inode_after_old_read = meta.get_inode_call_count();

    publish_external_fixture_version(&meta, &fs, &fixture, "version-new-head", b"new-content");
    let mut still_old = [0; 16];
    assert_eq!(fs.read(&context(), reader, 0, &mut still_old).unwrap(), 3);
    assert_eq!(&still_old[..3], b"old");
    assert_eq!(meta.get_inode_call_count(), get_inode_after_old_read);

    let fresh_reader = fs.open(&context(), inode, libc::O_RDONLY).unwrap();
    let fresh = fs.read_owned(&context(), fresh_reader, 0, 16).unwrap();
    assert_eq!(fresh, b"new-content");
    fs.release(&context(), fresh_reader).unwrap();

    assert_eq!(meta.file_version_load_count(), 2);
    fs.getattr(&context(), inode, Some(reader)).unwrap();
    let loads_after_getattr = meta.file_version_load_count();
    let mut pinned = [0; 16];
    assert_eq!(fs.read(&context(), reader, 0, &mut pinned).unwrap(), 3);
    assert_eq!(&pinned[..3], b"old", "getattr never moves an open reader");
    assert_eq!(meta.file_version_load_count(), loads_after_getattr);

    fs.release(&context(), reader).unwrap();
}

#[test]
fn readonly_external_version_denial_does_not_fallback_or_modify_output() {
    let (_temp, meta, fs, fixture) = test_fs_with_current_external_fixture();
    let inode_id = meta.inode.lock().unwrap().inode_id.clone();
    publish_external_fixture_version(&meta, &fs, &fixture, "version-denied", b"cached-view");
    let inode = fs.backend_inode(&inode_id).unwrap();
    let reader = fs.open(&context(), inode, libc::O_RDONLY).unwrap();
    let get_inode_after_cache_fill = meta.get_inode_call_count();
    let legacy_after_cache_fill = fixture.legacy_call_count();

    fixture.set_mode(CurrentExternalFixtureMode::Denied);
    meta.fail_next_get_inode_with(unavailable(
        "denied version authorization must not fall back to GetInode",
    ));
    let mut second = [b'!'; 16];
    assert_eq!(
        fs.read(&context(), reader, 0, &mut second)
            .expect_err("external version authorization denial must propagate")
            .code(),
        afs_error::IO_PERMISSION_DENIED
    );
    assert_eq!(second, [b'!'; 16]);
    assert!(fixture.current_call_count() >= 1);
    assert_eq!(fixture.legacy_call_count(), legacy_after_cache_fill);
    assert_eq!(meta.get_inode_call_count(), get_inode_after_cache_fill);

    fs.release(&context(), reader).unwrap();
}

#[test]
fn c1b_version_cache_estimate_shares_runtime_floor_and_checks_overflow() {
    let slot = (std::mem::size_of::<(FileVersionId, CachedReadOnlyVersion, u64)>()
        + std::mem::size_of::<(u64, FileVersionId)>())
        * 4
        + 64;
    let fixed = std::mem::size_of::<ReadOnlyVersion>() + 2 * std::mem::size_of::<usize>();
    for entries in [1_usize, 4096, 65536] {
        let table = entries * slot + 2048;
        let minimum = version_cache_minimum_bytes(entries).unwrap();
        assert_eq!(
            ReadOnlyVersionCache::new(entries, minimum).table_bytes,
            table
        );
        assert_eq!(minimum, table + fixed);
    }
    assert!(version_cache_table_bytes(usize::MAX).is_none());
    assert!(version_cache_minimum_bytes(usize::MAX).is_none());
    let last_fitting = (usize::MAX - 2048 - fixed) / slot;
    assert!(version_cache_minimum_bytes(last_fitting).is_some());
    assert!(version_cache_minimum_bytes(last_fitting + 1).is_none());
    assert_eq!(
        ReadOnlyVersionCache::new(usize::MAX, 1).table_bytes,
        usize::MAX
    );
}

#[test]
fn c1b_version_cache_floor_does_not_exempt_actual_identifier_capacity() {
    let value = p2_byte_cache_version("nonempty-version", 0);
    let key = value.version.as_ref().unwrap().id.clone();
    let minimum = version_cache_minimum_bytes(1).unwrap();
    let table = version_cache_table_bytes(1).unwrap();
    let retained = value.cache_retained_bytes(&key);
    assert!(retained > ReadOnlyVersion::fixed_retained_bytes());
    let mut cache = ReadOnlyVersionCache::new(1, minimum);
    cache.insert(key.clone(), value.clone());
    assert_eq!(
        cache.len(),
        0,
        "startup floor does not waive actual heap charges"
    );
    assert_eq!(cache.value_bytes, 0);
    let mut cache = ReadOnlyVersionCache::new(1, table + retained);
    cache.insert(key.clone(), value);
    assert!(cache.get(&key).is_some());
    assert_eq!(cache.table_bytes + cache.value_bytes, table + retained);
}

/// R75: a read that starts on a valid view and outlives the view's reuse
/// period (or its captured lease) is still delivered, because the Node's read
/// authority never lapsed in between.
#[test]
fn r75_read_crossing_view_expiry_is_delivered() {
    let (_temp, meta, fs, authority) = test_fs_with_native_authority();
    let fs = Arc::new(fs);
    *fs.hooks.native_read_view_ttl.lock().unwrap() = Some(Duration::from_millis(50));
    let inode_id = meta.inode.lock().unwrap().inode_id.clone();
    publish_external_version_with_bytes(&meta, &fs, "r75-cross", b"data");
    let inode = fs.backend_inode(&inode_id).unwrap();
    let reader = fs.open(&context(), inode, libc::O_RDONLY).unwrap();
    assert_eq!(fs.read_owned(&context(), reader, 0, 16).unwrap(), b"data");
    let heartbeat = authority.clone();
    *fs.hooks.native_read_after_committed.lock().unwrap() = Some(Box::new(move || {
        std::thread::sleep(Duration::from_millis(150));
        heartbeat
            .refresh(1, Instant::now() + Duration::from_secs(30))
            .unwrap();
    }));
    assert_eq!(fs.read_owned(&context(), reader, 0, 16).unwrap(), b"data");
    fs.release(&context(), reader).unwrap();
}

/// R75 reverse: a real authority change while the read is in flight still
/// rejects delivery, whether the Node stopped or re-registered.
#[test]
fn r75_authority_change_during_a_slow_read_still_rejects_delivery() {
    for change in ["stop", "epoch"] {
        let (_temp, meta, fs, authority) = test_fs_with_native_authority();
        let fs = Arc::new(fs);
        *fs.hooks.native_read_view_ttl.lock().unwrap() = Some(Duration::from_millis(50));
        let inode_id = meta.inode.lock().unwrap().inode_id.clone();
        publish_external_version_with_bytes(&meta, &fs, "r75-change", b"data");
        let inode = fs.backend_inode(&inode_id).unwrap();
        let reader = fs.open(&context(), inode, libc::O_RDONLY).unwrap();
        assert_eq!(fs.read_owned(&context(), reader, 0, 16).unwrap(), b"data");
        let changed = authority.clone();
        *fs.hooks.native_read_after_committed.lock().unwrap() = Some(Box::new(move || {
            std::thread::sleep(Duration::from_millis(150));
            if change == "stop" {
                changed.stop();
            } else {
                let _ = changed.refresh(2, Instant::now() + Duration::from_secs(30));
            }
        }));
        let mut output = [7; 16];
        let error = fs.read(&context(), reader, 0, &mut output).unwrap_err();
        assert_eq!(
            error.kind(),
            afs_error::ErrorKind::PermissionDenied,
            "{change}"
        );
        assert_eq!(output, [7; 16], "{change}");
        fs.release(&context(), reader).unwrap();
    }
}
