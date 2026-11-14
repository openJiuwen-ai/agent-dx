use super::*;

/// The retained body of a frozen extent: inline for S3 (BLAKE3) tails, or
/// held by its native upload for CRC32C tails, which keep their immutable
/// object id across retries (S1a-7, F6).
fn frozen_extent_body(extent: &DirtyExtent) -> Option<SharedChunkBytes> {
    extent.data.clone().or_else(|| {
        extent
            .native_upload
            .as_ref()
            .and_then(|upload| upload.state.lock().unwrap().body.clone())
    })
}

#[test]
fn native_builder_six_megabytes_freezes_two_allocations_without_body_move() {
    let pool = WriteBufferPool::new(16 * 1024 * 1024);
    let mut dirty = DirtyExtentMap::default();
    for index in 0..6u64 {
        let bytes = vec![index as u8 + 1; 1024 * 1024];
        assert_eq!(
            dirty
                .write_native_at(
                    index * 1024 * 1024,
                    &bytes,
                    index + 1,
                    &pool,
                    index * 1024 * 1024
                )
                .unwrap(),
            bytes.len()
        );
    }
    assert_eq!(dirty.builders.len(), 2);
    assert_eq!(dirty.builders[0].builder.len(), 4 * 1024 * 1024);
    assert_eq!(dirty.builders[1].builder.len(), 2 * 1024 * 1024);
    let identities = dirty
        .builders
        .iter()
        .map(|entry| {
            (
                entry.builder.data_ptr(),
                entry.builder.capacity(),
                entry.builder.allocation_id(),
            )
        })
        .collect::<Vec<_>>();
    dirty.freeze_builders();
    assert!(dirty.builders.is_empty());
    let mut bodies = HashMap::new();
    for extent in dirty.iter_extents() {
        let body = extent.data.as_ref().unwrap();
        bodies.entry(body.allocation_id()).or_insert(body.clone());
    }
    assert_eq!(bodies.len(), 2);
    for (pointer, capacity, generation) in identities {
        let body = &bodies[&generation];
        assert_eq!(body.as_ptr(), pointer);
        assert_eq!(body.capacity(), capacity);
    }
    assert_eq!(dirty.dirty_data_bytes(), 6 * 1024 * 1024);
}

/// R81 F1: sequential 4 KiB writes inside a file (for example after
/// `ftruncate` to the final size) grow their builders instead of making one
/// chunk per write. Before, each write got a 4 KiB builder: 4096 chunks for a
/// 16 MiB file. A write that does not continue the last one stays small.
#[test]
fn sequential_writes_inside_a_file_double_their_builders_up_to_a_chunk() {
    let pool = WriteBufferPool::new(64 * 1024 * 1024);
    let mut dirty = DirtyExtentMap::default();
    let logical = 16 * 1024 * 1024;
    let write = |dirty: &mut DirtyExtentMap, position: u64, seq: u64| {
        let data = vec![(seq % 251) as u8; 4096];
        let selection = dirty.native_selection(position);
        let prepared = selection.is_none().then(|| {
            ChunkBuilder::with_pool(pool.clone(), dirty.native_capacity(position, 4096, logical))
                .unwrap()
        });
        dirty
            .write_native_prepared(position, &data, seq, selection, prepared, None)
            .unwrap()
    };
    for index in 0..4096u64 {
        assert_eq!(write(&mut dirty, index * 4096, index + 1), 4096);
    }
    assert!(
        dirty.builders.len() <= 16,
        "{} builders for 4096 sequential writes",
        dirty.builders.len()
    );
    assert!(
        dirty
            .builders
            .iter()
            .any(|entry| entry.builder.len() == COMMIT_CHUNK_BYTES as usize)
    );
    assert_eq!(dirty.dirty_data_bytes(), logical);
    // A random write elsewhere is not a continuation: its own size only.
    assert_eq!(dirty.native_capacity(3 * 4096 + 123, 4096, logical), 4096);
}

struct NativeFixtureStore {
    local: Arc<LocalChunkStore>,
    required: bool,
    calls: AtomicUsize,
    fail_next: AtomicBool,
    pause: Mutex<Option<Arc<CommitPause>>>,
}

impl ChunkSink for NativeFixtureStore {
    fn put_chunks(&self, staged: Vec<StagedChunk>, intent: PutIntent) -> Result<Vec<StoredChunk>> {
        if intent == PutIntent::Commit {
            return self.local.put_chunks(staged, intent);
        }
        self.calls.fetch_add(staged.len(), Ordering::SeqCst);
        if let Some(pause) = self.pause.lock().unwrap().take() {
            pause.block();
        }
        if self.fail_next.swap(false, Ordering::SeqCst) {
            return Err(unavailable("injected native upload failure"));
        }
        let mut stored = self.local.put_chunks(staged, intent)?;
        for stored in &mut stored {
            stored.body_release = if self.required {
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
        self.local
            .open_receipt_reader(&receipt.chunk)?
            .read_at(offset, out)
    }
}

fn native_pipeline_fixture(
    required: bool,
    live: usize,
    descriptors: usize,
) -> (
    tempfile::TempDir,
    Arc<RecordingMeta>,
    DistributedFs,
    tokio::runtime::Runtime,
    Arc<NativeFixtureStore>,
) {
    let temp = tempfile::tempdir().unwrap();
    let meta = Arc::new(RecordingMeta::new());
    let local = Arc::new(LocalChunkStore::open(temp.path(), "node-a").unwrap());
    let store = Arc::new(NativeFixtureStore {
        local: local.clone(),
        required,
        calls: AtomicUsize::new(0),
        fail_next: AtomicBool::new(false),
        pause: Mutex::new(None),
    });
    let runtime = tokio::runtime::Builder::new_multi_thread()
        .worker_threads(2)
        .enable_all()
        .build()
        .unwrap();
    let fs = distributed_fs_with_chunk_store(meta.clone(), local, store.clone())
        .with_write_buffer_pool(WriteBufferPool::new(0))
        .with_write_pipeline(
            DirtyBudget::new(live),
            WritePipelineConfig {
                unpublished_descriptors: descriptors,
                upload_inflight_chunks: 2,
            },
            runtime.handle().clone(),
            None,
        )
        .unwrap();
    (temp, meta, fs, runtime, store)
}

fn native_wait_uploads(fs: &DistributedFs, completed: u64) {
    let deadline = Instant::now() + Duration::from_secs(5);
    loop {
        fs.progress_writeback().unwrap();
        if fs
            .write_resources
            .pipeline
            .as_ref()
            .unwrap()
            .metadata
            .state
            .lock()
            .unwrap()
            .completed
            >= completed
        {
            return;
        }
        assert!(
            Instant::now() < deadline,
            "native uploads did not finish within bounded test window"
        );
        std::thread::sleep(Duration::from_millis(1));
    }
}

#[test]
fn native_stalled_upload_keeps_ordinary_write_off_the_fuse_receiver() {
    let (_temp, _meta, fs, _runtime, store) = native_pipeline_fixture(true, 4 * 1024 * 1024, 8);
    let fs = Arc::new(fs);
    let created = fs
        .create(
            &context(),
            fs.root_inode(),
            OsStr::new("stalled-write-admission"),
            0o600,
            libc::O_RDWR,
        )
        .unwrap();
    let body = vec![0x5b; 64 * 1024];
    fs.write(&context(), created.handle, 0, &body).unwrap();
    let pause = Arc::new(CommitPause::new(0));
    *store.pause.lock().unwrap() = Some(pause.clone());
    let writer_fs = fs.clone();
    let writer =
        std::thread::spawn(move || writer_fs.fsync(&context(), created.handle, SyncMode::Full));
    pause.wait_until_reached();
    // Another ordinary write can wait for this inode's mutation/publication.
    // Its admission contract must keep that wait off the receive thread.
    let admission = fs.write_admission_mode(created.handle);
    pause.release();
    writer.join().unwrap().unwrap();
    let mut actual = vec![0; body.len()];
    assert_eq!(
        fs.read(&context(), created.handle, 0, &mut actual).unwrap(),
        body.len()
    );
    fs.release(&context(), created.handle).unwrap();
    assert_eq!(
        actual, body,
        "queued admission must retain the durability barrier"
    );
    assert_eq!(
        admission,
        WriteAdmissionMode::Queued,
        "ordinary native writes can wait for publication and must not hold the FUSE receiver"
    );
}

#[test]
fn native_generation_retains_gc_reference_through_upload_and_publication() {
    #[derive(Default)]
    struct Control {
        active: Mutex<HashSet<String>>,
    }
    impl crate::node::gc::GcControl for Control {
        fn retain(&self, id: &str, _: &InodeId, _: Option<&FileVersionId>) -> Result<()> {
            self.active.lock().unwrap().insert(id.into());
            Ok(())
        }
        fn retain_fenced(
            &self,
            id: &str,
            _: &InodeId,
            _: Option<&FileVersionId>,
        ) -> Result<crate::dfs::GcFence> {
            self.active.lock().unwrap().insert(id.into());
            Ok(crate::dfs::GcFence::WriteGeneration)
        }
        fn release(&self, ids: &[String]) -> Result<()> {
            let mut active = self.active.lock().unwrap();
            for id in ids {
                active.remove(id);
            }
            Ok(())
        }
        fn collect(&self, _: u64, _: Vec<ChunkObject>) -> Result<crate::dfs::GcState> {
            Ok(crate::dfs::GcState::default())
        }
    }
    let (_temp, meta, fs, _runtime, _store) = native_pipeline_fixture(true, 8 * 1024 * 1024, 8);
    let control = Arc::new(Control::default());
    let references = crate::node::gc::GcReferences::new(control.clone());
    let fs = fs.with_gc_references(Some(references.clone()));
    let file = fs
        .create(
            &context(),
            fs.root_inode(),
            OsStr::new("native-gc"),
            0o640,
            libc::O_RDWR,
        )
        .unwrap();
    let body = vec![7; 4 * 1024 * 1024];
    fs.write(&context(), file.handle, 0, &body).unwrap();
    native_wait_uploads(&fs, 1);
    references.flush().unwrap();
    assert_eq!(meta.commits.lock().unwrap().len(), 0);
    assert_eq!(
        control
            .active
            .lock()
            .unwrap()
            .iter()
            .filter(|id| id.starts_with("write:"))
            .count(),
        1,
        "uploaded but unpublished generation must remain protected"
    );
    fs.fsync(&context(), file.handle, SyncMode::Full).unwrap();
    references.flush().unwrap();
    assert_eq!(meta.commits.lock().unwrap().len(), 1);
    assert_eq!(
        control
            .active
            .lock()
            .unwrap()
            .iter()
            .filter(|id| id.starts_with("write:"))
            .count(),
        0,
        "published generation must release its reference"
    );
}

#[test]
fn p2_w2_required_upload_retires_body_and_dirty_read_uses_anchored_local_object() {
    let (_temp, meta, fs, _runtime, store) = native_pipeline_fixture(true, 8 * 1024 * 1024, 8);
    let file = fs
        .create(
            &context(),
            fs.root_inode(),
            OsStr::new("w2-required"),
            0o640,
            libc::O_RDWR,
        )
        .unwrap();
    let body = vec![0x29; 4 * 1024 * 1024];
    assert_eq!(
        fs.write(&context(), file.handle, 0, &body).unwrap(),
        body.len()
    );
    native_wait_uploads(&fs, 1);
    assert_eq!(store.calls.load(Ordering::SeqCst), 1);
    assert_eq!(meta.commits.lock().unwrap().len(), 0);
    assert_eq!(
        fs.write_resources
            .pipeline
            .as_ref()
            .unwrap()
            .budget
            .snapshot()
            .live_bytes,
        0
    );
    let mut out = [0; 4096];
    assert_eq!(
        fs.read(&context(), file.handle, 8192, &mut out).unwrap(),
        out.len()
    );
    assert_eq!(out, [0x29; 4096]);
    fs.fsync(&context(), file.handle, SyncMode::Full).unwrap();
    assert_eq!(meta.commits.lock().unwrap().len(), 1);
    assert_eq!(
        fs.write_resources
            .pipeline
            .as_ref()
            .unwrap()
            .metadata
            .state
            .lock()
            .unwrap()
            .descriptors,
        0
    );
    fs.release(&context(), file.handle).unwrap();
}

#[test]
fn p2_w2_nonrequired_upload_keeps_body_until_publication_and_pressure_progresses() {
    let (_temp, meta, fs, _runtime, _store) = native_pipeline_fixture(false, 4 * 1024 * 1024, 8);
    let file = fs
        .create(
            &context(),
            fs.root_inode(),
            OsStr::new("w2-nonrequired"),
            0o640,
            libc::O_RDWR,
        )
        .unwrap();
    let body = vec![0x47; 4 * 1024 * 1024];
    fs.write(&context(), file.handle, 0, &body).unwrap();
    native_wait_uploads(&fs, 1);
    assert_eq!(
        fs.write_resources
            .pipeline
            .as_ref()
            .unwrap()
            .budget
            .snapshot()
            .live_bytes,
        body.len()
    );
    assert!(meta.commits.lock().unwrap().is_empty());
    assert_eq!(
        fs.write(&context(), file.handle, body.len() as u64, &body)
            .unwrap(),
        body.len()
    );
    assert_eq!(
        meta.commits.lock().unwrap().len(),
        1,
        "D14 live pressure must publish the older generation without changing copy policy"
    );
    fs.fsync(&context(), file.handle, SyncMode::Full).unwrap();
    assert_eq!(meta.commits.lock().unwrap().len(), 2);
    let pipeline = fs.write_resources.pipeline.as_ref().unwrap();
    assert_eq!(pipeline.budget.snapshot().live_bytes, 0);
    assert!(pipeline.metadata.state.lock().unwrap().pressure_live > 0);
    fs.release(&context(), file.handle).unwrap();
}

#[test]
fn p2_w2_descriptor_pressure_publishes_before_accepting_the_next_descriptor() {
    let (_temp, meta, fs, _runtime, _store) = native_pipeline_fixture(true, 8 * 1024 * 1024, 1);
    let file = fs
        .create(
            &context(),
            fs.root_inode(),
            OsStr::new("w2-descriptor"),
            0o640,
            libc::O_RDWR,
        )
        .unwrap();
    let body = vec![0x63; 4 * 1024 * 1024];
    fs.write(&context(), file.handle, 0, &body).unwrap();
    native_wait_uploads(&fs, 1);
    assert!(meta.commits.lock().unwrap().is_empty());
    fs.write(&context(), file.handle, body.len() as u64, &body)
        .unwrap();
    assert_eq!(meta.commits.lock().unwrap().len(), 1);
    let pipeline = fs.write_resources.pipeline.as_ref().unwrap();
    assert_eq!(
        pipeline
            .metadata
            .state
            .lock()
            .unwrap()
            .descriptor_peak_count,
        1
    );
    assert!(pipeline.metadata.state.lock().unwrap().pressure_descriptor > 0);
    fs.fsync(&context(), file.handle, SyncMode::Full).unwrap();
    fs.release(&context(), file.handle).unwrap();
}

#[test]
fn p2_w2_timer_and_stop_drain_publish_tails_but_closed_admission_rejects_new_input() {
    let (_temp, meta, fs, _runtime, _store) = native_pipeline_fixture(true, 8 * 1024 * 1024, 8);
    let file = fs
        .create(
            &context(),
            fs.root_inode(),
            OsStr::new("w2-timer-stop"),
            0o640,
            libc::O_RDWR,
        )
        .unwrap();
    fs.write(&context(), file.handle, 0, b"tail").unwrap();
    fs.progress_writeback().unwrap();
    assert!(meta.commits.lock().unwrap().is_empty());
    assert_eq!(
        fs.writeback_pending().unwrap(),
        1,
        "the Node timer is an explicit publication entry"
    );
    assert_eq!(meta.commits.lock().unwrap().len(), 1);
    fs.write(&context(), file.handle, 4, b"more").unwrap();
    fs.close_write_admission();
    assert!(fs.write(&context(), file.handle, 8, b"x").is_err());
    assert_eq!(fs.drain().unwrap(), 1);
    assert_eq!(meta.commits.lock().unwrap().len(), 2);
    assert_eq!(
        fs.write_resources
            .pipeline
            .as_ref()
            .unwrap()
            .budget
            .snapshot()
            .live_bytes,
        0
    );
    fs.release(&context(), file.handle).unwrap();
}

#[test]
fn p2_w2_unknown_request_keeps_original_identity_and_body_until_exact_retry() {
    let (_temp, meta, fs, _runtime, _store) = native_pipeline_fixture(false, 8 * 1024 * 1024, 8);
    let file = fs
        .create(
            &context(),
            fs.root_inode(),
            OsStr::new("w2-unknown"),
            0o640,
            libc::O_RDWR,
        )
        .unwrap();
    fs.write(&context(), file.handle, 0, b"exact").unwrap();
    meta.fail_next_commit();
    assert!(fs.fsync(&context(), file.handle, SyncMode::Full).is_err());
    let failed = meta.failed_commits.lock().unwrap()[0].clone();
    let cell = fs
        .write_state(&fs.inode_id(file.entry.inode).unwrap())
        .unwrap()
        .unwrap();
    let owner = {
        let state = cell.lock().unwrap();
        let Some(InFlightCommit::File(pending)) = state.in_flight.as_ref() else {
            panic!("unknown request identity was not retained");
        };
        assert_eq!(pending.batch.commit.as_ref(), &failed);
        frozen_extent_body(pending.frozen.dirty_extents.iter_extents().next().unwrap()).unwrap()
    };
    meta.fail_next_commit();
    assert!(fs.write(&context(), file.handle, 5, b"!").is_err());
    assert_eq!(meta.failed_commits.lock().unwrap()[1], failed);
    for change in [
        AttributeChange {
            size: Some(2),
            ..AttributeChange::default()
        },
        AttributeChange {
            mode: Some(0o600),
            ..AttributeChange::default()
        },
    ] {
        meta.fail_next_commit();
        assert!(
            fs.setattr(&context(), file.entry.inode, Some(file.handle), &change)
                .is_err()
        );
        assert_eq!(meta.failed_commits.lock().unwrap().last().unwrap(), &failed);
    }
    fs.fsync(&context(), file.handle, SyncMode::Full).unwrap();
    assert_eq!(meta.commits.lock().unwrap()[0], failed);
    assert!(
        fs.write_resources
            .pipeline
            .as_ref()
            .unwrap()
            .budget
            .snapshot()
            .live_bytes
            > 0,
        "a real frozen read holder must keep its allocation live after publish"
    );
    drop(owner);
    assert_eq!(
        fs.write_resources
            .pipeline
            .as_ref()
            .unwrap()
            .budget
            .snapshot()
            .live_bytes,
        0
    );
    fs.release(&context(), file.handle).unwrap();
}

#[test]
fn p2_w2_control_fifo_cancellation_and_last_descriptor_owner_release_capacity() {
    let pipeline = Arc::new(WritePipeline::new(
        DirtyBudget::new(4 * 1024 * 1024),
        WritePipelineConfig::default(),
        test_node_runtime(),
        None,
    ));
    let mut queue = MutationTicketQueue::default();
    queue.push(10, &pipeline.metadata).unwrap();
    queue.push(11, &pipeline.metadata).unwrap();
    queue.push(12, &pipeline.metadata).unwrap();
    queue.remove(11);
    assert_eq!(
        queue.values.iter().copied().collect::<Vec<_>>(),
        vec![10, 12]
    );
    queue.remove(10);
    assert_eq!(queue.values.front(), Some(&12));
    queue.remove(12);
    assert_eq!(queue.values.capacity(), 0);
    let descriptor = Arc::new(Mutex::new(
        pipeline.metadata.reserve(16 * 1024, true).unwrap().unwrap(),
    ));
    descriptor.lock().unwrap().set_actual(512).unwrap();
    let frozen = descriptor.clone();
    drop(descriptor);
    assert_eq!(pipeline.metadata.state.lock().unwrap().descriptors, 1);
    drop(frozen);
    assert_eq!(pipeline.metadata.state.lock().unwrap().descriptors, 0);
    assert_eq!(
        pipeline
            .metadata
            .state
            .lock()
            .unwrap()
            .descriptor_reserved_bytes,
        0
    );
    assert_eq!(pipeline.metadata.state.lock().unwrap().descriptor_bytes, 0);
}

#[test]
fn p2_w2_large_existing_layout_small_patch_keeps_the_original_fsync_capability() {
    let (_temp, meta, fs, _runtime, _store) = native_pipeline_fixture(true, 8 * 1024 * 1024, 8);
    let file = fs
        .create(
            &context(),
            fs.root_inode(),
            OsStr::new("w2-large-base"),
            0o640,
            libc::O_RDWR,
        )
        .unwrap();
    fs.release(&context(), file.handle).unwrap();
    let extents = (0..65_536_u64)
        .map(|index| Extent {
            file_offset: index * 2,
            length: 1,
            chunk_id: crate::dfs::ChunkId::new("existing-immutable-base-object"),
            chunk_offset: 0,
        })
        .collect();
    meta.publish_external_version_with_receipts(131_072, extents, Vec::new());
    let inode = fs.inode_id(file.entry.inode).unwrap();
    fs.writes.states.write(&inode).unwrap().remove(&inode);
    let writer = fs.open(&context(), file.entry.inode, libc::O_RDWR).unwrap();
    fs.write(&context(), writer, 8192, &[0x92; 4096]).unwrap();
    fs.fsync(&context(), writer, SyncMode::Full).unwrap();
    let commits = meta.commits.lock().unwrap();
    let committed = commits.last().unwrap();
    assert_eq!(committed.file_version.length, 131_072);
    assert!(committed.layout_root.inline_extents.len() > 60_000);
    assert_eq!(committed.chunk_receipts.len(), 1);
    drop(commits);
    fs.release(&context(), writer).unwrap();
}

#[test]
fn p2_w2_actual_cross_block_allocation_failure_returns_the_visible_prefix() {
    let (_temp, meta, fs, _runtime, _store) = native_pipeline_fixture(false, 8 * 1024 * 1024, 8);
    let file = fs
        .create(
            &context(),
            fs.root_inode(),
            OsStr::new("w2-short"),
            0o640,
            libc::O_RDWR,
        )
        .unwrap();
    fs.write(&context(), file.handle, 0, &vec![1; 1024 * 1024])
        .unwrap();
    fs.write_resources.buffer_pool.fail_next_allocation();
    assert_eq!(
        fs.write(
            &context(),
            file.handle,
            1024 * 1024,
            &vec![2; 4 * 1024 * 1024]
        )
        .unwrap(),
        3 * 1024 * 1024
    );
    assert!(meta.commits.lock().unwrap().is_empty());
    fs.fsync(&context(), file.handle, SyncMode::Full).unwrap();
    assert_eq!(
        meta.commits.lock().unwrap()[0].file_version.length,
        4 * 1024 * 1024
    );
    assert_eq!(
        fs.write_resources.buffer_pool.snapshot().copied_bytes,
        4 * 1024 * 1024
    );
    fs.release(&context(), file.handle).unwrap();
}

#[test]
fn p2_w2_budget_waiter_fifo_does_not_block_timer_or_the_following_sync() {
    let (_temp, meta, fs, _runtime, _store) = native_pipeline_fixture(true, 4 * 1024 * 1024, 8);
    let fs = Arc::new(fs);
    let file = fs
        .create(
            &context(),
            fs.root_inode(),
            OsStr::new("w2-fifo-progress"),
            0o640,
            libc::O_RDWR,
        )
        .unwrap();
    let other = fs.open(&context(), file.entry.inode, libc::O_RDWR).unwrap();
    fs.write(&context(), file.handle, 0, b"a").unwrap();
    let worker_fs = fs.clone();
    let worker = std::thread::spawn(move || worker_fs.write(&context(), other, 4096, b"b"));
    let deadline = Instant::now() + Duration::from_secs(5);
    while fs
        .write_resources
        .pipeline
        .as_ref()
        .unwrap()
        .allocation_queue
        .lock()
        .unwrap()
        .values
        .is_empty()
    {
        assert!(Instant::now() < deadline);
        std::thread::sleep(Duration::from_millis(1));
    }
    let sync_fs = fs.clone();
    let sync = std::thread::spawn(move || sync_fs.fsync(&context(), file.handle, SyncMode::Full));
    let cell = fs
        .write_state(&fs.inode_id(file.entry.inode).unwrap())
        .unwrap()
        .unwrap();
    loop {
        let state = cell.lock().unwrap();
        if state.native_queue.values.len() == 2 {
            assert!(!state.operation_busy);
            break;
        }
        drop(state);
        assert!(Instant::now() < deadline);
        std::thread::sleep(Duration::from_millis(1));
    }
    fs.writeback_pending().unwrap();
    assert_eq!(worker.join().unwrap().unwrap(), 1);
    sync.join().unwrap().unwrap();
    assert_eq!(meta.commits.lock().unwrap().len(), 2);
    assert!(cell.lock().unwrap().native_queue.values.is_empty());
    fs.release(&context(), other).unwrap();
    fs.release(&context(), file.handle).unwrap();
}

#[test]
fn p2_w2_upload_failure_reaches_the_sync_barrier_without_publishing() {
    let (_temp, meta, fs, _runtime, store) = native_pipeline_fixture(false, 8 * 1024 * 1024, 8);
    let file = fs
        .create(
            &context(),
            fs.root_inode(),
            OsStr::new("w2-upload-failure"),
            0o640,
            libc::O_RDWR,
        )
        .unwrap();
    store.fail_next.store(true, Ordering::SeqCst);
    fs.write(&context(), file.handle, 0, &vec![0x31; 4 * 1024 * 1024])
        .unwrap();
    assert!(fs.fsync(&context(), file.handle, SyncMode::Full).is_err());
    assert!(meta.commits.lock().unwrap().is_empty());
    assert_eq!(
        fs.write_resources
            .pipeline
            .as_ref()
            .unwrap()
            .budget
            .snapshot()
            .live_bytes,
        4 * 1024 * 1024
    );
    // A latched background failure may be reported once more before retry;
    // the exact object remains available and a later barrier may succeed.
    for _ in 0..3 {
        if fs.fsync(&context(), file.handle, SyncMode::Full).is_ok() {
            break;
        }
    }
    assert_eq!(meta.commits.lock().unwrap().len(), 1);
    fs.release(&context(), file.handle).unwrap();
}

#[test]
fn p2_w2_preparing_freezes_through_seq_and_busy_timer_preserves_the_later_generation() {
    let (_temp, meta, fs, _runtime, store) = native_pipeline_fixture(true, 8 * 1024 * 1024, 8);
    let fs = Arc::new(fs);
    let file = fs
        .create(
            &context(),
            fs.root_inode(),
            OsStr::new("w2-preparing"),
            0o640,
            libc::O_RDWR,
        )
        .unwrap();
    let second = fs.open(&context(), file.entry.inode, libc::O_RDWR).unwrap();
    let pause = Arc::new(CommitPause::new(0));
    struct ResumeOnDrop(Arc<CommitPause>);
    impl Drop for ResumeOnDrop {
        fn drop(&mut self) {
            self.0.release();
        }
    }
    let _resume = ResumeOnDrop(pause.clone());
    *store.pause.lock().unwrap() = Some(pause.clone());
    fs.write(&context(), file.handle, 0, &vec![0x51; 4 * 1024 * 1024])
        .unwrap();
    let deadline = Instant::now() + Duration::from_secs(5);
    while !pause.was_reached() {
        assert!(Instant::now() < deadline);
        std::thread::sleep(Duration::from_millis(1));
    }
    let timer_fs = fs.clone();
    let timer = std::thread::spawn(move || timer_fs.writeback_pending());
    let cell = fs
        .write_state(&fs.inode_id(file.entry.inode).unwrap())
        .unwrap()
        .unwrap();
    loop {
        if matches!(
            cell.lock().unwrap().in_flight.as_ref(),
            Some(InFlightCommit::Preparing(_))
        ) {
            break;
        }
        assert!(Instant::now() < deadline);
        std::thread::sleep(Duration::from_millis(1));
    }
    assert_eq!(
        fs.write(&context(), second, 4 * 1024 * 1024, b"n").unwrap(),
        1
    );
    let later_seq = cell.lock().unwrap().visible_write_seq;
    assert_eq!(
        fs.writeback_pending().unwrap(),
        0,
        "a busy timer records the later target instead of waiting behind itself"
    );
    {
        let state = cell.lock().unwrap();
        let Some(InFlightCommit::Preparing(frozen)) = state.in_flight.as_ref() else {
            panic!("old preparation unexpectedly changed");
        };
        assert!(frozen.through_seq < later_seq);
        assert_eq!(frozen.logical_length, 4 * 1024 * 1024);
        assert!(state.native_due);
    }
    pause.release();
    assert_eq!(timer.join().unwrap().unwrap(), 1);
    assert_eq!(
        meta.commits.lock().unwrap()[0].file_version.length,
        4 * 1024 * 1024
    );
    assert!(cell.lock().unwrap().native_due);
    assert_eq!(fs.progress_writeback().unwrap(), 1);
    assert_eq!(
        meta.commits.lock().unwrap()[1].file_version.length,
        4 * 1024 * 1024 + 1
    );
    fs.release(&context(), second).unwrap();
    fs.release(&context(), file.handle).unwrap();
}

#[test]
fn p2_w2_empty_generation_truncate_then_extend_fsync_keeps_holes_and_eof() {
    for size in [4096, 4 * 1024 * 1024] {
        let (_temp, meta, fs, _runtime, _store) = native_pipeline_fixture(true, 8 * 1024 * 1024, 8);
        let file = fs
            .create(
                &context(),
                fs.root_inode(),
                OsStr::new("w2-empty-generation"),
                0o640,
                libc::O_RDWR,
            )
            .unwrap();
        fs.write(&context(), file.handle, 0, &vec![0x83; size])
            .unwrap();
        if size == 4 * 1024 * 1024 {
            native_wait_uploads(&fs, 1);
        }
        for size in [0, 8192] {
            fs.setattr(
                &context(),
                file.entry.inode,
                Some(file.handle),
                &AttributeChange {
                    size: Some(size),
                    ..AttributeChange::default()
                },
            )
            .unwrap();
        }
        fs.fsync(&context(), file.handle, SyncMode::Full).unwrap();
        let commits = meta.commits.lock().unwrap();
        assert_eq!(commits.last().unwrap().file_version.length, 8192);
        assert!(commits.last().unwrap().chunk_receipts.is_empty());
        assert!(
            commits
                .last()
                .unwrap()
                .layout_root
                .inline_extents
                .is_empty()
        );
        drop(commits);
        let mut out = vec![0x55; 8192];
        assert_eq!(
            fs.read(&context(), file.handle, 0, &mut out).unwrap(),
            out.len()
        );
        assert!(out.iter().all(|byte| *byte == 0));
        assert_eq!(
            fs.read(&context(), file.handle, 8192, &mut [0x55]).unwrap(),
            0
        );
        fs.release(&context(), file.handle).unwrap();
    }
}

#[test]
fn p2_w2_upload_error_is_owned_by_its_inode_while_drain_still_aggregates() {
    let (_a_temp, a_meta, a_fs, _a_runtime, a_store) =
        native_pipeline_fixture(false, 16 * 1024 * 1024, 8);
    let (_b_temp, b_meta, mut b_fs, _b_runtime, _b_store) =
        native_pipeline_fixture(false, 16 * 1024 * 1024, 8);
    b_meta.inode.lock().unwrap().inode_id = InodeId::new("inode:unrelated-b");
    b_meta.lease.lock().unwrap().inode_id = InodeId::new("inode:unrelated-b");
    b_fs.write_resources.pipeline = a_fs.write_resources.pipeline.clone();
    b_fs.identity.next_operation.store(10_000, Ordering::SeqCst);
    let a = a_fs
        .create(
            &context(),
            a_fs.root_inode(),
            OsStr::new("failure-owner-a"),
            0o640,
            libc::O_RDWR,
        )
        .unwrap();
    let b = b_fs
        .create(
            &context(),
            b_fs.root_inode(),
            OsStr::new("healthy-owner-b"),
            0o640,
            libc::O_RDWR,
        )
        .unwrap();
    let pipeline = a_fs.write_resources.pipeline.as_ref().unwrap();
    let a_cell = a_fs
        .write_state(&a_fs.inode_id(a.entry.inode).unwrap())
        .unwrap()
        .unwrap();
    let b_cell = b_fs
        .write_state(&b_fs.inode_id(b.entry.inode).unwrap())
        .unwrap()
        .unwrap();
    let body = vec![0x64; 4 * 1024 * 1024];
    a_store.fail_next.store(true, Ordering::SeqCst);
    assert_eq!(
        a_fs.write(&context(), a.handle, 0, &body).unwrap(),
        body.len()
    );
    let upload = pipeline.tasks.lock().unwrap()[0].clone();
    let operation = upload.operation_id.clone();
    let chunk = upload.chunk.clone();
    let deadline = Instant::now() + Duration::from_secs(5);
    while !upload
        .state
        .lock()
        .unwrap()
        .task
        .as_ref()
        .unwrap()
        .is_finished()
    {
        assert!(
            Instant::now() < deadline,
            "injected upload failure did not complete"
        );
        std::thread::sleep(Duration::from_millis(1));
    }
    assert!(
        a_cell.lock().unwrap().background_error.is_none(),
        "no caller has polled the completed task yet"
    );
    assert_eq!(
        b_fs.write(&context(), b.handle, 0, b"healthy").unwrap(),
        7,
        "polling another inode's failed upload must not fail an unrelated ordinary write"
    );
    b_fs.fsync(&context(), b.handle, SyncMode::Full).unwrap();
    assert_eq!(b_meta.commits.lock().unwrap().len(), 1);
    assert!(
        b_cell.lock().unwrap().background_error.is_none(),
        "A's failure must never be relabelled as B's error"
    );
    assert!(a_fs.fsync(&context(), a.handle, SyncMode::Full).is_err());
    assert!(
        a_meta.commits.lock().unwrap().is_empty(),
        "the failed owner may not publish"
    );
    {
        let state = upload.state.lock().unwrap();
        assert_eq!(upload.operation_id, operation);
        assert_eq!(upload.chunk, chunk);
        assert!(
            state.body.as_ref().unwrap().as_ref() == body.as_slice(),
            "owner body changed after failed upload"
        );
    }
    // The failure was reported once; retry still uses the same immutable object.
    a_fs.fsync(&context(), a.handle, SyncMode::Full).unwrap();
    {
        let commits = a_meta.commits.lock().unwrap();
        assert_eq!(commits.len(), 1);
        assert_eq!(commits[0].operation_id, operation);
        assert_eq!(commits[0].chunk_receipts[0].chunk, chunk);
    }
    drop(upload);
    // B has its own unpublished tail. Drain must publish B successfully,
    // but retain A's error even if B's preparation would harvest that task.
    b_fs.write(&context(), b.handle, 7, b"!").unwrap();
    a_store.fail_next.store(true, Ordering::SeqCst);
    a_fs.write(&context(), a.handle, body.len() as u64, &body)
        .unwrap();
    let failed = pipeline.tasks.lock().unwrap()[0].clone();
    let deadline = Instant::now() + Duration::from_secs(5);
    while !failed
        .state
        .lock()
        .unwrap()
        .task
        .as_ref()
        .unwrap()
        .is_finished()
    {
        assert!(Instant::now() < deadline, "drain fault did not complete");
        std::thread::sleep(Duration::from_millis(1));
    }
    let error = b_fs.drain().unwrap_err();
    assert!(error.to_string().contains("injected native upload failure"));
    assert!(
        pipeline.tasks.lock().unwrap().is_empty(),
        "failed tasks must still be joined"
    );
    assert!(a_cell.lock().unwrap().background_error.is_some());
    assert!(b_cell.lock().unwrap().background_error.is_none());
    assert_eq!(a_meta.commits.lock().unwrap().len(), 1);
    assert_eq!(b_meta.commits.lock().unwrap().len(), 2);
    assert_eq!(b_meta.commits.lock().unwrap()[1].file_version.length, 8);
}

/// 生产模式下 Node 级的上传轮询可能收割另一个 inode 的上传结果，失败只能归属到它自己的 inode：
/// B 的写入与 fsync 不报告 A 的上传失败；A 的 fsync 报告该失败，重试后提交。
#[test]
fn p2_review_node_wide_poll_never_reports_another_inode_upload_error() {
    // Room for A's retained 4 MiB body plus B: without the Node's background
    // commit loop nothing retries A, so a shared 4 MiB budget would park B.
    let (_a_temp, a_meta, a_fs, _runtime, store) =
        native_pipeline_fixture(false, 8 * 1024 * 1024, 8);
    let (_b_temp, b_meta, mut b_fs) = test_fs();
    b_meta.inode.lock().unwrap().inode_id = InodeId::new("inode:shared-poll-b");
    b_meta.lease.lock().unwrap().inode_id = InodeId::new("inode:shared-poll-b");
    b_fs.write_resources.pipeline = a_fs.write_resources.pipeline.clone();
    b_fs.chunk_store = store.clone();
    b_fs.identity.next_operation.store(10_000, Ordering::SeqCst);
    let a = a_fs
        .create(
            &context(),
            a_fs.root_inode(),
            OsStr::new("shared-poll-a"),
            0o640,
            libc::O_RDWR,
        )
        .unwrap();
    let b = b_fs
        .create(
            &context(),
            b_fs.root_inode(),
            OsStr::new("shared-poll-b"),
            0o640,
            libc::O_RDWR,
        )
        .unwrap();
    let a_id = a_fs.inode_id(a.entry.inode).unwrap();
    let a_cell = a_fs.write_state(&a_id).unwrap().unwrap();
    // The single-inode Meta fixture uses separate facades; share A's real cell
    // as well as the store/pipeline so B's poll scans a Node-wide task table.
    b_fs.writes.insert_cell(a_id, a_cell).unwrap();
    store.fail_next.store(true, Ordering::SeqCst);
    a_fs.write(&context(), a.handle, 0, &vec![0x48; 4 * 1024 * 1024])
        .unwrap();
    let deadline = Instant::now() + Duration::from_secs(5);
    while store.calls.load(Ordering::SeqCst) == 0 {
        assert!(
            Instant::now() < deadline,
            "A's full chunk upload never started"
        );
        a_fs.progress_writeback().unwrap();
        std::thread::sleep(Duration::from_millis(1));
    }
    assert_eq!(b_fs.write(&context(), b.handle, 0, b"b").unwrap(), 1);
    b_fs.fsync(&context(), b.handle, SyncMode::Full).unwrap();
    assert_eq!(b_meta.commits.lock().unwrap().len(), 1);
    assert!(a_meta.commits.lock().unwrap().is_empty());
    // R56: the failure is reported to A's handle exactly once and never reaches B.
    let mut a_failures = 0;
    while let Err(error) = a_fs.fsync(&context(), a.handle, SyncMode::Full) {
        assert!(
            error.to_string().contains("injected native upload failure"),
            "{error}"
        );
        a_failures += 1;
    }
    assert_eq!(a_failures, 1);
    assert_eq!(a_meta.commits.lock().unwrap().len(), 1);
}

#[test]
fn p2_review_structural_install_failure_cleans_join_without_retiring_body() {
    let (_temp, meta, fs, _runtime, store) = native_pipeline_fixture(false, 8 * 1024 * 1024, 8);
    let file = fs
        .create(
            &context(),
            fs.root_inode(),
            OsStr::new("structural-install"),
            0o640,
            libc::O_RDWR,
        )
        .unwrap();
    fs.write(&context(), file.handle, 0, &vec![0x72; 4 * 1024 * 1024])
        .unwrap();
    let pipeline = fs.write_resources.pipeline.as_ref().unwrap().clone();
    let upload = pipeline.tasks.lock().unwrap()[0].clone();
    let operation = upload.operation_id.clone();
    let chunk = upload.chunk.clone();
    let deadline = Instant::now() + Duration::from_secs(5);
    while !upload
        .state
        .lock()
        .unwrap()
        .task
        .as_ref()
        .unwrap()
        .is_finished()
    {
        assert!(
            Instant::now() < deadline,
            "structural fixture upload did not finish"
        );
        std::thread::sleep(Duration::from_millis(1));
    }
    let reserved_before = pipeline
        .metadata
        .state
        .lock()
        .unwrap()
        .descriptor_reserved_bytes;
    // A real installer outer error: capacity rejection would be an inner
    // business error and cannot prove task-table cleanup on this branch.
    let poisoned = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
        let _guard = upload._descriptor.lock().unwrap();
        panic!("intentional descriptor poison for terminal cleanup regression");
    }));
    assert!(poisoned.is_err());
    let error = fs.poll_uploads().unwrap_err();
    assert!(
        error
            .to_string()
            .contains("native descriptor reservation is poisoned")
    );
    assert!(
        pipeline.tasks.lock().unwrap().is_empty(),
        "a consumed JoinHandle must not leave an unjoinable task slot"
    );
    let cell = fs
        .write_state(&fs.inode_id(file.entry.inode).unwrap())
        .unwrap()
        .unwrap();
    assert!(
        cell.lock().unwrap().terminal_error.is_some(),
        "the damaged owner must become terminal rather than retry as healthy"
    );
    {
        let state = upload.state.lock().unwrap();
        assert!(!state.joining);
        assert!(state.task.is_none());
        assert_eq!(upload.operation_id, operation);
        assert_eq!(upload.chunk, chunk);
        assert_eq!(state.body.as_ref().unwrap().len(), 4 * 1024 * 1024);
        assert_eq!(
            state.receipt.as_ref().unwrap().receipt.chunk,
            chunk,
            "preserve the successful durable receipt for terminal diagnostics"
        );
    }
    assert_eq!(
        pipeline.budget.snapshot().live_bytes,
        4 * 1024 * 1024,
        "a terminal error is not permission to retire a still-owned body"
    );
    let mut output = [0xa5; 16];
    assert!(upload.read_at(store.as_ref(), 0, &mut output).is_err());
    assert_eq!(
        output, [0xa5; 16],
        "terminal diagnostic receipt must not enable a read"
    );
    assert!(fs.start_upload(&upload).is_err());
    let before = Instant::now();
    for _ in 0..2 {
        assert!(fs.wait_upload(&upload).is_err());
    }
    assert!(fs.drain().is_err());
    assert!(before.elapsed() < Duration::from_secs(5));
    assert_eq!(
        store.calls.load(Ordering::SeqCst),
        1,
        "terminal state must not retry through a poisoned descriptor"
    );
    assert!(meta.commits.lock().unwrap().is_empty());
    assert_eq!(
        pipeline
            .metadata
            .state
            .lock()
            .unwrap()
            .descriptor_reserved_bytes,
        reserved_before,
        "a poisoned installer must retain the original reservation"
    );
    drop(upload);
    drop(fs);
    assert_eq!(
        pipeline.budget.snapshot().live_bytes,
        4 * 1024 * 1024,
        "the explicit inode view still owns the dirty body after its facade drops"
    );
    drop(cell);
    assert_eq!(
        pipeline.budget.snapshot().live_bytes,
        0,
        "only the final body owner releases its charge"
    );
    let metadata = pipeline.metadata.state.lock().unwrap();
    assert_eq!(metadata.descriptors, 0);
    assert_eq!(metadata.descriptor_reserved_bytes, 0);
}

// W-2 changes version publication, not merely a dirty-budget constant.
// This uses only existing interfaces so the pre-W2 candidate must compile
// before the old 16MiB automatic publication is observed as a behavior RED.
#[test]
fn p2_w2_native_ordinary_write_does_not_publish_at_the_old_sixteen_mib_threshold() {
    let (_temp, meta, fs) = test_fs();
    let created = fs
        .create(
            &context(),
            fs.root_inode(),
            OsStr::new("w2-no-old-threshold.bin"),
            0o640,
            libc::O_RDWR,
        )
        .unwrap();
    let body = vec![0x39; 1024 * 1024];
    let before = meta.commits.lock().unwrap().len();
    for index in 0..16_u64 {
        assert_eq!(
            fs.write(&context(), created.handle, index * body.len() as u64, &body)
                .unwrap(),
            body.len()
        );
    }
    assert_eq!(
        meta.commits.lock().unwrap().len(),
        before,
        "ordinary native writes below W2 descriptor/live pressure must not publish at the old 16MiB threshold"
    );
    fs.fsync(&context(), created.handle, SyncMode::Full)
        .unwrap();
    assert_eq!(meta.commits.lock().unwrap().len(), before + 1);
    let reader = fs
        .open(&context(), created.entry.inode, libc::O_RDONLY)
        .unwrap();
    let mut output = vec![0; body.len()];
    assert_eq!(
        fs.read(&context(), reader, 15 * body.len() as u64, &mut output)
            .unwrap(),
        body.len()
    );
    assert_eq!(output, body);
    let mut eof = [0; 1];
    assert_eq!(
        fs.read(&context(), reader, 16 * body.len() as u64, &mut eof)
            .unwrap(),
        0
    );
    fs.release(&context(), reader).unwrap();
    fs.release(&context(), created.handle).unwrap();
}

// Existing buffer ownership must remain true before a native quota is
// attached at that same allocation owner. This is a guard, not a RED claim.
#[test]
fn p2_w2_existing_shared_slices_hold_the_final_native_allocation_owner() {
    let pool = WriteBufferPool::new(0);
    let mut builder = ChunkBuilder::with_pool(pool.clone(), 4096).unwrap();
    builder.write_at(0, &[7; 4096]).unwrap();
    let pointer = builder.data_ptr();
    let capacity = builder.capacity();
    let body = builder.freeze();
    assert_eq!(body.as_ptr(), pointer);
    let tail = body.slice(4095..4096);
    let staged = StagedChunk::from_shared(OperationId::new("w2-owner-guard"), body.clone());
    drop(body);
    drop(staged);
    assert_eq!(pool.snapshot().live_bytes, capacity);
    assert_eq!(tail.as_ref(), &[7]);
    drop(tail);
    assert_eq!(pool.snapshot().live_bytes, 0);
}

#[test]
fn native_builder_uncommitted_overwrite_is_in_place_and_counts_admission() {
    let pool = WriteBufferPool::new(0);
    let mut dirty = DirtyExtentMap::default();
    dirty
        .write_native_at(0, &vec![3; 4 * 1024 * 1024], 1, &pool, 0)
        .unwrap();
    let pointer = dirty.builders[0].builder.data_ptr();
    dirty
        .write_native_at(8192, &[7; 4096], 2, &pool, 4 * 1024 * 1024)
        .unwrap();
    assert_eq!(dirty.builders.len(), 1);
    assert_eq!(dirty.builders[0].builder.data_ptr(), pointer);
    assert_eq!(
        &dirty.builders[0].builder.as_bytes()[8192..12288],
        &[7; 4096]
    );
    assert_eq!(dirty.dirty_data_bytes(), 4 * 1024 * 1024 + 4096);
}

#[test]
fn native_builder_committed_patch_is_small_and_truncate_extension_is_a_hole() {
    let pool = WriteBufferPool::new(0);
    let mut patch = DirtyExtentMap::default();
    patch
        .write_native_at(8192, &[9; 4096], 1, &pool, 6 * 1024 * 1024)
        .unwrap();
    assert_eq!(patch.builders[0].builder.capacity(), 4096);
    assert_eq!(patch.builders[0].builder.len(), 4096);
    let mut dirty = DirtyExtentMap::default();
    dirty.write_native_at(0, &[5; 16384], 1, &pool, 0).unwrap();
    dirty.resize(16384, 8192, 2).unwrap();
    dirty.resize(8192, 16384, 3).unwrap();
    let snapshot = dirty.snapshot_range(0, 16384).unwrap();
    let mut out = vec![99; 16384];
    snapshot.overlay(0, &mut out).unwrap();
    assert_eq!(&out[..8192], &[5; 8192]);
    assert_eq!(&out[8192..], &[0; 8192]);
    dirty.freeze_builders();
    assert_eq!(
        dirty
            .extents
            .first_key_value()
            .unwrap()
            .1
            .data
            .as_ref()
            .unwrap()
            .len(),
        8192
    );
}

#[test]
fn native_builder_allocation_failure_retains_old_body_and_allows_short_write() {
    let pool = WriteBufferPool::new(0);
    let mut dirty = DirtyExtentMap::default();
    dirty
        .write_native_at(0, &[1; 4096], 1, &pool, 8192)
        .unwrap();
    pool.fail_next_allocation();
    let error = dirty
        .write_native_at(8192, &[2; 4096], 2, &pool, 16384)
        .unwrap_err();
    assert_eq!(error.code(), afs_error::IO_OUT_OF_MEMORY);
    assert_eq!(dirty.dirty_data_bytes(), 4096);
    assert_eq!(dirty.builders[0].builder.as_bytes(), &[1; 4096]);
    // The first 4KiB overwrites an existing owner; the second needs an allocation.
    pool.fail_next_allocation();
    assert_eq!(
        dirty
            .write_native_at(0, &[3; 8192], 3, &pool, 16384)
            .unwrap(),
        4096
    );
    assert_eq!(dirty.dirty_data_bytes(), 8192);
    assert_eq!(dirty.builders[0].builder.as_bytes(), &[3; 4096]);
}

#[cfg(feature = "dfs")]
#[test]
fn native_builder_commit_planner_keeps_two_bodies_and_small_followup_patch() {
    let (_temp, meta, fs) = test_fs();
    let pool = WriteBufferPool::new(16 * 1024 * 1024);
    let fs = fs.with_write_buffer_pool(pool.clone());
    let created = fs
        .create(
            &context(),
            fs.root_inode(),
            OsStr::new("native-bodies.bin"),
            0o640,
            libc::O_RDWR,
        )
        .unwrap();
    let mut expected = Vec::new();
    for index in 0..6u64 {
        let bytes = vec![index as u8 + 1; 1024 * 1024];
        fs.write(&context(), created.handle, index * 1024 * 1024, &bytes)
            .unwrap();
        expected.extend_from_slice(&bytes);
    }
    assert_eq!(meta.commit_count(), 0);
    assert_eq!(pool.snapshot().copied_bytes, expected.len() as u64);
    assert_eq!(pool.snapshot().allocations, 2);
    fs.fsync(&context(), created.handle, SyncMode::Full)
        .unwrap();
    let first = meta.commits.lock().unwrap()[0].clone();
    assert_eq!(first.layout_root.inline_extents.len(), 2);
    assert_eq!(first.layout_root.inline_extents[0].length, 4 * 1024 * 1024);
    assert_eq!(first.layout_root.inline_extents[1].length, 2 * 1024 * 1024);
    assert_eq!(pool.snapshot().copied_bytes, expected.len() as u64);
    fs.write(&context(), created.handle, 8192, &[17; 4096])
        .unwrap();
    fs.fsync(&context(), created.handle, SyncMode::Full)
        .unwrap();
    expected[8192..12288].fill(17);
    let second = meta.commits.lock().unwrap()[1].clone();
    assert_eq!(second.layout_root.inline_extents.len(), 4);
    assert_eq!(second.layout_root.inline_extents[1].length, 4096);
    assert_eq!(
        second.layout_root.inline_extents[0].chunk_id,
        first.layout_root.inline_extents[0].chunk_id
    );
    assert_eq!(
        second.layout_root.inline_extents[2].chunk_id,
        first.layout_root.inline_extents[0].chunk_id
    );
    let mut actual = vec![0; expected.len()];
    assert_eq!(
        fs.read(&context(), created.handle, 0, &mut actual).unwrap(),
        expected.len()
    );
    assert_eq!(actual, expected);
    assert_eq!(pool.snapshot().copied_bytes, expected.len() as u64 + 4096);
}

#[cfg(feature = "dfs")]
#[test]
fn queued_write_capability_requires_native_local_ordinary_writable_handle() {
    let (_temp, _meta, fs) = test_fs();
    let created = fs
        .create(
            &context(),
            fs.root_inode(),
            OsStr::new("queue-capability"),
            0o640,
            libc::O_RDWR,
        )
        .unwrap();
    assert!(fs.accepts_queued_writes(created.handle));
    for flag in [libc::O_APPEND, libc::O_SYNC, libc::O_DSYNC, libc::O_RDONLY] {
        let flags = if flag == libc::O_RDONLY {
            flag
        } else {
            libc::O_RDWR | flag
        };
        let handle = fs.open(&context(), created.entry.inode, flags).unwrap();
        assert!(!fs.accepts_queued_writes(handle), "flags {flags}");
        fs.release(&context(), handle).unwrap();
    }
    assert!(!fs.accepts_queued_writes(FileHandle(u64::MAX)));
    assert!(fs.getxattr_is_fixed_local(OsStr::new("security.capability")));
    assert!(!fs.getxattr_is_fixed_local(OsStr::new("user.capability")));
    fs.release(&context(), created.handle).unwrap();
}

#[test]
fn native_builder_vfs_short_write_does_not_extend_unaccepted_tail() {
    for sync_flag in [0, libc::O_SYNC, libc::O_DSYNC] {
        let (_temp, meta, fs) = test_fs();
        let pool = WriteBufferPool::new(0);
        let fs = fs.with_write_buffer_pool(pool.clone());
        let created = fs
            .create(
                &context(),
                fs.root_inode(),
                OsStr::new("native-short.bin"),
                0o640,
                libc::O_RDWR,
            )
            .unwrap();
        let prefix = COMMIT_CHUNK_BYTES as usize - 4096;
        assert_eq!(
            fs.write(&context(), created.handle, 0, &vec![5; prefix])
                .unwrap(),
            prefix
        );
        let writer = fs
            .open(&context(), created.entry.inode, libc::O_RDWR | sync_flag)
            .unwrap();
        pool.fail_next_allocation();
        assert_eq!(
            fs.write(&context(), writer, prefix as u64, &[7; 8192])
                .unwrap(),
            4096
        );
        assert_eq!(
            fs.getattr(&context(), created.entry.inode, Some(writer))
                .unwrap()
                .size,
            COMMIT_CHUNK_BYTES
        );
        assert_eq!(
            meta.commit_count(),
            usize::from(sync_flag != 0),
            "accepted short prefix follows the handle's original sync flag"
        );
        fs.fsync(&context(), writer, SyncMode::Full).unwrap();
        assert_eq!(
            meta.commits.lock().unwrap()[0].file_version.length,
            COMMIT_CHUNK_BYTES
        );
        let mut tail = [0; 8192];
        assert_eq!(
            fs.read(&context(), writer, prefix as u64, &mut tail)
                .unwrap(),
            4096
        );
        assert_eq!(&tail[..4096], &[7; 4096]);
        assert_eq!(
            fs.read(&context(), writer, COMMIT_CHUNK_BYTES, &mut tail)
                .unwrap(),
            0
        );
        assert_eq!(pool.snapshot().copied_bytes, COMMIT_CHUNK_BYTES);
    }
}

/// Inode write-state locks taken by Node-wide writeback passes (the notify
/// pass and the timer pass) beside `idle` retained write states: inodes this
/// Node opened for write earlier, closed and published (or never wrote).
/// Returns the locks taken while one writer has pending data, and the locks
/// taken once that writer, too, is closed and published.
fn writeback_pass_write_state_locks(idle: usize) -> (usize, usize) {
    let (_temp, meta, fs, _runtime, _store) = native_pipeline_fixture(true, 8 * 1024 * 1024, 8);
    for index in 0..idle {
        // The single-inode Meta fixture cannot create more files; install the
        // write state an earlier open/close of another inode leaves behind.
        let mut inode = meta.inode.lock().unwrap().clone();
        inode.inode_id = InodeId::new(format!("inode:idle-{index}"));
        let mut lease = meta.lease.lock().unwrap().clone();
        lease.inode_id = inode.inode_id.clone();
        fs.install_write_state(inode, lease).unwrap();
    }
    let active = fs
        .create(
            &context(),
            fs.root_inode(),
            OsStr::new("active"),
            0o640,
            libc::O_RDWR,
        )
        .unwrap();
    fs.write(&context(), active.handle, 0, b"pending").unwrap();
    // One settling pass, then the measured passes.
    fs.progress_writeback().unwrap();
    DFS_WRITE_STATE_LOCKS.with(|count| count.set(0));
    fs.progress_writeback().unwrap();
    fs.writeback_pending().unwrap();
    let with_writer = DFS_WRITE_STATE_LOCKS.with(std::cell::Cell::get);
    assert_eq!(meta.commits.lock().unwrap().len(), 1);
    fs.release(&context(), active.handle).unwrap();
    fs.writeback_pending().unwrap();
    fs.progress_writeback().unwrap();
    DFS_WRITE_STATE_LOCKS.with(|count| count.set(0));
    fs.progress_writeback().unwrap();
    fs.writeback_pending().unwrap();
    let all_idle = DFS_WRITE_STATE_LOCKS.with(std::cell::Cell::get);
    // Retention of closed write states is unchanged: they stay for reopen.
    assert_eq!(fs.writes.states.len().unwrap(), idle + 1);
    (with_writer, all_idle)
}

#[test]
fn writeback_pass_cost_does_not_grow_with_idle_retained_write_states() {
    let (few_with_writer, few_idle) = writeback_pass_write_state_locks(4);
    let (many_with_writer, many_idle) = writeback_pass_write_state_locks(256);
    assert_eq!(
        few_with_writer, many_with_writer,
        "a writeback pass must examine only inodes with pending write work, \
         not every closed and published inode this Node has written"
    );
    assert_eq!(
        (few_idle, many_idle),
        (0, 0),
        "with no pending write work a writeback pass examines no inode"
    );
}

#[test]
fn upload_completion_wakes_its_waiting_barrier_without_a_background_pass() {
    let (_temp, _meta, fs, _runtime, store) = native_pipeline_fixture(false, 8 * 1024 * 1024, 8);
    let pause = Arc::new(CommitPause::new(0));
    *store.pause.lock().unwrap() = Some(pause.clone());
    let file = fs
        .create(
            &context(),
            fs.root_inode(),
            OsStr::new("upload-wakes-waiter"),
            0o640,
            libc::O_RDWR,
        )
        .unwrap();
    fs.write(&context(), file.handle, 0, &vec![0x31; 4 * 1024 * 1024])
        .unwrap();
    pause.wait_until_reached();
    let pipeline = fs.write_resources.pipeline.as_ref().unwrap().clone();
    let upload = pipeline.tasks.lock().unwrap()[0].clone();
    let waiting = upload.clone();
    let waiter = std::thread::spawn(move || {
        let state = waiting.state.lock().unwrap();
        let (state, timeout) = waiting
            .cv
            .wait_timeout_while(state, Duration::from_secs(5), |state| {
                state.finished.is_none()
            })
            .unwrap();
        !timeout.timed_out() && state.finished.is_some()
    });
    std::thread::sleep(Duration::from_millis(20));
    pause.release();
    assert!(
        waiter.join().unwrap(),
        "the upload worker must hand its result to a waiting barrier directly"
    );
    // The barrier harvests the result itself; no writeback pass ran.
    let receipt = fs.wait_upload(&upload).unwrap();
    assert_eq!(receipt.chunk, upload.chunk);
    assert!(upload.state.lock().unwrap().task.is_none());
    assert!(pipeline.tasks.lock().unwrap().is_empty());
}

#[test]
fn writeback_readiness_is_true_only_when_a_pass_can_act() {
    let (_temp, _meta, fs, _runtime, store) = native_pipeline_fixture(false, 8 * 1024 * 1024, 8);
    assert!(
        !fs.writeback_ready().unwrap(),
        "an idle pipeline has no work"
    );
    let file = fs
        .create(
            &context(),
            fs.root_inode(),
            OsStr::new("readiness"),
            0o640,
            libc::O_RDWR,
        )
        .unwrap();
    fs.write(&context(), file.handle, 0, &[0x11; 64 * 1024])
        .unwrap();
    assert!(
        !fs.writeback_ready().unwrap(),
        "an open builder below a chunk is not background work"
    );

    let pause = Arc::new(CommitPause::new(0));
    *store.pause.lock().unwrap() = Some(pause.clone());
    fs.write(&context(), file.handle, 0, &vec![0x22; 4 * 1024 * 1024])
        .unwrap();
    pause.wait_until_reached();
    assert!(
        !fs.writeback_ready().unwrap(),
        "a running upload has nothing to harvest yet"
    );
    pause.release();
    let pipeline = fs.write_resources.pipeline.as_ref().unwrap().clone();
    let upload = pipeline.tasks.lock().unwrap()[0].clone();
    let deadline = Instant::now() + Duration::from_secs(5);
    while upload.state.lock().unwrap().finished.is_none() {
        assert!(Instant::now() < deadline, "fixture upload did not finish");
        std::thread::sleep(Duration::from_millis(1));
    }
    assert!(
        fs.writeback_ready().unwrap(),
        "a finished upload must be harvested"
    );
    fs.progress_writeback().unwrap();
    assert!(!fs.writeback_ready().unwrap());

    // The last writer leaving dirty data asks for a background commit.
    fs.release(&context(), file.handle).unwrap();
    assert!(
        fs.writeback_ready().unwrap(),
        "a requested background commit is work"
    );
    assert_eq!(fs.progress_writeback().unwrap(), 1);
    assert!(!fs.writeback_ready().unwrap());
}

/// Meta stand-in for performance batch 1 (D): `retain_fenced` can be held
/// closed or scripted to fail; `collect` plans like Meta (`plan_bounded`): an
/// inventoried chunk is reclaimed unless an admitted, unreleased `write:`
/// reference names its write generation.
#[derive(Default)]
struct GatedAdmissionMeta {
    open: Mutex<bool>,
    opened: std::sync::Condvar,
    /// Wire ids of admission attempts, in order.
    attempts: Mutex<Vec<String>>,
    /// Errors returned by the next attempts, front first.
    failures: Mutex<std::collections::VecDeque<Error>>,
    /// Stored generation ids of admitted, unreleased `write:` references.
    admitted: Mutex<HashSet<String>>,
    released: Mutex<Vec<String>>,
    reclaimed: Mutex<Vec<crate::dfs::ChunkId>>,
}

impl GatedAdmissionMeta {
    fn open_gate(&self) {
        *self.open.lock().unwrap() = true;
        self.opened.notify_all();
    }
}

fn stored_generation(id: &str) -> Option<String> {
    id.strip_prefix("write:")
        .map(|operation| ChunkObject::generation_id(&crate::dfs::OperationId::new(operation)))
}

impl crate::node::gc::GcControl for GatedAdmissionMeta {
    fn retain(&self, _: &str, _: &InodeId, _: Option<&FileVersionId>) -> Result<()> {
        Ok(())
    }
    fn retain_fenced(
        &self,
        id: &str,
        _: &InodeId,
        _: Option<&FileVersionId>,
    ) -> Result<crate::dfs::GcFence> {
        let mut open = self.open.lock().unwrap();
        while !*open {
            open = self.opened.wait(open).unwrap();
        }
        drop(open);
        self.attempts.lock().unwrap().push(id.into());
        if let Some(error) = self.failures.lock().unwrap().pop_front() {
            return Err(error);
        }
        self.admitted.lock().unwrap().extend(stored_generation(id));
        Ok(crate::dfs::GcFence::WriteGeneration)
    }
    fn release(&self, ids: &[String]) -> Result<()> {
        let mut admitted = self.admitted.lock().unwrap();
        for id in ids {
            if let Some(generation) = stored_generation(id) {
                admitted.remove(&generation);
            }
        }
        self.released.lock().unwrap().extend_from_slice(ids);
        Ok(())
    }
    fn collect(&self, completed: u64, inventory: Vec<ChunkObject>) -> Result<crate::dfs::GcState> {
        if inventory.is_empty() {
            return Ok(crate::dfs::GcState {
                generation: completed,
                ..Default::default()
            });
        }
        let admitted = self.admitted.lock().unwrap();
        let chunks = inventory
            .into_iter()
            .filter(|chunk| {
                !chunk
                    .write_generation()
                    .is_some_and(|generation| admitted.contains(generation))
            })
            .map(|chunk| chunk.id)
            .collect::<Vec<_>>();
        self.reclaimed
            .lock()
            .unwrap()
            .extend(chunks.iter().cloned());
        Ok(crate::dfs::GcState {
            generation: completed + 1,
            waiting_nodes: if chunks.is_empty() {
                Vec::new()
            } else {
                vec!["node-a".into()]
            },
            chunks,
        })
    }
}

fn gated_collector(
    control: Arc<GatedAdmissionMeta>,
    store: &Arc<NativeFixtureStore>,
) -> crate::node::gc::Collector {
    let engine = Arc::new(crate::node::read::engine::DfsReadEngine::new(
        crate::dfs::NamespaceId::new("default"),
        "node-a".into(),
        store.local.clone(),
        Arc::new(crate::node::read::engine::UnimplementedReadSourceProvider),
        Arc::new(crate::node::read::engine::UnimplementedChunkTransfer),
        crate::node::read::engine::DfsReadConfig::default(),
    ));
    crate::node::gc::Collector::new(
        crate::node::gc::GcReferences::new(control),
        store.local.clone(),
        engine,
        "node-a".into(),
    )
}

/// D (GC safety): the first write only starts the generation's admission;
/// until Meta admits it no chunk of the generation exists anywhere GC can
/// see, so a collection pass in that window reclaims nothing. Afterwards the
/// upload, the commit and the read-back all succeed.
#[test]
fn deferred_admission_keeps_every_chunk_out_of_gc_until_admitted() {
    let (_temp, _meta, fs, _runtime, store) = native_pipeline_fixture(true, 8 * 1024 * 1024, 8);
    let control = Arc::new(GatedAdmissionMeta::default());
    let references = crate::node::gc::GcReferences::new(control.clone());
    let fs = Arc::new(fs.with_gc_references(Some(references.clone())));
    let file = fs
        .create(
            &context(),
            fs.root_inode(),
            OsStr::new("gated"),
            0o640,
            libc::O_RDWR,
        )
        .unwrap();
    // A full 4 MiB builder seals and starts its upload during the write.
    let body = (0..4 * 1024 * 1024)
        .map(|index| (index % 251) as u8)
        .collect::<Vec<_>>();
    assert_eq!(
        fs.write(&context(), file.handle, 0, &body).unwrap(),
        body.len(),
        "the write thread must not wait for the admission"
    );
    let syncing = {
        let fs = fs.clone();
        std::thread::spawn(move || fs.fsync(&context(), file.handle, SyncMode::Full))
    };
    std::thread::sleep(Duration::from_millis(300));
    assert!(control.attempts.lock().unwrap().is_empty());
    assert!(
        store.local.gc_inventory().unwrap().is_empty(),
        "no chunk may be persisted before its write reference is admitted"
    );
    let collector = gated_collector(control.clone(), &store);
    assert_eq!(collector.collect_once().unwrap(), 0);
    assert!(control.reclaimed.lock().unwrap().is_empty());
    assert!(!syncing.is_finished(), "fsync waits for the admission");

    control.open_gate();
    syncing.join().unwrap().unwrap();
    let inventory = store.local.gc_inventory().unwrap();
    assert!(!inventory.is_empty());
    // The admitted, still pinned generation protects what it uploaded.
    assert_eq!(collector.collect_once().unwrap(), 0);
    assert!(control.reclaimed.lock().unwrap().is_empty());
    let mut actual = vec![0; body.len()];
    assert_eq!(
        fs.read(&context(), file.handle, 0, &mut actual).unwrap(),
        body.len()
    );
    assert_eq!(actual, body);
    let attempts = control.attempts.lock().unwrap().clone();
    assert_eq!(attempts.len(), 1);
    assert!(attempts[0].starts_with("write:"));
}

/// D: a failed admission Meta may still accept is retried once with the
/// same reference id by the next waiter; the write publishes normally.
#[test]
fn transient_admission_failure_is_retried_with_the_same_reference_id() {
    let (_temp, meta, fs, _runtime, _store) = native_pipeline_fixture(true, 8 * 1024 * 1024, 8);
    let control = Arc::new(GatedAdmissionMeta::default());
    control.open_gate();
    control
        .failures
        .lock()
        .unwrap()
        .push_back(Error::coded(afs_error::IO_UNAVAILABLE, "lost reply"));
    let references = crate::node::gc::GcReferences::new(control.clone());
    let fs = fs.with_gc_references(Some(references));
    let file = fs
        .create(
            &context(),
            fs.root_inode(),
            OsStr::new("retry"),
            0o640,
            libc::O_RDWR,
        )
        .unwrap();
    fs.write(&context(), file.handle, 0, b"retry me").unwrap();
    fs.fsync(&context(), file.handle, SyncMode::Full).unwrap();
    let attempts = control.attempts.lock().unwrap().clone();
    assert_eq!(attempts.len(), 2);
    assert_eq!(attempts[0], attempts[1], "a retry reuses the reference id");
    assert_eq!(meta.commits.lock().unwrap().len(), 1);
}

/// D: Meta refuses the admission for good (deadline passed). The accepted
/// bytes are never persisted and never reported as durable: the refusal
/// reaches the writer at fsync or at close (flush), and the next open for
/// write discards the generation, queuing its reference for release.
#[test]
fn permanent_admission_failure_rejects_the_generation_and_releases_its_pin() {
    let (_temp, meta, fs, _runtime, store) = native_pipeline_fixture(true, 8 * 1024 * 1024, 8);
    let control = Arc::new(GatedAdmissionMeta::default());
    control.open_gate();
    for _ in 0..32 {
        control.failures.lock().unwrap().push_back(Error::coded(
            afs_error::META_DFS_REFERENCE_ADMISSION_EXPIRED,
            "reference admission deadline expired",
        ));
    }
    let references = crate::node::gc::GcReferences::new(control.clone());
    let fs = fs.with_gc_references(Some(references.clone()));
    for (name, via_fsync) in [("expired-fsync", true), ("expired-close", false)] {
        control.attempts.lock().unwrap().clear();
        let file = fs
            .create(
                &context(),
                fs.root_inode(),
                OsStr::new(name),
                0o640,
                libc::O_RDWR,
            )
            .unwrap();
        fs.write(&context(), file.handle, 0, &vec![5; 4 * 1024 * 1024])
            .unwrap();
        let error = if via_fsync {
            let error = fs
                .fsync(&context(), file.handle, SyncMode::Full)
                .unwrap_err();
            let _ = fs.release(&context(), file.handle);
            error
        } else {
            // close(2): FUSE FLUSH, then RELEASE.
            let error = fs.flush(&context(), file.handle).unwrap_err();
            let _ = fs.release(&context(), file.handle);
            error
        };
        assert_eq!(
            error.code(),
            afs_error::META_DFS_REFERENCE_ADMISSION_EXPIRED,
            "{name}"
        );
        assert!(store.local.gc_inventory().unwrap().is_empty(), "{name}");
        assert!(meta.commits.lock().unwrap().is_empty(), "{name}");
        let attempts = control.attempts.lock().unwrap().clone();
        assert!(!attempts.is_empty() && attempts.iter().all(|id| id == &attempts[0]));
        let reopened = fs.open(&context(), file.entry.inode, libc::O_RDWR).unwrap();
        // The background pass harvests the last failed upload task, which
        // holds the generation pin too.
        let deadline = Instant::now() + Duration::from_secs(5);
        while !control.released.lock().unwrap().contains(&attempts[0]) && Instant::now() < deadline
        {
            // The pass also reports the harvested upload's error Node-wide.
            let _ = fs.progress_writeback();
            references.flush().unwrap();
            std::thread::sleep(Duration::from_millis(10));
        }
        assert!(
            control.released.lock().unwrap().contains(&attempts[0]),
            "{name}: the abandoned generation's reference is queued for release"
        );
        fs.release(&context(), reopened)
            .unwrap_or_else(|error| panic!("{name}: fresh writer saw {error}"));
    }
}

fn r91_open_dirty(fs: &DistributedFs, name: &str) -> (FileHandle, InodeId) {
    let file = fs
        .create(
            &context(),
            fs.root_inode(),
            OsStr::new(name),
            0o640,
            libc::O_RDWR,
        )
        .unwrap();
    // A write at the end of the file reserves a whole 4 MiB builder.
    fs.write(&context(), file.handle, 0, b"x").unwrap();
    (file.handle, fs.inode_id(file.entry.inode).unwrap())
}

/// R91: a write waiting for dirty budget must not wait for the periodic
/// commit. The oldest file holding budget is committed early, which frees its
/// builder; younger holders keep theirs.
#[test]
fn a_write_waiting_for_dirty_budget_commits_the_oldest_holder_early() {
    // Budget for exactly two open dirty files.
    let (_temp, meta, fs, _runtime, _store) = native_pipeline_fixture(true, 8 * 1024 * 1024, 8);
    *meta.files.lock().unwrap() = Some(HashMap::new());
    let fs = Arc::new(fs.with_write_timeout(Duration::from_secs(20)));
    let (oldest, oldest_inode) = r91_open_dirty(&fs, "r91-oldest");
    let (younger, younger_inode) = r91_open_dirty(&fs, "r91-younger");
    let started = Instant::now();
    // No timer pass runs in this test: nothing calls `writeback_pending`.
    let (third, _) = r91_open_dirty(&fs, "r91-third");
    assert!(
        started.elapsed() < Duration::from_secs(5),
        "the third file's write waited {:?} for budget",
        started.elapsed()
    );
    let committed = meta
        .commits
        .lock()
        .unwrap()
        .iter()
        .map(|commit| commit.inode_id.clone())
        .collect::<Vec<_>>();
    assert_eq!(
        committed,
        vec![oldest_inode.clone()],
        "only the oldest budget holder is committed early"
    );
    let dirty = |inode: &InodeId| {
        fs.write_state(inode)
            .unwrap()
            .unwrap()
            .lock()
            .unwrap()
            .dirty
    };
    assert!(!dirty(&oldest_inode));
    assert!(dirty(&younger_inode), "a younger holder keeps its builder");
    for handle in [oldest, younger, third] {
        fs.fsync(&context(), handle, SyncMode::Full).unwrap();
        fs.release(&context(), handle).unwrap();
    }
}

/// R91: the early commit is asked for once per wait, not once per holder:
/// with room for three files, the fourth and fifth each evict one.
#[test]
fn each_budget_wait_commits_only_as_many_holders_as_it_needs() {
    let (_temp, meta, fs, _runtime, _store) = native_pipeline_fixture(true, 12 * 1024 * 1024, 8);
    *meta.files.lock().unwrap() = Some(HashMap::new());
    let fs = Arc::new(fs.with_write_timeout(Duration::from_secs(20)));
    let mut handles = Vec::new();
    let mut inodes = Vec::new();
    for index in 0..5 {
        let (handle, inode) = r91_open_dirty(&fs, &format!("r91-many-{index}"));
        handles.push(handle);
        inodes.push(inode);
    }
    let committed = meta
        .commits
        .lock()
        .unwrap()
        .iter()
        .map(|commit| commit.inode_id.clone())
        .collect::<Vec<_>>();
    assert_eq!(
        committed,
        inodes[..2].to_vec(),
        "oldest first, one per wait"
    );
    for handle in handles {
        fs.release(&context(), handle).unwrap();
    }
}

#[test]
fn a_holder_synced_during_selection_does_not_block_later_budget_relief() {
    let (_temp, meta, fs, _runtime, _store) = native_pipeline_fixture(true, 8 * 1024 * 1024, 8);
    *meta.files.lock().unwrap() = Some(HashMap::new());
    let fs = Arc::new(fs.with_write_timeout(Duration::from_secs(20)));
    let (oldest, oldest_inode) = r91_open_dirty(&fs, "r91-synced");
    let (younger, younger_inode) = r91_open_dirty(&fs, "r91-still-dirty");
    let oldest_cell = fs.write_state(&oldest_inode).unwrap().unwrap();
    let younger_cell = fs.write_state(&younger_inode).unwrap().unwrap();
    let held_since = |cell: &SharedInodeWriteState| cell.lock().unwrap().native_owner.held_since();
    assert!(held_since(&oldest_cell).unwrap() < held_since(&younger_cell).unwrap());
    let committed_inodes = || {
        meta.commits
            .lock()
            .unwrap()
            .iter()
            .map(|commit| commit.inode_id.clone())
            .collect::<Vec<_>>()
    };

    // Let fsync finish on another thread after the budget waiter has picked
    // this holder. Keep the synced file open and listed for writeback.
    let sync_fs = fs.clone();
    *fs.hooks.dirty_budget_before_mark_due.lock().unwrap() = Some(Box::new(move || {
        std::thread::spawn(move || sync_fs.fsync(&context(), oldest, SyncMode::Full))
            .join()
            .unwrap()
            .unwrap();
    }));
    let (third, _) = r91_open_dirty(&fs, "r91-uses-released-budget");
    assert!(
        fs.hooks
            .dirty_budget_before_mark_due
            .lock()
            .unwrap()
            .is_none()
    );
    assert_eq!(committed_inodes(), vec![oldest_inode.clone()]);
    assert!(younger_cell.lock().unwrap().dirty);
    {
        let state = oldest_cell.lock().unwrap();
        assert_eq!(state.open_writers, 1);
        assert!(!state.dirty);
        assert!(state.native_owner.held_since().is_none());
        assert!(
            !state.native_due,
            "fsync already cleaned the selected holder"
        );
    }

    // The third write used the space freed by fsync. The fourth must commit
    // the next holder to get capacity. No periodic writeback runs here.
    let (fourth, _) = r91_open_dirty(&fs, "r91-needs-early-commit");
    assert_eq!(committed_inodes(), vec![oldest_inode, younger_inode]);

    for handle in [oldest, younger, third, fourth] {
        fs.fsync(&context(), handle, SyncMode::Full).unwrap();
        fs.release(&context(), handle).unwrap();
    }
}
