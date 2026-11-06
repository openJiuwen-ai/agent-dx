use super::*;

fn wait_for_s3_puts(store: &FakeS3Store, fs: &DistributedFs, puts: usize) {
    let deadline = Instant::now() + Duration::from_secs(5);
    while store.puts.load(Ordering::SeqCst) < puts {
        assert!(Instant::now() < deadline, "S3 uploads did not start");
        fs.progress_writeback().unwrap();
        std::thread::sleep(Duration::from_millis(1));
    }
}

/// S1a-5: S3 writes go through the native pipeline. Reading back your own
/// uncommitted writes is served locally and never commits (decision D3);
/// fsync commits only external receipts; a new handle then reads through S3.
#[test]
fn s1a5_s3_write_reads_own_data_without_commit_and_fsync_publishes_external_receipts() {
    let (_temp, meta, fs, _store) = test_fs_on_fake_s3();
    let created = fs
        .create(
            &context(),
            fs.root_inode(),
            OsStr::new("s3.bin"),
            0o640,
            libc::O_RDWR,
        )
        .unwrap();
    let data: Vec<u8> = (0..6 * 1024 * 1024).map(|i| (i % 251) as u8).collect();
    assert_eq!(
        fs.write(&context(), created.handle, 0, &data).unwrap(),
        data.len()
    );
    let mut head = vec![0; 8192];
    assert_eq!(
        fs.read(
            &context(),
            created.handle,
            4 * 1024 * 1024 - 4096,
            &mut head
        )
        .unwrap(),
        head.len()
    );
    assert_eq!(
        &head[..],
        &data[4 * 1024 * 1024 - 4096..4 * 1024 * 1024 + 4096]
    );
    assert_eq!(meta.commit_count(), 0, "reading never commits");

    fs.fsync(&context(), created.handle, SyncMode::Full)
        .unwrap();
    let commits = meta.commits.lock().unwrap();
    assert_eq!(commits.len(), 1);
    assert_eq!(commits[0].file_version.length, data.len() as u64);
    assert!(
        commits[0]
            .chunk_receipts
            .iter()
            .all(|receipt| receipt.external_commit.is_some() && receipt.durable_acks.is_empty()),
        "S3 commits carry only external receipts"
    );
    drop(commits);
    fs.release(&context(), created.handle).unwrap();

    let reader = fs
        .open(&context(), created.entry.inode, libc::O_RDONLY)
        .unwrap();
    let mut back = vec![0; data.len()];
    let mut done = 0;
    while done < back.len() {
        let n = fs
            .read(&context(), reader, done as u64, &mut back[done..])
            .unwrap();
        assert!(n > 0);
        done += n;
    }
    assert_eq!(back, data);
    fs.release(&context(), reader).unwrap();
}

/// S1a-5 decision D1 (accepted, R05 reclaims): a full chunk uploaded before
/// an overwrite is no longer referenced by the committed version.
#[test]
fn s1a5_s3_overwrite_after_upload_leaves_an_unreferenced_object_today() {
    let (_temp, meta, fs, store) = test_fs_on_fake_s3();
    let created = fs
        .create(
            &context(),
            fs.root_inode(),
            OsStr::new("overwrite.bin"),
            0o640,
            libc::O_RDWR,
        )
        .unwrap();
    fs.write(&context(), created.handle, 0, &vec![1; 4 * 1024 * 1024])
        .unwrap();
    wait_for_s3_puts(&store, &fs, 1);
    fs.write(&context(), created.handle, 0, &vec![2; 4 * 1024 * 1024])
        .unwrap();
    fs.fsync(&context(), created.handle, SyncMode::Full)
        .unwrap();
    let commits = meta.commits.lock().unwrap();
    let referenced: std::collections::BTreeSet<_> = commits[0]
        .layout_root
        .inline_extents
        .iter()
        .map(|extent| extent.chunk_id.clone())
        .collect();
    let objects = store.fixture.chunks.lock().unwrap();
    let orphans = objects
        .keys()
        .filter(|id| !referenced.contains(*id))
        .count();
    assert_eq!(orphans, 1, "the fully overwritten upload is orphaned");
    assert_eq!(objects.len(), referenced.len() + orphans);
    drop(objects);
    drop(commits);
    fs.release(&context(), created.handle).unwrap();
}

/// S1a-5: an S3 PUT failure is reported to the writer, nothing is committed,
/// once, and the next barrier retries the same object.
#[test]
fn s1a5_s3_put_failure_is_reported_then_retried() {
    let (_temp, meta, fs, store) = test_fs_on_fake_s3();
    let created = fs
        .create(
            &context(),
            fs.root_inode(),
            OsStr::new("retry.bin"),
            0o640,
            libc::O_RDWR,
        )
        .unwrap();
    store.fail_next.store(true, Ordering::SeqCst);
    let write = fs.write(&context(), created.handle, 0, &vec![3; 4 * 1024 * 1024]);
    let mut failures = usize::from(write.is_err());
    while let Err(error) = fs.fsync(&context(), created.handle, SyncMode::Full) {
        assert!(
            error.to_string().contains("injected S3 PUT failure"),
            "{error}"
        );
        assert_eq!(meta.commit_count(), 0);
        failures += 1;
    }
    assert_eq!(failures, 1);
    assert_eq!(meta.commit_count(), 1);
    assert_eq!(store.objects(), 1);
    fs.release(&context(), created.handle).unwrap();
}

fn s3_payload(chunks: usize) -> Vec<u8> {
    (0..chunks * COMMIT_CHUNK_BYTES as usize)
        .map(|index| (index % 251) as u8)
        .collect()
}

fn s3_create(fs: &DistributedFs, name: &str, flags: i32) -> CreatedFile {
    fs.create(&context(), fs.root_inode(), OsStr::new(name), 0o640, flags)
        .unwrap()
}

/// A reader of an inode whose local write state holds nothing uncommitted
/// takes the committed S3 read path (cached version authorization), not
/// the dirty-overlay fallback with per-chunk source lookups.
#[test]
fn s1a5_s3_reader_of_a_clean_write_state_uses_the_committed_path() {
    let (_temp, _meta, fs, store) = test_fs_on_fake_s3();
    let created = s3_create(&fs, "clean.bin", libc::O_RDWR);
    let data = s3_payload(2);
    fs.write(&context(), created.handle, 0, &data).unwrap();
    fs.fsync(&context(), created.handle, SyncMode::Full)
        .unwrap();
    fs.release(&context(), created.handle).unwrap();
    let reader = fs
        .open(&context(), created.entry.inode, libc::O_RDONLY)
        .unwrap();
    let snapshot = fs.handle_snapshot(reader).unwrap();
    assert!(fs.write_state(&snapshot.inode_id).unwrap().is_some());
    let sources = store.fixture.source_calls.load(Ordering::SeqCst);
    let authorizations = store.fixture.current_calls.load(Ordering::SeqCst);

    let bytes = fs.read_owned(&context(), reader, 0, data.len()).unwrap();

    assert_eq!(&bytes[..], &data[..]);
    assert!(store.fixture.current_calls.load(Ordering::SeqCst) > authorizations);
    assert_eq!(store.fixture.source_calls.load(Ordering::SeqCst), sources);
    fs.release(&context(), reader).unwrap();
}

/// An unknown Meta ACK retries the retained request without another PUT.
#[test]
fn s1a5_s3_unknown_ack_retries_same_request_without_reupload() {
    let (_temp, meta, fs, store) = test_fs_on_fake_s3();
    let created = s3_create(&fs, "unknown-ack.bin", libc::O_RDWR);
    fs.write(&context(), created.handle, 0, &s3_payload(2))
        .unwrap();
    meta.fail_next_commit();

    fs.fsync(&context(), created.handle, SyncMode::DataOnly)
        .unwrap_err();
    let puts = store.puts.load(Ordering::SeqCst);
    let failed = meta.failed_commits.lock().unwrap()[0].clone();
    fs.fsync(&context(), created.handle, SyncMode::DataOnly)
        .unwrap();

    let commits = meta.commits.lock().unwrap();
    assert_eq!(commits.len(), 1);
    assert_eq!(failed.operation_id, commits[0].operation_id);
    assert_eq!(failed.file_version.id, commits[0].file_version.id);
    assert_eq!(failed.layout_root.id, commits[0].layout_root.id);
    assert_eq!(store.puts.load(Ordering::SeqCst), puts);
}

/// Writes before EOF and at EOF share one path and read back merged.
#[test]
fn s1a5_s3_non_eof_write_merges_with_committed_data() {
    let (_temp, meta, fs, _store) = test_fs_on_fake_s3();
    let created = s3_create(&fs, "mixed.bin", libc::O_RDWR);
    fs.write(&context(), created.handle, 0, b"abcdef").unwrap();
    fs.fsync(&context(), created.handle, SyncMode::DataOnly)
        .unwrap();
    fs.write(&context(), created.handle, 1, b"ZZ").unwrap();
    fs.write(&context(), created.handle, 6, b"tail").unwrap();
    let mut own = [0_u8; 10];
    assert_eq!(
        fs.read(&context(), created.handle, 0, &mut own).unwrap(),
        10
    );
    assert_eq!(&own, b"aZZdeftail");
    fs.fsync(&context(), created.handle, SyncMode::DataOnly)
        .unwrap();
    assert_eq!(
        meta.commits
            .lock()
            .unwrap()
            .last()
            .unwrap()
            .file_version
            .length,
        10
    );
    fs.release(&context(), created.handle).unwrap();

    let reader = fs
        .open(&context(), created.entry.inode, libc::O_RDONLY)
        .unwrap();
    let mut out = [0_u8; 10];
    assert_eq!(fs.read(&context(), reader, 0, &mut out).unwrap(), 10);
    assert_eq!(&out, b"aZZdeftail");
    fs.release(&context(), reader).unwrap();
}

/// A same-length resize over uploaded data keeps the data and needs no
/// second version once fsync has published it.
#[test]
fn s1a5_s3_same_length_resize_keeps_uploaded_data() {
    let (_temp, meta, fs, store) = test_fs_on_fake_s3();
    let created = s3_create(&fs, "resize.bin", libc::O_RDWR);
    let payload = s3_payload(2);
    fs.write(&context(), created.handle, 0, &payload).unwrap();
    wait_for_s3_puts(&store, &fs, 1);
    fs.setattr(
        &context(),
        created.entry.inode,
        Some(created.handle),
        &AttributeChange {
            size: Some(payload.len() as u64),
            ..AttributeChange::default()
        },
    )
    .unwrap();
    fs.fsync(&context(), created.handle, SyncMode::DataOnly)
        .unwrap();
    let committed = meta.commit_count();
    fs.fsync(&context(), created.handle, SyncMode::DataOnly)
        .unwrap();
    assert_eq!(meta.commit_count(), committed);
    assert_eq!(
        meta.commits
            .lock()
            .unwrap()
            .last()
            .unwrap()
            .file_version
            .length,
        payload.len() as u64
    );
    fs.release(&context(), created.handle).unwrap();
}

/// O_SYNC returns only after the whole write is published.
#[test]
fn s1a5_s3_o_sync_write_publishes_before_returning() {
    let (_temp, meta, fs, _store) = test_fs_on_fake_s3();
    let created = s3_create(&fs, "o-sync.bin", libc::O_RDWR | libc::O_SYNC);
    let payload = s3_payload(3);
    assert_eq!(
        fs.write(&context(), created.handle, 0, &payload).unwrap(),
        payload.len()
    );
    assert_eq!(
        meta.commits
            .lock()
            .unwrap()
            .last()
            .unwrap()
            .file_version
            .length,
        payload.len() as u64
    );
    fs.release(&context(), created.handle).unwrap();
}

/// fsync is an inode-wide barrier, also from a handle with nothing of its own.
#[test]
fn s1a5_s3_fsync_from_synced_fd_drains_other_writer() {
    let (_temp, meta, fs, _store) = test_fs_on_fake_s3();
    let created = s3_create(&fs, "two-fd.bin", libc::O_RDWR);
    fs.write(&context(), created.handle, 0, b"aaaa").unwrap();
    fs.fsync(&context(), created.handle, SyncMode::DataOnly)
        .unwrap();
    let second = fs
        .open(&context(), created.entry.inode, libc::O_RDWR)
        .unwrap();
    fs.write(&context(), second, 4, b"bbbb").unwrap();
    fs.fsync(&context(), created.handle, SyncMode::DataOnly)
        .unwrap();
    assert!(
        meta.commits
            .lock()
            .unwrap()
            .iter()
            .any(|commit| commit.file_version.length == 8)
    );
    fs.release(&context(), second).unwrap();
    fs.release(&context(), created.handle).unwrap();
}

/// S3 bodies stay in memory until published (D2), so writing past the dirty
/// budget has to publish by itself instead of blocking forever.
#[test]
fn s1a5_s3_writing_past_the_dirty_budget_publishes_under_pressure() {
    let (_temp, meta, fs, store) = test_fs_on_fake_s3();
    let created = s3_create(&fs, "pressure.bin", libc::O_RDWR);
    let chunk = vec![7; COMMIT_CHUNK_BYTES as usize];
    let chunks = 2 * PRODUCTION_NATIVE_BUDGET_BYTES / COMMIT_CHUNK_BYTES;
    let writer = {
        let (done_tx, done_rx) = std::sync::mpsc::channel();
        let fs = &fs;
        let chunk = &chunk;
        std::thread::scope(|scope| {
            scope.spawn(move || {
                for index in 0..chunks {
                    fs.write(
                        &context(),
                        created.handle,
                        index * COMMIT_CHUNK_BYTES,
                        chunk,
                    )
                    .unwrap();
                }
                done_tx.send(()).unwrap();
            });
            done_rx.recv_timeout(Duration::from_secs(60))
        })
    };
    writer.expect("writes past the dirty budget must not block forever");
    assert!(
        meta.commit_count() >= 1,
        "budget pressure publishes without an explicit barrier"
    );
    fs.fsync(&context(), created.handle, SyncMode::Full)
        .unwrap();
    assert_eq!(
        meta.commits
            .lock()
            .unwrap()
            .last()
            .unwrap()
            .file_version
            .length,
        chunks * COMMIT_CHUNK_BYTES
    );
    assert!(store.objects() >= 1);
    fs.release(&context(), created.handle).unwrap();
}
