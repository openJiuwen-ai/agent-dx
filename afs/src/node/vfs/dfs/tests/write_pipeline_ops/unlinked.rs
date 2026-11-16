use super::*;

const FILE_NAME: &str = "unlinked-terminal";
const CHUNK: usize = 4 * 1024 * 1024;

fn with_generation_pins(
    fs: DistributedFs,
) -> (
    DistributedFs,
    Arc<crate::node::gc::GcReferences>,
    Arc<GatedAdmissionMeta>,
) {
    let control = Arc::new(GatedAdmissionMeta::default());
    control.open_gate();
    let references = crate::node::gc::GcReferences::new(control.clone());
    let fs = fs.with_gc_references(Some(references.clone()));
    fs.hooks
        .create_admits_no_generation
        .store(true, Ordering::SeqCst);
    (fs, references, control)
}

fn create_file(fs: &DistributedFs) -> crate::node::vfs::types::CreatedFile {
    fs.create(
        &context(),
        fs.root_inode(),
        OsStr::new(FILE_NAME),
        0o640,
        libc::O_RDWR,
    )
    .unwrap()
}

fn unlink_file(fs: &DistributedFs, meta: &RecordingMeta) {
    meta.inode.lock().unwrap().attributes.nlink = 0;
    fs.unlink(&context(), fs.root_inode(), OsStr::new(FILE_NAME))
        .unwrap();
}

fn reject_next(meta: &RecordingMeta) {
    meta.fail_next_commit_with(Error::coded(
        afs_error::META_DFS_CONFLICT,
        "injected definite rejection for unlinked write",
    ));
}

fn assert_resources_released(
    fs: &DistributedFs,
    inode: &InodeId,
    references: &crate::node::gc::GcReferences,
    control: &GatedAdmissionMeta,
) {
    references.flush().unwrap();
    let pipeline = fs.write_resources.pipeline.as_ref().unwrap();
    let budget = pipeline.budget.snapshot();
    let pins = control.admitted.lock().unwrap().len();
    let retained = fs.write_state(inode).unwrap().is_some();
    eprintln!(
        "unlinked write: retained={retained}, dirty_bytes={}, budget_leases={}, generation_pins={pins}",
        budget.live_bytes, budget.leases
    );
    assert!(!retained, "unlinked terminal state must be retired");
    assert!(fs.pending_write_states().unwrap().is_empty());
    assert!(pipeline.tasks.lock().unwrap().is_empty());
    assert_eq!(budget.live_bytes, 0);
    assert_eq!(budget.leases, 0);
    assert_eq!(pins, 0);
    let metadata = pipeline.metadata.state.lock().unwrap();
    assert_eq!(metadata.descriptors, 0);
    assert_eq!(metadata.descriptor_reserved_bytes, 0);
}

#[test]
fn rejection_before_unlink_and_last_close_releases_resources() {
    replay_rejection(false);
}

#[test]
fn rejection_after_unlink_and_last_close_releases_resources() {
    replay_rejection(true);
}

fn replay_rejection(after_close: bool) {
    let (_temp, meta, fs, _runtime, _store) = native_pipeline_fixture(false, 2 * CHUNK, 8);
    let (fs, references, control) = with_generation_pins(fs);
    let file = create_file(&fs);
    let inode = fs.inode_id(file.entry.inode).unwrap();
    fs.write(&context(), file.handle, 0, &vec![0x51; CHUNK])
        .unwrap();
    native_wait_uploads(&fs, 1);
    assert_eq!(control.admitted.lock().unwrap().len(), 1);

    if !after_close {
        reject_next(&meta);
        assert_eq!(
            fs.fsync(&context(), file.handle, SyncMode::Full)
                .unwrap_err()
                .code(),
            afs_error::META_DFS_CONFLICT
        );
    }
    unlink_file(&fs, &meta);
    if let Err(error) = fs.release(&context(), file.handle) {
        assert!(!after_close);
        assert_eq!(error.code(), afs_error::META_DFS_CONFLICT);
    }
    assert!(fs.handles.get(file.handle.0).unwrap().is_none());

    if after_close {
        let cell = fs.write_state(&inode).unwrap().unwrap();
        assert!(cell.lock().unwrap().terminal_error.is_none());
        drop(cell);
        reject_next(&meta);
        assert_eq!(fs.writeback_pending().unwrap(), 0);
    }
    assert_eq!(meta.commit_count(), 0);
    assert_eq!(meta.failed_commits.lock().unwrap().len(), 1);
    assert_resources_released(&fs, &inode, &references, &control);
}

#[test]
fn open_reader_keeps_dirty_view_until_its_last_close() {
    replay_reader(false);
}

#[test]
fn concurrent_reader_open_keeps_dirty_view_across_meta_preflight() {
    replay_reader(true);
}

fn replay_reader(concurrent: bool) {
    let (_temp, meta, fs, _runtime, _store) = native_pipeline_fixture(false, 2 * CHUNK, 8);
    let (fs, references, control) = with_generation_pins(fs);
    let fs = Arc::new(fs);
    let file = create_file(&fs);
    let inode = fs.inode_id(file.entry.inode).unwrap();
    fs.write(&context(), file.handle, 0, b"accepted-dirty-data")
        .unwrap();
    reject_next(&meta);
    assert_eq!(
        fs.fsync(&context(), file.handle, SyncMode::Full)
            .unwrap_err()
            .code(),
        afs_error::META_DFS_CONFLICT
    );
    let reader = if concurrent {
        let pause = Arc::new(NativeReadPause::default());
        *meta.next_get_inode_pause.lock().unwrap() = Some(pause.clone());
        let fs_open = fs.clone();
        let open =
            std::thread::spawn(move || fs_open.open(&context(), file.entry.inode, libc::O_RDONLY));
        pause.wait_until_reached();
        unlink_file(&fs, &meta);
        fs.release(&context(), file.handle).unwrap();
        let retained = fs.write_state(&inode).unwrap().is_some();
        pause.resume();
        let reader = open.join().unwrap().unwrap();
        assert!(
            retained,
            "the open started before unlink still needs the dirty view"
        );
        reader
    } else {
        let reader = fs
            .open(&context(), file.entry.inode, libc::O_RDONLY)
            .unwrap();
        unlink_file(&fs, &meta);
        fs.release(&context(), file.handle).unwrap();
        reader
    };
    assert!(fs.write_state(&inode).unwrap().is_some());
    let mut output = [0; b"accepted-dirty-data".len()];
    assert_eq!(
        fs.read(&context(), reader, 0, &mut output).unwrap(),
        output.len()
    );
    assert_eq!(&output, b"accepted-dirty-data");
    fs.release(&context(), reader).unwrap();
    assert_resources_released(&fs, &inode, &references, &control);
}

struct ResumeUpload(Arc<CommitPause>);

impl Drop for ResumeUpload {
    fn drop(&mut self) {
        self.0.release();
    }
}

fn pause_upload(store: &NativeFixtureStore) -> ResumeUpload {
    let pause = Arc::new(CommitPause::new(0));
    *store.pause.lock().unwrap() = Some(pause.clone());
    ResumeUpload(pause)
}

fn wait_until(mut ready: impl FnMut() -> bool) {
    let deadline = Instant::now() + Duration::from_secs(5);
    while !ready() {
        assert!(Instant::now() < deadline, "upload barrier did not arrive");
        std::thread::sleep(Duration::from_millis(1));
    }
}

#[test]
fn last_upload_finishes_after_unlink_and_all_closes() {
    let (_temp, meta, fs, _runtime, store) = native_pipeline_fixture(false, 4 * CHUNK, 8);
    let (fs, references, control) = with_generation_pins(fs);
    let fs = Arc::new(fs);
    let file = create_file(&fs);
    let second = fs.open(&context(), file.entry.inode, libc::O_RDWR).unwrap();
    let inode = fs.inode_id(file.entry.inode).unwrap();
    let first_pause = pause_upload(&store);
    fs.write(&context(), file.handle, 0, &vec![0x51; CHUNK])
        .unwrap();
    wait_until(|| first_pause.0.was_reached());
    let background_fs = fs.clone();
    let background = std::thread::spawn(move || background_fs.writeback_pending());
    let cell = fs.write_state(&inode).unwrap().unwrap();
    wait_until(|| {
        matches!(
            cell.lock().unwrap().in_flight,
            Some(InFlightCommit::Preparing(_))
        )
    });

    let last_pause = pause_upload(&store);
    fs.write(&context(), second, CHUNK as u64, &vec![0x62; CHUNK])
        .unwrap();
    wait_until(|| last_pause.0.was_reached());
    reject_next(&meta);
    first_pause.0.release();
    assert_eq!(background.join().unwrap().unwrap(), 0);
    assert_eq!(meta.failed_commits.lock().unwrap().len(), 1);
    assert!(cell.lock().unwrap().terminal_error.is_some());
    drop(cell);

    unlink_file(&fs, &meta);
    for handle in [second, file.handle] {
        if let Err(error) = fs.release(&context(), handle) {
            assert_eq!(error.code(), afs_error::META_DFS_CONFLICT);
        }
        assert!(fs.handles.get(handle.0).unwrap().is_none());
    }
    assert!(fs.write_state(&inode).unwrap().is_some());
    assert!(!control.admitted.lock().unwrap().is_empty());
    last_pause.0.release();
    native_wait_uploads(&fs, 2);
    assert_resources_released(&fs, &inode, &references, &control);
}

#[cfg(target_os = "linux")]
#[test]
#[ignore = "requires Linux /dev/fuse and fusermount3"]
fn fuse_releases_unlinked_rejected_writes_in_both_orders() {
    use std::{fs::OpenOptions, os::unix::fs::FileExt};

    for after_close in [false, true] {
        let (temp, meta, fs, runtime, _store) = native_pipeline_fixture(false, 2 * CHUNK, 8);
        let (fs, references, control) = with_generation_pins(fs);
        let file = create_file(&fs);
        let inode = fs.inode_id(file.entry.inode).unwrap();
        fs.release(&context(), file.handle).unwrap();
        let mut root = meta.inode.lock().unwrap().clone();
        root.inode_id = fs.inode_id(fs.root_inode()).unwrap();
        root.kind = InodeKind::Directory;
        root.head_version = None;
        root.attributes.mode = 0o755;
        root.attributes.nlink = 2;
        *meta.files.lock().unwrap() = Some(HashMap::from([(root.inode_id.clone(), root)]));
        let mount = temp.path().join("mnt");
        std::fs::create_dir(&mount).unwrap();
        let _runtime_guard = runtime.enter();
        let fs = Arc::new(fs);
        let session = crate::node::fuse::mount_dfs(fs.clone(), &mount).unwrap();
        let path = mount.join(FILE_NAME);
        let writer = OpenOptions::new()
            .read(true)
            .write(true)
            .open(&path)
            .unwrap();
        let reader = std::fs::File::open(&path).unwrap();
        writer.write_all_at(&vec![0x53; CHUNK], 0).unwrap();

        if !after_close {
            reject_next(&meta);
            assert!(writer.sync_all().is_err());
        } else {
            // FUSE FLUSH on close may send the commit. Model an uncertain reply
            // there and issue the definite rejection on the background replay
            // after both kernel RELEASE requests have run.
            meta.fail_next_commit_with(Error::coded(
                afs_error::CLIENT_DEADLINE_EXCEEDED,
                "injected unknown close commit outcome",
            ));
        }
        meta.inode.lock().unwrap().attributes.nlink = 0;
        std::fs::remove_file(&path).unwrap();
        drop(writer);
        let mut bytes = [0; 32];
        reader.read_exact_at(&mut bytes, 0).unwrap();
        assert_eq!(bytes, [0x53; 32]);
        assert!(fs.write_state(&inode).unwrap().is_some());
        drop(reader);
        wait_until(|| fs.handles.is_empty());
        // RELEASE takes an entry out of the handle table before completing
        // write-state bookkeeping. Let the latter reach its terminal state.
        if after_close {
            wait_until(|| {
                let Some(cell) = fs.write_state(&inode).unwrap() else {
                    return false;
                };
                let state = cell.lock().unwrap();
                state.open_writers == 0 && !state.commit_busy && state.terminal_error.is_none()
            });
            reject_next(&meta);
            wait_until(|| {
                fs.writeback_pending().unwrap();
                fs.write_state(&inode).unwrap().is_none()
            });
        } else {
            wait_until(|| fs.write_state(&inode).unwrap().is_none());
        }
        assert_eq!(meta.commit_count(), 0);
        eprintln!("kernel FUSE: after_close={after_close}, existing reader returned dirty bytes");
        assert_resources_released(&fs, &inode, &references, &control);
        session.join().unwrap();
    }
}
