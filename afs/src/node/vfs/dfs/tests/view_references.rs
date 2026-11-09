// R94: a handle's read view and its GC reference move together. A local
// commit moves open handles to the new version only once that version has a
// reference; the replaced reference stays until the last read using it ends.
use super::*;

#[derive(Default)]
struct RecordingGc {
    /// Admitted `open:` references and the version each names.
    retained: Mutex<Vec<(String, Option<FileVersionId>)>>,
    /// `open:` admissions that were refused.
    refused: Mutex<Vec<String>>,
    released: Mutex<Vec<String>>,
    refuse_open: std::sync::atomic::AtomicBool,
}

impl RecordingGc {
    fn live(&self, version: &FileVersionId) -> usize {
        let released = self.released.lock().unwrap();
        self.retained
            .lock()
            .unwrap()
            .iter()
            .filter(|(id, named)| named.as_ref() == Some(version) && !released.contains(id))
            .count()
    }

    fn admissions(&self, version: &FileVersionId) -> usize {
        self.retained
            .lock()
            .unwrap()
            .iter()
            .filter(|(_, named)| named.as_ref() == Some(version))
            .count()
    }
}

impl crate::node::gc::GcControl for RecordingGc {
    fn retain(&self, id: &str, _: &InodeId, version: Option<&FileVersionId>) -> Result<()> {
        if self.refuse_open.load(Ordering::SeqCst) {
            self.refused.lock().unwrap().push(id.to_owned());
            return Err(unavailable("injected GC admission failure"));
        }
        self.retained
            .lock()
            .unwrap()
            .push((id.to_owned(), version.cloned()));
        Ok(())
    }
    fn retain_fenced(
        &self,
        _: &str,
        _: &InodeId,
        _: Option<&FileVersionId>,
    ) -> Result<crate::dfs::GcFence> {
        Ok(crate::dfs::GcFence::WriteGeneration)
    }
    fn release(&self, ids: &[String]) -> Result<()> {
        self.released.lock().unwrap().extend_from_slice(ids);
        Ok(())
    }
    fn collect(&self, _: u64, _: Vec<ChunkObject>) -> Result<crate::dfs::GcState> {
        Ok(crate::dfs::GcState::default())
    }
}

struct Fixture {
    _temp: tempfile::TempDir,
    meta: Arc<RecordingMeta>,
    fs: DistributedFs,
    gc: Arc<RecordingGc>,
    references: Arc<crate::node::gc::GcReferences>,
    inode: BackendInode,
    v1: FileVersionId,
}

fn fixture() -> Fixture {
    let (temp, meta, fs) = test_fs();
    let gc = Arc::new(RecordingGc::default());
    let references = crate::node::gc::GcReferences::new(gc.clone());
    let fs = fs.with_gc_references(Some(references.clone()));
    let inode_id = meta.inode.lock().unwrap().inode_id.clone();
    let v1 = publish_external_version_with_bytes(&meta, &fs, "r94-v1", b"version-one");
    let inode = fs.backend_inode(&inode_id).unwrap();
    Fixture {
        _temp: temp,
        meta,
        fs,
        gc,
        references,
        inode,
        v1,
    }
}

fn read_all(fs: &DistributedFs, handle: FileHandle) -> Vec<u8> {
    fs.read_owned(&context(), handle, 0, 64).unwrap()
}

fn head(meta: &RecordingMeta) -> FileVersionId {
    meta.inode.lock().unwrap().head_version.clone().unwrap()
}

fn opened_version(fs: &DistributedFs, handle: FileHandle) -> Option<FileVersionId> {
    fs.handle_snapshot(handle)
        .unwrap()
        .opened_inode
        .head_version
        .clone()
}

/// Local commit of `bytes` through a writer that is closed afterwards.
fn commit_locally(fx: &Fixture, bytes: &[u8]) -> FileVersionId {
    let writer = fx.fs.open(&context(), fx.inode, libc::O_RDWR).unwrap();
    fx.fs.write(&context(), writer, 0, bytes).unwrap();
    fx.fs.fsync(&context(), writer, SyncMode::Full).unwrap();
    fx.fs.release(&context(), writer).unwrap();
    head(&fx.meta)
}

#[test]
fn r94_local_commit_moves_the_reader_reference_to_the_new_version() {
    let fx = fixture();
    let reader = fx.fs.open(&context(), fx.inode, libc::O_RDONLY).unwrap();
    assert_eq!(read_all(&fx.fs, reader), b"version-one");
    assert_eq!(fx.gc.live(&fx.v1), 1);

    let v2 = commit_locally(&fx, b"VERSION-TWO");
    assert_ne!(v2, fx.v1);
    // Decision 4a: this Node's commit is visible to the open reader ...
    assert_eq!(opened_version(&fx.fs, reader), Some(v2.clone()));
    assert_eq!(read_all(&fx.fs, reader), b"VERSION-TWO");
    // ... and the version it now reads is protected by its own reference,
    // admitted once for all handles of this Node.
    fx.references.flush().unwrap();
    assert_eq!(fx.gc.live(&v2), 1, "the reader's new version is referenced");
    assert_eq!(fx.gc.admissions(&v2), 1, "one admission per local commit");
    assert_eq!(fx.gc.live(&fx.v1), 0, "the replaced reference is released");

    // Closing the last handle releases the reference: nothing keeps V2 for
    // ever once no handle reads it.
    fx.fs.release(&context(), reader).unwrap();
    fx.references.flush().unwrap();
    assert_eq!(fx.gc.live(&v2), 0);
}

#[test]
fn r94_in_flight_read_keeps_the_replaced_reference_until_it_ends() {
    let fx = fixture();
    let reader = fx.fs.open(&context(), fx.inode, libc::O_RDONLY).unwrap();
    // A read that captured the handle before the commit still uses V1.
    let in_flight = fx.fs.handle_snapshot(reader).unwrap();
    assert_eq!(in_flight.opened_inode.head_version, Some(fx.v1.clone()));

    let v2 = commit_locally(&fx, b"VERSION-TWO");
    assert_eq!(opened_version(&fx.fs, reader), Some(v2.clone()));
    fx.references.flush().unwrap();
    assert_eq!(fx.gc.live(&fx.v1), 1, "the in-flight read keeps V1");
    assert_eq!(fx.gc.live(&v2), 1);
    drop(in_flight);
    fx.references.flush().unwrap();
    assert_eq!(fx.gc.live(&fx.v1), 0, "released once the read ended");
    fx.fs.release(&context(), reader).unwrap();
    fx.references.flush().unwrap();
    assert_eq!(fx.gc.live(&v2), 0);
}

#[test]
fn r94_failed_reference_keeps_the_reader_on_its_protected_version_until_a_retry() {
    let fx = fixture();
    let reader = fx.fs.open(&context(), fx.inode, libc::O_RDONLY).unwrap();
    let writer = fx.fs.open(&context(), fx.inode, libc::O_RDWR).unwrap();
    fx.fs.write(&context(), writer, 0, b"VERSION-TWO").unwrap();
    fx.gc.refuse_open.store(true, Ordering::SeqCst);
    fx.fs.fsync(&context(), writer, SyncMode::Full).unwrap();
    let v2 = head(&fx.meta);
    assert_ne!(v2, fx.v1);
    // The reference is admitted when the last local writer closes; its
    // failure does not fail the close, and the commit stays durable.
    fx.fs.release(&context(), writer).unwrap();
    assert!(!fx.gc.refused.lock().unwrap().is_empty());
    assert_eq!(head(&fx.meta), v2);

    // The reader stays on V1, which its reference still protects, and never
    // publishes the unprotected V2; reads keep retrying.
    assert_eq!(opened_version(&fx.fs, reader), Some(fx.v1.clone()));
    assert!(fx.fs.handle_view_pending(reader).unwrap());
    assert_eq!(read_all(&fx.fs, reader), b"version-one");
    assert_eq!(opened_version(&fx.fs, reader), Some(fx.v1.clone()));
    fx.references.flush().unwrap();
    assert_eq!(fx.gc.live(&fx.v1), 1);
    assert_eq!(fx.gc.live(&v2), 0);
    // A refused admission leaves no reference behind: its id is released.
    let released = fx.gc.released.lock().unwrap().clone();
    for id in fx.gc.refused.lock().unwrap().iter() {
        assert!(released.contains(id), "refused admission {id} is released");
    }

    // Meta admits again: the next read publishes V2 and releases V1.
    fx.gc.refuse_open.store(false, Ordering::SeqCst);
    assert_eq!(read_all(&fx.fs, reader), b"VERSION-TWO");
    assert_eq!(opened_version(&fx.fs, reader), Some(v2.clone()));
    assert!(!fx.fs.handle_view_pending(reader).unwrap());
    fx.references.flush().unwrap();
    assert_eq!(fx.gc.live(&v2), 1);
    assert_eq!(fx.gc.live(&fx.v1), 0);
    fx.fs.release(&context(), reader).unwrap();
    fx.references.flush().unwrap();
    assert_eq!(fx.gc.live(&v2), 0);
}

#[test]
fn r94_failed_reference_is_retried_when_the_next_writer_closes() {
    let fx = fixture();
    let reader = fx.fs.open(&context(), fx.inode, libc::O_RDONLY).unwrap();
    fx.gc.refuse_open.store(true, Ordering::SeqCst);
    let v2 = commit_locally(&fx, b"VERSION-TWO");
    assert_eq!(opened_version(&fx.fs, reader), Some(fx.v1.clone()));
    assert!(fx.fs.handle_view_pending(reader).unwrap());

    fx.gc.refuse_open.store(false, Ordering::SeqCst);
    let v3 = commit_locally(&fx, b"VERSION-333");
    assert_ne!(v3, v2);
    assert_eq!(opened_version(&fx.fs, reader), Some(v3.clone()));
    assert!(!fx.fs.handle_view_pending(reader).unwrap());
    assert_eq!(read_all(&fx.fs, reader), b"VERSION-333");
    fx.references.flush().unwrap();
    assert_eq!(fx.gc.live(&v3), 1);
    // The second writer's own open referenced V2; nothing keeps it now.
    assert_eq!(fx.gc.live(&v2), 0);
    assert_eq!(fx.gc.live(&fx.v1), 0);
    fx.fs.release(&context(), reader).unwrap();
    fx.references.flush().unwrap();
    assert_eq!(fx.gc.live(&v3), 0);
}

#[test]
fn r94_commits_under_an_open_writer_admit_once_when_it_closes() {
    let fx = fixture();
    let reader = fx.fs.open(&context(), fx.inode, libc::O_RDONLY).unwrap();
    let writer = fx.fs.open(&context(), fx.inode, libc::O_RDWR).unwrap();
    let mut versions = Vec::new();
    for bytes in [b"VERSION-TWO", b"VERSION-333", b"VERSION-444"] {
        fx.fs.write(&context(), writer, 0, bytes).unwrap();
        fx.fs.fsync(&context(), writer, SyncMode::Full).unwrap();
        versions.push(head(&fx.meta));
        // The open reader sees this Node's commit through the write state ...
        assert_eq!(read_all(&fx.fs, reader), bytes.as_slice());
    }
    // ... and no reference is admitted while the writer is open.
    for version in &versions {
        assert_eq!(fx.gc.admissions(version), 0);
    }
    fx.fs.release(&context(), writer).unwrap();
    let newest = versions.last().unwrap().clone();
    assert_eq!(opened_version(&fx.fs, reader), Some(newest.clone()));
    assert_eq!(fx.gc.admissions(&newest), 1, "one admission at the close");
    assert_eq!(read_all(&fx.fs, reader), b"VERSION-444");
    fx.references.flush().unwrap();
    assert_eq!(fx.gc.live(&fx.v1), 0);
    fx.fs.release(&context(), reader).unwrap();
    fx.references.flush().unwrap();
    assert_eq!(fx.gc.live(&newest), 0);
}

#[test]
fn r94_a_lone_writer_commit_admits_no_reader_reference() {
    let fx = fixture();
    // Open, write, fsync, close with no other handle: the small-file case.
    let v2 = commit_locally(&fx, b"VERSION-TWO");
    assert_eq!(fx.gc.admissions(&v2), 0);
    fx.references.flush().unwrap();
    assert_eq!(
        fx.gc.live(&fx.v1),
        0,
        "the writer's own reference is released"
    );
}

#[test]
fn r94_close_racing_a_pending_view_leaks_no_reference() {
    let fx = fixture();
    let reader = fx.fs.open(&context(), fx.inode, libc::O_RDONLY).unwrap();
    fx.gc.refuse_open.store(true, Ordering::SeqCst);
    let v2 = commit_locally(&fx, b"VERSION-TWO");
    assert!(fx.fs.handle_view_pending(reader).unwrap());
    fx.gc.refuse_open.store(false, Ordering::SeqCst);
    fx.fs.release(&context(), reader).unwrap();
    // A retry after the close finds no handle and admits nothing.
    fx.fs.retry_pending_view(reader).unwrap();
    fx.references.flush().unwrap();
    assert_eq!(fx.gc.live(&fx.v1), 0);
    assert_eq!(fx.gc.admissions(&v2), 0);
}
