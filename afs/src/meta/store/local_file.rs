//! Local file persistence for opaque Meta Store snapshots.
//!
//! This backend owns only durable bytes plus a monotonically increasing version.
//! The Store above it owns all semantic validation and state transitions.

use std::{
    fs::{self, File, OpenOptions, TryLockError},
    io::{self, Read, Write},
    path::{Path, PathBuf},
    sync::{Arc, Mutex},
};

use afs_error::{Error, ErrorKind, Result};

use super::{BackendPersistence, MetaFuture, StoreBackend};

const LOCK_FILE: &str = "LOCK";
const SNAPSHOT_FILE: &str = "snapshot";
const SNAPSHOT_TMP_FILE: &str = "snapshot.tmp";
const WAL_FILE: &str = "wal";
const FRAME_MAGIC: &[u8; 4] = b"AFSL";
const FRAME_HEADER_LEN: usize = 28;
const COMPACT_AFTER_FRAMES: usize = 64;
const COMPACT_AFTER_WAL_BYTES: u64 = 8 * 1024 * 1024;

/// Durable local-file backend for versioned Store snapshots.
#[derive(Clone)]
pub struct LocalFileBackend {
    inner: Arc<LocalFileInner>,
}

struct LocalFileInner {
    dir: PathBuf,
    _lock: File,
    op_lock: Mutex<()>,
}

impl std::fmt::Debug for LocalFileBackend {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("LocalFileBackend")
            .field("dir", &self.inner.dir)
            .finish_non_exhaustive()
    }
}

impl LocalFileBackend {
    /// Opens a local Store backend rooted at `path` and takes an exclusive process lock.
    pub fn open(path: impl AsRef<Path>) -> Result<Self> {
        let dir = path.as_ref().to_path_buf();
        create_dir_durable(&dir)?;

        let lock = OpenOptions::new()
            .create(true)
            .truncate(false)
            .read(true)
            .write(true)
            .open(dir.join(LOCK_FILE))?;
        lock_exclusive(&lock)?;
        lock.sync_all()?;
        sync_dir(&dir)?;

        let backend = Self {
            inner: Arc::new(LocalFileInner {
                dir,
                _lock: lock,
                op_lock: Mutex::new(()),
            }),
        };
        let _ = backend.load()?;
        Ok(backend)
    }

    /// Loads the latest complete snapshot, if one has been committed.
    pub fn load(&self) -> Result<Option<(u64, Vec<u8>)>> {
        let _guard = self
            .inner
            .op_lock
            .lock()
            .map_err(|_| internal_error("local file backend lock poisoned"))?;
        Ok(self.load_inner()?.state)
    }

    /// Appends and fsyncs a new snapshot if `expected_version` is current.
    pub fn commit(&self, expected_version: u64, bytes: &[u8]) -> Result<u64> {
        let _guard = self
            .inner
            .op_lock
            .lock()
            .map_err(|_| internal_error("local file backend lock poisoned"))?;
        let loaded = self.load_inner()?;
        let current = loaded
            .state
            .as_ref()
            .map(|(version, _)| *version)
            .unwrap_or(0);
        if current != expected_version {
            return Err(failed_precondition(format!(
                "local file backend expected version {expected_version}, found {current}"
            )));
        }

        let new_version = expected_version
            .checked_add(1)
            .ok_or_else(|| failed_precondition("local file backend version overflow"))?;
        let wal_path = self.inner.dir.join(WAL_FILE);
        let wal_existed = wal_path.exists();
        let mut wal = OpenOptions::new()
            .create(true)
            .append(true)
            .read(true)
            .open(&wal_path)?;
        if !wal_existed {
            sync_dir(&self.inner.dir)?;
        }
        write_frame(&mut wal, new_version, bytes)?;
        wal.sync_all()?;

        let new_frame_len = frame_len(bytes)?;
        let wal_frames = loaded
            .complete_wal_frames
            .checked_add(1)
            .ok_or_else(|| data_loss("wal frame count overflow"))?;
        let wal_bytes = loaded
            .complete_wal_bytes
            .checked_add(new_frame_len)
            .ok_or_else(|| data_loss("wal byte count overflow"))?;
        if should_compact(wal_frames, wal_bytes) {
            self.compact(new_version, bytes)?;
        }
        Ok(new_version)
    }

    fn load_inner(&self) -> Result<ReplayResult> {
        let mut state = read_snapshot(&self.inner.dir.join(SNAPSHOT_FILE))?;
        let base_version = state.as_ref().map(|(version, _)| *version).unwrap_or(0);
        let mut replay = replay_wal(&self.inner.dir.join(WAL_FILE), base_version, state.take())?;
        if let Some(truncate_at) = replay.truncate_at {
            truncate_wal(&self.inner.dir.join(WAL_FILE), truncate_at)?;
        }
        if (should_compact(
            replay.frames_after_snapshot,
            replay.wal_bytes_after_snapshot,
        ) || should_compact(replay.complete_wal_frames, replay.complete_wal_bytes))
            && let Some((version, bytes)) = replay.state.as_ref()
        {
            self.compact(*version, bytes)?;
            replay.frames_after_snapshot = 0;
            replay.wal_bytes_after_snapshot = 0;
            replay.complete_wal_frames = 0;
            replay.complete_wal_bytes = 0;
            replay.truncate_at = None;
        }
        Ok(replay)
    }

    fn compact(&self, version: u64, bytes: &[u8]) -> Result<()> {
        let tmp_path = self.inner.dir.join(SNAPSHOT_TMP_FILE);
        let snapshot_path = self.inner.dir.join(SNAPSHOT_FILE);
        {
            let mut tmp = OpenOptions::new()
                .create(true)
                .truncate(true)
                .write(true)
                .open(&tmp_path)?;
            write_frame(&mut tmp, version, bytes)?;
            tmp.sync_all()?;
        }
        fs::rename(&tmp_path, &snapshot_path)?;
        sync_dir(&self.inner.dir)?;

        let wal = OpenOptions::new()
            .create(true)
            .write(true)
            .truncate(true)
            .open(self.inner.dir.join(WAL_FILE))?;
        wal.sync_all()?;
        sync_dir(&self.inner.dir)?;
        Ok(())
    }
}

impl StoreBackend for LocalFileBackend {
    fn persistence(&self) -> BackendPersistence {
        BackendPersistence::Persistent
    }

    fn load(&self) -> MetaFuture<'_, Option<(u64, Vec<u8>)>> {
        let backend = self.clone();
        Box::pin(async move {
            tokio::task::spawn_blocking(move || LocalFileBackend::load(&backend))
                .await
                .map_err(|error| {
                    Error::coded(
                        afs_error::IO_UNAVAILABLE,
                        format!("local store load task failed: {error}"),
                    )
                })?
        })
    }

    fn commit(&self, expected_version: u64, bytes: Vec<u8>) -> MetaFuture<'_, u64> {
        let backend = self.clone();
        Box::pin(async move {
            tokio::task::spawn_blocking(move || {
                LocalFileBackend::commit(&backend, expected_version, &bytes)
            })
            .await
            .map_err(|error| {
                Error::coded(
                    afs_error::IO_UNAVAILABLE,
                    format!("local store commit task failed: {error}"),
                )
            })?
        })
    }
}

impl Drop for LocalFileInner {
    fn drop(&mut self) {
        let _ = unlock(&self._lock);
    }
}

struct ReplayResult {
    state: Option<(u64, Vec<u8>)>,
    frames_after_snapshot: usize,
    wal_bytes_after_snapshot: u64,
    complete_wal_frames: usize,
    complete_wal_bytes: u64,
    truncate_at: Option<u64>,
}

fn read_snapshot(path: &Path) -> Result<Option<(u64, Vec<u8>)>> {
    let mut file = match File::open(path) {
        Ok(file) => file,
        Err(error) if error.kind() == io::ErrorKind::NotFound => return Ok(None),
        Err(error) => return Err(error.into()),
    };
    let mut bytes = Vec::new();
    file.read_to_end(&mut bytes)?;
    if bytes.is_empty() {
        return Err(data_loss("snapshot file is empty"));
    }
    let (frame, end) = decode_frame(&bytes, 0, TailPolicy::Reject).map_err(frame_error_to_error)?;
    if end != bytes.len() {
        return Err(data_loss("snapshot has trailing bytes"));
    }
    Ok(Some((frame.version, frame.payload)))
}

fn replay_wal(
    path: &Path,
    snapshot_version: u64,
    snapshot: Option<(u64, Vec<u8>)>,
) -> Result<ReplayResult> {
    let mut file = match File::open(path) {
        Ok(file) => file,
        Err(error) if error.kind() == io::ErrorKind::NotFound => {
            return Ok(ReplayResult {
                state: snapshot,
                frames_after_snapshot: 0,
                wal_bytes_after_snapshot: 0,
                complete_wal_frames: 0,
                complete_wal_bytes: 0,
                truncate_at: None,
            });
        }
        Err(error) => return Err(error.into()),
    };
    let mut bytes = Vec::new();
    file.read_to_end(&mut bytes)?;

    let mut offset = 0usize;
    let mut truncate_at = None;
    let mut state = snapshot;
    let mut current_version = snapshot_version;
    let mut frames_after_snapshot = 0usize;
    let mut wal_bytes_after_snapshot = 0u64;
    let mut complete_wal_frames = 0usize;
    let mut complete_wal_bytes = 0u64;

    while offset < bytes.len() {
        match decode_frame(&bytes, offset, TailPolicy::Truncate) {
            Ok((frame, next)) => {
                let frame_bytes =
                    u64::try_from(next - offset).map_err(|_| data_loss("wal frame too large"))?;
                complete_wal_frames = complete_wal_frames
                    .checked_add(1)
                    .ok_or_else(|| data_loss("wal frame count overflow"))?;
                complete_wal_bytes = complete_wal_bytes
                    .checked_add(frame_bytes)
                    .ok_or_else(|| data_loss("wal byte count overflow"))?;
                if frame.version > current_version {
                    let expected = current_version.checked_add(1).ok_or_else(|| {
                        failed_precondition("local file backend version overflow")
                    })?;
                    if frame.version != expected {
                        return Err(data_loss(format!(
                            "wal version gap: expected {expected}, found {}",
                            frame.version
                        )));
                    }
                    current_version = frame.version;
                    state = Some((frame.version, frame.payload));
                    frames_after_snapshot += 1;
                    wal_bytes_after_snapshot = wal_bytes_after_snapshot
                        .checked_add(frame_bytes)
                        .ok_or_else(|| data_loss("wal byte count overflow"))?;
                }
                offset = next;
            }
            Err(FrameError::Incomplete) => {
                truncate_at = Some(u64::try_from(offset).map_err(|_| data_loss("wal too large"))?);
                break;
            }
            Err(FrameError::Corrupt(message)) => return Err(data_loss(message)),
        }
    }

    Ok(ReplayResult {
        state,
        frames_after_snapshot,
        wal_bytes_after_snapshot,
        complete_wal_frames,
        complete_wal_bytes,
        truncate_at,
    })
}

fn truncate_wal(path: &Path, len: u64) -> Result<()> {
    let wal = OpenOptions::new()
        .create(true)
        .write(true)
        .truncate(false)
        .open(path)?;
    wal.set_len(len)?;
    wal.sync_all()?;
    Ok(())
}

struct Frame {
    version: u64,
    payload: Vec<u8>,
}

enum TailPolicy {
    Truncate,
    Reject,
}

enum FrameError {
    Incomplete,
    Corrupt(String),
}

fn write_frame(file: &mut File, version: u64, payload: &[u8]) -> Result<()> {
    let len = u64::try_from(payload.len())
        .map_err(|_| failed_precondition("local file backend payload too large"))?;
    let checksum = checksum(version, len, payload);
    file.write_all(FRAME_MAGIC)?;
    file.write_all(&version.to_le_bytes())?;
    file.write_all(&len.to_le_bytes())?;
    file.write_all(&checksum.to_le_bytes())?;
    file.write_all(payload)?;
    Ok(())
}

fn frame_len(payload: &[u8]) -> Result<u64> {
    let payload_len = u64::try_from(payload.len())
        .map_err(|_| failed_precondition("local file backend payload too large"))?;
    u64::try_from(FRAME_HEADER_LEN)
        .map_err(|_| internal_error("frame header length overflow"))?
        .checked_add(payload_len)
        .ok_or_else(|| failed_precondition("local file backend payload too large"))
}

fn should_compact(frames_after_snapshot: usize, wal_bytes_after_snapshot: u64) -> bool {
    frames_after_snapshot >= COMPACT_AFTER_FRAMES
        || wal_bytes_after_snapshot > COMPACT_AFTER_WAL_BYTES
}

fn decode_frame(
    bytes: &[u8],
    offset: usize,
    tail_policy: TailPolicy,
) -> std::result::Result<(Frame, usize), FrameError> {
    if bytes.len() - offset < FRAME_HEADER_LEN {
        return match tail_policy {
            TailPolicy::Truncate => Err(FrameError::Incomplete),
            TailPolicy::Reject => Err(FrameError::Corrupt("incomplete frame header".to_owned())),
        };
    }

    let header = &bytes[offset..offset + FRAME_HEADER_LEN];
    if &header[0..4] != FRAME_MAGIC {
        return Err(FrameError::Corrupt("frame magic mismatch".to_owned()));
    }
    let version = u64::from_le_bytes(header[4..12].try_into().expect("version slice"));
    let len = u64::from_le_bytes(header[12..20].try_into().expect("len slice"));
    let expected_checksum = u64::from_le_bytes(header[20..28].try_into().expect("checksum slice"));
    let payload_len = usize::try_from(len)
        .map_err(|_| FrameError::Corrupt("frame payload length overflows usize".to_owned()))?;
    let payload_start = offset + FRAME_HEADER_LEN;
    let payload_end = payload_start
        .checked_add(payload_len)
        .ok_or_else(|| FrameError::Corrupt("frame payload length overflows offset".to_owned()))?;
    if payload_end > bytes.len() {
        return match tail_policy {
            TailPolicy::Truncate => Err(FrameError::Incomplete),
            TailPolicy::Reject => Err(FrameError::Corrupt("incomplete frame payload".to_owned())),
        };
    }

    let payload = &bytes[payload_start..payload_end];
    let actual_checksum = checksum(version, len, payload);
    if actual_checksum != expected_checksum {
        return Err(FrameError::Corrupt(format!(
            "frame checksum mismatch at offset {offset}"
        )));
    }
    Ok((
        Frame {
            version,
            payload: payload.to_vec(),
        },
        payload_end,
    ))
}

fn checksum(version: u64, len: u64, payload: &[u8]) -> u64 {
    let mut hash = 0xcbf2_9ce4_8422_2325u64;
    for byte in FRAME_MAGIC
        .iter()
        .copied()
        .chain(version.to_le_bytes())
        .chain(len.to_le_bytes())
        .chain(payload.iter().copied())
    {
        hash ^= u64::from(byte);
        hash = hash.wrapping_mul(0x0000_0100_0000_01b3);
    }
    hash
}

fn create_dir_durable(dir: &Path) -> Result<()> {
    if let Some(parent) = dir.parent()
        && !parent.as_os_str().is_empty()
    {
        fs::create_dir_all(parent)?;
        sync_dir(parent)?;
    }
    fs::create_dir_all(dir)?;
    sync_dir(dir)?;
    if let Some(parent) = dir.parent()
        && !parent.as_os_str().is_empty()
    {
        sync_dir(parent)?;
    }
    Ok(())
}

fn sync_dir(path: &Path) -> Result<()> {
    match File::open(path) {
        Ok(dir) => dir.sync_all().map_err(Into::into),
        Err(error) if error.kind() == io::ErrorKind::NotFound => Ok(()),
        Err(error) => Err(error.into()),
    }
}

fn lock_exclusive(file: &File) -> Result<()> {
    file.try_lock().map_err(|error| match error {
        TryLockError::WouldBlock => Error::new(
            afs_error::IO_UNAVAILABLE,
            ErrorKind::Unavailable,
            "local file backend is already locked",
        ),
        TryLockError::Error(error) => error.into(),
    })
}

fn unlock(file: &File) -> io::Result<()> {
    file.unlock()
}

fn data_loss(message: impl Into<String>) -> Error {
    Error::new(afs_error::IO_INVALID, ErrorKind::DataLoss, message)
}

fn frame_error_to_error(error: FrameError) -> Error {
    match error {
        FrameError::Incomplete => data_loss("incomplete frame"),
        FrameError::Corrupt(message) => data_loss(message),
    }
}

fn failed_precondition(message: impl Into<String>) -> Error {
    Error::new(
        afs_error::IO_INVALID,
        ErrorKind::FailedPrecondition,
        message,
    )
}

fn internal_error(message: impl Into<String>) -> Error {
    Error::new(afs_error::IO_OTHER, ErrorKind::Internal, message)
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::io::{Seek, SeekFrom};

    fn payload(byte: u8, len: usize) -> Vec<u8> {
        vec![byte; len]
    }

    fn append_raw_frame(dir: &Path, version: u64, payload: &[u8]) {
        let mut wal = OpenOptions::new()
            .create(true)
            .append(true)
            .open(dir.join(WAL_FILE))
            .expect("open wal");
        write_frame(&mut wal, version, payload).expect("write frame");
        wal.sync_all().expect("sync wal");
    }

    fn write_snapshot(dir: &Path, version: u64, payload: &[u8]) {
        let mut snapshot = OpenOptions::new()
            .create(true)
            .truncate(true)
            .write(true)
            .open(dir.join(SNAPSHOT_FILE))
            .expect("open snapshot");
        write_frame(&mut snapshot, version, payload).expect("write snapshot");
        snapshot.sync_all().expect("sync snapshot");
    }

    #[test]
    fn replays_committed_frames_after_reopen() {
        let dir = tempfile::tempdir().expect("tempdir");
        {
            let backend = LocalFileBackend::open(dir.path()).expect("open backend");
            assert_eq!(backend.load().expect("initial load"), None);
            assert_eq!(backend.commit(0, b"one").expect("commit one"), 1);
            assert_eq!(backend.commit(1, b"two").expect("commit two"), 2);
        }

        let backend = LocalFileBackend::open(dir.path()).expect("reopen backend");
        assert_eq!(
            backend.load().expect("load after reopen"),
            Some((2, b"two".to_vec()))
        );
    }

    #[test]
    fn torn_wal_tail_is_truncated_and_ignored() {
        let dir = tempfile::tempdir().expect("tempdir");
        {
            let backend = LocalFileBackend::open(dir.path()).expect("open backend");
            backend.commit(0, b"stable").expect("commit stable");
        }
        {
            let mut wal = OpenOptions::new()
                .append(true)
                .open(dir.path().join(WAL_FILE))
                .expect("open wal");
            wal.write_all(FRAME_MAGIC).expect("write torn tail");
            wal.sync_all().expect("sync torn tail");
        }

        let backend = LocalFileBackend::open(dir.path()).expect("reopen backend");
        assert_eq!(
            backend.load().expect("load with torn tail"),
            Some((1, b"stable".to_vec()))
        );
        let wal_len = fs::metadata(dir.path().join(WAL_FILE))
            .expect("wal metadata")
            .len();
        assert_eq!(
            wal_len,
            u64::try_from(FRAME_HEADER_LEN + b"stable".len()).unwrap()
        );
    }

    #[test]
    fn checksum_corruption_is_rejected() {
        let dir = tempfile::tempdir().expect("tempdir");
        {
            let backend = LocalFileBackend::open(dir.path()).expect("open backend");
            backend.commit(0, b"stable").expect("commit stable");
        }
        {
            let mut wal = OpenOptions::new()
                .read(true)
                .write(true)
                .open(dir.path().join(WAL_FILE))
                .expect("open wal");
            wal.seek(SeekFrom::End(-1)).expect("seek last byte");
            wal.write_all(b"x").expect("corrupt last byte");
            wal.sync_all().expect("sync corruption");
        }

        let error = LocalFileBackend::open(dir.path()).expect_err("corruption rejected");
        assert_eq!(error.kind(), ErrorKind::DataLoss);
    }

    #[test]
    fn empty_snapshot_is_corruption_not_fresh_authority() {
        let dir = tempfile::tempdir().expect("tempdir");
        fs::write(dir.path().join(SNAPSHOT_FILE), []).expect("write empty snapshot");
        let error = LocalFileBackend::open(dir.path()).expect_err("empty snapshot rejected");
        assert_eq!(error.kind(), ErrorKind::DataLoss);
    }

    #[test]
    fn process_lock_rejects_second_open() {
        let dir = tempfile::tempdir().expect("tempdir");
        let _backend = LocalFileBackend::open(dir.path()).expect("first open");
        let error = LocalFileBackend::open(dir.path()).expect_err("second open fails");
        assert_eq!(error.kind(), ErrorKind::Unavailable);
    }

    #[test]
    fn compaction_bounds_wal_growth() {
        let dir = tempfile::tempdir().expect("tempdir");
        let backend = LocalFileBackend::open(dir.path()).expect("open backend");
        let mut version = 0;
        for index in 0..70 {
            version = backend
                .commit(version, format!("payload-{index}").as_bytes())
                .expect("commit");
        }
        assert_eq!(
            backend.load().expect("load compacted"),
            Some((70, b"payload-69".to_vec()))
        );
        let wal_len = fs::metadata(dir.path().join(WAL_FILE))
            .expect("wal metadata")
            .len();
        assert!(wal_len < 6 * u64::try_from(FRAME_HEADER_LEN + 16).unwrap());
    }

    #[test]
    fn byte_bound_compacts_before_sixty_four_frames() {
        let dir = tempfile::tempdir().expect("tempdir");
        let backend = LocalFileBackend::open(dir.path()).expect("open backend");
        let one = payload(1, 3 * 1024 * 1024);
        let two = payload(2, 3 * 1024 * 1024);
        let three = payload(3, 3 * 1024 * 1024);

        assert_eq!(backend.commit(0, &one).expect("commit one"), 1);
        assert_eq!(backend.commit(1, &two).expect("commit two"), 2);
        assert_eq!(backend.commit(2, &three).expect("commit three"), 3);

        assert_eq!(backend.load().expect("load compacted"), Some((3, three)));
        assert_eq!(
            fs::metadata(dir.path().join(WAL_FILE))
                .expect("wal metadata")
                .len(),
            0
        );
    }

    #[test]
    fn open_checkpoints_oversized_historical_wal() {
        let dir = tempfile::tempdir().expect("tempdir");
        let one = payload(1, 3 * 1024 * 1024);
        let two = payload(2, 3 * 1024 * 1024);
        let three = payload(3, 3 * 1024 * 1024);
        append_raw_frame(dir.path(), 1, &one);
        append_raw_frame(dir.path(), 2, &two);
        append_raw_frame(dir.path(), 3, &three);

        let backend = LocalFileBackend::open(dir.path()).expect("open backend");

        assert_eq!(backend.load().expect("load"), Some((3, three)));
        assert!(fs::metadata(dir.path().join(SNAPSHOT_FILE)).is_ok());
        assert_eq!(
            fs::metadata(dir.path().join(WAL_FILE))
                .expect("wal metadata")
                .len(),
            0
        );
    }

    #[test]
    fn open_clears_oversized_wal_already_covered_by_snapshot() {
        let dir = tempfile::tempdir().expect("tempdir");
        let snapshot = payload(7, 1024);
        write_snapshot(dir.path(), 3, &snapshot);
        append_raw_frame(dir.path(), 1, &payload(1, 5 * 1024 * 1024));
        append_raw_frame(dir.path(), 2, &payload(2, 5 * 1024 * 1024));
        append_raw_frame(dir.path(), 3, &payload(3, 5 * 1024 * 1024));

        let backend = LocalFileBackend::open(dir.path()).expect("open backend");

        assert_eq!(backend.load().expect("load"), Some((3, snapshot)));
        assert_eq!(
            fs::metadata(dir.path().join(WAL_FILE))
                .expect("wal metadata")
                .len(),
            0
        );
    }

    #[test]
    fn commit_counts_covered_wal_bytes_when_crossing_byte_bound() {
        let dir = tempfile::tempdir().expect("tempdir");
        let snapshot = payload(7, 1024);
        write_snapshot(dir.path(), 1, &snapshot);
        append_raw_frame(dir.path(), 1, &payload(1, 1024 * 1024));
        let backend = LocalFileBackend::open(dir.path()).expect("open backend");

        assert_eq!(
            fs::metadata(dir.path().join(WAL_FILE))
                .expect("wal metadata")
                .len(),
            frame_len(&payload(1, 1024 * 1024)).expect("frame len")
        );

        let next = payload(2, 7 * 1024 * 1024 + 1);
        assert_eq!(backend.commit(1, &next).expect("commit crossing bound"), 2);

        assert_eq!(backend.load().expect("load compacted"), Some((2, next)));
        assert_eq!(
            fs::metadata(dir.path().join(WAL_FILE))
                .expect("wal metadata")
                .len(),
            0
        );
    }

    #[test]
    fn oversized_wal_with_torn_tail_truncates_then_checkpoints() {
        let dir = tempfile::tempdir().expect("tempdir");
        let one = payload(1, 5 * 1024 * 1024);
        let two = payload(2, 5 * 1024 * 1024);
        append_raw_frame(dir.path(), 1, &one);
        append_raw_frame(dir.path(), 2, &two);
        {
            let mut wal = OpenOptions::new()
                .append(true)
                .open(dir.path().join(WAL_FILE))
                .expect("open wal");
            wal.write_all(FRAME_MAGIC).expect("write torn tail");
            wal.sync_all().expect("sync torn tail");
        }

        let backend = LocalFileBackend::open(dir.path()).expect("open backend");

        assert_eq!(backend.load().expect("load"), Some((2, two)));
        assert_eq!(
            fs::metadata(dir.path().join(WAL_FILE))
                .expect("wal metadata")
                .len(),
            0
        );
    }

    #[test]
    fn oversized_wal_checksum_corruption_is_rejected() {
        let dir = tempfile::tempdir().expect("tempdir");
        append_raw_frame(dir.path(), 1, &payload(1, 5 * 1024 * 1024));
        append_raw_frame(dir.path(), 2, &payload(2, 5 * 1024 * 1024));
        {
            let mut wal = OpenOptions::new()
                .read(true)
                .write(true)
                .open(dir.path().join(WAL_FILE))
                .expect("open wal");
            wal.seek(SeekFrom::End(-1)).expect("seek last byte");
            wal.write_all(b"x").expect("corrupt last byte");
            wal.sync_all().expect("sync corruption");
        }

        let error = LocalFileBackend::open(dir.path()).expect_err("corruption rejected");
        assert_eq!(error.kind(), ErrorKind::DataLoss);
    }

    #[test]
    fn live_load_rejects_corrupted_wal() {
        let dir = tempfile::tempdir().expect("tempdir");
        let backend = LocalFileBackend::open(dir.path()).expect("open backend");
        backend.commit(0, b"stable").expect("commit stable");
        {
            let mut wal = OpenOptions::new()
                .read(true)
                .write(true)
                .open(dir.path().join(WAL_FILE))
                .expect("open wal");
            wal.seek(SeekFrom::End(-1)).expect("seek last byte");
            wal.write_all(b"x").expect("corrupt last byte");
            wal.sync_all().expect("sync corruption");
        }

        let error = backend.load().expect_err("corruption rejected");
        assert_eq!(error.kind(), ErrorKind::DataLoss);
    }

    #[test]
    fn post_compaction_cas_and_reopen_use_latest_version() {
        let dir = tempfile::tempdir().expect("tempdir");
        let backend = LocalFileBackend::open(dir.path()).expect("open backend");
        let one = payload(1, 3 * 1024 * 1024);
        let two = payload(2, 3 * 1024 * 1024);
        let three = payload(3, 3 * 1024 * 1024);
        assert_eq!(backend.commit(0, &one).expect("commit one"), 1);
        assert_eq!(backend.commit(1, &two).expect("commit two"), 2);
        assert_eq!(backend.commit(2, &three).expect("commit three"), 3);

        let error = backend
            .commit(1, b"stale")
            .expect_err("stale expected version rejected");
        assert_eq!(error.kind(), ErrorKind::FailedPrecondition);
        drop(backend);

        let backend = LocalFileBackend::open(dir.path()).expect("reopen backend");
        assert_eq!(backend.load().expect("load after reopen"), Some((3, three)));
        assert_eq!(backend.commit(3, b"four").expect("commit four"), 4);
        assert_eq!(
            backend.load().expect("load final"),
            Some((4, b"four".to_vec()))
        );
    }
}
