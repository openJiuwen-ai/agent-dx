//! Local canonical operation log. The legacy JSON backend is not read here.
//! Two checked tail slots share the WAL's single group durability barrier.
//!
//! Writer exclusion contract: the directory `LOCK` (`flock`, held for the
//! handle's lifetime) is the mutual exclusion; the writer term in `CURRENT`
//! is a backstop. Every operation re-reads `CURRENT` and fails
//! `META_LOG_FENCED` once a newer term is visible, but that check and the
//! following write are not one atomic unit across processes. The directory
//! must therefore be on exclusive local storage where `flock` works (not NFS,
//! not a block device or directory shared by two hosts, and `LOCK` is never
//! deleted while Meta runs). If the lock does not exclude an older writer and
//! a newer writer opens and appends inside the older writer's check-then-write
//! window, the older writer overwrites the newer writer's acknowledged group;
//! the newer writer latches at its next operation
//! (`lost_lock_window_between_term_check_and_write`).
use std::{
    fs::{self, File, OpenOptions, TryLockError},
    io::{Read, Seek, SeekFrom, Write},
    os::{
        fd::AsRawFd,
        unix::fs::{MetadataExt, OpenOptionsExt},
    },
    path::{Path, PathBuf},
    sync::{Arc, Mutex},
};

use afs_error::{Error, Result};

use super::{
    BackendPersistence, MetaFuture, StoreRevision,
    journal::{
        AppendLimits, BATCH_RECORDS_MAX, CheckpointLimits, LogCursor, LogStatus, LogTip, MetaLog,
        RecoveredCheckpoint, RecoveredLog, StoreIdentity, chain_term, decode_frame, encode_batch,
        fenced, frame_length, full, invalid_log, log_context, unavailable,
    },
    log::Record,
};
use crate::format::{self, FormatKind};

mod checkpoint;
mod current;
mod init;
mod replay;
mod segment;
use checkpoint::*;
use current::*;
use init::*;
use replay::*;
use segment::*;

/// Fixed size of each local WAL segment header (static header plus tail slots).
pub const LOG_HEADER_BYTES: u64 = 4096;
const WAL: &str = "wal-00000000000000000001.log";
const IDENTITY: &str = "IDENTITY";
const STATIC_BYTES: usize = 256;
const SLOT_BYTES: usize = 64;
const EVIDENCE_LIMIT: usize = 2;

/// Local WAL bounds. Append bounds are framed bytes; segment and directory
/// bounds include each segment's `LOG_HEADER_BYTES` header.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct LocalLogOptions {
    pub record_max_bytes: usize,
    pub batch_max_bytes: usize,
    /// Directory WAL budget: every live or not-yet-removed segment file,
    /// headers included.
    pub wal_max_bytes: u64,
    /// Soft rotation target; an atomic batch is never split between segments.
    pub segment_bytes: u64,
    pub segments_max: usize,
}
impl Default for LocalLogOptions {
    fn default() -> Self {
        Self {
            record_max_bytes: 64 * 1024 * 1024,
            batch_max_bytes: 128 * 1024 * 1024,
            wal_max_bytes: 1024 * 1024 * 1024,
            segment_bytes: 64 * 1024 * 1024,
            segments_max: 64,
        }
    }
}
impl LocalLogOptions {
    /// Backend-neutral append bounds this WAL accepts.
    pub fn append(self) -> AppendLimits {
        AppendLimits {
            record_max_bytes: self.record_max_bytes,
            batch_max_bytes: self.batch_max_bytes,
            batch_records_max: BATCH_RECORDS_MAX,
        }
    }
    pub fn validate(self) -> Result<Self> {
        let record = u64::try_from(self.record_max_bytes)
            .map_err(|_| invalid_log("record limit overflow"))?;
        if self.segment_bytes < LOG_HEADER_BYTES + 256
            || !(2..=256).contains(&self.segments_max)
            || self.record_max_bytes < 256
            || self.record_max_bytes > u32::MAX as usize
            || self.batch_max_bytes < self.record_max_bytes
            || u64::try_from(self.batch_max_bytes)
                .map_err(|_| invalid_log("batch limit overflow"))?
                > self.wal_max_bytes.saturating_sub(LOG_HEADER_BYTES)
            || record > self.wal_max_bytes.saturating_sub(LOG_HEADER_BYTES)
        {
            return Err(invalid_log("invalid finite record/batch/WAL limits"));
        }
        self.append().validate()?;
        Ok(self)
    }
}
/// Local checkpoint bounds. `image_max_bytes` is the backend-neutral image
/// bound; the rest bound local files. `validation_charge_max_bytes` bounds the
/// memory charge held while proving an unreferenced image owned before
/// deletion (the same 2 GiB default as Store's capture budget).
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct LocalCheckpointLimits {
    pub image_max_bytes: usize,
    pub validation_charge_max_bytes: usize,
    pub disk_max_bytes: u64,
    pub files_max: usize,
}
impl Default for LocalCheckpointLimits {
    fn default() -> Self {
        Self {
            image_max_bytes: CheckpointLimits::default().image_max_bytes,
            validation_charge_max_bytes: 2 * 1024 * 1024 * 1024,
            disk_max_bytes: 4 * 1024 * 1024 * 1024,
            files_max: 8,
        }
    }
}
impl LocalCheckpointLimits {
    pub fn validate(self) -> Result<Self> {
        let image = u64::try_from(self.image_max_bytes)
            .map_err(|_| invalid_log("checkpoint limit overflow"))?;
        if self.image_max_bytes < 256
            || self.validation_charge_max_bytes < self.image_max_bytes
            || image
                .checked_mul(3)
                .and_then(|n| n.checked_add(2 * 64 * 1024))
                .is_none_or(|n| n > self.disk_max_bytes)
            || self.files_max < 5
        {
            return Err(invalid_log("invalid finite checkpoint limits"));
        }
        Ok(self)
    }
}

#[derive(Clone)]
pub struct LocalLog {
    inner: Arc<Inner>,
}
struct Inner {
    owner_pid: u32,
    dir: PathBuf,
    identity: StoreIdentity,
    limits: LocalLogOptions,
    checkpoint_limits: LocalCheckpointLimits,
    checkpoint_install: Mutex<()>,
    _lock: File,
    state: Mutex<LocalState>,
    /// Test-only: runs once inside the next append, after the CURRENT term
    /// and tip checks and before any byte is written.
    #[cfg(test)]
    before_write: Mutex<Option<Box<dyn FnOnce() + Send>>>,
}
impl Inner {
    fn ensure_owner(&self) -> Result<()> {
        if self.owner_pid != std::process::id() {
            return Err(unavailable(
                "inherited local log handle is not a writer authority",
            ));
        }
        Ok(())
    }
}
impl Drop for Inner {
    fn drop(&mut self) {
        // Only the final owner in the creating process may release this lock.
        // A fork/dup can keep its open file description alive beyond close;
        // a child must neither use the inherited writer nor unlock its parent.
        if self.owner_pid == std::process::id()
            && let Err(error) = self._lock.unlock()
        {
            afs_tracing::tracing::warn!(error = %error, "Meta log owner unlock failed");
        }
    }
}
struct LocalState {
    file: File,
    tip: LogTip,
    end: u64,
    generation: u64,
    poisoned: bool,
    recovered: Option<RecoveredLog>,
    manifest: Current,
    revision: StoreRevision,
    wal_bytes: u64,
}
#[derive(Clone, Copy, Debug, Eq, PartialEq, serde::Deserialize, serde::Serialize)]
struct Tail {
    generation: u64,
    tip: LogTip,
    end: u64,
}

impl LocalLog {
    /// New directories only; old-format or foreign data is never adopted.
    pub fn open(path: impl AsRef<Path>, limits: LocalLogOptions) -> Result<Self> {
        Self::open_with_checkpoint_limits(path, limits, LocalCheckpointLimits::default())
    }
    pub fn open_with_checkpoint_limits(
        path: impl AsRef<Path>,
        limits: LocalLogOptions,
        checkpoint_limits: LocalCheckpointLimits,
    ) -> Result<Self> {
        let limits = limits.validate()?;
        let checkpoint_limits = checkpoint_limits.validate()?;
        let dir = path.as_ref().to_path_buf();
        create_dir(&dir)?;
        let lock = OpenOptions::new()
            .read(true)
            .write(true)
            .create(true)
            .truncate(false)
            .mode(0o600)
            .custom_flags(libc::O_NOFOLLOW | libc::O_CLOEXEC)
            .open(dir.join("LOCK"))?;
        match lock.try_lock() {
            Ok(()) => {}
            Err(TryLockError::WouldBlock) => return Err(unavailable("Meta log already locked")),
            Err(TryLockError::Error(error)) => return Err(error.into()),
        }
        let identity = if dir.join("INITIALIZING").try_exists()? {
            initialize_marked(&dir, checkpoint_limits)?
        } else if dir.join(IDENTITY).try_exists()? {
            read_identity(&dir)?
        } else {
            for entry in fs::read_dir(&dir)? {
                if entry?.file_name() != "LOCK" {
                    return Err(invalid_log(
                        "nonempty directory without IDENTITY; legacy/recovery data retained; use an empty directory for the new format and keep the original binary and directory for old data",
                    ));
                }
            }
            let marker = open_file(&dir.join("INITIALIZING"), true)?;
            marker.sync_all()?;
            sync_dir(&dir)?;
            initialize_marked(&dir, checkpoint_limits)?
        };
        let mut replay = replay(&dir, identity, limits, checkpoint_limits)?;
        let mut manifest = replay.manifest;
        cleanup_unreferenced_images(
            &dir,
            &manifest,
            checkpoint_limits,
            replay
                .recovered
                .checkpoint
                .as_ref()
                .map_or(0, |image| image.bytes.capacity()),
        )?;
        let (retired_segments, retired_images) =
            cleanup_retired(&dir, &manifest, limits, checkpoint_limits)?;
        manifest.retired_segments = retired_segments;
        manifest.retired_images = retired_images;
        // This open is a new writer: its term is durable in CURRENT (file and
        // directory synced) before any handle exists, so every older handle
        // is fenced at its next CURRENT check and no term is ever reused.
        manifest.term = manifest
            .term
            .checked_add(1)
            .ok_or_else(|| invalid_log("CURRENT writer term exhausted"))?;
        #[cfg(test)]
        super::crash_tests::pause(
            "open_term_prepared",
            "recovered; next writer term not yet in CURRENT",
        );
        write_current(&dir, &manifest, checkpoint_limits)?;
        #[cfg(test)]
        super::crash_tests::pause(
            "open_term_selected",
            "next writer term durable in CURRENT; handle not returned",
        );
        replay.recovered.term = manifest.term;
        let wal_bytes = wal_budget(&dir, limits, 0, 0, &manifest.retired_segments)?;
        Ok(Self {
            inner: Arc::new(Inner {
                owner_pid: std::process::id(),
                dir,
                identity,
                limits,
                checkpoint_limits,
                checkpoint_install: Mutex::new(()),
                _lock: lock,
                #[cfg(test)]
                before_write: Mutex::new(None),
                state: Mutex::new(LocalState {
                    file: replay.file,
                    tip: replay.recovered.tip,
                    end: replay.end,
                    generation: replay.generation,
                    poisoned: false,
                    recovered: Some(replay.recovered),
                    manifest,
                    revision: replay.revision,
                    wal_bytes,
                }),
            }),
        })
    }
    pub fn identity(&self) -> StoreIdentity {
        self.inner.identity
    }
    /// Test-only: the writer term this handle selected at open.
    #[cfg(test)]
    pub(super) fn term(&self) -> u64 {
        self.inner
            .state
            .lock()
            .map_or(0, |state| state.manifest.term)
    }
    /// Test-only: drop this handle's directory lock while keeping the handle,
    /// modelling a writer that lost its local exclusivity so a newer writer
    /// can open. The newer term in CURRENT must then fence this handle.
    #[cfg(test)]
    pub(super) fn release_lock_for_takeover(&self) -> Result<()> {
        Ok(self.inner._lock.unlock()?)
    }
    /// Test-only: run `hook` once inside the next append, between the
    /// CURRENT term check and the first written byte.
    #[cfg(test)]
    pub(super) fn pause_next_append_before_write(&self, hook: impl FnOnce() + Send + 'static) {
        if let Ok(mut slot) = self.inner.before_write.lock() {
            *slot = Some(Box::new(hook));
        }
    }
    /// Test-only: replace the active WAL descriptor with a read-only one so the
    /// next append hits a real write(2) failure (EBADF) and latches.
    #[cfg(test)]
    pub(super) fn inject_write_failure(&self) -> Result<()> {
        let mut state = self
            .inner
            .state
            .lock()
            .map_err(|_| unavailable("local WAL lock poisoned"))?;
        let active = state
            .manifest
            .segments
            .last()
            .ok_or_else(|| invalid_log("missing active segment"))?;
        state.file = File::open(self.inner.dir.join(wal_name(active.base)?))?;
        Ok(())
    }
    fn check_files(inner: &Inner, state: &mut LocalState) -> Result<()> {
        if state.poisoned {
            return Err(unavailable("Meta WAL write failed; reopen required"));
        }
        if read_identity(&inner.dir)? != inner.identity {
            return Err(invalid_log("live IDENTITY changed"));
        }
        let active = state
            .manifest
            .segments
            .last()
            .ok_or_else(|| invalid_log("missing active segment"))?;
        let selected = read_current(&inner.dir, inner.identity)?
            .ok_or_else(|| invalid_log("live CURRENT missing"))?;
        if selected.term != state.manifest.term {
            return Err(fenced(state.manifest.term, selected.term));
        }
        if selected != state.manifest {
            return Err(invalid_log("live CURRENT changed"));
        }
        let actual = fs::symlink_metadata(inner.dir.join(wal_name(active.base)?))?;
        let opened = state.file.metadata()?;
        if !actual.is_file()
            || actual.dev() != opened.dev()
            || actual.ino() != opened.ino()
            || actual.len() != state.end
        {
            return Err(invalid_log("live WAL identity/length changed"));
        }
        let declared = header(&mut state.file, inner.identity, active.base)?;
        if declared.tip != state.tip
            || declared.end != state.end
            || declared.generation != state.generation
        {
            return Err(invalid_log("live WAL declared tail changed"));
        }
        Ok(())
    }
    fn rotate(inner: &Inner, state: &mut LocalState) -> Result<()> {
        let mut manifest = state.manifest.clone();
        let last = manifest
            .segments
            .last_mut()
            .ok_or_else(|| invalid_log("missing active segment"))?;
        if state.tip == last.base {
            return Err(invalid_log("cannot seal an empty active segment"));
        }
        last.sealed = Some((
            Tail {
                generation: state.generation,
                tip: state.tip,
                end: state.end,
            },
            state.revision,
        ));
        let next = SegmentRef {
            base: state.tip,
            base_revision: state.revision,
            sealed: None,
        };
        let target = inner.dir.join(wal_name(next.base)?);
        let exists = target.try_exists()?;
        let bytes = wal_budget(
            &inner.dir,
            inner.limits,
            if exists { 0 } else { LOG_HEADER_BYTES },
            usize::from(!exists),
            &state.manifest.retired_segments,
        )?;
        let mut file = if exists {
            let mut file = open_file(&target, false)?;
            let tail = header(&mut file, inner.identity, next.base)?;
            if tail
                != (Tail {
                    generation: 0,
                    tip: next.base,
                    end: LOG_HEADER_BYTES,
                })
                || file.metadata()?.len() != LOG_HEADER_BYTES
            {
                return Err(invalid_log(
                    "unselected next segment is not an exact empty prepared segment; retained",
                ));
            }
            file
        } else {
            let mut file = open_file(&target, true)?;
            reserve_file(&file, LOG_HEADER_BYTES)?;
            let mut bytes = [0u8; LOG_HEADER_BYTES as usize];
            bytes[..STATIC_BYTES].copy_from_slice(&static_header(inner.identity, next.base));
            bytes[STATIC_BYTES..STATIC_BYTES + SLOT_BYTES].copy_from_slice(&tail_bytes(Tail {
                generation: 0,
                tip: next.base,
                end: LOG_HEADER_BYTES,
            }));
            file.write_all(&bytes)?;
            file
        };
        file.sync_all()?;
        sync_dir(&inner.dir)?;
        #[cfg(test)]
        super::crash_tests::pause(
            "rotation_prepared",
            "empty next segment synced; old CURRENT",
        );
        manifest.segments.push(next);
        if manifest.segments.len() > inner.limits.segments_max {
            return Err(full("WAL segment count exhausted"));
        }
        // Selector durability precedes all writes through the new active FD.
        if let Err(error) = write_current(&inner.dir, &manifest, inner.checkpoint_limits) {
            state.poisoned = true;
            return Err(error);
        }
        #[cfg(test)]
        super::crash_tests::pause(
            "rotation_selected",
            "new CURRENT synced; no append attempted",
        );
        file.seek(SeekFrom::Start(LOG_HEADER_BYTES))?;
        state.file = file;
        state.manifest = manifest;
        state.end = LOG_HEADER_BYTES;
        state.generation = 0;
        state.wal_bytes = bytes;
        Ok(())
    }
    /// Install one checkpoint under the single-installer gate: verify the
    /// anchor, keep a valid previous image, write the new image, then select it
    /// in CURRENT and retire what it covers.
    fn install_checkpoint_blocking(inner: &Inner, covered: LogTip, bytes: Vec<u8>) -> Result<()> {
        Self::cleanup(inner)?;
        let limits = inner.checkpoint_limits;
        let revision = super::log::StateImage::validate_encoded(&bytes, limits.image_max_bytes)?;
        let (mut prefix, old) = {
            let mut state = inner
                .state
                .lock()
                .map_err(|_| unavailable("WAL lock poisoned"))?;
            Self::check_files(inner, &mut state)?;
            if covered.sequence > state.tip.sequence {
                return Err(invalid_log("checkpoint covers future log sequence"));
            }
            let mut prefix = state.manifest.segments.clone();
            let last = prefix
                .last_mut()
                .ok_or_else(|| invalid_log("missing checkpoint active segment"))?;
            last.sealed = Some((
                Tail {
                    generation: state.generation,
                    tip: state.tip,
                    end: state.end,
                },
                state.revision,
            ));
            (prefix, state.manifest.clone())
        };
        verify_anchor(
            &inner.dir,
            inner.identity,
            inner.limits,
            &prefix,
            covered,
            revision,
        )?;
        let (header, reference) = image_header(inner.identity, covered, revision, &bytes);
        if let Some(current) = old.current {
            if covered.sequence < current.covered.sequence {
                return Err(invalid_log("checkpoint installation regresses CURRENT"));
            }
            if covered.sequence == current.covered.sequence {
                if current != reference {
                    return Err(invalid_log("same checkpoint position has different state"));
                }
                read_image(&inner.dir, inner.identity, reference, limits)?;
                open_file(&inner.dir.join(image_name(reference)), false)?.sync_all()?;
                open_file(&inner.dir.join(CURRENT), false)?.sync_all()?;
                return sync_dir(&inner.dir);
            }
        }
        let previous = Self::retained_previous_image(inner, &prefix, &old)?;
        cleanup_unreferenced_images(&inner.dir, &old, limits, bytes.capacity())?;
        // The captured prefix is no longer needed after anchors validate.
        prefix.clear();
        Self::write_checkpoint_image(inner, reference, &header, &bytes)?;
        Self::select_checkpoint_manifest(inner, reference, previous)?;
        Self::cleanup(inner)
    }
    /// The newest selected image that is still readable and anchored on the
    /// chain; it stays as `previous` so one damaged image never loses history.
    fn retained_previous_image(
        inner: &Inner,
        prefix: &[SegmentRef],
        old: &Current,
    ) -> Result<Option<CheckpointRef>> {
        for candidate in old.current.into_iter().chain(old.previous) {
            if read_image(
                &inner.dir,
                inner.identity,
                candidate,
                inner.checkpoint_limits,
            )
            .is_ok()
                && verify_anchor(
                    &inner.dir,
                    inner.identity,
                    inner.limits,
                    prefix,
                    candidate.covered,
                    candidate.revision,
                )
                .is_ok()
            {
                return Ok(Some(candidate));
            }
        }
        if old.current.is_some() {
            return Err(invalid_log(
                "prior CURRENT recovery point invalid; originals retained",
            ));
        }
        Ok(None)
    }
    /// Make the image durable under its final name (or prove an existing one).
    fn write_checkpoint_image(
        inner: &Inner,
        reference: CheckpointRef,
        header: &[u8; IMAGE_HEADER],
        bytes: &[u8],
    ) -> Result<()> {
        let limits = inner.checkpoint_limits;
        quarantine_checkpoint_tmp(&inner.dir, "checkpoint.tmp", limits)?;
        let target = inner.dir.join(image_name(reference));
        let exists = target.try_exists()?;
        let extra = if exists {
            0
        } else {
            reference.bytes + IMAGE_HEADER as u64
        };
        // The CURRENT that selects this image is covered by the selector
        // reservation inside `checkpoint_budget`.
        checkpoint_budget(&inner.dir, extra, usize::from(!exists), limits)?;
        if exists {
            read_image(&inner.dir, inner.identity, reference, limits)?;
            open_file(&target, false)?.sync_all()?;
        } else {
            let tmp = inner.dir.join("checkpoint.tmp");
            let mut file = open_file(&tmp, true)?;
            reserve_file(&file, reference.bytes + IMAGE_HEADER as u64)?;
            file.write_all(header)?;
            #[cfg(test)]
            if super::crash_tests::active("checkpoint_partial") {
                file.write_all(&bytes[..bytes.len() / 2])?;
                super::crash_tests::pause("checkpoint_partial", "half image body written");
            }
            file.write_all(bytes)?;
            file.sync_all()?;
            fs::rename(tmp, &target)?;
            sync_dir(&inner.dir)?;
            #[cfg(test)]
            super::crash_tests::pause("checkpoint_image_installed", "image durable; old CURRENT");
        }
        Ok(())
    }
    /// Select `reference` in a durable CURRENT, keeping `previous`, retiring
    /// other images and every segment both chains cover. A failed CURRENT
    /// write latches the log.
    fn select_checkpoint_manifest(
        inner: &Inner,
        reference: CheckpointRef,
        previous: Option<CheckpointRef>,
    ) -> Result<()> {
        let mut state = inner
            .state
            .lock()
            .map_err(|_| unavailable("WAL lock poisoned"))?;
        Self::check_files(inner, &mut state)?;
        let mut manifest = state.manifest.clone();
        for image in manifest.current.into_iter().chain(manifest.previous) {
            if image != reference
                && Some(image) != previous
                && !manifest.retired_images.contains(&image)
            {
                manifest.retired_images.push(image);
            }
        }
        manifest.current = Some(reference);
        manifest.previous = previous;
        let oldest = previous.unwrap_or(reference).covered.sequence;
        let remove = manifest
            .segments
            .iter()
            .take_while(|segment| {
                segment
                    .sealed
                    .is_some_and(|(tail, _)| tail.tip.sequence <= oldest)
            })
            .count();
        manifest
            .retired_segments
            .extend(manifest.segments.drain(..remove));
        if let Err(error) = write_current(&inner.dir, &manifest, inner.checkpoint_limits) {
            state.poisoned = true;
            return Err(error);
        }
        #[cfg(test)]
        super::crash_tests::pause(
            "checkpoint_selected",
            "new CURRENT durable; before retirement cleanup",
        );
        state.manifest = manifest;
        state.recovered = None;
        Ok(())
    }
    fn cleanup(inner: &Inner) -> Result<()> {
        let manifest = {
            let mut state = inner
                .state
                .lock()
                .map_err(|_| unavailable("WAL lock poisoned"))?;
            Self::check_files(inner, &mut state)?;
            state.manifest.clone()
        };
        // No writer lock while checking retired bodies or deleting files.
        // The checkpoint-install guard serializes retirement/cleanup; rotation
        // can only append live descriptors and never reselect a retired file.
        let (segments, images) =
            cleanup_retired(&inner.dir, &manifest, inner.limits, inner.checkpoint_limits)?;
        let mut state = inner
            .state
            .lock()
            .map_err(|_| unavailable("WAL lock poisoned"))?;
        Self::check_files(inner, &mut state)?;
        if state.manifest.retired_segments != segments || state.manifest.retired_images != images {
            let mut current = state.manifest.clone();
            current.retired_segments = segments;
            current.retired_images = images;
            if let Err(error) = write_current(&inner.dir, &current, inner.checkpoint_limits) {
                state.poisoned = true;
                return Err(error);
            }
            state.manifest = current;
        }
        state.wal_bytes = wal_budget(
            &inner.dir,
            inner.limits,
            0,
            0,
            &state.manifest.retired_segments,
        )?;
        Ok(())
    }
}
impl MetaLog for LocalLog {
    fn persistence(&self) -> BackendPersistence {
        BackendPersistence::Persistent
    }
    fn limits(&self) -> AppendLimits {
        self.inner.limits.append()
    }
    fn checkpoint_limits(&self) -> CheckpointLimits {
        CheckpointLimits {
            image_max_bytes: self.inner.checkpoint_limits.image_max_bytes,
        }
    }
    fn supports_checkpoint(&self) -> bool {
        true
    }
    fn health(&self) -> MetaFuture<'_, ()> {
        let inner = Arc::clone(&self.inner);
        Box::pin(async move {
            inner.ensure_owner()?;
            tokio::task::spawn_blocking(move || {
                inner.ensure_owner()?;
                let mut state = inner
                    .state
                    .lock()
                    .map_err(|_| unavailable("local WAL lock poisoned"))?;
                Self::check_files(&inner, &mut state)
            })
            .await
            .map_err(|e| unavailable(format!("WAL health task: {e}")))?
        })
    }
    /// Charges every segment holding any record after the checkpoint as a
    /// whole (`end - LOG_HEADER_BYTES`), so the bound may include the covered
    /// prefix of one segment.
    fn status(&self) -> MetaFuture<'_, LogStatus> {
        let inner = Arc::clone(&self.inner);
        Box::pin(async move {
            inner.ensure_owner()?;
            tokio::task::spawn_blocking(move || {
                inner.ensure_owner()?;
                let mut state = inner
                    .state
                    .lock()
                    .map_err(|_| unavailable("WAL lock poisoned"))?;
                Self::check_files(&inner, &mut state)?;
                let checkpoint = state
                    .manifest
                    .current
                    .map_or(LogTip::EMPTY, |image| image.covered);
                let mut pending = 0u64;
                for segment in &state.manifest.segments {
                    let (tip, end) = segment
                        .sealed
                        .map_or((state.tip, state.end), |(tail, _)| (tail.tip, tail.end));
                    if tip.sequence > checkpoint.sequence {
                        pending = pending
                            .checked_add(end - LOG_HEADER_BYTES)
                            .ok_or_else(|| invalid_log("checkpoint trigger byte overflow"))?;
                    }
                }
                Ok(LogStatus {
                    tip: state.tip,
                    checkpoint,
                    uncheckpointed_bytes_upper_bound: pending,
                })
            })
            .await
            .map_err(|error| unavailable(format!("WAL status task: {error}")))?
        })
    }
    fn can_append(&self, expected: LogCursor, framed_bytes: usize) -> MetaFuture<'_, bool> {
        let inner = Arc::clone(&self.inner);
        Box::pin(async move {
            inner.ensure_owner()?;
            tokio::task::spawn_blocking(move || {
                inner.ensure_owner()?;
                let mut state = inner
                    .state
                    .lock()
                    .map_err(|_| unavailable("WAL lock poisoned"))?;
                Self::check_files(&inner, &mut state)?;
                if expected.term != state.manifest.term {
                    return Err(fenced(expected.term, state.manifest.term));
                }
                if expected.tip != state.tip {
                    return Err(unavailable("WAL admission tip changed"));
                }
                let rotate = state.end > LOG_HEADER_BYTES
                    && state
                        .end
                        .checked_add(framed_bytes as u64)
                        .is_none_or(|end| end > inner.limits.segment_bytes);
                let (extra, files) = if rotate {
                    let exists = inner.dir.join(wal_name(state.tip)?).try_exists()?;
                    (
                        if exists { 0 } else { LOG_HEADER_BYTES },
                        usize::from(!exists),
                    )
                } else {
                    (0, 0)
                };
                let charge = (framed_bytes as u64)
                    .checked_add(extra)
                    .ok_or_else(|| full("WAL admission byte overflow"))?;
                // This path performs only reads: capacity failure proves append
                // was not attempted. Never apply this classification to write IO.
                match wal_budget(
                    &inner.dir,
                    inner.limits,
                    charge,
                    files,
                    &state.manifest.retired_segments,
                ) {
                    Ok(_) => Ok(true),
                    Err(error) if error.code() == afs_error::IO_CAPACITY => Ok(false),
                    Err(error) => Err(error),
                }
            })
            .await
            .map_err(|error| unavailable(format!("WAL admission task: {error}")))?
        })
    }
    fn install_checkpoint(&self, covered: LogTip, bytes: Vec<u8>) -> MetaFuture<'_, ()> {
        let inner = Arc::clone(&self.inner);
        Box::pin(async move {
            inner.ensure_owner()?;
            tokio::task::spawn_blocking(move || {
                inner.ensure_owner()?;
                let _install = inner
                    .checkpoint_install
                    .try_lock()
                    .map_err(|_| unavailable("checkpoint install already active or poisoned"))?;
                Self::install_checkpoint_blocking(&inner, covered, bytes)
            })
            .await
            .map_err(|error| unavailable(format!("checkpoint installer task: {error}")))?
        })
    }
    fn recover(&self) -> MetaFuture<'_, RecoveredLog> {
        let inner = Arc::clone(&self.inner);
        Box::pin(async move {
            inner.ensure_owner()?;
            tokio::task::spawn_blocking(move || {
                inner.ensure_owner()?;
                let mut state = inner
                    .state
                    .lock()
                    .map_err(|_| unavailable("local WAL lock poisoned"))?;
                Self::check_files(&inner, &mut state)?;
                if let Some(recovered) = state.recovered.take() {
                    return Ok(recovered);
                }
                Ok(replay(
                    &inner.dir,
                    inner.identity,
                    inner.limits,
                    inner.checkpoint_limits,
                )?
                .recovered)
            })
            .await
            .map_err(|e| unavailable(format!("WAL recovery task: {e}")))?
        })
    }
    fn append_and_sync(&self, expected: LogCursor, records: Vec<Record>) -> MetaFuture<'_, LogTip> {
        let inner = Arc::clone(&self.inner);
        Box::pin(async move {
            inner.ensure_owner()?;
            tokio::task::spawn_blocking(move || {
                inner.ensure_owner()?;
                let mut state = inner
                    .state
                    .lock()
                    .map_err(|_| unavailable("local WAL lock poisoned"))?;
                Self::check_files(&inner, &mut state)?;
                if expected.term != state.manifest.term {
                    return Err(fenced(expected.term, state.manifest.term));
                }
                if state.tip != expected.tip {
                    return Err(unavailable("local log compare-and-append tip changed"));
                }
                let revision = records
                    .last()
                    .map(|record| record.revision_after())
                    .ok_or_else(|| invalid_log("empty append group"))?;
                let (frames, tip, bytes) =
                    encode_batch(inner.identity, expected, records, inner.limits.append())?;
                let rotate = state.end > LOG_HEADER_BYTES
                    && state
                        .end
                        .checked_add(bytes as u64)
                        .is_none_or(|size| size > inner.limits.segment_bytes);
                let header_charge =
                    if rotate && !inner.dir.join(wal_name(state.tip)?).try_exists()? {
                        LOG_HEADER_BYTES
                    } else {
                        0
                    };
                let extra = (bytes as u64)
                    .checked_add(header_charge)
                    .ok_or_else(|| full("WAL byte budget overflow"))?;
                if state
                    .wal_bytes
                    .checked_add(extra)
                    .is_none_or(|size| size > inner.limits.wal_max_bytes)
                {
                    return Err(full("WAL byte budget exhausted; checkpoint required"));
                }
                if rotate {
                    Self::rotate(&inner, &mut state)?;
                }
                let end = state
                    .end
                    .checked_add(bytes as u64)
                    .ok_or_else(|| full("WAL byte budget overflow"))?;
                let generation = state
                    .generation
                    .checked_add(1)
                    .ok_or_else(|| invalid_log("tail generation exhausted"))?;
                let start = state.end;
                #[cfg(test)]
                if let Some(hook) = inner
                    .before_write
                    .lock()
                    .ok()
                    .and_then(|mut slot| slot.take())
                {
                    hook();
                }
                let result = (|| {
                    state.file.seek(SeekFrom::Start(start))?;
                    for frame in frames {
                        #[cfg(test)]
                        if super::crash_tests::active("partial_second")
                            && state.file.stream_position()? > start
                        {
                            state.file.write_all(&frame[..frame.len() / 2])?;
                            super::crash_tests::pause(
                                "partial_second",
                                "one full, one partial frame",
                            );
                        }
                        state.file.write_all(&frame)?;
                    }
                    write_tail(
                        &mut state.file,
                        Tail {
                            generation,
                            tip,
                            end,
                        },
                    )?;
                    #[cfg(test)]
                    super::crash_tests::pause(
                        "after_append_before_sync",
                        "complete frames and tail, no sync yet",
                    );
                    state.file.sync_data()?;
                    Ok::<(), Error>(())
                })();
                if let Err(error) = result {
                    state.poisoned = true;
                    return Err(error);
                }
                state.tip = tip;
                state.end = end;
                state.generation = generation;
                state.revision = revision;
                state.wal_bytes = state
                    .wal_bytes
                    .checked_add(bytes as u64)
                    .ok_or_else(|| full("WAL byte budget overflow"))?;
                state.recovered = None;
                Ok(tip)
            })
            .await
            .map_err(|e| unavailable(format!("WAL append task: {e}")))?
        })
    }
}

#[cfg(test)]
mod tests;

#[cfg(test)]
mod format_golden_tests;

#[cfg(test)]
mod format_version_tests;
