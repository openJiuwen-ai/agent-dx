//! Shared-service Meta operation log (R16): the `MetaLog` contract over a
//! durable key-value service. `redis_log` and `etcd_log` supply the service;
//! this module owns everything they share: key layout, framing, recovery,
//! fencing, checkpoint chunking and cleanup.
//!
//! Meta stays a single process (decision 2): the service only persists the
//! operation log and checkpoints. Writer exclusion is the term. Every writer
//! open atomically takes the next term; every append compares the term and the
//! tip in the same server-side atomic unit as its writes (a Redis script, an
//! etcd transaction), so a superseded writer can never write.
//!
//! Layout under the configured prefix `P`:
//!
//! | key | value |
//! |---|---|
//! | `P/layout` | `AFSK` + u16 BE layout version + 48-byte store identity |
//! | `P/term` | current writer term (decimal) |
//! | `P/tip` | sequence (u64 BE) + record hash: the last appended record |
//! | `P/wal/<seq, 20 digits>` | one framed record, exactly the local WAL frame |
//! | `P/checkpoint` | pointer to the selected checkpoint (see `Pointer`) |
//! | `P/chunk/<term>-<seq>/<index, 6 digits>` | checkpoint image chunks |
//! | `P/part/<term, 20 digits>/<seq, 20 digits>/<index, 6 digits>` | segments of a staged record |
//!
//! A record too large for one atomic request (an etcd transaction is bounded
//! by the server's request size) is staged: its frame is first written in
//! segments under the writer's term, invisible, and the atomic append then
//! stores a fixed-size stub (term, sequence, segment count, length, hash) in
//! `P/wal/<seq>`. Recovery reassembles the frame, checks its length and hash
//! and refuses with the frame and segment named when one is missing or
//! differs. Segments no committed stub references (a crash before the
//! append, a fenced writer) and segments of covered records are deleted at
//! the next open and after each checkpoint selection; a writer never deletes
//! segments of its own or a newer term that a live stub may still need.
//!
//! A checkpoint is written chunk by chunk, then selected by one atomic
//! pointer switch that compares the term and the previous pointer. Frames the
//! selected checkpoint covers and chunks of unselected checkpoints are
//! deleted afterwards; leftovers from a crash are deleted at the next open.
//! Chunks are only ever deleted when their writer term is older than the
//! deleting writer's, or when they belonged to the pointer it replaced, so a
//! stale writer cannot remove a newer writer's in-progress checkpoint.

use std::{
    future::Future,
    pin::Pin,
    sync::atomic::{AtomicBool, Ordering},
    time::Duration,
};

use afs_error::{Error, Result};

use super::{
    BackendPersistence, MetaFuture, StoreRevision,
    journal::{
        AppendLimits, BATCH_RECORDS_MAX, CheckpointLimits, LogCursor, LogStatus, LogTip, MetaLog,
        RecoveredCheckpoint, RecoveredLog, StoreIdentity, chain_term, decode_frame, encode_batch,
        fenced, full, invalid_log, unavailable,
    },
    log::{Record, StateImage},
};
use crate::format::{self, FormatKind};

pub type KvFuture<'a, T> = Pin<Box<dyn Future<Output = Result<T>> + Send + 'a>>;

/// Checkpoint image bound of the etcd backend.
pub const ETCD_IMAGE_MAX_BYTES: usize = 512 * 1024 * 1024;
/// Default uncheckpointed log bound of the etcd backend (`meta_wal_max_bytes`).
pub const ETCD_WAL_MAX_BYTES: u64 = 512 * 1024 * 1024;
/// Frames read per request during recovery and anchor checks.
pub(super) const FRAME_PAGE: u64 = 256;
/// Deadline of one service request. An append that misses it has an unknown
/// outcome and latches the handle.
pub(super) const REQUEST_TIMEOUT: Duration = Duration::from_secs(30);

/// Outcome of a compare-and-write step.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum Swap {
    Done,
    /// The current term differs; carries it.
    Fenced(u64),
    /// The term matched but the compared value (tip or pointer) changed.
    Changed,
}

/// Current term, tip and checkpoint pointer, read together.
#[derive(Clone, Debug, Default)]
pub struct Head {
    pub term: u64,
    pub tip: Option<Vec<u8>>,
    pub checkpoint: Option<Vec<u8>>,
}

/// One durable key-value service holding a Meta log under a key prefix. Each
/// method is one server request (or a bounded series for paged reads and
/// deletes); the compare-and-write methods are atomic on the server.
/// Implemented by `redis_log::RedisService` and `etcd_log::EtcdService` only.
pub trait KvService: Send + Sync + 'static {
    fn name(&self) -> &'static str;
    /// Largest framed group one atomic append may carry on this service.
    fn append_bound_bytes(&self) -> usize;
    /// Size of one checkpoint chunk.
    fn chunk_bytes(&self) -> usize;
    /// Atomically: when `P/layout` is absent and no other key exists under
    /// the prefix, store `layout` and term 1; when present, increment the
    /// term. Returns the stored layout and the new term.
    fn open_writer<'a>(&'a self, layout: &'a [u8]) -> KvFuture<'a, (Vec<u8>, u64)>;
    fn term(&self) -> KvFuture<'_, u64>;
    fn head(&self) -> KvFuture<'_, Head>;
    /// Atomically: if term == `term` and tip == `expected_tip` (`None` =
    /// absent), write `frames` at `first_sequence..` and set the tip.
    fn append<'a>(
        &'a self,
        term: u64,
        expected_tip: Option<&'a [u8]>,
        tip: &'a [u8],
        first_sequence: u64,
        frames: &'a [Vec<u8>],
    ) -> KvFuture<'a, Swap>;
    /// Frames `from..=through`, in order, `None` where absent.
    fn frames(&self, from: u64, through: u64) -> KvFuture<'_, Vec<Option<Vec<u8>>>>;
    fn put_chunk<'a>(&'a self, id: &'a str, index: u32, bytes: &'a [u8]) -> KvFuture<'a, ()>;
    fn chunk<'a>(&'a self, id: &'a str, index: u32) -> KvFuture<'a, Option<Vec<u8>>>;
    /// Atomically: if term == `term` and the pointer equals `expected`
    /// (`None` = absent), set it to `pointer`.
    fn switch_checkpoint<'a>(
        &'a self,
        term: u64,
        expected: Option<&'a [u8]>,
        pointer: &'a [u8],
    ) -> KvFuture<'a, Swap>;
    /// One segment of a staged record (not atomic with anything).
    fn put_part<'a>(
        &'a self,
        term: u64,
        sequence: u64,
        index: u32,
        bytes: &'a [u8],
    ) -> KvFuture<'a, ()>;
    fn part(&self, term: u64, sequence: u64, index: u32) -> KvFuture<'_, Option<Vec<u8>>>;
    /// Distinct `(term, sequence)` owners of stored segments.
    fn part_owners(&self) -> KvFuture<'_, Vec<(u64, u64)>>;
    fn delete_parts(&self, term: u64, sequence: u64) -> KvFuture<'_, ()>;
    /// Delete every frame with sequence `<= through`.
    fn delete_frames_through(&self, through: u64) -> KvFuture<'_, ()>;
    /// Distinct checkpoint ids that have chunks.
    fn chunk_ids(&self) -> KvFuture<'_, Vec<String>>;
    fn delete_chunks<'a>(&'a self, id: &'a str) -> KvFuture<'a, ()>;
    /// Test-only: every key and value under the prefix, sorted.
    #[cfg(test)]
    fn dump(&self) -> KvFuture<'_, Vec<(String, Vec<u8>)>>;
}

/// Bounds of a key-value log, from the Meta log configuration and the
/// service's own request limit.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct KvLogOptions {
    pub record_max_bytes: usize,
    pub batch_max_bytes: usize,
    /// Uncheckpointed framed bytes; an append beyond it fails `full`.
    pub wal_max_bytes: u64,
    pub image_max_bytes: usize,
}

impl KvLogOptions {
    fn validate(self) -> Result<Self> {
        AppendLimits {
            record_max_bytes: self.record_max_bytes,
            batch_max_bytes: self.batch_max_bytes,
            batch_records_max: BATCH_RECORDS_MAX,
        }
        .validate()?;
        CheckpointLimits {
            image_max_bytes: self.image_max_bytes,
        }
        .validate()?;
        if u64::try_from(self.batch_max_bytes).map_or(true, |batch| batch > self.wal_max_bytes) {
            return Err(invalid_log("invalid finite record/batch/WAL limits"));
        }
        Ok(self)
    }

    fn append(self) -> AppendLimits {
        AppendLimits {
            record_max_bytes: self.record_max_bytes,
            batch_max_bytes: self.batch_max_bytes,
            batch_records_max: BATCH_RECORDS_MAX,
        }
    }
}

pub(super) fn frame_key(prefix: &str, sequence: u64) -> String {
    format!("{prefix}/wal/{sequence:020}")
}

pub(super) fn chunk_key(prefix: &str, id: &str, index: u32) -> String {
    format!("{prefix}/chunk/{id}/{index:06}")
}

/// `P/chunk/<id>/...` → `<id>`.
pub(super) fn chunk_id_of(prefix: &str, key: &str) -> Option<String> {
    let rest = key.strip_prefix(prefix)?.strip_prefix("/chunk/")?;
    let (id, _) = rest.split_once('/')?;
    Some(id.to_owned())
}

pub(super) fn part_key(prefix: &str, term: u64, sequence: u64, index: u32) -> String {
    format!("{prefix}/part/{term:020}/{sequence:020}/{index:06}")
}

/// `P/part/<term>/<seq>/...` → `(term, seq)`.
pub(super) fn part_owner_of(prefix: &str, key: &str) -> Option<(u64, u64)> {
    let mut rest = key.strip_prefix(prefix)?.strip_prefix("/part/")?.split('/');
    Some((rest.next()?.parse().ok()?, rest.next()?.parse().ok()?))
}

/// The value stored in `P/wal/<seq>` for a staged record. A frame starts with
/// its big-endian length, so a 68-byte value starting with this magic would
/// claim a length above 1 GiB and can never be a frame.
const STUB_MAGIC: &[u8; 8] = b"AFSKPART";
const STUB_BYTES: usize = 8 + 8 + 8 + 4 + 8 + 32;

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
struct Stub {
    term: u64,
    sequence: u64,
    parts: u32,
    length: u64,
    hash: [u8; 32],
}

fn staged_hash(frame: &[u8]) -> [u8; 32] {
    let mut hash = blake3::Hasher::new();
    hash.update(b"afs.meta.kv.part.v1");
    hash.update(frame);
    *hash.finalize().as_bytes()
}

impl Stub {
    fn encode(&self) -> Vec<u8> {
        let mut bytes = Vec::with_capacity(STUB_BYTES);
        bytes.extend_from_slice(STUB_MAGIC);
        bytes.extend_from_slice(&self.term.to_be_bytes());
        bytes.extend_from_slice(&self.sequence.to_be_bytes());
        bytes.extend_from_slice(&self.parts.to_be_bytes());
        bytes.extend_from_slice(&self.length.to_be_bytes());
        bytes.extend_from_slice(&self.hash);
        bytes
    }

    fn decode(bytes: &[u8]) -> Option<Self> {
        if bytes.len() != STUB_BYTES || &bytes[..8] != STUB_MAGIC {
            return None;
        }
        let number = |range: std::ops::Range<usize>| -> Option<u64> {
            Some(u64::from_be_bytes(bytes[range].try_into().ok()?))
        };
        let mut hash = [0; 32];
        hash.copy_from_slice(&bytes[36..68]);
        Some(Self {
            term: number(8..16)?,
            sequence: number(16..24)?,
            parts: u32::from_be_bytes(bytes[24..28].try_into().ok()?),
            length: number(28..36)?,
            hash,
        })
    }
}

/// Test-only crash points inside a staged append, by store identity.
#[cfg(test)]
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(super) enum CrashPoint {
    /// After the first segment of the first staged record.
    PartsPartial,
    /// After every segment, before the atomic append.
    PartsWritten,
    /// After the atomic append succeeded, before the handle records it.
    AfterCommit,
}
#[cfg(test)]
static CRASH: std::sync::Mutex<Vec<(StoreIdentity, CrashPoint)>> =
    std::sync::Mutex::new(Vec::new());
#[cfg(test)]
pub(super) fn arm_crash(identity: StoreIdentity, point: CrashPoint) {
    if let Ok(mut armed) = CRASH.lock() {
        armed.push((identity, point));
    }
}
#[cfg(test)]
fn crash_at(identity: StoreIdentity, point: CrashPoint) -> bool {
    CRASH.lock().is_ok_and(|mut armed| {
        armed
            .iter()
            .position(|entry| *entry == (identity, point))
            .map(|index| armed.swap_remove(index))
            .is_some()
    })
}

const LAYOUT_BYTES: usize = 6 + 48;

fn encode_layout(identity: StoreIdentity) -> Vec<u8> {
    let mut bytes = Vec::with_capacity(LAYOUT_BYTES);
    bytes.extend_from_slice(&format::tagged_header(
        format::META_KV_LAYOUT_MAGIC,
        format::version::META_KV_LAYOUT,
    ));
    bytes.extend_from_slice(&identity.store_id);
    bytes.extend_from_slice(&identity.cluster_id);
    bytes.extend_from_slice(&identity.incarnation);
    bytes
}

fn decode_layout(service: &str, bytes: &[u8]) -> Result<StoreIdentity> {
    // Version first, before the length and identity.
    format::check_tagged_header(
        FormatKind::MetaKvLayout,
        format::META_KV_LAYOUT_MAGIC,
        format_args!("{service} layout key"),
        bytes,
    )?;
    if bytes.len() != LAYOUT_BYTES || bytes[..4] != format::META_KV_LAYOUT_MAGIC {
        return Err(invalid_log(format!(
            "{service} layout key is not an AFS Meta log"
        )));
    }
    let mut identity = StoreIdentity {
        store_id: [0; 16],
        cluster_id: [0; 16],
        incarnation: [0; 16],
    };
    identity.store_id.copy_from_slice(&bytes[6..22]);
    identity.cluster_id.copy_from_slice(&bytes[22..38]);
    identity.incarnation.copy_from_slice(&bytes[38..54]);
    Ok(identity)
}

fn encode_tip(tip: LogTip) -> Vec<u8> {
    let mut bytes = Vec::with_capacity(40);
    bytes.extend_from_slice(&tip.sequence.to_be_bytes());
    bytes.extend_from_slice(&tip.record_hash);
    bytes
}

fn decode_tip(bytes: Option<&[u8]>) -> Result<LogTip> {
    let Some(bytes) = bytes else {
        return Ok(LogTip::EMPTY);
    };
    if bytes.len() != 40 {
        return Err(invalid_log("tip key length"));
    }
    let mut tip = LogTip {
        sequence: u64::from_be_bytes(bytes[..8].try_into().map_err(|_| invalid_log("tip"))?),
        record_hash: [0; 32],
    };
    tip.record_hash.copy_from_slice(&bytes[8..]);
    Ok(tip)
}

/// The selected checkpoint: covered position, business revision, image
/// length, digest, chunk count and the writer term that wrote it.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
struct Pointer {
    covered: LogTip,
    revision: StoreRevision,
    bytes: u64,
    digest: [u8; 32],
    chunks: u32,
    term: u64,
}

const POINTER_BYTES: usize = 8 + 32 + 8 + 8 + 32 + 4 + 8;

impl Pointer {
    fn id(&self) -> String {
        format!("{}-{}", self.term, self.covered.sequence)
    }

    fn encode(&self) -> Vec<u8> {
        let mut bytes = Vec::with_capacity(POINTER_BYTES);
        bytes.extend_from_slice(&encode_tip(self.covered));
        bytes.extend_from_slice(&self.revision.0.to_be_bytes());
        bytes.extend_from_slice(&self.bytes.to_be_bytes());
        bytes.extend_from_slice(&self.digest);
        bytes.extend_from_slice(&self.chunks.to_be_bytes());
        bytes.extend_from_slice(&self.term.to_be_bytes());
        bytes
    }

    fn decode(bytes: &[u8]) -> Result<Self> {
        if bytes.len() != POINTER_BYTES {
            return Err(invalid_log("checkpoint pointer length"));
        }
        let number = |range: std::ops::Range<usize>| -> Result<u64> {
            Ok(u64::from_be_bytes(
                bytes[range]
                    .try_into()
                    .map_err(|_| invalid_log("checkpoint pointer field"))?,
            ))
        };
        let mut digest = [0; 32];
        digest.copy_from_slice(&bytes[56..88]);
        Ok(Self {
            covered: decode_tip(Some(&bytes[..40]))?,
            revision: StoreRevision(number(40..48)?),
            bytes: number(48..56)?,
            digest,
            chunks: u32::from_be_bytes(
                bytes[88..92]
                    .try_into()
                    .map_err(|_| invalid_log("checkpoint pointer field"))?,
            ),
            term: number(92..100)?,
        })
    }
}

fn image_digest(bytes: &[u8]) -> [u8; 32] {
    let mut hash = blake3::Hasher::new();
    hash.update(b"afs.meta.kv.checkpoint.v1");
    hash.update(bytes);
    *hash.finalize().as_bytes()
}

/// `id` = `<term>-<seq>`; the writer term that wrote the chunks.
fn chunk_term(id: &str) -> Option<u64> {
    id.split_once('-')?.0.parse().ok()
}

async fn timed<T>(what: &str, future: impl Future<Output = Result<T>>) -> Result<T> {
    tokio::time::timeout(REQUEST_TIMEOUT, future)
        .await
        .map_err(|_| unavailable(format!("Meta log {what}: request deadline exceeded")))?
}

struct KvState {
    /// Committed staged records after the checkpoint.
    staged: Staged,
    tip: LogTip,
    checkpoint: Option<Pointer>,
    /// Framed bytes after the checkpoint, exact.
    wal_bytes: u64,
    recovered: Option<RecoveredLog>,
}

/// The Redis Meta log.
pub type RedisLog = KvLog<super::redis_log::RedisService>;
/// The etcd Meta log.
pub type EtcdLog = KvLog<super::etcd_log::EtcdService>;

impl RedisLog {
    pub async fn open_redis(url: &str, prefix: &str, options: KvLogOptions) -> Result<Self> {
        Self::open(
            super::redis_log::RedisService::connect(url, prefix).await?,
            options,
        )
        .await
    }
}

impl EtcdLog {
    pub async fn open_etcd(
        endpoints: &[String],
        prefix: &str,
        max_request_bytes: usize,
        options: KvLogOptions,
    ) -> Result<Self> {
        let service =
            super::etcd_log::EtcdService::connect(endpoints, prefix, max_request_bytes).await?;
        match service.quota_bytes().await {
            Ok(Some(quota)) => {
                if let Some(warning) = etcd_quota_warning(quota, options) {
                    afs_logging::warn!("meta.log.etcd_quota_low"; "detail" => warning);
                }
            }
            Ok(None) => {}
            Err(error) => afs_logging::warn!("meta.log.etcd_quota_unknown";
                "error" => error.to_string()),
        }
        Self::open(service, options).await
    }
}

/// Room etcd needs beside the log: two images (old and new during a switch),
/// the uncheckpointed log and 1 GiB for MVCC history until compaction. A
/// smaller quota is reported, not refused: etcd turns read-only when full.
pub(super) fn etcd_quota_warning(quota: u64, options: KvLogOptions) -> Option<String> {
    let needed = (options.image_max_bytes as u64)
        .saturating_mul(2)
        .saturating_add(options.wal_max_bytes)
        .saturating_add(1 << 30);
    (quota < needed).then(|| {
        format!(
            "etcd --quota-backend-bytes {quota} is below {needed} (two checkpoint images of {} bytes, a {}-byte log and 1 GiB of history); raise it (8 GiB recommended) and enable auto-compaction",
            options.image_max_bytes, options.wal_max_bytes
        )
    })
}

/// The `MetaLog` over a key-value service.
pub struct KvLog<S: KvService> {
    service: S,
    identity: StoreIdentity,
    term: u64,
    options: KvLogOptions,
    state: tokio::sync::Mutex<KvState>,
    install: tokio::sync::Mutex<()>,
    poisoned: AtomicBool,
}

impl<S: KvService> KvLog<S> {
    /// Open as the next writer: take a term, recover, then delete leftovers
    /// a crash may have left (covered frames, unselected chunks of older
    /// writers).
    pub async fn open(service: S, options: KvLogOptions) -> Result<Self> {
        let options = options.validate()?;
        let fresh = encode_layout(StoreIdentity::new()?);
        let (stored, term) = timed("open", service.open_writer(&fresh)).await?;
        let identity = decode_layout(service.name(), &stored)?;
        let head = timed("open", service.head()).await?;
        if head.term != term {
            return Err(fenced(term, head.term));
        }
        let (recovered, checkpoint, wal_bytes, staged) =
            recover(&service, identity, term, &head, options).await?;
        if let Some(pointer) = checkpoint {
            timed(
                "cleanup",
                service.delete_frames_through(pointer.covered.sequence),
            )
            .await?;
        }
        cleanup_parts(
            &service,
            term,
            &staged,
            checkpoint.map_or(0, |pointer| pointer.covered.sequence),
        )
        .await?;
        let selected = checkpoint.map(|pointer| pointer.id());
        for id in timed("cleanup", service.chunk_ids()).await? {
            if Some(&id) != selected.as_ref() && chunk_term(&id).is_none_or(|owner| owner < term) {
                timed("cleanup", service.delete_chunks(&id)).await?;
            }
        }
        Ok(Self {
            service,
            identity,
            term,
            options,
            state: tokio::sync::Mutex::new(KvState {
                staged,
                tip: recovered.tip,
                checkpoint,
                wal_bytes,
                recovered: Some(recovered),
            }),
            install: tokio::sync::Mutex::new(()),
            poisoned: AtomicBool::new(false),
        })
    }

    pub fn identity(&self) -> StoreIdentity {
        self.identity
    }

    /// The writer term this handle holds.
    pub fn term(&self) -> u64 {
        self.term
    }

    /// Test-only: model a write whose outcome is unknown: nothing is sent and
    /// the next append, like every later operation, fails until reopened.
    #[cfg(test)]
    pub(super) fn inject_write_failure(&self) {
        self.poisoned.store(true, Ordering::Release);
    }

    fn check_latch(&self) -> Result<()> {
        if self.poisoned.load(Ordering::Acquire) {
            return Err(unavailable(format!(
                "{} Meta log write outcome unknown; reopen required",
                self.service.name()
            )));
        }
        Ok(())
    }

    fn poison(&self, error: Error) -> Error {
        self.poisoned.store(true, Ordering::Release);
        error
    }

    /// Fail `META_LOG_FENCED` once a newer writer took the log.
    async fn check_term(&self) -> Result<()> {
        self.check_latch()?;
        let current = timed("term", self.service.term()).await?;
        if current != self.term {
            return Err(fenced(self.term, current));
        }
        Ok(())
    }
}

impl<S: KvService> KvLog<S> {
    /// The values of one atomic append. A frame larger than one segment, or
    /// one that no longer fits the service's request bound beside the frames
    /// before it, is written in segments under this writer's term first and
    /// replaced by its stub. Returns the values and the staged sequences.
    async fn stage(&self, first: u64, frames: Vec<Vec<u8>>) -> Result<(Vec<Vec<u8>>, Vec<u64>)> {
        let segment = self.service.chunk_bytes().max(1);
        let bound = self
            .service
            .append_bound_bytes()
            .saturating_sub(frames.len() * STUB_BYTES);
        let mut inline = 0usize;
        let mut values = Vec::with_capacity(frames.len());
        let mut staged = Vec::new();
        for (offset, frame) in frames.into_iter().enumerate() {
            if frame.len() <= segment && inline + frame.len() <= bound {
                inline += frame.len();
                values.push(frame);
                continue;
            }
            if staged.is_empty() {
                // A superseded writer stages nothing.
                self.check_term().await?;
            }
            let sequence = first + offset as u64;
            let parts = u32::try_from(frame.len().div_ceil(segment))
                .map_err(|_| invalid_log("staged record segment count overflow"))?;
            for (index, part) in frame.chunks(segment).enumerate() {
                let index =
                    u32::try_from(index).map_err(|_| invalid_log("segment index overflow"))?;
                if let Err(error) = timed(
                    "stage",
                    self.service.put_part(self.term, sequence, index, part),
                )
                .await
                {
                    return Err(self.poison(error));
                }
                #[cfg(test)]
                if index == 0
                    && staged.is_empty()
                    && crash_at(self.identity, CrashPoint::PartsPartial)
                {
                    return Err(self.poison(unavailable("injected crash: one segment written")));
                }
            }
            values.push(
                Stub {
                    term: self.term,
                    sequence,
                    parts,
                    length: frame.len() as u64,
                    hash: staged_hash(&frame),
                }
                .encode(),
            );
            staged.push(sequence);
        }
        Ok((values, staged))
    }
}

/// Read the checkpoint (if any) and every frame after it, verifying the
/// image digest and revision, the hash chain, terms and business revisions.
async fn recover<S: KvService>(
    service: &S,
    identity: StoreIdentity,
    term: u64,
    head: &Head,
    options: KvLogOptions,
) -> Result<(RecoveredLog, Option<Pointer>, u64, Staged)> {
    let tip = decode_tip(head.tip.as_deref())?;
    let pointer = head
        .checkpoint
        .as_deref()
        .map(Pointer::decode)
        .transpose()?;
    let checkpoint = match pointer {
        Some(pointer) => Some(read_image(service, &pointer, options).await?),
        None => None,
    };
    let (base, revision) = pointer.map_or((LogTip::EMPTY, StoreRevision::ZERO), |pointer| {
        (pointer.covered, pointer.revision)
    });
    if tip.sequence < base.sequence {
        return Err(invalid_log("tip precedes the selected checkpoint"));
    }
    let (records, end, _, bytes, staged) = read_chain(
        service,
        identity,
        term,
        base,
        revision,
        tip.sequence,
        options,
    )
    .await?;
    if end != tip {
        return Err(invalid_log(
            "recovered chain does not end at the stored tip",
        ));
    }
    Ok((
        RecoveredLog {
            checkpoint,
            tip,
            term,
            records,
        },
        pointer,
        bytes,
        staged,
    ))
}

async fn read_image<S: KvService>(
    service: &S,
    pointer: &Pointer,
    options: KvLogOptions,
) -> Result<RecoveredCheckpoint> {
    let length =
        usize::try_from(pointer.bytes).map_err(|_| invalid_log("checkpoint length overflow"))?;
    if length > options.image_max_bytes {
        return Err(invalid_log("checkpoint length outside declared bound"));
    }
    let mut bytes = Vec::new();
    bytes
        .try_reserve_exact(length)
        .map_err(|_| Error::coded(afs_error::IO_OUT_OF_MEMORY, "checkpoint payload allocation"))?;
    let id = pointer.id();
    for index in 0..pointer.chunks {
        let chunk = timed("checkpoint read", service.chunk(&id, index))
            .await?
            .ok_or_else(|| invalid_log(format!("checkpoint {id} chunk {index} missing")))?;
        if bytes.len() + chunk.len() > length {
            return Err(invalid_log(format!("checkpoint {id} longer than declared")));
        }
        bytes.extend_from_slice(&chunk);
    }
    if bytes.len() != length || image_digest(&bytes) != pointer.digest {
        return Err(invalid_log(format!(
            "checkpoint {id} length/digest mismatch"
        )));
    }
    if StateImage::validate_encoded(&bytes, options.image_max_bytes)? != pointer.revision {
        return Err(invalid_log("checkpoint header/business revision mismatch"));
    }
    Ok(RecoveredCheckpoint {
        covered: pointer.covered,
        bytes,
    })
}

/// Decode frames `base.sequence + 1 ..= through` as one chain from `base`.
/// Returns the records, the reached tip, the business revision after them
/// and their framed bytes.
async fn read_chain<S: KvService>(
    service: &S,
    identity: StoreIdentity,
    term: u64,
    base: LogTip,
    revision: StoreRevision,
    through: u64,
    options: KvLogOptions,
) -> Result<(Vec<Record>, LogTip, StoreRevision, u64, Staged)> {
    let mut staged = Staged::new();
    let mut tip = base;
    let mut revision = revision;
    let mut chain = 0;
    let mut records = Vec::new();
    let mut bytes = 0u64;
    let mut next = base.sequence + 1;
    while next <= through {
        let last = through.min(next + FRAME_PAGE - 1);
        let page = timed("recover", service.frames(next, last)).await?;
        if page.len() as u64 != last - next + 1 {
            return Err(invalid_log("frame page length"));
        }
        for (offset, frame) in page.into_iter().enumerate() {
            let sequence = next + offset as u64;
            let frame = frame
                .ok_or_else(|| invalid_log(format!("frame {sequence} missing before the tip")))?;
            let (frame, owner) =
                resolve_frame(service, sequence, frame, options.record_max_bytes).await?;
            if let Some(owner) = owner {
                staged.insert(sequence, owner);
            }
            let (record, reached, found) =
                decode_frame(identity, tip, &frame, options.record_max_bytes)?;
            chain = chain_term(chain, found, term)?;
            if record.revision_before() != revision {
                return Err(invalid_log(format!(
                    "frame {sequence} business revision gap"
                )));
            }
            revision = record.revision_after();
            bytes = bytes
                .checked_add(frame.len() as u64)
                .ok_or_else(|| invalid_log("frame byte charge overflow"))?;
            records.push(record);
            tip = reached;
        }
        next = last + 1;
    }
    Ok((records, tip, revision, bytes, staged))
}

/// Committed staged records after the checkpoint: sequence → writer term.
type Staged = std::collections::BTreeMap<u64, u64>;

/// The frame stored at `sequence`: the value itself, or the record its
/// staged stub names, reassembled and checked. Returns the stub's term.
async fn resolve_frame<S: KvService>(
    service: &S,
    sequence: u64,
    value: Vec<u8>,
    record_max: usize,
) -> Result<(Vec<u8>, Option<u64>)> {
    let Some(stub) = Stub::decode(&value) else {
        return Ok((value, None));
    };
    let at = |detail: String| {
        invalid_log(format!(
            "frame {sequence} (staged by term {}, {} segments): {detail}",
            stub.term, stub.parts
        ))
    };
    if stub.sequence != sequence {
        return Err(at(format!("stub names sequence {}", stub.sequence)));
    }
    let length = usize::try_from(stub.length)
        .ok()
        .filter(|length| *length <= record_max)
        .ok_or_else(|| at(format!("length {} outside the record bound", stub.length)))?;
    let mut frame = Vec::new();
    frame
        .try_reserve_exact(length)
        .map_err(|_| Error::coded(afs_error::IO_OUT_OF_MEMORY, "staged record allocation"))?;
    for index in 0..stub.parts {
        let part = timed("recover", service.part(stub.term, sequence, index))
            .await?
            .ok_or_else(|| at(format!("segment {index} missing")))?;
        if frame.len() + part.len() > length {
            return Err(at(format!("segment {index} runs past the recorded length")));
        }
        frame.extend_from_slice(&part);
    }
    if frame.len() != length {
        return Err(at(format!(
            "segments hold {} of {length} bytes",
            frame.len()
        )));
    }
    if staged_hash(&frame) != stub.hash {
        return Err(at("segments do not match the recorded hash".into()));
    }
    Ok((frame, Some(stub.term)))
}

/// Delete segments no committed stub needs: those of covered records, and
/// unreferenced ones written by an older term (a crash before the append, a
/// fenced writer). Unreferenced segments of `term` itself may belong to an
/// append still in progress and are kept.
async fn cleanup_parts<S: KvService>(
    service: &S,
    term: u64,
    staged: &Staged,
    covered: u64,
) -> Result<usize> {
    let mut removed = 0;
    for (owner, sequence) in timed("cleanup", service.part_owners()).await? {
        let live = sequence > covered && staged.get(&sequence) == Some(&owner);
        if !live && (owner < term || sequence <= covered) {
            timed("cleanup", service.delete_parts(owner, sequence)).await?;
            removed += 1;
        }
    }
    Ok(removed)
}

impl<S: KvService> MetaLog for KvLog<S> {
    fn persistence(&self) -> BackendPersistence {
        BackendPersistence::Persistent
    }

    fn limits(&self) -> AppendLimits {
        self.options.append()
    }

    fn checkpoint_limits(&self) -> CheckpointLimits {
        CheckpointLimits {
            image_max_bytes: self.options.image_max_bytes,
        }
    }

    fn supports_checkpoint(&self) -> bool {
        true
    }

    fn health(&self) -> MetaFuture<'_, ()> {
        Box::pin(async move { self.check_term().await })
    }

    fn status(&self) -> MetaFuture<'_, LogStatus> {
        Box::pin(async move {
            self.check_term().await?;
            let state = self.state.lock().await;
            Ok(LogStatus {
                tip: state.tip,
                checkpoint: state
                    .checkpoint
                    .map_or(LogTip::EMPTY, |pointer| pointer.covered),
                uncheckpointed_bytes_upper_bound: state.wal_bytes,
            })
        })
    }

    fn can_append(&self, expected: LogCursor, framed_bytes: usize) -> MetaFuture<'_, bool> {
        Box::pin(async move {
            self.check_term().await?;
            if expected.term != self.term {
                return Err(fenced(expected.term, self.term));
            }
            let state = self.state.lock().await;
            if expected.tip != state.tip {
                return Err(unavailable("Meta log admission tip changed"));
            }
            Ok(state
                .wal_bytes
                .checked_add(framed_bytes as u64)
                .is_some_and(|bytes| bytes <= self.options.wal_max_bytes))
        })
    }

    fn recover(&self) -> MetaFuture<'_, RecoveredLog> {
        Box::pin(async move {
            self.check_term().await?;
            let mut state = self.state.lock().await;
            if let Some(recovered) = state.recovered.take() {
                return Ok(recovered);
            }
            drop(state);
            let head = timed("recover", self.service.head()).await?;
            if head.term != self.term {
                return Err(fenced(self.term, head.term));
            }
            Ok(
                recover(&self.service, self.identity, self.term, &head, self.options)
                    .await?
                    .0,
            )
        })
    }

    fn install_checkpoint(&self, covered: LogTip, bytes: Vec<u8>) -> MetaFuture<'_, ()> {
        Box::pin(async move {
            self.check_term().await?;
            let _install = self
                .install
                .try_lock()
                .map_err(|_| unavailable("checkpoint install already active"))?;
            let revision = StateImage::validate_encoded(&bytes, self.options.image_max_bytes)?;
            let (old, tip) = {
                let state = self.state.lock().await;
                (state.checkpoint, state.tip)
            };
            let (base, base_revision) = old.map_or((LogTip::EMPTY, StoreRevision::ZERO), |old| {
                (old.covered, old.revision)
            });
            if covered.sequence > tip.sequence {
                return Err(invalid_log("checkpoint covers future log sequence"));
            }
            if covered.sequence < base.sequence {
                return Err(invalid_log("checkpoint installation regresses the pointer"));
            }
            let digest = image_digest(&bytes);
            if covered.sequence == base.sequence {
                return match old {
                    Some(old)
                        if old.covered == covered
                            && old.digest == digest
                            && old.bytes == bytes.len() as u64 =>
                    {
                        Ok(())
                    }
                    Some(_) => Err(invalid_log("same checkpoint position has different state")),
                    None => Err(invalid_log("checkpoint covers no record")),
                };
            }
            let (_, reached, reached_revision, released, _) = read_chain(
                &self.service,
                self.identity,
                self.term,
                base,
                base_revision,
                covered.sequence,
                self.options,
            )
            .await?;
            if reached != covered || reached_revision != revision {
                return Err(invalid_log("checkpoint anchor/revision mismatch"));
            }
            let chunk = self.service.chunk_bytes().max(1);
            let chunks = u32::try_from(bytes.len().div_ceil(chunk))
                .map_err(|_| invalid_log("checkpoint chunk count overflow"))?;
            let pointer = Pointer {
                covered,
                revision,
                bytes: bytes.len() as u64,
                digest,
                chunks,
                term: self.term,
            };
            let id = pointer.id();
            for (index, part) in bytes.chunks(chunk).enumerate() {
                let index = u32::try_from(index)
                    .map_err(|_| invalid_log("checkpoint chunk count overflow"))?;
                timed("checkpoint write", self.service.put_chunk(&id, index, part)).await?;
            }
            let expected = old.map(|old| old.encode());
            let encoded = pointer.encode();
            match timed(
                "checkpoint select",
                self.service
                    .switch_checkpoint(self.term, expected.as_deref(), &encoded),
            )
            .await
            {
                Ok(Swap::Done) => {}
                Ok(Swap::Fenced(current)) => return Err(fenced(self.term, current)),
                Ok(Swap::Changed) => {
                    return Err(self.poison(invalid_log(
                        "checkpoint pointer changed under the current writer",
                    )));
                }
                // The switch may have been applied: this view is stale.
                Err(error) => return Err(self.poison(error)),
            }
            let staged = {
                let mut state = self.state.lock().await;
                state.checkpoint = Some(pointer);
                state.wal_bytes = state.wal_bytes.saturating_sub(released);
                state.recovered = None;
                state.staged = state.staged.split_off(&(covered.sequence + 1));
                state.staged.clone()
            };
            // Selected: the covered frames and the replaced image are
            // obsolete. A failure here only leaves garbage for the next open.
            if let Err(error) = timed(
                "cleanup",
                self.service.delete_frames_through(covered.sequence),
            )
            .await
            {
                afs_logging::warn!("meta.log.cleanup_deferred";
                    "backend" => self.service.name(),
                    "error" => error.to_string());
            }
            if let Err(error) =
                cleanup_parts(&self.service, self.term, &staged, covered.sequence).await
            {
                afs_logging::warn!("meta.log.cleanup_deferred";
                    "backend" => self.service.name(),
                    "error" => error.to_string());
            }
            if let Some(old) = old
                && let Err(error) = timed("cleanup", self.service.delete_chunks(&old.id())).await
            {
                afs_logging::warn!("meta.log.cleanup_deferred";
                    "backend" => self.service.name(),
                    "error" => error.to_string());
            }
            Ok(())
        })
    }

    fn append_and_sync(&self, expected: LogCursor, records: Vec<Record>) -> MetaFuture<'_, LogTip> {
        Box::pin(async move {
            self.check_latch()?;
            if expected.term != self.term {
                return Err(fenced(expected.term, self.term));
            }
            let mut state = self.state.lock().await;
            if state.tip != expected.tip {
                // A superseded handle reports the fence, not the stale tip.
                self.check_term().await?;
                return Err(unavailable("Meta log compare-and-append tip changed"));
            }
            let (frames, tip, bytes) =
                encode_batch(self.identity, expected, records, self.options.append())?;
            let total = state
                .wal_bytes
                .checked_add(bytes as u64)
                .filter(|total| *total <= self.options.wal_max_bytes)
                .ok_or_else(|| full("Meta log byte budget exhausted; checkpoint required"))?;
            let first = expected.tip.sequence + 1;
            let (values, staged) = self.stage(first, frames).await?;
            #[cfg(test)]
            if !staged.is_empty() && crash_at(self.identity, CrashPoint::PartsWritten) {
                return Err(self.poison(unavailable("injected crash: segments written")));
            }
            let expected_tip = (expected.tip != LogTip::EMPTY).then(|| encode_tip(expected.tip));
            let outcome = timed(
                "append",
                self.service.append(
                    self.term,
                    expected_tip.as_deref(),
                    &encode_tip(tip),
                    first,
                    &values,
                ),
            )
            .await;
            match outcome {
                Ok(Swap::Done) => {
                    #[cfg(test)]
                    if crash_at(self.identity, CrashPoint::AfterCommit) {
                        return Err(self.poison(unavailable("injected crash: appended")));
                    }
                    for sequence in staged {
                        state.staged.insert(sequence, self.term);
                    }
                    state.tip = tip;
                    state.wal_bytes = total;
                    state.recovered = None;
                    Ok(tip)
                }
                Ok(Swap::Fenced(current)) => Err(fenced(self.term, current)),
                Ok(Swap::Changed) => {
                    Err(self.poison(unavailable("Meta log tip changed under the current writer")))
                }
                Err(error) => Err(self.poison(error)),
            }
        })
    }
}

#[cfg(test)]
pub(super) mod tests {
    use super::*;

    #[test]
    fn layout_and_pointer_round_trip_and_layout_version_is_checked_first() {
        let identity = StoreIdentity::new().unwrap();
        let layout = encode_layout(identity);
        assert_eq!(&layout[..6], b"AFSK\0\x01");
        assert_eq!(decode_layout("redis", &layout).unwrap(), identity);
        let mut newer = layout.clone();
        newer[5] = 2;
        let error = decode_layout("redis", &newer).unwrap_err();
        assert_eq!(error.code(), afs_error::STORAGE_FORMAT_INCOMPATIBLE);
        assert!(decode_layout("redis", b"other").is_err());
        let pointer = Pointer {
            covered: LogTip {
                sequence: 7,
                record_hash: [3; 32],
            },
            revision: StoreRevision(9),
            bytes: 1234,
            digest: [5; 32],
            chunks: 2,
            term: 4,
        };
        assert_eq!(Pointer::decode(&pointer.encode()).unwrap(), pointer);
        assert_eq!(pointer.id(), "4-7");
        assert_eq!(chunk_term("4-7"), Some(4));
        assert_eq!(
            chunk_id_of("p", "p/chunk/4-7/000001").as_deref(),
            Some("4-7")
        );
        assert_eq!(chunk_id_of("p", "p/wal/1"), None);
    }

    #[test]
    fn options_validate_and_stubs_round_trip() {
        let options = KvLogOptions {
            record_max_bytes: 1024,
            batch_max_bytes: 2048,
            wal_max_bytes: 1 << 20,
            image_max_bytes: 1 << 20,
        };
        assert!(options.validate().is_ok());
        assert!(
            KvLogOptions {
                wal_max_bytes: 1024,
                ..options
            }
            .validate()
            .is_err()
        );
        let small = KvLogOptions {
            record_max_bytes: 1 << 20,
            batch_max_bytes: 1 << 20,
            wal_max_bytes: 512 << 20,
            image_max_bytes: 512 << 20,
        };
        assert!(
            etcd_quota_warning(2 << 30, small).is_some(),
            "etcd default 2 GiB"
        );
        assert!(etcd_quota_warning(8 << 30, small).is_none());
        let stub = Stub {
            term: 3,
            sequence: 9,
            parts: 4,
            length: 4 << 20,
            hash: [7; 32],
        };
        assert_eq!(Stub::decode(&stub.encode()), Some(stub));
        assert_eq!(Stub::decode(&stub.encode()[1..]), None);
        assert_eq!(
            part_key("p", 3, 9, 1),
            "p/part/00000000000000000003/00000000000000000009/000001"
        );
        assert_eq!(part_owner_of("p", &part_key("p", 3, 9, 1)), Some((3, 9)));
    }
}
