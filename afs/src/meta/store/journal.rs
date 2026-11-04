//! Bounded operation-log contract and common identity/hash/CRC framing.
//! Sequence is a physical log position, independent of business revision.
use super::{BackendPersistence, MetaFuture, codec, log::Record};
use afs_error::{Error, Result};
use serde::{Deserialize, Serialize};
use std::{fs::File, io::Read, sync::Mutex};

pub const FRAME_OVERHEAD: usize = 40;
/// Upper bound on records in one atomic `append_and_sync` group. Every backend
/// accepts at most this many; Store gathers at most this many ready requests.
pub const BATCH_RECORDS_MAX: usize = 64;
/// The frame envelope version (`FormatKind::MetaWalEnvelope`); the record
/// hash domain `afs.meta.record.v2` belongs to it.
const VERSION: u16 = crate::format::version::META_WAL_ENVELOPE;

#[derive(Clone, Copy, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub struct StoreIdentity {
    pub store_id: [u8; 16],
    pub cluster_id: [u8; 16],
    pub incarnation: [u8; 16],
}
impl StoreIdentity {
    pub fn new() -> Result<Self> {
        let mut bytes = [0u8; 48];
        File::open("/dev/urandom")?.read_exact(&mut bytes)?;
        let mut identity = Self {
            store_id: [0; 16],
            cluster_id: [0; 16],
            incarnation: [0; 16],
        };
        identity.store_id.copy_from_slice(&bytes[..16]);
        identity.cluster_id.copy_from_slice(&bytes[16..32]);
        identity.incarnation.copy_from_slice(&bytes[32..]);
        Ok(identity)
    }
}
#[derive(Clone, Copy, Debug, Default, Deserialize, Eq, PartialEq, Serialize)]
pub struct LogTip {
    pub sequence: u64,
    pub record_hash: [u8; 32],
}
impl LogTip {
    pub const EMPTY: Self = Self {
        sequence: 0,
        record_hash: [0; 32],
    };
    /// This tip as seen by a writer holding `term`.
    pub const fn at(self, term: u64) -> LogCursor {
        LogCursor { tip: self, term }
    }
}
/// A writer's append position: the expected tip and the writer term it holds.
/// Backends accept appends only at the current tip and the current term.
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub struct LogCursor {
    pub tip: LogTip,
    pub term: u64,
}
impl LogCursor {
    /// The same writer after its append reached `tip`.
    pub const fn advanced(self, tip: LogTip) -> Self {
        Self {
            tip,
            term: self.term,
        }
    }
}
/// Backend-neutral append bounds, in framed bytes (`frame_size`).
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct AppendLimits {
    /// Largest accepted single framed record.
    pub record_max_bytes: usize,
    /// Largest accepted sum of framed records in one group.
    pub batch_max_bytes: usize,
    /// Largest accepted record count in one group, `1..=BATCH_RECORDS_MAX`.
    pub batch_records_max: usize,
}
impl AppendLimits {
    pub fn validate(self) -> Result<Self> {
        if self.record_max_bytes < 256
            || self.record_max_bytes > u32::MAX as usize
            || self.batch_max_bytes < self.record_max_bytes
            || !(1..=BATCH_RECORDS_MAX).contains(&self.batch_records_max)
        {
            return Err(invalid_log("invalid finite record/batch/WAL limits"));
        }
        Ok(self)
    }
}
/// Backend-neutral checkpoint bound. Meaningful only when the backend reports
/// `supports_checkpoint`; it bounds both installed and recovered images.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct CheckpointLimits {
    pub image_max_bytes: usize,
}
impl Default for CheckpointLimits {
    fn default() -> Self {
        Self {
            image_max_bytes: 1024 * 1024 * 1024,
        }
    }
}
impl CheckpointLimits {
    pub fn validate(self) -> Result<Self> {
        if self.image_max_bytes < 256 {
            return Err(invalid_log("invalid finite checkpoint limits"));
        }
        Ok(self)
    }
}
#[derive(Clone)]
pub struct RecoveredCheckpoint {
    pub covered: LogTip,
    pub bytes: Vec<u8>,
}
pub struct RecoveredLog {
    pub checkpoint: Option<RecoveredCheckpoint>,
    pub tip: LogTip,
    /// The writer term this handle holds; every later append must carry it.
    pub term: u64,
    pub records: Vec<Record>,
}
impl RecoveredLog {
    /// The append position this recovery hands to its writer.
    pub fn cursor(&self) -> LogCursor {
        self.tip.at(self.term)
    }
}
/// Store-side checkpoint schedule and WAL capacity wait. Passed to
/// `Store::open_log_with_options`; never owned by a log backend. Background
/// thresholds are soft; byte/file admission remains a hard bound.
#[derive(Clone, Copy, Debug)]
pub struct MaintenancePolicy {
    pub wal_bytes: u64,
    pub wal_records: u64,
    pub interval: std::time::Duration,
    pub poll: std::time::Duration,
    pub checkpoint_timeout: std::time::Duration,
    pub capacity_wait: std::time::Duration,
}
impl Default for MaintenancePolicy {
    fn default() -> Self {
        Self {
            wal_bytes: 128 * 1024 * 1024,
            wal_records: 1_000_000,
            interval: std::time::Duration::from_secs(60),
            poll: std::time::Duration::from_secs(1),
            checkpoint_timeout: std::time::Duration::from_secs(30),
            capacity_wait: std::time::Duration::from_secs(5),
        }
    }
}
impl MaintenancePolicy {
    pub fn validate(self) -> Result<Self> {
        if self.wal_bytes == 0
            || self.wal_records == 0
            || self.interval.is_zero()
            || self.poll.is_zero()
            || self.poll > self.interval
            || self.poll > self.capacity_wait
            || self.checkpoint_timeout.is_zero()
            || self.capacity_wait.is_zero()
            || [
                self.interval,
                self.poll,
                self.checkpoint_timeout,
                self.capacity_wait,
            ]
            .into_iter()
            .any(|duration| {
                duration.as_millis() > i64::MAX as u128
                    || std::time::Instant::now().checked_add(duration).is_none()
            })
        {
            return Err(invalid_log(
                "invalid finite checkpoint schedule/wait policy",
            ));
        }
        Ok(self)
    }
}
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct LogStatus {
    pub tip: LogTip,
    /// Position covered by the selected checkpoint (`LogTip::EMPTY` if none).
    pub checkpoint: LogTip,
    /// Never less than the framed bytes of records after `checkpoint`. A
    /// backend may over-charge (for example by whole storage units); callers
    /// use it only as a trigger, never as a measured quantity.
    pub uncheckpointed_bytes_upper_bound: u64,
}
/// The durable operation-log contract every Meta backend implements.
///
/// Every method is required, so wrappers cannot silently drop admission,
/// status or checkpoint behavior. The contract, checked for every backend by
/// the shared suite in `log_contract_tests`:
///
/// - **Compare-and-append.** `append_and_sync(expected, ..)` and
///   `can_append(expected, ..)` fail when `expected` is not the current tip;
///   a failed compare changes nothing.
/// - **Groups.** A group holds `1..=limits().batch_records_max` records; each
///   framed record is at most `record_max_bytes` and the group at most
///   `batch_max_bytes`. Out-of-bound groups are rejected without writing.
/// - **Sequence.** Each record advances the sequence by exactly one; the
///   returned tip is `expected.sequence + records.len()`.
/// - **Durability.** `Ok` from `append_and_sync` means the whole group is
///   durable and will be recovered. An error never implies safe retry: the
///   group may be absent, partially written or durable (ENOSPC can be
///   partial). The caller must treat its view as stale.
/// - **Write-failure latch.** After a failed durable write every later
///   operation fails until the backend is reconstructed (reopened).
/// - **Read-only admission.** `can_append` performs no writes. `Ok(false)`
///   guarantees no append was attempted and nothing changed.
/// - **Recovery.** `recover` returns the checkpoint (if any) and every record
///   after it, in order; calling it twice without appends yields equal results
///   (a backend may serve the first call from an open-time cache).
/// - **Checkpoint.** When `supports_checkpoint` is true, `install_checkpoint`
///   rejects anchors that are not on the chain, beyond the tip, or a known
///   position with different bytes; success releases the covered prefix and
///   keeps the suffix replayable. When false, Store never schedules it and the
///   method returns an error.
/// - **Status.** `status().uncheckpointed_bytes_upper_bound` is never below the
///   true framed bytes after the checkpoint.
/// - **Term/fencing.** Each writer open obtains a strictly larger term, durable
///   before the backend is returned (`recover().term`). Every frame records
///   its writer's term. Appends and admission carry `LogCursor { tip, term }`;
///   a term other than the current one, or any operation through a handle
///   whose term was superseded, fails `META_LOG_FENCED` without writing a byte.
///   Recovery rejects a chain whose frame terms decrease or exceed the current
///   term. Frames with term 0 (written before terms existed) remain valid.
pub trait MetaLog: Send + Sync {
    fn persistence(&self) -> BackendPersistence;
    fn limits(&self) -> AppendLimits;
    fn checkpoint_limits(&self) -> CheckpointLimits;
    /// Capability: whether `install_checkpoint` can succeed. Store runs its
    /// checkpoint loop and WAL capacity wait only when this is true.
    fn supports_checkpoint(&self) -> bool;
    fn health(&self) -> MetaFuture<'_, ()>;
    fn recover(&self) -> MetaFuture<'_, RecoveredLog>;
    fn status(&self) -> MetaFuture<'_, LogStatus>;
    /// Read-only admission: false guarantees append was not attempted. An
    /// append error itself never implies safe retry (ENOSPC can be partial).
    fn can_append(&self, expected: LogCursor, framed_bytes: usize) -> MetaFuture<'_, bool>;
    fn install_checkpoint(&self, covered: LogTip, bytes: Vec<u8>) -> MetaFuture<'_, ()>;
    fn append_and_sync(&self, expected: LogCursor, records: Vec<Record>) -> MetaFuture<'_, LogTip>;
}
#[derive(Deserialize, Serialize)]
struct Envelope {
    version: u16,
    identity: StoreIdentity,
    sequence: u64,
    base_sequence: u64,
    term: u64,
    previous_hash: [u8; 32],
    decision: Record,
}
/// The `Envelope` fields before `decision`, in order: decoded first so the
/// record's own schema is identified before the full decode (inner layer).
type EnvelopeHead = (u16, StoreIdentity, u64, u64, u64, [u8; 32]);
#[derive(Serialize)]
struct EnvelopeRef<'a> {
    version: u16,
    identity: StoreIdentity,
    sequence: u64,
    base_sequence: u64,
    term: u64,
    previous_hash: [u8; 32],
    decision: &'a Record,
}
/// Exact framed size of `decision` appended at `cursor` (term is a varint, so
/// the size depends on it).
pub(super) fn frame_size(cursor: LogCursor, decision: &Record) -> Result<usize> {
    let LogCursor { tip, term } = cursor;
    let envelope = EnvelopeRef {
        version: VERSION,
        identity: StoreIdentity {
            store_id: [0; 16],
            cluster_id: [0; 16],
            incarnation: [0; 16],
        },
        sequence: tip
            .sequence
            .checked_add(1)
            .ok_or_else(|| invalid_log("sequence exhausted"))?,
        base_sequence: tip.sequence,
        term,
        previous_hash: tip.record_hash,
        decision,
    };
    codec::encoded_size(&envelope)?
        .checked_add(FRAME_OVERHEAD)
        .ok_or_else(|| invalid_log("frame length overflow"))
}
pub(super) fn invalid_log(message: impl Into<String>) -> Error {
    Error::coded(
        afs_error::CONFIG_INVALID,
        format!("Meta operation log: {}", message.into()),
    )
}
/// Adds `context` (file, offset) to a log error. A format incompatibility
/// keeps `STORAGE_FORMAT_INCOMPATIBLE` so operators can tell a binary/data
/// mismatch from corruption; every other error stays an invalid log, as before.
pub(super) fn log_context(error: Error, context: impl std::fmt::Display) -> Error {
    if error.code() == afs_error::STORAGE_FORMAT_INCOMPATIBLE {
        Error::coded(
            afs_error::STORAGE_FORMAT_INCOMPATIBLE,
            format!("Meta operation log: {context}: {}", error.message()),
        )
    } else {
        invalid_log(format!("{context}: {error}"))
    }
}
pub(super) fn unavailable(message: impl Into<String>) -> Error {
    Error::coded(afs_error::IO_UNAVAILABLE, message)
}
pub(super) fn full(message: impl Into<String>) -> Error {
    Error::coded(afs_error::IO_CAPACITY, message)
}
/// A writer holding term `found` was superseded by term `current`.
pub(super) fn fenced(found: u64, current: u64) -> Error {
    Error::coded(
        afs_error::META_LOG_FENCED,
        format!(
            "Meta operation log writer fenced: found term {found}, current term {current}; a newer writer owns the log and nothing was written"
        ),
    )
}
/// Recovery check for one frame's term against the chain and current term.
pub(super) fn chain_term(previous: u64, found: u64, current: u64) -> Result<u64> {
    if found < previous {
        return Err(invalid_log(format!(
            "record term {found} decreases below previous term {previous}"
        )));
    }
    if found > current {
        return Err(invalid_log(format!(
            "record term {found} exceeds current term {current}"
        )));
    }
    Ok(found)
}

pub(super) fn encode_frame(
    identity: StoreIdentity,
    cursor: LogCursor,
    decision: Record,
    limit: usize,
) -> Result<(Vec<u8>, LogTip)> {
    decision.validate()?;
    let LogCursor { tip, term } = cursor;
    let sequence = tip
        .sequence
        .checked_add(1)
        .ok_or_else(|| invalid_log("sequence exhausted"))?;
    let envelope = Envelope {
        version: VERSION,
        identity,
        sequence,
        base_sequence: tip.sequence,
        term,
        previous_hash: tip.record_hash,
        decision,
    };
    let payload = codec::encode_bounded(
        &envelope,
        limit
            .checked_sub(FRAME_OVERHEAD)
            .ok_or_else(|| invalid_log("record limit too small"))?,
    )?;
    let mut hasher = blake3::Hasher::new();
    hasher.update(b"afs.meta.record.v2");
    hasher.update(&payload);
    let hash = *hasher.finalize().as_bytes();
    let length =
        u32::try_from(payload.len() + 32).map_err(|_| invalid_log("frame length overflow"))?;
    let mut frame = Vec::new();
    frame
        .try_reserve_exact(payload.len() + FRAME_OVERHEAD)
        .map_err(|_| Error::coded(afs_error::IO_OUT_OF_MEMORY, "allocate Meta log frame"))?;
    frame.extend_from_slice(&length.to_be_bytes());
    frame.extend_from_slice(&[0; 4]);
    frame.extend_from_slice(&payload);
    frame.extend_from_slice(&hash);
    let crc = crc32c::crc32c_append(crc32c::crc32c(&frame[..4]), &frame[8..]);
    frame[4..8].copy_from_slice(&crc.to_be_bytes());
    Ok((
        frame,
        LogTip {
            sequence,
            record_hash: hash,
        },
    ))
}
pub(super) fn frame_length(header: &[u8], limit: usize) -> Result<usize> {
    let bytes: [u8; 4] = header
        .get(..4)
        .ok_or_else(|| invalid_log("incomplete frame length"))?
        .try_into()
        .map_err(|_| invalid_log("frame length width"))?;
    let length = (u32::from_be_bytes(bytes) as usize)
        .checked_add(8)
        .ok_or_else(|| invalid_log("frame length overflow"))?;
    if length < FRAME_OVERHEAD || length > limit {
        return Err(invalid_log("frame length outside record bound"));
    }
    Ok(length)
}
/// Decode one frame after `tip`; returns the record, its tip and its term.
/// Term ordering is the caller's chain check (`chain_term`).
pub(super) fn decode_frame(
    identity: StoreIdentity,
    tip: LogTip,
    frame: &[u8],
    limit: usize,
) -> Result<(Record, LogTip, u64)> {
    if frame.len() != frame_length(frame, limit)? {
        return Err(invalid_log("incomplete/trailing frame"));
    }
    // Version first (outer layer): the envelope version is the payload's
    // first varint and is read before the CRC, hash and full decode.
    crate::format::check_postcard_u16(
        crate::format::FormatKind::MetaWalEnvelope,
        "frame",
        &frame[8..frame.len() - 32],
    )?;
    let crc_bytes: [u8; 4] = frame[4..8]
        .try_into()
        .map_err(|_| invalid_log("CRC width"))?;
    if u32::from_be_bytes(crc_bytes)
        != crc32c::crc32c_append(crc32c::crc32c(&frame[..4]), &frame[8..])
    {
        return Err(invalid_log("frame CRC mismatch"));
    }
    let body_end = frame.len() - 32;
    let payload = &frame[8..body_end];
    let mut hasher = blake3::Hasher::new();
    hasher.update(b"afs.meta.record.v2");
    hasher.update(payload);
    let hash = *hasher.finalize().as_bytes();
    if frame[body_end..] != hash {
        return Err(invalid_log("record hash mismatch"));
    }
    // Inner layer: after the frame's integrity, before the full decode.
    if let Ok((_, record)) = postcard::take_from_bytes::<EnvelopeHead>(payload) {
        Record::check_schema(record)?;
    }
    let (envelope, tail): (Envelope, _) = postcard::take_from_bytes(payload)
        .map_err(|e| invalid_log(format!("decode envelope: {e}")))?;
    if !tail.is_empty()
        || envelope.version != VERSION
        || envelope.identity != identity
        || envelope.base_sequence != tip.sequence
        || tip.sequence.checked_add(1) != Some(envelope.sequence)
        || envelope.previous_hash != tip.record_hash
    {
        return Err(invalid_log(
            "record format/identity/sequence/base/hash-chain mismatch",
        ));
    }
    envelope.decision.validate()?;
    Ok((
        envelope.decision,
        LogTip {
            sequence: envelope.sequence,
            record_hash: hash,
        },
        envelope.term,
    ))
}
pub(super) fn encode_batch(
    identity: StoreIdentity,
    cursor: LogCursor,
    records: Vec<Record>,
    limits: AppendLimits,
) -> Result<(Vec<Vec<u8>>, LogTip, usize)> {
    if records.is_empty() || records.len() > limits.batch_records_max {
        return Err(invalid_log(format!(
            "batch record count outside 1..{}",
            limits.batch_records_max
        )));
    }
    let mut frames = Vec::new();
    let mut tip = cursor.tip;
    let mut total = 0usize;
    for record in records {
        let (frame, next) = encode_frame(
            identity,
            tip.at(cursor.term),
            record,
            limits.record_max_bytes,
        )?;
        total = total
            .checked_add(frame.len())
            .filter(|size| *size <= limits.batch_max_bytes)
            .ok_or_else(|| full("batch byte budget exhausted"))?;
        frames.push(frame);
        tip = next;
    }
    Ok((frames, tip, total))
}
/// Explicitly volatile log for tests/disposable use, with the same wire checks.
pub struct MemoryLog {
    identity: StoreIdentity,
    options: MemoryLogOptions,
    /// The writer term this handle holds; superseded once `state.term` grows.
    term: u64,
    state: std::sync::Arc<Mutex<MemoryState>>,
    /// Test-only write-failure injection; absent from production builds.
    #[cfg(test)]
    fault: MemoryFault,
}
/// Bounds for the volatile log. Every byte bound counts framed records.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct MemoryLogOptions {
    pub record_max_bytes: usize,
    pub batch_max_bytes: usize,
    /// Retained (uncheckpointed) framed bytes; appends beyond it fail `full`.
    pub wal_max_bytes: u64,
}
impl Default for MemoryLogOptions {
    fn default() -> Self {
        Self {
            record_max_bytes: 64 * 1024 * 1024,
            batch_max_bytes: 128 * 1024 * 1024,
            wal_max_bytes: 1024 * 1024 * 1024,
        }
    }
}
impl MemoryLogOptions {
    fn append(self) -> AppendLimits {
        AppendLimits {
            record_max_bytes: self.record_max_bytes,
            batch_max_bytes: self.batch_max_bytes,
            batch_records_max: BATCH_RECORDS_MAX,
        }
    }
    pub fn validate(self) -> Result<Self> {
        self.append().validate()?;
        let batch =
            u64::try_from(self.batch_max_bytes).map_err(|_| invalid_log("batch limit overflow"))?;
        if batch > self.wal_max_bytes {
            return Err(invalid_log("invalid finite record/batch/WAL limits"));
        }
        Ok(self)
    }
}
#[cfg(test)]
#[derive(Default)]
struct MemoryFault {
    fail_next_append: std::sync::atomic::AtomicBool,
    latched: std::sync::atomic::AtomicBool,
}
struct MemoryState {
    /// Current writer term; only the newest handle may operate.
    term: u64,
    checkpoint: Option<RecoveredCheckpoint>,
    tip: LogTip,
    base: LogTip,
    base_revision: super::StoreRevision,
    frames: Vec<Vec<u8>>,
    bytes: u64,
}
impl MemoryLog {
    pub fn new(options: MemoryLogOptions) -> Result<Self> {
        Ok(Self {
            identity: StoreIdentity::new()?,
            options: options.validate()?,
            term: 1,
            state: std::sync::Arc::new(Mutex::new(MemoryState {
                term: 1,
                checkpoint: None,
                tip: LogTip::EMPTY,
                base: LogTip::EMPTY,
                base_revision: super::StoreRevision::ZERO,
                frames: Vec::new(),
                bytes: 0,
            })),
            #[cfg(test)]
            fault: MemoryFault::default(),
        })
    }
    fn lock(&self) -> Result<std::sync::MutexGuard<'_, MemoryState>> {
        #[cfg(test)]
        if self
            .fault
            .latched
            .load(std::sync::atomic::Ordering::Acquire)
        {
            return Err(unavailable("memory log write failed; reconstruct required"));
        }
        let state = self
            .state
            .lock()
            .map_err(|_| unavailable("memory log poisoned"))?;
        if state.term != self.term {
            return Err(fenced(self.term, state.term));
        }
        Ok(state)
    }
    /// Test-only: the writer term this handle holds.
    #[cfg(test)]
    pub(super) fn term(&self) -> u64 {
        self.term
    }
    /// Test-only: the next append fails before changing anything, then every
    /// operation on this handle fails, like a latched durable-write failure.
    #[cfg(test)]
    pub(super) fn inject_write_failure(&self) {
        self.fault
            .fail_next_append
            .store(true, std::sync::atomic::Ordering::Release);
    }
    /// Test-only: a new writer over the same volatile records with the next
    /// term, modelling a reopen or takeover. Every older handle is fenced.
    #[cfg(test)]
    pub(super) fn reopen(&self) -> Self {
        let mut state = self
            .state
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        state.term += 1;
        Self {
            identity: self.identity,
            options: self.options,
            term: state.term,
            state: std::sync::Arc::clone(&self.state),
            fault: MemoryFault::default(),
        }
    }
}
impl MetaLog for MemoryLog {
    fn persistence(&self) -> BackendPersistence {
        BackendPersistence::Volatile
    }
    fn limits(&self) -> AppendLimits {
        self.options.append()
    }
    fn checkpoint_limits(&self) -> CheckpointLimits {
        CheckpointLimits::default()
    }
    fn supports_checkpoint(&self) -> bool {
        true
    }
    fn health(&self) -> MetaFuture<'_, ()> {
        Box::pin(async {
            drop(self.lock()?);
            Ok(())
        })
    }
    fn status(&self) -> MetaFuture<'_, LogStatus> {
        Box::pin(async {
            let state = self.lock()?;
            Ok(LogStatus {
                tip: state.tip,
                checkpoint: state.base,
                uncheckpointed_bytes_upper_bound: state.bytes,
            })
        })
    }
    fn can_append(&self, expected: LogCursor, framed_bytes: usize) -> MetaFuture<'_, bool> {
        Box::pin(async move {
            let state = self.lock()?;
            if expected.term != state.term {
                return Err(fenced(expected.term, state.term));
            }
            if expected.tip != state.tip {
                return Err(unavailable("memory admission tip changed"));
            }
            Ok(state
                .bytes
                .checked_add(framed_bytes as u64)
                .is_some_and(|bytes| bytes <= self.options.wal_max_bytes))
        })
    }
    fn recover(&self) -> MetaFuture<'_, RecoveredLog> {
        Box::pin(async {
            let state = self.lock()?;
            let mut tip = state.base;
            let mut records = Vec::new();
            let mut revision = state.base_revision;
            let mut term = 0;
            for frame in &state.frames {
                let (record, next, found) =
                    decode_frame(self.identity, tip, frame, self.options.record_max_bytes)?;
                term = chain_term(term, found, state.term)?;
                if record.revision_before() != revision {
                    return Err(invalid_log("memory replay revision gap"));
                }
                revision = record.revision_after();
                records.push(record);
                tip = next;
            }
            if tip != state.tip {
                return Err(invalid_log("memory log tip mismatch"));
            }
            Ok(RecoveredLog {
                checkpoint: state.checkpoint.clone(),
                tip,
                term: state.term,
                records,
            })
        })
    }
    fn install_checkpoint(&self, covered: LogTip, bytes: Vec<u8>) -> MetaFuture<'_, ()> {
        Box::pin(async move {
            // The latch check is taken here; the blocking task owns the state.
            drop(self.lock()?);
            let state = std::sync::Arc::clone(&self.state);
            let identity = self.identity;
            let term = self.term;
            let record_limit = self.options.record_max_bytes;
            let limits = self.checkpoint_limits();
            tokio::task::spawn_blocking(move || {
                install_memory_checkpoint(
                    &state,
                    (identity, term),
                    record_limit,
                    limits,
                    covered,
                    bytes,
                )
            })
            .await
            .map_err(|error| unavailable(format!("memory checkpoint task: {error}")))?
        })
    }
    fn append_and_sync(&self, expected: LogCursor, records: Vec<Record>) -> MetaFuture<'_, LogTip> {
        Box::pin(async move {
            let mut state = self.lock()?;
            if expected.term != state.term {
                return Err(fenced(expected.term, state.term));
            }
            if state.tip != expected.tip {
                return Err(unavailable("log compare-and-append tip changed"));
            }
            let (frames, tip, bytes) =
                encode_batch(self.identity, expected, records, self.options.append())?;
            let total = state
                .bytes
                .checked_add(bytes as u64)
                .filter(|size| *size <= self.options.wal_max_bytes)
                .ok_or_else(|| full("memory WAL byte budget exhausted"))?;
            #[cfg(test)]
            if self
                .fault
                .fail_next_append
                .swap(false, std::sync::atomic::Ordering::AcqRel)
            {
                self.fault
                    .latched
                    .store(true, std::sync::atomic::Ordering::Release);
                return Err(unavailable("injected memory log write failure"));
            }
            state.frames.extend(frames);
            state.tip = tip;
            state.bytes = total;
            Ok(tip)
        })
    }
}
fn install_memory_checkpoint(
    state: &Mutex<MemoryState>,
    (identity, term): (StoreIdentity, u64),
    record_limit: usize,
    limits: CheckpointLimits,
    covered: LogTip,
    bytes: Vec<u8>,
) -> Result<()> {
    let limits = limits.validate()?;
    let revision = super::log::StateImage::validate_encoded(&bytes, limits.image_max_bytes)?;
    let mut state = state
        .lock()
        .map_err(|_| unavailable("memory log poisoned"))?;
    if state.term != term {
        return Err(fenced(term, state.term));
    }
    if covered.sequence > state.tip.sequence || covered.sequence < state.base.sequence {
        return Err(invalid_log("memory checkpoint position invalid"));
    }
    let count = usize::try_from(covered.sequence - state.base.sequence)
        .map_err(|_| invalid_log("memory checkpoint index"))?;
    if count > state.frames.len() {
        return Err(invalid_log("memory checkpoint missing prefix"));
    }
    let mut anchor = state.base;
    let mut business = state.base_revision;
    let mut released = 0u64;
    for frame in state.frames.iter().take(count) {
        let (record, next, _) = decode_frame(identity, anchor, frame, record_limit)?;
        if record.revision_before() != business {
            return Err(invalid_log("memory checkpoint revision gap"));
        }
        anchor = next;
        business = record.revision_after();
        released = released
            .checked_add(frame.len() as u64)
            .ok_or_else(|| invalid_log("memory checkpoint byte charge overflow"))?;
    }
    if anchor != covered || business != revision {
        return Err(invalid_log("memory checkpoint anchor/revision mismatch"));
    }
    if let Some(old) = &state.checkpoint
        && old.covered == covered
        && old.bytes != bytes
    {
        return Err(invalid_log("same checkpoint position has different state"));
    }
    let remaining = state
        .bytes
        .checked_sub(released)
        .ok_or_else(|| invalid_log("memory checkpoint byte charge underflow"))?;
    state.checkpoint = Some(RecoveredCheckpoint { covered, bytes });
    state.frames.drain(..count);
    state.base = covered;
    state.base_revision = revision;
    state.bytes = remaining;
    Ok(())
}

#[cfg(test)]
pub(super) fn test_records(count: usize) -> Vec<Record> {
    use super::{NodeSessionLease, RequestKey, state::State};
    use std::time::Duration;
    let mut state = State::default();
    let mut records = Vec::new();
    for number in 0..count {
        let (next, _, record) = state
            .prepare_node_record(
                RequestKey::new("log-test", format!("request-{number}")),
                NodeSessionLease {
                    node_id: "node".into(),
                    session_id: format!("session-{number}"),
                    grpc_addr: "grpc".into(),
                    data_addr: "data".into(),
                    rest_addr: "rest".into(),
                    ownerfs_enabled: true,
                    storage_devices: Vec::new(),
                    lease_ttl: Duration::from_secs(30),
                },
                12345,
                1024 * 1024,
            )
            .unwrap();
        state = next;
        records.push(record.unwrap().into());
    }
    records
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn p3_corrupt_authority_refuses_before_listener_and_preserves_files() {
        use super::super::local_log::{LOG_HEADER_BYTES, LocalLog, LocalLogOptions};
        use crate::{
            config::{Cli, Config, MetaStoreBackend, Role},
            runtime::Observability,
        };
        use clap::Parser;
        use std::{collections::BTreeMap, fs, path::Path};

        fn inventory(root: &Path) -> BTreeMap<std::ffi::OsString, Vec<u8>> {
            fs::read_dir(root)
                .unwrap()
                .map(|entry| {
                    let entry = entry.unwrap();
                    (entry.file_name(), fs::read(entry.path()).unwrap())
                })
                .collect()
        }
        fn unchecked_frame(payload: &[u8]) -> Vec<u8> {
            // Intentionally bypass encode_frame validation: semantic damage
            // has a valid outer CRC/BLAKE3, so replay must reject its content.
            let mut hash = blake3::Hasher::new();
            hash.update(b"afs.meta.record.v2");
            hash.update(payload);
            let mut frame = Vec::new();
            frame.extend_from_slice(&u32::try_from(payload.len() + 32).unwrap().to_be_bytes());
            frame.extend_from_slice(&[0; 4]);
            frame.extend_from_slice(payload);
            frame.extend_from_slice(hash.finalize().as_bytes());
            let crc = crc32c::crc32c_append(crc32c::crc32c(&frame[..4]), &frame[8..]);
            frame[4..8].copy_from_slice(&crc.to_be_bytes());
            frame
        }
        async fn refused(dir: &Path, file: &str, markers: &[&str]) {
            let root = dir.join("meta-store");
            let before = inventory(&root);
            let mut cfg = Config::resolve(Role::Meta, Cli::parse_from(["afs-meta"])).unwrap();
            cfg.meta_store = MetaStoreBackend::LocalFile;
            cfg.data_dir = dir.to_owned();
            cfg.dfs = true;
            let cert = dir.join("identity-table.der");
            fs::write(&cert, [1, 2, 3, 4]).unwrap();
            cfg.trusted_node_certs.insert("node".into(), cert.clone());
            cfg.tls_ca_certificate = Some(cert.clone());
            cfg.tls_identity_certificate = Some(cert.clone());
            cfg.tls_identity_private_key = Some(cert);
            cfg.tls_server_name = Some("afs-cluster".into());
            let occupied = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
            cfg.grpc_listen = occupied.local_addr().unwrap();
            // Recovery must fail before the deliberately occupied listener is
            // touched, not merely return some unrelated startup error.
            let error = crate::meta::run(cfg, Observability::new().unwrap())
                .await
                .unwrap_err();
            let message = error.to_string();
            assert_eq!(
                error.downcast_ref::<Error>().unwrap().code(),
                afs_error::CONFIG_INVALID
            );
            assert!(message.contains(file), "{message}");
            for marker in markers {
                assert!(message.contains(marker), "missing {marker}: {message}");
            }
            assert_eq!(inventory(&root), before, "startup changed corrupt evidence");
            println!(
                "P3_CORRUPTION_REFUSED file={file} files={} error={message}",
                before.len()
            );
        }

        // Keep failed fixtures for diagnosis. Only explicitly successful owned
        // cases are removed; a panic leaves the actual original bytes on disk.
        for case in [
            "business-base",
            "crc",
            "hash",
            "store",
            "cluster",
            "incarnation",
            "sequence",
            "base",
            "previous-hash",
            "revision-backward",
            "key-value",
            "extra-events",
        ] {
            let dir = tempfile::Builder::new()
                .prefix(&format!("p3-corrupt-{case}-"))
                .tempdir()
                .unwrap()
                .keep();
            println!("P3_CORRUPTION_CASE case={case} directory={}", dir.display());
            let root = dir.join("meta-store");
            let log = LocalLog::open(&root, LocalLogOptions::default()).unwrap();
            let record = test_records(1).pop().unwrap();
            let (original, _) = encode_frame(
                log.identity(),
                LogTip::EMPTY.at(log.term()),
                record.clone(),
                1024 * 1024,
            )
            .unwrap();
            log.append_and_sync(LogTip::EMPTY.at(log.term()), vec![record])
                .await
                .unwrap();
            drop(log);
            let (mut envelope, tail): (Envelope, _) =
                postcard::take_from_bytes(&original[8..original.len() - 32]).unwrap();
            assert!(tail.is_empty());
            match case {
                "store" => envelope.identity.store_id[0] ^= 1,
                "cluster" => envelope.identity.cluster_id[0] ^= 1,
                "incarnation" => envelope.identity.incarnation[0] ^= 1,
                "sequence" => envelope.sequence += 1,
                "base" => envelope.base_sequence += 1,
                "previous-hash" => envelope.previous_hash[0] ^= 1,
                "business-base" | "revision-backward" | "key-value" => {
                    let mut value = serde_json::to_value(&envelope.decision).unwrap();
                    let decision = &mut value["Decision"]["decision"];
                    if case == "business-base" {
                        decision["revision_before"] = serde_json::json!(1);
                        decision["revision_after"] = serde_json::json!(2);
                    } else if case == "revision-backward" {
                        decision["revision_after"] = serde_json::json!(0);
                    } else {
                        let key =
                            &mut decision["changes"][0]["Put"]["key"]["NodeSession"]["node_id"];
                        assert_eq!(*key, serde_json::json!("node"));
                        *key = serde_json::json!("nope");
                    }
                    envelope.decision = serde_json::from_value(value).unwrap();
                }
                "crc" | "hash" | "extra-events" => {}
                _ => unreachable!(),
            }
            let mut payload = postcard::to_allocvec(&envelope).unwrap();
            if case == "extra-events" {
                // There is no second serialized watch-event authority. A
                // trailing would-be events field is rejected, not applied.
                payload.extend_from_slice(&[0, 0]);
            }
            let mut frame = unchecked_frame(&payload);
            if case == "crc" {
                // Byte 8 is the envelope version, identified before the CRC;
                // corrupt the first identity byte instead.
                frame[9] ^= 1;
            }
            if case == "hash" {
                let last = frame.len() - 1;
                frame[last] ^= 1;
                let crc = crc32c::crc32c_append(crc32c::crc32c(&frame[..4]), &frame[8..]);
                frame[4..8].copy_from_slice(&crc.to_be_bytes());
            }
            let wal = root.join("wal-00000000000000000001.log");
            let mut bytes = fs::read(&wal).unwrap();
            bytes.truncate(LOG_HEADER_BYTES as usize);
            bytes.extend_from_slice(&frame);
            fs::write(&wal, bytes).unwrap();
            let item = match case {
                "business-base" => "business revision",
                "crc" => "CRC",
                "hash" => "hash mismatch",
                "revision-backward" | "key-value" => "canonical",
                _ => "format/identity",
            };
            refused(
                &dir,
                "wal-00000000000000000001.log",
                &["offset 4096", "sequence 0", item],
            )
            .await;
            fs::remove_dir_all(dir).unwrap();
        }
        for case in ["missing-middle", "rotated-header"] {
            let dir = tempfile::Builder::new()
                .prefix(&format!("p3-corrupt-{case}-"))
                .tempdir()
                .unwrap()
                .keep();
            println!("P3_CORRUPTION_CASE case={case} directory={}", dir.display());
            let root = dir.join("meta-store");
            let limits = LocalLogOptions {
                record_max_bytes: 1024,
                batch_max_bytes: 4096,
                wal_max_bytes: 128 * 1024,
                segment_bytes: 4608,
                segments_max: 16,
            };
            let log = LocalLog::open(&root, limits).unwrap();
            let mut tip = LogTip::EMPTY;
            for record in test_records(12) {
                tip = log
                    .append_and_sync(tip.at(log.term()), vec![record])
                    .await
                    .unwrap();
            }
            drop(log);
            let mut segments = fs::read_dir(&root)
                .unwrap()
                .map(|entry| entry.unwrap().file_name())
                .filter(|name| name.to_str().unwrap().starts_with("wal-"))
                .collect::<Vec<_>>();
            segments.sort();
            assert!(segments.len() >= 3);
            let name = segments[1].to_str().unwrap();
            let wal = root.join(name);
            if case == "missing-middle" {
                fs::rename(&wal, root.join("preserved-missing-middle")).unwrap();
            } else {
                let mut bytes = fs::read(&wal).unwrap();
                bytes[6] ^= 1;
                fs::write(&wal, bytes).unwrap();
            }
            refused(&dir, name, &["offset 0", "sequence"]).await;
            fs::remove_dir_all(dir).unwrap();
        }
    }

    #[test]
    fn p3_frame_roundtrip_exact_size_and_checks() {
        let identity = StoreIdentity::new().unwrap();
        let record = test_records(1).pop().unwrap();
        let size = frame_size(LogTip::EMPTY.at(1), &record).unwrap();
        let (frame, tip) =
            encode_frame(identity, LogTip::EMPTY.at(1), record, 1024 * 1024).unwrap();
        assert_eq!(frame.len(), size);
        let decoded = decode_frame(identity, LogTip::EMPTY, &frame, size).unwrap();
        assert_eq!((decoded.1, decoded.2), (tip, 1));
        for index in [0, 4, 8, frame.len() - 1] {
            let mut broken = frame.clone();
            broken[index] ^= 1;
            assert!(decode_frame(identity, LogTip::EMPTY, &broken, 1024 * 1024).is_err());
        }
        assert!(
            decode_frame(
                StoreIdentity::new().unwrap(),
                LogTip::EMPTY,
                &frame,
                1024 * 1024
            )
            .is_err()
        );
        assert!(decode_frame(identity, tip, &frame, 1024 * 1024).is_err());
        assert!(
            decode_frame(
                identity,
                LogTip::EMPTY,
                &frame[..frame.len() - 1],
                1024 * 1024
            )
            .is_err()
        );
        let mut hash_bad = frame.clone();
        // Byte 8 is the envelope version (checked first); flip an identity byte.
        hash_bad[9] ^= 1;
        let crc = crc32c::crc32c_append(crc32c::crc32c(&hash_bad[..4]), &hash_bad[8..]);
        hash_bad[4..8].copy_from_slice(&crc.to_be_bytes());
        assert!(
            decode_frame(identity, LogTip::EMPTY, &hash_bad, 1024 * 1024)
                .unwrap_err()
                .to_string()
                .contains("hash mismatch")
        );
    }

    #[tokio::test]
    async fn p3_maintenance_memory_checkpoint_releases_covered_frames() {
        let log = MemoryLog::new(MemoryLogOptions::default()).unwrap();
        let records = test_records(2);
        let first = super::super::state::State::default()
            .replay_record(&records[0])
            .unwrap();
        let (frame, covered) = encode_frame(
            log.identity,
            LogTip::EMPTY.at(log.term()),
            records[0].clone(),
            1024 * 1024,
        )
        .unwrap();
        let tip = log
            .append_and_sync(LogTip::EMPTY.at(log.term()), records)
            .await
            .unwrap();
        let before = log.state.lock().unwrap().bytes;
        log.install_checkpoint(
            covered,
            super::super::log::StateImage::encode(&first, 1024 * 1024).unwrap(),
        )
        .await
        .unwrap();
        assert_eq!(
            log.state.lock().unwrap().frames.len(),
            1,
            "covered frames must actually leave the volatile log"
        );
        assert_eq!(log.state.lock().unwrap().bytes, before - frame.len() as u64);
        let recovered = log.recover().await.unwrap();
        assert_eq!(recovered.tip, tip);
        assert_eq!(recovered.records.len(), 1);
        let image = recovered.checkpoint.unwrap();
        assert_eq!(image.covered, covered);
        let state = super::super::log::StateImage::decode(&image.bytes, 1024 * 1024)
            .unwrap()
            .replay_record(&recovered.records[0])
            .unwrap();
        assert_eq!(state.revision, super::super::StoreRevision(2));
    }

    #[tokio::test]
    async fn p3_memory_log_cas_batch_recovery_and_quota() {
        let log = MemoryLog::new(MemoryLogOptions::default()).unwrap();
        let tip = log
            .append_and_sync(LogTip::EMPTY.at(log.term()), test_records(2))
            .await
            .unwrap();
        assert_eq!(tip.sequence, 2);
        let recovered = log.recover().await.unwrap();
        assert_eq!(recovered.records.len(), 2);
        assert_eq!(recovered.tip, tip);
        assert!(
            log.append_and_sync(LogTip::EMPTY.at(log.term()), test_records(1))
                .await
                .is_err()
        );
        assert!(
            log.append_and_sync(tip.at(log.term()), Vec::new())
                .await
                .is_err()
        );
        assert_eq!(log.recover().await.unwrap().tip, tip);
        let tiny = MemoryLog::new(MemoryLogOptions {
            record_max_bytes: 256,
            batch_max_bytes: 256,
            wal_max_bytes: 8192,
        })
        .unwrap();
        let state = super::super::state::State::default();
        let (_, _, large) = state
            .prepare_node_record(
                super::super::RequestKey::new("large", "one"),
                super::super::NodeSessionLease {
                    node_id: "node".into(),
                    session_id: "session".into(),
                    grpc_addr: "g".repeat(1024),
                    data_addr: "data".into(),
                    rest_addr: "rest".into(),
                    ownerfs_enabled: true,
                    storage_devices: Vec::new(),
                    lease_ttl: std::time::Duration::from_secs(30),
                },
                12345,
                1024 * 1024,
            )
            .unwrap();
        let large = Record::from(large.unwrap());
        assert!(frame_size(LogTip::EMPTY.at(1), &large).unwrap() > tiny.limits().record_max_bytes);
        assert!(
            tiny.append_and_sync(LogTip::EMPTY.at(tiny.term()), vec![large])
                .await
                .is_err()
        );
        assert_eq!(tiny.recover().await.unwrap().tip, LogTip::EMPTY);
        let quota = MemoryLog::new(MemoryLogOptions {
            record_max_bytes: 1024,
            batch_max_bytes: 1024,
            wal_max_bytes: 8192,
        })
        .unwrap();
        let mut tip = LogTip::EMPTY;
        let mut exhausted = false;
        for record in test_records(64) {
            assert!(frame_size(tip.at(1), &record).unwrap() <= quota.limits().record_max_bytes);
            match quota
                .append_and_sync(tip.at(quota.term()), vec![record])
                .await
            {
                Ok(next) => tip = next,
                Err(error) => {
                    assert!(error.to_string().contains("WAL byte budget"));
                    exhausted = true;
                    break;
                }
            }
        }
        assert!(exhausted);
        assert!(tip.sequence > 0);
        assert_eq!(quota.recover().await.unwrap().tip, tip);
        assert!(quota.state.lock().unwrap().bytes <= quota.options.wal_max_bytes);
    }

    /// A record whose framed size grows with `pad`.
    fn padded_record(pad: usize) -> Record {
        let (_, _, record) = super::super::state::State::default()
            .prepare_node_record(
                super::super::RequestKey::new("term", "padded"),
                super::super::NodeSessionLease {
                    node_id: "node".into(),
                    session_id: "session".into(),
                    grpc_addr: "g".repeat(pad),
                    data_addr: "data".into(),
                    rest_addr: "rest".into(),
                    ownerfs_enabled: true,
                    storage_devices: Vec::new(),
                    lease_ttl: std::time::Duration::from_secs(30),
                },
                12345,
                1024 * 1024,
            )
            .unwrap();
        Record::from(record.unwrap())
    }

    /// The term is a postcard varint: from 128 it takes more than one byte, so
    /// the single-record bound must be checked with the real term.
    #[tokio::test]
    async fn frame_size_uses_actual_term_from_128() {
        let identity = StoreIdentity::new().unwrap();
        let record = padded_record(512);
        let size = |term| frame_size(LogTip::EMPTY.at(term), &record).unwrap();
        assert_eq!(size(0), size(127));
        assert_eq!(size(128), size(127) + 1);
        assert_eq!(size(u64::MAX), size(0) + 9);
        for term in [0, 127, 128, 300, u64::MAX] {
            let (frame, tip) = encode_frame(
                identity,
                LogTip::EMPTY.at(term),
                record.clone(),
                1024 * 1024,
            )
            .unwrap();
            assert_eq!(frame.len(), size(term), "term {term}");
            let decoded = decode_frame(identity, LogTip::EMPTY, &frame, 1024 * 1024).unwrap();
            assert_eq!((decoded.1, decoded.2), (tip, term));
        }
        // Exactly at the old (term < 128) size the record no longer fits once
        // the writer term reaches 128; one byte more admits it.
        for (limit, accepted) in [(size(127), false), (size(128), true)] {
            let first = MemoryLog::new(MemoryLogOptions {
                record_max_bytes: limit,
                batch_max_bytes: limit,
                wal_max_bytes: 1024 * 1024,
            })
            .unwrap();
            let mut log = first.reopen();
            while log.term() < 128 {
                log = log.reopen();
            }
            assert_eq!(log.term(), 128);
            let result = log
                .append_and_sync(LogTip::EMPTY.at(128), vec![record.clone()])
                .await;
            assert_eq!(result.is_ok(), accepted, "limit {limit}");
            let recovered = log.recover().await.unwrap();
            assert_eq!(recovered.tip.sequence, u64::from(accepted));
            assert_eq!(recovered.term, 128);
        }
    }

    /// Recovery rejects a chain whose term decreases or exceeds the current
    /// term; term 0 (pre-term frames) stays valid below any current term.
    #[tokio::test]
    async fn memory_recover_rejects_decreasing_or_future_terms() {
        let log = MemoryLog::new(MemoryLogOptions::default()).unwrap();
        let log = log.reopen().reopen();
        assert_eq!(log.term(), 3);
        let records = test_records(2);
        for (terms, rejected) in [
            ([0, 3], None),
            ([3, 2], Some("decreases below previous term 3")),
            ([1, 4], Some("exceeds current term 3")),
        ] {
            let mut tip = LogTip::EMPTY;
            let mut frames = Vec::new();
            for (record, term) in records.iter().zip(terms) {
                let (frame, next) =
                    encode_frame(log.identity, tip.at(term), record.clone(), 1024 * 1024).unwrap();
                frames.push(frame);
                tip = next;
            }
            {
                let mut state = log.state.lock().unwrap();
                state.bytes = frames.iter().map(|frame| frame.len() as u64).sum();
                state.frames = frames;
                state.tip = tip;
            }
            match (log.recover().await, rejected) {
                (Ok(recovered), None) => assert_eq!(recovered.tip, tip),
                (Err(error), Some(marker)) => {
                    assert_eq!(error.code(), afs_error::CONFIG_INVALID);
                    assert!(error.to_string().contains(marker), "{error}");
                }
                (result, _) => panic!("terms {terms:?}: {:?}", result.map(|r| r.tip)),
            }
        }
    }
}
