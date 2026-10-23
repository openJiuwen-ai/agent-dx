//! Bounded operation-log contract and common identity/hash/CRC framing.
//! Sequence is a physical log position, independent of business revision.
use super::{BackendPersistence, MetaFuture, codec, log::Record};
use afs_error::{Error, Result};
use serde::{Deserialize, Serialize};
use std::{fs::File, io::Read, sync::Mutex};

pub const FRAME_OVERHEAD: usize = 40;
pub const LOG_HEADER_BYTES: u64 = 4096;
const VERSION: u16 = 2;

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
}
#[derive(Clone, Copy, Debug)]
pub struct LogLimits {
    pub record_max_bytes: usize,
    pub batch_max_bytes: usize,
    pub wal_max_bytes: u64,
    /// Soft rotation target; an atomic batch is never split between segments.
    pub segment_bytes: u64,
    pub segments_max: usize,
}
impl Default for LogLimits {
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
impl LogLimits {
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
        Ok(self)
    }
}
/// Logical snapshot admission; allocator/RSS must be measured separately.
#[derive(Clone, Copy, Debug)]
pub struct CheckpointLimits {
    pub image_max_bytes: usize,
    pub capture_charge_max_bytes: usize,
    pub disk_max_bytes: u64,
    pub files_max: usize,
}
impl Default for CheckpointLimits {
    fn default() -> Self {
        Self {
            image_max_bytes: 1024 * 1024 * 1024,
            capture_charge_max_bytes: 2 * 1024 * 1024 * 1024,
            disk_max_bytes: 4 * 1024 * 1024 * 1024,
            files_max: 8,
        }
    }
}
impl CheckpointLimits {
    pub fn validate(self) -> Result<Self> {
        let image = u64::try_from(self.image_max_bytes)
            .map_err(|_| invalid_log("checkpoint limit overflow"))?;
        if self.image_max_bytes < 256
            || self.capture_charge_max_bytes < self.image_max_bytes
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
pub struct RecoveredCheckpoint {
    pub covered: LogTip,
    pub bytes: Vec<u8>,
}
pub struct RecoveredLog {
    pub checkpoint: Option<RecoveredCheckpoint>,
    pub tip: LogTip,
    pub records: Vec<Record>,
}
/// Background thresholds are soft; byte/file admission remains a hard bound.
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
#[derive(Clone, Copy, Debug)]
pub struct LogStatus {
    pub tip: LogTip,
    pub checkpoint: LogTip,
    /// Conservative whole-segment charge. May include the covered prefix of
    /// one segment; never subtract this from measured E2E time.
    pub pending_bytes_upper_bound: u64,
}
pub trait MetaLog: Send + Sync {
    fn persistence(&self) -> BackendPersistence;
    fn limits(&self) -> LogLimits;
    fn health(&self) -> MetaFuture<'_, ()>;
    fn recover(&self) -> MetaFuture<'_, RecoveredLog>;
    fn maintenance_policy(&self) -> Option<MaintenancePolicy> {
        None
    }
    fn status(&self) -> MetaFuture<'_, LogStatus> {
        Box::pin(async { Err(unavailable("selected log has no maintenance status")) })
    }
    /// Read-only admission: false guarantees append was not attempted. An
    /// append error itself never implies safe retry (ENOSPC can be partial).
    fn can_append(&self, _expected: LogTip, _framed_bytes: usize) -> MetaFuture<'_, bool> {
        Box::pin(async { Ok(true) })
    }
    fn checkpoint_limits(&self) -> CheckpointLimits {
        CheckpointLimits::default()
    }
    fn install_checkpoint(&self, _covered: LogTip, _bytes: Vec<u8>) -> MetaFuture<'_, ()> {
        Box::pin(async { Err(unavailable("selected log does not install checkpoints")) })
    }
    fn append_and_sync(&self, expected: LogTip, records: Vec<Record>) -> MetaFuture<'_, LogTip>;
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
pub(super) fn frame_size(tip: LogTip, decision: &Record) -> Result<usize> {
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
        term: 0,
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
pub(super) fn unavailable(message: impl Into<String>) -> Error {
    Error::coded(afs_error::IO_UNAVAILABLE, message)
}
pub(super) fn full(message: impl Into<String>) -> Error {
    Error::coded(afs_error::IO_CAPACITY, message)
}

pub(super) fn encode_frame(
    identity: StoreIdentity,
    tip: LogTip,
    decision: Record,
    limit: usize,
) -> Result<(Vec<u8>, LogTip)> {
    decision.validate()?;
    let sequence = tip
        .sequence
        .checked_add(1)
        .ok_or_else(|| invalid_log("sequence exhausted"))?;
    let envelope = Envelope {
        version: VERSION,
        identity,
        sequence,
        base_sequence: tip.sequence,
        term: 0,
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
pub(super) fn decode_frame(
    identity: StoreIdentity,
    tip: LogTip,
    frame: &[u8],
    limit: usize,
) -> Result<(Record, LogTip)> {
    if frame.len() != frame_length(frame, limit)? {
        return Err(invalid_log("incomplete/trailing frame"));
    }
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
    let (envelope, tail): (Envelope, _) = postcard::take_from_bytes(payload)
        .map_err(|e| invalid_log(format!("decode envelope: {e}")))?;
    if !tail.is_empty()
        || envelope.version != VERSION
        || envelope.identity != identity
        || envelope.term != 0
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
    ))
}
pub(super) fn encode_batch(
    identity: StoreIdentity,
    tip: LogTip,
    records: Vec<Record>,
    limits: LogLimits,
) -> Result<(Vec<Vec<u8>>, LogTip, usize)> {
    if records.is_empty() || records.len() > 64 {
        return Err(invalid_log("batch record count outside 1..64"));
    }
    let mut frames = Vec::new();
    let mut tip = tip;
    let mut total = 0usize;
    for record in records {
        let (frame, next) = encode_frame(identity, tip, record, limits.record_max_bytes)?;
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
    policy: MaintenancePolicy,
    identity: StoreIdentity,
    limits: LogLimits,
    state: std::sync::Arc<Mutex<MemoryState>>,
}
struct MemoryState {
    checkpoint: Option<RecoveredCheckpoint>,
    tip: LogTip,
    base: LogTip,
    base_revision: super::StoreRevision,
    frames: Vec<Vec<u8>>,
    bytes: u64,
}
impl MemoryLog {
    pub fn new(limits: LogLimits) -> Result<Self> {
        Self::with_policy(limits, MaintenancePolicy::default())
    }
    /// Volatile authority with a validated, caller-selected maintenance policy.
    pub fn with_policy(limits: LogLimits, policy: MaintenancePolicy) -> Result<Self> {
        Ok(Self {
            policy: policy.validate()?,
            identity: StoreIdentity::new()?,
            limits: limits.validate()?,
            state: std::sync::Arc::new(Mutex::new(MemoryState {
                checkpoint: None,
                tip: LogTip::EMPTY,
                base: LogTip::EMPTY,
                base_revision: super::StoreRevision::ZERO,
                frames: Vec::new(),
                bytes: 0,
            })),
        })
    }
}
impl MetaLog for MemoryLog {
    fn persistence(&self) -> BackendPersistence {
        BackendPersistence::Volatile
    }
    fn limits(&self) -> LogLimits {
        self.limits
    }
    fn health(&self) -> MetaFuture<'_, ()> {
        Box::pin(async {
            drop(
                self.state
                    .lock()
                    .map_err(|_| unavailable("memory log poisoned"))?,
            );
            Ok(())
        })
    }
    fn maintenance_policy(&self) -> Option<MaintenancePolicy> {
        Some(self.policy)
    }
    fn status(&self) -> MetaFuture<'_, LogStatus> {
        Box::pin(async {
            let state = self
                .state
                .lock()
                .map_err(|_| unavailable("memory log poisoned"))?;
            Ok(LogStatus {
                tip: state.tip,
                checkpoint: state.base,
                pending_bytes_upper_bound: state.bytes,
            })
        })
    }
    fn can_append(&self, expected: LogTip, framed_bytes: usize) -> MetaFuture<'_, bool> {
        Box::pin(async move {
            let state = self
                .state
                .lock()
                .map_err(|_| unavailable("memory log poisoned"))?;
            if expected != state.tip {
                return Err(unavailable("memory admission tip changed"));
            }
            Ok(state
                .bytes
                .checked_add(framed_bytes as u64)
                .is_some_and(|bytes| bytes <= self.limits.wal_max_bytes))
        })
    }
    fn recover(&self) -> MetaFuture<'_, RecoveredLog> {
        Box::pin(async {
            let state = self
                .state
                .lock()
                .map_err(|_| unavailable("memory log poisoned"))?;
            let mut tip = state.base;
            let mut records = Vec::new();
            let mut revision = state.base_revision;
            for frame in &state.frames {
                let (record, next) =
                    decode_frame(self.identity, tip, frame, self.limits.record_max_bytes)?;
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
                records,
            })
        })
    }
    fn install_checkpoint(&self, covered: LogTip, bytes: Vec<u8>) -> MetaFuture<'_, ()> {
        let state = std::sync::Arc::clone(&self.state);
        let identity = self.identity;
        let record_limit = self.limits.record_max_bytes;
        let limits = self.checkpoint_limits();
        Box::pin(async move {
            tokio::task::spawn_blocking(move || {
                let limits = limits.validate()?;
                let revision =
                    super::log::StateImage::validate_encoded(&bytes, limits.image_max_bytes)?;
                let mut state = state
                    .lock()
                    .map_err(|_| unavailable("memory log poisoned"))?;
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
                    let (record, next) = decode_frame(identity, anchor, frame, record_limit)?;
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
            })
            .await
            .map_err(|error| unavailable(format!("memory checkpoint task: {error}")))?
        })
    }
    fn append_and_sync(&self, expected: LogTip, records: Vec<Record>) -> MetaFuture<'_, LogTip> {
        Box::pin(async move {
            let mut state = self
                .state
                .lock()
                .map_err(|_| unavailable("memory log poisoned"))?;
            if state.tip != expected {
                return Err(unavailable("log compare-and-append tip changed"));
            }
            let (frames, tip, bytes) = encode_batch(self.identity, expected, records, self.limits)?;
            let total = state
                .bytes
                .checked_add(bytes as u64)
                .filter(|size| *size <= self.limits.wal_max_bytes)
                .ok_or_else(|| full("memory WAL byte budget exhausted"))?;
            state.frames.extend(frames);
            state.tip = tip;
            state.bytes = total;
            Ok(tip)
        })
    }
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
        use super::super::local_log::LocalLog;
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
            let log = LocalLog::open(&root, LogLimits::default()).unwrap();
            let record = test_records(1).pop().unwrap();
            let (original, _) =
                encode_frame(log.identity(), LogTip::EMPTY, record.clone(), 1024 * 1024).unwrap();
            log.append_and_sync(LogTip::EMPTY, vec![record])
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
                frame[8] ^= 1;
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
            let limits = LogLimits {
                record_max_bytes: 1024,
                batch_max_bytes: 4096,
                wal_max_bytes: 128 * 1024,
                segment_bytes: 4608,
                segments_max: 16,
            };
            let log = LocalLog::open(&root, limits).unwrap();
            let mut tip = LogTip::EMPTY;
            for record in test_records(12) {
                tip = log.append_and_sync(tip, vec![record]).await.unwrap();
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
        let size = frame_size(LogTip::EMPTY, &record).unwrap();
        let (frame, tip) = encode_frame(identity, LogTip::EMPTY, record, 1024 * 1024).unwrap();
        assert_eq!(frame.len(), size);
        assert_eq!(
            decode_frame(identity, LogTip::EMPTY, &frame, size)
                .unwrap()
                .1,
            tip
        );
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
        hash_bad[8] ^= 1;
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
        let log = MemoryLog::new(LogLimits::default()).unwrap();
        let records = test_records(2);
        let first = super::super::state::State::default()
            .replay_record(&records[0])
            .unwrap();
        let (frame, covered) =
            encode_frame(log.identity, LogTip::EMPTY, records[0].clone(), 1024 * 1024).unwrap();
        let tip = log.append_and_sync(LogTip::EMPTY, records).await.unwrap();
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
        let log = MemoryLog::new(LogLimits::default()).unwrap();
        let tip = log
            .append_and_sync(LogTip::EMPTY, test_records(2))
            .await
            .unwrap();
        assert_eq!(tip.sequence, 2);
        let recovered = log.recover().await.unwrap();
        assert_eq!(recovered.records.len(), 2);
        assert_eq!(recovered.tip, tip);
        assert!(
            log.append_and_sync(LogTip::EMPTY, test_records(1))
                .await
                .is_err()
        );
        assert!(log.append_and_sync(tip, Vec::new()).await.is_err());
        assert_eq!(log.recover().await.unwrap().tip, tip);
        let tiny = MemoryLog::new(LogLimits {
            record_max_bytes: 256,
            batch_max_bytes: 256,
            wal_max_bytes: 8192,
            ..Default::default()
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
                    storage_devices: Vec::new(),
                    lease_ttl: std::time::Duration::from_secs(30),
                },
                12345,
                1024 * 1024,
            )
            .unwrap();
        let large = Record::from(large.unwrap());
        assert!(frame_size(LogTip::EMPTY, &large).unwrap() > tiny.limits().record_max_bytes);
        assert!(
            tiny.append_and_sync(LogTip::EMPTY, vec![large])
                .await
                .is_err()
        );
        assert_eq!(tiny.recover().await.unwrap().tip, LogTip::EMPTY);
        let quota = MemoryLog::new(LogLimits {
            record_max_bytes: 1024,
            batch_max_bytes: 1024,
            wal_max_bytes: 8192,
            ..Default::default()
        })
        .unwrap();
        let mut tip = LogTip::EMPTY;
        let mut exhausted = false;
        for record in test_records(64) {
            assert!(frame_size(tip, &record).unwrap() <= quota.limits().record_max_bytes);
            match quota.append_and_sync(tip, vec![record]).await {
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
        assert!(quota.state.lock().unwrap().bytes <= quota.limits().wal_max_bytes);
    }
}
