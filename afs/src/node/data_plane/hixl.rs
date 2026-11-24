//! HIXL batch transfers, export ownership and slot integrity.

mod export;
mod metrics;

use afs_transport::hixl::{
    EngineInfo, HixlError, HixlErrorKind, HixlReadOutcome, HixlRuntime, HixlRuntimeGroup,
};
pub(crate) use export::{ExportProvider, PendingExport, runtime_export_provider};
pub use metrics::HixlMetrics;
pub(crate) use metrics::{Operation, observe_optional};
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::time::Instant;

pub(crate) fn runtime_error(error: HixlError) -> afs_error::Error {
    let code = match error.kind {
        HixlErrorKind::Unsupported => afs_error::NODE_TRANSFER_UNSUPPORTED,
        HixlErrorKind::InvalidArgument => afs_error::NODE_TRANSFER_INVALID,
        HixlErrorKind::ResourceExhausted => afs_error::NODE_TRANSFER_RESOURCE_EXHAUSTED,
        HixlErrorKind::DeadlineExceeded => afs_error::IO_TIMEOUT,
        HixlErrorKind::EnginePoisoned | HixlErrorKind::ShutdownIncomplete => {
            afs_error::NODE_TRANSFER_DRAIN_INCOMPLETE
        }
        HixlErrorKind::Internal => afs_error::NODE_TRANSFER_INTERNAL,
        HixlErrorKind::InvalidState | HixlErrorKind::NativeFailure => {
            afs_error::NODE_TRANSFER_UNAVAILABLE
        }
    };
    afs_error::Error::coded(code, error.to_string())
}

/// Native calls are the only replaceable boundary in the reader control path.
pub(crate) trait TransferLane: Send + Sync {
    fn connect(&self, peer: &str, deadline: Instant) -> Result<(), HixlError>;
    /// Allocates the private output for one native multi-window batch.
    fn prepare_read_batch_buffer(&self, length: usize) -> Result<Vec<u8>, HixlError>;
    /// Submits every window through one native SDK batch call and returns the
    /// filled output after all slots have been copied.
    fn read_batch_timed_into(
        &self,
        peer: &str,
        windows: &[(u64, usize)],
        output: Vec<u8>,
        deadline: Instant,
    ) -> Result<HixlReadOutcome, HixlError>;

    /// Completes a native WRITE into the peer's still-reserved windows.
    fn write_batch(
        &self,
        peer: &str,
        windows: &[(u64, usize)],
        bytes: Vec<u8>,
        deadline: Instant,
    ) -> Result<(), HixlError>;
}

/// The engine view the reader uses; windows and batches share one implementation.
pub(crate) trait TransferRuntime: Send + Sync {
    fn info(&self) -> EngineInfo;
    fn infos(&self) -> Vec<EngineInfo>;
    fn lane_count(&self) -> usize;
    fn lane(&self, lane: usize) -> Option<Arc<dyn TransferLane>>;
    fn quarantine(&self) -> Result<(), HixlError>;
}

impl TransferRuntime for HixlRuntimeGroup {
    fn info(&self) -> EngineInfo {
        self.info()
    }

    fn infos(&self) -> Vec<EngineInfo> {
        self.infos()
    }

    fn lane_count(&self) -> usize {
        self.len()
    }

    fn lane(&self, lane: usize) -> Option<Arc<dyn TransferLane>> {
        self.lane(lane)
            .map(|runtime| runtime as Arc<dyn TransferLane>)
    }

    fn quarantine(&self) -> Result<(), HixlError> {
        self.quarantine()
    }
}

impl TransferLane for HixlRuntime {
    fn connect(&self, peer: &str, deadline: Instant) -> Result<(), HixlError> {
        HixlRuntime::connect(self, peer, deadline)
    }

    fn prepare_read_batch_buffer(&self, length: usize) -> Result<Vec<u8>, HixlError> {
        HixlRuntime::prepare_read_batch_buffer(self, length)
    }

    fn read_batch_timed_into(
        &self,
        peer: &str,
        windows: &[(u64, usize)],
        output: Vec<u8>,
        deadline: Instant,
    ) -> Result<HixlReadOutcome, HixlError> {
        self.read_batch_timed_into(peer, windows, output, deadline)
    }

    fn write_batch(
        &self,
        peer: &str,
        windows: &[(u64, usize)],
        bytes: Vec<u8>,
        deadline: Instant,
    ) -> Result<(), HixlError> {
        HixlRuntime::write_batch(self, peer, windows, bytes, deadline)
    }
}

/// Digest of one HIXL slot, negotiated per session.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum SlotChecksum {
    Blake3,
    Crc32c,
}

impl SlotChecksum {
    /// Byte length of one slot digest.
    pub(crate) fn len(self) -> usize {
        match self {
            Self::Blake3 => blake3::OUT_LEN,
            Self::Crc32c => std::mem::size_of::<u32>(),
        }
    }

    fn matches(self, bytes: &[u8], expected: &[u8]) -> bool {
        match self {
            Self::Blake3 => blake3::hash(bytes).as_bytes() == expected,
            Self::Crc32c => super::checksum::checksum(bytes).to_le_bytes() == expected,
        }
    }

    pub(crate) fn digest(self, bytes: &[u8]) -> Vec<u8> {
        match self {
            Self::Blake3 => blake3::hash(bytes).as_bytes().to_vec(),
            Self::Crc32c => super::checksum::checksum(bytes)
                .to_le_bytes()
                .to_vec(),
        }
    }
}

/// A native READ left uncertain: it may still be in flight on the device.
fn uncertain_read(error: &HixlError) -> bool {
    error.read_may_be_in_flight
        || matches!(
            error.kind,
            HixlErrorKind::EnginePoisoned | HixlErrorKind::ShutdownIncomplete
        )
}

pub(crate) struct ReadWindow {
    pub address: u64,
    pub length: u64,
    pub checksum: Vec<u8>,
}

pub(crate) fn checked_slot_length(
    address: u64,
    generation: u64,
    length: u64,
    digest_bytes: usize,
    capacity: usize,
    checksum: SlotChecksum,
) -> afs_error::Result<usize> {
    usize::try_from(length)
        .ok()
        .filter(|length| {
            address != 0
                && generation != 0
                && (1..=capacity).contains(length)
                && digest_bytes == checksum.len()
        })
        .filter(|_| address.checked_add(length).is_some())
        .ok_or_else(|| protocol_error("HIXL slot descriptor is invalid"))
}

pub(crate) enum Completion {
    NotSubmitted,
    Completed,
    Unknown,
}

pub(crate) struct ReadFailure {
    pub error: afs_error::Error,
    pub completion: Completion,
    pub reuse_session: bool,
}

impl ReadFailure {
    fn before_submit(error: afs_error::Error) -> Self {
        Self {
            error,
            completion: Completion::NotSubmitted,
            reuse_session: true,
        }
    }

    fn after_completion(error: afs_error::Error) -> Self {
        Self {
            error,
            completion: Completion::Completed,
            reuse_session: false,
        }
    }
}

fn protocol_error(message: &'static str) -> afs_error::Error {
    afs_error::Error::coded(afs_error::CLIENT_PROTOCOL_VIOLATION, message)
}

fn worker_error(error: tokio::task::JoinError) -> afs_error::Error {
    afs_error::Error::coded(afs_error::CLIENT_WORKER_FAILED, error.to_string())
}

async fn quarantine(runtime: &Arc<dyn TransferRuntime>) {
    let runtime = runtime.clone();
    let _ = tokio::task::spawn_blocking(move || runtime.quarantine()).await;
}

/// The caller must let the owned operation finish if it stops waiting.
/// Native completion is reported separately from checksum validation.
pub(crate) async fn read_batch(
    runtime: &Arc<dyn TransferRuntime>,
    metrics: Option<&HixlMetrics>,
    lane_id: usize,
    engine: String,
    slots: Vec<ReadWindow>,
    checksum: SlotChecksum,
    deadline: Instant,
) -> Result<Vec<u8>, ReadFailure> {
    let bad_length = || ReadFailure::before_submit(protocol_error("HIXL read length overflow"));
    let windows = slots
        .iter()
        .map(|slot| {
            usize::try_from(slot.length)
                .map(|length| (slot.address, length))
                .map_err(|_| bad_length())
        })
        .collect::<Result<Vec<_>, _>>()?;
    let read = windows
        .iter()
        .try_fold(0_usize, |total, (_, length)| total.checked_add(*length))
        .ok_or_else(bad_length)?;
    let native = runtime
        .lane(lane_id)
        .ok_or_else(|| ReadFailure::before_submit(protocol_error("HIXL local lane disappeared")))?;
    let buffer = native
        .prepare_read_batch_buffer(read)
        .map_err(|error| ReadFailure::before_submit(runtime_error(error)))?;
    let started = Instant::now();
    let transfer = tokio::task::spawn_blocking(move || {
        native.read_batch_timed_into(&engine, &windows, buffer, deadline)
    })
    .await;
    let elapsed = started.elapsed();
    let outcome = match transfer {
        Ok(Ok(outcome)) => outcome,
        Ok(Err(error)) => {
            observe_optional(metrics, Operation::Read, elapsed, false);
            let uncertain = uncertain_read(&error);
            let completion = if error.read_may_be_in_flight {
                Completion::Unknown
            } else {
                Completion::NotSubmitted
            };
            if uncertain {
                quarantine(runtime).await;
            }
            let exhausted = !uncertain && error.kind == HixlErrorKind::ResourceExhausted;
            return Err(ReadFailure {
                error: runtime_error(error),
                completion,
                reuse_session: exhausted,
            });
        }
        Err(error) => {
            observe_optional(metrics, Operation::Read, elapsed, false);
            quarantine(runtime).await;
            return Err(ReadFailure {
                error: worker_error(error),
                completion: Completion::Unknown,
                reuse_session: false,
            });
        }
    };
    observe_optional(metrics, Operation::Read, elapsed, true);
    if let Some(metrics) = metrics {
        metrics.observe_lane_read(lane_id, elapsed, true);
        metrics.observe_read_timing(&outcome.timing);
        metrics.observe_native_batch(slots.len(), read);
    }
    let started = Instant::now();
    let (bytes, validated) = tokio::task::spawn_blocking(move || {
        let bytes = outcome.bytes;
        let validated = bytes.len() == read && validate_slots(&slots, &bytes, checksum);
        (bytes, validated)
    })
    .await
    .map_err(|error| ReadFailure::after_completion(worker_error(error)))?;
    observe_optional(metrics, Operation::Validate, started.elapsed(), validated);
    if !validated {
        return Err(ReadFailure::after_completion(afs_error::Error::coded(
            afs_error::NODE_TRANSFER_CORRUPT_DATA,
            "HIXL slot checksum differs",
        )));
    }
    if let Some(metrics) = metrics {
        metrics.record_validated_bytes(read as u64);
        metrics.record_lane_validated_bytes(lane_id, read as u64);
    }
    Ok(bytes)
}

/// Verifies every slot digest on the shared bounded hash pool.
fn validate_slots(slots: &[ReadWindow], bytes: &[u8], checksum: SlotChecksum) -> bool {
    let mut ranges = Vec::with_capacity(slots.len());
    let mut offset = 0_usize;
    for slot in slots {
        let Ok(length) = usize::try_from(slot.length) else {
            return false;
        };
        let Some(end) = offset.checked_add(length) else {
            return false;
        };
        let Some(data) = bytes.get(offset..end) else {
            return false;
        };
        ranges.push((data, slot.checksum.as_slice()));
        offset = end;
    }
    if offset != bytes.len() {
        return false;
    }
    let valid = AtomicBool::new(true);
    let check = |index: usize| {
        let (data, expected) = ranges[index];
        if !checksum.matches(data, expected) {
            valid.store(false, Ordering::Release);
        }
    };
    match validation_pool() {
        Some(pool) if ranges.len() > 1 => pool.install(|| {
            rayon_core::scope(|scope| {
                for index in 0..ranges.len() {
                    scope.spawn(move |_| check(index));
                }
            });
        }),
        _ => (0..ranges.len()).for_each(check),
    }
    valid.load(Ordering::Acquire)
}

/// Bounded hash pool for native window verification. Every window is hashed
/// independently, so a group completes in roughly one window's hash time
/// instead of the sum of its windows.
fn validation_pool() -> Option<&'static rayon_core::ThreadPool> {
    static POOL: std::sync::OnceLock<Option<rayon_core::ThreadPool>> = std::sync::OnceLock::new();
    POOL.get_or_init(|| {
        let workers = std::thread::available_parallelism()
            .map_or(4, |parallel| parallel.get())
            .clamp(1, 8);
        rayon_core::ThreadPoolBuilder::new()
            .num_threads(workers)
            .thread_name(|index| format!("afs-hixl-validate-{index}"))
            .build()
            .ok()
    })
    .as_ref()
}

#[cfg(test)]
mod tests {
    use super::{SlotChecksum, checked_slot_length};

    #[test]
    fn invalid_slot_ranges_never_reach_the_native_read() {
        let crc = SlotChecksum::Crc32c;
        assert_eq!(
            checked_slot_length(4096, 1, 4096, 4, 4096, crc).unwrap(),
            4096
        );
        for (address, generation, length, digest_bytes) in [
            (0, 1, 4096, 4),
            (4096, 0, 4096, 4),
            (4096, 1, 0, 4),
            (4096, 1, 4097, 4),
            (4096, 1, 4096, 32),
            (u64::MAX, 1, 1, 4),
        ] {
            let error = checked_slot_length(address, generation, length, digest_bytes, 4096, crc)
                .unwrap_err();
            assert_eq!(error.code(), afs_error::CLIENT_PROTOCOL_VIOLATION);
        }
    }
}
