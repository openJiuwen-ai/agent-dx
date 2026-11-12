//! Optional HIXL rH2H transport runtime.
//!
//! The public boundary in this module owns only transport resources. AFS
//! grants, chunk identity, and source-window authorization stay in the node
//! control plane. When the `hixl` feature is enabled, one dedicated owner
//! thread performs every native call so the ACL context never crosses a Rust
//! task boundary.

#![allow(unsafe_code)]

#[cfg(any(feature = "hixl", test))]
use std::collections::HashSet;
use std::fmt;
#[cfg(feature = "hixl")]
use std::sync::Mutex;
#[cfg(feature = "hixl")]
use std::sync::mpsc::{self, Receiver, SyncSender, TrySendError};
use std::sync::{Arc, RwLock};
#[cfg(any(feature = "hixl", test))]
use std::thread;
#[cfg(feature = "hixl")]
use std::thread::JoinHandle;
use std::time::{Duration, Instant};

#[cfg(feature = "hixl")]
const HIXL_SUCCESS: u32 = 0;
#[cfg(any(feature = "hixl", test))]
const HIXL_TIMEOUT: u32 = 103_901;
const HIXL_UNSUPPORTED: u32 = 103_905;
const HIXL_RESOURCE_EXHAUSTED: u32 = 203_900;
const MAX_WINDOW_BYTES: usize = 4 * 1024 * 1024;
/// Relay HBM budget: `buffer_pool_count × buffer_pool_size_mib × 2 MiB`.
const MAX_POOL_BYTES: usize = 512 * 1024 * 1024;
/// One Host slot pool per device lane: 128 receive + 128 export slots of 4 MiB
/// windows. The configuration bounds the sum over all lanes.
const MAX_HOST_POOL_BYTES: usize = 1024 * 1024 * 1024;
#[cfg(feature = "hixl")]
const ERR_BYTES: usize = 512;
/// Maximum descriptors in one native READ. Mirrors `AFS_HIXL_MAX_BATCH_WINDOWS`.
pub const MAX_BATCH_WINDOWS: usize = 64;

/// Default fixed Host-window size.
pub const DEFAULT_WINDOW_BYTES: usize = 4 * 1024 * 1024;
/// Default number of local receive slots.
pub const DEFAULT_RECEIVE_SLOTS: usize = 16;
/// Default number of source export slots.
pub const DEFAULT_EXPORT_SLOTS: usize = 16;

mod group;
pub use group::{HixlRuntimeGroup, validate_group_options};

/// Immutable Host-to-Host read data path. HCCS relay requires HCCS-reachable
/// A3 devices.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub enum ReadTransport {
    #[default]
    Roce,
    HccsBufferPool,
}

impl fmt::Display for ReadTransport {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(match self {
            Self::Roce => "roce",
            Self::HccsBufferPool => "hccs-buffer-pool",
        })
    }
}

/// Parameters for one fixed HIXL runtime generation.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct RuntimeOptions {
    pub device_id: i32,
    pub engine_id: String,
    /// Optional local SDK device listener port; the peer engine_id port is separate.
    pub device_listen_port: Option<u16>,
    pub window_bytes: usize,
    pub receive_slots: usize,
    pub export_slots: usize,
    pub transport: ReadTransport,
    pub buffer_pool_count: usize,
    pub buffer_pool_size_mib: usize,
}

impl Default for RuntimeOptions {
    fn default() -> Self {
        Self {
            device_id: 0,
            engine_id: "127.0.0.1:7920".to_owned(),
            device_listen_port: None,
            window_bytes: DEFAULT_WINDOW_BYTES,
            receive_slots: DEFAULT_RECEIVE_SLOTS,
            export_slots: DEFAULT_EXPORT_SLOTS,
            transport: ReadTransport::Roce,
            buffer_pool_count: 4,
            buffer_pool_size_mib: 8,
        }
    }
}

impl RuntimeOptions {
    /// HCCS relay dimensions; direct RoCE carries none. Every caller lives
    /// behind the hixl feature.
    #[cfg(any(feature = "hixl", test))]
    fn buffer_pool_dims(&self) -> (usize, usize) {
        if self.transport == ReadTransport::HccsBufferPool {
            (self.buffer_pool_count, self.buffer_pool_size_mib)
        } else {
            (0, 0)
        }
    }

    fn relay_hbm_budget_bytes(&self) -> usize {
        if self.transport == ReadTransport::Roce {
            return 0;
        }
        // HIXL allocates two Device pools, each count * size MiB. Validation
        // rejects overflow and budgets above 512 MiB before any native entry.
        self.buffer_pool_count
            .saturating_mul(self.buffer_pool_size_mib)
            .saturating_mul(2 * 1024 * 1024)
    }
}

/// Stable engine identity returned after initialization.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct EngineInfo {
    pub engine_id: String,
    pub generation: u64,
    pub window_bytes: usize,
    pub transport: ReadTransport,
    /// Canonical relay dimensions; both are zero for direct RoCE.
    pub buffer_pool_count: usize,
    pub buffer_pool_size_mib: usize,
    /// Actual SDK-reported super-pod identity; relay peers must share this domain.
    pub hccs_domain_id: Option<u64>,
}

/// An authenticated source window descriptor backed by a fixed ACL Host slot.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ExportWindow {
    pub slot_id: u64,
    pub generation: u64,
    pub address: u64,
    pub length: usize,
}

/// Exclusive writable reservation for one export slot before publication.
///
/// The slot may contain bytes from a previous reservation. Publish only the
/// prefix filled by this reservation, and release it through
/// [`HixlRuntime::release_export`] whether or not it was published.
#[derive(Debug)]
pub struct ReservedExport {
    window: ExportWindow,
    address: *mut u8,
}

impl ReservedExport {
    /// Wraps a pinned allocation for its exclusive reservation holder.
    ///
    /// # Safety
    ///
    /// Starting at non-null `address`, `window.length` initialized bytes
    /// must be valid for reads and writes within one allocation and fit in an
    /// `isize`. Keep that memory pinned and exclusive until all borrows end
    /// and this reservation can no longer be used to access the bytes.
    pub unsafe fn from_raw(window: ExportWindow, address: *mut u8) -> Self {
        Self { window, address }
    }

    pub fn window(&self) -> &ExportWindow {
        &self.window
    }

    pub fn as_mut_slice(&mut self) -> &mut [u8] {
        // SAFETY: `from_raw` guarantees initialized, exclusive pinned memory;
        // `&mut self` prevents overlapping slices from this reservation.
        unsafe { std::slice::from_raw_parts_mut(self.address, self.window.length) }
    }
}

/// Completed HIXL read data and timing details from its ACL owner.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct HixlReadOutcome {
    pub bytes: Vec<u8>,
    pub timing: HixlReadTiming,
}

/// Per-read durations measured on the local reader process.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct HixlReadTiming {
    /// Time from enqueueing the read command until the ACL owner dequeues it.
    pub owner_queue: Duration,
    /// Native HIXL transfer duration, absent if the transport cannot report it.
    pub sdk_transfer: Option<Duration>,
    /// Copy from the pinned Host windows into the returned buffer, off the
    /// ACL owner thread; the windows are copied concurrently.
    pub output_copy: Option<Duration>,
}

/// Runtime lifecycle state exposed to health and diagnostics.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum RuntimeState {
    Stopped,
    Running,
    Quarantined,
    /// Native cleanup failed; every data-plane operation is refused, but
    /// shutdown may be retried because the native cleanup steps are
    /// idempotent.
    ShutdownFailed,
    Shutdown,
}

impl fmt::Display for RuntimeState {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        let name = match self {
            Self::Stopped => "stopped",
            Self::Running => "running",
            Self::Quarantined => "quarantined",
            Self::ShutdownFailed => "shutdown-failed",
            Self::Shutdown => "shutdown",
        };
        formatter.write_str(name)
    }
}

/// A bounded, redacted status snapshot. It intentionally omits raw VAs and
/// grant material.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct RuntimeStatus {
    pub state: RuntimeState,
    pub generation: u64,
    pub allocated_bytes: usize,
    pub quarantined_bytes: usize,
    /// Fixed source-export pool capacity for this generation.
    pub export_slots: usize,
    pub receive_slots_in_use: usize,
    pub export_slots_in_use: usize,
    pub transport: ReadTransport,
    /// Configured capacity of both internal HIXL HBM pools, not measured usage.
    pub relay_hbm_budget_bytes: usize,
    pub hccs_domain_id: Option<u64>,
}

/// A transport operation phase used in structured HIXL errors.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum HixlPhase {
    Start,
    Initialize,
    Connect,
    PrepareExport,
    ReleaseExport,
    SubmitRead,
    PollRead,
    Quarantine,
    Shutdown,
}

impl fmt::Display for HixlPhase {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        let name = match self {
            Self::Start => "start",
            Self::Initialize => "initialize",
            Self::Connect => "connect",
            Self::PrepareExport => "prepare_export",
            Self::ReleaseExport => "release_export",
            Self::SubmitRead => "submit_read",
            Self::PollRead => "poll_read",
            Self::Quarantine => "quarantine",
            Self::Shutdown => "shutdown",
        };
        formatter.write_str(name)
    }
}

/// Error category independent of the native HIXL numeric status.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum HixlErrorKind {
    Unsupported,
    InvalidArgument,
    InvalidState,
    ResourceExhausted,
    DeadlineExceeded,
    NativeFailure,
    CompletionFailed,
    EnginePoisoned,
    ShutdownIncomplete,
    Internal,
}

impl fmt::Display for HixlErrorKind {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        let name = match self {
            Self::Unsupported => "unsupported",
            Self::InvalidArgument => "invalid_argument",
            Self::InvalidState => "invalid_state",
            Self::ResourceExhausted => "resource_exhausted",
            Self::DeadlineExceeded => "deadline_exceeded",
            Self::NativeFailure => "native_failure",
            Self::CompletionFailed => "completion_failed",
            Self::EnginePoisoned => "engine_poisoned",
            Self::ShutdownIncomplete => "shutdown_incomplete",
            Self::Internal => "internal",
        };
        formatter.write_str(name)
    }
}

/// Structured caller-visible HIXL failure.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct HixlError {
    pub phase: HixlPhase,
    pub native_status: Option<u32>,
    pub kind: HixlErrorKind,
    pub message: String,
    /// For errors returned by `read`, true means the native submission boundary
    /// was entered or the owner reply was lost. False proves no DMA submission.
    pub read_may_be_in_flight: bool,
}

impl HixlError {
    fn new(
        phase: HixlPhase,
        kind: HixlErrorKind,
        native_status: Option<u32>,
        message: impl Into<String>,
    ) -> Self {
        Self {
            phase,
            native_status,
            kind,
            message: message.into(),
            read_may_be_in_flight: false,
        }
    }

    #[cfg(not(feature = "hixl"))]
    fn unsupported(phase: HixlPhase, message: impl Into<String>) -> Self {
        Self::new(
            phase,
            HixlErrorKind::Unsupported,
            Some(HIXL_UNSUPPORTED),
            message,
        )
    }

    #[cfg(any(feature = "hixl", test))]
    fn with_read_in_flight(mut self) -> Self {
        self.read_may_be_in_flight = true;
        self
    }
}

impl fmt::Display for HixlError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self.native_status {
            Some(status) => write!(
                formatter,
                "HIXL {} {} (native status {}): {}",
                self.phase, self.kind, status, self.message
            ),
            None => write!(
                formatter,
                "HIXL {} {}: {}",
                self.phase, self.kind, self.message
            ),
        }
    }
}

impl std::error::Error for HixlError {}

#[cfg(any(feature = "hixl", test))]
#[derive(Clone, Debug)]
struct NativeCallError {
    status: Option<u32>,
    message: String,
    accepted: bool,
}

#[cfg(any(feature = "hixl", test))]
impl NativeCallError {
    #[cfg(test)]
    fn new(status: Option<u32>, message: &str) -> Self {
        Self {
            status,
            message: message.to_owned(),
            accepted: false,
        }
    }
}

#[cfg(any(feature = "hixl", test))]
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
struct NativeRequest(u64);

#[cfg(any(feature = "hixl", test))]
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
struct NativeReadTiming {
    sdk_transfer: Option<Duration>,
}

/// One receive slot a completed READ left pinned for its caller to copy.
#[cfg(any(feature = "hixl", test))]
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
struct PinnedSlot {
    slot_id: usize,
    address: usize,
    length: usize,
}

/// A READ that finished on the ACL owner and still holds its receive slots.
#[cfg(any(feature = "hixl", test))]
#[derive(Debug)]
struct PinnedRead {
    output: Vec<u8>,
    slots: Vec<PinnedSlot>,
    timing: HixlReadTiming,
}

#[cfg(any(feature = "hixl", test))]
impl PinnedRead {
    fn slot_ids(&self) -> Vec<usize> {
        self.slots.iter().map(|slot| slot.slot_id).collect()
    }
}

#[cfg(any(feature = "hixl", test))]
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
struct NativeReadWindow {
    slot_id: u64,
    remote_addr: u64,
    len: usize,
    local_offset: usize,
}

#[cfg(any(feature = "hixl", test))]
#[derive(Clone, Debug, PartialEq, Eq)]
enum NativePoll {
    Waiting,
    Completed { timing: NativeReadTiming },
}

/// The only fakeable boundary in unit tests: every implementation below this
/// trait represents the external CANN/HIXL/ACL side of the transport.
#[cfg(any(feature = "hixl", test))]
trait NativeBackend: Send + 'static {
    #[cfg(feature = "hixl")]
    fn start(options: &RuntimeOptions) -> Result<Self, NativeCallError>
    where
        Self: Sized;
    fn hccs_domain_id(&self) -> Option<u64>;
    fn connect(&mut self, peer: &str, timeout_ms: u32) -> Result<(), NativeCallError>;
    fn reserve_export(
        &mut self,
        slot_id: u64,
        generation: u64,
        length: usize,
    ) -> Result<u64, NativeCallError>;
    fn release_export(&mut self, slot_id: u64, generation: u64) -> Result<(), NativeCallError>;
    /// Leaves every slot pinned on completion and fills `addresses`.
    fn poll_read(
        &mut self,
        request: NativeRequest,
        addresses: &mut [u64],
    ) -> Result<NativePoll, NativeCallError>;
    fn release_receives(&mut self, slot_ids: &[u64]) -> Result<(), NativeCallError>;
    fn submit_read_batch(
        &mut self,
        windows: &[NativeReadWindow],
        peer: &str,
    ) -> Result<NativeRequest, NativeCallError>;
    fn read_sync_batch(
        &mut self,
        windows: &[NativeReadWindow],
        peer: &str,
        addresses: &mut [u64],
        timeout_ms: u32,
    ) -> Result<NativeReadTiming, NativeCallError>;
    fn shutdown(&mut self) -> Result<(), NativeCallError>;
}

#[cfg(any(feature = "hixl", test))]
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum SlotState {
    Free,
    Active,
    Quarantined,
}

#[cfg(any(feature = "hixl", test))]
#[derive(Clone, Debug)]
struct ExportSlot {
    generation: u64,
    state: SlotState,
}

#[cfg(any(feature = "hixl", test))]
#[derive(Clone, Debug)]
struct ReceiveSlot {
    state: SlotState,
    request: Option<NativeRequest>,
}

/// Synchronous state machine executed exclusively by the ACL owner thread.
#[cfg(any(feature = "hixl", test))]
struct RuntimeCore<B> {
    backend: B,
    options: RuntimeOptions,
    info: EngineInfo,
    state: RuntimeState,
    connected_peers: HashSet<String>,
    exports: Vec<ExportSlot>,
    receives: Vec<ReceiveSlot>,
    quarantined_bytes: usize,
}

#[cfg(any(feature = "hixl", test))]
fn fresh_generation() -> Result<u64, HixlError> {
    let mut entropy = [0_u8; 8];
    getrandom::fill(&mut entropy).map_err(|error| {
        HixlError::new(
            HixlPhase::Initialize,
            HixlErrorKind::Internal,
            None,
            format!("cannot generate a fresh HIXL engine generation: {error}"),
        )
    })?;
    Ok(u64::from_ne_bytes(entropy).max(1))
}

#[cfg(any(feature = "hixl", test))]
impl<B: NativeBackend> RuntimeCore<B> {
    #[cfg(test)]
    fn new(backend: B, options: RuntimeOptions) -> Result<Self, HixlError> {
        Self::new_with_generation(backend, options, fresh_generation()?)
    }

    fn new_with_generation(
        backend: B,
        options: RuntimeOptions,
        generation: u64,
    ) -> Result<Self, HixlError> {
        validate_options(&options)?;
        let hccs_domain_id = if options.transport == ReadTransport::HccsBufferPool {
            Some(backend.hccs_domain_id().ok_or_else(|| {
                HixlError::new(
                    HixlPhase::Initialize,
                    HixlErrorKind::Unsupported,
                    Some(HIXL_UNSUPPORTED),
                    "HCCS relay requires a verifiable SDK super-pod identity",
                )
            })?)
        } else {
            None
        };
        let export_count = options.export_slots;
        let receive_count = options.receive_slots;
        let (buffer_pool_count, buffer_pool_size_mib) = options.buffer_pool_dims();
        Ok(Self {
            backend,
            info: EngineInfo {
                engine_id: options.engine_id.clone(),
                generation,
                window_bytes: options.window_bytes,
                transport: options.transport,
                hccs_domain_id,
                buffer_pool_count,
                buffer_pool_size_mib,
            },
            state: RuntimeState::Running,
            connected_peers: HashSet::new(),
            exports: (0..export_count)
                .map(|_| ExportSlot {
                    generation: 0,
                    state: SlotState::Free,
                })
                .collect(),
            receives: (0..receive_count)
                .map(|_| ReceiveSlot {
                    state: SlotState::Free,
                    request: None,
                })
                .collect(),
            options,
            quarantined_bytes: 0,
        })
    }

    fn info(&self) -> EngineInfo {
        self.info.clone()
    }

    fn status(&self) -> RuntimeStatus {
        RuntimeStatus {
            state: self.state,
            transport: self.options.transport,
            relay_hbm_budget_bytes: self.options.relay_hbm_budget_bytes(),
            hccs_domain_id: self.info.hccs_domain_id,
            generation: self.info.generation,
            allocated_bytes: self.options.window_bytes.saturating_mul(
                self.options
                    .receive_slots
                    .saturating_add(self.options.export_slots),
            ),
            quarantined_bytes: self.quarantined_bytes,
            export_slots: self.options.export_slots,
            receive_slots_in_use: self
                .receives
                .iter()
                .filter(|slot| slot.state != SlotState::Free)
                .count(),
            export_slots_in_use: self
                .exports
                .iter()
                .filter(|slot| slot.state != SlotState::Free)
                .count(),
        }
    }

    fn connect(&mut self, peer: &str, deadline: Instant) -> Result<(), HixlError> {
        self.ensure_running(HixlPhase::Connect)?;
        validate_peer(peer, HixlPhase::Connect)?;
        if self.connected_peers.contains(peer) {
            return Ok(());
        }
        let timeout_ms = deadline_timeout_ms(deadline, HixlPhase::Connect)?;
        if let Err(error) = self.backend.connect(peer, timeout_ms) {
            if error.accepted {
                self.state = RuntimeState::Quarantined;
                return Err(HixlError::new(
                    HixlPhase::Connect,
                    HixlErrorKind::EnginePoisoned,
                    error.status,
                    error.message,
                ));
            }
            return Err(map_native_error(
                HixlPhase::Connect,
                error,
                HixlErrorKind::NativeFailure,
            ));
        }
        self.connected_peers.insert(peer.to_owned());
        Ok(())
    }

    /// Pins a free slot for its caller to fill directly.
    fn reserve_export_slot(
        &mut self,
        length: usize,
        deadline: Instant,
    ) -> Result<ExportWindow, HixlError> {
        let (slot_id, generation) = self.claim_export_slot(length, deadline)?;
        let native = self
            .backend
            .reserve_export(slot_id as u64, generation, length);
        self.finish_export(slot_id, generation, length, native)
    }

    fn claim_export_slot(
        &mut self,
        length: usize,
        deadline: Instant,
    ) -> Result<(usize, u64), HixlError> {
        self.ensure_running(HixlPhase::PrepareExport)?;
        if length == 0 || length > self.options.window_bytes {
            return Err(HixlError::new(
                HixlPhase::PrepareExport,
                HixlErrorKind::InvalidArgument,
                None,
                format!(
                    "export length must be 1..{} bytes",
                    self.options.window_bytes
                ),
            ));
        }
        let _ = deadline_timeout_ms(deadline, HixlPhase::PrepareExport)?;
        let slot_id = self
            .exports
            .iter()
            .enumerate()
            .find(|(_, slot)| slot.state == SlotState::Free)
            .map(|(slot_id, _)| slot_id)
            .ok_or_else(|| {
                HixlError::new(
                    HixlPhase::PrepareExport,
                    HixlErrorKind::ResourceExhausted,
                    Some(HIXL_RESOURCE_EXHAUSTED),
                    "all export slots are pinned",
                )
            })?;
        let generation = self.exports[slot_id]
            .generation
            .checked_add(1)
            .ok_or_else(|| {
                HixlError::new(
                    HixlPhase::PrepareExport,
                    HixlErrorKind::Internal,
                    None,
                    "export slot generation exhausted",
                )
            })?;
        self.exports[slot_id].generation = generation;
        self.exports[slot_id].state = SlotState::Active;
        Ok((slot_id, generation))
    }

    fn finish_export(
        &mut self,
        slot_id: usize,
        generation: u64,
        length: usize,
        native: Result<u64, NativeCallError>,
    ) -> Result<ExportWindow, HixlError> {
        let address = match native {
            Ok(address) if address != 0 => address,
            Ok(_) => {
                self.quarantine_slot(false, slot_id);
                return Err(HixlError::new(
                    HixlPhase::PrepareExport,
                    HixlErrorKind::EnginePoisoned,
                    None,
                    "native export returned an empty address",
                ));
            }
            Err(error) => {
                if error.accepted {
                    self.quarantine_slot(false, slot_id);
                    return Err(HixlError::new(
                        HixlPhase::PrepareExport,
                        HixlErrorKind::EnginePoisoned,
                        error.status,
                        error.message,
                    ));
                }
                self.exports[slot_id].state = SlotState::Free;
                return Err(map_native_error(
                    HixlPhase::PrepareExport,
                    error,
                    HixlErrorKind::NativeFailure,
                ));
            }
        };
        Ok(ExportWindow {
            slot_id: slot_id as u64,
            generation,
            address,
            length,
        })
    }

    fn release_export(&mut self, slot_id: u64, generation: u64) -> Result<(), HixlError> {
        self.ensure_running(HixlPhase::ReleaseExport)?;
        let slot_index = usize::try_from(slot_id).map_err(|_| {
            HixlError::new(
                HixlPhase::ReleaseExport,
                HixlErrorKind::InvalidArgument,
                None,
                "unknown export slot",
            )
        })?;
        if slot_index >= self.exports.len() {
            return Err(HixlError::new(
                HixlPhase::ReleaseExport,
                HixlErrorKind::InvalidArgument,
                None,
                "unknown export slot",
            ));
        }
        if self.exports[slot_index].generation != generation || generation == 0 {
            return Err(HixlError::new(
                HixlPhase::ReleaseExport,
                HixlErrorKind::InvalidArgument,
                None,
                "export generation does not match the current slot",
            ));
        }
        if self.exports[slot_index].state == SlotState::Free {
            return Ok(());
        }
        if self.exports[slot_index].state == SlotState::Quarantined {
            return Err(HixlError::new(
                HixlPhase::ReleaseExport,
                HixlErrorKind::EnginePoisoned,
                None,
                "quarantined export slot cannot be reused",
            ));
        }
        if let Err(error) = self.backend.release_export(slot_id, generation) {
            self.quarantine_slot(false, slot_index);
            return Err(HixlError::new(
                HixlPhase::ReleaseExport,
                HixlErrorKind::EnginePoisoned,
                error.status,
                error.message,
            ));
        }
        self.exports[slot_index].state = SlotState::Free;
        Ok(())
    }

    fn free_receives(&mut self, slots: &[usize]) {
        for &slot_id in slots {
            self.receives[slot_id].state = SlotState::Free;
            self.receives[slot_id].request = None;
        }
    }

    fn quarantine_receives(&mut self, slots: &[usize]) {
        for &slot_id in slots {
            self.quarantine_slot(true, slot_id);
        }
    }

    /// One native READ call carrying every window in `windows`.  Each tuple is
    /// `(remote_addr, length)`. The slots stay pinned on success so the caller
    /// can copy them into `output` off this thread, then release them.
    fn read_batch(
        &mut self,
        peer: &str,
        windows: &[(u64, usize)],
        output: Vec<u8>,
        deadline: Instant,
        owner_queue: Duration,
    ) -> Result<PinnedRead, HixlError> {
        self.ensure_running(HixlPhase::SubmitRead)?;
        validate_peer(peer, HixlPhase::SubmitRead)?;
        if !self.connected_peers.contains(peer) {
            return Err(HixlError::new(
                HixlPhase::SubmitRead,
                HixlErrorKind::InvalidState,
                None,
                "peer must be connected before READ",
            ));
        }
        if windows.is_empty() || windows.len() > self.receives.len() {
            return Err(HixlError::new(
                HixlPhase::SubmitRead,
                HixlErrorKind::InvalidArgument,
                None,
                "batch READ window count is outside the receive pool",
            ));
        }
        let mut total = 0_usize;
        for (remote_addr, len) in windows {
            if *remote_addr == 0 || *len == 0 || *len > self.options.window_bytes {
                return Err(HixlError::new(
                    HixlPhase::SubmitRead,
                    HixlErrorKind::InvalidArgument,
                    None,
                    "batch READ address and length are outside the exported window",
                ));
            }
            total = total.checked_add(*len).ok_or_else(|| {
                HixlError::new(
                    HixlPhase::SubmitRead,
                    HixlErrorKind::InvalidArgument,
                    None,
                    "batch READ byte count overflows",
                )
            })?;
        }
        if !output.is_empty() || output.capacity() < total {
            return Err(HixlError::new(
                HixlPhase::SubmitRead,
                HixlErrorKind::InvalidArgument,
                None,
                "batch READ output cannot hold the submitted windows",
            ));
        }
        let timeout_ms = deadline_timeout_ms(deadline, HixlPhase::SubmitRead)?;
        let mut slots = Vec::with_capacity(windows.len());
        for _ in windows {
            let Some(slot_id) = self
                .receives
                .iter()
                .position(|slot| slot.state == SlotState::Free)
            else {
                self.free_receives(&slots);
                return Err(HixlError::new(
                    HixlPhase::SubmitRead,
                    HixlErrorKind::ResourceExhausted,
                    Some(HIXL_RESOURCE_EXHAUSTED),
                    "all receive slots are in use or quarantined",
                ));
            };
            self.receives[slot_id].state = SlotState::Active;
            slots.push(slot_id);
        }
        let mut local_offset = 0_usize;
        let native_windows = windows
            .iter()
            .zip(&slots)
            .map(|((remote_addr, len), slot_id)| {
                let window = NativeReadWindow {
                    slot_id: *slot_id as u64,
                    remote_addr: *remote_addr,
                    len: *len,
                    local_offset,
                };
                local_offset += *len;
                window
            })
            .collect::<Vec<_>>();
        let mut addresses = vec![0_u64; native_windows.len()];
        if self.options.transport == ReadTransport::HccsBufferPool {
            let timing = match self.backend.read_sync_batch(
                &native_windows,
                peer,
                &mut addresses,
                timeout_ms,
            ) {
                Ok(timing) => timing,
                Err(error) => {
                    if error.accepted {
                        self.quarantine_receives(&slots);
                        let kind = if error.status == Some(HIXL_TIMEOUT) {
                            HixlErrorKind::DeadlineExceeded
                        } else {
                            HixlErrorKind::EnginePoisoned
                        };
                        return Err(HixlError::new(
                            HixlPhase::SubmitRead,
                            kind,
                            error.status,
                            error.message,
                        )
                        .with_read_in_flight());
                    }
                    self.free_receives(&slots);
                    return Err(map_native_error(
                        HixlPhase::SubmitRead,
                        error,
                        HixlErrorKind::NativeFailure,
                    ));
                }
            };
            return self.pin_completed(
                &slots,
                &native_windows,
                &addresses,
                output,
                owner_queue,
                timing.sdk_transfer,
            );
        }
        let request = match self.backend.submit_read_batch(&native_windows, peer) {
            Ok(request) => request,
            Err(error) => {
                if error.accepted {
                    self.quarantine_receives(&slots);
                    return Err(HixlError::new(
                        HixlPhase::SubmitRead,
                        HixlErrorKind::EnginePoisoned,
                        error.status,
                        error.message,
                    )
                    .with_read_in_flight());
                }
                self.free_receives(&slots);
                return Err(map_native_error(
                    HixlPhase::SubmitRead,
                    error,
                    HixlErrorKind::NativeFailure,
                ));
            }
        };
        for &slot_id in &slots {
            self.receives[slot_id].request = Some(request);
        }
        loop {
            if Instant::now() >= deadline {
                self.quarantine_receives(&slots);
                return Err(HixlError::new(
                    HixlPhase::PollRead,
                    HixlErrorKind::DeadlineExceeded,
                    Some(HIXL_TIMEOUT),
                    "accepted batch READ did not complete before the deadline",
                )
                .with_read_in_flight());
            }
            let poll = match self.backend.poll_read(request, &mut addresses) {
                Ok(poll) => poll,
                Err(error) => {
                    self.quarantine_receives(&slots);
                    return Err(HixlError::new(
                        HixlPhase::PollRead,
                        if error.status == Some(HIXL_TIMEOUT) {
                            HixlErrorKind::DeadlineExceeded
                        } else {
                            HixlErrorKind::CompletionFailed
                        },
                        error.status,
                        error.message,
                    )
                    .with_read_in_flight());
                }
            };
            match poll {
                NativePoll::Waiting => thread::sleep(Duration::from_millis(1)),
                NativePoll::Completed { timing } => {
                    for &slot_id in &slots {
                        self.receives[slot_id].request = None;
                    }
                    return self.pin_completed(
                        &slots,
                        &native_windows,
                        &addresses,
                        output,
                        owner_queue,
                        timing.sdk_transfer,
                    );
                }
            }
        }
    }

    fn pin_completed(
        &mut self,
        slots: &[usize],
        windows: &[NativeReadWindow],
        addresses: &[u64],
        output: Vec<u8>,
        owner_queue: Duration,
        sdk_transfer: Option<Duration>,
    ) -> Result<PinnedRead, HixlError> {
        let mut pinned = Vec::with_capacity(slots.len());
        for ((&slot_id, window), address) in slots.iter().zip(windows).zip(addresses) {
            let Some(address) = usize::try_from(*address).ok().filter(|value| *value != 0) else {
                self.quarantine_receives(slots);
                return Err(HixlError::new(
                    HixlPhase::PollRead,
                    HixlErrorKind::EnginePoisoned,
                    None,
                    "native READ completed without a usable slot address",
                ));
            };
            pinned.push(PinnedSlot {
                slot_id,
                address,
                length: window.len,
            });
        }
        Ok(PinnedRead {
            output,
            slots: pinned,
            timing: HixlReadTiming {
                owner_queue,
                sdk_transfer,
                output_copy: None,
            },
        })
    }

    fn release_receives(&mut self, slots: &[usize]) -> Result<(), HixlError> {
        self.ensure_running(HixlPhase::PollRead)?;
        let pinned = slots.iter().all(|&slot_id| {
            self.receives
                .get(slot_id)
                .is_some_and(|slot| slot.state == SlotState::Active)
        });
        if !pinned {
            return Err(HixlError::new(
                HixlPhase::PollRead,
                HixlErrorKind::InvalidArgument,
                None,
                "receive slot is not pinned",
            ));
        }
        let slot_ids = slots
            .iter()
            .map(|&slot_id| slot_id as u64)
            .collect::<Vec<_>>();
        if let Err(error) = self.backend.release_receives(&slot_ids) {
            self.quarantine_receives(slots);
            return Err(HixlError::new(
                HixlPhase::PollRead,
                HixlErrorKind::EnginePoisoned,
                error.status,
                error.message,
            ));
        }
        self.free_receives(slots);
        Ok(())
    }

    fn shutdown(&mut self) -> Result<(), HixlError> {
        if self.state == RuntimeState::Shutdown {
            return Ok(());
        }
        if self.state == RuntimeState::Quarantined
            || self
                .exports
                .iter()
                .any(|slot| slot.state != SlotState::Free)
            || self
                .receives
                .iter()
                .any(|slot| slot.state != SlotState::Free)
        {
            return Err(HixlError::new(
                HixlPhase::Shutdown,
                HixlErrorKind::ShutdownIncomplete,
                None,
                "native engine or Host pool still has an admitted reference",
            ));
        }
        if let Err(error) = self.backend.shutdown() {
            // A failed cleanup is retryable: the native steps are idempotent,
            // and data-plane entry stays closed until one attempt completes.
            self.state = RuntimeState::ShutdownFailed;
            return Err(HixlError::new(
                HixlPhase::Shutdown,
                HixlErrorKind::ShutdownIncomplete,
                error.status,
                error.message,
            ));
        }
        self.state = RuntimeState::Shutdown;
        Ok(())
    }

    fn quarantine(&mut self) -> Result<(), HixlError> {
        if self.state == RuntimeState::Shutdown {
            return Err(HixlError::new(
                HixlPhase::Quarantine,
                HixlErrorKind::InvalidState,
                None,
                "cannot quarantine a shut down HIXL generation",
            ));
        }
        if self.state == RuntimeState::Quarantined {
            return Ok(());
        }
        if self.state == RuntimeState::ShutdownFailed {
            // Data-plane entry is already closed; keeping the state retryable
            // lets a later shutdown attempt still reclaim native resources.
            return Ok(());
        }
        for slot_id in 0..self.exports.len() {
            if self.exports[slot_id].state == SlotState::Active {
                self.quarantine_slot(false, slot_id);
            }
        }
        for slot_id in 0..self.receives.len() {
            if self.receives[slot_id].state == SlotState::Active {
                self.quarantine_slot(true, slot_id);
            }
        }
        self.state = RuntimeState::Quarantined;
        Ok(())
    }

    fn ensure_running(&self, phase: HixlPhase) -> Result<(), HixlError> {
        match self.state {
            RuntimeState::Running => Ok(()),
            RuntimeState::Quarantined => Err(HixlError::new(
                phase,
                HixlErrorKind::EnginePoisoned,
                None,
                "HIXL generation is quarantined after an uncertain native operation",
            )),
            RuntimeState::ShutdownFailed => Err(HixlError::new(
                phase,
                HixlErrorKind::ShutdownIncomplete,
                None,
                "HIXL generation failed to clean up; only shutdown may be retried",
            )),
            RuntimeState::Shutdown | RuntimeState::Stopped => Err(HixlError::new(
                phase,
                HixlErrorKind::InvalidState,
                None,
                format!("runtime is {}", self.state),
            )),
        }
    }

    /// Marks one receive or export slot quarantined, billing its window once,
    /// and stops admitting new work on the whole generation.
    fn quarantine_slot(&mut self, receive: bool, slot_id: usize) {
        let slot = if receive {
            &mut self.receives[slot_id].state
        } else {
            &mut self.exports[slot_id].state
        };
        if *slot != SlotState::Quarantined {
            *slot = SlotState::Quarantined;
            self.quarantined_bytes = self
                .quarantined_bytes
                .saturating_add(self.options.window_bytes);
        }
        self.state = RuntimeState::Quarantined;
    }
}

fn validate_options(options: &RuntimeOptions) -> Result<(), HixlError> {
    if options.device_listen_port == Some(0) {
        return Err(HixlError::new(
            HixlPhase::Initialize,
            HixlErrorKind::InvalidArgument,
            None,
            "hixl.device_listen_port must be 1..=65535 when configured",
        ));
    }
    if options.device_id < 0
        || options.engine_id.is_empty()
        || options.engine_id.contains('\0')
        || options.window_bytes == 0
        || options.window_bytes > MAX_WINDOW_BYTES
        || options.receive_slots == 0
        || options.export_slots == 0
        || options.receive_slots > 128
        || options.export_slots > 128
        || !(1..=64).contains(&options.buffer_pool_count)
        || !(1..=64).contains(&options.buffer_pool_size_mib)
    {
        return Err(HixlError::new(
            HixlPhase::Initialize,
            HixlErrorKind::InvalidArgument,
            None,
            "invalid HIXL runtime options",
        ));
    }
    let slots = options
        .receive_slots
        .checked_add(options.export_slots)
        .ok_or_else(|| {
            HixlError::new(
                HixlPhase::Initialize,
                HixlErrorKind::InvalidArgument,
                None,
                "HIXL pool slot count overflows",
            )
        })?;
    let bytes = options.window_bytes.checked_mul(slots).ok_or_else(|| {
        HixlError::new(
            HixlPhase::Initialize,
            HixlErrorKind::InvalidArgument,
            None,
            "HIXL Host pool byte count overflows",
        )
    })?;
    if bytes > MAX_HOST_POOL_BYTES {
        return Err(HixlError::new(
            HixlPhase::Initialize,
            HixlErrorKind::ResourceExhausted,
            Some(HIXL_RESOURCE_EXHAUSTED),
            format!("HIXL Host pool exceeds {} bytes", MAX_HOST_POOL_BYTES),
        ));
    }
    if options.relay_hbm_budget_bytes() > MAX_POOL_BYTES {
        return Err(HixlError::new(
            HixlPhase::Initialize,
            HixlErrorKind::ResourceExhausted,
            Some(HIXL_RESOURCE_EXHAUSTED),
            "HIXL BufferPool exceeds the 512 MiB combined HBM budget",
        ));
    }
    Ok(())
}

#[cfg(any(feature = "hixl", test))]
fn validate_peer(peer: &str, phase: HixlPhase) -> Result<(), HixlError> {
    if peer.is_empty() || peer.contains('\0') {
        return Err(HixlError::new(
            phase,
            HixlErrorKind::InvalidArgument,
            None,
            "HIXL peer engine id must be non-empty and NUL-free",
        ));
    }
    Ok(())
}

#[cfg(any(feature = "hixl", test))]
fn deadline_timeout_ms(deadline: Instant, phase: HixlPhase) -> Result<u32, HixlError> {
    let remaining = deadline.saturating_duration_since(Instant::now());
    if remaining.is_zero() {
        return Err(HixlError::new(
            phase,
            HixlErrorKind::DeadlineExceeded,
            Some(HIXL_TIMEOUT),
            "HIXL operation deadline has elapsed",
        ));
    }
    let millis = remaining.as_millis().min(i32::MAX as u128) as u32;
    Ok(millis.max(1))
}

#[cfg(any(feature = "hixl", test))]
fn map_native_error(
    phase: HixlPhase,
    error: NativeCallError,
    default_kind: HixlErrorKind,
) -> HixlError {
    let kind = if phase == HixlPhase::Initialize && error.accepted {
        HixlErrorKind::ShutdownIncomplete
    } else {
        match error.status {
            Some(HIXL_TIMEOUT) => HixlErrorKind::DeadlineExceeded,
            Some(HIXL_RESOURCE_EXHAUSTED) => HixlErrorKind::ResourceExhausted,
            Some(HIXL_UNSUPPORTED) => HixlErrorKind::Unsupported,
            _ => default_kind,
        }
    };
    HixlError::new(phase, kind, error.status, error.message)
}

#[cfg(feature = "hixl")]
enum Command {
    Connect {
        peer: String,
        deadline: Instant,
        reply: SyncSender<Result<(), HixlError>>,
    },
    ReserveExport {
        length: usize,
        deadline: Instant,
        reply: SyncSender<Result<ExportWindow, HixlError>>,
    },
    ReleaseExport {
        slot_id: u64,
        generation: u64,
        reply: SyncSender<Result<(), HixlError>>,
    },
    ReadBatch {
        peer: String,
        windows: Vec<(u64, usize)>,
        output: Vec<u8>,
        queued_at: Instant,
        deadline: Instant,
        reply: SyncSender<Result<PinnedRead, HixlError>>,
    },
    ReleaseReceives {
        slots: Vec<usize>,
        reply: SyncSender<Result<(), HixlError>>,
    },
    Quarantine {
        reply: SyncSender<Result<(), HixlError>>,
    },
    Shutdown {
        reply: SyncSender<Result<(), HixlError>>,
    },
}

/// A started HIXL runtime and its dedicated ACL owner thread.
pub struct HixlRuntime {
    info: EngineInfo,
    #[cfg(feature = "hixl")]
    sender: SyncSender<Command>,
    status: Arc<RwLock<RuntimeStatus>>,
    #[cfg(feature = "hixl")]
    owner: Mutex<Option<JoinHandle<()>>>,
}

impl HixlRuntime {
    /// Starts one fixed pool generation. Without the optional native feature,
    /// this method always returns a structured `Unsupported` error.
    pub fn start(options: RuntimeOptions) -> Result<Arc<Self>, HixlError> {
        #[cfg(not(feature = "hixl"))]
        {
            let _ = options;
            Err(HixlError::unsupported(
                HixlPhase::Start,
                "afs-transport was built without the hixl feature",
            ))
        }
        #[cfg(feature = "hixl")]
        {
            validate_options(&options).map_err(|mut error| {
                error.phase = HixlPhase::Start;
                error
            })?;
            let capacity = options
                .receive_slots
                .checked_add(options.export_slots)
                .and_then(|slots| slots.checked_add(8))
                .ok_or_else(|| {
                    HixlError::new(
                        HixlPhase::Start,
                        HixlErrorKind::ResourceExhausted,
                        None,
                        "HIXL command budget overflow",
                    )
                })?;
            let generation = fresh_generation()?;
            let (sender, receiver) = mpsc::sync_channel(capacity);
            let (startup_sender, startup_receiver) = mpsc::sync_channel(1);
            let status = Arc::new(RwLock::new(RuntimeStatus {
                transport: options.transport,
                relay_hbm_budget_bytes: options.relay_hbm_budget_bytes(),
                hccs_domain_id: None,
                state: RuntimeState::Stopped,
                generation: 0,
                allocated_bytes: 0,
                quarantined_bytes: 0,
                export_slots: 0,
                receive_slots_in_use: 0,
                export_slots_in_use: 0,
            }));
            let owner_status = Arc::clone(&status);
            let owner_options = options.clone();
            let owner = thread::Builder::new()
                .name("afs-hixl-acl-owner".to_owned())
                .spawn(move || {
                    let result = RealNative::start(&owner_options)
                        .map_err(|error| {
                            map_native_error(
                                HixlPhase::Initialize,
                                error,
                                HixlErrorKind::NativeFailure,
                            )
                        })
                        .and_then(|backend| {
                            RuntimeCore::new_with_generation(backend, owner_options, generation)
                        });
                    match result {
                        Ok(core) => {
                            let info = core.info();
                            update_status(&owner_status, core.status());
                            if startup_sender.send(Ok(info)).is_err() {
                                return;
                            }
                            owner_loop(core, receiver, owner_status);
                        }
                        Err(error) => {
                            let _ = startup_sender.send(Err(error));
                        }
                    }
                })
                .map_err(|error| {
                    HixlError::new(
                        HixlPhase::Start,
                        HixlErrorKind::Internal,
                        None,
                        format!("failed to spawn HIXL ACL owner: {error}"),
                    )
                })?;
            let info = match startup_receiver.recv() {
                Ok(Ok(info)) => info,
                Ok(Err(error)) => {
                    let _ = owner.join();
                    return Err(error);
                }
                Err(error) => {
                    let _ = owner.join();
                    return Err(HixlError::new(
                        HixlPhase::Start,
                        HixlErrorKind::Internal,
                        None,
                        format!("HIXL ACL owner stopped before startup: {error}"),
                    ));
                }
            };
            Ok(Arc::new(Self {
                info,
                sender,
                status,
                owner: Mutex::new(Some(owner)),
            }))
        }
    }

    #[must_use]
    pub fn info(&self) -> EngineInfo {
        self.info.clone()
    }

    #[must_use]
    pub fn status(&self) -> RuntimeStatus {
        match self.status.read() {
            Ok(status) => status.clone(),
            Err(poisoned) => poisoned.into_inner().clone(),
        }
    }

    pub fn connect(&self, peer: &str, deadline: Instant) -> Result<(), HixlError> {
        #[cfg(not(feature = "hixl"))]
        {
            let _ = (peer, deadline);
            Err(HixlError::unsupported(
                HixlPhase::Connect,
                "afs-transport was built without the hixl feature",
            ))
        }
        #[cfg(feature = "hixl")]
        {
            let (reply, receiver) = mpsc::sync_channel(1);
            self.send(
                Command::Connect {
                    peer: peer.to_owned(),
                    deadline,
                    reply,
                },
                HixlPhase::Connect,
            )?;
            recv_reply(receiver, HixlPhase::Connect)
        }
    }

    /// Pins one export slot of `length` bytes for the caller to fill in place,
    /// so the window never passes through a private copy. Release it through
    /// [`Self::release_export`] whether or not it was published.
    pub fn reserve_export(
        &self,
        length: usize,
        deadline: Instant,
    ) -> Result<ReservedExport, HixlError> {
        #[cfg(not(feature = "hixl"))]
        {
            let _ = (length, deadline);
            Err(HixlError::unsupported(
                HixlPhase::PrepareExport,
                "afs-transport was built without the hixl feature",
            ))
        }
        #[cfg(feature = "hixl")]
        {
            let (reply, receiver) = mpsc::sync_channel(1);
            self.send(
                Command::ReserveExport {
                    length,
                    deadline,
                    reply,
                },
                HixlPhase::PrepareExport,
            )?;
            let window = recv_reply(receiver, HixlPhase::PrepareExport)?;
            let address = usize::try_from(window.address).map_err(|_| {
                HixlError::new(
                    HixlPhase::PrepareExport,
                    HixlErrorKind::Internal,
                    None,
                    "native export address does not fit this process",
                )
            })?;
            // SAFETY: the export pool is initialized at startup. Native has
            // pinned this allocation of at least `length` bytes to this
            // reservation, and shutdown and quarantine retain pinned slots.
            Ok(unsafe {
                ReservedExport::from_raw(window, std::ptr::with_exposed_provenance_mut(address))
            })
        }
    }

    /// Returns the export slot to the pool for reuse.
    ///
    /// # Safety
    ///
    /// Every Rust borrow of the slot must have ended and no reservation may
    /// access the slot again. If the descriptor was published, the caller
    /// must also have established that no native READ can still access it.
    ///
    /// ```compile_fail,E0133
    /// use afs_transport::hixl::HixlRuntime;
    ///
    /// fn release_by_id(runtime: &HixlRuntime, slot_id: u64, generation: u64) {
    ///     runtime.release_export(slot_id, generation).unwrap();
    /// }
    /// ```
    pub unsafe fn release_export(&self, slot_id: u64, generation: u64) -> Result<(), HixlError> {
        #[cfg(not(feature = "hixl"))]
        {
            let _ = (slot_id, generation);
            Err(HixlError::unsupported(
                HixlPhase::ReleaseExport,
                "afs-transport was built without the hixl feature",
            ))
        }
        #[cfg(feature = "hixl")]
        {
            let (reply, receiver) = mpsc::sync_channel(1);
            self.send(
                Command::ReleaseExport {
                    slot_id,
                    generation,
                    reply,
                },
                HixlPhase::ReleaseExport,
            )?;
            recv_reply(receiver, HixlPhase::ReleaseExport)
        }
    }

    /// Reserves capacity for one native multi-window batch. The result stays
    /// empty until every byte has been copied from the completed receive slots.
    pub fn prepare_read_batch_buffer(&self, len: usize) -> Result<Vec<u8>, HixlError> {
        let limit = self.info.window_bytes.saturating_mul(MAX_BATCH_WINDOWS);
        if len == 0 || len > limit {
            return Err(HixlError::new(
                HixlPhase::SubmitRead,
                HixlErrorKind::InvalidArgument,
                None,
                format!("batch read buffer length must be in 1..={limit} bytes"),
            ));
        }
        let mut output = Vec::new();
        output.try_reserve_exact(len).map_err(|error| {
            HixlError::new(
                HixlPhase::SubmitRead,
                HixlErrorKind::ResourceExhausted,
                Some(HIXL_RESOURCE_EXHAUSTED),
                format!("cannot reserve HIXL batch read output buffer: {error}"),
            )
        })?;
        Ok(output)
    }

    /// Reads all windows in one native batch into an empty reserved vector.
    /// The ACL owner handles the transfer; callers copy the pinned slots in
    /// order outside the owner thread and publish only a complete buffer.
    pub fn read_batch_timed_into(
        &self,
        peer: &str,
        windows: &[(u64, usize)],
        output: Vec<u8>,
        deadline: Instant,
    ) -> Result<HixlReadOutcome, HixlError> {
        #[cfg(not(feature = "hixl"))]
        {
            let _ = (peer, windows, output, deadline);
            Err(HixlError::unsupported(
                HixlPhase::SubmitRead,
                "afs-transport was built without the hixl feature",
            ))
        }
        #[cfg(feature = "hixl")]
        {
            let (reply, receiver) = mpsc::sync_channel(1);
            let queued_at = Instant::now();
            self.send(
                Command::ReadBatch {
                    peer: peer.to_owned(),
                    windows: windows.to_vec(),
                    output,
                    queued_at,
                    deadline,
                    reply,
                },
                HixlPhase::SubmitRead,
            )?;
            let mut pinned = recv_reply(receiver, HixlPhase::PollRead)?;
            let copy_started = Instant::now();
            let length = pinned.slots.iter().map(|slot| slot.length).sum();
            let copied = copy_pinned(&pinned.slots, &mut pinned.output, length);
            let output_copy = copy_started.elapsed();
            self.release_pinned(pinned.slot_ids());
            copied?;
            Ok(HixlReadOutcome {
                bytes: pinned.output,
                timing: HixlReadTiming {
                    output_copy: Some(output_copy),
                    ..pinned.timing
                },
            })
        }
    }

    /// Keep responsibility for these slots until the owner accepts release.
    /// Completed bytes stay valid even when native release quarantines a slot.
    #[cfg(feature = "hixl")]
    fn release_pinned(&self, slots: Vec<usize>) {
        let (reply, receiver) = mpsc::sync_channel(1);
        // Queue pressure is a valid rejection before admitting a read, but a
        // completed read already owns slots. Wait for queue space and then the
        // owner's reply. The owner records native release errors in its state.
        if self
            .sender
            .send(Command::ReleaseReceives { slots, reply })
            .is_err()
            || receiver.recv().is_err()
        {
            // A disconnected owner cannot prove native cleanup. RealNative
            // retains the engine and entire Host pool on owner loss.
            let mut status = match self.status.write() {
                Ok(status) => status,
                Err(poisoned) => poisoned.into_inner(),
            };
            if status.state != RuntimeState::Shutdown {
                status.state = RuntimeState::Quarantined;
                status.quarantined_bytes = status.allocated_bytes;
            }
        }
    }

    pub fn shutdown(&self) -> Result<(), HixlError> {
        #[cfg(not(feature = "hixl"))]
        {
            Err(HixlError::unsupported(
                HixlPhase::Shutdown,
                "afs-transport was built without the hixl feature",
            ))
        }
        #[cfg(feature = "hixl")]
        {
            if self.status().state == RuntimeState::Shutdown {
                return Ok(());
            }
            let (reply, receiver) = mpsc::sync_channel(1);
            self.send(Command::Shutdown { reply }, HixlPhase::Shutdown)?;
            let result = recv_reply(receiver, HixlPhase::Shutdown);
            if result.is_ok() {
                let owner = match self.owner.lock() {
                    Ok(mut owner) => owner.take(),
                    Err(poisoned) => poisoned.into_inner().take(),
                };
                if let Some(owner) = owner
                    && owner.join().is_err()
                {
                    return Err(HixlError::new(
                        HixlPhase::Shutdown,
                        HixlErrorKind::Internal,
                        None,
                        "HIXL ACL owner panicked during shutdown",
                    ));
                }
            }
            result
        }
    }

    /// Retains the current engine and all admitted Host-pool generations after
    /// an external control-plane outcome becomes unknown. This never claims a
    /// native drain and therefore rejects all future work and shutdown cleanup.
    pub fn quarantine(&self) -> Result<(), HixlError> {
        #[cfg(not(feature = "hixl"))]
        {
            Err(HixlError::unsupported(
                HixlPhase::Quarantine,
                "afs-transport was built without the hixl feature",
            ))
        }
        #[cfg(feature = "hixl")]
        {
            let (reply, receiver) = mpsc::sync_channel(1);
            self.send(Command::Quarantine { reply }, HixlPhase::Quarantine)?;
            recv_reply(receiver, HixlPhase::Quarantine)
        }
    }

    #[cfg(feature = "hixl")]
    fn send(&self, command: Command, phase: HixlPhase) -> Result<(), HixlError> {
        self.sender.try_send(command).map_err(|error| {
            HixlError::new(
                phase,
                match error {
                    TrySendError::Full(_) => HixlErrorKind::ResourceExhausted,
                    TrySendError::Disconnected(_) => HixlErrorKind::InvalidState,
                },
                None,
                "HIXL ACL owner queue is full or closed",
            )
        })
    }
}

#[cfg(feature = "hixl")]
fn recv_reply<T>(
    receiver: Receiver<Result<T, HixlError>>,
    phase: HixlPhase,
) -> Result<T, HixlError> {
    match receiver.recv() {
        Ok(result) => result,
        Err(error) => {
            let mut failure = HixlError::new(
                phase,
                HixlErrorKind::EnginePoisoned,
                None,
                format!("HIXL ACL owner dropped the operation reply: {error}"),
            );
            failure.read_may_be_in_flight = phase == HixlPhase::PollRead;
            Err(failure)
        }
    }
}

#[cfg(feature = "hixl")]
fn update_status(target: &RwLock<RuntimeStatus>, status: RuntimeStatus) {
    match target.write() {
        Ok(mut target) => *target = status,
        Err(poisoned) => *poisoned.into_inner() = status,
    }
}

#[cfg(feature = "hixl")]
fn owner_loop<B: NativeBackend>(
    mut core: RuntimeCore<B>,
    receiver: Receiver<Command>,
    status: Arc<RwLock<RuntimeStatus>>,
) {
    while let Ok(command) = receiver.recv() {
        let should_stop = match command {
            Command::Connect {
                peer,
                deadline,
                reply,
            } => {
                let result = core.connect(&peer, deadline);
                update_status(&status, core.status());
                let _ = reply.send(result);
                false
            }
            Command::ReserveExport {
                length,
                deadline,
                reply,
            } => {
                let result = core.reserve_export_slot(length, deadline);
                update_status(&status, core.status());
                let _ = reply.send(result);
                false
            }
            Command::ReleaseExport {
                slot_id,
                generation,
                reply,
            } => {
                let result = core.release_export(slot_id, generation);
                update_status(&status, core.status());
                let _ = reply.send(result);
                false
            }
            Command::ReadBatch {
                peer,
                windows,
                output,
                queued_at,
                deadline,
                reply,
            } => {
                let owner_queue = Instant::now().saturating_duration_since(queued_at);
                let mut admitted = core.status();
                if admitted.state == RuntimeState::Running {
                    admitted.receive_slots_in_use = admitted
                        .receive_slots_in_use
                        .saturating_add(windows.len())
                        .min(core.options.receive_slots);
                    update_status(&status, admitted);
                }
                let result = core.read_batch(&peer, &windows, output, deadline, owner_queue);
                update_status(&status, core.status());
                let _ = reply.send(result);
                false
            }
            Command::ReleaseReceives { slots, reply } => {
                let result = core.release_receives(&slots);
                update_status(&status, core.status());
                let _ = reply.send(result);
                false
            }
            Command::Quarantine { reply } => {
                let result = core.quarantine();
                update_status(&status, core.status());
                let _ = reply.send(result);
                false
            }
            Command::Shutdown { reply } => {
                let result = core.shutdown();
                update_status(&status, core.status());
                let stop = result.is_ok();
                let _ = reply.send(result);
                stop
            }
        };
        if should_stop {
            break;
        }
    }
    // Last-producer drop also covers startup errors in the embedding process.
    // Core shutdown independently rejects any active or uncertain reference.
    if core.state != RuntimeState::Shutdown {
        let _ = core.shutdown();
        update_status(&status, core.status());
    }
}

#[cfg(feature = "hixl")]
mod ffi {
    use std::ffi::{c_char, c_void};

    #[repr(C)]
    #[derive(Clone, Copy, Debug, Default)]
    pub struct Window {
        pub slot_id: u64,
        pub generation: u64,
        pub address: u64,
        pub length: u64,
    }

    #[repr(C)]
    #[derive(Clone, Copy, Debug, Default)]
    pub struct BatchWindow {
        pub slot_id: u64,
        pub remote_address: u64,
        pub length: u64,
        pub local_offset: u64,
    }

    // SAFETY: these declarations mirror native/hixl.h. Rust keeps every
    // pointer live for the duration of the synchronous owner-thread call.
    unsafe extern "C" {
        pub fn afs_hixl_start(
            device_id: i32,
            engine_id: *const c_char,
            device_listen_port: u32,
            window_bytes: usize,
            receive_slots: usize,
            export_slots: usize,
            transport: u32,
            buffer_pool_count: usize,
            buffer_pool_size_mib: usize,
            out_hccs_domain_id: *mut u64,
            out: *mut *mut c_void,
            error: *mut c_char,
            error_len: usize,
        ) -> u32;
        pub fn afs_hixl_connect(
            runtime: *mut c_void,
            peer: *const c_char,
            timeout_ms: u32,
            error: *mut c_char,
            error_len: usize,
        ) -> u32;
        pub fn afs_hixl_reserve_export(
            runtime: *mut c_void,
            slot_id: u64,
            generation: u64,
            len: usize,
            out: *mut Window,
            error: *mut c_char,
            error_len: usize,
        ) -> u32;
        pub fn afs_hixl_release_export(
            runtime: *mut c_void,
            slot_id: u64,
            generation: u64,
            error: *mut c_char,
            error_len: usize,
        ) -> u32;
        pub fn afs_hixl_submit_read_batch(
            runtime: *mut c_void,
            peer: *const c_char,
            windows: *const BatchWindow,
            count: usize,
            out_request: *mut *mut c_void,
            dispatch_started: *mut u32,
            error: *mut c_char,
            error_len: usize,
        ) -> u32;
        pub fn afs_hixl_read_sync_batch(
            runtime: *mut c_void,
            peer: *const c_char,
            windows: *const BatchWindow,
            count: usize,
            slot_addresses: *mut u64,
            timeout_ms: u32,
            dispatch_started: *mut u32,
            sdk_transfer_ns: *mut u64,
            error: *mut c_char,
            error_len: usize,
        ) -> u32;
        pub fn afs_hixl_release_receives(
            runtime: *mut c_void,
            slot_ids: *const u64,
            count: usize,
            error: *mut c_char,
            error_len: usize,
        ) -> u32;
        pub fn afs_hixl_poll_read(
            runtime: *mut c_void,
            request: *mut c_void,
            slot_addresses: *mut u64,
            slot_count: usize,
            out_state: *mut u32,
            sdk_transfer_ns: *mut u64,
            error: *mut c_char,
            error_len: usize,
        ) -> u32;
        pub fn afs_hixl_shutdown(runtime: *mut c_void, error: *mut c_char, error_len: usize)
        -> u32;
        pub fn afs_hixl_destroy(runtime: *mut c_void);
    }
}

#[cfg(feature = "hixl")]
struct RealNative {
    handle: Option<std::ptr::NonNull<std::ffi::c_void>>,
    hccs_domain_id: Option<u64>,
}

/// A native runtime the C ABI retained after an incomplete startup cleanup.
///
/// The bridge marks such a runtime poisoned and refuses to shut it down, so no
/// code path may free, disconnect or reuse it. The record keeps the pointer
/// reachable for the process lifetime instead of dropping it silently; only
/// process exit releases the device resources it holds.
#[cfg(feature = "hixl")]
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
struct RetainedStart {
    /// Address of the retained native runtime. It is reported for diagnostics
    /// and never dereferenced.
    address: usize,
    /// Native status that failed the start.
    status: Option<u32>,
}

/// Failed-start runtimes of this process. They are never freed.
#[cfg(feature = "hixl")]
static RETAINED_STARTS: Mutex<Vec<RetainedStart>> = Mutex::new(Vec::new());

/// Number of native runtimes retained after an incomplete startup cleanup. A
/// nonzero value means the process must exit to release their device
/// resources.
#[cfg(all(feature = "hixl", test))]
fn retained_start_count() -> usize {
    match RETAINED_STARTS.lock() {
        Ok(retained) => retained.len(),
        Err(poisoned) => poisoned.into_inner().len(),
    }
}

/// Retained failed-start generations for diagnostics.
#[cfg(all(feature = "hixl", test))]
fn retained_starts() -> Vec<RetainedStart> {
    match RETAINED_STARTS.lock() {
        Ok(retained) => retained.to_vec(),
        Err(poisoned) => poisoned.into_inner().to_vec(),
    }
}

#[cfg(feature = "hixl")]
fn retain_failed_start(handle: *mut std::ffi::c_void, status: Option<u32>) -> usize {
    let mut retained = match RETAINED_STARTS.lock() {
        Ok(retained) => retained,
        Err(poisoned) => poisoned.into_inner(),
    };
    retained.push(RetainedStart {
        address: handle as usize,
        status,
    });
    retained.len()
}

/// Error for a native start that failed. A non-null handle means the bridge
/// kept its resources; the error reports that retention and names the record
/// so the residual generation can be traced.
#[cfg(feature = "hixl")]
fn failed_start_error(
    handle: *mut std::ffi::c_void,
    status: u32,
    message: String,
) -> NativeCallError {
    if handle.is_null() {
        return NativeCallError {
            status: Some(status),
            message,
            accepted: false,
        };
    }
    let retained = retain_failed_start(handle, Some(status));
    NativeCallError {
        status: Some(status),
        message: format!(
            "{message}; startup cleanup incomplete, native runtime {retained} retained with its \
             device resources until process exit"
        ),
        accepted: true,
    }
}

#[cfg(feature = "hixl")]
// SAFETY: RealNative is only moved into and accessed by the dedicated owner
// thread. Native HIXL and ACL handles are never shared across Rust threads.
unsafe impl Send for RealNative {}

#[cfg(feature = "hixl")]
impl NativeBackend for RealNative {
    fn start(options: &RuntimeOptions) -> Result<Self, NativeCallError> {
        let engine_id = std::ffi::CString::new(options.engine_id.as_str()).map_err(|error| {
            NativeCallError {
                status: None,
                message: format!("engine id contains an interior NUL: {error}"),
                accepted: false,
            }
        })?;
        let (buffer_pool_count, buffer_pool_size_mib) = options.buffer_pool_dims();
        let mut handle = std::ptr::null_mut();
        let mut hccs_domain_id = 0;
        let mut error = ErrorBuffer::default();
        // SAFETY: engine_id and error remain live for the synchronous native call;
        // options were validated before the owner thread was spawned.
        let status = unsafe {
            ffi::afs_hixl_start(
                options.device_id,
                engine_id.as_ptr(),
                options.device_listen_port.map(u32::from).unwrap_or(0),
                options.window_bytes,
                options.receive_slots,
                options.export_slots,
                match options.transport {
                    ReadTransport::Roce => 1,
                    ReadTransport::HccsBufferPool => 2,
                },
                buffer_pool_count,
                buffer_pool_size_mib,
                &mut hccs_domain_id,
                &mut handle,
                error.ptr(),
                ERR_BYTES,
            )
        };
        if status != HIXL_SUCCESS || handle.is_null() {
            // The native bridge intentionally retains a non-null failed-start
            // handle when cleanup is incomplete. It is recorded below and
            // never destroyed or reused, and preferred mode must not treat it
            // as a safe capability refusal.
            return Err(failed_start_error(handle, status, error.message()));
        }
        let handle = std::ptr::NonNull::new(handle).ok_or_else(|| NativeCallError {
            status: None,
            message: "native HIXL returned a null runtime handle".to_owned(),
            accepted: false,
        })?;
        Ok(Self {
            handle: Some(handle),
            hccs_domain_id: (options.transport == ReadTransport::HccsBufferPool)
                .then_some(hccs_domain_id),
        })
    }

    fn hccs_domain_id(&self) -> Option<u64> {
        self.hccs_domain_id
    }

    fn connect(&mut self, peer: &str, timeout_ms: u32) -> Result<(), NativeCallError> {
        let peer = std::ffi::CString::new(peer).map_err(|error| NativeCallError {
            status: None,
            message: format!("peer engine id contains an interior NUL: {error}"),
            accepted: false,
        })?;
        let handle = self.handle.ok_or_else(native_not_initialized)?;
        let mut error = ErrorBuffer::default();
        // SAFETY: the handle, NUL-terminated peer, and writable error buffer are live;
        // the owner thread has exclusive access to the native engine.
        let status = unsafe {
            ffi::afs_hixl_connect(
                handle.as_ptr(),
                peer.as_ptr(),
                timeout_ms,
                error.ptr(),
                ERR_BYTES,
            )
        };
        if status == HIXL_SUCCESS {
            Ok(())
        } else {
            Err(NativeCallError {
                status: Some(status),
                message: error.message(),
                accepted: false,
            })
        }
    }

    fn reserve_export(
        &mut self,
        slot_id: u64,
        generation: u64,
        length: usize,
    ) -> Result<u64, NativeCallError> {
        let handle = self.handle.ok_or_else(native_not_initialized)?;
        let mut output = ffi::Window::default();
        let mut error = ErrorBuffer::default();
        // SAFETY: output and error buffers remain live for the synchronous
        // call, and native retains neither pointer.
        let status = unsafe {
            ffi::afs_hixl_reserve_export(
                handle.as_ptr(),
                slot_id,
                generation,
                length,
                &mut output,
                error.ptr(),
                ERR_BYTES,
            )
        };
        if status != HIXL_SUCCESS {
            return Err(NativeCallError {
                status: Some(status),
                message: error.message(),
                accepted: false,
            });
        }
        if output.address == 0 || u64::try_from(length).ok() != Some(output.length) {
            return Err(NativeCallError {
                status: None,
                message: "native HIXL returned an invalid export window".to_owned(),
                accepted: false,
            });
        }
        Ok(output.address)
    }

    fn release_export(&mut self, slot_id: u64, generation: u64) -> Result<(), NativeCallError> {
        let handle = self.handle.ok_or_else(native_not_initialized)?;
        let mut error = ErrorBuffer::default();
        // SAFETY: the owner thread exclusively owns the valid native handle and
        // writable error buffer for this synchronous release call.
        let status = unsafe {
            ffi::afs_hixl_release_export(
                handle.as_ptr(),
                slot_id,
                generation,
                error.ptr(),
                ERR_BYTES,
            )
        };
        if status == HIXL_SUCCESS {
            Ok(())
        } else {
            Err(NativeCallError {
                status: Some(status),
                message: error.message(),
                accepted: false,
            })
        }
    }

    fn submit_read_batch(
        &mut self,
        windows: &[NativeReadWindow],
        peer: &str,
    ) -> Result<NativeRequest, NativeCallError> {
        let peer = std::ffi::CString::new(peer).map_err(|error| NativeCallError {
            status: None,
            message: format!("peer engine id contains an interior NUL: {error}"),
            accepted: false,
        })?;
        let descriptors = batch_descriptors(windows)?;
        let handle = self.handle.ok_or_else(native_not_initialized)?;
        let mut request = std::ptr::null_mut();
        let mut dispatch_started = 0_u32;
        let mut error = ErrorBuffer::default();
        // SAFETY: all pointers remain live for the synchronous submit; native
        // reserves every listed slot before building one SDK batch.
        let status = unsafe {
            ffi::afs_hixl_submit_read_batch(
                handle.as_ptr(),
                peer.as_ptr(),
                descriptors.as_ptr(),
                descriptors.len(),
                &mut request,
                &mut dispatch_started,
                error.ptr(),
                ERR_BYTES,
            )
        };
        if status != HIXL_SUCCESS || request.is_null() {
            return Err(NativeCallError {
                status: Some(status),
                message: error.message(),
                accepted: dispatch_started != 0,
            });
        }
        let request_id = u64::try_from(request as usize).map_err(|_| NativeCallError {
            status: None,
            message: "native HIXL request handle does not fit the ABI".to_owned(),
            accepted: true,
        })?;
        Ok(NativeRequest(request_id))
    }

    fn read_sync_batch(
        &mut self,
        windows: &[NativeReadWindow],
        peer: &str,
        addresses: &mut [u64],
        timeout_ms: u32,
    ) -> Result<NativeReadTiming, NativeCallError> {
        let peer = std::ffi::CString::new(peer).map_err(|error| NativeCallError {
            status: None,
            message: format!("peer engine id contains an interior NUL: {error}"),
            accepted: false,
        })?;
        let descriptors = batch_descriptors(windows)?;
        let handle = self.handle.ok_or_else(native_not_initialized)?;
        let mut error = ErrorBuffer::default();
        let mut dispatch_started = 0_u32;
        let mut sdk_transfer_ns = 0_u64;
        if addresses.len() != descriptors.len() {
            return Err(NativeCallError {
                status: None,
                message: "native HIXL batch READ needs one address per window".to_owned(),
                accepted: false,
            });
        }
        // SAFETY: the owner thread owns the runtime for the entire blocking
        // call, and `addresses` holds one writable entry per descriptor.
        let status = unsafe {
            ffi::afs_hixl_read_sync_batch(
                handle.as_ptr(),
                peer.as_ptr(),
                descriptors.as_ptr(),
                descriptors.len(),
                addresses.as_mut_ptr(),
                timeout_ms,
                &mut dispatch_started,
                &mut sdk_transfer_ns,
                error.ptr(),
                ERR_BYTES,
            )
        };
        if status == HIXL_SUCCESS {
            Ok(NativeReadTiming {
                sdk_transfer: Some(Duration::from_nanos(sdk_transfer_ns)),
            })
        } else {
            Err(NativeCallError {
                status: Some(status),
                message: error.message(),
                accepted: dispatch_started != 0,
            })
        }
    }

    fn release_receives(&mut self, slot_ids: &[u64]) -> Result<(), NativeCallError> {
        let handle = self.handle.ok_or_else(native_not_initialized)?;
        let mut error = ErrorBuffer::default();
        // SAFETY: `slot_ids` and the error buffer remain live for the
        // synchronous call, and native retains neither pointer.
        let status = unsafe {
            ffi::afs_hixl_release_receives(
                handle.as_ptr(),
                slot_ids.as_ptr(),
                slot_ids.len(),
                error.ptr(),
                ERR_BYTES,
            )
        };
        if status == HIXL_SUCCESS {
            Ok(())
        } else {
            Err(NativeCallError {
                status: Some(status),
                message: error.message(),
                accepted: true,
            })
        }
    }

    fn poll_read(
        &mut self,
        request: NativeRequest,
        addresses: &mut [u64],
    ) -> Result<NativePoll, NativeCallError> {
        let handle = self.handle.ok_or_else(native_not_initialized)?;
        let request_ptr = usize::try_from(request.0).map_err(|_| NativeCallError {
            status: None,
            message: "native HIXL request handle does not fit this platform".to_owned(),
            accepted: true,
        })?;
        let mut state = 0_u32;
        let mut sdk_transfer_ns = 0_u64;
        let mut error = ErrorBuffer::default();
        // SAFETY: request is an opaque handle returned by the same native engine,
        // and `addresses` holds one writable entry per submitted window.
        let status = unsafe {
            ffi::afs_hixl_poll_read(
                handle.as_ptr(),
                request_ptr as *mut std::ffi::c_void,
                addresses.as_mut_ptr(),
                addresses.len(),
                &mut state,
                &mut sdk_transfer_ns,
                error.ptr(),
                ERR_BYTES,
            )
        };
        if status != HIXL_SUCCESS {
            return Err(NativeCallError {
                status: Some(status),
                message: error.message(),
                accepted: true,
            });
        }
        match state {
            0 => Ok(NativePoll::Waiting),
            1 => Ok(NativePoll::Completed {
                timing: NativeReadTiming {
                    sdk_transfer: Some(Duration::from_nanos(sdk_transfer_ns)),
                },
            }),
            3 => Err(NativeCallError {
                status: Some(HIXL_TIMEOUT),
                message: "native HIXL READ timed out".to_owned(),
                accepted: true,
            }),
            2 => Err(NativeCallError {
                status: Some(503_900),
                message: "native HIXL READ failed".to_owned(),
                accepted: true,
            }),
            _ => Err(NativeCallError {
                status: None,
                message: "native HIXL READ returned an unknown completion state".to_owned(),
                accepted: true,
            }),
        }
    }

    fn shutdown(&mut self) -> Result<(), NativeCallError> {
        let Some(handle) = self.handle.take() else {
            return Ok(());
        };
        let mut error = ErrorBuffer::default();
        // SAFETY: the owner thread has exclusive access and shutdown consumes the
        // native engine only after the Rust state machine proves no slot is active.
        let status = unsafe { ffi::afs_hixl_shutdown(handle.as_ptr(), error.ptr(), ERR_BYTES) };
        if status != HIXL_SUCCESS {
            self.handle = Some(handle);
            return Err(NativeCallError {
                status: Some(status),
                message: error.message(),
                accepted: true,
            });
        }
        // SAFETY: native shutdown completed successfully and no caller can issue
        // another command after this method returns.
        unsafe { ffi::afs_hixl_destroy(handle.as_ptr()) };
        Ok(())
    }
}

/// Copies receive slots out of pinned Host memory on any thread but the ACL
/// owner, separate from the reader validation pool.
#[cfg(feature = "hixl")]
fn copy_pool() -> Option<&'static rayon_core::ThreadPool> {
    static POOL: std::sync::OnceLock<Option<rayon_core::ThreadPool>> = std::sync::OnceLock::new();
    POOL.get_or_init(|| {
        let workers = std::thread::available_parallelism()
            .map_or(4, |parallel| parallel.get())
            .clamp(1, 8);
        rayon_core::ThreadPoolBuilder::new()
            .num_threads(workers)
            .thread_name(|index| format!("afs-hixl-copy-{index}"))
            .build()
            .ok()
    })
    .as_ref()
}

#[cfg(feature = "hixl")]
fn copy_slot(address: usize, out: &mut [std::mem::MaybeUninit<u8>]) {
    // SAFETY: the completed receive slot contains at least out.len() bytes
    // and stays pinned until every copy returns. The separate output vector
    // provides a disjoint destination for each slot.
    unsafe {
        std::ptr::copy_nonoverlapping(
            std::ptr::with_exposed_provenance::<u8>(address),
            out.as_mut_ptr().cast::<u8>(),
            out.len(),
        );
    }
}

/// Fills the reserved output and publishes its length only after every slot
/// has been copied. The slots must tile the requested length in order.
#[cfg(feature = "hixl")]
fn copy_pinned(slots: &[PinnedSlot], output: &mut Vec<u8>, length: usize) -> Result<(), HixlError> {
    let untiled = || {
        HixlError::new(
            HixlPhase::PollRead,
            HixlErrorKind::Internal,
            None,
            "pinned receive slots do not tile the output",
        )
    };
    let mut copies = Vec::with_capacity(slots.len());
    let mut remaining = output
        .spare_capacity_mut()
        .get_mut(..length)
        .ok_or_else(untiled)?;
    for slot in slots {
        let (head, tail) = std::mem::take(&mut remaining)
            .split_at_mut_checked(slot.length)
            .ok_or_else(untiled)?;
        copies.push((slot.address, head));
        remaining = tail;
    }
    if !remaining.is_empty() {
        return Err(untiled());
    }
    match copy_pool() {
        Some(pool) if copies.len() > 1 => pool.install(|| {
            rayon_core::scope(|scope| {
                for (address, out) in copies {
                    scope.spawn(move |_| copy_slot(address, out));
                }
            });
        }),
        _ => {
            for (address, out) in copies {
                copy_slot(address, out);
            }
        }
    }
    // SAFETY: the spare-capacity split above assigned every byte of the
    // requested range to exactly one slot, and all copy tasks have joined.
    unsafe { output.set_len(length) };
    Ok(())
}

#[cfg(feature = "hixl")]
fn native_not_initialized() -> NativeCallError {
    NativeCallError {
        status: None,
        message: "native HIXL runtime is not initialized".to_owned(),
        accepted: false,
    }
}

#[cfg(feature = "hixl")]
fn batch_descriptors(
    windows: &[NativeReadWindow],
) -> Result<Vec<ffi::BatchWindow>, NativeCallError> {
    if windows.is_empty() || windows.len() > MAX_BATCH_WINDOWS {
        return Err(NativeCallError {
            status: None,
            message: format!("native HIXL batch READ window count must be 1..={MAX_BATCH_WINDOWS}"),
            accepted: false,
        });
    }
    windows
        .iter()
        .map(|window| {
            Ok(ffi::BatchWindow {
                slot_id: window.slot_id,
                remote_address: window.remote_addr,
                length: u64::try_from(window.len).map_err(|_| NativeCallError {
                    status: None,
                    message: "native HIXL batch READ length does not fit the ABI".to_owned(),
                    accepted: false,
                })?,
                local_offset: u64::try_from(window.local_offset).map_err(|_| NativeCallError {
                    status: None,
                    message: "native HIXL batch READ local offset does not fit the ABI".to_owned(),
                    accepted: false,
                })?,
            })
        })
        .collect()
}

#[cfg(feature = "hixl")]
#[derive(Clone)]
struct ErrorBuffer([u8; ERR_BYTES]);

#[cfg(feature = "hixl")]
impl Default for ErrorBuffer {
    fn default() -> Self {
        Self([0; ERR_BYTES])
    }
}

#[cfg(feature = "hixl")]
impl ErrorBuffer {
    fn ptr(&mut self) -> *mut std::ffi::c_char {
        self.0.as_mut_ptr().cast()
    }

    fn message(&self) -> String {
        let bytes = self
            .0
            .iter()
            .take_while(|byte| **byte != 0)
            .copied()
            .collect::<Vec<_>>();
        String::from_utf8_lossy(&bytes).into_owned()
    }
}

#[cfg(feature = "hixl")]
impl Drop for RealNative {
    fn drop(&mut self) {
        // A dropped owner is not evidence that HIXL has drained. Intentionally
        // leak an uncertain native generation instead of Disconnect/Deregister/
        // FreeHost reuse after a timeout or failed completion.
        let _ = self.handle.take();
    }
}

#[cfg(test)]
mod tests;
