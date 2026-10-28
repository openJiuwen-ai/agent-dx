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
const MAX_POOL_BYTES: usize = 512 * 1024 * 1024;
#[cfg(feature = "hixl")]
const ERR_BYTES: usize = 512;
/// Mirrors `AFS_HIXL_MAX_BATCH_WINDOWS` in native/hixl.h.
const MAX_BATCH_WINDOWS: usize = 64;

/// Default fixed Host-window size.
pub const DEFAULT_WINDOW_BYTES: usize = 4 * 1024 * 1024;
/// Default number of local receive slots.
pub const DEFAULT_RECEIVE_SLOTS: usize = 16;
/// Default number of source export slots.
pub const DEFAULT_EXPORT_SLOTS: usize = 16;
/// Default per-peer admission limit.
pub const DEFAULT_MAX_INFLIGHT_PER_PEER: usize = 1;

mod group;
pub use group::{HixlRuntimeGroup, validate_group_options};

/// Immutable DFS read data path. HCCS relay requires HCCS-reachable A3 devices.
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
    pub max_inflight_per_peer: usize,
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
            max_inflight_per_peer: DEFAULT_MAX_INFLIGHT_PER_PEER,
            transport: ReadTransport::Roce,
            buffer_pool_count: 4,
            buffer_pool_size_mib: 8,
        }
    }
}

impl RuntimeOptions {
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
    /// Native copy from the ACL Host window into the returned buffer.
    pub output_copy: Option<Duration>,
}

/// Runtime lifecycle state exposed to health and diagnostics.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum RuntimeState {
    Stopped,
    Running,
    Degraded,
    Quarantined,
    Shutdown,
}

impl fmt::Display for RuntimeState {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        let name = match self {
            Self::Stopped => "stopped",
            Self::Running => "running",
            Self::Degraded => "degraded",
            Self::Quarantined => "quarantined",
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
    RegisterPool,
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
            Self::RegisterPool => "register_pool",
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
    output_copy: Option<Duration>,
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
    fn prepare_export(
        &mut self,
        slot_id: u64,
        generation: u64,
        data: &[u8],
    ) -> Result<u64, NativeCallError>;
    fn release_export(&mut self, slot_id: u64, generation: u64) -> Result<(), NativeCallError>;
    fn submit_read(
        &mut self,
        slot_id: u64,
        peer: &str,
        remote_addr: u64,
        len: usize,
    ) -> Result<NativeRequest, NativeCallError>;
    fn read_sync(
        &mut self,
        slot_id: u64,
        peer: &str,
        remote_addr: u64,
        output: &mut [u8],
        timeout_ms: u32,
    ) -> Result<NativeReadTiming, NativeCallError>;
    fn poll_read(
        &mut self,
        request: NativeRequest,
        output: &mut [u8],
    ) -> Result<NativePoll, NativeCallError>;
    /// Batch submission defaults to Unsupported so no fake backend can silently
    /// emulate a multi-descriptor call by looping the single-window API.
    fn submit_read_batch(
        &mut self,
        _windows: &[NativeReadWindow],
        _peer: &str,
    ) -> Result<NativeRequest, NativeCallError> {
        Err(NativeCallError {
            status: Some(HIXL_UNSUPPORTED),
            message: "native backend does not implement batch READ submission".to_owned(),
            accepted: false,
        })
    }
    fn read_sync_batch(
        &mut self,
        _windows: &[NativeReadWindow],
        _peer: &str,
        _output: &mut [u8],
        _timeout_ms: u32,
    ) -> Result<NativeReadTiming, NativeCallError> {
        Err(NativeCallError {
            status: Some(HIXL_UNSUPPORTED),
            message: "native backend does not implement batch READ".to_owned(),
            accepted: false,
        })
    }
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
        Ok(Self {
            backend,
            info: EngineInfo {
                engine_id: options.engine_id.clone(),
                generation,
                window_bytes: options.window_bytes,
                transport: options.transport,
                hccs_domain_id,
                buffer_pool_count: if options.transport == ReadTransport::HccsBufferPool {
                    options.buffer_pool_count
                } else {
                    0
                },
                buffer_pool_size_mib: if options.transport == ReadTransport::HccsBufferPool {
                    options.buffer_pool_size_mib
                } else {
                    0
                },
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

    #[cfg(test)]
    fn prepare_export(
        &mut self,
        data: Vec<u8>,
        deadline: Instant,
    ) -> Result<ExportWindow, HixlError> {
        self.prepare_export_slice(&data, deadline)
    }

    fn prepare_export_slice(
        &mut self,
        data: &[u8],
        deadline: Instant,
    ) -> Result<ExportWindow, HixlError> {
        self.ensure_running(HixlPhase::PrepareExport)?;
        if data.is_empty() || data.len() > self.options.window_bytes {
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
        let address = match self
            .backend
            .prepare_export(slot_id as u64, generation, data)
        {
            Ok(address) if address != 0 => address,
            Ok(_) => {
                self.quarantine_export(slot_id);
                return Err(HixlError::new(
                    HixlPhase::PrepareExport,
                    HixlErrorKind::EnginePoisoned,
                    None,
                    "native export returned an empty address",
                ));
            }
            Err(error) => {
                if error.accepted {
                    self.exports[slot_id].state = SlotState::Quarantined;
                    self.quarantined_bytes = self
                        .quarantined_bytes
                        .saturating_add(self.options.window_bytes);
                    self.state = RuntimeState::Quarantined;
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
            length: data.len(),
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
            self.exports[slot_index].state = SlotState::Quarantined;
            self.quarantined_bytes = self
                .quarantined_bytes
                .saturating_add(self.options.window_bytes);
            self.state = RuntimeState::Quarantined;
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

    fn read(
        &mut self,
        peer: &str,
        remote_addr: u64,
        mut output: Vec<u8>,
        deadline: Instant,
        owner_queue: Duration,
    ) -> Result<HixlReadOutcome, HixlError> {
        let len = output.len();
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
        if remote_addr == 0 || len == 0 || len > self.options.window_bytes {
            return Err(HixlError::new(
                HixlPhase::SubmitRead,
                HixlErrorKind::InvalidArgument,
                None,
                "remote address and length are outside the exported window",
            ));
        }
        let timeout_ms = deadline_timeout_ms(deadline, HixlPhase::SubmitRead)?;
        let slot_id = self
            .receives
            .iter()
            .enumerate()
            .find(|(_, slot)| slot.state == SlotState::Free)
            .map(|(slot_id, _)| slot_id)
            .ok_or_else(|| {
                HixlError::new(
                    HixlPhase::SubmitRead,
                    HixlErrorKind::ResourceExhausted,
                    Some(HIXL_RESOURCE_EXHAUSTED),
                    "all receive slots are in use or quarantined",
                )
            })?;
        self.receives[slot_id].state = SlotState::Active;
        if self.options.transport == ReadTransport::HccsBufferPool {
            // The queued command owns this output through the blocking call, so
            // caller cancellation cannot free storage still used by the owner.
            let timing = match self.backend.read_sync(
                slot_id as u64,
                peer,
                remote_addr,
                &mut output,
                timeout_ms,
            ) {
                Ok(timing) => timing,
                Err(error) => {
                    if error.accepted {
                        self.quarantine_receive(slot_id);
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
                    self.receives[slot_id].state = SlotState::Free;
                    return Err(map_native_error(
                        HixlPhase::SubmitRead,
                        error,
                        HixlErrorKind::NativeFailure,
                    ));
                }
            };
            self.receives[slot_id].state = SlotState::Free;
            return Ok(HixlReadOutcome {
                bytes: output,
                timing: HixlReadTiming {
                    owner_queue,
                    sdk_transfer: timing.sdk_transfer,
                    output_copy: timing.output_copy,
                },
            });
        }
        let request = match self
            .backend
            .submit_read(slot_id as u64, peer, remote_addr, len)
        {
            Ok(request) => request,
            Err(error) => {
                if error.accepted {
                    self.quarantine_receive(slot_id);
                    return Err(HixlError::new(
                        HixlPhase::SubmitRead,
                        HixlErrorKind::EnginePoisoned,
                        error.status,
                        error.message,
                    )
                    .with_read_in_flight());
                }
                self.receives[slot_id].state = SlotState::Free;
                return Err(map_native_error(
                    HixlPhase::SubmitRead,
                    error,
                    HixlErrorKind::NativeFailure,
                ));
            }
        };
        self.receives[slot_id].request = Some(request);
        loop {
            if Instant::now() >= deadline {
                self.quarantine_receive(slot_id);
                return Err(HixlError::new(
                    HixlPhase::PollRead,
                    HixlErrorKind::DeadlineExceeded,
                    Some(HIXL_TIMEOUT),
                    "accepted READ did not complete before the deadline",
                )
                .with_read_in_flight());
            }
            let poll = match self.backend.poll_read(request, &mut output) {
                Ok(poll) => poll,
                Err(error) => {
                    self.quarantine_receive(slot_id);
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
                    let slot = &mut self.receives[slot_id];
                    slot.state = SlotState::Free;
                    slot.request = None;
                    return Ok(HixlReadOutcome {
                        bytes: output,
                        timing: HixlReadTiming {
                            owner_queue,
                            sdk_transfer: timing.sdk_transfer,
                            output_copy: timing.output_copy,
                        },
                    });
                }
            }
        }
    }

    fn free_receives(&mut self, slots: &[usize]) {
        for &slot_id in slots {
            self.receives[slot_id].state = SlotState::Free;
            self.receives[slot_id].request = None;
        }
    }

    fn quarantine_receives(&mut self, slots: &[usize]) {
        for &slot_id in slots {
            self.quarantine_receive(slot_id);
        }
    }

    /// One native READ call carrying every window in `windows`.  Each tuple is
    /// `(remote_addr, length)`; the caller output is scatted in the same order.
    fn read_batch(
        &mut self,
        peer: &str,
        windows: &[(u64, usize)],
        mut output: Vec<u8>,
        deadline: Instant,
        owner_queue: Duration,
    ) -> Result<HixlReadOutcome, HixlError> {
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
        if total == 0 || output.len() != total {
            return Err(HixlError::new(
                HixlPhase::SubmitRead,
                HixlErrorKind::InvalidArgument,
                None,
                "batch READ output length differs from the submitted windows",
            ));
        }
        let timeout_ms = deadline_timeout_ms(deadline, HixlPhase::SubmitRead)?;
        let mut slots = Vec::with_capacity(windows.len());
        for _ in windows {
            let Some(slot_id) = self
                .receives
                .iter()
                .enumerate()
                .find(|(index, slot)| {
                    slot.state == SlotState::Free && !slots.contains(&(*index as u64))
                })
                .map(|(index, _)| index as u64)
            else {
                let allocated = slots
                    .iter()
                    .map(|slot_id| *slot_id as usize)
                    .collect::<Vec<_>>();
                self.free_receives(&allocated);
                return Err(HixlError::new(
                    HixlPhase::SubmitRead,
                    HixlErrorKind::ResourceExhausted,
                    Some(HIXL_RESOURCE_EXHAUSTED),
                    "all receive slots are in use or quarantined",
                ));
            };
            self.receives[slot_id as usize].state = SlotState::Active;
            slots.push(slot_id);
        }
        let slot_indices = slots.iter().map(|slot| *slot as usize).collect::<Vec<_>>();
        let mut local_offset = 0_usize;
        let native_windows = windows
            .iter()
            .zip(&slots)
            .map(|((remote_addr, len), slot_id)| {
                let window = NativeReadWindow {
                    slot_id: *slot_id,
                    remote_addr: *remote_addr,
                    len: *len,
                    local_offset,
                };
                local_offset += *len;
                window
            })
            .collect::<Vec<_>>();
        if self.options.transport == ReadTransport::HccsBufferPool {
            let timing =
                match self
                    .backend
                    .read_sync_batch(&native_windows, peer, &mut output, timeout_ms)
                {
                    Ok(timing) => timing,
                    Err(error) => {
                        if error.accepted {
                            self.quarantine_receives(&slot_indices);
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
                        self.free_receives(&slot_indices);
                        return Err(map_native_error(
                            HixlPhase::SubmitRead,
                            error,
                            HixlErrorKind::NativeFailure,
                        ));
                    }
                };
            self.free_receives(&slot_indices);
            return Ok(HixlReadOutcome {
                bytes: output,
                timing: HixlReadTiming {
                    owner_queue,
                    sdk_transfer: timing.sdk_transfer,
                    output_copy: timing.output_copy,
                },
            });
        }
        let request = match self.backend.submit_read_batch(&native_windows, peer) {
            Ok(request) => request,
            Err(error) => {
                if error.accepted {
                    self.quarantine_receives(&slot_indices);
                    return Err(HixlError::new(
                        HixlPhase::SubmitRead,
                        HixlErrorKind::EnginePoisoned,
                        error.status,
                        error.message,
                    )
                    .with_read_in_flight());
                }
                self.free_receives(&slot_indices);
                return Err(map_native_error(
                    HixlPhase::SubmitRead,
                    error,
                    HixlErrorKind::NativeFailure,
                ));
            }
        };
        for &slot_id in &slot_indices {
            self.receives[slot_id].request = Some(request);
        }
        loop {
            if Instant::now() >= deadline {
                self.quarantine_receives(&slot_indices);
                return Err(HixlError::new(
                    HixlPhase::PollRead,
                    HixlErrorKind::DeadlineExceeded,
                    Some(HIXL_TIMEOUT),
                    "accepted batch READ did not complete before the deadline",
                )
                .with_read_in_flight());
            }
            let poll = match self.backend.poll_read(request, &mut output) {
                Ok(poll) => poll,
                Err(error) => {
                    self.quarantine_receives(&slot_indices);
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
                    self.free_receives(&slot_indices);
                    return Ok(HixlReadOutcome {
                        bytes: output,
                        timing: HixlReadTiming {
                            owner_queue,
                            sdk_transfer: timing.sdk_transfer,
                            output_copy: timing.output_copy,
                        },
                    });
                }
            }
        }
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
            self.state = RuntimeState::Quarantined;
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
        for slot_id in 0..self.exports.len() {
            if self.exports[slot_id].state == SlotState::Active {
                self.quarantine_export(slot_id);
            }
        }
        for slot_id in 0..self.receives.len() {
            if self.receives[slot_id].state == SlotState::Active {
                self.quarantine_receive(slot_id);
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
            RuntimeState::Shutdown | RuntimeState::Stopped | RuntimeState::Degraded => {
                Err(HixlError::new(
                    phase,
                    HixlErrorKind::InvalidState,
                    None,
                    format!("runtime is {}", self.state),
                ))
            }
        }
    }

    fn quarantine_receive(&mut self, slot_id: usize) {
        let slot = &mut self.receives[slot_id];
        if slot.state != SlotState::Quarantined {
            slot.state = SlotState::Quarantined;
            self.quarantined_bytes = self
                .quarantined_bytes
                .saturating_add(self.options.window_bytes);
        }
        self.state = RuntimeState::Quarantined;
    }

    fn quarantine_export(&mut self, slot_id: usize) {
        let slot = &mut self.exports[slot_id];
        if slot.state != SlotState::Quarantined {
            slot.state = SlotState::Quarantined;
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
        || options.receive_slots > 64
        || options.export_slots > 64
        || options.max_inflight_per_peer == 0
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
    if bytes > MAX_POOL_BYTES {
        return Err(HixlError::new(
            HixlPhase::Initialize,
            HixlErrorKind::ResourceExhausted,
            Some(HIXL_RESOURCE_EXHAUSTED),
            format!("HIXL Host pool exceeds {} bytes", MAX_POOL_BYTES),
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
    PrepareExport {
        data: Arc<Vec<u8>>,
        offset: usize,
        length: usize,
        deadline: Instant,
        reply: SyncSender<Result<ExportWindow, HixlError>>,
    },
    ReleaseExport {
        slot_id: u64,
        generation: u64,
        reply: SyncSender<Result<(), HixlError>>,
    },
    Read {
        peer: String,
        remote_addr: u64,
        output: Vec<u8>,
        queued_at: Instant,
        deadline: Instant,
        reply: SyncSender<Result<HixlReadOutcome, HixlError>>,
    },
    ReadBatch {
        peer: String,
        windows: Vec<(u64, usize)>,
        output: Vec<u8>,
        queued_at: Instant,
        deadline: Instant,
        reply: SyncSender<Result<HixlReadOutcome, HixlError>>,
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

    pub fn prepare_export(
        &self,
        data: Vec<u8>,
        deadline: Instant,
    ) -> Result<ExportWindow, HixlError> {
        let length = data.len();
        self.prepare_export_shared(Arc::new(data), 0, length, deadline)
    }

    /// Exports one window out of a shared snapshot without copying the window
    /// into a temporary `Vec`; only the native slot copy remains.
    pub fn prepare_export_shared(
        &self,
        data: Arc<Vec<u8>>,
        offset: usize,
        length: usize,
        deadline: Instant,
    ) -> Result<ExportWindow, HixlError> {
        #[cfg(not(feature = "hixl"))]
        {
            let _ = (data, offset, length, deadline);
            Err(HixlError::unsupported(
                HixlPhase::PrepareExport,
                "afs-transport was built without the hixl feature",
            ))
        }
        #[cfg(feature = "hixl")]
        {
            if length == 0
                || offset
                    .checked_add(length)
                    .is_none_or(|end| end > data.len())
            {
                return Err(HixlError::new(
                    HixlPhase::PrepareExport,
                    HixlErrorKind::InvalidArgument,
                    None,
                    "export range exceeds the shared snapshot",
                ));
            }
            let (reply, receiver) = mpsc::sync_channel(1);
            self.send(
                Command::PrepareExport {
                    data,
                    offset,
                    length,
                    deadline,
                    reply,
                },
                HixlPhase::PrepareExport,
            )?;
            recv_reply(receiver, HixlPhase::PrepareExport)
        }
    }

    pub fn release_export(&self, slot_id: u64, generation: u64) -> Result<(), HixlError> {
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

    /// Reserves the private result buffer for one native multi-window batch.
    /// The length covers every descriptor in the batch, so the bound is the
    /// window size multiplied by the native descriptor limit. Only the address
    /// space is prepared: the bytes stay untouched until the native READ fills
    /// them, so a group wave does not memset 128 MiB the transfer overwrites.
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
        // SAFETY: the private buffer only ever reaches the native READ below and
        // the caller's scatter, and that READ writes all `len` bytes before it
        // reports success; a failed or short READ drops the buffer without
        // publishing any byte. Initialising the reservation first would memset
        // the whole batch (a group wave reaches 128 MiB) only for the transfer
        // to overwrite it.
        unsafe { output.set_len(len) };
        Ok(output)
    }

    /// Reserves a private result buffer before a caller starts measuring the
    /// payload read interval. Like the batch form, the reservation is not
    /// written until the native READ fills it, so the preparation measured by
    /// `buffers` covers address space only and the first touch moves into the
    /// transfer it belongs to.
    pub fn prepare_read_buffer(&self, len: usize) -> Result<Vec<u8>, HixlError> {
        if len == 0 || len > self.info.window_bytes {
            return Err(HixlError::new(
                HixlPhase::SubmitRead,
                HixlErrorKind::InvalidArgument,
                None,
                format!(
                    "read buffer length must be in 1..={} bytes",
                    self.info.window_bytes
                ),
            ));
        }
        let mut output = Vec::new();
        output.try_reserve_exact(len).map_err(|error| {
            HixlError::new(
                HixlPhase::SubmitRead,
                HixlErrorKind::ResourceExhausted,
                Some(HIXL_RESOURCE_EXHAUSTED),
                format!("cannot reserve HIXL read output buffer: {error}"),
            )
        })?;
        // SAFETY: as in `prepare_read_batch_buffer`, the native READ fills every
        // byte of this private buffer before it reports success, so the content
        // becomes initialised before any caller reads it; skipping the memset
        // only removes work the transfer would overwrite.
        unsafe { output.set_len(len) };
        Ok(output)
    }

    /// Reads bytes and reports durations measured by the local ACL owner.
    pub fn read_timed(
        &self,
        peer: &str,
        remote_addr: u64,
        len: usize,
        deadline: Instant,
    ) -> Result<HixlReadOutcome, HixlError> {
        #[cfg(not(feature = "hixl"))]
        {
            let _ = (peer, remote_addr, len, deadline);
            Err(HixlError::unsupported(
                HixlPhase::SubmitRead,
                "afs-transport was built without the hixl feature",
            ))
        }
        #[cfg(feature = "hixl")]
        {
            let output = self.prepare_read_buffer(len)?;
            self.read_timed_into(peer, remote_addr, output, deadline)
        }
    }

    /// Reads into output storage prepared before the caller's payload timer.
    pub fn read_timed_into(
        &self,
        peer: &str,
        remote_addr: u64,
        output: Vec<u8>,
        deadline: Instant,
    ) -> Result<HixlReadOutcome, HixlError> {
        #[cfg(not(feature = "hixl"))]
        {
            let _ = (peer, remote_addr, output, deadline);
            Err(HixlError::unsupported(
                HixlPhase::SubmitRead,
                "afs-transport was built without the hixl feature",
            ))
        }
        #[cfg(feature = "hixl")]
        {
            let (reply, receiver) = mpsc::sync_channel(1);
            // `queued_at` is captured immediately before the bounded send. The
            // owner measures through the instant it dequeues this command.
            let queued_at = Instant::now();
            self.send(
                Command::Read {
                    peer: peer.to_owned(),
                    remote_addr,
                    output,
                    queued_at,
                    deadline,
                    reply,
                },
                HixlPhase::SubmitRead,
            )?;
            recv_reply(receiver, HixlPhase::PollRead)
        }
    }

    /// Reads every `(remote_addr, length)` window through one native SDK batch.
    /// `output` must be exactly the sum of the window lengths; the native layer
    /// scatters each window into the same order and never publishes partial data.
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
            recv_reply(receiver, HixlPhase::PollRead)
        }
    }

    /// Compatibility read API that returns the completed payload bytes.
    pub fn read(
        &self,
        peer: &str,
        remote_addr: u64,
        len: usize,
        deadline: Instant,
    ) -> Result<Vec<u8>, HixlError> {
        self.read_timed(peer, remote_addr, len, deadline)
            .map(|outcome| outcome.bytes)
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
            Command::PrepareExport {
                data,
                offset,
                length,
                deadline,
                reply,
            } => {
                let result = data
                    .get(offset..offset.saturating_add(length))
                    .ok_or_else(|| {
                        HixlError::new(
                            HixlPhase::PrepareExport,
                            HixlErrorKind::InvalidArgument,
                            None,
                            "export range exceeds the shared snapshot",
                        )
                    })
                    .and_then(|slice| core.prepare_export_slice(slice, deadline));
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
            Command::Read {
                peer,
                remote_addr,
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
                        .saturating_add(1)
                        .min(core.options.receive_slots);
                    update_status(&status, admitted);
                }
                let result = core.read(&peer, remote_addr, output, deadline, owner_queue);
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
        pub fn afs_hixl_prepare_export(
            runtime: *mut c_void,
            slot_id: u64,
            generation: u64,
            data: *const u8,
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
        pub fn afs_hixl_submit_read(
            runtime: *mut c_void,
            slot_id: u64,
            peer: *const c_char,
            remote_addr: u64,
            len: usize,
            out_request: *mut *mut c_void,
            dispatch_started: *mut u32,
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
        pub fn afs_hixl_read_sync(
            runtime: *mut c_void,
            slot_id: u64,
            peer: *const c_char,
            remote_addr: u64,
            output: *mut u8,
            output_len: usize,
            timeout_ms: u32,
            dispatch_started: *mut u32,
            sdk_transfer_ns: *mut u64,
            output_copy_ns: *mut u64,
            error: *mut c_char,
            error_len: usize,
        ) -> u32;
        pub fn afs_hixl_read_sync_batch(
            runtime: *mut c_void,
            peer: *const c_char,
            windows: *const BatchWindow,
            count: usize,
            output: *mut u8,
            output_len: usize,
            timeout_ms: u32,
            dispatch_started: *mut u32,
            sdk_transfer_ns: *mut u64,
            output_copy_ns: *mut u64,
            error: *mut c_char,
            error_len: usize,
        ) -> u32;
        pub fn afs_hixl_poll_read(
            runtime: *mut c_void,
            request: *mut c_void,
            output: *mut u8,
            output_len: usize,
            out_state: *mut u32,
            sdk_transfer_ns: *mut u64,
            output_copy_ns: *mut u64,
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
                if options.transport == ReadTransport::HccsBufferPool {
                    options.buffer_pool_count
                } else {
                    0
                },
                if options.transport == ReadTransport::HccsBufferPool {
                    options.buffer_pool_size_mib
                } else {
                    0
                },
                &mut hccs_domain_id,
                &mut handle,
                error.ptr(),
                ERR_BYTES,
            )
        };
        if status != HIXL_SUCCESS || handle.is_null() {
            // The native bridge intentionally retains a non-null failed-start
            // handle when cleanup is incomplete. Do not destroy it or allow a
            // preferred-mode capability fallback in that case.
            return Err(NativeCallError {
                status: Some(status),
                message: if handle.is_null() {
                    error.message()
                } else {
                    format!(
                        "{}; startup cleanup incomplete, resources retained",
                        error.message()
                    )
                },
                accepted: !handle.is_null(),
            });
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

    fn prepare_export(
        &mut self,
        slot_id: u64,
        generation: u64,
        data: &[u8],
    ) -> Result<u64, NativeCallError> {
        let handle = self.handle.ok_or_else(native_not_initialized)?;
        let mut output = ffi::Window::default();
        let mut error = ErrorBuffer::default();
        // SAFETY: data and output/error buffers remain live for the synchronous
        // copy/registration call; native does not retain the Rust data pointer.
        let status = unsafe {
            ffi::afs_hixl_prepare_export(
                handle.as_ptr(),
                slot_id,
                generation,
                data.as_ptr(),
                data.len(),
                &mut output,
                error.ptr(),
                ERR_BYTES,
            )
        };
        if status == HIXL_SUCCESS {
            let expected_length = u64::try_from(data.len()).map_err(|_| NativeCallError {
                status: None,
                message: "export length does not fit the native ABI".to_owned(),
                accepted: false,
            })?;
            if output.address == 0 || output.length != expected_length {
                return Err(NativeCallError {
                    status: None,
                    message: "native HIXL returned an invalid export window".to_owned(),
                    accepted: false,
                });
            }
            Ok(output.address)
        } else {
            Err(NativeCallError {
                status: Some(status),
                message: error.message(),
                accepted: false,
            })
        }
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

    fn submit_read(
        &mut self,
        slot_id: u64,
        peer: &str,
        remote_addr: u64,
        len: usize,
    ) -> Result<NativeRequest, NativeCallError> {
        let peer = std::ffi::CString::new(peer).map_err(|error| NativeCallError {
            status: None,
            message: format!("peer engine id contains an interior NUL: {error}"),
            accepted: false,
        })?;
        let handle = self.handle.ok_or_else(native_not_initialized)?;
        let mut request = std::ptr::null_mut();
        let mut dispatch_started = 0_u32;
        let mut error = ErrorBuffer::default();
        // SAFETY: all pointers remain live for the synchronous submit; native
        // receives only its own registered slot address and stores the request handle.
        let status = unsafe {
            ffi::afs_hixl_submit_read(
                handle.as_ptr(),
                slot_id,
                peer.as_ptr(),
                remote_addr,
                len,
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

    fn read_sync(
        &mut self,
        slot_id: u64,
        peer: &str,
        remote_addr: u64,
        output: &mut [u8],
        timeout_ms: u32,
    ) -> Result<NativeReadTiming, NativeCallError> {
        let peer = std::ffi::CString::new(peer).map_err(|error| NativeCallError {
            status: None,
            message: format!("peer engine id contains an interior NUL: {error}"),
            accepted: false,
        })?;
        let handle = self.handle.ok_or_else(native_not_initialized)?;
        let mut error = ErrorBuffer::default();
        let mut dispatch_started = 0;
        let mut sdk_transfer_ns = 0_u64;
        let mut output_copy_ns = 0_u64;
        // SAFETY: the owner thread owns the runtime and output allocation for
        // the entire blocking call. Native retains only its own ACL Host slot,
        // and copies into output only after TransferSync reports completion.
        let status = unsafe {
            ffi::afs_hixl_read_sync(
                handle.as_ptr(),
                slot_id,
                peer.as_ptr(),
                remote_addr,
                output.as_mut_ptr(),
                output.len(),
                timeout_ms,
                &mut dispatch_started,
                &mut sdk_transfer_ns,
                &mut output_copy_ns,
                error.ptr(),
                ERR_BYTES,
            )
        };
        if status == HIXL_SUCCESS {
            Ok(NativeReadTiming {
                sdk_transfer: Some(Duration::from_nanos(sdk_transfer_ns)),
                output_copy: Some(Duration::from_nanos(output_copy_ns)),
            })
        } else {
            Err(NativeCallError {
                status: Some(status),
                message: error.message(),
                accepted: dispatch_started != 0,
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
        output: &mut [u8],
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
        let mut output_copy_ns = 0_u64;
        // SAFETY: the owner thread owns the runtime and output allocation for
        // the entire blocking call. Native copies into output only after the
        // whole TransferSync batch reports completion.
        let status = unsafe {
            ffi::afs_hixl_read_sync_batch(
                handle.as_ptr(),
                peer.as_ptr(),
                descriptors.as_ptr(),
                descriptors.len(),
                output.as_mut_ptr(),
                output.len(),
                timeout_ms,
                &mut dispatch_started,
                &mut sdk_transfer_ns,
                &mut output_copy_ns,
                error.ptr(),
                ERR_BYTES,
            )
        };
        if status == HIXL_SUCCESS {
            Ok(NativeReadTiming {
                sdk_transfer: Some(Duration::from_nanos(sdk_transfer_ns)),
                output_copy: Some(Duration::from_nanos(output_copy_ns)),
            })
        } else {
            Err(NativeCallError {
                status: Some(status),
                message: error.message(),
                accepted: dispatch_started != 0,
            })
        }
    }

    fn poll_read(
        &mut self,
        request: NativeRequest,
        output: &mut [u8],
    ) -> Result<NativePoll, NativeCallError> {
        let handle = self.handle.ok_or_else(native_not_initialized)?;
        let request_ptr = usize::try_from(request.0).map_err(|_| NativeCallError {
            status: None,
            message: "native HIXL request handle does not fit this platform".to_owned(),
            accepted: true,
        })?;
        let mut state = 0_u32;
        let mut sdk_transfer_ns = 0_u64;
        let mut output_copy_ns = 0_u64;
        let mut error = ErrorBuffer::default();
        // SAFETY: request is an opaque handle returned by the same native engine;
        // output is writable for exactly one registered-slot copy after completion.
        let status = unsafe {
            ffi::afs_hixl_poll_read(
                handle.as_ptr(),
                request_ptr as *mut std::ffi::c_void,
                output.as_mut_ptr(),
                output.len(),
                &mut state,
                &mut sdk_transfer_ns,
                &mut output_copy_ns,
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
                    output_copy: Some(Duration::from_nanos(output_copy_ns)),
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
mod tests {
    #[cfg(feature = "hixl")]
    use super::{Command, HixlRuntime};
    use super::{
        HIXL_UNSUPPORTED, HixlErrorKind, HixlPhase, NativeBackend, NativeCallError, NativePoll,
        NativeReadWindow, NativeRequest, RuntimeCore, RuntimeOptions, RuntimeState,
        map_native_error,
    };
    #[cfg(feature = "hixl")]
    use std::sync::{Arc, Mutex, RwLock, mpsc};
    use std::time::{Duration, Instant};

    #[derive(Default)]
    struct FakeBackend {
        next_request: u64,
        connect_delay: Option<Duration>,
        polls: usize,
        submitted: Vec<(u64, u64, usize)>,
        poll_result: Option<NativePoll>,
        poll_sdk_transfer: Option<Duration>,
        poll_output_copy: Option<Duration>,
        poll_error: Option<NativeCallError>,
        submit_error: Option<NativeCallError>,
        sync_reads: usize,
        sync_timeout_ms: u32,
        sync_sdk_transfer: Option<Duration>,
        sync_output_copy: Option<Duration>,
        sync_error: Option<NativeCallError>,
        batch_submissions: Vec<Vec<NativeReadWindow>>,
        sync_batches: Vec<Vec<NativeReadWindow>>,
        submit_batch_error: Option<NativeCallError>,
        sync_batch_error: Option<NativeCallError>,
        shutdown_calls: usize,
    }

    impl NativeBackend for FakeBackend {
        #[cfg(feature = "hixl")]
        fn start(_: &RuntimeOptions) -> Result<Self, NativeCallError> {
            Ok(Self::default())
        }

        fn hccs_domain_id(&self) -> Option<u64> {
            Some(7)
        }

        fn connect(&mut self, _: &str, _: u32) -> Result<(), NativeCallError> {
            if let Some(delay) = self.connect_delay {
                std::thread::sleep(delay);
            }
            Ok(())
        }

        fn prepare_export(&mut self, _: u64, _: u64, _: &[u8]) -> Result<u64, NativeCallError> {
            Ok(0x1000)
        }

        fn release_export(&mut self, _: u64, _: u64) -> Result<(), NativeCallError> {
            Ok(())
        }

        fn submit_read(
            &mut self,
            _: u64,
            _: &str,
            remote_addr: u64,
            len: usize,
        ) -> Result<NativeRequest, NativeCallError> {
            if let Some(error) = self.submit_error.take() {
                return Err(error);
            }
            self.next_request += 1;
            self.submitted.push((remote_addr, 0x2000, len));
            Ok(NativeRequest(self.next_request))
        }

        fn poll_read(
            &mut self,
            _: NativeRequest,
            output: &mut [u8],
        ) -> Result<NativePoll, NativeCallError> {
            self.polls += 1;
            if let Some(error) = &self.poll_error {
                return Err(error.clone());
            }
            let default_timing = super::NativeReadTiming {
                sdk_transfer: self.poll_sdk_transfer,
                output_copy: self.poll_output_copy,
            };
            match self.poll_result.clone().unwrap_or(NativePoll::Completed {
                timing: default_timing,
            }) {
                NativePoll::Completed { timing } => {
                    output[..4].copy_from_slice(b"read");
                    Ok(NativePoll::Completed { timing })
                }
                result => Ok(result),
            }
        }

        fn read_sync(
            &mut self,
            _: u64,
            _: &str,
            _: u64,
            output: &mut [u8],
            timeout_ms: u32,
        ) -> Result<super::NativeReadTiming, NativeCallError> {
            self.sync_reads += 1;
            self.sync_timeout_ms = timeout_ms;
            if let Some(error) = self.sync_error.take() {
                return Err(error);
            }
            output.copy_from_slice(b"read");
            Ok(super::NativeReadTiming {
                sdk_transfer: self.sync_sdk_transfer,
                output_copy: self.sync_output_copy,
            })
        }

        fn submit_read_batch(
            &mut self,
            windows: &[NativeReadWindow],
            _peer: &str,
        ) -> Result<NativeRequest, NativeCallError> {
            if let Some(error) = self.submit_batch_error.take() {
                return Err(error);
            }
            self.batch_submissions.push(windows.to_vec());
            self.next_request += 1;
            Ok(NativeRequest(self.next_request))
        }

        fn read_sync_batch(
            &mut self,
            windows: &[NativeReadWindow],
            _peer: &str,
            output: &mut [u8],
            timeout_ms: u32,
        ) -> Result<super::NativeReadTiming, NativeCallError> {
            self.sync_timeout_ms = timeout_ms;
            if let Some(error) = self.sync_batch_error.take() {
                return Err(error);
            }
            self.sync_batches.push(windows.to_vec());
            output[..4].copy_from_slice(b"read");
            Ok(super::NativeReadTiming {
                sdk_transfer: self.sync_sdk_transfer,
                output_copy: self.sync_output_copy,
            })
        }

        fn shutdown(&mut self) -> Result<(), NativeCallError> {
            self.shutdown_calls += 1;
            Ok(())
        }
    }

    fn options() -> RuntimeOptions {
        RuntimeOptions {
            device_id: 0,
            engine_id: "127.0.0.1:7920".into(),
            window_bytes: 4,
            receive_slots: 1,
            export_slots: 1,
            max_inflight_per_peer: 1,
            ..RuntimeOptions::default()
        }
    }

    #[cfg(feature = "hixl")]
    fn runtime_with_fake_backend(backend: FakeBackend, options: RuntimeOptions) -> HixlRuntime {
        let mut core = RuntimeCore::new(backend, options).unwrap();
        core.connect("peer", Instant::now() + Duration::from_secs(1))
            .unwrap();
        let info = core.info();
        let status = Arc::new(RwLock::new(core.status()));
        let owner_status = Arc::clone(&status);
        let (sender, receiver) = mpsc::sync_channel(8);
        let owner = std::thread::spawn(move || {
            super::owner_loop(core, receiver, owner_status);
        });
        HixlRuntime {
            info,
            sender,
            status,
            owner: Mutex::new(Some(owner)),
        }
    }

    #[test]
    #[cfg(feature = "hixl")]
    fn timed_hccs_read_returns_owner_queue_sdk_and_copy_durations() {
        let mut read_options = options();
        read_options.transport = super::ReadTransport::HccsBufferPool;
        let runtime = runtime_with_fake_backend(
            FakeBackend {
                connect_delay: Some(Duration::from_millis(40)),
                sync_sdk_transfer: Some(Duration::from_millis(9)),
                sync_output_copy: Some(Duration::from_millis(2)),
                ..FakeBackend::default()
            },
            read_options,
        );

        let (reply, _response) = mpsc::sync_channel(1);
        runtime
            .send(
                Command::Connect {
                    peer: "queue-blocker".to_owned(),
                    deadline: Instant::now() + Duration::from_secs(1),
                    reply,
                },
                HixlPhase::Connect,
            )
            .unwrap();
        let outcome = runtime
            .read_timed("peer", 0xfeed, 4, Instant::now() + Duration::from_secs(1))
            .unwrap();

        assert_eq!(outcome.bytes, b"read");
        assert!(outcome.timing.owner_queue >= Duration::from_millis(15));
        assert_eq!(outcome.timing.sdk_transfer, Some(Duration::from_millis(9)));
        assert_eq!(outcome.timing.output_copy, Some(Duration::from_millis(2)));
        runtime.shutdown().unwrap();
    }

    #[test]
    #[cfg(feature = "hixl")]
    fn timed_roce_read_returns_dispatch_completion_and_copy_durations() {
        let runtime = runtime_with_fake_backend(
            FakeBackend {
                poll_sdk_transfer: Some(Duration::from_millis(11)),
                poll_output_copy: Some(Duration::from_millis(3)),
                ..FakeBackend::default()
            },
            options(),
        );
        let outcome = runtime
            .read_timed("peer", 0xfeed, 4, Instant::now() + Duration::from_secs(1))
            .unwrap();

        assert_eq!(outcome.bytes, b"read");
        assert_eq!(outcome.timing.sdk_transfer, Some(Duration::from_millis(11)));
        assert_eq!(outcome.timing.output_copy, Some(Duration::from_millis(3)));
        runtime.shutdown().unwrap();
    }

    #[test]
    #[cfg(feature = "hixl")]
    fn timed_read_preserves_native_uncertainty_errors() {
        let mut read_options = options();
        read_options.transport = super::ReadTransport::HccsBufferPool;
        let runtime = runtime_with_fake_backend(
            FakeBackend {
                sync_error: Some(NativeCallError {
                    status: Some(super::HIXL_TIMEOUT),
                    message: "uncertain synchronous completion".to_owned(),
                    accepted: true,
                }),
                ..FakeBackend::default()
            },
            read_options,
        );
        let error = runtime
            .read_timed("peer", 0xfeed, 4, Instant::now() + Duration::from_secs(1))
            .unwrap_err();

        assert_eq!(error.kind, HixlErrorKind::DeadlineExceeded);
        assert!(error.read_may_be_in_flight);
        assert_eq!(runtime.status().state, RuntimeState::Quarantined);
    }

    #[test]
    fn runtime_options_default_device_listen_port_is_unset() {
        assert_eq!(RuntimeOptions::default().device_listen_port, None);
    }

    #[test]
    fn runtime_core_rejects_zero_device_listen_port_before_backend_entry() {
        let mut opts = options();
        opts.device_listen_port = Some(0);
        let error = match RuntimeCore::new(FakeBackend::default(), opts) {
            Ok(_) => panic!("zero device listen port must be rejected"),
            Err(error) => error,
        };
        assert_eq!(error.kind, HixlErrorKind::InvalidArgument);
        assert!(error.message.contains("device_listen_port"));
    }

    #[test]
    fn runtime_core_preserves_max_device_listen_port() {
        let mut opts = options();
        opts.device_listen_port = Some(u16::MAX);
        let core = RuntimeCore::new(FakeBackend::default(), opts).unwrap();
        assert_eq!(core.options.device_listen_port, Some(u16::MAX));
    }

    #[test]
    fn runtime_core_uses_a_generation_created_before_native_startup() {
        let core = RuntimeCore::new_with_generation(FakeBackend::default(), options(), 42).unwrap();
        assert_eq!(core.info().generation, 42);
    }

    #[test]
    fn separate_runtime_instances_have_distinct_nonzero_generations() {
        let first = RuntimeCore::new(FakeBackend::default(), options()).unwrap();
        let second = RuntimeCore::new(FakeBackend::default(), options()).unwrap();
        assert_ne!(first.info().generation, 0);
        assert_ne!(first.info().generation, second.info().generation);
    }

    #[test]
    fn hccs_relay_budget_counts_both_internal_hbm_pools() {
        let mut opts = options();
        opts.transport = super::ReadTransport::HccsBufferPool;
        opts.buffer_pool_count = 4;
        opts.buffer_pool_size_mib = 8;
        let core = RuntimeCore::new(FakeBackend::default(), opts).unwrap();
        assert_eq!(core.status().relay_hbm_budget_bytes, 64 * 1024 * 1024);
        assert_eq!(core.status().allocated_bytes, 8);
        assert_eq!(core.info().transport, super::ReadTransport::HccsBufferPool);
    }

    #[test]
    fn roce_does_not_budget_an_unused_hbm_relay_pool() {
        let core = RuntimeCore::new(FakeBackend::default(), options()).unwrap();
        assert_eq!(core.status().transport, super::ReadTransport::Roce);
        assert_eq!(core.status().relay_hbm_budget_bytes, 0);
        assert_eq!(core.info().buffer_pool_count, 0);
        assert_eq!(core.info().buffer_pool_size_mib, 0);
    }

    #[test]
    fn hccs_rejects_hbm_budget_overflow_and_exhaustion_before_backend_entry() {
        for (count, size) in [
            (0, 8),
            (4, 0),
            (65, 8),
            (4, 65),
            (64, 8),
            (usize::MAX, usize::MAX),
        ] {
            let mut opts = options();
            opts.transport = super::ReadTransport::HccsBufferPool;
            opts.buffer_pool_count = count;
            opts.buffer_pool_size_mib = size;
            assert!(RuntimeCore::new(FakeBackend::default(), opts).is_err());
        }
    }

    #[test]
    fn hccs_read_uses_sync_backend_with_remaining_deadline_and_no_async_poll() {
        let mut opts = options();
        opts.transport = super::ReadTransport::HccsBufferPool;
        let mut core = RuntimeCore::new(FakeBackend::default(), opts).unwrap();
        let deadline = Instant::now() + Duration::from_secs(1);
        core.connect("peer", deadline).unwrap();
        assert_eq!(
            core.read("peer", 0xfeed, vec![0; 4], deadline, Duration::ZERO)
                .unwrap()
                .bytes,
            b"read"
        );
        assert_eq!(core.backend.sync_reads, 1);
        assert!((1..=1000).contains(&core.backend.sync_timeout_ms));
        assert!(core.backend.submitted.is_empty());
        assert_eq!(core.backend.polls, 0);
        assert_eq!(core.status().receive_slots_in_use, 0);
        core.shutdown().unwrap();
    }

    #[test]
    fn hccs_sync_failure_or_timeout_retains_generation_and_never_replays() {
        for status in [super::HIXL_TIMEOUT, 1] {
            let mut opts = options();
            opts.transport = super::ReadTransport::HccsBufferPool;
            let backend = FakeBackend {
                sync_error: Some(NativeCallError {
                    status: Some(status),
                    message: "sync outcome is uncertain".into(),
                    accepted: true,
                }),
                ..FakeBackend::default()
            };
            let mut core = RuntimeCore::new(backend, opts).unwrap();
            assert!(core.backend.sync_error.is_some());
            let deadline = Instant::now() + Duration::from_secs(1);
            core.connect("peer", deadline).unwrap();
            let error = core
                .read("peer", 0xfeed, vec![0; 4], deadline, Duration::ZERO)
                .unwrap_err();
            assert!(error.read_may_be_in_flight);
            assert_eq!(core.status().state, RuntimeState::Quarantined);
            assert_eq!(core.status().quarantined_bytes, 4);
            assert_eq!(core.status().relay_hbm_budget_bytes, 64 * 1024 * 1024);
            assert!(
                core.read("peer", 0xfeed, vec![0; 4], deadline, Duration::ZERO)
                    .is_err()
            );
            assert!(core.shutdown().is_err());
            assert_eq!(core.backend.sync_reads, 1);
            assert_eq!(core.backend.shutdown_calls, 0);
        }
    }

    #[test]
    fn read_uses_remote_to_local_direction_and_polls_completion_once() {
        let mut core = RuntimeCore::new(FakeBackend::default(), options()).unwrap();
        core.connect(
            "peer",
            std::time::Instant::now() + std::time::Duration::from_secs(1),
        )
        .unwrap();
        let outcome = core
            .read(
                "peer",
                0xfeed,
                vec![0; 4],
                std::time::Instant::now() + std::time::Duration::from_secs(1),
                Duration::ZERO,
            )
            .unwrap();
        assert_eq!(outcome.bytes, b"read");
        assert_eq!(core.backend.submitted, vec![(0xfeed, 0x2000, 4)]);
        assert_eq!(core.backend.polls, 1);
        assert_eq!(core.status().state, RuntimeState::Running);
        core.shutdown().unwrap();
        assert_eq!(core.backend.shutdown_calls, 1);
    }

    #[test]
    fn submit_failure_releases_receive_slot_without_poisoning_generation() {
        let backend = FakeBackend {
            submit_error: Some(NativeCallError::new(Some(503_900), "submit")),
            ..FakeBackend::default()
        };
        let mut core = RuntimeCore::new(backend, options()).unwrap();
        core.connect(
            "peer",
            std::time::Instant::now() + std::time::Duration::from_secs(1),
        )
        .unwrap();
        let error = core
            .read(
                "peer",
                0xfeed,
                vec![0; 4],
                std::time::Instant::now() + std::time::Duration::from_secs(1),
                Duration::ZERO,
            )
            .unwrap_err();
        assert_eq!(error.kind, HixlErrorKind::NativeFailure);
        assert!(!error.read_may_be_in_flight);
        assert_eq!(core.status().state, RuntimeState::Running);
        assert_eq!(core.status().receive_slots_in_use, 0);
        assert_eq!(core.status().quarantined_bytes, 0);
    }

    #[test]
    fn submit_error_after_entering_native_api_retains_generation() {
        let backend = FakeBackend {
            submit_error: Some(NativeCallError {
                status: Some(503_900),
                message: "uncertain submission".into(),
                accepted: true,
            }),
            ..FakeBackend::default()
        };
        let mut core = RuntimeCore::new(backend, options()).unwrap();
        core.connect("peer", Instant::now() + Duration::from_secs(1))
            .unwrap();
        let error = core
            .read(
                "peer",
                0xfeed,
                vec![0; 4],
                Instant::now() + Duration::from_secs(1),
                Duration::ZERO,
            )
            .unwrap_err();
        assert_eq!(error.kind, HixlErrorKind::EnginePoisoned);
        assert!(error.read_may_be_in_flight);
        assert_eq!(core.status().quarantined_bytes, 4);
        assert!(core.shutdown().is_err());
        assert_eq!(core.backend.shutdown_calls, 0);
    }

    #[test]
    #[cfg(feature = "hixl")]
    fn bounded_owner_queue_refuses_pressure_without_native_dispatch() {
        let core = RuntimeCore::new(FakeBackend::default(), options()).unwrap();
        let (sender, _receiver) = mpsc::sync_channel(1);
        let runtime = HixlRuntime {
            info: core.info(),
            sender,
            status: Arc::new(RwLock::new(core.status())),
            owner: Mutex::new(None),
        };
        let (reply, _response) = mpsc::sync_channel(1);
        runtime
            .send(Command::Quarantine { reply }, HixlPhase::Quarantine)
            .unwrap();
        let error = runtime
            .read("peer", 0xfeed, 4, Instant::now() + Duration::from_secs(1))
            .unwrap_err();
        assert_eq!(error.kind, HixlErrorKind::ResourceExhausted);
        assert!(!error.read_may_be_in_flight);
        assert_eq!(runtime.status().state, RuntimeState::Running);
        assert_eq!(core.backend.shutdown_calls, 0);
    }

    #[test]
    #[cfg(feature = "hixl")]
    fn lost_owner_reply_does_not_prove_a_read_was_never_submitted() {
        let (reply, receiver) =
            mpsc::sync_channel::<Result<super::HixlReadOutcome, super::HixlError>>(1);
        drop(reply);
        let error = super::recv_reply(receiver, HixlPhase::PollRead).unwrap_err();
        assert_eq!(error.kind, HixlErrorKind::EnginePoisoned);
        assert!(error.read_may_be_in_flight);
    }

    #[test]
    fn deadline_after_accepted_request_quarantines_receive_pool() {
        let backend = FakeBackend {
            poll_result: Some(NativePoll::Waiting),
            ..FakeBackend::default()
        };
        let mut core = RuntimeCore::new(backend, options()).unwrap();
        core.connect(
            "peer",
            std::time::Instant::now() + std::time::Duration::from_secs(1),
        )
        .unwrap();
        let error = core
            .read(
                "peer",
                0xfeed,
                vec![0; 4],
                std::time::Instant::now() + std::time::Duration::from_millis(2),
                Duration::ZERO,
            )
            .unwrap_err();
        assert_eq!(error.kind, HixlErrorKind::DeadlineExceeded);
        assert!(error.read_may_be_in_flight);
        assert_eq!(core.status().state, RuntimeState::Quarantined);
        assert_eq!(core.status().quarantined_bytes, 4);
        assert_eq!(core.status().receive_slots_in_use, 1);
        let shutdown = core.shutdown().unwrap_err();
        assert_eq!(shutdown.kind, HixlErrorKind::ShutdownIncomplete);
        assert_eq!(core.backend.shutdown_calls, 0);
    }

    #[test]
    fn unknown_completion_retains_native_generation_and_refuses_reuse() {
        let backend = FakeBackend {
            poll_error: Some(NativeCallError {
                status: None,
                message: "unknown completion".into(),
                accepted: true,
            }),
            ..FakeBackend::default()
        };
        let mut core = RuntimeCore::new(backend, options()).unwrap();
        core.connect(
            "peer",
            std::time::Instant::now() + std::time::Duration::from_secs(1),
        )
        .unwrap();
        let error = core
            .read(
                "peer",
                0xfeed,
                vec![0; 4],
                std::time::Instant::now() + std::time::Duration::from_secs(1),
                Duration::ZERO,
            )
            .unwrap_err();
        assert_eq!(error.kind, HixlErrorKind::CompletionFailed);
        assert!(error.read_may_be_in_flight);
        assert_eq!(core.status().state, RuntimeState::Quarantined);
        assert_eq!(core.status().quarantined_bytes, 4);
        let reuse = core
            .read(
                "peer",
                0xfeed,
                vec![0; 4],
                std::time::Instant::now() + std::time::Duration::from_secs(1),
                Duration::ZERO,
            )
            .unwrap_err();
        assert_eq!(reuse.kind, HixlErrorKind::EnginePoisoned);
        assert!(!reuse.read_may_be_in_flight);
        assert_eq!(core.backend.shutdown_calls, 0);
    }

    #[test]
    fn export_pool_capacity_and_generation_prevent_stale_release() {
        let mut core = RuntimeCore::new(FakeBackend::default(), options()).unwrap();
        let first = core
            .prepare_export(
                b"one".to_vec(),
                std::time::Instant::now() + std::time::Duration::from_secs(1),
            )
            .unwrap();
        let full = core
            .prepare_export(
                b"two".to_vec(),
                std::time::Instant::now() + std::time::Duration::from_secs(1),
            )
            .unwrap_err();
        assert_eq!(full.kind, HixlErrorKind::ResourceExhausted);
        core.release_export(first.slot_id, first.generation)
            .unwrap();
        let second = core
            .prepare_export(
                b"new".to_vec(),
                std::time::Instant::now() + std::time::Duration::from_secs(1),
            )
            .unwrap();
        assert_ne!(first.generation, second.generation);
        let stale = core
            .release_export(first.slot_id, first.generation)
            .unwrap_err();
        assert_eq!(stale.kind, HixlErrorKind::InvalidArgument);
    }

    #[test]
    fn explicit_quarantine_retains_unresolved_export_generation() {
        let mut core = RuntimeCore::new(FakeBackend::default(), options()).unwrap();
        let window = core
            .prepare_export(
                b"one".to_vec(),
                std::time::Instant::now() + std::time::Duration::from_secs(1),
            )
            .unwrap();
        assert_eq!(core.status().allocated_bytes, 8);
        assert_eq!(core.status().quarantined_bytes, 0);

        core.quarantine().unwrap();

        let status = core.status();
        assert_eq!(status.state, RuntimeState::Quarantined);
        assert_eq!(status.allocated_bytes, 8);
        assert_eq!(status.quarantined_bytes, 4);
        assert_eq!(status.export_slots_in_use, 1);
        assert_eq!(status.receive_slots_in_use, 0);
        let release = core
            .release_export(window.slot_id, window.generation)
            .unwrap_err();
        assert_eq!(release.kind, HixlErrorKind::EnginePoisoned);
        let shutdown = core.shutdown().unwrap_err();
        assert_eq!(shutdown.kind, HixlErrorKind::ShutdownIncomplete);
        assert_eq!(core.backend.shutdown_calls, 0);
    }

    #[test]
    fn failed_start_cleanup_cannot_be_a_safe_unsupported_capability() {
        let error = map_native_error(
            HixlPhase::Initialize,
            NativeCallError {
                status: Some(HIXL_UNSUPPORTED),
                message: "registered startup memory remains retained".to_owned(),
                accepted: true,
            },
            HixlErrorKind::NativeFailure,
        );
        assert_eq!(error.kind, HixlErrorKind::ShutdownIncomplete);
        assert_eq!(error.native_status, Some(HIXL_UNSUPPORTED));
    }

    const MIB: usize = 1024 * 1024;

    fn batch_options(receive_slots: usize) -> RuntimeOptions {
        RuntimeOptions {
            window_bytes: 4 * MIB,
            receive_slots,
            export_slots: 1,
            ..options()
        }
    }

    fn batch_windows(count: usize, len: usize) -> Vec<(u64, usize)> {
        (0..count)
            .map(|index| (0x10_0000 + (index * len) as u64, len))
            .collect()
    }

    fn connected_batch_core(mut core: RuntimeCore<FakeBackend>) -> RuntimeCore<FakeBackend> {
        core.connect("peer", Instant::now() + Duration::from_secs(1))
            .unwrap();
        core
    }

    #[test]
    fn batch_read_submits_25_windows_as_one_100_mib_native_call() {
        let mut core = connected_batch_core(
            RuntimeCore::new(FakeBackend::default(), batch_options(25)).unwrap(),
        );
        let windows = batch_windows(25, 4 * MIB);
        let outcome = core
            .read_batch(
                "peer",
                &windows,
                vec![0; 100 * MIB],
                Instant::now() + Duration::from_secs(1),
                Duration::ZERO,
            )
            .unwrap();

        assert_eq!(outcome.bytes.len(), 104_857_600);
        assert_eq!(core.backend.batch_submissions.len(), 1);
        let submitted = &core.backend.batch_submissions[0];
        assert_eq!(submitted.len(), 25);
        let mut local_offset = 0;
        for (index, window) in submitted.iter().enumerate() {
            assert_eq!(window.slot_id, index as u64);
            assert_eq!(window.remote_addr, windows[index].0);
            assert_eq!(window.len, 4 * MIB);
            assert_eq!(window.local_offset, local_offset);
            local_offset += 4 * MIB;
        }
        assert_eq!(local_offset, 104_857_600);
        assert_eq!(core.status().receive_slots_in_use, 0);
        assert_eq!(core.status().state, RuntimeState::Running);
        core.shutdown().unwrap();
    }

    #[test]
    fn batch_read_rejects_invalid_window_sets_before_native_entry() {
        let mut core = connected_batch_core(
            RuntimeCore::new(FakeBackend::default(), batch_options(4)).unwrap(),
        );
        let deadline = Instant::now() + Duration::from_secs(1);
        let cases: Vec<(Vec<(u64, usize)>, usize)> = vec![
            (Vec::new(), 0),
            (vec![(0x1000, 4 * MIB); 5], 20 * MIB),
            (vec![(0x1000, 4 * MIB + 1)], 4 * MIB + 1),
            (vec![(0x1000, 4)], 3),
            (vec![(0, 4)], 4),
            (vec![(0x1000, 0)], 0),
        ];
        for (windows, output_len) in cases {
            let error = core
                .read_batch(
                    "peer",
                    &windows,
                    vec![0; output_len],
                    deadline,
                    Duration::ZERO,
                )
                .unwrap_err();
            assert_eq!(error.kind, HixlErrorKind::InvalidArgument);
        }
        assert!(core.backend.batch_submissions.is_empty());
        assert_eq!(core.status().receive_slots_in_use, 0);
    }

    #[test]
    fn hccs_batch_read_uses_one_sync_call_and_reports_timing() {
        let mut opts = batch_options(2);
        opts.transport = super::ReadTransport::HccsBufferPool;
        let backend = FakeBackend {
            sync_sdk_transfer: Some(Duration::from_millis(7)),
            sync_output_copy: Some(Duration::from_millis(2)),
            ..FakeBackend::default()
        };
        let mut core = connected_batch_core(RuntimeCore::new(backend, opts).unwrap());
        let outcome = core
            .read_batch(
                "peer",
                &batch_windows(2, 4 * MIB),
                vec![0; 8 * MIB],
                Instant::now() + Duration::from_secs(1),
                Duration::from_millis(3),
            )
            .unwrap();

        assert_eq!(core.backend.sync_batches.len(), 1);
        assert_eq!(core.backend.sync_batches[0].len(), 2);
        assert_eq!(outcome.timing.owner_queue, Duration::from_millis(3));
        assert_eq!(outcome.timing.sdk_transfer, Some(Duration::from_millis(7)));
        assert_eq!(outcome.timing.output_copy, Some(Duration::from_millis(2)));
        assert_eq!(core.status().receive_slots_in_use, 0);
        core.shutdown().unwrap();
    }

    #[test]
    fn hccs_batch_expired_deadline_releases_every_slot_before_native_entry() {
        let mut opts = batch_options(2);
        opts.transport = super::ReadTransport::HccsBufferPool;
        let mut core =
            connected_batch_core(RuntimeCore::new(FakeBackend::default(), opts).unwrap());
        let deadline = Instant::now()
            .checked_sub(Duration::from_millis(1))
            .unwrap();
        let error = core
            .read_batch(
                "peer",
                &batch_windows(2, 4 * MIB),
                vec![0; 8 * MIB],
                deadline,
                Duration::ZERO,
            )
            .unwrap_err();

        assert_eq!(error.kind, HixlErrorKind::DeadlineExceeded);
        assert!(!error.read_may_be_in_flight);
        assert_eq!(core.status().state, RuntimeState::Running);
        assert_eq!(core.status().receive_slots_in_use, 0);
        assert!(core.backend.sync_batches.is_empty());
        core.shutdown().unwrap();
    }

    #[test]
    fn hccs_batch_uncertain_sync_quarantines_every_slot() {
        for status in [super::HIXL_TIMEOUT, 1] {
            let mut opts = batch_options(2);
            opts.transport = super::ReadTransport::HccsBufferPool;
            let backend = FakeBackend {
                sync_batch_error: Some(NativeCallError {
                    status: Some(status),
                    message: "batch sync outcome is uncertain".into(),
                    accepted: true,
                }),
                ..FakeBackend::default()
            };
            let mut core = connected_batch_core(RuntimeCore::new(backend, opts).unwrap());
            let error = core
                .read_batch(
                    "peer",
                    &batch_windows(2, 4 * MIB),
                    vec![0; 8 * MIB],
                    Instant::now() + Duration::from_secs(1),
                    Duration::ZERO,
                )
                .unwrap_err();

            assert!(error.read_may_be_in_flight);
            assert_eq!(core.status().state, RuntimeState::Quarantined);
            assert_eq!(core.status().receive_slots_in_use, 2);
            assert_eq!(core.status().quarantined_bytes, 8 * MIB);
            assert_eq!(core.backend.sync_batches.len(), 0);
            assert_eq!(core.backend.shutdown_calls, 0);
            assert!(core.shutdown().is_err());
        }
    }

    #[test]
    fn batch_submit_rejected_before_native_entry_releases_every_slot() {
        let backend = FakeBackend {
            submit_batch_error: Some(NativeCallError::new(Some(503_900), "batch submit")),
            ..FakeBackend::default()
        };
        let mut core = connected_batch_core(RuntimeCore::new(backend, batch_options(2)).unwrap());
        let error = core
            .read_batch(
                "peer",
                &batch_windows(2, 4 * MIB),
                vec![0; 8 * MIB],
                Instant::now() + Duration::from_secs(1),
                Duration::ZERO,
            )
            .unwrap_err();

        assert_eq!(error.kind, HixlErrorKind::NativeFailure);
        assert!(!error.read_may_be_in_flight);
        assert_eq!(core.status().state, RuntimeState::Running);
        assert_eq!(core.status().receive_slots_in_use, 0);
    }

    #[test]
    fn batch_submit_error_after_native_entry_quarantines_every_slot() {
        let backend = FakeBackend {
            submit_batch_error: Some(NativeCallError {
                status: Some(503_900),
                message: "uncertain batch submission".into(),
                accepted: true,
            }),
            ..FakeBackend::default()
        };
        let mut core = connected_batch_core(RuntimeCore::new(backend, batch_options(2)).unwrap());
        let error = core
            .read_batch(
                "peer",
                &batch_windows(2, 4 * MIB),
                vec![0; 8 * MIB],
                Instant::now() + Duration::from_secs(1),
                Duration::ZERO,
            )
            .unwrap_err();

        assert_eq!(error.kind, HixlErrorKind::EnginePoisoned);
        assert!(error.read_may_be_in_flight);
        assert_eq!(core.status().receive_slots_in_use, 2);
        assert_eq!(core.status().quarantined_bytes, 8 * MIB);
        assert_eq!(core.backend.shutdown_calls, 0);
        assert!(core.shutdown().is_err());
    }

    #[test]
    fn batch_deadline_or_unknown_poll_quarantines_every_slot() {
        let waiting = FakeBackend {
            poll_result: Some(NativePoll::Waiting),
            ..FakeBackend::default()
        };
        let mut core = connected_batch_core(RuntimeCore::new(waiting, batch_options(2)).unwrap());
        let error = core
            .read_batch(
                "peer",
                &batch_windows(2, 4 * MIB),
                vec![0; 8 * MIB],
                Instant::now() + Duration::from_millis(2),
                Duration::ZERO,
            )
            .unwrap_err();
        assert_eq!(error.kind, HixlErrorKind::DeadlineExceeded);
        assert!(error.read_may_be_in_flight);
        assert_eq!(core.status().quarantined_bytes, 8 * MIB);
        assert_eq!(core.status().receive_slots_in_use, 2);

        let unknown = FakeBackend {
            poll_error: Some(NativeCallError {
                status: None,
                message: "unknown batch completion".into(),
                accepted: true,
            }),
            ..FakeBackend::default()
        };
        let mut core = connected_batch_core(RuntimeCore::new(unknown, batch_options(2)).unwrap());
        let error = core
            .read_batch(
                "peer",
                &batch_windows(2, 4 * MIB),
                vec![0; 8 * MIB],
                Instant::now() + Duration::from_secs(1),
                Duration::ZERO,
            )
            .unwrap_err();
        assert_eq!(error.kind, HixlErrorKind::CompletionFailed);
        assert!(error.read_may_be_in_flight);
        assert_eq!(core.status().quarantined_bytes, 8 * MIB);
        assert_eq!(core.status().receive_slots_in_use, 2);
    }

    #[test]
    fn batch_success_releases_slots_for_the_next_generation_of_reads() {
        let mut core = connected_batch_core(
            RuntimeCore::new(FakeBackend::default(), batch_options(2)).unwrap(),
        );
        for _ in 0..3 {
            core.read_batch(
                "peer",
                &batch_windows(2, 4 * MIB),
                vec![0; 8 * MIB],
                Instant::now() + Duration::from_secs(1),
                Duration::ZERO,
            )
            .unwrap();
            assert_eq!(core.status().receive_slots_in_use, 0);
        }
        assert_eq!(core.backend.batch_submissions.len(), 3);
        core.shutdown().unwrap();
    }
}
