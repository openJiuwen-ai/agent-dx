//! In-memory native export boundary and Home file source shared by the Home
//! service tests and the reader loopback tests.

use super::{ExportProvider, OwnerWindowSource, PresentedRootAccess, RemoteFile};
use afs_transport::hixl::{
    EngineInfo, ExportWindow, HixlError, HixlErrorKind, HixlPhase, ReadTransport,
};
use std::collections::HashMap;
use std::sync::Mutex;
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::time::Instant;

pub(crate) const HOME_ENGINE: &str = "127.0.0.1:7930";
pub(crate) const OPEN_HANDLE: &[u8] = b"open-1";

pub(crate) fn engine(engine_id: &str, generation: u64, window_bytes: usize) -> EngineInfo {
    EngineInfo {
        engine_id: engine_id.to_owned(),
        generation,
        window_bytes,
        transport: ReadTransport::Roce,
        buffer_pool_count: 0,
        buffer_pool_size_mib: 0,
        hccs_domain_id: None,
    }
}

pub(crate) fn native_error(phase: HixlPhase, kind: HixlErrorKind, message: &str) -> HixlError {
    HixlError {
        phase,
        kind,
        native_status: None,
        message: message.into(),
        read_may_be_in_flight: false,
    }
}

#[derive(Default)]
struct Memory {
    next_address: u64,
    next_generation: u64,
    // slot id -> (generation, address)
    active: HashMap<u64, (u64, u64)>,
    bytes: HashMap<u64, Vec<u8>>,
}

pub(crate) struct MemoryExports {
    info: EngineInfo,
    slots: usize,
    memory: Mutex<Memory>,
    pub(crate) exports: AtomicUsize,
    pub(crate) releases: AtomicUsize,
    pub(crate) quarantines: AtomicUsize,
    /// Fails the export call with this ordinal (1-based).
    pub(crate) fail_export_at: AtomicUsize,
    pub(crate) fail_release: AtomicBool,
}

impl MemoryExports {
    pub(crate) fn new(window_bytes: usize, slots: usize) -> Self {
        Self {
            info: engine(HOME_ENGINE, 7, window_bytes),
            slots,
            memory: Mutex::new(Memory::default()),
            exports: AtomicUsize::new(0),
            releases: AtomicUsize::new(0),
            quarantines: AtomicUsize::new(0),
            fail_export_at: AtomicUsize::new(0),
            fail_release: AtomicBool::new(false),
        }
    }

    pub(crate) fn active(&self) -> usize {
        self.memory.lock().unwrap().active.len()
    }

    /// The native READ view of an exported slot.
    pub(crate) fn read(&self, address: u64, length: usize) -> Option<Vec<u8>> {
        let memory = self.memory.lock().unwrap();
        memory
            .bytes
            .get(&address)
            .filter(|bytes| bytes.len() == length)
            .cloned()
    }
}

impl ExportProvider for MemoryExports {
    fn info(&self) -> EngineInfo {
        self.info.clone()
    }

    fn export_slots(&self, _lane: usize) -> usize {
        self.slots
    }

    fn prepare_export(
        &self,
        _lane: usize,
        bytes: Vec<u8>,
        _deadline: Instant,
    ) -> Result<ExportWindow, HixlError> {
        let ordinal = self.exports.fetch_add(1, Ordering::SeqCst) + 1;
        if self.fail_export_at.load(Ordering::SeqCst) == ordinal {
            return Err(native_error(
                HixlPhase::PrepareExport,
                HixlErrorKind::NativeFailure,
                "injected export failure",
            ));
        }
        let mut memory = self.memory.lock().unwrap();
        let slot_id = (0..self.slots as u64)
            .find(|slot| !memory.active.contains_key(slot))
            .ok_or_else(|| {
                native_error(
                    HixlPhase::PrepareExport,
                    HixlErrorKind::ResourceExhausted,
                    "all export slots are in use",
                )
            })?;
        memory.next_address += 0x1000;
        memory.next_generation += 1;
        let (address, generation) = (memory.next_address, memory.next_generation);
        memory.active.insert(slot_id, (generation, address));
        let length = bytes.len();
        memory.bytes.insert(address, bytes);
        Ok(ExportWindow {
            slot_id,
            generation,
            address,
            length,
        })
    }

    fn release_export(&self, _lane: usize, slot_id: u64, generation: u64) -> Result<(), HixlError> {
        if self.fail_release.load(Ordering::SeqCst) {
            return Err(native_error(
                HixlPhase::ReleaseExport,
                HixlErrorKind::NativeFailure,
                "injected release failure",
            ));
        }
        let mut memory = self.memory.lock().unwrap();
        match memory.active.get(&slot_id).copied() {
            Some((active, address)) if active == generation => {
                memory.active.remove(&slot_id);
                memory.bytes.remove(&address);
                self.releases.fetch_add(1, Ordering::SeqCst);
                Ok(())
            }
            _ => Err(native_error(
                HixlPhase::ReleaseExport,
                HixlErrorKind::InvalidState,
                "slot generation is not active",
            )),
        }
    }

    fn quarantine(&self) -> Result<(), HixlError> {
        self.quarantines.fetch_add(1, Ordering::SeqCst);
        Ok(())
    }
}

/// One Home file behind the `open-1` handle of root `root-a`.
pub(crate) struct MemorySource {
    pub(crate) data: Mutex<Vec<u8>>,
    /// Non-empty source reads; empty reads are pure authorization checks.
    pub(crate) reads: AtomicUsize,
    max_chunk: usize,
}

impl MemorySource {
    pub(crate) fn new(data: Vec<u8>, max_chunk: usize) -> Self {
        Self {
            data: Mutex::new(data),
            reads: AtomicUsize::new(0),
            max_chunk,
        }
    }
}

impl OwnerWindowSource for MemorySource {
    fn authorize_transport(
        &self,
        _peer: &str,
        access: &PresentedRootAccess,
    ) -> afs_error::Result<()> {
        if access.id.0 != "root-a" {
            return Err(afs_error::Error::coded(
                afs_error::NODE_OWNER_INVALID_GRANT,
                "unknown root",
            ));
        }
        Ok(())
    }

    fn read(
        &self,
        peer: &str,
        access: &PresentedRootAccess,
        file: &RemoteFile,
        offset: u64,
        out: &mut [u8],
    ) -> afs_error::Result<usize> {
        self.authorize_transport(peer, access)?;
        if file.handle != OPEN_HANDLE {
            return Err(afs_error::Error::coded(
                afs_error::NODE_OWNER_STALE_HANDLE,
                "unknown open handle",
            ));
        }
        if out.is_empty() {
            return Ok(0);
        }
        self.reads.fetch_add(1, Ordering::SeqCst);
        let data = self.data.lock().unwrap();
        let start = usize::try_from(offset)
            .unwrap_or(usize::MAX)
            .min(data.len());
        let count = (data.len() - start).min(out.len()).min(self.max_chunk);
        out[..count].copy_from_slice(&data[start..start + count]);
        Ok(count)
    }
}
