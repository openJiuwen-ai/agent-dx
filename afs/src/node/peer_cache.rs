//! Disposable native Peer object cache, never replicas or read authority.
//!
//! Two streaming fills share Node transport work; a legacy body is exclusive.
//! Metadata locks never cover runtime disk I/O, verification or Peer waits.
//! Ready bodies live only on disk. Hits verify complete requested 32 KiB coverage
//! groups against the trusted standard BLAKE3 root. Changed identity requires a root scan.
//! Callers must discard private output on a miss or error.

use std::{
    collections::BTreeMap,
    fs::{self, DirBuilder, File, OpenOptions},
    io::{Read, Seek, SeekFrom, Write},
    os::{
        fd::AsRawFd,
        unix::fs::{DirBuilderExt, MetadataExt, OpenOptionsExt},
    },
    path::{Component, Path, PathBuf},
    sync::{Arc, Condvar, Mutex},
    time::{Duration, Instant},
};

use afs_error::{Error, Result};
use serde::{Deserialize, Serialize};

use crate::{
    dfs::{ChunkEncoding, ChunkObject, DigestAlgorithm, NamespaceId},
    node::chunk::{
        VerifiedRangeCopy,
        outboard::{
            ChunkOutboard, OutboardBudget, OutboardBuilder, OutboardLease, OutboardReadWork,
            SharedOutboard, build_borrowed,
        },
    },
};

pub const MAX_PEER_CACHE_ENTRIES: usize = 4096;
const MAX_OBJECT_BYTES: u64 = 4 * 1024 * 1024;
const MAX_HEADER_BYTES: usize = 4096;
const SCRATCH_BYTES: usize = 64 * 1024;
#[cfg(test)]
const PEER_CACHE_SEGMENT_BYTES: usize = 1024 * 1024;
pub const MAX_PEER_CACHE_PROOF_BYTES: u64 = 1024 * 1024;
#[cfg(test)]
const COVERAGE_GROUP_BYTES: usize = 32 * 1024;
const MAX_OUTBOARD_BYTES: usize = 8192;
const MARKER: &str = ".afs-peer-cache-v1";
const PENDING: &str = ".afs-peer-cache.pending";
const MAGIC: &[u8; 8] = b"AFSPC002";
const OLD_MAGIC: &[u8; 8] = b"AFSPC001";

#[derive(Clone, Debug)]
pub struct PeerCacheOptions {
    pub directory: PathBuf,
    pub capacity_bytes: u64,
    pub namespace_id: NamespaceId,
}

/// One complete BLAKE3 verification against trusted Meta identity, borrowing
/// the caller's temporary body. Neither proof nor cache retains a body.
#[derive(Debug)]
pub struct VerifiedPeerChunk<'a> {
    chunk: &'a ChunkObject,
    bytes: &'a [u8],
    outboard: Option<SharedOutboard>,
    stream_origin: bool,
}

impl<'a> VerifiedPeerChunk<'a> {
    pub fn verify(
        chunk: &'a ChunkObject,
        bytes: &'a [u8],
        budget: Arc<OutboardBudget>,
    ) -> Result<Self> {
        validate_chunk(chunk)?;
        if bytes.len() as u64 != chunk.length {
            return Err(corrupt(
                "Peer cache fill differs from trusted object length",
            ));
        }
        if chunk.length > MAX_OBJECT_BYTES {
            if blake3::hash(bytes).as_bytes() != &chunk.content_digest.bytes {
                return Err(corrupt(
                    "Peer cache fill differs from trusted object identity",
                ));
            }
            return Ok(Self {
                chunk,
                bytes,
                outboard: None,
                stream_origin: false,
            });
        }
        let built = build_borrowed(bytes, &budget);
        if built.digest != chunk.content_digest {
            return Err(corrupt(
                "Peer cache fill differs from trusted object identity",
            ));
        }
        Ok(Self {
            chunk,
            bytes,
            outboard: built.outboard,
            stream_origin: false,
        })
    }

    pub(in crate::node) fn from_completed_stream(
        chunk: &'a ChunkObject,
        bytes: &'a [u8],
        proof: crate::node::rpc::peer::CompletedPeerObjectProof<'a>,
    ) -> Result<Self> {
        validate_chunk(chunk)?;
        if bytes.len() as u64 != chunk.length
            || !std::ptr::eq(bytes, proof.bytes())
            || proof.root_digest() != &chunk.content_digest.bytes
        {
            return Err(corrupt(
                "Peer completed stream proof differs from trusted object",
            ));
        }
        let outboard = proof.into_outboard();
        if let Some(proof) = &outboard {
            validate_outboard(chunk, proof)?;
        }
        Ok(Self {
            chunk,
            bytes,
            outboard,
            stream_origin: true,
        })
    }

    pub fn bytes(&self) -> &'a [u8] {
        self.bytes
    }
    pub fn chunk(&self) -> &'a ChunkObject {
        self.chunk
    }
    pub fn uses_completed_stream_proof(&self) -> bool {
        self.stream_origin
    }
}

/// Opaque per-fill binding. Only a sink can mint it; decoding carries it from
/// first byte through EOF so another same-object pending file cannot reuse proof.
#[derive(Clone, Debug)]
pub(in crate::node) struct PeerStreamBinding {
    identity: Arc<()>,
    pointer: usize,
    length: usize,
    offset: u64,
}

impl PeerStreamBinding {
    pub(in crate::node) fn matches_output(&self, offset: u64, output: &[u8]) -> bool {
        self.offset == offset
            && self.pointer == output.as_ptr() as usize
            && self.length == output.len()
    }
}

/// Complete stream receipt borrowing only the unpublished requested output.
/// The file proof is separately bound to its actual pending descriptor identity.
#[derive(Debug)]
pub struct CompletedPeerDiskRead<'output> {
    chunk: ChunkObject,
    requested_offset: u64,
    output: &'output [u8],
    root_digest: [u8; 32],
    outboard: Option<SharedOutboard>,
    file_identity: Option<FileIdentity>,
    binding: PeerStreamBinding,
}

impl<'output> CompletedPeerDiskRead<'output> {
    pub fn chunk(&self) -> &ChunkObject {
        &self.chunk
    }
    pub fn requested_offset(&self) -> u64 {
        self.requested_offset
    }
    pub fn requested_length(&self) -> usize {
        self.output.len()
    }
    pub fn root_digest(&self) -> &[u8; 32] {
        &self.root_digest
    }
    #[cfg(test)]
    pub fn private_output(&self) -> &'output [u8] {
        self.output
    }
    pub fn matches_private_output_identity(&self, pointer: usize, length: usize) -> bool {
        self.output.as_ptr() as usize == pointer && self.output.len() == length
    }
}

/// Private staging borrows the unique native fill allowance, not a table lock.
/// Cache failures disable this sink; they never stop the same RPC verification.
pub struct PeerStreamSink<'a> {
    cache: &'a DiskPeerCache,
    _fill: &'a FillPermit<'a>,
    chunk: ChunkObject,
    key: String,
    received: u64,
    header_length: u64,
    file: Option<File>,
    pending_identity: Option<(u64, u64)>,
    pending_generation: u64,
    owns_pending: bool,
    committed: bool,
    prepare_error: Option<Error>,
    allow_eviction: bool,
    entry_lease: Option<OutboardLease>,
    identity: Arc<()>,
    #[cfg(test)]
    write_delay: std::time::Duration,
}

impl PeerStreamSink<'_> {
    pub(crate) fn is_caching(&self) -> bool {
        self.file.is_some()
    }
    pub(in crate::node) fn proof_budget(&self) -> Arc<OutboardBudget> {
        self.cache.proof_budget()
    }

    pub(in crate::node) fn bind_output(
        &self,
        offset: u64,
        output: &[u8],
    ) -> Result<PeerStreamBinding> {
        if offset
            .checked_add(output.len() as u64)
            .is_none_or(|end| end > self.chunk.length)
        {
            return Err(invalid("Peer stream output binding exceeds object"));
        }
        Ok(PeerStreamBinding {
            identity: self.identity.clone(),
            pointer: output.as_ptr() as usize,
            length: output.len(),
            offset,
        })
    }

    pub(in crate::node) fn accepts_binding(&self, binding: &PeerStreamBinding) -> bool {
        Arc::ptr_eq(&self.identity, &binding.identity)
    }

    fn prepare(&mut self) -> Result<()> {
        self.cache.check_root()?;
        if self.allow_eviction {
            self.cache.cleanup_pending()?;
        }
        self.header_length = MAX_HEADER_BYTES as u64;
        let unit = self
            .cache
            .root
            .metadata()
            .map_err(Error::from)?
            .blksize()
            .max(512);
        let length = self.header_length + self.chunk.length + MAX_OUTBOARD_BYTES as u64;
        let reservation = length
            .checked_add(unit - 1)
            .and_then(|size| size.checked_div(unit))
            .and_then(|blocks| blocks.checked_add(2))
            .and_then(|blocks| blocks.checked_mul(unit))
            .ok_or_else(|| invalid("Peer stream reservation overflow"))?;
        let metadata_charge = entry_metadata_charge(&self.key, &self.chunk);
        self.entry_lease = if self.allow_eviction {
            self.cache.reserve_entry_metadata(metadata_charge)?
        } else {
            self.cache
                .proof_budget
                .try_reserve_resident(metadata_charge)
        };
        if self.entry_lease.is_none() {
            return Ok(());
        }
        if !self.cache.reserve_pending(
            &self.key,
            self.pending_generation,
            reservation,
            metadata_charge,
            &mut self.entry_lease,
            self.allow_eviction,
        )? {
            return Ok(());
        }
        let opened = OpenOptions::new()
            .write(true)
            .create_new(true)
            .mode(0o600)
            .custom_flags(libc::O_NOFOLLOW | libc::O_CLOEXEC)
            .open(self.cache.pending_path(self.pending_generation));
        let file = match opened {
            Ok(file) => file,
            Err(error) => {
                // No file was created by this attempt; retain any foreign file.
                let mut state = self.cache.lock_state()?;
                remove_pending_charge(&mut state, self.pending_generation);
                return Err(Error::from(error));
            }
        };
        self.file = Some(file);
        self.owns_pending = true;
        #[cfg(test)]
        if self.cache.lock_state()?.fail_stream_metadata {
            return Err(unavailable("injected pending descriptor metadata failure"));
        }
        let file = self
            .file
            .as_mut()
            .ok_or_else(|| unavailable("Peer stream file was not opened"))?;
        let metadata = file.metadata().map_err(Error::from)?;
        self.pending_identity = Some((metadata.dev(), metadata.ino()));
        if let Some(pending) = self
            .cache
            .lock_state()?
            .pending
            .get_mut(&self.pending_generation)
        {
            pending.identity = self.pending_identity;
        }
        write_header(
            file,
            &Header {
                version: 2,
                namespace_id: self.cache.options.namespace_id.clone(),
                chunk: self.chunk.clone(),
                outboard_length: 0,
            },
        )?;
        Ok(())
    }

    fn abort_pending(&mut self) -> Result<()> {
        // Pin the inode until identity checking and unlink finish, so close
        // cannot open an inode-reuse gap while deciding which path is owned.
        let _file = self.file.take();
        if !self.owns_pending {
            return Ok(());
        }
        self.cache.check_root()?;
        match fs::symlink_metadata(self.cache.pending_path(self.pending_generation)) {
            Ok(metadata) if Some((metadata.dev(), metadata.ino())) == self.pending_identity => {}
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => {}
            _ => {
                return Err(invalid(
                    "Peer stream cleanup refuses a replaced pending file",
                ));
            }
        }
        self.cache
            .clear_pending(self.pending_generation, self.pending_identity)?;
        self.owns_pending = false;
        Ok(())
    }

    fn disable(&mut self, error: &Error) {
        afs_logging::warn!("dfs.peer_stream_cache_disabled"; "error" => error.to_string());
        if let Err(cleanup) = self.abort_pending() {
            afs_logging::warn!("dfs.peer_stream_cache_cleanup_failed"; "error" => cleanup.to_string());
        }
    }

    #[cfg(test)]
    pub(in crate::node) fn delay_writes_for_test(&mut self, delay: std::time::Duration) {
        self.write_delay = delay;
    }

    pub(in crate::node) fn write(&mut self, offset: u64, bytes: &[u8]) -> Result<()> {
        if offset != self.received
            || offset
                .checked_add(bytes.len() as u64)
                .is_none_or(|end| end > self.chunk.length)
        {
            return Err(invalid("Peer stream sink range differs from the object"));
        }
        self.received += bytes.len() as u64;
        #[cfg(test)]
        std::thread::sleep(self.write_delay);
        #[cfg(test)]
        if self.cache.lock_state()?.fail_stream_write {
            self.disable(&unavailable("injected stream cache write failure"));
        }
        if let Some(file) = &mut self.file
            && let Err(error) = file.write_all(bytes)
        {
            self.disable(&Error::from(error));
        }
        Ok(())
    }

    pub(in crate::node) fn seal_completed<'output>(
        &mut self,
        proof: crate::node::rpc::peer::CompletedPeerStreamProof,
        requested_offset: u64,
        output: &'output [u8],
    ) -> Result<CompletedPeerDiskRead<'output>> {
        if !self.accepts_binding(proof.binding())
            || !proof.binding().matches_output(requested_offset, output)
            || proof.chunk() != &self.chunk
            || self.received != self.chunk.length
            || proof.root_digest() != &self.chunk.content_digest.bytes
            || requested_offset
                .checked_add(output.len() as u64)
                .is_none_or(|end| end > self.chunk.length)
        {
            return Err(corrupt(
                "Peer stream completion differs from the sink object",
            ));
        }
        let binding = proof.binding().clone();
        let root_digest = *proof.root_digest();
        let outboard = proof
            .into_outboard()
            .filter(|proof| proof.uses_budget(&self.cache.proof_budget));
        if let Some(proof) = &outboard {
            validate_outboard(&self.chunk, proof)?;
        }
        let checked = (|| {
            let Some(file) = &mut self.file else {
                return Ok(None);
            };
            let trailer = outboard.as_ref().map_or(&[][..], |proof| proof.encoded());
            file.write_all(trailer).map_err(Error::from)?;
            file.seek(SeekFrom::Start(0)).map_err(Error::from)?;
            write_header(
                file,
                &Header {
                    version: 2,
                    namespace_id: self.cache.options.namespace_id.clone(),
                    chunk: self.chunk.clone(),
                    outboard_length: trailer.len() as u32,
                },
            )?;
            file.flush().map_err(Error::from)?;
            let metadata = file.metadata().map_err(Error::from)?;
            validate_private_file(&metadata)?;
            self.cache.check_root()?;
            let current = fs::symlink_metadata(self.cache.pending_path(self.pending_generation))
                .map_err(Error::from)?;
            if metadata.len()
                != self.header_length
                    + self.chunk.length
                    + outboard
                        .as_ref()
                        .map_or(0, |proof| proof.encoded().len() as u64)
                || Some((metadata.dev(), metadata.ino())) != self.pending_identity
                || file_identity(&metadata) != file_identity(&current)
            {
                return Err(corrupt(
                    "Peer stream pending descriptor changed before sealing",
                ));
            }
            let charge = allocated(&metadata)?;
            if self
                .cache
                .account_pending_growth(self.pending_generation, charge)?
            {
                return Err(unavailable(
                    "Peer stream allocation exceeds reservation before seal",
                ));
            }
            Ok(Some(file_identity(&metadata)))
        })();
        let file_identity = match checked {
            Ok(identity) => identity,
            Err(error) => {
                self.disable(&error);
                None
            }
        };
        Ok(CompletedPeerDiskRead {
            chunk: self.chunk.clone(),
            requested_offset,
            output,
            root_digest,
            outboard,
            file_identity,
            binding,
        })
    }

    pub fn commit(&mut self, completed: CompletedPeerDiskRead<'_>) -> Result<bool> {
        if !self.accepts_binding(&completed.binding)
            || completed.chunk != self.chunk
            || completed.root_digest != self.chunk.content_digest.bytes
        {
            return Err(corrupt("Peer disk completion belongs to another object"));
        }
        let Some(file) = &self.file else {
            return Ok(false);
        };
        self.cache.check_root()?;
        let metadata = file.metadata().map_err(Error::from)?;
        validate_private_file(&metadata)?;
        let current = fs::symlink_metadata(self.cache.pending_path(self.pending_generation))
            .map_err(Error::from)?;
        if completed.file_identity != Some(file_identity(&metadata))
            || file_identity(&metadata) != file_identity(&current)
        {
            return Err(corrupt(
                "Peer disk completion does not bind this pending descriptor",
            ));
        }
        let charge = allocated(&metadata)?;
        if self
            .cache
            .account_pending_growth(self.pending_generation, charge)?
        {
            return Err(unavailable(
                "Peer stream allocation exceeds reserved blocks",
            ));
        }
        match fs::symlink_metadata(self.cache.path(&self.key)) {
            Ok(_) => return Err(invalid("Peer stream publication refuses an untracked file")),
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => {}
            Err(error) => return Err(Error::from(error)),
        }
        fs::rename(
            self.cache.pending_path(self.pending_generation),
            self.cache.path(&self.key),
        )
        .map_err(Error::from)?;
        self.committed = true;
        self.owns_pending = false;
        let published = file
            .metadata()
            .ok()
            .map(|metadata| file_identity(&metadata));
        self.cache.publish_entry(
            (self.pending_generation, self.key.clone()),
            self.chunk.clone(),
            charge,
            published.unwrap_or_else(|| file_identity(&metadata)),
            if published.is_some() {
                completed.outboard
            } else {
                None
            },
            self.entry_lease.take(),
        )?;
        Ok(true)
    }
}

impl Drop for PeerStreamSink<'_> {
    fn drop(&mut self) {
        if !self.committed
            && let Err(error) = self.abort_pending()
        {
            afs_logging::warn!("dfs.peer_stream_drop_cleanup_failed"; "error" => error.to_string());
        }
        drop(self.entry_lease.take());
        if let Ok(mut state) = self.cache.state.lock()
            && let Some(pending) = state.pending.get_mut(&self.pending_generation)
        {
            pending.active = false;
        }
        self.cache.report_resources();
    }
}

type FileIdentity = (u64, u64, u64, i64, i64, i64, i64);

#[derive(Debug)]
struct VerifiedDiskOutboard {
    identity: FileIdentity,
    outboard: SharedOutboard,
    // The Arc owns encoded storage; this lease charges only our binding wrapper.
    _lease: OutboardLease,
}

#[derive(Clone, Copy, Debug, Default)]
pub struct PeerCacheHashWork {
    pub cache_segment_bytes: u64,
    pub cache_fullscan_bytes: u64,
    pub segment_index_bytes: u64,
    /// Metadata CV inputs hashed while authenticating covered groups.
    pub outboard_parent_bytes: u64,
}

/// Low-frequency aggregate wall time inside read_range only. Admission lock,
/// operation root checks, Meta, final engine authority and metric emission are
/// outside these boundaries. `stream_read_verify_ns` is the inclusive local
/// operation; data_read/segment_verify are child intervals and must not be
/// added to it. Complete-root fallback cannot separate read and hash.
#[derive(Clone, Copy, Debug, Default)]
pub struct PeerCacheReadStages {
    pub prepare_ns: u64,
    pub data_read_ns: u64,
    pub segment_verify_ns: u64,
    pub stream_read_verify_ns: u64,
    pub finalize_ns: u64,
}

#[derive(Clone, Debug, Default)]
pub struct PeerCacheStats {
    pub hits: u64,
    pub misses: u64,
    pub corruptions: u64,
    pub evictions: u64,
    pub hash_bytes_total: u64,
    /// Actual body-read attempts, excluding header and metadata reads.
    pub data_read_calls: u64,
    /// Explicit local cache body copies, including Bao encode/decode and
    /// final private-output copies, including failed reads. The historical
    /// metric label is `cache_to_output`; it is not just final delivery bytes.
    pub copied_bytes_total: u64,
    pub disk_bytes: u64,
    /// Peak charged ready and temporary blocks, including conservative reservations.
    pub disk_peak_bytes: u64,
    pub entries: usize,
    pub scratch_peak_bytes: u64,
    pub proof_metadata_bytes: u64,
    pub proof_metadata_peak_bytes: u64,
    pub hash_work: PeerCacheHashWork,
    pub read_stages: PeerCacheReadStages,
}

#[derive(Debug)]
pub struct DiskPeerCache {
    options: PeerCacheOptions,
    root: File,
    _marker: File,
    started: Instant,
    state: Mutex<CacheState>,
    changed: Condvar,
    asynchronous_change: tokio::sync::Notify,
    max_flights: usize,
    read_work: Option<super::read_work::ReadWorkMetrics>,
    proof_budget: Arc<OutboardBudget>,
}

#[derive(Debug)]
struct CacheState {
    budget: Arc<OutboardBudget>,
    closed: bool,
    entries: BTreeMap<String, Entry>,
    retired: BTreeMap<String, RetiredEntry>,
    fill_active: bool,
    stream_active: usize,
    foreground_waiters: usize,
    next_pending_generation: u64,
    pending: BTreeMap<u64, PendingEntry>,
    operations: usize,
    flights: BTreeMap<String, Arc<FillFlight>>,
    flight_live: usize,
    deleting_slots: usize,
    local_index_available: bool,
    // Rust drops fields in declaration order: entries and their proof wrappers
    // must release before the reservation covering their pointer slots.
    index_lease: Option<OutboardLease>,
    used_bytes: u64,
    pending_charge: u64,
    pending_metadata_charge: usize,
    operation_metadata_bytes: usize,
    entry_metadata_bytes: usize,
    retired_disk_bytes: u64,
    flight_metadata_bytes: usize,
    stats: PeerCacheStats,
    #[cfg(test)]
    fail_remove: bool,
    #[cfg(test)]
    fail_stream_write: bool,
    #[cfg(test)]
    fail_stream_metadata: bool,
    #[cfg(test)]
    proof_limit: Option<u64>,
}

#[derive(Debug)]
struct Entry {
    chunk: ChunkObject,
    charge: u64,
    identity: (u64, u64),
    touched: u64,
    proof: Option<Arc<VerifiedDiskOutboard>>,
    generation: Arc<()>,
    _metadata: Option<OutboardLease>,
    #[cfg(test)]
    drop_observation: Option<DropObservation>,
}

#[cfg(test)]
#[derive(Debug)]
struct DropObservation {
    budget: Arc<OutboardBudget>,
    observed: Arc<std::sync::atomic::AtomicUsize>,
}
#[cfg(test)]
impl Drop for DropObservation {
    fn drop(&mut self) {
        self.observed
            .store(self.budget.used(), std::sync::atomic::Ordering::SeqCst);
    }
}

#[derive(Serialize, Deserialize)]
struct Header {
    version: u32,
    namespace_id: NamespaceId,
    chunk: ChunkObject,
    #[serde(default)]
    outboard_length: u32,
}

pub struct PeerCacheOperation<'a> {
    cache: &'a DiskPeerCache,
    // Per-operation work only, never a MutexGuard or a body allocation.
    state: CacheState,
    fill: Option<FillPermit<'a>>,
    _metadata: Option<OutboardLease>,
}

#[derive(Debug)]
struct RetiredEntry {
    entry: Entry,
    deleting: bool,
}

struct EntryPin<'a> {
    cache: &'a DiskPeerCache,
    generation: Option<Arc<()>>,
}

struct FillPermit<'a> {
    cache: &'a DiskPeerCache,
    streaming: bool,
}

#[derive(Debug)]
struct PendingEntry {
    charge: u64,
    identity: Option<(u64, u64)>,
    active: bool,
    metadata_charge: usize,
    _metadata: Option<OutboardLease>,
}

fn remove_pending_charge(state: &mut CacheState, generation: u64) -> Option<PendingEntry> {
    let pending = state.pending.remove(&generation)?;
    state.used_bytes = state.used_bytes.saturating_sub(pending.charge);
    state.pending_charge = state.pending_charge.saturating_sub(pending.charge);
    state.pending_metadata_charge = state
        .pending_metadata_charge
        .saturating_sub(pending.metadata_charge);
    Some(pending)
}

#[derive(Clone, Debug)]
pub(crate) enum FillOutcome {
    Ready,
    Uncached,
    Failed(Error),
}

#[derive(Debug)]
struct FillFlight {
    result: Mutex<Option<FillOutcome>>,
    changed: Condvar,
    asynchronous_change: tokio::sync::Notify,
    _metadata: OutboardLease,
}

pub(crate) struct FillLeader<'a> {
    cache: &'a DiskPeerCache,
    key: String,
    flight: Option<Arc<FillFlight>>,
    completed: bool,
}

pub(crate) struct OwnedFillLeader {
    cache: Arc<DiskPeerCache>,
    key: String,
    flight: Option<Arc<FillFlight>>,
    completed: bool,
}

pub(crate) enum FillRole<'a> {
    Leader(FillLeader<'a>),
    Finished(FillOutcome),
}

impl CacheState {
    fn new_local(budget: Arc<OutboardBudget>) -> Self {
        let mut state = Self::new_without_index(budget);
        state.local_index_available = true;
        state
    }

    fn new_without_index(budget: Arc<OutboardBudget>) -> Self {
        Self {
            budget,
            closed: false,
            entries: BTreeMap::new(),
            retired: BTreeMap::new(),
            fill_active: false,
            stream_active: 0,
            foreground_waiters: 0,
            next_pending_generation: 1,
            pending: BTreeMap::new(),
            operations: 0,
            flights: BTreeMap::new(),
            flight_live: 0,
            deleting_slots: 0,
            local_index_available: false,
            index_lease: None,
            used_bytes: 0,
            pending_charge: 0,
            pending_metadata_charge: 0,
            operation_metadata_bytes: 0,
            entry_metadata_bytes: 0,
            retired_disk_bytes: 0,
            flight_metadata_bytes: 0,
            stats: PeerCacheStats::default(),
            #[cfg(test)]
            fail_remove: false,
            #[cfg(test)]
            fail_stream_write: false,
            #[cfg(test)]
            fail_stream_metadata: false,
            #[cfg(test)]
            proof_limit: None,
        }
    }

    fn new(budget: Arc<OutboardBudget>) -> Self {
        // All optional proof slots are bounded up front. A small shared budget
        // can disable proof acceleration without disabling complete verification.
        let index_lease = budget.try_reserve_resident(
            MAX_PEER_CACHE_ENTRIES * std::mem::size_of::<Option<Box<VerifiedDiskOutboard>>>(),
        );
        Self {
            budget,
            index_lease,
            closed: false,
            entries: BTreeMap::new(),
            retired: BTreeMap::new(),
            fill_active: false,
            stream_active: 0,
            foreground_waiters: 0,
            next_pending_generation: 1,
            pending: BTreeMap::new(),
            operations: 0,
            flights: BTreeMap::new(),
            flight_live: 0,
            deleting_slots: 0,
            local_index_available: false,
            used_bytes: 0,
            pending_charge: 0,
            pending_metadata_charge: 0,
            operation_metadata_bytes: 0,
            entry_metadata_bytes: 0,
            retired_disk_bytes: 0,
            flight_metadata_bytes: 0,
            stats: PeerCacheStats::default(),
            #[cfg(test)]
            fail_remove: false,
            #[cfg(test)]
            fail_stream_write: false,
            #[cfg(test)]
            fail_stream_metadata: false,
            #[cfg(test)]
            proof_limit: None,
        }
    }
}

impl DiskPeerCache {
    pub fn open(options: PeerCacheOptions) -> Result<Self> {
        Self::open_with_outboard_budget(
            options,
            OutboardBudget::new(MAX_PEER_CACHE_PROOF_BYTES as usize),
        )
    }

    pub(crate) fn open_with_outboard_budget(
        options: PeerCacheOptions,
        proof_budget: Arc<OutboardBudget>,
    ) -> Result<Self> {
        if !options.directory.is_absolute()
            || options.capacity_bytes == 0
            || options.namespace_id.0.is_empty()
            || options.namespace_id.0.len() > 512
        {
            return Err(invalid(
                "Peer cache requires an absolute directory and positive budget",
            ));
        }
        reject_symlink_components(&options.directory)?;
        DirBuilder::new()
            .recursive(true)
            .mode(0o700)
            .create(&options.directory)
            .map_err(Error::from)?;
        reject_symlink_components(&options.directory)?;
        let root = OpenOptions::new()
            .read(true)
            .custom_flags(libc::O_NOFOLLOW | libc::O_DIRECTORY | libc::O_CLOEXEC)
            .open(&options.directory)
            .map_err(Error::from)?;
        let metadata = root.metadata().map_err(Error::from)?;
        if !metadata.is_dir() || metadata.mode() & 0o077 != 0 || metadata.uid() != effective_uid() {
            return Err(invalid(
                "Peer cache directory must be private and owned by this user",
            ));
        }
        reject_symlink_components(&options.directory)?;
        let current = fs::symlink_metadata(&options.directory).map_err(Error::from)?;
        if (current.dev(), current.ino()) != (metadata.dev(), metadata.ino()) {
            return Err(invalid("Peer cache directory changed during open"));
        }
        // All child operations resolve through the pinned Linux directory FD,
        // never through a configured pathname that can be replaced later.
        let directory = fd_directory(&root);
        let marker_path = directory.join(MARKER);
        let mut marker = match OpenOptions::new()
            .read(true)
            .write(true)
            .create_new(true)
            .mode(0o600)
            .custom_flags(libc::O_NOFOLLOW | libc::O_CLOEXEC)
            .open(&marker_path)
        {
            Ok(mut file) => {
                if fs::read_dir(&directory).map_err(Error::from)?.any(|entry| {
                    entry
                        .map(|entry| entry.file_name() != MARKER)
                        .unwrap_or(true)
                }) {
                    drop(file);
                    fs::remove_file(marker_path).map_err(Error::from)?;
                    return Err(invalid(
                        "Peer cache refuses an unmarked non-empty directory",
                    ));
                }
                file.write_all(b"AFS disposable Peer cache v1\n")
                    .map_err(Error::from)?;
                file.sync_all().map_err(Error::from)?;
                file
            }
            Err(error) if error.kind() == std::io::ErrorKind::AlreadyExists => OpenOptions::new()
                .read(true)
                .write(true)
                .custom_flags(libc::O_NOFOLLOW | libc::O_CLOEXEC | libc::O_NONBLOCK)
                .open(marker_path)
                .map_err(Error::from)?,
            Err(error) => return Err(Error::from(error)),
        };
        validate_private_file(&marker.metadata().map_err(Error::from)?)?;
        // SAFETY: the descriptor belongs to marker; flock does not own it.
        if unsafe { libc::flock(marker.as_raw_fd(), libc::LOCK_EX | libc::LOCK_NB) } != 0 {
            return Err(unavailable("Peer cache directory is already in use"));
        }
        marker.seek(SeekFrom::Start(0)).map_err(Error::from)?;
        let mut contents = String::new();
        Read::by_ref(&mut marker)
            .take(128)
            .read_to_string(&mut contents)
            .map_err(Error::from)?;
        if contents != "AFS disposable Peer cache v1\n" {
            return Err(invalid("Peer cache marker is not recognized"));
        }
        let cache = Self {
            options,
            root,
            _marker: marker,
            started: Instant::now(),
            state: Mutex::new(CacheState::new(proof_budget.clone())),
            changed: Condvar::new(),
            asynchronous_change: tokio::sync::Notify::new(),
            max_flights: 8,
            proof_budget,
            read_work: None,
        };
        cache.recover()?;
        Ok(cache)
    }

    pub(crate) fn max_inflight_objects(&self) -> usize {
        self.max_flights
    }

    pub(crate) fn try_contains_ready(&self, chunk: &ChunkObject) -> Result<Option<bool>> {
        let key = self.key(chunk)?;
        Ok(self
            .try_lock_state()?
            .map(|state| state.entries.contains_key(&key)))
    }

    pub(crate) fn proof_budget(&self) -> Arc<OutboardBudget> {
        self.proof_budget.clone()
    }

    pub fn namespace_id(&self) -> &NamespaceId {
        &self.options.namespace_id
    }

    pub fn with_read_work_metrics(mut self, metrics: super::read_work::ReadWorkMetrics) -> Self {
        if let Ok(state) = self.state.lock() {
            report_work(
                &metrics,
                state.stats.hash_work,
                PeerCacheHashWork::default(),
            );
            metrics.peer_cache_data_reads(state.stats.data_read_calls);
            metrics.copy("cache_to_output", state.stats.copied_bytes_total);
            report_stages(
                &metrics,
                state.stats.read_stages,
                PeerCacheReadStages::default(),
            );
            metrics.peer_cache_metadata(
                state.stats.proof_metadata_bytes,
                state.stats.proof_metadata_peak_bytes,
            );
        }
        self.read_work = Some(metrics);
        self
    }

    pub(crate) fn with_max_inflight_objects(mut self, maximum: usize) -> Result<Self> {
        if !(1..=8).contains(&maximum) {
            return Err(invalid(
                "Peer cache metadata flights must be between one and eight",
            ));
        }
        self.max_flights = maximum;
        Ok(self)
    }

    pub fn operation(&self) -> Result<PeerCacheOperation<'_>> {
        self.operation_mode(false)?
            .ok_or_else(|| unavailable("Peer cache operation unavailable"))
    }

    pub(crate) fn try_operation(&self) -> Result<Option<PeerCacheOperation<'_>>> {
        self.operation_mode(true)
    }

    fn operation_mode(&self, background: bool) -> Result<Option<PeerCacheOperation<'_>>> {
        self.check_root()?;
        let wait = self
            .read_work
            .as_ref()
            .and_then(|metrics| metrics.coarse_diagnostic("cache_lock_wait", 0));
        let mut state = if background {
            let Some(state) = self.try_lock_state()? else {
                return Ok(None);
            };
            state
        } else {
            self.lock_state()?
        };
        drop(wait);
        // Bound operation-local entry/proof snapshots independently of workers.
        if state.closed || state.operations >= 128 {
            if background {
                return Ok(None);
            }
            return Err(unavailable(
                "Peer cache is stopped or operation slots are full",
            ));
        }
        let metadata = self.proof_budget.try_reserve(8192);
        if background && (metadata.is_none() || state.foreground_waiters != 0) {
            return Ok(None);
        }
        state.operations += 1;
        if metadata.is_some() {
            state.operation_metadata_bytes += 8192;
        }
        let mut local = CacheState::new_local(self.proof_budget());
        local.local_index_available = state.index_lease.is_some();
        #[cfg(test)]
        {
            local.proof_limit = state.proof_limit;
        }
        drop(state);
        if !background {
            self.report_resources();
        }
        Ok(Some(PeerCacheOperation {
            cache: self,
            state: local,
            fill: None,
            _metadata: metadata,
        }))
    }

    pub(crate) fn close_admission(&self) {
        if let Ok(mut state) = self.state.lock() {
            state.closed = true;
            self.changed.notify_all();
            self.asynchronous_change.notify_waiters();
            for flight in state.flights.values() {
                flight.changed.notify_all();
                flight.asynchronous_change.notify_waiters();
            }
        }
    }

    pub fn shutdown(&self) -> Result<()> {
        self.close_admission();
        let deadline = Instant::now() + Duration::from_secs(10);
        let mut state = self
            .state
            .lock()
            .map_err(|_| unavailable("Peer cache lock poisoned"))?;
        while state.operations != 0
            || state.fill_active
            || state.stream_active != 0
            || state.flight_live != 0
            || state.deleting_slots != 0
            || state.retired.values().any(|retired| retired.deleting)
        {
            let remaining = deadline.saturating_duration_since(Instant::now());
            if remaining.is_zero() {
                return Err(unavailable("Peer cache drain deadline exceeded"));
            }
            state = self
                .changed
                .wait_timeout(state, remaining)
                .map_err(|_| unavailable("Peer cache drain lock poisoned"))?
                .0;
        }
        // Preserve healthy disk entries for startup recovery. Charges and proof
        // owners leave the lock before final destruction.
        let entries = std::mem::take(&mut state.entries);
        let released_entry_metadata: usize = entries
            .iter()
            .map(|(key, entry)| entry_metadata_charge(key, &entry.chunk))
            .sum();
        state.entry_metadata_bytes = state
            .entry_metadata_bytes
            .saturating_sub(released_entry_metadata);
        refresh_metadata(&mut state);
        drop(state);
        drop(entries);
        self.cleanup_retired()?;
        let index = self.lock_state()?.index_lease.take();
        drop(index);
        self.report_resources();
        Ok(())
    }

    pub fn stats(&self) -> Result<PeerCacheStats> {
        let state = self
            .state
            .lock()
            .map_err(|_| unavailable("Peer cache lock poisoned"))?;
        let mut stats = state.stats.clone();
        stats.disk_bytes = state.used_bytes;
        stats.entries = state.entries.len();
        Ok(stats)
    }

    fn check_root(&self) -> Result<()> {
        reject_symlink_components(&self.options.directory)?;
        let current = fs::symlink_metadata(&self.options.directory).map_err(Error::from)?;
        let pinned = self.root.metadata().map_err(Error::from)?;
        if !current.is_dir()
            || current.mode() & 0o077 != 0
            || current.uid() != effective_uid()
            || pinned.mode() & 0o077 != 0
            || pinned.uid() != effective_uid()
            || (current.dev(), current.ino()) != (pinned.dev(), pinned.ino())
        {
            return Err(invalid("Peer cache root identity changed"));
        }
        Ok(())
    }

    fn key(&self, chunk: &ChunkObject) -> Result<String> {
        let input = serde_json::to_vec(&(&self.options.namespace_id, chunk))
            .map_err(|_| invalid("Peer cache object identity cannot be encoded"))?;
        Ok(blake3::hash(&input).to_hex().to_string())
    }

    fn directory(&self) -> PathBuf {
        fd_directory(&self.root)
    }

    fn bucket(&self) -> u64 {
        self.started.elapsed().as_secs()
    }

    fn pending_path(&self, generation: u64) -> PathBuf {
        self.directory()
            .join(format!("{PENDING}-{generation:016x}"))
    }

    fn path(&self, key: &str) -> PathBuf {
        self.directory().join(format!("{key}.ready"))
    }

    fn recover(&self) -> Result<()> {
        let mut state = self
            .state
            .lock()
            .map_err(|_| unavailable("Peer cache lock poisoned"))?;
        // Preflight bounded names and allocated space before any private cleanup.
        let mut ready = Vec::new();
        let mut pending = Vec::new();
        let mut total = 0_u64;
        for entry in fs::read_dir(self.directory()).map_err(Error::from)? {
            let entry = entry.map_err(Error::from)?;
            let name = entry.file_name();
            if name == MARKER {
                continue;
            }
            let name = name
                .to_str()
                .ok_or_else(|| invalid("Peer cache contains a foreign name"))?;
            let metadata = fs::symlink_metadata(entry.path()).map_err(Error::from)?;
            validate_private_file(&metadata)?;
            let charge = allocated(&metadata)?;
            total = total
                .checked_add(charge)
                .ok_or_else(|| invalid("Peer cache size overflow"))?;
            if name == PENDING || valid_pending_name(name) {
                pending.push((entry.path(), charge));
            } else if valid_ready_name(name) {
                ready.push((name.trim_end_matches(".ready").to_owned(), metadata));
            } else {
                return Err(invalid(
                    "Peer cache contains foreign files; preserved without cleanup",
                ));
            }
            if ready.len() + pending.len() > MAX_PEER_CACHE_ENTRIES
                || total > self.options.capacity_bytes
            {
                return Err(invalid("Peer cache recovery exceeds disk or index budget"));
            }
        }
        state.used_bytes = total;
        state.stats.disk_peak_bytes = state.stats.disk_peak_bytes.max(total);
        for (path, charge) in pending {
            // Startup is exclusive; every accepted private file was preflighted.
            fs::remove_file(path).map_err(Error::from)?;
            state.used_bytes = state.used_bytes.saturating_sub(charge);
        }
        for (key, metadata) in ready {
            let (mut file, _) = open_private(&self.path(&key))?;
            let (header, body_start) = read_header(&mut file)?;
            validate_chunk(&header.chunk)?;
            if !matches!(header.version, 1 | 2)
                || header.namespace_id != self.options.namespace_id
                || self.key(&header.chunk)? != key
                || header.chunk.length > MAX_OBJECT_BYTES
            {
                return Err(invalid("Peer cache ready identity is foreign; preserved"));
            }
            // Recovery is single-threaded before this cache is published.
            // Make room by retiring optional summaries, never healthy bodies.
            let metadata_lease = loop {
                if let Some(lease) = self
                    .proof_budget
                    .try_reserve_resident(entry_metadata_charge(&key, &header.chunk))
                {
                    break lease;
                }
                let Some(entry) = state
                    .entries
                    .values_mut()
                    .filter(|entry| entry.proof.is_some())
                    .min_by_key(|entry| entry.touched)
                else {
                    return Err(unavailable(
                        "Peer cache recovery metadata capacity exceeded",
                    ));
                };
                entry.proof = None;
                refresh_metadata(&mut state);
            };
            state.entries.insert(
                key.clone(),
                Entry {
                    chunk: header.chunk.clone(),
                    charge: allocated(&metadata)?,
                    identity: (metadata.dev(), metadata.ino()),
                    touched: 0,
                    proof: None,
                    generation: Arc::new(()),
                    _metadata: Some(metadata_lease),
                    #[cfg(test)]
                    drop_observation: None,
                },
            );
            state.entry_metadata_bytes += entry_metadata_charge(&key, &header.chunk);
            refresh_metadata(&mut state);
            if header.version == 1 {
                // This directory is explicitly disposable. Retire only recognized
                // old cache objects; authoritative LocalChunkStore is elsewhere.
                drop(file);
                self.bootstrap_remove_entry(&mut state, &key, false)?;
                continue;
            }
            let before = file.metadata().map_err(Error::from)?;
            let result = (|| {
                validate_file_length(&header, &before)?;
                let index = proof_can_fit(&state);
                let peer = if index {
                    read_disk_outboard(&mut file, &header, &state.budget)?
                } else {
                    None
                };
                scan_body(
                    &mut file,
                    body_start,
                    &header.chunk,
                    0,
                    &mut [],
                    &mut state.stats,
                    index.then(|| (self.proof_budget(), peer)),
                )
            })();
            match result {
                Ok(outboard) => {
                    let after = file.metadata().map_err(Error::from)?;
                    let current = fs::symlink_metadata(self.path(&key)).map_err(Error::from)?;
                    if file_identity(&before) != file_identity(&after)
                        || file_identity(&after) != file_identity(&current)
                    {
                        return Err(corrupt("Peer cache file changed during recovery"));
                    }
                    install_proof(&mut state, &key, file_identity(&after), outboard);
                }
                Err(_) => {
                    state.stats.corruptions = state.stats.corruptions.saturating_add(1);
                    drop(file);
                    self.bootstrap_remove_entry(&mut state, &key, false)?;
                }
            }
        }
        Ok(())
    }

    fn bootstrap_remove_entry(
        &self,
        state: &mut CacheState,
        key: &str,
        eviction: bool,
    ) -> Result<()> {
        self.check_root()?;
        let Some(entry) = state.entries.get(key) else {
            return Ok(());
        };
        #[cfg(test)]
        if state.fail_remove {
            return Err(unavailable("injected Peer cache cleanup failure"));
        }
        match fs::symlink_metadata(self.path(key)) {
            Ok(metadata) => {
                validate_private_file(&metadata)?;
                if (metadata.dev(), metadata.ino()) != entry.identity {
                    return Err(invalid("Peer cache cleanup refuses a replaced file"));
                }
                fs::remove_file(self.path(key)).map_err(Error::from)?;
            }
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => {}
            Err(error) => return Err(Error::from(error)),
        }
        let charge = entry.charge;
        let metadata_charge = entry_metadata_charge(key, &entry.chunk);
        state.entry_metadata_bytes = state.entry_metadata_bytes.saturating_sub(metadata_charge);
        state.entries.remove(key);
        refresh_metadata(state);
        state.used_bytes = state.used_bytes.saturating_sub(charge);
        if eviction {
            state.stats.evictions = state.stats.evictions.saturating_add(1);
        }
        Ok(())
    }
}

impl DiskPeerCache {
    fn report_resources(&self) {
        let Some(metrics) = &self.read_work else {
            return;
        };
        let Ok(state) = self.state.lock() else {
            return;
        };
        let index = if state.index_lease.is_some() {
            MAX_PEER_CACHE_ENTRIES * std::mem::size_of::<Option<Arc<VerifiedDiskOutboard>>>()
        } else {
            0
        };
        // Fixed pre-registered in-memory gauges publish under the short metadata
        // lock: no stale snapshot can overwrite a newer resource observation.
        metrics.peer_cache_metadata(
            state.stats.proof_metadata_bytes,
            state.stats.proof_metadata_peak_bytes,
        );
        metrics.peer_cache_resources(&[
            ("operations", state.operations as u64),
            ("flights", state.flight_live as u64),
            (
                "fill_body_bytes",
                if state.fill_active || state.stream_active != 0 {
                    MAX_OBJECT_BYTES
                } else {
                    0
                },
            ),
            ("indexed_entries", state.entries.len() as u64),
            (
                "retired_entries",
                (state.retired.len() + state.deleting_slots) as u64,
            ),
            ("retired_disk_bytes", state.retired_disk_bytes),
            (
                "control_bytes",
                (state.entry_metadata_bytes
                    + state.flight_metadata_bytes
                    + state.operation_metadata_bytes
                    + state.pending_metadata_charge
                    + index) as u64,
            ),
        ]);
    }

    #[cfg(test)]
    pub(crate) fn has_fill_flight_for_test(&self) -> bool {
        !self.state.lock().unwrap().flights.is_empty()
    }

    #[cfg(test)]
    pub(crate) fn waiting_fill_readers_for_test(&self) -> usize {
        self.state
            .lock()
            .unwrap()
            .flights
            .values()
            .map(|flight| Arc::strong_count(flight).saturating_sub(2))
            .sum()
    }

    fn lock_state(&self) -> Result<std::sync::MutexGuard<'_, CacheState>> {
        self.state
            .lock()
            .map_err(|_| unavailable("Peer cache lock poisoned"))
    }

    fn try_lock_state(&self) -> Result<Option<std::sync::MutexGuard<'_, CacheState>>> {
        match self.state.try_lock() {
            Ok(state) => Ok(Some(state)),
            Err(std::sync::TryLockError::WouldBlock) => Ok(None),
            Err(std::sync::TryLockError::Poisoned(_)) => {
                Err(unavailable("Peer cache lock poisoned"))
            }
        }
    }

    fn cleanup_retired(&self) -> Result<()> {
        loop {
            let ticket = {
                let mut state = self.lock_state()?;
                let Some((key, retired)) = state.retired.iter_mut().find(|(_, retired)| {
                    !retired.deleting && Arc::strong_count(&retired.entry.generation) == 1
                }) else {
                    return Ok(());
                };
                retired.deleting = true;
                (
                    key.clone(),
                    retired.entry.generation.clone(),
                    retired.entry.identity,
                )
            };
            let (key, generation, identity) = ticket;
            let result = (|| {
                self.check_root()?;
                #[cfg(test)]
                if self.lock_state()?.fail_remove {
                    return Err(unavailable("injected Peer cache cleanup failure"));
                }
                match fs::symlink_metadata(self.path(&key)) {
                    Ok(metadata) => {
                        validate_private_file(&metadata)?;
                        if (metadata.dev(), metadata.ino()) != identity {
                            return Err(invalid("Peer cache cleanup refuses a replaced file"));
                        }
                        fs::remove_file(self.path(&key)).map_err(Error::from)
                    }
                    Err(error) if error.kind() == std::io::ErrorKind::NotFound => Ok(()),
                    Err(error) => Err(Error::from(error)),
                }
            })();
            let removed = {
                let mut state = self.lock_state()?;
                if let Some(retired) = state.retired.get_mut(&key)
                    && Arc::ptr_eq(&generation, &retired.entry.generation)
                {
                    retired.deleting = false;
                    if result.is_ok() {
                        let removed = state.retired.remove(&key);
                        if let Some(retired) = &removed {
                            state.used_bytes =
                                state.used_bytes.saturating_sub(retired.entry.charge);
                            state.retired_disk_bytes = state
                                .retired_disk_bytes
                                .saturating_sub(retired.entry.charge);
                            state.deleting_slots += 1;
                        }
                        refresh_metadata(&mut state);
                        removed
                    } else {
                        None
                    }
                } else {
                    None
                }
            };
            let metadata_charge = removed
                .as_ref()
                .map(|retired| entry_metadata_charge(&key, &retired.entry.chunk));
            drop(removed);
            if let Some(charge) = metadata_charge {
                let mut state = self.lock_state()?;
                state.entry_metadata_bytes = state.entry_metadata_bytes.saturating_sub(charge);
                state.deleting_slots = state.deleting_slots.saturating_sub(1);
            }
            self.changed.notify_all();
            self.asynchronous_change.notify_waiters();
            self.report_resources();
            result?;
        }
    }

    fn retire(&self, key: &str, generation: Option<&Arc<()>>, eviction: bool) -> Result<()> {
        {
            let mut state = self.lock_state()?;
            let eligible = state.entries.get(key).is_some_and(|entry| {
                generation.is_none_or(|generation| Arc::ptr_eq(generation, &entry.generation))
            });
            if eligible && let Some(entry) = state.entries.remove(key) {
                state.retired_disk_bytes = state.retired_disk_bytes.saturating_add(entry.charge);
                state.retired.insert(
                    key.to_owned(),
                    RetiredEntry {
                        entry,
                        deleting: false,
                    },
                );
                if eviction {
                    state.stats.evictions = state.stats.evictions.saturating_add(1);
                }
                refresh_metadata(&mut state);
            }
        }
        self.cleanup_retired()
    }

    fn reserve_entry_metadata(&self, charge: usize) -> Result<Option<OutboardLease>> {
        loop {
            if let Some(lease) = self.proof_budget.try_reserve_resident(charge) {
                return Ok(Some(lease));
            }
            // Retire acceleration metadata, never a healthy body or its trust
            // identity. Existing readers keep the wrapper's actual lease alive.
            let old = {
                let mut state = self.lock_state()?;
                let victim = state
                    .entries
                    .values_mut()
                    .filter(|entry| entry.proof.is_some())
                    .min_by_key(|entry| entry.touched);
                let old = victim.and_then(|entry| entry.proof.take());
                refresh_metadata(&mut state);
                old
            };
            let released = old.is_some();
            drop(old);
            if !released {
                return Ok(None);
            }
        }
    }

    fn reserve_pending(
        &self,
        key: &str,
        generation: u64,
        charge: u64,
        metadata_charge: usize,
        metadata: &mut Option<OutboardLease>,
        allow_eviction: bool,
    ) -> Result<bool> {
        if !allow_eviction {
            let Some(mut state) = self.try_lock_state()? else {
                return Ok(false);
            };
            if state.closed
                || state.foreground_waiters != 0
                || state.entries.contains_key(key)
                || state.retired.contains_key(key)
                || state.pending.len() >= self.max_flights
                || state.used_bytes.saturating_add(charge) > self.options.capacity_bytes
                || state.entries.len()
                    + state.retired.len()
                    + state.deleting_slots
                    + state.pending.len()
                    >= MAX_PEER_CACHE_ENTRIES
            {
                return Ok(false);
            }
            state.used_bytes += charge;
            state.pending_charge += charge;
            state.pending_metadata_charge += metadata_charge;
            state.pending.insert(
                generation,
                PendingEntry {
                    charge,
                    identity: None,
                    active: true,
                    metadata_charge,
                    _metadata: metadata.take(),
                },
            );
            state.stats.disk_peak_bytes = state.stats.disk_peak_bytes.max(state.used_bytes);
            return Ok(true);
        }
        self.retire(key, None, false)?;
        loop {
            self.cleanup_retired()?;
            let victim = {
                let mut state = self.lock_state()?;
                if state.closed
                    || state.retired.contains_key(key)
                    || charge > self.options.capacity_bytes
                    || state.pending.len() >= self.max_flights
                {
                    return Ok(false);
                }
                if state.used_bytes.saturating_add(charge) <= self.options.capacity_bytes
                    && state.entries.len()
                        + state.retired.len()
                        + state.deleting_slots
                        + state.pending.len()
                        < MAX_PEER_CACHE_ENTRIES
                {
                    state.used_bytes += charge;
                    state.pending_charge += charge;
                    state.pending_metadata_charge += metadata_charge;
                    state.pending.insert(
                        generation,
                        PendingEntry {
                            charge,
                            identity: None,
                            active: true,
                            metadata_charge,
                            _metadata: metadata.take(),
                        },
                    );
                    state.stats.disk_peak_bytes = state.stats.disk_peak_bytes.max(state.used_bytes);
                    return Ok(true);
                }
                state
                    .entries
                    .iter()
                    .min_by_key(|(_, entry)| entry.touched)
                    .map(|(key, _)| key.clone())
            };
            let Some(victim) = victim else {
                return Ok(false);
            };
            self.retire(&victim, None, true)?;
        }
    }

    fn cleanup_pending(&self) -> Result<()> {
        // Only inactive owned generations are eligible. An active stream's file
        // and an unidentified/replaced file are never another fill's cleanup.
        let retired: Vec<_> = self
            .lock_state()?
            .pending
            .iter()
            .filter(|(_, entry)| !entry.active)
            .map(|(generation, entry)| (*generation, entry.identity))
            .collect();
        for (generation, identity) in retired {
            if let Err(error) = self.clear_pending(generation, identity) {
                // Its full charge and metadata lease remain in the bounded table.
                // Another object may still fit without touching this generation.
                afs_logging::warn!("dfs.peer_pending_cleanup_failed"; "error" => error.to_string());
            }
        }
        Ok(())
    }

    fn clear_pending(&self, generation: u64, expected: Option<(u64, u64)>) -> Result<()> {
        self.check_root()?;
        #[cfg(test)]
        if self.lock_state()?.fail_remove {
            return Err(unavailable("injected Peer cache cleanup failure"));
        }
        match fs::symlink_metadata(self.pending_path(generation)) {
            Ok(metadata) => {
                validate_private_file(&metadata)?;
                if expected.is_none_or(|identity| identity != (metadata.dev(), metadata.ino())) {
                    return Err(invalid(
                        "Peer cache cleanup refuses an unowned pending file",
                    ));
                }
                fs::remove_file(self.pending_path(generation)).map_err(Error::from)?;
            }
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => {}
            Err(error) => return Err(Error::from(error)),
        }
        let removed = {
            let mut state = self.lock_state()?;
            remove_pending_charge(&mut state, generation)
        };
        drop(removed);
        Ok(())
    }

    fn account_pending_growth(&self, generation: u64, charge: u64) -> Result<bool> {
        let mut state = self.lock_state()?;
        let pending = state
            .pending
            .get_mut(&generation)
            .ok_or_else(|| unavailable("Peer pending reservation missing"))?;
        if charge <= pending.charge {
            return Ok(false);
        }
        let extra = charge - pending.charge;
        pending.charge = charge;
        state.used_bytes = state.used_bytes.saturating_add(extra);
        state.pending_charge = state.pending_charge.saturating_add(extra);
        state.stats.disk_peak_bytes = state.stats.disk_peak_bytes.max(state.used_bytes);
        Ok(true)
    }

    fn publish_entry(
        &self,
        pending_key: (u64, String),
        chunk: ChunkObject,
        charge: u64,
        identity: FileIdentity,
        proof: Option<SharedOutboard>,
        metadata: Option<OutboardLease>,
    ) -> Result<()> {
        let (generation, key) = pending_key;
        let metadata = {
            let mut state = self.lock_state()?;
            state
                .pending
                .get_mut(&generation)
                .and_then(|entry| entry._metadata.take())
                .or(metadata)
        };
        let mut local = CacheState::new_local(self.proof_budget());
        {
            let state = self.lock_state()?;
            local.local_index_available = state.index_lease.is_some();
            #[cfg(test)]
            {
                local.proof_limit = state.proof_limit;
            }
        }
        local.entries.insert(
            key.clone(),
            Entry {
                chunk,
                charge,
                identity: (identity.0, identity.1),
                touched: self.bucket(),
                proof: None,
                generation: Arc::new(()),
                _metadata: metadata,
                #[cfg(test)]
                drop_observation: None,
            },
        );
        install_proof(&mut local, &key, identity, proof);
        let entry = local
            .entries
            .remove(&key)
            .ok_or_else(|| unavailable("Peer cache entry missing"))?;
        let mut state = self.lock_state()?;
        let removed = remove_pending_charge(&mut state, generation);
        state.used_bytes += charge;
        drop(removed);
        state.entry_metadata_bytes += entry_metadata_charge(&key, &entry.chunk);
        state.entries.insert(key, entry);
        refresh_metadata(&mut state);
        drop(state);
        self.report_resources();
        Ok(())
    }

    #[cfg(test)]
    pub(crate) fn fill_flight<'a>(
        &'a self,
        chunk: &ChunkObject,
        check: impl Fn() -> Result<()>,
    ) -> Result<FillRole<'a>> {
        let key = self.key(chunk)?;
        let (flight, leader) = loop {
            check()?;
            let mut state = self.lock_state()?;
            if state.closed {
                return Err(unavailable("Peer cache is stopped"));
            }
            if let Some(flight) = state.flights.get(&key) {
                break (flight.clone(), false);
            }
            if state.flight_live < self.max_flights {
                let Some(metadata) = self
                    .proof_budget
                    .try_reserve(std::mem::size_of::<FillFlight>() + 512 + key.capacity() + 256)
                else {
                    return Ok(FillRole::Finished(FillOutcome::Uncached));
                };
                let flight = Arc::new(FillFlight {
                    result: Mutex::new(None),
                    changed: Condvar::new(),
                    asynchronous_change: tokio::sync::Notify::new(),
                    _metadata: metadata,
                });
                state.flights.insert(key.clone(), flight.clone());
                state.flight_live += 1;
                state.flight_metadata_bytes +=
                    std::mem::size_of::<FillFlight>() + 512 + key.capacity() + 256;
                break (flight, true);
            }
            drop(
                self.changed
                    .wait_timeout(state, Duration::from_millis(25))
                    .map_err(|_| unavailable("Peer flight admission lock poisoned"))?,
            );
        };
        if leader {
            self.report_resources();
            return Ok(FillRole::Leader(FillLeader {
                cache: self,
                key,
                flight: Some(flight),
                completed: false,
            }));
        }
        let outcome = (|| loop {
            check()?;
            if self.lock_state()?.closed {
                return Err(unavailable("Peer cache is stopped"));
            }
            let result = flight
                .result
                .lock()
                .map_err(|_| unavailable("Peer flight lock poisoned"))?;
            if let Some(outcome) = &*result {
                return Ok(outcome.clone());
            }
            drop(
                flight
                    .changed
                    .wait_timeout(result, Duration::from_millis(25))
                    .map_err(|_| unavailable("Peer flight wait lock poisoned"))?,
            );
        })();
        self.release_flight(&key, flight);
        outcome.map(FillRole::Finished)
    }
    pub(crate) async fn fill_flight_async<'a>(
        &'a self,
        chunk: &ChunkObject,
        check: impl Fn() -> Result<()> + Send + Sync,
    ) -> Result<FillRole<'a>> {
        let key = self.key(chunk)?;
        let (flight, leader) = loop {
            let changed = self.asynchronous_change.notified();
            tokio::pin!(changed);
            changed.as_mut().enable();
            check()?;
            let selected = {
                let mut state = self.lock_state()?;
                if state.closed {
                    return Err(unavailable("Peer cache is stopped"));
                }
                if let Some(flight) = state.flights.get(&key) {
                    Some((flight.clone(), false))
                } else if state.flight_live < self.max_flights {
                    let charge = std::mem::size_of::<FillFlight>() + 512 + key.capacity() + 256;
                    let Some(metadata) = self.proof_budget.try_reserve(charge) else {
                        return Ok(FillRole::Finished(FillOutcome::Uncached));
                    };
                    let flight = Arc::new(FillFlight {
                        result: Mutex::new(None),
                        changed: Condvar::new(),
                        asynchronous_change: tokio::sync::Notify::new(),
                        _metadata: metadata,
                    });
                    state.flights.insert(key.clone(), flight.clone());
                    state.flight_live += 1;
                    state.flight_metadata_bytes += charge;
                    Some((flight, true))
                } else {
                    None
                }
            };
            if let Some(selected) = selected {
                break selected;
            }
            let _ = tokio::time::timeout(Duration::from_millis(25), changed).await;
        };
        if leader {
            self.report_resources();
            return Ok(FillRole::Leader(FillLeader {
                cache: self,
                key,
                flight: Some(flight),
                completed: false,
            }));
        }
        let follower = AsyncFillFollower {
            cache: self,
            key,
            flight: Some(flight),
        };
        let flight = follower
            .flight
            .as_ref()
            .ok_or_else(|| unavailable("Peer follower flight missing"))?;
        let outcome = async {
            loop {
                let changed = flight.asynchronous_change.notified();
                tokio::pin!(changed);
                changed.as_mut().enable();
                check()?;
                if self.lock_state()?.closed {
                    return Err(unavailable("Peer cache is stopped"));
                }
                let ready = flight
                    .result
                    .lock()
                    .map_err(|_| unavailable("Peer flight lock poisoned"))?
                    .clone();
                if let Some(outcome) = ready {
                    return Ok(outcome);
                }
                let _ = tokio::time::timeout(Duration::from_millis(25), changed).await;
            }
        }
        .await;
        drop(follower);
        outcome.map(FillRole::Finished)
    }

    pub(crate) fn try_fill_flight(
        self: &Arc<Self>,
        chunk: &ChunkObject,
    ) -> Result<Option<OwnedFillLeader>> {
        let key = self.key(chunk)?;
        let Some(mut state) = self.try_lock_state()? else {
            return Ok(None);
        };
        if state.closed
            || state.foreground_waiters != 0
            || state.flights.contains_key(&key)
            || state.flight_live >= self.max_flights
        {
            return Ok(None);
        }
        let charge = std::mem::size_of::<FillFlight>() + 512 + key.capacity() + 256;
        let Some(metadata) = self.proof_budget.try_reserve(charge) else {
            return Ok(None);
        };
        let flight = Arc::new(FillFlight {
            result: Mutex::new(None),
            changed: Condvar::new(),
            asynchronous_change: tokio::sync::Notify::new(),
            _metadata: metadata,
        });
        state.flights.insert(key.clone(), flight.clone());
        state.flight_live += 1;
        state.flight_metadata_bytes += charge;
        drop(state);
        Ok(Some(OwnedFillLeader {
            cache: self.clone(),
            key,
            flight: Some(flight),
            completed: false,
        }))
    }

    fn release_flight(&self, key: &str, flight: Arc<FillFlight>) {
        let removed = if let Ok(mut state) = self.state.lock() {
            let same = state
                .flights
                .get(key)
                .is_some_and(|current| Arc::ptr_eq(current, &flight));
            drop(flight);
            if same
                && state
                    .flights
                    .get(key)
                    .is_some_and(|current| Arc::strong_count(current) == 1)
            {
                state.flights.remove(key)
            } else {
                None
            }
        } else {
            None
        };
        let released = removed.is_some();
        drop(removed);
        if released && let Ok(mut state) = self.state.lock() {
            state.flight_live = state.flight_live.saturating_sub(1);
            state.flight_metadata_bytes = state
                .flight_metadata_bytes
                .saturating_sub(std::mem::size_of::<FillFlight>() + 512 + key.len() + 256);
        }
        self.changed.notify_all();
        self.asynchronous_change.notify_waiters();
        self.report_resources();
    }
}

struct AsyncFillFollower<'a> {
    cache: &'a DiskPeerCache,
    key: String,
    flight: Option<Arc<FillFlight>>,
}
impl Drop for AsyncFillFollower<'_> {
    fn drop(&mut self) {
        if let Some(flight) = self.flight.take() {
            self.cache.release_flight(&self.key, flight);
        }
    }
}

fn finish_flight(flight: &Option<Arc<FillFlight>>, completed: &mut bool, outcome: FillOutcome) {
    if *completed {
        return;
    }
    let outcome = match outcome {
        FillOutcome::Failed(error) if error.message().len() > 512 => {
            afs_logging::warn!("dfs.peer_fill_shared_error_truncated"; "code" => error.code().to_string());
            let mut boundary = 512;
            while !error.message().is_char_boundary(boundary) {
                boundary -= 1;
            }
            FillOutcome::Failed(Error::new(
                error.code(),
                error.kind(),
                &error.message()[..boundary],
            ))
        }
        outcome => outcome,
    };
    if let Some(flight) = flight
        && let Ok(mut result) = flight.result.lock()
    {
        *result = Some(outcome);
        *completed = true;
        flight.changed.notify_all();
        flight.asynchronous_change.notify_waiters();
    }
}

impl FillLeader<'_> {
    pub(crate) fn finish(&mut self, outcome: FillOutcome) {
        finish_flight(&self.flight, &mut self.completed, outcome);
    }
}
impl OwnedFillLeader {
    pub(crate) fn finish(&mut self, outcome: FillOutcome) {
        finish_flight(&self.flight, &mut self.completed, outcome);
    }
}
impl Drop for OwnedFillLeader {
    fn drop(&mut self) {
        self.finish(FillOutcome::Failed(unavailable(
            "Peer prefetch stopped before completion",
        )));
        if let Some(flight) = self.flight.take() {
            self.cache.release_flight(&self.key, flight);
        }
    }
}

impl Drop for FillLeader<'_> {
    fn drop(&mut self) {
        self.finish(FillOutcome::Failed(unavailable(
            "Peer fill leader stopped before completion",
        )));
        if let Some(flight) = self.flight.take() {
            self.cache.release_flight(&self.key, flight);
        }
    }
}

struct FillWaiter<'a>(&'a DiskPeerCache);
impl<'a> FillWaiter<'a> {
    fn new(cache: &'a DiskPeerCache) -> Result<Self> {
        cache.lock_state()?.foreground_waiters += 1;
        Ok(Self(cache))
    }
}
impl Drop for FillWaiter<'_> {
    fn drop(&mut self) {
        if let Ok(mut state) = self.0.state.lock() {
            state.foreground_waiters = state.foreground_waiters.saturating_sub(1);
        }
        self.0.changed.notify_all();
        self.0.asynchronous_change.notify_waiters();
    }
}

impl Drop for FillPermit<'_> {
    fn drop(&mut self) {
        if let Ok(mut state) = self.cache.state.lock() {
            if self.streaming {
                state.stream_active = state.stream_active.saturating_sub(1);
            } else {
                state.fill_active = false;
            }
        }
        self.cache.changed.notify_all();
        self.cache.asynchronous_change.notify_waiters();
        self.cache.report_resources();
    }
}

impl Drop for EntryPin<'_> {
    fn drop(&mut self) {
        drop(self.generation.take());
        if let Err(error) = self.cache.cleanup_retired() {
            afs_logging::warn!("dfs.peer_cache_retired_cleanup_failed"; "error" => error.to_string());
        }
    }
}

impl PeerCacheOperation<'_> {
    pub(crate) fn has_control_capacity(&self) -> bool {
        self._metadata.is_some()
    }

    pub(crate) fn acquire_fill(&mut self, check: impl Fn() -> Result<()>) -> Result<()> {
        self.acquire_fill_mode(false, check)
    }

    pub(crate) fn acquire_stream_fill(&mut self, check: impl Fn() -> Result<()>) -> Result<()> {
        self.acquire_fill_mode(true, check)
    }

    pub(crate) fn try_acquire_stream_fill(&mut self) -> Result<bool> {
        if self.fill.is_some() {
            return Ok(true);
        }
        let Some(mut state) = self.cache.try_lock_state()? else {
            return Ok(false);
        };
        if state.closed
            || state.foreground_waiters != 0
            || state.fill_active
            || state.stream_active >= 2
        {
            return Ok(false);
        }
        state.stream_active += 1;
        self.fill = Some(FillPermit {
            cache: self.cache,
            streaming: true,
        });
        drop(state);
        Ok(true)
    }

    pub(crate) fn release_fill(&mut self) {
        drop(self.fill.take());
    }

    fn acquire_fill_mode(&mut self, streaming: bool, check: impl Fn() -> Result<()>) -> Result<()> {
        if let Some(fill) = &self.fill {
            if fill.streaming == streaming || !fill.streaming {
                return Ok(());
            }
            return Err(invalid(
                "legacy fill requires releasing the stream allowance",
            ));
        }
        let _waiting = FillWaiter::new(self.cache)?;
        loop {
            check()?;
            let mut state = self.cache.lock_state()?;
            if state.closed {
                return Err(unavailable("Peer cache is stopped"));
            }
            if !state.fill_active
                && if streaming {
                    state.stream_active < 2
                } else {
                    state.stream_active == 0
                }
            {
                if streaming {
                    state.stream_active += 1;
                } else {
                    state.fill_active = true;
                }
                self.fill = Some(FillPermit {
                    cache: self.cache,
                    streaming,
                });
                drop(state);
                self.cache.report_resources();
                return Ok(());
            }
            drop(
                self.cache
                    .changed
                    .wait_timeout(state, Duration::from_millis(25))
                    .map_err(|_| unavailable("Peer fill admission lock poisoned"))?,
            );
        }
    }

    pub(crate) async fn acquire_fill_async(
        &mut self,
        streaming: bool,
        check: impl Fn() -> Result<()> + Send + Sync,
    ) -> Result<()> {
        if let Some(fill) = &self.fill {
            if fill.streaming == streaming || !fill.streaming {
                return Ok(());
            }
            return Err(invalid(
                "legacy fill requires releasing the stream allowance",
            ));
        }
        let _waiting = FillWaiter::new(self.cache)?;
        loop {
            let changed = self.cache.asynchronous_change.notified();
            tokio::pin!(changed);
            changed.as_mut().enable();
            check()?;
            let admitted = {
                let mut state = self.cache.lock_state()?;
                if state.closed {
                    return Err(unavailable("Peer cache is stopped"));
                }
                if !state.fill_active
                    && if streaming {
                        state.stream_active < 2
                    } else {
                        state.stream_active == 0
                    }
                {
                    if streaming {
                        state.stream_active += 1
                    } else {
                        state.fill_active = true
                    }
                    self.fill = Some(FillPermit {
                        cache: self.cache,
                        streaming,
                    });
                    true
                } else {
                    false
                }
            };
            if admitted {
                self.cache.report_resources();
                return Ok(());
            }
            let _ = tokio::time::timeout(Duration::from_millis(25), changed).await;
        }
    }

    pub fn hash_work(&self) -> PeerCacheHashWork {
        self.state.stats.hash_work
    }

    /// A miss/error may have modified private output; never publish that output.
    pub fn begin_stream<'operation>(
        &'operation mut self,
        chunk: &ChunkObject,
    ) -> Result<PeerStreamSink<'operation>> {
        self.acquire_stream_fill(|| Ok(()))?;
        self.start_stream(chunk, true, None)
    }

    pub(crate) fn try_begin_stream<'operation>(
        &'operation mut self,
        chunk: &ChunkObject,
    ) -> Result<Option<PeerStreamSink<'operation>>> {
        if !self.try_acquire_stream_fill()? {
            return Ok(None);
        }
        let generation = {
            let Some(mut state) = self.cache.try_lock_state()? else {
                return Ok(None);
            };
            let generation = state.next_pending_generation;
            state.next_pending_generation = generation
                .checked_add(1)
                .ok_or_else(|| unavailable("Peer pending generation exhausted"))?;
            generation
        };
        self.start_stream(chunk, false, Some(generation)).map(Some)
    }

    fn start_stream<'operation>(
        &'operation mut self,
        chunk: &ChunkObject,
        allow_eviction: bool,
        reserved_generation: Option<u64>,
    ) -> Result<PeerStreamSink<'operation>> {
        validate_chunk(chunk)?;
        if chunk.length > MAX_OBJECT_BYTES {
            return Err(invalid("Peer stream object exceeds bound"));
        }
        let key = self.cache.key(chunk)?;
        let generation = if let Some(generation) = reserved_generation {
            generation
        } else {
            let mut state = self.cache.lock_state()?;
            let generation = state.next_pending_generation;
            state.next_pending_generation = generation
                .checked_add(1)
                .ok_or_else(|| unavailable("Peer pending generation exhausted"))?;
            generation
        };
        let mut sink = PeerStreamSink {
            cache: self.cache,
            _fill: self
                .fill
                .as_ref()
                .ok_or_else(|| unavailable("Peer fill allowance missing"))?,
            chunk: chunk.clone(),
            key,
            received: 0,
            header_length: 0,
            file: None,
            pending_identity: None,
            pending_generation: generation,
            owns_pending: false,
            committed: false,
            prepare_error: None,
            allow_eviction,
            entry_lease: None,
            identity: Arc::new(()),
            #[cfg(test)]
            write_delay: std::time::Duration::ZERO,
        };
        if let Err(error) = sink.prepare() {
            sink.prepare_error = Some(error.clone());
            sink.disable(&error);
        }
        Ok(sink)
    }

    pub fn read_range(&mut self, chunk: &ChunkObject, offset: u64, out: &mut [u8]) -> Result<bool> {
        if self._metadata.is_none() {
            return self.read_range_root_fallback(chunk, offset, out);
        }
        let prepare_started = Instant::now();
        let selected = (|| {
            self.cache.check_root()?;
            validate_chunk(chunk)?;
            if offset
                .checked_add(out.len() as u64)
                .is_none_or(|end| end > chunk.length)
            {
                return Err(invalid("Peer cache range exceeds immutable object"));
            }
            let key = self.cache.key(chunk)?;
            let selected = {
                let state = self.cache.lock_state()?;
                state.entries.get(&key).map(|entry| {
                    (
                        entry.chunk.clone(),
                        entry.charge,
                        entry.identity,
                        entry.touched,
                        entry.proof.clone(),
                        entry.generation.clone(),
                    )
                })
            };
            let Some((object, charge, identity, touched, proof, generation)) = selected else {
                self.state.stats.misses = self.state.stats.misses.saturating_add(1);
                return Ok(None);
            };
            if object != *chunk {
                return Err(invalid("Peer cache index identity differs"));
            }
            let pin = EntryPin {
                cache: self.cache,
                generation: Some(generation),
            };
            self.state.entries.insert(
                key.clone(),
                Entry {
                    chunk: object,
                    charge,
                    identity,
                    touched,
                    proof,
                    generation: Arc::new(()),
                    _metadata: None,
                    #[cfg(test)]
                    drop_observation: None,
                },
            );
            Ok(Some((key, identity, pin)))
        })();
        let (key, expected, pin) = match selected {
            Ok(Some(selected)) => selected,
            early => {
                self.state.stats.read_stages.prepare_ns = self
                    .state
                    .stats
                    .read_stages
                    .prepare_ns
                    .saturating_add(elapsed_ns(prepare_started));
                return early.map(|_| false);
            }
        };
        // Only indexed hit attempts: misses above do not count as disk hits.
        // Includes opening/header/identity/range verification and finalization.
        let _diagnostic = self.cache.read_work.as_ref().and_then(|metrics| {
            metrics.coarse_diagnostic("cache_hit_read_verify", out.len() as u64)
        });
        let object_diagnostic = self.cache.read_work.as_ref().and_then(|metrics| {
            metrics.object_diagnostic(
                super::read_work::ReadObjectDiagnosticStage::CacheHitReadVerify,
                &chunk.id.0,
            )
        });
        let path = self.cache.path(&key);
        let mut finalize_started = None;
        let result = (|| {
            let prepared = (|| {
                let (mut file, before) = open_private(&path)?;
                if (before.dev(), before.ino()) != expected {
                    return Err(invalid("Peer cache ready file was replaced"));
                }
                let (header, body_start) = read_header(&mut file)?;
                if header.version != 2
                    || header.namespace_id != self.cache.options.namespace_id
                    || header.chunk != *chunk
                {
                    return Err(corrupt("Peer cache header identity differs"));
                }
                validate_file_length(&header, &before)?;
                let identity = file_identity(&before);
                let reusable = self.state.entries.get(&key).and_then(|entry| {
                    entry
                        .proof
                        .as_ref()
                        .filter(|proof| proof.identity == identity)
                        .map(|proof| proof.outboard.clone())
                });
                Ok((file, before, body_start, header, reusable))
            })();
            self.state.stats.read_stages.prepare_ns = self
                .state
                .stats
                .read_stages
                .prepare_ns
                .saturating_add(elapsed_ns(prepare_started));
            let (mut file, before, body_start, header, reusable) = prepared?;
            let fresh = if let Some(outboard) = reusable {
                match scan_outboard(
                    &file,
                    body_start,
                    chunk,
                    offset,
                    out,
                    &outboard,
                    &mut self.state.stats,
                ) {
                    Ok(()) => None,
                    Err(error) if error.code() == afs_error::IO_CAPACITY => {
                        self.state.budget.record_range_fullscan_fallback();
                        // Optional proof work must not create a new read failure
                        // when another live proof temporarily holds the ledger.
                        let started = Instant::now();
                        let result = scan_body(
                            &mut file,
                            body_start,
                            chunk,
                            offset,
                            out,
                            &mut self.state.stats,
                            None,
                        );
                        self.state.stats.read_stages.stream_read_verify_ns = self
                            .state
                            .stats
                            .read_stages
                            .stream_read_verify_ns
                            .saturating_add(elapsed_ns(started));
                        result?
                    }
                    Err(error) => return Err(error),
                }
            } else {
                // Metadata changes invalidate old summaries before a complete root scan.
                let started = Instant::now();
                if let Some(entry) = self.state.entries.get_mut(&key) {
                    entry.proof = None;
                }
                refresh_metadata(&mut self.state);
                let index = proof_can_fit(&self.state);
                let peer = if index {
                    read_disk_outboard(&mut file, &header, &self.state.budget)?
                } else {
                    None
                };
                // Without the retained complete FileIdentity we cannot claim
                // unchanged bytes merely because a trailer is well shaped.
                self.state.budget.record_range_fullscan_unretained();
                let result = scan_body(
                    &mut file,
                    body_start,
                    chunk,
                    offset,
                    out,
                    &mut self.state.stats,
                    index.then(|| (self.cache.proof_budget(), peer)),
                );
                self.state.stats.read_stages.stream_read_verify_ns = self
                    .state
                    .stats
                    .read_stages
                    .stream_read_verify_ns
                    .saturating_add(elapsed_ns(started));
                result?
            };
            finalize_started = Some(Instant::now());
            let after = file.metadata().map_err(Error::from)?;
            let current = fs::symlink_metadata(&path).map_err(Error::from)?;
            if file_identity(&before) != file_identity(&after)
                || file_identity(&before) != file_identity(&current)
            {
                return Err(corrupt("Peer cache file changed during verified read"));
            }
            if fresh.is_some() {
                install_proof(&mut self.state, &key, file_identity(&after), fresh);
            }
            Ok(())
        })();
        let finalize_started = finalize_started.unwrap_or_else(Instant::now);
        let outcome = (|| match result {
            Ok(()) => {
                if let Some(entry) = self.state.entries.get_mut(&key) {
                    entry.touched = self.cache.bucket();
                }
                self.state.stats.hits = self.state.stats.hits.saturating_add(1);
                Ok(true)
            }
            Err(error)
                if error.code() == afs_error::NODE_TRANSFER_CORRUPT_DATA
                    || error.kind() == afs_error::ErrorKind::NotFound =>
            {
                self.state.stats.corruptions = self.state.stats.corruptions.saturating_add(1);
                self.state.stats.misses = self.state.stats.misses.saturating_add(1);
                self.cache.retire(&key, pin.generation.as_ref(), false)?;
                Ok(false)
            }
            Err(error) => Err(error),
        })();
        self.state.stats.read_stages.finalize_ns = self
            .state
            .stats
            .read_stages
            .finalize_ns
            .saturating_add(elapsed_ns(finalize_started));
        let local_entry = self.state.entries.remove(&key);
        let old_proof = if outcome.as_ref().is_ok_and(|hit| *hit) {
            let mut state = self.cache.lock_state()?;
            if let Some(entry) = state.entries.get_mut(&key)
                && pin
                    .generation
                    .as_ref()
                    .is_some_and(|generation| Arc::ptr_eq(generation, &entry.generation))
            {
                entry.touched = self.cache.bucket();
                let previous = entry.proof.take();
                entry.proof = local_entry.as_ref().and_then(|entry| entry.proof.clone());
                refresh_metadata(&mut state);
                previous
            } else {
                None
            }
        } else {
            None
        };
        drop(old_proof);
        drop(local_entry);
        drop(pin);
        if let Some(diagnostic) = object_diagnostic {
            diagnostic.finish(outcome.as_ref().is_ok_and(|hit| *hit));
        }
        outcome
    }

    fn read_range_root_fallback(
        &mut self,
        chunk: &ChunkObject,
        offset: u64,
        out: &mut [u8],
    ) -> Result<bool> {
        self.cache.check_root()?;
        validate_chunk(chunk)?;
        if offset
            .checked_add(out.len() as u64)
            .is_none_or(|end| end > chunk.length)
        {
            return Err(invalid("Peer cache range exceeds immutable object"));
        }
        let key = self.cache.key(chunk)?;
        let selected = {
            let state = self.cache.lock_state()?;
            state
                .entries
                .get(&key)
                .map(|entry| (entry.identity, entry.generation.clone()))
        };
        let Some((expected, generation)) = selected else {
            self.state.stats.misses += 1;
            return Ok(false);
        };
        let pin = EntryPin {
            cache: self.cache,
            generation: Some(generation),
        };
        let object_diagnostic = self.cache.read_work.as_ref().and_then(|metrics| {
            metrics.object_diagnostic(
                super::read_work::ReadObjectDiagnosticStage::CacheHitRootFallback,
                &chunk.id.0,
            )
        });
        let result = (|| {
            let (mut file, before) = open_private(&self.cache.path(&key))?;
            if (before.dev(), before.ino()) != expected {
                return Err(invalid("Peer cache ready file was replaced"));
            }
            let (header, body_start) = read_header(&mut file)?;
            if header.version != 2
                || header.namespace_id != self.cache.options.namespace_id
                || header.chunk != *chunk
            {
                return Err(corrupt("Peer cache header identity differs"));
            }
            validate_file_length(&header, &before)?;
            self.cache.proof_budget.record_range_fullscan_fallback();
            let started = Instant::now();
            let scanned = scan_body(
                &mut file,
                body_start,
                chunk,
                offset,
                out,
                &mut self.state.stats,
                None,
            );
            self.state.stats.read_stages.stream_read_verify_ns += elapsed_ns(started);
            scanned?;
            let after = file.metadata().map_err(Error::from)?;
            let current = fs::symlink_metadata(self.cache.path(&key)).map_err(Error::from)?;
            if file_identity(&before) != file_identity(&after)
                || file_identity(&after) != file_identity(&current)
            {
                return Err(corrupt("Peer cache file changed during verified read"));
            }
            Ok(())
        })();
        let outcome = match result {
            Ok(()) => {
                self.state.stats.hits += 1;
                Ok(true)
            }
            Err(error)
                if error.code() == afs_error::NODE_TRANSFER_CORRUPT_DATA
                    || error.kind() == afs_error::ErrorKind::NotFound =>
            {
                self.state.stats.corruptions += 1;
                self.state.stats.misses += 1;
                self.cache.retire(&key, pin.generation.as_ref(), false)?;
                Ok(false)
            }
            Err(error) => Err(error),
        };
        if let Some(diagnostic) = object_diagnostic {
            diagnostic.finish(outcome.as_ref().is_ok_and(|hit| *hit));
        }
        outcome
    }

    /// Only borrowed typed proof is admitted; this method does not hash again.
    pub fn store_verified(&mut self, verified: &VerifiedPeerChunk<'_>) -> Result<bool> {
        if verified.chunk.length > MAX_OBJECT_BYTES {
            return Ok(false);
        }
        self.acquire_fill(|| Ok(()))?;
        let mut sink = self.begin_stream(verified.chunk)?;
        if let Some(error) = sink.prepare_error.take() {
            return Err(error);
        }
        sink.write(0, verified.bytes)?;
        let Some(mut file) = sink.file.take() else {
            return Ok(false);
        };
        let outboard = verified
            .outboard
            .as_ref()
            .filter(|proof| proof.uses_budget(&sink.cache.proof_budget));
        let trailer = outboard.map_or(&[][..], |proof| proof.encoded());
        if let Some(proof) = outboard {
            validate_outboard(verified.chunk, proof)?;
        }
        let result = (|| {
            file.write_all(trailer).map_err(Error::from)?;
            file.seek(SeekFrom::Start(0)).map_err(Error::from)?;
            write_header(
                &mut file,
                &Header {
                    version: 2,
                    namespace_id: sink.cache.options.namespace_id.clone(),
                    chunk: verified.chunk.clone(),
                    outboard_length: trailer.len() as u32,
                },
            )?;
            file.flush().map_err(Error::from)?;
            sink.cache.check_root()?;
            let before = file.metadata().map_err(Error::from)?;
            validate_private_file(&before)?;
            let current = fs::symlink_metadata(sink.cache.pending_path(sink.pending_generation))
                .map_err(Error::from)?;
            if sink.pending_identity != Some((before.dev(), before.ino()))
                || file_identity(&before) != file_identity(&current)
            {
                return Err(corrupt("Peer cache staging identity changed"));
            }
            let charge = allocated(&before)?;
            if sink
                .cache
                .account_pending_growth(sink.pending_generation, charge)?
            {
                return Err(unavailable("Peer cache allocation exceeds reserved blocks"));
            }
            match fs::symlink_metadata(sink.cache.path(&sink.key)) {
                Ok(_) => return Err(invalid("Peer cache publication refuses an untracked file")),
                Err(error) if error.kind() == std::io::ErrorKind::NotFound => {}
                Err(error) => return Err(Error::from(error)),
            }
            fs::rename(
                sink.cache.pending_path(sink.pending_generation),
                sink.cache.path(&sink.key),
            )
            .map_err(Error::from)?;
            sink.committed = true;
            sink.owns_pending = false;
            let published = file
                .metadata()
                .ok()
                .map(|metadata| file_identity(&metadata));
            sink.cache.publish_entry(
                (sink.pending_generation, sink.key.clone()),
                verified.chunk.clone(),
                charge,
                published.unwrap_or_else(|| file_identity(&before)),
                if published.is_some() {
                    outboard.cloned()
                } else {
                    None
                },
                sink.entry_lease.take(),
            )?;
            Ok(true)
        })();
        drop(file);
        // Sink Drop performs identity-bound cleanup; keep I/O errors visible to
        // the caller, which retains the original verified private output.
        result
    }
}

fn scan_body(
    file: &mut File,
    body_start: u64,
    chunk: &ChunkObject,
    offset: u64,
    out: &mut [u8],
    stats: &mut PeerCacheStats,
    proof: Option<(Arc<OutboardBudget>, Option<SharedOutboard>)>,
) -> Result<Option<SharedOutboard>> {
    file.seek(SeekFrom::Start(body_start))
        .map_err(Error::from)?;
    let mut scratch = [0_u8; SCRATCH_BYTES];
    stats.scratch_peak_bytes = stats.scratch_peak_bytes.max(SCRATCH_BYTES as u64);
    let mut builder = proof.map(|(budget, peer)| match peer {
        Some(peer) => OutboardBuilder::validating(chunk.clone(), peer, budget),
        None => OutboardBuilder::new(chunk.length, budget),
    });
    let mut hash = blake3::Hasher::new();
    let mut position = 0_u64;
    while position < chunk.length {
        let requested = (chunk.length - position).min(SCRATCH_BYTES as u64) as usize;
        stats.data_read_calls = stats.data_read_calls.saturating_add(1);
        let read = file.read(&mut scratch[..requested]).map_err(Error::from)?;
        if read == 0 {
            return Err(corrupt("Peer cache body is truncated"));
        }
        stats.hash_bytes_total = stats.hash_bytes_total.saturating_add(read as u64);
        stats.hash_work.cache_fullscan_bytes = stats
            .hash_work
            .cache_fullscan_bytes
            .saturating_add(read as u64);
        if let Some(builder) = &mut builder {
            builder.update(&scratch[..read]).map_err(|error| {
                if error.code() == afs_error::IO_CAPACITY {
                    error
                } else {
                    corrupt("Peer cache outboard differs from complete body")
                }
            })?;
        } else {
            hash.update(&scratch[..read]);
        }
        stats.copied_bytes_total = stats.copied_bytes_total.saturating_add(copy_overlap(
            position,
            &scratch[..read],
            offset,
            out,
        ));
        position += read as u64;
    }
    if let Some(builder) = builder {
        let built = builder.finish().map_err(|error| {
            if error.code() == afs_error::IO_CAPACITY {
                error
            } else {
                corrupt("Peer cache outboard or root differs from complete body")
            }
        })?;
        if built.digest != chunk.content_digest {
            return Err(corrupt("Peer cache complete body digest differs"));
        }
        stats.hash_work.outboard_parent_bytes = stats
            .hash_work
            .outboard_parent_bytes
            .saturating_add(built.parent_hash_bytes);
        Ok(built.outboard)
    } else if hash.finalize().as_bytes() != &chunk.content_digest.bytes {
        Err(corrupt("Peer cache complete body digest differs"))
    } else {
        Ok(None)
    }
}

fn scan_outboard(
    file: &File,
    body_start: u64,
    chunk: &ChunkObject,
    offset: u64,
    out: &mut [u8],
    proof: &SharedOutboard,
    stats: &mut PeerCacheStats,
) -> Result<()> {
    validate_outboard(chunk, proof)?;
    let mut work = OutboardReadWork::default();
    let started = Instant::now();
    let result = proof.read_ranges_into_with_work(
        file,
        body_start,
        chunk,
        &[VerifiedRangeCopy {
            chunk_offset: offset,
            output_offset: 0,
            length: out.len(),
        }],
        out,
        &mut work,
    );
    stats.hash_bytes_total = stats.hash_bytes_total.saturating_add(work.hash_bytes);
    stats.hash_work.cache_segment_bytes = stats
        .hash_work
        .cache_segment_bytes
        .saturating_add(work.hash_bytes);
    stats.hash_work.outboard_parent_bytes = stats
        .hash_work
        .outboard_parent_bytes
        .saturating_add(work.parent_hash_bytes);
    stats.data_read_calls = stats.data_read_calls.saturating_add(work.read_calls);
    stats.copied_bytes_total = stats.copied_bytes_total.saturating_add(work.copied_bytes);
    stats.scratch_peak_bytes = stats.scratch_peak_bytes.max(work.scratch_peak as u64);
    stats.read_stages.data_read_ns = stats.read_stages.data_read_ns.saturating_add(work.read_ns);
    stats.read_stages.segment_verify_ns = stats
        .read_stages
        .segment_verify_ns
        .saturating_add(work.verify_ns);
    // End-to-end overlap is measured separately; this is one local operation.
    stats.read_stages.stream_read_verify_ns = stats
        .read_stages
        .stream_read_verify_ns
        .saturating_add(elapsed_ns(started));
    result.map(|_| ()).map_err(|error| {
        if error.code() == afs_error::IO_INVALID {
            corrupt("Peer cache coverage proof differs from trusted object")
        } else {
            error
        }
    })
}

fn copy_overlap(position: u64, bytes: &[u8], offset: u64, out: &mut [u8]) -> u64 {
    let start = offset.max(position);
    let stop = (offset + out.len() as u64).min(position + bytes.len() as u64);
    if stop > start {
        out[(start - offset) as usize..(stop - offset) as usize]
            .copy_from_slice(&bytes[(start - position) as usize..(stop - position) as usize]);
        stop - start
    } else {
        0
    }
}

fn entry_metadata_charge(key: &str, chunk: &ChunkObject) -> usize {
    // Chunk IDs are bounded to 512 bytes. Two entry slots per live item cover
    // B-tree node occupancy; the fixed index reservation also covers sparse
    // roots in the indexed/retired maps. The charge survives generation pins.
    2 * (std::mem::size_of::<Entry>() + key.len() + chunk.id.0.capacity() + 256)
}

fn refresh_metadata(state: &mut CacheState) {
    let pointers = (state.entries.len() + state.retired.len())
        * std::mem::size_of::<Option<Box<VerifiedDiskOutboard>>>();
    let proofs: usize = state
        .entries
        .values()
        .chain(state.retired.values().map(|retired| &retired.entry))
        .filter_map(|entry| entry.proof.as_ref())
        .map(|proof| proof.outboard.charged_bytes() + proof_allocation_charge() as usize)
        .sum();
    // Resident cache ownership only; shared budget.used() also includes local
    // readers and in-flight Arc holders after an entry has been evicted.
    state.stats.proof_metadata_bytes = (pointers + proofs) as u64;
    state.stats.proof_metadata_peak_bytes = state
        .stats
        .proof_metadata_peak_bytes
        .max(state.stats.proof_metadata_bytes);
}

fn proof_can_fit(state: &CacheState) -> bool {
    #[cfg(test)]
    if state.proof_limit.is_some_and(|limit| {
        state
            .stats
            .proof_metadata_bytes
            .saturating_add(proof_allocation_charge())
            > limit
    }) {
        return false;
    }
    state.index_lease.is_some() || state.local_index_available
}

fn proof_allocation_charge() -> u64 {
    let logical = std::mem::size_of::<VerifiedDiskOutboard>() as u64;
    logical.div_ceil(64) * 64 + 64
}

fn install_proof(
    state: &mut CacheState,
    key: &str,
    identity: FileIdentity,
    outboard: Option<SharedOutboard>,
) {
    if let Some(entry) = state.entries.get_mut(key) {
        entry.proof = None;
    }
    refresh_metadata(state);
    if !proof_can_fit(state) {
        return;
    }
    let Some(outboard) = outboard.filter(|proof| proof.uses_budget(&state.budget)) else {
        return;
    };
    loop {
        // The pointer index, identity wrapper and shared proof all use the same
        // resident sublimit. This promotion never charges the proof twice total.
        if let Some(lease) = state
            .budget
            .try_reserve_resident(proof_allocation_charge() as usize)
            && outboard.try_mark_resident()
        {
            if let Some(entry) = state.entries.get_mut(key) {
                entry.proof = Some(Arc::new(VerifiedDiskOutboard {
                    identity,
                    outboard,
                    _lease: lease,
                }));
            }
            break;
        }
        // Retire only old proof ownership. Healthy body entries remain usable;
        // outside Arc holders keep their own resident charge until last drop.
        let victim = state
            .entries
            .values_mut()
            .filter(|entry| entry.proof.is_some())
            .min_by_key(|entry| entry.touched);
        let Some(entry) = victim else {
            break;
        };
        entry.proof = None;
        refresh_metadata(state);
    }
    refresh_metadata(state);
}

fn validate_outboard(chunk: &ChunkObject, proof: &ChunkOutboard) -> Result<()> {
    if proof.object_length() != chunk.length || proof.digest() != &chunk.content_digest {
        return Err(corrupt("Peer outboard differs from trusted Meta object"));
    }
    Ok(())
}

fn validate_file_length(header: &Header, metadata: &fs::Metadata) -> Result<()> {
    let expected = (MAX_HEADER_BYTES as u64)
        .checked_add(header.chunk.length)
        .and_then(|length| length.checked_add(u64::from(header.outboard_length)))
        .ok_or_else(|| corrupt("Peer cache file length overflow"))?;
    if header.outboard_length as usize > MAX_OUTBOARD_BYTES || metadata.len() != expected {
        return Err(corrupt(
            "Peer cache body or outboard trailer length differs",
        ));
    }
    Ok(())
}

fn read_disk_outboard(
    file: &mut File,
    header: &Header,
    budget: &Arc<OutboardBudget>,
) -> Result<Option<SharedOutboard>> {
    let length = header.outboard_length as usize;
    if length == 0 {
        return Ok(None);
    }
    if !(64..=MAX_OUTBOARD_BYTES).contains(&length) {
        return Err(corrupt("Peer cache outboard trailer exceeds bound"));
    }
    let Some(mut lease) = budget.try_reserve(ChunkOutboard::resident_charge(length)?) else {
        return Ok(None);
    };
    let mut encoded = Vec::new();
    if encoded.try_reserve_exact(length).is_err() {
        return Ok(None);
    }
    if lease
        .retain_capacity(ChunkOutboard::resident_charge(encoded.capacity())?)
        .is_err()
    {
        return Ok(None);
    }
    encoded.resize(length, 0);
    file.seek(SeekFrom::Start(
        MAX_HEADER_BYTES as u64 + header.chunk.length,
    ))
    .map_err(Error::from)?;
    file.read_exact(&mut encoded)
        .map_err(|_| corrupt("Peer cache outboard trailer is truncated"))?;
    match ChunkOutboard::from_encoded(&header.chunk, encoded, lease) {
        Ok(proof) => Ok(Some(proof)),
        Err(error) if error.code() == afs_error::IO_CAPACITY => Ok(None),
        Err(error) => Err(error),
    }
}

fn write_header(file: &mut File, header: &Header) -> Result<()> {
    let encoded =
        serde_json::to_vec(header).map_err(|_| invalid("Peer cache header cannot be encoded"))?;
    if encoded.len() > MAX_HEADER_BYTES - 12 {
        return Err(invalid("Peer cache header exceeds bound"));
    }
    let mut block = [0_u8; MAX_HEADER_BYTES];
    block[..8].copy_from_slice(MAGIC);
    block[8..12].copy_from_slice(&(encoded.len() as u32).to_le_bytes());
    block[12..12 + encoded.len()].copy_from_slice(&encoded);
    file.write_all(&block).map_err(Error::from)
}

fn report_work(
    metrics: &super::read_work::ReadWorkMetrics,
    current: PeerCacheHashWork,
    before: PeerCacheHashWork,
) {
    metrics.hash(
        "cache_segment_hash",
        current
            .cache_segment_bytes
            .saturating_sub(before.cache_segment_bytes),
    );
    metrics.hash(
        "cache_full_hash",
        current
            .cache_fullscan_bytes
            .saturating_sub(before.cache_fullscan_bytes),
    );
    metrics.hash(
        "outboard_parent_hash",
        current
            .outboard_parent_bytes
            .saturating_sub(before.outboard_parent_bytes),
    );
    metrics.hash(
        "segment_index_bytes",
        current
            .segment_index_bytes
            .saturating_sub(before.segment_index_bytes),
    );
}

fn elapsed_ns(started: Instant) -> u64 {
    u64::try_from(started.elapsed().as_nanos()).unwrap_or(u64::MAX)
}

fn report_stages(
    metrics: &super::read_work::ReadWorkMetrics,
    current: PeerCacheReadStages,
    before: PeerCacheReadStages,
) {
    for (stage, current, before) in [
        ("prepare", current.prepare_ns, before.prepare_ns),
        ("data_read", current.data_read_ns, before.data_read_ns),
        (
            "segment_verify",
            current.segment_verify_ns,
            before.segment_verify_ns,
        ),
        (
            "stream_read_verify",
            current.stream_read_verify_ns,
            before.stream_read_verify_ns,
        ),
        ("finalize", current.finalize_ns, before.finalize_ns),
    ] {
        let delta = current.saturating_sub(before);
        if delta != 0 {
            metrics.peer_cache_stage_ns(stage, delta);
        }
    }
}

impl Drop for PeerCacheOperation<'_> {
    fn drop(&mut self) {
        self.state.entries.clear();
        // Explicitly release the real fill allowance after all sink borrowers
        // have ended and before announcing this operation's drain completion.
        drop(self.fill.take());
        let had_metadata = self._metadata.is_some();
        drop(self._metadata.take());
        if let Ok(mut state) = self.cache.state.lock() {
            add_stats(&mut state.stats, &self.state.stats);
            state.operations = state.operations.saturating_sub(1);
            if had_metadata {
                state.operation_metadata_bytes =
                    state.operation_metadata_bytes.saturating_sub(8192);
            }
        }
        self.cache.changed.notify_all();
        self.cache.asynchronous_change.notify_waiters();
        self.cache.report_resources();
        if let Some(metrics) = &self.cache.read_work {
            report_work(
                metrics,
                self.state.stats.hash_work,
                PeerCacheHashWork::default(),
            );
            metrics.peer_cache_data_reads(self.state.stats.data_read_calls);
            metrics.copy("cache_to_output", self.state.stats.copied_bytes_total);
            report_stages(
                metrics,
                self.state.stats.read_stages,
                PeerCacheReadStages::default(),
            );
        }
    }
}

fn add_stats(total: &mut PeerCacheStats, local: &PeerCacheStats) {
    macro_rules! add { ($($field:ident),* $(,)?) => { $(total.$field = total.$field.saturating_add(local.$field);)* }; }
    add!(
        hits,
        misses,
        corruptions,
        hash_bytes_total,
        data_read_calls,
        copied_bytes_total
    );
    total.scratch_peak_bytes = total.scratch_peak_bytes.max(local.scratch_peak_bytes);
    total.hash_work.cache_segment_bytes += local.hash_work.cache_segment_bytes;
    total.hash_work.cache_fullscan_bytes += local.hash_work.cache_fullscan_bytes;
    total.hash_work.segment_index_bytes += local.hash_work.segment_index_bytes;
    total.hash_work.outboard_parent_bytes += local.hash_work.outboard_parent_bytes;
    total.read_stages.prepare_ns += local.read_stages.prepare_ns;
    total.read_stages.data_read_ns += local.read_stages.data_read_ns;
    total.read_stages.segment_verify_ns += local.read_stages.segment_verify_ns;
    total.read_stages.stream_read_verify_ns += local.read_stages.stream_read_verify_ns;
    total.read_stages.finalize_ns += local.read_stages.finalize_ns;
}

impl Drop for DiskPeerCache {
    fn drop(&mut self) {
        if let Some(metrics) = &self.read_work
            && let Ok(state) = self.state.get_mut()
        {
            metrics.peer_cache_metadata(0, state.stats.proof_metadata_peak_bytes);
        }
    }
}

fn read_header(file: &mut File) -> Result<(Header, u64)> {
    let mut prefix = [0_u8; 12];
    file.read_exact(&mut prefix)
        .map_err(|_| corrupt("Peer cache header is truncated"))?;
    if &prefix[..8] != MAGIC && &prefix[..8] != OLD_MAGIC {
        return Err(corrupt("Peer cache header magic differs"));
    }
    let count = u32::from_le_bytes([prefix[8], prefix[9], prefix[10], prefix[11]]) as usize;
    if count == 0 || count > MAX_HEADER_BYTES {
        return Err(corrupt("Peer cache header exceeds bound"));
    }
    let mut header = vec![0; count];
    file.read_exact(&mut header)
        .map_err(|_| corrupt("Peer cache header is truncated"))?;
    let header: Header =
        serde_json::from_slice(&header).map_err(|_| corrupt("Peer cache header is invalid"))?;
    if &prefix[..8] == MAGIC {
        if header.version != 2 || count > MAX_HEADER_BYTES - 12 {
            return Err(corrupt("Peer cache v2 header shape differs"));
        }
        let mut padding = [0_u8; MAX_HEADER_BYTES];
        let padding = &mut padding[..MAX_HEADER_BYTES - 12 - count];
        file.read_exact(padding)
            .map_err(|_| corrupt("Peer cache header padding is truncated"))?;
        if padding.iter().any(|byte| *byte != 0) {
            return Err(corrupt("Peer cache header padding differs"));
        }
        Ok((header, MAX_HEADER_BYTES as u64))
    } else if header.version == 1 {
        Ok((header, 12 + count as u64))
    } else {
        Err(corrupt("Peer cache legacy header shape differs"))
    }
}

fn validate_chunk(chunk: &ChunkObject) -> Result<()> {
    if chunk.id.0.is_empty()
        || chunk.id.0.len() > 512
        || chunk.length == 0
        || chunk.encoding != ChunkEncoding::Raw
        || chunk.content_digest.algorithm != DigestAlgorithm::Blake3
    {
        return Err(invalid(
            "Peer cache requires a bounded immutable BLAKE3 object",
        ));
    }
    Ok(())
}

fn reject_symlink_components(path: &Path) -> Result<()> {
    let mut prefix = PathBuf::new();
    for component in path.components() {
        if !matches!(component, Component::RootDir | Component::Normal(_)) {
            return Err(invalid("Peer cache directory is not normalized"));
        }
        prefix.push(component.as_os_str());
        match fs::symlink_metadata(&prefix) {
            Ok(metadata) if metadata.file_type().is_symlink() => {
                return Err(invalid("Peer cache path contains a symlink"));
            }
            Ok(_) => {}
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => {}
            Err(error) => return Err(Error::from(error)),
        }
    }
    Ok(())
}

fn open_private(path: &Path) -> Result<(File, fs::Metadata)> {
    let file = OpenOptions::new()
        .read(true)
        .custom_flags(libc::O_NOFOLLOW | libc::O_CLOEXEC | libc::O_NONBLOCK)
        .open(path)
        .map_err(Error::from)?;
    let metadata = file.metadata().map_err(Error::from)?;
    validate_private_file(&metadata)?;
    Ok((file, metadata))
}

fn validate_private_file(metadata: &fs::Metadata) -> Result<()> {
    if !metadata.is_file()
        || metadata.file_type().is_symlink()
        || metadata.mode() & 0o077 != 0
        || metadata.uid() != effective_uid()
        || metadata.nlink() != 1
    {
        return Err(invalid(
            "Peer cache file must be private, regular and not linked elsewhere",
        ));
    }
    Ok(())
}

fn effective_uid() -> u32 {
    // SAFETY: geteuid has no arguments and does not access borrowed memory.
    unsafe { libc::geteuid() }
}

fn fd_directory(root: &File) -> PathBuf {
    PathBuf::from(format!("/proc/self/fd/{}", root.as_raw_fd()))
}

fn allocated(metadata: &fs::Metadata) -> Result<u64> {
    metadata
        .blocks()
        .checked_mul(512)
        .ok_or_else(|| invalid("Peer cache allocated size overflow"))
}

fn file_identity(metadata: &fs::Metadata) -> (u64, u64, u64, i64, i64, i64, i64) {
    (
        metadata.dev(),
        metadata.ino(),
        metadata.len(),
        metadata.mtime(),
        metadata.mtime_nsec(),
        metadata.ctime(),
        metadata.ctime_nsec(),
    )
}

fn valid_pending_name(name: &str) -> bool {
    name.strip_prefix(&format!("{PENDING}-"))
        .is_some_and(|suffix| {
            suffix.len() == 16
                && suffix
                    .bytes()
                    .all(|byte| byte.is_ascii_digit() || (b'a'..=b'f').contains(&byte))
        })
}

fn valid_ready_name(name: &str) -> bool {
    name.strip_suffix(".ready").is_some_and(|key| {
        key.len() == 64
            && key
                .bytes()
                .all(|byte| byte.is_ascii_digit() || (b'a'..=b'f').contains(&byte))
    })
}

fn invalid(message: impl Into<String>) -> Error {
    Error::coded(afs_error::IO_INVALID, message)
}
fn unavailable(message: impl Into<String>) -> Error {
    Error::coded(afs_error::IO_UNAVAILABLE, message)
}
fn corrupt(message: impl Into<String>) -> Error {
    Error::coded(afs_error::NODE_TRANSFER_CORRUPT_DATA, message)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{dfs::OperationId, node::chunk::StagedChunk};
    use std::os::unix::fs::{PermissionsExt, symlink};

    fn options(directory: PathBuf) -> PeerCacheOptions {
        PeerCacheOptions {
            directory,
            capacity_bytes: 256 * 1024,
            namespace_id: NamespaceId::new("test-namespace"),
        }
    }

    fn pending_files(cache: &DiskPeerCache) -> Vec<PathBuf> {
        fs::read_dir(cache.directory())
            .unwrap()
            .map(|entry| entry.unwrap().path())
            .filter(|path| {
                path.file_name()
                    .and_then(|name| name.to_str())
                    .is_some_and(valid_pending_name)
            })
            .collect()
    }

    fn fill(cache: &DiskPeerCache, staged: &StagedChunk) -> Result<bool> {
        cache
            .operation()?
            .store_verified(&VerifiedPeerChunk::verify(
                &staged.chunk,
                staged.bytes(),
                cache.proof_budget(),
            )?)
    }

    #[test]
    fn r7_background_cache_admission_does_not_wait_or_evict_for_space() {
        let temp = tempfile::tempdir().unwrap();
        let cache = Arc::new(DiskPeerCache::open(options(temp.path().join("cache"))).unwrap());
        let staged = StagedChunk::new(OperationId::new("r7-background"), vec![17; 64 * 1024]);
        let mut operation = cache.operation().unwrap();
        let state = cache.state.lock().unwrap();
        assert!(cache.try_operation().unwrap().is_none());
        assert!(cache.try_fill_flight(&staged.chunk).unwrap().is_none());
        assert!(!operation.try_acquire_stream_fill().unwrap());
        drop(state);
        let waiting = FillWaiter::new(&cache).unwrap();
        assert!(cache.try_operation().unwrap().is_none());
        assert!(cache.try_fill_flight(&staged.chunk).unwrap().is_none());
        assert!(!operation.try_acquire_stream_fill().unwrap());
        drop(waiting);
        drop(operation);
        assert!(fill(&cache, &staged).unwrap());
        let used = cache.state.lock().unwrap().used_bytes;
        // A different object cannot evict this healthy entry to run ahead.
        let next = StagedChunk::new(OperationId::new("r7-next"), vec![29; 256 * 1024]);
        let mut operation = cache.try_operation().unwrap().unwrap();
        let sink = operation.try_begin_stream(&next.chunk).unwrap().unwrap();
        assert!(!sink.is_caching());
        drop(sink);
        drop(operation);
        assert_eq!(cache.try_contains_ready(&staged.chunk).unwrap(), Some(true));
        assert_eq!(cache.state.lock().unwrap().used_bytes, used);
        assert!(pending_files(&cache).is_empty());
    }

    #[test]
    fn disk_peer_cache_different_object_stream_fills_overlap_without_sharing_pending() {
        use std::{sync::mpsc, thread, time::Duration};

        let temp = tempfile::tempdir().unwrap();
        let mut config = options(temp.path().join("cache"));
        config.capacity_bytes = 1024 * 1024;
        let cache = DiskPeerCache::open(config).unwrap();
        let first = StagedChunk::new(OperationId::new("f2-first"), vec![17; 64 * 1024]);
        let second = StagedChunk::new(OperationId::new("f2-second"), vec![29; 64 * 1024]);
        let (first_ready_tx, first_ready_rx) = mpsc::channel();
        let (release_first_tx, release_first_rx) = mpsc::channel();
        let (second_ready_tx, second_ready_rx) = mpsc::channel();
        let (release_second_tx, release_second_rx) = mpsc::channel();
        let deadline = Duration::from_secs(5);
        let overlapped = thread::scope(|scope| {
            let cache_ref = &cache;
            let first = &first;
            let first_task = scope.spawn(move || {
                let mut operation = cache_ref.operation().unwrap();
                let mut sink = operation.begin_stream(&first.chunk).unwrap();
                assert!(sink.file.is_some());
                first_ready_tx.send(()).unwrap();
                release_first_rx.recv_timeout(deadline).unwrap();
                let mut out = [0; 8];
                let proof = crate::node::rpc::peer::complete_peer_disk_read_for_test(
                    &first.chunk,
                    first.bytes(),
                    0,
                    &mut out,
                    &mut sink,
                )
                .unwrap();
                assert!(sink.commit(proof).unwrap());
            });
            first_ready_rx.recv_timeout(deadline).unwrap();
            let cache_ref = &cache;
            let second = &second;
            let second_task = scope.spawn(move || {
                let mut operation = cache_ref.operation().unwrap();
                let mut sink = operation.begin_stream(&second.chunk).unwrap();
                assert!(sink.file.is_some());
                second_ready_tx.send(()).unwrap();
                release_second_rx.recv_timeout(deadline).unwrap();
                let mut out = [0; 8];
                let proof = crate::node::rpc::peer::complete_peer_disk_read_for_test(
                    &second.chunk,
                    second.bytes(),
                    0,
                    &mut out,
                    &mut sink,
                )
                .unwrap();
                assert!(sink.commit(proof).unwrap());
            });
            // Always release and join before asserting: the old single allowance
            // fails this progress contract without leaving a blocked test thread.
            let overlapped = second_ready_rx.recv_timeout(Duration::from_secs(2)).is_ok();
            release_first_tx.send(()).unwrap();
            if !overlapped {
                second_ready_rx.recv_timeout(deadline).unwrap();
            }
            release_second_tx.send(()).unwrap();
            first_task.join().unwrap();
            second_task.join().unwrap();
            overlapped
        });
        assert_eq!(cache.stats().unwrap().entries, 2);
        assert!(
            overlapped,
            "different-object stream fills waited for one global fill"
        );
        for (staged, byte) in [(&first, 17), (&second, 29)] {
            let mut out = [0; 8];
            assert!(
                cache
                    .operation()
                    .unwrap()
                    .read_range(&staged.chunk, 0, &mut out)
                    .unwrap()
            );
            assert_eq!(out, [byte; 8]);
        }
    }

    #[test]
    fn disk_peer_cache_same_object_follower_waits_for_its_single_flight() {
        use std::{sync::mpsc, thread, time::Duration};

        let temp = tempfile::tempdir().unwrap();
        let cache = DiskPeerCache::open(options(temp.path().join("cache"))).unwrap();
        let staged = StagedChunk::new(OperationId::new("f2-same"), vec![17; 64 * 1024]);
        let FillRole::Leader(mut leader) = cache.fill_flight(&staged.chunk, || Ok(())).unwrap()
        else {
            panic!("first request must lead");
        };
        let (entered_tx, entered_rx) = mpsc::channel();
        let (done_tx, done_rx) = mpsc::channel();
        thread::scope(|scope| {
            let follower = scope.spawn(|| {
                let outcome = cache
                    .fill_flight(&staged.chunk, || {
                        let _ = entered_tx.send(());
                        Ok(())
                    })
                    .unwrap();
                assert!(matches!(outcome, FillRole::Finished(FillOutcome::Uncached)));
                done_tx.send(()).unwrap();
            });
            entered_rx.recv_timeout(Duration::from_secs(2)).unwrap();
            assert!(done_rx.try_recv().is_err());
            assert_eq!(cache.state.lock().unwrap().flight_live, 1);
            leader.finish(FillOutcome::Uncached);
            done_rx.recv_timeout(Duration::from_secs(2)).unwrap();
            follower.join().unwrap();
        });
        drop(leader);
        assert_eq!(cache.state.lock().unwrap().flight_live, 0);
    }

    #[test]
    fn disk_peer_cache_stream_limit_cancel_and_legacy_exclusivity() {
        use std::{sync::mpsc, thread};
        let temp = tempfile::tempdir().unwrap();
        let cache = DiskPeerCache::open(options(temp.path().join("cache"))).unwrap();
        let mut first = cache.operation().unwrap();
        let mut second = cache.operation().unwrap();
        first.acquire_stream_fill(|| Ok(())).unwrap();
        second.acquire_stream_fill(|| Ok(())).unwrap();
        let deadline = Instant::now() + Duration::from_millis(30);
        let mut third = cache.operation().unwrap();
        assert!(
            third
                .acquire_stream_fill(|| {
                    if Instant::now() >= deadline {
                        Err(unavailable("cancelled third stream"))
                    } else {
                        Ok(())
                    }
                })
                .is_err()
        );
        assert_eq!(cache.state.lock().unwrap().stream_active, 2);
        let (checked_tx, checked_rx) = mpsc::channel();
        let (admitted_tx, admitted_rx) = mpsc::channel();
        thread::scope(|scope| {
            let cache_ref = &cache;
            let legacy = scope.spawn(move || {
                let mut operation = cache_ref.operation().unwrap();
                operation
                    .acquire_fill(|| {
                        let _ = checked_tx.send(());
                        Ok(())
                    })
                    .unwrap();
                admitted_tx.send(()).unwrap();
            });
            checked_rx.recv_timeout(Duration::from_secs(2)).unwrap();
            drop(first);
            assert!(
                admitted_rx.recv_timeout(Duration::from_millis(50)).is_err(),
                "legacy must wait for the final streaming owner"
            );
            drop(second);
            admitted_rx.recv_timeout(Duration::from_secs(2)).unwrap();
            legacy.join().unwrap();
        });
        assert_eq!(cache.state.lock().unwrap().stream_active, 0);
        let mut legacy = cache.operation().unwrap();
        legacy.acquire_fill(|| Ok(())).unwrap();
        let deadline = Instant::now() + Duration::from_millis(30);
        assert!(
            third
                .acquire_stream_fill(|| {
                    if Instant::now() >= deadline {
                        Err(unavailable("cancelled behind legacy"))
                    } else {
                        Ok(())
                    }
                })
                .is_err()
        );
        assert!(cache.state.lock().unwrap().fill_active);
        cache.close_admission();
        assert!(third.acquire_stream_fill(|| Ok(())).is_err());
        drop(legacy);
        assert!(!cache.state.lock().unwrap().fill_active);
    }

    #[test]
    fn disk_peer_cache_pending_generations_keep_failed_charge_and_other_fill_private() {
        let temp = tempfile::tempdir().unwrap();
        let mut config = options(temp.path().join("cache"));
        config.capacity_bytes = 512 * 1024;
        let cache = DiskPeerCache::open(config).unwrap();
        let first = StagedChunk::new(OperationId::new("f2-retained"), vec![17; 64 * 1024]);
        let second = StagedChunk::new(OperationId::new("f2-isolated"), vec![29; 64 * 1024]);
        let mut first_op = cache.operation().unwrap();
        let first_sink = first_op.begin_stream(&first.chunk).unwrap();
        let first_path = cache.pending_path(first_sink.pending_generation);
        let first_charge =
            cache.state.lock().unwrap().pending[&first_sink.pending_generation].charge;
        let mut second_op = cache.operation().unwrap();
        let mut second_sink = second_op.begin_stream(&second.chunk).unwrap();
        let second_path = cache.pending_path(second_sink.pending_generation);
        assert_ne!(first_path, second_path);
        assert!(first_path.exists() && second_path.exists());
        assert_eq!(cache.state.lock().unwrap().pending.len(), 2);
        assert!(cache.stats().unwrap().disk_bytes <= cache.options.capacity_bytes);
        cache.state.lock().unwrap().fail_remove = true;
        drop(first_sink);
        drop(first_op);
        cache.state.lock().unwrap().fail_remove = false;
        assert!(first_path.exists());
        let mut out = [0; 8];
        let proof = crate::node::rpc::peer::complete_peer_disk_read_for_test(
            &second.chunk,
            second.bytes(),
            0,
            &mut out,
            &mut second_sink,
        )
        .unwrap();
        assert!(second_sink.commit(proof).unwrap());
        drop(second_sink);
        drop(second_op);
        let state = cache.state.lock().unwrap();
        assert_eq!(state.pending_charge, first_charge);
        assert_eq!(state.pending.len(), 1);
        assert!(
            state.pending_metadata_charge > 0,
            "failed deletion retains its metadata lease"
        );
        assert_eq!(
            state.used_bytes,
            first_charge
                + state
                    .entries
                    .values()
                    .map(|entry| entry.charge)
                    .sum::<u64>()
        );
        drop(state);
        assert!(first_path.exists());
        assert!(!second_path.exists());
        cache.cleanup_pending().unwrap();
        assert!(!first_path.exists());
        assert_eq!(cache.state.lock().unwrap().pending_charge, 0);
    }

    #[test]
    fn disk_peer_cache_replaced_pending_is_preserved_without_touching_another_fill() {
        let temp = tempfile::tempdir().unwrap();
        let mut config = options(temp.path().join("cache"));
        config.capacity_bytes = 512 * 1024;
        let cache = DiskPeerCache::open(config).unwrap();
        let first = StagedChunk::new(OperationId::new("f2-replaced"), vec![17; 64 * 1024]);
        let second = StagedChunk::new(OperationId::new("f2-preserved"), vec![29; 64 * 1024]);
        let mut first_op = cache.operation().unwrap();
        let first_sink = first_op.begin_stream(&first.chunk).unwrap();
        let first_path = cache.pending_path(first_sink.pending_generation);
        let mut second_op = cache.operation().unwrap();
        let second_sink = second_op.begin_stream(&second.chunk).unwrap();
        let second_path = cache.pending_path(second_sink.pending_generation);
        fs::remove_file(&first_path).unwrap();
        OpenOptions::new()
            .write(true)
            .create_new(true)
            .mode(0o600)
            .open(&first_path)
            .unwrap()
            .write_all(b"foreign replacement")
            .unwrap();
        drop(first_sink);
        assert_eq!(fs::read(&first_path).unwrap(), b"foreign replacement");
        assert!(second_path.exists());
        assert!(cache.state.lock().unwrap().pending_charge > 0);
        drop(second_sink);
        assert!(!second_path.exists());
        assert_eq!(fs::read(&first_path).unwrap(), b"foreign replacement");
    }

    #[test]
    fn disk_peer_cache_blocked_fill_does_not_block_another_cached_hit() {
        use std::{sync::mpsc, thread, time::Duration};

        let temp = tempfile::tempdir().unwrap();
        let mut config = options(temp.path().join("cache"));
        config.capacity_bytes = 1024 * 1024;
        let cache = DiskPeerCache::open(config).unwrap();
        let cached = StagedChunk::new(OperationId::new("r5-cached-hit"), vec![73; 64 * 1024]);
        let incoming = StagedChunk::new(OperationId::new("r5-blocked-fill"), vec![91; 64 * 1024]);
        assert!(fill(&cache, &cached).unwrap());
        let (fill_ready_tx, fill_ready_rx) = mpsc::channel();
        let (release_fill_tx, release_fill_rx) = mpsc::channel();
        let (hit_started_tx, hit_started_rx) = mpsc::channel();
        let (hit_finished_tx, hit_finished_rx) = mpsc::channel();
        let deadline = Duration::from_secs(5);

        let hit_completed_before_release = thread::scope(|scope| {
            let cache_ref = &cache;
            let incoming_ref = &incoming;
            let fill_thread = scope.spawn(move || {
                let mut operation = cache_ref.operation().unwrap();
                let mut sink = operation.begin_stream(&incoming_ref.chunk).unwrap();
                assert!(
                    sink.file.is_some(),
                    "fixture must own an actual pending file"
                );
                fill_ready_tx.send(()).unwrap();
                // Model a Peer response paused before its body arrives. Keep the
                // real prepared sink alive, rather than duplicating cache locks.
                release_fill_rx.recv_timeout(deadline).unwrap();
                let mut output = [0; 8];
                let proof = crate::node::rpc::peer::complete_peer_disk_read_for_test(
                    &incoming_ref.chunk,
                    incoming_ref.bytes(),
                    0,
                    &mut output,
                    &mut sink,
                )
                .unwrap();
                assert_eq!(proof.private_output(), &[91; 8]);
                assert!(sink.commit(proof).unwrap());
            });
            fill_ready_rx.recv_timeout(deadline).unwrap();

            let cache_ref = &cache;
            let cached_ref = &cached;
            let hit_thread = scope.spawn(move || {
                hit_started_tx.send(()).unwrap();
                let mut output = [0; 4096];
                assert!(
                    cache_ref
                        .operation()
                        .unwrap()
                        .read_range(&cached_ref.chunk, 4096, &mut output)
                        .unwrap()
                );
                assert_eq!(output, [73; 4096]);
                hit_finished_tx.send(()).unwrap();
            });
            hit_started_rx.recv_timeout(deadline).unwrap();
            // This timeout is a bounded progress assertion, not a latency gate.
            // Release and join both threads before reporting the old lock bug.
            let completed = hit_finished_rx.recv_timeout(Duration::from_secs(2)).is_ok();
            release_fill_tx.send(()).unwrap();
            fill_thread.join().unwrap();
            hit_thread.join().unwrap();
            completed
        });
        assert_eq!(cache.stats().unwrap().entries, 2);
        assert!(
            hit_completed_before_release,
            "another object's cached hit waited for an unrelated in-flight fill"
        );
    }

    #[test]
    fn disk_peer_cache_retired_pin_keeps_disk_charge_and_blocks_same_key_replacement() {
        let temp = tempfile::tempdir().unwrap();
        let cache = DiskPeerCache::open(options(temp.path().join("cache"))).unwrap();
        let staged = StagedChunk::new(OperationId::new("r5-retired-pin"), vec![73; 64 * 1024]);
        assert!(fill(&cache, &staged).unwrap());
        let key = cache.key(&staged.chunk).unwrap();
        let generation = cache
            .state
            .lock()
            .unwrap()
            .entries
            .get(&key)
            .unwrap()
            .generation
            .clone();
        let pin = EntryPin {
            cache: &cache,
            generation: Some(generation),
        };
        let (mut file, before) = open_private(&cache.path(&key)).unwrap();
        let (_, body_start) = read_header(&mut file).unwrap();
        let charged = cache.stats().unwrap().disk_bytes;
        cache.retire(&key, pin.generation.as_ref(), true).unwrap();
        assert_eq!(cache.stats().unwrap().entries, 0);
        assert_eq!(cache.stats().unwrap().disk_bytes, charged);
        assert!(
            !fill(&cache, &staged).unwrap(),
            "cannot reuse a pinned generation's ready path"
        );
        let mut out = [0; 4096];
        scan_body(
            &mut file,
            body_start,
            &staged.chunk,
            0,
            &mut out,
            &mut PeerCacheStats::default(),
            None,
        )
        .unwrap();
        assert_eq!(out, [73; 4096]);
        assert_eq!(
            file_identity(&before),
            file_identity(&file.metadata().unwrap())
        );
        drop(file);
        drop(pin);
        assert!(!cache.path(&key).exists());
        assert_eq!(cache.stats().unwrap().disk_bytes, 0);
        assert!(cache.state.lock().unwrap().retired.is_empty());
        assert!(fill(&cache, &staged).unwrap());
    }

    #[test]
    fn disk_peer_cache_close_wakes_fill_waiter_without_refunding_active_fill() {
        use std::{sync::mpsc, thread, time::Duration};
        let temp = tempfile::tempdir().unwrap();
        let cache = DiskPeerCache::open(options(temp.path().join("cache"))).unwrap();
        let mut owner = cache.operation().unwrap();
        owner.acquire_fill(|| Ok(())).unwrap();
        let (started_tx, started_rx) = mpsc::channel();
        thread::scope(|scope| {
            let waiter = scope.spawn(|| {
                let mut operation = cache.operation().unwrap();
                started_tx.send(()).unwrap();
                operation.acquire_fill(|| Ok(()))
            });
            started_rx.recv_timeout(Duration::from_secs(2)).unwrap();
            cache.close_admission();
            assert!(waiter.join().unwrap().is_err());
            assert!(cache.state.lock().unwrap().fill_active);
        });
        drop(owner);
        assert!(!cache.state.lock().unwrap().fill_active);
        cache.shutdown().unwrap();
        let state = cache.state.lock().unwrap();
        assert_eq!(state.operations, 0);
        assert!(state.flights.is_empty());
    }

    #[test]
    fn disk_peer_cache_flight_drop_and_repeated_rounds_leave_no_slots() {
        let temp = tempfile::tempdir().unwrap();
        let cache = DiskPeerCache::open(options(temp.path().join("cache"))).unwrap();
        let staged = StagedChunk::new(OperationId::new("r5-flight-drop"), vec![73; 64 * 1024]);
        for _ in 0..32 {
            let FillRole::Leader(mut leader) = cache.fill_flight(&staged.chunk, || Ok(())).unwrap()
            else {
                panic!("fresh flight must have one leader");
            };
            leader.finish(FillOutcome::Failed(unavailable("bounded shared failure")));
            let mut checks = 0;
            // Same-thread follower observes the terminal data outcome, while
            // retaining a separate authorization callback.
            let check_count = std::cell::Cell::new(0);
            match cache
                .fill_flight(&staged.chunk, || {
                    check_count.set(check_count.get() + 1);
                    Ok(())
                })
                .unwrap()
            {
                FillRole::Finished(FillOutcome::Failed(error)) => {
                    assert_eq!(error, unavailable("bounded shared failure"));
                    checks += check_count.get();
                }
                _ => panic!("follower must observe the same shared data failure"),
            }
            assert!(checks >= 2);
            drop(leader);
            let state = cache.state.lock().unwrap();
            assert_eq!(state.flight_live, 0);
            assert!(state.flights.is_empty());
        }
        let leader = cache.fill_flight(&staged.chunk, || Ok(())).unwrap();
        drop(leader);
        assert_eq!(cache.state.lock().unwrap().flight_live, 0);
    }

    #[test]
    fn disk_peer_cache_failed_retired_delete_preserves_slots_and_index_until_retry() {
        let temp = tempfile::tempdir().unwrap();
        let cache = DiskPeerCache::open(options(temp.path().join("cache"))).unwrap();
        let staged = StagedChunk::new(
            OperationId::new("r5-stop-delete-failure"),
            vec![73; 64 * 1024],
        );
        assert!(fill(&cache, &staged).unwrap());
        let key = cache.key(&staged.chunk).unwrap();
        let before = cache.stats().unwrap().disk_bytes;
        cache.state.lock().unwrap().fail_remove = true;
        assert!(cache.retire(&key, None, true).is_err());
        assert!(cache.shutdown().is_err());
        {
            let state = cache.state.lock().unwrap();
            assert!(state.index_lease.is_some());
            assert_eq!(state.retired.len(), 1);
            assert_eq!(state.retired_disk_bytes, before);
            assert_eq!(state.used_bytes, before);
        }
        cache.state.lock().unwrap().fail_remove = false;
        cache.shutdown().unwrap();
        let state = cache.state.lock().unwrap();
        assert!(state.retired.is_empty());
        assert!(state.index_lease.is_none());
        assert_eq!(state.retired_disk_bytes, 0);
        assert_eq!(state.entry_metadata_bytes, 0);
        assert_eq!(state.used_bytes, 0);
    }

    #[test]
    fn disk_peer_cache_pinned_foreign_replacement_is_never_unlinked_or_refunded() {
        let temp = tempfile::tempdir().unwrap();
        let cache = DiskPeerCache::open(options(temp.path().join("cache"))).unwrap();
        let staged = StagedChunk::new(OperationId::new("r5-foreign-retired"), vec![73; 64 * 1024]);
        assert!(fill(&cache, &staged).unwrap());
        let key = cache.key(&staged.chunk).unwrap();
        let generation = cache
            .state
            .lock()
            .unwrap()
            .entries
            .get(&key)
            .unwrap()
            .generation
            .clone();
        let pin = EntryPin {
            cache: &cache,
            generation: Some(generation),
        };
        let before = cache.stats().unwrap().disk_bytes;
        cache.retire(&key, pin.generation.as_ref(), true).unwrap();
        let original = temp.path().join("retired-original");
        fs::rename(cache.path(&key), &original).unwrap();
        fs::write(cache.path(&key), b"foreign replacement").unwrap();
        drop(pin);
        assert_eq!(fs::read(cache.path(&key)).unwrap(), b"foreign replacement");
        assert_eq!(cache.stats().unwrap().disk_bytes, before);
        assert_eq!(cache.state.lock().unwrap().retired.len(), 1);
        fs::remove_file(cache.path(&key)).unwrap();
        fs::rename(original, cache.path(&key)).unwrap();
        cache.cleanup_retired().unwrap();
        assert_eq!(cache.stats().unwrap().disk_bytes, 0);
    }

    #[test]
    fn disk_peer_cache_flights_are_transient_bounded_and_close_wakes_admission() {
        use std::{sync::mpsc, thread, time::Duration};
        let temp = tempfile::tempdir().unwrap();
        let cache = DiskPeerCache::open(options(temp.path().join("cache"))).unwrap();
        let resident_before = cache.proof_budget.resident_used();
        let mut leaders = Vec::new();
        for index in 0..8 {
            let staged = StagedChunk::new(
                OperationId::new(format!("r5-flight-{index}")),
                vec![index; 64],
            );
            leaders.push(cache.fill_flight(&staged.chunk, || Ok(())).unwrap());
        }
        assert_eq!(cache.state.lock().unwrap().flight_live, 8);
        assert_eq!(cache.proof_budget.resident_used(), resident_before);
        let ninth = StagedChunk::new(OperationId::new("r5-flight-ninth"), vec![91; 64]);
        let (started_tx, started_rx) = mpsc::channel();
        thread::scope(|scope| {
            let waiting = scope.spawn(|| {
                cache.fill_flight(&ninth.chunk, || {
                    started_tx.send(()).unwrap();
                    Ok(())
                })
            });
            started_rx.recv_timeout(Duration::from_secs(2)).unwrap();
            cache.close_admission();
            assert!(waiting.join().unwrap().is_err());
        });
        drop(leaders);
        cache.shutdown().unwrap();
        assert_eq!(cache.proof_budget.used(), 0);
    }

    #[test]
    fn disk_peer_cache_small_hit_verifies_only_32k_coverage_group() {
        let temp = tempfile::tempdir().unwrap();
        let mut config = options(temp.path().join("cache"));
        config.capacity_bytes = 8 * 1024 * 1024;
        let cache = DiskPeerCache::open(config).unwrap();
        let staged = StagedChunk::new(
            OperationId::new("outboard-small-hit"),
            vec![73; 4 * 1024 * 1024],
        );
        assert!(fill(&cache, &staged).unwrap());
        let before = cache.stats().unwrap();
        let mut out = [0; 4096];
        assert!(
            cache
                .operation()
                .unwrap()
                .read_range(&staged.chunk, 4096, &mut out)
                .unwrap()
        );
        let after = cache.stats().unwrap();
        assert_eq!(out, [73; 4096]);
        assert_eq!(after.hash_bytes_total - before.hash_bytes_total, 32 * 1024);
        assert_eq!(
            after.hash_work.cache_fullscan_bytes,
            before.hash_work.cache_fullscan_bytes
        );
    }

    #[test]
    fn disk_peer_cache_v2_body_has_fixed_offset_and_trailer() {
        let temp = tempfile::tempdir().unwrap();
        let cache = DiskPeerCache::open(options(temp.path().join("cache"))).unwrap();
        let staged = StagedChunk::new(OperationId::new("outboard-format"), vec![39; 64 * 1024]);
        assert!(fill(&cache, &staged).unwrap());
        let bytes = fs::read(cache.path(&cache.key(&staged.chunk).unwrap())).unwrap();
        assert_eq!(&bytes[..8], b"AFSPC002");
        assert_eq!(&bytes[4096..4096 + staged.bytes().len()], staged.bytes());
        assert!(bytes.len() >= 4096 + staged.bytes().len() + 64);
    }

    #[test]
    fn disk_peer_cache_rejects_half_or_forged_outboard_trailer() {
        for truncated in [false, true] {
            let temp = tempfile::tempdir().unwrap();
            let cache = DiskPeerCache::open(options(temp.path().join("cache"))).unwrap();
            let staged =
                StagedChunk::new(OperationId::new("outboard-corrupt"), vec![47; 64 * 1024]);
            assert!(fill(&cache, &staged).unwrap());
            let path = cache.path(&cache.key(&staged.chunk).unwrap());
            let before = file_identity(&fs::metadata(&path).unwrap());
            let mut bytes = fs::read(&path).unwrap();
            if truncated {
                bytes.truncate(bytes.len() - 16);
            } else {
                *bytes.last_mut().unwrap() ^= 1;
            }
            fs::write(&path, bytes).unwrap();
            // Cached proof reuse is valid until file identity changes. Make
            // trailer revalidation explicit instead of relying on tick timing.
            File::open(&path)
                .unwrap()
                .set_modified(std::time::SystemTime::UNIX_EPOCH)
                .unwrap();
            assert_ne!(before, file_identity(&fs::metadata(&path).unwrap()));
            let mut out = [0; 19];
            assert!(
                !cache
                    .operation()
                    .unwrap()
                    .read_range(&staged.chunk, 0, &mut out)
                    .unwrap()
            );
            assert!(!path.exists());
            assert_eq!(cache.stats().unwrap().entries, 0);
        }
    }

    #[test]
    fn disk_peer_cache_outboard_never_replaces_trusted_meta_root() {
        let temp = tempfile::tempdir().unwrap();
        let cache = DiskPeerCache::open(options(temp.path().join("cache"))).unwrap();
        let staged = StagedChunk::new(OperationId::new("outboard-meta-root"), vec![49; 64 * 1024]);
        assert!(fill(&cache, &staged).unwrap());
        let held = cache
            .state
            .lock()
            .unwrap()
            .entries
            .values()
            .next()
            .unwrap()
            .proof
            .as_ref()
            .unwrap()
            .outboard
            .clone();
        let mut changed = staged.chunk.clone();
        changed.content_digest.bytes[0] ^= 1;
        assert!(validate_outboard(&changed, &held).is_err());
        let mut output = [0; 19];
        assert!(
            !cache
                .operation()
                .unwrap()
                .read_range(&changed, 0, &mut output)
                .unwrap()
        );
        assert_eq!(output, [0; 19]);
    }

    #[test]
    fn disk_peer_cache_drop_releases_entries_before_index_reservation() {
        let temp = tempfile::tempdir().unwrap();
        let budget = OutboardBudget::new(MAX_PEER_CACHE_PROOF_BYTES as usize);
        let observed = Arc::new(std::sync::atomic::AtomicUsize::new(usize::MAX));
        let cache = DiskPeerCache::open_with_outboard_budget(
            options(temp.path().join("cache")),
            budget.clone(),
        )
        .unwrap();
        let staged = StagedChunk::new(OperationId::new("outboard-drop-order"), vec![79; 64 * 1024]);
        assert!(fill(&cache, &staged).unwrap());
        cache
            .state
            .lock()
            .unwrap()
            .entries
            .values_mut()
            .next()
            .unwrap()
            .drop_observation = Some(DropObservation {
            budget: budget.clone(),
            observed: observed.clone(),
        });
        drop(cache);
        let pointers =
            MAX_PEER_CACHE_ENTRIES * std::mem::size_of::<Option<Box<VerifiedDiskOutboard>>>();
        assert_eq!(
            observed.load(std::sync::atomic::Ordering::SeqCst),
            pointers,
            "entry destructor must still observe its pointer-index reservation"
        );
        assert_eq!(budget.used(), 0);
    }

    #[test]
    fn disk_peer_cache_shared_budget_pressure_falls_back_without_changing_read_result() {
        let temp = tempfile::tempdir().unwrap();
        let budget = OutboardBudget::new(MAX_PEER_CACHE_PROOF_BYTES as usize);
        let cache = DiskPeerCache::open_with_outboard_budget(
            options(temp.path().join("cache")),
            budget.clone(),
        )
        .unwrap();
        let staged = StagedChunk::new(OperationId::new("outboard-pressure"), vec![67; 64 * 1024]);
        assert!(fill(&cache, &staged).unwrap());
        let held = budget.try_reserve(budget.limit() - budget.used()).unwrap();
        let mut out = [0; 19];
        assert!(
            cache
                .operation()
                .unwrap()
                .read_range(&staged.chunk, 0, &mut out)
                .unwrap()
        );
        assert_eq!(out, [67; 19]);
        assert_eq!(
            cache.stats().unwrap().hash_work.cache_fullscan_bytes,
            staged.chunk.length
        );
        assert_eq!(budget.used(), budget.limit());
        drop(held);
    }

    #[test]
    fn pr60_natural_resident_pressure_preserves_shared_working_headroom() {
        use std::sync::{
            Barrier,
            atomic::{AtomicUsize, Ordering},
        };
        let temp = tempfile::tempdir().unwrap();
        let budget = OutboardBudget::new(MAX_PEER_CACHE_PROOF_BYTES as usize);
        let mut config = options(temp.path().join("cache"));
        config.capacity_bytes = 64 * 1024 * 1024;
        let cache = DiskPeerCache::open_with_outboard_budget(config, budget.clone()).unwrap();
        let index_charge = budget.used();
        let first = StagedChunk::new(OperationId::new("resident-first"), vec![0; 4096]);
        assert!(fill(&cache, &first).unwrap());
        let one_proof = budget.used() - index_charge;
        assert!(one_proof > 0);
        // Use actual natural proof ownership, not an injected global reservation.
        // Unique 4KiB objects keep real body data below 16MiB / 4096 entries.
        let objects = (budget.limit() / 2) / one_proof + 16;
        assert!(objects < MAX_PEER_CACHE_ENTRIES);
        for index in 1..objects {
            let mut bytes = vec![0; 4096];
            bytes[..8].copy_from_slice(&(index as u64).to_le_bytes());
            let staged = StagedChunk::new(OperationId::new(format!("resident-{index}")), bytes);
            assert!(fill(&cache, &staged).unwrap());
        }
        assert_eq!(
            cache.stats().unwrap().entries,
            objects,
            "proof retention may retire metadata, never healthy body entries"
        );
        assert!(
            budget.used() <= budget.limit() / 2,
            "natural resident proofs consumed the working half: used={} total={}",
            budget.used(),
            budget.limit()
        );

        let ready = Arc::new(Barrier::new(9));
        let release = Arc::new(Barrier::new(9));
        let peak = Arc::new(AtomicUsize::new(budget.used()));
        let mut threads = Vec::new();
        for _ in 0..8 {
            let (budget, ready, release, peak) =
                (budget.clone(), ready.clone(), release.clone(), peak.clone());
            threads.push(std::thread::spawn(move || {
                let work = budget.try_reserve(64 * 1024);
                peak.fetch_max(budget.used(), Ordering::AcqRel);
                ready.wait();
                release.wait();
                work.is_some()
            }));
        }
        ready.wait();
        release.wait();
        let admitted: Vec<_> = threads
            .into_iter()
            .map(|thread| thread.join().unwrap())
            .collect();
        assert!(
            admitted.into_iter().all(|value| value),
            "bounded 512KiB working admission must remain available after resident pressure"
        );
        assert!(peak.load(Ordering::Acquire) <= budget.limit());
        assert!(budget.used() <= budget.limit() / 2);
        let mut output = [99; 19];
        assert!(
            cache
                .operation()
                .unwrap()
                .read_range(&first.chunk, 0, &mut output)
                .unwrap()
        );
        assert_eq!(output, [0; 19]);
        cache.shutdown().unwrap();
        assert_eq!(
            budget.used(),
            0,
            "resident and working ownership must both retire"
        );
    }

    #[test]
    fn disk_peer_cache_only_retires_recognized_legacy_disposable_file() {
        let temp = tempfile::tempdir().unwrap();
        let config = options(temp.path().join("cache"));
        let staged = StagedChunk::new(OperationId::new("legacy-cache"), vec![51; 64 * 1024]);
        let cache = DiskPeerCache::open(config.clone()).unwrap();
        let key = cache.key(&staged.chunk).unwrap();
        let path = cache.path(&key);
        let header = serde_json::to_vec(&Header {
            version: 1,
            namespace_id: config.namespace_id.clone(),
            chunk: staged.chunk.clone(),
            outboard_length: 0,
        })
        .unwrap();
        let mut file = OpenOptions::new()
            .create_new(true)
            .write(true)
            .mode(0o600)
            .open(&path)
            .unwrap();
        file.write_all(OLD_MAGIC).unwrap();
        file.write_all(&(header.len() as u32).to_le_bytes())
            .unwrap();
        file.write_all(&header).unwrap();
        file.write_all(staged.bytes()).unwrap();
        drop(file);
        let persistent = temp.path().join("authoritative-chunk");
        fs::write(&persistent, staged.bytes()).unwrap();
        drop(cache);
        let cache = DiskPeerCache::open(config).unwrap();
        assert_eq!(cache.stats().unwrap().entries, 0);
        assert!(!cache.path(&key).exists());
        assert_eq!(fs::read(persistent).unwrap(), staged.bytes());
    }

    #[test]
    fn disk_peer_cache_foreign_ledger_proof_cannot_bypass_node_budget() {
        let temp = tempfile::tempdir().unwrap();
        let budget = OutboardBudget::new(MAX_PEER_CACHE_PROOF_BYTES as usize);
        let cache = DiskPeerCache::open_with_outboard_budget(
            options(temp.path().join("cache")),
            budget.clone(),
        )
        .unwrap();
        let staged = StagedChunk::new(
            OperationId::new("outboard-other-ledger"),
            vec![41; 64 * 1024],
        );
        let foreign = OutboardBudget::new(MAX_PEER_CACHE_PROOF_BYTES as usize);
        let verified = VerifiedPeerChunk::verify(&staged.chunk, staged.bytes(), foreign).unwrap();
        assert!(
            cache
                .operation()
                .unwrap()
                .store_verified(&verified)
                .unwrap()
        );
        assert!(
            cache
                .state
                .lock()
                .unwrap()
                .entries
                .values()
                .all(|entry| entry.proof.is_none())
        );
        let mut out = [0; 9];
        assert!(
            cache
                .operation()
                .unwrap()
                .read_range(&staged.chunk, 0, &mut out)
                .unwrap()
        );
        assert_eq!(out, [41; 9]);
        assert_eq!(
            cache.stats().unwrap().hash_work.cache_fullscan_bytes,
            staged.chunk.length
        );
        assert!(budget.used() <= budget.limit());
    }

    #[test]
    fn peer_stream_sink_stays_private_and_charges_pending_before_body() {
        let temp = tempfile::tempdir().unwrap();
        let cache = DiskPeerCache::open(options(temp.path().join("cache"))).unwrap();
        let staged = StagedChunk::new(OperationId::new("stream-pending"), vec![37; 64 * 1024]);
        let mut operation = cache.operation().unwrap();
        let mut sink = operation.begin_stream(&staged.chunk).unwrap();
        assert!(
            cache.state.lock().unwrap().pending_charge > 0,
            "private stream file must hold its disk reservation before body receipt"
        );
        sink.write(0, staged.bytes()).unwrap();
        assert!(
            cache.state.lock().unwrap().entries.is_empty(),
            "body completion alone cannot publish Ready"
        );
    }

    #[test]
    fn peer_stream_sink_complete_proof_stays_private_until_commit() {
        let temp = tempfile::tempdir().unwrap();
        let cache = DiskPeerCache::open(options(temp.path().join("cache"))).unwrap();
        let staged = StagedChunk::new(OperationId::new("stream-commit"), vec![37; 64 * 1024]);
        let mut out = [0; 19];
        {
            let mut operation = cache.operation().unwrap();
            let mut sink = operation.begin_stream(&staged.chunk).unwrap();
            let proof = crate::node::rpc::peer::complete_peer_disk_read_for_test(
                &staged.chunk,
                staged.bytes(),
                63,
                &mut out,
                &mut sink,
            )
            .unwrap();
            assert_eq!(proof.private_output(), &[37; 19]);
            assert!(cache.state.lock().unwrap().entries.is_empty());
            assert!(sink.commit(proof).unwrap());
        }
        assert_eq!(out, [37; 19]);
        assert_eq!(cache.stats().unwrap().entries, 1);
        assert!(
            cache
                .operation()
                .unwrap()
                .read_range(&staged.chunk, 63, &mut out)
                .unwrap()
        );
    }

    #[test]
    fn peer_stream_completed_disk_receipt_cannot_commit_another_sink() {
        let temp = tempfile::tempdir().unwrap();
        let cache_a = DiskPeerCache::open(options(temp.path().join("a"))).unwrap();
        let cache_b = DiskPeerCache::open(options(temp.path().join("b"))).unwrap();
        let staged = StagedChunk::new(OperationId::new("receipt-replay"), vec![19; 64 * 1024]);
        let mut operation_a = cache_a.operation().unwrap();
        let mut operation_b = cache_b.operation().unwrap();
        let mut sink_a = operation_a.begin_stream(&staged.chunk).unwrap();
        let mut sink_b = operation_b.begin_stream(&staged.chunk).unwrap();
        let mut out = [0; 8];
        let proof = crate::node::rpc::peer::complete_peer_disk_read_for_test(
            &staged.chunk,
            staged.bytes(),
            0,
            &mut out,
            &mut sink_a,
        )
        .unwrap();
        sink_b.write(0, staged.bytes()).unwrap();
        assert!(sink_b.commit(proof).is_err());
        assert!(cache_b.state.lock().unwrap().entries.is_empty());
    }

    #[test]
    fn peer_stream_sink_quota_or_write_failure_keeps_verified_output_without_ready() {
        for write_failure in [false, true] {
            let temp = tempfile::tempdir().unwrap();
            let mut config = options(temp.path().join("cache"));
            if !write_failure {
                config.capacity_bytes = 4096;
            }
            let cache = DiskPeerCache::open(config).unwrap();
            let staged = StagedChunk::new(OperationId::new("stream-degraded"), vec![19; 64 * 1024]);
            let mut out = [0; 31];
            {
                let mut operation = cache.operation().unwrap();
                cache.state.lock().unwrap().fail_stream_write = write_failure;
                let mut sink = operation.begin_stream(&staged.chunk).unwrap();
                let proof = crate::node::rpc::peer::complete_peer_disk_read_for_test(
                    &staged.chunk,
                    staged.bytes(),
                    2,
                    &mut out,
                    &mut sink,
                )
                .unwrap();
                assert_eq!(proof.private_output(), &[19; 31]);
                assert!(!sink.commit(proof).unwrap());
            }
            assert_eq!(out, [19; 31]);
            assert_eq!(cache.stats().unwrap().entries, 0);
            assert_eq!(cache.state.lock().unwrap().pending_charge, 0);
        }
    }

    #[test]
    fn peer_stream_sink_unknown_created_file_and_failed_cleanup_remain_charged() {
        let temp = tempfile::tempdir().unwrap();
        let cache = DiskPeerCache::open(options(temp.path().join("cache"))).unwrap();
        let staged = StagedChunk::new(OperationId::new("stream-unidentified"), vec![19; 64 * 1024]);
        {
            let mut operation = cache.operation().unwrap();
            cache.state.lock().unwrap().fail_stream_metadata = true;
            let sink = operation.begin_stream(&staged.chunk).unwrap();
            assert!(sink.file.is_none());
            assert!(cache.state.lock().unwrap().pending_charge > 0);
        }
        let state = cache.state.lock().unwrap();
        assert!(state.pending_charge > 0);
        assert_eq!(state.used_bytes, state.pending_charge);
        assert_eq!(pending_files(&cache).len(), 1);
    }

    #[test]
    fn peer_stream_sink_observed_extra_blocks_stay_charged_after_cleanup_failure() {
        let temp = tempfile::tempdir().unwrap();
        let cache = DiskPeerCache::open(options(temp.path().join("cache"))).unwrap();
        let staged = StagedChunk::new(OperationId::new("stream-charge"), vec![19; 64 * 1024]);
        let actual;
        {
            let mut operation = cache.operation().unwrap();
            let mut sink = operation.begin_stream(&staged.chunk).unwrap();
            let mut out = [0; 2];
            let proof = crate::node::rpc::peer::complete_peer_disk_read_for_test(
                &staged.chunk,
                staged.bytes(),
                2,
                &mut out,
                &mut sink,
            )
            .unwrap();
            actual = allocated(&sink.file.as_ref().unwrap().metadata().unwrap()).unwrap();
            cache.state.lock().unwrap().used_bytes = actual - 512;
            {
                let mut state = cache.state.lock().unwrap();
                state.pending_charge = actual - 512;
                state
                    .pending
                    .get_mut(&sink.pending_generation)
                    .unwrap()
                    .charge = actual - 512;
            }
            cache.state.lock().unwrap().fail_remove = true;
            assert!(sink.commit(proof).is_err());
            assert_eq!(cache.state.lock().unwrap().used_bytes, actual);
            assert_eq!(cache.state.lock().unwrap().pending_charge, actual);
        }
        assert_eq!(cache.state.lock().unwrap().used_bytes, actual);
        assert_eq!(pending_files(&cache).len(), 1);
    }

    #[test]
    fn coarse_cache_diagnostics_separate_misses_from_verified_hit_attempts() {
        let temp = tempfile::tempdir().unwrap();
        let registry = afs_metrics::Registry::new();
        let metrics = super::super::read_work::ReadWorkMetrics::register(&registry)
            .unwrap()
            .with_read_diagnostics(true);
        let cache = DiskPeerCache::open(options(temp.path().join("cache")))
            .unwrap()
            .with_read_work_metrics(metrics.clone());
        let staged = StagedChunk::new(OperationId::new("diagnostic-hit"), vec![37; 64 * 1024]);
        let mut out = [9; 19];
        assert!(
            !cache
                .operation()
                .unwrap()
                .read_range(&staged.chunk, 63, &mut out)
                .unwrap()
        );
        assert_eq!(out, [9; 19]);
        assert!(fill(&cache, &staged).unwrap());
        assert!(
            cache
                .operation()
                .unwrap()
                .read_range(&staged.chunk, 63, &mut out)
                .unwrap()
        );
        assert_eq!(out, [37; 19]);
        let snapshot = metrics.diagnostics_snapshot().unwrap();
        assert!(snapshot.records.is_empty());
        assert_eq!(snapshot.overflow, 0);
        let lock = snapshot
            .coarse_phases
            .iter()
            .find(|phase| phase.phase == "cache_lock_wait")
            .unwrap();
        assert_eq!(lock.calls, 3);
        let hit = snapshot
            .coarse_phases
            .iter()
            .find(|phase| phase.phase == "cache_hit_read_verify")
            .unwrap();
        assert_eq!(hit.calls, 1);
        assert_eq!(hit.bytes, out.len() as u64);
        // The earlier miss consumes no compact object slot.
        assert_eq!(snapshot.object_records.len(), 1);
        assert_eq!(snapshot.object_overflow, 0);
        assert!(snapshot.object_records[0].completed);
        assert!(snapshot.object_records[0].success);
        assert_eq!(
            snapshot.object_records[0].stage,
            super::super::read_work::ReadObjectDiagnosticStage::CacheHitReadVerify
        );
    }

    #[test]
    fn disk_peer_cache_admits_verified_object_and_reads_requested_range() {
        let temp = tempfile::tempdir().unwrap();
        let cache = DiskPeerCache::open(options(temp.path().join("cache"))).unwrap();
        let staged = StagedChunk::new(OperationId::new("cache-read"), vec![37; 64 * 1024]);
        assert!(fill(&cache, &staged).unwrap());
        let mut out = [0; 19];
        assert!(
            cache
                .operation()
                .unwrap()
                .read_range(&staged.chunk, 63, &mut out)
                .unwrap()
        );
        assert_eq!(out, [37; 19]);
        let stats = cache.stats().unwrap();
        assert_eq!(stats.hash_bytes_total, COVERAGE_GROUP_BYTES as u64);
        assert!(stats.scratch_peak_bytes <= (2 * COVERAGE_GROUP_BYTES + MAX_OUTBOARD_BYTES) as u64);
        assert_eq!(stats.entries, 1);
        assert!(stats.disk_bytes >= staged.chunk.length);
    }

    #[test]
    fn disk_peer_cache_aligned_range_reports_private_range_validator_work() {
        let temp = tempfile::tempdir().unwrap();
        let mut config = options(temp.path().join("cache"));
        config.capacity_bytes = 8 * 1024 * 1024;
        let cache = DiskPeerCache::open(config).unwrap();
        let staged = StagedChunk::new(
            OperationId::new("direct-private-segment"),
            vec![73; 4 * PEER_CACHE_SEGMENT_BYTES],
        );
        assert!(fill(&cache, &staged).unwrap());
        let before = cache.stats().unwrap();
        let mut out = vec![0; PEER_CACHE_SEGMENT_BYTES];
        assert!(
            cache
                .operation()
                .unwrap()
                .read_range(&staged.chunk, PEER_CACHE_SEGMENT_BYTES as u64, &mut out)
                .unwrap()
        );
        assert!(out.iter().all(|byte| *byte == 73));
        let after = cache.stats().unwrap();
        assert_eq!(
            (
                after.copied_bytes_total - before.copied_bytes_total,
                after.data_read_calls - before.data_read_calls
            ),
            // One aligned body read plus one copy into the official validator's
            // scratch; unaligned ranges retain the separate codec fallback.
            (PEER_CACHE_SEGMENT_BYTES as u64, 1),
        );
        assert_eq!(
            after.hash_bytes_total - before.hash_bytes_total,
            PEER_CACHE_SEGMENT_BYTES as u64
        );
        assert!(after.read_stages.prepare_ns > before.read_stages.prepare_ns);
        assert!(after.read_stages.data_read_ns > before.read_stages.data_read_ns);
        assert!(after.read_stages.segment_verify_ns > before.read_stages.segment_verify_ns);
        assert!(after.read_stages.finalize_ns > before.read_stages.finalize_ns);
        assert!(after.read_stages.stream_read_verify_ns > before.read_stages.stream_read_verify_ns);
    }

    #[test]
    fn disk_peer_cache_short_file_fails_preflight_without_body_io_or_hashing() {
        let mut file = tempfile::tempfile().unwrap();
        file.write_all(&[79; 17]).unwrap();
        let mut private = vec![0; PEER_CACHE_SEGMENT_BYTES];
        let mut stats = PeerCacheStats::default();

        let staged = StagedChunk::new(
            OperationId::new("outboard-short-file"),
            vec![79; PEER_CACHE_SEGMENT_BYTES],
        );
        let proof = build_borrowed(
            staged.bytes(),
            &OutboardBudget::new(MAX_PEER_CACHE_PROOF_BYTES as usize),
        )
        .outboard
        .unwrap();
        let error = scan_outboard(&file, 0, &staged.chunk, 0, &mut private, &proof, &mut stats)
            .unwrap_err();

        assert_eq!(error.code(), afs_error::NODE_TRANSFER_CORRUPT_DATA);
        // Metadata proves the body shorter than the trusted object before the
        // official codec is entered. No positioned body-read attempt is made.
        assert_eq!(stats.data_read_calls, 0);
        assert_eq!(stats.copied_bytes_total, 0);
        assert_eq!(private, vec![0; PEER_CACHE_SEGMENT_BYTES]);
        assert_eq!(stats.hash_bytes_total, 0);
        assert_eq!(stats.hash_work.cache_segment_bytes, 0);
        assert_eq!(stats.read_stages.segment_verify_ns, 0);
    }

    #[test]
    fn disk_peer_cache_direct_private_digest_failure_retires_file_without_success() {
        let temp = tempfile::tempdir().unwrap();
        let mut config = options(temp.path().join("cache"));
        config.capacity_bytes = 8 * 1024 * 1024;
        let cache = DiskPeerCache::open(config).unwrap();
        let staged = StagedChunk::new(
            OperationId::new("direct-private-failure"),
            vec![74; 4 * PEER_CACHE_SEGMENT_BYTES],
        );
        assert!(fill(&cache, &staged).unwrap());
        let key = cache.key(&staged.chunk).unwrap();
        let path = cache.path(&key);
        let mut bytes = fs::read(&path).unwrap();
        let body_start = MAX_HEADER_BYTES;
        bytes[body_start + PEER_CACHE_SEGMENT_BYTES + 17] ^= 1;
        fs::write(&path, bytes).unwrap();
        // Preserve trusted old digests while simulating an unobserved stat
        // change, so the failing current segment takes the direct-read path.
        cache
            .state
            .lock()
            .unwrap()
            .entries
            .get_mut(&key)
            .unwrap()
            .proof
            .as_mut()
            .map(|proof| Arc::get_mut(proof).unwrap())
            .unwrap()
            .identity = file_identity(&fs::metadata(&path).unwrap());
        let before = cache.stats().unwrap();
        let mut private = vec![0; PEER_CACHE_SEGMENT_BYTES];
        assert!(
            !cache
                .operation()
                .unwrap()
                .read_range(&staged.chunk, PEER_CACHE_SEGMENT_BYTES as u64, &mut private,)
                .unwrap()
        );
        assert_ne!(private[17], 74);
        let after = cache.stats().unwrap();
        assert!(
            after.copied_bytes_total - before.copied_bytes_total <= 3 * COVERAGE_GROUP_BYTES as u64
        );
        assert_eq!(after.data_read_calls - before.data_read_calls, 1);
        assert_eq!(
            after.hash_bytes_total - before.hash_bytes_total,
            COVERAGE_GROUP_BYTES as u64
        );
        assert_eq!(after.corruptions, 1);
        assert_eq!(after.proof_metadata_bytes, 0);
        assert!(!path.exists());
    }

    #[test]
    fn disk_peer_cache_stable_one_mib_hit_hashes_only_its_verified_segment() {
        let temp = tempfile::tempdir().unwrap();
        let mut config = options(temp.path().join("cache"));
        config.capacity_bytes = 8 * 1024 * 1024;
        let cache = DiskPeerCache::open(config).unwrap();
        let staged = StagedChunk::new(OperationId::new("segment-read"), vec![71; 4 * 1024 * 1024]);
        assert!(fill(&cache, &staged).unwrap());
        let before = cache.stats().unwrap().hash_bytes_total;
        let mut out = vec![0; 1024 * 1024];
        assert!(
            cache
                .operation()
                .unwrap()
                .read_range(&staged.chunk, 1024 * 1024, &mut out)
                .unwrap()
        );
        assert!(out.iter().all(|byte| *byte == 71));
        assert_eq!(
            cache.stats().unwrap().hash_bytes_total - before,
            1024 * 1024
        );
    }

    #[test]
    fn disk_peer_cache_reopens_disk_objects_without_retained_payloads() {
        let temp = tempfile::tempdir().unwrap();
        let config = options(temp.path().join("cache"));
        let staged = StagedChunk::new(OperationId::new("cache-reopen"), vec![17; 64 * 1024]);
        {
            let cache = DiskPeerCache::open(config.clone()).unwrap();
            assert!(fill(&cache, &staged).unwrap());
        }
        let cache = DiskPeerCache::open(config).unwrap();
        let mut out = [0; 31];
        assert!(
            cache
                .operation()
                .unwrap()
                .read_range(&staged.chunk, 2, &mut out)
                .unwrap()
        );
        assert_eq!(out, [17; 31]);
        assert_eq!(cache.stats().unwrap().entries, 1);
        assert_eq!(
            cache.stats().unwrap().hash_bytes_total,
            staged.chunk.length + COVERAGE_GROUP_BYTES as u64
        );
    }

    #[test]
    fn disk_peer_cache_corrupt_or_truncated_body_is_retired_and_refillable() {
        for truncate in [false, true] {
            let temp = tempfile::tempdir().unwrap();
            let cache = DiskPeerCache::open(options(temp.path().join("cache"))).unwrap();
            let staged = StagedChunk::new(OperationId::new("cache-corrupt"), vec![33; 64 * 1024]);
            assert!(fill(&cache, &staged).unwrap());
            let path = cache.path(&cache.key(&staged.chunk).unwrap());
            let previous_identity = file_identity(&fs::metadata(&path).unwrap());
            let mut bytes = fs::read(&path).unwrap();
            if truncate {
                bytes.pop();
            } else {
                bytes[MAX_HEADER_BYTES + staged.bytes().len() - 1] ^= 1;
            }
            fs::write(&path, bytes).unwrap();
            // Timestamp granularity can make a same-jiffy write appear unchanged.
            // This test is specifically the changed-identity full-root contract;
            // the separate unobserved-damage test covers an unchanged identity.
            File::open(&path)
                .unwrap()
                .set_modified(std::time::UNIX_EPOCH)
                .unwrap();
            assert_ne!(
                previous_identity,
                file_identity(&fs::metadata(&path).unwrap())
            );
            let mut private = [0; 19];
            assert!(
                !cache
                    .operation()
                    .unwrap()
                    .read_range(&staged.chunk, 0, &mut private)
                    .unwrap()
            );
            assert!(!path.exists());
            assert_eq!(cache.stats().unwrap().disk_bytes, 0);
            assert!(fill(&cache, &staged).unwrap());
        }
    }

    #[test]
    fn disk_peer_cache_refuses_bad_proof_and_oversized_admission() {
        let temp = tempfile::tempdir().unwrap();
        let cache = DiskPeerCache::open(options(temp.path().join("cache"))).unwrap();
        let staged = StagedChunk::new(OperationId::new("bad-proof"), vec![21; 64 * 1024]);
        assert!(
            VerifiedPeerChunk::verify(&staged.chunk, &[0; 64 * 1024], cache.proof_budget())
                .is_err()
        );
        assert!(VerifiedPeerChunk::verify(&staged.chunk, &[], cache.proof_budget()).is_err());
        let large = StagedChunk::new(
            OperationId::new("oversize"),
            vec![44; MAX_OBJECT_BYTES as usize + 1],
        );
        assert!(!fill(&cache, &large).unwrap());
        assert_eq!(cache.stats().unwrap().disk_bytes, 0);
    }

    #[test]
    fn disk_peer_cache_evicts_with_actual_blocks_and_failed_delete_stays_charged() {
        let temp = tempfile::tempdir().unwrap();
        let mut config = options(temp.path().join("cache"));
        config.capacity_bytes = 96 * 1024;
        let cache = DiskPeerCache::open(config.clone()).unwrap();
        let first = StagedChunk::new(OperationId::new("first"), vec![1; 64 * 1024]);
        let second = StagedChunk::new(OperationId::new("second"), vec![2; 64 * 1024]);
        assert!(fill(&cache, &first).unwrap());
        let before = cache.stats().unwrap().disk_bytes;
        cache.state.lock().unwrap().fail_remove = true;
        assert!(fill(&cache, &second).is_err());
        assert_eq!(cache.stats().unwrap().disk_bytes, before);
        cache.state.lock().unwrap().fail_remove = false;
        assert!(fill(&cache, &second).unwrap());
        assert!(cache.stats().unwrap().disk_bytes <= config.capacity_bytes);
        assert_eq!(cache.stats().unwrap().evictions, 1);
        assert!(
            !cache
                .operation()
                .unwrap()
                .read_range(&first.chunk, 0, &mut [0; 1])
                .unwrap()
        );
    }

    #[test]
    fn disk_peer_cache_preserves_foreign_files_and_rejects_symlinks() {
        let temp = tempfile::tempdir().unwrap();
        let root = temp.path().join("cache");
        fs::create_dir(&root).unwrap();
        fs::set_permissions(&root, fs::Permissions::from_mode(0o700)).unwrap();
        fs::write(root.join("foreign"), b"preserve").unwrap();
        assert!(DiskPeerCache::open(options(root.clone())).is_err());
        assert_eq!(fs::read(root.join("foreign")).unwrap(), b"preserve");
        let link = temp.path().join("link");
        symlink(&root, &link).unwrap();
        assert!(DiskPeerCache::open(options(link)).is_err());
    }

    #[test]
    fn disk_peer_cache_rejects_linked_ready_files_without_deleting_target() {
        let temp = tempfile::tempdir().unwrap();
        let cache = DiskPeerCache::open(options(temp.path().join("cache"))).unwrap();
        let staged = StagedChunk::new(OperationId::new("cache-link"), vec![27; 1024]);
        assert!(fill(&cache, &staged).unwrap());
        let path = cache.path(&cache.key(&staged.chunk).unwrap());
        let target = temp.path().join("target");
        fs::write(&target, b"preserve").unwrap();
        fs::remove_file(&path).unwrap();
        symlink(&target, &path).unwrap();
        assert!(
            cache
                .operation()
                .unwrap()
                .read_range(&staged.chunk, 0, &mut [0; 1])
                .is_err()
        );
        assert_eq!(fs::read(target).unwrap(), b"preserve");
    }

    #[test]
    fn disk_peer_cache_recovery_retires_body_corruption_and_stale_private_temp() {
        let temp = tempfile::tempdir().unwrap();
        let config = options(temp.path().join("cache"));
        let staged = StagedChunk::new(OperationId::new("restart-corrupt"), vec![18; 64 * 1024]);
        let path;
        {
            let cache = DiskPeerCache::open(config.clone()).unwrap();
            assert!(fill(&cache, &staged).unwrap());
            path = config
                .directory
                .join(format!("{}.ready", cache.key(&staged.chunk).unwrap()));
        }
        let mut bytes = fs::read(&path).unwrap();
        bytes[MAX_HEADER_BYTES + staged.bytes().len() - 1] ^= 1;
        fs::write(&path, bytes).unwrap();
        let pending = config.directory.join(PENDING);
        OpenOptions::new()
            .write(true)
            .create_new(true)
            .mode(0o600)
            .open(&pending)
            .unwrap()
            .write_all(b"interrupted fill")
            .unwrap();
        let cache = DiskPeerCache::open(config).unwrap();
        assert!(!pending.exists());
        assert!(!path.exists());
        assert_eq!(cache.stats().unwrap().entries, 0);
        assert_eq!(cache.stats().unwrap().disk_bytes, 0);
    }

    #[test]
    fn disk_peer_cache_namespace_identity_and_single_owner_are_enforced() {
        let temp = tempfile::tempdir().unwrap();
        let config = options(temp.path().join("cache"));
        let staged = StagedChunk::new(OperationId::new("namespace"), vec![15; 1024]);
        {
            let cache = DiskPeerCache::open(config.clone()).unwrap();
            assert!(fill(&cache, &staged).unwrap());
            assert!(DiskPeerCache::open(config.clone()).is_err());
        }
        let mut wrong = config;
        wrong.namespace_id = NamespaceId::new("other");
        assert!(DiskPeerCache::open(wrong).is_err());
    }

    #[test]
    fn disk_peer_cache_shutdown_stops_new_operations() {
        let temp = tempfile::tempdir().unwrap();
        let cache = DiskPeerCache::open(options(temp.path().join("cache"))).unwrap();
        cache.shutdown().unwrap();
        assert!(cache.operation().is_err());
    }

    #[test]
    fn disk_peer_cache_failed_pending_cleanup_keeps_reserved_blocks_charged() {
        let temp = tempfile::tempdir().unwrap();
        let cache = DiskPeerCache::open(options(temp.path().join("cache"))).unwrap();
        let staged = StagedChunk::new(OperationId::new("pending-failure"), vec![11; 64 * 1024]);
        let foreign = cache.path(&cache.key(&staged.chunk).unwrap());
        OpenOptions::new()
            .write(true)
            .create_new(true)
            .mode(0o600)
            .open(&foreign)
            .unwrap()
            .write_all(b"preserve foreign publication target")
            .unwrap();
        cache.state.lock().unwrap().fail_remove = true;
        assert!(fill(&cache, &staged).is_err());
        let stats = cache.stats().unwrap();
        assert!(stats.disk_bytes >= staged.chunk.length);
        assert!(stats.disk_bytes <= cache.options.capacity_bytes);
        assert_eq!(pending_files(&cache).len(), 1);
        assert_eq!(
            fs::read(&foreign).unwrap(),
            b"preserve foreign publication target"
        );
        cache.state.lock().unwrap().fail_remove = false;
        fs::remove_file(foreign).unwrap();
        assert!(fill(&cache, &staged).unwrap());
        assert!(pending_files(&cache).is_empty());
    }

    #[test]
    fn disk_peer_cache_startup_limits_index_before_reading_unbounded_metadata() {
        let temp = tempfile::tempdir().unwrap();
        let mut config = options(temp.path().join("cache"));
        config.capacity_bytes = 64 * 1024 * 1024;
        drop(DiskPeerCache::open(config.clone()).unwrap());
        for index in 0..=MAX_PEER_CACHE_ENTRIES {
            OpenOptions::new()
                .write(true)
                .create_new(true)
                .mode(0o600)
                .open(config.directory.join(format!("{index:064x}.ready")))
                .unwrap()
                .write_all(b"x")
                .unwrap();
        }
        let error = DiskPeerCache::open(config.clone()).unwrap_err();
        assert!(error.to_string().contains("index budget"));
        assert_eq!(
            fs::read_dir(config.directory).unwrap().count(),
            MAX_PEER_CACHE_ENTRIES + 2
        );
    }

    #[test]
    fn disk_peer_cache_pins_root_and_preserves_foreign_replacement() {
        let temp = tempfile::tempdir().unwrap();
        let config = options(temp.path().join("cache"));
        let cache = DiskPeerCache::open(config.clone()).unwrap();
        let staged = StagedChunk::new(OperationId::new("root-replacement"), vec![23; 1024]);
        assert!(fill(&cache, &staged).unwrap());
        let key = cache.key(&staged.chunk).unwrap();
        let anchored = cache.path(&key);
        let original = fs::read(&anchored).unwrap();
        let mut operation = cache.operation().unwrap();
        fs::rename(&config.directory, temp.path().join("old-root")).unwrap();
        DirBuilder::new()
            .mode(0o700)
            .create(&config.directory)
            .unwrap();
        let foreign = config.directory.join(format!("{key}.ready"));
        fs::write(&foreign, b"foreign replacement must survive").unwrap();
        // The checked-before-rename operation cannot touch the new root.
        assert!(operation.read_range(&staged.chunk, 0, &mut [0; 1]).is_err());
        assert!(
            operation
                .store_verified(
                    &VerifiedPeerChunk::verify(&staged.chunk, staged.bytes(), cache.proof_budget())
                        .unwrap()
                )
                .is_err()
        );
        // Even stale child paths still resolve to the originally pinned root.
        assert_eq!(fs::read(anchored).unwrap(), original);
        assert_eq!(
            fs::read(foreign).unwrap(),
            b"foreign replacement must survive"
        );
        assert!(!config.directory.join(MARKER).exists());
    }

    #[test]
    fn disk_peer_cache_segments_cover_cross_boundary_and_short_tail() {
        let temp = tempfile::tempdir().unwrap();
        let mut config = options(temp.path().join("cache"));
        config.capacity_bytes = 8 * 1024 * 1024;
        let cache = DiskPeerCache::open(config).unwrap();
        let bytes: Vec<_> = (0..2 * PEER_CACHE_SEGMENT_BYTES + 17)
            .map(|index| (index % 251) as u8)
            .collect();
        let staged = StagedChunk::new(OperationId::new("segment-boundaries"), bytes.clone());
        assert!(fill(&cache, &staged).unwrap());
        let mut cross = [0; 19];
        let start = PEER_CACHE_SEGMENT_BYTES - 7;
        assert!(
            cache
                .operation()
                .unwrap()
                .read_range(&staged.chunk, start as u64, &mut cross)
                .unwrap()
        );
        assert_eq!(cross, bytes[start..start + cross.len()]);
        let mut tail = [0; 14];
        let start = 2 * PEER_CACHE_SEGMENT_BYTES + 3;
        assert!(
            cache
                .operation()
                .unwrap()
                .read_range(&staged.chunk, start as u64, &mut tail)
                .unwrap()
        );
        assert_eq!(tail, bytes[start..]);
        let stats = cache.stats().unwrap();
        assert_eq!(
            stats.hash_work.cache_segment_bytes,
            (2 * COVERAGE_GROUP_BYTES + 17) as u64
        );
        assert_eq!(stats.hash_work.cache_fullscan_bytes, 0);
        assert_eq!(stats.hash_work.segment_index_bytes, 0);
        assert_eq!(
            stats.copied_bytes_total,
            2 * (2 * COVERAGE_GROUP_BYTES as u64 + 17) + 33
        );
        assert_eq!(stats.data_read_calls, 3);
        assert!(stats.read_stages.stream_read_verify_ns > 0);
        assert!(stats.read_stages.data_read_ns > 0);
        assert!(stats.read_stages.segment_verify_ns > 0);
    }

    #[test]
    fn disk_peer_cache_changed_metadata_rechecks_entire_root_before_range_return() {
        let temp = tempfile::tempdir().unwrap();
        let mut config = options(temp.path().join("cache"));
        config.capacity_bytes = 8 * 1024 * 1024;
        let cache = DiskPeerCache::open(config).unwrap();
        let staged = StagedChunk::new(
            OperationId::new("changed-tail"),
            vec![51; 4 * PEER_CACHE_SEGMENT_BYTES],
        );
        assert!(fill(&cache, &staged).unwrap());
        let path = cache.path(&cache.key(&staged.chunk).unwrap());
        let mut bytes = fs::read(&path).unwrap();
        bytes[MAX_HEADER_BYTES + staged.bytes().len() - 1] ^= 1;
        fs::write(&path, bytes).unwrap();
        File::open(&path)
            .unwrap()
            .set_modified(std::time::UNIX_EPOCH)
            .unwrap();
        let mut private = [0; 19];
        assert!(
            !cache
                .operation()
                .unwrap()
                .read_range(&staged.chunk, 0, &mut private)
                .unwrap()
        );
        assert_eq!(
            cache.stats().unwrap().hash_work.cache_fullscan_bytes,
            staged.chunk.length
        );
        assert_eq!(cache.stats().unwrap().proof_metadata_bytes, 0);
    }

    #[test]
    fn disk_peer_cache_unobserved_damage_is_detected_when_its_segment_is_read() {
        let temp = tempfile::tempdir().unwrap();
        let mut config = options(temp.path().join("cache"));
        config.capacity_bytes = 8 * 1024 * 1024;
        let cache = DiskPeerCache::open(config).unwrap();
        let staged = StagedChunk::new(
            OperationId::new("silent-tail"),
            vec![52; 4 * PEER_CACHE_SEGMENT_BYTES],
        );
        assert!(fill(&cache, &staged).unwrap());
        let key = cache.key(&staged.chunk).unwrap();
        let path = cache.path(&key);
        let mut bytes = fs::read(&path).unwrap();
        bytes[MAX_HEADER_BYTES + staged.bytes().len() - 1] ^= 1;
        fs::write(&path, bytes).unwrap();
        // Fault injection: model a disk-sector change with no observable stat
        // change. Preserve the trusted old digests, adjust only the observation.
        cache
            .state
            .lock()
            .unwrap()
            .entries
            .get_mut(&key)
            .unwrap()
            .proof
            .as_mut()
            .map(|proof| Arc::get_mut(proof).unwrap())
            .unwrap()
            .identity = file_identity(&fs::metadata(&path).unwrap());
        let mut first = [0; 19];
        assert!(
            cache
                .operation()
                .unwrap()
                .read_range(&staged.chunk, 0, &mut first)
                .unwrap()
        );
        assert_eq!(first, [52; 19]);
        let mut private_tail = [0; 1];
        assert!(
            !cache
                .operation()
                .unwrap()
                .read_range(&staged.chunk, staged.chunk.length - 1, &mut private_tail)
                .unwrap()
        );
        assert_eq!(cache.stats().unwrap().corruptions, 1);
        assert_eq!(cache.stats().unwrap().proof_metadata_bytes, 0);
    }

    #[test]
    fn disk_peer_cache_metadata_quota_falls_back_to_complete_verification() {
        let temp = tempfile::tempdir().unwrap();
        let mut config = options(temp.path().join("cache"));
        config.capacity_bytes = 8 * 1024 * 1024;
        let cache = DiskPeerCache::open(config).unwrap();
        let limit = std::mem::size_of::<Option<Box<VerifiedDiskOutboard>>>() as u64;
        cache.state.lock().unwrap().proof_limit = Some(limit);
        let staged = StagedChunk::new(
            OperationId::new("metadata-quota"),
            vec![53; 4 * PEER_CACHE_SEGMENT_BYTES],
        );
        assert!(fill(&cache, &staged).unwrap());
        let mut out = [0; 19];
        assert!(
            cache
                .operation()
                .unwrap()
                .read_range(&staged.chunk, 0, &mut out)
                .unwrap()
        );
        assert_eq!(out, [53; 19]);
        let stats = cache.stats().unwrap();
        assert!(stats.proof_metadata_bytes <= limit);
        assert!(stats.proof_metadata_peak_bytes <= MAX_PEER_CACHE_PROOF_BYTES);
        assert_eq!(stats.hash_work.cache_fullscan_bytes, staged.chunk.length);
        assert_eq!(stats.hash_work.cache_segment_bytes, 0);
        assert_eq!(stats.hash_work.segment_index_bytes, 0);
    }

    #[test]
    fn disk_peer_cache_restart_rebuilds_root_bound_segments_without_disk_digest_trust() {
        let temp = tempfile::tempdir().unwrap();
        let mut config = options(temp.path().join("cache"));
        config.capacity_bytes = 8 * 1024 * 1024;
        let staged = StagedChunk::new(
            OperationId::new("segment-restart"),
            vec![54; 4 * PEER_CACHE_SEGMENT_BYTES],
        );
        {
            let cache = DiskPeerCache::open(config.clone()).unwrap();
            assert!(fill(&cache, &staged).unwrap());
        }
        let cache = DiskPeerCache::open(config).unwrap();
        let recovered = cache.stats().unwrap();
        assert_eq!(
            recovered.hash_work.cache_fullscan_bytes,
            staged.chunk.length
        );
        assert_eq!(recovered.hash_work.segment_index_bytes, 0);
        let mut out = [0; 19];
        assert!(
            cache
                .operation()
                .unwrap()
                .read_range(&staged.chunk, 0, &mut out)
                .unwrap()
        );
        assert_eq!(
            cache.stats().unwrap().hash_work.cache_fullscan_bytes,
            staged.chunk.length
        );
        assert_eq!(
            cache.stats().unwrap().hash_work.cache_segment_bytes,
            COVERAGE_GROUP_BYTES as u64
        );
        assert!(cache.stats().unwrap().proof_metadata_bytes <= MAX_PEER_CACHE_PROOF_BYTES);
    }

    #[test]
    fn disk_peer_cache_metadata_retires_on_replacement_eviction_and_shutdown() {
        let temp = tempfile::tempdir().unwrap();
        let mut config = options(temp.path().join("cache"));
        config.capacity_bytes = 96 * 1024;
        let cache = DiskPeerCache::open(config).unwrap();
        let first = StagedChunk::new(OperationId::new("metadata-first"), vec![55; 64 * 1024]);
        let second = StagedChunk::new(OperationId::new("metadata-second"), vec![56; 64 * 1024]);
        assert!(fill(&cache, &first).unwrap());
        let one = cache.stats().unwrap().proof_metadata_bytes;
        assert!(one > 0 && one <= MAX_PEER_CACHE_PROOF_BYTES);
        assert!(fill(&cache, &first).unwrap());
        assert_eq!(cache.stats().unwrap().proof_metadata_bytes, one);
        assert!(fill(&cache, &second).unwrap());
        assert_eq!(cache.stats().unwrap().proof_metadata_bytes, one);
        assert_eq!(cache.stats().unwrap().evictions, 1);
        cache.shutdown().unwrap();
        assert_eq!(cache.stats().unwrap().proof_metadata_bytes, 0);
        assert_eq!(cache.stats().unwrap().proof_metadata_peak_bytes, one);
    }

    #[test]
    fn disk_peer_cache_proof_quota_reserves_future_entry_pointers() {
        let temp = tempfile::tempdir().unwrap();
        let budget = OutboardBudget::new(MAX_PEER_CACHE_PROOF_BYTES as usize);
        let cache = DiskPeerCache::open_with_outboard_budget(
            options(temp.path().join("cache")),
            budget.clone(),
        )
        .unwrap();
        let pointers =
            MAX_PEER_CACHE_ENTRIES * std::mem::size_of::<Option<Box<VerifiedDiskOutboard>>>();
        assert_eq!(budget.used(), pointers);
        let staged = StagedChunk::new(OperationId::new("ledger-proof"), vec![3; 64 * 1024]);
        assert!(fill(&cache, &staged).unwrap());
        let held = cache
            .state
            .lock()
            .unwrap()
            .entries
            .values()
            .next()
            .unwrap()
            .proof
            .as_ref()
            .unwrap()
            .outboard
            .clone();
        let charged = held.charged_bytes();
        cache.shutdown().unwrap();
        assert_eq!(cache.stats().unwrap().proof_metadata_bytes, 0);
        assert_eq!(
            budget.used(),
            charged,
            "eviction must retain the shared allocation charge until the final Arc drops"
        );
        drop(held);
        assert_eq!(budget.used(), 0);
    }

    #[test]
    fn disk_peer_cache_reports_actual_segment_work_and_metadata_until_shutdown() {
        let temp = tempfile::tempdir().unwrap();
        let registry = afs_metrics::Registry::new();
        let mut config = options(temp.path().join("cache"));
        config.capacity_bytes = 8 * 1024 * 1024;
        let cache = DiskPeerCache::open(config).unwrap().with_read_work_metrics(
            super::super::read_work::ReadWorkMetrics::register(&registry).unwrap(),
        );
        let staged = StagedChunk::new(
            OperationId::new("metadata-metrics"),
            vec![57; 4 * PEER_CACHE_SEGMENT_BYTES],
        );
        assert!(fill(&cache, &staged).unwrap());
        let mut out = [0; 19];
        assert!(
            cache
                .operation()
                .unwrap()
                .read_range(&staged.chunk, 0, &mut out)
                .unwrap()
        );
        let encoded = afs_metrics::encode_text(&registry).unwrap();
        assert!(
            encoded.contains("afs_dfs_read_hash_bytes_total{stage=\"cache_segment_hash\"} 32768")
        );
        assert!(encoded.contains("afs_dfs_read_hash_bytes_total{stage=\"cache_full_hash\"} 0"));
        assert!(encoded.contains("afs_dfs_peer_disk_cache_data_read_calls_total 1"));
        assert!(encoded.contains("afs_dfs_read_copy_bytes_total{stage=\"cache_to_output\"} 65555"));
        assert!(encoded.contains(
            "afs_dfs_peer_disk_cache_stage_observations_total{stage=\"stream_read_verify\"} 1"
        ));
        let mut direct = vec![0; PEER_CACHE_SEGMENT_BYTES];
        assert!(
            cache
                .operation()
                .unwrap()
                .read_range(&staged.chunk, PEER_CACHE_SEGMENT_BYTES as u64, &mut direct,)
                .unwrap()
        );
        let encoded = afs_metrics::encode_text(&registry).unwrap();
        assert!(encoded.contains("afs_dfs_peer_disk_cache_data_read_calls_total 2"));
        assert!(
            encoded.contains("afs_dfs_read_copy_bytes_total{stage=\"cache_to_output\"} 1114131")
        );
        assert!(
            encoded.contains(
                "afs_dfs_peer_disk_cache_stage_observations_total{stage=\"data_read\"} 2"
            )
        );
        assert!(encoded.contains(
            "afs_dfs_peer_disk_cache_stage_observations_total{stage=\"segment_verify\"} 2"
        ));
        let current = cache.stats().unwrap().proof_metadata_bytes;
        assert!(encoded.contains(&format!("afs_dfs_peer_disk_cache_metadata_bytes {current}")));
        cache.shutdown().unwrap();
        assert_eq!(
            cache.proof_budget.used(),
            0,
            "shutdown must release actual metadata leases"
        );
        assert_eq!(cache.proof_budget.resident_used(), 0);
        {
            let state = cache.state.lock().unwrap();
            assert!(state.entries.is_empty() && state.retired.is_empty());
            assert!(state.flights.is_empty() && state.index_lease.is_none());
            assert_eq!(
                state.operations + state.flight_live + state.deleting_slots,
                0
            );
            assert!(!state.fill_active);
            assert_eq!(
                state.operation_metadata_bytes
                    + state.entry_metadata_bytes
                    + state.flight_metadata_bytes
                    + state.pending_metadata_charge,
                0
            );
        }
        let encoded = afs_metrics::encode_text(&registry).unwrap();
        assert!(encoded.contains("afs_dfs_peer_disk_cache_metadata_bytes 0"));
        assert!(
            encoded.contains("afs_dfs_peer_disk_cache_resources{resource=\"control_bytes\"} 0")
        );
        assert!(encoded.contains(&format!(
            "afs_dfs_peer_disk_cache_metadata_peak_bytes {current}"
        )));
    }
    #[tokio::test(flavor = "current_thread")]
    async fn unified_cancelled_fill_follower_releases_last_flight_owner() {
        let temp = tempfile::tempdir().unwrap();
        let cache = Arc::new(
            DiskPeerCache::open(PeerCacheOptions {
                directory: temp.path().join("cache"),
                capacity_bytes: 1024 * 1024,
                namespace_id: crate::dfs::NamespaceId::new("default"),
            })
            .unwrap(),
        );
        let chunk = StagedChunk::new(
            crate::dfs::OperationId::new("cancel-follower"),
            vec![7; 4096],
        )
        .chunk;
        let leader = match cache.fill_flight_async(&chunk, || Ok(())).await.unwrap() {
            FillRole::Leader(leader) => leader,
            _ => panic!("expected leader"),
        };
        let other = cache.clone();
        let object = chunk.clone();
        let waiting = tokio::spawn(async move {
            let _ = other.fill_flight_async(&object, || Ok(())).await;
        });
        while Arc::strong_count(leader.flight.as_ref().unwrap()) < 3 {
            tokio::task::yield_now().await;
        }
        waiting.abort();
        drop(leader);
        assert!(waiting.await.unwrap_err().is_cancelled());
        assert_eq!(cache.lock_state().unwrap().flight_live, 0);
        assert!(cache.lock_state().unwrap().flights.is_empty());
    }
}
