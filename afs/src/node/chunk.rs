//! Crash-safe local storage for immutable DFS chunks.
//!
//! A batch becomes durable in three ordered barriers: every staging file is
//! synced, final names are installed without replacement and the chunk
//! directory is synced, then one LocalCatalog transaction is appended and
//! synced. Only the catalog revision produced by the final barrier may appear
//! in a `ReplicaAck`.

use std::{
    collections::{HashMap, HashSet, VecDeque},
    fs::{self, File, OpenOptions},
    hash::{Hash, Hasher},
    io::{BufRead, BufReader, Write},
    ops::{Bound, Deref, RangeBounds},
    path::{Path, PathBuf},
    sync::{
        Arc, Condvar, Mutex, Weak,
        atomic::{AtomicU64, Ordering},
    },
    time::{Duration, Instant, SystemTime, UNIX_EPOCH},
};

mod catalog_commit;
mod gc;
#[cfg(any(feature = "dfs", test))]
mod native_live;
#[cfg(test)]
mod native_live_tests;
pub mod outboard;
use catalog_commit::CatalogCommitter;
#[cfg(feature = "dfs")]
pub(crate) use native_live::NativeWriteBudgetSnapshot;
#[cfg(any(feature = "dfs", test))]
pub(crate) use native_live::{
    NativeWriteAdmission, NativeWriteBudget, NativeWriteLease, NativeWriteOwner,
};
pub use outboard::OutboardBudget;
use outboard::{
    ChunkOutboard, DEFAULT_OUTBOARD_BUDGET_BYTES, MAX_ENCODED_OUTBOARD_BYTES, OutboardBuilder,
    OutboardLease, OutboardReadContext, OutboardReadWork, SharedOutboard, SidecarOutboard,
};

use afs_error::{Error, Result};
use afs_tracing::tracing;
use serde::{Deserialize, Serialize};
#[cfg(unix)]
use std::os::unix::fs::FileExt;

#[cfg(test)]
use crate::dfs::ReplicaGroupId;
use crate::{
    dfs::{
        ChunkEncoding, ChunkId, ChunkObject, ChunkReceipt, ContentDigest, DigestAlgorithm,
        OperationId, ReplicaAck, ReplicaTarget, StorageDeviceDescriptor,
    },
    node::{storage::localfs::capacity_from_statvfs_fd, vfs::types::FilesystemCapacity},
};

mod catalog;
mod lifecycle;
mod outboard_file;
mod peer_serve;
mod reader;
mod sidecar_install;
mod staged;
mod store;
mod write_buffer;
pub use catalog::LocalCatalog;
use catalog::*;
pub use lifecycle::QuarantineTrigger;
use lifecycle::*;
use outboard_file::*;
#[cfg(feature = "dfs")]
pub(crate) use peer_serve::PeerGroupReader;
#[cfg(feature = "rdma")]
pub(crate) use peer_serve::PeerReadReservation;
use peer_serve::*;
pub use reader::PinnedChunkReader;
pub(crate) use reader::VerifiedRangeCopy;
use reader::*;
use sidecar_install::*;
#[cfg(feature = "dfs")]
pub(crate) use staged::MAX_STAGED_CHUNK_BYTES;
pub use staged::StagedChunk;
#[cfg(test)]
use store::*;
use write_buffer::*;
pub use write_buffer::{ChunkBuilder, SharedChunkBytes, WriteBufferPool, WriteBufferPoolStats};

const OUTBOARD_REPAIR_SHARDS: usize = 64;
type OutboardRepairGates = [Mutex<()>; OUTBOARD_REPAIR_SHARDS];

/// Why chunks are being stored. Everything else about the write is the store's.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum PutIntent {
    /// Published by the commit that stores them: register their GC publication
    /// first, and make no local release decision.
    Commit,
    /// Background upload of a native generation, which fences itself at commit.
    /// `DfsChunkSink` decides the [`BodyRelease`] of each [`StoredChunk`].
    NativeUpload,
}

/// What the writer may do with the in-memory body once a native upload is durable.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum BodyRelease {
    /// This node's exact durable copy is proven: drop the body now and serve
    /// later reads through [`ChunkSink::read_uploaded_at`].
    ReleaseAfterUpload,
    /// No proven local copy: keep the body until the generation is published.
    RetainUntilPublish,
}

/// A persistence receipt with an explicit, conservative body decision. Only the
/// native replication implementation may prove the effective Required policy and
/// the matching local durable ACK; [`Self::unreleased`] never grants a release.
#[derive(Clone, Debug)]
pub struct StoredChunk {
    pub receipt: ChunkReceipt,
    pub body_release: BodyRelease,
}

impl StoredChunk {
    pub fn unreleased(receipt: ChunkReceipt) -> Self {
        Self {
            receipt,
            body_release: BodyRelease::RetainUntilPublish,
        }
    }
}

/// Where sealed chunks go. Reads of published data do not come through here; a
/// sink only answers for bodies its own receipts anchor. Implementations
/// compose: a sink may hold other sinks and decide the [`BodyRelease`] itself.
pub trait ChunkSink: Send + Sync {
    /// The single write entry.
    fn put_chunks(&self, staged: Vec<StagedChunk>, intent: PutIntent) -> Result<Vec<StoredChunk>>;

    /// Reserve native control metadata before accepting a body. Production
    /// replication overrides this from its cached authoritative placement.
    /// The fallback is only a conservative allowance for small custom stores:
    /// retained actual capacity must still be checked against that allowance.
    fn descriptor_reservation_bytes(&self, operation_id: &OperationId) -> Result<usize> {
        const FALLBACK_BYTES: usize = 16 * 1024;
        if operation_id.0.capacity() > FALLBACK_BYTES / 4 {
            return Err(Error::coded(
                afs_error::IO_CAPACITY,
                "native operation metadata exceeds the custom-store reservation allowance",
            ));
        }
        Ok(FALLBACK_BYTES)
    }

    /// Read an unpublished native object anchored by its validated receipt.
    /// Unsupported stores must keep the original body until publication.
    fn read_uploaded_at(
        &self,
        _receipt: &ChunkReceipt,
        _offset: u64,
        _out: &mut [u8],
    ) -> Result<usize> {
        Err(Error::coded(
            afs_error::NODE_VFS_UNIMPLEMENTED,
            "native uploaded local read is unsupported for this backend",
        ))
    }

    /// Commit-intent shorthand for the many tests that only want receipts.
    #[cfg(test)]
    fn put_batch(&self, staged: Vec<StagedChunk>) -> Result<Vec<ChunkReceipt>> {
        let stored = self.put_chunks(staged, PutIntent::Commit)?;
        Ok(stored.into_iter().map(|stored| stored.receipt).collect())
    }

    /// Single-chunk form of [`Self::put_batch`], for tests.
    #[cfg(test)]
    fn put(&self, staged: StagedChunk) -> Result<ChunkReceipt> {
        self.put_batch(vec![staged])?
            .pop()
            .ok_or_else(|| invalid("ChunkStore returned no receipt for one staged Chunk"))
    }

    fn capacity(&self) -> Result<FilesystemCapacity> {
        Err(Error::coded(
            afs_error::NODE_VFS_UNIMPLEMENTED,
            "ChunkStore capacity is unsupported for this backend",
        ))
    }
}

#[derive(Clone, Copy, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub enum LocalChunkState {
    Durable,
    Quarantined,
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub enum PhysicalLocation {
    PerChunkFile { relative_path: String },
}

#[derive(Clone, Copy, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub enum PhysicalEncoding {
    Raw,
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub struct LocalChunkRecord {
    pub chunk: ChunkObject,
    pub stored_length: u64,
    pub stored_checksum: ContentDigest,
    pub device_id: String,
    pub device_epoch: u64,
    pub location: PhysicalLocation,
    pub encoding: PhysicalEncoding,
    pub state: LocalChunkState,
    pub catalog_revision: u64,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub(crate) segment_crc: Option<super::chunk_crc32c::CrcTable>,
}

#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub struct RecoveryReport {
    pub removed_staging_files: u64,
    pub durable_chunks: u64,
    pub orphan_files: u64,
}

#[cfg(test)]
mod catalog_tail_recovery_tests;

#[cfg(test)]
mod format_golden_tests;

#[derive(Debug)]
pub struct LocalChunkStore {
    node_id: String,
    device_id: String,
    device_epoch: u64,
    root: PathBuf,
    root_dir: File,
    chunks: PathBuf,
    staging: PathBuf,
    outboards: PathBuf,
    outboard_budget: Arc<OutboardBudget>,
    outboard_repairs: Arc<OutboardRepairGates>,
    catalog_path: PathBuf,
    catalog: Mutex<LocalCatalog>,
    catalog_committer: CatalogCommitter,
    next_staging: AtomicU64,
    gc_inventory_cursor: AtomicU64,
    /// This Node's own remote-read policy as a reader. Served peer requests
    /// follow the caller's requested mode instead.
    peer_read_crc_mode: crate::config::DfsReadCrcMode,
    read_work: Option<super::read_work::ReadWorkMetrics>,
    sidecar_installer: SidecarInstaller,
    #[cfg(test)]
    catalog_commit_test: CatalogCommitTestControl,
    #[cfg(test)]
    sidecar_install_test: Arc<SidecarInstallTestControl>,
    #[cfg(test)]
    peer_fill_test_blocks: Mutex<HashMap<ChunkId, PeerFillTestBlock>>,
    #[cfg(test)]
    peer_ready_test_block: Mutex<Option<PeerFillTestBlock>>,
    #[cfg(test)]
    peer_payload_test_block: Mutex<Option<PeerFillTestBlock>>,
    #[cfg(any(feature = "dfs", test))]
    peer_read_budget: Arc<PeerReadBudget>,
    #[cfg(any(feature = "dfs", test))]
    peer_buffer_pool: Arc<super::read_buffer::ReadBufferPool>,
    #[cfg(any(feature = "dfs", test))]
    peer_read_cache: Mutex<PeerServeState>,
    #[cfg(any(feature = "dfs", test))]
    peer_serve_changed: Condvar,
}

fn open_device_epoch(root: &Path) -> Result<u64> {
    let path = root.join("device_epoch");
    if path.exists() {
        let value = fs::read_to_string(path).map_err(Error::from)?;
        return value
            .trim()
            .parse::<u64>()
            .map_err(|_| invalid("local chunk device epoch is corrupt"));
    }
    let epoch = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map_err(|_| invalid("system clock is before UNIX epoch"))?
        .as_nanos() as u64
        ^ u64::from(std::process::id());
    let temp = root.join("device_epoch.tmp");
    if temp.exists() {
        fs::remove_file(&temp).map_err(Error::from)?;
    }
    let mut file = OpenOptions::new()
        .create_new(true)
        .write(true)
        .open(&temp)
        .map_err(Error::from)?;
    writeln!(file, "{epoch}").map_err(Error::from)?;
    file.sync_all().map_err(Error::from)?;
    fs::rename(&temp, &path).map_err(Error::from)?;
    File::open(root)
        .and_then(|directory| directory.sync_all())
        .map_err(Error::from)?;
    Ok(epoch)
}

fn verify_file(path: &Path, chunk: &ChunkObject) -> Result<()> {
    let file = File::open(path).map_err(Error::from)?;
    verified_range(&file, chunk, 0, 0, None).map(|_| ())
}

/// Hash the pinned file once and retain only the requested overlap. The
/// temporary range is bounded by the caller's output; scan memory is 64KiB.
/// Nothing is published to the caller until all content checks pass.
fn verified_range(
    file: &File,
    chunk: &ChunkObject,
    offset: u64,
    length: usize,
    metrics: Option<&super::read_work::ReadWorkMetrics>,
) -> Result<Vec<u8>> {
    offset
        .checked_add(length as u64)
        .filter(|end| *end <= chunk.length)
        .ok_or_else(|| invalid("verified Chunk range exceeds its length"))?;
    let mut bytes = vec![0; length];
    verified_ranges_into(
        file,
        chunk,
        &[VerifiedRangeCopy {
            chunk_offset: offset,
            length,
            output_offset: 0,
        }],
        &mut bytes,
        true,
        metrics,
    )?;
    Ok(bytes)
}

fn verified_ranges_into(
    file: &File,
    chunk: &ChunkObject,
    ranges: &[VerifiedRangeCopy],
    out: &mut [u8],
    force_scan: bool,
    metrics: Option<&super::read_work::ReadWorkMetrics>,
) -> Result<Vec<usize>> {
    verified_ranges_into_building(file, chunk, ranges, out, force_scan, metrics, None)
        .map(|(counts, _)| counts)
}

fn verified_ranges_into_building(
    file: &File,
    chunk: &ChunkObject,
    ranges: &[VerifiedRangeCopy],
    out: &mut [u8],
    force_scan: bool,
    metrics: Option<&super::read_work::ReadWorkMetrics>,
    mut builder: Option<OutboardBuilder>,
) -> Result<(Vec<usize>, Option<SharedOutboard>)> {
    let mut counts = Vec::with_capacity(ranges.len());
    let mut active = Vec::new();
    for range in ranges.iter().copied() {
        let allowed = if range.chunk_offset >= chunk.length || range.length == 0 {
            0
        } else {
            usize::try_from((chunk.length - range.chunk_offset).min(range.length as u64))
                .map_err(|_| invalid("Chunk read length is too large"))?
        };
        let end = range
            .output_offset
            .checked_add(allowed)
            .ok_or_else(|| invalid("verified Chunk output range overflow"))?;
        if end > out.len() {
            return Err(invalid("verified Chunk output range exceeds buffer"));
        }
        counts.push(allowed);
        if allowed != 0 {
            active.push((range, allowed));
        }
    }
    if active.is_empty() && !force_scan {
        return Ok((counts, None));
    }
    if file.metadata().map_err(Error::from)?.len() != chunk.length {
        return Err(corrupt(&chunk.id, "length mismatch"));
    }
    let mut work = metrics.map(super::read_work::ReadWorkMetrics::scan);
    let mut scratch = [0; 64 * 1024];
    let mut hasher = blake3::Hasher::new();
    let mut position = 0;
    while position < chunk.length {
        let limit = (chunk.length - position).min(scratch.len() as u64) as usize;
        let count = read_positioned(file, position, &mut scratch[..limit])?;
        if count == 0 {
            return Err(corrupt(&chunk.id, "ended during verification"));
        }
        if let Some(builder) = &mut builder {
            builder.update(&scratch[..count])?;
        } else {
            hasher.update(&scratch[..count]);
        }
        if let Some(work) = &mut work {
            work.bytes += count as u64;
        }
        #[cfg(test)]
        VERIFIED_SCAN_BYTES.with(|bytes| bytes.set(bytes.get() + count as u64));
        let next = position + count as u64;
        for (range, allowed) in &active {
            let range_end = range.chunk_offset + *allowed as u64;
            let overlap_start = position.max(range.chunk_offset);
            let overlap_end = next.min(range_end);
            if overlap_start < overlap_end {
                let output_start =
                    range.output_offset + (overlap_start - range.chunk_offset) as usize;
                let output_end = range.output_offset + (overlap_end - range.chunk_offset) as usize;
                if let Some(work) = &mut work {
                    work.copied += (output_end - output_start) as u64;
                }
                out[output_start..output_end].copy_from_slice(
                    &scratch
                        [(overlap_start - position) as usize..(overlap_end - position) as usize],
                );
            }
        }
        position = next;
    }
    if file.metadata().map_err(Error::from)?.len() != chunk.length {
        return Err(corrupt(&chunk.id, "length changed during verification"));
    }
    let (actual, proof) = if let Some(builder) = builder {
        let built = builder.finish()?;
        debug_assert_eq!(built.hash_body_bytes, position);
        if let Some(metrics) = metrics {
            metrics.hash("outboard_parent_hash", built.parent_hash_bytes);
        }
        (built.digest, built.outboard)
    } else {
        (
            ContentDigest {
                algorithm: DigestAlgorithm::Blake3,
                bytes: *hasher.finalize().as_bytes(),
            },
            None,
        )
    };
    if actual != chunk.content_digest {
        return Err(corrupt(&chunk.id, "digest mismatch"));
    }
    Ok((counts, proof))
}

fn digest(bytes: &[u8]) -> ContentDigest {
    ContentDigest {
        algorithm: DigestAlgorithm::Blake3,
        bytes: *blake3::hash(bytes).as_bytes(),
    }
}

fn digest_hex(digest: &ContentDigest) -> String {
    digest
        .bytes
        .iter()
        .map(|byte| format!("{byte:02x}"))
        .collect()
}

fn safe_id(value: &str) -> String {
    value
        .chars()
        .map(|character| {
            if character.is_ascii_alphanumeric() || matches!(character, '-' | '_') {
                character
            } else {
                '_'
            }
        })
        .collect()
}

fn corrupt(id: &ChunkId, detail: &str) -> Error {
    Error::coded(
        afs_error::NODE_TRANSFER_CORRUPT_DATA,
        format!("local Chunk '{}' {detail}", id.0),
    )
}

fn invalid(message: impl Into<String>) -> Error {
    Error::coded(afs_error::NODE_STORAGE_INVALID, message)
}

#[cfg(test)]
std::thread_local! {
    static FAIL_STORE_CATALOG_SYNC: std::cell::Cell<bool> = const { std::cell::Cell::new(false) };
    static STORE_CATALOG_SYNCS: std::cell::RefCell<Option<Vec<PathBuf>>> = const { std::cell::RefCell::new(None) };
    static FAIL_STORE_DIRECTORY_SYNC: std::cell::Cell<bool> = const { std::cell::Cell::new(false) };
    static STORE_DIRECTORY_SYNCS: std::cell::RefCell<Option<Vec<PathBuf>>> = const { std::cell::RefCell::new(None) };
    static VERIFIED_SCAN_BYTES: std::cell::Cell<u64> = const { std::cell::Cell::new(0) };
    static OUTBOARD_PERSIST_IO: std::cell::RefCell<OutboardPersistIo> = const {
        std::cell::RefCell::new(OutboardPersistIo {
            writes: 0,
            file_syncs: 0,
            renames: 0,
            directory_syncs: 0,
        })
    };
    static OUTBOARD_INSTALL_HOOK: std::cell::RefCell<Option<Box<dyn FnOnce()>>> = const {
        std::cell::RefCell::new(None)
    };
    static FAIL_OUTBOARD_DIRECTORY_SYNC: std::cell::Cell<bool> = const {
        std::cell::Cell::new(false)
    };
}

#[cfg(test)]
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
// Count only successful real I/O operations; injected failures do not count.
struct OutboardPersistIo {
    writes: usize,
    file_syncs: usize,
    renames: usize,
    directory_syncs: usize,
}

#[cfg(test)]
mod replica_replay_tests;

#[cfg(test)]
mod write_buffer_tests;

#[cfg(test)]
mod crc32c_storage_tests;

#[cfg(test)]
mod crc_read_modes_storage_tests;
