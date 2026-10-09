//! Crash-safe local storage for immutable DFS chunks.
//!
//! A batch becomes durable in three ordered barriers: every staging file is
//! synced, final names are installed without replacement and the chunk
//! directory is synced, then one LocalCatalog transaction is appended and
//! synced. Only the catalog revision produced by the final barrier may appear
//! in a `ReplicaAck`.

use std::{
    collections::{HashMap, HashSet},
    fs::{self, File, OpenOptions},
    io::{BufRead, BufReader, Write},
    path::{Path, PathBuf},
    sync::{
        Arc, Mutex,
        atomic::{AtomicU64, Ordering},
    },
    time::{SystemTime, UNIX_EPOCH},
};

use afs_error::{Error, Result};
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

#[derive(Debug, Default)]
pub struct ChunkBuilder {
    bytes: Vec<u8>,
}

impl ChunkBuilder {
    pub fn write_at(&mut self, offset: u64, data: &[u8]) -> Result<usize> {
        let offset = usize::try_from(offset).map_err(|_| invalid("chunk offset is too large"))?;
        let end = offset
            .checked_add(data.len())
            .ok_or_else(|| invalid("chunk write range overflow"))?;
        if end > self.bytes.len() {
            self.bytes.resize(end, 0);
        }
        self.bytes[offset..end].copy_from_slice(data);
        Ok(data.len())
    }

    pub fn replace(&mut self, bytes: Vec<u8>) {
        self.bytes = bytes;
    }

    pub fn stage(self, operation_id: OperationId) -> StagedChunk {
        StagedChunk::new(operation_id, self.bytes)
    }
}

/// Shared producer/replica budget per immutable staged chunk, never a file limit.
#[cfg(feature = "dfs")]
pub(crate) const MAX_STAGED_CHUNK_BYTES: usize = 4 * 1024 * 1024;

#[derive(Clone, Debug)]
pub struct StagedChunk {
    pub operation_id: OperationId,
    pub chunk: ChunkObject,
    bytes: Arc<[u8]>,
}

impl StagedChunk {
    pub fn new(operation_id: OperationId, bytes: Vec<u8>) -> Self {
        let bytes: Arc<[u8]> = bytes.into();
        let content_digest = digest(&bytes);
        let chunk = ChunkObject {
            id: ChunkId::new(format!(
                "blake3-{}-{}",
                digest_hex(&content_digest),
                bytes.len()
            )),
            length: bytes.len() as u64,
            content_digest,
            encoding: ChunkEncoding::Raw,
        };
        Self {
            operation_id,
            chunk,
            bytes,
        }
    }

    pub fn bytes(&self) -> &[u8] {
        &self.bytes
    }
}

pub trait ChunkStore: Send + Sync {
    fn put_batch(&self, staged: Vec<StagedChunk>) -> Result<Vec<ChunkReceipt>>;

    fn put(&self, staged: StagedChunk) -> Result<ChunkReceipt> {
        self.put_batch(vec![staged])?
            .pop()
            .ok_or_else(|| invalid("ChunkStore returned no receipt for one staged Chunk"))
    }

    fn read_at(&self, chunk_id: &ChunkId, offset: u64, out: &mut [u8]) -> Result<usize>;

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
}

#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub struct RecoveryReport {
    pub removed_staging_files: u64,
    pub durable_chunks: u64,
    pub orphan_files: u64,
}

#[derive(Debug)]
pub struct PinnedChunkReader {
    file: File,
    chunk: ChunkObject,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) struct VerifiedRangeCopy {
    pub chunk_offset: u64,
    pub length: usize,
    pub output_offset: usize,
}

impl PinnedChunkReader {
    pub fn read_at(&self, offset: u64, out: &mut [u8]) -> Result<usize> {
        if offset >= self.chunk.length || out.is_empty() {
            return Ok(0);
        }
        let allowed = usize::try_from((self.chunk.length - offset).min(out.len() as u64))
            .map_err(|_| invalid("Chunk read length is too large"))?;
        // A pinned descriptor protects identity across rename, not integrity
        // after disk damage. Return the exact bytes covered by this digest,
        // rather than verifying and then issuing another unverified read.
        let bytes = verified_range(&self.file, &self.chunk, offset, allowed)?;
        out[..allowed].copy_from_slice(&bytes);
        Ok(allowed)
    }

    /// Verify the pinned file once and scatter requested ranges into an
    /// uncommitted staging buffer. The staging buffer may be dirtied before an
    /// error is returned; callers must publish it only after the outer read
    /// batch succeeds.
    pub(crate) fn read_ranges_at_uncommitted(
        &self,
        ranges: &[VerifiedRangeCopy],
        out: &mut [u8],
    ) -> Result<Vec<usize>> {
        verified_ranges_into(&self.file, &self.chunk, ranges, out, false)
    }
}

#[cfg(unix)]
fn read_positioned(file: &File, offset: u64, out: &mut [u8]) -> Result<usize> {
    loop {
        match file.read_at(out, offset) {
            Err(error) if error.kind() == std::io::ErrorKind::Interrupted => continue,
            result => return result.map_err(Error::from),
        }
    }
}

#[cfg(not(unix))]
fn read_positioned(file: &File, offset: u64, out: &mut [u8]) -> Result<usize> {
    let mut file = file.try_clone().map_err(Error::from)?;
    use std::io::{Seek, SeekFrom};
    file.seek(SeekFrom::Start(offset)).map_err(Error::from)?;
    file.read(out).map_err(Error::from)
}

#[derive(Debug, Deserialize, Serialize)]
struct CatalogTxn {
    revision: u64,
    records: Vec<LocalChunkRecord>,
    checksum: String,
}

impl CatalogTxn {
    fn new(revision: u64, records: Vec<LocalChunkRecord>) -> Result<Self> {
        let checksum = catalog_checksum(revision, &records)?;
        Ok(Self {
            revision,
            records,
            checksum,
        })
    }

    fn verify(&self) -> Result<()> {
        if self.checksum != catalog_checksum(self.revision, &self.records)? {
            return Err(invalid("local chunk catalog transaction checksum mismatch"));
        }
        Ok(())
    }
}

#[derive(Debug, Default)]
pub struct LocalCatalog {
    revision: u64,
    records: HashMap<ChunkId, LocalChunkRecord>,
}

impl LocalCatalog {
    pub fn revision(&self) -> u64 {
        self.revision
    }

    fn recover(path: &Path) -> Result<Self> {
        if !path.exists() {
            return Ok(Self::default());
        }
        let file = File::open(path).map_err(Error::from)?;
        let mut catalog = Self::default();
        for line in BufReader::new(file).lines() {
            let line = line.map_err(Error::from)?;
            if line.trim().is_empty() {
                continue;
            }
            let txn: CatalogTxn = serde_json::from_str(&line)
                .map_err(|error| invalid(format!("local chunk catalog is corrupt: {error}")))?;
            txn.verify()?;
            if txn.revision != catalog.revision.saturating_add(1) {
                return Err(invalid("local chunk catalog revision is not contiguous"));
            }
            for record in txn.records {
                if record.catalog_revision != txn.revision {
                    return Err(invalid("local chunk record has the wrong catalog revision"));
                }
                catalog.records.insert(record.chunk.id.clone(), record);
            }
            catalog.revision = txn.revision;
        }
        Ok(catalog)
    }
}

#[derive(Debug)]
pub struct LocalChunkStore {
    node_id: String,
    device_id: String,
    device_epoch: u64,
    root: PathBuf,
    root_dir: File,
    chunks: PathBuf,
    staging: PathBuf,
    catalog_path: PathBuf,
    catalog: Mutex<LocalCatalog>,
    next_staging: AtomicU64,
}

impl LocalChunkStore {
    pub fn open(root: impl AsRef<Path>, node_id: impl Into<String>) -> Result<Self> {
        let root = root.as_ref().to_path_buf();
        let chunks = root.join("chunks");
        let staging = root.join("staging");
        fs::create_dir_all(&chunks).map_err(Error::from)?;
        fs::create_dir_all(&staging).map_err(Error::from)?;
        let device_epoch = open_device_epoch(&root)?;
        let root_dir = File::open(&root).map_err(Error::from)?;
        let catalog_path = root.join("catalog.wal");
        let catalog = LocalCatalog::recover(&catalog_path)?;
        let store = Self {
            node_id: node_id.into(),
            device_id: "local-0".into(),
            device_epoch,
            root,
            root_dir,
            chunks,
            staging,
            catalog_path,
            catalog: Mutex::new(catalog),
            next_staging: AtomicU64::new(1),
        };
        store.recover()?;
        Ok(store)
    }

    pub fn capacity(&self) -> Result<FilesystemCapacity> {
        capacity_from_statvfs_fd(&self.root_dir).map_err(Error::from)
    }

    pub fn device_descriptor(&self) -> Result<StorageDeviceDescriptor> {
        let catalog = self
            .catalog
            .lock()
            .map_err(|_| invalid("local chunk catalog lock is poisoned"))?;
        Ok(StorageDeviceDescriptor {
            device_id: self.device_id.clone(),
            device_epoch: self.device_epoch,
            catalog_revision: catalog.revision,
            failure_domain: self.node_id.clone(),
        })
    }

    pub fn recover(&self) -> Result<RecoveryReport> {
        let mut report = RecoveryReport::default();
        for entry in fs::read_dir(&self.staging).map_err(Error::from)? {
            let entry = entry.map_err(Error::from)?;
            if entry.file_type().map_err(Error::from)?.is_file() {
                fs::remove_file(entry.path()).map_err(Error::from)?;
                report.removed_staging_files = report.removed_staging_files.saturating_add(1);
            }
        }
        let mut catalog = self
            .catalog
            .lock()
            .map_err(|_| invalid("local chunk catalog lock is poisoned"))?;
        let mut damaged = Vec::new();
        for record in catalog.records.values() {
            if record.device_id != self.device_id
                || record.device_epoch != self.device_epoch
                || record.stored_length != record.chunk.length
                || record.stored_checksum != record.chunk.content_digest
            {
                return Err(invalid(
                    "durable local Chunk record is internally inconsistent",
                ));
            }
            if record.state != LocalChunkState::Durable {
                continue;
            }
            let path = self.path_for_record(record)?;
            match path.metadata() {
                Ok(metadata) if metadata.is_file() && metadata.len() == record.chunk.length => {
                    report.durable_chunks = report.durable_chunks.saturating_add(1);
                }
                Ok(_) => damaged.push(record.clone()),
                Err(error) if error.kind() == std::io::ErrorKind::NotFound => {
                    damaged.push(record.clone());
                }
                Err(error) => return Err(Error::from(error)),
            }
        }
        if !damaged.is_empty() {
            self.quarantine_records(&mut catalog, damaged)?;
        }
        for entry in fs::read_dir(&self.chunks).map_err(Error::from)? {
            let entry = entry.map_err(Error::from)?;
            let id = ChunkId::new(entry.file_name().to_string_lossy().into_owned());
            if !catalog.records.contains_key(&id) {
                report.orphan_files = report.orphan_files.saturating_add(1);
            }
        }
        Ok(report)
    }

    fn chunk_path(&self, id: &ChunkId) -> PathBuf {
        self.chunks.join(&id.0)
    }

    fn path_for_record(&self, record: &LocalChunkRecord) -> Result<PathBuf> {
        match &record.location {
            PhysicalLocation::PerChunkFile { relative_path } => {
                if relative_path != &format!("chunks/{}", record.chunk.id.0) {
                    return Err(invalid("local chunk record has a non-canonical path"));
                }
                let path = self.root.join(relative_path);
                if !path.starts_with(&self.chunks) {
                    return Err(invalid("local chunk record escapes the chunk directory"));
                }
                Ok(path)
            }
        }
    }

    pub fn persist_batch(
        &self,
        staged: &[StagedChunk],
        target: &ReplicaTarget,
        placement_revision: u64,
        placement_epoch: u64,
    ) -> Result<Vec<ReplicaAck>> {
        if target.node_id != self.node_id
            || target.device.device_id != self.device_id
            || target.device.device_epoch != self.device_epoch
        {
            return Err(invalid(
                "replica target does not identify this local chunk store",
            ));
        }
        let mut catalog = self
            .catalog
            .lock()
            .map_err(|_| invalid("local chunk catalog lock is poisoned"))?;
        if catalog.revision < target.device.catalog_revision {
            return Err(invalid(
                "local catalog is older than the placement revision floor",
            ));
        }

        let mut new_chunks = Vec::new();
        let mut installed_chunk_ids = HashSet::new();
        for item in staged {
            let mut replace_existing = false;
            if let Some(existing) = catalog.records.get(&item.chunk.id) {
                if existing.chunk != item.chunk {
                    return Err(invalid(
                        "immutable Chunk ID collides with another local record",
                    ));
                }
                if existing.state == LocalChunkState::Quarantined {
                    replace_existing = true;
                } else {
                    match verify_file(&self.path_for_record(existing)?, &item.chunk) {
                        Ok(()) => continue,
                        Err(error) if verified_copy_is_invalid(&error) => replace_existing = true,
                        Err(error) => return Err(error),
                    }
                }
            }
            let temp_path = self.staging.join(format!(
                "{}.{}.tmp",
                safe_id(&item.operation_id.0),
                self.next_staging.fetch_add(1, Ordering::Relaxed)
            ));
            let mut file = OpenOptions::new()
                .create_new(true)
                .write(true)
                .open(&temp_path)
                .map_err(Error::from)?;
            file.write_all(item.bytes()).map_err(Error::from)?;
            file.sync_all().map_err(Error::from)?;
            let final_path = self.chunk_path(&item.chunk.id);
            if replace_existing {
                // Publish a new inode; never truncate a file held by readers.
                // Its immutable content identity is unchanged. Directory and
                // catalog barriers below still precede the durable ACK.
                fs::rename(&temp_path, &final_path).map_err(Error::from)?;
            } else {
                match fs::hard_link(&temp_path, &final_path) {
                    Ok(()) => {}
                    Err(error) if error.kind() == std::io::ErrorKind::AlreadyExists => {
                        verify_file(&final_path, &item.chunk)?;
                    }
                    Err(error) => return Err(Error::from(error)),
                }
                fs::remove_file(&temp_path).map_err(Error::from)?;
            }
            if installed_chunk_ids.insert(item.chunk.id.clone()) {
                new_chunks.push(item.chunk.clone());
            }
        }

        if !new_chunks.is_empty() {
            File::open(&self.chunks)
                .and_then(|directory| directory.sync_all())
                .map_err(Error::from)?;
            let revision = catalog.revision.saturating_add(1);
            let records = new_chunks
                .into_iter()
                .map(|chunk| LocalChunkRecord {
                    location: PhysicalLocation::PerChunkFile {
                        relative_path: format!("chunks/{}", chunk.id.0),
                    },
                    stored_length: chunk.length,
                    stored_checksum: chunk.content_digest.clone(),
                    device_id: self.device_id.clone(),
                    device_epoch: self.device_epoch,
                    chunk,
                    encoding: PhysicalEncoding::Raw,
                    state: LocalChunkState::Durable,
                    catalog_revision: revision,
                })
                .collect::<Vec<_>>();
            append_catalog_txn(
                &self.catalog_path,
                &CatalogTxn::new(revision, records.clone())?,
            )?;
            for record in records {
                catalog.records.insert(record.chunk.id.clone(), record);
            }
            catalog.revision = revision;
        }
        let catalog_revision = catalog.revision;
        Ok(staged
            .iter()
            .map(|item| ReplicaAck {
                operation_id: item.operation_id.clone(),
                chunk_id: item.chunk.id.clone(),
                placement_revision,
                placement_epoch,
                node_id: target.node_id.clone(),
                node_epoch: target.node_epoch,
                device_id: target.device.device_id.clone(),
                device_epoch: target.device.device_epoch,
                catalog_revision,
                persisted_bytes: item.chunk.length,
                verified_digest: item.chunk.content_digest.clone(),
            })
            .collect())
    }

    pub fn persist(
        &self,
        staged: &StagedChunk,
        target: &ReplicaTarget,
        placement_revision: u64,
        placement_epoch: u64,
    ) -> Result<ReplicaAck> {
        self.persist_batch(
            std::slice::from_ref(staged),
            target,
            placement_revision,
            placement_epoch,
        )?
        .pop()
        .ok_or_else(|| invalid("local batch finalize returned no ReplicaAck"))
    }

    pub fn read_at(&self, chunk_id: &ChunkId, offset: u64, out: &mut [u8]) -> Result<usize> {
        self.open_verified(chunk_id)?
            .read_at(offset, out)
            .inspect_err(|error| {
                if verified_copy_is_invalid(error) {
                    self.try_quarantine(chunk_id);
                }
            })
    }

    pub fn open_verified(&self, chunk_id: &ChunkId) -> Result<PinnedChunkReader> {
        let record = self
            .catalog
            .lock()
            .map_err(|_| invalid("local chunk catalog lock is poisoned"))?
            .records
            .get(chunk_id)
            .cloned()
            .ok_or_else(|| invalid(format!("local Chunk '{}' is not cataloged", chunk_id.0)))?;
        if record.state != LocalChunkState::Durable {
            return Err(corrupt(chunk_id, "local copy is quarantined"));
        }
        let path = self.path_for_record(&record)?;
        let file = File::open(path).map_err(Error::from).and_then(|file| {
            verified_range(&file, &record.chunk, 0, 0)?;
            Ok(file)
        });
        let file = match file {
            Ok(file) => file,
            Err(error) => {
                if verified_copy_is_invalid(&error) {
                    self.try_quarantine(chunk_id);
                }
                return Err(error);
            }
        };
        Ok(PinnedChunkReader {
            file,
            chunk: record.chunk,
        })
    }

    /// Recheck the current physical file under the publication lock. An old
    /// failed reader must not quarantine a healthy replacement inode.
    pub(crate) fn quarantine_if_invalid(&self, chunk_id: &ChunkId) -> Result<()> {
        let mut catalog = self
            .catalog
            .lock()
            .map_err(|_| invalid("local chunk catalog lock is poisoned"))?;
        let Some(record) = catalog.records.get(chunk_id).cloned() else {
            return Ok(());
        };
        if record.state == LocalChunkState::Quarantined {
            return Ok(());
        }
        match verify_file(&self.path_for_record(&record)?, &record.chunk) {
            Ok(()) => Ok(()),
            Err(error) if verified_copy_is_invalid(&error) => {
                self.quarantine_records(&mut catalog, vec![record])
            }
            Err(error) => Err(error),
        }
    }

    pub(crate) fn try_quarantine(&self, chunk_id: &ChunkId) {
        if let Err(error) = self.quarantine_if_invalid(chunk_id) {
            // Read fallback can still succeed. No durable quarantine or report
            // is claimed if its catalog barrier fails; every read rechecks.
            afs_logging::warn!("dfs.local_quarantine_failed";
                "chunk" => chunk_id.0.clone(), "error" => error.to_string());
        }
    }

    pub(crate) fn quarantined_chunks(&self) -> Result<Vec<LocalChunkRecord>> {
        let catalog = self
            .catalog
            .lock()
            .map_err(|_| invalid("local chunk catalog lock is poisoned"))?;
        let mut records: Vec<_> = catalog
            .records
            .values()
            .filter(|record| record.state == LocalChunkState::Quarantined)
            .cloned()
            .collect();
        records.sort_by(|left, right| {
            (left.catalog_revision, &left.chunk.id).cmp(&(right.catalog_revision, &right.chunk.id))
        });
        Ok(records)
    }

    fn quarantine_records(
        &self,
        catalog: &mut LocalCatalog,
        mut records: Vec<LocalChunkRecord>,
    ) -> Result<()> {
        let revision = catalog
            .revision
            .checked_add(1)
            .ok_or_else(|| invalid("local catalog revision exhausted"))?;
        for record in &mut records {
            record.state = LocalChunkState::Quarantined;
            record.catalog_revision = revision;
        }
        append_catalog_txn(
            &self.catalog_path,
            &CatalogTxn::new(revision, records.clone())?,
        )?;
        for record in records {
            catalog.records.insert(record.chunk.id.clone(), record);
        }
        catalog.revision = revision;
        Ok(())
    }
}

fn verified_copy_is_invalid(error: &Error) -> bool {
    matches!(
        error.kind(),
        afs_error::ErrorKind::DataLoss | afs_error::ErrorKind::NotFound
    )
}

#[cfg(test)]
impl ChunkStore for LocalChunkStore {
    fn put_batch(&self, staged: Vec<StagedChunk>) -> Result<Vec<ChunkReceipt>> {
        let descriptor = self.device_descriptor()?;
        let target = ReplicaTarget {
            node_id: self.node_id.clone(),
            node_epoch: 1,
            data_endpoint: String::new(),
            device: descriptor,
        };
        let acks = self.persist_batch(&staged, &target, 1, 1)?;
        Ok(staged
            .into_iter()
            .zip(acks)
            .map(|(item, ack)| ChunkReceipt {
                operation_id: item.operation_id,
                chunk: item.chunk,
                placement_revision: 1,
                placement_epoch: 1,
                replica_group_id: ReplicaGroupId::new(format!("local:{}", self.node_id)),
                durable_acks: vec![ack],
            })
            .collect())
    }

    fn read_at(&self, chunk_id: &ChunkId, offset: u64, out: &mut [u8]) -> Result<usize> {
        LocalChunkStore::read_at(self, chunk_id, offset, out)
    }

    fn capacity(&self) -> Result<FilesystemCapacity> {
        LocalChunkStore::capacity(self)
    }
}

fn append_catalog_txn(path: &Path, txn: &CatalogTxn) -> Result<()> {
    let mut encoded = serde_json::to_vec(txn)
        .map_err(|error| invalid(format!("cannot encode local chunk catalog: {error}")))?;
    encoded.push(b'\n');
    let mut file = OpenOptions::new()
        .create(true)
        .append(true)
        .open(path)
        .map_err(Error::from)?;
    file.write_all(&encoded).map_err(Error::from)?;
    file.sync_all().map_err(Error::from)?;
    let parent = path
        .parent()
        .ok_or_else(|| invalid("local chunk catalog has no parent directory"))?;
    File::open(parent)
        .and_then(|directory| directory.sync_all())
        .map_err(Error::from)
}

fn catalog_checksum(revision: u64, records: &[LocalChunkRecord]) -> Result<String> {
    let encoded = serde_json::to_vec(&(revision, records))
        .map_err(|error| invalid(format!("cannot checksum local chunk catalog: {error}")))?;
    Ok(blake3::hash(&encoded).to_hex().to_string())
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
    verified_range(&file, chunk, 0, 0).map(|_| ())
}

/// Hash the pinned file once and retain only the requested overlap. The
/// temporary range is bounded by the caller's output; scan memory is 64KiB.
/// Nothing is published to the caller until all content checks pass.
fn verified_range(file: &File, chunk: &ChunkObject, offset: u64, length: usize) -> Result<Vec<u8>> {
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
    )?;
    Ok(bytes)
}

fn verified_ranges_into(
    file: &File,
    chunk: &ChunkObject,
    ranges: &[VerifiedRangeCopy],
    out: &mut [u8],
    force_scan: bool,
) -> Result<Vec<usize>> {
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
        return Ok(counts);
    }
    if file.metadata().map_err(Error::from)?.len() != chunk.length {
        return Err(corrupt(&chunk.id, "length mismatch"));
    }
    let mut scratch = [0; 64 * 1024];
    let mut hasher = blake3::Hasher::new();
    let mut position = 0;
    while position < chunk.length {
        let limit = (chunk.length - position).min(scratch.len() as u64) as usize;
        let count = read_positioned(file, position, &mut scratch[..limit])?;
        if count == 0 {
            return Err(corrupt(&chunk.id, "ended during verification"));
        }
        hasher.update(&scratch[..count]);
        let next = position + count as u64;
        for (range, allowed) in &active {
            let range_end = range.chunk_offset + *allowed as u64;
            let overlap_start = position.max(range.chunk_offset);
            let overlap_end = next.min(range_end);
            if overlap_start < overlap_end {
                let output_start =
                    range.output_offset + (overlap_start - range.chunk_offset) as usize;
                let output_end = range.output_offset + (overlap_end - range.chunk_offset) as usize;
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
    let actual = ContentDigest {
        algorithm: DigestAlgorithm::Blake3,
        bytes: *hasher.finalize().as_bytes(),
    };
    if actual != chunk.content_digest {
        return Err(corrupt(&chunk.id, "digest mismatch"));
    }
    Ok(counts)
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
mod replica_replay_tests {
    use super::*;

    #[test]
    fn corrupt_cataloged_chunk_is_replaced_by_immutable_retry() {
        let temp = tempfile::tempdir().unwrap();
        let store = LocalChunkStore::open(temp.path(), "node-a").unwrap();
        let staged = StagedChunk::new(OperationId::new("retry-corrupt"), b"abcdef".to_vec());
        let first = store.put(staged.clone()).unwrap();
        let pin = store.open_verified(&staged.chunk.id).unwrap();
        fs::write(
            temp.path().join("chunks").join(&staged.chunk.id.0),
            b"abXdef",
        )
        .unwrap();
        let repaired = store.put(staged.clone()).unwrap();
        assert!(repaired.durable_acks[0].catalog_revision > first.durable_acks[0].catalog_revision);
        let mut out = [9; 6];
        assert!(pin.read_at(0, &mut out).is_err());
        assert_eq!(out, [9; 6]);
        // Revalidating an old pin cannot quarantine the newer healthy file.
        store.quarantine_if_invalid(&staged.chunk.id).unwrap();
        assert!(store.quarantined_chunks().unwrap().is_empty());
        assert_eq!(store.read_at(&staged.chunk.id, 0, &mut out).unwrap(), 6);
        assert_eq!(&out, b"abcdef");
        drop(store);
        let recovered = LocalChunkStore::open(temp.path(), "node-a").unwrap();
        assert_eq!(recovered.read_at(&staged.chunk.id, 0, &mut out).unwrap(), 6);
        assert_eq!(&out, b"abcdef");
    }

    #[test]
    fn capacity_reports_fd_backed_root_after_path_replacement_and_catalog_advance() {
        let temp = tempfile::tempdir().unwrap();
        let root = temp.path().join("store");
        let store = LocalChunkStore::open(&root, "node-a").unwrap();
        let first = store.capacity().unwrap();
        assert!(first.bsize > 0);
        assert!(first.frsize > 0);
        assert!(first.namelen > 0);
        assert!(first.blocks >= first.bfree);
        assert!(first.bfree >= first.bavail);

        store
            .put(StagedChunk::new(
                OperationId::new("capacity"),
                b"bytes".to_vec(),
            ))
            .unwrap();
        let descriptor_after_write = store.device_descriptor().unwrap();
        assert!(descriptor_after_write.catalog_revision > 0);

        let moved = temp.path().join("store-moved");
        fs::rename(&root, &moved).unwrap();
        // A path-based reopen must fail; the held directory FD remains valid.
        std::os::unix::fs::symlink(temp.path().join("missing"), &root).unwrap();

        let after_replacement = store.capacity().unwrap();
        assert!(after_replacement.bsize > 0);
        assert!(after_replacement.frsize > 0);
        assert_eq!(store.device_descriptor().unwrap(), descriptor_after_write);
    }

    #[test]
    fn quarantine_survives_restart_and_verified_replacement_clears_it() {
        let temp = tempfile::tempdir().unwrap();
        let store = LocalChunkStore::open(temp.path(), "node-a").unwrap();
        let staged = StagedChunk::new(OperationId::new("quarantine"), b"abcdef".to_vec());
        store.put(staged.clone()).unwrap();
        fs::write(
            temp.path().join("chunks").join(&staged.chunk.id.0),
            b"abXdef",
        )
        .unwrap();
        assert!(store.open_verified(&staged.chunk.id).is_err());
        let records = store.quarantined_chunks().unwrap();
        assert_eq!(records.len(), 1);
        let quarantine_revision = records[0].catalog_revision;
        drop(store);
        let recovered = LocalChunkStore::open(temp.path(), "node-a").unwrap();
        assert_eq!(recovered.quarantined_chunks().unwrap(), records);
        assert!(recovered.open_verified(&staged.chunk.id).is_err());
        let receipt = recovered.put(staged.clone()).unwrap();
        assert!(receipt.durable_acks[0].catalog_revision > quarantine_revision);
        assert!(recovered.quarantined_chunks().unwrap().is_empty());
        let mut out = [0; 6];
        recovered.read_at(&staged.chunk.id, 0, &mut out).unwrap();
        assert_eq!(&out, b"abcdef");
    }

    #[test]
    fn recovery_quarantines_missing_or_truncated_chunks_without_losing_healthy_files() {
        let temp = tempfile::tempdir().unwrap();
        let store = LocalChunkStore::open(temp.path(), "node-a").unwrap();
        let items: Vec<_> = ["missing", "truncated", "healthy"]
            .into_iter()
            .map(|name| StagedChunk::new(OperationId::new(name), name.as_bytes().to_vec()))
            .collect();
        for item in &items {
            store.put(item.clone()).unwrap();
        }
        fs::remove_file(temp.path().join("chunks").join(&items[0].chunk.id.0)).unwrap();
        fs::write(temp.path().join("chunks").join(&items[1].chunk.id.0), b"x").unwrap();
        drop(store);
        let recovered = LocalChunkStore::open(temp.path(), "node-a").unwrap();
        assert_eq!(recovered.quarantined_chunks().unwrap().len(), 2);
        let mut out = [0; 7];
        recovered.read_at(&items[2].chunk.id, 0, &mut out).unwrap();
        assert_eq!(&out, b"healthy");
        for item in &items[..2] {
            recovered.put(item.clone()).unwrap();
        }
        assert!(recovered.quarantined_chunks().unwrap().is_empty());
    }

    #[test]
    fn pinned_reader_verifies_partial_ranges_and_rejects_damage_outside_them() {
        let temp = tempfile::tempdir().unwrap();
        let store = LocalChunkStore::open(temp.path(), "node-a").unwrap();
        let bytes: Vec<_> = (0..524_289).map(|index| (index % 251) as u8).collect();
        let staged = StagedChunk::new(OperationId::new("range"), bytes.clone());
        store.put(staged.clone()).unwrap();
        let reader = store.open_verified(&staged.chunk.id).unwrap();
        let mut out = [9; 19];
        // Cover both the old 64KiB and candidate 256KiB scan boundaries,
        // including a final short scan outside either requested range.
        for offset in [65_531, 262_139] {
            assert_eq!(reader.read_at(offset, &mut out).unwrap(), 19);
            assert_eq!(&out, &bytes[offset as usize..offset as usize + 19]);
        }
        let path = temp.path().join("chunks").join(&staged.chunk.id.0);
        let mut damaged = bytes;
        damaged[524_288] ^= 1;
        fs::write(path, damaged).unwrap();
        out.fill(9);
        assert_eq!(
            reader.read_at(65_531, &mut out).unwrap_err().code(),
            afs_error::NODE_TRANSFER_CORRUPT_DATA
        );
        assert_eq!(out, [9; 19]);
    }

    #[test]
    fn verify_file_zero_range_still_hashes_the_whole_chunk() {
        let temp = tempfile::tempdir().unwrap();
        let store = LocalChunkStore::open(temp.path(), "node-a").unwrap();
        let staged = StagedChunk::new(OperationId::new("verify-zero"), b"abcdef".to_vec());
        store.put(staged.clone()).unwrap();
        let path = temp.path().join("chunks").join(&staged.chunk.id.0);
        verify_file(&path, &staged.chunk).unwrap();
        fs::write(&path, b"abcXef").unwrap();
        assert_eq!(
            verify_file(&path, &staged.chunk).unwrap_err().code(),
            afs_error::NODE_TRANSFER_CORRUPT_DATA
        );
    }

    #[test]
    fn pinned_reader_rejects_truncation_without_publishing_a_partial_read() {
        let temp = tempfile::tempdir().unwrap();
        let store = LocalChunkStore::open(temp.path(), "node-a").unwrap();
        let staged = StagedChunk::new(OperationId::new("truncate"), b"abcdef".to_vec());
        store.put(staged.clone()).unwrap();
        let reader = store.open_verified(&staged.chunk.id).unwrap();
        fs::write(temp.path().join("chunks").join(&staged.chunk.id.0), b"abc").unwrap();
        let mut out = [9; 6];
        assert_eq!(
            reader.read_at(0, &mut out).unwrap_err().code(),
            afs_error::NODE_TRANSFER_CORRUPT_DATA
        );
        assert_eq!(out, [9; 6]);
    }

    #[test]
    fn pinned_reader_preserves_descriptor_identity_and_eof_behavior() {
        let temp = tempfile::tempdir().unwrap();
        let store = LocalChunkStore::open(temp.path(), "node-a").unwrap();
        let staged = StagedChunk::new(OperationId::new("pin"), b"abcdef".to_vec());
        store.put(staged.clone()).unwrap();
        let reader = store.open_verified(&staged.chunk.id).unwrap();
        let replacement = temp.path().join("replacement");
        fs::write(&replacement, b"abXdef").unwrap();
        fs::rename(
            replacement,
            temp.path().join("chunks").join(&staged.chunk.id.0),
        )
        .unwrap();
        let mut out = [9; 8];
        assert_eq!(reader.read_at(0, &mut out).unwrap(), 6);
        assert_eq!(&out, &[b'a', b'b', b'c', b'd', b'e', b'f', 9, 9]);
        out.fill(9);
        assert_eq!(reader.read_at(4, &mut out).unwrap(), 2);
        assert_eq!(&out, &[b'e', b'f', 9, 9, 9, 9, 9, 9]);
        assert_eq!(reader.read_at(u64::MAX, &mut out).unwrap(), 0);
        assert_eq!(reader.read_at(0, &mut []).unwrap(), 0);
        assert!(store.open_verified(&staged.chunk.id).is_err());
    }

    #[test]
    fn immutable_chunk_retry_preserves_content_after_catalog_advances_and_restart() {
        let temp = tempfile::tempdir().unwrap();
        let store = LocalChunkStore::open(temp.path(), "node-a").unwrap();
        let target = ReplicaTarget {
            node_id: "node-a".into(),
            node_epoch: 3,
            data_endpoint: "https://node-a".into(),
            device: store.device_descriptor().unwrap(),
        };
        let first = StagedChunk::new(OperationId::new("op-first"), b"first".to_vec());
        let ack = store.persist(&first, &target, 4, 5).unwrap();
        store
            .persist(
                &StagedChunk::new(OperationId::new("op-second"), b"second".to_vec()),
                &target,
                4,
                5,
            )
            .unwrap();
        let replay = store.persist(&first, &target, 4, 5).unwrap();
        assert_eq!(replay.chunk_id, ack.chunk_id);
        assert_eq!(replay.verified_digest, ack.verified_digest);
        assert_eq!(replay.persisted_bytes, ack.persisted_bytes);
        assert!(replay.catalog_revision > ack.catalog_revision);
        let mut newer_target = target.clone();
        newer_target.device = store.device_descriptor().unwrap();
        let floor_ack = store.persist(&first, &newer_target, 6, 7).unwrap();
        assert_eq!(
            floor_ack.catalog_revision,
            newer_target.device.catalog_revision
        );
        drop(store);
        let recovered = LocalChunkStore::open(temp.path(), "node-a").unwrap();
        let recovered_ack = recovered.persist(&first, &target, 4, 5).unwrap();
        assert_eq!(recovered_ack.chunk_id, ack.chunk_id);
        assert_eq!(recovered_ack.verified_digest, ack.verified_digest);
        assert_eq!(recovered_ack.catalog_revision, replay.catalog_revision);
        let mut bytes = [0; 5];
        assert_eq!(
            recovered.read_at(&first.chunk.id, 0, &mut bytes).unwrap(),
            5
        );
        assert_eq!(&bytes, b"first");
        assert_eq!(
            recovered.persist(&first, &newer_target, 6, 7).unwrap(),
            floor_ack
        );
    }
}
