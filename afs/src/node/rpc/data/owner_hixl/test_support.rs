//! In-memory native export boundary and Home file source shared by the Home
//! service tests and the reader loopback tests.

use super::{
    BatchEntry, ExportProvider, OwnerBatchDirectory, OwnerBatchSource, OwnerWindowSource,
    PresentedRootAccess, RemoteFile,
};
use crate::node::rpc::hixl::test_support::{engine, native_error};
use afs_transport::hixl::{
    EngineInfo, ExportWindow, HixlError, HixlErrorKind, HixlPhase, ReservedExport,
};
use std::collections::{BTreeMap, HashMap, HashSet};
use std::ffi::{OsStr, OsString};
use std::os::unix::ffi::OsStrExt;
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::sync::{Arc, Mutex, mpsc};
use std::time::{Duration, Instant};

pub(crate) const HOME_ENGINE: &str = "127.0.0.1:7930";
pub(crate) const OPEN_HANDLE: &[u8] = b"open-1";

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
    /// Reports a full pool on the export call with this ordinal (1-based).
    pub(crate) fail_export_exhausted_at: AtomicUsize,
    /// Reports an unsupported prepare before publishing any slots.
    pub(crate) fail_export_unsupported_at: AtomicUsize,
    pub(crate) fail_release: AtomicBool,
    /// Fails the next quarantine call once.
    pub(crate) fail_quarantine: AtomicBool,
    pause: Mutex<Option<(mpsc::SyncSender<()>, mpsc::Receiver<()>)>>,
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
            fail_export_exhausted_at: AtomicUsize::new(0),
            fail_export_unsupported_at: AtomicUsize::new(0),
            fail_release: AtomicBool::new(false),
            fail_quarantine: AtomicBool::new(false),
            pause: Mutex::new(None),
        }
    }

    pub(crate) fn active(&self) -> usize {
        self.memory.lock().unwrap().active.len()
    }

    /// Pauses the next reserve after taking a slot, with the pool lock released.
    pub(crate) fn pause_next_export(&self) -> (mpsc::Receiver<()>, mpsc::SyncSender<()>) {
        let (entered, wait_for_entry) = mpsc::sync_channel(1);
        let (resume, wait_for_resume) = mpsc::sync_channel(1);
        *self.pause.lock().unwrap() = Some((entered, wait_for_resume));
        (wait_for_entry, resume)
    }

    /// The native READ view of an exported slot.
    pub(crate) fn read(&self, address: u64, length: usize) -> Option<Vec<u8>> {
        let memory = self.memory.lock().unwrap();
        memory
            .bytes
            .get(&address)
            .filter(|bytes| bytes.len() >= length)
            .map(|bytes| bytes[..length].to_vec())
    }
}

impl ExportProvider for MemoryExports {
    fn info(&self) -> EngineInfo {
        self.info.clone()
    }

    fn export_slots(&self, _lane: usize) -> usize {
        self.slots
    }

    fn reserve_exports(
        &self,
        _lane: usize,
        lengths: &[usize],
        _deadline: Instant,
    ) -> Result<Vec<ReservedExport>, HixlError> {
        let mut group = Vec::with_capacity(lengths.len());
        for &length in lengths {
            match self.reserve_one(length) {
                Ok(slot) => group.push(slot),
                Err(error) => {
                    for slot in group {
                        let window = slot.window();
                        self.release_one(window.slot_id, window.generation)?;
                    }
                    return Err(error);
                }
            }
        }
        Ok(group)
    }

    unsafe fn release_exports(
        &self,
        _lane: usize,
        slots: Vec<(u64, u64)>,
    ) -> Result<(), HixlError> {
        for (id, generation) in slots {
            self.release_one(id, generation)?;
        }
        Ok(())
    }

    fn quarantine(&self) -> Result<(), HixlError> {
        self.quarantines.fetch_add(1, Ordering::SeqCst);
        if self.fail_quarantine.swap(false, Ordering::SeqCst) {
            return Err(native_error(
                HixlPhase::Quarantine,
                HixlErrorKind::ResourceExhausted,
                "injected quarantine failure",
            ));
        }
        Ok(())
    }
}

impl MemoryExports {
    fn reserve_one(&self, length: usize) -> Result<ReservedExport, HixlError> {
        let ordinal = self.exports.fetch_add(1, Ordering::SeqCst) + 1;
        if self.fail_export_unsupported_at.load(Ordering::SeqCst) == ordinal {
            return Err(native_error(
                HixlPhase::PrepareExport,
                HixlErrorKind::Unsupported,
                "injected unsupported prepare",
            ));
        }
        if self.fail_export_exhausted_at.load(Ordering::SeqCst) == ordinal {
            return Err(native_error(
                HixlPhase::PrepareExport,
                HixlErrorKind::ResourceExhausted,
                "injected full export pool",
            ));
        }
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
        // A slot holds stale bytes until it is filled, as native memory does.
        let bytes = memory.bytes.entry(address).or_insert(vec![0xA5; length]);
        let window = ExportWindow {
            slot_id,
            generation,
            address,
            length,
        };
        // SAFETY: the initialized Vec stays in `memory.bytes` until release.
        // The prepare holds this provider, and the fake reader only accesses
        // slots from published descriptors, once filling has finished.
        let reservation = unsafe { ReservedExport::from_raw(window, bytes.as_mut_ptr()) };
        drop(memory);
        let pause = self.pause.lock().unwrap().take();
        if let Some((entered, resume)) = pause {
            entered.send(()).unwrap();
            resume.recv_timeout(Duration::from_secs(5)).unwrap();
        }
        Ok(reservation)
    }

    fn release_one(&self, slot_id: u64, generation: u64) -> Result<(), HixlError> {
        if self.fail_release.load(Ordering::SeqCst) {
            return Err(native_error(
                HixlPhase::ReleaseExport,
                HixlErrorKind::EnginePoisoned,
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
}

/// One Home file behind the `open-1` handle of root `root-a`.
/// Every open of the file uses a handle that starts with `OPEN_HANDLE`.
pub(crate) struct MemorySource {
    pub(crate) data: Mutex<Vec<u8>>,
    /// Non-empty source reads; empty reads are pure authorization checks.
    pub(crate) reads: AtomicUsize,
    max_chunk: usize,
    overlap: Option<OverlapProbe>,
    /// A non-empty read covering one of these offsets fails with its error code.
    failing_offsets: Mutex<Vec<(u64, afs_error::ErrorCode)>>,
    /// Files a batch prepare can list, keyed by root-relative path. An open
    /// handle `OPEN_HANDLE#<path>` reads the tree file at `<path>`.
    pub(crate) tree: Arc<MemoryTree>,
}

/// One regular file or other entry of the in-memory tree.
#[derive(Clone)]
pub(crate) struct MemoryEntry {
    pub(crate) data: Vec<u8>,
    pub(crate) regular: bool,
    pub(crate) identity: Vec<u8>,
    pub(crate) mtime_ns: u64,
}

impl MemoryEntry {
    fn batch_entry(&self) -> BatchEntry {
        BatchEntry {
            identity: self.identity.clone(),
            size: self.data.len() as u64,
            mtime_ns: self.mtime_ns,
            ctime_ns: self.mtime_ns,
        }
    }
}

/// Directory tree behind batch prepares and `OPEN_HANDLE#<path>` handles.
#[derive(Default)]
pub(crate) struct MemoryTree {
    entries: Mutex<BTreeMap<Vec<u8>, MemoryEntry>>,
    /// Batch opens of these paths fail.
    failing_opens: Mutex<HashSet<Vec<u8>>>,
    /// These paths are rewritten between the batch stat and its open.
    rewrite_on_open: Mutex<HashSet<Vec<u8>>>,
    pub(crate) listings: AtomicUsize,
    directory_version: AtomicUsize,
    pub(crate) batch_opens: AtomicUsize,
}

impl MemoryTree {
    /// Adds a regular file whose identity is derived from its path.
    pub(crate) fn insert(&self, path: &str, data: Vec<u8>) {
        self.directory_version.fetch_add(1, Ordering::SeqCst);
        self.entries.lock().unwrap().insert(
            path.as_bytes().to_vec(),
            MemoryEntry {
                data,
                regular: true,
                identity: format!("identity:{path}").into_bytes(),
                mtime_ns: 1,
            },
        );
    }

    pub(crate) fn insert_directory(&self, path: &str) {
        self.directory_version.fetch_add(1, Ordering::SeqCst);
        self.entries.lock().unwrap().insert(
            path.as_bytes().to_vec(),
            MemoryEntry {
                data: Vec::new(),
                regular: false,
                identity: format!("identity:{path}").into_bytes(),
                mtime_ns: 1,
            },
        );
    }

    /// Replaces the contents in place: same identity, a later mtime.
    pub(crate) fn write(&self, path: &str, data: Vec<u8>) {
        if let Some(entry) = self.entries.lock().unwrap().get_mut(path.as_bytes()) {
            entry.data = data;
            entry.mtime_ns += 1;
        }
    }

    pub(crate) fn get(&self, path: &[u8]) -> Option<MemoryEntry> {
        self.entries.lock().unwrap().get(path).cloned()
    }

    pub(crate) fn fail_open(&self, path: &str) {
        self.failing_opens
            .lock()
            .unwrap()
            .insert(path.as_bytes().to_vec());
    }

    pub(crate) fn rewrite_on_open(&self, path: &str) {
        self.rewrite_on_open
            .lock()
            .unwrap()
            .insert(path.as_bytes().to_vec());
    }
}

/// The tree path an `OPEN_HANDLE#<path>` handle names.
pub(crate) fn tree_path(handle: &[u8]) -> Option<&[u8]> {
    let rest = handle.strip_prefix(OPEN_HANDLE)?;
    let index = rest.iter().position(|byte| *byte == b'#')?;
    Some(&rest[index + 1..])
}

fn child_path(parent: &[u8], name: &[u8]) -> Vec<u8> {
    if parent.is_empty() {
        name.to_vec()
    } else {
        [parent, b"/", name].concat()
    }
}

struct MemoryDirectory {
    tree: Arc<MemoryTree>,
    parent: Vec<u8>,
}

impl OwnerBatchDirectory for MemoryDirectory {
    fn version(&self) -> afs_error::Result<super::PeerBatchDirectoryVersion> {
        Ok(super::PeerBatchDirectoryVersion::for_test(
            self.tree.directory_version.load(Ordering::SeqCst),
        ))
    }

    fn names(&self) -> afs_error::Result<Vec<OsString>> {
        self.tree.listings.fetch_add(1, Ordering::SeqCst);
        let prefix = child_path(&self.parent, b"");
        let mut names = self
            .tree
            .entries
            .lock()
            .unwrap()
            .keys()
            .filter_map(|path| {
                let name = if self.parent.is_empty() {
                    path.as_slice()
                } else {
                    path.strip_prefix(prefix.as_slice())?
                };
                (!name.is_empty() && !name.contains(&b'/'))
                    .then(|| OsStr::from_bytes(name).to_owned())
            })
            .collect::<Vec<_>>();
        names.sort();
        Ok(names)
    }

    fn stat(&self, name: &OsStr) -> afs_error::Result<Option<BatchEntry>> {
        let entry = self
            .tree
            .get(&child_path(&self.parent, name.as_bytes()))
            .ok_or_else(|| afs_error::Error::coded(afs_error::NODE_VFS_NOT_FOUND, "no entry"))?;
        Ok(entry.regular.then(|| entry.batch_entry()))
    }

    fn open(&self, name: &OsStr) -> afs_error::Result<Box<dyn OwnerBatchSource>> {
        self.tree.batch_opens.fetch_add(1, Ordering::SeqCst);
        let path = child_path(&self.parent, name.as_bytes());
        if self.tree.failing_opens.lock().unwrap().contains(&path) {
            return Err(afs_error::Error::coded(
                afs_error::NODE_VFS_NOT_FOUND,
                "injected open failure",
            ));
        }
        if self.tree.rewrite_on_open.lock().unwrap().contains(&path)
            && let Some(entry) = self.tree.entries.lock().unwrap().get_mut(&path)
        {
            entry
                .data
                .iter_mut()
                .for_each(|byte| *byte = byte.wrapping_add(1));
            entry.mtime_ns += 1;
        }
        let entry = self
            .tree
            .get(&path)
            .filter(|entry| entry.regular)
            .ok_or_else(|| afs_error::Error::coded(afs_error::NODE_VFS_NOT_FOUND, "no file"))?;
        Ok(Box::new(MemoryBatchSource {
            batch: entry.batch_entry(),
            data: entry.data,
        }))
    }
}

struct MemoryBatchSource {
    batch: BatchEntry,
    data: Vec<u8>,
}

impl OwnerBatchSource for MemoryBatchSource {
    fn entry(&self) -> &BatchEntry {
        &self.batch
    }

    fn read_at(&self, offset: u64, out: &mut [u8]) -> afs_error::Result<usize> {
        let start = usize::try_from(offset)
            .unwrap_or(usize::MAX)
            .min(self.data.len());
        // Short positional reads prove the prepare loops until a file is whole.
        let count = (self.data.len() - start).min(out.len()).min(3000);
        out[..count].copy_from_slice(&self.data[start..start + count]);
        Ok(count)
    }
}

/// Holds the first source reads until `target` of them run at once or the
/// deadline passes, and records the largest concurrency it observed.
struct OverlapProbe {
    target: usize,
    deadline: Mutex<Option<Instant>>,
    active: AtomicUsize,
    peak: AtomicUsize,
}

impl MemorySource {
    pub(crate) fn new(data: Vec<u8>, max_chunk: usize) -> Self {
        Self {
            data: Mutex::new(data),
            reads: AtomicUsize::new(0),
            max_chunk,
            overlap: None,
            failing_offsets: Mutex::new(Vec::new()),
            tree: Arc::new(MemoryTree::default()),
        }
    }

    /// Makes every non-empty read whose range covers `offset` fail with `code`.
    pub(crate) fn fail_read_at(&self, offset: u64, code: afs_error::ErrorCode) {
        self.failing_offsets.lock().unwrap().push((offset, code));
    }

    pub(crate) fn with_overlap_probe(mut self, target: usize) -> Self {
        self.overlap = Some(OverlapProbe {
            target,
            deadline: Mutex::new(None),
            active: AtomicUsize::new(0),
            peak: AtomicUsize::new(0),
        });
        self
    }

    /// The largest number of source reads that ran at the same time.
    pub(crate) fn peak_overlap(&self) -> usize {
        self.overlap
            .as_ref()
            .map_or(0, |probe| probe.peak.load(Ordering::SeqCst))
    }

    fn enter_probe(&self) {
        let Some(probe) = &self.overlap else {
            return;
        };
        let active = probe.active.fetch_add(1, Ordering::SeqCst) + 1;
        probe.peak.fetch_max(active, Ordering::SeqCst);
        let deadline = *probe
            .deadline
            .lock()
            .unwrap()
            .get_or_insert_with(|| Instant::now() + std::time::Duration::from_millis(500));
        while Instant::now() < deadline && probe.peak.load(Ordering::SeqCst) < probe.target {
            std::thread::sleep(std::time::Duration::from_millis(2));
        }
    }

    fn exit_probe(&self) {
        if let Some(probe) = &self.overlap {
            probe.active.fetch_sub(1, Ordering::SeqCst);
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
        if !file.handle.starts_with(OPEN_HANDLE) {
            return Err(afs_error::Error::coded(
                afs_error::NODE_OWNER_STALE_HANDLE,
                "unknown open handle",
            ));
        }
        if out.is_empty() {
            return Ok(0);
        }
        self.reads.fetch_add(1, Ordering::SeqCst);
        if let Some((_, code)) = self
            .failing_offsets
            .lock()
            .unwrap()
            .iter()
            .find(|(failing, _)| {
                (offset..offset.saturating_add(out.len().min(self.max_chunk) as u64))
                    .contains(failing)
            })
        {
            return Err(afs_error::Error::coded(
                *code,
                "injected source read failure",
            ));
        }
        self.enter_probe();
        let data = match tree_path(&file.handle) {
            Some(path) => self
                .tree
                .get(path)
                .map(|entry| entry.data)
                .unwrap_or_default(),
            None => self.data.lock().unwrap().clone(),
        };
        self.exit_probe();
        let start = usize::try_from(offset)
            .unwrap_or(usize::MAX)
            .min(data.len());
        let count = (data.len() - start).min(out.len()).min(self.max_chunk);
        out[..count].copy_from_slice(&data[start..start + count]);
        Ok(count)
    }

    fn batch_directory(
        &self,
        peer: &str,
        access: &PresentedRootAccess,
        parent: &OsStr,
    ) -> afs_error::Result<Box<dyn OwnerBatchDirectory>> {
        self.authorize_transport(peer, access)?;
        if access.fencing_token == "revoked" {
            return Err(afs_error::Error::coded(
                afs_error::NODE_OWNER_INVALID_GRANT,
                "revoked batch grant",
            ));
        }
        let parent = parent.as_bytes().to_vec();
        if !parent.is_empty() && self.tree.get(&parent).is_none_or(|entry| entry.regular) {
            return Err(afs_error::Error::coded(
                afs_error::NODE_VFS_NOT_FOUND,
                "no such directory",
            ));
        }
        Ok(Box::new(MemoryDirectory {
            tree: self.tree.clone(),
            parent,
        }))
    }
}
