//! Per-open read windows, readahead, and directory batch claims for OwnerFs.

use super::batch::{
    BatchFetched, BatchRequest, BatchRoute, BatchStore, Claim, FileVersion, batch_cap,
    batch_reservation,
};
use super::{
    FetchRequest, Fetched, OwnerHixlReader, ReadOutcome, protocol_error, remaining,
    unsupported_read,
};
use crate::config::DataMode;
use crate::node::rpc::peer::{OwnerRuntime, root_access};
use crate::node::vfs::ownerfs::{OPEN_PREFETCH_LIMIT, files::RemoteFile, root::RootGrant};
use crate::node::vfs::types::FileAttributes;
use afs_protocol::node_data::owner_files_client::OwnerFilesClient;
use std::collections::HashMap;
use std::ffi::OsStr;
use std::os::unix::ffi::OsStrExt;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex as StdMutex};
use std::time::Instant;
use tokio::sync::oneshot;
use tonic::transport::Channel;

/// A verified window owned by one open.
struct CachedWindow {
    offset: u64,
    /// Shared so a FUSE reply can slice a hit; a reply still in flight keeps
    /// its bytes after the window is replaced.
    bytes: prost::bytes::Bytes,
    eof: bool,
    epoch: u64,
}

impl CachedWindow {
    fn end(&self) -> u64 {
        self.offset.saturating_add(self.bytes.len() as u64)
    }

    /// A shared slice when the window holds the whole requested range, or the
    /// tail of a window that ends at EOF.
    fn slice_at(&self, cursor: u64, size: usize) -> Option<prost::bytes::Bytes> {
        let start = usize::try_from(cursor.checked_sub(self.offset)?).ok()?;
        if start >= self.bytes.len() {
            return None;
        }
        let end = start.checked_add(size)?;
        if end <= self.bytes.len() {
            Some(self.bytes.slice(start..end))
        } else {
            self.eof.then(|| self.bytes.slice(start..))
        }
    }

    /// Copies from `cursor` when it falls inside the window.
    fn copy_at(&self, cursor: u64, out: &mut [u8]) -> usize {
        if cursor < self.offset || cursor >= self.end() {
            return 0;
        }
        let Ok(start) = usize::try_from(cursor - self.offset) else {
            return 0;
        };
        let count = (self.bytes.len() - start).min(out.len());
        out[..count].copy_from_slice(&self.bytes[start..start + count]);
        count
    }
}

struct PendingWindow {
    offset: u64,
    epoch: u64,
    receiver: oneshot::Receiver<afs_error::Result<Fetched>>,
}

/// The directory-batch slice an open claimed; dropped after this node
/// changes the file, like a window.
struct ClaimedSlice {
    bytes: prost::bytes::Bytes,
    epoch: u64,
}

#[derive(Default)]
struct WindowState {
    window: Option<CachedWindow>,
    pending: Option<PendingWindow>,
    claimed: Option<ClaimedSlice>,
    next_offset: u64,
    ramp: usize,
    /// READs in a row that started where the previous one ended.
    streak: u32,
}

/// Consecutive sequential READs that confirm a stream after a jump. The kernel
/// readahead that follows one random read is a single sequential READ, so it
/// alone does not open a window.
const SEQUENTIAL_STREAK: u32 = 2;

struct OpenEntry {
    identity: Vec<u8>,
    epoch: AtomicU64,
    cacheable: bool,
    state: StdMutex<WindowState>,
    /// Set when a directory batch may serve this open.
    batch: Option<BatchRoute>,
}

/// Result of a window read for one caller buffer.
pub(in crate::node::rpc::peer) enum WindowRead {
    /// Every requested byte, or a short count that stops at a recorded EOF.
    Done(usize),
    /// The first `served` bytes came from windows; the caller reads the rest
    /// through the original gRPC path.
    Fallback { served: usize },
}

/// Per-`OwnerPeerClient` open registry. Absent for off mode, so every hook
/// below leaves the original read path untouched.
pub(in crate::node::rpc::peer) struct OwnerReadSession {
    pub(super) reader: Arc<OwnerHixlReader>,
    pub(super) home: String,
    opens: StdMutex<HashMap<Vec<u8>, Arc<OpenEntry>>>,
    batches: Arc<StdMutex<BatchStore>>,
    writers: StdMutex<std::collections::HashSet<Vec<u8>>>,
}

impl OwnerReadSession {
    pub(in crate::node::rpc::peer) fn new(reader: Arc<OwnerHixlReader>, home: String) -> Self {
        Self {
            reader,
            home,
            opens: StdMutex::new(HashMap::new()),
            writers: StdMutex::default(),
            batches: Arc::new(StdMutex::new(BatchStore::default())),
        }
    }

    fn batches(&self) -> std::sync::MutexGuard<'_, BatchStore> {
        self.batches
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
    }

    /// Starts a background session to this Home for a newly acquired grant.
    /// The private current-thread runtime cannot progress spawned tasks, so it
    /// leaves the first read to connect.
    pub(in crate::node::rpc::peer) fn warm(
        &self,
        runtime: &OwnerRuntime,
        client: &OwnerFilesClient<Channel>,
        grant: &RootGrant,
    ) {
        if let Some(handle) = runtime.handle() {
            self.reader.warm(
                &handle,
                self.home.clone(),
                client.clone(),
                root_access(grant),
            );
        }
    }

    pub(in crate::node::rpc::peer) fn allows_grpc_open_prefetch(&self) -> bool {
        self.reader.policy.mode == DataMode::Auto
            && self.reader.policy.min_file_bytes > OPEN_PREFETCH_LIMIT
    }

    fn opens(&self) -> std::sync::MutexGuard<'_, HashMap<Vec<u8>, Arc<OpenEntry>>> {
        self.opens
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
    }

    pub(in crate::node::rpc::peer) fn track_writer(&self, handle: &[u8], flags: i32) {
        if flags & (libc::O_PATH | libc::O_DIRECT) == 0
            && matches!(flags & libc::O_ACCMODE, libc::O_WRONLY | libc::O_RDWR)
        {
            self.writers
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner)
                .insert(handle.to_vec());
        }
    }

    pub(in crate::node::rpc::peer) fn is_buffered_writer(&self, handle: &[u8]) -> bool {
        self.writers
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .contains(handle)
    }

    /// Registers buffered file handles. Writable handles use demand reads only.
    pub(in crate::node::rpc::peer) fn opened(
        &self,
        grant: &RootGrant,
        path: &OsStr,
        handle: &[u8],
        identity: &[u8],
        flags: i32,
        attributes: &FileAttributes,
    ) {
        let policy = self.reader.policy;
        let size = attributes.size;
        if flags & (libc::O_PATH | libc::O_DIRECT) != 0 {
            return;
        }
        let mode = flags & libc::O_ACCMODE;
        self.track_writer(handle, flags);
        if mode != libc::O_RDONLY && mode != libc::O_RDWR {
            return;
        }
        let windowed = policy.mode == DataMode::Hixl || size >= policy.min_file_bytes;
        let cacheable = mode == libc::O_RDONLY;
        let batch =
            (cacheable && size > OPEN_PREFETCH_LIMIT && size <= policy.batch_max_file_bytes)
                .then(|| batch_route(grant, path, attributes, !windowed))
                .flatten();
        if windowed || batch.is_some() {
            self.opens().insert(
                handle.to_vec(),
                Arc::new(OpenEntry {
                    identity: identity.to_vec(),
                    epoch: AtomicU64::new(0),
                    cacheable,
                    state: StdMutex::new(WindowState::default()),
                    batch,
                }),
            );
        }
    }

    pub(in crate::node::rpc::peer) fn released(&self, handle: &[u8]) {
        self.writers
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .remove(handle);
        self.opens().remove(handle);
    }

    pub(in crate::node::rpc::peer) fn mark_uncertain_write(&self, identity: &[u8]) {
        self.reader
            .uncertain_files
            .get_or_init(Default::default)
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .mark(&self.home, identity);
    }

    pub(in crate::node::rpc::peer) fn cache_allowed(&self, identity: &[u8]) -> bool {
        self.reader.uncertain_files.get().is_none_or(|files| {
            let files = files
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner);
            !files.all
                && !files
                    .by_home
                    .get(&self.home)
                    .is_some_and(|ids| ids.contains(identity))
        })
    }

    /// Drops every window, prefetch and batch slice of a file after this node
    /// changed it, including slices of batches still in flight.
    pub(in crate::node::rpc::peer) fn invalidate(&self, identity: &[u8]) {
        if identity.is_empty() {
            return;
        }
        for entry in self.opens().values() {
            if entry.identity == identity {
                entry.epoch.fetch_add(1, Ordering::AcqRel);
            }
        }
        self.batches().invalidate(identity);
    }

    /// Answers a READ that lies in this open's cached window or batch slice
    /// with a shared slice, or `None` when it needs the copying path. A hit
    /// continues the stream exactly like a copied hit, so prefetch keeps its
    /// depth. `client` is only asked for when a batch claim starts a lookahead.
    pub(in crate::node::rpc::peer) fn slice_window(
        &self,
        runtime: &OwnerRuntime,
        client: impl Fn() -> Option<OwnerFilesClient<Channel>>,
        grant: &RootGrant,
        file: &RemoteFile,
        offset: u64,
        size: usize,
    ) -> Option<prost::bytes::Bytes> {
        let entry = self.opens().get(&file.handle).cloned()?;
        if file.owner_session_id != grant.home_session_id || !self.cache_allowed(&entry.identity) {
            return None;
        }
        let mut state = entry.state.lock().ok()?;
        let epoch = entry.epoch.load(Ordering::Acquire);
        if let Some(route) = &entry.batch {
            let handle = runtime.handle();
            // Only claims here; waiting for or starting a batch blocks, so it
            // belongs to the copying path.
            let client = state.claimed.is_none().then(client).flatten();
            if let Some(bytes) = self
                .batch_slice(
                    handle.as_ref(),
                    client.as_ref(),
                    grant,
                    &entry,
                    route,
                    &mut state,
                    epoch,
                    false,
                )
                .ok()
                .flatten()
            {
                let start = usize::try_from(offset)
                    .unwrap_or(usize::MAX)
                    .min(bytes.len());
                let end = start.saturating_add(size).min(bytes.len());
                state.next_offset = offset.saturating_add((end - start) as u64);
                self.served_batch(end - start);
                return Some(bytes.slice(start..end));
            }
        }
        let window = state
            .window
            .as_ref()
            .filter(|window| window.epoch == epoch)?;
        let hit = window.slice_at(offset, size)?;
        state.streak = if offset == state.next_offset {
            state.streak.saturating_add(1)
        } else {
            0
        };
        state.next_offset = offset.saturating_add(hit.len() as u64);
        self.reader.count(ReadOutcome::WindowHit);
        if let Some(metrics) = &self.reader.owner_metrics {
            metrics.record_payload("client", "read", "hixl", hit.len() as u64);
        }
        Some(hit)
    }

    /// Serves `out` from this open's windows, fetching missing windows through
    /// HIXL. Must run outside the async runtime, like every OwnerFiles call.
    pub(in crate::node::rpc::peer) fn read(
        &self,
        runtime: &OwnerRuntime,
        client: &OwnerFilesClient<Channel>,
        grant: &RootGrant,
        file: &RemoteFile,
        offset: u64,
        out: &mut [u8],
    ) -> afs_error::Result<WindowRead> {
        let Some(entry) = self.opens().get(&file.handle).cloned() else {
            return Ok(WindowRead::Fallback { served: 0 });
        };
        if file.owner_session_id != grant.home_session_id {
            return Err(afs_error::Error::coded(
                afs_error::NODE_OWNER_STALE_HANDLE,
                "OwnerFs read session was opened by a different Home process",
            ));
        }
        let Some(handle) = runtime.handle() else {
            return Ok(WindowRead::Fallback { served: 0 });
        };
        let mut state = entry
            .state
            .lock()
            .map_err(|_| protocol_error("OwnerFs HIXL window lock poisoned"))?;
        let epoch = entry.epoch.load(Ordering::Acquire);
        let cacheable = entry.cacheable && self.cache_allowed(&entry.identity);
        if !cacheable {
            // A lookup may have raced the invalidation on another peer client.
            *state = WindowState::default();
            if entry.batch.as_ref().is_some_and(|route| route.batch_only) {
                return Ok(WindowRead::Fallback { served: 0 });
            }
        }
        if state
            .window
            .as_ref()
            .is_some_and(|window| window.epoch != epoch)
        {
            state.window = None;
        }
        if state
            .pending
            .as_ref()
            .is_some_and(|pending| pending.epoch != epoch)
        {
            state.pending = None;
        }
        if cacheable && let Some(route) = &entry.batch {
            if let Some(bytes) = self.batch_slice(
                Some(&handle),
                Some(client),
                grant,
                &entry,
                route,
                &mut state,
                epoch,
                true,
            )? {
                let start = usize::try_from(offset)
                    .unwrap_or(usize::MAX)
                    .min(bytes.len());
                let count = (bytes.len() - start).min(out.len());
                out[..count].copy_from_slice(&bytes[start..start + count]);
                state.next_offset = offset.saturating_add(count as u64);
                self.served_batch(count);
                return Ok(WindowRead::Done(count));
            }
            self.reader.count(ReadOutcome::BatchFallback);
            if route.batch_only {
                state.next_offset = offset.saturating_add(out.len() as u64);
                return Ok(WindowRead::Fallback { served: 0 });
            }
        }
        state.streak = if offset == state.next_offset {
            state.streak.saturating_add(1)
        } else {
            0
        };
        // A stream starts at offset 0, or continues after a confirmed streak.
        let stream =
            offset == state.next_offset && (offset == 0 || state.streak >= SEQUENTIAL_STREAK);
        let reader = &self.reader;
        let policy = reader.policy;
        let mut served = 0_usize;
        let mut cursor = offset;
        let mut fetched = false;
        while served < out.len() {
            if let Some(window) = &state.window {
                let copied = window.copy_at(cursor, &mut out[served..]);
                if copied > 0 {
                    served += copied;
                    cursor += copied as u64;
                    continue;
                }
                if window.eof && cursor >= window.end() {
                    break;
                }
            }
            let wanted = out.len() - served;
            let sequential = served > 0 || stream;
            if let Some(pending) = state.pending.take()
                && pending.offset == cursor
            {
                let waited = handle.block_on(async {
                    match remaining(Instant::now() + reader.timeout) {
                        Ok(wait) => tokio::time::timeout(wait, pending.receiver).await.ok(),
                        Err(_) => None,
                    }
                });
                match waited {
                    Some(Ok(Ok(Fetched::Window(window)))) => {
                        if entry.epoch.load(Ordering::Acquire) == epoch {
                            state.window = Some(CachedWindow {
                                offset: window.offset,
                                bytes: window.bytes,
                                eof: window.eof,
                                epoch,
                            });
                            reader.count(ReadOutcome::PrefetchHit);
                            continue;
                        }
                    }
                    Some(Ok(Ok(Fetched::Fallback))) if policy.mode == DataMode::Auto => {
                        return self.fallback(&mut state, offset, served, out.len());
                    }
                    Some(Ok(Ok(Fetched::Fallback))) => return Err(unsupported_read()),
                    Some(Ok(Err(error))) => return Err(error),
                    Some(Err(error)) => {
                        return Err(afs_error::Error::coded(
                            afs_error::CLIENT_WORKER_FAILED,
                            error.to_string(),
                        ));
                    }
                    None => {
                        return Err(afs_error::Error::coded(
                            afs_error::CLIENT_DEADLINE_EXCEEDED,
                            "OwnerFs HIXL prefetch did not finish before the read deadline",
                        ));
                    }
                }
            }
            let size = if sequential && cacheable {
                state.ramp = next_ramp(state.ramp, reader.slot_bytes(), policy.max_group_bytes);
                state.ramp.max(wanted)
            } else {
                state.ramp = 0;
                wanted
            };
            // Drop the replaced window before reserving budget. Without
            // cache budget the verified window only serves this read.
            state.window = None;
            let (size, budget) = match reader.budget.reserve(size) {
                Some(budget) => (size, Some(budget)),
                None if size > wanted => (wanted, reader.budget.reserve(wanted)),
                None => (size, None),
            };
            let request = FetchRequest {
                home: self.home.clone(),
                client: client.clone(),
                access: root_access(grant),
                handle: file.handle.clone(),
                offset: cursor,
                size,
                deadline: Instant::now()
                    .checked_add(reader.timeout)
                    .ok_or_else(|| protocol_error("OwnerFs HIXL deadline overflow"))?,
            };
            let deadline = request.deadline;
            let keep_window = cacheable && budget.is_some();
            let task = handle.spawn(reader.clone().fetch(request, budget));
            let result = handle.block_on(async move {
                tokio::time::timeout(remaining(deadline)?, task)
                    .await
                    .map_err(|_| {
                        afs_error::Error::coded(
                            afs_error::CLIENT_DEADLINE_EXCEEDED,
                            "OwnerFs HIXL window fetch exceeded its deadline",
                        )
                    })?
                    .map_err(|error| {
                        afs_error::Error::coded(afs_error::CLIENT_WORKER_FAILED, error.to_string())
                    })?
            })?;
            match result {
                // Without budget the window serves only this READ.
                Fetched::Window(window) if !keep_window => {
                    fetched = true;
                    let transient = CachedWindow {
                        offset: window.offset,
                        bytes: window.bytes,
                        eof: window.eof,
                        epoch,
                    };
                    let copied = transient.copy_at(cursor, &mut out[served..]);
                    served += copied;
                    cursor += copied as u64;
                    if copied == 0 || transient.eof {
                        break;
                    }
                }
                Fetched::Window(window) => {
                    fetched = true;
                    state.window = Some(CachedWindow {
                        offset: window.offset,
                        bytes: window.bytes,
                        eof: window.eof,
                        epoch,
                    });
                }
                Fetched::Fallback if policy.mode == DataMode::Auto => {
                    return self.fallback(&mut state, offset, served, out.len());
                }
                Fetched::Fallback => return Err(unsupported_read()),
            }
        }
        state.next_offset = offset.saturating_add(served as u64);
        reader.count(if fetched {
            ReadOutcome::Fetch
        } else {
            ReadOutcome::WindowHit
        });
        if let Some(metrics) = &reader.owner_metrics {
            metrics.record_payload("client", "read", "hixl", served as u64);
        }
        if cacheable && policy.max_inflight > 1 {
            self.start_prefetch(&handle, &mut state, grant, file, client, epoch);
        }
        Ok(WindowRead::Done(served))
    }

    fn served_batch(&self, bytes: usize) {
        self.reader.count(ReadOutcome::BatchHit);
        if let Some(metrics) = &self.reader.owner_metrics {
            metrics.record_payload("client", "read", "hixl", bytes as u64);
        }
    }

    /// The whole-file slice that serves a batch-eligible open: the slice it
    /// already claimed, or one claimed from a resident batch. With `wait`, a
    /// miss may wait for a batch of the directory in flight or start one at
    /// this file. `None` leaves the READ to the single-file path; an open this
    /// node invalidated never claims again.
    #[allow(clippy::too_many_arguments)]
    fn batch_slice(
        &self,
        handle: Option<&tokio::runtime::Handle>,
        client: Option<&OwnerFilesClient<Channel>>,
        grant: &RootGrant,
        entry: &OpenEntry,
        route: &BatchRoute,
        state: &mut WindowState,
        epoch: u64,
        wait: bool,
    ) -> afs_error::Result<Option<prost::bytes::Bytes>> {
        if let Some(claimed) = &state.claimed {
            if claimed.epoch == epoch {
                return Ok(Some(claimed.bytes.clone()));
            }
            state.claimed = None;
            return Ok(None);
        }
        if epoch != 0 {
            return Ok(None);
        }
        let mut may_start = wait && handle.is_some() && client.is_some();
        // Start, wait and claim: three rounds at most.
        for _ in 0..3 {
            let claim = self.batches().claim(&entry.identity, route, may_start);
            match claim {
                Claim::Hit(bytes, lookahead) => {
                    state.claimed = Some(ClaimedSlice {
                        bytes: bytes.clone(),
                        epoch,
                    });
                    if let (Some(anchor), Some(handle), Some(client)) = (lookahead, handle, client)
                    {
                        self.start_batch(handle, client, grant, route, anchor, true);
                    }
                    return Ok(Some(bytes));
                }
                Claim::Wait(mut done) if wait => {
                    let Some(handle) = handle else {
                        return Ok(None);
                    };
                    self.reader.count(ReadOutcome::BatchWait);
                    let timeout = self.reader.timeout;
                    handle.block_on(async move {
                        let landed = tokio::time::timeout(timeout, done.wait_for(Option::is_some))
                            .await
                            .map_err(|_| afs_error::Error::coded(
                                afs_error::CLIENT_DEADLINE_EXCEEDED,
                                "OwnerFs HIXL directory batch did not finish before the read deadline",
                            ))?
                            .map_err(|error| afs_error::Error::coded(
                                afs_error::CLIENT_WORKER_FAILED, error.to_string()
                            ))?;
                        landed.as_ref().cloned().unwrap_or(Ok(()))
                    })?;
                }
                Claim::Failed(error) => return Err(error),
                Claim::Start if may_start => {
                    let (Some(handle), Some(client)) = (handle, client) else {
                        return Ok(None);
                    };
                    if !self.start_batch(handle, client, grant, route, route.name.clone(), false) {
                        return Err(afs_error::Error::coded(
                            afs_error::NODE_TRANSFER_RESOURCE_EXHAUSTED,
                            "OwnerFs HIXL directory batch admission is full",
                        ));
                    }
                    self.reader.count(ReadOutcome::BatchFetch);
                    may_start = false;
                }
                _ => return Ok(None),
            }
        }
        Ok(None)
    }

    /// Starts a batch of the open's directory at `anchor` in the background:
    /// the one this open needs, or with `lookahead` the one after a batch
    /// being read. The task owns its budget and wakes every open waiting for
    /// it.
    fn start_batch(
        &self,
        handle: &tokio::runtime::Handle,
        client: &OwnerFilesClient<Channel>,
        grant: &RootGrant,
        route: &BatchRoute,
        anchor: Vec<u8>,
        lookahead: bool,
    ) -> bool {
        let reader = &self.reader;
        let Some(deadline) = Instant::now().checked_add(reader.timeout) else {
            return false;
        };
        let (reserve, cap) = (batch_reservation(reader), batch_cap(reader));
        let start = {
            let mut batches = self.batches();
            if lookahead {
                batches.begin_lookahead(&route.parent, &anchor, reserve, cap, &reader.budget)
            } else if batches.has_pending(&route.parent) {
                // Another open started the batch after this open looked up its claim.
                return true;
            } else {
                batches.begin(&route.parent, &anchor, reserve, cap, &reader.budget)
            }
        };
        let Some(start) = start else {
            return false;
        };
        let request = BatchRequest {
            home: self.home.clone(),
            client: client.clone(),
            access: root_access(grant),
            parent_path: route.parent_path.clone(),
            anchor,
            max_file_bytes: reader.policy.batch_max_file_bytes,
            deadline,
        };
        let fetch = reader.clone().fetch_batch(request);
        let batches = self.batches.clone();
        let parent = route.parent.clone();
        let home = self.home.clone();
        handle.spawn(async move {
            let fetched = match fetch.await {
                Ok(BatchFetched::Batch(batch)) => Ok(Some(batch)),
                Ok(BatchFetched::Unavailable) => Ok(None),
                Err(error) => {
                    afs_logging::warn!("ownerfs.hixl_batch_failed";
                        "home" => home.as_str(),
                        "code" => error.code().to_string(),
                        "error" => error.to_string(),
                    );
                    Err(error)
                }
            };
            batches
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner)
                .finish(&parent, start, fetched);
        });
        true
    }

    fn fallback(
        &self,
        state: &mut WindowState,
        offset: u64,
        served: usize,
        requested: usize,
    ) -> afs_error::Result<WindowRead> {
        state.next_offset = offset.saturating_add(requested as u64);
        self.reader.count(ReadOutcome::Fallback);
        if served > 0
            && let Some(metrics) = &self.reader.owner_metrics
        {
            metrics.record_payload("client", "read", "hixl", served as u64);
        }
        Ok(WindowRead::Fallback { served })
    }

    /// Prefetches the window after the current one as soon as the current one
    /// is installed, so the Home prepare of the next window overlaps the whole
    /// consumption of this one. The task owns its lease and releases its slots
    /// itself.
    fn start_prefetch(
        &self,
        handle: &tokio::runtime::Handle,
        state: &mut WindowState,
        grant: &RootGrant,
        file: &RemoteFile,
        client: &OwnerFilesClient<Channel>,
        epoch: u64,
    ) {
        let Some(window) = &state.window else {
            return;
        };
        if state.pending.is_some() || window.eof || state.next_offset >= window.end() {
            return;
        }
        let offset = window.end();
        let reader = &self.reader;
        let size = next_ramp(
            state.ramp,
            reader.slot_bytes(),
            reader.policy.max_group_bytes,
        );
        let Some(budget) = reader.budget.reserve(size) else {
            return;
        };
        let Some(deadline) = Instant::now().checked_add(reader.timeout) else {
            return;
        };
        state.ramp = size;
        let request = FetchRequest {
            home: self.home.clone(),
            client: client.clone(),
            access: root_access(grant),
            handle: file.handle.clone(),
            offset,
            size,
            deadline,
        };
        let (sender, receiver) = oneshot::channel();
        let fetch = reader.clone().fetch(request, Some(budget));
        handle.spawn(async move {
            let _ = sender.send(fetch.await);
        });
        state.pending = Some(PendingWindow {
            offset,
            epoch,
            receiver,
        });
    }
}

/// The directory of a batch-eligible open, keyed by root so two roots of one
/// Home never share a directory entry.
fn batch_route(
    grant: &RootGrant,
    path: &OsStr,
    attributes: &FileAttributes,
    batch_only: bool,
) -> Option<BatchRoute> {
    let path = path.as_bytes();
    let (parent_path, name) = match path.iter().rposition(|byte| *byte == b'/') {
        Some(index) => (&path[..index], &path[index + 1..]),
        None => (&path[..0], path),
    };
    if name.is_empty() {
        return None;
    }
    Some(BatchRoute {
        parent: [grant.id.0.as_bytes(), b"\0", parent_path].concat(),
        parent_path: parent_path.to_vec(),
        name: name.to_vec(),
        version: FileVersion {
            size: attributes.size,
            mtime_ns: time_ns(attributes.mtime),
            ctime_ns: time_ns(attributes.ctime),
        },
        batch_only,
    })
}

fn time_ns(time: std::time::SystemTime) -> u64 {
    time.duration_since(std::time::UNIX_EPOCH)
        .map(|duration| duration.as_nanos().min(u128::from(u64::MAX)) as u64)
        .unwrap_or(0)
}

/// Doubles a sequential window from one slot up to the configured maximum.
fn next_ramp(previous: usize, slot_bytes: usize, max_window: usize) -> usize {
    let slot = slot_bytes.max(1);
    if previous == 0 {
        slot.min(max_window.max(slot))
    } else {
        previous.saturating_mul(2).min(max_window.max(slot))
    }
}

#[cfg(test)]
#[path = "tests.rs"]
mod tests;
