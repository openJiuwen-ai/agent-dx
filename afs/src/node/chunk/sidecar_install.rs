//! Background installer for verified Chunk outboard sidecars.

use super::*;

const SIDECAR_INSTALL_LIMIT: usize = 64;

const SIDECAR_INSTALL_BATCH: usize = 32;

pub(super) const SIDECAR_STOP_WAIT: Duration = Duration::from_secs(10);

pub(super) const SIDECAR_ID_CAPACITY: usize = 96;

#[derive(Debug)]
pub(super) struct SidecarInstallItem {
    chunk_id: ChunkId,
    proof: SidecarOutboard,
}

#[derive(Debug, Default)]
pub(super) struct SidecarInstallState {
    pub(super) pending: VecDeque<SidecarInstallItem>,
    pub(super) unfinished: HashSet<ChunkId>,
    pub(super) active: usize,
    pub(super) deadline: Option<Instant>,
    control: Option<OutboardLease>,
    pub(super) control_bytes: usize,
    pub(super) directory_capacity: usize,
}

#[derive(Debug, Default)]
pub(super) struct SidecarInstallQueue {
    pub(super) state: Mutex<SidecarInstallState>,
    pub(super) changed: Condvar,
}

#[derive(Debug)]
pub(super) struct SidecarInstaller {
    pub(super) limit: usize,
    pub(super) queue: Arc<SidecarInstallQueue>,
    pub(super) worker: Mutex<Option<std::thread::JoinHandle<()>>>,
    joining: Mutex<()>,
    pub(super) metrics: Option<super::super::metrics::work::ReadWorkMetrics>,
}

#[derive(Clone, Debug)]
struct SidecarInstallContext {
    directory: PathBuf,
    repairs: Arc<OutboardRepairGates>,
    budget: Arc<OutboardBudget>,
    pub(super) metrics: Option<super::super::metrics::work::ReadWorkMetrics>,
    #[cfg(test)]
    test: Arc<SidecarInstallTestControl>,
}

// Fields are deliberately dropped before lease: optional metadata failure must
// free the actual backing capacities before refunding the shared ledger.
struct SidecarControlBuffers {
    pub(super) pending: VecDeque<SidecarInstallItem>,
    pub(super) unfinished: HashSet<ChunkId>,
    lease: OutboardLease,
    pub(super) bytes: usize,
}

pub(super) fn sidecar_control_bytes(
    queue_capacity: usize,
    hash_capacity: usize,
    limit: usize,
    path_capacity: usize,
) -> Option<usize> {
    // std HashSet reports usable entries, not buckets. Rounding up bounds its
    // backing key/control table; 4KiB covers fixed queue/context/Arc metadata.
    let buckets = hash_capacity.max(1).checked_next_power_of_two()?;
    queue_capacity
        .checked_mul(std::mem::size_of::<SidecarInstallItem>())?
        .checked_add(buckets.checked_mul(std::mem::size_of::<ChunkId>() + 1)?)?
        .checked_add(limit.checked_mul(2 * SIDECAR_ID_CAPACITY)?)?
        .checked_add(sidecar_path_bytes(path_capacity)?)?
        .checked_add(4096)
}

pub(super) fn sidecar_path_bytes(directory_capacity: usize) -> Option<usize> {
    // Include the retained worker directory and the temporary formal/tmp
    // joins and ID formatting used during optional installation. Joins may
    // grow their backing buffers; this is deliberately conservative.
    directory_capacity
        .checked_add(SIDECAR_ID_CAPACITY)?
        .checked_add(128)?
        .checked_mul(8)
}

fn prepare_sidecar_control(store: &LocalChunkStore, limit: usize) -> Option<SidecarControlBuffers> {
    // HashSet's smallest allocation can hold three entries even for limit=1.
    // A minimum of four buckets and two per allowed entry bounds that case.
    let hash_bound = limit.checked_mul(2)?.max(4).checked_next_power_of_two()?;
    let bytes = sidecar_control_bytes(limit, hash_bound, limit, store.outboards.as_os_str().len())?;
    let lease = store.outboard_budget.try_reserve_sidecar_control(bytes)?;
    let mut pending = VecDeque::new();
    pending.try_reserve_exact(limit).ok()?;
    let mut unfinished = HashSet::new();
    unfinished.try_reserve(limit).ok()?;
    let observed = sidecar_control_bytes(
        pending.capacity(),
        unfinished.capacity(),
        limit,
        store.outboards.as_os_str().len(),
    )?;
    if observed > bytes {
        // Never keep a container whose real retained capacity exceeds its
        // conservative ticket; failed optional admission does not affect ACK.
        return None;
    }
    Some(SidecarControlBuffers {
        pending,
        unfinished,
        lease,
        bytes,
    })
}

fn sidecar_chunk_id(id: &ChunkId) -> Option<ChunkId> {
    if id.0.len() > SIDECAR_ID_CAPACITY {
        return None;
    }
    let mut copied = String::new();
    copied.try_reserve_exact(id.0.len()).ok()?;
    copied.push_str(&id.0);
    (copied.capacity() <= SIDECAR_ID_CAPACITY).then(|| ChunkId::new(copied))
}

impl Default for SidecarInstaller {
    fn default() -> Self {
        Self {
            limit: SIDECAR_INSTALL_LIMIT,
            queue: Arc::new(SidecarInstallQueue::default()),
            worker: Mutex::new(None),
            joining: Mutex::new(()),
            metrics: None,
        }
    }
}

impl SidecarInstaller {
    pub(super) fn observe(&self, state: &SidecarInstallState) {
        if let Some(metrics) = &self.metrics {
            metrics.sidecar_install_counts(
                state.pending.len(),
                state.active,
                self.limit,
                state.control_bytes,
            );
        }
    }

    pub(super) fn enqueue_batch(&self, store: &LocalChunkStore, staged: &[StagedChunk]) {
        if !staged.iter().any(|item| item.outboard.is_some()) {
            return;
        }
        let Ok(mut worker) = self.worker.lock() else {
            store
                .outboard_budget
                .sidecar_event("sidecar_install_dropped");
            return;
        };
        let Ok(mut state) = self.queue.state.lock() else {
            store
                .outboard_budget
                .sidecar_event("sidecar_install_dropped");
            return;
        };
        if state.deadline.is_some() {
            for item in staged {
                if item.outboard.is_some() {
                    store
                        .outboard_budget
                        .sidecar_event("sidecar_install_dropped");
                }
            }
            return;
        }
        if state.control.is_none() {
            drop(state);
            let Some(buffers) = prepare_sidecar_control(store, self.limit) else {
                store
                    .outboard_budget
                    .sidecar_event("sidecar_install_dropped");
                return;
            };
            state = self
                .queue
                .state
                .lock()
                .unwrap_or_else(|error| error.into_inner());
            if state.deadline.is_some() {
                drop(state);
                drop(buffers);
                store
                    .outboard_budget
                    .sidecar_event("sidecar_install_dropped");
                return;
            }
            let SidecarControlBuffers {
                pending,
                unfinished,
                lease,
                bytes,
            } = buffers;
            state.pending = pending;
            state.unfinished = unfinished;
            state.control = Some(lease);
            state.control_bytes = bytes;
            self.observe(&state);
        }
        if worker.is_none() && state.deadline.is_none() {
            #[cfg(test)]
            if store
                .test_hooks
                .sidecar_install_test
                .fail_worker_start
                .swap(false, Ordering::Relaxed)
            {
                store
                    .outboard_budget
                    .sidecar_event("sidecar_install_failed");
                store
                    .outboard_budget
                    .sidecar_event("sidecar_install_dropped");
                return;
            }
            let mut directory = PathBuf::new();
            if directory
                .try_reserve_exact(store.outboards.as_os_str().len())
                .is_err()
            {
                store
                    .outboard_budget
                    .sidecar_event("sidecar_install_dropped");
                return;
            }
            directory.push(&store.outboards);
            if sidecar_control_bytes(
                state.pending.capacity(),
                state.unfinished.capacity(),
                self.limit,
                directory.capacity(),
            )
            .is_none_or(|observed| observed > state.control_bytes)
            {
                store
                    .outboard_budget
                    .sidecar_event("sidecar_install_dropped");
                return;
            }
            state.directory_capacity = directory.capacity();
            let context = SidecarInstallContext {
                directory,
                repairs: store.outboard_repairs.clone(),
                budget: store.outboard_budget.clone(),
                metrics: store.read_work.clone(),
                #[cfg(test)]
                test: store.test_hooks.sidecar_install_test.clone(),
            };
            let queue = self.queue.clone();
            let limit = self.limit;
            match std::thread::Builder::new()
                .name("afs-sidecar-install".into())
                .spawn(move || {
                    run_sidecar_installer(queue, context, limit);
                }) {
                Ok(handle) => *worker = Some(handle),
                Err(_) => {
                    store
                        .outboard_budget
                        .sidecar_event("sidecar_install_failed");
                    store
                        .outboard_budget
                        .sidecar_event("sidecar_install_dropped");
                    return;
                }
            }
        }
        for item in staged {
            let Some(proof) = &item.outboard else {
                continue;
            };
            if state.unfinished.contains(&item.chunk.id) {
                continue;
            }
            if state.deadline.is_some()
                || state.unfinished.len() >= self.limit
                || ChunkObject::from_content_addressed_id(&item.chunk.id).as_ref()
                    != Some(&item.chunk)
                || ChunkOutboard::validate_encoded_shape(&item.chunk, proof.encoded()).is_err()
            {
                store
                    .outboard_budget
                    .sidecar_event("sidecar_install_dropped");
                continue;
            }
            let Some(proof) = store.outboard_budget.pin_sidecar(proof) else {
                store
                    .outboard_budget
                    .sidecar_event("sidecar_install_dropped");
                continue;
            };
            let Some(key) = sidecar_chunk_id(&item.chunk.id) else {
                drop(state);
                drop(proof);
                self.queue.changed.notify_all();
                store
                    .outboard_budget
                    .sidecar_event("sidecar_install_dropped");
                return;
            };
            let Some(queued_id) = sidecar_chunk_id(&item.chunk.id) else {
                drop(state);
                drop(proof);
                self.queue.changed.notify_all();
                store
                    .outboard_budget
                    .sidecar_event("sidecar_install_dropped");
                return;
            };
            state.unfinished.insert(key);
            state.pending.push_back(SidecarInstallItem {
                chunk_id: queued_id,
                proof,
            });
        }
        self.observe(&state);
        self.queue.changed.notify_all();
    }

    pub(super) fn close(&self, deadline: Instant) {
        let mut state = self
            .queue
            .state
            .lock()
            .unwrap_or_else(|error| error.into_inner());
        // Repeated stop requests can shorten, never renew, the first deadline.
        state.deadline = Some(state.deadline.map_or(deadline, |old| old.min(deadline)));
        self.queue.changed.notify_all();
    }

    pub(super) fn shutdown(&self) -> Result<()> {
        self.close(Instant::now() + SIDECAR_STOP_WAIT);
        let _joining = self
            .joining
            .lock()
            .unwrap_or_else(|error| error.into_inner());
        let handle = self
            .worker
            .lock()
            .unwrap_or_else(|error| error.into_inner())
            .take();
        let result = if let Some(handle) = handle {
            // A synchronous filesystem operation cannot safely be cancelled.
            // Keep owning it and its proof; the existing process watchdog is
            // the final bound, never detach or report a clean incomplete stop.
            handle
                .join()
                .map_err(|_| invalid("sidecar installer worker failed"))
        } else {
            Ok(())
        };
        let (pending, unfinished, control) = {
            let mut state = self
                .queue
                .state
                .lock()
                .unwrap_or_else(|error| error.into_inner());
            state.active = 0;
            (
                std::mem::take(&mut state.pending),
                std::mem::take(&mut state.unfinished),
                state.control.take(),
            )
        };
        // The worker's owned directory has already been freed by join. Free
        // every retained container/string before refunding its metadata lease.
        drop(pending);
        drop(unfinished);
        drop(control);
        let mut state = self
            .queue
            .state
            .lock()
            .unwrap_or_else(|error| error.into_inner());
        state.control_bytes = 0;
        state.directory_capacity = 0;
        self.observe(&state);
        result
    }

    #[cfg(test)]
    pub(super) fn wait_idle(&self) {
        let deadline = Instant::now() + Duration::from_secs(5);
        let mut state = self.queue.state.lock().unwrap();
        while !state.unfinished.is_empty() {
            let remaining = deadline
                .checked_duration_since(Instant::now())
                .expect("sidecar installer did not become idle");
            state = self.queue.changed.wait_timeout(state, remaining).unwrap().0;
        }
    }
}

impl Drop for SidecarInstaller {
    fn drop(&mut self) {
        self.close(Instant::now());
        let _ = self.shutdown();
    }
}

fn run_sidecar_installer(
    queue: Arc<SidecarInstallQueue>,
    context: SidecarInstallContext,
    limit: usize,
) {
    loop {
        let mut state = queue
            .state
            .lock()
            .unwrap_or_else(|error| error.into_inner());
        while state.pending.is_empty() && state.deadline.is_none() {
            state = queue
                .changed
                .wait(state)
                .unwrap_or_else(|error| error.into_inner());
        }
        if state.pending.is_empty() {
            return;
        }
        let count = state.pending.len().min(SIDECAR_INSTALL_BATCH);
        let mut batch: [Option<SidecarInstallItem>; SIDECAR_INSTALL_BATCH] =
            std::array::from_fn(|_| None);
        for slot in batch.iter_mut().take(count) {
            *slot = state.pending.pop_front();
        }
        state.active += count;
        if let Some(metrics) = &context.metrics {
            metrics.sidecar_install_counts(
                state.pending.len(),
                state.active,
                limit,
                state.control_bytes,
            );
        }
        drop(state);
        let mut installed_items = 0usize;
        for item in batch.iter().flatten() {
            let expired = queue
                .state
                .lock()
                .unwrap_or_else(|error| error.into_inner())
                .deadline
                .is_some_and(|deadline| Instant::now() >= deadline);
            if expired {
                context.budget.sidecar_event("sidecar_install_dropped");
                continue;
            }
            #[cfg(test)]
            context.test.pause_if_requested();
            let path = context
                .directory
                .join(format!("{}.bao", safe_id(&item.chunk_id.0)));
            let _diagnostic = context.metrics.as_ref().and_then(|metrics| {
                metrics.coarse_diagnostic(
                    "persist_proof_install_sync",
                    item.proof.proof().encoded().len() as u64,
                )
            });
            let mut hash = std::collections::hash_map::DefaultHasher::new();
            path.hash(&mut hash);
            let index = (hash.finish() % OUTBOARD_REPAIR_SHARDS as u64) as usize;
            let installed = context.repairs[index]
                .lock()
                .map_err(|_| invalid("sidecar repair gate poisoned"))
                .and_then(|_repair| {
                    if !outboard_file_matches(&path, item.proof.proof()).unwrap_or(false) {
                        install_outboard_file(&path, item.proof.proof())?;
                    }
                    Ok(())
                });
            if installed.is_ok() {
                installed_items += 1;
            } else {
                context.budget.sidecar_event("sidecar_install_failed");
            }
        }
        #[cfg(test)]
        if installed_items != 0 {
            context.test.pause_before_directory_sync();
        }
        let directory_expired = queue
            .state
            .lock()
            .unwrap_or_else(|error| error.into_inner())
            .deadline
            .is_some_and(|deadline| Instant::now() >= deadline);
        if installed_items != 0 && directory_expired {
            // Files already installed remain optional and may be repaired on
            // a later read. Do not begin another filesystem barrier after the
            // absolute stop decision; active references retire only below.
            for _ in 0..installed_items {
                context.budget.sidecar_event("sidecar_install_dropped");
            }
        } else if installed_items != 0 {
            let _diagnostic = context
                .metrics
                .as_ref()
                .and_then(|metrics| metrics.coarse_diagnostic("persist_proof_dir_sync", 0));
            #[cfg(test)]
            let fail_directory = context
                .test
                .fail_directory_sync
                .swap(false, Ordering::Relaxed);
            #[cfg(not(test))]
            let fail_directory = false;
            if fail_directory || sync_outboard_directory(&context.directory).is_err() {
                context.budget.sidecar_event("sidecar_install_failed");
            }
        }
        #[cfg(test)]
        {
            let io = OUTBOARD_PERSIST_IO.with(|counts| std::mem::take(&mut *counts.borrow_mut()));
            let mut total = context.test.optional_io.lock().unwrap();
            total.writes += io.writes;
            total.file_syncs += io.file_syncs;
            total.renames += io.renames;
            total.directory_syncs += io.directory_syncs;
        }
        for item in batch.into_iter().flatten() {
            let SidecarInstallItem { chunk_id, proof } = item;
            // Never drop a charged proof while holding the queue mutex.
            drop(proof);
            let mut state = queue
                .state
                .lock()
                .unwrap_or_else(|error| error.into_inner());
            state.unfinished.remove(&chunk_id);
            state.active -= 1;
            if let Some(metrics) = &context.metrics {
                metrics.sidecar_install_counts(
                    state.pending.len(),
                    state.active,
                    limit,
                    state.control_bytes,
                );
            }
            queue.changed.notify_all();
        }
    }
}

#[cfg(test)]
#[derive(Debug, Default)]
pub(super) struct SidecarInstallTestControl {
    pub(super) block: Mutex<Option<(std::sync::mpsc::Sender<()>, std::sync::mpsc::Receiver<()>)>>,
    pub(super) before_directory_sync:
        Mutex<Option<(std::sync::mpsc::Sender<()>, std::sync::mpsc::Receiver<()>)>>,
    pub(super) optional_io: Mutex<OutboardPersistIo>,
    pub(super) fail_directory_sync: std::sync::atomic::AtomicBool,
    pub(super) fail_worker_start: std::sync::atomic::AtomicBool,
}

#[cfg(test)]
impl SidecarInstallTestControl {
    fn pause_if_requested(&self) {
        let block = self.block.lock().unwrap().take();
        if let Some((entered, release)) = block {
            let _ = entered.send(());
            let _ = release.recv();
        }
    }
    fn pause_before_directory_sync(&self) {
        let block = self.before_directory_sync.lock().unwrap().take();
        if let Some((entered, release)) = block {
            let _ = entered.send(());
            let _ = release.recv();
        }
    }
}

impl LocalChunkStore {
    #[cfg(any(feature = "dfs", test))]
    pub(crate) fn with_sidecar_install_queue(mut self, limit: usize) -> Result<Self> {
        if !(1..=SIDECAR_INSTALL_LIMIT).contains(&limit) {
            return Err(invalid("sidecar install queue must be 1..=64"));
        }
        self.sidecar_installer.limit = limit;
        {
            let state = self
                .sidecar_installer
                .queue
                .state
                .lock()
                .unwrap_or_else(|error| error.into_inner());
            self.sidecar_installer.observe(&state);
        }
        Ok(self)
    }

    #[cfg(feature = "dfs")]
    pub(crate) fn stop_sidecar_installation(&self) {
        self.sidecar_installer
            .close(Instant::now() + SIDECAR_STOP_WAIT);
    }

    #[cfg(feature = "dfs")]
    pub(crate) fn shutdown_sidecar_installer(&self) -> Result<()> {
        self.sidecar_installer.shutdown()
    }
}
