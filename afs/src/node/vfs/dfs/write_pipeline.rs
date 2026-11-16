use super::*;

const PIPELINE_METADATA_BYTES: usize = 4 * 1024 * 1024;
const PIPELINE_CONTROL_BYTES: usize = 64 * 1024;
pub(super) const PIPELINE_DESCRIPTOR_BYTES: usize =
    PIPELINE_METADATA_BYTES - PIPELINE_CONTROL_BYTES;

#[derive(Clone, Copy)]
pub(crate) struct WritePipelineConfig {
    pub unpublished_descriptors: usize,
    pub upload_inflight_chunks: usize,
}

impl Default for WritePipelineConfig {
    fn default() -> Self {
        Self {
            unpublished_descriptors: 1024,
            upload_inflight_chunks: 8,
        }
    }
}

#[derive(Default)]
pub(super) struct PipelineMetadataState {
    control_bytes: usize,
    control_peak: usize,
    pub(super) descriptor_bytes: usize,
    pub(super) descriptor_reserved_bytes: usize,
    pub(super) scratch_reserved_bytes: usize,
    pub(super) descriptor_peak: usize,
    pub(super) descriptors: usize,
    pub(super) descriptor_peak_count: usize,
    pub(super) inflight: usize,
    pub(super) inflight_peak: usize,
    pub(super) completed: u64,
    pub(super) failed: u64,
    pub(super) pressure_descriptor: u64,
    pub(super) pressure_live: u64,
    /// Early commits requested because a write waited on a stalled budget (R91).
    pub(super) pressure_budget: u64,
}

pub(super) struct PipelineMetadataBudget {
    pub(super) state: Mutex<PipelineMetadataState>,
    descriptor_limit: usize,
    pub(super) upload_limit: usize,
    notify: Arc<tokio::sync::Notify>,
}

pub(super) struct PipelineMetadataLease {
    pub(super) budget: Arc<PipelineMetadataBudget>,
    pub(super) bytes: usize,
    descriptor: bool,
    counted_descriptor: bool,
    pub(super) actual_bytes: usize,
}

impl PipelineMetadataBudget {
    pub(super) fn reserve(
        self: &Arc<Self>,
        bytes: usize,
        descriptor: bool,
    ) -> Result<Option<PipelineMetadataLease>> {
        let mut state = self
            .state
            .lock()
            .map_err(|_| unavailable("native metadata budget is poisoned"))?;
        if descriptor {
            if state.descriptors >= self.descriptor_limit
                || bytes > PIPELINE_DESCRIPTOR_BYTES.saturating_sub(state.descriptor_reserved_bytes)
            {
                return Ok(None);
            }
            state.descriptors += 1;
            state.descriptor_reserved_bytes += bytes;
            state.descriptor_peak_count = state.descriptor_peak_count.max(state.descriptors);
        } else {
            if bytes > PIPELINE_CONTROL_BYTES.saturating_sub(state.control_bytes) {
                return Ok(None);
            }
            state.control_bytes += bytes;
            state.control_peak = state.control_peak.max(state.control_bytes);
        }
        Ok(Some(PipelineMetadataLease {
            budget: self.clone(),
            bytes,
            descriptor,
            counted_descriptor: descriptor,
            actual_bytes: 0,
        }))
    }
}

impl PipelineMetadataLease {
    pub(super) fn set_actual(&mut self, actual: usize) -> Result<()> {
        let budget = self.budget.clone();
        let mut state = budget
            .state
            .lock()
            .map_err(|_| unavailable("native metadata budget is poisoned"))?;
        self.set_actual_locked(actual, &mut state)
    }

    // Caller has already separated a structural lock failure from an
    // owner-scoped capacity refusal. The arithmetic and limits stay shared.
    pub(super) fn set_actual_locked(
        &mut self,
        actual: usize,
        state: &mut PipelineMetadataState,
    ) -> Result<()> {
        if self.descriptor {
            let reserved = self.bytes.max(actual.saturating_mul(4));
            if reserved - self.bytes
                > PIPELINE_DESCRIPTOR_BYTES.saturating_sub(state.descriptor_reserved_bytes)
            {
                return Err(unavailable(
                    "native metadata actual capacity exceeds its reservation",
                ));
            }
            let next_reserved = state.descriptor_reserved_bytes + reserved - self.bytes;
            let next_actual = state.descriptor_bytes - self.actual_bytes + actual;
            if next_actual.saturating_add(state.scratch_reserved_bytes) > next_reserved {
                return Err(unavailable(
                    "native receipt cannot consume reserved publication scratch",
                ));
            }
            state.descriptor_reserved_bytes = next_reserved;
            state.descriptor_bytes = next_actual;
            state.descriptor_peak = state.descriptor_peak.max(state.descriptor_bytes);
            self.bytes = reserved;
            self.actual_bytes = actual;
        }
        Ok(())
    }
}

pub(super) fn receipt_heap(receipt: &ChunkReceipt) -> usize {
    receipt.operation_id.0.capacity()
        + receipt.chunk.id.0.capacity()
        + receipt.replica_group_id.0.capacity()
        + receipt.durable_acks.capacity() * std::mem::size_of::<crate::dfs::ReplicaAck>()
        + receipt
            .durable_acks
            .iter()
            .map(|ack| {
                ack.operation_id.0.capacity()
                    + ack.chunk_id.0.capacity()
                    + ack.node_id.capacity()
                    + ack.device_id.capacity()
            })
            .sum::<usize>()
        + receipt.external_commit.as_ref().map_or(0, |external| {
            external.store_id.capacity()
                + external.generation.capacity()
                + external.object_key.capacity()
        })
}

impl Drop for PipelineMetadataLease {
    fn drop(&mut self) {
        if let Ok(mut state) = self.budget.state.lock() {
            if self.descriptor {
                state.descriptor_reserved_bytes =
                    state.descriptor_reserved_bytes.saturating_sub(self.bytes);
                state.descriptor_bytes = state.descriptor_bytes.saturating_sub(self.actual_bytes);
                if self.counted_descriptor {
                    state.descriptors = state.descriptors.saturating_sub(1);
                }
            } else {
                state.control_bytes = state.control_bytes.saturating_sub(self.bytes);
            }
        }
        self.budget.notify.notify_one();
    }
}

#[derive(Default)]
pub(super) struct MutationTicketQueue {
    pub(super) values: VecDeque<u64>,
    allocation: Option<PipelineMetadataLease>,
}

impl MutationTicketQueue {
    pub(super) fn push(&mut self, id: u64, budget: &Arc<PipelineMetadataBudget>) -> Result<()> {
        if self.values.len() == self.values.capacity() {
            let target = self.values.capacity().saturating_mul(2).max(4);
            let bytes = target
                .checked_mul(std::mem::size_of::<u64>())
                .ok_or_else(file_too_large)?;
            // Charge the replacement while the old capacity is still owned.
            let Some(mut allocation) = budget.reserve(bytes, false)? else {
                return Err(unavailable("native admission control capacity is full"));
            };
            let mut replacement = VecDeque::new();
            replacement.try_reserve_exact(target).map_err(|_| {
                Error::coded(
                    afs_error::IO_OUT_OF_MEMORY,
                    "native admission queue allocation refused",
                )
            })?;
            let actual = replacement.capacity() * std::mem::size_of::<u64>();
            if actual > allocation.bytes {
                let Some(mut extra) = budget.reserve(actual - allocation.bytes, false)? else {
                    return Err(unavailable(
                        "native admission queue actual capacity exceeds its control budget",
                    ));
                };
                // Transfer the two reservations into one without refunding the
                // extra capacity while the replacement remains live.
                allocation.bytes += extra.bytes;
                extra.bytes = 0;
            }
            replacement.extend(self.values.drain(..));
            self.values = replacement;
            self.allocation = Some(allocation);
        }
        self.values.push_back(id);
        Ok(())
    }

    pub(super) fn remove(&mut self, id: u64) {
        if let Some(index) = self.values.iter().position(|value| *value == id) {
            self.values.remove(index);
        }
        if self.values.is_empty() {
            self.values = VecDeque::new();
            self.allocation = None;
        }
    }
}

/// Native write resources: the upload pipeline (installed by the Node
/// assembly, absent in plain unit fixtures) and the staging buffer pool.
pub(super) struct WriteResources {
    pub(super) pipeline: Option<Arc<WritePipeline>>,
    pub(super) buffer_pool: Arc<WriteBufferPool>,
}

impl Default for WriteResources {
    fn default() -> Self {
        Self {
            // 原生存储的写流水线由装配方用 `with_write_pipeline` 装上（需要 Node runtime）；
            // 构造时不装，避免出现只在测试里存在的"无 runtime 流水线"。
            pipeline: None,
            buffer_pool: WriteBufferPool::new(128 * 1024 * 1024),
        }
    }
}

pub(super) struct WritePipeline {
    pub(super) budget: Arc<DirtyBudget>,
    pub(super) metadata: Arc<PipelineMetadataBudget>,
    pub(super) runtime: tokio::runtime::Handle,
    pub(super) notify: Arc<tokio::sync::Notify>,
    pub(super) metrics: Option<crate::node::metrics::work::ReadWorkMetrics>,
    pub(super) closed: AtomicBool,
    pub(super) next_ticket: AtomicU64,
    pub(super) allocation_queue: Mutex<MutationTicketQueue>,
    pub(super) tasks: Mutex<Vec<Arc<PipelineUpload>>>,
    _task_capacity: Option<PipelineMetadataLease>,
}

impl WritePipeline {
    pub(super) fn new(
        budget: Arc<DirtyBudget>,
        config: WritePipelineConfig,
        runtime: tokio::runtime::Handle,
        metrics: Option<crate::node::metrics::work::ReadWorkMetrics>,
    ) -> Self {
        let notify = Arc::new(tokio::sync::Notify::new());
        let metadata = Arc::new(PipelineMetadataBudget {
            state: Mutex::new(PipelineMetadataState::default()),
            descriptor_limit: config.unpublished_descriptors,
            upload_limit: config.upload_inflight_chunks,
            notify: notify.clone(),
        });
        let tasks = Vec::with_capacity(config.upload_inflight_chunks);
        let task_capacity = metadata
            .reserve(
                tasks.capacity() * std::mem::size_of::<Arc<PipelineUpload>>(),
                false,
            )
            .ok()
            .flatten();
        Self {
            budget,
            metadata,
            runtime,
            notify,
            metrics,
            closed: AtomicBool::new(false),
            next_ticket: AtomicU64::new(1),
            allocation_queue: Mutex::new(MutationTicketQueue::default()),
            tasks: Mutex::new(tasks),
            _task_capacity: task_capacity,
        }
    }

    pub(super) fn observe(&self) {
        let Some(metrics) = &self.metrics else {
            return;
        };
        let live = self.budget.snapshot();
        metrics.native_write_live(&live);
        if let Ok(state) = self.metadata.state.lock() {
            metrics.native_write_resources(&[
                (
                    "descriptor_reserved_bytes",
                    state.descriptor_reserved_bytes as u64,
                ),
                (
                    "publication_scratch_reserved_bytes",
                    state.scratch_reserved_bytes as u64,
                ),
                ("descriptor_bytes", state.descriptor_bytes as u64),
                ("descriptor_peak_bytes", state.descriptor_peak as u64),
                ("descriptors", state.descriptors as u64),
                ("descriptor_peak_count", state.descriptor_peak_count as u64),
                ("control_bytes", state.control_bytes as u64),
                ("control_peak_bytes", state.control_peak as u64),
                ("control_reserve_bytes", PIPELINE_CONTROL_BYTES as u64),
                ("metadata_limit_bytes", PIPELINE_METADATA_BYTES as u64),
                ("descriptor_limit_bytes", PIPELINE_DESCRIPTOR_BYTES as u64),
                ("upload_inflight", state.inflight as u64),
                ("upload_peak", state.inflight_peak as u64),
                ("upload_limit", self.metadata.upload_limit as u64),
                ("pressure_descriptor", state.pressure_descriptor),
                ("pressure_live", state.pressure_live),
                ("pressure_budget", state.pressure_budget),
                ("completed_uploads", state.completed),
                ("failed_uploads", state.failed),
            ]);
        }
    }
}

pub(super) struct MutationTicket {
    pub(super) id: u64,
    pub(super) cell: SharedInodeWriteState,
    pub(super) pipeline: Arc<WritePipeline>,
}

impl Drop for MutationTicket {
    fn drop(&mut self) {
        if let Ok(mut state) = self.cell.lock() {
            if state.native_queue.values.front() == Some(&self.id) {
                state.native_thread = None;
            }
            state.native_queue.remove(self.id);
        }
        self.cell.notify_all();
        self.pipeline.notify.notify_one();
    }
}

pub(super) struct AllocationTurn {
    pub(super) id: u64,
    pub(super) pipeline: Arc<WritePipeline>,
}

impl Drop for AllocationTurn {
    fn drop(&mut self) {
        if let Ok(mut queue) = self.pipeline.allocation_queue.lock() {
            queue.remove(self.id);
        }
        self.pipeline.notify.notify_one();
    }
}

pub(super) struct CommitAccounting {
    pub(super) budget: Arc<PipelineMetadataBudget>,
    pub(super) reserved: usize,
    pub(super) actual: AtomicUsize,
    pub(super) _owners: Vec<Arc<Mutex<PipelineMetadataLease>>>,
}

impl CommitAccounting {
    pub(super) fn set_actual(&self, bytes: usize) -> Result<()> {
        if bytes > self.reserved {
            return Err(unavailable(
                "native commit metadata exceeds its publication scratch",
            ));
        }
        let mut state = self
            .budget
            .state
            .lock()
            .map_err(|_| unavailable("native metadata budget is poisoned"))?;
        let previous = self.actual.swap(bytes, Ordering::Relaxed);
        state.descriptor_bytes = state
            .descriptor_bytes
            .saturating_sub(previous)
            .saturating_add(bytes);
        state.descriptor_peak = state.descriptor_peak.max(state.descriptor_bytes);
        Ok(())
    }
}

impl Drop for CommitAccounting {
    fn drop(&mut self) {
        if let Ok(mut state) = self.budget.state.lock() {
            state.scratch_reserved_bytes =
                state.scratch_reserved_bytes.saturating_sub(self.reserved);
            state.descriptor_bytes = state
                .descriptor_bytes
                .saturating_sub(self.actual.load(Ordering::Relaxed));
        }
        self.budget.notify.notify_one();
    }
}

pub(super) struct CommitRpcAccounting {
    pub(super) accounting: Arc<CommitAccounting>,
    pub(super) base: usize,
}
impl Drop for CommitRpcAccounting {
    fn drop(&mut self) {
        let _ = self.accounting.set_actual(self.base);
    }
}

// P1M already retains complete base/layout/version and owned Meta RPC values.
// Do not impose a new file-size limit on those existing layers. This ledger
// covers W2's additional retained receipts and incremental publication owners;
// full-layout/domain/wire capacity remains a separately reported RSS cost.
pub(super) fn commit_heap(commit: &CommitFileVersion) -> usize {
    std::mem::size_of::<CommitFileVersion>()
        + commit.operation_id.0.capacity()
        + commit.inode_id.0.capacity()
        + commit
            .expected_head_version
            .as_ref()
            .map_or(0, |id| id.0.capacity())
        + commit.write_lease.inode_id.0.capacity()
        + commit.write_lease.owner_node_id.capacity()
        + commit.write_lease.owner_session_id.capacity()
        + commit.chunk_receipts.capacity() * std::mem::size_of::<ChunkReceipt>()
        + commit
            .chunk_receipts
            .iter()
            .map(receipt_heap)
            .sum::<usize>()
}

pub(super) struct UploadPermit(pub(super) Arc<PipelineMetadataBudget>);
impl Drop for UploadPermit {
    fn drop(&mut self) {
        if let Ok(mut state) = self.0.state.lock() {
            state.inflight = state.inflight.saturating_sub(1);
        }
        self.0.notify.notify_one();
    }
}

pub(super) struct PipelineUpload {
    pub(super) _gc_pin: Option<Arc<crate::node::gc::GcPin>>,
    pub(super) operation_id: OperationId,
    pub(super) chunk: ChunkObject,
    pub(super) owner: Weak<InodeWriteStateCell>,
    pub(super) state: Mutex<PipelineUploadState>,
    pub(super) cv: Condvar,
    pub(super) _descriptor: Arc<Mutex<PipelineMetadataLease>>,
}

pub(super) struct PipelineUploadState {
    pub(super) body: Option<SharedChunkBytes>,
    pub(super) staged: Option<StagedChunk>,
    /// The upload worker. It leaves its result in `finished` and wakes `cv`,
    /// so a waiting barrier harvests it without a background pass.
    pub(super) task: Option<tokio::task::JoinHandle<()>>,
    pub(super) finished: Option<Result<StoredChunk>>,
    pub(super) receipt: Option<StoredChunk>,
    pub(super) error: Option<Error>,
    pub(super) terminal_error: Option<Error>,
    pub(super) joining: bool,
}

impl PipelineUpload {
    pub(super) fn read_at(
        &self,
        store: &dyn ChunkSink,
        offset: u64,
        out: &mut [u8],
    ) -> Result<usize> {
        let state = self
            .state
            .lock()
            .map_err(|_| unavailable("native upload state is poisoned"))?;
        if let Some(error) = &state.terminal_error {
            return Err(error.clone());
        }
        if let Some(body) = &state.body {
            let start = usize::try_from(offset)
                .map_err(|_| file_too_large())?
                .min(body.len());
            let size = out.len().min(body.len() - start);
            out[..size].copy_from_slice(&body[start..start + size]);
            return Ok(size);
        }
        let receipt = state
            .receipt
            .as_ref()
            .ok_or_else(|| unavailable("native uploaded body has no durable receipt"))?;
        if receipt.body_release != BodyRelease::ReleaseAfterUpload {
            return Err(unavailable(
                "native nonlocal body was released before publication",
            ));
        }
        store.read_uploaded_at(&receipt.receipt, offset, out)
    }
}

pub(super) struct InodeMutationGuard<'a> {
    pub(super) state: InodeWriteStateGuard<'a>,
    pub(super) _ticket: Option<MutationTicket>,
}
impl std::ops::Deref for InodeMutationGuard<'_> {
    type Target = InodeWriteState;
    fn deref(&self) -> &Self::Target {
        &self.state
    }
}
impl std::ops::DerefMut for InodeMutationGuard<'_> {
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut self.state
    }
}
