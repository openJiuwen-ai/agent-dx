//! Bounded work and root verification for streaming DFS peer reads.

use super::*;

#[cfg(feature = "dfs")]
pub(super) const STREAM_MESSAGE_BYTES: usize = 128 * 1024;

#[cfg(feature = "dfs")]
pub(super) const STREAM_DATA_BYTES: usize = 64 * 1024;

#[cfg(feature = "dfs")]
pub(super) const STREAM_FRAME_BACKING_BYTES: usize = 256 * 1024;

#[cfg(feature = "dfs")]
pub(super) const STREAM_KEY_BYTES: usize = 8 * 1024;

#[cfg(feature = "dfs")]
pub(super) const STREAM_REQUEST_METADATA_BYTES: usize = 32 * 1024;

#[cfg(feature = "dfs")]
pub(super) struct PeerStreamBlock {
    // Drop the actual allocation before retiring its accounting lease.
    pub(super) bytes: Vec<u8>,
    pub(super) offset: usize,
    // Two small accounting owners are allocated once per shared session,
    // then move with the same blocks; handing off a block allocates nothing.
    pub(super) _work: Option<Box<crate::node::metrics::work::PeerStreamFrameWork>>,
}

#[cfg(feature = "dfs")]
impl PeerStreamBlock {
    pub(super) fn new(metrics: Option<&ReadWorkMetrics>) -> afs_error::Result<Self> {
        let bytes = Vec::with_capacity(STREAM_FRAME_BACKING_BYTES);
        if bytes.capacity() > STREAM_FRAME_BACKING_BYTES {
            return Err(dfs_protocol_error(
                "Peer stream block allocation exceeds budget",
            ));
        }
        let work = metrics.map(|metrics| {
            metrics.peer_read_event("stream_block_alloc");
            Box::new(metrics.peer_stream_frame(bytes.capacity()))
        });
        Ok(Self {
            bytes,
            offset: 0,
            _work: work,
        })
    }
}

// One Node owns the original two payload allocations and control allowance.
// Active operations pin one exact Channel, even if the connection LRU evicts it.
// Thus two streams share one <=2MiB connection window; a different connection
// waits. Two bounded decoders add <=256KiB: 2MiB + 512KiB + 256KiB + 256KiB
// plus <=256KiB bounded request/key/fixed metadata remains below the original
// 4MiB transport envelope (private output/proof separate). A key owns <=8KiB;
// each operation retains two <=32KiB request representations. The cache/pool
// guards admit only two operations and one shared session.
#[cfg(feature = "dfs")]
#[derive(Default)]
pub(super) struct PeerStreamWorkPool {
    pub(super) state: std::sync::Mutex<PeerStreamWorkState>,
    pub(super) changed: tokio::sync::Notify,
}

#[cfg(feature = "dfs")]
#[derive(Default)]
pub(super) struct PeerStreamWorkState {
    pub(super) active: usize,
    pub(super) foreground_waiters: usize,
    pub(super) session: Option<Arc<PeerStreamSession>>,
}

#[cfg(feature = "dfs")]
pub(super) struct PeerStreamWaiter {
    pub(super) pool: Arc<PeerStreamWorkPool>,
}

#[cfg(feature = "dfs")]
impl PeerStreamWaiter {
    pub(super) fn new(pool: Arc<PeerStreamWorkPool>) -> afs_error::Result<Self> {
        pool.state
            .lock()
            .map_err(|_| dfs_protocol_error("Peer stream wait lock poisoned"))?
            .foreground_waiters += 1;
        Ok(Self { pool })
    }
}

#[cfg(feature = "dfs")]
impl Drop for PeerStreamWaiter {
    fn drop(&mut self) {
        if let Ok(mut state) = self.pool.state.lock() {
            state.foreground_waiters = state.foreground_waiters.saturating_sub(1);
        }
        self.pool.changed.notify_waiters();
    }
}

#[cfg(feature = "dfs")]
pub(super) struct PeerStreamSession {
    pub(super) key: PeerChannelKey,
    pub(super) channel: Channel,
    pub(super) blocks: std::sync::Mutex<Vec<PeerStreamBlock>>,
    pub(super) available: tokio::sync::Notify,
    pub(super) controls: Arc<tokio::sync::Semaphore>,
    // Fields drop in order: all payload owners precede this shared observation.
    pub(super) _work: std::sync::Mutex<Option<crate::node::metrics::work::PeerCacheStreamWork>>,
}

#[cfg(feature = "dfs")]
pub(super) struct PeerStreamActive {
    pub(super) pool: Arc<PeerStreamWorkPool>,
    pub(super) session: Arc<PeerStreamSession>,
}

#[cfg(feature = "dfs")]
impl PeerStreamActive {
    pub(super) async fn block(self: &Arc<Self>) -> afs_error::Result<PeerStreamBlockLease> {
        loop {
            let available = self.session.available.notified();
            tokio::pin!(available);
            available.as_mut().enable();
            if let Some(block) = self
                .session
                .blocks
                .lock()
                .map_err(|_| dfs_protocol_error("Peer stream block lock poisoned"))?
                .pop()
            {
                return Ok(PeerStreamBlockLease {
                    block,
                    active: self.clone(),
                });
            }
            available.await;
        }
    }
}

#[cfg(feature = "dfs")]
impl Drop for PeerStreamActive {
    fn drop(&mut self) {
        let retired = {
            let mut state = self
                .pool
                .state
                .lock()
                .unwrap_or_else(|error| error.into_inner());
            if state.active == 1 {
                // Retire the actual allocations and observation before publishing
                // zero. Waking another admission before our Arc fields drop could
                // otherwise overlap the old two blocks with two new allocations.
                self.session
                    .blocks
                    .lock()
                    .unwrap_or_else(|error| error.into_inner())
                    .clear();
                drop(
                    self.session
                        ._work
                        .lock()
                        .unwrap_or_else(|error| error.into_inner())
                        .take(),
                );
            }
            state.active = state.active.saturating_sub(1);
            if state.active == 0 {
                state.session.take()
            } else {
                None
            }
        };
        // Last operation/producer/frame drops before its ticket. Idle blocks are
        // destroyed here, never kept as a permanent 512KiB Node allocation.
        drop(retired);
        self.pool.changed.notify_waiters();
    }
}

#[cfg(feature = "dfs")]
pub(super) struct PeerStreamBlockLease {
    pub(super) block: PeerStreamBlock,
    pub(super) active: Arc<PeerStreamActive>,
}

#[cfg(feature = "dfs")]
impl std::ops::Deref for PeerStreamBlockLease {
    type Target = PeerStreamBlock;
    fn deref(&self) -> &Self::Target {
        &self.block
    }
}

#[cfg(feature = "dfs")]
impl std::ops::DerefMut for PeerStreamBlockLease {
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut self.block
    }
}

#[cfg(feature = "dfs")]
impl Drop for PeerStreamBlockLease {
    fn drop(&mut self) {
        // Move the existing allocation/lease back without allocating a new body,
        // including on producer cancellation, failed send and consumer errors.
        let mut block = PeerStreamBlock {
            bytes: std::mem::take(&mut self.block.bytes),
            offset: 0,
            _work: self.block._work.take(),
        };
        block.bytes.clear();
        self.active
            .session
            .blocks
            .lock()
            .unwrap_or_else(|error| error.into_inner())
            .push(block);
        self.active.session.available.notify_waiters();
    }
}

#[cfg(feature = "dfs")]
pub(super) struct OwnedPeerStreamControl {
    // Typed, budgeted metadata only; never a decoded opaque Bytes backing.
    pub(super) outboard_builder: Option<OutboardBuilder>,
    pub(super) frame: afs_protocol::node_data::DfsReadRangesFrame,
    pub(super) _permit: tokio::sync::OwnedSemaphorePermit,
}

#[cfg(feature = "dfs")]
pub(super) enum OwnedPeerStreamMessage {
    Control(Box<OwnedPeerStreamControl>),
    Block(PeerStreamBlockLease),
}

#[cfg(feature = "dfs")]
pub(super) fn peer_stream_control_capacity(
    frame: &mut afs_protocol::node_data::DfsReadRangesFrame,
) -> afs_error::Result<usize> {
    let backing = match &mut frame.body {
        Some(dfs_read_ranges_frame::Body::Header(header)) => {
            if !header.blake3_outboard.is_empty() {
                return Err(dfs_protocol_error(
                    "decoded outboard cannot enter the control queue",
                ));
            }
            header.read_id.capacity()
                + header.attempt_id.capacity()
                + header.chunk_id.capacity()
                + header.source_copy_id.capacity()
        }
        Some(dfs_read_ranges_frame::Body::Completion(completion)) => {
            completion.range_checksum.capacity()
                + completion.read_id.capacity()
                + completion.attempt_id.capacity()
                + completion.source_copy_id.capacity()
        }
        Some(dfs_read_ranges_frame::Body::Data(data)) if data.is_empty() => {
            // Empty Data still has protocol order semantics. Retire any decoder
            // backing before it crosses the control queue.
            *data = prost::bytes::Bytes::new();
            0
        }
        Some(dfs_read_ranges_frame::Body::Data(_)) => {
            return Err(dfs_protocol_error(
                "Peer data cannot enter the control queue",
            ));
        }
        None => 0,
    };
    if backing > STREAM_FRAME_BACKING_BYTES {
        return Err(dfs_protocol_error(
            "Peer stream control exceeds backing budget",
        ));
    }
    Ok(backing)
}

// Metadata is copied once on B, after admission to the Node's shared proof
// ledger. Decoded Bytes belong to the existing codec reservation and are dropped
// before any control frame is queued; no decoded backing escapes as proof state.
#[cfg(feature = "dfs")]
pub(super) fn peer_outboard_builder(
    expected: &ChunkObject,
    encoded: prost::bytes::Bytes,
    budget: Option<Arc<OutboardBudget>>,
    metrics: Option<&ReadWorkMetrics>,
) -> afs_error::Result<Option<OutboardBuilder>> {
    const MAX_ENCODED_OUTBOARD_BYTES: usize = 8192;
    if encoded.len() > MAX_ENCODED_OUTBOARD_BYTES {
        return Err(dfs_protocol_error("Peer outboard exceeds 8192-byte bound"));
    }
    let Some(budget) = budget else {
        return if encoded.is_empty() {
            Ok(None)
        } else {
            Err(dfs_protocol_error(
                "Peer outboard has no shared proof budget",
            ))
        };
    };
    if encoded.is_empty() {
        return Ok(Some(OutboardBuilder::new(expected.length, budget)));
    }
    // Shape/binding are checked even if the optional metadata pool is full.
    // Unused CVs are never advertised as verified; the original independent
    // complete-body root remains authoritative on this safe fallback path.
    ChunkOutboard::validate_encoded_shape(expected, &encoded)?;
    let charge = ChunkOutboard::resident_charge(encoded.len())?;
    let Some(mut lease) = budget.try_reserve(charge) else {
        return Ok(None);
    };
    let mut owned = Vec::new();
    if owned.try_reserve_exact(encoded.len()).is_err()
        || lease
            .retain_capacity(ChunkOutboard::resident_charge(owned.capacity())?)
            .is_err()
    {
        return Ok(None);
    }
    owned.extend_from_slice(&encoded);
    if let Some(metrics) = metrics {
        metrics.copy("peer_outboard_metadata", encoded.len() as u64);
    }
    drop(encoded);
    let proof = ChunkOutboard::from_encoded(expected, owned, lease)?;
    Ok(Some(OutboardBuilder::validating(
        expected.clone(),
        proof,
        budget,
    )))
}

/// The header fixes which local worker authenticates the complete object.
/// The producer always owns transport order/length/EOF; receipts never own bodies.
#[cfg(feature = "dfs")]
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(super) enum PeerRootMode {
    Producer,
    ConsumerTree,
}

#[cfg(feature = "dfs")]
impl PeerRootMode {
    /// Fix one complete-root owner before any body byte is accepted. A locally
    /// built charged tree is sufficient; optional peer CVs are not required.
    pub(super) fn select(builder: &mut Option<OutboardBuilder>) -> Self {
        if builder.as_ref().is_some_and(OutboardBuilder::has_tree) {
            Self::ConsumerTree
        } else {
            // A root-only builder would hash the same body as the plain root
            // owner, but cannot install an outboard. Retire it before input.
            builder.take();
            Self::Producer
        }
    }
}

#[cfg(feature = "dfs")]
#[derive(Clone, Copy, Debug)]
pub(super) enum PeerRootDigest {
    Producer([u8; 32]),
    ConsumerTree,
}

#[cfg(feature = "dfs")]
pub(super) struct PeerRootProducer {
    chunk: Option<ChunkObject>,
    proof_budget: Option<Arc<OutboardBudget>>,
    pub(super) outboard_builder: Option<OutboardBuilder>,
    pub(super) outboard_header_seen: bool,
    root_mode: PeerRootMode,
    hasher: blake3::Hasher,
    pub(super) length: u64,
    pub(super) expected_length: u64,
    pub(super) deadline: std::time::Instant,
    pub(super) binding: crate::node::read::peer_cache::PeerStreamBinding,
    pub(super) diagnostic: Option<ReadDiagnosticProducer>,
}

#[cfg(feature = "dfs")]
pub(super) struct PeerRootReceipt {
    pub(super) root_intervals: Option<ReadDiagnosticIntervals>,
    pub(super) length: u64,
    pub(super) digest: PeerRootDigest,
    pub(super) binding: crate::node::read::peer_cache::PeerStreamBinding,
}

#[cfg(feature = "dfs")]
impl PeerRootProducer {
    pub(super) fn new(
        expected_length: u64,
        binding: crate::node::read::peer_cache::PeerStreamBinding,
        diagnostic: Option<ReadDiagnosticProducer>,
        deadline: std::time::Instant,
    ) -> Self {
        Self {
            chunk: None,
            proof_budget: None,
            outboard_builder: None,
            outboard_header_seen: false,
            root_mode: PeerRootMode::Producer,
            hasher: blake3::Hasher::new(),
            length: 0,
            expected_length,
            binding,
            diagnostic,
            deadline,
        }
    }

    pub(super) fn with_outboard_context(
        mut self,
        chunk: ChunkObject,
        budget: Arc<OutboardBudget>,
    ) -> Self {
        self.chunk = Some(chunk);
        self.proof_budget = Some(budget);
        self
    }

    pub(super) fn accept_outboard_header(
        &mut self,
        header: &mut afs_protocol::node_data::DfsReadRangesHeader,
        metrics: Option<&ReadWorkMetrics>,
    ) -> afs_error::Result<()> {
        if self.outboard_header_seen || self.length != 0 {
            return Err(dfs_protocol_error("Peer outboard header order differs"));
        }
        self.outboard_header_seen = true;
        let encoded = std::mem::take(&mut header.blake3_outboard);
        if let Some(chunk) = &self.chunk {
            if header.chunk_id != chunk.id.0
                || header.chunk_offset != 0
                || header.length != chunk.length
            {
                return Err(dfs_protocol_error(
                    "Peer outboard header differs from trusted object",
                ));
            }
            self.outboard_builder =
                peer_outboard_builder(chunk, encoded, self.proof_budget.clone(), metrics)?;
            self.root_mode = PeerRootMode::select(&mut self.outboard_builder);
            if self.root_mode == PeerRootMode::Producer
                && let Some(metrics) = metrics
            {
                metrics.peer_read_event("root_fallback");
            }
        } else if !encoded.is_empty() {
            return Err(dfs_protocol_error("Peer outboard lacks a trusted object"));
        }
        Ok(())
    }

    pub(super) fn update(
        &mut self,
        offset: usize,
        bytes: &[u8],
        metrics: Option<&ReadWorkMetrics>,
    ) -> afs_error::Result<()> {
        peer_stream_deadline(self.deadline)?;
        let end = self
            .length
            .checked_add(bytes.len() as u64)
            .filter(|end| *end <= self.expected_length)
            .ok_or_else(|| {
                dfs_protocol_error("Peer producer root exceeds trusted object length")
            })?;
        if offset as u64 != self.length {
            return Err(dfs_protocol_error("Peer producer root block order differs"));
        }
        if self.root_mode == PeerRootMode::Producer {
            match &self.diagnostic {
                Some(diagnostic) => diagnostic.measure(bytes.len() as u64, || {
                    self.hasher.update(bytes);
                }),
                None => {
                    self.hasher.update(bytes);
                }
            }
            if let Some(metrics) = metrics {
                metrics.hash("root_in_receive", bytes.len() as u64);
            }
        }
        self.length = end;
        // Synchronous hashing is not preempted by timeout_at. Count actual work
        // above even when it consumed the remaining original shared deadline.
        peer_stream_deadline(self.deadline)
    }

    // Called only after actual transport EOF and final block handoff. The
    // consumer-tree variant attests transport completion, never a computed root.
    // It must be joined and combined with the consumer's independently computed root.
    pub(super) fn finish_after_eof(self) -> afs_error::Result<PeerRootReceipt> {
        peer_stream_deadline(self.deadline)?;
        if self.length != self.expected_length {
            return Err(dfs_protocol_error("Peer producer root is truncated"));
        }
        let digest = match self.root_mode {
            PeerRootMode::Producer => PeerRootDigest::Producer(match &self.diagnostic {
                Some(diagnostic) => diagnostic.measure(0, || *self.hasher.finalize().as_bytes()),
                None => *self.hasher.finalize().as_bytes(),
            }),
            PeerRootMode::ConsumerTree => PeerRootDigest::ConsumerTree,
        };
        if self.outboard_builder.is_some() {
            return Err(dfs_protocol_error(
                "Peer producer retained outboard work instead of handing it to the consumer",
            ));
        }
        peer_stream_deadline(self.deadline)?;
        Ok(PeerRootReceipt {
            root_intervals: self
                .diagnostic
                .as_ref()
                .map(ReadDiagnosticProducer::intervals),
            length: self.length,
            digest,
            binding: self.binding,
        })
    }
}

#[cfg(feature = "dfs")]
pub(super) async fn send_peer_stream_block(
    current: &mut Option<PeerStreamBlockLease>,
    sender: &tokio::sync::mpsc::Sender<afs_error::Result<OwnedPeerStreamMessage>>,
    metrics: Option<&ReadWorkMetrics>,
    root: &mut PeerRootProducer,
) -> afs_error::Result<()> {
    if let Some(block) = current.take() {
        if block.bytes.is_empty() {
            return Err(dfs_protocol_error("Peer stream handed off an empty block"));
        }
        // Check transport order/length before handoff. Hash here only when the
        // header selected the legacy producer-root fallback.
        root.update(block.offset, &block.bytes, metrics)?;
        let handoff_wait = metrics.and_then(|metrics| {
            metrics.coarse_diagnostic("peer_block_handoff_wait", block.bytes.len() as u64)
        });
        sender
            .send(Ok(OwnedPeerStreamMessage::Block(block)))
            .await
            .map_err(|_| dfs_protocol_error("Peer stream consumer stopped"))?;
        drop(handoff_wait);
        if let Some(metrics) = metrics {
            metrics.peer_read_event("stream_block_handoff");
        }
    }
    Ok(())
}

#[cfg(feature = "dfs")]
pub(super) async fn send_peer_stream_control(
    mut frame: afs_protocol::node_data::DfsReadRangesFrame,
    current: &mut Option<PeerStreamBlockLease>,
    sender: &tokio::sync::mpsc::Sender<afs_error::Result<OwnedPeerStreamMessage>>,
    controls: &Arc<tokio::sync::Semaphore>,
    metrics: Option<&ReadWorkMetrics>,
    root: &mut PeerRootProducer,
) -> afs_error::Result<()> {
    send_peer_stream_block(current, sender, metrics, root).await?;
    let backing = peer_stream_control_capacity(&mut frame)?;
    let permit = controls
        .clone()
        .acquire_many_owned(
            u32::try_from(backing)
                .map_err(|_| dfs_protocol_error("Peer control capacity conversion overflow"))?,
        )
        .await
        .map_err(|_| dfs_protocol_error("Peer stream control admission stopped"))?;
    let outboard_builder = if matches!(&frame.body, Some(dfs_read_ranges_frame::Body::Header(_))) {
        root.outboard_builder.take()
    } else {
        None
    };
    sender
        .send(Ok(OwnedPeerStreamMessage::Control(Box::new(
            OwnedPeerStreamControl {
                outboard_builder,
                frame,
                _permit: permit,
            },
        ))))
        .await
        .map_err(|_| dfs_protocol_error("Peer stream consumer stopped"))
}

#[cfg(feature = "dfs")]
pub(super) fn peer_stream_deadline(deadline: std::time::Instant) -> afs_error::Result<()> {
    if std::time::Instant::now() >= deadline {
        return Err(afs_error::Error::coded(
            afs_error::CLIENT_DEADLINE_EXCEEDED,
            "Peer stream disk total deadline exceeded",
        ));
    }
    Ok(())
}

#[cfg(feature = "dfs")]
pub(super) enum PeerSinkInput<'a> {
    Frame(afs_protocol::node_data::DfsReadRangesFrame),
    Block { offset: usize, bytes: &'a [u8] },
}

#[cfg(feature = "dfs")]
#[derive(Clone, Copy, Default)]
pub(super) struct PeerConsumerObservation<'a> {
    pub(super) metrics: Option<&'a ReadWorkMetrics>,
    pub(super) diagnostic: Option<&'a ReadDiagnosticConsumer>,
}

#[cfg(feature = "dfs")]
impl PeerConsumerObservation<'_> {
    pub(super) fn measure<T>(
        self,
        stage: ReadDiagnosticConsumerStage,
        bytes: u64,
        operation: impl FnOnce() -> T,
    ) -> T {
        match self.diagnostic {
            Some(diagnostic) => diagnostic.measure(stage, bytes, operation),
            None => operation(),
        }
    }
}

#[cfg(feature = "dfs")]
pub(super) fn consume_peer_frame_into_sink(
    decoder: &mut ReadFrameDecoder<'_>,
    frame: afs_protocol::node_data::DfsReadRangesFrame,
    requested_offset: u64,
    out: &mut [u8],
    sink: &mut PeerStreamSink<'_>,
    deadline: std::time::Instant,
    observation: PeerConsumerObservation<'_>,
) -> afs_error::Result<()> {
    consume_peer_input_into_sink(
        decoder,
        PeerSinkInput::Frame(frame),
        requested_offset,
        out,
        sink,
        deadline,
        observation,
    )
}

#[cfg(feature = "dfs")]
pub(super) fn consume_peer_input_into_sink(
    decoder: &mut ReadFrameDecoder<'_>,
    input: PeerSinkInput<'_>,
    requested_offset: u64,
    out: &mut [u8],
    sink: &mut PeerStreamSink<'_>,
    deadline: std::time::Instant,
    observation: PeerConsumerObservation<'_>,
) -> afs_error::Result<()> {
    let metrics = observation.metrics;
    peer_stream_deadline(deadline)?;
    let binding = decoder
        .stream_binding
        .as_ref()
        .ok_or_else(|| dfs_protocol_error("Peer stream output was not bound before decoding"))?;
    if !sink.accepts_binding(binding) || !binding.matches_output(requested_offset, out) {
        return Err(dfs_protocol_error(
            "Peer stream changed sink or private output during decoding",
        ));
    }
    match &input {
        PeerSinkInput::Frame(_) => {}
        PeerSinkInput::Block { offset, bytes } => {
            if bytes.is_empty()
                || bytes.len() > STREAM_FRAME_BACKING_BYTES
                || decoder
                    .active
                    .as_ref()
                    .is_none_or(|active| active.cursor != *offset)
            {
                return Err(dfs_protocol_error(
                    "Peer stream block offset/order/length mismatch",
                ));
            }
        }
    }
    let block_write = matches!(&input, PeerSinkInput::Block { .. });
    let full_length = decoder
        .verified_object
        .as_ref()
        .ok_or_else(|| dfs_protocol_error("Peer disk stream lacks full-object decoder"))?
        .expected_length;
    let write = |position: usize, bytes: &[u8]| {
        peer_stream_deadline(deadline)?;
        if block_write && let Some(metrics) = metrics {
            metrics.peer_read_event("stream_block_write");
        }
        observation.measure(
            ReadDiagnosticConsumerStage::SinkWrite,
            bytes.len() as u64,
            || sink.write(position as u64, bytes),
        )?;
        peer_stream_deadline(deadline)?;
        let from = (position as u64).max(requested_offset);
        let to = (position as u64 + bytes.len() as u64).min(requested_offset + out.len() as u64);
        if from < to {
            let length = (to - from) as usize;
            observation.measure(
                ReadDiagnosticConsumerStage::RangeExtract,
                length as u64,
                || {
                    out[(from - requested_offset) as usize..(to - requested_offset) as usize]
                        .copy_from_slice(
                            &bytes[(from - position as u64) as usize
                                ..(to - position as u64) as usize],
                        );
                },
            );
            if let Some(metrics) = metrics {
                metrics.copy("fill_to_output", length as u64);
            }
        }
        Ok(())
    };
    match input {
        PeerSinkInput::Frame(frame) => {
            decoder.accept_into(frame, full_length, write, observation)?
        }
        PeerSinkInput::Block { bytes, .. } => decoder.accept_data(bytes, write, observation)?,
    }
    peer_stream_deadline(deadline)
}
