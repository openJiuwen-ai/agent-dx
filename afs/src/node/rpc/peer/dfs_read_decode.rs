//! Decoding and proving DFS peer read frames.

use super::*;

/// Only EOF-validated decoding can construct this non-memory stream receipt.
#[cfg(feature = "dfs")]
pub(in crate::node) struct CompletedPeerStreamProof {
    chunk: ChunkObject,
    root_digest: [u8; 32],
    pub(super) binding: crate::node::peer_cache::PeerStreamBinding,
    pub(super) outboard: Option<SharedOutboard>,
}

#[cfg(feature = "dfs")]
impl CompletedPeerStreamProof {
    pub(in crate::node) fn into_outboard(self) -> Option<SharedOutboard> {
        self.outboard
    }

    pub(in crate::node) fn binding(&self) -> &crate::node::peer_cache::PeerStreamBinding {
        &self.binding
    }
    pub(in crate::node) fn chunk(&self) -> &ChunkObject {
        &self.chunk
    }
    pub(in crate::node) fn root_digest(&self) -> &[u8; 32] {
        &self.root_digest
    }
}

#[cfg(all(test, feature = "dfs"))]
pub(super) fn peer_disk_test_frames(
    chunk: &ChunkObject,
    body: &[u8],
) -> (
    DfsReadRangesRequest,
    Vec<afs_protocol::node_data::DfsReadRangesFrame>,
) {
    use afs_protocol::node_data::{
        DfsReadRangesCompletion, DfsReadRangesFrame, DfsReadRangesHeader,
    };
    let request = DfsReadRangesRequest {
        include_blake3_outboard: true,
        read_id: "sink-test-read".into(),
        attempt_id: "sink-test-attempt".into(),
        file_version_id: "version".into(),
        layout_root_id: "layout".into(),
        operations: vec![DfsChunkReadOp {
            operation_index: 0,
            chunk_id: chunk.id.0.clone(),
            chunk_offset: 0,
            length: chunk.length,
            destination_offset: 0,
            source_copy_id: "sink-test-copy".into(),
            grant: None,
        }],
    };
    let mut frames = vec![DfsReadRangesFrame {
        body: Some(dfs_read_ranges_frame::Body::Header(DfsReadRangesHeader {
            crc32c_table: Default::default(),
            read_id: request.read_id.clone(),
            attempt_id: request.attempt_id.clone(),
            operation_index: 0,
            chunk_id: chunk.id.0.clone(),
            chunk_offset: 0,
            length: chunk.length,
            source_copy_id: "sink-test-copy".into(),
            blake3_outboard: Default::default(),
        })),
    }];
    frames.extend(
        body.chunks(STREAM_DATA_BYTES)
            .map(|data| DfsReadRangesFrame {
                body: Some(dfs_read_ranges_frame::Body::Data(data.to_vec().into())),
            }),
    );
    frames.push(DfsReadRangesFrame {
        body: Some(dfs_read_ranges_frame::Body::Completion(
            DfsReadRangesCompletion {
                crc32c_table: Default::default(),
                read_id: request.read_id.clone(),
                attempt_id: request.attempt_id.clone(),
                operation_index: 0,
                source_copy_id: "sink-test-copy".into(),
                transferred_bytes: body.len() as u64,
                range_checksum_algorithm: afs_protocol::node_data::DfsDigestAlgorithm::Blake3
                    as i32,
                range_checksum: blake3::hash(body).as_bytes().to_vec(),
            },
        )),
    });
    (request, frames)
}

#[cfg(all(test, feature = "dfs"))]
pub(in crate::node) fn complete_peer_disk_read_for_test<'output>(
    chunk: &ChunkObject,
    body: &[u8],
    requested_offset: u64,
    out: &'output mut [u8],
    sink: &mut PeerStreamSink<'_>,
) -> afs_error::Result<CompletedPeerDiskRead<'output>> {
    let (request, frames) = peer_disk_test_frames(chunk, body);
    let mut decoder = ReadFrameDecoder::for_object(&request, chunk)?
        .with_outboard_budget(Some(sink.proof_budget()));
    decoder.stream_binding = Some(sink.bind_output(requested_offset, out)?);
    let deadline = std::time::Instant::now() + Duration::from_secs(30);
    for frame in frames {
        consume_peer_frame_into_sink(
            &mut decoder,
            frame,
            requested_offset,
            out,
            sink,
            deadline,
            PeerConsumerObservation::default(),
        )?;
    }
    sink.seal_completed(decoder.finish_stream(chunk)?, requested_offset, out)
}

/// Only a complete, successful stream can mint this buffer-bound proof.
/// Borrowing the body prevents mutation or allocator reuse while it is usable.
#[cfg(feature = "dfs")]
#[derive(Debug)]
pub(in crate::node) struct CompletedPeerObjectProof<'a> {
    pub(super) bytes: &'a [u8],
    root_digest: [u8; 32],
    pub(super) outboard: Option<SharedOutboard>,
}

#[cfg(feature = "dfs")]
impl<'a> CompletedPeerObjectProof<'a> {
    pub(in crate::node) fn into_outboard(self) -> Option<SharedOutboard> {
        self.outboard
    }

    pub(in crate::node) fn bytes(&self) -> &'a [u8] {
        self.bytes
    }

    pub(in crate::node) fn root_digest(&self) -> &[u8; 32] {
        &self.root_digest
    }
}

#[cfg(feature = "dfs")]
pub(super) async fn consume_peer_read_stream<'a, S>(
    request: &DfsReadRangesRequest,
    chunk: Option<&'a ChunkObject>,
    out: &'a mut [u8],
    mut stream: S,
    metrics: Option<&ReadWorkMetrics>,
    proof_budget: Option<Arc<OutboardBudget>>,
) -> afs_error::Result<Option<VerifiedPeerChunk<'a>>>
where
    S: tokio_stream::Stream<
            Item = Result<afs_protocol::node_data::DfsReadRangesFrame, tonic::Status>,
        > + Unpin,
{
    let mut decoder = match chunk {
        Some(chunk) => {
            ReadFrameDecoder::for_object(request, chunk)?.with_outboard_budget(proof_budget)
        }
        None => ReadFrameDecoder::new(request),
    };
    decoder.work_metrics = metrics.cloned();
    while let Some(frame) = stream.next().await {
        let frame = frame.map_err(afs_transport::grpc::error_status::status_to_error)?;
        let copied = match &frame.body {
            Some(dfs_read_ranges_frame::Body::Data(bytes)) => bytes.len() as u64,
            _ => 0,
        };
        decoder.accept(frame, out)?;
        if let Some(metrics) = metrics {
            metrics.copy("frame_to_attempt", copied);
        }
    }
    // EOF, including a possible late gRPC Status, precedes proof creation.
    match chunk {
        Some(chunk) => {
            let body: &'a [u8] = out;
            let proof = decoder.finish_object(body)?;
            VerifiedPeerChunk::from_completed_stream(chunk, body, proof).map(Some)
        }
        None => decoder.finish().map(|()| None),
    }
}

#[cfg(feature = "dfs")]
pub(super) struct ReadFrameDecoder<'a> {
    request: &'a DfsReadRangesRequest,
    completed: Vec<bool>,
    pub(super) active: Option<ActivePeerRead>,
    pub(super) verified_object: Option<VerifiedObjectDecode>,
    pub(super) stream_binding: Option<crate::node::peer_cache::PeerStreamBinding>,
    producer_root_pending: bool,
    producer_outboard_installed: bool,
    root_mode: PeerRootMode,
    pub(super) work_metrics: Option<ReadWorkMetrics>,
}

#[cfg(feature = "dfs")]
pub(super) struct VerifiedObjectDecode {
    chunk: ChunkObject,
    proof_budget: Option<Arc<OutboardBudget>>,
    pub(super) outboard_builder: Option<OutboardBuilder>,
    pub(super) outboard: Option<SharedOutboard>,
    expected_root: [u8; 32],
    pub(super) expected_length: usize,
    actual_root: Option<[u8; 32]>,
    buffer: Option<(usize, usize)>,
}

#[cfg(feature = "dfs")]
impl VerifiedObjectDecode {
    fn finish_outboard(
        &mut self,
        observation: PeerConsumerObservation<'_>,
    ) -> afs_error::Result<Option<[u8; 32]>> {
        if let Some(builder) = self.outboard_builder.take() {
            // Legacy diagnostic name: this candidate's SegmentBlake3 lane is
            // standard-root/outboard work, not the removed v1 segment digest.
            let built =
                observation.measure(ReadDiagnosticConsumerStage::SegmentBlake3, 0, || {
                    builder.finish()
                })?;
            if built.digest.bytes != self.expected_root {
                return Err(dfs_corrupt_error(
                    "outboard body differs from trusted Meta root",
                ));
            }
            if let Some(metrics) = observation.metrics {
                metrics.hash("outboard_parent_hash", built.parent_hash_bytes);
            }
            self.outboard = built.outboard;
            return Ok(Some(built.digest.bytes));
        }
        Ok(None)
    }
}

#[cfg(feature = "dfs")]
pub(super) struct ActivePeerRead {
    index: usize,
    start: usize,
    end: usize,
    pub(super) cursor: usize,
    pub(super) checksum: Option<blake3::Hasher>,
}

#[cfg(feature = "dfs")]
impl<'a> ReadFrameDecoder<'a> {
    pub(super) fn new(request: &'a DfsReadRangesRequest) -> Self {
        Self {
            request,
            completed: vec![false; request.operations.len()],
            active: None,
            verified_object: None,
            stream_binding: None,
            producer_root_pending: false,
            producer_outboard_installed: false,
            root_mode: PeerRootMode::Producer,
            work_metrics: None,
        }
    }

    pub(super) fn for_object(
        request: &'a DfsReadRangesRequest,
        chunk: &ChunkObject,
    ) -> afs_error::Result<Self> {
        if chunk.length == 0
            || chunk.length > crate::node::chunk::MAX_STAGED_CHUNK_BYTES as u64
            || request.operations.len() != 1
            || request.operations[0].chunk_id != chunk.id.0
            || request.operations[0].chunk_offset != 0
            || request.operations[0].destination_offset != 0
            || request.operations[0].length != chunk.length
        {
            return Err(dfs_protocol_error(
                "verified Peer proof requires one complete trusted object range",
            ));
        }
        let mut decoder = Self::new(request);
        decoder.verified_object = Some(VerifiedObjectDecode {
            chunk: chunk.clone(),
            proof_budget: None,
            outboard_builder: None,
            outboard: None,
            expected_root: chunk.content_digest.bytes,
            expected_length: chunk.length as usize,
            actual_root: None,
            buffer: None,
        });
        Ok(decoder)
    }

    pub(super) fn with_outboard_budget(mut self, budget: Option<Arc<OutboardBudget>>) -> Self {
        if let Some(object) = &mut self.verified_object {
            object.proof_budget = budget;
        }
        self
    }

    pub(super) fn install_producer_outboard(
        &mut self,
        mut builder: Option<OutboardBuilder>,
    ) -> afs_error::Result<()> {
        if !self.producer_root_pending || self.active.is_some() || self.producer_outboard_installed
        {
            return Err(dfs_protocol_error(
                "outboard handoff is not before a producer header",
            ));
        }
        let object = self
            .verified_object
            .as_mut()
            .ok_or_else(|| dfs_protocol_error("outboard handoff lacks a trusted object"))?;
        if object.outboard_builder.is_some() {
            return Err(dfs_protocol_error("outboard work was already handed off"));
        }
        self.root_mode = PeerRootMode::select(&mut builder);
        object.outboard_builder = builder;
        self.producer_outboard_installed = true;
        Ok(())
    }

    pub(super) fn for_producer_object(
        request: &'a DfsReadRangesRequest,
        chunk: &ChunkObject,
    ) -> afs_error::Result<Self> {
        let mut decoder = Self::for_object(request, chunk)?;
        decoder.producer_root_pending = true;
        Ok(decoder)
    }

    pub(super) fn finish_stream_from_producer(
        mut self,
        chunk: &ChunkObject,
        receipt: PeerRootReceipt,
        sink: &PeerStreamSink<'_>,
        offset: u64,
        output: &[u8],
        observation: PeerConsumerObservation<'_>,
    ) -> afs_error::Result<CompletedPeerStreamProof> {
        self.check_frames_finished()?;
        let object = self
            .verified_object
            .as_ref()
            .ok_or_else(|| dfs_protocol_error("Peer producer receipt has no trusted object"))?;
        if !self.producer_root_pending
            || !self.producer_outboard_installed
            || receipt.length != object.expected_length as u64
            || !sink.accepts_binding(&receipt.binding)
            || !receipt.binding.matches_output(offset, output)
        {
            return Err(dfs_corrupt_error(
                "Peer joined transport receipt differs from trusted object or sink",
            ));
        }
        match (self.root_mode, receipt.digest) {
            (PeerRootMode::Producer, PeerRootDigest::Producer(digest))
                if digest == object.expected_root && Some(digest) == object.actual_root =>
            {
                if object.outboard_builder.is_some() {
                    return Err(dfs_protocol_error(
                        "producer root retained duplicate consumer root work",
                    ));
                }
            }
            (PeerRootMode::ConsumerTree, PeerRootDigest::ConsumerTree) => {
                let object = self
                    .verified_object
                    .as_mut()
                    .ok_or_else(|| dfs_protocol_error("consumer root lacks trusted object"))?;
                // actual_root still holds the wire Completion here. It is only
                // comparison evidence, never our independently computed root.
                let completion_root = object.actual_root;
                let computed_root = object
                    .finish_outboard(observation)?
                    .ok_or_else(|| dfs_protocol_error("consumer root work was not completed"))?;
                if computed_root != object.expected_root || Some(computed_root) != completion_root {
                    return Err(dfs_corrupt_error(
                        "Peer consumer root differs from Meta or wire completion",
                    ));
                }
                object.actual_root = Some(computed_root);
            }
            _ => return Err(dfs_corrupt_error("Peer root mode or joined digest differs")),
        }
        self.producer_root_pending = false;
        self.finish_stream(chunk)
    }

    pub(super) fn accept(
        &mut self,
        frame: afs_protocol::node_data::DfsReadRangesFrame,
        out: &mut [u8],
    ) -> afs_error::Result<()> {
        if let Some(object) = &mut self.verified_object {
            let buffer = (out.as_ptr() as usize, out.len());
            if out.len() != object.expected_length
                || object.buffer.is_some_and(|previous| previous != buffer)
            {
                return Err(dfs_protocol_error(
                    "verified Peer stream changed its private object buffer",
                ));
            }
            object.buffer = Some(buffer);
        }
        let work_metrics = self.work_metrics.clone();
        self.accept_into(
            frame,
            out.len(),
            |start, data| {
                out[start..start + data.len()].copy_from_slice(data);
                Ok(())
            },
            PeerConsumerObservation {
                metrics: work_metrics.as_ref(),
                diagnostic: None,
            },
        )
    }

    pub(super) fn accept_into(
        &mut self,
        frame: afs_protocol::node_data::DfsReadRangesFrame,
        output_length: usize,
        write: impl FnMut(usize, &[u8]) -> afs_error::Result<()>,
        observation: PeerConsumerObservation<'_>,
    ) -> afs_error::Result<()> {
        match frame.body {
            Some(dfs_read_ranges_frame::Body::Header(header)) => {
                let index = header.operation_index as usize;
                let op = self
                    .request
                    .operations
                    .get(index)
                    .ok_or_else(|| dfs_protocol_error("unknown peer read operation"))?;
                if self.active.is_some()
                    || self.completed[index]
                    || header.read_id != self.request.read_id
                    || header.attempt_id != self.request.attempt_id
                    || header.operation_index != op.operation_index
                    || header.chunk_id != op.chunk_id
                    || header.chunk_offset != op.chunk_offset
                    || header.length != op.length
                    || header.source_copy_id != op.source_copy_id
                {
                    return Err(dfs_protocol_error(
                        "peer read header identity/order mismatch",
                    ));
                }
                if !header.blake3_outboard.is_empty()
                    && (!self.request.include_blake3_outboard || self.verified_object.is_none())
                {
                    // A partial grant returns only its exact range and checksum;
                    // it can never manufacture a complete-object root capability.
                    return Err(dfs_protocol_error(
                        "outboard requires a trusted complete object",
                    ));
                }
                if !self.producer_root_pending {
                    if let Some(object) = &mut self.verified_object {
                        object.outboard_builder = peer_outboard_builder(
                            &object.chunk,
                            header.blake3_outboard,
                            object.proof_budget.clone(),
                            observation.metrics,
                        )?;
                        self.root_mode = PeerRootMode::select(&mut object.outboard_builder);
                        if self.root_mode == PeerRootMode::Producer
                            && let Some(metrics) = observation.metrics
                        {
                            metrics.peer_read_event("root_fallback");
                        }
                    }
                } else if !header.blake3_outboard.is_empty() {
                    return Err(dfs_protocol_error(
                        "producer did not retire decoded outboard backing",
                    ));
                }
                let start = usize::try_from(op.destination_offset)
                    .map_err(|_| dfs_protocol_error("read destination overflow"))?;
                let length = usize::try_from(op.length)
                    .map_err(|_| dfs_protocol_error("read length overflow"))?;
                let end = start
                    .checked_add(length)
                    .filter(|end| *end <= output_length)
                    .ok_or_else(|| dfs_protocol_error("read exceeds destination"))?;
                self.active = Some(ActivePeerRead {
                    index,
                    start,
                    end,
                    cursor: start,
                    checksum: (!self.producer_root_pending
                        && self.root_mode == PeerRootMode::Producer)
                        .then(blake3::Hasher::new),
                });
            }
            Some(dfs_read_ranges_frame::Body::Data(data)) => {
                self.accept_data(&data, write, observation)?
            }
            Some(dfs_read_ranges_frame::Body::Completion(completion)) => {
                let active = self
                    .active
                    .take()
                    .ok_or_else(|| dfs_protocol_error("completion without header"))?;
                let op = &self.request.operations[active.index];
                if completion.read_id != self.request.read_id
                    || completion.attempt_id != self.request.attempt_id
                    || completion.operation_index != op.operation_index
                    || completion.source_copy_id != op.source_copy_id
                    || active.cursor != active.end
                    || completion.transferred_bytes != (active.end - active.start) as u64
                    || completion.range_checksum_algorithm
                        != afs_protocol::node_data::DfsDigestAlgorithm::Blake3 as i32
                {
                    return Err(dfs_protocol_error(
                        "peer read completion identity/length mismatch",
                    ));
                }
                let digest =
                    if self.producer_root_pending {
                        blake3::Hash::from(
                            <[u8; 32]>::try_from(completion.range_checksum.as_slice()).map_err(
                                |_| dfs_corrupt_error("peer read completion checksum mismatch"),
                            )?,
                        )
                    } else if self.root_mode == PeerRootMode::ConsumerTree {
                        let object = self.verified_object.as_mut().ok_or_else(|| {
                            dfs_protocol_error("consumer tree lacks trusted object")
                        })?;
                        let root = object.finish_outboard(observation)?.ok_or_else(|| {
                            dfs_protocol_error("consumer tree root was not completed")
                        })?;
                        blake3::Hash::from(root)
                    } else {
                        let checksum = active.checksum.as_ref().ok_or_else(|| {
                            dfs_protocol_error("Peer consumer root state is missing")
                        })?;
                        observation.measure(ReadDiagnosticConsumerStage::RootBlake3, 0, || {
                            checksum.finalize()
                        })
                    };
                if completion.range_checksum.as_slice() != digest.as_bytes() {
                    return Err(dfs_corrupt_error("peer read completion checksum mismatch"));
                }
                if let Some(object) = &mut self.verified_object {
                    if !self.producer_root_pending && digest.as_bytes() != &object.expected_root {
                        return Err(dfs_corrupt_error(
                            "peer read object differs from trusted Meta root",
                        ));
                    }
                    object.actual_root = Some(*digest.as_bytes());
                }
                self.completed[active.index] = true;
            }
            None => return Err(dfs_protocol_error("empty peer frame")),
        }
        Ok(())
    }

    pub(super) fn accept_data(
        &mut self,
        data: &[u8],
        mut write: impl FnMut(usize, &[u8]) -> afs_error::Result<()>,
        observation: PeerConsumerObservation<'_>,
    ) -> afs_error::Result<()> {
        let active = self
            .active
            .as_mut()
            .ok_or_else(|| dfs_protocol_error("peer data without header"))?;
        let next = active
            .cursor
            .checked_add(data.len())
            .filter(|end| *end <= active.end)
            .ok_or_else(|| dfs_protocol_error("peer sent too much data"))?;
        write(active.cursor, data)?;
        if let Some(checksum) = &mut active.checksum {
            observation.measure(
                ReadDiagnosticConsumerStage::RootBlake3,
                data.len() as u64,
                || {
                    checksum.update(data);
                },
            );
            if let Some(metrics) = observation.metrics {
                metrics.hash(
                    if self.verified_object.is_some() {
                        "root_in_receive"
                    } else {
                        "peer_range_in_receive"
                    },
                    data.len() as u64,
                );
            }
        }
        if let Some(object) = &mut self.verified_object
            && let Some(builder) = &mut object.outboard_builder
        {
            observation.measure(
                ReadDiagnosticConsumerStage::SegmentBlake3,
                data.len() as u64,
                || builder.update(data),
            )?;
            if let Some(metrics) = observation.metrics {
                metrics.hash("outboard_body_hash", data.len() as u64);
            }
        }
        active.cursor = next;
        Ok(())
    }

    fn check_finished(&self) -> afs_error::Result<()> {
        if self.producer_root_pending {
            return Err(dfs_protocol_error(
                "Peer producer root receipt has not been joined and validated",
            ));
        }
        self.check_frames_finished()
    }

    fn check_frames_finished(&self) -> afs_error::Result<()> {
        if self.active.is_some()
            || self.completed.is_empty()
            || self.completed.iter().any(|done| !done)
        {
            return Err(dfs_protocol_error(
                "peer stream omitted a requested operation or completion",
            ));
        }
        Ok(())
    }

    pub(super) fn finish(self) -> afs_error::Result<()> {
        self.check_finished()
    }

    pub(super) fn finish_stream(
        self,
        chunk: &ChunkObject,
    ) -> afs_error::Result<CompletedPeerStreamProof> {
        self.check_finished()?;
        let object = self
            .verified_object
            .ok_or_else(|| dfs_protocol_error("ordinary ranges cannot mint stream proofs"))?;
        if self.request.operations.len() != 1
            || self.request.operations[0].chunk_id != chunk.id.0
            || object.expected_root != chunk.content_digest.bytes
            || object.expected_length as u64 != chunk.length
        {
            return Err(dfs_protocol_error("Peer stream proof object differs"));
        }
        Ok(CompletedPeerStreamProof {
            chunk: chunk.clone(),
            root_digest: object
                .actual_root
                .ok_or_else(|| dfs_protocol_error("Peer stream root was not completed"))?,
            outboard: object.outboard,
            binding: self
                .stream_binding
                .ok_or_else(|| dfs_protocol_error("Peer stream completion has no sink binding"))?,
        })
    }

    pub(super) fn finish_object<'body>(
        self,
        body: &'body [u8],
    ) -> afs_error::Result<CompletedPeerObjectProof<'body>> {
        self.check_finished()?;
        let object = self
            .verified_object
            .ok_or_else(|| dfs_protocol_error("ordinary ranges cannot mint object proofs"))?;
        if object.buffer != Some((body.as_ptr() as usize, body.len())) {
            return Err(dfs_protocol_error("Peer object proof buffer differs"));
        }
        Ok(CompletedPeerObjectProof {
            bytes: body,
            root_digest: object
                .actual_root
                .ok_or_else(|| dfs_protocol_error("Peer object root was not completed"))?,
            outboard: object.outboard,
        })
    }
}

#[cfg(feature = "dfs")]
pub(super) fn wire_dfs_read_grant(grant: &crate::dfs::DfsReadGrant) -> PbDfsReadGrant {
    PbDfsReadGrant {
        namespace_id: grant.namespace_id.0.clone(),
        file_version_id: grant.file_version_id.0.clone(),
        layout_root_id: grant.layout_root_id.0.clone(),
        caller_node_id: grant.caller_node_id.clone(),
        caller_node_epoch: grant.caller_node_epoch,
        expires_at_unix_ms: grant.expires_at_unix_ms,
        fence: grant.fence,
        token: grant.token.clone(),
    }
}

#[cfg(feature = "dfs")]
pub(super) fn dfs_protocol_error(message: impl Into<String>) -> afs_error::Error {
    afs_error::Error::coded(afs_error::CLIENT_PROTOCOL_VIOLATION, message)
}

#[cfg(feature = "dfs")]
pub(super) fn dfs_corrupt_error(message: impl Into<String>) -> afs_error::Error {
    afs_error::Error::coded(afs_error::NODE_TRANSFER_CORRUPT_DATA, message)
}
