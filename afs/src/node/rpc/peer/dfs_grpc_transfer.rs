//! DFS peer reads over gRPC.

use super::*;

#[cfg(feature = "dfs")]
pub struct GrpcChunkTransfer {
    pub(super) read_pipeline: std::sync::OnceLock<Arc<read_pipeline::PeerReadPipeline>>,
    pub(super) peers: Arc<PeerConnectionPool>,
    pub(super) runtime: PeerRuntime,
    pub(super) timeout: Duration,
    pub(super) next_read: std::sync::atomic::AtomicU64,
}

#[cfg(feature = "dfs")]
impl GrpcChunkTransfer {
    pub fn new(peers: Arc<PeerConnectionPool>, timeout: Duration) -> afs_error::Result<Self> {
        // Standalone synchronous callers own a small runtime. Production
        // reuses its existing Tokio runtime.
        let runtime = PeerRuntime::current_or_new_with_worker()?;
        Ok(Self {
            read_pipeline: std::sync::OnceLock::new(),
            peers,
            runtime,
            timeout,
            next_read: std::sync::atomic::AtomicU64::new(1),
        })
    }
}

#[cfg(feature = "dfs")]
impl GrpcChunkTransfer {
    fn read_ranges_impl<'a>(
        &self,
        batch: &crate::node::dfs_read::PeerReadBatch,
        chunk: Option<&'a ChunkObject>,
        out: &'a mut [u8],
        proof_budget: Option<Arc<OutboardBudget>>,
    ) -> afs_error::Result<Option<VerifiedPeerChunk<'a>>> {
        self.runtime
            .block_on(self.read_ranges_impl_async(batch, chunk, out, proof_budget))
    }

    async fn read_ranges_impl_async<'a>(
        &self,
        batch: &crate::node::dfs_read::PeerReadBatch,
        chunk: Option<&'a ChunkObject>,
        out: &'a mut [u8],
        proof_budget: Option<Arc<OutboardBudget>>,
    ) -> afs_error::Result<Option<VerifiedPeerChunk<'a>>> {
        let _read_owner = self.admit_borrowed_read().await?;

        match chunk {
            Some(chunk) => crate::node::dfs_read::validate_verified_peer_object_request(
                batch,
                chunk,
                out.len(),
            )?,
            None => batch.validate(out.len())?,
        }
        let first = &batch.operations[0].source;
        let crate::dfs::CopyLocation::Node {
            node_id,
            node_epoch,
            ..
        } = &first.location
        else {
            return Err(dfs_protocol_error("peer read candidate is not a Node copy"));
        };
        let sequence = self
            .next_read
            .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
        let request = DfsReadRangesRequest {
            include_blake3_outboard: chunk.is_some(),
            read_id: format!(
                "{}-{}-{sequence}",
                first.read_grant.caller_node_id, first.read_grant.caller_node_epoch
            ),
            attempt_id: format!("{node_id}-{node_epoch}-{sequence}"),
            file_version_id: batch.file_version_id.0.clone(),
            layout_root_id: batch.layout_root_id.0.clone(),
            operations: batch
                .operations
                .iter()
                .enumerate()
                .map(|(index, item)| DfsChunkReadOp {
                    operation_index: index as u32,
                    chunk_id: item.op.chunk_id.0.clone(),
                    chunk_offset: item.op.chunk_offset,
                    length: item.op.length,
                    destination_offset: item.op.output_offset as u64,
                    source_copy_id: item.source.copy_id.0.clone(),
                    grant: Some(wire_dfs_read_grant(&item.source.read_grant)),
                })
                .collect(),
        };
        let timeout = crate::node::read_pipeline::current_read_scope()
            .map(|s| crate::node::read_pipeline::remaining_read_budget(s.deadline, self.timeout))
            .transpose()?
            .unwrap_or(self.timeout);
        async {
            // One deadline covers connection, response headers and every frame.
            tokio::time::timeout(timeout, async {
                let channel = self
                    .peers
                    .channel(
                        node_id,
                        *node_epoch,
                        first
                            .node_data_endpoint()
                            .ok_or_else(|| dfs_protocol_error("peer source has no endpoint"))?,
                    )
                    .await?;
                let mut client = DfsChunksClient::new(channel)
                    .max_encoding_message_size(self.peers.config.max_encoding_message_bytes)
                    .max_decoding_message_size(self.peers.config.max_decoding_message_bytes);
                let mut rpc_request = request_with_current_context(request.clone());
                rpc_request.set_timeout(timeout);
                if let Some(metrics) = &self.peers.read_work_metrics {
                    metrics.read_rpc("peer_read_ranges");
                }
                let response = client
                    .read_ranges(rpc_request)
                    .await
                    .map_err(afs_transport::grpc::error_status::status_to_error)?;
                consume_peer_read_stream(
                    &request,
                    chunk,
                    out,
                    response.into_inner(),
                    self.peers.read_work_metrics.as_ref(),
                    proof_budget,
                )
                .await
            })
            .await
            .map_err(|_| {
                afs_error::Error::coded(
                    afs_error::CLIENT_DEADLINE_EXCEEDED,
                    "DFS peer read total deadline exceeded",
                )
            })?
        }
        .await
    }
}

#[cfg(feature = "dfs")]
impl GrpcChunkTransfer {
    fn stream_sink_supported(&self) -> bool {
        let flavor = match &self.runtime {
            PeerRuntime::Existing(handle) => handle.runtime_flavor(),
            PeerRuntime::Owned(runtime) => runtime.handle().runtime_flavor(),
        };
        flavor == tokio::runtime::RuntimeFlavor::MultiThread
            && self.peers.config.initial_connection_window_size <= 2 * 1024 * 1024
            && self.peers.config.initial_stream_window_size <= 2 * 1024 * 1024
    }

    fn stream_sink_fits(&self, length: usize) -> bool {
        self.stream_sink_supported()
            && length
                .checked_add(4 * 1024 * 1024)
                .is_some_and(|bytes| bytes <= crate::node::dfs_read::MAX_DFS_READ_BYTES as usize)
    }

    fn read_object_to_sink<'output>(
        &self,
        batch: &crate::node::dfs_read::PeerReadBatch,
        chunk: &ChunkObject,
        requested_offset: u64,
        out: &'output mut [u8],
        sink: &mut PeerStreamSink<'_>,
        prefetch: Option<&crate::node::dfs_read::PrefetchControl>,
    ) -> afs_error::Result<Option<CompletedPeerDiskRead<'output>>> {
        // Capability rejection predates the sync API boundary: do not enter a
        // runtime or reserve transport work for a genuinely unsupported sink.
        if !self.stream_sink_fits(out.len()) {
            return Ok(None);
        }

        self.runtime.block_on(self.read_object_to_sink_async(
            batch,
            chunk,
            requested_offset,
            out,
            sink,
            prefetch,
        ))
    }

    async fn read_object_to_sink_async<'output>(
        &self,
        batch: &crate::node::dfs_read::PeerReadBatch,
        chunk: &ChunkObject,
        requested_offset: u64,
        out: &'output mut [u8],
        sink: &mut PeerStreamSink<'_>,
        prefetch: Option<&crate::node::dfs_read::PrefetchControl>,
    ) -> afs_error::Result<Option<CompletedPeerDiskRead<'output>>> {
        if !self.stream_sink_fits(out.len()) {
            return Ok(None);
        }
        let read_owner = self.admit_borrowed_read().await?;
        let Some(target) = self.peer_stream_target(batch, chunk, requested_offset, out.len())?
        else {
            return Ok(None);
        };
        let PeerStreamTarget {
            node_id,
            node_epoch,
            endpoint,
            request,
        } = target;
        let handle = match &self.runtime {
            PeerRuntime::Existing(handle) => handle.clone(),
            PeerRuntime::Owned(runtime) => runtime.handle().clone(),
        };
        let peers = self.peers.clone();
        let timeout = crate::node::read_pipeline::current_read_scope()
            .map(|s| crate::node::read_pipeline::remaining_read_budget(s.deadline, self.timeout))
            .transpose()?
            .unwrap_or(self.timeout);
        let deadline = std::time::Instant::now() + timeout;
        let output_binding = sink.bind_output(requested_offset, out)?;
        // Capture tracing metadata on the caller before crossing the task boundary.
        let mut producer_request = request_with_current_context(request.clone());
        producer_request.set_timeout(timeout);
        // Acquire before spawning, and retain through producer join and all
        // queued/consumer blocks. Every block also pins this operation ticket.
        if let Some(control) = prefetch {
            control.check()?;
        }
        let Some(active) = self
            .admit_peer_stream(&node_id, node_epoch, &endpoint, deadline, prefetch)
            .await?
        else {
            return Ok(None);
        };
        let controls = active.session.controls.clone();
        let producer_active = active.clone();
        if let Some(metrics) = &self.peers.read_work_metrics {
            metrics.peer_stream_private_output(out.len());
        }
        let (sender, mut receiver) =
            tokio::sync::mpsc::channel::<afs_error::Result<OwnedPeerStreamMessage>>(1);
        if let Some(metrics) = &self.peers.read_work_metrics {
            metrics.peer_read_event("stream_pipeline");
        }
        // Request and original payload owners are ready before this parent begins.
        // The journal stores metadata only and never extends a payload lifetime.
        let diagnostic = self
            .peers
            .read_work_metrics
            .as_ref()
            .and_then(|metrics| metrics.diagnostic(ReadDiagnosticStage::PeerObject));
        let consumer_diagnostic = diagnostic.as_ref().map(|span| span.token());
        let producer = PeerStreamProducer {
            sender,
            controls,
            peers,
            active: producer_active,
            control: prefetch.cloned(),
            diagnostic: consumer_diagnostic.clone(),
            deadline,
            expected_body_bytes: chunk.length,
        };
        let payload = PeerStreamPayload {
            request: producer_request,
            chunk: chunk.clone(),
            proof_budget: sink.proof_budget(),
            binding: output_binding.clone(),
        };
        let producer = handle.spawn(produce_peer_stream(read_owner, producer, payload));
        let received = consume_peer_stream(
            &mut receiver,
            PeerStreamConsumer {
                request: &request,
                chunk,
                metrics: self.peers.read_work_metrics.as_ref(),
                diagnostic: consumer_diagnostic.as_ref(),
                requested_offset,
                deadline,
                prefetch,
            },
            output_binding,
            out,
            sink,
        )
        .await;
        drop(receiver);
        if received.is_err() {
            producer.abort();
        }
        // Join/drain before output proof, sink cleanup or ownership retirement.
        let joined = producer.await;
        let (decoder, consumer_work) = received?;
        let observation = PeerConsumerObservation {
            metrics: self.peers.read_work_metrics.as_ref(),
            diagnostic: consumer_work.as_ref(),
        };
        let root = joined.map_err(|error| {
            afs_error::Error::coded(
                afs_error::CLIENT_WORKER_FAILED,
                format!("Peer stream producer join failed: {error}"),
            )
        })??;
        peer_stream_deadline(deadline)?;
        let root_intervals = root.root_intervals;
        let finish_start = consumer_diagnostic
            .as_ref()
            .map(|diagnostic| diagnostic.now());
        let proof = crate::node::bridge::local_io(|| {
            decoder.finish_stream_from_producer(
                chunk,
                root,
                sink,
                requested_offset,
                out,
                observation,
            )
        });
        if let (Some(diagnostic), Some(start)) = (&consumer_diagnostic, finish_start) {
            diagnostic.consumer(start);
        }
        let segment_intervals = consumer_work
            .as_ref()
            .map(ReadDiagnosticConsumer::segment_intervals);
        drop(consumer_work);
        if let (Some(diagnostic), Some(root_intervals), Some(segment_intervals)) =
            (&consumer_diagnostic, &root_intervals, &segment_intervals)
        {
            diagnostic.root_segment_overlap(root_intervals, segment_intervals);
        }
        let proof = proof?;
        peer_stream_deadline(deadline)?;
        if let Some(control) = prefetch {
            control.check()?;
        }
        let result = sink.seal_completed(proof, requested_offset, out).map(Some);
        if let Some(diagnostic) = diagnostic {
            diagnostic.finish(result.is_ok());
        }
        result
    }

    /// Validate the object request and build its single-object stream RPC.
    /// `None` is "unsupported": metadata too large for the stream bounds.
    fn peer_stream_target(
        &self,
        batch: &crate::node::dfs_read::PeerReadBatch,
        chunk: &ChunkObject,
        requested_offset: u64,
        out_len: usize,
    ) -> afs_error::Result<Option<PeerStreamTarget>> {
        crate::node::dfs_read::validate_verified_peer_object_request(
            batch,
            chunk,
            chunk.length as usize,
        )?;
        if requested_offset
            .checked_add(out_len as u64)
            .is_none_or(|end| end > chunk.length)
        {
            return Err(dfs_protocol_error(
                "Peer stream requested output exceeds object",
            ));
        }
        let source = &batch.operations[0].source;
        let crate::dfs::CopyLocation::Node {
            node_id,
            node_epoch,
            ..
        } = &source.location
        else {
            return Err(dfs_protocol_error("Peer stream source is not a Node"));
        };
        let endpoint = source
            .node_data_endpoint()
            .ok_or_else(|| dfs_protocol_error("Peer stream source endpoint is missing"))?;
        let grant = &source.read_grant;
        let metadata = [
            node_id.as_str(),
            endpoint,
            batch.file_version_id.0.as_str(),
            batch.layout_root_id.0.as_str(),
            chunk.id.0.as_str(),
            source.copy_id.0.as_str(),
            grant.namespace_id.0.as_str(),
            grant.file_version_id.0.as_str(),
            grant.layout_root_id.0.as_str(),
            grant.caller_node_id.as_str(),
            grant.caller_node_id.as_str(),
            grant.token.as_str(),
            node_id.as_str(),
        ]
        .iter()
        .fold(0usize, |total, value| total.saturating_add(value.len()));
        if metadata > STREAM_REQUEST_METADATA_BYTES / 2
            || node_id.len().saturating_add(endpoint.len()) > STREAM_KEY_BYTES
        {
            // Unsupported before cloning metadata or issuing an RPC; the caller
            // drains this sink and acquires the original exclusive legacy fill.
            return Ok(None);
        }
        let node_id = node_id.clone();
        let node_epoch = *node_epoch;
        let endpoint = endpoint.to_owned();
        let sequence = self
            .next_read
            .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
        let request = DfsReadRangesRequest {
            include_blake3_outboard: true,
            read_id: format!(
                "{}-{}-{sequence}",
                source.read_grant.caller_node_id, source.read_grant.caller_node_epoch
            ),
            attempt_id: format!("{node_id}-{node_epoch}-{sequence}"),
            file_version_id: batch.file_version_id.0.clone(),
            layout_root_id: batch.layout_root_id.0.clone(),
            operations: vec![DfsChunkReadOp {
                operation_index: 0,
                chunk_id: chunk.id.0.clone(),
                chunk_offset: 0,
                length: chunk.length,
                destination_offset: 0,
                source_copy_id: source.copy_id.0.clone(),
                grant: Some(wire_dfs_read_grant(&source.read_grant)),
            }],
        };
        let op = &request.operations[0];
        let request_capacity = request.read_id.capacity()
            + request.attempt_id.capacity()
            + request.file_version_id.capacity()
            + request.layout_root_id.capacity()
            + op.chunk_id.capacity()
            + op.source_copy_id.capacity()
            + op.grant.as_ref().map_or(0, |grant| {
                grant.namespace_id.capacity()
                    + grant.file_version_id.capacity()
                    + grant.layout_root_id.capacity()
                    + grant.caller_node_id.capacity()
                    + grant.token.capacity()
            });
        if request_capacity > STREAM_REQUEST_METADATA_BYTES {
            return Ok(None);
        }
        Ok(Some(PeerStreamTarget {
            node_id,
            node_epoch,
            endpoint,
            request,
        }))
    }

    /// Stream work admission for this peer, bounded by `deadline` and, for a
    /// prefetch, by its cancellation. `None` is a stopped background pool.
    async fn admit_peer_stream(
        &self,
        node_id: &str,
        node_epoch: u64,
        endpoint: &str,
        deadline: std::time::Instant,
        prefetch: Option<&crate::node::dfs_read::PrefetchControl>,
    ) -> afs_error::Result<Option<Arc<PeerStreamActive>>> {
        let admit = async {
            tokio::time::timeout_at(tokio::time::Instant::from_std(deadline), async {
                if prefetch.is_some() {
                    self.peers
                        .acquire_stream_work_mode(node_id, node_epoch, endpoint, true)
                        .await
                } else {
                    self.peers
                        .acquire_stream_work(node_id, node_epoch, endpoint)
                        .await
                        .map(Some)
                }
            })
            .await
            .map_err(|_| {
                afs_error::Error::coded(
                    afs_error::CLIENT_DEADLINE_EXCEEDED,
                    "Peer stream admission deadline exceeded",
                )
            })?
        };
        if let Some(control) = prefetch {
            tokio::select! {
                result = admit => result,
                _ = control.cancelled() => Err(afs_error::Error::coded(afs_error::IO_UNAVAILABLE, "Peer prefetch cancelled before admission")),
            }
        } else {
            admit.await
        }
    }
}

/// A validated single-object stream request and the peer it targets.
#[cfg(feature = "dfs")]
struct PeerStreamTarget {
    node_id: String,
    node_epoch: u64,
    endpoint: String,
    request: DfsReadRangesRequest,
}

/// Owned state of the spawned stream producer task.
#[cfg(feature = "dfs")]
struct PeerStreamProducer {
    sender: tokio::sync::mpsc::Sender<afs_error::Result<OwnedPeerStreamMessage>>,
    controls: Arc<tokio::sync::Semaphore>,
    peers: Arc<PeerConnectionPool>,
    active: Arc<PeerStreamActive>,
    control: Option<crate::node::dfs_read::PrefetchControl>,
    diagnostic: Option<crate::node::read_work::ReadDiagnosticToken>,
    deadline: std::time::Instant,
    expected_body_bytes: u64,
}

/// What the producer consumes once: the RPC and the root-proof inputs.
#[cfg(feature = "dfs")]
struct PeerStreamPayload {
    request: tonic::Request<DfsReadRangesRequest>,
    chunk: ChunkObject,
    proof_budget: Arc<crate::node::chunk::OutboardBudget>,
    binding: crate::node::peer_cache::PeerStreamBinding,
}

/// Borrowed inputs of the consumer half on the caller's task.
#[cfg(feature = "dfs")]
struct PeerStreamConsumer<'c> {
    request: &'c DfsReadRangesRequest,
    chunk: &'c ChunkObject,
    metrics: Option<&'c ReadWorkMetrics>,
    diagnostic: Option<&'c crate::node::read_work::ReadDiagnosticToken>,
    requested_offset: u64,
    deadline: std::time::Instant,
    prefetch: Option<&'c crate::node::dfs_read::PrefetchControl>,
}

/// The spawned producer: read frames within the deadline (and prefetch
/// cancellation), hand blocks and controls to the consumer, return the root.
#[cfg(feature = "dfs")]
async fn produce_peer_stream(
    read_owner: crate::node::read_pipeline::ReadLifetime,
    producer: PeerStreamProducer,
    payload: PeerStreamPayload,
) -> afs_error::Result<PeerRootReceipt> {
    let _read_owner = read_owner;
    let receive = async {
        tokio::time::timeout_at(
            tokio::time::Instant::from_std(producer.deadline),
            producer.receive_frames(payload),
        )
        .await
        .map_err(|_| {
            afs_error::Error::coded(
                afs_error::CLIENT_DEADLINE_EXCEEDED,
                "Peer stream producer deadline exceeded",
            )
        })?
    };
    let result: afs_error::Result<PeerRootReceipt> = if let Some(control) = &producer.control {
        tokio::select! {
            result = receive => result,
            _ = control.cancelled() => Err(afs_error::Error::coded(afs_error::IO_UNAVAILABLE, "Peer prefetch cancelled")),
        }
    } else {
        receive.await
    };
    if let Err(error) = &result {
        // The joined producer result carries the same failure. Never
        // wait again outside its deadline/cancellation boundary merely
        // to notify a consumer whose bounded queue may already be full.
        let _ = producer.sender.try_send(Err(error.clone()));
    }
    result
}

#[cfg(feature = "dfs")]
impl PeerStreamProducer {
    async fn receive_frames(
        &self,
        payload: PeerStreamPayload,
    ) -> afs_error::Result<PeerRootReceipt> {
        let peers = &self.peers;
        if let Some(control) = &self.control {
            control.check()?;
        }
        let mut client = DfsChunksClient::new(self.active.session.channel.clone())
            .max_encoding_message_size(peers.config.max_encoding_message_bytes)
            .max_decoding_message_size(
                peers
                    .config
                    .max_decoding_message_bytes
                    .min(STREAM_MESSAGE_BYTES),
            );
        let request = payload.request;
        if let Some(metrics) = &peers.read_work_metrics {
            metrics.read_rpc("peer_read_ranges");
        }
        let mut stream = client
            .read_ranges(request)
            .await
            .map_err(afs_transport::grpc::error_status::status_to_error)?
            .into_inner();
        let mut root = PeerRootProducer::new(
            self.expected_body_bytes,
            payload.binding,
            self.diagnostic.clone().map(ReadDiagnosticProducer::new),
            self.deadline,
        )
        .with_outboard_context(payload.chunk, payload.proof_budget);
        let mut current: Option<PeerStreamBlockLease> = None;
        let mut received_bytes = 0usize;
        while let Some(frame) = stream.next().await {
            if let Some(control) = &self.control {
                control.check()?;
            }
            let frame = frame.map_err(afs_transport::grpc::error_status::status_to_error)?;
            if let Some(metrics) = &peers.read_work_metrics {
                metrics.peer_read_event("stream_frame");
            }
            match frame.body {
                Some(dfs_read_ranges_frame::Body::Header(mut header)) => {
                    root.accept_outboard_header(&mut header, peers.read_work_metrics.as_ref())?;
                    self.send_control(
                        Some(dfs_read_ranges_frame::Body::Header(header)),
                        &mut current,
                        &mut root,
                    )
                    .await?;
                }
                Some(dfs_read_ranges_frame::Body::Data(data)) => {
                    self.forward_data(data, &mut current, &mut root, &mut received_bytes)
                        .await?;
                }
                body => self.send_control(body, &mut current, &mut root).await?,
            }
        }
        if let Some(diagnostic) = &self.diagnostic {
            diagnostic.eof();
        }
        // Flush truncated tails too: they still cannot mint a proof.
        send_peer_stream_block(
            &mut current,
            &self.sender,
            peers.read_work_metrics.as_ref(),
            &mut root,
        )
        .await?;
        root.finish_after_eof()
    }

    async fn send_control(
        &self,
        body: Option<dfs_read_ranges_frame::Body>,
        current: &mut Option<PeerStreamBlockLease>,
        root: &mut PeerRootProducer,
    ) -> afs_error::Result<()> {
        send_peer_stream_control(
            afs_protocol::node_data::DfsReadRangesFrame { body },
            current,
            &self.sender,
            &self.controls,
            self.peers.read_work_metrics.as_ref(),
            root,
        )
        .await
    }

    /// Copy one decoded data frame into the current shared block, handing
    /// blocks off whole. A wire frame is never split across owners.
    async fn forward_data(
        &self,
        data: prost::bytes::Bytes,
        current: &mut Option<PeerStreamBlockLease>,
        root: &mut PeerRootProducer,
        received_bytes: &mut usize,
    ) -> afs_error::Result<()> {
        let metrics = self.peers.read_work_metrics.as_ref();
        if !root.outboard_header_seen {
            return Err(dfs_protocol_error("Peer stream body precedes header"));
        }
        if data.len() > STREAM_DATA_BYTES {
            return Err(dfs_protocol_error(
                "Peer streaming data frame exceeds 64KiB bound",
            ));
        }
        if data.is_empty() {
            return self
                .send_control(Some(dfs_read_ranges_frame::Body::Data(data)), current, root)
                .await;
        }
        if let Some(diagnostic) = &self.diagnostic {
            // First nonempty decoded body, before staging/copy/hash.
            diagnostic.first_data();
        }
        // The transport endpoint is decoded body completion,
        // before frame-to-block copying, hashing or cache writes.
        // EOF and trusted proof remain separate later events.
        if let Some(diagnostic) = &self.diagnostic
            && (*received_bytes as u64).checked_add(data.len() as u64)
                == Some(self.expected_body_bytes)
        {
            diagnostic.body_complete();
        }
        // Do not split a wire frame across owners. An irregular
        // frame can flush a short block; its decoded backing is
        // still charged to the decoder, never queued as payload.
        if current
            .as_ref()
            .is_some_and(|block| block.bytes.len() + data.len() > STREAM_FRAME_BACKING_BYTES)
        {
            send_peer_stream_block(current, &self.sender, metrics, root).await?;
        }
        if current.is_none() {
            let wait = metrics.and_then(|metrics| metrics.coarse_diagnostic("peer_block_wait", 0));
            let mut block = self.active.block().await?;
            drop(wait);
            block.bytes.clear();
            block.offset = *received_bytes;
            *current = Some(block);
        }
        let block = current
            .as_mut()
            .ok_or_else(|| dfs_protocol_error("Peer stream block is missing"))?;
        {
            let _copy = metrics.and_then(|metrics| {
                metrics.coarse_diagnostic("peer_frame_handoff_copy", data.len() as u64)
            });
            block.bytes.extend_from_slice(&data);
        }
        *received_bytes = received_bytes
            .checked_add(data.len())
            .ok_or_else(|| dfs_protocol_error("Peer stream byte count overflow"))?;
        if let Some(metrics) = metrics {
            metrics.copy("peer_frame_to_block", data.len() as u64);
        }
        // The original decoder Bytes never enters the handoff.
        drop(data);
        if block.bytes.len() == STREAM_FRAME_BACKING_BYTES {
            send_peer_stream_block(current, &self.sender, metrics, root).await?;
        }
        Ok(())
    }
}

/// The consumer half on the caller's task: decode every handed-off control
/// and block into the sink and the private output, in order.
#[cfg(feature = "dfs")]
async fn consume_peer_stream<'c>(
    receiver: &mut tokio::sync::mpsc::Receiver<afs_error::Result<OwnedPeerStreamMessage>>,
    consumer: PeerStreamConsumer<'c>,
    output_binding: crate::node::peer_cache::PeerStreamBinding,
    out: &mut [u8],
    sink: &mut PeerStreamSink<'_>,
) -> afs_error::Result<(ReadFrameDecoder<'c>, Option<ReadDiagnosticConsumer>)> {
    let PeerStreamConsumer {
        request,
        chunk,
        metrics,
        diagnostic: consumer_diagnostic,
        requested_offset,
        deadline,
        prefetch,
    } = consumer;
    let consumer_work = consumer_diagnostic
        .cloned()
        .map(ReadDiagnosticConsumer::new);
    let mut decoder = ReadFrameDecoder::for_producer_object(request, chunk)?;
    decoder.stream_binding = Some(output_binding);
    while let Some(message) = receiver.recv().await {
        let observation = PeerConsumerObservation {
            metrics,
            diagnostic: consumer_work.as_ref(),
        };
        if let Some(control) = prefetch {
            control.check()?;
        }
        match message? {
            OwnedPeerStreamMessage::Control(control) => {
                let OwnedPeerStreamControl {
                    frame,
                    outboard_builder,
                    _permit,
                } = *control;
                if matches!(&frame.body, Some(dfs_read_ranges_frame::Body::Header(_))) {
                    decoder.install_producer_outboard(outboard_builder)?;
                } else if outboard_builder.is_some() {
                    return Err(dfs_protocol_error(
                        "outboard metadata must accompany its header",
                    ));
                }
                let consume_start = consumer_diagnostic.map(|diagnostic| diagnostic.now());
                let result = crate::node::bridge::local_io(|| {
                    consume_peer_frame_into_sink(
                        &mut decoder,
                        frame,
                        requested_offset,
                        out,
                        sink,
                        deadline,
                        observation,
                    )
                });
                if let (Some(diagnostic), Some(start)) = (consumer_diagnostic, consume_start) {
                    diagnostic.consumer(start);
                }
                result?;
            }
            OwnedPeerStreamMessage::Block(block) => {
                let consume_start = consumer_diagnostic.map(|diagnostic| diagnostic.now());
                let result = crate::node::bridge::local_io(|| {
                    consume_peer_input_into_sink(
                        &mut decoder,
                        PeerSinkInput::Block {
                            offset: block.offset,
                            bytes: &block.bytes,
                        },
                        requested_offset,
                        out,
                        sink,
                        deadline,
                        observation,
                    )
                });
                if let (Some(diagnostic), Some(start)) = (consumer_diagnostic, consume_start) {
                    diagnostic.consumer(start);
                }
                result?;
                // RAII returns the shared block on success and every error.
                drop(block);
            }
        }
    }
    Ok((decoder, consumer_work))
}

#[cfg(feature = "dfs")]
impl crate::node::dfs_read::ChunkTransfer for GrpcChunkTransfer {
    fn block_on_read(
        &self,
        future: crate::node::read_pipeline::ReadFuture<'_, ()>,
    ) -> afs_error::Result<()> {
        self.runtime.block_on(future)
    }
    fn read_ranges_async<'a>(
        &'a self,
        batch: &'a crate::node::dfs_read::PeerReadBatch,
        out: &'a mut [u8],
    ) -> crate::node::read_pipeline::ReadFuture<'a, ()> {
        Box::pin(async move {
            self.read_ranges_impl_async(batch, None, out, None)
                .await
                .map(|_| ())
        })
    }
    fn read_crc32c_range_async<'a>(
        &'a self,
        batch: &'a crate::node::dfs_read::PeerReadBatch,
        chunk: &'a ChunkObject,
        out: &'a mut [u8],
    ) -> crate::node::read_pipeline::ReadFuture<'a, ()> {
        Box::pin(async move {
            let _read_owner = self.admit_borrowed_read().await?;
            let deadline = crate::node::read_pipeline::current_read_scope().map(|s| s.deadline);
            self.read_crc32c_attempt_async(batch, chunk, out, deadline)
                .await
        })
    }

    fn read_verified_object_async<'a>(
        &'a self,
        batch: &'a crate::node::dfs_read::PeerReadBatch,
        chunk: &'a ChunkObject,
        out: &'a mut [u8],
        proof_budget: Arc<OutboardBudget>,
    ) -> crate::node::read_pipeline::ReadFuture<'a, VerifiedPeerChunk<'a>> {
        Box::pin(async move {
            self.read_ranges_impl_async(batch, Some(chunk), out, Some(proof_budget))
                .await?
                .ok_or_else(|| dfs_protocol_error("verified Peer stream omitted its object proof"))
        })
    }
    fn read_verified_object_to_sink_async<'a, 'output: 'a>(
        &'a self,
        batch: &'a crate::node::dfs_read::PeerReadBatch,
        chunk: &'a ChunkObject,
        requested_offset: u64,
        out: &'output mut [u8],
        sink: &'a mut PeerStreamSink<'_>,
        prefetch: Option<&'a crate::node::dfs_read::PrefetchControl>,
    ) -> crate::node::read_pipeline::ReadFuture<'a, Option<CompletedPeerDiskRead<'output>>> {
        Box::pin(self.read_object_to_sink_async(
            batch,
            chunk,
            requested_offset,
            out,
            sink,
            prefetch,
        ))
    }

    fn supports_async_native_read(&self) -> bool {
        true
    }

    fn admit_native_read_async(
        &self,
    ) -> crate::node::read_pipeline::ReadFuture<'_, crate::node::read_pipeline::ReadLifetime> {
        Box::pin(self.admit_borrowed_read())
    }
    fn configure_read_pipeline(
        &self,
        limits: crate::node::read_pipeline::ReadPipelineLimits,
    ) -> afs_error::Result<()> {
        self.configure_native_read_pipeline(limits)
    }

    fn read_crc32c_range(
        &self,
        batch: &crate::node::dfs_read::PeerReadBatch,
        chunk: &ChunkObject,
        out: &mut [u8],
    ) -> afs_error::Result<()> {
        self.read_crc32c_impl(batch, chunk, out)
    }

    fn supports_verified_object_sink(&self) -> bool {
        self.stream_sink_supported()
    }

    fn read_verified_object_to_sink<'output>(
        &self,
        batch: &crate::node::dfs_read::PeerReadBatch,
        chunk: &ChunkObject,
        requested_offset: u64,
        private_out: &'output mut [u8],
        sink: &mut PeerStreamSink<'_>,
    ) -> afs_error::Result<Option<CompletedPeerDiskRead<'output>>> {
        self.read_object_to_sink(batch, chunk, requested_offset, private_out, sink, None)
    }

    fn try_read_verified_object_to_sink<'output>(
        &self,
        batch: &crate::node::dfs_read::PeerReadBatch,
        chunk: &ChunkObject,
        requested_offset: u64,
        private_out: &'output mut [u8],
        sink: &mut PeerStreamSink<'_>,
        control: &crate::node::dfs_read::PrefetchControl,
    ) -> afs_error::Result<Option<CompletedPeerDiskRead<'output>>> {
        self.read_object_to_sink(
            batch,
            chunk,
            requested_offset,
            private_out,
            sink,
            Some(control),
        )
    }

    fn verification_metrics(&self) -> Option<&ReadWorkMetrics> {
        self.peers.read_work_metrics.as_ref()
    }

    fn read_ranges(
        &self,
        batch: &crate::node::dfs_read::PeerReadBatch,
        out: &mut [u8],
    ) -> afs_error::Result<()> {
        self.read_ranges_impl(batch, None, out, None).map(|_| ())
    }

    fn read_verified_object<'a>(
        &self,
        batch: &crate::node::dfs_read::PeerReadBatch,
        chunk: &'a ChunkObject,
        out: &'a mut [u8],
        proof_budget: Arc<OutboardBudget>,
    ) -> afs_error::Result<VerifiedPeerChunk<'a>> {
        self.read_ranges_impl(batch, Some(chunk), out, Some(proof_budget))?
            .ok_or_else(|| dfs_protocol_error("verified Peer stream omitted its object proof"))
    }
}
