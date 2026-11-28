//! Prepare every authorized range before publishing any part of a holder response.
use super::*;
use crate::node::read::holders::HolderInFlight;
use afs_protocol::node_data::DfsChunkReadOp;
use dfs_read_ranges_frame::Body;
use std::time::Instant;

// A small disk range can retain a complete verified Chunk allocation.
const MAX_PINNED_RANGES: usize = 8;

struct PreparedRange {
    header: DfsReadRangesHeader,
    body: prost::bytes::Bytes,
    completion: DfsReadRangesCompletion,
    partial: bool,
}

pub(super) struct HolderStream {
    pub(super) local: Arc<LocalChunkStore>,
    pub(super) request: Arc<DfsReadRangesRequest>,
    pub(super) sender: tokio::sync::mpsc::Sender<Result<DfsReadRangesFrame, Status>>,
    pub(super) metrics: Option<DfsPayloadMetrics>,
    pub(super) read_mode: crate::config::DfsReadCrcMode,
    pub(super) admission_deadline: Instant,
    pub(super) authorized_until: Instant,
}

impl HolderStream {
    fn range_stream(&self) -> RangeStream<'_> {
        RangeStream {
            local: &self.local,
            request: &self.request,
            sender: &self.sender,
            metrics: self.metrics.as_ref(),
            admission_deadline: self.admission_deadline,
            read_mode: self.read_mode,
        }
    }

    fn not_ready(&self) -> Status {
        self.local.record_read_cache_event("holder_not_ready");
        super::dfs_holder_serve::holder_not_ready()
    }

    pub(super) async fn serve(
        self: &Arc<Self>,
        flight: &HolderInFlight,
        epoch: u64,
    ) -> Result<(), Status> {
        if self.request.operations.len() > MAX_PINNED_RANGES {
            return Err(self.not_ready());
        }
        let mut prepared = Vec::with_capacity(self.request.operations.len());
        for op in &self.request.operations {
            if self.sender.is_closed() {
                return Ok(());
            }
            prepared.push(self.prepare(flight, epoch, op).await?);
        }
        let mut sent = 0;
        let result = self.send_ranges(prepared, &mut sent).await;
        if result.is_err()
            && sent > 0
            && let Some(metrics) = self.local.read_work_metrics()
        {
            metrics.holder_abandoned(sent);
        }
        result
    }

    async fn prepare(
        self: &Arc<Self>,
        flight: &HolderInFlight,
        epoch: u64,
        op: &DfsChunkReadOp,
    ) -> Result<PreparedRange, Status> {
        let chunk = crate::dfs::ChunkId::new(op.chunk_id.clone());
        if !self.local.has_servable_record(&chunk) {
            if let Some(view) = flight
                .partials
                .covering(&chunk, epoch, op.chunk_offset, op.length)
            {
                let body = view
                    .read(op.chunk_offset, op.length, |bytes| {
                        self.local.copy_verified_peer_range(bytes)
                    })
                    .map_err(|e| {
                        if e.code() == afs_error::IO_CAPACITY {
                            self.not_ready()
                        } else {
                            error_to_status(e)
                        }
                    })?;
                if let Some(body) = body {
                    let (header, completion) =
                        crc_range_metadata(&self.range_stream(), op, &view.table, body.as_slice())?;
                    return Ok(PreparedRange {
                        header,
                        completion,
                        body: body.into_shared_bytes(),
                        partial: true,
                    });
                }
            }
            // Installation can move a covered range into the catalog during the lookup.
            if !self.local.has_servable_record(&chunk) {
                return Err(self.not_ready());
            }
        }
        let stream = self.clone();
        let op = op.clone();
        crate::node::bridge::spawn_blocking(move || stream.prepare_disk_range(&op))
            .await
            .map_err(|_| Status::internal("DFS holder range worker stopped"))?
    }

    async fn send_ranges(&self, ranges: Vec<PreparedRange>, sent: &mut u64) -> Result<(), Status> {
        for range in ranges {
            self.send(Body::Header(range.header)).await?;
            for start in (0..range.body.len()).step_by(DFS_REPLICA_FRAME_BYTES) {
                let data = range
                    .body
                    .slice(start..(start + DFS_REPLICA_FRAME_BYTES).min(range.body.len()));
                let length = data.len() as u64;
                self.send(Body::Data(data)).await?;
                *sent += length;
                self.local.record_peer_read_event("data_frame");
                self.local.record_peer_read_event("shared_frame");
                if let Some(metrics) = &self.metrics {
                    metrics.record("grpc", "send", "read", length);
                }
            }
            self.send(Body::Completion(range.completion)).await?;
            if range.partial {
                self.local
                    .record_holder_served_partial(range.body.len() as u64);
            }
            #[cfg(test)]
            self.local.holder_op_complete();
        }
        Ok(())
    }

    // Admission controls acquiring resources. Once admitted, response flow control
    // has the remaining authorization lifetime, including time spent waiting for space.
    async fn send(&self, body: Body) -> Result<(), Status> {
        let permit = tokio::time::timeout_at(self.authorized_until.into(), self.sender.reserve())
            .await
            .map_err(|_| super::dfs_holder_serve::holder_denied())?
            .map_err(|_| Status::cancelled("DFS range consumer disconnected"))?;
        if tokio::time::Instant::now() >= self.authorized_until.into() {
            return Err(super::dfs_holder_serve::holder_denied());
        }
        self.local
            .record_read_check(crate::node::metrics::work::ReadCheck::RequestValidate);
        validate_served_read(&self.request)?;
        self.local
            .ensure_peer_serve_open()
            .map_err(error_to_status)?;
        permit.send(Ok(DfsReadRangesFrame { body: Some(body) }));
        Ok(())
    }
    fn prepare_disk_range(&self, op: &DfsChunkReadOp) -> Result<PreparedRange, Status> {
        let local = &self.local;
        let request = &self.request;
        let chunk = crate::dfs::ChunkId::new(op.chunk_id.clone());
        let classify = |error: afs_error::Error| {
            if error.code() == afs_error::IO_CAPACITY
                || (error.kind() != afs_error::ErrorKind::DataLoss
                    && !local.has_catalog_record(&chunk))
            {
                self.not_ready()
            } else {
                error_to_status(error)
            }
        };
        let table = local.peer_crc32c_table(&chunk).map_err(classify)?;
        // Keep the verified immutable allocation and its payload charge through transmission.
        let verified = local
            .read_peer_range_until(
                &chunk,
                op.chunk_offset,
                op.length as usize,
                self.admission_deadline,
                // A holder must verify its snapshot before publication even when
                // the peer requests a lighter wire checksum policy.
                crate::config::DfsReadCrcMode::Both,
            )
            .map_err(classify)?;
        if verified.len() as u64 != op.length {
            return Err(Status::data_loss("holder range ended early"));
        }
        let (header, completion) = if let Some(table) = table {
            crc_range_metadata(&self.range_stream(), op, &table, verified.as_slice())?
        } else {
            let outboard = if request.include_blake3_outboard {
                verified
                    .outboard()
                    .map(|p| prost::bytes::Bytes::from_owner(PeerOutboardOwner(p)))
                    .unwrap_or_default()
            } else {
                Default::default()
            };
            let digest = verified.verified_full_object_digest().map_or_else(
                || {
                    local.record_read_hash("stream_range_hash", op.length);
                    blake3::hash(verified.as_slice()).as_bytes().to_vec()
                },
                |digest| digest.bytes.to_vec(),
            );
            (
                DfsReadRangesHeader {
                    read_id: request.read_id.clone(),
                    attempt_id: request.attempt_id.clone(),
                    operation_index: op.operation_index,
                    source_copy_id: op.source_copy_id.clone(),
                    chunk_id: op.chunk_id.clone(),
                    chunk_offset: op.chunk_offset,
                    length: op.length,
                    blake3_outboard: outboard,
                    crc32c_table: Default::default(),
                },
                DfsReadRangesCompletion {
                    read_id: request.read_id.clone(),
                    attempt_id: request.attempt_id.clone(),
                    operation_index: op.operation_index,
                    source_copy_id: op.source_copy_id.clone(),
                    transferred_bytes: op.length,
                    range_checksum: digest,
                    range_checksum_algorithm: afs_protocol::node_data::DfsDigestAlgorithm::Blake3
                        as i32,
                    crc32c_table: Default::default(),
                },
            )
        };
        Ok(PreparedRange {
            header,
            completion,
            body: verified.into_shared_bytes(),
            partial: false,
        })
    }
}
