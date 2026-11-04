//! Streaming verified DFS ranges to a peer.

use super::*;

/// Owns metadata only; body frames keep their separate payload lease.
#[cfg(feature = "dfs")]
pub(super) struct PeerOutboardOwner(pub(super) crate::node::chunk::outboard::SharedOutboard);

#[cfg(feature = "dfs")]
impl AsRef<[u8]> for PeerOutboardOwner {
    fn as_ref(&self) -> &[u8] {
        self.0.encoded()
    }
}

#[cfg(all(feature = "dfs", test))]
pub(super) fn stream_dfs_ranges(
    local: &LocalChunkStore,
    request: &DfsReadRangesRequest,
    sender: &tokio::sync::mpsc::Sender<Result<DfsReadRangesFrame, Status>>,
    metrics: Option<&DfsPayloadMetrics>,
) -> Result<(), Status> {
    stream_dfs_ranges_until(
        local,
        request,
        sender,
        metrics,
        local.peer_serve_deadline(),
        local.peer_read_crc_mode(),
    )
}

/// The caller's verification policy governs what this serving Node checks and
/// signals for the request; the serving Node's own setting applies only to its
/// own reads. A missing header keeps the full sender-and-receiver policy.
#[cfg(feature = "dfs")]
pub(super) fn requested_crc_read_mode(
    metadata: &tonic::metadata::MetadataMap,
) -> Result<crate::config::DfsReadCrcMode, Status> {
    use crate::config::DfsReadCrcMode;
    let requested = metadata
        .get("x-afs-read-crc-mode")
        .map(|v| v.to_str())
        .transpose()
        .map_err(|_| Status::invalid_argument("invalid read CRC mode metadata"))?;
    match requested {
        None | Some("both") => Ok(DfsReadCrcMode::Both),
        Some("receiver") => Ok(DfsReadCrcMode::Receiver),
        Some("off") => Ok(DfsReadCrcMode::Off),
        Some(_) => Err(Status::invalid_argument("unknown read CRC mode")),
    }
}

#[cfg(feature = "dfs")]
pub(super) fn stream_dfs_ranges_until(
    local: &LocalChunkStore,
    request: &DfsReadRangesRequest,
    sender: &tokio::sync::mpsc::Sender<Result<DfsReadRangesFrame, Status>>,
    metrics: Option<&DfsPayloadMetrics>,
    admission_deadline: std::time::Instant,
    read_mode: crate::config::DfsReadCrcMode,
) -> Result<(), Status> {
    let send = |body| {
        sender
            .blocking_send(Ok(DfsReadRangesFrame { body: Some(body) }))
            .map_err(|_| Status::cancelled("DFS range consumer disconnected"))
    };
    for op in &request.operations {
        if sender.is_closed() {
            return Ok(());
        }
        let diagnostic =
            local.read_diagnostic(crate::node::read_work::ReadDiagnosticStage::PeerServe);
        let diagnostic_token = diagnostic.as_ref().map(|span| span.token());
        if let Some(table) = local
            .peer_crc32c_table(&crate::dfs::ChunkId::new(op.chunk_id.clone()))
            .map_err(error_to_status)?
        {
            let verified = local
                .read_peer_range_until(
                    &crate::dfs::ChunkId::new(op.chunk_id.clone()),
                    op.chunk_offset,
                    usize::try_from(op.length)
                        .map_err(|_| Status::invalid_argument("CRC range too large"))?,
                    admission_deadline,
                    read_mode,
                )
                .map_err(error_to_status)?;
            if verified.len() as u64 != op.length {
                return Err(Status::data_loss("CRC range ended early"));
            }
            // Exact grants never disclose prefix/suffix bytes of a covering
            // storage segment. read_peer_range verified those segments first.
            // The persisted segment table already binds these complete segments
            // to Meta. Signal that mode explicitly; old receivers fail closed.
            // Fragments retain their original exact-range transport checksum.
            let segment_bytes = crate::node::chunk_crc32c::SEGMENT as u64;
            let segment_only = op.chunk_offset.is_multiple_of(segment_bytes)
                && (op.length.is_multiple_of(segment_bytes)
                    || op.chunk_offset.checked_add(op.length) == Some(table.length));
            let checksum = if segment_only || read_mode == crate::config::DfsReadCrcMode::Off {
                None
            } else {
                let crc_scope = local.coarse_diagnostic("crc_range_send", verified.len() as u64);
                local.record_crc_read("range_send", verified.len() as u64);
                let checksum = crate::node::chunk_crc32c::checksum(verified.as_slice());
                drop(crc_scope);
                Some(checksum)
            };
            local.record_read_check(crate::node::read_work::ReadCheck::DescriptorEncode);
            let descriptor =
                serde_json::to_vec(&table).map_err(|error| Status::internal(error.to_string()))?;
            local.record_read_check(crate::node::read_work::ReadCheck::RequestValidate);
            validate_dfs_read_request(request)?;
            local.ensure_peer_serve_open().map_err(error_to_status)?;
            send(dfs_read_ranges_frame::Body::Header(DfsReadRangesHeader {
                read_id: request.read_id.clone(),
                attempt_id: request.attempt_id.clone(),
                operation_index: op.operation_index,
                chunk_id: op.chunk_id.clone(),
                chunk_offset: op.chunk_offset,
                length: op.length,
                source_copy_id: op.source_copy_id.clone(),
                blake3_outboard: Default::default(),
                crc32c_table: descriptor,
            }))?;
            let body = verified.into_shared_bytes();
            for start in (0..body.len()).step_by(DFS_REPLICA_FRAME_BYTES) {
                local.record_read_check(crate::node::read_work::ReadCheck::RequestValidate);
                validate_dfs_read_request(request)?;
                local.ensure_peer_serve_open().map_err(error_to_status)?;
                let data = body.slice(start..(start + DFS_REPLICA_FRAME_BYTES).min(body.len()));
                local.record_peer_read_event("data_frame");
                local.record_peer_read_event("shared_frame");
                if let Some(metrics) = metrics {
                    metrics.record("grpc", "send", "read", data.len() as u64);
                }
                send(dfs_read_ranges_frame::Body::Data(data))?;
            }
            send(dfs_read_ranges_frame::Body::Completion(
                DfsReadRangesCompletion {
                    crc32c_table: Default::default(),
                    read_id: request.read_id.clone(),
                    attempt_id: request.attempt_id.clone(),
                    operation_index: op.operation_index,
                    source_copy_id: op.source_copy_id.clone(),
                    transferred_bytes: op.length,
                    range_checksum: checksum
                        .map(|value| value.to_le_bytes().to_vec())
                        .unwrap_or_default(),
                    range_checksum_algorithm: if read_mode == crate::config::DfsReadCrcMode::Off {
                        afs_protocol::node_data::DfsDigestAlgorithm::ReadUnchecked as i32
                    } else if segment_only && read_mode == crate::config::DfsReadCrcMode::Receiver {
                        afs_protocol::node_data::DfsDigestAlgorithm::Crc32cReceiverOnly as i32
                    } else if segment_only {
                        afs_protocol::node_data::DfsDigestAlgorithm::Crc32cSegmentsV1 as i32
                    } else {
                        afs_protocol::node_data::DfsDigestAlgorithm::Crc32c as i32
                    },
                },
            ))?;
            if let Some(diagnostic) = diagnostic {
                diagnostic.finish(true);
            }
            continue;
        }
        // Sidecars bind every streamed coverage group to the catalog's
        // standard root. There is no whole-object body allocation or pre-scan.
        // Legacy, partial and proof-capacity fallbacks retain the path below.
        if request.include_blake3_outboard
            && let Some(mut groups) = local
                .open_peer_group_stream_until(
                    &crate::dfs::ChunkId::new(op.chunk_id.clone()),
                    op.chunk_offset,
                    op.length as usize,
                    admission_deadline,
                )
                .map_err(error_to_status)?
        {
            let check_caller = || {
                if sender.is_closed() {
                    return Err(afs_transport::grpc::error_status::status_to_error(
                        Status::cancelled("DFS range consumer disconnected"),
                    ));
                }
                validate_dfs_read_request(request)
                    .map_err(afs_transport::grpc::error_status::status_to_error)
            };
            check_caller().map_err(error_to_status)?;
            local.ensure_peer_serve_open().map_err(error_to_status)?;
            send(dfs_read_ranges_frame::Body::Header(DfsReadRangesHeader {
                crc32c_table: Default::default(),
                read_id: request.read_id.clone(),
                attempt_id: request.attempt_id.clone(),
                operation_index: op.operation_index,
                chunk_id: op.chunk_id.clone(),
                chunk_offset: op.chunk_offset,
                length: op.length,
                source_copy_id: op.source_copy_id.clone(),
                blake3_outboard: prost::bytes::Bytes::from_owner(PeerOutboardOwner(
                    groups.outboard(),
                )),
            }))?;
            let mut first = true;
            while let Some(data) = groups.next_group(check_caller).map_err(error_to_status)? {
                let length = data.len();
                let first_enqueue = first
                    .then(|| local.coarse_diagnostic("peer_first_frame_enqueue", length as u64));
                send(dfs_read_ranges_frame::Body::Data(data))?;
                drop(first_enqueue);
                if first && let Some(token) = &diagnostic_token {
                    token.first_data();
                }
                first = false;
                local.record_peer_read_event("data_frame");
                local.record_peer_read_event("shared_frame");
                if let Some(metrics) = metrics {
                    metrics.record("grpc", "send", "read", length as u64);
                }
            }
            // next_group(None) has checked the current path, catalog and grant
            // after the last group. B still verifies the complete object before
            // installing it; a partial prefix is never an object publication.
            send(dfs_read_ranges_frame::Body::Completion(
                DfsReadRangesCompletion {
                    crc32c_table: Default::default(),
                    read_id: request.read_id.clone(),
                    attempt_id: request.attempt_id.clone(),
                    operation_index: op.operation_index,
                    source_copy_id: op.source_copy_id.clone(),
                    transferred_bytes: op.length,
                    range_checksum: groups.digest().bytes.to_vec(),
                    range_checksum_algorithm: afs_protocol::node_data::DfsDigestAlgorithm::Blake3
                        as i32,
                },
            ))?;
            if let Some(diagnostic) = diagnostic {
                diagnostic.finish(true);
            }
            continue;
        }
        // Authorization has already succeeded for this request. Reuse only
        // verified immutable bytes, with the shared payload lease held by
        // every queued frame. No separate range or frame body is allocated.
        let verified = local
            .read_peer_range_until(
                &crate::dfs::ChunkId::new(op.chunk_id.clone()),
                op.chunk_offset,
                op.length as usize,
                admission_deadline,
                read_mode,
            )
            .map_err(error_to_status)?;
        if verified.len() != op.length as usize {
            return Err(coded_status(
                afs_error::NODE_TRANSFER_CORRUPT_DATA,
                "local Chunk ended before the requested DFS range",
            ));
        }
        // This capability exists only for the full allocation already verified
        // against the local catalog. Partial/fallback views keep hashing the
        // actual range; no request or self-reported header supplies the proof.
        let whole_digest = verified.verified_full_object_digest();
        // The Bytes owner retains the same budgeted proof allocation through
        // queueing and tonic encoding. Codec/network buffers retain their
        // existing transport budget; they are not charged to this proof lease.
        let blake3_outboard = if request.include_blake3_outboard {
            verified
                .outboard()
                .map(|proof| prost::bytes::Bytes::from_owner(PeerOutboardOwner(proof)))
                .unwrap_or_default()
        } else {
            Default::default()
        };
        let verified = verified.into_shared_bytes();
        // A fill follower can wait behind I/O. Content sharing never renews the
        // caller's grant or bypasses a known Node stop; no new Meta RPC here.
        local.record_read_check(crate::node::read_work::ReadCheck::RequestValidate);
        validate_dfs_read_request(request)?;
        local.ensure_peer_serve_open().map_err(error_to_status)?;
        send(dfs_read_ranges_frame::Body::Header(DfsReadRangesHeader {
            crc32c_table: Default::default(),
            read_id: request.read_id.clone(),
            attempt_id: request.attempt_id.clone(),
            operation_index: op.operation_index,
            chunk_id: op.chunk_id.clone(),
            chunk_offset: op.chunk_offset,
            length: op.length,
            source_copy_id: op.source_copy_id.clone(),
            blake3_outboard,
        }))?;
        let mut offset = 0;
        let mut checksum = blake3::Hasher::new();
        while offset < op.length {
            if sender.is_closed() {
                return Ok(());
            }
            let length = (op.length - offset).min(64 * 1024) as usize;
            let data = verified.slice(offset as usize..offset as usize + length);
            if whole_digest.is_none() {
                checksum.update(&data);
                local.record_read_hash("stream_range_hash", length as u64);
            }
            let first_enqueue = (offset == 0)
                .then(|| local.coarse_diagnostic("peer_first_frame_enqueue", length as u64));
            send(dfs_read_ranges_frame::Body::Data(data))?;
            drop(first_enqueue);
            if offset == 0
                && let Some(token) = &diagnostic_token
            {
                // Accepted by the existing channel, not an on-wire/peer receipt.
                token.first_data();
            }
            local.record_peer_read_event("data_frame");
            local.record_peer_read_event("shared_frame");
            if let Some(metrics) = metrics {
                metrics.record("grpc", "send", "read", length as u64);
            }
            offset += length as u64;
        }
        send(dfs_read_ranges_frame::Body::Completion(
            DfsReadRangesCompletion {
                crc32c_table: Default::default(),
                read_id: request.read_id.clone(),
                attempt_id: request.attempt_id.clone(),
                operation_index: op.operation_index,
                source_copy_id: op.source_copy_id.clone(),
                transferred_bytes: op.length,
                range_checksum: whole_digest.map_or_else(
                    || checksum.finalize().as_bytes().to_vec(),
                    |digest| digest.bytes.to_vec(),
                ),
                range_checksum_algorithm: afs_protocol::node_data::DfsDigestAlgorithm::Blake3
                    as i32,
            },
        ))?;
        if let Some(diagnostic) = diagnostic {
            diagnostic.finish(true);
        }
    }
    Ok(())
}
