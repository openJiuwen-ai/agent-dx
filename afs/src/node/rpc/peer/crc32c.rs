//! CRC range transport shares the existing grant, connection and retry machinery.
use super::*;
use crate::node::{
    chunk_crc32c::{CrcTable, SEGMENT, checksum},
    read::engine::PeerReadBatch,
};

#[cfg(feature = "rdma")]
pub(super) fn validate_rdma_crc(
    request: &DfsReadRangesRequest,
    reply: &afs_protocol::node_data::DfsReadRangesRdmaReply,
    bytes: &[u8],
    chunk: &ChunkObject,
) -> afs_error::Result<()> {
    if !chunk.valid_immutable_identity() || !chunk.is_segment_crc32c() {
        return Err(dfs_protocol_error("RDMA CRC object identity invalid"));
    }
    for (op, completion) in request.operations.iter().zip(&reply.completions) {
        if op.chunk_id != chunk.id.0
            || op
                .chunk_offset
                .checked_add(op.length)
                .is_none_or(|end| end > chunk.length)
            || completion.range_checksum_algorithm
                != afs_protocol::node_data::DfsDigestAlgorithm::Crc32c as i32
            || completion.crc32c_table.is_empty()
            || completion.crc32c_table.len() > 4096
        {
            return Err(dfs_protocol_error("RDMA CRC descriptor or range differs"));
        }
        let table: CrcTable = serde_json::from_slice(&completion.crc32c_table)
            .map_err(|_| dfs_corrupt_error("invalid RDMA CRC table"))?;
        if !table.valid_shape(chunk.length) || table.descriptor_digest() != chunk.content_digest {
            return Err(dfs_corrupt_error("RDMA CRC table differs from Meta"));
        }
        if op.chunk_offset.is_multiple_of(SEGMENT as u64)
            && (op.length.is_multiple_of(SEGMENT as u64)
                || op.chunk_offset + op.length == chunk.length)
        {
            let start = usize::try_from(op.destination_offset)
                .map_err(|_| dfs_protocol_error("RDMA CRC destination overflow"))?;
            let count = usize::try_from(op.length)
                .map_err(|_| dfs_protocol_error("RDMA CRC length overflow"))?;
            let body = bytes
                .get(
                    start
                        ..start
                            .checked_add(count)
                            .ok_or_else(|| dfs_protocol_error("RDMA CRC range overflow"))?,
                )
                .ok_or_else(|| dfs_protocol_error("RDMA CRC range exceeds MR"))?;
            let base = (op.chunk_offset / SEGMENT as u64) as usize;
            for (index, segment) in body.chunks(SEGMENT).enumerate() {
                if table.checksums.get(base + index) != Some(&checksum(segment)) {
                    return Err(dfs_corrupt_error("RDMA CRC segment differs from Meta"));
                }
            }
        }
    }
    Ok(())
}

impl GrpcChunkTransfer {
    pub(super) fn read_crc32c_impl(
        &self,
        batch: &PeerReadBatch,
        chunk: &ChunkObject,
        out: &mut [u8],
    ) -> afs_error::Result<()> {
        self.runtime
            .block_on(self.read_crc32c_attempt_async(batch, chunk, out, None))
    }

    pub(super) async fn read_crc32c_attempt_async(
        &self,
        batch: &PeerReadBatch,
        chunk: &ChunkObject,
        out: &mut [u8],
        deadline: Option<std::time::Instant>,
    ) -> afs_error::Result<()> {
        batch.validate(out.len())?;
        if !chunk.is_segment_crc32c()
            || !chunk.valid_immutable_identity()
            || batch.operations.len() != 1
        {
            return Err(dfs_protocol_error(
                "CRC transport needs one trusted object range",
            ));
        }
        let item = &batch.operations[0];
        if item.op.chunk_id != chunk.id
            || item.source.chunk_id != chunk.id
            || item
                .op
                .chunk_offset
                .checked_add(item.op.length)
                .is_none_or(|end| end > chunk.length)
        {
            return Err(dfs_protocol_error("CRC range differs from trusted object"));
        }
        let crate::dfs::CopyLocation::Node {
            node_id,
            node_epoch,
            ..
        } = &item.source.location
        else {
            return Err(dfs_protocol_error("CRC source is not a Node replica"));
        };
        if let Some(metrics) = &self.peers.read_work_metrics {
            metrics.check(crate::node::read_work::ReadCheck::FullGrant);
        }
        let sequence = self
            .next_read
            .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
        let request = DfsReadRangesRequest {
            read_id: format!("crc-{}-{sequence}", item.source.read_grant.caller_node_id),
            attempt_id: format!("{node_id}-{node_epoch}-{sequence}"),
            file_version_id: batch.file_version_id.0.clone(),
            layout_root_id: batch.layout_root_id.0.clone(),
            include_blake3_outboard: false,
            operations: vec![DfsChunkReadOp {
                operation_index: 0,
                chunk_id: chunk.id.0.clone(),
                chunk_offset: item.op.chunk_offset,
                length: item.op.length,
                destination_offset: item.op.output_offset as u64,
                source_copy_id: item.source.copy_id.0.clone(),
                grant: Some(wire_dfs_read_grant(&item.source.read_grant)),
            }],
        };
        let timeout = deadline
            .map(|d| crate::node::read::admission::remaining_read_budget(d, self.timeout))
            .transpose()?
            .unwrap_or(self.timeout);
        tokio::time::timeout(timeout, async {
            let channel = self
                .peers
                .channel(
                    node_id,
                    *node_epoch,
                    item.source
                        .data_endpoint
                        .as_ref()
                        .ok_or_else(|| dfs_protocol_error("CRC source endpoint missing"))?,
                )
                .await?;
            let mut client = DfsChunksClient::new(channel)
                .max_encoding_message_size(self.peers.config.max_encoding_message_bytes)
                .max_decoding_message_size(self.peers.config.max_decoding_message_bytes);
            let mut wire_request = tonic::Request::new(request.clone());
            wire_request.metadata_mut().insert(
                "x-afs-read-crc-mode",
                tonic::metadata::MetadataValue::from_static(self.peers.read_crc_mode.as_str()),
            );
            if let Some(deadline) = deadline {
                wire_request.set_timeout(crate::node::read::admission::remaining_read_budget(
                    deadline,
                    self.timeout,
                )?);
            }
            if let Some(metrics) = &self.peers.read_work_metrics {
                metrics.read_rpc("peer_read_ranges");
            }
            let _activity = self.peers.read_work_metrics.as_ref().map(|m| {
                m.activities
                    .enter(crate::node::read::activity::Activity::RangeRpc, 0)
            });
            let response = client
                .read_ranges(wire_request)
                .await
                .map_err(peer_status_error)?;
            let mut stream = response.into_inner();
            let mut decoder = CrcRangeDecoder::new(&request, chunk, out)?;
            decoder.metrics = self.peers.read_work_metrics.clone();
            decoder.mode = self.peers.read_crc_mode;
            while let Some(frame) = stream.message().await.map_err(peer_status_error)? {
                decoder.accept(frame)?;
            }
            decoder.finish()?;
            Ok(())
        })
        .await
        .map_err(|_| {
            afs_error::Error::coded(
                afs_error::CLIENT_DEADLINE_EXCEEDED,
                "CRC range deadline exceeded",
            )
        })?
    }
}

pub(super) fn peer_status_error(status: tonic::Status) -> afs_error::Error {
    afs_transport::grpc::error_status::status_to_error(status)
}

pub(super) struct CrcRangeDecoder<'a> {
    request: &'a DfsReadRangesRequest,
    chunk: &'a ChunkObject,
    out: &'a mut [u8],
    table: Option<CrcTable>,
    header_seen: bool,
    mode: crate::config::DfsReadCrcMode,
    cursor: usize,
    complete: bool,
    metrics: Option<ReadWorkMetrics>,
}

impl<'a> CrcRangeDecoder<'a> {
    pub(super) fn new(
        request: &'a DfsReadRangesRequest,
        chunk: &'a ChunkObject,
        out: &'a mut [u8],
    ) -> afs_error::Result<Self> {
        if request.operations.len() != 1 {
            return Err(dfs_protocol_error("CRC decoder requires one range"));
        }
        Ok(Self {
            request,
            chunk,
            out,
            table: None,
            header_seen: false,
            mode: crate::config::DfsReadCrcMode::Both,
            cursor: 0,
            complete: false,
            metrics: None,
        })
    }

    pub(super) fn accept(
        &mut self,
        frame: afs_protocol::node_data::DfsReadRangesFrame,
    ) -> afs_error::Result<()> {
        let op = &self.request.operations[0];
        if self.complete {
            return Err(dfs_protocol_error("CRC frame after completion"));
        }
        match frame.body {
            Some(dfs_read_ranges_frame::Body::Header(header)) => {
                if self.header_seen
                    || header.read_id != self.request.read_id
                    || header.attempt_id != self.request.attempt_id
                    || header.operation_index != 0
                    || header.chunk_id != op.chunk_id
                    || header.chunk_offset != op.chunk_offset
                    || header.length != op.length
                    || header.source_copy_id != op.source_copy_id
                    || !header.blake3_outboard.is_empty()
                    || header.crc32c_table.is_empty()
                    || header.crc32c_table.len() > 4096
                {
                    return Err(dfs_protocol_error(
                        "CRC header identity/descriptor mismatch",
                    ));
                }
                self.header_seen = true;
                if let Some(metrics) = &self.metrics {
                    metrics.check(crate::node::read_work::ReadCheck::DescriptorDecode);
                }
                let table: CrcTable = serde_json::from_slice(&header.crc32c_table)
                    .map_err(|_| dfs_corrupt_error("invalid CRC descriptor"))?;
                if let Some(metrics) = &self.metrics {
                    metrics.hash("crc_descriptor_hash", table.descriptor_input_bytes());
                }
                if !table.valid_shape(self.chunk.length)
                    || table.descriptor_digest() != self.chunk.content_digest
                {
                    return Err(dfs_corrupt_error(
                        "CRC descriptor differs from trusted Meta identity",
                    ));
                }
                self.table = Some(table);
            }
            Some(dfs_read_ranges_frame::Body::Data(data)) => {
                if !self.header_seen
                    || data.is_empty()
                    || data.len() > super::super::data::DFS_REPLICA_FRAME_BYTES
                {
                    return Err(dfs_protocol_error("CRC data order/frame size mismatch"));
                }
                let next = self
                    .cursor
                    .checked_add(data.len())
                    .filter(|end| *end as u64 <= op.length)
                    .ok_or_else(|| dfs_protocol_error("CRC data exceeds range"))?;
                let start = usize::try_from(op.destination_offset)
                    .map_err(|_| dfs_protocol_error("CRC destination overflow"))?;
                let target = self
                    .out
                    .get_mut(start + self.cursor..start + next)
                    .ok_or_else(|| dfs_protocol_error("CRC output exceeds destination"))?;
                target.copy_from_slice(&data);
                if let Some(metrics) = &self.metrics {
                    metrics.copy("frame_to_attempt", data.len() as u64);
                }
                self.cursor = next;
            }
            Some(dfs_read_ranges_frame::Body::Completion(done)) => {
                let complete_segments = op.chunk_offset.is_multiple_of(SEGMENT as u64)
                    && (op.length.is_multiple_of(SEGMENT as u64)
                        || op.chunk_offset.checked_add(op.length) == Some(self.chunk.length));
                let segment_only = done.range_checksum_algorithm
                    == afs_protocol::node_data::DfsDigestAlgorithm::Crc32cSegmentsV1 as i32;
                let legacy_range = done.range_checksum_algorithm
                    == afs_protocol::node_data::DfsDigestAlgorithm::Crc32c as i32;
                let receiver_only = done.range_checksum_algorithm
                    == afs_protocol::node_data::DfsDigestAlgorithm::Crc32cReceiverOnly as i32;
                let unchecked = done.range_checksum_algorithm
                    == afs_protocol::node_data::DfsDigestAlgorithm::ReadUnchecked as i32;
                let policy_matches = match self.mode {
                    crate::config::DfsReadCrcMode::Both => {
                        legacy_range || (segment_only && complete_segments)
                    }
                    crate::config::DfsReadCrcMode::Receiver => {
                        if complete_segments {
                            receiver_only
                        } else {
                            legacy_range
                        }
                    }
                    crate::config::DfsReadCrcMode::Off => unchecked,
                };
                if !self.header_seen
                    || done.read_id != self.request.read_id
                    || done.attempt_id != self.request.attempt_id
                    || done.operation_index != 0
                    || done.source_copy_id != op.source_copy_id
                    || done.transferred_bytes != op.length
                    || self.cursor as u64 != op.length
                    || !policy_matches
                    || ((segment_only || receiver_only || unchecked)
                        && !done.range_checksum.is_empty())
                {
                    return Err(dfs_protocol_error(
                        "CRC completion identity/length mismatch",
                    ));
                }
                let start = usize::try_from(op.destination_offset)
                    .map_err(|_| dfs_protocol_error("CRC destination overflow"))?;
                let body = self
                    .out
                    .get(start..start + self.cursor)
                    .ok_or_else(|| dfs_protocol_error("CRC output exceeds destination"))?;
                if legacy_range {
                    let crc_scope = self.metrics.as_ref().and_then(|metrics| {
                        metrics.coarse_diagnostic("crc_range_receive", body.len() as u64)
                    });
                    if let Some(metrics) = &self.metrics {
                        metrics.crc_read("range_receive", body.len() as u64);
                    }
                    let actual_crc = checksum(body);
                    drop(crc_scope);
                    if done.range_checksum.as_slice() != actual_crc.to_le_bytes() {
                        return Err(dfs_corrupt_error("CRC transport checksum mismatch"));
                    }
                }
                // Complete segments can additionally bind directly to Meta's
                // table. Boundary fragments rely on the authenticated server's
                // storage verification, without expanding the read grant.
                if complete_segments && self.mode != crate::config::DfsReadCrcMode::Off {
                    let table = self
                        .table
                        .as_ref()
                        .ok_or_else(|| dfs_protocol_error("CRC table missing"))?;
                    let index = (op.chunk_offset / SEGMENT as u64) as usize;
                    for (relative, bytes) in body.chunks(SEGMENT).enumerate() {
                        let crc_scope = self.metrics.as_ref().and_then(|metrics| {
                            metrics.coarse_diagnostic("crc_trusted_receive", bytes.len() as u64)
                        });
                        if let Some(metrics) = &self.metrics {
                            metrics.crc_read("trusted_segments_receive", bytes.len() as u64);
                        }
                        let actual_crc = checksum(bytes);
                        drop(crc_scope);
                        if table.checksums.get(index + relative) != Some(&actual_crc) {
                            return Err(dfs_corrupt_error(
                                "CRC range differs from trusted segment",
                            ));
                        }
                    }
                }
                self.complete = true;
            }
            None => return Err(dfs_protocol_error("empty CRC frame")),
        }
        Ok(())
    }
    pub(super) fn finish(self) -> afs_error::Result<()> {
        if !self.complete {
            return Err(dfs_protocol_error("CRC stream omitted completion"));
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    fn mode_frames(
        offset: usize,
        len: usize,
        mode: crate::config::DfsReadCrcMode,
        damaged: bool,
    ) -> (
        ChunkObject,
        DfsReadRangesRequest,
        Vec<DfsReadRangesFrame>,
        Vec<u8>,
    ) {
        let (chunk, request, mut frames, expected) = fixture(offset, len);
        let Some(dfs_read_ranges_frame::Body::Completion(done)) =
            frames.last_mut().unwrap().body.as_mut()
        else {
            panic!("completion absent")
        };
        done.range_checksum.clear();
        done.range_checksum_algorithm = match mode {
            crate::config::DfsReadCrcMode::Both => {
                afs_protocol::node_data::DfsDigestAlgorithm::Crc32cSegmentsV1
            }
            crate::config::DfsReadCrcMode::Receiver => {
                afs_protocol::node_data::DfsDigestAlgorithm::Crc32cReceiverOnly
            }
            crate::config::DfsReadCrcMode::Off => {
                afs_protocol::node_data::DfsDigestAlgorithm::ReadUnchecked
            }
        } as i32;
        if damaged {
            for frame in &mut frames {
                if let Some(dfs_read_ranges_frame::Body::Data(data)) = &mut frame.body {
                    let mut changed = data.to_vec();
                    changed[0] ^= 1;
                    *data = changed.into();
                    break;
                }
            }
        }
        (chunk, request, frames, expected)
    }
    #[test]
    fn crc_read_modes_receiver_detects_corruption_off_explicitly_does_not() {
        for (mode, offset, len, damaged, succeeds) in [
            (
                crate::config::DfsReadCrcMode::Receiver,
                0,
                SEGMENT,
                false,
                true,
            ),
            (
                crate::config::DfsReadCrcMode::Receiver,
                0,
                SEGMENT,
                true,
                false,
            ),
            (
                crate::config::DfsReadCrcMode::Receiver,
                2 * SEGMENT,
                13,
                false,
                true,
            ),
            (crate::config::DfsReadCrcMode::Off, 0, SEGMENT, true, true),
            (crate::config::DfsReadCrcMode::Off, 1, 7, true, true),
        ] {
            let (chunk, request, frames, expected) = mode_frames(offset, len, mode, damaged);
            let mut out = vec![0; len];
            let mut decoder = CrcRangeDecoder::new(&request, &chunk, &mut out).unwrap();
            decoder.mode = mode;
            let result = frames
                .into_iter()
                .try_for_each(|f| decoder.accept(f))
                .and_then(|()| decoder.finish());
            assert_eq!(result.is_ok(), succeeds, "mode={mode:?}");
            if succeeds {
                assert_eq!(out == expected, !damaged);
            }
        }
    }
    #[test]
    fn crc_read_modes_policy_tags_cannot_silently_downgrade() {
        use crate::config::DfsReadCrcMode::*;
        for caller in [Both, Receiver, Off] {
            for sender in [Both, Receiver, Off] {
                let (chunk, request, frames, _) = mode_frames(0, SEGMENT, sender, false);
                let mut out = vec![0; SEGMENT];
                let mut decoder = CrcRangeDecoder::new(&request, &chunk, &mut out).unwrap();
                decoder.mode = caller;
                let result = frames
                    .into_iter()
                    .try_for_each(|f| decoder.accept(f))
                    .and_then(|()| decoder.finish());
                assert_eq!(result.is_ok(), caller == sender);
            }
        }
    }
    #[test]
    fn crc_read_modes_off_still_requires_descriptor_identity_length_and_completion() {
        for defect in 0..4 {
            let (chunk, request, mut frames, _) =
                mode_frames(0, SEGMENT, crate::config::DfsReadCrcMode::Off, false);
            match defect {
                0 => {
                    frames.pop();
                }
                1 => {
                    if let Some(dfs_read_ranges_frame::Body::Header(h)) = frames[0].body.as_mut() {
                        h.crc32c_table = b"{}".to_vec();
                    }
                }
                2 => {
                    if let Some(dfs_read_ranges_frame::Body::Completion(c)) =
                        frames.last_mut().unwrap().body.as_mut()
                    {
                        c.transferred_bytes -= 1;
                    }
                }
                _ => {
                    if let Some(dfs_read_ranges_frame::Body::Header(h)) = frames[0].body.as_mut() {
                        h.chunk_id = "other".into();
                    }
                }
            }
            let mut out = vec![0; SEGMENT];
            let mut decoder = CrcRangeDecoder::new(&request, &chunk, &mut out).unwrap();
            decoder.mode = crate::config::DfsReadCrcMode::Off;
            let result = frames
                .into_iter()
                .try_for_each(|f| decoder.accept(f))
                .and_then(|()| decoder.finish());
            assert!(result.is_err());
        }
    }

    use super::*;
    use afs_protocol::node_data::{
        DfsReadRangesCompletion, DfsReadRangesFrame, DfsReadRangesHeader,
    };

    #[cfg(feature = "rdma")]
    #[test]
    fn crc32c_rdma_completion_binds_table_and_actual_body() {
        let (chunk, request, frames, body) = fixture(0, SEGMENT);
        let Some(dfs_read_ranges_frame::Body::Header(header)) = frames[0].body.clone() else {
            panic!("header missing")
        };
        let Some(dfs_read_ranges_frame::Body::Completion(mut done)) =
            frames.last().unwrap().body.clone()
        else {
            panic!("completion missing")
        };
        done.crc32c_table = header.crc32c_table;
        let reply = afs_protocol::node_data::DfsReadRangesRdmaReply {
            read_id: request.read_id.clone(),
            attempt_id: request.attempt_id.clone(),
            completions: vec![done],
            transferred_bytes: body.len() as u64,
        };
        validate_dfs_rdma_read_reply(&request, &reply, &body).unwrap();
        validate_rdma_crc(&request, &reply, &body, &chunk).unwrap();
        let mut bad_body = body.clone();
        bad_body[0] ^= 1;
        assert!(validate_dfs_rdma_read_reply(&request, &reply, &bad_body).is_err());
        let mut bad_table = reply;
        bad_table.completions[0].crc32c_table = b"{}".to_vec();
        assert!(validate_rdma_crc(&request, &bad_table, &body, &chunk).is_err());
    }

    fn fixture(
        offset: usize,
        length: usize,
    ) -> (
        ChunkObject,
        DfsReadRangesRequest,
        Vec<DfsReadRangesFrame>,
        Vec<u8>,
    ) {
        let bytes: Vec<u8> = (0..(2 * SEGMENT + 13)).map(|i| (i % 251) as u8).collect();
        let staged = crate::node::chunk::StagedChunk::new_crc32c(
            crate::dfs::OperationId::new("birth"),
            bytes.clone(),
        )
        .unwrap();
        let table = CrcTable::build(&bytes).unwrap();
        let request = DfsReadRangesRequest {
            read_id: "read".into(),
            attempt_id: "attempt".into(),
            file_version_id: "version".into(),
            layout_root_id: "layout".into(),
            include_blake3_outboard: false,
            operations: vec![DfsChunkReadOp {
                operation_index: 0,
                chunk_id: staged.chunk.id.0.clone(),
                chunk_offset: offset as u64,
                length: length as u64,
                destination_offset: 0,
                source_copy_id: "copy".into(),
                grant: None,
            }],
        };
        let expected = bytes[offset..offset + length].to_vec();
        let mut frames = vec![DfsReadRangesFrame {
            body: Some(dfs_read_ranges_frame::Body::Header(DfsReadRangesHeader {
                read_id: "read".into(),
                attempt_id: "attempt".into(),
                operation_index: 0,
                chunk_id: staged.chunk.id.0.clone(),
                chunk_offset: offset as u64,
                length: length as u64,
                source_copy_id: "copy".into(),
                blake3_outboard: Default::default(),
                crc32c_table: serde_json::to_vec(&table).unwrap(),
            })),
        }];
        frames.extend(expected.chunks(64 * 1024).map(|body| DfsReadRangesFrame {
            body: Some(dfs_read_ranges_frame::Body::Data(body.to_vec().into())),
        }));
        frames.push(DfsReadRangesFrame {
            body: Some(dfs_read_ranges_frame::Body::Completion(
                DfsReadRangesCompletion {
                    crc32c_table: Default::default(),
                    read_id: "read".into(),
                    attempt_id: "attempt".into(),
                    operation_index: 0,
                    source_copy_id: "copy".into(),
                    transferred_bytes: length as u64,
                    range_checksum: checksum(&expected).to_le_bytes().to_vec(),
                    range_checksum_algorithm: afs_protocol::node_data::DfsDigestAlgorithm::Crc32c
                        as i32,
                },
            )),
        });
        (staged.chunk, request, frames, expected)
    }

    #[test]
    fn crc32c_segment_only_completion_keeps_trusted_segments() {
        for (offset, length) in [(0, SEGMENT), (SEGMENT, SEGMENT), (2 * SEGMENT, 13)] {
            let (chunk, request, mut frames, expected) = fixture(offset, length);
            for frame in &mut frames {
                if let Some(dfs_read_ranges_frame::Body::Completion(done)) = &mut frame.body {
                    done.range_checksum_algorithm =
                        afs_protocol::node_data::DfsDigestAlgorithm::Crc32cSegmentsV1 as i32;
                    done.range_checksum.clear();
                }
            }
            let mut out = vec![0; length];
            let mut decoder = CrcRangeDecoder::new(&request, &chunk, &mut out).unwrap();
            let result = frames
                .into_iter()
                .try_for_each(|frame| decoder.accept(frame))
                .and_then(|()| decoder.finish());
            assert!(
                result.is_ok(),
                "segment-only completion must verify original segments without a range checksum: {result:?}"
            );
            assert_eq!(out, expected);
        }
    }

    #[test]
    fn crc32c_segment_only_rejects_fragments_damage_and_ambiguous_checksum() {
        for damage in 0..4 {
            let (offset, length) = if damage == 0 { (13, 79) } else { (0, SEGMENT) };
            let (chunk, request, mut frames, _) = fixture(offset, length);
            for frame in &mut frames {
                if let Some(dfs_read_ranges_frame::Body::Completion(done)) = &mut frame.body {
                    done.range_checksum_algorithm =
                        afs_protocol::node_data::DfsDigestAlgorithm::Crc32cSegmentsV1 as i32;
                    done.range_checksum.clear();
                    if damage == 2 {
                        done.range_checksum = vec![0; 4];
                    }
                }
                if damage == 1
                    && let Some(dfs_read_ranges_frame::Body::Data(data)) = &mut frame.body
                {
                    let mut body = data.to_vec();
                    body[0] ^= 1;
                    *data = body.into();
                }
            }
            if damage == 3 {
                frames.pop();
            }
            let mut out = vec![0; length];
            let mut decoder = CrcRangeDecoder::new(&request, &chunk, &mut out).unwrap();
            assert!(
                frames
                    .into_iter()
                    .try_for_each(|frame| decoder.accept(frame))
                    .and_then(|()| decoder.finish())
                    .is_err(),
                "segment-only damage {damage} accepted"
            );
        }
    }

    #[test]
    fn crc32c_remote_frames_cover_unaligned_range_and_short_tail() {
        for (offset, length) in [(SEGMENT - 9, 31), (0, 2 * SEGMENT + 13), (2 * SEGMENT, 13)] {
            let (chunk, request, frames, expected) = fixture(offset, length);
            let mut out = vec![0; length];
            let mut decoder = CrcRangeDecoder::new(&request, &chunk, &mut out).unwrap();
            for frame in frames {
                decoder.accept(frame).unwrap();
            }
            decoder.finish().unwrap();
            assert_eq!(out, expected);
        }
    }

    #[test]
    fn crc32c_remote_frames_reject_bad_table_body_identity_and_missing_completion() {
        for damage in 0..5 {
            let (chunk, request, mut frames, _) = fixture(0, SEGMENT);
            match damage {
                0 => {
                    if let Some(dfs_read_ranges_frame::Body::Header(header)) = &mut frames[0].body {
                        header.crc32c_table = b"{}".to_vec();
                    }
                }
                1 => {
                    if let Some(dfs_read_ranges_frame::Body::Data(body)) = &mut frames[1].body {
                        let mut corrupt = body.to_vec();
                        corrupt[0] ^= 1;
                        *body = corrupt.into();
                    }
                }
                2 => {
                    if let Some(dfs_read_ranges_frame::Body::Header(header)) = &mut frames[0].body {
                        header.source_copy_id = "wrong".into();
                    }
                }
                3 => {
                    frames.pop();
                }
                _ => {
                    let extra = frames.last().unwrap().clone();
                    frames.push(extra);
                }
            }
            let mut out = vec![0; SEGMENT];
            let mut decoder = CrcRangeDecoder::new(&request, &chunk, &mut out).unwrap();
            let result = frames
                .into_iter()
                .try_for_each(|frame| decoder.accept(frame))
                .and_then(|()| decoder.finish());
            assert!(result.is_err(), "damage {damage} accepted");
        }
    }
}
