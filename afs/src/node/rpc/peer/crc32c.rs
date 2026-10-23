//! CRC range transport shares the existing grant, connection and retry machinery.
use super::*;
use crate::node::{
    chunk_crc32c::{CrcTable, SEGMENT, checksum},
    dfs_read::PeerReadBatch,
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
        self.runtime.block_on(async {
            tokio::time::timeout(self.timeout, async {
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
                let mut stream = client
                    .read_ranges(request.clone())
                    .await
                    .map_err(peer_status_error)?
                    .into_inner();
                let mut decoder = CrcRangeDecoder::new(&request, chunk, out)?;
                while let Some(frame) = stream.message().await.map_err(peer_status_error)? {
                    decoder.accept(frame)?;
                }
                decoder.finish()
            })
            .await
            .map_err(|_| {
                afs_error::Error::coded(
                    afs_error::CLIENT_DEADLINE_EXCEEDED,
                    "CRC range deadline exceeded",
                )
            })?
        })
    }
}

fn peer_status_error(status: tonic::Status) -> afs_error::Error {
    afs_transport::grpc::error_status::status_to_error(status)
}

struct CrcRangeDecoder<'a> {
    request: &'a DfsReadRangesRequest,
    chunk: &'a ChunkObject,
    out: &'a mut [u8],
    table: Option<CrcTable>,
    cursor: usize,
    complete: bool,
}

impl<'a> CrcRangeDecoder<'a> {
    fn new(
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
            cursor: 0,
            complete: false,
        })
    }
    fn accept(
        &mut self,
        frame: afs_protocol::node_data::DfsReadRangesFrame,
    ) -> afs_error::Result<()> {
        let op = &self.request.operations[0];
        if self.complete {
            return Err(dfs_protocol_error("CRC frame after completion"));
        }
        match frame.body {
            Some(dfs_read_ranges_frame::Body::Header(header)) => {
                if self.table.is_some()
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
                let table: CrcTable = serde_json::from_slice(&header.crc32c_table)
                    .map_err(|_| dfs_corrupt_error("invalid CRC descriptor"))?;
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
                if self.table.is_none()
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
                self.cursor = next;
            }
            Some(dfs_read_ranges_frame::Body::Completion(done)) => {
                if self.table.is_none()
                    || done.read_id != self.request.read_id
                    || done.attempt_id != self.request.attempt_id
                    || done.operation_index != 0
                    || done.source_copy_id != op.source_copy_id
                    || done.transferred_bytes != op.length
                    || self.cursor as u64 != op.length
                    || done.range_checksum_algorithm
                        != afs_protocol::node_data::DfsDigestAlgorithm::Crc32c as i32
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
                if done.range_checksum.as_slice() != checksum(body).to_le_bytes() {
                    return Err(dfs_corrupt_error("CRC transport checksum mismatch"));
                }
                // Complete segments can additionally bind directly to Meta's
                // table. Boundary fragments rely on the authenticated server's
                // storage verification, without expanding the read grant.
                if op.chunk_offset.is_multiple_of(SEGMENT as u64)
                    && (op.length.is_multiple_of(SEGMENT as u64)
                        || op.chunk_offset + op.length == self.chunk.length)
                {
                    let table = self
                        .table
                        .as_ref()
                        .ok_or_else(|| dfs_protocol_error("CRC table missing"))?;
                    let index = (op.chunk_offset / SEGMENT as u64) as usize;
                    for (relative, bytes) in body.chunks(SEGMENT).enumerate() {
                        if table.checksums.get(index + relative) != Some(&checksum(bytes)) {
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
    fn finish(self) -> afs_error::Result<()> {
        if !self.complete {
            return Err(dfs_protocol_error("CRC stream omitted completion"));
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
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
