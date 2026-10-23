//! Prepared TCP payload measurement inside the normal authenticated DFS adapter.
use super::{ReadFrameDecoder, dfs_protocol_error};
use afs_protocol::node_data::{
    DfsFetchPreparedReadRequest, DfsPreparedReadReply, DfsReadRangesRequest,
    DfsReleasePreparedReadRequest, dfs_chunks_client::DfsChunksClient,
};
use afs_tracing::request_with_current_context;
use afs_transport::grpc::error_status::status_to_error;
use std::time::{Duration, Instant};
use tonic::transport::Channel;

fn validate_ready(
    request: &DfsReadRangesRequest,
    ready: &DfsPreparedReadReply,
) -> afs_error::Result<usize> {
    let length = request
        .operations
        .iter()
        .try_fold(0_u64, |sum, op| sum.checked_add(op.length))
        .ok_or_else(|| dfs_protocol_error("prepared length overflow"))?;
    if ready.prepared_read_id.is_empty()
        || ready.read_id != request.read_id
        || ready.attempt_id != request.attempt_id
        || ready.packed_length != length
        || ready.completions.len() != request.operations.len()
    {
        return Err(dfs_protocol_error(
            "prepared reply identity or length mismatch",
        ));
    }
    for (op, completion) in request.operations.iter().zip(&ready.completions) {
        if completion.read_id != request.read_id
            || completion.attempt_id != request.attempt_id
            || completion.operation_index != op.operation_index
            || completion.source_copy_id != op.source_copy_id
            || completion.transferred_bytes != op.length
            || completion.range_checksum.len() != 32
            || completion.range_checksum_algorithm
                != afs_protocol::node_data::DfsDigestAlgorithm::Blake3 as i32
        {
            return Err(dfs_protocol_error("prepared completion identity mismatch"));
        }
    }
    usize::try_from(length).map_err(|_| dfs_protocol_error("prepared length overflow"))
}

pub(super) async fn read(
    client: &mut DfsChunksClient<Channel>,
    original_request: &DfsReadRangesRequest,
    out: &mut [u8],
    timeout: Duration,
    metrics: &crate::node::dfs_read::DfsReadPayloadMetrics,
) -> afs_error::Result<()> {
    // Wire payload is packed independently of caller scatter offsets.
    let mut packed_request = original_request.clone();
    let mut packed_length = 0_u64;
    for operation in &mut packed_request.operations {
        operation.destination_offset = packed_length;
        packed_length = packed_length
            .checked_add(operation.length)
            .ok_or_else(|| dfs_protocol_error("prepared length overflow"))?;
    }
    let private_length = usize::try_from(packed_length)
        .map_err(|_| dfs_protocol_error("prepared length overflow"))?;
    let request = &packed_request;
    let mut prepare = request_with_current_context(request.clone());
    prepare.set_timeout(timeout);
    let prepare_started = Instant::now();
    let ready = client
        .prepare_read_ranges(prepare)
        .await
        .map_err(status_to_error)?
        .into_inner();
    metrics.observe_phase("grpc", "prepare", prepare_started.elapsed());
    // Allocation is complete before Fetch can start the source stream.
    let mut private = Vec::new();
    let metadata = validate_ready(request, &ready);
    let mut elapsed = None;
    let result = async {
        let length = metadata?;
        let allocate_started = Instant::now();
        private.try_reserve_exact(private_length).map_err(|_| {
            afs_error::Error::coded(
                afs_error::NODE_TRANSFER_RESOURCE_EXHAUSTED,
                "prepared receiver allocation failed",
            )
        })?;
        private.resize(private_length, 0);
        metrics.observe_phase("grpc", "allocate", allocate_started.elapsed());
        let mut decoder = ReadFrameDecoder::prepared(request);
        let mut fetch = request_with_current_context(DfsFetchPreparedReadRequest {
            prepared_read_id: ready.prepared_read_id.clone(),
            read_id: request.read_id.clone(),
            attempt_id: request.attempt_id.clone(),
        });
        fetch.set_timeout(timeout);
        let started = Instant::now();
        let transfer = async {
            let mut stream = client
                .fetch_prepared_read(fetch)
                .await
                .map_err(status_to_error)?
                .into_inner();
            while let Some(frame) = stream.message().await.map_err(status_to_error)? {
                decoder.accept(frame, &mut private)?;
            }
            Ok::<_, afs_error::Error>(())
        }
        .await;
        elapsed = Some(started.elapsed());
        transfer?;
        // Fetch must match the checksums made available before the timer started.
        let validate_started = Instant::now();
        if decoder.deferred.as_ref() != Some(&ready.completions) {
            return Err(dfs_protocol_error("Fetch completions differ from Prepare"));
        }
        decoder.finish_prepared(&private)?;
        metrics.observe_phase("grpc", "validate", validate_started.elapsed());
        Ok(length)
    }
    .await;
    if let Some(elapsed) = elapsed {
        metrics.observe_phase("grpc", "transfer", elapsed);
    }
    let mut release = request_with_current_context(DfsReleasePreparedReadRequest {
        prepared_read_id: ready.prepared_read_id,
        read_id: request.read_id.clone(),
        attempt_id: request.attempt_id.clone(),
    });
    release.set_timeout(timeout);
    let release_started = Instant::now();
    let released = client
        .release_prepared_read(release)
        .await
        .map_err(status_to_error);
    metrics.observe_phase("grpc", "release", release_started.elapsed());
    let result = result.and_then(|length| {
        // Fetch consumes the snapshot. `released=false` acknowledges an already
        // consumed/expired item; a successful RPC is the idempotent cleanup ack.
        released?;
        Ok(length)
    });
    if let Some(elapsed) = elapsed {
        metrics.observe(
            "grpc",
            if result.is_ok() { "ok" } else { "error" },
            result.as_ref().copied().unwrap_or(0),
            elapsed,
        );
    }
    result?;
    let scatter_started = Instant::now();
    for (original, packed) in original_request.operations.iter().zip(&request.operations) {
        // Batch validation checked caller ranges; decoder checked packed ranges.
        let start = usize::try_from(original.destination_offset)
            .map_err(|_| dfs_protocol_error("destination overflow"))?;
        let source = usize::try_from(packed.destination_offset)
            .map_err(|_| dfs_protocol_error("packed offset overflow"))?;
        let length =
            usize::try_from(original.length).map_err(|_| dfs_protocol_error("length overflow"))?;
        out[start..start + length].copy_from_slice(&private[source..source + length]);
    }
    metrics.observe_phase("grpc", "scatter", scatter_started.elapsed());
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::super::ReadFrameDecoder;
    use super::{read, validate_ready};
    use crate::node::{
        chunk::{ChunkStore, LocalChunkStore, StagedChunk},
        rpc::data::{DfsReadAuthorizer, PeerAuthenticator, make_dfs_chunks_server},
    };
    use afs_protocol::node_data::{
        DfsChunkReadOp, DfsPreparedReadReply, DfsReadRangesCompletion, DfsReadRangesFrame,
        DfsReadRangesHeader, DfsReadRangesRequest, dfs_chunks_client::DfsChunksClient,
    };
    use std::{net::SocketAddr, sync::Arc, time::Duration};
    use tokio::net::TcpListener;
    use tokio_stream::wrappers::TcpListenerStream;
    use tonic::transport::Server;

    fn request() -> DfsReadRangesRequest {
        DfsReadRangesRequest {
            read_id: "read".into(),
            attempt_id: "attempt".into(),
            file_version_id: "version".into(),
            layout_root_id: "layout".into(),
            include_blake3_outboard: false,
            operations: vec![DfsChunkReadOp {
                operation_index: 0,
                chunk_id: "chunk".into(),
                chunk_offset: 0,
                length: 2,
                destination_offset: 0,
                source_copy_id: "copy".into(),
                grant: Some(afs_protocol::node_data::DfsReadGrant {
                    namespace_id: "namespace".into(),
                    file_version_id: "version".into(),
                    layout_root_id: "layout".into(),
                    caller_node_id: "reader".into(),
                    caller_node_epoch: 1,
                    expires_at_unix_ms: u64::MAX,
                    fence: 1,
                    token: "fixture-grant".into(),
                }),
            }],
        }
    }

    fn completion(request: &DfsReadRangesRequest, bytes: &[u8]) -> DfsReadRangesCompletion {
        let operation = &request.operations[0];
        DfsReadRangesCompletion {
            crc32c_table: Default::default(),
            read_id: request.read_id.clone(),
            attempt_id: request.attempt_id.clone(),
            operation_index: operation.operation_index,
            source_copy_id: operation.source_copy_id.clone(),
            transferred_bytes: operation.length,
            range_checksum: blake3::hash(bytes).as_bytes().to_vec(),
            range_checksum_algorithm: afs_protocol::node_data::DfsDigestAlgorithm::Blake3 as i32,
        }
    }

    fn ready(request: &DfsReadRangesRequest, bytes: &[u8]) -> DfsPreparedReadReply {
        DfsPreparedReadReply {
            prepared_read_id: "prepared-id".into(),
            read_id: request.read_id.clone(),
            attempt_id: request.attempt_id.clone(),
            packed_length: bytes.len() as u64,
            completions: vec![completion(request, bytes)],
        }
    }

    fn frames(
        request: &DfsReadRangesRequest,
        bytes: &[u8],
    ) -> Vec<afs_protocol::node_data::DfsReadRangesFrame> {
        let operation = &request.operations[0];
        vec![
            DfsReadRangesFrame {
                body: Some(
                    afs_protocol::node_data::dfs_read_ranges_frame::Body::Header(
                        DfsReadRangesHeader {
                            crc32c_table: Default::default(),
                            read_id: request.read_id.clone(),
                            attempt_id: request.attempt_id.clone(),
                            operation_index: operation.operation_index,
                            chunk_id: operation.chunk_id.clone(),
                            chunk_offset: operation.chunk_offset,
                            length: operation.length,
                            source_copy_id: operation.source_copy_id.clone(),
                            blake3_outboard: prost::bytes::Bytes::new(),
                        },
                    ),
                ),
            },
            DfsReadRangesFrame {
                body: Some(afs_protocol::node_data::dfs_read_ranges_frame::Body::Data(
                    bytes.to_vec().into(),
                )),
            },
            DfsReadRangesFrame {
                body: Some(
                    afs_protocol::node_data::dfs_read_ranges_frame::Body::Completion(completion(
                        request, bytes,
                    )),
                ),
            },
        ]
    }

    #[test]
    fn ready_accepts_matching_identity_length_and_completion() {
        let request = request();
        assert_eq!(
            validate_ready(&request, &ready(&request, b"ok")).unwrap(),
            2
        );
    }

    #[test]
    fn ready_rejects_identity_length_and_completion_mismatches() {
        let request = request();

        let mut wrong_identity = ready(&request, b"ok");
        wrong_identity.attempt_id = "stale-attempt".into();
        assert!(validate_ready(&request, &wrong_identity).is_err());

        let mut wrong_length = ready(&request, b"ok");
        wrong_length.packed_length = 1;
        assert!(validate_ready(&request, &wrong_length).is_err());

        let mut malformed_completion = ready(&request, b"ok");
        malformed_completion.completions[0].range_checksum.pop();
        assert!(validate_ready(&request, &malformed_completion).is_err());
    }

    #[test]
    fn deferred_decoder_verifies_private_payload_after_transfer() {
        let request = request();
        let mut output = [0; 2];
        let mut decoder = ReadFrameDecoder::prepared(&request);
        for frame in frames(&request, b"ok") {
            decoder.accept(frame, &mut output).unwrap();
        }
        assert_eq!(&output, b"ok");
        decoder.finish_prepared(&output).unwrap();
    }

    #[test]
    fn deferred_decoder_rejects_corrupt_private_payload() {
        let request = request();
        let mut output = [0; 2];
        let mut decoder = ReadFrameDecoder::prepared(&request);
        for frame in frames(&request, b"ok") {
            decoder.accept(frame, &mut output).unwrap();
        }
        output[1] ^= 1;
        assert!(decoder.finish_prepared(&output).is_err());
    }

    struct FixturePeer;

    impl PeerAuthenticator for FixturePeer {
        fn authenticate(
            &self,
            _: &tonic::metadata::MetadataMap,
            _: Option<SocketAddr>,
            _: Option<&[u8]>,
        ) -> afs_error::Result<String> {
            Ok("reader".into())
        }
    }

    struct FixtureReadAuthority;

    impl DfsReadAuthorizer for FixtureReadAuthority {
        fn authorize(&self, peer: &str, request: &DfsReadRangesRequest) -> afs_error::Result<()> {
            if peer == "reader"
                && request.operations.iter().all(|operation| {
                    operation.grant.as_ref().is_some_and(|grant| {
                        grant.caller_node_id == "reader" && grant.token == "fixture-grant"
                    })
                })
            {
                Ok(())
            } else {
                Err(afs_error::Error::coded(
                    afs_error::NODE_TRANSFER_INVALID,
                    "fixture prepared-read grant rejected",
                ))
            }
        }
    }

    async fn spawn_source(local: Arc<LocalChunkStore>) -> (String, tokio::task::JoinHandle<()>) {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let endpoint = format!("http://{}", listener.local_addr().unwrap());
        let server = make_dfs_chunks_server(
            Some(local),
            Arc::new(FixturePeer),
            Arc::new(FixtureReadAuthority),
        );
        let task = tokio::spawn(async move {
            Server::builder()
                .add_service(server)
                .serve_with_incoming(TcpListenerStream::new(listener))
                .await
                .unwrap();
        });
        (endpoint, task)
    }

    #[tokio::test]
    async fn real_source_fetch_consumes_snapshot_and_release_false_is_acknowledged() {
        let temp = tempfile::tempdir().unwrap();
        let local = Arc::new(LocalChunkStore::open(temp.path(), "source").unwrap());
        let chunk = StagedChunk::new(
            crate::dfs::OperationId::new("prepared-peer-reader"),
            b"abcd".to_vec(),
        );
        local.put(chunk.clone()).unwrap();
        let (endpoint, server) = spawn_source(local).await;
        let mut client = DfsChunksClient::connect(endpoint).await.unwrap();
        let registry = afs_metrics::Registry::new();
        let metrics = crate::node::dfs_read::DfsReadPayloadMetrics::register(&registry).unwrap();
        let mut request = request();
        request.operations[0].chunk_id = chunk.chunk.id.0.clone();
        request.operations[0].destination_offset = 1;
        let mut second = request.operations[0].clone();
        second.operation_index = 1;
        second.chunk_offset = 2;
        second.destination_offset = 4;
        second.source_copy_id = "copy-1".into();
        request.operations.push(second);
        let mut output = [b'!'; 7];

        read(
            &mut client,
            &request,
            &mut output,
            Duration::from_secs(2),
            &metrics,
        )
        .await
        .unwrap();

        assert_eq!(&output, b"!ab!cd!");
        let encoded = afs_metrics::encode_text(&registry).unwrap();
        assert!(
            encoded.contains("afs_dfs_read_payload_bytes_total{transport=\"grpc\"} 4"),
            "{encoded}"
        );
        assert!(
            encoded
                .contains("afs_dfs_read_payload_seconds_count{result=\"ok\",transport=\"grpc\"} 1"),
            "{encoded}"
        );
        server.abort();
    }
}
