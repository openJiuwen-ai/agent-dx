use super::*;
use crate::config::HixlDfsReadMode;
use crate::node::rpc::hixl::test_support::{engine, native_error, negotiations};
use afs_protocol::node_data::{
    DfsChunkReadOp, DfsDigestAlgorithm, DfsHixlLane, DfsHixlReadRange, DfsHixlTransport,
    DfsReadGrant, DfsReadRangesCompletion,
};
use afs_transport::hixl::{EngineInfo, HixlErrorKind, HixlPhase, ReadTransport};

fn pack_windows(
    request: DfsReadRangesRequest,
    capacity: usize,
) -> afs_error::Result<Vec<ReadWindow>> {
    pack_windows_with_ops(request, capacity, crate::node::dfs_read::MAX_DFS_READ_OPS)
}

#[test]
fn prepare_refusals_fence_only_non_capacity_failures_and_never_quarantine() {
    for (code, fenced) in [
        (afs_error::IO_CAPACITY, false),
        (afs_error::NODE_TRANSFER_RESOURCE_EXHAUSTED, false),
        (afs_error::IO_UNAVAILABLE, true),
        (afs_error::NODE_TRANSFER_UNAVAILABLE, true),
        (afs_error::NODE_TRANSFER_INVALID, true),
        (afs_error::NODE_TRANSFER_CORRUPT_DATA, true),
    ] {
        let failure = WindowFailure::from_prepare(afs_error::Error::coded(code, "source refused"));
        assert_eq!(
            (failure.fence_peer, failure.quarantine_local),
            (fenced, false),
            "capacity shortfalls narrow the wave instead of ending the session: {}",
            failure.error
        );
    }
}

fn control_session() -> Arc<PeerSession> {
    Arc::new(PeerSession {
        reply: descriptor().1,
        caller_epoch: 1,
        client: DfsChunksClient::new(
            tonic::transport::Endpoint::from_static("http://127.0.0.1:1").connect_lazy(),
        ),
        poisoned: Arc::new(AtomicBool::new(false)),
    })
}

#[tokio::test]
async fn released_idle_session_is_retired_even_when_control_close_fails() {
    let mut slot = PeerSlot {
        session: Some(control_session()),
        touched: Instant::now() - Duration::from_secs(20),
        warming: None,
    };
    assert!(!retire_idle_session(&mut slot, Duration::from_millis(20)).await);
    assert!(
        slot.session.is_none(),
        "a fully released control session must not stick on CloseHixl failure"
    );
}

#[test]
fn session_reuse_never_outlives_the_source_session() {
    assert_eq!(session_idle_limit(1), Duration::from_millis(1));
    assert_eq!(session_idle_limit(20_000), SESSION_REUSE_LIMIT);
    assert_eq!(session_idle_limit(300_000), SESSION_REUSE_LIMIT);
}

#[tokio::test]
async fn unsupported_peers_do_not_consume_native_peer_budget() {
    let slots: PeerSlots = Arc::new(Mutex::new(HashMap::new()));
    for index in 0..MAX_HIXL_PEERS + 1 {
        let key = (format!("unsupported-{index}"), 1, "grpc".into());
        let slot = Arc::new(Mutex::new(PeerSlot {
            session: None,
            touched: Instant::now(),
            warming: None,
        }));
        {
            let mut map = slots.lock().await;
            assert!(
                map.len() < MAX_HIXL_PEERS,
                "unsupported peers exhausted native admission"
            );
            map.insert(key.clone(), slot.clone());
        }
        retire_unbound_peer(&slots, &key, &slot).await;
    }
    assert!(slots.lock().await.is_empty());
}

#[test]
fn definite_admission_refusal_does_not_become_unknown_dma() {
    let mut error = native_error(
        HixlPhase::SubmitRead,
        HixlErrorKind::ResourceExhausted,
        "owner queue full",
    );
    assert_eq!(
        failed_read_outcome(&error),
        DfsHixlReleaseOutcome::NotSubmitted
    );
    error.read_may_be_in_flight = true;
    assert_eq!(failed_read_outcome(&error), DfsHixlReleaseOutcome::Unknown);
}

fn request() -> DfsReadRangesRequest {
    DfsReadRangesRequest {
        read_id: "r".into(),
        attempt_id: "a".into(),
        file_version_id: "v".into(),
        layout_root_id: "l".into(),
        include_blake3_outboard: false,
        operations: vec![DfsChunkReadOp {
            operation_index: 0,
            chunk_id: "chunk".into(),
            chunk_offset: 2,
            length: 3,
            destination_offset: 5,
            source_copy_id: "copy".into(),
            grant: Some(DfsReadGrant {
                token: "grant".into(),
                ..Default::default()
            }),
        }],
    }
}

fn descriptor() -> (
    ReadWindow,
    DfsNegotiateHixlReply,
    DfsPrepareReadRangesHixlReply,
) {
    let mut request = request();
    request.operations[0].destination_offset = 0;
    let window = ReadWindow {
        request: request.clone(),
        mappings: vec![(5, 0..3)],
        length: 3,
    };
    let session = DfsNegotiateHixlReply {
        session_id: 1,
        source_node_id: "source".into(),
        source_node_epoch: 7,
        engine_id: "127.0.0.1:7920".into(),
        engine_generation: 9,
        window_bytes: 4,
        export_slots: 2,
        protocol_version: HIXL_PROTOCOL_VERSION,
        transport: DfsHixlTransport::Roce as i32,
        buffer_pool_count: 0,
        buffer_pool_size_mib: 0,
        hccs_domain_id: None,
        lanes: vec![DfsHixlLane {
            lane_id: 0,
            engine_id: "127.0.0.1:7920".into(),
            engine_generation: 9,
            window_bytes: 4,
            export_slots: 2,
        }],
    };
    let checksum = blake3::hash(b"abc").as_bytes().to_vec();
    let reply = DfsPrepareReadRangesHixlReply {
        session_id: 1,
        read_id: request.read_id,
        attempt_id: request.attempt_id,
        source_node_id: session.source_node_id.clone(),
        source_node_epoch: 7,
        engine_id: session.engine_id.clone(),
        engine_generation: 9,
        pool_generation: 9,
        slot_id: 0,
        slot_generation: 1,
        address: 4096,
        capacity: 4,
        packed_length: 3,
        ranges: vec![DfsHixlReadRange {
            operation_index: 0,
            chunk_id: "chunk".into(),
            chunk_offset: 2,
            length: 3,
            packed_offset: 0,
            source_copy_id: "copy".into(),
            range_checksum: checksum.clone(),
            range_checksum_algorithm: DfsDigestAlgorithm::Blake3 as i32,
            crc32c_table: Default::default(),
        }],
        completions: vec![DfsReadRangesCompletion {
            crc32c_table: Default::default(),
            read_id: "r".into(),
            attempt_id: "a".into(),
            operation_index: 0,
            source_copy_id: "copy".into(),
            transferred_bytes: 3,
            range_checksum: checksum,
            range_checksum_algorithm: DfsDigestAlgorithm::Blake3 as i32,
        }],
    };
    (window, session, reply)
}

fn engine_info(
    transport: ReadTransport,
    count: usize,
    size_mib: usize,
    hccs_domain_id: Option<u64>,
) -> EngineInfo {
    EngineInfo {
        engine_id: "client-engine".into(),
        generation: 1,
        window_bytes: 4,
        transport,
        buffer_pool_count: count,
        buffer_pool_size_mib: size_mib,
        hccs_domain_id,
    }
}

fn negotiated(
    transport: DfsHixlTransport,
    count: u32,
    size_mib: u32,
    hccs_domain_id: Option<u64>,
) -> DfsNegotiateHixlReply {
    DfsNegotiateHixlReply {
        transport: transport as i32,
        buffer_pool_count: count,
        buffer_pool_size_mib: size_mib,
        hccs_domain_id,
        ..descriptor().1
    }
}

#[test]
fn negotiation_accepts_matching_roce_and_hccs_parameters() {
    for (reply, local) in [
        (
            negotiated(DfsHixlTransport::Roce, 0, 0, None),
            engine_info(ReadTransport::Roce, 0, 0, None),
        ),
        (
            negotiated(DfsHixlTransport::HccsBufferPool, 4, 64, Some(0)),
            engine_info(ReadTransport::HccsBufferPool, 4, 64, Some(0)),
        ),
    ] {
        validate_negotiation(&reply, "source", 7, &local).unwrap();
    }
}

#[test]
fn negotiation_rejects_unknown_or_mismatched_transport_and_protocol() {
    use DfsHixlTransport::{HccsBufferPool, Roce};
    let roce = engine_info(ReadTransport::Roce, 0, 0, None);
    let hccs = engine_info(ReadTransport::HccsBufferPool, 4, 64, Some(0));
    let mut unknown = negotiated(Roce, 0, 0, None);
    unknown.transport = 99;
    let mut cases = vec![
        (unknown, roce.clone()),
        (negotiated(Roce, 1, 0, None), roce.clone()),
        (negotiated(Roce, 0, 0, Some(0)), roce.clone()),
        (
            negotiated(HccsBufferPool, 4, 64, Some(0)),
            engine_info(ReadTransport::HccsBufferPool, 8, 64, Some(0)),
        ),
        (negotiated(HccsBufferPool, 4, 32, Some(0)), hccs.clone()),
        (negotiated(HccsBufferPool, 0, 0, None), hccs.clone()),
        (negotiated(HccsBufferPool, 4, 64, None), hccs.clone()),
        (negotiated(HccsBufferPool, 4, 64, Some(7)), hccs),
    ];
    for version in [0, HIXL_PROTOCOL_VERSION + 1] {
        let mut mismatched = negotiated(Roce, 0, 0, None);
        mismatched.protocol_version = version;
        cases.push((mismatched, roce.clone()));
    }

    for (index, (bad_reply, local)) in cases.into_iter().enumerate() {
        let error = validate_negotiation(&bad_reply, "source", 7, &local)
            .expect_err("invalid HIXL transport negotiation");
        assert_eq!(
            error.code(),
            afs_error::CLIENT_PROTOCOL_VIOLATION,
            "case {index}"
        );
        assert!(
            !may_fallback(HixlDfsReadMode::Preferred, &error),
            "case {index}"
        );
    }
}

#[test]
fn scatter_preserves_overlap_order_and_validates_before_publishing() {
    let mut authority = request();
    authority.operations[0].length = 4;
    authority.operations[0].destination_offset = 0;
    authority.operations[0].chunk_id = "first".into();
    let mut second = authority.operations[0].clone();
    second.operation_index = 1;
    second.chunk_id = "last".into();
    authority.operations.push(second);
    let mut chunks = pack_windows(authority, 2)
        .unwrap()
        .into_iter()
        .map(|window| {
            let byte = if window.request.operations[0].chunk_id == "first" {
                b'a'
            } else {
                b'b'
            };
            PreparedChunk {
                bytes: vec![byte; window.length],
                windows: vec![window],
            }
        })
        .collect::<Vec<_>>();
    let mut output = [0; 4];
    scatter_prepared(
        chunks
            .iter()
            .map(|chunk| PreparedChunk {
                bytes: chunk.bytes.clone(),
                windows: chunk.windows.clone(),
            })
            .collect(),
        &mut output,
    )
    .unwrap();
    assert_eq!(&output, b"bbbb");
    chunks[0].windows[0].mappings[0].0 = usize::MAX;
    let mut unchanged = [9; 4];
    assert!(scatter_prepared(chunks, &mut unchanged).is_err());
    assert_eq!(unchanged, [9; 4]);
}

#[test]
fn only_uncertain_local_native_lifetimes_quarantine_the_local_group() {
    let mut native = native_error(
        HixlPhase::SubmitRead,
        HixlErrorKind::ResourceExhausted,
        "queue full",
    );
    assert!(!WindowFailure::from_native(native.clone(), true).quarantine_local);
    native.read_may_be_in_flight = true;
    assert!(WindowFailure::from_native(native, false).quarantine_local);
    assert!(WindowFailure::worker_failed("unmanaged lane".into()).quarantine_local);
}

#[test]
fn windows_fill_capacity_and_preserve_packed_identity() {
    let mut authority = request();
    authority.operations[0].length = 17;
    let batch = pack_read_batch(authority.clone()).unwrap();
    let windows = pack_windows(authority, 4).unwrap();
    assert_eq!(
        windows
            .iter()
            .map(|window| window.length)
            .collect::<Vec<_>>(),
        vec![4, 4, 4, 4, 1],
    );
    let mut offset = 0_u64;
    for window in windows {
        assert_eq!(
            crate::node::rpc::hixl::batch::slice_read_batch(
                &batch,
                offset,
                u64::try_from(window.length).unwrap(),
                &window.request.attempt_id
            )
            .unwrap(),
            window.request
        );
        offset += u64::try_from(window.length).unwrap();
    }
    assert_eq!(offset, 17);
}

#[test]
fn lane_negotiation_rejects_count_order_duplicate_engine_and_primary_mismatch() {
    let (_, reply, _) = descriptor();
    let local = engine_info(ReadTransport::Roce, 0, 0, None);
    validate_lanes(&reply, std::slice::from_ref(&local)).unwrap();
    let mut two = reply.clone();
    let mut second = two.lanes[0].clone();
    second.lane_id = 1;
    second.engine_id = "127.0.0.1:7921".into();
    two.lanes.push(second);
    validate_lanes(&two, &[local.clone(), local.clone()]).unwrap();
    assert!(validate_lanes(&two, std::slice::from_ref(&local)).is_err());
    let mut wrong_order = two.clone();
    wrong_order.lanes.swap(0, 1);
    assert!(validate_lanes(&wrong_order, &[local.clone(), local.clone()]).is_err());
    let mut duplicate = two;
    duplicate.lanes[1].engine_id = duplicate.lanes[0].engine_id.clone();
    assert!(validate_lanes(&duplicate, &[local.clone(), local.clone()]).is_err());
    duplicate.engine_id = "[::1]:7920".into();
    duplicate.lanes[0].engine_id = duplicate.engine_id.clone();
    duplicate.lanes[1].engine_id = "[0:0:0:0:0:0:0:1]:7920".into();
    assert!(validate_lanes(&duplicate, &[local.clone(), local.clone()]).is_err());
    let mut wrong_primary = reply;
    wrong_primary.lanes[0].engine_generation += 1;
    assert!(validate_lanes(&wrong_primary, &[local]).is_err());
}

#[test]
fn packing_splits_large_operation_and_preserves_grant_and_scatter() {
    let mut request = request();
    request.operations[0].length = 7;
    let windows = pack_windows(request, 4).unwrap();
    assert_eq!(windows.len(), 2);
    assert_eq!(
        windows.iter().map(|w| w.length).collect::<Vec<_>>(),
        vec![4, 3]
    );
    assert_eq!(windows[0].mappings, vec![(5, 0..4)]);
    assert_eq!(windows[1].mappings, vec![(9, 0..3)]);
    assert_eq!(windows[1].request.operations[0].chunk_offset, 6);
    assert_ne!(windows[0].request.attempt_id, windows[1].request.attempt_id);
    for window in windows {
        assert_eq!(
            window.request.operations[0].grant.as_ref().unwrap().token,
            "grant"
        );
    }
}

#[test]
fn packed_parent_slices_match_windows_and_scatter_to_original_destinations() {
    use crate::node::rpc::hixl::batch::slice_read_batch;

    let mut authority = request();
    authority.attempt_id = "parent-attempt".into();
    let template = authority.operations.remove(0);
    let operation = |index, chunk: &str, (chunk_offset, length, destination_offset), copy: &str| {
        let mut operation = template.clone();
        operation.operation_index = index;
        operation.chunk_id = chunk.into();
        operation.chunk_offset = chunk_offset;
        operation.length = length;
        operation.destination_offset = destination_offset;
        operation.source_copy_id = copy.into();
        operation.grant.as_mut().unwrap().token = format!("grant-{chunk}");
        operation
    };
    authority.operations = vec![
        operation(0, "chunk-a", (10, 5, 3), "copy-a"),
        operation(1, "chunk-b", (40, 4, 12), "copy-b"),
        operation(2, "chunk-a", (90, 3, 22), "copy-c"),
    ];

    let packed = pack_read_batch(authority.clone()).unwrap();
    assert_eq!(
        packed
            .operations
            .iter()
            .map(|operation| operation.destination_offset)
            .collect::<Vec<_>>(),
        vec![0, 5, 9]
    );

    let sources = std::collections::HashMap::from([
        ("chunk-a".to_owned(), (0..=127_u8).collect::<Vec<_>>()),
        ("chunk-b".to_owned(), (128..=255_u8).collect::<Vec<_>>()),
    ]);
    let mut expected = vec![0_u8; 25];
    for operation in &authority.operations {
        let source = sources
            .get(&operation.chunk_id)
            .expect("fixture source chunk exists");
        let destination = usize::try_from(operation.destination_offset).unwrap();
        let source_start = usize::try_from(operation.chunk_offset).unwrap();
        let length = usize::try_from(operation.length).unwrap();
        expected[destination..destination + length]
            .copy_from_slice(&source[source_start..source_start + length]);
    }

    for capacity in [1, 3, 4] {
        let windows = pack_windows(authority.clone(), capacity).unwrap();
        assert!(
            windows.len() > 1,
            "capacity {capacity} must split the batch"
        );
        let mut output = vec![0_u8; expected.len()];
        let mut batch_offset = 0_u64;

        for window in &windows {
            let window_length = u64::try_from(window.length).unwrap();
            let sliced = slice_read_batch(
                &packed,
                batch_offset,
                window_length,
                &window.request.attempt_id,
            )
            .unwrap();
            assert_eq!(
                sliced,
                window.request.clone(),
                "capacity {capacity}, batch offset {batch_offset}"
            );

            let mut window_bytes = vec![0_u8; window.length];
            for operation in &window.request.operations {
                let source = sources
                    .get(&operation.chunk_id)
                    .expect("fixture source chunk exists");
                let source_start = usize::try_from(operation.chunk_offset).unwrap();
                let packed_start = usize::try_from(operation.destination_offset).unwrap();
                let length = usize::try_from(operation.length).unwrap();
                window_bytes[packed_start..packed_start + length]
                    .copy_from_slice(&source[source_start..source_start + length]);
            }
            for (destination, packed_range) in &window.mappings {
                let end = destination.checked_add(packed_range.len()).unwrap();
                output[*destination..end].copy_from_slice(&window_bytes[packed_range.clone()]);
            }
            batch_offset = batch_offset.checked_add(window_length).unwrap();
        }

        assert_eq!(batch_offset, 12);
        assert_eq!(output, expected, "capacity {capacity}");
    }
}

#[test]
fn descriptor_rejects_stale_identity_overflow_or_bad_ranges_before_read() {
    let (window, session, reply) = descriptor();
    validate_prepare(&window, &session, &reply, 0).unwrap();
    let mut populated_range = reply.clone();
    populated_range.capacity = populated_range.packed_length;
    validate_prepare(&window, &session, &populated_range, 0).unwrap();
    for change in 0..9 {
        let mut bad = reply.clone();
        match change {
            0 => bad.engine_generation += 1,
            1 => bad.source_node_epoch += 1,
            2 => bad.slot_id = 2,
            3 => bad.address = u64::MAX,
            4 => bad.packed_length = 5,
            5 => bad.ranges[0].chunk_offset += 1,
            6 => bad.completions[0].source_copy_id = "wrong".into(),
            7 => bad.ranges[0].range_checksum.pop().map(|_| ()).unwrap(),
            _ => bad.slot_generation = 0,
        }
        assert!(
            validate_prepare(&window, &session, &bad, 0).is_err(),
            "mutation {change}"
        );
    }
}

#[test]
fn completed_native_read_still_requires_exact_bytes_and_checksum() {
    let (_, _, reply) = descriptor();
    validate_bytes(&reply, b"abc", None).unwrap();
    assert_eq!(
        validate_bytes(&reply, b"abd", None).unwrap_err().code(),
        afs_error::NODE_TRANSFER_CORRUPT_DATA
    );
    assert!(validate_bytes(&reply, b"ab", None).is_err());
}

#[test]
fn crc_window_binds_range_checksum_and_meta_descriptor() {
    let (_request, _session, template) = descriptor();
    let bytes = vec![7_u8; crate::node::chunk_crc32c::SEGMENT];
    let table = crate::node::chunk_crc32c::CrcTable::build(&bytes).unwrap();
    let mut expected = CrcLookup::new();
    expected.insert(
        "crc-chunk".to_owned(),
        CrcExpectation {
            content_digest: table.descriptor_digest(),
            length: bytes.len() as u64,
        },
    );
    let mut reply = template;
    reply.packed_length = bytes.len() as u64;
    reply.ranges = vec![DfsHixlReadRange {
        operation_index: 0,
        chunk_id: "crc-chunk".into(),
        chunk_offset: 0,
        length: bytes.len() as u64,
        packed_offset: 0,
        source_copy_id: "copy".into(),
        range_checksum: crate::node::chunk_crc32c::checksum(&bytes)
            .to_le_bytes()
            .to_vec(),
        range_checksum_algorithm: DfsDigestAlgorithm::Crc32c as i32,
        crc32c_table: serde_json::to_vec(&table).unwrap(),
    }];
    validate_bytes(&reply, &bytes, Some(&expected)).unwrap();

    let mut damaged = bytes.clone();
    damaged[5] ^= 1;
    assert_eq!(
        validate_bytes(&reply, &damaged, Some(&expected))
            .unwrap_err()
            .code(),
        afs_error::NODE_TRANSFER_CORRUPT_DATA
    );

    let mut foreign = reply.clone();
    let other = crate::node::chunk_crc32c::CrcTable::build(&vec![9_u8; bytes.len()]).unwrap();
    foreign.ranges[0].crc32c_table = serde_json::to_vec(&other).unwrap();
    assert!(
        validate_bytes(&foreign, &bytes, Some(&expected)).is_err(),
        "a table from another object must not bind"
    );

    assert!(
        validate_bytes(&reply, &bytes, None).is_err(),
        "a BLAKE3 batch must reject CRC32C ranges"
    );
}

#[test]
fn parallel_window_validation_preserves_order_and_reports_each_digest() {
    let (_, _, good) = descriptor();
    let mut bad = good.clone();
    bad.ranges[0].range_checksum = blake3::hash(b"other").as_bytes().to_vec();
    let windows = vec![
        (&good, b"abc".as_slice()),
        (&bad, b"abc".as_slice()),
        (&good, b"abc".as_slice()),
    ];
    let observed = validate_windows(&windows, None);
    assert_eq!(observed.len(), 3);
    assert!(observed[0].1.is_ok());
    assert_eq!(
        observed[1]
            .1
            .as_ref()
            .expect_err("mismatched digest must fail")
            .code(),
        afs_error::NODE_TRANSFER_CORRUPT_DATA
    );
    assert!(observed[2].1.is_ok());
}

#[test]
fn only_typed_pre_dispatch_capability_refusal_can_fallback() {
    let unsupported = afs_error::Error::coded(afs_error::NODE_TRANSFER_UNSUPPORTED, "unsupported");
    assert!(may_fallback(HixlDfsReadMode::Preferred, &unsupported));
    assert!(!may_fallback(HixlDfsReadMode::Required, &unsupported));
    let bare = afs_transport::grpc::error_status::status_to_error(tonic::Status::unimplemented(
        "arbitrary",
    ));
    assert!(!may_fallback(HixlDfsReadMode::Preferred, &bare));
    assert!(!may_fallback(
        HixlDfsReadMode::Preferred,
        &afs_error::Error::coded(afs_error::IO_TIMEOUT, "timeout")
    ));
}

#[test]
fn engine_address_cannot_be_reconnected_across_source_generations() {
    let mut engines = HashMap::new();
    let bind = |engines: &mut HashMap<SocketAddr, (String, u64, u64)>, engine, generation| {
        bind_engines(
            engines,
            [(engine, ("source".to_owned(), 7, generation))],
            MAX_HIXL_ENGINES,
        )
    };
    bind(&mut engines, "[::1]:7920", 9).unwrap();
    bind(&mut engines, "[0:0:0:0:0:0:0:1]:7920", 9).unwrap();
    assert_eq!(
        bind(&mut engines, "[0:0:0:0:0:0:0:1]:7920", 10)
            .unwrap_err()
            .code(),
        afs_error::NODE_TRANSFER_DRAIN_INCOMPLETE,
        "another textual form of the same endpoint is the same engine"
    );
    assert_eq!(engines.len(), 1);
    assert_eq!(engines[&"[::1]:7920".parse().unwrap()].2, 9);
}

#[test]
fn engine_binding_is_all_or_nothing() {
    let mut engines = HashMap::new();
    bind_engines(&mut engines, [("127.0.0.1:1", 1_u64)], 2).unwrap();
    let conflicting = bind_engines(
        &mut engines,
        [("127.0.0.1:2", 1), ("127.0.0.1:1", 2)],
        MAX_HIXL_ENGINES,
    );
    assert_eq!(
        conflicting.unwrap_err().code(),
        afs_error::NODE_TRANSFER_DRAIN_INCOMPLETE
    );
    let over_budget = bind_engines(&mut engines, [("127.0.0.1:2", 1), ("127.0.0.1:3", 1)], 2);
    assert_eq!(
        over_budget.unwrap_err().code(),
        afs_error::NODE_TRANSFER_RESOURCE_EXHAUSTED
    );
    assert_eq!(
        engines.len(),
        1,
        "a refused lane set must not leave its earlier lanes bound"
    );
}

#[tokio::test]
async fn outer_deadline_detaches_owned_cleanup_and_fences_the_peer() {
    let cancel = Arc::new(Cancellation::default());
    let peer = Arc::new(AtomicBool::new(false));
    cancel.publish(peer.clone()).unwrap();
    let (finish, finishing) = tokio::sync::oneshot::channel();
    let (returned, returning) = tokio::sync::oneshot::channel();
    let task = tokio::spawn(async move {
        let owned_buffer = vec![42_u8; 4];
        finishing.await.unwrap();
        returned.send(owned_buffer).unwrap();
        Ok(())
    });
    let error = wait_owned_task(
        task,
        Instant::now() + Duration::from_millis(10),
        cancel.clone(),
    )
    .await
    .unwrap_err();
    assert_eq!(error.code(), afs_error::CLIENT_DEADLINE_EXCEEDED);
    assert!(peer.load(Ordering::Acquire));
    assert!(cancel.cancelled.load(Ordering::Acquire));
    finish.send(()).unwrap();
    assert_eq!(returning.await.unwrap(), vec![42; 4]);
}

mod loopback_integration {
    use super::*;
    use crate::node::chunk::{ChunkStore, LocalChunkStore, StagedChunk};
    use crate::node::dfs_read::{ChunkReadOp, ResolvedReadOp};
    use crate::node::rpc::data::{
        DenyDfsReplicaAuthorizer, DfsChunkTransportResources, DfsReadAuthorizer, PeerAuthenticator,
        hixl as source_hixl, make_dfs_chunks_server_with_transport,
    };
    use afs_transport::hixl::{ExportWindow, HixlError};
    use tokio_stream::wrappers::TcpListenerStream;

    struct MemoryWindow {
        lane: usize,
        generation: u64,
        bytes: Vec<u8>,
    }

    #[derive(Default)]
    struct MemoryExports {
        next_address: u64,
        generations: [u64; 2],
        active: HashMap<u64, MemoryWindow>,
        reads: Vec<(usize, u64, usize)>,
    }

    #[derive(Default)]
    struct ConnectGate {
        closed: bool,
        entered: usize,
        completed: usize,
    }

    #[derive(Default)]
    struct ReadGate {
        closed: bool,
        entered: usize,
        /// Window count of each batch READ, in arrival order.
        windows: Vec<usize>,
    }

    #[derive(Default)]
    struct NativeBoundary {
        memory: StdMutex<MemoryExports>,
        prepare_calls: AtomicU64,
        batch_reads: AtomicU64,
        reader_quarantines: AtomicU64,
        source_quarantines: AtomicU64,
        connects: StdMutex<ConnectGate>,
        connects_changed: std::sync::Condvar,
        /// Holds native batch READs so a test can observe what overlaps them.
        reads: StdMutex<ReadGate>,
        reads_changed: std::sync::Condvar,
        /// The next native batch READ fails before submitting any window.
        fail_read: std::sync::atomic::AtomicBool,
    }

    impl NativeBoundary {
        fn close_connect_gate(&self) {
            self.connects.lock().unwrap().closed = true;
        }
        fn open_connect_gate(&self) {
            self.connects.lock().unwrap().closed = false;
            self.connects_changed.notify_all();
        }
        fn close_read_gate(&self) {
            self.reads.lock().unwrap().closed = true;
        }
        fn open_read_gate(&self) {
            self.reads.lock().unwrap().closed = false;
            self.reads_changed.notify_all();
        }
        fn fail_next_read(&self) {
            self.fail_read.store(true, Ordering::SeqCst);
        }
        async fn wait_read_entered(&self, count: usize) {
            for _ in 0..300 {
                if self.reads.lock().unwrap().entered >= count {
                    return;
                }
                tokio::time::sleep(Duration::from_millis(10)).await;
            }
            panic!("no native batch READ entered");
        }
        async fn wait_connect_entered(&self, count: usize) {
            for _ in 0..300 {
                if self.connects.lock().unwrap().entered >= count {
                    return;
                }
                tokio::time::sleep(Duration::from_millis(10)).await;
            }
            panic!("warm task did not enter connect");
        }
    }

    fn boundary_infos(
        first_port: u16,
        generation: u64,
        lanes: u16,
        window_bytes: usize,
    ) -> Vec<EngineInfo> {
        (0..lanes)
            .map(|offset| {
                engine(
                    &format!("127.0.0.1:{}", first_port + offset),
                    generation,
                    window_bytes,
                )
            })
            .collect()
    }

    struct SourceNative {
        boundary: Arc<NativeBoundary>,
        lanes: u16,
        window_bytes: usize,
        export_slots: usize,
    }

    impl source_hixl::ExportProvider for SourceNative {
        fn info(&self) -> EngineInfo {
            self.lane_infos().remove(0)
        }

        fn lane_infos(&self) -> Vec<EngineInfo> {
            boundary_infos(7920, 7, self.lanes, self.window_bytes)
        }

        fn export_slots(&self, _lane: usize) -> usize {
            self.export_slots
        }

        fn prepare_export_shared(
            &self,
            lane: usize,
            data: Arc<Vec<u8>>,
            offset: usize,
            length: usize,
            _deadline: Instant,
        ) -> Result<ExportWindow, HixlError> {
            let bytes = data[offset..offset + length].to_vec();
            if self.boundary.prepare_calls.fetch_add(1, Ordering::SeqCst) == 3 {
                return Err(native_error(
                    HixlPhase::PrepareExport,
                    HixlErrorKind::ResourceExhausted,
                    "one definite refusal in the second wave",
                ));
            }
            let mut memory = self.boundary.memory.lock().unwrap();
            let active = memory
                .active
                .values()
                .filter(|window| window.lane == lane)
                .count();
            if active >= self.export_slots {
                return Err(native_error(
                    HixlPhase::PrepareExport,
                    HixlErrorKind::ResourceExhausted,
                    "the lane export slots are still active",
                ));
            }
            memory.generations[lane] += 1;
            let generation = memory.generations[lane];
            memory.next_address += 0x100;
            let address = 0x1000 + memory.next_address;
            let length = bytes.len();
            memory.active.insert(
                address,
                MemoryWindow {
                    lane,
                    generation,
                    bytes,
                },
            );
            Ok(ExportWindow {
                slot_id: 0,
                generation,
                address,
                length,
            })
        }

        fn release_export(
            &self,
            lane: usize,
            slot_id: u64,
            generation: u64,
        ) -> Result<(), HixlError> {
            let mut memory = self.boundary.memory.lock().unwrap();
            let address = memory
                .active
                .iter()
                .find_map(|(address, window)| {
                    (slot_id == 0 && window.lane == lane && window.generation == generation)
                        .then_some(*address)
                })
                .ok_or_else(|| {
                    native_error(
                        HixlPhase::ReleaseExport,
                        HixlErrorKind::InvalidState,
                        "release did not identify the active lane generation",
                    )
                })?;
            memory.active.remove(&address);
            Ok(())
        }

        fn quarantine(&self) -> Result<(), HixlError> {
            self.boundary
                .source_quarantines
                .fetch_add(1, Ordering::SeqCst);
            Ok(())
        }
    }

    struct ReaderNative {
        boundary: Arc<NativeBoundary>,
        lanes: u16,
        window_bytes: usize,
    }

    impl ReadRuntime for ReaderNative {
        fn info(&self) -> EngineInfo {
            self.infos().remove(0)
        }

        fn infos(&self) -> Vec<EngineInfo> {
            boundary_infos(7930, 11, self.lanes, self.window_bytes)
        }

        fn lane_count(&self) -> usize {
            usize::from(self.lanes)
        }

        fn lane(&self, lane: usize) -> Option<Arc<dyn ReadLane>> {
            let infos = self.infos();
            infos.get(lane).map(|_| {
                Arc::new(ReaderLane {
                    boundary: self.boundary.clone(),
                    source_engine: format!("127.0.0.1:{}", 7920 + lane as u16),
                    lane,
                }) as Arc<dyn ReadLane>
            })
        }

        fn quarantine(&self) -> Result<(), HixlError> {
            self.boundary
                .reader_quarantines
                .fetch_add(1, Ordering::SeqCst);
            Ok(())
        }
    }

    struct ReaderLane {
        boundary: Arc<NativeBoundary>,
        source_engine: String,
        lane: usize,
    }

    impl ReaderLane {
        fn read_window(
            &self,
            peer: &str,
            address: u64,
            length: usize,
            deadline: Instant,
        ) -> Result<Vec<u8>, HixlError> {
            self.connect(peer, deadline)?;
            let mut memory = self.boundary.memory.lock().unwrap();
            let window = memory.active.get(&address).ok_or_else(|| {
                native_error(
                    HixlPhase::SubmitRead,
                    HixlErrorKind::InvalidState,
                    "READ has no live source export",
                )
            })?;
            if window.lane != self.lane || window.bytes.len() != length {
                return Err(native_error(
                    HixlPhase::SubmitRead,
                    HixlErrorKind::InvalidArgument,
                    "READ differs from source lane or length",
                ));
            }
            let bytes = window.bytes.clone();
            memory.reads.push((self.lane, address, length));
            Ok(bytes)
        }
    }

    impl ReadLane for ReaderLane {
        fn connect(&self, peer: &str, _deadline: Instant) -> Result<(), HixlError> {
            if peer != self.source_engine {
                return Err(native_error(
                    HixlPhase::Connect,
                    HixlErrorKind::InvalidArgument,
                    "wrong lane engine",
                ));
            }
            let mut gate = self.boundary.connects.lock().unwrap();
            gate.entered += 1;
            while gate.closed {
                gate = self.boundary.connects_changed.wait(gate).unwrap();
            }
            gate.completed += 1;
            Ok(())
        }

        fn prepare_read_batch_buffer(&self, length: usize) -> Result<Vec<u8>, HixlError> {
            if length == 0 {
                return Err(native_error(
                    HixlPhase::SubmitRead,
                    HixlErrorKind::InvalidArgument,
                    "empty read buffer",
                ));
            }
            let mut output = Vec::new();
            output.try_reserve_exact(length).map_err(|error| {
                native_error(
                    HixlPhase::SubmitRead,
                    HixlErrorKind::ResourceExhausted,
                    &format!("test read buffer allocation failed: {error}"),
                )
            })?;
            output.resize(length, 0);
            Ok(output)
        }

        fn read_batch_timed_into(
            &self,
            peer: &str,
            windows: &[(u64, usize)],
            mut output: Vec<u8>,
            deadline: Instant,
        ) -> Result<HixlReadOutcome, HixlError> {
            self.boundary.batch_reads.fetch_add(1, Ordering::SeqCst);
            {
                let mut gate = self.boundary.reads.lock().unwrap();
                gate.entered += 1;
                gate.windows.push(windows.len());
                while gate.closed {
                    gate = self.boundary.reads_changed.wait(gate).unwrap();
                }
            }
            if self.boundary.fail_read.swap(false, Ordering::SeqCst) {
                return Err(native_error(
                    HixlPhase::SubmitRead,
                    HixlErrorKind::InvalidArgument,
                    "injected batch READ refusal",
                ));
            }
            let mut offset = 0;
            for (address, length) in windows {
                let bytes = self.read_window(peer, *address, *length, deadline)?;
                let end = offset + length;
                output[offset..end].copy_from_slice(&bytes);
                offset = end;
            }
            if offset != output.len() {
                return Err(native_error(
                    HixlPhase::SubmitRead,
                    HixlErrorKind::InvalidArgument,
                    "batch READ output differs from its windows",
                ));
            }
            Ok(HixlReadOutcome {
                bytes: output,
                timing: afs_transport::hixl::HixlReadTiming {
                    owner_queue: Duration::from_micros(1),
                    sdk_transfer: Some(Duration::from_micros(2)),
                    output_copy: Some(Duration::from_micros(1)),
                },
            })
        }
    }

    struct TestAuthority;

    impl PeerAuthenticator for TestAuthority {
        fn authenticate(
            &self,
            _: &tonic::metadata::MetadataMap,
            _: Option<SocketAddr>,
            _: Option<&[u8]>,
        ) -> afs_error::Result<String> {
            Ok("reader".into())
        }
    }

    impl DfsReadAuthorizer for TestAuthority {
        fn authorize(&self, peer: &str, request: &DfsReadRangesRequest) -> afs_error::Result<()> {
            if peer != "reader"
                || request.operations.iter().any(|operation| {
                    operation
                        .grant
                        .as_ref()
                        .is_none_or(|grant| grant.token != "offline-grant")
                })
            {
                return Err(afs_error::Error::coded(
                    afs_error::NODE_TRANSFER_INVALID,
                    "offline grant rejected",
                ));
            }
            Ok(())
        }
    }

    /// One whole-chunk operation per chunk, written back to back from output
    /// offset 1.
    fn read_batch(
        chunks: Vec<(crate::dfs::ChunkId, Option<crate::dfs::ChunkObject>, u64)>,
        endpoint: &str,
    ) -> PeerReadBatch {
        let version = crate::dfs::FileVersionId::new("version");
        let layout = crate::dfs::LayoutRootId::new("layout");
        let mut output_offset = 1;
        let operations = chunks
            .into_iter()
            .map(|(chunk_id, chunk, length)| {
                let operation = ResolvedReadOp {
                    op: ChunkReadOp {
                        chunk_id: chunk_id.clone(),
                        chunk_offset: 0,
                        length,
                        output_offset,
                    },
                    source: crate::dfs::SourceCandidate {
                        copy_id: crate::dfs::CopyId::new("copy"),
                        chunk_id,
                        role: crate::dfs::CopyRole::DurableReplica,
                        state: crate::dfs::CopyState::Ready,
                        location: crate::dfs::CopyLocation::Node {
                            node_id: "source".into(),
                            node_epoch: 9,
                            device_id: "disk".into(),
                            device_epoch: 1,
                            catalog_revision: 1,
                        },
                        data_endpoint: Some(endpoint.into()),
                        load_hint: 0,
                        read_grant: crate::dfs::DfsReadGrant {
                            namespace_id: crate::dfs::NamespaceId::new("namespace"),
                            file_version_id: version.clone(),
                            layout_root_id: layout.clone(),
                            caller_node_id: "reader".into(),
                            caller_node_epoch: 1,
                            expires_at_unix_ms: u64::MAX,
                            fence: 1,
                            token: "offline-grant".into(),
                        },
                    },
                    chunk,
                };
                output_offset += usize::try_from(length).unwrap();
                operation
            })
            .collect();
        PeerReadBatch {
            file_version_id: version,
            layout_root_id: layout,
            operations,
        }
    }

    /// A loopback source and reader sharing one in-memory native boundary.
    struct Setup {
        lanes: u16,
        window_bytes: usize,
        export_slots: usize,
        mode: HixlDfsReadMode,
        source_supported: bool,
        group: Option<ReadGroupPolicy>,
        /// CRC32C chunks served instead of the single BLAKE3 `abcdefgh` chunk.
        crc32c_chunks: &'static [&'static str],
        /// Read CRC policy of both the source server and the reader's pool.
        read_crc_mode: crate::config::DfsReadCrcMode,
    }

    impl Setup {
        fn new(lanes: u16, window_bytes: usize, export_slots: usize) -> Self {
            Self {
                lanes,
                window_bytes,
                export_slots,
                mode: HixlDfsReadMode::Required,
                source_supported: true,
                group: None,
                crc32c_chunks: &[],
                read_crc_mode: crate::config::DfsReadCrcMode::Both,
            }
        }

        async fn start(self) -> Harness {
            let temp = tempfile::tempdir().unwrap();
            let local = Arc::new(
                LocalChunkStore::open(temp.path(), "source")
                    .unwrap()
                    .with_peer_read_crc_mode(self.read_crc_mode),
            );
            let chunks = if self.crc32c_chunks.is_empty() {
                let staged = StagedChunk::new(
                    crate::dfs::OperationId::new("offline-read"),
                    b"abcdefgh".to_vec(),
                );
                let chunk_id = staged.chunk.id.clone();
                local.put(staged).unwrap();
                vec![(chunk_id, None, 8)]
            } else {
                self.crc32c_chunks
                    .iter()
                    .enumerate()
                    .map(|(index, text)| {
                        let staged = StagedChunk::new_crc32c(
                            crate::dfs::OperationId::new(format!("crc-{index}")),
                            text.as_bytes().to_vec(),
                        )
                        .unwrap();
                        let chunk = staged.chunk.clone();
                        local.put(staged).unwrap();
                        (chunk.id.clone(), Some(chunk), text.len() as u64)
                    })
                    .collect()
            };
            let native = Arc::new(NativeBoundary::default());
            let source_metrics = afs_metrics::Registry::new();
            let source = Arc::new(
                source_hixl::HixlReadService::from_provider(
                    Arc::new(SourceNative {
                        boundary: native.clone(),
                        lanes: self.lanes,
                        window_bytes: self.window_bytes,
                        export_slots: self.export_slots,
                    }),
                    "source".into(),
                    9,
                )
                .with_metrics(HixlMetrics::register(&source_metrics).unwrap()),
            );
            let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
            let endpoint = format!("http://{}", listener.local_addr().unwrap());
            let service = make_dfs_chunks_server_with_transport(
                Some(local.clone()),
                Arc::new(TestAuthority),
                Arc::new(TestAuthority),
                Arc::new(DenyDfsReplicaAuthorizer),
                None,
                Duration::from_secs(5),
                DfsChunkTransportResources {
                    read_pipeline: Default::default(),
                    rdma_sessions: crate::node::rpc::control::RdmaSessionRegistry::new(None),
                    payload_metrics: None,
                    hixl: self.source_supported.then(|| source.clone()),
                },
            );
            let (stop, stopping) = tokio::sync::oneshot::channel();
            let server = tokio::spawn(async move {
                tonic::transport::Server::builder()
                    .add_service(service)
                    .serve_with_incoming_shutdown(TcpListenerStream::new(listener), async {
                        let _ = stopping.await;
                    })
                    .await
                    .unwrap();
            });
            let peers = Arc::new(
                PeerConnectionPool::new(
                    afs_transport::GrpcConfig::default(),
                    afs_transport::TlsConfig::Disabled,
                    16,
                )
                .unwrap()
                .with_read_crc_mode(self.read_crc_mode),
            );
            let mut reader = HixlChunkTransfer::from_runtime(
                peers,
                Arc::new(ReaderNative {
                    boundary: native.clone(),
                    lanes: self.lanes,
                    window_bytes: self.window_bytes,
                }),
                self.mode,
                Duration::from_secs(5),
            )
            .unwrap()
            .with_receive_slots(self.export_slots);
            if let Some(group) = self.group {
                reader = reader.with_read_group(group);
            }
            Harness {
                _temp: temp,
                local,
                native,
                source,
                source_metrics,
                reader: Arc::new(reader),
                batch: read_batch(chunks, &endpoint),
                endpoint,
                stop,
                server,
            }
        }
    }

    struct Harness {
        _temp: tempfile::TempDir,
        local: Arc<LocalChunkStore>,
        native: Arc<NativeBoundary>,
        source: Arc<source_hixl::HixlReadService>,
        source_metrics: afs_metrics::Registry,
        reader: Arc<HixlChunkTransfer>,
        batch: PeerReadBatch,
        endpoint: String,
        stop: tokio::sync::oneshot::Sender<()>,
        server: tokio::task::JoinHandle<()>,
    }

    impl Harness {
        /// Reads the batch on a blocking thread into `length` bytes of `!`.
        fn spawn_read(&self, length: usize) -> tokio::task::JoinHandle<afs_error::Result<Vec<u8>>> {
            let reader = self.reader.clone();
            let batch = self.batch.clone();
            tokio::task::spawn_blocking(move || {
                let mut output = vec![b'!'; length];
                reader.read_ranges(&batch, &mut output)?;
                Ok(output)
            })
        }

        async fn read(&self, length: usize) -> afs_error::Result<Vec<u8>> {
            self.spawn_read(length).await.unwrap()
        }

        /// The (node id, node epoch, data endpoint) key of the batch source.
        fn peer(&self) -> (String, u64, String) {
            let source = &self.batch.operations[0].source;
            let crate::dfs::CopyLocation::Node {
                node_id,
                node_epoch,
                ..
            } = &source.location
            else {
                panic!("loopback source must be a node copy");
            };
            let address = source.node_data_endpoint().unwrap().to_owned();
            (node_id.clone(), *node_epoch, address)
        }

        fn sessions(&self) -> Vec<source_hixl::test_support::SessionState> {
            source_hixl::test_support::sessions(&self.source)
        }

        /// Negotiations the source answered, admitted or refused.
        fn negotiations(&self) -> u64 {
            negotiations(&self.source_metrics)
        }

        /// Waits until no detached warm task is connecting the batch source.
        async fn settle_warm(&self) {
            let (node_id, node_epoch, address) = self.peer();
            let deadline = Instant::now() + Duration::from_secs(5);
            self.reader
                .context
                .settle_warm(&node_id, node_epoch, &address, deadline)
                .await
                .unwrap();
        }

        async fn wait_warm_session(&self) {
            let key = self.peer();
            for _ in 0..300 {
                let slot = self.reader.context.slots.lock().await.get(&key).cloned();
                if let Some(slot) = slot {
                    let guard = slot.lock().await;
                    if guard.session.is_some() && guard.warming.is_none() {
                        return;
                    }
                }
                tokio::time::sleep(Duration::from_millis(10)).await;
            }
            panic!("warm session was not published");
        }

        async fn shutdown(self) {
            self.stop.send(()).unwrap();
            tokio::time::timeout(Duration::from_secs(1), self.server)
                .await
                .unwrap()
                .unwrap();
        }
    }

    async fn harness() -> Harness {
        Setup::new(2, 2, 1).start().await
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn small_batches_keep_one_window_on_the_bound_lane() {
        let mut h = harness().await;
        h.batch.operations[0].op.length = 2;
        for expected_lanes in [vec![0], vec![0, 0], vec![0, 0, 0]] {
            assert_eq!(h.read(4).await.unwrap(), b"!ab!");
            let memory = h.native.memory.lock().unwrap();
            assert_eq!(
                memory.reads.iter().map(|read| read.0).collect::<Vec<_>>(),
                expected_lanes,
                "one small batch uses one window on the peer's bound lane"
            );
            assert!(memory.reads.iter().all(|read| read.2 == 2));
            assert!(memory.active.is_empty());
            assert_eq!(
                h.native.batch_reads.load(Ordering::SeqCst),
                expected_lanes.len() as u64,
                "a read outside group mode still submits one native batch"
            );
            assert_eq!(
                h.native.prepare_calls.load(Ordering::SeqCst),
                expected_lanes.len() as u64,
                "a read outside group mode still prepares through the group RPC"
            );
            drop(memory);
            let sessions = h.sessions();
            assert_eq!(sessions.len(), 1, "one peer keeps one control session");
            assert_eq!(sessions[0].prepared_bytes, 0);
            assert_eq!(sessions[0].active_windows, 0);
        }
        h.shutdown().await;
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn peers_spread_over_lanes_before_sharing_one() {
        let h = harness().await;
        let context = &h.reader.context;
        let first = ("peer-a".to_owned(), 9, "grpc-a".to_owned());
        let second = ("peer-b".to_owned(), 9, "grpc-b".to_owned());
        let third = ("peer-c".to_owned(), 9, "grpc-c".to_owned());
        assert_eq!(context.bind_lane(&first), 0);
        assert_eq!(context.bind_lane(&second), 1);
        assert_eq!(context.bind_lane(&first), 0, "a bound peer keeps its lane");
        assert_eq!(
            context.bind_lane(&third),
            0,
            "a peer without a free lane shares the least-used one"
        );
        h.shutdown().await;
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn unsupported_fallback_preserves_the_group_budget() {
        let mut h = Setup {
            mode: HixlDfsReadMode::Preferred,
            source_supported: false,
            group: Some(ReadGroupPolicy {
                bytes: 1024,
                max_inflight: 1,
                max_descriptors: crate::node::dfs_read::MAX_DFS_GROUP_OPS,
                host_budget_bytes: 2048,
            }),
            ..Setup::new(2, 2, 1)
        }
        .start()
        .await;
        let template = h.batch.operations[0].clone();
        h.batch.operations = (0..=crate::node::dfs_read::MAX_DFS_READ_OPS)
            .map(|index| {
                let mut operation = template.clone();
                operation.op.length = 1;
                operation.op.output_offset = index;
                operation
            })
            .collect();
        let expected = vec![b'a'; h.batch.operations.len()];

        let reader = h.reader.clone();
        let batch = h.batch.clone();
        let output = tokio::task::spawn_blocking(move || {
            let mut output = vec![b'!'; batch.operations.len()];
            reader
                .grpc_fallback(reader.context.timeout)?
                .read_ranges(&batch, &mut output)?;
            Ok::<_, afs_error::Error>(output)
        })
        .await
        .unwrap()
        .unwrap();
        assert_eq!(output, expected);
        assert!(h.native.memory.lock().unwrap().reads.is_empty());
        h.shutdown().await;
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn preferred_cold_batch_uses_grpc_while_the_session_warms() {
        let mut h = Setup {
            mode: HixlDfsReadMode::Preferred,
            ..Setup::new(2, 2, 1)
        }
        .start()
        .await;
        h.batch.operations[0].op.length = 2;
        h.native.close_connect_gate();
        let first = tokio::time::timeout(Duration::from_secs(2), h.spawn_read(4))
            .await
            .expect("cold gRPC fallback must not wait for the HIXL connect")
            .unwrap()
            .unwrap();
        assert_eq!(first, b"!ab!");
        h.native.wait_connect_entered(1).await;
        assert_eq!(
            h.native.connects.lock().unwrap().completed,
            0,
            "the first batch must finish while the native connect is still blocked"
        );
        assert!(
            h.native.memory.lock().unwrap().reads.is_empty(),
            "the cold batch must not use a native HIXL read"
        );

        h.native.open_connect_gate();
        h.wait_warm_session().await;
        assert_eq!(h.read(4).await.unwrap(), b"!ab!");
        assert_eq!(
            h.native.memory.lock().unwrap().reads.len(),
            1,
            "the warmed session must serve the next batch natively"
        );
        assert_eq!(
            h.sessions().len(),
            1,
            "warming must negotiate exactly one control session"
        );
        h.shutdown().await;
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn refused_source_keeps_preferred_reads_on_grpc_without_renegotiating() {
        let mut h = Setup {
            mode: HixlDfsReadMode::Preferred,
            ..Setup::new(2, 2, 1)
        }
        .start()
        .await;
        h.batch.operations[0].op.length = 2;
        // An idle drain stops source admission without pinning a window.
        h.source.drain(Duration::ZERO).await.unwrap();
        for _ in 0..3 {
            assert_eq!(h.read(4).await.unwrap(), b"!ab!");
            h.settle_warm().await;
        }
        assert_eq!(
            h.negotiations(),
            1,
            "a refused source must not be renegotiated on every read"
        );
        assert!(h.native.memory.lock().unwrap().reads.is_empty());
        h.shutdown().await;
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn required_mode_reports_a_refused_source_without_remembering_it() {
        let mut h = harness().await;
        h.batch.operations[0].op.length = 2;
        h.source.drain(Duration::ZERO).await.unwrap();
        let error = h.read(4).await.unwrap_err();
        assert_eq!(error.code(), afs_error::NODE_TRANSFER_UNAVAILABLE);
        assert!(!h.reader.context.is_refused(&h.peer()));
        h.shutdown().await;
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn refusal_table_is_bounded_and_expires() {
        let h = harness().await;
        let context = &h.reader.context;
        let key = |index: usize| (format!("node-{index}"), 1, "http://peer".to_owned());
        let until = || Instant::now().checked_add(REFUSED_RETRY);
        for index in 0..=MAX_HIXL_PEERS {
            context.set_refused(key(index), until());
        }
        assert!(context.is_refused(&key(0)));
        assert!(!context.is_refused(&key(MAX_HIXL_PEERS)));
        context.set_refused(key(0), None);
        assert!(!context.is_refused(&key(0)));
        context.set_refused(key(1), Some(Instant::now()));
        assert!(!context.is_refused(&key(1)), "an elapsed refusal expires");
        h.shutdown().await;
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn concurrent_peer_reads_queue_instead_of_failing() {
        let mut h = harness().await;
        h.batch.operations[0].op.length = 2;
        // The first read holds the peer admission while its native connect
        // is blocked; a second read must queue for it instead of reporting
        // an exhausted peer.
        h.native.close_connect_gate();
        let first = h.spawn_read(3);
        h.native.wait_connect_entered(1).await;
        tokio::time::sleep(Duration::from_millis(50)).await;
        let second = h.spawn_read(3);
        tokio::time::sleep(Duration::from_millis(50)).await;
        h.native.open_connect_gate();
        assert_eq!(&first.await.unwrap().unwrap()[1..], b"ab");
        assert_eq!(&second.await.unwrap().unwrap()[1..], b"ab");
        h.shutdown().await;
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn crc32c_chunks_travel_through_native_windows() {
        // Two CRC32C chunks share one window: each operation carries its
        // own Meta-bound descriptor, exactly like a BLAKE3 batch.
        let h = Setup {
            crc32c_chunks: &["abcdefgh", "ijklmnop"],
            ..Setup::new(1, 16, 4)
        }
        .start()
        .await;
        let output = h.read(17).await.unwrap();
        assert_eq!(&output[1..9], b"abcdefgh");
        assert_eq!(&output[9..17], b"ijklmnop");
        assert_eq!(
            h.native.prepare_calls.load(Ordering::SeqCst),
            1,
            "both CRC32C ranges must share one native window"
        );
        h.shutdown().await;
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn crc32c_group_wave_is_one_prepare_while_the_peer_budget_is_held() {
        let h = Setup {
            group: Some(ReadGroupPolicy {
                bytes: 16,
                max_inflight: 1,
                max_descriptors: 8,
                host_budget_bytes: 1024 * 1024,
            }),
            crc32c_chunks: &["abcdefgh", "ijkl"],
            ..Setup::new(1, 4, 8)
        }
        .start()
        .await;
        // Concurrent peer reads hold the whole shared payload budget. The
        // group snapshot must not need any of it for CRC32C ranges.
        let held = h
            .local
            .reserve_peer_read_payload(crate::node::chunk::PEER_READ_PAYLOAD_LIMIT)
            .unwrap();
        let output = h.read(13).await.unwrap();
        assert_eq!(&output[1..9], b"abcdefgh");
        assert_eq!(&output[9..13], b"ijkl");
        // Three 4-byte windows: one group Prepare exports all of them and
        // one native batch reads them, with no per-window retry.
        assert_eq!(h.native.prepare_calls.load(Ordering::SeqCst), 3);
        assert_eq!(h.native.batch_reads.load(Ordering::SeqCst), 1);
        let sessions = h.sessions();
        assert_eq!(sessions.len(), 1);
        assert_eq!(sessions[0].active_windows, 0);
        assert_eq!(sessions[0].prepared_bytes, 0);
        drop(held);
        h.shutdown().await;
    }

    /// Reads `h.batch` while the HIXL connect is held, so a preferred-mode
    /// reader must serve it through its cold gRPC fallback. The gate reopens
    /// before any check so a failing test cannot strand the warm task.
    async fn cold_read(h: &Harness, length: usize) -> afs_error::Result<Vec<u8>> {
        h.native.close_connect_gate();
        let result = tokio::time::timeout(Duration::from_secs(2), h.spawn_read(length)).await;
        let native_reads = h.native.memory.lock().unwrap().reads.len();
        h.native.open_connect_gate();
        let result = result
            .expect("cold gRPC fallback must not wait for the HIXL connect")
            .unwrap();
        assert_eq!(
            native_reads, 0,
            "the cold batch must not use a native HIXL read"
        );
        result
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn preferred_cold_crc32c_batch_reads_each_range_through_the_crc_adapter() {
        // HIXL batches CRC32C ranges, so the engine hands them over through
        // `read_ranges` with each object on its operation. The cold fallback
        // must still verify them with the CRC adapter, one range per request.
        for chunks in [&["abcdefgh"][..], &["abcdefgh", "ijklmnop"][..]] {
            let h = Setup {
                mode: HixlDfsReadMode::Preferred,
                crc32c_chunks: chunks,
                ..Setup::new(1, 16, 4)
            }
            .start()
            .await;
            let expected = chunks.concat();
            let output = cold_read(&h, expected.len() + 1).await.unwrap();
            assert_eq!(&output[1..], expected.as_bytes());
            assert_eq!(output[0], b'!');

            h.wait_warm_session().await;
            let output = h.read(expected.len() + 1).await.unwrap();
            assert_eq!(&output[1..], expected.as_bytes());
            assert_eq!(h.native.batch_reads.load(Ordering::SeqCst), 1);
            h.shutdown().await;
        }
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn preferred_cold_mixed_batch_keeps_blake3_and_crc32c_ranges_apart() {
        let mut h = Setup {
            mode: HixlDfsReadMode::Preferred,
            crc32c_chunks: &["abcdefgh"],
            ..Setup::new(1, 16, 4)
        }
        .start()
        .await;
        let staged = StagedChunk::new(
            crate::dfs::OperationId::new("blake3-neighbour"),
            b"qrstuvwx".to_vec(),
        );
        let blake3_id = staged.chunk.id.clone();
        h.local.put(staged).unwrap();
        let crc = h.batch.operations[0].chunk.clone().unwrap();
        h.batch = read_batch(
            vec![(crc.id.clone(), Some(crc), 8), (blake3_id, None, 4)],
            &h.endpoint,
        );
        assert_eq!(cold_read(&h, 13).await.unwrap(), b"!abcdefghqrst");

        h.wait_warm_session().await;
        assert_eq!(h.read(13).await.unwrap(), b"!abcdefghqrst");
        h.shutdown().await;
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn blake3_cold_fallback_follows_the_plain_grpc_read_crc_policy() {
        // Native DFS gRPC defaults the read CRC policy to `off`. The plain
        // BLAKE3 stream sends no policy metadata, which the source reads as
        // the legacy `both` and refuses. The cold fallback is that same plain
        // stream, so it fails exactly like a reader with HIXL off, while the
        // native window path is unaffected by the gRPC read policy.
        let mut h = Setup {
            mode: HixlDfsReadMode::Preferred,
            read_crc_mode: crate::config::DfsReadCrcMode::Off,
            ..Setup::new(2, 2, 1)
        }
        .start()
        .await;
        h.batch.operations[0].op.length = 2;
        let cold = cold_read(&h, 4).await.unwrap_err();
        assert_eq!(cold.kind(), afs_error::ErrorKind::FailedPrecondition);
        assert!(cold.to_string().contains("read CRC mode differs"), "{cold}");

        let reader = h.reader.clone();
        let batch = h.batch.clone();
        let plain = tokio::task::spawn_blocking(move || {
            reader
                .grpc_fallback(reader.context.timeout)?
                .read_ranges(&batch, &mut [0; 4])
        })
        .await
        .unwrap()
        .unwrap_err();
        assert_eq!(plain.code(), cold.code());

        h.wait_warm_session().await;
        assert_eq!(h.read(4).await.unwrap(), b"!ab!");
        h.shutdown().await;
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn prewarm_connects_the_session_before_the_first_read() {
        let mut h = harness().await;
        h.batch.operations[0].op.length = 2;
        let reader = h.reader.clone();
        let endpoint = h.endpoint.clone();
        let connected =
            tokio::task::spawn_blocking(move || reader.prewarm("source", 9, &endpoint, 1))
                .await
                .unwrap();
        assert!(connected, "pre-warm must publish a session");
        assert_eq!(
            h.native.connects.lock().unwrap().completed,
            2,
            "pre-warm connects every lane once"
        );

        assert_eq!(&h.read(3).await.unwrap()[1..], b"ab");
        // The reader-lane mock counts every native READ as one connect, so a
        // read that re-established the session would add the two lane
        // connects on top of this one.
        assert_eq!(
            h.native.connects.lock().unwrap().completed,
            3,
            "the first read reuses the pre-warmed session"
        );
        h.shutdown().await;
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn queued_admission_past_the_deadline_reports_a_deadline_error() {
        let mut h = harness().await;
        h.batch.operations[0].op.length = 2;
        h.native.close_connect_gate();
        let first = h.spawn_read(3);
        h.native.wait_connect_entered(1).await;
        let deadline = Instant::now() + Duration::from_millis(100);
        let acquired = h
            .reader
            .context
            .acquire(
                "source",
                9,
                &h.endpoint,
                &DfsReadRangesRequest::default(),
                deadline,
            )
            .await;
        match acquired {
            Err(error) => {
                assert_eq!(error.code(), afs_error::CLIENT_DEADLINE_EXCEEDED);
            }
            Ok(_) => panic!("a queued read past its deadline must not be admitted"),
        }
        h.native.open_connect_gate();
        assert_eq!(&first.await.unwrap().unwrap()[1..], b"ab");
        h.shutdown().await;
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn settling_a_warm_slot_returns_when_the_task_clears_the_flag() {
        let h = harness().await;
        let context = h.reader.context.clone();
        let key = h.peer();
        let slot = Arc::new(Mutex::new(PeerSlot {
            session: None,
            touched: Instant::now(),
            warming: None,
        }));
        context.slots.lock().await.insert(key.clone(), slot.clone());
        let (sender, _) = watch::channel(false);
        slot.lock().await.warming = Some(sender);
        let release = tokio::spawn(async move {
            tokio::time::sleep(Duration::from_millis(50)).await;
            slot.lock().await.warming = None;
        });
        let deadline = Instant::now() + Duration::from_secs(2);
        let (node_id, node_epoch, address) = key;
        tokio::time::timeout(
            Duration::from_secs(1),
            context.settle_warm(&node_id, node_epoch, &address, deadline),
        )
        .await
        .expect("settle must observe the cleared warm flag")
        .unwrap();
        release.await.unwrap();
        h.shutdown().await;
    }

    fn expect_window<T>(result: Result<T, WindowFailure>, context: &str) -> T {
        match result {
            Ok(value) => value,
            Err(failure) => panic!("{context}: {}", failure.error),
        }
    }

    fn read_authority(batch: &PeerReadBatch) -> DfsReadRangesRequest {
        DfsReadRangesRequest {
            read_id: "group-read".into(),
            attempt_id: "group-attempt".into(),
            file_version_id: batch.file_version_id.0.clone(),
            layout_root_id: batch.layout_root_id.0.clone(),
            include_blake3_outboard: false,
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
        }
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn baseline_group_aggregates_one_native_batch() {
        let h = Setup {
            group: Some(ReadGroupPolicy {
                bytes: 8,
                max_inflight: 1,
                max_descriptors: 8,
                host_budget_bytes: 1024 * 1024,
            }),
            ..Setup::new(1, 4, 8)
        }
        .start()
        .await;
        let authority = pack_read_batch(read_authority(&h.batch)).unwrap();
        let deadline = Instant::now() + Duration::from_secs(5);
        let (node_id, node_epoch, address) = h.peer();
        // A grouped wave must still submit one Prepare round trip and one
        // native batch instead of one SDK read per window.
        let result = h
            .reader
            .context
            .read_batch(
                (node_id, node_epoch),
                address,
                authority,
                8,
                deadline,
                None,
                Arc::new(Cancellation::default()),
            )
            .await
            .unwrap();
        let mut output = vec![0_u8; 8];
        match result {
            ReadResult::Complete(chunks) => {
                scatter_prepared(chunks, &mut output).unwrap();
            }
            _ => panic!("grouped baseline read must use the native batch path"),
        }
        assert_eq!(output, b"abcdefgh");
        assert_eq!(h.native.batch_reads.load(Ordering::SeqCst), 1);
        let sessions = h.sessions();
        assert_eq!(sessions.len(), 1);
        assert_eq!(sessions[0].active_windows, 0);
        assert!(h.native.memory.lock().unwrap().active.is_empty());
        h.shutdown().await;
    }

    /// Group policy whose 4-byte windows split the 8-byte chunk into two waves
    /// when the source exposes two export slots.
    fn two_wave_setup() -> Setup {
        Setup {
            group: Some(ReadGroupPolicy {
                bytes: 8,
                max_inflight: 1,
                max_descriptors: 8,
                host_budget_bytes: 1024 * 1024,
            }),
            ..Setup::new(1, 4, 2)
        }
    }

    async fn wait_for_prepares(h: &Harness, count: u64) -> bool {
        for _ in 0..200 {
            if h.native.prepare_calls.load(Ordering::SeqCst) >= count {
                return true;
            }
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
        false
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn group_waves_prepare_the_next_wave_while_the_previous_one_reads() {
        // Each wave may use half of the source export slots, so the export of
        // wave k+1 overlaps the native read of wave k instead of following it.
        let h = two_wave_setup().start().await;
        h.native.close_read_gate();
        let read = h.spawn_read(9);
        h.native.wait_read_entered(1).await;
        let overlapped = wait_for_prepares(&h, 2).await;
        let first_read = h.native.reads.lock().unwrap().windows.first().copied();
        h.native.open_read_gate();
        let output = read.await.unwrap().unwrap();
        assert_eq!(first_read, Some(1), "each wave carries one window");
        assert!(
            overlapped,
            "the second wave must be exported while the first one reads"
        );
        assert_eq!(&output[1..9], b"abcdefgh");
        assert_eq!(h.native.prepare_calls.load(Ordering::SeqCst), 2);
        assert_eq!(h.native.batch_reads.load(Ordering::SeqCst), 2);
        let sessions = h.sessions();
        assert_eq!(sessions.len(), 1);
        assert_eq!(sessions[0].active_windows, 0);
        assert_eq!(sessions[0].prepared_bytes, 0);
        assert!(h.native.memory.lock().unwrap().active.is_empty());
        h.shutdown().await;
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn failed_wave_read_releases_the_prepared_next_wave() {
        // A failed read of wave k must not strand the export of wave k+1, which
        // was prepared alongside it and never reached a native READ.
        let h = two_wave_setup().start().await;
        h.native.close_read_gate();
        h.native.fail_next_read();
        let read = h.spawn_read(9);
        h.native.wait_read_entered(1).await;
        assert!(wait_for_prepares(&h, 2).await);
        h.native.open_read_gate();
        assert!(read.await.unwrap().is_err());
        assert_eq!(h.native.batch_reads.load(Ordering::SeqCst), 1);
        {
            let memory = h.native.memory.lock().unwrap();
            assert!(
                memory.reads.is_empty(),
                "the injected failure precedes every window READ"
            );
            assert!(
                memory.active.is_empty(),
                "neither the failed wave nor the unread next wave may stay exported"
            );
        }
        h.shutdown().await;
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn group_prepare_admits_all_windows_from_one_snapshot() {
        let h = Setup::new(1, 4, 8).start().await;
        let authority = read_authority(&h.batch);
        let context = h.reader.context.clone();
        let deadline = Instant::now() + Duration::from_secs(5);
        let (node_id, node_epoch, address) = h.peer();
        let lease = match context
            .acquire(&node_id, node_epoch, &address, &authority, deadline)
            .await
            .unwrap()
        {
            Acquired::Lease(lease) => lease,
            Acquired::Unsupported => panic!("loopback HIXL must be supported"),
        };
        let session = lease.session.clone();
        let packed = pack_read_batch(authority).unwrap();
        let mut offset = 0_u64;
        let planned = pack_windows(packed.clone(), 4)
            .unwrap()
            .into_iter()
            .map(|window| {
                let start = offset;
                offset += u64::try_from(window.length).unwrap();
                (window, start, 0_usize)
            })
            .collect::<Vec<_>>();
        let prepared = expect_window(
            context
                .prepare_group_windows(&session, &planned, &packed, deadline)
                .await,
            "one group prepare admits every window",
        );
        assert_eq!(prepared.len(), 2);
        let sessions = h.sessions();
        assert_eq!(sessions.len(), 1);
        assert_eq!(sessions[0].active_windows, 2);
        assert_eq!(
            sessions[0].prepared_bytes, 8,
            "both windows share one verified parent snapshot"
        );
        let mut output = vec![0_u8; 8];
        let chunks = expect_window(
            context
                .read_prepared_group(&session, prepared, output.len(), deadline, None)
                .await,
            "group native read",
        );
        scatter_prepared(chunks, &mut output).unwrap();
        assert_eq!(output, b"abcdefgh");
        assert_eq!(h.native.batch_reads.load(Ordering::SeqCst), 1);
        let sessions = h.sessions();
        assert_eq!(sessions[0].active_windows, 0);
        assert_eq!(sessions[0].prepared_bytes, 0);
        assert!(h.native.memory.lock().unwrap().active.is_empty());
        drop(lease);
        h.shutdown().await;
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn later_wave_prepare_refusal_closes_snapshot_and_allows_fresh_session() {
        let h = Setup::new(2, 2, 2).start().await;
        let reader = h.reader.clone();
        let batch = h.batch.clone();
        let (failed, output) = tokio::task::spawn_blocking(move || {
            let mut output = vec![b'!'; 10];
            let result = reader.read_ranges(&batch, &mut output);
            (result, output)
        })
        .await
        .unwrap();
        assert_eq!(
            failed.unwrap_err().code(),
            afs_error::NODE_TRANSFER_RESOURCE_EXHAUSTED
        );
        assert_eq!(output, b"!!!!!!!!!!");
        let sessions = h.sessions();
        assert_eq!(sessions.len(), 1);
        let retired = &sessions[0];
        assert!(
            retired.closed,
            "the partial batch control session must close"
        );
        assert_eq!(
            retired.prepared_bytes, 0,
            "close must discard the old snapshot"
        );
        assert_eq!(retired.active_windows, 0);
        let count = |outcome: DfsHixlReleaseOutcome| {
            retired
                .outcomes
                .iter()
                .filter(|&&recorded| recorded == outcome as i32)
                .count()
        };
        // Two export slots give one-window waves (half the slots each), so
        // three waves complete before the fourth export is refused.
        assert_eq!(count(DfsHixlReleaseOutcome::Completed), 3);
        assert_eq!(
            count(DfsHixlReleaseOutcome::NotSubmitted),
            0,
            "the source rolls a refused wave back itself"
        );
        let slot = h
            .reader
            .context
            .slots
            .lock()
            .await
            .get(&h.peer())
            .unwrap()
            .clone();
        assert!(slot.lock().await.session.is_none());
        {
            let memory = h.native.memory.lock().unwrap();
            assert!(memory.active.is_empty());
            assert_eq!(
                memory.reads.len(),
                3,
                "the refused wave must never enter READ"
            );
        }
        assert_eq!(h.native.reader_quarantines.load(Ordering::SeqCst), 0);
        assert_eq!(h.native.source_quarantines.load(Ordering::SeqCst), 0);

        assert_eq!(h.read(10).await.unwrap(), b"!abcdefgh!");
        let sessions = h.sessions();
        assert_eq!(sessions.len(), 2);
        let fresh = sessions
            .iter()
            .find(|session| session.session_id != retired.session_id)
            .unwrap();
        assert!(!fresh.closed);
        assert_eq!(fresh.prepared_bytes, 0);
        assert_eq!(fresh.active_windows, 0);
        assert_eq!(
            fresh.outcomes,
            vec![DfsHixlReleaseOutcome::Completed as i32; 4]
        );
        assert_eq!(
            slot.lock().await.session.as_ref().unwrap().reply.session_id,
            fresh.session_id
        );
        assert!(h.native.memory.lock().unwrap().active.is_empty());
        // Three windows read before the refusal plus four in the fresh session.
        assert_eq!(h.native.memory.lock().unwrap().reads.len(), 7);
        assert_eq!(h.native.reader_quarantines.load(Ordering::SeqCst), 0);
        assert_eq!(h.native.source_quarantines.load(Ordering::SeqCst), 0);
        h.shutdown().await;
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn a_session_the_source_forgot_is_renegotiated_within_the_read() {
        let mut h = harness().await;
        h.batch.operations[0].op.length = 2;
        assert_eq!(h.read(4).await.unwrap(), b"!ab!");
        source_hixl::test_support::forget_sessions(&h.source);
        assert_eq!(
            h.read(4).await.unwrap(),
            b"!ab!",
            "an expired source session must not fail the next read"
        );
        assert_eq!(h.negotiations(), 2);
        assert_eq!(h.sessions().len(), 1);
        h.shutdown().await;
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn failed_negotiations_do_not_hold_peer_slots() {
        let h = harness().await;
        let context = &h.reader.context;
        let authority = read_authority(&h.batch);
        let deadline = Instant::now() + Duration::from_secs(5);
        for index in 0..=MAX_HIXL_PEERS {
            // The source rejects any other node identity, so every
            // negotiation fails after its slot was created.
            let failed = context
                .acquire(
                    &format!("other-{index}"),
                    9,
                    &h.endpoint,
                    &authority,
                    deadline,
                )
                .await;
            assert!(failed.is_err());
            let warm = context
                .start_warm_task(
                    format!("warm-{index}"),
                    9,
                    h.endpoint.clone(),
                    Some(authority.clone()),
                    1,
                )
                .await;
            if let WarmStart::Started(task) = warm {
                task.run().await;
            }
        }
        assert!(
            context.slots.lock().await.is_empty(),
            "failed warm and inline negotiations must free their slots"
        );
        assert!(
            context
                .acquire("source", 9, &h.endpoint, &authority, deadline)
                .await
                .is_ok(),
            "the real source still gets a slot after the failures"
        );
        h.shutdown().await;
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn a_rejected_negotiation_closes_its_source_session() {
        let h = harness().await;
        let context = &h.reader.context;
        // Another source generation already owns the endpoints this source
        // reports, so binding refuses the negotiated reply.
        bind_engines(
            &mut *context.engines.lock().await,
            (0..2).map(|lane| {
                (
                    if lane == 0 {
                        "127.0.0.1:7920"
                    } else {
                        "127.0.0.1:7921"
                    },
                    ("source".to_owned(), 9, 99),
                )
            }),
            MAX_HIXL_ENGINES,
        )
        .unwrap();
        let error = h.read(10).await.unwrap_err();
        assert_eq!(error.code(), afs_error::NODE_TRANSFER_DRAIN_INCOMPLETE);
        let sessions = h.sessions();
        assert_eq!(sessions.len(), 1);
        assert!(
            sessions[0].closed,
            "the source session must be closed, not left to its TTL"
        );
        h.shutdown().await;
    }
}
