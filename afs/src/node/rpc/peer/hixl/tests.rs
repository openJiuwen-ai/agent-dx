use super::*;
use crate::config::HixlDfsReadMode;
use afs_protocol::node_data::{
    DfsChunkReadOp, DfsDigestAlgorithm, DfsHixlLane, DfsHixlReadRange, DfsHixlTransport,
    DfsReadGrant, DfsReadRangesCompletion,
};
use afs_transport::hixl::{EngineInfo, ReadTransport};

#[test]
fn a_capacity_shortfall_does_not_fence_the_session() {
    let shortfall = WindowFailure::from_prepare(afs_error::Error::coded(
        afs_error::IO_CAPACITY,
        "shared Peer read payload budget exhausted",
    ));
    assert!(
        !shortfall.fence_peer,
        "capacity shortfalls narrow the wave instead of ending the session"
    );
    let unavailable = WindowFailure::from_prepare(afs_error::Error::coded(
        afs_error::IO_UNAVAILABLE,
        "peer serve is stopped",
    ));
    assert!(unavailable.fence_peer);
    let exhausted = WindowFailure::from_prepare(afs_error::Error::coded(
        afs_error::NODE_TRANSFER_RESOURCE_EXHAUSTED,
        "transfer capacity exhausted",
    ));
    assert!(!exhausted.fence_peer);
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
fn zero_session_idle_limit_disables_retirement() {
    assert_eq!(session_idle_limit(0), None);
    assert_eq!(session_idle_limit(1), Some(Duration::from_millis(1)));
    assert_eq!(session_idle_limit(300_000), Some(Duration::from_secs(300)));
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
    use afs_transport::hixl::{HixlError, HixlErrorKind, HixlPhase};
    let mut error = HixlError {
        phase: HixlPhase::SubmitRead,
        native_status: None,
        kind: HixlErrorKind::ResourceExhausted,
        message: "owner queue full".into(),
        read_may_be_in_flight: false,
    };
    assert_eq!(
        native_release_outcome(&Err(error.clone())),
        DfsHixlReleaseOutcome::NotSubmitted
    );
    error.read_may_be_in_flight = true;
    assert_eq!(
        native_release_outcome(&Err(error)),
        DfsHixlReleaseOutcome::Unknown
    );
}

#[test]
fn prepare_capacity_refusal_is_retryable_without_session_fencing() {
    let failure = WindowFailure::from_prepare(afs_error::Error::coded(
        afs_error::NODE_TRANSFER_RESOURCE_EXHAUSTED,
        "export pool full",
    ));
    assert!(!failure.fence_peer);
    let failure = WindowFailure::from_prepare(afs_error::Error::coded(
        afs_error::NODE_TRANSFER_UNAVAILABLE,
        "prepare reply lost",
    ));
    assert!(failure.fence_peer);
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
        hixl_supported: true,
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

#[test]
fn negotiation_accepts_matching_roce_and_hccs_parameters() {
    let (_, mut reply, _) = descriptor();
    validate_negotiation(
        &reply,
        "source",
        7,
        &engine_info(ReadTransport::Roce, 0, 0, None),
    )
    .unwrap();

    reply.transport = DfsHixlTransport::HccsBufferPool as i32;
    reply.buffer_pool_count = 4;
    reply.buffer_pool_size_mib = 64;
    reply.hccs_domain_id = Some(0);
    validate_negotiation(
        &reply,
        "source",
        7,
        &engine_info(ReadTransport::HccsBufferPool, 4, 64, Some(0)),
    )
    .unwrap();
}

#[test]
fn negotiation_rejects_unknown_or_mismatched_transport_parameters() {
    let (_, reply, _) = descriptor();
    let local_roce = engine_info(ReadTransport::Roce, 0, 0, None);
    let mut cases = Vec::new();

    let mut unknown = reply.clone();
    unknown.transport = 99;
    cases.push((unknown, local_roce.clone()));

    let mut roce_pool = reply.clone();
    roce_pool.buffer_pool_count = 1;
    cases.push((roce_pool, local_roce.clone()));

    let mut roce_domain = reply.clone();
    roce_domain.hccs_domain_id = Some(0);
    cases.push((roce_domain, local_roce.clone()));

    let mut hccs_reply = reply.clone();
    hccs_reply.transport = DfsHixlTransport::HccsBufferPool as i32;
    hccs_reply.buffer_pool_count = 4;
    hccs_reply.buffer_pool_size_mib = 64;
    cases.push((
        hccs_reply.clone(),
        engine_info(ReadTransport::HccsBufferPool, 8, 64, Some(0)),
    ));
    hccs_reply.buffer_pool_size_mib = 32;
    cases.push((
        hccs_reply,
        engine_info(ReadTransport::HccsBufferPool, 4, 64, Some(0)),
    ));

    let mut empty_hccs = reply;
    empty_hccs.transport = DfsHixlTransport::HccsBufferPool as i32;
    cases.push((
        empty_hccs,
        engine_info(ReadTransport::HccsBufferPool, 4, 64, Some(0)),
    ));

    let mut missing_domain = descriptor().1;
    missing_domain.transport = DfsHixlTransport::HccsBufferPool as i32;
    missing_domain.buffer_pool_count = 4;
    missing_domain.buffer_pool_size_mib = 64;
    cases.push((
        missing_domain,
        engine_info(ReadTransport::HccsBufferPool, 4, 64, Some(0)),
    ));

    let mut wrong_domain = descriptor().1;
    wrong_domain.transport = DfsHixlTransport::HccsBufferPool as i32;
    wrong_domain.buffer_pool_count = 4;
    wrong_domain.buffer_pool_size_mib = 64;
    wrong_domain.hccs_domain_id = Some(7);
    cases.push((
        wrong_domain,
        engine_info(ReadTransport::HccsBufferPool, 4, 64, Some(0)),
    ));

    for (bad_reply, local) in cases {
        let error = validate_negotiation(&bad_reply, "source", 7, &local)
            .expect_err("invalid HIXL transport negotiation");
        assert_eq!(error.code(), afs_error::CLIENT_PROTOCOL_VIOLATION);
        assert!(!may_fallback(HixlDfsReadMode::Preferred, false, &error));
    }
}

#[test]
fn negotiation_rejects_mismatched_protocol_versions() {
    for version in [0, HIXL_PROTOCOL_VERSION + 1] {
        let (_, mut reply, _) = descriptor();
        reply.protocol_version = version;
        let error = validate_negotiation(
            &reply,
            "source",
            7,
            &engine_info(ReadTransport::Roce, 0, 0, None),
        )
        .expect_err("mismatched HIXL replies must be rejected");
        assert_eq!(error.code(), afs_error::CLIENT_PROTOCOL_VIOLATION);
    }
}

#[tokio::test]
async fn prepare_wave_refusal_releases_all_admitted_windows_as_not_submitted() {
    let (window, _, _) = descriptor();
    let planned = (0..3)
        .map(|index| (index, window.clone(), index as u64 * 3, index))
        .collect::<Vec<_>>();
    let attempts = Arc::new(AtomicU64::new(0));
    let releases = Arc::new(StdMutex::new(Vec::new()));
    let result = prepare_wave(
        &planned,
        |_, _, lane| {
            let attempts = attempts.clone();
            async move {
                attempts.fetch_add(1, Ordering::Relaxed);
                if lane == 2 {
                    return Err(WindowFailure::from_prepare(afs_error::Error::coded(
                        afs_error::NODE_TRANSFER_RESOURCE_EXHAUSTED,
                        "third lane refused",
                    )));
                }
                Ok(DfsPrepareReadRangesHixlReply {
                    slot_id: lane as u64,
                    ..Default::default()
                })
            }
        },
        |reply, outcome| {
            let releases = releases.clone();
            async move {
                releases.lock().unwrap().push((reply.slot_id, outcome));
                Ok(())
            }
        },
    )
    .await;
    let failure = result
        .err()
        .expect("prepare must refuse the wave before any READ");
    assert_eq!(attempts.load(Ordering::Relaxed), 3);
    assert_eq!(
        *releases.lock().unwrap(),
        vec![
            (0, DfsHixlReleaseOutcome::NotSubmitted),
            (1, DfsHixlReleaseOutcome::NotSubmitted)
        ]
    );
    assert!(!failure.fence_peer);
    assert!(!failure.quarantine_local);
}

#[tokio::test]
async fn collect_wave_waits_for_sibling_cleanup_after_unknown_lane_failure() {
    use afs_transport::hixl::{HixlError, HixlErrorKind, HixlPhase};

    let (window, _, _) = descriptor();
    let other = window.clone();
    let releases = Arc::new(AtomicU64::new(0));
    let (failed, failure_seen) = tokio::sync::oneshot::channel();
    let (finish, finishing) = tokio::sync::oneshot::channel();
    let mut tasks = tokio::task::JoinSet::new();
    tasks.spawn(async move {
        failed.send(()).unwrap();
        (
            0,
            window,
            Err(WindowFailure::from_native(
                HixlError {
                    phase: HixlPhase::SubmitRead,
                    native_status: None,
                    kind: HixlErrorKind::EnginePoisoned,
                    message: "unknown DMA".into(),
                    read_may_be_in_flight: true,
                },
                false,
            )),
        )
    });
    let sibling_releases = releases.clone();
    tasks.spawn(async move {
        finishing.await.unwrap();
        sibling_releases.fetch_add(1, Ordering::Release);
        let bytes = vec![7; other.length];
        (1, other, Ok(bytes))
    });
    let collection = tokio::spawn(collect_wave(tasks));
    failure_seen.await.unwrap();
    tokio::task::yield_now().await;
    assert!(!collection.is_finished());
    assert_eq!(releases.load(Ordering::Acquire), 0);
    finish.send(()).unwrap();
    let wave = collection.await.unwrap();
    assert!(wave.failure.unwrap().quarantine_local);
    assert_eq!(releases.load(Ordering::Acquire), 1);
    assert_eq!(wave.completed.len(), 1);
}

#[tokio::test]
async fn outer_timeout_keeps_all_lane_cleanup_alive() {
    let (window, _, _) = descriptor();
    let releases = Arc::new(AtomicU64::new(0));
    let mut finishers = Vec::new();
    let mut tasks = tokio::task::JoinSet::new();
    for lane in 0..2 {
        let (finish, finishing) = tokio::sync::oneshot::channel();
        finishers.push(finish);
        let window = window.clone();
        let releases = releases.clone();
        tasks.spawn(async move {
            finishing.await.unwrap();
            releases.fetch_add(1, Ordering::Release);
            let bytes = vec![7; window.length];
            (lane, window, Ok(bytes))
        });
    }
    let cancel = Arc::new(Cancellation::default());
    let peer = Arc::new(AtomicBool::new(false));
    cancel.publish(peer.clone()).unwrap();
    let (returned, returning) = tokio::sync::oneshot::channel();
    let task = tokio::spawn(async move {
        let wave = collect_wave(tasks).await;
        assert!(wave.failure.is_none());
        returned.send(wave.completed.len()).unwrap();
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
    assert!(cancel.cancelled.load(Ordering::Acquire));
    assert!(peer.load(Ordering::Acquire));
    assert_eq!(releases.load(Ordering::Acquire), 0);
    for finish in finishers {
        finish.send(()).unwrap();
    }
    assert_eq!(
        tokio::time::timeout(Duration::from_secs(1), returning)
            .await
            .unwrap()
            .unwrap(),
        2
    );
    assert_eq!(releases.load(Ordering::Acquire), 2);
}

#[test]
fn out_of_order_completion_preserves_overlap_order_and_atomic_scatter_validation() {
    let mut authority = request();
    authority.operations[0].length = 4;
    authority.operations[0].destination_offset = 0;
    authority.operations[0].chunk_id = "first".into();
    let mut second = authority.operations[0].clone();
    second.operation_index = 1;
    second.chunk_id = "last".into();
    authority.operations.push(second);
    let mut completed = pack_windows(authority, 2)
        .unwrap()
        .into_iter()
        .enumerate()
        .map(|(ordinal, window)| {
            let byte = if window.request.operations[0].chunk_id == "first" {
                b'a'
            } else {
                b'b'
            };
            let bytes = vec![byte; window.length];
            (ordinal, window, bytes)
        })
        .collect::<Vec<_>>();
    completed.reverse();
    let mut output = [0; 4];
    scatter_complete(completed.clone(), &mut output).unwrap();
    assert_eq!(&output, b"bbbb");
    completed[0].1.mappings[0].0 = usize::MAX;
    let mut unchanged = [9; 4];
    assert!(scatter_complete(completed, &mut unchanged).is_err());
    assert_eq!(unchanged, [9; 4]);
}

#[test]
fn only_uncertain_local_native_lifetimes_quarantine_the_local_group() {
    use afs_transport::hixl::{HixlError, HixlErrorKind, HixlPhase};
    for code in [
        afs_error::NODE_TRANSFER_INVALID,
        afs_error::NODE_TRANSFER_CORRUPT_DATA,
        afs_error::NODE_TRANSFER_RESOURCE_EXHAUSTED,
    ] {
        assert!(
            !WindowFailure::from_prepare(afs_error::Error::coded(code, "source refused"))
                .quarantine_local
        );
    }
    let mut native = HixlError {
        phase: HixlPhase::SubmitRead,
        native_status: None,
        kind: HixlErrorKind::ResourceExhausted,
        message: "queue full".into(),
        read_may_be_in_flight: false,
    };
    assert!(!WindowFailure::from_native(native.clone(), true).quarantine_local);
    native.read_may_be_in_flight = true;
    assert!(WindowFailure::from_native(native, false).quarantine_local);
    assert!(WindowFailure::worker_failed("unmanaged lane".into()).quarantine_local);
}

#[test]
fn parallel_windows_fill_capacity_and_preserve_packed_identity() {
    let mut authority = request();
    authority.operations[0].length = 17;
    let batch = pack_read_batch(authority.clone()).unwrap();
    for lane_count in [1, 2, 4, 8, 16] {
        let capacity = parallel_window_capacity(&batch, 4, lane_count).unwrap();
        let windows = pack_windows(authority.clone(), capacity).unwrap();
        assert_eq!(
            windows
                .iter()
                .map(|window| window.length)
                .collect::<Vec<_>>(),
            vec![4, 4, 4, 4, 1],
            "extra lanes must not multiply windows for the same batch"
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
    assert!(parallel_window_capacity(&batch, 4, 0).is_err());
    assert!(parallel_window_capacity(&batch, 4, 17).is_err());
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
    let mut first = authority.operations.remove(0);
    first.chunk_id = "chunk-a".into();
    first.chunk_offset = 10;
    first.length = 5;
    first.destination_offset = 3;
    first.source_copy_id = "copy-a".into();
    first.grant.as_mut().unwrap().token = "grant-a".into();

    let mut second = first.clone();
    second.operation_index = 1;
    second.chunk_id = "chunk-b".into();
    second.chunk_offset = 40;
    second.length = 4;
    second.destination_offset = 12;
    second.source_copy_id = "copy-b".into();
    second.grant.as_mut().unwrap().token = "grant-b".into();

    let mut third = first.clone();
    third.operation_index = 2;
    third.chunk_offset = 90;
    third.length = 3;
    third.destination_offset = 22;
    third.source_copy_id = "copy-c".into();
    authority.operations = vec![first, second, third];

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
        (
            "chunk-a".to_owned(),
            (0..128)
                .map(|byte| u8::try_from(byte).unwrap())
                .collect::<Vec<_>>(),
        ),
        (
            "chunk-b".to_owned(),
            (128..256)
                .map(|byte| u8::try_from(byte).unwrap())
                .collect::<Vec<_>>(),
        ),
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
    assert!(may_fallback(
        HixlDfsReadMode::Preferred,
        false,
        &unsupported
    ));
    assert!(!may_fallback(
        HixlDfsReadMode::Preferred,
        true,
        &unsupported
    ));
    assert!(!may_fallback(
        HixlDfsReadMode::Required,
        false,
        &unsupported
    ));
    let bare = afs_transport::grpc::error_status::status_to_error(tonic::Status::unimplemented(
        "arbitrary",
    ));
    assert!(!may_fallback(HixlDfsReadMode::Preferred, false, &bare));
    assert!(!may_fallback(
        HixlDfsReadMode::Preferred,
        false,
        &afs_error::Error::coded(afs_error::IO_TIMEOUT, "timeout")
    ));
}

#[test]
fn engine_address_cannot_be_reconnected_across_source_generations() {
    let (_, mut reply, _) = descriptor();
    let mut engines = HashMap::new();
    bind_engine(&mut engines, &reply).unwrap();
    bind_engine(&mut engines, &reply).unwrap();
    reply.engine_generation += 1;
    assert_eq!(
        bind_engine(&mut engines, &reply).unwrap_err().code(),
        afs_error::NODE_TRANSFER_DRAIN_INCOMPLETE
    );
    assert_eq!(engines[&reply.engine_id].2, 9);
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
    use afs_transport::hixl::{ExportWindow, HixlError, HixlErrorKind, HixlPhase};
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
    struct NativeBoundary {
        memory: StdMutex<MemoryExports>,
        prepare_calls: AtomicU64,
        batch_reads: AtomicU64,
        reader_quarantines: AtomicU64,
        source_quarantines: AtomicU64,
        connects: StdMutex<ConnectGate>,
        connects_changed: std::sync::Condvar,
    }

    impl NativeBoundary {
        fn close_connect_gate(&self) {
            self.connects.lock().unwrap().closed = true;
        }
        fn open_connect_gate(&self) {
            self.connects.lock().unwrap().closed = false;
            self.connects_changed.notify_all();
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

    fn boundary_error(phase: HixlPhase, kind: HixlErrorKind, message: &str) -> HixlError {
        HixlError {
            phase,
            kind,
            native_status: None,
            message: message.into(),
            read_may_be_in_flight: false,
        }
    }

    fn boundary_infos(
        first_port: u16,
        generation: u64,
        lanes: u16,
        window_bytes: usize,
    ) -> Vec<EngineInfo> {
        (0..lanes)
            .map(|offset| EngineInfo {
                engine_id: format!("127.0.0.1:{}", first_port + offset),
                generation,
                window_bytes,
                transport: ReadTransport::Roce,
                buffer_pool_count: 0,
                buffer_pool_size_mib: 0,
                hccs_domain_id: None,
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

        fn prepare_export(
            &self,
            lane: usize,
            bytes: Vec<u8>,
            _deadline: Instant,
        ) -> Result<ExportWindow, HixlError> {
            if self.boundary.prepare_calls.fetch_add(1, Ordering::SeqCst) == 3 {
                return Err(boundary_error(
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
                return Err(boundary_error(
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
                    boundary_error(
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

    impl ReadLane for ReaderLane {
        fn connect(&self, peer: &str, _deadline: Instant) -> Result<(), HixlError> {
            if peer != self.source_engine {
                return Err(boundary_error(
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

        fn read(
            &self,
            peer: &str,
            address: u64,
            length: usize,
            deadline: Instant,
        ) -> Result<Vec<u8>, HixlError> {
            self.connect(peer, deadline)?;
            let mut memory = self.boundary.memory.lock().unwrap();
            let window = memory.active.get(&address).ok_or_else(|| {
                boundary_error(
                    HixlPhase::SubmitRead,
                    HixlErrorKind::InvalidState,
                    "READ has no live source export",
                )
            })?;
            if window.lane != self.lane || window.bytes.len() != length {
                return Err(boundary_error(
                    HixlPhase::SubmitRead,
                    HixlErrorKind::InvalidArgument,
                    "READ differs from source lane or length",
                ));
            }
            let bytes = window.bytes.clone();
            memory.reads.push((self.lane, address, length));
            Ok(bytes)
        }

        fn prepare_read_buffer(&self, length: usize) -> Result<Vec<u8>, HixlError> {
            if length == 0 {
                return Err(boundary_error(
                    HixlPhase::SubmitRead,
                    HixlErrorKind::InvalidArgument,
                    "empty read buffer",
                ));
            }
            let mut output = Vec::new();
            output
                .try_reserve_exact(length)
                .map_err(|error| HixlError {
                    phase: HixlPhase::SubmitRead,
                    native_status: None,
                    kind: HixlErrorKind::ResourceExhausted,
                    message: format!("test read buffer allocation failed: {error}"),
                    read_may_be_in_flight: false,
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
            let mut offset = 0;
            for (address, length) in windows {
                let bytes = self.read(peer, *address, *length, deadline)?;
                let end = offset + length;
                output[offset..end].copy_from_slice(&bytes);
                offset = end;
            }
            if offset != output.len() {
                return Err(boundary_error(
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

    fn read_batch(chunk_id: crate::dfs::ChunkId, endpoint: &str) -> PeerReadBatch {
        let version = crate::dfs::FileVersionId::new("version");
        let layout = crate::dfs::LayoutRootId::new("layout");
        PeerReadBatch {
            file_version_id: version.clone(),
            layout_root_id: layout.clone(),
            operations: vec![ResolvedReadOp {
                op: ChunkReadOp {
                    chunk_id: chunk_id.clone(),
                    chunk_offset: 0,
                    length: 8,
                    output_offset: 1,
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
                        file_version_id: version,
                        layout_root_id: layout,
                        caller_node_id: "reader".into(),
                        caller_node_epoch: 1,
                        expires_at_unix_ms: u64::MAX,
                        fence: 1,
                        token: "offline-grant".into(),
                    },
                },
                chunk: None,
            }],
        }
    }

    struct Harness {
        _temp: tempfile::TempDir,
        native: Arc<NativeBoundary>,
        source: Arc<source_hixl::HixlReadService>,
        reader: Arc<HixlChunkTransfer>,
        batch: PeerReadBatch,
        endpoint: String,
        stop: tokio::sync::oneshot::Sender<()>,
        server: tokio::task::JoinHandle<()>,
    }

    async fn harness() -> Harness {
        harness_with(2, 2, 1).await
    }

    async fn preferred_harness() -> Harness {
        harness_with_mode(2, 2, 1, HixlDfsReadMode::Preferred).await
    }

    async fn group_harness() -> Harness {
        harness_with(1, 4, 8).await
    }

    async fn harness_with(lanes: u16, window_bytes: usize, export_slots: usize) -> Harness {
        harness_with_mode(lanes, window_bytes, export_slots, HixlDfsReadMode::Required).await
    }

    async fn harness_with_mode(
        lanes: u16,
        window_bytes: usize,
        export_slots: usize,
        mode: HixlDfsReadMode,
    ) -> Harness {
        harness_with_capabilities(lanes, window_bytes, export_slots, mode, true, None).await
    }

    async fn harness_with_capabilities(
        lanes: u16,
        window_bytes: usize,
        export_slots: usize,
        mode: HixlDfsReadMode,
        source_supported: bool,
        group: Option<ReadGroupPolicy>,
    ) -> Harness {
        let temp = tempfile::tempdir().unwrap();
        let local = Arc::new(LocalChunkStore::open(temp.path(), "source").unwrap());
        let staged = StagedChunk::new(
            crate::dfs::OperationId::new("offline-read"),
            b"abcdefgh".to_vec(),
        );
        let chunk_id = staged.chunk.id.clone();
        local.put(staged).unwrap();
        let native = Arc::new(NativeBoundary::default());
        let source = Arc::new(source_hixl::HixlReadService::for_test(
            Arc::new(SourceNative {
                boundary: native.clone(),
                lanes,
                window_bytes,
                export_slots,
            }),
            "source".into(),
            9,
        ));
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let endpoint = format!("http://{}", listener.local_addr().unwrap());
        let service = make_dfs_chunks_server_with_transport(
            Some(local),
            Arc::new(TestAuthority),
            Arc::new(TestAuthority),
            Arc::new(DenyDfsReplicaAuthorizer),
            None,
            Duration::from_secs(5),
            DfsChunkTransportResources {
                read_pipeline: Default::default(),
                rdma_sessions: crate::node::rpc::control::RdmaSessionRegistry::new(None),
                payload_metrics: None,
                hixl: source_supported.then(|| source.clone()),
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
            .unwrap(),
        );
        let mut reader = HixlChunkTransfer::from_runtime(
            peers,
            Arc::new(ReaderNative {
                boundary: native.clone(),
                lanes,
                window_bytes,
            }),
            mode,
            Duration::from_secs(5),
        )
        .unwrap();
        if let Some(group) = group {
            reader = reader.with_read_group(group, export_slots);
        }
        let reader = Arc::new(reader);
        let batch = read_batch(chunk_id, &endpoint);
        Harness {
            _temp: temp,
            native,
            source,
            reader,
            batch,
            endpoint,
            stop,
            server,
        }
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn small_batches_keep_one_window_on_the_bound_lane() {
        let Harness {
            _temp,
            native,
            source,
            reader,
            mut batch,
            stop,
            server,
            ..
        } = harness().await;
        batch.operations[0].op.length = 2;
        for expected_lanes in [vec![0], vec![0, 0], vec![0, 0, 0]] {
            let reader = reader.clone();
            let batch = batch.clone();
            let output = tokio::task::spawn_blocking(move || {
                let mut output = vec![b'!'; 4];
                reader.read_ranges(&batch, &mut output).unwrap();
                output
            })
            .await
            .unwrap();
            assert_eq!(output, b"!ab!");
            let memory = native.memory.lock().unwrap();
            assert_eq!(
                memory.reads.iter().map(|read| read.0).collect::<Vec<_>>(),
                expected_lanes,
                "one small batch uses one window on the peer's bound lane"
            );
            assert!(memory.reads.iter().all(|read| read.2 == 2));
            assert!(memory.active.is_empty());
            drop(memory);
            let sessions = source_hixl::test_support::sessions(&source);
            assert_eq!(sessions.len(), 1, "one peer keeps one control session");
            assert_eq!(sessions[0].prepared_bytes, 0);
            assert_eq!(sessions[0].active_windows, 0);
        }
        stop.send(()).unwrap();
        tokio::time::timeout(Duration::from_secs(1), server)
            .await
            .unwrap()
            .unwrap();
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn peers_spread_over_lanes_before_sharing_one() {
        let Harness {
            _temp,
            reader,
            stop,
            server,
            ..
        } = harness().await;
        let first = ("peer-a".to_owned(), 9, "grpc-a".to_owned());
        let second = ("peer-b".to_owned(), 9, "grpc-b".to_owned());
        let third = ("peer-c".to_owned(), 9, "grpc-c".to_owned());
        assert_eq!(reader.context.bind_lane(&first), 0);
        assert_eq!(reader.context.bind_lane(&second), 1);
        assert_eq!(
            reader.context.bind_lane(&first),
            0,
            "a bound peer keeps its lane"
        );
        assert_eq!(
            reader.context.bind_lane(&third),
            0,
            "a peer without a free lane shares the least-used one"
        );
        stop.send(()).unwrap();
        tokio::time::timeout(Duration::from_secs(1), server)
            .await
            .unwrap()
            .unwrap();
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn unsupported_fallback_preserves_the_group_budget() {
        let Harness {
            _temp,
            native,
            reader,
            mut batch,
            stop,
            server,
            ..
        } = harness_with_capabilities(
            2,
            2,
            1,
            HixlDfsReadMode::Preferred,
            false,
            Some(ReadGroupPolicy {
                bytes: 1024,
                max_inflight: 1,
                max_descriptors: crate::node::dfs_read::MAX_DFS_GROUP_OPS,
                host_budget_bytes: 2048,
            }),
        )
        .await;
        let template = batch.operations[0].clone();
        batch.operations = (0..=crate::node::dfs_read::MAX_DFS_READ_OPS)
            .map(|index| {
                let mut operation = template.clone();
                operation.op.length = 1;
                operation.op.output_offset = index;
                operation
            })
            .collect();
        let expected = vec![b'a'; batch.operations.len()];

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
        assert!(native.memory.lock().unwrap().reads.is_empty());
        stop.send(()).unwrap();
        tokio::time::timeout(Duration::from_secs(1), server)
            .await
            .unwrap()
            .unwrap();
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn preferred_cold_batch_uses_grpc_while_the_session_warms() {
        let Harness {
            _temp,
            native,
            source,
            reader,
            mut batch,
            stop,
            server,
            ..
        } = preferred_harness().await;
        batch.operations[0].op.length = 2;
        native.close_connect_gate();
        let first = {
            let reader = reader.clone();
            let batch = batch.clone();
            tokio::time::timeout(
                Duration::from_secs(2),
                tokio::task::spawn_blocking(move || {
                    let mut output = vec![b'!'; 4];
                    reader.read_ranges(&batch, &mut output)?;
                    Ok::<_, afs_error::Error>(output)
                }),
            )
            .await
            .expect("cold gRPC fallback must not wait for the HIXL connect")
            .unwrap()
            .unwrap()
        };
        assert_eq!(first, b"!ab!");
        native.wait_connect_entered(1).await;
        assert_eq!(
            native.connects.lock().unwrap().completed,
            0,
            "the first batch must finish while the native connect is still blocked"
        );
        assert!(
            native.memory.lock().unwrap().reads.is_empty(),
            "the cold batch must not use a native HIXL read"
        );

        native.open_connect_gate();
        wait_warm_session(&reader, &batch).await;
        let second = {
            let reader = reader.clone();
            let batch = batch.clone();
            tokio::task::spawn_blocking(move || {
                let mut output = vec![b'!'; 4];
                reader.read_ranges(&batch, &mut output)?;
                Ok::<_, afs_error::Error>(output)
            })
            .await
            .unwrap()
            .unwrap()
        };
        assert_eq!(second, b"!ab!");
        assert_eq!(
            native.memory.lock().unwrap().reads.len(),
            1,
            "the warmed session must serve the next batch natively"
        );
        assert_eq!(
            source_hixl::test_support::sessions(&source).len(),
            1,
            "warming must negotiate exactly one control session"
        );
        stop.send(()).unwrap();
        tokio::time::timeout(Duration::from_secs(1), server)
            .await
            .unwrap()
            .unwrap();
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn concurrent_peer_reads_queue_instead_of_failing() {
        let Harness {
            _temp,
            native,
            reader,
            mut batch,
            stop,
            server,
            ..
        } = harness().await;
        batch.operations[0].op.length = 2;
        // The first read holds the peer admission while its native connect
        // is blocked; a second read must queue for it instead of reporting
        // an exhausted peer.
        native.close_connect_gate();
        let first = {
            let reader = reader.clone();
            let batch = batch.clone();
            tokio::spawn(async move {
                tokio::task::spawn_blocking(move || {
                    let mut output = vec![b'!'; 3];
                    reader.read_ranges(&batch, &mut output)?;
                    Ok::<_, afs_error::Error>(output)
                })
                .await
                .unwrap()
            })
        };
        native.wait_connect_entered(1).await;
        let second = {
            let reader = reader.clone();
            let batch = batch.clone();
            tokio::spawn(async move {
                tokio::time::sleep(Duration::from_millis(50)).await;
                tokio::task::spawn_blocking(move || {
                    let mut output = vec![b'!'; 3];
                    reader.read_ranges(&batch, &mut output)?;
                    Ok::<_, afs_error::Error>(output)
                })
                .await
                .unwrap()
            })
        };
        tokio::time::sleep(Duration::from_millis(100)).await;
        native.open_connect_gate();
        let first = first.await.unwrap().unwrap();
        let second = second.await.unwrap().unwrap();
        assert_eq!(&first[1..], b"ab");
        assert_eq!(&second[1..], b"ab");
        stop.send(()).unwrap();
        tokio::time::timeout(Duration::from_secs(1), server)
            .await
            .unwrap()
            .unwrap();
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn crc32c_chunks_travel_through_native_windows() {
        let temp = tempfile::tempdir().unwrap();
        let local = Arc::new(LocalChunkStore::open(temp.path(), "source").unwrap());
        let staged = StagedChunk::new_crc32c(
            crate::dfs::OperationId::new("crc-window"),
            b"abcdefgh".to_vec(),
        )
        .unwrap();
        let chunk = staged.chunk.clone();
        let chunk_id = chunk.id.clone();
        local.put(staged).unwrap();
        let staged_tail = StagedChunk::new_crc32c(
            crate::dfs::OperationId::new("crc-window-tail"),
            b"ijklmnop".to_vec(),
        )
        .unwrap();
        let tail_chunk = staged_tail.chunk.clone();
        local.put(staged_tail).unwrap();
        let native = Arc::new(NativeBoundary::default());
        let source = Arc::new(source_hixl::HixlReadService::for_test(
            Arc::new(SourceNative {
                boundary: native.clone(),
                lanes: 1,
                window_bytes: 16,
                export_slots: 4,
            }),
            "source".into(),
            9,
        ));
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let endpoint = format!("http://{}", listener.local_addr().unwrap());
        let service = make_dfs_chunks_server_with_transport(
            Some(local),
            Arc::new(TestAuthority),
            Arc::new(TestAuthority),
            Arc::new(DenyDfsReplicaAuthorizer),
            None,
            Duration::from_secs(5),
            DfsChunkTransportResources {
                read_pipeline: Default::default(),
                rdma_sessions: crate::node::rpc::control::RdmaSessionRegistry::new(None),
                payload_metrics: None,
                hixl: Some(source.clone()),
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
            .unwrap(),
        );
        let reader = Arc::new(
            HixlChunkTransfer::from_runtime(
                peers,
                Arc::new(ReaderNative {
                    boundary: native.clone(),
                    lanes: 1,
                    window_bytes: 16,
                }),
                HixlDfsReadMode::Required,
                Duration::from_secs(5),
            )
            .unwrap(),
        );
        // Two CRC32C chunks share one window: each operation carries its
        // own Meta-bound descriptor, exactly like a BLAKE3 batch.
        let mut batch = read_batch(chunk_id.clone(), &endpoint);
        let mut tail_op = batch.operations[0].clone();
        tail_op.op = crate::node::dfs_read::ChunkReadOp {
            chunk_id: tail_chunk.id.clone(),
            chunk_offset: 0,
            length: 8,
            output_offset: 9,
        };
        batch.operations[0].chunk = Some(chunk.clone());
        batch.operations[0].op.length = 8;
        tail_op.source.chunk_id = tail_chunk.id.clone();
        tail_op.chunk = Some(tail_chunk.clone());
        batch.operations.push(tail_op);

        let read_reader = reader.clone();
        let read_batch_input = batch.clone();
        let output = tokio::task::spawn_blocking(move || {
            let mut output = vec![b'!'; 17];
            read_reader
                .read_ranges(&read_batch_input, &mut output)
                .unwrap();
            output
        })
        .await
        .unwrap();
        assert_eq!(&output[1..9], b"abcdefgh");
        assert_eq!(&output[9..17], b"ijklmnop");
        assert_eq!(
            native.prepare_calls.load(Ordering::SeqCst),
            1,
            "both CRC32C ranges must share one native window"
        );
        let _ = source;
        stop.send(()).unwrap();
        tokio::time::timeout(Duration::from_secs(1), server)
            .await
            .unwrap()
            .unwrap();
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn crc32c_group_wave_is_one_prepare_while_the_peer_budget_is_held() {
        use crate::node::dfs_read::ReadGroupPolicy;
        let temp = tempfile::tempdir().unwrap();
        let local = Arc::new(LocalChunkStore::open(temp.path(), "source").unwrap());
        let staged = StagedChunk::new_crc32c(
            crate::dfs::OperationId::new("crc-group"),
            b"abcdefgh".to_vec(),
        )
        .unwrap();
        let chunk = staged.chunk.clone();
        local.put(staged).unwrap();
        let staged_tail = StagedChunk::new_crc32c(
            crate::dfs::OperationId::new("crc-group-tail"),
            b"ijkl".to_vec(),
        )
        .unwrap();
        let tail_chunk = staged_tail.chunk.clone();
        local.put(staged_tail).unwrap();
        // Concurrent peer reads hold the whole shared payload budget. The
        // group snapshot must not need any of it for CRC32C ranges.
        let held = local
            .reserve_peer_read_payload(crate::node::chunk::PEER_READ_PAYLOAD_LIMIT)
            .unwrap();
        let native = Arc::new(NativeBoundary::default());
        let source = Arc::new(source_hixl::HixlReadService::for_test(
            Arc::new(SourceNative {
                boundary: native.clone(),
                lanes: 1,
                window_bytes: 4,
                export_slots: 8,
            }),
            "source".into(),
            9,
        ));
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
                hixl: Some(source.clone()),
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
            .unwrap(),
        );
        let reader = Arc::new(
            HixlChunkTransfer::from_runtime(
                peers,
                Arc::new(ReaderNative {
                    boundary: native.clone(),
                    lanes: 1,
                    window_bytes: 4,
                }),
                HixlDfsReadMode::Required,
                Duration::from_secs(5),
            )
            .unwrap()
            .with_read_group(
                ReadGroupPolicy {
                    bytes: 16,
                    max_inflight: 1,
                    max_descriptors: 8,
                    host_budget_bytes: 1024 * 1024,
                },
                8,
            ),
        );
        let mut batch = read_batch(chunk.id.clone(), &endpoint);
        let mut tail_op = batch.operations[0].clone();
        tail_op.op = crate::node::dfs_read::ChunkReadOp {
            chunk_id: tail_chunk.id.clone(),
            chunk_offset: 0,
            length: 4,
            output_offset: 9,
        };
        batch.operations[0].chunk = Some(chunk.clone());
        tail_op.source.chunk_id = tail_chunk.id.clone();
        tail_op.chunk = Some(tail_chunk.clone());
        batch.operations.push(tail_op);

        let read_reader = reader.clone();
        let output = tokio::task::spawn_blocking(move || {
            let mut output = vec![b'!'; 13];
            read_reader.read_ranges(&batch, &mut output).unwrap();
            output
        })
        .await
        .unwrap();
        assert_eq!(&output[1..9], b"abcdefgh");
        assert_eq!(&output[9..13], b"ijkl");
        // Three 4-byte windows: one group Prepare exports all of them and
        // one native batch reads them, with no per-window retry.
        assert_eq!(native.prepare_calls.load(Ordering::SeqCst), 3);
        assert_eq!(native.batch_reads.load(Ordering::SeqCst), 1);
        let sessions = source_hixl::test_support::sessions(&source);
        assert_eq!(sessions.len(), 1);
        assert_eq!(sessions[0].active_windows, 0);
        assert_eq!(sessions[0].prepared_bytes, 0);
        drop(held);
        stop.send(()).unwrap();
        tokio::time::timeout(Duration::from_secs(1), server)
            .await
            .unwrap()
            .unwrap();
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn prewarm_connects_the_session_before_the_first_read() {
        let Harness {
            _temp,
            native,
            reader,
            mut batch,
            endpoint,
            stop,
            server,
            ..
        } = harness_with_mode(2, 2, 1, HixlDfsReadMode::Required).await;
        batch.operations[0].op.length = 2;

        let prewarm_reader = reader.clone();
        let connected =
            tokio::task::spawn_blocking(move || prewarm_reader.prewarm("source", 9, &endpoint, 1))
                .await
                .unwrap();
        assert!(connected, "pre-warm must publish a session");
        {
            let gate = native.connects.lock().unwrap();
            assert_eq!(gate.completed, 2, "pre-warm connects every lane once");
        }

        let read_reader = reader.clone();
        let read_batch = batch.clone();
        let output = tokio::task::spawn_blocking(move || {
            let mut output = vec![b'!'; 3];
            read_reader.read_ranges(&read_batch, &mut output).unwrap();
            output
        })
        .await
        .unwrap();
        assert_eq!(&output[1..], b"ab");
        {
            let gate = native.connects.lock().unwrap();
            // The reader-lane mock counts every native READ as one
            // connect, so a read that re-established the session would
            // add the two lane connects on top of this one.
            assert_eq!(
                gate.completed, 3,
                "the first read reuses the pre-warmed session"
            );
        }
        stop.send(()).unwrap();
        tokio::time::timeout(Duration::from_secs(1), server)
            .await
            .unwrap()
            .unwrap();
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn queued_admission_past_the_deadline_reports_a_deadline_error() {
        let Harness {
            _temp,
            native,
            reader,
            mut batch,
            endpoint,
            stop,
            server,
            ..
        } = harness().await;
        batch.operations[0].op.length = 2;
        native.close_connect_gate();
        let first = {
            let reader = reader.clone();
            let batch = batch.clone();
            tokio::spawn(async move {
                tokio::task::spawn_blocking(move || {
                    let mut output = vec![b'!'; 3];
                    reader.read_ranges(&batch, &mut output)?;
                    Ok::<_, afs_error::Error>(output)
                })
                .await
                .unwrap()
            })
        };
        native.wait_connect_entered(1).await;
        let deadline = Instant::now() + Duration::from_millis(100);
        let acquired = reader
            .context
            .acquire(
                "source",
                9,
                &endpoint,
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
        native.open_connect_gate();
        let first = first.await.unwrap().unwrap();
        assert_eq!(&first[1..], b"ab");
        stop.send(()).unwrap();
        tokio::time::timeout(Duration::from_secs(1), server)
            .await
            .unwrap()
            .unwrap();
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn settling_a_warm_slot_returns_when_the_task_clears_the_flag() {
        let Harness {
            reader,
            batch,
            stop,
            server,
            ..
        } = harness().await;
        let context = reader.context.clone();
        let crate::dfs::CopyLocation::Node {
            node_id,
            node_epoch,
            ..
        } = &batch.operations[0].source.location
        else {
            panic!("loopback source must be a node copy");
        };
        let address = batch.operations[0]
            .source
            .node_data_endpoint()
            .unwrap()
            .to_owned();
        let key = (node_id.clone(), *node_epoch, address.clone());
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
        tokio::time::timeout(
            Duration::from_secs(1),
            context.settle_warm(node_id, *node_epoch, &address, deadline),
        )
        .await
        .expect("settle must observe the cleared warm flag")
        .unwrap();
        release.await.unwrap();
        stop.send(()).unwrap();
        tokio::time::timeout(Duration::from_secs(1), server)
            .await
            .unwrap()
            .unwrap();
    }

    async fn wait_warm_session(reader: &HixlChunkTransfer, batch: &PeerReadBatch) {
        let crate::dfs::CopyLocation::Node {
            node_id,
            node_epoch,
            ..
        } = &batch.operations[0].source.location
        else {
            panic!("loopback source must be a node copy");
        };
        let address = batch.operations[0]
            .source
            .node_data_endpoint()
            .unwrap()
            .to_owned();
        let key = (node_id.clone(), *node_epoch, address);
        for _ in 0..300 {
            let slot = {
                let slots = reader.context.slots.lock().await;
                slots.get(&key).cloned()
            };
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
    async fn prepared_group_submits_one_native_batch_and_releases_every_window() {
        let Harness {
            _temp,
            native,
            source,
            reader,
            batch,
            stop,
            server,
            ..
        } = group_harness().await;
        let authority = read_authority(&batch);
        let context = reader.context.clone();
        let deadline = Instant::now() + Duration::from_secs(5);
        let crate::dfs::CopyLocation::Node {
            node_id,
            node_epoch,
            ..
        } = &batch.operations[0].source.location
        else {
            panic!("loopback source must be a node copy");
        };
        let address = batch.operations[0]
            .source
            .node_data_endpoint()
            .unwrap()
            .to_owned();
        let lease = match context
            .acquire(node_id, *node_epoch, &address, &authority, deadline)
            .await
            .unwrap()
        {
            Acquired::Lease(lease) => lease,
            Acquired::Unsupported => panic!("loopback HIXL must be supported"),
        };
        let session = lease.session.clone();
        let packed = pack_read_batch(authority).unwrap();
        let windows = pack_windows(packed.clone(), 4).unwrap();
        assert_eq!(windows.len(), 2);
        let mut prepared = Vec::new();
        let mut offset = 0_u64;
        for (ordinal, window) in windows.into_iter().enumerate() {
            let reply = expect_window(
                context
                    .prepare_window(&session, &window, &packed, offset, 0, deadline)
                    .await,
                "group window preparation",
            );
            offset += u64::try_from(window.length).unwrap();
            prepared.push((ordinal, window, reply, 0_usize));
        }
        let mut output = vec![0_u8; 8];
        let chunks = expect_window(
            context
                .read_prepared_group(&session, prepared, output.len(), deadline, None)
                .await,
            "group native read",
        );
        scatter_prepared(chunks, &mut output).unwrap();

        assert_eq!(output, b"abcdefgh");
        assert_eq!(native.batch_reads.load(Ordering::SeqCst), 1);
        let sessions = source_hixl::test_support::sessions(&source);
        assert_eq!(sessions.len(), 1);
        assert_eq!(sessions[0].prepared_bytes, 0);
        assert_eq!(sessions[0].active_windows, 0);
        assert!(native.memory.lock().unwrap().active.is_empty());
        drop(lease);
        stop.send(()).unwrap();
        tokio::time::timeout(Duration::from_secs(1), server)
            .await
            .unwrap()
            .unwrap();
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn baseline_group_aggregates_one_native_batch() {
        use crate::node::dfs_read::ReadGroupPolicy;
        let Harness {
            _temp,
            native,
            source,
            reader,
            batch,
            stop,
            server,
            ..
        } = harness_with_capabilities(
            1,
            4,
            8,
            HixlDfsReadMode::Required,
            true,
            Some(ReadGroupPolicy {
                bytes: 8,
                max_inflight: 1,
                max_descriptors: 8,
                host_budget_bytes: 1024 * 1024,
            }),
        )
        .await;
        let authority = pack_read_batch(read_authority(&batch)).unwrap();
        let context = reader.context.clone();
        let deadline = Instant::now() + Duration::from_secs(5);
        let crate::dfs::CopyLocation::Node {
            node_id,
            node_epoch,
            ..
        } = &batch.operations[0].source.location
        else {
            panic!("loopback source must be a node copy");
        };
        let address = batch.operations[0]
            .source
            .node_data_endpoint()
            .unwrap()
            .to_owned();
        // A grouped wave must still submit one Prepare round trip and one
        // native batch instead of one SDK read per window.
        let result = context
            .read_batch(
                (node_id.clone(), *node_epoch),
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
            ReadResult::PreparedComplete(chunks) => {
                scatter_prepared(chunks, &mut output).unwrap();
            }
            _ => panic!("grouped baseline read must use the native batch path"),
        }
        assert_eq!(output, b"abcdefgh");
        assert_eq!(native.batch_reads.load(Ordering::SeqCst), 1);
        let sessions = source_hixl::test_support::sessions(&source);
        assert_eq!(sessions.len(), 1);
        assert_eq!(sessions[0].active_windows, 0);
        assert!(native.memory.lock().unwrap().active.is_empty());
        stop.send(()).unwrap();
        tokio::time::timeout(Duration::from_secs(1), server)
            .await
            .unwrap()
            .unwrap();
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn group_prepare_admits_all_windows_from_one_snapshot() {
        let Harness {
            _temp,
            native,
            source,
            reader,
            batch,
            stop,
            server,
            ..
        } = group_harness().await;
        let authority = read_authority(&batch);
        let context = reader.context.clone();
        let deadline = Instant::now() + Duration::from_secs(5);
        let crate::dfs::CopyLocation::Node {
            node_id,
            node_epoch,
            ..
        } = &batch.operations[0].source.location
        else {
            panic!("loopback source must be a node copy");
        };
        let address = batch.operations[0]
            .source
            .node_data_endpoint()
            .unwrap()
            .to_owned();
        let lease = match context
            .acquire(node_id, *node_epoch, &address, &authority, deadline)
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
            .enumerate()
            .map(|(ordinal, window)| {
                let start = offset;
                offset += u64::try_from(window.length).unwrap();
                (ordinal, window, start, 0_usize)
            })
            .collect::<Vec<_>>();
        let prepared = expect_window(
            context
                .prepare_group_windows(&session, &planned, &packed, deadline)
                .await,
            "one group prepare admits every window",
        );
        assert_eq!(prepared.len(), 2);
        let sessions = source_hixl::test_support::sessions(&source);
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
        assert_eq!(native.batch_reads.load(Ordering::SeqCst), 1);
        let sessions = source_hixl::test_support::sessions(&source);
        assert_eq!(sessions[0].active_windows, 0);
        assert_eq!(sessions[0].prepared_bytes, 0);
        assert!(native.memory.lock().unwrap().active.is_empty());
        drop(lease);
        stop.send(()).unwrap();
        tokio::time::timeout(Duration::from_secs(1), server)
            .await
            .unwrap()
            .unwrap();
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn later_wave_prepare_refusal_closes_snapshot_and_allows_fresh_session() {
        let Harness {
            _temp,
            native,
            source,
            reader,
            batch,
            endpoint,
            stop,
            server,
        } = harness_with(2, 2, 2).await;
        let failed_reader = reader.clone();
        let failed_batch = batch.clone();
        let (failed, output) = tokio::task::spawn_blocking(move || {
            let mut output = vec![b'!'; 10];
            let result = failed_reader.read_ranges(&failed_batch, &mut output);
            (result, output)
        })
        .await
        .unwrap();
        assert_eq!(
            failed.unwrap_err().code(),
            afs_error::NODE_TRANSFER_RESOURCE_EXHAUSTED
        );
        assert_eq!(output, b"!!!!!!!!!!");
        let sessions = source_hixl::test_support::sessions(&source);
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
        assert_eq!(
            retired
                .outcomes
                .iter()
                .filter(|&&outcome| outcome == DfsHixlReleaseOutcome::Completed as i32)
                .count(),
            2
        );
        assert_eq!(
            retired
                .outcomes
                .iter()
                .filter(|&&outcome| outcome == DfsHixlReleaseOutcome::NotSubmitted as i32)
                .count(),
            1
        );
        let key = ("source".into(), 9, endpoint);
        let slot = reader.context.slots.lock().await.get(&key).unwrap().clone();
        assert!(slot.lock().await.session.is_none());
        {
            let memory = native.memory.lock().unwrap();
            assert!(memory.active.is_empty());
            assert_eq!(
                memory.reads.len(),
                2,
                "the refused wave must never enter READ"
            );
        }
        assert_eq!(native.reader_quarantines.load(Ordering::SeqCst), 0);
        assert_eq!(native.source_quarantines.load(Ordering::SeqCst), 0);

        let recovered_reader = reader.clone();
        let (recovered, output) = tokio::task::spawn_blocking(move || {
            let mut output = vec![b'!'; 10];
            let result = recovered_reader.read_ranges(&batch, &mut output);
            (result, output)
        })
        .await
        .unwrap();
        recovered.unwrap();
        assert_eq!(output, b"!abcdefgh!");
        let sessions = source_hixl::test_support::sessions(&source);
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
        assert!(native.memory.lock().unwrap().active.is_empty());
        assert_eq!(native.memory.lock().unwrap().reads.len(), 6);
        assert_eq!(native.reader_quarantines.load(Ordering::SeqCst), 0);
        assert_eq!(native.source_quarantines.load(Ordering::SeqCst), 0);
        stop.send(()).unwrap();
        tokio::time::timeout(Duration::from_secs(1), server)
            .await
            .unwrap()
            .unwrap();
    }
}
