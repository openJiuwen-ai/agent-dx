use super::*;
use afs_protocol::node_data::{
    OwnerAbortWriteHixlRequest, OwnerFinishWriteHixlRequest, OwnerPrepareWriteHixlReply,
    OwnerPrepareWriteHixlRequest, owner_abort_write_hixl_reply,
};
use afs_transport::grpc::error_status::status_to_error;

fn request(session_id: u64, data: &[u8]) -> OwnerPrepareWriteHixlRequest {
    OwnerPrepareWriteHixlRequest {
        session_id,
        write_id: "write-1".into(),
        access: Some(access("root-a")),
        handle: Some(OwnerHandle {
            opaque: OPEN_HANDLE.to_vec(),
        }),
        offset: 0,
        length: data.len().try_into().unwrap(),
        kill_suidgid: true,
        data_checksum: SlotChecksum::Blake3.digest(data),
        data_checksum_algorithm: HixlSlotChecksumAlgorithm::Blake3 as i32,
        lane_id: 0,
        slot_bytes: SLOT as u64,
    }
}

async fn prepare(
    service: &OwnerHixlService,
    request: OwnerPrepareWriteHixlRequest,
) -> OwnerPrepareWriteHixlReply {
    service
        .prepare_write("node-b".into(), request)
        .await
        .unwrap()
        .into_inner()
}

fn commit(reply: &OwnerPrepareWriteHixlReply) -> OwnerFinishWriteHixlRequest {
    OwnerFinishWriteHixlRequest {
        session_id: reply.session_id,
        write_id: reply.write_id.clone(),
        engine_generation: reply.engine_generation,
        ..Default::default()
    }
}

fn abort(reply: &OwnerPrepareWriteHixlReply, unknown: bool) -> OwnerAbortWriteHixlRequest {
    OwnerAbortWriteHixlRequest {
        session_id: reply.session_id,
        write_id: reply.write_id.clone(),
        engine_generation: reply.engine_generation,
        unknown,
        cancel_next: false,
        recover_commit: None,
    }
}

#[tokio::test]
async fn write_replays_one_append_and_preserves_the_actual_short_write() {
    let (service, exports, source) = fixture(0, 2);
    let registry = afs_metrics::Registry::new();
    let service =
        service.with_owner_metrics(crate::node::rpc::OwnerRpcMetrics::register(&registry).unwrap());
    source.append_writes.store(true, Ordering::SeqCst);
    let payload: Vec<u8> = (0..SLOT + 17).map(|byte| (byte % 251) as u8).collect();
    let request = request(session(&service), &payload);
    let mut unspecified = request.clone();
    unspecified.data_checksum_algorithm = HixlSlotChecksumAlgorithm::Unspecified as i32;
    assert_eq!(
        service
            .prepare_write("node-b".into(), unspecified)
            .await
            .unwrap_err()
            .code(),
        tonic::Code::InvalidArgument
    );
    let reply = prepare(&service, request.clone()).await;
    assert_eq!(reply.slots.len(), 2);
    assert_eq!(
        service
            .prepare_write("node-b".into(), request.clone())
            .await
            .unwrap()
            .into_inner(),
        reply
    );
    let mut changed = request.clone();
    changed.offset = 1;
    assert_eq!(
        service
            .prepare_write("node-b".into(), changed)
            .await
            .unwrap_err()
            .code(),
        tonic::Code::InvalidArgument
    );
    for (slot, data) in reply.slots.iter().zip(payload.chunks(SLOT)) {
        exports.write(slot.address, data);
    }
    assert_eq!(
        service
            .commit_write("other-peer".into(), commit(&reply))
            .await
            .unwrap_err()
            .code(),
        tonic::Code::PermissionDenied
    );
    let mut wrong_generation = commit(&reply);
    wrong_generation.engine_generation += 1;
    assert!(
        service
            .commit_write("node-b".into(), wrong_generation)
            .await
            .is_err()
    );
    let mut finish = commit(&reply);
    finish.request_next = true;
    let (first, second) = tokio::join!(
        service.commit_write("node-b".into(), finish.clone()),
        service.commit_write("node-b".into(), finish.clone()),
    );
    for result in [first, second] {
        let result = result.unwrap().into_inner();
        assert_eq!(result.written, 3000);
        assert!(result.next_window.is_none());
    }
    let mut changed_finish = finish.clone();
    changed_finish.request_next = false;
    assert!(
        service
            .abort_write("node-b".into(), recover(&changed_finish))
            .await
            .is_err()
    );
    for _ in 0..2 {
        let reply = service
            .abort_write("node-b".into(), recover(&finish))
            .await
            .unwrap()
            .into_inner();
        assert_eq!(
            reply.recovery,
            Some(owner_abort_write_hixl_reply::Recovery::Written(3000))
        );
    }
    let counters = afs_metrics::encode_text(&registry).unwrap();
    assert!(counters.lines().any(
        |line| line.starts_with("afs_ownerfiles_payload_bytes_total{")
            && line.contains("side=\"server\"")
            && line.contains("direction=\"write\"")
            && line.contains("plane=\"hixl\"")
            && line.ends_with(" 3000")
    ));
    assert_eq!(*source.data.lock().unwrap(), payload[..3000]);
    assert_eq!(exports.active(), 0);
    assert!(
        service
            .prepare_write("node-b".into(), request)
            .await
            .is_err()
    );
}

fn recover(finish: &OwnerFinishWriteHixlRequest) -> OwnerAbortWriteHixlRequest {
    OwnerAbortWriteHixlRequest {
        session_id: finish.session_id,
        write_id: finish.write_id.clone(),
        engine_generation: finish.engine_generation,
        cancel_next: true,
        recover_commit: Some(finish.clone()),
        ..Default::default()
    }
}

#[tokio::test]
async fn recovery_fences_late_commit_and_can_replay_the_fence() {
    let (service, exports, source) = fixture(0, 1);
    let reply = prepare(&service, request(session(&service), b"payload")).await;
    exports.write(reply.slots[0].address, b"payload");
    let finish = commit(&reply);
    let recovery = recover(&finish);
    assert!(
        service
            .abort_write("node-c".into(), recovery.clone())
            .await
            .is_err()
    );
    let mut wrong_generation = recovery.clone();
    wrong_generation.engine_generation += 1;
    wrong_generation
        .recover_commit
        .as_mut()
        .unwrap()
        .engine_generation += 1;
    assert!(
        service
            .abort_write("node-b".into(), wrong_generation)
            .await
            .is_err()
    );
    assert_eq!(exports.active(), 1);
    for _ in 0..2 {
        let receipt = service
            .abort_write("node-b".into(), recovery.clone())
            .await
            .unwrap()
            .into_inner();
        assert!(matches!(
            receipt.recovery,
            Some(owner_abort_write_hixl_reply::Recovery::Failure(_))
        ));
        let status = service
            .commit_write("node-b".into(), finish.clone())
            .await
            .unwrap_err();
        assert_eq!(
            status_to_error(status).code(),
            afs_error::NODE_OWNER_WRITE_NOT_EXECUTED
        );
    }
    assert_eq!(exports.active(), 0);
    assert!(source.data.lock().unwrap().is_empty());
}

#[tokio::test]
async fn recovery_fences_reused_window_and_rejects_a_mismatched_binding() {
    let (service, exports, source) = fixture(0, 1);
    let mut first = request(session(&service), b"a");
    let reply = prepare(&service, first.clone()).await;
    exports.write(reply.slots[0].address, b"a");
    let mut finish = commit(&reply);
    finish.request_next = true;
    let window = service
        .commit_write("node-b".into(), finish)
        .await
        .unwrap()
        .into_inner()
        .next_window
        .unwrap();
    exports.write(window.slots[0].address, b"a");
    first.write_id = "write-2".into();
    first.offset = 1;
    let reused = OwnerFinishWriteHixlRequest {
        write_id: first.write_id.clone(),
        reused: Some(first),
        window_token: window.write_id.clone(),
        ..commit(&window)
    };
    let mut unbound = reused.clone();
    unbound
        .reused
        .as_mut()
        .unwrap()
        .handle
        .as_mut()
        .unwrap()
        .opaque
        .push(0);
    let status = service
        .abort_write("node-b".into(), recover(&unbound))
        .await
        .unwrap_err();
    assert_eq!(status.code(), tonic::Code::PermissionDenied);
    assert_eq!(exports.active(), 1);
    assert!(matches!(
        service
            .abort_write("node-b".into(), recover(&reused))
            .await
            .unwrap()
            .into_inner()
            .recovery,
        Some(owner_abort_write_hixl_reply::Recovery::Failure(_))
    ));
    let status = service
        .commit_write("node-b".into(), reused)
        .await
        .unwrap_err();
    assert_eq!(
        status_to_error(status).code(),
        afs_error::NODE_OWNER_WRITE_NOT_EXECUTED
    );
    assert_eq!(exports.active(), 0);
    assert_eq!(*source.data.lock().unwrap(), b"a");
}

#[tokio::test]
async fn corrupt_write_replays_failure_without_modifying_file() {
    let (service, exports, source) = fixture(0, 1);
    let reply = prepare(&service, request(session(&service), b"good")).await;
    exports.write(reply.slots[0].address, b"evil");
    for _ in 0..2 {
        assert_eq!(
            status_to_error(
                service
                    .commit_write("node-b".into(), commit(&reply))
                    .await
                    .unwrap_err()
            )
            .code(),
            afs_error::NODE_OWNER_WRITE_NOT_EXECUTED_CHECKSUM
        );
    }
    assert!(source.data.lock().unwrap().is_empty());
    assert_eq!(exports.active(), 0);
}

#[tokio::test]
async fn unknown_abort_pins_slots_and_prevents_commit_and_drain() {
    let (service, exports, source) = fixture(0, 2);
    let reply = prepare(&service, request(session(&service), b"payload")).await;
    service
        .abort_write("node-b".into(), abort(&reply, true))
        .await
        .unwrap();
    service
        .abort_write("node-b".into(), abort(&reply, true))
        .await
        .unwrap();
    assert!(
        service
            .abort_write("node-b".into(), abort(&reply, false))
            .await
            .is_err()
    );
    assert!(
        service
            .commit_write("node-b".into(), commit(&reply))
            .await
            .is_err()
    );
    assert!(service.drain(Duration::ZERO).await.is_err());
    assert_eq!(exports.quarantines.load(Ordering::SeqCst), 1);
    assert_eq!(exports.active(), 1);
    assert!(source.data.lock().unwrap().is_empty());
}

#[tokio::test]
async fn known_abort_releases_slots_but_cannot_readmit_its_id() {
    let (service, exports, _) = fixture(0, 1);
    let request = request(session(&service), b"payload");
    let reply = prepare(&service, request.clone()).await;
    service
        .abort_write("node-b".into(), abort(&reply, false))
        .await
        .unwrap();
    service
        .abort_write("node-b".into(), abort(&reply, false))
        .await
        .unwrap();
    assert_eq!(exports.active(), 0);
    assert!(
        service
            .commit_write("node-b".into(), commit(&reply))
            .await
            .is_err()
    );
    assert!(
        service
            .prepare_write("node-b".into(), request)
            .await
            .is_err()
    );
    service.drain(Duration::ZERO).await.unwrap();
}

#[tokio::test]
async fn prepare_and_commit_both_check_home_write_authorization() {
    let (service, exports, source) = fixture(0, 1);
    let request = request(session(&service), b"payload");
    source.deny_writes.store(true, Ordering::SeqCst);
    assert!(
        service
            .prepare_write("node-b".into(), request.clone())
            .await
            .is_err()
    );
    assert_eq!(exports.active(), 0);
    source.deny_writes.store(false, Ordering::SeqCst);
    let reply = prepare(&service, request).await;
    exports.write(reply.slots[0].address, b"payload");
    source.deny_writes.store(true, Ordering::SeqCst);
    assert!(
        service
            .commit_write("node-b".into(), commit(&reply))
            .await
            .is_err()
    );
    source.deny_writes.store(false, Ordering::SeqCst);
    assert!(
        service
            .commit_write("node-b".into(), commit(&reply))
            .await
            .is_err()
    );
    assert_eq!(exports.active(), 0);
    assert!(source.data.lock().unwrap().is_empty());
}

#[tokio::test]
async fn write_only_grant_negotiates_and_prepares_writes_but_cannot_prepare_reads() {
    let (service, exports, source) = fixture(0, 1);
    source.deny_reads.store(true, Ordering::SeqCst);
    let reply = service
        .negotiate("node-b".into(), negotiate_request("root-a"))
        .await
        .unwrap()
        .into_inner();
    assert!(reply.buffered_write_supported);
    let error = service
        .prepare(
            "node-b".into(),
            prepare_request(reply.session_id, "read", 0, 1),
        )
        .await
        .unwrap_err();
    assert_eq!(error.code(), tonic::Code::PermissionDenied);
    assert_eq!(exports.active(), 0);
    let write = service
        .prepare_write("node-b".into(), request(reply.session_id, b"payload"))
        .await
        .unwrap()
        .into_inner();
    service
        .abort_write("node-b".into(), abort(&write, false))
        .await
        .unwrap();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn abort_prevents_a_preparing_worker_from_publishing_slots() {
    let (service, exports, source) = fixture(0, 1);
    let request = request(session(&service), b"payload");
    let (entered, resume) = exports.pause_next_export();
    let prepare = tokio::spawn({
        let service = service.clone();
        let request = request.clone();
        async move { service.prepare_write("node-b".into(), request).await }
    });
    tokio::task::spawn_blocking(move || entered.recv_timeout(Duration::from_secs(2)))
        .await
        .unwrap()
        .unwrap();
    service
        .abort_write(
            "node-b".into(),
            OwnerAbortWriteHixlRequest {
                session_id: request.session_id,
                write_id: request.write_id.clone(),
                engine_generation: exports.info().generation,
                unknown: false,
                cancel_next: false,
                recover_commit: None,
            },
        )
        .await
        .unwrap();
    resume.send(()).unwrap();
    assert!(prepare.await.unwrap().is_err());
    assert!(
        service
            .prepare_write("node-b".into(), request)
            .await
            .is_err()
    );
    assert_eq!(exports.active(), 0);
    assert!(source.data.lock().unwrap().is_empty());
}

#[tokio::test]
async fn close_with_outstanding_write_quarantines_its_slots() {
    let (service, exports, _) = fixture(0, 1);
    let reply = prepare(&service, request(session(&service), b"payload")).await;
    let close = service
        .close(
            "node-b".into(),
            OwnerCloseHixlRequest {
                session_id: reply.session_id,
                home_session_id: "home-session-a".to_owned(),
            },
        )
        .await
        .unwrap()
        .into_inner();
    assert!(!close.drained);
    assert!(
        service
            .commit_write("node-b".into(), commit(&reply))
            .await
            .is_err()
    );
    assert!(service.drain(Duration::ZERO).await.is_err());
    assert_eq!(exports.active(), 1);
    assert_eq!(exports.quarantines.load(Ordering::SeqCst), 1);
}

#[tokio::test]
async fn release_failure_keeps_the_completed_file_result_and_fences_the_runtime() {
    let (service, exports, source) = fixture(0, 1);
    source.append_writes.store(true, Ordering::SeqCst);
    let reply = prepare(&service, request(session(&service), b"payload")).await;
    exports.write(reply.slots[0].address, b"payload");
    exports.fail_release.store(true, Ordering::SeqCst);
    for _ in 0..2 {
        assert_eq!(
            service
                .commit_write("node-b".into(), commit(&reply))
                .await
                .unwrap()
                .into_inner()
                .written,
            7
        );
    }
    assert_eq!(*source.data.lock().unwrap(), b"payload");
    assert_eq!(exports.active(), 1);
    assert_eq!(exports.quarantines.load(Ordering::SeqCst), 1);
    assert!(service.drain(Duration::ZERO).await.is_err());
}

#[tokio::test]
async fn successor_reuses_slot_and_preserves_exact_commit_receipts() {
    const CAPACITY: usize = crate::node::storage::MAX_TRANSFER_BYTES;
    let exports = Arc::new(MemoryExports::new(CAPACITY, 1));
    let source = Arc::new(MemorySource::new(Vec::new(), CAPACITY));
    let service = OwnerHixlService::from_parts(exports.clone(), source.clone());
    source.append_writes.store(true, Ordering::SeqCst);
    let mut initial = request(session(&service), b"a");
    initial.slot_bytes = CAPACITY as u64;
    let prepared = prepare(&service, initial).await;
    assert_eq!(prepared.slots[0].length, 1);
    exports.write(prepared.slots[0].address, b"a");
    let mut first = commit(&prepared);
    first.request_next = true;
    let receipt = service
        .commit_write("node-b".into(), first.clone())
        .await
        .unwrap()
        .into_inner();
    let window = receipt.next_window.as_ref().unwrap();
    assert_eq!(window.slots[0].length, CAPACITY as u64);
    assert_eq!(window.slots[0].address, prepared.slots[0].address);
    assert_eq!(
        exports.read(window.slots[0].address, CAPACITY).unwrap(),
        vec![0; CAPACITY]
    );
    let payload = vec![b'b'; CAPACITY];
    let mut reused = request(window.session_id, &payload);
    reused.write_id = "write-2".into();
    reused.slot_bytes = CAPACITY as u64;
    let mut second = OwnerFinishWriteHixlRequest {
        session_id: window.session_id,
        write_id: reused.write_id.clone(),
        engine_generation: window.engine_generation,
        request_next: true,
        reused: Some(reused),
        window_token: window.write_id.clone(),
    };
    // A grant is not a prepared file operation.
    assert!(
        service
            .commit_write("node-b".into(), commit(window))
            .await
            .is_err()
    );
    let mut oversized = second.clone();
    oversized.reused.as_mut().unwrap().length += 1;
    assert!(
        service
            .commit_write("node-b".into(), oversized)
            .await
            .is_err()
    );
    let mut wrong_handle = second.clone();
    wrong_handle
        .reused
        .as_mut()
        .unwrap()
        .handle
        .as_mut()
        .unwrap()
        .opaque = b"other".to_vec();
    assert!(
        service
            .commit_write("node-b".into(), wrong_handle)
            .await
            .is_err()
    );
    let mut wrong_fence = second.clone();
    wrong_fence
        .reused
        .as_mut()
        .unwrap()
        .access
        .as_mut()
        .unwrap()
        .fencing_token
        .push('x');
    assert!(
        service
            .commit_write("node-b".into(), wrong_fence)
            .await
            .is_err()
    );
    let mut wrong_generation = second.clone();
    wrong_generation.engine_generation += 1;
    assert!(
        service
            .commit_write("node-b".into(), wrong_generation)
            .await
            .is_err()
    );
    exports.write(window.slots[0].address, &payload);
    let next = service
        .commit_write("node-b".into(), second.clone())
        .await
        .unwrap()
        .into_inner();
    assert_eq!(next.written, CAPACITY as u32);
    assert_eq!(
        service
            .commit_write("node-b".into(), second.clone())
            .await
            .unwrap()
            .into_inner(),
        next
    );
    assert_eq!(
        service
            .commit_write("node-b".into(), first)
            .await
            .unwrap()
            .into_inner(),
        receipt
    );
    second.reused.as_mut().unwrap().offset = 1;
    assert!(
        service
            .commit_write("node-b".into(), second.clone())
            .await
            .is_err()
    );
    second.write_id = "write-3".into();
    second.reused.as_mut().unwrap().write_id = second.write_id.clone();
    assert!(service.commit_write("node-b".into(), second).await.is_err());
    service
        .abort_write("node-b".into(), abort(window, false))
        .await
        .unwrap();
    assert_eq!(exports.active(), 1);
    service
        .abort_write("node-b".into(), abort(&next.next_window.unwrap(), false))
        .await
        .unwrap();
    assert_eq!(exports.active(), 0);
    assert_eq!(exports.exports.load(Ordering::SeqCst), 1);
    assert_eq!(
        *source.data.lock().unwrap(),
        [b"a".as_slice(), &payload].concat()
    );
    assert!(
        service
            .close(
                "node-b".into(),
                OwnerCloseHixlRequest {
                    session_id: window.session_id,
                    home_session_id: "home-session-a".to_owned()
                }
            )
            .await
            .unwrap()
            .into_inner()
            .drained
    );
    assert_eq!(exports.quarantines.load(Ordering::SeqCst), 0);
}

#[tokio::test]
async fn negotiated_crc32c_detects_corruption_against_known_vector() {
    let (service, exports, source) = fixture(0, 1);
    let mut offer = negotiate_request("root-a");
    offer.slot_checksum_algorithms = vec![
        HixlSlotChecksumAlgorithm::Crc32c as i32,
        HixlSlotChecksumAlgorithm::Blake3 as i32,
    ];
    let session_id = service
        .negotiate_blocking("node-b", offer)
        .unwrap()
        .session_id;
    let mut valid = request(session_id, b"123456789");
    valid.data_checksum_algorithm = HixlSlotChecksumAlgorithm::Crc32c as i32;
    valid.data_checksum = vec![0x83, 0x92, 0x06, 0xe3];
    for algorithm in [
        HixlSlotChecksumAlgorithm::Unspecified as i32,
        HixlSlotChecksumAlgorithm::Blake3 as i32,
        i32::MAX,
    ] {
        let mut invalid = valid.clone();
        invalid.data_checksum_algorithm = algorithm;
        invalid.data_checksum = SlotChecksum::Blake3.digest(b"123456789");
        assert_eq!(
            service
                .prepare_write("node-b".into(), invalid)
                .await
                .unwrap_err()
                .code(),
            tonic::Code::InvalidArgument
        );
    }
    let mut wrong_length = valid.clone();
    wrong_length.data_checksum.pop();
    assert!(
        service
            .prepare_write("node-b".into(), wrong_length)
            .await
            .is_err()
    );
    let prepared = prepare(&service, valid.clone()).await;
    exports.write(prepared.slots[0].address, b"123456789");
    let mut done = commit(&prepared);
    done.request_next = true;
    let receipt = service
        .commit_write("node-b".into(), done)
        .await
        .unwrap()
        .into_inner();
    let window = receipt.next_window.unwrap();
    valid.write_id = "write-2".into();
    let reused = OwnerFinishWriteHixlRequest {
        session_id: window.session_id,
        write_id: valid.write_id.clone(),
        engine_generation: window.engine_generation,
        request_next: true,
        reused: Some(valid),
        window_token: window.write_id,
    };
    for algorithm in [
        HixlSlotChecksumAlgorithm::Unspecified,
        HixlSlotChecksumAlgorithm::Blake3,
    ] {
        let mut mismatched = reused.clone();
        let metadata = mismatched.reused.as_mut().unwrap();
        metadata.data_checksum_algorithm = algorithm as i32;
        metadata.data_checksum = SlotChecksum::Blake3.digest(b"123456789");
        assert_eq!(
            service
                .commit_write("node-b".into(), mismatched)
                .await
                .unwrap_err()
                .code(),
            tonic::Code::InvalidArgument
        );
    }
    exports.write(window.slots[0].address, b"123456780");
    for _ in 0..2 {
        assert_eq!(
            service
                .commit_write("node-b".into(), reused.clone())
                .await
                .unwrap_err()
                .code(),
            tonic::Code::DataLoss
        );
    }
    assert_eq!(exports.active(), 0);
    assert_eq!(*source.data.lock().unwrap(), b"123456789");
}

#[tokio::test]
async fn unknown_abort_of_unused_successor_pins_window() {
    let (service, exports, source) = fixture(0, 1);
    let prepared = prepare(&service, request(session(&service), b"x")).await;
    exports.write(prepared.slots[0].address, b"x");
    let mut first = commit(&prepared);
    first.request_next = true;
    let grant = service
        .commit_write("node-b".into(), first)
        .await
        .unwrap()
        .into_inner()
        .next_window
        .unwrap();
    service
        .abort_write("node-b".into(), abort(&grant, true))
        .await
        .unwrap();
    assert!(
        service
            .abort_write("node-b".into(), abort(&grant, false))
            .await
            .is_err()
    );
    assert_eq!(exports.active(), 1);
    assert_eq!(exports.quarantines.load(Ordering::SeqCst), 1);
    assert_eq!(*source.data.lock().unwrap(), b"x");
}
