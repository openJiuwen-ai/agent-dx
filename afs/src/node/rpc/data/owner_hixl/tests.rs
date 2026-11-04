use super::test_support::{MemoryExports, MemorySource, OPEN_HANDLE};
use super::*;
use afs_protocol::node_data::{OwnerHandle, RootAccess};

const SLOT: usize = 4096;

fn access(root: &str) -> RootAccess {
    RootAccess {
        root_id: root.to_owned(),
        root_epoch: 1,
        access_generation: 1,
        holder_node_id: "node-b".to_owned(),
        home_node_id: "node-a".to_owned(),
        session_id: "grant-session-b".to_owned(),
        fencing_token: "fence-a".to_owned(),
        home_session_id: "home-session-a".to_owned(),
    }
}

fn fixture(
    file_bytes: usize,
    slots: usize,
) -> (OwnerHixlService, Arc<MemoryExports>, Arc<MemorySource>) {
    let exports = Arc::new(MemoryExports::new(SLOT, slots));
    let data = (0..file_bytes).map(|index| (index % 251) as u8).collect();
    // A 3000-byte source chunk proves prepare loops until the window is full.
    let source = Arc::new(MemorySource::new(data, 3000));
    let service = OwnerHixlService::from_parts(exports.clone(), source.clone());
    (service, exports, source)
}

fn negotiate_request(root: &str) -> OwnerNegotiateHixlRequest {
    OwnerNegotiateHixlRequest {
        access: Some(access(root)),
        client_engine_id: "127.0.0.1:7940".to_owned(),
        client_engine_generation: 3,
        protocol_version: HIXL_PROTOCOL_VERSION,
        transport: afs_protocol::node_data::DfsHixlTransport::Roce as i32,
        buffer_pool_count: 0,
        buffer_pool_size_mib: 0,
        hccs_domain_id: None,
        lane_count: 1,
    }
}

fn prepare_request(
    session_id: u64,
    read_id: &str,
    offset: u64,
    length: u64,
) -> OwnerPrepareReadHixlRequest {
    OwnerPrepareReadHixlRequest {
        session_id,
        read_id: read_id.to_owned(),
        access: Some(access("root-a")),
        handle: Some(OwnerHandle {
            opaque: OPEN_HANDLE.to_vec(),
        }),
        offset,
        length,
        lane_id: 0,
        slot_bytes: SLOT as u64,
    }
}

fn release_request(
    reply: &OwnerPrepareReadHixlReply,
    outcome: DfsHixlReleaseOutcome,
) -> OwnerReleaseHixlReadRequest {
    OwnerReleaseHixlReadRequest {
        session_id: reply.session_id,
        read_id: reply.read_id.clone(),
        engine_generation: reply.engine_generation,
        outcome: outcome as i32,
        content_validated: outcome == DfsHixlReleaseOutcome::Completed,
    }
}

fn session(service: &OwnerHixlService) -> u64 {
    service
        .negotiate_blocking("node-b", negotiate_request("root-a"))
        .unwrap()
        .session_id
}

#[test]
fn negotiate_authorizes_the_presented_grant_and_reports_lanes() {
    let (service, _, _) = fixture(SLOT, 4);
    let reply = service
        .negotiate_blocking("node-b", negotiate_request("root-a"))
        .unwrap();
    assert_ne!(reply.session_id, 0);
    assert_eq!(reply.home_session_id, "home-session-a");
    assert_eq!(reply.lanes.len(), 1);
    assert_eq!(reply.lanes[0].export_slots, 4);
    assert_eq!(reply.lanes[0].window_bytes, SLOT as u64);

    let denied = service
        .negotiate_blocking("node-b", negotiate_request("root-x"))
        .unwrap_err();
    assert!(denied.message().contains("unknown root"));
    let mut old = negotiate_request("root-a");
    old.protocol_version += 1;
    assert!(service.negotiate_blocking("node-b", old).is_err());
    let mut lanes = negotiate_request("root-a");
    lanes.lane_count = 2;
    assert!(service.negotiate_blocking("node-b", lanes).is_err());
}

#[test]
fn prepare_fills_contiguous_slots_with_digests_and_reports_eof() {
    let (service, exports, source) = fixture(10_000, 4);
    let session_id = session(&service);
    let reply = service
        .prepare_blocking("node-b", prepare_request(session_id, "r1", 1000, 16_384))
        .unwrap();
    assert_eq!(reply.read, 9000);
    assert!(reply.eof);
    assert_eq!(reply.engine_generation, 7);
    let lengths = reply
        .slots
        .iter()
        .map(|slot| slot.length)
        .collect::<Vec<_>>();
    assert_eq!(lengths, vec![4096, 4096, 808]);
    let data = source.data.lock().unwrap().clone();
    let mut offset = 1000_usize;
    for slot in &reply.slots {
        let length = slot.length as usize;
        let bytes = exports.read(slot.address, length).unwrap();
        assert_eq!(bytes, data[offset..offset + length]);
        assert_eq!(slot.checksum, blake3::hash(&bytes).as_bytes().to_vec());
        offset += length;
    }
    assert!(source.reads.load(Ordering::SeqCst) >= 3);
    assert_eq!(exports.active(), 3);
}

#[test]
fn prepare_cuts_slots_to_the_requested_size_within_the_lane_window() {
    let (service, exports, _) = fixture(10_000, 8);
    let session_id = session(&service);
    let mut request = prepare_request(session_id, "small-slots", 0, 8192);
    request.slot_bytes = 2048;
    let reply = service.prepare_blocking("node-b", request).unwrap();
    assert_eq!(
        reply
            .slots
            .iter()
            .map(|slot| slot.length)
            .collect::<Vec<_>>(),
        vec![2048; 4],
        "a reader with a smaller window gets slots it can READ"
    );
    for slot_bytes in [0, SLOT as u64 + 1] {
        let mut request = prepare_request(session_id, "bad-slots", 0, 4096);
        request.slot_bytes = slot_bytes;
        let error = service.prepare_blocking("node-b", request).unwrap_err();
        assert_eq!(error.code(), tonic::Code::InvalidArgument, "{slot_bytes}");
    }
    assert_eq!(exports.active(), 4);
}

#[test]
fn prepare_at_eof_exports_nothing() {
    let (service, exports, _) = fixture(100, 4);
    let session_id = session(&service);
    let reply = service
        .prepare_blocking("node-b", prepare_request(session_id, "r1", 100, 4096))
        .unwrap();
    assert_eq!(reply.read, 0);
    assert!(reply.eof);
    assert!(reply.slots.is_empty());
    assert_eq!(exports.exports.load(Ordering::SeqCst), 0);
}

#[test]
fn prepare_rejects_stale_handles_and_foreign_peers_before_reading() {
    let (service, exports, source) = fixture(SLOT, 4);
    let session_id = session(&service);
    let mut stale = prepare_request(session_id, "r1", 0, 4096);
    stale.handle = Some(OwnerHandle {
        opaque: b"other".to_vec(),
    });
    assert!(service.prepare_blocking("node-b", stale).is_err());
    assert!(
        service
            .prepare_blocking("node-c", prepare_request(session_id, "r2", 0, 4096))
            .is_err()
    );
    assert!(
        service
            .prepare_blocking("node-b", prepare_request(session_id + 9, "r3", 0, 4096))
            .is_err()
    );
    assert_eq!(source.reads.load(Ordering::SeqCst), 0);
    assert_eq!(exports.exports.load(Ordering::SeqCst), 0);
}

#[test]
fn identical_replay_reuses_admitted_slots_and_different_replay_is_rejected() {
    let (service, exports, source) = fixture(SLOT * 2, 4);
    let session_id = session(&service);
    let first = service
        .prepare_blocking("node-b", prepare_request(session_id, "r1", 0, 8192))
        .unwrap();
    let reads = source.reads.load(Ordering::SeqCst);
    let replay = service
        .prepare_blocking("node-b", prepare_request(session_id, "r1", 0, 8192))
        .unwrap();
    assert_eq!(first, replay);
    assert_eq!(source.reads.load(Ordering::SeqCst), reads);
    assert_eq!(exports.exports.load(Ordering::SeqCst), 2);
    assert!(
        service
            .prepare_blocking("node-b", prepare_request(session_id, "r1", 4096, 4096))
            .is_err()
    );
}

#[test]
fn slot_capacity_is_checked_before_reading_the_source() {
    let (service, _, source) = fixture(SLOT * 4, 3);
    let session_id = session(&service);
    let too_large = service
        .prepare_blocking("node-b", prepare_request(session_id, "r1", 0, 16_384))
        .unwrap_err();
    assert!(too_large.message().contains("export pool"));
    service
        .prepare_blocking("node-b", prepare_request(session_id, "r2", 0, 8192))
        .unwrap();
    let reads = source.reads.load(Ordering::SeqCst);
    let busy = service
        .prepare_blocking("node-b", prepare_request(session_id, "r3", 8192, 8192))
        .unwrap_err();
    assert!(busy.message().contains("in use"));
    assert_eq!(source.reads.load(Ordering::SeqCst), reads);
}

#[test]
fn failed_export_rolls_back_earlier_slots_without_quarantine() {
    let (service, exports, _) = fixture(SLOT * 3, 4);
    exports.fail_export_at.store(3, Ordering::SeqCst);
    let session_id = session(&service);
    assert!(
        service
            .prepare_blocking("node-b", prepare_request(session_id, "r1", 0, 12_288))
            .is_err()
    );
    assert_eq!(exports.active(), 0);
    assert_eq!(exports.releases.load(Ordering::SeqCst), 2);
    assert_eq!(exports.quarantines.load(Ordering::SeqCst), 0);
}

#[test]
fn completed_release_frees_slots_and_duplicates_hit_the_tombstone() {
    let (service, exports, _) = fixture(SLOT * 2, 4);
    let session_id = session(&service);
    let reply = service
        .prepare_blocking("node-b", prepare_request(session_id, "r1", 0, 8192))
        .unwrap();
    let release = release_request(&reply, DfsHixlReleaseOutcome::Completed);
    let first = service.release_blocking("node-b", release.clone()).unwrap();
    assert!(first.consumed && !first.already_released);
    assert_eq!(exports.active(), 0);
    let duplicate = service.release_blocking("node-b", release).unwrap();
    assert!(duplicate.consumed && duplicate.already_released);
    // A released read id cannot be prepared again in the same session.
    assert!(
        service
            .prepare_blocking("node-b", prepare_request(session_id, "r1", 0, 8192))
            .is_err()
    );
    let closed = service
        .close_blocking("node-b", OwnerCloseHixlRequest { session_id })
        .unwrap();
    assert!(closed.drained);
    assert_eq!(exports.quarantines.load(Ordering::SeqCst), 0);
}

#[test]
fn unknown_outcome_keeps_slots_and_freezes_admission() {
    let (service, exports, _) = fixture(SLOT, 4);
    let session_id = session(&service);
    let reply = service
        .prepare_blocking("node-b", prepare_request(session_id, "r1", 0, 4096))
        .unwrap();
    let released = service
        .release_blocking(
            "node-b",
            release_request(&reply, DfsHixlReleaseOutcome::Unknown),
        )
        .unwrap();
    assert!(!released.consumed);
    assert_eq!(exports.active(), 1);
    assert_eq!(exports.quarantines.load(Ordering::SeqCst), 1);
    assert!(
        service
            .prepare_blocking("node-b", prepare_request(session_id, "r2", 0, 4096))
            .is_err()
    );
    assert!(
        service
            .negotiate_blocking("node-b", negotiate_request("root-a"))
            .is_err()
    );
}

#[test]
fn failed_native_release_quarantines_and_keeps_the_record() {
    let (service, exports, _) = fixture(SLOT, 4);
    let session_id = session(&service);
    let reply = service
        .prepare_blocking("node-b", prepare_request(session_id, "r1", 0, 4096))
        .unwrap();
    exports.fail_release.store(true, Ordering::SeqCst);
    assert!(
        service
            .release_blocking(
                "node-b",
                release_request(&reply, DfsHixlReleaseOutcome::Completed)
            )
            .is_err()
    );
    assert_eq!(exports.active(), 1);
    assert_eq!(exports.quarantines.load(Ordering::SeqCst), 1);
}

#[test]
fn closing_with_unreleased_slots_quarantines_the_runtime() {
    let (service, exports, _) = fixture(SLOT, 4);
    let session_id = session(&service);
    service
        .prepare_blocking("node-b", prepare_request(session_id, "r1", 0, 4096))
        .unwrap();
    let closed = service
        .close_blocking("node-b", OwnerCloseHixlRequest { session_id })
        .unwrap();
    assert!(!closed.drained);
    assert_eq!(exports.quarantines.load(Ordering::SeqCst), 1);
    assert_eq!(exports.active(), 1);
}

#[tokio::test]
async fn failed_expiry_quarantine_keeps_admission_frozen_until_a_retry_succeeds() {
    let (service, exports, _) = fixture(SLOT, 4);
    let session_id = session(&service);
    service
        .prepare_blocking("node-b", prepare_request(session_id, "r1", 0, 4096))
        .unwrap();
    for session in service.registry.lock().unwrap().sessions.values_mut() {
        session.last_used = Instant::now() - OWNER_HIXL_SESSION_TTL;
    }
    exports.fail_quarantine.store(true, Ordering::SeqCst);
    assert!(service.expire().await.is_err());
    {
        let registry = service.registry.lock().unwrap();
        assert!(registry.admission_frozen);
        assert!(!registry.runtime_quarantined);
    }
    // A failed quarantine must not reopen admission for any session.
    assert!(
        service
            .negotiate_blocking("node-b", negotiate_request("root-a"))
            .is_err()
    );
    service.expire().await.unwrap();
    let registry = service.registry.lock().unwrap();
    assert!(registry.admission_frozen && registry.runtime_quarantined);
    assert_eq!(exports.quarantines.load(Ordering::SeqCst), 2);
    assert_eq!(exports.active(), 1);
}

#[tokio::test]
async fn failed_forced_quarantine_is_retried_by_the_next_expiry() {
    let (service, exports, _) = fixture(SLOT, 4);
    let session_id = session(&service);
    let reply = service
        .prepare_blocking("node-b", prepare_request(session_id, "r1", 0, 4096))
        .unwrap();
    exports.fail_quarantine.store(true, Ordering::SeqCst);
    assert!(
        service
            .release_blocking(
                "node-b",
                release_request(&reply, DfsHixlReleaseOutcome::Unknown),
            )
            .is_err()
    );
    service.expire().await.unwrap();
    let registry = service.registry.lock().unwrap();
    assert!(registry.admission_frozen && registry.runtime_quarantined);
    assert_eq!(exports.quarantines.load(Ordering::SeqCst), 2);
    assert_eq!(exports.active(), 1);
}

#[tokio::test]
async fn admitted_prepare_replays_after_admission_stops() {
    let (service, exports, _) = fixture(SLOT, 4);
    let session_id = session(&service);
    let request = prepare_request(session_id, "r1", 0, 4096);
    let admitted = service.prepare_blocking("node-b", request.clone()).unwrap();
    // A drain timeout stops admission and quarantines the runtime.
    service.drain(Duration::ZERO).await.unwrap_err();
    // A retry whose first reply was lost still learns its pinned slots.
    assert_eq!(
        service.prepare_blocking("node-b", request).unwrap(),
        admitted
    );
    let refused = service
        .prepare_blocking("node-b", prepare_request(session_id, "r2", 0, 4096))
        .unwrap_err();
    assert_eq!(
        afs_transport::grpc::error_status::status_to_error(refused).code(),
        afs_error::NODE_TRANSFER_UNAVAILABLE
    );
    assert_eq!(exports.active(), 1);
}

#[tokio::test]
async fn drain_accepts_releases_and_times_out_with_pending_slots() {
    let (service, exports, _) = fixture(SLOT, 4);
    let session_id = session(&service);
    let reply = service
        .prepare_blocking("node-b", prepare_request(session_id, "r1", 0, 4096))
        .unwrap();
    let error = service.drain(Duration::from_millis(30)).await.unwrap_err();
    assert!(error.message().contains("drain"));
    assert_eq!(exports.quarantines.load(Ordering::SeqCst), 1);
    service
        .release_blocking(
            "node-b",
            release_request(&reply, DfsHixlReleaseOutcome::Completed),
        )
        .unwrap();
    service.drain(Duration::from_millis(30)).await.unwrap();
    assert_eq!(exports.active(), 0);
}
