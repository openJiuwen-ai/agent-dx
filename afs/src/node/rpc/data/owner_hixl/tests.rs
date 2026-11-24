use super::test_support::{MemoryExports, MemorySource, OPEN_HANDLE, memory_home};
use super::*;
use afs_protocol::node_data::{HixlSlotChecksumAlgorithm, OwnerHandle, RootAccess};
use std::sync::atomic::Ordering;

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
    // A 3000-byte source chunk proves prepare loops until the window is full.
    memory_home(
        SLOT,
        slots,
        MemorySource::new(patterned(file_bytes, 0), 3000),
    )
}

fn negotiate_request(root: &str) -> OwnerNegotiateHixlRequest {
    OwnerNegotiateHixlRequest {
        access: Some(access(root)),
        client_engine_id: "127.0.0.1:7940".to_owned(),
        client_engine_generation: 3,
        protocol_version: HIXL_PROTOCOL_VERSION,
        transport: afs_protocol::node_data::HixlTransport::Roce as i32,
        buffer_pool_count: 0,
        buffer_pool_size_mib: 0,
        hccs_domain_id: None,
        lane_count: 1,
        slot_checksum_algorithms: Vec::new(),
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
        prior_release: None,
    }
}

// Callers exercising identity or request mutations use the raw RPC and builders.
fn prepare_window(
    service: &OwnerHixlService,
    session_id: u64,
    read_id: &str,
    offset: u64,
    length: u64,
) -> Result<OwnerPrepareReadHixlReply, Status> {
    service.prepare_blocking(
        "node-b",
        prepare_request(session_id, read_id, offset, length),
    )
}

fn prepare_page(
    service: &OwnerHixlService,
    session_id: u64,
    read_id: &str,
    anchor: &str,
    slots: usize,
) -> Result<OwnerPrepareBatchHixlReply, Status> {
    service.prepare_batch_blocking("node-b", batch_request(session_id, read_id, anchor, slots))
}

fn release_request(
    reply: &OwnerPrepareReadHixlReply,
    outcome: HixlReleaseOutcome,
) -> OwnerReleaseHixlReadRequest {
    OwnerReleaseHixlReadRequest {
        session_id: reply.session_id,
        read_id: reply.read_id.clone(),
        engine_generation: reply.engine_generation,
        outcome: outcome as i32,
        content_validated: outcome == HixlReleaseOutcome::Completed,
    }
}

fn session(service: &OwnerHixlService) -> u64 {
    service
        .negotiate_blocking("node-b", negotiate_request("root-a"))
        .unwrap()
        .session_id
}

#[test]
fn negotiation_reports_the_full_export_pool() {
    let (service, _, _) = fixture(SLOT, 128);
    let reply = service
        .negotiate_blocking("node-b", negotiate_request("root-a"))
        .unwrap();
    assert_eq!(reply.lanes[0].export_slots, 128);
}

#[test]
fn prepare_caps_descriptors_independently_of_the_export_pool() {
    let (service, exports, _) = batch_fixture(128);
    let session_id = session(&service);
    let window = prepare_request(session_id, "wide-window", 0, (65 * SLOT) as u64);
    assert_eq!(
        service
            .prepare_blocking("node-b", window)
            .unwrap_err()
            .code(),
        tonic::Code::InvalidArgument,
    );
    let batch = batch_request(session_id, "wide-batch", "", 65);
    assert_eq!(
        service
            .prepare_batch_blocking("node-b", batch)
            .unwrap_err()
            .code(),
        tonic::Code::InvalidArgument,
    );
    assert_eq!(exports.exports.load(Ordering::SeqCst), 0);
}

#[test]
fn a_late_window_prepare_cannot_publish_after_release() {
    let (service, exports, _) = fixture(SLOT, 4);
    let request = prepare_request(session(&service), "overlapping-window", 0, SLOT as u64);
    assert_late_prepare_rolls_back(
        &service,
        &exports,
        || service.prepare_blocking("node-b", request.clone()),
        |reply| release_request(&reply, HixlReleaseOutcome::Completed),
        false,
    );
}

#[test]
fn a_late_batch_prepare_cannot_publish_after_release() {
    let (service, exports, _) = batch_fixture(8);
    let request = batch_request(session(&service), "overlapping-batch", "", 1);
    assert_late_prepare_rolls_back(
        &service,
        &exports,
        || service.prepare_batch_blocking("node-b", request.clone()),
        |reply| OwnerReleaseHixlReadRequest {
            session_id: reply.session_id,
            read_id: reply.read_id,
            engine_generation: reply.engine_generation,
            outcome: HixlReleaseOutcome::Completed as i32,
            content_validated: true,
        },
        false,
    );
}

#[test]
fn a_late_prepare_cannot_publish_after_its_tombstone_expires() {
    let (service, exports, _) = fixture(SLOT, 4);
    let request = prepare_request(session(&service), "expired-tombstone", 0, SLOT as u64);
    assert_late_prepare_rolls_back(
        &service,
        &exports,
        || service.prepare_blocking("node-b", request.clone()),
        |reply| release_request(&reply, HixlReleaseOutcome::Completed),
        true,
    );
}

fn assert_late_prepare_rolls_back<R: std::fmt::Debug + Send>(
    service: &OwnerHixlService,
    exports: &MemoryExports,
    prepare: impl Fn() -> Result<R, Status> + Sync,
    release: impl FnOnce(R) -> OwnerReleaseHixlReadRequest,
    expire_tombstone: bool,
) {
    let (entered, resume) = exports.pause_next_export();
    std::thread::scope(|scope| {
        let late = scope.spawn(&prepare);
        entered.recv_timeout(Duration::from_secs(3)).unwrap();
        let release = release(prepare().unwrap());
        let reply = service.release_blocking("node-b", release.clone()).unwrap();
        assert!(reply.consumed && !reply.already_released);
        assert_eq!(exports.active(), 1);
        if expire_tombstone {
            let mut registry = service.lock_registry().unwrap();
            let session = registry.session_mut(release.session_id, "node-b").unwrap();
            session
                .tombstones
                .expire(Instant::now() + OWNER_HIXL_SESSION_TTL);
        }
        resume.send(()).unwrap();
        let error = late.join().unwrap().unwrap_err();
        assert_eq!(error.code(), tonic::Code::InvalidArgument);
        assert_eq!(exports.active(), 0);
        assert_eq!(exports.quarantines.load(Ordering::SeqCst), 0);
    });
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
    let reply = prepare_window(&service, session_id, "r1", 1000, 16_384).unwrap();
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
    let reply = prepare_window(&service, session_id, "r1", 100, 4096).unwrap();
    assert_eq!(reply.read, 0);
    assert!(reply.eof);
    assert!(reply.slots.is_empty());
    assert_eq!(exports.active(), 0);
}

#[test]
fn slots_past_a_short_read_are_released_before_the_reply() {
    let (service, exports, _) = fixture(SLOT + 100, 4);
    let session_id = session(&service);
    let reply = prepare_window(&service, session_id, "r1", 0, (SLOT * 4) as u64).unwrap();
    assert_eq!(reply.read, (SLOT + 100) as u64);
    assert!(reply.eof);
    assert_eq!(reply.slots.len(), 2);
    assert_eq!(exports.active(), 2);
    assert_eq!(exports.releases.load(Ordering::SeqCst), 2);
}

#[test]
fn a_full_export_pool_fails_the_prepare_before_reading_the_source() {
    let (service, exports, source) = fixture(SLOT * 2, 4);
    exports.fail_export_exhausted_at.store(2, Ordering::SeqCst);
    let session_id = session(&service);
    let error = prepare_window(&service, session_id, "r1", 0, (SLOT * 2) as u64).unwrap_err();
    assert!(error.message().contains("injected full export pool"));
    assert_eq!(exports.active(), 0);
    assert_eq!(source.reads.load(Ordering::SeqCst), 0);
    assert_eq!(exports.quarantines.load(Ordering::SeqCst), 0);
}

#[test]
fn a_failed_release_of_an_unused_slot_quarantines_the_runtime() {
    let (service, exports, source) = fixture(SLOT * 2, 4);
    source.fail_read_at(0, afs_error::IO_OTHER);
    exports.fail_release.store(true, Ordering::SeqCst);
    let session_id = session(&service);
    assert!(prepare_window(&service, session_id, "r1", 0, (SLOT * 2) as u64).is_err());
    assert_eq!(exports.quarantines.load(Ordering::SeqCst), 1);
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
    let first = prepare_window(&service, session_id, "r1", 0, 8192).unwrap();
    let reads = source.reads.load(Ordering::SeqCst);
    let replay = prepare_window(&service, session_id, "r1", 0, 8192).unwrap();
    assert_eq!(first, replay);
    assert_eq!(source.reads.load(Ordering::SeqCst), reads);
    assert_eq!(exports.exports.load(Ordering::SeqCst), 2);
    assert!(prepare_window(&service, session_id, "r1", 4096, 4096).is_err());
}

#[test]
fn slot_capacity_is_checked_before_reading_the_source() {
    let (service, _, source) = fixture(SLOT * 4, 3);
    let session_id = session(&service);
    let too_large = prepare_window(&service, session_id, "r1", 0, 16_384).unwrap_err();
    assert!(too_large.message().contains("export pool"));
    prepare_window(&service, session_id, "r2", 0, 8192).unwrap();
    let reads = source.reads.load(Ordering::SeqCst);
    let busy = prepare_window(&service, session_id, "r3", 8192, 8192).unwrap_err();
    assert!(busy.message().contains("in use"));
    assert_eq!(source.reads.load(Ordering::SeqCst), reads);
}

#[test]
fn failed_export_rolls_back_earlier_slots_without_quarantine() {
    let (service, exports, _) = fixture(SLOT * 3, 4);
    exports.fail_export_at.store(3, Ordering::SeqCst);
    let session_id = session(&service);
    assert!(prepare_window(&service, session_id, "r1", 0, 12_288).is_err());
    assert_eq!(exports.active(), 0);
    assert_eq!(exports.releases.load(Ordering::SeqCst), 2);
    assert_eq!(exports.quarantines.load(Ordering::SeqCst), 0);
}

#[test]
fn completed_release_frees_slots_and_duplicates_hit_the_tombstone() {
    let (service, exports, _) = fixture(SLOT * 2, 4);
    let session_id = session(&service);
    let reply = prepare_window(&service, session_id, "r1", 0, 8192).unwrap();
    let release = release_request(&reply, HixlReleaseOutcome::Completed);
    let first = service.release_blocking("node-b", release.clone()).unwrap();
    assert!(first.consumed && !first.already_released);
    assert_eq!(exports.active(), 0);
    let duplicate = service.release_blocking("node-b", release).unwrap();
    assert!(duplicate.consumed && duplicate.already_released);
    // A released read id cannot be prepared again in the same session.
    assert!(prepare_window(&service, session_id, "r1", 0, 8192).is_err());
    let closed = service
        .close_blocking(
            "node-b",
            OwnerCloseHixlRequest {
                session_id,
                home_session_id: "home-session-a".to_owned(),
            },
        )
        .unwrap();
    assert!(closed.drained);
    assert_eq!(exports.quarantines.load(Ordering::SeqCst), 0);
}

#[tokio::test]
async fn read_only_home_does_not_admit_hixl_writes() {
    let (home, exports, _) = fixture(SLOT, 2);
    let home = home.with_policy(&["node-b".to_owned()], false);
    assert!(home.check_peer("node-b").is_ok());
    assert!(home.check_peer("other-node").is_err());
    let session = home
        .negotiate_blocking("node-b", negotiate_request("root-a"))
        .unwrap();
    assert!(!session.buffered_write_supported);
    assert!(!session.write_checksum_negotiated);
    let reply = home
        .prepare_write("node-b".to_owned(), Default::default())
        .await
        .unwrap_err();
    assert_eq!(reply.code(), tonic::Code::Unimplemented);
    assert_eq!(exports.active(), 0);
}

#[test]
fn a_late_close_cannot_close_a_reused_session_id_or_quarantine_its_windows() {
    let (previous, _, _) = fixture(SLOT, 2);
    let previous_id = session(&previous);
    let (home, exports, _) = fixture(SLOT, 2);
    let mut negotiation = negotiate_request("root-a");
    negotiation.access.as_mut().unwrap().home_session_id = "new-home".to_owned();
    let current = home.negotiate_blocking("node-b", negotiation).unwrap();
    assert_eq!(current.session_id, previous_id);

    let mut prepare = prepare_request(current.session_id, "current-read", 0, SLOT as u64);
    prepare.access.as_mut().unwrap().home_session_id = current.home_session_id.clone();
    let window = home.prepare_blocking("node-b", prepare).unwrap();
    for old_generation in ["home-session-a", ""] {
        assert!(
            home.close_blocking(
                "node-b",
                OwnerCloseHixlRequest {
                    session_id: previous_id,
                    home_session_id: old_generation.to_owned(),
                }
            )
            .is_err()
        );
    }
    assert_eq!(exports.active(), 1);
    assert_eq!(exports.quarantines.load(Ordering::SeqCst), 0);
    assert!(!home.lock_registry().unwrap().sessions[&previous_id].closed);
    home.release_blocking(
        "node-b",
        release_request(&window, HixlReleaseOutcome::Completed),
    )
    .unwrap();
    assert!(
        home.close_blocking(
            "node-b",
            OwnerCloseHixlRequest {
                session_id: current.session_id,
                home_session_id: current.home_session_id,
            }
        )
        .unwrap()
        .drained
    );
}

#[test]
fn a_prior_release_frees_its_slots_before_the_next_prepare_is_admitted() {
    let (service, exports, _) = fixture(SLOT * 4, 4);
    let session_id = session(&service);
    let first = prepare_window(&service, session_id, "r1", 0, (SLOT * 4) as u64).unwrap();
    assert_eq!(exports.active(), 4);
    let full = prepare_window(&service, session_id, "r2", 0, SLOT as u64).unwrap_err();
    assert_eq!(full.code(), tonic::Code::ResourceExhausted);

    let mut next = prepare_request(session_id, "r2", 0, SLOT as u64);
    next.prior_release = Some(release_request(&first, HixlReleaseOutcome::Completed));
    let second = service.prepare_blocking("node-b", next.clone()).unwrap();
    assert_eq!(exports.active(), 1);
    // A retry whose reply was lost replays both the release and the prepare.
    assert_eq!(service.prepare_blocking("node-b", next).unwrap(), second);
    assert_eq!(exports.active(), 1);
    let duplicate = service
        .release_blocking(
            "node-b",
            release_request(&first, HixlReleaseOutcome::Completed),
        )
        .unwrap();
    assert!(duplicate.consumed && duplicate.already_released);
    assert_eq!(exports.quarantines.load(Ordering::SeqCst), 0);
}

#[test]
fn a_prior_release_of_another_session_is_rejected_without_effect() {
    let (service, exports, source) = fixture(SLOT * 2, 4);
    let session_id = session(&service);
    let first = prepare_window(&service, session_id, "r1", 0, (SLOT * 2) as u64).unwrap();
    let reads = source.reads.load(Ordering::SeqCst);
    let mut release = release_request(&first, HixlReleaseOutcome::Completed);
    release.session_id += 1;
    let mut next = prepare_request(session_id, "r2", 0, SLOT as u64);
    next.prior_release = Some(release);
    let error = service.prepare_blocking("node-b", next).unwrap_err();
    assert_eq!(error.code(), tonic::Code::InvalidArgument);
    assert_eq!(exports.active(), 2);
    assert_eq!(source.reads.load(Ordering::SeqCst), reads);
}

#[tokio::test]
async fn a_refused_prepare_still_applies_its_prior_release() {
    let (service, exports, _) = fixture(SLOT, 4);
    let session_id = session(&service);
    let first = prepare_window(&service, session_id, "r1", 0, 4096).unwrap();
    service.drain(Duration::ZERO).await.unwrap_err();
    let mut next = prepare_request(session_id, "r2", 0, 4096);
    next.prior_release = Some(release_request(&first, HixlReleaseOutcome::Completed));
    let refused = service.prepare_blocking("node-b", next).unwrap_err();
    assert_eq!(
        afs_transport::grpc::error_status::status_to_error(refused).code(),
        afs_error::NODE_TRANSFER_UNAVAILABLE
    );
    assert_eq!(exports.active(), 0);
    service.drain(Duration::ZERO).await.unwrap();
}

#[test]
fn unknown_outcome_keeps_slots_and_freezes_admission() {
    let (service, exports, _) = fixture(SLOT, 4);
    let session_id = session(&service);
    let reply = prepare_window(&service, session_id, "r1", 0, 4096).unwrap();
    let released = service
        .release_blocking(
            "node-b",
            release_request(&reply, HixlReleaseOutcome::Unknown),
        )
        .unwrap();
    assert!(!released.consumed);
    assert!(
        service
            .release_blocking(
                "node-b",
                release_request(&reply, HixlReleaseOutcome::NotSubmitted)
            )
            .is_err()
    );
    assert_eq!(exports.active(), 1);
    assert_eq!(exports.quarantines.load(Ordering::SeqCst), 1);
    assert!(prepare_window(&service, session_id, "r2", 0, 4096).is_err());
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
    let reply = prepare_window(&service, session_id, "r1", 0, 4096).unwrap();
    exports.fail_release.store(true, Ordering::SeqCst);
    assert!(
        service
            .release_blocking(
                "node-b",
                release_request(&reply, HixlReleaseOutcome::Completed)
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
    prepare_window(&service, session_id, "r1", 0, 4096).unwrap();
    let closed = service
        .close_blocking(
            "node-b",
            OwnerCloseHixlRequest {
                session_id,
                home_session_id: "home-session-a".to_owned(),
            },
        )
        .unwrap();
    assert!(!closed.drained);
    assert_eq!(exports.quarantines.load(Ordering::SeqCst), 1);
    assert_eq!(exports.active(), 1);
}

#[tokio::test]
async fn failed_expiry_quarantine_keeps_admission_frozen_until_a_retry_succeeds() {
    let (service, exports, _) = fixture(SLOT, 4);
    let session_id = session(&service);
    prepare_window(&service, session_id, "r1", 0, 4096).unwrap();
    for session in service.lock_registry().unwrap().sessions.values_mut() {
        session.last_used = Instant::now() - OWNER_HIXL_SESSION_TTL;
    }
    exports.fail_quarantine.store(true, Ordering::SeqCst);
    assert!(service.expire().await.is_err());
    {
        let registry = service.lock_registry().unwrap();
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
    let registry = service.lock_registry().unwrap();
    assert!(registry.admission_frozen && registry.runtime_quarantined);
    assert_eq!(exports.quarantines.load(Ordering::SeqCst), 2);
    assert_eq!(exports.active(), 1);
}

#[tokio::test]
async fn failed_forced_quarantine_is_retried_by_the_next_expiry() {
    let (service, exports, _) = fixture(SLOT, 4);
    let session_id = session(&service);
    let reply = prepare_window(&service, session_id, "r1", 0, 4096).unwrap();
    exports.fail_quarantine.store(true, Ordering::SeqCst);
    assert!(
        service
            .release_blocking(
                "node-b",
                release_request(&reply, HixlReleaseOutcome::Unknown),
            )
            .is_err()
    );
    service.expire().await.unwrap();
    let registry = service.lock_registry().unwrap();
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
    let refused = prepare_window(&service, session_id, "r2", 0, 4096).unwrap_err();
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
    let reply = prepare_window(&service, session_id, "r1", 0, 4096).unwrap();
    let error = service.drain(Duration::from_millis(30)).await.unwrap_err();
    assert!(error.message().contains("drain"));
    assert_eq!(exports.quarantines.load(Ordering::SeqCst), 1);
    service
        .release_blocking(
            "node-b",
            release_request(&reply, HixlReleaseOutcome::Completed),
        )
        .unwrap();
    service.drain(Duration::from_millis(30)).await.unwrap();
    assert_eq!(exports.active(), 0);
}

async fn start_drain(service: &OwnerHixlService) -> tokio::task::JoinHandle<Result<(), Status>> {
    let source = service.clone();
    let drain = tokio::spawn(async move { source.drain(Duration::from_secs(2)).await });
    tokio::time::timeout(Duration::from_secs(1), async {
        while !service.lock_registry().unwrap().draining {
            tokio::task::yield_now().await;
        }
    })
    .await
    .unwrap();
    drain
}

#[tokio::test]
async fn draining_home_refuses_new_prepares_but_consumes_releases() {
    let (service, exports, _) = batch_fixture(8);
    let session_id = session(&service);
    let live: Vec<_> = ["direct", "window-prior", "batch-prior"]
        .into_iter()
        .map(|read_id| prepare_window(&service, session_id, read_id, 0, SLOT as u64).unwrap())
        .collect();
    let drain = start_drain(&service).await;

    let prior = |index: usize| release_request(&live[index], HixlReleaseOutcome::Completed);
    let mut window = prepare_request(session_id, "new-window", 0, SLOT as u64);
    window.prior_release = Some(prior(1));
    let window_error = service.prepare("node-b".into(), window).await.unwrap_err();
    let mut batch = batch_request(session_id, "new-batch", "", 1);
    batch.prior_release = Some(prior(2));
    let batch_error = service
        .prepare_batch("node-b".into(), batch)
        .await
        .unwrap_err();
    for error in [window_error, batch_error] {
        assert_eq!(
            afs_transport::grpc::error_status::status_to_error(error).code(),
            afs_error::NODE_TRANSFER_UNAVAILABLE
        );
    }
    assert_eq!(exports.active(), 1);
    assert_eq!(exports.exports.load(Ordering::SeqCst), 3);
    let release = service
        .release("node-b".into(), prior(0))
        .await
        .unwrap()
        .into_inner();
    assert!(release.consumed && !release.already_released);
    drain.await.unwrap().unwrap();
    assert_eq!(exports.active(), 0);
    assert_eq!(exports.quarantines.load(Ordering::SeqCst), 0);
}

async fn spawn_paused_prepare(
    service: &OwnerHixlService,
    exports: &MemoryExports,
    batch: bool,
) -> (
    tokio::task::JoinHandle<Result<(), Status>>,
    std::sync::mpsc::SyncSender<()>,
) {
    let session_id = session(service);
    let (entered, resume) = exports.pause_next_export();
    let source = service.clone();
    let prepare = tokio::spawn(async move {
        if batch {
            source
                .prepare_batch(
                    "node-b".into(),
                    batch_request(session_id, "in-flight", "", 1),
                )
                .await
                .map(|_| ())
        } else {
            source
                .prepare(
                    "node-b".into(),
                    prepare_request(session_id, "in-flight", 0, SLOT as u64),
                )
                .await
                .map(|_| ())
        }
    });
    tokio::task::spawn_blocking(move || entered.recv_timeout(Duration::from_secs(1)))
        .await
        .unwrap()
        .unwrap();
    assert_eq!(exports.active(), 1);
    (prepare, resume)
}

#[tokio::test]
async fn draining_home_waits_for_unpublished_window_and_batch_rollback() {
    for batch in [false, true] {
        let (service, exports, _) = batch_fixture(8);
        let (prepare, resume) = spawn_paused_prepare(&service, &exports, batch).await;
        let mut drain = start_drain(&service).await;
        let early = tokio::time::timeout(Duration::from_millis(50), &mut drain).await;
        resume.send(()).unwrap();

        let refused = prepare.await.unwrap().unwrap_err();
        assert_eq!(
            afs_transport::grpc::error_status::status_to_error(refused).code(),
            afs_error::NODE_TRANSFER_UNAVAILABLE,
        );
        assert!(
            early.is_err(),
            "drain ended before the admitted Prepare rolled back (batch={batch})"
        );
        drain.await.unwrap().unwrap();
        assert_eq!(exports.active(), 0);
        assert_eq!(exports.releases.load(Ordering::SeqCst), 1);
        assert_eq!(exports.quarantines.load(Ordering::SeqCst), 0);
    }
}

#[tokio::test]
async fn drain_timeout_quarantines_even_without_a_published_slot() {
    let (service, exports, _) = batch_fixture(8);
    let (prepare, resume) = spawn_paused_prepare(&service, &exports, false).await;
    let result = service.drain(Duration::ZERO).await;
    let retained = exports.active();
    let quarantines = exports.quarantines.load(Ordering::SeqCst);
    resume.send(()).unwrap();
    assert!(prepare.await.unwrap().is_err());

    assert_eq!(
        afs_transport::grpc::error_status::status_to_error(result.unwrap_err()).code(),
        afs_error::NODE_TRANSFER_DRAIN_INCOMPLETE,
    );
    assert_eq!(retained, 1);
    assert_eq!(quarantines, 1);
}

#[tokio::test]
async fn a_cancelled_prepare_rpc_keeps_its_worker_in_the_home_drain() {
    let (service, exports, _) = batch_fixture(8);
    let (prepare, resume) = spawn_paused_prepare(&service, &exports, true).await;
    prepare.abort();
    assert!(prepare.await.unwrap_err().is_cancelled());

    let mut drain = start_drain(&service).await;
    let early = tokio::time::timeout(Duration::from_millis(50), &mut drain).await;
    resume.send(()).unwrap();
    assert!(
        early.is_err(),
        "drain ended before the cancelled RPC's worker rolled back"
    );
    drain.await.unwrap().unwrap();
    assert_eq!(exports.active(), 0);
    assert_eq!(exports.quarantines.load(Ordering::SeqCst), 0);
}

#[test]
fn negotiation_picks_crc32c_slot_checksums_when_the_reader_offers_them() {
    let (service, _, _) = fixture(SLOT, 4);
    let mut offered = negotiate_request("root-a");
    offered.slot_checksum_algorithms = vec![
        HixlSlotChecksumAlgorithm::Crc32c as i32,
        HixlSlotChecksumAlgorithm::Blake3 as i32,
    ];
    let reply = service.negotiate_blocking("node-b", offered).unwrap();
    assert_eq!(
        reply.slot_checksum_algorithm,
        HixlSlotChecksumAlgorithm::Crc32c as i32
    );

    // A reader that offers nothing keeps BLAKE3.
    let reply = service
        .negotiate_blocking("node-b", negotiate_request("root-a"))
        .unwrap();
    assert_eq!(
        reply.slot_checksum_algorithm,
        HixlSlotChecksumAlgorithm::Blake3 as i32
    );
}

#[test]
fn crc32c_session_slots_carry_crc32c_digests() {
    let (service, exports, source) = fixture(10_000, 4);
    let mut offered = negotiate_request("root-a");
    offered.slot_checksum_algorithms = vec![HixlSlotChecksumAlgorithm::Crc32c as i32];
    let session_id = service
        .negotiate_blocking("node-b", offered)
        .unwrap()
        .session_id;
    let reply = prepare_window(&service, session_id, "r1", 1000, 16_384).unwrap();
    let data = source.data.lock().unwrap().clone();
    let mut offset = 1000_usize;
    for slot in &reply.slots {
        let length = slot.length as usize;
        let bytes = exports.read(slot.address, length).unwrap();
        assert_eq!(bytes, data[offset..offset + length]);
        assert_eq!(
            slot.checksum,
            crate::node::chunk_crc32c::checksum(&bytes)
                .to_le_bytes()
                .to_vec()
        );
        offset += length;
    }
}

#[test]
fn slot_checksum_negotiation_falls_back_to_blake3() {
    use afs_protocol::node_data::HixlSlotChecksumAlgorithm;
    assert_eq!(
        SlotChecksum::negotiate(&[HixlSlotChecksumAlgorithm::Blake3 as i32]),
        SlotChecksum::Blake3
    );
    assert_eq!(SlotChecksum::negotiate(&[]), SlotChecksum::Blake3);
    // A Home that predates the field leaves it unset; that is BLAKE3.
    assert_eq!(
        SlotChecksum::from_reply(HixlSlotChecksumAlgorithm::Unspecified as i32),
        Some(SlotChecksum::Blake3)
    );
    assert_eq!(
        SlotChecksum::from_reply(2),
        None,
        "only BLAKE3 and plain CRC32C are slot algorithms"
    );
    assert_eq!(
        SlotChecksum::Crc32c.digest(b"abc").len(),
        SlotChecksum::Crc32c.len()
    );
    assert_eq!(
        SlotChecksum::Blake3.digest(b"abc").len(),
        SlotChecksum::Blake3.len()
    );
}

#[test]
fn prepare_reads_every_slot_of_a_window_concurrently() {
    let data = patterned(SLOT * 4, 0);
    // Each read waits until four run at once, so a sequential read peaks at one.
    let source = MemorySource::new(data.clone(), usize::MAX).with_overlap_probe(4);
    let (service, exports, source) = memory_home(SLOT, 4, source);
    let session_id = session(&service);
    let reply = prepare_window(&service, session_id, "r1", 0, (SLOT * 4) as u64).unwrap();

    assert!(
        source.peak_overlap() >= 2,
        "slot reads ran one at a time (peak {})",
        source.peak_overlap()
    );
    assert_eq!(reply.read, (SLOT * 4) as u64);
    assert_eq!(reply.slots.len(), 4);
    for (index, slot) in reply.slots.iter().enumerate() {
        let bytes = exports.read(slot.address, slot.length as usize).unwrap();
        assert_eq!(bytes, data[index * SLOT..(index + 1) * SLOT]);
        assert_eq!(slot.checksum, blake3::hash(&bytes).as_bytes().to_vec());
    }
}

#[test]
fn the_first_failing_slot_in_file_order_decides_the_error() {
    let (service, exports, source) = fixture(SLOT * 4, 4);
    source.fail_read_at(SLOT as u64, afs_error::IO_OTHER);
    source.fail_read_at((SLOT * 3) as u64, afs_error::NODE_OWNER_STALE_HANDLE);
    let session_id = session(&service);
    let error = prepare_window(&service, session_id, "r1", 0, (SLOT * 4) as u64).unwrap_err();

    let expected = error_to_status(afs_error::Error::coded(afs_error::IO_OTHER, "probe"));
    assert_eq!(error.code(), expected.code());
    assert_eq!(exports.active(), 0);
    assert_eq!(exports.releases.load(Ordering::SeqCst), 4);
}

fn patterned(length: usize, seed: u8) -> Vec<u8> {
    (0..length)
        .map(|index| (index % 251) as u8 ^ seed)
        .collect()
}

/// `d/` holds a: 1000 B, b: 3000 B, c: 5000 B (above a slot), d: 100 B
/// (tiny), e: 2000 B, f: a full slot, g: 10 B (tiny) and a subdirectory.
fn batch_fixture(slots: usize) -> (OwnerHixlService, Arc<MemoryExports>, Arc<MemorySource>) {
    let (service, exports, source) = fixture(SLOT, slots);
    source.tree.insert_directory("d");
    for (name, length, seed) in [
        ("a", 1000, 1),
        ("b", 3000, 2),
        ("c", 5000, 3),
        ("d", 100, 4),
        ("e", 2000, 5),
        ("f", SLOT, 6),
        ("g", 10, 7),
    ] {
        source
            .tree
            .insert(&format!("d/{name}"), patterned(length, seed));
    }
    source.tree.insert_directory("d/sub");
    (service, exports, source)
}

fn batch_request(
    session_id: u64,
    read_id: &str,
    anchor: &str,
    max_slots: usize,
) -> OwnerPrepareBatchHixlRequest {
    OwnerPrepareBatchHixlRequest {
        session_id,
        read_id: read_id.to_owned(),
        access: Some(access("root-a")),
        parent_path: b"d".to_vec(),
        anchor_name: anchor.as_bytes().to_vec(),
        lane_id: 0,
        slot_bytes: SLOT as u64,
        max_bytes: (SLOT * max_slots) as u64,
        min_file_bytes: 500,
        max_file_bytes: SLOT as u64,
        prior_release: None,
    }
}

#[test]
fn window_and_directory_batch_share_the_128_mib_prepare_limit() {
    const WINDOW: usize = 4 * 1024 * 1024;
    const GROUP: u64 = 128 * 1024 * 1024;
    let source = MemorySource::new(vec![42], 3000);
    source.tree.insert_directory("d");
    let (service, exports, _) = memory_home(WINDOW, 32, source);
    let session_id = session(&service);

    let mut window = prepare_request(session_id, "limit", 0, GROUP);
    window.slot_bytes = WINDOW as u64;
    let reply = service.prepare_blocking("node-b", window.clone()).unwrap();
    assert_eq!(reply.read, 1);
    assert!(reply.eof);
    assert_eq!(exports.active(), 1);
    service
        .release_blocking(
            "node-b",
            release_request(&reply, HixlReleaseOutcome::Completed),
        )
        .unwrap();
    assert_eq!(exports.active(), 0);
    window.read_id = "over-limit".to_owned();
    window.length += 1;
    let error = service.prepare_blocking("node-b", window).unwrap_err();
    assert_eq!(error.code(), tonic::Code::InvalidArgument);
    assert!(error.message().contains("identity or range"));

    let mut batch = batch_request(session_id, "batch-limit", "", 32);
    batch.slot_bytes = WINDOW as u64;
    batch.max_bytes = GROUP;
    let reply = service
        .prepare_batch_blocking("node-b", batch.clone())
        .unwrap();
    assert!(reply.slots.is_empty());
    batch.max_bytes += 1;
    let error = service.prepare_batch_blocking("node-b", batch).unwrap_err();
    assert_eq!(error.code(), tonic::Code::InvalidArgument);
    assert!(error.message().contains("identity or bounds"));
    assert_eq!(exports.active(), 0);
}

/// The bytes of each published file, read back through the exported slots.
fn batch_files(
    exports: &MemoryExports,
    reply: &OwnerPrepareBatchHixlReply,
) -> Vec<(String, Vec<u8>)> {
    reply
        .files
        .iter()
        .map(|file| {
            let slot = &reply.slots[file.slot_index as usize];
            let bytes = exports.read(slot.address, slot.length as usize).unwrap();
            let start = file.offset_in_slot as usize;
            (
                String::from_utf8(file.name.clone()).unwrap(),
                bytes[start..start + file.size as usize].to_vec(),
            )
        })
        .collect()
}

#[test]
fn batch_prepare_packs_whole_files_in_name_order_from_the_anchor() {
    let (service, exports, source) = batch_fixture(8);
    let session_id = session(&service);
    let reply = prepare_page(&service, session_id, "b1", "b", 2).unwrap();

    // b opens slot 0; c and d are out of range; e does not fit after b and
    // opens slot 1; f fits nowhere, so the batch stops there.
    let names = reply
        .files
        .iter()
        .map(|file| String::from_utf8(file.name.clone()).unwrap())
        .collect::<Vec<_>>();
    assert_eq!(names, ["b", "e"]);
    assert_eq!(
        reply
            .files
            .iter()
            .map(|file| (file.slot_index, file.offset_in_slot))
            .collect::<Vec<_>>(),
        [(0, 0), (1, 0)]
    );
    assert_eq!(
        reply
            .slots
            .iter()
            .map(|slot| slot.length)
            .collect::<Vec<_>>(),
        [3000, 2000]
    );
    assert_eq!(reply.bytes, 5000);
    assert_eq!(reply.next_name, b"f");
    assert_eq!(reply.engine_generation, 7);
    for (name, bytes) in batch_files(&exports, &reply) {
        assert_eq!(
            bytes,
            source
                .tree
                .get(format!("d/{name}").as_bytes())
                .unwrap()
                .data
        );
    }
    for slot in &reply.slots {
        let bytes = exports.read(slot.address, slot.length as usize).unwrap();
        assert_eq!(slot.checksum, blake3::hash(&bytes).as_bytes().to_vec());
    }
    let file = &reply.files[0];
    let entry = source.tree.get(b"d/b").unwrap();
    assert_eq!(file.identity.as_ref().unwrap().opaque, entry.identity);
    assert_eq!((file.size, file.mtime_ns), (3000, entry.mtime_ns));
    assert_eq!(exports.active(), 2);
}

#[test]
fn consecutive_batch_pages_reuse_names_but_revalidate_the_grant_and_files() {
    let (service, exports, source) = batch_fixture(8);
    let session_id = session(&service);
    let first = prepare_page(&service, session_id, "b1", "", 1).unwrap();
    assert_eq!(first.next_name, b"e");

    let mut denied = batch_request(session_id, "denied", "e", 1);
    denied.access.as_mut().unwrap().fencing_token = "revoked".to_owned();
    assert!(service.prepare_batch_blocking("node-b", denied).is_err());
    assert_eq!(source.tree.listings.load(Ordering::SeqCst), 1);

    // Content edits leave the names valid. The next page must use the current
    // stat, then omit a file that changes between stat and open.
    let updated = vec![71; 2048];
    source.tree.write("d/e", updated.clone());
    source.tree.rewrite_on_open("d/f");
    let second = prepare_page(&service, session_id, "b2", "e", 1).unwrap();
    assert_eq!(second.next_name, b"f");
    assert_eq!(batch_files(&exports, &second), [("e".to_owned(), updated)]);
    let third = prepare_page(&service, session_id, "b3", "f", 1).unwrap();
    assert!(third.files.is_empty(), "f changed after its fresh stat");
    assert_eq!(source.tree.listings.load(Ordering::SeqCst), 1);
}

#[test]
fn batch_name_snapshot_refreshes_after_membership_and_access_generation_changes() {
    let (service, exports, source) = batch_fixture(8);
    let session_id = session(&service);
    let first = prepare_page(&service, session_id, "b1", "", 1).unwrap();
    assert_eq!(first.next_name, b"e");

    let added = vec![42; 600];
    source.tree.insert("d/ea", added.clone());
    let second = prepare_page(&service, session_id, "b2", "e", 1).unwrap();
    assert_eq!(batch_files(&exports, &second)[1], ("ea".to_owned(), added));
    assert_eq!(second.next_name, b"f");
    assert_eq!(source.tree.listings.load(Ordering::SeqCst), 2);

    let mut third = batch_request(session_id, "b3", "f", 1);
    third.access.as_mut().unwrap().access_generation += 1;
    let third = service.prepare_batch_blocking("node-b", third).unwrap();
    assert_eq!(third.files[0].name, b"f");
    assert_eq!(source.tree.listings.load(Ordering::SeqCst), 3);
}

#[test]
fn oversized_batch_name_snapshot_skips_prefetch_and_remembers_the_limit() {
    let (service, exports, source) = batch_fixture(8);
    let session_id = session(&service);
    let suffix = "x".repeat(240);
    for index in 0..MAX_OWNER_HIXL_BATCH_SCAN {
        source
            .tree
            .insert_directory(&format!("d/x{index:06}{suffix}"));
    }
    source.tree.insert("d/z", vec![1; 601]);

    for (id, anchor) in [("b1", "a"), ("b2", "z")] {
        let reply = prepare_page(&service, session_id, id, anchor, 1).unwrap();
        assert!(reply.files.is_empty() && reply.slots.is_empty() && reply.next_name.is_empty());
    }
    let visited = source.tree.listed_entries.load(Ordering::SeqCst);
    assert!(visited > 0 && visited < MAX_OWNER_HIXL_BATCH_SCAN);
    assert_eq!(source.tree.listings.load(Ordering::SeqCst), 1);
    assert_eq!(exports.active(), 0);
    assert!(source.tree.get(b"d/z").is_some());

    let mut denied = batch_request(session_id, "denied", "z", 1);
    denied.access.as_mut().unwrap().fencing_token = "revoked".to_owned();
    assert!(service.prepare_batch_blocking("node-b", denied).is_err());
    assert_eq!(source.tree.listed_entries.load(Ordering::SeqCst), visited);

    source.tree.insert_directory("d/new");
    let reply = prepare_page(&service, session_id, "b3", "z", 1).unwrap();
    assert!(reply.files.is_empty() && reply.next_name.is_empty());
    assert_eq!(source.tree.listings.load(Ordering::SeqCst), 2);
}

#[test]
fn batch_name_snapshot_evicts_old_directories_at_the_service_limit() {
    let (service, _exports, source) = batch_fixture(8);
    let session_id = session(&service);
    for index in 0..=MAX_OWNER_HIXL_BATCH_SNAPSHOTS {
        let parent = format!("other-{index}");
        source.tree.insert_directory(&parent);
    }
    for index in 0..=MAX_OWNER_HIXL_BATCH_SNAPSHOTS {
        let mut request = batch_request(session_id, &format!("b{index}"), "", 1);
        request.parent_path = format!("other-{index}").into_bytes();
        service.prepare_batch_blocking("node-b", request).unwrap();
    }
    for parent in [
        format!("other-{MAX_OWNER_HIXL_BATCH_SNAPSHOTS}"),
        "other-0".to_owned(),
    ] {
        let mut request = batch_request(session_id, &parent, "", 1);
        request.parent_path = parent.into_bytes();
        service.prepare_batch_blocking("node-b", request).unwrap();
    }
    assert_eq!(
        source.tree.listings.load(Ordering::SeqCst),
        MAX_OWNER_HIXL_BATCH_SNAPSHOTS + 2
    );
}

#[test]
fn batch_prepare_shares_slots_and_reports_the_end_of_the_directory() {
    let (service, exports, _) = batch_fixture(8);
    let session_id = session(&service);
    let reply = prepare_page(&service, session_id, "b1", "", 4).unwrap();
    let layout = reply
        .files
        .iter()
        .map(|file| {
            (
                String::from_utf8(file.name.clone()).unwrap(),
                file.slot_index,
                file.offset_in_slot,
            )
        })
        .collect::<Vec<_>>();
    // a and b share slot 0; e opens slot 1; f fills slot 2.
    assert_eq!(
        layout,
        [
            ("a".to_owned(), 0, 0),
            ("b".to_owned(), 0, 1000),
            ("e".to_owned(), 1, 0),
            ("f".to_owned(), 2, 0),
        ]
    );
    assert!(reply.next_name.is_empty(), "g and sub are not packed");
    assert_eq!(reply.slots.len(), 3);
    assert_eq!(exports.active(), 3);
}

#[test]
fn a_failed_batch_file_is_zeroed_and_omitted_while_others_survive() {
    let (service, exports, source) = batch_fixture(8);
    source.tree.fail_open("d/a");
    source.tree.rewrite_on_open("d/e");
    let session_id = session(&service);
    let reply = prepare_page(&service, session_id, "b1", "a", 4).unwrap();
    let names = reply
        .files
        .iter()
        .map(|file| String::from_utf8(file.name.clone()).unwrap())
        .collect::<Vec<_>>();
    assert_eq!(
        names,
        ["b", "f"],
        "a failed to open and e changed after stat"
    );
    let slot = &reply.slots[0];
    let bytes = exports.read(slot.address, slot.length as usize).unwrap();
    assert!(bytes[..1000].iter().all(|byte| *byte == 0));
    assert_eq!(slot.checksum, blake3::hash(&bytes).as_bytes().to_vec());
    let slot = &reply.slots[1];
    let bytes = exports.read(slot.address, slot.length as usize).unwrap();
    assert!(bytes.iter().all(|byte| *byte == 0));
    for (name, bytes) in batch_files(&exports, &reply) {
        assert_eq!(
            bytes,
            source
                .tree
                .get(format!("d/{name}").as_bytes())
                .unwrap()
                .data
        );
    }
}

#[test]
fn batch_prepare_rejects_bad_requests_without_pinning_slots() {
    let (service, exports, _) = batch_fixture(8);
    let session_id = session(&service);
    let mut foreign = batch_request(session_id, "b1", "", 2);
    foreign.access = Some(access("root-x"));
    assert!(
        service
            .prepare_batch_blocking("node-b", foreign)
            .unwrap_err()
            .message()
            .contains("unknown root")
    );
    let mut missing = batch_request(session_id, "b2", "", 2);
    missing.parent_path = b"nope".to_vec();
    assert!(service.prepare_batch_blocking("node-b", missing).is_err());
    let mut oversized = batch_request(session_id, "b3", "", 2);
    oversized.max_file_bytes = SLOT as u64 + 1;
    assert_eq!(
        service
            .prepare_batch_blocking("node-b", oversized)
            .unwrap_err()
            .code(),
        tonic::Code::InvalidArgument
    );
    let mut too_many = batch_request(session_id, "b4", "", 9);
    too_many.max_bytes = (SLOT * 9) as u64;
    assert!(service.prepare_batch_blocking("node-b", too_many).is_err());
    assert_eq!(exports.active(), 0);
    assert_eq!(exports.exports.load(Ordering::SeqCst), 0);
}

#[test]
fn batch_replay_reuses_the_admitted_slots_and_release_frees_them() {
    let (service, exports, source) = batch_fixture(8);
    let session_id = session(&service);
    let request = batch_request(session_id, "b1", "", 4);
    let first = service
        .prepare_batch_blocking("node-b", request.clone())
        .unwrap();
    let opens = source.tree.batch_opens.load(Ordering::SeqCst);
    let replay = service.prepare_batch_blocking("node-b", request).unwrap();
    assert_eq!(first, replay);
    assert_eq!(source.tree.batch_opens.load(Ordering::SeqCst), opens);
    assert_eq!(exports.exports.load(Ordering::SeqCst), 3);

    // The same read id with another request is a different attempt.
    let error = prepare_page(&service, session_id, "b1", "b", 4).unwrap_err();
    assert_eq!(error.code(), tonic::Code::InvalidArgument);

    let release = OwnerReleaseHixlReadRequest {
        session_id,
        read_id: first.read_id.clone(),
        engine_generation: first.engine_generation,
        outcome: HixlReleaseOutcome::Completed as i32,
        content_validated: true,
    };
    assert!(
        service
            .release_blocking("node-b", release)
            .unwrap()
            .consumed
    );
    assert_eq!(exports.active(), 0);
}

#[test]
fn batch_slots_count_against_the_lane_export_pool() {
    let (service, exports, _) = batch_fixture(4);
    let session_id = session(&service);
    prepare_window(&service, session_id, "w1", 0, SLOT as u64).unwrap();
    // One window slot is pinned; a four-slot batch cannot be admitted.
    let error = prepare_page(&service, session_id, "b1", "", 4).unwrap_err();
    assert_eq!(error.code(), tonic::Code::ResourceExhausted);
    assert_eq!(exports.active(), 1);
}

#[test]
fn a_batch_prepare_applies_its_prior_release_before_admission() {
    let (service, exports, _) = batch_fixture(4);
    let session_id = session(&service);
    let window = prepare_window(&service, session_id, "w1", 0, SLOT as u64).unwrap();
    let mut request = batch_request(session_id, "b1", "", 4);
    request.prior_release = Some(release_request(&window, HixlReleaseOutcome::Completed));
    let batch = service.prepare_batch_blocking("node-b", request).unwrap();
    assert_eq!(exports.active(), batch.slots.len());
}

#[test]
fn entry_limit_accepts_a_complete_listing_and_stops_one_entry_past_it() {
    let (service, _exports, source) = fixture(SLOT, 8);
    source.tree.insert_directory("d");
    for index in 0..MAX_OWNER_HIXL_BATCH_SCAN - 1 {
        source.tree.insert(&format!("d/t{index:05}"), vec![1; 10]);
    }
    let session_id = session(&service);
    source.tree.insert("d/z", vec![1; 601]);
    let reply = prepare_page(&service, session_id, "below", "z", 2).unwrap();
    assert_eq!(reply.files[0].name, b"z");
    assert!(reply.next_name.is_empty());
    assert_eq!(
        source.tree.listed_entries.load(Ordering::SeqCst),
        MAX_OWNER_HIXL_BATCH_SCAN
    );

    for index in 0..10 {
        source.tree.insert_directory(&format!("d/y{index}"));
    }
    let reply = prepare_page(&service, session_id, "above", "", 2).unwrap();
    assert!(reply.files.is_empty() && reply.slots.is_empty() && reply.next_name.is_empty());
    assert_eq!(
        source.tree.listed_entries.load(Ordering::SeqCst),
        2 * MAX_OWNER_HIXL_BATCH_SCAN + 1
    );
}

#[test]
fn drained_sessions_leave_room_for_new_clients_but_live_sessions_remain_bounded() {
    let (service, _, _) = fixture(SLOT, 1);
    let mut previous = 0;
    for _ in 0..3 * MAX_OWNER_HIXL_SESSIONS {
        let id = session(&service);
        assert!(id > previous);
        previous = id;
        assert!(
            service
                .close_blocking(
                    "node-b",
                    OwnerCloseHixlRequest {
                        session_id: id,
                        home_session_id: "home-session-a".to_owned()
                    }
                )
                .unwrap()
                .drained
        );
    }
    assert!(service.lock_registry().unwrap().sessions.len() <= 2 * MAX_OWNER_HIXL_SESSIONS);
    for _ in 0..MAX_OWNER_HIXL_SESSIONS {
        session(&service);
    }
    assert_eq!(
        service
            .negotiate_blocking("node-b", negotiate_request("root-a"))
            .unwrap_err()
            .code(),
        tonic::Code::ResourceExhausted
    );
}

mod write_tests;

#[test]
fn not_submitted_recovers_window_and_batch_without_prepare_reply() {
    let (service, exports, _) = batch_fixture(8);
    let session_id = session(&service);
    for (read_id, batch) in [("lost-window", false), ("lost-batch", true)] {
        let prepare = || {
            if batch {
                service
                    .prepare_batch_blocking("node-b", batch_request(session_id, read_id, "", 1))
                    .map(|_| ())
            } else {
                prepare_window(&service, session_id, read_id, 0, SLOT as u64).map(|_| ())
            }
        };
        prepare().unwrap();
        assert!(exports.active() > 0);
        let release = not_submitted_request(session_id, read_id);
        let first = service.release_blocking("node-b", release.clone()).unwrap();
        assert!(first.consumed && !first.already_released);
        assert_eq!(exports.active(), 0);
        let duplicate = service.release_blocking("node-b", release).unwrap();
        assert!(duplicate.consumed && duplicate.already_released);
        assert_eq!(prepare().unwrap_err().code(), tonic::Code::InvalidArgument);
    }
    assert_eq!(exports.quarantines.load(Ordering::SeqCst), 0);
}

#[test]
fn not_submitted_fences_a_prepare_that_has_not_arrived_and_checks_identity() {
    let (service, exports, _) = batch_fixture(8);
    let session_id = session(&service);
    let release = not_submitted_request(session_id, "delayed");
    assert_eq!(
        service
            .release_blocking("node-c", release.clone())
            .unwrap_err()
            .code(),
        tonic::Code::PermissionDenied,
    );
    let mut wrong_generation = release.clone();
    wrong_generation.engine_generation += 1;
    assert_eq!(
        service
            .release_blocking("node-b", wrong_generation)
            .unwrap_err()
            .code(),
        tonic::Code::PermissionDenied,
    );
    let mut completed = release.clone();
    completed.outcome = HixlReleaseOutcome::Completed as i32;
    assert!(service.release_blocking("node-b", completed).is_err());

    // Neither a foreign session nor a Prepare presenting the wrong Home
    // incarnation may retire an ID on the authenticated read session.
    let other_session = session(&service);
    let mut stale_prepare = prepare_request(session_id, "carrier", 0, SLOT as u64);
    stale_prepare.access.as_mut().unwrap().home_session_id = "stale-home".to_owned();
    stale_prepare.prior_release = Some(not_submitted_request(session_id, "unaffected"));
    assert_eq!(
        service
            .prepare_blocking("node-b", stale_prepare)
            .unwrap_err()
            .code(),
        tonic::Code::PermissionDenied,
    );
    let unaffected = prepare_window(&service, session_id, "unaffected", 0, SLOT as u64).unwrap();
    service
        .release_blocking(
            "node-b",
            release_request(&unaffected, HixlReleaseOutcome::Completed),
        )
        .unwrap();

    let first = service.release_blocking("node-b", release.clone()).unwrap();
    assert!(first.consumed && !first.already_released);
    let duplicate = service.release_blocking("node-b", release).unwrap();
    assert!(duplicate.consumed && duplicate.already_released);
    assert!(prepare_window(&service, session_id, "delayed", 0, SLOT as u64).is_err());
    assert!(
        service
            .prepare_batch_blocking("node-b", batch_request(session_id, "delayed", "", 1))
            .is_err()
    );
    assert_eq!(exports.active(), 0);
    prepare_window(&service, other_session, "delayed", 0, SLOT as u64).unwrap();
    let next = prepare_window(&service, session_id, "next", 0, SLOT as u64).unwrap();
    // A recently active session can outlive the cancelled ID's tombstone. It
    // may replay admitted reads, but cannot publish an old or a new read ID.
    service
        .lock_registry()
        .unwrap()
        .session_mut(session_id, "node-b")
        .unwrap()
        .tombstones
        .expire(Instant::now() + OWNER_HIXL_SESSION_TTL);
    assert!(prepare_window(&service, session_id, "delayed", 0, SLOT as u64).is_err());
    assert!(prepare_window(&service, session_id, "new", 0, SLOT as u64).is_err());
    assert_eq!(
        prepare_window(&service, session_id, "next", 0, SLOT as u64).unwrap(),
        next
    );
}

#[test]
fn not_submitted_rolls_back_a_prepare_already_filling_exports() {
    let (service, exports, _) = batch_fixture(8);
    let session_id = session(&service);
    for (read_id, batch) in [("filling-window", false), ("filling-batch", true)] {
        let (entered, resume) = exports.pause_next_export();
        std::thread::scope(|scope| {
            let late = scope.spawn(|| {
                if batch {
                    service
                        .prepare_batch_blocking("node-b", batch_request(session_id, read_id, "", 1))
                        .map(|_| ())
                } else {
                    prepare_window(&service, session_id, read_id, 0, SLOT as u64).map(|_| ())
                }
            });
            entered.recv_timeout(Duration::from_secs(3)).unwrap();
            let result =
                service.release_blocking("node-b", not_submitted_request(session_id, read_id));
            resume.send(()).unwrap();
            assert!(result.unwrap().consumed);
            assert_eq!(
                late.join().unwrap().unwrap_err().code(),
                tonic::Code::InvalidArgument
            );
        });
        assert_eq!(exports.active(), 0);
    }
    assert_eq!(exports.quarantines.load(Ordering::SeqCst), 0);
}

fn not_submitted_request(session_id: u64, read_id: &str) -> OwnerReleaseHixlReadRequest {
    OwnerReleaseHixlReadRequest {
        session_id,
        read_id: read_id.to_owned(),
        engine_generation: 7,
        outcome: HixlReleaseOutcome::NotSubmitted as i32,
        content_validated: false,
    }
}
