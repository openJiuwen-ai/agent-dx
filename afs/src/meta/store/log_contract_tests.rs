//! Backend-neutral `MetaLog` contract suite. Every backend runs the same
//! assertions; backend-specific format and crash tests stay with the backend.
//!
//! Covered: (1) compare-and-append, (2) group bounds, (3) recovery and
//! sequence, (4) checkpoint anchors, (5) read-only admission, (6) write-failure
//! latch, (8) status upper bound.
// TODO(S1d-5): (7) fencing — an append carrying a stale term is rejected, and
// recovery rejects a chain whose term decreases or exceeds the current term.

use super::{
    NodeSessionLease, RequestKey,
    journal::{
        BATCH_RECORDS_MAX, LogStatus, LogTip, MemoryLog, MemoryLogOptions, MetaLog, RecoveredLog,
        frame_size, test_records,
    },
    local_log::{LOG_HEADER_BYTES, LocalLog, LocalLogOptions},
    log::{Record, StateImage},
    state::State,
};
use std::{path::Path, time::Duration};

/// Byte bounds a case needs; `None` asks for the backend defaults.
#[derive(Clone, Copy)]
struct CaseLimits {
    record_max_bytes: usize,
    batch_max_bytes: usize,
    wal_max_bytes: u64,
}

struct Fixture<L> {
    log: L,
    /// Drop the handle and reconstruct one over the same retained records.
    reopen: Box<dyn Fn(L) -> L>,
    /// Make the next append fail with a real (or modelled) write failure.
    inject_write_failure: Box<dyn Fn(&L)>,
    /// Backend storage bytes outside the trait (files); empty when none.
    storage: Box<dyn Fn() -> Vec<(String, Vec<u8>)>>,
    _dir: Option<tempfile::TempDir>,
}

fn memory_fixture(limits: Option<CaseLimits>) -> Fixture<MemoryLog> {
    let options = limits.map_or_else(MemoryLogOptions::default, |limits| MemoryLogOptions {
        record_max_bytes: limits.record_max_bytes,
        batch_max_bytes: limits.batch_max_bytes,
        wal_max_bytes: limits.wal_max_bytes,
    });
    Fixture {
        log: MemoryLog::new(options).unwrap(),
        reopen: Box::new(|log: MemoryLog| log.reopen()),
        inject_write_failure: Box::new(MemoryLog::inject_write_failure),
        storage: Box::new(Vec::new),
        _dir: None,
    }
}

fn local_fixture(limits: Option<CaseLimits>) -> Fixture<LocalLog> {
    let options = limits.map_or_else(LocalLogOptions::default, |limits| LocalLogOptions {
        record_max_bytes: limits.record_max_bytes,
        batch_max_bytes: limits.batch_max_bytes,
        wal_max_bytes: limits.wal_max_bytes,
        segment_bytes: (limits.wal_max_bytes / 2).max(LOG_HEADER_BYTES + 256),
        segments_max: 16,
    });
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("meta-store");
    let reopen_path = path.clone();
    let storage_path = path.clone();
    Fixture {
        log: LocalLog::open(&path, options).unwrap(),
        reopen: Box::new(move |log: LocalLog| {
            drop(log);
            LocalLog::open(&reopen_path, options).unwrap()
        }),
        inject_write_failure: Box::new(|log: &LocalLog| log.inject_write_failure().unwrap()),
        storage: Box::new(move || inventory(&storage_path)),
        _dir: Some(dir),
    }
}

fn inventory(path: &Path) -> Vec<(String, Vec<u8>)> {
    let mut files = std::fs::read_dir(path)
        .unwrap()
        .map(|entry| {
            let entry = entry.unwrap();
            (
                entry.file_name().to_string_lossy().into_owned(),
                std::fs::read(entry.path()).unwrap(),
            )
        })
        .collect::<Vec<_>>();
    files.sort();
    files
}

/// Comparable form of a recovery result: tip, checkpoint and encoded records.
type RecoveredView = (LogTip, Option<(LogTip, Vec<u8>)>, Vec<Vec<u8>>);

fn recovered_view(recovered: &RecoveredLog) -> RecoveredView {
    (
        recovered.tip,
        recovered
            .checkpoint
            .as_ref()
            .map(|image| (image.covered, image.bytes.clone())),
        encoded(&recovered.records),
    )
}

fn encoded(records: &[Record]) -> Vec<Vec<u8>> {
    records
        .iter()
        .map(|record| serde_json::to_vec(record).unwrap())
        .collect()
}

/// A first record (revision 0 -> 1) whose framed size is about `pad` bytes
/// larger than an ordinary registration.
fn padded_first_record(pad: usize) -> Record {
    let (_, _, record) = State::default()
        .prepare_node_record(
            RequestKey::new("contract", "padded"),
            NodeSessionLease {
                node_id: "node".into(),
                session_id: "padded".into(),
                grpc_addr: "g".repeat(pad),
                data_addr: "data".into(),
                rest_addr: "rest".into(),
                ownerfs_enabled: true,
                storage_devices: Vec::new(),
                lease_ttl: Duration::from_secs(30),
            },
            12345,
            1024 * 1024,
        )
        .unwrap();
    record.unwrap().into()
}

/// Checkpoint image of the state after replaying `records` from empty.
fn image_after(records: &[Record]) -> Vec<u8> {
    let mut state = State::default();
    for record in records {
        state = state.replay_record(record).unwrap();
    }
    StateImage::encode(&state, 1024 * 1024).unwrap()
}

async fn append_each(log: &impl MetaLog, mut tip: LogTip, records: &[Record]) -> Vec<LogTip> {
    let mut tips = Vec::new();
    for record in records {
        let next = log
            .append_and_sync(tip, vec![record.clone()])
            .await
            .unwrap();
        assert_eq!(next.sequence, tip.sequence + 1, "sequence advances by one");
        tips.push(next);
        tip = next;
    }
    tips
}

/// Run every contract case against fresh backends from `make`.
async fn contract_suite<L: MetaLog + 'static>(make: impl Fn(Option<CaseLimits>) -> Fixture<L>) {
    compare_and_append(&make).await;
    group_bounds(&make).await;
    recovery_and_sequence(&make).await;
    checkpoint_anchors(&make).await;
    read_only_admission(&make).await;
    write_failure_latch(&make).await;
    status_upper_bound(&make).await;
}

/// (1) A stale or forged `expected` fails for both append and admission and
/// changes nothing; the correct tip still appends afterwards.
async fn compare_and_append<L: MetaLog>(make: &impl Fn(Option<CaseLimits>) -> Fixture<L>) {
    let fixture = make(None);
    let log = &fixture.log;
    let records = test_records(3);
    let tip = log
        .append_and_sync(LogTip::EMPTY, records[..2].to_vec())
        .await
        .unwrap();
    let before = recovered_view(&log.recover().await.unwrap());
    let forged = LogTip {
        sequence: tip.sequence,
        record_hash: [7; 32],
    };
    for stale in [LogTip::EMPTY, forged] {
        assert!(
            log.append_and_sync(stale, vec![records[2].clone()])
                .await
                .is_err(),
            "append with stale tip {stale:?} must fail"
        );
        assert!(log.can_append(stale, 64).await.is_err(), "stale admission");
    }
    assert_eq!(recovered_view(&log.recover().await.unwrap()), before);
    assert_eq!(log.status().await.unwrap().tip, tip);
    let next = log
        .append_and_sync(tip, vec![records[2].clone()])
        .await
        .unwrap();
    assert_eq!(next.sequence, 3);
}

/// (2) Groups hold 1..=batch_records_max records; exact byte bounds accept,
/// one byte over rejects without writing.
async fn group_bounds<L: MetaLog>(make: &impl Fn(Option<CaseLimits>) -> Fixture<L>) {
    let fixture = make(None);
    let log = &fixture.log;
    assert_eq!(log.limits().batch_records_max, BATCH_RECORDS_MAX);
    let records = test_records(BATCH_RECORDS_MAX * 2 + 1);
    assert!(
        log.append_and_sync(LogTip::EMPTY, Vec::new())
            .await
            .is_err()
    );
    let tip = log
        .append_and_sync(LogTip::EMPTY, records[..BATCH_RECORDS_MAX].to_vec())
        .await
        .unwrap();
    assert_eq!(tip.sequence, BATCH_RECORDS_MAX as u64);
    assert!(
        log.append_and_sync(tip, records[BATCH_RECORDS_MAX..].to_vec())
            .await
            .is_err(),
        "batch_records_max + 1 records must be rejected"
    );
    assert_eq!(log.recover().await.unwrap().tip, tip);

    let large = padded_first_record(1024);
    let size = frame_size(LogTip::EMPTY, &large).unwrap();
    for (limit, accepted) in [(size, true), (size - 1, false)] {
        let fixture = make(Some(CaseLimits {
            record_max_bytes: limit,
            batch_max_bytes: limit,
            wal_max_bytes: 1024 * 1024,
        }));
        let result = fixture
            .log
            .append_and_sync(LogTip::EMPTY, vec![large.clone()])
            .await;
        assert_eq!(result.is_ok(), accepted, "record {size} vs limit {limit}");
        let expected = if accepted { 1 } else { 0 };
        assert_eq!(fixture.log.recover().await.unwrap().tip.sequence, expected);
    }

    let pair = test_records(2);
    let first = frame_size(LogTip::EMPTY, &pair[0]).unwrap();
    let second = frame_size(
        LogTip {
            sequence: 1,
            record_hash: [0; 32],
        },
        &pair[1],
    )
    .unwrap();
    let record_max_bytes = first.max(second).max(256);
    let total = first + second;
    for (limit, accepted) in [(total, true), (total - 1, false)] {
        assert!(record_max_bytes <= limit, "fixture records too small");
        let fixture = make(Some(CaseLimits {
            record_max_bytes,
            batch_max_bytes: limit,
            wal_max_bytes: 1024 * 1024,
        }));
        let result = fixture
            .log
            .append_and_sync(LogTip::EMPTY, pair.clone())
            .await;
        assert_eq!(result.is_ok(), accepted, "batch {total} vs limit {limit}");
        let expected = if accepted { 2 } else { 0 };
        assert_eq!(fixture.log.recover().await.unwrap().tip.sequence, expected);
    }
}

/// (3) Recovery returns exactly the appended records in order with the
/// returned tip; repeated recovery and reconstruction give equal results.
async fn recovery_and_sequence<L: MetaLog>(make: &impl Fn(Option<CaseLimits>) -> Fixture<L>) {
    let fixture = make(None);
    let log = &fixture.log;
    let empty = log.recover().await.unwrap();
    assert_eq!(empty.tip, LogTip::EMPTY);
    assert!(empty.checkpoint.is_none() && empty.records.is_empty());
    let records = test_records(7);
    let mut tip = LogTip::EMPTY;
    for group in [&records[..1], &records[1..4], &records[4..]] {
        let next = log.append_and_sync(tip, group.to_vec()).await.unwrap();
        assert_eq!(next.sequence, tip.sequence + group.len() as u64);
        tip = next;
    }
    let first = log.recover().await.unwrap();
    assert_eq!(first.tip, tip);
    assert!(first.checkpoint.is_none());
    assert_eq!(encoded(&first.records), encoded(&records));
    let second = log.recover().await.unwrap();
    assert_eq!(recovered_view(&second), recovered_view(&first));
    let Fixture {
        log, reopen, _dir, ..
    } = fixture;
    let reopened = reopen(log);
    assert_eq!(
        recovered_view(&reopened.recover().await.unwrap()),
        recovered_view(&first)
    );
    assert_eq!(reopened.status().await.unwrap().tip, tip);
}

/// (4) Off-chain, future and conflicting anchors are rejected; a valid
/// checkpoint advances `status.checkpoint` and keeps the suffix replayable.
async fn checkpoint_anchors<L: MetaLog>(make: &impl Fn(Option<CaseLimits>) -> Fixture<L>) {
    let fixture = make(None);
    let log = &fixture.log;
    assert!(log.supports_checkpoint(), "both backends checkpoint");
    let records = test_records(5);
    let tips = append_each(log, LogTip::EMPTY, &records[..3]).await;
    let tip = tips[2];
    let image = image_after(&records[..1]);
    let off_chain = LogTip {
        sequence: 1,
        record_hash: [9; 32],
    };
    let future = LogTip {
        sequence: tip.sequence + 1,
        record_hash: tip.record_hash,
    };
    for covered in [off_chain, future] {
        assert!(
            log.install_checkpoint(covered, image.clone())
                .await
                .is_err(),
            "anchor {covered:?} must be rejected"
        );
    }
    assert_eq!(log.status().await.unwrap().checkpoint, LogTip::EMPTY);
    log.install_checkpoint(tips[0], image.clone())
        .await
        .unwrap();
    // Same position and business revision, different state bytes.
    let conflicting = image_after(&[padded_first_record(16)]);
    assert_ne!(conflicting, image);
    assert!(
        log.install_checkpoint(tips[0], conflicting).await.is_err(),
        "same position with different bytes must be rejected"
    );
    let status = log.status().await.unwrap();
    assert_eq!((status.tip, status.checkpoint), (tip, tips[0]));
    let recovered = log.recover().await.unwrap();
    assert_eq!(recovered.tip, tip);
    let checkpoint = recovered.checkpoint.as_ref().unwrap();
    assert_eq!((checkpoint.covered, &checkpoint.bytes), (tips[0], &image));
    assert_eq!(encoded(&recovered.records), encoded(&records[1..3]));
    let mut state = StateImage::decode(&checkpoint.bytes, 1024 * 1024).unwrap();
    for record in &recovered.records {
        state = state.replay_record(record).unwrap();
    }
    let mut expected = State::default();
    for record in &records[..3] {
        expected = expected.replay_record(record).unwrap();
    }
    assert_eq!(
        state.snapshot_bytes_with_batch(0).unwrap(),
        expected.snapshot_bytes_with_batch(0).unwrap()
    );
    let next = append_each(log, tip, &records[3..]).await;
    let Fixture {
        log, reopen, _dir, ..
    } = fixture;
    let reopened = reopen(log).recover().await.unwrap();
    assert_eq!(reopened.tip, next[1]);
    assert_eq!(reopened.checkpoint.unwrap().covered, tips[0]);
    assert_eq!(encoded(&reopened.records), encoded(&records[1..]));
}

/// (5) `can_append` is read-only; `Ok(false)` proves nothing changed.
async fn read_only_admission<L: MetaLog>(make: &impl Fn(Option<CaseLimits>) -> Fixture<L>) {
    let fixture = make(Some(CaseLimits {
        record_max_bytes: 1024,
        batch_max_bytes: 4096,
        wal_max_bytes: 16 * 1024,
    }));
    let log = &fixture.log;
    let records = test_records(256);
    let mut tip = LogTip::EMPTY;
    let mut refused = false;
    for record in &records {
        let size = frame_size(tip, record).unwrap();
        let before = (
            recovered_view(&log.recover().await.unwrap()),
            log.status().await.unwrap(),
            (fixture.storage)(),
        );
        let admitted = log.can_append(tip, size).await.unwrap();
        let after = (
            recovered_view(&log.recover().await.unwrap()),
            log.status().await.unwrap(),
            (fixture.storage)(),
        );
        assert!(
            before == after,
            "admission changed state (admitted={admitted})"
        );
        if !admitted {
            refused = true;
            break;
        }
        tip = log
            .append_and_sync(tip, vec![record.clone()])
            .await
            .unwrap();
    }
    assert!(
        refused,
        "finite WAL budget must eventually refuse admission"
    );
    assert!(tip.sequence > 0);
}

/// (6) After a failed write every operation fails until reconstruction; the
/// reconstructed backend recovers the acknowledged prefix and appends again.
async fn write_failure_latch<L: MetaLog>(make: &impl Fn(Option<CaseLimits>) -> Fixture<L>) {
    let fixture = make(None);
    let log = &fixture.log;
    let records = test_records(4);
    let tip = log
        .append_and_sync(LogTip::EMPTY, records[..2].to_vec())
        .await
        .unwrap();
    (fixture.inject_write_failure)(log);
    assert!(
        log.append_and_sync(tip, vec![records[2].clone()])
            .await
            .is_err()
    );
    assert!(log.health().await.is_err(), "health after failed write");
    assert!(log.recover().await.is_err(), "recover after failed write");
    assert!(log.status().await.is_err(), "status after failed write");
    assert!(log.can_append(tip, 64).await.is_err(), "admission latched");
    assert!(
        log.append_and_sync(tip, vec![records[2].clone()])
            .await
            .is_err(),
        "append latched"
    );
    assert!(
        log.install_checkpoint(tip, image_after(&records[..2]))
            .await
            .is_err(),
        "checkpoint latched"
    );
    let Fixture {
        log, reopen, _dir, ..
    } = fixture;
    let reopened = reopen(log);
    let recovered = reopened.recover().await.unwrap();
    assert_eq!(recovered.tip, tip, "injected failure wrote nothing");
    assert_eq!(encoded(&recovered.records), encoded(&records[..2]));
    reopened
        .append_and_sync(tip, records[2..].to_vec())
        .await
        .unwrap();
}

/// (8) The status byte bound never falls below the true framed bytes after
/// the checkpoint.
async fn status_upper_bound<L: MetaLog>(make: &impl Fn(Option<CaseLimits>) -> Fixture<L>) {
    let fixture = make(None);
    let log = &fixture.log;
    let records = test_records(6);
    let mut sizes = Vec::new();
    let mut tips = Vec::new();
    let mut tip = LogTip::EMPTY;
    for record in &records {
        sizes.push(frame_size(tip, record).unwrap() as u64);
        tip = log
            .append_and_sync(tip, vec![record.clone()])
            .await
            .unwrap();
        tips.push(tip);
    }
    let status: LogStatus = log.status().await.unwrap();
    assert_eq!((status.tip, status.checkpoint), (tip, LogTip::EMPTY));
    assert!(status.uncheckpointed_bytes_upper_bound >= sizes.iter().sum::<u64>());
    log.install_checkpoint(tips[1], image_after(&records[..2]))
        .await
        .unwrap();
    let status = log.status().await.unwrap();
    assert_eq!(status.checkpoint, tips[1]);
    assert!(status.uncheckpointed_bytes_upper_bound >= sizes[2..].iter().sum::<u64>());
}

#[tokio::test]
async fn memory_log_satisfies_contract() {
    contract_suite(memory_fixture).await;
}

#[tokio::test]
async fn local_log_satisfies_contract() {
    contract_suite(local_fixture).await;
}
