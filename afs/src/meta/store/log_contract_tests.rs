//! Backend-neutral `MetaLog` contract suite. Every backend runs the same
//! assertions; backend-specific format and crash tests stay with the backend.
//!
//! Covered: (1) compare-and-append, (2) group bounds, (3) recovery and
//! sequence, (4) checkpoint anchors, (5) read-only admission, (6) write-failure
//! latch, (7) term fencing, (8) status upper bound. Recovery of a chain whose
//! term decreases or exceeds the current term needs backend-specific damage;
//! it is tested with each backend (`journal`, `local_log`, `kv_log_tests`).
//!
//! The Redis and etcd suites need a dedicated service and are `#[ignore]`d:
//! run them with `--ignored` and `AFS_TEST_REDIS_URL` / `AFS_TEST_ETCD_ENDPOINTS`
//! set. A selected suite without its endpoint fails; it never passes silently.

use super::{
    NodeSessionLease, RequestKey,
    journal::{
        BATCH_RECORDS_MAX, LogCursor, LogStatus, LogTip, MemoryLog, MemoryLogOptions, MetaLog,
        RecoveredLog, frame_size, test_records,
    },
    kv_log::{EtcdLog, KvLogOptions, RedisLog},
    local_log::{LOG_HEADER_BYTES, LocalLog, LocalLogOptions},
    log::{Record, StateImage},
    state::State,
};
use std::{path::Path, time::Duration};

/// Byte bounds a case needs; `None` asks for the backend defaults.
#[derive(Clone, Copy)]
pub(super) struct CaseLimits {
    pub(super) record_max_bytes: usize,
    pub(super) batch_max_bytes: usize,
    pub(super) wal_max_bytes: u64,
}

struct Fixture<L> {
    log: L,
    /// Drop the handle and reconstruct one over the same retained records.
    reopen: Box<dyn Fn(L) -> L>,
    /// Open a newer writer over the same records while `&L` is still alive.
    takeover: Box<dyn Fn(&L) -> L>,
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
        takeover: Box::new(MemoryLog::reopen),
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
    let takeover_path = path.clone();
    let storage_path = path.clone();
    Fixture {
        log: LocalLog::open(&path, options).unwrap(),
        reopen: Box::new(move |log: LocalLog| {
            drop(log);
            LocalLog::open(&reopen_path, options).unwrap()
        }),
        // The directory lock would refuse a second writer; model a writer
        // that lost local exclusivity so only the CURRENT term can fence it.
        takeover: Box::new(move |log: &LocalLog| {
            log.release_lock_for_takeover().unwrap();
            LocalLog::open(&takeover_path, options).unwrap()
        }),
        inject_write_failure: Box::new(|log: &LocalLog| log.inject_write_failure().unwrap()),
        storage: Box::new(move || inventory(&storage_path)),
        _dir: Some(dir),
    }
}

/// Runs `future` to completion from a synchronous fixture closure. Needs the
/// multi-thread test runtime.
pub(super) fn block_on<F: std::future::Future>(future: F) -> F::Output {
    tokio::task::block_in_place(|| tokio::runtime::Handle::current().block_on(future))
}

/// Redis tests share one instance; the probe test changes its settings, so
/// it takes this gate exclusively while every other Redis test shares it.
pub(super) static REDIS_GATE: tokio::sync::RwLock<()> = tokio::sync::RwLock::const_new(());

pub(super) fn redis_url() -> String {
    std::env::var("AFS_TEST_REDIS_URL").expect(
        "the Redis contract needs AFS_TEST_REDIS_URL: a dedicated Redis with appendonly yes, appendfsync always and noeviction",
    )
}

pub(super) fn etcd_endpoints() -> Vec<String> {
    std::env::var("AFS_TEST_ETCD_ENDPOINTS")
        .expect("the etcd contract needs AFS_TEST_ETCD_ENDPOINTS: a dedicated etcd")
        .split(',')
        .map(str::to_owned)
        .collect()
}

/// etcd's default `--max-request-bytes`, the bound the etcd suite runs under.
pub(super) const ETCD_TEST_MAX_REQUEST_BYTES: usize = 1536 * 1024;

/// A fresh key prefix per fixture: every fixture starts at term 1.
pub(super) fn unique_prefix() -> String {
    format!("afs-contract/{}", uuid::Uuid::new_v4())
}

/// The Meta defaults; records beyond one request are staged (`kv_log`).
pub(super) fn kv_options(limits: Option<CaseLimits>) -> KvLogOptions {
    limits.map_or(
        KvLogOptions {
            record_max_bytes: 64 * 1024 * 1024,
            batch_max_bytes: 128 * 1024 * 1024,
            wal_max_bytes: 1024 * 1024 * 1024,
            image_max_bytes: 1024 * 1024 * 1024,
        },
        |limits| KvLogOptions {
            record_max_bytes: limits.record_max_bytes,
            batch_max_bytes: limits.batch_max_bytes,
            wal_max_bytes: limits.wal_max_bytes,
            image_max_bytes: 1024 * 1024 * 1024,
        },
    )
}

fn redis_fixture(limits: Option<CaseLimits>) -> Fixture<RedisLog> {
    let url = redis_url();
    let prefix = unique_prefix();
    let options = kv_options(limits);
    let open = {
        let (url, prefix) = (url.clone(), prefix.clone());
        move || block_on(RedisLog::open_redis(&url, &prefix, options)).unwrap()
    };
    let reopen = open.clone();
    let takeover = open.clone();
    Fixture {
        log: open(),
        reopen: Box::new(move |log| {
            drop(log);
            reopen()
        }),
        // No lock to lose: a second writer simply opens and takes a term.
        takeover: Box::new(move |_| takeover()),
        inject_write_failure: Box::new(RedisLog::inject_write_failure),
        storage: Box::new(move || {
            block_on(async {
                use super::kv_log::KvService;
                super::redis_log::RedisService::connect(&url, &prefix)
                    .await
                    .unwrap()
                    .dump()
                    .await
                    .unwrap()
            })
        }),
        _dir: None,
    }
}

fn etcd_fixture(limits: Option<CaseLimits>) -> Fixture<EtcdLog> {
    let endpoints = etcd_endpoints();
    let prefix = unique_prefix();
    let options = kv_options(limits);
    let open = {
        let (endpoints, prefix) = (endpoints.clone(), prefix.clone());
        move || {
            block_on(EtcdLog::open_etcd(
                &endpoints,
                &prefix,
                ETCD_TEST_MAX_REQUEST_BYTES,
                options,
            ))
            .unwrap()
        }
    };
    let reopen = open.clone();
    let takeover = open.clone();
    Fixture {
        log: open(),
        reopen: Box::new(move |log| {
            drop(log);
            reopen()
        }),
        takeover: Box::new(move |_| takeover()),
        inject_write_failure: Box::new(EtcdLog::inject_write_failure),
        storage: Box::new(move || {
            block_on(async {
                use super::kv_log::KvService;
                super::etcd_log::EtcdService::connect(
                    &endpoints,
                    &prefix,
                    ETCD_TEST_MAX_REQUEST_BYTES,
                )
                .await
                .unwrap()
                .dump()
                .await
                .unwrap()
            })
        }),
        _dir: None,
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

/// The writer term `log` holds, as its recovery reports it.
async fn term_of(log: &impl MetaLog) -> u64 {
    log.recover().await.unwrap().term
}

async fn append_each(log: &impl MetaLog, cursor: LogCursor, records: &[Record]) -> Vec<LogTip> {
    let LogCursor { mut tip, term } = cursor;
    let mut tips = Vec::new();
    for record in records {
        let next = log
            .append_and_sync(tip.at(term), vec![record.clone()])
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
    fencing(&make).await;
    status_upper_bound(&make).await;
    large_records(&make).await;
}

/// (1) A stale or forged `expected` fails for both append and admission and
/// changes nothing; the correct tip still appends afterwards.
async fn compare_and_append<L: MetaLog>(make: &impl Fn(Option<CaseLimits>) -> Fixture<L>) {
    let fixture = make(None);
    let log = &fixture.log;
    let term = term_of(log).await;
    let records = test_records(3);
    let tip = log
        .append_and_sync(LogTip::EMPTY.at(term), records[..2].to_vec())
        .await
        .unwrap();
    let before = recovered_view(&log.recover().await.unwrap());
    let forged = LogTip {
        sequence: tip.sequence,
        record_hash: [7; 32],
    };
    for stale in [LogTip::EMPTY, forged] {
        assert!(
            log.append_and_sync(stale.at(term), vec![records[2].clone()])
                .await
                .is_err(),
            "append with stale tip {stale:?} must fail"
        );
        assert!(
            log.can_append(stale.at(term), 64).await.is_err(),
            "stale admission"
        );
    }
    assert_eq!(recovered_view(&log.recover().await.unwrap()), before);
    assert_eq!(log.status().await.unwrap().tip, tip);
    let next = log
        .append_and_sync(tip.at(term), vec![records[2].clone()])
        .await
        .unwrap();
    assert_eq!(next.sequence, 3);
}

/// (2) Groups hold 1..=batch_records_max records; exact byte bounds accept,
/// one byte over rejects without writing.
async fn group_bounds<L: MetaLog>(make: &impl Fn(Option<CaseLimits>) -> Fixture<L>) {
    let fixture = make(None);
    let log = &fixture.log;
    let term = term_of(log).await;
    assert_eq!(log.limits().batch_records_max, BATCH_RECORDS_MAX);
    let records = test_records(BATCH_RECORDS_MAX * 2 + 1);
    assert!(
        log.append_and_sync(LogTip::EMPTY.at(term), Vec::new())
            .await
            .is_err()
    );
    let tip = log
        .append_and_sync(
            LogTip::EMPTY.at(term),
            records[..BATCH_RECORDS_MAX].to_vec(),
        )
        .await
        .unwrap();
    assert_eq!(tip.sequence, BATCH_RECORDS_MAX as u64);
    assert!(
        log.append_and_sync(tip.at(term), records[BATCH_RECORDS_MAX..].to_vec())
            .await
            .is_err(),
        "batch_records_max + 1 records must be rejected"
    );
    assert_eq!(log.recover().await.unwrap().tip, tip);

    let large = padded_first_record(1024);
    let size = frame_size(LogTip::EMPTY.at(term), &large).unwrap();
    for (limit, accepted) in [(size, true), (size - 1, false)] {
        let fixture = make(Some(CaseLimits {
            record_max_bytes: limit,
            batch_max_bytes: limit,
            wal_max_bytes: 1024 * 1024,
        }));
        assert_eq!(term_of(&fixture.log).await, term, "fresh backends agree");
        let result = fixture
            .log
            .append_and_sync(LogTip::EMPTY.at(term), vec![large.clone()])
            .await;
        assert_eq!(result.is_ok(), accepted, "record {size} vs limit {limit}");
        let expected = if accepted { 1 } else { 0 };
        assert_eq!(fixture.log.recover().await.unwrap().tip.sequence, expected);
    }

    let pair = test_records(2);
    let first = frame_size(LogTip::EMPTY.at(term), &pair[0]).unwrap();
    let second = frame_size(
        LogTip {
            sequence: 1,
            record_hash: [0; 32],
        }
        .at(term),
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
        assert_eq!(term_of(&fixture.log).await, term, "fresh backends agree");
        let result = fixture
            .log
            .append_and_sync(LogTip::EMPTY.at(term), pair.clone())
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
    let term = empty.term;
    assert!(term >= 1, "a writer open always selects a term");
    assert_eq!(empty.tip, LogTip::EMPTY);
    assert!(empty.checkpoint.is_none() && empty.records.is_empty());
    let records = test_records(7);
    let mut tip = LogTip::EMPTY;
    for group in [&records[..1], &records[1..4], &records[4..]] {
        let next = log
            .append_and_sync(tip.at(term), group.to_vec())
            .await
            .unwrap();
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
    let again = reopened.recover().await.unwrap();
    assert_eq!(recovered_view(&again), recovered_view(&first));
    assert!(again.term > term, "reconstruction is a new, larger term");
    assert_eq!(reopened.status().await.unwrap().tip, tip);
}

/// (4) Off-chain, future and conflicting anchors are rejected; a valid
/// checkpoint advances `status.checkpoint` and keeps the suffix replayable.
async fn checkpoint_anchors<L: MetaLog>(make: &impl Fn(Option<CaseLimits>) -> Fixture<L>) {
    let fixture = make(None);
    let log = &fixture.log;
    assert!(log.supports_checkpoint(), "both backends checkpoint");
    let term = term_of(log).await;
    let records = test_records(5);
    let tips = append_each(log, LogTip::EMPTY.at(term), &records[..3]).await;
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
    let next = append_each(log, tip.at(term), &records[3..]).await;
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
    let term = term_of(log).await;
    let records = test_records(256);
    let mut tip = LogTip::EMPTY;
    let mut refused = false;
    for record in &records {
        let size = frame_size(tip.at(term), record).unwrap();
        let before = (
            recovered_view(&log.recover().await.unwrap()),
            log.status().await.unwrap(),
            (fixture.storage)(),
        );
        let admitted = log.can_append(tip.at(term), size).await.unwrap();
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
            .append_and_sync(tip.at(term), vec![record.clone()])
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
    let term = term_of(log).await;
    let records = test_records(4);
    let tip = log
        .append_and_sync(LogTip::EMPTY.at(term), records[..2].to_vec())
        .await
        .unwrap();
    (fixture.inject_write_failure)(log);
    assert!(
        log.append_and_sync(tip.at(term), vec![records[2].clone()])
            .await
            .is_err()
    );
    assert!(log.health().await.is_err(), "health after failed write");
    assert!(log.recover().await.is_err(), "recover after failed write");
    assert!(log.status().await.is_err(), "status after failed write");
    assert!(
        log.can_append(tip.at(term), 64).await.is_err(),
        "admission latched"
    );
    assert!(
        log.append_and_sync(tip.at(term), vec![records[2].clone()])
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
    assert!(recovered.term > term);
    reopened
        .append_and_sync(tip.at(recovered.term), records[2..].to_vec())
        .await
        .unwrap();
}

fn assert_fenced<T: std::fmt::Debug>(
    result: afs_error::Result<T>,
    found: u64,
    current: u64,
    what: &str,
) {
    let error = result.expect_err(what);
    assert_eq!(error.code(), afs_error::META_LOG_FENCED, "{what}: {error}");
    let message = error.to_string();
    assert!(
        message.contains(&format!("found term {found}"))
            && message.contains(&format!("current term {current}")),
        "{what}: {message}"
    );
}

/// (7) A newer writer fences every older handle: any operation through the
/// old handle, and append/admission carrying a non-current term, fail
/// `META_LOG_FENCED` without writing a byte; the newer writer continues the
/// same chain, and each reconstruction selects a strictly larger term.
async fn fencing<L: MetaLog>(make: &impl Fn(Option<CaseLimits>) -> Fixture<L>) {
    let fixture = make(None);
    let old = &fixture.log;
    let old_term = term_of(old).await;
    let records = test_records(4);
    let tip = old
        .append_and_sync(LogTip::EMPTY.at(old_term), records[..2].to_vec())
        .await
        .unwrap();
    let new = (fixture.takeover)(old);
    let recovered = new.recover().await.unwrap();
    let term = recovered.term;
    assert!(term > old_term, "takeover term {term} after {old_term}");
    assert_eq!(recovered.tip, tip);
    let before = (recovered_view(&recovered), (fixture.storage)());
    let next = vec![records[2].clone()];
    assert_fenced(
        old.append_and_sync(tip.at(old_term), next.clone()).await,
        old_term,
        term,
        "stale handle append",
    );
    assert_fenced(
        old.can_append(tip.at(old_term), 64).await,
        old_term,
        term,
        "stale handle admission",
    );
    assert_fenced(old.health().await, old_term, term, "stale handle health");
    assert_fenced(old.status().await, old_term, term, "stale handle status");
    assert_fenced(
        old.recover().await.map(|_| ()),
        old_term,
        term,
        "stale recover",
    );
    assert_fenced(
        old.install_checkpoint(tip, image_after(&records[..2]))
            .await,
        old_term,
        term,
        "stale handle checkpoint",
    );
    for (stale, what) in [(old_term, "old term"), (term + 1, "future term")] {
        assert_fenced(
            new.append_and_sync(tip.at(stale), next.clone()).await,
            stale,
            term,
            what,
        );
        assert_fenced(new.can_append(tip.at(stale), 64).await, stale, term, what);
    }
    assert!(
        (
            recovered_view(&new.recover().await.unwrap()),
            (fixture.storage)()
        ) == before,
        "fenced operations wrote nothing"
    );
    let after = new
        .append_and_sync(tip.at(term), next.clone())
        .await
        .unwrap();
    assert_fenced(
        old.append_and_sync(after.at(old_term), vec![records[3].clone()])
            .await,
        old_term,
        term,
        "still fenced after the newer writer appended",
    );
    let Fixture {
        log, reopen, _dir, ..
    } = fixture;
    drop(log);
    let reopened = reopen(new);
    let last = reopened.recover().await.unwrap();
    assert!(last.term > term);
    assert_eq!(last.tip, after);
    assert_eq!(encoded(&last.records), encoded(&records[..3]));
}

/// (8) The status byte bound never falls below the true framed bytes after
/// the checkpoint.
async fn status_upper_bound<L: MetaLog>(make: &impl Fn(Option<CaseLimits>) -> Fixture<L>) {
    let fixture = make(None);
    let log = &fixture.log;
    let term = term_of(log).await;
    let records = test_records(6);
    let mut sizes = Vec::new();
    let mut tips = Vec::new();
    let mut tip = LogTip::EMPTY;
    for record in &records {
        sizes.push(frame_size(tip.at(term), record).unwrap() as u64);
        tip = log
            .append_and_sync(tip.at(term), vec![record.clone()])
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

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
#[ignore = "requires a dedicated Redis: set AFS_TEST_REDIS_URL and run with --ignored"]
async fn redis_log_satisfies_contract() {
    let _shared = REDIS_GATE.read().await;
    contract_suite(redis_fixture).await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
#[ignore = "requires a dedicated etcd: set AFS_TEST_ETCD_ENDPOINTS and run with --ignored"]
async fn etcd_log_satisfies_contract() {
    contract_suite(etcd_fixture).await;
}

/// (9) Records of 4 MiB (over twice etcd's default 1.5 MiB request bound),
/// alone and in a group, append, recover and survive reconstruction byte for
/// byte; an older writer's large append is fenced without writing a byte.
async fn large_records<L: MetaLog>(make: &impl Fn(Option<CaseLimits>) -> Fixture<L>) {
    let fixture = make(None);
    let log = &fixture.log;
    let term = term_of(log).await;
    let records = super::kv_log_tests::padded_records(4, 4 * 1024 * 1024);
    assert!(frame_size(LogTip::EMPTY.at(term), &records[0]).unwrap() > 4 * 1024 * 1024);
    let tip = log
        .append_and_sync(LogTip::EMPTY.at(term), records[..1].to_vec())
        .await
        .unwrap();
    let tip = log
        .append_and_sync(tip.at(term), records[1..3].to_vec())
        .await
        .unwrap();
    assert_eq!(tip.sequence, 3);
    let first = log.recover().await.unwrap();
    assert_eq!(encoded(&first.records), encoded(&records[..3]));
    let new = (fixture.takeover)(log);
    let current = term_of(&new).await;
    let before = (fixture.storage)();
    assert_fenced(
        log.append_and_sync(tip.at(term), records[3..].to_vec())
            .await,
        term,
        current,
        "stale handle large append",
    );
    assert!(
        (fixture.storage)() == before,
        "fenced large append wrote nothing"
    );
    let last = new
        .append_and_sync(tip.at(current), records[3..].to_vec())
        .await
        .unwrap();
    let Fixture {
        log, reopen, _dir, ..
    } = fixture;
    drop(log);
    let again = reopen(new).recover().await.unwrap();
    assert_eq!(again.tip, last);
    assert_eq!(encoded(&again.records), encoded(&records));
}
