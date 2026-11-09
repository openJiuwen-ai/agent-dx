//! Backend-specific tests of the shared-service Meta log (`kv_log`) on Redis
//! and etcd: chain damage, checkpoint chunking, leftover cleanup and Redis's
//! durability probe. Like the shared contract suite they need a dedicated
//! service and are `#[ignore]`d; selected without an endpoint they fail.

use super::{
    NodeSessionLease, RequestKey,
    journal::{LogTip, MetaLog, encode_frame, test_records},
    kv_log::{CrashPoint, KvLog, KvService, RedisLog, arm_crash},
    log::{Record, StateImage},
    log_contract_tests::{
        ETCD_TEST_MAX_REQUEST_BYTES, REDIS_GATE, etcd_endpoints, kv_options, redis_url,
        unique_prefix,
    },
    state::State,
};
use std::{future::Future, pin::Pin, time::Duration};

type Connect<S> = Box<dyn Fn() -> Pin<Box<dyn Future<Output = S> + Send>>>;
type Open<S> = Box<dyn Fn() -> Pin<Box<dyn Future<Output = afs_error::Result<KvLog<S>>> + Send>>>;

fn redis_pair(
    prefix: String,
    chunk: Option<usize>,
) -> (
    Connect<super::redis_log::RedisService>,
    Open<super::redis_log::RedisService>,
) {
    let url = redis_url();
    let options = kv_options(None);
    let (connect_url, connect_prefix) = (url.clone(), prefix.clone());
    let connect: Connect<super::redis_log::RedisService> = Box::new(move || {
        let (url, prefix) = (connect_url.clone(), connect_prefix.clone());
        Box::pin(async move {
            super::redis_log::RedisService::connect(&url, &prefix)
                .await
                .unwrap()
        })
    });
    let open: Open<super::redis_log::RedisService> = Box::new(move || {
        let (url, prefix) = (url.clone(), prefix.clone());
        Box::pin(async move {
            let mut service = super::redis_log::RedisService::connect(&url, &prefix).await?;
            if let Some(bytes) = chunk {
                service = service.with_chunk_bytes(bytes);
            }
            KvLog::open(service, options).await
        })
    });
    (connect, open)
}

fn etcd_pair(
    prefix: String,
    max_request_bytes: usize,
) -> (
    Connect<super::etcd_log::EtcdService>,
    Open<super::etcd_log::EtcdService>,
) {
    let endpoints = etcd_endpoints();
    let options = kv_options(None);
    let (connect_endpoints, connect_prefix) = (endpoints.clone(), prefix.clone());
    let connect: Connect<super::etcd_log::EtcdService> = Box::new(move || {
        let (endpoints, prefix) = (connect_endpoints.clone(), connect_prefix.clone());
        Box::pin(async move {
            super::etcd_log::EtcdService::connect(&endpoints, &prefix, max_request_bytes)
                .await
                .unwrap()
        })
    });
    let open: Open<super::etcd_log::EtcdService> = Box::new(move || {
        let (endpoints, prefix) = (endpoints.clone(), prefix.clone());
        Box::pin(async move {
            let service =
                super::etcd_log::EtcdService::connect(&endpoints, &prefix, max_request_bytes)
                    .await?;
            KvLog::open(service, options).await
        })
    });
    (connect, open)
}

fn tip_bytes(tip: LogTip) -> Vec<u8> {
    let mut bytes = tip.sequence.to_be_bytes().to_vec();
    bytes.extend_from_slice(&tip.record_hash);
    bytes
}

/// A chain whose frame terms decrease, or exceed the current term, is
/// refused at open and left in place.
async fn refuses_bad_chain_terms<S: KvService>(make: impl Fn() -> (Connect<S>, Open<S>)) {
    for (terms, marker) in [
        ([2u64, 1], "decreases below previous term 2"),
        ([1, 9], "exceeds current term"),
    ] {
        let (connect, open) = make();
        let log = open().await.unwrap();
        let identity = log.identity();
        drop(log);
        // Reach term 2 so a term-2 frame is legal when written.
        let log = open().await.unwrap();
        let writer_term = log.term();
        drop(log);
        let service = connect().await;
        let mut tip = LogTip::EMPTY;
        for (record, term) in test_records(2).into_iter().zip(terms) {
            let (frame, next) = encode_frame(identity, tip.at(term), record, 1024 * 1024).unwrap();
            let expected = (tip != LogTip::EMPTY).then(|| tip_bytes(tip));
            let outcome = service
                .append(
                    writer_term,
                    expected.as_deref(),
                    &tip_bytes(next),
                    tip.sequence + 1,
                    &[frame],
                )
                .await
                .unwrap();
            assert_eq!(outcome, super::kv_log::Swap::Done);
            tip = next;
        }
        let before = service.dump().await.unwrap();
        let error = open().await.err().expect("bad chain terms must be refused");
        assert_eq!(error.code(), afs_error::CONFIG_INVALID);
        assert!(error.to_string().contains(marker), "{error}");
        // The refused open took a term but kept every frame.
        let after = service.dump().await.unwrap();
        let frames = |dump: &[(String, Vec<u8>)]| {
            dump.iter()
                .filter(|(key, _)| key.contains("/wal/"))
                .cloned()
                .collect::<Vec<_>>()
        };
        assert_eq!(frames(&after), frames(&before));
        assert_eq!(frames(&after).len(), 2);
    }
}

/// Records whose framed size is about `pad` bytes each, a valid chain.
pub(super) fn padded_records(count: usize, pad: usize) -> Vec<Record> {
    let mut state = State::default();
    let mut records = Vec::new();
    for number in 0..count {
        let (next, _, record) = state
            .prepare_node_record(
                RequestKey::new("kv", format!("padded-{number}")),
                NodeSessionLease {
                    node_id: format!("node-{number}"),
                    session_id: format!("session-{number}"),
                    grpc_addr: "g".repeat(pad),
                    data_addr: "data".into(),
                    rest_addr: "rest".into(),
                    ownerfs_enabled: true,
                    storage_devices: Vec::new(),
                    lease_ttl: Duration::from_secs(30),
                },
                12345,
                64 * 1024 * 1024,
            )
            .unwrap();
        state = next;
        records.push(record.unwrap().into());
    }
    records
}

fn image_after(records: &[Record]) -> Vec<u8> {
    let mut state = State::default();
    for record in records {
        state = state.replay_record(record).unwrap();
    }
    StateImage::encode(&state, 64 * 1024 * 1024).unwrap()
}

/// A checkpoint larger than one chunk is written in several chunks, selected
/// once, recovered byte for byte; leftovers of a crashed install (an older
/// writer's unselected chunks, covered frames) are removed at the next open.
async fn checkpoint_chunks_and_cleanup<S: KvService>(
    connect: Connect<S>,
    open: Open<S>,
    pad: usize,
) {
    let records = padded_records(6, pad);
    let log = open().await.unwrap();
    let term = log.term();
    let mut tip = LogTip::EMPTY;
    for record in &records {
        tip = log
            .append_and_sync(tip.at(term), vec![record.clone()])
            .await
            .unwrap();
    }
    let covered = log.recover().await.unwrap();
    assert_eq!(covered.tip, tip);
    let tips = {
        let mut tips = Vec::new();
        let mut cursor = LogTip::EMPTY;
        let identity = log.identity();
        for record in &records {
            let (_, next) =
                encode_frame(identity, cursor.at(term), record.clone(), 64 * 1024 * 1024).unwrap();
            tips.push(next);
            cursor = next;
        }
        tips
    };
    let image = image_after(&records[..4]);
    log.install_checkpoint(tips[3], image.clone())
        .await
        .unwrap();
    let service = connect().await;
    let dump = service.dump().await.unwrap();
    let chunk_keys = dump
        .iter()
        .filter(|(key, _)| key.contains("/chunk/"))
        .count();
    assert!(
        chunk_keys >= 2,
        "image of {} bytes must span chunks (got {chunk_keys})",
        image.len()
    );
    assert!(
        !dump
            .iter()
            .any(|(key, _)| key.ends_with("/wal/00000000000000000001")),
        "covered frames are deleted after selection"
    );
    let status = log.status().await.unwrap();
    assert_eq!((status.tip, status.checkpoint), (tip, tips[3]));
    // A crash after writing chunks but before selecting them.
    service
        .put_chunk(&format!("{term}-999"), 0, b"orphan")
        .await
        .unwrap();
    drop(log);
    let reopened = open().await.unwrap();
    let recovered = reopened.recover().await.unwrap();
    assert_eq!(recovered.tip, tip);
    let checkpoint = recovered.checkpoint.unwrap();
    assert_eq!((checkpoint.covered, checkpoint.bytes), (tips[3], image));
    assert_eq!(recovered.records.len(), 2);
    let ids = service.chunk_ids().await.unwrap();
    assert_eq!(
        ids,
        vec![format!("{term}-4")],
        "orphan chunks of term {term} removed"
    );
    drop(reopened);
    let again = open().await.unwrap();
    assert_eq!(again.recover().await.unwrap().tip, tip);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
#[ignore = "requires a dedicated Redis: set AFS_TEST_REDIS_URL and run with --ignored"]
async fn redis_refuses_bad_chain_terms() {
    let _shared = REDIS_GATE.read().await;
    refuses_bad_chain_terms(|| redis_pair(unique_prefix(), None)).await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
#[ignore = "requires a dedicated etcd: set AFS_TEST_ETCD_ENDPOINTS and run with --ignored"]
async fn etcd_refuses_bad_chain_terms() {
    refuses_bad_chain_terms(|| etcd_pair(unique_prefix(), ETCD_TEST_MAX_REQUEST_BYTES)).await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
#[ignore = "requires a dedicated Redis: set AFS_TEST_REDIS_URL and run with --ignored"]
async fn redis_checkpoint_chunks_and_cleanup() {
    let _shared = REDIS_GATE.read().await;
    let (connect, open) = redis_pair(unique_prefix(), Some(4096));
    checkpoint_chunks_and_cleanup(connect, open, 2048).await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
#[ignore = "requires a dedicated etcd: set AFS_TEST_ETCD_ENDPOINTS and run with --ignored"]
async fn etcd_checkpoint_chunks_and_cleanup() {
    // 256 KiB requests: 192 KiB chunks; four ~100 KiB records span two.
    let (connect, open) = etcd_pair(unique_prefix(), 256 * 1024);
    checkpoint_chunks_and_cleanup(connect, open, 100 * 1024).await;
}

/// Redis is refused unless an acknowledged write is durable: `appendfsync`
/// other than `always`, an eviction policy, or a TTL on a Meta key.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
#[ignore = "requires a dedicated Redis: set AFS_TEST_REDIS_URL and run with --ignored"]
async fn redis_probe_refuses_settings_that_lose_acknowledged_writes() {
    let _exclusive = REDIS_GATE.write().await;
    let url = redis_url();
    let client = redis::Client::open(url.as_str()).unwrap();
    let mut connection = client.get_multiplexed_async_connection().await.unwrap();
    let options = kv_options(None);
    for (name, unsafe_value, safe_value) in [
        ("appendfsync", "everysec", "always"),
        ("maxmemory-policy", "allkeys-lru", "noeviction"),
    ] {
        let _: () = redis::cmd("CONFIG")
            .arg("SET")
            .arg(name)
            .arg(unsafe_value)
            .query_async(&mut connection)
            .await
            .unwrap();
        let refused = RedisLog::open_redis(&url, &unique_prefix(), options).await;
        let _: () = redis::cmd("CONFIG")
            .arg("SET")
            .arg(name)
            .arg(safe_value)
            .query_async(&mut connection)
            .await
            .unwrap();
        let error = refused.err().expect("unsafe Redis setting must be refused");
        assert_eq!(error.code(), afs_error::CONFIG_INVALID, "{name}");
        assert!(error.to_string().contains(name), "{error}");
    }
    let prefix = unique_prefix();
    drop(RedisLog::open_redis(&url, &prefix, options).await.unwrap());
    let _: () = redis::cmd("PEXPIRE")
        .arg(format!("{prefix}/term"))
        .arg(600_000)
        .query_async(&mut connection)
        .await
        .unwrap();
    let error = RedisLog::open_redis(&url, &prefix, options)
        .await
        .err()
        .expect("a TTL on a Meta key must be refused");
    assert_eq!(error.code(), afs_error::CONFIG_INVALID);
    assert!(error.to_string().contains("TTL"), "{error}");
}

/// Records of about 600 KiB: staged on both test services (etcd with 256 KiB
/// requests, Redis with 256 KiB segments).
const STAGED_PAD: usize = 600 * 1024;

async fn owners<S: KvService>(connect: &Connect<S>) -> Vec<(u64, u64)> {
    connect().await.part_owners().await.unwrap()
}

/// A crash with one segment written, with all segments written and not
/// committed, and after the commit: recovery shows exactly the committed
/// records, and no uncommitted segment is ever assembled into one.
async fn staged_crash_atomicity<S: KvService>(make: impl Fn() -> (Connect<S>, Open<S>)) {
    for (point, committed) in [
        (CrashPoint::PartsPartial, false),
        (CrashPoint::PartsWritten, false),
        (CrashPoint::AfterCommit, true),
    ] {
        let (connect, open) = make();
        let records = padded_records(3, STAGED_PAD);
        let log = open().await.unwrap();
        let term = log.term();
        let tip = log
            .append_and_sync(LogTip::EMPTY.at(term), records[..1].to_vec())
            .await
            .unwrap();
        arm_crash(log.identity(), point);
        assert!(
            log.append_and_sync(tip.at(term), records[1..2].to_vec())
                .await
                .is_err(),
            "{point:?}: injected crash"
        );
        assert!(
            !owners(&connect).await.is_empty(),
            "{point:?}: segments written"
        );
        drop(log);
        let reopened = open().await.unwrap();
        let recovered = reopened.recover().await.unwrap();
        let want = if committed { 2 } else { 1 };
        assert_eq!(recovered.records.len(), want, "{point:?}");
        assert_eq!(
            recovered
                .records
                .iter()
                .map(|r| serde_json::to_vec(r).unwrap())
                .collect::<Vec<_>>(),
            records[..want]
                .iter()
                .map(|r| serde_json::to_vec(r).unwrap())
                .collect::<Vec<_>>(),
            "{point:?}: only committed records"
        );
        // Uncommitted segments are gone; a committed record keeps its own.
        let live = owners(&connect).await;
        assert_eq!(
            live.len(),
            usize::from(committed) + 1,
            "{point:?}: {live:?}"
        );
        // The next writer appends the same sequence with its own segments.
        let term = reopened.term();
        let next = records[want..].to_vec();
        reopened
            .append_and_sync(recovered.tip.at(term), next.clone())
            .await
            .unwrap();
        drop(reopened);
        let last = open().await.unwrap().recover().await.unwrap();
        assert_eq!(last.records.len(), 3, "{point:?}");
    }
}

/// An older writer that keeps staging after a newer writer took over can
/// neither commit nor overwrite the newer writer's segments.
async fn staged_fencing<S: KvService>(connect: Connect<S>, open: Open<S>) {
    let records = padded_records(2, STAGED_PAD);
    let old = open().await.unwrap();
    let old_term = old.term();
    let tip = old
        .append_and_sync(LogTip::EMPTY.at(old_term), records[..1].to_vec())
        .await
        .unwrap();
    let new = open().await.unwrap();
    let term = new.term();
    // The old writer's handle refuses before writing anything.
    let error = old
        .append_and_sync(tip.at(old_term), records[1..].to_vec())
        .await
        .unwrap_err();
    assert_eq!(error.code(), afs_error::META_LOG_FENCED);
    // An old writer racing past that check stages under its own term and
    // then tries to commit a stub: the commit compares term and tip.
    let service = connect().await;
    service
        .put_part(old_term, 2, 0, b"stale segment")
        .await
        .unwrap();
    let stub = {
        let mut bytes = b"AFSKPART".to_vec();
        bytes.extend_from_slice(&old_term.to_be_bytes());
        bytes.extend_from_slice(&2u64.to_be_bytes());
        bytes.extend_from_slice(&1u32.to_be_bytes());
        bytes.extend_from_slice(&13u64.to_be_bytes());
        bytes.extend_from_slice(&[0; 32]);
        bytes
    };
    let mut fake_tip = 2u64.to_be_bytes().to_vec();
    fake_tip.extend_from_slice(&[1; 32]);
    let outcome = service
        .append(old_term, Some(&tip_bytes(tip)), &fake_tip, 2, &[stub])
        .await
        .unwrap();
    assert_eq!(outcome, super::kv_log::Swap::Fenced(term));
    let next = new
        .append_and_sync(tip.at(term), records[1..].to_vec())
        .await
        .unwrap();
    drop((old, new));
    let reopened = open().await.unwrap();
    let recovered = reopened.recover().await.unwrap();
    assert_eq!(recovered.tip, next);
    assert_eq!(recovered.records.len(), 2);
    let live = owners(&connect).await;
    // The old writer's committed record keeps its segments; its stale
    // segment for sequence 2 is removed.
    assert_eq!(live, vec![(old_term, 1), (term, 2)], "{live:?}");
}

/// A committed staged record with a missing or altered segment refuses
/// recovery, naming the frame and segment.
async fn staged_integrity<S: KvService>(make: impl Fn() -> (Connect<S>, Open<S>)) {
    for damage in ["missing", "altered"] {
        let (connect, open) = make();
        let records = padded_records(1, STAGED_PAD);
        let log = open().await.unwrap();
        let term = log.term();
        log.append_and_sync(LogTip::EMPTY.at(term), records.clone())
            .await
            .unwrap();
        drop(log);
        let service = connect().await;
        let (owner, sequence) = service.part_owners().await.unwrap()[0];
        if damage == "missing" {
            service.delete_parts(owner, sequence).await.unwrap();
        } else {
            let mut part = service.part(owner, sequence, 1).await.unwrap().unwrap();
            part[100] ^= 1;
            service.put_part(owner, sequence, 1, &part).await.unwrap();
        }
        let error = open()
            .await
            .err()
            .expect("damaged staged record must be refused");
        assert_eq!(error.code(), afs_error::CONFIG_INVALID, "{damage}");
        let message = error.to_string();
        let marker = if damage == "missing" {
            "segment 0 missing"
        } else {
            "do not match the recorded hash"
        };
        assert!(
            message.contains("frame 1 (staged by term") && message.contains(marker),
            "{damage}: {message}"
        );
    }
}

/// Crashing before the commit again and again never accumulates segments:
/// each open removes the previous writer's uncommitted ones.
async fn staged_cleanup_bounded<S: KvService>(connect: Connect<S>, open: Open<S>) {
    let records = padded_records(1, STAGED_PAD);
    for round in 0..5 {
        let log = open().await.unwrap();
        assert!(
            owners(&connect).await.is_empty(),
            "round {round}: leftovers removed at open"
        );
        arm_crash(log.identity(), CrashPoint::PartsWritten);
        let term = log.term();
        assert!(
            log.append_and_sync(LogTip::EMPTY.at(term), records.clone())
                .await
                .is_err()
        );
        assert_eq!(owners(&connect).await.len(), 1, "round {round}");
    }
    let log = open().await.unwrap();
    assert!(owners(&connect).await.is_empty());
    assert_eq!(log.recover().await.unwrap().records.len(), 0);
}

fn redis_staged() -> (
    Connect<super::redis_log::RedisService>,
    Open<super::redis_log::RedisService>,
) {
    redis_pair(unique_prefix(), Some(256 * 1024))
}

fn etcd_staged() -> (
    Connect<super::etcd_log::EtcdService>,
    Open<super::etcd_log::EtcdService>,
) {
    etcd_pair(unique_prefix(), 256 * 1024)
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
#[ignore = "requires a dedicated Redis: set AFS_TEST_REDIS_URL and run with --ignored"]
async fn redis_staged_records() {
    let _shared = REDIS_GATE.read().await;
    staged_crash_atomicity(redis_staged).await;
    let (connect, open) = redis_staged();
    staged_fencing(connect, open).await;
    staged_integrity(redis_staged).await;
    let (connect, open) = redis_staged();
    staged_cleanup_bounded(connect, open).await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
#[ignore = "requires a dedicated etcd: set AFS_TEST_ETCD_ENDPOINTS and run with --ignored"]
async fn etcd_staged_records() {
    staged_crash_atomicity(etcd_staged).await;
    let (connect, open) = etcd_staged();
    staged_fencing(connect, open).await;
    staged_integrity(etcd_staged).await;
    let (connect, open) = etcd_staged();
    staged_cleanup_bounded(connect, open).await;
}
