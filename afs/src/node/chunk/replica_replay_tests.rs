use super::*;

#[cfg(feature = "dfs")]
#[test]
fn buffer_reuse_peer_payload_lease_and_bytes_outlive_last_frame() {
    let temp = tempfile::tempdir().unwrap();
    let store = LocalChunkStore::open(temp.path(), "node-a")
        .unwrap()
        .with_peer_read_crc_mode(crate::config::DfsReadCrcMode::Off);
    let staged = StagedChunk::new_crc32c(OperationId::new("pool-lease"), vec![41; 65536]).unwrap();
    store.put(staged.clone()).unwrap();
    let payload = store.read_peer_range(&staged.chunk.id, 0, 65536).unwrap();
    let ptr = payload.as_slice().as_ptr();
    let bytes = payload.into_shared_bytes();
    let frame = bytes.slice(7..31);
    drop(bytes);
    assert!(store.peer_read_budget.used.load(Ordering::Acquire) > 0);
    let other = store.read_peer_range(&staged.chunk.id, 0, 65536).unwrap();
    assert_ne!(other.as_slice().as_ptr(), ptr);
    assert_eq!(&frame[..], &[41; 24]);
    drop(frame);
    let reused = store.read_peer_range(&staged.chunk.id, 0, 65536).unwrap();
    assert_eq!(reused.as_slice().as_ptr(), ptr);
    drop(other);
    drop(reused);
    assert_eq!(store.peer_read_budget.used.load(Ordering::Acquire), 0);
}

#[test]
fn buffer_reuse_crc_off_reads_final_payload_without_scratch_copy() {
    let temp = tempfile::tempdir().unwrap();
    let registry = afs_metrics::Registry::new();
    let metrics = super::super::read_work::ReadWorkMetrics::register(&registry).unwrap();
    let store = LocalChunkStore::open(temp.path(), "node-a")
        .unwrap()
        .with_peer_read_crc_mode(crate::config::DfsReadCrcMode::Off)
        .with_read_work_metrics(metrics);
    let body: Vec<u8> = (0..131085).map(|i| (i % 251) as u8).collect();
    let staged = StagedChunk::new_crc32c(OperationId::new("direct"), body.clone()).unwrap();
    store.put(staged.clone()).unwrap();
    for (offset, length) in [(0, 131085), (9, 70000), (131080, 5)] {
        let payload = store
            .read_peer_range(&staged.chunk.id, offset as u64, length)
            .unwrap();
        assert_eq!(payload.as_slice(), &body[offset..offset + length]);
    }
    let copies: u64 = registry
        .gather()
        .iter()
        .filter(|f| f.name() == "afs_dfs_read_copy_bytes_total")
        .flat_map(|f| f.get_metric())
        .filter(|m| {
            m.get_label()
                .iter()
                .any(|l| l.value() == "crc_storage_to_range")
        })
        .map(|m| m.get_counter().get_value() as u64)
        .sum();
    assert_eq!(
        copies, 0,
        "off read must fill final owner without scratch copy"
    );
}

#[test]
fn p2_w3_uncontrolled_sixteen_requests_report_actual_groups_and_barriers() {
    let temp = tempfile::tempdir().unwrap();
    let registry = afs_metrics::Registry::new();
    let metrics = super::super::read_work::ReadWorkMetrics::register(&registry)
        .unwrap()
        .with_read_diagnostics(true);
    let store = Arc::new(
        LocalChunkStore::open(temp.path(), "node-a")
            .unwrap()
            .with_read_work_metrics(metrics.clone()),
    );
    assert!(store.catalog_commit_test.ready.lock().unwrap().is_none());
    let start = Arc::new(std::sync::Barrier::new(17));
    let staged: Vec<_> = (0..16)
        .map(|index| {
            StagedChunk::new(
                OperationId::new(format!("uncontrolled-{index}")),
                vec![index; 64],
            )
        })
        .collect();
    let workers: Vec<_> = staged
        .iter()
        .cloned()
        .enumerate()
        .map(|(index, item)| {
            let store = store.clone();
            let start = start.clone();
            std::thread::spawn(move || {
                start.wait(); // Entry only: no ready / batch-taking barrier.
                (index, store.put(item).unwrap())
            })
        })
        .collect();
    start.wait();
    let receipts: Vec<_> = workers
        .into_iter()
        .map(|worker| worker.join().unwrap())
        .collect();
    let wal = fs::read(&store.catalog_path).unwrap();
    let transactions: Vec<CatalogTxn> = wal
        .split(|byte| *byte == b'\n')
        .filter(|line| !line.is_empty())
        .map(|line| serde_json::from_slice(line).unwrap())
        .collect();
    let stats = store.catalog_commit_stats();
    assert!((1..=16).contains(&transactions.len()));
    assert_eq!(stats.groups, transactions.len() as u64);
    assert_eq!(
        transactions
            .iter()
            .map(|txn| txn.records.len())
            .sum::<usize>(),
        16
    );
    assert_eq!(
        stats.max_group,
        transactions
            .iter()
            .map(|txn| txn.records.len())
            .max()
            .unwrap()
    );
    assert_eq!((stats.requests, stats.ready, stats.waiters), (0, 0, 0));
    assert!(!stats.writer && !stats.failed);
    for (index, txn) in transactions.iter().enumerate() {
        assert_eq!(txn.revision, index as u64 + 1);
        assert_eq!(
            catalog_checksum(txn.revision, &txn.records).unwrap(),
            txn.checksum
        );
        assert!(
            txn.records
                .iter()
                .all(|record| record.catalog_revision == txn.revision)
        );
    }
    let snapshot = metrics.diagnostics_snapshot().unwrap();
    let calls = |phase: &str| {
        snapshot
            .coarse_phases
            .iter()
            .find(|item| item.phase == phase)
            .map_or(0, |item| item.calls)
    };
    for phase in [
        "persist_chunks_dir_sync",
        "persist_catalog_write",
        "persist_catalog_sync",
        "persist_catalog_parent_sync",
    ] {
        assert_eq!(
            calls(phase),
            stats.groups,
            "actual successful {phase} must match WAL groups"
        );
    }
    assert_eq!(calls("persist_body_sync"), 16);
    for (index, receipt) in &receipts {
        assert_eq!(receipt.operation_id, staged[*index].operation_id);
        assert_eq!(receipt.chunk, staged[*index].chunk);
        assert_eq!(receipt.durable_acks.len(), 1);
        let revision = receipt.durable_acks[0].catalog_revision;
        let txn = transactions
            .iter()
            .find(|txn| txn.revision == revision)
            .unwrap();
        assert!(
            txn.records
                .iter()
                .any(|record| record.chunk == receipt.chunk)
        );
    }
    eprintln!(
        "W3_UNCONTROLLED_LEDGER {}",
        serde_json::json!({
            "requests": receipts.iter().map(|(index, receipt)| serde_json::json!({
                "request": index, "operation_id": receipt.operation_id.0,
                "chunk_id": receipt.chunk.id.0, "revision": receipt.durable_acks[0].catalog_revision,
            })).collect::<Vec<_>>(),
            "groups": transactions.iter().map(|txn| serde_json::json!({
                "revision": txn.revision, "records": txn.records.len(),
            })).collect::<Vec<_>>(),
            "barriers": {
                "body_sync": calls("persist_body_sync"),
                "chunks_dir_sync": calls("persist_chunks_dir_sync"),
                "wal_write": calls("persist_catalog_write"),
                "wal_sync": calls("persist_catalog_sync"),
                "catalog_parent_sync": calls("persist_catalog_parent_sync"),
            },
        })
    );
    drop(store);
    let recovered = LocalChunkStore::open(temp.path(), "node-a").unwrap();
    for item in staged {
        let mut output = [0; 65];
        assert_eq!(
            recovered.read_at(&item.chunk.id, 0, &mut output).unwrap(),
            64
        );
        assert_eq!(&output[..64], item.bytes());
        assert_eq!(output[64], 0);
    }
}

fn p2_w3_pre_ack_chunks() -> Vec<StagedChunk> {
    (0..4)
        .map(|index| {
            StagedChunk::new(
                OperationId::new(format!("pre-ack-{index}")),
                vec![51 + index; 97],
            )
        })
        .collect()
}

#[test]
#[ignore = "child process only; parent executes both bounded pre-ACK crash modes"]
fn p2_w3_pre_ack_child() {
    let Some(root) = std::env::var_os("AFS_TEST_W3_PRE_ACK_ROOT") else {
        return;
    };
    let mode = std::env::var("AFS_TEST_W3_PRE_ACK_MODE").unwrap();
    let registry = afs_metrics::Registry::new();
    let metrics = super::super::read_work::ReadWorkMetrics::register(&registry)
        .unwrap()
        .with_read_diagnostics(true);
    let store = Arc::new(
        LocalChunkStore::open(root, "node-a")
            .unwrap()
            .with_read_work_metrics(metrics.clone()),
    );
    if mode == "before-wal" {
        let (block, entered, _release) = p2_w3_block();
        store
            .catalog_commit_test
            .before_wal
            .lock()
            .unwrap()
            .push_back(block);
        let worker_store = store.clone();
        let _blocked_worker = std::thread::spawn(move || {
            worker_store.put_batch(p2_w3_pre_ack_chunks()).unwrap();
            panic!("pre-ACK worker unexpectedly returned a successful receipt");
        });
        entered.recv_timeout(Duration::from_secs(5)).unwrap();
        let snapshot = metrics.diagnostics_snapshot().unwrap();
        let calls = |phase: &str| {
            snapshot
                .coarse_phases
                .iter()
                .find(|item| item.phase == phase)
                .map_or(0, |item| item.calls)
        };
        assert_eq!(calls("persist_body_sync"), 4);
        assert_eq!(calls("persist_chunks_dir_sync"), 1);
        assert_eq!(calls("persist_catalog_write"), 0);
        eprintln!(
            "W3_PRE_ACK before-wal body_sync=4 chunks_dir_sync=1 wal_write=0 successful_ack=0"
        );
        // No unwinding or store Drop: the writer is still blocked before WAL.
        std::process::exit(74);
    }
    assert_eq!(mode, "half-wal");
    store
        .catalog_commit_test
        .fail_wal_step
        .store(1, Ordering::Release);
    assert!(store.put_batch(p2_w3_pre_ack_chunks()).is_err());
    assert!(store.catalog_commit_stats().failed);
    eprintln!("W3_PRE_ACK half-wal real_partial_write=true successful_ack=0");
    // Existing test hook writes a real partial multi-record transaction;
    // it does not emulate physical fsync EIO or loss of OS page cache.
    std::process::exit(75);
}

fn p2_w3_run_pre_ack_child(root: &Path, mode: &str) {
    let log = root.join(format!("pre-ack-{mode}.log"));
    let output = File::create(&log).unwrap();
    let mut child = std::process::Command::new(std::env::current_exe().unwrap())
        .args([
            "--ignored",
            "--exact",
            "node::chunk::replica_replay_tests::p2_w3_pre_ack_child",
            "--nocapture",
            "--test-threads=1",
        ])
        .env("AFS_TEST_W3_PRE_ACK_ROOT", root)
        .env("AFS_TEST_W3_PRE_ACK_MODE", mode)
        .stdout(output.try_clone().unwrap())
        .stderr(output)
        .spawn()
        .unwrap();
    let deadline = Instant::now() + Duration::from_secs(15);
    let status = loop {
        match child.try_wait() {
            Ok(Some(status)) => break status,
            Ok(None) if Instant::now() < deadline => std::thread::sleep(Duration::from_millis(10)),
            failure => {
                let _ = child.kill();
                let _ = child.wait();
                panic!(
                    "pre-ACK child timeout / wait error {failure:?}: {}",
                    fs::read_to_string(&log).unwrap_or_default()
                );
            }
        }
    };
    assert_eq!(
        status.code(),
        Some(if mode == "before-wal" { 74 } else { 75 }),
        "{}",
        fs::read_to_string(&log).unwrap()
    );
    eprintln!("{}", fs::read_to_string(log).unwrap());
}

#[test]
fn p2_w3_pre_ack_process_exits_keep_prefix_orphans_and_exact_retry() {
    for mode in ["before-wal", "half-wal"] {
        let temp = tempfile::tempdir().unwrap();
        let store = LocalChunkStore::open(temp.path(), "node-a").unwrap();
        let prefix = StagedChunk::new(
            OperationId::new("already-acked-prefix"),
            b"prefix-durable".to_vec(),
        );
        let prefix_ack = store.put(prefix.clone()).unwrap();
        assert_eq!(prefix_ack.durable_acks[0].catalog_revision, 1);
        let prefix_wal = fs::read(&store.catalog_path).unwrap();
        drop(store);
        p2_w3_run_pre_ack_child(temp.path(), mode);
        let expected = p2_w3_pre_ack_chunks();
        let original_files: Vec<_> = expected
            .iter()
            .map(|item| {
                let path = temp.path().join("chunks").join(&item.chunk.id.0);
                let body = fs::read(&path).unwrap();
                assert_eq!(body, item.bytes());
                (path, body)
            })
            .collect();
        let crash_wal = fs::read(temp.path().join("catalog.wal")).unwrap();
        assert!(crash_wal.starts_with(&prefix_wal));
        if mode == "before-wal" {
            assert_eq!(crash_wal, prefix_wal);
        } else {
            assert!(crash_wal.len() > prefix_wal.len());
            let error =
                serde_json::from_slice::<CatalogTxn>(&crash_wal[prefix_wal.len()..]).unwrap_err();
            assert!(
                error.is_eof(),
                "partial final multi-record transaction: {error}"
            );
        }
        let recovered = LocalChunkStore::open(temp.path(), "node-a").unwrap();
        assert_eq!(fs::read(&recovered.catalog_path).unwrap(), prefix_wal);
        let report = recovered.recover().unwrap();
        assert_eq!(report.durable_chunks, 1);
        assert_eq!(report.orphan_files, 4);
        let mut output = [9; 14];
        assert_eq!(
            recovered.read_at(&prefix.chunk.id, 0, &mut output).unwrap(),
            prefix.bytes().len()
        );
        assert_eq!(&output[..prefix.bytes().len()], prefix.bytes());
        for (path, body) in &original_files {
            assert_eq!(
                &fs::read(path).unwrap(),
                body,
                "orphan bytes must not be deleted or rewritten on recovery"
            );
        }
        let retried = recovered.put_batch(expected.clone()).unwrap();
        assert_eq!(retried.len(), 4);
        for (receipt, item) in retried.iter().zip(&expected) {
            assert_eq!(receipt.operation_id, item.operation_id);
            assert_eq!(receipt.chunk, item.chunk);
            assert_eq!(receipt.durable_acks[0].catalog_revision, 2);
        }
        assert_eq!(recovered.recover().unwrap().orphan_files, 0);
        drop(recovered);
        let reopened = LocalChunkStore::open(temp.path(), "node-a").unwrap();
        assert_eq!(reopened.recover().unwrap().durable_chunks, 5);
        for item in &expected {
            let mut body = [0; 98];
            assert_eq!(reopened.read_at(&item.chunk.id, 0, &mut body).unwrap(), 97);
            assert_eq!(&body[..97], item.bytes());
            assert_eq!(body[97], 0);
        }
        eprintln!(
            "W3_PRE_ACK_RECOVERY mode={mode} acked_prefix=1 orphans_preserved=4 retry_records=4 retry_revision=2 restart_durable=5"
        );
    }
}

#[test]
fn p2_w3_sixteen_ready_requests_share_one_durable_group() {
    let temp = tempfile::tempdir().unwrap();
    let registry = afs_metrics::Registry::new();
    let metrics = super::super::read_work::ReadWorkMetrics::register(&registry)
        .unwrap()
        .with_read_diagnostics(true);
    let store = Arc::new(
        LocalChunkStore::open(temp.path(), "node-a")
            .unwrap()
            .with_read_work_metrics(metrics.clone()),
    );
    *store.catalog_commit_test.ready.lock().unwrap() = Some(Arc::new(std::sync::Barrier::new(16)));
    let staged: Vec<_> = (0..16)
        .map(|index| StagedChunk::new(OperationId::new(format!("group-{index}")), vec![index; 64]))
        .collect();
    let workers: Vec<_> = staged
        .iter()
        .cloned()
        .map(|item| {
            let store = store.clone();
            std::thread::spawn(move || store.put(item))
        })
        .collect();
    let receipts: Vec<_> = workers
        .into_iter()
        .map(|worker| worker.join().unwrap().unwrap())
        .collect();
    let snapshot = metrics.diagnostics_snapshot().unwrap();
    for phase in [
        "persist_chunks_dir_sync",
        "persist_catalog_write",
        "persist_catalog_sync",
        "persist_catalog_parent_sync",
    ] {
        assert_eq!(
            snapshot
                .coarse_phases
                .iter()
                .find(|item| item.phase == phase)
                .unwrap()
                .calls,
            1,
            "{phase}: all sixteen requests were ready before taking the group"
        );
    }
    assert_eq!(
        snapshot
            .coarse_phases
            .iter()
            .find(|item| item.phase == "persist_body_sync")
            .unwrap()
            .calls,
        16
    );
    for receipt in &receipts {
        assert_eq!(receipt.durable_acks[0].catalog_revision, 1);
    }
    *store.catalog_commit_test.ready.lock().unwrap() = None;
    let stats = store.catalog_commit_stats();
    assert_eq!(stats.requests, 0);
    assert_eq!(stats.ready, 0);
    assert_eq!(stats.waiters, 0);
    assert!(!stats.writer && !stats.failed && !stats.exclusive);
    assert_eq!(stats.groups, 1);
    assert_eq!(stats.max_group, 16);
    assert!(stats.reserved_bytes <= stats.peak_reserved_bytes);
    assert!(stats.actual_peak_bytes <= stats.peak_reserved_bytes);
    assert!(stats.peak_reserved_bytes <= 1024 * 1024);
    drop(store);
    let recovered = LocalChunkStore::open(temp.path(), "node-a").unwrap();
    for item in staged {
        let mut output = [0; 64];
        assert_eq!(
            recovered.read_at(&item.chunk.id, 0, &mut output).unwrap(),
            64
        );
        assert_eq!(output.as_slice(), item.bytes());
    }
}

fn p2_w3_block() -> (
    PeerFillTestBlock,
    std::sync::mpsc::Receiver<()>,
    std::sync::mpsc::Sender<()>,
) {
    let (entered, observed) = std::sync::mpsc::channel();
    let (release, wait) = std::sync::mpsc::channel();
    ((entered, wait), observed, release)
}

fn p2_w3_wait(
    store: &LocalChunkStore,
    predicate: impl Fn(catalog_commit::CatalogCommitStats) -> bool,
) {
    let deadline = Instant::now() + Duration::from_secs(3);
    while !predicate(store.catalog_commit_stats()) {
        assert!(
            Instant::now() < deadline,
            "catalog condition did not become ready"
        );
        std::thread::sleep(Duration::from_millis(1));
    }
}

#[test]
fn p2_w3_prepare_does_not_hold_catalog_or_block_a_different_id() {
    let temp = tempfile::tempdir().unwrap();
    let store = Arc::new(LocalChunkStore::open(temp.path(), "node-a").unwrap());
    let (block, observed, release) = p2_w3_block();
    *store.catalog_commit_test.before_prepare.lock().unwrap() = Some(block);
    let first_store = store.clone();
    let first = std::thread::spawn(move || {
        first_store.put(StagedChunk::new(
            OperationId::new("blocked-prepare"),
            b"first".to_vec(),
        ))
    });
    observed.recv_timeout(Duration::from_secs(3)).unwrap();
    let other_store = store.clone();
    let (done, completion) = std::sync::mpsc::channel();
    let second = std::thread::spawn(move || {
        let result = other_store.device_descriptor().and_then(|_| {
            other_store.put(StagedChunk::new(
                OperationId::new("independent"),
                b"second".to_vec(),
            ))
        });
        done.send(result).unwrap();
    });
    let while_blocked = completion.recv_timeout(Duration::from_secs(1));
    release.send(()).unwrap();
    assert!(first.join().unwrap().is_ok());
    second.join().unwrap();
    assert!(
        while_blocked.unwrap().is_ok(),
        "different object must progress while preparation is blocked"
    );
}

#[test]
fn p2_w3_same_id_replay_and_quarantine_wait_for_the_install_flight() {
    let temp = tempfile::tempdir().unwrap();
    let store = Arc::new(LocalChunkStore::open(temp.path(), "node-a").unwrap());
    let staged = StagedChunk::new(OperationId::new("same-flight"), b"same".to_vec());
    let (block, observed, release) = p2_w3_block();
    store
        .catalog_commit_test
        .before_wal
        .lock()
        .unwrap()
        .push_back(block);
    let first_store = store.clone();
    let first_item = staged.clone();
    let first = std::thread::spawn(move || first_store.put(first_item));
    observed.recv_timeout(Duration::from_secs(3)).unwrap();
    let replay_store = store.clone();
    let replay_item = staged.clone();
    let replay = std::thread::spawn(move || replay_store.put(replay_item));
    let quarantine_store = store.clone();
    let id = staged.chunk.id.clone();
    let quarantine = std::thread::spawn(move || quarantine_store.quarantine_if_invalid(&id));
    p2_w3_wait(&store, |stats| stats.waiters == 2);
    release.send(()).unwrap();
    let ack = first.join().unwrap().unwrap();
    let replay_ack = replay.join().unwrap().unwrap();
    assert_eq!(
        ack.durable_acks[0].catalog_revision,
        replay_ack.durable_acks[0].catalog_revision
    );
    assert!(quarantine.join().unwrap().is_ok());
    assert!(store.quarantined_chunks().unwrap().is_empty());
    assert_eq!(store.catalog_commit_stats().groups, 1);
}

#[test]
fn p2_w3_first_group_ack_does_not_wait_for_a_blocked_following_group() {
    let temp = tempfile::tempdir().unwrap();
    let store = Arc::new(LocalChunkStore::open(temp.path(), "node-a").unwrap());
    let (block1, observed1, release1) = p2_w3_block();
    let (block2, observed2, release2) = p2_w3_block();
    store
        .catalog_commit_test
        .before_wal
        .lock()
        .unwrap()
        .extend([block1, block2]);
    let first_store = store.clone();
    let (done, completion) = std::sync::mpsc::channel();
    let first = std::thread::spawn(move || {
        done.send(first_store.put(StagedChunk::new(
            OperationId::new("first-group"),
            b"first".to_vec(),
        )))
        .unwrap()
    });
    observed1.recv_timeout(Duration::from_secs(3)).unwrap();
    let second_store = store.clone();
    let second = std::thread::spawn(move || {
        second_store.put(StagedChunk::new(
            OperationId::new("second-group"),
            b"second".to_vec(),
        ))
    });
    p2_w3_wait(&store, |stats| stats.ready == 1);
    release1.send(()).unwrap();
    observed2.recv_timeout(Duration::from_secs(3)).unwrap();
    let first_ack = completion.recv_timeout(Duration::from_secs(1));
    release2.send(()).unwrap();
    first.join().unwrap();
    assert!(second.join().unwrap().is_ok());
    assert!(
        first_ack.unwrap().is_ok(),
        "first ACK must not be held by continuously arriving later groups"
    );
    assert_eq!(store.catalog_commit_stats().groups, 2);
}

#[test]
fn p2_w3_oversized_batch_progresses_as_one_exclusive_original_transaction() {
    let temp = tempfile::tempdir().unwrap();
    let store = Arc::new(
        LocalChunkStore::open(temp.path(), "node-a")
            .unwrap()
            .with_catalog_commit_queue(1)
            .unwrap(),
    );
    let staged: Vec<_> = (0..80)
        .map(|index| StagedChunk::new(OperationId::new(format!("large-{index}")), vec![index; 64]))
        .collect();
    let (block, observed, release) = p2_w3_block();
    store
        .catalog_commit_test
        .before_wal
        .lock()
        .unwrap()
        .push_back(block);
    let worker_store = store.clone();
    let worker = std::thread::spawn(move || worker_store.put_batch(staged));
    observed.recv_timeout(Duration::from_secs(3)).unwrap();
    let held = store.catalog_commit_stats();
    assert!(held.exclusive);
    assert_eq!(
        held.requests, 0,
        "large original metadata must not enter the bounded new queue"
    );
    release.send(()).unwrap();
    let receipts = worker.join().unwrap().unwrap();
    assert_eq!(receipts.len(), 80);
    assert!(
        receipts
            .iter()
            .all(|receipt| receipt.durable_acks[0].catalog_revision == 1)
    );
    assert_eq!(store.catalog_commit_stats().groups, 1);
    assert!(!store.catalog_commit_stats().exclusive);
    drop(store);
    let recovered = LocalChunkStore::open(temp.path(), "node-a").unwrap();
    assert_eq!(recovered.recover().unwrap().durable_chunks, 80);
}

#[test]
fn p2_w3_partial_write_sync_and_parent_failures_prevent_every_later_append() {
    for step in [1, 2, 3] {
        let temp = tempfile::tempdir().unwrap();
        let store = LocalChunkStore::open(temp.path(), "node-a").unwrap();
        let healthy = StagedChunk::new(OperationId::new("healthy"), b"healthy".to_vec());
        store.put(healthy.clone()).unwrap();
        store
            .catalog_commit_test
            .fail_wal_step
            .store(step, Ordering::Release);
        assert!(
            store
                .put(StagedChunk::new(
                    OperationId::new("failed-group"),
                    b"failed".to_vec()
                ))
                .is_err()
        );
        let failed_bytes = fs::read(&store.catalog_path).unwrap();
        assert!(store.catalog_commit_stats().failed);
        assert!(store.put(healthy.clone()).is_err());
        fs::write(store.chunk_path(&healthy.chunk.id), b"damaged").unwrap();
        assert!(store.quarantine_if_invalid(&healthy.chunk.id).is_err());
        assert!(
            store
                .put(StagedChunk::new(
                    OperationId::new("later"),
                    b"later".to_vec()
                ))
                .is_err()
        );
        assert_eq!(fs::read(&store.catalog_path).unwrap(), failed_bytes);
        drop(store);
        let recovered = LocalChunkStore::open(temp.path(), "node-a").unwrap();
        assert!(recovered.put(healthy).is_ok());
    }
}

#[test]
fn p2_w3_stop_waits_for_owned_io_and_keeps_the_final_drain_commit() {
    let temp = tempfile::tempdir().unwrap();
    let store = Arc::new(
        LocalChunkStore::open(temp.path(), "node-a")
            .unwrap()
            .with_catalog_commit_queue(1)
            .unwrap(),
    );
    store.note_catalog_stop();
    // The existing DFS drain invokes persist after the stop notification.
    assert!(
        store
            .put(StagedChunk::new(
                OperationId::new("final-drain"),
                b"drain".to_vec()
            ))
            .is_ok()
    );
    let (block, observed, release) = p2_w3_block();
    store
        .catalog_commit_test
        .before_wal
        .lock()
        .unwrap()
        .push_back(block);
    let worker_store = store.clone();
    let worker = std::thread::spawn(move || {
        worker_store.put(StagedChunk::new(
            OperationId::new("owned-io"),
            b"owned".to_vec(),
        ))
    });
    observed.recv_timeout(Duration::from_secs(3)).unwrap();
    let waiting_store = store.clone();
    let waiter = std::thread::spawn(move || {
        waiting_store.put(StagedChunk::new(
            OperationId::new("queued"),
            b"queued".to_vec(),
        ))
    });
    p2_w3_wait(&store, |stats| stats.waiters == 1);
    let stopping_store = store.clone();
    let (done, stopped) = std::sync::mpsc::channel();
    let stopping = std::thread::spawn(move || {
        done.send(stopping_store.shutdown_catalog_committer())
            .unwrap()
    });
    p2_w3_wait(&store, |stats| stats.closed);
    let still_owned = stopped.recv_timeout(Duration::from_millis(30)).is_err();
    release.send(()).unwrap();
    assert!(worker.join().unwrap().is_ok());
    assert!(waiter.join().unwrap().is_err());
    assert!(
        stopped
            .recv_timeout(Duration::from_secs(3))
            .unwrap()
            .is_ok()
    );
    stopping.join().unwrap();
    assert!(still_owned, "stop must not detach active I/O");
    assert_eq!(store.catalog_commit_stats().requests, 0);
    assert!(
        store
            .put(StagedChunk::new(
                OperationId::new("after-close"),
                b"no".to_vec()
            ))
            .is_err()
    );
}

#[test]
fn p2_w3_unconfirmed_replacement_cannot_replay_until_a_reopen_directory_barrier() {
    for later_prepare_failure in [false, true] {
        let temp = tempfile::tempdir().unwrap();
        let store = LocalChunkStore::open(temp.path(), "node-a").unwrap();
        let healthy = StagedChunk::new(OperationId::new("healthy-replace"), b"healthy".to_vec());
        store.put(healthy.clone()).unwrap();
        fs::write(store.chunk_path(&healthy.chunk.id), b"BROKEN!").unwrap();
        if later_prepare_failure {
            store
                .catalog_commit_test
                .fail_prepare_after
                .store(2, Ordering::Release);
        } else {
            store
                .catalog_commit_test
                .fail_chunks_dir
                .store(true, Ordering::Release);
        }
        let mut staged = vec![healthy.clone()];
        if later_prepare_failure {
            staged.push(StagedChunk::new(
                OperationId::new("second-body"),
                b"second".to_vec(),
            ));
        }
        assert!(store.put_batch(staged).is_err());
        assert_eq!(
            fs::read(store.chunk_path(&healthy.chunk.id)).unwrap(),
            b"healthy"
        );
        assert!(store.catalog_commit_stats().failed);
        let wal = fs::read(&store.catalog_path).unwrap();
        assert!(
            store.put(healthy.clone()).is_err(),
            "unconfirmed rename cannot gain a healthy replay ACK"
        );
        assert_eq!(fs::read(&store.catalog_path).unwrap(), wal);
        drop(store);
        FAIL_STORE_DIRECTORY_SYNC.with(|fail| fail.set(true));
        assert!(
            LocalChunkStore::open(temp.path(), "node-a").is_err(),
            "reopen must complete its chunks namespace barrier"
        );
        STORE_DIRECTORY_SYNCS.with(|syncs| *syncs.borrow_mut() = Some(Vec::new()));
        let recovered = LocalChunkStore::open(temp.path(), "node-a").unwrap();
        let syncs = STORE_DIRECTORY_SYNCS.with(|syncs| syncs.borrow_mut().take().unwrap());
        assert_eq!(syncs.first(), Some(&temp.path().join("chunks")));
        assert!(syncs.contains(&temp.path().to_path_buf()));
        assert!(recovered.put(healthy).is_ok());
        assert_eq!(
            recovered.catalog_commit_stats().groups,
            0,
            "healthy replay uses the proven startup barriers"
        );
    }
}

#[test]
fn p2_w3_multi_key_requests_do_not_deadlock_and_owner_drop_releases_admission() {
    let temp = tempfile::tempdir().unwrap();
    let store = Arc::new(LocalChunkStore::open(temp.path(), "node-a").unwrap());
    let a = StagedChunk::new(OperationId::new("key-a"), b"a".to_vec());
    let b = StagedChunk::new(OperationId::new("key-b"), b"b".to_vec());
    let cancelled = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
        let _admission = store.admit_catalog(&[a.clone(), b.clone()]).unwrap();
        panic!("test-owned caller unwind before I/O");
    }));
    assert!(cancelled.is_err());
    assert_eq!(store.catalog_commit_stats().requests, 0);
    let first_store = store.clone();
    let forward = vec![a.clone(), b.clone()];
    let first = std::thread::spawn(move || first_store.put_batch(forward));
    let second_store = store.clone();
    let second = std::thread::spawn(move || second_store.put_batch(vec![b, a]));
    assert_eq!(first.join().unwrap().unwrap().len(), 2);
    assert_eq!(second.join().unwrap().unwrap().len(), 2);
    assert_eq!(store.catalog_commit_stats().groups, 1);
}

#[test]
fn p2_w3_shared_group_failure_wakes_every_waiter_without_ack() {
    for step in [2, 3] {
        let temp = tempfile::tempdir().unwrap();
        let store = Arc::new(LocalChunkStore::open(temp.path(), "node-a").unwrap());
        let healthy = StagedChunk::new(
            OperationId::new("previously-confirmed"),
            b"confirmed".to_vec(),
        );
        store.put(healthy.clone()).unwrap();
        *store.catalog_commit_test.ready.lock().unwrap() =
            Some(Arc::new(std::sync::Barrier::new(16)));
        store
            .catalog_commit_test
            .fail_wal_step
            .store(step, Ordering::Release);
        let workers: Vec<_> = (0..16)
            .map(|index| {
                let store = store.clone();
                std::thread::spawn(move || {
                    store.put(StagedChunk::new(
                        OperationId::new(format!("shared-error-{index}")),
                        vec![index; 64],
                    ))
                })
            })
            .collect();
        let errors: Vec<_> = workers
            .into_iter()
            .map(|worker| worker.join().unwrap().unwrap_err())
            .collect();
        assert_eq!(errors.len(), 16);
        assert!(
            errors.iter().all(|error| error == &errors[0]),
            "every failed group member must observe the same structured error"
        );
        *store.catalog_commit_test.ready.lock().unwrap() = None;
        let stats = store.catalog_commit_stats();
        assert!(stats.failed);
        assert_eq!(stats.requests, 0);
        assert_eq!(
            stats.groups, 1,
            "only the previous confirmed request may count as a successful group"
        );
        assert_eq!(stats.max_group, 16);
        let bytes = fs::read(&store.catalog_path).unwrap();
        assert!(store.put(healthy.clone()).is_err());
        assert!(store.quarantine_if_invalid(&healthy.chunk.id).is_err());
        assert!(
            store
                .put(StagedChunk::new(
                    OperationId::new("after-shared-failure"),
                    b"later".to_vec()
                ))
                .is_err()
        );
        assert_eq!(
            fs::read(&store.catalog_path).unwrap(),
            bytes,
            "failed group cannot be followed by another append"
        );
        drop(store);
        let recovered = LocalChunkStore::open(temp.path(), "node-a").unwrap();
        let mut output = [0; 9];
        assert_eq!(
            recovered
                .read_at(&healthy.chunk.id, 0, &mut output)
                .unwrap(),
            9
        );
        assert_eq!(&output, b"confirmed");
    }
}

#[test]
fn p2_w3_reopen_syncs_complete_unsynced_wal_before_replay_ack() {
    for newline in [false, true] {
        let temp = tempfile::tempdir().unwrap();
        let store = LocalChunkStore::open(temp.path(), "node-a").unwrap();
        assert!(
            !store.catalog_path.exists(),
            "empty startup must not invent a WAL file"
        );
        let healthy =
            StagedChunk::new(OperationId::new("previous-confirmed"), b"previous".to_vec());
        store.put(healthy).unwrap();
        let unconfirmed = StagedChunk::new(
            OperationId::new("complete-unsynced"),
            b"unconfirmed".to_vec(),
        );
        let mut body = File::create(store.chunk_path(&unconfirmed.chunk.id)).unwrap();
        body.write_all(unconfirmed.bytes()).unwrap();
        body.sync_all().unwrap();
        let record = LocalChunkRecord {
            chunk: unconfirmed.chunk.clone(),
            segment_crc: None,
            stored_length: unconfirmed.chunk.length,
            stored_checksum: unconfirmed.chunk.content_digest.clone(),
            device_id: store.device_id.clone(),
            device_epoch: store.device_epoch,
            location: PhysicalLocation::PerChunkFile {
                relative_path: format!("chunks/{}", unconfirmed.chunk.id.0),
            },
            encoding: PhysicalEncoding::Raw,
            state: LocalChunkState::Durable,
            catalog_revision: 2,
        };
        let mut encoded = serde_json::to_vec(&CatalogTxn::new(2, vec![record]).unwrap()).unwrap();
        if newline {
            encoded.push(b'\n');
        }
        // Deliberately omit sync_all: complete bytes are visible in the same
        // OS but must not be mistaken for a completed durability barrier.
        OpenOptions::new()
            .append(true)
            .open(&store.catalog_path)
            .unwrap()
            .write_all(&encoded)
            .unwrap();
        let wal = fs::read(&store.catalog_path).unwrap();
        drop(store);
        FAIL_STORE_CATALOG_SYNC.with(|fail| fail.set(true));
        assert!(LocalChunkStore::open(temp.path(), "node-a").is_err());
        STORE_CATALOG_SYNCS.with(|syncs| *syncs.borrow_mut() = Some(Vec::new()));
        let recovered = LocalChunkStore::open(temp.path(), "node-a").unwrap();
        let syncs = STORE_CATALOG_SYNCS.with(|syncs| syncs.borrow_mut().take().unwrap());
        assert_eq!(syncs, vec![temp.path().join("catalog.wal")]);
        assert_eq!(
            fs::read(&recovered.catalog_path).unwrap(),
            wal,
            "valid full transaction bytes are preserved"
        );
        let receipt = recovered.put(unconfirmed).unwrap();
        assert_eq!(receipt.durable_acks[0].catalog_revision, 2);
        assert_eq!(recovered.catalog_commit_stats().groups, 0);
        drop(recovered);
        // Complete malformed content remains a validation error, never a
        // recovery-sync fast path that silently accepts it.
        OpenOptions::new()
            .append(true)
            .open(temp.path().join("catalog.wal"))
            .unwrap()
            .write_all(b"\n{}\n")
            .unwrap();
        STORE_CATALOG_SYNCS.with(|syncs| *syncs.borrow_mut() = Some(Vec::new()));
        assert!(LocalChunkStore::open(temp.path(), "node-a").is_err());
        assert!(
            STORE_CATALOG_SYNCS
                .with(|syncs| syncs.borrow_mut().take().unwrap())
                .is_empty()
        );
    }
}

#[test]
fn p2_w3_catalog_failure_latches_until_reopen() {
    let temp = tempfile::tempdir().unwrap();
    let store = LocalChunkStore::open(temp.path(), "node-a").unwrap();
    let healthy = StagedChunk::new(OperationId::new("healthy"), b"healthy".to_vec());
    store.put(healthy.clone()).unwrap();
    let backup = temp.path().join("catalog-backup.wal");
    fs::rename(&store.catalog_path, &backup).unwrap();
    fs::create_dir(&store.catalog_path).unwrap();
    assert!(
        store
            .put(StagedChunk::new(
                OperationId::new("failed"),
                b"failed".to_vec()
            ))
            .is_err()
    );
    fs::remove_dir(&store.catalog_path).unwrap();
    fs::rename(&backup, &store.catalog_path).unwrap();
    let bytes = fs::read(&store.catalog_path).unwrap();
    assert!(
        store.put(healthy.clone()).is_err(),
        "a failed catalog cannot issue a later successful replay ACK"
    );
    assert!(
        store
            .put(StagedChunk::new(
                OperationId::new("later"),
                b"later".to_vec()
            ))
            .is_err()
    );
    assert_eq!(fs::read(&store.catalog_path).unwrap(), bytes);
    drop(store);
    let recovered = LocalChunkStore::open(temp.path(), "node-a").unwrap();
    assert!(recovered.put(healthy).is_ok());
}

#[test]
fn p2_w0_blocked_installer_does_not_delay_durable_ack() {
    use std::{sync::mpsc, thread, time::Duration};
    let temp = tempfile::tempdir().unwrap();
    let budget = OutboardBudget::new(DEFAULT_OUTBOARD_BUDGET_BYTES);
    let staged = pooled_outboard_chunks(&budget, 1, 128 * 1024)
        .pop()
        .unwrap();
    let store = Arc::new(
        LocalChunkStore::open(temp.path(), "node-a")
            .unwrap()
            .with_outboard_budget(budget),
    );
    let (entered, entered_rx) = mpsc::channel();
    let (release, release_rx) = mpsc::channel();
    *store.sidecar_install_test.block.lock().unwrap() = Some((entered, release_rx));
    let worker_store = store.clone();
    let worker_staged = staged.clone();
    let (completed, completed_rx) = mpsc::channel();
    let writer = thread::spawn(move || {
        completed.send(worker_store.put(worker_staged)).unwrap();
    });
    entered_rx.recv_timeout(Duration::from_secs(5)).unwrap();
    // This is an explicit in-installer barrier, not a file-presence race.
    // The required body/catalog transaction has completed, while optional
    // sidecar installation remains blocked on the test-owned receiver.
    assert_eq!(
        fs::read(store.chunk_path(&staged.chunk.id)).unwrap(),
        staged.bytes()
    );
    assert!(!fs::read(&store.catalog_path).unwrap().is_empty());
    let while_blocked = completed_rx.recv_timeout(Duration::from_millis(100));
    release.send(()).unwrap();
    writer.join().unwrap();
    assert!(
        while_blocked.is_ok(),
        "durable persist ACK was blocked by the optional sidecar installer"
    );
    let receipt = while_blocked.unwrap().unwrap();
    assert_eq!(receipt.chunk, staged.chunk);
    assert_eq!(receipt.durable_acks.len(), 1);
    let ack = &receipt.durable_acks[0];
    assert_eq!(ack.chunk_id, staged.chunk.id);
    assert_eq!(ack.persisted_bytes, staged.chunk.length);
    assert_eq!(ack.verified_digest, staged.chunk.content_digest);
}

fn block_next_sidecar_install(
    store: &LocalChunkStore,
) -> (std::sync::mpsc::Receiver<()>, std::sync::mpsc::Sender<()>) {
    let (entered, entered_rx) = std::sync::mpsc::channel();
    let (release, release_rx) = std::sync::mpsc::channel();
    *store.sidecar_install_test.block.lock().unwrap() = Some((entered, release_rx));
    (entered_rx, release)
}

#[test]
fn p2_w0_required_barriers_precede_ack_while_optional_installer_is_blocked() {
    let temp = tempfile::tempdir().unwrap();
    let registry = afs_metrics::Registry::new();
    let metrics = super::super::read_work::ReadWorkMetrics::register(&registry)
        .unwrap()
        .with_read_diagnostics(true);
    let budget = OutboardBudget::new(DEFAULT_OUTBOARD_BUDGET_BYTES);
    let staged = pooled_outboard_chunks(&budget, 1, 128 * 1024)
        .pop()
        .unwrap();
    let store = LocalChunkStore::open(temp.path(), "node-a")
        .unwrap()
        .with_outboard_budget(budget)
        .with_read_work_metrics(metrics.clone());
    let (entered, release) = block_next_sidecar_install(&store);
    let receipt = store.put(staged.clone()).unwrap();
    entered.recv_timeout(Duration::from_secs(5)).unwrap();
    let snapshot = metrics.diagnostics_snapshot().unwrap();
    let mut previous_end = 0;
    for name in [
        "persist_body_write",
        "persist_body_sync",
        "persist_chunks_dir_sync",
        "persist_catalog_write",
        "persist_catalog_sync",
        "persist_catalog_parent_sync",
    ] {
        let phase = snapshot
            .coarse_phases
            .iter()
            .find(|phase| phase.phase == name)
            .unwrap();
        assert_eq!(phase.calls, 1, "{name}");
        assert!(
            phase.first_start_ns >= previous_end,
            "required barrier order changed at {name}"
        );
        previous_end = phase.last_end_ns;
    }
    for name in ["persist_proof_install_sync", "persist_proof_dir_sync"] {
        assert_eq!(
            snapshot
                .coarse_phases
                .iter()
                .find(|phase| phase.phase == name)
                .unwrap()
                .calls,
            0
        );
    }
    assert_eq!(receipt.durable_acks.len(), 1);
    assert_eq!(
        *store.sidecar_install_test.optional_io.lock().unwrap(),
        OutboardPersistIo::default()
    );
    release.send(()).unwrap();
    store.sidecar_installer.wait_idle();
}

#[test]
fn p2_w0_queue_limit_includes_active_and_releases_only_its_own_proof_refs() {
    let temp = tempfile::tempdir().unwrap();
    let registry = afs_metrics::Registry::new();
    let metrics = super::super::read_work::ReadWorkMetrics::register(&registry).unwrap();
    let budget = OutboardBudget::new_with_metrics(DEFAULT_OUTBOARD_BUDGET_BYTES, metrics.clone());
    let staged = pooled_outboard_chunks(&budget, 65, 4096);
    let store = LocalChunkStore::open(temp.path(), "node-a")
        .unwrap()
        .with_outboard_budget(budget.clone())
        .with_read_work_metrics(metrics);
    let (entered, release) = block_next_sidecar_install(&store);
    store.put(staged[0].clone()).unwrap();
    entered.recv_timeout(Duration::from_secs(5)).unwrap();
    assert_eq!(store.put_batch(staged[1..].to_vec()).unwrap().len(), 64);
    {
        let state = store.sidecar_installer.queue.state.lock().unwrap();
        assert_eq!(
            (state.pending.len(), state.active, state.unfinished.len()),
            (63, 1, 64)
        );
        let actual_bound = sidecar_control_bytes(
            state.pending.capacity(),
            state.unfinished.capacity(),
            store.sidecar_installer.limit,
            state.directory_capacity,
        )
        .unwrap();
        assert!(actual_bound <= state.control_bytes);
        assert!(state.control_bytes > 0);
    }
    let charged = budget.used();
    assert!(charged > 0 && charged <= budget.limit());
    assert_eq!(budget.resident_used(), 0);
    assert!(
        afs_metrics::encode_text(&registry)
            .unwrap()
            .contains("afs_dfs_outboard_events_total{event=\"sidecar_install_dropped\"} 1\n")
    );
    release.send(()).unwrap();
    store.sidecar_installer.wait_idle();
    assert_eq!(
        budget.used(),
        charged,
        "staged owners still retain every proof allocation"
    );
    let io = *store.sidecar_install_test.optional_io.lock().unwrap();
    assert_eq!(
        (io.writes, io.file_syncs, io.renames, io.directory_syncs),
        (64, 64, 64, 3)
    );
    let retained = staged[0].outboard.clone();
    drop(staged);
    assert!(
        budget.used() > 0,
        "last independent holder must keep its charge"
    );
    drop(retained);
    let idle_control = store
        .sidecar_installer
        .queue
        .state
        .lock()
        .unwrap()
        .control_bytes;
    assert_eq!(
        budget.used(),
        idle_control,
        "bounded idle control capacity must remain charged"
    );
    store.sidecar_installer.shutdown().unwrap();
    assert_eq!(budget.used(), 0);
}

#[test]
fn p2_w0_installer_failure_keeps_ack_and_safe_read_repairs_once() {
    let temp = tempfile::tempdir().unwrap();
    let registry = afs_metrics::Registry::new();
    let metrics = super::super::read_work::ReadWorkMetrics::register(&registry).unwrap();
    let budget = OutboardBudget::new_with_metrics(DEFAULT_OUTBOARD_BUDGET_BYTES, metrics.clone());
    let staged = pooled_outboard_chunks(&budget, 1, 128 * 1024)
        .pop()
        .unwrap();
    let store = LocalChunkStore::open(temp.path(), "node-a")
        .unwrap()
        .with_outboard_budget(budget)
        .with_read_work_metrics(metrics);
    let path = store
        .outboards
        .join(format!("{}.bao", safe_id(&staged.chunk.id.0)));
    fs::create_dir(&path).unwrap();
    let receipt = store.put(staged.clone()).unwrap();
    store.sidecar_installer.wait_idle();
    assert_eq!(receipt.chunk, staged.chunk);
    assert!(
        afs_metrics::encode_text(&registry)
            .unwrap()
            .contains("afs_dfs_outboard_events_total{event=\"sidecar_install_failed\"} 1\n")
    );
    fs::remove_dir(&path).unwrap();
    let catalog = fs::read(&store.catalog_path).unwrap();
    take_outboard_persist_io();
    for _ in 0..2 {
        let reader = store.open_verified(&staged.chunk.id).unwrap();
        let mut out = [0; 19];
        assert_eq!(reader.read_at(3, &mut out).unwrap(), out.len());
        assert_eq!(out, staged.bytes()[3..22]);
    }
    assert_eq!(take_outboard_persist_io().writes, 1);
    assert_eq!(fs::read(&store.catalog_path).unwrap(), catalog);
    assert_eq!(
        fs::read(path).unwrap(),
        staged.outboard.as_ref().unwrap().encoded()
    );
}

#[test]
fn p2_w0_ack_before_sidecar_keeps_verified_full_scan_fallback_and_repair() {
    let temp = tempfile::tempdir().unwrap();
    let budget = OutboardBudget::new(DEFAULT_OUTBOARD_BUDGET_BYTES);
    let staged = pooled_outboard_chunks(&budget, 1, 4 * 1024 * 1024)
        .pop()
        .unwrap();
    let store = LocalChunkStore::open(temp.path(), "node-a")
        .unwrap()
        .with_outboard_budget(budget);
    let (entered, release) = block_next_sidecar_install(&store);
    let receipt = store.put(staged.clone()).unwrap();
    entered.recv_timeout(Duration::from_secs(5)).unwrap();
    let path = store
        .outboards
        .join(format!("{}.bao", safe_id(&staged.chunk.id.0)));
    assert!(
        !path.exists(),
        "installer is explicitly blocked before sidecar installation"
    );
    let catalog = fs::read(&store.catalog_path).unwrap();
    VERIFIED_SCAN_BYTES.with(|count| count.set(0));
    take_outboard_persist_io();
    let payload = store.read_peer_range(&staged.chunk.id, 3, 17).unwrap();
    assert_eq!(payload.as_slice(), &staged.bytes()[3..20]);
    assert_eq!(
        VERIFIED_SCAN_BYTES.with(|count| count.get()),
        staged.chunk.length
    );
    assert_eq!(
        take_outboard_persist_io().writes,
        1,
        "missing optional proof is safely repaired once"
    );
    assert_eq!(
        fs::read(&path).unwrap(),
        staged.outboard.as_ref().unwrap().encoded()
    );
    assert_eq!(fs::read(&store.catalog_path).unwrap(), catalog);
    assert_eq!(receipt.chunk, staged.chunk);
    release.send(()).unwrap();
    store.sidecar_installer.wait_idle();
    assert_eq!(
        store
            .sidecar_install_test
            .optional_io
            .lock()
            .unwrap()
            .writes,
        0,
        "installer rechecks the sidecar repaired by the verified read"
    );
}

#[test]
fn p2_w0_control_metadata_admission_failure_drops_optional_work_not_ack() {
    let temp = tempfile::tempdir().unwrap();
    let registry = afs_metrics::Registry::new();
    let metrics = super::super::read_work::ReadWorkMetrics::register(&registry).unwrap();
    let budget = OutboardBudget::new_with_metrics(DEFAULT_OUTBOARD_BUDGET_BYTES, metrics.clone());
    let staged = pooled_outboard_chunks(&budget, 1, 128 * 1024)
        .pop()
        .unwrap();
    let store = LocalChunkStore::open(temp.path(), "node-a")
        .unwrap()
        .with_outboard_budget(budget.clone())
        .with_read_work_metrics(metrics);
    let pressure = budget
        .try_reserve(budget.limit() - budget.used() - 1)
        .unwrap();
    let before = budget.used();
    let receipt = store.put(staged.clone()).unwrap();
    assert_eq!(receipt.chunk, staged.chunk);
    assert_eq!(budget.used(), before);
    assert_eq!(
        store
            .sidecar_installer
            .queue
            .state
            .lock()
            .unwrap()
            .control_bytes,
        0
    );
    assert!(store.sidecar_installer.worker.lock().unwrap().is_none());
    assert!(
        afs_metrics::encode_text(&registry)
            .unwrap()
            .contains("afs_dfs_outboard_events_total{event=\"sidecar_install_dropped\"} 1\n")
    );
    drop(pressure);
    take_outboard_persist_io();
    drop(store.open_verified(&staged.chunk.id).unwrap());
    assert_eq!(take_outboard_persist_io().writes, 1);
}

#[test]
fn p2_w0_control_charge_is_observed_before_optional_worker_start_failure() {
    let temp = tempfile::tempdir().unwrap();
    let registry = afs_metrics::Registry::new();
    let metrics = super::super::read_work::ReadWorkMetrics::register(&registry).unwrap();
    let budget = OutboardBudget::new_with_metrics(DEFAULT_OUTBOARD_BUDGET_BYTES, metrics.clone());
    let staged = pooled_outboard_chunks(&budget, 1, 128 * 1024)
        .pop()
        .unwrap();
    let store = LocalChunkStore::open(temp.path(), "node-a")
        .unwrap()
        .with_outboard_budget(budget.clone())
        .with_read_work_metrics(metrics)
        .with_sidecar_install_queue(2)
        .unwrap();
    assert!(
        afs_metrics::encode_text(&registry)
            .unwrap()
            .contains("afs_dfs_sidecar_install_items{state=\"limit\"} 2\n")
    );
    store
        .sidecar_install_test
        .fail_worker_start
        .store(true, Ordering::Relaxed);
    let receipt = store.put(staged.clone()).unwrap();
    assert_eq!(receipt.chunk, staged.chunk);
    let control_bytes = store
        .sidecar_installer
        .queue
        .state
        .lock()
        .unwrap()
        .control_bytes;
    assert!(control_bytes > 0);
    let text = afs_metrics::encode_text(&registry).unwrap();
    assert!(
        text.contains(&format!(
            "afs_dfs_sidecar_install_control_bytes {control_bytes}\n"
        )),
        "{text}"
    );
    assert!(text.contains("afs_dfs_outboard_events_total{event=\"sidecar_install_failed\"} 1\n"));
    assert!(store.sidecar_installer.worker.lock().unwrap().is_none());
    drop(staged);
    assert_eq!(budget.used(), control_bytes);
    store.sidecar_installer.shutdown().unwrap();
    assert_eq!(budget.used(), 0);
    assert!(
        afs_metrics::encode_text(&registry)
            .unwrap()
            .contains("afs_dfs_sidecar_install_control_bytes 0\n")
    );
}

#[test]
fn p2_w0_deadline_between_install_and_directory_barrier_skips_new_sync() {
    let temp = tempfile::tempdir().unwrap();
    let registry = afs_metrics::Registry::new();
    let metrics = super::super::read_work::ReadWorkMetrics::register(&registry).unwrap();
    let budget = OutboardBudget::new_with_metrics(DEFAULT_OUTBOARD_BUDGET_BYTES, metrics.clone());
    let staged = pooled_outboard_chunks(&budget, 1, 128 * 1024)
        .pop()
        .unwrap();
    let store = LocalChunkStore::open(temp.path(), "node-a")
        .unwrap()
        .with_outboard_budget(budget)
        .with_read_work_metrics(metrics);
    let (entered, entered_rx) = std::sync::mpsc::channel();
    let (release, release_rx) = std::sync::mpsc::channel();
    *store
        .sidecar_install_test
        .before_directory_sync
        .lock()
        .unwrap() = Some((entered, release_rx));
    let receipt = store.put(staged.clone()).unwrap();
    entered_rx.recv_timeout(Duration::from_secs(5)).unwrap();
    assert_eq!(
        fs::read(
            store
                .outboards
                .join(format!("{}.bao", safe_id(&staged.chunk.id.0)))
        )
        .unwrap(),
        staged.outboard.as_ref().unwrap().encoded()
    );
    store.sidecar_installer.close(Instant::now());
    release.send(()).unwrap();
    store.sidecar_installer.wait_idle();
    let io = *store.sidecar_install_test.optional_io.lock().unwrap();
    assert_eq!(
        (io.writes, io.file_syncs, io.renames, io.directory_syncs),
        (1, 1, 1, 0)
    );
    assert!(
        afs_metrics::encode_text(&registry)
            .unwrap()
            .contains("afs_dfs_outboard_events_total{event=\"sidecar_install_dropped\"} 1\n")
    );
    assert_eq!(receipt.chunk, staged.chunk);
    assert_eq!(
        fs::read(store.chunk_path(&staged.chunk.id)).unwrap(),
        staged.bytes()
    );
    store.sidecar_installer.shutdown().unwrap();
}

#[test]
fn p2_w0_all_configured_queue_sizes_can_admit_real_sidecars() {
    for limit in [1, 32, 64] {
        let temp = tempfile::tempdir().unwrap();
        let budget = OutboardBudget::new(DEFAULT_OUTBOARD_BUDGET_BYTES);
        let staged = pooled_outboard_chunks(&budget, 1, 128 * 1024)
            .pop()
            .unwrap();
        let store = LocalChunkStore::open(temp.path(), "node-a")
            .unwrap()
            .with_outboard_budget(budget.clone())
            .with_sidecar_install_queue(limit)
            .unwrap();
        store.put(staged.clone()).unwrap();
        store.sidecar_installer.wait_idle();
        assert_eq!(
            fs::read(
                store
                    .outboards
                    .join(format!("{}.bao", safe_id(&staged.chunk.id.0)))
            )
            .unwrap(),
            staged.outboard.as_ref().unwrap().encoded(),
            "queue limit={limit}"
        );
        drop(staged);
        store.sidecar_installer.shutdown().unwrap();
        assert_eq!(budget.used(), 0, "queue limit={limit}");
    }
}

#[test]
fn p2_w0_control_path_allowance_covers_long_lengths_and_checks_overflow() {
    for length in [0, 64, 1024, 4095] {
        let allowance = sidecar_path_bytes(length).unwrap();
        let directory = length;
        let formal_join = 2 * (length + SIDECAR_ID_CAPACITY);
        let temporary_join = 2 * (length + 128);
        assert!(allowance >= directory + formal_join + temporary_join + SIDECAR_ID_CAPACITY + 128);
        assert!(sidecar_control_bytes(64, 112, 64, length).unwrap() >= allowance);
    }
    assert!(sidecar_path_bytes(usize::MAX).is_none());
    assert!(sidecar_control_bytes(usize::MAX, 112, 64, 4095).is_none());
}

#[test]
fn p2_w0_stop_rejects_new_items_discards_pending_and_keeps_active_owned() {
    let temp = tempfile::tempdir().unwrap();
    let budget = OutboardBudget::new(DEFAULT_OUTBOARD_BUDGET_BYTES);
    let staged = pooled_outboard_chunks(&budget, 3, 4096);
    let store = LocalChunkStore::open(temp.path(), "node-a")
        .unwrap()
        .with_outboard_budget(budget.clone());
    let (entered, release) = block_next_sidecar_install(&store);
    store.put(staged[0].clone()).unwrap();
    entered.recv_timeout(Duration::from_secs(5)).unwrap();
    store.put(staged[1].clone()).unwrap();
    store.sidecar_installer.close(Instant::now());
    let deadline = store.sidecar_installer.queue.state.lock().unwrap().deadline;
    store
        .sidecar_installer
        .close(Instant::now() + Duration::from_secs(1));
    assert_eq!(
        store.sidecar_installer.queue.state.lock().unwrap().deadline,
        deadline
    );
    store.put(staged[2].clone()).unwrap();
    assert!(budget.used() > 0);
    release.send(()).unwrap();
    store.sidecar_installer.shutdown().unwrap();
    let state = store.sidecar_installer.queue.state.lock().unwrap();
    assert_eq!(
        (state.pending.len(), state.active, state.unfinished.len()),
        (0, 0, 0)
    );
    drop(state);
    for item in &staged[1..] {
        assert!(
            !store
                .outboards
                .join(format!("{}.bao", safe_id(&item.chunk.id.0)))
                .exists()
        );
    }
    assert!(budget.used() > 0);
    drop(staged);
    assert_eq!(budget.used(), 0);
}

#[test]
#[ignore = "child process only; invoked by the bounded crash-recovery test"]
fn p2_w0_crash_after_ack_child() {
    let Ok(root) = std::env::var("AFS_TEST_SIDECAR_CRASH_ROOT") else {
        return;
    };
    let budget = OutboardBudget::new(DEFAULT_OUTBOARD_BUDGET_BYTES);
    let staged = pooled_outboard_chunks(&budget, 1, 128 * 1024)
        .pop()
        .unwrap();
    let store = LocalChunkStore::open(root, "node-a")
        .unwrap()
        .with_outboard_budget(budget);
    let (entered, _release) = block_next_sidecar_install(&store);
    store.put(staged).unwrap();
    entered.recv_timeout(Duration::from_secs(5)).unwrap();
    std::process::exit(73);
}

#[test]
fn p2_w0_crash_after_ack_recovers_body_and_repairs_sidecar_once() {
    let temp = tempfile::tempdir().unwrap();
    let output = std::process::Command::new(std::env::current_exe().unwrap())
        .args([
            "--ignored",
            "--exact",
            "node::chunk::replica_replay_tests::p2_w0_crash_after_ack_child",
            "--nocapture",
        ])
        .env("AFS_TEST_SIDECAR_CRASH_ROOT", temp.path())
        .output()
        .unwrap();
    assert_eq!(
        output.status.code(),
        Some(73),
        "{}",
        String::from_utf8_lossy(&output.stderr)
    );
    let staged = StagedChunk::new(OperationId::new("expected"), vec![0; 128 * 1024]);
    let store = LocalChunkStore::open(temp.path(), "node-a").unwrap();
    let catalog = fs::read(&store.catalog_path).unwrap();
    take_outboard_persist_io();
    for _ in 0..2 {
        let reader = store.open_verified(&staged.chunk.id).unwrap();
        let mut out = vec![0xa5; staged.bytes().len() + 1];
        assert_eq!(reader.read_at(0, &mut out).unwrap(), staged.bytes().len());
        assert_eq!(&out[..staged.bytes().len()], staged.bytes());
        assert_eq!(out[staged.bytes().len()], 0xa5);
        assert_eq!(reader.read_at(staged.chunk.length, &mut [0; 1]).unwrap(), 0);
    }
    assert_eq!(take_outboard_persist_io().writes, 1);
    assert_eq!(fs::read(&store.catalog_path).unwrap(), catalog);
}

#[test]
#[ignore = "child process only; invoked by the blocked-worker watchdog test"]
fn p2_w0_blocked_worker_watchdog_child() {
    if std::env::var_os("AFS_TEST_SIDECAR_WATCHDOG").is_none() {
        return;
    }
    let temp = tempfile::tempdir().unwrap();
    let budget = OutboardBudget::new(DEFAULT_OUTBOARD_BUDGET_BYTES);
    let staged = pooled_outboard_chunks(&budget, 1, 128 * 1024)
        .pop()
        .unwrap();
    let store = LocalChunkStore::open(temp.path(), "node-a")
        .unwrap()
        .with_outboard_budget(budget);
    let (entered, _release) = block_next_sidecar_install(&store);
    store.put(staged).unwrap();
    entered.recv_timeout(Duration::from_secs(5)).unwrap();
    let watchdog = crate::runtime::ShutdownDeadline::new(Duration::from_millis(100)).unwrap();
    watchdog.trigger().arm();
    // Joining must keep ownership of this blocked worker. Only the
    // independent process watchdog is allowed to end this failure case.
    store.sidecar_installer.shutdown().unwrap();
    watchdog.complete();
}

#[test]
fn p2_w0_blocked_worker_uses_existing_failure_watchdog_not_detach() {
    let output = std::process::Command::new(std::env::current_exe().unwrap())
        .args([
            "--ignored",
            "--exact",
            "node::chunk::replica_replay_tests::p2_w0_blocked_worker_watchdog_child",
            "--nocapture",
        ])
        .env("AFS_TEST_SIDECAR_WATCHDOG", "1")
        .output()
        .unwrap();
    assert_eq!(
        output.status.code(),
        Some(124),
        "{}",
        String::from_utf8_lossy(&output.stderr)
    );
}

fn pooled_outboard_chunks(
    budget: &Arc<OutboardBudget>,
    count: usize,
    length: usize,
) -> Vec<StagedChunk> {
    let pool = WriteBufferPool::new_with_outboard_budget(count * length, budget.clone());
    (0..count)
        .map(|index| {
            let mut builder = ChunkBuilder::with_pool(pool.clone(), length).unwrap();
            builder.write_at(0, &vec![index as u8; length]).unwrap();
            let staged = builder.stage(OperationId::new(format!("batch-proof-{index}")));
            assert!(staged.outboard.as_ref().unwrap().uses_budget(budget));
            staged
        })
        .collect()
}

fn take_outboard_persist_io() -> OutboardPersistIo {
    OUTBOARD_PERSIST_IO.with(|counts| std::mem::take(&mut *counts.borrow_mut()))
}

fn take_store_outboard_persist_io(store: &LocalChunkStore) -> OutboardPersistIo {
    store.sidecar_installer.wait_idle();
    let mut io = take_outboard_persist_io();
    let background = std::mem::take(&mut *store.sidecar_install_test.optional_io.lock().unwrap());
    io.writes += background.writes;
    io.file_syncs += background.file_syncs;
    io.renames += background.renames;
    io.directory_syncs += background.directory_syncs;
    io
}

#[test]
fn coarse_persist_diagnostics_preserve_body_proof_barriers_and_receipts() {
    for enabled in [false, true] {
        let temp = tempfile::tempdir().unwrap();
        let registry = afs_metrics::Registry::new();
        let metrics = super::super::read_work::ReadWorkMetrics::register(&registry)
            .unwrap()
            .with_read_diagnostics(enabled);
        let budget = OutboardBudget::new(DEFAULT_OUTBOARD_BUDGET_BYTES);
        let staged = pooled_outboard_chunks(&budget, 2, 128 * 1024);
        let store = LocalChunkStore::open(temp.path(), "node-a")
            .unwrap()
            .with_outboard_budget(budget)
            .with_read_work_metrics(metrics.clone());
        take_store_outboard_persist_io(&store);
        let receipts = store.put_batch(staged.clone()).unwrap();
        store.sidecar_installer.wait_idle();
        assert_eq!(receipts.len(), 2);
        assert_eq!(
            take_store_outboard_persist_io(&store),
            OutboardPersistIo {
                writes: 2,
                file_syncs: 2,
                renames: 2,
                directory_syncs: 1,
            }
        );
        for (receipt, item) in receipts.iter().zip(&staged) {
            assert_eq!(receipt.chunk, item.chunk);
            assert_eq!(
                receipt.durable_acks[0].verified_digest,
                item.chunk.content_digest
            );
            assert_eq!(
                fs::read(store.chunk_path(&item.chunk.id)).unwrap(),
                item.bytes()
            );
        }
        match metrics.diagnostics_snapshot() {
            None => assert!(!enabled),
            Some(snapshot) => {
                assert!(enabled);
                assert_eq!(snapshot.records.len(), 1);
                assert_eq!(
                    snapshot.records[0].stage,
                    super::super::read_work::ReadDiagnosticStage::PersistBatch
                );
                assert!(snapshot.records[0].success);
                assert_eq!(snapshot.overflow, 0);
                for (name, calls) in [
                    ("persist_batch", 1),
                    ("persist_body_write", 2),
                    ("persist_body_sync", 2),
                    ("persist_chunks_dir_sync", 1),
                    ("persist_catalog_write", 1),
                    ("persist_catalog_sync", 1),
                    ("persist_catalog_parent_sync", 1),
                    ("persist_proof_install_sync", 2),
                    ("persist_proof_dir_sync", 1),
                ] {
                    let phase = snapshot
                        .coarse_phases
                        .iter()
                        .find(|phase| phase.phase == name)
                        .unwrap();
                    assert_eq!(phase.calls, calls, "{name}");
                    assert!(phase.last_end_ns >= phase.first_start_ns, "{name}");
                }
            }
        }
    }
}

#[test]
fn pr60_healthy_sidecars_are_read_only_across_peer_objects_and_pinned_opens() {
    let temp = tempfile::tempdir().unwrap();
    let budget = OutboardBudget::new(DEFAULT_OUTBOARD_BUDGET_BYTES);
    let staged = pooled_outboard_chunks(&budget, 3, 128 * 1024);
    let store = LocalChunkStore::open(temp.path(), "node-a")
        .unwrap()
        .with_outboard_budget(budget);
    store.put_batch(staged.clone()).unwrap();
    store.sidecar_installer.wait_idle();
    let catalog = fs::read(&store.catalog_path).unwrap();
    let catalog_identity =
        PeerFileIdentity::read(&File::open(&store.catalog_path).unwrap()).unwrap();
    let bodies = staged
        .iter()
        .map(|item| {
            let path = store.chunks.join(&item.chunk.id.0);
            (
                fs::read(&path).unwrap(),
                PeerFileIdentity::read(&File::open(path).unwrap()).unwrap(),
            )
        })
        .collect::<Vec<_>>();
    take_store_outboard_persist_io(&store);
    for item in &staged {
        let payload = store
            .read_peer_range(&item.chunk.id, 0, item.bytes().len())
            .unwrap();
        assert_eq!(payload.as_slice(), item.bytes());
        drop(payload);
        let reader = store.open_verified(&item.chunk.id).unwrap();
        let mut out = [0; 23];
        assert_eq!(reader.read_at(7, &mut out).unwrap(), out.len());
        assert_eq!(&out, &item.bytes()[7..30]);
    }
    assert_eq!(
        take_store_outboard_persist_io(&store),
        OutboardPersistIo::default(),
        "healthy read paths must not write or fsync sidecars"
    );
    assert_eq!(fs::read(&store.catalog_path).unwrap(), catalog);
    assert_eq!(
        PeerFileIdentity::read(&File::open(&store.catalog_path).unwrap()).unwrap(),
        catalog_identity
    );
    for (item, (body, identity)) in staged.iter().zip(bodies) {
        let path = store.chunks.join(&item.chunk.id.0);
        assert_eq!(fs::read(&path).unwrap(), body);
        assert_eq!(
            PeerFileIdentity::read(&File::open(path).unwrap()).unwrap(),
            identity
        );
    }
}

#[test]
fn pr60_new_pinned_readers_share_one_missing_sidecar_repair() {
    let temp = tempfile::tempdir().unwrap();
    let budget = OutboardBudget::new(DEFAULT_OUTBOARD_BUDGET_BYTES);
    let staged = pooled_outboard_chunks(&budget, 1, 128 * 1024)
        .pop()
        .unwrap();
    let store = Arc::new(
        LocalChunkStore::open(temp.path(), "node-a")
            .unwrap()
            .with_outboard_budget(budget),
    );
    store.put(staged.clone()).unwrap();
    store.sidecar_installer.wait_idle();
    let proof_path = store
        .outboards
        .join(format!("{}.bao", safe_id(&staged.chunk.id.0)));
    fs::remove_file(&proof_path).unwrap();
    let catalog = fs::read(&store.catalog_path).unwrap();
    let barrier = Arc::new(std::sync::Barrier::new(5));
    let mut threads = Vec::new();
    for _ in 0..4 {
        let store = store.clone();
        let staged = staged.clone();
        let barrier = barrier.clone();
        threads.push(std::thread::spawn(move || {
            take_outboard_persist_io();
            barrier.wait();
            let reader = store.open_verified(&staged.chunk.id).unwrap();
            let mut out = [0; 19];
            assert_eq!(reader.read_at(3, &mut out).unwrap(), out.len());
            assert_eq!(&out, &staged.bytes()[3..22]);
            take_outboard_persist_io()
        }));
    }
    barrier.wait();
    let mut total = OutboardPersistIo::default();
    for thread in threads {
        let io = thread.join().unwrap();
        total.writes += io.writes;
        total.file_syncs += io.file_syncs;
        total.renames += io.renames;
        total.directory_syncs += io.directory_syncs;
    }
    assert_eq!(
        total,
        OutboardPersistIo {
            writes: 1,
            file_syncs: 1,
            renames: 1,
            directory_syncs: 1
        }
    );
    assert_eq!(
        fs::read(&proof_path).unwrap(),
        staged.outboard.as_ref().unwrap().encoded()
    );
    assert_eq!(fs::read(&store.catalog_path).unwrap(), catalog);
    assert_eq!(
        fs::read(store.chunks.join(&staged.chunk.id.0)).unwrap(),
        staged.bytes()
    );
}

#[test]
fn pr60_sidecar_repair_runs_after_releasing_peer_slot() {
    let temp = tempfile::tempdir().unwrap();
    let budget = OutboardBudget::new(DEFAULT_OUTBOARD_BUDGET_BYTES);
    let staged = pooled_outboard_chunks(&budget, 1, 128 * 1024)
        .pop()
        .unwrap();
    let store = Arc::new(
        LocalChunkStore::open(temp.path(), "node-a")
            .unwrap()
            .with_outboard_budget(budget),
    );
    store.put(staged.clone()).unwrap();
    store.sidecar_installer.wait_idle();
    fs::remove_file(
        store
            .outboards
            .join(format!("{}.bao", safe_id(&staged.chunk.id.0))),
    )
    .unwrap();
    let (entered_tx, entered_rx) = std::sync::mpsc::channel();
    let (release_tx, release_rx) = std::sync::mpsc::channel();
    let worker_store = store.clone();
    let worker = std::thread::spawn(move || {
        OUTBOARD_INSTALL_HOOK.with(|hook| {
            *hook.borrow_mut() = Some(Box::new(move || {
                entered_tx.send(()).unwrap();
                release_rx.recv().unwrap();
            }))
        });
        worker_store
            .read_peer_range(&staged.chunk.id, 0, staged.bytes().len())
            .unwrap()
    });
    entered_rx
        .recv_timeout(std::time::Duration::from_secs(3))
        .unwrap();
    let available = store.peer_read_cache.try_lock().is_ok();
    release_tx.send(()).unwrap();
    let payload = worker.join().unwrap();
    assert_eq!(payload.as_slice(), &[0; 128 * 1024]);
    assert!(
        available,
        "optional proof repair must not hold the global body-cache slot"
    );
}

#[test]
fn pr60_corrupt_sidecar_repairs_once_without_rewriting_body_or_catalog() {
    let temp = tempfile::tempdir().unwrap();
    let budget = OutboardBudget::new(DEFAULT_OUTBOARD_BUDGET_BYTES);
    let staged = pooled_outboard_chunks(&budget, 1, 128 * 1024)
        .pop()
        .unwrap();
    let store = LocalChunkStore::open(temp.path(), "node-a")
        .unwrap()
        .with_outboard_budget(budget);
    store.put(staged.clone()).unwrap();
    store.sidecar_installer.wait_idle();
    let path = store
        .outboards
        .join(format!("{}.bao", safe_id(&staged.chunk.id.0)));
    let mut bytes = fs::read(&path).unwrap();
    bytes[64] ^= 1;
    fs::write(&path, bytes).unwrap();
    let catalog = fs::read(&store.catalog_path).unwrap();
    take_store_outboard_persist_io(&store);
    for _ in 0..2 {
        drop(store.open_verified(&staged.chunk.id).unwrap());
    }
    assert_eq!(
        take_store_outboard_persist_io(&store),
        OutboardPersistIo {
            writes: 1,
            file_syncs: 1,
            renames: 1,
            directory_syncs: 1
        }
    );
    assert_eq!(
        fs::read(&path).unwrap(),
        staged.outboard.as_ref().unwrap().encoded()
    );
    assert_eq!(fs::read(&store.catalog_path).unwrap(), catalog);
    assert_eq!(
        fs::read(store.chunks.join(&staged.chunk.id.0)).unwrap(),
        staged.bytes()
    );
}

/// R17 derived-data rule: a sidecar of another version is recognised,
/// discarded and rebuilt from the verified body, like a damaged one.
#[test]
fn sidecar_of_another_version_is_discarded_and_rebuilt() {
    let temp = tempfile::tempdir().unwrap();
    let budget = OutboardBudget::new(DEFAULT_OUTBOARD_BUDGET_BYTES);
    let staged = pooled_outboard_chunks(&budget, 1, 128 * 1024)
        .pop()
        .unwrap();
    let store = LocalChunkStore::open(temp.path(), "node-a")
        .unwrap()
        .with_outboard_budget(budget);
    store.put(staged.clone()).unwrap();
    store.sidecar_installer.wait_idle();
    let path = store
        .outboards
        .join(format!("{}.bao", safe_id(&staged.chunk.id.0)));
    let mut bytes = fs::read(&path).unwrap();
    assert_eq!(bytes[8..10], 1_u16.to_le_bytes());
    bytes[8] = 2;
    let error = ChunkOutboard::validate_encoded_shape(&staged.chunk, &bytes).unwrap_err();
    assert_eq!(error.code(), afs_error::NODE_TRANSFER_CORRUPT_DATA);
    for part in [
        "Node Bao sidecar",
        "version 2",
        "supports version 1",
        "discarded and rebuilt",
    ] {
        assert!(error.message().contains(part), "{part}: {error}");
    }
    fs::write(&path, bytes).unwrap();
    take_store_outboard_persist_io(&store);
    drop(store.open_verified(&staged.chunk.id).unwrap());
    assert_eq!(take_store_outboard_persist_io(&store).renames, 1);
    assert_eq!(
        fs::read(&path).unwrap(),
        staged.outboard.as_ref().unwrap().encoded()
    );
}

#[test]
fn pr60_capacity_fallback_preserves_healthy_pinned_proof_and_sidecar() {
    let temp = tempfile::tempdir().unwrap();
    let budget = OutboardBudget::new(DEFAULT_OUTBOARD_BUDGET_BYTES);
    let staged = pooled_outboard_chunks(&budget, 1, 128 * 1024)
        .pop()
        .unwrap();
    let store = LocalChunkStore::open(temp.path(), "node-a")
        .unwrap()
        .with_outboard_budget(budget.clone());
    store.put(staged.clone()).unwrap();
    store.sidecar_installer.wait_idle();
    let reader = store.open_verified(&staged.chunk.id).unwrap();
    let proof = reader.load_outboard().unwrap().unwrap();
    let sidecar = fs::read(&reader.outboard_path).unwrap();
    let before = budget.used();
    let held = budget.try_reserve(budget.limit() - budget.used()).unwrap();
    take_store_outboard_persist_io(&store);
    let mut out = [0xa5; 23];
    assert_eq!(reader.read_at(3, &mut out).unwrap(), out.len());
    assert_eq!(&out, &staged.bytes()[3..26]);
    assert!(Arc::ptr_eq(
        reader.outboard.lock().unwrap().as_ref().unwrap(),
        &proof
    ));
    assert_eq!(
        take_store_outboard_persist_io(&store),
        OutboardPersistIo::default()
    );
    assert_eq!(fs::read(&reader.outboard_path).unwrap(), sidecar);
    drop(held);
    assert_eq!(budget.used(), before);
    let payload = store
        .read_peer_range(&staged.chunk.id, 0, staged.bytes().len())
        .unwrap();
    assert!(
        payload
            .object
            .outboard
            .as_ref()
            .unwrap()
            .try_mark_resident()
    );
}

#[test]
fn pr60_hot_peer_hit_skips_sidecar_repair_but_a_later_miss_repairs() {
    let temp = tempfile::tempdir().unwrap();
    let budget = OutboardBudget::new(DEFAULT_OUTBOARD_BUDGET_BYTES);
    let staged = pooled_outboard_chunks(&budget, 1, 128 * 1024)
        .pop()
        .unwrap();
    let store = LocalChunkStore::open(temp.path(), "node-a")
        .unwrap()
        .with_outboard_budget(budget);
    store.put(staged.clone()).unwrap();
    store.sidecar_installer.wait_idle();
    let cached = store
        .read_peer_range(&staged.chunk.id, 0, staged.bytes().len())
        .unwrap();
    assert!(
        cached.object.outboard.is_some(),
        "exercise the resident proof hit branch"
    );
    let path = store
        .outboards
        .join(format!("{}.bao", safe_id(&staged.chunk.id.0)));
    fs::remove_file(&path).unwrap();
    take_store_outboard_persist_io(&store);
    let hit = store.read_peer_range(&staged.chunk.id, 3, 17).unwrap();
    assert_eq!(hit.as_slice(), &staged.bytes()[3..20]);
    assert!(Arc::ptr_eq(&hit.object, &cached.object));
    assert!(
        !path.exists(),
        "a verified hot hit must not recreate an optional sidecar"
    );
    assert_eq!(
        take_store_outboard_persist_io(&store),
        OutboardPersistIo::default()
    );
    // Retain the old payload to prove slot eviction does not retire an
    // active allocation, while the next miss independently repairs proof.
    store.clear_peer_residents_for_test();
    let miss = store
        .read_peer_range(&staged.chunk.id, 0, staged.bytes().len())
        .unwrap();
    assert_eq!(miss.as_slice(), staged.bytes());
    assert!(!Arc::ptr_eq(&miss.object, &cached.object));
    assert_eq!(hit.as_slice(), &staged.bytes()[3..20]);
    assert_eq!(
        fs::read(&path).unwrap(),
        staged.outboard.as_ref().unwrap().encoded()
    );
    assert_eq!(
        take_store_outboard_persist_io(&store),
        OutboardPersistIo {
            writes: 1,
            file_syncs: 1,
            renames: 1,
            directory_syncs: 1,
        }
    );
}

#[test]
fn pr60_partial_other_object_and_missing_id_do_not_evict_a_healthy_peer_slot() {
    let temp = tempfile::tempdir().unwrap();
    let budget = OutboardBudget::new(DEFAULT_OUTBOARD_BUDGET_BYTES);
    let staged = pooled_outboard_chunks(&budget, 2, 128 * 1024);
    let store = LocalChunkStore::open(temp.path(), "node-a")
        .unwrap()
        .with_outboard_budget(budget);
    store.put_batch(staged.clone()).unwrap();
    store.sidecar_installer.wait_idle();
    let cached = store
        .read_peer_range(&staged[0].chunk.id, 0, staged[0].bytes().len())
        .unwrap();
    let range = store.read_peer_range(&staged[1].chunk.id, 3, 17).unwrap();
    assert_eq!(range.as_slice(), &staged[1].bytes()[3..20]);
    assert!(Arc::ptr_eq(
        store.peer_read_cache.lock().unwrap().residents[0]
            .as_ref()
            .unwrap(),
        &cached.object
    ));
    assert!(
        store
            .read_peer_range(&ChunkId::new("missing-pr60"), 0, 17)
            .is_err()
    );
    assert!(Arc::ptr_eq(
        store.peer_read_cache.lock().unwrap().residents[0]
            .as_ref()
            .unwrap(),
        &cached.object
    ));
}

#[test]
fn pr60_two_hundred_pinned_readers_leave_writer_and_range_work_capacity() {
    let temp = tempfile::tempdir().unwrap();
    let budget = OutboardBudget::new(DEFAULT_OUTBOARD_BUDGET_BYTES);
    let pool = WriteBufferPool::new_with_outboard_budget(4 * 1024 * 1024, budget.clone());
    let store = LocalChunkStore::open(temp.path(), "node-a")
        .unwrap()
        .with_outboard_budget(budget.clone());
    let mut readers = Vec::new();
    let mut body = vec![37; 4 * 1024 * 1024];
    for index in 0..200_u64 {
        body[..8].copy_from_slice(&index.to_le_bytes());
        let mut builder = ChunkBuilder::with_pool(pool.clone(), body.len()).unwrap();
        builder.write_at(0, &body).unwrap();
        let staged = builder.stage(OperationId::new(format!("natural-pinned-{index}")));
        assert!(
            staged.outboard.is_some(),
            "resident proofs must not starve writer proof generation"
        );
        let id = staged.chunk.id.clone();
        store.put(staged).unwrap();
        store.sidecar_installer.wait_idle();
        let reader = store.open_verified(&id).unwrap();
        let mut out = [0; 8];
        VERIFIED_SCAN_BYTES.with(|count| count.set(0));
        assert_eq!(reader.read_at(0, &mut out).unwrap(), 8);
        assert_eq!(
            VERIFIED_SCAN_BYTES.with(|count| count.get()),
            outboard::OUTBOARD_GROUP_BYTES as u64,
            "resident pressure must use a transient sidecar, not rescan 4MiB"
        );
        assert_eq!(out, index.to_le_bytes());
        readers.push(reader);
        assert!(budget.used() <= budget.limit());
        assert!(budget.resident_used() <= budget.resident_limit());
    }
    assert!(
        budget
            .reserve_range_work(
                &[VerifiedRangeCopy {
                    chunk_offset: 0,
                    length: 8,
                    output_offset: 0,
                }],
                body.len() as u64,
            )
            .is_some()
    );
    drop(readers);
    store.sidecar_installer.wait_idle();
    store.sidecar_installer.shutdown().unwrap();
    assert_eq!(budget.used(), 0);
}

#[test]
fn wi6_outboard_batch_syncs_directory_once_and_replay_skips_writes() {
    let temp = tempfile::tempdir().unwrap();
    let budget = OutboardBudget::new(DEFAULT_OUTBOARD_BUDGET_BYTES);
    let staged = pooled_outboard_chunks(&budget, 16, 4 * 1024 * 1024);
    let store = LocalChunkStore::open(temp.path(), "node-a")
        .unwrap()
        .with_outboard_budget(budget);
    take_store_outboard_persist_io(&store);
    let receipts = store.put_batch(staged.clone()).unwrap();
    store.sidecar_installer.wait_idle();
    assert_eq!(receipts.len(), 16);
    assert_eq!(
        take_store_outboard_persist_io(&store),
        OutboardPersistIo {
            writes: 16,
            file_syncs: 16,
            renames: 16,
            directory_syncs: 1,
        }
    );
    for item in &staged {
        let path = store
            .outboards
            .join(format!("{}.bao", safe_id(&item.chunk.id.0)));
        assert_eq!(
            fs::read(path).unwrap(),
            item.outboard.as_ref().unwrap().encoded()
        );
    }
    let revision = store.device_descriptor().unwrap().catalog_revision;
    let mut replay = staged.clone();
    replay.extend(staged);
    assert_eq!(store.put_batch(replay).unwrap().len(), 32);
    assert_eq!(
        store.device_descriptor().unwrap().catalog_revision,
        revision
    );
    assert_eq!(
        take_store_outboard_persist_io(&store),
        OutboardPersistIo {
            writes: 0,
            file_syncs: 0,
            renames: 0,
            directory_syncs: 1,
        }
    );
}

#[test]
fn wi6_outboard_batch_deduplicates_installs_and_tracks_actual_batches() {
    let temp = tempfile::tempdir().unwrap();
    let budget = OutboardBudget::new(DEFAULT_OUTBOARD_BUDGET_BYTES);
    let staged = pooled_outboard_chunks(&budget, 4, 128 * 1024);
    let store = LocalChunkStore::open(temp.path(), "node-a")
        .unwrap()
        .with_outboard_budget(budget);
    take_store_outboard_persist_io(&store);
    for pair in staged.chunks(2) {
        let mut batch = pair.to_vec();
        batch.extend_from_slice(pair);
        assert_eq!(store.put_batch(batch).unwrap().len(), 4);
        store.sidecar_installer.wait_idle();
    }
    assert_eq!(
        take_store_outboard_persist_io(&store),
        OutboardPersistIo {
            writes: 4,
            file_syncs: 4,
            renames: 4,
            directory_syncs: 2,
        }
    );
    store.put_batch(Vec::new()).unwrap();
    store.sidecar_installer.wait_idle();
    store
        .put(StagedChunk::new(
            OperationId::new("no-proof"),
            vec![8; 4096],
        ))
        .unwrap();
    assert_eq!(
        take_store_outboard_persist_io(&store),
        OutboardPersistIo::default()
    );
}

#[test]
fn wi6_outboard_batch_repairs_corrupt_cv_even_with_matching_header() {
    let temp = tempfile::tempdir().unwrap();
    let budget = OutboardBudget::new(DEFAULT_OUTBOARD_BUDGET_BYTES);
    let staged = pooled_outboard_chunks(&budget, 1, 128 * 1024)
        .pop()
        .unwrap();
    let store = LocalChunkStore::open(temp.path(), "node-a")
        .unwrap()
        .with_outboard_budget(budget);
    store.put(staged.clone()).unwrap();
    store.sidecar_installer.wait_idle();
    let path = store
        .outboards
        .join(format!("{}.bao", safe_id(&staged.chunk.id.0)));
    let mut corrupt = fs::read(&path).unwrap();
    corrupt[64] ^= 1;
    ChunkOutboard::validate_encoded_shape(&staged.chunk, &corrupt).unwrap();
    fs::write(&path, corrupt).unwrap();
    take_store_outboard_persist_io(&store);
    store.put(staged.clone()).unwrap();
    store.sidecar_installer.wait_idle();
    assert_eq!(
        fs::read(path).unwrap(),
        staged.outboard.as_ref().unwrap().encoded()
    );
    assert_eq!(
        take_store_outboard_persist_io(&store),
        OutboardPersistIo {
            writes: 1,
            file_syncs: 1,
            renames: 1,
            directory_syncs: 1,
        }
    );
}

#[test]
fn wi6_outboard_batch_optional_failures_keep_acks_and_retry_barriers() {
    let temp = tempfile::tempdir().unwrap();
    let budget = OutboardBudget::new(DEFAULT_OUTBOARD_BUDGET_BYTES);
    let staged = pooled_outboard_chunks(&budget, 2, 128 * 1024);
    let store = LocalChunkStore::open(temp.path(), "node-a")
        .unwrap()
        .with_outboard_budget(budget);
    let blocked = store
        .outboards
        .join(format!("{}.bao", safe_id(&staged[0].chunk.id.0)));
    fs::create_dir(&blocked).unwrap();
    take_store_outboard_persist_io(&store);
    assert_eq!(store.put_batch(staged.clone()).unwrap().len(), 2);
    assert_eq!(
        take_store_outboard_persist_io(&store),
        OutboardPersistIo {
            writes: 2,
            file_syncs: 2,
            renames: 1,
            directory_syncs: 1,
        }
    );
    assert!(fs::read_dir(&store.outboards).unwrap().all(|entry| {
        !entry
            .unwrap()
            .file_name()
            .to_string_lossy()
            .ends_with(".tmp")
    }));
    fs::remove_dir(blocked).unwrap();
    store
        .sidecar_install_test
        .fail_directory_sync
        .store(true, Ordering::Relaxed);
    assert_eq!(store.put_batch(staged.clone()).unwrap().len(), 2);
    assert_eq!(
        take_store_outboard_persist_io(&store),
        OutboardPersistIo {
            writes: 1,
            file_syncs: 1,
            renames: 1,
            directory_syncs: 0,
        }
    );
    assert_eq!(store.put_batch(staged.clone()).unwrap().len(), 2);
    assert_eq!(
        take_store_outboard_persist_io(&store),
        OutboardPersistIo {
            writes: 0,
            file_syncs: 0,
            renames: 0,
            directory_syncs: 1,
        }
    );
    drop(store);
    let recovered = LocalChunkStore::open(temp.path(), "node-a").unwrap();
    for item in staged {
        let mut out = [0; 17];
        assert_eq!(recovered.read_at(&item.chunk.id, 3, &mut out).unwrap(), 17);
        assert_eq!(out, item.bytes()[3..20]);
    }
}

fn t1_pause_budget_wait(
    store: &LocalChunkStore,
) -> (std::sync::mpsc::Receiver<()>, std::sync::mpsc::Sender<()>) {
    let (entered, observed) = std::sync::mpsc::channel();
    let (release, waiting) = std::sync::mpsc::channel();
    *store.peer_read_budget.before_wait.lock().unwrap() = Some((entered, waiting));
    (observed, release)
}

#[test]
fn p2_t1_last_lease_release_cannot_be_lost_before_wait() {
    let temp = tempfile::tempdir().unwrap();
    let store = LocalChunkStore::open(temp.path(), "node-a")
        .unwrap()
        .with_peer_serve_wait(Duration::from_millis(50))
        .unwrap();
    let held = store
        .reserve_peer_read_payload(PEER_READ_PAYLOAD_LIMIT)
        .unwrap();
    let (observed, release) = t1_pause_budget_wait(&store);
    std::thread::scope(|scope| {
        let reader =
            scope.spawn(|| store.reserve_peer_read_payload_until(17, store.peer_serve_deadline()));
        observed.recv_timeout(Duration::from_secs(3)).unwrap();
        // The waiter holds the predicate mutex here. A releaser racing this
        // gap must synchronize before refunding and notifying it.
        let (started, starting) = std::sync::mpsc::channel();
        let returning = scope.spawn(move || {
            started.send(()).unwrap();
            drop(held);
        });
        starting.recv_timeout(Duration::from_secs(3)).unwrap();
        release.send(()).unwrap();
        returning.join().unwrap();
        let admitted = reader.join().unwrap().unwrap();
        assert_eq!(store.peer_read_budget.used.load(Ordering::Acquire), 17);
        drop(admitted);
    });
    assert_eq!(store.peer_read_budget.used.load(Ordering::Acquire), 0);
}

#[test]
fn p2_t1_control_release_and_stop_wake_budget_waiters() {
    for stop in [false, true] {
        let temp = tempfile::tempdir().unwrap();
        let store = LocalChunkStore::open(temp.path(), "node-a")
            .unwrap()
            .with_peer_serve_wait(Duration::from_millis(50))
            .unwrap();
        let limit = PEER_SERVE_CONTROL_LIMIT - peer_serve_fixed_control_bytes();
        let held = store.reserve_peer_control(limit).unwrap();
        let (observed, release) = t1_pause_budget_wait(&store);
        std::thread::scope(|scope| {
            let reader =
                scope.spawn(|| store.reserve_peer_control_until(17, store.peer_serve_deadline()));
            observed.recv_timeout(Duration::from_secs(3)).unwrap();
            release.send(()).unwrap();
            if stop {
                store.close_peer_serve_admission();
                assert_eq!(
                    reader.join().unwrap().unwrap_err().code(),
                    afs_error::IO_UNAVAILABLE
                );
                assert_eq!(
                    store.peer_read_budget.control_used.load(Ordering::Acquire),
                    limit as u64
                );
                drop(held);
            } else {
                drop(held);
                let admitted = reader.join().unwrap().unwrap();
                assert_eq!(
                    store.peer_read_budget.control_used.load(Ordering::Acquire),
                    17
                );
                drop(admitted);
            }
        });
        assert_eq!(
            store.peer_read_budget.control_used.load(Ordering::Acquire),
            0
        );
    }
}

#[test]
fn p2_t1_zero_wait_and_expired_entry_deadline_never_enter_wait() {
    for wait in [Duration::ZERO, Duration::from_millis(20)] {
        let temp = tempfile::tempdir().unwrap();
        let store = LocalChunkStore::open(temp.path(), "node-a")
            .unwrap()
            .with_peer_serve_wait(wait)
            .unwrap();
        let held = store
            .reserve_peer_read_payload(PEER_READ_PAYLOAD_LIMIT)
            .unwrap();
        let (_observed, _release) = t1_pause_budget_wait(&store);
        assert_eq!(
            store
                .reserve_peer_read_payload_until(1, Instant::now())
                .unwrap_err()
                .code(),
            afs_error::IO_CAPACITY
        );
        assert!(
            store.peer_read_budget.before_wait.lock().unwrap().is_some(),
            "expired/zero allowance cannot be renewed at inner admission"
        );
        drop(held);
        let immediate = store
            .reserve_peer_read_payload_until(1, Instant::now())
            .unwrap();
        drop(immediate);
    }
    let temp = tempfile::tempdir().unwrap();
    let store = LocalChunkStore::open(temp.path(), "node-a").unwrap();
    assert_eq!(store.peer_read_budget.wait, Duration::from_millis(20));
    assert!(
        store
            .with_peer_serve_wait(Duration::from_millis(51))
            .is_err()
    );
}

#[test]
fn p2_t1_notifications_do_not_renew_the_entry_deadline() {
    let temp = tempfile::tempdir().unwrap();
    let store = LocalChunkStore::open(temp.path(), "node-a").unwrap();
    let held = store
        .reserve_peer_read_payload(PEER_READ_PAYLOAD_LIMIT)
        .unwrap();
    let notifying = std::sync::atomic::AtomicBool::new(true);
    let (done, completed) = std::sync::mpsc::channel();
    std::thread::scope(|scope| {
        let reader = scope.spawn(|| {
            done.send(
                store
                    .reserve_peer_read_payload_until(1, store.peer_serve_deadline())
                    .map(drop),
            )
            .unwrap();
        });
        let notifier = scope.spawn(|| {
            while notifying.load(Ordering::Acquire) {
                store.peer_read_budget.changed.notify_all();
                std::thread::yield_now();
            }
        });
        // This generous bound checks termination, not a performance claim.
        // Continual spurious notifications must not restart a 20ms wait.
        let result = completed.recv_timeout(Duration::from_secs(1));
        notifying.store(false, Ordering::Release);
        if result.is_err() {
            store.close_peer_serve_admission();
        }
        notifier.join().unwrap();
        reader.join().unwrap();
        assert_eq!(result.unwrap().unwrap_err().code(), afs_error::IO_CAPACITY);
    });
    assert_eq!(
        store.peer_read_budget.used.load(Ordering::Acquire),
        PEER_READ_PAYLOAD_LIMIT as u64
    );
    drop(held);
}

#[cfg(feature = "dfs")]
#[test]
fn p2_t1_last_stream_slice_keeps_capacity_until_its_release() {
    let temp = tempfile::tempdir().unwrap();
    let store = LocalChunkStore::open(temp.path(), "node-a")
        .unwrap()
        .with_peer_serve_wait(Duration::from_millis(50))
        .unwrap();
    let staged = StagedChunk::new(OperationId::new("t1-pinned-frame"), vec![37; 3 * 32768]);
    store.put(staged.clone()).unwrap();
    drop(store.open_verified(&staged.chunk.id).unwrap());
    let mut stream = store
        .open_peer_group_stream(&staged.chunk.id, 0, staged.bytes().len())
        .unwrap()
        .unwrap();
    let first = stream.next_group(|| Ok(())).unwrap().unwrap();
    let retained = first.slice(3..11);
    let second = stream.next_group(|| Ok(())).unwrap().unwrap();
    drop(stream);
    drop(first);
    drop(second);
    let held = store
        .reserve_peer_read_payload(PEER_READ_PAYLOAD_LIMIT - 65536)
        .unwrap();
    assert_eq!(
        store.peer_read_budget.used.load(Ordering::Acquire),
        PEER_READ_PAYLOAD_LIMIT as u64
    );
    let (observed, release) = t1_pause_budget_wait(&store);
    std::thread::scope(|scope| {
        let reader =
            scope.spawn(|| store.reserve_peer_read_payload_until(17, store.peer_serve_deadline()));
        observed.recv_timeout(Duration::from_secs(3)).unwrap();
        assert_eq!(retained.as_ref(), &[37; 8]);
        assert_eq!(
            store.peer_read_budget.used.load(Ordering::Acquire),
            PEER_READ_PAYLOAD_LIMIT as u64
        );
        release.send(()).unwrap();
        drop(retained);
        let admitted = reader.join().unwrap().unwrap();
        assert_eq!(
            store.peer_read_budget.used.load(Ordering::Acquire),
            (PEER_READ_PAYLOAD_LIMIT - 65536 + 17) as u64
        );
        drop(admitted);
    });
    drop(held);
    assert_eq!(store.peer_read_budget.used.load(Ordering::Acquire), 0);
}

#[cfg(feature = "dfs")]
#[test]
fn p2_r4_two_frames_hold_exact_budget_until_last_reference() {
    let temp = tempfile::tempdir().unwrap();
    let store = LocalChunkStore::open(temp.path(), "node-a").unwrap();
    let staged = StagedChunk::new(OperationId::new("r4-frame-leases"), vec![37; 3 * 32768]);
    store.put(staged.clone()).unwrap();
    drop(store.open_verified(&staged.chunk.id).unwrap());
    let mut stream = store
        .open_peer_group_stream(&staged.chunk.id, 0, staged.bytes().len())
        .unwrap()
        .unwrap();
    let first = stream.next_group(|| Ok(())).unwrap().unwrap();
    let second = stream.next_group(|| Ok(())).unwrap().unwrap();
    assert_eq!(first.len(), 32768);
    assert_eq!(second.len(), 32768);
    let retained = first.slice(17..29);
    assert_eq!(store.peer_read_budget.used.load(Ordering::Acquire), 65536);
    drop(stream);
    drop(first);
    assert_eq!(store.peer_read_budget.used.load(Ordering::Acquire), 65536);
    drop(second);
    assert_eq!(
        store.peer_read_budget.used.load(Ordering::Acquire),
        65536,
        "the surviving sliced frame still owns the two-block pool"
    );
    assert_eq!(retained.as_ref(), &[37; 12]);
    drop(retained);
    assert_eq!(store.peer_read_budget.used.load(Ordering::Acquire), 0);
    assert_eq!(
        store.peer_read_budget.control_used.load(Ordering::Acquire),
        0
    );
    assert!(
        store
            .peer_read_cache
            .lock()
            .unwrap()
            .residents
            .iter()
            .all(Option::is_none)
    );
}

#[cfg(feature = "dfs")]
#[test]
fn p2_r4_retained_frames_backpressure_without_early_refund() {
    let temp = tempfile::tempdir().unwrap();
    let store = LocalChunkStore::open(temp.path(), "node-a").unwrap();
    let staged = StagedChunk::new(OperationId::new("r4-backpressure"), vec![37; 4 * 32768]);
    store.put(staged.clone()).unwrap();
    drop(store.open_verified(&staged.chunk.id).unwrap());
    let mut stream = store
        .open_peer_group_stream(&staged.chunk.id, 0, staged.bytes().len())
        .unwrap()
        .unwrap();
    let first = stream.next_group(|| Ok(())).unwrap().unwrap();
    let second = stream.next_group(|| Ok(())).unwrap().unwrap();
    let (entered, observed) = std::sync::mpsc::channel();
    let (done, received) = std::sync::mpsc::channel();
    std::thread::scope(|scope| {
        let producer = scope.spawn(|| {
            // Repeated checks prove we reached the no-free-block wait,
            // rather than inferring blocking from scheduler delays.
            let calls = std::cell::Cell::new(0);
            let third = stream
                .next_group(|| {
                    calls.set(calls.get() + 1);
                    if calls.get() == 3 {
                        entered.send(()).unwrap();
                    }
                    Ok(())
                })
                .unwrap()
                .unwrap();
            done.send(third).unwrap();
        });
        observed.recv_timeout(Duration::from_secs(3)).unwrap();
        assert!(received.try_recv().is_err());
        assert_eq!(store.peer_read_budget.used.load(Ordering::Acquire), 65536);
        drop(first);
        let third = received.recv_timeout(Duration::from_secs(3)).unwrap();
        assert_eq!(third.as_ref(), &[37; 32768]);
        drop(third);
        producer.join().unwrap();
    });
    drop(second);
    drop(stream);
    assert_eq!(store.peer_read_budget.used.load(Ordering::Acquire), 0);
}

#[cfg(feature = "dfs")]
#[test]
fn p2_r4_stop_and_caller_failure_interrupt_retained_frame_wait() {
    for stop_node in [false, true] {
        let temp = tempfile::tempdir().unwrap();
        let store = LocalChunkStore::open(temp.path(), "node-a").unwrap();
        let staged = StagedChunk::new(OperationId::new("r4-wait-cancel"), vec![37; 4 * 32768]);
        store.put(staged.clone()).unwrap();
        drop(store.open_verified(&staged.chunk.id).unwrap());
        let mut stream = store
            .open_peer_group_stream(&staged.chunk.id, 0, staged.bytes().len())
            .unwrap()
            .unwrap();
        let first = stream.next_group(|| Ok(())).unwrap().unwrap();
        let second = stream.next_group(|| Ok(())).unwrap().unwrap();
        let valid = std::sync::atomic::AtomicBool::new(true);
        let (entered, observed) = std::sync::mpsc::channel();
        std::thread::scope(|scope| {
            let producer = scope.spawn(|| {
                let calls = std::cell::Cell::new(0);
                stream.next_group(|| {
                    calls.set(calls.get() + 1);
                    if calls.get() == 3 {
                        entered.send(()).unwrap();
                    }
                    if valid.load(Ordering::Acquire) {
                        Ok(())
                    } else {
                        Err(Error::coded(
                            afs_error::IO_PERMISSION_DENIED,
                            "caller revoked",
                        ))
                    }
                })
            });
            observed.recv_timeout(Duration::from_secs(3)).unwrap();
            if stop_node {
                store.close_peer_serve_admission();
            } else {
                valid.store(false, Ordering::Release);
            }
            let error = producer.join().unwrap().unwrap_err();
            assert_eq!(
                error.code(),
                if stop_node {
                    afs_error::IO_UNAVAILABLE
                } else {
                    afs_error::IO_PERMISSION_DENIED
                }
            );
        });
        drop(stream);
        assert_eq!(store.peer_read_budget.used.load(Ordering::Acquire), 65536);
        drop(first);
        drop(second);
        assert_eq!(store.peer_read_budget.used.load(Ordering::Acquire), 0);
    }
}

#[cfg(feature = "dfs")]
#[test]
fn p2_r4_stream_reuses_two_addresses_and_checks_each_group_root() {
    let temp = tempfile::tempdir().unwrap();
    let store = LocalChunkStore::open(temp.path(), "node-a").unwrap();
    let body: Vec<_> = (0..150_001).map(|n| (n % 251) as u8).collect();
    let staged = StagedChunk::new(OperationId::new("r4-reuse"), body.clone());
    store.put(staged.clone()).unwrap();
    drop(store.open_verified(&staged.chunk.id).unwrap());
    VERIFIED_SCAN_BYTES.with(|count| count.set(0));
    let mut stream = store
        .open_peer_group_stream(&staged.chunk.id, 0, body.len())
        .unwrap()
        .unwrap();
    let mut received = Vec::new();
    let mut addresses = HashSet::new();
    while let Some(group) = stream.next_group(|| Ok(())).unwrap() {
        addresses.insert(group.as_ptr());
        received.extend_from_slice(&group);
        assert!(store.peer_read_budget.used.load(Ordering::Acquire) <= 65536);
    }
    assert_eq!(received, body);
    assert!(addresses.len() <= 2);
    assert_eq!(
        VERIFIED_SCAN_BYTES.with(|count| count.get()),
        body.len() as u64
    );
    drop(stream);
    assert_eq!(store.peer_read_budget.used.load(Ordering::Acquire), 0);
}

#[cfg(feature = "dfs")]
#[test]
fn p2_r4_path_replacement_and_catalog_change_cannot_complete_stream() {
    for replace_path in [false, true] {
        let temp = tempfile::tempdir().unwrap();
        let store = LocalChunkStore::open(temp.path(), "node-a").unwrap();
        let staged = StagedChunk::new(OperationId::new("r4-stream-identity"), vec![37; 3 * 32768]);
        store.put(staged.clone()).unwrap();
        drop(store.open_verified(&staged.chunk.id).unwrap());
        let mut stream = store
            .open_peer_group_stream(&staged.chunk.id, 0, staged.bytes().len())
            .unwrap()
            .unwrap();
        drop(stream.next_group(|| Ok(())).unwrap().unwrap());
        if replace_path {
            let replacement = temp.path().join("replacement");
            fs::write(&replacement, staged.bytes()).unwrap();
            fs::rename(replacement, store.chunk_path(&staged.chunk.id)).unwrap();
        } else {
            store
                .catalog
                .lock()
                .unwrap()
                .records
                .get_mut(&staged.chunk.id)
                .unwrap()
                .catalog_revision += 1;
        }
        let result = loop {
            match stream.next_group(|| Ok(())) {
                Ok(Some(bytes)) => drop(bytes),
                other => break other,
            }
        };
        assert_eq!(
            result.unwrap_err().code(),
            afs_error::NODE_TRANSFER_CORRUPT_DATA
        );
        drop(stream);
        assert_eq!(store.peer_read_budget.used.load(Ordering::Acquire), 0);
    }
}

#[cfg(feature = "dfs")]
#[test]
fn p2_r4_bad_sidecar_is_retired_without_quarantining_valid_body() {
    let temp = tempfile::tempdir().unwrap();
    let store = LocalChunkStore::open(temp.path(), "node-a").unwrap();
    let staged = StagedChunk::new(
        OperationId::new("r4-stream-proof-damage"),
        vec![37; 3 * 32768],
    );
    store.put(staged.clone()).unwrap();
    drop(store.open_verified(&staged.chunk.id).unwrap());
    let path = store
        .outboards
        .join(format!("{}.bao", safe_id(&staged.chunk.id.0)));
    let mut damaged = fs::read(&path).unwrap();
    *damaged.last_mut().unwrap() ^= 1;
    fs::write(&path, damaged).unwrap();
    let mut stream = store
        .open_peer_group_stream(&staged.chunk.id, 0, staged.bytes().len())
        .unwrap()
        .unwrap();
    let error = loop {
        match stream.next_group(|| Ok(())) {
            Ok(Some(bytes)) => drop(bytes),
            Ok(None) => panic!("damaged proof must not complete"),
            Err(error) => break error,
        }
    };
    assert!(verified_copy_is_invalid(&error));
    assert!(
        !path.exists(),
        "only the observed bad optional proof is retired"
    );
    assert!(
        store.quarantined_chunks().unwrap().is_empty(),
        "valid body is not quarantined for proof metadata damage"
    );
    drop(stream);
    assert!(
        store
            .open_peer_group_stream(&staged.chunk.id, 0, staged.bytes().len())
            .unwrap()
            .is_none()
    );
    let recovered = store
        .read_peer_range(&staged.chunk.id, 0, staged.bytes().len())
        .unwrap();
    assert_eq!(recovered.as_slice(), staged.bytes());
}

#[cfg(feature = "dfs")]
#[test]
fn p2_r4_absent_proof_and_optional_capacity_fall_back_before_header() {
    let temp = tempfile::tempdir().unwrap();
    let store = LocalChunkStore::open(temp.path(), "node-a").unwrap();
    let staged = StagedChunk::new(OperationId::new("r4-fallback"), vec![37; 3 * 32768]);
    store.put(staged.clone()).unwrap();
    drop(store.open_verified(&staged.chunk.id).unwrap());
    let lease = store
        .reserve_peer_read_payload(PEER_READ_PAYLOAD_LIMIT - 32768)
        .unwrap();
    assert!(
        store
            .open_peer_group_stream(&staged.chunk.id, 0, staged.bytes().len())
            .unwrap()
            .is_none()
    );
    assert_eq!(
        store.peer_read_budget.used.load(Ordering::Acquire),
        (PEER_READ_PAYLOAD_LIMIT - 32768) as u64
    );
    drop(lease);
    assert_eq!(store.peer_read_budget.used.load(Ordering::Acquire), 0);
    fs::remove_file(
        store
            .outboards
            .join(format!("{}.bao", safe_id(&staged.chunk.id.0))),
    )
    .unwrap();
    assert!(
        store
            .open_peer_group_stream(&staged.chunk.id, 0, staged.bytes().len())
            .unwrap()
            .is_none()
    );
    assert_eq!(store.peer_read_budget.used.load(Ordering::Acquire), 0);
    assert_eq!(
        store
            .read_peer_range(&staged.chunk.id, 0, staged.bytes().len())
            .unwrap()
            .as_slice(),
        staged.bytes()
    );
}

#[test]
fn peer_object_digest_capability_requires_the_complete_verified_view() {
    let temp = tempfile::tempdir().unwrap();
    let store = LocalChunkStore::open(temp.path(), "node-a").unwrap();
    let staged = StagedChunk::new(OperationId::new("whole-proof"), vec![37; 150_001]);
    store.put(staged.clone()).unwrap();
    VERIFIED_SCAN_BYTES.with(|count| count.set(0));
    let whole = store
        .read_peer_range(&staged.chunk.id, 0, staged.bytes().len())
        .unwrap();
    assert_eq!(
        whole.verified_full_object_digest(),
        Some(staged.chunk.content_digest.clone())
    );
    assert!(
        store
            .read_peer_range(&staged.chunk.id, 0, staged.bytes().len() - 1)
            .unwrap()
            .verified_full_object_digest()
            .is_none()
    );
    assert!(
        store
            .read_peer_range(&staged.chunk.id, 1, staged.bytes().len() - 1)
            .unwrap()
            .verified_full_object_digest()
            .is_none()
    );
    assert_eq!(
        VERIFIED_SCAN_BYTES.with(|count| count.get()),
        staged.chunk.length
    );
}

#[test]
fn peer_object_digest_capability_is_absent_for_full_range_fallback() {
    let temp = tempfile::tempdir().unwrap();
    let store = LocalChunkStore::open(temp.path(), "node-a").unwrap();
    let staged = StagedChunk::new(
        OperationId::new("fallback-proof"),
        vec![19; PEER_READ_OBJECT_LIMIT + 1],
    );
    store.put(staged.clone()).unwrap();
    let whole = store
        .read_peer_range(&staged.chunk.id, 0, staged.bytes().len())
        .unwrap();
    assert_eq!(whole.as_slice(), staged.bytes());
    assert!(whole.object.identity.is_none());
    assert!(whole.verified_full_object_digest().is_none());
    assert!(
        store
            .peer_read_cache
            .lock()
            .unwrap()
            .residents
            .iter()
            .all(Option::is_none)
    );
    drop(whole);
    assert_eq!(store.peer_read_budget.used.load(Ordering::Acquire), 0);
}

#[test]
fn peer_object_digest_does_not_release_shared_frame_budget_early() {
    let temp = tempfile::tempdir().unwrap();
    let store = LocalChunkStore::open(temp.path(), "node-a").unwrap();
    let staged = StagedChunk::new(OperationId::new("proof-lease"), vec![29; 150_001]);
    store.put(staged.clone()).unwrap();
    let whole = store
        .read_peer_range(&staged.chunk.id, 0, staged.bytes().len())
        .unwrap();
    let digest = whole.verified_full_object_digest().unwrap();
    let bytes = whole.into_shared_bytes();
    let frame = bytes.slice(17..100);
    store.clear_peer_residents_for_test();
    drop(bytes);
    assert_eq!(
        store.peer_read_budget.used.load(Ordering::Acquire),
        staged.chunk.length
    );
    assert_eq!(digest, staged.chunk.content_digest);
    assert_eq!(&frame[..], &[29; 83]);
    drop(frame);
    assert_eq!(store.peer_read_budget.used.load(Ordering::Acquire), 0);
}

#[test]
fn wi6_pinned_small_reads_do_not_repeat_complete_object_scans() {
    let temp = tempfile::tempdir().unwrap();
    let store = LocalChunkStore::open(temp.path(), "node-a").unwrap();
    let staged = StagedChunk::new(
        OperationId::new("outboard-old-copy"),
        vec![19; 4 * 1024 * 1024],
    );
    store.put(staged.clone()).unwrap();
    let reader = store.open_verified(&staged.chunk.id).unwrap();
    VERIFIED_SCAN_BYTES.with(|count| count.set(0));
    for offset in [3, 32771] {
        let mut bytes = [0; 17];
        assert_eq!(reader.read_at(offset, &mut bytes).unwrap(), 17);
        assert_eq!(bytes, [19; 17]);
    }
    let scanned = VERIFIED_SCAN_BYTES.with(|count| count.get());
    assert!(
        scanned <= 2 * 32768,
        "two small ranges scanned {scanned} bytes"
    );
}

#[test]
fn wi6_legacy_and_half_sidecars_recover_without_rewriting_body_or_catalog() {
    let temp = tempfile::tempdir().unwrap();
    let store = LocalChunkStore::open(temp.path(), "node-a").unwrap();
    let staged = StagedChunk::new(OperationId::new("outboard-recovery"), vec![23; 150_001]);
    store.put(staged.clone()).unwrap();
    let original_revision = store.device_descriptor().unwrap().catalog_revision;
    let body_path = store.chunk_path(&staged.chunk.id);
    let identity = PeerFileIdentity::read(&File::open(&body_path).unwrap()).unwrap();
    let proof_path = store
        .outboards
        .join(format!("{}.bao", safe_id(&staged.chunk.id.0)));
    assert!(
        !proof_path.exists(),
        "unpooled writes retain old root-only behavior"
    );
    let mut permissions = fs::metadata(&body_path).unwrap().permissions();
    permissions.set_readonly(true);
    fs::set_permissions(&body_path, permissions).unwrap();
    // chmod changes ctime, record the identity after restricting the body.
    let identity = PeerFileIdentity::read(&File::open(&body_path).unwrap()).unwrap_or(identity);
    fs::write(&proof_path, b"partial").unwrap();
    fs::write(store.outboards.join(".abandoned.tmp"), b"partial").unwrap();
    drop(store);
    let recovered = LocalChunkStore::open(temp.path(), "node-a").unwrap();
    assert!(!recovered.outboards.join(".abandoned.tmp").exists());
    let reader = recovered.open_verified(&staged.chunk.id).unwrap();
    assert_eq!(
        PeerFileIdentity::read(&File::open(&body_path).unwrap()).unwrap(),
        identity
    );
    assert_eq!(
        recovered.device_descriptor().unwrap().catalog_revision,
        original_revision
    );
    let encoded = fs::read(&proof_path).unwrap();
    ChunkOutboard::validate_encoded_shape(&staged.chunk, &encoded).unwrap();
    let mut out = [0; 19];
    assert_eq!(reader.read_at(3, &mut out).unwrap(), 19);
    assert_eq!(out, [23; 19]);
    assert!(fs::read_dir(&recovered.outboards).unwrap().all(|entry| {
        !entry
            .unwrap()
            .file_name()
            .to_string_lossy()
            .ends_with(".tmp")
    }));
    let empty_budget = OutboardBudget::new(0);
    let constrained = LocalChunkStore::open(temp.path(), "node-a")
        .unwrap()
        .with_outboard_budget(empty_budget.clone());
    let mut out = [0; 19];
    assert_eq!(
        constrained.read_at(&staged.chunk.id, 3, &mut out).unwrap(),
        19
    );
    assert_eq!(out, [23; 19]);
    assert_eq!(empty_budget.used(), 0);
}

#[test]
fn wi6_canonical_catalog_mismatch_cannot_replace_the_trusted_root() {
    let temp = tempfile::tempdir().unwrap();
    let store = LocalChunkStore::open(temp.path(), "node-a").unwrap();
    let staged = StagedChunk::new(OperationId::new("canonical-anchor"), vec![3; 150_001]);
    store.put(staged.clone()).unwrap();
    let forged = StagedChunk::new(OperationId::new("forged-catalog"), vec![7; 150_001]);
    fs::write(store.chunk_path(&staged.chunk.id), forged.bytes()).unwrap();
    {
        let mut catalog = store.catalog.lock().unwrap();
        let record = catalog.records.get_mut(&staged.chunk.id).unwrap();
        record.chunk.content_digest = forged.chunk.content_digest.clone();
        record.stored_checksum = forged.chunk.content_digest.clone();
    }
    for error in [
        store.open_verified(&staged.chunk.id).unwrap_err(),
        store.read_peer_range(&staged.chunk.id, 3, 17).unwrap_err(),
    ] {
        assert_eq!(error.code(), afs_error::NODE_TRANSFER_CORRUPT_DATA);
    }
}

#[test]
fn wi6_range_work_budget_pressure_falls_back_to_complete_root() {
    let temp = tempfile::tempdir().unwrap();
    let budget = OutboardBudget::new(16 * 1024);
    let store = LocalChunkStore::open(temp.path(), "node-a")
        .unwrap()
        .with_outboard_budget(budget.clone());
    let staged = StagedChunk::new(OperationId::new("work-budget-fallback"), vec![5; 150_001]);
    store.put(staged.clone()).unwrap();
    let reader = store.open_verified(&staged.chunk.id).unwrap();
    assert!(reader.load_outboard().unwrap().is_some());
    let before = budget.used();
    VERIFIED_SCAN_BYTES.with(|count| count.set(0));
    let mut out = [0; 17];
    assert_eq!(reader.read_at(3, &mut out).unwrap(), 17);
    assert_eq!(out, [5; 17]);
    assert_eq!(VERIFIED_SCAN_BYTES.with(|count| count.get()), 150_001);
    assert_eq!(budget.used(), before);
}

#[test]
fn wi6_pooled_stage_keeps_body_address_and_persists_optional_same_budget_proof() {
    let temp = tempfile::tempdir().unwrap();
    let budget = OutboardBudget::new(DEFAULT_OUTBOARD_BUDGET_BYTES);
    let pool = WriteBufferPool::new_with_outboard_budget(32768, budget.clone());
    let mut builder = ChunkBuilder::with_pool(pool.clone(), 32768).unwrap();
    builder.write_at(0, &vec![7; 32768]).unwrap();
    let frozen = builder.freeze();
    let pointer = frozen.as_ptr();
    let staged = StagedChunk::from_shared(OperationId::new("outboard-pooled"), frozen);
    assert_eq!(staged.bytes().as_ptr(), pointer);
    assert!(staged.outboard.as_ref().unwrap().uses_budget(&budget));
    assert_eq!(pool.snapshot().copied_bytes, 32768);
    let store = LocalChunkStore::open(temp.path(), "node-a")
        .unwrap()
        .with_outboard_budget(budget.clone());
    store.put(staged.clone()).unwrap();
    // This assertion is specifically about a completed optional install,
    // not the earlier durable body/catalog ACK.
    store.sidecar_installer.wait_idle();
    let proof_path = store
        .outboards
        .join(format!("{}.bao", safe_id(&staged.chunk.id.0)));
    ChunkOutboard::validate_encoded_shape(&staged.chunk, &fs::read(proof_path).unwrap()).unwrap();
    let plain_pool = WriteBufferPool::new(32768);
    let mut plain = ChunkBuilder::with_pool(plain_pool, 32768).unwrap();
    plain.write_at(0, b"plain").unwrap();
    assert!(
        plain
            .stage(OperationId::new("root-only"))
            .outboard
            .is_none()
    );
}

#[test]
fn wi6_peer_partial_uses_covering_groups_and_exact_payload_lease() {
    let temp = tempfile::tempdir().unwrap();
    let budget = OutboardBudget::new(DEFAULT_OUTBOARD_BUDGET_BYTES);
    let pool = WriteBufferPool::new_with_outboard_budget(4 * 1024 * 1024, budget.clone());
    let mut builder = ChunkBuilder::with_pool(pool.clone(), 4 * 1024 * 1024).unwrap();
    builder.write_at(0, &vec![11; 4 * 1024 * 1024]).unwrap();
    let staged = builder.stage(OperationId::new("outboard-partial"));
    let store = LocalChunkStore::open(temp.path(), "node-a")
        .unwrap()
        .with_outboard_budget(budget);
    store.put(staged.clone()).unwrap();
    // Measure the proof-ready range path. An ACK-before-sidecar read may
    // legitimately verify the full object, covered separately above.
    store.sidecar_installer.wait_idle();
    VERIFIED_SCAN_BYTES.with(|count| count.set(0));
    let payload = store.read_peer_range(&staged.chunk.id, 3, 17).unwrap();
    assert_eq!(payload.as_slice(), &[11; 17]);
    assert!(payload.verified_full_object_digest().is_none());
    assert!(payload.outboard().is_none());
    assert_eq!(VERIFIED_SCAN_BYTES.with(|count| count.get()), 32768);
    assert_eq!(store.peer_read_budget.used.load(Ordering::Acquire), 17);
    drop(payload);
    assert_eq!(store.peer_read_budget.used.load(Ordering::Acquire), 0);
}

#[test]
fn peer_object_reuses_one_complete_scan_for_multiple_ranges() {
    let temp = tempfile::tempdir().unwrap();
    let store = LocalChunkStore::open(temp.path(), "node-a").unwrap();
    let bytes = vec![37; 4 * 1024 * 1024];
    let staged = StagedChunk::new(OperationId::new("peer-reuse"), bytes.clone());
    store.put(staged.clone()).unwrap();
    VERIFIED_SCAN_BYTES.with(|count| count.set(0));
    for index in 0..4 {
        let payload = store
            .read_peer_range(&staged.chunk.id, index * 1024 * 1024, 1024 * 1024)
            .unwrap();
        assert_eq!(payload.as_slice(), &bytes[..1024 * 1024]);
    }
    assert_eq!(
        VERIFIED_SCAN_BYTES.with(|count| count.get()),
        bytes.len() as u64
    );
    assert_eq!(
        store.peer_read_budget.used.load(Ordering::Acquire),
        bytes.len() as u64
    );
}

#[test]
fn peer_object_revalidates_replacement_missing_and_quarantined_files() {
    for change in [
        "replace",
        "missing",
        "quarantine",
        "corrupt",
        "epoch",
        "revision",
    ] {
        let temp = tempfile::tempdir().unwrap();
        let store = LocalChunkStore::open(temp.path(), "node-a").unwrap();
        let bytes = vec![53; 150_001];
        let staged = StagedChunk::new(OperationId::new("peer-identity"), bytes.clone());
        store.put(staged.clone()).unwrap();
        drop(store.read_peer_range(&staged.chunk.id, 3, 19).unwrap());
        VERIFIED_SCAN_BYTES.with(|count| count.set(0));
        match change {
            "replace" => {
                let replacement = temp.path().join("replacement");
                fs::write(&replacement, &bytes).unwrap();
                fs::rename(replacement, store.chunk_path(&staged.chunk.id)).unwrap();
            }
            "missing" => fs::remove_file(store.chunk_path(&staged.chunk.id)).unwrap(),
            "corrupt" => {
                let path = store.chunk_path(&staged.chunk.id);
                let modified = fs::metadata(&path).unwrap().modified().unwrap();
                fs::write(&path, vec![1; bytes.len()]).unwrap();
                // Same-size writes can share one filesystem timestamp tick.
                // Exercise revalidation with a deterministically new identity.
                File::options()
                    .write(true)
                    .open(path)
                    .unwrap()
                    .set_times(fs::FileTimes::new().set_modified(modified + Duration::from_secs(1)))
                    .unwrap();
            }
            _ => {
                let mut catalog = store.catalog.lock().unwrap();
                let record = catalog.records.get_mut(&staged.chunk.id).unwrap();
                match change {
                    "quarantine" => record.state = LocalChunkState::Quarantined,
                    "epoch" => record.device_epoch += 1,
                    "revision" => record.catalog_revision += 1,
                    _ => unreachable!(),
                }
            }
        }
        let result = store.read_peer_range(&staged.chunk.id, 3, 19);
        if change == "replace" || change == "revision" {
            assert_eq!(result.unwrap().as_slice(), &bytes[3..22]);
            assert_eq!(
                VERIFIED_SCAN_BYTES.with(|count| count.get()),
                bytes.len() as u64
            );
        } else {
            assert!(result.is_err(), "{change} must not reuse a stale proof");
            assert!(
                store
                    .peer_read_cache
                    .lock()
                    .unwrap()
                    .residents
                    .iter()
                    .all(Option::is_none)
            );
        }
    }
}

#[test]
fn peer_object_failed_fill_releases_budget_and_never_publishes() {
    let temp = tempfile::tempdir().unwrap();
    let store = LocalChunkStore::open(temp.path(), "node-a").unwrap();
    let staged = StagedChunk::new(OperationId::new("peer-failed"), vec![5; 4096]);
    store.put(staged.clone()).unwrap();
    fs::write(store.chunk_path(&staged.chunk.id), vec![9; 4096]).unwrap();
    assert!(store.read_peer_range(&staged.chunk.id, 0, 3).is_err());
    assert!(
        store
            .peer_read_cache
            .lock()
            .unwrap()
            .residents
            .iter()
            .all(Option::is_none)
    );
    assert_eq!(store.peer_read_budget.used.load(Ordering::Acquire), 0);
}

#[test]
fn peer_object_evicted_references_and_packed_payload_share_the_limit() {
    let temp = tempfile::tempdir().unwrap();
    let payload_limit = 8 * 1024 * 1024;
    let store = LocalChunkStore::open(temp.path(), "node-a")
        .unwrap()
        .with_peer_serve_config(1, payload_limit as u64)
        .unwrap();
    let chunks: Vec<_> = (1..=3)
        .map(|byte| {
            let chunk = StagedChunk::new(
                OperationId::new(format!("peer-budget-{byte}")),
                vec![byte; 4 * 1024 * 1024],
            );
            store.put(chunk.clone()).unwrap();
            chunk
        })
        .collect();
    let first = store.read_peer_range(&chunks[0].chunk.id, 0, 1).unwrap();
    let second = store.read_peer_range(&chunks[1].chunk.id, 0, 1).unwrap();
    assert_eq!(
        store.peer_read_budget.used.load(Ordering::Acquire),
        payload_limit as u64
    );
    assert_eq!(
        store
            .read_peer_range(&chunks[2].chunk.id, 0, 1)
            .unwrap_err()
            .code(),
        afs_error::IO_CAPACITY
    );
    assert_eq!(
        store.reserve_peer_read_payload(1).unwrap_err().code(),
        afs_error::IO_CAPACITY
    );
    assert_eq!(first.as_slice(), &[1]);
    drop(first);
    drop(second);
    let mut packed = store.reserve_peer_read_payload(4 * 1024 * 1024).unwrap();
    packed.retain_capacity(4 * 1024 * 1024).unwrap();
    assert!(store.read_peer_range(&chunks[2].chunk.id, 0, 1).is_ok());
    assert_eq!(
        store.peer_read_budget.used.load(Ordering::Acquire),
        payload_limit as u64
    );
    drop(packed);
    assert_eq!(
        store.peer_read_budget.used.load(Ordering::Acquire),
        4 * 1024 * 1024
    );
    assert_eq!(
        store
            .reserve_peer_read_payload(payload_limit + 1)
            .unwrap_err()
            .code(),
        afs_error::IO_CAPACITY
    );
}

#[test]
fn peer_object_budget_reservation_drop_and_capacity_growth_are_charged() {
    let temp = tempfile::tempdir().unwrap();
    let store = LocalChunkStore::open(temp.path(), "node-a").unwrap();
    let mut lease = store.reserve_peer_read_payload(19).unwrap();
    lease.retain_capacity(31).unwrap();
    assert_eq!(store.peer_read_budget.used.load(Ordering::Acquire), 31);
    assert!(lease.retain_capacity(PEER_READ_PAYLOAD_LIMIT + 1).is_err());
    assert_eq!(store.peer_read_budget.used.load(Ordering::Acquire), 31);
    drop(lease);
    assert_eq!(store.peer_read_budget.used.load(Ordering::Acquire), 0);
}

#[test]
fn p2_r6_default_lru_and_control_owners_release_only_after_last_frame() {
    let temp = tempfile::tempdir().unwrap();
    let registry = afs_metrics::Registry::new();
    let store = LocalChunkStore::open(temp.path(), "node-a")
        .unwrap()
        .with_read_work_metrics(
            super::super::read_work::ReadWorkMetrics::register(&registry).unwrap(),
        );
    println!(
        "R6_CONTROL fixed={} object={} flight={} cap={}",
        peer_serve_fixed_control_bytes(),
        std::mem::size_of::<PeerVerifiedObject>(),
        std::mem::size_of::<PeerServeFlight>(),
        PEER_SERVE_CONTROL_LIMIT
    );
    assert_eq!(store.peer_read_budget.limit, 16 * 1024 * 1024);
    assert_eq!(store.peer_read_cache.lock().unwrap().resident_limit, 2);
    let chunks: Vec<_> = (1..=3)
        .map(|byte| {
            StagedChunk::new(
                OperationId::new(format!("r6-lru-{byte}")),
                vec![byte; 64 * 1024],
            )
        })
        .collect();
    store.put_batch(chunks.clone()).unwrap();
    drop(
        store
            .read_peer_range(&chunks[0].chunk.id, 0, 64 * 1024)
            .unwrap(),
    );
    drop(
        store
            .read_peer_range(&chunks[1].chunk.id, 0, 64 * 1024)
            .unwrap(),
    );
    // Refresh first, then third must evict second.
    drop(
        store
            .read_peer_range(&chunks[0].chunk.id, 0, 64 * 1024)
            .unwrap(),
    );
    let third = store
        .read_peer_range(&chunks[2].chunk.id, 0, 64 * 1024)
        .unwrap();
    {
        let state = store.peer_read_cache.lock().unwrap();
        assert!(state.resident(&chunks[0].chunk.id).is_some());
        assert!(state.resident(&chunks[1].chunk.id).is_none());
        assert!(state.resident(&chunks[2].chunk.id).is_some());
        assert_eq!(state.residents.iter().flatten().count(), 2);
    }
    let bytes = third.into_shared_bytes();
    let frame = bytes.slice(17..100);
    drop(bytes);
    store.clear_peer_residents_for_test();
    assert_eq!(
        store.peer_read_budget.used.load(Ordering::Acquire),
        64 * 1024
    );
    assert!(store.peer_read_budget.control_used.load(Ordering::Acquire) > 0);
    assert!(
        store.peer_read_budget.control_peak.load(Ordering::Acquire) as usize
            + peer_serve_fixed_control_bytes()
            <= PEER_SERVE_CONTROL_LIMIT
    );
    store.close_peer_serve_admission();
    assert!(store.read_peer_range(&chunks[0].chunk.id, 0, 1).is_err());
    assert_eq!(&frame[..], &[3; 83]);
    assert_eq!(
        store.peer_read_budget.used.load(Ordering::Acquire),
        64 * 1024
    );
    drop(frame);
    assert_eq!(store.peer_read_budget.used.load(Ordering::Acquire), 0);
    assert_eq!(
        store.peer_read_budget.control_used.load(Ordering::Acquire),
        0
    );
    store.shutdown_peer_serve().unwrap();
    let text = afs_metrics::encode_text(&registry).unwrap();
    assert!(text.contains("afs_dfs_peer_serve_resources{resource=\"control_bytes\"} 0\n"));
}

#[test]
fn p2_r6_eight_keys_are_bounded_and_stop_does_not_release_active_flights() {
    let temp = tempfile::tempdir().unwrap();
    let store = LocalChunkStore::open(temp.path(), "node-a").unwrap();
    let chunks: Vec<_> = (0..9)
        .map(|index| {
            StagedChunk::new(
                OperationId::new(format!("r6-keys-{index}")),
                vec![index; 64 * 1024],
            )
        })
        .collect();
    store.put_batch(chunks.clone()).unwrap();
    let mut blocks = Vec::new();
    for chunk in &chunks[..8] {
        blocks.push(store.pause_next_peer_fill_for_test(chunk.chunk.id.clone()));
    }
    std::thread::scope(|scope| {
        let mut readers = Vec::new();
        for chunk in &chunks[..8] {
            readers.push(scope.spawn(|| store.read_peer_range(&chunk.chunk.id, 0, 64 * 1024)));
        }
        for (entered, _) in &blocks {
            entered.recv_timeout(Duration::from_secs(3)).unwrap();
        }
        {
            let state = store.peer_read_cache.lock().unwrap();
            assert_eq!(state.flights.iter().flatten().count(), 8);
            assert_eq!(state.readers, 8);
        }
        assert_eq!(
            store
                .read_peer_range(&chunks[8].chunk.id, 0, 64 * 1024)
                .unwrap_err()
                .code(),
            afs_error::IO_CAPACITY
        );
        let charged = store.peer_read_budget.control_used.load(Ordering::Acquire);
        store.close_peer_serve_admission();
        let deadline = store.peer_read_cache.lock().unwrap().stop_deadline;
        store.close_peer_serve_admission();
        assert_eq!(
            store.peer_read_cache.lock().unwrap().stop_deadline,
            deadline
        );
        assert_eq!(
            store.peer_read_budget.control_used.load(Ordering::Acquire),
            charged
        );
        for (_, release) in &blocks {
            release.send(()).unwrap();
        }
        for reader in readers {
            assert!(reader.join().unwrap().is_err());
        }
    });
    assert_eq!(store.peer_read_budget.used.load(Ordering::Acquire), 0);
    assert_eq!(
        store.peer_read_budget.control_used.load(Ordering::Acquire),
        0
    );
    store.shutdown_peer_serve().unwrap();
    let state = store.peer_read_cache.lock().unwrap();
    assert_eq!(state.readers, 0);
    assert!(state.flights.iter().all(Option::is_none));
}

#[test]
fn p2_r6_live_owner_blocks_reconfiguration_and_control_pressure_is_bounded() {
    let temp = tempfile::tempdir().unwrap();
    let store = LocalChunkStore::open(temp.path(), "node-a").unwrap();
    let chunk = StagedChunk::new(OperationId::new("r6-reconfigure"), vec![91; 64 * 1024]);
    store.put(chunk.clone()).unwrap();
    let payload = store
        .read_peer_range(&chunk.chunk.id, 0, 64 * 1024)
        .unwrap();
    let bytes = payload.into_shared_bytes();
    store.clear_peer_residents_for_test();
    assert!(store.with_peer_serve_config(1, 4 * 1024 * 1024).is_err());
    assert_eq!(bytes[17], 91);
    drop(bytes);

    let store = LocalChunkStore::open(temp.path(), "node-a").unwrap();
    let control = store
        .reserve_peer_control(PEER_SERVE_CONTROL_LIMIT - peer_serve_fixed_control_bytes())
        .unwrap();
    assert_eq!(
        store
            .read_peer_range(&chunk.chunk.id, 0, 64 * 1024)
            .unwrap_err()
            .code(),
        afs_error::IO_CAPACITY
    );
    assert_eq!(store.peer_read_budget.used.load(Ordering::Acquire), 0);
    drop(control);
    let payload = store
        .read_peer_range(&chunk.chunk.id, 0, 64 * 1024)
        .unwrap();
    assert_eq!(payload.as_slice(), chunk.bytes());
    drop(payload);
    store.shutdown_peer_serve().unwrap();
}

#[test]
fn p2_r6_bad_fill_shares_one_failure_and_never_installs_bytes() {
    let temp = tempfile::tempdir().unwrap();
    let store = LocalChunkStore::open(temp.path(), "node-a").unwrap();
    let chunk = StagedChunk::new(OperationId::new("r6-bad-flight"), vec![13; 64 * 1024]);
    store.put(chunk.clone()).unwrap();
    let (entered, release) = store.pause_next_peer_fill_for_test(chunk.chunk.id.clone());
    std::thread::scope(|scope| {
        let leader = scope.spawn(|| store.read_peer_range(&chunk.chunk.id, 0, 64 * 1024));
        entered.recv_timeout(Duration::from_secs(3)).unwrap();
        let followers: Vec<_> = (0..7)
            .map(|_| scope.spawn(|| store.read_peer_range(&chunk.chunk.id, 0, 64 * 1024)))
            .collect();
        let deadline = Instant::now() + Duration::from_secs(3);
        while store.waiting_peer_fill_readers_for_test() < 7 {
            assert!(
                Instant::now() < deadline,
                "followers did not join the blocked fill"
            );
            std::thread::yield_now();
        }
        fs::write(store.chunk_path(&chunk.chunk.id), vec![17; 64 * 1024]).unwrap();
        release.send(()).unwrap();
        let error = leader.join().unwrap().unwrap_err();
        for follower in followers {
            let failure = follower.join().unwrap().unwrap_err();
            assert_eq!(
                (failure.code(), failure.kind()),
                (error.code(), error.kind())
            );
        }
    });
    assert!(
        store
            .peer_read_cache
            .lock()
            .unwrap()
            .residents
            .iter()
            .all(Option::is_none)
    );
    assert_eq!(store.peer_read_budget.used.load(Ordering::Acquire), 0);
    assert_eq!(
        store.peer_read_budget.control_used.load(Ordering::Acquire),
        0
    );
    store.shutdown_peer_serve().unwrap();
}

#[test]
fn p2_r6_zero_byte_lease_is_a_real_owner_until_its_last_drop() {
    let temp = tempfile::tempdir().unwrap();
    let store = LocalChunkStore::open(temp.path(), "node-a").unwrap();
    let mut lease = store.reserve_peer_read_payload(0).unwrap();
    assert_eq!(store.peer_read_budget.used.load(Ordering::Acquire), 0);
    assert_eq!(Arc::strong_count(&store.peer_read_budget), 2);
    store.close_peer_serve_admission();
    let (started_tx, started_rx) = std::sync::mpsc::channel();
    let (done_tx, done_rx) = std::sync::mpsc::channel();
    std::thread::scope(|scope| {
        let stopping = scope.spawn(|| {
            started_tx.send(()).unwrap();
            let result = store.shutdown_peer_serve();
            done_tx.send(()).unwrap();
            result
        });
        started_rx.recv_timeout(Duration::from_secs(3)).unwrap();
        assert!(done_rx.recv_timeout(Duration::from_millis(100)).is_err());
        // A pre-existing owner can retain capacity; shutdown must still see
        // that real ownership even when its initial byte charge was zero.
        lease.retain_capacity(31).unwrap();
        assert_eq!(store.peer_read_budget.used.load(Ordering::Acquire), 31);
        drop(lease);
        stopping.join().unwrap().unwrap();
    });
    assert_eq!(Arc::strong_count(&store.peer_read_budget), 1);
    assert_eq!(store.peer_read_budget.used.load(Ordering::Acquire), 0);
}

#[test]
fn p2_t1_stop_rejects_new_owners_but_keeps_existing_capacity_charged() {
    let temp = tempfile::tempdir().unwrap();
    let store = LocalChunkStore::open(temp.path(), "node-a").unwrap();
    let mut lease = store.reserve_peer_read_payload(0).unwrap();
    store.close_peer_serve_admission();
    for bytes in [0, 1] {
        assert_eq!(
            store.reserve_peer_read_payload(bytes).unwrap_err().code(),
            afs_error::IO_UNAVAILABLE
        );
        assert_eq!(
            store
                .reserve_peer_read_payload_until(bytes, store.peer_serve_deadline())
                .unwrap_err()
                .code(),
            afs_error::IO_UNAVAILABLE
        );
    }
    lease.retain_capacity(31).unwrap();
    assert_eq!(store.peer_read_budget.used.load(Ordering::Acquire), 31);
    assert_eq!(store.peer_read_budget.peak.load(Ordering::Acquire), 31);
    assert_eq!(
        lease
            .retain_capacity(PEER_READ_PAYLOAD_LIMIT + 1)
            .unwrap_err()
            .code(),
        afs_error::IO_CAPACITY
    );
    assert_eq!(lease.bytes, 31);
    assert_eq!(store.peer_read_budget.used.load(Ordering::Acquire), 31);
    assert_eq!(Arc::strong_count(&store.peer_read_budget), 2);
    drop(lease);
    assert_eq!(store.peer_read_budget.used.load(Ordering::Acquire), 0);
    assert_eq!(Arc::strong_count(&store.peer_read_budget), 1);
    store.shutdown_peer_serve().unwrap();
}

#[test]
fn p2_r6_packed_admission_cannot_reopen_budget_after_shutdown() {
    let temp = tempfile::tempdir().unwrap();
    let store = LocalChunkStore::open(temp.path(), "node-a").unwrap();
    let (entered_tx, entered_rx) = std::sync::mpsc::channel();
    let (release_tx, release_rx) = std::sync::mpsc::channel();
    let (drained_tx, drained_rx) = std::sync::mpsc::channel();
    *store.peer_payload_test_block.lock().unwrap() = Some((entered_tx, release_rx));
    let drained = std::sync::atomic::AtomicBool::new(false);
    let (premature, admitted_after_drain) = std::thread::scope(|scope| {
        let allocating = scope.spawn(|| {
            let lease = store.reserve_peer_read_payload(64 * 1024);
            let late = lease.is_ok() && drained.load(Ordering::Acquire);
            drop(lease);
            late
        });
        entered_rx.recv_timeout(Duration::from_secs(3)).unwrap();
        store.close_peer_serve_admission();
        let stopping = scope.spawn(|| {
            let result = store.shutdown_peer_serve();
            drained.store(true, Ordering::Release);
            drained_tx.send(()).unwrap();
            result
        });
        // Both accepted I/O and rejected old admission are legal. Returning
        // successful shutdown while admission is still paused is not.
        let premature = drained_rx.recv_timeout(Duration::from_secs(2)).is_ok();
        release_tx.send(()).unwrap();
        let late = allocating.join().unwrap();
        stopping.join().unwrap().unwrap();
        (premature, late)
    });
    assert!(
        !premature && !admitted_after_drain,
        "packed admission acquired a new payload after shutdown had declared the budget drained"
    );
    assert_eq!(store.peer_read_budget.used.load(Ordering::Acquire), 0);
}

#[test]
fn p2_r6_old_ready_follower_cannot_retire_a_new_healthy_resident() {
    let temp = tempfile::tempdir().unwrap();
    let store = LocalChunkStore::open(temp.path(), "node-a").unwrap();
    let chunk = StagedChunk::new(OperationId::new("r6-old-ready"), vec![73; 64 * 1024]);
    store.put(chunk.clone()).unwrap();
    let (fill_entered, fill_release) = store.pause_next_peer_fill_for_test(chunk.chunk.id.clone());
    let (ready_entered, ready_observed) = std::sync::mpsc::channel();
    let (ready_release, ready_wait) = std::sync::mpsc::channel();
    *store.peer_ready_test_block.lock().unwrap() = Some((ready_entered, ready_wait));
    std::thread::scope(|scope| {
        let first = scope.spawn(|| store.read_peer_range(&chunk.chunk.id, 0, 64 * 1024));
        fill_entered.recv_timeout(Duration::from_secs(3)).unwrap();
        let old_follower = scope.spawn(|| store.read_peer_range(&chunk.chunk.id, 0, 64 * 1024));
        let deadline = Instant::now() + Duration::from_secs(3);
        while store.waiting_peer_fill_readers_for_test() == 0 {
            assert!(
                Instant::now() < deadline,
                "old follower did not join first fill"
            );
            std::thread::yield_now();
        }
        fill_release.send(()).unwrap();
        ready_observed.recv_timeout(Duration::from_secs(3)).unwrap();
        let old = first.join().unwrap().unwrap();
        let path = store.chunk_path(&chunk.chunk.id);
        fs::rename(&path, temp.path().join("old-healthy-inode")).unwrap();
        fs::write(&path, chunk.bytes()).unwrap();
        let replacement = store
            .read_peer_range(&chunk.chunk.id, 0, 64 * 1024)
            .unwrap();
        assert!(!Arc::ptr_eq(&old.object, &replacement.object));
        assert!(Arc::ptr_eq(
            &store
                .peer_read_cache
                .lock()
                .unwrap()
                .resident(&chunk.chunk.id)
                .unwrap(),
            &replacement.object
        ));
        ready_release.send(()).unwrap();
        assert_eq!(
            old_follower.join().unwrap().unwrap().as_slice(),
            chunk.bytes()
        );
        let indexed = store
            .peer_read_cache
            .lock()
            .unwrap()
            .resident(&chunk.chunk.id);
        assert!(
            indexed
                .as_ref()
                .is_some_and(|object| Arc::ptr_eq(object, &replacement.object)),
            "old Ready follower retired the newer healthy resident"
        );
        assert!(store.quarantined_chunks().unwrap().is_empty());
    });
    store.shutdown_peer_serve().unwrap();
}

#[test]
fn p2_r6_different_objects_fill_outside_shared_table_lock() {
    use std::sync::mpsc;
    let temp = tempfile::tempdir().unwrap();
    let store = LocalChunkStore::open(temp.path(), "node-a").unwrap();
    let first = StagedChunk::new(OperationId::new("r6-fill-first"), vec![31; 64 * 1024]);
    let second = StagedChunk::new(OperationId::new("r6-fill-second"), vec![47; 64 * 1024]);
    store
        .put_batch(vec![first.clone(), second.clone()])
        .unwrap();
    let (first_entered_tx, first_entered_rx) = mpsc::channel();
    let (first_release_tx, first_release_rx) = mpsc::channel();
    let (second_entered_tx, second_entered_rx) = mpsc::channel();
    let (second_release_tx, second_release_rx) = mpsc::channel();
    {
        let mut blocks = store.peer_fill_test_blocks.lock().unwrap();
        blocks.insert(first.chunk.id.clone(), (first_entered_tx, first_release_rx));
        blocks.insert(
            second.chunk.id.clone(),
            (second_entered_tx, second_release_rx),
        );
    }
    let concurrent = std::thread::scope(|scope| {
        let first_read =
            scope.spawn(|| store.read_peer_range(&first.chunk.id, 0, first.bytes().len()));
        first_entered_rx
            .recv_timeout(Duration::from_secs(2))
            .unwrap();
        let second_read =
            scope.spawn(|| store.read_peer_range(&second.chunk.id, 0, second.bytes().len()));
        // A bounded progress assertion, not a performance threshold. Always
        // release both workers and join before reporting the old slot bug.
        let concurrent = second_entered_rx
            .recv_timeout(Duration::from_secs(2))
            .is_ok();
        first_release_tx.send(()).unwrap();
        second_release_tx.send(()).unwrap();
        assert_eq!(
            first_read.join().unwrap().unwrap().as_slice(),
            first.bytes()
        );
        assert_eq!(
            second_read.join().unwrap().unwrap().as_slice(),
            second.bytes()
        );
        concurrent
    });
    assert!(
        concurrent,
        "an unrelated Peer fill remained behind the global object slot"
    );
}

#[test]
fn peer_object_concurrent_ranges_share_the_same_verified_payload() {
    let temp = tempfile::tempdir().unwrap();
    let store = Arc::new(LocalChunkStore::open(temp.path(), "node-a").unwrap());
    let staged = StagedChunk::new(
        OperationId::new("peer-singleflight"),
        vec![73; 4 * 1024 * 1024],
    );
    store.put(staged.clone()).unwrap();
    let barrier = Arc::new(std::sync::Barrier::new(8));
    let threads: Vec<_> = (0..8)
        .map(|_| {
            let store = store.clone();
            let id = staged.chunk.id.clone();
            let barrier = barrier.clone();
            std::thread::spawn(move || {
                barrier.wait();
                VERIFIED_SCAN_BYTES.with(|count| count.set(0));
                let payload = store.read_peer_range(&id, 0, 4 * 1024 * 1024).unwrap();
                (payload, VERIFIED_SCAN_BYTES.with(|count| count.get()))
            })
        })
        .collect();
    let payloads: Vec<_> = threads
        .into_iter()
        .map(|thread| thread.join().unwrap())
        .collect();
    assert_eq!(
        payloads.iter().map(|(_, scanned)| *scanned).sum::<u64>(),
        staged.chunk.length
    );
    for (payload, _) in &payloads {
        assert!(Arc::ptr_eq(&payload.object, &payloads[0].0.object));
        assert_eq!(payload.as_slice(), staged.bytes());
    }
    assert_eq!(
        store.peer_read_budget.used.load(Ordering::Acquire),
        4 * 1024 * 1024
    );
}

#[test]
fn peer_object_shared_frame_slices_retain_allocation_and_lease() {
    let temp = tempfile::tempdir().unwrap();
    let store = LocalChunkStore::open(temp.path(), "node-a").unwrap();
    let staged = StagedChunk::new(
        OperationId::new("shared-frame-owner"),
        vec![29; 4 * 1024 * 1024],
    );
    store.put(staged.clone()).unwrap();
    let payload = store.read_peer_range(&staged.chunk.id, 17, 128).unwrap();
    let pointer = payload.as_slice().as_ptr();
    let bytes = payload.into_shared_bytes();
    assert_eq!(bytes.as_ptr(), pointer);
    let frame = bytes.slice(3..67);
    assert_eq!(frame.as_ptr(), bytes[3..].as_ptr());
    let retained = frame.clone();
    drop(frame);
    drop(bytes);
    store.clear_peer_residents_for_test();
    assert_eq!(&retained[..], &[29; 64]);
    assert_eq!(
        store.peer_read_budget.used.load(Ordering::Acquire),
        4 * 1024 * 1024
    );
    drop(retained);
    assert_eq!(store.peer_read_budget.used.load(Ordering::Acquire), 0);
}

#[test]
fn peer_object_oversized_chunk_uses_charged_range_fallback_without_residency() {
    let temp = tempfile::tempdir().unwrap();
    let store = LocalChunkStore::open(temp.path(), "node-a").unwrap();
    let staged = StagedChunk::new(
        OperationId::new("peer-fallback"),
        vec![91; PEER_READ_OBJECT_LIMIT + 1],
    );
    store.put(staged.clone()).unwrap();
    let range = store
        .read_peer_range(&staged.chunk.id, PEER_READ_OBJECT_LIMIT as u64, 19)
        .unwrap();
    assert_eq!(range.as_slice(), &[91]);
    assert!(
        store
            .peer_read_cache
            .lock()
            .unwrap()
            .residents
            .iter()
            .all(Option::is_none)
    );
    assert_eq!(store.peer_read_budget.used.load(Ordering::Acquire), 1);
    let pointer = range.as_slice().as_ptr();
    let bytes = range.into_shared_bytes();
    assert_eq!(bytes.as_ptr(), pointer);
    let retained = bytes.slice(..);
    drop(bytes);
    assert_eq!(store.peer_read_budget.used.load(Ordering::Acquire), 1);
    assert_eq!(&retained[..], &[91]);
    drop(retained);
    assert_eq!(store.peer_read_budget.used.load(Ordering::Acquire), 0);
}

#[test]
fn private_range_reads_scan_once_and_copy_only_the_requested_overlap() {
    let temp = tempfile::tempdir().unwrap();
    let store = LocalChunkStore::open(temp.path(), "node-a").unwrap();
    let bytes: Vec<_> = (0..150_001).map(|index| (index % 251) as u8).collect();
    let staged = StagedChunk::new(OperationId::new("single-scan"), bytes.clone());
    store.put(staged.clone()).unwrap();
    let mut out = [9; 19];
    VERIFIED_SCAN_BYTES.with(|bytes| bytes.set(0));
    assert_eq!(
        store.read_at(&staged.chunk.id, 65_531, &mut out).unwrap(),
        19
    );
    assert_eq!(
        VERIFIED_SCAN_BYTES.with(|bytes| bytes.get()),
        bytes.len() as u64 + 2 * outboard::OUTBOARD_GROUP_BYTES as u64
    );
    for offset in [65_531, 149_990] {
        out.fill(9);
        VERIFIED_SCAN_BYTES.with(|bytes| bytes.set(0));
        let count = store
            .read_range_at_uncommitted(&staged.chunk.id, offset, &mut out)
            .unwrap();
        let expected = 19.min(bytes.len() - offset as usize);
        assert_eq!(count, expected);
        assert_eq!(
            &out[..count],
            &bytes[offset as usize..offset as usize + count]
        );
        assert!(out[count..].iter().all(|byte| *byte == 9));
        assert_eq!(
            VERIFIED_SCAN_BYTES.with(|bytes| bytes.get()),
            bytes.len() as u64
        );
    }
}

#[test]
fn private_range_pins_identity_before_its_only_scan() {
    let temp = tempfile::tempdir().unwrap();
    let store = LocalChunkStore::open(temp.path(), "node-a").unwrap();
    let staged = StagedChunk::new(OperationId::new("single-pin"), b"abcdef".to_vec());
    store.put(staged.clone()).unwrap();
    VERIFIED_SCAN_BYTES.with(|bytes| bytes.set(0));
    let reader = store.open_cataloged(&staged.chunk.id).unwrap();
    assert_eq!(VERIFIED_SCAN_BYTES.with(|bytes| bytes.get()), 0);
    let path = temp.path().join("chunks").join(&staged.chunk.id.0);
    fs::rename(&path, temp.path().join("old-inode")).unwrap();
    fs::write(&path, b"abXdef").unwrap();
    let mut private = [9; 3];
    assert_eq!(
        reader
            .read_ranges_at_uncommitted(
                &[VerifiedRangeCopy {
                    chunk_offset: 1,
                    length: private.len(),
                    output_offset: 0,
                }],
                &mut private,
            )
            .unwrap(),
        vec![3]
    );
    assert_eq!(&private, b"bcd");
    assert_eq!(VERIFIED_SCAN_BYTES.with(|bytes| bytes.get()), 6);
    assert!(
        store
            .read_range_at_uncommitted(&staged.chunk.id, 1, &mut private)
            .is_err()
    );
    assert_eq!(store.quarantined_chunks().unwrap().len(), 1);
}

#[test]
fn characterize_failed_local_reads_quarantine_before_returning() {
    let temp = tempfile::tempdir().unwrap();
    let store = LocalChunkStore::open(temp.path(), "node-a").unwrap();
    let first = StagedChunk::new(OperationId::new("read-at"), b"abcdef".to_vec());
    let second = StagedChunk::new(OperationId::new("private"), b"ghijkl".to_vec());
    store.put(first.clone()).unwrap();
    store.put(second.clone()).unwrap();
    fs::write(
        temp.path().join("chunks").join(&first.chunk.id.0),
        b"abXdef",
    )
    .unwrap();
    fs::write(
        temp.path().join("chunks").join(&second.chunk.id.0),
        b"ghXjkl",
    )
    .unwrap();
    assert!(store.quarantined_chunks().unwrap().is_empty());

    let mut out = [0; 6];
    assert!(store.read_at(&first.chunk.id, 0, &mut out).is_err());
    assert_eq!(store.quarantined_chunks().unwrap().len(), 1);
    assert!(
        store
            .read_range_at_uncommitted(&second.chunk.id, 0, &mut out)
            .is_err()
    );
    assert_eq!(store.quarantined_chunks().unwrap().len(), 2);
}

#[test]
fn private_range_rejects_outside_damage_and_quarantines_until_replacement() {
    let temp = tempfile::tempdir().unwrap();
    let store = LocalChunkStore::open(temp.path(), "node-a").unwrap();
    let staged = StagedChunk::new(OperationId::new("single-corrupt"), vec![42; 150_001]);
    store.put(staged.clone()).unwrap();
    let path = temp.path().join("chunks").join(&staged.chunk.id.0);
    let mut damaged = staged.bytes().to_vec();
    damaged[150_000] ^= 1;
    fs::write(&path, damaged).unwrap();
    let mut private = [9; 19];
    assert_eq!(
        store
            .read_range_at_uncommitted(&staged.chunk.id, 65_531, &mut private)
            .unwrap_err()
            .code(),
        afs_error::NODE_TRANSFER_CORRUPT_DATA
    );
    assert_eq!(store.quarantined_chunks().unwrap().len(), 1);
    store.put(staged.clone()).unwrap();
    assert!(store.quarantined_chunks().unwrap().is_empty());
    assert_eq!(
        store
            .read_range_at_uncommitted(&staged.chunk.id, 65_531, &mut private)
            .unwrap(),
        private.len()
    );
    assert_eq!(private, [42; 19]);
}

#[test]
fn private_range_and_verified_open_quarantine_missing_cataloged_files() {
    for private_read in [false, true] {
        let temp = tempfile::tempdir().unwrap();
        let store = LocalChunkStore::open(temp.path(), "node-a").unwrap();
        let staged = StagedChunk::new(OperationId::new("missing-file"), b"abcdef".to_vec());
        store.put(staged.clone()).unwrap();
        fs::remove_file(temp.path().join("chunks").join(&staged.chunk.id.0)).unwrap();
        if private_read {
            assert!(
                store
                    .read_range_at_uncommitted(&staged.chunk.id, 0, &mut [9; 3])
                    .is_err()
            );
        } else {
            assert!(store.open_verified(&staged.chunk.id).is_err());
        }
        assert_eq!(store.quarantined_chunks().unwrap().len(), 1);
        store.put(staged.clone()).unwrap();
        assert!(store.quarantined_chunks().unwrap().is_empty());
        let mut out = [9; 3];
        assert_eq!(
            store
                .read_range_at_uncommitted(&staged.chunk.id, 0, &mut out)
                .unwrap(),
            3
        );
        assert_eq!(&out, b"abc");
    }
}

#[test]
fn private_range_work_metrics_count_actual_scan_and_copy_bytes() {
    let temp = tempfile::tempdir().unwrap();
    let registry = afs_metrics::Registry::new();
    let store = LocalChunkStore::open(temp.path(), "node-a")
        .unwrap()
        .with_read_work_metrics(
            super::super::read_work::ReadWorkMetrics::register(&registry).unwrap(),
        );
    let staged = StagedChunk::new(OperationId::new("work-counts"), vec![7; 65_537]);
    store.put(staged.clone()).unwrap();
    store.read_at(&staged.chunk.id, 0, &mut [0; 3]).unwrap();
    store
        .read_range_at_uncommitted(&staged.chunk.id, 0, &mut [0; 3])
        .unwrap();
    let text = afs_metrics::encode_text(&registry).unwrap();
    assert!(text.contains("afs_dfs_read_scans_total 2\n"), "{text}");
    assert!(
        text.contains("afs_dfs_read_scan_bytes_total 131074\n"),
        "{text}"
    );
    assert!(
        text.contains("afs_dfs_read_copy_bytes_total{stage=\"scan_to_range\"} 3\n"),
        "{text}"
    );
    assert!(
        text.contains("afs_dfs_read_copy_bytes_total{stage=\"pinned_range_to_output\"} 3\n"),
        "{text}"
    );
    // The verified range passes through the official Bao codec before
    // exact extraction: two full covering-group copies plus three bytes.
    assert!(
        text.contains("afs_dfs_read_copy_bytes_total{stage=\"outboard_range_delivery\"} 65539\n"),
        "{text}"
    );
}

#[test]
fn private_range_rejects_truncated_files_and_keeps_eof_behavior() {
    let temp = tempfile::tempdir().unwrap();
    let store = LocalChunkStore::open(temp.path(), "node-a").unwrap();
    let staged = StagedChunk::new(OperationId::new("single-truncate"), b"abcdef".to_vec());
    store.put(staged.clone()).unwrap();
    let mut private = [9; 8];
    assert_eq!(
        store
            .read_range_at_uncommitted(&staged.chunk.id, 4, &mut private)
            .unwrap(),
        2
    );
    assert_eq!(&private, &[b'e', b'f', 9, 9, 9, 9, 9, 9]);
    assert_eq!(
        store
            .read_range_at_uncommitted(&staged.chunk.id, u64::MAX, &mut private)
            .unwrap(),
        0
    );
    assert_eq!(
        store
            .read_range_at_uncommitted(&staged.chunk.id, 0, &mut [])
            .unwrap(),
        0
    );
    fs::write(temp.path().join("chunks").join(&staged.chunk.id.0), b"abc").unwrap();
    assert_eq!(
        store
            .read_range_at_uncommitted(&staged.chunk.id, 0, &mut private)
            .unwrap_err()
            .code(),
        afs_error::NODE_TRANSFER_CORRUPT_DATA
    );
    assert_eq!(store.quarantined_chunks().unwrap().len(), 1);
}

#[test]
fn corrupt_cataloged_chunk_is_replaced_by_immutable_retry() {
    let temp = tempfile::tempdir().unwrap();
    let store = LocalChunkStore::open(temp.path(), "node-a").unwrap();
    let staged = StagedChunk::new(OperationId::new("retry-corrupt"), b"abcdef".to_vec());
    let first = store.put(staged.clone()).unwrap();
    let pin = store.open_verified(&staged.chunk.id).unwrap();
    fs::write(
        temp.path().join("chunks").join(&staged.chunk.id.0),
        b"abXdef",
    )
    .unwrap();
    let repaired = store.put(staged.clone()).unwrap();
    assert!(repaired.durable_acks[0].catalog_revision > first.durable_acks[0].catalog_revision);
    let mut out = [9; 6];
    assert!(pin.read_at(0, &mut out).is_err());
    assert_eq!(out, [9; 6]);
    // Revalidating an old pin cannot quarantine the newer healthy file.
    store.quarantine_if_invalid(&staged.chunk.id).unwrap();
    assert!(store.quarantined_chunks().unwrap().is_empty());
    assert_eq!(store.read_at(&staged.chunk.id, 0, &mut out).unwrap(), 6);
    assert_eq!(&out, b"abcdef");
    drop(store);
    let recovered = LocalChunkStore::open(temp.path(), "node-a").unwrap();
    assert_eq!(recovered.read_at(&staged.chunk.id, 0, &mut out).unwrap(), 6);
    assert_eq!(&out, b"abcdef");
}

#[test]
fn capacity_reports_fd_backed_root_after_path_replacement_and_catalog_advance() {
    let temp = tempfile::tempdir().unwrap();
    let root = temp.path().join("store");
    let store = LocalChunkStore::open(&root, "node-a").unwrap();
    let first = store.capacity().unwrap();
    assert!(first.bsize > 0);
    assert!(first.frsize > 0);
    assert!(first.namelen > 0);
    assert!(first.blocks >= first.bfree);
    assert!(first.bfree >= first.bavail);

    store
        .put(StagedChunk::new(
            OperationId::new("capacity"),
            b"bytes".to_vec(),
        ))
        .unwrap();
    let descriptor_after_write = store.device_descriptor().unwrap();
    assert!(descriptor_after_write.catalog_revision > 0);

    let moved = temp.path().join("store-moved");
    fs::rename(&root, &moved).unwrap();
    // A path-based reopen must fail; the held directory FD remains valid.
    std::os::unix::fs::symlink(temp.path().join("missing"), &root).unwrap();

    let after_replacement = store.capacity().unwrap();
    assert!(after_replacement.bsize > 0);
    assert!(after_replacement.frsize > 0);
    assert_eq!(store.device_descriptor().unwrap(), descriptor_after_write);
}

#[test]
fn quarantine_survives_restart_and_verified_replacement_clears_it() {
    let temp = tempfile::tempdir().unwrap();
    let store = LocalChunkStore::open(temp.path(), "node-a").unwrap();
    let staged = StagedChunk::new(OperationId::new("quarantine"), b"abcdef".to_vec());
    store.put(staged.clone()).unwrap();
    fs::write(
        temp.path().join("chunks").join(&staged.chunk.id.0),
        b"abXdef",
    )
    .unwrap();
    assert!(store.open_verified(&staged.chunk.id).is_err());
    let records = store.quarantined_chunks().unwrap();
    assert_eq!(records.len(), 1);
    let quarantine_revision = records[0].catalog_revision;
    drop(store);
    let recovered = LocalChunkStore::open(temp.path(), "node-a").unwrap();
    assert_eq!(recovered.quarantined_chunks().unwrap(), records);
    assert!(recovered.open_verified(&staged.chunk.id).is_err());
    let receipt = recovered.put(staged.clone()).unwrap();
    assert!(receipt.durable_acks[0].catalog_revision > quarantine_revision);
    assert!(recovered.quarantined_chunks().unwrap().is_empty());
    let mut out = [0; 6];
    recovered.read_at(&staged.chunk.id, 0, &mut out).unwrap();
    assert_eq!(&out, b"abcdef");
}

#[test]
fn recovery_quarantines_missing_or_truncated_chunks_without_losing_healthy_files() {
    let temp = tempfile::tempdir().unwrap();
    let store = LocalChunkStore::open(temp.path(), "node-a").unwrap();
    let items: Vec<_> = ["missing", "truncated", "healthy"]
        .into_iter()
        .map(|name| StagedChunk::new(OperationId::new(name), name.as_bytes().to_vec()))
        .collect();
    for item in &items {
        store.put(item.clone()).unwrap();
    }
    fs::remove_file(temp.path().join("chunks").join(&items[0].chunk.id.0)).unwrap();
    fs::write(temp.path().join("chunks").join(&items[1].chunk.id.0), b"x").unwrap();
    drop(store);
    let recovered = LocalChunkStore::open(temp.path(), "node-a").unwrap();
    assert_eq!(recovered.quarantined_chunks().unwrap().len(), 2);
    let mut out = [0; 7];
    recovered.read_at(&items[2].chunk.id, 0, &mut out).unwrap();
    assert_eq!(&out, b"healthy");
    for item in &items[..2] {
        recovered.put(item.clone()).unwrap();
    }
    assert!(recovered.quarantined_chunks().unwrap().is_empty());
}

#[test]
fn pinned_reader_verifies_partial_ranges_and_rejects_damage_outside_them() {
    let temp = tempfile::tempdir().unwrap();
    let store = LocalChunkStore::open(temp.path(), "node-a").unwrap();
    let bytes: Vec<_> = (0..524_289).map(|index| (index % 251) as u8).collect();
    let staged = StagedChunk::new(OperationId::new("range"), bytes.clone());
    store.put(staged.clone()).unwrap();
    let reader = store.open_verified(&staged.chunk.id).unwrap();
    let mut out = [9; 19];
    // Cover both the old 64KiB and candidate 256KiB scan boundaries,
    // including a final short scan outside either requested range.
    for offset in [65_531, 262_139] {
        assert_eq!(reader.read_at(offset, &mut out).unwrap(), 19);
        assert_eq!(&out, &bytes[offset as usize..offset as usize + 19]);
    }
    let path = temp.path().join("chunks").join(&staged.chunk.id.0);
    let mut damaged = bytes;
    damaged[524_288] ^= 1;
    fs::write(path, damaged).unwrap();
    out.fill(9);
    assert_eq!(
        reader.read_at(65_531, &mut out).unwrap_err().code(),
        afs_error::NODE_TRANSFER_CORRUPT_DATA
    );
    assert_eq!(out, [9; 19]);
}

#[test]
fn verify_file_zero_range_still_hashes_the_whole_chunk() {
    let temp = tempfile::tempdir().unwrap();
    let store = LocalChunkStore::open(temp.path(), "node-a").unwrap();
    let staged = StagedChunk::new(OperationId::new("verify-zero"), b"abcdef".to_vec());
    store.put(staged.clone()).unwrap();
    let path = temp.path().join("chunks").join(&staged.chunk.id.0);
    verify_file(&path, &staged.chunk).unwrap();
    fs::write(&path, b"abcXef").unwrap();
    assert_eq!(
        verify_file(&path, &staged.chunk).unwrap_err().code(),
        afs_error::NODE_TRANSFER_CORRUPT_DATA
    );
}

#[test]
fn pinned_reader_rejects_truncation_without_publishing_a_partial_read() {
    let temp = tempfile::tempdir().unwrap();
    let store = LocalChunkStore::open(temp.path(), "node-a").unwrap();
    let staged = StagedChunk::new(OperationId::new("truncate"), b"abcdef".to_vec());
    store.put(staged.clone()).unwrap();
    let reader = store.open_verified(&staged.chunk.id).unwrap();
    fs::write(temp.path().join("chunks").join(&staged.chunk.id.0), b"abc").unwrap();
    let mut out = [9; 6];
    assert_eq!(
        reader.read_at(0, &mut out).unwrap_err().code(),
        afs_error::NODE_TRANSFER_CORRUPT_DATA
    );
    assert_eq!(out, [9; 6]);
}

#[test]
fn pinned_reader_preserves_descriptor_identity_and_eof_behavior() {
    let temp = tempfile::tempdir().unwrap();
    let store = LocalChunkStore::open(temp.path(), "node-a").unwrap();
    let staged = StagedChunk::new(OperationId::new("pin"), b"abcdef".to_vec());
    store.put(staged.clone()).unwrap();
    let reader = store.open_verified(&staged.chunk.id).unwrap();
    let replacement = temp.path().join("replacement");
    fs::write(&replacement, b"abXdef").unwrap();
    fs::rename(
        replacement,
        temp.path().join("chunks").join(&staged.chunk.id.0),
    )
    .unwrap();
    let mut out = [9; 8];
    assert_eq!(reader.read_at(0, &mut out).unwrap(), 6);
    assert_eq!(&out, &[b'a', b'b', b'c', b'd', b'e', b'f', 9, 9]);
    out.fill(9);
    assert_eq!(reader.read_at(4, &mut out).unwrap(), 2);
    assert_eq!(&out, &[b'e', b'f', 9, 9, 9, 9, 9, 9]);
    assert_eq!(reader.read_at(u64::MAX, &mut out).unwrap(), 0);
    assert_eq!(reader.read_at(0, &mut []).unwrap(), 0);
    assert!(store.open_verified(&staged.chunk.id).is_err());
}

#[test]
fn immutable_chunk_retry_preserves_content_after_catalog_advances_and_restart() {
    let temp = tempfile::tempdir().unwrap();
    let store = LocalChunkStore::open(temp.path(), "node-a").unwrap();
    let target = ReplicaTarget {
        node_id: "node-a".into(),
        node_epoch: 3,
        data_endpoint: "https://node-a".into(),
        device: store.device_descriptor().unwrap(),
    };
    let first = StagedChunk::new(OperationId::new("op-first"), b"first".to_vec());
    let ack = store.persist(&first, &target, 4, 5).unwrap();
    store
        .persist(
            &StagedChunk::new(OperationId::new("op-second"), b"second".to_vec()),
            &target,
            4,
            5,
        )
        .unwrap();
    let replay = store.persist(&first, &target, 4, 5).unwrap();
    assert_eq!(replay.chunk_id, ack.chunk_id);
    assert_eq!(replay.verified_digest, ack.verified_digest);
    assert_eq!(replay.persisted_bytes, ack.persisted_bytes);
    assert!(replay.catalog_revision > ack.catalog_revision);
    let mut newer_target = target.clone();
    newer_target.device = store.device_descriptor().unwrap();
    let floor_ack = store.persist(&first, &newer_target, 6, 7).unwrap();
    assert_eq!(
        floor_ack.catalog_revision,
        newer_target.device.catalog_revision
    );
    drop(store);
    let recovered = LocalChunkStore::open(temp.path(), "node-a").unwrap();
    let recovered_ack = recovered.persist(&first, &target, 4, 5).unwrap();
    assert_eq!(recovered_ack.chunk_id, ack.chunk_id);
    assert_eq!(recovered_ack.verified_digest, ack.verified_digest);
    assert_eq!(recovered_ack.catalog_revision, replay.catalog_revision);
    let mut bytes = [0; 5];
    assert_eq!(
        recovered.read_at(&first.chunk.id, 0, &mut bytes).unwrap(),
        5
    );
    assert_eq!(&bytes, b"first");
    assert_eq!(
        recovered.persist(&first, &newer_target, 6, 7).unwrap(),
        floor_ack
    );
}
